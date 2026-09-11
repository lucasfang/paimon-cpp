/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

#include "paimon/common/utils/file_block_cache.h"

#include <cstring>
#include <string>

#include "paimon/common/memory/bytes_utils.h"
#include "paimon/common/utils/io_trace.h"

namespace paimon {

FileBlockCache::FileBlockCache(const std::shared_ptr<InputStream>& stream, uint64_t file_size,
                               uint64_t block_size, uint64_t capacity,
                               const std::shared_ptr<MemoryPool>& memory_pool)
    : stream_(stream),
      file_size_(file_size),
      block_size_(block_size),
      capacity_(capacity),
      memory_pool_(memory_pool),
      trace_uri_(io_trace::Uri(stream)) {}

FileBlockCache::~FileBlockCache() {
    // The fetches write into the block buffers, so they must not outlive the
    // stream they read from.
    std::lock_guard<std::mutex> lock(mutex_);
    for (auto& block : blocks_) {
        block.second->future.wait();
    }
}

bool FileBlockCache::Read(const ByteRange& range, char* dest) {
    if (!CanServe(range)) {
        return false;
    }
    const uint64_t index = IndexOf(range.offset);
    std::shared_ptr<Block> block;
    bool dispatch = false;
    {
        // Publishing the promise-backed block under the lock before its fetch is
        // dispatched is what makes concurrent readers of the same block wait for
        // that one fetch instead of issuing their own.
        std::lock_guard<std::mutex> lock(mutex_);
        auto it = blocks_.find(index);
        if (it != blocks_.end()) {
            block = it->second;
        } else {
            const ByteRange block_range = RangeOf(index);
            // Blocks are never evicted, so an exhausted capacity means this read
            // goes back to the caller instead of replacing a cached block.
            if (cached_bytes_ + block_range.length > capacity_) {
                return false;
            }
            block = std::make_shared<Block>();
            block->range = block_range;
            // The buffer keeps the pool alive, for the fetch callbacks that a
            // stream destroys later than it resolves them.
            block->buffer = AllocateBytesKeepingPoolAlive(block_range.length, memory_pool_);
            block->promise = std::make_shared<std::promise<Status>>();
            block->future = block->promise->get_future().share();
            blocks_.emplace(index, block);
            cached_bytes_ += block_range.length;
            dispatch = true;
        }
    }
    if (dispatch) {
        Fetch(block);
    }
    // Wait and copy OUTSIDE the lock, so that a reader waiting for a fetch does
    // not keep the other readers out of the map.
    //
    // TEMPORARY: trace how long the reader waits for the fetch of its block, see
    // io_trace.h. Near zero for a block that is already cached.
    const bool trace = io_trace::Enabled();
    const io_trace::Instant wait_started_at = trace ? io_trace::Now() : io_trace::Instant{};
    const Status fetch_status = block->future.get();
    if (trace) {
        io_trace::Emit("block-wait", trace_uri_, range.offset, range.length, wait_started_at,
                       io_trace::ElapsedMicros(wait_started_at), io_trace::kNotApplicable,
                       fetch_status.ok() ? "ok" : fetch_status.ToString().c_str());
    }
    if (!fetch_status.ok()) {
        // A block fetch reads more than the caller asked for, so its failure must
        // not fail the caller's read: the read goes back to the caller, which
        // reports the real error itself if its own bytes cannot be read either.
        // This is what keeps a block whose range the file does not have - the
        // file metadata records a size larger than the physical file - from
        // failing the reads of the region it covers.
        //
        // The block is left in place with its failed future, so the later reads
        // of that block are declined without fetching it again.
        return false;
    }
    std::memcpy(dest, block->buffer->data() + (range.offset - block->range.offset), range.length);
    hits_.Add(range.length);
    return true;
}

void FileBlockCache::Release() {
    std::lock_guard<std::mutex> lock(mutex_);
    // Blocks are never evicted, so waiting on blocks_ covers every dispatched
    // fetch before the buffers they write into go away.
    for (auto& block : blocks_) {
        block.second->future.wait();
    }
    blocks_.clear();
    cached_bytes_ = 0;
}

void FileBlockCache::ResetCounters() {
    hits_.Reset();
    fetches_.Reset();
}

FileBlockCache::Counters FileBlockCache::GetCounters() const {
    Counters counters;
    counters.hits = hits_.Count();
    counters.hit_bytes = hits_.Bytes();
    counters.fetches = fetches_.Count();
    counters.fetch_bytes = fetches_.Bytes();
    return counters;
}

bool FileBlockCache::CanServe(const ByteRange& range) const {
    if (capacity_ == 0 || block_size_ == 0 || file_size_ == 0) {
        return false;
    }
    if (range.length == 0 || range.length > block_size_) {
        return false;
    }
    // A read reaching past EOF is left to the caller: serving it would mean
    // short-reading into the block buffer.
    if (range.offset >= file_size_ || range.length > file_size_ - range.offset) {
        return false;
    }
    // A read straddling two blocks would need both of them to be present; it is
    // left to the caller instead, which keeps one block per served read.
    return IndexOf(range.offset) == IndexOf(range.offset + range.length - 1);
}

uint64_t FileBlockCache::IndexOf(uint64_t offset) const {
    // Counted from the end of the file, so that block 0 is the last block.
    return (file_size_ - 1 - offset) / block_size_;
}

ByteRange FileBlockCache::RangeOf(uint64_t index) const {
    const uint64_t end = file_size_ - index * block_size_;
    const uint64_t offset = end > block_size_ ? end - block_size_ : 0;
    return {offset, end - offset};
}

void FileBlockCache::Fetch(const std::shared_ptr<Block>& block) {
    fetches_.Add(block->range.length);
    auto promise = block->promise;
    auto buffer = block->buffer;
    // The buffer and the promise are captured, so the async read keeps its
    // destination and the future it resolves alive. The buffer keeps the memory
    // pool alive as well, so a callback outliving this cache still frees the
    // buffer against a live pool.
    //
    // TEMPORARY: trace the IO this cache issues, see io_trace.h. The uri is
    // copied into the callback rather than reached for through `this`, which the
    // thread resolving the fetch may outlive.
    const bool trace = io_trace::Enabled();
    io_trace::Instant dispatched_at;
    if (trace) {
        dispatched_at = io_trace::Now();
        io_trace::Emit("block-dispatch", trace_uri_, block->range.offset, block->range.length,
                       dispatched_at, io_trace::kNotApplicable, io_trace::EnterInflight(), nullptr);
    }
    stream_->ReadAsync(buffer->data(), static_cast<int64_t>(buffer->size()),
                       static_cast<int64_t>(block->range.offset),
                       [promise, buffer, trace, dispatched_at, range = block->range,
                        uri = trace_uri_](Status status) {
                           if (trace) {
                               io_trace::Emit("block-done", uri, range.offset, range.length,
                                              dispatched_at, io_trace::ElapsedMicros(dispatched_at),
                                              io_trace::LeaveInflight(),
                                              status.ok() ? "ok" : status.ToString().c_str());
                           }
                           promise->set_value(status);
                       });
}

}  // namespace paimon
