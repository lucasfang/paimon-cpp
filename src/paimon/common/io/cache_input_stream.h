/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#pragma once

#include <memory>
#include <string>
#include <utility>

#include "paimon/common/utils/io_trace.h"
#include "paimon/common/utils/math.h"
#include "paimon/common/utils/read_ahead_cache.h"
#include "paimon/fs/file_system.h"

namespace paimon {

/// Serves the positional reads it can out of the given read-ahead cache and forwards the rest to
/// the underlying stream.
///
/// The underlying stream is held through a shared pointer so that several readers of the same file
/// can share one open stream. Sharing is only safe for readers that read positionally: the
/// stateful `Seek()`/`GetPos()`/`Read(buffer, size)` methods are forwarded as they are and remain
/// subject to the thread safety of the underlying stream.
class CacheInputStream : public InputStream {
 public:
    CacheInputStream(const std::shared_ptr<InputStream>& input_stream,
                     const std::shared_ptr<ReadAheadCache>& cache)
        : cache_(cache),
          input_stream_(input_stream),
          trace_uri_(io_trace::Uri(input_stream)) {}

    Status Seek(int64_t offset, SeekOrigin origin) override {
        return input_stream_->Seek(offset, origin);
    }
    Result<int64_t> GetPos() const override {
        return input_stream_->GetPos();
    }
    Result<int64_t> Read(char* buffer, int64_t size) override {
        return input_stream_->Read(buffer, size);
    }
    Result<int64_t> Read(char* buffer, int64_t size, int64_t offset) override {
        if (cache_) {
            PAIMON_RETURN_NOT_OK(ValidateValueInRange<uint64_t>(offset, "read offset"));
            PAIMON_RETURN_NOT_OK(ValidateValueInRange<uint64_t>(size, "read size"));
            ByteRange range{static_cast<uint64_t>(offset), static_cast<uint64_t>(size)};
            PAIMON_ASSIGN_OR_RAISE(bool hit, cache_->Read(range, buffer));
            if (hit) {
                return size;
            }
        }
        // TEMPORARY: trace the direct read this stream falls back to on a cache miss, see
        // io_trace.h. This is the underlying IO the cache did not manage to fold into a prefetch
        // or a block, and it blocks the reader for its whole duration.
        const bool trace = io_trace::Enabled();
        if (!trace) {
            return input_stream_->Read(buffer, size, offset);
        }
        const io_trace::Instant dispatched_at = io_trace::Now();
        const int64_t inflight = io_trace::EnterInflight();
        Result<int64_t> result = input_stream_->Read(buffer, size, offset);
        io_trace::LeaveInflight();
        io_trace::Emit("miss-read", trace_uri_, static_cast<uint64_t>(offset),
                       static_cast<uint64_t>(size), dispatched_at,
                       io_trace::ElapsedMicros(dispatched_at), inflight,
                       result.ok() ? "ok" : result.status().ToString().c_str());
        return result;
    }
    void ReadAsync(char* buffer, int64_t size, int64_t offset,
                   std::function<void(Status)>&& callback) override {
        if (cache_) {
            Status status = ValidateValueInRange<uint64_t>(offset, "read offset");
            if (!status.ok()) {
                callback(status);
                return;
            }
            status = ValidateValueInRange<uint64_t>(size, "read size");
            if (!status.ok()) {
                callback(status);
                return;
            }
            ByteRange range{static_cast<uint64_t>(offset), static_cast<uint64_t>(size)};
            Result<bool> hit = cache_->Read(range, buffer);
            if (!hit.ok()) {
                callback(hit.status());
                return;
            }
            if (hit.value()) {
                callback(Status::OK());
                return;
            }
        }
        // TEMPORARY: trace the async direct read this stream falls back to on a cache miss, the
        // way the prefetch fetches are traced, with the uri copied into the callback rather than
        // reached for through `this`, which the thread resolving the read may outlive. See
        // io_trace.h.
        const bool trace = io_trace::Enabled();
        if (!trace) {
            return input_stream_->ReadAsync(buffer, size, offset, std::move(callback));
        }
        const io_trace::Instant dispatched_at = io_trace::Now();
        io_trace::Emit("miss-read-async-dispatch", trace_uri_, static_cast<uint64_t>(offset),
                       static_cast<uint64_t>(size), dispatched_at, io_trace::kNotApplicable,
                       io_trace::EnterInflight(), nullptr);
        return input_stream_->ReadAsync(
            buffer, size, offset,
            [dispatched_at, offset, size, uri = trace_uri_,
             callback = std::move(callback)](Status status) mutable {
                io_trace::Emit("miss-read-async-done", uri, static_cast<uint64_t>(offset),
                               static_cast<uint64_t>(size), dispatched_at,
                               io_trace::ElapsedMicros(dispatched_at), io_trace::LeaveInflight(),
                               status.ok() ? "ok" : status.ToString().c_str());
                callback(status);
            });
    }

    Status Close() override {
        return input_stream_->Close();
    }

    Result<std::string> GetUri() const override {
        return input_stream_->GetUri();
    }

    Result<int64_t> Length() const override {
        return input_stream_->Length();
    }

 private:
    std::shared_ptr<ReadAheadCache> cache_;
    std::shared_ptr<InputStream> input_stream_;
    // TEMPORARY: the file the traced fallback reads read, resolved once, empty when the tracing is
    // off. See io_trace.h.
    std::string trace_uri_;
};

}  // namespace paimon
