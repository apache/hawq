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

#include "storage/format/orc/rle.h"
#include "storage/format/orc/exceptions.h"
#include "storage/format/orc/rle-v0.h"
#include "storage/format/orc/rle-v1.h"
#include "storage/format/orc/rle-v2.h"

namespace orc {

// must be non-inline!
RleDecoder::~RleDecoder() {}

std::unique_ptr<RleDecoder> createRleDecoder(
    std::unique_ptr<SeekableInputStream> input, bool isSigned,
    RleVersion version, dbcommon::MemoryPool& pool,  // NOLINT
    ORCTypeKind type) {
  switch (static_cast<int64_t>(version)) {
    case RleVersion_1:
      // We don't have std::make_unique() yet.
      switch (type) {
        case LONG:
          return std::unique_ptr<RleDecoder>(
              new RleDecoderV1<int64_t, uint64_t>(std::move(input), isSigned));
        case INT:
          return std::unique_ptr<RleDecoder>(
              new RleDecoderV1<int32_t, uint64_t>(std::move(input), isSigned));
        case SHORT:
          return std::unique_ptr<RleDecoder>(
              new RleDecoderV1<int16_t, uint64_t>(std::move(input), isSigned));
        default:
          LOG_ERROR(ERRCODE_INTERNAL_ERROR, "Not implemented yet");
      }
    case RleVersion_2:
      switch (type) {
        case LONG:
          return std::unique_ptr<RleDecoder>(
              new RleDecoderV2<int64_t, uint64_t>(std::move(input), isSigned,
                                                  pool));
        case INT:
          return std::unique_ptr<RleDecoder>(
              new RleDecoderV2<int32_t, uint32_t>(std::move(input), isSigned,
                                                  pool));
        case SHORT:
          return std::unique_ptr<RleDecoder>(
              new RleDecoderV2<int16_t, uint16_t>(std::move(input), isSigned,
                                                  pool));
        default:
          LOG_ERROR(ERRCODE_INTERNAL_ERROR, "Not implemented yet");
      }
    case RleVersion_0:
      switch (type) {
        case LONG:
          return std::unique_ptr<RleDecoder>(
              new RleDecoderV0<int64_t>(std::move(input)));
        case INT:
          return std::unique_ptr<RleDecoder>(
              new RleDecoderV0<int32_t>(std::move(input)));
        case SHORT:
          return std::unique_ptr<RleDecoder>(
              new RleDecoderV0<int16_t>(std::move(input)));
        default:
          LOG_ERROR(ERRCODE_INTERNAL_ERROR, "Not implemented yet");
      }
    default:
      LOG_ERROR(ERRCODE_INTERNAL_ERROR, "Not implemented yet");
  }
}

std::unique_ptr<RleCoder> createRleCoder(bool isSigned, RleVersion version,
                                         ORCTypeKind type, CompressionKind kind,
                                         bool alignedBitpacking) {  // NOLINT
  switch (static_cast<int64_t>(version)) {
    case RleVersion_1:
      // We don't have std::make_unique() yet.
      switch (type) {
        case LONG:
          return std::unique_ptr<RleCoder>(
              new RleCoderV1<int64_t>(createBlockCompressor(kind), isSigned));
        case INT:
          return std::unique_ptr<RleCoder>(
              new RleCoderV1<int32_t>(createBlockCompressor(kind), isSigned));
        case SHORT:
          return std::unique_ptr<RleCoder>(
              new RleCoderV1<int16_t>(createBlockCompressor(kind), isSigned));
        default:
          LOG_ERROR(ERRCODE_INTERNAL_ERROR, "Not implemented yet");
      }
    case RleVersion_2:
      switch (type) {
        case LONG:
          return std::unique_ptr<RleCoder>(new RleCoderV2<int64_t>(
              createBlockCompressor(kind), isSigned, alignedBitpacking));
        case INT:
          return std::unique_ptr<RleCoder>(new RleCoderV2<int32_t>(
              createBlockCompressor(kind), isSigned, alignedBitpacking));
        case SHORT:
          return std::unique_ptr<RleCoder>(new RleCoderV2<int16_t>(
              createBlockCompressor(kind), isSigned, alignedBitpacking));
        default:
          LOG_ERROR(ERRCODE_INTERNAL_ERROR, "Not implemented yet");
      }
    case RleVersion_0:
      switch (type) {
        case LONG:
          return std::unique_ptr<RleCoder>(
              new RleCoderV0<int64_t>(createBlockCompressor(kind)));
        case INT:
          return std::unique_ptr<RleCoder>(
              new RleCoderV0<int32_t>(createBlockCompressor(kind)));
        case SHORT:
          return std::unique_ptr<RleCoder>(
              new RleCoderV0<int16_t>(createBlockCompressor(kind)));
        default:
          LOG_ERROR(ERRCODE_INTERNAL_ERROR, "Not implemented yet");
      }
    default:
      LOG_ERROR(ERRCODE_INTERNAL_ERROR, "Not implemented yet");
  }
}

}  // namespace orc
