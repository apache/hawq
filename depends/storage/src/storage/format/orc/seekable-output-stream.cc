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

#include "storage/format/orc/seekable-output-stream.h"

namespace orc {

uint64_t SeekableOutputStream::COMPRESS_BLOCK_SIZE = ORC_COMPRESSION_BLOCK_SIZE;

std::unique_ptr<SeekableOutputStream> createBlockCompressor(
    orc::CompressionKind kind) {
  std::unique_ptr<SeekableOutputStream> stream;
  switch (kind) {
    case orc::CompressionKind::CompressionKind_SNAPPY:
      stream.reset(new SnappyCompressionStream());
      break;
    case orc::CompressionKind::CompressionKind_LZ4:
      stream.reset(new LZ4CompressionStream());
      break;
    case orc::CompressionKind::CompressionKind_NONE:
      stream.reset(new BufferedStream());
      break;
    default:
      break;
  }

  return std::move(stream);
}

}  // end of namespace orc
