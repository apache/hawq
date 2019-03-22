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

#include <fstream>

#include "dbcommon/common/node-serializer.h"
#include "dbcommon/common/tuple-batch-store.h"
#include "dbcommon/utils/byte-buffer.h"

#define EOF_MARK ((size_t)(0xFFFFFFFFFFFFFFFF))

namespace dbcommon {

NTupleBatchStore::~NTupleBatchStore() {
  if (file_) {
    file_->close();
  }
}

void NTupleBatchStore::writeEOF() {
  if (file_) {
    if (mode_ == Mode::OUTPUT) {
      size_t sizeOfBuffer = EOF_MARK;
      filesystem_->write(file_.get(), reinterpret_cast<char *>(&sizeOfBuffer),
                         sizeof(sizeOfBuffer));
    }
  }
}

void NTupleBatchStore::reset() {
  if (file_) {
    filesystem_->seek(file_.get(), 0);
  } else {
    tbsOffset_ = 0;
  }
}

void NTupleBatchStore::PutIntoNTupleBatchStore(dbcommon::TupleBatch::uptr tb) {
  assert(mode_ == Mode::IN_MEMEORY || mode_ == Mode::OUTPUT);
  totalRows_ += tb->getNumOfRows();
  if (mode_ == Mode::IN_MEMEORY) {
    tb->materialize();
    tbs_.push_back(std::move(tb));
  } else {
    size_t sizeBuf;
    std::string dataBuf;
    tb->serialize(&dataBuf, tb->getTupleBatchSize());
    sizeBuf = dataBuf.size();
    filesystem_->write(file_.get(), reinterpret_cast<char *>(&sizeBuf),
                       sizeof(sizeBuf));
    filesystem_->write(file_.get(), dataBuf.data(), dataBuf.size());
  }
}

void NTupleBatchStore::PutIntoNTupleBatchStore(dbcommon::TupleBatch *tb) {
  assert(mode_ == Mode::IN_MEMEORY || mode_ == Mode::OUTPUT);
  totalRows_ += tb->getNumOfRows();
  if (mode_ == Mode::IN_MEMEORY) {
    tbs_.push_back(tb->clone());
  } else {
    size_t sizeBuf;
    std::string dataBuf;
    tb->serialize(&dataBuf, tb->getTupleBatchSize());
    sizeBuf = dataBuf.size();
    filesystem_->write(file_.get(), reinterpret_cast<char *>(&sizeBuf),
                       sizeof(sizeBuf));
    filesystem_->write(file_.get(), dataBuf.data(), dataBuf.size());
  }
}

dbcommon::TupleBatch::uptr NTupleBatchStore::GetFromNTupleBatchStore() {
  if (mode_ == Mode::IN_MEMEORY) {
    if (tbsOffset_ < tbs_.size()) {
      auto tb = tbs_[tbsOffset_++]->clone();
      return std::move(tb);
    } else {
      return nullptr;
    }
  } else {
    size_t sizeOfBuffer;
    filesystem_->read(file_.get(), reinterpret_cast<char *>(&sizeOfBuffer),
                      sizeof(sizeOfBuffer));
    if (sizeOfBuffer == EOF_MARK) return nullptr;
    buffer_.resize(sizeOfBuffer);
    filesystem_->read(file_.get(), buffer_.data(), sizeOfBuffer);
    TupleBatch::uptr tb(new TupleBatch);
    tb->deserialize(std::string(buffer_.data(), buffer_.size()));
    return std::move(tb);
  }
}

}  // namespace dbcommon
