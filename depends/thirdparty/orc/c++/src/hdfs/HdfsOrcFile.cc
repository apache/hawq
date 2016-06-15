/**
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

#include <string>
#include <memory>

#include "Exceptions.hh"
#include "orc/OrcFile.hh"

#include "wrap/libhdfs3-wrapper.h"

namespace orc {

  class HdfsFileInputStream : public InputStream {
  private:
    std::string filename;
    hdfsFS fs;
    hdfsFile file;
    int bufferSize;
    short replication;
    tOffset blockSize;
    uint64_t totalLength;

  private:
    void init(hdfsFS _fs, std::string _filename) {
      fs = _fs;
      filename = _filename;
      file = hdfsOpenFile(fs, filename.c_str(), O_RDONLY,
                          bufferSize, replication, blockSize);
      if (!file) {
        throw ParseError("Can't open hdfs file " + filename);
      }
      auto fileInfo = hdfsGetPathInfo(fs, filename.c_str());
      if (fileInfo == NULL) {
        throw ParseError("Can't get info of file " + filename);
      }
      totalLength = static_cast<uint64_t>(fileInfo->mSize);
      hdfsFreeFileInfo(fileInfo, 1);
    }

  public:
    HdfsFileInputStream(hdfsFS _fs, std::string _filename,
                        int _bufferSize = 0,
                        int _replication = 0,
                        tOffset _blockSize = 0) 
        : bufferSize(_bufferSize),
        replication(_replication),
        blockSize(_blockSize) {
      init(_fs, _filename);
    }

    HdfsFileInputStream(hdfsFS _fs, const char * _filename,
                        int _bufferSize = 0,
                        int _replication = 0,
                        tOffset _blockSize = 0) 
        : bufferSize(_bufferSize),
        replication(_replication),
        blockSize(_blockSize) {
      init(_fs, std::string(_filename));
    }
 
    ~HdfsFileInputStream();

    uint64_t getLength() const override {
      return totalLength;
    }

    uint64_t getNaturalReadSize() const override {
      return 128 * 1024;
    }

    void read(void* buf,
              uint64_t length,
              uint64_t offset) override {
      if (!buf) {
        throw ParseError("Buffer is null");
      }
      if (hdfsSeek(fs, file, static_cast<tOffset>(offset))) {
        throw ParseError("Fail to seek " + std::to_string(offset) + " for reading");
      }
      tSize byteRead = hdfsRead(fs, file, buf, length);
      if (byteRead == -1) {
        throw ParseError("Bad read of " + filename);
      }
      if (static_cast<uint64_t>(byteRead) != length) {
        throw ParseError("Short read of " + filename);
      }
    }

    const std::string& getName() const override {
      return filename;
    }
  }; // class HdfsFileInputStream
    
  HdfsFileInputStream::~HdfsFileInputStream() {
    hdfsCloseFile(fs, file);
  }

  std::unique_ptr<InputStream> readHdfsFile(hdfsFS fs,
                                            const std::string& path,
                                            int bufferSize,
                                            int replication,
                                            tOffset blockSize) {
    return std::unique_ptr<InputStream>(new HdfsFileInputStream(fs, path,
                                                                bufferSize,
                                                                replication,
                                                                blockSize));
  }
} // namespace orc

#ifndef HAS_STOLL

  #include <sstream>

  int64_t std::stoll(std::string str) {
    int64_t val = 0;
    stringstream ss ;
    ss << str ;
    ss >> val ;
    return val;
  }

#endif

