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
#include <iostream>
#include <string>

#include "orc/ColumnPrinter.hh"

#include "Exceptions.hh"

#include "wrap/libhdfs3-wrapper.h"

int main(int argc, char* argv[]) {
  if (argc < 2) {
    std::cout << "Usage: hdfs-orc-scan <filename> [--batch=rows_in_batch]\n"
              << "If batch is specified, only this number of rows to read.\n";
    return 1;
  }

  orc::ReaderOptions opts;
  std::unique_ptr<orc::Reader> reader;
  try{
    auto addr = std::getenv("LIBHDFS3_NAMENODE_ADDRESS");
    std::string nn = "localhost";
    if(addr != NULL) {
      nn = std::string(addr);
    }
    auto portStr = std::getenv("LIBHDFS3_NAMENODE_PORT");
    tPort port = 8020;
    if (portStr != NULL) {
      port = static_cast<tPort>(atoi(portStr));
    }
    hdfsFS fs = hdfsConnect(nn.c_str(), port);
    reader = orc::createReader(orc::readHdfsFile(fs, std::string(argv[1])), opts);
  } catch (std::exception& ex) {
    std::cerr << "Caught exception: " << ex.what() << "\n";
    return 1;
  }

  const std::string BATCH_PREFIX = "--batch=";
  uint64_t batchSize = 1000;
  char *param;
  if (argc == 3) {
    if ( (param=strstr(argv[2], BATCH_PREFIX.c_str())) ) {
      batchSize = static_cast<uint64_t>(std::atoi(param+BATCH_PREFIX.length()));
    } else {
      std::cout << "Usage: hdfs-orc-scan <filename> [--batch=rows_in_batch]\n"
                << "If batch is specified, only this number of rows to read.\n";
      return 1;
    }
  }

  std::unique_ptr<orc::ColumnVectorBatch> batch = reader->createRowBatch(batchSize);
  unsigned long rows = 0;
  unsigned long batches = 0;
  while (reader->next(*batch)) {
    batches += 1;
    rows += batch->numElements;
  }
  std::cout << "Rows: " << rows << std::endl;
  std::cout << "Batches: " << batches << std::endl;
  return 0;
}
