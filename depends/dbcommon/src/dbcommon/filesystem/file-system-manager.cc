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

#include <cassert>

#include "dbcommon/log/logger.h"
#include "dbcommon/utils/url.h"

#include "dbcommon/filesystem/hdfs/hdfs-file-system.h"
#include "dbcommon/filesystem/local/local-file-system.h"

#include "dbcommon/filesystem/file-system-manager.h"

namespace dbcommon {
bool enable_secure_filesystem = 0;

void FileSystemManager::setTokenMap(const std::string& tokenKey,
                                    const std::string& token) {
  URL parser(tokenKey);
  std::string NormarlizedtokenKey = URL::generateNormalizedServiceName(
      parser.getProtocol(), parser.getHost(), parser.getPort());
  fsTokenMap[NormarlizedtokenKey] = token;
}

void FileSystemManager::setCcname(const std::string& ccname) {
  fsCcname = ccname;
}
void FileSystemManager::clearFsMap() { fsMap.clear(); }
void FileSystemManager::clearFsTokenMap() { fsTokenMap.clear(); }
FileSystem* FileSystemManager::get(const std::string& url) {
  FileSystem* ret = nullptr;
  dbcommon::URL parser(url);

  std::string normalizedService = parser.getNormalizedServiceName();

  {
    std::lock_guard<std::mutex> lock(fsMapLock);
    FileSystemInstanceMap::iterator iter = fsMap.find(normalizedService);
    if (iter != fsMap.end()) {
      ret = (iter->second).get();
    }
  }

  // createFileSystem might take some time, esp, when the file system
  // cannot be connected. so move this piece of code out of lock scope.
  // it is possible there are several clients try to create the file system,
  // but it is fine. Only one will be inserted into the map.

  if (ret == nullptr) {
    std::unique_ptr<FileSystem> fs = createFileSystem(
        parser.getProtocol(), parser.getHost(), parser.getPort());

    {
      std::lock_guard<std::mutex> lock(fsMapLock);
      FileSystemInstanceMap::iterator iter = fsMap.find(normalizedService);
      if (iter != fsMap.end()) {
        ret = (iter->second).get();
      } else {
        ret = fs.get();
        fsMap[normalizedService] = std::move(fs);
      }
    }
  }
  assert(ret != nullptr);

  return ret;
}

std::unique_ptr<FileSystem> FileSystemManager::createFileSystem(
    const std::string& protocol, const std::string& host, int port) {
  std::unique_ptr<FileSystem> fs;
  if (protocol == "hdfs") {
    if (port == dbcommon::URL::INVALID_PORT) {
      LOG_ERROR(ERRCODE_INVALID_PARAMETER_VALUE, "port is invalid %d", port);
    }
    std::string NormarlizedtokenKey =
        URL::generateNormalizedServiceName(protocol, host, port);
    fs.reset(new HdfsFileSystem(host.c_str(), port, fsCcname.c_str(),
                                fsTokenMap[NormarlizedtokenKey].c_str()));
  } else if (protocol == "hive") {
    if (port == dbcommon::URL::INVALID_PORT) {
      LOG_ERROR(ERRCODE_INVALID_PARAMETER_VALUE, "port is invalid %d", port);
    }
    fs.reset(new HdfsFileSystem(host.c_str(), port));
  } else if (protocol == "file") {
    fs.reset(new LocalFileSystem());
  } else {
    LOG_ERROR(ERRCODE_INVALID_PARAMETER_VALUE, "protocol %s not supported",
              protocol.c_str());
  }

  fs->connect();
  return std::move(fs);
}

}  // namespace dbcommon
