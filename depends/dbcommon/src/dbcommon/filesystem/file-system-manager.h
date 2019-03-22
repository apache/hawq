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

#ifndef DBCOMMON_SRC_DBCOMMON_FILESYSTEM_FILE_SYSTEM_MANAGER_H_
#define DBCOMMON_SRC_DBCOMMON_FILESYSTEM_FILE_SYSTEM_MANAGER_H_

#include <mutex>  //NOLINT
#include <string>
#include <unordered_map>

#include "dbcommon/filesystem/file-system.h"

namespace dbcommon {

typedef std::unordered_map<std::string, std::unique_ptr<FileSystem>>
    FileSystemInstanceMap;

typedef std::unordered_map<std::string, std::string> FileSystemCredentialMap;

class FileSystemManagerInterface {
 public:
  FileSystemManagerInterface() {}
  virtual ~FileSystemManagerInterface() {}

  virtual FileSystem* get(const std::string& url) = 0;
  virtual int getNumberOfCachedFileSystem() = 0;
};

class FileSystemManager : public FileSystemManagerInterface {
 public:
  FileSystemManager() {}
  virtual ~FileSystemManager() {}

  typedef std::unique_ptr<FileSystemManager> uptr;

  FileSystem* get(const std::string& url) override;

  // return
  int getNumberOfCachedFileSystem() override { return fsMap.size(); }

  void setTokenMap(const std::string& tokenKey, const std::string& token);
  void setCcname(const std::string& ccname);
  void clearFsMap();
  void clearFsTokenMap();

 private:
  std::unique_ptr<FileSystem> createFileSystem(  // NOLINT
      const std::string& protocol, const std::string& host, int port);
  FileSystemInstanceMap fsMap;
  std::mutex fsMapLock;
  FileSystemCredentialMap fsTokenMap;
  std::string fsCcname;
};

}  // end of namespace dbcommon
#endif  // DBCOMMON_SRC_DBCOMMON_FILESYSTEM_FILE_SYSTEM_MANAGER_H_
