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

#include <fcntl.h>

#include <cerrno>

#include "dbcommon/filesystem/hdfs/hdfs-file-system.h"
#include "dbcommon/log/logger.h"

namespace dbcommon {

void HdfsFileSystem::connect() {
  struct hdfsBuilder *builder = hdfsNewBuilder();
  hdfsBuilderSetNameNode(builder, fsNameNode.c_str());
  if (fsPort != 0) hdfsBuilderSetNameNodePort(builder, fsPort);
  if (!fsToken.empty()) hdfsBuilderSetToken(builder, fsToken.c_str());
  if (!fsCcname.empty())
    hdfsBuilderSetKerbTicketCachePath(builder, fsCcname.c_str());
  hdfsBuilderSetForceNewInstance(builder);
  fsHandle = hdfsBuilderConnect(builder);
  hdfsFreeBuilder(builder);
  if (fsHandle == nullptr) {
    LOG_NOT_RETRY_ERROR(ERRCODE_CONNECTION_FAILURE,
                        "connect to hdfs://%s:%d error", fsNameNode.c_str(),
                        fsPort);
  }
}

void HdfsFileSystem::disconnect() {
  if (fsHandle != INVALID_FILESYSTEM_HANDLE) {
    hdfsDisconnect(fsHandle);
    fsHandle = INVALID_FILESYSTEM_HANDLE;
  }
}

std::unique_ptr<File> HdfsFileSystem::open(const std::string &path, int flag) {
  if (flag != O_WRONLY && flag != O_CREAT && flag != O_RDONLY &&
      flag != (O_WRONLY | O_SYNC)) {
    LOG_ERROR(ERRCODE_INVALID_PARAMETER_VALUE,
              "open call failed, invalid flags: %d, path: %s. "
              "The only supported flag combinations are "
              "1) O_RDONLY 2) O_WRONLY 3) O_WRONLY|O_SYNC 4) O_CREAT",
              flag, path.c_str());
  }

  hdfsFile handle = hdfsOpenFile(fsHandle, path.c_str(), flag, fsBufferSize,
                                 fsBlockReplication, fsBlockSize);
  if (handle == INVALID_HDFS_FILE_HANDLE) {
    LOG_ERROR(ERRCODE_IO_ERROR, "open HDFS API call failed, path:%s, error: %s",
              path.c_str(), hdfsGetLastError());
  }

  std::unique_ptr<File> ret(new HdfsFile(fsHandle, handle));
  ret->setFileName(path);

  return std::move(ret);
}

void HdfsFileSystem::seek(File *file, uint64_t offset) {
  HdfsFile *hdfsfile = static_cast<HdfsFile *>(file);
  int ret = hdfsSeek(fsHandle, hdfsfile->getFileHandle(), offset);
  if (ret < 0) {
    LOG_ERROR(ERRCODE_IO_ERROR, "seek HDFS API call failed.");
  }
}

void HdfsFileSystem::remove(const std::string &path) {
  int ret = hdfsDelete(fsHandle, path.c_str(), 1 /* do recursive remove */);
  if (ret < 0) {
    LOG_ERROR(ERRCODE_IO_ERROR, "remove HDFS API call failed.");
  }
}

std::unique_ptr<dbcommon::FileInfo> HdfsFileSystem::getFileInfo(
    const std::string &fileName) {
  hdfsFileInfo *fileinfo = hdfsGetPathInfo(fsHandle, fileName.c_str());
  if (fileinfo == NULL) {
    LOG_ERROR(ERRCODE_IO_ERROR,
              "get file status HDFS API failed, file name: %s",
              fileName.c_str());
  }

  std::unique_ptr<dbcommon::FileInfo> fi(new dbcommon::FileInfo());

  fi->size = fileinfo->mSize;
  fi->name = fileName;
  fi->isDir = fileinfo->mKind == kObjectKindDirectory;
  fi->accessTime = fileinfo->mLastAccess;
  fi->modifyTime = fileinfo->mLastMod;

  hdfsFreeFileInfo(fileinfo, 1 /* only one file info instance to free */);

  return std::move(fi);
}

bool HdfsFileSystem::exists(const std::string &path) {
  // hfdsExists interface looks has some issues
  // if there are some hdfs issues, it cannot
  // know whether the file exists, it still return -1.
  // so it is strange.
  int ret = hdfsExists(fsHandle, path.c_str());
  if (ret == 0)
    return true;
  else
    return false;
}

int64_t HdfsFileSystem::getFileLength(const std::string &fileName) {
  hdfsFileInfo *fileinfo = hdfsGetPathInfo(fsHandle, fileName.c_str());
  if (fileinfo == NULL) {
    LOG_ERROR(ERRCODE_IO_ERROR,
              "get file status HDFS API failed, file name: %s, errno: %d",
              fileName.c_str(), errno);
  }
  int64_t ret = fileinfo->mSize;
  hdfsFreeFileInfo(fileinfo, 1 /* only one file info instance to free */);
  return ret;
}

char HdfsFileSystem::getFileKind(const std::string &fileName) {
  hdfsFileInfo *fileinfo = hdfsGetPathInfo(fsHandle, fileName.c_str());
  if (fileinfo == NULL) {
    LOG_ERROR(ERRCODE_IO_ERROR,
              "get file status HDFS API failed, file name: %s, errno: %d",
              fileName.c_str(), errno);
  }
  tObjectKind ret = fileinfo->mKind;
  hdfsFreeFileInfo(fileinfo, 1 /* only one file info instance to free */);
  return ret;
}

int HdfsFileSystem::read(File *file, void *buf, int size) {
  HdfsFile *hdfsfile = static_cast<HdfsFile *>(file);
  int nRead = 0;
retry:
  int ret = hdfsRead(fsHandle, hdfsfile->getFileHandle(),
                     static_cast<char *>(buf) + nRead, size - nRead);
  if (ret > 0) {
    nRead += ret;
    if (nRead < size) goto retry;
  } else if (ret < 0) {
    // OK to retry if interrupted
    if (errno == EINTR) goto retry;
    LOG_ERROR(ERRCODE_IO_ERROR,
              "read HDFS API call failed, path: %s, errno: %d",
              file->fileName().c_str(), errno);
  } else {  // nRead = 0, EOF
  }
  return nRead;
}

void HdfsFileSystem::write(File *file, const void *buf, int size) {
  HdfsFile *hdfsfile = static_cast<HdfsFile *>(file);
  int nWrite = 0;
retry:
  int ret = hdfsWrite(fsHandle, hdfsfile->getFileHandle(),
                      static_cast<const char *>(buf) + nWrite, size - nWrite);
  if (ret >= 0) {
    nWrite += ret;
    if (nWrite < size) goto retry;
  } else {
    LOG_ERROR(ERRCODE_IO_ERROR,
              "write HDFS API call failed, path: %s, error: %s",
              file->fileName().c_str(), hdfsGetLastError());
  }
}
void HdfsFileSystem::createDir(const std::string &path) {
  int ret = hdfsCreateDirectory(fsHandle, path.c_str());
  if (ret < 0) {
    LOG_ERROR(ERRCODE_IO_ERROR,
              "create dir HDFS API call failed, path:%s, errno: %d",
              path.c_str(), errno);
  }
}

std::vector<std::unique_ptr<dbcommon::FileInfo> > HdfsFileSystem::dir(
    const std::string &path) {
  HdfsDir hDir(fsHandle, path);
  size_t dirLen = path.length();

  std::vector<std::unique_ptr<dbcommon::FileInfo> > entries;
  hdfsFileInfo *array = hDir.getDir();
  for (int index = 0; index < hDir.getNumEntries(); ++index) {
    std::unique_ptr<dbcommon::FileInfo> p(new dbcommon::FileInfo());
    p->size = array[index].mSize;
    if (array[index].mName) {
      p->name = array[index].mName + dirLen + 1;
    } else {
      p->name = array[index].mName;
    }
    p->isDir = array[index].mKind == kObjectKindDirectory;
    p->accessTime = array[index].mLastAccess;
    p->modifyTime = array[index].mLastMod;
    entries.push_back(std::move(p));
  }

  return std::move(entries);
}

void HdfsFileSystem::chmod(const std::string &path, int mode) {
  int ret = hdfsChmod(fsHandle, path.c_str(), mode);
  if (ret < 0) {
    LOG_ERROR(ERRCODE_IO_ERROR, "error occurred for chmod, path: %s, errno: %d",
              path.c_str(), errno);
  }
}

int64_t HdfsFileSystem::tell(File *file) {
  HdfsFile *hdfsfile = static_cast<HdfsFile *>(file);
  int64_t ret = hdfsTell(fsHandle, hdfsfile->getFileHandle());
  if (ret < 0) {
    LOG_ERROR(ERRCODE_IO_ERROR,
              "hdfsTell HDFS API call failed. path: %s, errno: %d",
              file->fileName().c_str(), errno);
  }
  return ret;
}

void HdfsFileSystem::rename(const std::string &oldPath,
                            const std::string &newPath) {
  int ret = hdfsRename(fsHandle, oldPath.c_str(), newPath.c_str());
  if (ret < 0) {
    LOG_ERROR(ERRCODE_IO_ERROR,
              "hdfsRename HDFS API call failed. from oldpath %s to newpath %s, "
              "errno: %d",
              oldPath.c_str(), newPath.c_str(), errno);
  }
}

std::vector<std::unique_ptr<FileBlockLocation> >
HdfsFileSystem::getFileBlockLocation(const std::string &path, int64_t start,
                                     int64_t length) {
  int numOfBlock;
  std::vector<std::unique_ptr<FileBlockLocation> > entries;
  BlockLocation *locations = hdfsGetFileBlockLocations(
      fsHandle, path.c_str(), start, length, &numOfBlock);
  if (locations == nullptr) {
    LOG_ERROR(ERRCODE_IO_ERROR,
              "hdfsGetFileBlockLocations API failed, file name: %s",
              path.c_str());
  }

  for (int i = 0; i < numOfBlock; ++i) {
    std::unique_ptr<FileBlockLocation> location(  // NOLINT
        new FileBlockLocation());
    location->length = locations[i].length;
    location->offset = locations[i].offset;
    location->corrupt = locations[i].corrupt;
    for (int j = 0; j < locations[i].numOfNodes; ++j) {
      location->hosts.push_back(locations[i].hosts[j]);
      location->names.push_back(locations[i].names[j]);
      location->topoPaths.push_back(locations[i].topologyPaths[j]);
    }
    entries.push_back(std::move(location));
  }

  hdfsFreeFileBlockLocations(locations, numOfBlock);
  return std::move(entries);  // NOLINT
}
void HdfsFileSystem::setBlockSize(int size) { fsBlockSize = size; }
int HdfsFileSystem::getBlockSize() { return fsBlockSize; }
}  // namespace dbcommon
