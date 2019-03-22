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

#include <ftw.h>

#include <algorithm>
#include <cassert>
#include <cerrno>
#include <string>

#include "dbcommon/filesystem/local/local-file-system.h"

namespace dbcommon {

#define IS_DIR_SEP(ch) ((ch) == '/')

#define is_absolute_path(filename) (IS_DIR_SEP((filename).at(0)))

Lock *LocalFilesCache::getFilesCacheLock() { return cacheLock.get(); }

LocalFileInCache *LocalFilesCache::findFile(uint64_t fileNum, bool refresh) {
  std::unordered_map<uint64_t, dbcommon::LocalFileInCache>::iterator fileIter =
      filesCache.find(fileNum);
  if (fileIter != filesCache.end()) {
    fileIter->second.access();
    if (refresh) {
      auto fqIt = std::find(filesQueue.begin(), filesQueue.end(), fileNum);
      filesQueue.erase(fqIt);
      filesQueue.push_front(fileNum);
    }
    return &fileIter->second;
  } else {
    return nullptr;
  }
}

void LocalFilesCache::putFile(uint64_t fileNum, std::unique_ptr<File> file,
                              dbcommon::Lock *lock) {
  if (filesCache.size() == MAXFILENUM) {
    uint64_t rmFileNum = filesQueue.back();
    filesCache.erase(rmFileNum);
    filesQueue.pop_back();
  }
  dbcommon::LocalFileInCache fileCache(std::move(file), lock);
  filesCache[fileNum] = std::move(fileCache);
  filesQueue.push_front(fileNum);
}

void LocalFilesCache::cleanUpFile() {
  LockGuard cacheLockGuard(cacheLock.get(), dbcommon::EXCLUSIVELOCK);
  uint64_t now = std::chrono::duration_cast<std::chrono::milliseconds>(
                     std::chrono::system_clock::now().time_since_epoch())
                     .count();
  while (filesQueue.size() > 0) {
    uint64_t fileNum = filesQueue.back();
    std::unordered_map<uint64_t, dbcommon::LocalFileInCache>::iterator
        fileIter = filesCache.find(fileNum);
    assert(fileIter != filesCache.end());
    if (now - fileIter->second.getLastAccessTime() >= OPENFILETIMEOUT) {
      filesCache.erase(fileNum);
      filesQueue.pop_back();
    } else {
      break;
    }
  }
}

OSDir::OSDir(const std::string &path) {
  dir = opendir(path.c_str());
  if (dir == NULL) {
    LOG_ERROR(ERRCODE_IO_ERROR, "cannot open directory: %s, errno: %d",
              path.c_str(), errno);
  }
}

OSDir::~OSDir() {
  if (dir) {
    closedir(dir);
  }
}

std::unique_ptr<File> LocalFileSystem::open(const std::string &path, int flag) {
  if (path.empty() || !is_absolute_path(path)) {
    LOG_ERROR(ERRCODE_INVALID_PARAMETER_VALUE,
              "only absolute path is supported, the path is: %s", path.c_str());
  }

  if (flag != O_WRONLY && flag != O_CREAT && flag != O_RDONLY &&
      flag != (O_WRONLY | O_SYNC) && flag != O_RDWR) {
    LOG_ERROR(ERRCODE_INVALID_PARAMETER_VALUE,
              "open call failed, invalid flags: %d, path: %s. "
              "The only supported flag combinations are "
              "1) O_RDONLY 2) O_WRONLY 3) O_WRONLY|O_SYNC 4) O_CREAT",
              flag, path.c_str());
  }

  if (flag & O_RDWR) {
    flag |= O_CREAT;
  }

  if (flag & O_WRONLY) {
    flag |= O_CREAT;
    flag |= O_TRUNC;
  }

  if (flag == O_CREAT) {
    flag |= O_WRONLY;
    flag |= O_EXCL;
  }

  int handle = ::open(path.c_str(), flag, 0666);
  if (handle < 0) {
    LOG_ERROR(ERRCODE_IO_ERROR, "open OS API call failed, path:%s, errno: %d",
              path.c_str(), errno);
  }

  // Intentionally don't use try/catch
  // need to verify try/catch cost
  LocalFile lc(handle, flag);
  std::unique_ptr<File> ret(new LocalFile(handle, flag));
  ret->setFileName(path);
  lc.setHandle(INVALID_LOCAL_FILE_HANDLE);

  return std::move(ret);
}

void LocalFileSystem::seek(File *file, uint64_t offset) {
  LocalFile *lf = static_cast<LocalFile *>(file);

/*
    Unlock seeking on one open file for writing in local file system, as some
    callers need this function. Keep this logic commented for reference.

if (lf->openForWrite())
  LOG_ERROR(ERRCODE_DATA_EXCEPTION, "cannot seek on 'open for write' file");
*/
#ifdef __APPLE__
  int64_t ret = lseek(lf->getHandle(), offset, SEEK_SET);
#else
  int64_t ret = lseek64(lf->getHandle(), offset, SEEK_SET);
#endif

  if (ret < 0) {
    LOG_ERROR(ERRCODE_IO_ERROR,
              "seek OS API call failed, path: %s, errno: %d, offset %llu",
              file->fileName().c_str(), errno, offset);
  }
}

static int rm(const char *fpath, const struct stat *sb, int tflag,
              struct FTW *ftwbuf) {
#ifdef __APPLE__
#define FTW_STOP 1
#endif

  if (sb->st_mode & S_IFDIR) {
    if (rmdir(fpath) < 0) {
      LOG_WARNING("rmdir failed, path: %s, errno: %d", fpath, errno);
      return FTW_STOP;
    }
  } else {
    if (unlink(fpath) < 0) {
      LOG_WARNING("unlink failed, path: %s, errno: %d", fpath, errno);
      return FTW_STOP;
    }
  }

  return 0;
}

void LocalFileSystem::remove(const std::string &path) {
  int ret = ::remove(path.c_str());
  if (ret == 0) return;

  if (errno == ENOTEMPTY) {
    int flags = 0;
    flags |= (FTW_DEPTH | FTW_PHYS);

    if (nftw(path.c_str(), rm, 20, flags) == -1) {
      LOG_ERROR(ERRCODE_IO_ERROR, "remove (nftw) failed, path: %s, errno: %d",
                path.c_str(), errno);
    }
  } else {
    LOG_ERROR(ERRCODE_IO_ERROR, "remove failed, path: %s, errno: %d",
              path.c_str(), errno);
  }
}

std::unique_ptr<dbcommon::FileInfo> LocalFileSystem::getFileInfo(
    const std::string &fileName) {
  struct stat st;
  int ret;

  ret = stat(fileName.c_str(), &st);
  if (ret < 0) {
    LOG_ERROR(ERRCODE_IO_ERROR,
              "get file status failed, file name: %s, errno:%d",
              fileName.c_str(), ret);
  }

  std::unique_ptr<dbcommon::FileInfo> fi(new dbcommon::FileInfo());

  fi->size = st.st_size;
  fi->name = fileName;
  fi->isDir = (st.st_mode & S_IFDIR);

  /* TODO: set the access time and modify time */
  fi->accessTime = 0;
  fi->modifyTime = 0;
#if defined(__linux__)
  fi->modifyTime = st.st_mtim.tv_sec * 1000000 + st.st_mtim.tv_nsec / 1000;
#elif defined(__APPLE__)
  fi->modifyTime =
      st.st_mtimespec.tv_sec * 1000000 + st.st_mtimespec.tv_nsec / 1000;
#endif

  return std::move(fi);
}

bool LocalFileSystem::exists(const std::string &path) {
  struct stat st;
  int ret;

  ret = stat(path.c_str(), &st);
  if (ret < 0 && errno == ENOENT) {
    if (errno == ENOENT) {
      return false;
    } else {
      LOG_ERROR(ERRCODE_IO_ERROR,
                "get file status failed, file name: %s, errno:%d", path.c_str(),
                ret);
    }
  }

  return true;
}

int64_t LocalFileSystem::getFileLength(const std::string &fileName) {
  std::unique_ptr<dbcommon::FileInfo> fi =
      LocalFileSystem::getFileInfo(fileName);
  return fi->size;
}

char LocalFileSystem::getFileKind(const std::string &fileName) {
  std::unique_ptr<dbcommon::FileInfo> fi =
      LocalFileSystem::getFileInfo(fileName);
  if (fi->isDir)
    return 'D';
  else
    return 'F';
}

// if return value < size,  means EOF.
// otherwise return value == size;
int LocalFileSystem::read(File *file, void *buf, int size) {
  assert(size > 0 && buf != NULL && file != NULL);

  LocalFile *lf = static_cast<LocalFile *>(file);
  int nRead = 0;

retry:
  int ret =
      ::read(lf->getHandle(), static_cast<char *>(buf) + nRead, size - nRead);
  if (ret > 0) {
    nRead += ret;
    if (nRead < size) goto retry;
  } else if (ret < 0) {
    /* OK to retry if interrupted */
    if (errno == EINTR) goto retry;
    LOG_ERROR(ERRCODE_IO_ERROR, "read OS API call failed, path: %s, errno: %d",
              file->fileName().c_str(), errno);
  }

  return nRead;
}

void LocalFileSystem::write(File *file, const void *buf, int size) {
  assert(size > 0 && buf != NULL && file != NULL);

  LocalFile *lf = static_cast<LocalFile *>(file);
  int nWrite = 0;

retry:
  int ret = ::write(lf->getHandle(), static_cast<const char *>(buf) + nWrite,
                    size - nWrite);
  if (ret >= 0) {
    nWrite += ret;
    if (nWrite < size) goto retry;
  } else {
    if (errno == EINTR) goto retry;
    LOG_ERROR(ERRCODE_IO_ERROR, "write OS API call failed, path: %s, errno: %d",
              file->fileName().c_str(), errno);
  }
}

void LocalFileSystem::createDir(const std::string &path) {
  int pre = 0;
  int pos = 0;
  int ret;

  std::string dir(path);
  std::string subDir;

  int mode = 0755;

  if (dir[dir.size() - 1] != '/') {
    // force trailing / so we can handle everything in loop
    dir.append("/");
  }

  while ((pos = dir.find_first_of('/', pre)) != std::string::npos) {
    subDir = dir.substr(0, pos++);
    pre = pos;
    if (subDir.size() == 0) continue;

    if ((ret = mkdir(subDir.c_str(), mode)) && errno != EEXIST) {
      LOG_ERROR(ERRCODE_IO_ERROR, "mkdir failed, path: %s, errno: %d",
                path.c_str(), ret);
    }
  }
}

std::vector<std::unique_ptr<dbcommon::FileInfo> > LocalFileSystem::dir(
    const std::string &path) {
  OSDir directory(path);

  std::vector<std::unique_ptr<dbcommon::FileInfo> > entries;
  errno = 0;
  while (true) {
    struct dirent *entry = NULL;
    entry = readdir(directory.getDir());  // no need to free entry
    if (entry == NULL) break;

    if (strcmp(entry->d_name, ".") == 0 || strcmp(entry->d_name, "..") == 0)
      continue;

    std::string filePath(path);
    filePath.append("/");
    filePath.append(entry->d_name);
    std::unique_ptr<dbcommon::FileInfo> p(
        LocalFileSystem::getFileInfo(filePath.c_str()));
    p->name = entry->d_name;
    entries.push_back(std::move(p));
  }

  if (errno != 0) {
    LOG_ERROR(ERRCODE_IO_ERROR, "read directory failed, path: %s, errno: %d",
              path.c_str(), errno);
  }

  return std::move(entries);
}

void LocalFileSystem::chmod(const std::string &path, int mode) {
  LOG_ERROR(ERRCODE_FEATURE_NOT_SUPPORTED, "chmod not implemented yet.");
}

int64_t LocalFileSystem::tell(File *file) {
  LocalFile *lf = static_cast<LocalFile *>(file);

#ifdef __APPLE__
  int64_t ret = lseek(lf->getHandle(), 0, SEEK_CUR);
#else
  int64_t ret = lseek64(lf->getHandle(), 0, SEEK_CUR);
#endif

  if (ret < 0) {
    LOG_ERROR(ERRCODE_IO_ERROR, "seek OS API call failed, path: %s, errno: %d",
              file->fileName().c_str(), errno);
  }
  return ret;
}

void LocalFileSystem::rename(const std::string &oldPath,
                             const std::string &newPath) {
  int ret = ::rename(oldPath.c_str(), newPath.c_str());
  if (ret == 0) return;

  LOG_ERROR(ERRCODE_IO_ERROR,
            "rename failed from oldpath %s to newpath %s, errno: %d",
            oldPath.c_str(), newPath.c_str(), errno);
}

std::vector<std::unique_ptr<FileBlockLocation> >
LocalFileSystem::getFileBlockLocation(const std::string &path, int64_t start,
                                      int64_t length) {
  std::vector<std::unique_ptr<FileBlockLocation> > entries;

  std::unique_ptr<dbcommon::FileInfo> fi = LocalFileSystem::getFileInfo(path);
  assert(!fi->isDir);
  assert(start >= 0 && start <= fi->size);
  assert(length >= 0 && start + length <= fi->size);

  // TODO(zhenglin): hack here
  char hostName[1024] = "localhost";
  // int64_t ret = gethostname(hostName, 1024);
  //  if (ret < 0) {
  //    LOG_ERROR(ERRCODE_IO_ERROR, "gethostname OS API call failed, errno: %d",
  //              errno);
  //  }

  while (length > 0) {
    std::unique_ptr<FileBlockLocation> location(new FileBlockLocation());
    location->offset = start;
    location->length = (length < blockSize) ? length : blockSize;
    location->hosts.push_back(hostName);
    start = location->offset + location->length;
    length -= location->length;
    entries.push_back(std::move(location));
  }

  return std::move(entries);
}

void LocalFileSystem::truncate(File *file, uint64_t offset) {
  LocalFile *lf = static_cast<LocalFile *>(file);

  int64_t ret = ftruncate(lf->getHandle(), offset);
  if (ret < 0) {
    LOG_ERROR(ERRCODE_IO_ERROR,
              "truncate OS API call failed, path: %s, errno: %d",
              file->fileName().c_str(), errno);
  }
  return;
}

void LocalFileSystem::preallocate(File *file, uint64_t size) {
  LocalFile *lf = static_cast<LocalFile *>(file);
#ifdef __APPLE__
  fstore_t store = {F_ALLOCATECONTIG, F_PEOFPOSMODE, 0, (off_t)size};
  int ret = fcntl(lf->getHandle(), F_PREALLOCATE, &store);
  if (ret < 0) {
    LOG_ERROR(ERRCODE_IO_ERROR,
              "preallocate OS API call failed, path: %s, errno: %d",
              file->fileName().c_str(), errno);
  }

  ret = ftruncate(lf->getHandle(), size);
  if (ret < 0) {
    LOG_ERROR(ERRCODE_IO_ERROR,
              "preallocate OS API call failed, path: %s, errno: %d",
              file->fileName().c_str(), errno);
  }
#elif defined(HAVE_POSIX_FALLOCATE)
  int64_t ret = posix_fallocate(lf->getHandle(), 0, SEEK_CUR);
  if (ret < 0) {
    LOG_ERROR(ERRCODE_IO_ERROR,
              "posix_fallocate OS API call failed, path: %s, errno: %d",
              file->fileName().c_str(), errno);
  }
#else
  int64_t ret = ftruncate(lf->getHandle(), size);
  if (ret < 0) {
    LOG_ERROR(ERRCODE_IO_ERROR,
              "ftruncate OS API call failed, path: %s, errno: %d",
              file->fileName().c_str(), errno);
  }
#endif

  return;
}
void LocalFileSystem::fdatasync(File *file) {
  LocalFile *lf = static_cast<LocalFile *>(file);
#ifdef __APPLE__
  int ret = fcntl(lf->getHandle(), F_FULLFSYNC);
  if (ret < 0) {
    LOG_ERROR(ERRCODE_IO_ERROR,
              "fcntl F_FULLFSYNC OS API call failed, path: %s, errno: %d",
              file->fileName().c_str(), errno);
  }
#else
  int ret = ::fdatasync((lf->getHandle()));
  if (ret < 0) {
    LOG_ERROR(ERRCODE_IO_ERROR,
              "fdatasync OS API call failed, path: %s, errno: %d",
              file->fileName().c_str(), errno);
  }
#endif

  return;
}

}  // namespace dbcommon
