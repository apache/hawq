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

#include "storage/cwrapper/hdfs-file-system-c.h"

#include <string>
#include <vector>

#include "dbcommon/filesystem/hdfs/hdfs-file-system.h"
#include "dbcommon/log/logger.h"
#include "dbcommon/utils/file-info.h"
#include "dbcommon/utils/global.h"
#include "dbcommon/utils/macro.h"

extern "C" {

struct FscHdfsFileC {
  void *f;
};

struct FscHdfsFileInfoC {
  void *fi;
};

struct FscHdfsFileInfoArrayC {
  int size;
  void **fiVec;
};

struct FscHdfsFileBlockLocationArrayC {
  int size;
  void **fblVec;
};

struct FscHdfsFileBlockLocationC {
  void *bl;
};

struct FscHdfsFileSystemC {
  void *fs;
  CatchedError error;
};

#define FETCH_FILE_SYSTEM_HANDLE(ofs, ifs) \
  dbcommon::FileSystem *ifs = static_cast<dbcommon::FileSystem *>((ofs)->fs);

#define FETCH_HDFS_FILE_SYSTEM_HANDLE(ofs, ifs) \
  dbcommon::HdfsFileSystem *ifs =               \
      static_cast<dbcommon::HdfsFileSystem *>((ofs)->fs);

#define FETCH_FILE_HANDLE(ofile, ifile) \
  dbcommon::File *ifile = static_cast<dbcommon::File *>((ofile)->f);

#define FETCH_FILE_INFO_HANDLE(ofi, ifi) \
  dbcommon::FileInfo *ifi = static_cast<dbcommon::FileInfo *>((ofi)->fi);

#define FETCH_FILE_BLOCK_LOCATION_HANDLE(obl, ibl) \
  dbcommon::FileBlockLocation *ibl =               \
      static_cast<dbcommon::FileBlockLocation *>((obl)->bl);

void FscHdfsSetError(CatchedError *ce, int errCode, const char *reason) {
  assert(ce != nullptr);
  FscHdfsFreeErrorContent(ce); /* free the old one if it was filled already */
  ce->errCode = errCode;
  ce->errMessage = new char[strlen(reason) + 1]();
  strcpy(ce->errMessage, reason); /* NOLINT */
}

void FscHdfsCloseFileC(FscHdfsFileSystemC *fs, FscHdfsFileC *f) {
  int errCode;
  std::string errMessage;
  FETCH_FILE_HANDLE(f, ifile)
  try {
    ifile->close();
  } catch (dbcommon::TransactionAbortException &e) {
    FscHdfsSetError(&(fs->error), e.errCode(), e.what());
  }
}

FscHdfsFileSystemC *FscHdfsNewFileSystem(const char *namenode, uint16_t port) {
  try {
    std::string url("hdfs://");
    url += namenode;
    url += ":" + std::to_string(port);
    dbcommon::FileSystem *fs = FSManager.get(url);

    FscHdfsFileSystemC *result = new FscHdfsFileSystemC();
    result->error.errCode = 0;
    result->error.errMessage = nullptr;
    result->fs = fs;
    return result;
  } catch (dbcommon::TransactionAbortException &e) {
    return nullptr;
  }
}

FscHdfsFileC *FscHdfsOpenFile(FscHdfsFileSystemC *fs, const char *path,
                              int flags) {
  FETCH_FILE_SYSTEM_HANDLE(fs, ifs)
  try {
    std::unique_ptr<dbcommon::File> file = ifs->open(path, flags);
    FscHdfsFileC *result = new FscHdfsFileC();
    result->f = file.release();
    return result;
  } catch (dbcommon::TransactionAbortException &e) {
    FscHdfsSetError(&(fs->error), e.errCode(), e.what());
    return nullptr;
  }
}

void FscHdfsSeekFile(FscHdfsFileSystemC *fs, FscHdfsFileC *f, uint64_t offset) {
  FETCH_FILE_SYSTEM_HANDLE(fs, ifs)
  FETCH_FILE_HANDLE(f, ifile)
  try {
    ifs->seek(ifile, offset);
  } catch (dbcommon::TransactionAbortException &e) {
    FscHdfsSetError(&(fs->error), e.errCode(), e.what());
  }
}

void FscHdfsRemovePath(FscHdfsFileSystemC *fs, const char *path) {
  FETCH_FILE_SYSTEM_HANDLE(fs, ifs)
  try {
    ifs->remove(path);
  } catch (dbcommon::TransactionAbortException &e) {
    FscHdfsSetError(&(fs->error), e.errCode(), e.what());
  }
}

void FscHdfsRemovePathIfExists(FscHdfsFileSystemC *fs, const char *path) {
  FETCH_FILE_SYSTEM_HANDLE(fs, ifs)
  try {
    ifs->removeIfExists(path);
  } catch (dbcommon::TransactionAbortException &e) {
    FscHdfsSetError(&(fs->error), e.errCode(), e.what());
  }
}

FscHdfsFileInfoC *FscHdfsGetFileInfo(FscHdfsFileSystemC *fs,
                                     const char *fileName) {
  FETCH_FILE_SYSTEM_HANDLE(fs, ifs)
  try {
    std::unique_ptr<dbcommon::FileInfo> finfo = ifs->getFileInfo(fileName);
    FscHdfsFileInfoC *result = new FscHdfsFileInfoC();
    result->fi = finfo.release();
    return result;
  } catch (dbcommon::TransactionAbortException &e) {
    FscHdfsSetError(&(fs->error), e.errCode(), e.what());
    return nullptr;
  }
}

int FscHdfsExistPath(FscHdfsFileSystemC *fs, const char *path) {
  FETCH_FILE_SYSTEM_HANDLE(fs, ifs)
  return ifs->exists(path) ? 1 : 0;
}

int64_t FscHdfsGetFileLength(FscHdfsFileSystemC *fs, const char *path) {
  FETCH_FILE_SYSTEM_HANDLE(fs, ifs)
  try {
    return ifs->getFileLength(path);
  } catch (dbcommon::TransactionAbortException &e) {
    FscHdfsSetError(&(fs->error), e.errCode(), e.what());
    return -1;
  }
}

char FscHdfsGetFileKind(FscHdfsFileSystemC *fs, const char *path) {
  FETCH_FILE_SYSTEM_HANDLE(fs, ifs)
  try {
    return ifs->getFileKind(path);
  } catch (dbcommon::TransactionAbortException &e) {
    FscHdfsSetError(&(fs->error), e.errCode(), e.what());
    return 'U';
  }
}

int FscHdfsReadFile(FscHdfsFileSystemC *fs, FscHdfsFileC *f, void *buf,
                    int size) {
  FETCH_FILE_SYSTEM_HANDLE(fs, ifs)
  FETCH_FILE_HANDLE(f, ifile)
  try {
    return ifs->read(ifile, buf, size);
  } catch (dbcommon::TransactionAbortException &e) {
    FscHdfsSetError(&(fs->error), e.errCode(), e.what());
    return -1;
  }
}

void FscHdfsWriteFile(FscHdfsFileSystemC *fs, FscHdfsFileC *f, void *buf,
                      int size) {
  FETCH_FILE_SYSTEM_HANDLE(fs, ifs)
  FETCH_FILE_HANDLE(f, ifile)
  try {
    ifs->write(ifile, buf, size);
  } catch (dbcommon::TransactionAbortException &e) {
    FscHdfsSetError(&(fs->error), e.errCode(), e.what());
  }
}

void FscHdfsCreateDir(FscHdfsFileSystemC *fs, const char *path) {
  FETCH_FILE_SYSTEM_HANDLE(fs, ifs)
  try {
    ifs->createDir(path);
  } catch (dbcommon::TransactionAbortException &e) {
    FscHdfsSetError(&(fs->error), e.errCode(), e.what());
  }
}

int FscHdfsExistInsertPath(FscHdfsFileSystemC *fs, const char *path) {
  FETCH_FILE_SYSTEM_HANDLE(fs, ifs)
  std::string fullPath(path);
  fullPath += INSERT_HIDDEN_DIR;
  return ifs->exists(fullPath.c_str()) ? 1 : 0;
}

void FscHdfsCreateInsertDir(FscHdfsFileSystemC *fs, const char *path) {
  FETCH_FILE_SYSTEM_HANDLE(fs, ifs)
  try {
    std::string fullPath(path);
    fullPath += INSERT_HIDDEN_DIR;
    ifs->createDir(fullPath.c_str());
  } catch (dbcommon::TransactionAbortException &e) {
    FscHdfsSetError(&(fs->error), e.errCode(), e.what());
  }
}

FscHdfsFileInfoArrayC *FscHdfsDirPath(FscHdfsFileSystemC *fs,
                                      const char *path) {
  FETCH_FILE_SYSTEM_HANDLE(fs, ifs)
  try {
    std::vector<std::unique_ptr<dbcommon::FileInfo> > finfovector =
        ifs->dir(path);
    FscHdfsFileInfoArrayC *result = new FscHdfsFileInfoArrayC();
    result->size = finfovector.size();
    result->fiVec = new void *[result->size];
    for (int i = 0; i < result->size; ++i) {
      dbcommon::FileInfo *newfi = new dbcommon::FileInfo();
      FscHdfsFileInfoC *newfic = new FscHdfsFileInfoC();
      newfic->fi = newfi;
      result->fiVec[i] = newfic;

      *newfi = *(finfovector[i]);
    }
    return result;
  } catch (dbcommon::TransactionAbortException &e) {
    FscHdfsSetError(&(fs->error), e.errCode(), e.what());
    return nullptr;
  }
}

void FscHdfsChmodPath(FscHdfsFileSystemC *fs, const char *path, int mode) {
  FETCH_FILE_SYSTEM_HANDLE(fs, ifs)
  try {
    ifs->chmod(path, mode);
  } catch (dbcommon::TransactionAbortException &e) {
    FscHdfsSetError(&(fs->error), e.errCode(), e.what());
  }
}

int64_t FscHdfsTellFile(FscHdfsFileSystemC *fs, FscHdfsFileC *f) {
  FETCH_FILE_SYSTEM_HANDLE(fs, ifs)
  FETCH_FILE_HANDLE(f, ifile)
  try {
    return ifs->tell(ifile);
  } catch (dbcommon::TransactionAbortException &e) {
    FscHdfsSetError(&(fs->error), e.errCode(), e.what());
    return -1;
  }
}

FscHdfsFileBlockLocationArrayC *FscHdfsGetPathFileBlockLocation(
    FscHdfsFileSystemC *fs, const char *path, int64_t start, int64_t length) {
  FETCH_FILE_SYSTEM_HANDLE(fs, ifs)
  try {
    std::vector<std::unique_ptr<dbcommon::FileBlockLocation> >  // NOLINT
        fblvector = ifs->getFileBlockLocation(path, start, length);
    FscHdfsFileBlockLocationArrayC *result =
        new FscHdfsFileBlockLocationArrayC();
    result->size = fblvector.size();
    result->fblVec = new void *[result->size];
    for (int i = 0; i < result->size; ++i) {
      FscHdfsFileBlockLocationC *newblc = new FscHdfsFileBlockLocationC();
      dbcommon::FileBlockLocation *newbl = new dbcommon::FileBlockLocation();
      newblc->bl = newbl;
      result->fblVec[i] = newblc;

      newbl->corrupt = fblvector[i]->corrupt;
      newbl->length = fblvector[i]->length;
      newbl->offset = fblvector[i]->offset;
      newbl->hosts = fblvector[i]->hosts;
      newbl->names = fblvector[i]->names;
      newbl->ports = fblvector[i]->ports;
      newbl->topoPaths = fblvector[i]->topoPaths;
    }
    return result;
  } catch (dbcommon::TransactionAbortException &e) {
    FscHdfsSetError(&(fs->error), e.errCode(), e.what());
    return nullptr;
  }
}

void FscHdfsSetFileSystemBlockSize(FscHdfsFileSystemC *fs, int size) {
  FETCH_FILE_SYSTEM_HANDLE(fs, ifs)
  ifs->setBlockSize(size);
}

int FscHdfsGetFileSystemBlockSize(FscHdfsFileSystemC *fs) {
  FETCH_FILE_SYSTEM_HANDLE(fs, ifs)
  return ifs->getBlockSize();
}

const char *FscHdfsGetFileSystemAddress(FscHdfsFileSystemC *fs) {
  FETCH_HDFS_FILE_SYSTEM_HANDLE(fs, ifs)
  return ifs->getFileSystemNameNodeAddr().c_str();
}

uint16_t FscHdfsGetFileSystemPort(FscHdfsFileSystemC *fs) {
  FETCH_HDFS_FILE_SYSTEM_HANDLE(fs, ifs)
  return ifs->getFileSystemPort();
}

int FscHdfsHasErrorRaised(FscHdfsFileSystemC *fs) {
  FETCH_FILE_SYSTEM_HANDLE(fs, ifs)
  return fs->error.errCode != 0 ? 1 : 0;
}

void FscHdfsFreeFileSystemC(FscHdfsFileSystemC **fs) {
  if (*fs == nullptr) return;
  FscHdfsFreeErrorContent(&((*fs)->error));
  delete *fs;
  *fs = nullptr;
}

void FscHdfsFreeFileC(FscHdfsFileC **f) {
  if (*f == nullptr) return;
  FETCH_FILE_HANDLE((*f), ifile)
  delete ifile;
  delete *f;
  *f = nullptr;
}

void FscHdfsFreeFileInfoArrayC(FscHdfsFileInfoArrayC **fiArray) {
  if (*fiArray == nullptr) return;
  for (int i = 0; i < (*fiArray)->size; ++i) {
    FscHdfsFileInfoC *pfic =
        static_cast<FscHdfsFileInfoC *>((*fiArray)->fiVec[i]);
    dbcommon::FileInfo *pfi = static_cast<dbcommon::FileInfo *>(pfic->fi);
    delete pfi;
    delete pfic;
  }
  delete *fiArray;
  *fiArray = nullptr;
}

void FscHdfsFreeFileBlockLocationArrayC(
    FscHdfsFileBlockLocationArrayC **fblArray) {
  if (*fblArray == nullptr) return;
  for (int i = 0; i < (*fblArray)->size; ++i) {
    FscHdfsFileBlockLocationC *fblc =
        static_cast<FscHdfsFileBlockLocationC *>((*fblArray)->fblVec[i]);
    dbcommon::FileBlockLocation *fbl =
        static_cast<dbcommon::FileBlockLocation *>(fblc->bl);
    delete fbl;
    delete fblc;
  }
  delete *fblArray;
  *fblArray = nullptr;
}

void FscHdfsFreeErrorContent(CatchedError *ce) {
  assert(ce != nullptr);
  if (ce->errMessage != nullptr) {
    delete[] ce->errMessage;
  }
}

FscHdfsFileInfoC *FscHdfsGetFileInfoFromArray(FscHdfsFileInfoArrayC *fia,
                                              int index) {
  if (index < 0 || index >= fia->size) {
    return nullptr;
  }
  return static_cast<FscHdfsFileInfoC *>(fia->fiVec[index]);
}

const char *FscHdfsGetFileInfoName(FscHdfsFileInfoC *fi) {
  FETCH_FILE_INFO_HANDLE(fi, ifi)
  return ifi->name.c_str();
}

int64_t FscHdfsGetFileInfoLength(FscHdfsFileInfoC *fi) {
  FETCH_FILE_INFO_HANDLE(fi, ifi)
  return ifi->size;
}

FscHdfsFileBlockLocationC *FscHdfsGetFileBlockLocationFromArray(
    FscHdfsFileBlockLocationArrayC *bla, int index) {
  if (index < 0 || index >= bla->size) {
    return nullptr;
  }
  return static_cast<FscHdfsFileBlockLocationC *>(bla->fblVec[index]);
}

int FscHdfsGetFileBlockLocationArraySize(FscHdfsFileBlockLocationArrayC *bla) {
  return bla->size;
}

int FscHdfsGetFileBlockLocationNNodes(FscHdfsFileBlockLocationC *bl) {
  FETCH_FILE_BLOCK_LOCATION_HANDLE(bl, ibl)
  return ibl->hosts.size();
}

int64_t FscHdfsGetFileBlockLocationOffset(FscHdfsFileBlockLocationC *bl) {
  FETCH_FILE_BLOCK_LOCATION_HANDLE(bl, ibl)
  return ibl->offset;
}
int64_t FscHdfsGetFileBlockLocationLength(FscHdfsFileBlockLocationC *bl) {
  FETCH_FILE_BLOCK_LOCATION_HANDLE(bl, ibl)
  return ibl->length;
}

int FscHdfsGetFileBlockLocationCorrupt(FscHdfsFileBlockLocationC *bl) {
  FETCH_FILE_BLOCK_LOCATION_HANDLE(bl, ibl)
  return ibl->corrupt;
}

const char *FscHdfsGetFileBlockLocationNodeHost(FscHdfsFileBlockLocationC *bl,
                                                int index) {
  FETCH_FILE_BLOCK_LOCATION_HANDLE(bl, ibl)
  return ibl->hosts[index].c_str();
}
const char *FscHdfsGetFileBlockLocationNodeName(FscHdfsFileBlockLocationC *bl,
                                                int index) {
  FETCH_FILE_BLOCK_LOCATION_HANDLE(bl, ibl)
  return ibl->names[index].c_str();
}
const char *FscHdfsGetFileBlockLocationNodeTopoPath(
    FscHdfsFileBlockLocationC *bl, int index) {
  FETCH_FILE_BLOCK_LOCATION_HANDLE(bl, ibl)
  return ibl->topoPaths[index].c_str();
}

void FscHdfsFreeString(char **pstr) {
  delete[] * pstr;
  *pstr = nullptr;
}

CatchedError *FscHdfsGetFileSystemError(FscHdfsFileSystemC *fs) {
  return &(fs->error);
}

void SetToken(const char *tokenkey, const char *token) {
  if (token) {
    std::string Token(token);
    std::string TokenKey(tokenkey);
    FSManager.setTokenMap(TokenKey, Token);
  }
}

void SetCcname(const char *ccname) {
  if (ccname) {
    std::string Ccname(ccname);
    FSManager.setCcname(Ccname);
  }
}
void cleanup_FSManager() {
  FSManager.clearFsMap();
  FSManager.clearFsTokenMap();
}
}
