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

#ifndef STORAGE_SRC_STORAGE_CWRAPPER_HDFS_FILE_SYSTEM_C_H_
#define STORAGE_SRC_STORAGE_CWRAPPER_HDFS_FILE_SYSTEM_C_H_

#include <fcntl.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

struct FscHdfsFileC;
struct FscHdfsFileInfoC;
struct FscHdfsFileInfoArrayC;
struct FscHdfsFileBlockLocationC;
struct FscHdfsFileBlockLocationArrayC;
struct FscHdfsFileSystemC;

typedef struct FscHdfsFileC FscHdfsFileC;
typedef struct FscHdfsFileInfoC FscHdfsFileInfoC;
typedef struct FscHdfsFileInfoArrayC FscHdfsFileInfoArrayC;
typedef struct FscHdfsFileBlockLocationC FscHdfsFileBlockLocationC;
typedef struct FscHdfsFileBlockLocationArrayC FscHdfsFileBlockLocationArrayC;
typedef struct FscHdfsFileSystemC FscHdfsFileSystemC;

typedef struct CatchedError {
  int errCode;
  char *errMessage;
} CatchedError;

// Set error
__attribute__((weak)) void FscHdfsSetError(CatchedError *ce, int errCode, const char *reason) {}
__attribute__((weak)) CatchedError *FscHdfsGetFileSystemError(FscHdfsFileSystemC *fs) {}

// File APIs
__attribute__((weak)) void FscHdfsCloseFileC(FscHdfsFileSystemC *fs, FscHdfsFileC *f) {}

// File system APIs
__attribute__((weak)) FscHdfsFileSystemC *FscHdfsNewFileSystem(const char *namenode, uint16_t port) {}

__attribute__((weak)) FscHdfsFileC *FscHdfsOpenFile(FscHdfsFileSystemC *fs, const char *path,
                              int flags) {}
__attribute__((weak)) void FscHdfsSeekFile(FscHdfsFileSystemC *fs, FscHdfsFileC *f, uint64_t offset) {}
__attribute__((weak)) void FscHdfsRemovePath(FscHdfsFileSystemC *fs, const char *path) {}
__attribute__((weak)) void FscHdfsRemovePathIfExists(FscHdfsFileSystemC *fs, const char *path) {}
__attribute__((weak)) FscHdfsFileInfoC *FscHdfsGetFileInfo(FscHdfsFileSystemC *fs,
                                     const char *fileName) {}
__attribute__((weak)) int FscHdfsExistPath(FscHdfsFileSystemC *fs, const char *path) {}
__attribute__((weak)) int64_t FscHdfsGetFileLength(FscHdfsFileSystemC *fs, const char *path) {}
__attribute__((weak)) int FscHdfsReadFile(FscHdfsFileSystemC *fs, FscHdfsFileC *f, void *buf,
                    int size) {}
__attribute__((weak)) void FscHdfsWriteFile(FscHdfsFileSystemC *fs, FscHdfsFileC *f, void *buf,
                      int size) {}
__attribute__((weak)) void FscHdfsCreateDir(FscHdfsFileSystemC *fs, const char *path) {}
// this is a special interface to create dir with a hidden ".tmp" subdir
__attribute__((weak)) int FscHdfsExistInsertPath(FscHdfsFileSystemC *fs, const char *path) {}
__attribute__((weak)) void FscHdfsCreateInsertDir(FscHdfsFileSystemC *fs, const char *path) {}
__attribute__((weak)) void FscHdfsRenamePath(FscHdfsFileSystemC *fs, const char *oldpath,
                       const char *newpath) {}
__attribute__((weak)) FscHdfsFileInfoArrayC *FscHdfsDirPath(FscHdfsFileSystemC *fs, const char *path) {}
__attribute__((weak)) void FscHdfsChmodPath(FscHdfsFileSystemC *fs, const char *path, int mode) {}
__attribute__((weak)) int64_t FscHdfsTellFile(FscHdfsFileSystemC *fs, FscHdfsFileC *f) {}
__attribute__((weak)) FscHdfsFileBlockLocationArrayC *FscHdfsGetPathFileBlockLocation(
    FscHdfsFileSystemC *fs, const char *path, int64_t start, int64_t length) {}
__attribute__((weak)) void FscHdfsSetFileSystemBlockSize(FscHdfsFileSystemC *fs, int size) {}
__attribute__((weak)) int FscHdfsGetFileSystemBlockSize(FscHdfsFileSystemC *fs) {}

__attribute__((weak)) const char *FscHdfsGetFileSystemAddress(FscHdfsFileSystemC *fs) {}
__attribute__((weak)) uint16_t FscHdfsGetFileSystemPort(FscHdfsFileSystemC *fs) {}

__attribute__((weak)) FscHdfsFileInfoC *FscHdfsGetFileInfoFromArray(FscHdfsFileInfoArrayC *fia,
                                              int index) {}
__attribute__((weak)) const char *FscHdfsGetFileInfoName(FscHdfsFileInfoC *fi) {}
__attribute__((weak)) int64_t FscHdfsGetFileInfoLength(FscHdfsFileInfoC *fi) {}

__attribute__((weak)) FscHdfsFileBlockLocationC *FscHdfsGetFileBlockLocationFromArray(
    FscHdfsFileBlockLocationArrayC *bla, int index) {}

__attribute__((weak)) int FscHdfsGetFileBlockLocationArraySize(FscHdfsFileBlockLocationArrayC *bla) {}

__attribute__((weak)) int FscHdfsGetFileBlockLocationNNodes(FscHdfsFileBlockLocationC *bl) {}
__attribute__((weak)) int64_t FscHdfsGetFileBlockLocationOffset(FscHdfsFileBlockLocationC *bl) {}
__attribute__((weak)) int64_t FscHdfsGetFileBlockLocationLength(FscHdfsFileBlockLocationC *bl) {}
__attribute__((weak)) int FscHdfsGetFileBlockLocationCorrupt(FscHdfsFileBlockLocationC *bl) {}
__attribute__((weak)) const char *FscHdfsGetFileBlockLocationNodeHost(FscHdfsFileBlockLocationC *bl,
                                                int index) {}
__attribute__((weak)) const char *FscHdfsGetFileBlockLocationNodeName(FscHdfsFileBlockLocationC *bl,
                                                int index) {}
__attribute__((weak)) const char *FscHdfsGetFileBlockLocationNodeTopoPath(
    FscHdfsFileBlockLocationC *bl, int index) {}

__attribute__((weak)) int FscHdfsHasErrorRaised(FscHdfsFileSystemC *fs) {}

// Still need some additional free/delete APIs to help release memory
__attribute__((weak)) void FscHdfsFreeFileSystemC(FscHdfsFileSystemC **fs) {}
__attribute__((weak)) void FscHdfsFreeFileC(FscHdfsFileC **f) {}
__attribute__((weak)) void FscHdfsFreeFileInfoArrayC(FscHdfsFileInfoArrayC **fiArray) {}
__attribute__((weak)) void FscHdfsFreeFileBlockLocationArrayC(
    FscHdfsFileBlockLocationArrayC **fblArray) {}
__attribute__((weak)) void FscHdfsFreeErrorContent(CatchedError *ce) {}
__attribute__((weak)) void SetToken(const char *tokenkey, const char *token) {}
__attribute__((weak)) void SetCcname(const char *ccname) {}
__attribute__((weak)) void cleanup_FSManager() {}

__attribute__((weak)) char FscHdfsGetFileKind(FscHdfsFileSystemC *fs, const char *path) {}

#ifdef __cplusplus
}
#endif

#endif  // STORAGE_SRC_STORAGE_CWRAPPER_HDFS_FILE_SYSTEM_C_H_
