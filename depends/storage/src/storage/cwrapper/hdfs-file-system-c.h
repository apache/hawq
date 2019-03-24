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
///////////////////////////////////////////////////////////////////////////////

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
void FscHdfsSetError(CatchedError *ce, int errCode, const char *reason);
CatchedError *FscHdfsGetFileSystemError(FscHdfsFileSystemC *fs);

// File APIs
void FscHdfsCloseFileC(FscHdfsFileSystemC *fs, FscHdfsFileC *f);

// File system APIs
FscHdfsFileSystemC *FscHdfsNewFileSystem(const char *namenode, uint16_t port);

FscHdfsFileC *FscHdfsOpenFile(FscHdfsFileSystemC *fs, const char *path,
                              int flags);
void FscHdfsSeekFile(FscHdfsFileSystemC *fs, FscHdfsFileC *f, uint64_t offset);
void FscHdfsRemovePath(FscHdfsFileSystemC *fs, const char *path);
void FscHdfsRemovePathIfExists(FscHdfsFileSystemC *fs, const char *path);
FscHdfsFileInfoC *FscHdfsGetFileInfo(FscHdfsFileSystemC *fs,
                                     const char *fileName);
int FscHdfsExistPath(FscHdfsFileSystemC *fs, const char *path);
int64_t FscHdfsGetFileLength(FscHdfsFileSystemC *fs, const char *path);
int FscHdfsReadFile(FscHdfsFileSystemC *fs, FscHdfsFileC *f, void *buf,
                    int size);
void FscHdfsWriteFile(FscHdfsFileSystemC *fs, FscHdfsFileC *f, void *buf,
                      int size);
void FscHdfsCreateDir(FscHdfsFileSystemC *fs, const char *path);
// this is a special interface to create dir with a hidden ".tmp" subdir
int FscHdfsExistInsertPath(FscHdfsFileSystemC *fs, const char *path);
void FscHdfsCreateInsertDir(FscHdfsFileSystemC *fs, const char *path);
FscHdfsFileInfoArrayC *FscHdfsDirPath(FscHdfsFileSystemC *fs, const char *path);
void FscHdfsChmodPath(FscHdfsFileSystemC *fs, const char *path, int mode);
int64_t FscHdfsTellFile(FscHdfsFileSystemC *fs, FscHdfsFileC *f);
FscHdfsFileBlockLocationArrayC *FscHdfsGetPathFileBlockLocation(
    FscHdfsFileSystemC *fs, const char *path, int64_t start, int64_t length);
void FscHdfsSetFileSystemBlockSize(FscHdfsFileSystemC *fs, int size);
int FscHdfsGetFileSystemBlockSize(FscHdfsFileSystemC *fs);

const char *FscHdfsGetFileSystemAddress(FscHdfsFileSystemC *fs);
uint16_t FscHdfsGetFileSystemPort(FscHdfsFileSystemC *fs);

FscHdfsFileInfoC *FscHdfsGetFileInfoFromArray(FscHdfsFileInfoArrayC *fia,
                                              int index);
const char *FscHdfsGetFileInfoName(FscHdfsFileInfoC *fi);
int64_t FscHdfsGetFileInfoLength(FscHdfsFileInfoC *fi);

FscHdfsFileBlockLocationC *FscHdfsGetFileBlockLocationFromArray(
    FscHdfsFileBlockLocationArrayC *bla, int index);

int FscHdfsGetFileBlockLocationArraySize(FscHdfsFileBlockLocationArrayC *bla);

int FscHdfsGetFileBlockLocationNNodes(FscHdfsFileBlockLocationC *bl);
int64_t FscHdfsGetFileBlockLocationOffset(FscHdfsFileBlockLocationC *bl);
int64_t FscHdfsGetFileBlockLocationLength(FscHdfsFileBlockLocationC *bl);
int FscHdfsGetFileBlockLocationCorrupt(FscHdfsFileBlockLocationC *bl);
const char *FscHdfsGetFileBlockLocationNodeHost(FscHdfsFileBlockLocationC *bl,
                                                int index);
const char *FscHdfsGetFileBlockLocationNodeName(FscHdfsFileBlockLocationC *bl,
                                                int index);
const char *FscHdfsGetFileBlockLocationNodeTopoPath(
    FscHdfsFileBlockLocationC *bl, int index);

int FscHdfsHasErrorRaised(FscHdfsFileSystemC *fs);

// Still need some additional free/delete APIs to help release memory
void FscHdfsFreeFileSystemC(FscHdfsFileSystemC **fs);
void FscHdfsFreeFileC(FscHdfsFileC **f);
void FscHdfsFreeFileInfoArrayC(FscHdfsFileInfoArrayC **fiArray);
void FscHdfsFreeFileBlockLocationArrayC(
    FscHdfsFileBlockLocationArrayC **fblArray);
void FscHdfsFreeErrorContent(CatchedError *ce);
void SetToken(const char *tokenkey, const char *token);
void SetCcname(const char *ccname);
void cleanup_FSManager();

#ifdef __cplusplus
}
#endif

#endif  // STORAGE_SRC_STORAGE_CWRAPPER_HDFS_FILE_SYSTEM_C_H_
