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

#ifndef CDBTMPDIR_H
#define CDBTMPDIR_H
#include "c.h"

#define MAX_TMP_DIR_LEN    8192

typedef struct TmpDirInfo
{
    bool available;
    char path[MAX_TMP_DIR_LEN];
} TmpDirInfo;

extern int32_t TmpDirNum;

Size TmpDirInfoArrayShmemSize(void);
void TmpDirInfoArrayShmemInit(void);
char* GetTmpDirPathFromArray(int64_t idx);
bool DestroyTmpDirInfoArray(TmpDirInfo *info);
bool CheckTmpDirAvailable(char *path);
void destroyTmpDirList(List *list);
void checkTmpDirStatus(void);

void getMasterLocalTmpDirFromShmem(int session_id);
void getSegmentLocalTmpDirFromShmem(int session_id, int command_id, int qeidx);

#endif
