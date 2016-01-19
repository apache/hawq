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

#include "postgres.h"
#include "cdb/cdbvars.h"
#include "cdb/cdbutil.h"
#include "cdb/cdbtmpdir.h"
#include "cdb/cdbvars.h"
#include "miscadmin.h"
#include "postmaster/autovacuum.h"
#include "storage/ipc.h"
#include "storage/shmem.h"
#include <sys/stat.h>

TmpDirInfo* TmpDirInfoArray = NULL;

static List *tmpDirList = NULL;

int32_t TmpDirNum = 0;

Size TmpDirInfoArraySize(void);

void TmpDirInfoArray_ShmemInit(void);

char* GetTmpDirPathFromArray(int64_t idx);

bool DestroyTmpDirInfoArray(TmpDirInfo *info);

bool CheckTmpDirAvailable(char *path);

void destroyTmpDirList(List *list)
{
    ListCell *lc = NULL;

    foreach(lc, list)
    {
        char *tmpdir = (char *)lfirst(lc);
        pfree(tmpdir);
    }
    list_free(list);
}

static bool CheckDirValid(char* path)
{
    struct stat info;
    if (path == NULL || stat(path, &info) < 0)
    {
        return false;
    }
    else
    {
        if (!S_ISDIR(info.st_mode))
            return false;
        else
            return true;
    }
}

static int GetTmpDirNumber(char* szTmpDir)
{
    int i = 0, idx = -1;
    char *tmpdir = NULL;
    int tmpDirNum = 0;
    tmpDirList = NULL;

    for (i = 0; i <= strlen(szTmpDir); i++)
    {
        if (szTmpDir[i] == ',' || i == strlen(szTmpDir))
        {
            /* in case two commas are written together */
            if (i-idx > 1 && i-idx <= MAX_TMP_DIR_LEN)
            {
                tmpdir = (char *)palloc0(i-idx);
                strncpy(tmpdir, szTmpDir+idx+1, i-idx-1);
                if(CheckDirValid(tmpdir))
                {
                    tmpDirNum++;
                    elog(LOG, "Get a temporary directory:%s", tmpdir);
                    tmpDirList = lappend(tmpDirList, tmpdir);
                }
                else
                {
                    pfree(tmpdir);
                }
            }
            idx = i;
        }
    }

    elog(LOG, "Get %d temporary directories", tmpDirNum);
    return tmpDirNum;
}

/*
 *  Calculate the size of share memory for temporary directory information
 */
Size TmpDirInfoArrayShmemSize(void)
{

    if (AmIMaster())
    {
        TmpDirNum = GetTmpDirNumber(rm_master_tmp_dirs);
    }
    else if (AmISegment())
    {
        TmpDirNum = GetTmpDirNumber(rm_seg_tmp_dirs);
    }
    else
    {
        elog(LOG, "Don't need create share memory for temporary directory information");
        TmpDirNum = 0;
    }

    return MAXALIGN(TmpDirNum*sizeof(TmpDirInfo));
}

/*
 *  Initialize share memory for temporary directory information
 */
void TmpDirInfoArrayShmemInit(void)
{
    bool found = false;

    if (TmpDirNum == 0)
        return;

    TmpDirInfoArray = (TmpDirInfo *)ShmemInitStruct("Temporary Directory Information Cache",
                                                    TmpDirNum*sizeof(TmpDirInfo), &found);
    if(!TmpDirInfoArray)
    {
        elog(FATAL,
             "Could not initialize Temporary Directory Information shared memory");
    }

    if(!found)
    {
        ListCell *lc = NULL;
        int32_t i = 0;
        MemSet(TmpDirInfoArray, 0, TmpDirNum*sizeof(TmpDirInfo));
        foreach(lc, tmpDirList) {
            if (strlen((char*)lfirst(lc)) < MAX_TMP_DIR_LEN)
            {
                strncpy(TmpDirInfoArray[i].path, (char*)lfirst(lc), strlen((char*)lfirst(lc)));
                TmpDirInfoArray[i].available = true;
                i++;
            }
        }

        if (tmpDirList)
        {
            destroyTmpDirList(tmpDirList);
        }
    }
    elog(LOG, "Initialize share memeory for temporary directory info finish.");
}

/*
 *  Check if this temporary directory is OK to read or write.
 *  If not, it's probably due to disk error.
 */
bool CheckTmpDirAvailable(char *path)
{
    FILE  *tmp = NULL;
    bool  ret = true;
    char* fname = NULL;
    char* testfile = "/checktmpdir.log";

    /* write some bytes to a file to check if
     * this temporary directory is OK.
     */
    fname = palloc0(strlen(path) + strlen(testfile) + 1);
    strncpy(fname, path, strlen(path));
    strncpy(fname + strlen(path), testfile, strlen(testfile));
    tmp = fopen(fname, "w");
    if (tmp == NULL)
    {
        elog(LOG, "Can't open file:%s when check temporary directory", fname);
        ret = false;
        goto _exit;
    }

    if (fseek(tmp, 0, SEEK_SET) != 0)
    {
        elog(LOG, "Can't seek file:%s when check temporary directory", fname);
        ret = false;
        goto _exit;
    }

    if (strlen("test") != fwrite("test", 1, strlen("test"), tmp))
    {
        elog(LOG, "Can't write file:%s when check temporary directory", fname);
        ret = false;
        goto _exit;
    }

_exit:
    pfree(fname);
    if (tmp != NULL)
        fclose(tmp);
    return ret;
}

/*
 * Check the status of each temporary directory kept in
 * shared memory, set to false if it is not available.
 */
void checkTmpDirStatus(void)
{
    LWLockAcquire(TmpDirInfoLock, LW_SHARED);

    for (int i = 0; i < TmpDirNum; i++)
    {
        bool oldStatus = TmpDirInfoArray[i].available;
        bool newStatus = CheckTmpDirAvailable(TmpDirInfoArray[i].path);
        if (oldStatus != newStatus)
        {
            LWLockRelease(TmpDirInfoLock);
            LWLockAcquire(TmpDirInfoLock, LW_EXCLUSIVE);
            TmpDirInfoArray[i].available = newStatus;
            LWLockRelease(TmpDirInfoLock);
            LWLockAcquire(TmpDirInfoLock, LW_SHARED);
        }
    }

    LWLockRelease(TmpDirInfoLock);
    elog(LOG, "checkTmpDirStatus finish!");
}

/*
 * Get a list of failed temporary directory
 */
List* getFailedTmpDirList(void)
{
    List *failedList = NULL;
    char *failedDir = NULL;

    LWLockAcquire(TmpDirInfoLock, LW_SHARED);
    for (int i = 0; i < TmpDirNum; i++)
    {
        if (!TmpDirInfoArray[i].available)
        {
            failedDir = pstrdup(TmpDirInfoArray[i].path);
            failedList = lappend(failedList, failedDir);
        }
    }
    LWLockRelease(TmpDirInfoLock);
    return failedList;
}

/*
 *  Get a temporary directory path from array by its index
 */
char* GetTmpDirPathFromArray(int64_t idx)
{
    Insist(idx >=0 && idx <= TmpDirNum-1);

    LWLockAcquire(TmpDirInfoLock, LW_SHARED);

    if (TmpDirInfoArray[idx].available)
    {
        LWLockRelease(TmpDirInfoLock);
        return TmpDirInfoArray[idx].path;
    }
    else
    {
        LWLockRelease(TmpDirInfoLock);
        ereport(FATAL,
                (errcode(ERRCODE_CDB_INTERNAL_ERROR),
                errmsg("Temporary directory:%s is failed", TmpDirInfoArray[idx].path)));
    }
    return NULL;
}

void getMasterLocalTmpDirFromShmem(int session_id)
{
    LocalTempPath = GetTmpDirPathFromArray(session_id % TmpDirNum);
}

void getSegmentLocalTmpDirFromShmem(int session_id, int command_id, int qeidx)
{
    if(qeidx == -1)
    {
        getMasterLocalTmpDirFromShmem(session_id);
    }
    else
    {
        int64_t session_key = session_id;
        int64_t key = (session_key << 32) + command_id + qeidx;
        LocalTempPath = GetTmpDirPathFromArray(key % TmpDirNum);
    }
}
