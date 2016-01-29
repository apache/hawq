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
#include <sys/stat.h>

static List *initTmpDirList(List *list, char *tmpdir_config);

List *initTmpDirList(List *list, char *szTmpDir)
{
    int idx = -1, i = 0;
    char *tmpdir;
    int tmpDirNum = 0;

    for (i = 0; i <= strlen(szTmpDir); i++)
    {
        if (szTmpDir[i] == ',' || i == strlen(szTmpDir))
        {
            /* in case two commas are written together */
            if (i-idx > 1)
            {
                tmpdir = (char *)palloc0(i-idx);
                strncpy(tmpdir, szTmpDir+idx+1, i-idx-1);
                tmpDirNum++;
                elog(DEBUG5, "Get a temporary directory:%s", tmpdir);
                list = lappend(list, tmpdir);
            }
            idx = i;
        }
    }

    return list;
}

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

void getLocalTmpDirFromMasterConfig(int session_id)
{
    List *tmpdirs = NULL;

    tmpdirs = initTmpDirList(tmpdirs, rm_master_tmp_dirs);
    if (tmpdirs != NULL )
    {
        LocalTempPath = pstrdup((char *)lfirst(list_nth_cell(tmpdirs, gp_session_id % list_length(tmpdirs))));
        destroyTmpDirList(tmpdirs);
    }
}

void getLocalTmpDirFromSegmentConfig(int session_id, int command_id, int qeidx)
{
    List *tmpdirs = NULL;

    if (qeidx == -1)
    {
        // QE on master
        getLocalTmpDirFromMasterConfig(session_id);
    }
    else
    {
        // QE on segment
        tmpdirs = initTmpDirList(tmpdirs, rm_seg_tmp_dirs);
        int64_t session_key = session_id;
        int64_t key = (session_key << 32) + command_id + qeidx;
        if (tmpdirs != NULL )
        {
            LocalTempPath = pstrdup((char *)lfirst(list_nth_cell(tmpdirs, key % list_length(tmpdirs))));
            destroyTmpDirList(tmpdirs);
        }
    }
}




