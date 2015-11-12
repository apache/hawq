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

#include "cdb/cdbtmpdir.h"
#include "postgres.h"

#include <fcntl.h>
#include <unistd.h>

#include "access/heapam.h"
#include "access/xact.h"
#include "catalog/catalog.h"
#include "catalog/namespace.h"
#include "catalog/pg_authid.h"
#include "catalog/pg_database.h"
#include "catalog/pg_tablespace.h"
#include "libpq/hba.h"
#include "libpq/libpq-be.h"
#include "cdb/cdbvars.h"
#include "cdb/cdbutil.h"
#include "mb/pg_wchar.h"
#include "miscadmin.h"
#include "pgstat.h"
#include "postmaster/autovacuum.h"
#include "postmaster/postmaster.h"
#include "storage/backendid.h"
#include "storage/fd.h"
#include "storage/ipc.h"
#include "storage/proc.h"
#include "storage/procarray.h"
#include "storage/procsignal.h"
#include "storage/sinvaladt.h"
#include "storage/smgr.h"
#include "utils/acl.h"
#include "utils/flatfiles.h"
#include "utils/guc.h"
#include "utils/relcache.h"
#include "utils/resscheduler.h"
#include "utils/syscache.h"
#include "utils/tqual.h"        /* SharedSnapshot */
#include "utils/portal.h"
#include "pgstat.h"

static List *initTmpDirList(List *list, char *tmpdir_config);
static void  destroyTmpDirList(List *list);

List *initTmpDirList(List *list, char *tmpdir_string)
{
    int idx = -1;
    int i = 0;
    char *tmpdir;
    
    for (i=0;i<strlen(tmpdir_string);i++)
    {
        if (tmpdir_string[i] == ',')
        {
            tmpdir = (char *)palloc0(i-idx);
            memcpy(tmpdir, tmpdir_string+idx+1, i-idx-1);
            list = lappend(list, tmpdir);
            idx = i;
        }
    }
    tmpdir = (char *)palloc0(i-idx);
    memcpy(tmpdir, tmpdir_string+idx+1, i-idx-1);
    list = lappend(list, tmpdir);

    return list;
}

void  destroyTmpDirList(List *list)
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
    
    LocalTempPath = pstrdup((char *)lfirst(list_nth_cell(tmpdirs, gp_session_id % list_length(tmpdirs))));

    destroyTmpDirList(tmpdirs);
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
        getLocalTmpDirFromMasterConfig(session_id);
       
        // QE on segment
        tmpdirs = initTmpDirList(tmpdirs, rm_seg_tmp_dirs);  
        int64_t session_key = session_id;
        int64_t key = (session_key << 32) + command_id + qeidx;
        LocalTempPath = pstrdup((char *)lfirst(list_nth_cell(tmpdirs, key % list_length(tmpdirs))));
        destroyTmpDirList(tmpdirs);
    }
}
