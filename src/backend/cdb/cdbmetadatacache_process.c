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

#include <unistd.h>

#include "postgres.h"
#include "miscadmin.h"
#include "pgstat.h"
#include "utils/palloc.h"
#include "storage/fd.h"
#include "storage/relfilenode.h"

#include "catalog/catalog.h"
#include "catalog/pg_namespace.h"
#include "catalog/pg_authid.h"
#include "catalog/pg_tablespace.h"
#include "catalog/pg_database.h"
#include "cdb/cdbsharedoidsearch.h"
#include "cdb/cdbdirectopen.h"
#include "cdb/cdbvars.h"
#include "storage/itemptr.h"
#include "utils/hsearch.h"
#include "storage/shmem.h"
#include "access/genam.h"
#include "access/heapam.h"
#include "access/aomd.h"
#include "access/transam.h"
#include "utils/guc.h"
#include "storage/smgr.h"
#include "storage/ipc.h"
#include "storage/proc.h"
#include "storage/pmsignal.h"           /* PostmasterIsAlive */
#include "storage/sinvaladt.h"
#include "utils/builtins.h"
#include "utils/faultinjector.h"
#include "commands/tablespace.h"
#include "commands/dbcommands.h"
#include "cdb/cdbmetadatacache.h"
#include "tcop/tcopprot.h" /* quickdie() */
#include "storage/backendid.h"
#include "postmaster/fork_process.h"
#include "postmaster/postmaster.h"
#include "utils/ps_status.h"
#include "libpq/pqsignal.h"
#include "utils/memutils.h"

#include "funcapi.h"
#include "fmgr.h"
#include "utils/builtins.h"

/*
 * Metadata Cache Process Functions
 */
static void 
RequestShutdown(SIGNAL_ARGS);

static void 
InitMetadataCacheMemoryContext(void);

static void 
MetadataCacheServerLoop(void);

static void
GenerateMetadataCacheLRUList(void);

static void
GenerateMetadataCacheRefreshList(void);

static void 
ProcessMetadataCacheCheck(void);

static void 
ProcessMetadataCacheRefresh(void);

static int
CompareMetadataCacheEntryByLastAccessTime(const void *e1, const void *e2);

extern bool 
FindMyDatabase(const char *name, Oid *db_id, Oid *db_tablespace);

static MemoryContext            MetadataCacheMemoryContext = NULL;

List                     *MetadataCacheLRUList = NULL;
List                     *MetadataCacheRefreshList = NULL;

static volatile bool            shutdown_requested = false;

static char                     *probeDatabase = "template1";

/*
 *Main entry point for metadata cache process.
 */
int 
metadatacache_start(void)
{
    pid_t   MetadataCachePID;

    switch ((MetadataCachePID = fork_process()))
    {
        case -1:
            ereport(LOG, (errmsg("could not fork metadata cache process")));
            return 0;
        case 0:
            /* in postmaster child ... */
            /* Close the postmaster's sockets */
            ClosePostmasterPorts(false);

            MetadataCacheMain(0, NULL);
            return 0; 

        default:
            return (int)MetadataCachePID;
    }
}

void
RequestShutdown(SIGNAL_ARGS)
{
    shutdown_requested = true;
}

/*
 * MetadataCacheMain
 */
void 
MetadataCacheMain(int argc, char *argv[])
{
    sigjmp_buf  local_sigjmp_buf;
    char       *fullpath;

    IsUnderPostmaster = true;
    /* reset MyProcPid */
    MyProcPid = getpid();
    
    /* Lose the postmaster's on-exit routines */
    on_exit_reset();

    /* Identify myself via ps */
    init_ps_display("DFS Metadata Cache process", "", "", "");

    SetProcessingMode(InitProcessing);

    /*
     * Set up signal handlers.  We operate on databases much like a regular
     * backend, so we use the same signal handling.  See equivalent code in
     * tcop/postgres.c.
     *
     * Currently, we don't pay attention to postgresql.conf changes that
     * happen during a single daemon iteration, so we can ignore SIGHUP.
     */
    pqsignal(SIGHUP, SIG_IGN);

    /*
     * Presently, SIGINT will lead to autovacuum shutdown, because that's how
     * we handle ereport(ERROR).  It could be improved however.
     */
    pqsignal(SIGINT, SIG_IGN);
    pqsignal(SIGTERM, die);
    pqsignal(SIGQUIT, quickdie); /* we don't do any ftsprobe specific cleanup, just use the standard. */
    pqsignal(SIGALRM, handle_sig_alarm);

    pqsignal(SIGPIPE, SIG_IGN);
    pqsignal(SIGUSR1, SIG_IGN);
    /* We don't listen for async notifies */
    pqsignal(SIGUSR2, RequestShutdown);
    pqsignal(SIGFPE, FloatExceptionHandler);
    pqsignal(SIGCHLD, SIG_DFL);

        /* Early initialization */
    BaseInit();

    /* See InitPostgres()... */
    InitProcess();
    InitBufferPoolBackend();
    InitXLOGAccess();

    SetProcessingMode(NormalProcessing);

    /*
     * If an exception is encountered, processing resumes here.
     *
     * See notes in postgres.c about the design of this coding.
     */
    if (sigsetjmp(local_sigjmp_buf, 1) != 0)
    {
        /* Prevents interrupts while cleaning up */
        HOLD_INTERRUPTS();

        /* Report the error to the server log */
        EmitErrorReport();

        /*
         * We can now go away.  Note that because we'll call InitProcess, a
         * callback will be registered to do ProcKill, which will clean up
         * necessary state.
         */
        proc_exit(0);
    }

    /* We can now handle ereport(ERROR) */
    PG_exception_stack = &local_sigjmp_buf;

    PG_SETMASK(&UnBlockSig);

    MyDatabaseId = TemplateDbOid;
    MyDatabaseTableSpace = DEFAULTTABLESPACE_OID;
    if (!FindMyDatabase(probeDatabase, &MyDatabaseId, &MyDatabaseTableSpace))
        ereport(FATAL, (errcode(ERRCODE_UNDEFINED_DATABASE),
            errmsg("database 'postgres' does not exist")));

    fullpath = GetDatabasePath(MyDatabaseId, MyDatabaseTableSpace);

    SetDatabasePath(fullpath);    

    /*
     * Finish filling in the PGPROC struct, and add it to the ProcArray. (We
     * need to know MyDatabaseId before we can do this, since it's entered
     * into the PGPROC struct.)
     *
     * Once I have done this, I am visible to other backends!
     */
    InitProcessPhase2();

    /*
     * Initialize my entry in the shared-invalidation manager's array of
     * per-backend data.
     *
     * Sets up MyBackendId, a unique backend identifier.
     */
    MyBackendId = InvalidBackendId;

    SharedInvalBackendInit(false);

    if (MyBackendId > MaxBackends || MyBackendId <= 0)
        elog(FATAL, "bad backend id: %d", MyBackendId);

    /*
     * bufmgr needs another initialization call too
     */
    InitBufferPoolBackend();

    InitMetadataCacheMemoryContext();
    MemoryContextSwitchTo(MetadataCacheMemoryContext);

    /* main loop */
    MetadataCacheServerLoop();

    /* One iteration done, go away */
    proc_exit(0);
}

void 
MetadataCacheServerLoop(void)
{
    uint32_t probe_start_time;
    int check_times = 0;

    while (true) 
    {
        if (shutdown_requested)
            break;

        /* no need to live on if postmaster has died */
        if (!PostmasterIsAlive(true))
            exit(1);

        probe_start_time = time(NULL);

        // check time
        elog(DEBUG1, "[MetadataCache] Metadata Cache Process Check Time. time:%u", probe_start_time);
        ProcessMetadataCacheCheck();

        if (check_times >= metadata_cache_refresh_interval / metadata_cache_check_interval)
        {
            // refresh
            elog(DEBUG1, "Metadata Cache Process Refresh Time. time:%u", probe_start_time);
            check_times = 0;
            ProcessMetadataCacheRefresh();
        }
        else
        {
            /* check if we need to sleep before starting next iteration */
            pg_usleep(metadata_cache_check_interval * USECS_PER_SEC);
            check_times++;
        }
    } 
}

void 
InitMetadataCacheMemoryContext(void)
{
    if (MetadataCacheMemoryContext == NULL)
    {
        MetadataCacheMemoryContext  = AllocSetContextCreate(TopMemoryContext,
                                                        "MetadataCacheMemoryContext",
                                                        ALLOCSET_DEFAULT_MINSIZE,
                                                        ALLOCSET_DEFAULT_INITSIZE,
                                                        ALLOCSET_DEFAULT_MAXSIZE);
    }
    else
    {
        MemoryContextResetAndDeleteChildren(MetadataCacheMemoryContext);
    }

    return;
}

void
GenerateMetadataCacheLRUList()
{
    HASH_SEQ_STATUS hstat;
    MetadataCacheEntry *entry;
    long cache_entry_num = 0;

    LWLockAcquire(MetadataCacheLock, LW_EXCLUSIVE);
    cache_entry_num = hash_get_num_entries(MetadataCache);

    if (cache_entry_num == 0) {
        LWLockRelease(MetadataCacheLock);
        return;
    }

    MetadataCacheEntry** entry_vector = (MetadataCacheEntry**)palloc(sizeof(MetadataCacheEntry*) * cache_entry_num);   

    int i=0;
    hash_seq_init(&hstat, MetadataCache);
    while ((entry = (MetadataCacheEntry *)hash_seq_search(&hstat)) != NULL)
    {
        entry_vector[i++] = entry; 
    }

    qsort(entry_vector, cache_entry_num, sizeof(MetadataCacheEntry*), CompareMetadataCacheEntryByLastAccessTime);

    for (i=0;i<cache_entry_num;i++) 
    {
        MetadataCacheCheckInfo *check_info = (MetadataCacheCheckInfo *)palloc(sizeof(MetadataCacheCheckInfo));
        check_info->key = entry_vector[i]->key;
        check_info->file_size = entry_vector[i]->file_size;
        check_info->block_num = entry_vector[i]->block_num;
        check_info->create_time = entry_vector[i]->create_time;
        check_info->last_access_time = entry_vector[i]->last_access_time;
        MetadataCacheLRUList = lappend(MetadataCacheLRUList, check_info);
    }

    pfree(entry_vector);

    LWLockRelease(MetadataCacheLock);
}

void
GenerateMetadataCacheRefreshList()
{
    HASH_SEQ_STATUS hstat;
    MetadataCacheEntry *entry;
    uint32_t cur_time = time(NULL);
    
    if (MetadataCacheRefreshList)
    {
        list_free_deep(MetadataCacheRefreshList);
        MetadataCacheRefreshList = NULL;
    }
    
    LWLockAcquire(MetadataCacheLock, LW_EXCLUSIVE);
    
    hash_seq_init(&hstat, MetadataCache);
    while ((entry = (MetadataCacheEntry *)hash_seq_search(&hstat)) != NULL)
    {
        if (cur_time - entry->create_time >= metadata_cache_refresh_timeout)
        {
            MetadataCacheCheckInfo *refresh_info = (MetadataCacheCheckInfo *)palloc(sizeof(MetadataCacheCheckInfo));
            refresh_info->key = entry->key;
            refresh_info->file_size = entry->file_size;
            refresh_info->block_num = entry->block_num;
            refresh_info->create_time = entry->create_time;
            refresh_info->last_access_time = entry->last_access_time;
            MetadataCacheRefreshList = lappend(MetadataCacheRefreshList, refresh_info);
            if (list_length(MetadataCacheRefreshList) >= metadata_cache_refresh_max_num)
            {
                hash_seq_term(&hstat);
                break;
            }
        }
    }

    LWLockRelease(MetadataCacheLock);
    
    elog(DEBUG1, "[MetadataCache] ProcessMetadataCacheRefresh, get refresh list:%d", list_length(MetadataCacheRefreshList));
}

void 
ProcessMetadataCacheCheck()
{
    ListCell *lc;
    RelFileNode rnode;
    HdfsFileInfo *file_info;

    double free_block_ratio = ((double)FREE_BLOCK_NUM) / metadata_cache_block_capacity;
    long cache_entry_num = hash_get_num_entries(MetadataCache);
    double cache_entry_ratio = ((double)cache_entry_num) / metadata_cache_max_hdfs_file_num;

    elog(DEBUG1, "[MetadataCache] ProcessMetadataCacheCheck free_block_ratio:%f", free_block_ratio);

    if (free_block_ratio < metadata_cache_free_block_max_ratio || cache_entry_ratio > metadata_cache_flush_ratio)
    {
    	if(cache_entry_num >= metadata_cache_max_hdfs_file_num)
    	{
    		elog(LOG, "[MetadataCache] ProcessMetadataCacheCheck : Metadata cache is full.The cache entry num is:%ld. The metadata_cache_max_hdfs_file_num is:%d", cache_entry_num, metadata_cache_max_hdfs_file_num);
    	}
    	elog(DEBUG1, "[MetadataCache] ProcessMetadataCacheCheck cache_entry_ratio:%f", cache_entry_ratio);
        if (NULL == MetadataCacheLRUList) 
        {
            GenerateMetadataCacheLRUList();
        }

        int total_remove_files = 0;
        LWLockAcquire(MetadataCacheLock, LW_EXCLUSIVE);

        foreach(lc, MetadataCacheLRUList)
        {
            MetadataCacheCheckInfo *check_info = (MetadataCacheCheckInfo *)lfirst(lc);

            rnode.spcNode = check_info->key.tablespace_oid;
            rnode.dbNode = check_info->key.database_oid;
            rnode.relNode = check_info->key.relation_oid;

            file_info = CreateHdfsFileInfo(rnode, check_info->key.segno);
            RemoveHdfsFileBlockLocations(file_info);
            DestroyHdfsFileInfo(file_info);
            total_remove_files++;

            double cache_entry_ratio = ((double)hash_get_num_entries(MetadataCache)) / metadata_cache_max_hdfs_file_num;

            if ((((double)FREE_BLOCK_NUM) / metadata_cache_block_capacity) >= metadata_cache_free_block_normal_ratio
            		&& cache_entry_ratio < metadata_cache_reduce_ratio)
            {
                break;
            }
        }
    
        list_free_deep(MetadataCacheLRUList);
        MetadataCacheLRUList = NULL;

        LWLockRelease(MetadataCacheLock);
            
        elog(DEBUG1, "[MetadataCache] ProcessMetadataCacheCheck, total remove files:%d", total_remove_files);
    }
}

void 
ProcessMetadataCacheRefresh()
{
    ListCell *lc;
    BlockLocation *hdfs_locations;
    int block_num;

    if (NULL == MetadataCacheRefreshList)
    {
        GenerateMetadataCacheRefreshList();
    }

    foreach(lc, MetadataCacheRefreshList)
    {
        MetadataCacheCheckInfo *refresh_file = (MetadataCacheCheckInfo *)lfirst(lc);

        RelFileNode rnode;
        rnode.spcNode = refresh_file->key.tablespace_oid;
        rnode.dbNode = refresh_file->key.database_oid;
        rnode.relNode = refresh_file->key.relation_oid;

        HdfsFileInfo *file_info = CreateHdfsFileInfo(rnode, refresh_file->key.segno);

        hdfs_locations = HdfsGetFileBlockLocations(file_info->filepath, refresh_file->file_size, &block_num);
    
        LWLockAcquire(MetadataCacheLock, LW_EXCLUSIVE);
        if (!hdfs_locations)
        {
            // error file, delete
            RemoveHdfsFileBlockLocations(file_info); 
            elog(DEBUG1, "[MetadataCache] ProcessMetadataCacheRefresh remove filename:%s filesize:"INT64_FORMAT" block_num:%d",
                            file_info->filepath, 
                            refresh_file->file_size, 
                            block_num);
        }
        else
        {
            // fetch ok, update
            RemoveHdfsFileBlockLocations(file_info); 
            if (NULL == MetadataCacheNew(file_info, refresh_file->file_size, hdfs_locations, block_num))
            {
                elog(DEBUG1, "[MetadataCache] ProcessMetadataCacheRefresh update fail, filename:%s filesize:"INT64_FORMAT" block_num:%d",
                                file_info->filepath, 
                                refresh_file->file_size, 
                                block_num);
            }
            else
            {
                elog(DEBUG1, "[MetadataCache] ProcessMetadataCacheRefresh update filename:%s filesize:"INT64_FORMAT" block_num:%d",
                                file_info->filepath, 
                                refresh_file->file_size, 
                                block_num);
            }
            HdfsFreeFileBlockLocations(hdfs_locations, block_num);
        }
        LWLockRelease(MetadataCacheLock);

        DestroyHdfsFileInfo(file_info);
    }
}

int
CompareMetadataCacheEntryByLastAccessTime(const void *e1, const void *e2)
{
    MetadataCacheEntry **ce1 = (MetadataCacheEntry**)e1;
    MetadataCacheEntry **ce2 = (MetadataCacheEntry**)e2;
    
    if ((*ce1)->last_access_time < (*ce2)->last_access_time)
    {
        return -1;
    }

    if ((*ce1)->last_access_time > (*ce2)->last_access_time)
    {
        return 1;
    }

    return 0;
}
