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
#include "cdb/cdbmetadatacache_internal.h"
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

#define MAX_HDFS_HOST_NUM               1024
#define MAX_BLOCK_INFO_LEN              128
#define BLOCK_INFO_BIT_NUM              16

/*
 *  HDFS Block Info Structure
 */
typedef enum MetadataBlockInfoType
{
    METADATA_BLOCK_INFO_TYPE_HOSTS          = 0,
    METADATA_BLOCK_INFO_TYPE_NAMES,
    METADATA_BLOCK_INFO_TYPE_TOPOLOGYPATHS,
    METADATA_BLOCK_INFO_TYPE_NUM,
} MetadataBlockInfoType;

typedef struct BlockInfoKey
{
    char                block_info[MAX_BLOCK_INFO_LEN];  
} BlockInfoKey;

typedef struct BlockInfoEntry
{
    BlockInfoKey        key;
    uint16_t            index;            
} BlockInfoEntry;

typedef struct RevertBlockInfoKey
{
    uint16_t            index;
} RevertBlockInfoKey;

typedef struct RevertBlockInfoEntry
{
    RevertBlockInfoKey  key;
    char                block_info[MAX_BLOCK_INFO_LEN];
} RevertBlockInfoEntry;

/*
 * Metadata Cache Global Initialization Functions
 */
static bool 
MetadataCacheHashTableInit(void);

static bool 
MetadataBlockInfoTablesInit(void);

static bool
MetadataRevertBlockInfoTablesInit(void);

static bool 
MetadataCacheHdfsBlockArrayInit(void);

/*
 *  Metadata Cache Operation Functions
 */
static void                 
InitMetadataCacheKey(MetadataCacheKey *key, const HdfsFileInfo *file_info);

static MetadataCacheEntry *
MetadataCacheExists(const HdfsFileInfo *file_info);

static MetadataCacheEntry *
MetadataCacheEnter(const HdfsFileInfo *file_info);

static void 
MetadataCacheRemove(const HdfsFileInfo *file_info);

/*
 *  HDFS Block Operation Functions
 */
static void 
AllocMetadataBlock(int block_num, uint32_t *first_block_id, uint32_t *last_block_id);

static void 
ReleaseMetadataBlock(int block_num, uint32_t first_block_id, uint32_t last_block_id);

static uint64_t 
SetMetadataBlockInfo(MetadataBlockInfoType type, char **infos, uint32_t num);

static char *
GetMetadataBlockInfo(MetadataBlockInfoType type, uint64_t infos, int index);

/*
 *  Metadata Cache Global Variables
 */
MetadataCacheSharedData  *MetadataCacheSharedDataInstance = NULL;
HTAB                     *MetadataCache = NULL;
MetadataHdfsBlockInfo    *MetadataBlockArray = NULL;

static HTAB                     *BlockHostsMap = NULL;
static HTAB                     *BlockNamesMap = NULL;
static HTAB                     *BlockTopologyPathsMap = NULL;

static HTAB                     *RevertBlockHostsMap = NULL;
static HTAB                     *RevertBlockNamesMap = NULL;
static HTAB                     *RevertBlockTopologyPathsMap = NULL;

//static BlockLocation *CreateHdfsFileBlockLocations(BlockLocation *hdfs_locations, int block_num);
//static BlockLocation *MergeHdfsFileBlockLocations(BlockLocation *locations1, int block_num1, BlockLocation *locations2, int block_num2);


// create block locations for user
static BlockLocation *GetHdfsFileBlockLocationsNoCache(const HdfsFileInfo *file_info, uint64_t filesize, int *block_num);
static BlockLocation *GetHdfsFileBlockLocationsFromCache(MetadataCacheEntry *entry, uint64_t filesize, int *block_num);
//static BlockLocation *AppendHdfsFileBlockLocationsToCache(const HdfsFileInfo *file_info, MetadataCacheEntry *entry, uint64_t filesize, int *block_num, double *hit_ratio);

/*
 *  Estimate metadata cache shared memory size
 *      - Metadata cache hash size
 *      - Block info (3 types) hash size
 *      - Metadata cache shared structure size
 *      - Metadata hdfs block array size
 */
Size 
MetadataCache_ShmemSize(void)
{
    Size size;
    
    size = hash_estimate_size((Size)metadata_cache_max_hdfs_file_num, sizeof(MetadataCacheEntry));

    size = add_size(size, hash_estimate_size((Size)MAX_HDFS_HOST_NUM, sizeof(BlockInfoEntry)) * METADATA_BLOCK_INFO_TYPE_NUM);

    size = add_size(size, sizeof(MetadataCacheSharedData));

    size = add_size(size, metadata_cache_block_capacity* sizeof(MetadataHdfsBlockInfo));

    return size;
}

/*
 *  Initialize all the data structure in the share memory for metadata cache
 *      - Metadata cache shared data structure
 *      - Metadata cache hash table
 *      - Metadata cache block info hash tables (3 types) 
 *      - Metadata hdfs block array
 */
void 
MetadataCache_ShmemInit(void)
{
    bool found; 

    MetadataCacheSharedDataInstance = (MetadataCacheSharedData *)ShmemInitStruct("Metadata Cache Shared Data", 
                                                        sizeof(MetadataCacheSharedData), &found);

    if (found && MetadataCacheSharedDataInstance)
    {
        return;
    }

    if (NULL == MetadataCacheSharedDataInstance)
    {
        elog(FATAL, "[MetadataCache] fail to allocate share memory for metadata cache shared data");
    }

    MetadataCacheSharedDataInstance->free_block_num = metadata_cache_block_capacity;
    MetadataCacheSharedDataInstance->free_block_head = 0;
    MetadataCacheSharedDataInstance->cur_hosts_idx = 0;
    MetadataCacheSharedDataInstance->cur_names_idx = 0;
    MetadataCacheSharedDataInstance->cur_topologyPaths_idx = 0;

    if (!MetadataCacheHashTableInit())
    {
        elog(FATAL, "[MetadataCache] fail to allocate share memory for metadata cache hash table");
    }
  
    if (!MetadataBlockInfoTablesInit())
    {
        elog(FATAL, "[MetadataCache] fail to allocate share memory for metadata cache block info hash tables");
    }

    if (!MetadataRevertBlockInfoTablesInit())
    {
        elog(FATAL, "[MetadataCache] fail to allocate share memory for metadata cache revert block info hash tables");
    }

    if (!MetadataCacheHdfsBlockArrayInit())
    {
        elog(FATAL, "[MetadataCache] fail to allocate share memory for metadata cache hdfs block array");
    }

    elog(LOG, "[MetadataCache] Metadata cache initialize successfully. block_capacity:%d", metadata_cache_block_capacity);

    return;
}

/*
 * Initialize metadata cache hash table
 */
bool 
MetadataCacheHashTableInit(void)
{
    HASHCTL     info;
    int         hash_flags;
 
    MemSet(&info, 0, sizeof(info));

    info.keysize = sizeof(MetadataCacheKey);
    info.entrysize = sizeof(MetadataCacheEntry);
    info.hash = tag_hash;
    hash_flags = (HASH_ELEM | HASH_FUNCTION);

    MetadataCache = ShmemInitHash("Metadata Cache", metadata_cache_max_hdfs_file_num, metadata_cache_max_hdfs_file_num, &info, hash_flags);
    if (NULL == MetadataCache)
    {
        return false;
    }

    return true;
}

/*
 *  Initialize metadata block info hash tables
 *      - block host ip hash
 *      - block host name hash
 *      - block host topology hash
 */
bool 
MetadataBlockInfoTablesInit(void)
{
    HASHCTL     info;
    int         hash_flags;
 
    MemSet(&info, 0, sizeof(info));

    info.keysize = sizeof(BlockInfoKey);
    info.entrysize = sizeof(BlockInfoEntry);
    hash_flags = HASH_ELEM;

    BlockHostsMap = ShmemInitHash("Metadata Block Hosts Map", MAX_HDFS_HOST_NUM, MAX_HDFS_HOST_NUM, &info, hash_flags);
    if (NULL == BlockHostsMap)
    {
        return false;
    }
 
    BlockNamesMap = ShmemInitHash("Metadata Block Names Map", MAX_HDFS_HOST_NUM, MAX_HDFS_HOST_NUM, &info, hash_flags);
    if (NULL == BlockNamesMap)
    {
        return false;
    }
    
    BlockTopologyPathsMap = ShmemInitHash("Metadata Block TopologyPaths Map", MAX_HDFS_HOST_NUM, MAX_HDFS_HOST_NUM, &info, hash_flags);
    if (NULL == BlockTopologyPathsMap)
    {
        return false;
    }

    return true;
}

/*
 *  Initialize metadata revert block info hash tables
 */
bool 
MetadataRevertBlockInfoTablesInit(void)
{
    HASHCTL     info;
    int         hash_flags;
 
    MemSet(&info, 0, sizeof(info));

    info.keysize = sizeof(RevertBlockInfoKey);
    info.entrysize = sizeof(RevertBlockInfoEntry);
    hash_flags = HASH_ELEM;

    RevertBlockHostsMap = ShmemInitHash("Metadata Revert Block Hosts Map", MAX_HDFS_HOST_NUM, MAX_HDFS_HOST_NUM, &info, hash_flags);
    if (NULL == RevertBlockHostsMap)
    {
        return false;
    }
 
    RevertBlockNamesMap = ShmemInitHash("Metadata Revert Block Names Map", MAX_HDFS_HOST_NUM, MAX_HDFS_HOST_NUM, &info, hash_flags);
    if (NULL == RevertBlockNamesMap)
    {
        return false;
    }
    
    RevertBlockTopologyPathsMap = ShmemInitHash("Metadata Revert Block TopologyPaths Map", MAX_HDFS_HOST_NUM, MAX_HDFS_HOST_NUM, &info, hash_flags);
    if (NULL == RevertBlockTopologyPathsMap)
    {
        return false;
    }

    return true;
}


/*
 *  Initialize metadata hdfs block array
 */
bool 
MetadataCacheHdfsBlockArrayInit(void)
{
    Insist(MetadataCacheSharedDataInstance != NULL);
    
    int i = 0;
    bool found;
    
    MetadataBlockArray = (MetadataHdfsBlockInfo *)ShmemInitStruct("Metadata Cache HDFS Block Array",
                                metadata_cache_block_capacity * sizeof(MetadataHdfsBlockInfo), &found);

    if (NULL == MetadataBlockArray)
    {
        return false;
    }

    FREE_BLOCK_NUM = metadata_cache_block_capacity;
    FREE_BLOCK_HEAD = 0;

    for (i=0;i<FREE_BLOCK_NUM;i++)
    {
        NEXT_BLOCK_ID(i) = i + 1;
    }
    NEXT_BLOCK_ID(FREE_BLOCK_NUM - 1) = END_OF_BLOCK;

    return true;
}

/*
 *  Create HdfsFileInfo structure before calling GetHdfsFileBlockLocations 
 */
HdfsFileInfo *
CreateHdfsFileInfo(RelFileNode rnode, int segno)
{
    char *basepath = NULL;
    int relfile_len = 0;
    
    HdfsFileInfo *file_info = (HdfsFileInfo *)palloc(sizeof(HdfsFileInfo));
    if (NULL == file_info)
    {
        return NULL;
    }

    file_info->tablespace_oid = rnode.spcNode;
    file_info->database_oid = rnode.dbNode;
    file_info->relation_oid = rnode.relNode;
    file_info->segno = segno;

    basepath = relpath(rnode);
    relfile_len = strlen(basepath) + 9;
    file_info->filepath = (char *)palloc0(relfile_len);
    FormatAOSegmentFileName(basepath, file_info->segno, -1, 0, &file_info->segno, file_info->filepath);
    pfree(basepath);

    return file_info;
}

/*
 *  Destroy HdfsFileInfo structure after call GetHdfsFileBlockLocations 
 */
void 
DestroyHdfsFileInfo(HdfsFileInfo *file_info)
{
    if (file_info)
    {
        if (file_info->filepath)
        {
            pfree(file_info->filepath);
        }
        pfree(file_info);
        file_info = NULL;
    }
}

/*
 *  Initialize metadata cache key
 */
void 
InitMetadataCacheKey(MetadataCacheKey *key, const HdfsFileInfo *file_info)
{
    Insist(NULL != key);
    Insist(NULL != file_info);
    
    key->tablespace_oid = file_info->tablespace_oid;
    key->database_oid = file_info->database_oid;
    key->relation_oid = file_info->relation_oid;
    key->segno = file_info->segno;
}

/*
 *  Metadata cache exist
 */
MetadataCacheEntry *
MetadataCacheExists(const HdfsFileInfo *file_info)
{
    Insist(file_info != NULL);

    bool found;
    MetadataCacheKey key;
    
    InitMetadataCacheKey(&key, file_info);
    return (MetadataCacheEntry *)hash_search(MetadataCache, (void *)&key, HASH_FIND, &found);
}

/*
 *  Metadata cache insert
 */
MetadataCacheEntry *
MetadataCacheEnter(const HdfsFileInfo *file_info)
{
    Insist(file_info != NULL);
    
    bool found;
    MetadataCacheKey key;

    InitMetadataCacheKey(&key, file_info);
    return (MetadataCacheEntry *)hash_search(MetadataCache, (void *)&key, HASH_ENTER_NULL, &found);
}

/*
 *  Metadata cache remove
 */
void 
MetadataCacheRemove(const HdfsFileInfo *file_info)
{
    Insist(file_info != NULL);
    
    bool found;
    MetadataCacheKey key;

    InitMetadataCacheKey(&key, file_info);
    hash_search(MetadataCache, (void *)&key, HASH_REMOVE, &found);
}


/*
 *  Allocate block in the MetadataBlockArray
 */
void 
AllocMetadataBlock(int block_num, uint32_t *first_block_id, uint32_t *last_block_id)
{
    Insist(block_num > 0);
    Insist(FREE_BLOCK_NUM >= block_num);
    
    int i;
    
    *first_block_id = FREE_BLOCK_HEAD;
    for (i=0;i<block_num;i++)
    {
        *last_block_id = FREE_BLOCK_HEAD; 
        FREE_BLOCK_HEAD = NEXT_BLOCK_ID(*last_block_id);
    }
    NEXT_BLOCK_ID(*last_block_id) = END_OF_BLOCK;

    FREE_BLOCK_NUM -= block_num;
}

/*
 *  Release block in the MetadataBlockArray
 */
void 
ReleaseMetadataBlock(int block_num, uint32_t first_block_id, uint32_t last_block_id)
{
    Insist(block_num > 0);
    Insist(NEXT_BLOCK_ID(last_block_id) == END_OF_BLOCK);

    NEXT_BLOCK_ID(last_block_id) = FREE_BLOCK_HEAD;
    FREE_BLOCK_HEAD = first_block_id; 

    FREE_BLOCK_NUM += block_num; 
}

/*
 *  Transfer hdfs block info to uint64 and put it into related hash table
 */
uint64_t 
SetMetadataBlockInfo(MetadataBlockInfoType type, char **infos, uint32_t num)
{
    Insist(num <= 4);
  
    uint64_t result = 0;
    uint64_t entry_idx = 0;
    bool found; 
    int i;    
    BlockInfoKey key;
    BlockInfoEntry *entry;
    HTAB *htab = NULL;

    RevertBlockInfoKey rkey;
    RevertBlockInfoEntry *rentry;
    HTAB *rhtab = NULL;

    uint32_t *cur_idx = NULL;

    switch (type)
    {
    case METADATA_BLOCK_INFO_TYPE_HOSTS:
        htab = BlockHostsMap;
        rhtab = RevertBlockHostsMap;
        cur_idx = &MetadataCacheSharedDataInstance->cur_hosts_idx;
        break;

    case METADATA_BLOCK_INFO_TYPE_NAMES:
        htab = BlockNamesMap;
        rhtab = RevertBlockNamesMap;
        cur_idx = &MetadataCacheSharedDataInstance->cur_names_idx;
        break;
    
    case METADATA_BLOCK_INFO_TYPE_TOPOLOGYPATHS:
        htab = BlockTopologyPathsMap;
        rhtab = RevertBlockTopologyPathsMap;
        cur_idx = &MetadataCacheSharedDataInstance->cur_topologyPaths_idx;
        break;

    default:
        return 0;
    }

    for (i=0;i<num;i++)
    {
        if (strlen(infos[i]) >= MAX_BLOCK_INFO_LEN)
        {
            elog(ERROR, "The length of hostname must little than %d", MAX_BLOCK_INFO_LEN);
        }

        memset(key.block_info, 0, MAX_BLOCK_INFO_LEN);
        snprintf(key.block_info, MAX_BLOCK_INFO_LEN, "%s", infos[i]);
        entry = (BlockInfoEntry *)hash_search(htab, (void *)&key, HASH_ENTER_NULL, &found);

        if (!found)
        {
            (*cur_idx)++;
            entry->index = (*cur_idx);
            
            rkey.index = (*cur_idx);
            rentry = hash_search(rhtab, (void *)&rkey, HASH_ENTER_NULL, &found);
            memset(rentry->block_info, 0, MAX_BLOCK_INFO_LEN);
            snprintf(rentry->block_info, MAX_BLOCK_INFO_LEN, "%s", infos[i]);
        }
        entry_idx = entry->index;
        result |= (entry_idx << (i * BLOCK_INFO_BIT_NUM));
    }

    return result;

}

/*
 *  Get uint64 hdfs block info by index and transfer it to string
 */
char *
GetMetadataBlockInfo(MetadataBlockInfoType type, uint64_t infos, int index)
{
    Insist(index >= 0 && index < 4);
 
    RevertBlockInfoKey rkey;
    RevertBlockInfoEntry *rentry;
    HTAB *rhtab = NULL;
    uint64_t mask;
    bool found;

    switch (type)
    {
    case METADATA_BLOCK_INFO_TYPE_HOSTS:
        rhtab = RevertBlockHostsMap;
        break;

    case METADATA_BLOCK_INFO_TYPE_NAMES:
        rhtab = RevertBlockNamesMap;
        break;
    
    case METADATA_BLOCK_INFO_TYPE_TOPOLOGYPATHS:
        rhtab = RevertBlockTopologyPathsMap;
        break;

    default:
        return NULL;
    }

    mask = 0x000000000000ffff;
    mask = mask << (index * BLOCK_INFO_BIT_NUM);
    rkey.index = (mask & infos) >> (index * BLOCK_INFO_BIT_NUM);

    rentry = (RevertBlockInfoEntry *)hash_search(rhtab, (void *)&rkey, HASH_FIND, &found);
    if (!found) {
        return NULL;
    }
    return rentry->block_info;
}

/*
 *  Get hdfs file block locations for specfic file size, return hdfs block num and cache hit ratio
 */
BlockLocation *GetHdfsFileBlockLocations(const HdfsFileInfo *file_info, uint64_t filesize, int *block_num, double *hit_ratio)
{
    Insist(file_info != NULL);

    if (0 == filesize)
    {
        // empty file
        *block_num = 0;
        *hit_ratio = 0;
        return NULL;
    }

    MetadataCacheEntry *cache_entry = NULL;
    BlockLocation *locations = NULL;

    LWLockAcquire(MetadataCacheLock, LW_SHARED);

    cache_entry = MetadataCacheExists(file_info);
    if (!cache_entry)
    {
        // Cache Not Hit
        LWLockRelease(MetadataCacheLock); 

        elog(DEBUG1, "[MetadataCache] GetHdfsFileBlockLocations NOT HIT CACHE. filename:%s filesize:"INT64_FORMAT"", 
                                file_info->filepath, 
                                filesize);

        locations = GetHdfsFileBlockLocationsNoCache(file_info, filesize, block_num);
        *hit_ratio = 0;
    }
    else
    {
        elog(DEBUG1, "[MetadataCache] GetHdfsFileBlockLocations HIT CACHE. filename:%s filesize:"INT64_FORMAT", \
                                cache_info[filesize:"INT64_FORMAT" block_num:%u first_block_id:%u last_block_id:%u]",
                                file_info->filepath, 
                                filesize, 
                                cache_entry->file_size, 
                                cache_entry->block_num, 
                                cache_entry->first_block_id, 
                                cache_entry->last_block_id);
        
        if (filesize <= cache_entry->file_size)
        {
            // Cache Hit Fully
            locations = GetHdfsFileBlockLocationsFromCache(cache_entry, filesize, block_num);
            *hit_ratio = 1.0;    

            LWLockRelease(MetadataCacheLock); 
        } 
        else 
        {
            /*
            // Cache Hit Partly
            if (cache_entry->block_num <= 1)
            {
                // only one file, re-fetch 
            */
            
                // re-fetch file's all block locations, because hdfs will get incorrect result when fetch partly
                LWLockRelease(MetadataCacheLock); 
        
                LWLockAcquire(MetadataCacheLock, LW_EXCLUSIVE);
                RemoveHdfsFileBlockLocations(file_info);
                LWLockRelease(MetadataCacheLock); 
                
                locations = GetHdfsFileBlockLocationsNoCache(file_info, filesize, block_num);
                *hit_ratio = 0;
            /*
            }
            else
            {
                LWLockRelease(MetadataCacheLock); 
                
                // fetch extra hdfs block locations and append to cache
                locations = AppendHdfsFileBlockLocationsToCache(file_info, cache_entry, filesize, block_num, hit_ratio);
            }
            */
        }
    }
    
    return locations;
}

/*
 *  Get hdfs file block locations from Hadoop HDFS and put the result into metadata cache
 */
BlockLocation *
GetHdfsFileBlockLocationsNoCache(const HdfsFileInfo *file_info, uint64_t filesize, int *block_num)
{
    BlockLocation *hdfs_locations = NULL; 
    BlockLocation *locations = NULL; 
    MetadataCacheEntry *entry = NULL;

    // 1. fetch hdfs block locations
    hdfs_locations = HdfsGetFileBlockLocations(file_info->filepath, filesize, block_num);
    if ((NULL == hdfs_locations) || (0 == *block_num))
    {
        elog(DEBUG1, "[MetadataCache] GetHdfsFileBlockLocationsNoCache fetch hdfs block locatons fail. filename:%s filesize:"INT64_FORMAT" block_num:%d",
                                file_info->filepath, 
                                filesize, 
                                *block_num);
        goto done;
    }
    
    elog(DEBUG1, "[MetadataCache] GetHdfsFileBlockLocationsNoCache fetch hdfs block locatons successfully. filename:%s filesize:"INT64_FORMAT" block_num:%d",
                                file_info->filepath, 
                                filesize, 
                                *block_num);

    // 2. insert fetch results into cache
    LWLockAcquire(MetadataCacheLock, LW_EXCLUSIVE);

    // 3. generate result block locations
    locations = CreateHdfsFileBlockLocations(hdfs_locations, *block_num);

    entry = MetadataCacheNew(file_info, filesize, hdfs_locations, *block_num); 
    if (NULL == entry)
    {
        LWLockRelease(MetadataCacheLock);
        elog(DEBUG1, "[MetadataCache] GetHdfsFileBlockLocationsNoCache put hdfs block locations info cache fail. filename:%s filesize:"INT64_FORMAT" block_num:%d",
                                file_info->filepath, 
                                filesize, 
                                *block_num);
        goto done;
    }
    entry->last_access_time = entry->create_time = time(NULL);

    LWLockRelease(MetadataCacheLock);

done:
    if (hdfs_locations)
    {
        HdfsFreeFileBlockLocations(hdfs_locations, *block_num);
        hdfs_locations = NULL;
    }

    return locations;
}

/*
 *  Get hdfs file block locations from metadata cache 
 */
BlockLocation *
GetHdfsFileBlockLocationsFromCache(MetadataCacheEntry *entry, uint64_t filesize, int *block_num)
{
    Insist(NULL != entry);
    Insist(entry->file_size >= filesize);

    int i, j, k;
    int last_block_length = 0;
    BlockLocation *locations = NULL;
    char *metadata_block_info = NULL;

    if (entry->file_size == filesize)
    {
        *block_num = entry->block_num;
        last_block_length = GET_BLOCK(entry->last_block_id)->length;
    }
    else
    {
        last_block_length = filesize;
        j = entry->first_block_id;
        *block_num = 0;
        while (true) 
        {
            MetadataHdfsBlockInfo *block_info = GET_BLOCK(j);
            (*block_num)++;

            if (filesize <= block_info->length)
            {
                last_block_length = filesize;
                break; 
            }
            else
            {
                filesize -= block_info->length;
                j = NEXT_BLOCK_ID(j);
            }
        }
    }

    locations = (BlockLocation *)palloc(sizeof(BlockLocation) * (*block_num)); 
    if (NULL == locations)
    {
        return NULL;
    }

    k = entry->first_block_id;
    for (i=0;i<(*block_num);i++)
    {
        MetadataHdfsBlockInfo *block_info = GET_BLOCK(k);

        locations[i].corrupt = 0;
        locations[i].numOfNodes = block_info->node_num; 
        locations[i].hosts = (char **)palloc(sizeof(char *) * locations[i].numOfNodes);
        locations[i].names= (char **)palloc(sizeof(char *) * locations[i].numOfNodes);
        locations[i].topologyPaths = (char **)palloc(sizeof(char *) * locations[i].numOfNodes);

        for (j=0;j<locations[i].numOfNodes;j++)
        {
            metadata_block_info = GetMetadataBlockInfo(METADATA_BLOCK_INFO_TYPE_HOSTS, block_info->hosts, j); 
            if (NULL == metadata_block_info)
            {
                goto err;
            }
            locations[i].hosts[j] = pstrdup(metadata_block_info);

            metadata_block_info = GetMetadataBlockInfo(METADATA_BLOCK_INFO_TYPE_NAMES, block_info->names, j);
            if (NULL == metadata_block_info)
            {
                goto err;
            }
            locations[i].names[j] = pstrdup(metadata_block_info);
            
            metadata_block_info = GetMetadataBlockInfo(METADATA_BLOCK_INFO_TYPE_TOPOLOGYPATHS, block_info->topologyPaths, j);
            if (NULL == metadata_block_info)
            {
                goto err;
            }
            locations[i].topologyPaths[j] = pstrdup(metadata_block_info);
        }
        locations[i].offset = block_info->offset;
        
        if (i == (*block_num) -1)
        {
            // last block
            locations[i].length = last_block_length;
        }
        else
        {
            locations[i].length = block_info->length;
        }
        k = NEXT_BLOCK_ID(k);
    }

    entry->last_access_time = time(NULL);

    return locations;

err:
    FreeHdfsFileBlockLocations(locations, *block_num);

    return NULL;
}

/*
 *  Get hdfs block locations from cache and fetch extra part from hadoop hdfs
 */
/*
BlockLocation *
AppendHdfsFileBlockLocationsToCache(const HdfsFileInfo *file_info, MetadataCacheEntry *entry, uint64_t filesize, int *block_num, double *hit_ratio)
{
    Insist(file_info != NULL);
    Insist(entry != NULL);
    Insist(entry->block_num > 1);
    Insist(entry->file_size < filesize);
    
    int block_size = 0;
    uint64_t entry_file_size = 0;
    BlockLocation *hdfs_locations = NULL; 
    BlockLocation *locations_from_cache = NULL; 
    BlockLocation *locations_from_hdfs = NULL;
    BlockLocation *locations;
    uint32_t extra_first_block_id = 0;
    uint32_t extra_last_block_id = 0;
    uint32_t extra_block_num = 0;
    int i, j;
    int hit_block_num = entry->block_num;

    LWLockAcquire(MetadataCacheLock, LW_SHARED);

    block_size = GET_BLOCK(entry->first_block_id)->length;
    entry_file_size = entry->file_size;

    LWLockRelease(MetadataCacheLock);
   
    // 1. fetch extra hdfs block locations
    hdfs_locations = HdfsGetFileBlockLocations2(file_info->filepath, entry_file_size, filesize - entry_file_size, block_num);
    if (!hdfs_locations)
    {
        elog(DEBUG1, "[MetadataCache] AppendHdfsFileBlockLocationsToCache fetch extra hdfs block locations fail. \
                        filename:%s filesize:"INT64_FORMAT","INT64_FORMAT" block_num:%d",
                            file_info->filepath, 
                            entry_file_size, 
                            filesize, 
                            *block_num);
        goto done;
    }
    
    elog(DEBUG1, "[MetadataCache] AppendHdfsFileBlockLocationsToCache fetch extra hdfs block locations successfully. \
                        filename:%s filesize:"INT64_FORMAT","INT64_FORMAT" block_num:%d",
                            file_info->filepath, 
                            entry_file_size, 
                            filesize, 
                            *block_num);
   
    LWLockAcquire(MetadataCacheLock, LW_EXCLUSIVE);

    locations_from_cache = GetHdfsFileBlockLocationsFromCache(entry, entry->file_size, (int *)&entry->block_num);
    locations_from_hdfs = CreateHdfsFileBlockLocations(hdfs_locations, *block_num);

    if (NULL == locations_from_cache || NULL == locations_from_hdfs)
    {
        goto done;
    }

    if (GET_BLOCK(entry->last_block_id)->length < block_size)
    {
        locations = MergeHdfsFileBlockLocations(locations_from_cache, entry->block_num-1, locations_from_hdfs, *block_num);
    }
    else
    {
        locations = MergeHdfsFileBlockLocations(locations_from_cache, entry->block_num, locations_from_hdfs, *block_num);
    }
    FreeHdfsFileBlockLocations(locations_from_cache, entry->block_num);
    FreeHdfsFileBlockLocations(locations_from_hdfs, *block_num);

    if (*block_num > FREE_BLOCK_NUM)
    {
        elog(DEBUG1, "[MetadataCache] AppendHdfsFileBlockLocationsToCache not enough free block. \
                        filename:%s filesize:"INT64_FORMAT","INT64_FORMAT" block_num:%d",
                            file_info->filepath, 
                            entry->file_size, 
                            filesize, 
                            *block_num);
        goto done;
    }

    if (GET_BLOCK(entry->last_block_id)->length < block_size)
    {
        // merge last block
        extra_block_num = *block_num - 1;
        if (extra_block_num > 0)
        {
            AllocMetadataBlock(extra_block_num, &extra_first_block_id, &extra_last_block_id);
        }
        j = entry->last_block_id;
        hit_block_num--;
    }
    else
    {
        extra_block_num = *block_num;
        AllocMetadataBlock(extra_block_num, &extra_first_block_id, &extra_last_block_id);
        j = extra_first_block_id;
    }

    entry->file_size = filesize;
    entry->block_num += extra_block_num;
    if (extra_block_num > 0)
    {
        NEXT_BLOCK_ID(entry->last_block_id) = extra_first_block_id;
        entry->last_block_id = extra_last_block_id;
    }

    // copy extra blocks into cache
    for (i=0;i<(*block_num);i++)
    {
        MetadataHdfsBlockInfo *block = GET_BLOCK(j);

        block->node_num = hdfs_locations[i].numOfNodes < 4 ? hdfs_locations[i].numOfNodes : 4;
        block->hosts = SetMetadataBlockInfo(METADATA_BLOCK_INFO_TYPE_HOSTS, hdfs_locations[i].hosts, block->node_num);
        block->names = SetMetadataBlockInfo(METADATA_BLOCK_INFO_TYPE_NAMES, hdfs_locations[i].names, block->node_num);
        block->topologyPaths = SetMetadataBlockInfo(METADATA_BLOCK_INFO_TYPE_TOPOLOGYPATHS, hdfs_locations[i].topologyPaths, block->node_num);
        block->length = hdfs_locations[i].length;
        block->offset = hdfs_locations[i].offset;
        
        j = NEXT_BLOCK_ID(j);
    }

done:
    if (hdfs_locations)
    {
        HdfsFreeFileBlockLocations(hdfs_locations, *block_num);
        hdfs_locations = NULL;
    }
    
    *block_num = entry->block_num;
    *hit_ratio = (hit_block_num * 1.0) / (*block_num);
    LWLockRelease(MetadataCacheLock);

    return locations;
}
*/

/*
 * Free hdf file block location which create from GetHdfsFileBlockLocations
 */
void 
FreeHdfsFileBlockLocations(BlockLocation *locations, int block_num)
{
    Insist(NULL != locations);
    Insist(block_num >= 0);

    int i, j;
    
    for (i=0;i<block_num;i++)
    {
        for (j=0;j<locations[i].numOfNodes;j++)
        {
            if (locations[i].hosts[j])
            {
                pfree(locations[i].hosts[j]);
            }

            if (locations[i].names[j])
            {
                pfree(locations[i].names[j]);
            }
            
            if (locations[i].topologyPaths[j])
            {
                pfree(locations[i].topologyPaths[j]);
            }
        }

        if (locations[i].hosts)
        {
            pfree(locations[i].hosts);
        }

        if (locations[i].names)
        {
            pfree(locations[i].names);
        }
        
        if (locations[i].topologyPaths)
        {
            pfree(locations[i].topologyPaths);
        }
    }
    pfree(locations);
    locations = NULL; 
}

/*
 *  Create metadata hdfs file block locations from original hdfs block locations
 */
BlockLocation *
CreateHdfsFileBlockLocations(BlockLocation *hdfs_locations, int block_num)
{
    Insist(hdfs_locations != NULL);
    Insist(block_num > 0);

    int i, j;
    BlockLocation *locations = NULL;

    locations = (BlockLocation *)palloc(sizeof(BlockLocation) * block_num);
    if (NULL == locations)
    {
        return NULL;
    }
    for (i=0;i<block_num;i++)
    {
        locations[i].corrupt = 0;
        locations[i].numOfNodes = hdfs_locations[i].numOfNodes; 

        locations[i].hosts = (char **)palloc(sizeof(char *) * locations[i].numOfNodes);
        locations[i].names= (char **)palloc(sizeof(char *) * locations[i].numOfNodes);
        locations[i].topologyPaths = (char **)palloc(sizeof(char *) * locations[i].numOfNodes);
        for (j=0;j<locations[i].numOfNodes;j++)
        {
            locations[i].hosts[j] = pstrdup(hdfs_locations[i].hosts[j]);
            locations[i].names[j] = pstrdup(hdfs_locations[i].names[j]);
            locations[i].topologyPaths[j] = pstrdup(hdfs_locations[i].topologyPaths[j]);
        }
        locations[i].length = hdfs_locations[i].length;
        locations[i].offset = hdfs_locations[i].offset;
    }

    return locations;
}

/*
BlockLocation *
MergeHdfsFileBlockLocations(BlockLocation *locations1, int block_num1, BlockLocation *locations2, int block_num2)
{
    Insist(locations1 != NULL && locations2 != NULL);
    Insist(block_num1 > 0 && block_num2 > 0);

    int i, j, k;
    BlockLocation *locations = NULL;
    BlockLocation *tmp_locations = NULL;

    locations = (BlockLocation *)palloc(sizeof(BlockLocation) * (block_num1 + block_num2));
    if (NULL == locations)
    {
        return NULL;
    }
    for (k=0;k<(block_num1+block_num2);k++)
    {
        if (k < block_num1) 
        {
            i = k;
            tmp_locations = locations1;
        }
        else
        {
            i = k - block_num1;
            tmp_locations = locations2;
        }

        locations[k].corrupt = 0;
        locations[k].numOfNodes = tmp_locations[i].numOfNodes; 

        locations[k].hosts = (char **)palloc(sizeof(char *) * locations[k].numOfNodes);
        locations[k].names= (char **)palloc(sizeof(char *) * locations[k].numOfNodes);
        locations[k].topologyPaths = (char **)palloc(sizeof(char *) * locations[k].numOfNodes);
        for (j=0;j<locations[k].numOfNodes;j++)
        {
            locations[k].hosts[j] = pstrdup(tmp_locations[i].hosts[j]);
            locations[k].names[j] = pstrdup(tmp_locations[i].names[j]);
            locations[k].topologyPaths[j] = pstrdup(tmp_locations[i].topologyPaths[j]);
        }
        locations[k].length = tmp_locations[i].length;
        locations[k].offset = tmp_locations[i].offset;
    }

    return locations;

}
*/

void 
DumpHdfsFileBlockLocations(BlockLocation *locations, int block_num)
{
    int i, j;
    char msg[1024];

    for (i=0;i<block_num;i++)
    {
        memset(msg, 0, 1024);
        snprintf(msg, MAX_BLOCK_INFO_LEN, "node_num:%d ", locations[i].numOfNodes);
        for (j=0;j<locations[i].numOfNodes;j++)
        {
            snprintf(msg + strlen(msg), MAX_BLOCK_INFO_LEN, "host%d:%s ", j, locations[i].hosts[j]);
            snprintf(msg + strlen(msg), MAX_BLOCK_INFO_LEN, "name%d:%s ", j, locations[i].names[j]);
            snprintf(msg + strlen(msg), MAX_BLOCK_INFO_LEN, "topologyPath%d:%s ", j, locations[i].topologyPaths[j]);
        }
        snprintf(msg + strlen(msg), MAX_BLOCK_INFO_LEN, "offset:"INT64_FORMAT" ", locations[i].offset);
        snprintf(msg + strlen(msg), MAX_BLOCK_INFO_LEN, "length:"INT64_FORMAT" ", locations[i].length);
        elog(DEBUG5, "[MetadataCache] DumpHdfsFileBlockLocations block%d %s", i, msg);
    }
        
    elog(DEBUG5, "[MetadataCache] DumpHdfsFileBlockLocations block_num:%d", block_num);
}

/*
 * Transfer hdfs block locations to cache format and put it into cache
 */
MetadataCacheEntry *
MetadataCacheNew(const HdfsFileInfo *file_info, uint64_t filesize, BlockLocation *hdfs_locations, int block_num)
{
    Insist(file_info != NULL);
    Insist(hdfs_locations != NULL);
    Insist(block_num > 0);

    int i, j;
    MetadataCacheEntry *entry = NULL;

    if (block_num > FREE_BLOCK_NUM)
    {
        elog(DEBUG1, "[Metadata] MetadataCacheNew not enough free block. \
                    filename:%s filesize:"INT64_FORMAT" block_num:%d free_block_num:%d",
                        file_info->filepath, 
                        filesize, 
                        block_num, 
                        FREE_BLOCK_NUM);
        return NULL;
    }
    
    entry = MetadataCacheEnter(file_info);
    if (!entry)
    {
        elog(DEBUG1, "[MetadataCache] MetadataCacheNew enter cache fail. \
                    filename:%s filesize:"INT64_FORMAT" block_num:%d",
                        file_info->filepath, 
                        filesize, 
                        block_num);
        return NULL;
    }

    entry->file_size = filesize;
    entry->block_num = block_num;
    
    AllocMetadataBlock(entry->block_num, &entry->first_block_id, &entry->last_block_id);

    // copy hdfs blocks into cache
    j = entry->first_block_id; 
    for (i=0;i<entry->block_num;i++)
    {
        MetadataHdfsBlockInfo *block = GET_BLOCK(j);
        
        block->node_num = hdfs_locations[i].numOfNodes < 4 ? hdfs_locations[i].numOfNodes : 4;
        block->hosts = SetMetadataBlockInfo(METADATA_BLOCK_INFO_TYPE_HOSTS, hdfs_locations[i].hosts, block->node_num);
        block->names = SetMetadataBlockInfo(METADATA_BLOCK_INFO_TYPE_NAMES, hdfs_locations[i].names, block->node_num);
        block->topologyPaths = SetMetadataBlockInfo(METADATA_BLOCK_INFO_TYPE_TOPOLOGYPATHS, hdfs_locations[i].topologyPaths, block->node_num);
        block->length = hdfs_locations[i].length;
        block->offset = hdfs_locations[i].offset;
       
        j = NEXT_BLOCK_ID(j);
    }
   
    return entry;
}

/*
 *  Remove entry from metadata cache
 */
void 
RemoveHdfsFileBlockLocations(const HdfsFileInfo *file_info)
{
    Insist(NULL != file_info);

    MetadataCacheEntry *entry = MetadataCacheExists(file_info);
    if (NULL == entry)
    {
        elog(DEBUG1, "[MetadataCache] RemoveHdfsFileBlockLocations file not exists. filename:%s", file_info->filepath);
    }
    else
    {
        elog(DEBUG1, "[Metadata] RemoveHdfsFileBlockLocations, filename:%s block_num:%d", 
                    file_info->filepath,
                    entry->block_num); 
        ReleaseMetadataBlock(entry->block_num, entry->first_block_id, entry->last_block_id);
        MetadataCacheRemove(file_info);
    }
}


/*
 *  Metadata Cache UDF
 *
 *  Clear all content in metadata cache
 */
extern Datum gp_metadata_cache_clear(PG_FUNCTION_ARGS)
{
    HASH_SEQ_STATUS hstat;
    MetadataCacheEntry *entry;
    bool found;
    long entry_num = 0;

    LWLockAcquire(MetadataCacheLock, LW_EXCLUSIVE);
    
    entry_num = hash_get_num_entries(MetadataCache);
    if (MetadataCacheRefreshList)
    {
        list_free(MetadataCacheRefreshList);
        MetadataCacheRefreshList = NULL;
    }

    hash_seq_init(&hstat, MetadataCache);
    while ((entry = (MetadataCacheEntry *)hash_seq_search(&hstat)) != NULL)
    {
        ReleaseMetadataBlock(entry->block_num, entry->first_block_id, entry->last_block_id);
        hash_search(MetadataCache, (void *)&entry->key, HASH_REMOVE, &found);
    }
    
    LWLockRelease(MetadataCacheLock);

    char message[1024] = {0};
    snprintf(message, 1024, "Metadata cache clear %ld items", entry_num);
    PG_RETURN_TEXT_P(cstring_to_text(message));    
}

/*
 *  Metadata Cache UDF
 *
 *  Get entry number of metadata cache 
 */
extern Datum gp_metadata_cache_current_num(PG_FUNCTION_ARGS)
{
    LWLockAcquire(MetadataCacheLock, LW_SHARED);
    int64 num = hash_get_num_entries(MetadataCache); 
    LWLockRelease(MetadataCacheLock);

    PG_RETURN_INT64(num);
}
/*
 *  Metadata Cache UDF
 *
 *  Get block number of metadata cache
 */
extern Datum gp_metadata_cache_current_block_num(PG_FUNCTION_ARGS)
{
    PG_RETURN_INT64(metadata_cache_block_capacity - FREE_BLOCK_NUM);
}
/*
 *  Metadata Cache UDF
 *
 *  Check whether an entry in the metadata cache 
 */
extern Datum gp_metadata_cache_exists(PG_FUNCTION_ARGS)
{
    Oid tablespace_oid = PG_GETARG_OID(0);
    Oid database_oid = PG_GETARG_OID(1);
    Oid relation_oid = PG_GETARG_OID(2);
    int4 segno = PG_GETARG_INT32(3);

    RelFileNode rnode;
    rnode.spcNode = tablespace_oid;
    rnode.dbNode = database_oid;
    rnode.relNode = relation_oid;
    MetadataCacheKey key;
    MetadataCacheEntry *entry;

    HdfsFileInfo *file_info = CreateHdfsFileInfo(rnode, segno);
    InitMetadataCacheKey(&key, file_info);

    LWLockAcquire(MetadataCacheLock, LW_SHARED);
    entry = MetadataCacheExists(file_info);
    LWLockRelease(MetadataCacheLock);

    DestroyHdfsFileInfo(file_info);

    PG_RETURN_BOOL(entry != NULL);
}

/*
 *  Metadata Cache UDF
 *
 *  Get entry info in the metadata cache
 */
extern Datum gp_metadata_cache_info(PG_FUNCTION_ARGS)
{
    Oid tablespace_oid = PG_GETARG_OID(0);
    Oid database_oid = PG_GETARG_OID(1);
    Oid relation_oid = PG_GETARG_OID(2);
    int4 segno = PG_GETARG_INT32(3);

    RelFileNode rnode;
    rnode.spcNode = tablespace_oid;
    rnode.dbNode = database_oid;
    rnode.relNode = relation_oid;
    MetadataCacheKey key;
    MetadataCacheEntry *entry;

    HdfsFileInfo *file_info = CreateHdfsFileInfo(rnode, segno);
    InitMetadataCacheKey(&key, file_info);

    char message[1024] = {0};
    LWLockAcquire(MetadataCacheLock, LW_SHARED);
    entry = MetadataCacheExists(file_info);
    if (!entry)
    {
        snprintf(message, 1024, "Metadata cache don't have %s", file_info->filepath);        
    }
    else
    {
        snprintf(message, 1024, "Metadata cache have %s [file_size:"INT64_FORMAT" block_num:%u first_block_id:%u last_block_id:%u create_time:%u ]",
                            file_info->filepath, 
                            entry->file_size, 
                            entry->block_num, 
                            entry->first_block_id, 
                            entry->last_block_id, 
                            entry->create_time); 
    }

    LWLockRelease(MetadataCacheLock);

    DestroyHdfsFileInfo(file_info);
    PG_RETURN_TEXT_P(cstring_to_text(message));    
}

/*
 *  Metadata Cache UDF
 *
 *  Put entry into the metadata cache
 */
extern Datum gp_metadata_cache_put_entry_for_test(PG_FUNCTION_ARGS)
{
    Oid tablespace_oid = PG_GETARG_OID(0);
    Oid database_oid = PG_GETARG_OID(1);
    Oid relation_oid = PG_GETARG_OID(2);
    int4 start = PG_GETARG_INT32(3);
    int4 end = PG_GETARG_INT32(4);

    LWLockAcquire(MetadataCacheLock, LW_EXCLUSIVE);

    int i;
    int4 stop = (start -end) / 10;
    int4 current = 0;
    int4 success = 0;
    for(i=start;i<end;i++)
    {
    	MetadataCacheKey key;
    	key.tablespace_oid = tablespace_oid;
    	key.database_oid = database_oid;
    	key.relation_oid = relation_oid;
    	key.segno = i;

    	bool found;
    	MetadataCacheEntry *entry = (MetadataCacheEntry *)hash_search(MetadataCache, (void *)&key, HASH_ENTER_NULL, &found);
        if(entry == NULL)
        {
            continue;
        }
        entry->file_size = 134217728;
        entry->block_num = 1;

        AllocMetadataBlock(entry->block_num, &entry->first_block_id, &entry->last_block_id);

    	current++;
    	success++;

    	if(current == stop)
    	{
    		current = 0;
    		LWLockRelease(MetadataCacheLock);
    		pg_usleep(1 * USECS_PER_SEC);
    	    LWLockAcquire(MetadataCacheLock, LW_EXCLUSIVE);
    	}
    }
    LWLockRelease(MetadataCacheLock);

    char message[1024] = {0};
    snprintf(message, 1024, "Metadata cache successed putting %d entries. Failed putting %d entries.", success, end - start - success);
    PG_RETURN_TEXT_P(cstring_to_text(message));
}
