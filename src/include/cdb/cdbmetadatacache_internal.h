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

#ifndef CDBMETADATACACHE_INTERNAL_H
#define CDBMETADATACACHE_INTERNAL_H

#include "utils/palloc.h"
#include "storage/fd.h"
#include "storage/itemptr.h"
#include "cdb/cdbdoublylinked.h"
#include "hdfs/hdfs.h"
#include "cdb/cdbmetadatacache.h"

#define END_OF_BLOCK                -1

#define GET_BLOCK(idx)              (MetadataBlockArray + (idx))

#define FIRST_BLOCK_ID(entry)       (entry)->first_block_id
#define LAST_BLOCK_ID(entry)        (entry)->last_block_id
#define NEXT_BLOCK_ID(idx)          MetadataBlockArray[(idx)].next_block_id

#define FREE_BLOCK_NUM              (MetadataCacheSharedDataInstance->free_block_num)
#define FREE_BLOCK_HEAD             (MetadataCacheSharedDataInstance->free_block_head)

typedef struct MetadataHdfsBlockInfo
{
    uint32_t            node_num;
    uint64_t            hosts;
    uint64_t            names;
    uint64_t            topologyPaths;
    uint64_t            length;
    uint64_t            offset;
    uint32_t            next_block_id;
} MetadataHdfsBlockInfo;

typedef struct MetadataCacheSharedData 
{
    uint32_t    free_block_num;
    uint32_t    free_block_head;
    uint32_t    cur_hosts_idx;
    uint32_t    cur_names_idx;
    uint32_t    cur_topologyPaths_idx;
} MetadataCacheSharedData;

/*
 *  Metadata Cache Structure
 */
typedef struct MetadataCacheKey
{
    uint32_t            tablespace_oid;
    uint32_t            database_oid;
    uint32_t            relation_oid;
    int                 segno;
} MetadataCacheKey;

typedef struct MetadataCacheEntry
{
    MetadataCacheKey    key;
    uint64_t            file_size;
    uint32_t            block_num;
    uint32_t            first_block_id;
    uint32_t            last_block_id;
    uint32_t            create_time;
    uint32_t            last_access_time;
} MetadataCacheEntry;

/*
 *  Metadata Cache Process Structure
 */
typedef struct MetadataCacheCheckInfo
{
    MetadataCacheKey    key;
    uint64_t    file_size;
    uint32_t    block_num;
    uint32_t    create_time;
    uint32_t    last_access_time;
} MetadataCacheCheckInfo;


extern MetadataCacheSharedData  *MetadataCacheSharedDataInstance;
extern HTAB                     *MetadataCache;
extern MetadataHdfsBlockInfo    *MetadataBlockArray;

extern List                     *MetadataCacheLRUList;
extern List                     *MetadataCacheRefreshList;

BlockLocation *CreateHdfsFileBlockLocations(BlockLocation *hdfs_locations, int block_num);

#endif
