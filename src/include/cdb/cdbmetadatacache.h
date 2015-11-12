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

#ifndef CDBMETADATACACHE_H
#define CDBMETADATACACHE_H

#include "utils/palloc.h"
#include "storage/fd.h"
#include "storage/itemptr.h"
#include "cdb/cdbdoublylinked.h"
#include "hdfs/hdfs.h"
#include "cdb/cdbmetadatacache_internal.h"

typedef struct HdfsFileInfo
{
    uint32_t        tablespace_oid;
    uint32_t        database_oid;
    uint32_t        relation_oid;
    int             segno;
    char            *filepath;
} HdfsFileInfo;

/*
 *  Metadata Cache Share Memroy Init
 */
Size MetadataCache_ShmemSize(void);

void MetadataCache_ShmemInit(void);

/*
 * Metadata Cache User Interfaces
 */
HdfsFileInfo *CreateHdfsFileInfo(RelFileNode rnode, int segno);

void DestroyHdfsFileInfo(HdfsFileInfo *file_info);

BlockLocation *GetHdfsFileBlockLocations(const HdfsFileInfo *file_info, uint64_t filesize, int *block_num, double *hit_ratio);

void FreeHdfsFileBlockLocations(BlockLocation *locations, int block_num);

void DumpHdfsFileBlockLocations(BlockLocation *locations, int block_num);

void RemoveHdfsFileBlockLocations(const HdfsFileInfo *file_info);

MetadataCacheEntry *MetadataCacheNew(const HdfsFileInfo *file_info, uint64_t filesize, BlockLocation *hdfs_locations, int block_num);

/*
 *  Metadata Cache Test User Interfaces
 */
BlockLocation *GetHdfsFileBlockLocationsForTest(const char *filepath, uint64_t filesize, int *block_num);

/*
 *  Metadata Cache Process Interfaces
 */ 
int metadatacache_start(void);

void MetadataCacheMain(int argc, char *argv[]);

#endif
