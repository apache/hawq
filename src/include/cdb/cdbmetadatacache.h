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
