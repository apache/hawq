/*-------------------------------------------------------------------------
 *
 * cdbchunkpool.h
 *
 * Allocates chunks of a fixed size from an underlying AsetDirectContext,
 * and keeps a freelist for recycling them.
 *
 * Copyright (c) 2007-2008, Greenplum inc
 *
 *-------------------------------------------------------------------------
 */
#ifndef CDBCHUNKPOOL_H
#define CDBCHUNKPOOL_H

#include "cdb/cdbptrbuf.h"              /* CdbPtrBuf */

typedef struct CdbChunkPool
{
    Size                bytesperchunk;  /* chunk size in bytes */
    Size                nchunk;         /* current num chunks (in use + free) */
    CdbPtrBuf           freechunks;     /* collection of chunk ptrs to reuse */
    MemoryContext       chunkcontext;   /* alloc new chunks from this context */

    /*
     * Some extra storage is allocated following the struct to accommodate
     * the first few pointers of the 'freechunks' collection.
     */
} CdbChunkPool;


CdbChunkPool *
CdbChunkPool_Create(Size            bytesperchunk,
                    MemoryContext   parentcontext,
                    const char     *chunkcontextname);

void
CdbChunkPool_Destroy(CdbChunkPool *pool);

void *
CdbChunkPool_Alloc(CdbChunkPool *pool);

void
CdbChunkPool_Free(CdbChunkPool *pool, void *chunk);

Size
CdbChunkPool_GetCurrentSpace(CdbChunkPool *pool);

#endif   /* CDBCHUNKPOOL_H */
