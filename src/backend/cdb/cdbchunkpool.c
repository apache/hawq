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

/*-------------------------------------------------------------------------
 *
 * cdbchunkpool.c
 *
 * Allocates chunks of a fixed size from an underlying AsetDirectContext, 
 * and keeps a freelist for recycling them.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "cdb/cdbchunkpool.h"               /* me */
#include "utils/memutils.h"                 /* AsetDirectContextCreate() etc */


#ifdef USE_ASSERT_CHECKING
/* Fill a 32 or 64 bit pointer with 32 or 64 bits of 0xDEAD avoiding warnings */
#define DEADDEAD    ((void *)(Size)((((((0xDEAD << 16) + 0xDEAD) << 16) + 0xDEAD) << 16) + 0xDEAD))
#endif


CdbChunkPool *
CdbChunkPool_Create(Size            bytesperchunk,
                    MemoryContext   parentcontext,
                    const char     *chunkcontextname)
{
    CdbChunkPool   *pool;

    /* Allocate the ChunkPool object with empty 'freechunks' collection. */
    pool = (CdbChunkPool *)CdbPtrBuf_CreateWrap(sizeof(*pool),
                                                offsetof(CdbChunkPool, freechunks),
                                                10,   /* num_initial_cells */
                                                50,   /* num_cells_expand */
                                                parentcontext);

    /* Round 'bytesperchunk' up to MAXALIGN. */
    pool->bytesperchunk = MAXALIGN(bytesperchunk);

    /*
     * Create a private memory context from which to allocate chunks.
     *
     * A special AsetDirectContext is used to obtain each chunk directly
     * from the host system's allocator (malloc/mmap/etc) without wrapping
     * it in additional header/trailer bytes of our own.
     *
     * Chunks allocated from an AsetDirectContext must not be passed to
     * pfree() or repalloc().  Chunks are to be freed only by deleting or
     * resetting the context.
     */
    pool->chunkcontext = AsetDirectContextCreate(parentcontext, chunkcontextname);

    return pool;
}                               /* CdbChunkPool_Create */


void
CdbChunkPool_Destroy(CdbChunkPool *pool)
{
    CdbPtrBuf_Reset(&pool->freechunks);
    if (pool->chunkcontext)
        MemoryContextDelete(pool->chunkcontext);
    pfree(pool);
}                               /* CdbChunkPool_Destroy */


void *
CdbChunkPool_Alloc(CdbChunkPool *pool)
{
    void   *chunk;

#ifdef USE_ASSERT_CHECKING
    if (!CdbPtrBuf_IsOk(&pool->freechunks) ||
        !MemoryContextIsValid(pool->chunkcontext))
        elog(WARNING, "Chunk pool internal error in context '%s'",
             pool->chunkcontext->name);
#endif

    if (CdbPtrBuf_IsEmpty(&pool->freechunks))
    {
        pool->nchunk++;
        chunk = MemoryContextAlloc(pool->chunkcontext, pool->bytesperchunk);
    }
    else
    {
        chunk = *CdbPtrBuf_PopCell(&pool->freechunks);
#ifdef USE_ASSERT_CHECKING
        if (!chunk ||
            ((void **)chunk)[0] != DEADDEAD ||
            ((void **)chunk)[1] != (void *)pool ||
            ((void **)((char *)chunk + pool->bytesperchunk))[-1] != DEADDEAD)
            elog(ERROR, "Chunk pool internal error in context '%s'",
                 pool->chunkcontext->name);
        ((void **)chunk)[0] = 0;
        ((void **)chunk)[1] = 0;
#endif
    }

    return chunk;
}                               /* CdbChunkPool_Alloc */


void
CdbChunkPool_Free(CdbChunkPool *pool, void *chunk)
{
#ifdef USE_ASSERT_CHECKING
    if (!CdbPtrBuf_IsOk(&pool->freechunks) ||
        !MemoryContextIsValid(pool->chunkcontext))
        elog(WARNING, "Chunk pool internal error in context '%s'",
             pool->chunkcontext->name);
    if (!chunk)
        elog(WARNING, "CdbChunkPool_Free(pool, NULL) in context '%s'",
             pool->chunkcontext->name);
    if (pool->nchunk <= CdbPtrBuf_Length(&pool->freechunks))
        elog(WARNING, "CdbChunkPool_Free to wrong pool in context '%s'",
             pool->chunkcontext->name);
    if (((void **)chunk)[0] == DEADDEAD)
        elog(WARNING, "Possible double CdbChunkPool_Free in context '%s'",
             pool->chunkcontext->name);
    ((void **)chunk)[0] = DEADDEAD;
    ((void **)chunk)[1] = pool;
    ((void **)((char *)chunk + pool->bytesperchunk))[-1] = DEADDEAD;
#endif
    CdbPtrBuf_Append(&pool->freechunks, chunk);
}                               /* CdbChunkPool_Free */


Size
CdbChunkPool_GetCurrentSpace(CdbChunkPool *pool)
{
    return (pool->nchunk - CdbPtrBuf_Length(&pool->freechunks))
                * pool->bytesperchunk;
}                               /* CdbChunkPool_GetCurrentSpace */


