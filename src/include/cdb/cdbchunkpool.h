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
 * cdbchunkpool.h
 *
 * Allocates chunks of a fixed size from an underlying AsetDirectContext,
 * and keeps a freelist for recycling them.
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
