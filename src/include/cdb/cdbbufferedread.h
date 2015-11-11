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
 * cdbbufferedread.h
 *	  Read buffers sequentially from a file efficiently.
 *
 * The client is given direct access to large read buffer for reading
 * buffers efficiency.
 *
 *-------------------------------------------------------------------------
 */
#ifndef CDBBUFFEREDREAD_H
#define CDBBUFFEREDREAD_H

#include "postgres.h"
#include "storage/fd.h"

typedef struct BufferedRead
{
	/*
	 * Init level.
	 */
	char				*relationName;

	/*
	 * Large-read memory level members.
	 */
    int32      			 maxBufferLen;
    int32      			 maxLargeReadLen;
	
	uint8                *memory;
    int32                memoryLen;

    uint8                *beforeBufferMemory;
							/*
							 * We allocate maxBufferLen bytes before largeReadMemory
							 * to support buffers that cross large read boundaries.
							 */

    uint8                *largeReadMemory;

	int64				 largeReadPosition;
    int32                largeReadLen;
							/*
							 * The position within the current file of the current read
							 * and the number of bytes read into in largeReadMemory.
							 */
	
	/*
	 * Buffer level members.
	 */	
	int32                bufferOffset;
	int32                bufferLen;
							/*
							 * The buffer offset and length within the largeReadMemory.
							 *
							 * NOTE: The currently buffer may actually start in
							 * beforeBufferMemory if bufferOffset is negative.
							 */

	/*
	 * File level members.
	 */
	File 				 file;
    char				 *filePathName;
    int64                fileLen;
	int64				 splitLen;
	/*
	 * Temporary limit support for random reading.
	 */
	bool				haveTemporaryLimitInEffect;
	int64				temporaryLimitFileLen;

} BufferedRead;

/*
 * Determines the amount of memory to supply for
 * BufferedRead given the desired buffer and
 * large read lengths.
 */
extern int32 BufferedReadMemoryLen(
    int32                maxBufferLen,
    int32                maxLargeReadLen);

/*
 * Initialize BufferedRead.
 *
 * Use the BufferedReadMemoryLen procedure to
 * determine the amount of memory to supply.
 */
extern void BufferedReadInit(
    BufferedRead         *bufferedRead,
    uint8                *memory,
    int32                memoryLen,
    int32                maxBufferLen,
    int32                maxLargeReadLen,
    char				 *relationName);

/*
 * Takes an open file handle for the next file.
 */
extern void BufferedReadSetFile(
    BufferedRead         *bufferedRead,
    File 				 file,
    char				 *filePathName,
    int64				 splitLen,
    int64                fileLen);

/*
 * Set a temporary read range in the current open segment file.
 *
 * The beginFileOffset must be to the beginning of an Append-Only Storage block.
 *
 * The afterFileOffset serves as the temporary EOF.  It will cause reads to return
 * false (no more blocks) when reached.  It must be at the end of an Append-Only Storage
 * block.
 *
 * When a read returns false (no more blocks), the temporary read range is forgotten.
 */
extern void BufferedReadSetTemporaryRange(
    BufferedRead         *bufferedRead,
	int64				  beginFileOffset,
	int64				  afterFileOffset);

/*
 * Return the position of the next read buffer in bytes.
 */
extern int64 BufferedReadNextBufferPosition(
    BufferedRead       *bufferedRead);

/*
 * Get the next, maximum buffer space for reading.
 *
 * Returns NULL when the current file has been completely read.
 */
extern uint8 *BufferedReadGetMaxBuffer(
    BufferedRead       *bufferedRead,
    int32              *nextBufferLen);

/*
 * Get the next buffer space for reading with a specified max read-ahead
 * amount.
 *
 * Returns NULL when the current file has been completely read.
 */
extern uint8 *BufferedReadGetNextBuffer(
    BufferedRead       *bufferedRead,
    int32              maxReadAheadLen,
    int32              *nextBufferLen,
    bool				isUseSplitLen);

/*
 * Grow the available length of the current buffer.
 *
 * NOTE: The buffer address returned can be different, even for previously
 * examined buffer data.  In other words, don't keep buffer pointers in
 * the buffer region.  Use offsets to re-establish pointers after this call.
 *
 * If the current file has been completely read, availableLen will remain
 * the current value.
 */
uint8 *BufferedReadGrowBuffer(
    BufferedRead       *bufferedRead,
    int32              newMaxReadAheadLen,
    int32              *growBufferLen,
    bool 				isUseSplitLen);

/*
 * Return the address of the current read buffer.
 */
uint8 *BufferedReadGetCurrentBuffer(
    BufferedRead       *bufferedRead);

/*
 * Return the current buffer's start position.
 */
int64 BufferedReadCurrentPosition(
    BufferedRead       *bufferedRead);

/*
 * Finishes the current file for reading.  Caller is resposible for closing
 * the file afterwards.
 */
extern void BufferedReadCompleteFile(
    BufferedRead       *bufferedRead);

/*
 * Finish with reading all together.
 */
extern void BufferedReadFinish(
    BufferedRead *bufferedRead);

#endif   /* CDBBUFFEREDREAD_H */

