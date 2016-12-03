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

#include "utils/memutilities.h"
#include "envswitch.h"
#include "dynrm.h"
#include "utils/network_utils.h"

static char ZEROPAD[8] = {'\0','\0','\0','\0','\0','\0','\0','\0'};

#ifndef BUILT_IN_HAWQ

void *__palloc0(int size) {
    void *res = malloc(size);
    memset(res,0,size);
    return res;
}

#endif

void *_rm_palloc(MCTYPE context, uint32_t size,
				 const char *filename, int line, const char *function)
{
 	void *res = NULL;
 	if ( context != NULL ) {
		MEMORY_CONTEXT_SWITCH_TO(context)
		res = palloc(size);
		MEMORY_CONTEXT_SWITCH_BACK
		elog(DEBUG5, "RM_PALLOC %d bytes at %lx from %s:%d:%s",
					size, (unsigned long)(res), filename, line, function);
 	}
 	else {
 		res = malloc(size);
 		if (!res)
 		{
 			elog(FATAL, "_rm_palloc malloc failed, out of memory, size %d (errno %d)", size, errno);
 		}
 	}
	return res;
}

void *_rm_palloc0(MCTYPE context, uint32_t size,
				  const char *filename, int line, const char *function)
{
	void *res = NULL;
	if ( context != NULL ) {
		MEMORY_CONTEXT_SWITCH_TO(context)
		res = palloc0(size);
		MEMORY_CONTEXT_SWITCH_BACK
		elog(DEBUG5, "RM_PALLOC0 %d bytes at %lx from %s:%d:%s",
					size, (unsigned long)(res), filename, line, function);
	}
	else {
		res = malloc(size);
		if (!res)
		{
			elog(FATAL, "_rm_palloc0 malloc failed, out of memory, size %d (errno %d)", size, errno);
		}
		else
		{
			memset(res, 0, size);
		}
	}
	return res;
}

void *_rm_repalloc(MCTYPE context, void * ptr, uint32_t newsize,
				   const char *filename, int line, const char *function)
{
	void *res = NULL;
	if ( context != NULL ) {
		MEMORY_CONTEXT_SWITCH_TO(context)
		res = repalloc(ptr, newsize);
		MEMORY_CONTEXT_SWITCH_BACK
		elog(DEBUG5, "RM_REPALLOC %d bytes at %lx from %s:%d:%s",
					newsize, (unsigned long)(res), filename, line, function);
	}
	else {
		res = realloc(ptr, newsize);
		if (!res)
		{
			elog(FATAL, "_rm_repalloc realloc failed, out of memory, newsize %d (errno %d)", newsize, errno);
		}
	}
	return res;
}

void  _rm_pfree(MCTYPE context, void *ptr,
			    const char *filename, int line, const char *function)
{
	if ( context != NULL ) {
		elog(DEBUG5, "RM_PFREE at %lx from %s:%d:%s",
					(unsigned long)(ptr), filename, line, function);

		MEMORY_CONTEXT_SWITCH_TO(context)
		pfree(ptr);
		MEMORY_CONTEXT_SWITCH_BACK
	}
	else {
		free(ptr);
	}
}

SelfMaintainBuffer createSelfMaintainBuffer(MCTYPE context)
{
	SelfMaintainBuffer result = NULL;
	result = (SelfMaintainBuffer)
			 rm_palloc0(context,
			            sizeof(SelfMaintainBufferData));
	initializeSelfMaintainBuffer(result, context);
	return result;
}

void deleteSelfMaintainBuffer(SelfMaintainBuffer buffer)
{
	destroySelfMaintainBuffer(buffer);
	rm_pfree(buffer->Context, buffer);
}

void initializeSelfMaintainBuffer(SelfMaintainBuffer buffer,
								  MCTYPE context)
{
	Assert( buffer!= NULL );
	buffer->Context = context;
	buffer->Buffer  = NULL;
	buffer->Size    = -1;
	buffer->Cursor  = -1;
}

char *getSMBCursor(SelfMaintainBuffer buffer)
{
	return buffer->Buffer + buffer->Cursor + 1;
}

int32_t getSMBContentSize(SelfMaintainBuffer buffer) {
	return buffer->Cursor + 1;
}

void destroySelfMaintainBuffer(SelfMaintainBuffer buffer)
{
	Assert( buffer != NULL );
	if ( buffer->Buffer != NULL ) {
		rm_pfree(buffer->Context, buffer->Buffer);
	}
}

void resetSelfMaintainBuffer(SelfMaintainBuffer buffer)
{
	Assert( buffer != NULL );
	buffer->Cursor = -1;
}

void resetSelfMaintainBufferCursor(SelfMaintainBuffer buffer, int newcursor)
{
	Assert( buffer != NULL );
	buffer->Cursor = newcursor;
}

void appendSelfMaintainBuffer(SelfMaintainBuffer buffer,
							 const char *source,
							 uint32_t size)
{
	Assert( buffer != NULL );
	Assert( size <= SELFMAINTAINBUFFER_MAX_SIZE );

	/* Make sure the buffer is sufficient to contain the appended content. */
	if ( buffer->Buffer == NULL ) {
		buffer->Buffer = rm_palloc0( buffer->Context, size);
		buffer->Cursor = -1;
		buffer->Size   = size;
	}
	else if ( buffer->Size - buffer->Cursor - 1 < size ) {
		int newbuffersize = buffer->Cursor + 1 + size;
		Assert( newbuffersize <= SELFMAINTAINBUFFER_MAX_SIZE );
		buffer->Buffer = rm_repalloc( buffer->Context,
									  buffer->Buffer,
									  newbuffersize);
		buffer->Size = newbuffersize;
	}

	/* Append content now. */
	memcpy(buffer->Buffer+buffer->Cursor+1, source, size);
	buffer->Cursor += size;
}

void prepareSelfMaintainBuffer(SelfMaintainBuffer buffer,
							   uint32_t 		  size,
							   bool				  setzero)
{
	Assert( buffer != NULL );
	Assert( size <= SELFMAINTAINBUFFER_MAX_SIZE );

	/* Make sure the buffer is sufficient to contain expected content. */
	if ( buffer->Buffer == NULL ) {
		buffer->Buffer = rm_palloc( buffer->Context,
									size);
		buffer->Cursor = -1;
		buffer->Size   = size;
	}
	else if ( buffer->Size - buffer->Cursor - 1 < size ) {
		int newbuffersize = buffer->Cursor + 1 + size;
		Assert( newbuffersize <= SELFMAINTAINBUFFER_MAX_SIZE );
		buffer->Buffer = rm_repalloc( buffer->Context,
									  buffer->Buffer,
									  newbuffersize);
		buffer->Size = newbuffersize;
	}

	if ( setzero ) {
		memset(buffer->Buffer + buffer->Cursor + 1, 0, size);
	}
}

void jumpforwardSelfMaintainBuffer(SelfMaintainBuffer buffer, uint32_t offset)
{
	Assert( buffer != NULL );
	buffer->Cursor += offset;
}

void appendSelfMaintainBufferTill64bitAligned(SelfMaintainBuffer buffer)
{
	Assert( buffer != NULL );
	int padsize = __SIZE_ALIGN64(buffer->Cursor + 1) - buffer->Cursor - 1;
	if ( padsize > 0 )
		appendSelfMaintainBuffer(buffer, ZEROPAD, padsize);
}

void shiftLeftSelfMaintainBuffer(SelfMaintainBuffer buffer, int shift)
{
	Assert( buffer != NULL );
	Assert( shift >= 0 );
	if ( shift > 0 && buffer->Cursor + 1 < shift ) {
		resetSelfMaintainBuffer(buffer);
	}
	else {
		memmove(buffer->Buffer, buffer->Buffer + shift, buffer->Cursor + 1 - shift);
		buffer->Cursor -= shift;
	}
}
