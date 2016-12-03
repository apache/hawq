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

#ifndef RESOURCE_MANAGER_UTIL_MEMORY_UTILITIES_H
#define RESOURCE_MANAGER_UTIL_MEMORY_UTILITIES_H

#include "resourcemanager/envswitch.h"

#ifndef BUILT_IN_HAWQ
#define Assert(e)       assert(e);

#define palloc(size)    malloc((size))
#define pfree(p)        free((p))
#define palloc0(size)   __palloc0((size))

void *__palloc0(int size);

#define MEMORY_CONTEXT_SWITCH_TO(newcontext) ;
#define MEMORY_CONTEXT_SWITCH_BACK 			 ;

#define MCTYPE   void *

#else

#define MCTYPE   MemoryContext

#define MEMORY_CONTEXT_SWITCH_TO(newcontext) 							\
		Assert((newcontext) != NULL);									\
		MemoryContext oldcontext = MemoryContextSwitchTo((newcontext));

#define MEMORY_CONTEXT_SWITCH_BACK 										\
		MemoryContextSwitchTo(oldcontext);

#endif

/* Type explicit conversion. */
#define TYPCONVERT(type,val) ((type)((unsigned long)(val)))

/* Memory alignment. */
#define __SIZE_ALIGN16(s) ((((s)+1)>>1)<<1)
#define __SIZE_ALIGN32(s) ((((s)+3)>>2)<<2)
#define __SIZE_ALIGN64(s) ((((s)+7)>>3)<<3)

#define rm_palloc(context,size) 											   \
		_rm_palloc(context,size,__FILE__,__LINE__,__FUNCTION__)
#define rm_palloc0(context,size)											   \
		_rm_palloc0(context,size,__FILE__,__LINE__,__FUNCTION__)
#define rm_repalloc(context,ptr,newsize)									   \
		_rm_repalloc(context,ptr,newsize,__FILE__,__LINE__,__FUNCTION__)
#define rm_pfree(context,ptr)												   \
		_rm_pfree(context,ptr,__FILE__,__LINE__,__FUNCTION__)

void *_rm_palloc   (MCTYPE context, uint32_t size,
					const char *filename, int line, const char *function);
void *_rm_palloc0  (MCTYPE context, uint32_t size,
					const char *filename, int line, const char *function);
void *_rm_repalloc (MCTYPE context, void *ptr,
					uint32_t newsize, const char *filename, int line, const char *function);
void  _rm_pfree    (MCTYPE context, void *ptr,
					const char *filename, int line, const char *function);

/******************************************************************************
 *  Self-grow buffer.
 *
 *  |<--------------------Size--------------------->|
 *  +-----------------------------------------------+
 *  |XXXXXXXXXXXXXXXXXXXXXXXXXXXXX                  |
 *  +-----------------------------------------------+
 *  ^                            ^
 *  |                            |
 *  Buffer                       Cursor
 *
 ******************************************************************************/
struct SelfMaintainBufferData
{
	MCTYPE  Context;
	char   *Buffer;
	int32_t Size;
	int32_t Cursor;
};

#define SMBUFF_CONTENT(buffptr)		 ((buffptr)->Buffer)
#define SMBUFF_HEAD(ptrtype,buffptr) ((ptrtype)((buffptr)->Buffer))

typedef struct SelfMaintainBufferData  SelfMaintainBufferData;
typedef struct SelfMaintainBufferData *SelfMaintainBuffer;

#define SELFMAINTAINBUFFER_MAX_SIZE 1024*1024*512
#define SELFMAINTAINBUFFER_DEF_SIZE 1024

SelfMaintainBuffer createSelfMaintainBuffer(MCTYPE context);

void initializeSelfMaintainBuffer(SelfMaintainBuffer buffer, MCTYPE context);

char   *getSMBCursor(SelfMaintainBuffer buffer);
int32_t getSMBContentSize(SelfMaintainBuffer buffer);

void resetSelfMaintainBuffer(SelfMaintainBuffer buffer);
void resetSelfMaintainBufferCursor(SelfMaintainBuffer buffer, int newcursor);

void appendSelfMaintainBuffer(SelfMaintainBuffer buffer,
							  const char *source,
							  uint32_t size);

#define appendSMBVar(buffer, val)                                     		   \
		appendSelfMaintainBuffer((buffer), (char *)(&(val)), (sizeof(val)))
#define appendSMBTyp(buffer, ptr, typ)										   \
		appendSelfMaintainBuffer((buffer), (char *)(ptr), sizeof(typ))
#define appendSMBStr(buffer, ptr)											   \
		appendSelfMaintainBuffer((buffer), (char *)(ptr), strlen((ptr))+1)
#define appendSMBSimpStr(buffer, str)										   \
		{																	   \
			appendSelfMaintainBuffer((buffer), 								   \
									 (char *)((str)->Str),					   \
									 (str)->Len);							   \
			prepareSelfMaintainBuffer(buffer, 1, true);						   \
			jumpforwardSelfMaintainBuffer(buffer, 1);						   \
		}

void prepareSelfMaintainBuffer(SelfMaintainBuffer buffer,
							   uint32_t size,
							   bool setzero);

void jumpforwardSelfMaintainBuffer(SelfMaintainBuffer buffer, uint32_t offset);

/* Destroy the content of this buffer. */
void destroySelfMaintainBuffer(SelfMaintainBuffer buffer);

/* Free the buffer instance including its content. */
void deleteSelfMaintainBuffer(SelfMaintainBuffer buffer);

void appendSelfMaintainBufferTill64bitAligned(SelfMaintainBuffer buffer);

void shiftLeftSelfMaintainBuffer(SelfMaintainBuffer buffer, int shift);
#endif /* RESOURCE_MANAGER_UTIL_MEMORY_UTILITIES_H */
