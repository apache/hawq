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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/*-------------------------------------------------------------------------
 *
 * palloc.h
 *	  POSTGRES memory allocator definitions.
 *
 * This file contains the basic memory allocation interface that is
 * needed by almost every backend module.  It is included directly by
 * postgres.h, so the definitions here are automatically available
 * everywhere.	Keep it lean!
 *
 * Memory allocation occurs within "contexts".	Every chunk obtained from
 * palloc()/MemoryContextAlloc() is allocated within a specific context.
 * The entire contents of a context can be freed easily and quickly by
 * resetting or deleting the context --- this is both faster and less
 * prone to memory-leakage bugs than releasing chunks individually.
 * We organize contexts into context trees to allow fine-grain control
 * over chunk lifetime while preserving the certainty that we will free
 * everything that should be freed.  See utils/mmgr/README for more info.
 *
 *
 * Portions Copyright (c) 2007-2008, Greenplum inc
 * Portions Copyright (c) 1996-2008, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * $PostgreSQL: pgsql/src/include/utils/palloc.h,v 1.35 2006/03/05 15:59:07 momjian Exp $
 *
 *-------------------------------------------------------------------------
 */
#ifndef PALLOC_H
#define PALLOC_H

/*
 * Optional #defines for debugging...
 *
 * If CDB_PALLOC_CALLER_ID is defined, MemoryContext error and warning
 * messages (such as "out of memory" and "invalid memory alloc request
 * size") will include the caller's source file name and line number.
 * This can be useful in optimized builds where the error handler's
 * stack trace doesn't accurately identify the call site.  Overhead
 * is minimal: two extra parameters to memory allocation functions,
 * and 8 to 16 bytes per context.
 *
 * If CDB_PALLOC_TAGS is defined, every allocation from a standard
 * memory context (aset.c) is tagged with an extra 16 to 32 bytes of
 * debugging info preceding the first byte of the area.  The added
 * header fields identify the allocation call site (source file name
 * and line number).  Also each context keeps a linked list of all
 * of its allocated areas.  The dump_memory_allocation() and
 * dump_memory_allocation_ctxt() functions in aset.c may be called 
 * from a debugger to write the area headers to a file.
 */

/*
#define CDB_PALLOC_CALLER_ID
*/

#ifdef USE_ASSERT_CHECKING
#define CDB_PALLOC_TAGS
#endif

/* CDB_PALLOC_TAGS implies CDB_PALLOC_CALLER_ID */
#if defined(CDB_PALLOC_TAGS) && !defined(CDB_PALLOC_CALLER_ID)
#define CDB_PALLOC_CALLER_ID
#endif

#include "pg_trace.h"
#include "utils/memaccounting.h"

/*
 * We track last OOM time to identify culprit processes that
 * consume too much memory. For 64-bit platform we have high
 * precision time variable based on TimeStampTz. However, on
 * 32-bit platform we only have "per-second" precision.
 * OOMTimeType abstracts this different types.
 */
#if defined(__x86_64__)
typedef int64 OOMTimeType;
#else
typedef uint32 OOMTimeType;
#endif

/*
 * Type MemoryContextData is declared in nodes/memnodes.h.	Most users
 * of memory allocation should just treat it as an abstract type, so we
 * do not provide the struct contents here.
 */
typedef struct MemoryContextData *MemoryContext;

/*
 * CurrentMemoryContext is the default allocation context for palloc().
 * We declare it here so that palloc() can be a macro.	Avoid accessing it
 * directly!  Instead, use MemoryContextSwitchTo() to change the setting.
 */
extern PGDLLIMPORT MemoryContext CurrentMemoryContext;

/* GP vmem protect */
extern bool gp_mp_inited;
extern volatile OOMTimeType* segmentOOMTime;
extern volatile OOMTimeType oomTrackerStartTime;
extern volatile OOMTimeType alreadyReportedOOMTime;

/*
 * Fundamental memory-allocation operations (more are in utils/memutils.h)
 */
extern void * __attribute__((malloc)) MemoryContextAllocImpl(MemoryContext context, Size size, const char* file, const char *func, int line);
extern void * __attribute__((malloc)) MemoryContextAllocZeroImpl(MemoryContext context, Size size, const char* file, const char *func, int line);
extern void * __attribute__((malloc)) MemoryContextAllocZeroAlignedImpl(MemoryContext context, Size size, const char* file, const char *func, int line);
extern void * __attribute__((malloc)) MemoryContextReallocImpl(void *pointer, Size size, const char* file, const char *func, int line);
extern void MemoryContextFreeImpl(void *pointer, const char* file, const char *func, int sline);

#define MemoryContextAlloc(ctxt, sz) MemoryContextAllocImpl((ctxt), (sz), __FILE__, PG_FUNCNAME_MACRO, __LINE__)
#define palloc(sz) MemoryContextAlloc(CurrentMemoryContext, (sz)) 
#define ctxt_alloc(ctxt, sz) MemoryContextAlloc((ctxt), (sz)) 

#define MemoryContextAllocZero(ctxt, sz) MemoryContextAllocZeroImpl((ctxt), (sz), __FILE__, PG_FUNCNAME_MACRO, __LINE__)
#define palloc0(sz) MemoryContextAllocZero(CurrentMemoryContext, (sz))

#define repalloc(ptr, sz) MemoryContextReallocImpl(ptr, (sz), __FILE__, PG_FUNCNAME_MACRO, __LINE__)
#define pfree(ptr) MemoryContextFreeImpl(ptr, __FILE__, PG_FUNCNAME_MACRO, __LINE__)

/*
 * The result of palloc() is always word-aligned, so we can skip testing
 * alignment of the pointer when deciding which MemSet variant to use.
 * Note that this variant does not offer any advantage, and should not be
 * used, unless its "sz" argument is a compile-time constant; therefore, the
 * issue that it evaluates the argument multiple times isn't a problem in
 * practice.
 */
#define palloc0fast(sz) \
	( MemSetTest(0, (sz)) ? \
		MemoryContextAllocZeroAlignedImpl(CurrentMemoryContext, (sz), __FILE__, PG_FUNCNAME_MACRO, __LINE__) : \
		MemoryContextAllocZeroImpl(CurrentMemoryContext, (sz), __FILE__, PG_FUNCNAME_MACRO, __LINE__))

/*
 * MemoryContextSwitchTo can't be a macro in standard C compilers.
 * But we can make it an inline function when using GCC.
 */
static inline MemoryContext
MemoryContextSwitchTo(MemoryContext context)
{
	MemoryContext old = CurrentMemoryContext;

	CurrentMemoryContext = context;
	return old;
}

/*
 * These are like standard strdup() except the copied string is
 * allocated in a context, not with malloc().
 */
extern char * __attribute__((malloc)) MemoryContextStrdup(MemoryContext context, const char *string);
extern void MemoryContextStats(MemoryContext context);

#define pstrdup(str)  MemoryContextStrdup(CurrentMemoryContext, (str))

extern char *pnstrdup(const char *in, Size len);

#if defined(WIN32) || defined(__CYGWIN__)
extern void *pgport_palloc(Size sz);
extern char *pgport_pstrdup(const char *str);
extern void pgport_pfree(void *pointer);
#endif

/* Mem Protection */
extern int max_chunks_per_query;

extern PGDLLIMPORT MemoryContext TopMemoryContext;

extern bool MemoryProtection_IsOwnerThread(void);
extern void InitPerProcessOOMTracking(void);
extern void GPMemoryProtect_ShmemInit(void);
extern void GPMemoryProtect_Init(void);
extern void GPMemoryProtect_Shutdown(void);
extern void UpdateTimeAtomically(volatile OOMTimeType* time_var);

/*
 * ReportOOMConsumption
 *
 * Checks if there was any new OOM event in this segment.
 * In case of a new OOM, it reports the memory consumption
 * of the current process.
 */
#define ReportOOMConsumption()\
{\
	if (gp_mp_inited && *segmentOOMTime >= oomTrackerStartTime && *segmentOOMTime > alreadyReportedOOMTime)\
	{\
		Assert(MemoryProtection_IsOwnerThread());\
		UpdateTimeAtomically(&alreadyReportedOOMTime);\
		write_stderr("One or more query execution processes ran out of memory on this segment. Logging memory usage.");\
		MemoryAccounting_SaveToLog();\
		MemoryContextStats(TopMemoryContext);\
	}\
}

#ifdef USE_SYSV_SEMAPHORES
extern void *gp_malloc(int64 sz);
extern void *gp_calloc(int64 nmemb, int64 sz);
extern void *gp_realloc(void *ptr, int64 sz, int64 newsz);
extern void gp_free2(void *ptr, int64 sz);

/* Current available memory quota */
extern int64 gp_vmem_avail(void);

/* Current dynamic memory allocation in bytes. */
extern uint64 gp_vmem_used(void);

/* Maximum allowed dynamic memory allocation in bytes. */
extern uint64 gp_vmem_max(void);

/* Increase/Decrease memory quota in MB */
extern int gp_update_mem_quota(int mem_quota_total);

#else
#define gp_malloc(sz) malloc(sz)
#define gp_calloc(sz1, sz2) calloc((sz1), (sz2))
#define gp_realloc(ptr, sz1, sz2) realloc((ptr), (sz2))
#define gp_free2(ptr, sz) free(ptr)

/* Increase/Decrease memory quota in MB */
extern int gp_update_mem_quota(int mem_quota_total);
#endif

#endif   /* PALLOC_H */
