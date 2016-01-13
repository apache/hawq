/*-------------------------------------------------------------------------
 *
 * inval.h
 *	  POSTGRES cache invalidation dispatcher definitions.
 *
 *
 * Portions Copyright (c) 1996-2008, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * $PostgreSQL: pgsql/src/include/utils/inval.h,v 1.39 2006/07/13 17:47:02 momjian Exp $
 *
 *-------------------------------------------------------------------------
 */
#ifndef INVAL_H
#define INVAL_H

#include "access/htup.h"
#include "utils/rel.h"

typedef void (*CacheCallbackFunction) (Datum arg, Oid relid);


extern void AcceptInvalidationMessages(void);

extern void AtStart_Inval(void);

extern void AtSubStart_Inval(void);

extern void AtEOXact_Inval(bool isCommit);

extern void AtEOSubXact_Inval(bool isCommit);

extern void AtPrepare_Inval(void);

extern void PostPrepare_Inval(void);

extern void CommandEndInvalidationMessages(void);

extern void CacheInvalidateHeapTuple(Relation relation, HeapTuple tuple, SysCacheInvalidateAction action);

extern void CacheInvalidateRelcache(Relation relation);

extern void CacheInvalidateRelcacheByTuple(HeapTuple classTuple);

extern void CacheInvalidateRelcacheByRelid(Oid relid);

extern void CacheRegisterSyscacheCallback(int cacheid,
							  CacheCallbackFunction func,
							  Datum arg);

extern void CacheRegisterRelcacheCallback(CacheCallbackFunction func,
							  Datum arg);

extern void inval_twophase_postcommit(TransactionId xid, uint16 info,
						  void *recdata, uint32 len);

extern void ResetSystemCaches(void);

/* Enum for system cache invalidation mode */
typedef enum SysCacheFlushForce
{
	SysCacheFlushForce_Off = 0,
	SysCacheFlushForce_NonRecursive,
	SysCacheFlushForce_Recursive,
	SysCacheFlushForce_Max				/* must always be last */
} SysCacheFlushForce;

#define SysCacheFlushForce_IsValid(subclass) \
	(subclass >= SysCacheFlushForce_Off && subclass < SysCacheFlushForce_Max)

/* GUCs */
extern int gp_test_system_cache_flush_force; /* session GUC, forces system cache invalidation on each access */

#endif   /* INVAL_H */
