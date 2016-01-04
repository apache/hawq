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
 * proc.h
 *	  per-process shared memory data structures
 *
 *
 * Portions Copyright (c) 2006-2008, Greenplum inc
 * Portions Copyright (c) 1996-2009, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * $PostgreSQL: pgsql/src/include/storage/proc.h,v 1.91 2006/10/04 00:30:10 momjian Exp $
 *
 *-------------------------------------------------------------------------
 */
#ifndef _PROC_H_
#define _PROC_H_

#include "storage/lock.h"
#include "storage/spin.h"
#include "storage/pg_sema.h"

#include "cdb/cdbpublic.h"  /* LocalDistribXactRef */


/*
 * Each backend advertises up to PGPROC_MAX_CACHED_SUBXIDS TransactionIds
 * for non-aborted subtransactions of its current top transaction.	These
 * have to be treated as running XIDs by other backends.
 *
 * We also keep track of whether the cache overflowed (ie, the transaction has
 * generated at least one subtransaction that didn't fit in the cache).
 * If none of the caches have overflowed, we can assume that an XID that's not
 * listed anywhere in the PGPROC array is not a running transaction.  Else we
 * have to look at pg_subtrans.
 */
#define PGPROC_MAX_CACHED_SUBXIDS 64	/* XXX guessed-at value */

struct XidCache
{
	bool		overflowed;
	int			nxids;
	TransactionId xids[PGPROC_MAX_CACHED_SUBXIDS];
};

/* Flags for PGPROC->vacuumFlags */
#define		PROC_IS_AUTOVACUUM	0x01	/* is it an autovac worker? */
#define		PROC_IN_VACUUM		0x02	/* currently running lazy vacuum */
#define		PROC_IN_ANALYZE		0x04	/* currently running analyze */
#define		PROC_VACUUM_FOR_WRAPAROUND 0x08		/* set by autovac only */

/* flags reset at EOXact */
#define		PROC_VACUUM_STATE_MASK (0x0E)

/*
 * Each backend has a PGPROC struct in shared memory.  There is also a list of
 * currently-unused PGPROC structs that will be reallocated to new backends.
 *
 * links: list link for any list the PGPROC is in.	When waiting for a lock,
 * the PGPROC is linked into that lock's waitProcs queue.  A recycled PGPROC
 * is linked into ProcGlobal's freeProcs list.
 *
 * Note: twophase.c also sets up a dummy PGPROC struct for each currently
 * prepared transaction.  These PGPROCs appear in the ProcArray data structure
 * so that the prepared transactions appear to be still running and are
 * correctly shown as holding locks.  A prepared transaction PGPROC can be
 * distinguished from a real one at need by the fact that it has pid == 0.
 * The semaphore and lock-activity fields in a prepared-xact PGPROC are unused,
 * but its myProcLocks[] lists are valid.
 */
struct PGPROC
{
	/* proc->links MUST BE FIRST IN STRUCT (see ProcSleep,ProcWakeup,etc) */
	SHM_QUEUE	links;			/* list link if process is in a list */

	PGSemaphoreData sem;		/* ONE semaphore to sleep on */
	int			waitStatus;		/* STATUS_WAITING, STATUS_OK or STATUS_ERROR */

	TransactionId xid;			/* transaction currently being executed by
								 * this proc */

	LocalDistribXactRef	localDistribXactRef;
								/* Reference to the LocalDistribXact 
								 * element. */
	TransactionId xmin;			/* minimal running XID as it was when we were
								 * starting our xact, excluding LAZY VACUUM:
								 * vacuum must not remove tuples deleted by
								 * xid >= xmin ! */

	int			pid;			/* This backend's process id, or 0 */
	Oid			databaseId;		/* OID of database this backend is using */
	Oid			roleId;			/* OID of role using this backend */
    int         mppSessionId;   /* serial num of the qDisp process */
    int         mppLocalProcessSerial;  /* this backend's PGPROC serial num */
    bool		mppIsWriter;	/* The writer gang member, holder of locks */
	bool		postmasterResetRequired; /* Whether postmaster reset is required when this child exits */

	bool		inVacuum;		/* true if current xact is a LAZY VACUUM */

	/* Info about LWLock the process is currently waiting for, if any. */
	bool		lwWaiting;		/* true if waiting for an LW lock */
	bool		lwExclusive;	/* true if waiting for exclusive access */
	struct PGPROC *lwWaitLink;	/* next waiter for same LW lock */

	/* Info about lock the process is currently waiting for, if any. */
	/* waitLock and waitProcLock are NULL if not currently waiting. */
	LOCK	   *waitLock;		/* Lock object we're sleeping on ... */
	PROCLOCK   *waitProcLock;	/* Per-holder info for awaited lock */
	LOCKMODE	waitLockMode;	/* type of lock we're waiting for */
	LOCKMASK	heldLocks;		/* bitmask for lock types already held on this
								 * lock object by this backend */

	/*
	 * All PROCLOCK objects for locks held or awaited by this backend are
	 * linked into one of these lists, according to the partition number of
	 * their lock.
	 */
	SHM_QUEUE	myProcLocks[NUM_LOCK_PARTITIONS];

	struct XidCache subxids;	/* cache for subtransaction XIDs */

	/*
	 * Info for Resource Scheduling, what portal (i.e statement) we might
	 * be waiting on.
	 */
	uint32		waitPortalId;	/* portal id we are waiting on */

	/*
	 * Information for our combocid-map (populated in writer/dispatcher backends only)
	 */
	uint32		combocid_map_count; /* how many entries in the map ? */
	char		combocid_map_name[MAXPGPATH]; /* name of the map */

	int queryCommandId; /* command_id for the running query */
};

/* NOTE: "typedef struct PGPROC PGPROC" appears in storage/lock.h. */

extern PGDLLIMPORT PGPROC *MyProc;

/* Special for MPP reader gangs */
extern PGDLLIMPORT PGPROC *lockHolderProcPtr;

/*
 * There is one ProcGlobal struct for the whole database cluster.
 */
typedef struct PROC_HDR
{
	/* The PGPROC structures */
	PGPROC *procs;
	/* Head of list of free PGPROC structures */
	SHMEM_OFFSET freeProcs;
	/* Current shared estimate of appropriate spins_per_delay value */
	int			spins_per_delay;

    /* Counter for assigning serial numbers to processes */
    int         mppLocalProcessCounter;

	/*
	 * Number of free PGPROC entries.
	 *
	 * Note that this value is not updated synchronously with freeProcs.
	 * Thus, in some small time window, this value may not reflect
	 * the real number of free entries in freeProcs. However, since
	 * this is only used to check whether there are enough free entries
	 * to be reserved for superusers, it is okay.
	 */
	int numFreeProcs;

} PROC_HDR;

/*
 * We set aside some extra PGPROC structures for auxiliary processes,
 * ie things that aren't full-fledged backends but need shmem access.
 *
 * Background writer, WAL writer, and autovacuum launcher run during
 * normal operation. Startup process also consumes one slot, but WAL
 * writer and autovacuum launcher are launched only after it has
 * exited (4 slots).
 *
 * FileRep Process uses 
 *			a) 10 slots on Primary 
 *					1) Sender
 *					2) Receiver Ack
 *					3) Consumer Ack 
 *					4) Recovery 
 *					5) Resync Manager 
 *					6) Resync Worker 1
 *					7) Resync Worker 2
 *					8) Resync Worker 3
 *					9) Resync Worker 4
 *				   10) Verification
 * 
 *			b) 6 slots on Mirror 
 *					1) Receiver 
 *					2) Consumer 
 *					3) Consumer Writer
 *					4) Consumer Append Only
 *					5) Consumer Verification
 *					6) Sender Ack
 */
#define NUM_AUXILIARY_PROCS	 14

/* configurable options */
extern int	DeadlockTimeout;
extern int	StatementTimeout;
extern int IdleSessionGangTimeout;

extern volatile bool cancel_from_timeout;


/*
 * Function Prototypes
 */
extern int	ProcGlobalSemas(void);
extern Size ProcGlobalShmemSize(void);
extern void InitProcGlobal(int mppLocalProcessCounter);
extern void InitProcess(void);
extern void InitProcessPhase2(void);
extern void InitAuxiliaryProcess(void);
extern bool HaveNFreeProcs(int n);
extern void ProcReleaseLocks(bool isCommit);

extern void ProcQueueInit(PROC_QUEUE *queue);
extern int	ProcSleep(LOCALLOCK *locallock, LockMethod lockMethodTable);
extern PGPROC *ProcWakeup(PGPROC *proc, int waitStatus);
extern void ProcLockWakeup(LockMethod lockMethodTable, LOCK *lock);
extern bool LockWaitCancel(void);

extern void ProcWaitForSignal(void);
extern void ProcSendSignal(int pid);

extern bool enable_sig_alarm(int delayms, bool is_statement_timeout);
extern bool disable_sig_alarm(bool is_statement_timeout);
extern void handle_sig_alarm(SIGNAL_ARGS);
extern void EnableClientWaitTimeoutInterrupt(void);
extern bool DisableClientWaitTimeoutInterrupt(void);

extern int ResProcSleep(LOCKMODE lockmode, LOCALLOCK *locallock, void *incrementSet);

extern void ResLockWaitCancel(void);
extern bool ProcGetMppLocalProcessCounter(int *mppLocalProcessCounter);
extern bool ProcCanSetMppSessionId(void);
extern void ProcNewMppSessionId(int *newSessionId);
extern bool freeAuxiliaryProcEntryAndReturnReset(int pid, bool *inArray);
extern bool freeProcEntryAndReturnReset(int pid);
#endif   /* PROC_H */
