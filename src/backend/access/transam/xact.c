/*-------------------------------------------------------------------------
 *
 * xact.c
 *	  top level transaction system support routines
 *
 * See src/backend/access/transam/README for more information.
 *
 * Portions Copyright (c) 1996-2009, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  $PostgreSQL: pgsql/src/backend/access/transam/xact.c,v 1.229.2.2 2007/05/30 21:01:45 tgl Exp $
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include <time.h>
#include <unistd.h>

#include "access/appendonlywriter.h"
#include "access/multixact.h"
#include "access/subtrans.h"
#include "access/transam.h"
#include "access/twophase.h"
#include "access/xlogutils.h"
#include "access/fileam.h"
#include "catalog/namespace.h"
#include "commands/async.h"
#include "commands/tablecmds.h"
#include "commands/trigger.h"
#include "executor/spi.h"
#include "libpq/be-fsstubs.h"
#include "miscadmin.h"
#include "pgstat.h"
#include "storage/fd.h"
#include "storage/lmgr.h"
#include "storage/procarray.h"
#include "storage/smgr.h"
#include "storage/freespace.h"
#include "utils/combocid.h"
#include "utils/faultinjector.h"
#include "utils/flatfiles.h"
#include "utils/guc.h"
#include "utils/inval.h"
#include "utils/memutils.h"
#include "utils/relcache.h"
#include "utils/resscheduler.h"
#include "access/clog.h"

#include "cdb/cdbgang.h"
#include "cdb/cdbvars.h" /* Gp_role, Gp_is_writer, interconnect_setup_timeout */

#include "cdb/cdbpersistentstore.h"
#include "cdb/cdbquerycontextdispatching.h"
#include "postmaster/primary_mirror_mode.h"

/*
 *	User-tweakable parameters
 */
int			DefaultXactIsoLevel = XACT_READ_COMMITTED;
int			XactIsoLevel;

bool		DefaultXactReadOnly = false;
bool		XactReadOnly;

int			CommitDelay = 0;	/* precommit delay in microseconds */
int			CommitSiblings = 5; /* # concurrent xacts needed to sleep */

XidBuffer subxbuf;
int32 gp_subtrans_warn_limit = 16777216; /* 16 million */

/* gp-specific
 * routine for marking when a sequence makes a mark in the xlog.  we need
 * to keep track of this because sequences are the only reason a reader should
 * ever write to the xlog during commit.  As a result, we keep track of such
 * and will complain loudly if its violated.
 */
bool		seqXlogWrite;

/*
 *	transaction states - transaction state from server perspective
 */
typedef enum TransState
{
	TRANS_DEFAULT,				/* idle */
	TRANS_START,				/* transaction starting */
	TRANS_INPROGRESS,			/* inside a valid transaction */
	TRANS_COMMIT,				/* commit in progress */
	TRANS_ABORT,				/* abort in progress */
	TRANS_PREPARE				/* prepare in progress */
} TransState;

/*
 *	transaction block states - transaction state of client queries
 *
 * Note: the subtransaction states are used only for non-topmost
 * transactions; the others appear only in the topmost transaction.
 */
typedef enum TBlockState
{
	/* not-in-transaction-block states */
	TBLOCK_DEFAULT,				/* idle */
	TBLOCK_STARTED,				/* running single-query transaction */

	/* transaction block states */
	TBLOCK_BEGIN,				/* starting transaction block */
	TBLOCK_INPROGRESS,			/* live transaction */
	TBLOCK_END,					/* COMMIT received */
	TBLOCK_ABORT,				/* failed xact, awaiting ROLLBACK */
	TBLOCK_ABORT_END,			/* failed xact, ROLLBACK received */
	TBLOCK_ABORT_PENDING,		/* live xact, ROLLBACK received */
	TBLOCK_PREPARE,				/* live xact, PREPARE received */

	/* subtransaction states */
	TBLOCK_SUBBEGIN,			/* starting a subtransaction */
	TBLOCK_SUBINPROGRESS,		/* live subtransaction */
	TBLOCK_SUBEND,				/* RELEASE received */
	TBLOCK_SUBABORT,			/* failed subxact, awaiting ROLLBACK */
	TBLOCK_SUBABORT_END,		/* failed subxact, ROLLBACK received */
	TBLOCK_SUBABORT_PENDING,	/* live subxact, ROLLBACK received */
	TBLOCK_SUBRESTART,			/* live subxact, ROLLBACK TO received */
	TBLOCK_SUBABORT_RESTART		/* failed subxact, ROLLBACK TO received */
} TBlockState;

/*
 *	transaction state structure
 */
typedef struct TransactionStateData
{
	DistributedTransactionId distribXid;	/* My distributed transaction id, or Invalid if none. */
	TransactionId transactionId;	/* my XID, or Invalid if none */
	SubTransactionId subTransactionId;	/* my subxact ID */
	char	   *name;			/* savepoint name, if any */
	int			savepointLevel; /* savepoint level */
	TransState	state;			/* low-level state */
	TBlockState blockState;		/* high-level state */
	int			nestingLevel;	/* nest depth */
	MemoryContext curTransactionContext;		/* my xact-lifetime context */
	ResourceOwner curTransactionOwner;	/* my query resources */
	List	   *childXids;		/* subcommitted child XIDs */
	Oid			prevUser;		/* previous CurrentUserId setting */
	bool		prevSecDefCxt;	/* previous SecurityDefinerContext setting */
	bool		prevXactReadOnly;		/* entry-time xact r/o state */
	bool		executorSaysXactDoesWrites;	/* GP executor says xact does writes */
	struct TransactionStateData *parent;		/* back link to parent */

	struct TransactionStateData *fastLink;        /* back link to jump to parent for efficient search */
} TransactionStateData;

typedef TransactionStateData *TransactionState;

#define NUM_NODES_TO_SKIP_FOR_FAST_SEARCH 100 
static int fastNodeCount;
static TransactionState previousFastLink;

/*
 * CurrentTransactionState always points to the current transaction state
 * block.  It will point to TopTransactionStateData when not in a
 * transaction at all, or when in a top-level transaction.
 */
static TransactionStateData TopTransactionStateData = {
	0,							/* distributed transaction id */
	0,							/* transaction id */
	0,							/* subtransaction id */
	NULL,						/* savepoint name */
	0,							/* savepoint level */
	TRANS_DEFAULT,				/* transaction state */
	TBLOCK_DEFAULT,				/* transaction block state from the client
								 * perspective */
	0,							/* nesting level */
	NULL,						/* cur transaction context */
	NULL,						/* cur transaction resource owner */
	NIL,						/* subcommitted child Xids */
	InvalidOid,					/* previous CurrentUserId setting */
	false,						/* previous SecurityDefinerContext setting */
	false,						/* entry-time xact r/o state */
	false,						/* executorSaysXactDoesWrites */
	NULL,						/* link to parent state block */
	NULL
};

static TransactionState CurrentTransactionState = &TopTransactionStateData;

/*
 * The subtransaction ID and command ID assignment counters are global
 * to a whole transaction, so we do not keep them in the state stack.
 */
static SubTransactionId currentSubTransactionId;
static CommandId currentCommandId;

/*
 * xactStartTimestamp is the value of transaction_timestamp().
 * stmtStartTimestamp is the value of statement_timestamp().
 * xactStopTimestamp is the time at which we log a commit or abort WAL record.
 * These do not change as we enter and exit subtransactions, so we don't
 * keep them inside the TransactionState stack.
 */
static TimestampTz xactStartTimestamp;
static TimestampTz stmtStartTimestamp;
static TimestampTz xactStopTimestamp;

/*
 * Total number of SAVEPOINT commands executed by this transaction.
 *
 */
static int currentSavepointTotal;

/*
 * GID to be used for preparing the current transaction.  This is also
 * global to a whole transaction, so we don't keep it in the state stack.
 */
static char *prepareGID;

/*
 * Private context for transaction-abort work --- we reserve space for this
 * at startup to ensure that AbortTransaction and AbortSubTransaction can work
 * when we've run out of memory.
 */
static MemoryContext TransactionAbortContext = NULL;

/*
 * List of add-on start- and end-of-xact callbacks
 */
typedef struct XactCallbackItem
{
	struct XactCallbackItem *next;
	XactCallback callback;
	void	   *arg;
} XactCallbackItem;

static XactCallbackItem *Xact_callbacks = NULL;
static XactCallbackItem *Xact_callbacks_once = NULL;

/*
 * List of add-on start- and end-of-subxact callbacks
 */
typedef struct SubXactCallbackItem
{
	struct SubXactCallbackItem *next;
	SubXactCallback callback;
	void	   *arg;
} SubXactCallbackItem;

static SubXactCallbackItem *SubXact_callbacks = NULL;

/*
 * Subtransaction file used to keep subtransaction Ids that spill over from
 * shared snapshot. Kept outside of shared snapshot because readers and writer
 * have their own File pointer.
 */
File subxip_file = 0;

/* local function prototypes */
static void AssignSubTransactionId(TransactionState s);
static void AtAbort_ActiveQueryResource(void);
static void AtAbort_Memory(void);
static void AtCleanup_Memory(void);
static void AtAbort_ResourceOwner(void);
static void AtCommit_LocalCache(void);
static void AtCommit_Memory(void);
static void AtStart_Cache(void);
static void AtStart_Memory(void);
static void AtStart_ResourceOwner(void);
static void CallXactCallbacks(XactEvent event);
static void CallXactCallbacksOnce(XactEvent event);
static void CallSubXactCallbacks(SubXactEvent event,
					 SubTransactionId mySubid,
					 SubTransactionId parentSubid);
static void CleanupTransaction(void);
static void RecordTransactionAbort(void);

static void RecordSubTransactionCommit(void);
static void StartSubTransaction(void);
static void CommitSubTransaction(void);
static void AbortSubTransaction(void);
static void CleanupSubTransaction(void);
static void PushTransaction(void);
static void PopTransaction(void);

static void AtSubAbort_ActiveQueryResource(void);
static void AtSubAbort_Memory(void);
static void AtSubCleanup_Memory(void);
static void AtSubAbort_ResourceOwner(void);
static void AtSubCommit_Memory(void);
static void AtSubStart_Memory(void);
static void AtSubStart_ResourceOwner(void);

static void ShowTransactionState(const char *str);
static void ShowTransactionStateRec(TransactionState state);
static const char *BlockStateAsString(TBlockState blockState);
static const char *TransStateAsString(TransState state);

static bool search_binary_xids(TransactionId *ids, uint32 size,
			       TransactionId xid, int32 *index);

extern void FtsCondSetTxnReadOnly(bool *);

/* 
 * Make the following three functions external because old dtrace
 * cannot reference static symbol.
 */
extern void StartTransaction(void);
extern void CommitTransaction(void);
extern void AbortTransaction(void);

char *XactInfoKind_Name(
	const XactInfoKind		kind)
{
	switch (kind)
	{
	case XACT_INFOKIND_NONE: 		return "None";
	case XACT_INFOKIND_COMMIT: 		return "Commit";
	case XACT_INFOKIND_ABORT: 		return "Abort";
	case XACT_INFOKIND_PREPARE: 	return "Prepare";
	default:
		return "Unknown";
	}
}

/* ----------------------------------------------------------------
 *	transaction state accessors
 * ----------------------------------------------------------------
 */

/*
 *	IsTransactionState
 *
 *	This returns true if we are inside a valid transaction; that is,
 *	it is safe to initiate database access, take heavyweight locks, etc.
 */
bool
IsTransactionState(void)
{
	TransactionState s = CurrentTransactionState;

	switch (s->state)
	{
		case TRANS_DEFAULT:
			return false;
		case TRANS_START:
			return true;
		case TRANS_INPROGRESS:
			return true;
		case TRANS_COMMIT:
			return true;
		case TRANS_ABORT:
			return true;
		case TRANS_PREPARE:
			return true;
	}

	/*
	 * Shouldn't get here, but lint is not happy without this...
	 */
	return false;
}

bool
IsAbortInProgress(void)
{
	TransactionState s = CurrentTransactionState;

	return (s->state == TRANS_ABORT);
}

bool
IsCommitInProgress(void)
{
	TransactionState s = CurrentTransactionState;
	
	return (s->state == TRANS_COMMIT);
}

/*
 *	IsAbortedTransactionBlockState
 *
 *	This returns true if we are currently running a query
 *	within an aborted transaction block.
 */
bool
IsAbortedTransactionBlockState(void)
{
	TransactionState s = CurrentTransactionState;

	if (s->blockState == TBLOCK_ABORT ||
		s->blockState == TBLOCK_SUBABORT)
		return true;

	return false;
}

void 
GetAllTransactionXids(
	DistributedTransactionId	*distribXid,
	TransactionId				*localXid,
	TransactionId				*subXid)
{
	TransactionState s = CurrentTransactionState;

	*distribXid = s->distribXid;
	*localXid = s->transactionId;
	*subXid = s->subTransactionId;
}

/*
 *	GetTopTransactionId
 *
 * Get the ID of the main transaction, even if we are currently inside
 * a subtransaction.
 */
TransactionId
GetTopTransactionId(void)
{
	return TopTransactionStateData.transactionId;
}


/*
 *	GetCurrentTransactionId
 *
 * We do not assign XIDs to subtransactions until/unless this is called.
 * When we do assign an XID to a subtransaction, recursively make sure
 * its parent has one as well (this maintains the invariant that a child
 * transaction has an XID following its parent's).
 */
TransactionId
GetCurrentTransactionId(void)
{
	TransactionState s = CurrentTransactionState;

	if (!TransactionIdIsValid(s->transactionId))
		AssignSubTransactionId(s);

	return s->transactionId;
}

static void
AssignSubTransactionId(TransactionState s)
{
	ResourceOwner currentOwner;

	Assert(s->parent != NULL);
	Assert(s->state == TRANS_INPROGRESS);
	if (!TransactionIdIsValid(s->parent->transactionId))
		AssignSubTransactionId(s->parent);

	/*
	 * Generate a new Xid and record it in PG_PROC and pg_subtrans.
	 *
	 * NB: we must make the subtrans entry BEFORE the Xid appears anywhere in
	 * shared storage other than PG_PROC; because if there's no room for it in
	 * PG_PROC, the subtrans entry is needed to ensure that other backends see
	 * the Xid as "running".  See GetNewTransactionId.
	 */
	s->transactionId = GetNewTransactionId(true, true);
	elog((Debug_print_full_dtm ? LOG : DEBUG5), "AssignSubTransactionId(): assigned xid %u", s->transactionId);

	Assert(TransactionIdPrecedes(s->parent->transactionId, s->transactionId));

	SubTransSetParent(s->transactionId, s->parent->transactionId);

	/*
	 * Acquire lock on the transaction XID.  (We assume this cannot block.) We
	 * have to ensure that the lock is assigned to the transaction's own
	 * ResourceOwner.
	 */
	currentOwner = CurrentResourceOwner;
	PG_TRY();
	{
		CurrentResourceOwner = s->curTransactionOwner;

		XactLockTableInsert(s->transactionId);
	}
	PG_CATCH();
	{
		/* Ensure CurrentResourceOwner is restored on error */
		CurrentResourceOwner = currentOwner;
		PG_RE_THROW();
	}
	PG_END_TRY();
	CurrentResourceOwner = currentOwner;
}


/*
 *	GetCurrentTransactionIdIfAny
 *
 * Unlike GetCurrentTransactionId, this will return InvalidTransactionId
 * if we are currently not in a transaction, or in a transaction or
 * subtransaction that has not yet assigned itself an XID.
 */
TransactionId
GetCurrentTransactionIdIfAny(void)
{
	TransactionState s = CurrentTransactionState;

	return s->transactionId;
}

/*
 *	GetCurrentSubTransactionId
 */
SubTransactionId
GetCurrentSubTransactionId(void)
{
	TransactionState s = CurrentTransactionState;

	return s->subTransactionId;
}


/*
 *	GetCurrentCommandId
 */
CommandId
GetCurrentCommandId(void)
{
	/* this is global to a transaction, not subtransaction-local */
	return currentCommandId;
}

/*
 *	GetCurrentTransactionStartTimestamp
 */
TimestampTz
GetCurrentTransactionStartTimestamp(void)
{
	return xactStartTimestamp;
}

/*
 *	GetCurrentStatementStartTimestamp
 */
TimestampTz
GetCurrentStatementStartTimestamp(void)
{
	return stmtStartTimestamp;
}

/*
 *	GetCurrentTransactionStopTimestamp
 *
 * We return current time if the transaction stop time hasn't been set
 * (which can happen if we decide we don't need to log an XLOG record).
 */
TimestampTz
GetCurrentTransactionStopTimestamp(void)
{
	if (xactStopTimestamp != 0)
		return xactStopTimestamp;
	return GetCurrentTimestamp();
}

/*
 *	SetCurrentStatementStartTimestamp
 */
void
SetCurrentStatementStartTimestamp(void)
{
	stmtStartTimestamp = GetCurrentTimestamp();
}

/*
 *	SetCurrentStatementStartTimestampToMaster
 */
void
SetCurrentStatementStartTimestampToMaster(TimestampTz masterTime)
{
	if (masterTime != 0)
		stmtStartTimestamp = masterTime;
}

/*
 *	SetCurrentTransactionStopTimestamp
 */
static inline void
SetCurrentTransactionStopTimestamp(void)
{
	xactStopTimestamp = GetCurrentTimestamp();
}

/*
 *	GetCurrentTransactionNestLevel
 *
 * Note: this will return zero when not inside any transaction, one when
 * inside a top-level transaction, etc.
 */
int
GetCurrentTransactionNestLevel(void)
{
	TransactionState s = CurrentTransactionState;

	return s->nestingLevel;
}

#ifdef WATCH_VISIBILITY_IN_ACTION
#define MAX_WATCH_TRANSACTION_BUFFER 2000
static int WatchBufferOffset = 0;
static char WatchCurrentTransactionBuffer[MAX_WATCH_TRANSACTION_BUFFER];

char* WatchCurrentTransactionString(void)
{
	return WatchCurrentTransactionBuffer;
}
#endif

/*
 * We will return true for the Xid of the current subtransaction, any of
 * its subcommitted children, any of its parents, or any of their
 * previously subcommitted children.  However, a transaction being aborted
 * is no longer "current", even though it may still have an entry on the
 * state stack.
 *
 * The parent list and childXIDs list within it are in sorted decreasing order.
 * Taking advantage of this fact simple optimizations are added instead of linear traversal to fasten the search 
 *  1] Added fastLink/skipLink pointers to skip nodes in list and scan fast across, instead of visiting all nodes in list 
 *  2] Break-out as soon as XID to search is greater than the current node in (parent / child) list
*/
static bool
TransactionIdIsCurrentTransactionIdInternal(TransactionId xid)
{
	TransactionState s = CurrentTransactionState;

	while (s != NULL)
	{
		ListCell   *cell;

		if ((s->state != TRANS_ABORT) &&
				(TransactionIdIsValid(s->transactionId)))
		{
			if (TransactionIdEquals(xid, s->transactionId))
			{
#ifdef WATCH_VISIBILITY_IN_ACTION
				if (s == &TopTransactionStateData)
					WatchBufferOffset += snprintf(&WatchCurrentTransactionBuffer[WatchBufferOffset],
										MAX_WATCH_TRANSACTION_BUFFER-WatchBufferOffset,
										" is parent,");
				else
					WatchBufferOffset += snprintf(&WatchCurrentTransactionBuffer[WatchBufferOffset],
										MAX_WATCH_TRANSACTION_BUFFER-WatchBufferOffset,
										" is subtransaction,");
#endif
				return true;
			}
			foreach(cell, s->childXids)
			{
				if (TransactionIdEquals(xid, lfirst_xid(cell)))
					return true;
				/* 
				 * childXid list is maintained in decreasing order.
				 * hence, can safely breakout if XID follows currentListNode XID.
				 */
				if (TransactionIdFollows(xid, lfirst_xid(cell)))
				{
#ifdef WATCH_VISIBILITY_IN_ACTION
					WatchBufferOffset += snprintf(&WatchCurrentTransactionBuffer[WatchBufferOffset],
										MAX_WATCH_TRANSACTION_BUFFER-WatchBufferOffset,
										" subxid %u reached not checking ahead(childlist),", 
										lfirst_xid(cell));
#endif
					break;
				}
			}

			/*
			 * If not found in childXID list and greater than s->transactionId
			 * it cannot be on stack below this node, 
			 * as stack is in decreasing order of XIDs
			 * So, can safely breakout.
			 */
			 if (TransactionIdFollows(xid, s->transactionId))
			 {
#ifdef WATCH_VISIBILITY_IN_ACTION
				WatchBufferOffset += snprintf(&WatchCurrentTransactionBuffer[WatchBufferOffset],
									MAX_WATCH_TRANSACTION_BUFFER-WatchBufferOffset,
									" subxid %u reached not checking ahead(parent),", 
									s->transactionId);
#endif
				break;
			 }
		}

		if (s->fastLink)
		{
			if (TransactionIdPrecedesOrEquals(xid, s->fastLink->transactionId))
			{
#ifdef WATCH_VISIBILITY_IN_ACTION
				WatchBufferOffset += snprintf(&WatchCurrentTransactionBuffer[WatchBufferOffset],
									MAX_WATCH_TRANSACTION_BUFFER-WatchBufferOffset,
									" fast tracking search to subxid %u parent,", 
									s->fastLink->transactionId);
#endif
				s = s->fastLink;
				continue;
			}
		}

		s = s->parent;
	}

	return false;
}

/*
 *	TransactionIdIsCurrentTransactionId
 */
bool
TransactionIdIsCurrentTransactionId(TransactionId xid)
{
#ifdef WATCH_VISIBILITY_IN_ACTION
	WatchBufferOffset = 0;

	WatchBufferOffset += snprintf(&WatchCurrentTransactionBuffer[WatchBufferOffset],
						MAX_WATCH_TRANSACTION_BUFFER-WatchBufferOffset,
						"TransactionIdIsCurrentTransactionId xid %u", 
						xid);
#endif
    /*
	 * We always say that BootstrapTransactionId is "not my transaction ID"
	 * even when it is (ie, during bootstrap).	Along with the fact that
	 * transam.c always treats BootstrapTransactionId as already committed,
	 * this causes the tqual.c routines to see all tuples as committed, which
	 * is what we need during bootstrap.  (Bootstrap mode only inserts tuples,
	 * it never updates or deletes them, so all tuples can be presumed good
	 * immediately.)
	 *
	 * Likewise, InvalidTransactionId and FrozenTransactionId are certainly
	 * not my transaction ID, so we can just return "false" immediately for
	 * any non-normal XID.
	 */
	if (!TransactionIdIsNormal(xid))
	{
#ifdef WATCH_VISIBILITY_IN_ACTION
		WatchBufferOffset += snprintf(&WatchCurrentTransactionBuffer[WatchBufferOffset],
							MAX_WATCH_TRANSACTION_BUFFER-WatchBufferOffset,
							" not normal");
#endif
		return false;
	}
	
#ifdef WATCH_VISIBILITY_IN_ACTION
	WatchBufferOffset += snprintf(&WatchCurrentTransactionBuffer[WatchBufferOffset],
						MAX_WATCH_TRANSACTION_BUFFER-WatchBufferOffset,
						" normal check");
#endif

	bool flag = TransactionIdIsCurrentTransactionIdInternal(xid);
	return flag;
}

/*
 *	CommandCounterIncrement
 */
void
CommandCounterIncrement(void)
{
	currentCommandId += 1;
	if (currentCommandId == FirstCommandId)		/* check for overflow */
	{
		currentCommandId -= 1;
		ereport(ERROR,
				(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
		  errmsg("cannot have more than 2^32-1 commands in a transaction")));
	}

	/* Propagate new command ID into static snapshots, if set */
	if (SerializableSnapshot)
		SerializableSnapshot->curcid = currentCommandId;
	if (LatestSnapshot)
		LatestSnapshot->curcid = currentCommandId;
	
	/*
	 * make cache changes visible to me.
	 */
	AtCommit_LocalCache();
	AtStart_Cache();
}


/* ----------------------------------------------------------------
 *						StartTransaction stuff
 * ----------------------------------------------------------------
 */

/*
 *	AtStart_Cache
 */
static void
AtStart_Cache(void)
{
	AcceptInvalidationMessages();
}

/*
 *	AtStart_Memory
 */
static void
AtStart_Memory(void)
{
	TransactionState s = CurrentTransactionState;

	/*
	 * If this is the first time through, create a private context for
	 * AbortTransaction to work in.  By reserving some space now, we can
	 * insulate AbortTransaction from out-of-memory scenarios.  Like
	 * ErrorContext, we set it up with slow growth rate and a nonzero minimum
	 * size, so that space will be reserved immediately.
	 */
	if (TransactionAbortContext == NULL)
		TransactionAbortContext =
			AllocSetContextCreate(TopMemoryContext,
								  "TransactionAbortContext",
								  32 * 1024,
								  32 * 1024,
								  32 * 1024);

	/*
	 * We shouldn't have a transaction context already.
	 */
	Assert(TopTransactionContext == NULL);

	/*
	 * Create a toplevel context for the transaction.
	 */
	TopTransactionContext =
		AllocSetContextCreate(TopMemoryContext,
							  "TopTransactionContext",
							  ALLOCSET_DEFAULT_MINSIZE,
							  ALLOCSET_DEFAULT_INITSIZE,
							  ALLOCSET_DEFAULT_MAXSIZE);

	/*
	 * In a top-level transaction, CurTransactionContext is the same as
	 * TopTransactionContext.
	 */
	CurTransactionContext = TopTransactionContext;
	s->curTransactionContext = CurTransactionContext;

	/* Make the CurTransactionContext active. */
	MemoryContextSwitchTo(CurTransactionContext);

  /*
   * Create context for in-memory-only mappings.
   * This context is not deleted explicitly -
   * will be deleted when the TopTransactionContext is.
   */
  MemoryContext InMemoryContext =
      AllocSetContextCreate(TopTransactionContext,
                  "InMemoryContext",
                  ALLOCSET_DEFAULT_MINSIZE,
                  ALLOCSET_DEFAULT_INITSIZE,
                  ALLOCSET_DEFAULT_MAXSIZE);
  InitOidInMemHeapMapping(10, InMemoryContext, INMEM_ONLY_MAPPING);
}

/*
 *	AtStart_ResourceOwner
 */
static void
AtStart_ResourceOwner(void)
{
	TransactionState s = CurrentTransactionState;

	/*
	 * We shouldn't have a transaction resource owner already.
	 */
	Assert(TopTransactionResourceOwner == NULL);

	/*
	 * Create a toplevel resource owner for the transaction.
	 */
	s->curTransactionOwner = ResourceOwnerCreate(NULL, "TopTransaction");

	TopTransactionResourceOwner = s->curTransactionOwner;
	CurTransactionResourceOwner = s->curTransactionOwner;
	CurrentResourceOwner = s->curTransactionOwner;
}

/* ----------------------------------------------------------------
 *						StartSubTransaction stuff
 * ----------------------------------------------------------------
 */

/*
 * AtSubStart_Memory
 */
static void
AtSubStart_Memory(void)
{
	TransactionState s = CurrentTransactionState;

	Assert(CurTransactionContext != NULL);

	/*
	 * Create a CurTransactionContext, which will be used to hold data that
	 * survives subtransaction commit but disappears on subtransaction abort.
	 * We make it a child of the immediate parent's CurTransactionContext.
	 */
	CurTransactionContext = AllocSetContextCreate(CurTransactionContext,
												  "CurTransactionContext",
												  ALLOCSET_DEFAULT_MINSIZE,
												  ALLOCSET_DEFAULT_INITSIZE,
												  ALLOCSET_DEFAULT_MAXSIZE);
	s->curTransactionContext = CurTransactionContext;

	/* Make the CurTransactionContext active. */
	MemoryContextSwitchTo(CurTransactionContext);
}

/*
 * AtSubStart_ResourceOwner
 */
static void
AtSubStart_ResourceOwner(void)
{
	TransactionState s = CurrentTransactionState;

	Assert(s->parent != NULL);

	/*
	 * Create a resource owner for the subtransaction.	We make it a child of
	 * the immediate parent's resource owner.
	 */
	s->curTransactionOwner =
		ResourceOwnerCreate(s->parent->curTransactionOwner,
							"SubTransaction");

	CurTransactionResourceOwner = s->curTransactionOwner;
	CurrentResourceOwner = s->curTransactionOwner;
}

/* ----------------------------------------------------------------
 *						CommitTransaction stuff
 * ----------------------------------------------------------------
 */

/*
 *	RecordTransactionCommit
 */
void
RecordTransactionCommit(void)
{
	CHECKPOINT_START_LOCK_DECLARE;

	int32						persistentCommitSerializeLen;
	PersistentEndXactRecObjects persistentCommitObjects;
	int16						persistentCommitObjectCount;
	char						*persistentCommitBuffer = NULL;

	int			nchildren;
	TransactionId *children;
	bool		omitCommitRecordForDirtyQEReader;

	/* Get data needed for commit record */
	persistentCommitSerializeLen =
			PersistentEndXactRec_FetchObjectsFromSmgr(
										&persistentCommitObjects,
										EndXactRecKind_Commit,
										&persistentCommitObjectCount);

	nchildren = xactGetCommittedChildren(&children);

	omitCommitRecordForDirtyQEReader = false;

	/*
	 * If we made neither any XLOG entries nor any temp-rel updates, and have
	 * no files to be deleted, we can omit recording the transaction commit at
	 * all.  (This test includes the effects of subtransactions, so the
	 * presence of committed subxacts need not alone force a write.)
	 */
	if (MyXactMadeXLogEntry || MyXactMadeTempRelUpdate ||
		persistentCommitObjectCount > 0)
	{
		TransactionId xid = GetCurrentTransactionId();
		bool		madeTCentries;
		XLogRecPtr	recptr;

		/* Tell bufmgr and smgr to prepare for commit */
		BufmgrCommit();

		if (!omitCommitRecordForDirtyQEReader)
		{
		START_CRIT_SECTION();

		/*
		 * If our transaction made any transaction-controlled XLOG entries, we
		 * need to lock out checkpoint start between writing our XLOG record
		 * and updating pg_clog.  Otherwise it is possible for the checkpoint
		 * to set REDO after the XLOG record but fail to flush the pg_clog
		 * update to disk, leading to loss of the transaction commit if we
		 * crash a little later.  Slightly klugy fix for problem discovered
		 * 2004-08-10.
		 *
		 * (If it made no transaction-controlled XLOG entries, its XID appears
		 * nowhere in permanent storage, so no one else will ever care if it
		 * committed; so it doesn't matter if we lose the commit flag.)
		 *
		 * Note we only need a shared lock.
		 */
		madeTCentries = (MyLastRecPtr.xrecoff != 0);
		if (madeTCentries)
		{
			/*
			 * When we use CheckpointStartLock, we make sure we already have the
			 * MirroredLock first.
			 *
			 * The lock order is: MirroredLock then CheckpointStartLock.
			 */
			CHECKPOINT_START_LOCK;
		}

		SetCurrentTransactionStopTimestamp();
		/*
		 * We only need to log the commit in XLOG if the transaction made any
		 * transaction-controlled XLOG entries or will delete files.
		 */
		if (madeTCentries || persistentCommitObjectCount > 0)
		{
			XLogRecData rdata[4];
			int			lastrdata = 0;
			xl_xact_commit xlrec;

			xlrec.xtime = time(NULL);
		    //xlrec.xact_time = xactStopTimestamp;
			xlrec.persistentCommitObjectCount = persistentCommitObjectCount;
			xlrec.nsubxacts = nchildren;
			rdata[0].data = (char *) (&xlrec);
			rdata[0].len = MinSizeOfXactCommit;
			rdata[0].buffer = InvalidBuffer;
			/* dump persistent commit objects */
			if (persistentCommitObjectCount > 0)
			{
				int16 objectCount;

				Assert(persistentCommitSerializeLen > 0);
				persistentCommitBuffer = 
							(char*)palloc(persistentCommitSerializeLen);
				
				PersistentEndXactRec_Serialize(
										&persistentCommitObjects,
										EndXactRecKind_Commit,
										&objectCount,
										(uint8*)persistentCommitBuffer,
										persistentCommitSerializeLen);

				
				if (Debug_persistent_print)
					PersistentEndXactRec_Print("RecordTransactionCommit", &persistentCommitObjects);
				
				rdata[0].next = &(rdata[1]);
				rdata[1].data = persistentCommitBuffer;
				rdata[1].len = persistentCommitSerializeLen;
				rdata[1].buffer = InvalidBuffer;
				lastrdata = 1;
			}
			/* dump committed child Xids */
			if (nchildren > 0)
			{
				rdata[lastrdata].next = &(rdata[2]);
				rdata[2].data = (char *) children;
				rdata[2].len = nchildren * sizeof(TransactionId);
				rdata[2].buffer = InvalidBuffer;
				lastrdata = 2;
			}

			rdata[lastrdata].next = NULL;

			recptr = XLogInsert(RM_XACT_ID, XLOG_XACT_COMMIT, rdata);
		}
		else
		{
			/* Just flush through last record written by me */
			recptr = ProcLastRecEnd;
		}

		/* MPP: If we are the QD and we've used sequences (from the sequence server) then we need 
		 * to make sure that we flush the XLOG entries made by the sequence server.  We do this
		 * by moving our recptr ahead to where the sequence server is if its later than our own.
		 * 
		 */
		if (Gp_role == GP_ROLE_DISPATCH && seqXlogWrite)
		{
			LWLockAcquire(SeqServerControlLock, LW_EXCLUSIVE);
			
			if (XLByteLT(recptr, seqServerCtl->lastXlogEntry))
			{	
				recptr.xlogid = seqServerCtl->lastXlogEntry.xlogid;
				recptr.xrecoff = seqServerCtl->lastXlogEntry.xrecoff;
			}
			
			LWLockRelease(SeqServerControlLock);
		}
		
		/*
		 * We must flush our XLOG entries to disk if we made any XLOG entries,
		 * whether in or out of transaction control.  For example, if we
		 * reported a nextval() result to the client, this ensures that any
		 * XLOG record generated by nextval will hit the disk before we report
		 * the transaction committed.
		 *
		 * Note: if we generated a commit record above, MyXactMadeXLogEntry
		 * will certainly be set now.
		 */
		if (MyXactMadeXLogEntry)
		{
			/*
			 * Sleep before flush! So we can flush more than one commit
			 * records per single fsync.  (The idea is some other backend may
			 * do the XLogFlush while we're sleeping.  This needs work still,
			 * because on most Unixen, the minimum select() delay is 10msec or
			 * more, which is way too long.)
			 *
			 * We do not sleep if enableFsync is not turned on, nor if there
			 * are fewer than CommitSiblings other backends with active
			 * transactions.
			 */
			if (CommitDelay > 0 && enableFsync &&
				CountActiveBackends() >= CommitSiblings)
				pg_usleep(CommitDelay);

			XLogFlush(recptr);
						
#ifdef FAULT_INJECTOR
			if (CurrentTransactionState->blockState == TBLOCK_END)
			{
					FaultInjector_InjectFaultIfSet(
												   LocalTmRecordTransactionCommit,
												   DDLNotSpecified,
												   "",  // databaseName
												   ""); // tableName
			}
#endif
		}

		/*
		 * We must mark the transaction committed in clog if its XID appears
		 * either in permanent rels or in local temporary rels. We test this
		 * by seeing if we made transaction-controlled entries *OR* local-rel
		 * tuple updates.  Note that if we made only the latter, we have not
		 * emitted an XLOG record for our commit, and so in the event of a
		 * crash the clog update might be lost.  This is okay because no one
		 * else will ever care whether we committed.
		 */
		if (madeTCentries || MyXactMadeTempRelUpdate)
		{
			TransactionIdCommit(xid);
			/* to avoid race conditions, the parent must commit first */
			TransactionIdCommitTree(nchildren, children);
		}

#ifdef FAULT_INJECTOR	
		FaultInjector_InjectFaultIfSet(
					       DtmXLogDistributedCommit, 
					       DDLNotSpecified,
					       "",	// databaseName
					       ""); // tableName
#endif

		/* Unlock checkpoint lock if we acquired it */
		if (madeTCentries)
		{
			CHECKPOINT_START_UNLOCK;
		}

		END_CRIT_SECTION();
	}
	}

	/* Break the chain of back-links in the XLOG records I output */
	MyLastRecPtr.xrecoff = 0;
	MyXactMadeXLogEntry = false;
	MyXactMadeTempRelUpdate = false;

	/* And clean up local data */

	// UNDONE: Free storage in persistentCommitObjects...

	if (persistentCommitBuffer != NULL)
		pfree(persistentCommitBuffer);
	if (children)
		pfree(children);
}

/*
 *	AtCommit_LocalCache
 */
static void
AtCommit_LocalCache(void)
{
	/*
	 * Make catalog changes visible to me for the next command.
	 */
	CommandEndInvalidationMessages();
}

/*
 *	AtCommit_Memory
 */
static void
AtCommit_Memory(void)
{
	/*
	 * Now that we're "out" of a transaction, have the system allocate things
	 * in the top memory context instead of per-transaction contexts.
	 */
	MemoryContextSwitchTo(TopMemoryContext);

  /* Release in memory mappings */
  InMemHeap_DropAll(INMEM_ONLY_MAPPING);
  CleanupOidInMemHeapMapping(INMEM_ONLY_MAPPING);

	/*
	 * Release all transaction-local memory.
	 */
	Assert(TopTransactionContext != NULL);
	MemoryContextDelete(TopTransactionContext);
	TopTransactionContext = NULL;
	CurTransactionContext = NULL;
	CurrentTransactionState->curTransactionContext = NULL;
}

/* ----------------------------------------------------------------
 *						CommitSubTransaction stuff
 * ----------------------------------------------------------------
 */

/*
 * AtSubCommit_Memory
 */
static void
AtSubCommit_Memory(void)
{
	TransactionState s = CurrentTransactionState;

	Assert(s->parent != NULL);

	/* Return to parent transaction level's memory context. */
	CurTransactionContext = s->parent->curTransactionContext;
	MemoryContextSwitchTo(CurTransactionContext);

	/*
	 * Ordinarily we cannot throw away the child's CurTransactionContext,
	 * since the data it contains will be needed at upper commit.  However, if
	 * there isn't actually anything in it, we can throw it away.  This avoids
	 * a small memory leak in the common case of "trivial" subxacts.
	 */
	if (MemoryContextIsEmpty(s->curTransactionContext))
	{
		MemoryContextDelete(s->curTransactionContext);
		s->curTransactionContext = NULL;
	}
}

/*
 * AtSubCommit_childXids
 *
 * Pass my own XID and my child XIDs up to my parent as committed children.
 */
static void
AtSubCommit_childXids(void)
{
	TransactionState s = CurrentTransactionState;
	MemoryContext old_cxt;

	Assert(s->parent != NULL);

	/*
	 * We keep the child-XID lists in TopTransactionContext; this avoids
	 * setting up child-transaction contexts for what might be just a few
	 * bytes of grandchild XIDs.
	 */
	old_cxt = MemoryContextSwitchTo(TopTransactionContext);

	/*
	 * The childXids is maintained in decreasing order. This is to
	 * enable collection of subtransaction ids in decreasing order
	 * for efficiency. See GetAddedSubtransactionsFromTree().
	 * parent->childXids, if not NULL, has xids that precede s->childXids.
	 */

#ifdef USE_ASSERT_CHECKING
	if (s->childXids && list_length(s->childXids))
	{
		Assert(TransactionIdPrecedes(s->transactionId, llast_oid(s->childXids)));
	}
#endif
	List *new_childXids = lappend_xid(s->childXids,
                                s->transactionId);

	if (s->parent->childXids != NIL)
	{
		Assert(TransactionIdPrecedes(linitial_oid(s->parent->childXids), llast_oid(new_childXids)));

		new_childXids = list_concat(new_childXids,
				  s->parent->childXids);

		/*
		 * list_concat doesn't free the list header for the second list;
		 *  do so here to avoid memory leakage (kluge)
		 */
		pfree(s->parent->childXids);
	}
	s->parent->childXids = new_childXids;
	s->childXids = NIL;
	MemoryContextSwitchTo(old_cxt);
}

/*
 * RecordSubTransactionCommit
 */
static void
RecordSubTransactionCommit(void)
{
	/*
	 * We do not log the subcommit in XLOG; it doesn't matter until the
	 * top-level transaction commits.
	 *
	 * We must mark the subtransaction subcommitted in clog if its XID appears
	 * either in permanent rels or in local temporary rels. We test this by
	 * seeing if we made transaction-controlled entries *OR* local-rel tuple
	 * updates.  (The test here actually covers the entire transaction tree so
	 * far, so it may mark subtransactions that don't really need it, but it's
	 * probably not worth being tenser. Note that if a prior subtransaction
	 * dirtied these variables, then RecordTransactionCommit will have to do
	 * the full pushup anyway...)
	 */
	if (MyLastRecPtr.xrecoff != 0 || MyXactMadeTempRelUpdate)
	{
		TransactionId xid = GetCurrentTransactionId();

		/* XXX does this really need to be a critical section? */
		START_CRIT_SECTION();

		/* Record subtransaction subcommit */
		TransactionIdSubCommit(xid);

		END_CRIT_SECTION();
	}
}

/* ----------------------------------------------------------------
 *						AbortTransaction stuff
 * ----------------------------------------------------------------
 */

/*
 *	RecordTransactionAbort
 */
static void
RecordTransactionAbort(void)
{
	int32						persistentAbortSerializeLen;
	PersistentEndXactRecObjects persistentAbortObjects;
	int16						persistentAbortObjectCount;
	char						*persistentAbortBuffer = NULL;

	int			nchildren;
	TransactionId *children;

	/* Get data needed for abort record */
	persistentAbortSerializeLen =
			PersistentEndXactRec_FetchObjectsFromSmgr(
										&persistentAbortObjects,
										EndXactRecKind_Abort,
										&persistentAbortObjectCount);

	nchildren = xactGetCommittedChildren(&children);

	/*
	 * If we made neither any transaction-controlled XLOG entries nor any
	 * temp-rel updates, and are not going to delete any files, we can omit
	 * recording the transaction abort at all.	No one will ever care that it
	 * aborted.  (These tests cover our whole transaction tree.)
	 */
	SetCurrentTransactionStopTimestamp();
	if (MyLastRecPtr.xrecoff != 0 || MyXactMadeTempRelUpdate ||
		persistentAbortObjectCount > 0)
	{
		TransactionId xid = GetCurrentTransactionId();

		/*
		 * Catch the scenario where we aborted partway through
		 * RecordTransactionCommit ...
		 */
		if (TransactionIdDidCommit(xid))
			elog(PANIC, "cannot abort transaction %u, it was already committed", xid);

		START_CRIT_SECTION();

		/*
		 * We only need to log the abort in XLOG if the transaction made any
		 * transaction-controlled XLOG entries or will delete files. (If it
		 * made no transaction-controlled XLOG entries, its XID appears
		 * nowhere in permanent storage, so no one else will ever care if it
		 * committed.)
		 *
		 * We do not flush XLOG to disk unless deleting files, since the
		 * default assumption after a crash would be that we aborted, anyway.
		 * For the same reason, we don't need to worry about interlocking
		 * against checkpoint start.
		 */
		if (MyLastRecPtr.xrecoff != 0 || persistentAbortObjectCount > 0)
		{
			XLogRecData rdata[3];
			int			lastrdata = 0;
			xl_xact_abort xlrec;
			XLogRecPtr	recptr;

			xlrec.xtime = time(NULL);
			/* Write the ABORT record */
			/*
			if (isSubXact)
				xlrec.xact_time = GetCurrentTimestamp();
			else
			{
				xlrec.xact_time = xactStopTimestamp;
			}*/
			xlrec.persistentAbortObjectCount = persistentAbortObjectCount;
			xlrec.nsubxacts = nchildren;
			rdata[0].data = (char *) (&xlrec);
			rdata[0].len = MinSizeOfXactAbort;
			rdata[0].buffer = InvalidBuffer;
			/* dump persistent abort objects */
			if (persistentAbortObjectCount > 0)
			{
				int16 objectCount;

				Assert(persistentAbortSerializeLen > 0);
				persistentAbortBuffer = 
							(char*)palloc(persistentAbortSerializeLen);
				
				PersistentEndXactRec_Serialize(
										&persistentAbortObjects,
										EndXactRecKind_Abort,
										&objectCount,
										(uint8*)persistentAbortBuffer,
										persistentAbortSerializeLen);

				if (Debug_persistent_print)
					PersistentEndXactRec_Print("RecordTransactionAbort", &persistentAbortObjects);

				rdata[0].next = &(rdata[1]);
				rdata[1].data = persistentAbortBuffer;
				rdata[1].len = persistentAbortSerializeLen;
				rdata[1].buffer = InvalidBuffer;
				lastrdata = 1;
			}
			/* dump committed child Xids */
			if (nchildren > 0)
			{
				rdata[lastrdata].next = &(rdata[2]);
				rdata[2].data = (char *) children;
				rdata[2].len = nchildren * sizeof(TransactionId);
				rdata[2].buffer = InvalidBuffer;
				lastrdata = 2;
			}
			rdata[lastrdata].next = NULL;

			recptr = XLogInsert(RM_XACT_ID, XLOG_XACT_ABORT, rdata);

			/* Must flush if we are deleting files... */
			if (persistentAbortObjectCount > 0)
				XLogFlush(recptr);
		}

		/*
		 * Mark the transaction aborted in clog.  This is not absolutely
		 * necessary but we may as well do it while we are here.
		 *
		 * The ordering here isn't critical but it seems best to mark the
		 * parent first.  This assures an atomic transition of all the
		 * subtransactions to aborted state from the point of view of
		 * concurrent TransactionIdDidAbort calls.
		 */
		TransactionIdAbort(xid);
		TransactionIdAbortTree(nchildren, children);

		END_CRIT_SECTION();
	}

	/* Break the chain of back-links in the XLOG records I output */
	MyLastRecPtr.xrecoff = 0;
	MyXactMadeXLogEntry = false;
	MyXactMadeTempRelUpdate = false;

	/* And clean up local data */

	// UNDONE: Free storage in persistentAbortObjects...

	if (persistentAbortBuffer != 0)
		pfree(persistentAbortBuffer);
	if (children)
		pfree(children);
}


bool RecordCrashTransactionAbortRecord(
	TransactionId				xid,
	PersistentEndXactRecObjects *persistentAbortObjects)
{
	int 	persistentAbortObjectCount;
	int 	persistentAbortSerializeLen;
	char	*persistentAbortBuffer = NULL;

	XLogRecData rdata[3];
	int			lastrdata = 0;
	xl_xact_abort xlrec;
	XLogRecPtr	recptr;
	XidStatus status;
	bool           validStatus;

	/* Fix for MPP-12614. The call to TransactionIdGetStatus() has been removed since    */
	/* The clog many not have the the referenced transation as of crash recovery         */
	/* pass 2. The clog will be fully built in pass 3 of crash recovery.                 */
	/* The new call "InRecoveryTansactionIdGetStatus will return TRUE or FALSE in the    */
	/* validStatus parameter, which indicates whether or not the returned value is good. */

	validStatus = false;

	status = InRecoveryTransactionIdGetStatus(xid, &validStatus);
	if (validStatus == true && status != 0 &&
	    status != TRANSACTION_STATUS_ABORTED)
	    {
	    int elevel;

	    if (gp_crash_recovery_abort_suppress_fatal)
	      elevel = WARNING;
	    else
	      elevel = FATAL;

	    elog( elevel,"Crash recovery abort invalid for transaction %u current status '%s' (0x%x) and new status '%s' (0x%x)",
		  xid,
		  XidStatus_Name(status),
		  status,
		  XidStatus_Name(TRANSACTION_STATUS_ABORTED),
		  TRANSACTION_STATUS_ABORTED
		  );
	    return false;
	    }

	persistentAbortObjectCount = 
				PersistentEndXactRec_ObjectCount(
										persistentAbortObjects,
										EndXactRecKind_Abort);
	Assert(persistentAbortObjectCount > 0);

	persistentAbortSerializeLen =
				PersistentEndXactRec_SerializeLen(
										persistentAbortObjects,
										EndXactRecKind_Abort);

	START_CRIT_SECTION();
	
	xlrec.xtime = time(NULL);
	/* Write the ABORT record */
	/*
	if (isSubXact)
		xlrec.xact_time = GetCurrentTimestamp();
	else
	{
		xlrec.xact_time = xactStopTimestamp;
	}*/
	xlrec.persistentAbortObjectCount = persistentAbortObjectCount;
	xlrec.nsubxacts = 0;
	rdata[0].data = (char *) (&xlrec);
	rdata[0].len = MinSizeOfXactAbort;
	rdata[0].buffer = InvalidBuffer;
	/* dump persistent abort objects */

	{
		int16 objectCount;

		Assert(persistentAbortSerializeLen > 0);
		persistentAbortBuffer = 
					(char*)palloc(persistentAbortSerializeLen);
		
		PersistentEndXactRec_Serialize(
								persistentAbortObjects,
								EndXactRecKind_Abort,
								&objectCount,
								(uint8*)persistentAbortBuffer,
								persistentAbortSerializeLen);

		if (Debug_persistent_print)
			PersistentEndXactRec_Print("RecordCrashTransactionAbortRecord", persistentAbortObjects);

		rdata[0].next = &(rdata[1]);
		rdata[1].data = persistentAbortBuffer;
		rdata[1].len = persistentAbortSerializeLen;
		rdata[1].buffer = InvalidBuffer;
		lastrdata = 1;
	}
	/* no committed child Xids */
	rdata[lastrdata].next = NULL;

	/*
	 * Raw set the transaction id... so XLogInsert can call GetCurrentTransactionIdIfAny.
	 */
	{
		TransactionState s = CurrentTransactionState;
	
		s->transactionId = xid;
	}
	recptr = XLogInsert(RM_XACT_ID, XLOG_XACT_ABORT, rdata);

	/*
	 * Mark the transaction aborted in clog.  This is not absolutely
	 * necessary but we may as well do it while we are here.
	 *
	 * The ordering here isn't critical but it seems best to mark the
	 * parent first.  This assures an atomic transition of all the
	 * subtransactions to aborted state from the point of view of
	 * concurrent TransactionIdDidAbort calls.
	 */

	/* Fix for MPP-12614. Only set the clog status for the transaciton if the returned */
	/* value for TransactionIdGetStatus() was good.                                    */

	if (validStatus == true)
	  TransactionIdAbort(xid);

	END_CRIT_SECTION();

	return true;
}

static void
AtAbort_ActiveQueryResource(void)
{
	if ( Gp_role == GP_ROLE_DISPATCH ) {
		CleanupActiveQueryResource();
	}
}

static void
AtSubAbort_ActiveQueryResource(void)
{
	if ( Gp_role == GP_ROLE_DISPATCH ) {
		CleanupActiveQueryResource();
	}
}

/*
 *	AtAbort_Memory
 */
static void
AtAbort_Memory(void)
{
	/*
	 * Switch into TransactionAbortContext, which should have some free space
	 * even if nothing else does.  We'll work in this context until we've
	 * finished cleaning up.
	 *
	 * It is barely possible to get here when we've not been able to create
	 * TransactionAbortContext yet; if so use TopMemoryContext.
	 */
	if (TransactionAbortContext != NULL)
		MemoryContextSwitchTo(TransactionAbortContext);
	else
		MemoryContextSwitchTo(TopMemoryContext);
}

/*
 * AtSubAbort_Memory
 */
static void
AtSubAbort_Memory(void)
{
	Assert(TransactionAbortContext != NULL);

	MemoryContextSwitchTo(TransactionAbortContext);
}


/*
 *	AtAbort_ResourceOwner
 */
static void
AtAbort_ResourceOwner(void)
{
	/*
	 * Make sure we have a valid ResourceOwner, if possible (else it will be
	 * NULL, which is OK)
	 */
	CurrentResourceOwner = TopTransactionResourceOwner;
}

/*
 * AtSubAbort_ResourceOwner
 */
static void
AtSubAbort_ResourceOwner(void)
{
	TransactionState s = CurrentTransactionState;

	/* Make sure we have a valid ResourceOwner */
	CurrentResourceOwner = s->curTransactionOwner;
}


/*
 * AtSubAbort_childXids
 */
static void
AtSubAbort_childXids(void)
{
	TransactionState s = CurrentTransactionState;

	/*
	 * We keep the child-XID lists in TopTransactionContext (see
	 * AtSubCommit_childXids).	This means we'd better free the list
	 * explicitly at abort to avoid leakage.
	 */
	list_free(s->childXids);
	s->childXids = NIL;
}

/*
 * RecordSubTransactionAbort
 */
static void
RecordSubTransactionAbort(void)
{
	int32						persistentAbortSerializeLen;
	PersistentEndXactRecObjects persistentAbortObjects;
	int16						persistentAbortObjectCount;
	char						*persistentAbortBuffer = NULL;

	TransactionId xid = GetCurrentTransactionId();
	int			nchildren;
	TransactionId *children;

	/* Get data needed for abort record */
	persistentAbortSerializeLen =
			PersistentEndXactRec_FetchObjectsFromSmgr(
										&persistentAbortObjects,
										EndXactRecKind_Abort,
										&persistentAbortObjectCount);

	nchildren = xactGetCommittedChildren(&children);

	/*
	 * If we made neither any transaction-controlled XLOG entries nor any
	 * temp-rel updates, and are not going to delete any files, we can omit
	 * recording the transaction abort at all.	No one will ever care that it
	 * aborted.  (These tests cover our whole transaction tree, and therefore
	 * may mark subxacts that don't really need it, but it's probably not
	 * worth being tenser.)
	 *
	 * In this case we needn't worry about marking subcommitted children as
	 * aborted, because they didn't mark themselves as subcommitted in the
	 * first place; see the optimization in RecordSubTransactionCommit.
	 */
	if (MyLastRecPtr.xrecoff != 0 || MyXactMadeTempRelUpdate || 
		persistentAbortObjectCount > 0)
	{
		START_CRIT_SECTION();

		/*
		 * We only need to log the abort in XLOG if the transaction made any
		 * transaction-controlled XLOG entries or will delete files.
		 */
		if (MyLastRecPtr.xrecoff != 0 || persistentAbortObjectCount > 0)
		{
			XLogRecData rdata[3];
			int			lastrdata = 0;
			xl_xact_abort xlrec;
			XLogRecPtr	recptr;

			xlrec.xtime = time(NULL);
			xlrec.persistentAbortObjectCount = persistentAbortObjectCount;
			xlrec.nsubxacts = nchildren;
			rdata[0].data = (char *) (&xlrec);
			rdata[0].len = MinSizeOfXactAbort;
			rdata[0].buffer = InvalidBuffer;
			/* dump persistent abort objects */
			if (persistentAbortObjectCount > 0)
			{
				int16 objectCount;

				Assert(persistentAbortSerializeLen > 0);
				persistentAbortBuffer = 
							(char*)palloc(persistentAbortSerializeLen);
				
				PersistentEndXactRec_Serialize(
										&persistentAbortObjects,
										EndXactRecKind_Abort,
										&objectCount,
										(uint8*)persistentAbortBuffer,
										persistentAbortSerializeLen);

				if (Debug_persistent_print)
					PersistentEndXactRec_Print("RecordSubTransactionAbort", &persistentAbortObjects);

				rdata[0].next = &(rdata[1]);
				rdata[1].data = persistentAbortBuffer;
				rdata[1].len = persistentAbortSerializeLen;
				rdata[1].buffer = InvalidBuffer;
				lastrdata = 1;
			}
			/* dump committed child Xids */
			if (nchildren > 0)
			{
				rdata[lastrdata].next = &(rdata[2]);
				rdata[2].data = (char *) children;
				rdata[2].len = nchildren * sizeof(TransactionId);
				rdata[2].buffer = InvalidBuffer;
				lastrdata = 2;
			}
			rdata[lastrdata].next = NULL;

			recptr = XLogInsert(RM_XACT_ID, XLOG_XACT_ABORT, rdata);

			/* Must flush if we are deleting files... */
			if (persistentAbortObjectCount > 0)
				XLogFlush(recptr);
		}

		/*
		 * Mark the transaction aborted in clog.  This is not absolutely
		 * necessary but XactLockTableWait makes use of it to avoid waiting
		 * for already-aborted subtransactions.
		 */
		TransactionIdAbort(xid);
		TransactionIdAbortTree(nchildren, children);

		END_CRIT_SECTION();
	}

	/*
	 * We can immediately remove failed XIDs from PGPROC's cache of running
	 * child XIDs. It's easiest to do it here while we have the child XID
	 * array at hand, even though in the main-transaction case the equivalent
	 * work happens just after return from RecordTransactionAbort.
	 */
	XidCacheRemoveRunningXids(xid, nchildren, children);

	/* And clean up local data */

	// UNDONE: Free storage in persistentAbortObjects...

	if (persistentAbortBuffer != 0)
		pfree(persistentAbortBuffer);
	if (children)
		pfree(children);
}

/* ----------------------------------------------------------------
 *						CleanupTransaction stuff
 * ----------------------------------------------------------------
 */

/*
 *	AtCleanup_Memory
 */
static void
AtCleanup_Memory(void)
{
	Assert(CurrentTransactionState->parent == NULL);

	/*
	 * Now that we're "out" of a transaction, have the system allocate things
	 * in the top memory context instead of per-transaction contexts.
	 */
	MemoryContextSwitchTo(TopMemoryContext);

  /* Release in memory mappings */
  InMemHeap_DropAll(INMEM_ONLY_MAPPING);
  CleanupOidInMemHeapMapping(INMEM_ONLY_MAPPING);

	/*
	 * Clear the special abort context for next time.
	 */
	if (TransactionAbortContext != NULL)
		MemoryContextResetAndDeleteChildren(TransactionAbortContext);

	/*
	 * Release all transaction-local memory.
	 */
	if (TopTransactionContext != NULL)
		MemoryContextDelete(TopTransactionContext);
	TopTransactionContext = NULL;
	CurTransactionContext = NULL;
	CurrentTransactionState->curTransactionContext = NULL;
}


/* ----------------------------------------------------------------
 *						CleanupSubTransaction stuff
 * ----------------------------------------------------------------
 */

/*
 * AtSubCleanup_Memory
 */
static void
AtSubCleanup_Memory(void)
{
	TransactionState s = CurrentTransactionState;

	Assert(s->parent != NULL);

	/* Make sure we're not in an about-to-be-deleted context */
	MemoryContextSwitchTo(s->parent->curTransactionContext);
	CurTransactionContext = s->parent->curTransactionContext;

	/*
	 * Clear the special abort context for next time.
	 */
	if (TransactionAbortContext != NULL)
		MemoryContextResetAndDeleteChildren(TransactionAbortContext);

	/*
	 * Delete the subxact local memory contexts. Its CurTransactionContext can
	 * go too (note this also kills CurTransactionContexts from any children
	 * of the subxact).
	 */
	if (s->curTransactionContext)
		MemoryContextDelete(s->curTransactionContext);
	s->curTransactionContext = NULL;
}

/* ----------------------------------------------------------------
 *						interface routines
 * ----------------------------------------------------------------
 */
void
SetSharedTransactionId_reader(TransactionId xid, CommandId cid)
{
	TopTransactionStateData.transactionId = xid;
	currentCommandId = cid;
}

/*
 * ============== XID BUFFER Functions ===================
 * Functions to manipulate XidBuffer: pages of sorted transaction ids.
 */

static bool
search_binary_xids(TransactionId *ids, uint32 size, TransactionId xid,
		   int32 *index)
{
	uint32 mid;
	uint32 begin;
	uint32 end;

	*index = -1;
	if (size == 0)
	{
		return false;
	}

	begin = 0;
	end = size;
	while (begin < end)
	{
		mid = (begin + end)/2;

		if (TransactionIdFollows(ids[mid], xid))
		{
			end = mid;
		}
		else if (TransactionIdPrecedes(ids[mid], xid))
		{
			begin = mid+1;
		}
		else
		{
			*index = mid;
			return true;
		}
	}

	return false;
}

static inline bool
IsXidBufferEmpty(XidBuffer *xidbuf)
{
	return (xidbuf->actual_bufs == 0);
}

bool
FindXidInXidBuffer(XidBuffer *xidbuf, TransactionId xid, uint32 *cnt,
		   int32 *index)
{
	uint32 size;
	
	if (IsXidBufferEmpty(xidbuf))
		return false;

	for (*cnt = 1; *cnt < xidbuf->actual_bufs; (*cnt)++)
	{
		if (TransactionIdFollows(*xidbuf->ids_buf[*cnt], xid))
			break;
	}

	size = ((*cnt == xidbuf->actual_bufs) || (xidbuf->actual_bufs == 1))
		? xidbuf->last_buf_cnt : MAX_XIDBUF_XIDS;
	(*cnt)--;

	return search_binary_xids(xidbuf->ids_buf[*cnt], size, xid, index);
}

static inline void
AdvanceNextIndexForXidBuffer(XidBuffer *xidbuf, uint32 *cnt, uint32 *index)
{
	int i;

	if ((xidbuf->last_buf_cnt != 0) &&
	    (xidbuf->last_buf_cnt < MAX_XIDBUF_XIDS))
	{
		*cnt = xidbuf->actual_bufs - 1;
		*index = xidbuf->last_buf_cnt;
		xidbuf->last_buf_cnt++;
		return;
	}

again:
	if (xidbuf->actual_bufs < xidbuf->max_bufs)
	{
		xidbuf->ids_buf[xidbuf->actual_bufs] = (TransactionId *)
			MemoryContextAllocZero(TopMemoryContext,
					       MAX_XIDBUF_SIZE);
		*cnt = xidbuf->actual_bufs++;
		xidbuf->last_buf_cnt = 1;
		*index = 0;
		return;
	}

	if (xidbuf->max_bufs)
	{
		xidbuf->max_bufs *= 2;
	}
	else
	{
		xidbuf->max_bufs = MAX_XIDBUF_INIT_PAGES;
	}

	if (xidbuf->ids_buf != NULL)
	{
		xidbuf->ids_buf = (TransactionId **)repalloc(xidbuf->ids_buf,
						    xidbuf->max_bufs *
						    sizeof(TransactionId *));
	}
	else
	{
		xidbuf->ids_buf = (TransactionId **)MemoryContextAlloc(
						    TopMemoryContext,
						    xidbuf->max_bufs *
						    sizeof(TransactionId *));
	}

	for (i = xidbuf->actual_bufs; i < xidbuf->max_bufs; i++)
	{
		xidbuf->ids_buf[i] = NULL;
	}

	goto again;

	/* Not reached */
	return;
}

void
AddSortedToXidBuffer(XidBuffer *xidbuf, TransactionId *xids,
			      uint32 count)
{
	uint32 index;
	uint32 cnt = 0;
	uint32 i;

	for (i = 0; i < count; i++)
	{
		AdvanceNextIndexForXidBuffer(xidbuf, &cnt, &index);
		xidbuf->ids_buf[cnt][index] = xids[i];
	}
}

void
ResetXidBuffer(XidBuffer *xidbuf)
{
	int i;

	for (i = 0; i < xidbuf->actual_bufs; i ++)
	{
		pfree(xidbuf->ids_buf[i]);
	}

	if (xidbuf->max_bufs)
		pfree(xidbuf->ids_buf);

	bzero(xidbuf, sizeof(*xidbuf));
}

static char *
Subtransaction_filename(volatile TransactionId xid)
{
	static char filename[MAXPGPATH];

	snprintf(filename, sizeof(filename), "sess%u", xid);
	return filename;
}

static inline void
OpenOrCreateSubtransIdFile(DistributedTransactionId dxid)
{
	if (subxip_file == 0)
	{
		subxip_file = OpenTemporaryFile(
			Subtransaction_filename(dxid),
			1, false, true, true, false);
		// What should be done if this fails ??
	}
}

/*
 * MPP routine for marking when a sequence makes a mark in the xlog.
 * we need to keep track of this because sequences are the only reason
 * a reader should ever write to the xlog during commit.  As a result,
 * we keep track of such and will complain loudly if its violated.
 */
void
SetXactSeqXlog(void)
{
	seqXlogWrite = true;
}

/*
 *	StartTransaction
 */
void
StartTransaction(void)
{
	TransactionState s;

	/*
	 * Let's just make sure the state stack is empty
	 */
	s = &TopTransactionStateData;
	CurrentTransactionState = s;

	/*
	 * check the current transaction state
	 */
	if (s->state != TRANS_DEFAULT)
		elog(WARNING, "StartTransaction while in %s state",
			 TransStateAsString(s->state));

	/*
	 * set the current transaction state information appropriately during
	 * start processing
	 */
	s->state = TRANS_START;
	s->transactionId = InvalidTransactionId;	/* until assigned */

	/*
	 * Make sure we've freed any old snapshot, and reset xact state variables
	 */
	FreeXactSnapshot();
	XactIsoLevel = DefaultXactIsoLevel;
	XactReadOnly = DefaultXactReadOnly;
	seqXlogWrite = false;

	/* set read only by fts, if any fts action is read only */
	FtsCondSetTxnReadOnly(&XactReadOnly);

	/*
	 * reinitialize within-transaction counters
	 */
	s->subTransactionId = TopSubTransactionId;
	currentSubTransactionId = TopSubTransactionId;
	currentCommandId = FirstCommandId;
	currentSavepointTotal = 0;

	fastNodeCount = 0;
	previousFastLink = NULL;

	/*
	 * must initialize resource-management stuff first
	 */
	AtStart_Memory();
	AtStart_ResourceOwner();

  /*
   * Reset external Oid counter for transaction
   */
  ResetExternalObjectId();

	/*
	 * generate a new transaction id
	 */
	s->transactionId = GetNewTransactionId(false, true);

	XactLockTableInsert(s->transactionId);

	PG_TRACE1(transaction__start, s->transactionId);

	/*
 	 * set transaction_timestamp() (a/k/a now()).  We want this to be the
 	 * same as the first command's statement_timestamp(), so don't do a
 	 * fresh GetCurrentTimestamp() call (which'd be expensive anyway).
  	 */
  	xactStartTimestamp = stmtStartTimestamp;
  	xactStopTimestamp = 0;
	pgstat_report_xact_timestamp(xactStartTimestamp);

	/*
	 * initialize current transaction state fields
	 *
	 * note: prevXactReadOnly is not used at the outermost level
	 */
	s->nestingLevel = 1;
	s->childXids = NIL;
	GetUserIdAndContext(&s->prevUser, &s->prevSecDefCxt);
	/* SecurityDefinerContext should never be set outside a transaction */
	Assert(!s->prevSecDefCxt);

	/*
	 * initialize other subsystems for new transaction
	 */
	AtStart_Inval();
	AtStart_Cache();
	AfterTriggerBeginXact();

	/*
	 * done with start processing, set current transaction state to "in
	 * progress"
	 */
	s->state = TRANS_INPROGRESS;

	ShowTransactionState("StartTransaction");
}

/*
 *	CommitTransaction
 *
 * NB: if you change this routine, better look at PrepareTransaction too!
 */
void
CommitTransaction(void)
{
	CHECKPOINT_START_LOCK_DECLARE;

	TransactionState s = CurrentTransactionState;

	bool willHaveObjectsFromSmgr;

	ShowTransactionState("CommitTransaction");

	/*
	 * check the current transaction state
	 */
	if (s->state != TRANS_INPROGRESS)
		elog(WARNING, "CommitTransaction while in %s state",
			 TransStateAsString(s->state));
	Assert(s->parent == NULL);


	if (Gp_role == GP_ROLE_EXECUTE && !Gp_is_writer)
		elog(DEBUG1,"CommitTransaction: called as segment Reader");
	
	/*
	 * Do pre-commit processing (most of this stuff requires database access,
	 * and in fact could still cause an error...)
	 *
	 * It is possible for CommitHoldablePortals to invoke functions that queue
	 * deferred triggers, and it's also possible that triggers create holdable
	 * cursors.  So we have to loop until there's nothing left to do.
	 */
	for (;;)
	{
		/*
		 * Fire all currently pending deferred triggers.
		 */
		AfterTriggerFireDeferred();

		/*
		 * Convert any open holdable cursors into static portals.  If there
		 * weren't any, we are done ... otherwise loop back to check if they
		 * queued deferred triggers.  Lather, rinse, repeat.
		 */
		if (!CommitHoldablePortals())
			break;
	}

	/* Now we can shut down the deferred-trigger manager */
	AfterTriggerEndXact(true);

	/* Close any open regular cursors */
	AtCommit_Portals();

	/* Perform any AO table commit processing */
	AtCommit_AppendOnly(false);
		
	/*
	 * Let ON COMMIT management do its thing (must happen after closing
	 * cursors, to avoid dangling-reference problems)
	 */
	PreCommit_on_commit_actions();

	/* close large objects before lower-level cleanup */
	AtEOXact_LargeObject(true);

	/* NOTIFY commit must come before lower-level cleanup */
	AtCommit_Notify();

	/*
	 * Update flat files if we changed pg_database, pg_authid or
	 * pg_auth_members.  This should be the last step before commit.
	 */
	AtEOXact_UpdateFlatFiles(true);

	willHaveObjectsFromSmgr =
			PersistentEndXactRec_WillHaveObjectsFromSmgr(EndXactRecKind_Commit);

	/* In previous version, we ensured the recording of the [distributed-]commit record and the
	 * persistent post-commit work will be done either before or after a checkpoint.
	 *
	 * However the persistent table status will be synchronized with AOSeg_XXXX
	 * table and hdfs file in PersistentRecovery_Scan() at recovery PASS2.
	 * We don't need to worry about inconsistent states between them. So no
	 * CHECKPOINT_START_LOCK any more.
	 */
#ifdef FAULT_INJECTOR
	FaultInjector_InjectFaultIfSet(
			Checkpoint,
			DDLNotSpecified,
			"",	// databaseName
			""); // tableName
#endif

	/* Prevent cancel/die interrupt while cleaning up */
	HOLD_INTERRUPTS();

	/*
	 * set the current transaction state information appropriately during
	 * commit processing
	 */
	s->state = TRANS_COMMIT;

	/*
	 * Here is where we really truly commit.
	 */
	RecordTransactionCommit();

	/*----------
	 * Let others know about no transaction in progress by me. Note that
	 * this must be done _before_ releasing locks we hold and _after_
	 * RecordTransactionCommit.
	 *
	 * LWLockAcquire(ProcArrayLock) is required; consider this example:
	 *		UPDATE with xid 0 is blocked by xid 1's UPDATE.
	 *		xid 1 is doing commit while xid 2 gets snapshot.
	 * If xid 2's GetSnapshotData sees xid 1 as running then it must see
	 * xid 0 as running as well, or it will be able to see two tuple versions
	 * - one deleted by xid 1 and one inserted by xid 0.  See notes in
	 * GetSnapshotData.
	 *
	 * Note: MyProc may be null during bootstrap.
	 *----------
	 */
	if (MyProc != NULL)
	{
		/* Lock ProcArrayLock because that's what GetSnapshotData uses. */

		// UNDONE: Move lock up over RecordTransactionCommit ??
		
		LWLockAcquire(ProcArrayLock, LW_EXCLUSIVE);
		MyProc->xid = InvalidTransactionId;
		MyProc->xmin = InvalidTransactionId;
		MyProc->inVacuum = false;		/* must be cleared with xid/xmin */
		
		/* Clear the subtransaction-XID cache too while holding the lock */
		MyProc->subxids.nxids = 0;
		MyProc->subxids.overflowed = false;
		LWLockRelease(ProcArrayLock);
	}

	PG_TRACE1(transaction__commit, s->transactionId);

	/*
	 * This is all post-commit cleanup.  Note that if an error is raised here,
	 * it's too late to abort the transaction.  This should be just
	 * noncritical resource releasing.
	 *
	 * The ordering of operations is not entirely random.  The idea is:
	 * release resources visible to other backends (eg, files, buffer pins);
	 * then release locks; then release backend-local resources. We want to
	 * release locks at the point where any backend waiting for us will see
	 * our transaction as being fully cleaned up.
	 *
	 * Resources that can be associated with individual queries are handled by
	 * the ResourceOwner mechanism.  The other calls here are for backend-wide
	 * state.
	 */

	CallXactCallbacks(XACT_EVENT_COMMIT);
	CallXactCallbacksOnce(XACT_EVENT_COMMIT);
	
	ResourceOwnerRelease(TopTransactionResourceOwner,
						 RESOURCE_RELEASE_BEFORE_LOCKS,
						 true, true);

	/* All relations that are in the vacuum process are being commited now. */
	ResetVacuumRels();

	/* Check we've released all buffer pins */
	AtEOXact_Buffers(true);

	/* Clean up the relation cache */
	AtEOXact_RelationCache(true);

	/*
	 * Make catalog changes visible to all backends.  This has to happen after
	 * relcache references are dropped (see comments for
	 * AtEOXact_RelationCache), but before locks are released (if anyone is
	 * waiting for lock on a relation we've modified, we want them to know
	 * about the catalog change before they start using the relation).
	 */
	AtEOXact_Inval(true);

	AtEOXact_QueryContext();

	/*
	 * Likewise, dropping of files deleted during the transaction is best done
	 * after releasing relcache and buffer pins.  (This is not strictly
	 * necessary during commit, since such pins should have been released
	 * already, but this ordering is definitely critical during abort.)
	 */
	AtEOXact_smgr(true);

	AtEOXact_MultiXact();

	ResourceOwnerRelease(TopTransactionResourceOwner,
						 RESOURCE_RELEASE_LOCKS,
						 true, true);
	ResourceOwnerRelease(TopTransactionResourceOwner,
						 RESOURCE_RELEASE_AFTER_LOCKS,
						 true, true);

	/* Check we've released all catcache entries */
	AtEOXact_CatCache(true);

	AtEOXact_AppendOnly();
	AtEOXact_GUC(true, false);
	AtEOXact_SPI(true);
	AtEOXact_on_commit_actions(true);
	AtEOXact_Namespace(true);
	/* smgrcommit already done */
	AtEOXact_Files();
	AtEOXact_ComboCid();
	AtEOXact_HashTables(true);
	AtEOXact_PgStat(true);
	//AtEOXact_Snapshot(true);
	pgstat_report_xact_timestamp(0);

	CurrentResourceOwner = GetTopResourceOwner();
	ResourceOwnerDelete(TopTransactionResourceOwner);
	s->curTransactionOwner = NULL;
	CurTransactionResourceOwner = NULL;
	TopTransactionResourceOwner = NULL;

	AtCommit_Memory();

	ResetXidBuffer(&subxbuf);

	s->distribXid = InvalidDistributedTransactionId;
	s->transactionId = InvalidTransactionId;
	s->subTransactionId = InvalidSubTransactionId;
	s->nestingLevel = 0;
	s->childXids = NIL;
	s->executorSaysXactDoesWrites = false;

	/*
	 * done with commit processing, set current transaction state back to
	 * default
	 */
	s->state = TRANS_DEFAULT;

	/* we're now in a consistent state to handle an interrupt. */
	RESUME_INTERRUPTS();
}


/*
 *	PrepareTransaction
 *
 * NB: if you change this routine, better look at CommitTransaction too!
 */
static void
PrepareTransaction(void)
{
	TransactionState s = CurrentTransactionState;
	TransactionId xid = GetCurrentTransactionId();
	GlobalTransaction gxact;
	TimestampTz prepared_at;

	ShowTransactionState("PrepareTransaction");

	/*
	 * check the current transaction state
	 */
	if (s->state != TRANS_INPROGRESS)
		elog(WARNING, "PrepareTransaction while in %s state",
			 TransStateAsString(s->state));
	Assert(s->parent == NULL);

	/*
	 * Do pre-commit processing (most of this stuff requires database access,
	 * and in fact could still cause an error...)
	 *
	 * It is possible for PrepareHoldablePortals to invoke functions that
	 * queue deferred triggers, and it's also possible that triggers create
	 * holdable cursors.  So we have to loop until there's nothing left to do.
	 */
	for (;;)
	{
		/*
		 * Fire all currently pending deferred triggers.
		 */
		AfterTriggerFireDeferred();

		/*
		 * Convert any open holdable cursors into static portals.  If there
		 * weren't any, we are done ... otherwise loop back to check if they
		 * queued deferred triggers.  Lather, rinse, repeat.
		 */
		if (!PrepareHoldablePortals())
			break;
	}

	/* Now we can shut down the deferred-trigger manager */
	AfterTriggerEndXact(true);

	/* Close any open regular cursors */
	AtCommit_Portals();

	/*
	 * Let ON COMMIT management do its thing (must happen after closing
	 * cursors, to avoid dangling-reference problems)
	 */
	PreCommit_on_commit_actions();

	/* close large objects before lower-level cleanup */
	AtEOXact_LargeObject(true);

	/* NOTIFY and flatfiles will be handled below */

	/* Prevent cancel/die interrupt while cleaning up */
	HOLD_INTERRUPTS();

	/*
	 * set the current transaction state information appropriately during
	 * prepare processing
	 */
	s->state = TRANS_PREPARE;

	prepared_at = GetCurrentTimestamp();

	/* Tell bufmgr and smgr to prepare for commit */
	BufmgrCommit();

	/*
	 * Reserve the GID for this transaction. This could fail if the requested
	 * GID is invalid or already in use.
	 */
	gxact = MarkAsPreparing(xid, 
							&MyProc->localDistribXactRef,
							prepareGID, prepared_at,
				GetUserId(), MyDatabaseId, NULL);
	prepareGID = NULL;

	/*
	 * Collect data for the 2PC state file.  Note that in general, no actual
	 * state change should happen in the called modules during this step,
	 * since it's still possible to fail before commit, and in that case we
	 * want transaction abort to be able to clean up.  (In particular, the
	 * AtPrepare routines may error out if they find cases they cannot
	 * handle.)  State cleanup should happen in the PostPrepare routines
	 * below.  However, some modules can go ahead and clear state here because
	 * they wouldn't do anything with it during abort anyway.
	 *
	 * Note: because the 2PC state file records will be replayed in the same
	 * order they are made, the order of these calls has to match the order in
	 * which we want things to happen during COMMIT PREPARED or ROLLBACK
	 * PREPARED; in particular, pay attention to whether things should happen
	 * before or after releasing the transaction's locks.
	 */
	StartPrepare(gxact);

	AtPrepare_Notify();
	AtPrepare_UpdateFlatFiles();
	AtPrepare_Inval();
	AtPrepare_Locks();
	AtPrepare_PgStat();

	/*
	 * Here is where we really truly prepare.
	 *
	 * We have to record transaction prepares even if we didn't make any
	 * updates, because the transaction manager might get confused if we lose
	 * a global transaction.
	 */
	EndPrepare(gxact);

	/*
	 * Now we clean up backend-internal state and release internal resources.
	 */

	/* Break the chain of back-links in the XLOG records I output */
	MyLastRecPtr.xrecoff = 0;
	MyXactMadeXLogEntry = false;
	MyXactMadeTempRelUpdate = false;

	/*
	 * Let others know about no transaction in progress by me.	This has to be
	 * done *after* the prepared transaction has been marked valid, else
	 * someone may think it is unlocked and recyclable.
	 */

	/* Lock ProcArrayLock because that's what GetSnapshotData uses. */
	LWLockAcquire(ProcArrayLock, LW_EXCLUSIVE);
	MyProc->xid = InvalidTransactionId;
	
	MyProc->xmin = InvalidTransactionId;
	MyProc->inVacuum = false;	/* must be cleared with xid/xmin */

	/* Clear the subtransaction-XID cache too while holding the lock */
	MyProc->subxids.nxids = 0;
	MyProc->subxids.overflowed = false;

	LWLockRelease(ProcArrayLock);

	/*
	 * This is all post-transaction cleanup.  Note that if an error is raised
	 * here, it's too late to abort the transaction.  This should be just
	 * noncritical resource releasing.	See notes in CommitTransaction.
	 */

	CallXactCallbacks(XACT_EVENT_PREPARE);
	CallXactCallbacksOnce(XACT_EVENT_PREPARE);

	ResourceOwnerRelease(TopTransactionResourceOwner,
						 RESOURCE_RELEASE_BEFORE_LOCKS,
						 true, true);

	/* Check we've released all buffer pins */
	AtEOXact_Buffers(true);

	/* Clean up the relation cache */
	AtEOXact_RelationCache(true);

	/* notify and flatfiles don't need a postprepare call */

	PostPrepare_PgStat();

	PostPrepare_Inval();

	PostPrepare_smgr();

	AtEOXact_MultiXact();

	PostPrepare_Locks(xid);

	ResourceOwnerRelease(TopTransactionResourceOwner,
						 RESOURCE_RELEASE_LOCKS,
						 true, true);
	ResourceOwnerRelease(TopTransactionResourceOwner,
						 RESOURCE_RELEASE_AFTER_LOCKS,
						 true, true);

	/* Check we've released all catcache entries */
	AtEOXact_CatCache(true);

	/* PREPARE acts the same as COMMIT as far as GUC is concerned */
	AtEOXact_GUC(true, false);
	AtEOXact_SPI(true);
	AtEOXact_on_commit_actions(true);
	AtEOXact_Namespace(true);
	/* smgrcommit already done */
	AtEOXact_Files();
	AtEOXact_ComboCid();
	AtEOXact_HashTables(true);
	/* don't call AtEOXact_PgStat here */

	CurrentResourceOwner = GetTopResourceOwner();
	ResourceOwnerDelete(TopTransactionResourceOwner);
	s->curTransactionOwner = NULL;
	CurTransactionResourceOwner = NULL;
	TopTransactionResourceOwner = NULL;

	AtCommit_Memory();

	s->distribXid = InvalidDistributedTransactionId;
	s->transactionId = InvalidTransactionId;
	s->subTransactionId = InvalidSubTransactionId;
	s->nestingLevel = 0;
	s->childXids = NIL;
	s->executorSaysXactDoesWrites = false;

	/*
	 * done with 1st phase commit processing, set current transaction state
	 * back to default
	 */
	s->state = TRANS_DEFAULT;

	ResetXidBuffer(&subxbuf);

	RESUME_INTERRUPTS();
}


/*
 *	AbortTransaction
 */
void
AbortTransaction(void)
{
	CHECKPOINT_START_LOCK_DECLARE;

	TransactionState s = CurrentTransactionState;

	bool willHaveObjectsFromSmgr;

#ifdef FAULT_INJECTOR
	FaultInjector_InjectFaultIfSet(
			AbortTransactionFail,
			DDLNotSpecified,
			"",  // databaseName
			""); // tableName
#endif

	/* Prevent cancel/die interrupt while cleaning up */
	HOLD_INTERRUPTS();


	/* Make sure we have a valid memory context and resource owner */
	AtAbort_Memory();
	AtAbort_ResourceOwner();

	AtAbort_ActiveQueryResource();
	/*
	 * Release any LW locks we might be holding as quickly as possible.
	 * (Regular locks, however, must be held till we finish aborting.)
	 * Releasing LW locks is critical since we might try to grab them again
	 * while cleaning up!
	 */
	LWLockReleaseAll();

	/* Clean up buffer I/O and buffer context locks, too */
	AbortBufferIO();
	UnlockBuffers();

	/*
	 * Also clean up any open wait for lock, since the lock manager will choke
	 * if we try to wait for another lock before doing this.
	 */
	LockWaitCancel();

	/*
	 * check the current transaction state
	 */
	if (s->state != TRANS_INPROGRESS && s->state != TRANS_PREPARE)
		elog(DEBUG1, "WARNING: AbortTransaction while in %s state",
			 TransStateAsString(s->state));
	Assert(s->parent == NULL);

	/*
	 * set the current transaction state information appropriately during the
	 * abort processing
	 */
	s->state = TRANS_ABORT;

	/*
	 * Reset user ID which might have been changed transiently.  We need this
	 * to clean up in case control escaped out of a SECURITY DEFINER function
	 * or other local change of CurrentUserId; therefore, the prior value of
	 * SecurityDefinerContext also needs to be restored.
	 *
	 * (Note: it is not necessary to restore session authorization or role
	 * settings here because those can only be changed via GUC, and GUC will
	 * take care of rolling them back if need be.)
	 */
	SetUserIdAndContext(s->prevUser, s->prevSecDefCxt);

	/*
	 * Clear the freespace map entries for any relations that
	 * are in the vacuum process.
	 */
	ClearFreeSpaceForVacuumRels();

	/*
	 * do abort processing
	 */
	AfterTriggerEndXact(false);
	AtAbort_Portals();
	AtAbort_ExtTables();
		
	/* Perform any AO table abort processing */
	AtAbort_AppendOnly(false);

	AtEOXact_LargeObject(false);	/* 'false' means it's abort */
	AtAbort_Notify();
	AtEOXact_UpdateFlatFiles(false);


	willHaveObjectsFromSmgr =
			PersistentEndXactRec_WillHaveObjectsFromSmgr(EndXactRecKind_Abort);


	/* In previous version, we ensured the recording of the the abort record and the
	 * persistent post-abort work will be done either before or after a checkpoint.
	 *
	 * However the persistent table status will be synchronized with AOSeg_XXXX
	 * table and hdfs file in PersistentRecovery_Scan() at recovery PASS2.
	 * We don't need to worry about inconsistent states between them. So no
	 * CHECKPOINT_START_LOCK any more.
	 */
#ifdef FAULT_INJECTOR
	FaultInjector_InjectFaultIfSet(
			Checkpoint,
			DDLNotSpecified,
			"",	// databaseName
			""); // tableName
#endif
	/*
	 * Advertise the fact that we aborted in pg_clog (assuming that we got as
	 * far as assigning an XID to advertise).
	 */
	if (TransactionIdIsValid(s->transactionId))
		RecordTransactionAbort();

	ResetXidBuffer(&subxbuf);

	/*
	 * Let others know about no transaction in progress by me. Note that this
	 * must be done _before_ releasing locks we hold and _after_
	 * RecordTransactionAbort.
	 */
	if (MyProc != NULL)
	{
		/* Lock ProcArrayLock because that's what GetSnapshotData uses. */

		// UNDONE: Move lock up over RecordTransactionAbort ??
		
		LWLockAcquire(ProcArrayLock, LW_EXCLUSIVE);

		MyProc->xid = InvalidTransactionId;
		MyProc->xmin = InvalidTransactionId;
		MyProc->inVacuum = false;		/* must be cleared with xid/xmin */

		/* Clear the subtransaction-XID cache too while holding the lock */
		MyProc->subxids.nxids = 0;
		MyProc->subxids.overflowed = false;

		LWLockRelease(ProcArrayLock);
	}

	PG_TRACE1(transaction__abort, s->transactionId);

	/*
	 * Post-abort cleanup.	See notes in CommitTransaction() concerning
	 * ordering.
	 */

	CallXactCallbacks(XACT_EVENT_ABORT);
	CallXactCallbacksOnce(XACT_EVENT_ABORT);

	ResourceOwnerRelease(TopTransactionResourceOwner,
						 RESOURCE_RELEASE_BEFORE_LOCKS,
						 false, true);
	AtEOXact_Buffers(false);
	AtEOXact_RelationCache(false);
	AtEOXact_Inval(false);
	AtEOXact_QueryContext();
	AtEOXact_smgr(false);
	
	AtEOXact_MultiXact();
	ResourceOwnerRelease(TopTransactionResourceOwner,
						 RESOURCE_RELEASE_LOCKS,
						 false, true);
	ResourceOwnerRelease(TopTransactionResourceOwner,
						 RESOURCE_RELEASE_AFTER_LOCKS,
						 false, true);
	AtEOXact_CatCache(false);

	AtEOXact_AppendOnly();
	AtEOXact_GUC(false, false);
	AtEOXact_SPI(false);
	AtEOXact_on_commit_actions(false);
	AtEOXact_Namespace(false);
	smgrabort();
	AtEOXact_Files();
	AtEOXact_ComboCid();
	AtEOXact_HashTables(false);
	AtEOXact_PgStat(false);
	//AtEOXact_Snapshot(false);
	pgstat_report_xact_timestamp(0);

	/*
	 * State remains TRANS_ABORT until CleanupTransaction().
	 */
	RESUME_INTERRUPTS();

	/* If memprot decides to kill process, make sure we destroy all processes
	 * so that all mem/resource will be freed
	 */
	if(elog_geterrcode() == ERRCODE_GP_MEMPROT_KILL)
		disconnectAndDestroyAllGangs();
}

/*
 *	CleanupTransaction
 */
static void
CleanupTransaction(void)
{
	TransactionState s = CurrentTransactionState;

	/*
	 * State should still be TRANS_ABORT from AbortTransaction().
	 */
	if (s->state != TRANS_ABORT)
		elog(FATAL, "CleanupTransaction: unexpected state %s",
			 TransStateAsString(s->state));

	/*
	 * do abort cleanup processing
	 */
	AtCleanup_Portals();		/* now safe to release portal memory */

	CurrentResourceOwner = GetTopResourceOwner();	/* and resource owner */
	if (TopTransactionResourceOwner)
		ResourceOwnerDelete(TopTransactionResourceOwner);
	s->curTransactionOwner = NULL;
	CurTransactionResourceOwner = NULL;
	TopTransactionResourceOwner = NULL;

	AtCleanup_Memory();			/* and transaction memory */

	s->distribXid = InvalidDistributedTransactionId;
	s->transactionId = InvalidTransactionId;
	s->subTransactionId = InvalidSubTransactionId;
	s->nestingLevel = 0;
	s->childXids = NIL;
	s->executorSaysXactDoesWrites = false;

	/*
	 * done with abort processing, set current transaction state back to
	 * default
	 */
	s->state = TRANS_DEFAULT;

	/*
	 * no distributed transaction in hawq
	 */
	/*finishDistributedTransactionContext("CleanupTransaction", true);*/

}

static void
SetBlockState(TransactionState s, TBlockState state)
{
//	elog(INFO, "Block State To : %d", state);
	s->blockState = state;
}

/*
 *	StartTransactionCommand
 */
void
StartTransactionCommand(void)
{
	TransactionState s = CurrentTransactionState;

	switch (s->blockState)
	{
		/*
		 * if we aren't in a transaction block, we just do our usual start
		 * transaction.
		 */
		case TBLOCK_DEFAULT:
			StartTransaction();
			SetBlockState(s, TBLOCK_STARTED );
			break;

			/*
			 * We are somewhere in a transaction block or subtransaction and
			 * about to start a new command.  For now we do nothing, but
			 * someday we may do command-local resource initialization. (Note
			 * that any needed CommandCounterIncrement was done by the
			 * previous CommitTransactionCommand.)
			 */
		case TBLOCK_INPROGRESS:
		case TBLOCK_SUBINPROGRESS:
			break;

			/*
			 * Here we are in a failed transaction block (one of the commands
			 * caused an abort) so we do nothing but remain in the abort
			 * state.  Eventually we will get a ROLLBACK command which will
			 * get us out of this state.  (It is up to other code to ensure
			 * that no commands other than ROLLBACK will be processed in these
			 * states.)
			 */
		case TBLOCK_ABORT:
		case TBLOCK_SUBABORT:
			break;

			/* These cases are invalid. */
		case TBLOCK_STARTED:
		case TBLOCK_BEGIN:
		case TBLOCK_SUBBEGIN:
		case TBLOCK_END:
		case TBLOCK_SUBEND:
		case TBLOCK_ABORT_END:
		case TBLOCK_SUBABORT_END:
		case TBLOCK_ABORT_PENDING:
		case TBLOCK_SUBABORT_PENDING:
		case TBLOCK_SUBRESTART:
		case TBLOCK_SUBABORT_RESTART:
		case TBLOCK_PREPARE:
			elog(ERROR, "StartTransactionCommand: unexpected state %s",
				 BlockStateAsString(s->blockState));
			break;
	}

	/*
	 * We must switch to CurTransactionContext before returning. This is
	 * already done if we called StartTransaction, otherwise not.
	 */
	Assert(CurTransactionContext != NULL);
	MemoryContextSwitchTo(CurTransactionContext);
}

/*
 *	CommitTransactionCommand
 */
void
CommitTransactionCommand(void)
{
	TransactionState s = CurrentTransactionState;

	if (Gp_role == GP_ROLE_EXECUTE && !Gp_is_writer)
		elog(DEBUG1,"CommitTransactionCommand: called as segment Reader in state %s",
		     BlockStateAsString(s->blockState));
	
	switch (s->blockState)
	{
		/*
		 * This shouldn't happen, because it means the previous
		 * StartTransactionCommand didn't set the STARTED state
		 * appropriately.
		 */
		case TBLOCK_DEFAULT:
			elog(FATAL, "CommitTransactionCommand: unexpected state %s",
				 BlockStateAsString(s->blockState));
			break;

			/*
			 * If we aren't in a transaction block, just do our usual
			 * transaction commit, and return to the idle state.
			 */
		case TBLOCK_STARTED:
			CommitTransaction();
			SetBlockState(s, TBLOCK_DEFAULT);
			break;

			/*
			 * We are completing a "BEGIN TRANSACTION" command, so we change
			 * to the "transaction block in progress" state and return.  (We
			 * assume the BEGIN did nothing to the database, so we need no
			 * CommandCounterIncrement.)
			 */
		case TBLOCK_BEGIN:
			SetBlockState(s, TBLOCK_INPROGRESS);
			break;

			/*
			 * This is the case when we have finished executing a command
			 * someplace within a transaction block.  We increment the command
			 * counter and return.
			 */
		case TBLOCK_INPROGRESS:
		case TBLOCK_SUBINPROGRESS:
			CommandCounterIncrement();
			break;

			/*
			 * We are completing a "COMMIT" command.  Do it and return to the
			 * idle state.
			 */
		case TBLOCK_END:
			CommitTransaction();
			SetBlockState(s, TBLOCK_DEFAULT );
			break;

			/*
			 * Here we are in the middle of a transaction block but one of the
			 * commands caused an abort so we do nothing but remain in the
			 * abort state.  Eventually we will get a ROLLBACK comand.
			 */
		case TBLOCK_ABORT:
		case TBLOCK_SUBABORT:
			break;

			/*
			 * Here we were in an aborted transaction block and we just got
			 * the ROLLBACK command from the user, so clean up the
			 * already-aborted transaction and return to the idle state.
			 */
		case TBLOCK_ABORT_END:
			CleanupTransaction();
			SetBlockState(s, TBLOCK_DEFAULT );
			break;

			/*
			 * Here we were in a perfectly good transaction block but the user
			 * told us to ROLLBACK anyway.	We have to abort the transaction
			 * and then clean up.
			 */
		case TBLOCK_ABORT_PENDING:
			AbortTransaction();
			CleanupTransaction();
			SetBlockState(s, TBLOCK_DEFAULT );
			break;

			/*
			 * We are completing a "PREPARE TRANSACTION" command.  Do it and
			 * return to the idle state.
			 */
		case TBLOCK_PREPARE:
			PrepareTransaction();
			SetBlockState(s, TBLOCK_DEFAULT );
			break;

			/*
			 * We were just issued a SAVEPOINT inside a transaction block.
			 * Start a subtransaction.	(DefineSavepoint already did
			 * PushTransaction, so as to have someplace to put the SUBBEGIN
			 * state.)
			 */
		case TBLOCK_SUBBEGIN:
			StartSubTransaction();
			SetBlockState(s, TBLOCK_SUBINPROGRESS);
			break;

			/*
			 * We were issued a COMMIT or RELEASE command, so we end the
			 * current subtransaction and return to the parent transaction.
			 * The parent might be ended too, so repeat till we are all the
			 * way out or find an INPROGRESS transaction.
			 */
		case TBLOCK_SUBEND:
			do
			{
				CommitSubTransaction();
				s = CurrentTransactionState;	/* changed by pop */
			} while (s->blockState == TBLOCK_SUBEND);
			/* If we had a COMMIT command, finish off the main xact too */
			if (s->blockState == TBLOCK_END)
			{
				Assert(s->parent == NULL);
				CommitTransaction();
				SetBlockState(s, TBLOCK_DEFAULT);
			}
			else if (s->blockState == TBLOCK_PREPARE)
			{
				Assert(s->parent == NULL);
				PrepareTransaction();
				SetBlockState(s, TBLOCK_DEFAULT);
			}
			else
			{
				Assert(s->blockState == TBLOCK_INPROGRESS ||
					   s->blockState == TBLOCK_SUBINPROGRESS);
			}
			break;

			/*
			 * The current already-failed subtransaction is ending due to a
			 * ROLLBACK or ROLLBACK TO command, so pop it and recursively
			 * examine the parent (which could be in any of several states).
			 */
		case TBLOCK_SUBABORT_END:
			CleanupSubTransaction();
			CommitTransactionCommand();
			break;

			/*
			 * As above, but it's not dead yet, so abort first.
			 */
		case TBLOCK_SUBABORT_PENDING:
			AbortSubTransaction();
			CleanupSubTransaction();
			CommitTransactionCommand();
			break;

			/*
			 * The current subtransaction is the target of a ROLLBACK TO
			 * command.  Abort and pop it, then start a new subtransaction
			 * with the same name.
			 */
		case TBLOCK_SUBRESTART:
		{
			char	   *name;
			int			savepointLevel;

			/* save name and keep Cleanup from freeing it */
			name = s->name;
			s->name = NULL;
			savepointLevel = s->savepointLevel;

			AbortSubTransaction();
			CleanupSubTransaction();

			DefineSavepoint(name);
			s = CurrentTransactionState;	/* changed by push */
			if (name)
			{
				pfree(name);
			}
			s->savepointLevel = savepointLevel;

			/* This is the same as TBLOCK_SUBBEGIN case */
			AssertState(s->blockState == TBLOCK_SUBBEGIN);
			StartSubTransaction();
			SetBlockState(s, TBLOCK_SUBINPROGRESS);
		}
		break;

		/*
		 * Same as above, but the subtransaction had already failed, so we
		 * don't need AbortSubTransaction.
		 */
		case TBLOCK_SUBABORT_RESTART:
		{
			char	   *name;
			int			savepointLevel;

			/* save name and keep Cleanup from freeing it */
			name = s->name;
			s->name = NULL;
			savepointLevel = s->savepointLevel;

			CleanupSubTransaction();

			DefineSavepoint(name);
			s = CurrentTransactionState;	/* changed by push */
			s->name = name;
			s->savepointLevel = savepointLevel;

			/* This is the same as TBLOCK_SUBBEGIN case */
			AssertState(s->blockState == TBLOCK_SUBBEGIN);
			StartSubTransaction();
			SetBlockState(s, TBLOCK_SUBINPROGRESS);
		}
		break;
	}
}

/*
 *	AbortCurrentTransaction
 */
void
AbortCurrentTransaction(void)
{
	TransactionState s = CurrentTransactionState;

	switch (s->blockState)
	{
		case TBLOCK_DEFAULT:
			if (s->state == TRANS_DEFAULT)
			{
				/* we are idle, so nothing to do */
			}
			else
			{
				/*
				 * We can get here after an error during transaction start
				 * (state will be TRANS_START).  Need to clean up the
				 * incompletely started transaction.  First, adjust the
				 * low-level state to suppress warning message from
				 * AbortTransaction.
				 */
				if (s->state == TRANS_START)
					s->state = TRANS_INPROGRESS;
				AbortTransaction();
				CleanupTransaction();
			}
			break;

			/*
			 * if we aren't in a transaction block, we just do the basic abort
			 * & cleanup transaction.
			 */
		case TBLOCK_STARTED:
			AbortTransaction();
			CleanupTransaction();
			SetBlockState(s, TBLOCK_DEFAULT );
			break;

			/*
			 * If we are in TBLOCK_BEGIN it means something screwed up right
			 * after reading "BEGIN TRANSACTION".  We assume that the user
			 * will interpret the error as meaning the BEGIN failed to get him
			 * into a transaction block, so we should abort and return to idle
			 * state.
			 */
		case TBLOCK_BEGIN:
			AbortTransaction();
			CleanupTransaction();
			SetBlockState(s, TBLOCK_DEFAULT);
			break;

			/*
			 * We are somewhere in a transaction block and we've gotten a
			 * failure, so we abort the transaction and set up the persistent
			 * ABORT state.  We will stay in ABORT until we get a ROLLBACK.
			 */
		case TBLOCK_INPROGRESS:
			AbortTransaction();
			SetBlockState(s, TBLOCK_ABORT);
			/* CleanupTransaction happens when we exit TBLOCK_ABORT_END */
			break;

			/*
			 * Here, we failed while trying to COMMIT.	Clean up the
			 * transaction and return to idle state (we do not want to stay in
			 * the transaction).
			 */
		case TBLOCK_END:
			AbortTransaction();
			CleanupTransaction();
			SetBlockState(s, TBLOCK_DEFAULT);
			break;

			/*
			 * Here, we are already in an aborted transaction state and are
			 * waiting for a ROLLBACK, but for some reason we failed again! So
			 * we just remain in the abort state.
			 */
		case TBLOCK_ABORT:
		case TBLOCK_SUBABORT:
			break;

			/*
			 * We are in a failed transaction and we got the ROLLBACK command.
			 * We have already aborted, we just need to cleanup and go to idle
			 * state.
			 */
		case TBLOCK_ABORT_END:
			CleanupTransaction();
			SetBlockState(s, TBLOCK_DEFAULT);
			break;

			/*
			 * We are in a live transaction and we got a ROLLBACK command.
			 * Abort, cleanup, go to idle state.
			 */
		case TBLOCK_ABORT_PENDING:
			AbortTransaction();
			CleanupTransaction();
			SetBlockState(s, TBLOCK_DEFAULT);
			break;

			/*
			 * Here, we failed while trying to PREPARE.  Clean up the
			 * transaction and return to idle state (we do not want to stay in
			 * the transaction).
			 */
		case TBLOCK_PREPARE:
			AbortTransaction();
			CleanupTransaction();
			SetBlockState(s, TBLOCK_DEFAULT);
			break;

			/*
			 * We got an error inside a subtransaction.  Abort just the
			 * subtransaction, and go to the persistent SUBABORT state until
			 * we get ROLLBACK.
			 */
		case TBLOCK_SUBINPROGRESS:
			AbortSubTransaction();
			SetBlockState(s, TBLOCK_SUBABORT);
			break;

			/*
			 * If we failed while trying to create a subtransaction, clean up
			 * the broken subtransaction and abort the parent.	The same
			 * applies if we get a failure while ending a subtransaction.
			 */
		case TBLOCK_SUBBEGIN:
		case TBLOCK_SUBEND:
		case TBLOCK_SUBABORT_PENDING:
		case TBLOCK_SUBRESTART:
			AbortSubTransaction();
			CleanupSubTransaction();
			AbortCurrentTransaction();
			break;

			/*
			 * Same as above, except the Abort() was already done.
			 */
		case TBLOCK_SUBABORT_END:
		case TBLOCK_SUBABORT_RESTART:
			CleanupSubTransaction();
			AbortCurrentTransaction();
			break;
	}
}

/*
 *	PreventTransactionChain
 *
 *	This routine is to be called by statements that must not run inside
 *	a transaction block, typically because they have non-rollback-able
 *	side effects or do internal commits.
 *
 *	If we have already started a transaction block, issue an error; also issue
 *	an error if we appear to be running inside a user-defined function (which
 *	could issue more commands and possibly cause a failure after the statement
 *	completes).  Subtransactions are verboten too.
 *
 *	stmtNode: pointer to parameter block for statement; this is used in
 *	a very klugy way to determine whether we are inside a function.
 *	stmtType: statement type name for error messages.
 */
void
PreventTransactionChain(void *stmtNode, const char *stmtType)
{
	/*
	 * xact block already started?
	 */
	if (IsTransactionBlock())
		ereport(ERROR,
				(errcode(ERRCODE_ACTIVE_SQL_TRANSACTION),
				 /* translator: %s represents an SQL statement name */
				 errmsg("%s cannot run inside a transaction block",
						stmtType),
								   errOmitLocation(true)));

	/*
	 * subtransaction?
	 */
	if (IsSubTransaction())
		ereport(ERROR,
				(errcode(ERRCODE_ACTIVE_SQL_TRANSACTION),
				 /* translator: %s represents an SQL statement name */
				 errmsg("%s cannot run inside a subtransaction",
						stmtType),
								   errOmitLocation(true)));

	/*
	 * Are we inside a function call?  If the statement's parameter block was
	 * allocated in QueryContext, assume it is an interactive command.
	 * Otherwise assume it is coming from a function.
	 */
	if (!MemoryContextContains(QueryContext, stmtNode))
		ereport(ERROR,
				(errcode(ERRCODE_ACTIVE_SQL_TRANSACTION),
				 /* translator: %s represents an SQL statement name */
				 errmsg("%s cannot be executed from a function", stmtType),
						   errOmitLocation(true)));

	/* If we got past IsTransactionBlock test, should be in default state */
	if (CurrentTransactionState->blockState != TBLOCK_DEFAULT &&
		CurrentTransactionState->blockState != TBLOCK_STARTED)
		elog(FATAL, "cannot prevent transaction chain");
	/* all okay */
}

/*
 *	RequireTransactionChain
 *
 *	This routine is to be called by statements that must run inside
 *	a transaction block, because they have no effects that persist past
 *	transaction end (and so calling them outside a transaction block
 *	is presumably an error).  DECLARE CURSOR is an example.
 *
 *	If we appear to be running inside a user-defined function, we do not
 *	issue an error, since the function could issue more commands that make
 *	use of the current statement's results.  Likewise subtransactions.
 *	Thus this is an inverse for PreventTransactionChain.
 *
 *	stmtNode: pointer to parameter block for statement; this is used in
 *	a very klugy way to determine whether we are inside a function.
 *	stmtType: statement type name for error messages.
 */
void
RequireTransactionChain(void *stmtNode, const char *stmtType)
{
	/*
	 * xact block already started?
	 */
	if (IsTransactionBlock())
		return;

	/*
	 * subtransaction?
	 */
	if (IsSubTransaction())
		return;

	/*
	 * Are we inside a function call?  If the statement's parameter block was
	 * allocated in QueryContext, assume it is an interactive command.
	 * Otherwise assume it is coming from a function.
	 */
	if (!MemoryContextContains(QueryContext, stmtNode))
		return;
	ereport(ERROR,
			(errcode(ERRCODE_NO_ACTIVE_SQL_TRANSACTION),
			 /* translator: %s represents an SQL statement name */
			 errmsg("%s may only be used in transaction blocks",
					stmtType)));
}

/*
 *	IsInTransactionChain
 *
 *	This routine is for statements that need to behave differently inside
 *	a transaction block than when running as single commands.  ANALYZE is
 *	currently the only example.
 *
 *	stmtNode: pointer to parameter block for statement; this is used in
 *	a very klugy way to determine whether we are inside a function.
 */
bool
IsInTransactionChain(void *stmtNode)
{
	/*
	 * Return true on same conditions that would make PreventTransactionChain
	 * error out
	 */
	if (IsTransactionBlock())
		return true;

	if (IsSubTransaction())
		return true;

	if (!MemoryContextContains(QueryContext, stmtNode))
		return true;

	if (CurrentTransactionState->blockState != TBLOCK_DEFAULT &&
		CurrentTransactionState->blockState != TBLOCK_STARTED)
		return true;

	return false;
}


/*
 * Register or deregister callback functions for start- and end-of-xact
 * operations.
 *
 * These functions are intended for use by dynamically loaded modules.
 * For built-in modules we generally just hardwire the appropriate calls
 * (mainly because it's easier to control the order that way, where needed).
 *
 * At transaction end, the callback occurs post-commit or post-abort, so the
 * callback functions can only do noncritical cleanup.
 */
void
RegisterXactCallback(XactCallback callback, void *arg)
{
	XactCallbackItem *item;

	item = (XactCallbackItem *)
		MemoryContextAlloc(TopMemoryContext, sizeof(XactCallbackItem));
	item->callback = callback;
	item->arg = arg;
	item->next = Xact_callbacks;
	Xact_callbacks = item;
}

void
UnregisterXactCallback(XactCallback callback, void *arg)
{
	XactCallbackItem *item;
	XactCallbackItem *prev;

	prev = NULL;
	for (item = Xact_callbacks; item; prev = item, item = item->next)
	{
		if (item->callback == callback && item->arg == arg)
		{
			if (prev)
				prev->next = item->next;
			else
				Xact_callbacks = item->next;
			pfree(item);
			break;
		}
	}
}

static void
CallXactCallbacks(XactEvent event)
{
	XactCallbackItem *item;

	for (item = Xact_callbacks; item; item = item->next)
		(*item->callback) (event, item->arg);
}

/* Register or deregister callback functions for start/end Xact.  Call only once. */
void
RegisterXactCallbackOnce(XactCallback callback, void *arg)
{
	XactCallbackItem *item;

	item = (XactCallbackItem *)
		MemoryContextAlloc(TopMemoryContext, sizeof(XactCallbackItem));
	item->callback = callback;
	item->arg = arg;
	item->next = Xact_callbacks_once;
	Xact_callbacks_once = item;
}

void
UnregisterXactCallbackOnce(XactCallback callback, void *arg)
{
	XactCallbackItem *item;
	XactCallbackItem *prev;

	prev = NULL;
	for (item = Xact_callbacks_once; item; prev = item, item = item->next)
	{
		if (item->callback == callback && item->arg == arg)
		{
			if (prev)
				prev->next = item->next;
			else
				Xact_callbacks_once = item->next;
			pfree(item);
			break;
		}
	}
}

static void
CallXactCallbacksOnce(XactEvent event)
{
	/* currently callback once should ignore prepare. */
	if (event == XACT_EVENT_PREPARE)
		return;

	while(Xact_callbacks_once)
	{
		XactCallbackItem *next = Xact_callbacks_once->next;
		XactCallback callback=Xact_callbacks_once->callback;
		void*arg=Xact_callbacks_once->arg;
		pfree(Xact_callbacks_once);
		Xact_callbacks_once = next;
		callback(event,arg);
	}
}

/*
 * Register or deregister callback functions for start- and end-of-subxact
 * operations.
 *
 * Pretty much same as above, but for subtransaction events.
 *
 * At subtransaction end, the callback occurs post-subcommit or post-subabort,
 * so the callback functions can only do noncritical cleanup.  At
 * subtransaction start, the callback is called when the subtransaction has
 * finished initializing.
 */
void
RegisterSubXactCallback(SubXactCallback callback, void *arg)
{
	SubXactCallbackItem *item;

	item = (SubXactCallbackItem *)
		MemoryContextAlloc(TopMemoryContext, sizeof(SubXactCallbackItem));
	item->callback = callback;
	item->arg = arg;
	item->next = SubXact_callbacks;
	SubXact_callbacks = item;
}

void
UnregisterSubXactCallback(SubXactCallback callback, void *arg)
{
	SubXactCallbackItem *item;
	SubXactCallbackItem *prev;

	prev = NULL;
	for (item = SubXact_callbacks; item; prev = item, item = item->next)
	{
		if (item->callback == callback && item->arg == arg)
		{
			if (prev)
				prev->next = item->next;
			else
				SubXact_callbacks = item->next;
			pfree(item);
			break;
		}
	}
}

static void
CallSubXactCallbacks(SubXactEvent event,
					 SubTransactionId mySubid,
					 SubTransactionId parentSubid)
{
	SubXactCallbackItem *item;

	for (item = SubXact_callbacks; item; item = item->next)
		(*item->callback) (event, mySubid, parentSubid, item->arg);
}


/* ----------------------------------------------------------------
 *					   transaction block support
 * ----------------------------------------------------------------
 */

/*
 *	BeginTransactionBlock
 *		This executes a BEGIN command.
 */
void
BeginTransactionBlock(void)
{
	TransactionState s = CurrentTransactionState;

	switch (s->blockState)
	{
		/*
		 * We are not inside a transaction block, so allow one to begin.
		 */
		case TBLOCK_STARTED:
			SetBlockState(s, TBLOCK_BEGIN );
			break;

			/*
			 * Already a transaction block in progress.
			 */
		case TBLOCK_INPROGRESS:
		case TBLOCK_SUBINPROGRESS:
		case TBLOCK_ABORT:
		case TBLOCK_SUBABORT:
			if (Gp_role == GP_ROLE_EXECUTE)
				ereport(DEBUG1,
						(errcode(ERRCODE_ACTIVE_SQL_TRANSACTION),
						 errmsg("there is already a transaction in progress")));
			else
				ereport(WARNING,
						(errcode(ERRCODE_ACTIVE_SQL_TRANSACTION),
						 errmsg("there is already a transaction in progress")));
			break;

			/* These cases are invalid. */
		case TBLOCK_DEFAULT:
		case TBLOCK_BEGIN:
		case TBLOCK_SUBBEGIN:
		case TBLOCK_END:
		case TBLOCK_SUBEND:
		case TBLOCK_ABORT_END:
		case TBLOCK_SUBABORT_END:
		case TBLOCK_ABORT_PENDING:
		case TBLOCK_SUBABORT_PENDING:
		case TBLOCK_SUBRESTART:
		case TBLOCK_SUBABORT_RESTART:
		case TBLOCK_PREPARE:
			elog(FATAL, "BeginTransactionBlock: unexpected state %s",
				 BlockStateAsString(s->blockState));
			break;
	}
}

/*
 *	PrepareTransactionBlock
 *		This executes a PREPARE command.
 *
 * Since PREPARE may actually do a ROLLBACK, the result indicates what
 * happened: TRUE for PREPARE, FALSE for ROLLBACK.
 *
 * Note that we don't actually do anything here except change blockState.
 * The real work will be done in the upcoming PrepareTransaction().
 * We do it this way because it's not convenient to change memory context,
 * resource owner, etc while executing inside a Portal.
 */
bool
PrepareTransactionBlock(char *gid)
{
	TransactionState s;
	bool		result;

	/* Set up to commit the current transaction */
	result = EndTransactionBlock();

	/* If successful, change outer tblock state to PREPARE */
	if (result)
	{
		s = CurrentTransactionState;

		while (s->parent != NULL)
			s = s->parent;

		if (s->blockState == TBLOCK_END)
		{
			/* Save GID where PrepareTransaction can find it again */
			prepareGID = MemoryContextStrdup(TopTransactionContext, gid);

			SetBlockState(s, TBLOCK_PREPARE);
		}
		else
		{
			/*
			 * ignore case where we are not in a transaction;
			 * EndTransactionBlock already issued a warning.
			 */
			Assert(s->blockState == TBLOCK_STARTED);
			/* Don't send back a PREPARE result tag... */
			result = false;
		}
	}

	return result;
}

/*
 *	EndTransactionBlock
 *		This executes a COMMIT command.
 *
 * Since COMMIT may actually do a ROLLBACK, the result indicates what
 * happened: TRUE for COMMIT, FALSE for ROLLBACK.
 *
 * Note that we don't actually do anything here except change blockState.
 * The real work will be done in the upcoming CommitTransactionCommand().
 * We do it this way because it's not convenient to change memory context,
 * resource owner, etc while executing inside a Portal.
 */
bool
EndTransactionBlock(void)
{
	TransactionState s = CurrentTransactionState;
	bool		result = false;

	switch (s->blockState)
	{
		/*
		 * We are in a transaction block, so tell CommitTransactionCommand
		 * to COMMIT.
		 */
		case TBLOCK_INPROGRESS:
			SetBlockState(s, TBLOCK_END);
			result = true;
			break;

			/*
			 * We are in a failed transaction block.  Tell
			 * CommitTransactionCommand it's time to exit the block.
			 */
		case TBLOCK_ABORT:
			SetBlockState(s, TBLOCK_ABORT_END);
			break;

			/*
			 * We are in a live subtransaction block.  Set up to subcommit all
			 * open subtransactions and then commit the main transaction.
			 */
		case TBLOCK_SUBINPROGRESS:
			while (s->parent != NULL)
			{
				if (s->blockState == TBLOCK_SUBINPROGRESS)
					SetBlockState(s, TBLOCK_SUBEND );
				else
					elog(FATAL, "EndTransactionBlock: unexpected state %s",
						 BlockStateAsString(s->blockState));
				s = s->parent;
			}
			if (s->blockState == TBLOCK_INPROGRESS)
				SetBlockState(s, TBLOCK_END);
			else
				elog(FATAL, "EndTransactionBlock: unexpected state %s",
					 BlockStateAsString(s->blockState));
			result = true;
			break;

			/*
			 * Here we are inside an aborted subtransaction.  Treat the COMMIT
			 * as ROLLBACK: set up to abort everything and exit the main
			 * transaction.
			 */
		case TBLOCK_SUBABORT:
			while (s->parent != NULL)
			{
				if (s->blockState == TBLOCK_SUBINPROGRESS)
					SetBlockState(s, TBLOCK_SUBABORT_PENDING);
				else if (s->blockState == TBLOCK_SUBABORT)
					SetBlockState(s, TBLOCK_SUBABORT_END);
				else
					elog(FATAL, "EndTransactionBlock: unexpected state %s",
						 BlockStateAsString(s->blockState));
				s = s->parent;
			}
			if (s->blockState == TBLOCK_INPROGRESS)
				SetBlockState(s, TBLOCK_ABORT_PENDING);
			else if (s->blockState == TBLOCK_ABORT)
				SetBlockState(s, TBLOCK_ABORT_END);
			else
				elog(FATAL, "EndTransactionBlock: unexpected state %s",
					 BlockStateAsString(s->blockState));
			break;

			/*
			 * The user issued COMMIT when not inside a transaction.  Issue a
			 * WARNING, staying in TBLOCK_STARTED state.  The upcoming call to
			 * CommitTransactionCommand() will then close the transaction and
			 * put us back into the default state.
			 */
		case TBLOCK_STARTED:
			if (Gp_role == GP_ROLE_EXECUTE)
			{
				ereport(DEBUG2,
						(errcode(ERRCODE_NO_ACTIVE_SQL_TRANSACTION),
						 errmsg("there is no transaction in progress")));
			}
			else
				ereport(WARNING,
						(errcode(ERRCODE_NO_ACTIVE_SQL_TRANSACTION),
						 errmsg("there is no transaction in progress")));
			result = true;
			break;

			/* These cases are invalid. */
		case TBLOCK_DEFAULT:
		case TBLOCK_BEGIN:
		case TBLOCK_SUBBEGIN:
		case TBLOCK_END:
		case TBLOCK_SUBEND:
		case TBLOCK_ABORT_END:
		case TBLOCK_SUBABORT_END:
		case TBLOCK_ABORT_PENDING:
		case TBLOCK_SUBABORT_PENDING:
		case TBLOCK_SUBRESTART:
		case TBLOCK_SUBABORT_RESTART:
		case TBLOCK_PREPARE:
			elog(FATAL, "EndTransactionBlock: unexpected state %s",
				 BlockStateAsString(s->blockState));
			break;
	}

	return result;
}

/*
 *	UserAbortTransactionBlock
 *		This executes a ROLLBACK command.
 *
 * As above, we don't actually do anything here except change blockState.
 */
void
UserAbortTransactionBlock(void)
{
	TransactionState s = CurrentTransactionState;

	switch (s->blockState)
	{
		/*
		 * We are inside a transaction block and we got a ROLLBACK command
		 * from the user, so tell CommitTransactionCommand to abort and
		 * exit the transaction block.
		 */
		case TBLOCK_INPROGRESS:
			SetBlockState(s, TBLOCK_ABORT_PENDING);
			break;

			/*
			 * We are inside a failed transaction block and we got a ROLLBACK
			 * command from the user.  Abort processing is already done, so
			 * CommitTransactionCommand just has to cleanup and go back to
			 * idle state.
			 */
		case TBLOCK_ABORT:
			SetBlockState(s, TBLOCK_ABORT_END);
			break;

			/*
			 * We are inside a subtransaction.	Mark everything up to top
			 * level as exitable.
			 */
		case TBLOCK_SUBINPROGRESS:
		case TBLOCK_SUBABORT:
			while (s->parent != NULL)
			{
				if (s->blockState == TBLOCK_SUBINPROGRESS)
					SetBlockState(s, TBLOCK_SUBABORT_PENDING);
				else if (s->blockState == TBLOCK_SUBABORT)
					SetBlockState(s, TBLOCK_SUBABORT_END);
				else
					elog(FATAL, "UserAbortTransactionBlock: unexpected state %s",
						 BlockStateAsString(s->blockState));
				s = s->parent;
			}
			if (s->blockState == TBLOCK_INPROGRESS)
				SetBlockState(s, TBLOCK_ABORT_PENDING);
			else if (s->blockState == TBLOCK_ABORT)
				SetBlockState(s, TBLOCK_ABORT_END);
			else
				elog(FATAL, "UserAbortTransactionBlock: unexpected state %s",
					 BlockStateAsString(s->blockState));
			break;

			/*
			 * The user issued ABORT when not inside a transaction. Issue a
			 * WARNING and go to abort state.  The upcoming call to
			 * CommitTransactionCommand() will then put us back into the
			 * default state.
			 */
		case TBLOCK_STARTED:
			if (Gp_role == GP_ROLE_EXECUTE)
			{
				ereport(DEBUG2,
						(errcode(ERRCODE_NO_ACTIVE_SQL_TRANSACTION),
						 errmsg("there is no transaction in progress")));
			}
			else
				ereport(WARNING,
						(errcode(ERRCODE_NO_ACTIVE_SQL_TRANSACTION),
						 errmsg("there is no transaction in progress")));
			SetBlockState(s, TBLOCK_ABORT_PENDING);
			break;

			/* These cases are invalid. */
		case TBLOCK_DEFAULT:
		case TBLOCK_BEGIN:
		case TBLOCK_SUBBEGIN:
		case TBLOCK_END:
		case TBLOCK_SUBEND:
		case TBLOCK_ABORT_END:
		case TBLOCK_SUBABORT_END:
		case TBLOCK_ABORT_PENDING:
		case TBLOCK_SUBABORT_PENDING:
		case TBLOCK_SUBRESTART:
		case TBLOCK_SUBABORT_RESTART:
		case TBLOCK_PREPARE:
			elog(FATAL, "UserAbortTransactionBlock: unexpected state %s",
				 BlockStateAsString(s->blockState));
			break;
	}
}

void
DefineDispatchSavepoint(char *name)
{
	TransactionState s = CurrentTransactionState;

	if ((s->blockState != TBLOCK_INPROGRESS) &&
	    (s->blockState != TBLOCK_SUBINPROGRESS))
	{
		elog(FATAL, "DefineSavepoint: unexpected state %s",
			    BlockStateAsString(s->blockState));
	}	

	DefineSavepoint(name);
}

/*
 * DefineSavepoint
 *		This executes a SAVEPOINT command.
 */
void
DefineSavepoint(char *name)
{
	TransactionState s = CurrentTransactionState;

	switch (s->blockState)
	{
		case TBLOCK_INPROGRESS:
		case TBLOCK_SUBINPROGRESS:

			/* Normal subtransaction start */
			PushTransaction();
			s = CurrentTransactionState;		/* changed by push */

			/*
			 * Savepoint names, like the TransactionState block itself, live
			 * in TopTransactionContext.
			 */
			if (name)
				s->name = MemoryContextStrdup(TopTransactionContext, name);
			break;

			/* These cases are invalid. */
		case TBLOCK_DEFAULT:
		case TBLOCK_STARTED:
		case TBLOCK_BEGIN:
		case TBLOCK_SUBBEGIN:
		case TBLOCK_END:
		case TBLOCK_SUBEND:
		case TBLOCK_ABORT:
		case TBLOCK_SUBABORT:
		case TBLOCK_ABORT_END:
		case TBLOCK_SUBABORT_END:
		case TBLOCK_ABORT_PENDING:
		case TBLOCK_SUBABORT_PENDING:
		case TBLOCK_SUBRESTART:
		case TBLOCK_SUBABORT_RESTART:
		case TBLOCK_PREPARE:
			elog(FATAL, "DefineSavepoint: unexpected state %s",
				 BlockStateAsString(s->blockState));
			break;
	}
}

/*
 * ReleaseSavepoint
 *		This executes a RELEASE command.
 *
 * As above, we don't actually do anything here except change blockState.
 */
void
ReleaseSavepoint(List *options)
{
	TransactionState s = CurrentTransactionState;
	TransactionState target,
		xact;
	ListCell   *cell;
	char	   *name = NULL;

	switch (s->blockState)
	{
		/*
		 * We can't rollback to a savepoint if there is no savepoint
		 * defined.
		 */
		case TBLOCK_INPROGRESS:
			ereport(ERROR,
					(errcode(ERRCODE_S_E_INVALID_SPECIFICATION),
					 errmsg("no such savepoint"),
							   errOmitLocation(true)));
			break;

			/*
			 * We are in a non-aborted subtransaction.	This is the only valid
			 * case.
			 */
		case TBLOCK_SUBINPROGRESS:
			break;

			/* These cases are invalid. */
		case TBLOCK_DEFAULT:
		case TBLOCK_STARTED:
		case TBLOCK_BEGIN:
		case TBLOCK_SUBBEGIN:
		case TBLOCK_END:
		case TBLOCK_SUBEND:
		case TBLOCK_ABORT:
		case TBLOCK_SUBABORT:
		case TBLOCK_ABORT_END:
		case TBLOCK_SUBABORT_END:
		case TBLOCK_ABORT_PENDING:
		case TBLOCK_SUBABORT_PENDING:
		case TBLOCK_SUBRESTART:
		case TBLOCK_SUBABORT_RESTART:
		case TBLOCK_PREPARE:
			elog(FATAL, "ReleaseSavepoint: unexpected state %s",
				 BlockStateAsString(s->blockState));
			break;
	}

	foreach(cell, options)
	{
		DefElem    *elem = lfirst(cell);

		if (strcmp(elem->defname, "savepoint_name") == 0)
			name = strVal(elem->arg);
	}

	Assert(PointerIsValid(name));

	for (target = s; PointerIsValid(target); target = target->parent)
	{
		if (PointerIsValid(target->name) && strcmp(target->name, name) == 0)
			break;
	}

	if (!PointerIsValid(target))
		ereport(ERROR,
				(errcode(ERRCODE_S_E_INVALID_SPECIFICATION),
				 errmsg("no such savepoint")));

	/* disallow crossing savepoint level boundaries */
	if (target->savepointLevel != s->savepointLevel)
		ereport(ERROR,
				(errcode(ERRCODE_S_E_INVALID_SPECIFICATION),
				 errmsg("no such savepoint")));

	/*
	 * Mark "commit pending" all subtransactions up to the target
	 * subtransaction.	The actual commits will happen when control gets to
	 * CommitTransactionCommand.
	 */
	xact = CurrentTransactionState;
	for (;;)
	{
		Assert(xact->blockState == TBLOCK_SUBINPROGRESS);
		xact->blockState = TBLOCK_SUBEND;
		if (xact == target)
			break;
		xact = xact->parent;
		Assert(PointerIsValid(xact));
	}
}

/*
 * RollbackToSavepoint
 *		This executes a ROLLBACK TO <savepoint> command.
 *
 * As above, we don't actually do anything here except change blockState.
 */
void
RollbackToSavepoint(List *options)
{
	TransactionState s = CurrentTransactionState;
	TransactionState target,
		xact;
	ListCell   *cell;
	char	   *name = NULL;

	switch (s->blockState)
	{
		/*
		 * We can't rollback to a savepoint if there is no savepoint
		 * defined.
		 */
		case TBLOCK_INPROGRESS:
		case TBLOCK_ABORT:
			ereport(ERROR,
					(errcode(ERRCODE_S_E_INVALID_SPECIFICATION),
					 errmsg("no such savepoint")));
			break;

			/*
			 * There is at least one savepoint, so proceed.
			 */
		case TBLOCK_SUBINPROGRESS:
		case TBLOCK_SUBABORT:
			break;

			/* These cases are invalid. */
		case TBLOCK_DEFAULT:
		case TBLOCK_STARTED:
		case TBLOCK_BEGIN:
		case TBLOCK_SUBBEGIN:
		case TBLOCK_END:
		case TBLOCK_SUBEND:
		case TBLOCK_ABORT_END:
		case TBLOCK_SUBABORT_END:
		case TBLOCK_ABORT_PENDING:
		case TBLOCK_SUBABORT_PENDING:
		case TBLOCK_SUBRESTART:
		case TBLOCK_SUBABORT_RESTART:
		case TBLOCK_PREPARE:
			elog(FATAL, "RollbackToSavepoint: unexpected state %s",
				 BlockStateAsString(s->blockState));
			break;
	}

	foreach(cell, options)
	{
		DefElem    *elem = lfirst(cell);

		if (strcmp(elem->defname, "savepoint_name") == 0)
			name = strVal(elem->arg);
	}

	Assert(PointerIsValid(name));

	for (target = s; PointerIsValid(target); target = target->parent)
	{
		if (PointerIsValid(target->name) && strcmp(target->name, name) == 0)
			break;
	}

	if (!PointerIsValid(target))
		ereport(ERROR,
				(errcode(ERRCODE_S_E_INVALID_SPECIFICATION),
				 errmsg("no such savepoint")));

	/* disallow crossing savepoint level boundaries */
	if (target->savepointLevel != s->savepointLevel)
		ereport(ERROR,
				(errcode(ERRCODE_S_E_INVALID_SPECIFICATION),
				 errmsg("no such savepoint")));

	/*
	 * Mark "abort pending" all subtransactions up to the target
	 * subtransaction.	The actual aborts will happen when control gets to
	 * CommitTransactionCommand.
	 */
	xact = CurrentTransactionState;
	for (;;)
	{
		if (xact == target)
			break;
		if (xact->blockState == TBLOCK_SUBINPROGRESS)
			xact->blockState = TBLOCK_SUBABORT_PENDING;
		else if (xact->blockState == TBLOCK_SUBABORT)
			xact->blockState = TBLOCK_SUBABORT_END;
		else
			elog(FATAL, "RollbackToSavepoint: unexpected state %s",
				 BlockStateAsString(xact->blockState));
		xact = xact->parent;
		Assert(PointerIsValid(xact));
	}

	/* And mark the target as "restart pending" */
	if (xact->blockState == TBLOCK_SUBINPROGRESS)
		xact->blockState = TBLOCK_SUBRESTART;
	else if (xact->blockState == TBLOCK_SUBABORT)
		xact->blockState = TBLOCK_SUBABORT_RESTART;
	else
		elog(FATAL, "RollbackToSavepoint: unexpected state %s",
			 BlockStateAsString(xact->blockState));
}

/*
 * BeginInternalSubTransaction
 *		This is the same as DefineSavepoint except it allows TBLOCK_STARTED,
 *		TBLOCK_END, and TBLOCK_PREPARE states, and therefore it can safely be
 *		used in functions that might be called when not inside a BEGIN block
 *		or when running deferred triggers at COMMIT/PREPARE time.  Also, it
 *		automatically does CommitTransactionCommand/StartTransactionCommand
 *		instead of expecting the caller to do it.
 */
void
BeginInternalSubTransaction(char *name)
{
	TransactionState s = CurrentTransactionState;

	switch (s->blockState)
	{
		case TBLOCK_STARTED:
		case TBLOCK_INPROGRESS:
		case TBLOCK_END:
		case TBLOCK_PREPARE:
		case TBLOCK_SUBINPROGRESS:
			/* Normal subtransaction start */
			PushTransaction();
			s = CurrentTransactionState;		/* changed by push */

			/*
			 * Savepoint names, like the TransactionState block itself, live
			 * in TopTransactionContext.
			 */
			if (name)
				s->name = MemoryContextStrdup(TopTransactionContext, name);
			break;

			/* These cases are invalid. */
		case TBLOCK_DEFAULT:
		case TBLOCK_BEGIN:
		case TBLOCK_SUBBEGIN:
		case TBLOCK_SUBEND:
		case TBLOCK_ABORT:
		case TBLOCK_SUBABORT:
		case TBLOCK_ABORT_END:
		case TBLOCK_SUBABORT_END:
		case TBLOCK_ABORT_PENDING:
		case TBLOCK_SUBABORT_PENDING:
		case TBLOCK_SUBRESTART:
		case TBLOCK_SUBABORT_RESTART:
			elog(FATAL, "BeginInternalSubTransaction: unexpected state %s",
				 BlockStateAsString(s->blockState));
			break;
	}

	CommitTransactionCommand();
	StartTransactionCommand();
}

/*
 * ReleaseCurrentSubTransaction
 *
 * RELEASE (ie, commit) the innermost subtransaction, regardless of its
 * savepoint name (if any).
 * NB: do NOT use CommitTransactionCommand/StartTransactionCommand with this.
 */
void
ReleaseCurrentSubTransaction(void)
{
	TransactionState s = CurrentTransactionState;

	if (s->blockState != TBLOCK_SUBINPROGRESS)
		elog(ERROR, "ReleaseCurrentSubTransaction: unexpected state %s",
			 BlockStateAsString(s->blockState));
	Assert(s->state == TRANS_INPROGRESS);
	MemoryContextSwitchTo(CurTransactionContext);
	CommitSubTransaction();
	s = CurrentTransactionState;	/* changed by pop */
	Assert(s->state == TRANS_INPROGRESS);
}

/*
 * RollbackAndReleaseCurrentSubTransaction
 *
 * ROLLBACK and RELEASE (ie, abort) the innermost subtransaction, regardless
 * of its savepoint name (if any).
 * NB: do NOT use CommitTransactionCommand/StartTransactionCommand with this.
 */
void
RollbackAndReleaseCurrentSubTransaction(void)
{
	TransactionState s = CurrentTransactionState;

	switch (s->blockState)
	{
		/* Must be in a subtransaction */
		case TBLOCK_SUBINPROGRESS:
		case TBLOCK_SUBABORT:
			break;

			/* These cases are invalid. */
		case TBLOCK_DEFAULT:
		case TBLOCK_STARTED:
		case TBLOCK_BEGIN:
		case TBLOCK_SUBBEGIN:
		case TBLOCK_INPROGRESS:
		case TBLOCK_END:
		case TBLOCK_SUBEND:
		case TBLOCK_ABORT:
		case TBLOCK_ABORT_END:
		case TBLOCK_SUBABORT_END:
		case TBLOCK_ABORT_PENDING:
		case TBLOCK_SUBABORT_PENDING:
		case TBLOCK_SUBRESTART:
		case TBLOCK_SUBABORT_RESTART:
		case TBLOCK_PREPARE:
			elog(FATAL, "RollbackAndReleaseCurrentSubTransaction: unexpected state %s",
				 BlockStateAsString(s->blockState));
			break;
	}

	/*
	 * Abort the current subtransaction, if needed.
	 */
	if (s->blockState == TBLOCK_SUBINPROGRESS)
		AbortSubTransaction();

	/* And clean it up, too */
	CleanupSubTransaction();

	s = CurrentTransactionState;	/* changed by pop */
	AssertState(s->blockState == TBLOCK_SUBINPROGRESS ||
				s->blockState == TBLOCK_INPROGRESS ||
				s->blockState == TBLOCK_STARTED);
}

/*
 *	AbortOutOfAnyTransaction
 *
 *	This routine is provided for error recovery purposes.  It aborts any
 *	active transaction or transaction block, leaving the system in a known
 *	idle state.
 */
void
AbortOutOfAnyTransaction(void)
{
	TransactionState s = CurrentTransactionState;

	/*
	 * Get out of any transaction or nested transaction
	 */
	do
	{
		switch (s->blockState)
		{
			case TBLOCK_DEFAULT:
				/* Not in a transaction, do nothing */
				break;
			case TBLOCK_STARTED:
			case TBLOCK_BEGIN:
			case TBLOCK_INPROGRESS:
			case TBLOCK_END:
			case TBLOCK_ABORT_PENDING:
			case TBLOCK_PREPARE:
				/* In a transaction, so clean up */
				AbortTransaction();
				CleanupTransaction();
				SetBlockState(s, TBLOCK_DEFAULT);
				break;
			case TBLOCK_ABORT:
			case TBLOCK_ABORT_END:
				/* AbortTransaction already done, still need Cleanup */
				CleanupTransaction();
				SetBlockState(s, TBLOCK_DEFAULT);
				break;

				/*
				 * In a subtransaction, so clean it up and abort parent too
				 */
			case TBLOCK_SUBBEGIN:
			case TBLOCK_SUBINPROGRESS:
			case TBLOCK_SUBEND:
			case TBLOCK_SUBABORT_PENDING:
			case TBLOCK_SUBRESTART:
				AbortSubTransaction();
				CleanupSubTransaction();
				s = CurrentTransactionState;	/* changed by pop */
				break;

			case TBLOCK_SUBABORT:
			case TBLOCK_SUBABORT_END:
			case TBLOCK_SUBABORT_RESTART:
				/* As above, but AbortSubTransaction already done */
				CleanupSubTransaction();
				s = CurrentTransactionState;	/* changed by pop */
				break;
		}
	} while (s->blockState != TBLOCK_DEFAULT);

	/* Should be out of all subxacts now */
	Assert(s->parent == NULL);
}

/*
 * IsTransactionBlock --- are we within a transaction block?
 */
bool
IsTransactionBlock(void)
{
	TransactionState s = CurrentTransactionState;

	if (s->blockState == TBLOCK_DEFAULT || s->blockState == TBLOCK_STARTED)
		return false;

	return true;
}

/*
 * IsTransactionOrTransactionBlock --- are we within either a transaction
 * or a transaction block?	(The backend is only really "idle" when this
 * returns false.)
 *
 * This should match up with IsTransactionBlock and IsTransactionState.
 */
bool
IsTransactionOrTransactionBlock(void)
{
	TransactionState s = CurrentTransactionState;

	if (s->blockState == TBLOCK_DEFAULT)
		return false;

	return true;
}

/*
 * Did the transaction do work that requires a commit record to be written?
 */
bool
IsTransactionDirty(void)
{
	bool result = (MyXactMadeXLogEntry || MyXactMadeTempRelUpdate || smgrIsPendingFileSysWork(EndXactRecKind_Commit));

	elog((Debug_print_full_dtm ? LOG : DEBUG5), "IsTransactionDirty: TopTransactionId %u, dirty = %s",
		 GetTopTransactionId(), (result ? "true" : "false"));
	
	return result;
}

void
ExecutorMarkTransactionUsesSequences(void)
{
	seqXlogWrite = true;
	MyXactMadeXLogEntry = true;
}

void 
ExecutorMarkTransactionDoesWrites(void)
{
	// UNDONE: Verify we are in transaction...
	if (!TopTransactionStateData.executorSaysXactDoesWrites)
	{
		elog((Debug_print_full_dtm ? LOG : DEBUG5), "ExecutorMarkTransactionDoesWrites called");
		TopTransactionStateData.executorSaysXactDoesWrites = true;
	}
}

bool
ExecutorSaysTransactionDoesWrites(void)
{
	return TopTransactionStateData.executorSaysXactDoesWrites;
}

/*
 * TransactionBlockStatusCode - return status code to send in ReadyForQuery
 */
char
TransactionBlockStatusCode(void)
{
	TransactionState s = CurrentTransactionState;

	switch (s->blockState)
	{
		case TBLOCK_DEFAULT:
		case TBLOCK_STARTED:
			return 'I';			/* idle --- not in transaction */
		case TBLOCK_BEGIN:
		case TBLOCK_SUBBEGIN:
		case TBLOCK_INPROGRESS:
		case TBLOCK_SUBINPROGRESS:
		case TBLOCK_END:
		case TBLOCK_SUBEND:
		case TBLOCK_PREPARE:
			return 'T';			/* in transaction */
		case TBLOCK_ABORT:
		case TBLOCK_SUBABORT:
		case TBLOCK_ABORT_END:
		case TBLOCK_SUBABORT_END:
		case TBLOCK_ABORT_PENDING:
		case TBLOCK_SUBABORT_PENDING:
		case TBLOCK_SUBRESTART:
		case TBLOCK_SUBABORT_RESTART:
			return 'E';			/* in failed transaction */
	}

	/* should never get here */
	elog(FATAL, "invalid transaction block state: %s",
		 BlockStateAsString(s->blockState));
	return 0;					/* keep compiler quiet */
}

void
TransactionInformationQEWriter(DistributedTransactionId *QEDistributedTransactionId, CommandId *QECommandId, bool *QEDirty)
{
	*QEDistributedTransactionId = 1;
	*QECommandId = 1;
	*QEDirty = IsTransactionDirty();
}

/*
 * IsSubTransaction
 */
bool
IsSubTransaction(void)
{
	TransactionState s = CurrentTransactionState;

	if (s->nestingLevel >= 2)
		return true;

	return false;
}

/*
 * StartSubTransaction
 *
 * If you're wondering why this is separate from PushTransaction: it's because
 * we can't conveniently do this stuff right inside DefineSavepoint.  The
 * SAVEPOINT utility command will be executed inside a Portal, and if we
 * muck with CurrentMemoryContext or CurrentResourceOwner then exit from
 * the Portal will undo those settings.  So we make DefineSavepoint just
 * push a dummy transaction block, and when control returns to the main
 * idle loop, CommitTransactionCommand will be called, and we'll come here
 * to finish starting the subtransaction.
 */
static void
StartSubTransaction(void)
{
	TransactionState s = CurrentTransactionState;

	if (s->state != TRANS_DEFAULT)
		elog(WARNING, "StartSubTransaction while in %s state",
			 TransStateAsString(s->state));

	s->state = TRANS_START;

	/*
	 * Initialize subsystems for new subtransaction
	 *
	 * must initialize resource-management stuff first
	 */
	AtSubStart_Memory();
	AtSubStart_ResourceOwner();
	AtSubStart_Inval();
	AtSubStart_Notify();
	AfterTriggerBeginSubXact();

	s->state = TRANS_INPROGRESS;

	/*
	 * Call start-of-subxact callbacks
	 */
	CallSubXactCallbacks(SUBXACT_EVENT_START_SUB, s->subTransactionId,
						 s->parent->subTransactionId);

	ShowTransactionState("StartSubTransaction");
}

/*
 * CommitSubTransaction
 *
 *	The caller has to make sure to always reassign CurrentTransactionState
 *	if it has a local pointer to it after calling this function.
 */
static void
CommitSubTransaction(void)
{
	TransactionState s = CurrentTransactionState;

	ShowTransactionState("CommitSubTransaction");

	if (s->state != TRANS_INPROGRESS)
		elog(WARNING, "CommitSubTransaction while in %s state",
			 TransStateAsString(s->state));

#ifdef FAULT_INJECTOR
		FaultInjector_InjectFaultIfSet(
			SubtransactionRelease,
			DDLNotSpecified,
			"",  // databaseName
			""); // tableName
#endif

	/* Pre-commit processing goes here -- nothing to do at the moment */

	s->state = TRANS_COMMIT;

	/* Must CCI to ensure commands of subtransaction are seen as done */
	CommandCounterIncrement();

	/* Mark subtransaction as subcommitted */
	if (TransactionIdIsValid(s->transactionId))
	{
		RecordSubTransactionCommit();
		AtSubCommit_childXids();
	}

	AtCommit_AppendOnly(true);
	/* Post-commit cleanup */
	AfterTriggerEndSubXact(true);
	AtSubCommit_Portals(s->subTransactionId,
						s->parent->subTransactionId,
						s->parent->curTransactionOwner);
	AtEOSubXact_LargeObject(true, s->subTransactionId,
							s->parent->subTransactionId);
	AtSubCommit_Notify();
	AtEOSubXact_UpdateFlatFiles(true, s->subTransactionId,
								s->parent->subTransactionId);

	CallSubXactCallbacks(SUBXACT_EVENT_COMMIT_SUB, s->subTransactionId,
						 s->parent->subTransactionId);

	ResourceOwnerRelease(s->curTransactionOwner,
						 RESOURCE_RELEASE_BEFORE_LOCKS,
						 true, false);
	AtEOSubXact_RelationCache(true, s->subTransactionId,
							  s->parent->subTransactionId);
	AtEOSubXact_Inval(true);
	AtSubCommit_smgr();

	/*
	 * The only lock we actually release here is the subtransaction XID lock.
	 * The rest just get transferred to the parent resource owner.
	 */
	CurrentResourceOwner = s->curTransactionOwner;
	if (TransactionIdIsValid(s->transactionId))
		XactLockTableDelete(s->transactionId);

	ResourceOwnerRelease(s->curTransactionOwner,
						 RESOURCE_RELEASE_LOCKS,
						 true, false);
	ResourceOwnerRelease(s->curTransactionOwner,
						 RESOURCE_RELEASE_AFTER_LOCKS,
						 true, false);

	AtEOXact_GUC(true, true);
	AtEOSubXact_SPI(true, s->subTransactionId);
	AtEOSubXact_on_commit_actions(true, s->subTransactionId,
								  s->parent->subTransactionId);
	AtEOSubXact_Namespace(true, s->subTransactionId,
						  s->parent->subTransactionId);
	AtEOSubXact_Files(true, s->subTransactionId,
					  s->parent->subTransactionId);
	AtEOSubXact_HashTables(true, s->nestingLevel);
	AtEOSubXact_PgStat(true, s->nestingLevel);

	/*
	 * We need to restore the upper transaction's read-only state, in case the
	 * upper is read-write while the child is read-only; GUC will incorrectly
	 * think it should leave the child state in place.
	 */
	XactReadOnly = s->prevXactReadOnly;

	CurrentResourceOwner = s->parent->curTransactionOwner;
	CurTransactionResourceOwner = s->parent->curTransactionOwner;
	ResourceOwnerDelete(s->curTransactionOwner);
	s->curTransactionOwner = NULL;

	AtSubCommit_Memory();

	s->state = TRANS_DEFAULT;

	PopTransaction();
}

/*
 * AbortSubTransaction
 */
static void
AbortSubTransaction(void)
{
	TransactionState s = CurrentTransactionState;

	/* Prevent cancel/die interrupt while cleaning up */
	HOLD_INTERRUPTS();

	/* Make sure we have a valid memory context and resource owner */
	AtSubAbort_Memory();
	AtSubAbort_ResourceOwner();
	AtAbort_AppendOnly(true);
	/* In sub transaction, ActiveQueryResource should not be set NULL
	 * sub transaction inherits resource from parent.
	 * parent transaction may still use ActiveQueryResource to reference the original resource.
	 */
	// AtSubAbort_ActiveQueryResource();

	/*
	 * Release any LW locks we might be holding as quickly as possible.
	 * (Regular locks, however, must be held till we finish aborting.)
	 * Releasing LW locks is critical since we might try to grab them again
	 * while cleaning up!
	 *
	 * FIXME This may be incorrect --- Are there some locks we should keep?
	 * Buffer locks, for example?  I don't think so but I'm not sure.
	 */
	LWLockReleaseAll();

	AbortBufferIO();
	UnlockBuffers();

	LockWaitCancel();

	/*
	 * check the current transaction state
	 */
	ShowTransactionState("AbortSubTransaction");

	if (s->state != TRANS_INPROGRESS)
		elog(WARNING, "AbortSubTransaction while in %s state",
			 TransStateAsString(s->state));

#ifdef FAULT_INJECTOR
		FaultInjector_InjectFaultIfSet(
			SubtransactionRollback,
			DDLNotSpecified,
			"",  // databaseName
			""); // tableName
#endif

	s->state = TRANS_ABORT;

	/*
	 * Reset user ID which might have been changed transiently.  (See notes in
	 * AbortTransaction.)
	 */
	SetUserIdAndContext(s->prevUser, s->prevSecDefCxt);

	/*
	 * We can skip all this stuff if the subxact failed before creating a
	 * ResourceOwner...
	 */
	if (s->curTransactionOwner)
	{
		AfterTriggerEndSubXact(false);
		AtSubAbort_Portals(s->subTransactionId,
						   s->parent->subTransactionId,
						   s->parent->curTransactionOwner);
		AtEOSubXact_LargeObject(false, s->subTransactionId,
								s->parent->subTransactionId);
		AtSubAbort_Notify();
		AtEOSubXact_UpdateFlatFiles(false, s->subTransactionId,
									s->parent->subTransactionId);

		/* Advertise the fact that we aborted in pg_clog. */
		if (TransactionIdIsValid(s->transactionId))
		{
			RecordSubTransactionAbort();
			AtSubAbort_childXids();
		}

		/* Post-abort cleanup */
		CallSubXactCallbacks(SUBXACT_EVENT_ABORT_SUB, s->subTransactionId,
							 s->parent->subTransactionId);

		ResourceOwnerRelease(s->curTransactionOwner,
							 RESOURCE_RELEASE_BEFORE_LOCKS,
							 false, false);
		AtEOSubXact_RelationCache(false, s->subTransactionId,
								  s->parent->subTransactionId);
		AtEOSubXact_Inval(false);
		AtSubAbort_smgr();
		ResourceOwnerRelease(s->curTransactionOwner,
							 RESOURCE_RELEASE_LOCKS,
							 false, false);
		ResourceOwnerRelease(s->curTransactionOwner,
							 RESOURCE_RELEASE_AFTER_LOCKS,
							 false, false);

		AtEOXact_GUC(false, true);
		AtEOSubXact_SPI(false, s->subTransactionId);
		AtEOSubXact_on_commit_actions(false, s->subTransactionId,
									  s->parent->subTransactionId);
		AtEOSubXact_Namespace(false, s->subTransactionId,
							  s->parent->subTransactionId);
		AtEOSubXact_Files(false, s->subTransactionId,
						  s->parent->subTransactionId);
		AtEOSubXact_HashTables(false, s->nestingLevel);
		AtEOSubXact_PgStat(false, s->nestingLevel);
	}

	/*
	 * Restore the upper transaction's read-only state, too.  This should be
	 * redundant with GUC's cleanup but we may as well do it for consistency
	 * with the commit case.
	 */
	XactReadOnly = s->prevXactReadOnly;

	RESUME_INTERRUPTS();
}

/*
 * CleanupSubTransaction
 *
 *	The caller has to make sure to always reassign CurrentTransactionState
 *	if it has a local pointer to it after calling this function.
 */
static void
CleanupSubTransaction(void)
{
	TransactionState s = CurrentTransactionState;

	ShowTransactionState("CleanupSubTransaction");

	if (s->state != TRANS_ABORT)
		elog(WARNING, "CleanupSubTransaction while in %s state",
			 TransStateAsString(s->state));

	AtSubCleanup_Portals(s->subTransactionId);

	CurrentResourceOwner = s->parent->curTransactionOwner;
	CurTransactionResourceOwner = s->parent->curTransactionOwner;
	if (s->curTransactionOwner)
		ResourceOwnerDelete(s->curTransactionOwner);
	s->curTransactionOwner = NULL;

	AtSubCleanup_Memory();

	s->state = TRANS_DEFAULT;

	PopTransaction();
}

/*
 * PushTransaction
 *		Create transaction state stack entry for a subtransaction
 *
 *	The caller has to make sure to always reassign CurrentTransactionState
 *	if it has a local pointer to it after calling this function.
 */
static void
PushTransaction(void)
{
	TransactionState p = CurrentTransactionState;
	TransactionState s;

	currentSavepointTotal++;
	if (currentSavepointTotal >= MaxGpSavePoints)
		elog(DEBUG5, "Using subtransaction file for storing subtransactions");

	if ((currentSavepointTotal >= gp_subtrans_warn_limit) &&
	    (currentSavepointTotal % gp_subtrans_warn_limit == 0))
	{
		ereport(WARNING,
		(errmsg("Using too many subtransactions in one transaction."),
		errhint("Close open transactions soon to avoid wraparound "
			"problems.")));
	}

	/*
	 * We keep subtransaction state nodes in TopTransactionContext.
	 */
	s = (TransactionState)
		MemoryContextAllocZero(TopTransactionContext,
							   sizeof(TransactionStateData));

	/*
	 * Assign a subtransaction ID, watching out for counter wraparound.
	 */
	currentSubTransactionId += 1;
	if (currentSubTransactionId == InvalidSubTransactionId)
	{
		currentSubTransactionId -= 1;
		pfree(s);
		ereport(ERROR,
				(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
				 errmsg("cannot have more than 2^32-1 subtransactions in a transaction")));
	}

	/*
	 * We can now stack a minimally valid subtransaction without fear of
	 * failure.
	 */
	s->distribXid = p->distribXid;
	s->transactionId = InvalidTransactionId;	/* until assigned */
	s->subTransactionId = currentSubTransactionId;
	s->parent = p;
	s->nestingLevel = p->nestingLevel + 1;
	s->savepointLevel = p->savepointLevel;
	s->state = TRANS_DEFAULT;
	SetBlockState(s, TBLOCK_SUBBEGIN);
	GetUserIdAndContext(&s->prevUser, &s->prevSecDefCxt);
	s->prevXactReadOnly = XactReadOnly;
	s->executorSaysXactDoesWrites = false;

	fastNodeCount++;
	if (fastNodeCount == NUM_NODES_TO_SKIP_FOR_FAST_SEARCH)
	{
		fastNodeCount = 0;
		s->fastLink = previousFastLink;
		previousFastLink = s;
	}

	CurrentTransactionState = s;

	/*
	 * AbortSubTransaction and CleanupSubTransaction have to be able to cope
	 * with the subtransaction from here on out; in particular they should not
	 * assume that it necessarily has a transaction context, resource owner,
	 * or XID.
	 */
}

/*
 * PopTransaction
 *		Pop back to parent transaction state
 *
 *	The caller has to make sure to always reassign CurrentTransactionState
 *	if it has a local pointer to it after calling this function.
 */
static void
PopTransaction(void)
{
	TransactionState s = CurrentTransactionState;

	if (s->state != TRANS_DEFAULT)
		elog(WARNING, "PopTransaction while in %s state",
			 TransStateAsString(s->state));

	if (s->parent == NULL)
		elog(FATAL, "PopTransaction with no parent");

	CurrentTransactionState = s->parent;

	/* Let's just make sure CurTransactionContext is good */
	CurTransactionContext = s->parent->curTransactionContext;
	MemoryContextSwitchTo(CurTransactionContext);

	/* Ditto for ResourceOwner links */
	CurTransactionResourceOwner = s->parent->curTransactionOwner;
	CurrentResourceOwner = s->parent->curTransactionOwner;

	if (fastNodeCount)
	{
		fastNodeCount--;
	}

	/*
	 * Deleting node where last fastLink is stored
	 * hence retrive the fastLink to update in node to be added next
	 */
	if (previousFastLink == s)
	{
		fastNodeCount = NUM_NODES_TO_SKIP_FOR_FAST_SEARCH - 1;
		previousFastLink = s->fastLink;
	}

	/* Free the old child structure */
	if (s->name)
		pfree(s->name);
	pfree(s);
}

/*
 * ShowTransactionState
 *		Debug support
 */
static void
ShowTransactionState(const char *str)
{
	/* skip work if message will definitely not be printed */
	if (log_min_messages <= DEBUG3 || client_min_messages <= DEBUG3)
	{
		elog(DEBUG3, "%s", str);
		ShowTransactionStateRec(CurrentTransactionState);
	}
}

/*
 * ShowTransactionStateRec
 *		Recursive subroutine for ShowTransactionState
 */
static void
ShowTransactionStateRec(TransactionState s)
{
	if (s->parent)
		ShowTransactionStateRec(s->parent);

	/* use ereport to suppress computation if msg will not be printed */
	ereport(DEBUG3,
			(errmsg_internal("name: %s; blockState: %13s; state: %7s, xid/subid/cid: %u/%u/%u, nestlvl: %d, children: %s",
							 PointerIsValid(s->name) ? s->name : "unnamed",
							 BlockStateAsString(s->blockState),
							 TransStateAsString(s->state),
							 (unsigned int) s->transactionId,
							 (unsigned int) s->subTransactionId,
							 (unsigned int) currentCommandId,
							 s->nestingLevel,
							 nodeToString(s->childXids))));
}

/*
 * BlockStateAsString
 *		Debug support
 */
static const char *
BlockStateAsString(TBlockState blockState)
{
	switch (blockState)
	{
		case TBLOCK_DEFAULT:
			return "DEFAULT";
		case TBLOCK_STARTED:
			return "STARTED";
		case TBLOCK_BEGIN:
			return "BEGIN";
		case TBLOCK_INPROGRESS:
			return "INPROGRESS";
		case TBLOCK_END:
			return "END";
		case TBLOCK_ABORT:
			return "ABORT";
		case TBLOCK_ABORT_END:
			return "ABORT END";
		case TBLOCK_ABORT_PENDING:
			return "ABORT PEND";
		case TBLOCK_PREPARE:
			return "PREPARE";
		case TBLOCK_SUBBEGIN:
			return "SUB BEGIN";
		case TBLOCK_SUBINPROGRESS:
			return "SUB INPROGRS";
		case TBLOCK_SUBEND:
			return "SUB END";
		case TBLOCK_SUBABORT:
			return "SUB ABORT";
		case TBLOCK_SUBABORT_END:
			return "SUB ABORT END";
		case TBLOCK_SUBABORT_PENDING:
			return "SUB ABRT PEND";
		case TBLOCK_SUBRESTART:
			return "SUB RESTART";
		case TBLOCK_SUBABORT_RESTART:
			return "SUB AB RESTRT";
	}
	return "UNRECOGNIZED";
}

/*
 * TransStateAsString
 *		Debug support
 */
static const char *
TransStateAsString(TransState state)
{
	switch (state)
	{
		case TRANS_DEFAULT:
			return "DEFAULT";
		case TRANS_START:
			return "START";
		case TRANS_INPROGRESS:
			return "INPROGR";
		case TRANS_COMMIT:
			return "COMMIT";
		case TRANS_ABORT:
			return "ABORT";
		case TRANS_PREPARE:
			return "PREPARE";
	}
	return "UNRECOGNIZED";
}

/*
 * IsoLevelAsUpperString
 *		Formatting helper.
 */
const char *
IsoLevelAsUpperString(int IsoLevel)
{
	switch (IsoLevel)
	{
		case XACT_READ_UNCOMMITTED:
			return "READ UNCOMMITTED";
		case XACT_READ_COMMITTED:
			return "READ COMMITTED";
		case XACT_REPEATABLE_READ:
			return "REPEATABLE READ";
		case XACT_SERIALIZABLE:
			return "SERIALIZABLE";
		default:
			return "UNKNOWN";
	}
}


/*
 * xactGetCommittedChildren
 *
 * Gets the list of committed children of the current transaction.	The return
 * value is the number of child transactions.  *ptr is set to point to an
 * array of TransactionIds.  The array is allocated in TopTransactionContext;
 * the caller should *not* pfree() it (this is a change from pre-8.4 code!).
 * If there are no subxacts, *ptr is set to NULL.
 */
int
xactGetCommittedChildren(TransactionId **ptr)
{
	TransactionState s = CurrentTransactionState;
	int			nchildren;
	TransactionId *children;
	ListCell   *p;

	nchildren = list_length(s->childXids);
	if (nchildren == 0)
	{
		*ptr = NULL;
		return 0;
	}

	children = (TransactionId *) palloc(nchildren * sizeof(TransactionId));
	*ptr = children;

	foreach(p, s->childXids)
	{
		TransactionId child = lfirst_xid(p);

		*children++ = child;
	}

	return nchildren;
}


static void
xact_redo_commit(xl_xact_commit *xlrec, TransactionId xid)
{
	uint8 *data;
	PersistentEndXactRecObjects persistentCommitObjects;
	TransactionId *sub_xids;
	TransactionId max_xid;
	int			i;

	TransactionIdCommit(xid);

	data = xlrec->data;
	PersistentEndXactRec_Deserialize(
								data,
								xlrec->persistentCommitObjectCount,
								&persistentCommitObjects,
								&data);
	
	if (Debug_persistent_print)
		PersistentEndXactRec_Print("xact_redo_commit", &persistentCommitObjects);

	sub_xids = (TransactionId *)data; 

	/* Mark committed subtransactions as committed */
	TransactionIdCommitTree(xlrec->nsubxacts, sub_xids);

	/* Make sure nextXid is beyond any XID mentioned in the record */
	max_xid = xid;
	for (i = 0; i < xlrec->nsubxacts; i++)
	{
		if (TransactionIdPrecedes(max_xid, sub_xids[i]))
			max_xid = sub_xids[i];
	}
	if (TransactionIdFollowsOrEquals(max_xid,
									 ShmemVariableCache->nextXid))
	{
		ShmemVariableCache->nextXid = max_xid;
		TransactionIdAdvance(ShmemVariableCache->nextXid);
	}
}

static void
xact_redo_abort(xl_xact_abort *xlrec, TransactionId xid)
{
	uint8 *data;
	PersistentEndXactRecObjects persistentAbortObjects;
	TransactionId *sub_xids;
	TransactionId max_xid;
	int			i;

	TransactionIdAbort(xid);

	data = xlrec->data;
	PersistentEndXactRec_Deserialize(
								data,
								xlrec->persistentAbortObjectCount,
								&persistentAbortObjects,
								&data);
	
	if (Debug_persistent_print)
		PersistentEndXactRec_Print("xact_redo_abort", &persistentAbortObjects);

	sub_xids = (TransactionId *)data; 

	/* Mark subtransactions as aborted */
	TransactionIdAbortTree(xlrec->nsubxacts, sub_xids);

	/* Make sure nextXid is beyond any XID mentioned in the record */
	max_xid = xid;
	for (i = 0; i < xlrec->nsubxacts; i++)
	{
		if (TransactionIdPrecedes(max_xid, sub_xids[i]))
			max_xid = sub_xids[i];
	}
	if (TransactionIdFollowsOrEquals(max_xid,
									 ShmemVariableCache->nextXid))
	{
		ShmemVariableCache->nextXid = max_xid;
		TransactionIdAdvance(ShmemVariableCache->nextXid);
	}
}

void
xact_redo(XLogRecPtr beginLoc __attribute__((unused)), XLogRecPtr lsn __attribute__((unused)), XLogRecord *record)
{
	uint8		info = record->xl_info & ~XLR_INFO_MASK;

	if (info == XLOG_XACT_COMMIT)
	{
		xl_xact_commit *xlrec = (xl_xact_commit *) XLogRecGetData(record);

		xact_redo_commit(xlrec, record->xl_xid);
	}
	else if (info == XLOG_XACT_ABORT)
	{
		xl_xact_abort *xlrec = (xl_xact_abort *) XLogRecGetData(record);

		xact_redo_abort(xlrec, record->xl_xid);
	}
	else if (info == XLOG_XACT_PREPARE)
	{
		/* the record contents are exactly the 2PC file */
		RecreateTwoPhaseFile(record->xl_xid,
				     XLogRecGetData(record), record->xl_len, &beginLoc);
	}
	else if (info == XLOG_XACT_COMMIT_PREPARED)
	{
		xl_xact_commit_prepared *xlrec = (xl_xact_commit_prepared *) XLogRecGetData(record);

		xact_redo_commit(&xlrec->crec, xlrec->xid);
		RemoveTwoPhaseFile(xlrec->xid, false);
	}
	else if (info == XLOG_XACT_ABORT_PREPARED)
	{
		xl_xact_abort_prepared *xlrec = (xl_xact_abort_prepared *) XLogRecGetData(record);

		xact_redo_abort(&xlrec->arec, xlrec->xid);
		RemoveTwoPhaseFile(xlrec->xid, false);
	}
	else
		elog(PANIC, "xact_redo: unknown op code %u", info);
}

static void
xact_redo_get_commit_info(
	xl_xact_commit *xlrec,

	PersistentEndXactRecObjects *persistentCommitObjects,

	TransactionId 	**subXids,

	int 			*subXidCount)
{
	uint8 *data;

	data = xlrec->data;
	PersistentEndXactRec_Deserialize(
								data,
								xlrec->persistentCommitObjectCount,
								persistentCommitObjects,
								&data);
	
	*subXids = (TransactionId *)data;
	*subXidCount = xlrec->nsubxacts;
}

static void
xact_redo_get_abort_info(
	xl_xact_abort *xlrec,

	PersistentEndXactRecObjects *persistentAbortObjects,

	TransactionId 	**subXids,

	int 			*subXidCount)
{
	uint8 *data;

	data = xlrec->data;
	PersistentEndXactRec_Deserialize(
								data,
								xlrec->persistentAbortObjectCount,
								persistentAbortObjects,
								&data);
	
	*subXids = (TransactionId *)data;
	*subXidCount = xlrec->nsubxacts;
}

bool
xact_redo_get_info(
	XLogRecord 					*record,

	XactInfoKind				*infoKind,

	TransactionId				*xid,

	PersistentEndXactRecObjects *persistentObjects,

	TransactionId 				**subXids,

	int 						*subXidCount)
{
	uint8		info = record->xl_info & ~XLR_INFO_MASK;

	if (info == XLOG_XACT_COMMIT)
	{
		xl_xact_commit *xlrec = (xl_xact_commit *) XLogRecGetData(record);

		xact_redo_get_commit_info(
								xlrec, 
								persistentObjects,
								subXids,
								subXidCount);
		*infoKind = XACT_INFOKIND_COMMIT;
		*xid = record->xl_xid;
	}
	else if (info == XLOG_XACT_ABORT)
	{
		xl_xact_abort *xlrec = (xl_xact_abort *) XLogRecGetData(record);

		xact_redo_get_abort_info(
								xlrec, 
								persistentObjects,
								subXids,
								subXidCount);
		*infoKind = XACT_INFOKIND_ABORT;
		*xid = record->xl_xid;
	}
	else if (info == XLOG_XACT_PREPARE)
	{
		// UNDONE: For now, return no prepare information
		MemSet(persistentObjects, 0, sizeof(PersistentEndXactRecObjects));
		subXids = NULL;
		subXidCount = 0;

		*infoKind = XACT_INFOKIND_PREPARE;
		*xid = record->xl_xid;
	}
	else if (info == XLOG_XACT_COMMIT_PREPARED)
	{
		xl_xact_commit_prepared *xlrec = (xl_xact_commit_prepared *) XLogRecGetData(record);

		xact_redo_get_commit_info(
								&xlrec->crec, 
								persistentObjects,
								subXids,
								subXidCount);
		*infoKind = XACT_INFOKIND_COMMIT;
		*xid = xlrec->xid;
	}
	else if (info == XLOG_XACT_ABORT_PREPARED)
	{
		xl_xact_abort_prepared *xlrec = (xl_xact_abort_prepared *) XLogRecGetData(record);

		xact_redo_get_abort_info(
								&xlrec->arec, 
								persistentObjects,
								subXids,
								subXidCount);
		*infoKind = XACT_INFOKIND_ABORT;
		*xid = xlrec->xid;
	}
	else
	{
		*infoKind = XACT_INFOKIND_NONE;
		*xid = InvalidTransactionId;
		return false;
	}

	return true;
}

static char*
xact_desc_commit(StringInfo buf, xl_xact_commit *xlrec)
{
	struct tm  *tm = localtime(&xlrec->xtime);
	int			i;
	uint8 *data;
	PersistentEndXactRecObjects persistentCommitObjects;
	TransactionId *sub_xids;

	appendStringInfo(buf, "%04u-%02u-%02u %02u:%02u:%02u",
					 tm->tm_year + 1900, tm->tm_mon + 1, tm->tm_mday,
					 tm->tm_hour, tm->tm_min, tm->tm_sec);
	data = xlrec->data;
	appendStringInfo(buf, "; persistent commit object count = %d",
					 xlrec->persistentCommitObjectCount);
	PersistentEndXactRec_Deserialize(
								data,
								xlrec->persistentCommitObjectCount,
								&persistentCommitObjects,
								&data);
	
	sub_xids = (TransactionId *)data; 

	if (persistentCommitObjects.typed.fileSysActionInfosCount > 0)
	{
		appendStringInfo(buf, "; drop file-system objects:");
		for (i = 0; i < persistentCommitObjects.typed.fileSysActionInfosCount; i++)
		{
			PersistentEndXactFileSysActionInfo	*fileSysActionInfo =
						&persistentCommitObjects.typed.fileSysActionInfos[i];

			if (fileSysActionInfo->action == PersistentEndXactFileSysAction_Drop)
			{
				if (i > 10)
				{
					appendStringInfo(buf, " ...");
					break;
				}

				appendStringInfo(buf, " %s",
								 PersistentFileSysObjName_TypeAndObjectName(&fileSysActionInfo->fsObjName));
			}
		}
	}

	if (xlrec->nsubxacts > 0)
	{
		appendStringInfo(buf, "; subxacts:");
		for (i = 0; i < xlrec->nsubxacts; i++)
		{
			if (i > 10)
			{
				appendStringInfo(buf, " ...");
				break;
			}

			appendStringInfo(buf, " %u", sub_xids[i]);
		}
	}
	
	/*
	 * MPP: Return end of regular commit information.
	 */
	return (char*) &sub_xids[xlrec->nsubxacts];
}

static void
xact_desc_abort(StringInfo buf, xl_xact_abort *xlrec)
{
	struct tm  *tm = localtime(&xlrec->xtime);
	uint8 *data;
	PersistentEndXactRecObjects persistentAbortObjects;
	TransactionId *sub_xids;
	int			i;

	appendStringInfo(buf, "%04u-%02u-%02u %02u:%02u:%02u",
					 tm->tm_year + 1900, tm->tm_mon + 1, tm->tm_mday,
					 tm->tm_hour, tm->tm_min, tm->tm_sec);
	data = xlrec->data;
	appendStringInfo(buf, "; persistent abort object count = %d",
					 xlrec->persistentAbortObjectCount);
	PersistentEndXactRec_Deserialize(
								data,
								xlrec->persistentAbortObjectCount,
								&persistentAbortObjects,
								&data);
	
	sub_xids = (TransactionId *)data; 

	if (persistentAbortObjects.typed.fileSysActionInfosCount > 0)
	{
		appendStringInfo(buf, "; aborted create file-system objects:");
		for (i = 0; i < persistentAbortObjects.typed.fileSysActionInfosCount; i++)
		{
			PersistentEndXactFileSysActionInfo	*fileSysActionInfo =
						&persistentAbortObjects.typed.fileSysActionInfos[i];
		
			if (i > 10)
			{
				appendStringInfo(buf, " ...");
				break;
			}

			appendStringInfo(buf, " %s",
							 PersistentFileSysObjName_TypeAndObjectName(&fileSysActionInfo->fsObjName));
		}
	}
	if (xlrec->nsubxacts > 0)
	{
		appendStringInfo(buf, "; subxacts:");
		for (i = 0; i < xlrec->nsubxacts; i++)
		{
			if (i > 10)
			{
				appendStringInfo(buf, " ...");
				break;
			}

			appendStringInfo(buf, " %u", sub_xids[i]);
		}
	}
}

void
xact_desc(StringInfo buf, XLogRecPtr beginLoc, XLogRecord *record)
{
	uint8		info = record->xl_info & ~XLR_INFO_MASK;
	char		*rec = XLogRecGetData(record);

	if (info == XLOG_XACT_COMMIT)
	{
		xl_xact_commit *xlrec = (xl_xact_commit *) rec;

		appendStringInfo(buf, "commit: ");
		xact_desc_commit(buf, xlrec);
	}
	else if (info == XLOG_XACT_ABORT)
	{
		xl_xact_abort *xlrec = (xl_xact_abort *) rec;

		appendStringInfo(buf, "abort: ");
		xact_desc_abort(buf, xlrec);
	}
	else if (info == XLOG_XACT_PREPARE)
	{
		appendStringInfo(buf, "prepare");
	}
	else if (info == XLOG_XACT_COMMIT_PREPARED)
	{
		xl_xact_commit_prepared *xlrec = (xl_xact_commit_prepared *) rec;

		appendStringInfo(buf, "commit %u: ", xlrec->xid);
		xact_desc_commit(buf, &xlrec->crec);
	}
	else if (info == XLOG_XACT_ABORT_PREPARED)
	{
		xl_xact_abort_prepared *xlrec = (xl_xact_abort_prepared *) rec;

		appendStringInfo(buf, "abort %u: ", xlrec->xid);
		xact_desc_abort(buf, &xlrec->arec);
	}
	else
		appendStringInfo(buf, "UNKNOWN");
}
