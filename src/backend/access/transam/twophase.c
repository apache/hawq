/*-------------------------------------------------------------------------
 *
 * Twophase.c
 *              Two-phase commit support functions.
 *
 * Portions Copyright (c) 1996-2008, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *              $PostgreSQL: pgsql/src/backend/access/transam/twophase.c,v 1.25.2.1 2007/02/13 19:39:48 tgl Exp $
 *
 * NOTES
 *              Each global transaction is associated with a global transaction
 *              identifier (GID). The client assigns a GID to a postgres
 *              transaction with the PREPARE TRANSACTION command.
 *
 *              We keep all active global transactions in a shared memory array.
 *              When the PREPARE TRANSACTION command is issued, the GID is
 *              reserved for the transaction in the array. This is done before
 *              a WAL entry is made, because the reservation checks for duplicate
 *              GIDs and aborts the transaction if there already is a global
 *              transaction in prepared state with the same GID.
 *
 *              A global transaction (gxact) also has a dummy PGPROC that is entered
 *              into the ProcArray array; this is what keeps the XID considered
 *              running by TransactionIdIsInProgress.  It is also convenient as a
 *              PGPROC to hook the gxact's locks to.
 *
 *              In order to survive crashes and shutdowns, all prepared
 *              transactions must be stored in permanent storage. This includes
 *              locking information, pending notifications etc. All that state
 *              information is written to the per-transaction state file in
 *              the pg_twophase directory.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <fcntl.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <time.h>
#include <unistd.h>

#include "access/heapam.h"
#include "access/xlogmm.h"
#include "access/subtrans.h"
#include "access/transam.h"
#include "access/twophase.h"
#include "access/twophase_rmgr.h"
#include "access/xact.h"
#include "catalog/pg_type.h"
#include "funcapi.h"
#include "miscadmin.h"
#include "pgstat.h"
#include "storage/fd.h"
#include "storage/procarray.h"
#include "storage/smgr.h"
#include "utils/builtins.h"
#include "utils/faultinjector.h"
#include "utils/guc.h"
#include "utils/memutils.h"

#include "cdb/cdbvars.h"
#include "cdb/cdbpersistentrecovery.h"

#include "cdb/cdbpersistentrelfile.h"
#include "cdb/cdbmirroredflatfile.h"
#include "cdb/cdbmirroredfilesysobj.h"
#include "cdb/cdbpersistentfilesysobj.h"
#include "postmaster/primary_mirror_mode.h"

/* GUC variable, can't be changed after startup */
int                     max_prepared_xacts = 5;

/*
 * This struct describes one global transaction that is in prepared state
 * or attempting to become prepared.
 *
 * The first component of the struct is a dummy PGPROC that is inserted
 * into the global ProcArray so that the transaction appears to still be
 * running and holding locks.  It must be first because we cast pointers
 * to PGPROC and pointers to GlobalTransactionData back and forth.
 *
 * The lifecycle of a global transaction is:
 *
 * 1. After checking that the requested GID is not in use, set up an
 * entry in the TwoPhaseState->prepXacts array with the correct XID and GID,
 * with locking_xid = my own XID and valid = false.
 *
 * 2. After successfully completing prepare, set valid = true and enter the
 * contained PGPROC into the global ProcArray.
 *
 * 3. To begin COMMIT PREPARED or ROLLBACK PREPARED, check that the entry
 * is valid and its locking_xid is no longer active, then store my current
 * XID into locking_xid.  This prevents concurrent attempts to commit or
 * rollback the same prepared xact.
 *
 * 4. On completion of COMMIT PREPARED or ROLLBACK PREPARED, remove the entry
 * from the ProcArray and the TwoPhaseState->prepXacts array and return it to
 * the freelist.
 *
 * Note that if the preparing transaction fails between steps 1 and 2, the
 * entry will remain in prepXacts until recycled.  We can detect recyclable
 * entries by checking for valid = false and locking_xid no longer active.
 *
 * typedef struct GlobalTransactionData *GlobalTransaction appears in
 * twophase.h
 */
#define GIDSIZE 200

extern List *expectedTLIs;


typedef struct GlobalTransactionData
{
        PGPROC          proc;                   /* dummy proc */
        TimestampTz prepared_at;        /* time of preparation */
        XLogRecPtr  prepare_begin_lsn;  /* XLOG begging offset of prepare record */
        XLogRecPtr      prepare_lsn;    /* XLOG offset of prepare record */
        Oid                     owner;                  /* ID of user that executed the xact */
        TransactionId locking_xid;      /* top-level XID of backend working on xact */
        bool            valid;                  /* TRUE if fully prepared */
        char            gid[GIDSIZE];   /* The GID assigned to the prepared xact */

        int                     prepareAppendOnlyIntentCount;
                                                                /*
                                                                 * The Append-Only Resync EOF intent count for
                                                                 * a non-crashed prepared transaction.
                                                                 */
} GlobalTransactionData;

/*
 * Two Phase Commit shared state.  Access to this struct is protected
 * by TwoPhaseStateLock.
 */
typedef struct TwoPhaseStateData
{
        /* Head of linked list of free GlobalTransactionData structs */
        SHMEM_OFFSET freeGXacts;

        /* Number of valid prepXacts entries. */
        int                     numPrepXacts;

        /*
         * There are max_prepared_xacts items in this array, but C wants a
         * fixed-size array.
         */
        GlobalTransaction prepXacts[1];         /* VARIABLE LENGTH ARRAY */
} TwoPhaseStateData;                    /* VARIABLE LENGTH STRUCT */

static TwoPhaseStateData *TwoPhaseState;



/*
 * The following list is
 */
static HTAB *crashRecoverPostCheckpointPreparedTransactions_map_ht = NULL;

static void add_recover_post_checkpoint_prepared_transactions_map_entry(TransactionId xid, XLogRecPtr *m, char *caller);

static void remove_recover_post_checkpoint_prepared_transactions_map_entry(TransactionId xid, char *caller);

static void RecordTransactionCommitPrepared(TransactionId xid,
                                                                const char *gid,
                                                                int nchildren,
                                                                TransactionId *children,
                                                                PersistentEndXactRecObjects *persistentPrepareObjects);
static void RecordTransactionAbortPrepared(TransactionId xid,
                                                           int nchildren,
                                                           TransactionId *children,
                                                           PersistentEndXactRecObjects *persistentPrepareObjects);
static void ProcessRecords(char *bufptr, TransactionId xid,
                           const TwoPhaseCallback callbacks[]);

/*
 * Generic initialisation of hash table.
 */
static HTAB *
init_hash(const char *name, Size keysize, Size entrysize, int initialSize)
{
  HASHCTL ctl;

  memset(&ctl, 0, sizeof(ctl));
  ctl.keysize = keysize;
  ctl.entrysize = entrysize;
  ctl.hash = tag_hash;
  return hash_create(name,
                     initialSize,
                     &ctl,
                     HASH_ELEM | HASH_FUNCTION);


}  /* end init_hash */


/*
 * Add a new mapping to the recover post checkpoint prepared transactions hash table.
 */
static void
add_recover_post_checkpoint_prepared_transactions_map_entry(TransactionId xid, XLogRecPtr *m, char *caller)
{
  prpt_map *entry = NULL;
  bool      found = false;

  if (Debug_persistent_print)
     elog(Persistent_DebugPrintLevel(),
          "add_recover_post_checkpoint_prepared_transactions_map_entry: start of function."
       );

  /*
   * The table is lazily initialised.
   */
  if (crashRecoverPostCheckpointPreparedTransactions_map_ht == NULL)
    {
      if (Debug_persistent_print)
        elog(Persistent_DebugPrintLevel(),
             "add_recover_post_checkpoint_prepared_transactions_map_entry: initial setup of global hash table. Caller = %s",
             caller);
    crashRecoverPostCheckpointPreparedTransactions_map_ht
                     = init_hash("two phase post checkpoint prepared transactions map",
                                 sizeof(TransactionId), /* keysize */
                                 sizeof(prpt_map),
                                 10 /* initialize for 10 entries */);
    }
  if (Debug_persistent_print)
    elog(Persistent_DebugPrintLevel(),
         "add_recover_post_checkpoint_prepared_transactions_map_entry: add entry xid = %u, XLogRecPtr = %s, caller = %s",
         xid,
         XLogLocationToString(m),
         caller);

  entry = hash_search(crashRecoverPostCheckpointPreparedTransactions_map_ht,
                      &xid,
                      HASH_ENTER,
                      &found);

  /*
   * KAS should probably put out an error if found == true (i.e. it already exists).
   */

  if (Debug_persistent_print)
    elog(Persistent_DebugPrintLevel(),
       "add_recover_post_checkpoint_prepared_transactions_map_entry: add entry xid = %u, address prpt_map = %p",
       xid,
       entry);

  /*
   * If this is a new entry, we need to add the data, if we found
   * an entry, we need to update it, so just copy our data
   * right over the top.
   */
  memcpy(&entry->xlogrecptr, m, sizeof(XLogRecPtr));

  if (Debug_persistent_print)
    elog(Persistent_DebugPrintLevel(),
           "Transaction id = %u, XLog Rec Ptr = %s, caller = %s",
           xid,
           XLogLocationToString(m),
           caller);

}  /* end add_recover_post_checkpoint_prepared_transactions_map_entry */

/*
 * Find a mapping in the recover post checkpoint prepared transactions hash table.
 */
bool
TwoPhaseFindRecoverPostCheckpointPreparedTransactionsMapEntry(TransactionId xid, XLogRecPtr *m, char *caller)
{
  prpt_map *entry = NULL;
  bool      found = false;

  if (Debug_persistent_print)
     elog(Persistent_DebugPrintLevel(),
          "find_recover_post_checkpoint_prepared_transactions_map_entry: start of function."
       );
  MemSet(m, 0, sizeof(XLogRecPtr));

  /*
   * The table is lazily initialised.
   */
  if (crashRecoverPostCheckpointPreparedTransactions_map_ht == NULL)
    {
      if (Debug_persistent_print)
        elog(Persistent_DebugPrintLevel(),
             "find_recover_post_checkpoint_prepared_transactions_map_entry: initial setup of global hash table. Caller = %s",
             caller);
    crashRecoverPostCheckpointPreparedTransactions_map_ht
                     = init_hash("two phase post checkpoint prepared transactions map",
                                 sizeof(TransactionId), /* keysize */
                                 sizeof(prpt_map),
                                 10 /* initialize for 10 entries */);
    }

  entry = hash_search(crashRecoverPostCheckpointPreparedTransactions_map_ht,
                      &xid,
                      HASH_FIND,
                      &found);
  if (entry == NULL)
  {
          if (Debug_persistent_print)
            elog(Persistent_DebugPrintLevel(),
                 "find_recover_post_checkpoint_prepared_transactions_map_entry: did not find entry xid = %u, caller = %s",
                 xid,
                 caller);
          return false;
  }

  memcpy(m, &entry->xlogrecptr, sizeof(XLogRecPtr));
  if (Debug_persistent_print)
    elog(Persistent_DebugPrintLevel(),
         "find_recover_post_checkpoint_prepared_transactions_map_entry: found entry xid = %u, XLogRecPtr = %s, caller = %s",
         xid,
         XLogLocationToString(m),
         caller);

  return true;
}  /* end add_recover_post_checkpoint_prepared_transactions_map_entry */


/*
 * Remove a mapping from the recover post checkpoint prepared transactions hash table.
 */
static void
remove_recover_post_checkpoint_prepared_transactions_map_entry(TransactionId xid, char *caller)
{
  prpt_map *entry = NULL;
  bool      found = false;;

  if (Debug_persistent_print)
    elog(Persistent_DebugPrintLevel(),
       "remove_recover_post_checkpoint_prepared_transactions_map_entry: entering..."
      );

  if (Debug_persistent_print)
    elog(Persistent_DebugPrintLevel(),
         "remove_recover_post_checkpoint_prepared_transactions_map_entry: TransactionId = %u",
         xid);

  if (crashRecoverPostCheckpointPreparedTransactions_map_ht != NULL)
    {
     entry = hash_search(crashRecoverPostCheckpointPreparedTransactions_map_ht,
                      &xid,
                      HASH_REMOVE,
                      &found);
    }

  /* KAS should probably put out an error if it is not found.  */
  if (found == true)
    {
    if (Debug_persistent_print)
      elog(Persistent_DebugPrintLevel(),
     "remove_recover_post_checkpoint_prepared_transaction_map_entry found = TRUE");
    }
  else
    {
    if (Debug_persistent_print)
        elog(Persistent_DebugPrintLevel(),
         "remove_recover_post_checkpoint_prepared_transaction_map_entry found = FALSE");
    }

}  /* end remove_recover_post_checkpoint_prepared_transactions_map_entry */


/*
 * Initialization of shared memory
 */
Size
TwoPhaseShmemSize(void)
{
        Size            size;

        /* Need the fixed struct, the array of pointers, and the GTD structs */
        size = offsetof(TwoPhaseStateData, prepXacts);
        size = add_size(size, mul_size(max_prepared_xacts,
                                                                   sizeof(GlobalTransaction)));
        size = MAXALIGN(size);
        size = add_size(size, mul_size(max_prepared_xacts,
                                                                   sizeof(GlobalTransactionData)));

        return size;
}

void
TwoPhaseShmemInit(void)
{
        bool            found;

        TwoPhaseState = ShmemInitStruct("Prepared Transaction Table",
                                                                        TwoPhaseShmemSize(),
                                                                        &found);
        if (!IsUnderPostmaster)
        {
                GlobalTransaction gxacts;
                int                     i;

                Assert(!found);
                TwoPhaseState->freeGXacts = INVALID_OFFSET;
                TwoPhaseState->numPrepXacts = 0;

                /*
                 * Initialize the linked list of free GlobalTransactionData structs
                 */
                gxacts = (GlobalTransaction)
                        ((char *) TwoPhaseState +
                         MAXALIGN(offsetof(TwoPhaseStateData, prepXacts) +
                                          sizeof(GlobalTransaction) * max_prepared_xacts));
                for (i = 0; i < max_prepared_xacts; i++)
                {
                        gxacts[i].proc.links.next = TwoPhaseState->freeGXacts;
                        TwoPhaseState->freeGXacts = MAKE_OFFSET(&gxacts[i]);
                }
        }
        else
        {
                Assert(found);
        }
}


/*
 * MarkAsPreparing
 *              Reserve the GID for the given transaction.
 *
 * Internally, this creates a gxact struct and puts it into the active array.
 * NOTE: this is also used when reloading a gxact after a crash; so avoid
 * assuming that we can use very much backend context.
 */
GlobalTransaction
MarkAsPreparing(TransactionId xid,
                                LocalDistribXactRef *localDistribXactRef,
                                const char *gid,
                                TimestampTz prepared_at, Oid owner, Oid databaseid
                , XLogRecPtr *xlogrecptr)
{
        GlobalTransaction gxact;
        int                     i;
        int                     idlen = strlen(gid);

        if (idlen >= GIDSIZE)
                ereport(ERROR,
                                (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                                 errmsg("transaction identifier \"%s\" is too long (%d > %d max)",
                                                gid, idlen, GIDSIZE)));

        LWLockAcquire(TwoPhaseStateLock, LW_EXCLUSIVE);

        /*
         * First, find and recycle any gxacts that failed during prepare. We do
         * this partly to ensure we don't mistakenly say their GIDs are still
         * reserved, and partly so we don't fail on out-of-slots unnecessarily.
         */
        for (i = 0; i < TwoPhaseState->numPrepXacts; i++)
        {
                gxact = TwoPhaseState->prepXacts[i];
                if (!gxact->valid && !TransactionIdIsActive(gxact->locking_xid))
                {
                        /* It's dead Jim ... remove from the active array */
                        if (Debug_persistent_print)
                           elog(Persistent_DebugPrintLevel(),
                                "MarkAsPreparing: TwoPhaseState->numPrepXacts = %d, subtracting 1", TwoPhaseState->numPrepXacts);
                        TwoPhaseState->numPrepXacts--;
                        TwoPhaseState->prepXacts[i] = TwoPhaseState->prepXacts[TwoPhaseState->numPrepXacts];
                        /* and put it back in the freelist */
                        gxact->proc.links.next = TwoPhaseState->freeGXacts;
                        TwoPhaseState->freeGXacts = MAKE_OFFSET(gxact);
                        /* Back up index count too, so we don't miss scanning one */
                        i--;
                }
        }

        /* Check for conflicting GID */
        for (i = 0; i < TwoPhaseState->numPrepXacts; i++)
        {
                gxact = TwoPhaseState->prepXacts[i];
                if (strcmp(gxact->gid, gid) == 0)
                {
                        ereport(ERROR,
                                        (errcode(ERRCODE_DUPLICATE_OBJECT),
                                         errmsg("transaction identifier \"%s\" is already in use",
                                                        gid)));
                }
        }

        /* Get a free gxact from the freelist */
        if (TwoPhaseState->freeGXacts == INVALID_OFFSET)
                ereport(ERROR,
                                (errcode(ERRCODE_OUT_OF_MEMORY),
                                 errmsg("maximum number of prepared transactions reached"),
                                 errhint("Increase max_prepared_transactions (currently %d).",
                                                 max_prepared_xacts)));
        gxact = (GlobalTransaction) MAKE_PTR(TwoPhaseState->freeGXacts);
        TwoPhaseState->freeGXacts = gxact->proc.links.next;

        /* Initialize it */
        MemSet(&gxact->proc, 0, sizeof(PGPROC));
        SHMQueueElemInit(&(gxact->proc.links));
        gxact->proc.waitStatus = STATUS_OK;
        gxact->proc.xid = xid;
        gxact->proc.xmin = InvalidTransactionId;
        gxact->proc.pid = 0;
        gxact->proc.databaseId = databaseid;
        gxact->proc.roleId = owner;
        gxact->proc.inVacuum = false;
        gxact->proc.lwWaiting = false;
        gxact->proc.lwExclusive = false;
        gxact->proc.lwWaitLink = NULL;
        gxact->proc.waitLock = NULL;
        gxact->proc.waitProcLock = NULL;

        for (i = 0; i < NUM_LOCK_PARTITIONS; i++)
                SHMQueueInit(&(gxact->proc.myProcLocks[i]));
        /* subxid data must be filled later by GXactLoadSubxactData */
        gxact->proc.subxids.overflowed = false;
        gxact->proc.subxids.nxids = 0;

        gxact->prepared_at = prepared_at;
        /* initialize LSN to 0 (start of WAL) */
        gxact->prepare_lsn.xlogid = 0;
        gxact->prepare_lsn.xrecoff = 0;
        if (xlogrecptr == NULL)
          {
          gxact->prepare_begin_lsn.xlogid = 0;
          gxact->prepare_begin_lsn.xrecoff = 0;
          }
        else
          {
            gxact->prepare_begin_lsn.xlogid = xlogrecptr->xlogid;
            gxact->prepare_begin_lsn.xrecoff = xlogrecptr->xrecoff;
            /* Assert(xlogrecptr->xrecoff > 0 || xlogrecptr->xlogid > 0); */
          }
        gxact->owner = owner;
        gxact->locking_xid = xid;

        gxact->valid = false;
        strcpy(gxact->gid, gid);
        gxact->prepareAppendOnlyIntentCount = 0;

        /* And insert it into the active array */
        Assert(TwoPhaseState->numPrepXacts < max_prepared_xacts);
        TwoPhaseState->prepXacts[TwoPhaseState->numPrepXacts++] = gxact;

        LWLockRelease(TwoPhaseStateLock);

        return gxact;
}

/*
 * GXactLoadSubxactData
 *
 * If the transaction being persisted had any subtransactions, this must
 * be called before MarkAsPrepared() to load information into the dummy
 * PGPROC.
 */
static void
GXactLoadSubxactData(GlobalTransaction gxact, int nsubxacts,
                                         TransactionId *children)
{
        /* We need no extra lock since the GXACT isn't valid yet */
        if (nsubxacts > PGPROC_MAX_CACHED_SUBXIDS)
        {
                gxact->proc.subxids.overflowed = true;
                nsubxacts = PGPROC_MAX_CACHED_SUBXIDS;
        }
        if (nsubxacts > 0)
        {
                memcpy(gxact->proc.subxids.xids, children,
                           nsubxacts * sizeof(TransactionId));
                gxact->proc.subxids.nxids = nsubxacts;
        }
}

/*
 * MarkAsPrepared
 *              Mark the GXACT as fully valid, and enter it into the global ProcArray.
 */
static void
MarkAsPrepared(GlobalTransaction gxact)
{
        /* Lock here may be overkill, but I'm not convinced of that ... */
        LWLockAcquire(TwoPhaseStateLock, LW_EXCLUSIVE);
        Assert(!gxact->valid);
        if (Debug_persistent_print)
          elog(Persistent_DebugPrintLevel(),
               "MarkAsPrepared: gxact->proc.xid = %d  and set valid = true", gxact->proc.xid);
        gxact->valid = true;
        LWLockRelease(TwoPhaseStateLock);

        elog((Debug_print_full_dtm ? LOG : DEBUG5),"MarkAsPrepared marking GXACT gid = %s as valid (prepared)",
                gxact->gid);

        /*
         * Put it into the global ProcArray so TransactionIdInProgress considers
         * the XID as still running.
         */
        ProcArrayAdd(&gxact->proc);
}

/*
 * LockGXact
 *              Locate the prepared transaction and mark it busy for COMMIT or PREPARE.
 */
static GlobalTransaction
LockGXact(const char *gid, Oid user, bool raiseErrorIfNotFound)
{
        int                     i;

        elog((Debug_print_full_dtm ? LOG : DEBUG5),"LockGXact called to lock identifier = %s.",gid);

        LWLockAcquire(TwoPhaseStateLock, LW_EXCLUSIVE);

        for (i = 0; i < TwoPhaseState->numPrepXacts; i++)
        {
                GlobalTransaction gxact = TwoPhaseState->prepXacts[i];

                elog((Debug_print_full_dtm ? LOG : DEBUG5), "LockGXact checking identifier = %s.",gxact->gid);

                /* Ignore not-yet-valid GIDs */
                if (!gxact->valid)
                        continue;
                if (strcmp(gxact->gid, gid) != 0)
                        continue;

                /* Found it, but has someone else got it locked? */
                if (TransactionIdIsValid(gxact->locking_xid))
                {
                        if (TransactionIdIsActive(gxact->locking_xid))
                        {
                                LWLockRelease(TwoPhaseStateLock);
                                ereport(ERROR,
                                                (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
                                errmsg("prepared transaction with identifier \"%s\" is busy",
                                           gid)));
                        }

                        gxact->locking_xid = InvalidTransactionId;
                }

                if (user != gxact->owner && !superuser_arg(user))
                {
                        LWLockRelease(TwoPhaseStateLock);
                        ereport(ERROR,
                                        (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
                                  errmsg("permission denied to finish prepared transaction"),
                                         errhint("Must be superuser or the user that prepared the transaction.")));
                }

                /*
                 * Note: it probably would be possible to allow committing from another
                 * database; but at the moment NOTIFY is known not to work and there
                 * may be some other issues as well.  Hence disallow until someone
                 * gets motivated to make it work.
                 */
                if (MyDatabaseId != gxact->proc.databaseId &&  (Gp_role != GP_ROLE_EXECUTE))
                {
                        LWLockRelease(TwoPhaseStateLock);
                        ereport(ERROR,
                                        (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                                         errmsg("prepared transaction belongs to another database"),
                                         errhint("Connect to the database where the transaction was prepared to finish it.")));
                }


                /* OK for me to lock it */
                gxact->locking_xid = GetTopTransactionId();

                LWLockRelease(TwoPhaseStateLock);

                /* we *must* have it locked with a valid xid here! */
                Assert(TransactionIdIsValid(gxact->locking_xid));

                return gxact;
        }
        LWLockRelease(TwoPhaseStateLock);

        if (raiseErrorIfNotFound)
        {
                ereport(ERROR,
                                (errcode(ERRCODE_UNDEFINED_OBJECT),
                         errmsg("prepared transaction with identifier \"%s\" does not exist",
                                        gid)));
        }

        return NULL;
}

/*
 * FindCurrentPrepareGXact
 *              Locate the current prepare transaction.
 */
static GlobalTransaction
FindPrepareGXact(const char *gid)
{
        int                     i;

        elog((Debug_print_full_dtm ? LOG : DEBUG5),"FindCurrentPrepareGXact called to lock identifier = %s.",gid);

        LWLockAcquire(TwoPhaseStateLock, LW_EXCLUSIVE);

        for (i = 0; i < TwoPhaseState->numPrepXacts; i++)
        {
                GlobalTransaction gxact = TwoPhaseState->prepXacts[i];

                elog((Debug_print_full_dtm ? LOG : DEBUG5), "FindCurrentPrepareGXact checking identifier = %s.",gxact->gid);

                if (strcmp(gxact->gid, gid) != 0)
                        continue;

                LWLockRelease(TwoPhaseStateLock);

                return gxact;
        }
        LWLockRelease(TwoPhaseStateLock);

        ereport(ERROR,
                        (errcode(ERRCODE_UNDEFINED_OBJECT),
                 errmsg("prepared transaction with identifier \"%s\" does not exist",
                                gid)));

        return NULL;
}

/*
 * RemoveGXact
 *              Remove the prepared transaction from the shared memory array.
 *
 * NB: caller should have already removed it from ProcArray
 */
static void
RemoveGXact(GlobalTransaction gxact)
{
        int                     i;

        if (Debug_persistent_print)
          elog(Persistent_DebugPrintLevel(),
               "RemoveGXact: entering...");

        LWLockAcquire(TwoPhaseStateLock, LW_EXCLUSIVE);

        for (i = 0; i < TwoPhaseState->numPrepXacts; i++)
        {
                if (gxact == TwoPhaseState->prepXacts[i])
                {
                        if (Debug_persistent_print)
                           elog(Persistent_DebugPrintLevel(),
                                "RemoveGXact: about to remove xid = %d", gxact->proc.xid);
                        /* remove from the active array */
                        if (Debug_persistent_print)
                           elog(Persistent_DebugPrintLevel(),
                                "RemoveGXact: TwoPhaseState->numPrepXacts = %d, subtracting 1", TwoPhaseState->numPrepXacts);
                        TwoPhaseState->numPrepXacts--;
                        TwoPhaseState->prepXacts[i] = TwoPhaseState->prepXacts[TwoPhaseState->numPrepXacts];

                        /* and put it back in the freelist */
                        gxact->proc.links.next = TwoPhaseState->freeGXacts;
                        TwoPhaseState->freeGXacts = MAKE_OFFSET(gxact);

                        LWLockRelease(TwoPhaseStateLock);

                        return;
                }
        }

        LWLockRelease(TwoPhaseStateLock);

        elog(ERROR, "failed to find %p in GlobalTransaction array", (void *)gxact);
}


/*
 * Returns an array of all prepared transactions for the user-level
 * function pg_prepared_xact.
 *
 * The returned array and all its elements are copies of internal data
 * structures, to minimize the time we need to hold the TwoPhaseStateLock.
 *
 * WARNING -- we return even those transactions that are not fully prepared
 * yet.  The caller should filter them out if he doesn't want them.
 *
 * The returned array is palloc'd.
 */
static int
GetPreparedTransactionList(GlobalTransaction *gxacts)
{
        GlobalTransaction array;
        int                     num;
        int                     i;

        LWLockAcquire(TwoPhaseStateLock, LW_SHARED);

        if (TwoPhaseState->numPrepXacts == 0)
        {
                LWLockRelease(TwoPhaseStateLock);

                *gxacts = NULL;
                return 0;
        }

        num = TwoPhaseState->numPrepXacts;
        array = (GlobalTransaction) palloc(sizeof(GlobalTransactionData) * num);
        *gxacts = array;
        for (i = 0; i < num; i++)
                memcpy(array + i, TwoPhaseState->prepXacts[i],
                           sizeof(GlobalTransactionData));

        LWLockRelease(TwoPhaseStateLock);

        return num;
}


/* Working status for pg_prepared_xact */
typedef struct
{
        GlobalTransaction array;
        int                     ngxacts;
        int                     currIdx;
} Working_State;

/*
 * pg_prepared_xact
 *              Produce a view with one row per prepared transaction.
 *
 * This function is here so we don't have to export the
 * GlobalTransactionData struct definition.
 */
Datum
pg_prepared_xact(PG_FUNCTION_ARGS)
{
        FuncCallContext *funcctx;
        Working_State *status;

        if (SRF_IS_FIRSTCALL())
        {
                TupleDesc       tupdesc;
                MemoryContext oldcontext;

                /* create a function context for cross-call persistence */
                funcctx = SRF_FIRSTCALL_INIT();

                /*
                 * Switch to memory context appropriate for multiple function calls
                 */
                oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

                /* build tupdesc for result tuples */
                /* this had better match pg_prepared_xacts view in system_views.sql */
                tupdesc = CreateTemplateTupleDesc(5, false);
                TupleDescInitEntry(tupdesc, (AttrNumber) 1, "transaction",
                                                   XIDOID, -1, 0);
                TupleDescInitEntry(tupdesc, (AttrNumber) 2, "gid",
                                                   TEXTOID, -1, 0);
                TupleDescInitEntry(tupdesc, (AttrNumber) 3, "prepared",
                                                   TIMESTAMPTZOID, -1, 0);
                TupleDescInitEntry(tupdesc, (AttrNumber) 4, "ownerid",
                                                   OIDOID, -1, 0);
                TupleDescInitEntry(tupdesc, (AttrNumber) 5, "dbid",
                                                   OIDOID, -1, 0);

                funcctx->tuple_desc = BlessTupleDesc(tupdesc);

                /*
                 * Collect all the 2PC status information that we will format and send
                 * out as a result set.
                 */
                status = (Working_State *) palloc(sizeof(Working_State));
                funcctx->user_fctx = (void *) status;

                status->ngxacts = GetPreparedTransactionList(&status->array);
                status->currIdx = 0;

                MemoryContextSwitchTo(oldcontext);
        }

        funcctx = SRF_PERCALL_SETUP();
        status = (Working_State *) funcctx->user_fctx;

        while (status->array != NULL && status->currIdx < status->ngxacts)
        {
                GlobalTransaction gxact = &status->array[status->currIdx++];
                Datum           values[5];
                bool            nulls[5];
                HeapTuple       tuple;
                Datum           result;

                if (!gxact->valid)
                        continue;

                /*
                 * Form tuple with appropriate data.
                 */
                MemSet(values, 0, sizeof(values));
                MemSet(nulls, 0, sizeof(nulls));

                values[0] = TransactionIdGetDatum(gxact->proc.xid);
                values[1] = DirectFunctionCall1(textin, CStringGetDatum(gxact->gid));
                values[2] = TimestampTzGetDatum(gxact->prepared_at);
                values[3] = ObjectIdGetDatum(gxact->owner);
                values[4] = ObjectIdGetDatum(gxact->proc.databaseId);

                tuple = heap_form_tuple(funcctx->tuple_desc, values, nulls);
                result = HeapTupleGetDatum(tuple);
                SRF_RETURN_NEXT(funcctx, result);
        }

        SRF_RETURN_DONE(funcctx);
}

/*
 * TwoPhaseGetDummyProc
 *              Get the PGPROC that represents a prepared transaction specified by XID
 */
PGPROC *
TwoPhaseGetDummyProc(TransactionId xid)
{
        PGPROC     *result = NULL;
        int                     i;

        static TransactionId cached_xid = InvalidTransactionId;
        static PGPROC *cached_proc = NULL;

        /*
         * During a recovery, COMMIT PREPARED, or ABORT PREPARED, we'll be called
         * repeatedly for the same XID.  We can save work with a simple cache.
         */
        if (xid == cached_xid)
                return cached_proc;

        LWLockAcquire(TwoPhaseStateLock, LW_SHARED);

        for (i = 0; i < TwoPhaseState->numPrepXacts; i++)
        {
                GlobalTransaction gxact = TwoPhaseState->prepXacts[i];

                if (gxact->proc.xid == xid)
                {
                        result = &gxact->proc;
                        break;
                }
        }

        LWLockRelease(TwoPhaseStateLock);

        if (result == NULL)                     /* should not happen */
                elog(ERROR, "failed to find dummy PGPROC for xid %u (%d entries)", xid, TwoPhaseState->numPrepXacts);

        cached_xid = xid;
        cached_proc = result;

        return result;
}

/************************************************************************/
/* State file support                                                                                                   */
/************************************************************************/

#define TwoPhaseFilePath(path, xid) \
        snprintf(path, MAXPGPATH, TWOPHASE_DIR "/%08X", xid)
#define TwoPhaseSimpleFileName(path, xid) \
                snprintf(path, MAXPGPATH, "/%08X", xid)

/*
 * 2PC state file format:
 *
 *      1. TwoPhaseFileHeader
 *      2. TransactionId[] (subtransactions)
 *      3. RelFileNode[] (files to be deleted at commit)
 *      4. RelFileNode[] (files to be deleted at abort)
 *      5. TwoPhaseRecordOnDisk
 *      6. ...
 *      7. TwoPhaseRecordOnDisk (end sentinel, rmid == TWOPHASE_RM_END_ID)
 *      8. CRC32
 *
 * Each segment except the final CRC32 is MAXALIGN'd.
 */

/*
 * Header for a 2PC state file
 */
#define TWOPHASE_MAGIC  0x57F94531              /* format identifier */

typedef struct TwoPhaseFileHeader
{
        uint32          magic;                  /* format identifier */
        uint32          total_len;              /* actual file length */
        TransactionId xid;                      /* original transaction XID */
        Oid                     database;               /* OID of database it was in */
        TimestampTz prepared_at;        /* time of preparation */
        Oid                     owner;                  /* user running the transaction */
        int32           nsubxacts;              /* number of following subxact XIDs */
        int16           persistentPrepareObjectCount;
                                                                /* number of PersistentEndXactRec style objects */
        char            gid[GIDSIZE];   /* GID for transaction */
} TwoPhaseFileHeader;

/*
 * Header for each record in a state file
 *
 * NOTE: len counts only the rmgr data, not the TwoPhaseRecordOnDisk header.
 * The rmgr data will be stored starting on a MAXALIGN boundary.
 */
typedef struct TwoPhaseRecordOnDisk
{
        uint32          len;                    /* length of rmgr data */
        TwoPhaseRmgrId rmid;            /* resource manager for this record */
        uint16          info;                   /* flag bits for use by rmgr */
} TwoPhaseRecordOnDisk;

/*
 * During prepare, the state file is assembled in memory before writing it
 * to WAL and the actual state file.  We use a chain of XLogRecData blocks
 * so that we will be able to pass the state file contents directly to
 * XLogInsert.
 */
static struct xllist
{
        XLogRecData *head;                      /* first data block in the chain */
        XLogRecData *tail;                      /* last block in chain */
        uint32          bytes_free;             /* free bytes left in tail block */
        uint32          total_len;              /* total data bytes in chain */
}       records;


/*
 * Append a block of data to records data structure.
 *
 * NB: each block is padded to a MAXALIGN multiple.  This must be
 * accounted for when the file is later read!
 *
 * The data is copied, so the caller is free to modify it afterwards.
 */
static void
save_state_data(const void *data, uint32 len)
{
        uint32          padlen = MAXALIGN(len);

        if (padlen > records.bytes_free)
        {
                records.tail->next = palloc0(sizeof(XLogRecData));
                records.tail = records.tail->next;
                records.tail->buffer = InvalidBuffer;
                records.tail->len = 0;
                records.tail->next = NULL;

                records.bytes_free = Max(padlen, 512);
                records.tail->data = palloc(records.bytes_free);
        }

        memcpy(((char *) records.tail->data) + records.tail->len, data, len);
        records.tail->len += padlen;
        records.bytes_free -= padlen;
        records.total_len += padlen;
}

/*
 * Start preparing a state file.
 *
 * Initializes data structure and inserts the 2PC file header record.
 */
void
StartPrepare(GlobalTransaction gxact)
{
        TransactionId xid = gxact->proc.xid;
        TwoPhaseFileHeader hdr;
        TransactionId *children;

        int32                                           persistentPrepareSerializeLen;
        PersistentEndXactRecObjects persistentPrepareObjects;

        /* Initialize linked list */
        records.head = palloc0(sizeof(XLogRecData));
        records.head->buffer = InvalidBuffer;
        records.head->len = 0;
        records.head->next = NULL;

        records.bytes_free = Max(sizeof(TwoPhaseFileHeader), 512);
        records.head->data = palloc(records.bytes_free);

        records.tail = records.head;

        records.total_len = 0;

        /* Create header */
        hdr.magic = TWOPHASE_MAGIC;
        hdr.total_len = 0;                      /* EndPrepare will fill this in */
        hdr.xid = xid;
        hdr.database = gxact->proc.databaseId;
        hdr.prepared_at = gxact->prepared_at;
        hdr.owner = gxact->owner;
        hdr.nsubxacts = xactGetCommittedChildren(&children);
        persistentPrepareSerializeLen =
                        PersistentEndXactRec_FetchObjectsFromSmgr(
                                                                                &persistentPrepareObjects,
                                                                                EndXactRecKind_Prepare,
                                                                                &hdr.persistentPrepareObjectCount);
        StrNCpy(hdr.gid, gxact->gid, GIDSIZE);

        save_state_data(&hdr, sizeof(TwoPhaseFileHeader));

        /* Add the additional info about subxacts and deletable files */
        if (hdr.nsubxacts > 0)
        {
                save_state_data(children, hdr.nsubxacts * sizeof(TransactionId));
                /* While we have the child-xact data, stuff it in the gxact too */
                GXactLoadSubxactData(gxact, hdr.nsubxacts, children);
                pfree(children);
        }
        if (hdr.persistentPrepareObjectCount > 0)
        {
                char *persistentPrepareBuffer;
                int16 objectCount;

                Assert(persistentPrepareSerializeLen > 0);
                persistentPrepareBuffer =
                                        (char*)palloc(persistentPrepareSerializeLen);

                PersistentEndXactRec_Serialize(
                                                                &persistentPrepareObjects,
                                                                EndXactRecKind_Prepare,
                                                                &objectCount,
                                                                (uint8*)persistentPrepareBuffer,
                                                                persistentPrepareSerializeLen);

                if (Debug_persistent_print)
                {
                        elog(Persistent_DebugPrintLevel(),
                                 "StartPrepare: persistentPrepareSerializeLen %d",
                                 persistentPrepareSerializeLen);
                        PersistentEndXactRec_Print("StartPrepare", &persistentPrepareObjects);
                }

                save_state_data(persistentPrepareBuffer, persistentPrepareSerializeLen);

                pfree(persistentPrepareBuffer);
        }

#ifdef FAULT_INJECTOR
        FaultInjector_InjectFaultIfSet(
                                                                   StartPrepareTx,
                                                                   DDLNotSpecified,
                                                                   "",  // databaseName
                                                                   ""); // tableName
#endif

}


/*
 * Finish preparing state file.
 *
 * Writes state file (the prepare record) to WAL.
 */
void EndPrepare(GlobalTransaction gxact)
{
        CHECKPOINT_START_LOCK_DECLARE;

        TransactionId       xid = gxact->proc.xid;

        if (Debug_persistent_print)
                elog(Persistent_DebugPrintLevel(), "EndPrepare: xid = %d", xid);

        /* Add the end sentinel to the list of 2PC records */
        RegisterTwoPhaseRecord(TWOPHASE_RM_END_ID, 0, NULL, 0);

        /*
         * We have to lock out checkpoint start here, too; otherwise a checkpoint
         * starting immediately after the WAL record is inserted could complete
         * without fsync'ing our state file.  (This is essentially the same kind
         * of race condition as the COMMIT-to-clog-write case that
         * RecordTransactionCommit uses CheckpointStartLock for; see notes there.)
         *
         * We save the PREPARE record's location in the gxact for later use by
         * CheckPointTwoPhase.
         *
         * NOTE: Critical seciton and CheckpointStartLock were moved up.
         */
        CHECKPOINT_START_LOCK;

        START_CRIT_SECTION();

        gxact->prepare_lsn       = XLogInsert(RM_XACT_ID, XLOG_XACT_PREPARE, records.head);
        gxact->prepare_begin_lsn = XLogLastInsertBeginLoc();

        /* Add the prepared record to our global list */
        add_recover_post_checkpoint_prepared_transactions_map_entry(xid, &gxact->prepare_begin_lsn, "EndPrepare");

        XLogFlush(gxact->prepare_lsn);

        /* If we crash now, we have prepared: WAL replay will fix things */

        if (Debug_persistent_print)
                elog(Persistent_DebugPrintLevel(),
                                "EndPrepare: proc.xid %u, prepare_lsn %s, gid %s", xid,
                                XLogLocationToString(&gxact->prepare_lsn), gxact->gid);

        if (Debug_abort_after_segment_prepared)
        {
                elog(PANIC,
                     "Raise an error as directed by Debug_abort_after_segment_prepared");
        }

        /*
         * Mark the prepared transaction as valid.      As soon as xact.c marks MyProc
         * as not running our XID (which it will do immediately after this
         * function returns), others can commit/rollback the xact.
         *
         * NB: a side effect of this is to make a dummy ProcArray entry for the
         * prepared XID.  This must happen before we clear the XID from MyProc,
         * else there is a window where the XID is not running according to
         * TransactionIdInProgress, and onlookers would be entitled to assume the
         * xact crashed.  Instead we have a window where the same XID appears
         * twice in ProcArray, which is OK.
         */
        MarkAsPrepared(gxact);

        END_CRIT_SECTION();

        /*
         * Now we can release the checkpoint start lock: a checkpoint starting
         * after this will certainly see the gxact as a candidate for fsyncing.
         */
        CHECKPOINT_START_UNLOCK;

#ifdef FAULT_INJECTOR
        FaultInjector_InjectFaultIfSet(
                        EndPreparedTwoPhaseSleep,
                        DDLNotSpecified,
                        "", // databaseName
                        ""); // tableName
#endif

        records.tail = records.head = NULL;
} /* end EndPrepare */


/*
 * Register a 2PC record to be written to state file.
 */
void
RegisterTwoPhaseRecord(TwoPhaseRmgrId rmid, uint16 info,
                                           const void *data, uint32 len)
{
        TwoPhaseRecordOnDisk record;

        record.rmid = rmid;
        record.info = info;
        record.len = len;
        save_state_data(&record, sizeof(TwoPhaseRecordOnDisk));
        if (len > 0)
                save_state_data(data, len);
}


void
PrepareIntentAppendOnlyCommitWork(char *gid)
{
        GlobalTransaction gxact;

        gxact = FindPrepareGXact(gid);

        Assert(gxact->prepareAppendOnlyIntentCount >= 0);
        gxact->prepareAppendOnlyIntentCount++;
}

void
PrepareDecrAppendOnlyCommitWork(char *gid)
{
        GlobalTransaction gxact;

        gxact = FindPrepareGXact(gid);

        Assert(gxact->prepareAppendOnlyIntentCount >= 1);
        gxact->prepareAppendOnlyIntentCount--;
}


/*
 * FinishPreparedTransaction: execute COMMIT PREPARED or ROLLBACK PREPARED
 */
bool
FinishPreparedTransaction(const char *gid, bool isCommit, bool raiseErrorIfNotFound)
{
	CHECKPOINT_START_LOCK_DECLARE;

        GlobalTransaction gxact;
        TransactionId xid;
        char       *buf;
        char       *bufptr = NULL;
        char       *dummy;
        TwoPhaseFileHeader *hdr;
        TransactionId *children;

        PersistentEndXactRecObjects persistentPrepareObjects;
        int32 deserializeLen;

        int prepareAppendOnlyIntentCount;

        /*
         * Validate the GID, and lock the GXACT to ensure that two backends do not
         * try to commit the same GID at once.
         */
        gxact = LockGXact(gid, GetUserId(), raiseErrorIfNotFound);
        if (!raiseErrorIfNotFound && gxact == NULL)
        {
                return false;
        }

        xid = gxact->proc.xid;

        elog((Debug_print_full_dtm ? LOG : DEBUG5),
                 "FinishPreparedTransaction(): got xid %d for gid '%s'", xid, gid);

    /*
     * Check for recovery control file, and if so set up state for offline
     * recovery
     */
    XLogReadRecoveryCommandFile(DEBUG5);

    /* Now we can determine the list of expected TLIs */
    expectedTLIs = XLogReadTimeLineHistory(ThisTimeLineID);

    XLogRecPtr   tfXLogRecPtr;
    XLogRecord  *tfRecord  = NULL;

    /* get the two phase information from the xlog */

    LWLockAcquire(TwoPhaseStateLock, LW_SHARED);

    int tfsIndex = 0;
           for (tfsIndex = 0; tfsIndex < TwoPhaseState->numPrepXacts; tfsIndex++)
             {
               GlobalTransaction gxact = TwoPhaseState->prepXacts[tfsIndex];

               if (gxact->valid && gxact->proc.xid == xid)
                 {
                  tfXLogRecPtr = gxact->prepare_begin_lsn;
                  elog(DEBUG1, "FinishPreparedTransaction: found TwoPhaseState entry for %d", xid);
                  break;
                 }
             }
           if (tfsIndex == TwoPhaseState->numPrepXacts)
             elog( ERROR, "FinishPreparedTransaction: Did not find xid = %d in TwoPhaseState"
                 , xid
                 );

           LWLockRelease(TwoPhaseStateLock);

       XLogCloseReadRecord();
       tfRecord = XLogReadRecord(&tfXLogRecPtr, LOG);
           if (tfRecord == NULL)
           {
                   /*
                        * Invalid XLOG record means record is corrupted.
                        * Failover is required, hopefully mirror is in healthy state.
                        */
                   ereport(WARNING,
                                   (errmsg("primary failure, "
                                                   "xlog record is invalid, "
                                                   "failover requested"),
                                        errhint("run gprecoverseg to re-establish mirror connectivity")));

                   ereport(ERROR,
                                   (errcode(ERRCODE_DATA_CORRUPTED),
                                        errmsg("xlog record is invalid"),
                                        errSendAlert(true)));
           }

       buf = XLogRecGetData(tfRecord);

        if (buf == NULL)
                ereport(ERROR,
                                (errcode(ERRCODE_DATA_CORRUPTED),
                                 errmsg("two-phase state information for transaction %u is corrupt",
                                                xid),
                                 errSendAlert(true)));

        /*
         * Disassemble the header area
         */
        hdr = (TwoPhaseFileHeader *) buf;
        Assert(TransactionIdEquals(hdr->xid, xid));
        bufptr = buf + MAXALIGN(sizeof(TwoPhaseFileHeader));
        children = (TransactionId *) bufptr;
        bufptr += MAXALIGN(hdr->nsubxacts * sizeof(TransactionId));

        /*
         * Although we return the end of the PersistentEndXactRec object, we really want the
         * rounded-up aligned next buffer.  So, that is why we compute the deserialized length
         * and calculated the next buffer with it.
         */
        deserializeLen =
                        PersistentEndXactRec_DeserializeLen(
                                                                                (uint8*)bufptr,
                                                                                hdr->persistentPrepareObjectCount);

        PersistentEndXactRec_Deserialize(
                                                                (uint8*)bufptr,
                                                                hdr->persistentPrepareObjectCount,
                                                                &persistentPrepareObjects,
                                                                (uint8**)&dummy);

        if (Debug_persistent_print)
        {
                elog(Persistent_DebugPrintLevel(),
                         "FinishPreparedTransaction: deserializedLen %d, persistentPrepareObjectCount %d",
                         deserializeLen,
                         hdr->persistentPrepareObjectCount);
                PersistentEndXactRec_Print("FinishPreparedTransaction", &persistentPrepareObjects);
        }

        bufptr += MAXALIGN(deserializeLen);

        // NOTE: This use to be inside RecordTransactionCommitPrepared  and
        // NOTE: RecordTransactionAbortPrepared.  Moved out here so the mirrored
        // NOTE: can cover both the XLOG record and the mirrored pg_twophase file
        // NOTE: work.
        START_CRIT_SECTION();

	/*
	 * We have to lock out checkpoint start here when updating persistent relation information
	 * like Appendonly segment's committed EOF. Otherwise there might be a window betwwen
	 * the time some data is added to an appendonly segment file and its EOF updated in the
	 * persistent relation tables. If there is a checkpoint before updating the peristent tables 
	 * and the system crash after the checkpoint, then during crash recovery we would not resync 
	 * to the right EOFs (MPP-18261).
	 * When we use CheckpointStartLock, we make sure we already have the MirroredLock
	 * first.
	 */
	CHECKPOINT_START_LOCK;

        /*
         * The order of operations here is critical: make the XLOG entry for
         * commit or abort, then mark the transaction committed or aborted in
         * pg_clog, then remove its PGPROC from the global ProcArray (which means
         * TransactionIdIsInProgress will stop saying the prepared xact is in
         * progress), then run the post-commit or post-abort callbacks. The
         * callbacks will release the locks the transaction held.
         */
        if (isCommit)
                RecordTransactionCommitPrepared(xid,
                                                                                gid,
                                                                                hdr->nsubxacts, children,
                                                                                &persistentPrepareObjects);
        else
                RecordTransactionAbortPrepared(xid,
                                                                           hdr->nsubxacts, children,
                                                                           &persistentPrepareObjects);

        prepareAppendOnlyIntentCount = gxact->prepareAppendOnlyIntentCount;

        ProcArrayRemove(&gxact->proc, isCommit);

        /*
         * In case we fail while running the callbacks, mark the gxact invalid so
         * no one else will try to commit/rollback, and so it can be recycled
         * properly later.      It is still locked by our XID so it won't go away yet.
         *
         * (We assume it's safe to do this without taking TwoPhaseStateLock.)
         */
        if (Debug_persistent_print)
          elog(Persistent_DebugPrintLevel(),
               "FinishPreparedTransaction: gxact->proc.xid = %d  and set valid = false", gxact->proc.xid);
        gxact->valid = false;

        /*
         * We have to remove any files that were supposed to be dropped. For
         * consistency with the regular xact.c code paths, must do this before
         * releasing locks, so do it before running the callbacks.
         *
         * NB: this code knows that we couldn't be dropping any temp rels ...
         */

        PersistentFileSysObj_PreparedEndXactAction(
                                                                                        xid,
                                                                                        gid,
                                                                                        &persistentPrepareObjects,
                                                                                        isCommit,
                                                                                        prepareAppendOnlyIntentCount);

        /* And now do the callbacks */
        if (isCommit)
                ProcessRecords(bufptr, xid, twophase_postcommit_callbacks);
        else
                ProcessRecords(bufptr, xid, twophase_postabort_callbacks);

        /*
         * And now we can clean up our mess.
         */
        remove_recover_post_checkpoint_prepared_transactions_map_entry(xid, "FinishPreparedTransaction");

        RemoveGXact(gxact);

	CHECKPOINT_START_UNLOCK;

        END_CRIT_SECTION();

        /* Need to figure out the memory allocation and deallocationfor "buffer". For now, just let it leak. */

        return true;
}

/*
 * Scan a 2PC state file (already read into memory by ReadTwoPhaseFile)
 * and call the indicated callbacks for each 2PC record.
 */
static void
ProcessRecords(char *bufptr, TransactionId xid,
                           const TwoPhaseCallback callbacks[])
{
        for (;;)
        {
                TwoPhaseRecordOnDisk *record = (TwoPhaseRecordOnDisk *) bufptr;

                Assert(record->rmid <= TWOPHASE_RM_MAX_ID);
                if (record->rmid == TWOPHASE_RM_END_ID)
                        break;

                bufptr += MAXALIGN(sizeof(TwoPhaseRecordOnDisk));

                if (callbacks[record->rmid] != NULL)
                        callbacks[record->rmid] (xid, record->info,
                                                                         (void *) bufptr, record->len);

                bufptr += MAXALIGN(record->len);
        }
}

/*
 * Remove the 2PC file for the specified XID.
 *
 * If giveWarning is false, do not complain about file-not-present;
 * this is an expected case during WAL replay.
 */
void RemoveTwoPhaseFile(TransactionId xid, bool giveWarning)
{

  remove_recover_post_checkpoint_prepared_transactions_map_entry(xid,
        "RemoveTwoPhaseFile: Removing from list");

} /* end RemoveTwoPhaseFile */

/*
 * This is used in WAL replay.
 *
 */
void RecreateTwoPhaseFile(TransactionId xid, void *content, int len,
    XLogRecPtr *xlogrecptr)
{
  if (Debug_persistent_print)
    elog(Persistent_DebugPrintLevel(), "RecreateTwoPhaseFile: entering...");

  add_recover_post_checkpoint_prepared_transactions_map_entry(xid, xlogrecptr, "RecreateTwoPhaseFile: add entry to hash list");
}

/*
 * CheckPointTwoPhase -- handle 2PC component of checkpointing.
 *
 * We must fsync the state file of any GXACT that is valid and has a PREPARE
 * LSN <= the checkpoint's redo horizon.  (If the gxact isn't valid yet or
 * has a later LSN, this checkpoint is not responsible for fsyncing it.)
 *
 * This is deliberately run as late as possible in the checkpoint sequence,
 * because GXACTs ordinarily have short lifespans, and so it is quite
 * possible that GXACTs that were valid at checkpoint start will no longer
 * exist if we wait a little bit.
 *
 * If a GXACT remains valid across multiple checkpoints, it'll be fsynced
 * each time.  This is considered unusual enough that we don't bother to
 * expend any extra code to avoid the redundant fsyncs.  (They should be
 * reasonably cheap anyway, since they won't cause I/O.)
 */
void
CheckPointTwoPhase(XLogRecPtr redo_horizon)
{

       /*
        * I think this is not needed with the new two phase logic.
        * We have already attached all the prepared transactions to
        * the checkpoint record. For now, just return from this.
        */
        return;

}


/*
 * PrescanPreparedTransactions
 *
 * This function will return the oldest valid XID, and will also set
 * the ShmemVariableCache->nextXid to the next available XID.
 *
 * This function is run during database startup, after we have completed
 * reading WAL.  ShmemVariableCache->nextXid has been set to one more than
 * the highest XID for which evidence exists in WAL. The
 * crashRecoverPostCheckpointPreparedTransactions_map_ht has already been
 * populated with all pre and post checkpoint inflight transactions.
 *
 * Wwe will advance nextXid beyond any subxact XIDs belonging to valid
 * prepared xacts.  We need to do this since subxact commit doesn't
 * write a WAL entry, and so there might be no evidence in WAL of those
 * subxact XIDs.
 *
 * Our other responsibility is to determine and return the oldest valid XID
 * among the prepared xacts (if none, return ShmemVariableCache->nextXid).
 * This is needed to synchronize pg_subtrans startup properly.
 */
TransactionId
PrescanPreparedTransactions(void)
  {
    prpt_map           *entry        = NULL;
    TransactionId       origNextXid  = ShmemVariableCache->nextXid;
    TransactionId       result       = origNextXid;
    XLogRecPtr         *tfXLogRecPtr = NULL;
    XLogRecord         *tfRecord     = NULL;
    HASH_SEQ_STATUS     hsStatus;
    TwoPhaseFileHeader *hdr          = NULL;
    TransactionId       xid;
    TransactionId      *subxids;

    if (crashRecoverPostCheckpointPreparedTransactions_map_ht != NULL)
      {
        hash_seq_init(&hsStatus,crashRecoverPostCheckpointPreparedTransactions_map_ht);

        entry = (prpt_map *)hash_seq_search(&hsStatus);

        if (Debug_persistent_print)
          elog(Persistent_DebugPrintLevel(),
               "PrescanPreparedTransactions:  address entry = %p",
               entry);

        if (entry != NULL)
          tfXLogRecPtr = (XLogRecPtr *) &entry->xlogrecptr;
      }

    while (tfXLogRecPtr != NULL)
      {
        if (Debug_persistent_print)
          elog(Persistent_DebugPrintLevel(),
               "PrescanPreparedTransactions:  XLogRecPtr = %s",
               XLogLocationToString(tfXLogRecPtr));


        tfRecord = XLogReadRecord(tfXLogRecPtr, LOG);
        hdr = (TwoPhaseFileHeader *) XLogRecGetData(tfRecord);
        xid = hdr->xid;

        if (TransactionIdDidCommit(xid) == false && TransactionIdDidAbort(xid) == false)
           {
           /*
            * Incorporate xid into the running-minimum result.
            */
           if (TransactionIdPrecedes(xid, result))
              result = xid;

           /*
            * Examine subtransaction XIDs ... they should all follow main
            * XID, and they may force us to advance nextXid.
            */
           subxids = (TransactionId *)((char *)hdr + MAXALIGN(sizeof(TwoPhaseFileHeader)));
           for (int i = 0; i < hdr->nsubxacts; i++)
               {
               TransactionId subxid = subxids[i];

               Assert(TransactionIdFollows(subxid, xid));
               if (TransactionIdFollowsOrEquals(subxid, ShmemVariableCache->nextXid))
                  {
                  ShmemVariableCache->nextXid = subxid;
                  TransactionIdAdvance(ShmemVariableCache->nextXid);
                  }
               }  /* end for (int i = 0; i < hdr->nsubxacts; i++) */
           }  /* end if (TransactionIdDidCommit(xid) == false && TransactionIdDidAbort(xid) == false) */

        /* Get the next entry */
        entry = (prpt_map *)hash_seq_search(&hsStatus);

        if (Debug_persistent_print)
          elog(Persistent_DebugPrintLevel(),
               "PrescanPreparedTransactions:  address entry = %p",
               entry);

        if (entry != NULL)
          tfXLogRecPtr = (XLogRecPtr *) &entry->xlogrecptr;
        else
          tfXLogRecPtr = NULL;

      }  /* end while (tfXLogRecPtr != NULL) */

    return result;
  }  /* end PrescanPreparedTransactions */


/*
 * RecoverPreparedTransactions
 *
 * Scan the global list of post checkpoint records  and reload shared-memory state for each
 * prepared transaction (reacquire locks, etc).  This is run during database
 * startup.
 */
void RecoverPreparedTransactions()
{
        prpt_map                   *entry        = NULL;
        XLogRecPtr                 *tfXLogRecPtr = NULL;
        XLogRecord                 *tfRecord     = NULL;
        PersistentEndXactRecObjects persistentPrepareObjects;
        LocalDistribXactRef         localDistribXactRef;
        TwoPhaseFileHeader         *hdr          = NULL;
        HASH_SEQ_STATUS             hsStatus;

        if (Debug_persistent_print)
          elog(Persistent_DebugPrintLevel(), "Entering RecoverPreparedTransactions");

        if (crashRecoverPostCheckpointPreparedTransactions_map_ht != NULL)
        {
                hash_seq_init(&hsStatus,crashRecoverPostCheckpointPreparedTransactions_map_ht);

                entry = (prpt_map *)hash_seq_search(&hsStatus);

                if (Debug_persistent_print)
                  elog(Persistent_DebugPrintLevel(),
                     "RecoverPreparedTransactions:  address entry = %p",
                     entry);

                if (entry != NULL)
                  tfXLogRecPtr = (XLogRecPtr *) &entry->xlogrecptr;
        }

        while (tfXLogRecPtr != NULL)
        {
                TransactionId                    xid;
                char                            *bufptr;
                TransactionId                   *subxids;
                GlobalTransaction                gxact;

                tfRecord = XLogReadRecord(tfXLogRecPtr, LOG);

                hdr = (TwoPhaseFileHeader *) XLogRecGetData(tfRecord);

                elog(Persistent_DebugPrintLevel(),
                     "RecoverPreparedTransactions: prepared twophase record total_len = %u, xid =  %d",
                     hdr->total_len, hdr->xid);

                xid = hdr->xid;
                bufptr = (char *) hdr + MAXALIGN(sizeof(TwoPhaseFileHeader));
                subxids = (TransactionId *) bufptr;
                bufptr += MAXALIGN(hdr->nsubxacts * sizeof(TransactionId));

                PersistentEndXactRec_Deserialize((uint8*) bufptr,
                                hdr->persistentPrepareObjectCount, &persistentPrepareObjects,
                                (uint8**) &bufptr);

                if (Debug_persistent_print) {
                        elog(
                                        Persistent_DebugPrintLevel(),
                                        "RecoverPreparedTransactions: deserializeLen %d, persistentPrepareObjectCount %d",
                                        PersistentEndXactRec_DeserializeLen((uint8*) bufptr,
                                                        hdr->persistentPrepareObjectCount),
                                        hdr->persistentPrepareObjectCount);
                        PersistentEndXactRec_Print("RecoverPreparedTransactions",
                                        &persistentPrepareObjects);
                }
                /*
                 * Reconstruct subtrans state for the transaction --- needed
                 * because pg_subtrans is not preserved over a restart.  Note that
                 * we are linking all the subtransactions directly to the
                 * top-level XID; there may originally have been a more complex
                 * hierarchy, but there's no need to restore that exactly.
                 */
                for (int iSub = 0; iSub < hdr->nsubxacts; iSub++)
                        SubTransSetParent(subxids[iSub], xid);

                gxact = MarkAsPreparing( xid
                                       ,&localDistribXactRef
                                       , hdr->gid
                                       , hdr->prepared_at
                                       , hdr->owner
                                       , hdr->database
                                       , tfXLogRecPtr
                                       );
                GXactLoadSubxactData(gxact, hdr->nsubxacts, subxids);
                MarkAsPrepared(gxact);

                /*
                 * Recover other state (notably locks) using resource managers
                 */
                ProcessRecords(bufptr, xid, twophase_recover_callbacks);

                /* Get the next entry */
                entry = (prpt_map *)hash_seq_search(&hsStatus);

                if (Debug_persistent_print)
                   elog(Persistent_DebugPrintLevel(),
                        "RecoverPreparedTransactions:  address entry = %p",
                        entry);

                if (entry != NULL)
                   tfXLogRecPtr = (XLogRecPtr *) &entry->xlogrecptr;
                else
                   tfXLogRecPtr = NULL;

        }  /* end while (xlogrecptr = (XLogRecPtr *)hash_seq_search(&hsStatus)) */

}

/*
 *      RecordTransactionCommitPrepared
 *
 * This is basically the same as RecordTransactionCommit: in particular,
 * we must take the CheckpointStartLock to avoid a race condition.
 *
 * We know the transaction made at least one XLOG entry (its PREPARE),
 * so it is never possible to optimize out the commit record.
 */
static void
RecordTransactionCommitPrepared(TransactionId xid,
                                                                const char *gid,
                                                                int nchildren,
                                                                TransactionId *children,
                                                                PersistentEndXactRecObjects *persistentPrepareObjects)
{
        int16   persistentCommitObjectCount;
        char    *persistentCommitBuffer = NULL;

        XLogRecData rdata[3];
        int                     lastrdata = 0;
        xl_xact_commit_prepared xlrec;
        XLogRecPtr      recptr;

        /*
         * Look at the prepare information with respect to a commit.
         */
        persistentCommitObjectCount =
                                        PersistentEndXactRec_ObjectCount(
                                                                                persistentPrepareObjects,
                                                                                EndXactRecKind_Commit);

        /*
         * Ensure the caller already has MirroredLock then CheckpointStartLock.
         */

        /* Emit the XLOG commit record */
        xlrec.xid = xid;
        xlrec.crec.xtime = time(NULL);
        xlrec.crec.persistentCommitObjectCount = persistentCommitObjectCount;
        xlrec.crec.nsubxacts = nchildren;
        rdata[0].data = (char *) (&xlrec);
        rdata[0].len = MinSizeOfXactCommitPrepared;
        rdata[0].buffer = InvalidBuffer;
        /* dump persistent commit objects */
        if (persistentCommitObjectCount > 0)
        {
                int32 persistentCommitSerializeLen;
                int16 objectCount;

                persistentCommitSerializeLen =
                                PersistentEndXactRec_SerializeLen(
                                                                                persistentPrepareObjects,
                                                                                EndXactRecKind_Commit);

                Assert(persistentCommitSerializeLen > 0);
                persistentCommitBuffer =
                                        (char*)palloc(persistentCommitSerializeLen);

                PersistentEndXactRec_Serialize(
                                                                persistentPrepareObjects,
                                                                EndXactRecKind_Commit,
                                                                &objectCount,
                                                                (uint8*)persistentCommitBuffer,
                                                                persistentCommitSerializeLen);

                if (Debug_persistent_print)
                {
                        elog(Persistent_DebugPrintLevel(),
                                 "RecordTransactionCommitPrepared: persistentCommitSerializeLen %d, objectCount %d",
                                 persistentCommitSerializeLen,
                                 objectCount);
                        PersistentEndXactRec_Print("RecordTransactionCommitPrepared", persistentPrepareObjects);
                }

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

#ifdef FAULT_INJECTOR
        FaultInjector_InjectFaultIfSet(
                                                                   TwoPhaseTransactionCommitPrepared,
                                                                   DDLNotSpecified,
                                                                   "" /* databaseName */,
                                                                   "" /* tableName */);
#endif

        recptr = XLogInsert(RM_XACT_ID,
                                                XLOG_XACT_COMMIT_PREPARED | XLOG_NO_TRAN,
                                                rdata);

        /* we don't currently try to sleep before flush here ... */

        /* Flush XLOG to disk */
        XLogFlush(recptr);

        /* Mark the transaction committed in pg_clog */
        TransactionIdCommit(xid);

        /* to avoid race conditions, the parent must commit first */
        TransactionIdCommitTree(nchildren, children);

        if (persistentCommitBuffer != NULL)
                pfree(persistentCommitBuffer);
}

/*
 *      RecordTransactionAbortPrepared
 *
 * This is basically the same as RecordTransactionAbort.
 *
 * We know the transaction made at least one XLOG entry (its PREPARE),
 * so it is never possible to optimize out the abort record.
 */
static void
RecordTransactionAbortPrepared(TransactionId xid,
                                                           int nchildren,
                                                           TransactionId *children,
                                                           PersistentEndXactRecObjects *persistentPrepareObjects)
{
        int16   persistentAbortObjectCount;
        char    *persistentAbortBuffer = NULL;

        XLogRecData rdata[3];
        int                     lastrdata = 0;
        xl_xact_abort_prepared xlrec;
        XLogRecPtr      recptr;

        /*
         * Catch the scenario where we aborted partway through
         * RecordTransactionCommitPrepared ...
         */
        if (TransactionIdDidCommit(xid))
                elog(PANIC, "cannot abort transaction %u, it was already committed",
                         xid);

        /*
         * Look at the prepare information with respect to an abort.
         */
        persistentAbortObjectCount =
                                        PersistentEndXactRec_ObjectCount(
                                                                                persistentPrepareObjects,
                                                                                EndXactRecKind_Abort);

        /* Emit the XLOG abort record */
        xlrec.xid = xid;
        xlrec.arec.xtime = time(NULL);
        xlrec.arec.persistentAbortObjectCount = persistentAbortObjectCount;
        xlrec.arec.nsubxacts = nchildren;
        rdata[0].data = (char *) (&xlrec);
        rdata[0].len = MinSizeOfXactAbortPrepared;
        rdata[0].buffer = InvalidBuffer;
        /* dump persistent abort objects */
        if (persistentAbortObjectCount > 0)
        {
                int32 persistentAbortSerializeLen;
                int16 objectCount;

                persistentAbortSerializeLen =
                                PersistentEndXactRec_SerializeLen(
                                                                                persistentPrepareObjects,
                                                                                EndXactRecKind_Abort);

                Assert(persistentAbortSerializeLen > 0);
                persistentAbortBuffer =
                                        (char*)palloc(persistentAbortSerializeLen);

                PersistentEndXactRec_Serialize(
                                                                persistentPrepareObjects,
                                                                EndXactRecKind_Abort,
                                                                &objectCount,
                                                                (uint8*)persistentAbortBuffer,
                                                                persistentAbortSerializeLen);

                if (Debug_persistent_print)
                {
                        elog(Persistent_DebugPrintLevel(),
                                 "RecordTransactionAbortPrepared: persistentAbortSerializeLen %d",
                                 persistentAbortSerializeLen);
                        PersistentEndXactRec_Print("RecordTransactionAbortPrepared", persistentPrepareObjects);
                }

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

#ifdef FAULT_INJECTOR
        FaultInjector_InjectFaultIfSet(
                                                                   TwoPhaseTransactionAbortPrepared,
                                                                   DDLNotSpecified,
                                                                   "" /* databaseName */,
                                                                   "" /* tableName */);
#endif

        recptr = XLogInsert(RM_XACT_ID,
                                                XLOG_XACT_ABORT_PREPARED | XLOG_NO_TRAN,
                                                rdata);

        /* Always flush, since we're about to remove the 2PC state file */
        XLogFlush(recptr);

        /*
         * Mark the transaction aborted in clog.  This is not absolutely necessary
         * but we may as well do it while we are here.
         */
        TransactionIdAbort(xid);
        TransactionIdAbortTree(nchildren, children);

        if (persistentAbortBuffer != NULL)
                pfree(persistentAbortBuffer);

}

/*
 * This function will gather up all the current prepared transaction xlog pointers,
 * and pass that information back to the caller.
 */
void
getTwoPhasePreparedTransactionData(prepared_transaction_agg_state **ptas, char *caller)
{

  int                numberOfPrepareXacts     = TwoPhaseState->numPrepXacts;
  GlobalTransaction *globalTransactionArray   = TwoPhaseState->prepXacts;
  TransactionId      xid;
  XLogRecPtr        *recordPtr                = NULL;
  int                maxCount;

  elog(PersistentRecovery_DebugPrintLevel(),
       "getTwoPhasePreparedTransactionData: start of function from caller %s",
       caller);

  Assert(*ptas == NULL);

  TwoPhaseAddPreparedTransactionInit(ptas, &maxCount);

  elog(PersistentRecovery_DebugPrintLevel(),
       "getTwoPhasePreparedTransactionData: numberOfPrepareXacts = %d",
       numberOfPrepareXacts);

  for (int i = 0; i < numberOfPrepareXacts; i++)
    {
      if ((globalTransactionArray[i])->valid == false)
        /* Skip any invalid prepared transacitons. */
        continue;
      xid       = (globalTransactionArray[i])->proc.xid;
      recordPtr = &(globalTransactionArray[i])->prepare_begin_lsn;

      elog(PersistentRecovery_DebugPrintLevel(),
           "getTwoPhasePreparedTransactionData: add entry xid = %u,  XLogRecPtr = %s, caller = %s",
           xid,
           XLogLocationToString(recordPtr),
           caller);

      TwoPhaseAddPreparedTransaction( ptas
                                    ,&maxCount
                                    , xid
                                    , recordPtr
                                    , caller
                                    );
    }


}  /* end getTwoPhasePreparedTransactionData */


/*
 * This function will allocate enough space to accomidate maxCount values.
 */
void
TwoPhaseAddPreparedTransactionInit(
                                     prepared_transaction_agg_state **ptas, int *maxCount)
{
  int len;

  Assert (*ptas == NULL);

  *maxCount = 10;         // Start off with at least this much room.
  len = PREPARED_TRANSACTION_CHECKPOINT_BYTES(*maxCount);
  *ptas = (prepared_transaction_agg_state*)palloc0(len);

}  /* end TwoPhaseAddPreparedTransactionInit */


/*
 * This function adds another entry to the list of prepared transactions.
 */
void
TwoPhaseAddPreparedTransaction( prepared_transaction_agg_state **ptas
                              , int                             *maxCount
                              , TransactionId                    xid
                              , XLogRecPtr                      *xlogPtr
                              , char                            *caller
                              )
{
  int       len;
  int       count;
  prpt_map *m;

  Assert(*ptas != NULL);
  Assert(*maxCount > 0);

  count = (*ptas)->count;
  Assert(count <= *maxCount);

  if (count == *maxCount)
    {
      prepared_transaction_agg_state *oldPtas;

      oldPtas = *ptas;

      (*maxCount) *= 2;               // Double.
      len = PREPARED_TRANSACTION_CHECKPOINT_BYTES(*maxCount);
      *ptas = (prepared_transaction_agg_state*)palloc0(len);
      memcpy(*ptas, oldPtas, PREPARED_TRANSACTION_CHECKPOINT_BYTES(count));
      pfree(oldPtas);
    }

  m = &(*ptas)->maps[count];
  m->xid = xid;
  m->xlogrecptr.xlogid = xlogPtr->xlogid;
  m->xlogrecptr.xrecoff = xlogPtr->xrecoff;

  if (Debug_persistent_recovery_print)
    {
      SUPPRESS_ERRCONTEXT_DECLARE;

      SUPPRESS_ERRCONTEXT_PUSH();

      elog(PersistentRecovery_DebugPrintLevel(),
           "TwoPhaseAddPreparedTransaction: add entry  XLogRecPtr = %s, caller = %s",
           XLogLocationToString(xlogPtr),
           caller);

      SUPPRESS_ERRCONTEXT_POP();
    }

  (*ptas)->count++;


}  /* end TwoPhaseAddPreparedTransaction */


/*
 * Return a pointer to the oldest XLogRecPtr in the list or NULL if the list is empty.
 */
XLogRecPtr *
getTwoPhaseOldestPreparedTransactionXLogRecPtr(XLogRecData *rdata)
{
  prepared_transaction_agg_state *ptas      = (prepared_transaction_agg_state *)rdata->data;
  int                             map_count = ptas->count;
  prpt_map                       *m         = ptas->maps;
  XLogRecPtr                     *oldest    = NULL;

  elog(PersistentRecovery_DebugPrintLevel(),
       "getTwoPhaseOldestPreparedTransactionXLogRecPtr: map_count = %d", map_count);

  if (map_count > 0)
    {
      oldest = &(m[0].xlogrecptr);
      for (int i = 1; i < map_count; i++)
        {
          elog(PersistentRecovery_DebugPrintLevel(),
               "getTwoPhaseOldestPreparedTransactionXLogRecPtr: checkpoint prepared pointer %d = %s", i, XLogLocationToString(oldest));
          if (XLByteLE(m[i].xlogrecptr, *oldest))
            oldest = &(m[i].xlogrecptr);
        }
    }

  return oldest;

}  /* end getTwoPhaseOldestPreparedTransactionXLogRecPtr */

