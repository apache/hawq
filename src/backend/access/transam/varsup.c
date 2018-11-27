/*-------------------------------------------------------------------------
 *
 * varsup.c
 *	  postgres OID & XID variables support routines
 *
 * Copyright (c) 2000-2008, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  $PostgreSQL: pgsql/src/backend/access/transam/varsup.c,v 1.76 2006/11/05 22:42:07 tgl Exp $
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "access/clog.h"
#include "access/subtrans.h"
#include "access/transam.h"
#include "catalog/catquery.h"
#include "executor/spi.h"
#include "miscadmin.h"
#include "postmaster/autovacuum.h"
#include "storage/pmsignal.h"
#include "storage/proc.h"
#include "utils/builtins.h"
#include "utils/guc.h"
#include "cdb/cdbpersistentstore.h"


/* Number of OIDs to prefetch (preallocate) per XLOG write */
#define VAR_OID_PREFETCH		8192

/* pointer to "variable cache" in shared memory (set up by shmem.c) */
VariableCache ShmemVariableCache = NULL;

int xid_stop_limit = 20000000;

static Oid master_highest_used_oid(void);

/* initialize external Oid on first access */
static bool IsExternalOidInitialized = false;
/* 
 * validate reserved range for external objects
 * by comparing with master_highest_used_oid when
 * NextExternalOid is initialized
 */
static bool IsValidExternalOidRange = false;
/* next OID to assign to external tables */
static Oid NextExternalOid = InvalidOid;

/*
 * Allocate the next XID for my new transaction or subtransaction.
 */
TransactionId
GetNewTransactionId(bool isSubXact, bool setProcXid)
{
	TransactionId xid;

	/*
	 * During bootstrap initialization, we return the special bootstrap
	 * transaction id.
	 */
	if (IsBootstrapProcessingMode())
	{
		Assert(!isSubXact);
		//MyProc->xid = BootstrapTransactionId;
		return BootstrapTransactionId;
	}

	LWLockAcquire(XidGenLock, LW_EXCLUSIVE);

	xid = ShmemVariableCache->nextXid;

	/*----------
	 * Check to see if it's safe to assign another XID.  This protects against
	 * catastrophic data loss due to XID wraparound.  The basic rules are:
	 *
	 * If we're past xidVacLimit, start trying to force autovacuum cycles.
	 * If we're past xidWarnLimit, start issuing warnings.
	 * If we're past xidStopLimit, refuse to execute transactions, unless
	 * we are running in a standalone backend (which gives an escape hatch
	 * to the DBA who somehow got past the earlier defenses).
	 *
	 * Test is coded to fall out as fast as possible during normal operation,
	 * ie, when the vac limit is set and we haven't violated it.
	 *----------
	 */
	if (TransactionIdFollowsOrEquals(xid, ShmemVariableCache->xidVacLimit) &&
		TransactionIdIsValid(ShmemVariableCache->xidVacLimit))
	{
		/*
		 * To avoid swamping the postmaster with signals, we issue the autovac
		 * request only once per 64K transaction starts.  This still gives
		 * plenty of chances before we get into real trouble.
		 */
		if (IsUnderPostmaster && (xid % 65536) == 0)
		{
			elog(LOG, "GetNewTransactionId: requesting autovac (xid %u xidVacLimit %u)", xid, ShmemVariableCache->xidVacLimit);
			SendPostmasterSignal(PMSIGNAL_START_AUTOVAC);
		}

		if (IsUnderPostmaster &&
			TransactionIdFollowsOrEquals(xid, ShmemVariableCache->xidStopLimit))
			ereport(ERROR,
					(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
					 errmsg("database is not accepting commands to avoid wraparound data loss in database \"%s\"",
							NameStr(ShmemVariableCache->limit_datname)),
					 errhint("Stop the postmaster and use a standalone backend to vacuum database \"%s\".",
							 NameStr(ShmemVariableCache->limit_datname))));
		else if (TransactionIdFollowsOrEquals(xid, ShmemVariableCache->xidWarnLimit))
			ereport(WARNING,
					(errmsg("database \"%s\" must be vacuumed within %u transactions",
							NameStr(ShmemVariableCache->limit_datname),
							ShmemVariableCache->xidWrapLimit - xid),
					 errhint("To avoid a database shutdown, execute a full-database VACUUM in \"%s\".",
							 NameStr(ShmemVariableCache->limit_datname))));
	}

	/*
	 * If we are allocating the first XID of a new page of the commit log,
	 * zero out that commit-log page before returning. We must do this while
	 * holding XidGenLock, else another xact could acquire and commit a later
	 * XID before we zero the page.  Fortunately, a page of the commit log
	 * holds 32K or more transactions, so we don't have to do this very often.
	 *
	 * Extend pg_subtrans too.
	 */
	ExtendCLOG(xid);
	ExtendSUBTRANS(xid);

	/*
	 * Now advance the nextXid counter.  This must not happen until after we
	 * have successfully completed ExtendCLOG() --- if that routine fails, we
	 * want the next incoming transaction to try it again.	We cannot assign
	 * more XIDs until there is CLOG space for them.
	 */
	TransactionIdAdvance(ShmemVariableCache->nextXid);

	/*
	 * We must store the new XID into the shared PGPROC array before releasing
	 * XidGenLock.	This ensures that when GetSnapshotData calls
	 * ReadNewTransactionId, all active XIDs before the returned value of
	 * nextXid are already present in PGPROC.  Else we have a race condition.
	 *
	 * XXX by storing xid into MyProc without acquiring ProcArrayLock, we are
	 * relying on fetch/store of an xid to be atomic, else other backends
	 * might see a partially-set xid here.	But holding both locks at once
	 * would be a nasty concurrency hit (and in fact could cause a deadlock
	 * against GetSnapshotData).  So for now, assume atomicity. Note that
	 * readers of PGPROC xid field should be careful to fetch the value only
	 * once, rather than assume they can read it multiple times and get the
	 * same answer each time.
	 *
	 * The same comments apply to the subxact xid count and overflow fields.
	 *
	 * A solution to the atomic-store problem would be to give each PGPROC its
	 * own spinlock used only for fetching/storing that PGPROC's xid and
	 * related fields.
	 *
	 * If there's no room to fit a subtransaction XID into PGPROC, set the
	 * cache-overflowed flag instead.  This forces readers to look in
	 * pg_subtrans to map subtransaction XIDs up to top-level XIDs. There is a
	 * race-condition window, in that the new XID will not appear as running
	 * until its parent link has been placed into pg_subtrans. However, that
	 * will happen before anyone could possibly have a reason to inquire about
	 * the status of the XID, so it seems OK.  (Snapshots taken during this
	 * window *will* include the parent XID, so they will deliver the correct
	 * answer later on when someone does have a reason to inquire.)
	 */
	if (setProcXid && MyProc != NULL)
	{
		/*
		 * Use volatile pointer to prevent code rearrangement; other backends
		 * could be examining my subxids info concurrently, and we don't want
		 * them to see an invalid intermediate state, such as incrementing
		 * nxids before filling the array entry.  Note we are assuming that
		 * TransactionId and int fetch/store are atomic.
		 */
		volatile PGPROC *myproc = MyProc;

		if (!isSubXact)
			myproc->xid = xid;
		else
		{
			int			nxids = myproc->subxids.nxids;

			if (nxids < PGPROC_MAX_CACHED_SUBXIDS)
			{
				myproc->subxids.xids[nxids] = xid;
				myproc->subxids.nxids = nxids + 1;
			}
			else
				myproc->subxids.overflowed = true;
		}
	}

	LWLockRelease(XidGenLock);

	return xid;
}

/*
 * Read nextXid but don't allocate it.
 */
TransactionId
ReadNewTransactionId(void)
{
	TransactionId xid;

	LWLockAcquire(XidGenLock, LW_SHARED);
	xid = ShmemVariableCache->nextXid;
	LWLockRelease(XidGenLock);

	return xid;
}

/*
 * Determine the last safe XID to allocate given the currently oldest
 * datfrozenxid (ie, the oldest XID that might exist in any database
 * of our cluster).
 */
void
SetTransactionIdLimit(TransactionId oldest_datfrozenxid,
					  Name oldest_datname)
{
	TransactionId xidVacLimit;
	TransactionId xidWarnLimit;
	TransactionId xidStopLimit;
	TransactionId xidWrapLimit;
	TransactionId curXid;

	Assert(TransactionIdIsNormal(oldest_datfrozenxid));

	/*
	 * The place where we actually get into deep trouble is halfway around
	 * from the oldest potentially-existing XID.  (This calculation is
	 * probably off by one or two counts, because the special XIDs reduce the
	 * size of the loop a little bit.  But we throw in plenty of slop below,
	 * so it doesn't matter.)
	 */
	xidWrapLimit = oldest_datfrozenxid + (MaxTransactionId >> 1);
	if (xidWrapLimit < FirstNormalTransactionId)
		xidWrapLimit += FirstNormalTransactionId;

	/*
	 * We'll refuse to continue assigning XIDs in interactive mode once we get
	 * within xid_stop_limit transactions of data loss.  This leaves lots of
	 * room for the DBA to fool around fixing things in a standalone backend,
	 * while not being significant compared to total XID space. (Note that since
	 * vacuuming requires one transaction per table cleaned, we had better be
	 * sure there's lots of XIDs left...)
	 */
	xidStopLimit = xidWrapLimit - (TransactionId)xid_stop_limit;
	if (xidStopLimit < FirstNormalTransactionId)
		xidStopLimit -= FirstNormalTransactionId;

	/*
	 * We'll start complaining loudly when we get within 10M + xid_stop_limit
	 * transactions of the stop point.	This is kind of arbitrary, but if
	 * you let your gas gauge get down to 1% of full, would you be looking for
	 * the next gas station?  We need to be fairly liberal about this number
	 * because there are lots of scenarios where most transactions are done by
	 * automatic clients that won't pay attention to warnings. (No, we're not
	 * gonna make this configurable.  If you know enough to configure it, you
	 * know enough to not get in this kind of trouble in the first place.)
	 */
	xidWarnLimit = xidStopLimit - 10000000 - (TransactionId)xid_stop_limit;
	if (xidWarnLimit < FirstNormalTransactionId)
		xidWarnLimit -= FirstNormalTransactionId;

	/*
	 * We'll start trying to force autovacuums when oldest_datfrozenxid gets
	 * to be more than autovacuum_freeze_max_age transactions old.
	 *
	 * Note: guc.c ensures that autovacuum_freeze_max_age is in a sane range,
	 * so that xidVacLimit will be well before xidWarnLimit.
	 *
	 * Note: autovacuum_freeze_max_age is a PGC_POSTMASTER parameter so that
	 * we don't have to worry about dealing with on-the-fly changes in its
	 * value.  It doesn't look practical to update shared state from a GUC
	 * assign hook (too many processes would try to execute the hook,
	 * resulting in race conditions as well as crashes of those not connected
	 * to shared memory).  Perhaps this can be improved someday.
	 */
	xidVacLimit = oldest_datfrozenxid + autovacuum_freeze_max_age;
	if (xidVacLimit < FirstNormalTransactionId)
		xidVacLimit += FirstNormalTransactionId;

	/* Grab lock for just long enough to set the new limit values */
	LWLockAcquire(XidGenLock, LW_EXCLUSIVE);
	ShmemVariableCache->oldestXid = oldest_datfrozenxid;
	ShmemVariableCache->xidVacLimit = xidVacLimit;
	ShmemVariableCache->xidWarnLimit = xidWarnLimit;
	ShmemVariableCache->xidStopLimit = xidStopLimit;
	ShmemVariableCache->xidWrapLimit = xidWrapLimit;
	namecpy(&ShmemVariableCache->limit_datname, oldest_datname);
	curXid = ShmemVariableCache->nextXid;
	LWLockRelease(XidGenLock);

	/* Log the info */
	ereport(DEBUG1,
	   (errmsg("transaction ID wrap limit is %u, limited by database \"%s\"",
			   xidWrapLimit, NameStr(*oldest_datname))));

	/*
	 * If past the autovacuum force point, immediately signal an autovac
	 * request.  The reason for this is that autovac only processes one
	 * database per invocation.  Once it's finished cleaning up the oldest
	 * database, it'll call here, and we'll signal the postmaster to start
	 * another iteration immediately if there are still any old databases.
	 */
	if (TransactionIdFollowsOrEquals(curXid, xidVacLimit) &&
		IsUnderPostmaster)
		SendPostmasterSignal(PMSIGNAL_START_AUTOVAC);

	/* Give an immediate warning if past the wrap warn point */
	if (TransactionIdFollowsOrEquals(curXid, xidWarnLimit))
		ereport(WARNING,
		   (errmsg("database \"%s\" must be vacuumed within %u transactions",
				   NameStr(*oldest_datname),
				   xidWrapLimit - curXid),
			errhint("To avoid a database shutdown, execute a full-database VACUUM in \"%s\".",
					NameStr(*oldest_datname))));
}


/*
 * GetNewObjectId -- allocate a new OID
 *
 * OIDs are generated by a cluster-wide counter.  Since they are only 32 bits
 * wide, counter wraparound will occur eventually, and therefore it is unwise
 * to assume they are unique unless precautions are taken to make them so.
 * Hence, this routine should generally not be used directly.  The only
 * direct callers should be GetNewOid() and GetNewRelFileNode() in
 * catalog/catalog.c.
 */
Oid
GetNewObjectId(void)
{
	Oid			result;

	LWLockAcquire(OidGenLock, LW_EXCLUSIVE);

	/*
	 * Check for wraparound of the OID counter.  We *must* not return 0
	 * (InvalidOid); and as long as we have to check that, it seems a good
	 * idea to skip over everything below FirstNormalObjectId too. (This
	 * basically just avoids lots of collisions with bootstrap-assigned OIDs
	 * right after a wrap occurs, so as to avoid a possibly large number of
	 * iterations in GetNewOid.)  Note we are relying on unsigned comparison.
	 *
	 * During initdb, we start the OID generator at FirstBootstrapObjectId, so
	 * we only enforce wrapping to that point when in bootstrap or standalone
	 * mode.  The first time through this routine after normal postmaster
	 * start, the counter will be forced up to FirstNormalObjectId. This
	 * mechanism leaves the OIDs between FirstBootstrapObjectId and
	 * FirstNormalObjectId available for automatic assignment during initdb,
	 * while ensuring they will never conflict with user-assigned OIDs.
	 */
	if (ShmemVariableCache->nextOid < ((Oid) FirstNormalObjectId) 
			|| ShmemVariableCache->nextOid >= (Oid) FirstExternalObjectId)
	{
		if (IsPostmasterEnvironment)
		{
			/* wraparound in normal environment */
			ShmemVariableCache->nextOid = FirstNormalObjectId;
			ShmemVariableCache->oidCount = 0;
		}
		else
		{
			/* we may be bootstrapping, so don't enforce the full range */
			if (ShmemVariableCache->nextOid < ((Oid) FirstBootstrapObjectId))
			{
				/* wraparound in standalone environment? */
				ShmemVariableCache->nextOid = FirstBootstrapObjectId;
				ShmemVariableCache->oidCount = 0;
			}
		}
	}

	/* If we run out of logged for use oids then we must log more */
	if (ShmemVariableCache->oidCount == 0)
	{
		XLogPutNextOid(ShmemVariableCache->nextOid + VAR_OID_PREFETCH);
		ShmemVariableCache->oidCount = VAR_OID_PREFETCH;
	}

	result = ShmemVariableCache->nextOid;

	(ShmemVariableCache->nextOid)++;
	(ShmemVariableCache->oidCount)--;

	LWLockRelease(OidGenLock);

	return result;
}

/*
 * GetNewExternalObjectId
 * 		Allocate a new OID for an externally imported table from the reserved range of oids
 */
Oid
GetNewExternalObjectId(void)
{
	Oid			result;

	Assert(IsPostmasterEnvironment);

	/*
	 * must perform check on External Oid range on
	 * initial access of NextExternalOid
	 *
	 * It's needed for upgrade scenario from old version
	 * of HAWQ which doesn't support dedicated oid pool for HCatalog objects
	 */
	if (!IsExternalOidInitialized)
	{
		IsExternalOidInitialized = true;
		IsValidExternalOidRange = master_highest_used_oid() < FirstExternalObjectId;
		ResetExternalObjectId();
	}

	/*
	 * This check is needed for upgrade from old HAWQ versions, which don't support
	 * oid pool for HCatalog objects.
	 * In current implementation max oid will be always less than FirstExternalObjectId.
	 */
	if (!IsValidExternalOidRange)
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_RESOURCES),
				 errmsg("database does not have enough available Oids to support HCatalog queries"),
				 errhint("Database VACUUM may recycle unused Oids.")));

	/* 
	 * NextExternalOid==0 indicates a rollover has occurred in the
	 * previous external Oid creation
	 *
	 * Current specifications do not support wrap around as
	 * in-memory constraints cannot be enforced for Oids in
	 * different catalog tables
	 */
	if (!OidIsValid(NextExternalOid) || NextExternalOid < (Oid) FirstExternalObjectId)
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_RESOURCES),
				 errmsg("number of external objects from HCatalog exceeded 10M during transaction"),
				 errhint("Separate HCatalog queries into different transactions to process.")));

	result = NextExternalOid;

	(NextExternalOid)++;

	return result;
}

/*
 * GetCurrentExternalObjectId
 * 		This getter method is for testing purpose only
 */
Oid
GetCurrentExternalObjectId(void)
{
	return NextExternalOid;
}

/*
 * SetCurrentExternalObjectId
 *     This setter method is for testing purpose only
 */
void
SetCurrentExternalObjectId(Oid nextExtOid)
{
	NextExternalOid = nextExtOid;
}

void
ResetExternalObjectId(void)
{
	if (IsValidExternalOidRange)
	{
		NextExternalOid = (Oid) FirstExternalObjectId;
	}
}

/*
 * master_highest_used_oid
 * 		Uses CAQL and SPI to find the highest used Oid among user and catalog tables
 *
 * 		Uses CAQL to query catalog tables
 * 		Uses SPI to query user tables, because CAQL supports tables from CatCoreRelation array only
 * 		1) Find all the relations that has Oids
 * 		2) Find max oid from those relations
 */
Oid
master_highest_used_oid(void)
{
	Oid oidMaxCatalog = InvalidOid;
	Oid oidMaxUser = InvalidOid;
	Oid oidMax = InvalidOid;
	Oid currentOid;
	Form_pg_class classForm;
	cqContext *pcqOuterCtx;
	cqContext *pcqInnerCtx;
	HeapTuple outerTuple;
	HeapTuple innerTuple;
	/* number of user tables having oids*/
	int userTablesNum = 0;
	int ret;

	pcqOuterCtx = caql_beginscan(NULL, cql("SELECT * FROM pg_class WHERE relhasoids = :1", BoolGetDatum(true)));

	outerTuple = caql_getnext(pcqOuterCtx);

	if (!HeapTupleIsValid(outerTuple))
	{
		caql_endscan(pcqOuterCtx);
		elog(DEBUG1, "Unable to get list of tables having oids");
		return oidMax;
	}

	/* construct query to get max oid from all tables with oids */
	StringInfo sqlstrCatalog = makeStringInfo();
	StringInfo sqlstrUser = makeStringInfo();
	appendStringInfo(sqlstrUser, "SELECT max(oid) FROM (");
	while (HeapTupleIsValid(outerTuple))
	{
		classForm = (Form_pg_class) GETSTRUCT(outerTuple);

		/* use CAQL for accessing catalog tables*/
		if (classForm->relnamespace == PG_CATALOG_NAMESPACE)
		{
			appendStringInfo(sqlstrCatalog,
					"SELECT oid FROM %s WHERE oid > :1 ORDER BY oid",
					classForm->relname.data);

			pcqInnerCtx = caql_beginscan(NULL,
					cql1(sqlstrCatalog->data, __FILE__, __LINE__,
							ObjectIdGetDatum(oidMaxCatalog)));

			innerTuple = caql_getnext(pcqInnerCtx);

			currentOid = InvalidOid;

			while (HeapTupleIsValid(innerTuple))
			{
				currentOid = HeapTupleGetOid(innerTuple);
				innerTuple = caql_getnext(pcqInnerCtx);
			}

			elog(DEBUG1, "Max Oid in catalog table %s: %d", classForm->relname.data, currentOid);

			caql_endscan(pcqInnerCtx);

			oidMaxCatalog = currentOid > oidMaxCatalog ? currentOid : oidMaxCatalog;

			resetStringInfo(sqlstrCatalog);
		}
		else
		/* use SPI for user tables*/
		{
			userTablesNum++;
			{
				if (userTablesNum > 1)
				{
					appendStringInfo(sqlstrUser, " UNION ALL ");
				}
				appendStringInfo(sqlstrUser, "SELECT MAX(oid) AS oid FROM %s", classForm->relname.data);
			}
		}

		outerTuple = caql_getnext(pcqOuterCtx);
	}

	caql_endscan(pcqOuterCtx);

	if (userTablesNum) {

		appendStringInfo(sqlstrUser, ") AS x");

		if (SPI_OK_CONNECT != SPI_connect())
		{
			ereport(ERROR, (errcode(ERRCODE_CDB_INTERNAL_ERROR),
					errmsg("Unable to connect to execute internal query for HCatalog.")));
		}

		ret = SPI_execute(sqlstrUser->data, true, 1);

		if (ret > 0 && NULL != SPI_tuptable) {
			TupleDesc tupdesc = SPI_tuptable->tupdesc;
			SPITupleTable *tuptable = SPI_tuptable;
			HeapTuple tuple = tuptable->vals[0];
			char *oidString = SPI_getvalue(tuple, tupdesc, 1);
			if (NULL != oidString)
			{
				oidMaxUser = DatumGetObjectId(DirectFunctionCall1(oidin, CStringGetDatum(oidString)));
			}
		}
	}

	pfree(sqlstrCatalog->data);
	pfree(sqlstrUser->data);
	SPI_finish();

	elog(DEBUG1, "Highest Oid currently in use among catalog tables: %u", oidMaxCatalog);
	elog(DEBUG1, "Highest Oid currently in use among user tables having oid: %u", oidMaxUser);

	oidMax = oidMaxCatalog > oidMaxUser ? oidMaxCatalog : oidMaxUser;

	elog(DEBUG1, "Highest Oid currently in use: %u", oidMax);

	return oidMax;
}
