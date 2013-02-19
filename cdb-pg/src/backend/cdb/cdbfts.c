/*-------------------------------------------------------------------------
 *
 * cdbfts.c
 *	  Provides fault tolerance service routines for mpp.
 *
 * Copyright (c) 2003-2008, Greenplum inc
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"
#include <assert.h>

#include "miscadmin.h"
#include "gp-libpq-fe.h"
#include "gp-libpq-int.h"
#include "utils/memutils.h"
#include "cdb/cdbvars.h"
#include "cdb/cdbconn.h"
#include "cdb/cdbutil.h"
#include "cdb/cdbdisp.h"
#include "access/xact.h"
#include "cdb/cdbfts.h"
#include "cdb/cdblink.h"
#include "cdb/cdbtm.h"
#include "cdb/cdbutil.h"
#include "libpq/libpq-be.h"
#include "commands/dbcommands.h"
#include "storage/proc.h"

#include "executor/spi.h"

#include "postmaster/service.h"
#include "postmaster/walsendserver.h"
#include "postmaster/fts.h"
#include "postmaster/primary_mirror_mode.h"
#include "utils/faultinjection.h"

#include "utils/fmgroids.h"
#include "catalog/pg_authid.h"

/* segment id for the master */
#define MASTER_SEGMENT_ID -1

FtsProbeInfo *ftsProbeInfo = NULL; /* Probe process updates this structure */
volatile bool	*ftsEnabled;
volatile bool	*ftsShutdownMaster;
static LWLockId	ftsControlLock;

static volatile bool	*ftsReadOnlyFlag;
static volatile bool	*ftsAdminRequestedRO;

static bool		local_fts_status_initialized=false;
static uint64	local_fts_statusVersion;

static LWLockId ftsQDMirrorLock;
static LWLockId ftsQDMirrorUpdateConfigLock;
static struct FtsQDMirrorInfo *ftsQDMirrorInfo=NULL;

static char *QDMirroringDisabledReasonToString(QDMIRRORDisabledReason disabledReason);
static AliveSegmentsInfo *GetSegmentsInfo(Bitmapset *last_alive_segment_bms);

/*
 * get fts share memory size
 */
int
FtsShmemSize(void)
{
	/*
	 * this shared memory block doesn't even need to *exist* on the
	 * QEs!
	 */
	if ((Gp_role != GP_ROLE_DISPATCH) && (Gp_role != GP_ROLE_UTILITY))
		return 0;

	return MAXALIGN(sizeof(FtsControlBlock));
}

void
FtsShmemInit(void)
{
	bool		found;
	FtsControlBlock *shared;

	shared = (FtsControlBlock *)ShmemInitStruct("Fault Tolerance manager", FtsShmemSize(), &found);
	if (!shared)
		elog(FATAL, "FTS: could not initialize fault tolerance manager share memory");

	/* Initialize locks and shared memory area */

	ftsEnabled = &shared->ftsEnabled;
	ftsShutdownMaster = &shared->ftsShutdownMaster;
	ftsControlLock = shared->ControlLock;

	ftsReadOnlyFlag = &shared->ftsReadOnlyFlag; /* global RO state */

	ftsAdminRequestedRO = &shared->ftsAdminRequestedRO; /* Admin request -- guc-controlled RO state */

	ftsQDMirrorLock = shared->ftsQDMirrorLock;
	ftsQDMirrorUpdateConfigLock = shared->ftsQDMirrorUpdateConfigLock;
	ftsQDMirrorInfo = &shared->ftsQDMirror;

	ftsProbeInfo = &shared->fts_probe_info;

	if (!IsUnderPostmaster)
	{
		shared->ControlLock = LWLockAssign();
		ftsControlLock = shared->ControlLock;

		shared->ftsQDMirrorLock = LWLockAssign();
		ftsQDMirrorLock = shared->ftsQDMirrorLock;

		shared->ftsQDMirrorUpdateConfigLock = LWLockAssign();
		ftsQDMirrorUpdateConfigLock = shared->ftsQDMirrorUpdateConfigLock;

		ftsQDMirrorInfo->configurationChecked = false;
		ftsQDMirrorInfo->state = QDMIRROR_STATE_NONE;
		ftsQDMirrorInfo->updateMask = QDMIRROR_UPDATEMASK_NONE;
		ftsQDMirrorInfo->disabledReason = QDMIRROR_DISABLEDREASON_NONE;
		memset(&ftsQDMirrorInfo->lastLogTimeVal, 0, sizeof(struct timeval));
		strcpy(ftsQDMirrorInfo->errorMessage, "");
		ftsQDMirrorInfo->haveNewCheckpointLocation = false;
		memset(&ftsQDMirrorInfo->newCheckpointLocation, 0, sizeof(XLogRecPtr));
		memset(ftsQDMirrorInfo->name, 0, sizeof(ftsQDMirrorInfo->name));
		ftsQDMirrorInfo->port = 0;
		ftsQDMirrorInfo->QDMirroringNotSynchronizedWarningGiven = false;

		/* initialize */
		shared->ftsReadOnlyFlag = gp_set_read_only;
		shared->ftsAdminRequestedRO = gp_set_read_only;

		shared->fts_probe_info.fts_probePid = 0;
		shared->fts_probe_info.fts_pauseProbes = false;
		shared->fts_probe_info.fts_discardResults = false;
		shared->fts_probe_info.fts_statusVersion = 0;

		shared->ftsEnabled = true; /* ??? */
		shared->ftsShutdownMaster = false;
	}
}

void
ftsLock(void)
{
	LWLockAcquire(ftsControlLock, LW_EXCLUSIVE);
}

void
ftsUnlock(void)
{
	LWLockRelease(ftsControlLock);
}

void
FtsNotifyProber(void)
{
	/*
	 * This is a full-scan request. We set the request-flag == to the bitmap version flag.
	 * When the version has been bumped, we know that the request has been filled.
	 */
	ftsProbeInfo->fts_probeScanRequested = ftsProbeInfo->fts_statusVersion;

	/* signal fts-probe */
	if (ftsProbeInfo->fts_probePid)
		kill(ftsProbeInfo->fts_probePid, SIGINT);

	/* sit and spin */
	while (ftsProbeInfo->fts_probeScanRequested == ftsProbeInfo->fts_statusVersion)
	{
		struct timeval tv;

		tv.tv_usec = 50000;
		tv.tv_sec = 0;
		select(0, NULL, NULL, NULL, &tv); /* don't care about return value. */

		CHECK_FOR_INTERRUPTS();
	}
}


/*
 * Check if master needs to shut down
 */
bool FtsMasterShutdownRequested()
{
	return *ftsShutdownMaster;
}


/*
 * Set flag indicating that master needs to shut down
 */
void FtsRequestMasterShutdown()
{
#ifdef USE_ASSERT_CHECKING
	Assert(!*ftsShutdownMaster);

	PrimaryMirrorMode pm_mode;
	getPrimaryMirrorStatusCodes(&pm_mode, NULL, NULL, NULL);
	Assert(pm_mode == PMModeMaster);
#endif /*USE_ASSERT_CHECKING*/

	*ftsShutdownMaster = true;
}


/*
 * Test-Connection: This is called from the threaded context inside the
 * dispatcher: ONLY CALL THREADSAFE FUNCTIONS -- elog() is NOT threadsafe.
 */
bool
FtsTestConnection(CdbComponentDatabaseInfo *failedDBInfo, bool fullScan)
{
	/* master is always reported as alive */
	if (failedDBInfo->segindex == MASTER_SEGMENT_ID)
	{
		return true;
	}

	/*
	 * If this is not a failover segment, use the old method.
	 * If this is a failover segment, skip the test is ok. The host segment will
	 * be checked to make sure the fault can be detected correctly.
	 */
	if (GpAliveSegmentsInfo.aliveSegmentsBitmap &&
		!bms_is_member(failedDBInfo->segindex, GpAliveSegmentsInfo.aliveSegmentsBitmap))
		return true;

	if (!fullScan)
	{
		return FTS_STATUS_ISALIVE(failedDBInfo->dbid, ftsProbeInfo->fts_status);
	}

	FtsNotifyProber();

	return FTS_STATUS_ISALIVE(failedDBInfo->dbid, ftsProbeInfo->fts_status);
}

/*
 * Re-Configure the system: if someone has noticed that the status
 * version has been updated, they call this to verify that they've got
 * the right configuration.
 *
 * NOTE: This *always* destroys gangs. And also attempts to inform the
 * fault-prober to do a full scan.
 */
void
FtsReConfigureMPP(bool create_new_gangs)
{
	/* need to scan to pick up the latest view */
	detectFailedConnections();
	local_fts_statusVersion = ftsProbeInfo->fts_statusVersion;

	ereport(LOG, (errmsg_internal("FTS: reconfiguration is in progress"),
			errSendAlert(true)));
	disconnectAndDestroyAllGangs();

	/* Caller should throw an error. */
	return;
}

static void
QDMirroringFormatTime(char *logTimeStr, int logTimeStrMax,
							  struct timeval *lastLogTimeVal)
{
	pg_time_t stamp_time = (pg_time_t) lastLogTimeVal->tv_sec;
	pg_tz	   *tz;

	/*
	 * The following code is copied from log_line_prefix.
	 */
	tz = log_timezone ? log_timezone : gmt_timezone;

	pg_strftime(logTimeStr, logTimeStrMax,
				"%Y-%m-%d %H:%M:%S %Z",
				pg_localtime(&stamp_time, tz));

}


/*
 * If calling from a thread it is illegal to leave username or dbName NULL.
 */
static void
initPQConnectionBuffer(CdbComponentDatabaseInfo *q, char *username, char *dbName, PQExpBuffer buffer, bool catdml)
{
	char *user = username;

	if (user == NULL)
		user = MyProcPort->user_name;

	initPQExpBuffer(buffer);

	if (dbName == NULL)
		dbName = MyProcPort->database_name;

	/*
	 * Build the connection string
	 */
	if (catdml)
		appendPQExpBuffer(buffer,
						  "options='-c gp_session_role=UTILITY -c allow_system_table_mods=dml' ");

	/*
	 * Use domain sockets on the master since we must do authentication
	 * of this connection.
	 */
	if (q->dbid != MASTER_DBID)
	{
		if (q->hostname == NULL)
			appendPQExpBufferStr(buffer, "host='' ");
		else if (isdigit(q->hostname[0]))
			appendPQExpBuffer(buffer, "hostaddr=%s ", q->hostname);
		else
			appendPQExpBuffer(buffer, "host=%s ", q->hostname);
	}

	appendPQExpBuffer(buffer, "port=%u ", q->port);
	appendPQExpBuffer(buffer, "dbname=%s ", dbName);
	appendPQExpBuffer(buffer, "user=%s ", user);
	appendPQExpBuffer(buffer, "connect_timeout=%d ", gp_segment_connect_timeout);
}

/*
 * Get a superuser name; return in malloc()ed buffer.
 */
static char *
getDBSuperuserName(CdbComponentDatabaseInfo *q, const char *errPrefix)
{
	char *suser = NULL;
	Relation auth_rel;
	HeapTuple	auth_tup;
	HeapScanDesc auth_scan;
	ScanKeyData key[2];
	bool	isNull;

	ScanKeyInit(&key[0],
				Anum_pg_authid_rolsuper,
				BTEqualStrategyNumber, F_BOOLEQ,
                BoolGetDatum(true));

	ScanKeyInit(&key[1],
				Anum_pg_authid_rolcanlogin,
				BTEqualStrategyNumber, F_BOOLEQ,
                BoolGetDatum(true));

	auth_rel = heap_open(AuthIdRelationId, AccessShareLock);
	auth_scan = heap_beginscan(auth_rel, SnapshotNow, 2, key);

	while (HeapTupleIsValid(auth_tup = heap_getnext(auth_scan, ForwardScanDirection)))
	{
		Datum	attrName;
		Datum	validuntil;

		validuntil = heap_getattr(auth_tup, Anum_pg_authid_rolvaliduntil, auth_rel->rd_att, &isNull);
		/* we actually want it to be NULL, that means always valid */
		if (!isNull)
			continue;

		attrName = heap_getattr(auth_tup, Anum_pg_authid_rolname, auth_rel->rd_att, &isNull);

		Assert(!isNull);

		suser = strdup(DatumGetCString(attrName));
	}
	heap_endscan(auth_scan);
	heap_close(auth_rel, AccessShareLock);

	return suser;
}

static void
QDMirroringUpdate(
	QDMIRRORUpdateMask 		updateMask,
	bool					validFlag,
	QDMIRRORState 			state,
	QDMIRRORDisabledReason	disabledReason,
	struct timeval          *lastLogTimeVal,
	char                    *errorMessage)
{
#define UPDATE_VALIDFLAG_CMD "update gp_configuration set valid='%c' where dbid = CAST(%d AS SMALLINT)"
#define UPDATE_MASTER_MIRRORING_CMD "update gp_master_mirroring set (summary_state, detail_state, log_time, error_message) = ('%s', %s, '%s'::timestamptz, %s);"
	int count = 0;
	char cmd[200 + QDMIRRORErrorMessageSize * 2 + 3];
	char detailValue[100];
	char logTimeStr[128];
	char *summaryStateString;
	char *detailStateString;
	char errorMessageQuoted[QDMIRRORErrorMessageSize * 2 + 3];
	char *user;
	CdbComponentDatabaseInfo *q;
	MemoryContext mcxt = CurrentMemoryContext;

	volatile PQExpBuffer entryBuffer = NULL;
	volatile PGconn		*entryConn = NULL;
	volatile PGresult	*rs = NULL;

	PG_TRY();
	{
		StartTransactionCommand();

		MemoryContextSwitchTo(TopMemoryContext);
		q = getEntrySegDB(&count);
		MemoryContextSwitchTo(mcxt);

		if (count > 1)
		{
			if (q->dbid != GpIdentity.dbid)
			{
				q = q + 1;
			}
		}

		user = getDBSuperuserName(q, "QDMirroringUpdate");

		Assert(user != NULL);

		entryBuffer = createPQExpBuffer();
		if (PQExpBufferBroken(entryBuffer))
		{
			destroyPQExpBuffer(entryBuffer);
			ereport(ERROR, (errcode(ERRCODE_OUT_OF_MEMORY),
							errmsg("QDMirroringUpdate: out of memory")));
			/* not reached. */
		}

		/*
		 * initialize libpq connection buffer, we only need to initialize it
		 * once.
		 */
		initPQConnectionBuffer(q, user, NULL, entryBuffer, true);

		free(user);

		/*
		 * Call libpq to connect
		 */
		entryConn = PQconnectdb(entryBuffer->data);

		if (PQstatus((PGconn *)entryConn) == CONNECTION_BAD)
		{
			/*
			 * When we get an error, we strdup it here.  When the main thread
			 * checks for errors, it makes a palloc copy of this, and frees
			 * this.
			 */
			char	   *error_message = strdup(PQerrorMessage((PGconn *)entryConn));
			if (!error_message)
			{
				ereport(ERROR, (errcode(ERRCODE_OUT_OF_MEMORY),
								errmsg("QDMirroringUpdate: out of memory")));
			}

			destroyPQExpBuffer(entryBuffer);
			PQfinish((PGconn *)entryConn);
			entryConn = NULL;
			elog(FATAL, "QDMirroringUpdate: setting segDB state failed, error connecting to entry db, error: %s", error_message);
		}

		/* finally, we're ready to actually get some stuff done. */

		do
		{
			rs = PQexec((PGconn *)entryConn, "BEGIN");
			if (PQresultStatus((PGresult *)rs) != PGRES_COMMAND_OK)
				break;

			if ((updateMask & QDMIRROR_UPDATEMASK_VALIDFLAG) != 0)
			{
				count = snprintf(cmd, sizeof(cmd), UPDATE_VALIDFLAG_CMD, (validFlag ? 't' : 'f'), ftsQDMirrorInfo->dbid);
				if (count >= sizeof(cmd))
				{
					ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR),
									errmsg("QDMirroringUpdate: format command string failure")));
				}

				rs = PQexec((PGconn *)entryConn, cmd);
				if (PQresultStatus((PGresult *)rs) != PGRES_COMMAND_OK)
				{
					ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR),
									errmsg("QDMirroringUpdate: could not execute command '%s'", cmd)));
					break;
				}
			}

			if ((updateMask & QDMIRROR_UPDATEMASK_MASTERMIRRORING)!= 0)
			{
				switch (state)
				{
					case QDMIRROR_STATE_NONE:
						summaryStateString = "None";
						break;

					case QDMIRROR_STATE_NOTCONFIGURED:
						summaryStateString = "Not Configured";
						break;

					case QDMIRROR_STATE_CONNECTINGWALSENDSERVER:
					case QDMIRROR_STATE_POSITIONINGTOEND:
					case QDMIRROR_STATE_CATCHUPPENDING:
					case QDMIRROR_STATE_CATCHINGUP:
						summaryStateString = "Synchronizing";
						break;

					case QDMIRROR_STATE_SYNCHRONIZED:
						summaryStateString = "Synchronized";
						break;

					case QDMIRROR_STATE_DISABLED:
						summaryStateString = "Not Synchronized";
						break;

					default:
						summaryStateString = "Unknown";
						break;
				}

				if (state == QDMIRROR_STATE_DISABLED)
				{
					detailStateString =
						QDMirroringDisabledReasonToString(disabledReason);
				}
				else
				{
					detailStateString = NULL;
				}

				if (detailStateString == NULL)
				{
					strcpy(detailValue, "null");
				}
				else
				{
					count = snprintf(detailValue, sizeof(detailValue), "'%s'", detailStateString);
					if (count >= sizeof(detailValue))
					{
						ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR),
										errmsg("QDMirroringUpdate: format command string failure")));
					}
				}

				QDMirroringFormatTime(logTimeStr, sizeof(logTimeStr), lastLogTimeVal);

				/*
				 * Escape quote the error string before putting in DML statement...
				 */
				if (errorMessage != NULL)
				{
					int errorMessageLen = strlen(errorMessage);

					if (errorMessageLen == 0)
					{
						strcpy(errorMessageQuoted, "null");
					}
					else
					{
						size_t escapedLen;

						errorMessageQuoted[0] = '\'';
						escapedLen = PQescapeString(&errorMessageQuoted[1], errorMessage, errorMessageLen);
						errorMessageQuoted[escapedLen + 1] = '\'';
						errorMessageQuoted[escapedLen + 2] = '\0';

						elog((Debug_print_qd_mirroring ? LOG : DEBUG5), "Error message quoted: \"%s\"", errorMessageQuoted);
					}
				}
				else
				{
					strcpy(errorMessageQuoted, "null");
				}
				count = snprintf(cmd, sizeof(cmd), UPDATE_MASTER_MIRRORING_CMD,
								 summaryStateString, detailValue, logTimeStr, errorMessageQuoted);

				if (count >= sizeof(cmd))
				{
					ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR),
									errmsg("QDMirroringUpdate: format command string failure")));
				}

				rs = PQexec((PGconn *)entryConn, cmd);
				if (PQresultStatus((PGresult *)rs) != PGRES_COMMAND_OK)
				{
					ereport(ERROR,
							(errcode(ERRCODE_INTERNAL_ERROR),
							 errmsg("QDMirroringUpdate: could not execute command '%s'", cmd)));
					break;
				}

				elog((Debug_print_qd_mirroring ? LOG : DEBUG5),
					 "Successfully executed command \"%s\"", cmd);

				rs = PQexec((PGconn *)entryConn, "COMMIT");
				if (PQresultStatus((PGresult *)rs) != PGRES_COMMAND_OK)
					break;
			}
		}
		while (0);

		PQclear((PGresult *)rs);
		PQfinish((PGconn *)entryConn);
		destroyPQExpBuffer(entryBuffer);

		CommitTransactionCommand();
	}
	PG_CATCH();
	{
		PQclear((PGresult *)rs);
		PQfinish((PGconn *)entryConn);
		destroyPQExpBuffer(entryBuffer);

		AbortCurrentTransaction();
	}
	PG_END_TRY();

	MemoryContextSwitchTo(mcxt); /* Just incase we hit an error */
	return;
}

void
CheckForQDMirroringWork(void)
{
	QDMIRRORUpdateMask 		updateMask;
	bool					validFlag;
	QDMIRRORState 			state;
	QDMIRRORDisabledReason	disabledReason;
	struct timeval          lastLogTimeVal;
	char					errorMessage[QDMIRRORErrorMessageSize];

	if (ftsQDMirrorInfo == NULL)
		return;
	/*
	 * Test without taking lock.
	 */
	if (ftsQDMirrorInfo->updateMask == QDMIRROR_UPDATEMASK_NONE)
		return;

	if (IsTransactionOrTransactionBlock())
		return;

	/*
	 * NOTE: We are trying to use the longer-term update config lock here.
	 */
	if (!LWLockConditionalAcquire(ftsQDMirrorUpdateConfigLock, LW_EXCLUSIVE))
		return;

	LWLockAcquire(ftsQDMirrorLock, LW_EXCLUSIVE);

	if (ftsQDMirrorInfo->updateMask == QDMIRROR_UPDATEMASK_NONE)
	{
		LWLockRelease(ftsQDMirrorLock);
		LWLockRelease(ftsQDMirrorUpdateConfigLock);
		return;
	}

	updateMask = ftsQDMirrorInfo->updateMask;
	ftsQDMirrorInfo->updateMask = QDMIRROR_UPDATEMASK_NONE;

	validFlag = false;
	if ((updateMask & QDMIRROR_UPDATEMASK_VALIDFLAG) != 0)
	{
		validFlag = !ftsQDMirrorInfo->valid;

		/*
		 * Assume we are successful...
		 */
		ftsQDMirrorInfo->valid = validFlag;
	}

	state = ftsQDMirrorInfo->state;
	disabledReason = ftsQDMirrorInfo->disabledReason;
	lastLogTimeVal = ftsQDMirrorInfo->lastLogTimeVal;
	strcpy(errorMessage, ftsQDMirrorInfo->errorMessage);

	LWLockRelease(ftsQDMirrorLock);

	QDMirroringUpdate(updateMask, validFlag,
		              state, disabledReason, &lastLogTimeVal, errorMessage);

	LWLockRelease(ftsQDMirrorUpdateConfigLock);
}

bool QDMirroringWriteCheck(void)
{
	bool giveWarning = false;
	QDMIRRORDisabledReason	disabledReason = QDMIRROR_DISABLEDREASON_NONE;
	struct timeval lastLogTimeVal = {0, 0};

	if (ftsQDMirrorInfo == NULL)
		return false;	// Don't know yet.

	LWLockAcquire(ftsQDMirrorLock, LW_EXCLUSIVE);
	if (ftsQDMirrorInfo->state == QDMIRROR_STATE_SYNCHRONIZED)
	{
		LWLockRelease(ftsQDMirrorLock);
		return true;
	}

	if (ftsQDMirrorInfo->QDMirroringNotSynchronizedWarningGiven == false &&
		ftsQDMirrorInfo->state == QDMIRROR_STATE_DISABLED)
	{
		giveWarning = true;
		ftsQDMirrorInfo->QDMirroringNotSynchronizedWarningGiven = true;
		disabledReason = ftsQDMirrorInfo->disabledReason;
		lastLogTimeVal = ftsQDMirrorInfo->lastLogTimeVal;
	}

	LWLockRelease(ftsQDMirrorLock);

	if (giveWarning)
	{
		char logTimeStr[100];

		QDMirroringFormatTime(logTimeStr, sizeof(logTimeStr),
					          &lastLogTimeVal);

		ereport(LOG,
				(errcode(ERRCODE_GP_INTERCONNECTION_ERROR),
				 errmsg("Master mirroring is not synchronized as of %s (%s).  "
				        "The GPDB system is currently not highly-available",
				        logTimeStr,
				        QDMirroringDisabledReasonToString(disabledReason)),
				 errSendAlert(true)));
	}

	return false;
}

void
disableQDMirroring(char *detail, char *errorMessage, QDMIRRORDisabledReason disabledReason)
{
	struct timeval lastLogTimeVal;

	Assert(ftsQDMirrorInfo != NULL);

	LWLockAcquire(ftsQDMirrorLock, LW_EXCLUSIVE);

	/*
	 * Don't overwrite.
	 */
	if (ftsQDMirrorInfo->state != QDMIRROR_STATE_DISABLED)
	{
		ftsQDMirrorInfo->state = QDMIRROR_STATE_DISABLED;
		ftsQDMirrorInfo->disabledReason = disabledReason;

		switch (disabledReason)
		{
			case QDMIRROR_DISABLEDREASON_TOOFARBEHIND:
			case QDMIRROR_DISABLEDREASON_CONNECTIONERROR:
			case QDMIRROR_DISABLEDREASON_WALSENDSERVERERROR:
			case QDMIRROR_DISABLEDREASON_ADMINISTRATORDISABLED:
			case QDMIRROR_DISABLEDREASON_UNEXPECTEDERROR:
				if (disabledReason == QDMIRROR_DISABLEDREASON_ADMINISTRATORDISABLED)
					ereport(NOTICE,
							(errcode(ERRCODE_GP_INTERCONNECTION_ERROR),
							 errmsg("Master mirroring not synchronized because the standby master was not started by the administrator")));
				else
					ereport(WARNING,
							(errcode(ERRCODE_GP_INTERCONNECTION_ERROR),
							 errSendAlert(true),
							 errmsg("Master mirroring synchronization lost"),
							 errdetail("%s\nThe Greenplum Database is no longer highly available.", detail)));

				gettimeofday(&lastLogTimeVal, NULL);

				ftsQDMirrorInfo->QDMirroringNotSynchronizedWarningGiven = true;

				if (ftsQDMirrorInfo->valid)
				{
					/*
					 * Only do the update if we need to.
					 */
					ftsQDMirrorInfo->updateMask |= QDMIRROR_UPDATEMASK_VALIDFLAG;
				}
				ftsQDMirrorInfo->updateMask |= QDMIRROR_UPDATEMASK_MASTERMIRRORING;

				ftsQDMirrorInfo->lastLogTimeVal = lastLogTimeVal;

				/*
				 * Not specified or too long error message string will end up NULL
				 * in gp_master_mirroring table.
				 */
				if (errorMessage == NULL ||
					strlen(errorMessage) + 1 >= sizeof(ftsQDMirrorInfo->errorMessage))
					strcpy(ftsQDMirrorInfo->errorMessage, "");
				else
					strcpy(ftsQDMirrorInfo->errorMessage, errorMessage);
				break;

			case QDMIRROR_DISABLEDREASON_SHUTDOWN:
				break;

			default:
				elog(ERROR, "Unknown disabled reason %d", (int)disabledReason);
		}
	}

	LWLockRelease(ftsQDMirrorLock);
}

// Helps avoid pulling in cdbfts.h in xlog.c
void
disableQDMirroring_WalSendServerError(char *detail)
{
	disableQDMirroring(detail, ServiceGetLastClientErrorString(),
		               QDMIRROR_DISABLEDREASON_WALSENDSERVERERROR);
}

// Helps avoid pulling in cdbfts.h in walsendserver.c
void
disableQDMirroring_TooFarBehind(char *detail)
{
	disableQDMirroring(detail, NULL, QDMIRROR_DISABLEDREASON_TOOFARBEHIND);
}

// Helps avoid pulling in cdbfts.h in walsendserver.c
void
disableQDMirroring_ConnectionError(char *detail, char *errorMessage)
{
	disableQDMirroring(detail, errorMessage, QDMIRROR_DISABLEDREASON_CONNECTIONERROR);
}

// Helps avoid pulling in cdbfts.h in walsendserver.c
void
disableQDMirroring_UnexpectedError(char *detail)
{
	disableQDMirroring(detail, NULL, QDMIRROR_DISABLEDREASON_UNEXPECTEDERROR);
}

// Helps avoid pulling in cdbfts.h in walsendserver.c
void
disableQDMirroring_ShutDown(void)
{
	disableQDMirroring(NULL, NULL, QDMIRROR_DISABLEDREASON_SHUTDOWN);
}

// Helps avoid pulling in cdbfts.h in walsendserver.c
void
disableQDMirroring_AdministratorDisabled(void)
{
	disableQDMirroring(NULL, NULL, QDMIRROR_DISABLEDREASON_ADMINISTRATORDISABLED);
}

void
enableQDMirroring(char *message, char *detail)
{
	Assert(ftsQDMirrorInfo != NULL);

	LWLockAcquire(ftsQDMirrorLock, LW_EXCLUSIVE);

	if (ftsQDMirrorInfo->state != QDMIRROR_STATE_CATCHINGUP)
	{
		/*
		 * We've changed state while unlocked.
		 */
		if (ftsQDMirrorInfo->state == QDMIRROR_STATE_DISABLED)
		{
			LWLockRelease(ftsQDMirrorLock);

			elog((Debug_print_qd_mirroring ? LOG : DEBUG5),
				 "Master mirroring already disabled");
			return;
		}
		else
		{
			QDMIRRORState state = ftsQDMirrorInfo->state;

			LWLockRelease(ftsQDMirrorLock);
			ereport(ERROR,
					(errcode(ERRCODE_INTERNAL_ERROR),
					 errmsg("Unexpected master mirror state '%s'",
						    QDMirroringStateToString(state))));
		}
	}

	ftsQDMirrorInfo->state = QDMIRROR_STATE_SYNCHRONIZED;
	ftsQDMirrorInfo->updateMask |= QDMIRROR_UPDATEMASK_MASTERMIRRORING;
	ereport(NOTICE,
			(errcode(ERRCODE_GP_INTERCONNECTION_ERROR),
			 errmsg("%s", message),
			 errdetail("%s", detail)));
	GetLastLogTimeVal(&ftsQDMirrorInfo->lastLogTimeVal);
	gettimeofday(&ftsQDMirrorInfo->lastLogTimeVal, NULL);


	// Get the log message time
	// from the NOTICE we just issued.

	if (!ftsQDMirrorInfo->valid)
	{
		ftsQDMirrorInfo->updateMask |= QDMIRROR_UPDATEMASK_VALIDFLAG;
	}

	LWLockRelease(ftsQDMirrorLock);
}


bool
isQDMirroringEnabled(void)
{
	bool result;

	if (ftsQDMirrorInfo == NULL)
		return false;	// Don't know yet.

	LWLockAcquire(ftsQDMirrorLock, LW_EXCLUSIVE);
	result = (ftsQDMirrorInfo->state == QDMIRROR_STATE_SYNCHRONIZED);
	LWLockRelease(ftsQDMirrorLock);

	return result;
}

bool
isQDMirroringCatchingUp(void)
{
	bool result;

	if (ftsQDMirrorInfo == NULL)
		return false;	// Don't know yet.

	LWLockAcquire(ftsQDMirrorLock, LW_EXCLUSIVE);
	result = (ftsQDMirrorInfo->state == QDMIRROR_STATE_CATCHINGUP);
	LWLockRelease(ftsQDMirrorLock);

	return result;
}

bool
isQDMirroringNotConfigured(void)
{
	bool result;

	if (ftsQDMirrorInfo == NULL)
		return false;	// Don't know yet.

	LWLockAcquire(ftsQDMirrorLock, LW_EXCLUSIVE);
	result = (ftsQDMirrorInfo->state == QDMIRROR_STATE_NOTCONFIGURED);
	LWLockRelease(ftsQDMirrorLock);

	return result;
}

bool
isQDMirroringNotKnownYet(void)
{
	bool result;

	if (ftsQDMirrorInfo == NULL)
		return true;	// Assume.

	LWLockAcquire(ftsQDMirrorLock, LW_EXCLUSIVE);
	result = (ftsQDMirrorInfo->state == QDMIRROR_STATE_NONE);
	LWLockRelease(ftsQDMirrorLock);

	return result;
}

bool
isQDMirroringDisabled(void)
{
	bool result;

	if (ftsQDMirrorInfo == NULL)
		return false;	// Don't know yet.

	LWLockAcquire(ftsQDMirrorLock, LW_EXCLUSIVE);
	result = (ftsQDMirrorInfo->state == QDMIRROR_STATE_DISABLED);
	LWLockRelease(ftsQDMirrorLock);

	return result;
}

char *
QDMirroringStateToString(QDMIRRORState state)
{
	switch (state)
	{
		case QDMIRROR_STATE_NONE:
			return "None";

		case QDMIRROR_STATE_NOTCONFIGURED:
			return "Not Configured";

		case QDMIRROR_STATE_CONNECTINGWALSENDSERVER:
			return "Connecting to WAL Send server";

		case QDMIRROR_STATE_POSITIONINGTOEND:
			return "Positioning to End";

		case QDMIRROR_STATE_CATCHUPPENDING:
			return "Catchup Pending";

		case QDMIRROR_STATE_CATCHINGUP:
			return "Catching Up";

		case QDMIRROR_STATE_SYNCHRONIZED:
			return "Synchronized";

		case QDMIRROR_STATE_DISABLED:
			return "Disabled";

		default:
			return "unknown";
	}
}

char *
QDMirroringStateString(void)
{
	QDMIRRORState state;

	if (ftsQDMirrorInfo == NULL)
		return "don't know yet";

	LWLockAcquire(ftsQDMirrorLock, LW_EXCLUSIVE);
	state = ftsQDMirrorInfo->state;
	LWLockRelease(ftsQDMirrorLock);

	return QDMirroringStateToString(state);
}

static char *
QDMirroringDisabledReasonToString(QDMIRRORDisabledReason disabledReason)
{
	switch (disabledReason)
	{
		case QDMIRROR_DISABLEDREASON_NONE:
			return "None";

		case QDMIRROR_DISABLEDREASON_CONNECTIONERROR:
			return "Connection error";

		case QDMIRROR_DISABLEDREASON_TOOFARBEHIND:
			return "Standby master too far behind";

		case QDMIRROR_DISABLEDREASON_WALSENDSERVERERROR:
			return "WAL Send server error";

		case QDMIRROR_DISABLEDREASON_UNEXPECTEDERROR:
			return "Unexpected error";

		case QDMIRROR_DISABLEDREASON_SHUTDOWN:
			return "Shutdown";

		case QDMIRROR_DISABLEDREASON_ADMINISTRATORDISABLED:
			return "Standby master was not started by the administrator";

		default:
			return "unknown";
	}
}

char *
QDMirroringDisabledReasonString(void)
{
	QDMIRRORState state;
	QDMIRRORDisabledReason disabledReason = QDMIRROR_DISABLEDREASON_NONE;

	if (ftsQDMirrorInfo == NULL)
		return NULL;

	LWLockAcquire(ftsQDMirrorLock, LW_EXCLUSIVE);
	state = ftsQDMirrorInfo->state;
	if (state == QDMIRROR_STATE_DISABLED)
		disabledReason = ftsQDMirrorInfo->disabledReason;
	LWLockRelease(ftsQDMirrorLock);

	if (state == QDMIRROR_STATE_DISABLED)
		return QDMirroringDisabledReasonToString(disabledReason);
	else
		return NULL;
}

void
FtsHandleNetFailure(SegmentDatabaseDescriptor ** segDB, int numOfFailed)
{
	elog(LOG, "FtsHandleNetFailure: numOfFailed %d", numOfFailed);

	FtsReConfigureMPP(true);

	ereport(ERROR, (errmsg_internal("MPP detected %d segment failures, system is reconnected", numOfFailed),
			errSendAlert(true)));
}

/*
 * FtsHandleGangConnectionFailure is called by createGang during
 * creating connections return true if error need to be thrown
 */
bool
FtsHandleGangConnectionFailure(SegmentDatabaseDescriptor * segdbDesc, int size)
{
	int			i;
	bool		dtx_active;
	bool		reportError = false;
    bool		realFaultFound = false;
	bool		forceRescan=true;

    for (i = 0; i < size; i++)
    {
        if (PQstatus(segdbDesc[i].conn) != CONNECTION_OK)
        {
			CdbComponentDatabaseInfo *segInfo = segdbDesc[i].segment_database_info;

			elog(DEBUG2, "FtsHandleGangConnectionFailure: looking for real fault on segment dbid %d", segInfo->dbid);

			if (!FtsTestConnection(segInfo, forceRescan))
			{
				elog(DEBUG2, "found fault with segment dbid %d", segInfo->dbid);
				realFaultFound = true;

				/* that at least one fault exists is enough, for now */
				break;
			}
			forceRescan = false; /* only force the rescan on the first call. */
        }
    }

    if (!realFaultFound)
	{
		/* If we successfully tested the gang and didn't notice a
		 * failure, our caller must've seen some kind of transient
		 * failure when the gang was originally constructed ...  */
		elog(DEBUG2, "FtsHandleGangConnectionFailure: no real fault found!");
        return false;
	}

	if (!isFTSEnabled())
	{
		return false;
	}

	ereport(LOG, (errmsg_internal("FTS: reconfiguration is in progress")));

	forceRescan = true;
	for (i = 0; i < size; i++)
	{
		CdbComponentDatabaseInfo *segInfo = segdbDesc[i].segment_database_info;

		if (PQstatus(segdbDesc[i].conn) != CONNECTION_OK)
		{
			if (!FtsTestConnection(segInfo, forceRescan))
			{
				ereport(LOG, (errmsg_internal("FTS: found bad segment with dbid %d", segInfo->dbid),
						errSendAlert(true)));
				/* probe process has already marked segment down. */
			}
			forceRescan = false; /* only force rescan on first call. */
		}
	}

	if (gangsExist())
	{
		reportError = true;
		disconnectAndDestroyAllGangs();
	}

	/*
	 * KLUDGE: Do not error out if we are attempting a DTM protocol retry
	 */
	if (DistributedTransactionContext == DTX_CONTEXT_QD_RETRY_PHASE_2)
	{
		return false;
	}

	/* is there a transaction active ? */
	dtx_active = isCurrentDtxActive();

    /* When the error is raised, it will abort the current DTM transaction */
	if (dtx_active)
	{
		elog((Debug_print_full_dtm ? LOG : DEBUG5),
			 "FtsHandleGangConnectionFailure found an active DTM transaction (returning true).");
		return true;
	}

	/*
	 * error out if this sets read only flag, at this stage the read only
	 * transaction checking has passed, so error out, but do not error out if
	 * tm is in recovery
	 */
	if ((*ftsReadOnlyFlag && !isTMInRecovery()) || reportError)
		return true;

	elog((Debug_print_full_dtm ? LOG : DEBUG5),
		 "FtsHandleGangConnectionFailure returning false.");

	return false;
}

void
FtsCondSetTxnReadOnly(bool *XactFlag)
{
	if (!isFTSEnabled())
		return;

	if (*ftsReadOnlyFlag && Gp_role != GP_ROLE_UTILITY)
		*XactFlag = true;
}

bool
verifyFtsSyncCount(void)
{
	if (ftsProbeInfo->fts_probePid == 0)
		return true;

	if (!local_fts_status_initialized)
	{
		local_fts_status_initialized = true;
		local_fts_statusVersion = ftsProbeInfo->fts_statusVersion;

		return true;
	}

	return (local_fts_statusVersion == ftsProbeInfo->fts_statusVersion);
}

bool
isFtsReadOnlySet(void)
{
	return *ftsReadOnlyFlag;
}

bool
isMirrorQDConfigurationChecked(void)
{
	bool result;

	if (ftsQDMirrorInfo == NULL)
		return false;

	LWLockAcquire(ftsQDMirrorLock, LW_EXCLUSIVE);
	result = ftsQDMirrorInfo->configurationChecked;
	LWLockRelease(ftsQDMirrorLock);

	return result;
}

bool
isQDMirroringPendingCatchup(void)
{
	bool result;

	if (ftsQDMirrorInfo == NULL)
		return false;

	LWLockAcquire(ftsQDMirrorLock, LW_EXCLUSIVE);
	result = (ftsQDMirrorInfo->state == QDMIRROR_STATE_CATCHUPPENDING);
	LWLockRelease(ftsQDMirrorLock);

	return result;
}

/*
 * While checking the mirroring configuration, call this procedure to set
 * that we are going to attempt connect and mirror to the standby.
 */
void
FtsSetQDMirroring(struct CdbComponentDatabaseInfo *dbinfo)
{
	int len;
	bool connectedToWalSendServer;
	bool walSendServerOk;

	Assert(dbinfo != NULL);

	LWLockAcquire(ftsQDMirrorLock, LW_EXCLUSIVE);
	if (ftsQDMirrorInfo->configurationChecked)
	{
		// Someone else finished the configuration check work first.
		LWLockRelease(ftsQDMirrorLock);
		return;
	}

	Assert(ftsQDMirrorInfo->state == QDMIRROR_STATE_NONE);

	len = strlen(dbinfo->hostname);
	if (len >= sizeof(ftsQDMirrorInfo->name))
	{
		LWLockRelease(ftsQDMirrorLock);
		elog(ERROR, "hostname too long for fts master mirror configuration storage");
	}
	memcpy(ftsQDMirrorInfo->name, dbinfo->hostname, len);
	ftsQDMirrorInfo->name[len] = 0;
	ftsQDMirrorInfo->port = dbinfo->port;

	ftsQDMirrorInfo->dbid = dbinfo->dbid;

	//ftsQDMirrorInfo->valid = dbinfo->valid;

	/*
	 * Connect to our WAL Send server, but not under lock.
	 *
	 */
	ftsQDMirrorInfo->configurationChecked = true;
	if (Master_mirroring_administrator_disable)
	{
		LWLockRelease(ftsQDMirrorLock);
		disableQDMirroring_AdministratorDisabled();
		return;
	}

	ftsQDMirrorInfo->state = QDMIRROR_STATE_CONNECTINGWALSENDSERVER;
	ftsQDMirrorInfo->updateMask |= QDMIRROR_UPDATEMASK_MASTERMIRRORING;
	elog(NOTICE, "Master mirroring synchronizing");

	/* Get the log message time from the NOTICE we just issued. */

	gettimeofday(&ftsQDMirrorInfo->lastLogTimeVal, NULL);

	LWLockRelease(ftsQDMirrorLock);

	connectedToWalSendServer = WalSendServerClientConnect(/* complain */ true);
	if (!connectedToWalSendServer)
	{
		disableQDMirroring_WalSendServerError("Cannot connect to the WAL Send server");
		return;
	}

	LWLockAcquire(ftsQDMirrorLock, LW_EXCLUSIVE);
	if (ftsQDMirrorInfo->state != QDMIRROR_STATE_CONNECTINGWALSENDSERVER)
	{
		/*
		 * We've changed state while unlocked.
		 */
		if (ftsQDMirrorInfo->state == QDMIRROR_STATE_DISABLED)
		{
			LWLockRelease(ftsQDMirrorLock);

			elog((Debug_print_qd_mirroring ? LOG : DEBUG5),
				 "Master mirroring already disabled (connecting)");
			return;
		}
		else
		{
			QDMIRRORState state = ftsQDMirrorInfo->state;

			LWLockRelease(ftsQDMirrorLock);
			ereport(ERROR, (errmsg_internal("Unexpected master mirroring state '%s'",
				 QDMirroringStateToString(state))));
		}
	}

	ftsQDMirrorInfo->state = QDMIRROR_STATE_POSITIONINGTOEND;
	LWLockRelease(ftsQDMirrorLock);

	walSendServerOk = XLogQDMirrorPositionToEnd();
	if (!walSendServerOk)
	{
		disableQDMirroring_WalSendServerError("Error occurred during the PositionToEnd request to the WAL Send server");
		return;
	}

	LWLockAcquire(ftsQDMirrorLock, LW_EXCLUSIVE);
	if (ftsQDMirrorInfo->state != QDMIRROR_STATE_POSITIONINGTOEND)
	{
		/*
		 * We've changed state while unlocked.
		 */
		if (ftsQDMirrorInfo->state == QDMIRROR_STATE_DISABLED)
		{
			LWLockRelease(ftsQDMirrorLock);

			elog((Debug_print_qd_mirroring ? LOG : DEBUG5),
				 "Master mirroring already disabled (request PositionToEnd -- could not send to WAL Send server)");
			return;
		}
		else
		{
			QDMIRRORState state = ftsQDMirrorInfo->state;

			LWLockRelease(ftsQDMirrorLock);
			elog(ERROR,"Unexpected master mirror state '%s'",
				 QDMirroringStateToString(state));
		}
	}
	ftsQDMirrorInfo->state = QDMIRROR_STATE_CATCHUPPENDING;
	LWLockRelease(ftsQDMirrorLock);
	elog((Debug_print_qd_mirroring ? LOG : DEBUG5),
		 "positioning master mirror to end complete -- next step is catch-up");
}

/*
 * Initiate catchup.  Called by XLogWrite when the write and flushed locations
 * are the same and a lock is held so those locations are momentarily stable.
 *
 * We queue the catchup request to the WAL Send server under the lock so the
 * server will get that request from the fifo queue before new log write
 * requests (since we are enabling mirroring here).
 */
void
FtsQDMirroringCatchup(XLogRecPtr *flushedLocation)
{
	bool walSendServerOk;

	Assert(flushedLocation != NULL);

	LWLockAcquire(ftsQDMirrorLock, LW_EXCLUSIVE);

	if (ftsQDMirrorInfo->state != QDMIRROR_STATE_CATCHUPPENDING)
	{
		LWLockRelease(ftsQDMirrorLock);
		return;
	}

	ftsQDMirrorInfo->state = QDMIRROR_STATE_CATCHINGUP;
	LWLockRelease(ftsQDMirrorLock);

	walSendServerOk = XLogQDMirrorCatchup(flushedLocation);
	if (!walSendServerOk)
	{
		disableQDMirroring_WalSendServerError("Error occurred during the Catchup request to the WAL Send server");
		return;
	}

	LWLockAcquire(ftsQDMirrorLock, LW_EXCLUSIVE);
	if (ftsQDMirrorInfo->state != QDMIRROR_STATE_SYNCHRONIZED)
	{
		/*
		 * We've changed state while unlocked.
		 */
		if (ftsQDMirrorInfo->state == QDMIRROR_STATE_DISABLED)
		{
			LWLockRelease(ftsQDMirrorLock);

			elog((Debug_print_qd_mirroring ? LOG : DEBUG5),
				 "Master mirroring already disabled (request Catchup -- could not send to WAL Send server)");
			return;
		}
		else
		{
			QDMIRRORState state = ftsQDMirrorInfo->state;

			LWLockRelease(ftsQDMirrorLock);
			ereport(ERROR, (errmsg_internal("Unexpected master mirror state '%s'",
				 QDMirroringStateToString(state))));
		}
	}

	LWLockRelease(ftsQDMirrorLock);
	elog((Debug_print_qd_mirroring ? LOG : DEBUG5),
		 "finished catching up master mirror");
}

/*
 * Save a new checkpoint location that the QD mirror can safely reply
 * through and release any XLOG files freed up by reply.
 *
 * The location will be saved and sent to the QD mirror on the WAL send
 * request.
 */
void
FtsQDMirroringNewCheckpointLoc(XLogRecPtr *newCheckpointLocation)
{
	Assert(newCheckpointLocation != NULL);

	LWLockAcquire(ftsQDMirrorLock, LW_EXCLUSIVE);

	if (ftsQDMirrorInfo->state != QDMIRROR_STATE_SYNCHRONIZED)
	{
		LWLockRelease(ftsQDMirrorLock);
		return;
	}

	if (ftsQDMirrorInfo->haveNewCheckpointLocation &&
		XLByteLT(*newCheckpointLocation, ftsQDMirrorInfo->newCheckpointLocation))
	{
		LWLockRelease(ftsQDMirrorLock);
		return;
	}

	ftsQDMirrorInfo->haveNewCheckpointLocation = true;
	ftsQDMirrorInfo->newCheckpointLocation = *newCheckpointLocation;

	LWLockRelease(ftsQDMirrorLock);
}

/*
 * Used to see if sending to the QD mirror is enabled.
 *
 */
void
FtsCheckReadyToSend(bool *ready, bool *haveNewCheckpointLocation, XLogRecPtr *newCheckpointLocation)
{
	Assert(ready != NULL);
	Assert(haveNewCheckpointLocation != NULL);
	Assert(newCheckpointLocation != NULL);

	*haveNewCheckpointLocation = false;
	memset(newCheckpointLocation, 0, sizeof(XLogRecPtr));

	LWLockAcquire(ftsQDMirrorLock, LW_EXCLUSIVE);

	if (ftsQDMirrorInfo->state != QDMIRROR_STATE_SYNCHRONIZED)
	{
		LWLockRelease(ftsQDMirrorLock);

		*ready = false;
		return;
	}

	if (ftsQDMirrorInfo->haveNewCheckpointLocation)
	{
		/*
		 * Pass new checkpoint back and forget.
		 */
		*haveNewCheckpointLocation = true;
		memcpy(newCheckpointLocation, &ftsQDMirrorInfo->newCheckpointLocation, sizeof(XLogRecPtr));

		ftsQDMirrorInfo->haveNewCheckpointLocation = false;
	}

	LWLockRelease(ftsQDMirrorLock);

	*ready = true;
}

/*
 * While checking the mirroring configuration, call this procedure to set
 * that we are NOT going do mirroring.
 */
void
FtsSetNoQDMirroring(void)
{
	LWLockAcquire(ftsQDMirrorLock, LW_EXCLUSIVE);
	if (ftsQDMirrorInfo->configurationChecked)
	{
		// Someone else finished the configuration check work first.
		LWLockRelease(ftsQDMirrorLock);
		return;
	}

	Assert(ftsQDMirrorInfo->state == QDMIRROR_STATE_NONE);

	ftsQDMirrorInfo->configurationChecked = true;
	ftsQDMirrorInfo->state = QDMIRROR_STATE_NOTCONFIGURED;

	LWLockRelease(ftsQDMirrorLock);
}

void
FtsDumpQDMirrorInfo(void)
{
	if (!isQDMirroringEnabled() ||
		ftsQDMirrorInfo->name[0] == 0 || ftsQDMirrorInfo->port == 0)
	{
		elog(LOG, "Master mirroring not enabled");
		return;
	}

	elog(LOG, "Master mirror active name (%s:%d), port %d",
		 ftsQDMirrorInfo->name,
		 (int)strlen(ftsQDMirrorInfo->name),
		 ftsQDMirrorInfo->port);
	return;
}

void
FtsGetQDMirrorInfo(char **hostname, uint16 *port)
{
	Assert(hostname != NULL);
	Assert(port != NULL);

	if (ftsQDMirrorInfo->name[0] == 0 || ftsQDMirrorInfo->port == 0)
	{
		*hostname = NULL;
		*port = 0;
		return;
	}

	*hostname = ftsQDMirrorInfo->name;
	*port = ftsQDMirrorInfo->port;
	return;
}

static int
cdbComponentDatabasesSorter(const void *a, const void *b)
{
	CdbComponentDatabaseInfo	*cdi1 = (CdbComponentDatabaseInfo *) a;
	CdbComponentDatabaseInfo	*cdi2 = (CdbComponentDatabaseInfo *) b;

	if (cdi1->segindex == cdi2->segindex)
		return 0;
	if (cdi1->segindex > cdi2->segindex)
		return 1;
	return -1;
}

/*
 * This function returns the segments information. If there are some failed
 * segments, this function will auto failover failed segments to alive segments.
 */
static AliveSegmentsInfo *
GetSegmentsInfo(Bitmapset *last_alive_segment_bms)
{
	AliveSegmentsInfo *info = NULL;
	int			i;
	int			current_id;
	int			alive_id;
	Bitmapset	*alive_bms = NULL;
	CdbComponentDatabases *databases = NULL;
	int			debug_log_level = DEBUG2;

	info = (AliveSegmentsInfo *) palloc0(sizeof(AliveSegmentsInfo));
	databases = getCdbComponentInfo(true);

	/* We will not leave the dead segment in its place. */
	for (current_id = 0, alive_id = 0;
		current_id < databases->total_segment_dbs;
		current_id++)
	{
		CdbComponentDatabaseInfo *p = &databases->segment_db_info[current_id];

		/* Fault injection for fake failed segment. */
		if (GpAliveSegmentsInfo.failed_segmentid_number > 0 &&
			GpAliveSegmentsInfo.failed_segmentid_start <= p->segindex &&
			p->segindex < GpAliveSegmentsInfo.failed_segmentid_start + GpAliveSegmentsInfo.failed_segmentid_number)
		{
			elog(debug_log_level, "fake segindex: %d status down actual status %s", p->segindex, p->status == 'u' ? "up" : "down");
			p->status = 'd';
		}

		if (p->status == 'u')
		{
			alive_bms = bms_add_member(alive_bms, p->segindex);
			/* Move the alive to the correct position. */
			if (current_id != alive_id)
				databases->segment_db_info[alive_id] = *p;
			alive_id++;
			continue;
		}

		/* free the failed segment. */
		freeCdbComponentDatabaseInfo(p);
	}

	/* Number of segments will change in the failover code. */
	databases->total_segment_dbs = alive_id;
	databases->total_segments = alive_id;
	info->aliveSegmentsCount = alive_id;
	info->aliveSegmentsBitmap = alive_bms;
	info->cdbComponentDatabases = databases;

	if (GetTotalSegmentsNumber() != info->aliveSegmentsCount)
		elog(debug_log_level, "total segment: %d alive segment: %d", GetTotalSegmentsNumber(), info->aliveSegmentsCount);

	if (info->aliveSegmentsCount == 0)
	{
		/* No alive segment, let the caller decide whether error out or not. */
		return info;
	}

	if (bms_equal(last_alive_segment_bms, alive_bms))
	{
		/* It looks like nothing happened. */
		bms_free(alive_bms);
		while (alive_id--)
			freeCdbComponentDatabaseInfo(&databases->segment_db_info[current_id]);
		pfree(databases);
		return NULL;
	}

	/*
	 * There are some failover segments, random failover the failed segments
	 * to alive segments.
	 */
	for (i = 0; i < GetTotalSegmentsNumber(); i++)
	{
		int		failover_seg_pos;
		int		failover_segindex;

		if (bms_is_member(i, alive_bms))
			continue;

		failover_seg_pos = random() % info->aliveSegmentsCount;
		failover_segindex = databases->segment_db_info[failover_seg_pos].segindex;
		elog(LOG, "random failover for failed segindex %d to alive segindex %d", i, failover_segindex);
		databases->segment_db_info[alive_id] = databases->segment_db_info[failover_seg_pos];
		databases->segment_db_info[alive_id].segindex = i;
		databases->segment_db_info[alive_id].dbid = contentid_get_dbid(i, 'p', true);
		alive_id++;
	}

	Insist(alive_id == GetTotalSegmentsNumber());

	info->cdbComponentDatabases->total_segment_dbs = alive_id;
	info->cdbComponentDatabases->total_segments = alive_id;
	info->aliveSegmentsCount = alive_id;

	/* There are lots of codes assume that this array is ordered by segindex. */
	qsort(databases->segment_db_info,
		  info->aliveSegmentsCount,
		  sizeof(CdbComponentDatabaseInfo),
		  cdbComponentDatabasesSorter);

	return info;
}

/*
 * UpdateGpAliveSegmentsInfo
 *	Update the UpdateGpAliveSegmentsInfo if the fts changes. if 'force' is true,
 *	Update it forcely.
 */
static void
UpdateGpAliveSegmentsInfo(bool force)
{
	MemoryContext		old;
	AliveSegmentsInfo	*info = NULL;
	int			i;

	old = MemoryContextSwitchTo(TopMemoryContext);
	info = GetSegmentsInfo(force ? NULL : GpAliveSegmentsInfo.aliveSegmentsBitmap);
	MemoryContextSwitchTo(old);

	/* Nothing happened. */
	if (info == NULL)
		return;

	GpAliveSegmentsInfo.fts_statusVersion = 0;
	GpAliveSegmentsInfo.tid = GetCurrentTransactionId();
	/* GpAliveSegmentsInfo.cleanGangs */
	GpAliveSegmentsInfo.forceUpdate = false;
	/* GpAliveSegmentsInfo.failed_segmentid_start */
	/* GpAliveSegmentsInfo.failed_segmentid_number */
	GpAliveSegmentsInfo.aliveSegmentsCount = info->aliveSegmentsCount;
	GpAliveSegmentsInfo.aliveSegmentsBitmap = info->aliveSegmentsBitmap;
	GpAliveSegmentsInfo.cdbComponentDatabases = info->cdbComponentDatabases;
	GpAliveSegmentsInfo.singleton_segindex = UNINITIALIZED_GP_IDENTITY_VALUE;
	/* Find the singleton. */
	for (i = 0; i < GpAliveSegmentsInfo.aliveSegmentsCount; i++)
		if (GpAliveSegmentsInfo.singleton_segindex == UNINITIALIZED_GP_IDENTITY_VALUE ||
			GpAliveSegmentsInfo.singleton_segindex > GpAliveSegmentsInfo.cdbComponentDatabases->segment_db_info[i].segindex)
			GpAliveSegmentsInfo.singleton_segindex = GpAliveSegmentsInfo.cdbComponentDatabases->segment_db_info[i].segindex;

	pfree(info);

	if (GpAliveSegmentsInfo.aliveSegmentsCount == 0)
		elog(ERROR, "No alive segment in the GPSQL.");

	/*
	 * The alive segments information has been changed. Try to failover to a new
	 * session. Segment failed or recoveryed.
	 */
	if (!force)
	{
		GpAliveSegmentsInfo.cleanGangs = true;
		elog(ERROR, "segment configuration changed, reset session");
	}
}


/*
 * UpdateAliveSegmentsInfo
 *	Update the alive segment information from ftsprobe.
 *
 *	The debug & test GUCs are tricky.
 *	  Start Transaction
 *	  UpdateAliveSegmentsInfo()	<- init
 *	  CreatePortal
 *	  AllocateGangs()			<- allocate writer gangs to portal
 *	  Change GUCs
 *	  Trigger ERROR
 *	  DropPortal				<- return gangs to free list
 *	  ReleaseGangs				<- cleanGangs, and free the temp namespace
 *	  End Transaction
 *	  Start Transaction
 *	  UpdateAliveSegmentsInfo() <- forceUpdate
 */
void
UpdateAliveSegmentsInfo(void)
{
	bool	init = GpAliveSegmentsInfo.cdbComponentDatabases == NULL;

	if (!init && GpAliveSegmentsInfo.forceUpdate)
	{
		UpdateGpAliveSegmentsInfo(true);
		return;
	}

	ftsLock();
	if (!FtsIsActive())
	{
		/* Make sure we have a initial state of GpAliveSegmentsInfo. */
		if (init)
		{
			UpdateGpAliveSegmentsInfo(true);
		}
		ftsUnlock();
		return;
	}
	ftsUnlock();

	GpAliveSegmentsInfo.forceUpdate = false;
	/*
	 * CANNOT change the segment information in the same transaction. There might
	 * be some gangs allocated to a specific session. They can be released iff
	 * not in the transaction.
	 */
	if (GpAliveSegmentsInfo.tid == GetCurrentTransactionId())
		return;

	/* TODO: fts version... not support */
	if (!init)
		elog(LOG, "segment reconfiguraion happened.");

	/* If it is the first time update alive information. Don't trigger error! */
	UpdateGpAliveSegmentsInfo(init);
}

