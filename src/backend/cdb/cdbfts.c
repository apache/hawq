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
 * cdbfts.c
 *	  Provides fault tolerance service routines for mpp.
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
initPQConnectionBuffer(Segment *master, char *username, char *dbName, PQExpBuffer buffer, bool catdml)
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
	appendPQExpBuffer(buffer, "port=%u ", master->port);
	appendPQExpBuffer(buffer, "dbname=%s ", dbName);
	appendPQExpBuffer(buffer, "user=%s ", user);
	appendPQExpBuffer(buffer, "connect_timeout=%d ", gp_segment_connect_timeout);
}

/*
 * Get a superuser name; return in malloc()ed buffer.
 */
static char *
getDBSuperuserName(const char *errPrefix)
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
	MemoryContext mcxt = CurrentMemoryContext;
	Segment *master = NULL;

	volatile PQExpBuffer entryBuffer = NULL;
	volatile PGconn		*entryConn = NULL;
	volatile PGresult	*rs = NULL;

	PG_TRY();
	{
		StartTransactionCommand();
		user = getDBSuperuserName("QDMirroringUpdate");
		Assert(user != NULL);

		master = GetMasterSegment();
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
		initPQConnectionBuffer(master, user, NULL, entryBuffer, true);
		FreeSegment(master);

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

	ereport(ERROR, (errmsg_internal("MPP detected %d segment failures, system is reconnected", numOfFailed),
			errSendAlert(true)));
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
FtsSetQDMirroring(Segment *seginfo)
{
	int len;
	bool connectedToWalSendServer;
	bool walSendServerOk;

	LWLockAcquire(ftsQDMirrorLock, LW_EXCLUSIVE);
	if (ftsQDMirrorInfo->configurationChecked)
	{
		// Someone else finished the configuration check work first.
		LWLockRelease(ftsQDMirrorLock);
		return;
	}

	Assert(ftsQDMirrorInfo->state == QDMIRROR_STATE_NONE);

	len = strlen(seginfo->hostname);
	if (len >= sizeof(ftsQDMirrorInfo->name))
	{
		LWLockRelease(ftsQDMirrorLock);
		elog(ERROR, "hostname too long for fts master mirror configuration storage");
	}
	memcpy(ftsQDMirrorInfo->name, seginfo->hostname, len);
	ftsQDMirrorInfo->name[len] = 0;
	ftsQDMirrorInfo->port = seginfo->port;

	//ftsQDMirrorInfo->dbid = seginfo->dbid;

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
	//elog(NOTICE, "Master mirroring synchronizing");

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
