/*-------------------------------------------------------------------------
 *
 * cdblink.c
 *
 *	  Setting up libpq connections to qRxecs from Dispatcher
 *
 * Copyright (c) 2005-2008, Greenplum inc
 *
 *-------------------------------------------------------------------------
 */
#define PROVIDE_64BIT_CRC
#include "postgres.h"

#include "gp-libpq-fe.h"
#include "gp-libpq-int.h"
#include "pqexpbuffer.h"
#include "fmgr.h"
#include "funcapi.h"
#include "miscadmin.h"
#include "access/heapam.h"
#include "access/tupdesc.h"
#include "catalog/pg_type.h"
#include "lib/stringinfo.h"
#include "libpq/libpq-be.h"
#include "storage/ipc.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "nodes/makefuncs.h"
#include "access/xlog.h"

#include "cdb/cdbcat.h"
#include "cdb/cdbconn.h"
#include "cdb/cdblink.h"
#include "cdb/cdbvars.h"
#include "cdb/cdbdisp.h"
#include "cdb/cdbfts.h"


/*
 * Points to the SegmentDatabaseDescriptor of mirrored entry database
 */
static SegmentDatabaseDescriptor *cdb_mirrored_entry_db = NULL;

static bool cdblink_connectQD(SegmentDatabaseDescriptor * segdbDesc);

/*
 * Called by backend/utils/init/postinit.c to setup stuff
 *
 * Note -- this function is called with TopTransactionContext *before*
 *		MessageContext and the other statement-oriented contexts are
 *		allocated.	We want to store the SegmentDbArray and the
 *		heap returned by getCdbSegmentInstances() in TopMemoryContext
 *		(basically permanent storage).	TopTransactionContext is
 *		reset shortly after this routine is called.
 */

void
cdblink_setup(void)
{
	/*
	 * If gp_role is UTILITY, skip this call.
	 */
	if (Gp_role == GP_ROLE_UTILITY)
		return;

	return;
}

/*
 * Returns the total number of segments
 */
int
GetTotalSegmentsNumber(void)
{
	return GpIdentity.numsegments;
}

/*
 * Returns the number of segments
 *
 * N.B.  Gp_role must be either dispatch or execute, since
 * when utiliy	no GP catalog tables are read.  An Assert is
 * thrown if Gp_role = utility.
 */
int
getgpsegmentCount(void)
{
	if (Gp_role == GP_ROLE_UTILITY)
	{
		if (GpIdentity.numsegments <= 0)
		{
            elog(DEBUG5, "getgpsegmentCount called when Gp_role == utility. returning zero segments.");
            return 0;
	    }

		elog(DEBUG1, "getgpsegmentCount called when Gp_role == utility, but is relying on gp_id info");

	}

	verifyGpIdentityIsSet();

	/* Return the alive segment information if initialized. */
	if (GpAliveSegmentsInfo.aliveSegmentsCount != UNINITIALIZED_GP_IDENTITY_VALUE)
	{
		if (GpAliveSegmentsInfo.aliveSegmentsCount == 0)
			elog(ERROR, "No alive segment in the cluser.");
		return GpAliveSegmentsInfo.aliveSegmentsCount;
	}

	Assert(GpIdentity.numsegments > 0);
	return GpIdentity.numsegments;
}

/*
 * Now we connect to mirrored entry db.
 */
void
buildMirrorQDDefinition(void)
{
	CdbComponentDatabases *cdb_component_dbs;
	CdbComponentDatabaseInfo *entryDb;
	CdbComponentDatabaseInfo *mirrorDb;

	if (isMirrorQDConfigurationChecked())
		return;

	cdb_component_dbs = getComponentDatabases();

	if (cdb_component_dbs == NULL)
	{
		FtsSetNoQDMirroring();
		return;
	}
	
	if (cdb_component_dbs->total_entry_dbs <= 1)
	{
		elog(LOG, "no master mirroring standby configuration found");
		FtsSetNoQDMirroring();
		return;
	}

	/* it should have maximum 2 dbs now */
	entryDb = cdb_component_dbs->entry_db_info;

	/*
	 * If the first entry is the primary, then the second is the mirror
	 * if the first entry is the mirror, then the second is the primary.
	 *
	 * So we are assuming that we got back exactly two entry databases.
	 */
	if (entryDb->dbid == GpIdentity.dbid)
		mirrorDb = entryDb + 1;
	else
	{
		mirrorDb = entryDb;
		entryDb = entryDb + 1;
	}

	/* The management scripts "activate" the warm-QD by doing an
	 * update which points the QD-dbid at the warm-QD host. The
	 * currently active QD always has the same dbid, so the only
	 * way we can really *tell* that we're running in the
	 * fail-over mode, is if the two hostnames are the same
	 *
	 * Someday the "isprimary" fields will be updated as well.
	 */

	
	if (SEGMENT_IS_ACTIVE_PRIMARY(mirrorDb) || 
		(strcmp(mirrorDb->hostname, entryDb->hostname) == 0 && !Debug_permit_same_host_standby))
	{
		/* We are running as the mirror! When running as the
		 * mirror, we don't want to connect to ourselves! So we
		 * turn off mirroring after issuing a 'NOTICE' message.
		 */			
		elog(LOG, "Master mirror active, don't connect to self.");
		FtsSetNoQDMirroring();
		return;
	}

	/* we are the primary connecting to the mirror */
	FtsSetQDMirroring(mirrorDb);

}

static bool HaveStandbyMasterEndLocation = false;
static uint32 StandbyMaster_xlogid = 0;
static uint32 StandbyMaster_xrecoff = 0;

/*
 * The bg-writer process needs to be able to build its communications
 * channel with the mirror-QD (if such exists), but has only the
 * cached values in shared memory.
 */
static void
buildQDMirrorFromShmem(void)
{
	uint16 port;
	CdbComponentDatabaseInfo mirrorDb;

	Assert(cdb_mirrored_entry_db == NULL);

	FtsGetQDMirrorInfo(&mirrorDb.hostname, &port);
	if (mirrorDb.hostname == NULL)
	{
		elog(ERROR, "no hostname for mirror");
	}

	mirrorDb.port = port;

	cdb_mirrored_entry_db = malloc(sizeof(SegmentDatabaseDescriptor));
	if (cdb_mirrored_entry_db == NULL)
		elog(ERROR, "could not allocate segment descriptor");
		
	memset(cdb_mirrored_entry_db, 0, sizeof(SegmentDatabaseDescriptor));
	cdbconn_initSegmentDescriptor(cdb_mirrored_entry_db, &mirrorDb);

	/* connect to mirrored QD */
	if (!cdblink_connectQD(cdb_mirrored_entry_db))
	{
		elog(ERROR, "Master mirroring connect failed for entry db. %s",
			 cdb_mirrored_entry_db->error_message.data);

		cdb_mirrored_entry_db->errcode = 0;
		resetPQExpBuffer(&cdb_mirrored_entry_db->error_message);

	}
}

bool
write_position_to_end(XLogRecPtr *endLocation, struct timeval *timeout, bool *shutdownGlobal)
{
	bool successful;
	
	HaveStandbyMasterEndLocation = false;
	StandbyMaster_xlogid = 0;
	StandbyMaster_xrecoff = 0;
	successful = write_qd_sync("position_to_end", NULL, 0, 
							   timeout, shutdownGlobal);
	if (!successful)
		return false;

	if (!HaveStandbyMasterEndLocation)
	{
		elog(ERROR,"did not get standby master end location information");
	}
	
	elog((Debug_print_qd_mirroring ? LOG : DEBUG1),"standby master returned end location: %X/%X",
		 StandbyMaster_xlogid, StandbyMaster_xrecoff);

	endLocation->xlogid = StandbyMaster_xlogid;
	endLocation->xrecoff = StandbyMaster_xrecoff;

	return true;
}

#define MAX_STANDBY_ERROR_STRING 200
static char StandbyErrorString[MAX_STANDBY_ERROR_STRING] = "";

char *
GetStandbyErrorString(void)
{
	return StandbyErrorString;
}

bool
write_qd_sync(char *cmd, void *buf, int len, struct timeval *timeout, bool *shutdownGlobal)
{
	PGconn	   *conn;
	bool		failed = false;
	char       *msg;
	int			sock;
	mpp_fd_set	rset;
	int			n;
	char        *message;
	bool		result = false;

	PG_TRY();
	{
		if (cdb_mirrored_entry_db == NULL)
		{
			buildQDMirrorFromShmem();
			/* did we successfully rebuild from shmem cache ? */
			if (cdb_mirrored_entry_db == NULL)
			{
				elog(ERROR, "No master mirror");
			}
			/* we were able to rebuild the mirror entry from our shared-memory cache */
		}

		conn = cdb_mirrored_entry_db->conn;

		if (PQstatus(conn) != CONNECTION_OK)
		{
			elog(ERROR, "Master mirror connection failed");
		}

		if (PQsendQuery(conn, cmd) == 0)
		{
			failed = true;
		}
		else if (len > 0)
		{
			/*
			 * UNDONE: This part can end up hung in this call stack:
			 *    poll
			 *    pqSocketPoll
			 *    pqSocketCheck
			 *    pqWaitTimed
			 *    pqWait
			 *    pqSendSome
			 *    pqPutMsgEnd
			 *    ...
			 *
			 * Which kind of defeats the purpose of asynchronous!?!?
			 */
			if (pqPutMsgStart(0, false, conn) < 0 ||
				pqPutnchar(buf, len, conn) < 0 ||
				pqPutMsgEnd(conn) < 0)
				failed = true;
			else
				pqFlush(conn);
		}

		if (!failed)
		{
			PGresult   *pRes;
		
			/*
			 * If we are doing a timed send, then use a timed select to wait
			 * for the connection to get a result.
			 */
			if (timeout != NULL)
			{
				/* Add socket to fd_set if still connected. */
				sock = PQsocket(conn);
				if (sock >= 0 &&
					PQstatus(conn) != CONNECTION_BAD)
				{
					struct timeval rundownTimeout = {0,0};
						// Use local variable since select modifies
						// the timeout parameter with remaining time.

					if (timeout != NULL)
						rundownTimeout = *timeout;
					
					MPP_FD_ZERO(&rset);
					MPP_FD_SET(sock, &rset);
					while (true)
					{
						CHECK_FOR_INTERRUPTS();
						if (shutdownGlobal != NULL && *shutdownGlobal)
							ereport(ERROR,
									(errcode(ERRCODE_GP_INTERCONNECTION_ERROR),
									 errmsg("Shutdown detected during send to standby master")));
						
						n = select(sock + 1, (fd_set *)&rset, NULL, NULL, (timeout == NULL ? NULL : &rundownTimeout));
						if (n == 0)
						{
							Assert(timeout != NULL);
							ereport(ERROR,
									(errcode(ERRCODE_GP_INTERCONNECTION_ERROR),
									 errSendAlert(true),
									 errmsg("Send to standby master timed out after %d.%03d seconds",
									        (int)timeout->tv_sec,
									        (int)timeout->tv_usec / 1000)));
						}
						else if (n < 0 && errno == EINTR)
						{
							continue;
						}
						
						if (MPP_FD_ISSET(sock, &rset))
							break;
					}
				}
			}
			
			/* get response */
			pRes = PQgetResult(conn);

			if (pRes != NULL)
			{
				ExecStatusType resultStatus = PQresultStatus(pRes);

				if (PGRES_COMMAND_OK != resultStatus &&
					PGRES_TUPLES_OK != resultStatus)
				{
					failed = true;
				}
				/* cleanup connection */
				do
				{
					if (pRes->Standby_HaveInfo)
					{
						elog(DEBUG5,"write_qd_sync found standby master info: %X/%X",
							 pRes->Standby_xlogid, pRes->Standby_xrecoff);
						HaveStandbyMasterEndLocation = true;
						StandbyMaster_xlogid = pRes->Standby_xlogid;
						StandbyMaster_xrecoff = pRes->Standby_xrecoff;
					}
					PQclear(pRes);
				}
				while ((pRes = PQgetResult(conn)) != NULL);

			}

			if (PQstatus(conn) == CONNECTION_BAD)
				failed = true;
		}

		if (failed)
		{
			msg = pstrdup(PQerrorMessage(conn));
			PQfinish(conn);
			cdb_mirrored_entry_db->conn = NULL;
			ereport(ERROR,
					(errcode(ERRCODE_GP_INTERCONNECTION_ERROR),
					 errSendAlert(true),
					 errmsg("error received sending data to standby master: %s", msg),
					 errdetail("The Greenplum Database is no longer highly available")));
		}
		else
		{
			elog((Debug_print_qd_mirroring ? LOG : DEBUG1), "successfully executed the cmd %s for length %d",
				 cmd, len);
		}
		result = true;
	}
	PG_CATCH();
	{
		/* Report the error to the server log */
	    if (!elog_demote(WARNING))
    	{
    		elog(LOG,"unable to demote error");
        	PG_RE_THROW();
    	}

		message = elog_message();
 		if (message != NULL)
		{
			/*
			 * strncpy doesn't behave well, so let's use snprintf instead.
			 */
			snprintf(StandbyErrorString, MAX_STANDBY_ERROR_STRING, "%s", message);
		}
		else
			strcpy(StandbyErrorString, "");
		
		EmitErrorReport();
		FlushErrorState();
	
		result = false;
	}
	PG_END_TRY();

	return result;
}

/* Connect to a QD-mirror as a client via libpq. */
static bool						/* returns true if connected */
cdblink_connectQD(SegmentDatabaseDescriptor * segdbDesc)
{
	CdbComponentDatabaseInfo *q = segdbDesc->segment_database_info;
	PQExpBufferData buffer;

	/*
	 * We use PQExpBufferData instead of StringInfoData
	 * because the former uses malloc, the latter palloc.
	 * We are in a thread, and we CANNOT use palloc since it's not
	 * thread safe.  We cannot call elog or ereport either for the
	 * same reason.
	 */
	initPQExpBuffer(&buffer);

	/*
	 * Build the connection string
	 */
	if (q->hostname == NULL)
		appendPQExpBufferStr(&buffer, "host='' ");
	else if (isdigit(q->hostname[0]))
		appendPQExpBuffer(&buffer, "hostaddr=%s ", q->hostname);
	else
		appendPQExpBuffer(&buffer, "host=%s ", q->hostname);

	appendPQExpBuffer(&buffer, "port=%u ", q->port);

	/*
	 * Call libpq to connect
	 */
	segdbDesc->conn = PQconnectdb(buffer.data);

	/*
	 * Check for connection failure.
	 */
	if (PQstatus(segdbDesc->conn) == CONNECTION_BAD)
	{
		if (!segdbDesc->errcode)
			segdbDesc->errcode = ERRCODE_GP_INTERCONNECTION_ERROR;
		appendPQExpBuffer(&segdbDesc->error_message,
						  "Master unable to connect to %s with options %s: %s\n",
						  q->hostname,
						  buffer.data,
						  PQerrorMessage(segdbDesc->conn));

		elog(LOG, "%s", segdbDesc->error_message.data);

		PQfinish(segdbDesc->conn);
		segdbDesc->conn = NULL;
	}
	/*
	 * Successfully connected.
	 */
	else
	{
		elog((Debug_print_qd_mirroring ? LOG : DEBUG1), "Connected to mirror-master %s with options %s\n",
			 q->hostname,
			 buffer.data);
	}

	free(buffer.data);
	return segdbDesc->conn != NULL;
}	/* cdblink_doConnect */

void *
getEntrySegDB(int *count)
{
	static CdbComponentDatabases *cdb_component_dbs = NULL;

	if (cdb_component_dbs == NULL)
		cdb_component_dbs = getCdbComponentDatabases();
	*count = cdb_component_dbs->total_entry_dbs;
	return cdb_component_dbs->entry_db_info;
}

bool
disconnectMirrorQD_SendClose(void)
{
	struct timeval closeTimeout = {0,250000};	// UNDONE: Better timeout?
	
	if (cdb_mirrored_entry_db == NULL || 
		PQstatus(cdb_mirrored_entry_db->conn) != CONNECTION_OK)
		return false;
	
	write_qd_sync("close", NULL, 0, &closeTimeout, NULL);
	
	if (PQstatus(cdb_mirrored_entry_db->conn) != CONNECTION_OK)
		return false;
	
	PQfinish(cdb_mirrored_entry_db->conn);
	cdb_mirrored_entry_db->conn = NULL;
	
	return true;
}

bool
disconnectMirrorQD_Abrupt(void)
{
	if (cdb_mirrored_entry_db == NULL || 
		PQstatus(cdb_mirrored_entry_db->conn) != CONNECTION_OK)
		return false;
	
	PQfinish(cdb_mirrored_entry_db->conn);
	cdb_mirrored_entry_db->conn = NULL;

	return true;
}

