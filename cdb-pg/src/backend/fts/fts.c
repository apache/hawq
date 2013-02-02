/*-------------------------------------------------------------------------
 *
 * fts.c
 *	  Process under QD postmaster polls the segments on a periodic basis
 *    or at the behest of QEs.
 *
 * Maintains an array in shared memory containing the state of each segment.
 *
 * Copyright (c) 2005-2010, Greenplum Inc.
 * Copyright (c) 2011, EMC Corp.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <unistd.h>

#include "access/genam.h"
#include "access/catquery.h"
#include "access/heapam.h"
#include "libpq/pqsignal.h"
#include "miscadmin.h"
#include "gp-libpq-fe.h"
#include "gp-libpq-int.h"
#include "utils/memutils.h"
#include "cdb/cdbvars.h"
#include "cdb/cdbfts.h"
#include "postmaster/fork_process.h"
#include "postmaster/postmaster.h"
#include "postmaster/fts.h"
#include "storage/ipc.h"
#include "storage/proc.h"
#include "storage/pmsignal.h"			/* PostmasterIsAlive */
#include "storage/sinval.h"
#include "utils/fmgroids.h"
#include "utils/ps_status.h"

#include "utils/builtins.h"

#include "utils/relcache.h"
#include "utils/syscache.h"

#include "catalog/pg_authid.h"
#include "catalog/pg_database.h"
#include "catalog/pg_tablespace.h"
#include "catalog/catalog.h"

#include "catalog/gp_san_config.h"
#include "catalog/gp_segment_config.h"

#include "storage/backendid.h"

#include "executor/spi.h"

#include "tcop/tcopprot.h" /* quickdie() */


/*
 * CONSTANTS
 */

/* maximum number of segments */
#define MAX_NUM_OF_SEGMENTS  32768

/* buffer size for timestamp */
#define TIMESTAMP_BUF_SIZE   128

/* buffer size for SQL command */
#define SQL_CMD_BUF_SIZE     1024

#define GpConfigHistoryRelName    "gp_configuration_history"


/*
 * STATIC VARIABLES
 */

/* one byte of status for each segment */
static uint8 scan_status[MAX_NUM_OF_SEGMENTS];

static bool am_ftsprobe = false;

static volatile bool shutdown_requested = false;
static volatile bool rescan_requested = false;

static char *probeUser = NULL;
static char *probeDatabase = "template0";

static char failover_strategy='n';

/* struct holding segment configuration */
static CdbComponentDatabases *cdb_component_dbs = NULL;


/*
 * FUNCTION PROTOTYPES
 */

#ifdef EXEC_BACKEND
static pid_t ftsprobe_forkexec(void);
#endif
NON_EXEC_STATIC void ftsMain(int argc, char *argv[]);
static void FtsLoop(void);

static void retrieveUserAndDb(char **probeUser);
extern bool FindMyDatabase(const char *name, Oid *db_id, Oid *db_tablespace);

static void readCdbComponentInfoAndUpdateStatus(void);
static bool probePublishUpdate(uint8 *scan_status);

static uint32 getTransition(bool isPrimaryAlive, bool isMirrorAlive);

static void
buildSegmentStateChange
	(
	CdbComponentDatabaseInfo *segInfo,
	FtsSegmentStatusChange *change,
	uint8 statusNew
	)
	;

static uint32 transition
	(
	uint32 stateOld,
	uint32 trans,
	CdbComponentDatabaseInfo *primary,
	CdbComponentDatabaseInfo *mirror,
	FtsSegmentStatusChange *changesPrimary,
	FtsSegmentStatusChange *changesMirror
	)
	;

static void updateConfiguration(FtsSegmentStatusChange *changes, int changeEntries);
static void probeUpdateConfig(PGconn *entryConn, FtsSegmentStatusChange *changes, int changeCount);

static PGconn *openSelfConn(CdbComponentDatabaseInfo *entryDB);
static void executeEntrySQL(PGconn *entryConn, const char *query);
static void closeSelfConn(PGconn *conn, bool commit);

static void getFailoverStrategy(char *strategy);
static void FtsFailoverNull(FtsSegmentStatusChange *changes);



/*
 * Main entry point for ftsprobe process.
 *
 * This code is heavily based on pgarch.c, q.v.
 */
int
ftsprobe_start(void)
{
	pid_t		FtsProbePID;

#ifdef EXEC_BACKEND
	switch ((FtsProbePID = ftsprobe_forkexec()))
#else
	switch ((FtsProbePID = fork_process()))
#endif
	{
		case -1:
			ereport(LOG,
					(errmsg("could not fork ftsprobe process: %m")));
			return 0;

#ifndef EXEC_BACKEND
		case 0:
			/* in postmaster child ... */
			/* Close the postmaster's sockets */
			ClosePostmasterPorts(false);

			ftsMain(0, NULL);
			break;
#endif
		default:
			return (int)FtsProbePID;
	}

	
	/* shouldn't get here */
	return 0;
}


/*=========================================================================
 * HELPER FUNCTIONS
 */


#ifdef EXEC_BACKEND
/*
 * ftsprobe_forkexec()
 *
 * Format up the arglist for the ftsprobe process, then fork and exec.
 */
static pid_t
ftsprobe_forkexec(void)
{
	char	   *av[10];
	int			ac = 0;

	av[ac++] = "postgres";
	av[ac++] = "--forkftsprobe";
	av[ac++] = NULL;			/* filled in by postmaster_forkexec */
	av[ac] = NULL;

	Assert(ac < lengthof(av));

	return postmaster_forkexec(ac, av);
}
#endif   /* EXEC_BACKEND */

static void
RequestShutdown(SIGNAL_ARGS)
{
	shutdown_requested = true;
}

/* SIGINT: set flag to run an fts full-scan */
static void
ReqFtsFullScan(SIGNAL_ARGS)
{
	rescan_requested = true;
}

/*
 * FtsProbeMain
 */
NON_EXEC_STATIC void
ftsMain(int argc, char *argv[])
{
	sigjmp_buf	local_sigjmp_buf;
	char	   *fullpath;

	IsUnderPostmaster = true;
	am_ftsprobe = true;
	
	/* reset MyProcPid */
	MyProcPid = getpid();
	
	/* Lose the postmaster's on-exit routines */
	on_exit_reset();

	/* Identify myself via ps */
	init_ps_display("ftsprobe process", "", "", "");

	SetProcessingMode(InitProcessing);

	/*
	 * Set up signal handlers.	We operate on databases much like a regular
	 * backend, so we use the same signal handling.  See equivalent code in
	 * tcop/postgres.c.
	 *
	 * Currently, we don't pay attention to postgresql.conf changes that
	 * happen during a single daemon iteration, so we can ignore SIGHUP.
	 */
	pqsignal(SIGHUP, SIG_IGN);

	/*
	 * Presently, SIGINT will lead to autovacuum shutdown, because that's how
	 * we handle ereport(ERROR).  It could be improved however.
	 */
	pqsignal(SIGINT, ReqFtsFullScan);		/* request full-scan */
	pqsignal(SIGTERM, die);
	pqsignal(SIGQUIT, quickdie); /* we don't do any ftsprobe specific cleanup, just use the standard. */
	pqsignal(SIGALRM, handle_sig_alarm);

	pqsignal(SIGPIPE, SIG_IGN);
	pqsignal(SIGUSR1, SIG_IGN);
	/* We don't listen for async notifies */
	pqsignal(SIGUSR2, RequestShutdown);
	pqsignal(SIGFPE, FloatExceptionHandler);
	pqsignal(SIGCHLD, SIG_DFL);

	/*
	 * Copied from bgwriter
	 */
	CurrentResourceOwner = ResourceOwnerCreate(NULL, "FTS Probe");

	/* Early initialization */
	BaseInit();

	/* See InitPostgres()... */
	InitProcess();	
	InitBufferPoolBackend();
	InitXLOGAccess();

	SetProcessingMode(NormalProcessing);

	/*
	 * If an exception is encountered, processing resumes here.
	 *
	 * See notes in postgres.c about the design of this coding.
	 */
	if (sigsetjmp(local_sigjmp_buf, 1) != 0)
	{
		/* Prevents interrupts while cleaning up */
		HOLD_INTERRUPTS();

		/* Report the error to the server log */
		EmitErrorReport();

		/*
		 * We can now go away.	Note that because we'll call InitProcess, a
		 * callback will be registered to do ProcKill, which will clean up
		 * necessary state.
		 */
		proc_exit(0);
	}

	/* We can now handle ereport(ERROR) */
	PG_exception_stack = &local_sigjmp_buf;

	PG_SETMASK(&UnBlockSig);

	/*
	 * In order to access the catalog, we need a database, and a
	 * tablespace; our access to the heap is going to be slightly
	 * limited, so we'll just use some defaults.
	 */
	MyDatabaseId = TemplateDbOid;
	MyDatabaseTableSpace = DEFAULTTABLESPACE_OID;

	if (!FindMyDatabase(probeDatabase, &MyDatabaseId, &MyDatabaseTableSpace))
		ereport(FATAL, (errcode(ERRCODE_UNDEFINED_DATABASE),
			errmsg("database 'postgres' does not exist")));

	fullpath = GetDatabasePath(MyDatabaseId, MyDatabaseTableSpace);

	SetDatabasePath(fullpath);

	/*
	 * Finish filling in the PGPROC struct, and add it to the ProcArray. (We
	 * need to know MyDatabaseId before we can do this, since it's entered
	 * into the PGPROC struct.)
	 *
	 * Once I have done this, I am visible to other backends!
	 */
	InitProcessPhase2();

	/*
	 * Initialize my entry in the shared-invalidation manager's array of
	 * per-backend data.
	 *
	 * Sets up MyBackendId, a unique backend identifier.
	 */
	MyBackendId = InvalidBackendId;

	InitBackendSharedInvalidationState();

	if (MyBackendId > MaxBackends || MyBackendId <= 0)
		elog(FATAL, "bad backend id: %d", MyBackendId);

	/*
	 * bufmgr needs another initialization call too
	 */
	InitBufferPoolBackend();

	/* heap access requires the rel-cache */
	RelationCacheInitialize();
	InitCatalogCache();

	/*
	 * It's now possible to do real access to the system catalogs.
	 *
	 * Load relcache entries for the system catalogs.  This must create at
	 * least the minimum set of "nailed-in" cache entries.
	 */
	RelationCacheInitializePhase2();

	/* shmem: publish probe pid */
	ftsProbeInfo->fts_probePid = MyProcPid;

	/*
	 * Before we can open probe connections, we need a username. (This
	 * will access catalog tables).
	 */
	retrieveUserAndDb(&probeUser);

	/* main loop */
	FtsLoop();

	/* One iteration done, go away */
	proc_exit(0);
}

static void
readCdbComponentInfoAndUpdateStatus(void)
{
	int i;

	cdb_component_dbs = getCdbComponentInfo(false);

	for (i=0; i < cdb_component_dbs->total_segment_dbs; i++)
	{
		CdbComponentDatabaseInfo *segInfo = &cdb_component_dbs->segment_db_info[i];
		uint8	segStatus;

		segStatus = 0;

		if (SEGMENT_IS_ALIVE(segInfo))
			segStatus |= FTS_STATUS_ALIVE;

		if (SEGMENT_IS_ACTIVE_PRIMARY(segInfo))
			segStatus |= FTS_STATUS_PRIMARY;

		if (segInfo->preferred_role == 'p')
			segStatus |= FTS_STATUS_DEFINEDPRIMARY;

		if (segInfo->mode == 's')
			segStatus |= FTS_STATUS_SYNCHRONIZED;

		if (segInfo->mode == 'c')
			segStatus |= FTS_STATUS_CHANGELOGGING;

		ftsProbeInfo->fts_status[segInfo->dbid] = segStatus;
	}
}

static
void FtsLoop()
{
	bool	updated_bitmap, processing_fullscan;
	MemoryContext probeContext = NULL, oldContext = NULL;
	time_t elapsed,	probe_start_time;

	probeContext = AllocSetContextCreate(TopMemoryContext,
										 "FtsProbeMemCtxt",
										 ALLOCSET_DEFAULT_INITSIZE,	/* always have some memory */
										 ALLOCSET_DEFAULT_INITSIZE,
										 ALLOCSET_DEFAULT_MAXSIZE);
	
	readCdbComponentInfoAndUpdateStatus();

	for (;;)
	{
		if (shutdown_requested)
			break;
		
		/* no need to live on if postmaster has died */
		if (!PostmasterIsAlive(true))
			exit(1);

		probe_start_time = time(NULL);

		ftsLock();

		/* atomically clear cancel flag and check pause flag */
		bool pauseProbes = ftsProbeInfo->fts_pauseProbes;
		ftsProbeInfo->fts_discardResults = false;

		ftsUnlock();

		if (pauseProbes)
		{
			if (gp_log_fts >= GPVARS_VERBOSITY_VERBOSE)
				elog(LOG, "skipping probe, we're paused.");
			goto prober_sleep;
		}

		if (cdb_component_dbs != NULL)
		{
			freeCdbComponentDatabases(cdb_component_dbs);
			cdb_component_dbs = NULL;
		}

		if (ftsProbeInfo->fts_probeScanRequested == ftsProbeInfo->fts_statusVersion)
			processing_fullscan = true;
		else
			processing_fullscan = false;

		readCdbComponentInfoAndUpdateStatus();
		getFailoverStrategy(&failover_strategy);

		elog(DEBUG3, "FTS: starting %s scan with %d segments and %d contents",
			 (processing_fullscan ? "full " : ""),
			 cdb_component_dbs->total_segment_dbs,
			 cdb_component_dbs->total_segments);

		/*
		 * We probe in a special context, some of the heap access
		 * stuff palloc()s internally
		 */
		oldContext = MemoryContextSwitchTo(probeContext);

		/* probe segments */
		FtsProbeSegments(cdb_component_dbs, scan_status);

		/*
		 * Now we've completed the scan, update shared-memory. if we
		 * change anything, we return true.
		 */
		updated_bitmap = probePublishUpdate(scan_status);


		MemoryContextSwitchTo(oldContext);

		/* free any pallocs we made inside probeSegments() */
		MemoryContextReset(probeContext);

		if (!FtsIsActive())
		{
			if (gp_log_fts >= GPVARS_VERBOSITY_VERBOSE)
				elog(LOG, "FTS: skipping probe, FTS is paused or shutting down.");
			goto prober_sleep;
		}

		/*
		 * If we're not processing a full-scan, but one has been requested; we start over.
		 */
		if (!processing_fullscan &&
			ftsProbeInfo->fts_probeScanRequested == ftsProbeInfo->fts_statusVersion)
			continue;

		/*
		 * bump the version (this also serves as an acknowledgement to
		 * a probe-request).
		 */
		if (updated_bitmap || processing_fullscan)
		{
			ftsProbeInfo->fts_statusVersion = ftsProbeInfo->fts_statusVersion + 1;
			rescan_requested = false;
		}

		/* if no full-scan has been requested, we can sleep. */
		if (ftsProbeInfo->fts_probeScanRequested >= ftsProbeInfo->fts_statusVersion)
		{
			/* we need to do a probe immediately */
			elog(LOG, "FTS: skipping sleep, requested version: %d, current version: %d.",
				 (int)ftsProbeInfo->fts_probeScanRequested, (int)ftsProbeInfo->fts_statusVersion);
			continue;
		}

	prober_sleep:
		{
			/* check if we need to sleep before starting next iteration */
			elapsed = time(NULL) - probe_start_time;
			if (elapsed < gp_fts_probe_interval && !shutdown_requested)
			{
				pg_usleep((gp_fts_probe_interval - elapsed) * USECS_PER_SEC);
			}
		}
	} /* end server loop */

	return;
}

/*
 * Check if FTS is active
 */
bool
FtsIsActive(void)
{
	return (!ftsProbeInfo->fts_discardResults && !shutdown_requested);
}


/*
 * Wrapper for catalog lookup for a super user appropriate for FTS probing. We
 * want to use the bootstrap super user as a priority, because it seems more
 * obvious for users and probably hasn't been messed with. If it has been (might
 * have been removed, or modified in other ways), find another which can login
 * and which has not expired yet.
 */
char *
FtsFindSuperuser(bool try_bootstrap)
{
	char *suser = NULL;
	Relation auth_rel;
	HeapTuple	auth_tup;
	cqContext  *pcqCtx;
	cqContext	cqc;
	bool	isNull;

	auth_rel = heap_open(AuthIdRelationId, AccessShareLock);

	if (try_bootstrap)
	{
		pcqCtx = caql_beginscan(
				caql_addrel(cqclr(&cqc), auth_rel),
				cql("SELECT * FROM pg_authid "
					" WHERE rolsuper = :1 "
					" AND rolcanlogin = :2 "
					" AND oid = :3 ",
					BoolGetDatum(true),
					BoolGetDatum(true),
					ObjectIdGetDatum(BOOTSTRAP_SUPERUSERID)));
	}
	else
	{
		pcqCtx = caql_beginscan(
				caql_addrel(cqclr(&cqc), auth_rel),
				cql("SELECT * FROM pg_authid "
					" WHERE rolsuper = :1 "
					" AND rolcanlogin = :2 ",
					BoolGetDatum(true),
					BoolGetDatum(true)));
	}

	while (HeapTupleIsValid(auth_tup = caql_getnext(pcqCtx)))
	{
		Datum	attrName;
		Oid		userOid;
		Datum	validuntil;

		validuntil = heap_getattr(auth_tup, Anum_pg_authid_rolvaliduntil, 
								  RelationGetDescr(auth_rel), &isNull);
		/* we actually want it to be NULL, that means always valid */
		if (!isNull)
			continue;

		attrName = heap_getattr(auth_tup, Anum_pg_authid_rolname, 
								RelationGetDescr(auth_rel), &isNull);
		Assert(!isNull);
		suser = pstrdup(DatumGetCString(attrName));

		userOid = HeapTupleGetOid(auth_tup);

		SetSessionUserId(userOid, true);

		break;
	}

	caql_endscan(pcqCtx);
	heap_close(auth_rel, AccessShareLock);
	return suser;
}

/*
 * Get a user for the FTS prober to connect as
 */
static void
retrieveUserAndDb(char **probeUser)
{
	Assert(probeUser != NULL);

	/* first, let's try the bootstrap super user */
	*probeUser = FtsFindSuperuser(true);
	if (!(*probeUser))
		*probeUser = FtsFindSuperuser(false);

	Assert(*probeUser != NULL);
}


/*
 * Build a set of changes, based on our current state, and the probe results.
 */
static bool
probePublishUpdate(uint8 *probe_results)
{
	bool update_found = false;
	int i;

	if (failover_strategy == 'f')
	{
		/* preprocess probe results to decide what is the current segment state */
		FtsPreprocessProbeResultsFilerep(cdb_component_dbs, probe_results);
	}

	for (i = 0; i < cdb_component_dbs->total_segment_dbs; i++)
	{
		CdbComponentDatabaseInfo *segInfo = &cdb_component_dbs->segment_db_info[i];

		/* if we've gotten a pause or shutdown request, we ignore our probe results. */
		if (!FtsIsActive())
		{
			return false;
		}

		/* we check segments in pairs of primary-mirror */
		if (!SEGMENT_IS_ACTIVE_PRIMARY(segInfo))
		{
			continue;
		}

		CdbComponentDatabaseInfo *primary = segInfo;
		CdbComponentDatabaseInfo *mirror = FtsGetPeerSegment(segInfo->segindex, segInfo->dbid);

		if (failover_strategy == 'n')
		{
			uint8 statusOld = ftsProbeInfo->fts_status[segInfo->dbid];

			Assert(SEGMENT_IS_ACTIVE_PRIMARY(segInfo));
			Assert(mirror == NULL);

			/* no mirror available to failover */
			if (!PROBE_IS_ALIVE(segInfo) && (statusOld & FTS_STATUS_ALIVE))
			{
				FtsSegmentStatusChange changes;
				uint8 statusNew = statusOld & ~FTS_STATUS_ALIVE;

				buildSegmentStateChange(segInfo, &changes, statusNew);

				/* GPSQL can mark the segment down. */
				updateConfiguration(&changes, 1);
				FtsFailoverNull(&changes);
				update_found = true;
			}
			else if (PROBE_IS_ALIVE(segInfo) && ~(statusOld | ~FTS_STATUS_ALIVE))
			{
				FtsSegmentStatusChange changes;
				uint8 statusNew = statusOld | FTS_STATUS_ALIVE;

				buildSegmentStateChange(segInfo, &changes, statusNew);

				/* The segment is back online. */
				updateConfiguration(&changes, 1);
				FtsFailoverNull(&changes);
				update_found = true;
			}

			continue;
		}

		Assert(failover_strategy == 'f' || failover_strategy == 's');
		Assert(mirror != NULL);

		/* changes required for primary and mirror */
		FtsSegmentStatusChange changes[2];

		uint32 stateOld = 0;
		uint32 stateNew = 0;

		bool isPrimaryAlive = PROBE_IS_ALIVE(primary);
		bool isMirrorAlive = PROBE_IS_ALIVE(mirror);

		/* get transition type */
		uint32 trans = getTransition(isPrimaryAlive, isMirrorAlive);

		if (gp_log_fts >= GPVARS_VERBOSITY_VERBOSE)
		{
			elog(LOG, "FTS: primary found %s, mirror found %s, transition %d.",
				 (isPrimaryAlive ? "alive" : "dead"), (isMirrorAlive ? "alive" : "dead"), trans);
		}

		if (trans == TRANS_D_D)
		{
			elog(LOG, "FTS: detected double failure for content=%d, primary (dbid=%d), mirror (dbid=%d).",
			     primary->segindex, primary->dbid, mirror->dbid);
		}

		if (failover_strategy == 'f')
		{
			/* get current state */
			stateOld = FtsGetPairStateFilerep(primary, mirror);

			/* get new state */
			stateNew = transition(stateOld, trans, primary, mirror, &changes[0], &changes[1]);
		}
		else
		{
			Assert(failover_strategy == 's');

			/* get current state */
			stateOld = FtsGetPairStateSAN(primary, mirror);

			/* get new state */
			stateNew = transition(stateOld, trans, primary, mirror, &changes[0], &changes[1]);
		}

		/* check if transition is required */
		if (stateNew != stateOld)
		{
			update_found = true;
			updateConfiguration(changes, ARRAY_SIZE(changes));
		}
	}

	if (gp_log_fts >= GPVARS_VERBOSITY_VERBOSE)
	{
		elog(LOG, "FTS: probe result processing is complete.");
	}

	return update_found;
}


/*
 * Build struct with segment changes
 */
static void
buildSegmentStateChange(CdbComponentDatabaseInfo *segInfo, FtsSegmentStatusChange *change, uint8 statusNew)
{
	change->dbid = segInfo->dbid;
	change->segindex = segInfo->segindex;
	change->oldStatus = ftsProbeInfo->fts_status[segInfo->dbid];
	change->newStatus = statusNew;
}

/*
 * get transition type - derived from probed primary/mirror state
 */
static uint32
getTransition(bool isPrimaryAlive, bool isMirrorAlive)
{
	uint32 state = (isPrimaryAlive ? 2 : 0) + (isMirrorAlive ? 1 : 0);

	switch (state)
	{
		case (0):
			/* primary and mirror dead */
			return TRANS_D_D;
		case (1):
			/* primary dead, mirror alive */
			return TRANS_D_U;
		case (2):
			/* primary alive, mirror dead */
			return TRANS_U_D;
		case (3):
			/* primary and mirror alive */
			return TRANS_U_U;
		default:
			Assert(!"Invalid transition for FTS state machine");
			return 0;
	}
}


/*
 * find new state for primary and mirror
 */
static uint32
transition
	(
	uint32 stateOld,
	uint32 trans,
	CdbComponentDatabaseInfo *primary,
    CdbComponentDatabaseInfo *mirror,
    FtsSegmentStatusChange *changesPrimary,
    FtsSegmentStatusChange *changesMirror
    )
{
	Assert(IS_VALID_TRANSITION(trans));

	/* reset changes */
	memset(changesPrimary, 0, sizeof(*changesPrimary));
	memset(changesMirror, 0, sizeof(*changesMirror));

	uint32 stateNew = stateOld;

	/* in case of a double failure we don't do anything */
	if (trans == TRANS_D_D)
	{
		return stateOld;
	}

	/* get new state for primary and mirror */
	if (failover_strategy == 'f')
	{
		stateNew = FtsTransitionFilerep(stateOld, trans);
	}
	else
	{
		stateNew = FtsTransitionSAN(stateOld, trans);
	}

	/* check if transition is required */
	if (stateNew != stateOld)
	{
		FtsSegmentPairState pairState;
		memset(&pairState, 0, sizeof(pairState));
		pairState.primary = primary;
		pairState.mirror = mirror;
		pairState.stateNew = stateNew;
		pairState.statePrimary = 0;
		pairState.stateMirror = 0;

		if (gp_log_fts >= GPVARS_VERBOSITY_DEBUG)
		{
			elog(LOG, "FTS: state machine transition from %d to %d.", stateOld, stateNew);
		}

		if (failover_strategy == 'f')
		{
			FtsResolveStateFilerep(&pairState);
		}
		else
		{
			FtsResolveStateSAN(&pairState);
		}

		buildSegmentStateChange(primary, changesPrimary, pairState.statePrimary);
		buildSegmentStateChange(mirror, changesMirror, pairState.stateMirror);

		FtsDumpChanges(changesPrimary, 1);
		FtsDumpChanges(changesMirror, 1);
	}

	return stateNew;
}


/*
 * Apply requested segment transitions
 */
static void
updateConfiguration(FtsSegmentStatusChange *changes, int changeEntries)
{
	Assert(changes != NULL);

	PGconn *entryConn = NULL;
	char timestamp_str[TIMESTAMP_BUF_SIZE];

	CdbComponentDatabaseInfo *entryDB = &cdb_component_dbs->entry_db_info[0];

	if (entryDB->dbid != GpIdentity.dbid)
	{
		if (gp_log_fts >= GPVARS_VERBOSITY_DEBUG)
		{
			elog(LOG, "FTS: advancing to second entry-db.");
		}
		entryDB = entryDB + 1;
	}

	/* if we've gotten a pause or shutdown request, we ignore our probe results. */
	if (!FtsIsActive())
	{
		return;
	}

	entryConn = openSelfConn(entryDB);
	Assert(entryConn != NULL);

	/* update segment configuration */
	probeUpdateConfig(entryConn, changes, changeEntries);

	/* if FTS is not active, we ignore probe results (abort transaction) */
	ftsLock();
	bool commit = FtsIsActive();
	ftsUnlock();

	/* commit/abort configuration and history changes */
	closeSelfConn(entryConn, commit);

	if (commit)
	{
		if (failover_strategy == 'f')
		{
			/* FILEREP response */
			FtsFailoverFilerep(changes, changeEntries);
		}
		else if (failover_strategy == 's')
		{
			/* SAN response */
			FtsFailoverSAN(changes, changeEntries, timestamp_str);
		}
	}

	if (gp_log_fts >= GPVARS_VERBOSITY_VERBOSE)
	{
		elog(LOG, "FTS: finished segment modifications.");
	}
}


/*
 * update segment configuration in catalog and shared memory
 */
static void
probeUpdateConfig(PGconn *entryConn, FtsSegmentStatusChange *changes, int changeCount)
{
	int i;

	int count;
	char cmd[SQL_CMD_BUF_SIZE];

	bool	valid;
	bool	primary;
	bool	changelogging;

	Assert(changes != NULL);
	Assert(changeCount > 0);

	for (i = 0; i < changeCount; i++)
	{
		FtsSegmentStatusChange *change = &changes[i];
		valid   = (changes[i].newStatus & FTS_STATUS_ALIVE) ? true : false;
		primary = (changes[i].newStatus & FTS_STATUS_PRIMARY) ? true : false;
		changelogging = (changes[i].newStatus & FTS_STATUS_CHANGELOGGING) ? true : false;

		if (changelogging)
		{
			Assert(failover_strategy == 'f');
			Assert(primary && valid);
		}

		/*
		 * Log change to segment configuration
		 */
		count = snprintf(cmd, sizeof(cmd),
						 "insert into %s values (now(), %d, 'FTS: content %d fault marking status %s%s role %c')",
						 GpConfigHistoryRelName,
						 change->dbid, change->segindex,
						 valid ? "UP" : "DOWN",
						 (changelogging) ? " mode: change-tracking" : "",
						 primary ? 'p' : 'm');

		Assert(count < sizeof(cmd));

		executeEntrySQL(entryConn, cmd);

		/*
		 * Update segment configuration in catalog
		 */
		count = snprintf(cmd, sizeof(cmd),
						 "update %s set role='%c', status='%c'%s where dbid=CAST(%d AS SMALLINT)",
						 GpSegmentConfigRelationName,
						 primary ? 'p' : 'm',
						 valid ? 'u' : 'd',
						 (changelogging) ? ", mode = 'c'" : "",
						 changes[i].dbid);

		Assert(count < sizeof(cmd));

		executeEntrySQL(entryConn, cmd);

		/*
		 * Update shared memory
		 */
		ftsProbeInfo->fts_status[changes[i].dbid] = changes[i].newStatus;
	}
}


static void
getFailoverStrategy(char *strategy)
{
	Relation	strategy_rel;
	HeapTuple	strategy_tup;
	cqContext  *pcqCtx;
	cqContext	cqc;
	bool	isNull=true;

	Assert(strategy != NULL);

	strategy_rel = heap_open(GpFaultStrategyRelationId, AccessShareLock);

	/* XXX XXX: only one of these? then would be getfirst... */
	pcqCtx = caql_beginscan(
			caql_addrel(cqclr(&cqc), strategy_rel),
			cql("SELECT * FROM gp_fault_strategy ", NULL));

	while (HeapTupleIsValid(strategy_tup = caql_getnext(pcqCtx)))
	{
		Datum	strategy_datum;

		strategy_datum = heap_getattr(strategy_tup, Anum_gp_fault_strategy_fault_strategy, RelationGetDescr(strategy_rel), &isNull);

		if (isNull)
			break;

		*strategy = DatumGetChar(strategy_datum);
	}

	caql_endscan(pcqCtx);
	heap_close(strategy_rel, AccessShareLock);

	return;
}


bool
FtsIsSegmentAlive(CdbComponentDatabaseInfo *segInfo)
{
	switch (failover_strategy)
	{
		case 'f':
			if (SEGMENT_IS_ACTIVE_MIRROR(segInfo) && SEGMENT_IS_ALIVE(segInfo))
				return true;
			/* fallthrough */
		case 'n':
		case 's':
			if (SEGMENT_IS_ACTIVE_PRIMARY(segInfo))
				return true;
			break;
		default:
			write_log("segmentToProbe: invalid failover strategy (%c).", failover_strategy);
			break;
	}

	return false;
}


/*
 * Dump out the changes to our logfile.
 */
void
FtsDumpChanges(FtsSegmentStatusChange *changes, int changeEntries)
{
	Assert(changes != NULL);
	int i = 0;

	for (i = 0; i < changeEntries; i++)
	{
		bool new_alive, old_alive;
		bool new_pri, old_pri;

		new_alive = (changes[i].newStatus & FTS_STATUS_ALIVE ? true : false);
		old_alive = (changes[i].oldStatus & FTS_STATUS_ALIVE ? true : false);

		new_pri = (changes[i].newStatus & FTS_STATUS_PRIMARY ? true : false);
		old_pri = (changes[i].oldStatus & FTS_STATUS_PRIMARY ? true : false);

		elog(LOG, "FTS: change state for segment (dbid=%d, content=%d) from ('%c','%c') to ('%c','%c')",
			 changes[i].dbid,
			 changes[i].segindex,
			 (old_alive ? 'u' : 'd'),
			 (old_pri ? 'p' : 'm'),
			 (new_alive ? 'u' : 'd'),
			 (new_pri ? 'p' : 'm'));
	}
}

static void
FtsFailoverNull(FtsSegmentStatusChange *changePrimary)
{
	if (gp_log_fts >= GPVARS_VERBOSITY_VERBOSE)
	{
		FtsDumpChanges(changePrimary, 1);
	}
}


/**
 * Marks the given db as in-sync in the segment configuration.
 */
void
FtsMarkSegmentsInSync(CdbComponentDatabaseInfo *primary, CdbComponentDatabaseInfo *mirror)
{
	if (!FTS_STATUS_ISALIVE(primary->dbid, ftsProbeInfo->fts_status) ||
	    !FTS_STATUS_ISALIVE(mirror->dbid, ftsProbeInfo->fts_status) ||
	    !FTS_STATUS_ISPRIMARY(primary->dbid, ftsProbeInfo->fts_status) ||
 	    FTS_STATUS_ISPRIMARY(mirror->dbid, ftsProbeInfo->fts_status) ||
	    FTS_STATUS_IS_SYNCED(primary->dbid, ftsProbeInfo->fts_status) ||
	    FTS_STATUS_IS_SYNCED(mirror->dbid, ftsProbeInfo->fts_status) ||
	    FTS_STATUS_IS_CHANGELOGGING(primary->dbid, ftsProbeInfo->fts_status) ||
	    FTS_STATUS_IS_CHANGELOGGING(mirror->dbid, ftsProbeInfo->fts_status))
	{
		FtsRequestPostmasterShutdown(primary, mirror);
	}

	char cmd[SQL_CMD_BUF_SIZE];

	CdbComponentDatabaseInfo *entryDB = &cdb_component_dbs->entry_db_info[0];

	if (gp_log_fts >= GPVARS_VERBOSITY_DEBUG)
		elog(LOG, "FTS: updating segment to in-sync.");

	if (entryDB->dbid != GpIdentity.dbid)
	{
		if (gp_log_fts >= GPVARS_VERBOSITY_DEBUG)
			elog(LOG, "FTS: advancing to second entryDB.");
		entryDB = entryDB + 1;
	}

	if (ftsProbeInfo->fts_pauseProbes)
	{
		return;
	}

	uint8	segStatus=0;

	PGconn *entryConn = openSelfConn(entryDB);
	Assert(entryConn != NULL);

	/* update primary */
	segStatus = ftsProbeInfo->fts_status[primary->dbid];
	segStatus |= FTS_STATUS_SYNCHRONIZED;
	ftsProbeInfo->fts_status[primary->dbid] = segStatus;

	/* update mirror */
	segStatus = ftsProbeInfo->fts_status[mirror->dbid];
	segStatus |= FTS_STATUS_SYNCHRONIZED;
	ftsProbeInfo->fts_status[mirror->dbid] = segStatus;

	/* update gp_segment_configuration to insync */
	snprintf(cmd, sizeof(cmd),
			 "update %s set mode='s' where dbid=CAST(%d AS SMALLINT)",
			 GpSegmentConfigRelationName,
			 primary->dbid);
	executeEntrySQL(entryConn, cmd);

	snprintf(cmd, sizeof(cmd),
			 "update %s set mode='s' where dbid=CAST(%d AS SMALLINT)",
			 GpSegmentConfigRelationName,
			 mirror->dbid);
	executeEntrySQL(entryConn, cmd);

	/* update configuration history */
	snprintf(cmd, sizeof(cmd),
			 "insert into %s values (now(), %d, 'FTS: changed segment to insync from resync.')",
			 GpConfigHistoryRelName,
			 primary->dbid);
	executeEntrySQL(entryConn, cmd);

	snprintf(cmd, sizeof(cmd),
			 "insert into %s values (now(), %d, 'FTS: changed segment to insync from resync.')",
			 GpConfigHistoryRelName,
			 mirror->dbid);
	executeEntrySQL(entryConn, cmd);

	ereport(LOG,
			(errmsg("FTS: resynchronization of mirror (dbid=%d, content=%d) on %s:%d has completed.",
					mirror->dbid, mirror->segindex, mirror->address, mirror->port ),
			 errSendAlert(true)));

	/* if FTS is not active, we ignore probe results (abort transaction) */
	ftsLock();
	bool commit = FtsIsActive();
	ftsUnlock();

	/* commit/abort configuration and history changes */
	closeSelfConn(entryConn, commit);
}

/*
 * Get peer segment descriptor
 */
CdbComponentDatabaseInfo *FtsGetPeerSegment(int content, int dbid)
{
	int i;

	for (i=0; i < cdb_component_dbs->total_segment_dbs; i++)
	{
		CdbComponentDatabaseInfo *segInfo = &cdb_component_dbs->segment_db_info[i];

		if (segInfo->segindex == content && segInfo->dbid != dbid)
		{
			/* found it */
			return segInfo;
		}
	}

	return NULL;
}


/*
 * Notify postmaster to shut down due to inconsistent segment state
 */
void FtsRequestPostmasterShutdown(CdbComponentDatabaseInfo *primary, CdbComponentDatabaseInfo *mirror)
{
	FtsRequestMasterShutdown();

	elog(FATAL, "FTS: detected invalid state for content=%d: "
			    "primary (dbid=%d, mode='%c', status='%c'), "
			    "mirror (dbid=%d, mode='%c', status='%c'), "
			    "shutting down master.",
			    primary->segindex,
			    primary->dbid,
			    primary->mode,
			    primary->status,
			    mirror->dbid,
			    mirror->mode,
			    mirror->status
			    );
}


/*
 * Obtains utility-mode self-connection to master-DB, and opens a transaction.
 */
static PGconn *
openSelfConn(CdbComponentDatabaseInfo *entryDB)
{
	PQExpBuffer entryBuffer = NULL;
	PGconn *entryConn = NULL;

	PGresult   *rs;

	Assert(entryDB->segindex == -1);

	entryBuffer = createPQExpBuffer();
	if (PQExpBufferBroken(entryBuffer))
	{
		destroyPQExpBuffer(entryBuffer);
		elog(FATAL, "FTS: failed to create self-connection buffer.");
		/* never reached */
		return NULL;
	}

	appendPQExpBuffer(entryBuffer, "options='-c gp_session_role=UTILITY -c allow_system_table_mods=dml' ");

	/*
	 * Do not need to set host, since FTS is running on the same host
	 * as the master node.
	 *
	 * NOTE: do not be tempted to add a host and force TCP: we use ident
	 * authentication to test that this is the master connecting back to itself
	 * and we cannot do that via TCP. See MPP-15802.
	 */
	appendPQExpBuffer(entryBuffer, "port=%u ", entryDB->port);
	appendPQExpBuffer(entryBuffer, "dbname=%s ", probeDatabase);
	appendPQExpBuffer(entryBuffer, "user=%s ", probeUser);
	appendPQExpBuffer(entryBuffer, "connect_timeout=%d ", gp_segment_connect_timeout);

	/*
	 * Call libpq to connect
	 */
	entryConn = PQconnectdb(entryBuffer->data);

	if (PQstatus(entryConn) == CONNECTION_BAD)
	{
		/*
		 * When we get an error, we strdup it here.  When the main thread
		 * checks for errors, it makes a palloc copy of this, and frees
		 * this.
		 */
		char	*error_message = strdup(PQerrorMessage(entryConn));

		if (!error_message)
			ereport(ERROR, (errcode(ERRCODE_OUT_OF_MEMORY),
							errmsg("FTS: out of memory")));

		destroyPQExpBuffer(entryBuffer);
		PQfinish(entryConn);
		entryConn = NULL;

		elog(FATAL, "FTS: failed to connect to entry-db, error: %s.", error_message);
	}

	rs = PQexec(entryConn, "BEGIN");

	if (PQresultStatus(rs) != PGRES_COMMAND_OK)
	{
		/*
		 * When we get an error, we strdup it here.  When the main thread
		 * checks for errors, it makes a palloc copy of this, and frees
		 * this.
		 */
		char	*error_message = strdup(PQerrorMessage(entryConn));

		if (!error_message)
			ereport(ERROR, (errcode(ERRCODE_OUT_OF_MEMORY),
							errmsg("FTS: out of memory")));

		destroyPQExpBuffer(entryBuffer);
		PQfinish(entryConn);
		entryConn = NULL;

		elog(FATAL, "FTS: failed to start transaction to entry-db, error: %s.", error_message);
	}

	destroyPQExpBuffer(entryBuffer);

	return entryConn;
}


/*
 * Run SQL query on a previously established self-connection.
 */
static void
executeEntrySQL(PGconn *entryConn, const char *query)
{
	Assert(entryConn != NULL);
	Assert(query != NULL);

	PGresult   *rs = NULL;

	rs = PQexec(entryConn, query);
	if (PQresultStatus(rs) != PGRES_COMMAND_OK)
	{
		char	*error_message = strdup(PQerrorMessage(entryConn));

		if (!error_message)
			ereport(ERROR, (errcode(ERRCODE_OUT_OF_MEMORY),
							errmsg("FTS: out of memory")));

		PQclear(rs);
		PQfinish(entryConn);
		entryConn = NULL;

		elog(FATAL, "FTS: query '%s' failed to execute to entry-db, error: %s.",
		            query, error_message);
	}
}


/*
 * Commit and close self-connection to master-DB.
 */
static void
closeSelfConn(PGconn *conn, bool commit)
{
	PGresult   *rs;
	const char *stmt = (commit ? "COMMIT" : "ABORT");

	rs = PQexec(conn, stmt);
	if (PQresultStatus(rs) != PGRES_COMMAND_OK)
	{
		/*
		 * When we get an error, we strdup it here.  When the main thread
		 * checks for errors, it makes a palloc copy of this, and frees
		 * this.
		 */
		char	*error_message = strdup(PQerrorMessage(conn));

		if (!error_message)
			ereport(ERROR, (errcode(ERRCODE_OUT_OF_MEMORY),
							errmsg("FTS: out of memory")));

		PQclear(rs);
		PQfinish(conn);
		conn = NULL;

		elog(FATAL, "FTS: connection to master failed to %s, error: %s.", stmt, error_message);
	}

	/* No error, we're ready to go */
	PQclear(rs);
	PQfinish(conn);
	return;
}


/* EOF */
