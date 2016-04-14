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

#include "envswitch.h"
#include "dynrm.h"
#include "utils/hashtable.h"

#include "resourcemanager/resourcemanager.h"
#include "resourceenforcer/resourceenforcer.h"

#include "storage/ipc.h"
#include "postmaster/postmaster.h"
#include "postmaster/fork_process.h"
#include "postmaster/syslogger.h"
#include "utils/kvproperties.h"
#include "utils/network_utils.h"

#include "miscadmin.h"

#include "communication/rmcomm_QD2RM.h"
#include "communication/rmcomm_AsyncComm.h"
#include "communication/rmcomm_MessageHandler.h"
#include "communication/rmcomm_MessageServer.h"
#include "communication/rmcomm_RM2GRM_libyarn.h"
#include "communication/rmcomm_RM2GRM_none.h"
#include "communication/rmcomm_RMSEG2RM.h"
#include "communication/rmcomm_RM2RMSEG.h"
#include "storage/proc.h"
#include "catalog/pg_database.h"
#include "catalog/pg_tablespace.h"
#include "catalog/catalog.h"
#include "utils/syscache.h"
#include "storage/backendid.h"
#include "storage/sinvaladt.h"

#include "catalog/pg_authid.h"
#include "catalog/pg_resqueue.h"
#include "access/htup.h"
#include "utils/tqual.h"

#include "access/xact.h"

#include "resourcebroker/resourcebroker_API.h"

#include <executor/spi.h>

#include "gp-libpq-fe.h"
#include "gp-libpq-int.h"

extern bool FindMyDatabase(const char *name, Oid *db_id, Oid *db_tablespace);
static char *probeDatabase = "template1";

int  loadAllQueueAndUser(void);
int  loadHostInformationIntoResourcePool(void);

/** Functions for signal handling. 											 **/
void quitResManager(SIGNAL_ARGS);
void handleChildSignal(SIGNAL_ARGS);
void notifyPostmasterResManagerStarted(SIGNAL_ARGS);
int  generateAllocRequestToBroker(void);
void completeAllocRequestToBroker(int32_t 	 *reqmem,
								  int32_t 	 *reqcore,
								  List 		**preferred);
void processResourceBrokerTasks(void);
void generateResourceRequestToResourceBroker(void);
void cleanupAllGRMContainers(void);
bool cleanedAllGRMContainers(void);

/** Functions for loading resource queue and user definition. **/
#define DEC_VAR_PG_CATCOL(name) Datum name##Datum; bool name##IsNull = false;

#define GETATTR_PG_CATCOL(tbname,colname,relvar,tupvar)						   \
		colname##Datum = heap_getattr(tupvar,								   \
									  Anum_##tbname##_##colname,			   \
									  relvar->rd_att, 						   \
									  &colname##IsNull);

#define BLDPROP_PG_CATCOL(colname,list,tag1,attrtag,attrid,attrtyp,index)	   \
		insertDQueueTailNode(list,											   \
							 createProperty##attrtyp(list->Context,			   \
							 tag1,											   \
							 get##attrtag##AttributeName(attrid),			   \
							 &index,										   \
							 DatumGet##attrtyp(colname##Datum)));

#define BLDPROP_PG_CATCOL_OID(colname,list,tag1,attrtag,attrid,index)	   	   \
		insertDQueueTailNode(list,											   \
							 createPropertyOID(list->Context,			  	   \
							 tag1,											   \
							 get##attrtag##AttributeName(attrid),			   \
							 &index,										   \
							 colname##IsNull ? InvalidOid :					   \
								DatumGetObjectId(colname##Datum)));

#define BLDPROP_PG_CATCOL_SOID(list,tag1,attrtag,attrid,index,oid)	   		   \
		insertDQueueTailNode(list,											   \
							 createPropertyOID(list->Context,			   	   \
							 tag1,											   \
							 get##attrtag##AttributeName(attrid),			   \
							 &index,										   \
							 oid));

#define BLDPROP_PG_CATCOL_SET(list,tag1,attrtag,attrid,attrtyp,index,val)  	   \
		insertDQueueTailNode(list,											   \
							 createProperty##attrtyp(list->Context,			   \
							 tag1,											   \
							 get##attrtag##AttributeName(attrid),			   \
							 &index,										   \
							 val));

#define BLDPROP_PG_CATCOL_TXT(colname,list,tag1,attrtag,attrid,index)	       \
		insertDQueueTailNode(list,											   \
							 createPropertyText(list->Context,			 	   \
							 tag1,											   \
							 get##attrtag##AttributeName(attrid),			   \
							 &index,										   \
							 colname##IsNull ? NULL :					   	   \
								DatumGetTextP(colname##Datum)));

#define BLDPROP_PG_CATCOL_FLOAT(colname,list,tag1,attrtag,attrid,index)        \
		insertDQueueTailNode(list,                                             \
						     createPropertyFloat(list->Context,                \
						     tag1,                                             \
                             get##attrtag##AttributeName(attrid),              \
							 &index,                                           \
							 colname##IsNull ? -1.0 :                          \
								DatumGetFloat4(colname##Datum)));

#define BLDPROP_PG_CATCOL_STR(list,tag1,attrtag,attrid,index,val)	   	  	   \
		insertDQueueTailNode(list,											   \
							 createPropertyString(list->Context,			   \
							 tag1,											   \
							 get##attrtag##AttributeName(attrid),			   \
							 &index,										   \
							 val));

#define BLDPROP_PG_CATCOL_INT32(colname,list,tag1,attrtag,attrid,index)	       \
		insertDQueueTailNode(list,											   \
							 createPropertyInt32(list->Context,			 	   \
							 tag1,											   \
							 get##attrtag##AttributeName(attrid),			   \
							 &index,										   \
							 colname##IsNull ? -1 :					   	   \
								DatumGetInt32(colname##Datum)));

#define BLDPROP_PG_CATCOL_INT64(colname,list,tag1,attrtag,attrid,index)	       \
		insertDQueueTailNode(list,											   \
							 createPropertyInt32(list->Context,			 	   \
							 tag1,											   \
							 get##attrtag##AttributeName(attrid),			   \
							 &index,										   \
							 colname##IsNull ? -1 :					   	   \
								DatumGetInt64(colname##Datum)));

int	 loadUserPropertiesFromCatalog(List **users);
int	 loadQueuePropertiesFromCatalog(List **queues);

/******************************************************************************
 * The global instance of dynamic resource manager.
 ******************************************************************************/
DynRMGlobal   DRMGlobalInstance;/* The global instance for HAWQ RM logic. 	  */
volatile bool DRMStartup;		/* This variable is set only in postmaster
								   process by signal handler SIG_USR2. This is
								   used to check if HAWQ RM is ready to accept
								   request.									  */

pthread_t	t_move_cgroup;
queue		*g_queue_cgroup;
GHash		g_ghash_cgroup;
uint64		rm_enforce_last_cleanup_time;
volatile bool	g_enforcement_thread_quited = false;

#define SEGMENT_HEARTBEAT_INTERVAL (3LL * 1000000LL)
/***************************
 * The main entry of HAWQ RM
 ***************************/
int ResManagerMain(int argc, char *argv[])
{
    int 	 	 res 		= FUNC_RETURN_OK;

    /*******************************************************************/
    /* Step 1. Prepare memory context, log and create global instance. */
    /*******************************************************************/
	IsUnderPostmaster 	= true;
	MyProcPid 			= getpid();
	SetProcessingMode(InitProcessing);

    res = createDRMInstance();
	if ( res != FUNC_RETURN_OK ) {
		elog(FATAL, "HAWQ RM Can not create resource manager global instance.");
	}
	elog(DEBUG5, "HAWQ RM :: created dynamic resource manager instance.");

    res = createDRMMemoryContext();
    if ( res != FUNC_RETURN_OK ) {
    	elog(FATAL, "HAWQ RM Can not create resource manager global instance.");
    }
	elog(DEBUG5, "HAWQ RM :: created resource manager memory context.");

	res = initializeDRMInstance(PCONTEXT);
	if ( res != FUNC_RETURN_OK ) {
	    	elog(FATAL, "HAWQ RM Can not initialize global instance.");
	}

	elog(DEBUG5, "HAWQ RM :: initialized resource manager instance.");

	/**************************************************************************
	 * STEP 2. Load configuration.
	 **************************************************************************/

	/* Command line parser. */
    res = parseCommandLine(argc, argv);
    if ( res != FUNC_RETURN_OK ) {
    	elog(FATAL, "Wrong resource manager command line arguments.");
    }

	elog(DEBUG5, "HAWQ RM ::passed command line arguments.");

	/* Recognize all loaded configure properties. */
	res = loadDynamicResourceManagerConfigure();
	if ( res != FUNC_RETURN_OK ) {
		elog(FATAL, "Fail to load valid properties from configure files.");
	}

	elog(DEBUG5, "HAWQ RM :: passed loading configuration.");

    /*******************************************************/
    /* Step 3. Post-initialization of core data structure. */
    /*******************************************************/
    initializeDRMCore();

	elog(DEBUG5, "HAWQ RM :: Passed initializing core data structure.");

	/**************************************************************************/
	/* STEP 4. INIT for making RM process access catalog by CAQL etc.         */
	/**************************************************************************/
	/* BLOCK signal behavior. Only another specific thread has the capability to
	 * process the signal. */
	PG_SETMASK(&BlockSig);
	pqsignal(SIGHUP , SIG_IGN);
	pqsignal(SIGINT , quitResManager);
	pqsignal(SIGTERM, quitResManager);
	pqsignal(SIGQUIT, quitResManager);
	pqsignal(SIGPIPE, SIG_IGN);
	pqsignal(SIGUSR1, SIG_IGN);
	pqsignal(SIGUSR2, quitResManager);
	pqsignal(SIGCHLD, handleChildSignal);
	pqsignal(SIGTTIN, SIG_IGN);
	pqsignal(SIGTTOU, SIG_IGN);

	if ( DRMGlobalInstance->Role == START_RM_ROLE_MASTER ) {
		CurrentResourceOwner = ResourceOwnerCreate(NULL, "Resource Manager");

		BaseInit();
		InitProcess();
		InitBufferPoolBackend();
		InitXLOGAccess();

		SetProcessingMode(NormalProcessing);

		MyDatabaseId = TemplateDbOid;
		MyDatabaseTableSpace = DEFAULTTABLESPACE_OID;
		if (!FindMyDatabase(probeDatabase, &MyDatabaseId, &MyDatabaseTableSpace))
			ereport(FATAL, (errcode(ERRCODE_UNDEFINED_DATABASE),
				errmsg("database 'postgres' does not exist")));

		char *fullpath = GetDatabasePath(MyDatabaseId, MyDatabaseTableSpace);

		SetDatabasePath(fullpath);

		InitProcessPhase2();

		MyBackendId = InvalidBackendId;

		SharedInvalBackendInit(false);

		if (MyBackendId > MaxBackends || MyBackendId <= 0)
			elog(FATAL, "bad backend id: %d", MyBackendId);

		InitBufferPoolBackend();
		RelationCacheInitialize();
		InitCatalogCache();
		RelationCacheInitializePhase2();
	}
	/* END: INIT for making RM process access catalog by caql etc.            */
	/**************************************************************************/
	PG_SETMASK(&UnBlockSig);

    /* Save process fork mode. */
	DRMGlobalInstance->ThisPID	 = getpid();
	DRMGlobalInstance->ParentPID = getppid();

	/* Initialize socket connection pool. */
	initializeSocketConnectionPool();

	elog(DEBUG5, "HAWQ RM :: starts as role %d.", DRMGlobalInstance->Role);

	/*******************************************/
	/* STEP 5. Start concrete processing loop. */
	/*******************************************/
    switch(DRMGlobalInstance->Role) {
    case START_RM_ROLE_MASTER:
    {
    	/* Mark as master side resource manager. */
    	int oldid = gp_session_id;
    	gp_session_id = -1;
		init_ps_display("master resource manager", "     ", "", "");
		gp_session_id = oldid;

		/* Initialize resource broker service. */
		RB_prepareImplementation(DRMGlobalInstance->ImpType);
		res = RB_start(true);
		if ( res != FUNC_RETURN_OK ) {
			elog(FATAL, "Fail to create resource broker service.");
		}
		res = ResManagerMainServer2ndPhase();
		elog(LOG, "Master RM exits.\n");
		break;
    }
    case START_RM_ROLE_SEGMENT:
    {
		init_ps_display("segment resource manager", "", "","");
		res = ResManagerMainSegment2ndPhase();
		elog(LOG, "Segment RM exits.\n");
		break;
    }
    default:
    	Assert(false);	/* Should never come here. */
    }
    return res;
}


/*
 * Start sub-process as resource manager from postmaster.
 *
 * rmrole[in] 	Specify the role of RM process, possible values are :
 *				START_RM_ROLE_SERVER : RM server
 *				START_RM_ROLE_SEGMENT: RM agent.
 */
int ResManagerProcessStartup(void)
{
	pid_t ResMgrPID = 0;
	char *arguments[3] = {NULL, NULL, NULL};
	arguments[0] = "postgres";
	arguments[1] = "-role";

	int rmrole = AmIMaster() ? START_RM_ROLE_MASTER : START_RM_ROLE_SEGMENT;
    // if in upgrade mode, at the first calling, the old pg_resqueue data can't let resource manager works,
    // And also we only need to upgrade master node, no segment is enabled at this time, so no need for resource manager.
    if(gp_upgrade_mode) 
        return 0;
	/*
	 * Prepare temporary signal handler for SIGUSR2 to let postmaster process
	 * able to know when HAWQ RM is ready to accept requests.
	 */
	DRMStartup = false;
	pqsignal(SIGUSR2, notifyPostmasterResManagerStarted);
	PG_SETMASK(&UnBlockSig);

	/* Fork resource manager process. */
    switch ((ResMgrPID = fork_process()))
    {
        case -1:
        	pqsignal(SIGUSR2, SIG_IGN);
        	PG_SETMASK(&BlockSig);
            ereport(FATAL,
                    (errmsg("could not fork resource manager collector: %m")));
            return 0;
        case 0:
        	pqsignal(SIGUSR2, SIG_IGN);
            /* in postmaster child ... */
            /* Close the postmaster's sockets */
            ClosePostmasterPorts(false);

            /* Lose the postmaster's on-exit routines */
            on_exit_reset();

            ResourceManagerIsForked = true;

            switch(rmrole) {
            case START_RM_ROLE_MASTER:
            	arguments[2] = "server";
            	proc_exit(ResManagerMain(3, arguments));
            	break;
            case START_RM_ROLE_SEGMENT:
            	arguments[2] = "segment";
            	proc_exit(ResManagerMain(3, arguments));
            	break;
            }

            break;

        default:
        {
        	/*******************************************************************
        	 * IN POSTMASTER. Postmaster here waits for the signal SIGUSR2 sent
        	 * from HAWQ RM to gurantee that HAWQ RM can accept the request after
        	 * the return of this function.
        	 ******************************************************************/
        	/* wait to ensure that resource manager works basically. */
        	for ( int i = 0 ; i < 180 ; ++i ) {
        		if ( usleep(1000000) != 0 )
        			i--;
        		elog(LOG, "Wait for HAWQ RM %d", i);
        		if ( DRMStartup )
        			break;
        	}
        	pqsignal(SIGUSR2, SIG_IGN);
        	PG_SETMASK(&BlockSig);

        	if ( DRMStartup ) {
        		elog(LOG, "HAWQ :: Received signal notification that HAWQ RM "
        				  "works now.");
        	}
        	else {
        		elog(FATAL, "HAWQ RM can not work. Please check HAWQ RM log.");
        	}
            return ResMgrPID;
        }
    }

    /* shouldn't get here */
    return 0;
}

int ResManagerMainServer2ndPhase(void)
{
	int res = FUNC_RETURN_OK;

	/* Register message handlers */
	registerMessageHandler(REQUEST_QD_CONNECTION_REG        , handleRMRequestConnectionReg);
	registerMessageHandler(REQUEST_QD_CONNECTION_REG_OID    , handleRMRequestConnectionRegByOID);
	registerMessageHandler(REQUEST_QD_CONNECTION_UNREG      , handleRMRequestConnectionUnReg);
	registerMessageHandler(REQUEST_QD_ACQUIRE_RESOURCE      , handleRMRequestAcquireResource);
	registerMessageHandler(REQUEST_QD_RETURN_RESOURCE       , handleRMRequestReturnResource);
	registerMessageHandler(REQUEST_RM_IMALIVE               , handleRMSEGRequestIMAlive);
	registerMessageHandler(REQUEST_QD_DDL_MANIPULATERESQUEUE, handleRMDDLRequestManipulateResourceQueue);
	registerMessageHandler(REQUEST_QD_DDL_MANIPULATEROLE	, handleRMDDLRequestManipulateRole);
	registerMessageHandler(REQUEST_QD_ACQUIRE_RESOURCE_QUOTA, handleRMRequestAcquireResourceQuota);
	registerMessageHandler(REQUEST_QD_REFRESH_RESOURCE      , handleRMRequestRefreshResource);
	registerMessageHandler(REQUEST_QD_SEGMENT_ISDOWN        , handleRMRequestSegmentIsDown);
	registerMessageHandler(REQUEST_QD_DUMP_STATUS           , handleRMRequestDumpStatus);
    registerMessageHandler(REQUEST_QD_DUMP_RESQUEUE_STATUS  , handleRMRequestDumpResQueueStatus);
	registerMessageHandler(REQUEST_DUMMY                    , handleRMRequestDummy);
	registerMessageHandler(REQUEST_QD_QUOTA_CONTROL 		, handleRMRequestQuotaControl);
	/* New socket facility poll based server.*/
	res = initializeSocketServer();
	if ( res != FUNC_RETURN_OK ) {
		elog(FATAL, "Fail to initialize socket server.");
	}

	/*
	 * Notify postmaster that HAWQ RM is ready. Ignore the possible problem that
	 * the parent process quits. HAWQ RM will automatically detect if its parent
	 * dies, then HAWQ RM should exit normally.
	 */
	kill(DRMGlobalInstance->ParentPID, SIGUSR2);

	/* Clean up gp_segment_configuration table. */
	cleanup_segment_config();

	cleanup_segment_config_history();

	/*
	 * register itself into gp_segment_configuration table
	 * master internal id is 0, segment id starts from 1
	 */
	add_segment_config_row(MASTER_ORDER_ID,
                           DRMGlobalInstance->SocketLocalHostName.Str,
                           DRMGlobalInstance->SocketLocalHostName.Str,
                           PostPortNumber,
                           SEGMENT_ROLE_MASTER_CONFIG,
                           SEGMENT_STATUS_UP,
                           "");

	/* Load queue and user definition as no DDL now. */
	res = loadAllQueueAndUser();
	if ( res != FUNC_RETURN_OK ) {
		elog(FATAL, "Fail to load queue and user definition.");
	}

	elog(DEBUG5, "HAWQ RM :: passed loading queue and user definition.");

	/* Check slaves file firstly to ensure we have expected cluster size. */
	checkSlavesFile();

	if ( rm_resourcepool_test_filename != NULL &&
		 rm_resourcepool_test_filename[0] != '\0' ) {
		loadHostInformationIntoResourcePool();
	}

	/******* TILL NOW, resource manager starts providing services *******/
	elog(LOG, "HAWQ RM process works now.");

    /* Start request handler to provide services. */
    res = MainHandlerLoop();
    /* res is returned to the caller. */


    /******* TILL NOW, resource manager goes into EXIT PHASE *******/
    elog(LOG, "HAWQ RM server goes into exit phase.");

    /* Shutdown resource broker. */
    RB_stop();

    return res;
}

/**
 * The main loop of HAWQ RM process.
 */
int MainHandlerLoop(void)
{
	int res = FUNC_RETURN_OK;

	DRMGlobalInstance->ResourceManagerStartTime = gettime_microsec();

	while( DRMGlobalInstance->ResManagerMainKeepRun )
	{
		/* STEP 0. Check if postmaster process exists. */
		if (!PostmasterIsAlive(true)) {
			DRMGlobalInstance->ResManagerMainKeepRun = false;
			elog(LOG, "Postmaster is not alive, resource manager exits");
			break;
		}

		/* STEP 1. Check resource broker status. */
		RB_start(true);

		/* STEP 2. Check if resource manager is in clean up status. */
		if ( isCleanGRMResourceStatus() )
		{
			/*
			 * Fisrtly we move all currently accepted GRM containers into resource
			 * pool. We will drop them at once.
			 */
			moveAllAcceptedGRMContainersToResPool();

			/*
			 * Mark all current active connection tracks that they are in fact
			 * using old application's resource. When resource is returned, the
			 * resource is calculated in old resource level.
			 */
			setAllAllocatedResourceInConnectionTracksOld();

			/* Validate that all in-use resource are marked to old resource. */
			Assert(isAllResourceQueueIdle());

			/*
			 * Reset deadlock detector. The deadlock detector works for new
			 * resource only.
			 */
			resetAllDeadLockDetector();

			/*
			 * Mark all segments DOWN due to no GRM cluster report
			 */
			setAllNodesGRMDown();

			/* Move all resource broker allocated GRM containers to returned. */
			cleanupAllGRMContainers();

			/* Refresh resource queue resource usage and request quota. */
			refreshMemoryCoreRatioLevelUsage(gettime_microsec());

			resetAllSegmentsGRMContainerFailAllocCount();
			resetAllSegmentsNVSeg();

			/* Check if can resume using new available global resource manager.*/
			if ( cleanedAllGRMContainers() )
			{
				unsetCleanGRMResourceStatus();
			}
		}

		/*
		 * STEP 3. Process all communications between resource manager and
		 * 		   resource broker.
		 */
		processResourceBrokerTasks();

		/* STEP 4. Handle socket server inputs. */
		res = processAllCommFileDescs();
		if ( res != FUNC_RETURN_OK )
		{
			/*
			 * The possible error here is the failure of poll(), we won't keep
			 * running HAWQ RM any longer, graceful quit is requested.
			 */
			elog(WARNING, "System error cause resource manager not possible to "
						  "track network communications. Exit resource manager "
						  "process normally.");
			DRMGlobalInstance->ResManagerMainKeepRun = false;
		}

		/* STEP 5. Handle all submitted requests through socket clients. */
		processSubmittedRequests();

		/* STEP 6. Generate possible resource request to resource broker. */
		generateResourceRequestToResourceBroker();

        /* STEP 7. Check timeout resource allocation and timeout queuing requests. */
        timeoutDeadResourceAllocation();
        timeoutQueuedRequest();

		/*
		 * STEP 8. Check the status of all segment nodes, mark down if hasn't got
		 * 		   IMAlive message for a pre-defined period.
		 */
        uint64_t curtime = gettime_microsec();
		if ((rm_resourcepool_test_filename == NULL ||
			rm_resourcepool_test_filename[0] == '\0') &&
			(curtime - PRESPOOL->LastCheckTime >
        	 1000000LL * rm_segment_heartbeat_timeout))
		{
			updateStatusOfAllNodes();
			PRESPOOL->LastCheckTime = curtime;
		}


		/* STEP 9. Move all accepted GRM containers into resource pool. */
		moveAllAcceptedGRMContainersToResPool();

		/*
		 * STEP 10. Check if should pause dispatching resource to queries to
		 * collect resource back in order to return GRM containers.
		 */
		if ( DRMGlobalInstance->ImpType != NONE_HAWQ2 &&
			 PRESPOOL->AddPendingContainerCount == 0 &&
			 PRESPOOL->RetPendingContainerCount == 0 &&
			 PQUEMGR->ForcedReturnGRMContainerCount == 0 &&
			 PQUEMGR->GRMQueueCurCapacity > 1 &&
			 PQUEMGR->GRMQueueResourceTight )
		{
			elog(LOG, "Resource manager decides to breathe out resource. "
					  "Current relative GRM queue capacity %lf, "
					  "Expect GRM queue capacity %lf, "
					  "Estimae GRM queue %s",
					  PQUEMGR->GRMQueueCurCapacity,
					  PQUEMGR->GRMQueueCapacity,
					  PQUEMGR->GRMQueueResourceTight ? "busy" : "not busy");

			/* Calculate how many GRM containers should be returned. */
			setForcedReturnGRMContainerCount();
		}

		/* STEP 11. Dispatch resource to queries and send the messages out.*/

        if ( PRESPOOL->Segments.NodeCount > 0 && PQUEMGR->RatioCount > 0 &&
			 PQUEMGR->toRunQueryDispatch &&
			 PQUEMGR->ForcedReturnGRMContainerCount == 0 &&
			 PRESPOOL->SlavesHostCount > 0 )
        {
    		dispatchResourceToQueries();
        }
        else if ( PQUEMGR->ForcedReturnGRMContainerCount > 0 )
        {
        	forceReturnGRMResourceToRB();
        }
        else
        {
        	elog(DEBUG3, "Loop dummy. %d, %d, %d, %d",
        				 PRESPOOL->Segments.NodeCount,
						 PQUEMGR->RatioCount,
						 PQUEMGR->toRunQueryDispatch ? 1 : 0,
						 PQUEMGR->ForcedReturnGRMContainerCount);
        }

        /* STEP 12. Generate output content to client connections. */
        sendResponseToClients();

        /*
         * STEP 13. Return containers to global resource manager if there some
         * 			some idle resource.
         */
        timeoutIdleGRMResourceToRB();

		/* STEP 14. Notify segments to increase resource quota. */
		notifyToBeAcceptedGRMContainersToRMSEG();

        /* STEP 15. Notify segments to decrease resource. */
        notifyToBeKickedGRMContainersToRMSEG();

        /*
         * STEP 16. Check slaves file if the content is not checked or is
         * 			updated.
         */
        checkSlavesFile();
	}

	elog(LOG, "Resource manager main event handler exits.");

	return res;
}

/**
 * Parse passed in command line arguments. The acceptable argument keys and
 * values are all listed below which are locally used in parseCommandLine().
 */

/* -role + rolename */
#define HAWQDRM_COMMANDLINE_ROLE				"-role"

#define HAWQDRM_COMMANDLINE_ROLE_VALUE_SERVER   "server"
#define HAWQDRM_COMMANDLINE_ROLE_VALUE_SEGMENT  "segment"

int parseCommandLine(int argc, char **argv)
{
	int 		res = FUNC_RETURN_OK;
	SimpString 	value;

	initSimpleString(&value, PCONTEXT);
	for ( int i = 0 ; i < argc ; ++i ) {
		if ( strcmp(argv[i], HAWQDRM_COMMANDLINE_ROLE) == 0 ) {
			res = getStringValue(argc, argv, i+1, &value);
			if ( res != FUNC_RETURN_OK ) {
				elog( WARNING,
					  "Wrong command line argument %s behind %s.",
					  value.Str,
					  HAWQDRM_COMMANDLINE_ROLE);
				goto exit;
			}

			if ( SimpleStringComp(&value,
					 	 	 	  HAWQDRM_COMMANDLINE_ROLE_VALUE_SERVER) == 0 ) {
				DRMGlobalInstance->Role = START_RM_ROLE_MASTER;
			}
			else if ( SimpleStringComp(&value,
					  	  	  	  	   HAWQDRM_COMMANDLINE_ROLE_VALUE_SEGMENT) == 0 ) {
				DRMGlobalInstance->Role = START_RM_ROLE_SEGMENT;
			}
			else {
				elog(WARNING, "Wrong argument value '%s' for %s",
							  value.Str, HAWQDRM_COMMANDLINE_ROLE);
				res = MAIN_WRONG_COMMANDLINE;
			}

			elog(DEBUG5, "HAWQ RM :: Command line sets RM role: %d\n",
						 DRMGlobalInstance->Role);
		}
	}

exit:
	freeSimpleStringContent(&value);
	return res;
}

int getStringValue(int argc, char **argv, int pos, SimpString *val)
{
	if ( pos >= argc )
		return MAIN_WRONG_COMMANDLINE;
	setSimpleStringNoLen(val, argv[pos]);
	return FUNC_RETURN_OK;
}

int createDRMInstance(void)
{
	int res = FUNC_RETURN_OK;

	/* Create dynamic resource manager global instance holding all data. */
    DRMGlobalInstance = NULL;
	DRMGlobalInstance = (DynRMGlobal)
						rm_palloc0(TopMemoryContext,
								   sizeof(struct DynRMGlobalData));
	Assert( DRMGlobalInstance != NULL );
	return res;
}

/*
 * Initialize the dynamic resource manager instance. Return 0 if succeed, other-
 * wise, -1 is returned. Resource manager's memory context is created as one
 * child context which is referenced by DRMGlobalInstance->Context. 
 *
 *         TopMemoryContext ( global instance is allocated here )
 *                 |
 *                 V
 *      Resource manager context ( all consequent objects are allocated here )
 *
 */
int createDRMMemoryContext(void)
{
	int res = FUNC_RETURN_OK;

	Assert( DRMGlobalInstance != NULL );

	/* Create dynamic resource manager memory context,the new memory context is
	   created as the child of top memory context. */
	DRMGlobalInstance->Context = NULL;

	DRMGlobalInstance->Context = AllocSetContextCreate(
                                            TopMemoryContext,
										  	DRMGLOBAL_MEMORY_CONTEXT_NAME,
											ALLOCSET_DEFAULT_MINSIZE,
											ALLOCSET_DEFAULT_INITSIZE,
											ALLOCSET_DEFAULT_MAXSIZE );
	Assert( PCONTEXT != NULL );

	return res;
}

int initializeDRMInstance(MCTYPE context)
{
	int res = FUNC_RETURN_OK;

	DRMGlobalInstance->ResourceQueueManager     = NULL;
	DRMGlobalInstance->ConnTrackManager			= NULL;
	DRMGlobalInstance->ResourcePoolInstance		= NULL;
	DRMGlobalInstance->ImpType					= NONE_HAWQ2;

	DRMGlobalInstance->LocalHostLastUpdateTime	= 0;
	DRMGlobalInstance->HeartBeatLastSentTime    = 0;
	DRMGlobalInstance->TmpDirLastCheckTime      = 0;
	DRMGlobalInstance->LocalHostStat			= NULL;
	
    initializeDQueue(&(DRMGlobalInstance->LocalHostTempDirectories),   context);
    DRMGlobalInstance->LocalHostFailedTmpDirList = NULL;

    HASHCTL ctl;
    ctl.keysize                                 = sizeof(TmpDirKey);
    ctl.entrysize                               = sizeof(TmpDirEntry);
    ctl.hcxt                                    = context;

	/* Tell the working threads keep running. */
	DRMGlobalInstance->ResManagerMainKeepRun 	= true;

	/* Whether send IMAlive message */
	DRMGlobalInstance->SendIMAlive              = true;

	/* Where to send the heart-beat. */
	DRMGlobalInstance->SendToStandby            = false;

	DRMGlobalInstance->ResBrokerAppTimeStamp    = 0;
	DRMGlobalInstance->ResBrokerTriggerCleanup  = false;

	/* Get local host name here to make all components able to use this info. */
	initSimpleString(&(DRMGlobalInstance->SocketLocalHostName), 	   context);
	res = getLocalHostName(&(DRMGlobalInstance->SocketLocalHostName));
	if ( res != FUNC_RETURN_OK ) {
		elog(WARNING, "Fail to get local host name.");
	}

	/* Set resource manager server startup time to 0, i.e. not started yet. */
	DRMGlobalInstance->ResourceManagerStartTime = 0;

	return res;
}

int initializeDRMInstanceForQD(void)
{
	int res = FUNC_RETURN_OK;

	/* Get local host name here to make all components able to use this info. */
	initSimpleString(&(DRMGlobalInstance->SocketLocalHostName),
			         DRMGlobalInstance->Context);
	res = getLocalHostName(&(DRMGlobalInstance->SocketLocalHostName));
	if ( res != FUNC_RETURN_OK ) {
		elog(WARNING, "Fail to get local host name.");
	}
	return res;
}

#define DRMQE2RM_MEMORY_CONTEXT_NAME  "QE to RM communication"

bool 		  QE2RM_Initialized = false;
MemoryContext QE2RM_CommContext	= NULL;

void initializeDRMInstanceForQE(void)
{
	if ( QE2RM_Initialized )
		return;

	int res = FUNC_RETURN_OK;

    /* create dynamic resource manager instance to contain config data. */
    res = createDRMInstance();
    if ( res != FUNC_RETURN_OK ) {
    	elog(ERROR, "Fail to initialize data structure for communicating with "
                	"resource manager.");
    }

    MEMORY_CONTEXT_SWITCH_TO(TopMemoryContext)
    QE2RM_CommContext = AllocSetContextCreate( CurrentMemoryContext,
            DRMQE2RM_MEMORY_CONTEXT_NAME,
            ALLOCSET_DEFAULT_MINSIZE,
            ALLOCSET_DEFAULT_INITSIZE,
            ALLOCSET_DEFAULT_MAXSIZE );
    Assert( QE2RM_CommContext != NULL );
    MEMORY_CONTEXT_SWITCH_BACK

	DRMGlobalInstance->Context = QE2RM_CommContext;

	/* Get local host name here to make all components able to use this info. */
	initSimpleString(&(DRMGlobalInstance->SocketLocalHostName),
			         DRMGlobalInstance->Context);
	res = getLocalHostName(&(DRMGlobalInstance->SocketLocalHostName));
	if ( res != FUNC_RETURN_OK ) {
		elog(WARNING, "Fail to get local host name.");
	}

	/****** Resource enforcement GUCs begins ******/

	/* Get resource enforcement enablement flag */
	DRMGlobalInstance->ResourceEnforcerCpuEnable = rm_enforce_cpu_enable;

	/* Get resource enforcement CGroup mount point */
	initSimpleString(&(DRMGlobalInstance->ResourceEnforcerCgroupMountPoint),
			 PCONTEXT);
	setSimpleStringNoLen(&(DRMGlobalInstance->ResourceEnforcerCgroupMountPoint),
			     rm_enforce_cgrp_mnt_pnt);

	/* Get resource enforcement CGroup hierarchy name */
	initSimpleString(&(DRMGlobalInstance->ResourceEnforcerCgroupHierarchyName),
			 PCONTEXT);
	setSimpleStringNoLen(&(DRMGlobalInstance->ResourceEnforcerCgroupHierarchyName),
			     rm_enforce_cgrp_hier_name);

	/* Get resource enforcement mapping between hawq weight and yarn weight */
	DRMGlobalInstance->ResourceEnforcerCpuWeight = rm_enforce_cpu_weight;

	/* Get resource enforcement vcore-pcore ratio */
	DRMGlobalInstance->ResourceEnforcerVcorePcoreRatio = rm_enforce_core_vpratio;

	/* Get resource enforcement CGroup cleanup period */
	DRMGlobalInstance->ResourceEnforcerCleanupPeriod = rm_enforce_cleanup_period;

	/****** Resource enforcement GUCs ends ******/

    QE2RM_Initialized = true;
}

/**
 * Initialize resource manager core structure to get ready for services.
 */
void initializeDRMCore(void)
{
	/* Initialize resource queue management part. */
	DRMGlobalInstance->ResourceQueueManager =
			(DynResourceQueueManager)
			rm_palloc0(PCONTEXT, sizeof(DynResourceQueueManagerData));
	initializeResourceQueueManager();

	/* Initialize connection track manager part. */
	DRMGlobalInstance->ConnTrackManager =
			(ConnectionTrackManager)
			rm_palloc0(PCONTEXT, sizeof(ConnectionTrackManagerData));
	initializeConnectionTrackManager();

	/* Initialize node resource management part. */
	PRESPOOL = (ResourcePool) rm_palloc0(PCONTEXT, sizeof(ResourcePoolData));
	initializeResourcePoolManager();
}

static void InitTemporaryDirs(DQueue tmpdirs_list, char *tmpdirs_string)
{
    int        templocation = -1;
	SimpString tmpdirs;
	initSimpleString(&tmpdirs, PCONTEXT);
	setSimpleStringNoLen(&tmpdirs, tmpdirs_string);
	while (FUNC_RETURN_OK == SimpleStringLocateChar(&tmpdirs, ',', &templocation))
	{
		SimpStringPtr tempdir = createSimpleString(PCONTEXT);
		SimpleStringSubstring(&tmpdirs, 0, templocation, tempdir);
		insertDQueueTailNode(tmpdirs_list, tempdir);

		SimpString tmpstr;
		initSimpleString(&tmpstr, PCONTEXT);
		SimpleStringSubstring(&tmpdirs, templocation+1, -1, &tmpstr);
		freeSimpleStringContent(&tmpdirs);
		SimpleStringCopy(&tmpdirs, &tmpstr);
		freeSimpleStringContent(&tmpstr);
	}

	SimpStringPtr tempdirlast = createSimpleString(PCONTEXT);
	setSimpleStringWithContent(tempdirlast, tmpdirs.Str, tmpdirs.Len);
	insertDQueueTailNode(tmpdirs_list, tempdirlast);
	freeSimpleStringContent(&tmpdirs);
}


/**
 * Read loaded config properties and fill the verified values into global
 * instance.
 */
int  loadDynamicResourceManagerConfigure(void)
{
	elog(DEBUG3, "Resource manager loads Socket Listening Port %d",
				 rm_master_port);
	elog(DEBUG3, "Resource manager loads Segment Socket Listening Port %d",
				 rm_segment_port);

	/* Decide global resource manager mode. */
	if ( strcasecmp(rm_global_rm_type, HAWQDRM_CONFFILE_SVRTYPE_VAL_YARN) == 0 )
	{
		DRMGlobalInstance->ImpType = YARN_LIBYARN;
	}
	else if ( strcasecmp(rm_global_rm_type, HAWQDRM_CONFFILE_SVRTYPE_VAL_NONE) == 0 )
	{
		DRMGlobalInstance->ImpType = NONE_HAWQ2;
	}
	else
	{
		elog(WARNING, "Wrong global resource manager type set in %s.",
				  	  HAWQDRM_CONFFILE_SERVER_TYPE);
		return MAIN_CONF_UNSET_ROLE;
	}
	elog(DEBUG3, "Resource manager loads resource broker implement mode : %d",
				 DRMGlobalInstance->ImpType);

	SimpString segmem;
	if ( rm_seg_memory_use[0] == '\0' )
	{
		elog(WARNING, "%s is not set", HAWQDRM_CONFFILE_LIMIT_MEMORY_USE);
		return MAIN_CONF_UNSET_SEGMENT_MEMORY_USE;
	}

	setSimpleStringRefNoLen(&segmem, rm_seg_memory_use);
	int res = SimpleStringToStorageSizeMB(&segmem,
										  &(DRMGlobalInstance->SegmentMemoryMB));
	if ( res != FUNC_RETURN_OK)
	{
		elog(WARNING, "Can not understand the value '%s' of property %s.",
				  	  rm_seg_memory_use,
					  HAWQDRM_CONFFILE_LIMIT_MEMORY_USE);
		return MAIN_CONF_UNSET_SEGMENT_MEMORY_USE;
	}

	DRMGlobalInstance->SegmentCore = rm_seg_core_use;

	elog(DEBUG3, "HAWQ RM :: Accepted NONE mode resource management setting, "
				 "each host has (%d MB,%lf) resource capacity.\n",
				 DRMGlobalInstance->SegmentMemoryMB,
				 DRMGlobalInstance->SegmentCore);

    // For temporary directories
    InitTemporaryDirs(&DRMGlobalInstance->LocalHostTempDirectories, rm_seg_tmp_dirs);

	DQUEUE_LOOP_BEGIN(&DRMGlobalInstance->LocalHostTempDirectories, iter, SimpStringPtr, value)
		elog(LOG, "HAWQ Segment RM :: Temporary directory %s", value->Str);
	DQUEUE_LOOP_END

	checkAndBuildFailedTmpDirList();

	/****** Resource enforcement GUCs begins ******/

	/* Get resource enforcement enablement flag */
	DRMGlobalInstance->ResourceEnforcerCpuEnable = rm_enforce_cpu_enable;

	/* Get resource enforcement CGroup mount point */
	initSimpleString(&(DRMGlobalInstance->ResourceEnforcerCgroupMountPoint),
			 PCONTEXT);
	setSimpleStringNoLen(&(DRMGlobalInstance->ResourceEnforcerCgroupMountPoint),
			     rm_enforce_cgrp_mnt_pnt);

	/* Get resource enforcement CGroup hierarchy name */
	initSimpleString(&(DRMGlobalInstance->ResourceEnforcerCgroupHierarchyName),
			 PCONTEXT);
	setSimpleStringNoLen(&(DRMGlobalInstance->ResourceEnforcerCgroupHierarchyName),
			     rm_enforce_cgrp_hier_name);

	/* Get resource enforcement mapping between hawq weight and yarn weight */
	DRMGlobalInstance->ResourceEnforcerCpuWeight = rm_enforce_cpu_weight;

	/* Get resource enforcement vcore-pcore ratio */
	DRMGlobalInstance->ResourceEnforcerVcorePcoreRatio = rm_enforce_core_vpratio;

	/* Get resource enforcement CGroup cleanup period */
	DRMGlobalInstance->ResourceEnforcerCleanupPeriod = rm_enforce_cleanup_period;

	/****** Resource enforcement GUCs ends ******/
    return FUNC_RETURN_OK;
}

/*
 * Load resource queue and user information into HAWQ RM memory. If independent
 * mode, the definition is from xml file saving a list of properties. If normal
 * forked RM process, this can be loaded from catalog table: pg_authid and
 * pg_resqueue.
 *
 */
int  loadAllQueueAndUser(void)
{
	int   res 		 = FUNC_RETURN_OK;
	List *queueprops = NULL;			/* Save all queue properties.    */
	List *userprops  = NULL;			/* Save all role/user properties.*/

	res = loadUserPropertiesFromCatalog(&userprops);
	if ( res != FUNC_RETURN_OK )
	{
		goto exit;
	}

	res = loadQueuePropertiesFromCatalog(&queueprops);
	if ( res != FUNC_RETURN_OK )
	{
		goto exit;
	}

	/* Add resource queue and user into resource pool data structure. */
	res = addResourceQueueAndUserFromProperties(queueprops, userprops);
	/* res is checked before returning. */
	Assert(PQUEMGR->RootTrack != NULL);
	Assert(PQUEMGR->DefaultTrack != NULL);

exit:
	cleanPropertyList(PCONTEXT, &queueprops);
	cleanPropertyList(PCONTEXT, &userprops);

	if ( res != FUNC_RETURN_OK )
	{
		elog( LOG, "Fail to load queue and user definition.");
	}
	return res;
}

/*****************************************************************************
 * Load user information from catalog table pg_authid.
 *****************************************************************************/
int	 loadUserPropertiesFromCatalog(List **users)
{
	int			libpqres 		= CONNECTION_OK;
	int 		ret 	 		= FUNC_RETURN_OK;
	PGconn 	   *conn 	 		= NULL;
    static char conninfo[1024];
	PQExpBuffer sql 	 		= NULL;
	PGresult   *result 	 		= NULL;
	int 		ntups 	 		= 0;
	int 		i_oid 	 		= 0,
				i_rolname 		= 0,
				i_rolsuper 		= 0,
				i_rolresqueue 	= 0;

	Oid 		roloid 			= 0,
				rolresqueueOID  = 0;

	char 	   *rolname 		= NULL,
			   *rolresqueue 	= NULL;
	bool 		rolsuper 		= false;
	int8_t 		DummyPriority 	= 3;

	sprintf(conninfo, "options='-c gp_session_role=UTILITY' "
					  "dbname=template1 port=%d connect_timeout=%d",
					  master_addr_port,
					  LIBPQ_CONNECT_TIMEOUT);

	conn = PQconnectdb(conninfo);
	if ((libpqres = PQstatus(conn)) != CONNECTION_OK)
	{
		elog(WARNING, "Resource manager failed to connect database when loading"
					  "role specifications from pg_authid, error code %d, "
					  "reason: %s",
					  libpqres,
					  PQerrorMessage(conn));
		PQfinish(conn);
		return LIBPQ_FAIL_EXECUTE;
	}

	sql = createPQExpBuffer();
	if ( sql == NULL )
	{
		elog(WARNING, "Resource manager failed to allocate buffer for building "
					  "sql statement.");
		goto cleanup;
	}
	appendPQExpBuffer(sql, "SELECT oid,rolname,rolsuper,rolresqueue FROM pg_authid");
	result = PQexec(conn, sql->data);
	if (!result || PQresultStatus(result) != PGRES_TUPLES_OK)
	{
		elog(WARNING, "Resource manager failed to run SQL: %s "
					  "when loading role specifications from pg_authid, "
					  "reason : %s",
					  sql->data,
					  PQresultErrorMessage(result));
		ret = LIBPQ_FAIL_EXECUTE;
		goto cleanup;
	}

	ntups 		  = PQntuples(result);
	i_oid 		  = PQfnumber(result, "oid");
	i_rolname 	  = PQfnumber(result, "rolname");
	i_rolsuper 	  = PQfnumber(result, "rolsuper");
	i_rolresqueue = PQfnumber(result, "rolresqueue");

	for (int i = 0; i < ntups; i++)
	{
	    roloid 		= (Oid)strtoul(PQgetvalue(result, i, i_oid), NULL, 10);
	    rolname 	= PQgetvalue(result, i, i_rolname);
	    rolresqueue = PQgetvalue(result, i, i_rolresqueue);

	    if (rolresqueue == NULL || strlen(rolresqueue) == 0)
	    {
	    	rolresqueueOID = InvalidOid;
	    }
	    else
	    {
	    	rolresqueueOID = (Oid)strtoul(PQgetvalue(result, i, i_rolresqueue), NULL, 10);
	    }

	    if (PQgetvalue(result, i, i_rolsuper)[0] == 't')
	    {
			rolsuper = true;
	    }
	    else
	    {
	    	rolsuper = false;
	    }

	    MEMORY_CONTEXT_SWITCH_TO(PCONTEXT)
	    *users = lappend(*users,
	    				 createPropertyOID(
	    					 PCONTEXT,
							 "user",
							 getUSRTBLAttributeName(USR_TBL_ATTR_OID),
							 &i,
							 roloid));
	    *users = lappend(*users,
	    				 createPropertyName(
	    					 PCONTEXT,
							 "user",
							 getUSRTBLAttributeName(USR_TBL_ATTR_NAME),
							 &i,
							 (Name)rolname));
	    *users = lappend(*users,
	    				 createPropertyOID(
	    					 PCONTEXT,
							 "user",
							 getUSRTBLAttributeName(USR_TBL_ATTR_TARGET_QUEUE),
							 &i,
							 rolresqueueOID));
	    *users = lappend(*users,
	    				 createPropertyBool(
	    					 PCONTEXT,
							 "user",
							 getUSRTBLAttributeName(USR_TBL_ATTR_IS_SUPERUSER),
							 &i,
							 rolsuper));
	    *users = lappend(*users,
	    				 createPropertyInt8(
	    					 PCONTEXT,
							 "user",
							 getUSRTBLAttributeName(USR_TBL_ATTR_PRIORITY),
							 &i,
							 DummyPriority));
	    MEMORY_CONTEXT_SWITCH_BACK
	}

	ListCell *cell = NULL;
	foreach(cell, *users)
	{
		KVProperty property = lfirst(cell);
		elog(RMLOG, "Resource manager loaded role specifications from pg_authid : "
				    "[%s]=[%s]",
					property->Key.Str,
					property->Val.Str);
	}
	elog(LOG, "Resource manager successfully loaded role specifications.");

cleanup:
	if(sql != NULL)
	{
		destroyPQExpBuffer(sql);
	}
	if(result != NULL)
	{
		PQclear(result);
	}
	PQfinish(conn);
	return ret;
}

/*****************************************************************************
 * Load resource queue information from catalog table pg_resqueue.
 *****************************************************************************/
int	 loadQueuePropertiesFromCatalog(List **queues)
{
	int		 	 libpqres 			= CONNECTION_OK;
	int 	 	 ret 	 			= FUNC_RETURN_OK;
	PGconn 		*conn 				= NULL;
	static char  conninfo[1024];
	PQExpBuffer  sql 				= NULL;
	PGresult* result 				= NULL;
	int ntups 						= 0;
	int i_oid 						= 0,
		i_name 						= 0,
		i_parent 					= 0,
		i_active_stats_cluster  	= 0,
		i_memory_limit_cluster 		= 0,
		i_core_limit_cluster 		= 0,
		i_resource_overcommit 		= 0,
		i_allocation_policy 		= 0,
		i_vseg_resource_quota 		= 0,
		i_nvseg_upper_limit			= 0,
		i_nvseg_lower_limit			= 0,
		i_nvseg_upper_limit_perseg 	= 0,
		i_nvseg_lower_limit_perseg 	= 0,
		i_creation_time 			= 0,
		i_update_time 				= 0,
		i_status 					= 0;

	Oid oid 						= 0,
		parentoid 					= 0;

	char *name 						= NULL,
		 *parent 					= NULL,
		 *memory_limit_cluster 		= NULL,
		 *core_limit_cluster 		= NULL,
		 *allocation_policy 		= NULL,
		 *vseg_resource_quota 		= NULL,
		 *status 					= NULL;

	int active_stats_cluster 		= 0,
		nvseg_upper_limit			= 0,
		nvseg_lower_limit			= 0;

	float nvseg_upper_limit_perseg	= 0.0,
		  nvseg_lower_limit_perseg	= 0.0,
		  resource_overcommit		= 0.0;

	int64 creation_time 			= 0,
		  update_time 				= 0;

	snprintf(conninfo, sizeof(conninfo),
			 "options='-c gp_session_role=UTILITY' "
			 "dbname=template1 port=%d connect_timeout=%d",
			 master_addr_port,
			 LIBPQ_CONNECT_TIMEOUT);

	conn = PQconnectdb(conninfo);
	if ((libpqres = PQstatus(conn)) != CONNECTION_OK)
	{
		elog(WARNING, "Resource manager failed to connect database when loading "
					  "resource queue specifications from pg_resqueue, "
					  "error code: %d, reason: %s",
					  libpqres,
					  PQerrorMessage(conn));
		PQfinish(conn);
		return LIBPQ_FAIL_EXECUTE;
	}

	sql = createPQExpBuffer();
	if ( sql == NULL )
	{
		elog(WARNING, "Resource manager failed to allocate buffer for building "
					  "sql statement.");
		goto cleanup;
	}

	appendPQExpBuffer(sql,"SELECT oid,"
								 "rsqname,"
								 "parentoid,"
								 "activestats,"
								 "memorylimit, "
								 "corelimit, "
								 "resovercommit,"
								 "allocpolicy, "
								 "vsegresourcequota, "
								 "nvsegupperlimit, "
								 "nvseglowerlimit, "
								 "nvsegupperlimitperseg, "
								 "nvseglowerlimitperseg, "
								 "creationtime, "
								 "updatetime, "
								 "status "
								 "FROM pg_resqueue");
	result = PQexec(conn, sql->data);

	if (!result || PQresultStatus(result) != PGRES_TUPLES_OK)
	{
		elog(WARNING, "Resource manager failed to run SQL: %s when loading "
					  "resource queue specifications from pg_resqueue, "
					  "reason : %s",
					  sql->data,
					  PQresultErrorMessage(result));
		ret = LIBPQ_FAIL_EXECUTE;
		goto cleanup;
	}

	ntups = PQntuples(result);

	i_oid 						= PQfnumber(result, PG_RESQUEUE_COL_OID);
	i_name 						= PQfnumber(result, PG_RESQUEUE_COL_RSQNAME);
	i_parent 					= PQfnumber(result, PG_RESQUEUE_COL_PARENTOID);
	i_active_stats_cluster 		= PQfnumber(result, PG_RESQUEUE_COL_ACTIVESTATS);
	i_memory_limit_cluster 		= PQfnumber(result, PG_RESQUEUE_COL_MEMORYLIMIT);
	i_core_limit_cluster  		= PQfnumber(result, PG_RESQUEUE_COL_CORELIMIT);
	i_resource_overcommit 		= PQfnumber(result, PG_RESQUEUE_COL_RESOVERCOMMIT);
	i_allocation_policy 		= PQfnumber(result, PG_RESQUEUE_COL_ALLOCPOLICY);
	i_vseg_resource_quota 		= PQfnumber(result, PG_RESQUEUE_COL_VSEGRESOURCEQUOTA);
	i_nvseg_upper_limit			= PQfnumber(result, PG_RESQUEUE_COL_NVSEGUPPERLIMIT);
	i_nvseg_lower_limit			= PQfnumber(result, PG_RESQUEUE_COL_NVSEGLOWERLIMIT);
	i_nvseg_upper_limit_perseg	= PQfnumber(result, PG_RESQUEUE_COL_NVSEGUPPERLIMITPERSEG);
	i_nvseg_lower_limit_perseg	= PQfnumber(result, PG_RESQUEUE_COL_NVSEGLOWERLIMITPERSEG);
	i_creation_time 			= PQfnumber(result, PG_RESQUEUE_COL_CREATIONTIME);
	i_update_time 				= PQfnumber(result, PG_RESQUEUE_COL_UPDATETIME);
	i_status 					= PQfnumber(result, PG_RESQUEUE_COL_STATUS);

	for (int i = 0; i < ntups; i++)
	{
	    oid 					  = (Oid)strtoul(PQgetvalue(result, i, i_oid), NULL, 10);
	    name 				  	  = 			 PQgetvalue(result, i, i_name);
	    parent 					  = 			 PQgetvalue(result, i, i_parent);
	    if (parent == NULL || strlen(parent) == 0)
	    {
	    	parentoid = InvalidOid;
	    }
	    else
	    {
	    	parentoid = (Oid)strtoul(parent, NULL, 10);
	    }

	    parentoid 			     = (Oid)strtoul(PQgetvalue(result, i, i_parent), NULL, 10);
	    active_stats_cluster     = 		atoi(PQgetvalue(result, i, i_active_stats_cluster));
	    memory_limit_cluster     = 			 PQgetvalue(result, i, i_memory_limit_cluster);
	    core_limit_cluster 	     = 			 PQgetvalue(result, i, i_core_limit_cluster);
	    resource_overcommit      = 		atof(PQgetvalue(result, i, i_resource_overcommit));
	    allocation_policy 	     = 			 PQgetvalue(result, i, i_allocation_policy);
	    vseg_resource_quota      = 			 PQgetvalue(result, i, i_vseg_resource_quota);
	    nvseg_upper_limit	     = 		atoi(PQgetvalue(result, i, i_nvseg_upper_limit));
	    nvseg_lower_limit	     = 		atoi(PQgetvalue(result, i, i_nvseg_lower_limit));
	    nvseg_upper_limit_perseg = 		atof(PQgetvalue(result, i, i_nvseg_upper_limit_perseg));
	    nvseg_lower_limit_perseg = 		atof(PQgetvalue(result, i, i_nvseg_lower_limit_perseg));
	    creation_time 		     = 		atol(PQgetvalue(result, i, i_creation_time));
	    update_time 		     = 		atol(PQgetvalue(result, i, i_update_time));
	    status 				     = 			 PQgetvalue(result, i, i_status);

	    MEMORY_CONTEXT_SWITCH_TO(PCONTEXT)
	    *queues = lappend(*queues,
	    				  createPropertyOID(
	    					  PCONTEXT,
							  "queue",
							  getRSQTBLAttributeName(RSQ_TBL_ATTR_OID),
							  &i,
							  oid));

	    *queues = lappend(*queues,
	    				  createPropertyName(
	    					  PCONTEXT,
							  "queue",
							  getRSQTBLAttributeName(RSQ_TBL_ATTR_NAME),
							  &i,
							  (Name)name));

	    *queues = lappend(*queues,
	    				  createPropertyOID(
	    					  PCONTEXT,
	    					  "queue",
							  getRSQTBLAttributeName(RSQ_TBL_ATTR_PARENT),
							  &i,
							  parentoid));

	    *queues = lappend(*queues,
	    				  createPropertyInt32(
	    					  PCONTEXT,
	 						  "queue",
							  getRSQTBLAttributeName(RSQ_TBL_ATTR_ACTIVE_STATMENTS),
							  &i,
							  active_stats_cluster));

	    *queues = lappend(*queues,
	    				  createPropertyString(
	    					  PCONTEXT,
	 						  "queue",
	 						  getRSQTBLAttributeName(RSQ_TBL_ATTR_MEMORY_LIMIT_CLUSTER),
	 						  &i,
	 						  memory_limit_cluster));

	    *queues = lappend(*queues,
	    				  createPropertyString(
	    					  PCONTEXT,
	 						  "queue",
	 						  getRSQTBLAttributeName(RSQ_TBL_ATTR_CORE_LIMIT_CLUSTER),
	 						  &i,
							  core_limit_cluster));

	    *queues = lappend(*queues,
	    				  createPropertyString(
	    					  PCONTEXT,
	 						  "queue",
	 						  getRSQTBLAttributeName(RSQ_TBL_ATTR_ALLOCATION_POLICY),
	 						  &i,
							  allocation_policy));

	    *queues = lappend(*queues,
	    				  createPropertyFloat(
	    					  PCONTEXT,
							  "queue",
	 						  getRSQTBLAttributeName(RSQ_TBL_ATTR_RESOURCE_OVERCOMMIT_FACTOR),
							  &i,
							  resource_overcommit));

	    *queues = lappend(*queues,
	    				  createPropertyString(
	    					  PCONTEXT,
							  "queue",
	 						  getRSQTBLAttributeName(RSQ_TBL_ATTR_VSEG_RESOURCE_QUOTA),
							  &i,
							  vseg_resource_quota));

	    *queues = lappend(*queues,
	    				  createPropertyInt32(
	    					  PCONTEXT,
	 						  "queue",
							  getRSQTBLAttributeName(RSQ_TBL_ATTR_NVSEG_UPPER_LIMIT),
							  &i,
							  nvseg_upper_limit));

	    *queues = lappend(*queues,
	    				  createPropertyInt32(
	    					  PCONTEXT,
	 						  "queue",
							  getRSQTBLAttributeName(RSQ_TBL_ATTR_NVSEG_LOWER_LIMIT),
							  &i,
							  nvseg_lower_limit));

	    *queues = lappend(*queues,
	    				  createPropertyFloat(
	    					  PCONTEXT,
							  "queue",
	 						  getRSQTBLAttributeName(RSQ_TBL_ATTR_NVSEG_UPPER_LIMIT_PERSEG),
							  &i,
							  nvseg_upper_limit_perseg));

	    *queues = lappend(*queues,
	    				  createPropertyFloat(
	    					  PCONTEXT,
							  "queue",
	 						  getRSQTBLAttributeName(RSQ_TBL_ATTR_NVSEG_LOWER_LIMIT_PERSEG),
							  &i,
							  nvseg_lower_limit_perseg));

	    *queues = lappend(*queues,
	    				  createPropertyInt32(
	    					  PCONTEXT,
	 						  "queue",
							  getRSQTBLAttributeName(RSQ_TBL_ATTR_CREATION_TIME),
							  &i,
							  creation_time));

	    *queues = lappend(*queues,
	    				  createPropertyInt32(
	    					  PCONTEXT,
	 						  "queue",
							  getRSQTBLAttributeName(RSQ_TBL_ATTR_UPDATE_TIME),
							  &i,
							  update_time));

	    *queues = lappend(*queues,
	    				  createPropertyString(
	    					  PCONTEXT,
							  "queue",
	 						  getRSQTBLAttributeName(RSQ_TBL_ATTR_STATUS),
							  &i,
							  status));
		MEMORY_CONTEXT_SWITCH_BACK
	}

	ListCell *cell = NULL;
	foreach(cell, *queues)
	{
		KVProperty property = lfirst(cell);
		elog(RMLOG, "Resource manger loaded resource queue specifications from "
				  	"pg_resqueue : [%s]=[%s]",
					property->Key.Str,
					property->Val.Str);
	}
	elog(LOG, "Resource manger successfully loaded resource queue specifications");

cleanup:
	if(sql != NULL)
	{
		destroyPQExpBuffer(sql);
	}
	if(result != NULL)
	{
		PQclear(result);
	}
	PQfinish(conn);
	return ret;
}


void quitResManager(SIGNAL_ARGS)
{
	DRMGlobalInstance->ResManagerMainKeepRun = false;
}

void handleChildSignal(SIGNAL_ARGS)
{
	/* The resource broker process quits. */
	RB_handleSignalSIGCHLD();
}

void notifyPostmasterResManagerStarted(SIGNAL_ARGS)
{
	DRMStartup = true;
}

/*
 * Read properties and generate corresponding queue and user definitions.
 *
 * properties[in]	: list of KVPropertyData
 * queues[out]		: list of DynResourceQueue
 * users[out]		: list of UserInfo
 */
int addResourceQueueAndUserFromProperties(List *queueprops, List *userprops)
{
	static char	  errorbuf[1024];
	int			  res			= FUNC_RETURN_OK;
	char	     *substrstart	= NULL;
	int			  substrlen		= 0;
	char	     *substr2start	= NULL;
	int 		  substr2len	= 0;
	List 	  	 *queueattrlist = NULL;
	List		 *userattrlist	= NULL;
	ListCell	 *cell			= NULL;

	/*
	 * STEP 1. Process properties and build resource queue and user structure.
	 */
	int   currentindex = -1;
	List *currentattrs = NULL;
	foreach(cell, queueprops)
	{
		KVProperty value = lfirst(cell);

		elog(RMLOG, "Loads queue property %s=%s", value->Key.Str, value->Val.Str);

		/* Split key string into (attribute, index) */
		if ( SimpleStringStartWith(&(value->Key), "queue.") != FUNC_RETURN_OK )
		{
			elog(RMLOG, "Ignore property %s=%s", value->Key.Str, value->Val.Str);
			continue;
		}

		/*
		 *----------------------------------------------------------------------
		 * The key string is formatted as
		 *   queue.<queueattrstr>.<queueindexstr>=<value>
		 *    (0)       (1)            (2)
		 *----------------------------------------------------------------------
		 */
		int32_t		queueindex;
		SimpString	queueattrstr;
		SimpString	queueindexstr;

		/* Fill queueindex, queueattrstr */
		int parseres1 = PropertyKeySubstring(&(value->Key),
											 1,
											 &substrstart,
											 &substrlen);
		int parseres2 = PropertyKeySubstring(&(value->Key),
											 2,
											 &substr2start,
											 &substr2len);

		if( parseres1 != FUNC_RETURN_OK || parseres2 != FUNC_RETURN_OK )
		{
			Assert(false);
		}

		setSimpleStringRef(&queueindexstr, substr2start, substr2len);
		setSimpleStringRef(&queueattrstr, substrstart, substrlen);

		parseres1 = SimpleStringToInt32(&queueindexstr, &queueindex);
		if ( parseres1 != FUNC_RETURN_OK )
		{
			Assert(false);
		}

		/* Fill queue attribute to corresponding list. */

		KVProperty newprop = createPropertyEmpty(PCONTEXT);
		setSimpleStringWithContent(&(newprop->Key), substrstart, substrlen);
		setSimpleStringNoLen(&(newprop->Val), value->Val.Str);

		MEMORY_CONTEXT_SWITCH_TO(PCONTEXT)
		if ( queueindex != currentindex )
		{
			if ( currentattrs != NULL )
			{
				queueattrlist = lappend(queueattrlist, currentattrs);
			}
			currentattrs = NULL;
			currentindex = queueindex;
		}

		elog(RMLOG, "Resource manager loaded attribute for creating queue %s=%s",
				    newprop->Key.Str,
					newprop->Val.Str);

		currentattrs = lappend(currentattrs, newprop);
		MEMORY_CONTEXT_SWITCH_BACK
	}

	if ( currentattrs != NULL )
	{
		MEMORY_CONTEXT_SWITCH_TO(PCONTEXT)
		queueattrlist = lappend(queueattrlist, currentattrs);
		MEMORY_CONTEXT_SWITCH_BACK
	}

	currentindex = -1;
	currentattrs = NULL;

	foreach(cell, userprops)
	{
		KVProperty value = lfirst(cell);

		elog(RMLOG, "Loads user property %s=%s", value->Key.Str, value->Val.Str);

		/* Split key string into (attribute, index) */
		if ( SimpleStringStartWith(&(value->Key), "user.") != FUNC_RETURN_OK )
		{
			elog(RMLOG, "Ignore property %s=%s", value->Key.Str, value->Val.Str);
			continue;
		}

		/*
		 *----------------------------------------------------------------------
		 * The key string is formatted as
		 *   user.<userattrstr>.<userindexstr>=<value>
		 *    (0)      (1)            (2)
		 *----------------------------------------------------------------------
		 */
		int32_t					userindex;
		SimpString				userattrstr;
		SimpString  			userindexstr;

		/* Fill userindex, userattrstr */
		int parseres1 = PropertyKeySubstring(&(value->Key),
											 1,
											 &substrstart,
											 &substrlen);
		int parseres2 = PropertyKeySubstring(&(value->Key),
											 2,
											 &substr2start,
											 &substr2len);

		if ( parseres1 != FUNC_RETURN_OK || parseres2 != FUNC_RETURN_OK )
		{
			Assert(false);
		}

		setSimpleStringRef(&userindexstr, substr2start, substr2len);
		setSimpleStringRef(&userattrstr, substrstart, substrlen);

		parseres1 = SimpleStringToInt32(&userindexstr, &userindex);
		Assert(parseres1 == FUNC_RETURN_OK);

		/* Fill user attribute to corresponding list. */
		KVProperty newprop = createPropertyEmpty(PCONTEXT);
		setSimpleStringWithContent(&(newprop->Key), substrstart, substrlen);
		setSimpleStringNoLen(&(newprop->Val), value->Val.Str);

		MEMORY_CONTEXT_SWITCH_TO(PCONTEXT)
		if ( userindex != currentindex )
		{
			if ( currentattrs != NULL )
			{
				userattrlist = lappend(userattrlist, currentattrs);
			}
			currentattrs = NULL;
			currentindex = userindex;
		}

		elog(RMLOG, "Resource manager loaded attribute for creating role %s=%s",
				    newprop->Key.Str,
					newprop->Val.Str);

		currentattrs = lappend(currentattrs, newprop);
		MEMORY_CONTEXT_SWITCH_BACK
	}

	if ( currentattrs != NULL )
	{
		MEMORY_CONTEXT_SWITCH_TO(PCONTEXT)
		userattrlist = lappend(userattrlist, currentattrs);
		MEMORY_CONTEXT_SWITCH_BACK
	}

	/*
	 * STEP 2. Add resource queues.
	 */

	/* STEP 2.1. Build resource queue object with parsed properties set.     */
	List *rawrsqs = NULL;
	foreach(cell, queueattrlist)
	{
		List 	 *attrs = lfirst(cell);
		ListCell *cell2 = NULL;
		foreach(cell2, attrs)
		{
			KVProperty attrkv = lfirst(cell2);
			elog(RMLOG, "To parse : %s=%s", attrkv->Key.Str, attrkv->Val.Str);
		}

		DynResourceQueue newqueue = rm_palloc0(PCONTEXT,
		   	   	   	   	   	   	   			   sizeof(DynResourceQueueData));

		res = parseResourceQueueAttributes(attrs,
										   newqueue,
										   false,
										   true,
										   errorbuf,
										   sizeof(errorbuf));
		if ( res != FUNC_RETURN_OK )
		{
			rm_pfree(PCONTEXT, newqueue);
			elog(WARNING, "Resource manager can not create resource queue with its "
						  "attributes because %s",
						  errorbuf);
			continue;
		}

		MEMORY_CONTEXT_SWITCH_TO(PCONTEXT)
		rawrsqs = lappend(rawrsqs, newqueue);
		MEMORY_CONTEXT_SWITCH_BACK
	}

	/*
	 * STEP 2.2. Reorder the resource queue sequence to ensure that every time
	 * the queue is added into the manager, its parent queue can be found.
	 */
	List *orderedrsqs = NULL;
	bool orderchanged = true;
	while( orderchanged )
	{
		DynResourceQueue  toreordrsq = NULL;
		ListCell 		 *cell 	     = NULL;
		ListCell 		 *prevcell   = NULL;
		foreach(cell, rawrsqs)
		{
			DynResourceQueue rawqueue = lfirst(cell);
			if ( rawqueue->ParentOID == InvalidOid )
			{
				toreordrsq = rawqueue;
				MEMORY_CONTEXT_SWITCH_TO(PCONTEXT)
				rawrsqs = list_delete_cell(rawrsqs, cell, prevcell);
				MEMORY_CONTEXT_SWITCH_BACK
				break;
			}

			ListCell *cell2 = NULL;
			foreach(cell2, orderedrsqs)
			{
				DynResourceQueue ordqueue = lfirst(cell2);
				if ( ordqueue->OID == rawqueue->ParentOID )
				{
					toreordrsq = rawqueue;
					MEMORY_CONTEXT_SWITCH_TO(PCONTEXT)
					rawrsqs = list_delete_cell(rawrsqs, cell, prevcell);
					MEMORY_CONTEXT_SWITCH_BACK
				}
			}

			if ( toreordrsq != NULL )
			{
				break;
			}
			prevcell = cell;
		}

		orderchanged = false;
		if ( toreordrsq != NULL )
		{
			elog(RMLOG, "Find one resource queue valid to continue loading %s.",
					    toreordrsq->Name);
			orderchanged = true;
			MEMORY_CONTEXT_SWITCH_TO(PCONTEXT)
			orderedrsqs = lappend(orderedrsqs, toreordrsq);
			MEMORY_CONTEXT_SWITCH_BACK
		}
	}

	/*
	 * There is one possibility that we have some resource queues whose parent
	 * queue ids are not present in the pg_resqueue table or external configure
	 * file. In this case, these resource queues are logged out out as warnings,
	 * and HAWQ RM will ignore them.
	 *
	 * Through catalog, this should be impossible.
	 */
	if ( list_length(rawrsqs) > 0 )
	{
		elog(WARNING, "Invalid resource queues are detected due to no valid "
					  "parent resource queue id.");

		while( list_length(rawrsqs) > 0 )
		{
			DynResourceQueue rawqueue = lfirst(list_head(rawrsqs));
			MEMORY_CONTEXT_SWITCH_TO(PCONTEXT)
			rawrsqs = list_delete_first(rawrsqs);
			MEMORY_CONTEXT_SWITCH_BACK
			elog(WARNING, "Invalid resource queue : [%s]", rawqueue->Name);
			rm_pfree(PCONTEXT, rawqueue);
		}
	}

	/*
	 * STEP 2.3. List of DynResourceQueue, save partially filled attributes.
	 * Actually check and complete the resource queue definition and save it
	 * in the resource queue manager.
	 */
	while( list_length(orderedrsqs) > 0 )
	{
		DynResourceQueue partqueue = lfirst(list_head(orderedrsqs));

		MEMORY_CONTEXT_SWITCH_TO(PCONTEXT)
		orderedrsqs = list_delete_first(orderedrsqs);
		MEMORY_CONTEXT_SWITCH_BACK

		elog(RMLOG, "Load queue %s.", partqueue->Name);

		res = checkAndCompleteNewResourceQueueAttributes(partqueue,
														 errorbuf,
														 sizeof(errorbuf));
		if ( res != FUNC_RETURN_OK )
		{
			elog(RMLOG, "res=%d error=%s, after check and complete queue %s.",
						res,
						errorbuf,
						partqueue->Name);

			rm_pfree(PCONTEXT, partqueue);
			elog(WARNING, "Resource manager can not complete resource queue's "
						  "attributes because %s",
						  errorbuf);
			continue;
		}

		elog(RMLOG, "Checked and completed queue %s.", partqueue->Name);

		DynResourceQueueTrack newtrack = NULL;
		res = createQueueAndTrack(partqueue, &newtrack, errorbuf, sizeof(errorbuf));

		if ( res != FUNC_RETURN_OK )
		{
			rm_pfree(PCONTEXT, partqueue);
			if ( newtrack != NULL )
			{
				rm_pfree(PCONTEXT, newtrack);
			}

			elog( WARNING, "Resource manager can not create resource queue %s "
						   "because %s",
						   partqueue->Name,
						   errorbuf);
			continue;
		}

		elog(RMLOG, "Created queue %s.", partqueue->Name);

		char buffer[1024];
		generateQueueReport(partqueue->OID, buffer, sizeof(buffer));
		elog(LOG, "Resource manager created resource queue instance : %s",
				  buffer);
	}

	/*
	 * STEP 3. Add users.
	 */
	while( list_length(userattrlist) > 0 )
	{
		List *attrs = lfirst(list_head(userattrlist));

		MEMORY_CONTEXT_SWITCH_TO(PCONTEXT)
		userattrlist = list_delete_first(userattrlist);
		MEMORY_CONTEXT_SWITCH_BACK

		UserInfo newuser = rm_palloc0(PCONTEXT,
									  sizeof(UserInfoData));

		res = parseUserAttributes(attrs,newuser, errorbuf, sizeof(errorbuf));
		if ( res != FUNC_RETURN_OK )
		{
			elog(WARNING, "Can not create user with its attributes because %s",
						  errorbuf);
			rm_pfree(PCONTEXT, newuser);
			continue;
		}

		res = checkUserAttributes(newuser, errorbuf, sizeof(errorbuf));
		if ( res != FUNC_RETURN_OK )
		{
			elog(WARNING, "User is not valid because %s", errorbuf);
			rm_pfree(PCONTEXT, newuser);
			continue;
		}

		createUser(newuser);

		char buffer[256];
		generateUserReport(newuser->Name,
						   strlen(newuser->Name),
						   buffer,
						   sizeof(buffer));

		elog(LOG, "Resource manager created user. %s", buffer);
	}
	return res;
}

int generateAllocRequestToBroker(void)
{
	int res = FUNC_RETURN_OK;
	/*--------------------------------------------------------------------------
	 * This is a temporary restrict that HAWQ RM supports only one memory/core
	 * ratio in current version.
	 *--------------------------------------------------------------------------
	 */
	Assert( PQUEMGR->RatioCount == 1 );

	DynMemoryCoreRatioTrack mctrack = PQUEMGR->RatioTrackers[0];

	bool hasWorkload = mctrack->TotalUsed.MemoryMB +
					   mctrack->TotalRequest.MemoryMB > 0;
	if ( !hasWorkload )
	{
		/* Check if resource manager has workload recently. */
		if ( PQUEMGR->RatioCount == 1 &&
			 PQUEMGR->RatioWaterMarks[0].NodeCount > 0 )
		{
			DynMemoryCoreRatioWaterMark mark =
				(DynMemoryCoreRatioWaterMark)
				getDQueueHeadNodeData(&(PQUEMGR->RatioWaterMarks[0]));
			hasWorkload = mark->ClusterMemoryMB > 0 ;
		}
	}

	/* Decide water level of resource. */
	int wlevel = hasWorkload ? PQUEMGR->ActualMinGRMContainerPerSeg : 0;
	switch( DRMGlobalInstance->ImpType )
	{
	case YARN_LIBYARN:
		/* Do nothing. */
		break;
	case NONE_HAWQ2:
		/* We always expect all resource allocated. */
		wlevel = INT_MAX;
		break;
	default:
		Assert(false);
	}

	/*
	 * Go through each segment, decide how many containers should be allocated.
	 * And generate preferred host list based on minimum water level.
	 */
	int		  grmctnsize = 0;
	List	 *preferred	 = NULL;
	List 	 *ressegl	 = NULL;
	ListCell *cell		 = NULL;
	getAllPAIRRefIntoList(&(PRESPOOL->Segments), &ressegl);

	foreach(cell, ressegl)
	{
		PAIR pair = (PAIR)lfirst(cell);
		SegResource segres = (SegResource)(pair->Value);

		/*
		 * Resource manager skips this segment if
		 * 1) Not FTS available;
		 * 2) Having resource decrease pending.
		 */
		if (!IS_SEGSTAT_FTSAVAILABLE(segres->Stat) ||
			(segres->DecPending.MemoryMB > 0 && segres->DecPending.Core > 0))
		{
			continue;
		}

		/* Get segment resource capacity. */
		uint32_t memcap  = getSegResourceCapacityMemory(segres);
		uint32_t corecap = getSegResourceCapacityCore(segres);

		/*
		 * The segment capacity mem/core ratio maybe not equal to the cluster
		 * mem/core ratio.
		 */
		int ctnsize = memcap / mctrack->MemCoreRatio;
		ctnsize = ctnsize < corecap ? ctnsize : corecap;

		/* Follow the water level calculated. */
		ctnsize = ctnsize < wlevel ? ctnsize : wlevel;

		elog(RMLOG, "Host %s has %d GRM containers, (%d MB, %lf CORE) increase pending.",
					 GET_SEGRESOURCE_HOSTNAME(segres),
					 (segres->ContainerSets[0] == NULL ?
					  0 :
					  list_length(segres->ContainerSets[0]->Containers)),
					 segres->IncPending.MemoryMB,
					 segres->IncPending.Core);

		int ctnsizeneed = ctnsize -
						  (segres->ContainerSets[0] == NULL ?
						   0 :
						   list_length(segres->ContainerSets[0]->Containers)) -
						  segres->IncPending.MemoryMB / mctrack->MemCoreRatio;
		if ( ctnsizeneed <= 0 )
		{
			/*
			 * If the segment contains more resource than expected resource
			 * water level.
			 */
			continue;
		}

		/* Build preferred host information. */
		PAIR newpair = rm_palloc0(PCONTEXT, sizeof(PAIRData));
		newpair->Key = segres;
		ResourceBundle resource = rm_palloc0(PCONTEXT, sizeof(ResourceBundleData));
		resource->MemoryMB = mctrack->MemCoreRatio * (ctnsizeneed);
		resource->Core = ctnsizeneed;
		newpair->Value = resource;
		preferred = lappend(preferred, newpair);

		elog(DEBUG3, "Expect resource from resource broker with "
				     "preferred host, hostname:%s, container number:%lf.",
				     GET_SEGRESOURCE_HOSTNAME(segres),
				     resource->Core);


		grmctnsize += ctnsizeneed;
	}
	freePAIRRefList(&(PRESPOOL->Segments), &ressegl);

	elog(RMLOG, "Resource manager needs minimum %d GRM containers.", grmctnsize);

	/* Decide how much resource to acquire from global resource manager. */
	int32_t reqmem  = 0;
	int32_t reqcore = ceil(mctrack->TotalRequest.Core   +
			  	   	       mctrack->TotalUsed.Core      -
						   mctrack->TotalAllocated.Core -
						   mctrack->TotalPending.Core);

	elog(RMLOG, "Memory Core Track has %lf requested, %lf used, %lf allocated, "
				 "%lf pending.",
				 mctrack->TotalRequest.Core,
				 mctrack->TotalUsed.Core,
				 mctrack->TotalAllocated.Core,
				 mctrack->TotalPending.Core);
	/*
	 * If after checking water level of segment resource, we expect more resource
	 * containers, we expect more as well.
	 */
	reqcore = reqcore < grmctnsize - mctrack->TotalPending.Core ?
			  grmctnsize - mctrack->TotalPending.Core :
			  reqcore;
	reqmem = reqcore * mctrack->MemCoreRatio;

	elog(RMLOG, "Resource manager now needs %d GRM containers.", reqcore);

	/*
	 * Check if should raise water level to deal with resource fragment or
	 * resource uneven problems. We trigger this logic only when no resource
	 * request caused by lack of resource, and no pending resource are waited
	 * for.
	 */
	if ( reqcore <= 0 &&
		 mctrack->TotalPending.Core <= 0 &&
		 (PQUEMGR->hasResourceProblem[RESPROBLEM_FRAGMENT] ||
		  PQUEMGR->hasResourceProblem[RESPROBLEM_UNEVEN]   ||
		  PQUEMGR->hasResourceProblem[RESPROBLEM_TOOFEWSEG]) )
	{
		/* Check if it is possible to raise water level. */
		if ( mctrack->TotalAllocated.Core + 1 <=
			 PRESPOOL->GRMTotal.Core * PQUEMGR->GRMQueueMaxCapacity )
		{
			/*
			 * We only add one more GRM container to acquire, this will trigger
			 * the following logic to raise the water level.
			 */
			reqcore = 1;
			reqmem = reqcore * mctrack->MemCoreRatio;

			PQUEMGR->hasResourceProblem[RESPROBLEM_FRAGMENT]  = false;
			PQUEMGR->hasResourceProblem[RESPROBLEM_UNEVEN]    = false;
			PQUEMGR->hasResourceProblem[RESPROBLEM_TOOFEWSEG] = false;

			elog(LOG, "Resource manager raises segment resource water level.");
		}
	}

	/* Call resource broker to request resource. */
	if ( reqmem > 0 && reqcore > 0 )
	{
		/*
		 * Here we know that we have to allocate more resource from GRM, we should
		 * check again to enrich the preferred host list for new locality data
		 * of expected additional GRM containers.
		 *
		 * The expected size of GRM containers may vary because we expect GRM
		 * to have even number of GRM containers in each available segments.
		 */
		completeAllocRequestToBroker(&reqmem, &reqcore, &preferred);

		elog(RMLOG, "Resource manager needs %d GRM containers after adjusting "
					"overall water level.",
					reqcore);

		if ( reqcore > 0 )
		{
			PRESPOOL->LastResAcqTime = gettime_microsec();

			addResourceBundleData(&(mctrack->TotalPending), reqmem, reqcore);
			uint64_t oldtime = mctrack->TotalPendingStartTime;
			if ( mctrack->TotalPendingStartTime == 0 )
			{
				mctrack->TotalPendingStartTime = gettime_microsec();
				elog(DEBUG3, "Global resource total pending start time is updated "
							 "to "UINT64_FORMAT,
							 mctrack->TotalPendingStartTime);
			}

			/* Generate request to resource broker now. */
			res = RB_acquireResource(reqmem, reqcore, preferred);
			if ( res != FUNC_RETURN_OK && res != RESBROK_PIPE_BUSY )
			{
				minusResourceBundleData(&(mctrack->TotalPending), reqmem, reqcore);
				mctrack->TotalPendingStartTime = oldtime;
				elog(WARNING, "Resource manager failed to allocate resource from "
							  "resource broker (%d MB, %d CORE).",
							  reqmem, reqcore);
			}
			else if ( res == RESBROK_PIPE_BUSY )
			{
				minusResourceBundleData(&(mctrack->TotalPending), reqmem, reqcore);
				elog(DEBUG3, "Resource manager should retry to submit request.");
				res = FUNC_RETURN_OK;
			}
			else
			{
				elog(RMLOG, "Resource manager finished submitting resource "
							"allocation request to global resource manager for "
							"(%d MB, %d CORE).",
							reqmem,
							reqcore);
			}
		}
	}

	return res;
}

void completeAllocRequestToBroker(int32_t 	 *reqmem,
								  int32_t 	 *reqcore,
								  List 		**preferred)
{
	/*
	 * Go through each segment to get minimum water level. The idea of completing
	 * the request is to keep pulling up the lowest water level in the cluster
	 * until equal or more GRM containers are requested.
	 */
	Assert(*reqmem % *reqcore == 0);
	uint32_t ratio = *reqmem / *reqcore;

	/* Step 1. Get lowest water level and build up index. */
	List *ressegl = NULL;
	getAllPAIRRefIntoList(&(PRESPOOL->Segments), &ressegl);
	/* Index of each segment in current preferred host list. */
	PAIR *reqidx = rm_palloc0(PCONTEXT, sizeof(PAIR) * list_length(ressegl));
	int llevel = INT_MAX;
	int totalcount = 0;
	int index = 0;
	bool allunavail = true;
	ListCell *cell = NULL;
	foreach(cell, ressegl)
	{
		reqidx[index] = NULL;

		PAIR pair = (PAIR)lfirst(cell);
		SegResource segres = (SegResource)(pair->Value);

		/*
		 * Resource manager skips this segment if
		 * 1) Not FTS available;
		 * 2) Having resource decrease pending.
		 */
		if (!IS_SEGSTAT_FTSAVAILABLE(segres->Stat)  ||
			(segres->DecPending.MemoryMB > 0 && segres->DecPending.Core > 0))
		{
			index++;
			continue;
		}

		allunavail = false;

		int clevel = (segres->ContainerSets[0] == NULL ?
					  0 :
					  list_length(segres->ContainerSets[0]->Containers)) +
					 segres->IncPending.MemoryMB / ratio;

		ListCell *pcell = NULL;
		foreach(pcell, *preferred)
		{
			PAIR existpair = (PAIR)lfirst(pcell);
			if ( existpair->Key == segres )
			{
				reqidx[index] = existpair;
				totalcount += ((ResourceBundle)(reqidx[index]->Value))->MemoryMB /
							  ratio;
				break;
			}
		}

		int creqsize = reqidx[index] == NULL ?
					   0 :
					   ((ResourceBundle)(reqidx[index]->Value))->MemoryMB / ratio;

		llevel = (clevel+creqsize) < llevel ? (clevel+creqsize) : llevel;
		index++;
	}

	/* If no segment available for adjusting the request, no need to keep going */
	if ( allunavail )
	{
		rm_pfree(PCONTEXT, reqidx);
		return;
	}

	for ( int i = 0 ; i < list_length(ressegl) ; ++i )
	{
		if ( reqidx[i] == NULL )
		{
			continue;
		}

		elog(RMLOG, "Expect resource from resource broker with "
				    "preferred host, hostname:%s, container number:%lf, "
				    "increase pending %d.",
				    GET_SEGRESOURCE_HOSTNAME(((SegResource)(reqidx[i]->Key))),
					((ResourceBundle)(reqidx[i]->Value))->Core,
					((SegResource)(reqidx[i]->Key))->IncPending.MemoryMB/ratio);
	}

	elog(RMLOG, "Lowest water level before adjusting is %d.", llevel);

	/* Step 2. Adjust request. */
	int32_t reqcoreleft = *reqcore - totalcount;
	bool keeplooping = true;
	while( reqcoreleft > 0 && keeplooping )
	{
		llevel++;
		index = 0;
		keeplooping = false;
		foreach(cell, ressegl)
		{
			PAIR pair = (PAIR)lfirst(cell);
			SegResource segres = (SegResource)(pair->Value);

			/*
			 * Resource manager skips this segment if
			 * 1) Not FTS available;
			 * 2) Having resource decrease pending.
			 */
			if (!IS_SEGSTAT_FTSAVAILABLE(segres->Stat) ||
				(segres->DecPending.MemoryMB > 0 && segres->DecPending.Core > 0))
			{
				index++;
				continue;
			}

			/* Check total capacity of the segment. */
			uint32_t corecap = getSegResourceCapacityCore(segres);

			int clevel = (segres->ContainerSets[0] == NULL ?
						  0 :
						  list_length(segres->ContainerSets[0]->Containers)) +
						 segres->IncPending.MemoryMB / ratio;

			int aclevel = reqidx[index] == NULL ?
						  clevel :
						  (clevel + ((ResourceBundle)(reqidx[index]->Value))->MemoryMB / ratio);

			if ( llevel > aclevel && llevel <= corecap )
			{
				if ( reqidx[index] == NULL )
				{
					reqidx[index] = rm_palloc0(PCONTEXT, sizeof(PAIRData));
					reqidx[index]->Key = segres;
					ResourceBundle resource = rm_palloc0(PCONTEXT,
														 sizeof(ResourceBundleData));
					resetResourceBundleData(resource, 0, 0, ratio);
					reqidx[index]->Value = resource;
					*preferred = lappend(*preferred, reqidx[index]);
				}
				addResourceBundleData((ResourceBundle)(reqidx[index]->Value),
									  (llevel-aclevel) * ratio,
									  llevel-aclevel);
				reqcoreleft -= llevel-aclevel;
				keeplooping = true;

				elog(RMLOG, "Resource manager acquires %lf GRM containers on "
						    "host %s. Current level(having pending) %d, "
						    "expect level %d, acquired in current request %d.",
						    ((ResourceBundle)(reqidx[index]->Value))->Core,
						    GET_SEGRESOURCE_HOSTNAME(((SegResource)(reqidx[index]->Key))),
							clevel,
							llevel,
							aclevel-clevel);
			}
			index++;
		}
	}

	/* Adjust total mem and core request. */
	*reqcore -= reqcoreleft;
	*reqmem = *reqcore * ratio;

	rm_pfree(PCONTEXT, reqidx);
}
/*
 * Create all socket servers to accept connection from QD or RM agents.
 */
int	 initializeSocketServer(void)
{
	int 		res		= FUNC_RETURN_OK;
	int 		netres 	= 0;
	char 	   *allip   = "0.0.0.0";
	pgsocket 	RMListenSocket[HAWQRM_SERVER_PORT_COUNT];

	for ( int i = 0 ; i < HAWQRM_SERVER_PORT_COUNT ; ++i )
	{
		RMListenSocket[i] = PGINVALID_SOCKET;
	}

	netres = StreamServerPort(AF_UNSPEC,
			  	  	  	  	  allip,
							  rm_master_port,
							  NULL,
							  RMListenSocket,
							  HAWQRM_SERVER_PORT_COUNT);
	if ( netres != STATUS_OK )
	{
		res = REQUESTHANDLER_FAIL_START_SOCKET_SERVER;
		elog(LOG, "Resource manager cannot create socket server. Port=%d",
				  rm_master_port);
		return res;
	}

	/* Initialize array for polling all file descriptors. */
	initializeAsyncComm();
	int 			validfdcount = 0;
	AsyncCommBuffer newbuffer    = NULL;
	for ( int i = 0 ; i < HAWQRM_SERVER_PORT_COUNT ; ++i )
	{
		if (RMListenSocket[i] != PGINVALID_SOCKET)
		{
			netres = registerFileDesc(RMListenSocket[i],
									  ASYNCCOMM_READ,
									  &AsyncCommBufferHandlersMsgServer,
									  NULL,
									  &newbuffer);
			if ( netres != FUNC_RETURN_OK )
			{
				res = REQUESTHANDLER_FAIL_START_SOCKET_SERVER;
				elog(WARNING, "Resource manager cannot track socket server.");
				break;
			}
			validfdcount++;
			InitHandler_Message(newbuffer);
		}
	}

	if ( res != FUNC_RETURN_OK )
	{
		for ( int i = 0 ; i < HAWQRM_SERVER_PORT_COUNT ; ++i )
		{
			if ( RMListenSocket[i] != PGINVALID_SOCKET )
			{
				close(RMListenSocket[i]);
			}
		}
		return res;
	}

	elog(LOG, "Resource manager starts accepting resource request. "
			  "Listening normal socket port %d. "
			  "Total listened %d FDs.",
			  rm_master_port,
			  validfdcount);
	return res;
}


/*
 * Send built response content back to RM. The sending is asynchronous.
 *
 * If the progress is error, close the connection.
 */
void sendResponseToClients(void)
{
	while( list_length(PCONTRACK->ConnToSend) > 0 )
	{
		ConnectionTrack conntrack = (ConnectionTrack)
									lfirst(list_head(PCONTRACK->ConnToSend));
		MEMORY_CONTEXT_SWITCH_TO(PCONTEXT)
		PCONTRACK->ConnToSend = list_delete_first(PCONTRACK->ConnToSend);
		MEMORY_CONTEXT_SWITCH_BACK

		if ( conntrack->CommBuffer != NULL )
		{
			buildMessageToCommBuffer(conntrack->CommBuffer,
									 conntrack->MessageBuff.Buffer,
									 conntrack->MessageSize,
									 conntrack->MessageID,
									 conntrack->MessageMark1,
									 conntrack->MessageMark2);
		}
	}
}

/*
 * Check and set the nodes down that are not updated by IMAlive heart-beat for a
 * long time.
 */
void updateStatusOfAllNodes()
{
	SegResource node = NULL;
	uint64_t curtime = 0;

	bool changedstatus = false;
	curtime = gettime_microsec();
	for(uint32_t idx = 0; idx < PRESPOOL->SegmentIDCounter; idx++)
	{
		node = getSegResource(idx);
		Assert(node != NULL);
		uint8_t oldStatus = node->Stat->FTSAvailable;
		if ( (curtime - node->LastUpdateTime >
			 1000000LL * rm_segment_heartbeat_timeout) &&
			 (node->Stat->StatusDesc & SEG_STATUS_HEARTBEAT_TIMEOUT) == 0)
		{
			/*
			 * This segment is heartbeat timeout, update its description
			 * and set it to unavailable if needed.
			 */
			if (oldStatus == RESOURCE_SEG_STATUS_AVAILABLE)
			{
				/*
				 * This call makes resource manager able to adjust queue and mem/core
				 * trackers' capacity.
				 */
				setSegResHAWQAvailability(node, RESOURCE_SEG_STATUS_UNAVAILABLE);
				/*
				 * This call makes resource pool remove unused containers.
				 */
				returnAllGRMResourceFromSegment(node);
				changedstatus = true;
			}

			node->Stat->StatusDesc |= SEG_STATUS_HEARTBEAT_TIMEOUT;
			if (Gp_role != GP_ROLE_UTILITY)
			{
				SimpStringPtr description = build_segment_status_description(node->Stat);
				update_segment_status(idx + REGISTRATION_ORDER_OFFSET,
										SEGMENT_STATUS_DOWN,
										(description->Len > 0)?description->Str:"");
				add_segment_history_row(idx + REGISTRATION_ORDER_OFFSET,
										GET_SEGRESOURCE_HOSTNAME(node),
										(description->Len > 0)?description->Str:"");

				freeSimpleStringContent(description);
				rm_pfree(PCONTEXT, description);
			}

			elog(WARNING, "Resource manager sets host %s heartbeat timeout.",
						  GET_SEGRESOURCE_HOSTNAME(node));
		}
	}

	if ( changedstatus )
	{
		refreshResourceQueueCapacity(false);
		refreshActualMinGRMContainerPerSeg();
	}

	validateResourcePoolStatus(true);
}

/*
 * Set all nodes DOWN due to no GRM cluster report
 */
void setAllNodesGRMDown()
{
	SegResource node = NULL;

	bool changedstatus = false;
	for(uint32_t idx = 0; idx < PRESPOOL->SegmentIDCounter; idx++)
	{
		node = getSegResource(idx);
		Assert(node != NULL);
		uint8_t oldStatus = node->Stat->FTSAvailable;
		uint32_t oldDesc  = node->Stat->StatusDesc;

		if (oldStatus == RESOURCE_SEG_STATUS_AVAILABLE)
		{
			/*
			 * This call makes resource manager able to adjust queue and mem/core
			 * trackers' capacity.
			 */
			setSegResHAWQAvailability(node, RESOURCE_SEG_STATUS_UNAVAILABLE);
			changedstatus = true;
		}

		node->Stat->StatusDesc |= SEG_STATUS_NO_GRM_NODE_REPORT;
		if (Gp_role != GP_ROLE_UTILITY && oldDesc != node->Stat->StatusDesc)
		{
			SimpStringPtr description = build_segment_status_description(node->Stat);
			update_segment_status(idx + REGISTRATION_ORDER_OFFSET,
									SEGMENT_STATUS_DOWN,
									(description->Len > 0)?description->Str:"");
			add_segment_history_row(idx + REGISTRATION_ORDER_OFFSET,
									GET_SEGRESOURCE_HOSTNAME(node),
									(description->Len > 0)?description->Str:"");

			freeSimpleStringContent(description);
			rm_pfree(PCONTEXT, description);
		}

		if (oldDesc != node->Stat->StatusDesc)
		{
			elog(WARNING, "Resource manager sets host %s DOWN in cleanup phase for resource broker error.",
						  GET_SEGRESOURCE_HOSTNAME(node));
		}
	}

	if (changedstatus)
	{
		refreshResourceQueueCapacity(false);
		refreshActualMinGRMContainerPerSeg();
	}

	validateResourcePoolStatus(true);
}

/**
 * Load segment information from file.
 *
 * NOTE: This is a test facility.
 */
int  loadHostInformationIntoResourcePool(void)
{
    int                     res             = FUNC_RETURN_OK;
    SelfMaintainBufferData  seginfobuff;
    FILE				   *phostlist 		= NULL;

    /* Open the file */

    elog(LOG, "HAWQ RM :: To load file %s", rm_resourcepool_test_filename);

    phostlist = fopen(rm_resourcepool_test_filename, "r");
    if ( phostlist == NULL )
        return FUNC_RETURN_OK;

    /* This buffer is used for building machine id instance. */
    initializeSelfMaintainBuffer(&seginfobuff, PCONTEXT);

    /*
     * Loop each host, we identify hosts by hostname only. The first segment in
     * that host is selected as the target machine.
     */
    char line[1024];
    while( fgets(line, sizeof(line)-1, phostlist) != NULL ) {

        elog(LOG, "HAWQ RM :: Load line %s", line);

        /* Parse line. */
        char *phostname = strtok(line, ",");
        if ( phostname == NULL ) continue;
        char *phostport = strtok(NULL, ",");
        if ( phostport == NULL ) continue;
        char *phostip = strtok(NULL, ",");
        if ( phostip == NULL ) continue;
        uint32_t port = 0;
        if (sscanf(phostport, "%d", &port) != 1)
        {
            elog(LOG, "HAWQ RM :: Invalid port, skip machine %s", phostname);
            continue;
        }

        /* If the host is new, add new host instance in resource pool */
        int32_t segtid = SEGSTAT_ID_INVALID;
        if ( getSegIDByHostName(phostname,strlen(phostname), &segtid) ==
             FUNC_RETURN_OK )
        {
            elog(LOG, "HAWQ RM :: Skip machine %s", phostname);
            continue;
        }

        elog(LOG, "HAWQ RM :: Find machine to add. %s", phostname);

        /* Reserve fixed head part of the machine id instance. */
        resetSelfMaintainBuffer(&seginfobuff);
        prepareSelfMaintainBuffer(&seginfobuff, sizeof(SegInfoData),true);
        SegInfo newhost = (SegInfo)(seginfobuff.Buffer);
        newhost->master     = 0;
        newhost->standby    = 0;
        newhost->port       = port;
        newhost->alive      = 1;

        jumpforwardSelfMaintainBuffer(&seginfobuff, sizeof(SegInfoData));

        /* Add address offset and attributes. Currently, only one address. */
        newhost->HostAddrCount = 1;
        newhost->AddressAttributeOffset = sizeof(SegInfoData);
        /* Jump offset and reserved pad. */
        newhost->AddressContentOffset   = sizeof(SegInfoData) +
                sizeof(uint32_t) * (((newhost->HostAddrCount + 1) >> 1) << 1);
        uint16_t addrattr   = HOST_ADDRESS_CONTENT_STRING;
        uint16_t addroffset = newhost->AddressContentOffset;

        appendSMBVar(&seginfobuff, addroffset);
        appendSMBVar(&seginfobuff, addrattr);
        appendSelfMaintainBufferTill64bitAligned(&seginfobuff);

        /* Add hostip content. */
        uint32_t length   = strlen(phostip);
        while( length > 0 &&
               (phostip[length-1] == '\r' ||
                phostip[length-1] == '\n' ||
                phostip[length-1] == '\t' ||
                phostip[length-1] == ' ') ) {
            length--;
        }
        int     addrsize  = __SIZE_ALIGN64(offsetof(AddressStringData, Address) +
                                           length + 1);
        prepareSelfMaintainBuffer(&seginfobuff, addrsize, true);
        AddressString straddr = (AddressString)(seginfobuff.Buffer +
                                                seginfobuff.Cursor + 1);
        straddr->Length = length;
        memcpy(straddr->Address, phostip, length+1);
        straddr->Address[length] = '\0';
        jumpforwardSelfMaintainBuffer(&seginfobuff, addrsize);

        newhost = (SegInfo)(seginfobuff.Buffer);

        /* Append hostname string. */
        newhost->HostNameLen = strlen(phostname);
        newhost->HostNameOffset = seginfobuff.Cursor + 1;
        appendSMBStr(&seginfobuff,phostname);
        appendSelfMaintainBufferTill64bitAligned(&seginfobuff);

        /* Update total machine id instance size. */
        newhost = (SegInfo)(seginfobuff.Buffer);
        newhost->Size = seginfobuff.Cursor + 1;

        /* Build machine info instance. */
        SegStat segstat = (SegStat)rm_palloc0(PCONTEXT,
                                              offsetof(SegStatData, Info) +
                                              seginfobuff.Cursor + 1);
        segstat->ID                = SEGSTAT_ID_INVALID;
        segstat->FTSAvailable      = RESOURCE_SEG_STATUS_AVAILABLE;
        segstat->FTSTotalMemoryMB  = DRMGlobalInstance->SegmentMemoryMB;
        segstat->FTSTotalCore      = DRMGlobalInstance->SegmentCore;
        segstat->GRMTotalMemoryMB  = 0;
        segstat->GRMTotalCore      = 0;
        segstat->FailedTmpDirNum   = 0;

        memcpy((char *)segstat + offsetof(SegStatData, Info),
                seginfobuff.Buffer,
                seginfobuff.Cursor+1);

        SelfMaintainBufferData segreport;
        initializeSelfMaintainBuffer(&segreport,PCONTEXT);
        generateSegStatReport(segstat, &segreport);
        elog(LOG, "Resource manager builds available segment from file: %s",
        		  segreport.Buffer);
        destroySelfMaintainBuffer(&segreport);

        bool capstatchanged = false;
        res = addHAWQSegWithSegStat(segstat, &capstatchanged);
        if ( res != FUNC_RETURN_OK )
        {
            elog(WARNING, "Resource manager failed to add machine from file.");
            rm_pfree(PCONTEXT, segstat);
        }

        /* Fill HDFS and GRM host name cache */
        int32_t id = SEGSTAT_ID_INVALID;
        SimpString key;
        setSimpleStringRefNoLen(&key, GET_SEGINFO_HOSTNAME(&(segstat->Info)));
        getSegIDByHostName(GET_SEGINFO_HOSTNAME(&(segstat->Info)),
                           segstat->Info.HostNameLen,
                           &id);
        setHASHTABLENode(&(PRESPOOL->HDFSHostNameIndexed),
                         &key,
                         TYPCONVERT(void *, id),
                         false);
        setHASHTABLENode(&(PRESPOOL->GRMHostNameIndexed),
                         &key,
                         TYPCONVERT(void *, id),
                         false);
    }

	/* Refresh resource queue capacities. */
    refreshResourceQueueCapacity(false);
    refreshActualMinGRMContainerPerSeg();
	/* Recalculate all memory/core ratio instances' limits. */
	refreshMemoryCoreRatioLimits();
	/* Refresh memory/core ratio level water mark. */
	refreshMemoryCoreRatioWaterMark();

	/* Free buffer if used. */
	destroySelfMaintainBuffer(&seginfobuff);

	fclose(phostlist);

    return FUNC_RETURN_OK;
}

extern Datum dump_resource_manager_status(PG_FUNCTION_ARGS)
{
	static char errorbuf[ERRORMESSAGE_SIZE];
    int type = PG_GETARG_INT32(0);
    char message[1024] = {0};
    char dump_file[1024] = {0};

    switch (type)
    {
    case 1:
        strcpy(dump_file, "/tmp/resource_manager_conntrack_status");
        sprintf(message, "Dump resource manager connection track status to %s", dump_file);
        break;

    case 2:
        strcpy(dump_file, "/tmp/resource_manager_resqueue_status");
        sprintf(message, "Dump resource manager resource queue status to %s", dump_file);
        break;

    case 3:
        strcpy(dump_file, "/tmp/resource_manager_respool_status");
        sprintf(message, "Dump resource manager resource pool status to %s", dump_file);
        break;

    default:
        sprintf(message, "Dump resource manager status failed.\n"
                "1 -> dump resource manager connection track status\n"
                "2 -> dump resource manager resource queue status\n"
                "3 -> dump resource manager resource pool status");
        PG_RETURN_TEXT_P(cstring_to_text(message));    
    }

    dumpResourceManagerStatus(type, dump_file, errorbuf, sizeof(errorbuf));

    PG_RETURN_TEXT_P(cstring_to_text(message));    
}

void processResourceBrokerTasks(void)
{
	uint64_t curtime = 0;
	int		 res	 = FUNC_RETURN_OK;

	if ( !isCleanGRMResourceStatus() )
	{
		/*
		 * STEP 1. Check if should generate request to periodically check
		 * 		   GRM cluster status.
		 *
		 * Generate request to the global resource manager ( YARN, etc.) to get
		 * cluster report. This is a asynchronous request, therefore, we don't
		 * need to handle the real cluster report right now. The possibility is
		 * reserved that if one fake global resource manager is implemented in
		 * HAWQ RM process and the cluster report can be returned directly in
		 * this function.
		 */
        curtime = gettime_microsec();

		if ( (PRESPOOL->Segments.NodeCount > 0 ) &&
			 (curtime - PRESPOOL->LastUpdateTime  >
			  rm_cluster_report_period * 1000000LL ||
			  hasSegmentGRMCapacityNotUpdated() ) &&
			 (curtime - PRESPOOL->LastRequestTime > 5LL * 1000000LL) )
		{
			double  maxcap  = 0.0;
			List   *report	= NULL;

		    PRESPOOL->LastRequestTime = curtime;
		    res = RB_getClusterReport(rm_grm_yarn_queue, &report, &maxcap);
		    Assert(report == NULL);
		    if ( res != FUNC_RETURN_OK )
		    {
		    	elog(WARNING, "Resource manager fails to fresh cluster report "
		    				  "from global resource manager.");
		    	goto exit;
		    }
		}

		/* STEP 2. Check if should periodically check container status. */
		curtime = gettime_microsec();

		if ( PRESPOOL->AddPendingContainerCount == 0 &&
			 PRESPOOL->RetPendingContainerCount == 0 &&
			 (curtime - PRESPOOL->LastCheckContainerTime   >
			  rm_cluster_report_period * 1000000LL) &&
			 (curtime - PRESPOOL->LastRequestContainerTime > 5LL  * 1000000LL) )
		{
			List *report = NULL;
			PRESPOOL->LastRequestContainerTime = curtime;
			res = RB_getContainerReport(&report);
			Assert( report == NULL );
			if ( res != FUNC_RETURN_OK )
			{
				elog(WARNING, "Resource manager fails to get container report "
							  "from global resource manager.");
				goto exit;
			}
		}

        /* STEP 3. Return kicked GRM containers. */
        curtime = gettime_microsec();

    	if ( !PRESPOOL->pausePhase[QUOTA_PHASE_KICKED_TO_RETURN] )
    	{
			res = RB_returnResource(&(PRESPOOL->KickedContainers));
			if ( res != FUNC_RETURN_OK )
			{
				elog(WARNING, "Resource manager failed to return kicked container to "
							  "global resource manager.");
				goto exit;
			}
    	}
    	else
		{
			elog(LOG, "Paused returning GRM containers kicked to GRM.");
    	}
	    /*
	     * STEP 4. Handle resource broker input as new allocated resource or
		 * 		   cluster report, container report etc. The allocated resource
		 * 		   from resource broker will be added to the resource queue
		 * 		   manager and resource pool.
		 */
		res = RB_handleNotifications();
		if ( res != FUNC_RETURN_OK )
		{
			elog(WARNING, "Resource manager fails to handle response from global "
						  "resource broker");
			goto exit;
		}
	}
	else
	{
		RB_clearResource(&(PRESPOOL->KickedContainers));
	}

exit:
	RB_handleError(res);
}

void generateResourceRequestToResourceBroker(void)
{
	uint64_t curtime = 0;
	int		 res	 = FUNC_RETURN_OK;

	if ( !isCleanGRMResourceStatus() )
	{
		curtime = gettime_microsec();

        if ( PRESPOOL->Segments.NodeCount > 0 && PQUEMGR->RatioCount > 0 )
        {
        	refreshMemoryCoreRatioLevelUsage(curtime);
        	if ( curtime - PRESPOOL->LastResAcqTime > 1000000LL)
        	{
				res = generateAllocRequestToBroker();
				if ( res != FUNC_RETURN_OK )
				{
					elog(WARNING, "Resource manager fails to allocate container "
								  "from global resource manager.");
					goto exit;
				}
        	}
        }
	}

exit:
	RB_handleError(res);

}

void cleanupAllGRMContainers(void)
{
	dropAllResPoolGRMContainersToToBeKicked();
	dropAllToAcceptGRMContainersToKicked();
}

bool cleanedAllGRMContainers(void)
{
	/* Condition 1. All segments have no resource allocated. */
	if ( !allSegmentHasNoGRMContainersAllocated() )
	{
		return false;
	}

	if ( PRESPOOL->AddPendingContainerCount > 0 ||
		 PRESPOOL->RetPendingContainerCount > 0 )
	{
		elog(RMLOG, "Pending GRM container count inc %d, dec %d.",
					PRESPOOL->AddPendingContainerCount,
					PRESPOOL->RetPendingContainerCount);
	}
	/* Condition 2. No on-the-fly GRM containers for increasing and decreasing.*/
	return PRESPOOL->AddPendingContainerCount == 0 &&
		   PRESPOOL->RetPendingContainerCount == 0;
}
