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

#ifndef DYNAMIC_RESOURCE_MANAGEMENT_H
#define DYNAMIC_RESOURCE_MANAGEMENT_H

/*******************************************************************************
 * OVERVIEW of dynamic resource management.
 *
 * This header file defines all necessary  data  structures for dynamic resource
 * management. Basically, the resource management is designed as 3 components:
 * Resource Queue Manager, Connection Track Manager and Resource Pool Manager.
 *
 * Resource Queue Manager :it checks the resource queue definition to ensure all
 *                         resource usages are under resource queue and statement
 *                         level resource usage restriction. For each query, the
 *                         resource quota is assigned by it.
 *
 * Connection Track		  :it saves all in-use connections and tracks all
 * 						   resource allocated for each connection.
 *
 * Resource Pool Manager  :all physical nodes' resources are  maintained here by
 *						   node,the resources are dispatched from selected nodes
 *            			   and they build up one resource budget.  When external
 *						   resource manager ( YARN, for example ) gets / returns
 *             			   some resources, the changes are reflected here. Also,
 * 						   when some node  becomes  available / unavailable, the
 * 						   changes are reflected here as well.  This manager can
 * 						   support thread-safe and efficient node manipulation.
 *
 *          --------------------------------------------------------+
 *          |                   Request handler                     |
 *          +--------------------------------+----------------------+
 *          |     Resource Queue Manager     |   Connection Track   |
 *          |       resourcequeue.h          |     conntrack.h      |
 *          +--------------------------------+----------------------+
 *          |                Node Resource Manager                  |
 *          |                   resourcepool.h                      |
 *          +-------------------------------------------------------+
 *
 ******************************************************************************/
#include "envswitch.h"

#include "utils/balancedbst.h"
#include "utils/linkedlist.h"
#include "utils/simplestring.h"
#include "utils/memutilities.h"

#include "resourcepool.h"
#include "conntrack.h"
#include "errorcode.h"
#include "resqueuemanager.h"
#include "resourceenforcer/resourceenforcer.h"
#include "resourceenforcer/resourceenforcer_queue.h"
#include "resourceenforcer/resourceenforcer_list.h"
#include "resourceenforcer/resourceenforcer_hash.h"

struct DynRMGlobalData;
typedef struct DynRMGlobalData * DynRMGlobal;

int  createDRMInstance(void);
int  initializeDRMInstance(MCTYPE context);
int  initializeDRMInstanceForQD(void);
void initializeDRMInstanceForQE(void);

/*****************************************************************************
 *                              REQUEST HANDLER                              *
 *****************************************************************************/

/* The socket(s) we're listening to. */
#define MAXLISTEN   	64
#define INVALIDSOCKET 	(-1)

/*-----------------------------------------------------------------------------
 * Request Handler APIs
 *
 * This component handles all requests sent from Postmaster(PM) or Backend(BE).
 *---------------------------------------------------------------------------*/

bool handleRMRequestConnectionReg(void **arg);
bool handleRMRequestConnectionRegByOID(void **arg);
bool handleRMRequestConnectionUnReg(void **arg);
bool handleRMRequestAcquireResource(void **arg);
bool handleRMRequestReturnResource(void **arg);
bool handleRMSEGRequestIMAlive(void **arg);
bool handleRMSEGRequestRUAlive(void **arg);
bool handleRMRequestTmpDir(void **arg);
bool handleRMSEGRequestTmpDir(void **arg);
bool handleRMRequestAcquireResourceQuota(void **arg);
bool handleRMRequestRefreshResource(void **arg);
bool handleRMRequestSegmentIsDown(void **arg);
bool handleRMDDLRequestManipulateResourceQueue(void **arg);
bool handleRMDDLRequestManipulateRole(void **arg);
bool handleQEMoveToCGroup(void **arg);
bool handleQEMoveOutCGroup(void **arg);
bool handleQESetWeightCGroup(void **arg);
bool handleRMIncreaseMemoryQuota(void **arg);
bool handleRMDecreaseMemoryQuota(void **arg);
bool handleRMRequestDumpStatus(void **arg);
bool handleRMRequestDumpResQueueStatus(void **arg);

bool handleRMRequestDummy(void **arg);
bool handleRMRequestQuotaControl(void **arg);

int refreshLocalHostInstance(void);
void checkLocalPostmasterStatus(void);
void checkTmpDirStatus(void);
/*-----------------------------------------------------------------------------
 * Dynamic resource manager overall APIs
 *----------------------------------------------------------------------------*/

#define GLOBAL_CONFIG_MEMORY_CONTEXT_NAME "GlobConfContext"
#define DRMGLOBAL_MEMORY_CONTEXT_NAME     "DynRMContext"  /* Name of context. */

#define HAWQDRM_PROCMODE_INDEPENDENT	1
#define HAWQDRM_PROCMODE_FORKED			2

#define TMPDIR_MAX_LENGTH               1024

enum RB_IMP_TYPE {
	YARN_LIBYARN=0,
	NONE_HAWQ2,
	LAST_IMP		/* Never define new implementation types after this item. */
};

enum START_RM_ROLE_VALUE {
	START_RM_ROLE_UNSET = 0,
	START_RM_ROLE_MASTER,
	START_RM_ROLE_SEGMENT
};

enum LOCALHOST_STATUS {
	LOCALHOST_STATUS_UNSET = 0,
	LOCALHOST_STATUS_NORNAL,
	LOCALHOST_STATUS_ABNORMAL,
	LOCALHOST_STATUS_INSERVICE
};

typedef struct TmpDirKey
{
    uint32_t    session_id;
} TmpDirKey;

typedef struct TmpDirEntry
{
    TmpDirKey   key;
    uint32_t    command_id;
    HTAB        *qe_tmp_dirs;
} TmpDirEntry;

typedef struct QETmpDirKey
{
    int         qeidx;
} QETmpDirKey;

typedef struct QETmpDirEntry
{
    QETmpDirKey key;
    char        tmpdir[TMPDIR_MAX_LENGTH];
} QETmpDirEntry;

struct DynRMGlobalData{

	MCTYPE            		 Context;			   /* Meaningful only in HAWQ
													  memory facility         */
    pid_t					 ThisPID;
    pid_t					 ParentPID;
	int						 Role;

    DQueueData				 ResourceManagerConfig;

    enum RB_IMP_TYPE 	 	 ImpType;

    /* Key data structures */
    DynResourceQueueManager  ResourceQueueManager; /* Res queue management.   */
    ConnectionTrackManager   ConnTrackManager;	   /* Connection track. 	  */
    ResourcePool     	 	 ResourcePoolInstance; /* Node management.		  */
    volatile bool			 ResManagerMainKeepRun;
    uint64_t				 ResourceManagerStartTime;

    /*------------------------------------------------------------------------*/
    /* INTERCONN:: RM server and RM agents.                                   */
    /*------------------------------------------------------------------------*/
    SimpString				 SocketLocalHostName;
    /* used by segment host, whether send IMAlive message to master */
    bool 					 SendIMAlive;
    /* used by segment host, weather send message to standby or not */
    bool                     SendToStandby;

    /*------------------------------------------------------------------------*/
    /* SEGMENT:: Local host machine information.							  */
    /*------------------------------------------------------------------------*/
    SegStat 				 LocalHostStat;
    
    DQueueData				 LocalHostTempDirectoriesForQD;      
    int                      NextLocalHostTempDirIdxForQD;
    
    DQueueData				 LocalHostTempDirectories;
    int                      NextLocalHostTempDirIdx;
    HTAB                     *LocalTmpDirTable;
    int                      TmpDirTableCapacity;
    DQueueData               TmpDirLRUList;
    
    uint64_t				 LocalHostLastUpdateTime;
    uint64_t				 HeartBeatLastSentTime;
    uint64_t				 TmpDirLastCheckTime;
    int32_t					 SegmentMemoryMB;
    double					 SegmentCore;
    /*------------------------------------------------------------------------*/
    /* SEGMENT:: CGroup for resource enforcement                              */
    /*------------------------------------------------------------------------*/
    bool					ResourceEnforcerCpuEnable;
    SimpString				ResourceEnforcerCgroupMountPoint;
    SimpString				ResourceEnforcerCgroupHierarchyName;
    double					ResourceEnforcerCpuWeight;
    double					ResourceEnforcerVcorePcoreRatio;
    int						ResourceEnforcerCleanupPeriod;

    /*------------------------------------------------------------------------*/
    /* RESOURCE BROKER                                                        */
    /*------------------------------------------------------------------------*/
    int64_t					ResBrokerAppTimeStamp;
    bool					ResBrokerTriggerCleanup;

    /*------------------------------------------------------------------------*/
    /* MARKER FOR RESOURCE ON-THE-FLY                                         */
    /*------------------------------------------------------------------------*/
    int						IncreaseMemoryRPCCounter;
    int						DecreaseMemoryRPCCounter;

    /*------------------------------------------------------------------------*/
    /* MARKER FOR RESOURCE BREATH                                             */
    /*------------------------------------------------------------------------*/
    int						ForcedReturnGRMContainerCount;
};

extern DynRMGlobal DRMGlobalInstance;

extern pthread_t	t_move_cgroup;
extern queue		*g_queue_cgroup;
extern GHash		g_ghash_cgroup;
extern uint64		rm_enforce_last_cleanup_time;
extern volatile bool	g_enforcement_thread_quited;

#define PERRTRACK (DRMGlobalInstance->ErrorTrack)
#define PCONTEXT  (DRMGlobalInstance->Context)
#define PRESPOOL  (DRMGlobalInstance->ResourcePoolInstance)
#define PQUEMGR	  (DRMGlobalInstance->ResourceQueueManager)
#define PCONTRACK (DRMGlobalInstance->ConnTrackManager)

#define ASSERT_DRM_GLOBAL_INSTANCE_CREATED 									   \
						Assert(DRMGlobalInstance != NULL);
#define ASSERT_RESQUEUE_MANAGER_CREATED	   									   \
						Assert(DRMGlobalInstance->ResourceQueueManager != NULL);

int registerHAWQ2GlobalRM(void);
int refreshGlobalRMClusterInformation(void);

#define HAWQDRM_CONFFILE_SERVER_TYPE  				"hawq_resourcemanager_server_type"

#define HAWQDRM_CONFFILE_TEST_CONF_FILE 			"hawq_resourcemanager_test_configure_filename"
#define HAWQDRM_CONFFILE_TEST_QUEUSR_FILE 			"hawq_resourcemanager_test_queueuser_filename"

#define HAWQDRM_CONFFILE_LIMIT_MEMORY_USE 			"hawq_rm_memory_limit_perseg"
#define HAWQDRM_CONFFILE_LIMIT_CORE_USE  			"hawq_rm_nvcore_limit_perseg"

#define HAWQDRM_CONFFILE_HOSTNAMES_ALL				"hawq_resourcemanager_hostnames_all"
#define HAWQDRM_CONFFILE_HOSTNAMES_BLACK			"hawq_resourcemanager_hostnames_black"
#define HAWQDRM_CONFFILE_HOSTNAMES_NEW				"hawq_resourcemanager_hostnames_new"

#define HAWQ_CONFFILE_MASTER_ADDR_DOMAINSOCKET_PORT "hawq_resourcemanager_master_address_domainsocket_port"
#define HAWQ_CONFFILE_MASTER_ADDR_PORT 				"hawq_resourcemanager_master_address_port"
#define HAWQ_CONFFILE_STANDBY_ADDR                  "hawq_resourcemanager_standby_address"
#define HAWQ_CONFFILE_SEGMENT_PORT					"hawq_resourcemanager_segment_port"

/* Property for libYarn */
#define HAWQDRM_CONFFILE_YARN_SERVERADDR			"hawq_rm_yarn_address"
#define HAWQDRM_CONFFILE_YARN_SCHEDULERADDR			"hawq_rm_yarn_scheduler_address"
#define HAWQDRM_CONFFILE_YARN_QUEUE 				"hawq_rm_yarn_queue_name"
#define HAWQDRM_CONFFILE_YARN_APP_NAME  			"hawq_rm_yarn_app_name"

#define HAWQDRM_CONFFILE_SVRTYPE_VAL_YARN  			"yarn"
#define HAWQDRM_CONFFILE_SVRTYPE_VAL_MESOS 			"mesos"
#define HAWQDRM_CONFFILE_SVRTYPE_VAL_NONE			"none"


int  createDRMMemoryContext(void);
int  redirectSysLog(void);

void initializeDRMCore(void);

int  ResManagerMainServer2ndPhase(void);

void printHelpInfo(void);
int  parseCommandLine(int argc, char **argv);

int  loadHAWQClusterConfigure(void);

int	 initializeSocketServer(void);
int  startResourceBroker(bool isForked);
int  processSocketInputs(void);


int  getStringValue(int argc, char **argv, int pos, SimpString *val);

int  addResourceQueueAndUserFromProperties(List *queueprops, List *userprops);

void moveResourceBrokerLogToSysLog(void);

int  MainHandlerLoop(void);
void sendResponseToClients(void);

void updateStatusOfAllNodes(void);

/* HAWQ RM binds maximum count of different addresses to listen connections.  */
#define HAWQRM_SERVER_PORT_COUNT				64

int  ResManagerMainSegment2ndPhase(void);
int  initializeSocketServer_RMSEG(void);
int  MainHandlerLoop_RMSEG(void);
int  MainHandler_RMSEGDummyLoop(void);

#endif //DYNAMIC_RESOURCE_MANAGEMENT_H
