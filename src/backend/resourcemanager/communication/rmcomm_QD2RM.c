#include "communication/rmcomm_QD2RM.h"
#include "dynrm.h"
#include "utils/memutilities.h"
#include "utils/simplestring.h"
#include "utils/linkedlist.h"
#include "utils/nullablebool.h"
#include "nodes/pg_list.h"
#include "commands/defrem.h"
#include "pgstat.h"

#include "communication/rmcomm_MessageHandler.h"
#include "communication/rmcomm_SyncComm.h"
#include "resourcemanager/resourcemanager.h"
#include "resourcemanager/conntrack.h"
#include "resourcemanager/communication/rmcomm_MessageProtocol.h"

#include "funcapi.h"
#include "fmgr.h"
#include "miscadmin.h"

#define DRMQD2RM_MEMORY_CONTEXT_NAME  "QD to RM communication"

#define DRM_IPC_RETRY_TIMES     10
#define DRM_IPC_RETRY_SLEEP_US  100000

#define DRM_IPC_RESOURCE_SET_DEF_SIZE	8
#define DRM_IPC_RESOURCE_SET_MAX_SIZE	1024		/* maximum 1024 internal
													   parallel portals		  */

#define VALIDATE_RESOURCE_SET_INDEX(index, errorbuf, errorbufsize)			   \
	if ( (index) < 0 || (index) >= QD2RM_ResourceSetSize ||					   \
		 QD2RM_ResourceSets[(index)] == NULL ) 								   \
	{					   	   	   											   \
		snprintf((errorbuf), (errorbufsize), 							   	   \
				 "wrong resource set index %d", (index)); 	   		   		   \
		return COMM2RM_CLIENT_WRONG_INPUT;								   	   \
	}

#define RPC_QD_2_RM_HEAD													   \
	initializeQD2RMComm();													   \
    VALIDATE_RESOURCE_SET_INDEX(index, errorbuf, errorbufsize)				   \
    int 			   res 			= FUNC_RETURN_OK;						   \
    SelfMaintainBuffer sendbuffer 	= &(QD2RM_ResourceSets[index]->SendBuffer);\
    SelfMaintainBuffer recvbuffer	= &(QD2RM_ResourceSets[index]->RecvBuffer);

void buildManipulateResQueueRequest(SelfMaintainBuffer sendbuffer,
									uint32_t		   connid,
									char 			  *queuename,
									uint16_t 		   action,
									List    		  *options);
void *generateResourceRefreshHeartBeat(void *arg);

int callSyncRPCToRM(const char 	 	   *sendbuff,
					int   		 		sendbuffsize,
		  	  	    uint16_t			sendmsgid,
					uint16_t 		  	exprecvmsgid,
					SelfMaintainBuffer	recvsmb);

/*
 *------------------------------------------------------------------------------
 * Functions for testing resource manager by playing actions.
 *------------------------------------------------------------------------------
 */
struct TestActionConnData
{
	char  	  ConnectionName[64];
    int       ResourceID;
	int32_t   ConnectionID;
	ListCell *CurAction;
	List 	 *Actions;
};
typedef struct TestActionConnData  TestActionConnData;
typedef struct TestActionConnData *TestActionConn;

struct TestActionItemData
{
	char  ActionName[64];
	List *Arguments;
	int   ResultCode;
	char *ResultMessage;
};
typedef struct TestActionItemData  TestActionItemData;
typedef struct TestActionItemData *TestActionItem;

struct TestActionPlayData
{
	List 	 *ActionConns;
	ListCell *ActionConnCell;
	ListCell *ActionItemCell;
};

typedef struct TestActionPlayData  TestActionPlayData;
typedef struct TestActionPlayData *TestActionPlay;

#define RESOURCE_ACTION_PLAY_REGISTER			"register"
#define RESOURCE_ACTION_PLAY_ALLOCATE			"allocate"
#define RESOURCE_ACTION_PLAY_RETURN				"return"
#define RESOURCE_ACTION_PLAY_UNREGISTER			"unregister"

#define RESOURCE_ACTION_PLAY_WAIT				"wait"
#define RESOURCE_ACTION_PLAY_CREATE				"create"
#define RESOURCE_ACTION_PLAY_REMOVE				"remove"

#define RESOURCE_ACTION_RPC_FAULT				"rpcfault"
#define RESOURCE_ACTION_RPC_FAULT_RM			"rpcrmfault"

#define PG_PLAY_RESOURCE_ACTION_COLUMNS 5
#define PG_PLAY_RESOURCE_ACTION_BUFSIZE 1024

#define RESOURCE_ACTION_PLAY_ALLOCATE_OUT		"/tmp/allocate"

int loadTestActionScript(const char *filename, List **actions);
int runTestActionScript(List *actions, const char *filename);
int findFile(const char *filename);
int createFile(const char *filename);
int removeFile(const char *filename);

void outputAllcatedResourceToFile(const char *filename, int resourceid);
void *buildResourceActionPlayRowData(MCTYPE context, List *actions);
void freeResourceActionPlayRowData(MCTYPE context, TestActionPlay *actplay);
/*
 *------------------------------------------------------------------------------
 * Functions for UDF of explaining resource distribution.
 *------------------------------------------------------------------------------
 */

struct ResourceDistRowData {
	void        *pointerkey;
	char 		*hostname;
	int32_t		 segcount;
	int32_t		 segmem;
	double		 segcore;
	char        *mappedname;
	int32_t      splitcount;
};

typedef struct ResourceDistRowData  ResourceDistRowData;
typedef struct ResourceDistRowData *ResourceDistRow;

#define PG_EXPLAIN_RESOURCE_DISTRIBUTION_COLUMNS 6
#define PG_EXPLAIN_RESOURCE_DISTRIBUTION_BUFSIZE 256

DQueue buildResourceDistRowData(MCTYPE 				context,
								int 				resourceid,
								HostnameVolumnInfo *volinfo,
								int 				infosize);

/*
 *------------------------------------------------------------------------------
 * Global Variables.
 *
 * Postmaster side global variables saving the data not necessarily always sent
 * from resource manager.
 *------------------------------------------------------------------------------
 */

extern char 	   *UnixSocketDir;		  	/* Reference global configure.   */

MemoryContext		QD2RM_CommContext			  = NULL;

char				QD2RM_SocketFile[1024];	/* Unix domain socket file.      */

QDResourceContext  *QD2RM_ResourceSets            = NULL;
int					QD2RM_ResourceSetSize         = 0;
int					QD2RM_ResourceSetCount        = 0;
uint64_t			QD2RM_LastRefreshResourceTime = 0;

bool				QD2RM_Initialized			  = false;

pthread_t       	ResourceHeartBeatThreadHandle;
pthread_mutex_t 	ResourceSetsMutex;
uint64_t        	LastSendResourceRefreshHeartBeatTime = 0;


/**
 * Do necessary initialization for coming RPC communication between QD and RM.
 */
void initializeQD2RMComm(void)
{
	if ( QD2RM_Initialized )
		return;

	int res = FUNC_RETURN_OK;

    /* create dynamic resource manager instance to contain config data. */
    res = createDRMInstance();
    if ( res != FUNC_RETURN_OK )
    {
    	elog(ERROR, "Fail to initialize data structure for communicating with "
                	"resource manager.");
    }


    MEMORY_CONTEXT_SWITCH_TO(TopMemoryContext)
    QD2RM_CommContext = AllocSetContextCreate( CurrentMemoryContext,
            DRMQD2RM_MEMORY_CONTEXT_NAME,
            ALLOCSET_DEFAULT_MINSIZE,
            ALLOCSET_DEFAULT_INITSIZE,
            ALLOCSET_DEFAULT_MAXSIZE );
    Assert( QD2RM_CommContext != NULL );
    MEMORY_CONTEXT_SWITCH_BACK

	DRMGlobalInstance->Context = QD2RM_CommContext;

    res = initializeDRMInstanceForQD();
    if ( res != FUNC_RETURN_OK )
    {
		elog(ERROR, "Fail to initialize data structure for communicating with "
					"resource manager.");
    }

    /* Get UNIX domain socket file. */
    UNIXSOCK_PATH(QD2RM_SocketFile, rm_master_domain_port, UnixSocketDir);

    /* Initialize global variables for maintaining a list of resource sets. */
    QD2RM_ResourceSets 	   = rm_palloc0(QD2RM_CommContext,
            							sizeof(QDResourceContext) *
										DRM_IPC_RESOURCE_SET_DEF_SIZE);
    QD2RM_ResourceSetSize  = DRM_IPC_RESOURCE_SET_DEF_SIZE;
    QD2RM_ResourceSetCount = 0;
    for ( int i = 0 ; i < QD2RM_ResourceSetSize ; ++i )
    {
        QD2RM_ResourceSets[i] = NULL;
    }

    initializeSyncRPCComm();

    /* Init mutex for accessing resource sets. */
    if ( pthread_mutex_init(&ResourceSetsMutex, NULL) != 0 )
    {
    	elog(ERROR, "Fail to build mutex for communication with resource manager.");
    }

    /* Start resource heart-beat thread. */
    if ( rm_session_lease_heartbeat_enable )
    {
		if ( pthread_create(&ResourceHeartBeatThreadHandle,
							NULL,
							generateResourceRefreshHeartBeat,
							NULL) != 0)
		{
			elog(ERROR, "Fail to create background thread for communication with "
						"resource manager.");
		}
    }

    initializeMessageHandlers();

    QD2RM_Initialized = true;
}

int createNewResourceContext(int *index)
{
	initializeQD2RMComm();

	pthread_mutex_lock(&ResourceSetsMutex);
    /* Decide if should extend the array. The size is always doubled. 		  */
    /* TODO: Limit the maximum size of the array QD2RM_ResourceSets. 		  */
    if ( QD2RM_ResourceSetCount >= QD2RM_ResourceSetSize )
    {
        if ( QD2RM_ResourceSetSize >= DRM_IPC_RESOURCE_SET_MAX_SIZE )
        {
        	pthread_mutex_unlock(&ResourceSetsMutex);
        	return COMM2RM_CLIENT_FULL_RESOURCECONTEXT;
        }
        QD2RM_ResourceSets = rm_repalloc(QD2RM_CommContext,
                QD2RM_ResourceSets,
                sizeof(QDResourceContext) *
                QD2RM_ResourceSetSize * 2);

        for ( int i = QD2RM_ResourceSetSize ; i < QD2RM_ResourceSetSize * 2 ; ++i )
        {
            QD2RM_ResourceSets[i] = NULL;
        }

        QD2RM_ResourceSetSize = QD2RM_ResourceSetSize << 1;
    }

    /* Find one available slot with NULL set. */
    int availableIndex = 0;
    while( availableIndex < QD2RM_ResourceSetSize &&
            QD2RM_ResourceSets[availableIndex] != NULL)
    {
        availableIndex++;
    }

    /* Build new instance and initialize the properties. */
    QDResourceContext newrs = rm_palloc0(QD2RM_CommContext,
            							 sizeof(QDResourceContextData));
    QD2RM_ResourceSets[availableIndex] = newrs;

    newrs->QD_Conn_ID 		= INVALID_CONNID;
    newrs->QD_Resource		= NULL;
    newrs->QD_SegCore		= 0.0;
    newrs->QD_SegMemoryMB	= 0;
    newrs->QD_SegCount		= 0;
    newrs->QD_HdfsHostNames = NULL;
    newrs->QD_HostCount		= 0;
    newrs->QD_ResourceList  = NULL;

    initializeSelfMaintainBuffer(&(newrs->SendBuffer), QD2RM_CommContext);
    initializeSelfMaintainBuffer(&(newrs->RecvBuffer), QD2RM_CommContext);

    QD2RM_ResourceSetCount++;
    *index = availableIndex;

    pthread_mutex_unlock(&ResourceSetsMutex);

    return FUNC_RETURN_OK;
}

int releaseResourceContext(int index)
{
	initializeQD2RMComm();

	pthread_mutex_lock(&ResourceSetsMutex);

    if ( index < 0 || index >= QD2RM_ResourceSetSize ||
            QD2RM_ResourceSets[index] == NULL )
    {
    	pthread_mutex_unlock(&ResourceSetsMutex);
    	return COMM2RM_CLIENT_WRONG_INPUT;
    }


    destroySelfMaintainBuffer(&(QD2RM_ResourceSets[index]->SendBuffer));
    destroySelfMaintainBuffer(&(QD2RM_ResourceSets[index]->RecvBuffer));

    rm_pfree(QD2RM_CommContext, QD2RM_ResourceSets[index]);
    QD2RM_ResourceSets[index] = NULL;

    QD2RM_ResourceSetCount--;

    pthread_mutex_unlock(&ResourceSetsMutex);

    return FUNC_RETURN_OK;

}

void releaseResourceContextWithErrorReport(int index)
{
	int res = releaseResourceContext(index);
	if ( res != FUNC_RETURN_OK )
	{
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				errmsg("Can not release resource context.")));
	}
}

int getAllocatedResourceContext(int index, QDResourceContext *rescontext)
{
	initializeQD2RMComm();

    if ( index < 0 || index >= QD2RM_ResourceSetSize )
    {
        return COMM2RM_CLIENT_WRONG_INPUT;
    }
    *rescontext = QD2RM_ResourceSets[index];
    return FUNC_RETURN_OK;
}

int cleanupQD2RMComm(void)
{
	int res = FUNC_RETURN_OK;
	char errorbuf[1024];

	initializeQD2RMComm();

	pthread_mutex_lock(&ResourceSetsMutex);
    for ( int i = 0 ; i < QD2RM_ResourceSetSize ; ++i )
    {
        if ( QD2RM_ResourceSets[i] != NULL )
        {
            if ( QD2RM_ResourceSets[i]->QD_ResourceList != NULL )
            {
            	elog(LOG, "Un-returned resource is probed, will be returned. "
                          "(%d MB, %lf CORE) x %d. Conn ID=%d",
                          QD2RM_ResourceSets[i]->QD_SegMemoryMB,
                          QD2RM_ResourceSets[i]->QD_SegCore,
                          QD2RM_ResourceSets[i]->QD_SegCount,
                          QD2RM_ResourceSets[i]->QD_Conn_ID);

                res = returnResource(i, errorbuf, sizeof(errorbuf));
                if ( res != FUNC_RETURN_OK )
                {
                	elog(WARNING, "Failed to return resource when cleaning up "
                				  "resource context.");
            	}
                res = unregisterConnectionInRM(i, errorbuf, sizeof(errorbuf));
                if ( res != FUNC_RETURN_OK )
                {
                	elog(WARNING, "Failed to unregister when cleaning up "
                				  "resource context.");
                }
            }
        }
    }
    pthread_mutex_unlock(&ResourceSetsMutex);

    return FUNC_RETURN_OK;
}

/*
 * REGISTER CONNECTION by USER NAME.
 */
int registerConnectionInRMByStr(int 		   index,
								const char 	  *userid,
								char		  *errorbuf,
								int		 	   errorbufsize)
{
	RPC_QD_2_RM_HEAD

    /* Build request. */
    resetSelfMaintainBuffer(sendbuffer);
    appendSMBStr(sendbuffer, userid);
    appendSelfMaintainBufferTill64bitAligned(sendbuffer);

    /* Call RPC. */
    res = callSyncRPCToRM(sendbuffer->Buffer,
    					  getSMBContentSize(sendbuffer),
						  REQUEST_QD_CONNECTION_REG,
						  RESPONSE_QD_CONNECTION_REG,
						  recvbuffer);

    if ( res != FUNC_RETURN_OK )
    {
    	snprintf(errorbuf, errorbufsize,
    			 "failed to register in HAWQ resource manager because of %s.",
				 getErrorCodeExplain(res));
    	return res;
    }

    /* Parse response. */
    RPCResponseHeadRegisterConnectionInRMByStr response =
        (RPCResponseHeadRegisterConnectionInRMByStr)(recvbuffer->Buffer);

    QD2RM_ResourceSets[index]->QD_Conn_ID  = response->ConnID;
    if ( response->Result != FUNC_RETURN_OK )
    {
    	snprintf(errorbuf, errorbufsize,
    			 "failed to register in HAWQ resource manager because of %s.",
				 getErrorCodeExplain(response->Result));
    	return response->Result;
    }

    elog(DEBUG3, "Registered in HAWQ resource manager, Conn ID %d",
    			 QD2RM_ResourceSets[index]->QD_Conn_ID);
    return FUNC_RETURN_OK;
}

/*
 * REGISTER CONNECTION by USER OID.
 */
int registerConnectionInRMByOID(int 		   index,
								uint64_t 	   useridoid,
								char		  *errorbuf,
								int		 	   errorbufsize)
{
	RPC_QD_2_RM_HEAD

    /* Build request. */
    RPCRequestHeadRegisterConnectionInRMByOIDData requesthead;
    requesthead.UseridOid = useridoid;
    resetSelfMaintainBuffer(sendbuffer);
    appendSMBVar(sendbuffer,requesthead);

    /* Call RPC to get response. */
    res = callSyncRPCToRM(sendbuffer->Buffer,
    					  getSMBContentSize(sendbuffer),
						  REQUEST_QD_CONNECTION_REG_OID,
						  RESPONSE_QD_CONNECTION_REG_OID,
						  recvbuffer);
    if ( res != FUNC_RETURN_OK )
    {
    	snprintf(errorbuf, errorbufsize,
    			 "failed to register in HAWQ resource manager because of %s.",
				 getErrorCodeExplain(res));
    	return res;
    }

    /* Parse response. */
    RPCResponseHeadRegisterConnectionInRMByOID response =
    		(RPCResponseHeadRegisterConnectionInRMByOID)(recvbuffer->Buffer);

    QD2RM_ResourceSets[index]->QD_Conn_ID  = response->ConnID;
    if ( response->Result != FUNC_RETURN_OK )
    {
    	snprintf(errorbuf, errorbufsize,
    			 "failed to register in HAWQ resource manager because of %s.",
				 getErrorCodeExplain(response->Result));
    	return response->Result;
    }

    elog(DEBUG3, "Registered in HAWQ resource manager (By OID), Conn ID %d",
        		 QD2RM_ResourceSets[index]->QD_Conn_ID);
    return FUNC_RETURN_OK;
}

/*
 * UNREGISTER CONNECTION.
 */
int	unregisterConnectionInRM(int 			   index,
							 char		  	  *errorbuf,
							 int		 	   errorbufsize)
{
	RPC_QD_2_RM_HEAD

    /* Build request. */
    RPCRequestHeadUnregisterConnectionInRMData requesthead;
    requesthead.ConnID =QD2RM_ResourceSets[index]->QD_Conn_ID;

    resetSelfMaintainBuffer(sendbuffer);
    appendSMBVar(sendbuffer,requesthead);

    /* Call RPC to get response. */
    res = callSyncRPCToRM(sendbuffer->Buffer,
    					  getSMBContentSize(sendbuffer),
						  REQUEST_QD_CONNECTION_UNREG,
						  RESPONSE_QD_CONNECTION_UNREG,
						  recvbuffer);
    if ( res != FUNC_RETURN_OK )
    {
    	snprintf(errorbuf, errorbufsize,
    			 "failed to unregister in HAWQ resource manager because of %s.",
				 getErrorCodeExplain(res));
    	return res;
    }

    /* Parse response. */
    RPCResponseHeadUnregisterConnectionInRM response =
    		(RPCResponseHeadUnregisterConnectionInRM)(recvbuffer->Buffer);
    if ( response->Result != FUNC_RETURN_OK )
    {
    	res = response->Result;
    	snprintf(errorbuf, errorbufsize,
    			 "failed to unregister in HAWQ resource manager because of %s.",
				 getErrorCodeExplain(response->Result));
    }

    elog(DEBUG3, "Unregistered in HAWQ resource manager. Conn ID %d",
        		 QD2RM_ResourceSets[index]->QD_Conn_ID);

    QD2RM_ResourceSets[index]->QD_Conn_ID = INVALID_CONNID;
    return res;
}

void unregisterConnectionInRMWithErrorReport(int index)
{
	static char errorbuf[1024];
	int res = unregisterConnectionInRM(index, errorbuf, sizeof(errorbuf));
	if (res != FUNC_RETURN_OK)
	{
		ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR), errmsg("%s",errorbuf)));
	}
}

/*
 * ACQUIRE QUERY RESOURCE
 */
int acquireResourceFromRM(int 		  		  index,
						  int			  	  sessionid,
						  int			  	  slice_size,
						  int64_t			  iobytes,
						  HostnameVolumnInfo *preferred_nodes,
						  int				  preferred_nodes_size,
						  uint32_t    		  max_seg_count_fix,
						  uint32_t			  min_seg_count_fix,
						  char	     		 *errorbuf,
						  int	      		  errorbufsize)
{
	RPC_QD_2_RM_HEAD

    QDResourceContext curcontext  = QD2RM_ResourceSets[index];
    uint32_t		  nodecount   = (preferred_nodes == NULL ||
    								 preferred_nodes_size == 0) ?
    								0 :
									preferred_nodes_size;

    elog(DEBUG3, "Acquire request with Conn ID %d for index %d. "
    			 "Max vseg size %d Min vseg size %d"
    			 "Estimated slice size %d "
    			 "IO bytes size " INT64_FORMAT " "
				 "Preferred node count %d.",
				 curcontext->QD_Conn_ID,
				 index,
				 max_seg_count_fix,
				 min_seg_count_fix,
				 slice_size,
				 iobytes,
				 nodecount);

    /* Build request. */
    resetSelfMaintainBuffer(sendbuffer);
    /********** STEP1. Request message head ***********************************/
    RPCRequestHeadAcquireResourceFromRMData requesthead;
    requesthead.SessionID   	 = sessionid;
    requesthead.ConnID      	 = curcontext->QD_Conn_ID;
    requesthead.NodeCount   	 = nodecount;
    requesthead.MaxSegCountFix   = max_seg_count_fix;
    requesthead.MinSegCountFix   = min_seg_count_fix;
    requesthead.SliceSize  	 	 = slice_size;
    requesthead.VSegLimitPerSeg	 = rm_nvseg_perquery_perseg_limit;
    requesthead.VSegLimit		 = rm_nvseg_perquery_limit;
    requesthead.Reserved		 = 0;
    requesthead.IOBytes		 	 = iobytes;
    requesthead.StatNVSeg		 = rm_stmt_nvseg;

    requesthead.StatVSegMemoryMB = 0;
    int parseres = FUNC_RETURN_OK;
    SimpString valuestr;
    setSimpleStringRef(&valuestr, rm_stmt_vseg_mem_str, strlen(rm_stmt_vseg_mem_str));
    parseres = SimpleStringToStorageSizeMB(&valuestr,
    									   &(requesthead.StatVSegMemoryMB));
    Assert(parseres == FUNC_RETURN_OK);

    appendSMBVar(sendbuffer,requesthead);

    /********** STEP2. Preferred node scan size in MB *************************/
    /* Send each host scan size in MB. */
    for ( int i = 0 ; i < nodecount ; ++i )
    {
        appendSMBVar(sendbuffer,preferred_nodes[i].datavolumn);
    }

    /********** STEP3. Preferred node host names ******************************/
    /* Send host names. splitted by '\0' */
    for ( int i = 0 ; i < nodecount ; ++i )
    {
        appendSMBStr(sendbuffer,preferred_nodes[i].hostname);
    }
    /* send pad to ensure 64-bit aligned. */
    appendSelfMaintainBufferTill64bitAligned(sendbuffer);

    pgstat_report_waiting_resource(true);

    /* Call RPC to get response. */
    res = callSyncRPCToRM(sendbuffer->Buffer,
    					  getSMBContentSize(sendbuffer),
						  REQUEST_QD_ACQUIRE_RESOURCE,
						  RESPONSE_QD_ACQUIRE_RESOURCE,
						  recvbuffer);
    if ( res != FUNC_RETURN_OK )
    {
    	snprintf(errorbuf, errorbufsize,
    			 "failed to acquire resource because of %s.",
				 getErrorCodeExplain(res));
    	pgstat_report_waiting_resource(false);
    	return res;
    }
    pgstat_report_waiting_resource(false);

    RPCResponseAcquireResourceFromRMERROR errres =
    		(RPCResponseAcquireResourceFromRMERROR)(recvbuffer->Buffer);
    if ( errres->Result != FUNC_RETURN_OK )
    {
    	snprintf(errorbuf, errorbufsize,
    			 "failed to acquire resource because of %s.",
    			 getErrorCodeExplain(errres->Result));
    	return errres->Result;
    }

    /* Parse response. */
    RPCResponseHeadAcquireResourceFromRM response =
    		(RPCResponseHeadAcquireResourceFromRM)(recvbuffer->Buffer);

    curcontext->QD_SegCount	    = response->SegCount;
    curcontext->QD_SegMemoryMB 	= response->SegMemoryMB;
    curcontext->QD_SegCore 		= response->SegCore;
    curcontext->QD_Resource 	= (char *)response;
    curcontext->QD_HostCount	= response->HostCount;

    if ( curcontext->QD_SegCount > 0 )
    {
    	/* Build local HDFS hostname array that will be referenced by QDMachineId
    	 * instances. */
    	if ( nodecount > 0 )
    	{
    		curcontext->QD_HdfsHostNames =
    				(char **)rm_palloc0(QD2RM_CommContext,
    									sizeof(char *) * nodecount);
			int hnameidx = 0;
			for ( int i = 0 ; i < nodecount ; ++i )
			{
				HostnameVolumnInfo *info = &preferred_nodes[i];
				int hostnamelen = strlen(info->hostname);
				curcontext->QD_HdfsHostNames[hnameidx] =
						(char *)rm_palloc0(QD2RM_CommContext, hostnamelen + 1);
				strncpy(curcontext->QD_HdfsHostNames[hnameidx],
						info->hostname,
						hostnamelen);
				hnameidx++;
			}
    	}

        /* Get block of hdfs hostname index array. */
        uint32_t *hnameidxarray = (uint32_t *)
								  (recvbuffer->Buffer +
								   sizeof(RPCResponseHeadAcquireResourceFromRMData));
        uint32_t hnameidxarraysize = __SIZE_ALIGN64(sizeof(uint32_t) * curcontext->QD_SegCount);
        /* Get block of machine id instance offset array. */
        uint32_t *hoffsetarray = (uint32_t *)
								 (recvbuffer->Buffer +
								  sizeof(RPCResponseHeadAcquireResourceFromRMData) +
								  hnameidxarraysize);

        /* This is an array of pointers of MachineId. */
    	curcontext->QD_ResourceList = (QDSegInfo *)
    								  rm_palloc0(QD2RM_CommContext,
    										     sizeof(QDSegInfo) * curcontext->QD_SegCount);

        for ( int i = 0 ; i < curcontext->QD_SegCount ; ++i )
        {
        	QDSegInfo newqdseg = (QDSegInfo)
								 rm_palloc0(QD2RM_CommContext,
											sizeof(QDSegInfoData));
        	newqdseg->QD_HdfsHostName = (hnameidxarray[i] < nodecount) ?
        								curcontext->QD_HdfsHostNames[hnameidxarray[i]] :
										NULL;
        	newqdseg->QD_SegInfo = (SegInfo)(recvbuffer->Buffer + hoffsetarray[i]);
        	curcontext->QD_ResourceList[i] = newqdseg;

        	if ( log_min_messages == DEBUG5 )
        	{
				SelfMaintainBufferData segreport;
				initializeSelfMaintainBuffer(&segreport, QD2RM_CommContext);
				generateSegInfoReport(curcontext->QD_ResourceList[i]->QD_SegInfo,
									  &segreport);
				elog(DEBUG5, "Recognized resource on host. %s. "
							 "Mapped original HDFS host name %s",
							 segreport.Buffer,
							 (curcontext->QD_ResourceList[i]->QD_HdfsHostName != NULL ?
							  curcontext->QD_ResourceList[i]->QD_HdfsHostName	:
							  "UNSET"));
				destroySelfMaintainBuffer(&segreport);
        	}
        }
        elog(DEBUG3, "Acquired resource from HAWQ RM, (%d MB, %lf CORE) x %d.",
        			 curcontext->QD_SegMemoryMB,
					 curcontext->QD_SegCore,
					 curcontext->QD_SegCount);
    }
    else
    {
    	elog(WARNING, "Can not acquire resource from HAWQ RM.");
    	Assert( false );
    }
    return FUNC_RETURN_OK;
}

bool alreadyReturnedResource(int index)
{
	initializeQD2RMComm();

	Assert( index >= 0 && index < QD2RM_ResourceSetSize );
	return QD2RM_ResourceSets[index] == NULL;
}

/*
 * RETURN QUERY RESOURCE
 */
int returnResource(int 		index,
				   char	   *errorbuf,
				   int	    errorbufsize)
{
	RPC_QD_2_RM_HEAD

    /* Build request. */
    RPCRequestHeadReturnResourceData requesthead;
    requesthead.ConnID = QD2RM_ResourceSets[index]->QD_Conn_ID;
    requesthead.Reserved = 0;
    resetSelfMaintainBuffer(sendbuffer);
    appendSMBVar(sendbuffer,requesthead);
    appendSelfMaintainBufferTill64bitAligned(sendbuffer);

    /* Call RPC to get response. */
    res = callSyncRPCToRM(sendbuffer->Buffer,
    					  getSMBContentSize(sendbuffer),
						  REQUEST_QD_RETURN_RESOURCE,
						  RESPONSE_QD_RETURN_RESOURCE,
						  recvbuffer);
    if ( res != FUNC_RETURN_OK )
    {
    	snprintf(errorbuf, errorbufsize,
    			 "failed to return resource to HAWQ resource manager because of %s.",
				 getErrorCodeExplain(res));
    	return res;
    }

    /* Parse response. */
    RPCResponseHeadReturnResource response =
    		(RPCResponseHeadReturnResource)(recvbuffer->Buffer);
    if ( response->Result != FUNC_RETURN_OK )
    {
    	res = response->Result;
        snprintf(errorbuf, errorbufsize,
        		 "failed to return resource to HAWQ resource manager because of %s.",
				 getErrorCodeExplain(res));
        return res;
    }

    elog(DEBUG3, "Returned resource to HAWQ resource manager.");

    QD2RM_ResourceSets[index]->QD_SegMemoryMB 	= 0;
    QD2RM_ResourceSets[index]->QD_SegCore	 	= 0.0;
    QD2RM_ResourceSets[index]->QD_SegCount		= 0;

    if ( QD2RM_ResourceSets[index]->QD_ResourceList != NULL )
        rm_pfree(QD2RM_CommContext, QD2RM_ResourceSets[index]->QD_ResourceList);

    QD2RM_ResourceSets[index]->QD_Resource = NULL;
    QD2RM_ResourceSets[index]->QD_ResourceList = NULL;

    return FUNC_RETURN_OK;
}

int hasAllocatedResource(int index, bool *allocated)
{
	initializeQD2RMComm();

	char errorbuf[1024];
    /* Validate index */
    VALIDATE_RESOURCE_SET_INDEX(index, errorbuf, sizeof(errorbuf))

	*allocated = QD2RM_ResourceSets[index]->QD_SegCount > 0;
    return FUNC_RETURN_OK;
}

/*
 * MANIPULATE RESOURCE QUEUE
 *  For CREATE RESORUCE QUEUE, "withoutliststart" is the last one.
 *  For ALTER RESOURCE QUEUE, "withoutliststart" may has more consequent nodes.
 */
int manipulateResourceQueue(int 	 index,
							char 	*queuename,
							uint16_t action,
							List    *options,
							char	*errorbuf,
							int		 errorbufsize)
{
	RPC_QD_2_RM_HEAD

	/* Build request. */
	buildManipulateResQueueRequest(sendbuffer,
								   QD2RM_ResourceSets[index]->QD_Conn_ID,
								   queuename,
								   action,
								   options);

    /* Call RPC to get response. */
    res = callSyncRPCToRM(sendbuffer->Buffer,
    					  getSMBContentSize(sendbuffer),
						  REQUEST_QD_DDL_MANIPULATERESQUEUE,
						  RESPONSE_QD_DDL_MANIPULATERESQUEUE,
						  recvbuffer);
    if ( res != FUNC_RETURN_OK )
    {
    	snprintf(errorbuf, errorbufsize, "%s", getErrorCodeExplain(res));
    	return res;
    }

	/*Start parsing response. */
	RPCResponseHeadManipulateResQueue response =
		(RPCResponseHeadManipulateResQueue)(recvbuffer->Buffer);

	/* CASE 1. The response contains error message. */
	if ( response->Result != FUNC_RETURN_OK )
	{
		RPCResponseHeadManipulateResQueueERROR error =
			(RPCResponseHeadManipulateResQueueERROR)(recvbuffer->Buffer);

		elog(LOG, "Fail to manipulate resource queue because %s",
					  error->ErrorText);
		snprintf(errorbuf, errorbufsize, "%s", error->ErrorText);
	}

	elog(DEBUG3, "Manipulated resource queue and got result %d", response->Result);

	return response->Result;
}

int manipulateRoleForResourceQueue (int 	  index,
									Oid 	  roleid,
									Oid 	  queueid,
									uint16_t  action,
									uint8_t   isSuperUser,
									char	 *rolename,
									char 	 *errorbuf,
									int  	  errorbufsize)
{
	initializeQD2RMComm();

    Assert(queueid != -1);
	Assert(action == MANIPULATE_ROLE_RESQUEUE_CREATE ||
		   action == MANIPULATE_ROLE_RESQUEUE_ALTER ||
		   action == MANIPULATE_ROLE_RESQUEUE_DROP);

	int res = FUNC_RETURN_OK;
	SelfMaintainBuffer sendbuffer 	= &(QD2RM_ResourceSets[index]->SendBuffer);
	SelfMaintainBuffer recvbuffer	= &(QD2RM_ResourceSets[index]->RecvBuffer);

	RPCRequestHeadManipulateRoleData request;

	resetSelfMaintainBuffer(sendbuffer);
	prepareSelfMaintainBuffer(sendbuffer,
							  sizeof(RPCRequestHeadManipulateRoleData),
							  true);

	memset(&request, 0, sizeof(RPCRequestHeadManipulateRoleData));
	request.QueueOID = queueid;
	request.RoleOID = roleid;
	request.isSuperUser = isSuperUser;
	request.Action = action;
	if (strlen(rolename) < sizeof(request.Name))
	{
		strncpy(request.Name, rolename, strlen(rolename));
	}
	else
	{
		elog(WARNING, "Resource manager finds in valid role name %s.", rolename);
		snprintf(errorbuf, errorbufsize, "invalid role name %s.", rolename);
		return RESQUEMGR_NO_USERID;
	}

	elog(DEBUG3, "Resource manager (manipulateRoleForResourceQueue) "
				 "role oid:%d, queueID:%d, isSuper:%d, roleName:%s, action:%d",
				 request.RoleOID, request.QueueOID, request.isSuperUser,
				 request.Name, request.Action);

	appendSMBVar(sendbuffer, request);

	res = callSyncRPCToRM(sendbuffer->Buffer,
						  sendbuffer->Cursor + 1,
						  REQUEST_QD_DDL_MANIPULATEROLE,
						  RESPONSE_QD_DDL_MANIPULATEROLE,
						  recvbuffer);

    if ( res != FUNC_RETURN_OK )
    {
    	snprintf(errorbuf, errorbufsize, "%s", getErrorCodeExplain(res));
    	return res;
    }

	/* Start parsing response. */
	RPCResponseHeadManipulateRole response = (RPCResponseHeadManipulateRole)
											 (recvbuffer->Buffer);

	/* The response contains error message. */
	if ( response->Result != FUNC_RETURN_OK )
	{
		RPCResponseHeadManipulateRoleERROR error =
			(RPCResponseHeadManipulateRoleERROR)(recvbuffer->Buffer);

		elog(WARNING, "Resource manager failed to manipulate role %s. %s",
					  rolename,
					  error->ErrorText);
		snprintf(errorbuf, errorbufsize, "%s", error->ErrorText);
	}
	return response->Result;
}

void buildManipulateResQueueRequest(SelfMaintainBuffer sendbuffer,
									uint32_t		   connid,
									char 			  *queuename,
									uint16_t 		   action,
									List    		  *options)
{
	Assert( sendbuffer != NULL );
	Assert( connid != 0XFFFFFFFF );
	Assert( queuename != NULL );
	Assert( action >= MANIPULATE_RESQUEUE_CREATE && action <= MANIPULATE_RESQUEUE_DROP );

	uint16_t  withlength 	 	= 0;
	bool	  nowIsWithOption 	= false;
	bool	  need_free_value 	= false;
	ListCell *option	   		= NULL;

	resetSelfMaintainBuffer(sendbuffer);
	prepareSelfMaintainBuffer(sendbuffer,
							  sizeof(RPCRequestHeadManipulateResQueueData),
							  true);

	/* Build request head information. */
	RPCRequestHeadManipulateResQueue requestheadptr =
		(RPCRequestHeadManipulateResQueue)(sendbuffer->Buffer);

	requestheadptr->ConnID 			 = connid;
	requestheadptr->ManipulateAction = action;

	jumpforwardSelfMaintainBuffer(sendbuffer,
								  sizeof(RPCRequestHeadManipulateResQueueData));

	/* Build queue name string. */
	appendSMBStr(sendbuffer,queuename);

	/* Build request with and without attribute list information. In case DROP
	 * RESOURCE QUEUE, there is no options passed in. */
	if ( options != NULL ) {
		foreach(option, options)
		{
			DefElem    *defel = (DefElem *) lfirst(option);
			if ( strcmp(defel->defname, WITHLISTSTART_TAG) == 0 ) {
				nowIsWithOption = true;
				continue;
			}

			if ( strcmp(defel->defname, WITHOUTLISTSTART_TAG) == 0 ) {
				nowIsWithOption = false;
				continue;
			}

			/* Count how many options are WITH options. The left ones must be
			 * WITHOUT options. This is not checked here, Parser guarantees this
			 * order. */
			withlength = withlength + (nowIsWithOption ? 1 : 0);

			/* Convert defname to lower case. */
			for ( int i = 0 ; defel->defname[i] != '\0' ; ++i ) {
				defel->defname[i] = tolower(defel->defname[i]);
			}

			/* Append attribute keyword string. */
			appendSMBStr(sendbuffer,defel->defname);

			/* Append attribute value string. */
			if ( nowIsWithOption ) {
				char *attrvalue = defGetString(defel, &need_free_value);
				appendSMBStr(sendbuffer, attrvalue);
				elog(DEBUG3, "added attribute value string %s", attrvalue);
			}
		}
	}

    appendSelfMaintainBufferTill64bitAligned(sendbuffer);

    /* Update with actual with list size. */
    requestheadptr = (RPCRequestHeadManipulateResQueue)(sendbuffer->Buffer);
    requestheadptr->WithAttrLength = withlength;

    elog(DEBUG3, "WITH length is %d.", withlength);

    Assert(((sendbuffer->Cursor + 1) & 0X7) == 0 );
}

#define GET_STR(textp) DatumGetCString(DirectFunctionCall1(textout, PointerGetDatum(textp)))

void sendFailedNodeToResourceManager(int hostNum, char **pghost) {

	initializeQD2RMComm();

	int res    = FUNC_RETURN_OK;

	SelfMaintainBufferData sendBuffer;
	SelfMaintainBufferData recvBuffer;
	initializeSelfMaintainBuffer(&sendBuffer, QD2RM_CommContext);
	initializeSelfMaintainBuffer(&recvBuffer, QD2RM_CommContext);

	for ( int i = 0 ; i < hostNum ; ++i ) {
		appendSMBStr(&sendBuffer, pghost[i]);
		elog(LOG, "HAWQ RM :: QD thinks %s is down.", pghost[i]);
	}
	appendSelfMaintainBufferTill64bitAligned(&sendBuffer);

	elog(LOG, "HAWQ RM :: QD sends %d failed host(s) to resource manager.",
				 hostNum);

	res = callSyncRPCToRM(sendBuffer.Buffer,
						  sendBuffer.Cursor + 1,
						  REQUEST_QD_SEGMENT_ISDOWN,
						  RESPONSE_QD_SEGMENT_ISDOWN,
						  &recvBuffer);

    if ( res != FUNC_RETURN_OK )
    {
    	elog(LOG, "Fail to get response from resource manager RPC. %d", res);
    	goto exit;
    }

    elog(LOG, "Success for QD sending failed host to resource manager.");

exit:
	destroySelfMaintainBuffer(&sendBuffer);
	destroySelfMaintainBuffer(&recvBuffer);
}

int getLocalTmpDirFromMasterRM()
{
    initializeQD2RMComm();

	int 				   res 		   = FUNC_RETURN_OK;
	SelfMaintainBufferData sendBuffer;
	SelfMaintainBufferData recvBuffer;
	initializeSelfMaintainBuffer(&sendBuffer, QD2RM_CommContext);
	initializeSelfMaintainBuffer(&recvBuffer, QD2RM_CommContext);

    RPCRequestTmpDirForQDData   request;
    request.Reserved = 0;
	appendSMBVar(&sendBuffer, request);

	res = callSyncRPCToRM(sendBuffer.Buffer,
						  sendBuffer.Cursor + 1,
						  REQUEST_QD_TMPDIR,
						  RESPONSE_QD_TMPDIR,
						  &recvBuffer);
    if ( res != FUNC_RETURN_OK ) 
    {
        elog(ERROR, "getLocalTmpDirFromMasterRM fail");
    }

    RPCResponseTmpDirForQD response = (RPCResponseTmpDirForQD)(recvBuffer.Buffer);

    if ( response->Result != FUNC_RETURN_OK ) 
    {
    	goto exit;
    }

    LocalTempPath = pstrdup(response->tmpdir);

    if (LocalTempPath)
    {
        elog(LOG, "getLocalTmpDirFromMasterRM tmpdir:%s", LocalTempPath);
    }
    else
    {
        elog(LOG, "getLocalTmpDirFromMasterRM tmpdir:NULL");
    }

exit:
	destroySelfMaintainBuffer(&sendBuffer);
	destroySelfMaintainBuffer(&recvBuffer);
	return res;
}

int acquireResourceQuotaFromRM(int64_t		user_oid,
							   uint32_t		max_seg_count_fix,
							   uint32_t		min_seg_count_fix,
							   int	       *errorcode,
							   char	       *errorbuf,
							   int			errorbufsize,
							   uint32_t	   *seg_num,
							   uint32_t	   *seg_num_min,
							   uint32_t	   *seg_memory_mb,
							   double	   *seg_core)
{
	initializeQD2RMComm();

	int 				   res 		   = FUNC_RETURN_OK;
	SelfMaintainBufferData sendBuffer;
	SelfMaintainBufferData recvBuffer;
	initializeSelfMaintainBuffer(&sendBuffer, QD2RM_CommContext);
	initializeSelfMaintainBuffer(&recvBuffer, QD2RM_CommContext);

	RPCRequestHeadAcquireResourceQuotaFromRMByOIDData request;
	request.UseridOid		 = user_oid;
	request.MaxSegCountFix	 = max_seg_count_fix;
	request.MinSegCountFix	 = min_seg_count_fix;
	request.VSegLimitPerSeg	 = rm_nvseg_perquery_perseg_limit;
	request.VSegLimit		 = rm_nvseg_perquery_limit;
	request.StatNVSeg		 = rm_stmt_nvseg;

	request.StatVSegMemoryMB = 0;
	int parseres = FUNC_RETURN_OK;
	SimpString valuestr;
	setSimpleStringRef(&valuestr, rm_stmt_vseg_mem_str, strlen(rm_stmt_vseg_mem_str));
	parseres = SimpleStringToStorageSizeMB(&valuestr,
										   &(request.StatVSegMemoryMB));
	Assert(parseres == FUNC_RETURN_OK);

	appendSMBVar(&sendBuffer, request);

	elog(DEBUG3, "HAWQ RM :: Acquire resource quota for query with %d splits, "
				 "%d preferred virtual segments by user "INT64_FORMAT,
				 max_seg_count_fix,
				 min_seg_count_fix,
				 user_oid);

	res = callSyncRPCToRM(sendBuffer.Buffer,
						  sendBuffer.Cursor + 1,
						  REQUEST_QD_ACQUIRE_RESOURCE_QUOTA,
						  RESPONSE_QD_ACQUIRE_RESOURCE_QUOTA,
						  &recvBuffer);
    if ( res != FUNC_RETURN_OK )
    {
    	snprintf(errorbuf, errorbufsize,
    			 "failed to get response from resource manager RPC.");
    	*errorcode = res;
    	goto exit;
    }

    RPCResponseHeadAcquireResourceQuotaFromRMByOID response =
    	(RPCResponseHeadAcquireResourceQuotaFromRMByOID)(recvBuffer.Buffer);
    if ( response->Result == FUNC_RETURN_OK )
    {
    	*seg_num 		= response->SegNum;
    	*seg_num_min 	= response->SegNumMin;
    	*seg_memory_mb  = response->SegMemoryMB;
    	*seg_core		= response->SegCore;
    }
    else
    {
    	res = response->Result;
    	*errorcode = res;
    	snprintf(errorbuf, errorbufsize,
    			 "failed to get resource quota due to remote error %s.",
				 getErrorCodeExplain(res));
    }

exit:
	destroySelfMaintainBuffer(&sendBuffer);
	destroySelfMaintainBuffer(&recvBuffer);
	return res;
}

#define DEFAULT_HEARTBEAT_BUFFER 4096

void *generateResourceRefreshHeartBeat(void *arg)
{
	static char messagehead[16] = {'M' ,'S' ,'G' ,'S' ,'T' ,'A' ,'R' ,'T' ,
								   '\0','\0','\0','\0','\0','\0','\0','\0'};
	static char messagetail[8]  = {'M' ,'S' ,'G' ,'E' ,'N' ,'D' ,'S' ,'!' };

	SelfMaintainBufferData sendbuffer;
	SelfMaintainBufferData contbuffer;

	gp_set_thread_sigmasks();

	pthread_detach(pthread_self());

	initializeSelfMaintainBuffer(&sendbuffer, NULL);
	initializeSelfMaintainBuffer(&contbuffer, NULL);
	prepareSelfMaintainBuffer(&sendbuffer, DEFAULT_HEARTBEAT_BUFFER, true);
	prepareSelfMaintainBuffer(&contbuffer, DEFAULT_HEARTBEAT_BUFFER, true);

	while( true ) {

		resetSelfMaintainBuffer(&sendbuffer);
		resetSelfMaintainBuffer(&contbuffer);
		bool sendcontent = false;

		/* Lock to access array of resource sets */
		pthread_mutex_lock(&ResourceSetsMutex);

		RPCRequestHeadRefreshResourceHeartBeatData request;
		request.ConnIDCount = QD2RM_ResourceSetCount;
		request.Reserved    = 0;
		appendSMBVar(&contbuffer, request);

		/* Get all current in-use resource set IDs and build into request. */
	    for ( int i = 0 ; i < QD2RM_ResourceSetSize ; ++i ) {
	        if ( QD2RM_ResourceSets[i] == NULL ||
	        	 QD2RM_ResourceSets[i]->QD_Conn_ID == INVALID_CONNID )
	        {
	        	continue;
	        }
	        appendSMBVar(&contbuffer, QD2RM_ResourceSets[i]->QD_Conn_ID);
	        sendcontent = true;
	    }
		/* Unlock */
		pthread_mutex_unlock(&ResourceSetsMutex);

		/* Build final request content and send out. */
		appendSelfMaintainBufferTill64bitAligned(&contbuffer);

		if ( sendcontent )
		{
			int fd = -1;
			int res = connectToServerRemote(master_addr_host, rm_master_port, &fd);
			if ( res == FUNC_RETURN_OK )
			{
				RMMessageHead phead = (RMMessageHead)messagehead;
				RMMessageTail ptail = (RMMessageTail)messagetail;
				phead->Mark1       = 0;
				phead->Mark2       = 0;
				phead->MessageID   = REQUEST_QD_REFRESH_RESOURCE;
				phead->MessageSize = contbuffer.Cursor + 1;

				appendSelfMaintainBuffer(&sendbuffer, (char *)phead, sizeof(*phead));
				appendSelfMaintainBuffer(&sendbuffer, contbuffer.Buffer, contbuffer.Cursor+1);
				appendSelfMaintainBuffer(&sendbuffer, (char *)ptail, sizeof(*ptail));

				if ( sendWithRetry(fd, sendbuffer.Buffer, sendbuffer.Cursor+1, false) == FUNC_RETURN_OK) {
					RPCResponseRefreshResourceHeartBeatData response;
					/* Do not care response at all. */
					char recvbuf[16 + 8 + sizeof(response)];
					if (recvWithRetry(fd, recvbuf, sizeof(recvbuf), false) != FUNC_RETURN_OK)
					{
					  write_log("generateResourceRefreshHeartBeat recv error (errno %d)", errno);
					}
				}
				else
				{
				  write_log("generateResourceRefreshHeartBeat send error (errno %d)", errno);
				}
			}
			closeConnectionRemote(&fd);
		}
		pg_usleep(rm_session_lease_heartbeat_interval * 1000000);
	}

	return 0;
}

#define PG_RESQUEUE_STATUS_COLUMNS  10
#define PG_RESQUEUE_STATUS_BUFSIZE  1024

Datum pg_resqueue_status(PG_FUNCTION_ARGS)
{
	FuncCallContext		   *funcctx = NULL;
	Datum					result;
	MemoryContext			oldcontext = NULL;
	HeapTuple				tuple = NULL;
    int                     res = FUNC_RETURN_OK;

	if (SRF_IS_FIRSTCALL())
	{

		funcctx = SRF_FIRSTCALL_INIT();

		/* Switch context when allocating stuff to be used in later calls */
		oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

	    /*
         * Call RPC begin
         */
        initializeQD2RMComm();
        SelfMaintainBufferData sendBuffer;
        SelfMaintainBufferData recvBuffer;
        initializeSelfMaintainBuffer(&sendBuffer, QD2RM_CommContext);
        initializeSelfMaintainBuffer(&recvBuffer, QD2RM_CommContext);

        RPCRequestResQueueStatusData request;
        request.Reserved = 0;
        appendSMBVar(&sendBuffer, request);

    	res = callSyncRPCToRM(sendBuffer.Buffer,
							  sendBuffer.Cursor + 1,
							  REQUEST_QD_DUMP_RESQUEUE_STATUS,
							  RESPONSE_QD_DUMP_RESQUEUE_STATUS,
							  &recvBuffer);
        if ( res != FUNC_RETURN_OK )
        {
            destroySelfMaintainBuffer(&sendBuffer);
            destroySelfMaintainBuffer(&recvBuffer);
		    funcctx->max_calls = 0;
            SRF_RETURN_DONE(funcctx);
        }

        RPCResponseResQueueStatus response = (RPCResponseResQueueStatus)(recvBuffer.Buffer);

        if ( response->Result != FUNC_RETURN_OK )
        {
            destroySelfMaintainBuffer(&sendBuffer);
            destroySelfMaintainBuffer(&recvBuffer);
		    funcctx->max_calls = 0;
            SRF_RETURN_DONE(funcctx);
        }
       
        DQueue funcdata = createDQueue(funcctx->multi_call_memory_ctx);
        for (int i=0;i<response->queuenum;i++) 
        {
            ResQueueStatus resq = rm_palloc(funcctx->multi_call_memory_ctx,
            							    sizeof(ResQueueStatusData));
            sprintf(resq->name, "%s", response->queuedata[i].name); 
            resq->segmem = response->queuedata[i].segmem;
            resq->segcore = response->queuedata[i].segcore;
            resq->segsize = response->queuedata[i].segsize;
            resq->segsizemax = response->queuedata[i].segsizemax;
            resq->inusemem = response->queuedata[i].inusemem;
            resq->inusecore = response->queuedata[i].inusecore;
            resq->holders = response->queuedata[i].holders;
            resq->waiters = response->queuedata[i].waiters;
            resq->pausedispatch = response->queuedata[i].pausedispatch;
            insertDQueueTailNode(funcdata, resq);
        }        

        destroySelfMaintainBuffer(&sendBuffer);
        destroySelfMaintainBuffer(&recvBuffer);
        /*
         * Call RPC end
         */
        funcctx->user_fctx = (void *)funcdata; 
        
        TupleDesc tupledesc = CreateTemplateTupleDesc(
									PG_RESQUEUE_STATUS_COLUMNS,
									false);
		TupleDescInitEntry(tupledesc, (AttrNumber) 1,  "rsqname",  	TEXTOID, -1, 0);
		TupleDescInitEntry(tupledesc, (AttrNumber) 2,  "segmem", 	TEXTOID, -1, 0);
		TupleDescInitEntry(tupledesc, (AttrNumber) 3,  "segcore", 	TEXTOID, -1, 0);
		TupleDescInitEntry(tupledesc, (AttrNumber) 4,  "segsize",   TEXTOID, -1, 0);
		TupleDescInitEntry(tupledesc, (AttrNumber) 5,  "segsizemax",TEXTOID, -1, 0);
		TupleDescInitEntry(tupledesc, (AttrNumber) 6,  "inusemem",  TEXTOID, -1, 0);
		TupleDescInitEntry(tupledesc, (AttrNumber) 7,  "inusecore", TEXTOID, -1, 0);
		TupleDescInitEntry(tupledesc, (AttrNumber) 8,  "rsqholders",TEXTOID, -1, 0);
		TupleDescInitEntry(tupledesc, (AttrNumber) 9,  "rsqwaiters",TEXTOID, -1, 0);
		TupleDescInitEntry(tupledesc, (AttrNumber) 10, "paused",	TEXTOID, -1, 0);

		funcctx->tuple_desc = BlessTupleDesc(tupledesc);

		/* Return to original context when allocating transient memory */
		MemoryContextSwitchTo(oldcontext);

		funcctx->max_calls = ((DQueue)(funcctx->user_fctx))->NodeCount;
	}

	funcctx = SRF_PERCALL_SETUP();

	if (funcctx->call_cntr < funcctx->max_calls)
	{
		Datum		values[PG_RESQUEUE_STATUS_COLUMNS];
		bool		nulls[PG_RESQUEUE_STATUS_COLUMNS];
        char        buf[PG_RESQUEUE_STATUS_BUFSIZE];

        for (int i=0;i<PG_RESQUEUE_STATUS_COLUMNS;i++)
        {
            nulls[i] = false;
        }

        DQueue funcdata = (DQueue)(funcctx->user_fctx);
        ResQueueStatus resq = (ResQueueStatus)getDQueueNodeDataByIndex(funcdata, funcctx->call_cntr);

		values[0] = PointerGetDatum(cstring_to_text(resq->name));
        snprintf(buf, sizeof(buf), "%d", resq->segmem);
        values[1] = PointerGetDatum(cstring_to_text(buf));	
        snprintf(buf, sizeof(buf), "%f", resq->segcore);
        values[2] = PointerGetDatum(cstring_to_text(buf));	
        snprintf(buf, sizeof(buf), "%d", resq->segsize);
        values[3] = PointerGetDatum(cstring_to_text(buf));	
        snprintf(buf, sizeof(buf), "%d", resq->segsizemax);
        values[4] = PointerGetDatum(cstring_to_text(buf));	
        snprintf(buf, sizeof(buf), "%d", resq->inusemem);
        values[5] = PointerGetDatum(cstring_to_text(buf));	
        snprintf(buf, sizeof(buf), "%f", resq->inusecore);
        values[6] = PointerGetDatum(cstring_to_text(buf));	
        snprintf(buf, sizeof(buf), "%d", resq->holders);
        values[7] = PointerGetDatum(cstring_to_text(buf));	
        snprintf(buf, sizeof(buf), "%d", resq->waiters);
        values[8] = PointerGetDatum(cstring_to_text(buf));
        snprintf(buf, sizeof(buf), "%c", resq->pausedispatch);
        values[9] = PointerGetDatum(cstring_to_text(buf));
	
        /* Build and return the tuple. */
		tuple = heap_form_tuple(funcctx->tuple_desc, values, nulls);
		result = HeapTupleGetDatum(tuple);

		SRF_RETURN_NEXT(funcctx, result);
	}
	else {
		SRF_RETURN_DONE(funcctx);
	}
}

Datum pg_resqueue_status_kv(PG_FUNCTION_ARGS)
{
	return 0;
}

void dumpResourceManagerStatus(uint32_t type, const char *dump_file)
{
    initializeQD2RMComm();

    int                    res         = FUNC_RETURN_OK;
    SelfMaintainBufferData sendBuffer;
    SelfMaintainBufferData recvBuffer;
    initializeSelfMaintainBuffer(&sendBuffer, QD2RM_CommContext);
    initializeSelfMaintainBuffer(&recvBuffer, QD2RM_CommContext);

    RPCRequestDumpStatusData   request;
    request.type = type;
    request.Reserved = 0;
    strncpy(request.dump_file, dump_file, sizeof(request.dump_file) - 1);
    appendSMBVar(&sendBuffer, request);

   	res = callSyncRPCToRM(sendBuffer.Buffer,
						  sendBuffer.Cursor + 1,
						  REQUEST_QD_DUMP_STATUS,
						  RESPONSE_QD_DUMP_STATUS,
						  &recvBuffer);
   	if ( res != FUNC_RETURN_OK )
   	{
   		goto exit;
   	}

    RPCResponseDumpStatus response = (RPCResponseDumpStatus)(recvBuffer.Buffer);

    if ( response->Result != FUNC_RETURN_OK ) 
    {
        goto exit;
    }

exit:
    destroySelfMaintainBuffer(&sendBuffer);
    destroySelfMaintainBuffer(&recvBuffer);
}

extern Datum pg_explain_resource_distribution(PG_FUNCTION_ARGS)
{
	FuncCallContext		   *funcctx = NULL;
	Datum					result;
	MemoryContext			oldcontext = NULL;
	HeapTuple				tuple = NULL;
	SimpString				role;
	int						fixsegcountmin = 0;
	int						fixsegcountmax = 0;
	int						splitsize;
	int						slicesize;
	SimpString				locality;
	char					errorbuf[1024];

	if (SRF_IS_FIRSTCALL())
	{

		funcctx = SRF_FIRSTCALL_INIT();

		/* Switch context when allocating stuff to be used in later calls */
		oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

		/*
		 * Get arguments
		 * pg_explain_resource_distribution('gpadmin',-1,1200,
		 * 									‘host1,500,host2,300,host3,100,...’)
		 *
		 */
		if ( PG_ARGISNULL(0) )
		{
	       	ereport(ERROR,
	                (errcode(ERRCODE_E_R_I_E_NULL_VALUE_NOT_ALLOWED),
	                 errmsg("Role name must be specified."),
	                 errhint("Restart the server and try again")));
		}

		initSimpleString(&role, funcctx->multi_call_memory_ctx);
		setSimpleStringNoLen(&role, GET_STR(PG_GETARG_TEXT_P(0)));

		slicesize = 0;
		if ( !PG_ARGISNULL(1) )
		{
			slicesize = PG_GETARG_INT32(1);
		}

		splitsize = 0;
		if ( !PG_ARGISNULL(2) )
		{
			splitsize = PG_GETARG_INT32(2);
		}

		fixsegcountmin = 0;
		if ( !PG_ARGISNULL(3) )
		{
			fixsegcountmin = PG_GETARG_INT32(3);
		}

		fixsegcountmax = 0;
		if ( !PG_ARGISNULL(4) )
		{
			fixsegcountmax = PG_GETARG_INT32(4);
		}

		initSimpleString(&locality, funcctx->multi_call_memory_ctx);
		if ( !PG_ARGISNULL(5) )
		{
			setSimpleStringNoLen(&locality, GET_STR(PG_GETARG_TEXT_P(5)));
		}

		/* Call HAWQ RM RPC to get expected result. */
		int ret;
		int resourceId = -1;
		/* STEP 1. Create Context */
		ret = createNewResourceContext(&resourceId);
		if ( ret != FUNC_RETURN_OK )
		{
			elog(ERROR, "Fail to create resource context. %d", ret);
		}
		/* STEP 2. Register. */
		ret = registerConnectionInRMByStr(resourceId,
										  role.Str,
										  errorbuf, sizeof(errorbuf));
		if ( ret != FUNC_RETURN_OK )
		{
			elog(ERROR, "Fail to register by role %s", role.Str);
		}
		/* STEP 3. Acquire resource. */

		/* Build locality array. */
		int infosize = 0;
		HostnameVolumnInfo  *volinfo = NULL;
		int toksize  = 0;
		SimpStringPtr tokens = NULL;
		SimpleStringTokens(&locality, ',', &tokens, &toksize);
		if (toksize > 1 && toksize % 2 == 0)
		{
			infosize = toksize / 2;
			volinfo = rm_palloc(funcctx->multi_call_memory_ctx,
								sizeof(HostnameVolumnInfo) * infosize);
			for ( int i = 0 ; i < infosize ; ++i )
			{
				SimpStringPtr token1 = &(tokens[(i<<1)]);
				SimpStringPtr token2 = &(tokens[(i<<1)+1]);
				strcpy(volinfo[i].hostname, token1->Str);
				SimpleStringToInt64(token2, &(volinfo[i].datavolumn));
			}
		}
		freeSimpleStringTokens(&locality, &tokens, toksize);

		ret = acquireResourceFromRM(resourceId,
									gp_session_id,
									slicesize,
									splitsize,
									volinfo,
									infosize,
									fixsegcountmax,
									fixsegcountmin,
									errorbuf,
									sizeof(errorbuf));
		if ( ret != FUNC_RETURN_OK )
		{
			elog(ERROR, "%s", errorbuf);
		}

		/* Build result. */
		funcctx->user_fctx = (void *)buildResourceDistRowData(
										funcctx->multi_call_memory_ctx,
										resourceId,
										volinfo,
										infosize);

		/* STEP 4. Return resource. */
		ret = returnResource(resourceId, errorbuf, sizeof(errorbuf));
		if ( ret != FUNC_RETURN_OK )
		{
			elog(ERROR, "failed to return resource back to resource manager "
					    "because %s",
						errorbuf);
		}

		/* STEP 5. Unregister. */
		ret = unregisterConnectionInRM(resourceId, errorbuf, sizeof(errorbuf));
		if ( ret != FUNC_RETURN_OK )
		{
			elog(ERROR, "failed to unregister connection in RM because %s",
						errorbuf);
		}

		/* STEP 6. Remove Context */
		releaseResourceContext(resourceId);

		/* STEP 7. Construct a tuple descriptor for the result rows. */
		TupleDesc tupledesc = CreateTemplateTupleDesc(
									PG_EXPLAIN_RESOURCE_DISTRIBUTION_COLUMNS,
									false);
		TupleDescInitEntry(tupledesc, (AttrNumber) 1, "hostname",  		 TEXTOID, -1, 0);
		TupleDescInitEntry(tupledesc, (AttrNumber) 2, "vsegcount", 		 TEXTOID, -1, 0);
		TupleDescInitEntry(tupledesc, (AttrNumber) 3, "memory_mb", 		 TEXTOID, -1, 0);
		TupleDescInitEntry(tupledesc, (AttrNumber) 4, "core",            TEXTOID, -1, 0);
		TupleDescInitEntry(tupledesc, (AttrNumber) 5, "mapped_hostname", TEXTOID, -1, 0);
		TupleDescInitEntry(tupledesc, (AttrNumber) 6, "splitcount",      TEXTOID, -1, 0);

		funcctx->tuple_desc = BlessTupleDesc(tupledesc);

		/* Return to original context when allocating transient memory */
		MemoryContextSwitchTo(oldcontext);

		funcctx->max_calls = ((DQueue)(funcctx->user_fctx))->NodeCount;
	}

	funcctx = SRF_PERCALL_SETUP();

	if (funcctx->call_cntr < funcctx->max_calls)
	{
		Datum		values[PG_EXPLAIN_RESOURCE_DISTRIBUTION_COLUMNS];
		bool		nulls[PG_EXPLAIN_RESOURCE_DISTRIBUTION_COLUMNS];
		char		buf[PG_EXPLAIN_RESOURCE_DISTRIBUTION_BUFSIZE];

		nulls[0] = false;
		nulls[1] = false;
		nulls[2] = false;
		nulls[3] = false;
		nulls[4] = true;
		nulls[5] = true;

		/* Go to the expected row to return. */
		DQueue restable = (DQueue)(funcctx->user_fctx);
		ResourceDistRow resrow = (ResourceDistRow)
								 getDQueueNodeDataByIndex(restable, funcctx->call_cntr);

		values[0] = PointerGetDatum(cstring_to_text(resrow->hostname));
		snprintf(buf, sizeof(buf), "%d", resrow->segcount);
		values[1] = PointerGetDatum(cstring_to_text(buf));
		snprintf(buf, sizeof(buf), "%d", resrow->segmem);
		values[2] = PointerGetDatum(cstring_to_text(buf));
		snprintf(buf, sizeof(buf), "%f", resrow->segcore);
		values[3] = PointerGetDatum(cstring_to_text(buf));
		if ( resrow->mappedname != NULL )
		{
			nulls[4] = false;
			nulls[5] = false;
			values[4] = PointerGetDatum(cstring_to_text(resrow->mappedname));
			snprintf(buf, sizeof(buf), "%d", resrow->splitcount);
			values[5] = PointerGetDatum(cstring_to_text(buf));
		}

		/* Build and return the tuple. */
		tuple = heap_form_tuple(funcctx->tuple_desc, values, nulls);
		result = HeapTupleGetDatum(tuple);

		SRF_RETURN_NEXT(funcctx, result);
	}
	else
	{
		SRF_RETURN_DONE(funcctx);
	}
}

DQueue buildResourceDistRowData(MCTYPE 				context,
								int 				resourceid,
								HostnameVolumnInfo *volinfo,
								int 				infosize)
{
	QDResourceContext rescontext = NULL;
	getAllocatedResourceContext(resourceid, &rescontext);

	DQueue result = createDQueue(context);

	for ( int i = 0 ; i < rescontext->QD_SegCount ; ++i ) {
		bool foundold = false;
		DQUEUE_LOOP_BEGIN(result, iter, ResourceDistRow, resrow)
			if ( resrow->pointerkey == rescontext->QD_ResourceList[i]->QD_SegInfo ) {
				resrow->segcount++;
				resrow->segmem += rescontext->QD_SegMemoryMB;
				resrow->segcore += rescontext->QD_SegCore;
				foundold = true;
				break;
			}
		DQUEUE_LOOP_END

		if ( foundold )
			continue;
		ResourceDistRow newrow = rm_palloc0(context, sizeof(ResourceDistRowData));
		newrow->pointerkey  = rescontext->QD_ResourceList[i]->QD_SegInfo;
		newrow->hostname    = (char *)
						      rm_palloc0(context,
								   	     rescontext->QD_ResourceList[i]->QD_SegInfo->HostNameLen + 1);
		strcpy(newrow->hostname,
			   GET_SEGINFO_HOSTNAME(rescontext->QD_ResourceList[i]->QD_SegInfo));
		newrow->segcount    = 1;
		newrow->segmem      = rescontext->QD_SegMemoryMB;
		newrow->segcore     = rescontext->QD_SegCore;
		newrow->mappedname  = NULL;

		if ( rescontext->QD_ResourceList[i]->QD_HdfsHostName != NULL ) {
			newrow->mappedname =
				rm_palloc0(context,
						   strlen(rescontext->QD_ResourceList[i]->QD_HdfsHostName) + 1);
			strcpy(newrow->mappedname,
				   rescontext->QD_ResourceList[i]->QD_HdfsHostName);
			newrow->splitcount = 0;

			for ( int j = 0 ; j < infosize ; ++j ) {
				if ( strcmp(volinfo[j].hostname, newrow->mappedname) == 0 ) {
					newrow->splitcount = volinfo[j].datavolumn;
					break;
				}
			}
		}
		insertDQueueTailNode(result, newrow);
	}

	return result;
}

void *buildResourceActionPlayRowData(MCTYPE			context,
									 List		   *actions)
{
	TestActionPlay userdata = (TestActionPlay)
							  rm_palloc0(context, sizeof(TestActionPlayData));
	userdata->ActionConns = actions;
	userdata->ActionConnCell = list_head(actions);
	if (userdata->ActionConnCell != NULL)
	{
		TestActionConn actconn = (TestActionConn)lfirst(userdata->ActionConnCell);
		userdata->ActionItemCell = list_head(actconn->Actions);
	}
	return userdata;
}

void freeResourceActionPlayRowData(MCTYPE context, TestActionPlay *actplay)
{
	MEMORY_CONTEXT_SWITCH_TO(context)
	/* Free each action item and then its action conn instance. */
	ListCell *cell = NULL;
	foreach(cell, (*actplay)->ActionConns)
	{
		TestActionConn actconn = (TestActionConn)lfirst(cell);
		ListCell *itemcell = NULL;
		foreach(itemcell, actconn->Actions)
		{
			TestActionItem actitem = (TestActionItem)lfirst(itemcell);
			if ( actitem->ResultMessage != NULL )
			{
				rm_pfree(context, actitem->ResultMessage);
			}

			ListCell *argcell = NULL;
			foreach(argcell, actitem->Arguments)
			{
				char *argstr = (char *)lfirst(argcell);
				rm_pfree(context, argstr);
			}
			list_free(actitem->Arguments);
			actitem->Arguments = NULL;
			rm_pfree(context, actitem);
		}
		list_free(actconn->Actions);
		actconn->Actions = NULL;
		rm_pfree(context, actconn);
	}
	rm_pfree(context, *actplay);
	*actplay = NULL;

	MEMORY_CONTEXT_SWITCH_BACK
}
extern Datum pg_play_resource_action(PG_FUNCTION_ARGS)
{
	FuncCallContext		   *funcctx 	= NULL;
	Datum					result  	= 0;
	MemoryContext			oldcontext 	= NULL;
	SimpString				actfile;
	SimpString				outfile;
	List 				   *actions 	= NULL;
	HeapTuple				tuple 		= NULL;

	if (SRF_IS_FIRSTCALL())
	{
		funcctx = SRF_FIRSTCALL_INIT();
		/* Switch context when allocating stuff to be used in later calls. */
		oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);
		/* Get arguments. */
		if ( PG_ARGISNULL(0) ) {
	       	ereport(ERROR,
	                (errcode(ERRCODE_E_R_I_E_NULL_VALUE_NOT_ALLOWED),
	                 errmsg("input action file name must be specified."),
	                 errhint("Specify correct file name.")));
		}

		initSimpleString(&actfile, funcctx->multi_call_memory_ctx);
		setSimpleStringNoLen(&actfile, GET_STR(PG_GETARG_TEXT_P(0)));
		initSimpleString(&outfile, funcctx->multi_call_memory_ctx);
		setSimpleStringNoLen(&outfile, GET_STR(PG_GETARG_TEXT_P(1)));

		initializeQD2RMComm();

		/* Load action script. */
		int res = loadTestActionScript(actfile.Str, &actions);
		if ( res != FUNC_RETURN_OK )
		{
			elog(ERROR, "Fail to load resource play actions from %s.", actfile.Str);
		}

		/* Perform action script. */
		runTestActionScript(actions, outfile.Str);

		/* Collect results. connname, action, actionfull, resultcode, resultmessage. */
		ListCell *cell = NULL;
		int actsize = 0;
		foreach(cell, actions)
		{
			TestActionConn actconn = (TestActionConn)lfirst(cell);
			actsize += list_length(actconn->Actions);
		}

		elog(LOG, "Total action item size is %d", actsize);

		/* STEP 7. Construct a tuple descriptor for the result rows. */
		TupleDesc tupledesc = CreateTemplateTupleDesc(PG_PLAY_RESOURCE_ACTION_COLUMNS, false);
		TupleDescInitEntry(tupledesc, (AttrNumber) 1, "conn",  		TEXTOID, -1, 0);
		TupleDescInitEntry(tupledesc, (AttrNumber) 2, "action", 	TEXTOID, -1, 0);
		TupleDescInitEntry(tupledesc, (AttrNumber) 3, "actionfull", TEXTOID, -1, 0);
		TupleDescInitEntry(tupledesc, (AttrNumber) 4, "result",     TEXTOID, -1, 0);
		TupleDescInitEntry(tupledesc, (AttrNumber) 5, "message", 	TEXTOID, -1, 0);

		funcctx->tuple_desc = BlessTupleDesc(tupledesc);

		/* Return to original context when allocating transient memory */
		MemoryContextSwitchTo(oldcontext);

		/* Build result. */
		funcctx->max_calls = actsize;

		/* Initialize pointer to get ready for returning rows. */
		funcctx->user_fctx = buildResourceActionPlayRowData(QD2RM_CommContext,
															actions);


	}

	funcctx = SRF_PERCALL_SETUP();

	if (funcctx->call_cntr < funcctx->max_calls)
	{
		Datum		values[PG_PLAY_RESOURCE_ACTION_COLUMNS];
		bool		nulls[PG_PLAY_RESOURCE_ACTION_COLUMNS];
		char		buf[PG_PLAY_RESOURCE_ACTION_BUFSIZE];

		nulls[0] = false;
		nulls[1] = false;
		nulls[2] = false;
		nulls[3] = false;
		nulls[4] = true;

		/* Go to the expected row to return. */
		TestActionPlay userdata = (TestActionPlay)(funcctx->user_fctx);

		if ( userdata->ActionItemCell == NULL )
		{
			freeResourceActionPlayRowData(QD2RM_CommContext, &userdata);
			SRF_RETURN_DONE(funcctx);
		}

		TestActionConn conn = (TestActionConn)lfirst(userdata->ActionConnCell);
		TestActionItem item = (TestActionItem)lfirst(userdata->ActionItemCell);

		values[0] = PointerGetDatum(cstring_to_text(conn->ConnectionName));
		values[1] = PointerGetDatum(cstring_to_text(item->ActionName));

		strcpy(buf, item->ActionName);
		ListCell *cell = NULL;
		foreach(cell, item->Arguments)
		{
			char * argstr = (char *)lfirst(cell);
			if ( strlen(argstr) + strlen(buf) + 1 < sizeof(buf) )
			{
				strcat(buf, "$");
				strcat(buf, argstr);
			}
		}

		values[2] = PointerGetDatum(cstring_to_text(buf));
		snprintf(buf, sizeof(buf), "%d", item->ResultCode);
		values[3] = PointerGetDatum(cstring_to_text(buf));
		if ( item->ResultMessage != NULL )
		{
			nulls[4] = false;
			values[4] = PointerGetDatum(cstring_to_text(item->ResultMessage));
		}

		/* Build and return the tuple. */
		tuple = heap_form_tuple(funcctx->tuple_desc, values, nulls);
		result = HeapTupleGetDatum(tuple);

		/* Try next action item. */
		userdata->ActionItemCell = lnext(userdata->ActionItemCell);
		if ( userdata->ActionItemCell == NULL )
		{
			userdata->ActionConnCell = lnext(userdata->ActionConnCell);
			if( userdata->ActionConnCell != NULL )
			{
				TestActionConn conn = (TestActionConn)lfirst(userdata->ActionConnCell);
				userdata->ActionItemCell = list_head(conn->Actions);
			}
		}
		SRF_RETURN_NEXT(funcctx, result);
	}
	else
	{
		TestActionPlay userdata = (TestActionPlay)(funcctx->user_fctx);
		freeResourceActionPlayRowData(QD2RM_CommContext, &userdata);
		SRF_RETURN_DONE(funcctx);
	}
}


int findFile(const char *filename)
{
	struct stat buff;
	return stat(filename, &buff) == 0 ? FUNC_RETURN_OK : FUNC_RETURN_FAIL;
}

int createFile(const char *filename)
{
	int fd = 0;
	fd = open(filename, O_RDWR|O_CREAT);
	int res = fd > 0 ? FUNC_RETURN_OK : FUNC_RETURN_FAIL;
	if ( fd >= 0 )
	{
		close(fd);
	}
	return res;
}

int removeFile(const char *filename)
{
	int res = unlink(filename);
	return res == 0 ? FUNC_RETURN_OK : FUNC_RETURN_FAIL;
}

int loadTestActionScript(const char *filename, List **actions)
{
	Assert(actions != NULL && *actions == NULL);

	ListCell *cell 	   = NULL;
	ListCell *itemcell = NULL;
	ListCell *argcell  = NULL;

	char 	  line[1024];
	FILE 	  *fp = fopen(filename, "r");
	if ( fp == NULL )
	{
		return FUNC_RETURN_FAIL;
	}

	elog(LOG, "Start loading action file %s", filename);

	MEMORY_CONTEXT_SWITCH_TO(QD2RM_CommContext)

	while( fgets(line, sizeof(line)-1, fp) != NULL )
	{
		/* Remove white space at the end of the line. */
		int linesize = strlen(line);
		while( line[linesize-1] == '\n' ||
			   line[linesize-1] == '\r' ||
			   line[linesize-1] == '\t' ||
			   line[linesize-1] == ' ' )
		{
			line[linesize-1] = '\0';
			linesize--;
		}

		elog(LOG, "Loaded action line : %s", line);

		TestActionItem newitem = (TestActionItem)
								 rm_palloc0(QD2RM_CommContext,
											sizeof(TestActionItemData));
		newitem->Arguments 		= NULL;
		newitem->ResultCode 	= FUNC_NOT_EXECUTED;
		newitem->ResultMessage 	= NULL;

		/* Split based on ':' and build action item. */
		char *brk  	 = NULL;
		char *word 	 = NULL;
		int   argidx = 0;
		char  connname[64];
		for ( word = strtok_r(line, "$", &brk) ;
			  word ;
			  word = strtok_r(NULL, "$", &brk), argidx++ )
		{
			if ( argidx == 0 )
			{
				/* it is a connection name. */
				strncpy(connname, word, sizeof(connname)-1);
				elog(LOG, "Get action play argument connection name %s", connname);
			}
			else  if ( argidx == 1 )
			{
				/* it is an action name. */
				if ( strlen(word) < sizeof(newitem->ActionName) )
				{
					strcpy(newitem->ActionName, word);
					elog(LOG, "Get action play argument action name %s", newitem->ActionName);
				}
				else
				{
					elog(ERROR, "Too long action name %s that is not legal.",
								newitem->ActionName);
				}
			}
			else
			{
				/* it is an argument string. */
				char *newarg = rm_palloc0(QD2RM_CommContext, strlen(word) + 1);
				strcpy(newarg, word);
				newitem->Arguments = lappend(newitem->Arguments, newarg);
				elog(LOG, "Appended action play argument %s", newarg);
			}
		}

		/* Add action item into the action list. */
		bool found = false;
		foreach(cell, *actions)
		{
			TestActionConn actconn = (TestActionConn)lfirst(cell);
			if ( strcmp(actconn->ConnectionName, connname) == 0 )
			{
				actconn->Actions = lappend(actconn->Actions, newitem);
				if ( actconn->CurAction == NULL )
				{
					actconn->CurAction = list_head(actconn->Actions);
				}
				found = true;
				break;
			}
		}

		if ( !found )
		{
			TestActionConn newactconn = rm_palloc0(QD2RM_CommContext,
												   sizeof(TestActionConnData));
			newactconn->Actions 	 = NULL;
			newactconn->CurAction	 = NULL;
			newactconn->ConnectionID = -1;
			newactconn->Actions 	 = lappend(newactconn->Actions, newitem);
			strcpy(newactconn->ConnectionName, connname);
			*actions = lappend(*actions, newactconn);
			elog(LOG, "Build action play connection %s", connname);
		}
	}

	MEMORY_CONTEXT_SWITCH_BACK

	fclose(fp);

	/* Output all action items to log. */
	foreach(cell, *actions)
	{
		TestActionConn actconn = (TestActionConn)lfirst(cell);
		foreach(itemcell, actconn->Actions)
		{
			TestActionItem actitem = (TestActionItem)lfirst(itemcell);
			SelfMaintainBufferData smb;
			initializeSelfMaintainBuffer(&smb, QD2RM_CommContext);
			appendSMBStr(&smb, actitem->ActionName);
			appendSMBStr(&smb, "::");

			foreach(argcell, actitem->Arguments)
			{
				char *argstr = (char *)lfirst(argcell);
				appendSMBStr(&smb, argstr);
				appendSMBStr(&smb, ",");
			}

			appendSMBStr(&smb, "$");
			elog(LOG, "Loaded action play :: %s", smb.Buffer);
			destroySelfMaintainBuffer(&smb);
		}
	}

	return FUNC_RETURN_OK;
}

int runTestActionScript(List *actions, const char *filename)
{
	char 	  errorbuf[1024];
	int  	  errorcode	= FUNC_RETURN_OK;
	int  	  ret		= FUNC_RETURN_OK;
	ListCell *conncell  = NULL;
	bool 	  alldone   = false;

	errorbuf[0]  = '\0';

	while( !alldone )
	{
		foreach(conncell, actions)
		{
			errorcode	= FUNC_RETURN_OK;
			ret			= FUNC_RETURN_OK;

			/* Try one action connection. */
			TestActionConn actconn = (TestActionConn)lfirst(conncell);
			if ( actconn->CurAction == NULL )
			{
				continue;
			}

			/* Try the first action item. */
			TestActionItem actitem = (TestActionItem)lfirst(actconn->CurAction);
			if ( strcmp(actitem->ActionName, RESOURCE_ACTION_PLAY_WAIT) == 0 )
			{
				const char *filename = (const char *)lfirst(list_head(actitem->Arguments));
				if ( findFile(filename) != FUNC_RETURN_OK )
				{
					/* Can not find the file yet, so , try next possible action
					 * connection. */
					continue;
				}
			}

			/* Move to next action. */
			actconn->CurAction = lnext(actconn->CurAction);

			if ( strcmp(actitem->ActionName, RESOURCE_ACTION_PLAY_WAIT) == 0 )
			{
				/* We have processed the action. */
				ret = FUNC_RETURN_OK;
			}
			else if ( strcmp(actitem->ActionName, RESOURCE_ACTION_PLAY_REGISTER) == 0 )
			{
				/* Create Context */
				ret = createNewResourceContext(&(actconn->ResourceID));
				if ( ret != FUNC_RETURN_OK )
				{
					elog(ERROR, "Fail to create resource context. %d", ret);
				}
				/* STEP 2. Register. */
				char *role = (char *)lfirst(list_head(actitem->Arguments));
				errorbuf[0] = '\0';
				ret = registerConnectionInRMByStr(actconn->ResourceID,
												  role,
												  errorbuf,
												  sizeof(errorbuf));
				if ( ret == FUNC_RETURN_OK )
				{
					actconn->ConnectionID = QD2RM_ResourceSets[actconn->ResourceID]->QD_Conn_ID;
					actitem->ResultMessage = rm_palloc0(QD2RM_CommContext,
														PG_PLAY_RESOURCE_ACTION_BUFSIZE);
					snprintf(actitem->ResultMessage,
							 PG_PLAY_RESOURCE_ACTION_BUFSIZE - 1,
							 "ResourceID:%d,ConnID:%d",
							 actconn->ResourceID,
							 actconn->ConnectionID);
				}
			}
			else if ( strcmp(actitem->ActionName, RESOURCE_ACTION_PLAY_ALLOCATE) == 0 )
			{
				int sessionid	  = 0;
				int slice_size	  = 0;
				int split_size 	  = 0;
				int max_seg_count = 0;
				int min_seg_count = 0;
				int infosize 	  = 0;
				int i 			  = 0;
				char outfilename[512];


				HostnameVolumnInfo  *volinfo = NULL;
				ListCell *cell 	  = NULL;

				outfilename[0] = '\0';

				foreach(cell, actitem->Arguments)
				{
					char *argstr = (char *)lfirst(cell);
					switch(i)
					{
					case 0:
						sessionid = atoi(argstr);
						break;
					case 1:
						slice_size = atoi(argstr);
						break;
					case 2:
						split_size = atoi(argstr);
						break;
					case 3:
						min_seg_count = atoi(argstr);
						break;
					case 4:
						max_seg_count = atoi(argstr);
						break;
					case 5:
					{
						/* Build locality array. */
						SimpString 	  locality;
						SimpStringPtr tokens   = NULL;
						int 		  toksize  = 0;
						initSimpleString(&locality, QD2RM_CommContext);
						setSimpleStringNoLen(&locality, argstr);
						SimpleStringTokens(&locality, ',', &tokens, &toksize);
						if (toksize > 1 && toksize % 2 == 0)
						{
							infosize = toksize / 2;
							volinfo = rm_palloc(QD2RM_CommContext,
												sizeof(HostnameVolumnInfo) * infosize);
							for ( int i = 0 ; i < infosize ; ++i )
							{
								SimpStringPtr token1 = &(tokens[(i<<1)]);
								SimpStringPtr token2 = &(tokens[(i<<1)+1]);
								strcpy(volinfo[i].hostname, token1->Str);
								SimpleStringToInt64(token2, &(volinfo[i].datavolumn));
								elog(LOG, "locality data host %s with %s splits.",
										  token1->Str,
										  token2->Str);
							}
							elog(LOG, "Parse locality data %d hosts", infosize);
						}
						freeSimpleStringTokens(&locality, &tokens, toksize);
						freeSimpleStringContent(&locality);
						break;
					}
					case 6:
						strncpy(outfilename, argstr, sizeof(outfilename)-1);
					}
					i++;
				}

				errorbuf[0] = '\0';
				ret = acquireResourceFromRM(actconn->ResourceID,
											sessionid,
											slice_size,
											split_size,
											volinfo,
											infosize,
											max_seg_count,
											min_seg_count,
											errorbuf,
											sizeof(errorbuf));

				if ( ret == FUNC_RETURN_OK )
				{
					if ( outfilename[0] == '\0')
					{
						/* Set default output file name. */
						snprintf(outfilename, sizeof(outfilename)-1,
								 "%s." UINT64_FORMAT,
								 RESOURCE_ACTION_PLAY_ALLOCATE_OUT,
								 gettime_microsec());
					}

					actitem->ResultMessage = rm_palloc0(QD2RM_CommContext,
														PG_PLAY_RESOURCE_ACTION_BUFSIZE);
					snprintf(actitem->ResultMessage,
							 PG_PLAY_RESOURCE_ACTION_BUFSIZE - 1,
							 "Acquired resource in %s",
							 outfilename);

					/* Output the acquired resource details into the specified file. */
					outputAllcatedResourceToFile(outfilename, actconn->ResourceID);
				}
			}
			else if ( strcmp(actitem->ActionName, RESOURCE_ACTION_PLAY_RETURN) == 0 )
			{
				errorbuf[0] = '\0';
				/* STEP 4. Return resource. */
				ret = returnResource(actconn->ResourceID,
									 errorbuf,
									 sizeof(errorbuf));
			}
			else if ( strcmp(actitem->ActionName, RESOURCE_ACTION_PLAY_UNREGISTER) == 0 )
			{
				errorbuf[0] = '\0';
				/* STEP 5. Unregister. */
				ret = unregisterConnectionInRM(actconn->ResourceID,
											   errorbuf,
											   sizeof(errorbuf));

				/* STEP 6. Remove Context */
				releaseResourceContext(actconn->ResourceID);
				if ( ret == FUNC_RETURN_OK )
				{
					actconn->ResourceID   = -1;
					actconn->ConnectionID = -1;
				}
			}
			else if ( strcmp(actitem->ActionName, RESOURCE_ACTION_PLAY_CREATE) == 0 )
			{
				const char *filename = (const char *)
									   lfirst(list_head(actitem->Arguments));
				ret = createFile(filename);
			}
			else if ( strcmp(actitem->ActionName, RESOURCE_ACTION_PLAY_REMOVE) == 0 )
			{
				const char *filename = (const char *)
									   lfirst(list_head(actitem->Arguments));
				ret = removeFile(filename);
			}
			else if ( strcmp(actitem->ActionName, RESOURCE_ACTION_RPC_FAULT) == 0 )
			{
				/* Set the action into message handler error inject. */
				ListCell *cell = list_head(actitem->Arguments);
				const char *messageidstr = (const char *)lfirst(cell);
				cell = lnext(cell);
				const char *actionstr = (const char *)lfirst(cell);
				cell = lnext(cell);
				int countthread = atoi((const char *)lfirst(cell));

				setMessageErrorInject(messageidstr, actionstr, countthread);
			}
			else if ( strcmp(actitem->ActionName, RESOURCE_ACTION_RPC_FAULT_RM) == 0 )
			{
			}

			actitem->ResultCode = ret;
			actitem->ResultCode = actitem->ResultCode == FUNC_RETURN_OK ?
								  errorcode :
								  actitem->ResultCode;
			if ( actitem->ResultCode != FUNC_RETURN_OK )
			{
				actitem->ResultMessage = rm_palloc(QD2RM_CommContext,
												   strlen(errorbuf)+1);
				strcpy(actitem->ResultMessage, errorbuf);
			}

			break;
		}

		/* Check if all actions are done. */
		alldone = true;
		foreach(conncell, actions)
		{
			TestActionConn actconn = (TestActionConn)lfirst(conncell);
			if ( actconn->CurAction != NULL )
			{
				alldone = false;
				break;
			}

		}

		pg_usleep(100000);
	}
	return FUNC_RETURN_OK;
}

void outputAllcatedResourceToFile(const char *filename, int resourceid)
{
	QDResourceContext rescontext = NULL;
	FILE *fp = fopen(filename, "w");
	if ( fp == NULL )
	{
		return;
	}

	getAllocatedResourceContext(resourceid, &rescontext);
	if (rescontext != NULL)
	{
		/* Output memory quota of one virtual segment. */
		fprintf(fp, "%d\n", rescontext->QD_SegMemoryMB);
		/* Output core quota of one virtual segment. */
		fprintf(fp, "%lf\n", rescontext->QD_SegCore);
		/* Output virtual segment list. */

		int i;
		for (i = 0 ; i < rescontext->QD_SegCount ; ++i)
		{
			QDSegInfo qdseginfo = rescontext->QD_ResourceList[i];

			fprintf(fp, "%d,%s:%d,",
						i,
						GET_SEGINFO_HOSTNAME(qdseginfo->QD_SegInfo),
						qdseginfo->QD_SegInfo->port);
			/*
			 * Select the first ip address here as that is reported by HAWQ FTS
			 * component.
			 */
			AddressString paddr1 = NULL;
			getSegInfoHostAddrStr(qdseginfo->QD_SegInfo, 0, &paddr1);
			Assert(paddr1 != NULL);
			fprintf(fp, "%s,", paddr1->Address);
			fprintf(fp, "%s\n", qdseginfo->QD_HdfsHostName == NULL ?
					  	  	  	"NULL" :
					  	  	    qdseginfo->QD_HdfsHostName);
		}
	}

	fclose(fp);
}

int callSyncRPCToRM(const char 	 	   *sendbuff,
					int   		 		sendbuffsize,
		  	  	    uint16_t			sendmsgid,
					uint16_t 		  	exprecvmsgid,
					SelfMaintainBuffer	recvsmb)
{
#ifdef ENABLE_DOMAINSERVER
		return callSyncRPCDomain(QD2RM_SocketFile,
								 sendbuff,
								 sendbuffsize,
								 sendmsgid,
								 exprecvmsgid,
								 recvsmb);
#else
		return callSyncRPCRemote(master_addr_host,
								 rm_master_port,
								 sendbuff,
								 sendbuffsize,
								 sendmsgid,
								 exprecvmsgid,
								 recvsmb);
#endif
}
