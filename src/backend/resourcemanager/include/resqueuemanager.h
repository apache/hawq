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

#ifndef DYNAMIC_RESOURCE_MANAGEMENT_RESOURCE_QUEUE_H
#define DYNAMIC_RESOURCE_MANAGEMENT_RESOURCE_QUEUE_H
#include "envswitch.h"
#include "rmcommon.h"
#include "utils/balancedbst.h"
#include "utils/linkedlist.h"
#include "utils/simplestring.h"

#include "conntrack.h"
#include "resqueuedeadlock.h"

/******************************************************************************
 *                           RESOURCE QUEUE MANAGER                           *
 *                                                                            *
 * This component manages all resource queues and roles.                      *
 ******************************************************************************/

/*----------------------------------------------------------------------------*/
/*                    RESOURCE QUEUE MANAGER INTERNAL APIs                    */
/*----------------------------------------------------------------------------*/

#define RESQUEUE_IS_ROOT(queue)		(((queue)->Status & 					   \
									 RESOURCE_QUEUE_STATUS_IS_ROOT ) != 0 )
#define RESQUEUE_IS_DEFAULT(queue)	(((queue)->Status & 					   \
									 RESOURCE_QUEUE_STATUS_IS_DEFAULT ) != 0 )
#define RESQUEUE_IS_LEAF(queue)		(((queue)->Status & 					   \
									 RESOURCE_QUEUE_STATUS_VALID_LEAF ) != 0 )
#define RESQUEUE_IS_BRANCH(queue)	(((queue)->Status & 					   \
									 RESOURCE_QUEUE_STATUS_VALID_BRANCH ) != 0 )
#define RESQUEUE_IS_PERCENT(queue)  (((queue)->Status &					   	   \
									 RESOURCE_QUEUE_STATUS_EXPRESS_PERCENT) != 0)
/*
 * The DDL statement attribute name index.
 */
enum RESOURCE_QUEUE_DDL_ATTR_INDEX
{
	RSQ_DDL_ATTR_PARENT = 0,
	RSQ_DDL_ATTR_ACTIVE_STATMENTS,
	RSQ_DDL_ATTR_MEMORY_LIMIT_CLUSTER,
	RSQ_DDL_ATTR_CORE_LIMIT_CLUSTER,
	RSQ_DDL_ATTR_VSEG_RESOURCE_QUOTA,
	RSQ_DDL_ATTR_ALLOCATION_POLICY,
	RSQ_DDL_ATTR_RESOURCE_OVERCOMMIT_FACTOR,
	RSQ_DDL_ATTR_NVSEG_UPPER_LIMIT,
	RSQ_DDL_ATTR_NVSEG_LOWER_LIMIT,
	RSQ_DDL_ATTR_NVSEG_UPPER_LIMIT_PERSEG,
	RSQ_DDL_ATTR_NVSEG_LOWER_LIMIT_PERSEG,
	RSQ_DDL_ATTR_COUNT
};

extern char RSQDDLAttrNames[RSQ_DDL_ATTR_COUNT][RESOURCE_QUEUE_DDL_ATTR_LENGTH_MAX+1];

/*
 * The attributes for expressing one complete resource queue definition.
 */
enum RESOURCE_QUEUE_TABLE_ATTR_INDEX {

	/* The attributes user can specify. */
	RSQ_TBL_ATTR_PARENT=0,
	RSQ_TBL_ATTR_ACTIVE_STATMENTS,
	RSQ_TBL_ATTR_MEMORY_LIMIT_CLUSTER,
	RSQ_TBL_ATTR_CORE_LIMIT_CLUSTER,
	RSQ_TBL_ATTR_VSEG_RESOURCE_QUOTA,
	RSQ_TBL_ATTR_ALLOCATION_POLICY,
	RSQ_TBL_ATTR_RESOURCE_OVERCOMMIT_FACTOR,
	RSQ_TBL_ATTR_NVSEG_UPPER_LIMIT,
	RSQ_TBL_ATTR_NVSEG_LOWER_LIMIT,
	RSQ_TBL_ATTR_NVSEG_UPPER_LIMIT_PERSEG,
	RSQ_TBL_ATTR_NVSEG_LOWER_LIMIT_PERSEG,

	/* The attributes automatically generated. */
	RSQ_TBL_ATTR_OID,
	RSQ_TBL_ATTR_NAME,
	RSQ_TBL_ATTR_CREATION_TIME,
	RSQ_TBL_ATTR_UPDATE_TIME,
	RSQ_TBL_ATTR_STATUS,

	RSQ_TBL_ATTR_COUNT
};

extern char RSQTBLAttrNames[RSQ_TBL_ATTR_COUNT][RESOURCE_QUEUE_TBL_COLNAME_LENGTH_MAX+1];
/*
 * The attributes for expressing one complete role/user definition.
 */
enum USER_ATTR_INDEX {
	USR_TBL_ATTR_OID = 0,
	USR_TBL_ATTR_NAME,
	USR_TBL_ATTR_TARGET_QUEUE,
	USR_TBL_ATTR_PRIORITY,
	USR_TBL_ATTR_IS_SUPERUSER,

	USR_TBL_ATTR_COUNT
};

/*
 * RESOURCE QUEUE instance.
 *
 * In memory structure of one resource queue instance. (64-bit aligned.)
 */
struct DynResourceQueueData {
    int64_t        		OID;					/* Queue OID 				  */
    int64_t				ParentOID;				/* Queue parent OID 		  */
    int32_t            	NameLen;				/* Length of queue name       */
    int32_t				ParallelCount;			/* Parallel count	          */
    int32_t             ClusterMemoryMB;		/* Specified memory limit     */
    uint32_t			Status;

    double				ClusterVCore;			/* Specified core limit       */
    double				SegResourceQuotaVCore;	/* Segment resource quota     */
    int32_t				SegResourceQuotaMemoryMB;/* Segment resource quota    */
    int32_t				Reserved2;

    int32_t				NVSegUpperLimit;		/* vseg upper limit. 		  */
    int32_t				NVSegLowerLimit;		/* vseg upper limit. 		  */
    double				NVSegUpperLimitPerSeg;	/* vseg upper limit per seg.  */
    double				NVSegLowerLimitPerSeg;	/* vseg lower limit per seg.  */
    double				ResourceOvercommit;

    int8_t				AllocatePolicy;			/* Allocation policy          */
    int8_t				QueuingPolicy;			/* Temporary unused. 		  */
    int8_t				InterQueuePolicy; 		/* Temporary unused.		  */
    int8_t				Reserved3;

    double				ClusterMemoryPer;		/* Specified percentage exp.  */
    double				ClusterVCorePer;		/* Specified percentage exp.  */

    char                Name[64];				/* Expect maximum 64 bytes.   */
};

typedef struct DynResourceQueueData *DynResourceQueue;
typedef struct DynResourceQueueData  DynResourceQueueData;

/*
 * RESOURCE QUEUE TRACKER
 *
 * Track the resource allocation in each resource queue.
 */
struct DynResourceQueueTrackData;
typedef struct DynResourceQueueTrackData *DynResourceQueueTrack;
typedef struct DynResourceQueueTrackData  DynResourceQueueTrackData;

struct DynResourceQueueTrackData {
	DynResourceQueue	  QueueInfo;			/* Queue definition.		  */
	DynResourceQueueTrack ParentTrack;			/* Parent track information.  */
	List			  	 *ChildrenTracks;		/* Children 			   	  */

	uint32_t			  CurConnCounter;		/* Current number of conn.	  */
	DQueueData		  	  QueryResRequests;

	uint32_t              MemCoreRatio;			/* min_mem/min_core 	      */
	int32_t			  	  RatioIndex;			/* Memory/core ratio for leaf.*/
    int32_t				  ClusterSegNumber;		/* Standard segment number.   */
    int32_t				  ClusterSegNumberMax;  /* Maximum segment number.    */

    int32_t        		  ClusterMemoryMaxMB; 	/* Maximum memory limit.	  */
    double            	  ClusterVCoreMax;		/* Maximum core limit.  	  */

    double				  ClusterMemoryActPer;  /* Percentage expression.     */
    double				  ClusterMemoryMaxPer;	/* Percentage expression.     */
    double				  ClusterVCoreActPer;	/* Percentage expression.     */
    double				  ClusterVCoreMaxPer;	/* Percentage expression.     */

    ResourceBundleData	  TotalAllocated;		/* Allocated resource quota.  */
	ResourceBundleData    TotalRequest;			/* Request resource by queuing
	 	 	 	 	 	 	 	 	 	 	 	   queries.                   */
	ResourceBundleData	  TotalUsed;			/* Used resource in running
												   queries.                   */
	bool				  trackedMemCoreRatio;	/* If the queue's mem/core ratio
												   has been tracked by mem/core
												   ratio tracker.             */
	bool			  	  isBusy;				/* If the queue is busy.  	  */
	bool				  pauseAllocation;		/* Pause resource allocation. */
	bool				  troubledByFragment;	/* Can not allocate resource
												   due to resource fragment.  */
	bool				  expectMoreResource;	/* If need more resource.	  */
	int					  NumOfRunningQueries;  /* Number of running queries. */

	ResqueueDeadLockDetectorData DLDetector;	/* Deadlock detector.         */

	DynResourceQueueTrack	ShadowQueueTrack;	/* The shadow instance for
												   saving temporary status when
												   altering this queue.		  */
};

/**
 * MEMORY/CORE RATIO TRACKER
 *
 * Track the resource allocation in each memory/core ratio.
 */
struct DynMemoryCoreRatioTrackData {
	uint32_t              MemCoreRatio;			/* min_mem/min_core 	      */
	int32_t			  	  RatioIndex;			/* Memory/core ratio for leaf.*/

	int32_t				  ClusterMemory;		/* Explicit memory limit.	  */
	double				  ClusterVCore;			/* Explicit core limit.		  */
    int32_t        		  ClusterMemoryMaxMB; 	/* Maximum memory limit.	  */
    double            	  ClusterVCoreMax;		/* Maximum core limit.  	  */

    ResourceBundleData	  TotalPending;			/* Pending resource waiting for
    											   the response from resource
    											   broker. 					  */
    uint64_t			  TotalPendingStartTime;/* Start point when total pending
    											   is more than 0.	 		  */
    ResourceBundleData	  TotalAllocated;		/* Allocated resource quota.  */
	ResourceBundleData    TotalRequest;			/* Request resource by queuing
	 	 	 	 	 	 	 	 	 	 	 	   queries.                   */
	ResourceBundleData	  TotalUsed;			/* Used resource in running
												   queries.					  */
    double				  ClusterWeightMarker;	/* The marker of overall 100%
    											   resource usage.            */
    List			  	 *QueueTrackers;		/* All trackers having resource
    											   of this memory/core ratio. */
    int					  QueueIndexForLeftResource;
};

typedef struct DynMemoryCoreRatioTrackData  DynMemoryCoreRatioTrackData;
typedef struct DynMemoryCoreRatioTrackData *DynMemoryCoreRatioTrack;

struct DynMemoryCoreRatioWaterMarkData {
	int32_t				  ClusterMemoryMB;
	double                ClusterVCore;
	uint64_t			  LastRecordTime;
};

typedef struct DynMemoryCoreRatioWaterMarkData  DynMemoryCoreRatioWaterMarkData;
typedef struct DynMemoryCoreRatioWaterMarkData *DynMemoryCoreRatioWaterMark;

/**
 * USER/ROLE
 *
 * In memory user instance. 64-bit aligned.
 */
struct UserInfoData {
	int64_t			OID;				/* OID of this role/user. 			  */
	int64_t			QueueOID;			/* Queue OID 						  */
	int8_t			isSuperUser;		/* If it is a super user. 			  */
	int8_t	  		isInUse;			/* If currently, there are some queries
	 	 	 	 	 	 	 	 	 	   running using this role/user.	  */
	int8_t			Reserved[6];
	char			Name[72];			/* User name string                   */
};

typedef struct UserInfoData *UserInfo;
typedef struct UserInfoData  UserInfoData;

typedef enum RESOURCEPROBLEM
{
	RESPROBLEM_NO = 0,
	RESPROBLEM_FRAGMENT,
	RESPROBLEM_UNEVEN,
	RESPROBLEM_TOOFEWSEG,
	RESPROBLEM_COUNT
} RESOURCEPROBLEM;

/******************************************************************************
 * In resource queue manager,  a list of resource queues are saved. This can  *
 * be referenced from global instance DRMGlobalInstance->ResourceQueueManager.*
 *                                                                            *
 *  Queue information can be fetched from queue id or queue name.             *
 *  +----------------------+           +-----------------------+              *
 *  |  HASHTABLE(QueueID)  | -- ref--> |                       |              *
 *  +----------------------+           |      Queue Track      |---------+    *
 *                                     |         LIST          |         |    *
 *  +----------------------+           |                       |<----+   |    *
 *  | HASHTABLE(QueueName) | -- ref--> | (QueueInfo, ResTrack) |     |   |    *
 *  +----------------------+           +-----------------------+     |   |    *
 *                                                                   |   |    *
 *                                                                   |   |    *
 *                                                                  ref ref   *
 *  User info can be fetched by userid string,                       |   |    *
 *  and queue can be tracked directly.                               |   |    *
 *  +----------------------+           +-----------------------+     |   |    *
 *  |  HASHTABLE(UserID)   | -- ref--> |    LIST(UserInfo)     |-----+   |    *
 *  +----------------------+           +-----------------------+<--------+    *
 *                                       ^                                    *
 *  +----------------------+             |                                    *
 *  |  HASHTABLE(UserName) | -- ref------.                                    *
 *  +----------------------+                                                  *
 ******************************************************************************/
struct DynResourceQueueManagerData {
	/* Resource queue definition and overall counters.        				  */
	List			  	 *Queues;			/* All queues. 					  */
	HASHTABLEData		  QueuesIDIndex;	/* ID to queue object address.    */
											/* Hash of DQueueNode 			  */
	HASHTABLEData		  QueuesNameIndex; 	/* Name to queue object address.  */
											/* Hash of DQueueNode 			  */

	DynResourceQueueTrack RootTrack;		/* Reference root queue 'root'    */
	DynResourceQueueTrack DefaultTrack;		/* Reference default queue 'default' */

    /* For user id string, this is to search queue from user id. 			  */
    List			  	 *Users;			/* All users.                     */
    HASHTABLEData		  UsersIDIndex;		/* Hash table (User ID, UserInfo) */
    HASHTABLEData		  UsersNameIndex;	/* Hash table (User Name, UserInfo)*/

    /* Memory/Core ratio management. */
    int32_t				     RatioCount;
    HASHTABLEData		     RatioIndex;
    uint32_t			     RatioReverseIndex    [RESOURCE_QUEUE_RATIO_SIZE];
    int32_t				     RatioReferenceCounter[RESOURCE_QUEUE_RATIO_SIZE];
    DynMemoryCoreRatioTrack  RatioTrackers        [RESOURCE_QUEUE_RATIO_SIZE];
    DQueueData				 RatioWaterMarks      [RESOURCE_QUEUE_RATIO_SIZE];

    uint64_t				 LastCheckingDeadAllocationTime;
    uint64_t				 LastCheckingQueuedTimeoutTime;

    double					 GRMQueueCapacity;
    double					 GRMQueueCurCapacity;
    double					 GRMQueueMaxCapacity;
    bool					 GRMQueueResourceTight;
    int						 ForcedReturnGRMContainerCount;
    bool					 toRunQueryDispatch;
    bool	 				 hasResourceProblem[RESPROBLEM_COUNT];

    int						 ActualMinGRMContainerPerSeg;
};
typedef struct DynResourceQueueManagerData *DynResourceQueueManager;
typedef struct DynResourceQueueManagerData  DynResourceQueueManagerData;

/*----------------------------------------------------------------------------*/
/*                    RESOURCE QUEUE MANAGER EXTERNAL APIs                    */
/*----------------------------------------------------------------------------*/

/*
 * APIs for main process.
 */
/* Initialize resource queue manager. */
void initializeResourceQueueManager(void);
/* collect resource queues' resource usage status from bottom up. */
void refreshMemoryCoreRatioLevelUsage(uint64_t curmicrosec);
/* Refresh resource queue resource capacity and adjusts all queued requests. */
void refreshResourceQueueCapacity(bool queuechanged);
/* Refresh actual minimum GRM container water level. */
void refreshActualMinGRMContainerPerSeg(void);
/* Dispatch resource to the queuing queries. */
void dispatchResourceToQueries(void);
/* Time out the resource allocated whose QD owner does not have chance to return. */
void timeoutDeadResourceAllocation(void);
void timeoutQueuedRequest(void);
void refreshMemoryCoreRatioLimits(void);
void refreshMemoryCoreRatioWaterMark(void);
/*
 * APIs for resource queue loading, creating, etc.
 */

/* Recognize DDL attributes and shallow parse to fine grained attributes. */
int shallowparseResourceQueueWithAttributes(List 	*rawattr,
											List   **fineattr,
											char  	*errorbuf,
											int		 errorbufsize);

int parseResourceQueueAttributes( List 			 	*attributes,
								  DynResourceQueue 	 queue,
								  bool				 checkformatonly,
								  bool				 loadcatalog,
								  char 				*errorbuf,
								  int   			 errorbufsize);

int updateResourceQueueAttributesInShadow(List 			 		*attributes,
								  	  	  DynResourceQueueTrack	 queue,
										  char					*errorbuf,
										  int					 errorbufsize);

void shallowFreeResourceQueueTrack(DynResourceQueueTrack track);
void deepFreeResourceQueueTrack(DynResourceQueueTrack track);

int checkAndCompleteNewResourceQueueAttributes(DynResourceQueue  queue,
											   char				*errorbuf,
											   int				 errorbufsize);

/* Create queue definition and tracker in the resource queue manager. */
int createQueueAndTrack( DynResourceQueue		queue,
						 DynResourceQueueTrack *track,
						 char				   *errorbuf,
						 int					errorbufsize);

/* Drop a queue and its track. */
int dropQueueAndTrack( DynResourceQueueTrack track,
					   char				    *errorbuf,
					   int					 errorbufsize);

void setQueueTrackIndexedByQueueOID(DynResourceQueueTrack queuetrack);

void removeQueueTrackIndexedByQueueOID(DynResourceQueueTrack queuetrack);

void setQueueTrackIndexedByQueueName(DynResourceQueueTrack queuetrack);

void removeQueueTrackIndexedByQueueName(DynResourceQueueTrack queuetrack);

DynResourceQueueTrack getQueueTrackByQueueOID (int64_t queoid);

DynResourceQueueTrack getQueueTrackByQueueName(char 	*quename,
											   int 		 quenamelen);

bool hasUserAssignedToQueue(DynResourceQueue queue);

/** APIs for user loading, creating, etc. **/
int parseUserAttributes( List 	 	*attributes,
						 UserInfo 	 user,
						 char		*errorbuf,
						 int		 errorbufsize);

int checkUserAttributes(UserInfo user, char *errorbuf, int errorbufsize);

void createUser(UserInfo userinfo);

void setUserIndexedByUserOID(UserInfo userinfo);
void setUserIndexedByUserName(UserInfo userinfo);

UserInfo getUserByUserName( const char *userid, int useridlen, bool *exist);
UserInfo getUserByUserOID ( int64_t useroid, bool *exist);

int addNewResourceToResourceManager(int32_t memorymb, double core);
int minusResourceFromReourceManager(int32_t memorymb, double core);
int addNewResourceToResourceManagerByBundle(ResourceBundle bundle);
int minusResourceFromResourceManagerByBundle(ResourceBundle bundle);

void removePendingResourceRequestInRootQueue(int32_t 	memorymb,
											 uint32_t 	core,
											 bool 		updatependingtime);
void clearPendingResourceRequestInRootQueue(void);
void buildAcquireResourceResponseMessage(ConnectionTrack conn);


/* Drop one user */
int dropUser(int64_t userid, char* name);

/* NOTE: The buffer size limitation is not checked yet. */
void generateQueueReport(int queid, char *buff, int buffsize);
void generateUserReport(const char 		*userid,
						int 			 useridlen,
						char 			*buff,
						int 			 buffsize);


const char *getRSQTBLAttributeName(int attrindex);
const char *getRSQDDLAttributeName(int colindex);
const char *getUSRTBLAttributeName(int attrindex);
/*
 * APIs for request handlers.
 */
int registerConnectionByUserID(ConnectionTrack  conntrack,
							   char			   *errorbuf,
							   int				errorbufsize);
int acquireResourceFromResQueMgr(ConnectionTrack  conntrack,
								 char 			 *errorbuf,
								 int 			  errorbufsize);
int returnResourceToResQueMgr(ConnectionTrack conntrack);
void returnConnectionToQueue(ConnectionTrack conntrack, bool istimeout);
int acquireResourceQuotaFromResQueMgr(ConnectionTrack	conntrack,
									  char			   *errorbuf,
									  int				errorbufsize);
void cancelResourceAllocRequest(ConnectionTrack  conntrack,
								char 			*errorbuf,
								bool			 generror);
/*
 * APIs for operating resource detail instance.
 */
void resetResourceBundleData(ResourceBundle detail,
							 uint32_t 		mem,
							 double 		core,
							 uint32_t 		ratio);
void addResourceBundleData(ResourceBundle detail, int32_t mem, double core);
void minusResourceBundleData(ResourceBundle detail, int32_t mem, double core);

void resetResourceBundleDataByBundle(ResourceBundle detail, ResourceBundle source);
void addResourceBundleDataByBundle(ResourceBundle detail, ResourceBundle source);
void minusResourceBundleDataByBundle(ResourceBundle detail, ResourceBundle source);

/* Get ratio index by concrete ratio. */
int32_t getResourceQueueRatioIndex(uint32_t ratio);

/*
 * Get the minimum resource water level if resource manager has workload but not
 * high.
 */
void getIdleResourceRequest(int32_t *mem, double *core);

/* Check if all resource queues are idle. */
bool isAllResourceQueueIdle(void);

/* Reset deadlock detector of each queue. */
void resetAllDeadLockDetector(void);

/* Set forced number of GRM containers to return before dispatching. */
void setForcedReturnGRMContainerCount(void);

int computeQueryQuota(ConnectionTrack conn, char *errorbuf, int errorbufsize);

int adjustResourceExpectsByQueueNVSegLimits(ConnectionTrack	 conntrack,
											char			*errorbuf,
											int			 	 errorbufsize);

int addQueryResourceRequestToQueue(DynResourceQueueTrack queuetrack,
								   ConnectionTrack		 conntrack);

void buildQueueTrackShadows(DynResourceQueueTrack	toaltertrack,
							List 				  **qhavingshadow);
void buildQueueTrackShadow(DynResourceQueueTrack toaltertrack);

void cleanupQueueTrackShadows(List **qhavingshadow);

int rebuildAllResourceQueueTrackDynamicStatusInShadow(List *quehavingshadow,
													  bool  queuechanged,
													  char *errorbuf,
													  int	errorbufsize);

int rebuildResourceQueueTrackDynamicStatusInShadow(DynResourceQueueTrack  quetrack,
												   bool					  queuechanged,
												   char 				 *errorbuf,
												   int					  errorbufsize);

int detectAndDealWithDeadLockInShadow(DynResourceQueueTrack quetrack,
									  bool					queuechanged);

void applyResourceQueueTrackChangesFromShadows(List *quehavingshadow);

void cancelQueryRequestToBreakDeadLockInShadow(DynResourceQueueTrack shadowtrack,
											   DQueueNode			 iter,
											   int32_t				 expmemorymb,
											   int32_t				 availmemorymb);

/* Refresh resource queue resource capacity based on updated cluster info. */
void refreshResourceQueuePercentageCapacity(bool queuechanged);

/* Dump resource queue status to file system. */
void dumpResourceQueueStatus(const char *filename);
#endif /* DYNAMIC_RESOURCE_MANAGEMENT_RESOURCE_QUEUE_H */
