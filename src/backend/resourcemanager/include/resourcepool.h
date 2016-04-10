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

#ifndef DYNAMIC_RESOURCE_MANAGEMENT_RESOURCE_POOL_H
#define DYNAMIC_RESOURCE_MANAGEMENT_RESOURCE_POOL_H

#include "envswitch.h"
#include "rmcommon.h"
#include "utils/balancedbst.h"
#include "utils/linkedlist.h"
#include "utils/simplestring.h"
#include "utils/network_utils.h"

/*
 * The resource pool maintains a set of segments(hosts) which contain allocated
 * resource from resource broker.
 *
 * For each segment, the data structure is designed as below. The instance is
 * maintained as 64-bit aligned for ever. Another segment resource info instance
 * references the corresponding segment instance as the resource tracker.
 *
 *         |<----------- 64 bits (8 bytes) ----------->|
 * 		   +-------------------------------------------+
 *         |        Content of SegStatData             |
 * 		   +-------------------------------------------+
 *         |        Content of SegInfoData      	   |
 *         +-------------------------------------------+
 *                   ^
 *                   |(ref 1:1)
 *                   |
 *         +------------------+
 *         |  SegResInfoData  |
 *         +------------------+
 *
 */

/*
 *------------------------------------------------------------------------------
 * SegInfo
 *------------------------------------------------------------------------------
 */
struct SegInfoData {
	uint32_t		Size;
	uint32_t		AddressAttributeOffset;				   /* 64-bit aligned. */
	uint32_t		AddressContentOffset;
	uint32_t		HostNameOffset;						   /* 64-bit aligned. */
	uint32_t		HostNameLen;
	uint32_t		port;								   /* 64-bit aligned. */
	uint32_t		GRMHostNameOffset;
	uint32_t		GRMHostNameLen;
	uint32_t		GRMRackNameOffset;
	uint32_t		GRMRackNameLen;
	uint32_t		HostAddrCount;
	uint32_t		FailedTmpDirOffset;
	uint32_t		FailedTmpDirLen;
	uint8_t			master;
	uint8_t			standby;
	uint8_t			alive;
	uint8_t 		Reserved1;							   /* 64-bit aligned. */
};

typedef struct SegInfoData *SegInfo;
typedef struct SegInfoData  SegInfoData;

/*
 * Extract host name string from SegInfo instance.
 */
#define GET_SEGINFO_HOSTNAME(seginfo) 										   \
		((char *)(seginfo) + ((seginfo)->HostNameOffset))

/*
 * Extract GRM rack name string from SegInfo instance.
 */
#define GET_SEGINFO_GRMRACKNAME(seginfo) 										   \
		((char *)(seginfo) + ((seginfo)->GRMRackNameOffset))

/*
 * Extract GRM host name string from SegInfo instance.
 */
#define GET_SEGINFO_GRMHOSTNAME(seginfo) 										   \
		((char *)(seginfo) + ((seginfo)->GRMHostNameOffset))

/*
 * Extract failed temporary string from SegInfo instance.
 */
#define GET_SEGINFO_FAILEDTMPDIR(seginfo) 										   \
		((char *)(seginfo) + ((seginfo)->FailedTmpDirOffset))

/*
 * Macros for getting segment address content from SegInfo instance.
 */
#define IS_SEGINFO_ADDR_STR(attr)					   	    				   \
		(((attr) & HOST_ADDRESS_CONTENT_STRING) == HOST_ADDRESS_CONTENT_STRING)

#define GET_SEGINFO_ADDR_ATTR_START(seginfo) 								   \
		(char *)((char *)(seginfo) + (seginfo)->AddressAttributeOffset)

#define GET_SEGINFO_ADDR_ATTR_AT(seginfo,index) 							   \
		*((uint16_t *)(GET_SEGINFO_ADDR_ATTR_START(seginfo) + 				   \
					   (index) * sizeof(uint32_t) +			  			   	   \
					   sizeof(uint16_t)))

#define GET_SEGINFO_ADDR_OFFSET_AT(seginfo,index) 				  			   \
		*((uint16_t *)(GET_SEGINFO_ADDR_ATTR_START(seginfo) + 			   	   \
					   (index) * sizeof(uint32_t)))

/* Get host address from SegInfo instance. */
int  getSegInfoHostAddrStr (SegInfo seginfo, int addrindex, AddressString *addr);

int  findSegInfoHostAddrStr(SegInfo seginfo, AddressString addr, int *addrindex);

/*
 * Convert the address in SegInfo instance into string version address regardless
 * the type of address. The result is saved in self maintained buffer.
 */
void  generateSegInfoAddrStr(SegInfo seginfo, int addrindex, SelfMaintainBuffer buff);

/* Generate SegInfo instance's report as a string saved in self maintained buffer. */
void  generateSegInfoReport(SegInfo seginfo, SelfMaintainBuffer buff);


/*
 *------------------------------------------------------------------------------
 * SegStat
 *------------------------------------------------------------------------------
 */

struct SegStatData {
	int32_t			ID;					/* Internal ID.						  */
	uint16_t		FailedTmpDirNum;	/* Failed temporary directory number */
	uint8_t			FTSAvailable;		/* If it is available now.			  */
	uint8_t			GRMHandled;			/* If its GRM status is handled */
	uint32_t		FTSTotalMemoryMB;		/* FTS reports memory capacity.   */
	uint32_t		FTSTotalCore;			/* FTS reports core capacity.	  */
	uint32_t		GRMTotalMemoryMB;		/* GRM reports memory capacity.	  */
	uint32_t		GRMTotalCore;			/* GRM reports core capacity. 	  */
	uint64_t		RMStartTimestamp;		/* RM process reset timestamp */
	uint32_t		StatusDesc;				/* Description of status */
	uint32_t		Reserved;
	SegInfoData		Info;					/* 64-bit aligned.				  */
};

typedef struct SegStatData *SegStat;
typedef struct SegStatData  SegStatData;

#define SEGSTAT_ID_INVALID -1

/* Values for SegStatData::HAWQAvailable and SegStatData::GLOBAvailable		  */
enum SegAvailabilityStatus {
	RESOURCE_SEG_STATUS_UNSET = 0,
	RESOURCE_SEG_STATUS_AVAILABLE,
	RESOURCE_SEG_STATUS_UNAVAILABLE
};

int setSegStatHAWQAvailability( SegStat machine, uint8_t newstatus);

#define IS_SEGSTAT_FTSAVAILABLE(seg) \
		((seg)->FTSAvailable == RESOURCE_SEG_STATUS_AVAILABLE)

/* Generate SegStat instance's report as a string saved in self maintained buffer. */
void  generateSegStatReport(SegStat segstat, SelfMaintainBuffer buff);

/*
 *------------------------------------------------------------------------------
 * The following node resource maintenance structure is designed as
 *
 * +------------------+
 * | SegResourceData  |
 * +------------------+
 *       |  memcoreratio1
 *       |    (array)        +-----------------+
 *       +-----------------> | GRMContainerSet |--> a list of GRMContainer
 *       |                   +-----------------+
 *       |  memcoreratio2
 *       |    (array)        +-----------------+
 *       +-----------------> | GRMContainerSet |--> a list of GRMContainer
 *                           +-----------------+
 *
 * The nodes are indexed by balanced BST indices to maintain SegResource
 * instances always in sorted status by available resource, allocated resource
 * and io bytes workload.
 *------------------------------------------------------------------------------
 */

struct SegResource;
typedef struct SegResourceData *SegResource;
typedef struct SegResourceData  SegResourceData;

/*
 *------------------------------------------------------------------------------
 * GRMContainer (global resource manager container, YARN for example. )
 * GRMContainerSet
 *------------------------------------------------------------------------------
 */

struct GRMContainerData {
	int64_t		 	 ID;
	int32_t		 	 MemoryMB;
	int32_t		 	 Core;
	int32_t			 Life;			/* Ref container life-cycle management.   */
	bool			 CalcDecPending;/* If this container is calculated into
									   decrease pending resource quota.       */
	char	   		*HostName;		/* Reference the host containing this one.*/
	SegResource		 Resource;		/* The corresponding segment resource.    */
};

typedef struct GRMContainerData  *GRMContainer;
typedef struct GRMContainerData   GRMContainerData;

struct GRMContainerSetData {
	ResourceBundleData  Allocated;
	ResourceBundleData  Available;
	List  	   		   *Containers;
};

typedef struct GRMContainerSetData *GRMContainerSet;
typedef struct GRMContainerSetData  GRMContainerSetData;

GRMContainer createGRMContainer(int64_t      id,
                                int32_t      memory,
								double	 	 core,
								char      	*hostname,
								SegResource  segres);

void freeGRMContainer(GRMContainer ctn);

GRMContainerSet createGRMContainerSet(void);
void freeGRMContainerSet(GRMContainerSet ctns);

GRMContainer popGRMContainerSetContainerList(GRMContainerSet ctns);
GRMContainer getGRMContainerSetContainerFirst(GRMContainerSet ctns);
void appendGRMContainerSetContainer(GRMContainerSet ctns, GRMContainer ctn);
void moveGRMContainerSetContainerList(GRMContainerSet tctns, GRMContainerSet sctns);

/* Get GRMContainerSet of one SegResource instance. */
int getGRMContainerSet(SegResource segres, uint32_t ratio, GRMContainerSet *ctns);
/* Get GRMContainerSet set for specified ratio in one SegResource instance. */
int createAndGetGRMContainerSet(SegResource segres, uint32_t ratio, GRMContainerSet *ctns);

/*
 *------------------------------------------------------------------------------
 * SegResource
 *------------------------------------------------------------------------------
 */
struct SegResourceData {
	SegStat 		Stat;	 		 /* The machine physical information.	  */

	/* Machine level total resource statistics. */
	ResourceBundleData Allocated;
	ResourceBundleData Available;

	int64_t			IOBytesWorkload; /* Accumulated io bytes number.      	  */
	int				SliceWorkload;	 /* Accumulated slice number.             */
	int				NVSeg;			 /* Accumulated vseg number.			  */

	uint64_t        LastUpdateTime;  /* Update it when master receives IMAlive
										message from segment,                 */

	bool			RUAlivePending;  /* A Flag indicates if this segment is
										waiting for RUAlive response          */

	/* Resource maintained by memory/core ratio. */
	GRMContainerSet ContainerSets[RESOURCE_QUEUE_RATIO_SIZE];

	/* Total GRM container size. */
	int				GRMContainerCount;
	int				GRMContainerFailAllocCount;

	/*
	 * When resource manager has resource allocated from resource broker, the
	 * resource increasing quota
	 */
	ResourceBundleData IncPending;
	ResourceBundleData DecPending;

	/*
	 * When resource manager decides to reset resource, the resource level is
	 * marked.
	 */
	ResourceBundleData OldInuse;
};


#define GET_SEGRESOURCE_HOSTNAME(segres) 									   \
		GET_SEGINFO_HOSTNAME(&(segres->Stat->Info))

#define IS_SEGRESOURCE_USABLE(segresource)   								   \
		((!(segresource)->RUAlivePending) && 								   \
		 IS_SEGSTAT_FTSAVAILABLE((segresource)->Stat))

/* Create new SegResource instance that refers SegStat instance. */
SegResource createSegResource(SegStat segstat);

int setSegResHAWQAvailability( SegResource segres, uint8_t newstatus);

/* Set the segment is under or not under RUAlive pending status. */
bool setSegResRUAlivePending( SegResource segres, bool pending);

uint32_t getSegResourceCapacityMemory(SegResource segres);
uint32_t getSegResourceCapacityCore(SegResource segres);

int getSegmentGRMContainerSize(SegResource segres);

enum ResourcePoolQuotaControlFlags
{
	QUOTA_PHASE_TOACC_TO_ACCED = 0,
	QUOTA_PHASE_ACCED_TO_RESPOOL,
	QUOTA_PHASE_TOKICK_TO_KICKED,
	QUOTA_PHASE_KICKED_TO_RETURN,
	QUOTA_PHASE_COUNT
};

/*
 *------------------------------------------------------------------------------
 * RESOURCE POOL
 *
 * Global resource manager instance holds one instance of segment resource pool.
 * All allocated containers from global resource managers are organized here.
 * And all cluster information is maintained here as well, which means segment
 * expansion and shrinking occurs here according to the input from FTS.
 *------------------------------------------------------------------------------
 */

/*
 * function pointer array for allocating resource in resource pool.
 */
typedef int (* ALLOC_RES_FROM_RESPOOL_FUNC) (int32_t 	nodecount,
											 int32_t	minnodecount,
											 uint32_t 	memory,
											 double 	core,
											 int64_t	iobytes,
											 int32_t    slicesize,
											 int32_t	vseglimitpseg,
											 int 		preferredcount,
											 char 	  **preferredhostname,
											 int64_t   *preferredscansize,
											 bool		fixnodecount,
											 List 	  **vsegcounters,
											 int32_t   *totalvsegcount,
											 int64_t   *vsegiobytes);

struct ResourcePoolData {

	/*
	 * When HAWQ RM starts, HAWQ segments' heart-beats are received, and new
	 * segments are registered in resource pool.
	 *
	 * For each segment, there are two statuses,  HAWQ availability and GLOB
	 * availability. HAWQ Availability means it has HAWQ process running and can
	 * be invoked for query processing; GLOB Availability means it is available
	 * from a global resource manager's view (YARN for example).
	 */

	uint32_t		SegmentIDCounter;			/* Internal segment id.       */
	HASHTABLEData	Segments;					/* (uint32_t,ResourceInfo)    */
	HASHTABLEData	SegmentHostNameIndexed;		/* (string,uint32_t) 	      */
	HASHTABLEData	SegmentHostAddrIndexed; 	/* (string,uint32_t) 	      */
	uint32_t		AvailNodeCount;				/* Total count of available nodes. */

	/*
	 * Hold all used hostname strings by GRM containers. This is to save memory
	 * for duplicate hostname strings and simplify memory management when operating
	 * GRM containers.
	 */
	HASHTABLEData	BufferedHostNames;

	/*
	 * The cluster resource capacity counters that are used to efficiently get
	 * resource queue capacity. FTS capacity is defined in hawq-site.xml, GRM
	 * capacity is from global resource manager's node/cluster report.
	 */
	ResourceBundleData FTSTotal;
	ResourceBundleData GRMTotal;
	ResourceBundleData GRMTotalHavingNoHAWQNode;

    uint64_t LastUpdateTime; /* Last time the GRM cluster report is gotten.   */
    uint64_t LastRequestTime;/* Last time the GRM cluster report is sent.     */
    uint64_t LastCheckTime;	 /* Last time the segments' status are checked.   */
    uint64_t LastResAcqTime; /* Last time HAWQ RM acquiring resource .		  */

    uint64_t LastCheckContainerTime;   /* Last time the GRM container report is gotten. */
    uint64_t LastRequestContainerTime; /* Last time the GRM container report is sent.   */

	/*
	 * GRM containers are maintained by memory/core ratio.
	 *
	 * For each memory/core ratio, there is an index implemented as one balanced
	 * binary search tree to help dynamically maintain one fast index to count
	 * nodes having resources  more than expectation. */
    BBST 			OrderedSegResAvailByRatio[RESOURCE_QUEUE_RATIO_SIZE];

	/*
	 * This index is for getting segments having most resource, when resource pool
	 * should timeout some resource back to the global resource manager, this is
	 * used, the purpose is to ensure HAWQ RM has even resource allocated among
	 * segments.
	 */
    BBST 			OrderedSegResAllocByRatio[RESOURCE_QUEUE_RATIO_SIZE];

	/*
	 * The index to help finding the nodes having fewest io bytes number accumulated.
	 */
	BBSTData		OrderedCombinedWorkload;

	/*
	 * This is for caching all resolved hdfs hostnames which are mapped to one
	 * registered segment in resource pool containing HAWQ FTS fixed hostname and
	 * ip addresses.
	 */
	HASHTABLEData	HDFSHostNameIndexed;

	/*
	 * This is for caching all resolved global resource manager hostnames which
	 * are mapped to one registered segment in resource pool containing HAWQ FTS
	 * fixed host name and ip addresses.
	 */
	HASHTABLEData	GRMHostNameIndexed;

	/* The fixed cluster level memory to core ratio. */
	uint32_t		ClusterMemoryCoreRatio;

	/*
	 * GRM Container life-cycle management.
	 *
	 *                     Resource broker allocate containers.
	 *                            |
	 *                            v
	 *                     +------------------------+
	 * Failed notify +---->|   ToAcceptContainers   |
	 * but retry life|     +------------------------+
	 * is not long   |       |    | Notified corresponding
	 * 				 |       |    | segments new resource
	 *               +-------+    | is added without error.
	 * Failed notify |            v
	 * retry life is |     +------------------------+
	 * too long      |     |   AcceptedContainers   |
	 *               |     +------------------------+
	 *               |            | Add to segments maintained
	 *               |            | by resource pool manager.
	 *               |            | The resource is ready.
	 *               |            v
	 *               |     +------------------------+
	 *               |     | Resource pool segments |
	 *               | +---+------------------------+
	 *               | | Containers | Timeout containers
	 *               | | from down  | to be kicked back
	 *               | | segments   | to resource broker
	 *               | |            |
	 *               | |            |
	 *               | |            |              +---------+ Failed notifying
	 *               | |            v              |         | but retry life is
	 *               | |   +------------------------+        | not long
	 *               | |   |    ToKickContainers    |<-------+----+
	 *               | |   +------------------------+             |
	 *               | |          | Notified corresponding        |
	 *               | \          | segments existing resource    |
	 *               |  \         | is to be recycled.            |
	 *               |   v        v        |                      |
	 *               |     +------------------------+             |
	 *               +---->|     KickedContainers   |<------------+
	 *                     +------------------------+ Fail notifying but retry
	 *                            |                   life is too long. Kick it.
	 *                            |
	 *                            v
	 *                      Resource broker returns all the containers now.
	 *
	 */
	HASHTABLEData	ToAcceptContainers;
	HASHTABLEData	ToKickContainers;
	List		   *AcceptedContainers;
	List		   *KickedContainers;
	int				AddPendingContainerCount;
	int				RetPendingContainerCount;

	/*
	 * The flags for testing the life cycle of GRM containers by pausing specified
	 * phases.
	 */
	bool			pausePhase[QUOTA_PHASE_COUNT];

	/*
	 * The function array for extending multiple policies of allocating virtual
	 * segments from the segment pool.
	 */
	ALLOC_RES_FROM_RESPOOL_FUNC allocateResFuncs[RESOURCEPOOL_MAX_ALLOC_POLICY_SIZE];

	/* Slaves file content. */
	int64_t			SlavesFileTimestamp;
	int				SlavesHostCount;

	int				RBClusterReportCounter;
};

typedef struct ResourcePoolData *ResourcePool;
typedef struct ResourcePoolData  ResourcePoolData;

/*
 *------------------------------------------------------------------------------
 * Resource pool operating APIs
 *------------------------------------------------------------------------------
 */

/* Initialize Resource Pool Manager. */
void initializeResourcePoolManager(void);

/*
 * Add new host into the cluster. New segment will be registered, existing
 * segment maybe updated based on latest information passed in.
 */
int addHAWQSegWithSegStat(SegStat segstat, bool *capstatchanged);

/* Update existing host's grm capacity information. */
int updateHAWQSegWithGRMSegStat( SegStat segstat);

/* Find buffered resource host name string. */
void getBufferedHostName(char *hostname, char **buffhostname);

/* Find HAWQ node based on host address or host name. */
int getSegIDByHostName(const char *hostname, int hostnamelen, int32_t *id);
int getSegIDByHostAddr(uint8_t *hostaddr, int32_t hostaddrlen, int32_t *id);
int getSegIDByHDFSHostName(const char *hostname, int hostnamelen, int32_t *id);
int getSegIDByGRMHostName(const char *hostname, int hostnamelen, int32_t *id);
SegResource getSegResource(int32_t id);

/* Add resource container into the resource pool. */
int  addGRMContainerToToBeAccepted(GRMContainer ctn);
void GRMContainerToAccepted(GRMContainer ctn);
void addGRMContainerToResPool(GRMContainer ctn);

void addGRMContainerToToBeKicked(GRMContainer ctn);
void addGRMContainerToKicked(GRMContainer ctn);

void moveAllAcceptedGRMContainersToResPool(void);

void moveGRMContainerSetToAccepted(GRMContainerSet ctns);
void moveGRMContainerSetToKicked(GRMContainerSet ctns);

void dropAllResPoolGRMContainersToToBeKicked(void);
void dropAllToAcceptGRMContainersToKicked(void);

struct VSegmentCounterInternalData {
	uint32_t	HDFSNameIndex;		/* The HDFS host name index. 			  */
	uint32_t	VSegmentCount;		/* Count of segments totally allocated.   */
	uint32_t	SegId;				/* The id for tracking seg resource. 	  */
 	SegResource Resource;			/* Reference the selected segment.   	  */
};

typedef struct VSegmentCounterInternalData  VSegmentCounterInternalData;
typedef struct VSegmentCounterInternalData *VSegmentCounterInternal;

int allocateResourceFromResourcePoolIOBytes2(int32_t 	 nodecount,
										     int32_t	 minnodecount,
										     uint32_t 	 memory,
										     double 	 core,
										     int64_t	 iobytes,
										     int32_t   	 slicesize,
											 int32_t	 vseglimitpseg,
										     int 		 preferredcount,
										     char 	   **preferredhostname,
										     int64_t    *preferredscansize,
										     bool		 fixnodecount,
										     List 	   **vsegcounters,
										     int32_t    *totalvsegcount,
										     int64_t    *vsegiobytes);

int allocateResourceFromResourcePool(int32_t 	nodecount,
									 int32_t	minnodecount,
		 	 	 	 	 	 	 	 uint32_t 	memory,
									 double 	core,
									 int64_t	iobytes,
									 int32_t    slicesize,
									 int32_t	vseglimitpseg,
									 int 		preferredcount,
									 char 	  **preferredhostname,
									 int64_t   *preferredscansize,
									 bool		fixnodecount,
									 List 	  **vsegcounters,
									 int32_t   *totalvsegcount,
									 int64_t   *vsegiobytes);

/* Return query resource to resource pool. */
int returnResourceToResourcePool(int 		memory,
								 double 	core,
								 int64_t 	vsegiobytes,
								 int32_t 	slicesize,
								 List 	  **hosts,
								 bool 		isold);

void returnAllGRMResourceFromSegment(SegResource segres);
void dropAllGRMContainersFromSegment(SegResource segres);

void returnAllGRMResourceFromUnavailableSegments(void);

/* Notify RM SEG to update resource capacity. */
int notifyToBeAcceptedGRMContainersToRMSEG(void);
int notifyToBeKickedGRMContainersToRMSEG(void);

/* Time idle resource from nodes. */
void timeoutIdleGRMResourceToRB(void);
/* Return expected number of containers to GRM */
void forceReturnGRMResourceToRB(void);

/* Check if some hosts are not updated from GRM cluster report. */
bool hasSegmentGRMCapacityNotUpdated(void);
bool allSegmentHasNoGRMContainersAllocated(void);

int addOrderedResourceAllocTreeIndexByRatio(uint32_t ratio, BBST *tree);
int addOrderedResourceAvailTreeIndexByRatio(uint32_t ratio, BBST *tree);
int getOrderedResourceAvailTreeIndexByRatio(uint32_t ratio, BBST *tree);
int getOrderedResourceAllocTreeIndexByRatio(uint32_t ratio, BBST *tree);

void setAllSegResourceGRMUnhandled(void);

void resetAllSegmentsGRMContainerFailAllocCount(void);
void resetAllSegmentsNVSeg(void);

struct RB_GRMContainerStatData
{
	int64_t		ContainerID;
	uint8_t		isActive;
	uint8_t		isFound;
	uint8_t		Reserved[6];
};

typedef struct RB_GRMContainerStatData  RB_GRMContainerStatData;
typedef struct RB_GRMContainerStatData *RB_GRMContainerStat;

void checkGRMContainerStatus(RB_GRMContainerStat ctnstats, int size);

int getClusterGRMContainerSize(void);

void refreshAvailableNodeCount(void);

void checkSlavesFile(void);

void fixClusterMemoryCoreRatio(void);
void adjustSegmentCapacity(SegResource segres);
void adjustSegmentStatFTSCapacity(SegStat segstat);
void adjustSegmentStatGRMCapacity(SegStat segstat);
void adjustSegmentCapacityForNone(SegResource segres);
void adjustSegmentCapacityForGRM(SegResource segres);
void adjustMemoryCoreValue(uint32_t *memorymb, uint32_t *core);

/*
 *------------------------------------------------------------------------------
 * gp_segment_configuration catalog operating APIs.
 *------------------------------------------------------------------------------
 */

/* Clean up gp_segment_configuration */
void cleanup_segment_config(void);

/* Clean up gp_configuration_history */
void cleanup_segment_config_history(void);

#define SEG_STATUS_DESCRIPTION_UP "segment is up"
/* update a segment's status in gp_segment_configuration table */
void update_segment_status(int32_t id, char status, char* description);

/* Add a new entry into gp_segment_configuration table*/
void add_segment_config_row(int32_t 	 id,
							char		*hostname,
							char		*address,
							uint32_t 	 port,
							char 		 role,
							char		 status,
							char*		 description);

/*
 * SegStatData's StatusDesc is a combination of below flags
 */
#define	SEG_STATUS_HEARTBEAT_TIMEOUT			0x00000001
#define	SEG_STATUS_FAILED_PROBING_SEGMENT		0x00000002
#define	SEG_STATUS_COMMUNICATION_ERROR			0x00000004
#define	SEG_STATUS_FAILED_TMPDIR				0x00000008
#define	SEG_STATUS_RM_RESET						0x00000010
#define	SEG_STATUS_NO_GRM_NODE_REPORT			0x00000020

/* Add a new entry into gp_configuration_history table */
void add_segment_history_row(int32_t id,
							 char* hostname,
							 char* description);

/* build a string of status description based on SegStat */
SimpStringPtr build_segment_status_description(SegStat segstat);

/*
 * In resource pool, segment's id starts from 0, however in gp_segment_configuration table,
 * segment registration order starts from 1(0 is for master, -1 is for standby).
 */
#define REGISTRATION_ORDER_OFFSET 1

#define SEGMENT_STATUS_UP   	'u'
#define SEGMENT_STATUS_DOWN 	'd'

#define VALIDATE_RATIO_BIAS 	0.005
#define VALIDATE_RESOURCE_BIAS	0.0001

void validateResourcePoolStatus(bool refquemgr);

/*
 *------------------------------------------------------------------------------
 * Debug supporting functions
 *------------------------------------------------------------------------------
 */
void dumpResourcePoolHosts(const char *filename);
#endif /* DYNAMIC_RESOURCE_MANAGEMENT_RESOURCE_POOL_H */
