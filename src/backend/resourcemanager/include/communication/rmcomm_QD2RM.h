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

#ifndef RESOURCE_MAMANGER_INTER_PROCESS_COMMUNICATION_QD2RM_H
#define RESOURCE_MAMANGER_INTER_PROCESS_COMMUNICATION_QD2RM_H

#include "resourcemanager/communication/rmcomm_QD_RM_Protocol.h"
#include "resourcemanager/envswitch.h"
#include "resourcemanager/utils/linkedlist.h"
#include "resourcemanager/utils/simplestring.h"
#include "resourcemanager/dynrm.h"

/******************************************************************************
 * This header file contains the APIs for Backend process. It calls these APIs
 * to negotiate with HAWQ RM process.
 *
 * For one QD process, there are multiple portals which contain independent
 * QD resource set data (QDResourceSetData).
 ******************************************************************************/
struct QDSegInfoData {
	SegInfo			QD_SegInfo;
	char		   *QD_HdfsHostName;
};

typedef struct QDSegInfoData  QDSegInfoData;
typedef struct QDSegInfoData *QDSegInfo;

struct QDResourceContextData {
	int32_t			QD_Conn_ID;	  		/* Save the connection ID for QD.	  */
	char		   *QD_Resource;		/* Save the allocated resource nodes. */
	QDSegInfo      *QD_ResourceList;	/* List for accessing each node.      */
	char		  **QD_HdfsHostNames;	/* The original HDFS hostname strings.*/
	uint32_t		QD_SegMemoryMB;
	double			QD_SegCore;
	uint32_t		QD_SegCount;
	uint32_t		QD_HostCount;

	/* Reusable buffer for building request messages & receiving response
	 * messages.*/
	SelfMaintainBufferData 	SendBuffer;
	SelfMaintainBufferData  RecvBuffer;
};

typedef struct QDResourceContextData  QDResourceContextData;
typedef struct QDResourceContextData *QDResourceContext;

/* Initialize environment to HAWQ RM */
void initializeQD2RMComm(void);
/* Clean up all reset resource information and do necessary return to HAWQ RM.*/
int cleanupQD2RMComm(void);
/* Create local new resource context to maintain resource. */
int createNewResourceContext(int *index);

/* Release local resource context. */
int  releaseResourceContext(int index);
void releaseResourceContextWithErrorReport(int index);

/* Get the resource context instance. */
int getAllocatedResourceContext(int index, QDResourceContext *rescontext);
/* Register one connection by user name string . */
int registerConnectionInRMByStr(int 		   index,
								const char 	  *userid,
								char		  *errorbuf,
								int		 	   errorbufsize);
/* Register one connection by user oid. */
int registerConnectionInRMByOID(int 		   index,
								uint64_t 	   useridoid,
								char		  *errorbuf,
								int		 	   errorbufsize);

/* Acquire resource for executing current statement.*/
struct HostDataVolumeInfo{
	int64_t		scansize;
	char	   *hostname;
};

typedef struct HostDataVolumeInfo  HostDataVolumeInfo;
typedef struct HostDataVolumeInfo *HostDataVolumeInfoPtr;

int acquireResourceFromRM(int 		  		  index,
						  int			  	  sessionid,
						  int			  	  slice_size,
						  int64_t			  iobytes,
						  HostnameVolumeInfo *preferred_nodes,
						  int				  preferred_nodes_size,
						  uint32_t    		  max_seg_count_fix,
						  uint32_t			  min_seg_count_fix,
						  char	     		 *errorbuf,
						  int	      		  errorbufsize);

int acquireResourceQuotaFromRM(int64_t		user_oid,
							   uint32_t		max_seg_count_fix,
							   uint32_t		min_seg_count_fix,
							   char	       *errorbuf,
							   int			errorbufsize,
							   uint32_t	   *seg_num,
							   uint32_t	   *seg_num_min,
							   uint32_t	   *seg_memory_mb,
							   double	   *seg_core);

bool alreadyReturnedResource(int index);
/* Return resource for executing current statement. */
int returnResource(int index, char *errorbuf, int errorbufsize);

/* Release the connection identified by connection id. */
int	unregisterConnectionInRM(int 			   index,
							 char		  	  *errorbuf,
							 int		 	   errorbufsize);

void unregisterConnectionInRMWithErrorReport(int index);
/******************************************************************************
 * Check if already has gotten the resource.
 *
 * index[in]			The resource context index.
 * allocated[out]		The result.
 *
 * Return			FUNC_RETURN_OK:
 * 						The resource set instance is found.
 * 					COMM2RM_CLIENT_WRONG_INPUT:
 * 						Illegal index value.
 ******************************************************************************/
int hasAllocatedResource(int index, bool *allocated);

/* Set statement level resource quota. */
//int setStatementResourceQuota( uint32_t mem_MB, float core, uint32_t node_size);

/* Reset statement level resource quota. */
//int resetStatementResourceQuota(void);

/* Send heart-beat to refresh the fixed resource. */
/**** NOT IMPLEMENTED YET ****/
//int touchResource(int index);

int manipulateResourceQueue(int 	 index,
							char 	*queuename,
							uint16_t action,
							List    *options,
							char	*errorbuf,
							int		 errorbufsize);

int manipulateRoleForResourceQueue (int 	  index,
									Oid 	  roleid,
									Oid 	  queueid,
									uint16_t  action,
									uint8_t   isSuperUser,
									char	 *rolename,
									char	 *errorbuf,
									int		  errorbufsize);

void SendResourceRefreshHeartBeat(void);

void sendFailedNodeToResourceManager(int hostNum, char **pghost);

//int getLocalTmpDirFromMasterRM(char *errorbuf, int errorbufsize);

int dumpResourceManagerStatus(uint32_t		 type,
							  const char	*dump_file,
							  char			*errorbuf,
							  int			 errorbufsize);

#endif /* RESOURCE_MAMANGER_INTER_PROCESS_COMMUNICATION_QD2RM_H */
