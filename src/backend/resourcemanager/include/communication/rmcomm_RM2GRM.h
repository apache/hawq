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

#ifndef HAWQ_RESOURCE_MANAGER_COMMUNICATION_TO_GLOBAL_RESOURCE_MANAGER_H
#define HAWQ_RESOURCE_MANAGER_COMMUNICATION_TO_GLOBAL_RESOURCE_MANAGER_H

#include "envswitch.h"

#include "dynrm.h"

/******************************************************************************
 * This is part of the high-level interface of resource broker in HAWQ RM. This
 * file contains all interfaces for connecting to one global resource manager
 * (YARN, MESOS, etc) to acquire/return resource. The implementation includes 2
 * tiers, the global interface, and YARN specific implementation.
 *
 * Firstly, libyarn for YARN is implemented. Potential implementations:
 * native JNI client for YARN, client for Mesos.
 *
 *****************************************************************************/

typedef int ( * RM2GRM_FPTR_loadParameters) (void);
typedef int ( * RM2GRM_FPTR_connect) (void);
typedef int ( * RM2GRM_FPTR_disconnect) (void);
typedef int ( * RM2GRM_FPTR_register) (void);
typedef int ( * RM2GRM_FPTR_unregister) (void);

typedef int ( * RM2GRM_FPTR_getConnectReport)(DQueue);
typedef int ( * RM2GRM_FPTR_getClusterReport)(DQueue);
typedef int ( * RM2GRM_FPTR_getResQueueReport)(DQueue);
typedef int ( * RM2GRM_FPTR_acquireResource)(uint32_t,
											 uint32_t,
											 uint32_t,
											 DQueue);
typedef int ( * RM2GRM_FPTR_returnResource)(DQueue);
typedef int ( * RM2GRM_FPTR_cleanup)(void);


struct RM2GRM_FunctionEntriesData {
	RM2GRM_FPTR_loadParameters		loadParameters;
	RM2GRM_FPTR_connect				connect;
	RM2GRM_FPTR_disconnect			disconnect;
	RM2GRM_FPTR_register			registerHAWQ;
	RM2GRM_FPTR_unregister			unregisterHAWQ;
	RM2GRM_FPTR_getConnectReport	getConnectReport;
	RM2GRM_FPTR_getClusterReport	getClusterReport;
	RM2GRM_FPTR_getResQueueReport	getResQueueReport;
	RM2GRM_FPTR_acquireResource		acquireResource;
	RM2GRM_FPTR_returnResource		returnResource;
	RM2GRM_FPTR_cleanup				cleanup;
};

typedef struct RM2GRM_FunctionEntriesData  RM2GRM_FunctionEntriesData;
typedef struct RM2GRM_FunctionEntriesData *RM2GRM_FunctionEntries;

/* Prepare communication implementation. */
int RM2GRM_prepareImplementation(enum RB_IMP_TYPE imptype);

/* Load parameters from file system. */
int RM2GRM_loadParameters(void);

/* Connect and disconnect to the global resource manager. */
int RM2GRM_connect(void);
int RM2GRM_disconnect(void);

/* Register and unregister this application. */
int RM2GRM_register(void);
int RM2GRM_unregister(void);

/* Get information. */
int RM2GRM_getConnectReport(DQueue report);
int RM2GRM_getClusterReport(DQueue machines);
int RM2GRM_getResQueueReport(DQueue queues);

/* Acquire and return resource. */
int RM2GRM_acquireResource(uint32_t memorymb,
						   uint32_t core,
						   uint32_t contcount,
						   DQueue   containers);
int RM2GRM_returnResource(DQueue containers);

/* Clean all used memory and connections */
int RM2GRM_cleanup(void);

/* Get error message */
char *RM2GRM_getErrorMessage(void);
int   RM2GRM_getErrorCode(void);
void  RM2GRM_clearError(void);

extern int RM2GRM_LIBYARN_createEntries(RM2GRM_FunctionEntries entries);
extern int RM2GRM_NONE_createEntries(RM2GRM_FunctionEntries entries);

/*
 * Resource broker message.
 */
enum RESOURCE_BROKER_MSG_TYPES {
	REQUEST_MESSAGE_START = 0,
	GET_CLUSTER_REPORT,
	REQUEST_CONTAINERS,
	RETURN_CONTAINERS,

	RESPONSE_MESSAGE_START = 100,
	CLUSTER_REPORT,
	CONTAINERS_ALLOCATED,
	CONTAINERS_RETURNED
};

struct ResourceBrokerMessageData
{
	int32_t		Type;
	uint32_t	IssueTime;
	void	   *Data;
};

typedef struct ResourceBrokerMessageData *ResourceBrokerMessage;
typedef struct ResourceBrokerMessageData  ResourceBrokerMessageData;


extern int 	ErrorCode;
extern char	ErrorTrack[1024];
#endif /* HAWQ_RESOURCE_MANAGER_COMMUNICATION_TO_GLOBAL_RESOURCE_MANAGER_H */
