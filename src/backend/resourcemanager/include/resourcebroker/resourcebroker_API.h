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

#ifndef HAWQ_RESOURCE_MANAGER_RESOURCE_BROKER_API_H
#define HAWQ_RESOURCE_MANAGER_RESOURCE_BROKER_API_H

#include "envswitch.h"

#include "dynrm.h"

enum RESOURCE_BROKER_MESSAGE
{
	RM2RB_RUALIVE,
	RM2RB_GET_CLUSTERREPORT,
	RM2RB_ALLOC_RESOURCE,
	RM2RB_RETURN_RESOURCE,
	RM2RB_GET_CONTAINERREPORT,

	RB2RM_RUALIVE,
	RB2RM_CLUSTERREPORT,
	RB2RM_ALLOCATED_RESOURCE,
	RB2RM_RETURNED_RESOURCE,
	RB2RM_CONTAINERREPORT
};

typedef int  ( * RB_FPTR_start) (bool);
typedef int  ( * RB_FPTR_stop) (void);
typedef int  ( * RB_FPTR_getClusterReport)(const char *, List **, double *);
typedef int  ( * RB_FPTR_acquireResource)(uint32_t, uint32_t, List *preferred);
typedef int  ( * RB_FPTR_returnResource)(List **);
typedef int  ( * RB_FPTR_getContainerReport)(List **);
typedef int  ( * RB_FPTR_handleNotification) (void);
typedef void ( * RB_FPTR_freeClusterReport) (List **);
typedef void ( * RB_FPTR_handleSignalSIGCHLD) (void);
typedef void ( * RB_FPTR_handleError) (int);

struct RB_FunctionEntriesData {
	RB_FPTR_start				start;
	RB_FPTR_stop				stop;
	RB_FPTR_getClusterReport	getClusterReport;
	RB_FPTR_acquireResource		acquireResource;
	RB_FPTR_returnResource		returnResource;
	RB_FPTR_getContainerReport  getContainerReport;
	RB_FPTR_handleNotification	handleNotification;
	RB_FPTR_freeClusterReport	freeClusterReport;
	RB_FPTR_handleSignalSIGCHLD handleSigSIGCHLD;
	RB_FPTR_handleError			handleError;
};

typedef struct RB_FunctionEntriesData  RB_FunctionEntriesData;
typedef struct RB_FunctionEntriesData *RB_FunctionEntries;

/* Prepare communication implementation. */
void RB_prepareImplementation(enum RB_IMP_TYPE imptype);

/* Start resource broker service. */
int RB_start(bool isforked);
/* Stop resource broker service. */
int RB_stop(void);
/* Get information. */
int RB_getClusterReport(const char *queuename, List **machines, double *maxcapacity);
/* Acquire and return resource. */
int RB_acquireResource(uint32_t memorymb, uint32_t core, List *preferred);
int RB_returnResource(List **containers);

/* Get containers' report. */
int RB_getContainerReport(List **ctnstat);

int RB_handleNotifications(void);
void RB_freeContainerReport(List **containers);
void RB_handleSignalSIGCHLD(void);
void RB_handleError(int errorcode);

void setCleanGRMResourceStatus(void);
void unsetCleanGRMResourceStatus(void);
bool isCleanGRMResourceStatus(void);

void RB_clearResource(List **ctnl);

void RB_freePreferedHostsForGRMContainers(void);
void RB_updateSegmentsHavingNoExpectedGRMContainers(HASHTABLE segments);

/* Error message */
extern bool				ResourceManagerIsForked;
#endif /* HAWQ_RESOURCE_MANAGER_RESOURCE_BROKER_API_H */
