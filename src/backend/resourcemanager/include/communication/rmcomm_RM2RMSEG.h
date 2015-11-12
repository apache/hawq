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

#ifndef RESOURCE_MAMANGER_INTER_PROCESS_COMMUNICATION_RM2RMSEG_H
#define RESOURCE_MAMANGER_INTER_PROCESS_COMMUNICATION_RM2RMSEG_H
#include "envswitch.h"
#include "utils/linkedlist.h"
#include "utils/simplestring.h"
#include "dynrm.h"

extern MemoryContext RM2RMSEG_CommContext;
/******************************************************************************
 * This header file contains the APIs for resource manager master process.
 ******************************************************************************/

struct RM2RMSEGContextData
{
	int						RM2RMSEG_Conn_FD;

	/* Reusable buffer to build request messages */
	SelfMaintainBufferData	SendBuffer;

	/* Reusable buffer to build response messages */
	SelfMaintainBufferData	RecvBuffer;
};

typedef struct RM2RMSEGContextData  RM2RMSEGContextData;
typedef struct RM2RMSEGContextData *RM2RMSEGContext;

/* Initialize environment to HAWQ RM */
void initializeRM2RMSEGComm(void);

/* Clean up and reset resource information and do necessary return to HAWQ RM */
int cleanupRM2RMSEGComm(void);

int sendRUAlive(char *seghostname);

/* Update memory quota on specific machine */
int increaseMemoryQuota(char *seghostname, GRMContainerSet containerset);
int decreaseMemoryQuota(char *seghostname, GRMContainerSet containerset);

#endif /* RESOURCE_MAMANGER_INTER_PROCESS_COMMUNICATION_RM2RMSEG_H */
