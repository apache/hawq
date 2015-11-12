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

#ifndef RESOURCE_MAMANGER_INTER_PROCESS_COMMUNICATION_RMSEG2RM_H
#define RESOURCE_MAMANGER_INTER_PROCESS_COMMUNICATION_RMSEG2RM_H
#include "envswitch.h"
#include "utils/linkedlist.h"
#include "utils/simplestring.h"
#include "dynrm.h"

extern MemoryContext RMSEG2RM_CommContext;
/******************************************************************************
 * This header file contains the APIs for resource manager segment process.
 ******************************************************************************/

struct RMSEG2RMContextData {
	int				RMSEG2RM_Conn_FD;

	/* Reusable buffer for building request messages & receiving response
	 * messages.*/
	SelfMaintainBufferData 	SendBuffer;
	SelfMaintainBufferData  RecvBuffer;
};

typedef struct RMSEG2RMContextData  RMSEG2RMContextData;
typedef struct RMSEG2RMContextData *RMSEG2RMContext;

/* Initialize environment to HAWQ RM */
void initializeRMSEG2RMComm(void);

/* Clean up all reset resource information and do necessary return to HAWQ RM.*/
int cleanupRMSEG2RMComm(void);

int sendIMAlive(int  *errorcode,
				char *errorbuf,
				int	  errorbufsize);

int sendIShutdown(void);
#endif /* RESOURCE_MAMANGER_INTER_PROCESS_COMMUNICATION_RMSEG2RM_H */
