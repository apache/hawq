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

#ifndef HAWQ_RESOURCE_MANAGER_COMMUNICATION_TO_GLOBAL_RESOURCE_MANAGER_NONE_H
#define HAWQ_RESOURCE_MANAGER_COMMUNICATION_TO_GLOBAL_RESOURCE_MANAGER_NONE_H

#include "envswitch.h"

#include "rmcomm_RM2GRM.h"

/* Load parameters from file system. */
int RM2GRM_NONE_loadParameters(void);

/* Connect and disconnect to the global resource manager. */
int RM2GRM_NONE_connect(void);
int RM2GRM_NONE_disconnect(void);

/* Register and unregister this application. */
int RM2GRM_NONE_register(void);
int RM2GRM_NONE_unregister(void);

/* Get information. */
int RM2GRM_NONE_getConnectReport(DQueue report);
int RM2GRM_NONE_getClusterReport(DQueue report);
int RM2GRM_NONE_getResQueueReport(DQueue report);

/* Acquire and return resource. */
int RM2GRM_NONE_acquireResource(uint32_t memorymb,
							    uint32_t core,
							    uint32_t contcount,
							    DQueue   containers);
int RM2GRM_NONE_returnResource(DQueue containers);

/* Clean all used memory and connections */
int RM2GRM_NONE_cleanup(void);

#define HAWQDRM_COMMANDLINE_NONESERVER  "-none"	// -yarn + ip + port + quename

int RM2GRM_NONE_refreshClusterInformation(void);


#endif /* HAWQ_RESOURCE_MANAGER_COMMUNICATION_TO_GLOBAL_RESOURCE_MANAGER_NONE_H */
