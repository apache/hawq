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

#ifndef _HAWQ_RESOURCE_ENFORCER_H
#define _HAWQ_RESOURCE_ENFORCER_H

#include "resourcemanager/utils/memutilities.h"
#include "resourceenforcer_list.h"

/* Per physical segment resource requirement of a query */
typedef struct SegmentResource
{
	double		vcore;
	double		iopercent;
	uint32		memory;
} SegmentResource;

/* Per physical segment CGroup information of a query */
typedef struct CGroupInfo
{
	char		name[MAXPGPATH];
	uint64		creation_time;
	llist		*pids;
	int32		vcore_current;
	int32		vdisk_current;
	int			to_be_deleted;
} CGroupInfo;

void freeCGroupInfo(void *cgroup_info);
bool isCGroupEnabled(const char *sub_system);
bool isCGroupSetup(const char *sub_system);
void ShowCGroupEnablementInformation(const char *sub_system);

int MoveToCGroup(uint32 pid, const char *cgroup_name);
int MoveOutCGroup(uint32 pid, const char *cgroup_name);
int SetupWeightCGroup(uint32 pid, const char *cgroup_name, SegmentResource *resource);
int CleanUpCGroupAtStartup(const char *sub_system);
int CleanUpCGroupAtRuntime(void);

int HAWQMemoryEnforcementIncreaseQuota(uint64 mem_limit_delta);
int HAWQMemoryEnforcementDecreaseQuota(uint64 mem_limit_delta);

void initCGroupThreads(void);

#endif /* _HAWQ_RESOURCE_ENFORCER_H */
