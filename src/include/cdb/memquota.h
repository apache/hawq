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

/*-------------------------------------------------------------------------
 *
 * memquota.h
 * Routines related to memory quota for queries.
 *
 *-------------------------------------------------------------------------*/
#ifndef MEMQUOTA_H_
#define MEMQUOTA_H_

#include "postgres.h"
#include "nodes/plannodes.h"
#include "cdb/cdbplan.h"

typedef enum ResQueueMemoryPolicy
{
	RESQUEUE_MEMORY_POLICY_NONE,
	RESQUEUE_MEMORY_POLICY_AUTO,
	RESQUEUE_MEMORY_POLICY_EAGER_FREE
} ResQueueMemoryPolicy;

extern char                		*gp_resqueue_memory_policy_str;
extern ResQueueMemoryPolicy		gp_resqueue_memory_policy;
extern bool						gp_log_resqueue_memory;
extern int						gp_resqueue_memory_policy_auto_fixed_mem;
extern const int				gp_resqueue_memory_log_level;
extern bool						gp_resqueue_print_operator_memory_limits;

extern void PolicyAutoAssignOperatorMemoryKB(PlannedStmt *stmt, uint64 memoryAvailable);
extern void PolicyEagerFreeAssignOperatorMemoryKB(PlannedStmt *stmt, uint64 memoryAvailable);

/**
 * What is the memory reservation for superuser queries?
 */
extern uint64 ResourceQueueGetSuperuserQueryMemoryLimit(void);

/**
 * Inverse for explain analyze.
 */
extern uint64 PolicyAutoStatementMemForNoSpillKB(PlannedStmt *stmt, uint64 minOperatorMemKB);

/**
 * Is result node memory intensive?
 */
extern bool IsResultMemoryIntesive(Result *res);

#endif /* MEMQUOTA_H_ */
