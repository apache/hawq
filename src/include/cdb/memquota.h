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

extern int 	gp_resqueue_memory_policy_auto_fixed_mem;
extern bool	gp_resqueue_print_operator_memory_limits;

extern void AssignOperatorMemoryKB(PlannedStmt *stmt, uint64 memoryAvailable);

/**
 * Inverse for explain analyze.
 */
extern uint64 StatementMemForNoSpillKB(PlannedStmt *stmt, uint64 minOperatorMemKB);

/**
 * Is result node memory intensive?
 */
extern bool IsResultMemoryIntesive(Result *res);

#endif /* MEMQUOTA_H_ */
