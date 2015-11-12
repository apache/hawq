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
 * workermgr.h
 *
 *-------------------------------------------------------------------------
 */

#ifndef WORKERMGR_H
#define WORKERMGR_H

#include "nodes/pg_list.h"

struct WorkerMgrState;

typedef void *Task;
typedef void (*WorkerMgrTaskCallback) (Task task, struct WorkerMgrState *state);

extern struct WorkerMgrState *workermgr_create_workermgr_state(int threads_num);
extern void workermgr_free_workermgr_state(struct WorkerMgrState *state);
extern bool workermgr_submit_job(struct WorkerMgrState *state,
								List *tasks,
								WorkerMgrTaskCallback func);
extern void	workermgr_wait_job(struct WorkerMgrState *state);
extern void	workermgr_cancel_job(struct WorkerMgrState *state);
extern bool workermgr_should_query_stop(struct WorkerMgrState *state);
extern void workermgr_set_state_cancel(struct WorkerMgrState *state);

#endif	/* WORKERMGR_H */
