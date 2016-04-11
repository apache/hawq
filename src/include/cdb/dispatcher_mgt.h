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


#ifndef DISPATCHER_MGT_H
#define DISPATCHER_MGT_H

#include "nodes/pg_list.h"

struct DispatchData;
struct QueryExecutorTeam;
struct QueryExecutor;
struct WorkerMgrState;
struct Segment;
struct DispatchSlice;
struct DispatchTask;

/* Let caller see the declaration to ease the memory allocation problem. */
typedef struct QueryExecutorIterator
{
	struct QueryExecutorTeam	*team;
	int		group_id;
	int		executor_id;
} QueryExecutorIterator;

/*
 * ConcurrentConnectExecutorInfo
 *  Used to create the executor concurrently.
 */
typedef struct ConcurrentConnectExecutorInfo {
  bool          is_writer;
  bool          is_superuser;
  struct QueryExecutor  *executor;
  struct DispatchData   *data;
  struct DispatchSlice  *slice;
  struct DispatchTask   *task;

  struct SegmentDatabaseDescriptor *desc;
} ConcurrentConnectExecutorInfo;

/*
 * QueryExecutorTeam/QueryExecutorGroup
 */
typedef struct QueryExecutorGroup {
	struct QueryExecutorTeam	*team;		/* Reference to the parent */
	int						query_executor_num;

	struct QueryExecutor	**query_executors;
	struct pollfd			*fds;
} QueryExecutorGroup;

typedef struct QueryExecutorTeam {
	/* Must same with thread_num. */
	int						query_executor_group_num;
	QueryExecutorGroup		*query_executor_groups;

	/* Reference to other data structure */
	struct DispatchData 	*refDispatchData;
} QueryExecutorTeam;

/* Iterate all of executors in all groups. */
extern void dispmgt_init_query_executor_iterator(struct QueryExecutorTeam *team,
							QueryExecutorIterator *iterator);
extern struct QueryExecutor *dispmgt_get_query_executor_iterator(
							QueryExecutorIterator *iterator,
							bool mayContainInvalidExecutor);
extern int	dispmgt_get_group_num(struct QueryExecutorTeam *team);

extern struct QueryExecutorTeam *dispmgt_create_dispmgt_state(struct DispatchData *data,
								int threads_num,
								int total_executors_num,
								int avg_executors_per_thread);

extern struct ConcurrentConnectExecutorInfo *dispmgt_build_preconnect_info(
								struct Segment *segment,
								bool is_writer,
								struct QueryExecutor *executor,
								struct DispatchData *data,
								struct DispatchSlice *slice,
								struct DispatchTask *task);
extern void dispmgt_free_preconnect_info(struct ConcurrentConnectExecutorInfo *info);

extern void dispmgt_dispatch_and_run(struct WorkerMgrState *state,
									struct QueryExecutorTeam *team);

extern bool dispmgt_concurrent_connect(List *tasks, int executors_num_per_thread);

/* Expose the executor connection to COPY. */
extern List *dispmgt_takeover_segment_conns(struct QueryExecutorTeam *team);
extern void dispmgt_free_takeoved_segment_conns(List *takeoved_segment_conns);

#endif	/* DISPATCHER_MGT_H */

