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
 * executormgr.h
 *
 *
 *-------------------------------------------------------------------------
 */

#ifndef EXECUTORMGR_H
#define EXECUTORMGR_H

#include "portability/instr_time.h"	/* Monitor the dispatcher performance */

struct DispatchData;
struct DispatchTask;
struct DispatchSlice;
struct QueryExecutor;
struct Segment;
struct SegmentDatabaseDescriptor;

/* Executor state. */
extern struct QueryExecutor *executormgr_create_executor(void);
extern bool	executormgr_bind_executor_task(struct DispatchData *data,
							struct QueryExecutor *executor,
							struct SegmentDatabaseDescriptor *desc,
							struct DispatchTask *task,
							struct DispatchSlice *slice);
extern void executormgr_serialize_executor_state(struct DispatchData *data,
							struct QueryExecutor *executor,
							struct DispatchTask *task,
							struct DispatchSlice *slice);
extern void	executormgr_unbind_executor_task(struct DispatchData *data,
							struct QueryExecutor *executor,
							struct DispatchTask *task,
							struct DispatchSlice *slice);
extern void executormgr_get_statistics(struct QueryExecutor *executor,
								instr_time *time_connect_begin,
								instr_time *time_connect_end,
								instr_time *time_dispatch_begin,
								instr_time *time_dispatch_end,
			          instr_time *time_consume_begin,
			          instr_time *time_consume_end,
			          instr_time *time_free_begin,
			          instr_time *time_free_end);
extern struct SegmentDatabaseDescriptor *executormgr_allocate_executor(
							struct Segment *segment, bool is_writer, bool is_entrydb);
extern struct SegmentDatabaseDescriptor *executormgr_takeover_segment_conns(struct QueryExecutor *executor);
extern void executormgr_free_takeovered_segment_conn(struct SegmentDatabaseDescriptor *desc);
extern void executormgr_get_executor_connection_info(struct QueryExecutor *executor,
							char **address, int *port, int *pid);
extern bool	executormgr_is_stop(struct QueryExecutor *executor);
extern bool	executormgr_has_error(struct QueryExecutor *executor);
extern int	executormgr_get_executor_slice_id(struct QueryExecutor *executor);
extern int	executormgr_get_segment_ID(struct QueryExecutor *executor);
extern int	executormgr_get_fd(struct QueryExecutor *executor);
extern bool	executormgr_cancel(struct QueryExecutor * executor);
extern bool	executormgr_dispatch_and_run(struct DispatchData *data, struct QueryExecutor *executor);
extern bool	executormgr_check_segment_status(struct QueryExecutor *executor);
extern void	executormgr_seterrcode_if_needed(struct QueryExecutor *executor);
extern bool	executormgr_consume(struct QueryExecutor *executor);
extern bool	executormgr_discard(struct QueryExecutor *executor);
extern void	executormgr_merge_error(struct QueryExecutor *exeutor);
extern void executormgr_merge_error_for_dispatcher(
    struct QueryExecutor *executor, int *errHostSize,
    int *errNum, char ***errHostInfo);
extern bool executormgr_is_executor_error(struct QueryExecutor *executor);
extern bool executormgr_is_executor_valid(struct QueryExecutor *executor);
extern void executormgr_setup_env(MemoryContext ctx);
extern void executormgr_cleanup_env(void);
extern int executormgr_get_cached_executor_num(void);
extern int executormgr_get_cached_executor_num_onentrydb(void);

/* API used to connect executor concurrently. */
extern struct SegmentDatabaseDescriptor *executormgr_prepare_connect(
							struct Segment *segment,
							bool is_writer);
extern bool executormgr_connect(struct SegmentDatabaseDescriptor *desc,
							struct QueryExecutor *executor,
							bool is_writer, bool is_superuser);
extern void executormgr_free_executor(struct SegmentDatabaseDescriptor *desc);

#endif	/* EXECUTORMGR_H */

