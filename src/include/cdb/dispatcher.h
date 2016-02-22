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


#ifndef DISPATCHER_H
#define DISPATCHER_H

#include "lib/stringinfo.h"		/* TODO: Use function to return this? */
#include "postmaster/identity.h"
#include "nodes/pg_list.h"

/* Caller should not access these data sturcture. */
struct DispatchData;
struct DispatchTask;
struct QueryDesc;                   /* #include "executor/execdesc.h" */
struct QueryContextInfo;
struct Node;
struct QueryResource;

typedef struct DispatchDataResult
{
	struct pg_result	**result;
	StringInfoData		errbuf;
	int					numresults;

	/*
	 * Takeover segment connections is quite dangerous, the caller must takecare
	 * of exceptions and return the resources throungh dispatch_free_result().
	 * Maybe add a hint to takeover segments on demand is much better.
	 */
	List	*segment_conns;
} DispatchDataResult;

/*
 * Definition of dispatcher. There are two sets of functions. One set is
 * designed to integrate with executor or some other module. So the caller
 * have to call cleanup functions too.
 * Another set is do all of stuff for you. It will cleanup the workermgr and
 * throw the exception.
 */
extern struct DispatchData *initialize_dispatch_data(struct QueryResource *resource,
				bool dispatch_to_all_cached_executors);
extern void	prepare_dispatch_query_desc(struct DispatchData *data, struct QueryDesc *queryDesc);
extern void	dispatch_run(struct DispatchData *data);
extern void	dispatch_wait(struct DispatchData *data);
extern void	dispatch_catch_error(struct DispatchData *data);
extern void	dispatch_cleanup(struct DispatchData *data);
extern struct CdbDispatchResults	*dispatch_get_results(struct DispatchData *data);
extern int dispatch_get_segment_num(struct DispatchData *data);
extern void	cleanup_dispatch_data(struct DispatchData *data);

extern void	dispatch_statement_string(const char *string,
				const char *serializeQuerytree,
				int serializeLenQuerytree,
				struct QueryResource *resource,
				DispatchDataResult *result,
				bool sync_on_all_executors);
extern void	dispatch_statement_node(struct Node *node,
				struct QueryContextInfo *ctxt,
				struct QueryResource *resource,
				DispatchDataResult *result);
extern void	dispatch_free_result(DispatchDataResult *result);
extern bool	dispatcher_has_error(struct DispatchData *data);
extern void dispatcher_print_statistics(StringInfo buf, struct DispatchData *data);


/* For executormgr access internal data. */
extern ProcessIdentity *dispatch_get_task_identity(struct DispatchTask *task);
extern struct DispatchCommandQueryParms *dispatcher_get_QueryParms(struct DispatchData *data);

extern bool dispatch_validate_conn(pgsocket sock);

void dispatch_end_env(struct DispatchData *data);
void free_dispatch_data(struct DispatchData *data);

#endif	/* DISPATCHER_H */

