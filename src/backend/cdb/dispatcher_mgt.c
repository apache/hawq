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
 * dispatcher_mgt.c
 *
 *	Dispatcher management is part of execution codes.
 */

#include "postgres.h"

#include "cdb/dispatcher.h"
#include "cdb/dispatcher_mgt.h"
#include "cdb/executormgr.h"
#include "cdb/workermgr.h"
#include "cdb/cdbvars.h"
#include "miscadmin.h"			/* TODO: GetAuthenticatedUserId */

/* for poll */
#ifdef HAVE_POLL_H
#include <poll.h>
#endif
#ifdef HAVE_SYS_POLL_H
#include <sys/poll.h>
#endif
#include "cdb/cdbconn.h"		/* SOCK_ERRNO */


typedef enum DispMgtConstant {
	DISPMGT_POLL_TIME = 2 * 1000,
} DispMgtConstant;

/*
 * QueryExecutorInGroupIterator/QueryExecutorGroupIterator
 */
typedef struct QueryExecutorInGroupIterator
{
	int		executor_id;
	bool	skip_stopped_executor;
} QueryExecutorInGroupIterator;

typedef struct QueryExecutorGroupIterator
{
	struct QueryExecutorTeam	*team;
	int		group_id;
} QueryExecutorGroupIterator;

/* Iterate all of groups. */
static void	dispmgt_init_query_executor_group_iterator(QueryExecutorTeam *team,
							QueryExecutorGroupIterator *iterator);
static QueryExecutorGroup *dispmgt_get_query_executor_group_iterator(
							QueryExecutorGroupIterator *iterator);



/*
 * dispmgt_init_query_executor_group_iterator
 *	Use stack allocate QueryExecutorGroupIterator to simplify error handler.
 */
static void
dispmgt_init_query_executor_group_iterator(QueryExecutorTeam *team,
							  QueryExecutorGroupIterator *iterator)
{
	iterator->team = team;
	iterator->group_id = 0;
}

/*
 * dispmgt_get_query_executor_group_iterator
 */
static QueryExecutorGroup *
dispmgt_get_query_executor_group_iterator(
					QueryExecutorGroupIterator *iterator)
{
	if (iterator->group_id >= iterator->team->query_executor_group_num)
		return NULL;

	return &iterator->team->query_executor_groups[iterator->group_id++];
}

int
dispmgt_get_group_num(QueryExecutorTeam *team)
{
	return team->query_executor_group_num;
}

static void
dispmgt_init_query_executor_in_group_iterator(QueryExecutorGroup *group,
							  QueryExecutorInGroupIterator *iterator,
							  bool skip_stopped_executor)
{
	iterator->executor_id = 0;
	iterator->skip_stopped_executor = skip_stopped_executor;
}

/*
 * dispmgt_get_query_executor_in_group_iterator
 */
static struct QueryExecutor *
dispmgt_get_query_executor_in_group_iterator(QueryExecutorGroup *group,
							  QueryExecutorInGroupIterator *iterator)
{
	for (;
		iterator->executor_id < group->query_executor_num;
		iterator->executor_id++)
	{
		struct QueryExecutor	*executor = group->query_executors[iterator->executor_id];

		if (iterator->skip_stopped_executor &&
			executormgr_is_stop(executor))
			continue;

		if (!executormgr_is_executor_valid(executor))
		  continue;

		/* Increase index or we may return same object in the next iterate. */
		iterator->executor_id++;
		return executor;
	}

	return NULL;
}

void
dispmgt_init_query_executor_iterator(QueryExecutorTeam *team,
										QueryExecutorIterator *iterator)
{
	iterator->team = team;
	iterator->group_id = 0;
	iterator->executor_id = 0;
}

struct QueryExecutor *
dispmgt_get_query_executor_iterator(QueryExecutorIterator *iterator, bool mayContainInvalidExecutor)
{
	QueryExecutorGroup	*group;
	struct QueryExecutor *executor = NULL;

	do {
		if (iterator->group_id >= iterator->team->query_executor_group_num)
			break;

		group = &iterator->team->query_executor_groups[iterator->group_id];

		if (iterator->executor_id >= group->query_executor_num)
		{
			/* No more executor in this group, go to next group. */
			iterator->group_id++;
			iterator->executor_id = 0;
			continue;
		}

		executor = group->query_executors[iterator->executor_id++];
		if (mayContainInvalidExecutor && !executormgr_is_executor_valid(executor))
		  continue;
		else
		  return executor;
	} while (1);

	return NULL;
}

List *
dispmgt_takeover_segment_conns(QueryExecutorTeam *team)
{
	QueryExecutorIterator	iterator;
	struct QueryExecutor	*executor;
	List					*segment_conns = NIL;

	dispmgt_init_query_executor_iterator(team, &iterator);
	while ((executor = dispmgt_get_query_executor_iterator(&iterator, true)))
	{
		segment_conns = lappend(segment_conns, executormgr_takeover_segment_conns(executor));
	}

	return segment_conns;
}

void
dispmgt_free_takeoved_segment_conns(List *takeoved_segment_conns)
{
	ListCell	*lc;

	foreach(lc, takeoved_segment_conns)
	{
		struct SegmentDatabaseDescriptor *desc = lfirst(lc);

		executormgr_free_takeovered_segment_conn(desc);
	}

	list_free(takeoved_segment_conns);
}

static QueryExecutorTeam *
dispmgt_create_executor_team(int group_num)
{
	QueryExecutorTeam	*team = palloc0(sizeof(QueryExecutorTeam));

	team->query_executor_group_num = group_num;
	team->query_executor_groups = palloc0(sizeof(QueryExecutorGroup) * group_num);

	return team;
}

static bool
dispmgt_create_executor_group(QueryExecutorTeam *team, QueryExecutorGroup *group, int executor_num)
{
	int i;

	group->team = team;
	group->query_executor_num = executor_num;
	group->query_executors = palloc0(sizeof(struct QueryExecutor *) * executor_num);

	for (i = 0; i < group->query_executor_num; i++)
		group->query_executors[i] = executormgr_create_executor();

	group->fds = palloc0(sizeof(struct pollfd) * group->query_executor_num);
	return true;
}

/*
 * dispmgt_create_dispmgt_state
 *	Create data structure for workermgr(threads).
 */
QueryExecutorTeam *
dispmgt_create_dispmgt_state(struct DispatchData *data,
							int threads_num,
							int total_executors_num,
							int avg_executors_per_thread)
{
	QueryExecutorTeam		*team;
	QueryExecutorGroup		*group;
	QueryExecutorGroupIterator		group_iterator;

	/*
	 * Create the workermgr data structure based on our schedule.
	 */
	team = dispmgt_create_executor_team(threads_num);
	team->refDispatchData = data;

	dispmgt_init_query_executor_group_iterator(team, &group_iterator);
	while ((group = dispmgt_get_query_executor_group_iterator(&group_iterator)) != NULL)
	{
		int	executors_num_for_this_group = Min(total_executors_num, avg_executors_per_thread);

		total_executors_num -= executors_num_for_this_group;
		dispmgt_create_executor_group(team, group, executors_num_for_this_group);
	}
	Assert(total_executors_num == 0);

	return team;
}

/*
 * dispmgt_thread_func_run
 *	This function is called for dispatching and monitoring the executors in one
 *	group.
 *
 *	Error/Resource boundary: This function should not free memory and does not
 *	stop the thread too. It only cancel or
 */
static void
dispmgt_thread_func_run(QueryExecutorGroup *group, struct WorkerMgrState *state)
{
	QueryExecutorInGroupIterator	iterator;
	struct DispatchData 			*data = group->team->refDispatchData;
	struct QueryExecutor			*executor;

	/* Assume the connections are already set up. */
	dispmgt_init_query_executor_in_group_iterator(group, &iterator, false);
	while ((executor = dispmgt_get_query_executor_in_group_iterator(group, &iterator)) != NULL)
	{
		if (workermgr_should_query_stop(state)) {
			write_log("+++++++++dispmgr_thread_func_run meets should query "
					  "stop before dispatching, entering error_cleanup");
			goto error_cleanup;
		}

		if (!executormgr_dispatch_and_run(data, executor)) {
			write_log("+++++++++dispmgr_thread_func_run meets dispatch_and_run "
					  "problem when dispatching, entering error_cleanup");
			goto error_cleanup;
		}
	}

	/* Poll executors. */
	while (1)
	{
		int 	nfds = 0;
		int 	n;
		int 	cur_fds_idx = 0;

		/* Check global state to abort query, this let poll process easier. */
		if (workermgr_should_query_stop(state)){
			write_log("dispmgr_thread_func_run meets should query stop when "
					  "polling executors, entering error_cleanup");
			goto error_cleanup;
		}
		/* Skip the stopped executor make the logic easy to understand. */
		dispmgt_init_query_executor_in_group_iterator(group, &iterator, true);
		while ((executor = dispmgt_get_query_executor_in_group_iterator(group, &iterator)) != NULL)
		{
			/*
			 * The fds array may shorter than executor array.
			 * DO NOT mark executor stop!
			 */
			group->fds[nfds].fd = executormgr_get_fd(executor);
			group->fds[nfds].events = POLLIN;
			nfds++;
		}

		/* No need to work! */
		if (nfds == 0)
			goto thread_return;

		/*
		 * Use the following strategy to process poll results.
		 * 1. continue if poll was interrupted
		 * 2. 'poll' error should be treated as a query stop error
		 * 2. Lost connection check can be
		 * 3. check the executor state if timeout too many times
		 * 4. check executor returns and stop them if executor finish
		 */
		n = poll(group->fds, nfds, DISPMGT_POLL_TIME);

		if (n < 0 && SOCK_ERRNO == EINTR)
			continue;

		if (n < 0)
		{
			/*
			 * System call poll error is only caused by program bug or system
			 * resources unavailable. In this case, fail the query is okay.
			 */
			goto error_cleanup;
		}

		if (n == 0)
		{
			/*
			 * Network problem and long time query may get here.
			 * Check network problem is expensive.
			 * Long time query only concerns cancel query, check it later in the
			 * loop start.
			 */
			continue;
		}

		/* Someone returns, check it. */
		dispmgt_init_query_executor_in_group_iterator(group, &iterator, true);
		while ((executor = dispmgt_get_query_executor_in_group_iterator(group, &iterator)) != NULL)
		{
			int 	sockfd;

			sockfd = executormgr_get_fd(executor);
			/* TODO: is that safe to call Assert() in a thread ? */
			Assert(group->fds[cur_fds_idx].fd == sockfd);
			if (!(group->fds[cur_fds_idx++].revents & POLLIN))
				continue;

			if (!executormgr_consume(executor)) {
				write_log("dispmgr_thread_func_run meets consume error for executor, entering error_cleanup");
				goto error_cleanup;
			}
		}
	}

error_cleanup:
	/*
	 * Cleanup rules:
	 * 1. query cancel, result error, and poll error: mark the executor stop.
	 * 2. connection error: mark the gang error. Set by workermgr_mark_executor_error().
	 */
  workermgr_set_state_cancel(state);
	dispmgt_init_query_executor_in_group_iterator(group, &iterator, false);
	while ((executor = dispmgt_get_query_executor_in_group_iterator(group, &iterator)) != NULL)
	{
		if (!executormgr_is_stop(executor))
		{
			/*
			 * Executor is running, cancel it. If connection error occurs let
			 * following code cope with it.
			 */
			executormgr_cancel(executor);
		}

		/* Executor stopped but no error. */
		if (!executormgr_has_error(executor))
			continue;
	}

thread_return:
	return;
}

void
dispmgt_dispatch_and_run(struct WorkerMgrState *state,
						struct QueryExecutorTeam *team)
{
	QueryExecutorGroupIterator	group_iterator;
	struct QueryExecutorGroup	*query_executor_group;
	List	*tasks = NIL;

	/* Construct a list of work for each of threads. */

	dispmgt_init_query_executor_group_iterator(team, &group_iterator);
	while ((query_executor_group = dispmgt_get_query_executor_group_iterator(&group_iterator)) != NULL)
		tasks = lappend(tasks, (Task) query_executor_group);

	workermgr_submit_job(state, tasks, (WorkerMgrTaskCallback) dispmgt_thread_func_run);
}

/*
 * dispmgt_create_concurrent_connect_state
 */
static List *
dispmgt_create_concurrent_connect_state(List *executors, int executors_num_per_thread)
{
	int		threads_num = 1;
	int		executors_num = list_length(executors);
	int		left_executors_num = list_length(executors);
	List	*tasks = NIL;
	int		i, j;

	/* Compute threads_num */
	if (executors_num_per_thread > executors_num)
		threads_num = 1;
	else
		threads_num = (executors_num / executors_num_per_thread) + ((executors_num % executors_num_per_thread == 0) ? 0 : 1); 

	/* Create the List of List of info. */
	for (i = 0; i < threads_num; i++)
	{
		List	*task = NIL;

		for (j = 0; j < executors_num_per_thread && left_executors_num > 0; j++)
		{
			task = lappend(task, list_nth(executors, i * executors_num_per_thread + j));
			left_executors_num--;
		}

		tasks = lappend(tasks, task);
	}

	return tasks;
}

static void
dispmgt_free_concurrent_connect_state(List *tasks)
{
	ListCell	*lc;

	foreach(lc, tasks)
		list_free(lfirst(lc));

	list_free(tasks);
}


static void
dispmgt_thread_func_connect(List *executor_info, struct WorkerMgrState *state)
{
	ListCell	*lc;

	foreach(lc, executor_info)
	{
		ConcurrentConnectExecutorInfo *info = lfirst(lc);

		if (workermgr_should_query_stop(state))
			break;

		if (!executormgr_connect(info->desc, info->executor, info->is_writer, info->is_superuser))
			break;
	}

	return;
}

/*
 * dispmgt_concurrent_connect
 */
bool
dispmgt_concurrent_connect(List	*executors, int executors_num_per_thread)
{
	struct  WorkerMgrState *state = NULL;
	List			*tasks;
	volatile bool	has_error = false;
	
	Assert(list_length(executors) > 0);
	tasks = dispmgt_create_concurrent_connect_state(executors, executors_num_per_thread);
	state = workermgr_create_workermgr_state(list_length(tasks));

	PG_TRY();
	{
		workermgr_submit_job(state, tasks, (WorkerMgrTaskCallback) dispmgt_thread_func_connect);
		workermgr_wait_job(state);
	}
	PG_CATCH();
	{
		ListCell	*lc;

		workermgr_cancel_job(state);
		/* We have to clean up the executors. */
		foreach(lc, executors)
			executormgr_free_executor(((ConcurrentConnectExecutorInfo *) lfirst(lc))->desc);

		dispmgt_free_concurrent_connect_state(tasks);
		workermgr_free_workermgr_state(state);

		FlushErrorState();
		has_error = true;
	}
	PG_END_TRY();

	/* Error occured and cleaned up. */
	if (has_error)
		return false;

	CHECK_FOR_INTERRUPTS();
	dispmgt_free_concurrent_connect_state(tasks);
	workermgr_free_workermgr_state(state);

	return true;
}

ConcurrentConnectExecutorInfo *
dispmgt_build_preconnect_info(struct Segment *segment,
							bool is_writer,
							struct QueryExecutor *executor,
							struct DispatchData *data,
							struct DispatchSlice *slice,
							struct DispatchTask *task)
{
	ConcurrentConnectExecutorInfo	*info = palloc0(sizeof(ConcurrentConnectExecutorInfo));

	info->is_writer = is_writer;
	info->is_superuser = superuser_arg(GetAuthenticatedUserId());
	info->executor = executor;
	info->data = data;
	info->slice = slice;
	info->task = task;
	info->desc = executormgr_prepare_connect(segment, is_writer);

	return info;
}

void
dispmgt_free_preconnect_info(ConcurrentConnectExecutorInfo *info)
{
	pfree(info);
}


