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

/*
 * workermgr.c
 *	Worker manager is designed to delivery a general thread-based task framework.
 *
 *	From the callee sight, there is a 'job' to run and needs some threads.
 *	Each job contains some groups, which is schedule unit. Each group corresponds
 *	to a thread in the worker manager. In each group, there are some tasks. The
 *	tasks in one group have to be same property. But each group may have
 *	different property.
 */
#include "postgres.h"
#include <pthread.h>

#include "cdb/workermgr.h"

#include "cdb/cdbgang.h"		/* gp_pthread_create */
#include "miscadmin.h"			/* TODO: InterruptPending */

#include "utils/faultinjector.h"


/*
 * This structure abstract the general job.
 */
typedef struct WorkerMgrThread {
	bool	started;
	int		thread_errno;
	int		thread_ret;
	pthread_t	thread;

	/* Argument passed to thread. */
	struct WorkerMgrState		*state;
	WorkerMgrTaskCallback		func;
	Task						task;
} WorkerMgrThread;

typedef struct WorkerMgrState {
	/* Control flags */
	volatile bool	cancel;

	int					threads_num;
	WorkerMgrThread		threads[0];
} WorkerMgrState;

typedef struct WorkerMgrThreadIterator {
	int		thread_id;
} WorkerMgrThreadIterator;


static void	workermgr_init_thread_iterator(WorkerMgrState *state, WorkerMgrThreadIterator *iterator);
static WorkerMgrThread *workermgr_get_thread_iterator(WorkerMgrState *state, WorkerMgrThreadIterator *iterator);
static void *workermgr_thread_func(void *arg);
static void workermgr_join(WorkerMgrState *state);


/*
 * workermgr_create_workermgr_state
 *	Create data structures for threads_num of threads.
 */
WorkerMgrState *
workermgr_create_workermgr_state(int threads_num)
{
	WorkerMgrState	*state;

	/* Allocate the threads control data structure. */
	state = palloc0(sizeof(WorkerMgrState) + threads_num * sizeof(WorkerMgrThread));
	state->threads_num = threads_num;

	return state;
}

void
workermgr_free_workermgr_state(WorkerMgrState *state)
{
	pfree(state);
}

/*
 * workermgr_submit_job
 *	Error/Resource boundary: This function should not free memory. All of the
 *	other resources should be released.
 */
bool
workermgr_submit_job(WorkerMgrState *state,
					List *tasks,
					WorkerMgrTaskCallback func)
{
	WorkerMgrThreadIterator		thread_iterator;
	WorkerMgrThread				*worker_mgr_thread;
	int		i = 0;

	workermgr_init_thread_iterator(state, &thread_iterator);
	while ((worker_mgr_thread = workermgr_get_thread_iterator(state, &thread_iterator)) != NULL)
	{
		worker_mgr_thread->state = state;
		worker_mgr_thread->task = (Task) list_nth(tasks, i);
		i++;
		worker_mgr_thread->func = func;

#ifdef FAULT_INJECTOR
				FaultInjector_InjectFaultIfSet(
											   WorkerManagerSubmitJob,
											   DDLNotSpecified,
											   "",	// databaseName
											   ""); // tableName
#endif

		worker_mgr_thread->thread_ret = gp_pthread_create(&worker_mgr_thread->thread, workermgr_thread_func, worker_mgr_thread, "submit_plan_to_qe");
		if (worker_mgr_thread->thread_ret)
			goto error_cleanup;
		worker_mgr_thread->started = true;
	}

	return true;

error_cleanup:
	/* cleanup */
	state->cancel = true;
	workermgr_join(state);
	CHECK_FOR_INTERRUPTS();
	
	return false;
}

void
workermgr_set_state_cancel(WorkerMgrState *state)
{
  state->cancel = true;
}

/*
 * workermgr_cancel_job
 */
void
workermgr_cancel_job(WorkerMgrState *state)
{
	state->cancel = true;
	workermgr_join(state);
	CHECK_FOR_INTERRUPTS();
}

void
workermgr_wait_job(WorkerMgrState *state)
{
	state->cancel = false;
	workermgr_join(state);
}

static void
workermgr_init_thread_iterator(WorkerMgrState *state, WorkerMgrThreadIterator *iterator)
{
	iterator->thread_id = 0;
}

static WorkerMgrThread *
workermgr_get_thread_iterator(WorkerMgrState *state, WorkerMgrThreadIterator *iterator)
{
	if (iterator->thread_id >= state->threads_num)
		return NULL;

	return &state->threads[iterator->thread_id++];
}

/*
 * workermgr_thread_func
 */
static void *
workermgr_thread_func(void *arg)
{
	WorkerMgrThread	*thread = (WorkerMgrThread *) arg;

	thread->func(thread->task, thread->state);
	return NULL;
}

bool
workermgr_should_query_stop(WorkerMgrState *state)
{
	if (state->cancel)
		return true;
	/* Die/Cancel/Lost Client */
	if (InterruptPending)
		return true;

	return false;
}

/*
 * workermgr_thread_cleanup
 *	TODO: pthread_join may not be able to interrupt, so change it to a loop!
 */
static void
workermgr_thread_cleanup(WorkerMgrThread *thread)
{
	if (thread->started)
	{
		pthread_join(thread->thread, NULL);
		thread->started = false;
	}
}

/*
 * workermgr_join
 *	Cleanup the threads, set state->cancel properly before calling. This
 *	function have to be safe even if it was interrupted and get into again.
 */
static void
workermgr_join(WorkerMgrState *state)
{
	WorkerMgrThreadIterator		iterator;
	WorkerMgrThread				*thread;

	workermgr_init_thread_iterator(state, &iterator);
	while ((thread = workermgr_get_thread_iterator(state, &iterator)) != NULL)
	{
		workermgr_thread_cleanup(thread);
	}
}

