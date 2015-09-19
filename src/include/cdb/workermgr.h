/*-------------------------------------------------------------------------
 *
 * workermgr.h
 *
 * Copyright (c) 2014-2014, Pivotal inc
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
