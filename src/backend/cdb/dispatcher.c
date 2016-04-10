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
 * dispatcher.c
 *	  Dispatcher is a coordinator between optimizer, wokermgr, tx mgr, and so on.
 * Dispatcher like the logical dispatcher doing plan stuff and workermgr is like
 * physical part of dispatcher. But workermgr should still be general to other
 * module too.
 *
 * Dispatcher needs to compute the necessary information that QE needs and
 * delivery them to the workermgr.
 *
 *	TODO: QueryContext is not cool that its leve is not clear. ALTER use it
 *	outside of dispatcher and queryDesc use it inside. Have to redesign it.
 */

#include "postgres.h"
#include <pthread.h>
#include <limits.h>

#include "cdb/dispatcher.h"
#include "cdb/dispatcher_mgt.h"
#include "cdb/workermgr.h"
#include "cdb/executormgr.h"
#include "postmaster/identity.h"
#include "cdb/cdbdisp.h"	/* TODO: should remove */
#include "cdb/cdbgang.h"	/* SliceTable & Gang */
#include "cdb/cdbvars.h"	/* gp_max_plan_size */
#include "executor/executor.h"	/* RootSliceIndex */
#include "cdb/cdbrelsize.h"	/* clear_relsize_cache */
#include "utils/memutils.h"	/* GetMemoryChunkContext */
#include "cdb/cdbsrlz.h"	/* serializeNode */
#include "utils/datum.h"	/* datumGetSize */
#include "utils/faultinjector.h"
#include "utils/lsyscache.h"	/* get_typlenbyval */
#include "miscadmin.h"		/* CHECK_FOR_INTERRUPTS */
#include "tcop/tcopprot.h"	/* ResetUsage */
#include "portability/instr_time.h"	/* Monitor the dispatcher performance */
#include "cdb/cdbdispatchresult.h"	/* cdbdisp_makeDispatchResults */
#include "lib/stringinfo.h"			/* StringInfoData */
#include "tcop/pquery.h"			/* PortalGetResource */

#include "resourcemanager/communication/rmcomm_QD2RM.h"

/* Define and structure */
typedef struct DispatchTask
{
	ProcessIdentity	id;
	/*
	 * Information about executor which one is assigned this task.
	 * NULL means match any segment except entry db!
	 */
	Segment			*segment;

	bool  entrydb;  /*whether this task should be dispatched to entrydb*/
} DispatchTask;

typedef struct DispatchSlice
{
	int				tasks_num;
	DispatchTask	*tasks;
} DispatchSlice;

typedef struct DispatchMetaData {
	/* Share Information */
	int				all_slices_num;		/* EXPLAIN needs all slices num to store ANALYZE stats */
	int				used_slices_num;
	DispatchSlice	*slices;
} DispatchMetaData;

/*
 * DispatchState
 *	Change and check state should in a wise way.
 */
typedef enum DispatchState {
	DS_INIT,		/* Not running */
	DS_RUN,			/* Running */
	DS_OK,			/* Running without error */
	DS_ERROR,		/* Running encounter error */
	DS_DONE,		/* Cleanup and Error consumed */
} DispatchState;

#define dispatcher_set_state_init(data)		((data)->state = DS_INIT)
#define dispatcher_set_state_run(data)		((data)->state = DS_RUN)
#define dispatcher_set_state_ok(data)		((data)->state = DS_OK)
#define dispatcher_set_state_error(data)	((data)->state = DS_ERROR)
#define dispatcher_set_state_done(data)		((data)->state = DS_DONE)
#define dispatcher_is_state_init(data)		((data)->state == DS_INIT)
#define dispatcher_is_state_run(data)		((data)->state == DS_RUN)
#define dispatcher_is_state_ok(data)		((data)->state == DS_OK)
#define dispatcher_is_state_error(data)		((data)->state == DS_ERROR)
#define dispatcher_is_state_done(data)		((data)->state == DS_DONE)

#define ERROR_HOST_INIT_SIZE 5

typedef struct DispatchData
{
	DispatchState		state;

	/* Original Information */
	struct QueryDesc	*queryDesc;
	int					localSliceId;

	/* Payload */
	DispatchCommandQueryParms	*pQueryParms;
	struct SliceTable			*sliceTable;

	/* Resource that we can use. */
	bool				resource_is_mine;
	QueryResource		*resource;
	bool				dispatch_to_all_cached_executors;	/* sync all of executors */

	/* Readonly for worker manager */
	DispatchMetaData			job;

	/* Communication between dispatcher and worker manager. */
	struct QueryExecutorTeam	*query_executor_team;
	struct CdbDispatchResults	*results;	/* TODO: libpq is ugly, should invent a new connection manager. */

	/* Maintained by worker manager. */
	void				*worker_mgr_state;

	/* Statistics */
	int segment_num; /* the number of segments for this query */
	int segment_num_on_entrydb;
	int					num_of_cached_executors;
	int					num_of_new_connected_executors;

	/*
	 * This is the start time of dispatcher, use last dispatched executor
	 * finished time substract this is one is the total time.
	 */
	instr_time			time_begin;
	instr_time			time_total;			/* time_last_dispatch_end - time_begin */
	instr_time			time_connect;		/* time_last_connect_end - time_first_connect_begin */
	instr_time			time_dispatch;		/* time_last_dispatch_end - time_first_dispatch_begin */

	/* Performance Counter: dispatcher side */
	instr_time			time_first_connect_begin;
	instr_time			time_last_connect_end;
	instr_time			time_first_dispatch_begin;
	instr_time			time_last_dispatch_end;

	/* Performance Counter: executor side */
	instr_time			time_max_dispatched;
	instr_time			time_min_dispatched;
	instr_time			time_total_dispatched;

  instr_time      time_max_consumed;
  instr_time      time_min_consumed;
  instr_time      time_total_consumed;

  instr_time      time_max_free;
  instr_time      time_min_free;
  instr_time      time_total_free;
	int					num_of_dispatched;
} DispatchData;

/* The following two structures used to generailize dispatch statement */
typedef enum DispatchStatementType {
	DST_STRING,
	DST_NODE,
} DispatchStatementType;

typedef struct DispatchStatement {
	DispatchStatementType	type;

	/* String type: change this to union is better */
	const char		*string;
	const char		*serializeQuerytree;
	int				serializeLenQuerytree;

	/* Node type */
	struct Node *node;
	struct QueryContextInfo *ctxt;

	/* Control tag */
	bool			dispatch_to_all_cached_executor;
} DispatchStatement;

/* Global states: DO NOT add global state unless you have to. */
static MemoryContext	DispatchDataContext;
static int	            DispatchInitCount = -1;


/* Static function */
static DispatchCommandQueryParms *dispatcher_fill_query_param(const char *strCommand,
								char *serializeQuerytree,
								int serializeLenQuerytree,
								char *serializePlantree,
								int serializeLenPlantree,
								char *serializeParams,
								int serializeLenParams,
								char *serializeSliceInfo,
								int serializeLenSliceInfo,
								int rootIdx,
								QueryResource *resource);
static void dispatcher_split_logical_tasks_for_query_desc(DispatchData *data);
static void dispatcher_split_logical_tasks_for_statemet_or_node(DispatchData *data);
static bool dispatcher_init_works(DispatchData *data);
static bool dispatcher_compute_threads_num(DispatchData *data);
static bool	dispatch_collect_executors_error(DispatchData * data);
static void	dispatch_throw_error(DispatchData *data);
static void dispatch_init_env(void);
static int dispatcher_get_total_query_executors_num(DispatchData *data);


static void
remove_subquery_in_RTEs(Node *node)
{
	if (node == NULL)
	{
		return;
	}

 	if (IsA(node, RangeTblEntry))
 	{
 		RangeTblEntry *rte = (RangeTblEntry *)node;
 		if (RTE_SUBQUERY == rte->rtekind && NULL != rte->subquery)
 		{
 			/*
 			 * replace subquery with a dummy subquery
 			 */
 			rte->subquery = makeNode(Query);
 		}

 		return;
 	}

 	if (IsA(node, List))
 	{
 		List *list = (List *) node;
 		ListCell   *lc = NULL;
 		foreach(lc, list)
 		{
 			remove_subquery_in_RTEs((Node *) lfirst(lc));
 		}
 	}
}

/* should remove */
typedef struct {
	int sliceIndex;
	int children;
	Slice *slice;
} sliceVec;
static int
compare_slice_order(const void *aa, const void *bb)
{
	sliceVec *a = (sliceVec *)aa;
	sliceVec *b = (sliceVec *)bb;

	if (a->slice == NULL)
		return 1;
	if (b->slice == NULL)
		return -1;

	/* sort the writer gang slice first, because he sets the shared snapshot */
	if (a->slice->is_writer && !b->slice->is_writer)
		return -1;
	else if (b->slice->is_writer && !a->slice->is_writer)
		return 1;

	if (a->children == b->children)
		return 0;
	else if (a->children > b->children)
		return 1;
	else
		return -1;
}
static void
mark_bit(char *bits, int nth)
{
	int nthbyte = nth >> 3;
	char nthbit  = 1 << (nth & 7);
	bits[nthbyte] |= nthbit;
}
static void
or_bits(char* dest, char* src, int n)
{
	int i;

	for(i=0; i<n; i++)
		dest[i] |= src[i];
}

static int
count_bits(char* bits, int nbyte)
{
	int i;
	int nbit = 0;
	int bitcount[] = {
		0, 1, 1, 2, 1, 2, 2, 3, 1, 2, 2, 3, 2, 3, 3, 4
	};

	for(i=0; i<nbyte; i++)
	{
		nbit += bitcount[bits[i] & 0x0F];
		nbit += bitcount[(bits[i] >> 4) & 0x0F];
	}

	return nbit;
}

static int markbit_dep_children(SliceTable *sliceTable, int sliceIdx, sliceVec *sliceVec, int bitmasklen, char* bits)
{
	ListCell *sublist;
	Slice *slice = (Slice *) list_nth(sliceTable->slices, sliceIdx);

	foreach(sublist, slice->children)
	{
		int childIndex = lfirst_int(sublist);
		char *newbits = palloc0(bitmasklen);

		markbit_dep_children(sliceTable, childIndex, sliceVec, bitmasklen, newbits);
		or_bits(bits, newbits, bitmasklen);
		mark_bit(bits, childIndex);
		pfree(newbits);
	}

	sliceVec[sliceIdx].sliceIndex = sliceIdx;
	sliceVec[sliceIdx].children = count_bits(bits, bitmasklen);
	sliceVec[sliceIdx].slice = slice;

	return sliceVec[sliceIdx].children;
}
static int
count_dependent_children(SliceTable * sliceTable, int sliceIndex, sliceVec *sliceVector, int len)
{
	int 		ret = 0;
	int			bitmasklen = (len+7) >> 3;
	char 	   *bitmask = palloc0(bitmasklen);

	ret = markbit_dep_children(sliceTable, sliceIndex, sliceVector, bitmasklen, bitmask);
	pfree(bitmask);

	return ret;
}
static int
fillSliceVector(SliceTable *sliceTbl, int rootIdx, sliceVec *sliceVector, int sliceLim)
{
	int top_count;

	/* count doesn't include top slice add 1 */
	top_count = 1 + count_dependent_children(sliceTbl, rootIdx, sliceVector, sliceLim);

	qsort(sliceVector, sliceLim, sizeof(sliceVec), compare_slice_order);

	return top_count;
}

static void
aggregateQueryResource(QueryResource *queryRes)
{
	MemoryContext	old;
	old = MemoryContextSwitchTo(DispatchDataContext);

	if (queryRes &&
		queryRes->segments &&
		(list_length(queryRes->segments) > 0))
	{
		ListCell *lc1 = NULL;
		ListCell *lc2 = NULL;
		Segment *seg1 = NULL;
		Segment *seg2 = NULL;
		int		nseg  = 0;
		int		i	  = 0;

		queryRes->numSegments = list_length(queryRes->segments);
		queryRes->segment_vcore_agg = NULL;
		queryRes->segment_vcore_agg = palloc0(sizeof(int) * queryRes->numSegments);
		queryRes->segment_vcore_writer = palloc0(sizeof(int) * queryRes->numSegments);

		foreach (lc1, queryRes->segments)
		{
		  int j = 0;
			nseg = 0;
			seg1 = (Segment *) lfirst(lc1);
			queryRes->segment_vcore_writer[i] = i;
			foreach (lc2, queryRes->segments)
			{
				seg2 = (Segment *) lfirst(lc2);
				if (strcmp(seg1->hostname, seg2->hostname) == 0)
				{
					nseg++;
					if (j < queryRes->segment_vcore_writer[i])
					{
					  queryRes->segment_vcore_writer[i] = j;
					}
				}
				j++;
			}

			queryRes->segment_vcore_agg[i++] = nseg;
		}
	}

	MemoryContextSwitchTo(old);
}

static void
dispatch_init_env(void)
{
	++DispatchInitCount;
	if (DispatchDataContext) {
		if (DispatchInitCount == 0)
			MemoryContextResetAndDeleteChildren(DispatchDataContext);
		return;
	}
	DispatchDataContext = AllocSetContextCreate(TopMemoryContext,
			"Dispatch Data Context",
			ALLOCSET_DEFAULT_MINSIZE,
			ALLOCSET_DEFAULT_INITSIZE,
			ALLOCSET_DEFAULT_MAXSIZE);

	executormgr_setup_env(TopMemoryContext);
}


/* Implementation  TODO: Need Modify resource parameter */
DispatchData *
initialize_dispatch_data(QueryResource *resource, bool dispatch_to_all_cached_executors)
{
	DispatchData	*dispatch_data;
	MemoryContext	old;

	dispatch_init_env();

	old = MemoryContextSwitchTo(DispatchDataContext);
	dispatch_data = palloc0(sizeof(DispatchData));
	MemoryContextSwitchTo(old);
	dispatcher_set_state_init(dispatch_data);

	dispatch_data->dispatch_to_all_cached_executors = dispatch_to_all_cached_executors;

	if (dispatch_to_all_cached_executors)
	{
		/*
		 * If one statement needs to be dispatched to all cached
		 * executors, resource is useless.
		 */
		Assert(resource == NULL);
		dispatch_data->segment_num = executormgr_get_cached_executor_num();
		dispatch_data->segment_num_on_entrydb = executormgr_get_cached_executor_num_onentrydb();
	}
	else
	{
		Assert(resource);

		dispatch_data->resource_is_mine = false;
		dispatch_data->resource = resource;
		dispatch_data->segment_num = list_length(resource->segments);
	}

	INSTR_TIME_SET_ZERO(dispatch_data->time_max_dispatched);
	INSTR_TIME_SET_ZERO(dispatch_data->time_min_dispatched);
	INSTR_TIME_SET_ZERO(dispatch_data->time_total_dispatched);
  INSTR_TIME_SET_ZERO(dispatch_data->time_max_consumed);
  INSTR_TIME_SET_ZERO(dispatch_data->time_min_consumed);
  INSTR_TIME_SET_ZERO(dispatch_data->time_total_consumed);
  INSTR_TIME_SET_ZERO(dispatch_data->time_max_free);
  INSTR_TIME_SET_ZERO(dispatch_data->time_min_free);
  INSTR_TIME_SET_ZERO(dispatch_data->time_total_free);
	return dispatch_data;
}

/*
 * prepare_dispatch_query_desc
 *	Have to generate logical splits of the plan.
 */
void
prepare_dispatch_query_desc(DispatchData *data,
							struct QueryDesc *queryDesc)
{
	char	*splan,
			*sparams;

	int 	splan_len,
			splan_len_uncompressed,
			sparams_len;

	SliceTable *sliceTbl;
	int			rootIdx;
	PlannedStmt	   *stmt;
	bool		is_SRI;

	data->queryDesc = queryDesc;

	Assert(queryDesc != NULL && queryDesc->estate != NULL);

	/*
	 * Later we'll need to operate with the slice table provided via the
	 * EState structure in the argument QueryDesc.	Cache this information
	 * locally and assert our expectations about it.
	 */
	sliceTbl = queryDesc->estate->es_sliceTable;
	rootIdx = RootSliceIndex(queryDesc->estate);

	Assert(sliceTbl != NULL);
	Assert(rootIdx == 0 ||
		   (rootIdx > sliceTbl->nMotions && rootIdx <= sliceTbl->nMotions + sliceTbl->nInitPlans));

	/*
	 * Keep old value so we can restore it.  We use this field as a parameter.
	 */
	data->localSliceId = sliceTbl->localSlice;

	/*
	 * This function is called only for planned statements.
	 */
	stmt = queryDesc->plannedstmt;
	Assert(stmt);


	/*
	 * Let's evaluate STABLE functions now, so we get consistent values on the QEs
	 *
	 * Also, if this is a single-row INSERT statement, let's evaluate
	 * nextval() and currval() now, so that we get the QD's values, and a
	 * consistent value for everyone
	 *
	 */

	is_SRI = false;

	if (queryDesc->operation == CMD_INSERT)
	{
		Assert(stmt->commandType == CMD_INSERT);

		/* We might look for constant input relation (instead of SRI), but I'm afraid
		 * that wouldn't scale.
		 */
		is_SRI = IsA(stmt->planTree, Result) && stmt->planTree->lefttree == NULL;
	}

	if (!is_SRI)
		clear_relsize_cache();

	if (queryDesc->operation == CMD_INSERT ||
		queryDesc->operation == CMD_SELECT ||
		queryDesc->operation == CMD_UPDATE ||
		queryDesc->operation == CMD_DELETE)
	{

		MemoryContext oldContext;
		
		oldContext = CurrentMemoryContext;
		if ( stmt->qdContext ) /* Temporary! See comment in PlannedStmt. */
		{
			oldContext = MemoryContextSwitchTo(stmt->qdContext);
		}
		else /* MPP-8382: memory context of plan tree should not change */
		{
			MemoryContext mc = GetMemoryChunkContext(stmt->planTree);
			oldContext = MemoryContextSwitchTo(mc);
		}

		stmt->planTree = (Plan *) exec_make_plan_constant(stmt, is_SRI);
		
		MemoryContextSwitchTo(oldContext);
	}

	/*
	 *	MPP-20785:
	 *	remove subquery field from RTE's since it is not needed during query
	 *	execution,
	 *	this is an optimization to reduce size of serialized plan before dispatching
	 */
	remove_subquery_in_RTEs((Node *) (queryDesc->plannedstmt->rtable));

	/*
	 * serialized plan tree. Note that we're called for a single
	 * slice tree (corresponding to an initPlan or the main plan), so the
	 * parameters are fixed and we can include them in the prefix.
	 */
	splan = serializeNode((Node *) queryDesc->plannedstmt, &splan_len, &splan_len_uncompressed);

	/* compute the total uncompressed size of the query plan for all slices */
	int num_slices = queryDesc->plannedstmt->planTree->nMotionNodes + 1;
	uint64 plan_size_in_kb = ((uint64)splan_len_uncompressed * (uint64)num_slices) / (uint64)1024;
	
	elog(DEBUG1, "Query plan size to dispatch: " UINT64_FORMAT "KB", plan_size_in_kb);

	if (0 < gp_max_plan_size && plan_size_in_kb > gp_max_plan_size)
	{
		ereport(ERROR,
			(errcode(ERRCODE_STATEMENT_TOO_COMPLEX),
				(errmsg("Query plan size limit exceeded, current size: " UINT64_FORMAT "KB, max allowed size: %dKB", plan_size_in_kb, gp_max_plan_size),
				 errhint("Size controlled by gp_max_plan_size"))));
	}

	Assert(splan != NULL && splan_len > 0 && splan_len_uncompressed > 0);
	
	if (queryDesc->params != NULL && queryDesc->params->numParams > 0)
	{		
        ParamListInfoData  *pli;
        ParamExternData    *pxd;
        StringInfoData      parambuf;
		Size                length;
        int                 plioff;
		int32               iparam;

        /* Allocate buffer for params */
        initStringInfo(&parambuf);

        /* Copy ParamListInfoData header and ParamExternData array */
        pli = queryDesc->params;
        length = (char *)&pli->params[pli->numParams] - (char *)pli;
        plioff = parambuf.len;
        Assert(plioff == MAXALIGN(plioff));
        appendBinaryStringInfo(&parambuf, pli, length);

        /* Copy pass-by-reference param values. */
        for (iparam = 0; iparam < queryDesc->params->numParams; iparam++)
		{
			int16   typlen;
			bool    typbyval;

            /* Recompute pli each time in case parambuf.data is repalloc'ed */
            pli = (ParamListInfoData *)(parambuf.data + plioff);
			pxd = &pli->params[iparam];

            /* Does pxd->value contain the value itself, or a pointer? */
			get_typlenbyval(pxd->ptype, &typlen, &typbyval);
            if (!typbyval)
            {
				char   *s = DatumGetPointer(pxd->value);

				if (pxd->isnull ||
                    !PointerIsValid(s))
                {
                    pxd->isnull = true;
                    pxd->value = 0;
                }
				else
				{
			        length = datumGetSize(pxd->value, typbyval, typlen);

					/* MPP-1637: we *must* set this before we
					 * append. Appending may realloc, which will
					 * invalidate our pxd ptr. (obviously we could
					 * append first if we recalculate pxd from the new
					 * base address) */
                    pxd->value = Int32GetDatum(length);

                    appendBinaryStringInfo(&parambuf, &iparam, sizeof(iparam));
                    appendBinaryStringInfo(&parambuf, s, length);
				}
            }
		}
        sparams = parambuf.data;
        sparams_len = parambuf.len;
	}
	else
	{
		sparams = NULL;
		sparams_len = 0;
	}

	data->pQueryParms = dispatcher_fill_query_param(queryDesc->sourceText,
							NULL, 0,
							splan, splan_len,
							sparams, sparams_len,
							NULL, 0,
							rootIdx,
							queryDesc->resource);

	Assert(sliceTbl);
	Assert(sliceTbl->slices != NIL);

	data->sliceTable = sliceTbl;

	dispatcher_split_logical_tasks_for_query_desc(data);
}

static void
prepare_dispatch_statement_string(DispatchData *data,
								const char *statement,
								const char *serializeQuerytree,
								int serializeLenQuerytree)
{
	data->pQueryParms = dispatcher_fill_query_param(statement,
							(char *) serializeQuerytree, serializeLenQuerytree,
							NULL, 0,
							NULL, 0,
							NULL, 0,
							0,
							data->resource);
	dispatcher_split_logical_tasks_for_statemet_or_node(data);
}

static void
prepare_dispatch_statement_node(struct DispatchData *data,
								struct Node *node,
								struct QueryContextInfo *ctxt)
{
	char	   *serializedQuerytree;
	int			serializedQuerytree_len;
	Query	   *q = makeNode(Query);

	/* Construct an utility query node. */
	q->commandType = CMD_UTILITY;
	q->utilityStmt = node;
	q->contextdisp = ctxt;
	q->querySource = QSRC_ORIGINAL;
	/*
	 * We must set q->canSetTag = true.  False would be used to hide a command
	 * introduced by rule expansion which is not allowed to return its
	 * completion status in the command tag (PQcmdStatus/PQcmdTuples). For
	 * example, if the original unexpanded command was SELECT, the status
	 * should come back as "SELECT n" and should not reflect other commands
	 * inserted by rewrite rules.  True means we want the status.
	 */
	q->canSetTag = true;
	serializedQuerytree = serializeNode((Node *) q, &serializedQuerytree_len, NULL /*uncompressed_size*/);

	data->pQueryParms = dispatcher_fill_query_param(debug_query_string,
								serializedQuerytree, serializedQuerytree_len,
								NULL, 0,
								NULL, 0,
								NULL, 0,
								0,
								data->resource);

	dispatcher_split_logical_tasks_for_statemet_or_node(data);
}

/*
 * dispatcher_split_logical_tasks_for_query_desc
 *	Have to generate a parallel plan to execute.
 */
static void
dispatcher_split_logical_tasks_for_query_desc(DispatchData *data)
{
	MemoryContext	old;
	old = MemoryContextSwitchTo(DispatchDataContext);

	sliceVec	*sliceVector = NULL;
	int			i;
	int			slice_num;

	/*
	 * Dispatch order is top-bottom, this will reduce the useless pkts sends
	 * from scan nodes.
	 */
	sliceVector = palloc0(list_length(data->sliceTable->slices) * sizeof(*sliceVector));
	slice_num = fillSliceVector(data->sliceTable,
								 data->pQueryParms->rootIdx,
								 sliceVector,
								 list_length(data->sliceTable->slices));

	data->job.used_slices_num = slice_num;
	data->job.all_slices_num = list_length(data->sliceTable->slices);

	data->job.slices = palloc0(data->job.used_slices_num * sizeof(DispatchSlice));
	for (i = 0; i < data->job.used_slices_num; i++)
	{
		data->job.slices[i].tasks_num = list_length(data->resource->segments);
		data->job.slices[i].tasks = palloc0(data->job.slices[i].tasks_num * sizeof(DispatchTask));
	}

	for (i = 0; i < slice_num; i++)
	{
		Slice			*slice = sliceVector[i].slice;
		bool	is_writer = false;

		is_writer = (i == 0 && !data->queryDesc->extended_query);
		/* Not all of slices need to dispatch, such as root slice! */
		if (slice->gangType == GANGTYPE_UNALLOCATED)
		{
			data->job.used_slices_num--;
			continue;
		}

		/* Direct dispatch may not useful in Hawq 2.0. */
		if (slice->directDispatch.isDirectDispatch)
		{
			DispatchTask	*task = &data->job.slices[i].tasks[0];

			data->job.slices[i].tasks_num = 1;

			task->id.id_in_slice = linitial_int(slice->directDispatch.contentIds);
			task->id.slice_id = slice->sliceIndex;
			task->id.gang_member_num = list_length(data->resource->segments);
			task->id.command_count = gp_command_count;
			task->id.is_writer = is_writer;
			task->segment = list_nth(data->resource->segments, task->id.id_in_slice);
			task->id.init = true;
			
		}
		else if (slice->gangSize == 1)
		{
			DispatchTask	*task = &data->job.slices[i].tasks[0];

			/* Run some single executor case. Should merge with direct dispatch! */
			data->job.slices[i].tasks_num = 1;

			/* TODO: I really want to remove the MASTER_CONTENT_ID. */
			if (slice->gangType == GANGTYPE_ENTRYDB_READER)
			{
				task->id.id_in_slice = MASTER_CONTENT_ID;
				task->segment = data->resource->master;
			}
			else
			{
				task->id.id_in_slice = 0;
				task->segment = linitial(data->resource->segments);
			}

			task->id.slice_id = slice->sliceIndex;
			task->id.gang_member_num = list_length(data->resource->segments);
			task->id.command_count = gp_command_count;
			task->id.is_writer = is_writer;
			task->id.init = true;
		}
		else
		{
			int		j;

			/* Assign the id from 0. */
			for (j = 0; j < data->job.slices[i].tasks_num; j++)
			{
				DispatchTask	*task = &data->job.slices[i].tasks[j];

				task->id.id_in_slice = j;
				task->id.slice_id = slice->sliceIndex;
				task->id.gang_member_num = list_length(data->resource->segments);
				task->id.command_count = gp_command_count;
				task->id.is_writer = is_writer;
				task->segment = list_nth(data->resource->segments, task->id.id_in_slice);
				task->id.init = true;
			}
		}
	}

	pfree(sliceVector);

	MemoryContextSwitchTo(old);
}

/*
 * dispatcher_split_logical_tasks_for_statemet_or_node
 */
static void
dispatcher_split_logical_tasks_for_statemet_or_node(DispatchData *data)
{
	MemoryContext	old;
	int 	i;
	int		segment_num, segment_num_entrydb;

	segment_num = data->segment_num;
	segment_num_entrydb = data->segment_num_on_entrydb;

	if ((segment_num == 0) && (segment_num_entrydb == 0))
	{
		return;
	}

	old = MemoryContextSwitchTo(DispatchDataContext);

	data->job.used_slices_num = segment_num_entrydb + 1;
	data->job.all_slices_num = segment_num_entrydb + 1;
	data->job.slices = palloc0(data->job.used_slices_num * sizeof(DispatchSlice));
  for (i = 0; i < segment_num_entrydb; i++)
  {
    /* TODO: where should I store the runtime context ?? */
    data->job.slices[i].tasks_num = 1;
    data->job.slices[i].tasks = palloc0(
        data->job.slices[i].tasks_num * sizeof(DispatchTask));
    DispatchTask *task = &data->job.slices[i].tasks[0];
    task->id.slice_id = i;
    task->id.id_in_slice = MASTER_CONTENT_ID;
    task->id.gang_member_num = 1;
    task->id.command_count = gp_command_count;
    task->id.is_writer = true;
    /* NULL means ANY segment */
    task->segment = NULL; // ?? How to get the master resource
    task->entrydb = true;
    task->id.init = true;
  }
  for (i = segment_num_entrydb; i < data->job.used_slices_num; i++)
  {
    /* TODO: where should I store the runtime context ?? */
    data->job.slices[i].tasks_num = segment_num;
    data->job.slices[i].tasks = palloc0(data->job.slices[i].tasks_num * sizeof(DispatchTask));
  }

  /* Set tasks id for segment. We have only one slice! */
  for (i = 0; i < data->job.slices[segment_num_entrydb].tasks_num; i++)
  {
    DispatchTask  *task = &data->job.slices[segment_num_entrydb].tasks[i];
    task->id.slice_id = segment_num_entrydb;
    task->id.id_in_slice = i;
    task->id.gang_member_num = segment_num;
    task->id.command_count = gp_command_count;
    task->id.is_writer = true;
    /* NULL means ANY segment */
    task->segment = data->dispatch_to_all_cached_executors ? NULL : list_nth(data->resource->segments, i);
    task->id.init = true;
  }
	data->pQueryParms->primary_gang_id = -1;

	MemoryContextSwitchTo(old);
}

/*
 * dispatcher_bind_executor
 *	Bind the dispatcher smallest dispatch data to executor.
 */
static bool
dispatcher_bind_executor(DispatchData *data)
{
	int		i,
			j;
	struct QueryExecutor	*executor;
	QueryExecutorIterator	iterator;
	List		*concurrent_connect_executors = NIL;
	ListCell	*lc;
	bool		ret;

	dispmgt_init_query_executor_iterator(data->query_executor_team, &iterator);
	for (i = 0; i < data->job.used_slices_num; i++)
	{
		DispatchSlice	*slice = &data->job.slices[i];

		for (j = 0; j < slice->tasks_num; j++)
		{
			DispatchTask	*task = &slice->tasks[j];
			struct SegmentDatabaseDescriptor *desc;

			executor = dispmgt_get_query_executor_iterator(&iterator, false);
			desc = executormgr_allocate_executor(task->segment, task->id.is_writer, task->entrydb);

			if (!desc)
			{
			  // may have problem for tasks assigned to entrydb for dispatch_statement_node
			  // and dispatch_statement_string
			  if (task->segment != NULL)
			  {
			    struct ConcurrentConnectExecutorInfo *info;

			    info = dispmgt_build_preconnect_info(task->segment,
                                               task->id.is_writer, executor,
                                               data, slice, task);
			    concurrent_connect_executors = lappend(concurrent_connect_executors,
                                                 info);
			    data->num_of_new_connected_executors++;
			    if (!executormgr_bind_executor_task(data, executor, info->desc, task, slice))
			      return false;
			  }
			  continue;
			}

#ifdef FAULT_INJECTOR
				FaultInjector_InjectFaultIfSet(
											   ConnectionFailAfterGangCreation,
											   DDLNotSpecified,
											   "",	// databaseName
											   ""); // tableName
#endif

			if (!executormgr_bind_executor_task(data, executor, desc, task, slice))
				return false;
			data->num_of_cached_executors++;
		}
	}

	if (data->dispatch_to_all_cached_executors)
		elog(DEBUG1, "dispatch to all cached executors: expected: %d actual: %d new connected(should be 0): %d",
			dispatcher_get_total_query_executors_num(data),
			data->num_of_cached_executors,
			data->num_of_new_connected_executors);

	if (concurrent_connect_executors == NIL)
		return true;

	ret = dispmgt_concurrent_connect(concurrent_connect_executors,
										gp_connections_per_thread);

	foreach(lc, concurrent_connect_executors)
		dispmgt_free_preconnect_info(lfirst(lc));
	list_free(concurrent_connect_executors);

	return ret;
}

/*
 * dispatcher_serialize_state
 */
static void
dispatcher_serialize_state(DispatchData *data)
{
	int		i,
			j;
	struct QueryExecutor	*executor;
	QueryExecutorIterator	iterator;
	char		*sliceInfo;
	int			sliceInfoLen;
	SliceTable	*sliceTable = data->queryDesc ? data->queryDesc->estate->es_sliceTable : NULL;

	/* Firstly, serialize every executor state. */
	dispmgt_init_query_executor_iterator(data->query_executor_team, &iterator);
	for (i = 0; i < data->job.used_slices_num; i++)
	{
		DispatchSlice	*slice = &data->job.slices[i];

		for (j = 0; j < slice->tasks_num; j++)
		{
			DispatchTask	*task = &slice->tasks[j];

			executor = dispmgt_get_query_executor_iterator(&iterator, true);
			if (executor == NULL)
			  continue;
 			executormgr_serialize_executor_state(data, executor, task, slice);

			if (sliceTable)
			{
				Slice			*plan_slice = list_nth(sliceTable->slices, task->id.slice_id);
				CdbProcess		*proc = makeNode(CdbProcess);//list_nth(plan_slice->primaryProcesses, get_task_id_in_slice(slice, task));

				/* Update the real executors information for interconnect. */
				executormgr_get_executor_connection_info(executor, &proc->listenerAddr, &proc->listenerPort, &proc->pid);
				proc->contentid = task->id.id_in_slice;
				plan_slice->primaryProcesses = lappend(plan_slice->primaryProcesses, proc);
			}
		}
	}

	/* Ok, we can serialize the global state. */
	if (sliceTable)
	{
		sliceInfo = serializeNode((Node *) data->queryDesc->estate->es_sliceTable, &sliceInfoLen, NULL /*uncompressed_size*/);
		data->pQueryParms->serializedSliceInfo = sliceInfo;
		data->pQueryParms->serializedSliceInfolen = sliceInfoLen;
	}
}

/*
 * dispatcher_serialize_query_resource
 */
static void
dispatcher_serialize_query_resource(DispatchData *data)
{
	char		*serializedQueryResource = NULL;
	int		serializedQueryResourcelen = 0;
	QueryResource * queryResource = data->resource;

	if (queryResource)
	{
		aggregateQueryResource(queryResource);
		serializedQueryResource = serializeNode(( Node *) queryResource, &serializedQueryResourcelen, NULL);
		data->pQueryParms->serializedQueryResource = serializedQueryResource;
		data->pQueryParms->serializedQueryResourcelen = serializedQueryResourcelen;

		if (queryResource->segment_vcore_agg)
		{
			pfree(queryResource->segment_vcore_agg);
		}
		if (queryResource->segment_vcore_writer)
		{
		  pfree(queryResource->segment_vcore_writer);
		}
	}
}

/*
 * dispatcher_unbind_executor
 */
static void
dispatcher_unbind_executor(DispatchData *data)
{
	int 	i,
			j;
	struct QueryExecutor	*executor;
	QueryExecutorIterator	iterator;

	dispmgt_init_query_executor_iterator(data->query_executor_team, &iterator);
	for (i = 0; i < data->job.used_slices_num; i++)
	{
		DispatchSlice	*slice = &data->job.slices[i];

		for (j = 0; j < slice->tasks_num; j++)
		{
			DispatchTask	*task = &slice->tasks[j];

			executor = dispmgt_get_query_executor_iterator(&iterator, true);
			if (executor == NULL)
			  continue;
			executormgr_unbind_executor_task(data, executor, task, slice);
		}
	}
}

static void
dispatcher_update_statistics_per_executor(DispatchData *data, struct QueryExecutor *executor)
{
	instr_time		time_connect_begin;
	instr_time		time_connect_end;
	instr_time		time_connect_diff;
	instr_time		time_dispatch_begin;
	instr_time		time_dispatch_end;
	instr_time		time_dispatch_diff;
  instr_time    time_consume_begin;
  instr_time    time_consume_end;
  instr_time    time_consume_diff;
  instr_time    time_free_begin;
  instr_time    time_free_end;
  instr_time    time_free_diff;
	bool			first_time = (data->num_of_dispatched == 0);

	executormgr_get_statistics(executor, &time_connect_begin, &time_connect_end,
	                           &time_dispatch_begin, &time_dispatch_end,
	                           &time_consume_begin, &time_consume_end,
	                           &time_free_begin, &time_free_end);
	INSTR_TIME_SET_ZERO(time_connect_diff);
	INSTR_TIME_SET_ZERO(time_dispatch_diff);
  INSTR_TIME_SET_ZERO(time_consume_diff);
  INSTR_TIME_SET_ZERO(time_free_diff);
	INSTR_TIME_ACCUM_DIFF(time_connect_diff, time_connect_end, time_connect_begin);
	INSTR_TIME_ACCUM_DIFF(time_dispatch_diff, time_dispatch_end, time_dispatch_begin);
  INSTR_TIME_ACCUM_DIFF(time_consume_diff, time_consume_end, time_consume_begin);
  INSTR_TIME_ACCUM_DIFF(time_free_diff, time_free_end, time_free_begin);

	data->num_of_dispatched++;
	if (first_time)
	{
		/* dispatcher side */
		INSTR_TIME_ASSIGN(data->time_first_connect_begin, time_connect_begin);
		INSTR_TIME_ASSIGN(data->time_last_connect_end, time_connect_end);
		INSTR_TIME_ASSIGN(data->time_first_dispatch_begin, time_dispatch_begin);
		INSTR_TIME_ASSIGN(data->time_last_dispatch_end, time_dispatch_end);

		/* executor side */
		INSTR_TIME_ACCUM_DIFF(data->time_total_dispatched, time_dispatch_end, time_dispatch_begin);
		INSTR_TIME_ACCUM_DIFF(data->time_max_dispatched, time_dispatch_end, time_dispatch_begin);
		INSTR_TIME_ACCUM_DIFF(data->time_min_dispatched, time_dispatch_end, time_dispatch_begin);
    INSTR_TIME_ACCUM_DIFF(data->time_total_consumed, time_consume_end, time_consume_begin);
    INSTR_TIME_ACCUM_DIFF(data->time_max_consumed, time_consume_end, time_consume_begin);
    INSTR_TIME_ACCUM_DIFF(data->time_min_consumed, time_consume_end, time_consume_begin);
    INSTR_TIME_ACCUM_DIFF(data->time_total_free, time_free_end, time_free_begin);
    INSTR_TIME_ACCUM_DIFF(data->time_max_free, time_free_end, time_free_begin);
    INSTR_TIME_ACCUM_DIFF(data->time_min_free, time_free_end, time_free_begin);
		return;
	}

	/* dispatch side */
	if (data->num_of_new_connected_executors > 0)
	{
		if (INSTR_TIME_GREATER_THAN(data->time_first_connect_begin, time_connect_begin))
			INSTR_TIME_ASSIGN(data->time_first_connect_begin, time_connect_begin);
		if (INSTR_TIME_LESS_THAN(data->time_last_connect_end, time_connect_end))
			INSTR_TIME_ASSIGN(data->time_last_connect_end, time_connect_end);
	}
	if (INSTR_TIME_GREATER_THAN(data->time_first_dispatch_begin, time_dispatch_begin))
		INSTR_TIME_ASSIGN(data->time_first_dispatch_begin, time_dispatch_begin);
	if (INSTR_TIME_LESS_THAN(data->time_last_dispatch_end, time_dispatch_end))
		INSTR_TIME_ASSIGN(data->time_last_dispatch_end, time_dispatch_end);

	/* executor side */
	if (INSTR_TIME_LESS_THAN(data->time_max_dispatched, time_dispatch_diff))
		INSTR_TIME_ASSIGN(data->time_max_dispatched, time_dispatch_diff);
	if (INSTR_TIME_GREATER_THAN(data->time_min_dispatched, time_dispatch_diff))
		INSTR_TIME_ASSIGN(data->time_min_dispatched, time_dispatch_diff);
  if (INSTR_TIME_LESS_THAN(data->time_max_consumed, time_consume_diff))
    INSTR_TIME_ASSIGN(data->time_max_consumed, time_consume_diff);
  if (INSTR_TIME_GREATER_THAN(data->time_min_consumed, time_consume_diff))
    INSTR_TIME_ASSIGN(data->time_min_consumed, time_consume_diff);
  if (INSTR_TIME_LESS_THAN(data->time_max_free, time_free_diff))
    INSTR_TIME_ASSIGN(data->time_max_free, time_free_diff);
  if (INSTR_TIME_GREATER_THAN(data->time_min_free, time_free_diff))
    INSTR_TIME_ASSIGN(data->time_min_free, time_free_diff);


	INSTR_TIME_ADD(data->time_total_dispatched, time_dispatch_diff);
  INSTR_TIME_ADD(data->time_total_consumed, time_consume_diff);
  INSTR_TIME_ADD(data->time_total_free, time_free_diff);
}

static void
dispatcher_update_statistics(DispatchData *data)
{
	int 	i,
			j;
	struct QueryExecutor	*executor;
	QueryExecutorIterator	iterator;

	dispmgt_init_query_executor_iterator(data->query_executor_team, &iterator);
	for (i = 0; i < data->job.used_slices_num; i++)
	{
		DispatchSlice	*slice = &data->job.slices[i];

		for (j = 0; j < slice->tasks_num; j++)
		{
			executor = dispmgt_get_query_executor_iterator(&iterator, true);
			if (executor == NULL)
			  continue;
			dispatcher_update_statistics_per_executor(data, executor);
		}
	}

	INSTR_TIME_ACCUM_DIFF(data->time_total, data->time_last_dispatch_end, data->time_begin);
	INSTR_TIME_ACCUM_DIFF(data->time_connect, data->time_last_connect_end, data->time_first_connect_begin);
	INSTR_TIME_ACCUM_DIFF(data->time_dispatch, data->time_last_dispatch_end, data->time_first_dispatch_begin);
}

/*
 * dispatch_plan
 *	Create corresponding dispatching data structure. It requires caller splits
 *	plan. This function will compute the necessary threads number and dispatch
 *	plan to each executor.
 */
void
dispatch_run(DispatchData *data)
{
  if (Debug_print_execution_detail) {
    instr_time  time;
    INSTR_TIME_SET_CURRENT(time);
    elog(DEBUG1,"The time on entering dispatch_run: %.3f ms",
                     1000.0 * INSTR_TIME_GET_DOUBLE(time));
  }

	if ((data->segment_num == 0) && (data->segment_num_on_entrydb == 0))
	{
		data->state = DS_DONE;
		return;
	}

	dispatcher_init_works(data);
	INSTR_TIME_SET_CURRENT(data->time_begin);
	if (!dispatcher_bind_executor(data))
		goto error;

#ifdef FAULT_INJECTOR
				FaultInjector_InjectFaultIfSet(
											   FailQeAfterConnection,
											   DDLNotSpecified,
											   "",	// databaseName
											   ""); // tableName
#endif

	/*
	 * Only after we have the executors, we can serialize the state. Or we
	 * don't know the executor listening address.
	 */
	CHECK_FOR_INTERRUPTS();

	dispatcher_serialize_state(data);
	dispatcher_serialize_query_resource(data);
	dispatcher_set_state_run(data);
	dispmgt_dispatch_and_run(data->worker_mgr_state, data->query_executor_team);

  if (Debug_print_execution_detail) {
    instr_time  time;
    INSTR_TIME_SET_CURRENT(time);
    elog(DEBUG1,"The time after dispatching all the results: %.3f ms",
                     1000.0 * INSTR_TIME_GET_DOUBLE(time));
  }
	return;

error:
	dispatcher_set_state_error(data);
	elog(ERROR, "dispatcher encounter error");
	return;
}

/*
 * dispatch_wait
 *	Wait the workermgr return and setup error informaton returned from executors.
 */
void
dispatch_wait(DispatchData *data)
{
  if (Debug_print_execution_detail) {
    instr_time  time;
    INSTR_TIME_SET_CURRENT(time);
    elog(DEBUG1,"The time on entering dispatch_wait: %.3f ms",
                     1000.0 * INSTR_TIME_GET_DOUBLE(time));
  }
	if (dispatcher_is_state_ok(data) ||
		dispatcher_is_state_error(data) ||
		dispatcher_is_state_done(data))
		return;

	workermgr_wait_job(data->worker_mgr_state);
	CHECK_FOR_INTERRUPTS();

	/* Check executors state before return. */
	if (dispatch_collect_executors_error(data))
		dispatcher_set_state_error(data);
	else
		dispatcher_set_state_ok(data);

  if (Debug_print_execution_detail) {
    instr_time  time;
    INSTR_TIME_SET_CURRENT(time);
    elog(DEBUG1,"The time after collecting all the executor results: %.3f ms",
                     1000.0 * INSTR_TIME_GET_DOUBLE(time));
  }
}

/*
 * free_dispatch_data
 *	free the DispatchData allocated resource
 */
void free_dispatch_data(struct DispatchData *data) {
	if (data->pQueryParms) {
		pfree(data->pQueryParms);
		data->pQueryParms = NULL;
	}
	if (data->job.slices) {
		for (int i = 0; i < data->job.used_slices_num; ++i) {
			if (data->job.slices[i].tasks) {
				pfree(data->job.slices[i].tasks);
				data->job.slices[i].tasks = NULL;
			}
		}
		pfree(data->job.slices);
		data->job.slices = NULL;
	}
	if (data->query_executor_team) {
		QueryExecutorGroup *qeGrp = data->query_executor_team->query_executor_groups;
		if (qeGrp) {
			if (qeGrp->fds) {
				pfree(qeGrp->fds);
				qeGrp->fds = NULL;
			}
			for (int j = 0; j < qeGrp->query_executor_num; ++j) {
				pfree(qeGrp->query_executors[j]);
				qeGrp->query_executors[j] = NULL;
			}
		}
		pfree(data->query_executor_team);
		data->query_executor_team = NULL;
	}
	pfree(data);
}

/*
 * dispatch_end_env
 * accompanied with dispatch_init_env
 */
void dispatch_end_env(struct DispatchData *data) {
	Assert(data != NULL);

	--DispatchInitCount;

	cdbdisp_destroyDispatchResults(data->results);
	data->results = NULL;
	dispatcher_set_state_done(data);

	PG_TRY();
	{
		dispatcher_unbind_executor(data);
		if (data->resource_is_mine)
		{
			FreeResource(data->resource);
			data->resource_is_mine = false;
		}
	}
	PG_CATCH();
	{
		free_dispatch_data(data);
		PG_RE_THROW();
	}
	PG_END_TRY();
	free_dispatch_data(data);
}

/* 
 * dispatch_cleanup
 *	Cleanup the workermgr. Report error if something happended on executors.
 */
void
dispatch_cleanup(DispatchData *data)
{
	/* to avoid duplicate dispatch cleanup */
	if (data == NULL) return;
	if (dispatcher_is_state_done(data)) return;

	/*
	 * We should not get here when dispatcher hit an exception. But
	 * executors may have some troubles.
	 */
	if (dispatcher_is_state_error(data))
	{
		/* We cannot unbind executors until we retrieve error message! */
		dispatch_throw_error(data);
		Assert(false);
		return;	/* should not hit */
	}
	
	dispatch_end_env(data);
}

/*
 * dispatch_catch_error
 *	Cancel the workermgr work and report error based on bug on dispatcher or
 *	executors.
 */
void
dispatch_catch_error(DispatchData *data)
{
	int     qderrcode = elog_geterrcode();
	bool	useQeError = false;

	/* Init state means no thread and data is not correct! */
	if (!dispatcher_is_state_init(data))
	{
		workermgr_cancel_job(data->worker_mgr_state);

		/* Have to figure out exception source */
		dispatch_collect_executors_error(data);
	}

	/*
	 * When a QE stops executing a command due to an error, as a
	 * consequence there can be a cascade of interconnect errors
	 * (usually "sender closed connection prematurely") thrown in
	 * downstream processes (QEs and QD).  So if we are handling
	 * an interconnect error, and a QE hit a more interesting error,
	 * we'll let the QE's error report take precedence.
	 */
	if (qderrcode == ERRCODE_GP_INTERCONNECTION_ERROR)
	{
		bool	qd_lost_flag = false;
		char	*qderrtext = elog_message();

		if (qderrtext && strcmp(qderrtext, CDB_MOTION_LOST_CONTACT_STRING) == 0)
			qd_lost_flag = true;

		if (data->results && data->results->errcode)
		{
			if (qd_lost_flag && data->results->errcode == ERRCODE_GP_INTERCONNECTION_ERROR)
				useQeError = true;
			else if (data->results->errcode != ERRCODE_GP_INTERCONNECTION_ERROR)
				useQeError = true;
		}
	}

	if (useQeError)
	{
		/*
		 * Throw the QE's error, catch it, and fall thru to return
		 * normally so caller can finish cleaning up.  Afterwards
		 * caller must exit via PG_RE_THROW().
		 */
		PG_TRY();
		{
			dispatch_throw_error(data);
		}
		PG_CATCH();
		{}						/* nop; fall thru */
		PG_END_TRY();
	}

	dispatch_end_env(data);
}

void
cleanup_dispatch_data(DispatchData *data)
{
	if (data->queryDesc)
		data->queryDesc->estate->es_sliceTable->localSlice = data->localSliceId;
}

static void
dispatch_statement(DispatchStatement *stmt, QueryResource *resource, DispatchDataResult *result)
{
	volatile DispatchData	*data;

	data = initialize_dispatch_data(resource, stmt->dispatch_to_all_cached_executor);
	if (stmt->type == DST_STRING)
		prepare_dispatch_statement_string((DispatchData *) data,
										stmt->string,
										stmt->serializeQuerytree,
										stmt->serializeLenQuerytree);
	else
		prepare_dispatch_statement_node((DispatchData *) data,
										stmt->node,
										stmt->ctxt);

	PG_TRY();
	{
		dispatch_run((DispatchData *) data);
		dispatch_wait((DispatchData *) data);
		if (dispatcher_is_state_ok(data) && result)
		{
			int		segment_num = data->segment_num + data->segment_num_on_entrydb;

			if (segment_num > 0)
			{
				initStringInfo(&result->errbuf);

				result->result = cdbdisp_returnResults(segment_num, data->results, &result->errbuf, &result->numresults);
				/* cdbdisp_returnResults free the pointer ! */
				data->results = NULL;

				/* Takeover the list of segment connections. */
				result->segment_conns = dispmgt_takeover_segment_conns(data->query_executor_team);
			}
		}
		dispatch_cleanup((DispatchData *) data);
	}
	PG_CATCH();
	{
		dispatch_wait((DispatchData *) data);
		dispatch_cleanup((DispatchData *) data);
		PG_RE_THROW();
	}
	PG_END_TRY();
}

void
dispatch_statement_string(const char *string,
						const char *serializeQuerytree,
						int serializeLenQuerytree,
						QueryResource *resource,
						DispatchDataResult *result,
						bool sync_on_all_executors)
{
	DispatchStatement	stmt;

	MemSet(&stmt, 0, sizeof(stmt));
	stmt.type = DST_STRING;
	stmt.string = string;
	stmt.serializeQuerytree = serializeQuerytree;
	stmt.serializeLenQuerytree = serializeLenQuerytree;
	stmt.dispatch_to_all_cached_executor = sync_on_all_executors;

	dispatch_statement(&stmt, resource, result);
}

/*
 * dispatch_statement_node
 */
void
dispatch_statement_node(struct Node *node,
						struct QueryContextInfo *ctxt,
						QueryResource *resource,
						DispatchDataResult *result)
{
	DispatchStatement	stmt;

	Assert(resource);
	MemSet(&stmt, 0, sizeof(stmt));
	stmt.type = DST_NODE;
	stmt.node = node;
	stmt.ctxt = ctxt;
	stmt.dispatch_to_all_cached_executor = false;

	dispatch_statement(&stmt, resource, result);
}

void
dispatch_free_result(DispatchDataResult *result)
{
	if (!result)
		return;

	if(result->errbuf.data)
	{
		pfree(result->errbuf.data);
		result->errbuf.data = NULL;
	}

	if (result->result)
	{
	  for (int i = 0; i <= result->numresults; i++){
	    PQclear(result->result[i]);
	    result->result[i] = NULL;
	  }
	  free(result->result);
	}

	dispmgt_free_takeoved_segment_conns(result->segment_conns);
}

struct CdbDispatchResults *
dispatch_get_results(struct DispatchData *data)
{
	return data->results;
}

int
dispatch_get_segment_num(struct DispatchData *data)
{
	return list_length(data->resource->segments);
}

/*
 *
 */
static DispatchCommandQueryParms *
dispatcher_fill_query_param(const char *strCommand,
							char *serializeQuerytree,
							int serializeLenQuerytree,
							char *serializePlantree,
							int serializeLenPlantree,
							char *serializeParams,
							int serializeLenParams,
							char *serializeSliceInfo,
							int serializeLenSliceInfo,
							int rootIdx,
							QueryResource *resource)
{
	DispatchCommandQueryParms	*queryParms;
	Segment		*master = NULL;
	MemoryContext	old;

	if (resource)
	{
	  master = resource->master;
	}

	old = MemoryContextSwitchTo(DispatchDataContext);
	queryParms = palloc0(sizeof(*queryParms));
	MemoryContextSwitchTo(old);

	queryParms->strCommand = strCommand;
	queryParms->strCommandlen = strCommand ? strlen(strCommand) + 1 : 0;
	queryParms->serializedQuerytree = serializeQuerytree;
	queryParms->serializedQuerytreelen = serializeLenQuerytree;
	queryParms->serializedPlantree = serializePlantree;
	queryParms->serializedPlantreelen = serializeLenPlantree;
	queryParms->serializedParams = serializeParams;
	queryParms->serializedParamslen = serializeLenParams;
	queryParms->serializedSliceInfo = serializeSliceInfo;
	queryParms->serializedSliceInfolen= serializeLenSliceInfo;
	queryParms->rootIdx = rootIdx;

	/* sequence server info */
	if (master)
	{
	  queryParms->seqServerHost = pstrdup(master->hostip);
	  queryParms->seqServerHostlen = strlen(master->hostip) + 1;
	}
	queryParms->seqServerPort = seqServerCtl->seqServerPort;

	queryParms->primary_gang_id = 0;	/* We are relying on the slice table to provide gang ids */

	/*
	 * in hawq, there is no distributed transaction
	 */
	queryParms->serializedDtxContextInfo = NULL;
	queryParms->serializedDtxContextInfolen = 0;

	return queryParms;
}

/*
 * dispatcher_init_works
 *	This function is create all of data structures.
 */
static bool
dispatcher_init_works(DispatchData *data)
{
	MemoryContext	old;

	old = MemoryContextSwitchTo(DispatchDataContext);
	/* Compute the threads number &  */
	dispatcher_compute_threads_num(data);
	/* Create the threads data strucutre. */
	data->worker_mgr_state = workermgr_create_workermgr_state(dispmgt_get_group_num(data->query_executor_team));
	MemoryContextSwitchTo(old);

	return true;
}

static bool
dispatcher_compute_threads_num(DispatchData *data)
{
	int		threads_num = 1;
	int		query_executors_num;
	int		executors_num_per_thread = gp_connections_per_thread;

	query_executors_num = dispatcher_get_total_query_executors_num(data);
	data->results = cdbdisp_makeDispatchResults(query_executors_num, data->job.all_slices_num, false);

	/* TODO: decide the groups(threads) number. */
	if (query_executors_num == 0)
		threads_num = 1;
	if (executors_num_per_thread > query_executors_num)
		threads_num = 1;
	else
		threads_num = (query_executors_num / executors_num_per_thread) + ((query_executors_num % executors_num_per_thread == 0) ? 0 : 1); 

	data->query_executor_team = dispmgt_create_dispmgt_state(data,
									threads_num,
									query_executors_num,
									executors_num_per_thread);

	return true;
}

DispatchCommandQueryParms *
dispatcher_get_QueryParms(DispatchData *data)
{
	return data->pQueryParms;
}

static int
dispatcher_get_total_query_executors_num(DispatchData *data)
{
	int		num = 0;
	int		i;

	for (i = 0; i < data->job.used_slices_num; i++)
		num += data->job.slices[i].tasks_num;

	return num;
}

ProcessIdentity *
dispatch_get_task_identity(DispatchTask *task)
{
	return &task->id;
}

static bool
dispatch_collect_executors_error(DispatchData *data)
{
	MemoryContext	old;
	old = MemoryContextSwitchTo(DispatchDataContext);

	QueryExecutorIterator	iterator;
	struct QueryExecutor	*executor;
	bool ret = false;

	// the host list of error executors
	int errHostNum = 0;
	int errHostSize = ERROR_HOST_INIT_SIZE;
	char **errHostName = (char**) palloc0(errHostSize * sizeof(char *));

	dispmgt_init_query_executor_iterator(data->query_executor_team, &iterator);
	while ((executor = dispmgt_get_query_executor_iterator(&iterator, true)) != NULL)
	{
		executormgr_merge_error_for_dispatcher(executor, &errHostSize,
		                                       &errHostNum, &errHostName);
	}

	if (dispatcher_has_error(data)) {
	  if (errHostNum > 0)
	    sendFailedNodeToResourceManager(errHostNum, errHostName);
	  ret = true;
	}
	for (int i = 0; i < errHostNum; i++) {
	  pfree(errHostName[i]);
	}
	pfree(errHostName);

	MemoryContextSwitchTo(old);

	return ret;
}

static void
dispatch_throw_error(DispatchData *data)
{
	StringInfoData	buf;
	int		errorcode = data->results->errcode;

	/* Format error messages from the primary gang. */
	initStringInfo(&buf);
	cdbdisp_dumpDispatchResults(data->results, &buf, false);

	/* Too bad, our gang got an error. */
	PG_TRY();
	{
		ereport(ERROR, (errcode(errorcode),
						errOmitLocation(true),
						errmsg("%s", buf.data)));
	}
	PG_CATCH();
	{
		pfree(buf.data);
		PG_RE_THROW();
	}
	PG_END_TRY();
}

bool
dispatcher_has_error(DispatchData *data)
{
	return data->results && data->results->errcode;
}

void
dispatcher_print_statistics(StringInfo buf, DispatchData *data)
{
	if (!data)
		return;
	dispatcher_update_statistics(data);
	if (data->num_of_dispatched == 0)
		return;
	appendStringInfo(buf, "Dispatcher statistics:\n");
	appendStringInfo(buf,
			"  executors used(total/cached/new connection): (%d/%d/%d);",
			data->num_of_dispatched, data->num_of_cached_executors,
			data->num_of_new_connected_executors);
	appendStringInfo(buf,
			" dispatcher time(total/connection/dispatch data): (%.3f ms/%.3f ms/%.3f ms).\n",
			INSTR_TIME_GET_MILLISEC(data->time_total),
			INSTR_TIME_GET_MILLISEC(data->time_connect),
			INSTR_TIME_GET_MILLISEC(data->time_dispatch));
	appendStringInfo(buf,
			"  dispatch data time(max/min/avg): (%.3f ms/%.3f ms/%.3f ms);",
			INSTR_TIME_GET_MILLISEC(data->time_max_dispatched),
			INSTR_TIME_GET_MILLISEC(data->time_min_dispatched),
			INSTR_TIME_GET_MILLISEC(data->time_total_dispatched)
					/ data->num_of_dispatched);
	appendStringInfo(buf,
			" consume executor data time(max/min/avg): (%.3f ms/%.3f ms/%.3f ms);",
			INSTR_TIME_GET_MILLISEC(data->time_max_consumed),
			INSTR_TIME_GET_MILLISEC(data->time_min_consumed),
			INSTR_TIME_GET_MILLISEC(data->time_total_consumed)
					/ data->num_of_dispatched);
	appendStringInfo(buf,
			" free executor time(max/min/avg): (%.3f ms/%.3f ms/%.3f ms).\n",
			INSTR_TIME_GET_MILLISEC(data->time_max_free),
			INSTR_TIME_GET_MILLISEC(data->time_min_free),
			INSTR_TIME_GET_MILLISEC(data->time_total_free) / data->num_of_dispatched);
}



/*
 * Check the connection from the dispatcher to verify that it is still there.
 * Return true if the dispatcher connection is still alive.
 */
bool dispatch_validate_conn(pgsocket sock)
{
  ssize_t   ret;
  char    buf;

  if (sock < 0)
    return false;

#ifndef WIN32
    ret = recv(sock, &buf, 1, MSG_PEEK|MSG_DONTWAIT);
#else
    ret = recv(sock, &buf, 1, MSG_PEEK|MSG_PARTIAL);
#endif

  if (ret == 0) /* socket has been closed. EOF */
    return false;

  if (ret > 0) /* data waiting on socket */
  {
    if (buf == 'E') /* waiting data indicates error */
      return false;
    else
      return true;
  }
  if (ret == -1) /* error, or would be block. */
  {
    if (errno == EAGAIN || errno == EINPROGRESS || errno == EWOULDBLOCK)
      return true; /* connection intact, no data available */
    else
      return false;
  }
  /* not reached */

  return true;
}
