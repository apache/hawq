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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/*-------------------------------------------------------------------------
 *
 * execMain.c
 *	  top level executor interface routines
 *
 * INTERFACE ROUTINES
 *	ExecutorStart()
 *	ExecutorRun()
 *	ExecutorEnd()
 *
 *	The old ExecutorMain() has been replaced by ExecutorStart(),
 *	ExecutorRun() and ExecutorEnd()
 *
 *	These three procedures are the external interfaces to the executor.
 *	In each case, the query descriptor is required as an argument.
 *
 *	ExecutorStart() must be called at the beginning of execution of any
 *	query plan and ExecutorEnd() should always be called at the end of
 *	execution of a plan.
 *
 *	ExecutorRun accepts direction and count arguments that specify whether
 *	the plan is to be executed forwards, backwards, and for how many tuples.
 *
 * Portions Copyright (c) 2005-2010, Greenplum inc
 * Portions Copyright (c) 1996-2009, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  $PostgreSQL: pgsql/src/backend/executor/execMain.c,v 1.280.2.2 2007/02/02 00:07:27 tgl Exp $
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"
#include "gpmon/gpmon.h"

#include "access/heapam.h"
#include "access/aosegfiles.h"
#include "access/parquetsegfiles.h"
#include "access/appendonlywriter.h"
#include "access/fileam.h"
#include "access/filesplit.h"
#include "access/reloptions.h"
#include "access/transam.h"
#include "access/xact.h"
#include "catalog/heap.h"
#include "catalog/namespace.h"
#include "catalog/toasting.h"
#include "catalog/aoseg.h"
#include "catalog/catalog.h"
#include "catalog/pg_attribute_encoding.h"
#include "catalog/pg_type.h"
#include "cdb/cdbpartition.h"
#include "cdb/cdbmirroredfilesysobj.h"
#include "commands/dbcommands.h"
#include "commands/tablecmds.h" /* XXX: temp for get_parts() */
#include "commands/tablespace.h"
#include "commands/trigger.h"
#include "executor/execDML.h"
#include "executor/execdebug.h"
#include "executor/instrument.h"
#include "executor/nodeSubplan.h"
#include "libpq/pqformat.h"
#include "miscadmin.h"
#include "nodes/makefuncs.h" /* temporary */
#include "nodes/pg_list.h"
#include "optimizer/clauses.h"
#include "parser/parse_clause.h"
#include "parser/parse_expr.h"
#include "parser/parse_relation.h"
#include "parser/parsetree.h"
#include "storage/smgr.h"
#include "utils/acl.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/ps_status.h"
#include "utils/rel.h"
#include "utils/workfile_mgr.h"

#include "catalog/pg_statistic.h"
#include "catalog/pg_class.h"

#include "tcop/tcopprot.h"

#include "cdb/cdbappendonlyam.h"
#include "cdb/cdbparquetam.h"
#include "cdb/cdbcat.h"
#include "cdb/cdbdisp.h"
#include "cdb/cdbdispatchresult.h"
#include "cdb/dispatcher.h"
#include "cdb/cdbexplain.h"             /* cdbexplain_sendExecStats() */
#include "cdb/cdbplan.h"
#include "cdb/cdbsrlz.h"
#include "cdb/cdbsubplan.h"
#include "cdb/cdbvars.h"
#include "cdb/ml_ipc.h"
#include "cdb/cdbmotion.h"
#include "cdb/cdbgang.h"
#include "cdb/cdboidsync.h"
#include "cdb/cdbmirroredbufferpool.h"
#include "cdb/cdbpersistentstore.h"
#include "cdb/cdbpersistentfilesysobj.h"
#include "cdb/cdbllize.h"
#include "cdb/memquota.h"
#include "cdb/cdbsharedstorageop.h"
#include "cdb/cdbtargeteddispatch.h"
#include "cdb/cdbquerycontextdispatching.h"
#include "optimizer/prep.h"

#include "resourcemanager/dynrm.h"

extern bool		filesystem_support_truncate;

typedef struct evalPlanQual
{
	Index		rti;
	EState	   *estate;
	PlanState  *planstate;
	struct evalPlanQual *next;	/* stack of active PlanQual plans */
	struct evalPlanQual *free;	/* list of free PlanQual plans */
} evalPlanQual;

/* decls for local routines only used within this module */
static void InitPlan(QueryDesc *queryDesc, int eflags);
static void initResultRelInfo(ResultRelInfo *resultRelInfo,
		Index resultRelationIndex,
		List *rangeTable,
		CmdType operation,
		bool doInstrument,
		bool needLock);
static void ExecCheckPlanOutput(Relation resultRel, List *targetList);
static TupleTableSlot *ExecutePlan(EState *estate, PlanState *planstate,
		CmdType operation,
		long numberTuples,
		ScanDirection direction,
		DestReceiver *dest);
static void ExecSelect(TupleTableSlot *slot,
		DestReceiver *dest,
		EState *estate);
static TupleTableSlot *EvalPlanQualNext(EState *estate);
static void EndEvalPlanQual(EState *estate);
static void ExecCheckXactReadOnly(PlannedStmt *plannedstmt);
static void EvalPlanQualStart(evalPlanQual *epq, EState *estate,
		evalPlanQual *priorepq);
static void EvalPlanQualStop(evalPlanQual *epq);
static void OpenIntoRel(QueryDesc *queryDesc);
static void CloseIntoRel(QueryDesc *queryDesc);
static void intorel_startup(DestReceiver *self, int operation, TupleDesc typeinfo);
static void intorel_receive(TupleTableSlot *slot, DestReceiver *self);
static void intorel_shutdown(DestReceiver *self);
static void intorel_destroy(DestReceiver *self);
static void ClearPartitionState(EState *estate);
static void findPartTableOidsToDispatch(Plan *plan, List **partTables, List **oidLists);

typedef struct CopyDirectDispatchToSliceContext
{
	plan_tree_base_prefix	base; /* Required prefix for plan_tree_walker/mutator */
	EState					*estate; /* EState instance */
} CopyDirectDispatchToSliceContext;

static bool CopyDirectDispatchFromPlanToSliceTableWalker( Node *node, CopyDirectDispatchToSliceContext *context);

static void
CopyDirectDispatchToSlice( Plan *ddPlan, int sliceId, CopyDirectDispatchToSliceContext *context)
{
	EState	*estate = context->estate;
	Slice *slice = (Slice *)list_nth(estate->es_sliceTable->slices, sliceId);

	Assert( ! slice->directDispatch.isDirectDispatch );	/* should not have been set by some other process */
	Assert(ddPlan != NULL);

	if ( ddPlan->directDispatch.isDirectDispatch)
	{
		slice->directDispatch.isDirectDispatch = true;
		slice->directDispatch.contentIds = list_copy(ddPlan->directDispatch.contentIds);
	}
}

static bool
CopyDirectDispatchFromPlanToSliceTableWalker( Node *node, CopyDirectDispatchToSliceContext *context)
{
	int sliceId = -1;
	Plan *ddPlan = NULL;

	if (node == NULL)
		return false;

	if (IsA(node, Motion))
	{
		Motion *motion = (Motion *) node;

		ddPlan = (Plan*)node;
		sliceId = motion->motionID;
	}

	if (ddPlan != NULL)
	{
		CopyDirectDispatchToSlice(ddPlan, sliceId, context);
	}
	return plan_tree_walker(node, CopyDirectDispatchFromPlanToSliceTableWalker, context);
}

static void
CopyDirectDispatchFromPlanToSliceTable(PlannedStmt *stmt, EState *estate)
{
	CopyDirectDispatchToSliceContext context;
	exec_init_plan_tree_base(&context.base, stmt);
	context.estate = estate;
	CopyDirectDispatchToSlice( stmt->planTree, 0, &context);
	CopyDirectDispatchFromPlanToSliceTableWalker((Node *) stmt->planTree, &context);
}


typedef struct QueryCxtWalkerCxt {
	plan_tree_base_prefix	base;
	QueryContextInfo	   *info;
} QueryCxtWalkerCxt;

/**
 * SetupSegnoForErrorTable
 * 		travel the query plan to find out the external table scan, assign segfile for error table if exist.
 */
static bool
SetupSegnoForErrorTable(Node *node, QueryCxtWalkerCxt *cxt)
{
	ExternalScan   *scan = (ExternalScan *)node;
	QueryContextInfo *info = cxt->info;
	List *errSegnos;

	if (NULL == node)
		return false;

	switch (nodeTag(node))
	{
		case T_ExternalScan:
			/*
			 * has no error table
			 */
			if (!OidIsValid(scan->fmterrtbl))
				return false;

			/*
			 * check if two external table use the same error table in a statement
			 */
			if (info->errTblOid)
			{
				ListCell	   *c;
				Oid				errtbloid;
				foreach(c, info->errTblOid)
				{
					errtbloid = lfirst_oid(c);
					if (errtbloid == scan->fmterrtbl)
					{
						Relation rel = heap_open(scan->fmterrtbl, AccessShareLock);
						elog(ERROR, "Two or more external tables use the same error table \"%s\" in a statement",
								RelationGetRelationName(rel));
					}
				}
			}

            /*
             * Prepare error table for insert.
             */
            Assert(!rel_is_partitioned(scan->fmterrtbl));
            errSegnos = SetSegnoForWrite(NIL, scan->fmterrtbl, GetQEGangNum(), false, true);
            scan->errAosegnos = errSegnos;
            info->errTblOid = lcons_oid(scan->fmterrtbl, info->errTblOid);

            Relation errRel = heap_open(scan->fmterrtbl, RowExclusiveLock);
            CreateAppendOnlyParquetSegFileForRelationOnMaster(errRel, errSegnos);
            prepareDispatchedCatalogSingleRelation(info, scan->fmterrtbl, TRUE, errSegnos);
            scan->err_aosegfileinfos = fetchSegFileInfos(scan->fmterrtbl, errSegnos);

            heap_close(errRel, RowExclusiveLock);

            return false;
		default:
			break;

	}

	return plan_tree_walker(node, SetupSegnoForErrorTable, cxt);
}

/*
 * findPartTableOidsToDispatch
 *    find all partitioned tables for which static selection has been done.
 *    "partTables" will have the oids of these tables
 *    "oidLists" will have the corresponding lists of selected child oids for each table
 */
static void
findPartTableOidsToDispatch(Plan *plan, List **partTables, List **oidLists)
{
	Assert (NULL != plan);
	Assert (NULL != partTables);
	Assert (NIL == *partTables);
	Assert (NULL != oidLists);
	Assert (NIL == *oidLists);

	List *partitionSelectors = extract_nodes_plan(plan, T_PartitionSelector, true /*descendIntoSubqueries*/);

	// list of partitioned tables for which we have to ship metadata for all parts
	List *fullScanTables = NIL;

	ListCell *psc = NULL;
	foreach (psc, partitionSelectors)
	{
		PartitionSelector *ps = (PartitionSelector *) lfirst(psc);
		bool allPartsSelected = list_member_oid(fullScanTables, ps->relid);
		int idx = list_find_oid(*partTables, ps->relid);
		if (0 <= idx)
		{
			// table has been encountered before and it did not need all parts
			Assert(!allPartsSelected);
			List *partOids = list_nth(*oidLists, idx);
			if (ps->staticSelection)
			{
				// union the parts we needed before and the parts we need now
				List *newList = list_union_oid(partOids, ps->staticPartOids);
				list_nth_replace(*oidLists, idx, newList);
				list_free(partOids);
				partOids = newList;
			}
			else
			{
				// this instance of the table requires all parts - remove
				// it from the output list and add it to the full scan list
				*oidLists = list_delete_ptr(*oidLists, partOids);
				*partTables = list_delete_oid(*partTables, ps->relid);
				list_free(partOids);

				fullScanTables = lappend_oid(fullScanTables, ps->relid);
			}
		}
		else if (!allPartsSelected)
		{
			// table has not been encountered before
			if (ps->staticSelection)
			{
				// static selection, use list of selected parts + root oid
				List *partOids = list_copy(ps->staticPartOids);
				partOids = lappend_oid(partOids, ps->relid);

				*oidLists = lappend(*oidLists, partOids);
				*partTables = lappend_oid(*partTables, ps->relid);
			}
			else
			{
				// table needs all parts
				fullScanTables = lappend_oid(fullScanTables, ps->relid);
			}
		}
	}

	list_free(partitionSelectors);
	list_free(fullScanTables);
}


/* ----------------------------------------------------------------
 *		ExecutorStart
 *
 *		This routine must be called at the beginning of any execution of any
 *		query plan
 *
 * Takes a QueryDesc previously created by CreateQueryDesc (it's not real
 * clear why we bother to separate the two functions, but...).	The tupDesc
 * field of the QueryDesc is filled in to describe the tuples that will be
 * returned, and the internal fields (estate and planstate) are set up.
 *
 * eflags contains flag bits as described in executor.h.
 *
 * NB: the CurrentMemoryContext when this is called will become the parent
 * of the per-query context used for this Executor invocation.
 *
 * MPP: In here we take care of setting up all the necessary items that
 * will be needed to service the query, such as setting up interconnect,
 * and dispatching the query. Any other items in the future
 * must be added here.
 * ----------------------------------------------------------------
 */
void
ExecutorStart(QueryDesc *queryDesc, int eflags)
{
	EState	   *estate;
	MemoryContext oldcontext;
	GpExecIdentity exec_identity;
	bool		shouldDispatch;

    /* sanity checks: queryDesc must not be started already */
    Assert(queryDesc != NULL);
    Assert(queryDesc->estate == NULL);
    Assert(queryDesc->plannedstmt != NULL);

    PlannedStmt *plannedStmt = queryDesc->plannedstmt;
    
    if (NULL == plannedStmt->memoryAccount)
    {
        plannedStmt->memoryAccount = MemoryAccounting_CreateAccount(0, MEMORY_OWNER_TYPE_EXECUTOR);
    }
    
    START_MEMORY_ACCOUNT(plannedStmt->memoryAccount);
    {
        Assert(queryDesc->plannedstmt->intoPolicy == NULL
               || queryDesc->plannedstmt->intoPolicy->ptype == POLICYTYPE_PARTITIONED);
        
        if ( Gp_role == GP_ROLE_DISPATCH ) {
            queryDesc->savedResource = GetActiveQueryResource();
            SetActiveQueryResource(queryDesc->resource);
        }

        /**
         * Perfmon related stuff.
         */
        if (gp_enable_gpperfmon
            && Gp_role == GP_ROLE_DISPATCH
            && queryDesc->gpmon_pkt)
        {
            gpmon_qlog_query_start(queryDesc->gpmon_pkt);
        }

        /**
         * Distribute memory to operators.
         */
        if (Gp_role == GP_ROLE_DISPATCH)
        {
            QueryResource *resource = GetActiveQueryResource();
            if (resource) {
                queryDesc->plannedstmt->query_mem = resource->segment_memory_mb;
                queryDesc->plannedstmt->query_mem *= 1024 * 1024;
                
                elog(DEBUG3, "Query requested %.0fKB memory %lf core",
                     (double) (1.0 * queryDesc->plannedstmt->query_mem / 1024.0),
                     resource->segment_vcore);
            }
            
            /**
             * There are some statements that do not go through the resource queue,
             * so we cannot put in a strong assert here. Someday, we should fix
             * resource queues.
             */
            if (queryDesc->plannedstmt->query_mem > 0) {

                uint64 memAvailableBytes = 0;

                /* With resouce manager in NONE mode, we assign memory quota for operators normally */
                if ( strcasecmp(rm_global_rm_type, HAWQDRM_CONFFILE_SVRTYPE_VAL_NONE) == 0 )
                {
                    memAvailableBytes = queryDesc->plannedstmt->query_mem;
                }
                /* With resouce manager in YARN/MESOS mode, we assign memory quota for operators conservatively */
                else
                {
                    memAvailableBytes = queryDesc->plannedstmt->query_mem * hawq_re_memory_quota_allocation_ratio;
                }

                AssignOperatorMemoryKB(queryDesc->plannedstmt, memAvailableBytes);
            }
        }
        
        /*
         * If the transaction is read-only, we need to check if any writes are
         * planned to non-temporary tables.  EXPLAIN is considered read-only.
         */
        if ((XactReadOnly || Gp_role == GP_ROLE_DISPATCH) && !(eflags & EXEC_FLAG_EXPLAIN_ONLY))
            ExecCheckXactReadOnly(queryDesc->plannedstmt);

        /*
         * Build EState, switch into per-query memory context for startup.
         */
        estate = CreateExecutorState();
        queryDesc->estate = estate;
        
        oldcontext = MemoryContextSwitchTo(estate->es_query_cxt);
        
        /**
         * Attached the plannedstmt from queryDesc
         */
        estate->es_plannedstmt = queryDesc->plannedstmt;
        
        /*
         * Fill in parameters, if any, from queryDesc
         */
        estate->es_param_list_info = queryDesc->params;
        
        if (queryDesc->plannedstmt->nCrossLevelParams > 0)
            estate->es_param_exec_vals = (ParamExecData *)
            palloc0(queryDesc->plannedstmt->nCrossLevelParams * sizeof(ParamExecData));
        
        /*
         * Copy other important information into the EState
         */
        estate->es_snapshot = queryDesc->snapshot;
        estate->es_crosscheck_snapshot = queryDesc->crosscheck_snapshot;
        estate->es_instrument = queryDesc->doInstrument;
        estate->showstatctx = queryDesc->showstatctx;
        
        /*
         * Shared input info is needed when ROLE_EXECUTE or sequential plan
         */
        estate->es_sharenode = (List **) palloc0(sizeof(List *));

        /*
         * Initialize the motion layer for this query.
         *
         * NOTE: need to be in estate->es_query_cxt before the call.
         */
        initMotionLayerStructs((MotionLayerState **)&estate->motionlayer_context);

        /* Reset workfile disk full flag */
        WorkfileDiskspace_SetFull(false /* isFull */);
        /* Initialize per-query resource (diskspace) tracking */
        WorkfileQueryspace_InitEntry(gp_session_id, gp_command_count);

        /*
         * Handling of the Slice table depends on context.
         */
        if (Gp_role == GP_ROLE_DISPATCH && queryDesc->plannedstmt->planTree->dispatch == DISPATCH_PARALLEL)
        {
            SetupDispatcherIdentity(queryDesc->plannedstmt->planner_segments);
            /*
             * If this is an extended query (normally cursor or bind/exec) - before
             * starting the portal, we need to make sure that the shared snapshot is
             * already set by a writer gang, or the cursor query readers will
             * timeout waiting for one that may not exist (in some cases). Therefore
             * we insert a small hack here and dispatch a SET query that will do it
             * for us. (This is also done in performOpenCursor() for the simple
             * query protocol).
             *
             * MPP-7504/MPP-7448: We also call this down inside the dispatcher after
             * the pre-dispatch evaluator has run.
             */
            if (queryDesc->extended_query)
            {
                /* verify_shared_snapshot_ready(); */
            }

            /* Set up blank slice table to be filled in during InitPlan. */
            InitSliceTable(estate, queryDesc->plannedstmt->nMotionNodes, queryDesc->plannedstmt->nInitPlans);
            
            /**
             * Copy direct dispatch decisions out of the plan and into the slice table.  Must be done after slice table is built.
             * Note that this needs to happen whether or not the plan contains direct dispatch decisions. This
             * is because the direct dispatch partially forgets some of the decisions it has taken.
             **/
            if (gp_enable_direct_dispatch)
            {
                CopyDirectDispatchFromPlanToSliceTable(queryDesc->plannedstmt, estate );
            }
            
            /* Pass EXPLAIN ANALYZE flag to qExecs. */
            estate->es_sliceTable->doInstrument = queryDesc->doInstrument;
            
            /* set our global sliceid variable for elog. */
            currentSliceId = LocallyExecutingSliceIndex(estate);
            
            /* Determin OIDs for into relation, if any */
            if (queryDesc->plannedstmt->intoClause != NULL)
            {
                IntoClause *intoClause = queryDesc->plannedstmt->intoClause;
                Relation	pg_class_desc;
                Relation	pg_type_desc;
                Oid         reltablespace;
                
                /* MPP-10329 - must always dispatch the tablespace */
                if (intoClause->tableSpaceName)
                {
                    reltablespace = get_tablespace_oid(intoClause->tableSpaceName);
                    if (!OidIsValid(reltablespace))
                        ereport(ERROR,
                                (errcode(ERRCODE_UNDEFINED_OBJECT),
                                 errmsg("tablespace \"%s\" does not exist",
                                        intoClause->tableSpaceName)));
                }
                else
                {
                    reltablespace = GetDefaultTablespace();
                    
                    /* Need the real tablespace id for dispatch */
                    if (!OidIsValid(reltablespace))
                    /*reltablespace = MyDatabaseTableSpace;*/
                    reltablespace = get_database_dts(MyDatabaseId);
                    
                    intoClause->tableSpaceName = get_tablespace_name(reltablespace);
                }
                
                pg_class_desc = heap_open(RelationRelationId, RowExclusiveLock);
                pg_type_desc = heap_open(TypeRelationId, RowExclusiveLock);
                
                intoClause->oidInfo.relOid = GetNewRelFileNode(reltablespace, false, pg_class_desc, false);
                elog(DEBUG3, "ExecutorStart assigned new intoOidInfo.relOid = %d",
                     intoClause->oidInfo.relOid);
                
                intoClause->oidInfo.comptypeOid = GetNewRelFileNode(reltablespace, false, pg_type_desc, false);
                intoClause->oidInfo.toastOid = GetNewRelFileNode(reltablespace, false, pg_class_desc, false);
                intoClause->oidInfo.toastIndexOid = GetNewRelFileNode(reltablespace, false, pg_class_desc, false);
                intoClause->oidInfo.toastComptypeOid = GetNewRelFileNode(reltablespace, false, pg_type_desc, false);
                intoClause->oidInfo.aosegOid = GetNewRelFileNode(reltablespace, false, pg_class_desc, false);
                intoClause->oidInfo.aosegIndexOid = GetNewRelFileNode(reltablespace, false, pg_class_desc, false);
                intoClause->oidInfo.aosegComptypeOid = GetNewRelFileNode(reltablespace, false, pg_type_desc, false);
                intoClause->oidInfo.aoblkdirOid = GetNewRelFileNode(reltablespace, false, pg_class_desc, false);
                intoClause->oidInfo.aoblkdirIndexOid = GetNewRelFileNode(reltablespace, false, pg_class_desc, false);
                intoClause->oidInfo.aoblkdirComptypeOid = GetNewRelFileNode(reltablespace, false, pg_type_desc, false);
                
                heap_close(pg_class_desc, RowExclusiveLock);
                heap_close(pg_type_desc, RowExclusiveLock);
                
            }
        }
        else if (Gp_role == GP_ROLE_EXECUTE)
        {
            
            /* qDisp should have sent us a slice table via MPPEXEC */
            if (queryDesc->plannedstmt->sliceTable != NULL)
            {
                SliceTable *sliceTable;
                Slice	   *slice;
                
                sliceTable = (SliceTable *)queryDesc->plannedstmt->sliceTable;
                Assert(IsA(sliceTable, SliceTable));
                slice = (Slice *)list_nth(sliceTable->slices, sliceTable->localSlice);
                Assert(IsA(slice, Slice));
                
                estate->es_sliceTable = sliceTable;
                
                estate->currentSliceIdInPlan = slice->rootIndex;
                estate->currentExecutingSliceId = slice->rootIndex;
                
                /* set our global sliceid variable for elog. */
                currentSliceId = LocallyExecutingSliceIndex(estate);
                
                /* Should we collect statistics for EXPLAIN ANALYZE? */
                estate->es_instrument = sliceTable->doInstrument;
                queryDesc->doInstrument = sliceTable->doInstrument;
            }
            
            /* InitPlan() will acquire locks by walking the entire plan
             * tree -- we'd like to avoid acquiring the locks until
             * *after* we've set up the interconnect */
            if (queryDesc->plannedstmt->nMotionNodes > 0)
            {
                int i;
                
                PG_TRY();
                {
                    for (i=1; i <= queryDesc->plannedstmt->nMotionNodes; i++)
                    {
                        InitMotionLayerNode(estate->motionlayer_context, i);
                    }
                    
                    estate->es_interconnect_is_setup = true;
                    
                    Assert(!estate->interconnect_context);
                    SetupInterconnect(estate);
                    Assert(estate->interconnect_context);
                }
                PG_CATCH();
                {
                    mppExecutorCleanup(queryDesc);
                    if (GP_ROLE_DISPATCH == Gp_role)
                    {
                      SetActiveQueryResource(queryDesc->savedResource);
                    }
                    PG_RE_THROW();
                }
                PG_END_TRY();
            }
        }

        /* If the interconnect has been set up; we need to catch any
         * errors to shut it down -- so we have to wrap InitPlan in a PG_TRY() block. */
        PG_TRY();
        {
            /*
             * Initialize the plan state tree
             */
            Assert(CurrentMemoryContext == estate->es_query_cxt);
            InitPlan(queryDesc, eflags);
            
            Assert(queryDesc->planstate);
            
            if (Gp_role == GP_ROLE_DISPATCH &&
                queryDesc->plannedstmt->planTree->dispatch == DISPATCH_PARALLEL)
            {
                /* Assign gang descriptions to the root slices of the slice forest. */
                InitRootSlices(queryDesc, queryDesc->planner_segments);
                
                if (!(eflags & EXEC_FLAG_EXPLAIN_ONLY))
                {
                    /*
                     * Since we intend to execute the plan, inventory the slice tree,
                     * allocate gangs, and associate them with slices.
                     *
                     * For now, always use segment 'gp_singleton_segindex' for
                     * singleton gangs.
                     *
                     * On return, gangs have been allocated and CDBProcess lists have
                     * been filled in in the slice table.)
                     */
                    AssignGangs(queryDesc, gp_singleton_segindex);
                }
                
            }
            
#ifdef USE_ASSERT_CHECKING
            AssertSliceTableIsValid((struct SliceTable *) estate->es_sliceTable, queryDesc->plannedstmt);
#endif
            
            if (Debug_print_slice_table && Gp_role == GP_ROLE_DISPATCH)
                elog_node_display(DEBUG3, "slice table", estate->es_sliceTable, true);
            
            /*
             * If we're running as a QE and there's a slice table in our queryDesc,
             * then we need to finish the EState setup we prepared for back in
             * CdbExecQuery.
             */
            if (Gp_role == GP_ROLE_EXECUTE && estate->es_sliceTable != NULL)
            {
                MotionState *motionstate = NULL;
                
                /*
                 * Note that, at this point on a QE, the estate is setup (based on the
                 * slice table transmitted from the QD via MPPEXEC) so that fields
                 * es_sliceTable, cur_root_idx and es_cur_slice_idx are correct for
                 * the QE.
                 *
                 * If responsible for a non-root slice, arrange to enter the plan at the
                 * slice's sending Motion node rather than at the top.
                 */
                if (LocallyExecutingSliceIndex(estate) != RootSliceIndex(estate))
                {
                    motionstate = getMotionState(queryDesc->planstate, LocallyExecutingSliceIndex(estate));
                    Assert(motionstate != NULL && IsA(motionstate, MotionState));
                    
                    /* Patch Motion node so it looks like a top node. */
                    motionstate->ps.plan->nMotionNodes = estate->es_sliceTable->nMotions;
                    motionstate->ps.plan->nParamExec = estate->es_sliceTable->nInitPlans;
                }
                
                if (Debug_print_slice_table)
                    elog_node_display(DEBUG3, "slice table", estate->es_sliceTable, true);
                
                if (gp_log_interconnect >= GPVARS_VERBOSITY_DEBUG)
                    elog(DEBUG1, "seg%d executing slice%d under root slice%d",
                         GetQEIndex(),
                         LocallyExecutingSliceIndex(estate),
                         RootSliceIndex(estate));
            }
            
            /*
             * Are we going to dispatch this plan parallel?  Only if we're running as
             * a QD and the plan is a parallel plan.
             */
            if (Gp_role == GP_ROLE_DISPATCH &&
                queryDesc->plannedstmt->planTree->dispatch == DISPATCH_PARALLEL &&
                !(eflags & EXEC_FLAG_EXPLAIN_ONLY))
            {
                shouldDispatch = true;
            }
            else
            {
                shouldDispatch = false;
            }

            /*
             * if in dispatch mode, time to serialize plan and query
             * trees, and fire off cdb_exec command to each of the qexecs
             */
            if (shouldDispatch)
            {
                /*
                 * collect catalogs which should be dispatched to QE
                 */
                if (Gp_role == GP_ROLE_DISPATCH)
                {
                    int i;
                    
                    List *result_segfileinfos = NIL;
                    PlannedStmt *plannedstmt = queryDesc->plannedstmt;
                    Assert(NULL == plannedstmt->contextdisp);
                    plannedstmt->contextdisp = CreateQueryContextInfo();
                    
                    /*
                     * (GPSQL-872) Include all tuples from pg_aoseg_*
                     * catalog for relations that are used in FROM clause.
                     */
                    if (plannedstmt->rtable)
                    {
                        /*
                         * find all partitioned tables for which static selection has been done.
                         * "partTables" will have the oids of these tables
                         * "oidsToDispatch" will have the corresponding lists of selected child oids for each table
                         */
                        List *partTables = NIL;
                        List *oidsToDispatch = NIL;
                        findPartTableOidsToDispatch(plannedstmt->planTree, &partTables, &oidsToDispatch);
                        
                        ListCell *rtc;
                        int rti=0;
                        foreach(rtc, plannedstmt->rtable)
                        {
                            ++rti; /* List indices start with 1. */
                            RangeTblEntry *rte = lfirst(rtc);
                            if (rte->rtekind == RTE_RELATION)
                            {
                                ListCell *relc;
                                bool relForInsert = FALSE;
                                foreach(relc, plannedstmt->resultRelations)
                                {
                                    int reli = lfirst_int(relc);
                                    if (reli == rti)
                                    {
                                        relForInsert = TRUE;
                                        break;
                                    }
                                }
                                if (!relForInsert)
                                {
                                    int idx = list_find_oid(partTables, rte->relid);
                                    if (0 <= idx)
                                    {
                                        /*
                                         * static selection performed on table - add only
                                         * the oids of the selected parts
                                         */
                                        List *oidList = list_nth(oidsToDispatch, idx);
                                        ListCell *oidc = NULL;
                                        foreach (oidc, oidList)
                                        {
                                            prepareDispatchedCatalogSingleRelation(plannedstmt->contextdisp,
                                                                                   lfirst_oid(oidc), FALSE /*forInsert*/, 0 /*segno*/);
                                        }
                                    }
                                    else
                                    {
                                        prepareDispatchedCatalogRelation(plannedstmt->contextdisp,
                                                                         rte->relid, FALSE, NULL);
                                    }
                                }
                                
                            }
                        }
                        
                        list_free(partTables);
                        list_free_deep(oidsToDispatch);
                    }
                        
                    for (i = 0; i < estate->es_num_result_relations; ++i)
                    {
                        ResultRelInfo * relinfo;
                        relinfo = &estate->es_result_relation_info[i];
                        prepareDispatchedCatalogRelation(plannedstmt->contextdisp,
                                                         RelationGetRelid(relinfo->ri_RelationDesc), TRUE, estate->es_result_aosegnos);
                        result_segfileinfos = GetResultRelSegFileInfos(RelationGetRelid(relinfo->ri_RelationDesc),
                                                                       estate->es_result_aosegnos, result_segfileinfos);
                    }
                    plannedstmt->result_segfileinfos = result_segfileinfos;
                    
                    if (plannedstmt->intoClause != NULL)
                    {
                        List *segment_segnos = SetSegnoForWrite(NIL, 0, GetQEGangNum(), true, false);
                        prepareDispatchedCatalogSingleRelation(plannedstmt->contextdisp,
                                                               plannedstmt->intoClause->oidInfo.relOid, TRUE, segment_segnos);
                    }
                    
                    if (plannedstmt->rtable)
                        prepareDispatchedCatalog(plannedstmt->contextdisp, plannedstmt->rtable);
                    
                    if (plannedstmt->returningLists)
                    {
                        ListCell *lc;
                        foreach(lc, plannedstmt->returningLists)
                        {
                            List *targets = lfirst(lc);
                            
                            if (targets)
                                prepareDispatchedCatalogTargets(plannedstmt->contextdisp, targets);
                        }
                    }
                    
                    prepareDispatchedCatalogPlan(plannedstmt->contextdisp, plannedstmt->planTree);
                    
                    if (plannedstmt->subplans)
                    {
                        ListCell *lc;
                        foreach(lc, plannedstmt->subplans)
                        {
                            Plan *plantree = lfirst(lc);
                            if (plantree)
                                prepareDispatchedCatalogPlan(plannedstmt->contextdisp, plantree);
                        }
                    }
                    
                    /**
                     * travel the plan for external table scan to setup error table segno.
                     */
                    QueryCxtWalkerCxt cxt;
                    cxt.base.node = (Node *)plannedstmt;
                    cxt.info = plannedstmt->contextdisp;
                    plan_tree_walker((Node *)plannedstmt->planTree, SetupSegnoForErrorTable, &cxt);
                    
                    FinalizeQueryContextInfo(plannedstmt->contextdisp);
                }
                
                /*
                 * First, see whether we need to pre-execute any initPlan subplans.
                 */
                if (queryDesc->plannedstmt->planTree->nParamExec > 0)
                {
                    preprocess_initplans(queryDesc);
                    
                    /*
                     * Copy the values of the preprocessed subplans to the
                     * external parameters.
                     */
                    queryDesc->params = addRemoteExecParamsToParamList(queryDesc->plannedstmt,
                                                                       queryDesc->params,
                                                                       queryDesc->estate->es_param_exec_vals);
                }
                estate->dispatch_data = initialize_dispatch_data(queryDesc->resource, false);
                prepare_dispatch_query_desc(estate->dispatch_data, queryDesc);
                dispatch_run(estate->dispatch_data);
                cleanup_dispatch_data(estate->dispatch_data);
                
                DropQueryContextInfo(queryDesc->plannedstmt->contextdisp);
                queryDesc->plannedstmt->contextdisp = NULL;
            }

            /*
             * Get executor identity (who does the executor serve). we can assume
             * Forward scan direction for now just for retrieving the identity.
             */
            if (!(eflags & EXEC_FLAG_EXPLAIN_ONLY))
                exec_identity = getGpExecIdentity(queryDesc, ForwardScanDirection, estate);
            else
                exec_identity = GP_IGNORE;
            
            /* non-root on QE */
            if (exec_identity == GP_NON_ROOT_ON_QE)
            {
                
                MotionState *motionState = getMotionState(queryDesc->planstate, LocallyExecutingSliceIndex(estate));
                
                Assert(motionState);
                
                Assert(IsA(motionState->ps.plan, Motion));
                
                /* update the connection information, if needed */
                if (((PlanState *) motionState)->plan->nMotionNodes > 0)
                {
                    ExecUpdateTransportState((PlanState *)motionState,
                                             estate->interconnect_context);
                }
            }
            else if (exec_identity == GP_ROOT_SLICE)
            {
                /* Run a root slice. */
                if (queryDesc->planstate != NULL &&
                    queryDesc->planstate->plan->nMotionNodes > 0 && !estate->es_interconnect_is_setup)
                {
                    estate->es_interconnect_is_setup = true;
                    
                    Assert(!estate->interconnect_context);
                    SetupInterconnect(estate);
                    Assert(estate->interconnect_context);
                }
                if (estate->es_interconnect_is_setup)
                {
                    ExecUpdateTransportState(queryDesc->planstate,
                                             estate->interconnect_context);
                }
            }
            else if (exec_identity != GP_IGNORE)
            {
                /* should never happen */
                Assert(!"unsupported parallel execution strategy");
            }
            
            if(estate->es_interconnect_is_setup)
                Assert(estate->interconnect_context != NULL);
            
        }

        PG_CATCH();
        {
            if (queryDesc->plannedstmt->contextdisp)
            {
                DropQueryContextInfo(queryDesc->plannedstmt->contextdisp);
                queryDesc->plannedstmt->contextdisp = NULL;
            }
            mppExecutorCleanup(queryDesc);
            if (GP_ROLE_DISPATCH == Gp_role)
            {
              SetActiveQueryResource(queryDesc->savedResource);
            }
            PG_RE_THROW();
        }
        PG_END_TRY();
        
        if (DEBUG1 >= log_min_messages)
        {
            char		msec_str[32];
            switch (check_log_duration(msec_str, false))
            {
                case 1:
                case 2:
                    ereport(LOG, (errmsg("duration to ExecutorStart end: %s ms", msec_str)));
                    break;
            }
        }
        if (GP_ROLE_DISPATCH == Gp_role)
        {
          SetActiveQueryResource(queryDesc->savedResource);
        }
	}
	END_MEMORY_ACCOUNT();

	MemoryContextSwitchTo(oldcontext);
}

/* ----------------------------------------------------------------
 *		ExecutorRun
 *
 *		This is the main routine of the executor module. It accepts
 *		the query descriptor from the traffic cop and executes the
 *		query plan.
 *
 *		ExecutorStart must have been called already.
 *
 *		If direction is NoMovementScanDirection then nothing is done
 *		except to start up/shut down the destination.  Otherwise,
 *		we retrieve up to 'count' tuples in the specified direction.
 *
 *		Note: count = 0 is interpreted as no portal limit, i.e., run to
 *		completion.
 *
 *		MPP: In here we must ensure to only run the plan and not call
 *		any setup/teardown items (unless in a CATCH block).
 *
 * ----------------------------------------------------------------
 */
TupleTableSlot *
ExecutorRun(QueryDesc *queryDesc,
		ScanDirection direction, long count)
{
	EState	   *estate;
	CmdType		operation;
	DestReceiver *dest;
	bool		sendTuples;
	TupleTableSlot *result = NULL;
	MemoryContext oldcontext;
	/*
	 * NOTE: Any local vars that are set in the PG_TRY block and examined in the
	 * PG_CATCH block should be declared 'volatile'. (setjmp shenanigans)
	 */
	Slice              *currentSlice;
	GpExecIdentity		exec_identity;

	/* sanity checks */
	Assert(queryDesc != NULL);

	estate = queryDesc->estate;

	Assert(estate != NULL);

	Assert(NULL != queryDesc->plannedstmt && NULL != queryDesc->plannedstmt->memoryAccount);

	START_MEMORY_ACCOUNT(queryDesc->plannedstmt->memoryAccount);

  if (Debug_print_execution_detail) {
    instr_time  time;
    INSTR_TIME_SET_CURRENT(time);
    elog(DEBUG1,"The time on entering ExecutorRun: %.3f ms",
                     1000.0 * INSTR_TIME_GET_DOUBLE(time));
  }

  if (GP_ROLE_DISPATCH == Gp_role)
  {
    queryDesc->savedResource = GetActiveQueryResource();
    SetActiveQueryResource(queryDesc->resource);
  }
	/*
	 * Set dynamicTableScanInfo to the one in estate, and reset its value at
	 * the end of ExecutorRun(). This is to support two cases:
	 *
	 * (1) For PLPgsql/SQL functions. There might be multiple DynamicTableScanInfos
	 * involved, one for each statement in the function. We set the global variable
	 * dynamicTableScanInfo to the value for the running statement here, and reset
	 * its value at the end of ExecutorRun().
	 *
	 * (2) For cursor queries. Each cursor query has its own set of DynamicTableScanInfos,
	 * and they could be called in different orders.
	 */
	DynamicTableScanInfo *origDynamicTableScanInfo = dynamicTableScanInfo;
	dynamicTableScanInfo = estate->dynamicTableScanInfo;

	/*
	 * Switch into per-query memory context
	 */
	oldcontext = MemoryContextSwitchTo(estate->es_query_cxt);

	/*
	 * CDB: Update global slice id for log messages.
	 */
	currentSlice = getCurrentSlice(estate, LocallyExecutingSliceIndex(estate));
	if (currentSlice)
	{
		if (Gp_role == GP_ROLE_EXECUTE ||
				sliceRunsOnQD(currentSlice))
			currentSliceId = currentSlice->sliceIndex;
	}

	/*
	 * extract information from the query descriptor and the query feature.
	 */
	operation = queryDesc->operation;
	dest = queryDesc->dest;

	/*
	 * startup tuple receiver, if we will be emitting tuples
	 */
	estate->es_processed = 0;
	estate->es_lastoid = InvalidOid;

	sendTuples = (queryDesc->tupDesc != NULL &&
			(operation == CMD_SELECT ||
			 queryDesc->plannedstmt->returningLists));

	if (sendTuples)
		(*dest->rStartup) (dest, operation, queryDesc->tupDesc);

	/*
	 * Need a try/catch block here so that if an ereport is called from
	 * within ExecutePlan, we can clean up by calling CdbCheckDispatchResult.
	 * This cleans up the asynchronous commands running through the threads launched from
	 * CdbDispatchCommand.
	 */
	PG_TRY();
	{
		/*
		 * Run the plan locally.  There are three ways;
		 *
		 * 1. Do nothing
		 * 2. Run a root slice
		 * 3. Run a non-root slice on a QE.
		 *
		 * Here we decide what is our identity -- root slice, non-root
		 * on QE or other (in which case we do nothing), and then run
		 * the plan if required. For more information see
		 * getGpExecIdentity() in execUtils.
		 */
		exec_identity = getGpExecIdentity(queryDesc, direction, estate);

		if (exec_identity == GP_IGNORE)
		{
			result = NULL;
		}
		else if (exec_identity == GP_NON_ROOT_ON_QE)
		{
			/*
			 * Run a non-root slice on a QE.
			 *
			 * Since the top Plan node is a (Sending) Motion, run the plan
			 * forward to completion. The plan won't return tuples locally
			 * (tuples go out over the interconnect), so the destination is
			 * uninteresting.  The command type should be SELECT, however, to
			 * avoid other sorts of DML processing..
			 *
			 * This is the center of slice plan activity -- here we arrange to
			 * blunder into the middle of the plan rather than entering at the
			 * root.
			 */

			MotionState *motionState = getMotionState(queryDesc->planstate, LocallyExecutingSliceIndex(estate));

			Assert(motionState);

			result = ExecutePlan(estate,
					(PlanState *) motionState,
					CMD_SELECT,
					0,
					ForwardScanDirection,
					dest);
		}
		else if (exec_identity == GP_ROOT_SLICE)
		{
			/*
			 * Run a root slice
			 * It corresponds to the "normal" path through the executor
			 * in that we enter the plan at the top and count on the
			 * motion nodes at the fringe of the top slice to return
			 * without ever calling nodes below them.
			 */
			result = ExecutePlan(estate,
					queryDesc->planstate,
					operation,
					count,
					direction,
					dest);
		}
		else
		{
			/* should never happen */
			Assert(!"undefined parallel execution strategy");
		}

		/*
		 * if result is null we got end-of-stream. We need to mark it
		 * since with a cursor end-of-stream will only be received with
		 * the fetch that returns the last tuple. ExecutorEnd needs to
		 * know if EOS was received in order to do the right cleanup.
		 */
		if(result == NULL)
			estate->es_got_eos = true;

		dynamicTableScanInfo = origDynamicTableScanInfo;
	}
	PG_CATCH();
	{
		dynamicTableScanInfo = origDynamicTableScanInfo;

		/* If EXPLAIN ANALYZE, let qExec try to return stats to qDisp. */
		if (estate->es_sliceTable &&
				estate->es_sliceTable->doInstrument &&
				Gp_role == GP_ROLE_EXECUTE)
		{
			PG_TRY();
			{
				cdbexplain_sendExecStats(queryDesc);
			}
			PG_CATCH();
			{
				/* Close down interconnect etc. */
				mppExecutorCleanup(queryDesc);
				PG_RE_THROW();
			}
			PG_END_TRY();
		}

		/* Close down interconnect etc. */
		mppExecutorCleanup(queryDesc);
		if (GP_ROLE_DISPATCH == Gp_role)
		{
		  SetActiveQueryResource(queryDesc->savedResource);
		}
		PG_RE_THROW();
	}
	PG_END_TRY();

	/*
	 * shutdown tuple receiver, if we started it
	 */
	if (sendTuples)
		(*dest->rShutdown) (dest);

	MemoryContextSwitchTo(oldcontext);
	END_MEMORY_ACCOUNT();

  if (Debug_print_execution_detail) {
    instr_time  time;
    INSTR_TIME_SET_CURRENT(time);
    elog(DEBUG1,"The time before quit ExecutorRun: %.3f ms",
                     1000.0 * INSTR_TIME_GET_DOUBLE(time));
  }

  if (GP_ROLE_DISPATCH == Gp_role)
  {
    SetActiveQueryResource(queryDesc->savedResource);
  }

	return result;
}

/* ----------------------------------------------------------------
 *		ExecutorEnd
 *
 *		This routine must be called at the end of execution of any
 *		query plan
 * ----------------------------------------------------------------
 */
	void
ExecutorEnd(QueryDesc *queryDesc)
{
	EState	   *estate;
	MemoryContext oldcontext;

	/* sanity checks */
	Assert(queryDesc != NULL);

	/* Cleanup the global resource reference for spi/function resource inheritate. */
	if ( Gp_role == GP_ROLE_DISPATCH ) {
		AutoFreeResource(queryDesc->resource);
		queryDesc->resource = NULL;
	}

	estate = queryDesc->estate;

	Assert(estate != NULL);

	Assert(NULL != queryDesc->plannedstmt && NULL != queryDesc->plannedstmt->memoryAccount);

	START_MEMORY_ACCOUNT(queryDesc->plannedstmt->memoryAccount);

	if (DEBUG1 >= log_min_messages)
	{
		char		msec_str[32];
		switch (check_log_duration(msec_str, false))
		{
			case 1:
			case 2:
				ereport(LOG, (errmsg("duration to ExecutorEnd starting: %s ms", msec_str),
							errOmitLocation(true)));
				break;
		}
	}

	if (gp_partitioning_dynamic_selection_log &&
			estate->dynamicTableScanInfo != NULL &&
			estate->dynamicTableScanInfo->numScans > 0)
	{
		for (int scanNo = 0; scanNo < estate->dynamicTableScanInfo->numScans; scanNo++)
		{
			dumpDynamicTableScanPidIndex(scanNo);
		}
	}

	/*
	 * Switch into per-query memory context to run ExecEndPlan
	 */
	oldcontext = MemoryContextSwitchTo(estate->es_query_cxt);

	/*
	 * If EXPLAIN ANALYZE, qExec returns stats to qDisp now.
	 */
	if (estate->es_sliceTable &&
			estate->es_sliceTable->doInstrument &&
			Gp_role == GP_ROLE_EXECUTE)
		cdbexplain_sendExecStats(queryDesc);

	/*
	 * if needed, collect mpp dispatch results and tear down
	 * all mpp specific resources (interconnect, seq server).
	 */
	PG_TRY();
	{
		mppExecutorFinishup(queryDesc);
	}
	PG_CATCH();
	{
		/*
		 * we got an error. do all the necessary cleanup.
		 */
		mppExecutorCleanup(queryDesc);

		/*
		 * Remove our own query's motion layer.
		 */
		RemoveMotionLayer(estate->motionlayer_context, true);

		/*
		 * Release EState and per-query memory context.  This should release
		 * everything the executor has allocated.
		 */
		FreeExecutorState(estate);

		PG_RE_THROW();
	}
	PG_END_TRY();

	/*
	 * If normal termination, let each operator clean itself up.
	 * Otherwise don't risk it... an error might have left some
	 * structures in an inconsistent state.
	 */
	ExecEndPlan(queryDesc->planstate, estate);

	WorkfileQueryspace_ReleaseEntry();

	/*
	 * Remove our own query's motion layer.
	 */
	RemoveMotionLayer(estate->motionlayer_context, true);

	/*
	 * Close the SELECT INTO relation if any
	 */
	if (estate->es_select_into)
		CloseIntoRel(queryDesc);

	/*
	 * Must switch out of context before destroying it
	 */
	MemoryContextSwitchTo(oldcontext);

	queryDesc->es_processed = estate->es_processed;
	queryDesc->es_lastoid = estate->es_lastoid;

	/*
	 * Release EState and per-query memory context.  This should release
	 * everything the executor has allocated.
	 */
	FreeExecutorState(estate);

	/**
	 * Perfmon related stuff.
	 */
	if (gp_enable_gpperfmon 
			&& Gp_role == GP_ROLE_DISPATCH
			&& queryDesc->gpmon_pkt)
	{			
		gpmon_qlog_query_end(queryDesc->gpmon_pkt);
		queryDesc->gpmon_pkt = NULL;
	}

	/* Reset queryDesc fields that no longer point to anything */
	queryDesc->tupDesc = NULL;
	queryDesc->estate = NULL;
	queryDesc->planstate = NULL;
	if (DEBUG1 >= log_min_messages)
	{
		char		msec_str[32];
		switch (check_log_duration(msec_str, false))
		{
			case 1:
			case 2:
				ereport(LOG, (errmsg("duration to ExecutorEnd end: %s ms", msec_str),
							errOmitLocation(true)));
				break;
		}
	}
	END_MEMORY_ACCOUNT();

	if (gp_dump_memory_usage)
	{
		MemoryAccounting_SaveToFile(currentSliceId);
	}
	ReportOOMConsumption();
}

/* ----------------------------------------------------------------
 *		ExecutorRewind
 *
 *		This routine may be called on an open queryDesc to rewind it
 *		to the start.
 * ----------------------------------------------------------------
 */
	void
ExecutorRewind(QueryDesc *queryDesc)
{
	EState	   *estate;
	MemoryContext oldcontext;

	/* sanity checks */
	Assert(queryDesc != NULL);

	estate = queryDesc->estate;

	Assert(estate != NULL);

	Assert(NULL != queryDesc->plannedstmt && NULL != queryDesc->plannedstmt->memoryAccount);

	START_MEMORY_ACCOUNT(queryDesc->plannedstmt->memoryAccount);

	/* It's probably not sensible to rescan updating queries */
	Assert(queryDesc->operation == CMD_SELECT);

	/*
	 * Switch into per-query memory context
	 */
	oldcontext = MemoryContextSwitchTo(estate->es_query_cxt);

	/*
	 * rescan plan
	 */
	ExecReScan(queryDesc->planstate, NULL);

	MemoryContextSwitchTo(oldcontext);

	END_MEMORY_ACCOUNT();
}



/*
 * This function is used to check if the current statement will perform any writes.
 * It is used to enforce:
 *  (1) read-only mode (both fts and transcation isolation level read only)
 *      as well as
 *  (2) to keep track of when a distributed transaction becomes
 *      "dirty" and will require 2pc.
 Check that the query does not imply any writes to non-temp tables.
 */
	static void
ExecCheckXactReadOnly(PlannedStmt *plannedstmt)
{
	ListCell   *l;
	int         rti;
	bool		changesTempTables = false;

	/*
	 * CREATE TABLE AS or SELECT INTO?
	 *
	 * XXX should we allow this if the destination is temp?
	 */
	if (plannedstmt->intoClause != NULL)
	{
		Assert(plannedstmt->intoClause->rel);
		if (plannedstmt->intoClause->rel->istemp)
			changesTempTables = true;
		else
			goto fail;
	}

	/* Fail if write permissions are requested on any non-temp table */
	rti = 0;
	foreach(l, plannedstmt->rtable)
	{
		RangeTblEntry *rte = lfirst(l);

		rti++;

		if (rte->rtekind == RTE_SUBQUERY)
		{
			continue;
		}

		if (rte->rtekind != RTE_RELATION)
			continue;

		if ((rte->requiredPerms & (~ACL_SELECT)) == 0)
			continue;

		if (isTempNamespace(get_rel_namespace(rte->relid)))
		{
			changesTempTables = true;
			continue;
		}

		/* CDB: Allow SELECT FOR SHARE/UPDATE *
		 *
		 */
		if ((rte->requiredPerms & ~(ACL_SELECT | ACL_SELECT_FOR_UPDATE)) == 0)
		{
			ListCell   *cell;
			bool foundRTI = false;

			foreach(cell, plannedstmt->rowMarks)
			{
				RowMarkClause *rmc = lfirst(cell);
				if( rmc->rti == rti )
				{
					foundRTI = true;
					break;
				}
			}

			if (foundRTI)
				continue;
		}

		goto fail;
	}
	if (changesTempTables)
		ExecutorMarkTransactionDoesWrites();
	return;

fail:
	if (XactReadOnly)
		ereport(ERROR,
				(errcode(ERRCODE_READ_ONLY_SQL_TRANSACTION),
				 errmsg("transaction is read-only")));
	else
		ExecutorMarkTransactionDoesWrites();
}

/*
 * BuildPartitionNodeFromRoot
 *   Build PartitionNode for the root partition of a given partition oid.
 */
	static PartitionNode *
BuildPartitionNodeFromRoot(Oid relid)
{
	PartitionNode *partitionNode = NULL;

	if (rel_is_child_partition(relid))
	{
		relid = rel_partition_get_master(relid);
	}

	partitionNode = RelationBuildPartitionDescByOid(relid, false /* inctemplate */);

	return partitionNode;
}

/*
 * createPartitionAccessMethods
 *   Create a PartitionAccessMethods object.
 *
 * Note that the memory context for the access method is not set at this point. It will
 * be set during execution.
 */
	static PartitionAccessMethods *
createPartitionAccessMethods(int numLevels)
{
	PartitionAccessMethods *accessMethods = palloc(sizeof(PartitionAccessMethods));;
	accessMethods->partLevels = numLevels;
	accessMethods->amstate = palloc0(numLevels * sizeof(void *));
	accessMethods->part_cxt = NULL;

	return accessMethods;
}

/*
 * createPartitionState
 *   Create a PartitionState object.
 *
 * Note that the memory context for the access method is not set at this point. It will
 * be set during execution.
 */
	PartitionState *
createPartitionState(PartitionNode *partsAndRules,
		int resultPartSize)
{
	Assert(partsAndRules != NULL);

	PartitionState *partitionState = makeNode(PartitionState);
	partitionState->accessMethods = createPartitionAccessMethods(num_partition_levels(partsAndRules));
	partitionState->max_partition_attr = max_partition_attr(partsAndRules);
	partitionState->result_partition_array_size = resultPartSize;

	return partitionState;
}

/*
 * InitializeResultRelations
 *   Initialize result relation relevant information
 *
 * CDB: Note that we need this info even if we aren't the slice that will be doing
 * the actual updating, since it's where we learn things, such as if the row needs to
 * contain OIDs or not.
 */
	static void
InitializeResultRelations(PlannedStmt *plannedstmt, EState *estate, CmdType operation, int eflags)
{
	Assert(plannedstmt != NULL && estate != NULL);

	if (plannedstmt->resultRelations == NULL)
	{
		/*
		 * if no result relation, then set state appropriately
		 */
		estate->es_result_relations = NULL;
		estate->es_num_result_relations = 0;
		estate->es_result_relation_info = NULL;
		estate->es_result_partitions = NULL;
		estate->es_result_aosegnos = NIL;

		return;
	}

	List	   *resultRelations = plannedstmt->resultRelations;
	int			numResultRelations = list_length(resultRelations);
	ResultRelInfo *resultRelInfos;

	List *rangeTable = estate->es_range_table;

	if (numResultRelations > 1)
	{
		/*
		 * Multiple result relations (due to inheritance)
		 * stmt->resultRelations identifies them all
		 */
		ResultRelInfo *resultRelInfo;

		resultRelInfos = (ResultRelInfo *)
			palloc(numResultRelations * sizeof(ResultRelInfo));
		resultRelInfo = resultRelInfos;
		ListCell *lc = NULL;
		foreach(lc, resultRelations)
		{
			initResultRelInfo(resultRelInfo,
					lfirst_int(lc),
					rangeTable,
					operation,
					estate->es_instrument,
					(Gp_role != GP_ROLE_EXECUTE || Gp_is_writer));

			resultRelInfo++;
		}
	}
	else
	{
		/*
		 * Single result relation identified by stmt->queryTree->resultRelation
		 */
		numResultRelations = 1;
		resultRelInfos = (ResultRelInfo *) palloc(sizeof(ResultRelInfo));
		initResultRelInfo(resultRelInfos,
				linitial_int(plannedstmt->resultRelations),
				rangeTable,
				operation,
				estate->es_instrument,
				(Gp_role != GP_ROLE_EXECUTE || Gp_is_writer));
	}

	/*
	 * In some occasions when inserting data into a target relations we
	 * need to pass some specific information from the QD to the QEs.
	 * we do this information exchange here, via the parseTree. For now
	 * this is used for partitioned and append-only tables.
	 */

	estate->es_result_relations = resultRelInfos;
	estate->es_num_result_relations = numResultRelations;
	/* Initialize to first or only result rel */
	estate->es_result_relation_info = resultRelInfos;

	if (Gp_role == GP_ROLE_EXECUTE)
	{
		estate->es_result_partitions = plannedstmt->result_partitions;
		estate->es_result_aosegnos = plannedstmt->result_aosegnos;
		estate->es_result_segfileinfos = plannedstmt->result_segfileinfos;
	}
	else
	{
		List 	*all_relids = NIL;
		Oid		 relid = getrelid(linitial_int(plannedstmt->resultRelations), rangeTable);

		all_relids = lappend_oid(all_relids, relid);
		estate->es_result_partitions = BuildPartitionNodeFromRoot(relid);

		if (rel_is_partitioned(relid))
		    all_relids = list_concat(all_relids, all_partition_relids(estate->es_result_partitions));
		
		estate->es_result_aosegnos = assignPerRelSegno(all_relids, GetQEGangNum());
        
		plannedstmt->result_partitions = estate->es_result_partitions;
		plannedstmt->result_aosegnos = estate->es_result_aosegnos;

		/* Set any QD resultrels segno, just in case. The QEs set their own in ExecInsert(). */
		int relno = 0;
		ResultRelInfo* relinfo;
		for (relno = 0; relno < numResultRelations; relno ++)
		{
			relinfo = &(resultRelInfos[relno]);
			ResultRelInfoSetSegno(relinfo, estate->es_result_aosegnos);
		}

		ListCell *cell;
		foreach(cell, all_relids)
		{
			Oid relid =  lfirst_oid(cell);
			if (!(eflags & EXEC_FLAG_EXPLAIN_ONLY))
			{
			  CreateAppendOnlyParquetSegFileOnMaster(relid, estate->es_result_aosegnos);
			}
		}

	}

	estate->es_partition_state = NULL;
	if (estate->es_result_partitions)
	{
		estate->es_partition_state = createPartitionState(estate->es_result_partitions,
				estate->es_num_result_relations);
	}
}

/*
 * InitializeQueryPartsMetadata
 *   Initialize partitioning metadata for all partitions involved in the query.
 */
	static void
InitializeQueryPartsMetadata(PlannedStmt *plannedstmt, EState *estate)
{
	Assert(plannedstmt != NULL && estate != NULL);

	if (plannedstmt->queryPartOids == NIL)
	{
		plannedstmt->queryPartsMetadata = NIL;
		return;
	}

	if (Gp_role != GP_ROLE_EXECUTE)
	{
		/*
		 * Non-QEs populate the partitioning metadata for all
		 * relevant partitions in the query.
		 */
		plannedstmt->queryPartsMetadata = NIL;
		ListCell *lc = NULL;
		foreach (lc, plannedstmt->queryPartOids)
		{
			Oid relid = (Oid)lfirst_oid(lc);
			PartitionNode *partitionNode = BuildPartitionNodeFromRoot(relid);
			Assert(partitionNode != NULL);
			plannedstmt->queryPartsMetadata =
				lappend(plannedstmt->queryPartsMetadata, partitionNode);
		}
	}

	/* Populate the partitioning metadata to EState */
	Assert(estate->dynamicTableScanInfo != NULL &&
			estate->dynamicTableScanInfo->memoryContext != NULL);

	MemoryContext oldContext = MemoryContextSwitchTo(estate->dynamicTableScanInfo->memoryContext);

	ListCell *lc = NULL;
	foreach(lc, plannedstmt->queryPartsMetadata)
	{
		PartitionNode *partsAndRules = (PartitionNode *)lfirst(lc);

		PartitionMetadata *metadata = palloc(sizeof(PartitionMetadata));
		metadata->partsAndRules = partsAndRules;
		Assert(metadata->partsAndRules != NULL);
		metadata->accessMethods = createPartitionAccessMethods(num_partition_levels(metadata->partsAndRules));
		estate->dynamicTableScanInfo->partsMetadata =
			lappend(estate->dynamicTableScanInfo->partsMetadata, metadata);
	}

	MemoryContextSwitchTo(oldContext);
}

/*
 * InitializePartsMetadata
 *   Initialize partitioning metadata for the given partitioned table oid
 */
List *
InitializePartsMetadata(Oid rootOid)
{
	PartitionMetadata *metadata = palloc(sizeof(PartitionMetadata));
	metadata->partsAndRules = BuildPartitionNodeFromRoot(rootOid);
	Assert(metadata->partsAndRules != NULL);

	metadata->accessMethods = createPartitionAccessMethods(num_partition_levels(metadata->partsAndRules));
	return list_make1(metadata);
}

/* ----------------------------------------------------------------
 *		InitPlan
 *
 *		Initializes the query plan: open files, allocate storage
 *		and start up the rule manager
 * ----------------------------------------------------------------
 */
static void
InitPlan(QueryDesc *queryDesc, int eflags)
{
	CmdType		operation = queryDesc->operation;
	PlannedStmt *plannedstmt = queryDesc->plannedstmt;
	Plan	   *plan = plannedstmt->planTree;
	List	   *rangeTable = plannedstmt->rtable;
	EState	   *estate = queryDesc->estate;
	PlanState  *planstate;
	TupleDesc	tupType;
	ListCell   *l;
	bool		shouldDispatch = Gp_role == GP_ROLE_DISPATCH && plannedstmt->planTree->dispatch == DISPATCH_PARALLEL;

	Assert(plannedstmt->intoPolicy == NULL
			|| plannedstmt->intoPolicy->ptype == POLICYTYPE_PARTITIONED);

	if (DEBUG1 >= log_min_messages)
	{
		char msec_str[32];
		switch (check_log_duration(msec_str, false))
		{
			case 1:
			case 2:
				ereport(LOG, (errmsg("duration to InitPlan start: %s ms", msec_str),
							errOmitLocation(true)));
				break;
			default:
				/* do nothing */
				break;
		}
	}

	/*
	 * Do permissions checks.  It's sufficient to examine the query's top
	 * rangetable here --- subplan RTEs will be checked during
	 * ExecInitSubPlan().
	 */
	if (operation != CMD_SELECT ||
			(Gp_role != GP_ROLE_EXECUTE &&
			 !(shouldDispatch && cdbpathlocus_querysegmentcatalogs)))
	{
		ExecCheckRTPerms(plannedstmt->rtable);
	}
	else
	{
		/*
		 * We don't check the rights here, so we can query pg_statistic even if we are a non-privileged user.
		 * This shouldn't cause a problem, because "cdbpathlocus_querysegmentcatalogs" can only be true if we
		 * are doing special catalog queries for ANALYZE.  Otherwise, the QD will execute the normal access right
		 * check.  This does open a security hole, as it's possible for a hacker to connect to a segdb with GP_ROLE_EXECUTE,
		 * (at least, in theory, although it isn't easy) and then do a query.  But all they can see is
		 * pg_statistic and pg_class, and pg_class is normally readable by everyone.
		 */

		ListCell *lc = NULL;

		foreach(lc, plannedstmt->rtable)
		{
			RangeTblEntry *rte = lfirst(lc);

			if (rte->rtekind != RTE_RELATION)
				continue;

			if (rte->requiredPerms == 0)
				continue;

			/*
			 * Ignore access rights check on pg_statistic and pg_class, so
			 * the QD can retreive the statistics from the QEs.
			 */
			if (rte->relid != StatisticRelationId && rte->relid != RelationRelationId)
			{
				ExecCheckRTEPerms(rte);
			}
		}
	}

	/*
	 * get information from query descriptor
	 */
	rangeTable = plannedstmt->rtable;

	/*
	 * initialize the node's execution state
	 */
	estate->es_range_table = rangeTable;

	/*
	 * if there is a result relation, initialize result relation stuff
	 *
	 * CDB: Note that we need this info even if we aren't the slice that will be doing
	 * the actual updating, since it's where we learn things, such as if the row needs to
	 * contain OIDs or not.
	 */
	InitializeResultRelations(plannedstmt, estate, operation, eflags);

	/*
	 * If there are partitions involved in the query, initialize partitioning metadata.
	 */
	InitializeQueryPartsMetadata(plannedstmt, estate);

	/*
	 * set the number of partition selectors for every dynamic scan id
	 */
	estate->dynamicTableScanInfo->numSelectorsPerScanId = plannedstmt->numSelectorsPerScanId;

	/*
	 * Detect whether we're doing SELECT INTO.  If so, set the es_into_oids
	 * flag appropriately so that the plan tree will be initialized with the
	 * correct tuple descriptors.  (Other SELECT INTO stuff comes later.)
	 */
	estate->es_select_into = false;
	if (operation == CMD_SELECT && plannedstmt->intoClause != NULL)
	{
		estate->es_select_into = true;
		estate->es_into_oids = interpretOidsOption(plannedstmt->intoClause->options);
	}

	/*
	 * Have to lock relations selected FOR UPDATE/FOR SHARE before we
	 * initialize the plan tree, else we'd be doing a lock upgrade. While we
	 * are at it, build the ExecRowMark list.
	 */
	estate->es_rowMarks = NIL;
	foreach(l, plannedstmt->rowMarks)
	{
		RowMarkClause *rc = (RowMarkClause *) lfirst(l);
		Oid			relid = getrelid(rc->rti, rangeTable);
		Relation	relation;
		LOCKMODE    lockmode;
		bool        lockUpgraded;
		ExecRowMark *erm;

		/* CDB: On QD, lock whole table in S or X mode, if distributed. */
		lockmode = rc->forUpdate ? RowExclusiveLock : RowShareLock;
		relation = CdbOpenRelation(relid, lockmode, rc->noWait, &lockUpgraded);
		if (lockUpgraded)
		{
			heap_close(relation, NoLock);
			continue;
		}

		erm = (ExecRowMark *) palloc(sizeof(ExecRowMark));
		erm->relation = relation;
		erm->rti = rc->rti;
		erm->forUpdate = rc->forUpdate;
		erm->noWait = rc->noWait;
		snprintf(erm->resname, sizeof(erm->resname), "ctid%u", rc->rti);
		estate->es_rowMarks = lappend(estate->es_rowMarks, erm);
	}

	/*
	 * Initialize the executor "tuple" table.  We need slots for all the plan
	 * nodes, plus possibly output slots for the junkfilter(s). At this point
	 * we aren't sure if we need junkfilters, so just add slots for them
	 * unconditionally.  Also, if it's not a SELECT, set up a slot for use for
	 * trigger output tuples.  Also, one for RETURNING-list evaluation.
	 */
	{
		/* Slots for the main plan tree */
		int			nSlots = ExecCountSlotsNode(plannedstmt->planTree);

		/* 
		 * Note that, here, PG loops over the subplans in PlannedStmt, counts 
		 * the slots in each, and allocates the slots in the top-level state's 
		 * tuple table.  GP operates differently.  It uses the subplan code in 
		 * nodeSubplan.c to count slots for the subplan and to allocate them 
		 * in the subplan's state.
		 * 
		 * Since every slice does this, GP may over-allocate tuple slots, 
		 * however, the cost is small (about 80 bytes per entry) and probably
		 * worth the simplicity of the approach.
		 */

		/* Add slots for junkfilter(s) */
		if (plannedstmt->resultRelations != NIL)
			nSlots += list_length(plannedstmt->resultRelations);
		else
			nSlots += 1;
		if (operation != CMD_SELECT)
			nSlots++;			/* for es_trig_tuple_slot */
		if (plannedstmt->returningLists)
			nSlots++;			/* for RETURNING projection */

		estate->es_tupleTable = ExecCreateTupleTable(nSlots);

		if (operation != CMD_SELECT)
			estate->es_trig_tuple_slot =
				ExecAllocTableSlot(estate->es_tupleTable);
	}

	/* mark EvalPlanQual not active */
	estate->es_plannedstmt = plannedstmt;
	estate->es_evalPlanQual = NULL;
	estate->es_evTupleNull = NULL;
	estate->es_evTuple = NULL;
	estate->es_useEvalPlan = false;

	/*
	 * Initialize the private state information for all the nodes in the query
	 * tree.  This opens files, allocates storage and leaves us ready to start
	 * processing tuples.
	 */
	planstate = ExecInitNode(plannedstmt->planTree, estate, eflags);

	queryDesc->planstate = planstate;

	Assert(queryDesc->planstate);

	if (RootSliceIndex(estate) != LocallyExecutingSliceIndex(estate))
		return;

	/*
	 * Get the tuple descriptor describing the type of tuples to return. (this
	 * is especially important if we are creating a relation with "SELECT
	 * INTO")
	 */
	tupType = ExecGetResultType(planstate);

	/*
	 * Initialize the junk filter if needed.  SELECT and INSERT queries need a
	 * filter if there are any junk attrs in the tlist.  INSERT and SELECT
	 * INTO also need a filter if the plan may return raw disk tuples (else
	 * heap_insert will be scribbling on the source relation!). UPDATE and
	 * DELETE always need a filter, since there's always a junk 'ctid'
	 * attribute present --- no need to look first.
	 *
	 * This section of code is also a convenient place to verify that the
	 * output of an INSERT or UPDATE matches the target table(s).
	 */
	{
		bool		junk_filter_needed = false;
		ListCell   *tlist;

		switch (operation)
		{
			case CMD_SELECT:
			case CMD_INSERT:
				foreach(tlist, plan->targetlist)
				{
					TargetEntry *tle = (TargetEntry *) lfirst(tlist);

					if (tle->resjunk)
					{
						junk_filter_needed = true;
						break;
					}
				}
				if (!junk_filter_needed &&
						(operation == CMD_INSERT || estate->es_select_into) &&
						ExecMayReturnRawTuples(planstate))
					junk_filter_needed = true;

				break;
			case CMD_UPDATE:
			case CMD_DELETE:
				junk_filter_needed = true;
				break;
			default:
				break;
		}

		if (junk_filter_needed)
		{
			/*
			 * If there are multiple result relations, each one needs its own
			 * junk filter.  Note this is only possible for UPDATE/DELETE, so
			 * we can't be fooled by some needing a filter and some not.
			 */

			if (list_length(plannedstmt->resultRelations) > 1)
			{
				List *appendplans;
				int			as_nplans;
				ResultRelInfo *resultRelInfo;
				ListCell *lc;

				/* Top plan had better be an Append here. */
				Assert(IsA(plannedstmt->planTree, Append));
				Assert(((Append *) plannedstmt->planTree)->isTarget);
				Assert(IsA(planstate, AppendState));
				appendplans = ((Append *) plannedstmt->planTree)->appendplans;
				as_nplans = list_length(appendplans);
				Assert(as_nplans == estate->es_num_result_relations);
				resultRelInfo = estate->es_result_relations;
				foreach(lc, appendplans)
				{
					Plan  *subplan = (Plan *)lfirst(lc);
					JunkFilter *j;

					if (operation == CMD_UPDATE && PLANGEN_PLANNER == plannedstmt->planGen)
						ExecCheckPlanOutput(resultRelInfo->ri_RelationDesc,
								subplan->targetlist);

					TupleDesc cleanTupType = ExecCleanTypeFromTL(subplan->targetlist, 
							resultRelInfo->ri_RelationDesc->rd_att->tdhasoid);
					j = ExecInitJunkFilter(subplan->targetlist,
							cleanTupType,
							ExecAllocTableSlot(estate->es_tupleTable));
					resultRelInfo->ri_junkFilter = j;
					resultRelInfo++;
				}

				/*
				 * Set active junkfilter too; at this point ExecInitAppend has
				 * already selected an active result relation...
				 */
				estate->es_junkFilter =
					estate->es_result_relation_info->ri_junkFilter;
			}
			else
			{

				/* Normal case with just one JunkFilter */
				JunkFilter *j;

				if (PLANGEN_PLANNER == plannedstmt->planGen && (operation == CMD_INSERT || operation == CMD_UPDATE))
					ExecCheckPlanOutput(estate->es_result_relation_info->ri_RelationDesc,
							planstate->plan->targetlist);

				TupleDesc cleanTupType = ExecCleanTypeFromTL(planstate->plan->targetlist, 
						tupType->tdhasoid);

				j = ExecInitJunkFilter(planstate->plan->targetlist,
						cleanTupType,
						ExecAllocTableSlot(estate->es_tupleTable));

				estate->es_junkFilter = j;
				if (estate->es_result_relation_info)
					estate->es_result_relation_info->ri_junkFilter = j;

				/* For SELECT, want to return the cleaned tuple type */
				if (operation == CMD_SELECT)
					tupType = j->jf_cleanTupType;
			}
		}
		else
		{
			/* The planner requires that top node of the target list has the same
			 * number of columns than the output relation. This is not a requirement
			 * of the Optimizer. */
			if (operation == CMD_INSERT
					&& plannedstmt->planGen == PLANGEN_PLANNER)
			{
				ExecCheckPlanOutput(estate->es_result_relation_info->ri_RelationDesc,
						planstate->plan->targetlist);
			}

			estate->es_junkFilter = NULL;
		}
	}

	/*
	 * Initialize RETURNING projections if needed.
	 */
	if (plannedstmt->returningLists)
	{
		TupleTableSlot *slot;
		ExprContext *econtext;
		ResultRelInfo *resultRelInfo;

		/*
		 * We set QueryDesc.tupDesc to be the RETURNING rowtype in this case.
		 * We assume all the sublists will generate the same output tupdesc.
		 */
		tupType = ExecTypeFromTL((List *) linitial(plannedstmt->returningLists),
				false);

		/* Set up a slot for the output of the RETURNING projection(s) */
		slot = ExecAllocTableSlot(estate->es_tupleTable);
		ExecSetSlotDescriptor(slot, tupType);
		/* Need an econtext too */
		econtext = CreateExprContext(estate);

		/*
		 * Build a projection for each result rel.	Note that any SubPlans in
		 * the RETURNING lists get attached to the topmost plan node.
		 */
		Assert(list_length(plannedstmt->returningLists) == estate->es_num_result_relations);
		resultRelInfo = estate->es_result_relations;
		foreach(l, plannedstmt->returningLists)
		{
			List	   *rlist = (List *) lfirst(l);
			List	   *rliststate;

			rliststate = (List *) ExecInitExpr((Expr *) rlist, planstate);
			resultRelInfo->ri_projectReturning =
				ExecBuildProjectionInfo(rliststate, econtext, slot,
						resultRelInfo->ri_RelationDesc->rd_att);
			resultRelInfo++;
		}

		/*
		 * Because we already ran ExecInitNode() for the top plan node, any
		 * subplans we just attached to it won't have been initialized; so we
		 * have to do it here.	(Ugly, but the alternatives seem worse.)
		 */
		foreach(l, planstate->subPlan)
		{
			SubPlanState *sstate = (SubPlanState *) lfirst(l);

			Assert(IsA(sstate, SubPlanState));
			if (sstate->planstate == NULL)		/* already inited? */
				ExecInitSubPlan(sstate, estate, eflags);
		}
	}

	queryDesc->tupDesc = tupType;

	/*
	 * If doing SELECT INTO, initialize the "into" relation.  We must wait
	 * till now so we have the "clean" result tuple type to create the new
	 * table from.
	 *
	 * If EXPLAIN, skip creating the "into" relation.
	 */
	if (estate->es_select_into && !(eflags & EXEC_FLAG_EXPLAIN_ONLY) &&
			/* Only create the table if root slice */
			(Gp_role != GP_ROLE_EXECUTE || Gp_is_writer) )
		OpenIntoRel(queryDesc);


	if (DEBUG1 >= log_min_messages)
	{
		char		msec_str[32];
		switch (check_log_duration(msec_str, false))
		{
			case 1:
			case 2:
				ereport(LOG, (errmsg("duration to InitPlan end: %s ms", msec_str),
							errOmitLocation(true)));
				break;
		}
	}
}

/*
 * Initialize ResultRelInfo data for one result relation
 */
	static void
initResultRelInfo(ResultRelInfo *resultRelInfo,
		Index resultRelationIndex,
		List *rangeTable,
		CmdType operation,
		bool doInstrument,
		bool needLock)
{
	Oid			resultRelationOid;
	Relation	resultRelationDesc;
	LOCKMODE    lockmode;

	resultRelationOid = getrelid(resultRelationIndex, rangeTable);

	/*
	 * MPP-2879: The QEs don't pass their MPPEXEC statements through
	 * the parse (where locks would ordinarily get acquired). So we
	 * need to take some care to pick them up here (otherwise we get
	 * some very strange interactions with QE-local operations (vacuum?
	 * utility-mode ?)).
	 *
	 * NOTE: There is a comment in lmgr.c which reads forbids use of
	 * heap_open/relation_open with "NoLock" followed by use of
	 * RelationOidLock/RelationLock with a stronger lock-mode:
	 * RelationOidLock/RelationLock expect a relation to already be
	 * locked.
	 *
	 * But we also need to serialize CMD_UPDATE && CMD_DELETE to preserve
	 * order on mirrors.
	 *
	 * So we're going to ignore the "NoLock" issue above.
	 */
	/* CDB: we must promote locks for UPDATE and DELETE operations. */
	lockmode = needLock ? RowExclusiveLock : NoLock;
	if (operation == CMD_UPDATE || operation == CMD_DELETE)
	{
		resultRelationDesc = CdbOpenRelation(resultRelationOid,
				lockmode,
				false, /* noWait */ 
				NULL); /* lockUpgraded */
	}
	else
	{
		resultRelationDesc = heap_open(resultRelationOid, lockmode);
	}

	/*
	 * Check valid relkind ... parser and/or planner should have noticed this
	 * already, but let's make sure.
	 */
	if (!gp_upgrade_mode || Gp_role != GP_ROLE_DISPATCH)
	switch (resultRelationDesc->rd_rel->relkind)
	{
		case RELKIND_RELATION:
			/* OK */
			break;
		case RELKIND_SEQUENCE:
			ereport(ERROR,
					(errcode(ERRCODE_WRONG_OBJECT_TYPE),
					 errmsg("cannot change sequence \"%s\"",
						 RelationGetRelationName(resultRelationDesc))));
			break;
		case RELKIND_TOASTVALUE:
			ereport(ERROR,
					(errcode(ERRCODE_WRONG_OBJECT_TYPE),
					 errmsg("cannot change TOAST relation \"%s\"",
						 RelationGetRelationName(resultRelationDesc))));
			break;
		case RELKIND_AOSEGMENTS:
			ereport(ERROR,
					(errcode(ERRCODE_WRONG_OBJECT_TYPE),
					 errmsg("cannot change AO segment listing relation \"%s\"",
						 RelationGetRelationName(resultRelationDesc))));
			break;
		case RELKIND_AOBLOCKDIR:
			ereport(ERROR,
					(errcode(ERRCODE_WRONG_OBJECT_TYPE),
					 errmsg("cannot change AO block directory relation \"%s\"",
						 RelationGetRelationName(resultRelationDesc))));
			break;
		case RELKIND_VIEW:
			ereport(ERROR,
					(errcode(ERRCODE_WRONG_OBJECT_TYPE),
					 errmsg("cannot change view \"%s\"",
						 RelationGetRelationName(resultRelationDesc))));
			break;
	}

	/* OK, fill in the node */
	MemSet(resultRelInfo, 0, sizeof(ResultRelInfo));
	resultRelInfo->type = T_ResultRelInfo;
	resultRelInfo->ri_RangeTableIndex = resultRelationIndex;
	resultRelInfo->ri_RelationDesc = resultRelationDesc;
	resultRelInfo->ri_NumIndices = 0;
	resultRelInfo->ri_IndexRelationDescs = NULL;
	resultRelInfo->ri_IndexRelationInfo = NULL;

	/* make a copy so as not to depend on relcache info not changing... */
	resultRelInfo->ri_TrigDesc = CopyTriggerDesc(resultRelationDesc->trigdesc);
	if (resultRelInfo->ri_TrigDesc)
	{
		int			n = resultRelInfo->ri_TrigDesc->numtriggers;

		resultRelInfo->ri_TrigFunctions = (FmgrInfo *)
			palloc0(n * sizeof(FmgrInfo));
		if (doInstrument)
			resultRelInfo->ri_TrigInstrument = InstrAlloc(n);
		else
			resultRelInfo->ri_TrigInstrument = NULL;
	}
	else
	{
		resultRelInfo->ri_TrigFunctions = NULL;
		resultRelInfo->ri_TrigInstrument = NULL;
	}
	resultRelInfo->ri_ConstraintExprs = NULL;
	resultRelInfo->ri_junkFilter = NULL;
	resultRelInfo->ri_projectReturning = NULL;
	resultRelInfo->ri_aoInsertDesc = NULL;
	resultRelInfo->ri_extInsertDesc = NULL;
	resultRelInfo->ri_aosegnos = NIL;

	/*
	 * If there are indices on the result relation, open them and save
	 * descriptors in the result relation info, so that we can add new index
	 * entries for the tuples we add/update.  We need not do this for a
	 * DELETE, however, since deletion doesn't affect indexes.
	 */
	if (needLock) /* only needed by the root slice who will do the actual updating */
		if (resultRelationDesc->rd_rel->relhasindex &&
				operation != CMD_DELETE)
			ExecOpenIndices(resultRelInfo);
}

	void
CreateAppendOnlyParquetSegFileOnMaster(Oid relid, List *mapping)
{
	ListCell *relid_to_segno;
	bool	  found = false;

	Relation rel = heap_open(relid, AccessShareLock);

	/* only relevant for AO relations */
	if(!RelationIsAoRows(rel)  && !RelationIsParquet(rel))
	{
		heap_close(rel, AccessShareLock);
		return;
	}

	Assert(mapping);

	/* lookup the segfile # to write into, according to my relid */
	Oid myrelid = RelationGetRelid(rel);
	foreach(relid_to_segno, mapping)
	{
		SegfileMapNode *n = (SegfileMapNode *)lfirst(relid_to_segno);

		if(n->relid == myrelid)
		{
			Assert(n->segnos != NIL);

			/*
			 * in hawq, master create all segfile for segments
			 */
			if (Gp_role == GP_ROLE_DISPATCH)
				CreateAppendOnlyParquetSegFileForRelationOnMaster(rel, n->segnos);

			found = true;
			break;
		}
	}

	heap_close(rel, AccessShareLock);

	Assert(found);
}

	static void
CreaateAoRowSegFileForRelationOnMaster(Relation rel,
		AppendOnlyEntry * aoEntry, List *segnos, SharedStorageOpTasks *addTask, SharedStorageOpTasks *overwriteTask)
{
	FileSegInfo * fsinfo;
	ListCell *cell;

	Relation gp_relfile_node;
	HeapTuple tuple;

	ItemPointerData persistentTid;
	int64 persistentSerialNum;

	Assert(RelationIsAoRows(rel));

	char * relname = RelationGetRelationName(rel);

	gp_relfile_node = heap_open(GpRelfileNodeRelationId, AccessShareLock);

	foreach(cell, segnos)
	{
		int segno = lfirst_int(cell);
		fsinfo = GetFileSegInfo(rel, aoEntry, SnapshotNow, segno);

		if (NULL == fsinfo)
		{
			InsertInitialSegnoEntry(aoEntry, segno);
		}
		else if (fsinfo->eof != 0)
		{
			pfree(fsinfo);
			continue;
		}

		if (fsinfo)
		{
			pfree(fsinfo);
		}

		tuple = FetchGpRelfileNodeTuple(
					gp_relfile_node,
					rel->rd_node.relNode,
					segno,
					&persistentTid,
					&persistentSerialNum);

		if (HeapTupleIsValid(tuple))
		{
			bool currentTspSupportTruncate = false;

			if (filesystem_support_truncate)
					currentTspSupportTruncate = TestCurrentTspSupportTruncate(rel->rd_node.spcNode);

			heap_freetuple(tuple);

			/*
			 * here is a record in persistent table, we assume the file exist on filesystem.
			 * but there is no record in pg_aoseg_xxx catalog.
			 * We should overwrite that file in case that the file system do not support truncate.
			 */
			if (!currentTspSupportTruncate)
                SharedStorageOpAddTask(relname, &rel->rd_node, segno,
                        &persistentTid,
                        persistentSerialNum,
                        overwriteTask);

			continue;
		}

		SharedStorageOpPreAddTask(&rel->rd_node, segno, relname,
								  &persistentTid,
								  &persistentSerialNum);

		SharedStorageOpAddTask(relname, &rel->rd_node, segno,
								&persistentTid,
								persistentSerialNum,
								addTask);
	}

	heap_close(gp_relfile_node, AccessShareLock);
}


static void
CreateParquetSegFileForRelationOnMaster(Relation rel,
		AppendOnlyEntry *aoEntry, List *segnos, SharedStorageOpTasks *addTasks, SharedStorageOpTasks *overwriteTask)
{
	ParquetFileSegInfo * fsinfo;
	ListCell *cell;

	Relation gp_relfile_node;
	HeapTuple tuple;

	ItemPointerData persistentTid;
	int64 persistentSerialNum;

	Assert(RelationIsParquet(rel));

	char * relname = RelationGetRelationName(rel);

	gp_relfile_node = heap_open(GpRelfileNodeRelationId, AccessShareLock);

	foreach(cell, segnos)
	{
		int segno = lfirst_int(cell);
		fsinfo = GetParquetFileSegInfo(rel, aoEntry, SnapshotNow, segno);

		if (NULL == fsinfo)
		{
			InsertInitialParquetSegnoEntry(aoEntry, segno);
		}
		else if (fsinfo->eof != 0)
		{
			pfree(fsinfo);
			continue;
		}

		if (fsinfo)
		{
			pfree(fsinfo);
		}

		tuple = FetchGpRelfileNodeTuple(
					gp_relfile_node,
					rel->rd_node.relNode,
					segno,
					&persistentTid,
					&persistentSerialNum);

		if (HeapTupleIsValid(tuple))
		{
            bool currentTspSupportTruncate = false;

			if (filesystem_support_truncate)
				currentTspSupportTruncate = TestCurrentTspSupportTruncate(rel->rd_node.spcNode);

			heap_freetuple(tuple);

			/*
			 * here is a record in persistent table, we assume the file exist on filesystem.
			 * but there is no record in pg_aoseg_xxx catalog.
			 * We should overwrite that file in case that the file system do not support truncate.
			 */
            if (!currentTspSupportTruncate)
                SharedStorageOpAddTask(relname, &rel->rd_node, segno,
                        &persistentTid,
                        persistentSerialNum,
                        overwriteTask);
            continue;
		}

		SharedStorageOpPreAddTask(&rel->rd_node, segno, relname,
								  &persistentTid,
								  &persistentSerialNum);

		SharedStorageOpAddTask(relname, &rel->rd_node, segno,
								&persistentTid,
								persistentSerialNum,
								addTasks);
	}

	heap_close(gp_relfile_node, AccessShareLock);
}

void
CreateAppendOnlyParquetSegFileForRelationOnMaster(Relation rel, List *segnos)
{
	SharedStorageOpTasks *addTasks = CreateSharedStorageOpTasks();
	SharedStorageOpTasks *overwriteTasks = CreateSharedStorageOpTasks();


	if(RelationIsAoRows(rel) || RelationIsParquet(rel))
	{
		AppendOnlyEntry *aoEntry = GetAppendOnlyEntry(rel->rd_id, SnapshotNow);

		if(RelationIsAoRows(rel))
			CreaateAoRowSegFileForRelationOnMaster(rel, aoEntry, segnos, addTasks, overwriteTasks);
		else
			CreateParquetSegFileForRelationOnMaster(rel, aoEntry, segnos, addTasks, overwriteTasks);

		pfree(aoEntry);
	}

	PerformSharedStorageOpTasks(addTasks, Op_CreateSegFile);
	PostPerformSharedStorageOpTasks(addTasks);
  PerformSharedStorageOpTasks(overwriteTasks, Op_OverWriteSegFile);
	DropSharedStorageOpTasks(addTasks);
  DropSharedStorageOpTasks(overwriteTasks);
}

/*
 * ResultRelInfoSetSegno
 *
 * based on a list of relid->segno mapping, look for our own resultRelInfo
 * relid in the mapping and find the segfile number that this resultrel should
 * use if it is inserting into an AO relation. for any non AO relation this is
 * irrelevant and will return early.
 *
 * Note that we rely on the fact that the caller has a well constructed mapping
 * and that it includes all the relids of *any* AO relation that may insert
 * data during this transaction. For non partitioned tables the mapping list
 * will have only one element - our table. for partitioning it may have
 * multiple (depending on how many partitions are AO).
 *
 */
	void
ResultRelInfoSetSegno(ResultRelInfo *resultRelInfo, List *mapping)
{
	ListCell *relid_to_segno;
	bool	  found = false;

	/* only relevant for AO relations */
	if(!relstorage_is_ao(RelinfoGetStorage(resultRelInfo)))
		return;

	Assert(mapping);
	Assert(resultRelInfo->ri_RelationDesc);

	/* lookup the segfile # to write into, according to my relid */

	foreach(relid_to_segno, mapping)
	{
		SegfileMapNode *n = (SegfileMapNode *)lfirst(relid_to_segno);
		Oid myrelid = RelationGetRelid(resultRelInfo->ri_RelationDesc);
		if(n->relid == myrelid)
		{
			Assert(n->segnos != NIL);
			resultRelInfo->ri_aosegnos = n->segnos;
			found = true;
			break;
		}
	}

	Assert(found);
}

void
ResultRelInfoSetSegFileInfo(ResultRelInfo *resultRelInfo, List *mapping)
{
	ListCell *relid_to_segfileinfo;
	bool found = false;
	Oid myrelid;

	/*
	 * Only relevant for AO relations.
	 */
	if (!relstorage_is_ao(RelinfoGetStorage(resultRelInfo)))
	{
		return;
	}

	Assert(mapping);
	Assert(resultRelInfo->ri_RelationDesc);

	myrelid = RelationGetRelid(resultRelInfo->ri_RelationDesc);

	/*
	 * Lookup the segment file to write into, according to
	 * myrelid.
	 */
	foreach (relid_to_segfileinfo, mapping)
	{
		ResultRelSegFileInfoMapNode *n = (ResultRelSegFileInfoMapNode *)lfirst(relid_to_segfileinfo);
		if (n->relid == myrelid)
		{
			Assert(n->segfileinfos != NIL);
			resultRelInfo->ri_aosegfileinfos = n->segfileinfos;
			found = true;
			break;
		}
	}

	Assert(found);
}

ResultRelSegFileInfo *
InitResultRelSegFileInfo(int segno, char storageChar, int numfiles)
{
	ResultRelSegFileInfo *result = makeNode(ResultRelSegFileInfo);
	result->segno = segno;
	result->numfiles = numfiles;
	Assert(result->numfiles > 0);
	if ((storageChar == RELSTORAGE_AOROWS) || (storageChar == RELSTORAGE_PARQUET))
	{
		Assert(result->numfiles == 1);
	}
	result->eof = palloc0(sizeof(int64) * result->numfiles);
	result->uncompressed_eof = palloc0(sizeof(int64) * result->numfiles);

	return result;
}

/*
 *		ExecContextForcesOids
 *
 * This is pretty grotty: when doing INSERT, UPDATE, or SELECT INTO,
 * we need to ensure that result tuples have space for an OID iff they are
 * going to be stored into a relation that has OIDs.  In other contexts
 * we are free to choose whether to leave space for OIDs in result tuples
 * (we generally don't want to, but we do if a physical-tlist optimization
 * is possible).  This routine checks the plan context and returns TRUE if the
 * choice is forced, FALSE if the choice is not forced.  In the TRUE case,
 * *hasoids is set to the required value.
 *
 * One reason this is ugly is that all plan nodes in the plan tree will emit
 * tuples with space for an OID, though we really only need the topmost node
 * to do so.  However, node types like Sort don't project new tuples but just
 * return their inputs, and in those cases the requirement propagates down
 * to the input node.  Eventually we might make this code smart enough to
 * recognize how far down the requirement really goes, but for now we just
 * make all plan nodes do the same thing if the top level forces the choice.
 *
 * We assume that estate->es_result_relation_info is already set up to
 * describe the target relation.  Note that in an UPDATE that spans an
 * inheritance tree, some of the target relations may have OIDs and some not.
 * We have to make the decisions on a per-relation basis as we initialize
 * each of the child plans of the topmost Append plan.
 *
 * SELECT INTO is even uglier, because we don't have the INTO relation's
 * descriptor available when this code runs; we have to look aside at a
 * flag set by InitPlan().
 */
bool
ExecContextForcesOids(PlanState *planstate, bool *hasoids)
{
	if (planstate->state->es_select_into)
	{
		*hasoids = planstate->state->es_into_oids;
		return true;
	}
	else
	{
		ResultRelInfo *ri = planstate->state->es_result_relation_info;

		if (ri != NULL)
		{
			Relation	rel = ri->ri_RelationDesc;

			if (rel != NULL)
			{
				*hasoids = rel->rd_rel->relhasoids;
				return true;
			}
		}
	}

	return false;
}

void
SendAOTupCounts(EState *estate)
{
	/*
	 * If we're inserting into partitions, send tuple counts for
	 * AO tables back to the QD.
	 */
	if (Gp_role == GP_ROLE_EXECUTE && estate->es_result_partitions)
	{
		StringInfoData buf;
		ResultRelInfo *resultRelInfo;
		int aocount = 0;
		int i;

		resultRelInfo = estate->es_result_relations;
		for (i = 0; i < estate->es_num_result_relations; i++)
		{
			if (relstorage_is_ao(RelinfoGetStorage(resultRelInfo)))
				aocount++;

			resultRelInfo++;
		}


		if (aocount)
		{
			if (Debug_appendonly_print_insert)
				ereport(LOG,(errmsg("QE sending tuple counts of %d partitioned "
								"AO relations... ", aocount)));

			pq_beginmessage(&buf, 'o');
			pq_sendint(&buf, aocount, 4);

			resultRelInfo = estate->es_result_relations;
			for (i = 0; i < estate->es_num_result_relations; i++)
			{
				if (relstorage_is_ao(RelinfoGetStorage(resultRelInfo)))
				{
					Oid relid = RelationGetRelid(resultRelInfo->ri_RelationDesc);
					uint64 tupcount = resultRelInfo->ri_aoprocessed;

					pq_sendint(&buf, relid, 4);
					pq_sendint64(&buf, tupcount);

					if (Debug_appendonly_print_insert)
						ereport(LOG,(errmsg("sent tupcount " INT64_FORMAT " for "
										"relation %d", tupcount, relid),
									errOmitLocation(true)));

				}
				resultRelInfo++;
			}
			pq_endmessage(&buf);
		}
	}

}
/* ----------------------------------------------------------------
 *		ExecEndPlan
 *
 *		Cleans up the query plan -- closes files and frees up storage
 *
 * NOTE: we are no longer very worried about freeing storage per se
 * in this code; FreeExecutorState should be guaranteed to release all
 * memory that needs to be released.  What we are worried about doing
 * is closing relations and dropping buffer pins.  Thus, for example,
 * tuple tables must be cleared or dropped to ensure pins are released.
 * ----------------------------------------------------------------
 */
void
ExecEndPlan(PlanState *planstate, EState *estate)
{
	ResultRelInfo *resultRelInfo;
	int			i;
	ListCell   *l;

	int aocount = 0;

	/*
	 * shut down any PlanQual processing we were doing
	 */
	if (estate->es_evalPlanQual != NULL)
		EndEvalPlanQual(estate);

	if (planstate != NULL)
		ExecEndNode(planstate);

	ExecDropTupleTable(estate->es_tupleTable, true);
	estate->es_tupleTable = NULL;

	/* Report how many tuples we may have inserted into AO tables */
	SendAOTupCounts(estate);

	StringInfo buf = NULL;

	resultRelInfo = estate->es_result_relations;
	for (i = 0; i < estate->es_num_result_relations; i++)
	{
		if (resultRelInfo->ri_aoInsertDesc)
			++aocount;
		if (resultRelInfo->ri_parquetInsertDesc || resultRelInfo->ri_parquetSendBack)
			++aocount;
		resultRelInfo++;
	}

	if (Gp_role == GP_ROLE_EXECUTE && aocount > 0)
		buf = PreSendbackChangedCatalog(aocount);

	/*
	 * close the result relation(s) if any, but hold locks until xact commit.
	 */
	resultRelInfo = estate->es_result_relations;
	for (i = 0; i < estate->es_num_result_relations; i++)
	{
		QueryContextDispatchingSendBack sendback = NULL;

		/* end (flush) the INSERT operation in the access layer */
		if (resultRelInfo->ri_aoInsertDesc)
		{
			sendback = CreateQueryContextDispatchingSendBack(1);
			resultRelInfo->ri_aoInsertDesc->sendback = sendback;
			sendback->relid = RelationGetRelid(resultRelInfo->ri_RelationDesc);

			appendonly_insert_finish(resultRelInfo->ri_aoInsertDesc);
		}

		/*need add processing for parquet insert desc*/
		if (resultRelInfo->ri_parquetInsertDesc){

			AssertImply(resultRelInfo->ri_parquetSendBack, gp_parquet_insert_sort);

			if (NULL != resultRelInfo->ri_parquetSendBack)
			{
				/*
				 * The Parquet part we just finished inserting into already
				 * has sendBack information. This means we're inserting into the
				 * part twice, which is not supported. Error out (GPSQL-2291)
				 */
				ereport(ERROR, (errcode(ERRCODE_CDB_FEATURE_NOT_YET),
							errmsg("Cannot insert out-of-order tuples in parquet partitions"),
							errhint("Sort the data on the partitioning key(s) before inserting"),
							errOmitLocation(true)));
			}

			sendback = CreateQueryContextDispatchingSendBack(1);
			resultRelInfo->ri_parquetInsertDesc->sendback = sendback;
			sendback->relid = RelationGetRelid(resultRelInfo->ri_RelationDesc);

			parquet_insert_finish(resultRelInfo->ri_parquetInsertDesc);
		}

		/*
		 * This can happen if we inserted into this parquet part then
		 * closed it during insertion. SendBack information is saved
		 * in the resultRelInfo, since the ri_parquetInsertDesc is freed
		 * (GPSQL-2291)
		 */
		if (NULL != resultRelInfo->ri_parquetSendBack)
		{
			Assert(NULL == sendback);
			sendback = resultRelInfo->ri_parquetSendBack;
		}

		if (resultRelInfo->ri_extInsertDesc)
			external_insert_finish(resultRelInfo->ri_extInsertDesc);

		if (resultRelInfo->ri_resultSlot)
		{
			Assert(resultRelInfo->ri_resultSlot->tts_tupleDescriptor);
			ReleaseTupleDesc(resultRelInfo->ri_resultSlot->tts_tupleDescriptor);
			ExecClearTuple(resultRelInfo->ri_resultSlot);
		}

		if (sendback && (relstorage_is_ao(RelinfoGetStorage(resultRelInfo)))
				&& Gp_role == GP_ROLE_EXECUTE && aocount > 0)
			AddSendbackChangedCatalogContent(buf, sendback);

		DropQueryContextDispatchingSendBack(sendback);

		/* Close indices and then the relation itself */
		ExecCloseIndices(resultRelInfo);
		heap_close(resultRelInfo->ri_RelationDesc, NoLock);
		resultRelInfo++;
	}

	if (Gp_role == GP_ROLE_EXECUTE && aocount > 0)
		FinishSendbackChangedCatalog(buf);

	/*
	 * close any relations selected FOR UPDATE/FOR SHARE, again keeping locks
	 */
	foreach(l, estate->es_rowMarks)
	{
		ExecRowMark *erm = lfirst(l);

		heap_close(erm->relation, NoLock);
	}

	/*
	 * Release partition-related resources (esp. TupleDesc ref counts).
	 */
	if ( estate->es_partition_state )
		ClearPartitionState(estate);
}

/*
 * Verify that the tuples to be produced by INSERT or UPDATE match the
 * target relation's rowtype
 *
 * We do this to guard against stale plans.  If plan invalidation is
 * functioning properly then we should never get a failure here, but better
 * safe than sorry.  Note that this is called after we have obtained lock
 * on the target rel, so the rowtype can't change underneath us.
 *
 * The plan output is represented by its targetlist, because that makes
 * handling the dropped-column case easier.
 */
static void
ExecCheckPlanOutput(Relation resultRel, List *targetList)
{
	TupleDesc	resultDesc = RelationGetDescr(resultRel);
	int			attno = 0;
	ListCell   *lc;

	/*
	 * Don't do this during dispatch because the plan is not suitable
	 * structured to meet these tests
	 */
	if (Gp_role == GP_ROLE_DISPATCH)
		return;

	foreach(lc, targetList)
	{
		TargetEntry *tle = (TargetEntry *) lfirst(lc);
		Form_pg_attribute attr;

		if (tle->resjunk)
			continue;			/* ignore junk tlist items */

		if (attno >= resultDesc->natts)
			ereport(ERROR,
					(errcode(ERRCODE_DATATYPE_MISMATCH),
					 errmsg("table row type and query-specified row type do not match"),
					 errdetail("Query has too many columns.")));
		attr = resultDesc->attrs[attno++];

		if (!attr->attisdropped)
		{
			/* Normal case: demand type match */
			if (exprType((Node *) tle->expr) != attr->atttypid)
				ereport(ERROR,
						(errcode(ERRCODE_DATATYPE_MISMATCH),
						 errmsg("table row type and query-specified row type do not match"),
						 errdetail("Table has type %s at ordinal position %d, but query expects %s.",
							 format_type_be(attr->atttypid),
							 attno,
							 format_type_be(exprType((Node *) tle->expr)))));
		}
		else
		{
			/*
			 * For a dropped column, we can't check atttypid (it's likely 0).
			 * In any case the planner has most likely inserted an INT4 null.
			 * What we insist on is just *some* NULL constant.
			 */
			if (!IsA(tle->expr, Const) ||
					!((Const *) tle->expr)->constisnull)
				ereport(ERROR,
						(errcode(ERRCODE_DATATYPE_MISMATCH),
						 errmsg("table row type and query-specified row type do not match"),
						 errdetail("Query provides a value for a dropped column at ordinal position %d.",
							 attno)));
		}
	}
	if (attno != resultDesc->natts)
		ereport(ERROR,
				(errcode(ERRCODE_DATATYPE_MISMATCH),
				 errmsg("table row type and query-specified row type do not match"),
				 errdetail("Query has too few columns.")));
}


/* ----------------------------------------------------------------
 *		ExecutePlan
 *
 *		processes the query plan to retrieve 'numberTuples' tuples in the
 *		direction specified.
 *
 *		Retrieves all tuples if numberTuples is 0
 *
 *		result is either a slot containing the last tuple in the case
 *		of a SELECT or NULL otherwise.
 *
 * Note: the ctid attribute is a 'junk' attribute that is removed before the
 * user can see it
 * ----------------------------------------------------------------
 */
static TupleTableSlot *
ExecutePlan(EState *estate,
		PlanState *planstate,
		CmdType operation,
		long numberTuples,
		ScanDirection direction,
		DestReceiver *dest)
{
	JunkFilter *junkfilter;
	TupleTableSlot *planSlot;
	TupleTableSlot *slot;
	ItemPointer tupleid = NULL;
	ItemPointerData tuple_ctid;
	long		current_tuple_count;
	TupleTableSlot *result;

	/*
	 * initialize local variables
	 */
	current_tuple_count = 0;
	result = NULL;

	/*
	 * Set the direction.
	 */
	estate->es_direction = direction;

	/*
	 * Process BEFORE EACH STATEMENT triggers
	 */
	if (Gp_role != GP_ROLE_EXECUTE || Gp_is_writer)
	{
		switch (operation)
		{
			case CMD_UPDATE:
				ExecBSUpdateTriggers(estate, estate->es_result_relation_info);
				break;
			case CMD_DELETE:
				ExecBSDeleteTriggers(estate, estate->es_result_relation_info);
				break;
			case CMD_INSERT:
				ExecBSInsertTriggers(estate, estate->es_result_relation_info);
				break;
			default:
				/* do nothing */
				break;
		}
	}

	/* Error out for unsupported updates */
	if (operation == CMD_UPDATE)
	{
		Assert(estate->es_result_relation_info->ri_RelationDesc);
		Relation rel = estate->es_result_relation_info->ri_RelationDesc;
		bool rel_is_aorows = RelationIsAoRows(rel);
		bool rel_is_parquet = RelationIsParquet(rel);
		
		if (rel_is_aorows || rel_is_parquet)
		{
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("Append-only/Parquet tables are not updatable. Operation not permitted."),
					 errOmitLocation(true)));
		}
	}

	/*
	 * Make sure slice dependencies are met
	 */
	ExecSliceDependencyNode(planstate);

	/*
	 * Loop until we've processed the proper number of tuples from the plan.
	 */
	for (;;)
	{
		/* Reset the per-output-tuple exprcontext */
		ResetPerTupleExprContext(estate);

		/*
		 * Execute the plan and obtain a tuple
		 */
lnext:	;
	if (estate->es_useEvalPlan)
	{
		planSlot = EvalPlanQualNext(estate);
		if (TupIsNull(planSlot))
			planSlot = ExecProcNode(planstate);
	}
	else
		planSlot = ExecProcNode(planstate);

	/*
	 * if the tuple is null, then we assume there is nothing more to
	 * process so we just return null...
	 */
	if (TupIsNull(planSlot))
	{
		result = NULL;
		break;
	}

	if (estate->es_plannedstmt->planGen == PLANGEN_PLANNER ||
			operation == CMD_SELECT)
	{

		slot = planSlot;

		/*
		 * If we have a junk filter, then project a new tuple with the junk
		 * removed.
		 *
		 * Store this new "clean" tuple in the junkfilter's resultSlot.
		 * (Formerly, we stored it back over the "dirty" tuple, which is WRONG
		 * because that tuple slot has the wrong descriptor.)
		 *
		 * But first, extract all the junk information we need.
		 */
		if ((junkfilter = estate->es_junkFilter) != NULL)
		{
			Datum		datum;
			bool		isNull;

			/*
			 * extract the 'ctid' junk attribute.
			 */
			if (operation == CMD_UPDATE || operation == CMD_DELETE)
			{
				if (!ExecGetJunkAttribute(junkfilter,
							slot,
							"ctid",
							&datum,
							&isNull))
					elog(ERROR, "could not find junk ctid column");

				/* shouldn't ever get a null result... */
				if (isNull)
					elog(ERROR, "ctid is NULL");

				tupleid = (ItemPointer) DatumGetPointer(datum);
				tuple_ctid = *tupleid;	/* make sure we don't free the ctid!! */
				tupleid = &tuple_ctid;
			}

			/*
			 * Process any FOR UPDATE or FOR SHARE locking requested.
			 */
			else if (estate->es_rowMarks != NIL)
			{
				ListCell   *l;

lmark:	;
	foreach(l, estate->es_rowMarks)
	{
		ExecRowMark *erm = lfirst(l);
		HeapTupleData tuple;
		Buffer		buffer;
		ItemPointerData update_ctid;
		TransactionId update_xmax;
		TupleTableSlot *newSlot;
		LockTupleMode lockmode;
		HTSU_Result test;

		/* CDB: CTIDs were not fetched for distributed relation. */
		Relation relation = erm->relation;
		if (relation->rd_cdbpolicy &&
				relation->rd_cdbpolicy->ptype == POLICYTYPE_PARTITIONED)
			continue;

		if (!ExecGetJunkAttribute(junkfilter,
					slot,
					erm->resname,
					&datum,
					&isNull))
			elog(ERROR, "could not find junk \"%s\" column",
					erm->resname);

		/* shouldn't ever get a null result... */
		if (isNull)
			elog(ERROR, "\"%s\" is NULL", erm->resname);

		tuple.t_self = *((ItemPointer) DatumGetPointer(datum));

		if (erm->forUpdate)
			lockmode = LockTupleExclusive;
		else
			lockmode = LockTupleShared;

		test = heap_lock_tuple(erm->relation, &tuple, &buffer,
				&update_ctid, &update_xmax,
				estate->es_snapshot->curcid,
				lockmode,
				(erm->noWait ? LockTupleNoWait : LockTupleWait));
		ReleaseBuffer(buffer);
		switch (test)
		{
			case HeapTupleSelfUpdated:
				/* treat it as deleted; do not process */
				goto lnext;

			case HeapTupleMayBeUpdated:
				break;

			case HeapTupleUpdated:
				if (IsXactIsoLevelSerializable)
					ereport(ERROR,
							(errcode(ERRCODE_T_R_SERIALIZATION_FAILURE),
							 errmsg("could not serialize access due to concurrent update")));
				if (!ItemPointerEquals(&update_ctid,
							&tuple.t_self))
				{
					/* updated, so look at updated version */
					newSlot = EvalPlanQual(estate,
							erm->rti,
							&update_ctid,
							update_xmax,
							estate->es_snapshot->curcid);
					if (!TupIsNull(newSlot))
					{
						slot = planSlot = newSlot;
						estate->es_useEvalPlan = true;
						goto lmark;
					}
				}

				/*
				 * if tuple was deleted or PlanQual failed for
				 * updated tuple - we must not return this tuple!
				 */
				goto lnext;

			default:
				elog(ERROR, "unrecognized heap_lock_tuple status: %u",
						test);
				return NULL;
		}
	}
			}

			/*
			 * Create a new "clean" tuple with all junk attributes removed. We
			 * don't need to do this for DELETE, however (there will in fact
			 * be no non-junk attributes in a DELETE!)
			 */
			if (operation != CMD_DELETE)
				slot = ExecFilterJunk(junkfilter, slot);
		}

		if (operation != CMD_SELECT && Gp_role == GP_ROLE_EXECUTE && !Gp_is_writer)
		{
			elog(LOG,"INSERT/UPDATE/DELETE must be executed by a writer segworker group");
			Insist(false);
		}

		/*
		 * Based on the operation, a tuple is either
		 * returned it to the user (SELECT) or inserted, deleted, or updated.
		 */
		switch (operation)
		{
			case CMD_SELECT:
				ExecSelect(slot, dest, estate);
				result = slot;
				break;

			case CMD_INSERT:
				ExecInsert(slot, dest, estate, PLANGEN_PLANNER, false /* isUpdate */);
				result = NULL;
				break;

			case CMD_DELETE:
				ExecDelete(tupleid, planSlot, dest, estate, PLANGEN_PLANNER, false /* isUpdate */);
				result = NULL;
				break;

			case CMD_UPDATE:
				ExecUpdate(slot, tupleid, planSlot, dest, estate);
				result = NULL;
				break;

			default:
				elog(ERROR, "unrecognized operation code: %d",
						(int) operation);
				result = NULL;
				break;
		}
	}

	/*
	 * check our tuple count.. if we've processed the proper number then
	 * quit, else loop again and process more tuples.  Zero numberTuples
	 * means no limit.
	 */
	current_tuple_count++;
	if (numberTuples && numberTuples == current_tuple_count)
	{
		break;
	}
	}

	/*
	 * Process AFTER EACH STATEMENT triggers
	 */
	if (Gp_role != GP_ROLE_EXECUTE || Gp_is_writer)
	{
		switch (operation)
		{
			case CMD_UPDATE:
				ExecASUpdateTriggers(estate, estate->es_result_relation_info);
				break;
			case CMD_DELETE:
				ExecASDeleteTriggers(estate, estate->es_result_relation_info);
				break;
			case CMD_INSERT:
				ExecASInsertTriggers(estate, estate->es_result_relation_info);
				break;
			default:
				/* do nothing */
				break;
		}
	}
	/*
	 * here, result is either a slot containing a tuple in the case of a
	 * SELECT or NULL otherwise.
	 */
	return result;
}

/* ----------------------------------------------------------------
 *		ExecSelect
 *
 *		SELECTs are easy.. we just pass the tuple to the appropriate
 *		output function.
 * ----------------------------------------------------------------
 */
static void
ExecSelect(TupleTableSlot *slot,
		DestReceiver *dest,
		EState *estate)
{
	(*dest->receiveSlot) (slot, dest);
	IncrRetrieved();
	(estate->es_processed)++;
}

/*
 * ExecRelCheck --- check that tuple meets constraints for result relation
 */
static const char *
ExecRelCheck(ResultRelInfo *resultRelInfo,
		TupleTableSlot *slot, EState *estate)
{
	Relation	rel = resultRelInfo->ri_RelationDesc;
	int			ncheck = rel->rd_att->constr->num_check;
	ConstrCheck *check = rel->rd_att->constr->check;
	ExprContext *econtext;
	MemoryContext oldContext;
	List	   *qual;
	int			i;

	/*
	 * If first time through for this result relation, build expression
	 * nodetrees for rel's constraint expressions.  Keep them in the per-query
	 * memory context so they'll survive throughout the query.
	 */
	if (resultRelInfo->ri_ConstraintExprs == NULL)
	{
		oldContext = MemoryContextSwitchTo(estate->es_query_cxt);
		resultRelInfo->ri_ConstraintExprs =
			(List **) palloc(ncheck * sizeof(List *));
		for (i = 0; i < ncheck; i++)
		{
			/* ExecQual wants implicit-AND form */
			qual = make_ands_implicit(stringToNode(check[i].ccbin));
			resultRelInfo->ri_ConstraintExprs[i] = (List *)
				ExecPrepareExpr((Expr *) qual, estate);
		}
		MemoryContextSwitchTo(oldContext);
	}

	/*
	 * We will use the EState's per-tuple context for evaluating constraint
	 * expressions (creating it if it's not already there).
	 */
	econtext = GetPerTupleExprContext(estate);

	/* Arrange for econtext's scan tuple to be the tuple under test */
	econtext->ecxt_scantuple = slot;

	/* And evaluate the constraints */
	for (i = 0; i < ncheck; i++)
	{
		qual = resultRelInfo->ri_ConstraintExprs[i];

		/*
		 * NOTE: SQL92 specifies that a NULL result from a constraint
		 * expression is not to be treated as a failure.  Therefore, tell
		 * ExecQual to return TRUE for NULL.
		 */
		if (!ExecQual(qual, econtext, true))
			return check[i].ccname;
	}

	/* NULL result means no error */
	return NULL;
}

void
ExecConstraints(ResultRelInfo *resultRelInfo,
		TupleTableSlot *slot, EState *estate)
{
	Relation	rel = resultRelInfo->ri_RelationDesc;
	TupleConstr *constr = rel->rd_att->constr;

	Assert(constr);

	if (constr->has_not_null)
	{
		int			natts = rel->rd_att->natts;
		int			attrChk;

		for (attrChk = 1; attrChk <= natts; attrChk++)
		{
			if (rel->rd_att->attrs[attrChk - 1]->attnotnull &&
					slot_attisnull(slot, attrChk))
				ereport(ERROR,
						(errcode(ERRCODE_NOT_NULL_VIOLATION),
						 errmsg("null value in column \"%s\" violates not-null constraint",
							 NameStr(rel->rd_att->attrs[attrChk - 1]->attname)),
						 errOmitLocation(true)));
		}
	}

	if (constr->num_check > 0)
	{
		const char *failed;

		if ((failed = ExecRelCheck(resultRelInfo, slot, estate)) != NULL)
			ereport(ERROR,
					(errcode(ERRCODE_CHECK_VIOLATION),
					 errmsg("new row for relation \"%s\" violates check constraint \"%s\"",
						 RelationGetRelationName(rel), failed),
					 errOmitLocation(true)));
	}
}

/*
 * Check a modified tuple to see if we want to process its updated version
 * under READ COMMITTED rules.
 *
 * See backend/executor/README for some info about how this works.
 *
 *	estate - executor state data
 *	rti - rangetable index of table containing tuple
 *	*tid - t_ctid from the outdated tuple (ie, next updated version)
 *	priorXmax - t_xmax from the outdated tuple
 *	curCid - command ID of current command of my transaction
 *
 * *tid is also an output parameter: it's modified to hold the TID of the
 * latest version of the tuple (note this may be changed even on failure)
 *
 * Returns a slot containing the new candidate update/delete tuple, or
 * NULL if we determine we shouldn't process the row.
 */
TupleTableSlot *
EvalPlanQual(EState *estate, Index rti,
		ItemPointer tid, TransactionId priorXmax, CommandId curCid)
{
	evalPlanQual *epq;
	EState	   *epqstate;
	Relation	relation;
	HeapTupleData tuple;
	HeapTuple	copyTuple = NULL;
	bool		endNode;

	Assert(rti != 0);

	/*
	 * find relation containing target tuple
	 */
	if (estate->es_result_relation_info != NULL &&
			estate->es_result_relation_info->ri_RangeTableIndex == rti)
		relation = estate->es_result_relation_info->ri_RelationDesc;
	else
	{
		ListCell   *l;

		relation = NULL;
		foreach(l, estate->es_rowMarks)
		{
			if (((ExecRowMark *) lfirst(l))->rti == rti)
			{
				relation = ((ExecRowMark *) lfirst(l))->relation;
				break;
			}
		}
		if (relation == NULL)
			elog(ERROR, "could not find RowMark for RT index %u", rti);
	}

	/*
	 * fetch tid tuple
	 *
	 * Loop here to deal with updated or busy tuples
	 */
	tuple.t_self = *tid;
	for (;;)
	{
		Buffer		buffer;

		if (heap_fetch(relation, SnapshotDirty, &tuple, &buffer, true, NULL))
		{
			/*
			 * If xmin isn't what we're expecting, the slot must have been
			 * recycled and reused for an unrelated tuple.	This implies that
			 * the latest version of the row was deleted, so we need do
			 * nothing.  (Should be safe to examine xmin without getting
			 * buffer's content lock, since xmin never changes in an existing
			 * tuple.)
			 */
			if (!TransactionIdEquals(HeapTupleHeaderGetXmin(tuple.t_data),
						priorXmax))
			{
				ReleaseBuffer(buffer);
				return NULL;
			}

			/* otherwise xmin should not be dirty... */
			if (TransactionIdIsValid(SnapshotDirty->xmin))
				elog(ERROR, "t_xmin is uncommitted in tuple to be updated");

			/*
			 * If tuple is being updated by other transaction then we have to
			 * wait for its commit/abort.
			 */
			if (TransactionIdIsValid(SnapshotDirty->xmax))
			{
				ReleaseBuffer(buffer);
				XactLockTableWait(SnapshotDirty->xmax);
				continue;		/* loop back to repeat heap_fetch */
			}

			/*
			 * If tuple was inserted by our own transaction, we have to check
			 * cmin against curCid: cmin >= curCid means our command cannot
			 * see the tuple, so we should ignore it.  Without this we are
			 * open to the "Halloween problem" of indefinitely re-updating the
			 * same tuple.	(We need not check cmax because
			 * HeapTupleSatisfiesDirty will consider a tuple deleted by our
			 * transaction dead, regardless of cmax.)  We just checked that
			 * priorXmax == xmin, so we can test that variable instead of
			 * doing HeapTupleHeaderGetXmin again.
			 */
			if (TransactionIdIsCurrentTransactionId(priorXmax) &&
					HeapTupleHeaderGetCmin(tuple.t_data) >= curCid)
			{
				ReleaseBuffer(buffer);
				return NULL;
			}

			/*
			 * We got tuple - now copy it for use by recheck query.
			 */
			copyTuple = heap_copytuple(&tuple);
			ReleaseBuffer(buffer);
			break;
		}

		/*
		 * If the referenced slot was actually empty, the latest version of
		 * the row must have been deleted, so we need do nothing.
		 */
		if (tuple.t_data == NULL)
		{
			ReleaseBuffer(buffer);
			return NULL;
		}

		/*
		 * As above, if xmin isn't what we're expecting, do nothing.
		 */
		if (!TransactionIdEquals(HeapTupleHeaderGetXmin(tuple.t_data),
					priorXmax))
		{
			ReleaseBuffer(buffer);
			return NULL;
		}

		/*
		 * If we get here, the tuple was found but failed SnapshotDirty.
		 * Assuming the xmin is either a committed xact or our own xact (as it
		 * certainly should be if we're trying to modify the tuple), this must
		 * mean that the row was updated or deleted by either a committed xact
		 * or our own xact.  If it was deleted, we can ignore it; if it was
		 * updated then chain up to the next version and repeat the whole
		 * test.
		 *
		 * As above, it should be safe to examine xmax and t_ctid without the
		 * buffer content lock, because they can't be changing.
		 */
		if (ItemPointerEquals(&tuple.t_self, &tuple.t_data->t_ctid))
		{
			/* deleted, so forget about it */
			ReleaseBuffer(buffer);
			return NULL;
		}

		/* updated, so look at the updated row */
		tuple.t_self = tuple.t_data->t_ctid;
		/* updated row should have xmin matching this xmax */
		priorXmax = HeapTupleHeaderGetXmax(tuple.t_data);
		ReleaseBuffer(buffer);
		/* loop back to fetch next in chain */
	}

	/*
	 * For UPDATE/DELETE we have to return tid of actual row we're executing
	 * PQ for.
	 */
	*tid = tuple.t_self;

	/*
	 * Need to run a recheck subquery.	Find or create a PQ stack entry.
	 */
	epq = estate->es_evalPlanQual;
	endNode = true;

	if (epq != NULL && epq->rti == 0)
	{
		/* Top PQ stack entry is idle, so re-use it */
		Assert(!(estate->es_useEvalPlan) && epq->next == NULL);
		epq->rti = rti;
		endNode = false;
	}

	/*
	 * If this is request for another RTE - Ra, - then we have to check wasn't
	 * PlanQual requested for Ra already and if so then Ra' row was updated
	 * again and we have to re-start old execution for Ra and forget all what
	 * we done after Ra was suspended. Cool? -:))
	 */
	if (epq != NULL && epq->rti != rti &&
			epq->estate->es_evTuple[rti - 1] != NULL)
	{
		do
		{
			evalPlanQual *oldepq;

			/* stop execution */
			EvalPlanQualStop(epq);
			/* pop previous PlanQual from the stack */
			oldepq = epq->next;
			Assert(oldepq && oldepq->rti != 0);
			/* push current PQ to freePQ stack */
			oldepq->free = epq;
			epq = oldepq;
			estate->es_evalPlanQual = epq;
		} while (epq->rti != rti);
	}

	/*
	 * If we are requested for another RTE then we have to suspend execution
	 * of current PlanQual and start execution for new one.
	 */
	if (epq == NULL || epq->rti != rti)
	{
		/* try to reuse plan used previously */
		evalPlanQual *newepq = (epq != NULL) ? epq->free : NULL;

		if (newepq == NULL)		/* first call or freePQ stack is empty */
		{
			newepq = (evalPlanQual *) palloc0(sizeof(evalPlanQual));
			newepq->free = NULL;
			newepq->estate = NULL;
			newepq->planstate = NULL;
		}
		else
		{
			/* recycle previously used PlanQual */
			Assert(newepq->estate == NULL);
			epq->free = NULL;
		}
		/* push current PQ to the stack */
		newepq->next = epq;
		epq = newepq;
		estate->es_evalPlanQual = epq;
		epq->rti = rti;
		endNode = false;
	}

	Assert(epq->rti == rti);

	/*
	 * Ok - we're requested for the same RTE.  Unfortunately we still have to
	 * end and restart execution of the plan, because ExecReScan wouldn't
	 * ensure that upper plan nodes would reset themselves.  We could make
	 * that work if insertion of the target tuple were integrated with the
	 * Param mechanism somehow, so that the upper plan nodes know that their
	 * children's outputs have changed.
	 *
	 * Note that the stack of free evalPlanQual nodes is quite useless at the
	 * moment, since it only saves us from pallocing/releasing the
	 * evalPlanQual nodes themselves.  But it will be useful once we implement
	 * ReScan instead of end/restart for re-using PlanQual nodes.
	 */
	if (endNode)
	{
		/* stop execution */
		EvalPlanQualStop(epq);
	}

	/*
	 * Initialize new recheck query.
	 *
	 * Note: if we were re-using PlanQual plans via ExecReScan, we'd need to
	 * instead copy down changeable state from the top plan (including
	 * es_result_relation_info, es_junkFilter) and reset locally changeable
	 * state in the epq (including es_param_exec_vals, es_evTupleNull).
	 */
	EvalPlanQualStart(epq, estate, epq->next);

	/*
	 * free old RTE' tuple, if any, and store target tuple where relation's
	 * scan node will see it
	 */
	epqstate = epq->estate;
	if (epqstate->es_evTuple[rti - 1] != NULL)
		heap_freetuple(epqstate->es_evTuple[rti - 1]);
	epqstate->es_evTuple[rti - 1] = copyTuple;

	return EvalPlanQualNext(estate);
}

static TupleTableSlot *
EvalPlanQualNext(EState *estate)
{
	evalPlanQual *epq = estate->es_evalPlanQual;
	MemoryContext oldcontext;
	TupleTableSlot *slot;

	Assert(epq->rti != 0);

lpqnext:;
	oldcontext = MemoryContextSwitchTo(epq->estate->es_query_cxt);
	slot = ExecProcNode(epq->planstate);
	MemoryContextSwitchTo(oldcontext);

	/*
	 * No more tuples for this PQ. Continue previous one.
	 */
	if (TupIsNull(slot))
	{
		evalPlanQual *oldepq;

		/* stop execution */
		EvalPlanQualStop(epq);
		/* pop old PQ from the stack */
		oldepq = epq->next;
		if (oldepq == NULL)
		{
			/* this is the first (oldest) PQ - mark as free */
			epq->rti = 0;
			estate->es_useEvalPlan = false;
			/* and continue Query execution */
			return NULL;
		}
		Assert(oldepq->rti != 0);
		/* push current PQ to freePQ stack */
		oldepq->free = epq;
		epq = oldepq;
		estate->es_evalPlanQual = epq;
		goto lpqnext;
	}

	return slot;
}

static void
EndEvalPlanQual(EState *estate)
{
	evalPlanQual *epq = estate->es_evalPlanQual;

	if (epq->rti == 0)			/* plans already shutdowned */
	{
		Assert(epq->next == NULL);
		return;
	}

	for (;;)
	{
		evalPlanQual *oldepq;

		/* stop execution */
		EvalPlanQualStop(epq);
		/* pop old PQ from the stack */
		oldepq = epq->next;
		if (oldepq == NULL)
		{
			/* this is the first (oldest) PQ - mark as free */
			epq->rti = 0;
			estate->es_useEvalPlan = false;
			break;
		}
		Assert(oldepq->rti != 0);
		/* push current PQ to freePQ stack */
		oldepq->free = epq;
		epq = oldepq;
		estate->es_evalPlanQual = epq;
	}
}

/*
 * Start execution of one level of PlanQual.
 *
 * This is a cut-down version of ExecutorStart(): we copy some state from
 * the top-level estate rather than initializing it fresh.
 */
static void
EvalPlanQualStart(evalPlanQual *epq, EState *estate, evalPlanQual *priorepq)
{
	EState	   *epqstate;
	int			rtsize;
	MemoryContext oldcontext;

	rtsize = list_length(estate->es_range_table);

	/*
	 * It's tempting to think about using CreateSubExecutorState here, but
	 * at present we can't because of memory leakage concerns ...
	 */
	epq->estate = epqstate = CreateExecutorState();

	oldcontext = MemoryContextSwitchTo(epqstate->es_query_cxt);

	/*
	 * The epqstates share the top query's copy of unchanging state such as
	 * the snapshot, rangetable, result-rel info, and external Param info.
	 * They need their own copies of local state, including a tuple table,
	 * es_param_exec_vals, etc.
	 */
	epqstate->es_direction = ForwardScanDirection;
	epqstate->es_snapshot = estate->es_snapshot;
	epqstate->es_crosscheck_snapshot = estate->es_crosscheck_snapshot;
	epqstate->es_range_table = estate->es_range_table;
	epqstate->es_result_relations = estate->es_result_relations;
	epqstate->es_num_result_relations = estate->es_num_result_relations;
	epqstate->es_result_relation_info = estate->es_result_relation_info;
	epqstate->es_junkFilter = estate->es_junkFilter;
	epqstate->es_into_relation_descriptor = estate->es_into_relation_descriptor;
	epqstate->es_into_relation_is_bulkload = estate->es_into_relation_is_bulkload;
	epqstate->es_into_relation_last_heap_tid = estate->es_into_relation_last_heap_tid;
	epqstate->es_param_list_info = estate->es_param_list_info;
	if (estate->es_plannedstmt->planTree->nParamExec > 0)
		epqstate->es_param_exec_vals = (ParamExecData *)
			palloc0(estate->es_plannedstmt->planTree->nParamExec * sizeof(ParamExecData));
	epqstate->es_rowMarks = estate->es_rowMarks;
	epqstate->es_instrument = estate->es_instrument;
	epqstate->es_select_into = estate->es_select_into;
	epqstate->es_into_oids = estate->es_into_oids;
	epqstate->es_plannedstmt = estate->es_plannedstmt;

	/*
	 * Each epqstate must have its own es_evTupleNull state, but all the stack
	 * entries share es_evTuple state.	This allows sub-rechecks to inherit
	 * the value being examined by an outer recheck.
	 */
	epqstate->es_evTupleNull = (bool *) palloc0(rtsize * sizeof(bool));
	if (priorepq == NULL)
		/* first PQ stack entry */
		epqstate->es_evTuple = (HeapTuple *)
			palloc0(rtsize * sizeof(HeapTuple));
	else
		/* later stack entries share the same storage */
		epqstate->es_evTuple = priorepq->estate->es_evTuple;

	/*
	 * Create sub-tuple-table; we needn't redo the CountSlots work though.
	 */
	epqstate->es_tupleTable =
		ExecCreateTupleTable(estate->es_tupleTable->size);

	/*
	 * Initialize the private state information for all the nodes in the query
	 * tree.  This opens files, allocates storage and leaves us ready to start
	 * processing tuples.
	 */
	epq->planstate = ExecInitNode(estate->es_plannedstmt->planTree, epqstate, 0);

	MemoryContextSwitchTo(oldcontext);
}

/*
 * End execution of one level of PlanQual.
 *
 * This is a cut-down version of ExecutorEnd(); basically we want to do most
 * of the normal cleanup, but *not* close result relations (which we are
 * just sharing from the outer query).	We do, however, have to close any
 * trigger target relations that got opened, since those are not shared.
 */
static void
EvalPlanQualStop(evalPlanQual *epq)
{
	EState	   *epqstate = epq->estate;
	MemoryContext oldcontext;

	oldcontext = MemoryContextSwitchTo(epqstate->es_query_cxt);

	ExecEndNode(epq->planstate);

	ExecDropTupleTable(epqstate->es_tupleTable, true);
	epqstate->es_tupleTable = NULL;

	if (epqstate->es_evTuple[epq->rti - 1] != NULL)
	{
		heap_freetuple(epqstate->es_evTuple[epq->rti - 1]);
		epqstate->es_evTuple[epq->rti - 1] = NULL;
	}

	MemoryContextSwitchTo(oldcontext);

	FreeExecutorState(epqstate);

	epq->estate = NULL;
	epq->planstate = NULL;
}

/*
 * GetUpdatedTuple_Int
 *
 * This function is an extraction of interesting parts of EvalPlanQual (and
 * therefore it presents some code duplication, probably should clean up at
 * some point if possible). We use it instead of EvalPlanQual when we are
 * scanning a local relation and using heap_lock_tuple to lock entries for
 * update, but we don't have access to the executor state and any RTE's.
 *
 * The inputs to this function is the old tuple tid the relation it's in, and
 * the output is the actual updated relation.
 */
HeapTuple
GetUpdatedTuple_Int(Relation relation,
		ItemPointer tid,
		TransactionId priorXmax,
		CommandId curCid)
{
	HeapTupleData tuple;
	HeapTuple	copyTuple = NULL;

	/*
	 * fetch tid tuple
	 *
	 * Loop here to deal with updated or busy tuples
	 */
	tuple.t_self = *tid;
	for (;;)
	{
		Buffer		buffer;

		if (heap_fetch(relation, SnapshotDirty, &tuple, &buffer, true, NULL))
		{
			/*
			 * If xmin isn't what we're expecting, the slot must have been
			 * recycled and reused for an unrelated tuple.	This implies that
			 * the latest version of the row was deleted, so we need do
			 * nothing.  (Should be safe to examine xmin without getting
			 * buffer's content lock, since xmin never changes in an existing
			 * tuple.)
			 */
			if (!TransactionIdEquals(HeapTupleHeaderGetXmin(tuple.t_data),
						priorXmax))
			{
				ReleaseBuffer(buffer);
				return NULL;
			}

			/* otherwise xmin should not be dirty... */
			if (TransactionIdIsValid(SnapshotDirty->xmin))
				elog(ERROR, "t_xmin is uncommitted in tuple to be updated");

			/*
			 * If tuple is being updated by other transaction then we have to
			 * wait for its commit/abort.
			 */
			if (TransactionIdIsValid(SnapshotDirty->xmax))
			{
				ReleaseBuffer(buffer);
				XactLockTableWait(SnapshotDirty->xmax);
				continue;		/* loop back to repeat heap_fetch */
			}

			/*
			 * If tuple was inserted by our own transaction, we have to check
			 * cmin against curCid: cmin >= curCid means our command cannot
			 * see the tuple, so we should ignore it.  Without this we are
			 * open to the "Halloween problem" of indefinitely re-updating the
			 * same tuple.	(We need not check cmax because
			 * HeapTupleSatisfiesDirty will consider a tuple deleted by our
			 * transaction dead, regardless of cmax.)  We just checked that
			 * priorXmax == xmin, so we can test that variable instead of
			 * doing HeapTupleHeaderGetXmin again.
			 */
			if (TransactionIdIsCurrentTransactionId(priorXmax) &&
					HeapTupleHeaderGetCmin(tuple.t_data) >= curCid)
			{
				ReleaseBuffer(buffer);
				return NULL;
			}

			/*
			 * We got tuple - now copy it for use by recheck query.
			 */
			copyTuple = heap_copytuple(&tuple);
			ReleaseBuffer(buffer);
			break;
		}

		/*
		 * If the referenced slot was actually empty, the latest version of
		 * the row must have been deleted, so we need do nothing.
		 */
		if (tuple.t_data == NULL)
		{
			ReleaseBuffer(buffer);
			return NULL;
		}

		/*
		 * As above, if xmin isn't what we're expecting, do nothing.
		 */
		if (!TransactionIdEquals(HeapTupleHeaderGetXmin(tuple.t_data),
					priorXmax))
		{
			ReleaseBuffer(buffer);
			return NULL;
		}

		/*
		 * If we get here, the tuple was found but failed SnapshotDirty.
		 * Assuming the xmin is either a committed xact or our own xact (as it
		 * certainly should be if we're trying to modify the tuple), this must
		 * mean that the row was updated or deleted by either a committed xact
		 * or our own xact.  If it was deleted, we can ignore it; if it was
		 * updated then chain up to the next version and repeat the whole
		 * test.
		 *
		 * As above, it should be safe to examine xmax and t_ctid without the
		 * buffer content lock, because they can't be changing.
		 */
		if (ItemPointerEquals(&tuple.t_self, &tuple.t_data->t_ctid))
		{
			/* deleted, so forget about it */
			ReleaseBuffer(buffer);
			return NULL;
		}

		/* updated, so look at the updated row */
		tuple.t_self = tuple.t_data->t_ctid;
		/* updated row should have xmin matching this xmax */
		priorXmax = HeapTupleHeaderGetXmax(tuple.t_data);
		ReleaseBuffer(buffer);
		/* loop back to fetch next in chain */
	}

	//*tid = tuple.t_self;

	return copyTuple;
}

/*
 * Support for SELECT INTO (a/k/a CREATE TABLE AS)
 *
 * We implement SELECT INTO by diverting SELECT's normal output with
 * a specialized DestReceiver type.
 *
 * TODO: remove some of the INTO-specific cruft from EState, and keep
 * it in the DestReceiver instead.
 */

typedef struct
{
	DestReceiver pub;			/* publicly-known function pointers */
	EState	   *estate;			/* EState we are working with */
	AppendOnlyInsertDescData *ao_insertDesc; /* descriptor to AO tables */
	ParquetInsertDescData *parquet_insertDesc; /* descriptor to parquet tables */
} DR_intorel;

static Relation
CreateIntoRel(QueryDesc *queryDesc)
{
	EState	   *estate = queryDesc->estate;
	char	   *intoName;
	IntoClause *intoClause;
	List *segnos = NIL;

	Relation	intoRelationDesc;

	char		relkind = RELKIND_RELATION;
	char		relstorage;
	Oid			namespaceId;
	Oid			tablespaceId;
	Datum		reloptions;
	StdRdOptions *stdRdOptions;
	AclResult	aclresult;
	Oid			intoRelationId;
	TupleDesc	tupdesc;
	Oid         intoOid;
	Oid         intoComptypeOid;
	GpPolicy   *targetPolicy;
	int			safefswritesize = gp_safefswritesize;
	ItemPointerData persistentTid;
	int64			persistentSerialNum;

	Assert(Gp_role != GP_ROLE_EXECUTE);

	targetPolicy = queryDesc->plannedstmt->intoPolicy;
	intoClause = queryDesc->plannedstmt->intoClause;
	/*
	 * Check consistency of arguments
	 */
	Insist(intoClause);
	if (intoClause->onCommit != ONCOMMIT_NOOP && !intoClause->rel->istemp)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_TABLE_DEFINITION),
				 errmsg("ON COMMIT can only be used on temporary tables"),
				 errOmitLocation(true)));

	/* MPP specific stuff */
	intoOid = intoClause->oidInfo.relOid;
	intoComptypeOid = intoClause->oidInfo.comptypeOid;

	/*
	 * Find namespace to create in, check its permissions
	 */
	intoName = intoClause->rel->relname;
	namespaceId = RangeVarGetCreationNamespace(intoClause->rel);

	aclresult = pg_namespace_aclcheck(namespaceId, GetUserId(),
			ACL_CREATE);
	if (aclresult != ACLCHECK_OK)
		aclcheck_error(aclresult, ACL_KIND_NAMESPACE,
				get_namespace_name(namespaceId));

	/*
	 * Select tablespace to use.  If not specified, use default_tablespace
	 * (which may in turn default to database's default).
	 */
	if (intoClause->tableSpaceName)
	{
		tablespaceId = get_tablespace_oid(intoClause->tableSpaceName);
		if (!OidIsValid(tablespaceId))
			ereport(ERROR,
					(errcode(ERRCODE_UNDEFINED_OBJECT),
					 errmsg("tablespace \"%s\" does not exist",
						 intoClause->tableSpaceName)));
	}
	else
	{
		tablespaceId = GetDefaultTablespace();

		/* Need the real tablespace id for dispatch */
		if (!OidIsValid(tablespaceId)) 
			tablespaceId = MyDatabaseTableSpace;

		/* MPP-10329 - must dispatch tablespace */
		intoClause->tableSpaceName = get_tablespace_name(tablespaceId);
	}

	/* Check permissions except when using the database's default space */
	if (tablespaceId != MyDatabaseTableSpace &&
			tablespaceId != get_database_dts(MyDatabaseId))
	{
		AclResult	aclresult;

		aclresult = pg_tablespace_aclcheck(tablespaceId, GetUserId(),
				ACL_CREATE);

		if (aclresult != ACLCHECK_OK)
			aclcheck_error(aclresult, ACL_KIND_TABLESPACE,
					get_tablespace_name(tablespaceId));
	}


	/*
	 * check options, report if user want to create heap table.
	 */
	{
		bool	has_option = false;
		ListCell	*cell = NULL;

		foreach(cell, intoClause->options)
		{
			DefElem *e = (DefElem *) lfirst(cell);
			char	*s = NULL;

			if (!IsA(e, DefElem))
				continue;
			if (!IsA(e->arg, String))
				continue;
			if (pg_strcasecmp(e->defname, "appendonly"))
				continue;

			has_option = true;
			s = strVal(e->arg);
			if (pg_strcasecmp(s, "false") == 0)
				ereport(ERROR,
						(errcode(ERRCODE_GP_FEATURE_NOT_SUPPORTED),
						 errmsg("gpsql does not support heap table, use append only table instead"),
						 errOmitLocation(true)));
		}

		if (!has_option)
		{
			intoClause->options = lappend(intoClause->options, makeDefElem("appendonly", (Node *) makeString("true")));
		}
	}

	/* Parse and validate any reloptions */
	reloptions = transformRelOptions((Datum) 0, intoClause->options, true, false);
	/* get the relstorage (heap or AO tables) */
	stdRdOptions = (StdRdOptions*) heap_reloptions(relkind, reloptions, true);
	heap_test_override_reloptions(relkind, stdRdOptions, &safefswritesize);
	if(stdRdOptions->appendonly)
	{
		/*
		   relstorage = stdRdOptions->columnstore ? RELSTORAGE_AOCOLS : RELSTORAGE_AOROWS;
		   relstorage = stdRdOptions->parquetstore ? RELSTORAGE_PARQUET : RELSTORAGE_AOROWS;
		 */
		relstorage = stdRdOptions->columnstore;
	}
	else
	{
		relstorage = RELSTORAGE_HEAP;
		ereport(ERROR,
				(errcode(ERRCODE_GP_FEATURE_NOT_SUPPORTED),
				 errmsg("gpsql does not support heap table, use append only table instead"),
				 errOmitLocation(true)));
	}

	/* have to copy the actual tupdesc to get rid of any constraints */
	tupdesc = CreateTupleDescCopy(queryDesc->tupDesc);

	/* Now we can actually create the new relation */
	intoRelationId = heap_create_with_catalog(intoName,
			namespaceId,
			tablespaceId,
			intoOid,			/* MPP */
			GetUserId(),
			tupdesc,
			/* relam */ InvalidOid,
			relkind,
			relstorage,
			false,
			true,
			false,
			0,
			intoClause->onCommit,
			targetPolicy,  	/* MPP */
			reloptions,
			allowSystemTableModsDDL,
			&intoComptypeOid, 	/* MPP */
			&persistentTid,
			&persistentSerialNum);

	FreeTupleDesc(tupdesc);

	/*
	 * Advance command counter so that the newly-created relation's catalog
	 * tuples will be visible to heap_open.
	 */
	CommandCounterIncrement();

	/*
	 * If necessary, create a TOAST table for the new relation, or an Append
	 * Only segment table. Note that AlterTableCreateXXXTable ends with
	 * CommandCounterIncrement(), so that the new tables will be visible for
	 * insertion.
	 */
	AlterTableCreateToastTableWithOid(intoRelationId, 
			intoClause->oidInfo.toastOid, 
			intoClause->oidInfo.toastIndexOid, 
			&intoClause->oidInfo.toastComptypeOid, 
			false);
	AlterTableCreateAoSegTableWithOid(intoRelationId, 
			intoClause->oidInfo.aosegOid, 
			intoClause->oidInfo.aosegIndexOid,
			&intoClause->oidInfo.aosegComptypeOid, 
			false);
	/* don't create AO block directory here, it'll be created when needed */

	/*
	 * Advance command counter so that the newly-created relation's catalog
	 * tuples will be visible to heap_open.
	 */
	CommandCounterIncrement();

	/*
	 * And open the constructed table for writing.
	 */
	intoRelationDesc = heap_open(intoRelationId, AccessExclusiveLock);

	/*
	 * Add column encoding entries based on the WITH clause.
	 *
	 * NOTE:  we could also do this expansion during parse analysis, by
	 * expanding the IntoClause options field into some attr_encodings field
	 * (cf. CreateStmt and transformCreateStmt()). As it stands, there's no real
	 * benefit for doing that from a code complexity POV. In fact, it would mean
	 * more code. If, however, we supported column encoding syntax during CTAS,
	 * it would be a good time to relocate this code.
	 */
	AddDefaultRelationAttributeOptions(intoRelationDesc,
			intoClause->options);

	/*
	 * create a list of segment file numbers for insert.
	 */
	segnos = SetSegnoForWrite(NIL, intoRelationId, GetQEGangNum(), true, false);
	CreateAppendOnlyParquetSegFileForRelationOnMaster(intoRelationDesc, segnos);
	queryDesc->plannedstmt->into_aosegnos = segnos;

	/**
	 * lock segment files
	 */
	/*
	 * currently, we disable vacuum, do not lock since lock table is too small.
	 */
	/*if (Gp_role == GP_ROLE_DISPATCH)
		LockSegfilesOnMaster(intoRelationDesc, 1);*/

	intoClause->oidInfo.relOid = intoRelationId;
	estate->es_into_relation_descriptor = intoRelationDesc;

	return intoRelationDesc;
}

/*
 * OpenIntoRel --- actually create the SELECT INTO target relation
 *
 * This also replaces QueryDesc->dest with the special DestReceiver for
 * SELECT INTO.  We assume that the correct result tuple type has already
 * been placed in queryDesc->tupDesc.
 */
static void
OpenIntoRel(QueryDesc *queryDesc)
{
	EState	   *estate = queryDesc->estate;
	Relation	intoRelationDesc;

	IntoClause *intoClause;

	Oid			intoRelationId;
	DR_intorel *myState;

	if (Gp_role != GP_ROLE_EXECUTE)
		intoRelationDesc = CreateIntoRel(queryDesc);
	else
	{
		intoClause = queryDesc->plannedstmt->intoClause;
		intoRelationId = intoClause->oidInfo.relOid;
		Assert(OidIsValid(intoRelationId));
		/*
		 * And open the constructed table for writing.
		 */
		intoRelationDesc = heap_open(intoRelationId, RowShareLock);
		estate->es_into_relation_descriptor = intoRelationDesc;
	}

	/* use_wal off requires rd_targblock be initially invalid */
	Assert(intoRelationDesc->rd_targblock == InvalidBlockNumber);

	Assert (!estate->es_into_relation_is_bulkload);

	/*
	 * Now replace the query's DestReceiver with one for SELECT INTO
	 */
	queryDesc->dest = CreateDestReceiver(DestIntoRel, NULL);
	myState = (DR_intorel *) queryDesc->dest;
	Assert(myState->pub.mydest == DestIntoRel);
	myState->estate = estate;
	myState->estate->into_aosegnos = queryDesc->plannedstmt->into_aosegnos;
}

/*
 * CloseIntoRel --- clean up SELECT INTO at ExecutorEnd time
 */
static void
CloseIntoRel(QueryDesc *queryDesc)
{
	EState	   *estate = queryDesc->estate;
	Relation	rel = estate->es_into_relation_descriptor;

	/* Partition with SELECT INTO is not supported */
	Assert(!PointerIsValid(estate->es_result_partitions));

	/* OpenIntoRel might never have gotten called */
	if (rel)
	{
		/* APPEND_ONLY is closed in the intorel_shutdown */
		if (!(RelationIsAoRows(rel) || RelationIsParquet(rel)))
        {
			Insist(!"gpsql does not support heap table, use append only table instead");
		}

		/* close rel, but keep lock until commit */
		heap_close(rel, NoLock);

		rel = NULL;
	}
}

/*
 * CreateIntoRelDestReceiver -- create a suitable DestReceiver object
 *
 * Since CreateDestReceiver doesn't accept the parameters we'd need,
 * we just leave the private fields empty here.  OpenIntoRel will
 * fill them in.
 */
DestReceiver *
CreateIntoRelDestReceiver(void)
{
	DR_intorel *self = (DR_intorel *) palloc(sizeof(DR_intorel));

	self->pub.receiveSlot = intorel_receive;
	self->pub.rStartup = intorel_startup;
	self->pub.rShutdown = intorel_shutdown;
	self->pub.rDestroy = intorel_destroy;
	self->pub.mydest = DestIntoRel;

	self->estate = NULL;
	self->ao_insertDesc = NULL;
    self->parquet_insertDesc = NULL;

	return (DestReceiver *) self;
}

/*
 * intorel_startup --- executor startup
 */
static void
intorel_startup(DestReceiver *self, int operation, TupleDesc typeinfo)
{
	UnusedArg(self);
	UnusedArg(operation);
	UnusedArg(typeinfo);

	/* no-op */
}

/*
 * intorel_receive --- receive one tuple
 */
static void
intorel_receive(TupleTableSlot *slot, DestReceiver *self)
{
	DR_intorel *myState = (DR_intorel *) self;
	EState	   *estate = myState->estate;
	Relation	into_rel = estate->es_into_relation_descriptor;

	Assert(estate->es_result_partitions == NULL);

	if (RelationIsAoRows(into_rel))
	{
		MemTuple	tuple = ExecCopySlotMemTuple(slot);
		Oid			tupleOid;
		AOTupleId	aoTupleId;

		if (myState->ao_insertDesc == NULL)
		{
			int segno = list_nth_int(estate->into_aosegnos, GetQEIndex());
			ResultRelSegFileInfo *segfileinfo = InitResultRelSegFileInfo(segno, RELSTORAGE_AOROWS, 1);
			myState->ao_insertDesc = appendonly_insert_init(into_rel,
															segfileinfo);
		}

		appendonly_insert(myState->ao_insertDesc, tuple, &tupleOid, &aoTupleId);
		pfree(tuple);
	}
	else if(RelationIsParquet(into_rel))
	{
		if(myState->parquet_insertDesc == NULL)
		{
			int segno = list_nth_int(estate->into_aosegnos, GetQEIndex());
			ResultRelSegFileInfo *segfileinfo = InitResultRelSegFileInfo(segno, RELSTORAGE_PARQUET, 1);
			myState->parquet_insertDesc = parquet_insert_init(into_rel, segfileinfo);
		}

		parquet_insert(myState->parquet_insertDesc, slot);
	}
	else
	{

		/* gpsql do not support heap table */
		ereport(ERROR,
				(errcode(ERRCODE_GP_FEATURE_NOT_SUPPORTED),
				 errmsg("gpsql does not support heap table, use append only table instead")));
	}

	/* We know this is a newly created relation, so there are no indexes */

	IncrAppended();
}

/*
 * intorel_shutdown --- executor end
 */
static void
intorel_shutdown(DestReceiver *self)
{
	int aocount = 0;

	/* If target was append only, finalise */
	DR_intorel *myState = (DR_intorel *) self;
	EState	   *estate = myState->estate;
	Relation	into_rel = estate->es_into_relation_descriptor;

	StringInfo buf = NULL;
	QueryContextDispatchingSendBack sendback = NULL;

	if (RelationIsAoRows(into_rel) && myState->ao_insertDesc)
		++aocount;
	else if(RelationIsParquet(into_rel) && myState->parquet_insertDesc)
		++aocount;

	if (Gp_role == GP_ROLE_EXECUTE && aocount > 0)
		buf = PreSendbackChangedCatalog(aocount);

	if (RelationIsAoRows(into_rel) && myState->ao_insertDesc)
	{
		sendback = CreateQueryContextDispatchingSendBack(1);
		myState->ao_insertDesc->sendback = sendback;

		sendback->relid = RelationGetRelid(myState->ao_insertDesc->aoi_rel);

		appendonly_insert_finish(myState->ao_insertDesc);
	}
	else if (RelationIsParquet(into_rel) && myState->parquet_insertDesc)
	{
		sendback = CreateQueryContextDispatchingSendBack(1);
		myState->parquet_insertDesc->sendback = sendback;

		sendback->relid = RelationGetRelid(myState->parquet_insertDesc->parquet_rel);

		parquet_insert_finish(myState->parquet_insertDesc);
	}

	if (sendback && Gp_role == GP_ROLE_EXECUTE)
		AddSendbackChangedCatalogContent(buf, sendback);

	DropQueryContextDispatchingSendBack(sendback);

	if (Gp_role == GP_ROLE_EXECUTE && aocount > 0)
		FinishSendbackChangedCatalog(buf);
}

/*
 * intorel_destroy --- release DestReceiver object
 */
static void
intorel_destroy(DestReceiver *self)
{
	pfree(self);
}

/*
 * Calculate the part to use for the given key, then find or calculate
 * and cache required information about that part in the hash table
 * anchored in estate.
 * 
 * Return a pointer to the information, an entry in the table
 * estate->es_result_relations.  Note that the first entry in this
 * table is for the partitioned table itself and that the entire table
 * may be reallocated, changing the addresses of its entries.  
 *
 * Thus, it is important to avoid holding long-lived pointers to 
 * table entries (such as the pointer returned from this function).
 */
static ResultRelInfo *
get_part(EState *estate, Datum *values, bool *isnull, TupleDesc tupdesc)
{
	ResultRelInfo *resultRelInfo;
	Oid targetid;
	bool found;
	ResultPartHashEntry *entry;

	/* add a short term memory context if one wasn't assigned already */
	Assert(estate->es_partition_state != NULL &&
			estate->es_partition_state->accessMethods != NULL);
	if (!estate->es_partition_state->accessMethods->part_cxt)
		estate->es_partition_state->accessMethods->part_cxt =
			GetPerTupleExprContext(estate)->ecxt_per_tuple_memory;

	targetid = selectPartition(estate->es_result_partitions, values,
			isnull, tupdesc, estate->es_partition_state->accessMethods);

	if (!OidIsValid(targetid))
		ereport(ERROR,
				(errcode(ERRCODE_NO_PARTITION_FOR_PARTITIONING_KEY),
				 errmsg("no partition for partitioning key"),
				 errOmitLocation(true)));


	Assert(estate->es_result_partitions && estate->es_result_partitions->part);
	Assert(estate->es_result_relations && estate->es_result_relations->ri_RelationDesc);
	Oid parent = estate->es_result_partitions->part->parrelid;
	Oid result = RelationGetRelid(estate->es_result_relations->ri_RelationDesc);
	/*
	 * We insert into a partition's child table.
	 */
	if (parent != result && targetid != result)
	{
		ereport(ERROR,
				(errcode(ERRCODE_NO_PARTITION_FOR_PARTITIONING_KEY),
				 errmsg("the data does not belong to partition: %s",
					 RelationGetRelationName(estate->es_result_relations->ri_RelationDesc)),
				 errOmitLocation(true)));
	}

	if (estate->es_partition_state->result_partition_hash == NULL)
	{
		HASHCTL ctl;
		long num_buckets;

		/* reasonable assumption? */
		num_buckets =
			list_length(all_partition_relids(estate->es_result_partitions));
		num_buckets /= num_partition_levels(estate->es_result_partitions);

		ctl.keysize = sizeof(Oid);
		ctl.entrysize = sizeof(*entry);
		ctl.hash = oid_hash;

		estate->es_partition_state->result_partition_hash =
			hash_create("Partition Result Relation Hash",
					num_buckets,
					&ctl,
					HASH_ELEM | HASH_FUNCTION);
	}

	entry = hash_search(estate->es_partition_state->result_partition_hash,
			&targetid,
			HASH_ENTER,
			&found);

	if (found)
	{
		resultRelInfo = estate->es_result_relations;
		resultRelInfo += entry->offset;
		Assert(RelationGetRelid(resultRelInfo->ri_RelationDesc) == targetid);
	}
	else
	{
		int result_array_size =
			estate->es_partition_state->result_partition_array_size;
		RangeTblEntry *rte = makeNode(RangeTblEntry);
		List *rangeTable;
		int natts;

		if (estate->es_num_result_relations + 1 >= result_array_size)
		{
			int32 sz = result_array_size * 2;

			/* we shouldn't be able to overflow */
			Insist((int)sz > result_array_size);

			estate->es_result_relation_info = estate->es_result_relations =
				(ResultRelInfo *)repalloc(estate->es_result_relations,
						sz * sizeof(ResultRelInfo));
			estate->es_partition_state->result_partition_array_size = (int)sz;
		}

		resultRelInfo = estate->es_result_relations;
		natts = resultRelInfo->ri_RelationDesc->rd_att->natts; /* in base relation */
		resultRelInfo += estate->es_num_result_relations;
		entry->offset = estate->es_num_result_relations;

		estate->es_num_result_relations++;

		rte->relid = targetid; /* all we need */
		rangeTable = list_make1(rte);
		initResultRelInfo(resultRelInfo, 1,
				rangeTable,
				CMD_INSERT,
				estate->es_instrument,
				(Gp_role != GP_ROLE_EXECUTE || Gp_is_writer));

		map_part_attrs(estate->es_result_relations->ri_RelationDesc, 
				resultRelInfo->ri_RelationDesc,
				&(resultRelInfo->ri_partInsertMap),
				TRUE); /* throw on error, so result not needed */

		if (resultRelInfo->ri_partInsertMap)
			resultRelInfo->ri_partSlot = 
				MakeSingleTupleTableSlot(resultRelInfo->ri_RelationDesc->rd_att);
	}
	return resultRelInfo;
}

ResultRelInfo *
values_get_partition(Datum *values, bool *nulls, TupleDesc tupdesc,
		EState *estate)
{
	ResultRelInfo *relinfo;

	Assert(PointerIsValid(estate->es_result_partitions));

	relinfo = get_part(estate, values, nulls, tupdesc);

	return relinfo;
}

/*
 * Find the partition we want and get the ResultRelInfo for the
 * partition.
 */
ResultRelInfo *
slot_get_partition(TupleTableSlot *slot, EState *estate)
{
	ResultRelInfo *resultRelInfo;
	AttrNumber max_attr;
	Datum *values;
	bool *nulls;

	Assert(PointerIsValid(estate->es_result_partitions));

	max_attr = estate->es_partition_state->max_partition_attr;

	slot_getsomeattrs(slot, max_attr);
	values = slot_get_values(slot);
	nulls = slot_get_isnull(slot);

	resultRelInfo = get_part(estate, values, nulls, slot->tts_tupleDescriptor);

	return resultRelInfo;
}

/* Wrap an attribute map (presumably from base partitioned table to part
 * as created by map_part_attrs in execMain.c) with an AttrMap. The new
 * AttrMap will contain a copy of the argument map.  The caller retains
 * the responsibility to dispose of the argument map eventually.
 *
 * If the input AttrNumber vector is empty or null, it is taken as an
 * identity map, i.e., a null AttrMap.
 */
AttrMap *makeAttrMap(int base_count, AttrNumber *base_map)
{
	int i, n, p;
	AttrMap *map;

	if ( base_count < 1 || base_map == NULL )
		return NULL;

	map = palloc0(sizeof(AttrMap) + base_count * sizeof(AttrNumber));

	for ( i = n = p = 0; i <= base_count; i++ )
	{
		map->attr_map[i] = base_map[i];

		if ( map->attr_map[i] != 0 ) 
		{
			if ( map->attr_map[i] > p ) p = map->attr_map[i];
			n++;
		}
	}	

	map->live_count = n;
	map->attr_max = p;
	map->attr_count = base_count;

	return map;
}

/* Invert a base-to-part attribute map to produce a part-to-base attribute map.
 * The result attribute map will have a attribute count at least as large as
 * the largest part attribute in the base map, however, the argument inv_count
 * may be used to force a larger count.
 *
 * The identity map, null, is handled specially.
 */
AttrMap *invertedAttrMap(AttrMap *base_map, int inv_count)
{
	AttrMap *inv_map;
	int i;

	if ( base_map == NULL )
		return NULL;

	if ( inv_count < base_map->attr_max )
	{
		inv_count = base_map->attr_max;
	}	

	inv_map = palloc0(sizeof(AttrMap) + inv_count * sizeof(AttrNumber));

	inv_map->live_count = base_map->live_count;
	inv_map->attr_max = base_map->attr_count;
	inv_map->attr_count = inv_count;

	for ( i = 1; i <= base_map->attr_count; i++ )
	{
		AttrNumber inv = base_map->attr_map[i];

		if ( inv == 0 ) continue;
		inv_map->attr_map[inv] = i;
	}

	return inv_map;
}

static AttrMap *copyAttrMap(AttrMap *original)
{
	AttrMap *copy;
	size_t sz;

	if ( original == NULL ) return NULL;

	sz = sizeof(AttrMap) + original->attr_count *sizeof(AttrNumber);
	copy = palloc0(sz);
	memcpy(copy, original, sz);

	return copy;
}

/* Compose two attribute maps giving a map with the same effect as applying
 * the first (input) map, then the second (output) map.
 */
AttrMap *compositeAttrMap(AttrMap *input_map, AttrMap *output_map)
{
	AttrMap *composite_map;
	int i;

	if ( input_map == NULL )
		return copyAttrMap(output_map);
	if ( output_map == NULL )
		return copyAttrMap(input_map);

	composite_map = copyAttrMap(input_map);
	composite_map->attr_max = output_map->attr_max;

	for ( i = 1; i <=input_map->attr_count; i++ )
	{
		AttrNumber k = attrMap(output_map, attrMap(input_map, i));
		composite_map->attr_map[i] = k;
	}

	return composite_map;
}

/* Use the given attribute map to convert an attribute number in the
 * base relation to an attribute number in the other relation.  Forgive
 * out-of-range attributes by mapping them to zero.  Treat null as
 * the identity map.
 */
AttrNumber attrMap(AttrMap *map, AttrNumber anum)
{
	if ( map == NULL )
		return anum;
	if ( 0 < anum && anum <= map->attr_count )
		return map->attr_map[anum];
	return 0;
}

/* Apply attrMap over an integer list of attribute numbers.
 */
List *attrMapIntList(AttrMap *map, List *attrs)
{
	ListCell *lc;
	List *remapped_list = NIL;

	foreach (lc, attrs)
	{
		int anum = (int)attrMap(map, (AttrNumber)lfirst_int(lc));
		remapped_list = lappend_int(remapped_list, anum);
	}
	return remapped_list;
}

/* For attrMapExpr below.
 *
 * Mutate Var nodes in an expression using the given attribute map.
 * Insist the Var nodes have varno == 1 and the that the mapping
 * yields a live attribute number (non-zero).
 */
static Node *apply_attrmap_mutator(Node *node, AttrMap *map)
{
	if ( node == NULL )
		return NULL;

	if (IsA(node, Var) )
	{
		AttrNumber anum = 0;
		Var *var = (Var*)node;
		Assert(var->varno == 1); /* in CHECK contraints */
		anum = attrMap(map, var->varattno);

		if ( anum == 0 )
		{
			/* Should never happen, but best caught early. */
			elog(ERROR, "attribute map discrepancy");
		}
		else if ( anum != var->varattno )
		{
			var = copyObject(var);
			var->varattno = anum;
		}
		return (Node *)var;
	}
	return expression_tree_mutator(node, apply_attrmap_mutator, (void *)map);
}

/* Apply attrMap over the Var nodes in an expression.
 */
Node *attrMapExpr(AttrMap *map, Node *expr)
{
	return apply_attrmap_mutator(expr, map);
}


/* Check compatibility of the attributes of the given partitioned
 * table and part for purposes of INSERT (through the partitioned
 * table) or EXCHANGE (of the part into the partitioned table).
 * Don't use any partitioning catalogs, because this must run
 * on the segment databases as well as on the entry database.
 *
 * If requested and needed, make a vector mapping the attribute
 * numbers of the partitioned table to corresponding attribute 
 * numbers in the part.  Represent the "unneeded" identity map
 * as null.
 *
 * base -- the partitioned table
 * part -- the part table
 * map_ptr -- where to store a pointer to the result, or NULL
 * throw -- whether to throw an error in case of incompatibility
 *
 * The implicit result is a vector one longer than the number
 * of attributes (existing or not) in the base relation.
 * It is returned through the map_ptr argument, if that argument
 * is non-null.
 *
 * The explicit result indicates whether the part is compatible
 * with the base relation.  If the throw argument is true, however,
 * an error is issued rather than returning false.
 *
 * Note that, in the map, element 0 is wasted and is always zero, 
 * so the vector is indexed by attribute number (origin 1).
 *
 * The i-th element of the map is the attribute number in 
 * the part relation that corresponds to attribute i of the  
 * base relation, or it is zero to indicate that attribute 
 * i of the base relation doesn't exist (has been dropped).
 *
 * This is a handy map for renumbering attributes for use with
 * part relations that may have a different configuration of 
 * "holes" than the partitioned table in which they occur.
 * 
 * Be sure to call this in the memory context in which the result
 * vector ought to be stored.
 *
 * Though some error checking is done, it is not comprehensive.
 * If internal assumptions about possible tuple formats are
 * correct, errors should not occur.  Still, the downside is
 * incorrect data, so use errors (not assertions) for the case.
 *
 * Checks include same number of non-dropped attributes in all 
 * parts of a partitioned table, non-dropped attributes in 
 * corresponding relative positions must match in name, type
 * and alignment, attribute numbers must agree with their
 * position in tuple, etc.
 */
bool
map_part_attrs(Relation base, Relation part, AttrMap **map_ptr, bool throw)
{	
	AttrNumber i = 1;
	AttrNumber n = base->rd_att->natts;
	FormData_pg_attribute *battr = NULL;

	AttrNumber j = 1;
	AttrNumber m = part->rd_att->natts;
	FormData_pg_attribute *pattr = NULL;

	AttrNumber *v = NULL;

	/* If we might want a map, allocate one. */
	if ( map_ptr != NULL )
	{
		v = palloc0(sizeof(AttrNumber)*(n+1));
		*map_ptr = NULL;
	}

	bool identical = TRUE;
	bool compatible = TRUE;

	/* For each matched pair of attributes ... */
	while ( i <= n && j <= m )
	{
		battr = base->rd_att->attrs[i-1];
		pattr = part->rd_att->attrs[j-1];

		/* Skip dropped attributes. */

		if ( battr->attisdropped )
		{
			i++;
			continue;
		}

		if ( pattr->attisdropped )
		{
			j++;
			continue;
		}

		/* Check attribute conformability requirements. */

		/* -- Names must match. */
		if ( strncmp(NameStr(battr->attname), NameStr(pattr->attname), NAMEDATALEN) != 0 )
		{
			if ( throw )
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("relation \"%s\" must have the same "
							 "column names and column order as \"%s\"",
							 RelationGetRelationName(part),
							 RelationGetRelationName(base)),
						 errOmitLocation(true)));
			compatible = FALSE;
			break;
		}

		/* -- Types must match. */
		if (battr->atttypid != pattr->atttypid)
		{
			if ( throw )
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("type mismatch for attribute \"%s\"",
							 NameStr((battr->attname))),
						 errOmitLocation(true)));
			compatible = FALSE;
			break;
		}

		/* -- Alignment should match, but check just to be safe. */
		if (battr->attalign != pattr->attalign )
		{
			if ( throw )
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("alignment mismatch for attribute \"%s\"",
							 NameStr((battr->attname))),
						 errOmitLocation(true)));
			compatible = FALSE;
			break;
		}

		/* -- Attribute numbers must match position (+1) in tuple. 
		 *    This is a hard requirement so always throw.  This could
		 *    be an assertion, except that we want to fail even in a 
		 *    distribution build.
		 */
		if ( battr->attnum != i || pattr->attnum != j )
			elog(ERROR,
					"attribute numbers out of order");

		/* Note any attribute number difference. */
		if ( i != j )
			identical = FALSE;

		/* If we're building a map, update it. */
		if ( v != NULL )
			v[i] = j;

		i++;
		j++;
	}

	if ( compatible )
	{
		/* Any excess attributes in parent better be marked dropped */
		for ( ; i <= n; i++ )
		{
			if ( !base->rd_att->attrs[i-1]->attisdropped )
			{
				if ( throw )
					/* the partitioned table has more columns than the part */
					ereport(ERROR,
							(errcode(ERRCODE_SYNTAX_ERROR),
							 errmsg("relation \"%s\" must have the same number columns as relation \"%s\"",
								 RelationGetRelationName(part),
								 RelationGetRelationName(base)),
							 errOmitLocation(true)));
				compatible = FALSE;
			}
		}

		/* Any excess attributes in part better be marked dropped */
		for ( ; j <= m; j++ )
		{
			if ( !part->rd_att->attrs[j-1]->attisdropped )
			{
				if ( throw )
					/* the partitioned table has fewer columns than the part */
					ereport(ERROR,
							(errcode(ERRCODE_SYNTAX_ERROR),
							 errmsg("relation \"%s\" must have the same number columns as relation \"%s\"",
								 RelationGetRelationName(part),
								 RelationGetRelationName(base)),
							 errOmitLocation(true)));
				compatible = FALSE;
			}
		}
	}

	/* Identical tuple descriptors should have the same number of columns */
	if (n != m)
	{
		identical = FALSE;
	}

	if ( !compatible )
	{

		Assert( !throw );
		if ( v != NULL )
			pfree(v);
		return FALSE;
	}

	/* If parent and part are the same, don't use a map */
	if ( identical && v != NULL )
	{
		pfree(v);
		v = NULL;
	}

	if ( map_ptr != NULL && v != NULL )
	{
		*map_ptr = makeAttrMap(n, v);
		pfree(v);
	}
	return TRUE;
}

/*
 * Maps the tuple descriptor of a part to a target tuple descriptor.
 * The mapping assume that the attributes in the target descriptor
 * could be in any position in the part descriptor.
 */
void
map_part_attrs_from_targetdesc(TupleDesc target, TupleDesc part, AttrMap **map_ptr)
{
	Assert(target);
	Assert(part);
	Assert(NULL == *map_ptr);

	int ntarget = target->natts;
	int npart = part->natts;

	FormData_pg_attribute *tattr = NULL; 
	FormData_pg_attribute *pattr = NULL;

	AttrNumber *mapper = palloc0(sizeof(AttrNumber)*(ntarget+1));

	/* Map every attribute in the target to an attribute 
	 * in the part tuple descriptor 
	 */
	for(AttrNumber i = 0; i < ntarget; i++)
	{
		tattr = target->attrs[i];
		mapper[i+1] = 0;

		if ( tattr->attisdropped ) 
		{
			continue;
		}

		/* Scan all attributes in the part tuple descriptor */
		for (AttrNumber j = 0; j < npart; j++)
		{
			pattr = part->attrs[j];
			if ( pattr->attisdropped )
			{
				continue;
			}

			if (strncmp(NameStr(tattr->attname), NameStr(pattr->attname), NAMEDATALEN) == 0 &&
					tattr->atttypid == pattr->atttypid && 
					tattr->attalign == pattr->attalign )
			{
				mapper[i+1] = j+1;
				/* Continue with next attribute in the target list */
				break;
			}
		}
	}

	Assert ( map_ptr != NULL && mapper != NULL );

	*map_ptr = makeAttrMap(ntarget, mapper);
	pfree(mapper);
}



/*
 * Clear any partition state held in the argument EState node.  This is
 * called during ExecEndPlan and is not, itself, recursive.
 *
 * At present, the only required cleanup is to decrement reference counts
 * in any tuple descriptors held in slots in the partition state.
 */
static void
ClearPartitionState(EState *estate)
{
	PartitionState *pstate = estate->es_partition_state;
	HASH_SEQ_STATUS hash_seq_status;
	ResultPartHashEntry *entry;

	if ( pstate == NULL || pstate->result_partition_hash == NULL )
		return;

	/* Examine each hash table entry. */
	hash_freeze(pstate->result_partition_hash); 
	hash_seq_init(&hash_seq_status, pstate->result_partition_hash);
	while ( (entry = hash_seq_search(&hash_seq_status)) )
	{
		ResultPartHashEntry *part = (ResultPartHashEntry*)entry;
		ResultRelInfo *info = &estate->es_result_relations[part->offset];
		if ( info->ri_partSlot )
		{
			Assert( info->ri_partInsertMap ); /* paired with slot */
			if ( info->ri_partSlot->tts_tupleDescriptor )
				ReleaseTupleDesc(info->ri_partSlot->tts_tupleDescriptor);
			ExecClearTuple(info->ri_partSlot);
		}
	}
	/* No need for hash_seq_term() since we iterated to end. */
}


