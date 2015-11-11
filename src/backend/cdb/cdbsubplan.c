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
 * cdbsubplan.c
 *	  Provides routines for preprocessing initPlan subplans
 *		and executing queries with initPlans
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"
#include "executor/nodeSubplan.h"		/* For ExecSetParamPlan */
#include "executor/executor.h"          /* For CreateExprContext */
#include "cdb/cdbdisp.h"
#include "cdb/cdbplan.h"
#include "cdb/cdbllize.h"
#include "cdb/cdbsubplan.h"
#include "cdb/cdbvars.h"                /* currentSliceId */
#include "cdb/ml_ipc.h"

#include "utils/debugbreak.h"

typedef struct ParamWalkerContext
{
	plan_tree_base_prefix base; /* Required prefix for plan_tree_walker/mutator */
	List	   *params;
}	ParamWalkerContext;

static bool param_walker(Node *node, ParamWalkerContext * context);
static Oid	findParamType(List *params, int paramid);

/*
 * Function preprocess_initplans() is called from ExecutorRun running a
 * parallel plan on the QD.  The call happens prior to dispatch of the
 * main plan, and only if there are some initplans.
 *
 * Argument queryDesc is the one passed in to ExecutorRun.
 *
 * The function loops through the estate->es_param_exec_vals array, which
 * has plan->nParamExec elements.  Each element is a ParamExecData struct,
 * and the index of the element in the array is the paramid of the Param
 * node in the Plan that corresponds to the result of the subquery.
 *
 * The execPlan member points to a SubPlanState struct for the
 * subquery.  The value and isnull members hold the result
 * of executing the SubPlan.
 * I think that the order of the elements in this array guarantees
 * that for a subplan X within a subplan Y, X will come before Y in the array.
 * If a subplan returns multiple columns (like a MULTIEXPR_SUBLINK), each will be
 * a separate entry in the es_param_exec_vals array, but they will all have
 * the same value for execPlan.
 * In order to evaluate a subplan, we call ExecSetParamPlan.
 * This is a postgres function, but has been modified from its original form
 * to parallelize subplans. Inside ExecSetParamPlan, the
 * datum result(s) of the subplan are stuffed into the value field
 * of the ParamExecData struct(s).	It finds the proper one based on the
 * setParam list in the SubPlan node.
 * In order to handle SubPlans of SubPlans, we pass in the values of the
 * estate->es_param_exec_vals as ParamListInfo structs to the ExecSetParamPlan call.
 * These are then serialized into the mppexec all as parameters.  In this manner, the
 * result of a SubPlan of a SubPlan is available.
 */
void
preprocess_initplans(QueryDesc *queryDesc)
{
	ParamListInfo originalPli,
				augmentedPli;
	int			i;
//	Plan	   *plan = queryDesc->plantree;
	EState	   *estate = queryDesc->estate;
	int			originalRoot,
                originalSlice,
				rootIndex;

	if (queryDesc->plannedstmt->nCrossLevelParams == 0)
		return;

	originalPli = queryDesc->params;
	originalRoot = RootSliceIndex(queryDesc->estate);

    originalSlice = LocallyExecutingSliceIndex(queryDesc->estate);
	Assert(originalSlice == 0); /* Original slice being executed is slice 0 */

	/*
	 * Loop through the estate->es_param_exec_vals. This array has an element
	 * for each PARAM_EXEC (internal) param, and a pointer to the SubPlanState
	 * to execute to evaluate it. It seems that they are created in the proper
	 * order, i.e. if a subplan x has a sublan y, then y will come before x in
	 * the es_param_exec_vals array.
	 */
	for (i = 0; i < queryDesc->plannedstmt->nCrossLevelParams; i++)
	{
		ParamExecData *prm;
		SubPlanState *sps;

		prm = &estate->es_param_exec_vals[i];
		sps = (SubPlanState *) prm->execPlan;

		/*
		 * Append all the es_param_exec_vals datum values on to the external
		 * parameter list so they can be serialized in the mppexec call to the
		 * QEs.  Do this inside the loop since later initplans may depend on
		 * the results of earlier ones.
		 *
		 * TODO Some of the work of addRemoteExecParamsToParmList could be
		 *		factored out of the loop.
		 */
		augmentedPli = addRemoteExecParamsToParamList(queryDesc->plannedstmt,
													  originalPli,
													  estate->es_param_exec_vals);

		if (sps != NULL)
		{
            SubPlan    *subplan = (SubPlan *)sps->xprstate.expr;

            Assert(IsA(subplan, SubPlan) &&
                   subplan->qDispSliceId > 0);

			sps->planstate->plan->nParamExec = queryDesc->plannedstmt->nCrossLevelParams;
			sps->planstate->plan->nMotionNodes = queryDesc->plannedstmt->nMotionNodes;
			sps->planstate->plan->dispatch = DISPATCH_PARALLEL;

			/*
			 * Adjust for the slice to execute on the QD.
			 */
			rootIndex = subplan->qDispSliceId;
			queryDesc->estate->es_sliceTable->localSlice = rootIndex;

			/* set our global sliceid variable for elog. */
			currentSliceId = rootIndex;

			/*
			 * This runs the SubPlan and puts the answer back into prm->value.
			 */
			queryDesc->params = augmentedPli;

			/*
			 * Use ExprContext to set the param. If ExprContext is not initialized,
			 * create a new one here. (see MPP-3511)
			 */
			if (sps->planstate->ps_ExprContext == NULL)
				sps->planstate->ps_ExprContext = CreateExprContext(estate);
			
			/* MPP-12048: Set the right slice index before execution. */
			Assert( (subplan->qDispSliceId > queryDesc->plannedstmt->nMotionNodes)  &&
					(subplan->qDispSliceId <=
							(queryDesc->plannedstmt->nMotionNodes
							+ queryDesc->plannedstmt->nInitPlans) )   );

			Assert(LocallyExecutingSliceIndex(sps->planstate->state) == subplan->qDispSliceId);
		    //sps->planstate->state->es_cur_slice_idx = subplan->qDispSliceId;

			ExecSetParamPlan(sps, sps->planstate->ps_ExprContext, queryDesc);

			/*
			 * We dispatched, and have returned. We may have used the
			 * interconnect; so let's bump the interconnect-id.
			 */
			queryDesc->estate->es_sliceTable->ic_instance_id = ++gp_interconnect_id;
		}

		queryDesc->params = originalPli;
		queryDesc->estate->es_sliceTable->localSlice = originalSlice;
		currentSliceId = originalSlice;

		pfree(augmentedPli);
	}
}

/*
 * Function: addRemoteExecParamsToParamList()
 *
 * Creates a new ParamListInfo array from the existing one by appending
 * the array of ParamExecData (internal parameter) values as a new Param
 * type: PARAM_EXEC_REMOTE.
 *
 * When the query eventually runs (on the QD or a QE), it will have access
 * to the augmented ParamListInfo (locally or through serialization).
 *
 * Then, rather than lazily-evaluating the SubPlan to get its value (as for
 * an internal parameter), the plan will just look up the param value in the
 * ParamListInfo (as for an external parameter).
 */
ParamListInfo
addRemoteExecParamsToParamList(PlannedStmt *stmt, ParamListInfo extPrm, ParamExecData *intPrm)
{
	ParamListInfo augPrm;
	ParamWalkerContext context;
	int			i,
				j;
	int			nParams;
	ListCell *lc;
	Plan	   *plan = stmt->planTree;
	List	   *rtable = stmt->rtable;
	int			nIntPrm = stmt->nCrossLevelParams;

	if (nIntPrm == 0)
		return extPrm;
	Assert(intPrm != NULL);		/* So there must be some internal parameters. */

	/* Count existing external parameters. */
	nParams = (extPrm == NULL ? 0 : extPrm->numParams);

	/* Allocate space for augmented array and set final sentinel. */
	augPrm = palloc0(sizeof(ParamListInfoData) + (nParams + nIntPrm)* sizeof(ParamExternData));
	augPrm->numParams = nParams + nIntPrm;

	/* Copy in the existing external parameter entries. */
	for (i = 0; i < nParams; i++)
	{
		memcpy(&augPrm->params[i], &extPrm->params[i], sizeof(ParamExternData));
	}

	/*
	 * Walk the plan, looking for Param nodes of kind PARAM_EXEC, i.e.,
	 * executor internal parameters.
	 *
	 * We need these for their paramtype field, which isn't available in either
	 * the ParamExecData struct or the SubPlan struct.
	 */

	exec_init_plan_tree_base(&context.base, stmt);
	context.params = NIL;
	param_walker((Node *) plan, &context);
	
	/* 
	 * This code, unfortunately, duplicates code in the param_walker case
	 * for T_SubPlan.  That code checks for Param nodes in Function RTEs in
	 * the range table.  The outer range table is in the parsetree, though,
	 * so we check it specially here.
	 */
	foreach(lc, rtable)
	{
		RangeTblEntry *rte = lfirst(lc);
		if ( rte->rtekind == RTE_FUNCTION || rte->rtekind == RTE_TABLEFUNCTION )
		{
			FuncExpr *fexpr = (FuncExpr*)rte->funcexpr;
			param_walker((Node *) fexpr, &context);
		}
		else if ( rte->rtekind == RTE_VALUES )
			param_walker((Node*)rte->values_lists, &context);
	}

	if (context.params == NIL)
	{
		/* We apparently have an initplan with no corresponding parameter.
		 * This shouldn't happen, but we had a bug once (MPP-239) because
		 * we weren't looking for parameters in Function RTEs.  So we still 
		 * better check.  The old correct, but unhelpful to ENG, message was 
		 * "Subquery datatype information unavailable."
		 */
		ereport(ERROR,
				(errcode(ERRCODE_CDB_INTERNAL_ERROR),
				 errmsg("no parameter found for initplan subquery")));
	}

	/*
	 * Now initialize the ParamListInfo elements corresponding to the
	 * initplans we're "parameterizing".  Use the datatype info harvested
	 * above.
	 */
	for (i = 0; i < nIntPrm; i++)
	{
		j = nParams + i;


		augPrm->params[j].ptype = findParamType(context.params, i);
		augPrm->params[j].isnull = intPrm[i].isnull;
		augPrm->params[j].value = intPrm[i].value;
	}

	list_free(context.params);

	return augPrm;
}

/*
 * Helper function param_walker() walks a plan and adds any Param nodes
 * to a list in the ParamWalkerContext.
 *
 * This list is input to the function findParamType(), which loops over the
 * list looking for a specific paramid, and returns its type.
 */
bool
param_walker(Node *node, ParamWalkerContext * context)
{
	if (node == NULL)
		return false;

	if (nodeTag(node) == T_Param)
	{
		Param	   *param = (Param *) node;

		if (param->paramkind == PARAM_EXEC)
		{
			context->params = lappend(context->params, param);
			return false;
		}
	}
	return plan_tree_walker(node, param_walker, context);
}

/*
 * Helper function findParamType() iterates over a list of Param nodes,
 * trying to match on the passed-in paramid. Returns the paramtype of a
 * match, else error.
 */
Oid
findParamType(List *params, int paramid)
{
	ListCell   *l;

	foreach(l, params)
	{
		Param	   *p = lfirst(l);

		if (p->paramid == paramid)
			return p->paramtype;
	}

	ereport(ERROR,
			(errcode(ERRCODE_CDB_INTERNAL_ERROR),
			 errmsg("Failed to locate datatype for paramid %d",
					paramid
					)));

	return InvalidOid;
}
