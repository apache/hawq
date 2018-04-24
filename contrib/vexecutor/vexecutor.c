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


#include "vexecutor.h"
#include "utils/guc.h"
#include "vcheck.h"
#include "miscadmin.h"
#include "tuplebatch.h"
#include "executor/nodeTableScan.h"
#include "catalog/catquery.h"
#include "cdb/cdbvars.h"
#include "utils/memaccounting.h"
#include "execVScan.h"
#include "execVQual.h"
#include "vexecutor.h"
#include "nodeVMotion.h"

PG_MODULE_MAGIC;
int BATCHSIZE = 1024;
static int MINBATCHSIZE = 1;
static int MAXBATCHSIZE = 4096;
/*
 * hook function
 */
static PlanState* VExecInitNode(Plan *node,EState *eState,int eflags);
static PlanState* VExecVecNode(PlanState *Node, PlanState *parentNode, EState *eState,int eflags);
static TupleTableSlot* VExecProcNode(PlanState *node,TupleTableSlot** pRtr);
static bool VExecEndNode(PlanState *node);
extern int currentSliceId;

/*
 * _PG_init is called when the module is loaded. In this function we save the
 * previous utility hook, and then install our hook to pre-intercept calls to
 * the copy command.
 */
void
_PG_init(void)
{
	elog(DEBUG3, "PG INIT VEXECTOR");
	vmthd.CheckPlanVectorized_Hook = CheckAndReplacePlanVectorized;
	vmthd.ExecInitExpr_Hook = VExecInitExpr;
	vmthd.ExecInitNode_Hook = VExecInitNode;
	vmthd.ExecVecNode_Hook = VExecVecNode;
	vmthd.ExecProcNode_Hook = VExecProcNode;
	vmthd.ExecEndNode_Hook = VExecEndNode;
	vmthd.GetNType = GetNtype;
	DefineCustomBoolVariable("vectorized_executor_enable",
	                         gettext_noop("enable vectorized executor"),
	                         NULL,
	                         &vmthd.vectorized_executor_enable,
	                         PGC_USERSET,
	                         NULL,NULL);

    DefineCustomIntVariable("vectorized_batch_size",
							gettext_noop("set vectorized execution batch size"),
                            NULL,
                            &BATCHSIZE,
                            MINBATCHSIZE,MAXBATCHSIZE,
							PGC_USERSET,
							NULL,NULL);
}

/*
 * _PG_fini is called when the module is unloaded. This function uninstalls the
 * extension's hooks.
 */
void
_PG_fini(void)
{
	elog(DEBUG3, "PG FINI VEXECTOR");
	vmthd.CheckPlanVectorized_Hook = NULL;
	vmthd.ExecInitExpr_Hook = NULL;
	vmthd.ExecInitNode_Hook = NULL;
	vmthd.ExecVecNode_Hook = NULL;
	vmthd.ExecProcNode_Hook = NULL;
	vmthd.ExecEndNode_Hook = NULL;
}

/*
 *
 * backportTupleDescriptor
 *			Backport the TupleDesc information to normal type. Due to Vectorized type unrecognizable in normal execution node,
 *			the TupleDesc structure should backport metadata to normal type before we do V->N result pop up.
 */
static void backportTupleDescriptor(PlanState* ps,TupleDesc td)
{
	cqContext *pcqCtx;
	HeapTuple tuple;
	Oid oidtypeid;
	Form_pg_type  typeForm;
	for(int i = 0;i < td->natts; ++i)
	{
		oidtypeid = GetNtype(td->attrs[i]->atttypid);

		pcqCtx = caql_beginscan(
				NULL,
				cql("SELECT * FROM pg_type "
							" WHERE oid = :1 ",
					ObjectIdGetDatum(oidtypeid)));
		tuple = caql_getnext(pcqCtx);

		if (!HeapTupleIsValid(tuple))
			elog(ERROR, "cache lookup failed for type %u", oidtypeid);
		typeForm = (Form_pg_type) GETSTRUCT(tuple);

		td->attrs[i]->atttypid = oidtypeid;
		td->attrs[i]->attlen = typeForm->typlen;
		td->attrs[i]->attbyval = typeForm->typbyval;
		td->attrs[i]->attalign = typeForm->typalign;
		td->attrs[i]->attstorage = typeForm->typstorage;

		caql_endscan(pcqCtx);
	}

	ExecAssignResultType(ps,td);
}

static PlanState* VExecInitNode(Plan *node,EState *eState,int eflags)
{
	elog(DEBUG3, "PG VEXECINIT NODE");

	return NULL;
}

#define HAS_EXECUTOR_MEMORY_ACCOUNT(planNode, NodeType) \
	(NULL != planNode->memoryAccount && \
	MEMORY_OWNER_TYPE_Exec_##NodeType == planNode->memoryAccount->ownerType)

/*
 * when vectorized_executor_enable is ON, we have to process the plan.
 */
static PlanState*
VExecVecNode(PlanState *node, PlanState *parentNode, EState *eState,int eflags)
{
	Plan *plan = node->plan;
	VectorizedState *vstate = (VectorizedState*)palloc0(sizeof(VectorizedState));

	elog(DEBUG3, "PG VEXECINIT NODE");

	if(NULL == node)
		return node;

	/* set state */
	node->vectorized = (void*)vstate;

	vstate->vectorized = plan->vectorized;
	vstate->parent = parentNode;

	if(Gp_role != GP_ROLE_DISPATCH)
	{
		switch (nodeTag(node) )
		{
			case T_AppendOnlyScan:
			case T_ParquetScan:
			case T_TableScanState:
				if(HAS_EXECUTOR_MEMORY_ACCOUNT(plan, TableScan))
					START_MEMORY_ACCOUNT(plan->memoryAccount);

				TupleDesc td = ((TableScanState *)node)->ss.ss_ScanTupleSlot->tts_tupleDescriptor;
				((TableScanState *)node)->ss.ss_ScanTupleSlot->PRIVATE_tb = PointerGetDatum(tbGenerate(td->natts,BATCHSIZE));
				node->ps_ResultTupleSlot->PRIVATE_tb = PointerGetDatum(tbGenerate(td->natts,BATCHSIZE));

				/* if V->N */
				if( NULL == parentNode ||
					NULL == parentNode->vectorized ||
					!((VectorizedState *)parentNode->vectorized)->vectorized)
					backportTupleDescriptor(node,node->ps_ResultTupleSlot->tts_tupleDescriptor);

				if(HAS_EXECUTOR_MEMORY_ACCOUNT(plan, TableScan))
					END_MEMORY_ACCOUNT();
				break;
			case T_MotionState:
				if( NULL == parentNode ||
					NULL == parentNode->vectorized ||
					((Motion*)plan)->sendSorted ||
					((Motion*)plan)->motionType == MOTIONTYPE_HASH ||
					!((VectorizedState *)parentNode->vectorized)->vectorized)
					((VectorizedState *)node->vectorized)->vectorized = false;
				else
					((VectorizedState *)node->vectorized)->vectorized = true;
				break;
			default:
				((VectorizedState *)node->vectorized)->vectorized = false;
				break;
		}
	}

	/* recursively */
	if(NULL != node->lefttree)
		VExecVecNode(node->lefttree, node, eState, eflags);
	if(NULL != node->righttree)
		VExecVecNode(node->righttree, node, eState, eflags);

	return node;
}

static TupleTableSlot* VExecProcNode(PlanState *node,TupleTableSlot** pRtr)
{
	if(!((VectorizedState*)node->vectorized)->vectorized)
		return false;

	bool ret = true;
    TupleTableSlot* result = NULL;
    switch(nodeTag(node))
    {
        case T_ParquetScanState:
        case T_AppendOnlyScanState:
        case T_TableScanState:
            result = ExecTableVScanVirtualLayer((TableScanState*)node);
            break;
		case T_MotionState:
			result = ExecVMotionVirtualLayer((MotionState*)node);
			break;
        default:
			ret = false;
            break;
    }
	*pRtr = result;
    return ret;
}

static bool VExecEndNode(PlanState *node)
{
    if(Gp_role == GP_ROLE_DISPATCH)
		return false;

	elog(DEBUG3, "PG VEXEC END NODE");
	bool ret = false;
	switch (nodeTag(node))
	{
		case T_AppendOnlyScanState:
		case T_ParquetScanState:
		case T_TableScanState:
			ExecEndTableScan((TableScanState *)node);
			ret = true;
			break;
		default:
			break;
	}
	return ret;
}

/* check if there is an vectorized execution operator */
bool
HasVecExecOprator(NodeTag tag)
{
	bool result = false;

	switch(tag)
	{
	case T_AppendOnlyScan:
	case T_ParquetScan:
		result = true;
		break;
	default:
		result = false;
		break;
	}

	return result;
}

