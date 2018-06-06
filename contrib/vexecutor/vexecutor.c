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
#include "executor/nodeAgg.h"
#include "executor/nodeTableScan.h"
#include "catalog/catquery.h"
#include "cdb/cdbvars.h"
#include "nodes/execnodes.h"
#include "utils/memaccounting.h"
#include "execVScan.h"
#include "execVQual.h"
#include "vexecutor.h"
#include "nodeVMotion.h"
#include "vagg.h"

PG_MODULE_MAGIC;
int BATCHSIZE = 1024;
static int MINBATCHSIZE = 1;
static int MAXBATCHSIZE = 4096;
/*
 * hook function
 */
static PlanState* VExecInitNode(Plan *node,EState *eState,int eflags,bool isAlienPlanNode,MemoryAccount** pcurMemoryAccount);
static PlanState* VExecVecNode(PlanState *Node, PlanState *parentNode, EState *eState,int eflags);
static bool VExecProcNode(PlanState *node,TupleTableSlot** pRtr);
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

	vmthd.vectorized_executor_enable = true;
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
void BackportTupleDescriptor(PlanState* ps,TupleDesc td)
{
	cqContext *pcqCtx;
	HeapTuple tuple;
	Oid oidtypeid;
	Form_pg_type  typeForm;
	for(int i = 0;i < td->natts; ++i)
	{
		oidtypeid = GetNtype(td->attrs[i]->atttypid);

		if(InvalidOid == oidtypeid)
			continue;

		pcqCtx = caql_beginscan(
				NULL,
				cql("SELECT * FROM pg_type "
							" WHERE oid = :1 ",
					ObjectIdGetDatum(oidtypeid)));
		tuple = caql_getnext(pcqCtx);

		/* 
		 * the tuple descriptor of Agg node could be {vtype, vtype, ntype, ntype},
		 * we should convert the vtype to ntype and skip the ntype which alreay exists.
		 */
		if (!HeapTupleIsValid(tuple))
            goto endscan;

		typeForm = (Form_pg_type) GETSTRUCT(tuple);

		td->attrs[i]->atttypid = oidtypeid;
		td->attrs[i]->attlen = typeForm->typlen;
		td->attrs[i]->attbyval = typeForm->typbyval;
		td->attrs[i]->attalign = typeForm->typalign;
		td->attrs[i]->attstorage = typeForm->typstorage;

endscan:
		caql_endscan(pcqCtx);
	}
}

static PlanState* VExecInitNode(Plan *node,EState *eState,int eflags,bool isAlienPlanNode,MemoryAccount** pcurMemoryAccount)
{
	elog(DEBUG3, "PG VEXECINIT NODE");

	PlanState *state = NULL;

	if(NULL == node)
		return NULL;

	switch (nodeTag(node) )
	{
	case T_Agg:
		*pcurMemoryAccount = CREATE_EXECUTOR_MEMORY_ACCOUNT(isAlienPlanNode, node, Agg);

		START_MEMORY_ACCOUNT(*pcurMemoryAccount);
		{
			state = VExecInitAgg(node, eState, eflags);
		}
		END_MEMORY_ACCOUNT();
		break;
	default:
		break;
	}

	return state;
}

#define HAS_EXECUTOR_MEMORY_ACCOUNT(planNode, NodeType) \
	(NULL != planNode->memoryAccount && \
	MEMORY_OWNER_TYPE_Exec_##NodeType == planNode->memoryAccount->ownerType)

static void
VExecVecTableScan(PlanState *node, PlanState *parentNode, EState *eState,int eflags)
{
	TupleDesc td = ((TableScanState *)node)->ss.ss_ScanTupleSlot->tts_tupleDescriptor;
	((TableScanState *)node)->ss.ss_ScanTupleSlot->PRIVATE_tb = PointerGetDatum(tbGenerate(td->natts,BATCHSIZE));
	node->ps_ResultTupleSlot->PRIVATE_tb = PointerGetDatum(tbGenerate(td->natts,BATCHSIZE));

	/* if V->N */
	if( NULL == parentNode ||
		NULL == parentNode->vectorized ||
		!((VectorizedState *)parentNode->vectorized)->vectorized)
	{
		BackportTupleDescriptor(node,node->ps_ResultTupleSlot->tts_tupleDescriptor);
		ExecAssignResultType(node, node->ps_ResultTupleSlot->tts_tupleDescriptor);
	}

	return;
}

static void
VExecVecAgg(PlanState *node, PlanState *parentNode, EState *eState,int eflags)
{
	int aggno;
	VectorizedState *vstate;
	AggState *aggstate = (AggState*)node;
	for (aggno = 0; aggno < aggstate->numaggs; aggno++)
	{
		AggStatePerAgg peraggstate = &aggstate->peragg[aggno];
		TupleDesc td = peraggstate->evalslot->tts_tupleDescriptor;
		peraggstate->evalslot->PRIVATE_tb = PointerGetDatum(tbGenerate(td->natts,BATCHSIZE));
	}

	Assert(NULL != node->vectorized);
	vstate = (VectorizedState*)node->vectorized;
	vstate->transdata = InitAggVectorizedData(aggstate);
	vstate->aggslot = (TupleTableSlot**)palloc0(sizeof(TupleTableSlot*) * aggstate->numaggs);

	vstate->batchGroupData = (BatchAggGroupData*)palloc0(sizeof(BatchAggGroupData));
	vstate->groupData = (GroupData*)palloc0(sizeof(GroupData) * BATCHSIZE);
	vstate->indexList = (int*)palloc0(sizeof(int) * BATCHSIZE);

	return;
}

static void
VExecVecMotion(PlanState *node, PlanState *parentNode, EState *eState,int eflags)
{
	Plan *plan = node->plan;

	Assert(NULL != plan);

	if( NULL == parentNode ||
		NULL == parentNode->vectorized ||
		((Motion*)plan)->sendSorted ||
		((Motion*)plan)->motionType == MOTIONTYPE_HASH ||
		!((VectorizedState *)parentNode->vectorized)->vectorized)
	{
		((VectorizedState *)node->vectorized)->vectorized = false;
		plan->vectorized = false;
		BackportTupleDescriptor(node,node->ps_ResultTupleSlot->tts_tupleDescriptor);
		ExecAssignResultType(node, node->ps_ResultTupleSlot->tts_tupleDescriptor);
	}
	else
	{
		((VectorizedState *)node->vectorized)->vectorized = true;
	}
}

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

	switch (nodeTag(node) )
	{
		case T_AppendOnlyScanState:
		case T_ParquetScanState:
		case T_TableScanState:
			if(Gp_role != GP_ROLE_DISPATCH && vstate->vectorized)
			{
				if(HAS_EXECUTOR_MEMORY_ACCOUNT(plan, TableScan))
				{
					START_MEMORY_ACCOUNT(plan->memoryAccount);
					VExecVecTableScan(node, parentNode, eState, eflags);
					END_MEMORY_ACCOUNT();
				}
				else
					VExecVecTableScan(node, parentNode, eState, eflags);
			}
			break;
		case T_AggState:
			if(Gp_role != GP_ROLE_DISPATCH && vstate->vectorized)
			{
				if(HAS_EXECUTOR_MEMORY_ACCOUNT(plan, Agg))
				{
					START_MEMORY_ACCOUNT(plan->memoryAccount);
					VExecVecAgg(node, parentNode, eState, eflags);
					END_MEMORY_ACCOUNT();
				}
				else
					VExecVecAgg(node, parentNode, eState, eflags);
			}
			break;
		case T_MotionState:
			if(Gp_role != GP_ROLE_DISPATCH && vstate->vectorized)
			{
				if(HAS_EXECUTOR_MEMORY_ACCOUNT(plan, Motion))
				{
					START_MEMORY_ACCOUNT(plan->memoryAccount);
					VExecVecMotion(node, parentNode, eState, eflags);
					END_MEMORY_ACCOUNT();
				}
				else
					VExecVecMotion(node, parentNode, eState, eflags);
			}
			break;
		default:
			((VectorizedState *)node->vectorized)->vectorized = false;
			break;
	}

	/* recursively */
	if(NULL != node->lefttree)
		VExecVecNode(node->lefttree, node, eState, eflags);
	if(NULL != node->righttree)
		VExecVecNode(node->righttree, node, eState, eflags);

	return node;
}

static bool VExecProcNode(PlanState *node,TupleTableSlot** pRtr)
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
        case T_AggState:
            result = ExecVAgg((AggState*)node);
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
HasVecExecOprator(Plan* plan)
{
	NodeTag tag = nodeTag(plan);
	bool result = false;

	switch(tag)
	{
	case T_AppendOnlyScan:
	case T_ParquetScan:
	case T_Agg:
		result = true;
		break;
	case T_Motion:
		result = (((Motion*)plan)->motionType != MOTIONTYPE_HASH) ? true : false;
		break;
	default:
		result = false;
		break;
	}

	return result;
}

