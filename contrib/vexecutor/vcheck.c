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

#include "postgres.h"
#include "access/htup.h"
#include "catalog/catquery.h"
#include "catalog/pg_operator.h"
#include "cdb/cdbappendonlyam.h"
#include "cdb/cdbllize.h"
#include "parser/parse_oper.h"
#include "nodes/makefuncs.h"
#include "nodes/primnodes.h"
#include "nodes/plannodes.h"
#include "nodes/relation.h"
#include "optimizer/walkers.h"
#include "utils/lsyscache.h"
#include "utils/syscache.h"
#include "vcheck.h"
#include "vexecutor.h"


typedef struct VecTypeHashEntry
{
	Oid src;
	Oid dest;
}VecTypeHashEntry;

/* Map between the vectorized types and non-vectorized types */
static HTAB *hashMapN2V = NULL;
/* Map between the vectorized types and non-vectorized types */
static HTAB *hashMapV2N = NULL;

/*
 * We check the expressions tree recursively becuase the args can be a sub expression,
 * we must check the return type of sub expression to fit the parent expressions.
 * so the retType in Vectorized is a temporary values, after we check on expression,
 * we set the retType of this expression, and transfer this value to his parent.
 */
typedef struct VectorizedContext
{
	plan_tree_base_prefix base; /* Required prefix for plan_tree_walker/mutator */
	Oid retType;
	bool	 replace;
}VectorizedContext;

/*
 * Check all the expressions if they can be vectorized
 * NOTE: if an expressions is vectorized, we return false...,because we should check
 * all the expressions in the Plan node, if we return true, then the walker will be
 * over...
 */
static bool
CheckVectorizedExpression(Node *node, VectorizedContext *ctx)
{
	if(NULL == node)
		return false;

	if(is_plan_node(node))
		return false;

	//check the type of Var if it can be vectorized
	if(IsA(node, Var))
	{
		Var *var = (Var*)node;
		Oid vtype = GetVtype(var->vartype);
		if(InvalidOid == vtype)
			return true;
		ctx->retType = vtype;
		if(ctx->replace)
			var->vartype = vtype;
		return false;
	}

	//Const treat as can be vectorzied, its return type is non-vectorized type
	//because we support the function like this: vtype op(vtype, const);
	if(IsA(node, Const))
	{
		Const *c = (Const*)node;
		ctx->retType = c->consttype;
		return false;
	}

	//OpExpr:args, return types should can be vectorized,
	//and there must exists an vectorized function to implement the operator
	if(IsA(node, OpExpr))
	{
		OpExpr *op = (OpExpr*)node;
		Node *argnode = NULL;
		Oid ltype, rtype, rettype;
		Form_pg_operator voper;
		HeapTuple tuple;

		//OpExpr mostly have two args, check the first one
		argnode = linitial(op->args);
		if(CheckVectorizedExpression(argnode, ctx))
			return true;

		ltype = ctx->retType;

		//check the second one
		argnode = lsecond(op->args);
		if(CheckVectorizedExpression(argnode, ctx))
			return true;

		rtype = ctx->retType;

		//check the return type
		rettype = GetVtype(op->opresulttype);
		if(InvalidOid == rettype)
			return true;


		//get the vectorized operator functions
		//NOTE:we have no ParseState now, Give the NULL value is OK but not good...
		tuple = oper(NULL, list_make1(makeString(get_opname(op->opno))),
			ltype, rtype, false, -1);
		if(NULL == tuple)
			return true;

		voper = (Form_pg_operator)GETSTRUCT(tuple);
		if(voper->oprresult != rettype)
		{
			ReleaseOperator(tuple);
			return true;
		}

		if(ctx->replace)
		{
			op->opresulttype = rettype;
			op->opfuncid = voper->oprcode;
		}

		ReleaseOperator(tuple);

		ctx->retType = rettype;
		return false;
	}

	//now, other nodes treat as can not be vectorized
	return plan_tree_walker(node, CheckVectorizedExpression, ctx);;
}

/*
 * check an plan node, all the expressions in it should be checked
 * set the flag if an plan node can be vectorized
 */
static bool
CheckPlanNodeWalker(PlannerInfo *root, Plan *plan)
{
	VectorizedContext ctx;

	if(plan->vectorized)
		return true;

	/* skip to check if there are no corresponding vec-exec-operators*/
	if(!HasVecExecOprator(nodeTag(plan)))
	{
		plan->vectorized = false;
		return true;
	}

	planner_init_plan_tree_base(&ctx.base, root);

	ctx.replace =false;

	ctx.retType = InvalidOid;
	plan->vectorized = !plan_tree_walker((Node*)plan,
							CheckVectorizedExpression,
							 &ctx);

	return true;
}

/*
 * check the plan tree
 */
static Plan*
CheckPlanVectorzied(PlannerInfo *root, Plan *plan)
{
	if(NULL == plan)
		return plan;

	CheckPlanVectorzied(root, plan->lefttree);
	CheckPlanVectorzied(root, plan->righttree);
	CheckPlanNodeWalker(root, plan);

	return plan;
}

/*
 * Replace the non-vectorirzed type to vectorized type
 */
static bool
ReplacePlanNodeWalker(PlannerInfo *root, Plan *plan)
{
	VectorizedContext ctx;

	if(!plan->vectorized)
		return true;

	/* skip to replace if there are no corresponding vec-exec-operators*/
	if(!HasVecExecOprator(nodeTag(plan)))
	{
		plan->vectorized = false;
		return true;
	}

	planner_init_plan_tree_base(&ctx.base, root);

	ctx.replace = true;

	ctx.retType = InvalidOid;
	plan_tree_walker((Node*)plan,
					CheckVectorizedExpression,
					&ctx);


	return true;
}

/*
 * check the plan tree
 */
static Plan*
ReplacePlanVectorzied(PlannerInfo *root, Plan *plan)
{
	if(NULL == plan)
		return plan;

	ReplacePlanVectorzied(root, plan->lefttree);
	ReplacePlanVectorzied(root, plan->righttree);
	ReplacePlanNodeWalker(root, plan);

	return plan;
}

Plan*
CheckAndReplacePlanVectorized(PlannerInfo *root, Plan *plan)
{
	plan = CheckPlanVectorzied(root, plan);
	return ReplacePlanVectorzied(root, plan);
}

/*
 * map non-vectorized type to vectorized type.
 * To scan the PG_TYPE is inefficient, so we create a hashtable to map
 * the vectorized type and non-vectorized types.
 */
Oid GetVtype(Oid ntype)
{
	HeapTuple tuple;
	cqContext *pcqCtx;
	Oid vtype;
	VecTypeHashEntry *entry = NULL;
	bool found = false;

	//construct the hash table
	if(NULL == hashMapN2V)
	{
		HASHCTL	hash_ctl;
		MemSet(&hash_ctl, 0, sizeof(hash_ctl));

		hash_ctl.keysize = sizeof(Oid);
		hash_ctl.entrysize = sizeof(VecTypeHashEntry);
		hash_ctl.hash = oid_hash;

		hashMapN2V = hash_create("vectorized_v2n", 64/*enough?*/,
								&hash_ctl, HASH_ELEM | HASH_FUNCTION);
	}

	//first, find the vectorized type in hash table
	entry = hash_search(hashMapN2V, &ntype, HASH_ENTER, &found);
	if(found)
		return entry->dest;

	Assert(!found);

	//Second, if it can not be found in hash table, find in PG_TYPE
	pcqCtx = caql_beginscan(NULL,
							cql("SELECT * FROM pg_type "
								" WHERE typelem = :1 "
								" AND typstorage = :2 ",
								ObjectIdGetDatum(ntype),
								CharGetDatum('e')));

	tuple = caql_getnext(pcqCtx);

	if(!HeapTupleIsValid(tuple))
	{
		caql_endscan(pcqCtx);
		entry->dest = InvalidOid; /* storage the InvalidOid in hash table*/
		return InvalidOid;
	}

	vtype = HeapTupleGetOid(tuple);

	//storage in the hash table
	entry->dest = vtype;

	caql_endscan(pcqCtx);

	return DatumGetObjectId(vtype);
}

/*
 * Get the functions for the vectorized types
 */
Oid GetNtype(Oid vtype){
	HeapTuple tuple;
	bool isNull = true;
	cqContext *pcqCtx;
	VecTypeHashEntry *entry = NULL;
	Oid ntype;
	bool found = false;

	//construct the hash table
	if(NULL == hashMapV2N)
	{
		HASHCTL	hash_ctl;
		MemSet(&hash_ctl, 0, sizeof(hash_ctl));

		hash_ctl.keysize = sizeof(Oid);
		hash_ctl.entrysize = sizeof(VecTypeHashEntry);
		hash_ctl.hash = oid_hash;

		hashMapV2N = hash_create("hashvfunc", 64/*enough?*/, &hash_ctl, HASH_ELEM | HASH_FUNCTION);
	}

	//first, find the vectorized type in hash table
	entry = hash_search(hashMapV2N, &vtype, HASH_ENTER, &found);
	if(found)
		return entry->dest;

	Assert(!found);

	pcqCtx = caql_beginscan(NULL,
							cql("SELECT * FROM pg_type "
								" WHERE oid = :1 ",
								ObjectIdGetDatum(vtype)));

	tuple = caql_getnext(pcqCtx);

	if(!HeapTupleIsValid(tuple))
	{
		caql_endscan(pcqCtx);
		return InvalidOid;
	}

	ntype = caql_getattr(pcqCtx,
						 Anum_pg_type_typelem,
						 &isNull);
	Assert(!isNull);

	/* storage in hash table*/
	entry->dest = ntype;

	caql_endscan(pcqCtx);

	return entry->dest;
}

