//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CQueryMutators.cpp
//
//	@doc:
//		Implementation of methods used during translating a GPDB Query object into a
//		DXL Tree
//
//	@owner:
//		raghav
//
//	@test:
//
//---------------------------------------------------------------------------

#include "postgres.h"

#include "nodes/plannodes.h"
#include "nodes/parsenodes.h"
#include "nodes/makefuncs.h"
#include "optimizer/walkers.h"

#include "gpopt/base/CUtils.h"
#include "gpopt/mdcache/CMDAccessor.h"
#include "gpopt/mdcache/CMDAccessorUtils.h"
#include "gpopt/translate/CQueryMutators.h"
#include "gpopt/translate/CTranslatorDXLToPlStmt.h"

#include "md/IMDScalarOp.h"
#include "md/IMDAggregate.h"
#include "md/IMDTypeBool.h"

#include "gpopt/gpdbwrappers.h"

using namespace gpdxl;
using namespace gpmd;

//---------------------------------------------------------------------------
//	@function:
//		CQueryMutators::FNeedsPrLNormalization
//
//	@doc:
//		Is the group by project list flat (contains only aggregates
//		and grouping columns)
//---------------------------------------------------------------------------
BOOL
CQueryMutators::FNeedsPrLNormalization
	(
	const Query *pquery
	)
{
	if (!pquery->hasAggs && NULL == pquery->groupClause)
	{
		return false;
	}

	const ULONG ulArity = gpdb::UlListLength(pquery->targetList);
	for (ULONG ul = 0; ul < ulArity; ul++)
	{
		TargetEntry *pte  = (TargetEntry*) gpdb::PvListNth(pquery->targetList, ul);

		// Normalize when there is an expression that is neither used for grouping
		// nor is an aggregate function
		if (!IsA(pte->expr, PercentileExpr) && !IsA(pte->expr, Aggref) && !IsA(pte->expr, GroupingFunc) && !CTranslatorUtils::FGroupingColumn(pte, pquery->groupClause))
		{
			return true;
		}
	}

	return false;
}

//---------------------------------------------------------------------------
//	@function:
//		CQueryMutators::PqueryNormalizeGrpByPrL
//
//	@doc:
// 		Flatten expressions in project list to contain only aggregates and
//		grouping columns
//		ORGINAL QUERY:
//			SELECT * from r where r.a > (SELECT max(c) + min(d) FROM t where r.b = t.e)
// 		NEW QUERY:
//			SELECT * from r where r.a > (SELECT x1+x2 as x3
//										 FROM (SELECT max(c) as x2, min(d) as x2
//											   FROM t where r.b = t.e) t2)
//---------------------------------------------------------------------------
Query *
CQueryMutators::PqueryNormalizeGrpByPrL
	(
	IMemoryPool *pmp,
	CMDAccessor *pmda,
	const Query *pqueryOld
	)
{
	if (!FNeedsPrLNormalization(pqueryOld))
	{
		return (Query *) gpdb::PvCopyObject(const_cast<Query*>(pqueryOld));
	}

	Query *pqueryNew = PqueryConvertToDerivedTable(pqueryOld);

	GPOS_ASSERT(1 == gpdb::UlListLength(pqueryNew->rtable));
	Query *pqueryDrdTbl = (Query *) ((RangeTblEntry *) gpdb::PvListNth(pqueryNew->rtable, 0))->subquery;
	SContextGrpbyPlMutator ctxgrpbymutator(pmp, pmda, pqueryDrdTbl, NULL);
	List *plTEcopy = (List*) gpdb::PvCopyObject(pqueryDrdTbl->targetList);
	ListCell *plc = NULL;

	// normalize each project element
	ForEach (plc, plTEcopy)
	{
		TargetEntry *pte  = (TargetEntry*) lfirst(plc);
		GPOS_ASSERT(NULL != pte);

		pte->expr = (Expr*) CQueryMutators::PnodeGrpbyPrLMutator( (Node*) pte->expr, &ctxgrpbymutator);
		GPOS_ASSERT
			(
			(!IsA(pte->expr, Aggref) && !IsA(pte->expr, PercentileExpr)) && !IsA(pte->expr, GroupingFunc) &&
			"New target list entry should not contain any Aggrefs or PercentileExpr"
			);
	}


	pqueryDrdTbl->targetList = ctxgrpbymutator.m_plTENewGroupByQuery;
	pqueryNew->targetList = plTEcopy;

	SContextIncLevelsupMutator ctxinclvlmutator(0);

	// fix outer reference in the qual as well as in the range table entries
	pqueryDrdTbl->jointree->quals = CQueryMutators::PnodeIncrementLevelsupMutator
														(
														(Node*) pqueryDrdTbl->jointree->quals,
														&ctxinclvlmutator
														);
	pqueryDrdTbl->rtable = gpdb::PlMutateRangeTable
									(
									pqueryDrdTbl->rtable,
									(Pnode) CQueryMutators::PnodeIncrementLevelsupMutator,
									&ctxgrpbymutator,
									0 // mutate sub queries
									);
	ReassignSortClause(pqueryNew, pqueryDrdTbl);

	return pqueryNew;
}

//---------------------------------------------------------------------------
//	@function:
//		CQueryMutators::PnodeIncrementLevelsupMutator
//
//	@doc:
//		Increment any the query levels up of any outer reference by one
//---------------------------------------------------------------------------
Node *
CQueryMutators::PnodeIncrementLevelsupMutator
	(
	Node *pnode,
	void *pvCtx
	)
{
	if (NULL == pnode)
	{
		return NULL;
	}

	SContextIncLevelsupMutator *pctxinclvlmutator = (SContextIncLevelsupMutator *) pvCtx;

	if (IsA(pnode, Var))
	{
		Var *pvar = (Var *) gpdb::PvCopyObject(pnode);

		// Consider the following use case:
		//	ORGINAL QUERY:
		//		SELECT * from r where r.a > (SELECT max(c) + min(d)
		//									 FROM t where r.b = t.e)
		// NEW QUERY:
		//		SELECT * from r where r.a > (SELECT x1+x2 as x3
		//									 FROM (SELECT max(c) as x2, min(d) as x2
		//										   FROM t where r.b = t.e) t2)
		//
		// In such a scenario, we need increment the levels up for the
		// correlation variable r.b in the subquery by 1.

		if (pvar->varlevelsup > pctxinclvlmutator->m_ulCurrLevelsUp)
		{
			pvar->varlevelsup++;
			return (Node *) pvar;
		}
		return (Node *) pvar;
	}

	if (IsA(pnode, SubLink))
	{
		SubLink *psublink = (SubLink *) gpdb::PvCopyObject(pnode);
		pctxinclvlmutator->m_ulCurrLevelsUp++;

		GPOS_ASSERT(IsA(psublink->subselect, Query));

		psublink->subselect = gpdb::PnodeMutateQueryOrExpressionTree
										(
										psublink->subselect,
										(Pnode) CQueryMutators::PnodeIncrementLevelsupMutator,
										pvCtx,
										0 // mutate into cte-lists
										);

		pctxinclvlmutator->m_ulCurrLevelsUp--;

		return (Node *) psublink;
	}

	return gpdb::PnodeMutateExpressionTree(pnode, (Pnode) CQueryMutators::PnodeIncrementLevelsupMutator, pvCtx);
}

//---------------------------------------------------------------------------
//	@function:
//		CQueryMutators::PnodeFixCTELevelsupMutator
//
//	@doc:
//		Increment any the query levels up of any CTE range table reference by one
//---------------------------------------------------------------------------
Node *
CQueryMutators::PnodeFixCTELevelsupMutator
	(
	Node *pnode,
	void *pvCtx
	)
{
	if (NULL == pnode)
	{
		return NULL;
	}

	SContextIncLevelsupMutator *pctxinclvlmutator = (SContextIncLevelsupMutator *) pvCtx;

	// recurse into query structure
	if (IsA(pnode, Query))
	{
		Query *pquery = gpdb::PqueryMutateQueryTree
								(
								(Query *) pnode,
								(Pnode) CQueryMutators::PnodeFixCTELevelsupMutator,
								pvCtx,
								0 // mutate into cte-lists
								);

		// fix the levels up for CTE range table entry when needed
		List *plRtable = pquery->rtable;
		ListCell *plc = NULL;
		ForEach (plc, plRtable)
		{
			RangeTblEntry *prte = (RangeTblEntry *) lfirst(plc);
			if (RTE_CTE == prte->rtekind  && FNeedsLevelsUpCorrection(pctxinclvlmutator, prte->ctelevelsup))
			{
				prte->ctelevelsup++;
			}
			else if (RTE_SUBQUERY == prte->rtekind)
			{
				pctxinclvlmutator->m_ulCurrLevelsUp++;
				prte->subquery = (Query *) CQueryMutators::PnodeFixCTELevelsupMutator( (Node *) prte->subquery, pctxinclvlmutator);
				pctxinclvlmutator->m_ulCurrLevelsUp--;
			}
		}

		return (Node *) pquery;
	}

	// recurse into a query attached to sublink
	if (IsA(pnode, SubLink))
	{
		SubLink *psublink = (SubLink *) gpdb::PvCopyObject(pnode);
		pctxinclvlmutator->m_ulCurrLevelsUp++;
		GPOS_ASSERT(IsA(psublink->subselect, Query));
		psublink->subselect = CQueryMutators::PnodeFixCTELevelsupMutator(psublink->subselect, pctxinclvlmutator);
		pctxinclvlmutator->m_ulCurrLevelsUp--;

		return (Node *) psublink;
	}

	return gpdb::PnodeMutateExpressionTree(pnode, (Pnode) CQueryMutators::PnodeFixCTELevelsupMutator, pvCtx);
}

//---------------------------------------------------------------------------
//	@function:
//		CQueryMutators::FCorrectCTELevelsup
//
//	@doc:
//		Check if the cte levels up is the expected query level
//---------------------------------------------------------------------------
BOOL
CQueryMutators::FNeedsLevelsUpCorrection
	(
	SContextIncLevelsupMutator *pctxinclvlmutator,
	Index idxCtelevelsup
	)
{
	if (pctxinclvlmutator->m_fOnlyCurrentLevel)
	{
		return (idxCtelevelsup == pctxinclvlmutator->m_ulCurrLevelsUp);
	}
	return (idxCtelevelsup > pctxinclvlmutator->m_ulCurrLevelsUp);
}

//---------------------------------------------------------------------------
//	@function:
//		CQueryMutators::PnodeGrpbyPrLMutator
//
//	@doc:
// 		Traverse the project list of a groupby operator and extract all aggregate
//		functions in an arbitrarily complex project element
//---------------------------------------------------------------------------
Node *
CQueryMutators::PnodeGrpbyPrLMutator
	(
	Node *pnode,
	void *pctx
	)
{
	if (NULL == pnode)
	{
		return NULL;
	}

	SContextGrpbyPlMutator *pctxgrpbymutator = (SContextGrpbyPlMutator *) pctx;

	// if we find an aggregate or precentile expression then insert into the new derived table
	// and refer to it in the top-level query
	if (IsA(pnode, Aggref) || IsA(pnode, PercentileExpr) || IsA(pnode, GroupingFunc))
	{
		const ULONG ulAttno = gpdb::UlListLength(pctxgrpbymutator->m_plTENewGroupByQuery) + 1;
		TargetEntry *pte = PteAggregateOrPercentileExpr(pctxgrpbymutator->m_pmp, pctxgrpbymutator->m_pmda, pnode, ulAttno);

		// Add a new target entry to the query
		pctxgrpbymutator->m_plTENewGroupByQuery = gpdb::PlAppendElement(pctxgrpbymutator->m_plTENewGroupByQuery, pte);

		Var *pvarNew = gpdb::PvarMakeVar
								(
								1,
								(AttrNumber) ulAttno,
								gpdb::OidExprType(pnode),
								gpdb::IExprTypeMod(pnode),
								0 // query levelsup
								);

		return (Node*) pvarNew;
	}

	TargetEntry *pteFound = gpdb::PteMember(pnode, pctxgrpbymutator->m_plTENewGroupByQuery);

	// if we find a target entry in the new derived table then return the appropriate var
	// else investigate it to see if it needs to be added to the new derived table

	if (NULL != pteFound)
	{
		Var *pvarNew = gpdb::PvarMakeVar
								(
								1,
								pteFound->resno,
								gpdb::OidExprType((Node*) pteFound->expr),
								gpdb::IExprTypeMod( (Node*) pteFound->expr),
								0 // query levelsup
								);

		return (Node*) pvarNew;
	}

	TargetEntry *pteFoundOriginal = gpdb::PteMember(pnode, pctxgrpbymutator->m_pquery->targetList);

	// if it is grouping column then we need add it to the new derived table
	// and refer to it in the top-level query
	if (NULL != pteFoundOriginal  && CTranslatorUtils::FGroupingColumn(pteFoundOriginal, pctxgrpbymutator->m_pquery->groupClause))
	{
		ULONG ulArity = gpdb::UlListLength(pctxgrpbymutator->m_plTENewGroupByQuery) + 1;

		TargetEntry *pteNew = gpdb::PteMakeTargetEntry
								(
								(Expr*) pnode,
								(AttrNumber) ulArity,
								CQueryMutators::SzTEName(pteFoundOriginal,pctxgrpbymutator->m_pquery),
								false
								);

		pteNew->ressortgroupref = pteFoundOriginal->ressortgroupref;
		pteNew->resjunk = false;

		pctxgrpbymutator->m_plTENewGroupByQuery = gpdb::PlAppendElement(pctxgrpbymutator->m_plTENewGroupByQuery, pteNew);

		Var *pvarNew = gpdb::PvarMakeVar
								(
								1,
								(AttrNumber) ulArity,
								gpdb::OidExprType( (Node*) pteFoundOriginal->expr),
								gpdb::IExprTypeMod( (Node*) pteFoundOriginal->expr),
								0 // query levelsup
								);

		return (Node*) pvarNew;
	}

	// do not traverse into sub queries as they will be inserted are inserted into
	// top-level query as is
	if (IsA(pnode, SubLink))
	{
		return (Node *) gpdb::PvCopyObject(pnode);
	}

	return gpdb::PnodeMutateExpressionTree(pnode, (Pnode) CQueryMutators::PnodeGrpbyPrLMutator, pctx);
}

//---------------------------------------------------------------------------
//	@function:
//		CQueryMutators::PteAggregateOrPercentileExpr
//
//	@doc:
// 		Return a target entry for an aggregate or percentile expression
//---------------------------------------------------------------------------
TargetEntry *
CQueryMutators::PteAggregateOrPercentileExpr
	(
	IMemoryPool *pmp,
	CMDAccessor *pmda,
	Node *pnode,
	ULONG ulAttno
	)
{
	GPOS_ASSERT(IsA(pnode, PercentileExpr) || IsA(pnode, Aggref) || IsA(pnode, GroupingFunc));

	// get the function/aggregate name
	CHAR *szName = NULL;
	if (IsA(pnode, PercentileExpr))
	{
		PercentileExpr *ppercentile = (PercentileExpr*) pnode;

		if (PERC_MEDIAN == ppercentile->perckind)
		{
			szName = CTranslatorUtils::SzFromWsz(GPOS_WSZ_LIT("Median"));
		}
		else if (PERC_CONT == ppercentile->perckind)
		{
			szName = CTranslatorUtils::SzFromWsz(GPOS_WSZ_LIT("Cont"));
		}
		else
		{
			GPOS_ASSERT(PERC_DISC == ppercentile->perckind);
			szName = CTranslatorUtils::SzFromWsz(GPOS_WSZ_LIT("Disc"));
		}
	}
	else if (IsA(pnode, GroupingFunc))
	{
		szName = CTranslatorUtils::SzFromWsz(GPOS_WSZ_LIT("grouping"));
	}
	else
	{
		Aggref *paggref = (Aggref*) pnode;

		CMDIdGPDB *pmdidAgg = New(pmp) CMDIdGPDB(paggref->aggfnoid);
		const IMDAggregate *pmdagg = pmda->Pmdagg(pmdidAgg);
		pmdidAgg->Release();

		const CWStringConst *pstr = pmdagg->Mdname().Pstr();
		szName = CTranslatorUtils::SzFromWsz(pstr->Wsz());
	}
	GPOS_ASSERT(NULL != szName);

	return gpdb::PteMakeTargetEntry((Expr*) pnode, (AttrNumber) ulAttno, szName, false);
}

//---------------------------------------------------------------------------
//	@function:
//		CQueryMutators::PnodeHavingQualMutator
//
//	@doc:
// 		This mutator function checks to see if the current node is an AggRef
//		or a correlated variable from the derived table.
//		If it is an Aggref:
//			Then replaces it with the appropriate attribute from the top-level query.
//		If it is a correlated Var:
//			Then replaces it with a attribute from the top-level query.
//---------------------------------------------------------------------------
Node *
CQueryMutators::PnodeHavingQualMutator
	(
	Node *pnode,
	void *pctx
	)
{
	if (NULL == pnode)
	{
		return NULL;
	}

	SContextHavingQualMutator *context = (SContextHavingQualMutator *) pctx;

	// check to see if the node is in the target list of the derived table.
	// check if we have the corresponding pte entry in derived tables target list
	if (0 == context->m_ulCurrLevelsUp)
	{
		TargetEntry *pteFound = gpdb::PteMember(pnode, context->m_plTENewGroupByQuery);
		if (NULL != pteFound)
		{
			Var *pvarNew = gpdb::PvarMakeVar
							(
							1,
							pteFound->resno,
							gpdb::OidExprType( (Node*) pteFound->expr),
							gpdb::IExprTypeMod( (Node*) pteFound->expr),
							context->m_ulCurrLevelsUp
							);

			return (Node*) pvarNew;
		}

		if (IsA(pnode, PercentileExpr) || IsA(pnode, GroupingFunc))
		{
			Node *pnodeCopy = (Node*) gpdb::PvCopyObject(pnode);

			const ULONG ulAttno = gpdb::UlListLength(context->m_plTENewGroupByQuery) + 1;
			TargetEntry *pte = PteAggregateOrPercentileExpr(context->m_pmp, context->m_pmda, pnodeCopy, ulAttno);
			context->m_plTENewGroupByQuery = gpdb::PlAppendElement(context->m_plTENewGroupByQuery, pte);

			TargetEntry *pteNew = MakeNode(TargetEntry);
			Var *pvarNew = gpdb::PvarMakeVar
							(
							1,
							(AttrNumber) context->m_ulTECount,
							gpdb::OidExprType(pnodeCopy),
							gpdb::IExprTypeMod(pnodeCopy),
							context->m_ulCurrLevelsUp
							);

			pteNew->expr = (Expr*) pvarNew;
			pteNew->resno = (AttrNumber) context->m_ulTECount;
			pteNew->resname = pte->resname;
			pteNew->resjunk = true;

			context->m_plTENewRootQuery = gpdb::PlAppendElement(context->m_plTENewRootQuery, pteNew);
			context->m_ulTECount++;

			return (Node*) gpdb::PvCopyObject(pvarNew);
		}
	}

	if (IsA(pnode, Aggref))
	{
		Aggref *paggrefOld = (Aggref *) pnode;
		if (paggrefOld->agglevelsup == context->m_ulCurrLevelsUp)
		{
			Aggref *paggrefNew = MakeNode(Aggref);

			paggrefNew->aggfnoid = paggrefOld->aggfnoid;
			paggrefNew->aggdistinct = paggrefOld->aggdistinct;
			paggrefNew->agglevelsup = 0;
			paggrefNew->location = paggrefOld->location;
			paggrefNew->aggtype = paggrefOld->aggtype;
			paggrefNew->aggstage = paggrefOld->aggstage;
			paggrefNew->aggstar = paggrefOld->aggstar;

			List *plargsNew = NIL;
			ListCell *plc = NULL;

			ForEach (plc, paggrefOld->args)
			{
				Node *pnodeArg = (Node*) lfirst(plc);
				GPOS_ASSERT(NULL != pnodeArg);
				// traverse each argument and fix levels up when needed
				plargsNew = gpdb::PlAppendElement
									(
									plargsNew,
									gpdb::PnodeMutateQueryOrExpressionTree
											(
											pnodeArg,
											(Pnode) CQueryMutators::PnodeHavingQualMutator,
											(void *) pctx,
											0 // mutate into cte-lists
											)
									);
			}
			paggrefNew->args = plargsNew;

			// check if an entry already exists, if so no need for duplicate
			TargetEntry *pteFound = gpdb::PteMember((Node*) paggrefNew, context->m_plTENewGroupByQuery);
			if (NULL != pteFound)
			{
				gpdb::GPDBFree(paggrefNew);
				Var *pvarNew = gpdb::PvarMakeVar
									(
									1,
									pteFound->resno,
									gpdb::OidExprType( (Node*) pteFound->expr),
									gpdb::IExprTypeMod( (Node*) pteFound->expr),
									context->m_ulCurrLevelsUp
									);
				return (Node*) pvarNew;
			}

			const ULONG ulAttno = gpdb::UlListLength(context->m_plTENewGroupByQuery) + 1;
			TargetEntry *pte = PteAggregateOrPercentileExpr(context->m_pmp, context->m_pmda, (Node *) paggrefNew, ulAttno);
			context->m_plTENewGroupByQuery = gpdb::PlAppendElement(context->m_plTENewGroupByQuery, pte);

			TargetEntry *pteNew = MakeNode(TargetEntry);
			Var *pvarNew = gpdb::PvarMakeVar
							(
							1,
							(AttrNumber) context->m_ulTECount,
							gpdb::OidExprType((Node*) paggrefNew),
							gpdb::IExprTypeMod((Node*) paggrefNew),
							context->m_ulCurrLevelsUp
							);

			pteNew->expr = (Expr*) pvarNew;
			pteNew->resno = (AttrNumber) context->m_ulTECount;
			pteNew->resname = pte->resname;
			pteNew->resjunk = true;

			context->m_plTENewRootQuery = gpdb::PlAppendElement(context->m_plTENewRootQuery, pteNew);
			context->m_ulTECount++;

			return (Node*) gpdb::PvCopyObject(pvarNew);
		}
	}

	if (IsA(pnode, Var))
	{
		Var *pvar = (Var *) gpdb::PvCopyObject(pnode);
		if (pvar->varlevelsup == context->m_ulCurrLevelsUp)
		{
			pvar->varlevelsup = 0;
			TargetEntry *pteFound = gpdb::PteMember( (Node*) pvar, context->m_plTENewGroupByQuery);

			if (NULL != pteFound)
			{
				pvar->varlevelsup = context->m_ulCurrLevelsUp;
				pvar->varno = 1;
				pvar->varattno = pteFound->resno;
				return (Node*) pvar;
			}
		}
		return (Node *) pvar;
	}

	if (IsA(pnode, SubLink))
	{
		SubLink *psublinkOld = (SubLink *) pnode;

		SubLink *psublinkNew = MakeNode(SubLink);
		psublinkNew->subLinkType = psublinkOld->subLinkType;
		psublinkNew->location = psublinkOld->location;
		psublinkNew->operName = (List *) gpdb::PvCopyObject(psublinkOld->operName);

		psublinkNew->testexpr =	gpdb::PnodeMutateQueryOrExpressionTree
										(
										psublinkOld->testexpr,
										(Pnode) CQueryMutators::PnodeHavingQualMutator,
										(void *) pctx,
										0 // mutate into cte-lists
										);
		context->m_ulCurrLevelsUp++;

		GPOS_ASSERT(IsA(psublinkOld->subselect, Query));

		psublinkNew->subselect = gpdb::PnodeMutateQueryOrExpressionTree
										(
										psublinkOld->subselect,
										(Pnode) CQueryMutators::PnodeHavingQualMutator,
										(void *) pctx,
										0 // mutate into cte-lists
										);

		context->m_ulCurrLevelsUp--;

		return (Node *) psublinkNew;
	}

	return gpdb::PnodeMutateExpressionTree(pnode, (Pnode) CQueryMutators::PnodeHavingQualMutator, pctx);
}

//---------------------------------------------------------------------------
//	@function:
//		CQueryMutators::PqueryNormalizeHaving
//
//	@doc:
//		Pull up having qual into a select and fix correlated references
//		to the top-level query
//---------------------------------------------------------------------------
Query *
CQueryMutators::PqueryNormalizeHaving
	(
	IMemoryPool *pmp,
	CMDAccessor *pmda,
	const Query *pquery
	)
{
	if (NULL == pquery->havingQual)
	{
		return (Query *) gpdb::PvCopyObject(const_cast<Query*>(pquery));
	}

	Query *pqueryNew = PqueryConvertToDerivedTable(pquery);
	RangeTblEntry *prte = ((RangeTblEntry *) gpdb::PvListNth(pqueryNew->rtable, 0));
	Query *pqueryDrdTbl = (Query *) prte->subquery;

	// Add all necessary target list entries of subquery
	// into the target list of the RTE as well as the new top most query
	ListCell *plc = NULL;
	ULONG ulTECount = 1;
	ForEach (plc, pqueryDrdTbl->targetList)
	{
		TargetEntry *pte  = (TargetEntry*) lfirst(plc);
		GPOS_ASSERT(NULL != pte);

		// Add to the target lists:
		// 	(1) All grouping / sorting columns even if they do not appear in the subquery output (resjunked)
		//	(2) All non-resjunked target list entries
		if (CTranslatorUtils::FGroupingColumn(pte, pqueryDrdTbl->groupClause) ||
			CTranslatorUtils::FSortingColumn(pte, pqueryDrdTbl->sortClause) || !pte->resjunk)
		{
			TargetEntry *pteNew = Pte(pte, ulTECount);
			pqueryNew->targetList = gpdb::PlAppendElement(pqueryNew->targetList, pteNew);
			// Ensure that such target entries is not suppressed in the target list of the RTE
			// and has a name
			pte->resname = SzTEName(pte, pqueryDrdTbl);
			pte->resjunk = false;
			pteNew->ressortgroupref = pte->ressortgroupref;

			ulTECount++;
		}
	}

	SContextHavingQualMutator ctxhavingqualmutator(pmp, pmda, ulTECount, pqueryDrdTbl->targetList, pqueryNew->targetList);

	// fix outer references in the qual
	pqueryNew->jointree->quals = CQueryMutators::PnodeHavingQualMutator(pqueryDrdTbl->havingQual, &ctxhavingqualmutator);
	pqueryDrdTbl->havingQual = NULL;

	if (NULL != pqueryDrdTbl->cteList)
	{
		// the having quals has been translated into a select (qual) attached to the new query pqueryNew.
		// Therefore, the cte list must now be associated with the new query
		pqueryNew->cteList = pqueryDrdTbl->cteList;
		pqueryDrdTbl->cteList = NIL;

		// fix the CTE levels up of the derived table sub query since the CTE list is now associated with the top level query
		SContextIncLevelsupMutator ctxinclvlmutator
			(
			0 /* query level */,
			true /* m_fOnlyCurrentLevel bump references to ctes defined at the current level only*/
			);
		prte->subquery  = (Query *) CQueryMutators::PnodeFixCTELevelsupMutator( (Node *) pqueryDrdTbl, &ctxinclvlmutator);

		// clean up the old query object
		gpdb::GPDBFree(pqueryDrdTbl);
	}

	ReassignSortClause(pqueryNew, prte->subquery);

	if (!prte->subquery->hasAggs && NIL == prte->subquery->groupClause)
	{
		// if the derived table has no grouping columns or aggregates then the
		// subquery is equivalent to select XXXX FROM CONST-TABLE 
		// (where XXXX is the original subquery's target list)

		Query *pqueryNewSubquery = MakeNode(Query);

		pqueryNewSubquery->commandType = CMD_SELECT;
		pqueryNewSubquery->targetList = NIL;

		pqueryNewSubquery->hasAggs = false;
		pqueryNewSubquery->hasWindFuncs = false;
		pqueryNewSubquery->hasSubLinks = false;

		ListCell *plc = NULL;
		ForEach (plc, prte->subquery->targetList)
		{
			TargetEntry *pte  = (TargetEntry*) lfirst(plc);
			GPOS_ASSERT(NULL != pte);

			GPOS_ASSERT(!pte->resjunk);
			
			pqueryNewSubquery->targetList =  gpdb::PlAppendElement
													(
													pqueryNewSubquery->targetList,
													(TargetEntry *) gpdb::PvCopyObject(const_cast<TargetEntry*>(pte))
													);
		}

		gpdb::GPDBFree(prte->subquery);

		prte->subquery = pqueryNewSubquery;
		prte->subquery->jointree = MakeNode(FromExpr);
		prte->subquery->groupClause = NIL;
		prte->subquery->sortClause = NIL;
		prte->subquery->windowClause = NIL;
	}

	return pqueryNew;
}

//---------------------------------------------------------------------------
//	@function:
//		CQueryMutators::PqueryNormalize
//
//	@doc:
//		Normalize queries with having and group by clauses
//---------------------------------------------------------------------------
Query *
CQueryMutators::PqueryNormalize
	(
	IMemoryPool *pmp,
	CMDAccessor *pmda,
	const Query *pquery
	)
{
	// eliminate distinct clause
	Query *pqueryEliminateDistinct = CQueryMutators::PqueryEliminateDistinctClause(pquery);
	GPOS_ASSERT(NULL == pqueryEliminateDistinct->distinctClause);

	// fix window frame edge boundary
	Query *pqueryFixedWindowFrameEdge = CQueryMutators::PqueryFixWindowFrameEdgeBoundary(pqueryEliminateDistinct);
	gpdb::GPDBFree(pqueryEliminateDistinct);

	// normalize window operator's project list
	Query *pqueryWindowPlNormalized = CQueryMutators::PqueryNormalizeWindowPrL(pmp, pmda, pqueryFixedWindowFrameEdge);
	gpdb::GPDBFree(pqueryFixedWindowFrameEdge);

	// pull-up having quals into a select
	Query *pqueryHavingNormalized = CQueryMutators::PqueryNormalizeHaving(pmp, pmda, pqueryWindowPlNormalized);
	GPOS_ASSERT(NULL == pqueryHavingNormalized->havingQual);
	gpdb::GPDBFree(pqueryWindowPlNormalized);

	// normalize the group by project list
	Query *pqueryNew = CQueryMutators::PqueryNormalizeGrpByPrL(pmp, pmda, pqueryHavingNormalized);
	gpdb::GPDBFree(pqueryHavingNormalized);

	return pqueryNew;
}

//---------------------------------------------------------------------------
//	@function:
//		CQueryMutators::Pte
//
//	@doc:
//		Given an Target list entry in the derived table, create a new
//		TargetEntry to be added to the top level query. This function allocates
//		memory
//---------------------------------------------------------------------------
TargetEntry *
CQueryMutators::Pte
	(
	TargetEntry *pteOld,
	ULONG ulVarAttno
	)
{
	Var *pvarNew = gpdb::PvarMakeVar
							(
							1,
							(AttrNumber) ulVarAttno,
							gpdb::OidExprType( (Node*) pteOld->expr),
							gpdb::IExprTypeMod( (Node*) pteOld->expr),
							0 // query levelsup
							);

	TargetEntry *pteNew = gpdb::PteMakeTargetEntry((Expr*) pvarNew, (AttrNumber) ulVarAttno, pteOld->resname, pteOld->resjunk);

	return pteNew;
}

//---------------------------------------------------------------------------
//	@function:
//		CQueryMutators::SzTEName
//
//	@doc:
//		Return the column name of the target list entry
//---------------------------------------------------------------------------
CHAR *
CQueryMutators::SzTEName
	(
	TargetEntry *pte,
	Query *pquery
	)
{
	if (NULL != pte->resname)
	{
		return pte->resname;
	}

	// Columns that do not appear in the final column do not have have column name. In this case, get the column name
	if (IsA(pte->expr, Var))
	{
		Var *pvar = (Var*) pte->expr;

		RangeTblEntry *prte = (RangeTblEntry *) gpdb::PvListNth(pquery->rtable, pvar->varno -1);
		Alias *palias = prte->eref;

		GPOS_ASSERT(NULL != palias->colnames && pvar->varattno <= gpdb::UlListLength(palias->colnames) && NULL != gpdb::PvListNth(palias->colnames, pvar->varattno-1));
		return strVal(gpdb::PvListNth(palias->colnames, pvar->varattno-1));
	}

	// Since a resjunked target list entry will not have a column name create a dummy column name
	CWStringConst strUnnamedCol(GPOS_WSZ_LIT("?column?"));

	return CTranslatorUtils::SzFromWsz(strUnnamedCol.Wsz());
}

//---------------------------------------------------------------------------
//	@function:
//		CQueryMutators::PqueryConvertToDerivedTable
//
//	@doc:
//		Converts query into a derived table and return the new top-level query
//---------------------------------------------------------------------------
Query *
CQueryMutators::PqueryConvertToDerivedTable
	(
	const Query *pquery
	)
{
	Query *pqueryCopy = (Query *) gpdb::PvCopyObject(const_cast<Query*>(pquery));

	// create a range table entry for the query node
	RangeTblEntry *prte = MakeNode(RangeTblEntry);
	prte->rtekind = RTE_SUBQUERY;

	// do not walk down the cte list as their cte-levels up will no change
	List *plCTE = pqueryCopy->cteList;
	pqueryCopy->cteList = NIL;

	Node *pnodeHaving = pqueryCopy->havingQual;
	pqueryCopy->havingQual = NULL;

	// fix the CTE levels up
	SContextIncLevelsupMutator ctxinclvlmutator
		(
		0 /*starting level */,
		false /* bump all cte levels greater than the current level*/
		);
	prte->subquery  = (Query *) CQueryMutators::PnodeFixCTELevelsupMutator( (Node *) pqueryCopy, &ctxinclvlmutator);
	prte->inFromCl = true;
	prte->subquery->cteList = plCTE;
	prte->subquery->havingQual = pnodeHaving;

	// create a new range table reference for the new RTE
	RangeTblRef *prtref = MakeNode(RangeTblRef);
	prtref->rtindex = 1;

	// create a new top-level query with the new RTE in its from clause
	Query *pqueryNew = MakeNode(Query);
	pqueryNew->cteList = NIL;
	pqueryNew->hasAggs = false;
	pqueryNew->rtable = gpdb::PlAppendElement(pqueryNew->rtable, prte);

	FromExpr *pfromexpr = MakeNode(FromExpr);
	pfromexpr->quals = NULL;
	pfromexpr->fromlist = gpdb::PlAppendElement(pfromexpr->fromlist, prtref);

	pqueryNew->jointree = pfromexpr;
	pqueryNew->commandType = CMD_SELECT;

	GPOS_ASSERT(1 == gpdb::UlListLength(pqueryNew->rtable));
	return pqueryNew;
}

//---------------------------------------------------------------------------
//	@function:
//		CQueryMutators::PqueryEliminateDistinctClause
//
//	@doc:
//		Eliminate distinct columns by translating it into a grouping columns
//---------------------------------------------------------------------------
Query *
CQueryMutators::PqueryEliminateDistinctClause
	(
	const Query *pquery
	)
{
	if (0 == gpdb::UlListLength(pquery->distinctClause))
	{
		return (Query*) gpdb::PvCopyObject(const_cast<Query*>(pquery));
	}

	// create a derived table out of the previous query
	Query *pqueryNew = PqueryConvertToDerivedTable(pquery);

	GPOS_ASSERT(1 == gpdb::UlListLength(pqueryNew->rtable));
	Query *pqueryDrdTbl = (Query *) ((RangeTblEntry *) gpdb::PvListNth(pqueryNew->rtable, 0))->subquery;

	ReassignSortClause(pqueryNew, pqueryDrdTbl);

	pqueryNew->targetList = NIL;
	List *plTE = pqueryDrdTbl->targetList;
	ListCell *plc = NULL;

	// build the project list of the new top-level query
	ForEach (plc, plTE)
	{
		ULONG ulResNo = gpdb::UlListLength(pqueryNew->targetList) + 1;
		TargetEntry *pte  = (TargetEntry*) lfirst(plc);
		GPOS_ASSERT(NULL != pte);

		if (!pte->resjunk)
		{
			// create a new target entry that points to the corresponding entry in the derived table
			Var *pvarNew = gpdb::PvarMakeVar
									(
									1,
									pte->resno,
									gpdb::OidExprType((Node*) pte->expr),
									gpdb::IExprTypeMod((Node*) pte->expr),
									0 // query levels up
									);
			TargetEntry *pteNew= gpdb::PteMakeTargetEntry((Expr*) pvarNew, (AttrNumber) ulResNo, pte->resname, false);

			pteNew->ressortgroupref =  pte->ressortgroupref;
			pqueryNew->targetList = gpdb::PlAppendElement(pqueryNew->targetList, pteNew);
		}

		if (0 < pte->ressortgroupref && !CTranslatorUtils::FGroupingColumn(pte, pqueryDrdTbl->groupClause))
		{
			// initialize the ressortgroupref of target entries not used in the grouping clause
			 pte->ressortgroupref = 0;
		}
	}

	if (gpdb::UlListLength(pqueryNew->targetList) != gpdb::UlListLength(pquery->distinctClause))
	{
		GPOS_RAISE(gpdxl::ExmaDXL, gpdxl::ExmiQuery2DXLUnsupportedFeature, GPOS_WSZ_LIT("DISTINCT operation on a subset of target list columns"));
	}

	ListCell *plcDistinctCl = NULL;
	ForEach (plcDistinctCl, pquery->distinctClause)
	{
		SortClause *psortcl  = (SortClause*) lfirst(plcDistinctCl);
		GPOS_ASSERT(NULL != psortcl);

		GroupClause *pgrpcl = MakeNode(GroupClause);
		pgrpcl->tleSortGroupRef = psortcl->tleSortGroupRef;
		pgrpcl->sortop = psortcl->sortop;
		pqueryNew->groupClause = gpdb::PlAppendElement(pqueryNew->groupClause, pgrpcl);
	}
	pqueryNew->distinctClause = NIL;
	pqueryDrdTbl->distinctClause = NIL;

	return pqueryNew;
}

//---------------------------------------------------------------------------
//	@function:
//		CQueryMutators::FNeedsWindowPrLNormalization
//
//	@doc:
//		Check whether the window operator's project list only contains
//		window functions and columns used in the window specification
//---------------------------------------------------------------------------
BOOL
CQueryMutators::FNeedsWindowPrLNormalization
	(
	const Query *pquery
	)
{
	if (!pquery->hasWindFuncs)
	{
		return false;
	}

	const ULONG ulArity = gpdb::UlListLength(pquery->targetList);
	for (ULONG ul = 0; ul < ulArity; ul++)
	{
		TargetEntry *pte  = (TargetEntry*) gpdb::PvListNth(pquery->targetList, ul);

		if (!CTranslatorUtils::FWindowSpec(pte, pquery->windowClause) && !IsA(pte->expr, WindowRef) && !IsA(pte->expr, Var))
		{
			// computed columns in the target list that is not
			// used in the order by or partition by of the window specification(s)
			return true;
		}
	}

	return false;
}

//---------------------------------------------------------------------------
//	@function:
//		CQueryMutators::PqueryFixWindowFrameEdgeBoundary
//
//	@doc:
//		Fix window frame edge boundary when its value is defined by a subquery
//---------------------------------------------------------------------------
Query *
CQueryMutators::PqueryFixWindowFrameEdgeBoundary
	(
	const Query *pquery
	)
{
	Query *pqueryNew = (Query *) gpdb::PvCopyObject(const_cast<Query*>(pquery));

	List *plWindowClause = pqueryNew->windowClause;
	ListCell *plcWindowCl = NULL;
	ForEach (plcWindowCl, plWindowClause)
	{
		WindowSpec *pwindowspec = (WindowSpec*) lfirst(plcWindowCl);
		if (NULL != pwindowspec->frame)
		{
			WindowFrame *pwindowframe = pwindowspec->frame;
			if (NULL != pwindowframe->lead->val && IsA(pwindowframe->lead->val, SubLink))
			{
				if (WINDOW_BOUND_PRECEDING == pwindowframe->lead->kind)
				{
					pwindowframe->lead->kind = WINDOW_DELAYED_BOUND_PRECEDING;
				}
				else
				{
					GPOS_ASSERT(WINDOW_BOUND_FOLLOWING == pwindowframe->lead->kind);
					pwindowframe->lead->kind = WINDOW_DELAYED_BOUND_FOLLOWING;
				}
			}

			if (NULL != pwindowframe->trail->val && IsA(pwindowframe->trail->val, SubLink))
			{
				if (WINDOW_BOUND_PRECEDING == pwindowframe->trail->kind)
				{
					pwindowframe->trail->kind = WINDOW_DELAYED_BOUND_PRECEDING;
				}
				else
				{
					GPOS_ASSERT(WINDOW_BOUND_FOLLOWING == pwindowframe->trail->kind);
					pwindowframe->trail->kind = WINDOW_DELAYED_BOUND_FOLLOWING;
				}
			}
		}
	}

	return pqueryNew;
}

//---------------------------------------------------------------------------
//	@function:
//		CQueryMutators::PqueryNormalizeWindowPrL
//
//	@doc:
// 		Flatten expressions in project list to contain only window functions and
//		columns used in the window specification
//
//		ORGINAL QUERY:
//			SELECT row_number() over() + rank() over(partition by a+b order by a-b) from foo
//
// 		NEW QUERY:
//			SELECT rn+rk from (SELECT row_number() over() as rn rank() over(partition by a+b order by a-b) as rk FROM foo) foo_new
//---------------------------------------------------------------------------
Query *
CQueryMutators::PqueryNormalizeWindowPrL
	(
	IMemoryPool *pmp,
	CMDAccessor *pmda,
	const Query *pquery
	)
{
	if (!FNeedsWindowPrLNormalization(pquery))
	{
		return (Query *) gpdb::PvCopyObject(const_cast<Query*>(pquery));
	}

	Query *pqueryNew = PqueryConvertToDerivedTable(pquery);
	GPOS_ASSERT(1 == gpdb::UlListLength(pqueryNew->rtable));
	Query *pqueryDrdTbl = (Query *) ((RangeTblEntry *) gpdb::PvListNth(pqueryNew->rtable, 0))->subquery;

	SContextGrpbyPlMutator ctxgrpbymutator(pmp, pmda, pqueryDrdTbl, NULL);
	ListCell *plc = NULL;
	List *plTE = pqueryDrdTbl->targetList;
	ForEach (plc, plTE)
	{
		TargetEntry *pte  = (TargetEntry*) lfirst(plc);
		const ULONG ulResNoNew = gpdb::UlListLength(pqueryNew->targetList) + 1;

		if (CTranslatorUtils::FWindowSpec(pte, pquery->windowClause))
		{
			// insert the target list entry used in the window specification as is
			TargetEntry *pteNew = (TargetEntry *) gpdb::PvCopyObject(pte);
			pteNew->resno = gpdb::UlListLength(ctxgrpbymutator.m_plTENewGroupByQuery) + 1;
			ctxgrpbymutator.m_plTENewGroupByQuery = gpdb::PlAppendElement(ctxgrpbymutator.m_plTENewGroupByQuery, pteNew);

			if (!pte->resjunk || CTranslatorUtils::FSortingColumn(pte, pquery->sortClause))
			{
				// if the target list entry used in the window specification is present
				// in the query output then add it to the target list of the new top level query
				Var *pvarNew = gpdb::PvarMakeVar
										(
										1,
										pteNew->resno,
										gpdb::OidExprType((Node*) pte->expr),
										gpdb::IExprTypeMod((Node*) pte->expr),
										0 // query levels up
										);
				TargetEntry *pteNewCopy = gpdb::PteMakeTargetEntry((Expr*) pvarNew, ulResNoNew, pte->resname, pte->resjunk);

				// Copy the resortgroupref and resjunk information for the top-level target list entry
				// Set target list entry of the derived table to be non-resjunked
				pteNewCopy->resjunk = pteNew->resjunk;
				pteNewCopy->ressortgroupref = pteNew->ressortgroupref;
				pteNew->resjunk = false;

				pqueryNew->targetList = gpdb::PlAppendElement(pqueryNew->targetList, pteNewCopy);
			}
		}
		else
		{
			// normalize target list entry
			ctxgrpbymutator.m_ulRessortgroupref = pte->ressortgroupref;
			Expr *pexprNew = (Expr*) CQueryMutators::PnodeWindowPrLMutator( (Node*) pte->expr, &ctxgrpbymutator);
			TargetEntry *pteNew = gpdb::PteMakeTargetEntry(pexprNew, ulResNoNew, pte->resname, pte->resjunk);
			pteNew->ressortgroupref = pte->ressortgroupref;
			pqueryNew->targetList = gpdb::PlAppendElement(pqueryNew->targetList, pteNew);
		}
	}
	pqueryDrdTbl->targetList = ctxgrpbymutator.m_plTENewGroupByQuery;

	// fix outer reference in the qual as well as in the range table entries
	SContextIncLevelsupMutator ctxinclvlmutator(0);
	pqueryDrdTbl->jointree->quals = CQueryMutators::PnodeIncrementLevelsupMutator((Node*) pqueryDrdTbl->jointree->quals, &ctxinclvlmutator);
	pqueryDrdTbl->rtable = gpdb::PlMutateRangeTable
									(
									pqueryDrdTbl->rtable,
									(Pnode) CQueryMutators::PnodeIncrementLevelsupMutator,
									&ctxgrpbymutator,
									0 // mutate sub queries
									);

	GPOS_ASSERT(gpdb::UlListLength(pqueryNew->targetList) <= gpdb::UlListLength(pquery->targetList));

	pqueryNew->hasWindFuncs = false;
	ReassignSortClause(pqueryNew, pqueryDrdTbl);

	return pqueryNew;
}

//---------------------------------------------------------------------------
//	@function:
//		CQueryMutators::PnodeWindowPrLMutator
//
//	@doc:
// 		Traverse the project list of extract all window functions in an
//		arbitrarily complex project element
//---------------------------------------------------------------------------
Node *
CQueryMutators::PnodeWindowPrLMutator
	(
	Node *pnode,
	void *pctx
	)
{
	if (NULL == pnode)
	{
		return NULL;
	}

	// do not traverse into sub queries as they will be inserted are inserted into
	// top-level query as is
	if (IsA(pnode, SubLink))
	{
		return (Node *) gpdb::PvCopyObject(pnode);
	}

	SContextGrpbyPlMutator *pctxgrpbymutator = (SContextGrpbyPlMutator *) pctx;
	const ULONG ulResNo = gpdb::UlListLength(pctxgrpbymutator->m_plTENewGroupByQuery) + 1;

	if (IsA(pnode, WindowRef))
	{
		// insert window operator into the derived table
        // and refer to it in the top-level query's target list
		WindowRef *pwindowref = (WindowRef*) gpdb::PvCopyObject(pnode);

		// get the function name and add it to the target list
		CMDIdGPDB *pmdidFunc = New(pctxgrpbymutator->m_pmp) CMDIdGPDB(pwindowref->winfnoid);
		const CWStringConst *pstr = CMDAccessorUtils::PstrWindowFuncName(pctxgrpbymutator->m_pmda, pmdidFunc);
		pmdidFunc->Release();

		TargetEntry *pte = gpdb::PteMakeTargetEntry
								(
								(Expr*) gpdb::PvCopyObject(pnode),
								(AttrNumber) ulResNo,
								CTranslatorUtils::SzFromWsz(pstr->Wsz()),
								false /* resjunk */
								);
		pctxgrpbymutator->m_plTENewGroupByQuery = gpdb::PlAppendElement(pctxgrpbymutator->m_plTENewGroupByQuery, pte);

		// return a variable referring to the new derived table's corresponding target list entry
		Var *pvarNew = gpdb::PvarMakeVar
								(
								1,
								(AttrNumber) ulResNo,
								gpdb::OidExprType(pnode),
								gpdb::IExprTypeMod(pnode),
								0 // query levelsup
								);

		return (Node*) pvarNew;
	}

	if (IsA(pnode, Var))
	{
		Var *pvarNew = NULL;

		TargetEntry *pteFound = gpdb::PteMember(pnode, pctxgrpbymutator->m_plTENewGroupByQuery);
		if (NULL == pteFound)
		{
			// insert target entry into the target list of the derived table
			CWStringConst strUnnamedCol(GPOS_WSZ_LIT("?column?"));
			TargetEntry *pte = gpdb::PteMakeTargetEntry
									(
									(Expr*) gpdb::PvCopyObject(pnode),
									(AttrNumber) ulResNo,
									CTranslatorUtils::SzFromWsz(strUnnamedCol.Wsz()),
									false /* resjunk */
									);
			pctxgrpbymutator->m_plTENewGroupByQuery = gpdb::PlAppendElement(pctxgrpbymutator->m_plTENewGroupByQuery, pte);

			pvarNew = gpdb::PvarMakeVar
								(
								1,
								(AttrNumber) ulResNo,
								gpdb::OidExprType(pnode),
								gpdb::IExprTypeMod(pnode),
								0 // query levelsup
								);
		}
		else
		{
			pteFound->resjunk = false; // ensure that the derived target list is not resjunked
			pvarNew = gpdb::PvarMakeVar
								(
								1,
								pteFound->resno,
								gpdb::OidExprType(pnode),
								gpdb::IExprTypeMod(pnode),
								0 // query levelsup
								);
		}

		return (Node*) pvarNew;
	}

	return gpdb::PnodeMutateExpressionTree(pnode, (Pnode) CQueryMutators::PnodeWindowPrLMutator, pctx);
}

//---------------------------------------------------------------------------
//	@function:
//		CQueryMutators::ReassignSortClause
//
//	@doc:
//		Reassign the sorting clause from the derived table to the new top-level query
//---------------------------------------------------------------------------
void
CQueryMutators::ReassignSortClause
	(
	Query *pqueryNew,
	Query *pqueryDrdTbl
	)
{
	pqueryNew->sortClause = pqueryDrdTbl->sortClause;
	pqueryNew->limitOffset = pqueryDrdTbl->limitOffset;
	pqueryNew->limitCount = pqueryDrdTbl->limitCount;
	pqueryDrdTbl->sortClause = NULL;
	pqueryDrdTbl->limitOffset = NULL;
	pqueryDrdTbl->limitCount = NULL;
}

// EOF
