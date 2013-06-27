//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CQueryMutators.h
//
//	@doc:
//		Class providing methods for translating a GPDB Query object into a
//		DXL Tree
//
//	@owner:
//		raghav
//
//	@test:
//
//
//---------------------------------------------------------------------------

#ifndef GPDXL_CWalkerUtils_H
#define GPDXL_CWalkerUtils_H

#define GPDXL_QUERY_ID_START 0

#include "gpopt/translate/CMappingVarColId.h"
#include "gpopt/translate/CTranslatorScalarToDXL.h"
#include "gpopt/translate/CTranslatorUtils.h"

#include "gpos/base.h"

#include "dxl/operators/CDXLNode.h"
#include "md/IMDType.h"
#include "md/IMDType.h"

// fwd declarations
namespace gpopt
{
	class CMDAccessor;
}

struct Query;
struct RangeTblEntry;
struct GroupingClause;
struct SortClause;
typedef SortClause GroupClause;
struct Const;
struct List;


namespace gpdxl
{
	//---------------------------------------------------------------------------
	//	@class:
	//		CQueryMutators
	//
	//	@doc:
	//		Class providing methods for translating a GPDB Query object into a
	//      DXL Tree.
	//
	//---------------------------------------------------------------------------
	class CQueryMutators
	{
		typedef Node *(*Pfnode) ();
		typedef struct SContextHavingQualMutator
		{
			public:
				// memory pool
				IMemoryPool *m_pmp;

				// MD accessor for function names
				CMDAccessor *m_pmda;

				// the counter for Query's total number of target entries
				ULONG m_ulTECount;

				// the target list of the new group by query
				List *m_plTENewGroupByQuery;

				// the current query level
				ULONG m_ulCurrLevelsUp;

		 	 	// indicate whether we are mutating the argument of an aggregate
				BOOL m_fAggregateArg;

				// indicate the levels up of the aggregate we are mutating
				ULONG m_ulAggregateLevelUp;
				
				// ctor
				SContextHavingQualMutator
					(
					IMemoryPool *pmp,
					CMDAccessor *pmda,
					ULONG ulTECount,
					List *plTENewGroupByQuery
					)
					:
					m_pmp(pmp),
					m_pmda(pmda),
					m_ulTECount(ulTECount),
					m_plTENewGroupByQuery(plTENewGroupByQuery),
					m_ulCurrLevelsUp(0),
					m_fAggregateArg(false),
					m_ulAggregateLevelUp(ULONG_MAX)
				{
					GPOS_ASSERT(NULL != plTENewGroupByQuery);
				}

				// dtor
				~SContextHavingQualMutator()
				{}

		} CContextHavingQualMutator;

		typedef struct SContextGrpbyPlMutator
		{
			public:

				// memory pool
				IMemoryPool *m_pmp;

				// MD accessor to get the function name
				CMDAccessor *m_pmda;

				// original query
				Query *m_pquery;

				// the new target list of the group by query
				List *m_plTENewGroupByQuery;

				// the current query level
				ULONG m_ulCurrLevelsUp;

				// the sorting / grouping reference of the original target list entry
				ULONG m_ulRessortgroupref;

				// indicate whether we are mutating the argument of an aggregate
				BOOL m_fAggregateArg;

				// indicate whether we are mutating an expression representing a grouping column
				BOOL m_fGroupingCol;
				
				// ctor
				SContextGrpbyPlMutator
					(
					IMemoryPool *pmp,
					CMDAccessor *pmda,
					Query *pquery,
					List *plTENewGroupByQuery
					)
					:
					m_pmp(pmp),
					m_pmda(pmda),
					m_pquery(pquery),
					m_plTENewGroupByQuery(plTENewGroupByQuery),
					m_ulCurrLevelsUp(0),
					m_ulRessortgroupref(0),
					m_fAggregateArg(false),
					m_fGroupingCol(false)
				{
				}

				// dtor
				~SContextGrpbyPlMutator()
				{}

		} CContextGrpbyPlMutator;

		typedef struct SContextIncLevelsupMutator
		{
			public:

				// the current query level
				ULONG m_ulCurrLevelsUp;
				
				// the comparison operation used to compare query levels
				BOOL m_fOnlyCurrentLevel;

				// fix target list entry of the top level
				BOOL m_fFixTargetListTopLevel;

				// ctor
				SContextIncLevelsupMutator
					(
					ULONG ulCurrLevelsUp,
					BOOL fOnlyCurrentLevel,
					BOOL fFixTargetListTopLevel
					)
					:
					m_ulCurrLevelsUp(ulCurrLevelsUp),
					m_fOnlyCurrentLevel(fOnlyCurrentLevel),
					m_fFixTargetListTopLevel(fFixTargetListTopLevel)
				{
				}

				// dtor
				~SContextIncLevelsupMutator()
				{}

		} CContextIncLevelsupMutator;

		private:

			// check if the cte levels up needs to be corrected
			static
			BOOL FNeedsLevelsUpCorrection(SContextIncLevelsupMutator *pctxinclvlmutator, Index idxCtelevelsup);

		public:

			// check if the project list contains expressions on aggregates thereby needing normalization
			static
			BOOL FNeedsPrLNormalization(const Query *pquery);

			// normalize query
			static
			Query *PqueryNormalize(IMemoryPool *pmp, CMDAccessor *pmda, const Query *pquery);

			// check if the project list contains expressions on window operators thereby needing normalization
			static
			BOOL FNeedsWindowPrLNormalization(const Query *pquery);

			// flatten expressions in window operation project list
			static
			Query *PqueryNormalizeWindowPrL(IMemoryPool *pmp, CMDAccessor *pmda, const Query *pquery);

			// traverse the project list to extract all window functions in an arbitrarily complex project element
			static
			Node *PnodeWindowPrLMutator(Node *pnode, void *ctx);

			// flatten expressions in project list
			static
			Query *PqueryNormalizeGrpByPrL(IMemoryPool *pmp, CMDAccessor *pmda, const Query *pquery);

			// make a copy of the aggref (minus the arguments)
			static
			Aggref *PaggrefFlatCopy(Aggref *paggrefOld);

			// create a new entry in the derived table and return its corresponding var
			static
			Var *PvarInsertIntoDerivedTable(Node *pnode, SContextHavingQualMutator *context);

			// check if a matching node exists in the list of target entries
			static
			Node *PnodeFind(Node *pnode, SContextHavingQualMutator *pctx);

			// increment the levels up of outer references
			static
			Var *PvarOuterReferenceIncrLevelsUp(Var *pvar);

			// pull up having clause into a select
			static
			Query *PqueryNormalizeHaving(IMemoryPool *pmp, CMDAccessor *pmda, const Query *pquery);

			// traverse the expression and fix the levels up of any outer reference
			static
			Node *PnodeIncrementLevelsupMutator(Node *pnode, void *ctx);

			// traverse the expression and fix the levels up of any CTE
			static
			Node *PnodeFixCTELevelsupMutator(Node *pnode, void *ctx);

			// traverse the project list of a groupby operator, to
			// extract all aggregate functions in an arbitrarily complex project element,
			static
			Node *PnodeGrpbyPrLMutator(Node *pnode, void *ctx);

			// mutate the grouping columns, fix levels up when necessary
			static
			Node *PnodeGroupingColMutator(Node *pnode, TargetEntry *pteOriginal, SContextGrpbyPlMutator *pctxGrpByMutator);

			// return a target entry for the aggregate or percentile expression
			static
			TargetEntry *PteAggregateOrPercentileExpr(IMemoryPool *pmp, CMDAccessor *pmda, Node *pnode, ULONG ulAttno);

			// traverse the having qual to extract all aggregate functions,
			// fix correlated vars and return the modified having qual
			static
			Node *PnodeHavingQualMutator(Node *pnode, void *ctx);

			// for a given an TE in the derived table, create a new TE to be added to the top level query
			static
			TargetEntry *Pte(TargetEntry *pte, ULONG ulVarAttno);

			// return the column name of the target entry
			static
			CHAR* SzTEName(TargetEntry *pte, Query *pquery);

			// make the input query into a derived table and return a new root query
			static
			Query *PqueryConvertToDerivedTable(const Query *pquery, BOOL fFixTargetList, BOOL fFixHavingQual);

			// eliminate distinct clause
			static
			Query *PqueryEliminateDistinctClause(const Query *pquery);

			// reassign the sorting clause from the derived table to the new top-level query
			static
			void ReassignSortClause(Query *pqueryNew, Query *pqueryDrdTbl);

			// fix window frame edge boundary when its value is defined by a subquery
			static
			Query *PqueryFixWindowFrameEdgeBoundary(const Query *pquery);
	};
}
#endif // GPDXL_CWalkerUtils_H

//EOF
