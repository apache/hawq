//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2010 Greenplum, Inc.
//
//	@filename:
//		CTranslatorDXLToPlStmt.h
//
//	@doc:
//		Class providing methods for translating from DXL tree to GPDB PlannedStmt
//
//	@owner:
//		antovl
//
//	@test:
//
//
//---------------------------------------------------------------------------

#ifndef GPDXL_CTranslatorDxlToPlStmt_H
#define GPDXL_CTranslatorDxlToPlStmt_H

#include "postgres.h"
#include "gpopt/translate/CContextDXLToPlStmt.h"
#include "gpopt/translate/CDXLTranslateContext.h"
#include "gpopt/translate/CTranslatorDXLToScalar.h"
#include "gpopt/translate/CDXLTranslateContextBaseTable.h"
#include "gpopt/translate/CMappingColIdVarPlStmt.h"

#include "md/IMDRelationExternal.h"

#include "access/attnum.h"
#include "nodes/nodes.h"

#include "gpos/base.h"

#include "dxl/operators/dxlops.h"
#include "dxl/CIdGenerator.h"


// fwd declarations
namespace gpopt
{
	class CMDAccessor;
}

namespace gpmd
{
	class IMDIndex;
}

struct PlannedStmt;
struct Scan;
struct HashJoin;
struct NestLoop;
struct MergeJoin;
struct Hash;
struct RangeTblEntry;
struct Motion;
struct Limit;
struct Agg;
struct Append;
struct Sort;
struct SubqueryScan;
struct SubPlan;
struct Result;
struct Material;
struct ShareInputScan;
typedef Scan SeqScan;
typedef OpExpr DistinctExpr;
struct WindowFrame;
//struct Const;
//struct List;

namespace gpdxl
{

	using namespace gpopt;

	class CDXLNode;

	//---------------------------------------------------------------------------
	//	@class:
	//		CTranslatorDXLToPlStmt
	//
	//	@doc:
	//		Class providing methods for translating from DXL tree to GPDB PlannedStmt
	//
	//---------------------------------------------------------------------------
	class CTranslatorDXLToPlStmt
	{
		// shorthand for functions for translating DXL operator nodes into planner trees
		typedef Plan * (CTranslatorDXLToPlStmt::*PfPplan)(const CDXLNode *pdxln, CDXLTranslateContext *pdxltrctxOut, Plan *pplanParent);

		private:

			// pair of DXL operator type and the corresponding translator
			struct STranslatorMapping
			{
				// type
				Edxlopid edxlopid;

				// translator function pointer
				PfPplan pf;
			};
			// memory pool
			IMemoryPool *m_pmp;

			// meta data accessor
			CMDAccessor *m_pmda;

			// DXL operator translators indexed by the operator id
			PfPplan m_rgpfTranslators[EdxlopSentinel];

			CContextDXLToPlStmt *m_pctxdxltoplstmt;

			CTranslatorDXLToScalar *m_pdxlsctranslator;

			// command type
			CmdType m_cmdtype;
			
			// is target table distributed, false when in non DML statements
			BOOL m_fTargetTableDistributed;
			
			// list of result relations range table indexes for DML statements,
			// or NULL for select queries
			List *m_plResultRelations;
			
			// external scan counter
			ULONG m_ulExternalScanCounter;

			// private copy ctor
			CTranslatorDXLToPlStmt(const CTranslatorDXLToPlStmt&);

			// segment mapping for tables with LOCATION http:// or file://
			void MapLocationsFile
					(
					const IMDRelationExternal *pmdrelext,
					char **rgszSegFileMap,
					CdbComponentDatabases *pcdbCompDB
					);

			// segment mapping for tables with LOCATION gpfdist(s):// or custom protocol
			void MapLocationsFdist
					(
					const IMDRelationExternal *pmdrelext,
					char **rgszSegFileMap,
					CdbComponentDatabases *pcdbCompDB,
					Uri *pUri,
					const ULONG ulTotalPrimaries
					);

			// segment mapping for tables with EXECUTE 'cmd' ON.
			void MapLocationsExecute
					(
					const IMDRelationExternal *pmdrelext,
					char **rgszSegFileMap,
					CdbComponentDatabases *pcdbCompDB,
					const ULONG ulTotalPrimaries
					);

			// segment mapping for tables with EXECUTE 'cmd' on all segments
			void MapLocationsExecuteAllSegments
					(
					CHAR *szPrefixedCommand,
					char **rgszSegFileMap,
					CdbComponentDatabases *pcdbCompDB
					);

			// segment mapping for tables with EXECUTE 'cmd' per host
			void MapLocationsExecutePerHost
					(
					CHAR *szPrefixedCommand,
					char **rgszSegFileMap,
					CdbComponentDatabases *pcdbCompDB
					);

			// segment mapping for tables with EXECUTE 'cmd' on a given host
			void MapLocationsExecuteOneHost
					(
					CHAR *szHostName,
					CHAR *szPrefixedCommand,
					char **rgszSegFileMap,
					CdbComponentDatabases *pcdbCompDB
					);

			//segment mapping for tables with EXECUTE 'cmd' on N random segments
			void MapLocationsExecuteRandomSegments
					(
					ULONG ulSegments,
					const ULONG ulTotalPrimaries,
					CHAR *szPrefixedCommand,
					char **rgszSegFileMap,
					CdbComponentDatabases *pcdbCompDB
					);

			// segment mapping for tables with EXECUTE 'cmd' on a given segment
			void MapLocationsExecuteOneSegment
					(
					INT iTargetSegInd,
					CHAR *szPrefixedCommand,
					char **rgszSegFileMap,
					CdbComponentDatabases *pcdbCompDB
					);

			// list of URIs for external scan
			List* PlExternalScanUriList(const IMDRelationExternal *pmdrelext);

			// return the char representation of external scan format type
			CHAR CExternalScanFormatType(IMDRelationExternal::Erelextformattype erelextformat);

		public:
			// ctor
			CTranslatorDXLToPlStmt(IMemoryPool *pmp, CMDAccessor *pmda, CContextDXLToPlStmt *pctxdxltoplstmt);

			// dtor
			~CTranslatorDXLToPlStmt();

			// translate DXL operator node into a Plan node
			Plan *PplFromDXL
				(
				const CDXLNode *pdxln,
				CDXLTranslateContext *pdxltrctxOut,
				Plan *pplanParent
				);

			// main translation routine for DXL tree -> PlannedStmt
			PlannedStmt *PplstmtFromDXL(const CDXLNode *pdxln);

			// translate the join types from its DXL representation to the GPDB one
			static JoinType JtFromEdxljt(EdxlJoinType edxljt);

		private:

			// initialize index of operator translators
			void InitTranslators();

			// Set the bitmapset of a plan to the list of param_ids defined by the plan
			void SetParamIds(Plan *);

			// Set the qDispSliceId in the subplans defining an initplan
			void SetInitPlanSliceInformation(SubPlan *);

			// Set InitPlanVariable in PlannedStmt
			void SetInitPlanVariables(PlannedStmt *);

			// Returns the id of the plan;
			INT IPlanId(Plan* pplparent);

			// translate DXL table scan node into a SeqScan node
			Plan *PtsFromDXLTblScan
				(
				const CDXLNode *pdxlnTblScan,
				CDXLTranslateContext *pdxltrctxOut,
				Plan *pplanParent
				);

			// translate DXL index scan node into a IndexScan node
			Plan *PisFromDXLIndexScan
				(
				const CDXLNode *pdxlnIndexScan,
				CDXLTranslateContext *pdxltrctxOut,
				Plan *pplanParent
				);

			// translates a DXL index scan node into a IndexScan node
			Plan *PisFromDXLIndexScan
				(
				const CDXLNode *pdxlnIndexScan,
				CDXLPhysicalIndexScan *pdxlopIndexScan,
				CDXLTranslateContext *pdxltrctxOut,
				Plan *pplanParent,
				BOOL fIndexOnlyScan
				);

			// translate DXL hash join into a HashJoin node
			Plan *PhjFromDXLHJ
				(
				const CDXLNode *pdxlnHJ,
				CDXLTranslateContext *pdxltrctxOut,
				Plan *pplanParent
				);

			// translate DXL nested loop join into a NestLoop node
			Plan *PnljFromDXLNLJ
				(
				const CDXLNode *pdxlnNLJ,
				CDXLTranslateContext *pdxltrctxOut,
				Plan *pplanParent
				);

			// translate DXL merge join into a MergeJoin node
			Plan *PmjFromDXLMJ
				(
				const CDXLNode *pdxlnMJ,
				CDXLTranslateContext *pdxltrctxOut,
				Plan *pplanParent
				);

			// translate DXL motion node into GPDB Motion plan node
			Plan *PplanMotionFromDXLMotion
				(
				const CDXLNode *pdxlnMotion,
				CDXLTranslateContext *pdxltrctxOut,
				Plan *pplanParent
				);

			// translate DXL motion node
			Plan *PplanTranslateDXLMotion
				(
				const CDXLNode *pdxlnMotion,
				CDXLTranslateContext *pdxltrctxOut,
				Plan *pplanParent
				);

			// translate DXL duplicate sensitive redistribute motion node into 
			// GPDB result node with hash filters
			Plan *PplanResultHashFilters
				(
				const CDXLNode *pdxlnMotion,
				CDXLTranslateContext *pdxltrctxOut,
				Plan *pplanParent
				);

			// translate DXL aggregate node into GPDB Agg plan node
			Plan *PaggFromDXLAgg
				(
				const CDXLNode *pdxlnMotion,
				CDXLTranslateContext *pdxltrctxOut,
				Plan *pplanParent
				);

			// translate DXL window node into GPDB window node
			Plan *PwindowFromDXLWindow
				(
				const CDXLNode *pdxlnMotion,
				CDXLTranslateContext *pdxltrctxOut,
				Plan *pplanParent
				);

			// translate the DXL window frame into GPDB window frame node
			WindowFrame *Pwindowframe
				(
				const CDXLWindowFrame *pdxlwf,
				const CDXLTranslateContext *pdxltrctxChild,
				CDXLTranslateContext *pdxltrctxOut,
				Plan *pplan
				);

			// translate DXL sort node into GPDB Sort plan node
			Plan *PsortFromDXLSort
				(
				const CDXLNode *pdxlnSort,
				CDXLTranslateContext *pdxltrctxOut,
				Plan *pplanParent
				);

			// translate a DXL node into a Hash node
			Plan *PhhashFromDXL
				(
				const CDXLNode *pdxln,
				CDXLTranslateContext *pdxltrctxOut,
				Plan *pplanParent
				);

			// translate DXL Limit node into a Limit node
			Plan *PlimitFromDXLLimit
				(
				const CDXLNode *pdxlnLimit,
				CDXLTranslateContext *pdxltrctxOut,
				Plan *pplanParent
				);

			// translate DXL TVF into a GPDB Function Scan node
			Plan *PplanFunctionScanFromDXLTVF
				(
				const CDXLNode *pdxlnTVF,
				CDXLTranslateContext *pdxltrctxOut,
				Plan *pplanParent
				);

			Plan *PsubqscanFromDXLSubqScan
				(
				const CDXLNode *pdxlnSubqScan,
				CDXLTranslateContext *pdxltrctxOut,
				Plan *pplanParent
				);

			Plan *PresultFromDXLResult
				(
				const CDXLNode *pdxlnResult,
				CDXLTranslateContext *pdxltrctxOut,
				Plan *pplanParent
				);

			Plan *PappendFromDXLAppend
				(
				const CDXLNode *pdxlnAppend,
				CDXLTranslateContext *pdxltrctxOut,
				Plan *pplanParent
				);

			Plan *PmatFromDXLMaterialize
				(
				const CDXLNode *pdxlnMaterialize,
				CDXLTranslateContext *pdxltrctxOut,
				Plan *pplanParent
				);

			Plan *PshscanFromDXLSharedScan
				(
				const CDXLNode *pdxlnSharedScan,
				CDXLTranslateContext *pdxltrctxOut,
				Plan *pplanParent
				);

			// translate a sequence operator
			Plan *PplanSequence
				(
				const CDXLNode *pdxlnSequence,
				CDXLTranslateContext *pdxltrctxOut,
				Plan *pplanParent
				);

			// translate a dynamic table scan operator
			Plan *PplanDTS
				(
				const CDXLNode *pdxlnDTS,
				CDXLTranslateContext *pdxltrctxOut,
				Plan *pplanParent
				);	
			
			// translate a dynamic index scan operator
			Plan *PplanDIS
				(
				const CDXLNode *pdxlnDIS,
				CDXLTranslateContext *pdxltrctxOut,
				Plan *pplanParent
				);
			
			// translate a DML operator
			Plan *PplanDML
				(
				const CDXLNode *pdxlnDML,
				CDXLTranslateContext *pdxltrctxOut,
				Plan *pplanParent
				);

			// translate a Split operator
			Plan *PplanSplit
				(
				const CDXLNode *pdxlnSplit,
				CDXLTranslateContext *pdxltrctxOut,
				Plan *pplanParent
				);
			
			// translate a row trigger operator
			Plan *PplanRowTrigger
				(
				const CDXLNode *pdxlnRowTrigger,
				CDXLTranslateContext *pdxltrctxOut,
				Plan *pplanParent
				);

			// translate an Assert operator
			Plan *PplanAssert
				(
				const CDXLNode *pdxlnAssert,
				CDXLTranslateContext *pdxltrctxOut,
				Plan *pplanParent
				);

			// Initialize spooling information
			void InitializeSpoolingInfo
				(
				Plan *pplan,
				ULONG ulShareId
				);

			// retrieve the flow of the shared input scan of the cte consumers
			Flow *PflowCTEConsumer(List *plshscanCTEConsumer);

			// translate a CTE producer into a GPDB share input scan
			Plan *PshscanFromDXLCTEProducer
				(
				const CDXLNode *pdxlnCTEProducer,
				CDXLTranslateContext *pdxltrctxOut,
				Plan *pplanParent
				);

			// translate a CTE consumer into a GPDB share input scan
			Plan *PshscanFromDXLCTEConsumer
				(
				const CDXLNode *pdxlnCTEConsumer,
				CDXLTranslateContext *pdxltrctxOut,
				Plan *pplanParent
				);

			// create range table entry from a CDXLPhysicalTVF node
			RangeTblEntry *PrteFromDXLTVF
				(
				const CDXLNode *pdxlnTVF,
				CDXLTranslateContext *pdxltrctxOut,
				CDXLTranslateContextBaseTable *pdxltrctxbt,
				Plan *pplanParent
				);

			// create range table entry from a table descriptor
			RangeTblEntry *PrteFromTblDescr
				(
				const CDXLTableDescr *pdxltabdesc,
				const CDXLIndexDescr *pdxlid,
				ULONG ulRelColumns,
				Index iRel,
				CDXLTranslateContextBaseTable *pdxltrctxbtOut
				);

			// translate DXL projection list into a target list
			List *PlTargetListFromProjList
				(
				const CDXLNode *pdxlnPrL,
				const CDXLTranslateContextBaseTable *pdxltrctxbt,
				DrgPdxltrctx *pdrgpdxltrctx,
				CDXLTranslateContext *pdxltrctxOut,
				Plan *pplanParent
				);
			
			// insert NULL values for dropped attributes to construct the target list for a DML statement
			List *PlTargetListWithDroppedCols(List *plTargetList, const IMDRelation *pmdrel);

			// create a target list containing column references for a hash node from the
			// project list of its child node
			List *PlTargetListForHashNode
				(
				const CDXLNode *pdxlnPrL,
				CDXLTranslateContext *pdxltrctxChild,
				CDXLTranslateContext *pdxltrctxOut
				);
			
			List *PlQualFromFilter
				(
				const CDXLNode *pdxlnFilter,
				const CDXLTranslateContextBaseTable *pdxltrctxbt,
				DrgPdxltrctx *pdrgpdxltrctx,
				CDXLTranslateContext *pdxltrctxOut,
				Plan *pplanParent
				);


			// translate operator costs from the DXL cost structure into the types
			// used by GPDB
			void TranslatePlanCosts
				(
				const CDXLOperatorCost *pdxlopcost,
				Cost *pcostStartupOut,
				Cost *pcostTotalOut,
				Cost *pcostRowsOut,
				INT *piWidthOut
				);

			// shortcut for translating both the projection list and the filter
			void TranslateProjListAndFilter
				(
				const CDXLNode *pdxlnPrL,
				const CDXLNode *pdxlnFilter,
				const CDXLTranslateContextBaseTable *pdxltrctxbt,
				DrgPdxltrctx *pdrgpdxltrctx,
				List **pplTargetListOut,
				List **pplQualOut,
				CDXLTranslateContext *pdxltrctxOut,
				Plan *pplanParent
				);

			// translate the hash expr list of a redistribute motion node
			void TranslateHashExprList
				(
				const CDXLNode *pdxlnHashExprList,
				const CDXLTranslateContext *pdxltrctxChild,
				List **pplHashExprOut,
				List **pplHashExprTypesOut,
				CDXLTranslateContext *pdxltrctxOut,
				Plan *pplanParent
				);

			void TranslateSortCols
				(
				const CDXLNode *pdxlnSortColList,
				const CDXLTranslateContext *pdxltrctxChild,
				AttrNumber *pattnoSortColIds,
				Oid *poidSortOpIds
				);

			List *PlQualFromScalarCondNode
				(
				const CDXLNode *pdxlnFilter,
				const CDXLTranslateContextBaseTable *pdxltrctxbt,
				DrgPdxltrctx *pdrgpdxltrctx,
				CDXLTranslateContext *pdxltrctxOut,
				Plan *pplanParent
				);

			// parse string value into a Const
			static
			Cost CostFromStr(const CWStringBase *pstr);

			// check if the given operator is a DML operator on a distributed table
			BOOL FTargetTableDistributed(CDXLOperator *pdxlop);
			
			// add a target entry for the given colid to the given target list
			ULONG UlAddTargetEntryForColId
				(
				List **pplTargetList, 
				CDXLTranslateContext *pdxltrctx, 
				ULONG ulColId, 
				BOOL fResjunk
				);
			
			// translate the index condition list in an Index scan
			void TranslateIndexConditions
				(
				CDXLNode *pdxlnIndexCondList,
				const CDXLTableDescr *pdxltd,
				BOOL fIndexOnlyScan,
				const IMDIndex *pmdindex,
				const IMDRelation *pmdrel,
				CDXLTranslateContextBaseTable *pdxltrctxbt,
				CMappingColIdVarPlStmt *pmapcidvarplstmt,
				List **pplIndexConditions,
				List **pplIndexOrigConditions,
				List **pplIndexStratgey,
				List **pplIndexSubtype
				);

	};
}

#endif // !GPDXL_CTranslatorDxlToPlStmt_H

// EOF
