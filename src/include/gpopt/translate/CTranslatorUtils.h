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

//---------------------------------------------------------------------------
//	@filename:
//		CTranslatorUtils.h
//
//	@doc:
//		Class providing utility methods for translating GPDB's PlannedStmt/Query
//		into DXL Tree
//
//	@test:
//
//
//---------------------------------------------------------------------------

#ifndef GPDXL_CTranslatorUtils_H
#define GPDXL_CTranslatorUtils_H
#define GPDXL_SYSTEM_COLUMNS 8

#include "gpopt/translate/CTranslatorScalarToDXL.h"
#include "gpopt/translate/CMappingColIdVarQuery.h"

#include "gpos/base.h"
#include "gpos/common/CBitSet.h"

#include "naucrates/dxl/operators/dxlops.h"
#include "naucrates/dxl/CIdGenerator.h"

#include "naucrates/md/CMDRelationGPDB.h"
#include "naucrates/md/IMDType.h"

#include "naucrates/statistics/IStatistics.h"

#include "nodes/parsenodes.h"
#include "access/sdir.h"
#include "access/skey.h"

// fwd declarations
namespace gpopt
{
	class CMDAccessor;

	// dynamic array of bitsets
	typedef CDynamicPtrArray<CBitSet, CleanupRelease> DrgPbs;
}

namespace gpdxl
{
	class CDXLTranslateContext;
}

namespace gpdxl
{
	using namespace gpopt;

	// hash maps mapping INT -> ULONG
	typedef CHashMap<INT, ULONG, gpos::UlHash<INT>, gpos::FEqual<INT>,
					CleanupDelete<INT>, CleanupDelete<ULONG> > HMIUl;

	//---------------------------------------------------------------------------
	//	@class:
	//		CTranslatorUtils
	//
	//	@doc:
	//		Class providing methods for translating GPDB's PlannedStmt/Query
	//		into DXL Tree
	//
	//---------------------------------------------------------------------------
	class CTranslatorUtils
	{
		private:

			// construct a set of column attnos corresponding to a single grouping set
			static
			CBitSet *PbsGroupingSet(IMemoryPool *pmp, List *plGroupElems, ULONG ulCols, HMUlUl *phmululGrpColPos, CBitSet *pbsGrpCols);

			// create a set of grouping sets for a rollup
			static
			DrgPbs *PdrgpbsRollup(IMemoryPool *pmp, GroupingClause *pgrcl, ULONG ulCols, HMUlUl *phmululGrpColPos, CBitSet *pbsGrpCols);

			// check if the given mdid array contains any of the polymorphic
			// types (ANYELEMENT, ANYARRAY)
			static
			BOOL FContainsPolymorphicTypes(DrgPmdid *pdrgpmdidTypes);

			// check if the given type mdid is the "ANYELEMENT" type
			static
			BOOL FAnyElement(IMDId *pmdidType);

			// check if the given type mdid is the "ANYARRAY" type
			static
			BOOL FAnyArray(IMDId *pmdidType);

			// resolve polymorphic types in the given array of type ids, replacing
			// them with the actual types obtained from the query
			static
			DrgPmdid *PdrgpmdidResolvePolymorphicTypes
						(
						IMemoryPool *pmp,
						DrgPmdid *pdrgpmdidTypes,
						List *plArgTypes,
						List *plArgsFromQuery
						);
			
			// update grouping col position mappings
			static
			void UpdateGrpColMapping(IMemoryPool *pmp, HMUlUl *phmululGrpColPos, CBitSet *pbsGrpCols, ULONG ulSortGrpRef);

			// check if a given oid is associated with a built-in object (type, function, etc)
			static
			BOOL FBuiltinObject(OID oidTyp);

		public:

			typedef struct CContextPreloadMD
			{
				public:
					// memory pool
					IMemoryPool *m_pmp;

					// MD accessor for function names
					CMDAccessor *m_pmda;

					CContextPreloadMD
						(
						IMemoryPool *pmp,
						CMDAccessor *pmda
						)
						: m_pmp(pmp), m_pmda(pmda)
					{}

					~CContextPreloadMD()
					{}

			} CContextPreloadMD;

			struct SCmptypeStrategy
			{
				IMDType::ECmpType ecomptype;
				StrategyNumber sn;

			};

			// get the GPDB scan direction from its corresponding DXL representation
			static
			ScanDirection Scandirection(EdxlIndexScanDirection edxlisd);

			// get the oid of comparison operator
			static
			OID OidCmpOperator(Expr* pexpr);

			// get the opclass for index key
			static
			OID OidIndexQualOpclass(INT iAttno, OID oidIndex);
			
			// return the type for the system column with the given number
			static
			CMDIdGPDB *PmdidSystemColType(IMemoryPool *pmp, AttrNumber attno);

			// find the n-th column descriptor in the table descriptor
			static
			const CDXLColDescr *Pdxlcd(const CDXLTableDescr *pdxltabdesc, ULONG ulPos);

			// return the name for the system column with given number
			static
			const CWStringConst *PstrSystemColName(AttrNumber attno);

			// translate the join type from its GPDB representation into the DXL one
			static
			EdxlJoinType EdxljtFromJoinType(JoinType jt);

			// translate the index scan direction from its GPDB representation into the DXL one
			static
			EdxlIndexScanDirection EdxlIndexDirection(ScanDirection sd);

			// create a DXL index descriptor from an index MD id
			static
			CDXLIndexDescr *Pdxlid(IMemoryPool *pmp, CMDAccessor *pmda, IMDId *pmdid);

			// translate a RangeTableEntry into a CDXLTableDescr
			static
			CDXLTableDescr *Pdxltabdesc
								(
								IMemoryPool *pmp,
								CMDAccessor *pmda,
								CIdGenerator *pidgtor,
								const RangeTblEntry *prte,
								BOOL *pfDistributedTable = NULL
								);

			// translate a RangeTableEntry into a CDXLLogicalTVF
			static
			CDXLLogicalTVF *Pdxltvf
								(
								IMemoryPool *pmp,
								CMDAccessor *pmda,
								CIdGenerator *pidgtor,
								const RangeTblEntry *prte
								);

			// get column descriptors from a record type
			static
			DrgPdxlcd *PdrgdxlcdRecord
						(
						IMemoryPool *pmp,
						CIdGenerator *pidgtor,
						List *plColNames,
						List *plColTypes
						);

			// get column descriptors from a record type
			static
			DrgPdxlcd *PdrgdxlcdRecord
						(
						IMemoryPool *pmp,
						CIdGenerator *pidgtor,
						List *plColNames,
						DrgPmdid *pdrgpmdidOutArgTypes
						);

			// get column descriptor from a base type
			static
			DrgPdxlcd *PdrgdxlcdBase
						(
						IMemoryPool *pmp,
						CIdGenerator *pidgtor,
						IMDId *pmdidRetType,
						CMDName *pmdName
						);

			// get column descriptors from a composite type
			static
			DrgPdxlcd *PdrgdxlcdComposite
						(
						IMemoryPool *pmp,
						CMDAccessor *pmda,
						CIdGenerator *pidgtor,
						const IMDType *pmdType
						);

			// expand a composite type into an array of IMDColumns
			static
			DrgPmdcol *ExpandCompositeType
						(
						IMemoryPool *pmp,
						CMDAccessor *pmda,
						const IMDType *pmdType
						);

			// preload metadata for a given type
			static
			void PreloadMDType(CMDAccessor *pmda, const IMDType *pmdtype);

			// preload helpers
			static
			BOOL FPreloadMDStatsWalker(Node *pnode, CContextPreloadMD *pstrtxpreloadmd);

			static
			void PreloadMDStats(IMemoryPool *pmp, CMDAccessor *pmda, OID oidRelation);

			// preload basic information in the MD cache, including base types
			// and MD objects referenced in the given query
			static
			void PreloadMD(IMemoryPool *pmp, CMDAccessor *pmda, CSystemId sysid, Query *pquery);

			// construct a CMDIdGPDB object with the version as returned by MD Versioning
			static
			CMDIdGPDB *PmdidWithVersion(IMemoryPool *pmp, OID objId);

			// return the dxl representation of the set operation
			static
			EdxlSetOpType Edxlsetop(SetOperation setop, BOOL fAll);

			// make copy of the TE map
			static
			TEMap *PtemapCopy(IMemoryPool *pmp, TEMap *ptemap);

			// return the set operator type
			static
			SetOperation Setoptype(EdxlSetOpType edxlsetop);

			// return the GPDB frame exclusion strategy from its corresponding DXL representation
			static
			WindowExclusion Windowexclusion(EdxlFrameExclusionStrategy edxlfes);

			// return the GPDB frame boundary kind from its corresponding DXL representation
			static
			WindowBoundingKind Windowboundkind(EdxlFrameBoundary edxlfb);

			// construct a dynamic array of sets of column attnos corresponding
			// to the group by clause
			static
			DrgPbs *PdrgpbsGroupBy(IMemoryPool *pmp, List *plGroupClause, ULONG ulCols, HMUlUl *phmululGrpColPos, CBitSet *pbsGrpCols);

			// return a copy of the query with constant of unknown type being coerced
			// to the common data type of the output target list
			static
			Query *PqueryFixUnknownTypeConstant(Query *pquery, List *plTargetList);

			// return the type of the nth non-resjunked target list entry
			static OID OidTargetListReturnType(List *plTargetList, ULONG ulColPos);

			// construct an array of DXL column identifiers for a target list
			static
			DrgPul *PdrgpulGenerateColIds
					(
					IMemoryPool *pmp,
					List *plTargetList,
					DrgPmdid *pdrgpmdidInput,
					DrgPul *pdrgpulInput,
					BOOL *pfOuterRef,
					CIdGenerator *pidgtorColId
					);

			// construct an array of DXL column descriptors for a target list
			// using the column ids in the given array
			static
			DrgPdxlcd *Pdrgpdxlcd(IMemoryPool *pmp, List *plTargetList, DrgPul *pdrgpulColIds, BOOL fKeepResjunked);

			// return the positions of the target list entries included in the output
			static
			DrgPul *PdrgpulPosInTargetList(IMemoryPool *pmp, List *plTargetList, BOOL fKeepResjunked);

			// construct a column descriptor from the given target entry, column identifier and position in the output
			static
			CDXLColDescr *Pdxlcd(IMemoryPool *pmp, TargetEntry *pte, ULONG ulColId, ULONG ulPos);

			// create a dummy project element to rename the input column identifier
			static
			CDXLNode *PdxlnDummyPrElem(IMemoryPool *pmp, ULONG ulColIdInput, ULONG ulColIdOutput, CDXLColDescr *pdxlcd);

			// construct a list of colids corresponding to the given target list
			// using the given attno->colid map
			static
			DrgPul *PdrgpulColIds(IMemoryPool *pmp, List *plTargetList, HMIUl *phmiulAttnoColId);

			// construct an array of column ids for the given group by set
			static
			DrgPul *PdrgpulGroupingCols(IMemoryPool *pmp, CBitSet *pbsGroupByCols, HMIUl *phmiulSortGrpColsColId);

			// return the Colid of column with given index
			static
			ULONG UlColId(INT iIndex, HMIUl *phmiul);

			// return the corresponding ColId for the given varno, varattno and querylevel
			static
			ULONG UlColId(ULONG ulQueryLevel, INT iVarno, INT iVarAttno, IMDId *pmdid, CMappingVarColId *pmapvarcolid);

			// check to see if the target list entry is a sorting column
			static
			BOOL FSortingColumn(const TargetEntry *pte, List *plSortCl);

			// check to see if the target list entry is used in the window reference
			static
			BOOL FWindowSpec(const TargetEntry *pte, List *plWindowClause);

			// extract a matching target entry that is a window spec
			static
			TargetEntry *PteWindowSpec(Node *pnode, List *plWindowClause, List *plTargetList);

			// check if the expression has a matching target entry that is a window spec
			static
			BOOL FWindowSpec(Node *pnode, List *plWindowClause, List *plTargetList);

			// create a scalar const value expression for the given int4 value
			static
			CDXLNode *PdxlnInt4Const(IMemoryPool *pmp, CMDAccessor *pmda, INT iVal);

			// check to see if the target list entry is a grouping column
			static
			BOOL FGroupingColumn(const TargetEntry *pte, List *plGrpCl);

			// check to see if the target list entry is a grouping column
			static
			BOOL FGroupingColumn(const TargetEntry *pte, const GroupClause *pgrcl);

			// check to see if the sorting column entry is a grouping column
			static
			BOOL FGroupingColumn(const SortClause *psortcl, List *plGrpCl);

			// check if the expression has a matching target entry that is a grouping column
			static
			BOOL FGroupingColumn(Node *pnode, List *plGrpCl, List *plTargetList);

			// extract a matching target entry that is a grouping column
			static
			TargetEntry *PteGroupingColumn(Node *pnode, List *plGrpCl, List *plTargetList);

			// convert a list of column ids to a list of attribute numbers using
			// the provided context with mappings
			static
			List *PlAttnosFromColids(DrgPul *pdrgpul, CDXLTranslateContext *pdxltrctx);
			
			// parse string value into a Long Integer
			static
			LINT LFromStr(const CWStringBase *pstr);

			// parse string value into an Integer
			static
			INT IFromStr(const CWStringBase *pstr);

			// check whether the given project list has a project element of the given
			// operator type
			static
			BOOL FHasProjElem(CDXLNode *pdxlnPrL, Edxlopid edxlopid);

			// create a multi-byte character string from a wide character string
			static
			CHAR *SzFromWsz(const WCHAR *wsz);
			
			static 
			HMUlUl *PhmululMap(IMemoryPool *pmp, DrgPul *pdrgpulOld, DrgPul *pdrgpulNew);

			// check if the given tree contains a subquery
			static
			BOOL FHasSubquery(Node *pnode);

			// check if the given function is a SIRV (single row volatile) that reads
			// or modifies SQL data
			static
			BOOL FSirvFunc(IMemoryPool *pmp, CMDAccessor *pmda, OID oidFunc);
			
			// is this a motion sensitive to duplicates
			static
			BOOL FDuplicateSensitiveMotion(CDXLPhysicalMotion *pdxlopMotion);

			// construct a project element with a const NULL expression
			static
			CDXLNode *PdxlnPrElNull(IMemoryPool *pmp, CMDAccessor *pmda, IMDId *pmdid, ULONG ulColId, const WCHAR *wszColName);

			// construct a project element with a const NULL expression
			static
			CDXLNode *PdxlnPrElNull(IMemoryPool *pmp, CMDAccessor *pmda, IMDId *pmdid, ULONG ulColId, CHAR *szAliasName);

			// create a DXL project element node with a Const NULL of type provided
			// by the column descriptor
			static
			CDXLNode *PdxlnPrElNull(IMemoryPool *pmp, CMDAccessor *pmda, CIdGenerator *pidgtorCol, const IMDColumn *pmdcol);

			// check required permissions for the range table
			static 
			void CheckRTEPermissions(List *plRangeTable);

			// check if an aggregate window function has either prelim or inverse prelim func
			static
			void CheckAggregateWindowFn(Node *pnode);

			// check if given column ids are outer references in the tree rooted by given node
            static
			void MarkOuterRefs(ULONG *pulColId, BOOL *pfOuterRef, ULONG ulColumns, CDXLNode *pdxlnode);

			// map DXL Subplan type to GPDB SubLinkType
			static
			SubLinkType Slink(EdxlSubPlanType edxlsubplantype);

			// map GPDB SubLinkType to DXL Subplan type
			static
			EdxlSubPlanType Edxlsubplantype(SubLinkType slink);

			// check whether there are triggers for the given operation on
			// the given relation
			static
			BOOL FRelHasTriggers(IMemoryPool *pmp, CMDAccessor *pmda, const IMDRelation *pmdrel, const EdxlDmlType edxldmltype);

			// check whether the given trigger is applicable to the given DML operation
			static
			BOOL FApplicableTrigger(CMDAccessor *pmda, IMDId *pmdidTrigger, const EdxlDmlType edxldmltype);
						
			// check whether there are NOT NULL or CHECK constraints for the given relation
			static
			BOOL FRelHasConstraints(const IMDRelation *pmdrel);

			// translate the list of error messages from an assert constraint list
			static 
			List *PlAssertErrorMsgs(CDXLNode *pdxlnAssertConstraintList);
	};
}

#endif // !GPDXL_CTranslatorUtils_H

// EOF
