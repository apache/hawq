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
//		CTranslatorDXLToPlStmt.cpp
//
//	@doc:
//		Implementation of the methods for translating from DXL tree to GPDB
//		PlannedStmt.
//
//	@test:
//
//
//---------------------------------------------------------------------------

#include "postgres.h"
#include "nodes/nodes.h"
#include "nodes/pg_list.h"
#include "nodes/plannodes.h"
#include "nodes/primnodes.h"
#include "catalog/gp_policy.h"
#include "catalog/pg_exttable.h"
#include "cdb/cdbutil.h"
#include "cdb/cdbvars.h"
#include "cdb/partitionselection.h"
#include "utils/uri.h"
#include "gpos/base.h"

#include "gpopt/mdcache/CMDAccessor.h"
#include "gpopt/translate/CTranslatorDXLToPlStmt.h"
#include "gpopt/translate/CTranslatorUtils.h"
#include "gpopt/translate/CIndexQualInfo.h"

#include "naucrates/dxl/operators/CDXLNode.h"
#include "naucrates/dxl/operators/CDXLDirectDispatchInfo.h"

#include "naucrates/md/IMDFunction.h"
#include "naucrates/md/IMDScalarOp.h"
#include "naucrates/md/IMDAggregate.h"
#include "naucrates/md/IMDType.h"
#include "naucrates/md/IMDTypeBool.h"
#include "naucrates/md/IMDTypeInt4.h"
#include "naucrates/md/IMDIndex.h"
#include "naucrates/md/IMDRelationExternal.h"

#include "gpopt/gpdbwrappers.h"

using namespace gpdxl;
using namespace gpos;
using namespace gpopt;
using namespace gpmd;

#define GPDXL_ROOT_PLAN_ID -1
#define GPDXL_PLAN_ID_START 1
#define GPDXL_MOTION_ID_START 1
#define GPDXL_PARAM_ID_START 0

//---------------------------------------------------------------------------
//	@function:
//		ShtypeFromSpoolInfo
//
//	@doc:
//		Helper function for extracting the GPDB share type from a DXL spool info object
//
//---------------------------------------------------------------------------
ShareType
ShtypeFromSpoolInfo(const CDXLSpoolInfo *pspoolinfo)
{
	ShareType shtype = SHARE_NOTSHARED;

	GPOS_ASSERT(NULL != pspoolinfo);

	if (EdxlspoolMaterialize == pspoolinfo->Edxlsptype())
	{
		shtype = SHARE_MATERIAL;
		if (pspoolinfo->FMultiSlice())
		{
			shtype = SHARE_MATERIAL_XSLICE;
		}
	}
	else if (EdxlspoolSort == pspoolinfo->Edxlsptype())
	{
		shtype = SHARE_SORT;
		if (pspoolinfo->FMultiSlice())
		{
			shtype = SHARE_SORT_XSLICE;
		}
	}

	return shtype;
}


//---------------------------------------------------------------------------
//	@function:
//		CTranslatorDXLToPlStmt::CTranslatorDXLToPlStmt
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CTranslatorDXLToPlStmt::CTranslatorDXLToPlStmt
	(
	IMemoryPool *pmp,
	CMDAccessor *pmda,
	CContextDXLToPlStmt* pctxdxltoplstmt,
	ULONG ulSegments
	)
	:
	m_pmp(pmp),
	m_pmda(pmda),
	m_pctxdxltoplstmt(pctxdxltoplstmt),
	m_cmdtype(CMD_SELECT),
	m_fTargetTableDistributed(false),
	m_plResultRelations(NULL),
	m_ulExternalScanCounter(0),
	m_ulSegments(ulSegments),
	m_ulPartitionSelectorCounter(0)
{
	m_pdxlsctranslator = GPOS_NEW(m_pmp) CTranslatorDXLToScalar(m_pmp, m_pmda, m_ulSegments);
	InitTranslators();
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorDXLToPlStmt::~CTranslatorDXLToPlStmt
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CTranslatorDXLToPlStmt::~CTranslatorDXLToPlStmt()
{
	GPOS_DELETE(m_pdxlsctranslator);
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorDXLToPlStmt::InitTranslators
//
//	@doc:
//		Initialize index of translators
//
//---------------------------------------------------------------------------
void
CTranslatorDXLToPlStmt::InitTranslators()
{
	for (ULONG ul = 0; ul < GPOS_ARRAY_SIZE(m_rgpfTranslators); ul++)
	{
		m_rgpfTranslators[ul] = NULL;
	}

	// array mapping operator type to translator function
	STranslatorMapping rgTranslators[] =
	{
			{EdxlopPhysicalTableScan,				&gpopt::CTranslatorDXLToPlStmt::PtsFromDXLTblScan},
			{EdxlopPhysicalExternalScan,			&gpopt::CTranslatorDXLToPlStmt::PtsFromDXLTblScan},
			//{EdxlopPhysicalIndexOnlyScan,			&gpopt::CTranslatorDXLToPlStmt::PiosFromDXLIndexOnlyScan},
			{EdxlopPhysicalIndexScan,				&gpopt::CTranslatorDXLToPlStmt::PisFromDXLIndexScan},
			{EdxlopPhysicalHashJoin, 				&gpopt::CTranslatorDXLToPlStmt::PhjFromDXLHJ},
			{EdxlopPhysicalNLJoin, 					&gpopt::CTranslatorDXLToPlStmt::PnljFromDXLNLJ},
			{EdxlopPhysicalMergeJoin,				&gpopt::CTranslatorDXLToPlStmt::PmjFromDXLMJ},
			{EdxlopPhysicalMotionGather,			&gpopt::CTranslatorDXLToPlStmt::PplanMotionFromDXLMotion},
			{EdxlopPhysicalMotionBroadcast,			&gpopt::CTranslatorDXLToPlStmt::PplanMotionFromDXLMotion},
			{EdxlopPhysicalMotionRedistribute,		&gpopt::CTranslatorDXLToPlStmt::PplanTranslateDXLMotion},
			{EdxlopPhysicalMotionRandom,			&gpopt::CTranslatorDXLToPlStmt::PplanTranslateDXLMotion},
			{EdxlopPhysicalMotionRoutedDistribute,	&gpopt::CTranslatorDXLToPlStmt::PplanMotionFromDXLMotion},
			{EdxlopPhysicalLimit, 					&gpopt::CTranslatorDXLToPlStmt::PlimitFromDXLLimit},
			{EdxlopPhysicalAgg, 					&gpopt::CTranslatorDXLToPlStmt::PaggFromDXLAgg},
			{EdxlopPhysicalWindow, 					&gpopt::CTranslatorDXLToPlStmt::PwindowFromDXLWindow},
			{EdxlopPhysicalSort,					&gpopt::CTranslatorDXLToPlStmt::PsortFromDXLSort},
			{EdxlopPhysicalSubqueryScan,			&gpopt::CTranslatorDXLToPlStmt::PsubqscanFromDXLSubqScan},
			{EdxlopPhysicalResult, 					&gpopt::CTranslatorDXLToPlStmt::PresultFromDXLResult},
			{EdxlopPhysicalAppend, 					&gpopt::CTranslatorDXLToPlStmt::PappendFromDXLAppend},
			{EdxlopPhysicalMaterialize, 			&gpopt::CTranslatorDXLToPlStmt::PmatFromDXLMaterialize},
			{EdxlopPhysicalSharedScan, 				&gpopt::CTranslatorDXLToPlStmt::PshscanFromDXLSharedScan},
			{EdxlopPhysicalSequence, 				&gpopt::CTranslatorDXLToPlStmt::PplanSequence},
			{EdxlopPhysicalDynamicTableScan,		&gpopt::CTranslatorDXLToPlStmt::PplanDTS},
			{EdxlopPhysicalDynamicIndexScan,		&gpopt::CTranslatorDXLToPlStmt::PplanDIS},
			{EdxlopPhysicalTVF,						&gpopt::CTranslatorDXLToPlStmt::PplanFunctionScanFromDXLTVF},
			{EdxlopPhysicalDML,						&gpopt::CTranslatorDXLToPlStmt::PplanDML},
			{EdxlopPhysicalSplit,					&gpopt::CTranslatorDXLToPlStmt::PplanSplit},
			{EdxlopPhysicalRowTrigger,				&gpopt::CTranslatorDXLToPlStmt::PplanRowTrigger},
			{EdxlopPhysicalAssert,					&gpopt::CTranslatorDXLToPlStmt::PplanAssert},
			{EdxlopPhysicalCTEProducer, 			&gpopt::CTranslatorDXLToPlStmt::PshscanFromDXLCTEProducer},
			{EdxlopPhysicalCTEConsumer, 			&gpopt::CTranslatorDXLToPlStmt::PshscanFromDXLCTEConsumer},
			{EdxlopPhysicalBitmapTableScan,			&gpopt::CTranslatorDXLToPlStmt::PplanBitmapTableScan},
			{EdxlopPhysicalDynamicBitmapTableScan,	&gpopt::CTranslatorDXLToPlStmt::PplanBitmapTableScan},
			{EdxlopPhysicalCTAS, 					&gpopt::CTranslatorDXLToPlStmt::PplanCTAS},
			{EdxlopPhysicalPartitionSelector,		&gpopt::CTranslatorDXLToPlStmt::PplanPartitionSelector},
	};

	const ULONG ulTranslators = GPOS_ARRAY_SIZE(rgTranslators);

	for (ULONG ul = 0; ul < ulTranslators; ul++)
	{
		STranslatorMapping elem = rgTranslators[ul];
		m_rgpfTranslators[elem.edxlopid] = elem.pf;
	}
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorDXLToPlStmt::PplstmtFromDXL
//
//	@doc:
//		Translate DXL node into a PlannedStmt
//
//---------------------------------------------------------------------------
PlannedStmt *
CTranslatorDXLToPlStmt::PplstmtFromDXL
	(
	const CDXLNode *pdxln
	)
{
	GPOS_ASSERT(NULL != pdxln);

	CDXLTranslateContext dxltrctx(m_pmp, false);

	DrgPdxltrctx *pdrgpdxltrctxPrevSiblings = GPOS_NEW(m_pmp) DrgPdxltrctx(m_pmp);
	Plan *pplan = PplFromDXL(pdxln, &dxltrctx, NULL, pdrgpdxltrctxPrevSiblings);
	pdrgpdxltrctxPrevSiblings->Release();

	GPOS_ASSERT(NULL != pplan);

	// collect oids from rtable
	List *plOids = NIL;

	ListCell *plcRTE = NULL;
	ForEach (plcRTE, m_pctxdxltoplstmt->PlPrte())
	{
		RangeTblEntry *pRTE = (RangeTblEntry *) lfirst(plcRTE);

		if (pRTE->rtekind == RTE_RELATION)
		{
			plOids = gpdb::PlAppendOid(plOids, pRTE->relid);
		}
	}

	// assemble planned stmt
	PlannedStmt *pplstmt = MakeNode(PlannedStmt);
	pplstmt->planGen = PLANGEN_OPTIMIZER;
	
	pplstmt->rtable = m_pctxdxltoplstmt->PlPrte();
	pplstmt->subplans = m_pctxdxltoplstmt->PlPplanSubplan();
	pplstmt->planTree = pplan;

	// store partitioned table indexes in planned stmt
	pplstmt->queryPartOids = m_pctxdxltoplstmt->PlPartitionedTables();
	pplstmt->canSetTag = true;
	pplstmt->relationOids = plOids;
	pplstmt->numSelectorsPerScanId = m_pctxdxltoplstmt->PlNumPartitionSelectors();

	pplan->nMotionNodes  = m_pctxdxltoplstmt->UlCurrentMotionId()-1;
	pplstmt->nMotionNodes =  m_pctxdxltoplstmt->UlCurrentMotionId()-1;

	pplstmt->commandType = m_cmdtype;
	
	GPOS_ASSERT(pplan->nMotionNodes >= 0);
	if (0 == pplan->nMotionNodes && !m_fTargetTableDistributed)
	{
		// no motion nodes and not a DML on a distributed table
		pplan->dispatch = DISPATCH_SEQUENTIAL;
	}
	else
	{
		pplan->dispatch = DISPATCH_PARALLEL;
	}
	
	pplstmt->resultRelations = m_plResultRelations;
	pplstmt->intoClause = m_pctxdxltoplstmt->Pintocl();
	pplstmt->intoPolicy = m_pctxdxltoplstmt->Pdistrpolicy();
	
	SetInitPlanVariables(pplstmt);
	
	if (CMD_SELECT == m_cmdtype && NULL != pdxln->Pdxlddinfo())
	{
		List *plDirectDispatchSegIds = PlDirectDispatchSegIds(pdxln->Pdxlddinfo());
		pplan->directDispatch.contentIds = plDirectDispatchSegIds;
		pplan->directDispatch.isDirectDispatch = (NIL != plDirectDispatchSegIds);
		
		if (pplan->directDispatch.isDirectDispatch)
		{
			List *plMotions = gpdb::PlExtractNodesPlan(pplstmt->planTree, T_Motion, true /*descendIntoSubqueries*/);
			ListCell *plc = NULL;
			ForEach(plc, plMotions)
			{
				Motion *pmotion = (Motion *) lfirst(plc);
				GPOS_ASSERT(IsA(pmotion, Motion));
				GPOS_ASSERT(gpdb::FMotionGather(pmotion));
				
				pmotion->plan.directDispatch.isDirectDispatch = true;
				pmotion->plan.directDispatch.contentIds = pplan->directDispatch.contentIds;
			}
		}
	}
	
	return pplstmt;
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorDXLToPlStmt::PplFromDXL
//
//	@doc:
//		Translates a DXL tree into a Plan node
//
//---------------------------------------------------------------------------
Plan *
CTranslatorDXLToPlStmt::PplFromDXL
	(
	const CDXLNode *pdxln,
	CDXLTranslateContext *pdxltrctxOut,
	Plan *pplanParent,
	DrgPdxltrctx *pdrgpdxltrctxPrevSiblings
	)
{
	GPOS_ASSERT(NULL != pdxln);
	GPOS_ASSERT(NULL != pdrgpdxltrctxPrevSiblings);

	CDXLOperator *pdxlop = pdxln->Pdxlop();
	ULONG ulOpId =  (ULONG) pdxlop->Edxlop();

	PfPplan pf = m_rgpfTranslators[ulOpId];

	if (NULL == pf)
	{
		GPOS_RAISE(gpdxl::ExmaDXL, gpdxl::ExmiDXL2PlStmtConversion, pdxln->Pdxlop()->PstrOpName()->Wsz());
	}

	return (this->* pf)(pdxln, pdxltrctxOut, pplanParent, pdrgpdxltrctxPrevSiblings);
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorDXLToPlStmt::IPlanId
//
//	@doc:
//		If pplParent == NULL returns -1 else eeturns the plan id.
//
//---------------------------------------------------------------------------
INT
CTranslatorDXLToPlStmt::IPlanId(Plan* pplParent)
{
	if (NULL == pplParent)
	{
		return GPDXL_ROOT_PLAN_ID;
	}
	else
	{
		return pplParent->plan_node_id;
	}
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorDXLToPlStmt::SetInitPlanVariables
//
//	@doc:
//		Iterates over the plan to set the qDispSliceId that is found in the plan
//		as well as its subplans. Set the number of parameters used in the plan.
//---------------------------------------------------------------------------
void
CTranslatorDXLToPlStmt::SetInitPlanVariables(PlannedStmt* pplstmt)
{
	if(1 != m_pctxdxltoplstmt->UlCurrentMotionId()) // For Distributed Tables m_ulMotionId > 1
	{
		pplstmt->nInitPlans = m_pctxdxltoplstmt->UlCurrentParamId();
		pplstmt->planTree->nInitPlans = m_pctxdxltoplstmt->UlCurrentParamId();
	}

	pplstmt->planTree->nParamExec = m_pctxdxltoplstmt->UlCurrentParamId();
	pplstmt->nCrossLevelParams = m_pctxdxltoplstmt->UlCurrentParamId();

	// Extract all subplans defined in the planTree
	List *plSubPlans = gpdb::PlExtractNodesPlan(pplstmt->planTree, T_SubPlan, true);

	ListCell *plc = NULL;

	ForEach (plc, plSubPlans)
	{
		SubPlan *psubplan = (SubPlan*) lfirst(plc);
		if (psubplan->is_initplan)
		{
			SetInitPlanSliceInformation(psubplan);
		}
	}

	// InitPlans can also be defined in subplans. We therefore have to iterate
	// over all the subplans referred to in the planned statement.

	List *plInitPlans = pplstmt->subplans;

	ForEach (plc,plInitPlans)
	{
		plSubPlans = gpdb::PlExtractNodesPlan((Plan*) lfirst(plc), T_SubPlan, true);
		ListCell *plc2;

		ForEach (plc2, plSubPlans)
		{
			SubPlan *psubplan = (SubPlan*) lfirst(plc2);
			if (psubplan->is_initplan)
			{
				SetInitPlanSliceInformation(psubplan);
			}
		}
	}
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorDXLToPlStmt::SetInitPlanSliceInformation
//
//	@doc:
//		Set the qDispSliceId for a given subplans. In GPDB once all motion node
// 		have been assigned a slice, each initplan is assigned a slice number.
//		The initplan are traversed in an postorder fashion. Since in CTranslatorDXLToPlStmt
//		we assign the plan_id to each initplan in a postorder fashion, we take
//		advantage of this.
//
//---------------------------------------------------------------------------
void
CTranslatorDXLToPlStmt::SetInitPlanSliceInformation(SubPlan * psubplan)
{
	GPOS_ASSERT(psubplan->is_initplan && "This is processed for initplans only");

	if (psubplan->is_initplan)
	{
		GPOS_ASSERT(0 < m_pctxdxltoplstmt->UlCurrentMotionId());

		if(1 < m_pctxdxltoplstmt->UlCurrentMotionId())
		{
			psubplan->qDispSliceId =  m_pctxdxltoplstmt->UlCurrentMotionId() + psubplan->plan_id-1;
		}
		else
		{
			psubplan->qDispSliceId = 0;
		}
	}
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorDXLToPlStmt::SetParamIds
//
//	@doc:
//		Set the bitmapset with the param_ids defined in the plan
//
//---------------------------------------------------------------------------
void
CTranslatorDXLToPlStmt::SetParamIds(Plan* pplan)
{
	List *plParams = gpdb::PlExtractNodesPlan(pplan, T_Param, true);

	ListCell *plc = NULL;

	Bitmapset  *pbitmapset = NULL;

	ForEach (plc, plParams)
	{
		Param *pparam = (Param*) lfirst(plc);
		pbitmapset = gpdb::PbmsAddMember(pbitmapset, pparam->paramid);
	}

	pplan->extParam = pbitmapset;
	pplan->allParam = pbitmapset;
}


//---------------------------------------------------------------------------
//	@function:
//		CTranslatorDXLToPlStmt::MapLocationsFile
//
//	@doc:
//		Segment mapping for tables with LOCATION http:// or file://
//		These two protocols are very similar in that they enforce a 1-URI:1-segdb
//		relationship. The only difference between them is that file:// URI must
//		be assigned to a segdb on a host that is local to that URI.
//
//---------------------------------------------------------------------------
void
CTranslatorDXLToPlStmt::MapLocationsFile
	(
	OID oidRel,
	char **rgszSegFileMap,
	List *segments
	)
{
	// extract file path and name from URI strings and assign them a primary segdb
	
	ExtTableEntry *extentry = gpdb::Pexttable(oidRel);

	ListCell *plcLocation = NULL;
	ForEach (plcLocation, extentry->locations)
	{
		Value* pvLocation = (Value *)lfirst(plcLocation);
		CHAR *szUri = pvLocation->val.str;
		
		Uri *pUri = gpdb::PuriParseExternalTable(szUri);

		BOOL fCandidateFound = false;
		BOOL fMatchFound = false;

		ListCell *lcSegment;
		// try to find a segment database that can handle this uri
		ForEach(lcSegment, segments)
		{
		  Segment *segment = (Segment *)lfirst(lcSegment);
		  INT iSegInd = segment->segindex;
		  if (segment->alive)
		  {
        if (URI_FILE == pUri->protocol &&
          0 != gpdb::IStrCmpIgnoreCase(pUri->hostname, segment->hostname) &&
          0 != gpdb::IStrCmpIgnoreCase(pUri->hostname, segment->hostip) &&
          0 != gpdb::IStrCmpIgnoreCase(pUri->hostname, "localhost"))
        {
          continue;
        }

        fCandidateFound = true;
        if (NULL == rgszSegFileMap[iSegInd])
        {
          rgszSegFileMap[iSegInd] = PStrDup(szUri);
          fMatchFound = true;
        }
		  }
		  if (fMatchFound)
		  {
		    break;
		  }
		}
	}	
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorDXLToPlStmt::MapLocationsFdist
//
//	@doc:
// 		Segment mapping for tables with LOCATION gpfdist(s):// or custom protocol
//		The user supplied gpfdist(s):// URIs are duplicated so that there is one
//		available to every segdb. However, in some cases (as determined by
//		gp_external_max_segs GUC) we don't want to use *all* segdbs but instead
//		figure out how many and pick them randomly (for better performance)
//
//---------------------------------------------------------------------------
void
CTranslatorDXLToPlStmt::MapLocationsFdist
	(
	OID oidRel,
	char **rgszSegFileMap,
	List *segments,
	Uri *pUri,
	const ULONG ulTotalPrimaries,
	IMDId *pmdidRel,
	List *plQuals
	)
{
	ULONG ulParticipatingSegments = ulTotalPrimaries;
	ULONG ulMaxParticipants = ulParticipatingSegments;
	
	ExtTableEntry *extentry = gpdb::Pexttable(oidRel);

	const ULONG ulLocations = gpdb::UlListLength(extentry->locations);
	BOOL fPxfProtocol = gpdb::FPxfProtocol(pUri);

	if (URI_GPFDIST == pUri->protocol || URI_GPFDISTS == pUri->protocol)
	{
		ulMaxParticipants = ulLocations * gp_external_max_segs;
	}
	else if (fPxfProtocol)
	{
		ulMaxParticipants = gpdb::IMaxParticipantsPxf(ulTotalPrimaries);
		if(ulParticipatingSegments > ulMaxParticipants)
			elog(NOTICE, "External scan using PXF protocol will utilize %d out "
				 "of %d segment databases", ulMaxParticipants, ulParticipatingSegments);
		ulParticipatingSegments = ulMaxParticipants;
	}

	ULONG ulSkip = 0;
	BOOL fSkipRandomly = false;
	if (ulParticipatingSegments > ulMaxParticipants)
	{
		elog(NOTICE, "External scan using GPFDIST protocol will utilize %d out "
			 "of %d segment databases", ulMaxParticipants, ulParticipatingSegments);
		
		ulSkip = ulParticipatingSegments - ulMaxParticipants;
		ulParticipatingSegments = ulMaxParticipants;
		fSkipRandomly = true;
	}

	if (ulLocations > ulParticipatingSegments)
	{
		GPOS_RAISE(gpdxl::ExmaDXL, gpdxl::ExmiDXL2PlStmtExternalScanError,
				GPOS_WSZ_LIT("There are more external files (URLs) than primary segments that can read them"));
	}

	BOOL fDone = false;
	List *plModifiedLocations = NIL;
	ULONG ulModifiedLocations = 0;
	while (!fDone)
	{
		ListCell *plcLocation = NULL;
		ForEach (plcLocation, extentry->locations)
		{
			Value* pvLocation = (Value *)lfirst(plcLocation);
			CHAR *szUri = pvLocation->val.str;
			plModifiedLocations = gpdb::PlAppendElement(plModifiedLocations, gpdb::PvalMakeString(szUri));
			ulModifiedLocations ++;

			if (ulModifiedLocations == ulParticipatingSegments)
			{
				fDone = true;
				break;
			}

			if (ulModifiedLocations > ulParticipatingSegments)
			{
				GPOS_RAISE(gpdxl::ExmaDXL, gpdxl::ExmiDXL2PlStmtExternalScanError,
						GPOS_WSZ_LIT("External scan location list failed building distribution"));
			}
		}
	}

	BOOL *rgfSkipMap = NULL;
	if (fSkipRandomly)
	{
		rgfSkipMap = gpdb::RgfRandomSegMap(ulTotalPrimaries, ulSkip);
	}

	CHAR **rgszSegWorkMap = NULL;
	if (fPxfProtocol)
	{
		Relation rel = gpdb::RelGetRelation(CMDIdGPDB::PmdidConvert(pmdidRel)->OidObjectId());

		if (NULL == rel)
		{
			GPOS_RAISE(gpdxl::ExmaMD, gpdxl::ExmiMDCacheEntryNotFound, pmdidRel->Wsz());
		}

		GPOS_TRY
		{
			Value *pvUri = (Value *) gpdb::PvListNth(extentry->locations, 0);
			CHAR * szUri = pvUri->val.str;
			rgszSegWorkMap = gpdb::RgszMapHdDataToSegments(szUri, ulTotalPrimaries, ulMaxParticipants, rel, plQuals);

			GPOS_ASSERT(NULL != rgszSegWorkMap);
			gpdb::CloseRelation(rel);
		}
		GPOS_CATCH_EX(ex)
		{
			gpdb::CloseRelation(rel);
			GPOS_RETHROW(ex);
		}
		GPOS_CATCH_END;
	}

	// assign each URI from the new location list a primary segdb
	ListCell *plc = NULL;
	ForEach (plc, plModifiedLocations)
	{
		const CHAR *szUri = (CHAR *) strVal(lfirst(plc));

		BOOL fCandidateFound = false;
		BOOL fMatchFound = false;

		ListCell *lcSegment;
		ForEach(lcSegment, segments)
		{
		  Segment *segment = (Segment *)lfirst(lcSegment);
		  INT iSegInd = segment->segindex;
		  if (!segment->alive)
		  {
		    continue;
		  }

		  if (fSkipRandomly)
		  {
        GPOS_ASSERT(iSegInd < ulTotalPrimaries);
        if (rgfSkipMap[iSegInd])
        {
          continue;
        }
		  }

      fCandidateFound = true;
      if (NULL == rgszSegFileMap[iSegInd])
      {
        if (!fPxfProtocol)
        {
          rgszSegFileMap[iSegInd] = PStrDup(szUri);
          fMatchFound = true;
          break;
        }
        else if (NULL != rgszSegWorkMap[iSegInd])
        {
          CHAR cDelim = '?';
          StringInfoData seg_uri;

          gpdb::InitStringInfoOfSize(&seg_uri, 512 /*size*/);
          gpdb::AppendStringInfoString(&seg_uri, szUri);
          // if the segwork string is the first option we delimit it with '?' otherwise with '&'
          if (NULL != gpos::clib::SzStrChr(szUri, '?'))
          {
            cDelim = '&';
          }
          gpdb::AppendStringInfoChar(&seg_uri, cDelim);
          gpdb::AppendStringInfoString(&seg_uri, rgszSegWorkMap[iSegInd]);
          rgszSegFileMap[iSegInd] = seg_uri.data;
          fMatchFound = true;
         }
       }
		 }

		 if (!fMatchFound && !fPxfProtocol)
		 {
		    GPOS_RAISE(gpdxl::ExmaDXL, gpdxl::ExmiDXL2PlStmtExternalScanError,
		         GPOS_WSZ_LIT("Unable to assign segments for gpfdist(s)"));
		 }
	}

	if (fPxfProtocol)
	{
		gpdb::FreeHdDataToSegmentsMapping(rgszSegWorkMap, ulTotalPrimaries);
	}
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorDXLToPlStmt::MapLocationsExecute
//
//	@doc:
// 		Segment mapping for tables with EXECUTE 'cmd' ON.
//		In here we don't have URI's. We have a single command string and a
//		specification of the segdb granularity it should get executed on (the
//		ON clause). Depending on the ON clause specification we could go many
//		different ways, for example: assign the command to all segdb, or one
//		command per host, or assign to 5 random segments, etc...
//
//---------------------------------------------------------------------------
void
CTranslatorDXLToPlStmt::MapLocationsExecute
	(
	OID oidRel,
	char **rgszSegFileMap,
	List *segments,
	const ULONG ulTotalPrimaries
	)
{
	ExtTableEntry *extentry = gpdb::Pexttable(oidRel);
	CHAR *szCommand = extentry->command;
	const CHAR *szPrefix = "execute:";
	
	StringInfo si = gpdb::SiMakeStringInfo();
	gpdb::AppendStringInfo(si, szPrefix, szCommand);
	CHAR *szPrefixedCommand = PStrDup(si->data);

	gpdb::GPDBFree(si->data);
	gpdb::GPDBFree(si);
	si = NULL;

	// get the ON clause (execute location) information
	Value *pvOnClause = (Value *) gpdb::PvListNth(extentry->locations, 0);
	CHAR *szOnClause = pvOnClause->val.str;
	
	if (0 == gpos::clib::IStrCmp(szOnClause, "ALL_SEGMENTS"))
	{
		MapLocationsExecuteAllSegments(szPrefixedCommand, rgszSegFileMap, segments);
	}
	else if (0 == gpos::clib::IStrCmp(szOnClause, "PER_HOST"))
	{
		MapLocationsExecutePerHost(szPrefixedCommand, rgszSegFileMap, segments);
	}
	else if (0 == gpos::clib::IStrNCmp(szOnClause, "HOST:", gpos::clib::UlStrLen("HOST:")))
	{
		CHAR *szHostName = szOnClause + gpos::clib::UlStrLen("HOST:");
		MapLocationsExecuteOneHost(szHostName, szPrefixedCommand, rgszSegFileMap, segments);
	}
	else if (0 == gpos::clib::IStrNCmp(szOnClause, "SEGMENT_ID:", gpos::clib::UlStrLen("SEGMENT_ID:")))
	{
		CHAR *pcEnd = NULL;
		INT iTargetSegInd = (INT) gpos::clib::LStrToL(szOnClause + gpos::clib::UlStrLen("SEGMENT_ID:"), &pcEnd, 10);
		MapLocationsExecuteOneSegment(iTargetSegInd, szPrefixedCommand, rgszSegFileMap, segments);
	}
	else if (0 == gpos::clib::IStrNCmp(szOnClause, "TOTAL_SEGS:", gpos::clib::UlStrLen("TOTAL_SEGS:")))
	{
		// total n segments selected randomly
		CHAR *pcEnd = NULL;
		ULONG ulSegsToUse = gpos::clib::LStrToL(szOnClause + gpos::clib::UlStrLen("TOTAL_SEGS:"), &pcEnd, 10);

		MapLocationsExecuteRandomSegments(ulSegsToUse, ulTotalPrimaries, szPrefixedCommand, rgszSegFileMap, segments);
	}
	else if (0 == gpos::clib::IStrCmp(szOnClause, "MASTER_ONLY"))
	{
		rgszSegFileMap[0] = PStrDup(szPrefixedCommand);
	}
	else
	{
		GPOS_RAISE(gpdxl::ExmaDXL, gpdxl::ExmiDXL2PlStmtExternalScanError,
				GPOS_WSZ_LIT("Invalid ON clause"));
	}

	gpdb::GPDBFree(szPrefixedCommand);
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorDXLToPlStmt::MapLocationsExecuteAllSegments
//
//	@doc:
// 		Segment mapping for tables with EXECUTE 'cmd' on all segments
//
//---------------------------------------------------------------------------
void
CTranslatorDXLToPlStmt::MapLocationsExecuteAllSegments
	(
	CHAR *szPrefixedCommand,
	char **rgszSegFileMap,
	List *segments
	)
{
  ListCell *lcSegment;
  ForEach(lcSegment, segments)
  {
    Segment *segment = (Segment *)lfirst(lcSegment);
    INT iSegInd = segment->segindex;
    if (segment->alive)
    {
      rgszSegFileMap[iSegInd] = PStrDup(szPrefixedCommand);
    }
  }
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorDXLToPlStmt::MapLocationsExecutePerHost
//
//	@doc:
// 		Segment mapping for tables with EXECUTE 'cmd' per host
//
//---------------------------------------------------------------------------
void
CTranslatorDXLToPlStmt::MapLocationsExecutePerHost
	(
	CHAR *szPrefixedCommand,
	char **rgszSegFileMap,
	List *segments
	)
{
	List *plVisitedHosts = NIL;
	ListCell *lcSegment;
	ForEach(lcSegment, segments)
	{
	  Segment *segment = (Segment *)lfirst(lcSegment);
	  INT iSegInd = segment->segindex;
	  if (segment->alive)
	  {
      BOOL fHostTaken = false;
      ListCell *plc = NULL;
      ForEach (plc, plVisitedHosts)
      {
        const CHAR *szHostName = (CHAR *) strVal(lfirst(plc));
        if (0 == gpdb::IStrCmpIgnoreCase(szHostName, segment->hostname))
        {
          fHostTaken = true;
          break;
        }
      }

      if (!fHostTaken)
      {
        rgszSegFileMap[iSegInd] = PStrDup(szPrefixedCommand);
        plVisitedHosts = gpdb::PlAppendElement
                    (
                    plVisitedHosts,
                    gpdb::PvalMakeString(PStrDup(segment->hostname))
                    );
      }
	  }
	}
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorDXLToPlStmt::MapLocationsExecuteOneHost
//
//	@doc:
// 		Segment mapping for tables with EXECUTE 'cmd' on a given host
//
//---------------------------------------------------------------------------
void
CTranslatorDXLToPlStmt::MapLocationsExecuteOneHost
	(
	CHAR *szHostName,
	CHAR *szPrefixedCommand,
	char **rgszSegFileMap,
	List *segments
	)
{
	BOOL fMatchFound = false;
	ListCell *lcSegment;
	ForEach(lcSegment, segments)
	{
	  Segment *segment = (Segment *)lfirst(lcSegment);
	  INT iSegInd = segment->segindex;
	  if (segment->alive &&
	      (0 == gpdb::IStrCmpIgnoreCase(szHostName, segment->hostname)))
	  {
	    rgszSegFileMap[iSegInd] = PStrDup(szPrefixedCommand);
	    fMatchFound = true;
	  }
	}

	if (!fMatchFound)
	{
		GPOS_RAISE(gpdxl::ExmaDXL, gpdxl::ExmiDXL2PlStmtExternalScanError,
				GPOS_WSZ_LIT("Could not assign a segment database for given command. No valid primary segment was found in the requested host name."));
	}
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorDXLToPlStmt::MapLocationsExecuteOneSegment
//
//	@doc:
// 		Segment mapping for tables with EXECUTE 'cmd' on a given segment
//
//---------------------------------------------------------------------------
void
CTranslatorDXLToPlStmt::MapLocationsExecuteOneSegment
	(
	INT iTargetSegInd,
	CHAR *szPrefixedCommand,
	char **rgszSegFileMap,
	List *segments
	)
{
	BOOL fMatchFound = false;
	ListCell *lcSegment;
	ForEach(lcSegment, segments)
	{
	  Segment *segment = (Segment *)lfirst(lcSegment);
	  INT iSegInd = segment->segindex;
	  if (segment->alive &&
	      (iSegInd == iTargetSegInd))
	  {
      rgszSegFileMap[iSegInd] = PStrDup(szPrefixedCommand);
      fMatchFound = true;
	  }
	}

	if(!fMatchFound)
	{
		GPOS_RAISE(gpdxl::ExmaDXL, gpdxl::ExmiDXL2PlStmtExternalScanError,
				GPOS_WSZ_LIT("Could not assign a segment database for given command. The requested segment id is not a valid primary segment."));
	}
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorDXLToPlStmt::MapLocationsExecuteRandomSegments
//
//	@doc:
// 		Segment mapping for tables with EXECUTE 'cmd' on N random segments
//
//---------------------------------------------------------------------------
void
CTranslatorDXLToPlStmt::MapLocationsExecuteRandomSegments
	(
	ULONG ulSegments,
	const ULONG ulTotalPrimaries,
	CHAR *szPrefixedCommand,
	char **rgszSegFileMap,
	List *segments
	)
{
	if (ulSegments > ulTotalPrimaries)
	{
		GPOS_RAISE(gpdxl::ExmaDXL, gpdxl::ExmiDXL2PlStmtExternalScanError,
				GPOS_WSZ_LIT("More segments in table definition than valid primary segments in the database."));
	}

	ULONG ulSkip = ulTotalPrimaries - ulSegments;
	BOOL *rgfSkipMap = gpdb::RgfRandomSegMap(ulTotalPrimaries, ulSkip);
	ListCell *lcSegment;
	ForEach(lcSegment, segments)
	{
	  Segment *segment = (Segment *)lfirst(lcSegment);
	  INT iSegInd = segment->segindex;
	  if (segment->alive)
	  {
      GPOS_ASSERT(iSegInd < ulTotalPrimaries);
      if (rgfSkipMap[iSegInd])
      {
        continue;
      }
      rgszSegFileMap[iSegInd] = PStrDup(szPrefixedCommand);
	  }
	}
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorDXLToPlStmt::PlExternalScanUriList
//
//	@doc:
//		List of URIs for external scan
//
//---------------------------------------------------------------------------
List*
CTranslatorDXLToPlStmt::PlExternalScanUriList
	(
	OID oidRel,
	IMDId *pmdidRel,
	List *plQuals
	)
{
	ExtTableEntry *extentry = gpdb::Pexttable(oidRel);
	
	if (extentry->iswritable)
	{
		GPOS_RAISE(gpdxl::ExmaDXL, gpdxl::ExmiDXL2PlStmtExternalScanError,
				GPOS_WSZ_LIT("It is not possible to read from a WRITABLE external table. Create the table as READABLE instead."));
	}

	//get the total valid primary segdb count
	List *segments = gpdb::PcdbComponentDatabases();
	ListCell *lc;
  ULONG ulTotalPrimaries = 0;
	ForEach(lc, segments)
	{
	  Segment *segment = (Segment *)lfirst(lc);
	  if (segment->alive)
	  {
	    ulTotalPrimaries++;
	  }
	}

	char **rgszSegFileMap = NULL;
	if (ulTotalPrimaries > 0)
	{
    rgszSegFileMap = (char **) gpdb::GPDBAlloc(ulTotalPrimaries * sizeof(char *));
    gpos::clib::PvMemSet(rgszSegFileMap, 0, ulTotalPrimaries * sizeof(char *));
	}

	// is this an EXECUTE table or a LOCATION (URI) table
	BOOL fUsingExecute = false;
	BOOL fUsingLocation = false;
	const CHAR *szCommand = extentry->command;
	if (NULL != szCommand)
	{
		if (!gp_external_enable_exec)
		{
			GPOS_RAISE(gpdxl::ExmaDXL, gpdxl::ExmiDXL2PlStmtExternalScanError,
					GPOS_WSZ_LIT("Using external tables with OS level commands is disabled. Use (set gp_external_enable_exec = on) to enable"));
		}
		fUsingExecute = true;
	}
	else
	{
		fUsingLocation = true;
	}

	GPOS_ASSERT(0 < gpdb::UlListLength(extentry->locations));
	
	CHAR *szFirstUri = NULL;
	Uri *pUri = NULL;
	if (!fUsingExecute)
	{
		szFirstUri = ((Value *) gpdb::PvListNth(extentry->locations, 0))->val.str;
		pUri = gpdb::PuriParseExternalTable(szFirstUri);
	}

	if (fUsingLocation && (URI_FILE == pUri->protocol || URI_HTTP == pUri->protocol))
	{
	  if (ulTotalPrimaries > 0)
	  {
	    MapLocationsFile(oidRel, rgszSegFileMap, segments);
	  }
	}
	else if (fUsingLocation && (URI_GPFDIST == pUri->protocol || URI_GPFDISTS == pUri->protocol || URI_CUSTOM == pUri->protocol))
	{
	  if (ulTotalPrimaries > 0)
	  {
	    MapLocationsFdist(oidRel, rgszSegFileMap, segments, pUri, ulTotalPrimaries, pmdidRel, plQuals);
	  }
	}
	else if (fUsingExecute)
	{
	  if (ulTotalPrimaries > 0)
	  {
	    MapLocationsExecute(oidRel, rgszSegFileMap, segments, ulTotalPrimaries);
	  }
	}
	else
	{
		GPOS_RAISE(gpdxl::ExmaDXL, gpdxl::ExmiDXL2PlStmtExternalScanError, GPOS_WSZ_LIT("Unsupported protocol and/or file location"));
	}

    // convert array map to a list so it can be serialized as part of the plan
	List *plFileNames = NIL;
	for (ULONG ul = 0; ul < ulTotalPrimaries; ul++)
	{
		Value *pval = NULL;
	    if (NULL != rgszSegFileMap[ul])
	    {
	    	pval = gpdb::PvalMakeString(rgszSegFileMap[ul]);
	    }
	    else
		{
			// no file for this segdb. add a null entry
			pval = MakeNode(Value);
			pval->type = T_Null;
		}
		plFileNames = gpdb::PlAppendElement(plFileNames, pval);
	}

	return plFileNames;
}


//---------------------------------------------------------------------------
//	@function:
//		CTranslatorDXLToPlStmt::PtsFromDXLTblScan
//
//	@doc:
//		Translates a DXL table scan node into a TableScan node
//
//---------------------------------------------------------------------------
Plan *
CTranslatorDXLToPlStmt::PtsFromDXLTblScan
	(
	const CDXLNode *pdxlnTblScan,
	CDXLTranslateContext *pdxltrctxOut,
	Plan *pplanParent,
	DrgPdxltrctx *pdrgpdxltrctxPrevSiblings
	)
{
	// translate table descriptor into a range table entry
	CDXLPhysicalTableScan *pdxlopTS = CDXLPhysicalTableScan::PdxlopConvert(pdxlnTblScan->Pdxlop());

	// translation context for column mappings in the base relation
	CDXLTranslateContextBaseTable dxltrctxbt(m_pmp);

	// we will add the new range table entry as the last element of the range table
	Index iRel = gpdb::UlListLength(m_pctxdxltoplstmt->PlPrte()) + 1;

	const CDXLTableDescr *pdxltabdesc = pdxlopTS->Pdxltabdesc();
	
	IMDId *pmdidRel = pdxltabdesc->Pmdid();
	const IMDRelation *pmdrel = m_pmda->Pmdrel(pmdidRel);
	const ULONG ulRelCols = pmdrel->UlColumns() - pmdrel->UlSystemColumns();
	RangeTblEntry *prte = PrteFromTblDescr(pdxltabdesc, NULL /*pdxlid*/, ulRelCols, iRel, &dxltrctxbt);
	GPOS_ASSERT(NULL != prte);
	prte->requiredPerms |= ACL_SELECT;
	m_pctxdxltoplstmt->AddRTE(prte);

	Plan *pplan = NULL;
	Plan *pplanReturn = NULL;
	if (IMDRelation::ErelstorageExternal == pmdrel->Erelstorage())
	{
		const IMDRelationExternal *pmdrelext = dynamic_cast<const IMDRelationExternal*>(pmdrel);
		OID oidRel = CMDIdGPDB::PmdidConvert(pmdrel->Pmdid())->OidObjectId();
		ExtTableEntry *pextentry = gpdb::Pexttable(oidRel);
		
		// create external scan node
		ExternalScan *pes = MakeNode(ExternalScan);
		pplan = &(pes->scan.plan);
		// translate filter
		CDXLNode *pdxlnFilter = (*pdxlnTblScan)[EdxltsIndexFilter];
		pplan->qual = PlQualFromFilter(pdxlnFilter, &dxltrctxbt, NULL /*pdrgpdxltrctx*/, pdxltrctxOut, pplan);

		pes->scan.scanrelid = iRel;
		pes->uriList = PlExternalScanUriList(oidRel, pmdidRel, pplan->qual);
		Value *pval = gpdb::PvalMakeString(pextentry->fmtopts);
		pes->fmtOpts = ListMake1(pval);
		pes->fmtType = pextentry->fmtcode;
		pes->isMasterOnly = (IMDRelation::EreldistrMasterOnly == pmdrelext->Ereldistribution());
		pes->rejLimit = pmdrelext->IRejectLimit();
		pes->rejLimitInRows = pmdrelext->FRejLimitInRows();

		IMDId *pmdidFmtErrTbl = pmdrelext->PmdidFmtErrRel();
		if (NULL == pmdidFmtErrTbl)
		{
			pes->fmterrtbl = InvalidOid;
		}
		else
		{
			pes->fmterrtbl = CMDIdGPDB::PmdidConvert(pmdidFmtErrTbl)->OidObjectId();
		}

		pes->encoding = pextentry->encoding;
		pes->scancounter = m_ulExternalScanCounter++;

		pplanReturn = (Plan *) pes;
	}
	else
	{
		// create table scan node
		TableScan *pts = MakeNode(TableScan);
		pts->scanrelid = iRel;
		pplan = &(pts->plan);
		pplanReturn = (Plan *) pts;
		// translate filter
		CDXLNode *pdxlnFilter = (*pdxlnTblScan)[EdxltsIndexFilter];
		pplan->qual = PlQualFromFilter(pdxlnFilter, &dxltrctxbt, NULL /*pdrgpdxltrctx*/, pdxltrctxOut, pplan);
	}

	pplan->plan_node_id = m_pctxdxltoplstmt->UlNextPlanId();
	pplan->plan_parent_node_id = IPlanId(pplanParent);
	pplan->nMotionNodes = 0;

	// translate operator costs
	TranslatePlanCosts
		(
		CDXLPhysicalProperties::PdxlpropConvert(pdxlnTblScan->Pdxlprop())->Pdxlopcost(),
		&(pplan->startup_cost),
		&(pplan->total_cost),
		&(pplan->plan_rows),
		&(pplan->plan_width)
		);

	// a table scan node must have 2 children: projection list and filter
	GPOS_ASSERT(2 == pdxlnTblScan->UlArity());

	// translate proj list
	CDXLNode *pdxlnPrL = (*pdxlnTblScan)[EdxltsIndexProjList];
	pplan->targetlist = PlTargetListFromProjList(pdxlnPrL, &dxltrctxbt, NULL /*pdrgpdxltrctx*/, pdxltrctxOut, pplan);

	SetParamIds(pplan);

	return pplanReturn;
}


//---------------------------------------------------------------------------
//	@function:
//		CTranslatorDXLToPlStmt::FSetIndexVarAttno
//
//	@doc:
//		Walker to set index var attno's,
//		attnos of index vars are set to their relative positions in index keys,
//		skip any outer references while walking the expression tree
//
//---------------------------------------------------------------------------
BOOL
CTranslatorDXLToPlStmt::FSetIndexVarAttno
	(
	Node *pnode,
	SContextIndexVarAttno *pctxtidxvarattno
	)
{
	if (NULL == pnode)
	{
		return false;
	}

	if (IsA(pnode, Var) && ((Var *)pnode)->varno != OUTER)
	{
		INT iAttno = ((Var *)pnode)->varattno;
		const IMDRelation *pmdrel = pctxtidxvarattno->m_pmdrel;
		const IMDIndex *pmdindex = pctxtidxvarattno->m_pmdindex;

		ULONG ulIndexColPos = ULONG_MAX;
		const ULONG ulArity = pmdrel->UlColumns();
		for (ULONG ulColPos = 0; ulColPos < ulArity; ulColPos++)
		{
			const IMDColumn *pmdcol = pmdrel->Pmdcol(ulColPos);
			if (iAttno == pmdcol->IAttno())
			{
				ulIndexColPos = ulColPos;
				break;
			}
		}

		if (ULONG_MAX > ulIndexColPos)
		{
			((Var *)pnode)->varattno =  1 + pmdindex->UlPosInKey(ulIndexColPos);
		}

		return false;
	}

	return gpdb::FWalkExpressionTree
			(
			pnode,
			(BOOL (*)()) CTranslatorDXLToPlStmt::FSetIndexVarAttno,
			pctxtidxvarattno
			);
}


//---------------------------------------------------------------------------
//	@function:
//		CTranslatorDXLToPlStmt::PisFromDXLIndexScan
//
//	@doc:
//		Translates a DXL index scan node into a IndexScan node
//
//---------------------------------------------------------------------------
Plan *
CTranslatorDXLToPlStmt::PisFromDXLIndexScan
	(
	const CDXLNode *pdxlnIndexScan,
	CDXLTranslateContext *pdxltrctxOut,
	Plan *pplanParent,
	DrgPdxltrctx *pdrgpdxltrctxPrevSiblings
	)
{
	// translate table descriptor into a range table entry
	CDXLPhysicalIndexScan *pdxlopIndexScan = CDXLPhysicalIndexScan::PdxlopConvert(pdxlnIndexScan->Pdxlop());

	return PisFromDXLIndexScan(pdxlnIndexScan, pdxlopIndexScan, pdxltrctxOut, pplanParent, false /*fIndexOnlyScan*/, pdrgpdxltrctxPrevSiblings);
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorDXLToPlStmt::PisFromDXLIndexScan
//
//	@doc:
//		Translates a DXL index scan node into a IndexScan node
//
//---------------------------------------------------------------------------
Plan *
CTranslatorDXLToPlStmt::PisFromDXLIndexScan
	(
	const CDXLNode *pdxlnIndexScan,
	CDXLPhysicalIndexScan *pdxlopIndexScan,
	CDXLTranslateContext *pdxltrctxOut,
	Plan *pplanParent,
	BOOL fIndexOnlyScan,
	DrgPdxltrctx *pdrgpdxltrctxPrevSiblings
	)
{
	// translation context for column mappings in the base relation
	CDXLTranslateContextBaseTable dxltrctxbt(m_pmp);

	Index iRel = gpdb::UlListLength(m_pctxdxltoplstmt->PlPrte()) + 1;

	const CDXLIndexDescr *pdxlid = NULL;
	if (fIndexOnlyScan)
	{
		pdxlid = pdxlopIndexScan->Pdxlid();
	}

	const IMDRelation *pmdrel = m_pmda->Pmdrel(pdxlopIndexScan->Pdxltabdesc()->Pmdid());
	const ULONG ulRelCols = pmdrel->UlColumns() - pmdrel->UlSystemColumns();
	RangeTblEntry *prte = PrteFromTblDescr(pdxlopIndexScan->Pdxltabdesc(), pdxlid, ulRelCols, iRel, &dxltrctxbt);
	GPOS_ASSERT(NULL != prte);
	prte->requiredPerms |= ACL_SELECT;
	m_pctxdxltoplstmt->AddRTE(prte);

	IndexScan *pis = NULL;
	GPOS_ASSERT(!fIndexOnlyScan);
	pis = MakeNode(IndexScan);
	pis->scan.scanrelid = iRel;

	CMDIdGPDB *pmdidIndex = CMDIdGPDB::PmdidConvert(pdxlopIndexScan->Pdxlid()->Pmdid());
	const IMDIndex *pmdindex = m_pmda->Pmdindex(pmdidIndex);
	Oid oidIndex = pmdidIndex->OidObjectId();

	GPOS_ASSERT(InvalidOid != oidIndex);
	pis->indexid = oidIndex;

	Plan *pplan = &(pis->scan.plan);
	pplan->plan_node_id = m_pctxdxltoplstmt->UlNextPlanId();
	pplan->plan_parent_node_id = IPlanId(pplanParent);
	pplan->nMotionNodes = 0;

	// translate operator costs
	TranslatePlanCosts
		(
		CDXLPhysicalProperties::PdxlpropConvert(pdxlnIndexScan->Pdxlprop())->Pdxlopcost(),
		&(pplan->startup_cost),
		&(pplan->total_cost),
		&(pplan->plan_rows),
		&(pplan->plan_width)
		);

	// an index scan node must have 3 children: projection list, filter and index condition list
	GPOS_ASSERT(3 == pdxlnIndexScan->UlArity());

	// translate proj list and filter
	CDXLNode *pdxlnPrL = (*pdxlnIndexScan)[EdxlisIndexProjList];
	CDXLNode *pdxlnFilter = (*pdxlnIndexScan)[EdxlisIndexFilter];
	CDXLNode *pdxlnIndexCondList = (*pdxlnIndexScan)[EdxlisIndexCondition];

	// translate proj list
	pplan->targetlist = PlTargetListFromProjList(pdxlnPrL, &dxltrctxbt, NULL /*pdrgpdxltrctx*/, pdxltrctxOut, pplan);

	// translate index filter
	pplan->qual = PlTranslateIndexFilter
					(
					pdxlnFilter,
					pdxltrctxOut,
					&dxltrctxbt,
					pdrgpdxltrctxPrevSiblings,
					pplan
					);

	pis->indexorderdir = CTranslatorUtils::Scandirection(pdxlopIndexScan->EdxlScanDirection());

	// translate index condition list
	List *plIndexConditions = NIL;
	List *plIndexOrigConditions = NIL;
	List *plIndexStratgey = NIL;
	List *plIndexSubtype = NIL;

	TranslateIndexConditions
		(
		pdxlnIndexCondList, 
		pdxlopIndexScan->Pdxltabdesc(), 
		fIndexOnlyScan, 
		pmdindex, 
		pmdrel,
		pdxltrctxOut,
		&dxltrctxbt, 
		pdrgpdxltrctxPrevSiblings,
		pplan,
		&plIndexConditions, 
		&plIndexOrigConditions, 
		&plIndexStratgey, 
		&plIndexSubtype
		);

	pis->indexqual = plIndexConditions;
	pis->indexqualorig = plIndexOrigConditions;
	pis->indexstrategy = plIndexStratgey;
	pis->indexsubtype = plIndexSubtype;
	SetParamIds(pplan);

	return (Plan *) pis;
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorDXLToPlStmt::TranslateIndexFilter
//
//	@doc:
//		Translate the index filter list in an Index scan
//
//---------------------------------------------------------------------------
List *
CTranslatorDXLToPlStmt::PlTranslateIndexFilter
	(
	CDXLNode *pdxlnFilter,
	CDXLTranslateContext *pdxltrctxOut,
	CDXLTranslateContextBaseTable *pdxltrctxbt,
	DrgPdxltrctx *pdrgpdxltrctxPrevSiblings,
	Plan *pplanParent
	)
{
	List *plQuals = NIL;

	// build colid->var mapping
	CMappingColIdVarPlStmt mapcidvarplstmt(m_pmp, pdxltrctxbt, pdrgpdxltrctxPrevSiblings, pdxltrctxOut, m_pctxdxltoplstmt, pplanParent);

	const ULONG ulArity = pdxlnFilter->UlArity();
	for (ULONG ul = 0; ul < ulArity; ul++)
	{
		CDXLNode *pdxlnIndexFilter = (*pdxlnFilter)[ul];
		Expr *pexprIndexFilter = m_pdxlsctranslator->PexprFromDXLNodeScalar(pdxlnIndexFilter, &mapcidvarplstmt);
		plQuals = gpdb::PlAppendElement(plQuals, pexprIndexFilter);
	}

	return plQuals;
}


//---------------------------------------------------------------------------
//	@function:
//		CTranslatorDXLToPlStmt::TranslateIndexConditions
//
//	@doc:
//		Translate the index condition list in an Index scan
//
//---------------------------------------------------------------------------
void 
CTranslatorDXLToPlStmt::TranslateIndexConditions
	(
	CDXLNode *pdxlnIndexCondList,
	const CDXLTableDescr *pdxltd,
	BOOL fIndexOnlyScan,
	const IMDIndex *pmdindex,
	const IMDRelation *pmdrel,
	CDXLTranslateContext *pdxltrctxOut,
	CDXLTranslateContextBaseTable *pdxltrctxbt,
	DrgPdxltrctx *pdrgpdxltrctxPrevSiblings,
	Plan *pplanParent,
	List **pplIndexConditions,
	List **pplIndexOrigConditions,
	List **pplIndexStratgey,
	List **pplIndexSubtype
	)
{
	// array of index qual info
	DrgPindexqualinfo *pdrgpindexqualinfo = GPOS_NEW(m_pmp) DrgPindexqualinfo(m_pmp);

	// build colid->var mapping
	CMappingColIdVarPlStmt mapcidvarplstmt(m_pmp, pdxltrctxbt, pdrgpdxltrctxPrevSiblings, pdxltrctxOut, m_pctxdxltoplstmt, pplanParent);

	const ULONG ulArity = pdxlnIndexCondList->UlArity();
	for (ULONG ul = 0; ul < ulArity; ul++)
	{
		CDXLNode *pdxlnIndexCond = (*pdxlnIndexCondList)[ul];

		Expr *pexprOrigIndexCond = m_pdxlsctranslator->PexprFromDXLNodeScalar(pdxlnIndexCond, &mapcidvarplstmt);
		Expr *pexprIndexCond = m_pdxlsctranslator->PexprFromDXLNodeScalar(pdxlnIndexCond, &mapcidvarplstmt);
		GPOS_ASSERT(IsA(pexprIndexCond, OpExpr) && "expected OpExpr in index qual");

		// for indexonlyscan, we already have the attno referring to the index
		if (!fIndexOnlyScan)
		{
			// Otherwise, we need to perform mapping of Varattnos relative to column positions in index keys
			SContextIndexVarAttno ctxtidxvarattno(pmdrel, pmdindex);
			FSetIndexVarAttno((Node *) pexprIndexCond, &ctxtidxvarattno);
		}
		
		// find index key's attno
		List *plistArgs = ((OpExpr *) pexprIndexCond)->args;
		Node *pnodeFst = (Node *) lfirst(gpdb::PlcListHead(plistArgs));
		Node *pnodeSnd = (Node *) lfirst(gpdb::PlcListTail(plistArgs));
		GPOS_ASSERT(IsA(pnodeFst, Var) || IsA(pnodeSnd, Var) && "expected index key in index qual");

		INT iAttno = 0;
		if (IsA(pnodeFst, Var) && ((Var *) pnodeFst)->varno != OUTER)
		{
			// index key is on the left side
			iAttno =  ((Var *) pnodeFst)->varattno;
		}
		else
		{
			// index key is on the right side
			GPOS_ASSERT(((Var *) pnodeSnd)->varno != OUTER && "unexpected outer reference in index qual");
			iAttno = ((Var *) pnodeSnd)->varattno;
		}
		
		// retrieve index strategy and subtype
		INT iSN = 0;
		OID oidIndexSubtype = InvalidOid;
		BOOL fRecheck = false;
		
		OID oidCmpOperator = CTranslatorUtils::OidCmpOperator(pexprIndexCond);
		GPOS_ASSERT(InvalidOid != oidCmpOperator);
		OID oidOpclass = CTranslatorUtils::OidIndexQualOpclass(iAttno, CMDIdGPDB::PmdidConvert(pmdindex->Pmdid())->OidObjectId());
		GPOS_ASSERT(InvalidOid != oidOpclass);
		gpdb::IndexOpProperties(oidCmpOperator, oidOpclass, &iSN, &oidIndexSubtype, &fRecheck);
		GPOS_ASSERT(!fRecheck);
		
		// create index qual
		pdrgpindexqualinfo->Append(GPOS_NEW(m_pmp) CIndexQualInfo(iAttno, (OpExpr *)pexprIndexCond, (OpExpr *)pexprOrigIndexCond, (StrategyNumber) iSN, oidIndexSubtype));
	}

	// the index quals much be ordered by attribute number
	pdrgpindexqualinfo->Sort(CIndexQualInfo::IIndexQualInfoCmp);

	ULONG ulLen = pdrgpindexqualinfo->UlLength();
	for (ULONG ul = 0; ul < ulLen; ul++)
	{
		CIndexQualInfo *pindexqualinfo = (*pdrgpindexqualinfo)[ul];
		*pplIndexConditions = gpdb::PlAppendElement(*pplIndexConditions, pindexqualinfo->m_popExpr);
		*pplIndexOrigConditions = gpdb::PlAppendElement(*pplIndexOrigConditions, pindexqualinfo->m_popOriginalExpr);
		*pplIndexStratgey = gpdb::PlAppendInt(*pplIndexStratgey, pindexqualinfo->m_sn);
		*pplIndexSubtype = gpdb::PlAppendOid(*pplIndexSubtype, pindexqualinfo->m_oidIndexSubtype);
	}

	// clean up
	pdrgpindexqualinfo->Release();
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorDXLToPlStmt::PlTranslateAssertConstraints
//
//	@doc:
//		Translate the constraints from an Assert node into a list of quals
//
//---------------------------------------------------------------------------
List *
CTranslatorDXLToPlStmt::PlTranslateAssertConstraints
	(
	CDXLNode *pdxlnAssertConstraintList,
	CDXLTranslateContext *pdxltrctxOut,
	DrgPdxltrctx *pdrgpdxltrctx,
	Plan *pplanParent
	)
{
	List *plQuals = NIL;

	// build colid->var mapping
	CMappingColIdVarPlStmt mapcidvarplstmt(m_pmp, NULL /*pdxltrctxbt*/, pdrgpdxltrctx, pdxltrctxOut, m_pctxdxltoplstmt, pplanParent);

	const ULONG ulArity = pdxlnAssertConstraintList->UlArity();
	for (ULONG ul = 0; ul < ulArity; ul++)
	{
		CDXLNode *pdxlnpdxlnAssertConstraint = (*pdxlnAssertConstraintList)[ul];
		Expr *pexprAssertConstraint = m_pdxlsctranslator->PexprFromDXLNodeScalar((*pdxlnpdxlnAssertConstraint)[0], &mapcidvarplstmt);
		plQuals = gpdb::PlAppendElement(plQuals, pexprAssertConstraint);
	}

	return plQuals;
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorDXLToPlStmt::PlimitFromDXLLimit
//
//	@doc:
//		Translates a DXL Limit node into a Limit node
//
//---------------------------------------------------------------------------
Plan *
CTranslatorDXLToPlStmt::PlimitFromDXLLimit
	(
	const CDXLNode *pdxlnLimit,
	CDXLTranslateContext *pdxltrctxOut,
	Plan *pplanParent,
	DrgPdxltrctx *pdrgpdxltrctxPrevSiblings
	)
{
	// create limit node
	Limit *plimit = MakeNode(Limit);

	Plan *pplan = &(plimit->plan);
	pplan->plan_node_id = m_pctxdxltoplstmt->UlNextPlanId();
	pplan->plan_parent_node_id = IPlanId(pplanParent);

	// translate operator costs
	TranslatePlanCosts
		(
		CDXLPhysicalProperties::PdxlpropConvert(pdxlnLimit->Pdxlprop())->Pdxlopcost(),
		&(pplan->startup_cost),
		&(pplan->total_cost),
		&(pplan->plan_rows),
		&(pplan->plan_width)
		);

	GPOS_ASSERT(4 == pdxlnLimit->UlArity());

	CDXLTranslateContext dxltrctxLeft(m_pmp, false, pdxltrctxOut->PhmColParam());

	// translate proj list
	CDXLNode *pdxlnPrL = (*pdxlnLimit)[EdxllimitIndexProjList];
	CDXLNode *pdxlnChildPlan = (*pdxlnLimit)[EdxllimitIndexChildPlan];
	CDXLNode *pdxlnLimitCount = (*pdxlnLimit)[EdxllimitIndexLimitCount];
	CDXLNode *pdxlnLimitOffset = (*pdxlnLimit)[EdxllimitIndexLimitOffset];

	// NOTE: Limit node has only the left plan while the right plan is left empty
	Plan *pplanLeft = PplFromDXL(pdxlnChildPlan, &dxltrctxLeft, pplan, pdrgpdxltrctxPrevSiblings);

	DrgPdxltrctx *pdrgpdxltrctx = GPOS_NEW(m_pmp) DrgPdxltrctx(m_pmp);
	pdrgpdxltrctx->Append(&dxltrctxLeft);

	pplan->targetlist = PlTargetListFromProjList
								(
								pdxlnPrL,
								NULL,		// base table translation context
								pdrgpdxltrctx,
								pdxltrctxOut,
								pplan
								);

	pplan->lefttree = pplanLeft;

	if(NULL != pdxlnLimitCount && pdxlnLimitCount->UlArity() >0)
	{
		CMappingColIdVarPlStmt mapcidvarplstmt(m_pmp, NULL, pdrgpdxltrctx, pdxltrctxOut, m_pctxdxltoplstmt, pplan);
		Node *pnodeLimitCount = (Node *) m_pdxlsctranslator->PexprFromDXLNodeScalar((*pdxlnLimitCount)[0], &mapcidvarplstmt);
		plimit->limitCount = pnodeLimitCount;
	}

	if(NULL != pdxlnLimitOffset && pdxlnLimitOffset->UlArity() >0)
	{
		CMappingColIdVarPlStmt mapcidvarplstmt = CMappingColIdVarPlStmt(m_pmp, NULL, pdrgpdxltrctx, pdxltrctxOut, m_pctxdxltoplstmt, pplan);
		Node *pexprLimitOffset = (Node *) m_pdxlsctranslator->PexprFromDXLNodeScalar((*pdxlnLimitOffset)[0], &mapcidvarplstmt);
		plimit->limitOffset = pexprLimitOffset;
	}

	pplan->nMotionNodes = pplanLeft->nMotionNodes;
	SetParamIds(pplan);

	// cleanup
	pdrgpdxltrctx->Release();

	return  (Plan *) plimit;
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorDXLToPlStmt::PhjFromDXLHJ
//
//	@doc:
//		Translates a DXL hash join node into a HashJoin node
//
//---------------------------------------------------------------------------
Plan *
CTranslatorDXLToPlStmt::PhjFromDXLHJ
	(
	const CDXLNode *pdxlnHJ,
	CDXLTranslateContext *pdxltrctxOut,
	Plan *pplanParent,
	DrgPdxltrctx *pdrgpdxltrctxPrevSiblings
	)
{
	GPOS_ASSERT(pdxlnHJ->Pdxlop()->Edxlop() == EdxlopPhysicalHashJoin);
	GPOS_ASSERT(pdxlnHJ->UlArity() == EdxlhjIndexSentinel);

	// create hash join node
	HashJoin *phj = MakeNode(HashJoin);

	Join *pj = &(phj->join);
	Plan *pplan = &(pj->plan);
	pplan->plan_node_id = m_pctxdxltoplstmt->UlNextPlanId();
	pplan->plan_parent_node_id = IPlanId(pplanParent);

	CDXLPhysicalHashJoin *pdxlopHashJoin = CDXLPhysicalHashJoin::PdxlopConvert(pdxlnHJ->Pdxlop());

	// set join type
	pj->jointype = JtFromEdxljt(pdxlopHashJoin->Edxltype());
	pj->prefetch_inner = true;

	// translate operator costs
	TranslatePlanCosts
		(
		CDXLPhysicalProperties::PdxlpropConvert(pdxlnHJ->Pdxlprop())->Pdxlopcost(),
		&(pplan->startup_cost),
		&(pplan->total_cost),
		&(pplan->plan_rows),
		&(pplan->plan_width)
		);

	// translate join children
	CDXLNode *pdxlnLeft = (*pdxlnHJ)[EdxlhjIndexHashLeft];
	CDXLNode *pdxlnRight = (*pdxlnHJ)[EdxlhjIndexHashRight];
	CDXLNode *pdxlnPrL = (*pdxlnHJ)[EdxlhjIndexProjList];
	CDXLNode *pdxlnFilter = (*pdxlnHJ)[EdxlhjIndexFilter];
	CDXLNode *pdxlnJoinFilter = (*pdxlnHJ)[EdxlhjIndexJoinFilter];
	CDXLNode *pdxlnHashCondList = (*pdxlnHJ)[EdxlhjIndexHashCondList];

	CDXLTranslateContext dxltrctxLeft(m_pmp, false, pdxltrctxOut->PhmColParam());
	CDXLTranslateContext dxltrctxRight(m_pmp, false, pdxltrctxOut->PhmColParam());

	Plan *pplanLeft = PplFromDXL(pdxlnLeft, &dxltrctxLeft, pplan, pdrgpdxltrctxPrevSiblings);

	// the right side of the join is the one where the hash phase is done
	DrgPdxltrctx *pdrgpdxltrctxWithSiblings = GPOS_NEW(m_pmp) DrgPdxltrctx(m_pmp);
	pdrgpdxltrctxWithSiblings->Append(&dxltrctxLeft);
	pdrgpdxltrctxWithSiblings->AppendArray(pdrgpdxltrctxPrevSiblings);
	Plan *pplanRight = (Plan*) PhhashFromDXL(pdxlnRight, &dxltrctxRight, pplan, pdrgpdxltrctxWithSiblings);

	DrgPdxltrctx *pdrgpdxltrctx = GPOS_NEW(m_pmp) DrgPdxltrctx(m_pmp);
	pdrgpdxltrctx->Append(const_cast<CDXLTranslateContext*>(&dxltrctxLeft));
	pdrgpdxltrctx->Append(const_cast<CDXLTranslateContext*>(&dxltrctxRight));
	// translate proj list and filter
	TranslateProjListAndFilter
		(
		pdxlnPrL,
		pdxlnFilter,
		NULL,			// translate context for the base table
		pdrgpdxltrctx,
		&pplan->targetlist,
		&pplan->qual,
		pdxltrctxOut,
		pplan
		);

	// translate join filter
	pj->joinqual = PlQualFromFilter
					(
					pdxlnJoinFilter,
					NULL,			// translate context for the base table
					pdrgpdxltrctx,
					pdxltrctxOut,
					pplan
					);

	// translate hash cond
	List *plHashConditions = NIL;

	BOOL fHasINDFCond = false;

	const ULONG ulArity = pdxlnHashCondList->UlArity();
	for (ULONG ul = 0; ul < ulArity; ul++)
	{
		CDXLNode *pdxlnHashCond = (*pdxlnHashCondList)[ul];

		List *plHashCond = PlQualFromScalarCondNode
				(
				pdxlnHashCond,
				NULL,			// base table translation context
				pdrgpdxltrctx,
				pdxltrctxOut,
				pplan
				);

		GPOS_ASSERT(1 == gpdb::UlListLength(plHashCond));

		Expr *pexpr = (Expr *) LInitial(plHashCond);
		if (IsA(pexpr, BoolExpr) && ((BoolExpr *) pexpr)->boolop == NOT_EXPR)
		{
			// INDF test
			GPOS_ASSERT(gpdb::UlListLength(((BoolExpr *) pexpr)->args) == 1 &&
						(IsA((Expr *) LInitial(((BoolExpr *) pexpr)->args), DistinctExpr)));
			fHasINDFCond = true;
		}
		plHashConditions = gpdb::PlConcat(plHashConditions, plHashCond);
	}

	if (!fHasINDFCond)
	{
		// no INDF conditions in the hash condition list
		phj->hashclauses = plHashConditions;
	}
	else
	{
		// hash conditions contain INDF clauses -> extract equality conditions to
		// construct the hash clauses list
		List *plHashClauses = NIL;

		for (ULONG ul = 0; ul < ulArity; ul++)
		{
			CDXLNode *pdxlnHashCond = (*pdxlnHashCondList)[ul];

			// condition can be either a scalar comparison or a NOT DISTINCT FROM expression
			GPOS_ASSERT(EdxlopScalarCmp == pdxlnHashCond->Pdxlop()->Edxlop() ||
						EdxlopScalarBoolExpr == pdxlnHashCond->Pdxlop()->Edxlop());

			if (EdxlopScalarBoolExpr == pdxlnHashCond->Pdxlop()->Edxlop())
			{
				// clause is a NOT DISTINCT FROM check -> extract the distinct comparison node
				GPOS_ASSERT(Edxlnot == CDXLScalarBoolExpr::PdxlopConvert(pdxlnHashCond->Pdxlop())->EdxlBoolType());
				pdxlnHashCond = (*pdxlnHashCond)[0];
				GPOS_ASSERT(EdxlopScalarDistinct == pdxlnHashCond->Pdxlop()->Edxlop());
			}

			CMappingColIdVarPlStmt mapcidvarplstmt = CMappingColIdVarPlStmt
														(
														m_pmp,
														NULL,
														pdrgpdxltrctx,
														pdxltrctxOut,
														m_pctxdxltoplstmt,
														pplan
														);

			// translate the DXL scalar or scalar distinct comparison into an equality comparison
			// to store in the hash clauses
			Expr *pexpr2 = (Expr *) m_pdxlsctranslator->PopexprFromDXLNodeScCmp
									(
									pdxlnHashCond,
									&mapcidvarplstmt
									);

			plHashClauses = gpdb::PlAppendElement(plHashClauses, pexpr2);
		}

		phj->hashclauses = plHashClauses;
		phj->hashqualclauses = plHashConditions;
	}

	GPOS_ASSERT(NIL != phj->hashclauses);

	pplan->lefttree = pplanLeft;
	pplan->righttree = pplanRight;
	pplan->nMotionNodes = pplanLeft->nMotionNodes + pplanRight->nMotionNodes;
	SetParamIds(pplan);

	// cleanup
	pdrgpdxltrctxWithSiblings->Release();
	pdrgpdxltrctx->Release();

	return  (Plan *) phj;
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorDXLToPlStmt::PplanFunctionScanFromDXLTVF
//
//	@doc:
//		Translates a DXL TVF node into a GPDB Function scan node
//
//---------------------------------------------------------------------------
Plan *
CTranslatorDXLToPlStmt::PplanFunctionScanFromDXLTVF
	(
	const CDXLNode *pdxlnTVF,
	CDXLTranslateContext *pdxltrctxOut,
	Plan *pplanParent,
	DrgPdxltrctx *pdrgpdxltrctxPrevSiblings
	)
{
	// translation context for column mappings
	CDXLTranslateContextBaseTable dxltrctxbt(m_pmp);

	// we will add the new range table entry as the last element of the range table
	Index iRel = gpdb::UlListLength(m_pctxdxltoplstmt->PlPrte()) + 1;

	dxltrctxbt.SetIdx(iRel);

	// create function scan node
	FunctionScan *pfuncscan = MakeNode(FunctionScan);
	pfuncscan->scan.scanrelid = iRel;
	Plan *pplan = &(pfuncscan->scan.plan);

	RangeTblEntry *prte = PrteFromDXLTVF(pdxlnTVF, pdxltrctxOut, &dxltrctxbt, pplan);
	GPOS_ASSERT(NULL != prte);

	m_pctxdxltoplstmt->AddRTE(prte);

	pplan->plan_node_id = m_pctxdxltoplstmt->UlNextPlanId();
	pplan->plan_parent_node_id = IPlanId(pplanParent);
	pplan->nMotionNodes = 0;

	// translate operator costs
	TranslatePlanCosts
		(
		CDXLPhysicalProperties::PdxlpropConvert(pdxlnTVF->Pdxlprop())->Pdxlopcost(),
		&(pplan->startup_cost),
		&(pplan->total_cost),
		&(pplan->plan_rows),
		&(pplan->plan_width)
		);

	// a table scan node must have at least 1 child: projection list
	GPOS_ASSERT(1 <= pdxlnTVF->UlArity());

	CDXLNode *pdxlnPrL = (*pdxlnTVF)[EdxltsIndexProjList];

	// translate proj list
	List *plTargetList = PlTargetListFromProjList
						(
						pdxlnPrL,
						&dxltrctxbt,
						NULL,
						pdxltrctxOut,
						pplan
						);

	pplan->targetlist = plTargetList;

	ListCell *plcTe = NULL;

	ForEach (plcTe, plTargetList)
	{
		TargetEntry *pte = (TargetEntry *) lfirst(plcTe);
		OID oidType = gpdb::OidExprType((Node*) pte->expr);
		GPOS_ASSERT(InvalidOid != oidType);

		INT typMod = gpdb::IExprTypeMod((Node*) pte->expr);

		prte->funccoltypes = gpdb::PlAppendOid(prte->funccoltypes, oidType);
		prte->funccoltypmods = gpdb::PlAppendInt(prte->funccoltypmods, typMod);
	}

	SetParamIds(pplan);

	return (Plan *) pfuncscan;
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorDXLToPlStmt::PrteFromDXLTVF
//
//	@doc:
//		Create a range table entry from a CDXLPhysicalTVF node
//
//---------------------------------------------------------------------------
RangeTblEntry *
CTranslatorDXLToPlStmt::PrteFromDXLTVF
	(
	const CDXLNode *pdxlnTVF,
	CDXLTranslateContext *pdxltrctxOut,
	CDXLTranslateContextBaseTable *pdxltrctxbt,
	Plan *pplanParent
	)
{
	CDXLPhysicalTVF *pdxlop = CDXLPhysicalTVF::PdxlopConvert(pdxlnTVF->Pdxlop());

	RangeTblEntry *prte = MakeNode(RangeTblEntry);
	prte->rtekind = RTE_FUNCTION;

	FuncExpr *pfuncexpr = MakeNode(FuncExpr);

	pfuncexpr->funcid = CMDIdGPDB::PmdidConvert(pdxlop->PmdidFunc())->OidObjectId();
	pfuncexpr->funcretset = true;
	// this is a function call, as opposed to a cast
	pfuncexpr->funcformat = COERCE_EXPLICIT_CALL;
	pfuncexpr->funcresulttype = CMDIdGPDB::PmdidConvert(pdxlop->PmdidRetType())->OidObjectId();

	Alias *palias = MakeNode(Alias);
	palias->colnames = NIL;

	// get function alias
	palias->aliasname = CTranslatorUtils::SzFromWsz(pdxlop->Pstr()->Wsz());

	// project list
	CDXLNode *pdxlnPrL = (*pdxlnTVF)[EdxltsIndexProjList];

	// get column names
	const ULONG ulCols = pdxlnPrL->UlArity();
	for (ULONG ul = 0; ul < ulCols; ul++)
	{
		CDXLNode *pdxlnPrElem = (*pdxlnPrL)[ul];
		CDXLScalarProjElem *pdxlopPrEl = CDXLScalarProjElem::PdxlopConvert(pdxlnPrElem->Pdxlop());

		CHAR *szColName = CTranslatorUtils::SzFromWsz(pdxlopPrEl->PmdnameAlias()->Pstr()->Wsz());

		Value *pvalColName = gpdb::PvalMakeString(szColName);
		palias->colnames = gpdb::PlAppendElement(palias->colnames, pvalColName);

		// save mapping col id -> index in translate context
		(void) pdxltrctxbt->FInsertMapping(pdxlopPrEl->UlId(), ul+1 /*iAttno*/);
	}

	// function arguments
	const ULONG ulChildren = pdxlnTVF->UlArity();
	for (ULONG ul = 1; ul < ulChildren; ++ul)
	{
		CDXLNode *pdxlnFuncArg = (*pdxlnTVF)[ul];

		CMappingColIdVarPlStmt mapcidvarplstmt
									(
									m_pmp,
									pdxltrctxbt,
									NULL,
									pdxltrctxOut,
									m_pctxdxltoplstmt,
									pplanParent
									);

		Expr *pexprFuncArg = m_pdxlsctranslator->PexprFromDXLNodeScalar(pdxlnFuncArg, &mapcidvarplstmt);
		pfuncexpr->args = gpdb::PlAppendElement(pfuncexpr->args, pexprFuncArg);
	}

	prte->funcexpr = (Node *)pfuncexpr;
	prte->inFromCl = true;
	prte->eref = palias;

	return prte;
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorDXLToPlStmt::PnljFromDXLNLJ
//
//	@doc:
//		Translates a DXL nested loop join node into a NestLoop plan node
//
//---------------------------------------------------------------------------
Plan *
CTranslatorDXLToPlStmt::PnljFromDXLNLJ
	(
	const CDXLNode *pdxlnNLJ,
	CDXLTranslateContext *pdxltrctxOut,
	Plan *pplanParent,
	DrgPdxltrctx *pdrgpdxltrctxPrevSiblings
	)
{
	GPOS_ASSERT(pdxlnNLJ->Pdxlop()->Edxlop() == EdxlopPhysicalNLJoin);
	GPOS_ASSERT(pdxlnNLJ->UlArity() == EdxlnljIndexSentinel);

	// create hash join node
	NestLoop *pnlj = MakeNode(NestLoop);

	Join *pj = &(pnlj->join);
	Plan *pplan = &(pj->plan);
	pplan->plan_node_id = m_pctxdxltoplstmt->UlNextPlanId();
	pplan->plan_parent_node_id = IPlanId(pplanParent);

	CDXLPhysicalNLJoin *pdxlnlj = CDXLPhysicalNLJoin::PdxlConvert(pdxlnNLJ->Pdxlop());

	// set join type
	pj->jointype = JtFromEdxljt(pdxlnlj->Edxltype());

	// translate operator costs
	TranslatePlanCosts
		(
		CDXLPhysicalProperties::PdxlpropConvert(pdxlnNLJ->Pdxlprop())->Pdxlopcost(),
		&(pplan->startup_cost),
		&(pplan->total_cost),
		&(pplan->plan_rows),
		&(pplan->plan_width)
		);

	// translate join children
	CDXLNode *pdxlnLeft = (*pdxlnNLJ)[EdxlnljIndexLeftChild];
	CDXLNode *pdxlnRight = (*pdxlnNLJ)[EdxlnljIndexRightChild];

	CDXLNode *pdxlnPrL = (*pdxlnNLJ)[EdxlnljIndexProjList];
	CDXLNode *pdxlnFilter = (*pdxlnNLJ)[EdxlnljIndexFilter];
	CDXLNode *pdxlnJoinFilter = (*pdxlnNLJ)[EdxlnljIndexJoinFilter];

	CDXLTranslateContext dxltrctxLeft(m_pmp, false, pdxltrctxOut->PhmColParam());
	CDXLTranslateContext dxltrctxRight(m_pmp, false, pdxltrctxOut->PhmColParam());

	// setting of prefetch_inner to true except for the case of index NLJ where we cannot prefetch inner
	// because inner child depends on variables coming from outer child
	pj->prefetch_inner = !pdxlnlj->FIndexNLJ();

	DrgPdxltrctx *pdrgpdxltrctxWithSiblings = GPOS_NEW(m_pmp) DrgPdxltrctx(m_pmp);
	Plan *pplanLeft = NULL;
	Plan *pplanRight = NULL;
	if (pdxlnlj->FIndexNLJ())
	{
		// right child (the index scan side) has references to left child's columns,
		// we need to translate left child first to load its columns into translation context
		pplanLeft = PplFromDXL(pdxlnLeft, &dxltrctxLeft, pplan, pdrgpdxltrctxPrevSiblings);

		pdrgpdxltrctxWithSiblings->Append(&dxltrctxLeft);
		 pdrgpdxltrctxWithSiblings->AppendArray(pdrgpdxltrctxPrevSiblings);

		 // translate right child after left child translation is complete
		pplanRight = PplFromDXL(pdxlnRight, &dxltrctxRight, pplan, pdrgpdxltrctxWithSiblings);
	}
	else
	{
		// left child may include a PartitionSelector with references to right child's columns,
		// we need to translate right child first to load its columns into translation context
		pplanRight = PplFromDXL(pdxlnRight, &dxltrctxRight, pplan, pdrgpdxltrctxPrevSiblings);

		pdrgpdxltrctxWithSiblings->Append(&dxltrctxRight);
		pdrgpdxltrctxWithSiblings->AppendArray(pdrgpdxltrctxPrevSiblings);

		// translate left child after right child translation is complete
		pplanLeft = PplFromDXL(pdxlnLeft, &dxltrctxLeft, pplan, pdrgpdxltrctxWithSiblings);
	}
	DrgPdxltrctx *pdrgpdxltrctx = GPOS_NEW(m_pmp) DrgPdxltrctx(m_pmp);
	pdrgpdxltrctx->Append(&dxltrctxLeft);
	pdrgpdxltrctx->Append(&dxltrctxRight);

	// translate proj list and filter
	TranslateProjListAndFilter
		(
		pdxlnPrL,
		pdxlnFilter,
		NULL,			// translate context for the base table
		pdrgpdxltrctx,
		&pplan->targetlist,
		&pplan->qual,
		pdxltrctxOut,
		pplan
		);

	// translate join condition
	pj->joinqual = PlQualFromFilter
					(
					pdxlnJoinFilter,
					NULL,			// translate context for the base table
					pdrgpdxltrctx,
					pdxltrctxOut,
					pplan
					);

	pplan->lefttree = pplanLeft;
	pplan->righttree = pplanRight;
	pplan->nMotionNodes = pplanLeft->nMotionNodes + pplanRight->nMotionNodes;
	SetParamIds(pplan);

	// cleanup
	pdrgpdxltrctxWithSiblings->Release();
	pdrgpdxltrctx->Release();

	return  (Plan *) pnlj;
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorDXLToPlStmt::PmjFromDXLMJ
//
//	@doc:
//		Translates a DXL merge join node into a MergeJoin node
//
//---------------------------------------------------------------------------
Plan *
CTranslatorDXLToPlStmt::PmjFromDXLMJ
	(
	const CDXLNode *pdxlnMJ,
	CDXLTranslateContext *pdxltrctxOut,
	Plan *pplanParent,
	DrgPdxltrctx *pdrgpdxltrctxPrevSiblings
	)
{
	GPOS_ASSERT(pdxlnMJ->Pdxlop()->Edxlop() == EdxlopPhysicalMergeJoin);
	GPOS_ASSERT(pdxlnMJ->UlArity() == EdxlmjIndexSentinel);

	// create merge join node
	MergeJoin *pmj = MakeNode(MergeJoin);

	Join *pj = &(pmj->join);
	Plan *pplan = &(pj->plan);
	pplan->plan_node_id = m_pctxdxltoplstmt->UlNextPlanId();
	pplan->plan_parent_node_id = IPlanId(pplanParent);

	CDXLPhysicalMergeJoin *pdxlopMergeJoin = CDXLPhysicalMergeJoin::PdxlopConvert(pdxlnMJ->Pdxlop());

	// set join type
	pj->jointype = JtFromEdxljt(pdxlopMergeJoin->Edxltype());

	// translate operator costs
	TranslatePlanCosts
		(
		CDXLPhysicalProperties::PdxlpropConvert(pdxlnMJ->Pdxlprop())->Pdxlopcost(),
		&(pplan->startup_cost),
		&(pplan->total_cost),
		&(pplan->plan_rows),
		&(pplan->plan_width)
		);

	// translate join children
	CDXLNode *pdxlnLeft = (*pdxlnMJ)[EdxlmjIndexLeftChild];
	CDXLNode *pdxlnRight = (*pdxlnMJ)[EdxlmjIndexRightChild];

	CDXLNode *pdxlnPrL = (*pdxlnMJ)[EdxlmjIndexProjList];
	CDXLNode *pdxlnFilter = (*pdxlnMJ)[EdxlmjIndexFilter];
	CDXLNode *pdxlnJoinFilter = (*pdxlnMJ)[EdxlmjIndexJoinFilter];
	CDXLNode *pdxlnMergeCondList = (*pdxlnMJ)[EdxlmjIndexMergeCondList];

	CDXLTranslateContext dxltrctxLeft(m_pmp, false, pdxltrctxOut->PhmColParam());
	CDXLTranslateContext dxltrctxRight(m_pmp, false, pdxltrctxOut->PhmColParam());

	Plan *pplanLeft = PplFromDXL(pdxlnLeft, &dxltrctxLeft, pplan, pdrgpdxltrctxPrevSiblings);

	DrgPdxltrctx *pdrgpdxltrctxWithSiblings = GPOS_NEW(m_pmp) DrgPdxltrctx(m_pmp);
	pdrgpdxltrctxWithSiblings->Append(&dxltrctxLeft);
	pdrgpdxltrctxWithSiblings->AppendArray(pdrgpdxltrctxPrevSiblings);

	Plan *pplanRight = PplFromDXL(pdxlnRight, &dxltrctxRight, pplan, pdrgpdxltrctxWithSiblings);

	DrgPdxltrctx *pdrgpdxltrctx = GPOS_NEW(m_pmp) DrgPdxltrctx(m_pmp);
	pdrgpdxltrctx->Append(const_cast<CDXLTranslateContext*>(&dxltrctxLeft));
	pdrgpdxltrctx->Append(const_cast<CDXLTranslateContext*>(&dxltrctxRight));

	// translate proj list and filter
	TranslateProjListAndFilter
		(
		pdxlnPrL,
		pdxlnFilter,
		NULL,			// translate context for the base table
		pdrgpdxltrctx,
		&pplan->targetlist,
		&pplan->qual,
		pdxltrctxOut,
		pplan
		);

	// translate join filter
	pj->joinqual = PlQualFromFilter
					(
					pdxlnJoinFilter,
					NULL,			// translate context for the base table
					pdrgpdxltrctx,
					pdxltrctxOut,
					pplan
					);

	// translate merge cond
	List *plMergeConditions = NIL;

	const ULONG ulArity = pdxlnMergeCondList->UlArity();
	for (ULONG ul = 0; ul < ulArity; ul++)
	{
		CDXLNode *pdxlnMergeCond = (*pdxlnMergeCondList)[ul];
		List *plMergeCond = PlQualFromScalarCondNode
				(
				pdxlnMergeCond,
				NULL,			// base table translation context
				pdrgpdxltrctx,
				pdxltrctxOut,
				pplan
				);

		GPOS_ASSERT(1 == gpdb::UlListLength(plMergeCond));
		plMergeConditions = gpdb::PlConcat(plMergeConditions, plMergeCond);
	}

	GPOS_ASSERT(NIL != plMergeConditions);

	pmj->mergeclauses = plMergeConditions;

	pplan->lefttree = pplanLeft;
	pplan->righttree = pplanRight;
	pplan->nMotionNodes = pplanLeft->nMotionNodes + pplanRight->nMotionNodes;
	SetParamIds(pplan);

	// cleanup
	pdrgpdxltrctxWithSiblings->Release();
	pdrgpdxltrctx->Release();

	return  (Plan *) pmj;
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorDXLToPlStmt::PhhashFromDXL
//
//	@doc:
//		Translates a DXL physical operator node into a Hash node
//
//---------------------------------------------------------------------------
Plan *
CTranslatorDXLToPlStmt::PhhashFromDXL
	(
	const CDXLNode *pdxln,
	CDXLTranslateContext *pdxltrctxOut,
	Plan *pplanParent,
	DrgPdxltrctx *pdrgpdxltrctxPrevSiblings
	)
{
	Hash *ph = MakeNode(Hash);

	Plan *pplan = &(ph->plan);
	pplan->plan_node_id = m_pctxdxltoplstmt->UlNextPlanId();
	pplan->plan_parent_node_id = IPlanId(pplanParent);

	// translate dxl node
	CDXLTranslateContext dxltrctx(m_pmp, false, pdxltrctxOut->PhmColParam());

	Plan *pplanLeft = PplFromDXL(pdxln, &dxltrctx, pplan, pdrgpdxltrctxPrevSiblings);

	GPOS_ASSERT(0 < pdxln->UlArity());

	// create a reference to each entry in the child project list to create the target list of
	// the hash node
	CDXLNode *pdxlnPrL = (*pdxln)[0];
	List *plTargetList = PlTargetListForHashNode(pdxlnPrL, &dxltrctx, pdxltrctxOut);

	// copy costs from child node; the startup cost for the hash node is the total cost
	// of the child plan, see make_hash in createplan.c
	pplan->startup_cost = pplanLeft->total_cost;
	pplan->total_cost = pplanLeft->total_cost;
	pplan->plan_rows = pplanLeft->plan_rows;
	pplan->plan_width = pplanLeft->plan_width;

	pplan->targetlist = plTargetList;
	pplan->lefttree = pplanLeft;
	pplan->righttree = NULL;
	pplan->nMotionNodes = pplanLeft->nMotionNodes;
	pplan->qual = NIL;
	ph->rescannable = false;

	SetParamIds(pplan);

	return (Plan *) ph;
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorDXLToPlStmt::PplanTranslateDXLMotion
//
//	@doc:
//		Translate DXL motion node
//
//---------------------------------------------------------------------------
Plan *
CTranslatorDXLToPlStmt::PplanTranslateDXLMotion
	(
	const CDXLNode *pdxlnMotion,
	CDXLTranslateContext *pdxltrctxOut,
	Plan *pplanParent,
	DrgPdxltrctx *pdrgpdxltrctxPrevSiblings
	)
{
	CDXLPhysicalMotion *pdxlopMotion = CDXLPhysicalMotion::PdxlopConvert(pdxlnMotion->Pdxlop());
	if (CTranslatorUtils::FDuplicateSensitiveMotion(pdxlopMotion))
	{
		return PplanResultHashFilters(pdxlnMotion, pdxltrctxOut, pplanParent, pdrgpdxltrctxPrevSiblings);
	}
	
	return PplanMotionFromDXLMotion(pdxlnMotion, pdxltrctxOut, pplanParent, pdrgpdxltrctxPrevSiblings);
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorDXLToPlStmt::PplanMotionFromDXLMotion
//
//	@doc:
//		Translate DXL motion node into GPDB Motion plan node
//
//---------------------------------------------------------------------------
Plan *
CTranslatorDXLToPlStmt::PplanMotionFromDXLMotion
	(
	const CDXLNode *pdxlnMotion,
	CDXLTranslateContext *pdxltrctxOut,
	Plan *pplanParent,
	DrgPdxltrctx *pdrgpdxltrctxPrevSiblings
	)
{
	CDXLPhysicalMotion *pdxlopMotion = CDXLPhysicalMotion::PdxlopConvert(pdxlnMotion->Pdxlop());

	// create motion node
	Motion *pmotion = MakeNode(Motion);

	Plan *pplan = &(pmotion->plan);
	pplan->plan_node_id = m_pctxdxltoplstmt->UlNextPlanId();
	pplan->plan_parent_node_id = IPlanId(pplanParent);

	// translate operator costs
	TranslatePlanCosts
		(
		CDXLPhysicalProperties::PdxlpropConvert(pdxlnMotion->Pdxlprop())->Pdxlopcost(),
		&(pplan->startup_cost),
		&(pplan->total_cost),
		&(pplan->plan_rows),
		&(pplan->plan_width)
		);

	CDXLNode *pdxlnPrL = (*pdxlnMotion)[EdxlgmIndexProjList];
	CDXLNode *pdxlnFilter = (*pdxlnMotion)[EdxlgmIndexFilter];
	CDXLNode *pdxlnSortColList = (*pdxlnMotion)[EdxlgmIndexSortColList];

	// translate motion child
	// child node is in the same position in broadcast and gather motion nodes
	// but different in redistribute motion nodes

	ULONG ulChildIndex = pdxlopMotion->UlChildIndex();

	CDXLNode *pdxlnChild = (*pdxlnMotion)[ulChildIndex];

	CDXLTranslateContext dxltrctxChild(m_pmp, false, pdxltrctxOut->PhmColParam());

	Plan *pplanChild = PplFromDXL(pdxlnChild, &dxltrctxChild, pplan, pdrgpdxltrctxPrevSiblings);

	DrgPdxltrctx *pdrgpdxltrctx = GPOS_NEW(m_pmp) DrgPdxltrctx(m_pmp);
	pdrgpdxltrctx->Append(const_cast<CDXLTranslateContext*>(&dxltrctxChild));

	// translate proj list and filter
	TranslateProjListAndFilter
		(
		pdxlnPrL,
		pdxlnFilter,
		NULL,			// translate context for the base table
		pdrgpdxltrctx,
		&pplan->targetlist,
		&pplan->qual,
		pdxltrctxOut,
		pplan
		);

	// translate sorting info
	ULONG ulNumSortCols = pdxlnSortColList->UlArity();
	if (0 < ulNumSortCols)
	{
		pmotion->sendSorted = true;
		pmotion->numSortCols = ulNumSortCols;
		pmotion->sortColIdx = (AttrNumber *) gpdb::GPDBAlloc(ulNumSortCols * sizeof(AttrNumber));
		pmotion->sortOperators = (Oid *) gpdb::GPDBAlloc(ulNumSortCols * sizeof(Oid));

		TranslateSortCols(pdxlnSortColList, pdxltrctxOut, pmotion->sortColIdx, pmotion->sortOperators);
	}
	else
	{
		// not a sorting motion
		pmotion->sendSorted = false;
		pmotion->numSortCols = 0;
		pmotion->sortColIdx = NULL;
		pmotion->sortOperators = NULL;
	}

	if (pdxlopMotion->Edxlop() == EdxlopPhysicalMotionRedistribute ||
		pdxlopMotion->Edxlop() == EdxlopPhysicalMotionRoutedDistribute ||
		pdxlopMotion->Edxlop() == EdxlopPhysicalMotionRandom)
	{
		// translate hash expr list
		List *plHashExpr = NIL;
		List *plHashExprTypes = NIL;

		if (EdxlopPhysicalMotionRedistribute == pdxlopMotion->Edxlop())
		{
			CDXLNode *pdxlnHashExprList = (*pdxlnMotion)[EdxlrmIndexHashExprList];

			TranslateHashExprList
				(
				pdxlnHashExprList,
				&dxltrctxChild,
				&plHashExpr,
				&plHashExprTypes,
				pdxltrctxOut,
				pplan
				);
		}
		GPOS_ASSERT(gpdb::UlListLength(plHashExpr) == gpdb::UlListLength(plHashExprTypes));

		pmotion->hashExpr = plHashExpr;
		pmotion->hashDataTypes = plHashExprTypes;
	}

	// cleanup
	pdrgpdxltrctx->Release();

	// create flow for child node to distinguish between singleton flows and all-segment flows
	Flow *pflow = MakeNode(Flow);

	const DrgPi *pdrgpiInputSegmentIds = pdxlopMotion->PdrgpiInputSegIds();

	if (1 == pdrgpiInputSegmentIds->UlLength())
	{
		// only one sender - child has a singleton flow
		pflow->flotype = FLOW_SINGLETON;
		pflow->segindex = *((*pdrgpiInputSegmentIds)[0]);
	}
	else
	{
		pflow->flotype = FLOW_UNDEFINED;
	}

	pplanChild->flow = pflow;

	pmotion->motionID = m_pctxdxltoplstmt->UlNextMotionId();
	pplan->lefttree = pplanChild;
	pplan->nMotionNodes = pplanChild->nMotionNodes + 1;

	// translate properties of the specific type of motion operator

	switch (pdxlopMotion->Edxlop())
	{
		case EdxlopPhysicalMotionGather:
		{
			pmotion->motionType = MOTIONTYPE_FIXED;
			// get segment id
			INT iSegId = CDXLPhysicalGatherMotion::PdxlopConvert(pdxlopMotion)->IOutputSegIdx();
			pmotion->numOutputSegs = 1;
			pmotion->outputSegIdx = (INT *) gpdb::GPDBAlloc(sizeof(INT));
			*(pmotion->outputSegIdx) = iSegId;
			break;
		}
		case EdxlopPhysicalMotionRedistribute:
		case EdxlopPhysicalMotionRandom:
		{
			pmotion->motionType = MOTIONTYPE_HASH;
			// translate output segment ids
			const DrgPi *pdrgpiSegIds = CDXLPhysicalMotion::PdxlopConvert(pdxlopMotion)->PdrgpiOutputSegIds();

			GPOS_ASSERT(NULL != pdrgpiSegIds && 0 < pdrgpiSegIds->UlLength());
			ULONG ulSegIdCount = pdrgpiSegIds->UlLength();
			pmotion->outputSegIdx = (INT *) gpdb::GPDBAlloc (ulSegIdCount * sizeof(INT));
			pmotion->numOutputSegs = ulSegIdCount;

			for(ULONG ul = 0; ul < ulSegIdCount; ul++)
			{
				INT iSegId = *((*pdrgpiSegIds)[ul]);
				pmotion->outputSegIdx[ul] = iSegId;
			}

			break;
		}
		case EdxlopPhysicalMotionBroadcast:
		{
			pmotion->motionType = MOTIONTYPE_FIXED;
			pmotion->numOutputSegs = 0;
			pmotion->outputSegIdx = NULL;
			break;
		}
		case EdxlopPhysicalMotionRoutedDistribute:
		{
			pmotion->motionType = MOTIONTYPE_EXPLICIT;
			pmotion->numOutputSegs = 0;
			pmotion->outputSegIdx = NULL;
			ULONG ulSegIdCol = CDXLPhysicalRoutedDistributeMotion::PdxlopConvert(pdxlopMotion)->UlSegmentIdCol();
			const TargetEntry *pteSortCol = dxltrctxChild.Pte(ulSegIdCol);
			pmotion->segidColIdx = pteSortCol->resno;

			break;
			
		}
		default:
			GPOS_ASSERT(!"Unrecognized Motion operator");
			return NULL;
	}

	SetParamIds(pplan);

	return (Plan *) pmotion;
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorDXLToPlStmt::PplanResultHashFilters
//
//	@doc:
//		Translate DXL duplicate sensitive redistribute motion node into 
//		GPDB result node with hash filters
//
//---------------------------------------------------------------------------
Plan *
CTranslatorDXLToPlStmt::PplanResultHashFilters
	(
	const CDXLNode *pdxlnMotion,
	CDXLTranslateContext *pdxltrctxOut,
	Plan *pplanParent,
	DrgPdxltrctx *pdrgpdxltrctxPrevSiblings
	)
{
	// create motion node
	Result *presult = MakeNode(Result);

	Plan *pplan = &(presult->plan);
	pplan->plan_node_id = m_pctxdxltoplstmt->UlNextPlanId();
	pplan->plan_parent_node_id = IPlanId(pplanParent);

	CDXLPhysicalMotion *pdxlopMotion = CDXLPhysicalMotion::PdxlopConvert(pdxlnMotion->Pdxlop());

	// translate operator costs
	TranslatePlanCosts
		(
		CDXLPhysicalProperties::PdxlpropConvert(pdxlnMotion->Pdxlprop())->Pdxlopcost(),
		&(pplan->startup_cost),
		&(pplan->total_cost),
		&(pplan->plan_rows),
		&(pplan->plan_width)
		);

	CDXLNode *pdxlnPrL = (*pdxlnMotion)[EdxlrmIndexProjList];
	CDXLNode *pdxlnFilter = (*pdxlnMotion)[EdxlrmIndexFilter];
	CDXLNode *pdxlnChild = (*pdxlnMotion)[pdxlopMotion->UlChildIndex()];

	CDXLTranslateContext dxltrctxChild(m_pmp, false, pdxltrctxOut->PhmColParam());

	Plan *pplanChild = PplFromDXL(pdxlnChild, &dxltrctxChild, pplan, pdrgpdxltrctxPrevSiblings);

	DrgPdxltrctx *pdrgpdxltrctx = GPOS_NEW(m_pmp) DrgPdxltrctx(m_pmp);
	pdrgpdxltrctx->Append(const_cast<CDXLTranslateContext*>(&dxltrctxChild));

	// translate proj list and filter
	TranslateProjListAndFilter
		(
		pdxlnPrL,
		pdxlnFilter,
		NULL,			// translate context for the base table
		pdrgpdxltrctx,
		&pplan->targetlist,
		&pplan->qual,
		pdxltrctxOut,
		pplan
		);

	// translate hash expr list
	presult->hashFilter = true;

	if (EdxlopPhysicalMotionRedistribute == pdxlopMotion->Edxlop())
	{
		CDXLNode *pdxlnHashExprList = (*pdxlnMotion)[EdxlrmIndexHashExprList];
		const ULONG ulLength = pdxlnHashExprList->UlArity();
		GPOS_ASSERT(0 < ulLength);
		
		for (ULONG ul = 0; ul < ulLength; ul++)
		{
			CDXLNode *pdxlnHashExpr = (*pdxlnHashExprList)[ul];
			CDXLNode *pdxlnExpr = (*pdxlnHashExpr)[0];
			
			INT iResno = INT_MAX;
			if (EdxlopScalarIdent == pdxlnExpr->Pdxlop()->Edxlop())
			{
				ULONG ulColId = CDXLScalarIdent::PdxlopConvert(pdxlnExpr->Pdxlop())->Pdxlcr()->UlID();
				iResno = pdxltrctxOut->Pte(ulColId)->resno;
			}
			else
			{
				// The expression is not a scalar ident that points to an output column in the child node.
				// Rather, it is an expresssion that is evaluated by the hash filter such as CAST(a) or a+b.
				// We therefore, create a corresponding GPDB scalar expression and add it to the project list
				// of the hash filter
				CMappingColIdVarPlStmt mapcidvarplstmt = CMappingColIdVarPlStmt
															(
															m_pmp,
															NULL, // translate context for the base table
															pdrgpdxltrctx,
															pdxltrctxOut,
															m_pctxdxltoplstmt,
															pplanParent
															);
				
				Expr *pexpr = m_pdxlsctranslator->PexprFromDXLNodeScalar(pdxlnExpr, &mapcidvarplstmt);
				GPOS_ASSERT(NULL != pexpr);

				// create a target entry for the hash filter
				CWStringConst strUnnamedCol(GPOS_WSZ_LIT("?column?"));
				TargetEntry *pte = gpdb::PteMakeTargetEntry
											(
											pexpr, 
											gpdb::UlListLength(pplan->targetlist) + 1, 
											CTranslatorUtils::SzFromWsz(strUnnamedCol.Wsz()), 
											false /* resjunk */
											);
				pplan->targetlist = gpdb::PlAppendElement(pplan->targetlist, pte);

				iResno = pte->resno;
			}
			GPOS_ASSERT(INT_MAX != iResno);
			
			presult->hashList = gpdb::PlAppendInt(presult->hashList, iResno);
		}
	}
	
	// cleanup
	pdrgpdxltrctx->Release();

	pplan->lefttree = pplanChild;
	pplan->nMotionNodes = pplanChild->nMotionNodes;

	SetParamIds(pplan);

	return (Plan *) presult;
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorDXLToPlStmt::PaggFromDXLAgg
//
//	@doc:
//		Translate DXL aggregate node into GPDB Agg plan node
//
//---------------------------------------------------------------------------
Plan *
CTranslatorDXLToPlStmt::PaggFromDXLAgg
	(
	const CDXLNode *pdxlnAgg,
	CDXLTranslateContext *pdxltrctxOut,
	Plan *pplanParent,
	DrgPdxltrctx *pdrgpdxltrctxPrevSiblings
	)
{
	// create aggregate plan node
	Agg *pagg = MakeNode(Agg);

	Plan *pplan = &(pagg->plan);
	pplan->plan_node_id = m_pctxdxltoplstmt->UlNextPlanId();
	pplan->plan_parent_node_id = IPlanId(pplanParent);

	CDXLPhysicalAgg *pdxlopAgg = CDXLPhysicalAgg::PdxlopConvert(pdxlnAgg->Pdxlop());

	// translate operator costs
	TranslatePlanCosts
		(
		CDXLPhysicalProperties::PdxlpropConvert(pdxlnAgg->Pdxlprop())->Pdxlopcost(),
		&(pplan->startup_cost),
		&(pplan->total_cost),
		&(pplan->plan_rows),
		&(pplan->plan_width)
		);

	// translate agg child
	CDXLNode *pdxlnChild = (*pdxlnAgg)[EdxlaggIndexChild];

	CDXLNode *pdxlnPrL = (*pdxlnAgg)[EdxlaggIndexProjList];
	CDXLNode *pdxlnFilter = (*pdxlnAgg)[EdxlaggIndexFilter];

	CDXLTranslateContext dxltrctxChild(m_pmp, true, pdxltrctxOut->PhmColParam());

	Plan *pplanChild = PplFromDXL(pdxlnChild, &dxltrctxChild, pplan, pdrgpdxltrctxPrevSiblings);

	DrgPdxltrctx *pdrgpdxltrctx = GPOS_NEW(m_pmp) DrgPdxltrctx(m_pmp);
	pdrgpdxltrctx->Append(const_cast<CDXLTranslateContext*>(&dxltrctxChild));

	// translate proj list and filter
	TranslateProjListAndFilter
		(
		pdxlnPrL,
		pdxlnFilter,
		NULL,			// translate context for the base table
		pdrgpdxltrctx,			// pdxltrctxRight,
		&pplan->targetlist,
		&pplan->qual,
		pdxltrctxOut,
		pplan
		);

	pplan->lefttree = pplanChild;
	pplan->nMotionNodes = pplanChild->nMotionNodes;

	// translate aggregation strategy
	switch (pdxlopAgg->Edxlaggstr())
	{
		case EdxlaggstrategyPlain:
			pagg->aggstrategy = AGG_PLAIN;
			break;
		case EdxlaggstrategySorted:
			pagg->aggstrategy = AGG_SORTED;
			break;
		case EdxlaggstrategyHashed:
			pagg->aggstrategy = AGG_HASHED;
			break;
		default:
			GPOS_ASSERT(!"Invalid aggregation strategy");
	}

	pagg->streaming = pdxlopAgg->FStreamSafe();

	// translate grouping cols
	const DrgPul *pdrpulGroupingCols = pdxlopAgg->PdrgpulGroupingCols();
	pagg->numCols = pdrpulGroupingCols->UlLength();
	if (pagg->numCols > 0)
	{
		pagg->grpColIdx = (AttrNumber *) gpdb::GPDBAlloc(pagg->numCols * sizeof(AttrNumber));
	}
	else
	{
		pagg->grpColIdx = NULL;
	}

	const ULONG ulLen = pdrpulGroupingCols->UlLength();
	for (ULONG ul = 0; ul < ulLen; ul++)
	{
		ULONG ulGroupingColId = *((*pdrpulGroupingCols)[ul]);
		const TargetEntry *pteGroupingCol = dxltrctxChild.Pte(ulGroupingColId);
		if (NULL  == pteGroupingCol)
		{
			GPOS_RAISE(gpdxl::ExmaDXL, gpdxl::ExmiDXL2PlStmtAttributeNotFound, ulGroupingColId);
		}
		pagg->grpColIdx[ul] = pteGroupingCol->resno;
	}

	SetParamIds(pplan);

	// cleanup
	pdrgpdxltrctx->Release();

	return (Plan *) pagg;
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorDXLToPlStmt::PwindowFromDXLWindow
//
//	@doc:
//		Translate DXL window node into GPDB window plan node
//
//---------------------------------------------------------------------------
Plan *
CTranslatorDXLToPlStmt::PwindowFromDXLWindow
	(
	const CDXLNode *pdxlnWindow,
	CDXLTranslateContext *pdxltrctxOut,
	Plan *pplanParent,
	DrgPdxltrctx *pdrgpdxltrctxPrevSiblings
	)
{
	// create a window plan node
	Window *pwindow = MakeNode(Window);

	Plan *pplan = &(pwindow->plan);
	pplan->plan_node_id = m_pctxdxltoplstmt->UlNextPlanId();
	pplan->plan_parent_node_id = IPlanId(pplanParent);

	CDXLPhysicalWindow *pdxlopWindow = CDXLPhysicalWindow::PdxlopConvert(pdxlnWindow->Pdxlop());

	// translate the operator costs
	TranslatePlanCosts
		(
		CDXLPhysicalProperties::PdxlpropConvert(pdxlnWindow->Pdxlprop())->Pdxlopcost(),
		&(pplan->startup_cost),
		&(pplan->total_cost),
		&(pplan->plan_rows),
		&(pplan->plan_width)
		);

	// translate children
	CDXLNode *pdxlnChild = (*pdxlnWindow)[EdxlwindowIndexChild];
	CDXLNode *pdxlnPrL = (*pdxlnWindow)[EdxlwindowIndexProjList];
	CDXLNode *pdxlnFilter = (*pdxlnWindow)[EdxlwindowIndexFilter];

	CDXLTranslateContext dxltrctxChild(m_pmp, true, pdxltrctxOut->PhmColParam());
	Plan *pplanChild = PplFromDXL(pdxlnChild, &dxltrctxChild, pplan, pdrgpdxltrctxPrevSiblings);

	DrgPdxltrctx *pdrgpdxltrctx = GPOS_NEW(m_pmp) DrgPdxltrctx(m_pmp);
	pdrgpdxltrctx->Append(const_cast<CDXLTranslateContext*>(&dxltrctxChild));

	// translate proj list and filter
	TranslateProjListAndFilter
		(
		pdxlnPrL,
		pdxlnFilter,
		NULL,			// translate context for the base table
		pdrgpdxltrctx,			// pdxltrctxRight,
		&pplan->targetlist,
		&pplan->qual,
		pdxltrctxOut,
		pplan
		);

	pplan->lefttree = pplanChild;
	pplan->nMotionNodes = pplanChild->nMotionNodes;

	// translate partition columns
	const DrgPul *pdrpulPartCols = pdxlopWindow->PrgpulPartCols();
	pwindow->numPartCols = pdrpulPartCols->UlLength();
	pwindow->partColIdx = NULL;

	if (pwindow->numPartCols > 0)
	{
		pwindow->partColIdx = (AttrNumber *) gpdb::GPDBAlloc(pwindow->numPartCols * sizeof(AttrNumber));
	}

	const ULONG ulPartCols = pdrpulPartCols->UlLength();
	for (ULONG ul = 0; ul < ulPartCols; ul++)
	{
		ULONG ulPartColId = *((*pdrpulPartCols)[ul]);
		const TargetEntry *ptePartCol = dxltrctxChild.Pte(ulPartColId);
		if (NULL  == ptePartCol)
		{
			GPOS_RAISE(gpdxl::ExmaDXL, gpdxl::ExmiDXL2PlStmtAttributeNotFound, ulPartColId);
		}
		pwindow->partColIdx[ul] = ptePartCol->resno;
	}

	// translate window keys
	pwindow->windowKeys = NIL;
	const ULONG ulSize = pdxlopWindow->UlWindowKeys();
	for (ULONG ul = 0; ul < ulSize; ul++)
	{
		WindowKey *pwindowkey = MakeNode(WindowKey);

		// translate the sorting columns used in the window key
		const CDXLWindowKey *pdxlwindowkey = pdxlopWindow->PdxlWindowKey(ul);
		const CDXLNode *pdxlnSortColList = pdxlwindowkey->PdxlnSortColList();

		const ULONG ulNumCols = pdxlnSortColList->UlArity();
		pwindowkey->numSortCols = ulNumCols;
		pwindowkey->sortColIdx = (AttrNumber *) gpdb::GPDBAlloc(ulNumCols * sizeof(AttrNumber));
		pwindowkey->sortOperators = (Oid *) gpdb::GPDBAlloc(ulNumCols * sizeof(Oid));
		TranslateSortCols(pdxlnSortColList, &dxltrctxChild, pwindowkey->sortColIdx, pwindowkey->sortOperators);

		// translate the window frame specified in the window key
		pwindowkey->frame = NULL;
		if (NULL != pdxlwindowkey->Pdxlwf())
		{
			pwindowkey->frame = Pwindowframe(pdxlwindowkey->Pdxlwf(), &dxltrctxChild, pdxltrctxOut, pplan);
		}
		pwindow->windowKeys = gpdb::PlAppendElement(pwindow->windowKeys, pwindowkey);
	}

	SetParamIds(pplan);

	// cleanup
	pdrgpdxltrctx->Release();

	return (Plan *) pwindow;
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorDXLToPlStmt::Pwindowframe
//
//	@doc:
//		Translate the DXL window frame into GPDB Window frame node
//
//---------------------------------------------------------------------------
WindowFrame *
CTranslatorDXLToPlStmt::Pwindowframe
	(
	const CDXLWindowFrame *pdxlwf,
	const CDXLTranslateContext *pdxltrctxChild,
	CDXLTranslateContext *pdxltrctxOut,
	Plan *pplan
	)
{
	WindowFrame *pwindowframe = MakeNode(WindowFrame);

	if (EdxlfsRow == pdxlwf->Edxlfs())
	{
		pwindowframe->is_rows = true;
	}
	else
	{
		pwindowframe->is_rows = false;
	}
	pwindowframe->is_between = true;

	pwindowframe->exclude = CTranslatorUtils::Windowexclusion(pdxlwf->Edxlfes());

	// translate the CDXLNodes representing the leading and trailing edge
	DrgPdxltrctx *pdrgpdxltrctx = GPOS_NEW(m_pmp) DrgPdxltrctx(m_pmp);
	pdrgpdxltrctx->Append(pdxltrctxChild);

	CMappingColIdVarPlStmt mapcidvarplstmt = CMappingColIdVarPlStmt
												(
												m_pmp,
												NULL,
												pdrgpdxltrctx,
												pdxltrctxOut,
												m_pctxdxltoplstmt,
												pplan
												);

	CDXLNode *pdxlnLead = pdxlwf->PdxlnLeading();
	pwindowframe->lead = MakeNode(WindowFrameEdge);
	pwindowframe->lead->kind = CTranslatorUtils::Windowboundkind(CDXLScalarWindowFrameEdge::PdxlopConvert(pdxlnLead->Pdxlop())->Edxlfb());
	pwindowframe->lead->val = NULL;
	if (0 != pdxlnLead->UlArity())
	{
		pwindowframe->lead->val = (Node*) m_pdxlsctranslator->PexprFromDXLNodeScalar((*pdxlnLead)[0], &mapcidvarplstmt);
	}


	CDXLNode *pdxlnTrail = pdxlwf->PdxlnTrailing();
	pwindowframe->trail = MakeNode(WindowFrameEdge);
	pwindowframe->trail->kind = CTranslatorUtils::Windowboundkind(CDXLScalarWindowFrameEdge::PdxlopConvert(pdxlnTrail->Pdxlop())->Edxlfb());
	pwindowframe->trail->val = NULL;
	if (0 != pdxlnTrail->UlArity())
	{
		pwindowframe->trail->val = (Node*) m_pdxlsctranslator->PexprFromDXLNodeScalar((*pdxlnTrail)[0], &mapcidvarplstmt);
	}

	// cleanup
	pdrgpdxltrctx->Release();

	return pwindowframe;
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorDXLToPlStmt::PsortFromDXLSort
//
//	@doc:
//		Translate DXL sort node into GPDB Sort plan node
//
//---------------------------------------------------------------------------
Plan *
CTranslatorDXLToPlStmt::PsortFromDXLSort
	(
	const CDXLNode *pdxlnSort,
	CDXLTranslateContext *pdxltrctxOut,
	Plan *pplanParent,
	DrgPdxltrctx *pdrgpdxltrctxPrevSiblings
	)
{
	// create sort plan node
	Sort *psort = MakeNode(Sort);

	Plan *pplan = &(psort->plan);
	pplan->plan_node_id = m_pctxdxltoplstmt->UlNextPlanId();
	pplan->plan_parent_node_id = IPlanId(pplanParent);

	CDXLPhysicalSort *pdxlopSort = CDXLPhysicalSort::PdxlopConvert(pdxlnSort->Pdxlop());

	// translate operator costs
	TranslatePlanCosts
		(
		CDXLPhysicalProperties::PdxlpropConvert(pdxlnSort->Pdxlprop())->Pdxlopcost(),
		&(pplan->startup_cost),
		&(pplan->total_cost),
		&(pplan->plan_rows),
		&(pplan->plan_width)
		);

	// translate sort child
	CDXLNode *pdxlnChild = (*pdxlnSort)[EdxlsortIndexChild];
	CDXLNode *pdxlnPrL = (*pdxlnSort)[EdxlsortIndexProjList];
	CDXLNode *pdxlnFilter = (*pdxlnSort)[EdxlsortIndexFilter];
	CDXLNode *pdxlnLimitCount = (*pdxlnSort)[EdxlsortIndexLimitCount];
	CDXLNode *pdxlnLimitOffset = (*pdxlnSort)[EdxlsortIndexLimitOffset];

	CDXLTranslateContext dxltrctxChild(m_pmp, false, pdxltrctxOut->PhmColParam());

	Plan *pplanChild = PplFromDXL(pdxlnChild, &dxltrctxChild, pplan, pdrgpdxltrctxPrevSiblings);

	DrgPdxltrctx *pdrgpdxltrctx = GPOS_NEW(m_pmp) DrgPdxltrctx(m_pmp);
	pdrgpdxltrctx->Append(const_cast<CDXLTranslateContext*>(&dxltrctxChild));

	// translate proj list and filter
	TranslateProjListAndFilter
		(
		pdxlnPrL,
		pdxlnFilter,
		NULL,			// translate context for the base table
		pdrgpdxltrctx,
		&pplan->targetlist,
		&pplan->qual,
		pdxltrctxOut,
		pplan
		);

	pplan->lefttree = pplanChild;
	pplan->nMotionNodes = pplanChild->nMotionNodes;

	// set sorting info
	psort->noduplicates = pdxlopSort->FDiscardDuplicates();

	// translate sorting columns
	// TODO: antovl - Jan 19, 2011; GPDB only supports nullsLast right now, so nullsFirst is unused
	psort->nullsFirst = NULL;

	const CDXLNode *pdxlnSortColList = (*pdxlnSort)[EdxlsortIndexSortColList];

	const ULONG ulNumCols = pdxlnSortColList->UlArity();
	psort->numCols = ulNumCols;
	psort->sortColIdx = (AttrNumber *) gpdb::GPDBAlloc(ulNumCols * sizeof(AttrNumber));
	psort->sortOperators = (Oid *) gpdb::GPDBAlloc(ulNumCols * sizeof(Oid));

	TranslateSortCols(pdxlnSortColList, &dxltrctxChild, psort->sortColIdx, psort->sortOperators);

	// translate limit information
	if (0 < pdxlnLimitCount->UlArity())
	{
		GPOS_ASSERT(1 == pdxlnLimitCount->UlArity());
		const CDXLNode *pdxlnLimitCountExpr = (*pdxlnLimitCount)[0];
		CMappingColIdVarPlStmt mapcidvarplstmt = CMappingColIdVarPlStmt
																(
																m_pmp,
																NULL,
																pdrgpdxltrctx,
																pdxltrctxOut,
																m_pctxdxltoplstmt,
																pplan
																);

		psort->limitCount = (Node *) m_pdxlsctranslator->PexprFromDXLNodeScalar(pdxlnLimitCountExpr, &mapcidvarplstmt);
	}

	if (0 < pdxlnLimitOffset->UlArity())
	{
		GPOS_ASSERT(1 == pdxlnLimitOffset->UlArity());
		const CDXLNode *pdxlnLimitOffsetExpr = (*pdxlnLimitOffset)[0];

		CMappingColIdVarPlStmt mapcidvarplstmt = CMappingColIdVarPlStmt
																(
																m_pmp,
																NULL,
																pdrgpdxltrctx,
																pdxltrctxOut,
																m_pctxdxltoplstmt,
																pplan
																);

		psort->limitOffset = (Node *) m_pdxlsctranslator->PexprFromDXLNodeScalar(pdxlnLimitOffsetExpr, &mapcidvarplstmt);
	}
	SetParamIds(pplan);

	// cleanup
	pdrgpdxltrctx->Release();

	return (Plan *) psort;
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorDXLToPlStmt::PsubqscanFromDXLSubqScan
//
//	@doc:
//		Translate DXL subquery scan node into GPDB SubqueryScan plan node
//
//---------------------------------------------------------------------------
Plan *
CTranslatorDXLToPlStmt::PsubqscanFromDXLSubqScan
	(
	const CDXLNode *pdxlnSubqScan,
	CDXLTranslateContext *pdxltrctxOut,
	Plan *pplanParent,
	DrgPdxltrctx *pdrgpdxltrctxPrevSiblings
	)
{
	// create sort plan node
	SubqueryScan *psubqscan = MakeNode(SubqueryScan);

	Plan *pplan = &(psubqscan->scan.plan);
	pplan->plan_node_id = m_pctxdxltoplstmt->UlNextPlanId();
	pplan->plan_parent_node_id = IPlanId(pplanParent);

	CDXLPhysicalSubqueryScan *pdxlopSubqscan = CDXLPhysicalSubqueryScan::PdxlopConvert(pdxlnSubqScan->Pdxlop());

	// translate operator costs
	TranslatePlanCosts
		(
		CDXLPhysicalProperties::PdxlpropConvert(pdxlnSubqScan->Pdxlprop())->Pdxlopcost(),
		&(pplan->startup_cost),
		&(pplan->total_cost),
		&(pplan->plan_rows),
		&(pplan->plan_width)
		);

	// translate subplan
	CDXLNode *pdxlnChild = (*pdxlnSubqScan)[EdxlsubqscanIndexChild];
	CDXLNode *pdxlnPrL = (*pdxlnSubqScan)[EdxlsubqscanIndexProjList];
	CDXLNode *pdxlnFilter = (*pdxlnSubqScan)[EdxlsubqscanIndexFilter];

	CDXLTranslateContext dxltrctxChild(m_pmp, false, pdxltrctxOut->PhmColParam());

	Plan *pplanChild = PplFromDXL(pdxlnChild, &dxltrctxChild, pplan, pdrgpdxltrctxPrevSiblings);

	// create an rtable entry for the subquery scan
	RangeTblEntry *prte = MakeNode(RangeTblEntry);
	prte->rtekind = RTE_SUBQUERY;

	Alias *palias = MakeNode(Alias);
	palias->colnames = NIL;

	// get table alias
	palias->aliasname = CTranslatorUtils::SzFromWsz(pdxlopSubqscan->Pmdname()->Pstr()->Wsz());

	// get column names from child project list
	CDXLTranslateContextBaseTable dxltrctxbt(m_pmp);

	Index iRel = gpdb::UlListLength(m_pctxdxltoplstmt->PlPrte()) + 1;
	(psubqscan->scan).scanrelid = iRel;
	dxltrctxbt.SetIdx(iRel);

	ListCell *plcTE = NULL;

	CDXLNode *pdxlnChildProjList = (*pdxlnChild)[0];

	ULONG ul = 0;

	ForEach (plcTE, pplanChild->targetlist)
	{
		TargetEntry *pte = (TargetEntry *) lfirst(plcTE);

		// non-system attribute
		CHAR *szColName = PStrDup(pte->resname);
		Value *pvalColName = gpdb::PvalMakeString(szColName);
		palias->colnames = gpdb::PlAppendElement(palias->colnames, pvalColName);

		// get corresponding child project element
		CDXLScalarProjElem *pdxlopPrel = CDXLScalarProjElem::PdxlopConvert((*pdxlnChildProjList)[ul]->Pdxlop());

		// save mapping col id -> index in translate context
		(void) dxltrctxbt.FInsertMapping(pdxlopPrel->UlId(), pte->resno);
		ul++;
	}

	prte->eref = palias;

	// add range table entry for the subquery to the list
	m_pctxdxltoplstmt->AddRTE(prte);

	// translate proj list and filter
	TranslateProjListAndFilter
		(
		pdxlnPrL,
		pdxlnFilter,
		&dxltrctxbt,		// translate context for the base table
		NULL,
		&pplan->targetlist,
		&pplan->qual,
		pdxltrctxOut,
		pplan
		);

	psubqscan->subplan = pplanChild;
	pplan->nMotionNodes = pplanChild->nMotionNodes;

	SetParamIds(pplan);
	return (Plan *) psubqscan;
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorDXLToPlStmt::PresultFromDXLResult
//
//	@doc:
//		Translate DXL result node into GPDB result plan node
//
//---------------------------------------------------------------------------
Plan *
CTranslatorDXLToPlStmt::PresultFromDXLResult
	(
	const CDXLNode *pdxlnResult,
	CDXLTranslateContext *pdxltrctxOut,
	Plan *pplanParent,
	DrgPdxltrctx *pdrgpdxltrctxPrevSiblings
	)
{
	// create result plan node
	Result *presult = MakeNode(Result);

	Plan *pplan = &(presult->plan);
	pplan->plan_node_id = m_pctxdxltoplstmt->UlNextPlanId();
	pplan->plan_parent_node_id = IPlanId(pplanParent);

	// translate operator costs
	TranslatePlanCosts
		(
		CDXLPhysicalProperties::PdxlpropConvert(pdxlnResult->Pdxlprop())->Pdxlopcost(),
		&(pplan->startup_cost),
		&(pplan->total_cost),
		&(pplan->plan_rows),
		&(pplan->plan_width)
		);

	pplan->nMotionNodes = 0;

	CDXLNode *pdxlnChild = NULL;
	CDXLTranslateContext dxltrctxChild(m_pmp, false, pdxltrctxOut->PhmColParam());

	if (pdxlnResult->UlArity() - 1 == EdxlresultIndexChild)
	{
		// translate child plan
		pdxlnChild = (*pdxlnResult)[EdxlresultIndexChild];

		Plan *pplanChild = PplFromDXL(pdxlnChild, &dxltrctxChild, pplan, pdrgpdxltrctxPrevSiblings);

		GPOS_ASSERT(NULL != pplanChild && "child plan cannot be NULL");

		presult->plan.lefttree = pplanChild;

		pplan->nMotionNodes = pplanChild->nMotionNodes;
	}

	CDXLNode *pdxlnPrL = (*pdxlnResult)[EdxlresultIndexProjList];
	CDXLNode *pdxlnFilter = (*pdxlnResult)[EdxlresultIndexFilter];
	CDXLNode *pdxlnOneTimeFilter = (*pdxlnResult)[EdxlresultIndexOneTimeFilter];

	List *plQuals = NULL;

	DrgPdxltrctx *pdrgpdxltrctx = GPOS_NEW(m_pmp) DrgPdxltrctx(m_pmp);
	pdrgpdxltrctx->Append(const_cast<CDXLTranslateContext*>(&dxltrctxChild));

	// translate proj list and filter
	TranslateProjListAndFilter
		(
		pdxlnPrL,
		pdxlnFilter,
		NULL,		// translate context for the base table
		pdrgpdxltrctx,
		&pplan->targetlist,
		&plQuals,
		pdxltrctxOut,
		pplan
		);

	// translate one time filter
	List *plOneTimeQuals = PlQualFromFilter
							(
							pdxlnOneTimeFilter,
							NULL,			// base table translation context
							pdrgpdxltrctx,
							pdxltrctxOut,
							pplan
							);

	pplan->qual = plQuals;

	presult->resconstantqual = (Node *) plOneTimeQuals;

	SetParamIds(pplan);

	// cleanup
	pdrgpdxltrctx->Release();

	return (Plan *) presult;
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorDXLToPlStmt::PplanPartitionSelector
//
//	@doc:
//		Translate DXL PartitionSelector into a GPDB PartitionSelector node
//
//---------------------------------------------------------------------------
Plan *
CTranslatorDXLToPlStmt::PplanPartitionSelector
	(
	const CDXLNode *pdxlnPartitionSelector,
	CDXLTranslateContext *pdxltrctxOut,
	Plan *pplanParent,
	DrgPdxltrctx *pdrgpdxltrctxPrevSiblings
	)
{
	PartitionSelector *ppartsel = MakeNode(PartitionSelector);

	Plan *pplan = &(ppartsel->plan);
	pplan->plan_node_id = m_pctxdxltoplstmt->UlNextPlanId();
	pplan->plan_parent_node_id = IPlanId(pplanParent);

	CDXLPhysicalPartitionSelector *pdxlopPartSel = CDXLPhysicalPartitionSelector::PdxlopConvert(pdxlnPartitionSelector->Pdxlop());
	const ULONG ulLevels = pdxlopPartSel->UlLevels();
	ppartsel->nLevels = ulLevels;
	ppartsel->scanId = pdxlopPartSel->UlScanId();
	ppartsel->relid = CMDIdGPDB::PmdidConvert(pdxlopPartSel->PmdidRel())->OidObjectId();
	ppartsel->selectorId = m_ulPartitionSelectorCounter++;

	// translate operator costs
	TranslatePlanCosts
		(
		CDXLPhysicalProperties::PdxlpropConvert(pdxlnPartitionSelector->Pdxlprop())->Pdxlopcost(),
		&(pplan->startup_cost),
		&(pplan->total_cost),
		&(pplan->plan_rows),
		&(pplan->plan_width)
		);

	pplan->nMotionNodes = 0;

	CDXLNode *pdxlnChild = NULL;
	DrgPdxltrctx *pdrgpdxltrctx = GPOS_NEW(m_pmp) DrgPdxltrctx(m_pmp);
	DrgPdxltrctx *pdrgpdxltrctxWithSiblings = GPOS_NEW(m_pmp) DrgPdxltrctx(m_pmp);

	CDXLTranslateContext dxltrctxChild(m_pmp, false, pdxltrctxOut->PhmColParam());

	BOOL fHasChild = (EdxlpsIndexChild == pdxlnPartitionSelector->UlArity() - 1);
	if (fHasChild)
	{
		// translate child plan
		pdxlnChild = (*pdxlnPartitionSelector)[EdxlpsIndexChild];

		Plan *pplanChild = PplFromDXL(pdxlnChild, &dxltrctxChild, pplan, pdrgpdxltrctxPrevSiblings);
		GPOS_ASSERT(NULL != pplanChild && "child plan cannot be NULL");

		ppartsel->plan.lefttree = pplanChild;
		pplan->nMotionNodes = pplanChild->nMotionNodes;
	}

	pdrgpdxltrctx->Append(&dxltrctxChild);
	pdrgpdxltrctx->Append(pdxltrctxOut);

	pdrgpdxltrctxWithSiblings->AppendArray(pdrgpdxltrctx);
	if (NULL != pdrgpdxltrctxPrevSiblings)
	{
		pdrgpdxltrctxWithSiblings->AppendArray(pdrgpdxltrctxPrevSiblings);
	}

	CDXLNode *pdxlnPrL = (*pdxlnPartitionSelector)[EdxlpsIndexProjList];
	CDXLNode *pdxlnEqFilters = (*pdxlnPartitionSelector)[EdxlpsIndexEqFilters];
	CDXLNode *pdxlnFilters = (*pdxlnPartitionSelector)[EdxlpsIndexFilters];
	CDXLNode *pdxlnResidualFilter = (*pdxlnPartitionSelector)[EdxlpsIndexResidualFilter];
	CDXLNode *pdxlnPropExpr = (*pdxlnPartitionSelector)[EdxlpsIndexPropExpr];
	CDXLNode *pdxlnPrintableFilter = (*pdxlnPartitionSelector)[EdxlpsIndexPrintableFilter];

	// translate proj list
	pplan->targetlist = PlTargetListFromProjList(pdxlnPrL, NULL /*pdxltrctxbt*/, pdrgpdxltrctx, pdxltrctxOut, pplan);

	// translate filter lists
	GPOS_ASSERT(pdxlnEqFilters->UlArity() == ulLevels);
	ppartsel->levelEqExpressions = PlFilterList(pdxlnEqFilters, NULL /*pdxltrctxbt*/, pdrgpdxltrctx, pdxltrctxOut, pplan);

	GPOS_ASSERT(pdxlnFilters->UlArity() == ulLevels);
	ppartsel->levelExpressions = PlFilterList(pdxlnFilters, NULL /*pdxltrctxbt*/, pdrgpdxltrctx, pdxltrctxOut, pplan);

	//translate residual filter
	CMappingColIdVarPlStmt mapcidvarplstmt = CMappingColIdVarPlStmt(m_pmp, NULL /*pdxltrctxbt*/, pdrgpdxltrctxWithSiblings, pdxltrctxOut, m_pctxdxltoplstmt, pplan);
	if (!m_pdxlsctranslator->FConstTrue(pdxlnResidualFilter, m_pmda))
	{
		ppartsel->residualPredicate = (Node *) m_pdxlsctranslator->PexprFromDXLNodeScalar(pdxlnResidualFilter, &mapcidvarplstmt);
	}

	//translate propagation expression
	if (!m_pdxlsctranslator->FConstNull(pdxlnPropExpr))
	{
		ppartsel->propagationExpression = (Node *) m_pdxlsctranslator->PexprFromDXLNodeScalar(pdxlnPropExpr, &mapcidvarplstmt);
	}

	//translate printable filter
	if (!m_pdxlsctranslator->FConstTrue(pdxlnPrintableFilter, m_pmda))
	{
		ppartsel->printablePredicate = (Node *) m_pdxlsctranslator->PexprFromDXLNodeScalar(pdxlnPrintableFilter, &mapcidvarplstmt);
	}

	ppartsel->staticPartOids = NIL;
	ppartsel->staticScanIds = NIL;
	ppartsel->staticSelection = (optimizer_static_partition_selection && !fHasChild);

	if (ppartsel->staticSelection)
	{
		SelectedParts *sp = gpdb::SpStaticPartitionSelection(ppartsel);
		ppartsel->staticPartOids = sp->partOids;
		ppartsel->staticScanIds = sp->scanIds;
		gpdb::GPDBFree(sp);
	}
	else
	{
		// if we cannot do static elimination then add this partitioned table oid
		// to the planned stmt so we can ship the constraints with the plan
		m_pctxdxltoplstmt->AddPartitionedTable(ppartsel->relid);
	}

	// increment the number of partition selectors for the given scan id
	m_pctxdxltoplstmt->IncrementPartitionSelectors(ppartsel->scanId);

	SetParamIds(pplan);

	// cleanup
	pdrgpdxltrctx->Release();
	pdrgpdxltrctxWithSiblings->Release();

	return (Plan *) ppartsel;
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorDXLToPlStmt::PlFilterList
//
//	@doc:
//		Translate DXL filter list into GPDB filter list
//
//---------------------------------------------------------------------------
List *
CTranslatorDXLToPlStmt::PlFilterList
	(
	const CDXLNode *pdxlnFilterList,
	const CDXLTranslateContextBaseTable *pdxltrctxbt,
	DrgPdxltrctx *pdrgpdxltrctx,
	CDXLTranslateContext *pdxltrctxOut,
	Plan *pplanParent
	)
{
	GPOS_ASSERT(EdxlopScalarOpList == pdxlnFilterList->Pdxlop()->Edxlop());

	List *plFilters = NIL;

	CMappingColIdVarPlStmt mapcidvarplstmt = CMappingColIdVarPlStmt(m_pmp, pdxltrctxbt, pdrgpdxltrctx, pdxltrctxOut, m_pctxdxltoplstmt, pplanParent);
	const ULONG ulArity = pdxlnFilterList->UlArity();
	for (ULONG ul = 0; ul < ulArity; ul++)
	{
		CDXLNode *pdxlnChildFilter = (*pdxlnFilterList)[ul];

		if (m_pdxlsctranslator->FConstTrue(pdxlnChildFilter, m_pmda))
		{
			plFilters = gpdb::PlAppendElement(plFilters, NULL /*datum*/);
			continue;
		}

		Expr *pexprFilter = m_pdxlsctranslator->PexprFromDXLNodeScalar(pdxlnChildFilter, &mapcidvarplstmt);
		plFilters = gpdb::PlAppendElement(plFilters, pexprFilter);
	}

	return plFilters;
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorDXLToPlStmt::PappendFromDXLAppend
//
//	@doc:
//		Translate DXL append node into GPDB Append plan node
//
//---------------------------------------------------------------------------
Plan *
CTranslatorDXLToPlStmt::PappendFromDXLAppend
	(
	const CDXLNode *pdxlnAppend,
	CDXLTranslateContext *pdxltrctxOut,
	Plan *pplanParent,
	DrgPdxltrctx *pdrgpdxltrctxPrevSiblings
	)
{
	// create append plan node
	Append *pappend = MakeNode(Append);

	Plan *pplan = &(pappend->plan);
	pplan->plan_node_id = m_pctxdxltoplstmt->UlNextPlanId();
	pplan->plan_parent_node_id = IPlanId(pplanParent);

	CDXLPhysicalAppend *pdxlopAppend = CDXLPhysicalAppend::PdxlopConvert(pdxlnAppend->Pdxlop());

	pappend->isTarget = pdxlopAppend->FIsTarget();
	pappend->isZapped = pdxlopAppend->FIsZapped();

	// translate operator costs
	TranslatePlanCosts
		(
		CDXLPhysicalProperties::PdxlpropConvert(pdxlnAppend->Pdxlprop())->Pdxlopcost(),
		&(pplan->startup_cost),
		&(pplan->total_cost),
		&(pplan->plan_rows),
		&(pplan->plan_width)
		);

	const ULONG ulArity = pdxlnAppend->UlArity();
	GPOS_ASSERT(EdxlappendIndexFirstChild < ulArity);
	pplan->nMotionNodes = 0;
	pappend->appendplans = NIL;
	
	// translate children
	CDXLTranslateContext dxltrctxChild(m_pmp, false, pdxltrctxOut->PhmColParam());
	for (ULONG ul = EdxlappendIndexFirstChild; ul < ulArity; ul++)
	{
		CDXLNode *pdxlnChild = (*pdxlnAppend)[ul];

		Plan *pplanChild = PplFromDXL(pdxlnChild, &dxltrctxChild, pplan, pdrgpdxltrctxPrevSiblings);

		GPOS_ASSERT(NULL != pplanChild && "child plan cannot be NULL");

		pappend->appendplans = gpdb::PlAppendElement(pappend->appendplans, pplanChild);
		pplan->nMotionNodes += pplanChild->nMotionNodes;
	}

	CDXLNode *pdxlnPrL = (*pdxlnAppend)[EdxlappendIndexProjList];
	CDXLNode *pdxlnFilter = (*pdxlnAppend)[EdxlappendIndexFilter];

	pplan->targetlist = NIL;
	const ULONG ulLen = pdxlnPrL->UlArity();
	for (ULONG ul = 0; ul < ulLen; ++ul)
	{
		CDXLNode *pdxlnPrEl = (*pdxlnPrL)[ul];
		GPOS_ASSERT(EdxlopScalarProjectElem == pdxlnPrEl->Pdxlop()->Edxlop());

		CDXLScalarProjElem *pdxlopPrel = CDXLScalarProjElem::PdxlopConvert(pdxlnPrEl->Pdxlop());
		GPOS_ASSERT(1 == pdxlnPrEl->UlArity());

		// translate proj element expression
		CDXLNode *pdxlnExpr = (*pdxlnPrEl)[0];
		CDXLScalarIdent *pdxlopScIdent = CDXLScalarIdent::PdxlopConvert(pdxlnExpr->Pdxlop());

		Index idxVarno = OUTER;
		AttrNumber attno = (AttrNumber) (ul + 1);

		Var *pvar = gpdb::PvarMakeVar
							(
							idxVarno,
							attno,
							CMDIdGPDB::PmdidConvert(pdxlopScIdent->PmdidType())->OidObjectId(),
							-1,	// vartypmod
							0	// varlevelsup
							);

		TargetEntry *pte = MakeNode(TargetEntry);
		pte->expr = (Expr *) pvar;
		pte->resname = CTranslatorUtils::SzFromWsz(pdxlopScIdent->Pdxlcr()->Pmdname()->Pstr()->Wsz());
		pte->resno = attno;

		// add column mapping to output translation context
		pdxltrctxOut->InsertMapping(pdxlopPrel->UlId(), pte);

		pplan->targetlist = gpdb::PlAppendElement(pplan->targetlist, pte);
	}

	DrgPdxltrctx *pdrgpdxltrctx = GPOS_NEW(m_pmp) DrgPdxltrctx(m_pmp);
	pdrgpdxltrctx->Append(const_cast<CDXLTranslateContext*>(pdxltrctxOut));

	// translate filter
	pplan->qual = PlQualFromFilter
					(
					pdxlnFilter,
					NULL, // translate context for the base table
					pdrgpdxltrctx,
					pdxltrctxOut,
					pplanParent
					);

	SetParamIds(pplan);

	// cleanup
	pdrgpdxltrctx->Release();

	return (Plan *) pappend;
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorDXLToPlStmt::PmatFromDXLMaterialize
//
//	@doc:
//		Translate DXL materialize node into GPDB Material plan node
//
//---------------------------------------------------------------------------
Plan *
CTranslatorDXLToPlStmt::PmatFromDXLMaterialize
	(
	const CDXLNode *pdxlnMaterialize,
	CDXLTranslateContext *pdxltrctxOut,
	Plan *pplanParent,
	DrgPdxltrctx *pdrgpdxltrctxPrevSiblings
	)
{
	// create materialize plan node
	Material *pmat = MakeNode(Material);

	Plan *pplan = &(pmat->plan);
	pplan->plan_node_id = m_pctxdxltoplstmt->UlNextPlanId();
	pplan->plan_parent_node_id = IPlanId(pplanParent);

	CDXLPhysicalMaterialize *pdxlopMat = CDXLPhysicalMaterialize::PdxlopConvert(pdxlnMaterialize->Pdxlop());

	pmat->cdb_strict = pdxlopMat->FEager();

	// translate operator costs
	TranslatePlanCosts
		(
		CDXLPhysicalProperties::PdxlpropConvert(pdxlnMaterialize->Pdxlprop())->Pdxlopcost(),
		&(pplan->startup_cost),
		&(pplan->total_cost),
		&(pplan->plan_rows),
		&(pplan->plan_width)
		);

	// translate materialize child
	CDXLNode *pdxlnChild = (*pdxlnMaterialize)[EdxlmatIndexChild];

	CDXLNode *pdxlnPrL = (*pdxlnMaterialize)[EdxlmatIndexProjList];
	CDXLNode *pdxlnFilter = (*pdxlnMaterialize)[EdxlmatIndexFilter];

	CDXLTranslateContext dxltrctxChild(m_pmp, false, pdxltrctxOut->PhmColParam());

	Plan *pplanChild = PplFromDXL(pdxlnChild, &dxltrctxChild, pplan, pdrgpdxltrctxPrevSiblings);

	DrgPdxltrctx *pdrgpdxltrctx = GPOS_NEW(m_pmp) DrgPdxltrctx(m_pmp);
	pdrgpdxltrctx->Append(const_cast<CDXLTranslateContext*>(&dxltrctxChild));

	// translate proj list and filter
	TranslateProjListAndFilter
		(
		pdxlnPrL,
		pdxlnFilter,
		NULL,			// translate context for the base table
		pdrgpdxltrctx,
		&pplan->targetlist,
		&pplan->qual,
		pdxltrctxOut,
		pplan
		);

	pplan->lefttree = pplanChild;
	pplan->nMotionNodes = pplanChild->nMotionNodes;

	// set spooling info
	if (pdxlopMat->FSpooling())
	{
		pmat->share_id = pdxlopMat->UlSpoolId();
		pmat->driver_slice = pdxlopMat->IExecutorSlice();
		pmat->nsharer_xslice = pdxlopMat->UlConsumerSlices();
		pmat->share_type = (0 < pdxlopMat->UlConsumerSlices()) ?
							SHARE_MATERIAL_XSLICE : SHARE_MATERIAL;
	}
	else
	{
		pmat->share_type = SHARE_NOTSHARED;
	}

	SetParamIds(pplan);

	// cleanup
	pdrgpdxltrctx->Release();

	return (Plan *) pmat;
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorDXLToPlStmt::PshscanFromDXLSharedScan
//
//	@doc:
//		Translate DXL materialize node into GPDB Material plan node
//
//---------------------------------------------------------------------------
Plan *
CTranslatorDXLToPlStmt::PshscanFromDXLSharedScan
	(
	const CDXLNode *pdxlnSharedScan,
	CDXLTranslateContext *pdxltrctxOut,
	Plan *pplanParent,
	DrgPdxltrctx *pdrgpdxltrctxPrevSiblings
	)
{
	// create sort plan node
	ShareInputScan *pshscan = MakeNode(ShareInputScan);

	CDXLPhysicalSharedScan *pdxlopShscan = CDXLPhysicalSharedScan::PdxlopConvert(pdxlnSharedScan->Pdxlop());

	// set spooling info
	const CDXLSpoolInfo *pspoolinfo = pdxlopShscan->Pspoolinfo();

	pshscan->share_id = pspoolinfo->UlSpoolId();
	pshscan->driver_slice = pspoolinfo->IExecutorSlice();
	pshscan->share_type = ShtypeFromSpoolInfo(pspoolinfo);

	GPOS_ASSERT(SHARE_NOTSHARED != pshscan->share_type);

	Plan *pplan = &(pshscan->plan);
	pplan->plan_node_id = m_pctxdxltoplstmt->UlNextPlanId();
	pplan->plan_parent_node_id = IPlanId(pplanParent);

	// translate operator costs
	TranslatePlanCosts
		(
		CDXLPhysicalProperties::PdxlpropConvert(pdxlnSharedScan->Pdxlprop())->Pdxlopcost(),
		&(pplan->startup_cost),
		&(pplan->total_cost),
		&(pplan->plan_rows),
		&(pplan->plan_width)
		);

	CDXLTranslateContext *pdxltrctxChild = NULL;

	if (EdxlshscanIndexSentinel == pdxlnSharedScan->UlArity())
	{
		// translate shared scan child
		CDXLNode *pdxlnChild = (*pdxlnSharedScan)[EdxlshscanIndexChild];
		pdxltrctxChild = GPOS_NEW(m_pmp) CDXLTranslateContext(m_pmp, false, pdxltrctxOut->PhmColParam());

		Plan *pplanChild = PplFromDXL(pdxlnChild, pdxltrctxChild, pplan, pdrgpdxltrctxPrevSiblings);

		pplan->lefttree = pplanChild;
		pplan->nMotionNodes = pplanChild->nMotionNodes;

		// store translation context
		m_pctxdxltoplstmt->AddSharedScanTranslationContext(pspoolinfo->UlSpoolId(), pdxltrctxChild);
	}
	else
	{
		// no direct child: we must have translated the actual spool for this shared scan already
		// find the corresponding translation context
		pdxltrctxChild = const_cast<CDXLTranslateContext*>(m_pctxdxltoplstmt->PdxltrctxForSharedScan(pspoolinfo->UlSpoolId()));
	}

	GPOS_ASSERT(NULL != pdxltrctxChild);

	CDXLNode *pdxlnPrL = (*pdxlnSharedScan)[EdxlshscanIndexProjList];
	CDXLNode *pdxlnFilter = (*pdxlnSharedScan)[EdxlshscanIndexFilter];

	DrgPdxltrctx *pdrgpdxltrctx = GPOS_NEW(m_pmp) DrgPdxltrctx(m_pmp);
	pdrgpdxltrctx->Append(pdxltrctxChild);

	// translate proj list and filter
	TranslateProjListAndFilter
		(
		pdxlnPrL,
		pdxlnFilter,
		NULL,			// translate context for the base table
		pdrgpdxltrctx,
		&pplan->targetlist,
		&pplan->qual,
		pdxltrctxOut,
		pplan
		);

	SetParamIds(pplan);

	// cleanup
	pdrgpdxltrctx->Release();

	return (Plan *) pshscan;
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorDXLToPlStmt::PshscanFromDXLCTEProducer
//
//	@doc:
//		Translate DXL CTE Producer node into GPDB share input scan plan node
//
//---------------------------------------------------------------------------
Plan *
CTranslatorDXLToPlStmt::PshscanFromDXLCTEProducer
	(
	const CDXLNode *pdxlnCTEProducer,
	CDXLTranslateContext *pdxltrctxOut,
	Plan *pplanParent,
	DrgPdxltrctx *pdrgpdxltrctxPrevSiblings
	)
{
	CDXLPhysicalCTEProducer *pdxlopCTEProducer = CDXLPhysicalCTEProducer::PdxlopConvert(pdxlnCTEProducer->Pdxlop());
	ULONG ulCTEId = pdxlopCTEProducer->UlId();

	// create the shared input scan representing the CTE Producer
	ShareInputScan *pshscanCTEProducer = MakeNode(ShareInputScan);
	pshscanCTEProducer->share_id = ulCTEId;
	Plan *pplan = &(pshscanCTEProducer->plan);
	pplan->plan_node_id = m_pctxdxltoplstmt->UlNextPlanId();
	pplan->plan_parent_node_id = IPlanId(pplanParent);

	// store share scan node for the translation of CTE Consumers
	m_pctxdxltoplstmt->AddCTEConsumerInfo(ulCTEId, pshscanCTEProducer);

	// translate cost of the producer
	TranslatePlanCosts
		(
		CDXLPhysicalProperties::PdxlpropConvert(pdxlnCTEProducer->Pdxlprop())->Pdxlopcost(),
		&(pplan->startup_cost),
		&(pplan->total_cost),
		&(pplan->plan_rows),
		&(pplan->plan_width)
		);

	// translate child plan
	CDXLNode *pdxlnPrL = (*pdxlnCTEProducer)[0];
	CDXLNode *pdxlnChild = (*pdxlnCTEProducer)[1];

	CDXLTranslateContext dxltrctxChild(m_pmp, false, pdxltrctxOut->PhmColParam());
	Plan *pplanChild = PplFromDXL(pdxlnChild, &dxltrctxChild, pplan, pdrgpdxltrctxPrevSiblings);
	GPOS_ASSERT(NULL != pplanChild && "child plan cannot be NULL");

	DrgPdxltrctx *pdrgpdxltrctx = GPOS_NEW(m_pmp) DrgPdxltrctx(m_pmp);
	pdrgpdxltrctx->Append(&dxltrctxChild);
	// translate proj list
	pplan->targetlist = PlTargetListFromProjList
							(
							pdxlnPrL,
							NULL,		// base table translation context
							pdrgpdxltrctx,
							pdxltrctxOut,
							pplan
							);

	// if the child node is neither a sort or materialize node then add a materialize node
	if (!IsA(pplanChild, Material) && !IsA(pplanChild, Sort))
	{
		Material *pmat = MakeNode(Material);
		pmat->cdb_strict = false; // eager-free

		Plan *pplanMat = &(pmat->plan);
		pplanMat->plan_node_id = m_pctxdxltoplstmt->UlNextPlanId();
		pplanMat->plan_parent_node_id = pplan->plan_node_id ;

		TranslatePlanCosts
			(
			CDXLPhysicalProperties::PdxlpropConvert(pdxlnCTEProducer->Pdxlprop())->Pdxlopcost(),
			&(pplanMat->startup_cost),
			&(pplanMat->total_cost),
			&(pplanMat->plan_rows),
			&(pplanMat->plan_width)
			);

		// create a target list for the newly added materialize
		ListCell *plcTe = NULL;
		pplanMat->targetlist = NIL;
		ForEach (plcTe, pplan->targetlist)
		{
			TargetEntry *pte = (TargetEntry *) lfirst(plcTe);
			Expr *pexpr = pte->expr;
			GPOS_ASSERT(IsA(pexpr, Var));

			Var *pvar = (Var *) pexpr;
			Var *pvarNew = gpdb::PvarMakeVar(OUTER, pvar->varattno, pvar->vartype, -1 /* vartypmod */,	0 /* varlevelsup */);
			pvarNew->varnoold = pvar->varnoold;
			pvarNew->varoattno = pvar->varoattno;

			TargetEntry *pteNew = gpdb::PteMakeTargetEntry((Expr *) pvarNew, pvar->varattno, PStrDup(pte->resname), pte->resjunk);
			pplanMat->targetlist = gpdb::PlAppendElement(pplanMat->targetlist, pteNew);
		}

		pplanMat->lefttree = pplanChild;
		pplanMat->nMotionNodes = pplanChild->nMotionNodes;

		pplanChild = pplanMat;
	}

	InitializeSpoolingInfo(pplanChild, ulCTEId);

	pplan->lefttree = pplanChild;
	pplan->nMotionNodes = pplanChild->nMotionNodes;
	pplan->qual = NIL;
	SetParamIds(pplan);

	// cleanup
	pdrgpdxltrctx->Release();

	return (Plan *) pshscanCTEProducer;
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorDXLToPlStmt::InitializeSpoolingInfo
//
//	@doc:
//		Initialize spooling information for (1) the materialize/sort node under the
//		shared input scan nodes representing the CTE producer node and
//		(2) SIS nodes representing the producer/consumer nodes
//---------------------------------------------------------------------------
void
CTranslatorDXLToPlStmt::InitializeSpoolingInfo
	(
	Plan *pplan,
	ULONG ulShareId
	)
{
	List *plshscanCTEConsumer = m_pctxdxltoplstmt->PshscanCTEConsumer(ulShareId);
	GPOS_ASSERT(NULL != plshscanCTEConsumer);

	Flow *pflow = PflowCTEConsumer(plshscanCTEConsumer);

	const ULONG ulLenSis = gpdb::UlListLength(plshscanCTEConsumer);

	ShareType share_type = SHARE_NOTSHARED;

	if (IsA(pplan, Material))
	{
		Material *pmat = (Material *) pplan;
		pmat->share_id = ulShareId;
		pmat->nsharer = ulLenSis;
		share_type = SHARE_MATERIAL;
		// the share_type is later reset to SHARE_MATERIAL_XSLICE (if needed) by the apply_shareinput_xslice
		pmat->share_type = share_type;
		GPOS_ASSERT(NULL == (pmat->plan).flow);
		(pmat->plan).flow = pflow;
	}
	else
	{
		GPOS_ASSERT(IsA(pplan, Sort));
		Sort *psort = (Sort *) pplan;
		psort->share_id = ulShareId;
		psort->nsharer = ulLenSis;
		share_type = SHARE_SORT;
		// the share_type is later reset to SHARE_SORT_XSLICE (if needed) the apply_shareinput_xslice
		psort->share_type = share_type;
		GPOS_ASSERT(NULL == (psort->plan).flow);
		(psort->plan).flow = pflow;
	}

	GPOS_ASSERT(SHARE_NOTSHARED != share_type);

	// set the share type of the consumer nodes based on the producer
	ListCell *plcShscanCTEConsumer = NULL;
	ForEach (plcShscanCTEConsumer, plshscanCTEConsumer)
	{
		ShareInputScan *pshscanConsumer = (ShareInputScan *) lfirst(plcShscanCTEConsumer);
		pshscanConsumer->share_type = share_type;
		pshscanConsumer->driver_slice = -1; // default
		if (NULL == (pshscanConsumer->plan).flow)
		{
			(pshscanConsumer->plan).flow = (Flow *) gpdb::PvCopyObject(pflow);
		}
	}
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorDXLToPlStmt::PflowCTEConsumer
//
//	@doc:
//		Retrieve the flow of the shared input scan of the cte consumers. If
//		multiple CTE consumers have a flow then ensure that they are of the
//		same type
//---------------------------------------------------------------------------
Flow *
CTranslatorDXLToPlStmt::PflowCTEConsumer
	(
	List *plshscanCTEConsumer
	)
{
	Flow *pflow = NULL;

	ListCell *plcShscanCTEConsumer = NULL;
	ForEach (plcShscanCTEConsumer, plshscanCTEConsumer)
	{
		ShareInputScan *pshscanConsumer = (ShareInputScan *) lfirst(plcShscanCTEConsumer);
		Flow *pflowCte = (pshscanConsumer->plan).flow;
		if (NULL != pflowCte)
		{
			if (NULL == pflow)
			{
				pflow = (Flow *) gpdb::PvCopyObject(pflowCte);
			}
			else
			{
				GPOS_ASSERT(pflow->flotype == pflowCte->flotype);
			}
		}
	}

	if (NULL == pflow)
	{
		pflow = MakeNode(Flow);
		pflow->flotype = FLOW_UNDEFINED; // default flow
	}

	return pflow;
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorDXLToPlStmt::PshscanFromDXLCTEConsumer
//
//	@doc:
//		Translate DXL CTE Consumer node into GPDB share input scan plan node
//
//---------------------------------------------------------------------------
Plan *
CTranslatorDXLToPlStmt::PshscanFromDXLCTEConsumer
	(
	const CDXLNode *pdxlnCTEConsumer,
	CDXLTranslateContext *pdxltrctxOut,
	Plan *pplanParent,
	DrgPdxltrctx *pdrgpdxltrctxPrevSiblings
	)
{
	CDXLPhysicalCTEConsumer *pdxlopCTEConsumer = CDXLPhysicalCTEConsumer::PdxlopConvert(pdxlnCTEConsumer->Pdxlop());
	ULONG ulCTEId = pdxlopCTEConsumer->UlId();

	ShareInputScan *pshscanCTEConsumer = MakeNode(ShareInputScan);
	pshscanCTEConsumer->share_id = ulCTEId;

	Plan *pplan = &(pshscanCTEConsumer->plan);
	pplan->plan_node_id = m_pctxdxltoplstmt->UlNextPlanId();
	pplan->plan_parent_node_id = IPlanId(pplanParent);

	// translate operator costs
	TranslatePlanCosts
		(
		CDXLPhysicalProperties::PdxlpropConvert(pdxlnCTEConsumer->Pdxlprop())->Pdxlopcost(),
		&(pplan->startup_cost),
		&(pplan->total_cost),
		&(pplan->plan_rows),
		&(pplan->plan_width)
		);

#ifdef GPOS_DEBUG
	DrgPul *pdrgpulCTEColId = pdxlopCTEConsumer->PdrgpulColIds();
#endif

	// generate the target list of the CTE Consumer
	pplan->targetlist = NIL;
	CDXLNode *pdxlnPrL = (*pdxlnCTEConsumer)[0];
	const ULONG ulLenPrL = pdxlnPrL->UlArity();
	GPOS_ASSERT(ulLenPrL == pdrgpulCTEColId->UlLength());
	for (ULONG ul = 0; ul < ulLenPrL; ul++)
	{
		CDXLNode *pdxlnPrE = (*pdxlnPrL)[ul];
		CDXLScalarProjElem *pdxlopPrE = CDXLScalarProjElem::PdxlopConvert(pdxlnPrE->Pdxlop());
		ULONG ulColId = pdxlopPrE->UlId();
		GPOS_ASSERT(ulColId == *(*pdrgpulCTEColId)[ul]);

		CDXLNode *pdxlnScIdent = (*pdxlnPrE)[0];
		CDXLScalarIdent *pdxlopScIdent = CDXLScalarIdent::PdxlopConvert(pdxlnScIdent->Pdxlop());
		OID oidType = CMDIdGPDB::PmdidConvert(pdxlopScIdent->PmdidType())->OidObjectId();

		Var *pvar = gpdb::PvarMakeVar(OUTER, (AttrNumber) (ul + 1), oidType, -1 /* vartypmod */,  0	/* varlevelsup */);

		CHAR *szResname = CTranslatorUtils::SzFromWsz(pdxlopPrE->PmdnameAlias()->Pstr()->Wsz());
		TargetEntry *pte = gpdb::PteMakeTargetEntry((Expr *) pvar, (AttrNumber) (ul + 1), szResname, false /* resjunk */);
		pplan->targetlist = gpdb::PlAppendElement(pplan->targetlist, pte);

		pdxltrctxOut->InsertMapping(ulColId, pte);
	}

	pplan->qual = NULL;

	SetParamIds(pplan);

	// store share scan node for the translation of CTE Consumers
	m_pctxdxltoplstmt->AddCTEConsumerInfo(ulCTEId, pshscanCTEConsumer);

	return (Plan *) pshscanCTEConsumer;
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorDXLToPlStmt::PplanSequence
//
//	@doc:
//		Translate DXL sequence node into GPDB Sequence plan node
//
//---------------------------------------------------------------------------
Plan *
CTranslatorDXLToPlStmt::PplanSequence
	(
	const CDXLNode *pdxlnSequence,
	CDXLTranslateContext *pdxltrctxOut,
	Plan *pplanParent,
	DrgPdxltrctx *pdrgpdxltrctxPrevSiblings
	)
{
	// create append plan node
	Sequence *psequence = MakeNode(Sequence);

	Plan *pplan = &(psequence->plan);
	pplan->plan_node_id = m_pctxdxltoplstmt->UlNextPlanId();
	pplan->plan_parent_node_id = IPlanId(pplanParent);

	// translate operator costs
	TranslatePlanCosts
		(
		CDXLPhysicalProperties::PdxlpropConvert(pdxlnSequence->Pdxlprop())->Pdxlopcost(),
		&(pplan->startup_cost),
		&(pplan->total_cost),
		&(pplan->plan_rows),
		&(pplan->plan_width)
		);

	ULONG ulArity = pdxlnSequence->UlArity();
	
	// translate last child
	// since last child may be a DynamicIndexScan with outer references,
	// we pass the context received from parent to translate outer refs here

	CDXLNode *pdxlnLastChild = (*pdxlnSequence)[ulArity - 1];

	CDXLTranslateContext dxltrctxChild(m_pmp, false, pdxltrctxOut->PhmColParam());

	Plan *pplanLastChild = PplFromDXL(pdxlnLastChild, &dxltrctxChild, pplan, pdrgpdxltrctxPrevSiblings);
	pplan->nMotionNodes = pplanLastChild->nMotionNodes;

	CDXLNode *pdxlnPrL = (*pdxlnSequence)[0];

	DrgPdxltrctx *pdrgpdxltrctx = GPOS_NEW(m_pmp) DrgPdxltrctx(m_pmp);
	pdrgpdxltrctx->Append(const_cast<CDXLTranslateContext*>(&dxltrctxChild));

	// translate proj list
	pplan->targetlist = PlTargetListFromProjList
						(
						pdxlnPrL,
						NULL,		// base table translation context
						pdrgpdxltrctx,
						pdxltrctxOut,
						pplan
						);

	// translate the rest of the children
	for (ULONG ul = 1; ul < ulArity - 1; ul++)
	{
		CDXLNode *pdxlnChild = (*pdxlnSequence)[ul];

		Plan *pplanChild = PplFromDXL(pdxlnChild, &dxltrctxChild, pplan, pdrgpdxltrctxPrevSiblings);

		psequence->subplans = gpdb::PlAppendElement(psequence->subplans, pplanChild);
		pplan->nMotionNodes += pplanChild->nMotionNodes;
	}

	psequence->subplans = gpdb::PlAppendElement(psequence->subplans, pplanLastChild);

	SetParamIds(pplan);

	// cleanup
	pdrgpdxltrctx->Release();

	return (Plan *) psequence;
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorDXLToPlStmt::PplanDTS
//
//	@doc:
//		Translates a DXL dynamic table scan node into a DynamicTableScan node
//
//---------------------------------------------------------------------------
Plan *
CTranslatorDXLToPlStmt::PplanDTS
	(
	const CDXLNode *pdxlnDTS,
	CDXLTranslateContext *pdxltrctxOut,
	Plan *pplanParent,
	DrgPdxltrctx *pdrgpdxltrctxPrevSiblings
	)
{
	// translate table descriptor into a range table entry
	CDXLPhysicalDynamicTableScan *pdxlop = CDXLPhysicalDynamicTableScan::PdxlopConvert(pdxlnDTS->Pdxlop());

	// translation context for column mappings in the base relation
	CDXLTranslateContextBaseTable dxltrctxbt(m_pmp);

	// add the new range table entry as the last element of the range table
	Index iRel = gpdb::UlListLength(m_pctxdxltoplstmt->PlPrte()) + 1;

	const IMDRelation *pmdrel = m_pmda->Pmdrel(pdxlop->Pdxltabdesc()->Pmdid());
	const ULONG ulRelCols = pmdrel->UlColumns() - pmdrel->UlSystemColumns();
	
	RangeTblEntry *prte = PrteFromTblDescr(pdxlop->Pdxltabdesc(), NULL /*pdxlid*/, ulRelCols, iRel, &dxltrctxbt);
	GPOS_ASSERT(NULL != prte);
	prte->requiredPerms |= ACL_SELECT;

	m_pctxdxltoplstmt->AddRTE(prte);

	// create dynamic scan node
	DynamicTableScan *pdts = MakeNode(DynamicTableScan);

	pdts->scanrelid = iRel;
	pdts->partIndex = pdxlop->UlPartIndexId();
	pdts->partIndexPrintable = pdxlop->UlPartIndexIdPrintable();

	Plan *pplan = &(pdts->plan);
	pplan->plan_node_id = m_pctxdxltoplstmt->UlNextPlanId();
	pplan->plan_parent_node_id = IPlanId(pplanParent);
	pplan->nMotionNodes = 0;

	// translate operator costs
	TranslatePlanCosts
		(
		CDXLPhysicalProperties::PdxlpropConvert(pdxlnDTS->Pdxlprop())->Pdxlopcost(),
		&(pplan->startup_cost),
		&(pplan->total_cost),
		&(pplan->plan_rows),
		&(pplan->plan_width)
		);

	GPOS_ASSERT(2 == pdxlnDTS->UlArity());

	// translate proj list and filter
	CDXLNode *pdxlnPrL = (*pdxlnDTS)[EdxltsIndexProjList];
	CDXLNode *pdxlnFilter = (*pdxlnDTS)[EdxltsIndexFilter];

	TranslateProjListAndFilter
		(
		pdxlnPrL,
		pdxlnFilter,
		&dxltrctxbt,	// translate context for the base table
		NULL,			// pdxltrctxLeft and pdxltrctxRight,
		&pplan->targetlist,
		&pplan->qual,
		pdxltrctxOut,
		pplan
		);

	SetParamIds(pplan);

	return (Plan *) pdts;
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorDXLToPlStmt::PplanDIS
//
//	@doc:
//		Translates a DXL dynamic index scan node into a DynamicIndexScan node
//
//---------------------------------------------------------------------------
Plan *
CTranslatorDXLToPlStmt::PplanDIS
	(
	const CDXLNode *pdxlnDIS,
	CDXLTranslateContext *pdxltrctxOut,
	Plan *pplanParent,
	DrgPdxltrctx *pdrgpdxltrctxPrevSiblings
	)
{
	CDXLPhysicalDynamicIndexScan *pdxlop = CDXLPhysicalDynamicIndexScan::PdxlopConvert(pdxlnDIS->Pdxlop());
	
	// translation context for column mappings in the base relation
	CDXLTranslateContextBaseTable dxltrctxbt(m_pmp);

	Index iRel = gpdb::UlListLength(m_pctxdxltoplstmt->PlPrte()) + 1;

	const IMDRelation *pmdrel = m_pmda->Pmdrel(pdxlop->Pdxltabdesc()->Pmdid());
	const ULONG ulRelCols = pmdrel->UlColumns() - pmdrel->UlSystemColumns();

	RangeTblEntry *prte = PrteFromTblDescr(pdxlop->Pdxltabdesc(), NULL /*pdxlid*/, ulRelCols, iRel, &dxltrctxbt);
	GPOS_ASSERT(NULL != prte);
	prte->requiredPerms |= ACL_SELECT;
	m_pctxdxltoplstmt->AddRTE(prte);

	DynamicIndexScan *pdis = MakeNode(DynamicIndexScan);

	pdis->scan.scanrelid = iRel;
	pdis->scan.partIndex = pdxlop->UlPartIndexId();
	pdis->scan.partIndexPrintable = pdxlop->UlPartIndexIdPrintable();

	CMDIdGPDB *pmdidIndex = CMDIdGPDB::PmdidConvert(pdxlop->Pdxlid()->Pmdid());
	const IMDIndex *pmdindex = m_pmda->Pmdindex(pmdidIndex);
	Oid oidIndex = pmdidIndex->OidObjectId();

	GPOS_ASSERT(InvalidOid != oidIndex);
	pdis->indexid = oidIndex;
	pdis->logicalIndexInfo = gpdb::Plgidxinfo(prte->relid, oidIndex);

	Plan *pplan = &(pdis->scan.plan);
	pplan->plan_node_id = m_pctxdxltoplstmt->UlNextPlanId();
	pplan->plan_parent_node_id = IPlanId(pplanParent);
	pplan->nMotionNodes = 0;

	// translate operator costs
	TranslatePlanCosts
		(
		CDXLPhysicalProperties::PdxlpropConvert(pdxlnDIS->Pdxlprop())->Pdxlopcost(),
		&(pplan->startup_cost),
		&(pplan->total_cost),
		&(pplan->plan_rows),
		&(pplan->plan_width)
		);

	// an index scan node must have 3 children: projection list, filter and index condition list
	GPOS_ASSERT(3 == pdxlnDIS->UlArity());

	// translate proj list and filter
	CDXLNode *pdxlnPrL = (*pdxlnDIS)[CDXLPhysicalDynamicIndexScan::EdxldisIndexProjList];
	CDXLNode *pdxlnFilter = (*pdxlnDIS)[CDXLPhysicalDynamicIndexScan::EdxldisIndexFilter];
	CDXLNode *pdxlnIndexCondList = (*pdxlnDIS)[CDXLPhysicalDynamicIndexScan::EdxldisIndexCondition];

	// translate proj list
	pplan->targetlist = PlTargetListFromProjList(pdxlnPrL, &dxltrctxbt, NULL /*pdrgpdxltrctx*/, pdxltrctxOut, pplan);

	// translate index filter
	pplan->qual = PlTranslateIndexFilter
					(
					pdxlnFilter,
					pdxltrctxOut,
					&dxltrctxbt,
					pdrgpdxltrctxPrevSiblings,
					pplan
					);

	pdis->indexorderdir = CTranslatorUtils::Scandirection(pdxlop->EdxlScanDirection());

	// translate index condition list
	List *plIndexConditions = NIL;
	List *plIndexOrigConditions = NIL;
	List *plIndexStratgey = NIL;
	List *plIndexSubtype = NIL;

	TranslateIndexConditions
		(
		pdxlnIndexCondList, 
		pdxlop->Pdxltabdesc(), 
		false, // fIndexOnlyScan 
		pmdindex, 
		pmdrel,
		pdxltrctxOut,
		&dxltrctxbt,
		pdrgpdxltrctxPrevSiblings,
		pplan,
		&plIndexConditions, 
		&plIndexOrigConditions, 
		&plIndexStratgey, 
		&plIndexSubtype
		);


	pdis->indexqual = plIndexConditions;
	pdis->indexqualorig = plIndexOrigConditions;
	pdis->indexstrategy = plIndexStratgey;
	pdis->indexsubtype = plIndexSubtype;
	SetParamIds(pplan);

	return (Plan *) pdis;
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorDXLToPlStmt::PplanDML
//
//	@doc:
//		Translates a DXL DML node 
//
//---------------------------------------------------------------------------
Plan *
CTranslatorDXLToPlStmt::PplanDML
	(
	const CDXLNode *pdxlnDML,
	CDXLTranslateContext *pdxltrctxOut,
	Plan *pplanParent,
	DrgPdxltrctx *pdrgpdxltrctxPrevSiblings
	)
{
	// translate table descriptor into a range table entry
	CDXLPhysicalDML *pdxlop = CDXLPhysicalDML::PdxlopConvert(pdxlnDML->Pdxlop());

	// create DML node
	DML *pdml = MakeNode(DML);
	Plan *pplan = &(pdml->plan);
	AclMode aclmode = ACL_NO_RIGHTS;
	
	switch (pdxlop->EdxlDmlOpType())
	{
		case gpdxl::Edxldmldelete:
		{
			m_cmdtype = CMD_DELETE;
			aclmode = ACL_DELETE;
			break;
		}
		case gpdxl::Edxldmlupdate:
		{
			m_cmdtype = CMD_UPDATE;
			aclmode = ACL_UPDATE;
			break;
		}
		case gpdxl::Edxldmlinsert:
		{
			m_cmdtype = CMD_INSERT;
			aclmode = ACL_INSERT;
			break;
		}
	}
	
	IMDId *pmdidTargetTable = pdxlop->Pdxltabdesc()->Pmdid();
	if (IMDRelation::EreldistrMasterOnly != m_pmda->Pmdrel(pmdidTargetTable)->Ereldistribution())
	{
		m_fTargetTableDistributed = true;
	}
	
	// translation context for column mappings in the base relation
	CDXLTranslateContextBaseTable dxltrctxbt(m_pmp);

	// add the new range table entry as the last element of the range table
	Index iRel = gpdb::UlListLength(m_pctxdxltoplstmt->PlPrte()) + 1;
	pdml->scanrelid = iRel;
	
	m_plResultRelations = gpdb::PlAppendInt(m_plResultRelations, iRel);

	const IMDRelation *pmdrel = m_pmda->Pmdrel(pdxlop->Pdxltabdesc()->Pmdid());
	const ULONG ulRelCols = pmdrel->UlColumns() - pmdrel->UlSystemColumns();

	CDXLTableDescr *pdxltabdesc = pdxlop->Pdxltabdesc();
	RangeTblEntry *prte = PrteFromTblDescr(pdxltabdesc, NULL /*pdxlid*/, ulRelCols, iRel, &dxltrctxbt);
	GPOS_ASSERT(NULL != prte);
	prte->requiredPerms |= aclmode;
	m_pctxdxltoplstmt->AddRTE(prte);
	
	CDXLNode *pdxlnPrL = (*pdxlnDML)[0];
	CDXLNode *pdxlnChild = (*pdxlnDML)[1];

	CDXLTranslateContext dxltrctxChild(m_pmp, false, pdxltrctxOut->PhmColParam());

	Plan *pplanChild = PplFromDXL(pdxlnChild, &dxltrctxChild, pplan, pdrgpdxltrctxPrevSiblings);

	DrgPdxltrctx *pdrgpdxltrctx = GPOS_NEW(m_pmp) DrgPdxltrctx(m_pmp);
	pdrgpdxltrctx->Append(&dxltrctxChild);

	// translate proj list
	List *plTargetListDML = PlTargetListFromProjList
		(
		pdxlnPrL,
		NULL,			// translate context for the base table
		pdrgpdxltrctx,
		pdxltrctxOut,
		pplan
		);
	
	if (pmdrel->FHasDroppedColumns())
	{
		// pad DML target list with NULLs for dropped columns for all DML operator types
		List *plTargetListWithDroppedCols = PlTargetListWithDroppedCols(plTargetListDML, pmdrel);
		gpdb::GPDBFree(plTargetListDML);
		plTargetListDML = plTargetListWithDroppedCols;
	}
	
	// add ctid, action and oid columns to target list
	pdml->oidColIdx = UlAddTargetEntryForColId(&plTargetListDML, &dxltrctxChild, pdxlop->UlOid(), true /*fResjunk*/);
	pdml->actionColIdx = UlAddTargetEntryForColId(&plTargetListDML, &dxltrctxChild, pdxlop->UlAction(), true /*fResjunk*/);
	pdml->ctidColIdx = UlAddTargetEntryForColId(&plTargetListDML, &dxltrctxChild, pdxlop->UlCtid(), true /*fResjunk*/);
	if (pdxlop->FPreserveOids())
	{
		pdml->tupleoidColIdx = UlAddTargetEntryForColId(&plTargetListDML, &dxltrctxChild, pdxlop->UlTupleOid(), true /*fResjunk*/);
	}
	else
	{
		pdml->tupleoidColIdx = 0;
	}

	GPOS_ASSERT(0 != pdml->actionColIdx);
	GPOS_ASSERT(0 != pdml->oidColIdx);

	pplan->targetlist = plTargetListDML;
	
	pplan->lefttree = pplanChild;
	pplan->nMotionNodes = pplanChild->nMotionNodes;
	pplan->plan_node_id = m_pctxdxltoplstmt->UlNextPlanId();
	pplan->plan_parent_node_id = IPlanId(pplanParent);

	if (CMD_INSERT == m_cmdtype && 0 == pplan->nMotionNodes)
	{
		List *plDirectDispatchSegIds = PlDirectDispatchSegIds(pdxlop->Pdxlddinfo());
		pplan->directDispatch.contentIds = plDirectDispatchSegIds;
		pplan->directDispatch.isDirectDispatch = (NIL != plDirectDispatchSegIds);
	}
	
	SetParamIds(pplan);

	// cleanup
	pdrgpdxltrctx->Release();


	// translate operator costs
	TranslatePlanCosts
		(
		CDXLPhysicalProperties::PdxlpropConvert(pdxlnDML->Pdxlprop())->Pdxlopcost(),
		&(pplan->startup_cost),
		&(pplan->total_cost),
		&(pplan->plan_rows),
		&(pplan->plan_width)
		);

	return (Plan *) pdml;
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorDXLToPlStmt::PlDirectDispatchSegIds
//
//	@doc:
//		Translate the direct dispatch info
//
//---------------------------------------------------------------------------
List *
CTranslatorDXLToPlStmt::PlDirectDispatchSegIds
	(
	CDXLDirectDispatchInfo *pdxlddinfo
	)
{
	if (!optimizer_direct_dispatch || NULL == pdxlddinfo)
	{
		return NIL;
	}
	
	DrgPdrgPdxldatum *pdrgpdrgpdxldatum = pdxlddinfo->Pdrgpdrgpdxldatum();
	
	if (0 == pdrgpdrgpdxldatum->UlSafeLength())
	{
		return NIL;
	}
	
	DrgPdxldatum *pdrgpdxldatum = (*pdrgpdrgpdxldatum)[0];
	GPOS_ASSERT(0 < pdrgpdxldatum->UlLength());
		
	ULONG ulHashCode = UlCdbHash(pdrgpdxldatum);
	const ULONG ulLength = pdrgpdrgpdxldatum->UlLength();
	for (ULONG ul = 0; ul < ulLength; ul++)
	{
		DrgPdxldatum *pdrgpdxldatumDisj = (*pdrgpdrgpdxldatum)[ul];
		GPOS_ASSERT(0 < pdrgpdxldatumDisj->UlLength());
		ULONG ulHashCodeNew = UlCdbHash(pdrgpdxldatumDisj);
		
		if (ulHashCode != ulHashCodeNew)
		{
			// values don't hash to the same segment
			return NIL;
		}
	}
	
	List *plSegIds = gpdb::PlAppendInt(NIL, ulHashCode);
	return plSegIds;
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorDXLToPlStmt::UlCdbHash
//
//	@doc:
//		Hash a DXL datum
//
//---------------------------------------------------------------------------
ULONG
CTranslatorDXLToPlStmt::UlCdbHash
	(
	DrgPdxldatum *pdrgpdxldatum
	)
{
	List *plConsts = NIL;
	
	const ULONG ulLength = pdrgpdxldatum->UlLength();
	
	for (ULONG ul = 0; ul < ulLength; ul++)
	{
		CDXLDatum *pdxldatum = (*pdrgpdxldatum)[ul];
		
		Const *pconst = (Const *) m_pdxlsctranslator->PconstFromDXLDatum(pdxldatum);
		plConsts = gpdb::PlAppendElement(plConsts, pconst);
	}

	ULONG ulHash = gpdb::ICdbHashList(plConsts, m_ulSegments);

	gpdb::FreeListDeep(plConsts);
	
	return ulHash;
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorDXLToPlStmt::PplanSplit
//
//	@doc:
//		Translates a DXL Split node 
//
//---------------------------------------------------------------------------
Plan *
CTranslatorDXLToPlStmt::PplanSplit
	(
	const CDXLNode *pdxlnSplit,
	CDXLTranslateContext *pdxltrctxOut,
	Plan *pplanParent,
	DrgPdxltrctx *pdrgpdxltrctxPrevSiblings
	)
{
	CDXLPhysicalSplit *pdxlop = CDXLPhysicalSplit::PdxlopConvert(pdxlnSplit->Pdxlop());

	// create SplitUpdate node
	SplitUpdate *psplit = MakeNode(SplitUpdate);
	Plan *pplan = &(psplit->plan);
	
	CDXLNode *pdxlnPrL = (*pdxlnSplit)[0];
	CDXLNode *pdxlnChild = (*pdxlnSplit)[1];

	CDXLTranslateContext dxltrctxChild(m_pmp, false, pdxltrctxOut->PhmColParam());

	Plan *pplanChild = PplFromDXL(pdxlnChild, &dxltrctxChild, pplan, pdrgpdxltrctxPrevSiblings);

	DrgPdxltrctx *pdrgpdxltrctx = GPOS_NEW(m_pmp) DrgPdxltrctx(m_pmp);
	pdrgpdxltrctx->Append(&dxltrctxChild);

	// translate proj list and filter
	pplan->targetlist = PlTargetListFromProjList
		(
		pdxlnPrL,
		NULL,			// translate context for the base table
		pdrgpdxltrctx,
		pdxltrctxOut,
		pplan
		);

	// translate delete and insert columns
	DrgPul *pdrgpulDeleteCols = pdxlop->PdrgpulDelete();
	DrgPul *pdrgpulInsertCols = pdxlop->PdrgpulInsert();
		
	GPOS_ASSERT(pdrgpulInsertCols->UlLength() == pdrgpulDeleteCols->UlLength());
	
	psplit->deleteColIdx = CTranslatorUtils::PlAttnosFromColids(pdrgpulDeleteCols, &dxltrctxChild);
	psplit->insertColIdx = CTranslatorUtils::PlAttnosFromColids(pdrgpulInsertCols, &dxltrctxChild);
	
	const TargetEntry *pteActionCol = pdxltrctxOut->Pte(pdxlop->UlAction());
	const TargetEntry *pteCtidCol = pdxltrctxOut->Pte(pdxlop->UlCtid());
	const TargetEntry *pteTupleOidCol = pdxltrctxOut->Pte(pdxlop->UlTupleOid());

	if (NULL  == pteActionCol)
	{
		GPOS_RAISE(gpdxl::ExmaDXL, gpdxl::ExmiDXL2PlStmtAttributeNotFound, pdxlop->UlAction());
	}
	if (NULL  == pteCtidCol)
	{
		GPOS_RAISE(gpdxl::ExmaDXL, gpdxl::ExmiDXL2PlStmtAttributeNotFound, pdxlop->UlCtid());
	}	 
	
	psplit->actionColIdx = pteActionCol->resno;
	psplit->ctidColIdx = pteCtidCol->resno;
	
	psplit->tupleoidColIdx = FirstLowInvalidHeapAttributeNumber;
	if (NULL != pteTupleOidCol)
	{
		psplit->tupleoidColIdx = pteTupleOidCol->resno;
	}

	pplan->lefttree = pplanChild;
	pplan->nMotionNodes = pplanChild->nMotionNodes;
	pplan->plan_node_id = m_pctxdxltoplstmt->UlNextPlanId();
	pplan->plan_parent_node_id = IPlanId(pplanParent);

	SetParamIds(pplan);

	// cleanup
	pdrgpdxltrctx->Release();

	// translate operator costs
	TranslatePlanCosts
		(
		CDXLPhysicalProperties::PdxlpropConvert(pdxlnSplit->Pdxlprop())->Pdxlopcost(),
		&(pplan->startup_cost),
		&(pplan->total_cost),
		&(pplan->plan_rows),
		&(pplan->plan_width)
		);

	return (Plan *) psplit;
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorDXLToPlStmt::PplanAssert
//
//	@doc:
//		Translate DXL assert node into GPDB assert plan node
//
//---------------------------------------------------------------------------
Plan *
CTranslatorDXLToPlStmt::PplanAssert
	(
	const CDXLNode *pdxlnAssert,
	CDXLTranslateContext *pdxltrctxOut,
	Plan *pplanParent,
	DrgPdxltrctx *pdrgpdxltrctxPrevSiblings
	)
{
	// create assert plan node
	AssertOp *passert = MakeNode(AssertOp);

	Plan *pplan = &(passert->plan);
	pplan->plan_node_id = m_pctxdxltoplstmt->UlNextPlanId();
	pplan->plan_parent_node_id = IPlanId(pplanParent);

	CDXLPhysicalAssert *pdxlopAssert = CDXLPhysicalAssert::PdxlopConvert(pdxlnAssert->Pdxlop());

	// translate error code into the its internal GPDB representation
	const CHAR *szErrorCode = pdxlopAssert->SzSQLState();
	GPOS_ASSERT(GPOS_SQLSTATE_LENGTH == clib::UlStrLen(szErrorCode));
	
	passert->errcode = MAKE_SQLSTATE(szErrorCode[0], szErrorCode[1], szErrorCode[2], szErrorCode[3], szErrorCode[4]);
	CDXLNode *pdxlnFilter = (*pdxlnAssert)[CDXLPhysicalAssert::EdxlassertIndexFilter];

	passert->errmessage = CTranslatorUtils::PlAssertErrorMsgs(pdxlnFilter);

	// translate operator costs
	TranslatePlanCosts
		(
		CDXLPhysicalProperties::PdxlpropConvert(pdxlnAssert->Pdxlprop())->Pdxlopcost(),
		&(pplan->startup_cost),
		&(pplan->total_cost),
		&(pplan->plan_rows),
		&(pplan->plan_width)
		);

	CDXLTranslateContext dxltrctxChild(m_pmp, false, pdxltrctxOut->PhmColParam());

	// translate child plan
	CDXLNode *pdxlnChild = (*pdxlnAssert)[CDXLPhysicalAssert::EdxlassertIndexChild];
	Plan *pplanChild = PplFromDXL(pdxlnChild, &dxltrctxChild, pplan, pdrgpdxltrctxPrevSiblings);

	GPOS_ASSERT(NULL != pplanChild && "child plan cannot be NULL");

	passert->plan.lefttree = pplanChild;
	pplan->nMotionNodes = pplanChild->nMotionNodes;

	CDXLNode *pdxlnPrL = (*pdxlnAssert)[CDXLPhysicalAssert::EdxlassertIndexProjList];

	List *plQuals = NULL;

	DrgPdxltrctx *pdrgpdxltrctx = GPOS_NEW(m_pmp) DrgPdxltrctx(m_pmp);
	pdrgpdxltrctx->Append(const_cast<CDXLTranslateContext*>(&dxltrctxChild));

	// translate proj list
	pplan->targetlist = PlTargetListFromProjList
				(
				pdxlnPrL,
				NULL,			// translate context for the base table
				pdrgpdxltrctx,
				pdxltrctxOut,
				pplan
				);

	// translate assert constraints
	pplan->qual = PlTranslateAssertConstraints
					(
					pdxlnFilter,
					pdxltrctxOut,
					pdrgpdxltrctx,
					pplan
					);
	
	GPOS_ASSERT(gpdb::UlListLength(pplan->qual) == gpdb::UlListLength(passert->errmessage));
	SetParamIds(pplan);

	// cleanup
	pdrgpdxltrctx->Release();

	return (Plan *) passert;
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorDXLToPlStmt::PplanRowTrigger
//
//	@doc:
//		Translates a DXL Row Trigger node
//
//---------------------------------------------------------------------------
Plan *
CTranslatorDXLToPlStmt::PplanRowTrigger
	(
	const CDXLNode *pdxlnRowTrigger,
	CDXLTranslateContext *pdxltrctxOut,
	Plan *pplanParent,
	DrgPdxltrctx *pdrgpdxltrctxPrevSiblings
	)
{
	CDXLPhysicalRowTrigger *pdxlop = CDXLPhysicalRowTrigger::PdxlopConvert(pdxlnRowTrigger->Pdxlop());

	// create RowTrigger node
	RowTrigger *prowtrigger = MakeNode(RowTrigger);
	Plan *pplan = &(prowtrigger->plan);

	CDXLNode *pdxlnPrL = (*pdxlnRowTrigger)[0];
	CDXLNode *pdxlnChild = (*pdxlnRowTrigger)[1];

	CDXLTranslateContext dxltrctxChild(m_pmp, false, pdxltrctxOut->PhmColParam());

	Plan *pplanChild = PplFromDXL(pdxlnChild, &dxltrctxChild, pplan, pdrgpdxltrctxPrevSiblings);

	DrgPdxltrctx *pdrgpdxltrctx = GPOS_NEW(m_pmp) DrgPdxltrctx(m_pmp);
	pdrgpdxltrctx->Append(&dxltrctxChild);

	// translate proj list and filter
	pplan->targetlist = PlTargetListFromProjList
		(
		pdxlnPrL,
		NULL,			// translate context for the base table
		pdrgpdxltrctx,
		pdxltrctxOut,
		pplan
		);

	Oid oidRelid = CMDIdGPDB::PmdidConvert(pdxlop->PmdidRel())->OidObjectId();
	GPOS_ASSERT(InvalidOid != oidRelid);
	prowtrigger->relid = oidRelid;
	prowtrigger->eventFlags = pdxlop->IType();

	// translate old and new columns
	DrgPul *pdrgpulOldCols = pdxlop->PdrgpulOld();
	DrgPul *pdrgpulNewCols = pdxlop->PdrgpulNew();

	GPOS_ASSERT_IMP(NULL != pdrgpulOldCols && NULL != pdrgpulNewCols,
					pdrgpulNewCols->UlLength() == pdrgpulOldCols->UlLength());

	if (NULL == pdrgpulOldCols)
	{
		prowtrigger->oldValuesColIdx = NIL;
	}
	else
	{
		prowtrigger->oldValuesColIdx = CTranslatorUtils::PlAttnosFromColids(pdrgpulOldCols, &dxltrctxChild);
	}

	if (NULL == pdrgpulNewCols)
	{
		prowtrigger->newValuesColIdx = NIL;
	}
	else
	{
		prowtrigger->newValuesColIdx = CTranslatorUtils::PlAttnosFromColids(pdrgpulNewCols, &dxltrctxChild);
	}

	pplan->lefttree = pplanChild;
	pplan->nMotionNodes = pplanChild->nMotionNodes;
	pplan->plan_node_id = m_pctxdxltoplstmt->UlNextPlanId();
	pplan->plan_parent_node_id = IPlanId(pplanParent);

	SetParamIds(pplan);

	// cleanup
	pdrgpdxltrctx->Release();

	// translate operator costs
	TranslatePlanCosts
		(
		CDXLPhysicalProperties::PdxlpropConvert(pdxlnRowTrigger->Pdxlprop())->Pdxlopcost(),
		&(pplan->startup_cost),
		&(pplan->total_cost),
		&(pplan->plan_rows),
		&(pplan->plan_width)
		);

	return (Plan *) prowtrigger;
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorDXLToPlStmt::PrteFromTblDescr
//
//	@doc:
//		Translates a DXL table descriptor into a range table entry. If an index
//		descriptor is provided, we use the mapping from colids to index attnos
//		instead of table attnos
//
//---------------------------------------------------------------------------
RangeTblEntry *
CTranslatorDXLToPlStmt::PrteFromTblDescr
	(
	const CDXLTableDescr *pdxltabdesc,
	const CDXLIndexDescr *pdxlid, // should be NULL unless we have an index-only scan
	ULONG ulRelColumns,
	Index iRel,
	CDXLTranslateContextBaseTable *pdxltrctxbtOut
	)
{
	RangeTblEntry *prte = MakeNode(RangeTblEntry);
	prte->rtekind = RTE_RELATION;

	// get the index if given
	const IMDIndex *pmdindex = NULL;
	if (NULL != pdxlid)
	{
		pmdindex = m_pmda->Pmdindex(pdxlid->Pmdid());
	}

	// get oid for table
	Oid oid = CMDIdGPDB::PmdidConvert(pdxltabdesc->Pmdid())->OidObjectId();
	GPOS_ASSERT(InvalidOid != oid);

	prte->relid = oid;
	prte->checkAsUser = pdxltabdesc->UlExecuteAsUser();
	prte->requiredPerms |= ACL_NO_RIGHTS;

	// save oid and range index in translation context
	pdxltrctxbtOut->SetOID(oid);
	pdxltrctxbtOut->SetIdx(iRel);

	Alias *palias = MakeNode(Alias);
	palias->colnames = NIL;

	// get table alias
	palias->aliasname = CTranslatorUtils::SzFromWsz(pdxltabdesc->Pmdname()->Pstr()->Wsz());

	// get column names
	const ULONG ulArity = pdxltabdesc->UlArity();
	
	INT iLastAttno = 0;
	
	for (ULONG ul = 0; ul < ulArity; ++ul)
	{
		const CDXLColDescr *pdxlcd = pdxltabdesc->Pdxlcd(ul);
		GPOS_ASSERT(NULL != pdxlcd);

		INT iAttno = pdxlcd->IAttno();

		GPOS_ASSERT(0 != iAttno);

		if (0 < iAttno)
		{
			// if iAttno > iLastAttno + 1, there were dropped attributes
			// add those to the RTE as they are required by GPDB
			for (INT iDroppedColAttno = iLastAttno + 1; iDroppedColAttno < iAttno; iDroppedColAttno++)
			{
				Value *pvalDroppedColName = gpdb::PvalMakeString("");
				palias->colnames = gpdb::PlAppendElement(palias->colnames, pvalDroppedColName);
			}
			
			// non-system attribute
			CHAR *szColName = CTranslatorUtils::SzFromWsz(pdxlcd->Pmdname()->Pstr()->Wsz());
			Value *pvalColName = gpdb::PvalMakeString(szColName);

			palias->colnames = gpdb::PlAppendElement(palias->colnames, pvalColName);
			iLastAttno = iAttno;
		}

		// get the attno from the index, in case of indexonlyscan
		if (NULL != pmdindex)
		{
			iAttno = 1 + pmdindex->UlPosInKey((ULONG) iAttno - 1);
		}

		// save mapping col id -> index in translate context
		(void) pdxltrctxbtOut->FInsertMapping(pdxlcd->UlID(), iAttno);
	}

	// if there are any dropped columns at the end, add those too to the RangeTblEntry
	for (ULONG ul = iLastAttno + 1; ul <= ulRelColumns; ul++)
	{
		Value *pvalDroppedColName = gpdb::PvalMakeString("");
		palias->colnames = gpdb::PlAppendElement(palias->colnames, pvalDroppedColName);
	}
	
	prte->eref = palias;

	return prte;
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorDXLToPlStmt::PlTargetListFromProjList
//
//	@doc:
//		Translates a DXL projection list node into a target list.
//		For base table projection lists, the caller should provide a base table
//		translation context with table oid, rtable index and mappings for the columns.
//		For other nodes pdxltrctxLeft and pdxltrctxRight give
//		the mappings of column ids to target entries in the corresponding child nodes
//		for resolving the origin of the target entries
//
//---------------------------------------------------------------------------
List *
CTranslatorDXLToPlStmt::PlTargetListFromProjList
	(
	const CDXLNode *pdxlnPrL,
	const CDXLTranslateContextBaseTable *pdxltrctxbt,
	DrgPdxltrctx *pdrgpdxltrctx,
	CDXLTranslateContext *pdxltrctxOut,
	Plan *pplanParent
	)
{
	if (NULL == pdxlnPrL)
	{
		return NULL;
	}

	List *plTargetList = NIL;

	// translate each DXL project element into a target entry
	const ULONG ulArity = pdxlnPrL->UlArity();
	for (ULONG ul = 0; ul < ulArity; ++ul)
	{
		CDXLNode *pdxlnPrEl = (*pdxlnPrL)[ul];
		GPOS_ASSERT(EdxlopScalarProjectElem == pdxlnPrEl->Pdxlop()->Edxlop());
		CDXLScalarProjElem *pdxlopPrel = CDXLScalarProjElem::PdxlopConvert(pdxlnPrEl->Pdxlop());
		GPOS_ASSERT(1 == pdxlnPrEl->UlArity());

		// translate proj element expression
		CDXLNode *pdxlnExpr = (*pdxlnPrEl)[0];

		CMappingColIdVarPlStmt mapcidvarplstmt = CMappingColIdVarPlStmt
																(
																m_pmp,
																pdxltrctxbt,
																pdrgpdxltrctx,
																pdxltrctxOut,
																m_pctxdxltoplstmt,
																pplanParent
																);

		Expr *pexpr = m_pdxlsctranslator->PexprFromDXLNodeScalar(pdxlnExpr, &mapcidvarplstmt);

		GPOS_ASSERT(NULL != pexpr);

		TargetEntry *pte = MakeNode(TargetEntry);
		pte->expr = pexpr;
		pte->resname = CTranslatorUtils::SzFromWsz(pdxlopPrel->PmdnameAlias()->Pstr()->Wsz());
		pte->resno = (AttrNumber) (ul + 1);

		if (IsA(pexpr, Var))
		{
			// check the origin of the left or the right side
			// of the current operator and if it is derived from a base relation,
			// set resorigtbl and resorigcol appropriately

			if (NULL != pdxltrctxbt)
			{
				// translating project list of a base table
				pte->resorigtbl = pdxltrctxbt->OidRel();
				pte->resorigcol = ((Var *) pexpr)->varattno;
			}
			else
			{
				// not translating a base table proj list: variable must come from
				// the left or right child of the operator

				GPOS_ASSERT(NULL != pdrgpdxltrctx);
				GPOS_ASSERT(0 != pdrgpdxltrctx->UlSafeLength());
				ULONG ulColId = CDXLScalarIdent::PdxlopConvert(pdxlnExpr->Pdxlop())->Pdxlcr()->UlID();

				const CDXLTranslateContext *pdxltrctxLeft = (*pdrgpdxltrctx)[0];
				GPOS_ASSERT(NULL != pdxltrctxLeft);
				const TargetEntry *pteOriginal = pdxltrctxLeft->Pte(ulColId);

				if (NULL == pteOriginal)
				{
					// variable not found on the left side
					GPOS_ASSERT(2 == pdrgpdxltrctx->UlSafeLength());
					const CDXLTranslateContext *pdxltrctxRight = (*pdrgpdxltrctx)[1];

					GPOS_ASSERT(NULL != pdxltrctxRight);
					pteOriginal = pdxltrctxRight->Pte(ulColId);
				}

				if (NULL  == pteOriginal)
				{
					GPOS_RAISE(gpdxl::ExmaDXL, gpdxl::ExmiDXL2PlStmtAttributeNotFound, ulColId);
				}	
				pte->resorigtbl = pteOriginal->resorigtbl;
				pte->resorigcol = pteOriginal->resorigcol;
			}
		}

		// add column mapping to output translation context
		pdxltrctxOut->InsertMapping(pdxlopPrel->UlId(), pte);

		plTargetList = gpdb::PlAppendElement(plTargetList, pte);
	}

	return plTargetList;
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorDXLToPlStmt::PlTargetListWithDroppedCols
//
//	@doc:
//		Construct the target list for a DML statement by adding NULL elements
//		for dropped columns
//
//---------------------------------------------------------------------------
List *
CTranslatorDXLToPlStmt::PlTargetListWithDroppedCols
	(
	List *plTargetList,
	const IMDRelation *pmdrel
	)
{
	GPOS_ASSERT(NULL != plTargetList);
	GPOS_ASSERT(gpdb::UlListLength(plTargetList) <= pmdrel->UlColumns());

	List *plResult = NIL;
	ULONG ulLastTLElem = 0;
	ULONG ulResno = 1;
	
	const ULONG ulRelCols = pmdrel->UlColumns();
	
	for (ULONG ul = 0; ul < ulRelCols; ul++)
	{
		const IMDColumn *pmdcol = pmdrel->Pmdcol(ul);
		
		if (pmdcol->FSystemColumn())
		{
			continue;
		}
		
		Expr *pexpr = NULL;	
		if (pmdcol->FDropped())
		{
			// add a NULL element
			OID oidType = CMDIdGPDB::PmdidConvert(m_pmda->PtMDType<IMDTypeInt4>()->Pmdid())->OidObjectId();

			pexpr = (Expr *) gpdb::PnodeMakeNULLConst(oidType);
		}
		else
		{
			TargetEntry *pte = (TargetEntry *) gpdb::PvListNth(plTargetList, ulLastTLElem);
			pexpr = (Expr *) gpdb::PvCopyObject(pte->expr);
			ulLastTLElem++;
		}
		
		CHAR *szName = CTranslatorUtils::SzFromWsz(pmdcol->Mdname().Pstr()->Wsz());
		TargetEntry *pteNew = gpdb::PteMakeTargetEntry(pexpr, ulResno, szName, false /*resjunk*/);
		plResult = gpdb::PlAppendElement(plResult, pteNew);
		ulResno++;
	}
	
	return plResult;
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorDXLToPlStmt::PlTargetListForHashNode
//
//	@doc:
//		Create a target list for the hash node of a hash join plan node by creating a list
//		of references to the elements in the child project list
//
//---------------------------------------------------------------------------
List *
CTranslatorDXLToPlStmt::PlTargetListForHashNode
	(
	const CDXLNode *pdxlnPrL,
	CDXLTranslateContext *pdxltrctxChild,
	CDXLTranslateContext *pdxltrctxOut
	)
{
	List *plTargetList = NIL;
	const ULONG ulArity = pdxlnPrL->UlArity();
	for (ULONG ul = 0; ul < ulArity; ul++)
	{
		CDXLNode *pdxlnPrEl = (*pdxlnPrL)[ul];
		CDXLScalarProjElem *pdxlopPrel = CDXLScalarProjElem::PdxlopConvert(pdxlnPrEl->Pdxlop());

		const TargetEntry *pteChild = pdxltrctxChild->Pte(pdxlopPrel->UlId());
		if (NULL  == pteChild)
		{
			GPOS_RAISE(gpdxl::ExmaDXL, gpdxl::ExmiDXL2PlStmtAttributeNotFound, pdxlopPrel->UlId());
		}	

		// get type oid for project element's expression
		GPOS_ASSERT(1 == pdxlnPrEl->UlArity());

		// find column type
		OID oidType = gpdb::OidExprType((Node*) pteChild->expr);

		// find the original varno and attno for this column
		Index idxVarnoold = 0;
		AttrNumber attnoOld = 0;

		if (IsA(pteChild->expr, Var))
		{
			Var *pv = (Var*) pteChild->expr;
			idxVarnoold = pv->varnoold;
			attnoOld = pv->varoattno;
		}
		else
		{
			idxVarnoold = OUTER;
			attnoOld = pteChild->resno;
		}

		// create a Var expression for this target list entry expression
		Var *pvar = gpdb::PvarMakeVar
					(
					OUTER,
					pteChild->resno,
					oidType,
					-1,	// vartypmod
					0	// varlevelsup
					);

		// set old varno and varattno since makeVar does not set them
		pvar->varnoold = idxVarnoold;
		pvar->varoattno = attnoOld;

		CHAR *szResname = CTranslatorUtils::SzFromWsz(pdxlopPrel->PmdnameAlias()->Pstr()->Wsz());

		TargetEntry *pte = gpdb::PteMakeTargetEntry
							(
							(Expr *) pvar,
							(AttrNumber) (ul + 1),
							szResname,
							false		// resjunk
							);

		plTargetList = gpdb::PlAppendElement(plTargetList, pte);
		pdxltrctxOut->InsertMapping(pdxlopPrel->UlId(), pte);
	}

	return plTargetList;
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorDXLToPlStmt::PlQualFromFilter
//
//	@doc:
//		Translates a DXL filter node into a Qual list.
//
//---------------------------------------------------------------------------
List *
CTranslatorDXLToPlStmt::PlQualFromFilter
	(
	const CDXLNode * pdxlnFilter,
	const CDXLTranslateContextBaseTable *pdxltrctxbt,
	DrgPdxltrctx *pdrgpdxltrctx,
	CDXLTranslateContext *pdxltrctxOut,
	Plan *pplanParent
	)
{
	const ULONG ulArity = pdxlnFilter->UlArity();
	if (0 == ulArity)
	{
		return NIL;
	}

	GPOS_ASSERT(1 == ulArity);

	CDXLNode *pdxlnFilterCond = (*pdxlnFilter)[0];
	GPOS_ASSERT(CTranslatorDXLToScalar::FBoolean(pdxlnFilterCond, m_pmda));

	return PlQualFromScalarCondNode(pdxlnFilterCond, pdxltrctxbt, pdrgpdxltrctx, pdxltrctxOut, pplanParent);
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorDXLToPlStmt::PlQualFromScalarCondNode
//
//	@doc:
//		Translates a DXL scalar condition node node into a Qual list.
//
//---------------------------------------------------------------------------
List *
CTranslatorDXLToPlStmt::PlQualFromScalarCondNode
	(
	const CDXLNode *pdxlnCond,
	const CDXLTranslateContextBaseTable *pdxltrctxbt,
	DrgPdxltrctx *pdrgpdxltrctx,
	CDXLTranslateContext *pdxltrctxOut,
	Plan *pplanParent
	)
{
	List *plQuals = NIL;

	GPOS_ASSERT(CTranslatorDXLToScalar::FBoolean(const_cast<CDXLNode*>(pdxlnCond), m_pmda));

	CMappingColIdVarPlStmt mapcidvarplstmt = CMappingColIdVarPlStmt
															(
															m_pmp,
															pdxltrctxbt,
															pdrgpdxltrctx,
															pdxltrctxOut,
															m_pctxdxltoplstmt,
															pplanParent
															);

	Expr *pexpr = m_pdxlsctranslator->PexprFromDXLNodeScalar
					(
					pdxlnCond,
					&mapcidvarplstmt
					);

	plQuals = gpdb::PlAppendElement(plQuals, pexpr);

	return plQuals;
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorDXLToPlStmt::TranslatePlanCosts
//
//	@doc:
//		Translates DXL plan costs into the GPDB cost variables
//
//---------------------------------------------------------------------------
void
CTranslatorDXLToPlStmt::TranslatePlanCosts
	(
	const CDXLOperatorCost *pdxlopcost,
	Cost *pcostStartupOut,
	Cost *pcostTotalOut,
	Cost *pcostRowsOut,
	INT * piWidthOut
	)
{
	*pcostStartupOut = CostFromStr(pdxlopcost->PstrStartupCost());
	*pcostTotalOut = CostFromStr(pdxlopcost->PstrTotalCost());
	*pcostRowsOut = CostFromStr(pdxlopcost->PstrRows());
	*piWidthOut = CTranslatorUtils::IFromStr(pdxlopcost->PstrWidth());
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorDXLToPlStmt::TranslateProjListAndFilter
//
//	@doc:
//		Translates DXL proj list and filter into GPDB's target and qual lists,
//		respectively
//
//---------------------------------------------------------------------------
void
CTranslatorDXLToPlStmt::TranslateProjListAndFilter
	(
	const CDXLNode *pdxlnPrL,
	const CDXLNode *pdxlnFilter,
	const CDXLTranslateContextBaseTable *pdxltrctxbt,
	DrgPdxltrctx *pdrgpdxltrctx,
	List **pplTargetListOut,
	List **pplQualOut,
	CDXLTranslateContext *pdxltrctxOut,
	Plan *pplanParent
	)
{
	// translate proj list
	*pplTargetListOut = PlTargetListFromProjList
						(
						pdxlnPrL,
						pdxltrctxbt,		// base table translation context
						pdrgpdxltrctx,
						pdxltrctxOut,
						pplanParent
						);

	// translate filter
	*pplQualOut = PlQualFromFilter
					(
					pdxlnFilter,
					pdxltrctxbt,			// base table translation context
					pdrgpdxltrctx,
					pdxltrctxOut,
					pplanParent
					);
}


//---------------------------------------------------------------------------
//	@function:
//		CTranslatorDXLToPlStmt::TranslateHashExprList
//
//	@doc:
//		Translates DXL hash expression list in a redistribute motion node into
//		GPDB's hash expression and expression types lists, respectively
//
//---------------------------------------------------------------------------
void
CTranslatorDXLToPlStmt::TranslateHashExprList
	(
	const CDXLNode *pdxlnHashExprList,
	const CDXLTranslateContext *pdxltrctxChild,
	List **pplHashExprOut,
	List **pplHashExprTypesOut,
	CDXLTranslateContext *pdxltrctxOut,
	Plan *pplanParent
	)
{
	GPOS_ASSERT(NIL == *pplHashExprOut);
	GPOS_ASSERT(NIL == *pplHashExprTypesOut);

	List *plHashExpr = NIL;
	List *plHashExprTypes = NIL;

	DrgPdxltrctx *pdrgpdxltrctx = GPOS_NEW(m_pmp) DrgPdxltrctx(m_pmp);
	pdrgpdxltrctx->Append(pdxltrctxChild);

	const ULONG ulArity = pdxlnHashExprList->UlArity();
	for (ULONG ul = 0; ul < ulArity; ul++)
	{
		CDXLNode *pdxlnHashExpr = (*pdxlnHashExprList)[ul];
		CDXLScalarHashExpr *pdxlopHashExpr = CDXLScalarHashExpr::PdxlopConvert(pdxlnHashExpr->Pdxlop());

		// the type of the hash expression in GPDB is computed as the left operand 
		// of the equality operator of the actual hash expression type
		const IMDType *pmdtype = m_pmda->Pmdtype(pdxlopHashExpr->PmdidType());
		const IMDScalarOp *pmdscop = m_pmda->Pmdscop(pmdtype->PmdidCmp(IMDType::EcmptEq));
		
		const IMDId *pmdidHashType = pmdscop->PmdidTypeLeft();
		
		plHashExprTypes = gpdb::PlAppendOid(plHashExprTypes, CMDIdGPDB::PmdidConvert(pmdidHashType)->OidObjectId());

		GPOS_ASSERT(1 == pdxlnHashExpr->UlArity());
		CDXLNode *pdxlnExpr = (*pdxlnHashExpr)[0];

		CMappingColIdVarPlStmt mapcidvarplstmt = CMappingColIdVarPlStmt
																(
																m_pmp,
																NULL,
																pdrgpdxltrctx,
																pdxltrctxOut,
																m_pctxdxltoplstmt,
																pplanParent
																);

		Expr *pexpr = m_pdxlsctranslator->PexprFromDXLNodeScalar(pdxlnExpr, &mapcidvarplstmt);

		plHashExpr = gpdb::PlAppendElement(plHashExpr, pexpr);

		GPOS_ASSERT((ULONG) gpdb::UlListLength(plHashExpr) == ul + 1);
	}


	*pplHashExprOut = plHashExpr;
	*pplHashExprTypesOut = plHashExprTypes;

	// cleanup
	pdrgpdxltrctx->Release();
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorDXLToPlStmt::TranslateSortCols
//
//	@doc:
//		Translates DXL sorting columns list into GPDB's arrays of sorting attribute numbers,
//		and sorting operator ids, respectively.
//		The two arrays must be allocated by the caller.
//
//---------------------------------------------------------------------------
void
CTranslatorDXLToPlStmt::TranslateSortCols
	(
	const CDXLNode *pdxlnSortColList,
	const CDXLTranslateContext *pdxltrctxChild,
	AttrNumber *pattnoSortColIds,
	Oid *poidSortOpIds
	)
{
	const ULONG ulArity = pdxlnSortColList->UlArity();
	for (ULONG ul = 0; ul < ulArity; ul++)
	{
		CDXLNode *pdxlnSortCol = (*pdxlnSortColList)[ul];
		CDXLScalarSortCol *pdxlopSortCol = CDXLScalarSortCol::PdxlopConvert(pdxlnSortCol->Pdxlop());

		ULONG ulSortColId = pdxlopSortCol->UlColId();
		const TargetEntry *pteSortCol = pdxltrctxChild->Pte(ulSortColId);
		if (NULL  == pteSortCol)
		{
			GPOS_RAISE(gpdxl::ExmaDXL, gpdxl::ExmiDXL2PlStmtAttributeNotFound, ulSortColId);
		}	

		pattnoSortColIds[ul] = pteSortCol->resno;
		poidSortOpIds[ul] = CMDIdGPDB::PmdidConvert(pdxlopSortCol->PmdidSortOp())->OidObjectId();
	}
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorDXLToPlStmt::CostFromStr
//
//	@doc:
//		Parses a cost value from a string
//
//---------------------------------------------------------------------------
Cost
CTranslatorDXLToPlStmt::CostFromStr
	(
	const CWStringBase *pstr
	)
{
	CHAR *sz = CTranslatorUtils::SzFromWsz(pstr->Wsz());
	return gpos::clib::DStrToD(sz);
}


//---------------------------------------------------------------------------
//	@function:
//		CTranslatorDXLToPlStmt::FTargetTableDistributed
//
//	@doc:
//		Check if given operator is a DML on a distributed table
//
//---------------------------------------------------------------------------
BOOL
CTranslatorDXLToPlStmt::FTargetTableDistributed
	(
	CDXLOperator *pdxlop
	)
{
	if (EdxlopPhysicalDML != pdxlop->Edxlop())
	{
		return false;
	}

	CDXLPhysicalDML *pdxlopDML = CDXLPhysicalDML::PdxlopConvert(pdxlop);
	IMDId *pmdid = pdxlopDML->Pdxltabdesc()->Pmdid();

	return IMDRelation::EreldistrMasterOnly != m_pmda->Pmdrel(pmdid)->Ereldistribution();
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorDXLToPlStmt::IAddTargetEntryForColId
//
//	@doc:
//		Add a new target entry for the given colid to the given target list and
//		return the position of the new entry
//
//---------------------------------------------------------------------------
ULONG
CTranslatorDXLToPlStmt::UlAddTargetEntryForColId
	(
	List **pplTargetList,
	CDXLTranslateContext *pdxltrctx,
	ULONG ulColId,
	BOOL fResjunk
	)
{
	GPOS_ASSERT(NULL != pplTargetList);
	
	const TargetEntry *pte = pdxltrctx->Pte(ulColId);
	
	if (NULL == pte)
	{
		// colid not found in translate context
		return 0;
	}
	
	// TODO: antova - Oct 29, 2012; see if entry already exists in the target list
	
	OID oidExpr = gpdb::OidExprType((Node*) pte->expr);
	Var *pvar = gpdb::PvarMakeVar
						(
						OUTER,
						pte->resno,
						oidExpr,
						-1,	// vartypmod
						0	// varlevelsup
						);
	ULONG ulResNo = gpdb::UlListLength(*pplTargetList) + 1;
	CHAR *szResName = PStrDup(pte->resname);
	TargetEntry *pteNew = gpdb::PteMakeTargetEntry((Expr*) pvar, ulResNo, szResName, fResjunk);
	*pplTargetList = gpdb::PlAppendElement(*pplTargetList, pteNew);
	
	return pte->resno;
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorDXLToPlStmt::JtFromEdxljt
//
//	@doc:
//		Translates the join type from its DXL representation into the GPDB one
//
//---------------------------------------------------------------------------
JoinType
CTranslatorDXLToPlStmt::JtFromEdxljt
	(
	EdxlJoinType edxljt
	)
{
	GPOS_ASSERT(EdxljtSentinel > edxljt);

	JoinType jt = JOIN_INNER;

	switch (edxljt)
	{
		case EdxljtInner:
			jt = JOIN_INNER;
			break;
		case EdxljtLeft:
			jt = JOIN_LEFT;
			break;
		case EdxljtFull:
			jt = JOIN_FULL;
			break;
		case EdxljtRight:
			jt = JOIN_RIGHT;
			break;
		case EdxljtIn:
			jt = JOIN_IN;
			break;
		case EdxljtLeftAntiSemijoin:
			jt = JOIN_LASJ;
			break;
		case EdxljtLeftAntiSemijoinNotIn:
			jt = JOIN_LASJ_NOTIN;
			break;
		default:
			GPOS_ASSERT(!"Unrecognized join type");
	}

	return jt;
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorDXLToPlStmt::PplanCTAS
//
//	@doc:
//		Sets the vartypmod fields in the target entries of the given target list
//
//---------------------------------------------------------------------------
void
CTranslatorDXLToPlStmt::SetVarTypMod
	(
	const CDXLPhysicalCTAS *pdxlop,
	List *plTargetList
	)
{
	GPOS_ASSERT(NULL != plTargetList);

	DrgPi *pdrgpi = pdxlop->PdrgpiVarTypeMod();
	GPOS_ASSERT(pdrgpi->UlLength() == gpdb::UlListLength(plTargetList));

	ULONG ul = 0;
	ListCell *plc = NULL;
	ForEach (plc, plTargetList)
	{
		TargetEntry *pte = (TargetEntry *) lfirst(plc);
		GPOS_ASSERT(IsA(pte, TargetEntry));

		if (IsA(pte->expr, Var))
		{
			pte->expr;
			Var *var = (Var*) pte->expr;
			var->vartypmod = *(*pdrgpi)[ul];
		}
		++ul;
	}
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorDXLToPlStmt::PplanCTAS
//
//	@doc:
//		Translates a DXL CTAS node 
//
//---------------------------------------------------------------------------
Plan *
CTranslatorDXLToPlStmt::PplanCTAS
	(
	const CDXLNode *pdxlnCTAS,
	CDXLTranslateContext *pdxltrctxOut,
	Plan *pplanParent,
	DrgPdxltrctx *pdrgpdxltrctxPrevSiblings
	)
{
	CDXLPhysicalCTAS *pdxlop = CDXLPhysicalCTAS::PdxlopConvert(pdxlnCTAS->Pdxlop());
	CDXLNode *pdxlnPrL = (*pdxlnCTAS)[0];
	CDXLNode *pdxlnChild = (*pdxlnCTAS)[1];

	GPOS_ASSERT(NULL == pdxlop->Pdxlctasopt()->Pdrgpctasopt());
	
	CDXLTranslateContext dxltrctxChild(m_pmp, false, pdxltrctxOut->PhmColParam());

	Plan *pplan = PplFromDXL(pdxlnChild, &dxltrctxChild, pplanParent, pdrgpdxltrctxPrevSiblings);
	
	// fix target list to match the required column names
	DrgPdxltrctx *pdrgpdxltrctx = GPOS_NEW(m_pmp) DrgPdxltrctx(m_pmp);
	pdrgpdxltrctx->Append(&dxltrctxChild);
	
	List *plTargetList = PlTargetListFromProjList
						(
						pdxlnPrL,
						NULL,		// pdxltrctxbt
						pdrgpdxltrctx,
						pdxltrctxOut,
						pplan
						);
	SetVarTypMod(pdxlop, plTargetList);
	
	SetParamIds(pplan);

	// cleanup
	pdrgpdxltrctx->Release();


	// translate operator costs
	TranslatePlanCosts
		(
		CDXLPhysicalProperties::PdxlpropConvert(pdxlnCTAS->Pdxlprop())->Pdxlopcost(),
		&(pplan->startup_cost),
		&(pplan->total_cost),
		&(pplan->plan_rows),
		&(pplan->plan_width)
		);

	IntoClause *pintocl = PintoclFromCtas(pdxlop);
	GpPolicy *pdistrpolicy = PdistrpolicyFromCtas(pdxlop);
	m_pctxdxltoplstmt->AddCtasInfo(pintocl, pdistrpolicy);
	
	GPOS_ASSERT(IMDRelation::EreldistrMasterOnly != pdxlop->Ereldistrpolicy());
	
	m_fTargetTableDistributed = true;
	
	// Add a result node on top with the correct projection list
	Result *presult = MakeNode(Result);
	Plan *pplanResult = &(presult->plan);
	pplanResult->plan_node_id = m_pctxdxltoplstmt->UlNextPlanId();
	pplanResult->plan_parent_node_id = IPlanId(pplanParent);
	pplanResult->nMotionNodes = pplan->nMotionNodes;
	pplanResult->lefttree = pplan;
	pplan->plan_parent_node_id = IPlanId(pplanResult);

	pplanResult->targetlist = plTargetList;
	SetParamIds(pplanResult);

	pplan = (Plan *) presult;

	return (Plan *) pplan;
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorDXLToPlStmt::PintoclFromCtas
//
//	@doc:
//		Translates a DXL CTAS into clause 
//
//---------------------------------------------------------------------------
IntoClause *
CTranslatorDXLToPlStmt::PintoclFromCtas
	(
	const CDXLPhysicalCTAS *pdxlop
	)
{
	IntoClause *pintocl = MakeNode(IntoClause);
	pintocl->rel = MakeNode(RangeVar);
	pintocl->rel->istemp = pdxlop->FTemporary();
	pintocl->rel->relname = CTranslatorUtils::SzFromWsz(pdxlop->Pmdname()->Pstr()->Wsz());
	pintocl->rel->schemaname = NULL;
	if (NULL != pdxlop->PmdnameSchema())
	{
		pintocl->rel->schemaname = CTranslatorUtils::SzFromWsz(pdxlop->PmdnameSchema()->Pstr()->Wsz());
	}
	
	CDXLCtasStorageOptions *pdxlctasopt = pdxlop->Pdxlctasopt();
	if (NULL != pdxlctasopt->PmdnameTablespace())
	{
		pintocl->tableSpaceName = CTranslatorUtils::SzFromWsz(pdxlop->Pdxlctasopt()->PmdnameTablespace()->Pstr()->Wsz());
	}
	
	pintocl->onCommit = (OnCommitAction) pdxlctasopt->Ectascommit(); 
	pintocl->options = PlCtasOptions(pdxlctasopt->Pdrgpctasopt());
	
	// get column names
	DrgPdxlcd *pdrgpdxlcd = pdxlop->Pdrgpdxlcd();
	const ULONG ulCols = pdrgpdxlcd->UlLength();
	pintocl->colNames = NIL;
	for (ULONG ul = 0; ul < ulCols; ++ul)
	{
		const CDXLColDescr *pdxlcd = (*pdrgpdxlcd)[ul];

		CHAR *szColName = CTranslatorUtils::SzFromWsz(pdxlcd->Pmdname()->Pstr()->Wsz());
		
		ColumnDef *pcoldef = MakeNode(ColumnDef);
		pcoldef->colname = szColName;
		pcoldef->is_local = true;

		pintocl->colNames = gpdb::PlAppendElement(pintocl->colNames, pcoldef);

	}

	return pintocl;
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorDXLToPlStmt::PdistrpolicyFromCtas
//
//	@doc:
//		Translates distribution policy given by a physical CTAS operator 
//
//---------------------------------------------------------------------------
GpPolicy *
CTranslatorDXLToPlStmt::PdistrpolicyFromCtas
	(
	const CDXLPhysicalCTAS *pdxlop
	)
{
	DrgPul *pdrgpulDistrCols = pdxlop->PdrgpulDistr();

	const ULONG ulDistrCols = pdrgpulDistrCols->UlSafeLength();

	ULONG ulDistrColsAlloc = 1;
	if (0 < ulDistrCols)
	{
		ulDistrColsAlloc = ulDistrCols;
	}
	
	GpPolicy *pdistrpolicy = (GpPolicy *) gpdb::GPDBAlloc(sizeof(GpPolicy) + 
															ulDistrColsAlloc * sizeof(AttrNumber));
	GPOS_ASSERT(IMDRelation::EreldistrHash == pdxlop->Ereldistrpolicy() ||
				IMDRelation::EreldistrRandom == pdxlop->Ereldistrpolicy());
	
	pdistrpolicy->ptype = POLICYTYPE_PARTITIONED;
	pdistrpolicy->nattrs = 0;
	if (IMDRelation::EreldistrHash == pdxlop->Ereldistrpolicy())
	{
		
		GPOS_ASSERT(0 < ulDistrCols);
		pdistrpolicy->nattrs = ulDistrCols;
		
		for (ULONG ul = 0; ul < ulDistrCols; ul++)
		{
			ULONG ulColPos = *((*pdrgpulDistrCols)[ul]);
			pdistrpolicy->attrs[ul] = ulColPos + 1;
		}
	}
	return pdistrpolicy;
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorDXLToPlStmt::PlCtasOptions
//
//	@doc:
//		Translates CTAS options
//
//---------------------------------------------------------------------------
List *
CTranslatorDXLToPlStmt::PlCtasOptions
	(
	CDXLCtasStorageOptions::DrgPctasOpt *pdrgpctasopt
	)
{
	if (NULL == pdrgpctasopt)
	{
		return NIL;
	}
	
	const ULONG ulOptions = pdrgpctasopt->UlLength();
	List *plOptions = NIL;
	for (ULONG ul = 0; ul < ulOptions; ul++)
	{
		CDXLCtasStorageOptions::CDXLCtasOption *pdxlopt = (*pdrgpctasopt)[ul];
		CWStringBase *pstrName = pdxlopt->m_pstrName;
		CWStringBase *pstrValue = pdxlopt->m_pstrValue;
		DefElem *pdefelem = MakeNode(DefElem);
		pdefelem->defname = CTranslatorUtils::SzFromWsz(pstrName->Wsz());

		if (!pdxlopt->m_fNull)
		{
			NodeTag argType = (NodeTag) pdxlopt->m_ulType;

			GPOS_ASSERT(T_Integer == argType || T_String == argType);
			if (T_Integer == argType)
			{
				pdefelem->arg = (Node *) gpdb::PvalMakeInteger(CTranslatorUtils::LFromStr(pstrValue));
			}
			else
			{
				pdefelem->arg = (Node *) gpdb::PvalMakeString(CTranslatorUtils::SzFromWsz(pstrValue->Wsz()));
			}
		}

		plOptions = gpdb::PlAppendElement(plOptions, pdefelem);
	}
	
	return plOptions;
}


//---------------------------------------------------------------------------
//	@function:
//		CTranslatorDXLToPlStmt::PplanBitmapTableScan
//
//	@doc:
//		Translates a DXL bitmap table scan node into a BitmapTableScan node
//
//---------------------------------------------------------------------------
Plan *
CTranslatorDXLToPlStmt::PplanBitmapTableScan
	(
	const CDXLNode *pdxlnBitmapScan,
	CDXLTranslateContext *pdxltrctxOut,
	Plan *pplanParent,
	DrgPdxltrctx *pdrgpdxltrctxPrevSiblings
	)
{
	ULONG ulPartIndex = INVALID_PART_INDEX;
	ULONG ulPartIndexPrintable = INVALID_PART_INDEX;
	const CDXLTableDescr *pdxltabdesc = NULL;
	BOOL fDynamic = false;

	CDXLOperator *pdxlop = pdxlnBitmapScan->Pdxlop();
	if (EdxlopPhysicalBitmapTableScan == pdxlop->Edxlop())
	{
		pdxltabdesc = CDXLPhysicalBitmapTableScan::PdxlopConvert(pdxlop)->Pdxltabdesc();
	}
	else
	{
		GPOS_ASSERT(EdxlopPhysicalDynamicBitmapTableScan == pdxlop->Edxlop());
		CDXLPhysicalDynamicBitmapTableScan *pdxlopDynamic =
				CDXLPhysicalDynamicBitmapTableScan::PdxlopConvert(pdxlop);
		pdxltabdesc = pdxlopDynamic->Pdxltabdesc();

		ulPartIndex = pdxlopDynamic->UlPartIndexId();
		ulPartIndexPrintable = pdxlopDynamic->UlPartIndexIdPrintable();
		fDynamic = true;
	}

	// translation context for column mappings in the base relation
	CDXLTranslateContextBaseTable dxltrctxbt(m_pmp);

	// add the new range table entry as the last element of the range table
	Index iRel = gpdb::UlListLength(m_pctxdxltoplstmt->PlPrte()) + 1;

	const IMDRelation *pmdrel = m_pmda->Pmdrel(pdxltabdesc->Pmdid());
	const ULONG ulRelCols = pmdrel->UlColumns() - pmdrel->UlSystemColumns();

	RangeTblEntry *prte = PrteFromTblDescr(pdxltabdesc, NULL /*pdxlid*/, ulRelCols, iRel, &dxltrctxbt);
	GPOS_ASSERT(NULL != prte);
	prte->requiredPerms |= ACL_SELECT;

	m_pctxdxltoplstmt->AddRTE(prte);

	BitmapTableScan *pdbts = MakeNode(BitmapTableScan);
	pdbts->scan.scanrelid = iRel;
	pdbts->scan.partIndex = ulPartIndex;
	pdbts->scan.partIndexPrintable = ulPartIndexPrintable;

	Plan *pplan = &(pdbts->scan.plan);
	pplan->plan_node_id = m_pctxdxltoplstmt->UlNextPlanId();
	pplan->plan_parent_node_id = IPlanId(pplanParent);
	pplan->nMotionNodes = 0;

	// translate operator costs
	TranslatePlanCosts
		(
		CDXLPhysicalProperties::PdxlpropConvert(pdxlnBitmapScan->Pdxlprop())->Pdxlopcost(),
		&(pplan->startup_cost),
		&(pplan->total_cost),
		&(pplan->plan_rows),
		&(pplan->plan_width)
		);

	GPOS_ASSERT(4 == pdxlnBitmapScan->UlArity());

	// translate proj list and filter
	CDXLNode *pdxlnPrL = (*pdxlnBitmapScan)[0];
	CDXLNode *pdxlnFilter = (*pdxlnBitmapScan)[1];
	CDXLNode *pdxlnRecheckCond = (*pdxlnBitmapScan)[2];
	CDXLNode *pdxlnBitmapAccessPath = (*pdxlnBitmapScan)[3];

	List *plQuals = NULL;
	TranslateProjListAndFilter
		(
		pdxlnPrL,
		pdxlnFilter,
		&dxltrctxbt,	// translate context for the base table
		pdrgpdxltrctxPrevSiblings,
		&pplan->targetlist,
		&plQuals,
		pdxltrctxOut,
		pplan
		);
	pplan->qual = plQuals;

	pdbts->bitmapqualorig = PlQualFromFilter
							(
							pdxlnRecheckCond,
							&dxltrctxbt,
							pdrgpdxltrctxPrevSiblings,
							pdxltrctxOut,
							pplan
							);

	pdbts->scan.plan.lefttree = PplanBitmapAccessPath
								(
								pdxlnBitmapAccessPath,
								pdxltrctxOut,
								pmdrel,
								pdxltabdesc,
								&dxltrctxbt,
								pdrgpdxltrctxPrevSiblings,
								pdbts
								);
	SetParamIds(pplan);

	return (Plan *) pdbts;
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorDXLToPlStmt::PplanBitmapAccessPath
//
//	@doc:
//		Translate the tree of bitmap index operators that are under the given
//		(dynamic) bitmap table scan.
//
//---------------------------------------------------------------------------
Plan *
CTranslatorDXLToPlStmt::PplanBitmapAccessPath
	(
	const CDXLNode *pdxlnBitmapAccessPath,
	CDXLTranslateContext *pdxltrctxOut,
	const IMDRelation *pmdrel,
	const CDXLTableDescr *pdxltabdesc,
	CDXLTranslateContextBaseTable *pdxltrctxbt,
	DrgPdxltrctx *pdrgpdxltrctxPrevSiblings,
	BitmapTableScan *pdbts
	)
{
	Edxlopid edxlopid = pdxlnBitmapAccessPath->Pdxlop()->Edxlop();
	if (EdxlopScalarBitmapIndexProbe == edxlopid)
	{
		return PplanBitmapIndexProbe
				(
				pdxlnBitmapAccessPath,
				pdxltrctxOut,
				pmdrel,
				pdxltabdesc,
				pdxltrctxbt,
				pdrgpdxltrctxPrevSiblings,
				pdbts
				);
	}
	GPOS_ASSERT(EdxlopScalarBitmapBoolOp == edxlopid);

	return PplanBitmapBoolOp
			(
			pdxlnBitmapAccessPath, 
			pdxltrctxOut, 
			pmdrel, 
			pdxltabdesc, 
			pdxltrctxbt,
			pdrgpdxltrctxPrevSiblings,
			pdbts
			);
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorDXLToScalar::PplanBitmapBoolOp
//
//	@doc:
//		Translates a DML bitmap bool op expression 
//
//---------------------------------------------------------------------------
Plan *
CTranslatorDXLToPlStmt::PplanBitmapBoolOp
	(
	const CDXLNode *pdxlnBitmapBoolOp,
	CDXLTranslateContext *pdxltrctxOut,
	const IMDRelation *pmdrel,
	const CDXLTableDescr *pdxltabdesc,
	CDXLTranslateContextBaseTable *pdxltrctxbt,
	DrgPdxltrctx *pdrgpdxltrctxPrevSiblings,
	BitmapTableScan *pdbts
	)
{
	GPOS_ASSERT(NULL != pdxlnBitmapBoolOp);
	GPOS_ASSERT(EdxlopScalarBitmapBoolOp == pdxlnBitmapBoolOp->Pdxlop()->Edxlop());

	CDXLScalarBitmapBoolOp *pdxlop = CDXLScalarBitmapBoolOp::PdxlopConvert(pdxlnBitmapBoolOp->Pdxlop());
	
	CDXLNode *pdxlnLeft = (*pdxlnBitmapBoolOp)[0];
	CDXLNode *pdxlnRight = (*pdxlnBitmapBoolOp)[1];
	
	Plan *pplanLeft = PplanBitmapAccessPath
						(
						pdxlnLeft,
						pdxltrctxOut,
						pmdrel,
						pdxltabdesc,
						pdxltrctxbt,
						pdrgpdxltrctxPrevSiblings,
						pdbts
						);
	Plan *pplanRight = PplanBitmapAccessPath
						(
						pdxlnRight,
						pdxltrctxOut,
						pmdrel,
						pdxltabdesc,
						pdxltrctxbt,
						pdrgpdxltrctxPrevSiblings,
						pdbts
						);
	List *plChildPlans = ListMake2(pplanLeft, pplanRight);

	Plan *pplan = NULL;
	
	if (CDXLScalarBitmapBoolOp::EdxlbitmapAnd == pdxlop->Edxlbitmapboolop())
	{
		BitmapAnd *bitmapand = MakeNode(BitmapAnd);
		bitmapand->bitmapplans = plChildPlans;
		bitmapand->plan.targetlist = NULL;
		bitmapand->plan.qual = NULL;
		pplan = (Plan *) bitmapand;
	}
	else
	{
		BitmapOr *bitmapor = MakeNode(BitmapOr);
		bitmapor->bitmapplans = plChildPlans;
		bitmapor->plan.targetlist = NULL;
		bitmapor->plan.qual = NULL;
		pplan = (Plan *) bitmapor;
	}
	
	
	return pplan;
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorDXLToPlStmt::PplanBitmapIndexProbe
//
//	@doc:
//		Translate CDXLScalarBitmapIndexProbe into BitmapIndexScan
//
//---------------------------------------------------------------------------
Plan *
CTranslatorDXLToPlStmt::PplanBitmapIndexProbe
	(
	const CDXLNode *pdxlnBitmapIndexProbe,
	CDXLTranslateContext *pdxltrctxOut,
	const IMDRelation *pmdrel,
	const CDXLTableDescr *pdxltabdesc,
	CDXLTranslateContextBaseTable *pdxltrctxbt,
	DrgPdxltrctx *pdrgpdxltrctxPrevSiblings,
	BitmapTableScan *pdbts
	)
{
	CDXLScalarBitmapIndexProbe *pdxlopScalar =
			CDXLScalarBitmapIndexProbe::PdxlopConvert(pdxlnBitmapIndexProbe->Pdxlop());

	BitmapIndexScan *pbis = MakeNode(BitmapIndexScan);
	pbis->scan.scanrelid = pdbts->scan.scanrelid;
	pbis->scan.partIndex = pdbts->scan.partIndex;

	CMDIdGPDB *pmdidIndex = CMDIdGPDB::PmdidConvert(pdxlopScalar->Pdxlid()->Pmdid());
	const IMDIndex *pmdindex = m_pmda->Pmdindex(pmdidIndex);
	Oid oidIndex = pmdidIndex->OidObjectId();

	GPOS_ASSERT(InvalidOid != oidIndex);
	Plan *pplanParent = (Plan *) pdbts;
	pbis->indexid = oidIndex;
	OID oidRel = CMDIdGPDB::PmdidConvert(pdxltabdesc->Pmdid())->OidObjectId();
	pbis->logicalIndexInfo = gpdb::Plgidxinfo(oidRel, oidIndex);
	Plan *pplan = &(pbis->scan.plan);
	pplan->plan_node_id = m_pctxdxltoplstmt->UlNextPlanId();
	pplan->plan_parent_node_id = IPlanId(pplanParent);
	pplan->nMotionNodes = 0;

	GPOS_ASSERT(1 == pdxlnBitmapIndexProbe->UlArity());
	CDXLNode *pdxlnIndexCondList = (*pdxlnBitmapIndexProbe)[0];
	List *plIndexConditions = NIL;
	List *plIndexOrigConditions = NIL;
	List *plIndexStratgey = NIL;
	List *plIndexSubtype = NIL;

	TranslateIndexConditions
		(
		pdxlnIndexCondList,
		pdxltabdesc,
		false /*fIndexOnlyScan*/,
		pmdindex,
		pmdrel,
		pdxltrctxOut,
		pdxltrctxbt,
		pdrgpdxltrctxPrevSiblings,
		pplan,
		&plIndexConditions,
		&plIndexOrigConditions,
		&plIndexStratgey,
		&plIndexSubtype
		);

	pbis->indexqual = plIndexConditions;
	pbis->indexqualorig = plIndexOrigConditions;
	pbis->indexstrategy = plIndexStratgey;
	pbis->indexsubtype = plIndexSubtype;
	SetParamIds(pplan);

	return pplan;
}

// EOF
