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

//--------------------------------------------------------------------------
//	@filename:
//		COptTasks.cpp
//
//	@doc:
//		Routines to perform optimization related tasks using the gpos framework
//
//	@test:
//
//
//---------------------------------------------------------------------------

#define ALLOW_strcasecmp

#include "gpopt/utils/gpdbdefs.h"
#include "gpopt/utils/CConstExprEvaluatorProxy.h"
#include "gpopt/utils/COptTasks.h"
#include "gpopt/relcache/CMDProviderRelcache.h"
#include "gpopt/config/CConfigParamMapping.h"
#include "gpopt/translate/CStateDXLToQuery.h"
#include "gpopt/translate/CTranslatorDXLToExpr.h"
#include "gpopt/translate/CTranslatorExprToDXL.h"
#include "gpopt/translate/CTranslatorUtils.h"
#include "gpopt/translate/CTranslatorQueryToDXL.h"
#include "gpopt/translate/CTranslatorPlStmtToDXL.h"
#include "gpopt/translate/CTranslatorDXLToPlStmt.h"
#include "gpopt/translate/CTranslatorDXLToQuery.h"
#include "gpopt/translate/CContextDXLToPlStmt.h"
#include "gpopt/translate/CTranslatorRelcacheToDXL.h"
#include "gpopt/eval/CConstExprEvaluatorDXL.h"

#include "cdb/cdbvars.h"
#include "utils/guc.h"

#include "gpos/base.h"
#undef setstate

#include "gpos/_api.h"
#include "gpos/common/CAutoP.h"
#include "gpos/error/CErrorHandlerStandard.h"
#include "gpos/error/CLoggerStream.h"
#include "gpos/io/COstreamFile.h"
#include "gpos/io/COstreamString.h"
#include "gpos/memory/CAutoMemoryPool.h"
#include "gpos/task/CWorkerPoolManager.h"
#include "gpos/task/CAutoTaskProxy.h"
#include "gpos/task/CTaskContext.h"
#include "gpos/task/CAutoTraceFlag.h"
#include "gpos/common/CAutoP.h"

#include "gpopt/translate/CTranslatorDXLToExpr.h"
#include "gpopt/translate/CTranslatorExprToDXL.h"

#include "gpopt/base/CAutoOptCtxt.h"
#include "gpopt/base/CQueryContext.h"
#include "gpopt/engine/CEngine.h"
#include "gpopt/engine/CEnumeratorConfig.h"
#include "gpopt/engine/CStatisticsConfig.h"
#include "gpopt/engine/CCTEConfig.h"
#include "gpopt/mdcache/CAutoMDAccessor.h"
#include "gpopt/mdcache/CMDCache.h"
#include "gpopt/minidump/CMiniDumperDXL.h"
#include "gpopt/minidump/CMinidumperUtils.h"
#include "gpopt/minidump/CSerializableStackTrace.h"
#include "gpopt/minidump/CSerializableQuery.h"
#include "gpopt/minidump/CSerializablePlan.h"
#include "gpopt/minidump/CSerializableMDAccessor.h"
#include "gpopt/optimizer/COptimizer.h"
#include "gpopt/optimizer/COptimizerConfig.h"
#include "gpopt/search/CSearchStage.h"
#include "gpopt/xforms/CXformFactory.h"
#include "gpopt/exception.h"

#include "naucrates/init.h"
#include "naucrates/traceflags/traceflags.h"

#include "naucrates/base/CQueryToDXLResult.h"

#include "naucrates/md/IMDId.h"
#include "naucrates/md/CMDRelationGPDB.h"
#include "naucrates/md/CMDIdRelStats.h"
#include "naucrates/md/CMDIdColStats.h"

#include "naucrates/md/CSystemId.h"
#include "naucrates/md/IMDRelStats.h"
#include "naucrates/md/IMDColStats.h"
#include "naucrates/md/IMDTypeInt8.h"
#include "naucrates/md/CMDIdCast.h"
#include "naucrates/md/CMDIdScCmp.h"
#include "naucrates/md/CMDTypeInt8GPDB.h"

#include "naucrates/dxl/operators/CDXLNode.h"
#include "naucrates/dxl/parser/CParseHandlerDXL.h"
#include "naucrates/dxl/CDXLUtils.h"
#include "naucrates/dxl/CIdGenerator.h"
#include "naucrates/exception.h"

#include "gpdbcost/CCostModelGPDB.h"
#include "gpdbcost/CCostModelGPDBLegacy.h"


#include "gpopt/gpdbwrappers.h"

using namespace gpos;
using namespace gpopt;
using namespace gpdxl;
using namespace gpdbcost;

// size of error buffer
#define GPOPT_ERROR_BUFFER_SIZE 10 * 1024 * 1024

// definition of default AutoMemoryPool
#define AUTO_MEM_POOL(amp) CAutoMemoryPool amp(CAutoMemoryPool::ElcExc, CMemoryPoolManager::EatMalloc, optimizer_parallel)

// default id for the source system
const CSystemId sysidDefault(IMDId::EmdidGPDB, GPOS_WSZ_STR_LENGTH("GPDB"));

// array of optimizer minor exception types that trigger expected fallback to the planner
const ULONG rgulExpectedOptFallback[] =
	{
		gpopt::ExmiInvalidPlanAlternative,		// chosen plan id is outside range of possible plans
		gpopt::ExmiUnsupportedOp,				// unsupported operator
		gpopt::ExmiUnsupportedPred,				// unsupported predicate
		gpopt::ExmiUnsupportedCompositePartKey	// composite partitioning keys
	};

// array of DXL minor exception types that trigger expected fallback to the planner
const ULONG rgulExpectedDXLFallback[] =
	{
		gpdxl::ExmiMDObjUnsupported,			// unsupported metadata object
		gpdxl::ExmiQuery2DXLUnsupportedFeature,	// unsupported feature during algebrization
		gpdxl::ExmiPlStmt2DXLConversion,		// unsupported feature during plan freezing
		gpdxl::ExmiDXL2PlStmtConversion			// unsupported feature during planned statement translation
	};

// array of DXL minor exception types that error out and NOT fallback to the planner
const ULONG rgulExpectedDXLErrors[] =
	{
		gpdxl::ExmiDXL2PlStmtExternalScanError,	// external table error
		gpdxl::ExmiQuery2DXLNotNullViolation,	// not null violation
	};


//---------------------------------------------------------------------------
//	@function:
//		COptTasks::SOptContext::SOptContext
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
COptTasks::SOptContext::SOptContext()
	:
	m_szQueryDXL(NULL),
	m_pquery(NULL),
	m_szPlanDXL(NULL),
	m_pplstmt(NULL),
	m_fGeneratePlStmt(false),
	m_fSerializePlanDXL(false),
	m_fUnexpectedFailure(false),
	m_szErrorMsg(NULL)
{}


//---------------------------------------------------------------------------
//	@function:
//		COptTasks::SOptContext::Free
//
//	@doc:
//		Free all members except those pointed to by either epinInput or
//		epinOutput
//
//---------------------------------------------------------------------------
void
COptTasks::SOptContext::Free
	(
	COptTasks::SOptContext::EPin epinInput,
	COptTasks::SOptContext::EPin epinOutput
	)
{
	if (NULL != m_szQueryDXL && epinQueryDXL != epinInput && epinQueryDXL != epinOutput)
	{
		gpdb::GPDBFree(m_szQueryDXL);
	}
	
	if (NULL != m_pquery && epinQuery != epinInput && epinQuery != epinOutput)
	{
		gpdb::GPDBFree(m_pquery);
	}
	
	if (NULL != m_szPlanDXL && epinPlanDXL != epinInput && epinPlanDXL != epinOutput)
	{
		gpdb::GPDBFree(m_szPlanDXL);
	}
	
	if (NULL != m_pplstmt && epinPlStmt != epinInput && epinPlStmt != epinOutput)
	{
		gpdb::GPDBFree(m_pplstmt);
	}
	
	if (NULL != m_szErrorMsg && epinErrorMsg != epinInput && epinErrorMsg != epinOutput)
	{
		gpdb::GPDBFree(m_szErrorMsg);
	}
}


//---------------------------------------------------------------------------
//	@function:
//		COptTasks::SOptContext::PoptctxtConvert
//
//	@doc:
//		Casting function
//
//---------------------------------------------------------------------------
COptTasks::SOptContext *
COptTasks::SOptContext::PoptctxtConvert
	(
	void *pv
	)
{
	GPOS_ASSERT(NULL != pv);

	return reinterpret_cast<SOptContext*>(pv);
}


//---------------------------------------------------------------------------
//	@function:
//		COptTasks::SContextRelcacheToDXL::SContextRelcacheToDXL
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
COptTasks::SContextRelcacheToDXL::SContextRelcacheToDXL
	(
	List *plistOids,
	ULONG ulCmpt,
	const char *szFilename
	)
	:
	m_plistOids(plistOids),
	m_ulCmpt(ulCmpt),
	m_szFilename(szFilename)
{
	GPOS_ASSERT(NULL != plistOids);
}

//---------------------------------------------------------------------------
//	@function:
//		COptTasks::SContextRelcacheToDXL::PctxrelcacheConvert
//
//	@doc:
//		Casting function
//
//---------------------------------------------------------------------------
COptTasks::SContextRelcacheToDXL *
COptTasks::SContextRelcacheToDXL::PctxrelcacheConvert
	(
	void *pv
	)
{
	GPOS_ASSERT(NULL != pv);

	return reinterpret_cast<SContextRelcacheToDXL*>(pv);
}


//---------------------------------------------------------------------------
//	@function:
//		COptTasks::SEvalExprContext::PevalctxtConvert
//
//	@doc:
//		Casting function
//
//---------------------------------------------------------------------------
COptTasks::SEvalExprContext *
COptTasks::SEvalExprContext::PevalctxtConvert
	(
	void *pv
	)
{
	GPOS_ASSERT(NULL != pv);

	return reinterpret_cast<SEvalExprContext*>(pv);
}


//---------------------------------------------------------------------------
//	@function:
//		COptTasks::SOptimizeMinidumpContext::PexecmdpConvert
//
//	@doc:
//		Casting function
//
//---------------------------------------------------------------------------
COptTasks::SOptimizeMinidumpContext *
COptTasks::SOptimizeMinidumpContext::PoptmdpConvert
	(
	void *pv
	)
{
	GPOS_ASSERT(NULL != pv);

	return reinterpret_cast<SOptimizeMinidumpContext*>(pv);
}

//---------------------------------------------------------------------------
//	@function:
//		SzAllocate
//
//	@doc:
//		Allocate string buffer with protection against OOM
//
//---------------------------------------------------------------------------
CHAR *
COptTasks::SzAllocate
	(
	IMemoryPool *pmp,
	ULONG ulSize
	)
{
	CHAR *sz = NULL;
	GPOS_TRY
	{
		// allocation of string buffer may happen outside gpos_exec() function,
		// we must guard against system OOM here
#ifdef FAULT_INJECTOR
		gpdb::OptTasksFaultInjector(OptTaskAllocateStringBuffer);
#endif // FAULT_INJECTOR

		if (NULL == pmp)
		{
			sz = (CHAR *) gpdb::GPDBAlloc(ulSize);
		}
		else
		{
			sz = GPOS_NEW_ARRAY(pmp, CHAR, ulSize);
		}
	}
	GPOS_CATCH_EX(ex)
	{
		elog(ERROR, "no available memory to allocate string buffer");
	}
	GPOS_CATCH_END;

	return sz;
}


//---------------------------------------------------------------------------
//	@function:
//		SzFromWsz
//
//	@doc:
//		Return regular string from wide-character string
//
//---------------------------------------------------------------------------
CHAR *
COptTasks::SzFromWsz
	(
	const WCHAR *wsz
	)
{
	GPOS_ASSERT(NULL != wsz);

	const ULONG ulInputLength = GPOS_WSZ_LENGTH(wsz);
	const ULONG ulWCHARSize = GPOS_SIZEOF(WCHAR);
	const ULONG ulMaxLength = (ulInputLength + 1) * ulWCHARSize;

	CHAR *sz = SzAllocate(NULL, ulMaxLength);

	gpos::clib::LWcsToMbs(sz, const_cast<WCHAR *>(wsz), ulMaxLength);
	sz[ulMaxLength - 1] = '\0';

	return sz;
}

//---------------------------------------------------------------------------
//	@function:
//		COptTasks::PvMDCast
//
//	@doc:
//		Task that dumps the relcache info for a cast object into DXL
//
//---------------------------------------------------------------------------
void*
COptTasks::PvMDCast
	(
	void *pv
	)
{
	GPOS_ASSERT(NULL != pv);

	SContextRelcacheToDXL *pctxrelcache = SContextRelcacheToDXL::PctxrelcacheConvert(pv);
	GPOS_ASSERT(NULL != pctxrelcache);

	GPOS_ASSERT(2 == gpdb::UlListLength(pctxrelcache->m_plistOids));
	Oid oidSrc = gpdb::OidListNth(pctxrelcache->m_plistOids, 0);
	Oid oidDest = gpdb::OidListNth(pctxrelcache->m_plistOids, 1);

	CAutoMemoryPool amp(CAutoMemoryPool::ElcExc);
	IMemoryPool *pmp = amp.Pmp();

	DrgPimdobj *pdrgpmdobj = GPOS_NEW(pmp) DrgPimdobj(pmp);

	IMDId *pmdidCast = GPOS_NEW(pmp) CMDIdCast
			(
			CTranslatorUtils::PmdidWithVersion(pmp, oidSrc),
			CTranslatorUtils::PmdidWithVersion(pmp, oidDest)
			);

	// relcache MD provider
	CMDProviderRelcache *pmdpr = GPOS_NEW(pmp) CMDProviderRelcache(pmp);
	
	{
		CAutoMDAccessor amda(pmp, pmdpr, sysidDefault);
	
		IMDCacheObject *pmdobj = CTranslatorRelcacheToDXL::Pimdobj(pmp, amda.Pmda(), pmdidCast);
		GPOS_ASSERT(NULL != pmdobj);
	
		pdrgpmdobj->Append(pmdobj);
		pmdidCast->Release();
	
		CWStringDynamic str(pmp);
		COstreamString oss(&str);

		CDXLUtils::PstrSerializeMetadata(pmp, pdrgpmdobj, oss, true /*fSerializeHeaderFooter*/, true /*fIndent*/);
		CHAR *sz = SzFromWsz(str.Wsz());
		GPOS_ASSERT(NULL != sz);

		pctxrelcache->m_szDXL = sz;
		
		// cleanup
		pdrgpmdobj->Release();
	}
	
	return NULL;
}

//---------------------------------------------------------------------------
//	@function:
//		COptTasks::PvMDScCmp
//
//	@doc:
//		Task that dumps the relcache info for a scalar comparison object into DXL
//
//---------------------------------------------------------------------------
void*
COptTasks::PvMDScCmp
	(
	void *pv
	)
{
	GPOS_ASSERT(NULL != pv);

	SContextRelcacheToDXL *pctxrelcache = SContextRelcacheToDXL::PctxrelcacheConvert(pv);
	GPOS_ASSERT(NULL != pctxrelcache);
	GPOS_ASSERT(CmptOther > pctxrelcache->m_ulCmpt && "Incorrect comparison type specified");

	GPOS_ASSERT(2 == gpdb::UlListLength(pctxrelcache->m_plistOids));
	Oid oidLeft = gpdb::OidListNth(pctxrelcache->m_plistOids, 0);
	Oid oidRight = gpdb::OidListNth(pctxrelcache->m_plistOids, 1);
	CmpType cmpt = (CmpType) pctxrelcache->m_ulCmpt;
	
	CAutoMemoryPool amp(CAutoMemoryPool::ElcExc);
	IMemoryPool *pmp = amp.Pmp();

	DrgPimdobj *pdrgpmdobj = GPOS_NEW(pmp) DrgPimdobj(pmp);

	IMDId *pmdidScCmp = GPOS_NEW(pmp) CMDIdScCmp
			(
			CTranslatorUtils::PmdidWithVersion(pmp, oidLeft),
			CTranslatorUtils::PmdidWithVersion(pmp, oidRight),
			CTranslatorRelcacheToDXL::Ecmpt(cmpt)
			);

	// relcache MD provider
	CMDProviderRelcache *pmdpr = GPOS_NEW(pmp) CMDProviderRelcache(pmp);
	
	{
		CAutoMDAccessor amda(pmp, pmdpr, sysidDefault);
	
		IMDCacheObject *pmdobj = CTranslatorRelcacheToDXL::Pimdobj(pmp, amda.Pmda(), pmdidScCmp);
		GPOS_ASSERT(NULL != pmdobj);
	
		pdrgpmdobj->Append(pmdobj);
		pmdidScCmp->Release();
	
		CWStringDynamic str(pmp);
		COstreamString oss(&str);

		CDXLUtils::PstrSerializeMetadata(pmp, pdrgpmdobj, oss, true /*fSerializeHeaderFooter*/, true /*fIndent*/);
		CHAR *sz = SzFromWsz(str.Wsz());
		GPOS_ASSERT(NULL != sz);

		pctxrelcache->m_szDXL = sz;
		
		// cleanup
		pdrgpmdobj->Release();
	}
	
	return NULL;
}

//---------------------------------------------------------------------------
//	@function:
//		COptTasks::Execute
//
//	@doc:
//		Execute a task using GPOS. TODO extend gpos to provide
//		this functionality
//
//---------------------------------------------------------------------------
void
COptTasks::Execute
	(
	void *(*pfunc) (void *) ,
	void *pfuncArg
	)
{
	Assert(pfunc);

	if (optimizer_parallel)
	{
		// be-aware that parallel optimizer mode may conflict with GPDB signal handlers,
		// this mode should be avoided unless optimizer is spawned in a different process
		if (gpos_set_threads(4, 4))
		{
			elog(ERROR, "unable to set number of threads in gpos");
			return;
		}
	}

	// initialize DXL support
	InitDXL();

	bool abort_flag = false;

	CAutoMemoryPool amp(CAutoMemoryPool::ElcNone, CMemoryPoolManager::EatMalloc, optimizer_parallel);
	IMemoryPool *pmp = amp.Pmp();
	CHAR *err_buf = SzAllocate(pmp, GPOPT_ERROR_BUFFER_SIZE);

	gpos_exec_params params;
	params.func = pfunc;
	params.arg = pfuncArg;
	params.stack_start = &params;
	params.error_buffer = err_buf;
	params.error_buffer_size = GPOPT_ERROR_BUFFER_SIZE;
	params.abort_requested = &abort_flag;

	// execute task and send log message to server log
	(void) gpos_exec(&params);

	if ('\0' != err_buf[0])
	{
		elog(LOG, "%s", SzFromWsz((WCHAR *)err_buf));
	}

	GPOS_DELETE_ARRAY(err_buf);
}


//---------------------------------------------------------------------------
//	@function:
//		COptTasks::PvDXLFromQueryTask
//
//	@doc:
//		task that does the translation from query to XML
//
//---------------------------------------------------------------------------
void*
COptTasks::PvDXLFromQueryTask
	(
	void *pv
	)
{
	GPOS_ASSERT(NULL != pv);

	SOptContext *poctx = SOptContext::PoptctxtConvert(pv);

	GPOS_ASSERT(NULL != poctx->m_pquery);
	GPOS_ASSERT(NULL == poctx->m_szQueryDXL);

	AUTO_MEM_POOL(amp);
	IMemoryPool *pmp = amp.Pmp();

	// ColId generator
	CIdGenerator *pidgtorCol = GPOS_NEW(pmp) CIdGenerator(GPDXL_COL_ID_START);
	CIdGenerator *pidgtorCTE = GPOS_NEW(pmp) CIdGenerator(GPDXL_CTE_ID_START);

	// map that stores gpdb att to optimizer col mapping
	CMappingVarColId *pmapvarcolid = GPOS_NEW(pmp) CMappingVarColId(pmp);

	// relcache MD provider
	CMDProviderRelcache *pmdpr = GPOS_NEW(pmp) CMDProviderRelcache(pmp);

	{
		CAutoMDAccessor amda(pmp, pmdpr, sysidDefault);

		CAutoP<CTranslatorQueryToDXL> ptrquerytodxl;
		ptrquerytodxl = CTranslatorQueryToDXL::PtrquerytodxlInstance
						(
						pmp,
						amda.Pmda(),
						pidgtorCol,
						pidgtorCTE,
						pmapvarcolid,
						(Query*)poctx->m_pquery,
						0 /* ulQueryLevel */
						);
		CDXLNode *pdxlnQuery = ptrquerytodxl->PdxlnFromQuery();
		DrgPdxln *pdrgpdxlnQueryOutput = ptrquerytodxl->PdrgpdxlnQueryOutput();
		DrgPdxln *pdrgpdxlnCTE = ptrquerytodxl->PdrgpdxlnCTE();
		GPOS_ASSERT(NULL != pdrgpdxlnQueryOutput);

		GPOS_DELETE(pidgtorCol);
		GPOS_DELETE(pidgtorCTE);

		CWStringDynamic str(pmp);
		COstreamString oss(&str);

		CWStringDynamic *pstrDXL = 
				CDXLUtils::PstrSerializeQuery
							(
							pmp, 
							pdxlnQuery, 
							pdrgpdxlnQueryOutput, 
							pdrgpdxlnCTE, 
							true, // fSerializeHeaderFooter
							true // fIndent
							);
		poctx->m_szQueryDXL = SzFromWsz(pstrDXL->Wsz());

		// clean up
		pdxlnQuery->Release();
		GPOS_DELETE(pstrDXL);
	}

	return NULL;
}


//---------------------------------------------------------------------------
//	@function:
//		COptTasks::Pplstmt
//
//	@doc:
//		Translate a DXL tree into a planned statement
//
//---------------------------------------------------------------------------
PlannedStmt *
COptTasks::Pplstmt
	(
	IMemoryPool *pmp,
	CMDAccessor *pmda,
	const CDXLNode *pdxln
	)
{

	GPOS_ASSERT(NULL != pmda);
	GPOS_ASSERT(NULL != pdxln);

	CIdGenerator idgtorPlanId(1 /* ulStartId */);
	CIdGenerator idgtorMotionId(1 /* ulStartId */);
	CIdGenerator idgtorParamId(0 /* ulStartId */);

	List *plRTable = NULL;
	List *plSubplans = NULL;

	CContextDXLToPlStmt ctxdxltoplstmt
							(
							pmp,
							&idgtorPlanId,
							&idgtorMotionId,
							&idgtorParamId,
							&plRTable,
							&plSubplans
							);
	
	// translate DXL -> PlannedStmt
	CTranslatorDXLToPlStmt trdxltoplstmt(pmp, pmda, &ctxdxltoplstmt, gpdb::UlSegmentCountGP());
	return trdxltoplstmt.PplstmtFromDXL(pdxln);
}


//---------------------------------------------------------------------------
//	@function:
//		COptTasks::PdrgPssLoad
//
//	@doc:
//		Load search strategy from given file
//
//---------------------------------------------------------------------------
DrgPss *
COptTasks::PdrgPssLoad
	(
	IMemoryPool *pmp,
	char *szPath
	)
{
	DrgPss *pdrgpss = NULL;
	CParseHandlerDXL *pphDXL = NULL;

	GPOS_TRY
	{
		if (NULL != szPath)
		{
			pphDXL = CDXLUtils::PphdxlParseDXLFile(pmp, szPath, NULL);
			if (NULL != pphDXL)
			{
				elog(DEBUG2, "\n[OPT]: Using search strategy in (%s)", szPath);

				pdrgpss = pphDXL->Pdrgpss();
				pdrgpss->AddRef();
			}
		}
	}
	GPOS_CATCH_EX(ex)
	{
		GPOS_RESET_EX;

		elog(DEBUG2, "\n[OPT]: Using default search strategy");
	}
	GPOS_CATCH_END;

	GPOS_DELETE(pphDXL);

	return pdrgpss;
}

//---------------------------------------------------------------------------
//	@function:
//		COptTasks::PoconfCreate
//
//	@doc:
//		Create the optimizer configuration
//
//---------------------------------------------------------------------------
COptimizerConfig *
COptTasks::PoconfCreate
	(
	IMemoryPool *pmp,
	ICostModel *pcm
	)
{
	// get chosen plan number, cost threshold
	ULLONG ullPlanId =  (ULLONG) optimizer_plan_id;
	ULLONG ullSamples = (ULLONG) optimizer_samples_number;
	DOUBLE dCostThreshold = (DOUBLE) optimizer_cost_threshold;

	DOUBLE dDampingFactorFilter = (DOUBLE) optimizer_damping_factor_filter;
	DOUBLE dDampingFactorJoin = (DOUBLE) optimizer_damping_factor_join;
	DOUBLE dDampingFactorGroupBy = (DOUBLE) optimizer_damping_factor_groupby;

	ULONG ulCTEInliningCutoff =  (ULONG) optimizer_cte_inlining_bound;

	return GPOS_NEW(pmp) COptimizerConfig
						(
						GPOS_NEW(pmp) CEnumeratorConfig(pmp, ullPlanId, ullSamples, dCostThreshold),
						GPOS_NEW(pmp) CStatisticsConfig(pmp, dDampingFactorFilter, dDampingFactorJoin, dDampingFactorGroupBy),
						GPOS_NEW(pmp) CCTEConfig(ulCTEInliningCutoff),
						pcm
						);
}


//---------------------------------------------------------------------------
//	@function:
//		COptTasks::FExceptionFound
//
//	@doc:
//		Lookup given exception type in the given array
//
//---------------------------------------------------------------------------
BOOL
COptTasks::FExceptionFound
	(
	gpos::CException &exc,
	const ULONG *pulExceptions,
	ULONG ulSize
	)
{
	GPOS_ASSERT(NULL != pulExceptions);

	ULONG ulMinor = exc.UlMinor();
	BOOL fFound = false;
	for (ULONG ul = 0; !fFound && ul < ulSize; ul++)
	{
		fFound = (pulExceptions[ul] == ulMinor);
	}

	return fFound;
}


//---------------------------------------------------------------------------
//	@function:
//		COptTasks::FUnexpectedFailure
//
//	@doc:
//		Check if given exception is an unexpected reason for failing to
//		produce a plan
//
//---------------------------------------------------------------------------
BOOL
COptTasks::FUnexpectedFailure
	(
	gpos::CException &exc
	)
{
	ULONG ulMajor = exc.UlMajor();

	BOOL fExpectedOptFailure =
		gpopt::ExmaGPOPT == ulMajor &&
		FExceptionFound(exc, rgulExpectedOptFallback, GPOS_ARRAY_SIZE(rgulExpectedOptFallback));

	BOOL fExpectedDXLFailure =
		gpdxl::ExmaDXL == ulMajor &&
		FExceptionFound(exc, rgulExpectedDXLFallback, GPOS_ARRAY_SIZE(rgulExpectedDXLFallback));

	return (!fExpectedOptFailure && !fExpectedDXLFailure);
}

//---------------------------------------------------------------------------
//	@function:
//		COptTasks::FErrorOut
//
//	@doc:
//		Check if given exception should error out
//
//---------------------------------------------------------------------------
BOOL
COptTasks::FErrorOut
	(
	gpos::CException &exc
	)
{
	return
		gpdxl::ExmaDXL == exc.UlMajor() &&
		FExceptionFound(exc, rgulExpectedDXLErrors, GPOS_ARRAY_SIZE(rgulExpectedDXLErrors));
}

//---------------------------------------------------------------------------
//		@function:
//			COptTasks::SetCostModelParams
//
//      @doc:
//			Set cost model parameters
//
//---------------------------------------------------------------------------
void
COptTasks::SetCostModelParams
	(
	ICostModel *pcm
	)
{
	GPOS_ASSERT(NULL != pcm);

	if (optimizer_nestloop_factor > 1.0)
	{
		// change NLJ cost factor
		ICostModelParams::SCostParam *pcp = NULL;
		if (OPTIMIZER_GPDB_CALIBRATED == optimizer_cost_model)
		{
			pcp = pcm->Pcp()->PcpLookup(CCostModelParamsGPDB::EcpNLJFactor);
		}
		else
		{
			pcp = pcm->Pcp()->PcpLookup(CCostModelParamsGPDBLegacy::EcpNLJFactor);
		}
		CDouble dNLJFactor(optimizer_nestloop_factor);
		pcm->Pcp()->SetParam(pcp->UlId(), dNLJFactor, dNLJFactor - 0.5, dNLJFactor + 0.5);
	}
}


//---------------------------------------------------------------------------
//      @function:
//			COptTasks::Pcm
//
//      @doc:
//			Generate an instance of optimizer cost model
//
//---------------------------------------------------------------------------
ICostModel *
COptTasks::Pcm
	(
	IMemoryPool *pmp,
	ULONG ulSegments
	)
{
	ICostModel *pcm = NULL;
	if (OPTIMIZER_GPDB_CALIBRATED == optimizer_cost_model)
	{
		pcm = GPOS_NEW(pmp) CCostModelGPDB(pmp, ulSegments);
	}
	else
	{
		pcm = GPOS_NEW(pmp) CCostModelGPDBLegacy(pmp, ulSegments);
	}

	SetCostModelParams(pcm);

	return pcm;
}


//---------------------------------------------------------------------------
//	@function:
//		COptTasks::PvOptimizeTask
//
//	@doc:
//		task that does the optimizes query to physical DXL
//
//---------------------------------------------------------------------------
void*
COptTasks::PvOptimizeTask
	(
	void *pv
	)
{
	GPOS_ASSERT(NULL != pv);
	SOptContext *poctx = SOptContext::PoptctxtConvert(pv);

	GPOS_ASSERT(NULL != poctx->m_pquery);
	GPOS_ASSERT(NULL == poctx->m_szPlanDXL);
	GPOS_ASSERT(NULL == poctx->m_pplstmt);

	// initially assume no unexpected failure
	poctx->m_fUnexpectedFailure = false;

	AUTO_MEM_POOL(amp);
	IMemoryPool *pmp = amp.Pmp();

	// initialize metadata cache
	if (!CMDCache::FInitialized())
	{
		CMDCache::Init();
	}

	// load search strategy
	DrgPss *pdrgpss = PdrgPssLoad(pmp, optimizer_search_strategy_path);

	CBitSet *pbsTraceFlags = NULL;
	CBitSet *pbsEnabled = NULL;
	CBitSet *pbsDisabled = NULL;
	CDXLNode *pdxlnPlan = NULL;

	DrgPmdid *pdrgmdidCol = NULL;
	HMMDIdMDId *phmmdidRel = NULL;

	GPOS_TRY
	{
		// set trace flags
		pbsTraceFlags = CConfigParamMapping::PbsPack(pmp, CXform::ExfSentinel);
		SetTraceflags(pmp, pbsTraceFlags, &pbsEnabled, &pbsDisabled);

		// set up relcache MD provider
		CMDProviderRelcache *pmdpRelcache = GPOS_NEW(pmp) CMDProviderRelcache(pmp);

		{
			// scope for MD accessor
			CMDAccessor mda(pmp, CMDCache::Pcache(), sysidDefault, pmdpRelcache);

			// ColId generator
			CIdGenerator idgtorColId(GPDXL_COL_ID_START);
			CIdGenerator idgtorCTE(GPDXL_CTE_ID_START);

			// map that stores gpdb att to optimizer col mapping
			CMappingVarColId *pmapvarcolid = GPOS_NEW(pmp) CMappingVarColId(pmp);

			ULONG ulSegments = gpdb::UlSegmentCountGP();
			ULONG ulSegmentsForCosting = optimizer_segments;
			if (0 == ulSegmentsForCosting)
			{
				ulSegmentsForCosting = ulSegments;
			}

			CAutoP<CTranslatorQueryToDXL> ptrquerytodxl;
			ptrquerytodxl = CTranslatorQueryToDXL::PtrquerytodxlInstance
							(
							pmp,
							&mda,
							&idgtorColId,
							&idgtorCTE,
							pmapvarcolid,
							(Query*) poctx->m_pquery,
							0 /* ulQueryLevel */
							);

			ICostModel *pcm = Pcm(pmp, ulSegmentsForCosting);
			COptimizerConfig *pocconf = PoconfCreate(pmp, pcm);
			CConstExprEvaluatorProxy ceevalproxy(pmp, &mda);
			IConstExprEvaluator *pceeval =
					GPOS_NEW(pmp) CConstExprEvaluatorDXL(pmp, &mda, &ceevalproxy);

			// preload metadata if optimizer uses multiple threads
			if (optimizer_parallel)
			{
				// install opt context in TLS
				pocconf->AddRef();
				pceeval->AddRef();
				CAutoOptCtxt aoc(pmp, &mda, pceeval, pocconf);
				CTranslatorUtils::PreloadMD(pmp, &mda, sysidDefault, (Query*) poctx->m_pquery);
			}

			CDXLNode *pdxlnQuery = ptrquerytodxl->PdxlnFromQuery();
			DrgPdxln *pdrgpdxlnQueryOutput = ptrquerytodxl->PdrgpdxlnQueryOutput();
			DrgPdxln *pdrgpdxlnCTE = ptrquerytodxl->PdrgpdxlnCTE();
			GPOS_ASSERT(NULL != pdrgpdxlnQueryOutput);

			BOOL fMasterOnly = !optimizer_enable_motions ||
						(!optimizer_enable_motions_masteronly_queries && !ptrquerytodxl->FHasDistributedTables());
			CAutoTraceFlag atf(EopttraceDisableMotions, fMasterOnly);

			pdxlnPlan = COptimizer::PdxlnOptimize
									(
									pmp,
									&mda,
									pdxlnQuery,
									pdrgpdxlnQueryOutput,
									pdrgpdxlnCTE,
									pceeval,
									ulSegments,
									gp_session_id,
									gp_command_count,
									pdrgpss,
									pocconf
									);

			if (poctx->m_fSerializePlanDXL)
			{
				// serialize DXL to xml
				CWStringDynamic *pstrPlan = CDXLUtils::PstrSerializePlan(pmp, pdxlnPlan, pocconf->Pec()->UllPlanId(), pocconf->Pec()->UllPlanSpaceSize(), true /*fSerializeHeaderFooter*/, true /*fIndent*/);
				poctx->m_szPlanDXL = SzFromWsz(pstrPlan->Wsz());
				GPOS_DELETE(pstrPlan);
			}

			// translate DXL->PlStmt only when needed
			if (poctx->m_fGeneratePlStmt)
			{
				poctx->m_pplstmt = (PlannedStmt *) gpdb::PvCopyObject(Pplstmt(pmp, &mda, pdxlnPlan));
			}

			CStatisticsConfig *pstatsconf = pocconf->Pstatsconf();
			pdrgmdidCol = GPOS_NEW(pmp) DrgPmdid(pmp);
			pstatsconf->CollectMissingStatsColumns(pdrgmdidCol);

			phmmdidRel = GPOS_NEW(pmp) HMMDIdMDId(pmp);
			PrintMissingStatsWarning(pmp, &mda, pdrgmdidCol, phmmdidRel);

			phmmdidRel->Release();
			pdrgmdidCol->Release();

			pceeval->Release();
			pdxlnQuery->Release();
			pocconf->Release();
			pdxlnPlan->Release();
		}
	}
	GPOS_CATCH_EX(ex)
	{
		ResetTraceflags(pbsEnabled, pbsDisabled);
		CRefCount::SafeRelease(phmmdidRel);
		CRefCount::SafeRelease(pdrgmdidCol);
		CRefCount::SafeRelease(pbsEnabled);
		CRefCount::SafeRelease(pbsDisabled);
		CRefCount::SafeRelease(pbsTraceFlags);
		CRefCount::SafeRelease(pdxlnPlan);
		CMDCache::Shutdown();

		if (GPOS_MATCH_EX(ex, gpdxl::ExmaGPDB, gpdxl::ExmiGPDBError))
		{
			elog(DEBUG1, "GPDB Exception. Please check log for more information.");
		}
		else if (FErrorOut(ex))
		{
			IErrorContext *perrctxt = CTask::PtskSelf()->Perrctxt();
			poctx->m_szErrorMsg = SzFromWsz(perrctxt->WszMsg());
		}
		else
		{
			poctx->m_fUnexpectedFailure = FUnexpectedFailure(ex);
		}
		GPOS_RETHROW(ex);
	}
	GPOS_CATCH_END;

	// cleanup
	ResetTraceflags(pbsEnabled, pbsDisabled);
	CRefCount::SafeRelease(pbsEnabled);
	CRefCount::SafeRelease(pbsDisabled);
	CRefCount::SafeRelease(pbsTraceFlags);
	if (optimizer_release_mdcache)
	{
		CMDCache::Shutdown();
	}

	return NULL;
}


//---------------------------------------------------------------------------
//	@function:
//		COptTasks::PrintMissingStatsWarning
//
//	@doc:
//		Print warning messages for columns with missing statistics
//
//---------------------------------------------------------------------------
void
COptTasks::PrintMissingStatsWarning
	(
	IMemoryPool *pmp,
	CMDAccessor *pmda,
	DrgPmdid *pdrgmdidCol,
	HMMDIdMDId *phmmdidRel
	)
{
	GPOS_ASSERT(NULL != pmda);
	GPOS_ASSERT(NULL != pdrgmdidCol);
	GPOS_ASSERT(NULL != phmmdidRel);

	CWStringDynamic str(pmp);
	COstreamString oss(&str);

	const ULONG ulMissingColStats = pdrgmdidCol->UlLength();
	for (ULONG ul = 0; ul < ulMissingColStats; ul++)
	{
		IMDId *pmdid = (*pdrgmdidCol)[ul];
		CMDIdColStats *pmdidColStats = CMDIdColStats::PmdidConvert(pmdid);

		IMDId *pmdidRel = pmdidColStats->PmdidRel();
		const ULONG ulPos = pmdidColStats->UlPos();
		const IMDRelation *pmdrel = pmda->Pmdrel(pmdidRel);

		if (IMDRelation::ErelstorageExternal != pmdrel->Erelstorage())
		{
			if (NULL == phmmdidRel->PtLookup(pmdidRel))
			{
				if (0 != ul)
				{
					oss << ", ";
				}

				pmdidRel->AddRef();
				pmdidRel->AddRef();
				phmmdidRel->FInsert(pmdidRel, pmdidRel);
				oss << pmdrel->Mdname().Pstr()->Wsz();
			}

			CMDName mdname = pmdrel->Pmdcol(ulPos)->Mdname();
			elog(LOG, "Missing statistics for column: %s.%s", SzFromWsz(pmdrel->Mdname().Pstr()->Wsz()), SzFromWsz(mdname.Pstr()->Wsz()));
		}
	}

	if (0 < phmmdidRel->UlEntries())
	{
		ereport(NOTICE,
				(errcode(ERRCODE_SUCCESSFUL_COMPLETION),
				errmsg("One or more columns in the following table(s) do not have statistics: %s", SzFromWsz(str.Wsz())),
				errhint("For non-partitioned tables, run analyze <table_name>(<column_list>)."
						 " For partitioned tables, run analyze rootpartition <table_name>(<column_list>)."
						 " See log for columns missing statistics."),
				errOmitLocation(true)));
	}

}


//---------------------------------------------------------------------------
//	@function:
//		COptTasks::PvOptimizeMinidumpTask
//
//	@doc:
//		Task that loads and optimizes a minidump and returns the result as string-serialized DXL
//
//---------------------------------------------------------------------------
void*
COptTasks::PvOptimizeMinidumpTask
	(
	void *pv
	)
{
	GPOS_ASSERT(NULL != pv);

	SOptimizeMinidumpContext *poptmdpctxt = SOptimizeMinidumpContext::PoptmdpConvert(pv);
	GPOS_ASSERT(NULL != poptmdpctxt->m_szFileName);
	GPOS_ASSERT(NULL == poptmdpctxt->m_szDXLResult);

	AUTO_MEM_POOL(amp);
	IMemoryPool *pmp = amp.Pmp();

	ULONG ulSegments = gpdb::UlSegmentCountGP();
	ULONG ulSegmentsForCosting = optimizer_segments;
	if (0 == ulSegmentsForCosting)
	{
		ulSegmentsForCosting = ulSegments;
	}

	ICostModel *pcm = Pcm(pmp, ulSegmentsForCosting);
	COptimizerConfig *pocconf = PoconfCreate(pmp, pcm);
	CDXLNode *pdxlnResult = NULL;

	GPOS_TRY
	{
		pdxlnResult = CMinidumperUtils::PdxlnExecuteMinidump(pmp, poptmdpctxt->m_szFileName, ulSegments, gp_session_id, gp_command_count, pocconf);
	}
	GPOS_CATCH_EX(ex)
	{
		CRefCount::SafeRelease(pdxlnResult);
		CRefCount::SafeRelease(pocconf);
		GPOS_RETHROW(ex);
	}
	GPOS_CATCH_END;
	CWStringDynamic *pstrDXL =
			CDXLUtils::PstrSerializePlan
						(
						pmp,
						pdxlnResult,
						0,  // ullPlanId
						0,  // ullPlanSpaceSize
						true, // fSerializeHeaderFooter
						true // fIndent
						);
	poptmdpctxt->m_szDXLResult = SzFromWsz(pstrDXL->Wsz());
	GPOS_DELETE(pstrDXL);
	CRefCount::SafeRelease(pdxlnResult);
	pocconf->Release();

	return NULL;
}


//---------------------------------------------------------------------------
//	@function:
//		COptTasks::PvDXLFromPlstmtTask
//
//	@doc:
//		task that does the translation from planned stmt to XML
//
//---------------------------------------------------------------------------
void*
COptTasks::PvDXLFromPlstmtTask
	(
	void *pv
	)
{
	GPOS_ASSERT(NULL != pv);

	SOptContext *poctx = SOptContext::PoptctxtConvert(pv);

	GPOS_ASSERT(NULL != poctx->m_pplstmt);
	GPOS_ASSERT(NULL == poctx->m_szPlanDXL);

	AUTO_MEM_POOL(amp);
	IMemoryPool *pmp = amp.Pmp();

	CIdGenerator idgtor(1);

	// relcache MD provider
	CMDProviderRelcache *pmdpr = GPOS_NEW(pmp) CMDProviderRelcache(pmp);

	{
		CAutoMDAccessor amda(pmp, pmdpr, sysidDefault);

		CMappingParamIdScalarId mapps(pmp);

		CTranslatorPlStmtToDXL tplstmtdxl(pmp, amda.Pmda(), &idgtor, (PlannedStmt*) poctx->m_pplstmt, &mapps);
		CDXLNode *pdxlnPlan = tplstmtdxl.PdxlnFromPlstmt();

		GPOS_ASSERT(NULL != pdxlnPlan);

		CWStringDynamic str(pmp);
		COstreamString oss(&str);

		// get chosen plan number
		ULLONG ullPlanId =  (ULLONG) optimizer_plan_id;
		CWStringDynamic *pstrDXL = CDXLUtils::PstrSerializePlan(pmp, pdxlnPlan, ullPlanId, 0 /*ullPlanSpaceSize*/, true /*fSerializeHeaderFooter*/, true /*fIndent*/);

		poctx->m_szPlanDXL = SzFromWsz(pstrDXL->Wsz());

		// cleanup
		GPOS_DELETE(pstrDXL);
		pdxlnPlan->Release();
	}

	return NULL;
}


//---------------------------------------------------------------------------
//	@function:
//		COptTasks::PvPlstmtFromDXLTask
//
//	@doc:
//		task that does the translation from xml to dxl to pplstmt
//
//---------------------------------------------------------------------------
void*
COptTasks::PvPlstmtFromDXLTask
	(
	void *pv
	)
{
	GPOS_ASSERT(NULL != pv);

	SOptContext *poctx = SOptContext::PoptctxtConvert(pv);

	GPOS_ASSERT(NULL == poctx->m_pquery);
	GPOS_ASSERT(NULL != poctx->m_szPlanDXL);

	AUTO_MEM_POOL(amp);
	IMemoryPool *pmp = amp.Pmp();

	CWStringDynamic str(pmp);
	COstreamString oss(&str);

	ULLONG ullPlanId = 0;
	ULLONG ullPlanSpaceSize = 0;
	CDXLNode *pdxlnOriginal =
		CDXLUtils::PdxlnParsePlan(pmp, poctx->m_szPlanDXL, NULL /*XSD location*/, &ullPlanId, &ullPlanSpaceSize);

	CIdGenerator idgtorPlanId(1);
	CIdGenerator idgtorMotionId(1);
	CIdGenerator idgtorParamId(0);

	List *plRTable = NULL;
	List *plSubplans = NULL;

	CContextDXLToPlStmt ctxdxlplstmt
							(
							pmp,
							&idgtorPlanId,
							&idgtorMotionId,
							&idgtorParamId,
							&plRTable,
							&plSubplans
							);

	// relcache MD provider
	CMDProviderRelcache *pmdpr = GPOS_NEW(pmp) CMDProviderRelcache(pmp);

	{
		CAutoMDAccessor amda(pmp, pmdpr, sysidDefault);

		// translate DXL -> PlannedStmt
		CTranslatorDXLToPlStmt trdxltoplstmt(pmp, amda.Pmda(), &ctxdxlplstmt, gpdb::UlSegmentCountGP());
		PlannedStmt *pplstmt = trdxltoplstmt.PplstmtFromDXL(pdxlnOriginal);
		if (optimizer_print_plan)
		{
			elog(NOTICE, "Plstmt: %s", gpdb::SzNodeToString(pplstmt));
		}

		GPOS_ASSERT(NULL != pplstmt);
		GPOS_ASSERT(NULL != CurrentMemoryContext);

		poctx->m_pplstmt = (PlannedStmt *) gpdb::PvCopyObject(pplstmt);
	}

	// cleanup
	pdxlnOriginal->Release();

	return NULL;
}


//---------------------------------------------------------------------------
//	@function:
//		COptTasks::PvQueryFromDXLTask
//
//	@doc:
//		task that does the translation from xml to dxl to pquery
//
//---------------------------------------------------------------------------
void*
COptTasks::PvQueryFromDXLTask
	(
	void *pv
	)
{
	GPOS_ASSERT(NULL != pv);

	SOptContext *poctx = SOptContext::PoptctxtConvert(pv);

	GPOS_ASSERT(NULL == poctx->m_pquery);
	GPOS_ASSERT(NULL != poctx->m_szQueryDXL);

	AUTO_MEM_POOL(amp);
	IMemoryPool *pmp = amp.Pmp();

	CWStringDynamic str(pmp);
	COstreamString oss(&str);

	// parse the DXL
	CQueryToDXLResult *ptroutput = CDXLUtils::PdxlnParseDXLQuery(pmp, poctx->m_szQueryDXL, NULL);

	GPOS_ASSERT(NULL != ptroutput->Pdxln());

	// relcache MD provider
	CMDProviderRelcache *pmdpr = GPOS_NEW(pmp) CMDProviderRelcache(pmp);

	{
		CAutoMDAccessor amda(pmp, pmdpr, sysidDefault);

		// initialize hash table that maintains the mapping between ColId and Var
		TEMap *ptemap = GPOS_NEW(pmp) TEMap(pmp);

		CTranslatorDXLToQuery trdxlquery(pmp, amda.Pmda(), gpdb::UlSegmentCountGP());
		CStateDXLToQuery statedxltoquery(pmp);

		Query *pquery = trdxlquery.PqueryFromDXL(ptroutput->Pdxln(), ptroutput->PdrgpdxlnOutputCols(), &statedxltoquery, ptemap, GPDXL_QUERY_LEVEL);

		CRefCount::SafeRelease(ptemap);
		GPOS_DELETE(ptroutput);

		GPOS_ASSERT(NULL != pquery);
		GPOS_ASSERT(NULL != CurrentMemoryContext);

		poctx->m_pquery = pquery;
	}

	return NULL;
}


//---------------------------------------------------------------------------
//	@function:
//		COptTasks::PvDXLFromMDObjsTask
//
//	@doc:
//		task that does dumps the relcache info for an object into DXL
//
//---------------------------------------------------------------------------
void*
COptTasks::PvDXLFromMDObjsTask
	(
	void *pv
	)
{
	GPOS_ASSERT(NULL != pv);

	SContextRelcacheToDXL *pctxrelcache = SContextRelcacheToDXL::PctxrelcacheConvert(pv);
	GPOS_ASSERT(NULL != pctxrelcache);

	AUTO_MEM_POOL(amp);
	IMemoryPool *pmp = amp.Pmp();

	DrgPimdobj *pdrgpmdobj = GPOS_NEW(pmp) DrgPimdobj(pmp, 1024, 1024);

	// relcache MD provider
	CMDProviderRelcache *pmdp = GPOS_NEW(pmp) CMDProviderRelcache(pmp);
	{
		CAutoMDAccessor amda(pmp, pmdp, sysidDefault);
		ListCell *plc = NULL;
		ForEach (plc, pctxrelcache->m_plistOids)
		{
			Oid oid = lfirst_oid(plc);
			// get object from relcache
			CMDIdGPDB *pmdid = CTranslatorUtils::PmdidWithVersion(pmp, oid);

			IMDCacheObject *pimdobj = CTranslatorRelcacheToDXL::Pimdobj(pmp, amda.Pmda(), pmdid);
			GPOS_ASSERT(NULL != pimdobj);

			pdrgpmdobj->Append(pimdobj);
			pmdid->Release();
		}

		if (pctxrelcache->m_szFilename)
		{
			COstreamFile cofs(pctxrelcache->m_szFilename);

			CDXLUtils::PstrSerializeMetadata(pmp, pdrgpmdobj, cofs, true /*fSerializeHeaderFooter*/, true /*fIndent*/);
		}
		else
		{
			CWStringDynamic str(pmp);
			COstreamString oss(&str);

			CDXLUtils::PstrSerializeMetadata(pmp, pdrgpmdobj, oss, true /*fSerializeHeaderFooter*/, true /*fIndent*/);

			CHAR *sz = SzFromWsz(str.Wsz());

			GPOS_ASSERT(NULL != sz);

			pctxrelcache->m_szDXL = sz;
		}
	}
	// cleanup
	pdrgpmdobj->Release();

	return NULL;
}


//---------------------------------------------------------------------------
//	@function:
//		COptTasks::PvDXLFromRelStatsTask
//
//	@doc:
//		task that dumps relstats info for a table in DXL
//
//---------------------------------------------------------------------------
void*
COptTasks::PvDXLFromRelStatsTask
	(
	void *pv
	)
{
	GPOS_ASSERT(NULL != pv);

	SContextRelcacheToDXL *pctxrelcache = SContextRelcacheToDXL::PctxrelcacheConvert(pv);
	GPOS_ASSERT(NULL != pctxrelcache);

	AUTO_MEM_POOL(amp);
	IMemoryPool *pmp = amp.Pmp();

	// relcache MD provider
	CMDProviderRelcache *pmdpr = GPOS_NEW(pmp) CMDProviderRelcache(pmp);
	CAutoMDAccessor amda(pmp, pmdpr, sysidDefault);
	ICostModel *pcm = Pcm(pmp, gpdb::UlSegmentCountGP());
	CAutoOptCtxt aoc(pmp, amda.Pmda(), NULL /*pceeval*/, pcm);

	DrgPimdobj *pdrgpmdobj = GPOS_NEW(pmp) DrgPimdobj(pmp);

	ListCell *plc = NULL;
	ForEach (plc, pctxrelcache->m_plistOids)
	{
		Oid oidRelation = lfirst_oid(plc);

		// get object from relcache
		CMDIdGPDB *pmdid = CTranslatorUtils::PmdidWithVersion(pmp, oidRelation);

		// generate mdid for relstats
		CMDIdRelStats *pmdidRelStats = GPOS_NEW(pmp) CMDIdRelStats(pmdid);
		IMDRelStats *pimdobjrelstats = const_cast<IMDRelStats *>(amda.Pmda()->Pmdrelstats(pmdidRelStats));
		pdrgpmdobj->Append(dynamic_cast<IMDCacheObject *>(pimdobjrelstats));

		// extract out column stats for this relation
		Relation rel = gpdb::RelGetRelation(oidRelation);
		ULONG ulPosCounter = 0;
		for (ULONG ul = 0; ul < ULONG(rel->rd_att->natts); ul++)
		{
			if (!rel->rd_att->attrs[ul]->attisdropped)
			{
				pmdid->AddRef();
				CMDIdColStats *pmdidColStats = GPOS_NEW(pmp) CMDIdColStats(pmdid, ulPosCounter);
				ulPosCounter++;
				IMDColStats *pimdobjcolstats = const_cast<IMDColStats *>(amda.Pmda()->Pmdcolstats(pmdidColStats));
				pdrgpmdobj->Append(dynamic_cast<IMDCacheObject *>(pimdobjcolstats));
				pmdidColStats->Release();
			}
		}
		gpdb::CloseRelation(rel);
		pmdidRelStats->Release();
	}

	if (pctxrelcache->m_szFilename)
	{
		COstreamFile cofs(pctxrelcache->m_szFilename);

		CDXLUtils::PstrSerializeMetadata(pmp, pdrgpmdobj, cofs, true /*fSerializeHeaderFooter*/, true /*fIndent*/);
	}
	else
	{
		CWStringDynamic str(pmp);
		COstreamString oss(&str);

		CDXLUtils::PstrSerializeMetadata(pmp, pdrgpmdobj, oss, true /*fSerializeHeaderFooter*/, true /*fIndent*/);

		CHAR *sz = SzFromWsz(str.Wsz());

		GPOS_ASSERT(NULL != sz);

		pctxrelcache->m_szDXL = sz;
	}

	// cleanup
	pdrgpmdobj->Release();

	return NULL;
}


//---------------------------------------------------------------------------
//	@function:
//		COptTasks::PvEvalExprFromDXLTask
//
//	@doc:
//		Task that parses an XML representation of a DXL constant expression,
//		evaluates it and returns the result as a serialized DXL document.
//
//---------------------------------------------------------------------------
void*
COptTasks::PvEvalExprFromDXLTask
	(
	void *pv
	)
{
	GPOS_ASSERT(NULL != pv);
	SEvalExprContext *pevalctxt = SEvalExprContext::PevalctxtConvert(pv);

	GPOS_ASSERT(NULL != pevalctxt->m_szDXL);
	GPOS_ASSERT(NULL == pevalctxt->m_szDXLResult);

	AUTO_MEM_POOL(amp);
	IMemoryPool *pmp = amp.Pmp();

	CDXLNode *pdxlnInput = CDXLUtils::PdxlnParseScalarExpr(pmp, pevalctxt->m_szDXL, NULL /*szXSDPath*/);
	GPOS_ASSERT(NULL != pdxlnInput);

	CDXLNode *pdxlnResult = NULL;
	BOOL fReleaseCache = false;
	// initialize metadata cache
	if (!CMDCache::FInitialized())
	{
		CMDCache::Init();
		fReleaseCache = true;
	}

	GPOS_TRY
	{
		// set up relcache MD provider
		CMDProviderRelcache *pmdpRelcache = GPOS_NEW(pmp) CMDProviderRelcache(pmp);
		{
			// scope for MD accessor
			CMDAccessor mda(pmp, CMDCache::Pcache(), sysidDefault, pmdpRelcache);

			CConstExprEvaluatorProxy ceeval(pmp, &mda);
			pdxlnResult = ceeval.PdxlnEvaluateExpr(pdxlnInput);
		}
	}
	GPOS_CATCH_EX(ex)
	{
		CRefCount::SafeRelease(pdxlnResult);
		CRefCount::SafeRelease(pdxlnInput);
		if (fReleaseCache)
		{
			CMDCache::Shutdown();
		}
		// Catch GPDB exceptions
		if (GPOS_MATCH_EX(ex, gpdxl::ExmaGPDB, gpdxl::ExmiGPDBError))
		{
			elog(NOTICE, "Found non const expression. Please check log for more information.");
			GPOS_RESET_EX;
			return NULL;
		}
		if (FErrorOut(ex))
		{
			IErrorContext *perrctxt = CTask::PtskSelf()->Perrctxt();
			char *szErrorMsg = SzFromWsz(perrctxt->WszMsg());
			elog(DEBUG1, "%s", szErrorMsg);
			gpdb::GPDBFree(szErrorMsg);
		}
		GPOS_RETHROW(ex);
	}
	GPOS_CATCH_END;

	CWStringDynamic *pstrDXL =
			CDXLUtils::PstrSerializeScalarExpr
						(
						pmp,
						pdxlnResult,
						true, // fSerializeHeaderFooter
						true // fIndent
						);
	pevalctxt->m_szDXLResult = SzFromWsz(pstrDXL->Wsz());
	GPOS_DELETE(pstrDXL);
	CRefCount::SafeRelease(pdxlnResult);
	pdxlnInput->Release();

	if (fReleaseCache)
	{
		CMDCache::Shutdown();
	}

	return NULL;
}


//---------------------------------------------------------------------------
//	@function:
//		COptTasks::SzOptimize
//
//	@doc:
//		optimizes a query to physical DXL
//
//---------------------------------------------------------------------------
char *
COptTasks::SzOptimize
	(
	Query *pquery
	)
{
	Assert(pquery);

	SOptContext octx;
	octx.m_pquery = pquery;
	octx.m_fSerializePlanDXL = true;
	Execute(&PvOptimizeTask, &octx);

	// clean up context
	octx.Free(octx.epinQuery, octx.epinPlanDXL);

	return octx.m_szPlanDXL;
}


//---------------------------------------------------------------------------
//	@function:
//		COptTasks::PplstmtOptimize
//
//	@doc:
//		optimizes a query to plannedstmt
//
//---------------------------------------------------------------------------
PlannedStmt *
COptTasks::PplstmtOptimize
	(
	Query *pquery,
	BOOL *pfUnexpectedFailure // output : set to true if optimizer unexpectedly failed to produce plan
	)
{
	Assert(pquery);

	SOptContext octx;
	octx.m_pquery = pquery;
	octx.m_fGeneratePlStmt= true;
	Execute(&PvOptimizeTask, &octx);

	if (NULL != octx.m_szErrorMsg)
	{
		elog(ERROR, octx.m_szErrorMsg);
	}
	*pfUnexpectedFailure = octx.m_fUnexpectedFailure;

	// clean up context
	octx.Free(octx.epinQuery, octx.epinPlStmt);

	return octx.m_pplstmt;
}


//---------------------------------------------------------------------------
//	@function:
//		COptTasks::SzDXL
//
//	@doc:
//		serializes query to DXL
//
//---------------------------------------------------------------------------
char *
COptTasks::SzDXL
	(
	Query *pquery
	)
{
	Assert(pquery);

	SOptContext octx;
	octx.m_pquery = pquery;
	Execute(&PvDXLFromQueryTask, &octx);

	// clean up context
	octx.Free(octx.epinQuery, octx.epinQueryDXL);

	return octx.m_szQueryDXL;
}


//---------------------------------------------------------------------------
//	@function:
//		COptTasks::SzDXL
//
//	@doc:
//		serializes planned stmt to DXL
//
//---------------------------------------------------------------------------
char *
COptTasks::SzDXL
	(
	PlannedStmt *pplstmt
	)
{
	Assert(pplstmt);

	SOptContext octx;
	octx.m_pplstmt = pplstmt;
	octx.m_fSerializePlanDXL = true;
	Execute(&PvDXLFromPlstmtTask, &octx);

	// clean up context
	octx.Free(octx.epinPlStmt, octx.epinPlanDXL);

	return octx.m_szPlanDXL;
}


//---------------------------------------------------------------------------
//	@function:
//		COptTasks::PqueryFromXML
//
//	@doc:
//		deserializes query from DXL
//
//---------------------------------------------------------------------------
Query *
COptTasks::PqueryFromXML
	(
	char *szDXL
	)
{
	Assert(NULL != szDXL);

	SOptContext octx;
	octx.m_szQueryDXL = szDXL;
	Execute(&PvQueryFromDXLTask, &octx);

	// clean up context
	octx.Free(octx.epinQueryDXL, octx.epinQuery);

	return (Query *) octx.m_pquery;
}


//---------------------------------------------------------------------------
//	@function:
//		COptTasks::PplstmtFromXML
//
//	@doc:
//		deserializes planned stmt from DXL
//
//---------------------------------------------------------------------------
PlannedStmt *
COptTasks::PplstmtFromXML
	(
	char *szDXL
	)
{
	Assert(NULL != szDXL);

	SOptContext octx;
	octx.m_szPlanDXL = szDXL;
	Execute(&PvPlstmtFromDXLTask, &octx);

	// clean up context
	octx.Free(octx.epinPlanDXL, octx.epinPlStmt);

	return octx.m_pplstmt;
}


//---------------------------------------------------------------------------
//	@function:
//		COptTasks::DumpMDObjs
//
//	@doc:
//		Dump relcache objects into DXL file
//
//---------------------------------------------------------------------------
void
COptTasks::DumpMDObjs
	(
	List *plistOids,
	const char *szFilename
	)
{
	SContextRelcacheToDXL ctxrelcache(plistOids, ULONG_MAX /*ulCmpt*/, szFilename);
	Execute(&PvDXLFromMDObjsTask, &ctxrelcache);
}


//---------------------------------------------------------------------------
//	@function:
//		COptTasks::SzMDObjs
//
//	@doc:
//		Dump relcache objects into DXL string
//
//---------------------------------------------------------------------------
char *
COptTasks::SzMDObjs
	(
	List *plistOids
	)
{
	SContextRelcacheToDXL ctxrelcache(plistOids, ULONG_MAX /*ulCmpt*/, NULL /*szFilename*/);
	Execute(&PvDXLFromMDObjsTask, &ctxrelcache);

	return ctxrelcache.m_szDXL;
}

//---------------------------------------------------------------------------
//	@function:
//		COptTasks::SzMDCast
//
//	@doc:
//		Dump cast object into DXL string
//
//---------------------------------------------------------------------------
char *
COptTasks::SzMDCast
	(
	List *plistOids
	)
{
	SContextRelcacheToDXL ctxrelcache(plistOids, ULONG_MAX /*ulCmpt*/, NULL /*szFilename*/);
	Execute(&PvMDCast, &ctxrelcache);

	return ctxrelcache.m_szDXL;
}

//---------------------------------------------------------------------------
//	@function:
//		COptTasks::SzMDScCmp
//
//	@doc:
//		Dump scalar comparison object into DXL string
//
//---------------------------------------------------------------------------
char *
COptTasks::SzMDScCmp
	(
	List *plistOids,
	char *szCmpType
	)
{
	SContextRelcacheToDXL ctxrelcache(plistOids, UlCmpt(szCmpType), NULL /*szFilename*/);
	Execute(&PvMDScCmp, &ctxrelcache);

	return ctxrelcache.m_szDXL;
}


//---------------------------------------------------------------------------
//	@function:
//		COptTasks::SzRelStats
//
//	@doc:
//		Dump statistics objects into DXL string
//
//---------------------------------------------------------------------------
char *
COptTasks::SzRelStats
	(
	List *plistOids
	)
{
	SContextRelcacheToDXL ctxrelcache(plistOids, ULONG_MAX /*ulCmpt*/, NULL /*szFilename*/);
	Execute(&PvDXLFromRelStatsTask, &ctxrelcache);

	return ctxrelcache.m_szDXL;
}

//---------------------------------------------------------------------------
//	@function:
//		COptTasks::FSetXform
//
//	@doc:
//		Enable/Disable a given xform
//
//---------------------------------------------------------------------------
bool
COptTasks::FSetXform
	(
	char *szXform,
	bool fDisable
	)
{
	CXform *pxform = CXformFactory::Pxff()->Pxf(szXform);
	if (NULL != pxform)
	{
		optimizer_xforms[pxform->Exfid()] = fDisable;

		return true;
	}

	return false;
}

//---------------------------------------------------------------------------
//	@function:
//		COptTasks::UlCmpt
//
//	@doc:
//		Find the comparison type code given its string representation
//
//---------------------------------------------------------------------------
ULONG
COptTasks::UlCmpt
	(
	char *szCmpType
	)
{
	const ULONG ulCmpTypes = 6;
	const CmpType rgcmpt[] = {CmptEq, CmptNEq, CmptLT, CmptGT, CmptLEq, CmptGEq};
	const CHAR *rgszCmpTypes[] = {"Eq", "NEq", "LT", "GT", "LEq", "GEq"};
	
	for (ULONG ul = 0; ul < ulCmpTypes; ul++)
	{		
		if (0 == strcasecmp(szCmpType, rgszCmpTypes[ul]))
		{
			return rgcmpt[ul];
		}
	}
	
	elog(ERROR, "Invalid comparison type code. Valid values are Eq, NEq, LT, LEq, GT, GEq");
	return CmptOther;
}


//---------------------------------------------------------------------------
//	@function:
//		COptTasks::SzEvalExprFromXML
//
//	@doc:
//		Converts XML string to DXL and evaluates the expression. Caller keeps
//		ownership of 'szXmlString' and takes ownership of the returned result.
//
//---------------------------------------------------------------------------
char *
COptTasks::SzEvalExprFromXML
	(
	char *szXmlString
	)
{
	GPOS_ASSERT(NULL != szXmlString);

	SEvalExprContext evalctxt;
	evalctxt.m_szDXL = szXmlString;
	evalctxt.m_szDXLResult = NULL;

	Execute(&PvEvalExprFromDXLTask, &evalctxt);
	return evalctxt.m_szDXLResult;
}


//---------------------------------------------------------------------------
//	@function:
//		COptTasks::SzOptimizeMinidumpFromFile
//
//	@doc:
//		Loads a minidump from the given file path, optimizes it and returns
//		the serialized representation of the result as DXL.
//
//---------------------------------------------------------------------------
char *
COptTasks::SzOptimizeMinidumpFromFile
	(
	char *szFileName
	)
{
	GPOS_ASSERT(NULL != szFileName);
	SOptimizeMinidumpContext optmdpctxt;
	optmdpctxt.m_szFileName = szFileName;
	optmdpctxt.m_szDXLResult = NULL;

	Execute(&PvOptimizeMinidumpTask, &optmdpctxt);
	return optmdpctxt.m_szDXLResult;
}

// EOF
