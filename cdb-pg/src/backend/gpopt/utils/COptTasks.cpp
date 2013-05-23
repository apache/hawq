//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 EMC Greenplum, Inc.
//
//	@filename:
//		COptTasks.cpp
//
//	@doc:
//		Routines to perform optimization related tasks using the gpos framework
//
//	@owner:
//		raghav
//
//	@test:
//
//
//---------------------------------------------------------------------------

#define ALLOW_strcasecmp

#include "gpopt/utils/gpdbdefs.h"
#include "gpopt/utils/COptTasks.h"
#include "gpopt/relcache/CMDProviderRelcache.h"
#include "gpopt/config/CConfigParamMapping.h"
#include "gpopt/translate/CStateDXLToQuery.h"
#include "gpopt/translate/CTranslatorUtils.h"
#include "gpopt/translate/CTranslatorQueryToDXL.h"
#include "gpopt/translate/CTranslatorPlStmtToDXL.h"
#include "gpopt/translate/CTranslatorDXLToPlStmt.h"
#include "gpopt/translate/CTranslatorDXLToQuery.h"
#include "gpopt/translate/CContextDXLToPlStmt.h"
#include "gpopt/translate/CTranslatorRelcacheToDXL.h"

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

#include "base/CQueryToDXLResult.h"

#include "md/IMDId.h"
#include "md/CMDRelationGPDB.h"
#include "md/CMDIdRelStats.h"
#include "md/CMDIdColStats.h"

#include "md/CSystemId.h"
#include "md/IMDRelStats.h"
#include "md/IMDColStats.h"
#include "md/CMDIdCast.h"
#include "md/CMDIdScCmp.h"

#include "dxl/operators/CDXLNode.h"
#include "dxl/parser/CParseHandlerDXL.h"
#include "dxl/CDXLUtils.h"
#include "dxl/CIdGenerator.h"
#include "exception.h"


#include "gpopt/gpdbwrappers.h"

using namespace gpos;
using namespace gpopt;
using namespace gpdxl;

// size of error buffer
#define GPOPT_ERROR_BUFFER_SIZE 10 * 1024 * 1024

// definition of default AutoMemoryPool
#define AUTO_MEM_POOL(amp) CAutoMemoryPool amp(CAutoMemoryPool::ElcExc, CMemoryPoolManager::EatMalloc, gp_opt_parallel)

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
	};

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
	CHAR *sz = (CHAR *) gpdb::GPDBAlloc(ulMaxLength);
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

	SContextRelcacheToDXL *pctxrelcache = (SContextRelcacheToDXL *) pv;
	GPOS_ASSERT(NULL != pctxrelcache);

	GPOS_ASSERT(2 == gpdb::UlListLength(pctxrelcache->plOids));
	Oid oidSrc = gpdb::OidListNth(pctxrelcache->plOids, 0);
	Oid oidDest = gpdb::OidListNth(pctxrelcache->plOids, 1);

	CAutoMemoryPool amp(CAutoMemoryPool::ElcExc);
	IMemoryPool *pmp = amp.Pmp();

	DrgPimdobj *pdrgpmdobj = New(pmp) DrgPimdobj(pmp);

	IMDId *pmdidCast = New(pmp) CMDIdCast(New(pmp) CMDIdGPDB(oidSrc), New(pmp) CMDIdGPDB(oidDest));

	// relcache MD provider
	CMDProviderRelcache *pmdpr = New(pmp) CMDProviderRelcache(pmp);
	
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

		pctxrelcache->szDXL = sz;
		
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

	SContextRelcacheToDXL *pctxrelcache = (SContextRelcacheToDXL *) pv;
	GPOS_ASSERT(NULL != pctxrelcache);
	GPOS_ASSERT(CmptOther > pctxrelcache->ulCmpt && "Incorrect comparison type specified");

	GPOS_ASSERT(2 == gpdb::UlListLength(pctxrelcache->plOids));
	Oid oidLeft = gpdb::OidListNth(pctxrelcache->plOids, 0);
	Oid oidRight = gpdb::OidListNth(pctxrelcache->plOids, 1);
	CmpType cmpt = (CmpType) pctxrelcache->ulCmpt;
	
	CAutoMemoryPool amp(CAutoMemoryPool::ElcExc);
	IMemoryPool *pmp = amp.Pmp();

	DrgPimdobj *pdrgpmdobj = New(pmp) DrgPimdobj(pmp);

	IMDId *pmdidScCmp = New(pmp) CMDIdScCmp(New(pmp) CMDIdGPDB(oidLeft), New(pmp) CMDIdGPDB(oidRight), CTranslatorRelcacheToDXL::Ecmpt(cmpt));

	// relcache MD provider
	CMDProviderRelcache *pmdpr = New(pmp) CMDProviderRelcache(pmp);
	
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

		pctxrelcache->szDXL = sz;
		
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

	if (gp_opt_parallel)
	{
		// be-aware that parallel optimizer mode may conflict with GPDB signal handlers,
		// this mode should be avoided unless optimizer is spawned in a different process
		if (gpos_set_threads(4, 4))
		{
			elog(ERROR, "unable to set number of threads in gpos");
			return;
		}
	}

	bool abort_flag = false;

	CAutoMemoryPool amp(CAutoMemoryPool::ElcNone, CMemoryPoolManager::EatMalloc, gp_opt_parallel);
	IMemoryPool *pmp = amp.Pmp();
	CHAR *err_buf = New(pmp) CHAR[GPOPT_ERROR_BUFFER_SIZE];

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

	delete []err_buf;
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
	void *ptr
	)
{
	GPOS_ASSERT(ptr);

	SOptContext *poctx = (SOptContext *) ptr;

	GPOS_ASSERT(poctx->pquery);
	GPOS_ASSERT(poctx->szQueryDXL == NULL);

	AUTO_MEM_POOL(amp);
	IMemoryPool *pmp = amp.Pmp();

	// ColId generator
	CIdGenerator *pidgtorCol = New(pmp) CIdGenerator(GPDXL_COL_ID_START);
	CIdGenerator *pidgtorCTE = New(pmp) CIdGenerator(GPDXL_CTE_ID_START);

	// map that stores gpdb att to optimizer col mapping
	CMappingVarColId *pmapvarcolid = New(pmp) CMappingVarColId(pmp);

	// relcache MD provider
	CMDProviderRelcache *pmdpr = New(pmp) CMDProviderRelcache(pmp);

	{
		CAutoMDAccessor amda(pmp, pmdpr, sysidDefault);

		CTranslatorQueryToDXL trquerytodxl(pmp, amda.Pmda(), pidgtorCol, pidgtorCTE, pmapvarcolid, (Query*)poctx->pquery, 0 /* ulQueryLevel */);
		CDXLNode *pdxlnQuery = trquerytodxl.PdxlnFromQuery();
		DrgPdxln *pdrgpdxlnQueryOutput = trquerytodxl.PdrgpdxlnQueryOutput();
		DrgPdxln *pdrgpdxlnCTE = trquerytodxl.PdrgpdxlnCTE();
		GPOS_ASSERT(NULL != pdrgpdxlnQueryOutput);

		delete pidgtorCol;
		delete pidgtorCTE;

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
		poctx->szQueryDXL = SzFromWsz(pstrDXL->Wsz());

		// clean up
		pdxlnQuery->Release();
		delete pstrDXL;
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
	CTranslatorDXLToPlStmt trdxltoplstmt(pmp, pmda, &ctxdxltoplstmt);
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
				elog(LOG, "\n[OPT]: Using search strategy in (%s)", szPath);

				pdrgpss = pphDXL->Pdrgpss();
				pdrgpss->AddRef();
			}
		}
	}
	GPOS_CATCH_EX(ex)
	{
		GPOS_RESET_EX;

		elog(LOG, "\n[OPT]: Using default search strategy");
	}
	GPOS_CATCH_END;

	delete pphDXL;

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
	IMemoryPool *pmp
	)
{
	// get chosen plan number, cost threshold
	ULLONG ullPlanId =  (ULLONG) gp_opt_plan_id;
	ULLONG ullSamples = (ULLONG) gp_opt_samples_number;
	DOUBLE dCostThreshold = (DOUBLE) gp_opt_cost_threshold;

	DOUBLE dDampingFactorFilter = (DOUBLE) gp_opt_damping_factor_filter;
	DOUBLE dDampingFactorJoin = (DOUBLE) gp_opt_damping_factor_join;
	DOUBLE dDampingFactorGroupBy = (DOUBLE) gp_opt_damping_factor_groupby;

	ULONG ulCTEInliningCutoff =  (ULONG) gp_opt_cte_inlining_bound;

	return New(pmp) COptimizerConfig
						(
						New(pmp) CEnumeratorConfig(pmp, ullPlanId, ullSamples, dCostThreshold),
						New(pmp) CStatisticsConfig(dDampingFactorFilter, dDampingFactorJoin, dDampingFactorGroupBy),
						New(pmp) CCTEConfig(ulCTEInliningCutoff)
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
	void *ptr
	)
{
	GPOS_ASSERT(ptr);
	SOptContext *poctx = (SOptContext *) ptr;

	GPOS_ASSERT(poctx->pquery);
	GPOS_ASSERT(poctx->szPlanDXL == NULL);
	GPOS_ASSERT(poctx->pplstmt == NULL);

	// initially assume no unexpected failure
	poctx->fUnexpectedFailure = false;

	AUTO_MEM_POOL(amp);
	IMemoryPool *pmp = amp.Pmp();

	// initialize metadata cache
	CMDCache::Init();

	// load search strategy
	DrgPss *pdrgpss = PdrgPssLoad(pmp, gp_opt_search_strategy_path);

	CBitSet *pbsTraceFlags = NULL;
	CDXLNode *pdxlnPlan = NULL;

	GPOS_TRY
	{
		// set trace flags
		pbsTraceFlags = CConfigParamMapping::PbsPack(pmp, CXform::ExfSentinel);

		// set up relcache MD provider
		CMDProviderRelcache *pmdpRelcache = New(pmp) CMDProviderRelcache(pmp);

		{
			// scope for MD accessor
			CMDAccessor mda(pmp, CMDCache::Pcache(), sysidDefault, pmdpRelcache);

			// ColId generator
			CIdGenerator idgtorColId(GPDXL_COL_ID_START);
			CIdGenerator idgtorCTE(GPDXL_CTE_ID_START);

			// map that stores gpdb att to optimizer col mapping
			CMappingVarColId *pmapvarcolid = New(pmp) CMappingVarColId(pmp);

			CTranslatorQueryToDXL trquerytodxl(pmp, &mda, &idgtorColId, &idgtorCTE, pmapvarcolid, (Query*) poctx->pquery, 0 /* ulQueryLevel */);
			COptimizerConfig *pocconf = PoconfCreate(pmp);

			// preload metadata if optimizer uses multiple threads
			if (gp_opt_parallel)
			{
				// install opt context in TLS
				CAutoOptCtxt aoc(pmp, &mda, NULL /* pec */, pocconf);
				CTranslatorUtils::PreloadMD(pmp, &mda, sysidDefault, (Query*) poctx->pquery);
			}

			CDXLNode *pdxlnQuery = trquerytodxl.PdxlnFromQuery();
			DrgPdxln *pdrgpdxlnQueryOutput = trquerytodxl.PdrgpdxlnQueryOutput();
			DrgPdxln *pdrgpdxlnCTE = trquerytodxl.PdrgpdxlnCTE();
			GPOS_ASSERT(NULL != pdrgpdxlnQueryOutput);

			ULONG ulSegments = gpdb::UlSegmentCountGP();

			pdxlnPlan = COptimizer::PdxlnOptimize
									(
									pmp,
									CMDCache::Pcache(),
									pbsTraceFlags,
									pdxlnQuery,
									pdrgpdxlnQueryOutput,
									pdrgpdxlnCTE,
									pmdpRelcache,
									sysidDefault,
									ulSegments,
									gp_session_id,
									gp_command_count,
									pdrgpss,
									pocconf
									);

			// serialize DXL to xml
			CWStringDynamic *pstrPlan = CDXLUtils::PstrSerializePlan(pmp, pdxlnPlan, pocconf->Pec()->UllPlanId(), pocconf->Pec()->UllPlanSpaceSize(), true /*fSerializeHeaderFooter*/, true /*fIndent*/);
			poctx->szPlanDXL = SzFromWsz(pstrPlan->Wsz());

			// translate DXL->PlStmt only when needed
			if (poctx->fGeneratePlStmt)
			{
				poctx->pplstmt = (PlannedStmt *) gpdb::PvCopyObject(Pplstmt(pmp, &mda, pdxlnPlan));
			}

			delete pstrPlan;
			pdxlnQuery->Release();
			pocconf->Release();
		}
	}
	GPOS_CATCH_EX(ex)
	{
		CMDCache::Shutdown();

		if (GPOS_MATCH_EX(ex, gpdxl::ExmaGPDB, gpdxl::ExmiGPDBError))
		{
			elog(INFO, "GPDB Exception. Please check log for more information.");
		}
		else if (FErrorOut(ex))
		{
			IErrorContext *perrctxt = CTask::PtskSelf()->Perrctxt();
			poctx->szErrorMsg = SzFromWsz(perrctxt->WszMsg());
		}
		else
		{
			poctx->fUnexpectedFailure = FUnexpectedFailure(ex);
		}
		GPOS_RETHROW(ex);
	}
	GPOS_CATCH_END;

	// cleanup
	pbsTraceFlags->Release();
	CRefCount::SafeRelease(pdxlnPlan);
	CMDCache::Shutdown();

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
	void *ptr
	)
{
	GPOS_ASSERT(ptr);

	SOptContext *poctx = (SOptContext *) ptr;

	GPOS_ASSERT(poctx->pplstmt);
	GPOS_ASSERT(poctx->szPlanDXL == NULL);

	AUTO_MEM_POOL(amp);
	IMemoryPool *pmp = amp.Pmp();

	CIdGenerator idgtor(1);

	// relcache MD provider
	CMDProviderRelcache *pmdpr = New(pmp) CMDProviderRelcache(pmp);

	{
		CAutoMDAccessor amda(pmp, pmdpr, sysidDefault);

		CMappingParamIdScalarId mapps(pmp);

		CTranslatorPlStmtToDXL tplstmtdxl(pmp, amda.Pmda(), &idgtor, (PlannedStmt*) poctx->pplstmt, &mapps);
		CDXLNode *pdxlnPlan = tplstmtdxl.PdxlnFromPlstmt();

		GPOS_ASSERT(NULL != pdxlnPlan);

		CWStringDynamic str(pmp);
		COstreamString oss(&str);

		// get chosen plan number
		ULLONG ullPlanId =  (ULLONG) gp_opt_plan_id;
		CWStringDynamic *pstrDXL = CDXLUtils::PstrSerializePlan(pmp, pdxlnPlan, ullPlanId, 0 /*ullPlanSpaceSize*/, true /*fSerializeHeaderFooter*/, true /*fIndent*/);

		poctx->szPlanDXL = SzFromWsz(pstrDXL->Wsz());

		// cleanup
		delete pstrDXL;
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
	void *ptr
	)
{
	GPOS_ASSERT(ptr);

	SOptContext *poctx = (SOptContext *) ptr;

	GPOS_ASSERT(poctx->pquery == NULL);
	GPOS_ASSERT(poctx->szPlanDXL);

	AUTO_MEM_POOL(amp);
	IMemoryPool *pmp = amp.Pmp();

	CWStringDynamic str(pmp);
	COstreamString oss(&str);

	ULLONG ullPlanId = 0;
	ULLONG ullPlanSpaceSize = 0;
	CDXLNode *pdxlnOriginal =
		CDXLUtils::PdxlnParsePlan(pmp, poctx->szPlanDXL, NULL /*XSD location*/, &ullPlanId, &ullPlanSpaceSize);

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
	CMDProviderRelcache *pmdpr = New(pmp) CMDProviderRelcache(pmp);

	{
		CAutoMDAccessor amda(pmp, pmdpr, sysidDefault);

		// translate DXL -> PlannedStmt
		CTranslatorDXLToPlStmt trdxltoplstmt(pmp, amda.Pmda(), &ctxdxlplstmt);
		PlannedStmt *pplstmt = trdxltoplstmt.PplstmtFromDXL(pdxlnOriginal);
		if (gp_opt_print_plan)
		{
			elog(NOTICE, "Plstmt: %s", gpdb::SzNodeToString(pplstmt));
		}

		GPOS_ASSERT(NULL != pplstmt);
		GPOS_ASSERT(CurrentMemoryContext);

		poctx->pplstmt = (PlannedStmt *) gpdb::PvCopyObject(pplstmt);
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
	void *ptr
	)
{
	GPOS_ASSERT(ptr);

	SOptContext *poctx = (SOptContext *) ptr;

	GPOS_ASSERT(poctx->pquery == NULL);
	GPOS_ASSERT(poctx->szQueryDXL);

	AUTO_MEM_POOL(amp);
	IMemoryPool *pmp = amp.Pmp();

	CWStringDynamic str(pmp);
	COstreamString oss(&str);

	// parse the DXL
	CQueryToDXLResult *ptroutput = CDXLUtils::PdxlnParseDXLQuery(pmp, poctx->szQueryDXL, NULL);

	GPOS_ASSERT(NULL != ptroutput->Pdxln());

	// relcache MD provider
	CMDProviderRelcache *pmdpr = New(pmp) CMDProviderRelcache(pmp);

	{
		CAutoMDAccessor amda(pmp, pmdpr, sysidDefault);

		// initialize hash table that maintains the mapping between ColId and Var
		TEMap *ptemap = New (pmp) TEMap(pmp);

		CTranslatorDXLToQuery trdxlquery(pmp, amda.Pmda());
		CStateDXLToQuery statedxltoquery(pmp);

		Query *pquery = trdxlquery.PqueryFromDXL(ptroutput->Pdxln(), ptroutput->PdrgpdxlnOutputCols(), &statedxltoquery, ptemap, GPDXL_QUERY_LEVEL);

		CRefCount::SafeRelease(ptemap);
		delete ptroutput;

		GPOS_ASSERT(NULL != pquery);
		GPOS_ASSERT(CurrentMemoryContext);

		poctx->pquery = pquery;
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

	SContextRelcacheToDXL *pctxrelcache = (SContextRelcacheToDXL *) pv;
	GPOS_ASSERT(NULL != pctxrelcache);

	AUTO_MEM_POOL(amp);
	IMemoryPool *pmp = amp.Pmp();

	DrgPimdobj *pdrgpmdobj = New(pmp) DrgPimdobj(pmp, 1024, 1024);

	// relcache MD provider
	CMDProviderRelcache *pmdp = New(pmp) CMDProviderRelcache(pmp);
	{
		CAutoMDAccessor amda(pmp, pmdp, sysidDefault);
		ListCell *plc = NULL;
		ForEach (plc, pctxrelcache->plOids)
		{
			Oid oid = lfirst_oid(plc);
			// get object from relcache
			CMDIdGPDB *pmdid = New(pmp) CMDIdGPDB(oid, 1 /* major */, 0 /* minor */);

			IMDCacheObject *pimdobj = CTranslatorRelcacheToDXL::Pimdobj(pmp, amda.Pmda(), pmdid);
			GPOS_ASSERT(NULL != pimdobj);

			pdrgpmdobj->Append(pimdobj);
			pmdid->Release();
		}

		if (pctxrelcache->szFilename)
		{
			COstreamFile cofs(pctxrelcache->szFilename);

			CDXLUtils::PstrSerializeMetadata(pmp, pdrgpmdobj, cofs, true /*fSerializeHeaderFooter*/, true /*fIndent*/);
		}
		else
		{
			CWStringDynamic str(pmp);
			COstreamString oss(&str);

			CDXLUtils::PstrSerializeMetadata(pmp, pdrgpmdobj, oss, true /*fSerializeHeaderFooter*/, true /*fIndent*/);

			CHAR *sz = SzFromWsz(str.Wsz());

			GPOS_ASSERT(NULL != sz);

			pctxrelcache->szDXL = sz;
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

	SContextRelcacheToDXL *pctxrelcache = (SContextRelcacheToDXL *) pv;
	GPOS_ASSERT(NULL != pctxrelcache);

	AUTO_MEM_POOL(amp);
	IMemoryPool *pmp = amp.Pmp();

	// relcache MD provider
	CMDProviderRelcache *pmdpr = New(pmp) CMDProviderRelcache(pmp);
	CAutoMDAccessor amda(pmp, pmdpr, sysidDefault);
	CAutoOptCtxt aoc(pmp, amda.Pmda(), NULL /* pec */, NULL /* poconf */);

	DrgPimdobj *pdrgpmdobj = New(pmp) DrgPimdobj(pmp);

	ListCell *plc = NULL;
	ForEach (plc, pctxrelcache->plOids)
	{
		Oid oidRelation = lfirst_oid(plc);

		// get object from relcache
		CMDIdGPDB *pmdid = New(pmp) CMDIdGPDB(oidRelation, 1 /* major */, 0 /* minor */);

		// generate mdid for relstats
		CMDIdRelStats *pmdidRelStats = New(pmp) CMDIdRelStats(pmdid);
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
				CMDIdColStats *pmdidColStats = New(pmp) CMDIdColStats(pmdid, ulPosCounter);
				ulPosCounter++;
				IMDColStats *pimdobjcolstats = const_cast<IMDColStats *>(amda.Pmda()->Pmdcolstats(pmdidColStats));
				pdrgpmdobj->Append(dynamic_cast<IMDCacheObject *>(pimdobjcolstats));
				pmdidColStats->Release();
			}
		}
		gpdb::CloseRelation(rel);
		pmdidRelStats->Release();
	}

	if (pctxrelcache->szFilename)
	{
		COstreamFile cofs(pctxrelcache->szFilename);

		CDXLUtils::PstrSerializeMetadata(pmp, pdrgpmdobj, cofs, true /*fSerializeHeaderFooter*/, true /*fIndent*/);
	}
	else
	{
		CWStringDynamic str(pmp);
		COstreamString oss(&str);

		CDXLUtils::PstrSerializeMetadata(pmp, pdrgpmdobj, oss, true /*fSerializeHeaderFooter*/, true /*fIndent*/);

		CHAR *sz = SzFromWsz(str.Wsz());

		GPOS_ASSERT(NULL != sz);

		pctxrelcache->szDXL = sz;
	}

	// cleanup
	pdrgpmdobj->Release();

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
	octx.pquery = pquery;
	octx.szPlanDXL = NULL;
	octx.pplstmt = NULL;
	octx.fGeneratePlStmt = false;
	Execute(&PvOptimizeTask, &octx);

	return octx.szPlanDXL;
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
	octx.pquery = pquery;
	octx.szPlanDXL = NULL;
	octx.pplstmt = NULL;
	octx.fGeneratePlStmt= true;
	octx.szErrorMsg = NULL;
	Execute(&PvOptimizeTask, &octx);

	if (NULL != octx.szErrorMsg)
	{
		elog(ERROR, octx.szErrorMsg);
	}

	*pfUnexpectedFailure = octx.fUnexpectedFailure;
	return octx.pplstmt;
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
	octx.pquery = pquery;
	octx.szQueryDXL = NULL;
	octx.fGeneratePlStmt = false;
	Execute(&PvDXLFromQueryTask, &octx);

	return octx.szQueryDXL;
}


//---------------------------------------------------------------------------
//	@function:
//		COptTasks::SzDXL
//
//	@doc:
//		serializes planned stmt to DXL
//
//---------------------------------------------------------------------------
char *COptTasks::SzDXL
	(
	PlannedStmt *pplstmt
	)
{
	Assert(pplstmt);

	SOptContext octx;
	octx.pplstmt = pplstmt;
	octx.szPlanDXL = NULL;
	octx.fGeneratePlStmt = false;
	Execute(&PvDXLFromPlstmtTask, &octx);

	return octx.szPlanDXL;
}


//---------------------------------------------------------------------------
//	@function:
//		COptTasks::PqueryFromXML
//
//	@doc:
//		deserializes query from DXL
//
//---------------------------------------------------------------------------
Query *COptTasks::PqueryFromXML(char *szDXL)
{
	Assert(szDXL);

	SOptContext octx;
	octx.pquery = NULL;
	octx.szQueryDXL = szDXL;
	octx.fGeneratePlStmt = false;
	Execute(&PvQueryFromDXLTask, &octx);

	return (Query *) octx.pquery;
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
	Assert(szDXL);

	SOptContext octx;
	octx.pquery = NULL;
	octx.pplstmt = NULL;
	octx.szPlanDXL = szDXL;
	octx.fGeneratePlStmt = false;

	Execute(&PvPlstmtFromDXLTask, &octx);

	return octx.pplstmt;
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
	List *plOids,
	const char *szFilename
	)
{
	SContextRelcacheToDXL ctxrelcache;
	ctxrelcache.plOids = plOids;
	ctxrelcache.szFilename = szFilename;
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
	SContextRelcacheToDXL ctxrelcache;
	ctxrelcache.plOids = plistOids;
	ctxrelcache.szFilename = NULL;
	Execute(&PvDXLFromMDObjsTask, &ctxrelcache);
	return ctxrelcache.szDXL;
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
	SContextRelcacheToDXL ctxrelcache;
	ctxrelcache.plOids = plistOids;
	ctxrelcache.szFilename = NULL;
	Execute(&PvMDCast, &ctxrelcache);
	return ctxrelcache.szDXL;
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
	SContextRelcacheToDXL ctxrelcache;
	ctxrelcache.plOids = plistOids;
	ctxrelcache.szFilename = NULL;
	ctxrelcache.ulCmpt = UlCmpt(szCmpType);
	Execute(&PvMDScCmp, &ctxrelcache);
	return ctxrelcache.szDXL;
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
	SContextRelcacheToDXL ctxrelcache;
	ctxrelcache.plOids = plistOids;
	ctxrelcache.szFilename = NULL;
	Execute(&PvDXLFromRelStatsTask, &ctxrelcache);
	return ctxrelcache.szDXL;
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
		gp_opt_xforms[pxform->Exfid()] = fDisable;

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


// EOF
