//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		COptClient.cpp
//
//	@doc:
//		Implementation of communication handler for message exchange
//		between OPT and QD
//
//	@owner:
//		solimm1
//
//	@test:
//
//---------------------------------------------------------------------------

#define ALLOW_printf
#define ALLOW_sigsetjmp
#define ALLOW_DatumGetPointer
#define ALLOW_ntohl

#include "gpopt/utils/gpdbdefs.h"
#include "gpopt/config/CConfigParamMapping.h"
#include "gpopt/relcache/CMDProviderRelcache.h"
#include "gpopt/translate/CStateDXLToQuery.h"
#include "gpopt/translate/CTranslatorUtils.h"
#include "gpopt/translate/CTranslatorQueryToDXL.h"
#include "gpopt/translate/CTranslatorDXLToPlStmt.h"
#include "gpopt/translate/CTranslatorDXLToQuery.h"
#include "gpopt/translate/CContextDXLToPlStmt.h"
#include "gpopt/translate/CTranslatorRelcacheToDXL.h"

#include "cdb/cdbvars.h"
#include "utils/guc.h"

#include "gpos/_api.h"
#include "gpos/base.h"
#include "gpos/common/CBitSetIter.h"
#include "gpos/error/CErrorHandlerStandard.h"
#include "gpos/io/COstreamString.h"
#include "gpos/memory/CAutoMemoryPool.h"
#include "gpos/net/CSocketConnectorUDS.h"
#include "gpos/string/CWStringDynamic.h"

#include "gpopt/base/CAutoOptCtxt.h"
#include "gpopt/mdcache/CAutoMDAccessor.h"
#include "gpopt/xforms/CXform.h"

#include "exception.h"
#include "comm/CCommunicator.h"

#include "dxl/operators/CDXLNode.h"
#include "dxl/CDXLUtils.h"
#include "dxl/CIdGenerator.h"

#include "md/CMDProviderCommProxy.h"

#include "md/CSystemId.h"

#include "gpopt/utils/COptClient.h"
#include "gpopt/gpdbwrappers.h"

using namespace gpos;
using namespace gpnaucrates;
using namespace gpoptudfs;

// initialization of static members
const CSystemId COptClient::m_sysidDefault(IMDId::EmdidGPDB, GPOS_WSZ_STR_LENGTH("GPDB"));

ULONG COptClient::m_rgrgulSev[CException::ExsevSentinel][2] =
{
	{CException::ExsevInvalid, LOG},
	{CException::ExsevPanic, PANIC},
	{CException::ExsevFatal, FATAL},
	{CException::ExsevError, ERROR},
	{CException::ExsevWarning, WARNING},
	{CException::ExsevNotice, NOTICE},
	{CException::ExsevTrace, LOG},
};

//---------------------------------------------------------------------------
//	@function:
//		COptClient::PvRun
//
//	@doc:
//		Invoke optimizer instance
//
//---------------------------------------------------------------------------
void *
COptClient::PvRun
	(
	void *pv
	)
{
	for (ULONG ul = 0; ul < (ULONG) gp_opt_retries; ul++)
	{
		CErrorHandlerStandard errhdl;
		GPOS_TRY_HDL(&errhdl)
		{
			COptClient optProc((SOptParams *) pv);
			return optProc.PplstmtOptimize();
		}
		GPOS_CATCH_EX(ex)
		{
			if (!GPOS_MATCH_EX(ex, CException::ExmaSystem, CException::ExmiNetError) ||
			    ul + 1 == (ULONG) gp_opt_retries)
			{
				GPOS_RETHROW(ex);
			}

			GPOS_RESET_EX;
		}
		GPOS_CATCH_END;
	}

	return NULL;
}

//---------------------------------------------------------------------------
//	@function:
//		COptClient::PplstmtOptimize
//
//	@doc:
//		Request optimization from server
//
//---------------------------------------------------------------------------
PlannedStmt *
COptClient::PplstmtOptimize()
{
	ITask::PtskSelf()->PlogErr()->SetErrorInfoLevel(ILogger::EeilMsg);

	CAutoMemoryPool amp
		(
		CAutoMemoryPool::ElcNone,
		CMemoryPoolManager::EatMalloc,
		false /*fThreadSafe*/
		);
	m_pmp = amp.Pmp();

	// scope for socket connector
	{
		// setup communication with server
		CSocketConnectorUDS sc(m_pmp, m_szPath);
		sc.Connect();
		CCommunicator comm(m_pmp, sc.Psocket());
		m_pcomm = &comm;

		SetTraceflags();

		// scope for MD provider
		{
			IMDProvider *pmdp = New(m_pmp) CMDProviderRelcache(m_pmp);

			// scope for MD accessor
			{
				CAutoMDAccessor amda(m_pmp, pmdp, m_sysidDefault);

				// scope for optimization context
				{
					// install opt context in TLS
					CAutoOptCtxt aoc(m_pmp, amda.Pmda(), NULL /* CCostParams */, NULL /* COptimizerConfig */);

					// send request to client
					SendRequest(amda.Pmda());

					// retrieve serialized plan
					const CHAR *szPlanDXL = SzPlanDXL(pmdp);

					// create and return planned statement
					return PplstmtConstruct(amda.Pmda(), szPlanDXL);
				}
			}
		}
	}
}

//---------------------------------------------------------------------------
//	@function:
//		COptClient::SetTraceflags
//
//	@doc:
//		Set trace flags to current task
//
//---------------------------------------------------------------------------
void
COptClient::SetTraceflags()
{
	CBitSet *pbs = CConfigParamMapping::PbsPack(m_pmp, CXform::ExfSentinel);

	CWStringDynamic str(m_pmp);
	COstreamString oss(&str);

	oss << "Traceflags: " << *pbs;
	Elog(CException::ExsevNotice, str.Wsz());

	// scope for iterator
	{
		CBitSetIter bsi(*pbs);
		while (bsi.FAdvance())
		{
			GPOS_SET_TRACE(bsi.UlBit());
		}
	}

	pbs->Release();
}

//---------------------------------------------------------------------------
//	@function:
//		COptClient::SendRequest
//
//	@doc:
//		Send query optimization request to server
//
//---------------------------------------------------------------------------
void
COptClient::SendRequest
	(
	CMDAccessor *pmda
	)
{
	GPOS_ASSERT(NULL != m_pmp);
	GPOS_ASSERT(NULL != m_pcomm);

	// translate query to DXL
	CIdGenerator *pidgtorCol = New(m_pmp) CIdGenerator(GPDXL_COL_ID_START);
	CIdGenerator *pidgtorCTE = New(m_pmp) CIdGenerator(GPDXL_CTE_ID_START);
	CMappingVarColId *pmapvarcolid = New(m_pmp) CMappingVarColId(m_pmp);
	CTranslatorQueryToDXL *ptrquerytodxl = New(m_pmp) CTranslatorQueryToDXL
			(
			m_pmp,
			pmda,
			pidgtorCol,
			pidgtorCTE,
			pmapvarcolid,
			m_pquery,
			0
			);

	CDXLNode *pdxlnRoot = ptrquerytodxl->PdxlnFromQuery();
	DrgPdxln *pdrgpdxlnQueryOutput = ptrquerytodxl->PdrgpdxlnQueryOutput();
	DrgPdxln *pdrgpdxlnCTE = ptrquerytodxl->PdrgpdxlnCTE();
	GPOS_ASSERT(NULL != pdrgpdxlnQueryOutput);

	CWStringDynamic *pstrQuery = CDXLUtils::PstrSerializeQuery
		(
		m_pmp,
		pdxlnRoot,
		pdrgpdxlnQueryOutput,
		pdrgpdxlnCTE,
		true /*fDocumentHeaderFootter*/,
		false /*fIndent*/
		);

	// create optimization request message and send it
	CCommMessage msg(CCommMessage::EcmtOptRequest, pstrQuery->Wsz(), 0 /*ullUserData*/);
	m_pcomm->SendMsg(&msg);

	// clean up
	delete pstrQuery;
	pdxlnRoot->Release();
	pdrgpdxlnQueryOutput->Release();
}

//---------------------------------------------------------------------------
//	@function:
//		COptClient::SzPlanDXL
//
//	@doc:
//		Retrieve DXL plan
//
//---------------------------------------------------------------------------
const CHAR *
COptClient::SzPlanDXL
	(
	IMDProvider *pmdp
	)
{
	GPOS_ASSERT(NULL != m_pmp);
	GPOS_ASSERT(NULL != m_pcomm);

	const CHAR *szPlan = NULL;
	CMDProviderCommProxy mdpcp(m_pmp, pmdp);

	while (NULL == szPlan)
	{
		CCommMessage *pmsg = m_pcomm->PmsgReceiveMsg();
		switch (pmsg->Ecmt())
		{
			case CCommMessage::EcmtMDRequest:
				SendMDResponse(&mdpcp, pmsg->Wsz());
				break;

			case CCommMessage::EcmtLog:
				// log message
				Elog((ULONG) pmsg->UllInfo(), pmsg->Wsz());
				break;

			case CCommMessage::EcmtOptResponse:
				szPlan = CDXLUtils::SzFromWsz(m_pmp, pmsg->Wsz());
				break;

			default:
				GPOS_RAISE(gpdxl::ExmaComm, gpdxl::ExmiCommUnexpectedMessage, pmsg->Ecmt());
		}

		delete [] pmsg->Wsz();
		delete pmsg;
	}

	GPOS_ASSERT(NULL != szPlan);

	return szPlan;
}

//---------------------------------------------------------------------------
//	@function:
//		COptClient::SendMDResponse
//
//	@doc:
//		Send MD response
//
//---------------------------------------------------------------------------
void
COptClient::SendMDResponse
	(
	CMDProviderCommProxy *pmdpcp,
	const WCHAR *wszReq
	)
{
	GPOS_ASSERT(NULL != m_pmp);
	GPOS_ASSERT(NULL != m_pcomm);

	CWStringBase *pstrResponse = pmdpcp->PstrObject(wszReq);

	// send response
	CCommMessage msgResponse(CCommMessage::EcmtMDResponse, pstrResponse->Wsz(), 0 /*ullUserData*/);
	m_pcomm->SendMsg(&msgResponse);

	delete pstrResponse;
}

//---------------------------------------------------------------------------
//	@function:
//		COptClient::PplstmtConstruct
//
//	@doc:
//		Build planned statement from serialized plan
//
//---------------------------------------------------------------------------
PlannedStmt *
COptClient::PplstmtConstruct
	(
	CMDAccessor *pmda,
	const CHAR *szPlan
	)
{
	GPOS_ASSERT(NULL != m_pmp);

	CIdGenerator idgtorPlanId(1 /* ulStartId */);
	CIdGenerator idgtorMotionId(1 /* ulStartId */);
	CIdGenerator idgtorParamId(0 /* ulStartId */);

	List *plRTable = NULL;
	List *plSubplans = NULL;

	CContextDXLToPlStmt ctxdxltoplstmt
		(
		m_pmp,
		&idgtorPlanId,
		&idgtorMotionId,
		&idgtorParamId,
		&plRTable,
		&plSubplans
		);

	// translate DXL -> PlannedStmt
	CTranslatorDXLToPlStmt trdxltoplstmt(m_pmp, pmda, &ctxdxltoplstmt);
	ULLONG ullPlanId = 0;
	ULLONG ullPlanSpaceSize = 0;
	CDXLNode *pdxlnPlan = CDXLUtils::PdxlnParsePlan(m_pmp, szPlan, NULL /*szXSDPath*/, &ullPlanId, &ullPlanSpaceSize);
	PlannedStmt *pplstmt = trdxltoplstmt.PplstmtFromDXL(pdxlnPlan);

	GPOS_ASSERT(NULL != pplstmt);
	GPOS_ASSERT(CurrentMemoryContext);

	return (PlannedStmt *) gpdb::PvCopyObject(pplstmt);
}

//---------------------------------------------------------------------------
//	@function:
//		COptClient::Elog
//
//	@doc:
//		elog wrapper
//
//---------------------------------------------------------------------------
void
COptClient::Elog
	(
	ULONG ulSev,
	const WCHAR *wszMsg
	)
{
	GPOS_ASSERT(CException::ExsevSentinel > ulSev);
	GPOS_ASSERT(m_rgrgulSev[ulSev][0] == ulSev);

	PG_TRY();
	{
		elog(m_rgrgulSev[ulSev][1], "%ls", wszMsg);
	}
	PG_CATCH();
	{
		GPOS_RAISE(gpdxl::ExmaGPDB, gpdxl::ExmiGPDBError);
	}
	PG_END_TRY();
}

// EOF
