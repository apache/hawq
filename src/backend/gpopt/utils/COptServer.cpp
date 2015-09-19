//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		COptServer.cpp
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
//
//---------------------------------------------------------------------------

#define ALLOW_DatumGetPointer
#define ALLOW_ntohl
#define ALLOW_memset
#define ALLOW_printf

#include "postgres.h"
#include "gpopt/config/CConfigParamMapping.h"
#include "gpopt/translate/CStateDXLToQuery.h"
#include "gpopt/translate/CTranslatorUtils.h"
#include "gpopt/translate/CTranslatorQueryToDXL.h"
#include "gpopt/translate/CTranslatorDXLToPlStmt.h"
#include "gpopt/translate/CTranslatorDXLToQuery.h"
#include "gpopt/translate/CContextDXLToPlStmt.h"
#include "gpopt/translate/CTranslatorRelcacheToDXL.h"

#include "gpos/_api.h"
#include "gpos/base.h"
#include "gpos/utils.h"
#include "gpos/common/CAutoP.h"
#include "gpos/error/CErrorHandlerStandard.h"
#include "gpos/error/CAutoLogger.h"
#include "gpos/memory/CAutoMemoryPool.h"
#include "gpos/net/CAutoSocketProxy.h"
#include "gpos/net/CSocketListenerUDS.h"
#include "gpos/task/CAutoTaskProxy.h"

#include "base/CQueryToDXLResult.h"
#include "comm/CCommunicator.h"
#include "comm/CLoggerComm.h"

#include "gpopt/base/CAutoOptCtxt.h"
#include "gpopt/base/CQueryContext.h"
#include "gpopt/engine/CEngine.h"
#include "gpopt/mdcache/CAutoMDAccessor.h"
#include "gpopt/minidump/CMiniDumperDXL.h"
#include "gpopt/minidump/CMinidumperUtils.h"
#include "gpopt/minidump/CSerializableStackTrace.h"
#include "gpopt/minidump/CSerializableQuery.h"
#include "gpopt/minidump/CSerializablePlan.h"
#include "gpopt/minidump/CSerializableMDAccessor.h"
#include "gpopt/translate/CTranslatorDXLToExpr.h"
#include "gpopt/translate/CTranslatorExprToDXL.h"

#include "exception.h"
#include "gpopt/utils/COptServer.h"

#include "dxl/operators/CDXLNode.h"
#include "dxl/CDXLUtils.h"
#include "dxl/CIdGenerator.h"

#include "md/CMDProviderComm.h"
#include "md/CSystemId.h"

#include "gpopt/gpdbwrappers.h"

#include "CCostModelGPDB.h"

using namespace gpos;
using namespace gpmd;
using namespace gpnaucrates;
using namespace gpoptudfs;
using namespace gpdbcost;

#define GPOPT_FILE_NAME_LEN 		(1024)
#define GPOPT_CONN_HT_SIZE			(ULONG(128))
#define GPOPT_CONN_CHECK_SLEEP_MS	(ULONG(1000))

// initialization of static members
ULONG_PTR COptServer::SConnectionDescriptor::m_ulpInvalid = 0;
const CSystemId COptServer::m_sysidDefault(IMDId::EmdidGPDB, GPOS_WSZ_STR_LENGTH("GPDB"));

//---------------------------------------------------------------------------
//	@function:
//		COptServer::COptServer
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
COptServer::COptServer
	(
	const CHAR *szSocketPath
	)
	:
	m_szSocketPath(szSocketPath),
	m_pshtConnections(NULL)
{}


//---------------------------------------------------------------------------
//	@function:
//		COptServer::~COptServer
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
COptServer::~COptServer()
{}


//---------------------------------------------------------------------------
//	@function:
//		COptServer::PvRun
//
//	@doc:
//		Invoke optimizer instance
//
//---------------------------------------------------------------------------
void *
COptServer::PvRun
	(
	void *pv
	)
{
	COptServer optProc((const char *) pv);
	optProc.Loop();

	return NULL;
}


//---------------------------------------------------------------------------
//	@function:
//		COptServer::Loop
//
//	@doc:
//		Start serving requests
//
//---------------------------------------------------------------------------
void
COptServer::Loop()
{
	CAutoMemoryPool amp
		(
		CAutoMemoryPool::ElcNone,
		CMemoryPoolManager::EatMalloc,
		true /*fThreadSafe*/
		);

	m_pmp = amp.Pmp();

	gpos_set_threads(4, 4);

	// scope for socket listener
	{
		CSocketListenerUDS sl(m_pmp, m_szSocketPath);
		sl.StartListener();

		InitHT();

		// scope for tasks
		{
			CWorkerPoolManager *pwpm = CWorkerPoolManager::Pwpm();
			CAutoTaskProxy atp(m_pmp, pwpm, false /*fPropagateError*/);

			// start task that checks connection status
			CTask *ptskCheck = atp.PtskCreate(PvCheckConnections, m_pshtConnections);
			atp.Schedule(ptskCheck);

			// keep listening for new requests
			while (true)
			{
				CSocket *psocket = sl.PsocketNext();

				// create new task to handle communication
				CTask *ptsk = atp.PtskCreate(PvOptimize, psocket);
				ptsk->Tls().Reset(m_pmp);

				TrackConnection(ptsk, psocket);

				atp.Schedule(ptsk);

				// release completed tasks
				while (0 != atp.UlTasks() &&
					   GPOS_OK == atp.EresTimedWaitAny(&ptsk, 0 /*ulTimeoutMs*/))
				{
					ReleaseConnection(ptsk);
					atp.Destroy(ptsk);
				}

				GPOS_CHECK_ABORT;
			}
		}
	}
}


//---------------------------------------------------------------------------
//	@function:
//		COptServer::InitHT
//
//	@doc:
//		Initialize hashtable
//
//---------------------------------------------------------------------------
void
COptServer::InitHT()
{
	GPOS_ASSERT(NULL != m_pmp);
	GPOS_ASSERT(NULL == m_pshtConnections);

	m_pshtConnections = New(m_pmp) ConnectionHT();
	m_pshtConnections->Init
		(
		m_pmp,
		GPOPT_CONN_HT_SIZE,
		GPOS_OFFSET(SConnectionDescriptor, m_link),
		GPOS_OFFSET(SConnectionDescriptor, m_ulpId),
		&(SConnectionDescriptor::m_ulpInvalid),
		UlHashUlp,
		FEqualUlp
		);
}


//---------------------------------------------------------------------------
//	@function:
//		COptServer::TrackConnection
//
//	@doc:
//		Register connection for status checking
//
//---------------------------------------------------------------------------
void
COptServer::TrackConnection
	(
	CTask *ptsk,
	CSocket *psocket
	)
{
	GPOS_ASSERT(NULL != m_pmp);
	GPOS_ASSERT(NULL != psocket);
	GPOS_ASSERT(NULL != ptsk);

	SConnectionDescriptor *pcd = New(m_pmp) SConnectionDescriptor(ptsk, psocket);

	// scope for accessor
	{
		ConnectionKeyAccessor shtacc(*m_pshtConnections, (ULONG_PTR) pcd->m_ptsk);
		shtacc.Insert(pcd);
	}
}


//---------------------------------------------------------------------------
//	@function:
//		COptServer::ReleaseConnection
//
//	@doc:
//		Release connection
//
//---------------------------------------------------------------------------
void
COptServer::ReleaseConnection
	(
	CTask *ptsk
	)
{
	SConnectionDescriptor *pcd = NULL;

	// scope for accessor
	{
		ConnectionKeyAccessor shtacc(*m_pshtConnections, (ULONG_PTR) ptsk);
		pcd = shtacc.PtLookup();
		shtacc.Remove(pcd);
	}

	// release socket
	CAutoSocketProxy asp(pcd->m_psocket);
	delete pcd;
}


//---------------------------------------------------------------------------
//	@function:
//		COptServer::PvCheckConnections
//
//	@doc:
//		Connection check task
//
//---------------------------------------------------------------------------
void *
COptServer::PvCheckConnections
	(
	void *pv
	)
{
	ConnectionHT *pshtConnections = static_cast<ConnectionHT*>(pv);

	while (true)
	{
		GPOS_CHECK_ABORT;

		ConnectionIter conniter(*pshtConnections);
		while (conniter.FAdvance())
		{
			ConnectionIterAccessor shtacc(conniter);
			SConnectionDescriptor *pcd = shtacc.Pt();

			// cancel task if its connection is broken
			if (NULL != pcd && !pcd->m_psocket->FCheck())
			{
				pcd->m_ptsk->Cancel();
			}
		}

		clib::USleep(GPOPT_CONN_CHECK_SLEEP_MS);
	}

	return NULL;
}

//---------------------------------------------------------------------------
//	@function:
//		COptServer::PvOptimize
//
//	@doc:
//		Optimization task
//
//---------------------------------------------------------------------------
void *
COptServer::PvOptimize
	(
	void *pv
	)
{
	CSocket *psocket = static_cast<CSocket*>(pv);

	CAutoMemoryPool amp
		(
		CAutoMemoryPool::ElcNone,
		CMemoryPoolManager::EatMalloc,
		true /*fThreadSafe*/
		);

	IMemoryPool *pmp = amp.Pmp();
	CCommunicator comm(pmp, psocket);
	CCommunicator *pcomm = &comm;

	const ULONG ulSegments = gpdb::UlSegmentCountGP();
	// scope for setup objects
	{
		// setup loggers
		CAutoP<CLoggerComm> a_ploggerComm;
		a_ploggerComm = New(pmp) CLoggerComm(&comm);
		a_ploggerComm.Pt()->SetErrorInfoLevel(ILogger::EeilMsg);
		CAutoLogger alError(a_ploggerComm.Pt(), true /*fError*/);
		CAutoLogger alOut(a_ploggerComm.Pt(), false /*fError*/);

		// setup MD cache accessor
		CMDProviderComm *pmdp = New(pmp) CMDProviderComm(pmp, &comm);
		CAutoMDAccessor amda(pmp, pmdp, m_sysidDefault);

		CMiniDumperDXL mdmp(pmp);
		mdmp.Init();

		CErrorHandlerStandard errhdl;
		GPOS_TRY_HDL(&errhdl)
		{
			CSerializableStackTrace serStack;
			serStack.AllocateBuffer(pmp);
			CSerializableMDAccessor serMDA(amda.Pmda());

			// install opt context in TLS
			CAutoOptCtxt aoc
			  (
			   pmp,
			   amda.Pmda(),
			   NULL /*pceeval*/,
			   New(pmp) CCostModelGPDB(pmp, ulSegments)
			  );

			// build query context from requested query
			CQueryContext *pqc = PqcRecvQuery(pmp, pcomm, amda.Pmda());

			// optimize logical expression tree into physical expression tree.
			CEngine eng(pmp);
			eng.Init(pqc, NULL /*pdrgpss*/);
			eng.Optimize();

			// extract plan and send it back
			SendPlan(pmp, pcomm, amda.Pmda(), pqc, eng.PexprExtractPlan());

			// TODO: add traceflag to create minidump without error during optimization
		}
		GPOS_CATCH_EX(ex)
		{
			FinalizeMinidump(&mdmp);

			GPOS_RESET_EX;
		}
		GPOS_CATCH_END;
	}

	return NULL;
}


//---------------------------------------------------------------------------
//	@function:
//		COptServer::PqcRecvQuery
//
//	@doc:
//		Receive optimization request and construct query context for it
//
//---------------------------------------------------------------------------
CQueryContext *
COptServer::PqcRecvQuery
	(
	IMemoryPool *pmp,
	CCommunicator *pcomm,
	CMDAccessor *pmda
	)
{
	GPOS_ASSERT(NULL != pmp);
	GPOS_ASSERT(NULL != pcomm);
	GPOS_ASSERT(NULL != pmda);

	// receive query request
	CCommMessage *pmsg = pcomm->PmsgReceiveMsg();
	GPOS_ASSERT(CCommMessage::EcmtOptRequest == pmsg->Ecmt());
	const CHAR *szQuery = CDXLUtils::SzFromWsz(pmp, pmsg->Wsz());
	delete [] pmsg->Wsz();
	delete pmsg;

	CQueryToDXLResult *pqdxlr = CDXLUtils::PdxlnParseDXLQuery(pmp, szQuery, NULL /*szXSDPath*/);
	CSerializableQuery serQuery(pqdxlr->Pdxln(), pqdxlr->PdrgpdxlnOutputCols(), pqdxlr->PdrgpdxlnCTE());
	serQuery.Serialize(pmp);

	// translate DXL Tree -> Expr Tree
	CTranslatorDXLToExpr *pdxltr = New(pmp) CTranslatorDXLToExpr(pmp, pmda);
	CExpression *pexprTranslated =	pdxltr->PexprTranslateQuery(pqdxlr->Pdxln(), pqdxlr->PdrgpdxlnOutputCols(), pqdxlr->PdrgpdxlnCTE());
	gpdxl::DrgPul *pdrgul = pdxltr->PdrgpulOutputColRefs();
	gpmd::DrgPmdname *pdrgpmdname = pdxltr->Pdrgpmdname();

	CQueryContext *pqc = CQueryContext::PqcGenerate(pmp, pexprTranslated, pdrgul, pdrgpmdname, true /*fDeriveStats*/);

	if (GPOS_FTRACE(EopttracePrintQuery))
	{
		(void) pqc->Pexpr()->PdpDerive();

		CAutoTrace at(pmp);
		at.Os()
			<< "\nTranslated expression: \n" << *pexprTranslated
			<< "\nPreprocessed expression: \n" << *(pqc->Pexpr());
	}

	return pqc;
}

//---------------------------------------------------------------------------
//	@function:
//		COptServer::SendPlan
//
//	@doc:
//		Extract query plan, serialize it and send it to client
//
//---------------------------------------------------------------------------
void
COptServer::SendPlan
	(
	IMemoryPool *pmp,
	CCommunicator *pcomm,
	CMDAccessor *pmda,
	CQueryContext *pqc,
	CExpression *pexprPlan
	)
{
	GPOS_ASSERT(NULL != pmp);
	GPOS_ASSERT(NULL != pcomm);
	GPOS_ASSERT(NULL != pmda);
	GPOS_ASSERT(NULL != pqc);

	(void) pexprPlan->PrppCompute(pmp, pqc->Prpp());

	if (GPOS_FTRACE(EopttracePrintPlan))
	{
		CAutoTrace at(pmp);
		at.Os() << "\nPhysical plan: \n%ls" << *pexprPlan;
	}

	// translate plan into DXL
	DrgPi *pdrgpiSegments = New(pmp) DrgPi(pmp);

	// TODO: replace function with entry in request message
	const ULONG ulSegments = gpdb::UlSegmentCountGP();
	GPOS_ASSERT(0 < ulSegments);
	for (ULONG ul = 0; ul < ulSegments; ul++)
	{
		pdrgpiSegments->Append(New(pmp) INT(ul));
	}

	CTranslatorExprToDXL ptrexprtodxl(pmp, pmda, pdrgpiSegments);
	CDXLNode *pdxlnPlan = ptrexprtodxl.PdxlnTranslate(pexprPlan, pqc->PdrgPcr(), pqc->Pdrgpmdname());

	ULLONG ullPlanId = 0;
	ULLONG ullPlanSpaceSize = 0;
	CSerializablePlan serPlan(pdxlnPlan, ullPlanId, ullPlanSpaceSize);
	serPlan.Serialize(pmp);

	// serialize DXL to xml
	CWStringDynamic *pstrPlan = CDXLUtils::PstrSerializePlan
		(
		pmp,
		pdxlnPlan,
		ullPlanId,
		ullPlanSpaceSize,
		true /*fSerializeHeaderFooter*/,
		false /*fIndent*/
		);

	CCommMessage msg(CCommMessage::EcmtOptResponse, pstrPlan->Wsz(), 0 /*ullUserData*/);
	pcomm->SendMsg(&msg);
}


//---------------------------------------------------------------------------
//	@function:
//		COptServer::FinalizeMinidump
//
//	@doc:
//		Dump collected artifacts to file
//
//---------------------------------------------------------------------------
void
COptServer::FinalizeMinidump
	(
	CMiniDumperDXL *pmdmp
	)
{
	pmdmp->Finalize();

	// dump to a temp file
	//TODO: pass session and command ID from QD
	CHAR szFileName[GPOPT_FILE_NAME_LEN];
	CMinidumperUtils::GenerateMinidumpFileName
		(
		szFileName,
		GPOPT_FILE_NAME_LEN,
		0 /*gp_session_id*/,
		0 /*gp_command_count*/
		);

	// dump the same to given file
	CMinidumperUtils::Dump(szFileName, pmdmp);
}


// EOF
