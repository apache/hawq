//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 Greenplum, Inc.
//
//	@filename:
//		CMappingColIdVarPlStmt.cpp
//
//	@doc:
//		Implentation of the functions that provide the mapping between Var, Param
//		and variables of Sub-query to CDXLNode during Query->DXL translation
//
//
//	@owner:
//		raghav
//
//	@test:
//
//
//---------------------------------------------------------------------------

#include "postgres.h"
#include "nodes/primnodes.h"

#include "gpopt/translate/CMappingColIdVarPlStmt.h"
#include "gpopt/translate/CDXLTranslateContextBaseTable.h"

#include "naucrates/exception.h"
#include "naucrates/md/CMDIdGPDB.h"
#include "naucrates/dxl/operators/CDXLScalarIdent.h"

#include "gpos/base.h"
#include "gpos/common/CAutoP.h"

#include "gpopt/gpdbwrappers.h"

using namespace gpdxl;
using namespace gpos;
using namespace gpmd;

//---------------------------------------------------------------------------
//	@function:
//		CMappingColIdVarPlStmt::CMappingColIdVarPlStmt
//
//	@doc:
//		Constructor
//
//---------------------------------------------------------------------------
CMappingColIdVarPlStmt::CMappingColIdVarPlStmt
	(
	IMemoryPool *pmp,
	const CDXLTranslateContextBaseTable *pdxltrctxbt,
	DrgPdxltrctx *pdrgpdxltrctx,
	CDXLTranslateContext *pdxltrctxOut,
	CContextDXLToPlStmt *pctxdxltoplstmt,
	Plan *pplan
	)
	:
	CMappingColIdVar(pmp),
	m_pdxltrctxbt(pdxltrctxbt),
	m_pdrgpdxltrctx(pdrgpdxltrctx),
	m_pdxltrctxOut(pdxltrctxOut),
	m_pctxdxltoplstmt(pctxdxltoplstmt)
{
	GPOS_ASSERT(NULL != pplan);

	m_pplan = pplan;
}

//---------------------------------------------------------------------------
//	@function:
//		CMappingColIdVarPlStmt::Pctxdxltoplstmt
//
//	@doc:
//		Returns the DXL->PlStmt translation context
//
//---------------------------------------------------------------------------
CContextDXLToPlStmt *
CMappingColIdVarPlStmt::Pctxdxltoplstmt()
{
	return m_pctxdxltoplstmt;
}

//---------------------------------------------------------------------------
//	@function:
//		CMappingColIdVarPlStmt::PpdxltrctxOut
//
//	@doc:
//		Returns the output translation context
//
//---------------------------------------------------------------------------
CDXLTranslateContext *
CMappingColIdVarPlStmt::PpdxltrctxOut()
{
	return m_pdxltrctxOut;
}


//---------------------------------------------------------------------------
//	@function:
//		CMappingColIdVarPlStmt::Pplan
//
//	@doc:
//		Returns the plan
//
//---------------------------------------------------------------------------
Plan *
CMappingColIdVarPlStmt::Pplan()
{
	return m_pplan;
}

//---------------------------------------------------------------------------
//	@function:
//		CMappingColIdVarPlStmt::PparamFromDXLNodeScId
//
//	@doc:
//		Translates a DXL scalar identifier operator into a GPDB Param node
//
//---------------------------------------------------------------------------
Param *
CMappingColIdVarPlStmt::PparamFromDXLNodeScId
	(
	const CDXLScalarIdent *pdxlop
	)
{
	GPOS_ASSERT(NULL != m_pdxltrctxOut);

	Param *pparam = NULL;

	const ULONG ulColId = pdxlop->Pdxlcr()->UlID();
	const CMappingElementColIdParamId *pmecolidparamid = m_pdxltrctxOut->Pmecolidparamid(ulColId);

	if (NULL != pmecolidparamid)
	{
		pparam = MakeNode(Param);
		pparam->paramkind = PARAM_EXEC;
		pparam->paramid = pmecolidparamid->UlParamId();
		pparam->paramtype = CMDIdGPDB::PmdidConvert(pmecolidparamid->PmdidType())->OidObjectId();
	}

	return pparam;
}

//---------------------------------------------------------------------------
//	@function:
//		CMappingColIdVarPlStmt::PvarFromDXLNodeScId
//
//	@doc:
//		Translates a DXL scalar identifier operator into a GPDB Var node
//
//---------------------------------------------------------------------------
Var *
CMappingColIdVarPlStmt::PvarFromDXLNodeScId
	(
	const CDXLScalarIdent *pdxlop
	)
{
	Index idxVarno = 0;
	AttrNumber attno = 0;

	Index idxVarnoold = 0;
	AttrNumber attnoOld = 0;

	const ULONG ulColId = pdxlop->Pdxlcr()->UlID();
	if (NULL != m_pdxltrctxbt)
	{
		// scalar id is used in a base table operator node
		idxVarno = m_pdxltrctxbt->IRel();
		attno = (AttrNumber) m_pdxltrctxbt->IAttnoForColId(ulColId);

		idxVarnoold = idxVarno;
		attnoOld = attno;
	}

	// if lookup has failed in the first step, attempt lookup again using outer and inner contexts
	if (0 == attno && NULL != m_pdrgpdxltrctx)
	{
		GPOS_ASSERT(0 != m_pdrgpdxltrctx->UlSafeLength());

		const CDXLTranslateContext *pdxltrctxLeft = (*m_pdrgpdxltrctx)[0];

		//	const CDXLTranslateContext *pdxltrctxRight

		// not a base table
		GPOS_ASSERT(NULL != pdxltrctxLeft);

		// lookup column in the left child translation context
		const TargetEntry *pte = pdxltrctxLeft->Pte(ulColId);

		if (pdxltrctxLeft->FParentAggNode())
		{
			// variable appears in an Agg node: varno must be 0 as expected by GPDB
			// TODO: antovl - Jan 26, 2011; clean this up once MPP-12034 is fixed
			GPOS_ASSERT(NULL != pte);
			idxVarno = 0;
		}
		else if (NULL != pte)
		{
			// identifier comes from left child
			idxVarno = OUTER;
		}
		else
		{
			const ULONG ulContexts = m_pdrgpdxltrctx->UlSafeLength();
			if (2 > ulContexts)
			{
				// there are no more children. col id not found in this tree
				// and must be an outer ref
				return NULL;
			}

			const CDXLTranslateContext *pdxltrctxRight = (*m_pdrgpdxltrctx)[1];

			// identifier must come from right child
			GPOS_ASSERT(NULL != pdxltrctxRight);

			pte = pdxltrctxRight->Pte(ulColId);

			idxVarno = INNER;

			// check any additional contexts if col is still not found yet
			for (ULONG ul = 2; NULL == pte && ul < ulContexts; ul++)
			{
				const CDXLTranslateContext *pdxltrctx = (*m_pdrgpdxltrctx)[ul];
				GPOS_ASSERT(NULL != pdxltrctx);

				pte = pdxltrctx->Pte(ulColId);
				if (NULL == pte)
				{
					continue;
				}

				Var *pv = (Var*) pte->expr;
				idxVarno = pv->varno;
			}
		}

		if (NULL  == pte)
		{
			GPOS_RAISE(gpdxl::ExmaDXL, gpdxl::ExmiDXL2PlStmtAttributeNotFound, ulColId);
		}

		attno = pte->resno;

		// find the original varno and attno for this column
		if (IsA(pte->expr, Var))
		{
			Var *pv = (Var*) pte->expr;
			idxVarnoold = pv->varnoold;
			attnoOld = pv->varoattno;
		}
		else
		{
			idxVarnoold = idxVarno;
			attnoOld = attno;
		}
	}

	Var *pvar = gpdb::PvarMakeVar
						(
						idxVarno,
						attno,
						CMDIdGPDB::PmdidConvert(pdxlop->PmdidType())->OidObjectId(),
						-1,	// vartypmod
						0	// varlevelsup
						);

	// set varnoold and varoattno since makeVar does not set them properly
	pvar->varnoold = idxVarnoold;
	pvar->varoattno = attnoOld;

	return pvar;
}

// EOF
