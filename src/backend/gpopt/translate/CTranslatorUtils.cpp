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
//		CTranslatorUtils.cpp
//
//	@doc:
//		Implementation of the utility methods for translating GPDB's
//		Query / PlannedStmt into DXL Tree
//
//	@test:
//
//
//---------------------------------------------------------------------------

#include "postgres.h"

#include "cdb/cdbvars.h"
#include "nodes/plannodes.h"
#include "nodes/parsenodes.h"
#include "access/sysattr.h"
#include "catalog/pg_type.h"
#include "catalog/pg_trigger.h"
#include "optimizer/walkers.h"
#include "utils/rel.h"

#define GPDB_NEXTVAL 1574
#define GPDB_CURRVAL 1575
#define GPDB_SETVAL 1576

#include "gpos/base.h"
#include "gpos/common/CAutoTimer.h"
#include "gpos/common/CBitSetIter.h"
#include "gpos/string/CWStringDynamic.h"
#include "gpopt/translate/CTranslatorUtils.h"
#include "gpopt/translate/CDXLTranslateContext.h"

#include "naucrates/dxl/CDXLUtils.h"
#include "naucrates/dxl/xml/dxltokens.h"
#include "naucrates/dxl/operators/CDXLDatumOid.h"
#include "naucrates/dxl/operators/CDXLDatumInt4.h"
#include "naucrates/dxl/operators/CDXLNode.h"
#include "naucrates/dxl/operators/CDXLSpoolInfo.h"
#include "naucrates/dxl/operators/CDXLColDescr.h"
#include "naucrates/dxl/gpdb_types.h"

#include "naucrates/md/CMDIdColStats.h"
#include "naucrates/md/CMDIdRelStats.h"
#include "naucrates/md/IMDAggregate.h"
#include "naucrates/md/IMDRelation.h"
#include "naucrates/md/IMDTrigger.h"
#include "naucrates/md/IMDIndex.h"
#include "naucrates/md/IMDTypeBool.h"
#include "naucrates/md/IMDTypeInt2.h"
#include "naucrates/md/IMDTypeInt4.h"
#include "naucrates/md/IMDTypeInt8.h"
#include "naucrates/md/IMDTypeOid.h"
#include "naucrates/md/CMDTypeGenericGPDB.h"

#include "naucrates/dxl/operators/CDXLDatumInt2.h"
#include "naucrates/dxl/operators/CDXLDatumInt4.h"
#include "naucrates/dxl/operators/CDXLDatumInt8.h"
#include "naucrates/dxl/operators/CDXLDatumBool.h"
#include "naucrates/dxl/operators/CDXLDatumOid.h"

#include "naucrates/traceflags/traceflags.h"

#include "gpopt/gpdbwrappers.h"
#include "gpopt/base/CUtils.h"
#include "gpopt/mdcache/CMDAccessor.h"

using namespace gpdxl;
using namespace gpmd;
using namespace gpos;
using namespace gpopt;

extern bool optimizer_enable_master_only_queries;
extern bool optimizer_multilevel_partitioning;

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorUtils::PreloadMD
//
//	@doc:
//		Preload the cache with MD information
//
//---------------------------------------------------------------------------
void
CTranslatorUtils::PreloadMD
	(
	IMemoryPool *pmp,
	CMDAccessor *pmda,
	CSystemId sysid,
	Query *pquery
	)
{
	CAutoTimer at("\n[OPT]: Metadata Preload Time", GPOS_FTRACE(EopttracePrintOptStats));

	// TODO: venky Jan 11th 2012; this is a temporary fix that preloads the
	// cache with MD information of base types

	CContextPreloadMD ctxpreloadmd(pmp, pmda);

	// find all relations accessed by query and preload their
	// relstats and colstats
	(void) FPreloadMDStatsWalker
				(
				(Node *) pquery,
				&ctxpreloadmd
				);

	// preload types bool, oid, int2, int4 and int8
	const IMDType *pmdtypeBool = pmda->PtMDType<IMDTypeBool>(sysid);
	PreloadMDType(pmda, pmdtypeBool);
	const IMDType *pmdtypeInt2 = pmda->PtMDType<IMDTypeInt2>(sysid);
	PreloadMDType(pmda, pmdtypeInt2);
	const IMDType *pmdtypeInt4 = pmda->PtMDType<IMDTypeInt4>(sysid);
	PreloadMDType(pmda, pmdtypeInt4);
	const IMDType *pmdtypeInt8 = pmda->PtMDType<IMDTypeInt8>(sysid);
	PreloadMDType(pmda, pmdtypeInt8);
	const IMDType *pmdtypeOid = pmda->PtMDType<IMDTypeOid>(sysid);
	PreloadMDType(pmda, pmdtypeOid);

	CMDIdGPDB *pmdidDate = GPOS_NEW(pmp) CMDIdGPDB(CMDIdGPDB::m_mdidDate.OidObjectId());
	(void) pmda->Pmdtype(pmdidDate);

	CMDIdGPDB *pmdidNumeric = GPOS_NEW(pmp) CMDIdGPDB(CMDIdGPDB::m_mdidNumeric.OidObjectId());
	(void) pmda->Pmdtype(pmdidNumeric);

	CMDIdGPDB *pmdidTimestamp = GPOS_NEW(pmp) CMDIdGPDB(CMDIdGPDB::m_mdidTimestamp.OidObjectId());
	(void) pmda->Pmdtype(pmdidTimestamp);

	pmdidDate->Release();
	pmdidNumeric->Release();
	pmdidTimestamp->Release();
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorUtils::PreloadMDType
//
//	@doc:
//		Preload the cache with MD information of a particular type
//
//---------------------------------------------------------------------------
void
CTranslatorUtils::PreloadMDType
	(
	CMDAccessor *pmda,
	const IMDType *pmdtype
	)
{
	(void) pmda->Pmdscop(pmdtype->PmdidCmp(IMDType::EcmptEq));
	(void) pmda->Pmdscop(pmdtype->PmdidCmp(IMDType::EcmptNEq));
	(void) pmda->Pmdscop(pmdtype->PmdidCmp(IMDType::EcmptL));
	(void) pmda->Pmdscop(pmdtype->PmdidCmp(IMDType::EcmptLEq));
	(void) pmda->Pmdscop(pmdtype->PmdidCmp(IMDType::EcmptG));
	(void) pmda->Pmdscop(pmdtype->PmdidCmp(IMDType::EcmptGEq));
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorUtils::FPreloadMDStatsWalker
//
//	@doc:
//		Walks the expression, descends into queries, finds relations
//		mentioned in the query and preloads column stats for them.
//
//---------------------------------------------------------------------------
BOOL
CTranslatorUtils::FPreloadMDStatsWalker
	(
	Node *pnode,
	CContextPreloadMD *pctxpreloadmd
	)
{
	if (NULL == pnode)
	{
		return false;
	}

	if (IsA(pnode, RangeTblEntry))
	{
		RangeTblEntry *prte = (RangeTblEntry *) pnode;

		// need to exclude views
		if (prte->rtekind == RTE_RELATION)
		{
			PreloadMDStats(pctxpreloadmd->m_pmp, pctxpreloadmd->m_pmda, prte->relid);
		}
		else if (prte->rtekind == RTE_SUBQUERY)
		{
			gpdb::FWalkQueryOrExpressionTree
								(
								(Node *) prte->subquery,
								(BOOL (*)()) CTranslatorUtils::FPreloadMDStatsWalker,
								pctxpreloadmd,
								QTW_EXAMINE_RTES // query tree walker flags
								);
		}
		return false;
	}

	if (IsA(pnode, Query))
	{
		return gpdb::FWalkQueryOrExpressionTree
					(
					pnode,
					(BOOL (*)()) CTranslatorUtils::FPreloadMDStatsWalker,
					pctxpreloadmd,
					QTW_EXAMINE_RTES // query tree walker flags
					);
	}

	return gpdb::FWalkExpressionTree
			(
			pnode,
			(BOOL (*)()) CTranslatorUtils::FPreloadMDStatsWalker,
			pctxpreloadmd
			);
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorUtils::PreloadMDStats
//
//	@doc:
//		Preloads the relstats and column stats for relation
//
//---------------------------------------------------------------------------
void
CTranslatorUtils::PreloadMDStats
	(
	IMemoryPool *pmp,
	CMDAccessor *pmda,
	OID oidRelation
	)
{

	CMDIdGPDB *pmdidgpdbRel = PmdidWithVersion(pmp, oidRelation);

	// preload column stats
	const IMDRelation *pmdrelation = pmda->Pmdrel(pmdidgpdbRel);

	for (ULONG ulAttNo = 0; ulAttNo < pmdrelation->UlColumns(); ulAttNo++)
	{
		pmdidgpdbRel->AddRef();
		CMDIdColStats *pmdidColStats = GPOS_NEW(pmp) CMDIdColStats(pmdidgpdbRel, ulAttNo);
		(void *) pmda->Pmdcolstats(pmdidColStats);
		pmdidColStats->Release();
	}

	// preload relation stats
	{
		pmdidgpdbRel->AddRef();
		CMDIdRelStats *pmdidRelStats = GPOS_NEW(pmp) CMDIdRelStats(pmdidgpdbRel);
		(void *) pmda->Pmdrelstats(pmdidRelStats);
		pmdidRelStats->Release();
	}

	pmdidgpdbRel->Release();
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorUtils::FBuiltinObject
//
//	@doc:
//		Checks if a given type oid corresponds to a built-in type
//
//---------------------------------------------------------------------------
BOOL
CTranslatorUtils::FBuiltinObject
	(
	OID oidTyp
	)
{
	// TODO: gcaragea - Feb 17, 2015: Refactor to use CBitSet for efficiency
	return (oidTyp == GPDB_INT2 ||
			oidTyp == GPDB_INT4 ||
			oidTyp == GPDB_INT8 ||
			oidTyp == GPDB_BOOL ||
			oidTyp == GPDB_TID ||
			oidTyp == GPDB_OID ||
			oidTyp == GPDB_XID ||
			oidTyp == GPDB_CID ||

			oidTyp == GPDB_NUMERIC ||
			oidTyp == GPDB_FLOAT4 ||
			oidTyp == GPDB_FLOAT8 ||
			oidTyp == GPDB_CASH ||

			oidTyp == GPDB_DATE ||
			oidTyp == GPDB_TIME ||
			oidTyp == GPDB_TIMETZ ||
			oidTyp == GPDB_TIMESTAMP ||
			oidTyp == GPDB_TIMESTAMPTZ ||
			oidTyp == GPDB_ABSTIME ||
			oidTyp == GPDB_RELTIME ||
			oidTyp == GPDB_INTERVAL ||
			oidTyp == GPDB_TIMEINTERVAL ||

			oidTyp == GPDB_CHAR ||
			oidTyp == GPDB_VARCHAR ||
			oidTyp == GPDB_TEXT ||

			oidTyp == GPDB_INET ||
			oidTyp == GPDB_CIDR ||
			oidTyp == GPDB_MACADDR ||
			oidTyp == GPDB_UNKNOWN
	);
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorUtils::PmdidWithVersion
//
//	@doc:
//		Requests the version from MD Versioning for the given OID.
//		Returns a new CMDIdGPDB object containing the version
//
//---------------------------------------------------------------------------
CMDIdGPDB *
CTranslatorUtils::PmdidWithVersion
	(
	IMemoryPool *pmp,
	OID oidObj
	)
{
	ULONG ullDDLv = 0;
	ULONG ullDMLv = 0;

	if (InvalidOid == oidObj)
	{
		/* Invalid oid objects get invalid version value 0.0 */
		ullDDLv = 0;
		ullDMLv = 0;
	}
	else
	{
		/*
		 * All valid objects get 1.0 as the version
		 */
		ullDDLv = 1;
		ullDMLv = 0;
	}

	return GPOS_NEW(pmp) CMDIdGPDB(oidObj, ullDDLv, ullDMLv);
}


//---------------------------------------------------------------------------
//	@function:
//		CTranslatorUtils::Pdxlid
//
//	@doc:
//		Create a DXL index descriptor from an index MD id
//
//---------------------------------------------------------------------------
CDXLIndexDescr *
CTranslatorUtils::Pdxlid
	(
	IMemoryPool *pmp,
	CMDAccessor *pmda,
	IMDId *pmdid
	)
{
	const IMDIndex *pmdindex = pmda->Pmdindex(pmdid);
	const CWStringConst *pstrIndexName = pmdindex->Mdname().Pstr();
	CMDName *pmdnameIdx = GPOS_NEW(pmp) CMDName(pmp, pstrIndexName);

	return GPOS_NEW(pmp) CDXLIndexDescr(pmp, pmdid, pmdnameIdx);
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorUtils::Pdxltabdesc
//
//	@doc:
//		Create a DXL table descriptor from a GPDB range table entry
//
//---------------------------------------------------------------------------
CDXLTableDescr *
CTranslatorUtils::Pdxltabdesc
	(
	IMemoryPool *pmp,
	CMDAccessor *pmda,
	CIdGenerator *pidgtor,
	const RangeTblEntry *prte,
	BOOL *pfDistributedTable // output
	)
{
	// generate an MDId for the table desc.
	OID oidRel = prte->relid;
	CMDIdGPDB *pmdid = PmdidWithVersion(pmp, oidRel);

	const IMDRelation *pmdrel = pmda->Pmdrel(pmdid);
	
	// look up table name
	const CWStringConst *pstrTblName = pmdrel->Mdname().Pstr();
	CMDName *pmdnameTbl = GPOS_NEW(pmp) CMDName(pmp, pstrTblName);

	CDXLTableDescr *pdxltabdesc = GPOS_NEW(pmp) CDXLTableDescr(pmp, pmdid, pmdnameTbl, prte->checkAsUser);

	const ULONG ulLen = pmdrel->UlColumns();
	
	IMDRelation::Ereldistrpolicy ereldist = pmdrel->Ereldistribution();
	if (NULL != pfDistributedTable &&
		(IMDRelation::EreldistrHash == ereldist || IMDRelation::EreldistrRandom == ereldist))
	{
		*pfDistributedTable = true;
	}
	else if (!optimizer_enable_master_only_queries && (IMDRelation::EreldistrMasterOnly == ereldist))
		{
			// fall back to the planner for queries on master-only table if they are disabled with Orca. This is due to
			// the fact that catalog tables (master-only) are not analyzed often and will result in Orca producing
			// inferior plans.

			GPOS_RAISE(gpdxl::ExmaDXL, gpdxl::ExmiQuery2DXLUnsupportedFeature, GPOS_WSZ_LIT("Queries on master-only tables"));
		}
	// add columns from md cache relation object to table descriptor
	for (ULONG ul = 0; ul < ulLen; ul++)
	{
		const IMDColumn *pmdcol = pmdrel->Pmdcol(ul);
		if (pmdcol->FDropped())
		{
			continue;
		}
		
		CMDName *pmdnameCol = GPOS_NEW(pmp) CMDName(pmp, pmdcol->Mdname().Pstr());
		CMDIdGPDB *pmdidColType = CMDIdGPDB::PmdidConvert(pmdcol->PmdidType());
		pmdidColType->AddRef();

		// create a column descriptor for the column
		CDXLColDescr *pdxlcd = GPOS_NEW(pmp) CDXLColDescr
											(
											pmp,
											pmdnameCol,
											pidgtor->UlNextId(),
											pmdcol->IAttno(),
											pmdidColType,
											false, /* fColDropped */
											pmdcol->UlLength()
											);
		pdxltabdesc->AddColumnDescr(pdxlcd);
	}

	return pdxltabdesc;
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorUtils::FSirvFunc
//
//	@doc:
//		Check if the given function is a SIRV (single row volatile) that reads
//		or modifies SQL data
//
//---------------------------------------------------------------------------
BOOL
CTranslatorUtils::FSirvFunc
	(
	IMemoryPool *pmp,
	CMDAccessor *pmda,
	OID oidFunc
	)
{
	// we exempt the following 3 functions to avoid falling back to the planner
	// for DML on tables with sequences. The same exemption is also in the planner
	if (GPDB_NEXTVAL == oidFunc ||
		GPDB_CURRVAL == oidFunc ||
		GPDB_SETVAL == oidFunc)
	{
		return false;
	}

	CMDIdGPDB *pmdidFunc = CTranslatorUtils::PmdidWithVersion(pmp, oidFunc);
	const IMDFunction *pmdfunc = pmda->Pmdfunc(pmdidFunc);

	BOOL fSirv = (!pmdfunc->FReturnsSet() &&
				  IMDFunction::EfsVolatile == pmdfunc->EfsStability());

	pmdidFunc->Release();

	return fSirv;
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorUtils::FHasSubquery
//
//	@doc:
//		Check if the given tree contains a subquery
//
//---------------------------------------------------------------------------
BOOL
CTranslatorUtils::FHasSubquery
	(
	Node *pnode
	)
{
	List *plUnsupported = ListMake1Int(T_SubLink);
	INT iUnsupported = gpdb::IFindNodes(pnode, plUnsupported);
	gpdb::GPDBFree(plUnsupported);

	return (0 <= iUnsupported);
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorUtils::Pdxltvf
//
//	@doc:
//		Create a DXL logical TVF from a GPDB range table entry
//
//---------------------------------------------------------------------------
CDXLLogicalTVF *
CTranslatorUtils::Pdxltvf
	(
	IMemoryPool *pmp,
	CMDAccessor *pmda,
	CIdGenerator *pidgtor,
	const RangeTblEntry *prte
	)
{
	GPOS_ASSERT(NULL != prte->funcexpr);
	FuncExpr *pfuncexpr = (FuncExpr *)prte->funcexpr;

	// get function id
	CMDIdGPDB *pmdidFunc = CTranslatorUtils::PmdidWithVersion(pmp, pfuncexpr->funcid);
	CMDIdGPDB *pmdidRetType =  CTranslatorUtils::PmdidWithVersion(pmp, pfuncexpr->funcresulttype);
	const IMDType *pmdType = pmda->Pmdtype(pmdidRetType);

	// In the planner, scalar functions that are volatile (SIRV) or read or modify SQL
	// data get patched into an InitPlan. This is not supported in the optimizer
	if (FSirvFunc(pmp, pmda, pfuncexpr->funcid))
	{
		GPOS_RAISE(gpdxl::ExmaDXL, gpdxl::ExmiQuery2DXLUnsupportedFeature, GPOS_WSZ_LIT("SIRV functions"));
	}

	// get function from MDcache
	const IMDFunction *pmdfunc = pmda->Pmdfunc(pmdidFunc);

	DrgPmdid *pdrgpmdidOutArgTypes = pmdfunc->PdrgpmdidOutputArgTypes();

	DrgPdxlcd *pdrgdxlcd = NULL;

	if (NULL != prte->funccoltypes)
	{
		// function returns record - use col names and types from query
		pdrgdxlcd = PdrgdxlcdRecord(pmp, pidgtor, prte->eref->colnames, prte->funccoltypes);
	}
	else if (pmdType->FComposite() && pmdType->PmdidBaseRelation()->FValid())
	{
		// function returns a "table" type or a user defined type
		pdrgdxlcd = PdrgdxlcdComposite(pmp, pmda, pidgtor, pmdType);
	}
	else if (NULL != pdrgpmdidOutArgTypes)
	{
		// function returns record - but output col types are defined in catalog
		pdrgpmdidOutArgTypes->AddRef();
		if (FContainsPolymorphicTypes(pdrgpmdidOutArgTypes))
		{
			// resolve polymorphic types (anyelement/anyarray) using the
			// argument types from the query
			List *plArgTypes = gpdb::PlFuncArgTypes(pfuncexpr->funcid);
			DrgPmdid *pdrgpmdidResolved = PdrgpmdidResolvePolymorphicTypes
												(
												pmp,
												pdrgpmdidOutArgTypes,
												plArgTypes,
												pfuncexpr->args
												);
			pdrgpmdidOutArgTypes->Release();
			pdrgpmdidOutArgTypes = pdrgpmdidResolved;
		}

		pdrgdxlcd = PdrgdxlcdRecord(pmp, pidgtor, prte->eref->colnames, pdrgpmdidOutArgTypes);
		pdrgpmdidOutArgTypes->Release();
	}
	else
	{
		// function returns base type
		CMDName mdnameFunc = pmdfunc->Mdname();
		pdrgdxlcd = PdrgdxlcdBase(pmp, pidgtor, pmdidRetType, &mdnameFunc);
	}

	CMDName *pmdfuncname = GPOS_NEW(pmp) CMDName(pmp, pmdfunc->Mdname().Pstr());

	CDXLLogicalTVF *pdxlopTVF = GPOS_NEW(pmp) CDXLLogicalTVF(pmp, pmdidFunc, pmdidRetType, pmdfuncname, pdrgdxlcd);

	return pdxlopTVF;
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorUtils::PdrgpmdidResolvePolymorphicTypes
//
//	@doc:
//		Resolve polymorphic types in the given array of type ids, replacing
//		them with the actual types obtained from the query
//
//---------------------------------------------------------------------------
DrgPmdid *
CTranslatorUtils::PdrgpmdidResolvePolymorphicTypes
	(
	IMemoryPool *pmp,
	DrgPmdid *pdrgpmdidTypes,
	List *plArgTypes,
	List *plArgsFromQuery
	)
{
	OID oidAnyElement = InvalidOid;
	OID oidAnyArray = InvalidOid;
	const ULONG ulArgTypes = gpdb::UlListLength(plArgTypes);
	const ULONG ulArgsFromQuery = gpdb::UlListLength(plArgsFromQuery);
	for (ULONG ul = 0; ul < ulArgTypes && ul < ulArgsFromQuery; ul++)
	{
		OID oidArgType = gpdb::OidListNth(plArgTypes, ul);
		OID oidArgTypeFromQuery = gpdb::OidExprType((Node *) gpdb::PvListNth(plArgsFromQuery, ul));

		if (ANYELEMENTOID == oidArgType && InvalidOid == oidAnyElement)
		{
			oidAnyElement = oidArgTypeFromQuery;
		}

		if (ANYARRAYOID == oidArgType && InvalidOid == oidAnyArray)
		{
			oidAnyArray = oidArgTypeFromQuery;
		}
	}

	GPOS_ASSERT(InvalidOid != oidAnyElement || InvalidOid != oidAnyArray);

	// use the resolved type to deduce the other if necessary
	if (InvalidOid == oidAnyElement)
	{
		oidAnyElement = gpdb::OidResolveGenericType(ANYELEMENTOID, oidAnyArray, ANYARRAYOID);
	}

	if (InvalidOid == oidAnyArray)
	{
		oidAnyArray = gpdb::OidResolveGenericType(ANYARRAYOID, oidAnyElement, ANYELEMENTOID);
	}

	// generate a new array of mdids based on the resolved types
	DrgPmdid *pdrgpmdidResolved = GPOS_NEW(pmp) DrgPmdid(pmp);

	const ULONG ulLen = pdrgpmdidTypes->UlLength();
	for (ULONG ul = 0; ul < ulLen; ul++)
	{
		IMDId *pmdid = (*pdrgpmdidTypes)[ul];
		IMDId *pmdidResolved = NULL;
		if (FAnyElement(pmdid))
		{
			pmdidResolved = CTranslatorUtils::PmdidWithVersion(pmp, oidAnyElement);
		}
		else if (FAnyArray(pmdid))
		{
			pmdidResolved = CTranslatorUtils::PmdidWithVersion(pmp, oidAnyArray);
		}
		else
		{
			pmdid->AddRef();
			pmdidResolved = pmdid;
		}

		pdrgpmdidResolved->Append(pmdidResolved);
	}

	return pdrgpmdidResolved;
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorUtils::FContainsPolymorphicTypes
//
//	@doc:
//		Check if the given mdid array contains any of the polymorphic
//		types (ANYELEMENT, ANYARRAY)
//
//---------------------------------------------------------------------------
BOOL
CTranslatorUtils::FContainsPolymorphicTypes
	(
	DrgPmdid *pdrgpmdidTypes
	)
{
	GPOS_ASSERT(NULL != pdrgpmdidTypes);
	const ULONG ulLen = pdrgpmdidTypes->UlLength();
	for (ULONG ul = 0; ul < ulLen; ul++)
	{
		IMDId *pmdid = (*pdrgpmdidTypes)[ul];
		if (FAnyElement(pmdid) || FAnyArray(pmdid))
		{
			return true;
		}
	}

	return false;
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorUtils::FAnyElement
//
//	@doc:
//		Check if the given type mdid is the "ANYELEMENT" type
//
//---------------------------------------------------------------------------
BOOL
CTranslatorUtils::FAnyElement
	(
	IMDId *pmdidType
	)
{
	Oid oid = CMDIdGPDB::PmdidConvert(pmdidType)->OidObjectId();
	return (ANYELEMENTOID == oid);
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorUtils::FAnyArray
//
//	@doc:
//		Check if the given type mdid is the "ANYARRAY" type
//
//---------------------------------------------------------------------------
BOOL
CTranslatorUtils::FAnyArray
	(
	IMDId *pmdidType
	)
{
	Oid oid = CMDIdGPDB::PmdidConvert(pmdidType)->OidObjectId();
	return (ANYARRAYOID == oid);
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorUtils::PdrgdxlcdRecord
//
//	@doc:
//		Get column descriptors from a record type
//
//---------------------------------------------------------------------------
DrgPdxlcd *
CTranslatorUtils::PdrgdxlcdRecord
	(
	IMemoryPool *pmp,
	CIdGenerator *pidgtor,
	List *plColNames,
	List *plColTypes
	)
{
	ListCell *plcColName = NULL;
	ListCell *plcColType = NULL;

	ULONG ul = 0;
	DrgPdxlcd *pdrgdxlcd = GPOS_NEW(pmp) DrgPdxlcd(pmp);

	ForBoth (plcColName, plColNames,
			plcColType, plColTypes)
	{
		Value *pvalue = (Value *) lfirst(plcColName);
		Oid coltype = lfirst_oid(plcColType);

		CHAR *szColName = strVal(pvalue);
		CWStringDynamic *pstrColName = CDXLUtils::PstrFromSz(pmp, szColName);
		CMDName *pmdColName = GPOS_NEW(pmp) CMDName(pmp, pstrColName);
		GPOS_DELETE(pstrColName);

		IMDId *pmdidColType = CTranslatorUtils::PmdidWithVersion(pmp, coltype);

		CDXLColDescr *pdxlcd = GPOS_NEW(pmp) CDXLColDescr
										(
										pmp,
										pmdColName,
										pidgtor->UlNextId(),
										INT(ul + 1) /* iAttno */,
										pmdidColType,
										false /* fColDropped */
										);
		pdrgdxlcd->Append(pdxlcd);
		ul++;
	}

	return pdrgdxlcd;
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorUtils::PdrgdxlcdRecord
//
//	@doc:
//		Get column descriptors from a record type
//
//---------------------------------------------------------------------------
DrgPdxlcd *
CTranslatorUtils::PdrgdxlcdRecord
	(
	IMemoryPool *pmp,
	CIdGenerator *pidgtor,
	List *plColNames,
	DrgPmdid *pdrgpmdidOutArgTypes
	)
{
	GPOS_ASSERT(pdrgpmdidOutArgTypes->UlLength() == (ULONG) gpdb::UlListLength(plColNames));
	ListCell *plcColName = NULL;

	ULONG ul = 0;
	DrgPdxlcd *pdrgdxlcd = GPOS_NEW(pmp) DrgPdxlcd(pmp);

	ForEach (plcColName, plColNames)
	{
		Value *pvalue = (Value *) lfirst(plcColName);

		CHAR *szColName = strVal(pvalue);
		CWStringDynamic *pstrColName = CDXLUtils::PstrFromSz(pmp, szColName);
		CMDName *pmdColName = GPOS_NEW(pmp) CMDName(pmp, pstrColName);
		GPOS_DELETE(pstrColName);

		IMDId *pmdidColType = (*pdrgpmdidOutArgTypes)[ul];
		pmdidColType->AddRef();

		CDXLColDescr *pdxlcd = GPOS_NEW(pmp) CDXLColDescr
										(
										pmp,
										pmdColName,
										pidgtor->UlNextId(),
										INT(ul + 1) /* iAttno */,
										pmdidColType,
										false /* fColDropped */
										);
		pdrgdxlcd->Append(pdxlcd);
		ul++;
	}

	return pdrgdxlcd;
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorUtils::PdrgdxlcdBase
//
//	@doc:
//		Get column descriptor from a base type
//
//---------------------------------------------------------------------------
DrgPdxlcd *
CTranslatorUtils::PdrgdxlcdBase
	(
	IMemoryPool *pmp,
	CIdGenerator *pidgtor,
	IMDId *pmdidRetType,
	CMDName *pmdName
	)
{
	DrgPdxlcd *pdrgdxlcd = GPOS_NEW(pmp) DrgPdxlcd(pmp);

	pmdidRetType->AddRef();
	CMDName *pmdColName = GPOS_NEW(pmp) CMDName(pmp, pmdName->Pstr());

	CDXLColDescr *pdxlcd = GPOS_NEW(pmp) CDXLColDescr
									(
									pmp,
									pmdColName,
									pidgtor->UlNextId(),
									INT(1) /* iAttno */,
									pmdidRetType,
									false /* fColDropped */
									);

	pdrgdxlcd->Append(pdxlcd);

	return pdrgdxlcd;
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorUtils::PdrgdxlcdComposite
//
//	@doc:
//		Get column descriptors from a composite type
//
//---------------------------------------------------------------------------
DrgPdxlcd *
CTranslatorUtils::PdrgdxlcdComposite
	(
	IMemoryPool *pmp,
	CMDAccessor *pmda,
	CIdGenerator *pidgtor,
	const IMDType *pmdType
	)
{
	DrgPmdcol *pdrgPmdCol = ExpandCompositeType(pmp, pmda, pmdType);

	DrgPdxlcd *pdrgdxlcd = GPOS_NEW(pmp) DrgPdxlcd(pmp);

	for (ULONG ul = 0; ul < pdrgPmdCol->UlLength(); ul++)
	{
		CMDColumn *pmdcol = (*pdrgPmdCol)[ul];

		CMDName *pmdColName = GPOS_NEW(pmp) CMDName(pmp, pmdcol->Mdname().Pstr());
		IMDId *pmdidColType = pmdcol->PmdidType();

		pmdidColType->AddRef();
		CDXLColDescr *pdxlcd = GPOS_NEW(pmp) CDXLColDescr
										(
										pmp,
										pmdColName,
										pidgtor->UlNextId(),
										INT(ul + 1) /* iAttno */,
										pmdidColType,
										false /* fColDropped */
										);
		pdrgdxlcd->Append(pdxlcd);
	}

	pdrgPmdCol->Release();

	return pdrgdxlcd;
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorUtils::ExpandCompositeType
//
//	@doc:
//		Expand a composite type into an array of IMDColumns
//
//---------------------------------------------------------------------------
DrgPmdcol *
CTranslatorUtils::ExpandCompositeType
	(
	IMemoryPool *pmp,
	CMDAccessor *pmda,
	const IMDType *pmdType
	)
{
	GPOS_ASSERT(NULL != pmdType);
	GPOS_ASSERT(pmdType->FComposite());

	IMDId *pmdidRel = pmdType->PmdidBaseRelation();
	const IMDRelation *pmdrel = pmda->Pmdrel(pmdidRel);
	GPOS_ASSERT(NULL != pmdrel);

	DrgPmdcol *pdrgPmdcol = GPOS_NEW(pmp) DrgPmdcol(pmp);

	for(ULONG ul = 0; ul < pmdrel->UlColumns(); ul++)
	{
		CMDColumn *pmdcol = (CMDColumn *) pmdrel->Pmdcol(ul);

		if (!pmdcol->FSystemColumn())
		{
			pmdcol->AddRef();
			pdrgPmdcol->Append(pmdcol);
		}
	}

	return pdrgPmdcol;
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorUtils::EdxljtFromJoinType
//
//	@doc:
//		Translates the join type from its GPDB representation into the DXL one
//
//---------------------------------------------------------------------------
EdxlJoinType
CTranslatorUtils::EdxljtFromJoinType
	(
	JoinType jt
	)
{
	EdxlJoinType edxljt = EdxljtSentinel;

	switch (jt)
	{
		case JOIN_INNER:
			edxljt = EdxljtInner;
			break;

		case JOIN_LEFT:
			edxljt = EdxljtLeft;
			break;

		case JOIN_FULL:
			edxljt = EdxljtFull;
			break;

		case JOIN_RIGHT:
			edxljt = EdxljtRight;
			break;

		case JOIN_IN:
			edxljt = EdxljtIn;
			break;

		case JOIN_LASJ:
			edxljt = EdxljtLeftAntiSemijoin;
			break;

		case JOIN_LASJ_NOTIN:
			edxljt = EdxljtLeftAntiSemijoinNotIn;
			break;

		default:
			GPOS_ASSERT(!"Unrecognized join type");
	}

	GPOS_ASSERT(EdxljtSentinel > edxljt);

	return edxljt;
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorUtils::EdxlIndexDirection
//
//	@doc:
//		Translates the DXL index scan direction from GPDB representation
//
//---------------------------------------------------------------------------
EdxlIndexScanDirection
CTranslatorUtils::EdxlIndexDirection
	(
	ScanDirection sd
	)
{
	EdxlIndexScanDirection edxlisd = EdxlisdSentinel;

	switch (sd)
	{
		case BackwardScanDirection:
			edxlisd = EdxlisdBackward;
			break;

		case ForwardScanDirection:
			edxlisd = EdxlisdForward;
			break;

		case NoMovementScanDirection:
			edxlisd = EdxlisdNoMovement;
			break;

		default:
			GPOS_ASSERT(!"Unrecognized index scan direction");
	}

	GPOS_ASSERT(EdxlisdSentinel > edxlisd);

	return edxlisd;
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorUtils::Pdxlcd
//
//	@doc:
//		Find the n-th col descr entry
//
//---------------------------------------------------------------------------
const CDXLColDescr *
CTranslatorUtils::Pdxlcd
	(
	const CDXLTableDescr *pdxltabdesc,
	ULONG ulPos
	)
{
	GPOS_ASSERT(0 != ulPos);
	GPOS_ASSERT(ulPos < pdxltabdesc->UlArity());

	return pdxltabdesc->Pdxlcd(ulPos);
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorUtils::PstrSystemColName
//
//	@doc:
//		Return the name for the system attribute with the given attribute number.
//
//---------------------------------------------------------------------------
const CWStringConst *
CTranslatorUtils::PstrSystemColName
	(
	AttrNumber attno
	)
{
	GPOS_ASSERT(FirstLowInvalidHeapAttributeNumber < attno && 0 > attno);

	switch (attno)
	{
		case SelfItemPointerAttributeNumber:
			return CDXLTokens::PstrToken(EdxltokenCtidColName);

		case ObjectIdAttributeNumber:
			return CDXLTokens::PstrToken(EdxltokenOidColName);

		case MinTransactionIdAttributeNumber:
			return CDXLTokens::PstrToken(EdxltokenXminColName);

		case MinCommandIdAttributeNumber:
			return CDXLTokens::PstrToken(EdxltokenCminColName);

		case MaxTransactionIdAttributeNumber:
			return CDXLTokens::PstrToken(EdxltokenXmaxColName);

		case MaxCommandIdAttributeNumber:
			return CDXLTokens::PstrToken(EdxltokenCmaxColName);

		case TableOidAttributeNumber:
			return CDXLTokens::PstrToken(EdxltokenTableOidColName);

		case GpSegmentIdAttributeNumber:
			return CDXLTokens::PstrToken(EdxltokenGpSegmentIdColName);

		default:
			GPOS_RAISE
				(
				gpdxl::ExmaDXL,
				gpdxl::ExmiPlStmt2DXLConversion,
				GPOS_WSZ_LIT("Invalid attribute number")
				);
			return NULL;
	}
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorUtils::PmdidSystemColType
//
//	@doc:
//		Return the type id for the system attribute with the given attribute number.
//
//---------------------------------------------------------------------------
CMDIdGPDB *
CTranslatorUtils::PmdidSystemColType
	(
	IMemoryPool *pmp,
	AttrNumber attno
	)
{
	GPOS_ASSERT(FirstLowInvalidHeapAttributeNumber < attno && 0 > attno);

	switch (attno)
	{
		case SelfItemPointerAttributeNumber:
			// tid type
			return CTranslatorUtils::PmdidWithVersion(pmp, GPDB_TID);

		case ObjectIdAttributeNumber:
		case TableOidAttributeNumber:
			// OID type
			return CTranslatorUtils::PmdidWithVersion(pmp, GPDB_OID);

		case MinTransactionIdAttributeNumber:
		case MaxTransactionIdAttributeNumber:
			// xid type
			return CTranslatorUtils::PmdidWithVersion(pmp, GPDB_XID);

		case MinCommandIdAttributeNumber:
		case MaxCommandIdAttributeNumber:
			// cid type
			return CTranslatorUtils::PmdidWithVersion(pmp, GPDB_CID);

		case GpSegmentIdAttributeNumber:
			// int4
			return CTranslatorUtils::PmdidWithVersion(pmp, GPDB_INT4);

		default:
			GPOS_RAISE
				(
				gpdxl::ExmaDXL,
				gpdxl::ExmiPlStmt2DXLConversion,
				GPOS_WSZ_LIT("Invalid attribute number")
				);
			return NULL;
	}
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorUtils::Scandirection
//
//	@doc:
//		Return the GPDB specific scan direction from its corresponding DXL
//		representation
//
//---------------------------------------------------------------------------
ScanDirection
CTranslatorUtils::Scandirection
	(
	EdxlIndexScanDirection edxlisd
	)
{
	if (EdxlisdBackward == edxlisd)
	{
		return BackwardScanDirection;
	}

	if (EdxlisdForward == edxlisd)
	{
		return ForwardScanDirection;
	}

	return NoMovementScanDirection;
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorUtils::OidCmpOperator
//
//	@doc:
//		Extract comparison operator from an OpExpr, ScalarArrayOpExpr or RowCompareExpr
//
//---------------------------------------------------------------------------
OID
CTranslatorUtils::OidCmpOperator
	(
	Expr* pexpr
	)
{
	GPOS_ASSERT(IsA(pexpr, OpExpr) || IsA(pexpr, ScalarArrayOpExpr) || IsA(pexpr, RowCompareExpr));

	switch (pexpr->type)
	{
		case T_OpExpr:
			return ((OpExpr *) pexpr)->opno;
			
		case T_ScalarArrayOpExpr:
			return ((ScalarArrayOpExpr*) pexpr)->opno;

		case T_RowCompareExpr:
			return LInitialOID(((RowCompareExpr *) pexpr)->opnos);
			
		default:
			GPOS_RAISE
				(
				gpdxl::ExmaDXL,
				gpdxl::ExmiPlStmt2DXLConversion,
				GPOS_WSZ_LIT("Unsupported comparison")
				);
			return InvalidOid;
	}
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorUtils::OidCmpOperator
//
//	@doc:
//		Extract comparison operator from an OpExpr, ScalarArrayOpExpr or RowCompareExpr
//
//---------------------------------------------------------------------------
OID
CTranslatorUtils::OidIndexQualOpclass
	(
	INT iAttno,
	OID oidIndex
	)
{
	Relation relIndex = gpdb::RelGetRelation(oidIndex);
	GPOS_ASSERT(NULL != relIndex);
	GPOS_ASSERT(iAttno <= relIndex->rd_index->indnatts);
	
	OID oidOpclass = relIndex->rd_indclass->values[iAttno - 1];
	gpdb::CloseRelation(relIndex);
	
	return oidOpclass;
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorUtils::Edxlsetop
//
//	@doc:
//		Return the DXL representation of the set operation
//
//---------------------------------------------------------------------------
EdxlSetOpType
CTranslatorUtils::Edxlsetop
	(
	SetOperation setop,
	BOOL fAll
	)
{
	if (SETOP_UNION == setop && fAll)
	{
		return EdxlsetopUnionAll;
	}

	if (SETOP_INTERSECT == setop && fAll)
	{
		return EdxlsetopIntersectAll;
	}

	if (SETOP_EXCEPT == setop && fAll)
	{
		return EdxlsetopDifferenceAll;
	}

	if (SETOP_UNION == setop)
	{
		return EdxlsetopUnion;
	}

	if (SETOP_INTERSECT == setop)
	{
		return EdxlsetopIntersect;
	}

	if (SETOP_EXCEPT == setop)
	{
		return EdxlsetopDifference;
	}

	GPOS_ASSERT(!"Unrecognized set operator type");
	return EdxlsetopSentinel;
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorUtils::Setoptype
//
//	@doc:
//		Return set operator type
//
//---------------------------------------------------------------------------
SetOperation
CTranslatorUtils::Setoptype
	(
	EdxlSetOpType edxlsetop
	)
{
	if (EdxlsetopUnionAll == edxlsetop || EdxlsetopUnion == edxlsetop)
	{
		return SETOP_UNION;
	}

	if (EdxlsetopIntersect == edxlsetop || EdxlsetopIntersectAll == edxlsetop)
	{
		return SETOP_INTERSECT;
	}

	if (EdxlsetopDifference == edxlsetop || EdxlsetopDifferenceAll == edxlsetop)
	{
		return SETOP_EXCEPT;
	}

	GPOS_ASSERT(!"Unrecognized set operator type");

	return SETOP_NONE;
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorUtils::PtemapCopy
//
//	@doc:
//		Create a copy of the hash map
//
//---------------------------------------------------------------------------
TEMap *
CTranslatorUtils::PtemapCopy
	(
	IMemoryPool *pmp,
	TEMap *ptemap
	)
{
	TEMap *ptemapCopy = GPOS_NEW(pmp) TEMap(pmp);

	// iterate over full map
	TEMapIter temapiter(ptemap);
	while (temapiter.FAdvance())
	{
		CMappingElementColIdTE *pmapelement =  const_cast<CMappingElementColIdTE *>(temapiter.Pt());

		const ULONG ulColId = pmapelement->UlColId();
		ULONG *pulKey1 = GPOS_NEW(pmp) ULONG(ulColId);
		pmapelement->AddRef();

#ifdef GPOS_DEBUG
		BOOL fres =
#endif
		ptemapCopy->FInsert(pulKey1, pmapelement);

#ifdef GPOS_DEBUG
		GPOS_ASSERT(fres);
#endif

	}
	return ptemapCopy;
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorUtils::Windowexclusion
//
//	@doc:
//		Return the GPDB frame exclusion strategy from its corresponding
//		DXL representation
//
//---------------------------------------------------------------------------
WindowExclusion
CTranslatorUtils::Windowexclusion
	(
	EdxlFrameExclusionStrategy edxlfes
	)
{
	GPOS_ASSERT(EdxlfesSentinel > edxlfes);
	ULONG rgrgulMapping[][2] =
		{
		{EdxlfesNulls, WINDOW_EXCLUSION_NULL},
		{EdxlfesCurrentRow, WINDOW_EXCLUSION_CUR_ROW},
		{EdxlfesGroup, WINDOW_EXCLUSION_GROUP},
		{EdxlfesTies, WINDOW_EXCLUSION_TIES}
		};

	const ULONG ulArity = GPOS_ARRAY_SIZE(rgrgulMapping);
	WindowExclusion we = WINDOW_EXCLUSION_NO_OTHERS;

	for (ULONG ul = 0; ul < ulArity; ul++)
	{
		ULONG *pulElem = rgrgulMapping[ul];
		if ((ULONG) edxlfes == pulElem[0])
		{
			we = (WindowExclusion) pulElem[1];
			break;
		}
	}
	return we;
}


//---------------------------------------------------------------------------
//	@function:
//		CTranslatorUtils::Windowboundkind
//
//	@doc:
//		Return the GPDB frame boundary kind from its corresponding
//		DXL representation
//
//---------------------------------------------------------------------------
WindowBoundingKind
CTranslatorUtils::Windowboundkind
	(
	EdxlFrameBoundary edxlfb
	)
{
	GPOS_ASSERT(EdxlfbSentinel > edxlfb);

	ULONG rgrgulMapping[][2] =
			{
			{EdxlfbUnboundedPreceding, WINDOW_UNBOUND_PRECEDING},
			{EdxlfbBoundedPreceding, WINDOW_BOUND_PRECEDING},
			{EdxlfbCurrentRow, WINDOW_CURRENT_ROW},
			{EdxlfbBoundedFollowing, WINDOW_BOUND_FOLLOWING},
			{EdxlfbUnboundedFollowing, WINDOW_UNBOUND_FOLLOWING},
			{EdxlfbDelayedBoundedPreceding, WINDOW_DELAYED_BOUND_PRECEDING},
		    {EdxlfbDelayedBoundedFollowing, WINDOW_DELAYED_BOUND_FOLLOWING}
			};

	const ULONG ulArity = GPOS_ARRAY_SIZE(rgrgulMapping);
	WindowBoundingKind wbk = WINDOW_UNBOUND_PRECEDING;
	for (ULONG ul = 0; ul < ulArity; ul++)
	{
		ULONG *pulElem = rgrgulMapping[ul];
		if ((ULONG) edxlfb == pulElem[0])
		{
			wbk = (WindowBoundingKind) pulElem[1];
			break;
		}
	}
	GPOS_ASSERT(WINDOW_DELAYED_BOUND_FOLLOWING >= wbk && "Invalid window frame boundary");

	return wbk;
}


//---------------------------------------------------------------------------
//	@function:
//		CTranslatorUtils::PdrgpulGroupingCols
//
//	@doc:
//		Construct a dynamic array of column ids for the given set of grouping
// 		col attnos
//
//---------------------------------------------------------------------------
DrgPul *
CTranslatorUtils::PdrgpulGroupingCols
	(
	IMemoryPool *pmp,
	CBitSet *pbsGroupByCols,
	HMIUl *phmiulSortGrpColsColId
	)
{
	DrgPul *pdrgpul = GPOS_NEW(pmp) DrgPul(pmp);

	if (NULL != pbsGroupByCols)
	{
		CBitSetIter bsi(*pbsGroupByCols);

		while (bsi.FAdvance())
		{
			const ULONG ulColId = UlColId(bsi.UlBit(), phmiulSortGrpColsColId);
			pdrgpul->Append(GPOS_NEW(pmp) ULONG(ulColId));
		}
	}

	return pdrgpul;
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorUtils::PdrgpbsGroupBy
//
//	@doc:
//		Construct a dynamic array of sets of column attnos corresponding to the
// 		group by clause
//
//---------------------------------------------------------------------------
DrgPbs *
CTranslatorUtils::PdrgpbsGroupBy
	(
	IMemoryPool *pmp,
	List *plGroupClause,
	ULONG ulCols,
	HMUlUl *phmululGrpColPos,	// mapping of grouping col positions to SortGroupRef ids
	CBitSet *pbsGrpCols			// existing uniqueue grouping columns
	)
{
	GPOS_ASSERT(NULL != plGroupClause);
	GPOS_ASSERT(0 < gpdb::UlListLength(plGroupClause));
	GPOS_ASSERT(NULL != phmululGrpColPos);

	Node *pnode = (Node*) LInitial(plGroupClause);

	if (NULL == pnode || IsA(pnode, GroupClause))
	{
		// simple group by
		CBitSet *pbsGroupingSet = PbsGroupingSet(pmp, plGroupClause, ulCols, phmululGrpColPos, pbsGrpCols);
		DrgPbs *pdrgpbs = GPOS_NEW(pmp) DrgPbs(pmp);
		pdrgpbs->Append(pbsGroupingSet);
		return pdrgpbs;
	}

	if (!IsA(pnode, GroupingClause))
	{
		GPOS_RAISE(gpdxl::ExmaDXL, gpdxl::ExmiQuery2DXLUnsupportedFeature, GPOS_WSZ_LIT("Group by clause"));
	}

	// grouping sets
	if (1 != gpdb::UlListLength(plGroupClause))
	{
		GPOS_RAISE(gpdxl::ExmaDXL, gpdxl::ExmiQuery2DXLUnsupportedFeature, GPOS_WSZ_LIT("Multiple grouping sets specifications"));
	}

	GroupingClause *pgrcl = (GroupingClause *) pnode;

	if (GROUPINGTYPE_ROLLUP == pgrcl->groupType)
	{
		return PdrgpbsRollup(pmp, pgrcl, ulCols, phmululGrpColPos, pbsGrpCols);
	}

	if (GROUPINGTYPE_GROUPING_SETS != pgrcl->groupType)
	{
		GPOS_RAISE(gpdxl::ExmaDXL, gpdxl::ExmiQuery2DXLUnsupportedFeature, GPOS_WSZ_LIT("Rollup and cube"));
	}

	DrgPbs *pdrgpbs = GPOS_NEW(pmp) DrgPbs(pmp);

	ListCell *plcGroupingSet = NULL;
	ForEach (plcGroupingSet, pgrcl->groupsets)
	{
		Node *pnodeGroupingSet = (Node *) lfirst(plcGroupingSet);

		CBitSet *pbs = NULL;
		if (IsA(pnodeGroupingSet, GroupClause))
		{
			// grouping set contains a single grouping column
			pbs = GPOS_NEW(pmp) CBitSet(pmp, ulCols);
			ULONG ulSortGrpRef = ((GroupClause *) pnodeGroupingSet)->tleSortGroupRef;
			pbs->FExchangeSet(ulSortGrpRef);
			UpdateGrpColMapping(pmp, phmululGrpColPos, pbsGrpCols, ulSortGrpRef);
		}
		else if (IsA(pnodeGroupingSet, GroupingClause))
		{
			GPOS_RAISE(gpdxl::ExmaDXL, gpdxl::ExmiQuery2DXLUnsupportedFeature, GPOS_WSZ_LIT("Rollup and cube"));
		}
		else
		{
			// grouping set contains a list of columns
			GPOS_ASSERT(IsA(pnodeGroupingSet, List));

			List *plGroupingSet = (List *) pnodeGroupingSet;
			pbs = PbsGroupingSet(pmp, plGroupingSet, ulCols, phmululGrpColPos, pbsGrpCols);
		}
		pdrgpbs->Append(pbs);
	}

	return pdrgpbs;
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorUtils::PdrgpbsRollup
//
//	@doc:
//		Construct a dynamic array of sets of column attnos for a rollup
//
//---------------------------------------------------------------------------
DrgPbs *
CTranslatorUtils::PdrgpbsRollup
	(
	IMemoryPool *pmp,
	GroupingClause *pgrcl,
	ULONG ulCols,
	HMUlUl *phmululGrpColPos,	// mapping of grouping col positions to SortGroupRef ids,
	CBitSet *pbsGrpCols			// existing grouping columns
	)
{
	GPOS_ASSERT(NULL != pgrcl);

	DrgPbs *pdrgpbsGroupingSets = GPOS_NEW(pmp) DrgPbs(pmp);
	ListCell *plcGroupingSet = NULL;
	ForEach (plcGroupingSet, pgrcl->groupsets)
	{
		Node *pnode = (Node *) lfirst(plcGroupingSet);
		CBitSet *pbs = GPOS_NEW(pmp) CBitSet(pmp);
		if (IsA(pnode, GroupClause))
		{
			// simple group clause, create a singleton grouping set
			GroupClause *pgrpcl = (GroupClause *) pnode;
			ULONG ulSortGrpRef = pgrpcl->tleSortGroupRef;
			(void) pbs->FExchangeSet(ulSortGrpRef);
			pdrgpbsGroupingSets->Append(pbs);
			UpdateGrpColMapping(pmp, phmululGrpColPos, pbsGrpCols, ulSortGrpRef);
		}
		else if (IsA(pnode, List))
		{
			// list of group clauses, add all clauses into one grouping set
			// for example, rollup((a,b),(c,d));
			List *plist = (List *) pnode;
			ListCell *plcGrpCl = NULL;
			ForEach (plcGrpCl, plist)
			{
				Node *pnodeGrpCl = (Node *) lfirst(plcGrpCl);
				if (!IsA(pnodeGrpCl, GroupClause))
				{
					// each list entry must be a group clause
					// for example, rollup((a,b),(c,(d,e)));
					GPOS_RAISE(gpdxl::ExmaDXL, gpdxl::ExmiQuery2DXLUnsupportedFeature, GPOS_WSZ_LIT("Nested grouping sets"));
				}

				GroupClause *pgrpcl = (GroupClause *) pnodeGrpCl;
				ULONG ulSortGrpRef = pgrpcl->tleSortGroupRef;
				(void) pbs->FExchangeSet(ulSortGrpRef);
				UpdateGrpColMapping(pmp, phmululGrpColPos, pbsGrpCols, ulSortGrpRef);
			}
			pdrgpbsGroupingSets->Append(pbs);
		}
		else
		{
			// unsupported rollup operation
			GPOS_RAISE(gpdxl::ExmaDXL, gpdxl::ExmiQuery2DXLUnsupportedFeature, GPOS_WSZ_LIT("Nested grouping sets"));
		}
	}

	const ULONG ulGroupingSets = pdrgpbsGroupingSets->UlLength();
	DrgPbs *pdrgpbs = GPOS_NEW(pmp) DrgPbs(pmp);

	// compute prefixes of grouping sets array
	for (ULONG ulPrefix = 0; ulPrefix <= ulGroupingSets; ulPrefix++)
	{
		CBitSet *pbs = GPOS_NEW(pmp) CBitSet(pmp);
		for (ULONG ulIdx = 0; ulIdx < ulPrefix; ulIdx++)
		{
			CBitSet *pbsCurrent = (*pdrgpbsGroupingSets)[ulIdx];
			pbs->Union(pbsCurrent);
		}
		pdrgpbs->Append(pbs);
	}
	pdrgpbsGroupingSets->Release();

	return pdrgpbs;
}


//---------------------------------------------------------------------------
//	@function:
//		CTranslatorUtils::PbsGroupingSet
//
//	@doc:
//		Construct a set of column attnos corresponding to a grouping set
//
//---------------------------------------------------------------------------
CBitSet *
CTranslatorUtils::PbsGroupingSet
	(
	IMemoryPool *pmp,
	List *plGroupElems,
	ULONG ulCols,
	HMUlUl *phmululGrpColPos,	// mapping of grouping col positions to SortGroupRef ids,
	CBitSet *pbsGrpCols			// existing grouping columns
	)
{
	GPOS_ASSERT(NULL != plGroupElems);
	GPOS_ASSERT(0 < gpdb::UlListLength(plGroupElems));

	CBitSet *pbs = GPOS_NEW(pmp) CBitSet(pmp, ulCols);

	ListCell *plc = NULL;
	ForEach (plc, plGroupElems)
	{
		Node *pnodeElem = (Node*) lfirst(plc);

		if (NULL == pnodeElem)
		{
			continue;
		}

		if (!IsA(pnodeElem, GroupClause))
		{
			GPOS_RAISE(gpdxl::ExmaDXL, gpdxl::ExmiQuery2DXLUnsupportedFeature, GPOS_WSZ_LIT("Mixing grouping sets with simple group by lists"));
		}

		ULONG ulSortGrpRef = ((GroupClause *) pnodeElem)->tleSortGroupRef;
		pbs->FExchangeSet(ulSortGrpRef);
		
		UpdateGrpColMapping(pmp, phmululGrpColPos, pbsGrpCols, ulSortGrpRef);
	}
	
	return pbs;
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorUtils::PdrgpulGenerateColIds
//
//	@doc:
//		Construct an array of DXL column identifiers for a target list
//
//---------------------------------------------------------------------------
DrgPul *
CTranslatorUtils::PdrgpulGenerateColIds
	(
	IMemoryPool *pmp,
	List *plTargetList,
	DrgPmdid *pdrgpmdidInput,
	DrgPul *pdrgpulInput,
	BOOL *pfOuterRef,  // array of flags indicating if input columns are outer references
	CIdGenerator *pidgtorColId
	)
{
	GPOS_ASSERT(NULL != plTargetList);
	GPOS_ASSERT(NULL != pdrgpmdidInput);
	GPOS_ASSERT(NULL != pdrgpulInput);
	GPOS_ASSERT(NULL != pfOuterRef);
	GPOS_ASSERT(NULL != pidgtorColId);

	GPOS_ASSERT(pdrgpmdidInput->UlLength() == pdrgpulInput->UlLength());

	ULONG ulColPos = 0;
	ListCell *plcTE = NULL;
	DrgPul *pdrgpul = GPOS_NEW(pmp) DrgPul(pmp);

	ForEach (plcTE, plTargetList)
	{
		TargetEntry *pte = (TargetEntry *) lfirst(plcTE);
		GPOS_ASSERT(NULL != pte->expr);

		OID oidExprType = gpdb::OidExprType((Node*) pte->expr);
		if (!pte->resjunk)
		{
			ULONG ulColId = ULONG_MAX;
			IMDId *pmdid = (*pdrgpmdidInput)[ulColPos];
			if (CMDIdGPDB::PmdidConvert(pmdid)->OidObjectId() != oidExprType || 
				pfOuterRef[ulColPos])
			{
				// generate a new column when:
				//  (1) the type of input column does not match that of the output column, or
				//  (2) input column is an outer reference 
				ulColId = pidgtorColId->UlNextId();
			}
			else
			{
				// use the column identifier of the input
				ulColId = *(*pdrgpulInput)[ulColPos];
			}
			GPOS_ASSERT(ULONG_MAX != ulColId);
			
			pdrgpul->Append(GPOS_NEW(pmp) ULONG(ulColId));

			ulColPos++;
		}
	}

	return pdrgpul;
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorUtils::PqueryFixUnknownTypeConstant
//
//	@doc:
//		If the query has constant of unknown type, then return a copy of the
//		query with all constants of unknown type being coerced to the common data type
//		of the output target list; otherwise return the original query
//---------------------------------------------------------------------------
Query *
CTranslatorUtils::PqueryFixUnknownTypeConstant
	(
	Query *pqueryOld,
	List *plTargetListOutput
	)
{
	GPOS_ASSERT(NULL != pqueryOld);
	GPOS_ASSERT(NULL != plTargetListOutput);

	Query *pqueryNew = NULL;

	ULONG ulPos = 0;
	ULONG ulColPos = 0;
	ListCell *plcTE = NULL;
	ForEach (plcTE, pqueryOld->targetList)
	{
		TargetEntry *pteOld = (TargetEntry *) lfirst(plcTE);
		GPOS_ASSERT(NULL != pteOld->expr);

		if (!pteOld->resjunk)
		{
			if (IsA(pteOld->expr, Const) && (GPDB_UNKNOWN == gpdb::OidExprType((Node*) pteOld->expr) ))
			{
				if (NULL == pqueryNew)
				{
					pqueryNew = (Query*) gpdb::PvCopyObject(const_cast<Query*>(pqueryOld));
				}

				TargetEntry *pteNew = (TargetEntry *) gpdb::PvListNth(pqueryNew->targetList, ulPos);
				GPOS_ASSERT(pteOld->resno == pteNew->resno);
				// implicitly cast the unknown constants to the target data type
				OID oidTargetType = OidTargetListReturnType(plTargetListOutput, ulColPos);
				GPOS_ASSERT(InvalidOid != oidTargetType);
				Node *pnodeOld = (Node *) pteNew->expr;
				pteNew->expr = (Expr*) gpdb::PnodeCoerceToCommonType
											(
											NULL,	/* pstate */
											(Node *) pnodeOld,
											oidTargetType,
											"UNION/INTERSECT/EXCEPT"
											);

				gpdb::GPDBFree(pnodeOld);
			}
			ulColPos++;
		}

		ulPos++;
	}

	if (NULL == pqueryNew)
	{
		return pqueryOld;
	}

	gpdb::GPDBFree(pqueryOld);

	return pqueryNew;
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorUtils::OidTargetListReturnType
//
//	@doc:
//		Return the type of the nth non-resjunked target list entry
//
//---------------------------------------------------------------------------
OID
CTranslatorUtils::OidTargetListReturnType
	(
	List *plTargetList,
	ULONG ulColPos
	)
{
	ULONG ulColIdx = 0;
	ListCell *plcTE = NULL;

	ForEach (plcTE, plTargetList)
	{
		TargetEntry *pte = (TargetEntry *) lfirst(plcTE);
		GPOS_ASSERT(NULL != pte->expr);

		if (!pte->resjunk)
		{
			if (ulColIdx == ulColPos)
			{
				return gpdb::OidExprType((Node*) pte->expr);
			}

			ulColIdx++;
		}
	}

	return InvalidOid;
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorUtils::Pdrgpdxlcd
//
//	@doc:
//		Construct an array of DXL column descriptors for a target list using the
// 		column ids in the given array
//
//---------------------------------------------------------------------------
DrgPdxlcd *
CTranslatorUtils::Pdrgpdxlcd
	(
	IMemoryPool *pmp,
	List *plTargetList,
	DrgPul *pdrgpulColIds,
	BOOL fKeepResjunked
	)
{
	GPOS_ASSERT(NULL != plTargetList);
	GPOS_ASSERT(NULL != pdrgpulColIds);

	ListCell *plcTE = NULL;
	DrgPdxlcd *pdrgpdxlcd = GPOS_NEW(pmp) DrgPdxlcd(pmp);
	ULONG ul = 0;
	ForEach (plcTE, plTargetList)
	{
		TargetEntry *pte = (TargetEntry *) lfirst(plcTE);

		if (pte->resjunk && !fKeepResjunked)
		{
			continue;
		}

		ULONG ulColId = *(*pdrgpulColIds)[ul];
		CDXLColDescr *pdxlcd = Pdxlcd(pmp, pte, ulColId, ul+1 /*ulPos*/);
		pdrgpdxlcd->Append(pdxlcd);
		ul++;
	}

	GPOS_ASSERT(pdrgpdxlcd->UlLength() == pdrgpulColIds->UlLength());

	return pdrgpdxlcd;
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorUtils::PdrgpulPosInTargetList
//
//	@doc:
//		Return the positions of the target list entries included in the output
//		target list
//---------------------------------------------------------------------------
DrgPul *
CTranslatorUtils::PdrgpulPosInTargetList
	(
	IMemoryPool *pmp,
	List *plTargetList,
	BOOL fKeepResjunked
	)
{
	GPOS_ASSERT(NULL != plTargetList);

	ListCell *plcTE = NULL;
	DrgPul *pdrgul = GPOS_NEW(pmp) DrgPul(pmp);
	ULONG ul = 0;
	ForEach (plcTE, plTargetList)
	{
		TargetEntry *pte = (TargetEntry *) lfirst(plcTE);

		if (pte->resjunk && !fKeepResjunked)
		{
			continue;
		}

		pdrgul->Append(GPOS_NEW(pmp) ULONG(ul));
		ul++;
	}

	return pdrgul;
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorUtils::Pdxlcd
//
//	@doc:
//		Construct a column descriptor from the given target entry and column
//		identifier
//---------------------------------------------------------------------------
CDXLColDescr *
CTranslatorUtils::Pdxlcd
	(
	IMemoryPool *pmp,
	TargetEntry *pte,
	ULONG ulColId,
	ULONG ulPos
	)
{
	GPOS_ASSERT(NULL != pte);
	GPOS_ASSERT(ULONG_MAX != ulColId);

	CMDName *pmdname = NULL;
	if (NULL == pte->resname)
	{
		CWStringConst strUnnamedCol(GPOS_WSZ_LIT("?column?"));
		pmdname = GPOS_NEW(pmp) CMDName(pmp, &strUnnamedCol);
	}
	else
	{
		CWStringDynamic *pstrAlias = CDXLUtils::PstrFromSz(pmp, pte->resname);
		pmdname = GPOS_NEW(pmp) CMDName(pmp, pstrAlias);
		// CName constructor copies string
		GPOS_DELETE(pstrAlias);
	}

	// create a column descriptor
	OID oidType = gpdb::OidExprType((Node *) pte->expr);
	CMDIdGPDB *pmdidColType = CTranslatorUtils::PmdidWithVersion(pmp, oidType);
	CDXLColDescr *pdxlcd = GPOS_NEW(pmp) CDXLColDescr
									(
									pmp,
									pmdname,
									ulColId,
									ulPos, /* attno */
									pmdidColType,
									false /* fColDropped */
									);

	return pdxlcd;
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorUtils::PdxlnDummyPrElem
//
//	@doc:
//		Create a dummy project element to rename the input column identifier
//---------------------------------------------------------------------------
CDXLNode *
CTranslatorUtils::PdxlnDummyPrElem
	(
	IMemoryPool *pmp,
	ULONG ulColIdInput,
	ULONG ulColIdOutput,
	CDXLColDescr *pdxlcdOutput
	)
{
	CMDIdGPDB *pmdidOriginal = CMDIdGPDB::PmdidConvert(pdxlcdOutput->PmdidType());
	CMDIdGPDB *pmdidCopy = GPOS_NEW(pmp) CMDIdGPDB(pmdidOriginal->OidObjectId(), pmdidOriginal->UlVersionMajor(), pmdidOriginal->UlVersionMinor());

	// create a column reference for the scalar identifier to be casted
	ULONG ulColId = pdxlcdOutput->UlID();
	CMDName *pmdname = GPOS_NEW(pmp) CMDName(pmp, pdxlcdOutput->Pmdname()->Pstr());
	CDXLColRef *pdxlcr = GPOS_NEW(pmp) CDXLColRef(pmp, pmdname, ulColIdInput);
	CDXLScalarIdent *pdxlopIdent = GPOS_NEW(pmp) CDXLScalarIdent(pmp, pdxlcr, pmdidCopy);

	CDXLNode *pdxlnPrEl = GPOS_NEW(pmp) CDXLNode
										(
										pmp,
										GPOS_NEW(pmp) CDXLScalarProjElem
													(
													pmp,
													ulColIdOutput,
													GPOS_NEW(pmp) CMDName(pmp, pdxlcdOutput->Pmdname()->Pstr())
													),
										GPOS_NEW(pmp) CDXLNode(pmp, pdxlopIdent)
										);

	return pdxlnPrEl;
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorUtils::PdrgpulColIds
//
//	@doc:
//		Construct an array of colids for the given target list
//
//---------------------------------------------------------------------------
DrgPul *
CTranslatorUtils::PdrgpulColIds
	(
	IMemoryPool *pmp,
	List *plTargetList,
	HMIUl *phmiulAttnoColId
	)
{
	GPOS_ASSERT(NULL != plTargetList);
	GPOS_ASSERT(NULL != phmiulAttnoColId);

	DrgPul *pdrgpul = GPOS_NEW(pmp) DrgPul(pmp);

	ListCell *plcTE = NULL;
	ForEach (plcTE, plTargetList)
	{
		TargetEntry *pte = (TargetEntry *) lfirst(plcTE);
		ULONG ulResNo = (ULONG) pte->resno;
		INT iAttno = (INT) pte->resno;
		const ULONG *pul = phmiulAttnoColId->PtLookup(&iAttno);

		if (NULL == pul)
		{
			GPOS_RAISE(gpdxl::ExmaDXL, gpdxl::ExmiQuery2DXLAttributeNotFound, ulResNo);
		}

		pdrgpul->Append(GPOS_NEW(pmp) ULONG(*pul));
	}

	return pdrgpul;
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorUtils::UlColId
//
//	@doc:
//		Return the corresponding ColId for the given index into the target list
//
//---------------------------------------------------------------------------
ULONG
CTranslatorUtils::UlColId
	(
	INT iIndex,
	HMIUl *phmiulColId
	)
{
	GPOS_ASSERT(0 < iIndex);

	const ULONG *pul = phmiulColId->PtLookup(&iIndex);

	if (NULL == pul)
	{
		GPOS_RAISE(gpdxl::ExmaDXL, gpdxl::ExmiQuery2DXLAttributeNotFound, iIndex);
	}

	return *pul;
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorUtils::UlColId
//
//	@doc:
//		Return the corresponding ColId for the given varno, varattno and querylevel
//
//---------------------------------------------------------------------------
ULONG
CTranslatorUtils::UlColId
	(
	ULONG ulQueryLevel,
	INT iVarno,
	INT iVarAttno,
	IMDId *pmdid,
	CMappingVarColId *pmapvarcolid
	)
{
	OID oid = CMDIdGPDB::PmdidConvert(pmdid)->OidObjectId();
	Var *pvar = gpdb::PvarMakeVar(iVarno, iVarAttno, oid, -1, 0);
	ULONG ulColId = pmapvarcolid->UlColId(ulQueryLevel, pvar, EpspotNone);
	gpdb::GPDBFree(pvar);

	return ulColId;
}


//---------------------------------------------------------------------------
//	@function:
//		CTranslatorUtils::PteWindowSpec
//
//	@doc:
//		Extract a matching target entry that is a window spec
//		
//---------------------------------------------------------------------------
TargetEntry *
CTranslatorUtils::PteWindowSpec
	(
	Node *pnode,
	List *plWindowClause,
	List *plTargetList
	)
{
	GPOS_ASSERT(NULL != pnode);
	List *plTargetListSubset = gpdb::PteMembers(pnode, plTargetList);

	ListCell *plcTE = NULL;
	ForEach (plcTE, plTargetListSubset)
	{
		TargetEntry *pteCurr = (TargetEntry*) lfirst(plcTE);
		if (FWindowSpec(pteCurr, plWindowClause))
		{
			gpdb::GPDBFree(plTargetListSubset);
			return pteCurr;
		}
	}

	if (NIL != plTargetListSubset)
	{
		gpdb::GPDBFree(plTargetListSubset);
	}
	return NULL;
}


//---------------------------------------------------------------------------
//	@function:
//		CTranslatorUtils::FWindowSpec
//
//	@doc:
//		Check if the expression has a matching target entry that is a window spec
//---------------------------------------------------------------------------
BOOL
CTranslatorUtils::FWindowSpec
	(
	Node *pnode,
	List *plWindowClause,
	List *plTargetList
	)
{
	GPOS_ASSERT(NULL != pnode);
	
	TargetEntry *pteWindoSpec = PteWindowSpec(pnode, plWindowClause, plTargetList);

	return (NULL != pteWindoSpec);
}


//---------------------------------------------------------------------------
//	@function:
//		CTranslatorUtils::FWindowSpec
//
//	@doc:
//		Check if the TargetEntry is a used in the window specification
//---------------------------------------------------------------------------
BOOL
CTranslatorUtils::FWindowSpec
	(
	const TargetEntry *pte,
	List *plWindowClause
	)
{
	ListCell *plcWindowCl = NULL;
	ForEach (plcWindowCl, plWindowClause)
	{
		WindowSpec *pwindowspec = (WindowSpec*) lfirst(plcWindowCl);
		if (FSortingColumn(pte, pwindowspec->order) || FSortingColumn(pte, pwindowspec->partition))
		{
			return true;
		}
	}
	return false;
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorUtils::PdxlnInt4Const
//
//	@doc:
// 		Construct a scalar const value expression for the given INT value
//
//---------------------------------------------------------------------------
CDXLNode *
CTranslatorUtils::PdxlnInt4Const
	(
	IMemoryPool *pmp,
	CMDAccessor *pmda,
	INT iVal
	)
{
	GPOS_ASSERT(NULL != pmp);
	const IMDTypeInt4 *pmdtypeint4 = pmda->PtMDType<IMDTypeInt4>();
	pmdtypeint4->Pmdid()->AddRef();

	CDXLDatumInt4 *pdxldatum = GPOS_NEW(pmp) CDXLDatumInt4(pmp, pmdtypeint4->Pmdid(), false /*fConstNull*/, iVal);

	CDXLScalarConstValue *pdxlConst = GPOS_NEW(pmp) CDXLScalarConstValue(pmp, pdxldatum);

	return GPOS_NEW(pmp) CDXLNode(pmp, pdxlConst);
}


//---------------------------------------------------------------------------
//	@function:
//		CTranslatorUtils::FSortingColumn
//
//	@doc:
//		Check if the TargetEntry is a sorting column
//---------------------------------------------------------------------------
BOOL
CTranslatorUtils::FSortingColumn
	(
	const TargetEntry *pte,
	List *plSortCl
	)
{
	ListCell *plcSortCl = NULL;
	ForEach (plcSortCl, plSortCl)
	{
		Node *pnodeSortCl = (Node*) lfirst(plcSortCl);
		if (IsA(pnodeSortCl, SortClause) && pte->ressortgroupref == ((SortClause *) pnodeSortCl)->tleSortGroupRef)
		{
			return true;
		}
	}

	return false;
}


//---------------------------------------------------------------------------
//	@function:
//		CTranslatorUtils::PteGroupingColumn
//
//	@doc:
//		Extract a matching target entry that is a grouping column
//---------------------------------------------------------------------------
TargetEntry *
CTranslatorUtils::PteGroupingColumn
	(
	Node *pnode,
	List *plGrpCl,
	List *plTargetList
	)
{
	GPOS_ASSERT(NULL != pnode);
	List *plTargetListSubset = gpdb::PteMembers(pnode, plTargetList);

	ListCell *plcTE = NULL;
	ForEach (plcTE, plTargetListSubset)
	{
		TargetEntry *pteNext = (TargetEntry*) lfirst(plcTE);
		if (FGroupingColumn(pteNext, plGrpCl))
		{
			gpdb::GPDBFree(plTargetListSubset);
			return pteNext;
		}
	}

	if (NIL != plTargetListSubset)
	{
		gpdb::GPDBFree(plTargetListSubset);
	}
	return NULL;
}


//---------------------------------------------------------------------------
//	@function:
//		CTranslatorUtils::FGroupingColumn
//
//	@doc:
//		Check if the expression has a matching target entry that is a grouping column
//---------------------------------------------------------------------------
BOOL
CTranslatorUtils::FGroupingColumn
	(
	Node *pnode,
	List *plGrpCl,
	List *plTargetList
	)
{
	GPOS_ASSERT(NULL != pnode);

	TargetEntry *pteGroupingCol = PteGroupingColumn(pnode, plGrpCl, plTargetList);

	return (NULL != pteGroupingCol);
}


//---------------------------------------------------------------------------
//	@function:
//		CTranslatorUtils::FGroupingColumn
//
//	@doc:
//		Check if the TargetEntry is a grouping column
//---------------------------------------------------------------------------
BOOL
CTranslatorUtils::FGroupingColumn
	(
	const TargetEntry *pte,
	List *plGrpCl
	)
{
	ListCell *plcGrpCl = NULL;
	ForEach (plcGrpCl, plGrpCl)
	{
		Node *pnodeGrpCl = (Node*) lfirst(plcGrpCl);

		if (NULL == pnodeGrpCl)
		{
			continue;
		}

		if (IsA(pnodeGrpCl, GroupClause) && FGroupingColumn(pte, (GroupClause*) pnodeGrpCl))
		{
			return true;
		}

		if (IsA(pnodeGrpCl, GroupingClause))
		{
			GroupingClause *pgrcl = (GroupingClause *) pnodeGrpCl;

			ListCell *plcGroupingSet = NULL;
			ForEach (plcGroupingSet, pgrcl->groupsets)
			{
				Node *pnodeGroupingSet = (Node *) lfirst(plcGroupingSet);

				if (IsA(pnodeGroupingSet, GroupClause) && FGroupingColumn(pte, ((GroupClause *) pnodeGroupingSet)))
				{
					return true;
				}

				if (IsA(pnodeGroupingSet, List) && FGroupingColumn(pte, (List *) pnodeGroupingSet))
				{
					return true;
				}
			}
		}
	}

	return false;
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorUtils::FGroupingColumn
//
//	@doc:
//		Check if the TargetEntry is a grouping column
//---------------------------------------------------------------------------
BOOL
CTranslatorUtils::FGroupingColumn
	(
	const TargetEntry *pte,
	const GroupClause *pgrcl
	)
{
	GPOS_ASSERT(NULL != pgrcl);

	return (pte->ressortgroupref == pgrcl->tleSortGroupRef);
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorUtils::FGroupingColumn
//
//	@doc:
//		Check if the sorting column is a grouping column
//---------------------------------------------------------------------------
BOOL
CTranslatorUtils::FGroupingColumn
	(
	const SortClause *psortcl,
	List *plGrpCl
	)
{
	ListCell *plcGrpCl = NULL;
	ForEach (plcGrpCl, plGrpCl)
	{
		Node *pnodeGrpCl = (Node*) lfirst(plcGrpCl);
		GPOS_ASSERT(IsA(pnodeGrpCl, GroupClause) && "We currently do not support grouping sets.");

		GroupClause *pgrpcl = (GroupClause*) pnodeGrpCl;
		if (psortcl->tleSortGroupRef == pgrpcl->tleSortGroupRef)
		{
			return true;
		}
	}

	return false;
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorUtils::PlAttnosFromColids
//
//	@doc:
//		Translate an array of colids to a list of attribute numbers using
//		the mappings in the provided context
//---------------------------------------------------------------------------
List *
CTranslatorUtils::PlAttnosFromColids
	(
	DrgPul *pdrgpul,
	CDXLTranslateContext *pdxltrctx
	)
{
	GPOS_ASSERT(NULL != pdrgpul);
	GPOS_ASSERT(NULL != pdxltrctx);
	
	List *plResult = NIL;
	
	const ULONG ulLength = pdrgpul->UlLength();
	for (ULONG ul = 0; ul < ulLength; ul++)
	{
		ULONG ulColId = *((*pdrgpul)[ul]);
		const TargetEntry *pte = pdxltrctx->Pte(ulColId);
		GPOS_ASSERT(NULL != pte);
		plResult = gpdb::PlAppendInt(plResult, pte->resno);
	}
	
	return plResult;
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorUtils::LFromStr
//
//	@doc:
//		Parses a long integer value from a string
//
//---------------------------------------------------------------------------
LINT
CTranslatorUtils::LFromStr
	(
	const CWStringBase *pstr
	)
{
	CHAR *sz = SzFromWsz(pstr->Wsz());
	CHAR *pcEnd = NULL;
	return gpos::clib::LStrToL(sz, &pcEnd, 10);
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorUtils::IFromStr
//
//	@doc:
//		Parses an integer value from a string
//
//---------------------------------------------------------------------------
INT
CTranslatorUtils::IFromStr
	(
	const CWStringBase *pstr
	)
{
	return (INT) LFromStr(pstr);
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorUtils::SzFromWsz
//
//	@doc:
//		Converts a wide character string into a character array
//
//---------------------------------------------------------------------------
CHAR *
CTranslatorUtils::SzFromWsz
	(
	const WCHAR *wsz
	)
{
	GPOS_ASSERT(NULL != wsz);

	ULONG ulMaxLength = GPOS_WSZ_LENGTH(wsz) * GPOS_SIZEOF(WCHAR) + 1;
	CHAR *sz = (CHAR *) gpdb::GPDBAlloc(ulMaxLength);
#ifdef GPOS_DEBUG
	LINT li = (INT)
#endif
	clib::LWcsToMbs(sz, const_cast<WCHAR *>(wsz), ulMaxLength);
	GPOS_ASSERT(0 <= li);

	sz[ulMaxLength - 1] = '\0';

	return sz;
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorUtils::PhmululMap
//
//	@doc:
//		Create a mapping from old columns to the corresponding new column
//
//---------------------------------------------------------------------------
HMUlUl *
CTranslatorUtils::PhmululMap
	(
	IMemoryPool *pmp,
	DrgPul *pdrgpulOld,
	DrgPul *pdrgpulNew
	)
{
	GPOS_ASSERT(NULL != pdrgpulOld);
	GPOS_ASSERT(NULL != pdrgpulNew);
	GPOS_ASSERT(pdrgpulNew->UlLength() == pdrgpulOld->UlLength());
	
	HMUlUl *phmulul = GPOS_NEW(pmp) HMUlUl(pmp);
	const ULONG ulCols = pdrgpulOld->UlLength();
	for (ULONG ul = 0; ul < ulCols; ul++)
	{
		ULONG ulColIdOld = *((*pdrgpulOld)[ul]);
		ULONG ulColIdNew = *((*pdrgpulNew)[ul]);
#ifdef GPOS_DEBUG
		BOOL fResult = 
#endif // GPOS_DEBUG
		phmulul->FInsert(GPOS_NEW(pmp) ULONG(ulColIdOld), GPOS_NEW(pmp) ULONG(ulColIdNew));
		GPOS_ASSERT(fResult);
	}
	
	return phmulul;
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorUtils::FDuplicateSensitiveMotion
//
//	@doc:
//		Is this a motion sensitive to duplicates
//
//---------------------------------------------------------------------------
BOOL
CTranslatorUtils::FDuplicateSensitiveMotion
	(
	CDXLPhysicalMotion *pdxlopMotion
	)
{
	Edxlopid edxlopid = pdxlopMotion->Edxlop();
	
	if (EdxlopPhysicalMotionRedistribute == edxlopid)
	{
		return CDXLPhysicalRedistributeMotion::PdxlopConvert(pdxlopMotion)->FDuplicateSensitive();
	}
	
	if (EdxlopPhysicalMotionRandom == edxlopid)
	{
		return CDXLPhysicalRandomMotion::PdxlopConvert(pdxlopMotion)->FDuplicateSensitive();
	}
	
	// other motion operators are not sensitive to duplicates
	return false;
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorUtils::FHasProjElem
//
//	@doc:
//		Check whether the given project list has a project element of the given
//		operator type
//
//---------------------------------------------------------------------------
BOOL
CTranslatorUtils::FHasProjElem
	(
	CDXLNode *pdxlnPrL,
	Edxlopid edxlopid
	)
{
	GPOS_ASSERT(NULL != pdxlnPrL);
	GPOS_ASSERT(EdxlopScalarProjectList == pdxlnPrL->Pdxlop()->Edxlop());
	GPOS_ASSERT(EdxlopSentinel > edxlopid);

	const ULONG ulArity = pdxlnPrL->UlArity();
	for (ULONG ul = 0; ul < ulArity; ul++)
	{
		CDXLNode *pdxlnPrEl = (*pdxlnPrL)[ul];
		GPOS_ASSERT(EdxlopScalarProjectElem == pdxlnPrEl->Pdxlop()->Edxlop());

		CDXLNode *pdxlnChild = (*pdxlnPrEl)[0];
		if (edxlopid == pdxlnChild->Pdxlop()->Edxlop())
		{
			return true;
		}
	}

	return false;
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorUtils::PdxlnPrElNull
//
//	@doc:
//		Create a DXL project element node with a Const NULL of type provided
//		by the column descriptor. The function raises an exception if the
//		column is not nullable.
//
//---------------------------------------------------------------------------
CDXLNode *
CTranslatorUtils::PdxlnPrElNull
	(
	IMemoryPool *pmp,
	CMDAccessor *pmda,
	CIdGenerator *pidgtorCol,
	const IMDColumn *pmdcol
	)
{
	GPOS_ASSERT(NULL != pmdcol);
	GPOS_ASSERT(!pmdcol->FSystemColumn());

	const WCHAR *wszColName = pmdcol->Mdname().Pstr()->Wsz();
	if (!pmdcol->FNullable())
	{
		GPOS_RAISE(gpdxl::ExmaDXL, gpdxl::ExmiQuery2DXLNotNullViolation, wszColName);
	}

	ULONG ulColId = pidgtorCol->UlNextId();
	CDXLNode *pdxlnPrE = PdxlnPrElNull(pmp, pmda, pmdcol->PmdidType(), ulColId, wszColName);

	return pdxlnPrE;
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorUtils::PdxlnPrElNull
//
//	@doc:
//		Create a DXL project element node with a Const NULL expression
//---------------------------------------------------------------------------
CDXLNode *
CTranslatorUtils::PdxlnPrElNull
	(
	IMemoryPool *pmp,
	CMDAccessor *pmda,
	IMDId *pmdid,
	ULONG ulColId,
	const WCHAR *wszColName
	)
{
	CHAR *szColumnName = CDXLUtils::SzFromWsz(pmp, wszColName);
	CDXLNode *pdxlnPrE = PdxlnPrElNull(pmp, pmda, pmdid, ulColId, szColumnName);

	GPOS_DELETE_ARRAY(szColumnName);

	return pdxlnPrE;
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorUtils::PdxlnPrElNull
//
//	@doc:
//		Create a DXL project element node with a Const NULL expression
//---------------------------------------------------------------------------
CDXLNode *
CTranslatorUtils::PdxlnPrElNull
	(
	IMemoryPool *pmp,
	CMDAccessor *pmda,
	IMDId *pmdid,
	ULONG ulColId,
	CHAR *szAliasName
	)
{
	BOOL fByValue = pmda->Pmdtype(pmdid)->FByValue();

	// get the id and alias for the proj elem
	CMDName *pmdnameAlias = NULL;

	if (NULL == szAliasName)
	{
		CWStringConst strUnnamedCol(GPOS_WSZ_LIT("?column?"));
		pmdnameAlias = GPOS_NEW(pmp) CMDName(pmp, &strUnnamedCol);
	}
	else
	{
		CWStringDynamic *pstrAlias = CDXLUtils::PstrFromSz(pmp, szAliasName);
		pmdnameAlias = GPOS_NEW(pmp) CMDName(pmp, pstrAlias);
		GPOS_DELETE(pstrAlias);
	}

	pmdid->AddRef();
	CDXLDatum *pdxldatum = NULL;
	if (pmdid->FEquals(&CMDIdGPDB::m_mdidInt2))
	{
		pdxldatum = GPOS_NEW(pmp) CDXLDatumInt2(pmp, pmdid, true /*fConstNull*/, 0 /*value*/);
	}
	else if (pmdid->FEquals(&CMDIdGPDB::m_mdidInt4))
	{
		pdxldatum = GPOS_NEW(pmp) CDXLDatumInt4(pmp, pmdid, true /*fConstNull*/, 0 /*value*/);
	}
	else if (pmdid->FEquals(&CMDIdGPDB::m_mdidInt8))
	{
		pdxldatum = GPOS_NEW(pmp) CDXLDatumInt8(pmp, pmdid, true /*fConstNull*/, 0 /*value*/);
	}
	else if (pmdid->FEquals(&CMDIdGPDB::m_mdidBool))
	{
		pdxldatum = GPOS_NEW(pmp) CDXLDatumBool(pmp, pmdid, true /*fConstNull*/, 0 /*value*/);
	}
	else if (pmdid->FEquals(&CMDIdGPDB::m_mdidOid))
	{
		pdxldatum = GPOS_NEW(pmp) CDXLDatumOid(pmp, pmdid, true /*fConstNull*/, 0 /*value*/);
	}
	else
	{
		pdxldatum = CMDTypeGenericGPDB::Pdxldatum
										(
										pmp,
										pmdid,
										fByValue /*fConstByVal*/,
										true /*fConstNull*/,
										NULL, /*pba */
										0 /*ulLength*/,
										0 /*lValue*/,
										0 /*dValue*/
										);
	}

	CDXLNode *pdxlnConst = GPOS_NEW(pmp) CDXLNode(pmp, GPOS_NEW(pmp) CDXLScalarConstValue(pmp, pdxldatum));

	return GPOS_NEW(pmp) CDXLNode(pmp, GPOS_NEW(pmp) CDXLScalarProjElem(pmp, ulColId, pmdnameAlias), pdxlnConst);
}


//---------------------------------------------------------------------------
//	@function:
//		CTranslatorUtils::CheckRTEPermissions
//
//	@doc:
//		Check permissions on range table
//
//---------------------------------------------------------------------------
void
CTranslatorUtils::CheckRTEPermissions
	(
	List *plRangeTable
	)
{
	gpdb::CheckRTPermissions(plRangeTable);
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorUtils::CheckAggregateWindowFn
//
//	@doc:
//		Check if the window function is an aggregate and has either
//      prelim or inverse prelim function
//
//---------------------------------------------------------------------------
void
CTranslatorUtils::CheckAggregateWindowFn
	(
	Node *pnode
	)
{
	GPOS_ASSERT(NULL != pnode);
	GPOS_ASSERT(IsA(pnode, WindowRef));

	WindowRef *pwinref = (WindowRef*) pnode;

	if (gpdb::FAggregateExists(pwinref->winfnoid) && !gpdb::FAggHasPrelimOrInvPrelimFunc(pwinref->winfnoid))
	{
		GPOS_RAISE(gpdxl::ExmaDXL, gpdxl::ExmiQuery2DXLUnsupportedFeature,
				GPOS_WSZ_LIT("Aggregate window function without prelim or inverse prelim function"));
	}
}


//---------------------------------------------------------------------------
//	@function:
//		CTranslatorUtils::UpdateGrpColMapping
//
//	@doc:
//		Update grouping columns permission mappings
//
//---------------------------------------------------------------------------
void
CTranslatorUtils::UpdateGrpColMapping
	(
	IMemoryPool *pmp,
	HMUlUl *phmululGrpColPos, 
	CBitSet *pbsGrpCols,
	ULONG ulSortGrpRef
	)
{
	GPOS_ASSERT(NULL != phmululGrpColPos);
	GPOS_ASSERT(NULL != pbsGrpCols);
		
	if (!pbsGrpCols->FBit(ulSortGrpRef))
	{
		ULONG ulUniqueGrpCols = pbsGrpCols->CElements();
#ifdef GPOS_DEBUG
		BOOL fResult = 
#endif
		phmululGrpColPos->FInsert(GPOS_NEW(pmp) ULONG (ulUniqueGrpCols), GPOS_NEW(pmp) ULONG(ulSortGrpRef));
		(void) pbsGrpCols->FExchangeSet(ulSortGrpRef);
	}
}


//---------------------------------------------------------------------------
//      @function:
//              CTranslatorUtils::MarkOuterRefs
//
//      @doc:
//		check if given column ids are outer refs in the tree rooted by 
//		given node
//---------------------------------------------------------------------------
void
CTranslatorUtils::MarkOuterRefs
	(
	ULONG *pulColId,  // array of column ids to be checked
	BOOL *pfOuterRef,  // array of outer ref indicators, initially all set to true by caller 
	ULONG ulColumns,  // number of columns
	CDXLNode *pdxln
	)
{
	GPOS_ASSERT(NULL != pulColId);
	GPOS_ASSERT(NULL != pfOuterRef);
	GPOS_ASSERT(NULL != pdxln);
	
	const CDXLOperator *pdxlop = pdxln->Pdxlop();
	for (ULONG ulCol = 0; ulCol < ulColumns; ulCol++)
	{
		ULONG ulColId = pulColId[ulCol];
		if (pfOuterRef[ulCol] && pdxlop->FDefinesColumn(ulColId))
		{
			// column is defined by operator, reset outer reference flag
			pfOuterRef[ulCol] = false;
		}
	}

	// recursively process children
	const ULONG ulArity = pdxln->UlArity();
	for (ULONG ul = 0; ul < ulArity; ul++)
	{
		MarkOuterRefs(pulColId, pfOuterRef, ulColumns, (*pdxln)[ul]);
	}
}

//---------------------------------------------------------------------------
//      @function:
//              CTranslatorUtils::Slink
//
//      @doc:
//              Map DXL Subplan type to GPDB SubLinkType
//
//---------------------------------------------------------------------------
SubLinkType
CTranslatorUtils::Slink
        (
        EdxlSubPlanType edxlsubplantype
        )
{
        GPOS_ASSERT(EdxlSubPlanTypeSentinel > edxlsubplantype);
        ULONG rgrgulMapping[][2] =
                {
                {EdxlSubPlanTypeScalar, EXPR_SUBLINK},
                {EdxlSubPlanTypeExists, EXISTS_SUBLINK},
                {EdxlSubPlanTypeNotExists, NOT_EXISTS_SUBLINK},
                {EdxlSubPlanTypeAny, ANY_SUBLINK},
                {EdxlSubPlanTypeAll, ALL_SUBLINK}
                };

        const ULONG ulArity = GPOS_ARRAY_SIZE(rgrgulMapping);
        SubLinkType slink = EXPR_SUBLINK;
	BOOL fFound = false;
        for (ULONG ul = 0; ul < ulArity; ul++)
        {
                ULONG *pulElem = rgrgulMapping[ul];
                if ((ULONG) edxlsubplantype == pulElem[0])
                {
                        slink = (SubLinkType) pulElem[1];
                        fFound = true;
			break;
                }
        }

	GPOS_ASSERT(fFound && "Invalid SubPlanType");

        return slink;
}


//---------------------------------------------------------------------------
//      @function:
//              CTranslatorUtils::Edxlsubplantype
//
//      @doc:
//              Map GPDB SubLinkType to DXL subplan type
//
//---------------------------------------------------------------------------
EdxlSubPlanType
CTranslatorUtils::Edxlsubplantype
        (
        SubLinkType slink
        )
{
        ULONG rgrgulMapping[][2] =
                {
                {EXPR_SUBLINK, EdxlSubPlanTypeScalar},
                {EXISTS_SUBLINK , EdxlSubPlanTypeExists},
                {NOT_EXISTS_SUBLINK, EdxlSubPlanTypeNotExists},
                {ANY_SUBLINK, EdxlSubPlanTypeAny},
                {ALL_SUBLINK, EdxlSubPlanTypeAll}
                };

        const ULONG ulArity = GPOS_ARRAY_SIZE(rgrgulMapping);
        EdxlSubPlanType edxlsubplantype = EdxlSubPlanTypeScalar;
	BOOL fFound = false;
        for (ULONG ul = 0; ul < ulArity; ul++)
        {
                ULONG *pulElem = rgrgulMapping[ul];
                if ((ULONG) slink == pulElem[0])
                {
                        edxlsubplantype = (EdxlSubPlanType) pulElem[1];
                        fFound = true;
			break;
                }
        }

	 GPOS_ASSERT(fFound && "Invalid SubLinkType");

        return edxlsubplantype;
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorUtils::FRelHasTriggers
//
//	@doc:
//		Check whether there are triggers for the given operation on
//		the given relation
//
//---------------------------------------------------------------------------
BOOL
CTranslatorUtils::FRelHasTriggers
	(
	IMemoryPool *pmp,
	CMDAccessor *pmda,
	const IMDRelation *pmdrel,
	const EdxlDmlType edxldmltype
	)
{
	const ULONG ulTriggers = pmdrel->UlTriggers();
	for (ULONG ul = 0; ul < ulTriggers; ul++)
	{
		if (FApplicableTrigger(pmda, pmdrel->PmdidTrigger(ul), edxldmltype))
		{
			return true;
		}
	}

	// if table is partitioned, check for triggers on child partitions as well
	INT iType = 0;
	if (Edxldmlinsert == edxldmltype)
	{
		iType = TRIGGER_TYPE_INSERT;
	}
	else if (Edxldmldelete == edxldmltype)
	{
		iType = TRIGGER_TYPE_DELETE;
	}
	else
	{
		GPOS_ASSERT(Edxldmlupdate == edxldmltype);
		iType = TRIGGER_TYPE_UPDATE;
	}

	OID oidRel = CMDIdGPDB::PmdidConvert(pmdrel->Pmdid())->OidObjectId();
	return gpdb::FChildTriggers(oidRel, iType);
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorUtils::FApplicableTrigger
//
//	@doc:
//		Check whether the given trigger is applicable to the given DML operation
//
//---------------------------------------------------------------------------
BOOL
CTranslatorUtils::FApplicableTrigger
	(
	CMDAccessor *pmda,
	IMDId *pmdidTrigger,
	const EdxlDmlType edxldmltype
	)
{
	const IMDTrigger *pmdtrigger = pmda->Pmdtrigger(pmdidTrigger);
	if (!pmdtrigger->FEnabled())
	{
		return false;
	}

	return ((Edxldmlinsert == edxldmltype && pmdtrigger->FInsert()) ||
			(Edxldmldelete == edxldmltype && pmdtrigger->FDelete()) ||
			(Edxldmlupdate == edxldmltype && pmdtrigger->FUpdate()));
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorUtils::FRelHasConstraints
//
//	@doc:
//		Check whether there are constraints for the given relation
//
//---------------------------------------------------------------------------
BOOL
CTranslatorUtils::FRelHasConstraints
	(
	const IMDRelation *pmdrel
	)
{
	if (0 < pmdrel->UlCheckConstraints())
	{
		return true;
	}
	
	const ULONG ulCols = pmdrel->UlColumns();
	
	for (ULONG ul = 0; ul < ulCols; ul++)
	{
		const IMDColumn *pmdcol = pmdrel->Pmdcol(ul);
		if (!pmdcol->FSystemColumn() && !pmdcol->FNullable())
		{
			return true;
		}
	}
	
	return false;
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorUtils::PlAssertErrorMsgs
//
//	@doc:
//		Construct a list of error messages from a list of assert constraints 
//
//---------------------------------------------------------------------------
List *
CTranslatorUtils::PlAssertErrorMsgs
	(
	CDXLNode *pdxlnAssertConstraintList
	)
{
	GPOS_ASSERT(NULL != pdxlnAssertConstraintList);
	GPOS_ASSERT(EdxlopScalarAssertConstraintList == pdxlnAssertConstraintList->Pdxlop()->Edxlop());
	
	List *plErrorMsgs = NIL;
	const ULONG ulConstraints = pdxlnAssertConstraintList->UlArity();
	
	for (ULONG ul = 0; ul < ulConstraints; ul++)
	{
		CDXLNode *pdxlnConstraint = (*pdxlnAssertConstraintList)[ul];
		CDXLScalarAssertConstraint *pdxlopConstraint = CDXLScalarAssertConstraint::PdxlopConvert(pdxlnConstraint->Pdxlop());
		CWStringBase *pstrErrorMsg = pdxlopConstraint->PstrErrorMsg();
		plErrorMsgs = gpdb::PlAppendElement(plErrorMsgs, gpdb::PvalMakeString(SzFromWsz(pstrErrorMsg->Wsz())));
	}
	
	return plErrorMsgs;
}

// EOF
