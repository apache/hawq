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
//		CMappingColIdVarQuery.cpp
//
//	@doc:
//		Implementation of the functions that provide the mapping from CDXLNode to
//		Var during DXL->Query translation
//
//	@test:
//
//
//---------------------------------------------------------------------------

#include "postgres.h"
#include "gpopt/translate/CMappingColIdVarQuery.h"

#include "nodes/makefuncs.h"
#include "nodes/primnodes.h"

#include "naucrates/dxl/operators/CDXLScalarIdent.h"

#include "gpopt/gpdbwrappers.h"

using namespace gpdxl;
using namespace gpos;

//---------------------------------------------------------------------------
//	@function:
//		CMappingColIdVarQuery::CMappingColIdVarQuery
//
//	@doc:
//		Constructor for a query translator context associated to a sub query
//		at a certain depth (level) in the GPDB query object
//
//---------------------------------------------------------------------------
CMappingColIdVarQuery::CMappingColIdVarQuery
	(
	IMemoryPool *pmp,
	TEMap *ptemap,
	ULONG ulQueryLevel
	)
	:
	CMappingColIdVar(pmp),
	m_ptemap(ptemap),
	m_ulQueryLevel(ulQueryLevel)
{
}

//---------------------------------------------------------------------------
//	@function:
//		CMappingColIdVarQuery::Pte
//
//	@doc:
//		Lookup TargetEntry associated with a given col id
//
//---------------------------------------------------------------------------
const TargetEntry *
CMappingColIdVarQuery::Pte
	(
	ULONG ulColId
	)
	const
{
	const CMappingElementColIdTE *pmappingelement = m_ptemap->PtLookup(&ulColId);
	GPOS_ASSERT(NULL != pmappingelement);
	return pmappingelement->Pte();
}

//---------------------------------------------------------------------------
//	@function:
//		CMappingColIdVarQuery::FInsertMapping
//
//	@doc:
//		Insert
//
//---------------------------------------------------------------------------
BOOL
CMappingColIdVarQuery::FInsertMapping
	(
	ULONG ulColId,
	TargetEntry *pte
	)
{
	// Assert that there are no duplicate entries for a column id
	GPOS_ASSERT(NULL == m_ptemap->PtLookup(&ulColId));

	// create mapping element
	CMappingElementColIdTE *pmappingelement = GPOS_NEW(m_pmp) CMappingElementColIdTE(ulColId, m_ulQueryLevel, pte);

	// insert ColId->TE mapping
	ULONG *pulKey1 = GPOS_NEW(m_pmp) ULONG(ulColId);
	BOOL fRes1 = m_ptemap->FInsert(pulKey1, pmappingelement);
	GPOS_ASSERT(fRes1);

	return fRes1;
}

//---------------------------------------------------------------------------
//	@function:
//		CMappingColIdVarQuery::UlQueryLevel
//
//	@doc:
//		Returns the query level counter
//
//---------------------------------------------------------------------------
ULONG
CMappingColIdVarQuery::UlQueryLevel() const
{
	return m_ulQueryLevel;
}

//---------------------------------------------------------------------------
//	@function:
//		CMappingColIdVarQuery::PvarFromDXLNodeScId
//
//	@doc:
//		Translates a DXL scalar identifier operator into a GPDB Var node
//
//---------------------------------------------------------------------------
Var *
CMappingColIdVarQuery::PvarFromDXLNodeScId
	(
	const CDXLScalarIdent *pdxlop
	)
{
	ULONG ulColId = pdxlop->Pdxlcr()->UlID();

	const CMappingElementColIdTE *pmappingelement = m_ptemap->PtLookup(&ulColId);
	GPOS_ASSERT(NULL != pmappingelement);
	const TargetEntry *pte = pmappingelement->Pte();

	GPOS_ASSERT(NULL != pte);
	GPOS_ASSERT(IsA(pte->expr, Var));

	Var *pvar = ((Var*) pte->expr);
	Var *pvarNew = (Var*) gpdb::PvCopyObject(pvar);

	// lookup query level
	const ULONG ulLevel = pmappingelement->UlQueryLevel();

	pvarNew->varlevelsup = m_ulQueryLevel - ulLevel;

	return pvarNew;
}

// EOF
