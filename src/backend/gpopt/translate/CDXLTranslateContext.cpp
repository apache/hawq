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
//		CDXLTranslateContext.cpp
//
//	@doc:
//		Implementation of the methods for accessing translation context
//
//	@test:
//
//
//---------------------------------------------------------------------------

#include "gpopt/translate/CDXLTranslateContext.h"

using namespace gpdxl;
using namespace gpos;

//---------------------------------------------------------------------------
//	@function:
//		CDXLTranslateContext::CDXLTranslateContext
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CDXLTranslateContext::CDXLTranslateContext
	(
	IMemoryPool *pmp,
	BOOL fChildAggNode
	)
	:
	m_pmp(pmp),
	m_fChildAggNode(fChildAggNode)
{
	// initialize hash table
	m_phmulte = GPOS_NEW(m_pmp) HMUlTe(m_pmp);
	m_phmcolparam = GPOS_NEW(m_pmp) HMColParam(m_pmp);
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLTranslateContext::CDXLTranslateContext
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CDXLTranslateContext::CDXLTranslateContext
	(
	IMemoryPool *pmp,
	BOOL fChildAggNode,
	HMColParam *phmOriginal
	)
	:
	m_pmp(pmp),
	m_fChildAggNode(fChildAggNode)
{
	m_phmulte = GPOS_NEW(m_pmp) HMUlTe(m_pmp);
	m_phmcolparam = GPOS_NEW(m_pmp) HMColParam(m_pmp);
	CopyParamHashmap(phmOriginal);
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLTranslateContext::~CDXLTranslateContext
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CDXLTranslateContext::~CDXLTranslateContext()
{
	m_phmulte->Release();
	m_phmcolparam->Release();
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLTranslateContext::FParentAggNode
//
//	@doc:
//		Is this translation context created by a parent Agg node
//
//---------------------------------------------------------------------------
BOOL
CDXLTranslateContext::FParentAggNode() const
{
	return m_fChildAggNode;
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLTranslateContext::CopyParamHashmap
//
//	@doc:
//		copy the params hashmap
//
//---------------------------------------------------------------------------
void
CDXLTranslateContext::CopyParamHashmap
	(
	HMColParam *phmOriginal
	)
{
	// iterate over full map
	HMColParamIter hashmapiter(phmOriginal);
	while (hashmapiter.FAdvance())
	{
		CMappingElementColIdParamId *pmecolidparamid = const_cast<CMappingElementColIdParamId *>(hashmapiter.Pt());

		const ULONG ulColId = pmecolidparamid->UlColId();
		ULONG *pulKey = GPOS_NEW(m_pmp) ULONG(ulColId);
		pmecolidparamid->AddRef();
		m_phmcolparam->FInsert(pulKey, pmecolidparamid);
	}
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLTranslateContext::Pte
//
//	@doc:
//		Lookup target entry associated with a given col id
//
//---------------------------------------------------------------------------
const TargetEntry *
CDXLTranslateContext::Pte
	(
	ULONG ulColId
	)
	const
{
	return m_phmulte->PtLookup(&ulColId);
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLTranslateContext::Pmecolidparamid
//
//	@doc:
//		Lookup col->param mapping associated with a given col id
//
//---------------------------------------------------------------------------
const CMappingElementColIdParamId *
CDXLTranslateContext::Pmecolidparamid
	(
	ULONG ulColId
	)
	const
{
	return m_phmcolparam->PtLookup(&ulColId);
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLTranslateContext::InsertMapping
//
//	@doc:
//		Insert a (col id, target entry) mapping
//
//---------------------------------------------------------------------------
void
CDXLTranslateContext::InsertMapping
	(
	ULONG ulColId,
	TargetEntry *pte
	)
{
	// copy key
	ULONG *pulKey = GPOS_NEW(m_pmp) ULONG(ulColId);

	// insert colid->target entry mapping in the hash map
	BOOL fResult = m_phmulte->FInsert(pulKey, pte);

	if (!fResult)
	{
		GPOS_DELETE(pulKey);
	}
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLTranslateContext::FInsertParamMapping
//
//	@doc:
//		Insert a (col id, param id) mapping
//
//---------------------------------------------------------------------------
BOOL
CDXLTranslateContext::FInsertParamMapping
	(
	ULONG ulColId,
	CMappingElementColIdParamId *pmecolidparamid
	)
{
	// copy key
	ULONG *pulKey = GPOS_NEW(m_pmp) ULONG(ulColId);

	// insert colid->target entry mapping in the hash map
	return m_phmcolparam->FInsert(pulKey, pmecolidparamid);
}

// EOF
