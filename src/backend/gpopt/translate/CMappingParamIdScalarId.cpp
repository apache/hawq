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
//		CMappingParamIdScalarId.cpp
//
//	@doc:
//		Implementation of param id -> scalar id mapping class
//
//	@test:
//
//
//---------------------------------------------------------------------------
#include "postgres.h"
#include "gpopt/translate/CMappingParamIdScalarId.h"
using namespace gpdxl;

//---------------------------------------------------------------------------
//	@function:
//		CMappingParamIdScalarId::CMappingParamIdScalarId
//
//	@doc:
//		Constructor
//
//---------------------------------------------------------------------------
CMappingParamIdScalarId::CMappingParamIdScalarId
	(
	IMemoryPool *pmp
	)
	:
	m_pmp(pmp)
{
	m_phmps = GPOS_NEW(m_pmp) HMParamScalar(m_pmp);
}

//---------------------------------------------------------------------------
//	@function:
//		CMappingParamIdScalarId::Pscid
//
//	@doc:
//		Lookup scalar ident associated with a given param id
//
//---------------------------------------------------------------------------
CDXLScalarIdent *
CMappingParamIdScalarId::Pscid
	(
	ULONG ulParamId
	)
	const
{
	return m_phmps->PtLookup(&ulParamId);
}

//---------------------------------------------------------------------------
//	@function:
//		CMappingParamIdScalarId::FInsertMapping
//
//	@doc:
//		Insert a (col id, scalarident) mapping
//
//---------------------------------------------------------------------------
BOOL
CMappingParamIdScalarId::FInsertMapping
	(
	ULONG ulParamId,
	CDXLScalarIdent *pscid
	)
{
	// copy key
	ULONG *pulKey = GPOS_NEW(m_pmp) ULONG(ulParamId);

	// insert mapping in the hash map
	return m_phmps->FInsert(pulKey, pscid);
}

// EOF
