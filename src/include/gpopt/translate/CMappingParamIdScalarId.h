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
//		CMappingParamIdScalarId.h
//
//	@doc:
//		ParamId to ScalarId mapping during PlStmt->DXL translation
//
//	@test:
//
//
//---------------------------------------------------------------------------

#ifndef GPDXL_CMappingParamIdScalarId_H
#define GPDXL_CMappingParamIdScalarId_H

#include "gpos/base.h"
#include "gpos/common/CHashMap.h"
#include "nodes/primnodes.h"
#include "naucrates/dxl/operators/CDXLScalarIdent.h"

namespace gpdxl
{

	using namespace gpos;

	// hash maps mapping ULONG -> ScalarId
	typedef CHashMap<ULONG, CDXLScalarIdent, gpos::UlHash<ULONG>, gpos::FEqual<ULONG>,
			CleanupDelete<ULONG>, CleanupRelease<CDXLScalarIdent> > HMParamScalar;

	//---------------------------------------------------------------------------
	//	@class:
	//		CMappingParamIdScalarId
	//
	//	@doc:
	//		ParamId to ScalarId mapping during scalar operator translation during
	//		PlStmt->DXL translation
	//
	//---------------------------------------------------------------------------
	class CMappingParamIdScalarId
	{
		private:
			// memory pool
			IMemoryPool *m_pmp;

			// map from param id to scalar id
			HMParamScalar *m_phmps;

			// private copy constructor
			CMappingParamIdScalarId(const CMappingParamIdScalarId &);

		public:

			// ctor
			explicit
			CMappingParamIdScalarId(IMemoryPool *);

			// dtor
			virtual
			~CMappingParamIdScalarId()
			{
				m_phmps->Release();
			}

			// return the scalarident corresponding to the given ParamId
			CDXLScalarIdent *Pscid(ULONG ulParmId) const;

			// store the mapping of the given param id and scalar id
			BOOL FInsertMapping(ULONG ulParamId, CDXLScalarIdent *pstrid);
	};
}

#endif //GPDXL_CMappingParamIdScalarId_H

// EOF
