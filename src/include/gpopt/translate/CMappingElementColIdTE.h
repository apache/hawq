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
//		CMappingElementColIdTE.h
//
//	@doc:
//		Wrapper class providing functions for the mapping element (between ColId and TE)
//		during DXL->Query translation
//
//	@test:
//
//
//---------------------------------------------------------------------------
#ifndef GPDXL_CMappingElementColIdTE_H
#define GPDXL_CMappingElementColIdTE_H

#include "postgres.h"

#include "gpos/base.h"

#include "naucrates/dxl/operators/CDXLNode.h"
#include "nodes/primnodes.h"

// fwd decl
struct TargetEntry;

namespace gpdxl
{
	using namespace gpos;

	//---------------------------------------------------------------------------
	//	@class:
	//		CMappingElementColIdTE
	//
	//	@doc:
	//		Wrapper class providing functions for the mapping element (between ColId and TE)
	//		during DXL->Query translation
	//
	//---------------------------------------------------------------------------
	class CMappingElementColIdTE : public CRefCount
	{
		private:

			// the column identifier that is used as the key
			ULONG m_ulColId;

			// the query level
			ULONG m_ulQueryLevel;

			// the target entry
			TargetEntry *m_pte;

		public:

			// ctors and dtor
			CMappingElementColIdTE(ULONG, ULONG, TargetEntry *);

			// return the ColId
			ULONG UlColId() const
			{
				return m_ulColId;
			}

			// return the query level
			ULONG UlQueryLevel() const
			{
				return m_ulQueryLevel;
			}

			// return the column name for the given attribute no
			const TargetEntry *Pte() const
			{
				return m_pte;
			}
	};
}

#endif // GPDXL_CMappingElementColIdTE_H

// EOF
