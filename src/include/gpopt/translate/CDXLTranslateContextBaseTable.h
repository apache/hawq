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
//		CDXLTranslateContextBaseTable.h
//
//	@doc:
//		Class providing access to translation context for translating base table
//		column references, such as mappings from a DXL ColId to a GPDB attribute number in
//		the base table schema
//
//	@test:
//
//
//---------------------------------------------------------------------------

#ifndef GPDXL_CDXLTranslateContextBaseTable_H
#define GPDXL_CDXLTranslateContextBaseTable_H


#include "gpos/base.h"
#include "gpos/common/CHashMap.h"

#include "postgres.h"	// Index

#include "naucrates/dxl/gpdb_types.h"

namespace gpdxl
{

	using namespace gpos;


	//---------------------------------------------------------------------------
	//	@class:
	//		CDXLTranslateContextBaseTable
	//
	//	@doc:
	//		Class providing access to translation context, such as mappings between
	//		ColIds and target entries
	//
	//---------------------------------------------------------------------------
	class CDXLTranslateContextBaseTable
	{
		// hash maps mapping ULONG -> INT
		typedef CHashMap<ULONG, INT, gpos::UlHash<ULONG>, gpos::FEqual<ULONG>,
			CleanupDelete<ULONG>, CleanupDelete<INT> > HMUlI;


		private:
			IMemoryPool *m_pmp;

			// oid of the base table
			OID m_oid;

			// index of the relation in the rtable
			Index m_iRel;

			// maps a colid of a column to the attribute number of that column in the schema of the underlying relation
			HMUlI *m_phmuli;

			// private copy ctor
			CDXLTranslateContextBaseTable(const CDXLTranslateContextBaseTable&);

		public:
			// ctor/dtor
			explicit CDXLTranslateContextBaseTable(IMemoryPool *pmp);


			~CDXLTranslateContextBaseTable();

			// accessors
			OID OidRel() const;

			Index IRel() const;

			// return the index of the column in the base relation for the given DXL ColId
			INT IAttnoForColId(ULONG ulDXLColId) const;

			// setters
			void SetOID(OID oid);

			void SetIdx(Index iRel);

			// store the mapping of the given DXL column id and index in the base relation schema
			BOOL FInsertMapping(ULONG ulDXLColId, INT iAttno);

	};
}

#endif // !GPDXL_CDXLTranslateContextBaseTable_H

// EOF
