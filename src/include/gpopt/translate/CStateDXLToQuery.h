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
//		CStateDXLToQuery.h
//
//	@doc:
//		Class providing access to list of output column target entries and
//		the corresponding list of column names.
//
//	@test:
//
//
//---------------------------------------------------------------------------

#ifndef GPDXL_CStateDXLToQuery_H
#define GPDXL_CStateDXLToQuery_H

#include "gpos/base.h"
#include "naucrates/dxl/gpdb_types.h"
#include "naucrates/dxl/operators/CDXLNode.h"

// fwd decl
struct TargetEntry;
struct List;

namespace gpdxl
{

	//---------------------------------------------------------------------------
	//	@class:
	//		CStateDXLToQuery
	//
	//	@doc:
	//		Class providing access to list of output column TargetEntry and
	//		the corresponding list of column names.
	//
	//---------------------------------------------------------------------------
	class CStateDXLToQuery
	{
		private:
			// memory pool
			IMemoryPool *m_pmp;

			// list of target list entries for the output columns
			List *m_plTEColumns;

			// list of output column names
			List *m_plColumnNames;

			// list of column ids
			DrgPul *m_pdrgpulColIds;

		public:

			CStateDXLToQuery(IMemoryPool *pmp);

			~CStateDXLToQuery();

			void AddOutputColumnEntry(TargetEntry *pte, CHAR *szColname, ULONG ul);

			// return the target entry at the position ulIndex
			TargetEntry *PteColumn(ULONG ulIndex);

			// target entry found in the state
			BOOL FTEFound(TargetEntry *pte);

			// return the column at the position ulIndex
			CHAR* SzColumnName(ULONG ulIndex);

			// return the column id at the position ulIndex
			ULONG UlColId(ULONG ulIndex);

			// number of entries in the state
			ULONG UlLength();

			// clear all entries
			void Clear();

			// clear all entries and load content from another state
			void Reload(CStateDXLToQuery *pstatedxltoquery);
	};
}
#endif // GPDXL_CStateDXLToQuery_H

// EOF
