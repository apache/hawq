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
//		CMappingColIdVarQuery.h
//
//	@doc:
//		Class providing functions that provide the mapping between Var, Param
//		and variables of Sub-query to CDXLNode during Query->DXL translation
//
//	@test:
//
//
//---------------------------------------------------------------------------
#ifndef GPDXL_CMappingColIdVarQuery_H
#define GPDXL_CMappingColIdVarQuery_H

#include "gpopt/translate/CMappingColIdVar.h"
#include "gpopt/translate/CMappingElementColIdTE.h"
#include "gpopt/translate/CDXLTranslateContextBaseTable.h"
#include "gpopt/translate/CDXLTranslateContext.h"


#include "gpos/base.h"
#include "gpos/common/CHashMap.h"
#include "gpos/common/CHashMapIter.h"


// fwd decl
struct List;
struct Var;
struct Query;
struct Plan;

namespace gpdxl
{
	using namespace gpos;

	// Hash map that stores the mapping between Column Id -> TE
	typedef CHashMap<ULONG, CMappingElementColIdTE, gpos::UlHash<ULONG>, gpos::FEqual<ULONG>,
					CleanupDelete<ULONG>, CleanupRelease<CMappingElementColIdTE> > TEMap;

	typedef CHashMapIter<ULONG, CMappingElementColIdTE, gpos::UlHash<ULONG>, gpos::FEqual<ULONG>,
					CleanupDelete<ULONG>, CleanupRelease<CMappingElementColIdTE> > TEMapIter;

	//---------------------------------------------------------------------------
	//	@class:
	//		CMappingColIdVarQuery
	//
	//	@doc:
	//		Class defining functions that define the mappings between ColID and Var
	//
	//---------------------------------------------------------------------------
	class CMappingColIdVarQuery : public CMappingColIdVar
	{
		private:

			// mappings ColId->CMappingElementColIdTE containing the TargetEntry used for intermediate DXL nodes
			TEMap *m_ptemap;

			// Depth of the subquery; The topmost query is at level 1.
			ULONG m_ulQueryLevel;


		public:

			CMappingColIdVarQuery(IMemoryPool *pmp, TEMap *ptemap, ULONG ulQueryLevel);

			// translate DXL ScalarIdent node into GPDB Var node
			Var *PvarFromDXLNodeScId
					(
					const CDXLScalarIdent *pdxlop
					);

			// store the mapping of the given column id and target entry
			BOOL FInsertMapping
					(
					ULONG ulColId,
					TargetEntry *pte
					);

			// return the target entry corresponding to the given ColId
			const TargetEntry *Pte(ULONG) const;

			// accessors
			ULONG UlQueryLevel() const;

			TEMap *Ptemap()
			{
				return m_ptemap;
			}
	};
}

#endif // GPDXL_CMappingColIdVarQuery_H

// EOF
