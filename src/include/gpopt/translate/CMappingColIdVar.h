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
//		CMappingColIdVar.h
//
//	@doc:
//		Abstract base class of ColId to VAR mapping when translating CDXLScalars
//		If we need a CDXLScalar translator during DXL->PlStmt or DXL->Query translation
//		we implement a colid->Var mapping for PlStmt or Query respectively that
//		is derived from this interface.
//
//	@test:
//
//
//---------------------------------------------------------------------------

#ifndef GPDXL_CMappingColIdVar_H
#define GPDXL_CMappingColIdVar_H

#include "gpos/base.h"

//fwd decl
struct Var;
struct Param;
struct PlannedStmt;
struct Query;

namespace gpdxl
{
	using namespace gpos;

	// fwd decl
	class CDXLScalarIdent;

	//---------------------------------------------------------------------------
	//	@class:
	//		CMappingColIdVar
	//
	//	@doc:
	//		Class providing the interface for ColId in CDXLScalarIdent to Var
	//		mapping when translating CDXLScalar nodes
	//
	//---------------------------------------------------------------------------
	class CMappingColIdVar
	{
		protected:
			// memory pool
			IMemoryPool *m_pmp;

		public:

			// ctor/dtor
			explicit
			CMappingColIdVar(IMemoryPool *);

			virtual
			~CMappingColIdVar(){}

			// translate DXL ScalarIdent node into GPDB Var node
			virtual
			Var *PvarFromDXLNodeScId(const CDXLScalarIdent *) = 0;
	};
}

#endif //GPDXL_CMappingColIdVar_H

// EOF
