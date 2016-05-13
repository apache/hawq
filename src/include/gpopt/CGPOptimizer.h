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
//		CGPOptimizer.h
//
//	@doc:
//		Entry point to GP optimizer
//
//	@test:
//
//
//---------------------------------------------------------------------------
#ifndef CGPOptimizer_H
#define CGPOptimizer_H

#include "postgres.h"
#include "nodes/params.h"
#include "nodes/plannodes.h"
#include "nodes/parsenodes.h"

class CGPOptimizer
{
	private:

		// touch library initializers to enforce linker to include them
		static
		void TouchLibraryInitializers();

	public:

		// optimize given query using GP optimizer
		static
		PlannedStmt *PplstmtOptimize
			(
			Query *pquery,
			bool *pfUnexpectedFailure // output : set to true if optimizer unexpectedly failed to produce plan
			);

		// serialize planned statement into DXL
		static
		char *SzDXLPlan(Query *pquery);

		// gpopt initialize and terminate
		static
		void InitGPOPT();

		static
		void TerminateGPOPT();
};

#endif // CGPOptimizer_H
