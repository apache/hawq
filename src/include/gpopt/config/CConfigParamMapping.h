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
//		CConfigParamMapping.h
//
//	@doc:
//		Mapping of GPDB config params to traceflags
//
//	@test:
//
//
//---------------------------------------------------------------------------
#ifndef GPOPT_CGUCMapping_H
#define GPOPT_CGUCMapping_H

#include "gpos/base.h"
#include "gpos/common/CBitSet.h"
#include "gpos/memory/IMemoryPool.h"

#include "naucrates/traceflags/traceflags.h"

using namespace gpos;

namespace gpdxl
{
	//---------------------------------------------------------------------------
	//	@class:
	//		CConfigParamMapping
	//
	//	@doc:
	//		Functionality for mapping GPDB config params to traceflags
	//
	//---------------------------------------------------------------------------
	class CConfigParamMapping
	{
		private:
			//------------------------------------------------------------------
			//	@class:
			//		SConfigMappingElem
			//
			//	@doc:
			//		Unit describing the mapping of a single GPDB config param
			//		to a trace flag
			//
			//------------------------------------------------------------------
			struct SConfigMappingElem
			{
				// trace flag
				EOptTraceFlag m_etf;

				// config param address
				BOOL *m_pfParam;

				// if true, we negate the config param value before setting traceflag value
				BOOL m_fNegate;

				// description
				const WCHAR *wszDescription;
			};

			// array of mapping elements
			static SConfigMappingElem m_elem[];

			// private ctor
			CConfigParamMapping(const CConfigParamMapping &);

		public:
			// pack enabled optimizer config params in a traceflag bitset
			static
			CBitSet *PbsPack(IMemoryPool *pmp, ULONG ulXforms);
	};
}

#endif // ! GPOPT_CGUCMapping_H

// EOF

