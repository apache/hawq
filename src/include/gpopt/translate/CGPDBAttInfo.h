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
//		CGPDBAttInfo.h
//
//	@doc:
//		Class to uniquely identify a column in GPDB
//
//	@test:
//
//
//---------------------------------------------------------------------------

#ifndef GPDXL_CGPDBAttInfo_H
#define GPDXL_CGPDBAttInfo_H

#include "gpos/base.h"
#include "gpos/common/CRefCount.h"
#include "gpos/utils.h"
#include "naucrates/dxl/gpdb_types.h"

namespace gpdxl
{
	using namespace gpos;

	//---------------------------------------------------------------------------
	//	@class:
	//		CGPDBAttInfo
	//
	//	@doc:
	//		Class to uniquely identify a column in GPDB
	//
	//---------------------------------------------------------------------------
	class CGPDBAttInfo: public CRefCount
	{
		private:

			// query level number
			ULONG m_ulQueryLevel;

			// varno in the rtable
			ULONG m_ulVarNo;

			// attno
			INT	m_iAttNo;

			// copy c'tor
			CGPDBAttInfo(const CGPDBAttInfo&);

		public:
			// ctor
			CGPDBAttInfo(ULONG ulQueryLevel, ULONG ulVarNo, INT iAttNo)
				: m_ulQueryLevel(ulQueryLevel), m_ulVarNo(ulVarNo), m_iAttNo(iAttNo)
			{}

			// d'tor
			virtual
			~CGPDBAttInfo() {}

			// accessor
			ULONG UlQueryLevel() const
			{
				return m_ulQueryLevel;
			}

			// accessor
			ULONG UlVarNo() const
			{
				return m_ulVarNo;
			}

			// accessor
			INT IAttNo() const
			{
				return m_iAttNo;
			}

			// equality check
			BOOL FEquals(const CGPDBAttInfo& gpdbattinfo) const
			{
				return m_ulQueryLevel == gpdbattinfo.m_ulQueryLevel
						&& m_ulVarNo == gpdbattinfo.m_ulVarNo
						&& m_iAttNo == gpdbattinfo.m_iAttNo;
			}

			// hash value
			ULONG UlHash() const
			{
				return gpos::UlCombineHashes(
						gpos::UlHash(&m_ulQueryLevel),
						gpos::UlCombineHashes(gpos::UlHash(&m_ulVarNo),
								gpos::UlHash(&m_iAttNo)));
			}
	};

	// hash function
	inline ULONG UlHashGPDBAttInfo
		(
		const CGPDBAttInfo *pgpdbattinfo
		)
	{
		GPOS_ASSERT(NULL != pgpdbattinfo);
		return pgpdbattinfo->UlHash();
	}

	// equality function
	inline BOOL FEqualGPDBAttInfo
		(
		const CGPDBAttInfo *pgpdbattinfoA,
		const CGPDBAttInfo *pgpdbattinfoB
		)
	{
		GPOS_ASSERT(NULL != pgpdbattinfoA && NULL != pgpdbattinfoB);
		return pgpdbattinfoA->FEquals(*pgpdbattinfoB);
	}

}

#endif // !GPDXL_CGPDBAttInfo_H

// EOF
