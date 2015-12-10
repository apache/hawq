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
//		COptColInfo.h
//
//	@doc:
//		Class to uniquely identify a column in optimizer
//
//	@test:
//
//
//---------------------------------------------------------------------------

#ifndef GPDXL_COptColInfo_H
#define GPDXL_COptColInfo_H

#include "gpos/base.h"
#include "gpos/common/CRefCount.h"
#include "gpos/string/CWStringConst.h"
#include "gpos/utils.h"

namespace gpdxl
{
	using namespace gpos;

	//---------------------------------------------------------------------------
	//	@class:
	//		COptColInfo
	//
	//	@doc:
	//		pair of column id and column name
	//
	//---------------------------------------------------------------------------
	class COptColInfo: public CRefCount
	{
		private:

			// column id
			ULONG m_ulColId;

			// column name
			CWStringBase *m_pstr;

			// private copy c'tor
			COptColInfo(const COptColInfo&);

		public:
			// ctor
			COptColInfo(ULONG ulColId, CWStringBase *pstr)
				: m_ulColId(ulColId), m_pstr(pstr)
			{
				GPOS_ASSERT(m_pstr);
			}

			// dtor
			virtual
			~COptColInfo()
			{
				GPOS_DELETE(m_pstr);
			}

			// accessors
			ULONG UlColId() const
			{
				return m_ulColId;
			}

			CWStringBase* PstrColName() const
			{
				return m_pstr;
			}

			// equality check
			BOOL FEquals(const COptColInfo& optcolinfo) const
			{
				// don't need to check name as column id is unique
				return m_ulColId == optcolinfo.m_ulColId;
			}

			// hash value
			ULONG UlHash() const
			{
				return gpos::UlHash(&m_ulColId);
			}

	};

	// hash function
	inline
	ULONG UlHashOptColInfo
		(
		const COptColInfo *poptcolinfo
		)
	{
		GPOS_ASSERT(NULL != poptcolinfo);
		return poptcolinfo->UlHash();
	}

	// equality function
	inline
	BOOL FEqualOptColInfo
		(
		const COptColInfo *poptcolinfoA,
		const COptColInfo *poptcolinfoB
		)
	{
		GPOS_ASSERT(NULL != poptcolinfoA && NULL != poptcolinfoB);
		return poptcolinfoA->FEquals(*poptcolinfoB);
	}

}

#endif // !GPDXL_COptColInfo_H

// EOF
