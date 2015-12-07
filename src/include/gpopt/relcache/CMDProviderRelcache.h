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
//		CMDProviderRelcache.h
//
//	@doc:
//		Relcache-based provider of metadata objects.
//
//	@test:
//
//
//---------------------------------------------------------------------------



#ifndef GPMD_CMDProviderRelcache_H
#define GPMD_CMDProviderRelcache_H

#include "gpos/base.h"
#include "gpos/string/CWStringBase.h"

#include "naucrates/md/CSystemId.h"
#include "naucrates/md/IMDId.h"
#include "naucrates/md/IMDCacheObject.h"
#include "naucrates/md/IMDProvider.h"

// fwd decl
namespace gpopt
{
	class CMDAccessor;
}

namespace gpmd
{
	using namespace gpos;

	//---------------------------------------------------------------------------
	//	@class:
	//		CMDProviderRelcache
	//
	//	@doc:
	//		Relcache-based provider of metadata objects.
	//
	//---------------------------------------------------------------------------
	class CMDProviderRelcache : public IMDProvider
	{
		private:
			// memory pool
			IMemoryPool *m_pmp;

			// private copy ctor
			CMDProviderRelcache(const CMDProviderRelcache&);

		public:
			// ctor/dtor
			explicit
			CMDProviderRelcache(IMemoryPool *pmp);

			~CMDProviderRelcache()
			{
			}

			// returns the DXL string of the requested metadata object
			virtual
			CWStringBase *PstrObject(IMemoryPool *pmp, CMDAccessor *pmda, IMDId *pmdid) const;

			// return the mdid for the requested type
			virtual
			IMDId *Pmdid
				(
				IMemoryPool *pmp,
				CSystemId sysid,
				IMDType::ETypeInfo eti
				)
				const
			{
				return PmdidTypeGPDB(pmp, sysid, eti);
			}

	};
}



#endif // !GPMD_CMDProviderRelcache_H

// EOF
