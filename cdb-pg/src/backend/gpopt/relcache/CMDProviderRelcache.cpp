//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 EMC Corp.
//
//	@filename:
//		CMDProviderRelcache.cpp
//
//	@doc:
//		Implementation of a relcache-based metadata provider, which uses GPDB's
//		relcache to lookup objects given their ids.
//
//	@owner:
//		antovl
//
//	@test:
//
//
//---------------------------------------------------------------------------

#include "postgres.h"
#include "gpopt/relcache/CMDProviderRelcache.h"
#include "gpopt/translate/CTranslatorRelcacheToDXL.h"
#include "gpopt/mdcache/CMDAccessor.h"

#include "gpos/io/COstreamString.h"

#include "md/CMDTypeInt4GPDB.h"
#include "md/CMDTypeInt8GPDB.h"
#include "md/CMDTypeBoolGPDB.h"
#include "md/CMDTypeOidGPDB.h"

#include "dxl/CDXLUtils.h"

#include "exception.h"

using namespace gpos;
using namespace gpdxl;
using namespace gpmd;

//---------------------------------------------------------------------------
//	@function:
//		CMDProviderRelcache::CMDProviderRelcache
//
//	@doc:
//		Constructs a file-based metadata provider
//
//---------------------------------------------------------------------------
CMDProviderRelcache::CMDProviderRelcache
	(
	IMemoryPool *pmp
	)
	:
	m_pmp(pmp)
{
	GPOS_ASSERT(NULL != m_pmp);
}

//---------------------------------------------------------------------------
//	@function:
//		CMDProviderRelcache::PstrObject
//
//	@doc:
//		Returns the DXL of the requested object in the provided memory pool
//
//---------------------------------------------------------------------------
CWStringBase *
CMDProviderRelcache::PstrObject
	(
	IMemoryPool *pmp,
	CMDAccessor *pmda,
	IMDId *pmdid
	)
	const
{
	IMDCacheObject *pimdobj = CTranslatorRelcacheToDXL::Pimdobj(pmp, pmda, pmdid);

	GPOS_ASSERT(NULL != pimdobj);

	CWStringDynamic *pstr = CDXLUtils::PstrSerializeMDObj(m_pmp, pimdobj, true /*fSerializeHeaders*/, false /*findent*/);

	// cleanup DXL object
	pimdobj->Release();

	return pstr;
}

//---------------------------------------------------------------------------
//	@function:
//		CMDProviderRelcache::PmdidFunc
//
//	@doc:
//		Returns the mdid for the requested type
//
//---------------------------------------------------------------------------
IMDId *
CMDProviderRelcache::PmdidFunc
	(
	IMemoryPool *pmp,
	CSystemId , // sysid
	IMDFunction::EFuncType eft
	)
	const
{
	// find id of requested function by the function name
	const WCHAR *wszFuncName = IMDFunction::WszFuncName(eft);

	return CTranslatorRelcacheToDXL::PmdidFunc(pmp, wszFuncName);
}

// EOF
