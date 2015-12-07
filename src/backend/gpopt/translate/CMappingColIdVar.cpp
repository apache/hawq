//---------------------------------------------------------------------------
//	@filename:
//		CMappingColIdVar.cpp
//
//	@doc:
//		Implementation of base ColId-> Var mapping class
//
//	@test:
//
//
//---------------------------------------------------------------------------

#include "gpopt/translate/CMappingColIdVar.h"

using namespace gpdxl;

//---------------------------------------------------------------------------
//	@function:
//		CMappingColIdVar::CMappingColIdVar
//
//	@doc:
//		Constructor
//
//---------------------------------------------------------------------------
CMappingColIdVar::CMappingColIdVar
	(
	IMemoryPool *pmp
	)
	:
	m_pmp(pmp)
{
}

// EOF
