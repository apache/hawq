//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CMappingElementColIdParamId.h
//
//	@doc:
//		Wrapper class providing functions for the mapping element between ColId
//		and ParamId during DXL->PlStmt translation
//
//	@owner:
//		elhela
//
//	@test:
//
//
//---------------------------------------------------------------------------
#ifndef GPDXL_CMappingElementColIdParamId_H
#define GPDXL_CMappingElementColIdParamId_H

#include "gpos/base.h"

#include "naucrates/dxl/operators/CDXLNode.h"
#include "naucrates/md/IMDId.h"

namespace gpdxl
{
	using namespace gpos;
	using namespace gpmd;
	//---------------------------------------------------------------------------
	//	@class:
	//		CMappingElementColIdParamId
	//
	//	@doc:
	//		Wrapper class providing functions for the mapping element between
	//		ColId and ParamId during DXL->PlStmt translation
	//
	//---------------------------------------------------------------------------
	class CMappingElementColIdParamId : public CRefCount
	{
		private:

			// column identifier that is used as the key
			ULONG m_ulColId;

			// param identifier
			ULONG m_ulParamId;

			// param type
			IMDId *m_pmdid;

		public:

			// ctors and dtor
			CMappingElementColIdParamId(ULONG ulColId, ULONG ulParamId, IMDId *pmdid);

			virtual
			~CMappingElementColIdParamId()
			{}

			// return the ColId
			ULONG UlColId() const
			{
				return m_ulColId;
			}

			// return the ParamId
			ULONG UlParamId() const
			{
				return m_ulParamId;
			}

			// return the type
			IMDId *PmdidType() const
			{
				return m_pmdid;
			}
	};
}

#endif // GPDXL_CMappingElementColIdParamId_H

// EOF
