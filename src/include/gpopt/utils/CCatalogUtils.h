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
//		CCatalogUtils.h
//
//	@doc:
//		Routines to extract interesting information from the catalog
//
//	@test:
//
//
//---------------------------------------------------------------------------

#ifndef CCatalogUtils_H
#define CCatalogUtils_H

#include "gpdbdefs.h"


class CCatalogUtils {

	private:
		// return list of relation plOids in catalog
		static
		List *PlRelationOids();

		// return list of operator plOids in catalog
		static
		List *PlOperatorOids();

		// return list of function plOids in catalog
		static
		List *PlFunctionOids();

	public:

		// return list of all object plOids in catalog
		static
		List *PlAllOids();
};

#endif // CCatalogUtils_H

// EOF
