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
//		CCatalogUtils.cpp
//
//	@doc:
//		Routines to extract interesting information from the catalog
//
//	@test:
//
//
//---------------------------------------------------------------------------

#include "gpopt/utils/CCatalogUtils.h"

//---------------------------------------------------------------------------
//	@function:
//		CCatalogUtils::PlRelationOids
//
//	@doc:
//		Return list of relation oids from the catalog
//
//---------------------------------------------------------------------------
List *
CCatalogUtils::PlRelationOids()
{
	return relation_oids();
}

//---------------------------------------------------------------------------
//	@function:
//		CCatalogUtils::PlOperatorOids
//
//	@doc:
//		Return list of operator oids from the catalog
//
//---------------------------------------------------------------------------
List *
CCatalogUtils::PlOperatorOids()
{
	return operator_oids();
}

//---------------------------------------------------------------------------
//	@function:
//		CCatalogUtils::PlFunctionOids
//
//	@doc:
//		Return list of function plOids from the catalog
//
//---------------------------------------------------------------------------
List *
CCatalogUtils::PlFunctionOids()
{
	return function_oids();
}

//---------------------------------------------------------------------------
//	@function:
//		CCatalogUtils::PlAllOids
//
//	@doc:
//		Return list of all plOids from the catalog
//
//---------------------------------------------------------------------------
List *CCatalogUtils::PlAllOids()
{
	return list_concat(list_concat(PlRelationOids(), PlOperatorOids()), PlFunctionOids());
}

// EOF
