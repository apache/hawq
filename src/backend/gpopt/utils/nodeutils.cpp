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
//		nodeutils.cpp
//
//	@doc:
//
//	@test:
//
//
//---------------------------------------------------------------------------

#include "gpopt/utils/gpdbdefs.h"
#include "gpopt/utils/nodeutils.h"
#include "optimizer/walkers.h"

//---------------------------------------------------------------------------
//	@function:
//		textToString
//
//	@doc:
//		Convert text item to string. Caller is responsible for freeing memory.
//---------------------------------------------------------------------------

char *textToString(text *pt)
{
	char *sz = VARDATA(pt);
	int iLen = VARSIZE(pt) - VARHDRSZ;
	char *szResult = (char *) palloc(iLen + 1);
	memcpy(szResult, sz, iLen);
	szResult[iLen] = '\0';
	return szResult;
}

//---------------------------------------------------------------------------
//	@function:
//		stringToText
//
//	@doc:
//		String to text type. Caller is responsible for freeing memory.
//---------------------------------------------------------------------------

text *stringToText(char *sz)
{
	int len = (int) (strlen(sz) + VARHDRSZ);
	text *ptResult = (text *) palloc(len + VARHDRSZ);
	SET_VARSIZE(ptResult, len);
	memcpy(VARDATA(ptResult), sz, len);
	return ptResult;
}

// EOF
