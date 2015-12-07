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
//		nodeutils.h
//
//	@test:
//
//
//---------------------------------------------------------------------------

#ifndef NodeUtils_H
#define NodeUtils_H

#include "c.h"

extern struct List *extract_nodes(struct Node *ps, int nodeTag);

// String related functionality.

extern char *textToString(text *pt);
extern text *stringToText(char *sz);
extern struct Const *stringToConst(char *sz);
extern char *constToString(Const *pconst);

#endif // NodeUtils_H

// EOF
