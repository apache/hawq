/*
 * debugutils.h
 * 
 * Debugging tools and routines
 * 
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
 *
 */

#ifndef _DEBUGUTILS_H_
#define _DEBUGUTILS_H_

#include "executor/tuptable.h"

extern void dotnode(void *, const char*);
extern char *tup2str(TupleTableSlot *slot);
extern void dump_tupdesc(TupleDesc tupdesc, const char *fname);
extern void dump_mt_bind(MemTupleBinding *mt_bind, const char *fname);
#ifdef USE_ASSERT_CHECKING
extern int debug_write(const char *filename, const char *output_string);
#endif
#endif /* _DEBUGUTILS_H_ */


