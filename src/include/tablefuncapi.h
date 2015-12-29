/*-------------------------------------------------------------------------
 *
 * tablefuncapi.h
 *	  Declarations for Table Function API
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
 *-------------------------------------------------------------------------
 */
#ifndef TABLEFUNCAPI_H
#define TABLEFUNCAPI_H

#include "fmgr.h"
#include "access/tupdesc.h"
#include "access/htup.h"

typedef struct AnyTableData *AnyTable;

#define DatumGetAnyTable(d)   ((AnyTable) DatumGetPointer(d))
#define AnyTableGetDatum(x)   PointerGetDatum(x)

#define PG_GETARG_ANYTABLE(n) (DatumGetAnyTable(PG_GETARG_DATUM(n)))

#define TF_SET_USERDATA(userdata) tf_set_userdata_internal(fcinfo, userdata)
#define TF_GET_USERDATA() tf_get_userdata_internal(fcinfo)

extern TupleDesc AnyTable_GetTupleDesc(AnyTable);
extern HeapTuple AnyTable_GetNextTuple(AnyTable);

extern void tf_set_userdata_internal(FunctionCallInfo fcinfo, bytea *userdata);
extern bytea *tf_get_userdata_internal(FunctionCallInfo fcinfo);

#endif
