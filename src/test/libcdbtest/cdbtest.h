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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/*-------------------------------------------------------------------------
 * cdbtest.h
 *     Declarations of functions in the cdbtest library.
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
#ifndef CDBTEST_H
#define CDBTEST_H

/* These functions are defined in chunkfuncs.c. */

extern Datum GetSerializedRows__text1(PG_FUNCTION_ARGS);
extern Datum GetChunkedRows__text1(PG_FUNCTION_ARGS);

extern Datum GetDeserializedRows__text1_regtype1(PG_FUNCTION_ARGS);
extern Datum GetDechunkedRows__text1_regtype1(PG_FUNCTION_ARGS);

/* These functions are defined in ipcfuncs.c */
extern Datum ml_ipc_getMsg__text1(PG_FUNCTION_ARGS);

/* These functions are defined in mlipc_access.c */
extern Datum ml_ipc_bench(PG_FUNCTION_ARGS);

/* These functions are defined in mlapi_access.c */ 

extern Datum MLAPI_InitMLNode__int1_bool1_regtype1(PG_FUNCTION_ARGS);
extern Datum MLAPI_EndMLNode__int1(PG_FUNCTION_ARGS);

extern Datum MLAPI_SendTuple__int1_text1(PG_FUNCTION_ARGS);
extern Datum MLAPI_FinishSendTuple__int1(PG_FUNCTION_ARGS);
extern Datum MLAPI_RecvTuple__int1(PG_FUNCTION_ARGS);
extern Datum MLAPI_GetLastRecvCode(PG_FUNCTION_ARGS);

extern Datum MLAPI_SendRecvPerf__int1_text1(PG_FUNCTION_ARGS);

/* These functions are defined in hashapi_access.c */

extern Datum HASHAPI_Hash_1_BigInt(PG_FUNCTION_ARGS);
extern Datum HASHAPI_Hash_1_Int(PG_FUNCTION_ARGS);
extern Datum HASHAPI_Hash_1_SmallInt(PG_FUNCTION_ARGS);
extern Datum HASHAPI_Hash_2_Int_Int(PG_FUNCTION_ARGS);
extern Datum HASHAPI_Hash_1_BpChar(PG_FUNCTION_ARGS);
extern Datum HASHAPI_Hash_1_Text(PG_FUNCTION_ARGS);
extern Datum HASHAPI_Hash_2_Text_Text(PG_FUNCTION_ARGS);
extern Datum HASHAPI_Hash_1_Varchar(PG_FUNCTION_ARGS);
extern Datum HASHAPI_Hash_1_Bytea(PG_FUNCTION_ARGS);
extern Datum HASHAPI_Hash_1_float8(PG_FUNCTION_ARGS);
extern Datum HASHAPI_Hash_1_float4(PG_FUNCTION_ARGS);
extern Datum HASHAPI_Hash_1_null(PG_FUNCTION_ARGS);
extern Datum HASHAPI_Hash_1_timestamp(PG_FUNCTION_ARGS);
extern Datum HASHAPI_Hash_1_timestamptz(PG_FUNCTION_ARGS);
extern Datum HASHAPI_Hash_1_date(PG_FUNCTION_ARGS);
extern Datum HASHAPI_Hash_1_time(PG_FUNCTION_ARGS);
extern Datum HASHAPI_Hash_1_timetz(PG_FUNCTION_ARGS);
extern Datum HASHAPI_Hash_1_numeric(PG_FUNCTION_ARGS);
extern Datum HASHAPI_Hash_1_Bool(PG_FUNCTION_ARGS);


#endif /* CDBTEST_H */
