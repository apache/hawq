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
 *
 * hashapi_access.c
 *     Test functions for accessing the Cdb Hash API.
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
#include "postgres.h"

#include "fmgr.h"

#include "cdb/cdbhash.h"
#include "executor/spi.h"
#include "funcapi.h"
#include "utils/date.h"
#include "utils/datetime.h"
#include "utils/memutils.h"
#include "utils/numeric.h"
#include "utils/timestamp.h"
#define GET_STR(textp) \
  DatumGetCString(DirectFunctionCall1(textout, PointerGetDatum(textp)))

/*
 * Pointer to the hash session characteristics.
 */
static CdbHash *h;

/*==============================================
 *
 * SINGLE VALUE HASHING
 *
 *==============================================
 */

/*
 * HASHAPI_Hash_1_BigInt
 * Perform a simple 1-value bigint hash
 */
PG_FUNCTION_INFO_V1(HASHAPI_Hash_1_BigInt);
Datum HASHAPI_Hash_1_BigInt(PG_FUNCTION_ARGS) {
  int32 num_segs;            /* number of segments  */
  int16 algorithm;           /* hashing algorithm   */
  int64 val1;                /* big int input value */
  unsigned int targetbucket; /* 0-based  */
  Datum d1;
  Oid oid;
  /* Get the value to hash */
  val1 = PG_GETARG_INT64(0);
  d1 = Int64GetDatum(val1);
  /* create a CdbHash for this hash test. */
  h = makeCdbHash(1, 1);
  /* init cdb hash */
  cdbhashinit(h);
  oid = INT8OID;
  cdbhash(h, d1, oid);
  PG_RETURN_INT32(h->hash);
}

/*
 * HASHAPI_Hash_1_BigInt
 * Perform a simple 1-value bigint hash
 */
PG_FUNCTION_INFO_V1(HASHAPI_Hash_1_Bool);
Datum HASHAPI_Hash_1_Bool(PG_FUNCTION_ARGS) {
  Datum d1;
  Oid oid;
  bool val1; /* boolean input value */
  /* Get the value to hash */
  val1 = PG_GETARG_BOOL(0);
  d1 = BoolGetDatum(val1);
  h = makeCdbHash(1, 1);
  cdbhashinit(h);
  oid = BOOLOID;
  cdbhash(h, d1, oid);
  PG_RETURN_INT32(h->hash);
}

/*
 * HASHAPI_Hash_1_Int
 * Perform a simple 1-value int4 hash
 */
PG_FUNCTION_INFO_V1(HASHAPI_Hash_1_Int);
Datum HASHAPI_Hash_1_Int(PG_FUNCTION_ARGS) {
  int32 val1; /* int input value */
  Datum d1;
  Oid oid;
  val1 = PG_GETARG_INT32(0);
  d1 = Int32GetDatum(val1);
  h = makeCdbHash(1, 1);
  /* init cdb hash */
  cdbhashinit(h);
  oid = INT4OID;
  cdbhash(h, d1, oid);
  PG_RETURN_INT32(h->hash);
}

/*
 * HASHAPI_Hash_1_SmallInt
 * Perform a simple 1-value int2 hash
 */
PG_FUNCTION_INFO_V1(HASHAPI_Hash_1_SmallInt);
Datum HASHAPI_Hash_1_SmallInt(PG_FUNCTION_ARGS) {
  int32 value; /* int input value
                                  will be cast to int16 */
  int16 val1;
  unsigned int targetbucket; /* 0-based   */
  Datum d1;
  Oid oid;
  value = PG_GETARG_INT32(0);
  val1 = (int16)value;
  d1 = Int16GetDatum(val1);
  /* create a CdbHash for this hash test. */
  h = makeCdbHash(1, 1);
  /* init cdb hash */
  cdbhashinit(h);
  oid = INT2OID;
  cdbhash(h, d1, oid);
  PG_RETURN_INT32(h->hash);
}

/*
 * HASHAPI_Hash_1_Char
 * Perform a simple 1 character hash
 */
PG_FUNCTION_INFO_V1(HASHAPI_Hash_1_BpChar);
Datum HASHAPI_Hash_1_BpChar(PG_FUNCTION_ARGS) {
  BpChar *val1; /* char value          */
  Datum d1;
  Oid oid;
  val1 = PG_GETARG_BPCHAR_P(0);
  d1 = PointerGetDatum(val1);
  h = makeCdbHash(1, 1);
  cdbhashinit(h);
  oid = BPCHAROID;
  cdbhash(h, d1, oid);
  PG_RETURN_INT32(h->hash);
}

/*
 * HASHAPI_Hash_1_Text
 * Perform a simple 1 string hash.
 */
PG_FUNCTION_INFO_V1(HASHAPI_Hash_1_Text);
Datum HASHAPI_Hash_1_Text(PG_FUNCTION_ARGS) {
  text *val1; /* char value          */
  Datum d1;
  Oid oid;
  val1 = PG_GETARG_TEXT_P(0);
  d1 = PointerGetDatum(val1);
  h = makeCdbHash(1, 1);
  cdbhashinit(h);
  oid = TEXTOID;
  cdbhash(h, d1, oid);
  PG_RETURN_INT32(h->hash);
}

/*
 * HASHAPI_Hash_1_Varchar
 * Perform a simple 1 string (of type VARCHAR) hash.
 */
PG_FUNCTION_INFO_V1(HASHAPI_Hash_1_Varchar);
Datum HASHAPI_Hash_1_Varchar(PG_FUNCTION_ARGS) {
  VarChar *val1; /* varchar value		  */
  Datum d1;
  Oid oid;
  val1 = PG_GETARG_VARCHAR_P(0);
  d1 = PointerGetDatum(val1);
  /* create a CdbHash for this hash test. */
  h = makeCdbHash(1, 1);
  cdbhashinit(h);
  oid = VARCHAROID;
  cdbhash(h, d1, oid);
  /* Avoid leaking memory for toasted inputs */
  PG_RETURN_INT32(h->hash);
}

/*
 * HASHAPI_Hash_1_Bytea
 * Perform a simple 1 string (of type BYTEA) hash.
 */
PG_FUNCTION_INFO_V1(HASHAPI_Hash_1_Bytea);
Datum HASHAPI_Hash_1_Bytea(PG_FUNCTION_ARGS) {
  bytea *val1;     /* bytea value		  */
  int16 algorithm; /* hashing algorithm   */
  Datum d1;
  Oid oid;
  /* Get the value to hash */
  val1 = PG_GETARG_BYTEA_P(0);
  d1 = PointerGetDatum(val1);
  /* create a CdbHash for this hash test. */
  h = makeCdbHash(1, 1);
  cdbhashinit(h);
  oid = BYTEAOID;
  cdbhash(h, d1, oid);
  /* Avoid leaking memory for toasted inputs */
  PG_RETURN_INT32(h->hash);
}

/*
 * HASHAPI_Hash_1_float8
 * Perform a single float8 value (double) hash.
 */
PG_FUNCTION_INFO_V1(HASHAPI_Hash_1_float8);
Datum HASHAPI_Hash_1_float8(PG_FUNCTION_ARGS) {
  float8 val1; /* varchar value		  */
  Datum d1;
  Oid oid;
  /* Get the value to hash */
  val1 = PG_GETARG_FLOAT8(0);
  d1 = Float8GetDatum(val1);
  /* create a CdbHash for this hash test. */
  h = makeCdbHash(1, 1);
  /* init cdb hash */
  cdbhashinit(h);
  oid = FLOAT8OID;
  cdbhash(h, d1, oid);
  PG_RETURN_INT32(h->hash);
}

/*
 * HASHAPI_Hash_1_float4
 * Perform a single float4 value (double) hash.
 */
PG_FUNCTION_INFO_V1(HASHAPI_Hash_1_float4);
Datum HASHAPI_Hash_1_float4(PG_FUNCTION_ARGS) {
  float4 val1; /* varchar value		  */
  Datum d1;
  Oid oid;
  /* Get the value to hash */
  val1 = PG_GETARG_FLOAT4(0);
  d1 = Float4GetDatum(val1);
  /* create a CdbHash for this hash test. */
  h = makeCdbHash(1, 1);
  /* init cdb hash */
  cdbhashinit(h);
  oid = FLOAT4OID;
  cdbhash(h, d1, oid);
  PG_RETURN_INT32(h->hash);
}

/*
 * HASHAPI_Hash_1_timestamp
 * Perform a single timestamp value hash.
 */
PG_FUNCTION_INFO_V1(HASHAPI_Hash_1_timestamp);
Datum HASHAPI_Hash_1_timestamp(PG_FUNCTION_ARGS) {
  Timestamp val1; /* varchar value		  */
  Datum d1;
  Oid oid;
  val1 = PG_GETARG_TIMESTAMP(0);
  d1 = TimestampGetDatum(val1);
  /* create a CdbHash for this hash test. */
  h = makeCdbHash(1, 1);
  /* init cdb hash */
  cdbhashinit(h);
  oid = TIMESTAMPOID;
  cdbhash(h, d1, oid);
  PG_RETURN_INT32(h->hash);
}

/*
 * HASHAPI_Hash_1_timestamptz
 * Perform a single timestamp with time zone value hash.
 */
PG_FUNCTION_INFO_V1(HASHAPI_Hash_1_timestamptz);
Datum HASHAPI_Hash_1_timestamptz(PG_FUNCTION_ARGS) {
  TimestampTz val1; /* varchar value		  */
  Datum d1;
  Oid oid;
  /* Get the value to hash */
  val1 = PG_GETARG_TIMESTAMPTZ(0);
  d1 = TimestampTzGetDatum(val1);
  /* create a CdbHash for this hash test. */
  h = makeCdbHash(1, 1);
  /* init cdb hash */
  cdbhashinit(h);
  oid = TIMESTAMPTZOID;
  cdbhash(h, d1, oid);
  PG_RETURN_INT32(h->hash);
}

/*
 * HASHAPI_Hash_1_date
 * Perform a single date value hash.
 */
PG_FUNCTION_INFO_V1(HASHAPI_Hash_1_date);
Datum HASHAPI_Hash_1_date(PG_FUNCTION_ARGS) {
  DateADT val1; /* date value		  */
  Datum d1;
  Oid oid;
  /* Get the value to hash */
  val1 = PG_GETARG_DATEADT(0);
  d1 = DateADTGetDatum(val1);
  /* create a CdbHash for this hash test. */
  h = makeCdbHash(1, 1);
  /* init cdb hash */
  cdbhashinit(h);
  oid = DATEOID;
  cdbhash(h, d1, oid);
  /* reduce the result hash value */
  PG_RETURN_INT32(h->hash);
}

/*
 * HASHAPI_Hash_1_time
 * Perform a single time value hash.
 */
PG_FUNCTION_INFO_V1(HASHAPI_Hash_1_time);
Datum HASHAPI_Hash_1_time(PG_FUNCTION_ARGS) {
  TimeADT val1; /* time value		  */
  Datum d1;
  Oid oid;
  /* Get the value to hash */
  val1 = PG_GETARG_TIMEADT(0);
  d1 = TimeADTGetDatum(val1);
  /* create a CdbHash for this hash test. */
  h = makeCdbHash(1, 1);
  /* init cdb hash */
  cdbhashinit(h);
  oid = TIMEOID;
  cdbhash(h, d1, oid);
  PG_RETURN_INT32(h->hash);
}

/*
 * HASHAPI_Hash_1_timetz
 * Perform a single time with time zone value hash.
 */
PG_FUNCTION_INFO_V1(HASHAPI_Hash_1_timetz);
Datum HASHAPI_Hash_1_timetz(PG_FUNCTION_ARGS) {
  TimeTzADT *val1; /* time w/timezone value */
  int16 algorithm; /* hashing algorithm   */
  Datum d1;
  Oid oid;
  /* Get the value to hash */
  val1 = PG_GETARG_TIMETZADT_P(0);
  d1 = TimeTzADTPGetDatum(val1);
  /* create a CdbHash for this hash test. */
  h = makeCdbHash(1, 1);
  /* init cdb hash */
  cdbhashinit(h);
  oid = TIMETZOID;
  cdbhash(h, d1, oid);
  PG_RETURN_INT32(h->hash);
}

/*
 * HASHAPI_Hash_1_numeric
 * Perform a single NUMERIC value hash.
 */
PG_FUNCTION_INFO_V1(HASHAPI_Hash_1_numeric);
Datum HASHAPI_Hash_1_numeric(PG_FUNCTION_ARGS) {
  Numeric val1;    /* NUMERIC value */
  int16 algorithm; /* hashing algorithm   */
  Datum d1;
  Oid oid;
  val1 = PG_GETARG_NUMERIC(0);
  d1 = NumericGetDatum(val1);
  h = makeCdbHash(1, 1);
  cdbhashinit(h);
  oid = NUMERICOID;
  cdbhash(h, d1, oid);
  PG_RETURN_INT32(h->hash);
}

/*
 * HASHAPI_Hash_1_null
 * Perform a single null value hash.
 */
PG_FUNCTION_INFO_V1(HASHAPI_Hash_1_null);
Datum HASHAPI_Hash_1_null(PG_FUNCTION_ARGS) {
  h = makeCdbHash(1, 1);
  /* init cdb hash */
  cdbhashinit(h);
  cdbhashnull(h);
  PG_RETURN_INT32(h->hash);
}
