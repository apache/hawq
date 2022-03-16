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

#include "postgres.h"

#include "fmgr.h"

#include "executor/spi.h"
#include "utils/date.h"
#include "utils/datetime.h"
#include "utils/memutils.h"
#include "utils/numeric.h"
#include "utils/timestamp.h"

#include "dbcommon/cwrapper/dbcommon-c.h"
#include "dbcommon/type/type-kind.h"
PG_FUNCTION_INFO_V1(timestampToout);
Datum timestampToout(PG_FUNCTION_ARGS) {
  Timestamp timestamp = PG_GETARG_TIMESTAMP(0);
  char* result;
  struct pg_tm tt, *tm = &tt;
  fsec_t fsec;
  char* tzn = NULL;
  char buf[MAXDATELEN + 1];
  if (timestamp2tm(timestamp, NULL, tm, &fsec, NULL, NULL) == 0)
    EncodeDateTime(tm, fsec, NULL, &tzn, 1, buf);
  result = pstrdup(buf);
  PG_RETURN_CSTRING(result);
}

PG_FUNCTION_INFO_V1(dateToOut);
Datum dateToOut(PG_FUNCTION_ARGS) {
  DateADT date = PG_GETARG_DATEADT(0);
  char* result;
  struct pg_tm tt, *tm = &tt;
  char buf[MAXDATELEN + 1];

  j2date(date + POSTGRES_EPOCH_JDATE, &(tm->tm_year), &(tm->tm_mon),
         &(tm->tm_mday));

  EncodeDateOnly(tm, 1, buf);

  result = pstrdup(buf);
  PG_RETURN_CSTRING(result);
}

PG_FUNCTION_INFO_V1(timestamptzToout);
Datum timestamptzToout(PG_FUNCTION_ARGS) {
  TimestampTz dt = PG_GETARG_TIMESTAMPTZ(0);
  char* result;
  int tz;
  struct pg_tm tt, *tm = &tt;
  fsec_t fsec;
  char* tzn;
  char buf[MAXDATELEN + 1];
  if (timestamp2tm(dt, &tz, tm, &fsec, &tzn, NULL) == 0)
    EncodeDateTime(tm, fsec, &tz, &tzn, 1, buf);
  result = pstrdup(buf);
  PG_RETURN_CSTRING(result);
}

PG_FUNCTION_INFO_V1(cdbHashApiBigInt);
Datum cdbHashApiBigInt(PG_FUNCTION_ARGS) {
  char* str = DatumGetCString(
      DirectFunctionCall1(int8out, Int64GetDatum(PG_GETARG_INT64(0))));
  int32_t ret = dbcommonCdbHash(BIGINTID, str);
  PG_RETURN_INT32(ret);
}

PG_FUNCTION_INFO_V1(cdbHashApiBool);
Datum cdbHashApiBool(PG_FUNCTION_ARGS) {
  char* str = DatumGetCString(
      DirectFunctionCall1(boolout, BoolGetDatum(PG_GETARG_BOOL(0))));
  int32_t ret = dbcommonCdbHash(BOOLEANID, str);
  PG_RETURN_INT32(ret);
}

PG_FUNCTION_INFO_V1(cdbHashApiInt);
Datum cdbHashApiInt(PG_FUNCTION_ARGS) {
  char* str = DatumGetCString(
      DirectFunctionCall1(int4out, Int32GetDatum(PG_GETARG_INT32(0))));
  int32_t ret = dbcommonCdbHash(INTID, str);
  PG_RETURN_INT32(ret);
}

PG_FUNCTION_INFO_V1(cdbHashApiSmallInt);
Datum cdbHashApiSmallInt(PG_FUNCTION_ARGS) {
  char* str = DatumGetCString(
      DirectFunctionCall1(int2out, Int16GetDatum((int16)PG_GETARG_INT32(0))));
  int32_t ret = dbcommonCdbHash(SMALLINTID, str);
  PG_RETURN_INT32(ret);
}

PG_FUNCTION_INFO_V1(cdbHashApiTimestamp);
Datum cdbHashApiTimestamp(PG_FUNCTION_ARGS) {
  char* str = DatumGetCString(DirectFunctionCall1(
      timestampToout, TimestampGetDatum(PG_GETARG_TIMESTAMP(0))));
  int32_t ret = dbcommonCdbHash(TIMESTAMPID, str);
  PG_RETURN_INT32(ret);
}

PG_FUNCTION_INFO_V1(cdbHashApiTimestampTz);
Datum cdbHashApiTimestampTz(PG_FUNCTION_ARGS) {
  char* str = DatumGetCString(DirectFunctionCall1(
      timestampToout, TimestampGetDatum(PG_GETARG_TIMESTAMP(0))));
  int32_t ret = dbcommonCdbHash(TIMESTAMPTZID, str);
  PG_RETURN_INT32(ret);
}

PG_FUNCTION_INFO_V1(cdbHashApiBpchar);
Datum cdbHashApiBpchar(PG_FUNCTION_ARGS) {
  char* str = DatumGetCString(
      DirectFunctionCall1(bpcharout, PointerGetDatum(PG_GETARG_BPCHAR_P(0))));
  int32_t ret = dbcommonCdbHash(CHARID, str);
  PG_RETURN_INT32(ret);
}

PG_FUNCTION_INFO_V1(cdbHashApiText);
Datum cdbHashApiText(PG_FUNCTION_ARGS) {
  char* str = DatumGetCString(
      DirectFunctionCall1(textout, PointerGetDatum(PG_GETARG_TEXT_P(0))));
  int32_t ret = dbcommonCdbHash(STRINGID, str);
  PG_RETURN_INT32(ret);
}

PG_FUNCTION_INFO_V1(cdbHashApiVarchar);
Datum cdbHashApiVarchar(PG_FUNCTION_ARGS) {
  char* str = DatumGetCString(
      DirectFunctionCall1(varcharout, PointerGetDatum(PG_GETARG_VARCHAR_P(0))));
  int32_t ret = dbcommonCdbHash(VARCHARID, str);
  PG_RETURN_INT32(ret);
}

PG_FUNCTION_INFO_V1(cdbHashApiBytea);
Datum cdbHashApiBytea(PG_FUNCTION_ARGS) {
  char* str = DatumGetCString(
      DirectFunctionCall1(byteaout, PointerGetDatum(PG_GETARG_BYTEA_P(0))));
  int32_t ret = dbcommonCdbHash(BINARYID, str);
  PG_RETURN_INT32(ret);
}

PG_FUNCTION_INFO_V1(cdbHashApiFloat8);
Datum cdbHashApiFloat8(PG_FUNCTION_ARGS) {
  char* str = DatumGetCString(
      DirectFunctionCall1(float8out, Float8GetDatum(PG_GETARG_FLOAT8(0))));
  int32_t ret = dbcommonCdbHash(DOUBLEID, str);
  PG_RETURN_INT32(ret);
}

PG_FUNCTION_INFO_V1(cdbHashApiFloat4);
Datum cdbHashApiFloat4(PG_FUNCTION_ARGS) {
  char* str = DatumGetCString(
      DirectFunctionCall1(float4out, Float4GetDatum(PG_GETARG_FLOAT4(0))));
  int32_t ret = dbcommonCdbHash(FLOATID, str);
  PG_RETURN_INT32(ret);
}

PG_FUNCTION_INFO_V1(cdbHashApiDate);
Datum cdbHashApiDate(PG_FUNCTION_ARGS) {
  char* str = DatumGetCString(
      DirectFunctionCall1(dateToOut, DateADTGetDatum(PG_GETARG_DATEADT(0))));
  int32_t ret = dbcommonCdbHash(DATEID, str);
  PG_RETURN_INT32(ret);
}

PG_FUNCTION_INFO_V1(cdbHashApiTime);
Datum cdbHashApiTime(PG_FUNCTION_ARGS) {
  char* str = DatumGetCString(
      DirectFunctionCall1(time_out, TimeADTGetDatum(PG_GETARG_TIMEADT(0))));
  int32_t ret = dbcommonCdbHash(TIMEID, str);
  PG_RETURN_INT32(ret);
}

PG_FUNCTION_INFO_V1(cdbHashApiNumeric);
Datum cdbHashApiNumeric(PG_FUNCTION_ARGS) {
  char* str = DatumGetCString(
      DirectFunctionCall1(numeric_out, NumericGetDatum(PG_GETARG_NUMERIC(0))));
  int32_t ret = dbcommonCdbHash(DECIMALNEWID, str);
  PG_RETURN_INT32(ret);
}
