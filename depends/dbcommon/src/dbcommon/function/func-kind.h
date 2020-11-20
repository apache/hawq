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

#ifndef DBCOMMON_SRC_DBCOMMON_FUNCTION_FUNC_KIND_H_
#define DBCOMMON_SRC_DBCOMMON_FUNCTION_FUNC_KIND_H_

// clang-format off

// [[[cog
#if false
from cog import out, outl
import sys, os
python_path = os.path.dirname(cog.inFile) + "/../python"
python_path = os.path.abspath(python_path)
sys.path.append(python_path)
from code_generator import *
cog.outl("""
/*
 * DO NOT EDIT!"
 * This file is generated from : %s
 */
""" % cog.inFile)
global counter;
counter = 0;
#endif
// ]]]
// [[[end]]]

#ifdef __cplusplus
namespace dbcommon {
enum FuncKind : uint32_t {
#else
enum FuncKind {
#endif

// OpExpr::setOpFuncId chooses the specific function to use:
// 1. val op val 2. val op vec 3. vec op val 4. vec op vec
// [[[cog
#if false
def generate_func_kind(opmap, ltypemap, rtypemap) :
  global counter;
  for op_key in opmap:
    for ta in ltypemap:
      for tb in rtypemap:
        functionId = "%s_%s_%s = %d" % (ltypemap[ta].sqltype, opmap[op_key].name, rtypemap[tb].sqltype, counter)
        cog.out("""
  %s,""" % functionId.upper())

        counter += 1
#endif
// ]]]
// [[[end]]]

// [[[cog
#if false
cog.outl("//%s:%d" % (cog.inFile,cog.firstLineNum))
generate_func_kind(CMP_OP, NUMERIC_TYPES, NUMERIC_TYPES)
generate_func_kind(CMP_OP, STRING_TYPES, STRING_TYPES)
generate_func_kind(CMP_OP, BINARY_TYPES, BINARY_TYPES)
generate_func_kind(CMP_OP, BOOL_TYPE, BOOL_TYPE)
generate_func_kind(CMP_OP, TIMESTAMP_TYPE, TIMESTAMP_TYPE)
generate_func_kind(CMP_OP, DECIMAL_TYPE, DECIMAL_TYPE)
#endif
// ]]]
// [[[end]]]

// [[[cog
#if false
cog.outl("//%s:%d" % (cog.inFile,cog.firstLineNum))
generate_func_kind(ARITH_OP, NUMERIC_TYPES, NUMERIC_TYPES)
generate_func_kind(ARITH_OP, DECIMAL_TYPE, DECIMAL_TYPE)
#endif
// ]]]
// [[[end]]]

// aggregate function
// [[[cog
#if false
def output_agg_func_id(funcname, transname, prelimname, finname) :
  global counter
  cog.out("""
  %s = %d,
  %s = %d,""" % (funcname, counter, transname, counter + 1))
  counter += 2

  if prelimname != transname:
    cog.out("""
  %s = %d,""" % (prelimname, counter))
    counter += 1

  if finname is not None:
    cog.out("""
  %s = %d,""" % (finname, counter))
    counter += 1

def generate_agg_func_kind_any_type(aggs) :
  global counter
  for agg in aggs:
    if aggs[agg].anytype:
      (funcname, transname, prelimname, finname) = get_agg_func_id(aggs[agg], None)
      cog.out("""
  %s_STAR = %d,""" % (funcname, counter))
      counter += 1
      output_agg_func_id(funcname, transname, prelimname, finname)

def generate_agg_func_kind(aggs, types) :
  for agg in aggs:
    if not aggs[agg].anytype:
      for type in types:
        (funcname, transname, prelimname, finname) = get_agg_func_id(aggs[agg], types[type])
        output_agg_func_id(funcname, transname, prelimname, finname)
#endif
// ]]]
// [[[end]]]

// [[[cog
#if false
cog.outl("//%s:%d" % (cog.inFile,cog.firstLineNum))
generate_agg_func_kind(ALL_AGGS, NUMERIC_TYPES)
generate_agg_func_kind(ALL_AGGS, DECIMAL_TYPE)
generate_agg_func_kind(MIN_MAX_AGGS, STRING_TYPES)
generate_agg_func_kind(MIN_MAX_AGGS, BPCHAR_TYPES)
generate_agg_func_kind(MIN_MAX_AGGS, DATE_TYPES)
generate_agg_func_kind(MIN_MAX_AGGS, TIMESTAMP_TYPE)
generate_agg_func_kind_any_type(ALL_AGGS)
#endif
// ]]]
// [[[end]]]

// [[[cog
#if false
def generate_typecast_funcid(typemap) :
  global counter
  typeList = sorted(typemap.keys())
  for i in range(len(typeList)):
    for j in xrange(len(typeList)):
      if i != j:
        functionId = "%s_TO_%s = %d" % (typemap[typeList[i]].sqltype, typemap[typeList[j]].sqltype, counter)
        cog.out("""
  %s,""" % functionId.upper())
        counter += 1
#endif
// ]]]
// [[[end]]]

// [[[cog
#if false
cog.outl("//%s:%d" % (cog.inFile,cog.firstLineNum))
generate_typecast_funcid(NUMERIC_AND_DECIMAL_TYPES)
#endif
// ]]]
// [[[end]]]

// [[[cog
#if false
cog.outl("//%s:%d" % (cog.inFile,cog.firstLineNum))
generate_func_kind(CMP_OP, INTERVAL_TYPE, INTERVAL_TYPE)
#endif
// ]]]
// [[[end]]]

// [[[cog
#if false
cog.outl("//%s:%d" % (cog.inFile,cog.firstLineNum))
generate_func_kind(ARITH_OP, INTERVAL_TYPE, INTERVAL_TYPE)
#endif
// ]]]
// [[[end]]]

  // clang-format on

  // cast related functions
  SMALLINT_TO_TEXT,
  INT_TO_TEXT,
  BIGINT_TO_TEXT,
  FLOAT_TO_TEXT,
  DOUBLE_TO_TEXT,
  BOOL_TO_TEXT,
  TEXT_TO_CHAR,
  INT_TO_CHAR,
  DOUBLE_TO_TIMESTAMP,

  // date/time related functions
  DATE_TO_TIMESTAMP,
  TIMESTAMP_TO_DATE,
  IS_TIMESTAMP_FINITE,
  TIMESTAMP_DATE_PART,
  TIMESTAMP_DATE_TRUNC,
  TIMESTAMP_TO_TEXT,
  TIME_SUB_TIME,
  // interval related functions
  TIMESTAMP_SUB_TIMESTAMP,
  DATE_ADD_INTERVAL,
  DATE_SUB_INTERVAL,
  TIME_ADD_INTERVAL,
  TIME_SUB_INTERVAL,
  TIMESTAMP_ADD_INTERVAL,
  TIMESTAMP_SUB_INTERVAL,

  // string related functions
  CHAR_TO_STRING,
  STRING_OCTET_LENGTH,
  BPCHAR_OCTET_LENGTH,
  STRING_CHAR_LENGTH,
  BPCHAR_CHAR_LENGTH,
  STRING_LIKE,
  BPCHAR_LIKE,
  STRING_NOT_LIKE,
  BPCHAR_NOT_LIKE,
  STRING_SUBSTRING,
  STRING_SUBSTRING_NOLEN,
  STRING_LOWER,
  STRING_UPPER,
  STRING_CONCAT,
  STRING_POSITION,
  STRING_STRPOS,
  STRING_INITCAP,
  STRING_ASCII,
  STRING_VARCHAR,
  STRING_LTRIM_BLANK,
  STRING_LTRIM_CHARS,
  STRING_RTRIM_BLANK,
  STRING_RTRIM_CHARS,
  STRING_BTRIM_BLANK,
  STRING_BTRIM_CHARS,
  STRING_REPEAT,
  STRING_CHR,
  STRING_BPCHAR,
  STRING_LPAD,
  STRING_RPAD,
  STRING_LPAD_NOFILL,
  STRING_RPAD_NOFILL,
  STRING_TRANSLATE,

  // binary related functions
  BINARY_OCTET_LENGTH,

  // array related functions
  FLOAT_ARRAY_EUCLIDEAN_METRIC,
  DOUBLE_ARRAY_EUCLIDEAN_METRIC,
  FLOAT_ARRAY_COSINE_DISTANCE,
  DOUBLE_ARRAY_COSINE_DISTANCE,
  BIGINT_ARRAY_OVERLAP,
  BIGINT_ARRAY_CONTAINS,
  BIGINT_ARRAY_CONTAINED,

  // mathematic functions
  DOUBLE_ABS,
  FLOAT_ABS,
  INT64_ABS,
  INT32_ABS,
  INT16_ABS,
  DOUBLE_CBRT,
  DOUBLE_SQRT,
  INT16_BINARY_NOT,
  INT32_BINARY_NOT,
  INT64_BINARY_NOT,
  INT16_BINARY_SHIFT_LEFT,
  INT32_BINARY_SHIFT_LEFT,
  INT64_BINARY_SHIFT_LEFT,
  INT16_BINARY_SHIFT_RIGHT,
  INT32_BINARY_SHIFT_RIGHT,
  INT64_BINARY_SHIFT_RIGHT,
  INT16_BINARY_AND,
  INT32_BINARY_AND,
  INT64_BINARY_AND,
  INT16_BINARY_OR,
  INT32_BINARY_OR,
  INT64_BINARY_OR,
  INT16_BINARY_XOR,
  INT32_BINARY_XOR,
  INT64_BINARY_XOR,
  INT16_MOD,
  INT32_MOD,
  INT16_32_MOD,
  INT32_16_MOD,
  INT64_MOD,
  DOUBLE_POW,
  DOUBLE_CEIL,
  DOUBLE_FLOOR,
  DOUBLE_ROUND,
  DOUBLE_TRUNC,
  DOUBLE_SIGN,
  DOUBLE_EXP,
  DOUBLE_LN,
  DOUBLE_LG,
  DOUBLE_LOG,
  DOUBLE_ACOS,
  DOUBLE_ASIN,
  DOUBLE_ATAN,
  DOUBLE_ATAN2,
  DOUBLE_COS,
  DOUBLE_COT,
  DOUBLE_SIN,
  DOUBLE_TAN,

  // decimal function
  DECIMAL_ABS,
  DECIMAL_SIGN,
  DECIMAL_CEIL,
  DECIMAL_FLOOR,
  DECIMAL_ROUND,
  DECIMAL_ROUND_WITHOUT_SCALE,
  DECIMAL_TRUNC,
  DECIMAL_TRUNC_WITHOUT_SCALE,
  DECIMAL_TO_DECIMAL,
  DECIMAL_MOD,
  DECIMAL_SQRT,
  DECIMAL_EXP,
  DECIMAL_LN,
  DECIMAL_LOG,
  DECIMAL_FAC,
  DECIMAL_POW,

  TEXT_TO_SMALLINT,
  TEXT_TO_INT,
  TEXT_TO_BIGINT,
  TEXT_TO_FLOAT,
  TEXT_TO_DOUBLE,

  SMALLINT_TO_BOOLEAN,
  INT_TO_BOOLEAN,
  BIGINT_TO_BOOLEAN,
  BOOLEAN_TO_SMALLINT,
  BOOLEAN_TO_INT,
  BOOLEAN_TO_BIGINT,
  TEXT_TO_DECIMAL,
  TO_NUMBER,

  // random()
  RANDOMF,

  // Do nothing
  DONOTHING,

  FUNCINVALID = 38324,
};

#ifdef __cplusplus
}  // namespace dbcommon
#endif

#endif  // DBCOMMON_SRC_DBCOMMON_FUNCTION_FUNC_KIND_H_
