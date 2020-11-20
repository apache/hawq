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

#include "dbcommon/function/func.h"

#include <cassert>

#include "dbcommon/function/agg-func.h"
#include "dbcommon/function/arith-cmp-func.cg.h"
#include "dbcommon/function/array-function.h"
#include "dbcommon/function/date-function.h"
#include "dbcommon/function/decimal-function.h"
#include "dbcommon/function/func-kind.cg.h"
#include "dbcommon/function/mathematical-function.h"
#include "dbcommon/function/string-binary-function.h"
#include "dbcommon/function/typecast-func.cg.h"
#include "dbcommon/function/typecast-function.h"
#include "dbcommon/function/typecast-texttonum-func.h"
#include "dbcommon/function/volatile-func.h"

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
#endif
// ]]]
// [[[end]]]

namespace dbcommon {

Func *Func::inst = new Func();

Func::Func() {
  inst = this;
  setupFunctionTable();
  setupAggTable();
}

//
// Setup function tables
//
void Func::setupFunctionTable() {
// clang-format off
// [[[cog
#if false
def call_function_table(opmap, ltypemap, rtypemap) :
  for op_key in opmap:
    for ta in ltypemap:
      for tb in rtypemap:
		rettype = get_func_return_type(ta, tb, op_key)
		functionId = "%s_%s_%s" % (ltypemap[ta].sqltype, opmap[op_key].name, rtypemap[tb].sqltype)
		funcImpl = "%s_%s_%s" % (ltypemap[ta].name, opmap[op_key].name, rtypemap[tb].name)
		funcName = "%s_%s_%s" % (ltypemap[ta].name, opmap[op_key].name, rtypemap[tb].name)
		if ltypemap[ta].name == ltypemap[tb].name and ltypemap[ta].name == "string":
		  funcImpl = "%s_%s_%s" % ("binary", opmap[op_key].name, "binary")
		cog.out("""
  FuncEntryArray.push_back({%s, \"%s\", %s, {%s, %s}, %s, true});
  """ % (functionId.upper(), funcName, rettype.typekind, ltypemap[ta].typekind, rtypemap[tb].typekind, funcImpl))

#endif
// ]]]
// [[[end]]]

// [[[cog
#if false
cog.outl("//%s:%d" % (cog.inFile,cog.firstLineNum))
call_function_table(CMP_OP, NUMERIC_TYPES, NUMERIC_TYPES)
#endif
// ]]]
// [[[end]]]

// [[[cog
#if false
cog.outl("//%s:%d" % (cog.inFile,cog.firstLineNum))
call_function_table(CMP_OP, STRING_TYPES, STRING_TYPES)
call_function_table(CMP_OP, BINARY_TYPES, BINARY_TYPES)
#endif
// ]]]
// [[[end]]]

// [[[cog
#if false
cog.outl("//%s:%d" % (cog.inFile,cog.firstLineNum))
call_function_table(CMP_OP, BOOL_TYPE, BOOL_TYPE)
#endif
// ]]]
// [[[end]]]

// [[[cog
#if false
cog.outl("//%s:%d" % (cog.inFile,cog.firstLineNum))
call_function_table(CMP_OP, TIMESTAMP_TYPE, TIMESTAMP_TYPE)
call_function_table(CMP_OP, DECIMAL_TYPE, DECIMAL_TYPE)
#endif
// ]]]
// [[[end]]]

// [[[cog
#if false
cog.outl("//%s:%d" % (cog.inFile,cog.firstLineNum))
call_function_table(ARITH_OP, NUMERIC_TYPES, NUMERIC_TYPES)
call_function_table(ARITH_OP, DECIMAL_TYPE, DECIMAL_TYPE)
#endif
// ]]]
// [[[end]]]

  FuncEntryArray.push_back({AVG_TINYINT, "avg", DOUBLEID, {TINYINTID}});
  FuncEntryArray.push_back({AVG_TINYINT_ACCU, "avg_tinyint_accu", AVG_DECIMAL_TRANS_DATA_ID, {TINYINTID}, avg_int8_accu});
  FuncEntryArray.push_back({AVG_TINYINT_AMALG, "avg_tinyint_amalg", AVG_DECIMAL_TRANS_DATA_ID, {AVG_DECIMAL_TRANS_DATA_ID}, avg_int8_amalg});
  FuncEntryArray.push_back({AVG_TINYINT_AVG, "avg_tinyint_avg", DOUBLEID, {AVG_DECIMAL_TRANS_DATA_ID}, avg_double_avg});

  FuncEntryArray.push_back({AVG_SMALLINT, "avg", DOUBLEID, {SMALLINTID}});
  FuncEntryArray.push_back({AVG_SMALLINT_ACCU, "avg_smallint_accu", AVG_DECIMAL_TRANS_DATA_ID, {SMALLINTID}, avg_int16_accu});
  FuncEntryArray.push_back({AVG_SMALLINT_AMALG, "avg_smallint_amalg", AVG_DECIMAL_TRANS_DATA_ID, {AVG_DECIMAL_TRANS_DATA_ID}, avg_int16_amalg});
  FuncEntryArray.push_back({AVG_SMALLINT_AVG, "avg_smallint_avg", DOUBLEID, {AVG_DECIMAL_TRANS_DATA_ID}, avg_double_avg});

  FuncEntryArray.push_back({AVG_INT, "avg", DOUBLEID, {INTID}});
  FuncEntryArray.push_back({AVG_INT_ACCU, "avg_int_accu", AVG_DECIMAL_TRANS_DATA_ID, {INTID}, avg_int32_accu});
  FuncEntryArray.push_back({AVG_INT_AMALG, "avg_int_amalg", AVG_DECIMAL_TRANS_DATA_ID, {AVG_DECIMAL_TRANS_DATA_ID}, avg_int32_amalg});
  FuncEntryArray.push_back({AVG_INT_AVG, "avg_int_avg", DOUBLEID, {AVG_DECIMAL_TRANS_DATA_ID}, avg_double_avg});

  FuncEntryArray.push_back({AVG_BIGINT, "avg", DOUBLEID, {BIGINTID}});
  FuncEntryArray.push_back({AVG_BIGINT_ACCU, "avg_bigint_accu", AVG_DECIMAL_TRANS_DATA_ID, {BIGINTID}, avg_int64_accu});
  FuncEntryArray.push_back({AVG_BIGINT_AMALG, "avg_bigint_amalg", AVG_DECIMAL_TRANS_DATA_ID, {AVG_DECIMAL_TRANS_DATA_ID}, avg_int64_amalg});
  FuncEntryArray.push_back({AVG_BIGINT_AVG, "avg_bigint_avg", DOUBLEID, {AVG_DECIMAL_TRANS_DATA_ID}, avg_double_avg});

  FuncEntryArray.push_back({AVG_FLOAT, "avg", DOUBLEID, {FLOATID}});
  FuncEntryArray.push_back({AVG_FLOAT_ACCU, "avg_float_accu", AVG_DECIMAL_TRANS_DATA_ID, {FLOATID}, avg_float_accu});
  FuncEntryArray.push_back({AVG_FLOAT_AMALG, "avg_float_amalg", AVG_DECIMAL_TRANS_DATA_ID, {AVG_DECIMAL_TRANS_DATA_ID}, avg_float_amalg});
  FuncEntryArray.push_back({AVG_FLOAT_AVG, "avg_float_avg", DOUBLEID, {AVG_DECIMAL_TRANS_DATA_ID}, avg_double_avg});

  FuncEntryArray.push_back({AVG_DOUBLE, "avg", DOUBLEID, {DOUBLEID}});
  FuncEntryArray.push_back({AVG_DOUBLE_ACCU, "avg_double_accu", AVG_DECIMAL_TRANS_DATA_ID, {DOUBLEID}, avg_double_accu});
  FuncEntryArray.push_back({AVG_DOUBLE_AMALG, "avg_double_amalg", AVG_DECIMAL_TRANS_DATA_ID, {AVG_DECIMAL_TRANS_DATA_ID}, avg_double_amalg});
  FuncEntryArray.push_back({AVG_DOUBLE_AVG, "avg_double_avg", DOUBLEID, {AVG_DECIMAL_TRANS_DATA_ID}, avg_double_avg});

  FuncEntryArray.push_back({AVG_DECIMAL, "avg", DECIMALNEWID, {DECIMALNEWID}});
  FuncEntryArray.push_back({AVG_DECIMAL_ACCU, "avg_decimal_accu", AVG_DECIMAL_TRANS_DATA_ID, {DECIMALNEWID}, avg_decimal_accu});
  FuncEntryArray.push_back({AVG_DECIMAL_AMALG, "avg_decimal_amalg", AVG_DECIMAL_TRANS_DATA_ID, {AVG_DECIMAL_TRANS_DATA_ID}, avg_decimal_amalg});
  FuncEntryArray.push_back({AVG_DECIMAL_AVG, "avg_decimal_avg", DECIMALNEWID, {AVG_DECIMAL_TRANS_DATA_ID}, avg_decimal_avg});

  FuncEntryArray.push_back({SUM_TINYINT, "sum", BIGINTID, {TINYINTID}});
  FuncEntryArray.push_back({SUM_TINYINT_SUM, "sum_tinyint_sum", BIGINTID, {BIGINTID}, sum_int8_sum});
  FuncEntryArray.push_back({SUM_TINYINT_ADD, "sum_tinyint_add", BIGINTID, {TINYINTID}, sum_int8_add});

  FuncEntryArray.push_back({SUM_SMALLINT, "sum", BIGINTID, {SMALLINTID}});
  FuncEntryArray.push_back({SUM_SMALLINT_SUM, "sum_smallint_sum", BIGINTID, {BIGINTID}, sum_int16_sum});
  FuncEntryArray.push_back({SUM_SMALLINT_ADD, "sum_smallint_add", BIGINTID, {SMALLINTID}, sum_int16_add});

  FuncEntryArray.push_back({SUM_INT, "sum", BIGINTID, {INTID}});
  FuncEntryArray.push_back({SUM_INT_SUM, "sum_int_sum", BIGINTID, {BIGINTID}, sum_int32_sum});
  FuncEntryArray.push_back({SUM_INT_ADD, "sum_int_add", BIGINTID, {INTID}, sum_int32_add});

  FuncEntryArray.push_back({SUM_BIGINT, "sum", BIGINTID, {BIGINTID}});
  FuncEntryArray.push_back({SUM_BIGINT_SUM, "sum_bigint_sum", BIGINTID, {BIGINTID}, sum_int64_sum});
  FuncEntryArray.push_back({SUM_BIGINT_ADD, "sum_bigint_add", BIGINTID, {BIGINTID}, sum_int64_add});

  FuncEntryArray.push_back({SUM_FLOAT, "sum", DOUBLEID, {FLOATID}});
  FuncEntryArray.push_back({SUM_FLOAT_SUM, "sum_float_sum", DOUBLEID, {DOUBLEID}, sum_float_sum});
  FuncEntryArray.push_back({SUM_FLOAT_ADD, "sum_float_add", DOUBLEID, {FLOATID}, sum_float_add});

  FuncEntryArray.push_back({SUM_DOUBLE, "sum", DOUBLEID, {DOUBLEID}});
  FuncEntryArray.push_back({SUM_DOUBLE_SUM, "sum_double_sum", DOUBLEID, {DOUBLEID}, sum_double_sum});
  FuncEntryArray.push_back({SUM_DOUBLE_ADD, "sum_double_add", DOUBLEID, {DOUBLEID}, sum_double_add});

  FuncEntryArray.push_back({SUM_DECIMAL, "sum", DECIMALNEWID, {DECIMALNEWID}});
  FuncEntryArray.push_back({SUM_DECIMAL_SUM, "sum_decimal_sum", DECIMALNEWID, {DECIMALNEWID}, sum_decimal_sum});
  FuncEntryArray.push_back({SUM_DECIMAL_ADD, "sum_decimal_add", DECIMALNEWID, {DECIMALNEWID}, sum_decimal_add});

  FuncEntryArray.push_back({MIN_TINYINT, "min", TINYINTID, {TINYINTID}});
  FuncEntryArray.push_back({MIN_TINYINT_SMALLER, "min_tinyint_smaller", TINYINTID, {TINYINTID}, min_int8_smaller});

  FuncEntryArray.push_back({MIN_SMALLINT, "min", SMALLINTID, {SMALLINTID}});
  FuncEntryArray.push_back({MIN_SMALLINT_SMALLER, "min_smallint_smaller", SMALLINTID, {SMALLINTID}, min_int16_smaller});

  FuncEntryArray.push_back({MIN_INT, "min", INTID, {INTID}});
  FuncEntryArray.push_back({MIN_INT_SMALLER, "min_int_smaller", INTID, {INTID}, min_int32_smaller});

  FuncEntryArray.push_back({MIN_BIGINT, "min", BIGINTID, {BIGINTID}});
  FuncEntryArray.push_back({MIN_BIGINT_SMALLER, "min_bigint_smaller", BIGINTID, {BIGINTID}, min_int64_smaller});

  FuncEntryArray.push_back({MIN_FLOAT, "min", FLOATID, {FLOATID}});
  FuncEntryArray.push_back({MIN_FLOAT_SMALLER, "min_float_smaller", FLOATID, {FLOATID}, min_float_smaller});

  FuncEntryArray.push_back({MIN_DOUBLE, "min", DOUBLEID, {DOUBLEID}});
  FuncEntryArray.push_back({MIN_DOUBLE_SMALLER, "min_double_smaller", DOUBLEID, {DOUBLEID}, min_double_smaller});

  FuncEntryArray.push_back({MIN_STRING, "min", STRINGID, {STRINGID}});
  FuncEntryArray.push_back({MIN_STRING_SMALLER, "min_string_smaller", STRINGID, {STRINGID}, min_string_smaller});

  FuncEntryArray.push_back({MIN_BPCHAR, "min", CHARID, {CHARID}});
  FuncEntryArray.push_back({MIN_BPCHAR_SMALLER, "min_bpchar_smaller", CHARID, {CHARID}, min_string_smaller});

  FuncEntryArray.push_back({MIN_DATE, "min", DATEID, {DATEID}});
  FuncEntryArray.push_back({MIN_DATE_SMALLER, "min_date_smaller", DATEID, {DATEID}, min_date_smaller});

  FuncEntryArray.push_back({MIN_TIME, "min", TIMEID, {TIMEID}});
  FuncEntryArray.push_back({MIN_TIME_SMALLER, "min_time_smaller", TIMEID, {TIMEID}, min_time_smaller});

  FuncEntryArray.push_back({MIN_TIMESTAMP, "min", TIMESTAMPID, {TIMESTAMPID}});
  FuncEntryArray.push_back({MIN_TIMESTAMP_SMALLER, "min_timestamp_smaller", TIMESTAMPID, {TIMESTAMPID}, min_timestamp_smaller});

  FuncEntryArray.push_back({MIN_DECIMAL, "min", DECIMALNEWID, {DECIMALNEWID}});
  FuncEntryArray.push_back({MIN_DECIMAL_SMALLER, "min_decimal_smaller", DECIMALNEWID, {DECIMALNEWID}, min_decimal_smaller});

  FuncEntryArray.push_back({MAX_TINYINT, "max", TINYINTID, {TINYINTID}});
  FuncEntryArray.push_back({MAX_TINYINT_LARGER, "max_tinyint_larger", TINYINTID, {TINYINTID}, max_int8_larger});

  FuncEntryArray.push_back({MAX_SMALLINT, "max", SMALLINTID, {SMALLINTID}});
  FuncEntryArray.push_back({MAX_SMALLINT_LARGER, "max_smallint_larger", SMALLINTID, {SMALLINTID}, max_int16_larger});

  FuncEntryArray.push_back({MAX_INT, "max", INTID, {INTID}});
  FuncEntryArray.push_back({MAX_INT_LARGER, "max_int_larger", INTID, {INTID}, max_int32_larger});

  FuncEntryArray.push_back({MAX_BIGINT, "max", BIGINTID, {BIGINTID}});
  FuncEntryArray.push_back({MAX_BIGINT_LARGER, "max_bigint_larger", BIGINTID, {BIGINTID}, max_int64_larger});

  FuncEntryArray.push_back({MAX_FLOAT, "max", FLOATID, {FLOATID}});
  FuncEntryArray.push_back({MAX_FLOAT_LARGER, "max_float_larger", FLOATID, {FLOATID}, max_float_larger});

  FuncEntryArray.push_back({MAX_DOUBLE, "max", DOUBLEID, {DOUBLEID}});
  FuncEntryArray.push_back({MAX_DOUBLE_LARGER, "max_double_larger", DOUBLEID, {DOUBLEID}, max_double_larger});

  FuncEntryArray.push_back({MAX_STRING, "max", STRINGID, {STRINGID}});
  FuncEntryArray.push_back({MAX_STRING_LARGER, "max_string_larger", STRINGID, {STRINGID}, max_string_larger});

  FuncEntryArray.push_back({MAX_BPCHAR, "max", CHARID, {CHARID}});
  FuncEntryArray.push_back({MAX_BPCHAR_LARGER, "max_bpchar_larger", CHARID, {CHARID}, max_string_larger});

  FuncEntryArray.push_back({MAX_DATE, "max", DATEID, {DATEID}});
  FuncEntryArray.push_back({MAX_DATE_LARGER, "max_date_larger", DATEID, {DATEID}, max_date_larger});

  FuncEntryArray.push_back({MAX_TIME, "max", TIMEID, {TIMEID}});
  FuncEntryArray.push_back({MAX_TIME_LARGER, "max_time_larger", TIMEID, {TIMEID}, max_time_larger});

  FuncEntryArray.push_back({MAX_TIMESTAMP, "max", TIMESTAMPID, {TIMESTAMPID}});
  FuncEntryArray.push_back({MAX_TIMESTAMP_LARGER, "max_timestamp_larger", TIMESTAMPID, {TIMESTAMPID}, max_timestamp_larger});

  FuncEntryArray.push_back({MAX_DECIMAL, "max", DECIMALNEWID, {DECIMALNEWID}});
  FuncEntryArray.push_back({MAX_DECIMAL_LARGER, "max_decimal_larger", DECIMALNEWID, {DECIMALNEWID}, max_decimal_larger});


  FuncEntryArray.push_back({COUNT, "count", BIGINTID, {ANYID}});
  FuncEntryArray.push_back({COUNT_INC, "count_inc", BIGINTID, {ANYID}, count_inc});
  FuncEntryArray.push_back({COUNT_ADD, "count_add", BIGINTID, {ANYID}, count_add});
  FuncEntryArray.push_back({COUNT_STAR, "count", BIGINTID, {}, count_star});

// [[[cog
#if false
def generate_typecast_table(typemap) :
  typeList = sorted(typemap.keys())
  for i in range(len(typeList)):
    for j in xrange(len(typeList)):
      if i < j:
        paramtype = typemap[typeList[i]].typekind
        rettype = typemap[typeList[j]].typekind
        functionId = "%s_TO_%s" % (typemap[typeList[i]].sqltype, typemap[typeList[j]].sqltype)
        funcImpl = "%s_to_%s" % (typemap[typeList[i]].name, typemap[typeList[j]].name)
        cog.out("""
  FuncEntryArray.push_back({%s, \"%s\", %s, {%s}, %s, true});""" % (functionId.upper(), functionId.lower(), rettype, paramtype, funcImpl))
      elif i > j:
        paramtype = typemap[typeList[i]].typekind
        rettype = typemap[typeList[j]].typekind
        functionId = "%s_TO_%s" % (typemap[typeList[i]].sqltype, typemap[typeList[j]].sqltype)
        funcImpl = "%s_to_%s" % (typemap[typeList[i]].name, typemap[typeList[j]].name)
        cog.out("""
  FuncEntryArray.push_back({%s, \"%s\", %s, {%s}, %s, true});""" % (functionId.upper(), functionId.lower(), rettype, paramtype, funcImpl))
#endif
// ]]]
// [[[end]]]

// [[[cog
#if false
cog.outl("//%s:%d" % (cog.inFile, cog.firstLineNum))
generate_typecast_table(NUMERIC_AND_DECIMAL_TYPES)
#endif
// ]]]
// [[[end]]]

// [[[cog
#if false
cog.outl("//%s:%d" % (cog.inFile,cog.firstLineNum))
call_function_table(CMP_OP, INTERVAL_TYPE, INTERVAL_TYPE)
#endif
// ]]]
// [[[end]]]

// [[[cog
#if false
cog.outl("//%s:%d" % (cog.inFile,cog.firstLineNum))
call_function_table(ARITH_OP, INTERVAL_TYPE, INTERVAL_TYPE)
#endif
// ]]]
// [[[end]]]

  FuncEntryArray.push_back({SMALLINT_TO_TEXT, "smallint_to_text", STRINGID, {SMALLINTID}, int2_to_text});
  FuncEntryArray.push_back({INT_TO_TEXT, "int_to_text", STRINGID, {INTID}, int4_to_text});
  FuncEntryArray.push_back({BIGINT_TO_TEXT, "bigint_to_text", STRINGID, {BIGINTID}, int8_to_text});
  FuncEntryArray.push_back({FLOAT_TO_TEXT, "float_to_text", STRINGID, {FLOATID}, float4_to_text});
  FuncEntryArray.push_back({DOUBLE_TO_TEXT, "double_to_text", STRINGID, {DOUBLEID}, float8_to_text});
  FuncEntryArray.push_back({DECIMAL_TO_TEXT, "decimal_to_text", STRINGID, {DECIMALNEWID}, decimal_to_text});
  FuncEntryArray.push_back({BOOL_TO_TEXT, "bool_to_text", STRINGID, {BOOLEANID}, bool_to_text});
  FuncEntryArray.push_back({TEXT_TO_CHAR, "text_to_char", TINYINTID, {STRINGID}, text_to_char});
  FuncEntryArray.push_back({INT_TO_CHAR, "int4_to_char", TINYINTID, {INTID}, int4_to_char});
  FuncEntryArray.push_back({DOUBLE_TO_TIMESTAMP, "double_to_timestamp", TIMESTAMPTZID, {DOUBLEID}, double_to_timestamp});

  FuncEntryArray.push_back({CHAR_TO_STRING, "char_to_string", STRINGID, {CHARID},char_to_string, false});
  FuncEntryArray.push_back({DATE_TO_TIMESTAMP, "date_to_timestamp", TIMESTAMPID, {DATEID}, date_to_timestamp, false});
  FuncEntryArray.push_back({TIMESTAMP_TO_DATE, "timestamp_to_date", DATEID, {TIMESTAMPID}, timestamp_to_date, false});
  FuncEntryArray.push_back({IS_TIMESTAMP_FINITE, "is_timestamp_finite", BOOLEANID, {TIMESTAMPID}, is_timestamp_finite, false});
  FuncEntryArray.push_back({TIMESTAMP_DATE_PART, "timestamp_date_part",DOUBLEID , {STRINGID,TIMESTAMPID}, timestamp_date_part, false});
  FuncEntryArray.push_back({TIMESTAMP_TO_TEXT, "timestamp_to_text",STRINGID, {TIMESTAMPID}, timestamp_to_text,false});
  FuncEntryArray.push_back({TIMESTAMP_DATE_TRUNC, "timestamp_date_trunc",TIMESTAMPID, {STRINGID,TIMESTAMPID}, timestamp_date_trunc,false});
  FuncEntryArray.push_back({TIME_SUB_TIME, "time_sub_time", INTERVALID, {TIMEID, TIMEID}, time_sub_time, false});
  FuncEntryArray.push_back({TIMESTAMP_SUB_TIMESTAMP, "timestamp_sub_timestamp", INTERVALID, {TIMESTAMPID, TIMESTAMPID}, timestamp_sub_timestamp, false});
  FuncEntryArray.push_back({DATE_ADD_INTERVAL, "date_add_interval", TIMESTAMPID, {DATEID, INTERVALID}, date_add_interval, false});
  FuncEntryArray.push_back({DATE_SUB_INTERVAL, "date_sub_interval", TIMESTAMPID, {DATEID, INTERVALID}, date_sub_interval, false});
  FuncEntryArray.push_back({TIME_ADD_INTERVAL, "time_add_interval", TIMEID, {TIMEID, INTERVALID}, time_add_interval, false});
  FuncEntryArray.push_back({TIME_SUB_INTERVAL, "time_sub_interval", TIMEID, {TIMEID, INTERVALID}, time_sub_interval, false});
  FuncEntryArray.push_back({TIMESTAMP_ADD_INTERVAL, "timestamp_add_interval", TIMESTAMPID, {TIMESTAMPID, INTERVALID}, timestamp_add_interval, false});
  FuncEntryArray.push_back({TIMESTAMP_SUB_INTERVAL, "timestamp_sub_interval", TIMESTAMPID, {TIMESTAMPID, INTERVALID}, timestamp_sub_interval, false});
  FuncEntryArray.push_back({RANDOMF, "random", DOUBLEID, {}, random_function, false});

  FuncEntryArray.push_back({STRING_OCTET_LENGTH, "string_octet_length", INTID, {STRINGID}, binary_octet_length, false});
  FuncEntryArray.push_back({BPCHAR_OCTET_LENGTH, "bpchar_octet_length", INTID, {CHARID}, binary_octet_length, false});
  FuncEntryArray.push_back({STRING_CHAR_LENGTH, "string_char_length", INTID, {STRINGID}, string_char_length, false});
  FuncEntryArray.push_back({BPCHAR_CHAR_LENGTH, "bpchar_char_length", INTID, {CHARID}, bpchar_char_length, false});
  FuncEntryArray.push_back({STRING_LIKE, "string_like", BOOLEANID, {STRINGID, STRINGID}, string_like, false});
  FuncEntryArray.push_back({BPCHAR_LIKE, "bpchar_like", BOOLEANID, {CHARID, STRINGID}, bpchar_like, false});
  FuncEntryArray.push_back({STRING_NOT_LIKE, "string_not_like", BOOLEANID, {STRINGID, STRINGID}, string_not_like, false});
  FuncEntryArray.push_back({BPCHAR_NOT_LIKE, "bpchar_not_like", BOOLEANID, {CHARID, STRINGID}, bpchar_not_like, false});
  FuncEntryArray.push_back({STRING_SUBSTRING, "string_substring", STRINGID, {STRINGID, INTID, INTID}, string_substring, false});
  FuncEntryArray.push_back({STRING_SUBSTRING_NOLEN, "string_substring_nolen", STRINGID, {STRINGID, INTID}, string_substring_nolen, false});
  FuncEntryArray.push_back({STRING_LOWER, "string_lower", STRINGID, {STRINGID}, string_lower, false});
  FuncEntryArray.push_back({STRING_UPPER, "string_upper", STRINGID, {STRINGID}, string_upper, false});
  FuncEntryArray.push_back({STRING_CONCAT, "string_concat", STRINGID, {STRINGID, STRINGID}, string_concat, false});
  FuncEntryArray.push_back({STRING_POSITION, "string_position", INTID, {STRINGID, STRINGID}, string_position, false});
  FuncEntryArray.push_back({STRING_STRPOS, "string_strpos", INTID, {STRINGID, STRINGID}, string_position, false});
  FuncEntryArray.push_back({STRING_INITCAP, "string_initcap", STRINGID, {STRINGID}, string_initcap, false});
  FuncEntryArray.push_back({STRING_ASCII, "string_ascii", INTID, {STRINGID}, string_ascii, false});
  FuncEntryArray.push_back({STRING_VARCHAR, "string_varchar", STRINGID, {STRINGID, INTID, BOOLEANID}, string_varchar, false});
  FuncEntryArray.push_back({STRING_LTRIM_BLANK, "string_ltrim_blank", STRINGID, {STRINGID}, string_ltrim_blank});
  FuncEntryArray.push_back({STRING_LTRIM_CHARS, "string_ltrim_chars", STRINGID, {STRINGID, STRINGID}, string_ltrim_chars});
  FuncEntryArray.push_back({STRING_RTRIM_BLANK, "string_rtrim_blank", STRINGID, {STRINGID}, string_rtrim_blank});
  FuncEntryArray.push_back({STRING_RTRIM_CHARS, "string_rtrim_chars", STRINGID, {STRINGID, STRINGID}, string_rtrim_chars});
  FuncEntryArray.push_back({STRING_BTRIM_BLANK, "string_btrim_blank", STRINGID, {STRINGID}, string_btrim_blank});
  FuncEntryArray.push_back({STRING_BTRIM_CHARS, "string_btrim_chars", STRINGID, {STRINGID, STRINGID}, string_btrim_chars});
  FuncEntryArray.push_back({STRING_REPEAT, "string_repeat", STRINGID, {STRINGID, INTID}, string_repeat, false});
  FuncEntryArray.push_back({STRING_CHR, "string_chr", STRINGID, {INTID}, string_chr, false});
  FuncEntryArray.push_back({STRING_BPCHAR, "string_bpchar", CHARID, {CHARID, INTID, BOOLEANID}, string_bpchar, false});
  FuncEntryArray.push_back({STRING_LPAD, "string_lpad", STRINGID, {STRINGID, INTID, STRINGID}, string_lpad, false});
  FuncEntryArray.push_back({STRING_RPAD, "string_rpad", STRINGID, {STRINGID, INTID, STRINGID}, string_rpad, false});
  FuncEntryArray.push_back({STRING_LPAD_NOFILL, "string_lpad_nofill", STRINGID, {STRINGID, INTID}, string_lpad_nofill, false});
  FuncEntryArray.push_back({STRING_RPAD_NOFILL, "string_rpad_nofill", STRINGID, {STRINGID, INTID}, string_rpad_nofill, false});
  FuncEntryArray.push_back({STRING_TRANSLATE, "string_translate", STRINGID, {STRINGID, STRINGID, STRINGID}, string_translate, false});

  FuncEntryArray.push_back({BINARY_OCTET_LENGTH, "binary_octet_length", INTID, {BINARYID}, binary_octet_length, false});

  FuncEntryArray.push_back({FLOAT_ARRAY_EUCLIDEAN_METRIC, "float_array_euclidean_metric", FLOATID, {FLOATARRAYID, FLOATARRAYID}, float_array_euclidean_metric, false});
  FuncEntryArray.push_back({DOUBLE_ARRAY_EUCLIDEAN_METRIC, "double_array_euclidean_metric", DOUBLEID, {DOUBLEARRAYID, DOUBLEARRAYID}, double_array_euclidean_metric, false});
  FuncEntryArray.push_back({FLOAT_ARRAY_COSINE_DISTANCE, "float_array_cosine_distance", DOUBLEID, {FLOATARRAYID, FLOATARRAYID}, float_array_cosine_distance, false});
  FuncEntryArray.push_back({DOUBLE_ARRAY_COSINE_DISTANCE, "double_array_cosine_distance", DOUBLEID, {DOUBLEARRAYID, DOUBLEARRAYID}, double_array_cosine_distance, false});

  FuncEntryArray.push_back({BIGINT_ARRAY_OVERLAP, "bigint_array_overlap", BOOLEANID, {BIGINTARRAYID, BIGINTARRAYID}, bigint_array_overlap, false});
  FuncEntryArray.push_back({BIGINT_ARRAY_CONTAINS, "bigint_array_contains", BOOLEANID, {BIGINTARRAYID, BIGINTARRAYID}, bigint_array_contains, false});
  FuncEntryArray.push_back({BIGINT_ARRAY_CONTAINED, "bigint_array_contained", BOOLEANID, {BIGINTARRAYID, BIGINTARRAYID}, bigint_array_contained, false});

  FuncEntryArray.push_back({DOUBLE_ABS, "double_abs", DOUBLEID, { DOUBLEID }, double_abs, false});
  FuncEntryArray.push_back({FLOAT_ABS, "float_abs", FLOATID, { FLOATID }, float_abs, false});
  FuncEntryArray.push_back({INT64_ABS, "int64_abs", BIGINTID, { BIGINTID}, int64_abs, false});
  FuncEntryArray.push_back({INT32_ABS, "int32_abs", INTID, { INTID }, int32_abs, false});
  FuncEntryArray.push_back({INT16_ABS, "int16_abs", SMALLINTID, {SMALLINTID}, int16_abs, false});
  FuncEntryArray.push_back({DOUBLE_CBRT, "double_cbrt", DOUBLEID, { DOUBLEID }, double_cbrt, false});
  FuncEntryArray.push_back({INT16_BINARY_NOT, "int16_binary_not", SMALLINTID, { SMALLINTID }, int16_binary_not, false});
  FuncEntryArray.push_back({INT32_BINARY_NOT, "int32_binary_not", INTID, { INTID }, int32_binary_not, false});
  FuncEntryArray.push_back({INT64_BINARY_NOT, "int64_binary_not", BIGINTID, { BIGINTID }, int64_binary_not, false});
  FuncEntryArray.push_back({INT16_BINARY_SHIFT_LEFT, "int16_binary_shift_left", SMALLINTID, { SMALLINTID, INTID }, int16_binary_shift_left, false});
  FuncEntryArray.push_back({INT32_BINARY_SHIFT_LEFT, "int32_binary_shift_left", INTID, { INTID, INTID }, int32_binary_shift_left, false});
  FuncEntryArray.push_back({INT64_BINARY_SHIFT_LEFT, "int64_binary_shift_left", BIGINTID, { BIGINTID, INTID }, int64_binary_shift_left, false});
  FuncEntryArray.push_back({INT16_BINARY_SHIFT_RIGHT, "int16_binary_shift_right", SMALLINTID, { SMALLINTID, INTID }, int16_binary_shift_right, false});
  FuncEntryArray.push_back({INT32_BINARY_SHIFT_RIGHT, "int32_binary_shift_right", INTID, { INTID, INTID }, int32_binary_shift_right, false});
  FuncEntryArray.push_back({INT64_BINARY_SHIFT_RIGHT, "int64_binary_shift_right", BIGINTID, { BIGINTID, INTID }, int64_binary_shift_right, false});
  FuncEntryArray.push_back({INT16_BINARY_AND, "int16_binary_and", SMALLINTID, { SMALLINTID, SMALLINTID }, int16_binary_and, false});
  FuncEntryArray.push_back({INT32_BINARY_AND, "int32_binary_and", INTID, { INTID, INTID }, int32_binary_and, false});
  FuncEntryArray.push_back({INT64_BINARY_AND, "int64_binary_and", BIGINTID, { BIGINTID, BIGINTID }, int64_binary_and, false});
  FuncEntryArray.push_back({INT16_BINARY_OR, "int16_binary_or", SMALLINTID, { SMALLINTID, SMALLINTID }, int16_binary_or, false});
  FuncEntryArray.push_back({INT32_BINARY_OR, "int32_binary_or", INTID, { INTID, INTID }, int32_binary_or, false});
  FuncEntryArray.push_back({INT64_BINARY_OR, "int64_binary_or", BIGINTID, { BIGINTID, BIGINTID }, int64_binary_or, false});
  FuncEntryArray.push_back({INT16_BINARY_XOR, "int16_binary_xor", SMALLINTID, { SMALLINTID, SMALLINTID }, int16_binary_xor, false});
  FuncEntryArray.push_back({INT32_BINARY_XOR, "int32_binary_xor", INTID, { INTID, INTID }, int32_binary_xor, false});
  FuncEntryArray.push_back({INT64_BINARY_XOR, "int64_binary_xor", BIGINTID, { BIGINTID, BIGINTID }, int64_binary_xor, false});
  FuncEntryArray.push_back({INT16_MOD, "int16_mod", SMALLINTID, { SMALLINTID, SMALLINTID }, int16_mod, false});
  FuncEntryArray.push_back({INT32_MOD, "int32_mod", INTID, { INTID, INTID }, int32_mod, false});
  FuncEntryArray.push_back({INT16_32_MOD, "int16_32_mod", INTID, { SMALLINTID, INTID }, int16_32_mod, false});
  FuncEntryArray.push_back({INT32_16_MOD, "int32_16_mod", INTID, { INTID, SMALLINTID }, int32_16_mod, false});
  FuncEntryArray.push_back({INT64_MOD, "int64_mod", BIGINTID, { BIGINTID, BIGINTID }, int64_mod, false});
  FuncEntryArray.push_back({DOUBLE_SQRT, "double_sqrt", DOUBLEID, { DOUBLEID }, double_sqrt, false});
  FuncEntryArray.push_back({DOUBLE_POW, "double_pow", DOUBLEID, { DOUBLEID, DOUBLEID }, double_pow, false});
  FuncEntryArray.push_back({DOUBLE_CEIL, "double_ceil", DOUBLEID, {DOUBLEID}, double_ceil, false});
  FuncEntryArray.push_back({DOUBLE_FLOOR, "double_floor", DOUBLEID, {DOUBLEID}, double_floor, false});
  FuncEntryArray.push_back({DOUBLE_ROUND, "double_round", DOUBLEID, {DOUBLEID}, double_round, false});
  FuncEntryArray.push_back({DOUBLE_TRUNC, "double_trunc", DOUBLEID, {DOUBLEID}, double_trunc, false});
  FuncEntryArray.push_back({DOUBLE_SIGN, "double_sign", DOUBLEID, {DOUBLEID}, double_sign, false});
  FuncEntryArray.push_back({DOUBLE_EXP, "double_exp", DOUBLEID, {DOUBLEID}, double_exp, false});
  FuncEntryArray.push_back({DOUBLE_LN, "double_ln", DOUBLEID, {DOUBLEID}, double_ln, false});
  FuncEntryArray.push_back({DOUBLE_LG, "double_lg", DOUBLEID, {DOUBLEID}, double_lg, false});
  FuncEntryArray.push_back({DOUBLE_LOG, "double_log", DOUBLEID, {DOUBLEID, DOUBLEID}, double_log, false});
  FuncEntryArray.push_back({DOUBLE_ACOS, "double_acos", DOUBLEID, { DOUBLEID}, double_acos, false});
  FuncEntryArray.push_back({DOUBLE_ASIN, "double_asin", DOUBLEID, { DOUBLEID}, double_asin, false});
  FuncEntryArray.push_back({DOUBLE_ATAN, "double_atan", DOUBLEID, { DOUBLEID}, double_atan, false});
  FuncEntryArray.push_back({DOUBLE_ATAN2, "double_atan2", DOUBLEID, { DOUBLEID, DOUBLEID}, double_atan2, false});
  FuncEntryArray.push_back({DOUBLE_COS, "double_cos", DOUBLEID, { DOUBLEID}, double_cos, false});
  FuncEntryArray.push_back({DOUBLE_COT, "double_cot", DOUBLEID, { DOUBLEID}, double_cot, false});
  FuncEntryArray.push_back({DOUBLE_SIN, "double_sin", DOUBLEID, { DOUBLEID}, double_sin, false});
  FuncEntryArray.push_back({DOUBLE_TAN, "double_tan", DOUBLEID, { DOUBLEID}, double_tan, false});

  FuncEntryArray.push_back({DECIMAL_ABS, "decimal_abs", DECIMALNEWID, {DECIMALNEWID}, decimal_abs, false});
  FuncEntryArray.push_back({DECIMAL_SIGN, "decimal_sign", DECIMALNEWID, {DECIMALNEWID}, decimal_sign, false});
  FuncEntryArray.push_back({DECIMAL_CEIL, "decimal_ceil", DECIMALNEWID, {DECIMALNEWID}, decimal_ceil, false});
  FuncEntryArray.push_back({DECIMAL_FLOOR, "decimal_floor", DECIMALNEWID, {DECIMALNEWID}, decimal_floor, false});
  FuncEntryArray.push_back({DECIMAL_ROUND, "decimal_round", DECIMALNEWID, {DECIMALNEWID, INTID}, decimal_round, false});
  FuncEntryArray.push_back({DECIMAL_ROUND_WITHOUT_SCALE, "decimal_round_without_scale", DECIMALNEWID, {DECIMALNEWID}, decimal_round_without_scale, false});
  FuncEntryArray.push_back({DECIMAL_TRUNC, "decimal_trunc", DECIMALNEWID, {DECIMALNEWID, INTID}, decimal_trunc, false});
  FuncEntryArray.push_back({DECIMAL_TRUNC_WITHOUT_SCALE, "decimal_trunc_without_scale", DECIMALNEWID, {DECIMALNEWID, INTID}, decimal_trunc_without_scale, false});
  FuncEntryArray.push_back({DECIMAL_TO_DECIMAL, "decimal_to_decimal", DECIMALNEWID, {DECIMALNEWID, INTID}, decimal_to_decimal, false});
  FuncEntryArray.push_back({DECIMAL_MOD, "decimal_mod", DECIMALNEWID, {DECIMALNEWID, DECIMALNEWID}, decimal_mod, false});
  FuncEntryArray.push_back({DECIMAL_SQRT, "decimal_sqrt", DECIMALNEWID, { DECIMALNEWID }, decimal_sqrt, false});
  FuncEntryArray.push_back({DECIMAL_EXP, "decimal_exp", DECIMALNEWID, { DECIMALNEWID }, decimal_exp, false});
  FuncEntryArray.push_back({DECIMAL_LN, "decimal_ln", DECIMALNEWID, { DECIMALNEWID }, decimal_ln, false});
  FuncEntryArray.push_back({DECIMAL_LOG, "decimal_log", DECIMALNEWID, { DECIMALNEWID, DECIMALNEWID}, decimal_log, false});
  FuncEntryArray.push_back({DECIMAL_FAC, "decimal_fac", DECIMALNEWID, { BIGINTID }, decimal_fac, false});
  FuncEntryArray.push_back({DECIMAL_POW, "decimal_pow", DECIMALNEWID, { DECIMALNEWID, DECIMALNEWID }, decimal_pow, false});

  FuncEntryArray.push_back({TEXT_TO_SMALLINT, "text_smallint", SMALLINTID, {STRINGID}, text_to_int2});
  FuncEntryArray.push_back({TEXT_TO_INT, "text_int", INTID, {STRINGID}, text_to_int4});
  FuncEntryArray.push_back({TEXT_TO_BIGINT, "text_bigint", BIGINTID, {STRINGID}, text_to_int8});
  FuncEntryArray.push_back({TEXT_TO_FLOAT, "text_float", FLOATID, {STRINGID}, text_to_float4});
  FuncEntryArray.push_back({TEXT_TO_DOUBLE, "text_double", DOUBLEID, {STRINGID}, text_to_float8});
  FuncEntryArray.push_back{TEXT_TO_DECIMAL, "text_decimal", DECIMALNEWID, {STRINGID}, textToDecimal});
  FuncEntryArray.push_back({TO_NUMBER, "to_number", DECIMALNEWID, {STRINGID, STRINGID}, toNumber});
  FuncEntryArray.push_back({INTERVAL_TO_TEXT, "interval_text", STRINGID, {INTERVALID}, intervalToText});

  FuncEntryArray.push_back({BOOLEAN_TO_SMALLINT, "boolean_smallint", SMALLINTID, {BOOLEANID}, bool_to_int2});
  FuncEntryArray.push_back({BOOLEAN_TO_INT, "boolean_int", INTID, {BOOLEANID}, bool_to_int4});
  FuncEntryArray.push_back({BOOLEAN_TO_BIGINT, "boolean_bigint", BIGINTID, {BOOLEANID}, bool_to_int8});
  FuncEntryArray.push_back({SMALLINT_TO_BOOLEAN, "smallint_boolean", BOOLEANID, {SMALLINTID}, int2_to_bool});
  FuncEntryArray.push_back({INT_TO_BOOLEAN, "int_boolean", BOOLEANID, {INTID}, int4_to_bool});
  FuncEntryArray.push_back({BIGINT_TO_BOOLEAN, "bigint_boolean", BOOLEANID, {BIGINTID}, int8_to_bool});
  // clang-format on

  std::sort(FuncEntryArray.begin(), FuncEntryArray.end(),
            [](const FuncEntry &a, const FuncEntry &b) {
              return a.funcId < b.funcId;
            });

  //
  // Check if the function table is continuous
  //
  for (uint64_t i = 0; i < FuncEntryArray.size(); ++i) {
    if (i != FuncEntryArray[i].funcId) {
      assert(false && "function table is not ordered");
      std::terminate();
    }
  }

  //
  // Setup function name map
  //
  for (uint64_t i = 0; i < FuncEntryArray.size(); ++i) {
    FuncEntryMap.insert(
        std::make_pair(FuncEntryArray[i].funcName, FuncEntryArray[i]));
  }
}

const FuncEntry *Func::getFuncEntryById(FuncKind id) {
  assert(id >= 0 && id < FuncEntryArray.size());
  return &FuncEntryArray[id];
}

bool Func::hasFuncEntryById(FuncKind id) {
  return id < FuncEntryArray.size() ? true : false;
}

const FuncEntry *Func::getFuncEntryByName(const std::string &name) {
  const FuncMap::const_iterator iter = FuncEntryMap.find(name);

  if (iter != FuncEntryMap.end()) {
    return &(iter->second);
  }

  return nullptr;
}

void Func::setupAggTable() {
  AggEntryArray = {
      // clang-format off
      {AVG_TINYINT, AVG_TINYINT_ACCU, AVG_TINYINT_AMALG, AVG_TINYINT_AVG, FUNCINVALID, DOUBLEID, false, false},
      {AVG_SMALLINT, AVG_SMALLINT_ACCU, AVG_SMALLINT_AMALG, AVG_SMALLINT_AVG, FUNCINVALID, DOUBLEID, false, false},
      {AVG_INT, AVG_INT_ACCU, AVG_INT_AMALG, AVG_INT_AVG, FUNCINVALID, DOUBLEID, false, false},
      {AVG_BIGINT, AVG_BIGINT_ACCU, AVG_BIGINT_AMALG, AVG_BIGINT_AVG, FUNCINVALID, DOUBLEID, false, false},
      {AVG_FLOAT, AVG_FLOAT_ACCU, AVG_FLOAT_AMALG, AVG_FLOAT_AVG, FUNCINVALID, DOUBLEID, false, false},
      {AVG_DOUBLE, AVG_DOUBLE_ACCU, AVG_DOUBLE_AMALG, AVG_DOUBLE_AVG, FUNCINVALID, DOUBLEID, false, false},
      {AVG_DECIMAL, AVG_DECIMAL_ACCU, AVG_DECIMAL_AMALG, AVG_DECIMAL_AVG, FUNCINVALID, DECIMALNEWID, false, false},

      {SUM_TINYINT, SUM_TINYINT_ADD, SUM_TINYINT_SUM, FUNCINVALID, FUNCINVALID, BIGINTID, false, false},
      {SUM_SMALLINT, SUM_SMALLINT_ADD, SUM_SMALLINT_SUM, FUNCINVALID, FUNCINVALID, BIGINTID, false, false},
      {SUM_INT, SUM_INT_ADD, SUM_INT_SUM, FUNCINVALID, FUNCINVALID, BIGINTID, false, false},
      {SUM_BIGINT, SUM_BIGINT_ADD, SUM_BIGINT_SUM, FUNCINVALID, FUNCINVALID, BIGINTID, false, false},
      {SUM_FLOAT, SUM_FLOAT_ADD, SUM_FLOAT_SUM, FUNCINVALID, FUNCINVALID, DOUBLEID, false, false},
      {SUM_DOUBLE, SUM_DOUBLE_ADD, SUM_DOUBLE_SUM, FUNCINVALID, FUNCINVALID, DOUBLEID, false, false},
      {SUM_DECIMAL, SUM_DECIMAL_ADD, SUM_DECIMAL_SUM, FUNCINVALID, FUNCINVALID, DECIMALNEWID, false, false},

      {COUNT, COUNT_INC, COUNT_ADD, FUNCINVALID, FUNCINVALID, BIGINTID, true, false},
      {COUNT_STAR, COUNT_STAR, COUNT_ADD, FUNCINVALID, FUNCINVALID, BIGINTID, true, false},

      {MIN_TINYINT, MIN_TINYINT_SMALLER, MIN_TINYINT_SMALLER, FUNCINVALID, FUNCINVALID, TINYINTID, false, false},
      {MIN_SMALLINT, MIN_SMALLINT_SMALLER, MIN_SMALLINT_SMALLER, FUNCINVALID, FUNCINVALID, SMALLINTID, false, false},
      {MIN_INT, MIN_INT_SMALLER, MIN_INT_SMALLER, FUNCINVALID, FUNCINVALID, INTID, false, false},
      {MIN_BIGINT, MIN_BIGINT_SMALLER, MIN_BIGINT_SMALLER, FUNCINVALID, FUNCINVALID, BIGINTID, false, false},
      {MIN_FLOAT, MIN_FLOAT_SMALLER, MIN_FLOAT_SMALLER, FUNCINVALID, FUNCINVALID, FLOATID, false, false},
      {MIN_DOUBLE, MIN_DOUBLE_SMALLER, MIN_DOUBLE_SMALLER, FUNCINVALID, FUNCINVALID, DOUBLEID, false, false},
      {MIN_STRING, MIN_STRING_SMALLER, MIN_STRING_SMALLER, FUNCINVALID, FUNCINVALID, STRINGID, false, false},
      {MIN_BPCHAR, MIN_BPCHAR_SMALLER, MIN_BPCHAR_SMALLER, FUNCINVALID, FUNCINVALID, CHARID, false, false},
      {MIN_DATE, MIN_DATE_SMALLER, MIN_DATE_SMALLER, FUNCINVALID, FUNCINVALID, DATEID, false, false},
      {MIN_TIME, MIN_TIME_SMALLER, MIN_TIME_SMALLER, FUNCINVALID, FUNCINVALID, TIMEID, false, false},
      {MIN_TIMESTAMP, MIN_TIMESTAMP_SMALLER, MIN_TIMESTAMP_SMALLER, FUNCINVALID, FUNCINVALID, TIMESTAMPID, false, false},
      {MIN_DECIMAL, MIN_DECIMAL_SMALLER, MIN_DECIMAL_SMALLER, FUNCINVALID, FUNCINVALID, DECIMALNEWID, false, false},

      {MAX_TINYINT, MAX_TINYINT_LARGER, MAX_TINYINT_LARGER, FUNCINVALID, FUNCINVALID, TINYINTID, false, false},
      {MAX_SMALLINT, MAX_SMALLINT_LARGER, MAX_SMALLINT_LARGER, FUNCINVALID, FUNCINVALID, SMALLINTID, false, false},
      {MAX_INT, MAX_INT_LARGER, MAX_INT_LARGER, FUNCINVALID, FUNCINVALID, INTID, false, false},
      {MAX_BIGINT, MAX_BIGINT_LARGER, MAX_BIGINT_LARGER, FUNCINVALID, FUNCINVALID, BIGINTID, false, false},
      {MAX_FLOAT, MAX_FLOAT_LARGER, MAX_FLOAT_LARGER, FUNCINVALID, FUNCINVALID, FLOATID, false, false},
      {MAX_DOUBLE, MAX_DOUBLE_LARGER, MAX_DOUBLE_LARGER, FUNCINVALID, FUNCINVALID, DOUBLEID, false, false},
      {MAX_STRING, MAX_STRING_LARGER, MAX_STRING_LARGER, FUNCINVALID, FUNCINVALID, STRINGID, false, false},
      {MAX_BPCHAR, MAX_BPCHAR_LARGER, MAX_BPCHAR_LARGER, FUNCINVALID, FUNCINVALID, CHARID, false, false},
      {MAX_DATE, MAX_DATE_LARGER, MAX_DATE_LARGER, FUNCINVALID, FUNCINVALID, DATEID, false, false},
      {MAX_TIME, MAX_TIME_LARGER, MAX_TIME_LARGER, FUNCINVALID, FUNCINVALID, TIMEID, false, false},
      {MAX_TIMESTAMP, MAX_TIMESTAMP_LARGER, MAX_TIMESTAMP_LARGER, FUNCINVALID, FUNCINVALID, TIMESTAMPID, false, false},
      {MAX_DECIMAL, MAX_DECIMAL_LARGER, MAX_DECIMAL_LARGER, FUNCINVALID, FUNCINVALID, DECIMALNEWID, false, false},
      // clang-format on
  };

  uint64_t size = AggEntryArray.size();
  for (uint64_t i = 0; i < size; ++i) {
    AggEntryMap.insert(
        std::make_pair(AggEntryArray[i].aggFnId, AggEntryArray[i]));
  }
}

const AggEntry *Func::getAggEntryById(FuncKind id) {
  const AggMap::const_iterator iter = AggEntryMap.find(id);

  if (iter != AggEntryMap.end()) {
    return &(iter->second);
  }

  return nullptr;
}

Func *Func::instance() { return inst; }

}  // namespace dbcommon
