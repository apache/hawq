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

#ifndef DBCOMMON_SRC_DBCOMMON_FUNCTION_TYPECAST_FUNC_H_
#define DBCOMMON_SRC_DBCOMMON_FUNCTION_TYPECAST_FUNC_H_

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
#endif
// ]]]
// [[[end]]]

// clang-format on

#include "dbcommon/nodes/datum.h"
#include "dbcommon/common/vector.h"
#include "dbcommon/type/decimal.h"

namespace dbcommon {

// [[[cog
#if false
def generate_typecast_decl(typemap) :
  typeList = sorted(typemap.keys())
  for i in range(len(typeList)):
    for j in xrange(len(typeList)):
      if i != j:
        funcImpl = "%s_to_%s" % (typemap[typeList[i]].name, typemap[typeList[j]].name)
        cog.out("""
Datum %s (Datum *params, uint64_t size);
""" % funcImpl)
#endif
// ]]]
// [[[end]]]

// [[[cog
#if false
cog.outl("//%s:%d" % (cog.inFile, cog.firstLineNum))
generate_typecast_decl(NUMERIC_AND_DECIMAL_TYPES)
#endif
// ]]]
// [[[end]]]

Datum decimal_to_decimal(Datum *params, uint64_t size);
Datum valCastDecimalToDecimal(DecimalVar *param, int64_t typemod);
Datum vecCastDecimalToDecimal(Vector *rvec, Vector *vec, int64_t typemod);
Datum char_to_string(Datum *params, uint64_t size);
Datum bool_to_int2(Datum *params, uint64_t size);
Datum bool_to_int4(Datum *params, uint64_t size);
Datum bool_to_int8(Datum *params, uint64_t size);
Datum int2_to_bool(Datum *params, uint64_t size);
Datum int4_to_bool(Datum *params, uint64_t size);
Datum int8_to_bool(Datum *params, uint64_t size);

}  // namespace dbcommon

#endif  // DBCOMMON_SRC_DBCOMMON_FUNCTION_TYPECAST_FUNC_H_
