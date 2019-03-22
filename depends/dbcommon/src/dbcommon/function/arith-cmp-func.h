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

#ifndef DBCOMMON_SRC_DBCOMMON_FUNCTION_ARITH_CMP_FUNC_H_
#define DBCOMMON_SRC_DBCOMMON_FUNCTION_ARITH_CMP_FUNC_H_

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

#include <vector>

#include "dbcommon/function/invoker.h"
#include "dbcommon/common/vector.h"

namespace dbcommon {

// [[[cog
#if false
def generate_function_decl(opmap, ltypemap, rtypemap) :
  for op_key in opmap:
    for ta in ltypemap:
      for tb in rtypemap:
        cog.out("""
Datum %(t1)s_%(op)s_%(t2)s (Datum *params, uint64_t size);
Datum %(t1)s_val_%(op)s_%(t2)s_val (Datum *params, uint64_t size);
Datum %(t1)s_val_%(op)s_%(t2)s_vec (Datum *params, uint64_t size);
Datum %(t1)s_vec_%(op)s_%(t2)s_val (Datum *params, uint64_t size);
Datum %(t1)s_vec_%(op)s_%(t2)s_vec (Datum *params, uint64_t size);
""" % {'t1': ltypemap[ta].name, 't1_type':ltypemap[ta].symbol, 'op': opmap[op_key].name, 'opsym': opmap[op_key].symbol, 't2': rtypemap[tb].name, 't2_type':rtypemap[tb].symbol, })
#endif
// ]]]
// [[[end]]]

// [[[cog
#if false
cog.outl("//%s:%d" % (cog.inFile, cog.firstLineNum))
generate_function_decl(CMP_OP, NUMERIC_TYPES, NUMERIC_TYPES)
#endif
// ]]]
// [[[end]]]

// [[[cog
#if false
cog.outl("//%s:%d" % (cog.inFile, cog.firstLineNum))
generate_function_decl(CMP_OP, BINARY_TYPES, BINARY_TYPES)
#endif
// ]]]
// [[[end]]]

// [[[cog
#if false
cog.outl("//%s:%d" % (cog.inFile, cog.firstLineNum))
generate_function_decl(CMP_OP, BOOL_TYPE, BOOL_TYPE)
#endif
// ]]]
// [[[end]]]

// [[[cog
#if false
cog.outl("//%s:%d" % (cog.inFile, cog.firstLineNum))
generate_function_decl(CMP_OP, TIMESTAMP_TYPE, TIMESTAMP_TYPE)
generate_function_decl(CMP_OP, INTERVAL_TYPE, INTERVAL_TYPE)
#endif
// ]]]
// [[[end]]]
// [[[cog
#if false
cog.outl("//%s:%d" % (cog.inFile, cog.firstLineNum))
generate_function_decl(CMP_OP, DECIMAL_TYPE, DECIMAL_TYPE)
#endif
// ]]]
// [[[end]]]
// [[[cog

#if false
cog.outl("//%s:%d" % (cog.inFile, cog.firstLineNum))
generate_function_decl(ARITH_OP, NUMERIC_TYPES, NUMERIC_TYPES)
#endif
// ]]]
// [[[end]]]

// [[[cog
#if false
cog.outl("//%s:%d" % (cog.inFile, cog.firstLineNum))
generate_function_decl(ARITH_OP, DECIMAL_TYPE, DECIMAL_TYPE)
#endif
// ]]]
// [[[end]]]

// [[[cog
#if false
cog.outl("//%s:%d" % (cog.inFile, cog.firstLineNum))
generate_function_decl(ARITH_OP, INTERVAL_TYPE, INTERVAL_TYPE)
#endif
// ]]]
// [[[end]]]

}  // namespace dbcommon

#endif  // DBCOMMON_SRC_DBCOMMON_FUNCTION_ARITH_CMP_FUNC_H_
