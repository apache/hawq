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

#include "dbcommon/function/arith-cmp-func.cg.h"

#include <cassert>
#include <cmath>

#include "dbcommon/common/vector.h"
#include "dbcommon/common/vector/decimal-vector.h"
#include "dbcommon/common/vector/variable-length-vector.h"
#include "dbcommon/function/arithmetic-function.h"
#include "dbcommon/utils/macro.h"

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

namespace dbcommon {

// [[[cog
#if false

def generate_function_with_rettype_def(optype, opmap, ltypemap, rtypemap) :
  for op_key in opmap:
    for ta in ltypemap:
      for tb in rtypemap:
        rettype = get_func_return_type(ta, tb, op_key)
        is_div = "false"
        is_needof = "false"
        if opmap[op_key].name == "div":
            is_div = "true"
        if (opmap[op_key].name == "add" or opmap[op_key].name == "sub" or opmap[op_key].name == "mul") and (rettype.symbol == "int16_t" or rettype.symbol == "int32_t" or rettype.symbol == "int64_t" or rettype.symbol == "float" or rettype.symbol == "double"):
            is_needof = "true"
        cog.outl("""
Datum %(t1)s_%(op)s_%(t2)s(Datum *params, uint64_t size) {
  return type1_op_type2<%(t1_type)s, std::%(function_obj)s<%(rt_type)s>, %(t2_type)s, %(rt_type)s, %(is_div)s, %(is_needof)s>(params, size);
}
""" % {'t1': ltypemap[ta].name,'t1_type':ltypemap[ta].symbol, 'op': opmap[op_key].name, 'opsym': opmap[op_key].symbol, 't2': rtypemap[tb].name, 't2_type': rtypemap[tb].symbol, 'rt_type': rettype.symbol,
'function_obj': opmap[op_key].functionobj, 'is_div': is_div , 'is_needof': is_needof
})
#endif
// ]]]
// [[[end]]]

// [[[cog
#if false
cog.outl("//%s:%d" % (cog.inFile,cog.firstLineNum))
generate_function_with_rettype_def("ArithOp", ARITH_OP, NUMERIC_TYPES, NUMERIC_TYPES)
#endif
// ]]]
// [[[end]]]

}  // namespace dbcommon
