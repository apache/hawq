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

#include "dbcommon/common/vector.h"
#include "dbcommon/function/comparison-function.h"
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
def generate_function_def_wrapper(optype, opmap, ltypemap, rtypemap) :
  for op_key in opmap:
    for ta in ltypemap:
      for tb in rtypemap:
        cog.outl("""
Datum %(t1)s_%(op)s_%(t2)s (Datum *params, uint64_t size) {
  return type1_cmp_type2_bind<
      %(t1)s_vec_%(op)s_%(t2)s_vec,
      %(t1)s_vec_%(op)s_%(t2)s_val,
      %(t1)s_val_%(op)s_%(t2)s_vec,
      %(t1)s_val_%(op)s_%(t2)s_val>(params, size);
}
""" % {'t1': ltypemap[ta].name, 't1_type':ltypemap[ta].symbol, 'op': opmap[op_key].name, 'opsym': opmap[op_key].symbol, 't2': rtypemap[tb].name, 't2_type':rtypemap[tb].symbol,})

def generate_function_def(optype, opmap, ltypemap, rtypemap) :
  for op_key in opmap:
    for ta in ltypemap:
      for tb in rtypemap:
        cog.outl("""
Datum %(t1)s_%(op)s_%(t2)s(Datum *params, uint64_t size) {
  return type1_cmp_type2<%(t1_type)s, std::%(function_obj)s<std::common_type<%(t1_type)s, %(t2_type)s>::type>, %(t2_type)s>(params, size);
}
""" % {'t1': ltypemap[ta].name, 't1_type':ltypemap[ta].symbol, 'op': opmap[op_key].name, 'opsym': opmap[op_key].symbol, 't2': rtypemap[tb].name, 't2_type':rtypemap[tb].symbol,
'function_obj': opmap[op_key].functionobj
})
#endif
// ]]]
// [[[end]]]

// [[[cog
#if false
def generate_function_def_timestamp(optype, opmap, ltypemap, rtypemap) :
  for op_key in opmap:
    for ta in ltypemap:
      for tb in rtypemap:
        cog.outl("""
Datum %(t1)s_val_%(op)s_%(t2)s_val (Datum *params, uint64_t size) {
  assert(size == 3 && "invalid input");
  auto retval = DatumGetValue<Scalar*>(params[0]);
  auto scalar1 = DatumGetValue<Scalar*>(params[1]);
  auto scalar2 = DatumGetValue<Scalar*>(params[2]);
  Timestamp *value1 = DatumGetValue<Timestamp *>(scalar1->value);
  Timestamp *value2 = DatumGetValue<Timestamp *>(scalar2->value);
  auto result = (*value1) %(opsym)s (*value2);
  retval->value = CreateDatum(static_cast<bool>(result));
  return CreateDatum(retval);
}

Datum %(t1)s_val_%(op)s_%(t2)s_vec (Datum *params, uint64_t size) {
  assert(size == 3 && "invalid input");
  auto *selected = DatumGetValue<SelectList*>(params[0]);
  auto para = DatumGetValue<Scalar*>(params[1]);
  Timestamp *val = DatumGetValue<Timestamp *>(para->value);
  auto *vec = DatumGetValue<Vector *>(params[2]);
  const %(t1_type)s * v = (const %(t1_type)s*)(vec->getValue());  //NOLINT
  const %(t1_type)s * nano = (const %(t1_type)s*)(vec->getNanoseconds());  //NOLINT
  auto *sel = vec->getSelected();
  auto sz = vec->getNumOfRows();

  selected->clear();
  selected->setNulls(vec->getNumOfRowsPlain(), sel, selected->getNulls(), vec->getNulls());
  bool hasNull = vec->hasNullValue();
  if (sel) {
    if (hasNull) {
      const bool *nulls = vec->getNullBuffer()->getBools();
      for (uint64_t i = 0 ; i < sz; ++i) {
        auto index = (*sel)[i];
        if (!nulls[index]) {
          Timestamp ts = Timestamp(v[index], nano[index]);
          bool isSelected = (*val) %(opsym)s ts;
          if (isSelected) selected->push_back(index);
        }
      }
    } else {
      for (uint64_t i = 0 ; i < sz; ++i) {
        auto index = (*sel)[i];
        Timestamp ts = Timestamp(v[index], nano[index]);
        bool isSelected = (*val) %(opsym)s ts;
        if (isSelected) selected->push_back(index);
      }
    }
  } else {
    if (hasNull) {
      const bool *nulls = vec->getNullBuffer()->getBools();
#pragma clang loop unroll(full)
      for (uint64_t i = 0 ; i < sz; ++i) {
        if (!nulls[i]) {
          Timestamp ts = Timestamp(v[i], nano[i]);
          bool isSelected = (*val) %(opsym)s ts;
          if (isSelected) selected->push_back(i);
        }
      }
    } else {
#pragma clang loop unroll(full)
      for (uint64_t i = 0 ; i < sz; ++i) {
        Timestamp ts = Timestamp(v[i], nano[i]);
        bool isSelected = (*val) %(opsym)s ts;
        if (isSelected) selected->push_back(i);

      }
    }
  }

  return CreateDatum(selected);
}
Datum %(t1)s_vec_%(op)s_%(t2)s_val (Datum *params, uint64_t size) {
  assert(size == 3 && "invalid input");
  auto *selected = DatumGetValue<SelectList*>(params[0]);
  auto para = DatumGetValue<Scalar*>(params[2]);
  Timestamp* val = DatumGetValue<Timestamp *>(para->value);
  auto *vec = DatumGetValue<Vector *>(params[1]);
  const %(t1_type)s * v = (const %(t1_type)s*)(vec->getValue());  //NOLINT
  const %(t1_type)s * nano = (const %(t1_type)s*)(vec->getNanoseconds());  //NOLINT
  auto *sel = vec->getSelected();
  auto sz = vec->getNumOfRows();

  bool hasNull = vec->hasNullValue();

  selected->clear();
  selected->setNulls(vec->getNumOfRowsPlain(), sel, selected->getNulls(), vec->getNulls());
  if (sel) {
    if (hasNull) {
      const bool *nulls = vec->getNullBuffer()->getBools();
      for (uint64_t i = 0 ; i < sz; ++i) {
        auto index = (*sel)[i];
        if (!nulls[index]) {
          Timestamp ts = Timestamp(v[index], nano[index]);
          bool isSelected = ts %(opsym)s (*val);
          if (isSelected) selected->push_back(index);
        }
      }
    } else {
      for (uint64_t i = 0 ; i < sz; ++i) {
        auto index = (*sel)[i];
        Timestamp ts = Timestamp(v[index], nano[index]);
        bool isSelected = ts %(opsym)s (*val);
        if (isSelected) selected->push_back(index);
      }
    }
  } else {
    if (hasNull) {
      const bool *nulls = vec->getNullBuffer()->getBools();
#pragma clang loop unroll(full)
      for (uint64_t i = 0 ; i < sz; ++i) {
        if (!nulls[i]) {
          Timestamp ts = Timestamp(v[i], nano[i]);
          bool isSelected = ts %(opsym)s (*val);
          if (isSelected) selected->push_back(i);
        }
      }
    } else {
#pragma clang loop unroll(full)
      for (uint64_t i = 0 ; i < sz; ++i) {
        Timestamp ts = Timestamp(v[i], nano[i]);
        bool isSelected = ts %(opsym)s (*val);
        if (isSelected) selected->push_back(i);
      }
    }
  }

  return CreateDatum(selected);
}
Datum %(t1)s_vec_%(op)s_%(t2)s_vec (Datum *params, uint64_t size) {
  assert(size == 3 && "invalid input");
  auto *selected = DatumGetValue<SelectList*>(params[0]);
  auto *vec1 = DatumGetValue<Vector *>(params[1]);
  auto *vec2 = DatumGetValue<Vector *>(params[2]);
  const %(t1_type)s * v1 = (const %(t1_type)s*)(vec1->getValue());  //NOLINT
  const %(t1_type)s * nano1 = (const %(t1_type)s*)(vec1->getNanoseconds());  //NOLINT
  const %(t2_type)s * v2 = (const %(t2_type)s*)(vec2->getValue());  //NOLINT
  const %(t2_type)s * nano2 = (const %(t2_type)s*)(vec2->getNanoseconds());  //NOLINT
  auto *sel1 = vec1->getSelected();
  auto *sel2 = vec2->getSelected();

  auto sz = vec1->getNumOfRows();
  assert(vec1->getNumOfRows() == vec2->getNumOfRows() && "invalid parameter");

  bool hasNull1 = vec1->hasNullValue();
  bool hasNull2 = vec2->hasNullValue();

  selected->clear();
  selected->setNulls(vec1->getNumOfRowsPlain(), sel1, selected->getNulls(), vec1->getNulls(), vec2->getNulls());
  if (sel1) {
     assert(*sel1 == *sel2);
     if (!hasNull1 && !hasNull2) {
		#pragma clang loop unroll(full)
       for (uint64_t i = 0 ; i < sz; ++i) {
          auto index = (*sel1)[i];
          Timestamp ts1 = Timestamp(v1[index], nano1[index]);
          Timestamp ts2 = Timestamp(v2[index], nano2[index]);
          bool isSelected = ts1 %(opsym)s ts2;
					if (isSelected) selected->push_back(index);
        }
      }
      else {
		#pragma clang loop unroll(full)
      for (uint64_t i = 0 ; i < sz; ++i) {
          auto index = (*sel1)[i];
					if (!vec1->isNullPlain(index) && !vec2->isNullPlain(index)) {
					  Timestamp ts1 = Timestamp(v1[index], nano1[index]);
					  Timestamp ts2 = Timestamp(v2[index], nano2[index]);
					  bool isSelected = ts1 %(opsym)s ts2;
						if (isSelected) selected->push_back(index);
					}
        }
      }
    } else {
		if (!hasNull1 && !hasNull2) {
   #pragma clang loop unroll(full)
			for (uint64_t i = 0 ; i < sz; ++i) {
			  Timestamp ts1 = Timestamp(v1[i], nano1[i]);
			  Timestamp ts2 = Timestamp(v2[i], nano2[i]);
			  bool isSelected = ts1 %(opsym)s ts2;
				if (isSelected) selected->push_back(i);
			}
		}
		else {
	#pragma clang loop unroll(full)
			for (uint64_t i = 0 ; i < sz; ++i) {
				if (!vec1->isNull(i) && !vec2->isNull(i)) {
				  Timestamp ts1 = Timestamp(v1[i], nano1[i]);
				  Timestamp ts2 = Timestamp(v2[i], nano2[i]);
				  bool isSelected = ts1 %(opsym)s ts2;
					if (isSelected) selected->push_back(i);
				}
			}
		}
  }

  return CreateDatum(selected);
}
""" % {'t1': ltypemap[ta].name, 't1_type':ltypemap[ta].symbol, 'op': opmap[op_key].name, 'opsym': opmap[op_key].symbol, 't2': rtypemap[tb].name, 't2_type':rtypemap[tb].symbol,})
#endif
// ]]]
// [[[end]]]


// [[[cog
#if false
def generate_function_def_interval(optype, opmap, ltypemap, rtypemap) :
  for op_key in opmap:
    for ta in ltypemap:
      for tb in rtypemap:
        cog.outl("""
Datum %(t1)s_val_%(op)s_%(t2)s_val (Datum *params, uint64_t size) {
  assert(size == 3 && "invalid input");
  auto retval = DatumGetValue<Scalar*>(params[0]);
  auto scalar1 = DatumGetValue<Scalar*>(params[1]);
  auto scalar2 = DatumGetValue<Scalar*>(params[2]);
  IntervalVar *value1 = DatumGetValue<IntervalVar *>(scalar1->value);
  IntervalVar *value2 = DatumGetValue<IntervalVar *>(scalar2->value);
  auto result = (*value1) %(opsym)s (*value2);
  retval->value = CreateDatum(static_cast<bool>(result));
  return CreateDatum(retval);
}

Datum %(t1)s_val_%(op)s_%(t2)s_vec (Datum *params, uint64_t size) {
  assert(size == 3 && "invalid input");
  auto *selected = DatumGetValue<SelectList*>(params[0]);
  auto para = DatumGetValue<Scalar*>(params[1]);
  IntervalVar *val = DatumGetValue<IntervalVar *>(para->value);
  auto *vec = DatumGetValue<Vector *>(params[2]);
  const int64_t* v = (const int64_t*)(vec->getValue());  //NOLINT
  const int32_t* days = (const int32_t*)(vec->getDayValue());  //NOLINT
  const int32_t* months = (const int32_t*)(vec->getMonthValue());
  auto *sel = vec->getSelected();
  auto sz = vec->getNumOfRows();

  selected->clear();
  selected->setNulls(vec->getNumOfRowsPlain(), sel, selected->getNulls(), vec->getNulls());
  bool hasNull = vec->hasNullValue();
  if (sel) {
    if (hasNull) {
      const bool *nulls = vec->getNullBuffer()->getBools();
      for (uint64_t i = 0 ; i < sz; ++i) {
        auto index = (*sel)[i];
        if (!nulls[index]) {
          IntervalVar itv = IntervalVar(v[index], days[index], months[index]);
          bool isSelected = (*val) %(opsym)s itv;
          if (isSelected) selected->push_back(index);
        }
      }
    } else {
      for (uint64_t i = 0 ; i < sz; ++i) {
        auto index = (*sel)[i];
        IntervalVar itv = IntervalVar(v[index], days[index], months[index]);
        bool isSelected = (*val) %(opsym)s itv;
        if (isSelected) selected->push_back(index);
      }
    }
  } else {
    if (hasNull) {
      const bool *nulls = vec->getNullBuffer()->getBools();
#pragma clang loop unroll(full)
      for (uint64_t i = 0 ; i < sz; ++i) {
        if (!nulls[i]) {
          IntervalVar itv = IntervalVar(v[i], days[i], months[i]);
          bool isSelected = (*val) %(opsym)s itv;
          if (isSelected) selected->push_back(i);
        }
      }
    } else {
#pragma clang loop unroll(full)
      for (uint64_t i = 0 ; i < sz; ++i) {
        IntervalVar itv = IntervalVar(v[i], days[i], months[i]);
        bool isSelected = (*val) %(opsym)s itv;
        if (isSelected) selected->push_back(i);

      }
    }
  }

  return CreateDatum(selected);
}
Datum %(t1)s_vec_%(op)s_%(t2)s_val (Datum *params, uint64_t size) {
  assert(size == 3 && "invalid input");
  auto *selected = DatumGetValue<SelectList*>(params[0]);
  auto para = DatumGetValue<Scalar*>(params[2]);
  IntervalVar* val = DatumGetValue<IntervalVar *>(para->value);
  auto *vec = DatumGetValue<Vector *>(params[1]);
  const int64_t* v = (const int64_t*)(vec->getValue());  //NOLINT
  const int32_t* days = (const int32_t*)(vec->getDayValue());  //NOLINT
  const int32_t* months = (const int32_t*)(vec->getMonthValue());
  auto *sel = vec->getSelected();
  auto sz = vec->getNumOfRows();

  bool hasNull = vec->hasNullValue();

  selected->clear();
  selected->setNulls(vec->getNumOfRowsPlain(), sel, selected->getNulls(), vec->getNulls());
  if (sel) {
    if (hasNull) {
      const bool *nulls = vec->getNullBuffer()->getBools();
      for (uint64_t i = 0 ; i < sz; ++i) {
        auto index = (*sel)[i];
        if (!nulls[index]) {
          IntervalVar itv = IntervalVar(v[index], days[index], months[index]);
          bool isSelected = itv %(opsym)s (*val);
          if (isSelected) selected->push_back(index);
        }
      }
    } else {
      for (uint64_t i = 0 ; i < sz; ++i) {
        auto index = (*sel)[i];
        IntervalVar itv = IntervalVar(v[index], days[index], months[index]);
        bool isSelected = itv %(opsym)s (*val);
        if (isSelected) selected->push_back(index);
      }
    }
  } else {
    if (hasNull) {
      const bool *nulls = vec->getNullBuffer()->getBools();
#pragma clang loop unroll(full)
      for (uint64_t i = 0 ; i < sz; ++i) {
        if (!nulls[i]) {
          IntervalVar itv = IntervalVar(v[i], days[i], months[i]);
          bool isSelected = itv %(opsym)s (*val);
          if (isSelected) selected->push_back(i);
        }
      }
    } else {
#pragma clang loop unroll(full)
      for (uint64_t i = 0 ; i < sz; ++i) {
        IntervalVar itv = IntervalVar(v[i], days[i], months[i]);
        bool isSelected = itv %(opsym)s (*val);
        if (isSelected) selected->push_back(i);
      }
    }
  }

  return CreateDatum(selected);
}
Datum %(t1)s_vec_%(op)s_%(t2)s_vec (Datum *params, uint64_t size) {
  assert(size == 3 && "invalid input");
  auto *selected = DatumGetValue<SelectList*>(params[0]);
  auto *vec1 = DatumGetValue<Vector *>(params[1]);
  auto *vec2 = DatumGetValue<Vector *>(params[2]);
  const int64_t* v1 = (const int64_t*)(vec1->getValue());
  const int32_t* days1 = (const int32_t*)(vec1->getDayValue());
  const int32_t* months1 = (const int32_t*)(vec1->getMonthValue());
  const int64_t* v2 = (const int64_t*)(vec2->getValue());
  const int32_t* days2 = (const int32_t*)(vec2->getDayValue());
  const int32_t* months2 = (const int32_t*)(vec1->getMonthValue());
  auto *sel1 = vec1->getSelected();
  auto *sel2 = vec2->getSelected();

  auto sz = vec1->getNumOfRows();
  assert(vec1->getNumOfRows() == vec2->getNumOfRows() && "invalid parameter");

  bool hasNull1 = vec1->hasNullValue();
  bool hasNull2 = vec2->hasNullValue();

  selected->clear();
  selected->setNulls(vec1->getNumOfRowsPlain(), sel1, selected->getNulls(), vec1->getNulls(), vec2->getNulls());
  if (sel1) {
     assert(*sel1 == *sel2);
     if (!hasNull1 && !hasNull2) {
    #pragma clang loop unroll(full)
       for (uint64_t i = 0 ; i < sz; ++i) {
          auto index = (*sel1)[i];
          IntervalVar itv1 = IntervalVar(v1[index], days1[index], months1[index]);
          IntervalVar itv2 = IntervalVar(v2[index], days2[index], months2[index]);
          bool isSelected = itv1 %(opsym)s itv2;
          if (isSelected) selected->push_back(index);
        }
      }
      else {
    #pragma clang loop unroll(full)
      for (uint64_t i = 0 ; i < sz; ++i) {
          auto index = (*sel1)[i];
          if (!vec1->isNullPlain(index) && !vec2->isNullPlain(index)) {
            IntervalVar itv1 = IntervalVar(v1[index], days1[index], months1[index]);
            IntervalVar itv2 = IntervalVar(v2[index], days2[index], months2[index]);
            bool isSelected = itv1 %(opsym)s itv2;
            if (isSelected) selected->push_back(index);
          }
        }
      }
    } else {
    if (!hasNull1 && !hasNull2) {
   #pragma clang loop unroll(full)
      for (uint64_t i = 0 ; i < sz; ++i) {
        IntervalVar itv1 = IntervalVar(v1[i], days1[i], months1[i]);
        IntervalVar itv2 = IntervalVar(v2[i], days2[i], months2[i]);
        bool isSelected = itv1 %(opsym)s itv2;
        if (isSelected) selected->push_back(i);
      }
    }
    else {
  #pragma clang loop unroll(full)
      for (uint64_t i = 0 ; i < sz; ++i) {
        if (!vec1->isNull(i) && !vec2->isNull(i)) {
          IntervalVar itv1 = IntervalVar(v1[i], days1[i], months1[i]);
          IntervalVar itv2 = IntervalVar(v2[i], days2[i], months2[i]);
          bool isSelected = itv1 %(opsym)s itv2;
          if (isSelected) selected->push_back(i);
        }
      }
    }
  }

  return CreateDatum(selected);
}
""" % {'t1': ltypemap[ta].name, 't1_type':ltypemap[ta].symbol, 'op': opmap[op_key].name, 'opsym': opmap[op_key].symbol, 't2': rtypemap[tb].name, 't2_type':rtypemap[tb].symbol,})
#endif
// ]]]
// [[[end]]]


// [[[cog
#if false
cog.outl("//%s:%d" % (cog.inFile,cog.firstLineNum))
generate_function_def("CmpOp", CMP_OP, NUMERIC_TYPES, NUMERIC_TYPES)
#endif
// ]]]
// [[[end]]]

// [[[cog
#if false
cog.outl("//%s:%d" % (cog.inFile,cog.firstLineNum))
generate_function_def("CmpOp", CMP_OP, BOOL_TYPE, BOOL_TYPE)
#endif
// ]]]
// [[[end]]]

// [[[cog
#if false
cog.outl("//%s:%d" % (cog.inFile,cog.firstLineNum))
generate_function_def_timestamp("CmpOp", CMP_OP, TIMESTAMP_TYPE, TIMESTAMP_TYPE)
generate_function_def_interval("CmpOp", CMP_OP, INTERVAL_TYPE, INTERVAL_TYPE)
#endif
// ]]]
// [[[end]]]

// [[[cog
#if false
cog.outl("//%s:%d" % (cog.inFile,cog.firstLineNum))
generate_function_def_wrapper("CmpOp", CMP_OP, BINARY_TYPES, BINARY_TYPES)
generate_function_def_wrapper("CmpOp", CMP_OP, TIMESTAMP_TYPE, TIMESTAMP_TYPE)
generate_function_def_wrapper("CmpOp", CMP_OP, INTERVAL_TYPE, INTERVAL_TYPE)
generate_function_def_wrapper("CmpOp", CMP_OP, DECIMAL_TYPE, DECIMAL_TYPE)
#endif
// ]]]
// [[[end]]]

}  // namespace dbcommon
