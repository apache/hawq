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

#ifndef DBCOMMON_SRC_DBCOMMON_FUNCTION_INVOKER_H_
#define DBCOMMON_SRC_DBCOMMON_FUNCTION_INVOKER_H_

#include <cassert>
#include <vector>

#include "dbcommon/function/func.h"
#include "dbcommon/nodes/datum.h"

namespace dbcommon {

class Invoker {
 public:
  explicit Invoker(FuncKind fun);
  Invoker(FuncKind fun, uint64_t size);

  void resetPrarmeter() { params.clear(); }

  Invoker &setParam(size_t idx, const Datum &d) {
    params[idx] = d;
    return *this;
  }

  Datum &getParam(size_t idx) { return params[idx]; }

  Invoker &addParam(const Datum &d) {
    params.push_back(d);
    return *this;
  }

  Datum invoke() {
    assert(func && "invalid input");
    return func(params.data(), params.size());
  }

  bool callable() { return !!func; }

 private:
  FuncKind funcId;
  func_type func;
  std::vector<Datum> params;
};

}  // namespace dbcommon

#endif  // DBCOMMON_SRC_DBCOMMON_FUNCTION_INVOKER_H_
