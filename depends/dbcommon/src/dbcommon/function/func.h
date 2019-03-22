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

#ifndef DBCOMMON_SRC_DBCOMMON_FUNCTION_FUNC_H_
#define DBCOMMON_SRC_DBCOMMON_FUNCTION_FUNC_H_

#include <functional>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include "dbcommon/nodes/datum.h"
#include "dbcommon/type/type-kind.h"

namespace dbcommon {

class ExprContext;
enum FuncKind : uint32_t;

typedef std::function<Datum(Datum *, uint64_t size)> func_type;
typedef Datum (*Function)(Datum *, uint64_t size);

struct FuncEntry {
  FuncKind funcId;                 // FUNC_ID
  std::string funcName;            // FUNC_NAME
  TypeKind retType;                // return type id
  std::vector<TypeKind> argTypes;  // type list of args
  func_type func;                  // the implementation
  bool predictable;                // useful in filter push down
};

typedef std::unordered_map<std::string, FuncEntry> FuncMap;

struct AggEntry {
  FuncKind aggFnId;
  FuncKind aggTransFnId;   // transition function
  FuncKind aggPrelimFnId;  // preliminary aggregation function
  FuncKind aggFinalFn;     // final function
  FuncKind aggSortOp;      // associated sort operator
  TypeKind aggTransType;   // type of aggregate's transition
  bool aggInitVal;         // initial value for transition state
  bool aggOrdered;         // array of ordering columns
};

typedef std::unordered_map<FuncKind, AggEntry, std::hash<int64_t>> AggMap;

class Func {
 public:
  Func();
  virtual ~Func() {}

 public:
  const FuncEntry *getFuncEntryById(FuncKind id);
  bool hasFuncEntryById(FuncKind id);
  const FuncEntry *getFuncEntryByName(const std::string &name);
  const AggEntry *getAggEntryById(FuncKind id);
  FuncKind mapEqualToNotEqualFuncId(FuncKind id);

  static Func *instance();

 private:
  void setupFunctionTable();
  void setupAggTable();

  std::vector<FuncEntry> FuncEntryArray;
  FuncMap FuncEntryMap;
  std::vector<AggEntry> AggEntryArray;
  AggMap AggEntryMap;

  static Func *inst;
};
}  // namespace dbcommon

#endif  // DBCOMMON_SRC_DBCOMMON_FUNCTION_FUNC_H_
