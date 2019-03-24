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

#ifndef UNIVPLAN_SRC_UNIVPLAN_UNIVPLANBUILDER_UNIVPLANBUILDER_COLUMN_H_
#define UNIVPLAN_SRC_UNIVPLAN_UNIVPLANBUILDER_UNIVPLANBUILDER_COLUMN_H_

#include <string>

#include "univplan/proto/universal-plan.pb.h"

namespace univplan {

class UnivPlanBuilderColumn {
 public:
  explicit UnivPlanBuilderColumn(UnivPlanColumn *column) : ref(column) {}

  virtual ~UnivPlanBuilderColumn() {}

  typedef std::unique_ptr<UnivPlanBuilderColumn> uptr;

  void setColumnName(const std::string &columnName) {
    ref->set_columnname(columnName);
  }

  void setTypeId(int32_t typeId) { ref->set_typeid_(typeId); }
  void setTypeMod(int64_t typeMod) { ref->set_typemod(typeMod); }

  void setScale1(int32_t scale) { ref->set_scale1(scale); }
  void setScale2(int32_t scale) { ref->set_scale2(scale); }
  void setIsNullable(bool nullable) { ref->set_isnullable(nullable); }

 private:
  UnivPlanColumn *ref = nullptr;
};

}  // namespace univplan

#endif  // UNIVPLAN_SRC_UNIVPLAN_UNIVPLANBUILDER_UNIVPLANBUILDER_COLUMN_H_
