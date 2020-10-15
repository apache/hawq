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

#ifndef UNIVPLAN_SRC_UNIVPLAN_UNIVPLANBUILDER_UNIVPLANBUILDER_TABLE_H_
#define UNIVPLAN_SRC_UNIVPLAN_UNIVPLANBUILDER_UNIVPLANBUILDER_TABLE_H_

#include <string>

#include "univplan/proto/universal-plan.pb.h"
#include "univplan/univplanbuilder/univplanbuilder-column.h"

namespace univplan {

class UnivPlanBuilderTable {
 public:
  UnivPlanBuilderTable() {
    node.reset(new UnivPlanTable());
    ref = node.get();
  }
  virtual ~UnivPlanBuilderTable() {}

  typedef std::unique_ptr<UnivPlanBuilderTable> uptr;

  std::unique_ptr<UnivPlanTable> ownTable() { return std::move(node); }

  void setTableId(int64_t id) { ref->set_tableid(id); }

  void setTableFormat(UNIVPLANFORMATTYPE type) { ref->set_format(type); }

  void setTableLocation(const std::string &location) {
    ref->set_location(location);
  }

  void setTargetName(const std::string &targetName) {
    ref->set_targetname(targetName);
  }

  void setTableOptionsInJson(const std::string &optStr) {
    ref->set_tableoptionsinjson(optStr);
  }

  UnivPlanBuilderColumn::uptr addPlanColumnAndGetBuilder() {
    UnivPlanColumn *column = ref->add_columns();
    UnivPlanBuilderColumn::uptr bld(new UnivPlanBuilderColumn(column));
    return std::move(bld);
  }

 private:
  UnivPlanTable *ref;
  std::unique_ptr<UnivPlanTable> node;
};

}  // namespace univplan

#endif  // UNIVPLAN_SRC_UNIVPLAN_UNIVPLANBUILDER_UNIVPLANBUILDER_TABLE_H_
