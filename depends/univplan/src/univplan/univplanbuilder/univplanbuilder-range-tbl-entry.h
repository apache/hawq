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

#ifndef UNIVPLAN_SRC_UNIVPLAN_UNIVPLANBUILDER_UNIVPLANBUILDER_RANGE_TBL_ENTRY_H_
#define UNIVPLAN_SRC_UNIVPLAN_UNIVPLANBUILDER_UNIVPLANBUILDER_RANGE_TBL_ENTRY_H_

#include "univplan/proto/universal-plan.pb.h"

namespace univplan {

class UnivPlanBuilderRangeTblEntry {
 public:
  UnivPlanBuilderRangeTblEntry() {
    node.reset(new UnivPlanRangeTblEntry());
    ref = node.get();
  }

  explicit UnivPlanBuilderRangeTblEntry(UnivPlanRangeTblEntry *rangeTblEntry)
      : ref(rangeTblEntry) {}

  virtual ~UnivPlanBuilderRangeTblEntry() {}

  typedef std::unique_ptr<UnivPlanBuilderRangeTblEntry> uptr;

  void setTable(std::unique_ptr<UnivPlanTable> table) {
    ref->set_allocated_table(table.release());
  }

 private:
  UnivPlanRangeTblEntry *ref;
  std::unique_ptr<UnivPlanRangeTblEntry> node;
};

}  // namespace univplan

#endif  // UNIVPLAN_SRC_UNIVPLAN_UNIVPLANBUILDER_UNIVPLANBUILDER_RANGE_TBL_ENTRY_H_
