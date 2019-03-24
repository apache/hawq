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

#ifndef UNIVPLAN_SRC_UNIVPLAN_UNIVPLANBUILDER_UNIVPLANBUILDER_PARAMINFO_H_
#define UNIVPLAN_SRC_UNIVPLAN_UNIVPLANBUILDER_UNIVPLANBUILDER_PARAMINFO_H_

#include <string>

#include "univplan/proto/universal-plan.pb.h"

namespace univplan {

class UnivPlanBuilderParamInfo {
 public:
  explicit UnivPlanBuilderParamInfo(UnivPlanParamInfo *paramInfo)
      : ref_(paramInfo) {}

  virtual ~UnivPlanBuilderParamInfo() {}

  typedef std::unique_ptr<UnivPlanBuilderParamInfo> uptr;

  UnivPlanBuilderParamInfo &setType(int32_t type) {
    ref_->set_type(type);
    return *this;
  }

  UnivPlanBuilderParamInfo &setIsNull(bool isNull) {
    ref_->set_isnull(isNull);
    return *this;
  }

  UnivPlanBuilderParamInfo &setValue(const std::string &value) {
    ref_->set_value(value);
    return *this;
  }

 private:
  UnivPlanParamInfo *ref_;
};

}  // namespace univplan

#endif  // UNIVPLAN_SRC_UNIVPLAN_UNIVPLANBUILDER_UNIVPLANBUILDER_PARAMINFO_H_
