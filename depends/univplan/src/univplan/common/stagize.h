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

#ifndef UNIVPLAN_SRC_UNIVPLAN_COMMON_STAGIZE_H_
#define UNIVPLAN_SRC_UNIVPLAN_COMMON_STAGIZE_H_

#include <map>

#include "univplan/proto/universal-plan.pb.h"
#include "univplan/univplanbuilder/univplanbuilder.h"

namespace univplan {

typedef struct StagizerContext {
  UnivPlanBuilder::uptr upb;
  int32_t pid;
  bool isleft;
  uint32_t totalStageNo;
  std::map<int32_t, int32_t> subplanStageNo;
} StagizerContext;

class PlanStagizer {
 public:
  PlanStagizer() {}
  ~PlanStagizer() {}

  void stagize(const UnivPlanPlan *plan, StagizerContext *ctx);

 private:
  void stagize(const UnivPlanPlanNodePoly &node, StagizerContext *ctx,
               int32_t currentStageNo);
  int32_t nodeId = 0;
  int32_t stageNo = 0;  // stageNo start from 0
};

}  // namespace univplan

#endif  // UNIVPLAN_SRC_UNIVPLAN_COMMON_STAGIZE_H_
