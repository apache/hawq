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

#include "univplan/univplanbuilder/univplanbuilder-scan-seq.h"

namespace univplan {

UnivPlanBuilderScanSeq::UnivPlanBuilderScanSeq() : UnivPlanBuilderNode() {
  planNode.reset(new UnivPlanScanSeq);
  ref = planNode.get();

  ref->set_allocated_super(UnivPlanBuilderNode::basePlanNode.release());
}

UnivPlanBuilderScanSeq::~UnivPlanBuilderScanSeq() {}

std::unique_ptr<UnivPlanBuilderPlanNodePoly>
UnivPlanBuilderScanSeq::ownPlanNode() {
  UnivPlanBuilderPlanNodePoly::uptr pn(
      new UnivPlanBuilderPlanNodePoly(UNIVPLAN_SCAN_SEQ, planNode->super()));
  pn->getPlanNodePoly()->set_allocated_scanseq(planNode.release());
  return std::move(pn);
}

void UnivPlanBuilderScanSeq::from(const UnivPlanPlanNodePoly &node) {
  assert(node.type() == UNIVPLAN_SCAN_SEQ);
  ref->CopyFrom(node.scanseq());
  baseRef = ref->mutable_super();
  baseRef->clear_leftplan();
  baseRef->clear_rightplan();
}

void UnivPlanBuilderScanSeq::setScanRelId(uint32_t id) { ref->set_relid(id); }

void UnivPlanBuilderScanSeq::setColumnsToRead(
    const std::vector<int32_t> &nArray) {
  for (int i = 0; i < nArray.size(); ++i) {
    ref->add_columnstoread(nArray[i]);
  }
}

void UnivPlanBuilderScanSeq::setReadStatsOnly(bool readStatsOnly) {
  ref->set_readstatsonly(readStatsOnly);
}

UnivPlanBuilderScanTask::uptr
UnivPlanBuilderScanSeq::addScanTaskAndGetBuilder() {
  UnivPlanScanTask *task = ref->add_tasks();
  std::unique_ptr<UnivPlanBuilderScanTask> res(
      new UnivPlanBuilderScanTask(task));
  return std::move(res);
}

}  // namespace univplan
