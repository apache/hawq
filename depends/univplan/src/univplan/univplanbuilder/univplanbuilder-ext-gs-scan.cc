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

#include "univplan/univplanbuilder/univplanbuilder-ext-gs-scan.h"

#include <utility>
#include <vector>

namespace univplan {

UnivPlanBuilderExtGSScan::UnivPlanBuilderExtGSScan() : UnivPlanBuilderNode() {
  planNode.reset(new UnivPlanExtGSScan());
  ref = planNode.get();

  ref->set_allocated_super(UnivPlanBuilderNode::basePlanNode.release());
}

UnivPlanBuilderExtGSScan::~UnivPlanBuilderExtGSScan() {}

std::unique_ptr<UnivPlanBuilderPlanNodePoly>
UnivPlanBuilderExtGSScan::ownPlanNode() {
  UnivPlanBuilderPlanNodePoly::uptr pn(
      new UnivPlanBuilderPlanNodePoly(UNIVPLAN_EXT_GS_SCAN, planNode->super()));
  pn->getPlanNodePoly()->set_allocated_extgsscan(planNode.release());
  return std::move(pn);
}

void UnivPlanBuilderExtGSScan::from(const UnivPlanPlanNodePoly &node) {
  assert(node.type() == UNIVPLAN_EXT_GS_SCAN);
  ref->CopyFrom(node.extgsscan());
  baseRef = ref->mutable_super();
  baseRef->clear_leftplan();
  baseRef->clear_rightplan();
}

void UnivPlanBuilderExtGSScan::setScanRelId(uint32_t id) { ref->set_relid(id); }

void UnivPlanBuilderExtGSScan::setScanIndex(bool index = false) {
  ref->set_indexscan(index);
}

void UnivPlanBuilderExtGSScan::setIndexScanType(ExternalScanType type) {
  ref->set_type(univplan::ExternalScanType(type));
}

void UnivPlanBuilderExtGSScan::setDirectionType(
    ExternalScanDirection direction) {
  ref->set_direction(univplan::ExternalScanDirection(direction));
}

void UnivPlanBuilderExtGSScan::setIndexName(const char *indexName) {
  ref->set_indexname(indexName);
}

void UnivPlanBuilderExtGSScan::addIndexQual(
    UnivPlanBuilderExprTree::uptr exprTree) {
  ref->mutable_indexqual()->AddAllocated(exprTree->ownExprPoly().release());
}

void UnivPlanBuilderExtGSScan::setColumnsToRead(
    const std::vector<int32_t> &nArray) {
  for (int i = 0; i < nArray.size(); ++i) {
    ref->add_columnstoread(nArray[i]);
  }
}

void UnivPlanBuilderExtGSScan::setKeyColumnIndexes(
    const std::vector<int32_t> &nArray) {
  for (int i = 0; i < nArray.size(); ++i) {
    ref->add_keycolindex(nArray[i]);
  }
}

void UnivPlanBuilderExtGSScan::setFilterExpr(
    UnivPlanBuilderExprTree::uptr expr) {
  ref->set_allocated_filter(expr->ownExprPoly().release());
}

std::unique_ptr<UnivPlanBuilderScanTask>
UnivPlanBuilderExtGSScan::addScanTaskAndGetBuilder() {
  UnivPlanScanTask *task = ref->add_tasks();
  std::unique_ptr<UnivPlanBuilderScanTask> res(
      new UnivPlanBuilderScanTask(task));
  return std::move(res);
}

}  // namespace univplan
