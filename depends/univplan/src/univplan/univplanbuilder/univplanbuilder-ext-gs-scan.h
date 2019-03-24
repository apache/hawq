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

#ifndef UNIVPLAN_SRC_UNIVPLAN_UNIVPLANBUILDER_UNIVPLANBUILDER_EXT_GS_SCAN_H_
#define UNIVPLAN_SRC_UNIVPLAN_UNIVPLANBUILDER_UNIVPLANBUILDER_EXT_GS_SCAN_H_

#include <memory>
#include <vector>

#include "univplan/proto/universal-plan.pb.h"
#include "univplan/univplanbuilder/univplanbuilder-node.h"
#include "univplan/univplanbuilder/univplanbuilder-scan-task.h"

namespace univplan {

class UnivPlanBuilderExtGSScan : public UnivPlanBuilderNode {
 public:
  UnivPlanBuilderExtGSScan();
  virtual ~UnivPlanBuilderExtGSScan();

  typedef std::unique_ptr<UnivPlanBuilderExtGSScan> uptr;

  std::unique_ptr<UnivPlanBuilderPlanNodePoly> ownPlanNode() override;

  void from(const UnivPlanPlanNodePoly &node) override;

  void setScanRelId(uint32_t id);
  void setColumnsToRead(const std::vector<int32_t> &nArray);
  void setKeyColumnIndexes(const std::vector<int32_t> &nArray);
  void setFilterExpr(UnivPlanBuilderExprTree::uptr expr);
  std::unique_ptr<UnivPlanBuilderScanTask> addScanTaskAndGetBuilder();
  // set magma index info
  void setScanIndex(bool index);
  void setIndexScanType(ExternalScanType type);
  void setDirectionType(ExternalScanDirection direction);
  void setIndexName(const char *indexName);
  void addIndexQual(UnivPlanBuilderExprTree::uptr exprTree);

 private:
  UnivPlanExtGSScan *ref;
  std::unique_ptr<UnivPlanExtGSScan> planNode;
};

}  // namespace univplan

#endif  // UNIVPLAN_SRC_UNIVPLAN_UNIVPLANBUILDER_UNIVPLANBUILDER_EXT_GS_SCAN_H_
