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

#include "univplan/univplanbuilder/univplanbuilder.h"

#include <utility>

#include "dbcommon/log/logger.h"
#include "dbcommon/type/type-kind.h"

namespace univplan {

typedef dbcommon::TypeKind TypeKind;

UnivPlanBuilderPlan::uptr UnivPlanBuilder::swapBuilder(
    UnivPlanBuilderPlan::uptr newBuilder) {
  UnivPlanBuilderPlan::uptr ret = std::move(builder);
  builder = std::move(newBuilder);
  return std::move(ret);
}

void UnivPlanBuilder::addPlanNode(bool isleft, UnivPlanBuilderNode::uptr bld) {
  UnivPlanBuilderPlanNodePoly::uptr pn = bld->ownPlanNode();
  int32_t uid = bld->uid;  // required field
  int32_t pid = bld->pid;  // required field

  // Check if this node has node id set already
  if (uid < 0)
    LOG_ERROR(ERRCODE_INTERNAL_ERROR,
              "UnivPlanBuilder::addPlanNode uid is negative");

  // Check if the id is unique
  if (idToPlanNodes.find(uid) != idToPlanNodes.end())
    LOG_ERROR(ERRCODE_INTERNAL_ERROR,
              "UnivPlanBuilder::addPlanNode uid is duplicate");

  // Use hash table to track this node quickly
  idToPlanNodes[uid] = std::move(bld);

  UnivPlanPlan *plan = builder->getPlan();

  if (pid < 0) {
    // If a node has no parent id, it is a root node of the plan or subplan.
    if (builder->isSettingSubplan())
      builder->setSubplanNode(std::move(pn));
    else
      builder->setPlanNode(std::move(pn));

  } else {
    // If this node has a parent id, that means this node should be connected
    // to its expected parent node.

    // Find its parent node if its parent id is specified
    if (idToPlanNodes.find(pid) == idToPlanNodes.end())
      LOG_ERROR(ERRCODE_INTERNAL_ERROR,
                "UnivPlanBuilder::addPlanNode unexpected parent id %d found",
                pid);

    // add to parent node
    if (isleft) {
      idToPlanNodes[pid]->setNodeLeftPlan(std::move(pn));
    } else {
      idToPlanNodes[pid]->setNodeRightPlan(std::move(pn));
    }
  }
}

std::string UnivPlanBuilder::getJsonFormatedPlan() {
  return builder->getPlan()->DebugString();
}

std::string UnivPlanBuilder::serialize() {
  return builder->getPlan()->SerializeAsString();
}

}  // namespace univplan
