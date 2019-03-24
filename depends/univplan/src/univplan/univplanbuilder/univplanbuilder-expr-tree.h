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

#ifndef UNIVPLAN_SRC_UNIVPLAN_UNIVPLANBUILDER_UNIVPLANBUILDER_EXPR_TREE_H_
#define UNIVPLAN_SRC_UNIVPLAN_UNIVPLANBUILDER_UNIVPLANBUILDER_EXPR_TREE_H_

#include <map>
#include <memory>
#include <utility>

#include "univplan/common/univplan-type.h"
#include "univplan/proto/universal-plan.pb.h"
#include "univplan/univplanbuilder/univplanbuilder-expr-node.h"

namespace univplan {
class UnivPlanBuilderExprTree {
 public:
  UnivPlanBuilderExprTree() {}
  typedef std::unique_ptr<UnivPlanBuilderExprTree> uptr;

  std::unique_ptr<UnivPlanExprPoly> ownExprPoly() { return std::move(root); }
  void setRoot(UnivPlanBuilderExprPoly::uptr exprPoly) {
    root = std::move(exprPoly->ownExprPoly());
  }

  int32_t getUid() { return nodeCounter++; }

  // Create a new UnivPlanBuilderExprNode
  // @param pid Parent ID for the new UnivPlanBuilderExprNode
  template <typename UnivPlanBuilderExprNode>
  std::unique_ptr<UnivPlanBuilderExprNode> ExprNodeFactory(int32_t pid) {
    return std::unique_ptr<UnivPlanBuilderExprNode>(
        new UnivPlanBuilderExprNode(pid, getUid()));
  }

  void addExprNode(UnivPlanBuilderExprNode::uptr bld) {
    UnivPlanBuilderExprPoly::uptr pn = bld->getExprPolyBuilder();
    int32_t uid = bld->uid;  // required field
    int32_t pid = bld->pid;  // required field

    // Check if this node has node id set already
    if (uid < 0)
      LOG_ERROR(ERRCODE_INTERNAL_ERROR,
                "UnivPlanBuilderExprTree::addExprNode uid is negative");

    // Check if the id is unique
    if (idToExprNode.find(uid) != idToExprNode.end())
      LOG_ERROR(ERRCODE_INTERNAL_ERROR,
                "UnivPlanBuilderExprTree::addExprNode uid is duplicate");

    // Use hash table to track this node quickly
    idToExprNode[uid] = std::move(bld);

    if (pid < 0) {
      // If this node has no parent id, that means this node is a root, it is
      // not allowed to have more than one root plan node.

      // Set as a root plan node
      setRoot(std::move(pn));

    } else {
      // If this node has a parent id, that means this node should be connected
      // to its expected parent node.

      // Find its parent node if its parent id is specified
      if (idToExprNode.find(pid) == idToExprNode.end())
        LOG_ERROR(ERRCODE_INTERNAL_ERROR,
                  "UnivPlanBuilderExprTree::addExprNode unexpected parent id "
                  "%d found",
                  pid);

      // add to parent node
      idToExprNode[pid]->addArgs(std::move(pn));
    }
  }

  UnivPlanBuilderExprNode* getExprNode(int32_t uid) {
    return idToExprNode[uid].get();
  }

 private:
  std::unique_ptr<UnivPlanExprPoly> root = nullptr;
  std::map<int32_t, UnivPlanBuilderExprNode::uptr> idToExprNode;
  int32_t nodeCounter = 0;
};

}  // namespace univplan

#endif  // UNIVPLAN_SRC_UNIVPLAN_UNIVPLANBUILDER_UNIVPLANBUILDER_EXPR_TREE_H_
