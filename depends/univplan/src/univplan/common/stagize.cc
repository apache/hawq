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

#include "univplan/common/stagize.h"

#include <utility>

#include "dbcommon/log/logger.h"
#include "univplan/common/plannode-util.h"
#include "univplan/common/subplan-util.h"

namespace univplan {

// return a totally new Plan inside ctx->upb
void PlanStagizer::stagize(const UnivPlanPlan *plan, StagizerContext *ctx) {
  ctx->upb.reset(new UnivPlanBuilder);
  ctx->upb->getPlanBuilderPlan()->from(*plan);

  // stage main plan
  ctx->pid = -1;
  ctx->isleft = true;
  stageNo = 0;
  // -1 is a mark for the root stage, the stageNo of the plan tree's stage
  // is a postorder traversal except that the stageNo of root stage is zero
  ctx->upb->getPlanBuilderPlan()->setStageNo(-1);
  stagize(plan->plan(), ctx, 0);
  ctx->upb->getPlanBuilderPlan()->setStageNo(0);

  // stage subplan
  for (auto i = 0; i < plan->subplans_size(); i++) {
    ctx->pid = -1;
    ctx->isleft = true;
    if (ctx->subplanStageNo[i + 1] <= ctx->totalStageNo) {
      // retrieve stageNo for SubPlan
      stagize(plan->subplans(i), ctx, ctx->subplanStageNo[i + 1]);
    } else {
      // allocate new stage for InitPlan
      auto newStage =
          ctx->upb->getPlanBuilderPlan()->addChildStageAndGetBuilder();
      newStage->from(*(ctx->upb->getPlanBuilderPlan()->getPlan()));
      newStage->setStageNo(ctx->subplanStageNo[i + 1]);
      auto backupStage = ctx->upb->swapBuilder(std::move(newStage));
      stagize(plan->subplans(i), ctx, ctx->subplanStageNo[i + 1]);
      ctx->upb->swapBuilder(std::move(backupStage));
      ctx->upb->getPlanBuilderPlan()->getPlan()->add_subplans();  // placeholder
    }
  }
  assert(ctx->subplanStageNo.size() == plan->subplans_size());

  // link subplan to correlated stage
  auto rootPlan = ctx->upb->getPlanBuilderPlan()->getPlan();
  assert(rootPlan->subplans_size() == plan->subplans_size());
  auto rootStage = rootPlan;
  UnivPlanPlan backup;  // store stagized subplans
  backup.mutable_subplans()->Swap(rootPlan->mutable_subplans());
  SubplanUtil::linkSubplan(&backup, rootStage);
}

// FIXME(chiyang): To support subplan with correct stageNo(i.e. the
// corresponding sliceID/MotionID in HAWQ), we copy the motionID from HAWQ.
// However, motionID is not set in previous unittest, as a result of which we
// keep the old stageNo allocate strategy when needed(i.e. when the
// connector.stageno() == -1).
void PlanStagizer::stagize(const UnivPlanPlanNodePoly &originalNode,
                           StagizerContext *ctx, int32_t currentStageNo) {
  // ctx->upb->getPlanBuilderPlan referred to the current stage
  // ctx->pid referred to the pid of the new plan tree
  // ctx->isleft determine whether the current plannode is the left child of its
  // parent
  if (!originalNode.IsInitialized()) {
    LOG_ERROR(ERRCODE_INTERNAL_ERROR, "stagize node is empty");
  }
  const UnivPlanPlan *currentStage = ctx->upb->getPlanBuilderPlan()->getPlan();
  if (originalNode.has_connector()) {
    const UnivPlanConnector &connector = originalNode.connector();

    UnivPlanBuilderSinkInput::uptr builderSinkInput;
    switch (connector.type()) {
      case CONNECTORTYPE_CONVERGE:
        builderSinkInput.reset(new UnivPlanBuilderConverge());
        break;
      case CONNECTORTYPE_SHUFFLE:
        builderSinkInput.reset(new UnivPlanBuilderShuffle());
        break;
      case CONNECTORTYPE_BROADCAST:
        builderSinkInput.reset(new UnivPlanBuilderBroadcast());
        break;
      default:
        LOG_ERROR(ERRCODE_INTERNAL_ERROR, "unexpected connector node type %d",
                  connector.type());
    }
    // build sink
    UnivPlanBuilderSink::uptr builderSink(new UnivPlanBuilderSink());
    builderSink->from(connector);
    builderSink->setSourceStageNo(stageNo);
    builderSink->setCurrentStageNo(currentStage->stageno());
    builderSink->setConnectorType(connector.type());
    builderSink->uid = nodeId++;
    builderSink->pid = ctx->pid;
    UnivPlanBuilderSink *backupBuilderSink = builderSink.get();
    ctx->upb->addPlanNode(ctx->isleft, std::move(builderSink));

    // build new stage, need to be done before add sinkinput
    UnivPlanBuilderPlan::uptr builderNewStage =
        ctx->upb->getPlanBuilderPlan()->addChildStageAndGetBuilder();
    builderNewStage->from(*(ctx->upb->getPlanBuilderPlan()->getPlan()));
    builderNewStage->setStageNo(connector.stageno() != -1 ? connector.stageno()
                                                          : stageNo);

    UnivPlanBuilderPlan::uptr backupStage =
        ctx->upb->swapBuilder(std::move(builderNewStage));

    // sinkInput info
    builderSinkInput->from(connector);
    builderSinkInput->setTargetStageNo(currentStage->stageno());
    builderSinkInput->setCurrentStageNo(stageNo);
    builderSinkInput->uid = nodeId++;
    builderSinkInput->pid = -1;
    UnivPlanBuilderSinkInput *backupBuilderSinkInput = builderSinkInput.get();
    ctx->upb->addPlanNode(ctx->isleft, std::move(builderSinkInput));

    // sinkinput's left child comes from connector,
    // and there will be only one child
    const UnivPlanPlanNodePoly &originalNodeLeft =
        PlanNodeUtil::getPlanNode(originalNode).leftplan();
    ctx->pid = nodeId - 1;  // the nodeId of sinkinput
    ctx->isleft = true;
    stageNo++;
    stagize(originalNodeLeft, ctx, connector.stageno());

    builderNewStage = ctx->upb->swapBuilder(std::move(backupStage));

    // the old stageNo assign strategy, a post-order traversal
    int32_t childStageNo =
        ctx->totalStageNo - builderNewStage->getPlan()->stageno();
    if (currentStageNo == -1)
      currentStageNo =
          (backupBuilderSinkInput->getTargetStageNo() != -1)
              ? (ctx->totalStageNo - backupBuilderSinkInput->getTargetStageNo())
              : 0;
    // to bypass the old stageNo assign strategy when possible
    if (connector.stageno() != -1) {
      childStageNo = connector.stageno();
      if (currentStageNo == -1) currentStageNo = currentStage->stageno();
      if (currentStageNo == -1) currentStageNo = 0;
    }

    builderNewStage->setStageNo(childStageNo);
    backupBuilderSink->setCurrentStageNo(currentStageNo);
    backupBuilderSink->setSourceStageNo(childStageNo);
    backupBuilderSinkInput->setCurrentStageNo(childStageNo);
    backupBuilderSinkInput->setTargetStageNo(currentStageNo);
  } else {
    UnivPlanBuilderNode::uptr builderNodeNew =
        PlanNodeUtil::createPlanBuilderNode(originalNode.type());
    builderNodeNew->from(originalNode);
    int32_t builderNodeNewId = nodeId++;
    builderNodeNew->uid = builderNodeNewId;
    builderNodeNew->pid = ctx->pid;
    ctx->upb->addPlanNode(ctx->isleft, std::move(builderNodeNew));

    const UnivPlanPlanNode &originalNodePlanNode =
        PlanNodeUtil::getPlanNode(originalNode);

    auto relatedSubplans = SubplanUtil::getRelatedSubplanIds(originalNode);
    for (auto subplanId : relatedSubplans) {
      ctx->subplanStageNo[subplanId] = currentStageNo;
    }

    if (originalNodePlanNode.has_rightplan()) {
      ctx->pid = builderNodeNewId;
      ctx->isleft = false;
      const UnivPlanPlanNodePoly &originalNodeRight =
          originalNodePlanNode.rightplan();
      stagize(originalNodeRight, ctx, currentStageNo);
    }

    if (originalNodePlanNode.has_leftplan()) {
      ctx->pid = builderNodeNewId;
      ctx->isleft = true;
      const UnivPlanPlanNodePoly &originalNodeLeft =
          originalNodePlanNode.leftplan();
      stagize(originalNodeLeft, ctx, currentStageNo);
    }

    if (originalNode.has_subqueryscan()) {
      ctx->pid = builderNodeNewId;
      ctx->isleft = true;
      const UnivPlanPlanNodePoly &subplan =
          originalNode.subqueryscan().subplan();
      stagize(subplan, ctx, currentStageNo);
    }
  }
}

}  // namespace univplan
