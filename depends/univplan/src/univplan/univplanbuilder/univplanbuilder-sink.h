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

#ifndef UNIVPLAN_SRC_UNIVPLAN_UNIVPLANBUILDER_UNIVPLANBUILDER_SINK_H_
#define UNIVPLAN_SRC_UNIVPLAN_UNIVPLANBUILDER_UNIVPLANBUILDER_SINK_H_

#include "univplan/proto/universal-plan.pb.h"
#include "univplan/univplanbuilder/univplanbuilder-node.h"

namespace univplan {

// this class is only used in stagize
class UnivPlanBuilderSinkInput : public UnivPlanBuilderNode {
 public:
  typedef std::unique_ptr<UnivPlanBuilderSinkInput> uptr;
  UnivPlanBuilderSinkInput() : UnivPlanBuilderNode() {}
  virtual ~UnivPlanBuilderSinkInput() {}
  virtual void setTargetStageNo(int32_t targetStageNo) = 0;
  virtual int32_t getTargetStageNo() = 0;
  virtual void setCurrentStageNo(int32_t targetStageNo) = 0;
  virtual int32_t getCurrentStageNo() = 0;
  virtual void from(const UnivPlanConnector &from) {
    baseRef->CopyFrom(from.super());
    baseRef->clear_leftplan();
    baseRef->clear_rightplan();
  }
  // virtual UNIVPLANNODETYPE getType() = 0;
};

class UnivPlanBuilderSink : public UnivPlanBuilderNode {
 public:
  UnivPlanBuilderSink() : UnivPlanBuilderNode() {
    planNode.reset(new UnivPlanSink());
    ref = planNode.get();

    ref->set_allocated_super(UnivPlanBuilderNode::basePlanNode.release());
  }
  virtual ~UnivPlanBuilderSink() {}

  typedef std::unique_ptr<UnivPlanBuilderSink> uptr;

  // only used in stagize
  void from(const UnivPlanConnector &from) {
    baseRef->CopyFrom(from.super());
    baseRef->clear_leftplan();
    baseRef->clear_rightplan();
    ref->set_connectortype(from.type());
    for (int i = 0; i < from.colidx_size(); ++i)
      ref->add_colidx(from.colidx(i));
    for (int i = 0; i < from.sortfuncid_size(); ++i)
      ref->add_sortfuncid(from.sortfuncid(i));
  }

  void setSourceStageNo(int32_t sourceStageNo) {
    ref->set_sourcestageno(sourceStageNo);
  }

  void setCurrentStageNo(int32_t currentStageNo) {
    ref->set_currentstageno(currentStageNo);
  }

  void setConnectorType(UNIVPLANCONNECTORTYPE connectorType) {
    ref->set_connectortype(connectorType);
  }

  UnivPlanBuilderPlanNodePoly::uptr ownPlanNode() override {
    UnivPlanBuilderPlanNodePoly::uptr pn(
        new UnivPlanBuilderPlanNodePoly(UNIVPLAN_SINK, planNode->super()));
    pn->getPlanNodePoly()->set_allocated_sink(planNode.release());
    return std::move(pn);
  }

  void from(const UnivPlanPlanNodePoly &node) override {
    assert(node.type() == UNIVPLAN_SINK);
    ref->CopyFrom(node.sink());
    baseRef = ref->mutable_super();
    baseRef->clear_leftplan();
    baseRef->clear_rightplan();
  }

 private:
  UnivPlanSink *ref;
  std::unique_ptr<UnivPlanSink> planNode;
};

class UnivPlanBuilderConverge : public UnivPlanBuilderSinkInput {
 public:
  UnivPlanBuilderConverge() : UnivPlanBuilderSinkInput() {
    planNode.reset(new UnivPlanConverge());
    ref = planNode.get();

    ref->set_allocated_super(UnivPlanBuilderNode::basePlanNode.release());
  }
  virtual ~UnivPlanBuilderConverge() {}

  typedef std::unique_ptr<UnivPlanBuilderConverge> uptr;

  void from(const UnivPlanConnector &from) override {
    UnivPlanBuilderSinkInput::from(from);
  }

  void setTargetStageNo(int32_t targetStageNo) override {
    ref->set_targetstageno(targetStageNo);
  }

  int32_t getTargetStageNo() override { return ref->targetstageno(); }

  void setCurrentStageNo(int32_t currentStageNo) override {
    ref->set_currentstageno(currentStageNo);
  }

  int32_t getCurrentStageNo() override { return ref->currentstageno(); }

  UnivPlanBuilderPlanNodePoly::uptr ownPlanNode() override {
    UnivPlanBuilderPlanNodePoly::uptr pn(
        new UnivPlanBuilderPlanNodePoly(UNIVPLAN_CONVERGE, planNode->super()));
    pn->getPlanNodePoly()->set_allocated_converge(planNode.release());
    return std::move(pn);
  }

  void from(const UnivPlanPlanNodePoly &node) override {
    assert(node.type() == UNIVPLAN_CONVERGE);
    ref->CopyFrom(node.converge());
    baseRef = ref->mutable_super();
    baseRef->clear_leftplan();
    baseRef->clear_rightplan();
  }

 private:
  UnivPlanConverge *ref;
  std::unique_ptr<UnivPlanConverge> planNode;
};

class UnivPlanBuilderShuffle : public UnivPlanBuilderSinkInput {
 public:
  UnivPlanBuilderShuffle() : UnivPlanBuilderSinkInput() {
    planNode.reset(new UnivPlanShuffle());
    ref = planNode.get();

    ref->set_allocated_super(UnivPlanBuilderNode::basePlanNode.release());
  }
  virtual ~UnivPlanBuilderShuffle() {}

  typedef std::unique_ptr<UnivPlanBuilderShuffle> uptr;

  void from(const UnivPlanConnector &from) override {
    UnivPlanBuilderSinkInput::from(from);
    ref->set_magmatable(from.magmatable());
    ref->mutable_hashexpr()->CopyFrom(from.hashexpr());
    ref->mutable_magmamap()->CopyFrom(from.magmamap());
    ref->set_rangenum(from.rangenum());
  }

  void setTargetStageNo(int32_t targetStageNo) override {
    ref->set_targetstageno(targetStageNo);
  }

  int32_t getTargetStageNo() override { return ref->targetstageno(); }

  void setCurrentStageNo(int32_t currentStageNo) override {
    ref->set_currentstageno(currentStageNo);
  }

  int32_t getCurrentStageNo() override { return ref->currentstageno(); }

  UnivPlanBuilderPlanNodePoly::uptr ownPlanNode() override {
    UnivPlanBuilderPlanNodePoly::uptr pn(
        new UnivPlanBuilderPlanNodePoly(UNIVPLAN_SHUFFLE, planNode->super()));
    pn->getPlanNodePoly()->set_allocated_shuffle(planNode.release());
    return std::move(pn);
  }

  void from(const UnivPlanPlanNodePoly &node) override {
    assert(node.type() == UNIVPLAN_SHUFFLE);
    ref->CopyFrom(node.shuffle());
    baseRef = ref->mutable_super();
    baseRef->clear_leftplan();
    baseRef->clear_rightplan();
  }

 private:
  UnivPlanShuffle *ref;
  std::unique_ptr<UnivPlanShuffle> planNode;
};

class UnivPlanBuilderBroadcast : public UnivPlanBuilderSinkInput {
 public:
  UnivPlanBuilderBroadcast() : UnivPlanBuilderSinkInput() {
    planNode.reset(new UnivPlanBroadcast());
    ref = planNode.get();

    ref->set_allocated_super(UnivPlanBuilderNode::basePlanNode.release());
  }
  virtual ~UnivPlanBuilderBroadcast() {}

  typedef std::unique_ptr<UnivPlanBuilderBroadcast> uptr;

  void from(const UnivPlanConnector &from) override {
    UnivPlanBuilderSinkInput::from(from);
  }

  void setTargetStageNo(int32_t targetStageNo) override {
    ref->set_targetstageno(targetStageNo);
  }

  int32_t getTargetStageNo() override { return ref->targetstageno(); }

  void setCurrentStageNo(int32_t currentStageNo) override {
    ref->set_currentstageno(currentStageNo);
  }

  int32_t getCurrentStageNo() override { return ref->currentstageno(); }

  UnivPlanBuilderPlanNodePoly::uptr ownPlanNode() override {
    UnivPlanBuilderPlanNodePoly::uptr pn(
        new UnivPlanBuilderPlanNodePoly(UNIVPLAN_BROADCAST, planNode->super()));
    pn->getPlanNodePoly()->set_allocated_broadcast(planNode.release());
    return std::move(pn);
  }

  void from(const UnivPlanPlanNodePoly &node) override {
    assert(node.type() == UNIVPLAN_BROADCAST);
    ref->CopyFrom(node.broadcast());
    baseRef = ref->mutable_super();
    baseRef->clear_leftplan();
    baseRef->clear_rightplan();
  }

 private:
  UnivPlanBroadcast *ref;
  std::unique_ptr<UnivPlanBroadcast> planNode;
};

}  // namespace univplan

#endif  // UNIVPLAN_SRC_UNIVPLAN_UNIVPLANBUILDER_UNIVPLANBUILDER_SINK_H_
