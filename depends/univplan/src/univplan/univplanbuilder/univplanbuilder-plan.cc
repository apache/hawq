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

#include "univplan/univplanbuilder/univplanbuilder-plan.h"

#include <utility>

#include "dbcommon/log/logger.h"
#include "univplan/univplanbuilder/univplanbuilder-plan-node-poly.h"

namespace univplan {

void UnivPlanBuilderPlan::setPlanNode(UnivPlanBuilderPlanNodePoly::uptr pn) {
  ref->set_allocated_plan(pn->ownPlanNodePoly().release());
}

void UnivPlanBuilderPlan::setSubplanNode(UnivPlanBuilderPlanNodePoly::uptr pn) {
  ref->mutable_subplans()->AddAllocated(pn->ownPlanNodePoly().release());
}

void UnivPlanBuilderPlan::setStageNo(int32_t stageNo) {
  ref->set_stageno(stageNo);
}

void UnivPlanBuilderPlan::setTokenEntry(std::string protocol, std::string host,
                                        int port, std::string token) {
  UnivPlanTokenEntry *tokenEntry = ref->add_tokenmap();
  tokenEntry->set_token(token);
  auto key = tokenEntry->mutable_key();
  key->set_protocol(protocol);
  key->set_ip(host);
  key->set_port(port);
}

void UnivPlanBuilderPlan::setSnapshot(const std::string &snapshot) {
  ref->set_snapshot(snapshot);
}

void UnivPlanBuilderPlan::addGuc(const std::string &name,
                                 const std::string &value) {
  (*ref->mutable_guc())[name] = value;
}

void UnivPlanBuilderPlan::addCommonValue(const std::string &key,
                                         const std::string &value,
                                         std::string *newKey) {
  // if key exists, add suffix to avoid overwriting exisiting values, newKey
  // saves newly assigned key, if newKey is nullptr, original key is used
  std::string tmpKey = key;
  bool dup = false;
  if (newKey != nullptr) {
    uint64_t counter = 0;
    while (true) {
      *newKey = key + std::to_string(counter);
      if (ref->commonvalue().find(*newKey) == ref->commonvalue().end()) {
        tmpKey = *newKey;
        break;  // found new key
      } else if (ref->commonvalue().at(*newKey) == value) {
        tmpKey = *newKey;
        dup = true;
        break;  // found duplicate value, no need to allocate new key
      }
      counter++;
    }
  }
  if (!dup) {
    (*ref->mutable_commonvalue())[tmpKey] = value;
    LOG_DEBUG("set key %s into common values of plan size %lu", tmpKey.c_str(),
              value.size());
  } else {
    LOG_DEBUG("found duplicate key %s", tmpKey.c_str());
  }
}

void UnivPlanBuilderPlan::setDoInstrument(bool doInstrument) {
  ref->set_doinstrument(doInstrument);
}

void UnivPlanBuilderPlan::setNCrossLevelParams(int32_t nCrossLevelParams) {
  ref->set_ncrosslevelparams(nCrossLevelParams);
}

void UnivPlanBuilderPlan::setCmdType(UNIVPLANCMDTYPE cmdType) {
  ref->set_cmdtype(cmdType);
}

// xxx only called from stagize, need sync with univplanplan's fields
void UnivPlanBuilderPlan::from(const UnivPlanPlan &from) {
  // plan not set
  ref->set_stageno(from.stageno());
  if (from.snapshot().empty() == false) ref->set_snapshot(from.snapshot());
  ref->set_doinstrument(from.doinstrument());
  ref->set_ncrosslevelparams(from.ncrosslevelparams());
  ref->set_cmdtype(from.cmdtype());
  for (int i = 0; i < from.rangetables_size(); i++)
    ref->add_rangetables()->CopyFrom(from.rangetables(i));
  for (int i = 0; i < from.tokenmap_size(); i++)
    ref->add_tokenmap()->CopyFrom(from.tokenmap(i));
  for (int i = 0; i < from.receivers_size(); i++)
    ref->add_receivers()->CopyFrom(from.receivers(i));
  for (int i = 0; i < from.paraminfos_size(); ++i)
    ref->add_paraminfos()->CopyFrom(from.paraminfos(i));
  for (auto ent : from.guc()) (*ref->mutable_guc())[ent.first] = ent.second;
  for (auto ent : from.commonvalue())
    (*ref->mutable_commonvalue())[ent.first] = ent.second;

  // childStage not set
}

UnivPlanBuilderRangeTblEntry::uptr
UnivPlanBuilderPlan::addRangeTblEntryAndGetBuilder() {
  UnivPlanRangeTblEntry *entry = ref->add_rangetables();
  UnivPlanBuilderRangeTblEntry::uptr bld(
      new UnivPlanBuilderRangeTblEntry(entry));
  return std::move(bld);
}

UnivPlanBuilderReceiver::uptr UnivPlanBuilderPlan::addReceiverAndGetBuilder() {
  UnivPlanReceiver *receiver = ref->add_receivers();
  UnivPlanBuilderReceiver::uptr bld(new UnivPlanBuilderReceiver(receiver));
  return std::move(bld);
}

UnivPlanBuilderParamInfo::uptr
UnivPlanBuilderPlan::addParamInfoAndGetBuilder() {
  UnivPlanParamInfo *paramInfo = ref->add_paraminfos();
  UnivPlanBuilderParamInfo::uptr bld(new UnivPlanBuilderParamInfo(paramInfo));
  return std::move(bld);
}

UnivPlanBuilderPlan::uptr UnivPlanBuilderPlan::addChildStageAndGetBuilder() {
  UnivPlanPlan *child = ref->add_childstages();
  UnivPlanBuilderPlan::uptr bld(new UnivPlanBuilderPlan(child));
  return std::move(bld);
}

}  // namespace univplan
