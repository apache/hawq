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

#ifndef UNIVPLAN_SRC_UNIVPLAN_COMMON_PLANNODE_UTIL_H_
#define UNIVPLAN_SRC_UNIVPLAN_COMMON_PLANNODE_UTIL_H_

#include <algorithm>
#include <utility>

#include "dbcommon/log/logger.h"
#include "dbcommon/type/type-kind.h"

#include "univplan/proto/universal-plan.pb.h"
#include "univplan/univplanbuilder/univplanbuilder-agg.h"
#include "univplan/univplanbuilder/univplanbuilder-append.h"
#include "univplan/univplanbuilder/univplanbuilder-connector.h"
#include "univplan/univplanbuilder/univplanbuilder-ext-gs-scan.h"
#include "univplan/univplanbuilder/univplanbuilder-hash.h"
#include "univplan/univplanbuilder/univplanbuilder-hashjoin.h"
#include "univplan/univplanbuilder/univplanbuilder-insert.h"
#include "univplan/univplanbuilder/univplanbuilder-limit.h"
#include "univplan/univplanbuilder/univplanbuilder-material.h"
#include "univplan/univplanbuilder/univplanbuilder-mergejoin.h"
#include "univplan/univplanbuilder/univplanbuilder-nestloop.h"
#include "univplan/univplanbuilder/univplanbuilder-result.h"
#include "univplan/univplanbuilder/univplanbuilder-scan-seq.h"
#include "univplan/univplanbuilder/univplanbuilder-shareinput-scan.h"
#include "univplan/univplanbuilder/univplanbuilder-sink.h"
#include "univplan/univplanbuilder/univplanbuilder-sort.h"
#include "univplan/univplanbuilder/univplanbuilder-subquery-scan.h"
#include "univplan/univplanbuilder/univplanbuilder-unique.h"

namespace dbcommon {
enum FuncKind : uint32_t;
};

namespace univplan {

class PlanNodeUtil {
 public:
  PlanNodeUtil() {}
  ~PlanNodeUtil() {}

  static UnivPlanBuilderNode::uptr createPlanBuilderNode(
      UNIVPLANNODETYPE ntype) {
    UnivPlanBuilderNode::uptr res;
    switch (ntype) {
      case UNIVPLAN_AGG:
        res.reset(new UnivPlanBuilderAgg());
        break;
      case UNIVPLAN_SCAN_SEQ:
        res.reset(new UnivPlanBuilderScanSeq());
        break;
      case UNIVPLAN_CONNECTOR:
        res.reset(new UnivPlanBuilderConnector());
        break;
      case UNIVPLAN_SORT:
        res.reset(new UnivPlanBuilderSort());
        break;
      case UNIVPLAN_EXT_GS_SCAN:
        res.reset(new UnivPlanBuilderExtGSScan());
        break;
      case UNIVPLAN_LIMIT:
        res.reset(new UnivPlanBuilderLimit());
        break;
      case UNIVPLAN_APPEND:
        res.reset(new UnivPlanBuilderAppend());
        break;
      case UNIVPLAN_NESTLOOP:
        res.reset(new UnivPlanBuilderNestLoop());
        break;
      case UNIVPLAN_HASHJOIN:
        res.reset(new UnivPlanBuilderHashJoin());
        break;
      case UNIVPLAN_MERGEJOIN:
        res.reset(new UnivPlanBuilderMergeJoin());
        break;
      case UNIVPLAN_MATERIAL:
        res.reset(new UnivPlanBuilderMaterial());
        break;
      case UNIVPLAN_RESULT:
        res.reset(new UnivPlanBuilderResult());
        break;
      case UNIVPLAN_HASH:
        res.reset(new UnivPlanBuilderHash());
        break;
      case UNIVPLAN_SUBQUERYSCAN:
        res.reset(new UnivPlanBuilderSubqueryScan());
        break;
      case UNIVPLAN_UNIQUE:
        res.reset(new UnivPlanBuilderUnique());
        break;
      case UNIVPLAN_INSERT:
        res.reset(new UnivPlanBuilderInsert());
        break;
      case UNIVPLAN_SHAREINPUTSCAN:
        res.reset(new UnivPlanBuilderShareInputScan());
        break;
      default:
        LOG_ERROR(ERRCODE_INTERNAL_ERROR,
                  "PlanNodeUtil::createPlanBuilderNode can't handle "
                  "UNIVPLANNODETYPE %d",
                  ntype);
    }
    return std::move(res);
  }

  static const UnivPlanPlanNode &getPlanNode(const UnivPlanPlanNodePoly &node) {
    if (node.has_connector()) {
      return node.connector().super();
    } else if (node.has_scanseq()) {
      return node.scanseq().super();
    } else if (node.has_converge()) {
      return node.converge().super();
    } else if (node.has_shuffle()) {
      return node.shuffle().super();
    } else if (node.has_broadcast()) {
      return node.broadcast().super();
    } else if (node.has_sink()) {
      return node.sink().super();
    } else if (node.has_agg()) {
      return node.agg().super();
    } else if (node.has_sort()) {
      return node.sort().super();
    } else if (node.has_extgsscan()) {
      return node.extgsscan().super();
    } else if (node.has_extgsfilter()) {
      return node.extgsfilter().super();
    } else if (node.has_extgsproject()) {
      return node.extgsproject().super();
    } else if (node.has_limit()) {
      return node.limit().super();
    } else if (node.has_append()) {
      return node.append().super();
    } else if (node.has_nestloop()) {
      return node.nestloop().super();
    } else if (node.has_hashjoin()) {
      return node.hashjoin().super();
    } else if (node.has_mergejoin()) {
      return node.mergejoin().super();
    } else if (node.has_material()) {
      return node.material().super();
    } else if (node.has_result()) {
      return node.result().super();
    } else if (node.has_hash()) {
      return node.hash().super();
    } else if (node.has_subqueryscan()) {
      return node.subqueryscan().super();
    } else if (node.has_unique()) {
      return node.unique().super();
    } else if (node.has_insert()) {
      return node.insert().super();
    } else if (node.has_shareinputscan()) {
      return node.shareinputscan().super();
    } else {
      LOG_ERROR(ERRCODE_INTERNAL_ERROR,
                "PlanNodeUtil::getPlanNode type %d not supported", node.type());
    }
  }

  static UnivPlanPlanNode *getMutablePlanNode(UnivPlanPlanNodePoly *node) {
    if (node->has_connector()) {
      return node->mutable_connector()->mutable_super();
    } else if (node->has_scanseq()) {
      return node->mutable_scanseq()->mutable_super();
    } else if (node->has_converge()) {
      return node->mutable_converge()->mutable_super();
    } else if (node->has_shuffle()) {
      return node->mutable_shuffle()->mutable_super();
    } else if (node->has_broadcast()) {
      return node->mutable_broadcast()->mutable_super();
    } else if (node->has_sink()) {
      return node->mutable_sink()->mutable_super();
    } else if (node->has_agg()) {
      return node->mutable_agg()->mutable_super();
    } else if (node->has_sort()) {
      return node->mutable_sort()->mutable_super();
    } else if (node->has_limit()) {
      return node->mutable_limit()->mutable_super();
    } else if (node->has_append()) {
      return node->mutable_append()->mutable_super();
    } else if (node->has_nestloop()) {
      return node->mutable_nestloop()->mutable_super();
    } else if (node->has_hashjoin()) {
      return node->mutable_hashjoin()->mutable_super();
    } else if (node->has_mergejoin()) {
      return node->mutable_mergejoin()->mutable_super();
    } else if (node->has_material()) {
      return node->mutable_material()->mutable_super();
    } else if (node->has_result()) {
      return node->mutable_result()->mutable_super();
    } else if (node->has_hash()) {
      return node->mutable_hash()->mutable_super();
    } else if (node->has_subqueryscan()) {
      return node->mutable_subqueryscan()->mutable_super();
    } else if (node->has_extgsscan()) {
      return node->mutable_extgsscan()->mutable_super();
    } else if (node->has_extgsfilter()) {
      return node->mutable_extgsfilter()->mutable_super();
    } else if (node->has_extgsproject()) {
      return node->mutable_extgsproject()->mutable_super();
    } else if (node->has_unique()) {
      return node->mutable_unique()->mutable_super();
    } else if (node->has_insert()) {
      return node->mutable_insert()->mutable_super();
    } else if (node->has_shareinputscan()) {
      return node->mutable_shareinputscan()->mutable_super();
    } else {
      LOG_ERROR(ERRCODE_INTERNAL_ERROR,
                "PlanNodeUtil::getPlanNode type %d not supported",
                node->type());
    }
  }

  static const UnivPlanPlan *getPlan(const UnivPlanPlan &cur, int32_t stageNo) {
    if (cur.stageno() == stageNo) return &cur;
    for (int i = 0; i < cur.childstages_size(); i++) {
      const UnivPlanPlan *ret = getPlan(cur.childstages(i), stageNo);
      if (ret != nullptr) return ret;
    }
    return nullptr;
  }

  static dbcommon::TypeKind exprType(const UnivPlanExprPoly &expr) {
    if (expr.has_opexpr()) {
      return typeKindMapping(expr.opexpr().rettype());
    } else if (expr.has_val()) {
      return typeKindMapping(expr.val().type());
    } else if (expr.has_targetentry()) {
      return exprType(expr.targetentry().expression());
    } else if (expr.has_aggref()) {
      return typeKindMapping(expr.aggref().rettype());
    } else if (expr.has_var()) {
      return typeKindMapping(expr.var().typeid_());
    } else if (expr.has_funcexpr()) {
      return typeKindMapping(expr.funcexpr().rettype());
    } else if (expr.has_nulltest() || expr.has_boolexpr() ||
               expr.has_booltest()) {
      return dbcommon::TypeKind::BOOLEANID;
    } else if (expr.has_caseexpr()) {
      return typeKindMapping(expr.caseexpr().casetype());
    } else if (expr.has_param()) {
      return typeKindMapping(expr.param().typeid_());
    } else if (expr.has_subplan()) {
      if (expr.subplan().sublinktype() == univplan::SUBLINKTYPE::EXPR_SUBLINK)
        return typeKindMapping(expr.subplan().typeid_());
      else
        return dbcommon::TypeKind::BOOLEANID;
    } else if (expr.has_coalesceexpr()) {
      return typeKindMapping(expr.coalesceexpr().coalescetype());
    } else if (expr.has_nullifexpr()) {
      return typeKindMapping(expr.nullifexpr().rettype());
    } else if (expr.has_distinctexpr()) {
      return typeKindMapping(expr.distinctexpr().rettype());
    } else {
      LOG_ERROR(ERRCODE_INTERNAL_ERROR,
                "PlanNodeUtil::exprType expr %d not supported", expr.type());
    }
  }

  static int64_t exprTypeMod(const UnivPlanExprPoly &expr) {
    if (expr.has_val()) {
      return (expr.val().typemod());
    } else if (expr.has_var()) {
      return (expr.var().typemod());
    } else if (expr.has_targetentry()) {
      return exprType(expr.targetentry().expression());
    } else if (expr.has_param()) {
      return (expr.param().typemod());
    } else if (expr.has_subplan()) {
      return (expr.subplan().typemod());
    } else if (expr.has_coalesceexpr()) {
      return expr.coalesceexpr().coalescetypemod();
    } else if (expr.has_nullifexpr()) {
      return expr.nullifexpr().typemod();
    }

    int64_t ret = -1;
    if (expr.has_opexpr()) {
      for (auto arg : expr.opexpr().args())
        ret = std::max(exprTypeMod(arg), ret);
      return ret;
    } else if (expr.has_aggref()) {
      for (auto arg : expr.aggref().args())
        ret = std::max(exprTypeMod(arg), ret);
      return ret;
    } else if (expr.has_funcexpr()) {
      for (auto arg : expr.funcexpr().args())
        ret = std::max(exprTypeMod(arg), ret);
      return ret;
    } else if (expr.has_distinctexpr()) {
      for (auto arg : expr.funcexpr().args())
        ret = std::max(exprTypeMod(arg), ret);
      return ret;
    }

    if (expr.has_nulltest() || expr.has_boolexpr() || expr.has_booltest() ||
        expr.has_caseexpr()) {
      return -1;
    } else {
      LOG_ERROR(ERRCODE_INTERNAL_ERROR,
                "PlanNodeUtil::exprTypeMod expr %d not supported", expr.type());
    }
  }

  static dbcommon::TypeKind typeKindMapping(int32_t type) {
    dbcommon::TypeKind typeKind = static_cast<dbcommon::TypeKind>(type);
    if (typeKind == dbcommon::TypeKind::DECIMALID)
      typeKind = dbcommon::TypeKind::DECIMALNEWID;
    return typeKind;
  }

  static dbcommon::FuncKind funcKindMapping(int32_t funcType) {
    return static_cast<dbcommon::FuncKind>(funcType);
  }
};

}  // namespace univplan

#endif  // UNIVPLAN_SRC_UNIVPLAN_COMMON_PLANNODE_UTIL_H_
