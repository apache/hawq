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

#ifndef STORAGE_SRC_STORAGE_FORMAT_ORC_ORC_PREDICATES_H_
#define STORAGE_SRC_STORAGE_FORMAT_ORC_ORC_PREDICATES_H_

#include <string>
#include <vector>

#include "dbcommon/common/tuple-batch.h"
#include "dbcommon/common/tuple-desc.h"
#include "dbcommon/nodes/datum.h"

#include "storage/format/orc/orc-proto-definition.h"

#include "univplan/common/expression.h"
#include "univplan/common/univplan-type.h"
#include "univplan/common/var-util.h"
#include "univplan/minmax/minmax-predicates.h"

namespace orc {

class ReaderImpl;
class OrcPredicates : public univplan::MinMaxPredicatesPage {
 public:
  OrcPredicates(const univplan::Statistics* s, ReaderImpl* r,
                const univplan::UnivPlanExprPolyList* predicateExprs,
                const dbcommon::TupleDesc* tupleDesc)
      : univplan::MinMaxPredicatesPage(s, predicateExprs, tupleDesc),
        reader(r) {}
  virtual ~OrcPredicates() {}

  typedef std::unique_ptr<OrcPredicates> uptr;

 public:
  virtual bool hasNull(int32_t colId) const;
  virtual bool hasAllNull(int32_t colId) const;
  virtual bool canDropByBloomFilter(int32_t colId,
                                    univplan::PredicateStats* stat,
                                    dbcommon::TypeKind type) const;
  virtual univplan::PredicateStats getMinMax(int32_t colId) const;
  virtual univplan::PredicateStats getMinMax(
      int32_t colId, dbcommon::Timestamp* minTimestamp,
      dbcommon::Timestamp* maxTimestamp) const;

 private:
  void disableInvalidColId(int32_t colId) const;

 private:
  ReaderImpl* reader;
};

}  // end of namespace orc

#endif  // STORAGE_SRC_STORAGE_FORMAT_ORC_ORC_PREDICATES_H_
