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

#ifndef UNIVPLAN_SRC_UNIVPLAN_UNIVPLANBUILDER_UNIVPLANBUILDER_SCAN_TASK_H_
#define UNIVPLAN_SRC_UNIVPLAN_UNIVPLANBUILDER_UNIVPLANBUILDER_SCAN_TASK_H_

#include <string>

#include "dbcommon/common/tuple-batch.h"

#include "univplan/proto/universal-plan.pb.h"

namespace univplan {

#define TBSPLITS_COL_INDEX_FILENAME 0
#define TBSPLITS_COL_INDEX_START 1
#define TBSPLITS_COL_INDEX_LEN 2
#define TBSPLITS_COL_INDEX_RANGEID 3
#define TBSPLITS_COL_INDEX_RGID 4

class UnivPlanBuilderScanTask {
 public:
  UnivPlanBuilderScanTask() { prepare(); }
  explicit UnivPlanBuilderScanTask(UnivPlanScanTask *task) {
    ref = task;
    prepare();
  }
  virtual ~UnivPlanBuilderScanTask() {}

  typedef std::unique_ptr<UnivPlanBuilderScanTask> uptr;

  void prepare(void) {
    if (tbSplitsDesc_.getNumOfColumns() == 0) {
      // initialize tuple desc only once, col names are not meaningful
      tbSplitsDesc_.add("f", dbcommon::TypeKind::STRINGID);
      tbSplitsDesc_.add("s", dbcommon::TypeKind::BIGINTID);
      tbSplitsDesc_.add("l", dbcommon::TypeKind::BIGINTID);
      tbSplitsDesc_.add("rid", dbcommon::TypeKind::BIGINTID);
      tbSplitsDesc_.add("rgid", dbcommon::TypeKind::INTID);
    }
    tbSplits_.reset(new dbcommon::TupleBatch(tbSplitsDesc_, true));
  }

  void addScanFileSplit(const char *filename, int64_t start, int64_t len,
                        int64_t rangeid, int32_t rgid) {
    dbcommon::TupleBatchWriter &writers = tbSplits_->getTupleBatchWriter();
    writers[TBSPLITS_COL_INDEX_FILENAME]->append(filename, strlen(filename),
                                                 false);
    writers[TBSPLITS_COL_INDEX_START]->append(reinterpret_cast<char *>(&start),
                                              sizeof(int64_t), false);
    writers[TBSPLITS_COL_INDEX_LEN]->append(reinterpret_cast<char *>(&len),
                                            sizeof(int64_t), false);
    writers[TBSPLITS_COL_INDEX_RANGEID]->append(
        reinterpret_cast<char *>(&rangeid), sizeof(int64_t), false);
    writers[TBSPLITS_COL_INDEX_RGID]->append(reinterpret_cast<char *>(&rgid),
                                             sizeof(int32_t), false);
    tbSplits_->incNumOfRows(1);
  }

  void generate(void) {
    std::unique_ptr<std::string> serialized(new std::string());
    if (tbSplits_->getNumOfRows() > 0) {
      tbSplits_->serialize(serialized.get(), 0);
      ref->set_allocated_serializedsplits(serialized.release());
    }
  }

  dbcommon::TupleBatch::uptr releaseSplitsTb() { return std::move(tbSplits_); }

 private:
  UnivPlanScanTask *ref = nullptr;
  dbcommon::TupleDesc tbSplitsDesc_;
  dbcommon::TupleBatch::uptr tbSplits_;
};

}  // namespace univplan

#endif  // UNIVPLAN_SRC_UNIVPLAN_UNIVPLANBUILDER_UNIVPLANBUILDER_SCAN_TASK_H_
