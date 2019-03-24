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

#ifndef UNIVPLAN_SRC_UNIVPLAN_COMMON_UNIVPLAN_TYPE_H_
#define UNIVPLAN_SRC_UNIVPLAN_COMMON_UNIVPLAN_TYPE_H_

#include <list>
#include <string>

#include "dbcommon/common/tuple-batch.h"

#include "univplan/proto/universal-plan.pb.h"
#include "univplan/univplanbuilder/univplanbuilder-scan-task.h"

namespace univplan {
typedef google::protobuf::RepeatedPtrField<univplan::UnivPlanExprPoly>
    UnivPlanExprPolyList;

typedef google::protobuf::RepeatedPtrField<univplan::UnivPlanScanTask>
    UnivPlanScanTaskList;

// this class provides interface of accessing scan task and its splits
class UnivPlanScanFileSplitList {
 public:
  UnivPlanScanFileSplitList() : splitsTb_(nullptr) {}
  explicit UnivPlanScanFileSplitList(dbcommon::TupleBatch *tb)
      : splitsTb_(tb) {}

  void setSplitsTb(dbcommon::TupleBatch *tb) { splitsTb_ = tb; }
  uint32_t splits_size() const {
    return splitsTb_ == nullptr ? 0 : splitsTb_->getNumOfRows();
  }

  // methods of getting split properties indexed in tb
  void splits_filename(int index, std::string *filename) {
    const dbcommon::TupleBatchReader &readers =
        splitsTb_->getTupleBatchReader();
    uint64_t len = 0;
    bool isNull = false;
    const char *res =
        readers[TBSPLITS_COL_INDEX_FILENAME]->read(index, &len, &isNull);
    filename->assign(res, len);
  }

  int64_t splits_start(int index) const {
    const dbcommon::TupleBatchReader &readers =
        splitsTb_->getTupleBatchReader();
    uint64_t len = 0;
    bool isNull = false;
    const char *res =
        readers[TBSPLITS_COL_INDEX_START]->read(index, &len, &isNull);
    return *(reinterpret_cast<const int64_t *>(res));
  }

  int64_t splits_len(int index) const {
    const dbcommon::TupleBatchReader &readers =
        splitsTb_->getTupleBatchReader();
    uint64_t len = 0;
    bool isNull = false;
    const char *res =
        readers[TBSPLITS_COL_INDEX_LEN]->read(index, &len, &isNull);
    return *(reinterpret_cast<const int64_t *>(res));
  }

  int64_t splits_rangeid(int index) const {
    const dbcommon::TupleBatchReader &readers =
        splitsTb_->getTupleBatchReader();
    uint64_t len = 0;
    bool isNull = false;
    const char *res =
        readers[TBSPLITS_COL_INDEX_RANGEID]->read(index, &len, &isNull);
    return *(reinterpret_cast<const int64_t *>(res));
  }

  int32_t splits_rgid(int index) const {
    const dbcommon::TupleBatchReader &readers =
        splitsTb_->getTupleBatchReader();
    uint64_t len = 0;
    bool isNull = false;
    const char *res =
        readers[TBSPLITS_COL_INDEX_RGID]->read(index, &len, &isNull);
    return *(reinterpret_cast<const int32_t *>(res));
  }

  void debugOuput() {
    if (splitsTb_ == nullptr) {
      LOG_INFO("no rows in splits tb");
      return;
    }
    for (int i = 0; i < splitsTb_->getNumOfRows(); ++i) {
      std::string filename;
      splits_filename(i, &filename);
      int64_t start = splits_start(i);
      int64_t len = splits_len(i);
      int64_t rangeid = splits_rangeid(i);
      int32_t rgid = splits_rgid(i);
      LOG_INFO("[%d] %s,%lld,%lld,%lld,%d", i, filename.c_str(), start, len,
               rangeid, rgid);
    }
  }

 protected:
  dbcommon::TupleBatch *splitsTb_;
};

// this subclass owns deserialized tb
class UnivPlanScanFileSplitListTb : public UnivPlanScanFileSplitList {
 public:
  UnivPlanScanFileSplitListTb() : UnivPlanScanFileSplitList(nullptr) {}
  explicit UnivPlanScanFileSplitListTb(dbcommon::TupleBatch::uptr tb)
      : UnivPlanScanFileSplitList(tb.get()) {
    splitsTbInst_ = std::move(tb);
  }
  void deserialize(const std::string &serializedTb) {
    splitsTbInst_.reset(new dbcommon::TupleBatch());
    splitsTbInst_->deserialize(serializedTb);
    setSplitsTb(splitsTbInst_.get());
  }

 protected:
  std::unique_ptr<dbcommon::TupleBatch> splitsTbInst_;
};

typedef std::list<std::unique_ptr<univplan::UnivPlanScanFileSplitList>>
    UnivPlanScanFileSplitListList;

}  // namespace univplan

#endif  // UNIVPLAN_SRC_UNIVPLAN_COMMON_UNIVPLAN_TYPE_H_
