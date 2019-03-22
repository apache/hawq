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

#include "dbcommon/common/tuple-batch-store.h"

#include "dbcommon/testutil/tuple-batch-utils.h"
#include "dbcommon/utils/global.h"
#include "gtest/gtest.h"

namespace dbcommon {

TEST(TestTupleBatchStore, Test) {
  auto filesystem(FSManager.get("file://localhost"));
  TupleBatchUtility tbu;
  auto desc = tbu.generateTupleDesc(
      "schema: boolean int8 int16 int32 int64 float double "
      "bpchar bpchar(10) varchar varchar(5) string binary timestamp "
      "time date");
  auto tb0 = tbu.generateTupleBatchRandom(*desc, 0, 20, true, false);
  auto tb1 = tbu.generateTupleBatchRandom(*desc, 0, 233, true, true);
  auto tb2 = tbu.generateTupleBatchRandom(*desc, 0, 666, false, true);
  auto tb3 = tbu.generateTupleBatchRandom(*desc, 0, 555, false, false);

  std::string filename = "/tmp/TestTupleBatchStore";
  NTupleBatchStore tbs(filesystem, filename, NTupleBatchStore::Mode::OUTPUT);
  tbs.PutIntoNTupleBatchStore(tb0->clone());
  tbs.PutIntoNTupleBatchStore(tb1->clone());
  tbs.PutIntoNTupleBatchStore(tb2->clone());
  tbs.PutIntoNTupleBatchStore(tb3->clone());
  tbs.writeEOF();

  NTupleBatchStore tbsLoad(filesystem, filename, NTupleBatchStore::Mode::INPUT);
  EXPECT_EQ(tb0->toString(), tbsLoad.GetFromNTupleBatchStore()->toString());
  EXPECT_EQ(tb1->toString(), tbsLoad.GetFromNTupleBatchStore()->toString());
  EXPECT_EQ(tb2->toString(), tbsLoad.GetFromNTupleBatchStore()->toString());
  EXPECT_EQ(tb3->toString(), tbsLoad.GetFromNTupleBatchStore()->toString());
}

}  // namespace dbcommon
