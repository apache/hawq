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

#include <vector>

#include "gtest/gtest.h"

#include "dbcommon/log/debug-logger.h"
#include "dbcommon/nodes/select-list.h"
#include "dbcommon/testutil/vector-utils.h"

namespace dbcommon {

TEST(TestSelectList, TestGetComplement) {
  SelectList sel1{1, 7, 9};
  EXPECT_EQ(SelectList({0, 2, 3, 4, 5, 6, 8}), sel1.getComplement(10));
  EXPECT_EQ(SelectList({0, 2, 3, 4, 5, 6, 8, 10, 11, 12, 13, 14}),
            sel1.getComplement(15));

  SelectList sel2{0, 1, 7};
  EXPECT_EQ(SelectList({2, 3, 4, 5, 6, 8, 9}), sel2.getComplement(10));

  {
    VectorUtility vu(TypeKind::BOOLEANID);
    auto src = vu.generateVector("f t NULL f");
    auto comp = vu.generateVector("t f f t");

    SelectList sel;
    sel.fromVector(src.get());
    EXPECT_EQ(comp->toString(), sel.getComplement(4).toString());
  }
}

TEST(TestSelectList, TestSetNulls) {
  VectorUtility vu(TypeKind::BIGINTID);
  auto vec1 = vu.generateVector("NULL 0 NULL 0 0");
  auto vec2 = vu.generateVector("0 NULL 0 NULL 0");

  SelectList outSelectList{0, 1, 2, 3, 4};
  SelectList *out = &outSelectList;

  {
    LOG_TESTING("Without SelectList");

    {
      LOG_TESTING("Multi-channel switch ");
      out->setNulls(5, nullptr, vec1->getNulls(), vec2->getNulls());
      EXPECT_EQ("NULL NULL NULL NULL t ", out->toString());

      out->setNulls(5, nullptr, vec1->getNulls());
      EXPECT_EQ("NULL t NULL t t ", out->toString());

      out->setNulls(5, nullptr, nullptr, vec2->getNulls());
      EXPECT_EQ("t NULL t NULL t ", out->toString());

      out->setNulls(5, nullptr, false);
      EXPECT_EQ("t t t t t ", out->toString());
    }

    {
      LOG_TESTING("Fix null value");

      out->setNulls(5, nullptr, true);
      EXPECT_EQ("NULL NULL NULL NULL NULL ", out->toString());

      out->setNulls(5, nullptr, false);
      EXPECT_EQ("t t t t t ", out->toString());
    }
  }

  {
    LOG_TESTING("With SelectList");
    SelectList vecSelectList{2, 3};

    outSelectList = {2, 3, 4};
    out->setNulls(5, nullptr, false);
    EXPECT_EQ("f f t t t ", out->toString());

    {
      LOG_TESTING("Multi-channel switch ");

      out->setNulls(5, &vecSelectList, vec1->getNulls(), vec2->getNulls());
      EXPECT_EQ("f f NULL NULL t ", out->toString());

      out->setNulls(5, &vecSelectList, vec1->getNulls());
      EXPECT_EQ("f f NULL t t ", out->toString());

      out->setNulls(5, &vecSelectList, nullptr, vec2->getNulls());
      EXPECT_EQ("f f t NULL t ", out->toString());

      out->setNulls(5, &vecSelectList, false);
      EXPECT_EQ("f f t t t ", out->toString());
    }

    {
      LOG_TESTING("Fix null value");

      out->setNulls(5, &vecSelectList, true);
      EXPECT_EQ("f f NULL NULL t ", out->toString());

      out->setNulls(5, &vecSelectList, false);
      EXPECT_EQ("f f t t t ", out->toString());
    }
  }
}

TEST(TestSelectList, TestFromVector) {
  VectorUtility vu(TypeKind::BOOLEANID);
  auto ret = vu.generateVector("NULL");
  SelectList sel;

  auto expect = vu.generateVector("t f NULL");
  sel.fromVector(expect.get());
  EXPECT_EQ(expect->toString(), sel.toString());
}

TEST(TestSelectList, TestToVector) {
  VectorUtility vu(TypeKind::BOOLEANID);
  auto ret = vu.generateVector("NULL");
  SelectList sel;

  auto expect = vu.generateVector("t f NULL");
  sel.fromVector(expect.get());
  sel.toVector(ret.get());
  EXPECT_EQ(expect->toString(), ret->toString());
}

TEST(TestSelectList, TestCopyControl) {
  VectorUtility vu(TypeKind::BOOLEANID);
  SelectList src1, src2 = {3, 7};
  src2.setPlainSize(8);
  auto expect = vu.generateVector("t f NULL");
  src1.fromVector(expect.get());

  SelectList sel1(src1);
  EXPECT_EQ(src1.toString(), sel1.toString());

  SelectList sel2(std::move(sel1));
  EXPECT_EQ(src1.toString(), sel2.toString());

  sel2 = src2;
  EXPECT_EQ(src2.toString(), sel2.toString());

  sel2 = src1;
  EXPECT_EQ(src1.toString(), sel2.toString());

  sel2 = {3, 7};
  sel2.setPlainSize(8);
  EXPECT_EQ(src2.toString(), sel2.toString());
}

}  // namespace dbcommon
