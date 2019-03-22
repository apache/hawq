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

#include "dbcommon/common/vector.h"
#include "dbcommon/log/debug-logger.h"
#include "dbcommon/testutil/scalar-utils.h"
#include "dbcommon/testutil/tuple-batch-utils.h"
#include "dbcommon/testutil/vector-utils.h"
#include "dbcommon/type/type-kind.h"
#include "gtest/gtest.h"

namespace dbcommon {

std::set<TypeKind> supportedTypes = {
    // integer
    TypeKind::TINYINTID,
    TypeKind::SMALLINTID,
    TypeKind::INTID,
    TypeKind::BIGINTID,
    // float point
    TypeKind::FLOATID,
    TypeKind::DOUBLEID,
    TypeKind::DECIMALID,
    TypeKind::DECIMALNEWID,
    // date/time
    TypeKind::TIMESTAMPID,  // TypeKind::TIMESTAMPTZID,
    TypeKind::DATEID,
    TypeKind::TIMEID,  // TypeKind::TIMETZID
    TypeKind::INTERVALID,
    // string
    TypeKind::STRINGID,
    TypeKind::VARCHARID,
    TypeKind::CHARID,
    // misc
    TypeKind::BOOLEANID,
    TypeKind::BINARYID,
};

class TestCopyControlOnVector : public testing::TestWithParam<TypeKind> {
 public:
  TestCopyControlOnVector() {
    typekind_ = GetParam();
    typemod_ = (std::set<TypeKind>{CHARID, VARCHARID}.count(typekind_)
                    ? TypeModifierUtil::getTypeModifierFromMaxLength(23)
                    : typemod_);
    auto typeUtil = dbcommon::TypeUtil::instance();
    auto typeName = typeUtil->getTypeEntryById(typekind_)->name;
    LOG_TESTING("%s", typeName.c_str());
  }
  TypeKind typekind_;
  int64_t typemod_ = -1;
  VectorUtility util_;
};
INSTANTIATE_TEST_CASE_P(TestCopyControl, TestCopyControlOnVector,
                        ::testing::ValuesIn(supportedTypes));

TEST_P(TestCopyControlOnVector, TestClone) {
  for (auto hasNull : std::vector<bool>{false, true})
    for (auto hasSel : std::vector<bool>{false, true})
      for (auto ownData : std::vector<bool>{false, true}) {
        LOG_TESTING("%s null data \t %s select list \t %s data",
                    (hasNull ? "has" : "no"), (hasSel ? "has" : "no"),
                    (ownData ? "own" : "not own"));
        auto srcVec = util_.generateVectorRandom(typekind_, 666, hasNull,
                                                 hasSel, ownData);
        auto clonedVec = srcVec->clone();
        EXPECT_EQ(srcVec->toString(), clonedVec->toString());
      }
}

TEST_P(TestCopyControlOnVector, TestAppend) {
  for (auto srcHasNull : std::vector<bool>{false, true})
    for (auto srcHasSel : std::vector<bool>{false, true})
      for (auto srcOwnData : std::vector<bool>{true}) {
        LOG_TESTING("%s null data \t %s select list \t %s data",
                    (srcHasNull ? "has" : "no"), (srcHasSel ? "has" : "no"),
                    (srcOwnData ? "own" : "not own"));
        for (auto appendHasNull : std::vector<bool>{false, true})
          for (auto appendHasSel : std::vector<bool>{false, true})
            for (auto appendOwnData : std::vector<bool>{false, true}) {
              auto srcVec = util_.generateVectorRandom(
                  typekind_, 666, srcHasNull, srcHasSel, srcOwnData);
              auto resVec = srcVec->clone();
              auto appendVec = util_.generateVectorRandom(
                  typekind_, 666, appendHasNull, appendHasSel, appendOwnData);
              resVec->append(appendVec.get());
              EXPECT_EQ(srcVec->getNumOfRows() + appendVec->getNumOfRows(),
                        resVec->getNumOfRows());
              EXPECT_EQ(srcVec->toString() + appendVec->toString(),
                        resVec->toString());
            }
      }
}

TEST_P(TestCopyControlOnVector, TestSerializeAndDeserialize) {
  for (auto hasNull : std::vector<bool>{false, true})
    for (auto hasSel : std::vector<bool>{false, true})
      for (auto ownData : std::vector<bool>{false, true}) {
        LOG_TESTING("%s null data \t %s select list \t %s data",
                    (hasNull ? "has" : "no"), (hasSel ? "has" : "no"),
                    (ownData ? "own" : "not own"));
        auto srcVec = util_.generateVectorRandom(typekind_, 666, hasNull,
                                                 hasSel, ownData);
        auto tmp = srcVec->clone();
        std::string serializedStr;
        std::unique_ptr<NodeSerializer> serializer(
            new NodeSerializer(&serializedStr));
        srcVec->serialize(serializer.get());
        std::unique_ptr<NodeDeserializer> deserializer(
            new NodeDeserializer(serializedStr));
        std::unique_ptr<Vector> deserializedVec =
            Vector::deserialize(deserializer.get());
        EXPECT_EQ(srcVec->toString(), deserializedVec->toString());
      }
}

TEST_P(TestCopyControlOnVector, TestMerge) {
  if (BOOLEANID == typekind_ || TIMESTAMPID == typekind_ ||
      INTERVALID == typekind_)
    return;

  TupleBatchUtility tbu;
  VectorUtility vu(typekind_);
  ScalarUtility su(typekind_);

  TupleDesc td;
  td.add("", typekind_, typemod_);
  td.add("", typekind_, typemod_);
  td.add("", typekind_, typemod_);
  auto tb = tbu.generateTupleBatch(td,
                                   "1 0 0\n"
                                   "0 NULL 3\n"
                                   "3 4 5\n"
                                   "6 7 8");

  Scalar scalar1 = su.generateScalar("38");
  Scalar scalar2 = su.generateScalar("NULL");

  SelectList sel0{0, 2}, sel1{1}, sel2{}, sel3{3}, sel4{4};
  std::vector<SelectList*> sel{&sel0, &sel1, &sel2, &sel3, &sel4};
  std::vector<Datum> data;
  data.push_back(CreateDatum(tb->getColumn(0)));
  data.push_back(CreateDatum(tb->getColumn(1)));
  data.push_back(CreateDatum(tb->getColumn(2)));
  data.push_back(CreateDatum(&scalar1));
  data.push_back(CreateDatum(&scalar2));

  auto vec = Vector::BuildVector(typekind_, true, typemod_);
  vec->merge(5, data, sel);

  Vector::uptr ans = vu.generateVector("1 NULL 3 38 NULL");
  EXPECT_EQ(ans->toString(), vec->toString());
}

TEST_P(TestCopyControlOnVector, TestBooleanVectorMerge) {
  if (BOOLEANID != typekind_) return;
  TupleBatchUtility tbu;
  VectorUtility vu(typekind_);
  ScalarUtility su(typekind_);

  TupleDesc td;
  td.add("", typekind_, typemod_);
  td.add("", typekind_, typemod_);
  td.add("", typekind_, typemod_);
  auto tb = tbu.generateTupleBatch(td,
                                   "t f t\n"
                                   "t NULL f\n"
                                   "t f t\n"
                                   "t f f");

  Scalar scalar1 = su.generateScalar("f");
  Scalar scalar2 = su.generateScalar("NULL");
  Scalar scalar3 = su.generateScalar("t");

  SelectList sel0{0, 2}, sel1{1}, sel2{}, sel3{3}, sel4{4}, sel5{5, 7}, sel6{6};
  std::vector<SelectList*> sel{&sel0, &sel1, &sel2, &sel3, &sel4, &sel5, &sel6};
  std::vector<Datum> data;
  data.push_back(CreateDatum(tb->getColumn(0)));
  data.push_back(CreateDatum(tb->getColumn(1)));
  data.push_back(CreateDatum(tb->getColumn(2)));
  data.push_back(CreateDatum(&scalar1));
  data.push_back(CreateDatum(&scalar2));
  SelectList boolSel{5};
  data.push_back(CreateDatum(&boolSel));
  data.push_back(CreateDatum(&scalar3));

  auto vec = Vector::BuildVector(typekind_, true, typemod_);
  vec->merge(8, data, sel);

  Vector::uptr ans = vu.generateVector("t NULL t f NULL t t f");
  EXPECT_EQ(ans->toString(), vec->toString());
}

TEST_P(TestCopyControlOnVector, TestTimestampVectorMerge) {
  if (TIMESTAMPID != typekind_) return;

  TupleBatchUtility tbu;
  VectorUtility vu(typekind_);
  ScalarUtility su(typekind_);

  TupleDesc td;
  td.add("", typekind_, typemod_);
  td.add("", typekind_, typemod_);
  td.add("", typekind_, typemod_);
  auto tb = tbu.generateTupleBatch(
      td,
      "1995-12-01 00:23:23 1995-02-01 00:32:32 1995-02-01 00:32:32\n"
      "1995-02-01 00:32:32 NULL 1995-02-01 00:32:32\n"
      "2010-07-01 00:15:12 1995-02-01 00:32:32 1995-02-01 00:32:32\n"
      "1995-02-01 00:32:32 1995-02-01 00:32:32 1995-02-01 00:32:32");

  Scalar scalar1 = su.generateScalar("1970-02-01 12:34:56");
  Scalar scalar2 = su.generateScalar("NULL");

  SelectList sel0{0, 2}, sel1{1}, sel2{}, sel3{3}, sel4{4};
  std::vector<SelectList*> sel{&sel0, &sel1, &sel2, &sel3, &sel4};
  std::vector<Datum> data;
  data.push_back(CreateDatum(tb->getColumn(0)));
  data.push_back(CreateDatum(tb->getColumn(1)));
  data.push_back(CreateDatum(tb->getColumn(2)));
  data.push_back(CreateDatum(&scalar1));
  data.push_back(CreateDatum(&scalar2));

  auto vec = Vector::BuildVector(typekind_, true, typemod_);
  vec->merge(5, data, sel);

  Vector::uptr ans = vu.generateVector(
      "1995-12-01 00:23:23 "
      "NULL "
      "2010-07-01 00:15:12 "
      "1970-02-01 12:34:56 "
      "NULL");
  EXPECT_EQ(ans->toString(), vec->toString());
}

TEST_P(TestCopyControlOnVector, TestIntervalVectorMerge) {
  if (INTERVALID != typekind_) return;

  TupleBatchUtility tbu;
  VectorUtility vu(typekind_);
  ScalarUtility su(typekind_);

  TupleDesc td;
  td.add("", typekind_, typemod_);
  td.add("", typekind_, typemod_);
  td.add("", typekind_, typemod_);
  auto tb = tbu.generateTupleBatch(td,
                                   "3:10:0 1:10:0 1:10:18000000000\n"
                                   "3:5:0 NULL 3:5:3600000000\n"
                                   "12:0:0 5:12:10000000 3:10:21600000000\n"
                                   "3:5:0 1:10:18000000000 1:10:180000000002");

  Scalar scalar1 = su.generateScalar("-10:8:0");
  Scalar scalar2 = su.generateScalar("NULL");

  SelectList sel0{0, 2}, sel1{1}, sel2{}, sel3{3}, sel4{4};
  std::vector<SelectList*> sel{&sel0, &sel1, &sel2, &sel3, &sel4};
  std::vector<Datum> data;
  data.push_back(CreateDatum(tb->getColumn(0)));
  data.push_back(CreateDatum(tb->getColumn(1)));
  data.push_back(CreateDatum(tb->getColumn(2)));
  data.push_back(CreateDatum(&scalar1));
  data.push_back(CreateDatum(&scalar2));

  auto vec = Vector::BuildVector(typekind_, true, typemod_);
  vec->merge(5, data, sel);

  Vector::uptr ans = vu.generateVector(
      "3:10:0 "
      "NULL "
      "12:0:0 "
      "-10:8:0 "
      "NULL");
  EXPECT_EQ(ans->toString(), vec->toString());
}

TEST_P(TestCopyControlOnVector, TestSelectDistinctScalar) {
  if (BOOLEANID == typekind_ || TIMESTAMPID == typekind_ ||
      INTERVALID == typekind_)
    return;
  VectorUtility vu(typekind_);
  ScalarUtility su(typekind_);
  Vector::uptr vec = vu.generateVector("5 4 NULL 2 5");
  Vector::uptr vec_nonulls = vu.generateVector("5 4 8 2 5");
  SelectList sel = {0, 1, 2, 4};

  Scalar scalar1 = su.generateScalar("NULL");
  Scalar scalar2 = su.generateScalar("5");
  if (typekind_ == TypeKind::CHARID) {
    std::string str =
        newBlankPaddedChar("5", 1, TypeModifierUtil::getMaxLen(typemod_));
    scalar2 = su.generateScalar(str);
  }

  SelectList sel1, sel2, sel3, sel4, sel5, sel6, sel7, sel8;
  SelectList retsel1{0, 1, 3, 4}, retsel2{0, 1, 2, 3, 4}, retsel3{1, 2, 3},
      retsel4{1, 2, 3}, retsel5{0, 1, 4}, retsel6{0, 1, 2, 4}, retsel7{1, 2},
      retsel8{1, 2};
  vec->selectDistinct(&sel1, &scalar1);
  EXPECT_TRUE((sel1 == retsel1));
  vec_nonulls->selectDistinct(&sel2, &scalar1);
  EXPECT_TRUE((sel2 == retsel2));

  vec->selectDistinct(&sel3, &scalar2);
  EXPECT_TRUE((sel3 == retsel3));
  vec_nonulls->selectDistinct(&sel4, &scalar2);
  EXPECT_TRUE((sel4 == retsel4));

  vec->setSelected(&sel, true);
  vec_nonulls->setSelected(&sel, true);

  vec->selectDistinct(&sel5, &scalar1);
  EXPECT_TRUE((sel5 == retsel5));
  vec_nonulls->selectDistinct(&sel6, &scalar1);
  EXPECT_TRUE((sel6 == retsel6));

  vec->selectDistinct(&sel7, &scalar2);
  EXPECT_TRUE((sel7 == retsel7));
  vec_nonulls->selectDistinct(&sel8, &scalar2);
  EXPECT_TRUE((sel8 == retsel8));
}

TEST_P(TestCopyControlOnVector, TestSelectDistinctVector) {
  if (BOOLEANID == typekind_ || TIMESTAMPID == typekind_ ||
      INTERVALID == typekind_)
    return;
  VectorUtility vu(typekind_);
  Vector::uptr vec1 = vu.generateVector("1 NULL 4 7 8");
  Vector::uptr vec1_nonulls = vu.generateVector("1 6 4 7 8");
  Vector::uptr vec2 = vu.generateVector("8 NULL 4 NULL NULL");
  Vector::uptr vec2_nonulls = vu.generateVector("8 2 4 9 8");

  SelectList sel = {0, 1, 2, 4};

  SelectList sel1, sel2, sel3, sel4, sel5, sel6, sel7, sel8;
  SelectList retsel1{0, 3, 4}, retsel2{0, 1, 3, 4}, retsel3{0, 1, 3},
      retsel4{0, 1, 3}, retsel5{0, 4}, retsel6{0, 1, 4}, retsel7{0, 1},
      retsel8{0, 1};

  vec1->selectDistinct(&sel1, vec2.get());
  EXPECT_TRUE((sel1 == retsel1));
  vec1_nonulls->selectDistinct(&sel2, vec2.get());
  EXPECT_TRUE((sel2 == retsel2));
  vec1->selectDistinct(&sel3, vec2_nonulls.get());
  EXPECT_TRUE((sel3 == retsel3));
  vec1_nonulls->selectDistinct(&sel4, vec2_nonulls.get());
  EXPECT_TRUE((sel4 == retsel4));

  vec1->setSelected(&sel, true);
  vec2->setSelected(&sel, true);
  vec1_nonulls->setSelected(&sel, true);
  vec2_nonulls->setSelected(&sel, true);

  vec1->selectDistinct(&sel5, vec2.get());
  EXPECT_TRUE((sel5 == retsel5));
  vec1_nonulls->selectDistinct(&sel6, vec2.get());
  EXPECT_TRUE((sel6 == retsel6));
  vec1->selectDistinct(&sel7, vec2_nonulls.get());
  EXPECT_TRUE((sel7 == retsel7));
  vec1_nonulls->selectDistinct(&sel8, vec2_nonulls.get());
  EXPECT_TRUE((sel8 == retsel8));
}

TEST_P(TestCopyControlOnVector, TestBooleanSelectDistinctScalar) {
  if (BOOLEANID != typekind_) return;
  VectorUtility vu(typekind_);
  ScalarUtility su(typekind_);
  Vector::uptr vec = vu.generateVector("t f NULL t");
  Vector::uptr vec_nonulls = vu.generateVector("t f t t");
  SelectList sel = {0, 1, 2};

  Scalar scalar1 = su.generateScalar("NULL");
  Scalar scalar2 = su.generateScalar("f");

  SelectList sel1, sel2, sel3, sel4, sel5, sel6, sel7, sel8;
  SelectList retsel1{0, 1, 3}, retsel2{0, 1, 2, 3}, retsel3{0, 2, 3},
      retsel4{0, 2, 3}, retsel5{0, 1}, retsel6{0, 1, 2}, retsel7{0, 2},
      retsel8{0, 2};
  vec->selectDistinct(&sel1, &scalar1);
  EXPECT_TRUE((sel1 == retsel1));
  vec_nonulls->selectDistinct(&sel2, &scalar1);
  EXPECT_TRUE((sel2 == retsel2));

  vec->selectDistinct(&sel3, &scalar2);
  EXPECT_TRUE((sel3 == retsel3));
  vec_nonulls->selectDistinct(&sel4, &scalar2);
  EXPECT_TRUE((sel4 == retsel4));

  vec->setSelected(&sel, true);
  vec_nonulls->setSelected(&sel, true);

  vec->selectDistinct(&sel5, &scalar1);
  EXPECT_TRUE((sel5 == retsel5));
  vec_nonulls->selectDistinct(&sel6, &scalar1);
  EXPECT_TRUE((sel6 == retsel6));

  vec->selectDistinct(&sel7, &scalar2);
  EXPECT_TRUE((sel7 == retsel7));
  vec_nonulls->selectDistinct(&sel8, &scalar2);
  EXPECT_TRUE((sel8 == retsel8));
}

TEST_P(TestCopyControlOnVector, TestBooleanSelectDistinctVector) {
  if (BOOLEANID != typekind_) return;
  VectorUtility vu(typekind_);
  Vector::uptr vec1 = vu.generateVector("t f NULL t");
  Vector::uptr vec1_nonulls = vu.generateVector("t f f t");
  Vector::uptr vec2 = vu.generateVector("f f NULL NULL");
  Vector::uptr vec2_nonulls = vu.generateVector("f f f t");
  SelectList sel = {0, 1, 2};

  SelectList sel1, sel2, sel3, sel4, sel5, sel6, sel7, sel8;
  SelectList retsel1{0, 3}, retsel2{0, 2, 3}, retsel3{0, 2}, retsel4{0},
      retsel5{0}, retsel6{0, 2}, retsel7{0, 2}, retsel8{0};

  vec1->selectDistinct(&sel1, vec2.get());
  EXPECT_TRUE((sel1 == retsel1));
  vec1_nonulls->selectDistinct(&sel2, vec2.get());
  EXPECT_TRUE((sel2 == retsel2));
  vec1->selectDistinct(&sel3, vec2_nonulls.get());
  EXPECT_TRUE((sel3 == retsel3));
  vec1_nonulls->selectDistinct(&sel4, vec2_nonulls.get());
  EXPECT_TRUE((sel4 == retsel4));

  vec1->setSelected(&sel, true);
  vec2->setSelected(&sel, true);
  vec1_nonulls->setSelected(&sel, true);
  vec2_nonulls->setSelected(&sel, true);

  vec1->selectDistinct(&sel5, vec2.get());
  EXPECT_TRUE((sel5 == retsel5));
  vec1_nonulls->selectDistinct(&sel6, vec2.get());
  EXPECT_TRUE((sel6 == retsel6));
  vec1->selectDistinct(&sel7, vec2_nonulls.get());
  EXPECT_TRUE((sel7 == retsel7));
  vec1_nonulls->selectDistinct(&sel8, vec2_nonulls.get());
  EXPECT_TRUE((sel8 == retsel8));
}

TEST_P(TestCopyControlOnVector, TestTimestampSelectDistinctScalar) {
  if (TIMESTAMPID != typekind_) return;
  VectorUtility vu(typekind_);
  ScalarUtility su(typekind_);
  Vector::uptr vec = vu.generateVector(
      "2000-11-01 12:54:54 1999-02-11 03:12:11 NULL 2004-19-06 09:12:24");
  Vector::uptr vec_nonulls = vu.generateVector(
      "2000-11-01 12:54:54 1999-02-11 03:12:11 1992-07-11 08:12:43 2004-19-06 "
      "09:12:24");
  SelectList sel = {0, 1, 2};

  Scalar scalar1 = su.generateScalar("NULL");
  Scalar scalar2 = su.generateScalar("2004-19-06 09:12:24");

  SelectList sel1, sel2, sel3, sel4, sel5, sel6, sel7, sel8;
  SelectList retsel1{0, 1, 3}, retsel2{0, 1, 2, 3}, retsel3{0, 1, 2},
      retsel4{0, 1, 2}, retsel5{0, 1}, retsel6{0, 1, 2}, retsel7{0, 1, 2},
      retsel8{0, 1, 2};

  vec->selectDistinct(&sel1, &scalar1);
  EXPECT_TRUE((sel1 == retsel1));
  vec_nonulls->selectDistinct(&sel2, &scalar1);
  EXPECT_TRUE((sel2 == retsel2));

  vec->selectDistinct(&sel3, &scalar2);
  EXPECT_TRUE((sel3 == retsel3));
  vec_nonulls->selectDistinct(&sel4, &scalar2);
  EXPECT_TRUE((sel4 == retsel4));

  vec->setSelected(&sel, true);
  vec_nonulls->setSelected(&sel, true);

  vec->selectDistinct(&sel5, &scalar1);
  EXPECT_TRUE((sel5 == retsel5));
  vec_nonulls->selectDistinct(&sel6, &scalar1);
  EXPECT_TRUE((sel6 == retsel6));

  vec->selectDistinct(&sel7, &scalar2);
  EXPECT_TRUE((sel7 == retsel7));
  vec_nonulls->selectDistinct(&sel8, &scalar2);
  EXPECT_TRUE((sel8 == retsel8));
}

TEST_P(TestCopyControlOnVector, TestTimestampSelectDistinctVector) {
  if (TIMESTAMPID != typekind_) return;
  VectorUtility vu(typekind_);
  Vector::uptr vec1 = vu.generateVector(
      "2000-11-01 12:54:54 1999-02-11 03:12:11 NULL 2004-19-06 09:12:24");
  Vector::uptr vec1_nonulls = vu.generateVector(
      "2000-11-01 12:54:54 1999-02-11 03:12:11 1992-07-11 12:44:12 2004-19-06 "
      "09:12:24");
  Vector::uptr vec2 =
      vu.generateVector("2000-11-01 12:54:54 NULL NULL 2001-09-11 09:00:00");
  Vector::uptr vec2_nonulls = vu.generateVector(
      "2000-11-01 12:54:54 1998-03-18 09:21:44 1992-07-11 12:44:12 2001-09-11 "
      "09:00:00");
  SelectList sel = {0, 1, 2};

  SelectList sel1, sel2, sel3, sel4, sel5, sel6, sel7, sel8;
  SelectList retsel1{1, 3}, retsel2{1, 2, 3}, retsel3{1, 2, 3}, retsel4{1, 3},
      retsel5{1}, retsel6{1, 2}, retsel7{1, 2}, retsel8{1};

  vec1->selectDistinct(&sel1, vec2.get());
  EXPECT_TRUE((sel1 == retsel1));
  vec1_nonulls->selectDistinct(&sel2, vec2.get());
  EXPECT_TRUE((sel2 == retsel2));
  vec1->selectDistinct(&sel3, vec2_nonulls.get());
  EXPECT_TRUE((sel3 == retsel3));
  vec1_nonulls->selectDistinct(&sel4, vec2_nonulls.get());
  EXPECT_TRUE((sel4 == retsel4));

  vec1->setSelected(&sel, true);
  vec2->setSelected(&sel, true);
  vec1_nonulls->setSelected(&sel, true);
  vec2_nonulls->setSelected(&sel, true);

  vec1->selectDistinct(&sel5, vec2.get());
  EXPECT_TRUE((sel5 == retsel5));
  vec1_nonulls->selectDistinct(&sel6, vec2.get());
  EXPECT_TRUE((sel6 == retsel6));
  vec1->selectDistinct(&sel7, vec2_nonulls.get());
  EXPECT_TRUE((sel7 == retsel7));
  vec1_nonulls->selectDistinct(&sel8, vec2_nonulls.get());
  EXPECT_TRUE((sel8 == retsel8));
}

TEST_P(TestCopyControlOnVector, TestIntervalSelectDistinctScalar) {
  if (INTERVALID != typekind_) return;
  VectorUtility vu(typekind_);
  ScalarUtility su(typekind_);
  Vector::uptr vec =
      vu.generateVector("3:15:0 5:3:3600000000 NULL 10:0:21600000000");
  Vector::uptr vec_nonulls =
      vu.generateVector("3:15:0 5:3:3600000000 0:15:0 10:0:21600000000");
  SelectList sel = {0, 1, 2};

  Scalar scalar1 = su.generateScalar("NULL");
  Scalar scalar2 = su.generateScalar("10:0:21600000000");

  SelectList sel1, sel2, sel3, sel4, sel5, sel6, sel7, sel8;
  SelectList retsel1{0, 1, 3}, retsel2{0, 1, 2, 3}, retsel3{0, 1, 2},
      retsel4{0, 1, 2}, retsel5{0, 1}, retsel6{0, 1, 2}, retsel7{0, 1, 2},
      retsel8{0, 1, 2};

  vec->selectDistinct(&sel1, &scalar1);
  EXPECT_TRUE((sel1 == retsel1));
  vec_nonulls->selectDistinct(&sel2, &scalar1);
  EXPECT_TRUE((sel2 == retsel2));

  vec->selectDistinct(&sel3, &scalar2);
  EXPECT_TRUE((sel3 == retsel3));
  vec_nonulls->selectDistinct(&sel4, &scalar2);
  EXPECT_TRUE((sel4 == retsel4));

  vec->setSelected(&sel, true);
  vec_nonulls->setSelected(&sel, true);

  vec->selectDistinct(&sel5, &scalar1);
  EXPECT_TRUE((sel5 == retsel5));
  vec_nonulls->selectDistinct(&sel6, &scalar1);
  EXPECT_TRUE((sel6 == retsel6));

  vec->selectDistinct(&sel7, &scalar2);
  EXPECT_TRUE((sel7 == retsel7));
  vec_nonulls->selectDistinct(&sel8, &scalar2);
  EXPECT_TRUE((sel8 == retsel8));
}

TEST_P(TestCopyControlOnVector, TestIntervalSelectDistinctVector) {
  if (INTERVALID != typekind_) return;
  VectorUtility vu(typekind_);
  Vector::uptr vec1 =
      vu.generateVector("3:15:0 5:3:3600000000 NULL 10:0:21600000000");
  Vector::uptr vec1_nonulls =
      vu.generateVector("3:15:0 5:3:3600000000 0:15:0 10:0:21600000000");
  Vector::uptr vec2 = vu.generateVector("3:15:0 NULL NULL 8:5:21600000000");
  Vector::uptr vec2_nonulls =
      vu.generateVector("3:15:0 4:15:0 0:15:0 8:5:21600000000");
  SelectList sel = {0, 1, 2};

  SelectList sel1, sel2, sel3, sel4, sel5, sel6, sel7, sel8;
  SelectList retsel1{1, 3}, retsel2{1, 2, 3}, retsel3{1, 2, 3}, retsel4{1, 3},
      retsel5{1}, retsel6{1, 2}, retsel7{1, 2}, retsel8{1};

  vec1->selectDistinct(&sel1, vec2.get());
  EXPECT_TRUE((sel1 == retsel1));
  vec1_nonulls->selectDistinct(&sel2, vec2.get());
  EXPECT_TRUE((sel2 == retsel2));
  vec1->selectDistinct(&sel3, vec2_nonulls.get());
  EXPECT_TRUE((sel3 == retsel3));
  vec1_nonulls->selectDistinct(&sel4, vec2_nonulls.get());
  EXPECT_TRUE((sel4 == retsel4));

  vec1->setSelected(&sel, true);
  vec2->setSelected(&sel, true);
  vec1_nonulls->setSelected(&sel, true);
  vec2_nonulls->setSelected(&sel, true);

  vec1->selectDistinct(&sel5, vec2.get());
  EXPECT_TRUE((sel5 == retsel5));
  vec1_nonulls->selectDistinct(&sel6, vec2.get());
  EXPECT_TRUE((sel6 == retsel6));
  vec1->selectDistinct(&sel7, vec2_nonulls.get());
  EXPECT_TRUE((sel7 == retsel7));
  vec1_nonulls->selectDistinct(&sel8, vec2_nonulls.get());
  EXPECT_TRUE((sel8 == retsel8));
}

}  // namespace dbcommon
