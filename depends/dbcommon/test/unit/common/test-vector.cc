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

#include "gtest/gtest.h"

#include "dbcommon/common/vector.h"
#include "dbcommon/common/vector/decimal-vector.h"
#include "dbcommon/common/vector/fixed-length-vector.h"
#include "dbcommon/common/vector/list-vector.h"
#include "dbcommon/common/vector/struct-vector.h"
#include "dbcommon/common/vector/timestamp-vector.h"
#include "dbcommon/common/vector/variable-length-vector.h"
#include "dbcommon/log/debug-logger.h"
#include "dbcommon/testutil/vector-utils.h"

namespace dbcommon {

TEST(TestVector, TestAppendDatum) {
  DoubleVector vec(true);
  Datum d1 = CreateDatum<double>(1.1);
  vec.append(d1, false);

  bool isnull;
  EXPECT_EQ(
      0, strncmp("1.1", vec.Vector::read(0, &isnull).c_str(), strlen("1.1")));
  EXPECT_FALSE(isnull);
}

/* Not Open for this function is not used now */
// TEST(TestVector, TestAppendWithSelectList) {
//  std::unique_ptr<dbcommon::Vector> vec_buf =
//          dbcommon::Vector::BuildVector(STRINGID, true);
//  StringVector vec(true);
//  SelectList sl = {0,2};
//  bool null;
//  vec_buf->append("test1", false);
//  vec_buf->append("test2", false);
//  vec_buf->append("", true);
//  vec.append(vec_buf.get(), &sl);
//  EXPECT_EQ("test1", vec.Vector::read(0, &null));
//  EXPECT_EQ("", vec.Vector::read(1, &null));
//  EXPECT_EQ(2, vec.getNumOfRowsPlain());
//  EXPECT_EQ(2, vec.getNumOfRows());
//}

TEST(TestVector, TestCast) {
  SelectList list;
  Object* obj = &list;
  EXPECT_NE(nullptr, dynamic_cast<SelectList*>(obj));
}

TEST(TestVector, TestOffset) {
  DoubleVector dvec(true);
  dvec.append("1.0", false);
  EXPECT_EQ(dvec.getLengths(), nullptr);
  EXPECT_EQ(dvec.getLengths(), nullptr);

  StringVector vec(true);
  vec.append("abc", false);
  vec.append("def", false);
  EXPECT_NE(vec.getLengths(), nullptr);
}

TEST(TESTVector, TestHash) {
  VectorUtility vu;

  std::vector<int32_t> int32Values = {1, 2, 3, 4};
  SelectList lst = {1};
  std::vector<bool> nulls = {false, false, false, false};
  std::unique_ptr<Vector> int32Vect =
      vu.generateSelectVector<int32_t>(INTID, int32Values, &nulls, &lst);

  {  // less than 64bits
    std::vector<uint64_t> int32HashValues(int32Vect->getNumOfRows());
    int32Vect->hash(int32HashValues);
    EXPECT_EQ(std::vector<uint64_t>({8589934582}), int32HashValues);

    std::unique_ptr<Vector> int32VectAll =
        vu.generateSelectVector<int32_t>(INTID, int32Values, &nulls, nullptr);

    std::vector<uint64_t> int32AllHashValues(int32Vect->getNumOfRows());
    int32VectAll->hash(int32AllHashValues);
    EXPECT_EQ(std::vector<uint64_t>(
                  {4294967291, 8589934582, 12884901873, 17179869164}),
              int32AllHashValues);
  }

  {  // 64bits
    std::vector<int64_t> int64Values = {1, 2, 3, 4};
    lst = {0, 2};
    std::unique_ptr<Vector> int64Vect =
        vu.generateSelectVector<int64_t>(BIGINTID, int64Values, &nulls, &lst);

    std::vector<uint64_t> int64HashValues(int64Vect->getNumOfRows());
    int64Vect->hash(int64HashValues);
    EXPECT_EQ(std::vector<uint64_t>({1, 3}), int64HashValues);

    std::unique_ptr<Vector> int64AllVect = vu.generateSelectVector<int64_t>(
        BIGINTID, int64Values, &nulls, nullptr);

    std::vector<uint64_t> int64AllHashValues(int64AllVect->getNumOfRows());
    int64AllVect->hash(int64AllHashValues);
    EXPECT_EQ(std::vector<uint64_t>({1, 2, 3, 4}), int64AllHashValues);
  }

  {  // string
    std::vector<bool> nulls = {false, false, false, false,
                               false, false, false, false};
    std::vector<std::string> strValues = {
        "a", "db", "new", "good", "oushu", "having", "company", "excellent"};
    lst = {0, 2};
    std::unique_ptr<Vector> strVect =
        vu.generateSelectVector<std::string>(STRINGID, strValues, &nulls, &lst);

    std::vector<uint64_t> strHashValues(strVect->getNumOfRows());
    strVect->hash(strHashValues);
    EXPECT_EQ(std::vector<uint64_t>({97, 6647406}), strHashValues);

    std::unique_ptr<Vector> strAllVect = vu.generateSelectVector<std::string>(
        STRINGID, strValues, &nulls, nullptr);

    std::vector<uint64_t> strAllHashValues(strAllVect->getNumOfRows());
    strAllVect->hash(strAllHashValues);
    EXPECT_EQ(
        std::vector<uint64_t>({97, 25188, 6647406, 1685024615, 1937076079,
                               1634230632, 1668244323, 7126155391476019289}),
        strAllHashValues);
  }
}

TEST(TestVector, TestSetOwnData) {
  StringVector svec(false);
  EXPECT_EQ(svec.getOwnData(), false);

  DoubleVector dvec(true);
  EXPECT_EQ(dvec.getOwnData(), true);
}

TEST(TestVector, TestClone) {
  VectorUtility vu;

  std::vector<int32_t> int32Values1 = {1, 2, 3, 4};
  SelectList lst1 = {1, 2};
  std::vector<bool> nulls1 = {false, false, false, false};
  std::unique_ptr<Vector> int32Vect1 =
      vu.generateSelectVector<int32_t>(INTID, int32Values1, &nulls1, &lst1);

  EXPECT_EQ(int32Vect1->getNumOfRows(), 2);

  std::unique_ptr<Vector> clone1 = int32Vect1->clone();
  EXPECT_EQ(clone1->getNumOfRows(), 2);
  EXPECT_EQ(clone1->getSelected(), nullptr);
  EXPECT_EQ(Vector::equal(*int32Vect1, *clone1), true);

  std::vector<std::string> strValues = {"ab", "bc", "cd", "de"};
  SelectList lst2 = {0, 2};
  std::vector<bool> nulls2 = {false, false, false, false};
  std::unique_ptr<Vector> strVect =
      vu.generateSelectVector<std::string>(STRINGID, strValues, &nulls2, &lst2);

  EXPECT_EQ(strVect->getNumOfRows(), 2);

  std::unique_ptr<Vector> clone2 = strVect->clone();
  EXPECT_EQ(clone2->getNumOfRows(), 2);
  EXPECT_EQ(clone2->getSelected(), nullptr);
  EXPECT_EQ(Vector::equal(*strVect, *clone2), true);
}

TEST(TestVector, TestSwap) {
  VectorUtility vu;

  std::vector<int32_t> int32Values1 = {1, 2, 3, 4};
  SelectList lst1 = {1, 2};
  std::vector<bool> nulls1 = {false, false, false, false};
  std::unique_ptr<Vector> int32Vect1 =
      vu.generateSelectVector<int32_t>(INTID, int32Values1, &nulls1, &lst1);

  std::vector<int32_t> int32Values2 = {5, 7, 0};
  SelectList lst2 = {2};
  std::vector<bool> nulls2 = {false, false, true};
  std::unique_ptr<Vector> int32Vect2 =
      vu.generateSelectVector<int32_t>(INTID, int32Values2, &nulls2, &lst2);

  int32Vect1->swap(*int32Vect2);
  EXPECT_EQ(int32Vect1->getNumOfRows(), 1);
  EXPECT_EQ(int32Vect2->getNumOfRows(), 2);
}

TEST(TestStringVector, TestNullBuffer) {
  StringVector vec(true);
  vec.append("abc", false);
  const dbcommon::BoolBuffer* bb = vec.getNullBuffer();
  EXPECT_EQ(bb->size(), 1);
  EXPECT_EQ(bb->get(0), false);
}

TEST(TestStringVector, BuildEmptyVector) {
  StringVector vec(true);
  EXPECT_TRUE(vec.getOwnData());
  EXPECT_TRUE(vec.isValid());
  EXPECT_EQ(0, vec.getNumOfRows());
}

TEST(TestStringVector, TestAppend) {
  StringVector vec(true);
  vec.append("abc", false);
  EXPECT_TRUE(vec.getOwnData());
  EXPECT_TRUE(vec.isValid());
  EXPECT_EQ(1, vec.getNumOfRows());
  bool null = false;
  EXPECT_EQ("abc", vec.Vector::read(0, &null));
  EXPECT_EQ(false, null);

  vec.append("def", false);
  EXPECT_TRUE(vec.isValid());
  EXPECT_EQ(2, vec.getNumOfRows());
  EXPECT_EQ("abc", vec.Vector::read(0, &null));
  EXPECT_EQ("def", vec.Vector::read(1, &null));
  EXPECT_EQ(false, null);

  auto vec1 = vec.clone();
  EXPECT_TRUE(vec1->isValid());
  EXPECT_EQ(2, vec1->getNumOfRows());
  EXPECT_EQ("abc", vec1->Vector::read(0, &null));
  EXPECT_EQ("def", vec1->Vector::read(1, &null));
  EXPECT_EQ(false, null);

  vec.append(vec1.get());
  EXPECT_TRUE(vec.isValid());
  EXPECT_EQ(4, vec.getNumOfRows());
  EXPECT_EQ(3, vec.getElementWidthPlain(0));
  EXPECT_EQ(3, vec.getElementWidthPlain(1));
  EXPECT_EQ("abc", vec.Vector::read(2, &null));
  EXPECT_EQ("def", vec.Vector::read(3, &null));
  EXPECT_EQ(false, null);

  DoubleVector dvec(true);
  dvec.append("1.0", false);
  dvec.append("2.0", false);
  dvec.append("3.0", false);

  SelectList sl;
  sl.push_back(1);
  dvec.setSelected(&sl, false);

  dvec.append("4.0", false);
  EXPECT_EQ(dvec.getNumOfRows(), 2);

  DoubleVector dvec1(true);
  dvec1.append("5.0", false);
  dvec.materialize();
  dvec.append(&dvec1);
  EXPECT_EQ(dvec.getNumOfRows(), 3);

  dvec.reset();
  EXPECT_EQ(dvec.getNumOfRows(), 0);
}

TEST(TestStringVector, TestReset) {
  StringVector vec(true);
  vec.append("abc", false);
  vec.append("def", false);

  vec.reset();
  EXPECT_TRUE(vec.isValid());
  EXPECT_EQ(0, vec.getNumOfRows());
}

TEST(TestStringVector, TestSelectList) {
  StringVector vec(true);
  vec.append("abc", false);
  vec.append("def", false);
  vec.append("123", false);
  EXPECT_EQ(3, vec.getNumOfRows());

  SelectList sl;
  sl.push_back(1);
  vec.setSelected(&sl, false);

  EXPECT_TRUE(vec.isValid());
  EXPECT_EQ(1, vec.getNumOfRows());
  EXPECT_EQ(3, vec.getNumOfRowsPlain());
  EXPECT_EQ(9, vec.getValueBuffer()->size());
  EXPECT_EQ(1, vec.getNullBitMapNumBytesPlain());

  vec.materialize();
  EXPECT_EQ(1, vec.getNumOfRows());
  EXPECT_EQ(1, vec.getNumOfRowsPlain());
  EXPECT_EQ(3, vec.getValueBuffer()->size());
  EXPECT_EQ(1, vec.getNullBitMapNumBytesPlain());
}
TEST(TestStringVector, TestConvert) {
  StringVector vec(true);
  bool null;
  vec.append("这", false);
  vec.append("是", false);
  vec.append("中文", false);
  vec.convert(dbcommon::MbConverter::checkSupportedEncoding("UTF-8"),
              dbcommon::MbConverter::checkSupportedEncoding("GBK"));
  EXPECT_EQ(3, vec.getNumOfRows());
  EXPECT_EQ("\xd5\xe2", vec.Vector::read(0, &null));
  EXPECT_EQ("\xca\xc7", vec.Vector::read(1, &null));
  EXPECT_EQ("\xd6\xd0\xce\xc4", vec.Vector::read(2, &null));
  SelectList sel = {0, 2};
  vec.setSelected(&sel, false);
  vec.convert(dbcommon::MbConverter::checkSupportedEncoding("GBK"),
              dbcommon::MbConverter::checkSupportedEncoding("UTF-8"));
  EXPECT_EQ(2, vec.getNumOfRows());
  EXPECT_EQ("这", vec.Vector::read(0, &null));
  EXPECT_EQ("中文", vec.Vector::read(1, &null));
  DoubleVector vec1(true);
  vec1.append("1.1", false);
  vec1.convert(dbcommon::MbConverter::checkSupportedEncoding("GBK"),
               dbcommon::MbConverter::checkSupportedEncoding("UTF-8"));
  EXPECT_EQ("1.1", vec1.Vector::read(0, &null));
}

TEST(TestTimestampVector, TestNullBuffer) {
  TimestampVector vec(true);
  vec.append("2018-01-21 17:12:22.01", false);
  const dbcommon::BoolBuffer* bb = vec.getNullBuffer();
  EXPECT_EQ(bb->size(), 1);
  EXPECT_EQ(bb->get(0), false);
}

TEST(TestTimestampVector, BuildEmptyVector) {
  TimestampVector vec(true);
  EXPECT_TRUE(vec.getOwnData());
  EXPECT_TRUE(vec.isValid());
  EXPECT_EQ(0, vec.getNumOfRows());
}

TEST(TestTimestampVector, TestAppend) {
  TimestampVector vec(true);
  vec.append("2018-01-21 17:12:22.01", false);
  EXPECT_TRUE(vec.getOwnData());
  EXPECT_TRUE(vec.isValid());
  EXPECT_EQ(1, vec.getNumOfRows());
  bool null = false;
  uint64_t len;
  EXPECT_EQ("2018-01-21 17:12:22.01", vec.Vector::read(0, &null));
  EXPECT_EQ(false, null);

  vec.append("2018-01-21 17:12:22", false);
  EXPECT_TRUE(vec.isValid());
  EXPECT_EQ(2, vec.getNumOfRows());
  EXPECT_EQ("2018-01-21 17:12:22.01", vec.Vector::read(0, &null));
  EXPECT_EQ("2018-01-21 17:12:22", vec.Vector::read(1, &null));
  EXPECT_EQ(false, null);

  auto vec1 = vec.clone();
  EXPECT_TRUE(vec1->isValid());
  EXPECT_EQ(2, vec1->getNumOfRows());
  EXPECT_EQ("2018-01-21 17:12:22.01", vec.Vector::read(0, &null));
  EXPECT_EQ("2018-01-21 17:12:22", vec.Vector::read(1, &null));
  EXPECT_EQ(false, null);

  vec.append(vec1.get());
  EXPECT_TRUE(vec.isValid());
  EXPECT_EQ(4, vec.getNumOfRows());
  EXPECT_EQ("2018-01-21 17:12:22.01", vec.Vector::read(2, &null));
  EXPECT_EQ("2018-01-21 17:12:22", vec.Vector::read(3, &null));
  EXPECT_EQ(false, null);
}

TEST(TestTimestamptzVector, TestAppend) {
  TimestamptzVector vec(true);
  TimezoneUtil::setGMTOffset("PRC");
  vec.append("2018-01-21 17:12:22.00123+08", false);
  EXPECT_TRUE(vec.getOwnData());
  EXPECT_TRUE(vec.isValid());
  EXPECT_EQ(1, vec.getNumOfRows());
  bool null = false;
  uint64_t len;
  EXPECT_EQ("2018-01-21 17:12:22.00123+08", vec.Vector::read(0, &null));
  EXPECT_EQ(false, null);

  vec.append("2019-01-21 12:12:41-02", false);
  EXPECT_TRUE(vec.isValid());
  EXPECT_EQ(2, vec.getNumOfRows());
  EXPECT_EQ("2018-01-21 17:12:22.00123+08", vec.Vector::read(0, &null));
  EXPECT_EQ("2019-01-21 22:12:41+08", vec.Vector::read(1, &null));
  EXPECT_EQ(false, null);
}

TEST(TestTimestampVector, TestReset) {
  TimestampVector vec(true);
  vec.append("2018-01-21 17:12:22.01", false);
  vec.append("2018-01-21 17:12:22", false);

  vec.reset();
  EXPECT_TRUE(vec.isValid());
  EXPECT_EQ(0, vec.getNumOfRows());
}

TEST(TestTimestampVector, TestSelectList) {
  TimestampVector vec(true);
  vec.append("2018-01-21 17:12:22.01", false);
  vec.append("2018-01-21 17:12:22", false);
  vec.append("2018-01-21 17:12:22.1 BC", false);
  EXPECT_EQ(3, vec.getNumOfRows());

  SelectList sl;
  sl.push_back(1);
  vec.setSelected(&sl, false);

  EXPECT_TRUE(vec.isValid());
  EXPECT_EQ(1, vec.getNumOfRows());
  EXPECT_EQ(3, vec.getNumOfRowsPlain());

  vec.materialize();
  EXPECT_EQ(1, vec.getNumOfRows());
  EXPECT_EQ(1, vec.getNumOfRowsPlain());
}

TEST(TestIOBaseTypeVector, TestBasic) {
  IOBaseTypeVector vec(true);
  //  append string
  const char* p = "abcdefg";
  vec.append(p, false);
  //  append null
  vec.append(nullptr, 0, true);

  int32_t x = 1;
  int32_t y = 2;
  //  append x&y
  vec.append(reinterpret_cast<char*>(&x), sizeof(int32_t), false);
  vec.append(reinterpret_cast<char*>(&y), sizeof(int32_t), false);

  //  read
  uint64_t len = -1;
  bool null = false;
  const char* pRet = vec.read((uint64_t)0, &len, &null);
  char* pStr = reinterpret_cast<char*>(malloc(sizeof(char) * (len + 1)));
  memcpy(pStr, pRet, len);
  pStr[len] = '\0';
  EXPECT_STREQ(p, pStr);
  EXPECT_EQ(7, len);
  EXPECT_EQ(false, null);
  free(pStr);

  pRet = vec.read((uint64_t)1, &len, &null);
  EXPECT_EQ(0, len);
  EXPECT_EQ(true, null);

  pRet = vec.read((uint64_t)2, &len, &null);
  int32_t tmp = *(reinterpret_cast<const int32_t*>(pRet));
  EXPECT_EQ(x, tmp);
  EXPECT_EQ(4, len);
  EXPECT_EQ(false, null);

  pRet = vec.read((uint64_t)3, &len, &null);
  tmp = *(reinterpret_cast<const int32_t*>(pRet));
  EXPECT_EQ(y, tmp);
  EXPECT_EQ(4, len);
  EXPECT_EQ(false, null);

  // reset
  vec.reset();
  EXPECT_TRUE(vec.isValid());
  EXPECT_EQ(0, vec.getNumOfRows());
}

TEST(TestStructExTypeVector, TestBasic) {
  StructExTypeVector vec(true);
  //  append string
  const char* p = "abcdefg";
  vec.append(p, false);
  //  append null
  vec.append(nullptr, 0, true);

  int32_t x = 1;
  int32_t y = 2;
  //  append x&y
  vec.append(reinterpret_cast<char*>(&x), sizeof(int32_t), false);
  vec.append(reinterpret_cast<char*>(&y), sizeof(int32_t), false);

  //  read
  uint64_t len = -1;
  bool null = false;
  const char* pRet = vec.read((uint64_t)0, &len, &null);
  char* pStr = reinterpret_cast<char*>(malloc(sizeof(char) * (len + 1)));
  memcpy(pStr, pRet, len);
  pStr[len] = '\0';
  EXPECT_STREQ(p, pStr);
  EXPECT_EQ(7, len);
  EXPECT_EQ(false, null);
  free(pStr);

  pRet = vec.read((uint64_t)1, &len, &null);
  EXPECT_EQ(0, len);
  EXPECT_EQ(true, null);

  pRet = vec.read((uint64_t)2, &len, &null);
  int32_t tmp = *(reinterpret_cast<const int32_t*>(pRet));
  EXPECT_EQ(x, tmp);
  EXPECT_EQ(4, len);
  EXPECT_EQ(false, null);

  pRet = vec.read((uint64_t)3, &len, &null);
  tmp = *(reinterpret_cast<const int32_t*>(pRet));
  EXPECT_EQ(y, tmp);
  EXPECT_EQ(4, len);
  EXPECT_EQ(false, null);

  // reset
  vec.reset();
  EXPECT_TRUE(vec.isValid());
  EXPECT_EQ(0, vec.getNumOfRows());
}

TEST(TestNumericVector, BuildEmptyVector) {
  NumericVector vec(true);
  EXPECT_TRUE(vec.getOwnData());
  EXPECT_TRUE(vec.isValid());
  EXPECT_EQ(0, vec.getNumOfRows());
}

TEST(TestNumericVector, TestAppendReadReset) {
  NumericVector vec(true);
  NumericVector vec2(true);
  bool null = false;
  vec.append("6212123", 7, false);  // char
  // EXPECT_EQ("6212123", vec.Vector::read(0, &null));
  // vec2.append(&vec);  // vector
  // EXPECT_EQ("6212123", vec2.Vector::read(0, &null));

  // reset
  vec.reset();
  EXPECT_TRUE(vec.isValid());
  EXPECT_EQ(0, vec.getNumOfRows());

  // reuse
  vec.append("6212123", 7, false);  // char
  vec.append("6212123", 7, false);  // vector
  // EXPECT_EQ("6212123", vec.Vector::read(1, &null));
}

TEST(TestDecimalVector, TestNullBuffer) {
  DecimalVector vec(true);
  vec.append("10000.22", false);
  const dbcommon::BoolBuffer* bb = vec.getNullBuffer();
  EXPECT_EQ(bb->size(), 1);
  EXPECT_EQ(bb->get(0), false);
}

TEST(TestDecimalVector, BuildEmptyVector) {
  DecimalVector vec(true);
  EXPECT_TRUE(vec.getOwnData());
  EXPECT_TRUE(vec.isValid());
  EXPECT_EQ(0, vec.getNumOfRows());
}

TEST(TestDecimalVector, TestReset) {
  DecimalVector vec(true);
  vec.append("8246194.5252", false);
  vec.append("0.248167412", false);
  vec.append("-218515.35241", false);

  vec.reset();
  EXPECT_TRUE(vec.isValid());
  EXPECT_EQ(0, vec.getNumOfRows());
}

TEST(TestDecimalVector, TestAppend) {
  DecimalVector vec(true);
  vec.append("1295712124.55124", false);
  EXPECT_TRUE(vec.getOwnData());
  EXPECT_TRUE(vec.isValid());
  EXPECT_EQ(1, vec.getNumOfRows());
  bool null = false;
  uint64_t len;
  EXPECT_EQ("1295712124.55124", vec.Vector::read(0, &null));
  EXPECT_EQ(false, null);

  vec.append("-6112214.2112", false);
  EXPECT_TRUE(vec.isValid());
  EXPECT_EQ(2, vec.getNumOfRows());
  EXPECT_EQ("1295712124.55124", vec.Vector::read(0, &null));
  EXPECT_EQ("-6112214.2112", vec.Vector::read(1, &null));
  EXPECT_EQ(false, null);

  auto vec1 = vec.clone();
  EXPECT_TRUE(vec1->isValid());
  EXPECT_EQ(2, vec1->getNumOfRows());
  EXPECT_EQ("1295712124.55124", vec.Vector::read(0, &null));
  EXPECT_EQ("-6112214.2112", vec.Vector::read(1, &null));
  EXPECT_EQ(false, null);

  vec.append(vec1.get());
  EXPECT_TRUE(vec.isValid());
  EXPECT_EQ(4, vec.getNumOfRows());
  EXPECT_EQ("1295712124.55124", vec.Vector::read(2, &null));
  EXPECT_EQ("-6112214.2112", vec.Vector::read(3, &null));
  EXPECT_EQ(false, null);
}

TEST(TestDecimalVector, TestSelectList) {
  DecimalVector vec(true);
  vec.append("100000000.2121", false);
  vec.append("200000000.3232", false);
  vec.append("-1111242141.5333", false);
  EXPECT_EQ(3, vec.getNumOfRows());
  SelectList sel;
  sel.push_back(1);
  vec.setSelected(&sel, false);

  EXPECT_TRUE(vec.isValid());
  EXPECT_EQ(1, vec.getNumOfRows());
  EXPECT_EQ(3, vec.getNumOfRowsPlain());

  vec.materialize();
  EXPECT_EQ(1, vec.getNumOfRows());
  EXPECT_EQ(1, vec.getNumOfRowsPlain());
}

TEST(TestVector, TestResize) {
  VectorUtility vu(TypeKind::BIGINTID);
  auto vec1 = vu.generateVector("NULL 0 0 0");
  auto vec2 = vu.generateVector("0 NULL 0 0");
  auto vec3 = vu.generateVector("0 0 NULL 0 0");

  auto vec = vu.generateVector("0 1 2 3");

  {
    LOG_TESTING("Without SelectList");
    {
      LOG_TESTING("Defalut paramater");
      vec->resize(4, nullptr, vec1->getNulls(), vec2->getNulls(),
                  vec3->getNulls());
      EXPECT_EQ("NULL NULL NULL 3 ", vec->toString());
      vec->resize(4, nullptr, vec1->getNulls(), vec2->getNulls());
      EXPECT_EQ("NULL NULL 2 3 ", vec->toString());
      vec->resize(4, nullptr, vec1->getNulls());
      EXPECT_EQ("NULL 1 2 3 ", vec->toString());
    }

    {
      LOG_TESTING("Multi-channel switch ");
      vec->resize(4, nullptr, vec1->getNulls(), vec2->getNulls(),
                  vec3->getNulls());
      EXPECT_EQ("NULL NULL NULL 3 ", vec->toString());
      vec->resize(4, nullptr, vec1->getNulls(), vec2->getNulls());
      EXPECT_EQ("NULL NULL 2 3 ", vec->toString());
      vec->resize(4, nullptr, vec1->getNulls(), nullptr, vec3->getNulls());
      EXPECT_EQ("NULL 1 NULL 3 ", vec->toString());
      vec->resize(4, nullptr, vec1->getNulls());
      EXPECT_EQ("NULL 1 2 3 ", vec->toString());
      vec->resize(4, nullptr, nullptr, vec2->getNulls(), vec3->getNulls());
      EXPECT_EQ("0 NULL NULL 3 ", vec->toString());
      vec->resize(4, nullptr, nullptr, vec2->getNulls());
      EXPECT_EQ("0 NULL 2 3 ", vec->toString());
      vec->resize(4, nullptr, nullptr, nullptr, vec3->getNulls());
      EXPECT_EQ("0 1 NULL 3 ", vec->toString());
      vec->resize(4, nullptr, false);
      EXPECT_EQ("0 1 2 3 ", vec->toString());
    }

    {
      LOG_TESTING("Fix null value");
      vec->resize(4, nullptr, true);
      EXPECT_EQ("NULL NULL NULL NULL ", vec->toString());

      vec->resize(4, nullptr, false);
      EXPECT_EQ("0 1 2 3 ", vec->toString());
    }
  }

  {
    LOG_TESTING("With SelectList");
    SelectList sel{2, 3};
    {
      LOG_TESTING("Defalut paramater");
      vec->resize(4, &sel, vec1->getNulls(), vec2->getNulls(),
                  vec3->getNulls());
      EXPECT_EQ("NULL 3 ", vec->toString());
      vec->resize(4, &sel, vec1->getNulls(), vec2->getNulls());
      EXPECT_EQ("2 3 ", vec->toString());
      vec->resize(4, &sel, vec1->getNulls());
      EXPECT_EQ("2 3 ", vec->toString());
    }

    {
      LOG_TESTING("Multi-channel switch ");
      vec->resize(4, &sel, vec1->getNulls(), vec2->getNulls(),
                  vec3->getNulls());
      EXPECT_EQ("NULL 3 ", vec->toString());
      vec->resize(4, &sel, vec1->getNulls(), vec2->getNulls());
      EXPECT_EQ("2 3 ", vec->toString());
      vec->resize(4, &sel, vec1->getNulls(), nullptr, vec3->getNulls());
      EXPECT_EQ("NULL 3 ", vec->toString());
      vec->resize(4, &sel, vec1->getNulls());
      EXPECT_EQ("2 3 ", vec->toString());
      vec->resize(4, &sel, nullptr, vec2->getNulls(), vec3->getNulls());
      EXPECT_EQ("NULL 3 ", vec->toString());
      vec->resize(4, &sel, nullptr, vec2->getNulls());
      EXPECT_EQ("2 3 ", vec->toString());
      vec->resize(4, &sel, nullptr, nullptr, vec3->getNulls());
      EXPECT_EQ("NULL 3 ", vec->toString());
      vec->resize(4, &sel, false);
      EXPECT_EQ("2 3 ", vec->toString());
    }

    {
      LOG_TESTING("Fix null value");
      vec->resize(4, &sel, true);
      EXPECT_EQ("NULL NULL ", vec->toString());

      vec->resize(4, &sel, false);
      EXPECT_EQ("2 3 ", vec->toString());
    }
  }
}

}  // namespace dbcommon
