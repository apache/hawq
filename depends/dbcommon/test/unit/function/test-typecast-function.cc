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

#include "dbcommon/function/typecast-func.cg.h"
#include "dbcommon/testutil/function-utils.h"
#include "dbcommon/testutil/scalar-utils.h"
#include "dbcommon/testutil/vector-utils.h"

namespace dbcommon {
class TestCastTypeToInteger : public ::testing::TestWithParam<FuncKind> {};
TEST_P(TestCastTypeToInteger, Test) {
  FuncKind funcId = GetParam();
  auto funcEnt = Func::instance()->getFuncEntryById(funcId);
  LOG_TESTING("%s", funcEnt->funcName.c_str());

  VectorUtility vuRettype(funcEnt->retType);
  VectorUtility vuSrctype(funcEnt->argTypes[0]);

  FunctionUtility fu(funcId);
  {
    LOG_TESTING("Basic");
    auto srcVec = vuSrctype.generateVector("NULL 2 1");
    auto retVec = vuRettype.generateVector("NULL 2 1");
    auto ret = Vector::BuildVector(funcEnt->retType, true);
    std::vector<Datum> params{CreateDatum(ret.get()),
                              CreateDatum(srcVec.get())};
    fu.test(params.data(), params.size(), CreateDatum(retVec.get()));
  }

  {
    LOG_TESTING("Overflow");
    std::string srcStr;
    if (TypeKind::TINYINTID == funcEnt->retType) {
      srcStr = "128 -129";
    }
    if (TypeKind::SMALLINTID == funcEnt->retType) {
      if (funcEnt->argTypes[0] == TypeKind::TINYINTID)
        srcStr.clear();
      else
        srcStr = "32768 -32769";
    }
    if (TypeKind::INTID == funcEnt->retType) {
      if (funcEnt->argTypes[0] == TypeKind::TINYINTID ||
          funcEnt->argTypes[0] == TypeKind::SMALLINTID ||
          funcEnt->argTypes[0] == TypeKind::FLOATID)
        srcStr.clear();
      else
        srcStr = "2147483648 -2147483649";
    }
    if (TypeKind::BIGINTID == funcEnt->retType) {
      srcStr.clear();
    }
    auto ret = Vector::BuildVector(funcEnt->retType, true);
    if (!srcStr.empty()) {
      auto src = vuSrctype.generateVector(srcStr);
      std::vector<Datum> params{CreateDatum(ret.get()), CreateDatum(src.get())};
      fu.test(params.data(), params.size(), CreateDatum(0),
              ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE);
    }
  }

  {
    LOG_TESTING("Minimum/Maximum range bound");
    std::string srcStr;
    std::string retStr;
    if (TypeKind::BIGINTID == funcEnt->retType) {
      srcStr.clear();
      retStr.clear();
    }
    if (TypeKind::INTID == funcEnt->retType) {
      if (funcEnt->argTypes[0] == TypeKind::DOUBLEID) {
        srcStr = "2147483646.6 -2147483647.6";
        retStr = "2147483647 -2147483648";
      }
      if (funcEnt->argTypes[0] == TypeKind::BIGINTID) {
        srcStr = "2147483647 -2147483648";
        retStr = "2147483647 -2147483648";
      }
      if (funcEnt->argTypes[0] == TypeKind::TINYINTID ||
          funcEnt->argTypes[0] == TypeKind::SMALLINTID ||
          funcEnt->argTypes[0] == TypeKind::FLOATID) {
        srcStr.clear();
        retStr.clear();
      }
    }
    if (TypeKind::SMALLINTID == funcEnt->retType) {
      if (funcEnt->argTypes[0] == TypeKind::FLOATID ||
          funcEnt->argTypes[0] == TypeKind::DOUBLEID) {
        srcStr = "32766.6 -32767.6";
        retStr = "32767 -32768";
      }
      if (funcEnt->argTypes[0] == TypeKind::INTID ||
          funcEnt->argTypes[0] == TypeKind::BIGINTID) {
        srcStr = "32767 -32768";
        retStr = "32767 -32768";
      }
      if (funcEnt->argTypes[0] == TypeKind::TINYINTID) {
        srcStr.clear();
        retStr.clear();
      }
    }
    if (TypeKind::TINYINTID == funcEnt->retType) {
      if (funcEnt->argTypes[0] == TypeKind::FLOATID ||
          funcEnt->argTypes[0] == TypeKind::DOUBLEID) {
        srcStr = "126.6 -127.6";
        retStr = "127 -128";
      }
      if (funcEnt->argTypes[0] == TypeKind::SMALLINTID ||
          funcEnt->argTypes[0] == TypeKind::INTID ||
          funcEnt->argTypes[0] == TypeKind::BIGINTID) {
        srcStr = "127 -128";
        retStr = "127 -128";
      }
    }
    if (!srcStr.empty()) {
      auto ret = Vector::BuildVector(funcEnt->retType, true);
      auto src = vuSrctype.generateVector(srcStr);
      auto expect = vuRettype.generateVector(retStr);
      std::vector<Datum> params{CreateDatum(ret.get()), CreateDatum(src.get())};
      fu.test(params.data(), params.size(), CreateDatum(expect.get()));
    }
  }
}

INSTANTIATE_TEST_CASE_P(
    TestCastTypeFunction, TestCastTypeToInteger,
    ::testing::Values(FuncKind::TINYINT_TO_SMALLINT, FuncKind::TINYINT_TO_INT,
                      FuncKind::TINYINT_TO_BIGINT,
                      FuncKind::SMALLINT_TO_TINYINT, FuncKind::SMALLINT_TO_INT,
                      FuncKind::SMALLINT_TO_BIGINT, FuncKind::INT_TO_TINYINT,
                      FuncKind::INT_TO_SMALLINT, FuncKind::INT_TO_BIGINT,
                      FuncKind::BIGINT_TO_TINYINT, FuncKind::BIGINT_TO_SMALLINT,
                      FuncKind::BIGINT_TO_INT, FuncKind::FLOAT_TO_TINYINT,
                      FuncKind::FLOAT_TO_SMALLINT, FuncKind::FLOAT_TO_INT,
                      FuncKind::FLOAT_TO_BIGINT, FuncKind::DOUBLE_TO_TINYINT,
                      FuncKind::DOUBLE_TO_SMALLINT, FuncKind::DOUBLE_TO_INT,
                      FuncKind::DOUBLE_TO_BIGINT));

TEST(TestFunction, int2_to_text) {
  FunctionUtility fu(FuncKind::SMALLINT_TO_TEXT);
  LOG_TESTING("Basic");
  auto ret = fu.generatePara<0, Vector>("NULL");
  auto expect = fu.generatePara<0, Vector>("-32768 32767 12 -1 NULL");
  auto strs = fu.generatePara<1, Vector>("-32768 32767 12 -1 NULL");
  std::vector<Datum> params{ret, strs};
  fu.test(params.data(), params.size(), expect);
}

TEST(TestFunction, int4_to_text) {
  FunctionUtility fu(FuncKind::INT_TO_TEXT);
  LOG_TESTING("Basic");
  auto ret = fu.generatePara<0, Vector>("NULL");
  auto expect =
      fu.generatePara<0, Vector>("2147483647 -2147483648 3456 -3456 NULL");
  auto strs =
      fu.generatePara<1, Vector>("2147483647 -2147483648 3456 -3456 NULL");
  std::vector<Datum> params{ret, strs};
  fu.test(params.data(), params.size(), expect);
}

TEST(TestFunction, int8_to_text) {
  FunctionUtility fu(FuncKind::BIGINT_TO_TEXT);
  LOG_TESTING("Basic");
  auto ret = fu.generatePara<0, Vector>("NULL");
  auto expect = fu.generatePara<0, Vector>(
      "9223372036854775807 -9223372036854775808 0 10 100 1000 10000 100000 "
      "1000000 10000000 100000000 1000000000 10000000000 100000000000 "
      "1000000000000 10000000000000 100000000000000 1000000000000000 "
      "10000000000000000 100000000000000000 1000000000000000000 NULL");
  auto strs = fu.generatePara<1, Vector>(
      "9223372036854775807 -9223372036854775808 0 10 100 1000 10000 100000 "
      "1000000 10000000 100000000 1000000000 10000000000 100000000000 "
      "1000000000000 10000000000000 100000000000000 1000000000000000 "
      "10000000000000000 100000000000000000 1000000000000000000 NULL");
  std::vector<Datum> params{ret, strs};
  fu.test(params.data(), params.size(), expect);
}

INSTANTIATE_TEST_CASE_P(
    float4_to_text, TestFunction,
    ::testing::Values(TestFunctionEntry{
        FuncKind::FLOAT_TO_TEXT,
        "Vector: 2147.6 -2.14748e+09 111111 2147.11 -3456 0 NULL",
        {"Vector: 2147.600 -2147483648 111111 2147.106 -3456 0 NULL"}}));

INSTANTIATE_TEST_CASE_P(
    float8_to_text, TestFunction,
    ::testing::Values(TestFunctionEntry{
        FuncKind::DOUBLE_TO_TEXT,
        "Vector: -214741777777.101 2147.6 214741.101 -3456 NULL",
        {"Vector: -214741777777.10110 2147.600 214741.1010 -3456 NULL"}}));

INSTANTIATE_TEST_CASE_P(bool_to_text, TestFunction,
                        ::testing::Values(TestFunctionEntry{
                            FuncKind::BOOL_TO_TEXT,
                            "Vector: true false NULL",
                            {"Vector: t f NULL"}}));

INSTANTIATE_TEST_CASE_P(text_to_char, TestFunction,
                        ::testing::Values(TestFunctionEntry{
                            FuncKind::TEXT_TO_CHAR,
                            "Vector: 45 97 42 NULL 0",
                            {"Vector: -1 abcd **% NULL 中国"}}));

INSTANTIATE_TEST_CASE_P(
    int4_to_char, TestFunction,
    ::testing::Values(TestFunctionEntry{FuncKind::INT_TO_CHAR,
                                        "Vector: 127 3 0 -1 NULL",
                                        {"Vector: 127 3 0 -1 NULL"}},
                      TestFunctionEntry{FuncKind::INT_TO_CHAR,
                                        "Error",
                                        {"Vector: 128"},
                                        ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE},
                      TestFunctionEntry{FuncKind::INT_TO_CHAR,
                                        "Error",
                                        {"Vector: -129"},
                                        ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE}));

INSTANTIATE_TEST_CASE_P(
    decimal_to_text, TestFunction,
    ::testing::Values(TestFunctionEntry{
        FuncKind::DECIMAL_TO_TEXT,
        "Vector: 2147.600 214741.1010 -3456 -214741777.10110 0 10 100 1000 "
        "10000 100000 1000000 10000000 100000000 1000000000 10000000000 "
        "100000000000 1000000000000 10000000000000 100000000000000 "
        "1000000000000000 10000000000000000 100000000000000000 "
        "1000000000000000000 10000000000000000000 100000000000000000000 "
        "1000000000000000000000 10000000000000000000000 "
        "100000000000000000000000 1000000000000000000000000 "
        "10000000000000000000000000 100000000000000000000000000 "
        "1000000000000000000000000000 10000000000000000000000000000 "
        "100000000000000000000000000000 1000000000000000000000000000000 "
        "10000000000000000000000000000000 100000000000000000000000000000000 "
        "1000000000000000000000000000000000 "
        "10000000000000000000000000000000000 "
        "100000000000000000000000000000000000 "
        "1000000000000000000000000000000000000 "
        "10000000000000000000000000000000000000 "
        "100000000000000000000000000000000000000 "
        "-170141183460469231731687303715884105728 NULL",
        {"Vector: 2147.600 214741.1010 -3456 -214741777.10110 0 10 100 1000 "
         "10000 100000 1000000 10000000 100000000 1000000000 10000000000 "
         "100000000000 1000000000000 10000000000000 100000000000000 "
         "1000000000000000 10000000000000000 100000000000000000 "
         "1000000000000000000 10000000000000000000 100000000000000000000 "
         "1000000000000000000000 10000000000000000000000 "
         "100000000000000000000000 1000000000000000000000000 "
         "10000000000000000000000000 100000000000000000000000000 "
         "1000000000000000000000000000 10000000000000000000000000000 "
         "100000000000000000000000000000 1000000000000000000000000000000 "
         "10000000000000000000000000000000 100000000000000000000000000000000 "
         "1000000000000000000000000000000000 "
         "10000000000000000000000000000000000 "
         "100000000000000000000000000000000000 "
         "1000000000000000000000000000000000000 "
         "10000000000000000000000000000000000000 "
         "100000000000000000000000000000000000000 "
         "-170141183460469231731687303715884105728 NULL"}}));

INSTANTIATE_TEST_CASE_P(
    bool_cast_int, TestFunction,
    ::testing::Values(TestFunctionEntry{FuncKind::BOOLEAN_TO_SMALLINT,
                                        "Vector: 1 0 NULL",
                                        {"Vector: t f NULL"}},
                      TestFunctionEntry{FuncKind::BOOLEAN_TO_INT,
                                        "Vector: 1 0 NULL",
                                        {"Vector: t f NULL"}},
                      TestFunctionEntry{FuncKind::BOOLEAN_TO_BIGINT,
                                        "Vector: 1 0 NULL",
                                        {"Vector: t f NULL"}},
                      TestFunctionEntry{FuncKind::SMALLINT_TO_BOOLEAN,
                                        "Vector: t t t f NULL",
                                        {"Vector: 1 3 -2 0 NULL"}},
                      TestFunctionEntry{FuncKind::INT_TO_BOOLEAN,
                                        "Vector: t t t f NULL",
                                        {"Vector: 1 3 -2 0 NULL"}},
                      TestFunctionEntry{FuncKind::BIGINT_TO_BOOLEAN,
                                        "Vector: t t t f NULL",
                                        {"Vector: 1 3 -2 0 NULL"}}));
// TEST(TestFunction, double_to_timestamp) {}
INSTANTIATE_TEST_CASE_P(
    double_to_timestamp, TestFunction,
    ::testing::Values(TestFunctionEntry{
        FuncKind::DOUBLE_TO_TIMESTAMP,
        "Vector{delimiter=,}: 1927-12-31 23:54:08+08,1973-11-30 "
        "05:33:09+08,NULL",
        {"Vector{delimiter=,}: -1325491552,NULL"}}));

INSTANTIATE_TEST_CASE_P(
    to_number, TestFunction,
    ::testing::Values(
        TestFunctionEntry{
            FuncKind::TO_NUMBER,
            "Vector: -34338492 -34338492.654878 -0.00001 -5.01 -5.01 0.01 0.0 "
            "0 "
            "-0.01 -564646.654564 -0.01 NULL",
            {"Vector: -34,338,492 -34,338,492.654,878 0.00001- 5.01- 5.01- .01 "
             ".0 "
             "0 .01- <564646.654564> .-01 NULL",
             "Vector: 99G999G999 99G999G999D999G999 9.999999S FM9.999999S "
             "FM9.999999MI FM9.99 99999999.99999999 99.99 TH99.99S "
             "999999.999999PR "
             "S99.99 NULL"}},
        TestFunctionEntry{FuncKind::TO_NUMBER,
                          "Error",
                          {"Vector: -34,338,492 -34,338,492.654,878 0.00001- "
                           "5.01- 5.01- .01 .0 "
                           "0 .01- <564646.654564> 111.11",
                           "Vector: 99G999G99Ss9 99G999G999D.999G999 "
                           "MIMI9.999999S FM9.99999PR9S "
                           "FM9.999999PLS FM9.99SPL 99999999.99999999PRS "
                           "99.9PR0 99.99SPR 999999.999999RN "
                           "99.99"},
                          ERRCODE_INVALID_TEXT_REPRESENTATION}));

}  // namespace dbcommon
