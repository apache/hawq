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

#include "dbcommon/function/decimal-function.h"
#include "dbcommon/testutil/function-utils.h"
#include "dbcommon/testutil/scalar-utils.h"
#include "dbcommon/testutil/vector-utils.h"
#include "gtest/gtest.h"

namespace dbcommon {

class TestCastDecimalToInteger : public ::testing::TestWithParam<FuncKind> {};
TEST_P(TestCastDecimalToInteger, Test) {
  FuncKind funcId = GetParam();
  auto funcEnt = Func::instance()->getFuncEntryById(funcId);
  LOG_TESTING("%s", funcEnt->funcName.c_str());

  VectorUtility vuInt(funcEnt->retType);
  VectorUtility vuDecimal(TypeKind::DECIMALNEWID);
  FunctionUtility fu(funcId);

  {
    LOG_TESTING("Basic");
    auto decimalVec =
        vuDecimal.generateVector("-123 23.5 NULL 38.24 -12.4 -14.5 1.4");
    auto intVec = vuInt.generateVector("-123 24 NULL 38 -12 -15 1");
    auto ret = Vector::BuildVector(funcEnt->retType, true);

    std::vector<Datum> params{CreateDatum(ret.get()),
                              CreateDatum(decimalVec.get())};
    fu.test(params.data(), params.size(), CreateDatum(intVec.get()));
  }

  {
    LOG_TESTING("Overflow");
    std::string srcStr("88888888888888888888.88 -88888888888888888888.88");
    std::string placeholderStr("0 0");
    if (TypeKind::BIGINTID == funcEnt->retType) {
      srcStr += " 9223372036854775808 -9223372036854775809";
      placeholderStr += " 0 0";
    }
    if (TypeKind::INTID == funcEnt->retType) {
      srcStr += " 2147483648 -2147483649";
      placeholderStr += " 0 0";
    }
    if (TypeKind::SMALLINTID == funcEnt->retType) {
      srcStr += " 32768 -32769";
      placeholderStr += " 0 0";
    }
    auto ret = Vector::BuildVector(funcEnt->retType, true);
    auto src = vuDecimal.generateVector(srcStr);
    std::vector<Datum> params{CreateDatum(ret.get()), CreateDatum(src.get())};
    fu.test(params.data(), params.size(), CreateDatum(0),
            ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE);
  }

  {
    LOG_TESTING("Minimum/Maximum range bound");
    std::string srcStr("NULL NULL");
    std::string retStr("NULL NULL");
    if (TypeKind::BIGINTID == funcEnt->retType) {
      srcStr += " 9223372036854775807.4 -9223372036854775808.4";
      retStr += " 9223372036854775807 -9223372036854775808";
    }
    if (TypeKind::INTID == funcEnt->retType) {
      srcStr += " 2147483647.4 -2147483648.4";
      retStr += " 2147483647 -2147483648";
    }
    if (TypeKind::SMALLINTID == funcEnt->retType) {
      srcStr += " 32767.4 -32768.4";
      retStr += " 32767 -32768";
    }
    if (TypeKind::TINYINTID == funcEnt->retType) {
      srcStr += " 127.4 -128.4";
      retStr += " 127 -128";
    }
    auto ret = Vector::BuildVector(funcEnt->retType, true);
    auto src = vuDecimal.generateVector(srcStr);
    auto expect = vuInt.generateVector(retStr);
    std::vector<Datum> params{CreateDatum(ret.get()), CreateDatum(src.get())};
    fu.test(params.data(), params.size(), CreateDatum(expect.get()));
  }
}
INSTANTIATE_TEST_CASE_P(TestDecimalFunction, TestCastDecimalToInteger,
                        ::testing::Values(FuncKind::DECIMAL_TO_BIGINT,
                                          FuncKind::DECIMAL_TO_INT,
                                          FuncKind::DECIMAL_TO_SMALLINT,
                                          FuncKind::DECIMAL_TO_TINYINT));

class TestCastDecimalToFloat : public ::testing::TestWithParam<FuncKind> {};
TEST_P(TestCastDecimalToFloat, Test) {
  FuncKind funcId = GetParam();
  auto funcEnt = Func::instance()->getFuncEntryById(funcId);
  LOG_TESTING("%s", funcEnt->funcName.c_str());

  VectorUtility vuFloat(funcEnt->retType);
  VectorUtility vuDecimal(TypeKind::DECIMALNEWID);

  auto ret = Vector::BuildVector(funcEnt->retType, true);
  auto floatVec = vuFloat.generateVector("-3.14159 23.5 NULL 38.888");
  auto decimalVec = vuDecimal.generateVector("-3.14159 23.5 NULL 38.888");

  FunctionUtility fu(funcId);
  std::vector<Datum> params{CreateDatum(ret.get()),
                            CreateDatum(decimalVec.get())};
  fu.test(params.data(), params.size(), CreateDatum(floatVec.get()));
}
INSTANTIATE_TEST_CASE_P(TestDecimalFunction, TestCastDecimalToFloat,
                        ::testing::Values(FuncKind::DECIMAL_TO_DOUBLE,
                                          FuncKind::DECIMAL_TO_FLOAT));

class TestCastIntegerToDecimal : public ::testing::TestWithParam<FuncKind> {};
TEST_P(TestCastIntegerToDecimal, Test) {
  FuncKind funcId = GetParam();
  auto funcEnt = Func::instance()->getFuncEntryById(funcId);
  LOG_TESTING("%s", funcEnt->funcName.c_str());

  VectorUtility vuInt(funcEnt->argTypes[0]);
  VectorUtility vuDecimal(TypeKind::DECIMALNEWID);

  auto ret = Vector::BuildVector(TypeKind::DECIMALNEWID, true);
  auto intVec = vuInt.generateVector("123 24 NULL 38 -12");
  auto decimalVec = vuDecimal.generateVector("123 24 NULL 38 -12");

  FunctionUtility fu(funcId);
  std::vector<Datum> params{CreateDatum(ret.get()), CreateDatum(intVec.get())};
  fu.test(params.data(), params.size(), CreateDatum(decimalVec.get()));
}
INSTANTIATE_TEST_CASE_P(TestDecimalFunction, TestCastIntegerToDecimal,
                        ::testing::Values(FuncKind::BIGINT_TO_DECIMAL,
                                          FuncKind::INT_TO_DECIMAL,
                                          FuncKind::SMALLINT_TO_DECIMAL,
                                          FuncKind::TINYINT_TO_DECIMAL));

class TestCastFloatToDecimal : public ::testing::TestWithParam<FuncKind> {};
TEST_P(TestCastFloatToDecimal, Test) {
  FuncKind funcId = GetParam();
  auto funcEnt = Func::instance()->getFuncEntryById(funcId);
  LOG_TESTING("%s", funcEnt->funcName.c_str());

  VectorUtility vuFloat(funcEnt->argTypes[0]);
  VectorUtility vuDecimal(TypeKind::DECIMALNEWID);

  auto ret = Vector::BuildVector(TypeKind::DECIMALNEWID, true);
  auto floatVec = vuFloat.generateVector("-3.14159 23.5 NULL 38.888");
  auto decimalVec = vuDecimal.generateVector("-3.14159 23.5 NULL 38.888");

  FunctionUtility fu(funcId);
  std::vector<Datum> params{CreateDatum(ret.get()),
                            CreateDatum(floatVec.get())};
  fu.test(params.data(), params.size(), CreateDatum(decimalVec.get()));
}
INSTANTIATE_TEST_CASE_P(TestDecimalFunction, TestCastFloatToDecimal,
                        ::testing::Values(FuncKind::DOUBLE_TO_DECIMAL,
                                          FuncKind::FLOAT_TO_DECIMAL));

TEST(TestDecimalFunction, TestCastDecimalToDecimal) {
  ScalarUtility suInt(TypeKind::INTID);
  VectorUtility vuDecimal(TypeKind::DECIMALNEWID);

  {
    LOG_TESTING("Cast to smaller scale");
    auto typemod = TypeModifierUtil::getTypeModifierFromPrecisionScale(15, 3);
    auto decimalSrcVec = vuDecimal.generateVector("-3.14159 23.5 NULL 8.4444");
    auto decimalDstVec = vuDecimal.generateVector("-3.142 23.500 NULL 8.444");
    auto typemodScalar = suInt.generateScalar(std::to_string((int32_t)typemod));
    auto ret = Vector::BuildVector(TypeKind::DECIMALNEWID, true);

    FunctionUtility fu(FuncKind::DECIMAL_TO_DECIMAL);
    std::vector<Datum> params{CreateDatum(ret.get()),
                              CreateDatum(decimalSrcVec.get()),
                              CreateDatum(&typemodScalar)};
    fu.test(params.data(), params.size(), CreateDatum(decimalDstVec.get()));
  }

  {
    LOG_TESTING("Cast to larger scale");
    auto typemod = TypeModifierUtil::getTypeModifierFromPrecisionScale(10, 5);
    auto decimalSrcVec = vuDecimal.generateVector("-3.14159 23.5 NULL 38.888");
    auto decimalDstVec =
        vuDecimal.generateVector("-3.14159 23.50000 NULL 38.88800");
    auto typemodScalar = suInt.generateScalar(std::to_string((int32_t)typemod));
    auto ret = Vector::BuildVector(TypeKind::DECIMALNEWID, true);

    FunctionUtility fu(FuncKind::DECIMAL_TO_DECIMAL);
    std::vector<Datum> params{CreateDatum(ret.get()),
                              CreateDatum(decimalSrcVec.get()),
                              CreateDatum(&typemodScalar)};
    fu.test(params.data(), params.size(), CreateDatum(decimalDstVec.get()));
  }
}

TEST(TestDecimalFunction, TestDecimalSign) {
  FunctionUtility fu(FuncKind::DECIMAL_SIGN);

  auto ret = fu.generatePara<0, Vector>("NULL");
  auto src = fu.generatePara<1, Vector>("2.34 0 -7.5 NULL");
  auto expect = fu.generatePara<0, Vector>("1 0 -1 NULL");

  std::vector<Datum> params{ret, src};
  fu.test(params.data(), params.size(), expect);
}

TEST(TestDecimalFunction, TestDecimalCeil) {
  FunctionUtility fu(FuncKind::DECIMAL_CEIL);

  auto ret = fu.generatePara<0, Vector>("NULL");
  auto src = fu.generatePara<1, Vector>("2.3400 0.00 -7.5000 NULL");
  auto expect = fu.generatePara<0, Vector>("3 0 -7 NULL");

  std::vector<Datum> params{ret, src};
  fu.test(params.data(), params.size(), expect);
}

TEST(TestDecimalFunction, TestDecimalFloor) {
  FunctionUtility fu(FuncKind::DECIMAL_FLOOR);

  auto ret = fu.generatePara<0, Vector>("NULL");
  auto src = fu.generatePara<1, Vector>("2.3400 0.00 -7.5000 NULL");
  auto expect = fu.generatePara<0, Vector>("2 0 -8 NULL");

  std::vector<Datum> params{ret, src};
  fu.test(params.data(), params.size(), expect);
}

// Test decimal round
TEST(TestDecimalFunction, TestDecimalRound) {
  FunctionUtility fu(FuncKind::DECIMAL_ROUND);

  {
    LOG_TESTING("{Scalar, Scalar} argument");
    auto ret = fu.generatePara<0, Scalar>("0");
    auto lhs = fu.generatePara<1, Scalar>("-7.5000");
    auto rhs = fu.generatePara<2, Scalar>("5");
    auto expect = fu.generatePara<0, Scalar>("-7.50000");

    std::vector<Datum> params{ret, lhs, rhs};
    fu.test(params.data(), params.size(), expect);
  }

  {
    LOG_TESTING("{Scalar, Vector} argument");
    auto ret = fu.generatePara<0, Vector>("NULL");
    auto lhs = fu.generatePara<1, Scalar>("2.3400");
    auto rhs = fu.generatePara<2, Vector>("-2 5 NULL 4");
    auto expect = fu.generatePara<0, Vector>("0 2.34000 NULL 2.3400");

    std::vector<Datum> params{ret, lhs, rhs};
    fu.test(params.data(), params.size(), expect);
  }

  {
    LOG_TESTING("{Vector, Scalar} argument");
    auto ret = fu.generatePara<0, Vector>("NULL");
    auto lhs = fu.generatePara<1, Vector>("2.3400 0.00 -7.500 NULL");
    auto rhs = fu.generatePara<2, Scalar>("1");
    auto expect = fu.generatePara<0, Vector>("2.3 0.0 -7.5 NULL");

    std::vector<Datum> params{ret, lhs, rhs};
    fu.test(params.data(), params.size(), expect);
  }

  {
    LOG_TESTING("{Vector, Vector} argument");
    auto ret = fu.generatePara<0, Vector>("NULL");
    auto lhs = fu.generatePara<1, Vector>("12553.45520 0.00 -7.5000 NULL");
    auto rhs = fu.generatePara<2, Vector>("-2 1 0 1");
    auto expect = fu.generatePara<0, Vector>("12600 0 -8 NULL");

    std::vector<Datum> params{ret, lhs, rhs};
    fu.test(params.data(), params.size(), expect);
  }
}

TEST(TestDecimalFunction, TestDecimalRoundWithoutScale) {
  FunctionUtility fu(FuncKind::DECIMAL_ROUND_WITHOUT_SCALE);
  auto ret = fu.generatePara<0, Vector>("NULL");
  auto lhs = fu.generatePara<1, Vector>("2.3400 0.00 -7.5000 NULL");
  auto expect = fu.generatePara<0, Vector>("2 0 -8 NULL");

  std::vector<Datum> params{ret, lhs};
  fu.test(params.data(), params.size(), expect);
}

// Test decimal trunc
TEST(TestDecimalFunction, TestDecimalTrunc) {
  FunctionUtility fu(FuncKind::DECIMAL_TRUNC);

  {
    auto ret = fu.generatePara<0, Scalar>("0");
    auto lhs = fu.generatePara<1, Scalar>("-7.5000");
    auto rhs = fu.generatePara<2, Scalar>("5");
    auto expect = fu.generatePara<0, Scalar>("-7.50000");

    std::vector<Datum> params{ret, lhs, rhs};
    fu.test(params.data(), params.size(), expect);
  }

  {
    LOG_TESTING("{Scalar, Vector} argument");
    auto ret = fu.generatePara<0, Vector>("NULL");
    auto lhs = fu.generatePara<1, Scalar>("2.3400");
    auto rhs = fu.generatePara<2, Vector>("-2 5 NULL 4");
    auto expect = fu.generatePara<0, Vector>("0 2.34000 NULL 2.3400");

    std::vector<Datum> params{ret, lhs, rhs};
    fu.test(params.data(), params.size(), expect);
  }

  {
    LOG_TESTING("{Vector, Scalar} argument");
    auto ret = fu.generatePara<0, Vector>("NULL");
    auto lhs = fu.generatePara<1, Vector>("2.3400 0.00 -7.500 NULL");
    auto rhs = fu.generatePara<2, Scalar>("1");
    auto expect = fu.generatePara<0, Vector>("2.3 0.0 -7.5 NULL");

    std::vector<Datum> params{ret, lhs, rhs};
    fu.test(params.data(), params.size(), expect);
  }

  {
    LOG_TESTING("{Vector, Vector} argument");
    auto ret = fu.generatePara<0, Vector>("NULL");
    auto lhs = fu.generatePara<1, Vector>("12553.45520 0.00 -7.5000 NULL");
    auto rhs = fu.generatePara<2, Vector>("-2 1 2 1");
    auto expect = fu.generatePara<0, Vector>("12500 0 -7.50 NULL");

    std::vector<Datum> params{ret, lhs, rhs};
    fu.test(params.data(), params.size(), expect);
  }
}

TEST(TestDecimalFunction, TestDecimalTruncWithoutScale) {
  FunctionUtility fu(FuncKind::DECIMAL_TRUNC_WITHOUT_SCALE);
  auto ret = fu.generatePara<0, Vector>("NULL");
  auto lhs = fu.generatePara<1, Vector>("2.3400 0.00 -7.5000 NULL");
  auto expect = fu.generatePara<0, Vector>("2 0 -7 NULL");

  std::vector<Datum> params{ret, lhs};
  fu.test(params.data(), params.size(), expect);
}

TEST(TestDecimalFunction, TestDecimalMod) {
  FunctionUtility fu(FuncKind::DECIMAL_MOD);

  {
    LOG_TESTING("{Scalar, Scalar} argument");
    auto ret = fu.generatePara<0, Scalar>("NULL");
    auto lhs = fu.generatePara<1, Scalar>("2.0");
    auto rhs = fu.generatePara<2, Scalar>("1.530");
    auto expect = fu.generatePara<1, Scalar>("0.47");

    std::vector<Datum> params{ret, lhs, rhs};
    fu.test(params.data(), params.size(), expect);
  }

  {
    LOG_TESTING("{Scalar, Vector} argument");
    auto ret = fu.generatePara<0, Vector>("NULL");
    auto lhs = fu.generatePara<1, Scalar>("2.49");
    auto rhs = fu.generatePara<2, Vector>("1.45 3 4 NULL");
    auto expect = fu.generatePara<1, Vector>("1.04 2.49 2.49 NULL");

    std::vector<Datum> params{ret, lhs, rhs};
    fu.test(params.data(), params.size(), expect);
  }

  {
    LOG_TESTING("{Scalar, Vector} argument");
    auto ret = fu.generatePara<0, Vector>("NULL");
    auto lhs = fu.generatePara<1, Vector>("1.45 3 4 NULL");
    auto rhs = fu.generatePara<2, Scalar>("2.49");
    auto expect = fu.generatePara<1, Vector>("1.45 0.51 1.51 NULL");

    std::vector<Datum> params{ret, lhs, rhs};
    fu.test(params.data(), params.size(), expect);
  }

  {
    LOG_TESTING("{Vector, Vector} argument");
    auto ret = fu.generatePara<0, Vector>("NULL");
    auto lhs = fu.generatePara<1, Vector>("8.3400 2.0 NULL 0");
    auto rhs = fu.generatePara<2, Vector>("2.0 1.530 4.32 NULL");
    auto expect = fu.generatePara<1, Vector>("0.34 0.47 NULL NULL");

    std::vector<Datum> params{ret, lhs, rhs};
    fu.test(params.data(), params.size(), expect);
  }

  {
    LOG_TESTING("{Vector, Vector} argument");
    auto ret = fu.generatePara<0, Vector>("NULL");
    auto lhs = fu.generatePara<1, Vector>("1 2 1 1");
    auto rhs = fu.generatePara<2, Scalar>("0");
    auto expect = CreateDatum(0);

    std::vector<Datum> params{ret, lhs, rhs};
    fu.test(params.data(), params.size(), expect, ERRCODE_DIVISION_BY_ZERO);
  }
}

}  // namespace dbcommon
