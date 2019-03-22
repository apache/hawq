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

#include "dbcommon/function/mathematical-function.h"

#include "dbcommon/testutil/function-utils.h"
#include "dbcommon/testutil/scalar-utils.h"
#include "dbcommon/testutil/vector-utils.h"

namespace dbcommon {

class TestAbsFunction : public ::testing::TestWithParam<FuncKind> {};
TEST_P(TestAbsFunction, Test) {
  FuncKind funcId = GetParam();
  auto funcEnt = Func::instance()->getFuncEntryById(funcId);

  LOG_TESTING("%s", funcEnt->funcName.c_str());

  FunctionUtility fu(funcId);
  VectorUtility vuType(funcEnt->retType);

  auto src = vuType.generateVector("1.244 NULL 0.00 -1.111");
  auto absRes = vuType.generateVector("1.244 NULL 0.00 1.111");
  auto ret = Vector::BuildVector(funcEnt->retType, true);

  std::vector<Datum> params{CreateDatum(ret.get()), CreateDatum(src.get())};
  fu.test(params.data(), params.size(), CreateDatum(absRes.get()));
}

INSTANTIATE_TEST_CASE_P(
    TestFunction, TestAbsFunction,
    ::testing::Values(FuncKind::DOUBLE_ABS, FuncKind::FLOAT_ABS,
                      FuncKind::INT64_ABS, FuncKind::INT32_ABS,
                      FuncKind::INT16_ABS, FuncKind::DECIMAL_ABS));

class TestCbrtFunction : public ::testing::TestWithParam<FuncKind> {};
TEST_P(TestCbrtFunction, Test) {
  FuncKind funcId = GetParam();
  auto funcEnt = Func::instance()->getFuncEntryById(funcId);

  LOG_TESTING("%s", funcEnt->funcName.c_str());

  FunctionUtility fu(funcId);
  VectorUtility vuType(funcEnt->retType);

  auto src = vuType.generateVector("1 8 NULL 0");
  auto cbrtRes = vuType.generateVector("1 2 NULL 0");
  auto ret = Vector::BuildVector(funcEnt->retType, true);

  std::vector<Datum> params{CreateDatum(ret.get()), CreateDatum(src.get())};
  fu.test(params.data(), params.size(), CreateDatum(cbrtRes.get()));
}

INSTANTIATE_TEST_CASE_P(TestFunction, TestCbrtFunction,
                        ::testing::Values(FuncKind::DOUBLE_CBRT));

INSTANTIATE_TEST_CASE_P(
    double_sqrt, TestFunction,
    ::testing::Values(TestFunctionEntry{FuncKind::DOUBLE_SQRT,
                                        "Vector: NULL 1 4 0",
                                        {"Vector: NULL 1 16 0"}},
                      TestFunctionEntry{
                          FuncKind::DOUBLE_SQRT,
                          "Error",
                          {"Vector: -3.43"},
                          ERRCODE_INVALID_ARGUMENT_FOR_POWER_FUNCTION}));

class TestBinaryNotFunction : public ::testing::TestWithParam<FuncKind> {};
TEST_P(TestBinaryNotFunction, Test) {
  FuncKind funcId = GetParam();
  auto funcEnt = Func::instance()->getFuncEntryById(funcId);

  LOG_TESTING("%s", funcEnt->funcName.c_str());

  FunctionUtility fu(funcId);
  VectorUtility vuType(funcEnt->retType);

  auto src = vuType.generateVector("1 20 NULL 0");
  auto binaryNotRes = vuType.generateVector("-2 -21 NULL -1");
  auto ret = Vector::BuildVector(funcEnt->retType, true);

  std::vector<Datum> params{CreateDatum(ret.get()), CreateDatum(src.get())};
  fu.test(params.data(), params.size(), CreateDatum(binaryNotRes.get()));
}

INSTANTIATE_TEST_CASE_P(TestFunction, TestBinaryNotFunction,
                        ::testing::Values(FuncKind::INT16_BINARY_NOT,
                                          FuncKind::INT32_BINARY_NOT,
                                          FuncKind::INT64_BINARY_NOT));

class TestBinaryShiftLeftFunction : public ::testing::TestWithParam<FuncKind> {
};
TEST_P(TestBinaryShiftLeftFunction, Test) {
  FuncKind funcId = GetParam();
  auto funcEnt = Func::instance()->getFuncEntryById(funcId);

  LOG_TESTING("%s", funcEnt->funcName.c_str());

  std::vector<TypeKind> argTypes = funcEnt->argTypes;
  FunctionUtility fu(funcId);
  {
    LOG_TESTING("{Scalar, Scalar} argument");
    ScalarUtility retVuType(funcEnt->retType);
    ScalarUtility srcVuType(argTypes[0]);
    ScalarUtility argVuType(argTypes[1]);
    auto src = srcVuType.generateScalar("1");
    auto arg = argVuType.generateScalar("4");
    auto shiftLeftRes = retVuType.generateScalar("16");
    auto ret = srcVuType.generateScalar("3");

    std::vector<Datum> params{CreateDatum(&ret), CreateDatum(&src),
                              CreateDatum(&arg)};
    fu.test(params.data(), params.size(), CreateDatum(&shiftLeftRes));
  }

  {
    LOG_TESTING("{Vector, Scalar} argument");
    VectorUtility retVu(funcEnt->retType);
    VectorUtility srcVu(argTypes[0]);
    ScalarUtility argVu(argTypes[1]);

    auto src = srcVu.generateVector("2 4 NULL 0");
    auto arg = argVu.generateScalar("4");
    auto expect = retVu.generateVector("32 64 NULL 0");
    auto ret = Vector::BuildVector(funcEnt->retType, true);

    std::vector<Datum> params{CreateDatum(ret.get()), CreateDatum(src.get()),
                              CreateDatum(&arg)};

    fu.test(params.data(), params.size(), CreateDatum(expect.get()));
  }

  {
    LOG_TESTING("{Scalar, Vector} argument");
    VectorUtility retVu(funcEnt->retType);
    ScalarUtility srcVu(argTypes[0]);
    VectorUtility argVu(argTypes[1]);

    auto src = srcVu.generateScalar("4");
    auto arg = argVu.generateVector("2 4 NULL 0");
    auto expect = retVu.generateVector("16 64 NULL 4");
    auto ret = Vector::BuildVector(funcEnt->retType, true);

    std::vector<Datum> params{CreateDatum(ret.get()), CreateDatum(&src),
                              CreateDatum(arg.get())};

    fu.test(params.data(), params.size(), CreateDatum(expect.get()));
  }

  {
    LOG_TESTING("{Vector, Vector} argument");
    VectorUtility retVu(funcEnt->retType);
    VectorUtility srcVu(argTypes[0]);
    VectorUtility argVu(argTypes[1]);

    auto src = srcVu.generateVector("32 128 64 NULL");
    auto arg = argVu.generateVector("4 2 NULL 1");
    auto expect = retVu.generateVector("512 512 NULL NULL");
    auto ret = Vector::BuildVector(funcEnt->retType, true);

    std::vector<Datum> params{CreateDatum(ret.get()), CreateDatum(src.get()),
                              CreateDatum(arg.get())};
    fu.test(params.data(), params.size(), CreateDatum(expect.get()));
  }
}

INSTANTIATE_TEST_CASE_P(TestFunction, TestBinaryShiftLeftFunction,
                        ::testing::Values(FuncKind::INT16_BINARY_SHIFT_LEFT,
                                          FuncKind::INT32_BINARY_SHIFT_LEFT,
                                          FuncKind::INT64_BINARY_SHIFT_LEFT));

class TestBinaryShiftRightFunction : public ::testing::TestWithParam<FuncKind> {
};
TEST_P(TestBinaryShiftRightFunction, Test) {
  FuncKind funcId = GetParam();
  auto funcEnt = Func::instance()->getFuncEntryById(funcId);

  LOG_TESTING("%s", funcEnt->funcName.c_str());

  std::vector<TypeKind> argTypes = funcEnt->argTypes;
  FunctionUtility fu(funcId);
  {
    LOG_TESTING("{Scalar, Scalar} argument");
    ScalarUtility retVuType(funcEnt->retType);
    ScalarUtility srcVuType(argTypes[0]);
    ScalarUtility argVuType(argTypes[1]);
    auto src = srcVuType.generateScalar("16");
    auto arg = argVuType.generateScalar("4");
    auto shiftLeftRes = retVuType.generateScalar("1");
    auto ret = srcVuType.generateScalar("3");

    std::vector<Datum> params{CreateDatum(&ret), CreateDatum(&src),
                              CreateDatum(&arg)};
    fu.test(params.data(), params.size(), CreateDatum(&shiftLeftRes));
  }

  {
    LOG_TESTING("{Vector, Scalar} argument");
    VectorUtility retVu(funcEnt->retType);
    VectorUtility srcVu(argTypes[0]);
    ScalarUtility argVu(argTypes[1]);

    auto src = srcVu.generateVector("32 64 NULL 0");
    auto arg = argVu.generateScalar("4");
    auto expect = retVu.generateVector("2 4 NULL 0");
    auto ret = Vector::BuildVector(funcEnt->retType, true);

    std::vector<Datum> params{CreateDatum(ret.get()), CreateDatum(src.get()),
                              CreateDatum(&arg)};

    fu.test(params.data(), params.size(), CreateDatum(expect.get()));
  }

  {
    LOG_TESTING("{Scalar, Vector} argument");
    VectorUtility retVu(funcEnt->retType);
    ScalarUtility srcVu(argTypes[0]);
    VectorUtility argVu(argTypes[1]);

    auto src = srcVu.generateScalar("64");
    auto arg = argVu.generateVector("2 4 NULL 0");
    auto expect = retVu.generateVector("16 4 NULL 64");
    auto ret = Vector::BuildVector(funcEnt->retType, true);

    std::vector<Datum> params{CreateDatum(ret.get()), CreateDatum(&src),
                              CreateDatum(arg.get())};

    fu.test(params.data(), params.size(), CreateDatum(expect.get()));
  }

  {
    LOG_TESTING("{Vector, Vector} argument");
    VectorUtility retVu(funcEnt->retType);
    VectorUtility srcVu(argTypes[0]);
    VectorUtility argVu(argTypes[1]);

    auto src = srcVu.generateVector("512 512 NULL NULL");
    auto arg = argVu.generateVector("4 2 NULL 1");
    auto expect = retVu.generateVector("32 128 NULL NULL");
    auto ret = Vector::BuildVector(funcEnt->retType, true);

    std::vector<Datum> params{CreateDatum(ret.get()), CreateDatum(src.get()),
                              CreateDatum(arg.get())};
    fu.test(params.data(), params.size(), CreateDatum(expect.get()));
  }
}

INSTANTIATE_TEST_CASE_P(TestFunction, TestBinaryShiftRightFunction,
                        ::testing::Values(FuncKind::INT16_BINARY_SHIFT_RIGHT,
                                          FuncKind::INT32_BINARY_SHIFT_RIGHT,
                                          FuncKind::INT64_BINARY_SHIFT_RIGHT));

// test binary_or function
class TestBinaryOrFunction : public ::testing::TestWithParam<FuncKind> {};
TEST_P(TestBinaryOrFunction, Test) {
  FuncKind funcId = GetParam();
  auto funcEnt = Func::instance()->getFuncEntryById(funcId);

  LOG_TESTING("%s", funcEnt->funcName.c_str());

  std::vector<TypeKind> argTypes = funcEnt->argTypes;
  FunctionUtility fu(funcId);
  {
    LOG_TESTING("{Scalar, Scalar} argument");
    ScalarUtility retVuType(funcEnt->retType);
    ScalarUtility srcVuType(argTypes[0]);
    ScalarUtility argVuType(argTypes[1]);
    auto src = srcVuType.generateScalar("6");
    auto arg = argVuType.generateScalar("12");
    auto shiftLeftRes = retVuType.generateScalar("14");
    auto ret = srcVuType.generateScalar("3");

    std::vector<Datum> params{CreateDatum(&ret), CreateDatum(&src),
                              CreateDatum(&arg)};
    fu.test(params.data(), params.size(), CreateDatum(&shiftLeftRes));
  }

  {
    LOG_TESTING("{Vector, Scalar} argument");
    VectorUtility retVu(funcEnt->retType);
    VectorUtility srcVu(argTypes[0]);
    ScalarUtility argVu(argTypes[1]);

    auto src = srcVu.generateVector("6 64 NULL 0");
    auto arg = argVu.generateScalar("12");
    auto expect = retVu.generateVector("14 76 NULL 12");
    auto ret = Vector::BuildVector(funcEnt->retType, true);

    std::vector<Datum> params{CreateDatum(ret.get()), CreateDatum(src.get()),
                              CreateDatum(&arg)};

    fu.test(params.data(), params.size(), CreateDatum(expect.get()));
  }

  {
    LOG_TESTING("{Scalar, Vector} argument");
    VectorUtility retVu(funcEnt->retType);
    ScalarUtility srcVu(argTypes[0]);
    VectorUtility argVu(argTypes[1]);

    auto src = srcVu.generateScalar("64");
    auto arg = argVu.generateVector("2 4 NULL 0");
    auto expect = retVu.generateVector("66 68 NULL 64");
    auto ret = Vector::BuildVector(funcEnt->retType, true);

    std::vector<Datum> params{CreateDatum(ret.get()), CreateDatum(&src),
                              CreateDatum(arg.get())};

    fu.test(params.data(), params.size(), CreateDatum(expect.get()));
  }

  {
    LOG_TESTING("{Vector, Vector} argument");
    VectorUtility retVu(funcEnt->retType);
    VectorUtility srcVu(argTypes[0]);
    VectorUtility argVu(argTypes[1]);

    auto src = srcVu.generateVector("512 512 NULL NULL");
    auto arg = argVu.generateVector("4 2 NULL 1");
    auto expect = retVu.generateVector("516 514 NULL NULL");
    auto ret = Vector::BuildVector(funcEnt->retType, true);

    std::vector<Datum> params{CreateDatum(ret.get()), CreateDatum(src.get()),
                              CreateDatum(arg.get())};
    fu.test(params.data(), params.size(), CreateDatum(expect.get()));
  }
}

INSTANTIATE_TEST_CASE_P(TestFunction, TestBinaryOrFunction,
                        ::testing::Values(FuncKind::INT16_BINARY_OR,
                                          FuncKind::INT32_BINARY_OR,
                                          FuncKind::INT64_BINARY_OR));

// test binary_and function
class TestBinaryAndFunction : public ::testing::TestWithParam<FuncKind> {};
TEST_P(TestBinaryAndFunction, Test) {
  FuncKind funcId = GetParam();
  auto funcEnt = Func::instance()->getFuncEntryById(funcId);

  LOG_TESTING("%s", funcEnt->funcName.c_str());

  std::vector<TypeKind> argTypes = funcEnt->argTypes;
  FunctionUtility fu(funcId);
  {
    LOG_TESTING("{Scalar, Scalar} argument");
    ScalarUtility retVuType(funcEnt->retType);
    ScalarUtility srcVuType(argTypes[0]);
    ScalarUtility argVuType(argTypes[1]);
    auto src = srcVuType.generateScalar("16");
    auto arg = argVuType.generateScalar("4");
    auto shiftLeftRes = retVuType.generateScalar("0");
    auto ret = srcVuType.generateScalar("3");

    std::vector<Datum> params{CreateDatum(&ret), CreateDatum(&src),
                              CreateDatum(&arg)};
    fu.test(params.data(), params.size(), CreateDatum(&shiftLeftRes));
  }

  {
    LOG_TESTING("{Vector, Scalar} argument");
    VectorUtility retVu(funcEnt->retType);
    VectorUtility srcVu(argTypes[0]);
    ScalarUtility argVu(argTypes[1]);

    auto src = srcVu.generateVector("32 64 NULL 0");
    auto arg = argVu.generateScalar("4");
    auto expect = retVu.generateVector("0 0 NULL 0");
    auto ret = Vector::BuildVector(funcEnt->retType, true);

    std::vector<Datum> params{CreateDatum(ret.get()), CreateDatum(src.get()),
                              CreateDatum(&arg)};

    fu.test(params.data(), params.size(), CreateDatum(expect.get()));
  }

  {
    LOG_TESTING("{Scalar, Vector} argument");
    VectorUtility retVu(funcEnt->retType);
    ScalarUtility srcVu(argTypes[0]);
    VectorUtility argVu(argTypes[1]);

    auto src = srcVu.generateScalar("64");
    auto arg = argVu.generateVector("2 4 NULL 0");
    auto expect = retVu.generateVector("0 0 NULL 0");
    auto ret = Vector::BuildVector(funcEnt->retType, true);

    std::vector<Datum> params{CreateDatum(ret.get()), CreateDatum(&src),
                              CreateDatum(arg.get())};

    fu.test(params.data(), params.size(), CreateDatum(expect.get()));
  }

  {
    LOG_TESTING("{Vector, Vector} argument");
    VectorUtility retVu(funcEnt->retType);
    VectorUtility srcVu(argTypes[0]);
    VectorUtility argVu(argTypes[1]);

    auto src = srcVu.generateVector("512 512 NULL NULL");
    auto arg = argVu.generateVector("4 2 NULL 1");
    auto expect = retVu.generateVector("0 0 NULL NULL");
    auto ret = Vector::BuildVector(funcEnt->retType, true);

    std::vector<Datum> params{CreateDatum(ret.get()), CreateDatum(src.get()),
                              CreateDatum(arg.get())};
    fu.test(params.data(), params.size(), CreateDatum(expect.get()));
  }
}

INSTANTIATE_TEST_CASE_P(TestFunction, TestBinaryAndFunction,
                        ::testing::Values(FuncKind::INT16_BINARY_AND,
                                          FuncKind::INT32_BINARY_AND,
                                          FuncKind::INT64_BINARY_AND));

// test binary_xor function
class TestBinaryXorFunction : public ::testing::TestWithParam<FuncKind> {};
TEST_P(TestBinaryXorFunction, Test) {
  FuncKind funcId = GetParam();
  auto funcEnt = Func::instance()->getFuncEntryById(funcId);

  LOG_TESTING("%s", funcEnt->funcName.c_str());

  std::vector<TypeKind> argTypes = funcEnt->argTypes;
  FunctionUtility fu(funcId);
  {
    LOG_TESTING("{Scalar, Scalar} argument");
    ScalarUtility retVuType(funcEnt->retType);
    ScalarUtility srcVuType(argTypes[0]);
    ScalarUtility argVuType(argTypes[1]);
    auto src = srcVuType.generateScalar("16");
    auto arg = argVuType.generateScalar("4");
    auto shiftLeftRes = retVuType.generateScalar("20");
    auto ret = srcVuType.generateScalar("3");

    std::vector<Datum> params{CreateDatum(&ret), CreateDatum(&src),
                              CreateDatum(&arg)};
    fu.test(params.data(), params.size(), CreateDatum(&shiftLeftRes));
  }

  {
    LOG_TESTING("{Vector, Scalar} argument");
    VectorUtility retVu(funcEnt->retType);
    VectorUtility srcVu(argTypes[0]);
    ScalarUtility argVu(argTypes[1]);

    auto src = srcVu.generateVector("32 64 NULL 0");
    auto arg = argVu.generateScalar("4");
    auto expect = retVu.generateVector("36 68 NULL 4");
    auto ret = Vector::BuildVector(funcEnt->retType, true);

    std::vector<Datum> params{CreateDatum(ret.get()), CreateDatum(src.get()),
                              CreateDatum(&arg)};

    fu.test(params.data(), params.size(), CreateDatum(expect.get()));
  }

  {
    LOG_TESTING("{Scalar, Vector} argument");
    VectorUtility retVu(funcEnt->retType);
    ScalarUtility srcVu(argTypes[0]);
    VectorUtility argVu(argTypes[1]);

    auto src = srcVu.generateScalar("64");
    auto arg = argVu.generateVector("2 4 NULL 0");
    auto expect = retVu.generateVector("66 68 NULL 64");
    auto ret = Vector::BuildVector(funcEnt->retType, true);

    std::vector<Datum> params{CreateDatum(ret.get()), CreateDatum(&src),
                              CreateDatum(arg.get())};

    fu.test(params.data(), params.size(), CreateDatum(expect.get()));
  }

  {
    LOG_TESTING("{Vector, Vector} argument");
    VectorUtility retVu(funcEnt->retType);
    VectorUtility srcVu(argTypes[0]);
    VectorUtility argVu(argTypes[1]);

    auto src = srcVu.generateVector("512 512 NULL NULL");
    auto arg = argVu.generateVector("4 2 NULL 1");
    auto expect = retVu.generateVector("516 514 NULL NULL");
    auto ret = Vector::BuildVector(funcEnt->retType, true);

    std::vector<Datum> params{CreateDatum(ret.get()), CreateDatum(src.get()),
                              CreateDatum(arg.get())};
    fu.test(params.data(), params.size(), CreateDatum(expect.get()));
  }
}

INSTANTIATE_TEST_CASE_P(TestFunction, TestBinaryXorFunction,
                        ::testing::Values(FuncKind::INT16_BINARY_XOR,
                                          FuncKind::INT32_BINARY_XOR,
                                          FuncKind::INT64_BINARY_XOR));

// test mod function
class TestModFunction : public ::testing::TestWithParam<FuncKind> {};
TEST_P(TestModFunction, Test) {
  FuncKind funcId = GetParam();
  auto funcEnt = Func::instance()->getFuncEntryById(funcId);

  LOG_TESTING("%s", funcEnt->funcName.c_str());

  std::vector<TypeKind> argTypes = funcEnt->argTypes;
  FunctionUtility fu(funcId);
  {
    LOG_TESTING("{Scalar, Scalar} argument");
    ScalarUtility retVuType(funcEnt->retType);
    ScalarUtility srcVuType(argTypes[0]);
    ScalarUtility argVuType(argTypes[1]);
    auto src = srcVuType.generateScalar("15");
    auto arg = argVuType.generateScalar("4");
    auto shiftLeftRes = retVuType.generateScalar("3");
    auto ret = srcVuType.generateScalar("3");

    std::vector<Datum> params{CreateDatum(&ret), CreateDatum(&src),
                              CreateDatum(&arg)};
    fu.test(params.data(), params.size(), CreateDatum(&shiftLeftRes));
  }

  {
    LOG_TESTING("{Vector, Scalar} argument");
    VectorUtility retVu(funcEnt->retType);
    VectorUtility srcVu(argTypes[0]);
    ScalarUtility argVu(argTypes[1]);

    auto src = srcVu.generateVector("32 64 NULL 0");
    auto arg = argVu.generateScalar("3");
    auto expect = retVu.generateVector("2 1 NULL 0");
    auto ret = Vector::BuildVector(funcEnt->retType, true);

    std::vector<Datum> params{CreateDatum(ret.get()), CreateDatum(src.get()),
                              CreateDatum(&arg)};

    fu.test(params.data(), params.size(), CreateDatum(expect.get()));
  }

  {
    LOG_TESTING("{Scalar, Vector} argument");
    VectorUtility retVu(funcEnt->retType);
    ScalarUtility srcVu(argTypes[0]);
    VectorUtility argVu(argTypes[1]);

    auto src = srcVu.generateScalar("64");
    auto arg = argVu.generateVector("5 7 NULL 3");
    auto expect = retVu.generateVector("4 1 NULL 1");
    auto ret = Vector::BuildVector(funcEnt->retType, true);

    std::vector<Datum> params{CreateDatum(ret.get()), CreateDatum(&src),
                              CreateDatum(arg.get())};

    fu.test(params.data(), params.size(), CreateDatum(expect.get()));
  }

  {
    LOG_TESTING("{Vector, Vector} argument");
    VectorUtility retVu(funcEnt->retType);
    VectorUtility srcVu(argTypes[0]);
    VectorUtility argVu(argTypes[1]);

    auto src = srcVu.generateVector("33 77 NULL NULL");
    auto arg = argVu.generateVector("2 25 NULL 1");
    auto expect = retVu.generateVector("1 2 NULL NULL");
    auto ret = Vector::BuildVector(funcEnt->retType, true);

    std::vector<Datum> params{CreateDatum(ret.get()), CreateDatum(src.get()),
                              CreateDatum(arg.get())};
    fu.test(params.data(), params.size(), CreateDatum(expect.get()));
  }

  {
    LOG_TESTING("divide zero");
    VectorUtility retVu(funcEnt->retType);
    VectorUtility srcVu(argTypes[0]);
    VectorUtility argVu(argTypes[1]);

    auto src = srcVu.generateVector("33 77 22 6");
    auto arg = argVu.generateVector("0 0 0 0");
    auto expect = retVu.generateVector("NULL NULL NULL NULL");
    auto ret = Vector::BuildVector(funcEnt->retType, true);

    std::vector<Datum> params{CreateDatum(ret.get()), CreateDatum(src.get()),
                              CreateDatum(arg.get())};
    fu.test(params.data(), params.size(), CreateDatum(0),
            ERRCODE_DIVISION_BY_ZERO);
  }
}

INSTANTIATE_TEST_CASE_P(TestFunction, TestModFunction,
                        ::testing::Values(FuncKind::INT16_MOD,
                                          FuncKind::INT32_MOD,
                                          FuncKind::INT16_32_MOD,
                                          FuncKind::INT32_16_MOD,
                                          FuncKind::INT64_MOD));

// Test pow function
TEST(TestFunction, double_pow) {
  FuncKind funcId = FuncKind::DOUBLE_POW;
  auto funcEnt = Func::instance()->getFuncEntryById(funcId);

  LOG_TESTING("%s", funcEnt->funcName.c_str());

  std::vector<TypeKind> argTypes = funcEnt->argTypes;
  FunctionUtility fu(funcId);
  {
    LOG_TESTING("{Scalar, Scalar} argument");
    ScalarUtility retVuType(funcEnt->retType);
    ScalarUtility srcVuType(argTypes[0]);
    ScalarUtility argVuType(argTypes[1]);
    auto src = srcVuType.generateScalar("2");
    auto arg = argVuType.generateScalar("4");
    auto shiftLeftRes = retVuType.generateScalar("16");
    auto ret = srcVuType.generateScalar("3");

    std::vector<Datum> params{CreateDatum(&ret), CreateDatum(&src),
                              CreateDatum(&arg)};
    fu.test(params.data(), params.size(), CreateDatum(&shiftLeftRes));
  }

  {
    LOG_TESTING("{Vector, Scalar} argument");
    VectorUtility retVu(funcEnt->retType);
    VectorUtility srcVu(argTypes[0]);
    ScalarUtility argVu(argTypes[1]);

    auto src = srcVu.generateVector("1 3 NULL 0");
    auto arg = argVu.generateScalar("4");
    auto expect = retVu.generateVector("1 81 NULL 0");
    auto ret = Vector::BuildVector(funcEnt->retType, true);

    std::vector<Datum> params{CreateDatum(ret.get()), CreateDatum(src.get()),
                              CreateDatum(&arg)};

    fu.test(params.data(), params.size(), CreateDatum(expect.get()));
  }

  {
    LOG_TESTING("{Scalar, Vector} argument");
    VectorUtility retVu(funcEnt->retType);
    ScalarUtility srcVu(argTypes[0]);
    VectorUtility argVu(argTypes[1]);

    auto src = srcVu.generateScalar("2");
    auto arg = argVu.generateVector("3 5 NULL 0");
    auto expect = retVu.generateVector("8 32 NULL 1");
    auto ret = Vector::BuildVector(funcEnt->retType, true);

    std::vector<Datum> params{CreateDatum(ret.get()), CreateDatum(&src),
                              CreateDatum(arg.get())};

    fu.test(params.data(), params.size(), CreateDatum(expect.get()));
  }

  {
    LOG_TESTING("{Vector, Vector} argument");
    VectorUtility retVu(funcEnt->retType);
    VectorUtility srcVu(argTypes[0]);
    VectorUtility argVu(argTypes[1]);

    auto src = srcVu.generateVector("3 5 NULL NULL");
    auto arg = argVu.generateVector("2 3 NULL 1");
    auto expect = retVu.generateVector("9 125 NULL NULL");
    auto ret = Vector::BuildVector(funcEnt->retType, true);

    std::vector<Datum> params{CreateDatum(ret.get()), CreateDatum(src.get()),
                              CreateDatum(arg.get())};
    fu.test(params.data(), params.size(), CreateDatum(expect.get()));
  }
}
// Test ceil function
TEST(TestFunction, double_ceil) {
  FuncKind funcId = FuncKind::DOUBLE_CEIL;
  auto funcEnt = Func::instance()->getFuncEntryById(funcId);

  LOG_TESTING("%s", funcEnt->funcName.c_str());
  FunctionUtility fu(funcId);
  VectorUtility retVu(funcEnt->retType);
  VectorUtility srcVu(funcEnt->retType);

  auto src = srcVu.generateVector("2.34 4.56 -7.5 NULL");
  auto expect = srcVu.generateVector("3 5 -7 NULL");
  auto ret = Vector::BuildVector(funcEnt->retType, true);

  std::vector<Datum> params{CreateDatum(ret.get()), CreateDatum(src.get())};
  fu.test(params.data(), params.size(), CreateDatum(expect.get()));
}

// Test floor function
TEST(TestFunction, double_floor) {
  FuncKind funcId = FuncKind::DOUBLE_FLOOR;
  auto funcEnt = Func::instance()->getFuncEntryById(funcId);

  LOG_TESTING("%s", funcEnt->funcName.c_str());
  FunctionUtility fu(funcId);
  VectorUtility retVu(funcEnt->retType);
  VectorUtility srcVu(funcEnt->retType);

  auto src = srcVu.generateVector("2.34 4.56 -7.5 NULL");
  auto expect = srcVu.generateVector("2 4 -8 NULL");
  auto ret = Vector::BuildVector(funcEnt->retType, true);

  std::vector<Datum> params{CreateDatum(ret.get()), CreateDatum(src.get())};
  fu.test(params.data(), params.size(), CreateDatum(expect.get()));
}

// Test round function
TEST(TestFunction, double_round) {
  FuncKind funcId = FuncKind::DOUBLE_ROUND;
  auto funcEnt = Func::instance()->getFuncEntryById(funcId);

  LOG_TESTING("%s", funcEnt->funcName.c_str());
  FunctionUtility fu(funcId);
  VectorUtility retVu(funcEnt->retType);
  VectorUtility srcVu(funcEnt->retType);

  auto src = srcVu.generateVector("2.34 4.56 -7.5 NULL");
  auto expect = srcVu.generateVector("2 5 -8 NULL");
  auto ret = Vector::BuildVector(funcEnt->retType, true);

  std::vector<Datum> params{CreateDatum(ret.get()), CreateDatum(src.get())};
  fu.test(params.data(), params.size(), CreateDatum(expect.get()));
}

// Test trunc function with one arg
TEST(TestFunction, double_trunc) {
  FuncKind funcId = FuncKind::DOUBLE_TRUNC;
  auto funcEnt = Func::instance()->getFuncEntryById(funcId);

  LOG_TESTING("%s", funcEnt->funcName.c_str());
  FunctionUtility fu(funcId);
  VectorUtility retVu(funcEnt->retType);
  VectorUtility srcVu(funcEnt->retType);

  auto src = srcVu.generateVector("2.34 4.56 -7.5 NULL");
  auto expect = srcVu.generateVector("2 4 -7 NULL");
  auto ret = Vector::BuildVector(funcEnt->retType, true);

  std::vector<Datum> params{CreateDatum(ret.get()), CreateDatum(src.get())};
  fu.test(params.data(), params.size(), CreateDatum(expect.get()));
}

TEST(TestFunction, double_sign) {
  FuncKind funcId = FuncKind::DOUBLE_SIGN;
  auto funcEnt = Func::instance()->getFuncEntryById(funcId);

  LOG_TESTING("%s", funcEnt->funcName.c_str());
  FunctionUtility fu(funcId);
  VectorUtility retVu(funcEnt->retType);
  VectorUtility srcVu(funcEnt->retType);

  auto src = srcVu.generateVector("2.34 0 -7.5 NULL");
  auto expect = srcVu.generateVector("1 0 -1 NULL");
  auto ret = Vector::BuildVector(funcEnt->retType, true);

  std::vector<Datum> params{CreateDatum(ret.get()), CreateDatum(src.get())};
  fu.test(params.data(), params.size(), CreateDatum(expect.get()));
}
// Test double log10
TEST(TestFunction, double_exp) {
  FunctionUtility fu(FuncKind::DOUBLE_EXP);
  {
    auto ret = fu.generatePara<0, Vector>("NULL");
    auto src = fu.generatePara<1, Vector>("2.30 -1.40 0 NULL");
    auto exp =
        fu.generatePara<0, Vector>("9.97418245481472 0.246596963941606 1 NULL");

    std::vector<Datum> params{ret, src};
    fu.test(params.data(), params.size(), exp);
  }

  {
    auto ret = fu.generatePara<0, Vector>("NULL");
    auto src = fu.generatePara<1, Vector>("-708.5");
    auto exp = CreateDatum(0);

    std::vector<Datum> params{ret, src};
    fu.test(params.data(), params.size(), exp, ERRCODE_INTERVAL_FIELD_OVERFLOW);
  }
}

TEST(TestFunction, double_lg) {
  FunctionUtility fu(FuncKind::DOUBLE_LG);
  {
    LOG_TESTING("normal argument")
    auto ret = fu.generatePara<0, Vector>("NULL");
    auto src = fu.generatePara<1, Vector>("NULL 4 1000 200.32");
    auto exp =
        fu.generatePara<0, Vector>("NULL 0.602059991327962 3 2.30172431153034");

    std::vector<Datum> params{ret, src};
    fu.test(params.data(), params.size(), exp);
  }

  {
    LOG_TESTING("abnormal argument less than zero")
    auto ret = fu.generatePara<0, Vector>("NULL");
    auto src = fu.generatePara<1, Vector>("-1.40");
    auto exp = CreateDatum(0);

    std::vector<Datum> params{ret, src};
    fu.test(params.data(), params.size(), exp,
            ERRCODE_INVALID_ARGUMENT_FOR_LOG);
  }

  {
    LOG_TESTING("abnormal argument zero")
    auto ret = fu.generatePara<0, Vector>("NULL");
    auto src = fu.generatePara<1, Vector>("0");
    auto exp = CreateDatum(0);

    std::vector<Datum> params{ret, src};
    fu.test(params.data(), params.size(), exp,
            ERRCODE_INVALID_ARGUMENT_FOR_LOG);
  }
}

INSTANTIATE_TEST_CASE_P(double_log, TestFunction,
                        ::testing::Values(TestFunctionEntry{
                            FuncKind::DOUBLE_LOG,
                            "Vector: NULL 3 5 0 3",
                            {"Vector: NULL 2 3 4 5",
                             "Vector: 2 8 243 1 125"}}));

// Test double ln
TEST(TestFunction, double_ln) {
  FunctionUtility fu(FuncKind::DOUBLE_LN);
  {
    LOG_TESTING("normal argument")
    auto ret = fu.generatePara<0, Vector>("NULL");
    auto src = fu.generatePara<1, Vector>("NULL 4 1000 200.32");
    auto exp = fu.generatePara<0, Vector>(
        "NULL 1.38629436111989 6.90775527898214 5.29991608791173");

    std::vector<Datum> params{ret, src};
    fu.test(params.data(), params.size(), exp);
  }

  {
    LOG_TESTING("abnormal argument less than zero")
    auto ret = fu.generatePara<0, Vector>("NULL");
    auto src = fu.generatePara<1, Vector>("-1.40");
    auto exp = CreateDatum(0);

    std::vector<Datum> params{ret, src};
    fu.test(params.data(), params.size(), exp,
            ERRCODE_INVALID_ARGUMENT_FOR_LOG);
  }

  {
    LOG_TESTING("abnormal argument zero")
    auto ret = fu.generatePara<0, Vector>("NULL");
    auto src = fu.generatePara<1, Vector>("0");
    auto exp = CreateDatum(0);

    std::vector<Datum> params{ret, src};
    fu.test(params.data(), params.size(), exp,
            ERRCODE_INVALID_ARGUMENT_FOR_LOG);
  }
}

INSTANTIATE_TEST_CASE_P(double_acos, TestFunction,
                        ::testing::Values(TestFunctionEntry{
                            FuncKind::DOUBLE_ACOS,
                            "Vector: NULL 1.33871864393218 0 2.82403222429827",
                            {"Vector: NULL 0.23 1 -0.95"}}));

INSTANTIATE_TEST_CASE_P(
    double_asin, TestFunction,
    ::testing::Values(TestFunctionEntry{
        FuncKind::DOUBLE_ASIN,
        "Vector: NULL 0.232077682862713 1.5707963267949 -1.25323589750338",
        {"Vector: NULL 0.23 1 -0.95"}}));

INSTANTIATE_TEST_CASE_P(
    double_atan, TestFunction,
    ::testing::Values(TestFunctionEntry{
        FuncKind::DOUBLE_ATAN,
        "Vector: NULL -1.56580535206586 0 0.349092133951802",
        {"Vector: NULL -200.36 0 0.364"}}));

INSTANTIATE_TEST_CASE_P(
    double_atan2, TestFunction,
    ::testing::Values(TestFunctionEntry{
        FuncKind::DOUBLE_ATAN2,
        "Vector: NULL -1.54200612063041 3.14159265358979 1.5707963267949",
        {"Vector: NULL -200.36 0 0.364", "Vector: 26.39 5.77 -2.81 0"}}));

INSTANTIATE_TEST_CASE_P(
    double_cos, TestFunction,
    ::testing::Values(TestFunctionEntry{
        FuncKind::DOUBLE_COS,
        "Vector: NULL 0.763597533371372 1 0.934480243337817",
        {"Vector: NULL -200.36 0 0.364"}}));

INSTANTIATE_TEST_CASE_P(
    double_cot, TestFunction,
    ::testing::Values(TestFunctionEntry{
        FuncKind::DOUBLE_COT,
        "Vector: NULL 1.18260244792494 0.642092615934331 2.62483396288064",
        {"Vector: NULL -200.36 1 0.364"}}));

INSTANTIATE_TEST_CASE_P(
    double_sin, TestFunction,
    ::testing::Values(TestFunctionEntry{
        FuncKind::DOUBLE_SIN,
        "Vector: NULL 0.645692501914926 0.841470984807897 0.356014992396802",
        {"Vector: NULL -200.36 1 0.364"
         "0.356014992396802"}}));

INSTANTIATE_TEST_CASE_P(
    double_tan, TestFunction,
    ::testing::Values(TestFunctionEntry{
        FuncKind::DOUBLE_TAN,
        "Vector: NULL 0.845592702564292 0 0.380976478566494",
        {"Vector: NULL -200.36 0 0.364"}}));

INSTANTIATE_TEST_CASE_P(
    decimal_sqrt, TestFunction,
    ::testing::Values(
        TestFunctionEntry{FuncKind::DECIMAL_SQRT,
                          "Vector: NULL 1.859032006179560 10.022774067093401 "
                          "59.991666087882573",
                          {"Vector: NULL 3.4560 100.4560 3599.0000"}},
        TestFunctionEntry{FuncKind::DECIMAL_SQRT,
                          "Error",
                          {"Vector: -3.43"},
                          ERRCODE_INVALID_ARGUMENT_FOR_POWER_FUNCTION}));

INSTANTIATE_TEST_CASE_P(
    decimal_exp, TestFunction,
    ::testing::Values(
        TestFunctionEntry{FuncKind::DECIMAL_EXP,
                          "Vector: 0.00001670170079024566 "
                          "148.41315910257660 1 2.3538526683702e+17 NULL",
                          {"Vector: -11.0000 5.0000 0 40.0000 NULL"}},
        TestFunctionEntry{FuncKind::DECIMAL_EXP,
                          "Error",
                          {"Vector: 200"},
                          ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE}));

INSTANTIATE_TEST_CASE_P(
    decimal_ln, TestFunction,
    ::testing::Values(
        TestFunctionEntry{FuncKind::DECIMAL_LN,
                          "Vector: NULL 4.6076670661866786 "
                          "-1.1270117631898077 2.7650604558189897",
                          {"Vector: NULL 100.2500 0.3240 15.8800"}},
        TestFunctionEntry{FuncKind::DECIMAL_LN,
                          "Error",
                          {"Vector: -3.43"},
                          ERRCODE_INVALID_ARGUMENT_FOR_LOG},
        TestFunctionEntry{FuncKind::DECIMAL_LN,
                          "Error",
                          {"Vector: 0"},
                          ERRCODE_INVALID_ARGUMENT_FOR_LOG}));

INSTANTIATE_TEST_CASE_P(
    decimal_log, TestFunction,
    ::testing::Values(
        TestFunctionEntry{
            FuncKind::DECIMAL_LOG,
            "Vector: NULL 6.6474584264549203 "
            "-0.46852108295774487625 1.7180286573696623",
            {"Vector: NULL 2 10 5", "Vector: NULL 100.25 0.34 15.88"}},
        TestFunctionEntry{FuncKind::DECIMAL_LOG,
                          "Vector: NULL 6 3 2",
                          {"Vector: NULL 2 4 8", "Scalar: 64"}},
        TestFunctionEntry{FuncKind::DECIMAL_LOG,
                          "Vector: NULL 2 3 6",
                          {"Scalar: 2", "Vector: NULL 4 8 64"}},
        TestFunctionEntry{FuncKind::DECIMAL_LOG,
                          "Error",
                          {"Vector: -3.43", "Vector: 3"},
                          ERRCODE_INVALID_ARGUMENT_FOR_LOG},
        TestFunctionEntry{FuncKind::DECIMAL_LOG,
                          "Error",
                          {"Vector: 0", "Vector: 3"},
                          ERRCODE_INVALID_ARGUMENT_FOR_LOG},
        TestFunctionEntry{FuncKind::DECIMAL_LOG,
                          "Error",
                          {"Vector: 1", "Vector: 3"},
                          ERRCODE_DIVISION_BY_ZERO}));

INSTANTIATE_TEST_CASE_P(
    decimal_fac, TestFunction,
    ::testing::Values(TestFunctionEntry{FuncKind::DECIMAL_FAC,
                                        "Vector: NULL 1 1 1 120",
                                        {"Vector: NULL -3 1 0 5"}},
                      TestFunctionEntry{FuncKind::DECIMAL_FAC,
                                        "Error",
                                        {"Vector: 37"},
                                        ERRCODE_INTERVAL_FIELD_OVERFLOW}));

INSTANTIATE_TEST_CASE_P(
    decimal_pow, TestFunction,
    ::testing::Values(
        TestFunctionEntry{FuncKind::DECIMAL_POW,
                          "Vector: NULL 9 1 0 125",
                          {"Vector: NULL -3 1 0 5", "Vector: 3 2 5 7 3"}},
        TestFunctionEntry{FuncKind::DECIMAL_POW,
                          "Vector: NULL 4 16 64",
                          {"Vector: NULL 2 4 8", "Scalar: 2"}},
        TestFunctionEntry{FuncKind::DECIMAL_POW,
                          "Vector: NULL 4 8 16",
                          {"Scalar: 2", "Vector: NULL 2 3 4"}},
        TestFunctionEntry{FuncKind::DECIMAL_POW,
                          "Error",
                          {"Vector: 0", "Vector: -2"},
                          ERRCODE_INVALID_ARGUMENT_FOR_POWER_FUNCTION},
        TestFunctionEntry{FuncKind::DECIMAL_POW,
                          "Error",
                          {"Vector: -2", "Vector: 2.3"},
                          ERRCODE_INVALID_ARGUMENT_FOR_POWER_FUNCTION},
        TestFunctionEntry{FuncKind::DECIMAL_POW,
                          "Error",
                          {"Vector: 2", "Vector: 200"},
                          ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE},
        TestFunctionEntry{FuncKind::DECIMAL_POW,
                          "Error",
                          {"Vector: 2", "Vector: 10000000"},
                          ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE}));

} /* namespace dbcommon*/
