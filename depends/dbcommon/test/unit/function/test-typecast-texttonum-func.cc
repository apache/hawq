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

INSTANTIATE_TEST_CASE_P(
    text_to_int2, TestFunction,
    ::testing::Values(TestFunctionEntry{FuncKind::TEXT_TO_SMALLINT,
                                        "Vector: -32768 32767 12 -1 NULL",
                                        {"Vector: -32768 32767 12 -1 NULL"}},
                      TestFunctionEntry{FuncKind::TEXT_TO_SMALLINT,
                                        "Error",
                                        {"Vector: -32769 32768"},
                                        ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE},
                      TestFunctionEntry{FuncKind::TEXT_TO_SMALLINT,
                                        "Error",
                                        {"Vector: -32i69 32i68"},
                                        ERRCODE_INVALID_TEXT_REPRESENTATION}));

INSTANTIATE_TEST_CASE_P(
    text_to_int4, TestFunction,
    ::testing::Values(
        TestFunctionEntry{FuncKind::TEXT_TO_INT,
                          "Vector: 2147483647 -2147483648 3456 -3456 NULL",
                          {"Vector: 2147483647 -2147483648 3456 -3456 NULL"}},
        TestFunctionEntry{FuncKind::TEXT_TO_INT,
                          "Error",
                          {"Vector: -2147483649 2147483648"},
                          ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE},
        TestFunctionEntry{FuncKind::TEXT_TO_INT,
                          "Error",
                          {"Vector: -21474i3648 21474i3647"},
                          ERRCODE_INVALID_TEXT_REPRESENTATION}));

INSTANTIATE_TEST_CASE_P(
    text_to_int8, TestFunction,
    ::testing::Values(
        TestFunctionEntry{FuncKind::TEXT_TO_BIGINT,
                          "Vector: 9223372036854775807 -9223372036854775808 "
                          "123456 -123456 NULL",
                          {"Vector: 9223372036854775807 -9223372036854775808 "
                           "123456 -123456 NULL"}},
        TestFunctionEntry{FuncKind::TEXT_TO_BIGINT,
                          "Error",
                          {"Vector: -9223372036854775809 9223372036854775808"},
                          ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE},
        TestFunctionEntry{FuncKind::TEXT_TO_BIGINT,
                          "Error",
                          {"Vector: -9223372036i54775808 92233720a6854775807"},
                          ERRCODE_INVALID_TEXT_REPRESENTATION}));

INSTANTIATE_TEST_CASE_P(
    text_to_float4, TestFunction,
    ::testing::Values(
        TestFunctionEntry{
            FuncKind::TEXT_TO_FLOAT,
            "Vector: 9223372036854.775807 -9223372036854.775808 "
            "1234.56 -0.00123456 1234.56 -0.00123456 NULL",
            {"Vector: 9223372036854.775807 -9223372036854.775808 "
             "1.23456e+3 -1.23456e-3 1.23456E3 -1.23456E-3 NULL"}},
        TestFunctionEntry{FuncKind::TEXT_TO_FLOAT,
                          "Error",
                          {"Vector: -922337203i854775809 9223372036854l75808 "
                           "e33 1.23eq2 1.2i3e2 1.23e2i2"},
                          ERRCODE_INVALID_TEXT_REPRESENTATION}));

INSTANTIATE_TEST_CASE_P(
    text_to_float8, TestFunction,
    ::testing::Values(
        TestFunctionEntry{
            FuncKind::TEXT_TO_DOUBLE,
            "Vector: 922337203685.4775807 -9223372036854.775808 "
            "1234.56 -0.00123456 1234.56 -0.00123456 NULL",
            {"Vector: 922337203685.4775807 -9223372036854.775808 "
             "1.23456e+3 -1.23456e-3 1.23456E3 -1.23456E-3 NULL"}},
        TestFunctionEntry{FuncKind::TEXT_TO_DOUBLE,
                          "Error",
                          {"Vector: -922337203i854775809 9223372036854l75808 "
                           "e33 1.23eq2 1.2i3e2 1.23e2i2"},
                          ERRCODE_INVALID_TEXT_REPRESENTATION}));

INSTANTIATE_TEST_CASE_P(
    interval_to_text, TestFunction,
    ::testing::Values(TestFunctionEntry{
        FuncKind::INTERVAL_TO_TEXT,
        "Vector{delimiter=x}: 4 years 2 months 560 days 00:00:30x-4 year -2 "
        "month +560 days 00:00:00.003xNULL",
        {"Vector: 50:560:30000000 -50:560:3000 NULL"}}));

}  // namespace dbcommon
