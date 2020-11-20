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
    num_to_bytea, TestFunction,
    ::testing::Values(
        TestFunctionEntry{
            FuncKind::CHAR_TO_BYTEA, "Vector: \\003 NULL", {"Vector: 3 NULL"}},
        TestFunctionEntry{FuncKind::SMALLINT_TO_BYTEA,
                          "Vector: \\000{ NULL",
                          {"Vector: 123 NULL"}},
        TestFunctionEntry{FuncKind::INT_TO_BYTEA,
                          "Vector: \\000\\000\\000{ NULL",
                          {"Vector: 123 NULL"}},
        TestFunctionEntry{FuncKind::BIGINT_TO_BYTEA,
                          "Vector: \\000\\000\\000\\000\\000\\000\\000{ NULL",
                          {"Vector: 123 NULL"}},
        TestFunctionEntry{FuncKind::FLOAT_TO_BYTEA,
                          "Vector: B\\366>\\372 NULL",
                          {"Vector: 123.123 NULL"}},
        TestFunctionEntry{FuncKind::DOUBLE_TO_BYTEA,
                          "Vector: @^\\307\\337;dZ\\035 NULL",
                          {"Vector: 123.123 NULL"}},
        TestFunctionEntry{FuncKind::TEXT_TO_BYTEA,
                          "Vector: 123hub7.;8knjn NULL",
                          {"Vector: 123hub7.;8knjn NULL"}},
        TestFunctionEntry{FuncKind::DATE_TO_BYTEA,
                          "Vector: \\000\\000\\032\\034 NULL",
                          {"Vector: 2018-04-20 NULL"}},
        TestFunctionEntry{FuncKind::TIME_TO_BYTEA,
                          "Vector: \\000\\000\\000\\011u\\013\\345P NULL",
                          {"Vector: 11:16:58.419536 NULL"}},
        TestFunctionEntry{FuncKind::TIMESTAMP_TO_BYTEA,
                          "Vector: \\000\\002'\\203\\207Y\\245P NULL",
                          {"Vector: 2019-03-20 11:16:58.419536 NULL"}},
        TestFunctionEntry{FuncKind::TIMESTAMPTZ_TO_BYTEA,
                          "Vector: \\000\\002'|\\322\\274\\205P NULL",
                          {"Vector: 2019-03-20 11:16:58.419536+08 NULL"}},
        TestFunctionEntry{FuncKind::INTERVAL_TO_BYTEA,
                          "Vector: "
                          "\\000\\000\\000\\000\\000\\000\\000."
                          "\\000\\000\\000\\000\\000\\000\\000\\000 NULL",
                          {"Vector: 00:00:46 NULL"}},
        TestFunctionEntry{
            FuncKind::DECIMAL_TO_BYTEA,
            "Vector: "
            "\\000\\006\\000\\002@\\000\\000\\011\\000\\001\\011)"
            "\\032\\205\\004\\322\\026.#( "
            "\\000\\002\\000\\000\\000\\000\\000\\003\\000{\\004\\316 "
            "\\000\\002\\000\\000\\000\\000\\000\\002\\000\\014\\004\\260 NULL",
            {"Vector: -123456789.123456789 123.123 12.12 NULL"}}));

INSTANTIATE_TEST_CASE_P(
    text_to_Decimal, TestFunction,
    ::testing::Values(
        TestFunctionEntry{
            FuncKind::TEXT_TO_DECIMAL,
            "Vector: 922337203685.4775807 -9223372036854.775808 "
            "1234.56 -0.00123456 1234.56 -0.00123456 NULL",
            {"Vector: 922337203685.4775807 -9223372036854.775808 "
             "1.23456e+3 -1.23456e-3 1.23456E+3 -1.23456E-3 NULL"}},
        TestFunctionEntry{FuncKind::TEXT_TO_DECIMAL,
                          "Error",
                          {"Vector: -922337203i854775809 9223372036854l75808 "
                           "e33 1.23e2 1.2i3e2 1.23e2i2"},
                          ERRCODE_INVALID_TEXT_REPRESENTATION}));

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
INSTANTIATE_TEST_CASE_P(
    interval_to_text, TestFunction,
    ::testing::Values(TestFunctionEntry{
        FuncKind::INTERVAL_TO_TEXT,
        "Vector{delimiter=x}: 4 years 2 months 560 days 00:00:30x-4 year -2 "
        "month +560 days 00:00:00.003xNULL",
        {"Vector: 50:560:30000000 -50:560:3000 NULL"}}));

}  // namespace dbcommon
