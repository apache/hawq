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

#include <iostream>

#include "gtest/gtest.h"

#include "dbcommon/function/string-binary-function.h"
#include "dbcommon/log/debug-logger.h"
#include "dbcommon/testutil/function-utils.h"
#include "dbcommon/testutil/scalar-utils.h"
#include "dbcommon/testutil/vector-utils.h"

namespace dbcommon {

TEST(TestFunction, string_char_length) {
  FunctionUtility fu(FuncKind::STRING_CHAR_LENGTH);
  auto ret = fu.generatePara<0, Vector>("NULL");

  auto expect = fu.generatePara<0, Vector>("3 8 5 NULL 3 2 4");
  auto strs =
      fu.generatePara<1, Vector>("cat black的今天 sheep NULL fry is good");
  std::vector<Datum> params{ret, strs};
  fu.test(params.data(), params.size(), expect);

  {
    auto expect = fu.generatePara<0, Vector>("10 7 8 3");
    auto strs =
        fu.generatePara<1, Vector>("i am a stu, nic to,meet you,   ", ',');
    std::vector<Datum> params{ret, strs};
    fu.test(params.data(), params.size(), expect);
  }
}

TEST(TestFunction, string_octet_length) {
  FunctionUtility fu(FuncKind::STRING_OCTET_LENGTH);
  auto ret = fu.generatePara<0, Vector>("NULL");

  auto expect = fu.generatePara<0, Vector>("3 5 5 NULL 3 2 4");
  auto strs = fu.generatePara<1, Vector>("cat black sheep NULL fry is good");
  std::vector<Datum> params{ret, strs};
  fu.test(params.data(), params.size(), expect);
}

TEST(TestFunction, string_like) {
  FunctionUtility fu(FuncKind::STRING_LIKE);
  SelectList ret;

  auto strs = fu.generatePara<1, Vector>(
      "cat NULL sheep NULL fry is good peer flee 你ee");

  {
    LOG_TESTING("non-ANSI pattern");

    SelectList expect{7, 9};
    auto pattern = fu.generatePara<2, Scalar>("_ee%");
    std::vector<Datum> params{CreateDatum(&ret), strs, pattern};
    fu.test(params.data(), params.size(), CreateDatum(&expect));
  }

  {
    LOG_TESTING("ANSI pattern");
    SelectList expect{4, 7};
    auto pattern = fu.generatePara<2, Scalar>("%r%");
    std::vector<Datum> params{CreateDatum(&ret), strs, pattern};
    fu.test(params.data(), params.size(), CreateDatum(&expect));
  }
}

TEST(TestFunction, string_not_like) {
  FunctionUtility fu(FuncKind::STRING_NOT_LIKE);
  SelectList ret;

  auto strs = fu.generatePara<1, Vector>(
      "cat NULL sheep NULL fry is good peer flee 你ee try");

  {
    LOG_TESTING("non-ANSI pattern");

    auto pattern = fu.generatePara<2, Scalar>("_ry");
    SelectList expect{0, 2, 5, 6, 7, 8, 9};
    std::vector<Datum> params{CreateDatum(&ret), strs, pattern};
    fu.test(params.data(), params.size(), CreateDatum(&expect));
  }

  {
    LOG_TESTING("ANSI pattern");
    auto pattern = fu.generatePara<2, Scalar>("%ee%");
    SelectList expect{0, 4, 5, 6, 10};
    std::vector<Datum> params{CreateDatum(&ret), strs, pattern};
    fu.test(params.data(), params.size(), CreateDatum(&expect));
  }
}

TEST(TestFunction, string_substring_nolen) {
  FunctionUtility fu(FuncKind::STRING_SUBSTRING_NOLEN);
  auto ret = fu.generatePara<0, Vector>("NULL");

  {
    LOG_TESTING("Vector - Vector with ascii input");
    auto expect = fu.generatePara<0, Vector>("cat,ack,NULL,,b,66ccff", ',');
    auto strs = fu.generatePara<1, Vector>("cat,black,NULL,,b,66ccff", ',');
    auto pos = fu.generatePara<2, Vector>("0 3 1 2 1 -1");
    std::vector<Datum> params{ret, strs, pos};
    fu.test(params.data(), params.size(), expect);
  }

  {
    LOG_TESTING("Vector - Vector with utf8 input");
    auto expect = fu.generatePara<0, Vector>("一二三四,一二三四,,二三四", ',');
    auto strs =
        fu.generatePara<1, Vector>("一二三四,一二三四,一二三四,一二三四", ',');
    auto pos = fu.generatePara<2, Vector>("0 -1 5 2");
    std::vector<Datum> params{ret, strs, pos};
    fu.test(params.data(), params.size(), expect);
  }

  {
    LOG_TESTING("Vector - Scalar with ascii input");
    auto expect = fu.generatePara<0, Vector>("at,lack,NULL,,", ',');
    auto strs = fu.generatePara<1, Vector>("cat,black,NULL,,b", ',');
    auto pos = fu.generatePara<2, Scalar>("2");
    std::vector<Datum> params{ret, strs, pos};
    fu.test(params.data(), params.size(), expect);
  }

  {
    LOG_TESTING("Vector - Scalar with null input");
    auto expect = fu.generatePara<0, Vector>("NULL,NULL,NULL,NULL,NULL", ',');
    auto strs = fu.generatePara<1, Vector>("cat,black,NULL,,b", ',');
    auto pos = fu.generatePara<2, Scalar>("NULL");
    std::vector<Datum> params{ret, strs, pos};
    fu.test(params.data(), params.size(), expect);
  }

  {
    LOG_TESTING("Scalar - Vector with ascii input");
    auto expect = fu.generatePara<0, Vector>("abcdefg,cdefg,,abcdefg", ',');
    auto strs = fu.generatePara<1, Scalar>("abcdefg");
    auto pos = fu.generatePara<2, Vector>("0 3 10 -1");
    std::vector<Datum> params{ret, strs, pos};
    fu.test(params.data(), params.size(), expect);
  }

  {
    LOG_TESTING("Scalar - Vector with null input");
    auto expect = fu.generatePara<0, Vector>("NULL NULL NULL NULL");
    auto strs = fu.generatePara<1, Scalar>("NULL");
    auto pos = fu.generatePara<2, Vector>("0 3 10 -1");
    std::vector<Datum> params{ret, strs, pos};
    fu.test(params.data(), params.size(), expect);
  }
}

TEST(TestFunction, string_substring) {
  FunctionUtility fu(FuncKind::STRING_SUBSTRING);
  auto ret = fu.generatePara<0, Vector>("NULL");

  {
    LOG_TESTING("Vector - Vector - Vector with ascii input");
    auto expect = fu.generatePara<0, Vector>("at,,NULL,,ccff", ',');
    auto strs = fu.generatePara<1, Vector>("cat,black,NULL,,66ccff", ',');
    auto pos = fu.generatePara<2, Vector>("2 1 1 1 3");
    auto len = fu.generatePara<3, Vector>("4 0 1 2 4");
    std::vector<Datum> params{ret, strs, pos, len};
    fu.test(params.data(), params.size(), expect);
  }

  {
    LOG_TESTING("Vector - Vector - Vector with utf8 input");
    auto expect = fu.generatePara<0, Vector>("二三四五,,一,三四五", ',');
    auto strs = fu.generatePara<1, Vector>(
        "一二三四五,一二三四五,一二三四五,一二三四五", ',');
    auto pos = fu.generatePara<2, Vector>("2 1 1 3");
    auto len = fu.generatePara<3, Vector>("4 0 1 6");
    std::vector<Datum> params{ret, strs, pos, len};
    fu.test(params.data(), params.size(), expect);
  }

  {
    LOG_TESTING("Vector - Vector - Vector with error input");
    auto strs = fu.generatePara<1, Vector>("error");
    auto pos = fu.generatePara<2, Vector>("2");
    auto len = fu.generatePara<3, Vector>("-1");
    std::vector<Datum> params{ret, strs, pos, len};
    fu.test(params.data(), params.size(), CreateDatum(0),
            ERRCODE_SUBSTRING_ERROR);
  }

  {
    LOG_TESTING("Vector - Vector - Scalar with ascii input");
    auto expect = fu.generatePara<0, Vector>("at,blac,NULL,,ccff", ',');
    auto strs = fu.generatePara<1, Vector>("cat,black,NULL,,66ccff", ',');
    auto pos = fu.generatePara<2, Vector>("2 1 1 1 3");
    auto len = fu.generatePara<3, Scalar>("4");
    std::vector<Datum> params{ret, strs, pos, len};
    fu.test(params.data(), params.size(), expect);
  }

  {
    LOG_TESTING("Vector - Vector - Scalar with error input");
    auto strs = fu.generatePara<1, Vector>("error");
    auto pos = fu.generatePara<2, Vector>("2");
    auto len = fu.generatePara<3, Scalar>("-1");
    std::vector<Datum> params{ret, strs, pos, len};
    fu.test(params.data(), params.size(), CreateDatum(0),
            ERRCODE_SUBSTRING_ERROR);
  }

  {
    LOG_TESTING("Vector - Vector - Scalar with null input");
    auto expect = fu.generatePara<0, Vector>("NULL NULL NULL NULL NULL");
    auto strs = fu.generatePara<1, Vector>("cat,black,NULL,,66ccff", ',');
    auto pos = fu.generatePara<2, Vector>("2 1 1 1 3");
    auto len = fu.generatePara<3, Scalar>("NULL");
    std::vector<Datum> params{ret, strs, pos, len};
    fu.test(params.data(), params.size(), expect);
  }

  {
    LOG_TESTING("Vector - Scalar - Vector with ascii input");
    auto expect = fu.generatePara<0, Vector>("cat,bl,NULL,,66cc", ',');
    auto strs = fu.generatePara<1, Vector>("cat,black,NULL,,66ccff", ',');
    auto pos = fu.generatePara<2, Scalar>("1");
    auto len = fu.generatePara<3, Vector>("4 2 1 1 4");
    std::vector<Datum> params{ret, strs, pos, len};
    fu.test(params.data(), params.size(), expect);
  }

  {
    LOG_TESTING("Vector - Scalar - Vector with error input");
    auto strs = fu.generatePara<1, Vector>("error");
    auto pos = fu.generatePara<2, Scalar>("1");
    auto len = fu.generatePara<3, Vector>("-1");
    std::vector<Datum> params{ret, strs, pos, len};
    fu.test(params.data(), params.size(), CreateDatum(0),
            ERRCODE_SUBSTRING_ERROR);
  }

  {
    LOG_TESTING("Vector - Scalar - Vector with null input");
    auto expect = fu.generatePara<0, Vector>("NULL NULL NULL NULL NULL");
    auto strs = fu.generatePara<1, Vector>("cat,black,NULL,,66ccff", ',');
    auto pos = fu.generatePara<2, Scalar>("NULL");
    auto len = fu.generatePara<3, Vector>("4 2 1 1 4");
    std::vector<Datum> params{ret, strs, pos, len};
    fu.test(params.data(), params.size(), expect);
  }

  {
    LOG_TESTING("Vector - Scalar - Scalar with ascii input");
    auto expect = fu.generatePara<0, Vector>("at,lac,NULL,,,6cc", ',');
    auto strs = fu.generatePara<1, Vector>("cat,black,NULL,,b,66ccff", ',');
    auto pos = fu.generatePara<2, Scalar>("2");
    auto len = fu.generatePara<3, Scalar>("3");
    std::vector<Datum> params{ret, strs, pos, len};
    fu.test(params.data(), params.size(), expect);
  }

  {
    LOG_TESTING("Vector - Scalar - Scalar with error input");
    auto strs = fu.generatePara<1, Vector>("error");
    auto pos = fu.generatePara<2, Scalar>("2");
    auto len = fu.generatePara<3, Scalar>("-1");
    std::vector<Datum> params{ret, strs, pos, len};
    fu.test(params.data(), params.size(), CreateDatum(0),
            ERRCODE_SUBSTRING_ERROR);
  }

  {
    LOG_TESTING("Vector - Scalar - Scalar with pos null input");
    auto expect = fu.generatePara<0, Vector>("NULL NULL NULL NULL");
    auto strs = fu.generatePara<1, Vector>("cat,black,NULL,b", ',');
    auto pos = fu.generatePara<2, Scalar>("NULL");
    auto len = fu.generatePara<3, Scalar>("3");
    std::vector<Datum> params{ret, strs, pos, len};
    fu.test(params.data(), params.size(), expect);
  }

  {
    LOG_TESTING("Vector - Scalar - Scalar with len null input");
    auto expect = fu.generatePara<0, Vector>("NULL NULL NULL NULL");
    auto strs = fu.generatePara<1, Vector>("cat,black,NULL,b", ',');
    auto pos = fu.generatePara<2, Scalar>("2");
    auto len = fu.generatePara<3, Scalar>("NULL");
    std::vector<Datum> params{ret, strs, pos, len};
    fu.test(params.data(), params.size(), expect);
  }

  {
    LOG_TESTING("Scalar - Vector - Vector with ascii input");
    auto expect = fu.generatePara<0, Vector>("bcde,abc,,ab", ',');
    auto str = fu.generatePara<1, Scalar>("abcde");
    auto pos = fu.generatePara<2, Vector>("2 1 -2 -1");
    auto len = fu.generatePara<3, Vector>("8 3 1 4");
    std::vector<Datum> params{ret, str, pos, len};
    fu.test(params.data(), params.size(), expect);
  }

  {
    LOG_TESTING("Scalar - Vector - Vector with error input");
    auto str = fu.generatePara<1, Scalar>("abcde");
    auto pos = fu.generatePara<2, Vector>("2");
    auto len = fu.generatePara<3, Vector>("-1");
    std::vector<Datum> params{ret, str, pos, len};
    fu.test(params.data(), params.size(), CreateDatum(0),
            ERRCODE_SUBSTRING_ERROR);
  }

  {
    LOG_TESTING("Scalar - Vector - Vector with null input");
    auto expect = fu.generatePara<0, Vector>("NULL NULL NULL NULL");
    auto str = fu.generatePara<1, Scalar>("NULL");
    auto pos = fu.generatePara<2, Vector>("2 1 -2 -1");
    auto len = fu.generatePara<3, Vector>("8 3 1 4");
    std::vector<Datum> params{ret, str, pos, len};
    fu.test(params.data(), params.size(), expect);
  }

  {
    LOG_TESTING("Scalar - Scalar - Vector with ascii input");
    auto expect = fu.generatePara<0, Vector>("abcde,a,", ',');
    auto str = fu.generatePara<1, Scalar>("abcde");
    auto pos = fu.generatePara<2, Scalar>("-1");
    auto len = fu.generatePara<3, Vector>("8 3 1");
    std::vector<Datum> params{ret, str, pos, len};
    fu.test(params.data(), params.size(), expect);
  }

  {
    LOG_TESTING("Scalar - Scalar - Vector with error input");
    auto str = fu.generatePara<1, Scalar>("abcde");
    auto pos = fu.generatePara<2, Scalar>("-1");
    auto len = fu.generatePara<3, Vector>("-1");
    std::vector<Datum> params{ret, str, pos, len};
    fu.test(params.data(), params.size(), CreateDatum(0),
            ERRCODE_SUBSTRING_ERROR);
  }

  {
    LOG_TESTING("Scalar - Scalar - Vector with str null input");
    auto expect = fu.generatePara<0, Vector>("NULL NULL NULL");
    auto str = fu.generatePara<1, Scalar>("NULL");
    auto pos = fu.generatePara<2, Scalar>("-1");
    auto len = fu.generatePara<3, Vector>("8 3 1");
    std::vector<Datum> params{ret, str, pos, len};
    fu.test(params.data(), params.size(), expect);
  }

  {
    LOG_TESTING("Scalar - Scalar - Vector with pos null input");
    auto expect = fu.generatePara<0, Vector>("NULL NULL NULL");
    auto str = fu.generatePara<1, Scalar>("abcde");
    auto pos = fu.generatePara<2, Scalar>("NULL");
    auto len = fu.generatePara<3, Vector>("8 3 1");
    std::vector<Datum> params{ret, str, pos, len};
    fu.test(params.data(), params.size(), expect);
  }

  {
    LOG_TESTING("Scalar - Vector - Scalar with ascii input");
    auto expect = fu.generatePara<0, Vector>(",abc,e", ',');
    auto str = fu.generatePara<1, Scalar>("abcde");
    auto pos = fu.generatePara<2, Vector>("-2 1 5");
    auto len = fu.generatePara<3, Scalar>("3");
    std::vector<Datum> params{ret, str, pos, len};
    fu.test(params.data(), params.size(), expect);
  }

  {
    LOG_TESTING("Scalar - Vector - Scalar with error input");
    auto str = fu.generatePara<1, Scalar>("abcde");
    auto pos = fu.generatePara<2, Vector>("-2 1 5");
    auto len = fu.generatePara<3, Scalar>("-1");
    std::vector<Datum> params{ret, str, pos, len};
    fu.test(params.data(), params.size(), CreateDatum(0),
            ERRCODE_SUBSTRING_ERROR);
  }

  {
    LOG_TESTING("Scalar - Vector - Scalar with str null input");
    auto expect = fu.generatePara<0, Vector>("NULL NULL NULL");
    auto str = fu.generatePara<1, Scalar>("NULL");
    auto pos = fu.generatePara<2, Vector>("-2 1 5");
    auto len = fu.generatePara<3, Scalar>("3");
    std::vector<Datum> params{ret, str, pos, len};
    fu.test(params.data(), params.size(), expect);
  }

  {
    LOG_TESTING("Scalar - Vector - Scalar with len null input");
    auto ret = fu.generatePara<0, Vector>("NULL");
    auto expect = fu.generatePara<0, Vector>("NULL NULL NULL");
    auto str = fu.generatePara<1, Scalar>("abcde");
    auto pos = fu.generatePara<2, Vector>("-2 1 5");
    auto len = fu.generatePara<3, Scalar>("NULL");
    std::vector<Datum> params{ret, str, pos, len};
    fu.test(params.data(), params.size(), expect);
  }
}

TEST(TestFunction, string_lower) {
  FunctionUtility fu(FuncKind::STRING_LOWER);
  auto ret = fu.generatePara<0, Vector>("NULL");

  auto expect = fu.generatePara<0, Vector>("cat NULL sheep NULL 009bb is good");
  auto strs = fu.generatePara<1, Vector>("caT NULL shEEp NULL 009bb is GOOD");
  std::vector<Datum> params{ret, strs};
  fu.test(params.data(), params.size(), expect);
}

TEST(TestFunction, string_upper) {
  FunctionUtility fu(FuncKind::STRING_UPPER);
  auto ret = fu.generatePara<0, Vector>("NULL");

  auto expect =
      fu.generatePara<0, Vector>("CAT NULL SHEEP NULL 009BB IS GOOD 中国CHINA");
  auto strs =
      fu.generatePara<1, Vector>("caT NULL shEEp NULL 009bb is GOOD 中国China");
  std::vector<Datum> params{ret, strs};
  fu.test(params.data(), params.size(), expect);
}

TEST(TestFunction, string_concat) {
  FunctionUtility fu(FuncKind::STRING_CONCAT);
  VectorUtility vuString(TypeKind::STRINGID);
  ScalarUtility suString(TypeKind::STRINGID);
  auto ret = Vector::BuildVector(TypeKind::STRINGID, true);

  // Vector concat Vector in normal input
  {
    LOG_TESTING("Vector || Vector with noramal input");
    auto str1 = vuString.generateVector("abc,NULL,D e F,1258,G2333", ',');
    auto str2 = vuString.generateVector("NULL,xxx,112,G 2,x80", ',');
    auto concat =
        vuString.generateVector("NULL,NULL,D e F112,1258G 2,G2333x80", ',');

    std::vector<Datum> params{CreateDatum(ret.get()), CreateDatum(str1.get()),
                              CreateDatum(str2.get())};
    fu.test(params.data(), params.size(), CreateDatum(concat.get()));
  }

  // Vector concat Vector in NULL input
  {
    LOG_TESTING("Vector || Vector with null vector");
    auto str1 = vuString.generateVector("abc,cde,D e F,1258,G2333", ',');
    auto str2 = vuString.generateVector("NULL,NULL,NULL,NULL,NULL", ',');
    auto concat = vuString.generateVector("NULL,NULL,NULL,NULL,NULL", ',');

    std::vector<Datum> params{CreateDatum(ret.get()), CreateDatum(str1.get()),
                              CreateDatum(str2.get())};
    fu.test(params.data(), params.size(), CreateDatum(concat.get()));
  }

  // Vector concat Scalar while scalar is not null
  {
    LOG_TESTING("Vector || Scalar with normal input");
    auto str1 = vuString.generateVector("a,b 2333,NULL,cdEf,101,8x", ',');
    auto str2 = suString.generateScalar("t_");
    auto concat =
        vuString.generateVector("at_,b 2333t_,NULL,cdEft_,101t_,8xt_", ',');

    std::vector<Datum> params{CreateDatum(ret.get()), CreateDatum(str1.get()),
                              CreateDatum(&str2)};
    fu.test(params.data(), params.size(), CreateDatum(concat.get()));
  }

  // Vector concat Scalar while scalar is null
  {
    LOG_TESTING("Vector || Scalar with scalar is null");
    auto str1 = vuString.generateVector("a,b 2333,NULL,cdEf,101,8x", ',');
    auto str2 = suString.generateScalar("NULL");
    auto concat = vuString.generateVector("NULL,NULL,NULL,NULL,NULL,NULL", ',');

    std::vector<Datum> params{CreateDatum(ret.get()), CreateDatum(str1.get()),
                              CreateDatum(&str2)};
    fu.test(params.data(), params.size(), CreateDatum(concat.get()));
  }

  // Scalar concat Vector while scalar is not null
  {
    LOG_TESTING("Scalar || Vector with normal input");
    auto str1 = suString.generateScalar("a_");
    auto str2 = vuString.generateVector("b NULL NULL a");
    auto concat = vuString.generateVector("a_b NULL NULL a_a");

    std::vector<Datum> params{CreateDatum(ret.get()), CreateDatum(&str1),
                              CreateDatum(str2.get())};
    fu.test(params.data(), params.size(), CreateDatum(concat.get()));
  }

  // Scalar concat Vector while scalar is null
  {
    LOG_TESTING("scalar || vector with scalar is null");
    auto str1 = suString.generateScalar("NULL");
    auto str2 = vuString.generateVector("b NULL NULL a");
    auto concat = vuString.generateVector("NULL NULL NULL NULL");

    std::vector<Datum> params{CreateDatum(ret.get()), CreateDatum(&str1),
                              CreateDatum(str2.get())};
    fu.test(params.data(), params.size(), CreateDatum(concat.get()));
  }
}

TEST(TestFunction, string_position) {
  FunctionUtility fu(FuncKind::STRING_POSITION);
  VectorUtility vuString(TypeKind::STRINGID);
  VectorUtility vuInteger(TypeKind::INTID);
  ScalarUtility suString(TypeKind::STRINGID);
  auto ret = Vector::BuildVector(TypeKind::INTID, true);

  {
    LOG_TESTING("Vector in Vector with normal input");
    auto lhs = vuString.generateVector(
        "ab,NULL,,1258,a b,abcd, b c,abcabcabcabcabcabd", ',');
    auto rhs = vuString.generateVector(
        "abc,xxx,,NULL,abb,abc,a b c,abcabcabcabcabcabcabd", ',');
    auto position = vuInteger.generateVector("1 NULL 1 NULL 0 0 2 4");

    std::vector<Datum> params{CreateDatum(ret.get()), CreateDatum(rhs.get()),
                              CreateDatum(lhs.get())};
    fu.test(params.data(), params.size(), CreateDatum(position.get()));
  }

  {
    LOG_TESTING("Vector in Vector with normal input");
    auto lhs = vuString.generateVector("一2三,,ab西,六六六,我 2,三四", ',');
    auto rhs = vuString.generateVector(
        "一2三4,xxx,一二ab西,七七七,我2我 2快乐水,一二三四", ',');
    auto position = vuInteger.generateVector("1 1 3 0 3 3");

    std::vector<Datum> params{CreateDatum(ret.get()), CreateDatum(rhs.get()),
                              CreateDatum(lhs.get())};
    fu.test(params.data(), params.size(), CreateDatum(position.get()));
  }

  {
    LOG_TESTING("Vector in Vector with NULL input");
    auto lhs = vuString.generateVector("ab,NULL,21,1258,a b,abcd, b c", ',');
    auto rhs =
        vuString.generateVector("NULL,NULL,NULL,NULL,NULL,NULL,NULL", ',');
    auto position =
        vuInteger.generateVector("NULL NULL NULL NULL NULL NULL NULL");

    std::vector<Datum> params{CreateDatum(ret.get()), CreateDatum(rhs.get()),
                              CreateDatum(lhs.get())};
    fu.test(params.data(), params.size(), CreateDatum(position.get()));
  }

  {
    LOG_TESTING("Vector in Scalar with normal input");
    auto lhs = vuString.generateVector(
        "ab,NULL,abab,abcb,a b,abcabdab, ab,abcabcabcabcabda,", ',');
    auto rhs = suString.generateScalar("abcabd");
    auto position = vuInteger.generateVector("1 NULL 0 0 0 0 0 0 1");

    std::vector<Datum> params{CreateDatum(ret.get()), CreateDatum(&rhs),
                              CreateDatum(lhs.get())};
    fu.test(params.data(), params.size(), CreateDatum(position.get()));
  }

  {
    LOG_TESTING("Vector in Scalar with null input");
    auto lhs =
        vuString.generateVector("ab,NULL,abab,abcb,a b,abcabdab, ab", ',');
    auto rhs = suString.generateScalar("NULL");
    auto position =
        vuInteger.generateVector("NULL NULL NULL NULL NULL NULL NULL");

    std::vector<Datum> params{CreateDatum(ret.get()), CreateDatum(&rhs),
                              CreateDatum(lhs.get())};
    fu.test(params.data(), params.size(), CreateDatum(position.get()));
  }

  {
    LOG_TESTING("Scalar in Vector with normal input");
    auto lhs = suString.generateScalar("23");
    auto rhs = vuString.generateVector("233,NULL,666,", ',');
    auto position = vuInteger.generateVector("1 NULL 0 0");

    std::vector<Datum> params{CreateDatum(ret.get()), CreateDatum(rhs.get()),
                              CreateDatum(&lhs)};
    fu.test(params.data(), params.size(), CreateDatum(position.get()));
  }

  {
    LOG_TESTING("Scalar in Vector with null input");
    auto lhs = suString.generateScalar("NULL");
    auto rhs = vuString.generateVector("abc,NULL,ababcabd,a bcabd,abdabc", ',');
    auto position = vuInteger.generateVector("NULL NULL NULL NULL NULL");

    std::vector<Datum> params{CreateDatum(ret.get()), CreateDatum(rhs.get()),
                              CreateDatum(&lhs)};
    fu.test(params.data(), params.size(), CreateDatum(position.get()));
  }

  {
    LOG_TESTING("Scalar in Vector with special input");
    auto lhs = suString.generateScalar("233233233233234");
    auto rhs = vuString.generateVector(
        "233233233233233234,NULL,6666666666666666", ',');
    auto position = vuInteger.generateVector("4 NULL 0");

    std::vector<Datum> params{CreateDatum(ret.get()), CreateDatum(rhs.get()),
                              CreateDatum(&lhs)};
    fu.test(params.data(), params.size(), CreateDatum(position.get()));
  }
}

INSTANTIATE_TEST_CASE_P(string_initcap, TestFunction,
                        ::testing::Values(TestFunctionEntry{
                            FuncKind::STRING_INITCAP,
                            "Vector{delimiter=,}: Cat,NULL,I Like Fish, "
                            "52a*C,2b,Love中国,厉害abc,Abc,1b3,",
                            {"Vector{delimiter=,}: caT,NULL,i like fish, "
                             "52a*c,2B,love中国,厉害ABC,Abc,1b3,"}}));

TEST(TestFunction, string_initcap) {
  FunctionUtility fu(FuncKind::STRING_INITCAP);
  VectorUtility vuString(TypeKind::STRINGID);

  auto strs = vuString.generateVector(
      "caT,NULL,i like fish, 52a*c,2B,love中国,厉害ABC,Abc,1b3,", ',');
  auto initcap = vuString.generateVector(
      "Cat,NULL,I Like Fish, 52a*C,2b,Love中国,厉害abc,Abc,1b3,", ',');
  auto ret = Vector::BuildVector(TypeKind::STRINGID, true);

  std::vector<Datum> params{CreateDatum(ret.get()), CreateDatum(strs.get())};
  fu.test(params.data(), params.size(), CreateDatum(initcap.get()));
}

TEST(TestFunction, string_ascii) {
  FunctionUtility fu(FuncKind::STRING_ASCII);
  VectorUtility vuString(TypeKind::STRINGID);
  VectorUtility vuInteger(TypeKind::INTID);

  auto strs = vuString.generateVector("abc, 1,92,Я,中国,𒓔,NULL,", ',');
  auto ascii = vuInteger.generateVector("97 32 57 1071 20013 74964 NULL 0");
  auto ret = Vector::BuildVector(TypeKind::INTID, true);

  std::vector<Datum> params{CreateDatum(ret.get()), CreateDatum(strs.get())};
  fu.test(params.data(), params.size(), CreateDatum(ascii.get()));
}

TEST(TestFunction, string_varchar) {
  FunctionUtility fu(FuncKind::STRING_VARCHAR);

  VectorUtility vuStr(TypeKind::STRINGID);
  VectorUtility vuInt(TypeKind::INTID);
  VectorUtility vuBoo(TypeKind::BOOLEANID);

  ScalarUtility suStr(TypeKind::STRINGID);
  ScalarUtility suInt(TypeKind::INTID);
  ScalarUtility suBoo(TypeKind::BOOLEANID);

  // Case 1.1: vec vec vec
  {
    LOG_TESTING("string varchar: vec vec vec");

    auto strs = vuStr.generateVector("a abc NULL");
    auto lens = vuInt.generateVector("6 6 6");
    auto exps = vuBoo.generateVector("t t t");

    auto rtns = vuStr.generateVector("a ab NULL");

    auto rets = Vector::BuildVector(TypeKind::STRINGID, true);
    std::vector<Datum> params{CreateDatum(rets.get()), CreateDatum(strs.get()),
                              CreateDatum(lens.get()), CreateDatum(exps.get())};
    fu.test(params.data(), params.size(), CreateDatum(rtns.get()));
  }

  // Case 1.2: vec vec vec
  {
    LOG_TESTING("string varchar: vec vec vec");

    auto strs = vuStr.generateVector("abc abcd");
    auto lens = vuInt.generateVector("6 7");
    auto exps = vuBoo.generateVector("f f");

    auto rets = Vector::BuildVector(TypeKind::STRINGID, true);
    std::vector<Datum> params{CreateDatum(rets.get()), CreateDatum(strs.get()),
                              CreateDatum(lens.get()), CreateDatum(exps.get())};
    fu.test(params.data(), params.size(), CreateDatum(0),
            ERRCODE_STRING_DATA_RIGHT_TRUNCATION);
  }

  {
    LOG_TESTING("string varchar: vec vec vec");

    auto strs = vuStr.generateVector("中 a中国 NULL");
    auto lens = vuInt.generateVector("6 6 6");
    auto exps = vuBoo.generateVector("t t t");

    auto rtns = vuStr.generateVector("中 a中 NULL");

    auto rets = Vector::BuildVector(TypeKind::STRINGID, true);
    std::vector<Datum> params{CreateDatum(rets.get()), CreateDatum(strs.get()),
                              CreateDatum(lens.get()), CreateDatum(exps.get())};
    fu.test(params.data(), params.size(), CreateDatum(rtns.get()));
  }

  // Case 2.1: vec vec val(NOT NULL)
  {
    LOG_TESTING("string varchar: vec vec val(NOT NULL)");

    auto strs = vuStr.generateVector("a abc NULL");
    auto lens = vuInt.generateVector("6 6 6");
    auto exp = suBoo.generateScalar("t");

    auto rtns = vuStr.generateVector("a ab NULL");

    auto rets = Vector::BuildVector(TypeKind::STRINGID, true);
    std::vector<Datum> params{CreateDatum(rets.get()), CreateDatum(strs.get()),
                              CreateDatum(lens.get()), CreateDatum(&exp)};
    fu.test(params.data(), params.size(), CreateDatum(rtns.get()));
  }

  // Case 2.2: vec vec val(NULL)
  {
    LOG_TESTING("string varchar: vec vec val(NULL)");

    auto strs = vuStr.generateVector("a abc NULL");
    auto lens = vuInt.generateVector("6 6 6");
    auto exp = suBoo.generateScalar("NULL");

    auto rtns = vuStr.generateVector("NULL NULL NULL");

    auto rets = Vector::BuildVector(TypeKind::STRINGID, true);
    std::vector<Datum> params{CreateDatum(rets.get()), CreateDatum(strs.get()),
                              CreateDatum(lens.get()), CreateDatum(&exp)};
    fu.test(params.data(), params.size(), CreateDatum(rtns.get()));
  }

  // Case 2.3: vec vec val(NOT NULL)
  {
    LOG_TESTING("string varchar: vec vec val(NOT NULL)");

    auto strs = vuStr.generateVector("abc abcd");
    auto lens = vuInt.generateVector("6 7");
    auto exp = suBoo.generateScalar("f");

    auto rets = Vector::BuildVector(TypeKind::STRINGID, true);
    std::vector<Datum> params{CreateDatum(rets.get()), CreateDatum(strs.get()),
                              CreateDatum(lens.get()), CreateDatum(&exp)};
    fu.test(params.data(), params.size(), CreateDatum(0),
            ERRCODE_STRING_DATA_RIGHT_TRUNCATION);
  }

  // Case 3.1: vec val(NOT NULL) vec
  {
    LOG_TESTING("string varchar: vec val(NOT NULL) vec");

    auto strs = vuStr.generateVector("a abc NULL");
    auto lens = suInt.generateScalar("6");
    auto exp = vuBoo.generateVector("t t t");

    auto rtns = vuStr.generateVector("a ab NULL");

    auto rets = Vector::BuildVector(TypeKind::STRINGID, true);
    std::vector<Datum> params{CreateDatum(rets.get()), CreateDatum(strs.get()),
                              CreateDatum(&lens), CreateDatum(exp.get())};
    fu.test(params.data(), params.size(), CreateDatum(rtns.get()));
  }

  // Case 3.2: vec val(NULL) vec
  {
    LOG_TESTING("string varchar: vec val(NULL) vec");

    auto strs = vuStr.generateVector("a abc NULL");
    auto lens = suInt.generateScalar("NULL");
    auto exp = vuBoo.generateVector("t t t");

    auto rtns = vuStr.generateVector("NULL NULL NULL");

    auto rets = Vector::BuildVector(TypeKind::STRINGID, true);
    std::vector<Datum> params{CreateDatum(rets.get()), CreateDatum(strs.get()),
                              CreateDatum(&lens), CreateDatum(exp.get())};
    fu.test(params.data(), params.size(), CreateDatum(rtns.get()));
  }

  // Case 3.3: vec val(NOT NULL) vec
  {
    LOG_TESTING("string varchar: vec val(NOT NULL) vec");

    auto strs = vuStr.generateVector("abc abcd");
    auto lens = suInt.generateScalar("6");
    auto exp = vuBoo.generateVector("f f");

    auto rets = Vector::BuildVector(TypeKind::STRINGID, true);
    std::vector<Datum> params{CreateDatum(rets.get()), CreateDatum(strs.get()),
                              CreateDatum(&lens), CreateDatum(exp.get())};
    fu.test(params.data(), params.size(), CreateDatum(0),
            ERRCODE_STRING_DATA_RIGHT_TRUNCATION);
  }

  // Case 4.1: vec val(NOT NULL) val(NOT NULL)
  {
    LOG_TESTING("string varchar: vec val(NOT NULL) val(NOT NULL");

    auto strs = vuStr.generateVector("a abc NULL");
    auto lens = suInt.generateScalar("6");
    auto exp = suBoo.generateScalar("t");

    auto rtns = vuStr.generateVector("a ab NULL");

    auto rets = Vector::BuildVector(TypeKind::STRINGID, true);
    std::vector<Datum> params{CreateDatum(rets.get()), CreateDatum(strs.get()),
                              CreateDatum(&lens), CreateDatum(&exp)};
    fu.test(params.data(), params.size(), CreateDatum(rtns.get()));
  }

  // Case 4.2: vec val(NULL) val(NULL)
  {
    LOG_TESTING("string varchar: vec val(NULL) val((NULL");

    auto strs = vuStr.generateVector("a abc NULL");
    auto lens = suInt.generateScalar("NULL");
    auto exp = suBoo.generateScalar("NULL");

    auto rtns = vuStr.generateVector("NULL NULL NULL");

    auto rets = Vector::BuildVector(TypeKind::STRINGID, true);
    std::vector<Datum> params{CreateDatum(rets.get()), CreateDatum(strs.get()),
                              CreateDatum(&lens), CreateDatum(&exp)};
    fu.test(params.data(), params.size(), CreateDatum(rtns.get()));
  }

  // Case 4.3: vec val(NOT NULL) val(NOT NULL)
  {
    LOG_TESTING("string varchar: vec val(NOT NULL) val(NOT NULL");

    auto strs = vuStr.generateVector("abc abcd");
    auto lens = suInt.generateScalar("6");
    auto exp = suBoo.generateScalar("f");

    auto rets = Vector::BuildVector(TypeKind::STRINGID, true);
    std::vector<Datum> params{CreateDatum(rets.get()), CreateDatum(strs.get()),
                              CreateDatum(&lens), CreateDatum(&exp)};
    fu.test(params.data(), params.size(), CreateDatum(0),
            ERRCODE_STRING_DATA_RIGHT_TRUNCATION);
  }

  // Case 5.1: val(NOT NULL) vec vec
  {
    LOG_TESTING("string varchar: val(NOT NULL) vec vec");

    auto strs = suStr.generateScalar("abc");
    auto lens = vuInt.generateVector("6 7 8");
    auto exp = vuBoo.generateVector("t t NULL");

    auto rtns = vuStr.generateVector("ab abc NULL");

    auto rets = Vector::BuildVector(TypeKind::STRINGID, true);
    std::vector<Datum> params{CreateDatum(rets.get()), CreateDatum(&strs),
                              CreateDatum(lens.get()), CreateDatum(exp.get())};
    fu.test(params.data(), params.size(), CreateDatum(rtns.get()));
  }

  // Case 5.2: val(NULL) vec vec
  {
    LOG_TESTING("string varchar: val(NULL) vec vec");

    auto strs = suStr.generateScalar("NULL");
    auto lens = vuInt.generateVector("6 7 8");
    auto exp = vuBoo.generateVector("t t NULL");

    auto rtns = vuStr.generateVector("NULL NULL NULL");

    auto rets = Vector::BuildVector(TypeKind::STRINGID, true);
    std::vector<Datum> params{CreateDatum(rets.get()), CreateDatum(&strs),
                              CreateDatum(lens.get()), CreateDatum(exp.get())};
    fu.test(params.data(), params.size(), CreateDatum(rtns.get()));
  }

  // Case 5.3: val(NOT NULL) vec vec
  {
    LOG_TESTING("string varchar: val(NOT NULL) vec vec");

    auto strs = suStr.generateScalar("abcde");
    auto lens = vuInt.generateVector("6 7 8");
    auto exp = vuBoo.generateVector("f f f");

    auto rets = Vector::BuildVector(TypeKind::STRINGID, true);
    std::vector<Datum> params{CreateDatum(rets.get()), CreateDatum(&strs),
                              CreateDatum(lens.get()), CreateDatum(exp.get())};
    fu.test(params.data(), params.size(), CreateDatum(0),
            ERRCODE_STRING_DATA_RIGHT_TRUNCATION);
  }

  // Case 6.1: val(NOT NULL) vec val(NOT NULL)
  {
    LOG_TESTING("string varchar: val(NOT NULL) vec val(NOT NULL)");

    auto strs = suStr.generateScalar("abc");
    auto lens = vuInt.generateVector("6 7 NULL");
    auto exp = suBoo.generateScalar("t");

    auto rtns = vuStr.generateVector("ab abc NULL");

    auto rets = Vector::BuildVector(TypeKind::STRINGID, true);
    std::vector<Datum> params{CreateDatum(rets.get()), CreateDatum(&strs),
                              CreateDatum(lens.get()), CreateDatum(&exp)};
    fu.test(params.data(), params.size(), CreateDatum(rtns.get()));
  }

  // Case 6.2: val(NULL) vec val(NULL)
  {
    LOG_TESTING("string varchar: val(NULL) vec val(NULL)");

    auto strs = suStr.generateScalar("NULL");
    auto lens = vuInt.generateVector("6 7 NULL");
    auto exp = suBoo.generateScalar("NULL");

    auto rtns = vuStr.generateVector("NULL NULL NULL");

    auto rets = Vector::BuildVector(TypeKind::STRINGID, true);
    std::vector<Datum> params{CreateDatum(rets.get()), CreateDatum(&strs),
                              CreateDatum(lens.get()), CreateDatum(&exp)};
    fu.test(params.data(), params.size(), CreateDatum(rtns.get()));
  }

  // Case 6.3: val(NOT NULL) vec val(NOT NULL)
  {
    LOG_TESTING("string varchar: val(NOT NULL) vec val(NOT NULL)");

    auto strs = suStr.generateScalar("abcd");
    auto lens = vuInt.generateVector("6 7");
    auto exp = suBoo.generateScalar("f");

    auto rets = Vector::BuildVector(TypeKind::STRINGID, true);
    std::vector<Datum> params{CreateDatum(rets.get()), CreateDatum(&strs),
                              CreateDatum(lens.get()), CreateDatum(&exp)};
    fu.test(params.data(), params.size(), CreateDatum(0),
            ERRCODE_STRING_DATA_RIGHT_TRUNCATION);
  }

  // Case 7.1: val(NOT NULL) val(NOT NULL) vec
  {
    LOG_TESTING("string varchar: val(NOT NULL) val(NOT NULL) vec");

    auto strs = suStr.generateScalar("abc");
    auto lens = suInt.generateScalar("6");
    auto exp = vuBoo.generateVector("t t NULL");

    auto rtns = vuStr.generateVector("ab ab NULL");

    auto rets = Vector::BuildVector(TypeKind::STRINGID, true);
    std::vector<Datum> params{CreateDatum(rets.get()), CreateDatum(&strs),
                              CreateDatum(&lens), CreateDatum(exp.get())};
    fu.test(params.data(), params.size(), CreateDatum(rtns.get()));
  }

  // Case 7.2: val(NULL) val(NULL) vec
  {
    LOG_TESTING("string varchar: val(NULL) val(NULL)s vec");

    auto strs = suStr.generateScalar("NULL");
    auto lens = suInt.generateScalar("NULL");
    auto exp = vuBoo.generateVector("t t NULL");

    auto rtns = vuStr.generateVector("NULL NULL NULL");

    auto rets = Vector::BuildVector(TypeKind::STRINGID, true);
    std::vector<Datum> params{CreateDatum(rets.get()), CreateDatum(&strs),
                              CreateDatum(&lens), CreateDatum(exp.get())};
    fu.test(params.data(), params.size(), CreateDatum(rtns.get()));
  }

  // Case 7.3: val(NOT NULL) val(NOT NULL) vec
  {
    LOG_TESTING("string varchar: val(NOT NULL) val(NOT NULL) vec");

    auto strs = suStr.generateScalar("abc");
    auto lens = suInt.generateScalar("6");
    auto exp = vuBoo.generateVector("f f");

    auto rets = Vector::BuildVector(TypeKind::STRINGID, true);
    std::vector<Datum> params{CreateDatum(rets.get()), CreateDatum(&strs),
                              CreateDatum(&lens), CreateDatum(exp.get())};
    fu.test(params.data(), params.size(), CreateDatum(0),
            ERRCODE_STRING_DATA_RIGHT_TRUNCATION);
  }
}

TEST(TestFunction, string_ltrim_blank) {
  FunctionUtility fu(FuncKind::STRING_LTRIM_BLANK);
  VectorUtility vuStr(TypeKind::STRINGID);

  {
    LOG_TESTING("trim vector from vector with null input");
    auto lhs = vuStr.generateVector("NULL,abc,NULL, a bc d ,  aa bc dd  ", ',');
    auto exp = vuStr.generateVector("NULL,abc,NULL,a bc d ,aa bc dd  ", ',');
    auto ret = Vector::BuildVector(TypeKind::STRINGID, true);

    std::vector<Datum> params{CreateDatum(ret.get()), CreateDatum(lhs.get())};

    fu.test(params.data(), params.size(), CreateDatum(exp.get()));
  }

  {
    auto strs = vuStr.generateVector(
        " caT,NULL,  shEEp,NULL,   009bb, is,  GOOD,   中国China", ',');
    auto trim = vuStr.generateVector(
        "caT,NULL,shEEp,NULL,009bb,is,GOOD,中国China", ',');
    auto ret = Vector::BuildVector(TypeKind::STRINGID, true);

    std::vector<Datum> params{CreateDatum(ret.get()), CreateDatum(strs.get())};
    fu.test(params.data(), params.size(), CreateDatum(trim.get()));
  }
}

TEST(TestFunction, string_ltrim_chars) {
  FunctionUtility fu(FuncKind::STRING_LTRIM_CHARS);
  VectorUtility vu(TypeKind::STRINGID);
  ScalarUtility su(TypeKind::STRINGID);

  {
    auto strs = vu.generateVector(
        "XcaT NULL YYshEEp NULL ZZZ009bb xis yyGOOD zzz中国China");
    auto chrs = vu.generateVector("X A Y B Z x y z");
    auto trim =
        vu.generateVector("caT NULL shEEp NULL 009bb is GOOD 中国China");
    auto ret = Vector::BuildVector(TypeKind::STRINGID, true);

    std::vector<Datum> params{CreateDatum(ret.get()), CreateDatum(strs.get()),
                              CreateDatum(chrs.get())};
    fu.test(params.data(), params.size(), CreateDatum(trim.get()));
  }

  {
    auto strs = su.generateScalar("XYZxyz");
    auto chrs = vu.generateVector("X NULL x XY XYZ");
    auto trim = vu.generateVector("YZxyz NULL XYZxyz Zxyz xyz");
    auto ret = Vector::BuildVector(TypeKind::STRINGID, true);

    std::vector<Datum> params{CreateDatum(ret.get()), CreateDatum(&strs),
                              CreateDatum(chrs.get())};
    fu.test(params.data(), params.size(), CreateDatum(trim.get()));
  }

  {
    auto strs = vu.generateVector(
        "XcaT NULL YYshEEp NULL ZZZ009bb xis yyGOOD zzz中国China");
    auto trim =
        vu.generateVector("caT NULL shEEp NULL 009bb is GOOD 中国China");
    auto ret = Vector::BuildVector(TypeKind::STRINGID, true);

    auto set = su.generateScalar("XYZxyz");
    std::vector<Datum> params{CreateDatum(ret.get()), CreateDatum(strs.get()),
                              CreateDatum(&set)};
    fu.test(params.data(), params.size(), CreateDatum(trim.get()));
  }
}

TEST(TestFunction, string_rtrim_blank) {
  FunctionUtility fu(FuncKind::STRING_RTRIM_BLANK);
  VectorUtility vu(TypeKind::STRINGID);

  auto strs = vu.generateVector(
      "caT ,NULL,shEEp  ,NULL,009bb   ,is ,GOOD  ,中国China   ", ',');
  auto trim =
      vu.generateVector("caT,NULL,shEEp,NULL,009bb,is,GOOD,中国China", ',');
  auto ret = Vector::BuildVector(TypeKind::STRINGID, true);

  std::vector<Datum> params{CreateDatum(ret.get()), CreateDatum(strs.get())};
  fu.test(params.data(), params.size(), CreateDatum(trim.get()));
}

TEST(TestFunction, string_rtrim_chars) {
  FunctionUtility fu(FuncKind::STRING_RTRIM_CHARS);
  VectorUtility vu(TypeKind::STRINGID);
  ScalarUtility su(TypeKind::STRINGID);

  {
    auto strs = vu.generateVector(
        "caTX NULL shEEpYY NULL 009bbZZZ isx GOODyy 中国Chinazzz");
    auto chrs = vu.generateVector("X XYZ Y, xyz Z x y z");
    auto trim =
        vu.generateVector("caT NULL shEEp NULL 009bb is GOOD 中国China");
    auto ret = Vector::BuildVector(TypeKind::STRINGID, true);

    std::vector<Datum> params{CreateDatum(ret.get()), CreateDatum(strs.get()),
                              CreateDatum(chrs.get())};
    fu.test(params.data(), params.size(), CreateDatum(trim.get()));
  }

  {
    auto strs = vu.generateVector(
        "caTX NULL shEEpYY NULL 009bbZZZ isx GOODyy 中国Chinazzz");
    auto trim =
        vu.generateVector("caT NULL shEEp NULL 009bb is GOOD 中国China");
    auto ret = Vector::BuildVector(TypeKind::STRINGID, true);

    auto set = su.generateScalar("XYZxyz");
    std::vector<Datum> params{CreateDatum(ret.get()), CreateDatum(strs.get()),
                              CreateDatum(&set)};
    fu.test(params.data(), params.size(), CreateDatum(trim.get()));
  }

  {
    auto strs = su.generateScalar("XYZxyz");
    auto chrs = vu.generateVector("z NULL yz xyz");
    auto trim = vu.generateVector("XYZxy NULL XYZx XYZ");
    auto ret = Vector::BuildVector(TypeKind::STRINGID, true);

    std::vector<Datum> params{CreateDatum(ret.get()), CreateDatum(&strs),
                              CreateDatum(chrs.get())};
    fu.test(params.data(), params.size(), CreateDatum(trim.get()));
  }
}

TEST(TestFunction, string_btrim_blank) {
  FunctionUtility fu(FuncKind::STRING_BTRIM_BLANK);
  VectorUtility vu(TypeKind::STRINGID);

  auto strs = vu.generateVector(
      " caT ,NULL,  shEEp  ,NULL,   009bb   , is ,  GOOD  ,   中国China   ",
      ',');
  auto trim =
      vu.generateVector("caT,NULL,shEEp,NULL,009bb,is,GOOD,中国China", ',');
  auto ret = Vector::BuildVector(TypeKind::STRINGID, true);

  std::vector<Datum> params{CreateDatum(ret.get()), CreateDatum(strs.get())};
  fu.test(params.data(), params.size(), CreateDatum(trim.get()));
}

TEST(TestFunction, string_btrim_chars) {
  FunctionUtility fu(FuncKind::STRING_BTRIM_CHARS);
  VectorUtility vu(TypeKind::STRINGID);
  ScalarUtility su(TypeKind::STRINGID);

  {
    auto strs = vu.generateVector(
        "xcaTX NULL yyshEEpYY NULL zzz009bbZZZ Xisx YYGOODyy ZZZ中国Chinazzz");
    auto chrs = vu.generateVector("xX xyz yY XYZ zZ xX yY zZ");
    auto trim =
        vu.generateVector("caT NULL shEEp NULL 009bb is GOOD 中国China");
    auto ret = Vector::BuildVector(TypeKind::STRINGID, true);

    std::vector<Datum> params{CreateDatum(ret.get()), CreateDatum(strs.get()),
                              CreateDatum(chrs.get())};
    fu.test(params.data(), params.size(), CreateDatum(trim.get()));
  }

  {
    auto strs = vu.generateVector(
        "xcaTX NULL yyshEEpYY NULL zzz009bbZZZ Xisx YYGOODyy ZZZ中国Chinazzz");
    auto trim =
        vu.generateVector("caT NULL shEEp NULL 009bb is GOOD 中国China");
    auto ret = Vector::BuildVector(TypeKind::STRINGID, true);

    auto set = su.generateScalar("XYZxyz");
    std::vector<Datum> params{CreateDatum(ret.get()), CreateDatum(strs.get()),
                              CreateDatum(&set)};
    fu.test(params.data(), params.size(), CreateDatum(trim.get()));
  }

  {
    auto strs = su.generateScalar("xyzXYZ");
    auto chrs = vu.generateVector("NULL x xy xyZ");
    auto trim = vu.generateVector("NULL yzXYZ zXYZ zXY");
    auto ret = Vector::BuildVector(TypeKind::STRINGID, true);

    std::vector<Datum> params{CreateDatum(ret.get()), CreateDatum(&strs),
                              CreateDatum(chrs.get())};
    fu.test(params.data(), params.size(), CreateDatum(trim.get()));
  }
}

INSTANTIATE_TEST_CASE_P(
    string_repeat, TestFunction,
    ::testing::Values(
        TestFunctionEntry{
            FuncKind::STRING_REPEAT,
            "Vector{delimiter=,}: abc,NULL,AbCAbCAbC,,小可爱小可爱,",
            {"Vector{delimiter=,}: abc,NULL,AbC,bca,小可爱,",
             "Vector: 1 2 3 0 2 2"}},
        TestFunctionEntry{FuncKind::STRING_REPEAT,
                          "Error",
                          {"Vector: str", "Vector: 2147483640"},
                          ERRCODE_PROGRAM_LIMIT_EXCEEDED},
        TestFunctionEntry{
            FuncKind::STRING_REPEAT,
            "Vector{delimiter=,}: abcabc,NULL,AbCAbC,小可爱小可爱,",
            {"Vector{delimiter=,}: abc,NULL,AbC,小可爱,", "Scalar: 2"}},
        TestFunctionEntry{FuncKind::STRING_REPEAT,
                          "Error",
                          {"Vector: str", "Scalar: 2147483640"},
                          ERRCODE_PROGRAM_LIMIT_EXCEEDED},
        TestFunctionEntry{
            FuncKind::STRING_REPEAT,
            "Vector: NULL NULL NULL NULL NULL",
            {"Vector{delimiter=,}: abc,NULL,AbC,小可爱,", "Scalar: NULL"}},
        TestFunctionEntry{FuncKind::STRING_REPEAT,
                          "Vector{delimiter=,}: 2数2数,,NULL",
                          {"Scalar: 2数", "Vector: 2 0 NULL"}},
        TestFunctionEntry{FuncKind::STRING_REPEAT,
                          "Error",
                          {"Scalar: str", "Vector: 2147483640"},
                          ERRCODE_PROGRAM_LIMIT_EXCEEDED},
        TestFunctionEntry{FuncKind::STRING_REPEAT,
                          "Vector: NULL NULL NULL",
                          {"Scalar: NULL", "Vector: 2 0 4"}}));

TEST(TestFunction, string_repeat) {
  FunctionUtility fu(FuncKind::STRING_REPEAT);
  VectorUtility vuString(TypeKind::STRINGID);
  VectorUtility vuInteger(TypeKind::INTID);
  ScalarUtility suString(TypeKind::STRINGID);
  ScalarUtility suInteger(TypeKind::INTID);
  auto ret = Vector::BuildVector(TypeKind::STRINGID, true);

  {
    LOG_TESTING("Vector repeat Vecotr");
    auto strs = vuString.generateVector("abc,NULL,AbC,bca,小可爱,", ',');
    auto dup = vuInteger.generateVector("1 2 3 0 2 2");
    auto repeat =
        vuString.generateVector("abc,NULL,AbCAbCAbC,,小可爱小可爱,", ',');

    std::vector<Datum> params{CreateDatum(ret.get()), CreateDatum(strs.get()),
                              CreateDatum(dup.get())};
    fu.test(params.data(), params.size(), CreateDatum(repeat.get()));
  }

  {
    LOG_TESTING("Vector repeat Vecotr with error");
    auto strs = vuString.generateVector("str");
    auto dup = vuInteger.generateVector("2147483640");

    std::vector<Datum> params{CreateDatum(ret.get()), CreateDatum(strs.get()),
                              CreateDatum(dup.get())};
    fu.test(params.data(), params.size(), CreateDatum(0),
            ERRCODE_PROGRAM_LIMIT_EXCEEDED);
  }

  {
    LOG_TESTING("Vector repeat Scalar with normal input");
    auto strs = vuString.generateVector("abc,NULL,AbC,小可爱,", ',');
    auto dup = suInteger.generateScalar("2");
    auto repeat =
        vuString.generateVector("abcabc,NULL,AbCAbC,小可爱小可爱,", ',');

    std::vector<Datum> params{CreateDatum(ret.get()), CreateDatum(strs.get()),
                              CreateDatum(&dup)};
    fu.test(params.data(), params.size(), CreateDatum(repeat.get()));
  }

  {
    LOG_TESTING("Vector repeat Scalar with error");
    auto strs = vuString.generateVector("str");
    auto dup = suInteger.generateScalar("2147483640");

    std::vector<Datum> params{CreateDatum(ret.get()), CreateDatum(strs.get()),
                              CreateDatum(&dup)};
    fu.test(params.data(), params.size(), CreateDatum(0),
            ERRCODE_PROGRAM_LIMIT_EXCEEDED);
  }

  {
    LOG_TESTING("Vector repeat Scalar with null input");
    auto strs = vuString.generateVector("abc,NULL,AbC,小可爱,", ',');
    auto dup = suInteger.generateScalar("NULL");
    auto repeat = vuString.generateVector("NULL,NULL,NULL,NULL,NULL", ',');

    std::vector<Datum> params{CreateDatum(ret.get()), CreateDatum(strs.get()),
                              CreateDatum(&dup)};
    fu.test(params.data(), params.size(), CreateDatum(repeat.get()));
  }

  {
    LOG_TESTING("Scalar repeat Vector with normal input");
    auto strs = suString.generateScalar("2数");
    auto dup = vuInteger.generateVector("2 0 NULL");
    auto repeat = vuString.generateVector("2数2数,,NULL", ',');

    std::vector<Datum> params{CreateDatum(ret.get()), CreateDatum(&strs),
                              CreateDatum(dup.get())};
    fu.test(params.data(), params.size(), CreateDatum(repeat.get()));
  }

  {
    LOG_TESTING("Scalar repeat Vector with error");
    auto strs = suString.generateScalar("str");
    auto dup = vuInteger.generateVector("2147483640");

    std::vector<Datum> params{CreateDatum(ret.get()), CreateDatum(&strs),
                              CreateDatum(dup.get())};
    fu.test(params.data(), params.size(), CreateDatum(0),
            ERRCODE_PROGRAM_LIMIT_EXCEEDED);
  }

  {
    LOG_TESTING("Scalar repeat Vector with null input");
    auto strs = suString.generateScalar("NULL");
    auto dup = vuInteger.generateVector("1 0 5");
    auto repeat = vuString.generateVector("NULL NULL NULL");

    std::vector<Datum> params{CreateDatum(ret.get()), CreateDatum(&strs),
                              CreateDatum(dup.get())};
    fu.test(params.data(), params.size(), CreateDatum(repeat.get()));
  }
}

INSTANTIATE_TEST_CASE_P(
    string_chr, TestFunction,
    ::testing::Values(
        TestFunctionEntry{FuncKind::STRING_CHR,
                          "Vector: \x01 A \x7F \u0080 ÿ Ā ✐ 𒓔 NULL",
                          {"Vector: 1 65 127 128 255 256 10000 74964 NULL"}},
        TestFunctionEntry{FuncKind::STRING_CHR,
                          "Error",
                          {"Vector: 2333333"},
                          ERRCODE_PROGRAM_LIMIT_EXCEEDED},
        TestFunctionEntry{FuncKind::STRING_CHR,
                          "Error",
                          {"Vector: 0"},
                          ERRCODE_PROGRAM_LIMIT_EXCEEDED}));

INSTANTIATE_TEST_CASE_P(
    string_bpchar, TestFunction,
    ::testing::Values(
        TestFunctionEntry{FuncKind::STRING_BPCHAR,
                          "Vector{delimiter=,}: a ,ab,NULL",
                          {"Vector{delimiter=,}: a,ab ,NULL", "Vector: 6 6 6",
                           "Vector: t t t"}},
        TestFunctionEntry{FuncKind::STRING_BPCHAR,
                          "Error",
                          {"Vector: abc", "Vector: 6", "Vector: f"},
                          ERRCODE_STRING_DATA_RIGHT_TRUNCATION},
        TestFunctionEntry{FuncKind::STRING_BPCHAR,
                          "Vector{delimiter=,}: 中 ,中国,a中国,NULL",
                          {"Vector{delimiter=,}: 中,中国 ,a中国B,NULL",
                           "Vector: 6 6 7 6", "Vector: t t t t"}},
        TestFunctionEntry{
            FuncKind::STRING_BPCHAR,
            "Vector{delimiter=,}: a ,ab,NULL",
            {"Vector{delimiter=,}: a,ab ,NULL", "Vector: 6 6 6", "Scalar: t"}},
        TestFunctionEntry{
            FuncKind::STRING_BPCHAR,
            "Vector: NULL NULL NULL",
            {"Vector: a ab NULL", "Vector: 6 6 6", "Scalar: NULL"}},
        TestFunctionEntry{FuncKind::STRING_BPCHAR,
                          "Error",
                          {"Vector: abc", "Vector: 6", "Scalar: f"},
                          ERRCODE_STRING_DATA_RIGHT_TRUNCATION},
        TestFunctionEntry{
            FuncKind::STRING_BPCHAR,
            "Vector{delimiter=,}: a ,ab,NULL",
            {"Vector{delimiter=,}: a,ab ,NULL", "Scalar: 6", "Vector: t t t"}},
        TestFunctionEntry{
            FuncKind::STRING_BPCHAR,
            "Vector: NULL NULL NULL",
            {"Vector: a ab NULL", "Scalar: NULL", "Vector: t t t"}},
        TestFunctionEntry{FuncKind::STRING_BPCHAR,
                          "Error",
                          {"Vector: abc", "Scalar: 6", "Vector: f"},
                          ERRCODE_STRING_DATA_RIGHT_TRUNCATION},
        TestFunctionEntry{
            FuncKind::STRING_BPCHAR,
            "Vector{delimiter=,}: a ,ab,NULL",
            {"Vector{delimiter=,}: a,ab ,NULL", "Scalar: 6", "Scalar: t"}},
        TestFunctionEntry{FuncKind::STRING_BPCHAR,
                          "Vector: NULL NULL NULL",
                          {"Vector: a ab NULL", "Scalar: NULL", "Scalar: t"}},
        TestFunctionEntry{FuncKind::STRING_BPCHAR,
                          "Error",
                          {"Vector: abc", "Scalar: 6", "Scalar: f"},
                          ERRCODE_STRING_DATA_RIGHT_TRUNCATION},
        TestFunctionEntry{FuncKind::STRING_BPCHAR,
                          "Vector: ab abc NULL",
                          {"Scalar: abc", "Vector: 6 7 8", "Vector: t t NULL"}},
        TestFunctionEntry{
            FuncKind::STRING_BPCHAR,
            "Vector: NULL NULL NULL",
            {"Scalar: NULL", "Vector: 6 7 8", "Vector: t t NULL"}},
        TestFunctionEntry{FuncKind::STRING_BPCHAR,
                          "Error",
                          {"Scalar: abc", "Vector: 6", "Vector: f"},
                          ERRCODE_STRING_DATA_RIGHT_TRUNCATION},
        TestFunctionEntry{FuncKind::STRING_BPCHAR,
                          "Vector: ab abc NULL",
                          {"Scalar: abc", "Vector: 6 7 NULL", "Scalar: t"}},
        TestFunctionEntry{FuncKind::STRING_BPCHAR,
                          "Vector: NULL NULL NULL",
                          {"Scalar: abc", "Vector: 6 7 NULL", "Scalar: NULL"}},
        TestFunctionEntry{FuncKind::STRING_BPCHAR,
                          "Error",
                          {"Scalar: abc", "Vector: 6", "Scalar: f"},
                          ERRCODE_STRING_DATA_RIGHT_TRUNCATION},
        TestFunctionEntry{FuncKind::STRING_BPCHAR,
                          "Vector: ab ab NULL",
                          {"Scalar: abc", "Scalar: 6", "Vector: t t NULL"}},
        TestFunctionEntry{FuncKind::STRING_BPCHAR,
                          "Vector: NULL NULL NULL",
                          {"Scalar: NULL", "Scalar: 6", "Vector: t t NULL"}},
        TestFunctionEntry{FuncKind::STRING_BPCHAR,
                          "Error",
                          {"Scalar: abc", "Scalar: 6", "Vector: f"},
                          ERRCODE_STRING_DATA_RIGHT_TRUNCATION}));

INSTANTIATE_TEST_CASE_P(
    string_lpad, TestFunction,
    ::testing::Values(
        TestFunctionEntry{
            FuncKind::STRING_LPAD,
            "Vector: 1二1一二 3一2三 66嘻嘻ff 我喜欢娃哈哈 吃鸡",
            {"Vector: 一二 一2三 66嘻嘻ff 娃哈哈 吃鸡100次",
             "Vector: 5 4 6 6 2", "Vector: 1二 3二1 哎哎哎 我喜欢 x"}},
        TestFunctionEntry{FuncKind::STRING_LPAD,
                          "Vector: 121xy 3abc 66ccff NULL",
                          {"Vector: xy abc 66ccff NULL", "Vector: 5 4 6 5",
                           "Vector: 12 321 xxx zzz"}},
        TestFunctionEntry{
            FuncKind::STRING_LPAD,
            "Vector: zxcxy zabc 66ccff NULL",
            {"Vector: xy abc 66ccff ixx", "Vector: 5 4 6 NULL", "Scalar: zxc"}},
        TestFunctionEntry{
            FuncKind::STRING_LPAD,
            "Vector: NULL NULL NULL",
            {"Vector: xy abc 66ccff", "Vector: 5 4 6", "Scalar: NULL"}},
        TestFunctionEntry{FuncKind::STRING_LPAD,
                          "Vector: 121xy 32abc 66ccf NULL",
                          {"Vector: xy abc 66ccff uvw", "Scalar: 5",
                           "Vector: 12 321 xy NULL"}},
        TestFunctionEntry{
            FuncKind::STRING_LPAD,
            "Vector: NULL NULL NULL",
            {"Vector: xy abc 66ccff", "Scalar: NULL", "Vector: 12 321 xy"}},
        TestFunctionEntry{
            FuncKind::STRING_LPAD,
            "Vector: 123xy 12abc 66ccf NULL",
            {"Vector: xy abc 66ccff NULL", "Scalar: 5", "Scalar: 123"}},
        TestFunctionEntry{
            FuncKind::STRING_LPAD,
            "Vector: NULL NULL NULL",
            {"Vector: xy abc 66ccff", "Scalar: NULL", "Scalar: 123"}},
        TestFunctionEntry{
            FuncKind::STRING_LPAD,
            "Vector: 66ccf 66ccff z66ccff NULL",
            {"Scalar: 66ccff", "Vector: 5 6 7 NULL", "Vector: x y z x"}},
        TestFunctionEntry{FuncKind::STRING_LPAD,
                          "Vector: NULL NULL NULL",
                          {"Scalar: NULL", "Vector: 4 5 6", "Vector: 1 2 3"}},
        TestFunctionEntry{
            FuncKind::STRING_LPAD,
            "Vector: 66c 66ccff xx66ccff NULL",
            {"Scalar: 66ccff", "Vector: 3 6 8 NULL", "Scalar: x"}},
        TestFunctionEntry{FuncKind::STRING_LPAD,
                          "Vector: NULL NULL NULL",
                          {"Scalar: 66ccff", "Vector: 4 5 6", "Scalar: NULL"}},
        TestFunctionEntry{FuncKind::STRING_LPAD,
                          "Vector: x6ccf y6ccf z6ccf NULL",
                          {"Scalar: 6ccf", "Scalar: 5", "Vector: x y z NULL"}},
        TestFunctionEntry{
            FuncKind::STRING_LPAD,
            "Vector: NULL NULL NULL",
            {"Scalar: 66ccff", "Scalar: NULL", "Vector: x y z"}}));

INSTANTIATE_TEST_CASE_P(
    string_rpad, TestFunction,
    ::testing::Values(
        TestFunctionEntry{
            FuncKind::STRING_RPAD,
            "Vector: 一二1二1 一2三3 66嘻嘻ff 娃哈哈我喜欢 吃鸡",
            {"Vector: 一二 一2三 66嘻嘻ff 娃哈哈 吃鸡100次",
             "Vector: 5 4 6 6 2", "Vector: 1二 3二1 哎哎哎 我喜欢 x"}},
        TestFunctionEntry{FuncKind::STRING_RPAD,
                          "Vector: xy121 abc3 66ccff NULL",
                          {"Vector: xy abc 66ccff NULL", "Vector: 5 4 6 5",
                           "Vector: 12 321 xxx zzz"}},
        TestFunctionEntry{
            FuncKind::STRING_RPAD,
            "Vector: xyzxc abcz 66ccff NULL",
            {"Vector: xy abc 66ccff ixx", "Vector: 5 4 6 NULL", "Scalar: zxc"}},
        TestFunctionEntry{
            FuncKind::STRING_RPAD,
            "Vector: NULL NULL NULL",
            {"Vector: xy abc 66ccff", "Vector: 5 4 6", "Scalar: NULL"}},
        TestFunctionEntry{FuncKind::STRING_RPAD,
                          "Vector: xy121 abc32 66ccf NULL",
                          {"Vector: xy abc 66ccff uvw", "Scalar: 5",
                           "Vector: 12 321 xy NULL"}},
        TestFunctionEntry{
            FuncKind::STRING_RPAD,
            "Vector: NULL NULL NULL",
            {"Vector: xy abc 66ccff", "Scalar: NULL", "Vector: 12 321 xy"}},
        TestFunctionEntry{
            FuncKind::STRING_RPAD,
            "Vector: xy123 abc12 66ccf NULL",
            {"Vector: xy abc 66ccff NULL", "Scalar: 5", "Scalar: 123"}},
        TestFunctionEntry{
            FuncKind::STRING_RPAD,
            "Vector: NULL NULL NULL",
            {"Vector: xy abc 66ccff", "Scalar: NULL", "Scalar: 123"}},
        TestFunctionEntry{
            FuncKind::STRING_RPAD,
            "Vector: 66ccf 66ccff 66ccffz NULL",
            {"Scalar: 66ccff", "Vector: 5 6 7 NULL", "Vector: x y z x"}},
        TestFunctionEntry{FuncKind::STRING_RPAD,
                          "Vector: NULL NULL NULL",
                          {"Scalar: NULL", "Vector: 4 5 6", "Vector: 1 2 3"}},
        TestFunctionEntry{
            FuncKind::STRING_RPAD,
            "Vector: 66c 66ccff 66ccffxx NULL",
            {"Scalar: 66ccff", "Vector: 3 6 8 NULL", "Scalar: x"}},
        TestFunctionEntry{FuncKind::STRING_RPAD,
                          "Vector: NULL NULL NULL",
                          {"Scalar: 66ccff", "Vector: 4 5 6", "Scalar: NULL"}},
        TestFunctionEntry{FuncKind::STRING_RPAD,
                          "Vector: 6ccfx 6ccfy 6ccfz NULL",
                          {"Scalar: 6ccf", "Scalar: 5", "Vector: x y z NULL"}},
        TestFunctionEntry{
            FuncKind::STRING_RPAD,
            "Vector: NULL NULL NULL",
            {"Scalar: 66ccff", "Scalar: NULL", "Vector: x y z"}}));

INSTANTIATE_TEST_CASE_P(
    string_lpad_nofill, TestFunction,
    ::testing::Values(
        TestFunctionEntry{
            FuncKind::STRING_LPAD_NOFILL,
            "Vector{delimiter=,}:  一二三,  二三四,66嘻嘻ff,二三四",
            {"Vector: 一二三 二三四 66嘻嘻ff 二三四五", "Vector: 4 5 6 3"}},
        TestFunctionEntry{FuncKind::STRING_LPAD_NOFILL,
                          "Vector{delimiter=,}:  xyz,  abc,66ccff,NULL",
                          {"Vector: xyz abc 66ccff NULL", "Vector: 4 5 6 7"}},
        TestFunctionEntry{FuncKind::STRING_LPAD_NOFILL,
                          "Vector{delimiter=,}:  xyz,1abc,66cc,NULL",
                          {"Vector: xyz 1abc 66ccff NULL", "Scalar: 4"}},
        TestFunctionEntry{FuncKind::STRING_LPAD_NOFILL,
                          "Vector: NULL NULL NULL",
                          {"Vector: xyz abc 66ccff", "Scalar: NULL"}},
        TestFunctionEntry{FuncKind::STRING_LPAD_NOFILL,
                          "Vector{delimiter=,}: xyz,xyz1, xyz1,NULL",
                          {"Scalar: xyz1", "Vector: 3 4 5 NULL"}},
        TestFunctionEntry{FuncKind::STRING_LPAD_NOFILL,
                          "Vector: NULL NULL NULL",
                          {"Scalar: NULL", "Vector: 4 5 6"}}));

INSTANTIATE_TEST_CASE_P(
    string_rpad_nofill, TestFunction,
    ::testing::Values(
        TestFunctionEntry{
            FuncKind::STRING_RPAD_NOFILL,
            "Vector{delimiter=,}: 一二三 ,二三四  ,66嘻嘻ff,二三四",
            {"Vector: 一二三 二三四 66嘻嘻ff 二三四五", "Vector: 4 5 6 3"}},
        TestFunctionEntry{FuncKind::STRING_RPAD_NOFILL,
                          "Vector{delimiter=,}: xyz ,abc  ,66ccff,NULL",
                          {"Vector: xyz abc 66ccff NULL", "Vector: 4 5 6 7"}},
        TestFunctionEntry{FuncKind::STRING_RPAD_NOFILL,
                          "Vector{delimiter=,}: xyz ,1abc,66cc,NULL",
                          {"Vector: xyz 1abc 66ccff NULL", "Scalar: 4"}},
        TestFunctionEntry{FuncKind::STRING_RPAD_NOFILL,
                          "Vector: NULL NULL NULL",
                          {"Vector: xyz abc 66ccff", "Scalar: NULL"}},
        TestFunctionEntry{FuncKind::STRING_RPAD_NOFILL,
                          "Vector{delimiter=,}: xyz,xyz1,xyz1 ,NULL",
                          {"Scalar: xyz1", "Vector: 3 4 5 NULL"}},
        TestFunctionEntry{FuncKind::STRING_RPAD_NOFILL,
                          "Vector: NULL NULL NULL",
                          {"Scalar: NULL", "Vector: 4 5 6"}}));
INSTANTIATE_TEST_CASE_P(
    string_translate, TestFunction,
    ::testing::Values(
        TestFunctionEntry{
            FuncKind::STRING_TRANSLATE,
            "Vector: 小1灵b2 真笨 1六六五 6六caa 6六cb诶诶",
            {"Vector: 小a灵b通 真厉害 1二3四五 6六c西ff 6六c西ff",
             "Vector: a通 厉害 二3四 ff西 f西f", "Vector: 12 笨 六六 a比 诶b"}},
        TestFunctionEntry{FuncKind::STRING_TRANSLATE,
                          "Vector{delimiter=,}: 1b2d,1cc1,    ,bcbc,NULL,",
                          {"Vector{delimiter=,}: abcd,abccba,aaaa,bcbc,NULL,",
                           "Vector{delimiter=,}: ac,ab,a,,x,x",
                           "Vector{delimiter=,}: 123,1, ,b,y,y"}},
        TestFunctionEntry{FuncKind::STRING_TRANSLATE,
                          "Vector{delimiter=,}: 1b2d,12cc21,111,",
                          {"Vector{delimiter=,}: abcd,abccba,aaa,",
                           "Vector{delimiter=,}: ac,ab,a,x", "Scalar: 123"}},
        TestFunctionEntry{FuncKind::STRING_TRANSLATE,
                          "Vector{delimiter=,}: NULL,NULL,NULL,NULL",
                          {"Vector{delimiter=,}: abcd,abccba,aaa,",
                           "Vector{delimiter=,}: ac,ab,a,x", "Scalar: NULL"}},
        TestFunctionEntry{FuncKind::STRING_TRANSLATE,
                          "Vector{delimiter=,}: xb3d,b11b, ",
                          {"Vector{delimiter=,}: abcd,abccba, aaa",
                           "Scalar: ca", "Vector{delimiter=,}: 3x,1,"}},
        TestFunctionEntry{FuncKind::STRING_TRANSLATE,
                          "Vector{delimiter=,}: NULL,NULL,NULL",
                          {"Vector{delimiter=,}: abc,cdda,NULL", "Scalar: NULL",
                           "Vector{delimiter=,}: 3x,1,"}},
        TestFunctionEntry{FuncKind::STRING_TRANSLATE,
                          "Vector{delimiter=,}: 12cde,12cc21,1 2 c 21",
                          {"Vector{delimiter=,}: abcde,abccba,a b c ba",
                           "Scalar: ab", "Scalar: 12"}},
        TestFunctionEntry{FuncKind::STRING_TRANSLATE,
                          "Vector{delimiter=,}: NULL,NULL,NULL",
                          {"Vector{delimiter=,}: abcde,abccba,a b c ba",
                           "Scalar: NULL", "Scalar: 12"}},
        TestFunctionEntry{
            FuncKind::STRING_TRANSLATE,
            "Vector{delimiter=,}: 1b22b1,1cc1,1bccb1,bccb,abccba,NULL",
            {"Scalar: abccba", "Vector{delimiter=,}: ac,ba,ad,da,,NULL",
             "Vector{delimiter=,}: 123,1,12,2,1,y"}},
        TestFunctionEntry{
            FuncKind::STRING_TRANSLATE,
            "Vector{delimiter=,}: NULL,NULL,NULL,NULL,NULL,NULL",
            {"Scalar: NULL", "Vector{delimiter=,}: ac,ba,ad,da,,NULL",
             "Vector{delimiter=,}: 123,1,12,2,1,y"}},
        TestFunctionEntry{
            FuncKind::STRING_TRANSLATE,
            "Vector{delimiter=,}: a2112a,aa,NULL",
            {"Scalar: abccba", "Scalar: cb", "Vector{delimiter=,}: 123,,NULL"}},
        TestFunctionEntry{FuncKind::STRING_TRANSLATE,
                          "Vector{delimiter=,}: NULL,NULL,NULL,NULL,NULL,NULL",
                          {"Scalar: abc", "Scalar: NULL",
                           "Vector{delimiter=,}: 123,1,12,2,1,y"}},
        TestFunctionEntry{
            FuncKind::STRING_TRANSLATE,
            "Vector{delimiter=,}: a2112a,ab11ba,abccba,abccba,NULL",
            {"Scalar: abccba", "Vector{delimiter=,}: cb,c, ,,NULL",
             "Scalar: 123"}},
        TestFunctionEntry{
            FuncKind::STRING_TRANSLATE,
            "Vector{delimiter=,}: NULL,NULL,NULL,NULL,NULL,NULL",
            {"Scalar: NULL", "Vector{delimiter=,}: ac,ba,ad,da,,NULL",
             "Scalar: 123"}}));
}  // namespace dbcommon
