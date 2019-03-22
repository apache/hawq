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

#include "dbcommon/utils/mb/mb-converter.h"

using namespace dbcommon;  // NOLINT

TEST(TestMbConverter, Basic) {
  MbConverter converter1 =
      MbConverter(MbConverter::checkSupportedEncoding("UTF-8"),
                  MbConverter::checkSupportedEncoding("GBK"));
  MbConverter converter2 =
      MbConverter(MbConverter::checkSupportedEncoding("GBK"),
                  MbConverter::checkSupportedEncoding("UTF-8"));
  std::string utf8Str = u8"这是中文";
  std::string gbkStr = "\xd5\xe2\xca\xc7\xd6\xd0\xce\xc4";
  std::string res;
  // converter.printInHex(utf8_str.c_str());
  res = converter1.convert(utf8Str);
  EXPECT_EQ(gbkStr, res);
  res = converter2.convert(gbkStr);
  EXPECT_EQ(utf8Str, res);
  res = converter2.convert(gbkStr.c_str());
  EXPECT_EQ(utf8Str, res);
}

TEST(TestMbConverter, Canonical) {
  EXPECT_EQ("UTF-8", MbConverter::canonicalizeEncodingName("utf8"));
  EXPECT_EQ("UTF-8", MbConverter::canonicalizeEncodingName("UTF-8"));
  EXPECT_EQ("UTF-8", MbConverter::canonicalizeEncodingName("UTF8"));
}
