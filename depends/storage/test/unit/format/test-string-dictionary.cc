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
#include "storage/format/orc/string-dictionary.h"

namespace orc {

class TestStringDictionary : public ::testing::Test {
 public:
  TestStringDictionary() {}
  virtual ~TestStringDictionary() {}

  std::string convertToString(const char *val, uint64_t len) {
    std::string ret;
    ret.append(val, len);
    return ret;
  }
};

TEST_F(TestStringDictionary, FullTest) {
  StringDictionary dict;
  std::string s1 = "a2";
  std::string s2 = "a1";
  std::string s3 = "b1";
  std::string s4 = "b2";

  // test add
  EXPECT_EQ(dict.add(s1.data(), s1.size()), 0);
  EXPECT_EQ(dict.add(s2.data(), s1.size()), 1);
  EXPECT_EQ(dict.add(s3.data(), s1.size()), 2);
  EXPECT_EQ(dict.add(s4.data(), s1.size()), 3);
  EXPECT_EQ(dict.add(s1.data(), s1.size()), 0);
  EXPECT_EQ(dict.add(s2.data(), s1.size()), 1);
  EXPECT_EQ(dict.add(s3.data(), s1.size()), 2);
  EXPECT_EQ(dict.add(s4.data(), s1.size()), 3);

  // test dump
  std::vector<const char *> vals;
  std::vector<uint64_t> lens;
  std::vector<uint32_t> dumpOrder;
  dict.dump(&vals, &lens, &dumpOrder);
  EXPECT_STREQ(convertToString(vals[0], lens[0]).c_str(), "a1");
  EXPECT_STREQ(convertToString(vals[1], lens[1]).c_str(), "a2");
  EXPECT_STREQ(convertToString(vals[2], lens[2]).c_str(), "b1");
  EXPECT_STREQ(convertToString(vals[3], lens[3]).c_str(), "b2");
  EXPECT_EQ(dumpOrder[0], 1);
  EXPECT_EQ(dumpOrder[1], 0);
  EXPECT_EQ(dumpOrder[2], 2);
  EXPECT_EQ(dumpOrder[3], 3);

  // test size
  EXPECT_EQ(dict.size(), 4);
  dict.clear();
  EXPECT_EQ(dict.size(), 0);
}

}  // namespace orc
