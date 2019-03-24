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

#include "dbcommon/utils/macro.h"

#include "storage/common/bloom-filter.h"

namespace storage {

TEST(TestBloomFilter, TestInteger) {
  BloomFilter bloomFilter(6);
  bloomFilter.addInt(1);
  bloomFilter.addInt(9);
  bloomFilter.addInt(3);
  bloomFilter.addInt(9);
  bloomFilter.addInt(-2);
  bloomFilter.addInt(2);
  bloomFilter.addInt(0);

  EXPECT_EQ(bloomFilter.testInt(-2), true);
  EXPECT_EQ(bloomFilter.testInt(0), true);
  EXPECT_EQ(bloomFilter.testInt(1), true);
  EXPECT_EQ(bloomFilter.testInt(2), true);
  EXPECT_EQ(bloomFilter.testInt(3), true);
  EXPECT_EQ(bloomFilter.testInt(9), true);
}

TEST(TestBloomFilter, TestLotsOfInteger) {
  int num = DEFAULT_NUMBER_TUPLES_PER_BATCH * 5;
  BloomFilter bloomFilter(num);
  for (int i = 0; i < num; ++i) bloomFilter.addInt(i);

  EXPECT_EQ(bloomFilter.testInt(10239), true);
  EXPECT_EQ(bloomFilter.testInt(0), true);
  EXPECT_EQ(bloomFilter.testInt(10240), false);
  EXPECT_EQ(bloomFilter.testInt(-1), false);
}

TEST(TestBloomFilter, TestDouble) {
  BloomFilter bloomFilter(6);
  bloomFilter.addDouble(0);
  bloomFilter.addDouble(-1);
  bloomFilter.addDouble(1.2);
  bloomFilter.addDouble(2.3);
  bloomFilter.addDouble(-2.5);
  bloomFilter.addDouble(0);
  bloomFilter.addDouble(100.446);

  EXPECT_EQ(bloomFilter.testDouble(-2.5), true);
  EXPECT_EQ(bloomFilter.testDouble(-1), true);
  EXPECT_EQ(bloomFilter.testDouble(0), true);
  EXPECT_EQ(bloomFilter.testDouble(1.2), true);
  EXPECT_EQ(bloomFilter.testDouble(2.3), true);
  EXPECT_EQ(bloomFilter.testDouble(100.446), true);
}

TEST(TestBloomFilter, TestString) {
  BloomFilter bloomFilter(6);
  bloomFilter.addString("abc", 3);
  bloomFilter.addString("cd", 2);
  bloomFilter.addString("oushu", 5);
  bloomFilter.addString("a", 1);
  bloomFilter.addString("mn", 2);
  bloomFilter.addString("ab", 2);
  bloomFilter.addString("cd", 2);

  EXPECT_EQ(bloomFilter.testString("abc", 3), true);
  EXPECT_EQ(bloomFilter.testString("cd", 2), true);
  EXPECT_EQ(bloomFilter.testString("oushu", 5), true);
  EXPECT_EQ(bloomFilter.testString("a", 1), true);
  EXPECT_EQ(bloomFilter.testString("mn", 2), true);
  EXPECT_EQ(bloomFilter.testString("ab", 2), true);
}

TEST(TestBloomFilter, TestLongString) {
  BloomFilter bloomFilter(4);
  bloomFilter.addString("oushuoushu***oushuoushu", 23);
  bloomFilter.addString("oushuoushu1***oushuoushu1", 25);
  bloomFilter.addString("oushuoushu2***oushuoushu2", 25);
  bloomFilter.addString("oushu", 5);

  EXPECT_EQ(bloomFilter.testString("oushu", 5), true);
  EXPECT_EQ(bloomFilter.testString("oushuoushu1***oushuoushu1", 25), true);
  EXPECT_EQ(bloomFilter.testString("oushuoushu2***oushuoushu2", 25), true);
  EXPECT_EQ(bloomFilter.testString("oushuoushu***oushuoushu", 23), true);
}

TEST(TestBloomFilter, TestDeserialize) {
  BloomFilter bloomFilter(2);
  bloomFilter.addString("Oushu", 5);
  bloomFilter.addString("Inc.", 4);

  BloomFilter::uptr deserBloomFilter(
      new BloomFilter(bloomFilter.getBitSet(), bloomFilter.size(),
                      bloomFilter.getNumHashFunctions()));
  EXPECT_EQ(deserBloomFilter->testString("Oushu", 5), true);
  EXPECT_EQ(deserBloomFilter->testString("Inc.", 4), true);
}

}  // namespace storage
