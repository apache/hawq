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

#include "dbcommon/nodes/datum.h"
#include "dbcommon/type/type-util.h"

using namespace dbcommon;  // NOLINT

template <typename T>
static void TestSetGetInteger() {
  {
    Datum value(CreateDatum(static_cast<T>(0)));
    EXPECT_EQ(0, DatumGetValue<T>(value));
    EXPECT_EQ("0", DatumToString(value, TypeMapping<T>::type));
  }

  {
    Datum value(CreateDatum(static_cast<T>(1)));
    EXPECT_EQ(1, DatumGetValue<T>(value));
    EXPECT_EQ("1", DatumToString(value, TypeMapping<T>::type));
  }

  {
    Datum value(CreateDatum(static_cast<T>(-1)));
    EXPECT_EQ(-1, DatumGetValue<T>(value));
    EXPECT_EQ("-1", DatumToString(value, TypeMapping<T>::type));
  }

  {
    Datum value(CreateDatum(static_cast<T>(std::numeric_limits<T>::max())));
    EXPECT_EQ(std::numeric_limits<T>::max(), DatumGetValue<T>(value));
    EXPECT_EQ(std::to_string(std::numeric_limits<T>::max()),
              DatumToString(value, TypeMapping<T>::type));
  }

  {
    Datum value(CreateDatum(static_cast<T>(std::numeric_limits<T>::min())));
    EXPECT_EQ(std::numeric_limits<T>::min(), DatumGetValue<T>(value));
    EXPECT_EQ(std::to_string(std::numeric_limits<T>::min()),
              DatumToString(value, TypeMapping<T>::type));
  }
}

TEST(TestDatum, TestSetGetInteger) {
  TestSetGetInteger<int8_t>();
  TestSetGetInteger<int16_t>();
  TestSetGetInteger<int32_t>();
  TestSetGetInteger<int64_t>();
}

template <typename T>
static void TestSetGetFloat() {
  {
    Datum value(CreateDatum(static_cast<T>(0)));
    EXPECT_EQ(0, DatumGetValue<T>(value));
    EXPECT_EQ("0.000000", DatumToString(value, TypeMapping<T>::type));
  }

  {
    Datum value(CreateDatum(static_cast<T>(1)));
    EXPECT_EQ(1, DatumGetValue<T>(value));
    EXPECT_EQ("1.000000", DatumToString(value, TypeMapping<T>::type));
  }

  {
    Datum value(CreateDatum(static_cast<T>(-1)));
    EXPECT_EQ(-1, DatumGetValue<T>(value));
    EXPECT_EQ("-1.000000", DatumToString(value, TypeMapping<T>::type));
  }

  {
    Datum value(CreateDatum(static_cast<T>(0.1)));
    EXPECT_EQ(static_cast<T>(0.1), DatumGetValue<T>(value));
    EXPECT_EQ("0.100000", DatumToString(value, TypeMapping<T>::type));
  }

  {
    Datum value(CreateDatum(static_cast<T>(1.1)));
    EXPECT_EQ(static_cast<T>(1.1), DatumGetValue<T>(value));
  }

  {
    Datum value(CreateDatum(static_cast<T>(-1.1)));
    EXPECT_EQ(static_cast<T>(-1.1), DatumGetValue<T>(value));
  }

  {
    Datum value(CreateDatum(std::numeric_limits<T>::max()));
    EXPECT_EQ(std::numeric_limits<T>::max(), DatumGetValue<T>(value));
  }

  {
    Datum value(CreateDatum(std::numeric_limits<T>::min()));
    EXPECT_EQ(std::numeric_limits<T>::min(), DatumGetValue<T>(value));
  }
}

TEST(TestDatum, TestSetGetFloat) {
  TestSetGetFloat<float>();
  TestSetGetFloat<double>();
}

TEST(TestDatum, TestSetGetBool) {
  {
    Datum value(CreateDatum(true));
    EXPECT_EQ(true, DatumGetValue<bool>(value));
    EXPECT_EQ("t", DatumToString(value, BOOLEANID));
  }

  {
    Datum value(CreateDatum(false));
    EXPECT_EQ(false, DatumGetValue<bool>(value));
    EXPECT_EQ("f", DatumToString(value, BOOLEANID));
  }
}

TEST(TestDatum, TestSetGetDate) {
  {
    Datum value(CreateDatum(17533));
    EXPECT_EQ(17533, DatumGetValue<int32_t>(value));
    EXPECT_EQ("2018-01-02", DatumToString(value, DATEID));
  }

  {
    Datum value(CreateDatum(-1456221));
    EXPECT_EQ(-1456221, DatumGetValue<int32_t>(value));
    EXPECT_EQ("2018-01-02 BC", DatumToString(value, DATEID));
  }
}

TEST(TestDatum, TestSetGetTime) {
  {
    Datum value(CreateDatum(62178000000));
    EXPECT_EQ(62178000000, DatumGetValue<int64_t>(value));
    EXPECT_EQ("17:16:18", DatumToString(value, TIMEID));
  }

  {
    Datum value(CreateDatum(62178123456));
    EXPECT_EQ(62178123456, DatumGetValue<int64_t>(value));
    EXPECT_EQ("17:16:18.123456", DatumToString(value, TIMEID));
  }
}

TEST(TestDatum, TestSetGetTimestamp) {
  {
    Timestamp ts;
    int64_t second = 17533 * 60 * 60 * 24 + 62178;
    int64_t nanosecond = 0;
    Datum value(CreateDatum("2018-01-02 17:16:18", &ts, TIMESTAMPID));
    Timestamp *tsnew = DatumGetValue<Timestamp *>(value);
    EXPECT_EQ(1514913378, tsnew->second);
    EXPECT_EQ(0, tsnew->nanosecond);
    EXPECT_EQ("2018-01-02 17:16:18", DatumToString(value, TIMESTAMPID));
  }

  {
    Timestamp ts;
    int64_t second = 17533 * 60 * 60 * 24 + 62178;
    int64_t nanosecond = 123456789;
    Datum value(
        CreateDatum("2018-01-02 17:16:18.123456789 BC", &ts, TIMESTAMPID));
    Timestamp *tsnew = DatumGetValue<Timestamp *>(value);
    EXPECT_EQ(-125817432222, tsnew->second);
    EXPECT_EQ(123456789, tsnew->nanosecond);
    EXPECT_EQ("2018-01-02 17:16:18.123456789 BC",
              DatumToString(value, TIMESTAMPID));
  }
}

TEST(TestDatum, TestSetGetPointer) {
  {
    void *val = &val;
    Datum value(CreateDatum(val));
    EXPECT_EQ(&val, DatumGetValue<void *>(value));
  }
}
