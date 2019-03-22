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

#include "dbcommon/testutil/function-utils.h"
#include "dbcommon/testutil/scalar-utils.h"
#include "dbcommon/testutil/vector-utils.h"

namespace dbcommon {

TEST(TestFunction, date_to_timestamp) {
  FunctionUtility fu(FuncKind::DATE_TO_TIMESTAMP);
  VectorUtility vuDate(TypeKind::DATEID);
  VectorUtility vuTimestemp(TypeKind::TIMESTAMPID);

  auto ret = Vector::BuildVector(TypeKind::TIMESTAMPID, true);
  auto date = vuDate.generateVector("1970-01-01 NULL NULL");
  auto timestamp = vuTimestemp.generateVector("1970-01-01 00:00:00 NULL NULL");
  std::vector<Datum> params{CreateDatum(ret.get()), CreateDatum(date.get())};

  fu.test(params.data(), params.size(), CreateDatum(timestamp.get()));
}

TEST(TestFunction, TestTimestampToDate) {
  FunctionUtility fu(FuncKind::TIMESTAMP_TO_DATE);
  VectorUtility vuTimestemp(TypeKind::TIMESTAMPID);
  VectorUtility vuDate(TypeKind::DATEID);

  auto ret = Vector::BuildVector(TypeKind::DATEID, true);
  auto timestamp = vuTimestemp.generateVector("1970-01-01 00:00:00 NULL NULL");
  auto date = vuDate.generateVector("1970-01-01 NULL NULL");
  std::vector<Datum> params{CreateDatum(ret.get()),
                            CreateDatum(timestamp.get())};

  fu.test(params.data(), params.size(), CreateDatum(date.get()));
}

TEST(TestFunction, TestIsTimestampFinite) {
  FunctionUtility fu(FuncKind::IS_TIMESTAMP_FINITE);
  VectorUtility vuTimestemp(TypeKind::TIMESTAMPID);

  SelectList ret;
  auto timestamp = vuTimestemp.generateVector("2019-01-01 00:00:00 NULL NULL");
  SelectList expect{0};
  std::vector<Datum> params{CreateDatum(&ret), CreateDatum(timestamp.get())};
  fu.test(params.data(), params.size(), CreateDatum(&expect));
}

TEST(TestFunction, TestDate_Part) {
  FunctionUtility fu(FuncKind::TIMESTAMP_DATE_PART);
  VectorUtility vuText(TypeKind::STRINGID);
  VectorUtility vuTimestamp(TypeKind::TIMESTAMPID);
  VectorUtility vuDate_part(TypeKind::DOUBLEID);
  ScalarUtility suTimestamp(TypeKind::TIMESTAMPID);
  ScalarUtility suString(TypeKind::STRINGID);

  auto ret = Vector::BuildVector(TypeKind::DOUBLEID, true);

  {
    auto text = vuText.generateVector(
        "century day decade dow doy epoch hour microseconds millennium "
        "milliseconds minute month quarter second week week year NULL");
    auto timestamp = vuTimestamp.generateVector(
        "2019-01-21 12:34:10.2 2019-01-21 12:34:10.2 2019-01-21 12:34:10.2 "
        "2019-01-21 12:34:10.2 2019-01-21 12:34:10.2 1970-02-01 12:34:10.2 "
        "2019-01-21 12:34:10.2 2019-01-21 12:34:10.2 2019-01-21 12:34:10.2 "
        "2019-01-21 12:34:10.2 2019-01-21 12:34:10.2 2019-01-21 12:34:10.2 "
        "2019-01-21 12:34:10.2 2019-01-21 12:34:10.2 2018-12-31 12:34:10.2 "
        "2016-01-01 12:34:10.2 2019-01-21 12:34:10.2 NULL");
    auto datep = vuDate_part.generateVector(
        "21 21 201 1 21 2694850.2 12 10200000 3 10200 34 1 1 10.2 1 53 2019 "
        "NULL");
    std::vector<Datum> params{CreateDatum(ret.get()), CreateDatum(text.get()),
                              CreateDatum(timestamp.get())};

    fu.test(params.data(), params.size(), CreateDatum(datep.get()));
  }

  {
    auto timestamp = vuTimestamp.generateVector(
        "2019-01-21 12:34:10.2 NULL 1995-02-13 12:34:13.34 1983-05-01 "
        "21:44:32.12");
    auto year = vuDate_part.generateVector("2019 NULL 1995 1983");
    auto month = vuDate_part.generateVector("1 NULL 2 5");
    auto day = vuDate_part.generateVector("21 NULL 13 1");
    auto century = vuDate_part.generateVector("21 NULL 20 20");
    auto decade = vuDate_part.generateVector("201 NULL 199 198");
    auto dow = vuDate_part.generateVector("1 NULL 1 0");
    auto doy = vuDate_part.generateVector("21 NULL 44 121");
    auto epoch = vuDate_part.generateVector(
        "1548045250.2 NULL 792650053.34 420644672.12");
    auto hour = vuDate_part.generateVector("12 NULL 12 21");
    auto microseconds =
        vuDate_part.generateVector("10200000 NULL 13340000 32120000");
    auto millennium = vuDate_part.generateVector("3 NULL 2 2");
    auto milliseconds = vuDate_part.generateVector("10200 NULL 13340 32120");
    auto minute = vuDate_part.generateVector("34 NULL 34 44");
    auto quarter = vuDate_part.generateVector("1 NULL 1 2");
    auto second = vuDate_part.generateVector("10.2 NULL 13.34 32.12");
    auto week = vuDate_part.generateVector("4 NULL 7 17");

    {
      auto text = suString.generateScalar("year");
      std::vector<Datum> params{CreateDatum(ret.get()), CreateDatum(&text),
                                CreateDatum(timestamp.get())};
      fu.test(params.data(), params.size(), CreateDatum(year.get()));
    }

    {
      auto text = suString.generateScalar("month");
      std::vector<Datum> params{CreateDatum(ret.get()), CreateDatum(&text),
                                CreateDatum(timestamp.get())};
      fu.test(params.data(), params.size(), CreateDatum(month.get()));
    }

    {
      auto text = suString.generateScalar("day");
      std::vector<Datum> params{CreateDatum(ret.get()), CreateDatum(&text),
                                CreateDatum(timestamp.get())};
      fu.test(params.data(), params.size(), CreateDatum(day.get()));
    }

    {
      auto text = suString.generateScalar("century");
      std::vector<Datum> params{CreateDatum(ret.get()), CreateDatum(&text),
                                CreateDatum(timestamp.get())};
      fu.test(params.data(), params.size(), CreateDatum(century.get()));
    }

    {
      auto text = suString.generateScalar("decade");
      std::vector<Datum> params{CreateDatum(ret.get()), CreateDatum(&text),
                                CreateDatum(timestamp.get())};
      fu.test(params.data(), params.size(), CreateDatum(decade.get()));
    }

    {
      auto text = suString.generateScalar("dow");
      std::vector<Datum> params{CreateDatum(ret.get()), CreateDatum(&text),
                                CreateDatum(timestamp.get())};
      fu.test(params.data(), params.size(), CreateDatum(dow.get()));
    }

    {
      auto text = suString.generateScalar("doy");
      std::vector<Datum> params{CreateDatum(ret.get()), CreateDatum(&text),
                                CreateDatum(timestamp.get())};
      fu.test(params.data(), params.size(), CreateDatum(doy.get()));
    }

    {
      auto text = suString.generateScalar("epoch");
      std::vector<Datum> params{CreateDatum(ret.get()), CreateDatum(&text),
                                CreateDatum(timestamp.get())};
      fu.test(params.data(), params.size(), CreateDatum(epoch.get()));
    }

    {
      auto text = suString.generateScalar("hour");
      std::vector<Datum> params{CreateDatum(ret.get()), CreateDatum(&text),
                                CreateDatum(timestamp.get())};
      fu.test(params.data(), params.size(), CreateDatum(hour.get()));
    }

    {
      auto text = suString.generateScalar("microseconds");
      std::vector<Datum> params{CreateDatum(ret.get()), CreateDatum(&text),
                                CreateDatum(timestamp.get())};
      fu.test(params.data(), params.size(), CreateDatum(microseconds.get()));
    }

    {
      auto text = suString.generateScalar("millennium");
      std::vector<Datum> params{CreateDatum(ret.get()), CreateDatum(&text),
                                CreateDatum(timestamp.get())};
      fu.test(params.data(), params.size(), CreateDatum(millennium.get()));
    }

    {
      auto text = suString.generateScalar("milliseconds");
      std::vector<Datum> params{CreateDatum(ret.get()), CreateDatum(&text),
                                CreateDatum(timestamp.get())};
      fu.test(params.data(), params.size(), CreateDatum(milliseconds.get()));
    }

    {
      auto text = suString.generateScalar("minute");
      std::vector<Datum> params{CreateDatum(ret.get()), CreateDatum(&text),
                                CreateDatum(timestamp.get())};
      fu.test(params.data(), params.size(), CreateDatum(minute.get()));
    }

    {
      auto text = suString.generateScalar("quarter");
      std::vector<Datum> params{CreateDatum(ret.get()), CreateDatum(&text),
                                CreateDatum(timestamp.get())};
      fu.test(params.data(), params.size(), CreateDatum(quarter.get()));
    }

    {
      auto text = suString.generateScalar("second");
      std::vector<Datum> params{CreateDatum(ret.get()), CreateDatum(&text),
                                CreateDatum(timestamp.get())};
      fu.test(params.data(), params.size(), CreateDatum(second.get()));
    }

    {
      auto text = suString.generateScalar("week");
      std::vector<Datum> params{CreateDatum(ret.get()), CreateDatum(&text),
                                CreateDatum(timestamp.get())};
      fu.test(params.data(), params.size(), CreateDatum(week.get()));
    }
  }

  {
    auto text = vuText.generateVector(
        "century day decade dow doy epoch hour microseconds millennium "
        "milliseconds minute month quarter second week year NULL");
    auto timestamp = suTimestamp.generateScalar("2019-01-21 12:34:10.2");
    auto datep = vuDate_part.generateVector(
        "21 21 201 1 21 1548045250.2 12 10200000 3 10200 34 1 1 10.2 4 2019 "
        "NULL");
    std::vector<Datum> params{CreateDatum(ret.get()), CreateDatum(text.get()),
                              CreateDatum(&timestamp)};
    fu.test(params.data(), params.size(), CreateDatum(datep.get()));
  }

  {
    auto text = vuText.generateVector("century day");
    auto timestamp = suTimestamp.generateScalar("NULL");
    auto datep = vuDate_part.generateVector("NULL NULL");
    std::vector<Datum> params{CreateDatum(ret.get()), CreateDatum(text.get()),
                              CreateDatum(&timestamp)};
    fu.test(params.data(), params.size(), CreateDatum(datep.get()));
  }
}
TEST(TestFunction, TestDate_Trunc) {
  FunctionUtility fu(FuncKind::TIMESTAMP_DATE_TRUNC);
  VectorUtility vuText(TypeKind::STRINGID);
  VectorUtility vuTimestamp(TypeKind::TIMESTAMPID);
  VectorUtility vuDate_trunc(TypeKind::TIMESTAMPID);
  ScalarUtility suTimestamp(TypeKind::TIMESTAMPID);
  ScalarUtility suString(TypeKind::STRINGID);

  auto ret = Vector::BuildVector(TypeKind::TIMESTAMPID, true);
  {
    auto text = vuText.generateVector(
        "millennium century decade year quarter month week day hour minute "
        "second "
        "milliseconds microseconds NULL");
    auto timestamp = vuTimestamp.generateVector(
        "2019-02-21 12:34:10 1970-01-21 12:34:10 2019-01-21 12:34:10 "
        "2019-01-21 12:34:10 2019-02-21 12:34:10 2019-02-21 12:34:10 "
        "2019-01-22 12:34:10 2019-01-21 12:34:10 2019-01-21 12:34:10 "
        "2019-01-21 12:34:10 2019-01-21 12:34:10 2019-01-21 12:34:10.00001 "
        "2019-01-21 12:34:10.00001 NULL");
    auto datep = vuDate_trunc.generateVector(
        "2001-01-01 00:00:00 1901-01-01 00:00:00 2010-01-01 00:00:00 "
        "2019-01-01 00:00:00 2019-01-01 00:00:00 2019-02-01 00:00:00 "
        "2019-01-21 00:00:00 2019-01-21 00:00:00 2019-01-21 12:00:00 "
        "2019-01-21 12:34:00 2019-01-21 12:34:10 2019-01-21 12:34:10 "
        "2019-01-21 12:34:10.00001 NULL");
    std::vector<Datum> params{CreateDatum(ret.get()), CreateDatum(text.get()),
                              CreateDatum(timestamp.get())};

    fu.test(params.data(), params.size(), CreateDatum(datep.get()));
  }

  {
    auto timestamp = vuTimestamp.generateVector(
        "2019-01-21 12:34:10.0001 1995-02-13 12:34:13 1983-05-01 21:44:32");
    auto millennium = vuDate_trunc.generateVector(
        "2001-01-01 00:00:00 1001-01-01 00:00:00 1001-01-01 00:00:00");
    auto century = vuDate_trunc.generateVector(
        "2001-01-01 00:00:00 1901-01-01 00:00:00 1901-01-01 00:00:00");
    auto decade = vuDate_trunc.generateVector(
        "2010-01-01 00:00:00 1990-01-01 00:00:00 1980-01-01 00:00:00");
    auto year = vuDate_trunc.generateVector(
        "2019-01-01 00:00:00 1995-01-01 00:00:00 1983-01-01 00:00:00");
    auto quarter = vuDate_trunc.generateVector(
        "2019-01-01 00:00:00 1995-01-01 00:00:00 1983-04-01 00:00:00");
    auto month = vuDate_trunc.generateVector(
        "2019-01-01 00:00:00 1995-02-01 00:00:00 1983-05-01 00:00:00");
    auto week = vuDate_trunc.generateVector(
        "2019-01-21 00:00:00 1995-02-13 00:00:00 1983-04-25 00:00:00");
    auto day = vuDate_trunc.generateVector(
        "2019-01-21 00:00:00 1995-02-13 00:00:00 1983-05-01 00:00:00");
    auto hour = vuDate_trunc.generateVector(
        "2019-01-21 12:00:00 1995-02-13 12:00:00 1983-05-01 21:00:00");
    auto minute = vuDate_trunc.generateVector(
        "2019-01-21 12:34:00 1995-02-13 12:34:00 1983-05-01 21:44:00");
    auto second = vuDate_trunc.generateVector(
        "2019-01-21 12:34:10 1995-02-13 12:34:13 1983-05-01 21:44:32");
    auto milliseconds = vuDate_trunc.generateVector(
        "2019-01-21 12:34:10 1995-02-13 12:34:13 1983-05-01 21:44:32");
    auto microseconds = vuDate_trunc.generateVector(
        "2019-01-21 12:34:10.0001 1995-02-13 12:34:13 1983-05-01 21:44:32");

    {
      auto text = suString.generateScalar("millennium");
      std::vector<Datum> params{CreateDatum(ret.get()), CreateDatum(&text),
                                CreateDatum(timestamp.get())};
      fu.test(params.data(), params.size(), CreateDatum(millennium.get()));
    }

    {
      auto text = suString.generateScalar("century");
      std::vector<Datum> params{CreateDatum(ret.get()), CreateDatum(&text),
                                CreateDatum(timestamp.get())};
      fu.test(params.data(), params.size(), CreateDatum(century.get()));
    }

    {
      auto text = suString.generateScalar("decade");
      std::vector<Datum> params{CreateDatum(ret.get()), CreateDatum(&text),
                                CreateDatum(timestamp.get())};
      fu.test(params.data(), params.size(), CreateDatum(decade.get()));
    }

    {
      auto text = suString.generateScalar("year");
      std::vector<Datum> params{CreateDatum(ret.get()), CreateDatum(&text),
                                CreateDatum(timestamp.get())};
      fu.test(params.data(), params.size(), CreateDatum(year.get()));
    }

    {
      auto text = suString.generateScalar("quarter");
      std::vector<Datum> params{CreateDatum(ret.get()), CreateDatum(&text),
                                CreateDatum(timestamp.get())};
      fu.test(params.data(), params.size(), CreateDatum(quarter.get()));
    }

    {
      auto text = suString.generateScalar("month");
      std::vector<Datum> params{CreateDatum(ret.get()), CreateDatum(&text),
                                CreateDatum(timestamp.get())};
      fu.test(params.data(), params.size(), CreateDatum(month.get()));
    }

    {
      auto text = suString.generateScalar("week");
      std::vector<Datum> params{CreateDatum(ret.get()), CreateDatum(&text),
                                CreateDatum(timestamp.get())};
      fu.test(params.data(), params.size(), CreateDatum(week.get()));
    }

    {
      auto text = suString.generateScalar("day");
      std::vector<Datum> params{CreateDatum(ret.get()), CreateDatum(&text),
                                CreateDatum(timestamp.get())};
      fu.test(params.data(), params.size(), CreateDatum(day.get()));
    }

    {
      auto text = suString.generateScalar("hour");
      std::vector<Datum> params{CreateDatum(ret.get()), CreateDatum(&text),
                                CreateDatum(timestamp.get())};
      fu.test(params.data(), params.size(), CreateDatum(hour.get()));
    }

    {
      auto text = suString.generateScalar("minute");
      std::vector<Datum> params{CreateDatum(ret.get()), CreateDatum(&text),
                                CreateDatum(timestamp.get())};
      fu.test(params.data(), params.size(), CreateDatum(minute.get()));
    }

    {
      auto text = suString.generateScalar("second");
      std::vector<Datum> params{CreateDatum(ret.get()), CreateDatum(&text),
                                CreateDatum(timestamp.get())};
      fu.test(params.data(), params.size(), CreateDatum(second.get()));
    }

    {
      auto text = suString.generateScalar("milliseconds");
      std::vector<Datum> params{CreateDatum(ret.get()), CreateDatum(&text),
                                CreateDatum(timestamp.get())};
      fu.test(params.data(), params.size(), CreateDatum(milliseconds.get()));
    }

    {
      auto text = suString.generateScalar("microseconds");
      std::vector<Datum> params{CreateDatum(ret.get()), CreateDatum(&text),
                                CreateDatum(timestamp.get())};
      fu.test(params.data(), params.size(), CreateDatum(microseconds.get()));
    }
  }
  {
    auto text = vuText.generateVector(
        "millennium century decade year quarter month week day hour minute "
        "second milliseconds microseconds NULL");
    auto timestamp = suTimestamp.generateScalar("2019-01-21 12:34:10.0002");
    auto datep = vuDate_trunc.generateVector(
        "2001-01-01 00:00:00 2001-01-01 00:00:00 2010-01-01 00:00:00 "
        "2019-01-01 00:00:00 2019-01-01 00:00:00 2019-01-01 00:00:00 "
        "2019-01-21 00:00:00 2019-01-21 00:00:00 2019-01-21 12:00:00 "
        "2019-01-21 12:34:00 2019-01-21 12:34:10 2019-01-21 12:34:10 "
        "2019-01-21 12:34:10.0002 NULL");
    std::vector<Datum> params{CreateDatum(ret.get()), CreateDatum(text.get()),
                              CreateDatum(&timestamp)};
    fu.test(params.data(), params.size(), CreateDatum(datep.get()));
  }
  {
    auto text = vuText.generateVector("century day");
    auto timestamp = suTimestamp.generateScalar("NULL");
    auto datep = vuDate_trunc.generateVector("NULL NULL");
    std::vector<Datum> params{CreateDatum(ret.get()), CreateDatum(text.get()),
                              CreateDatum(&timestamp)};
    fu.test(params.data(), params.size(), CreateDatum(datep.get()));
  }
}

TEST(TestFunction, TestTimestampToText) {
  FunctionUtility fu(FuncKind::TIMESTAMP_TO_TEXT);
  VectorUtility vuTimestemp(TypeKind::TIMESTAMPID);
  VectorUtility vuText(TypeKind::STRINGID);

  auto ret = Vector::BuildVector(TypeKind::STRINGID, true);
  auto timestamp = vuTimestemp.generateVector(
      "1270-01-01 13:40:25.1234 0001-01-01 13:40:25.0023 NULL");
  auto text = vuText.generateVector(
      "1270-01-01 13:40:25.1234,0001-01-01 13:40:25.0023,NULL", ',');
  std::vector<Datum> params{CreateDatum(ret.get()),
                            CreateDatum(timestamp.get())};

  fu.test(params.data(), params.size(), CreateDatum(text.get()));
}
}  // namespace dbcommon
