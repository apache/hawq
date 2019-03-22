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

#ifndef DBCOMMON_SRC_DBCOMMON_FUNCTION_DATE_FUNCTION_H_
#define DBCOMMON_SRC_DBCOMMON_FUNCTION_DATE_FUNCTION_H_

#include <string>
#include <unordered_map>
#include "dbcommon/nodes/datum.h"

namespace dbcommon {

static const int32_t day_tab[2][13] = {
    {31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31, 0},
    {31, 29, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31, 0}};

enum DatetimeKind {
  EPOCH,
  MILLENNIUM,
  CENTURY,
  DECADE,
  YEAR,
  QUARTER,
  MONTH,
  WEEK,
  DAY,
  DOW,
  DOY,
  HOUR,
  MINUTE,
  SECOND,
  MILLISEC,
  MICROSEC,
  UNDEFINED
};

typedef std::unordered_map<std::string, DatetimeKind> DatetimeAliasMap;

class DatetimeTable {
 private:
  static const DatetimeAliasMap mapTable;
  static const DatetimeAliasMap createDatetimeMap();

 public:
  static DatetimeKind getDatetimeKindByName(const std::string &kindName) {
    const DatetimeAliasMap::const_iterator iter = mapTable.find(kindName);

    if (iter != mapTable.end()) {
      return iter->second;
    }
    return UNDEFINED;
  }
};

static inline bool isLeapYear(int32_t y) {
  return ((y) % 4) == 0 && (((y) % 100) != 0 || ((y) % 400) == 0);
}

Datum date_to_timestamp(Datum *params, uint64_t size);

Datum timestamp_to_date(Datum *params, uint64_t size);

Datum is_timestamp_finite(Datum *params, uint64_t size);

Datum timestamp_date_part(Datum *params, uint64_t size);

Datum timestamp_date_trunc(Datum *params, uint64_t size);

Datum timestamp_to_text(Datum *params, uint64_t size);

Datum time_sub_time(Datum *params, uint64_t size);

Datum timestamp_sub_timestamp(Datum *params, uint64_t size);

Datum date_add_interval(Datum *params, uint64_t size);

Datum date_sub_interval(Datum *params, uint64_t size);

Datum time_add_interval(Datum *params, uint64_t size);

Datum time_sub_interval(Datum *params, uint64_t size);

Datum timestamp_add_interval(Datum *params, uint64_t size);

Datum timestamp_sub_interval(Datum *params, uint64_t size);
}  // namespace dbcommon

#endif  // DBCOMMON_SRC_DBCOMMON_FUNCTION_DATE_FUNCTION_H_
