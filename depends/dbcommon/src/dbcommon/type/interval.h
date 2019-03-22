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

#ifndef DBCOMMON_SRC_DBCOMMON_TYPE_INTERVAL_H_
#define DBCOMMON_SRC_DBCOMMON_TYPE_INTERVAL_H_

#include <algorithm>
#include <memory>
#include <stdexcept>
#include <string>

#include "dbcommon/type/date.h"
#include "dbcommon/type/typebase.h"

namespace dbcommon {

enum IntervalStyle {
  INTSTYLE_POSTGRES,
  INTSTYLE_POSTGRES_VERBOSE,
  INTSTYLE_SQL_STANDARD,
  INTSTYLE_ISO_8601
};

struct IntervalVar {
  int64_t timeOffset;
  int32_t day;
  int32_t month;

  IntervalVar() : timeOffset(0), day(0), month(0) {}
  IntervalVar(int64_t time, int32_t day, int32_t month)
      : timeOffset(time), day(day), month(month) {}

  IntervalVar(const IntervalVar& itv)
      : timeOffset(itv.timeOffset), day(itv.day), month(itv.month) {}

  IntervalVar operator+=(const IntervalVar& r) {
    this->timeOffset += r.timeOffset;
    this->day += r.day;
    this->month += r.month;
    return *this;
  }

  IntervalVar operator-=(const IntervalVar& r) {
    this->timeOffset -= r.timeOffset;
    this->day -= r.day;
    this->month -= r.month;
    return *this;
  }

  IntervalVar operator+(const IntervalVar& r) const {
    IntervalVar ret(*this);
    ret += r;
    return ret;
  }

  IntervalVar operator-(const IntervalVar& r) const {
    IntervalVar ret(*this);
    ret -= r;
    return ret;
  }

  bool operator<(const IntervalVar& r) const { return intervalCmp(r) < 0; }

  bool operator<=(const IntervalVar& r) const { return intervalCmp(r) <= 0; }

  bool operator>(const IntervalVar& r) const { return intervalCmp(r) > 0; }

  bool operator>=(const IntervalVar& r) const { return intervalCmp(r) >= 0; }

  bool operator==(const IntervalVar& r) const { return intervalCmp(r) == 0; }

  bool operator!=(const IntervalVar& r) const { return intervalCmp(r) != 0; }

 private:
  int intervalCmp(const IntervalVar& r) const {
    int64_t timeOffset1 = timeOffset;
    int64_t timeOffset2 = r.timeOffset;

    timeOffset1 += month * INTERVAL_DAYS_PER_MONTH * USECS_PER_DAY;
    timeOffset1 += day * HOURS_PER_DAY * USECS_PER_HOUR;

    timeOffset2 += r.month * INTERVAL_DAYS_PER_MONTH * USECS_PER_DAY;
    timeOffset2 += r.day * HOURS_PER_DAY * USECS_PER_HOUR;

    return ((timeOffset1 < timeOffset2) ? -1
                                        : (timeOffset1 > timeOffset2) ? 1 : 0);
  }
};

class IntervalType : public FixedSizeTypeBase {
 public:
  IntervalType() { typeKind = INTERVALID; }

  virtual ~IntervalType() {}

  bool isFixedSizeType() const override { return true; }

  uint64_t getTypeWidth() const override {
    return sizeof(int64_t) + sizeof(int32_t) + sizeof(int32_t);
  }

  Datum getDatum(const char* str) const override { return Datum(0); }

  Datum getDatum(const char* str, IntervalVar* interval) {
    *interval = fromString(str);
    auto ret = CreateDatum(interval);
    return ret;
  }

  std::string DatumToString(const Datum& d) const override {
    IntervalVar* v = DatumGetValue<IntervalVar*>(d);
    int64_t tmOffset = v->timeOffset;
    int32_t day = v->day;
    int32_t month = v->month;
    return toString(tmOffset, day, month);
  }

  std::string DatumToBinary(const Datum& d) const override {
    IntervalVar* v = DatumGetValue<IntervalVar*>(d);
    return std::string(reinterpret_cast<char*>(v), sizeof(IntervalVar));
  }

  static inline std::string isoIntervalToString(int32_t year, int32_t month,
                                                int32_t day, int32_t hour,
                                                int32_t minute, int32_t second,
                                                int32_t msec) {
    if (year == 0 && month == 0 && day && hour && minute && second && msec)
      return "PT0S";
    std::string ret = "P";
    if (year != 0) ret += std::to_string(year) + "Y";
    if (month != 0) ret += std::to_string(month) + "M";
    if (day != 0) ret += std::to_string(day) + "D";
    if (hour != 0 || minute != 0 || second != 0 || msec != 0) ret += "T";
    if (hour != 0) ret += std::to_string(hour) + "H";
    if (minute != 0) ret += std::to_string(minute) + "M";
    if (second != 0 || msec != 0) {
      if (second < 0 || msec < 0) ret += "-";
      ret += std::to_string(second) + "." + std::to_string(msec) + "S";
    }
    return ret;
  }

  static inline std::string postgresIntervalToString(
      int32_t year, int32_t month, int32_t day, int32_t hour, int32_t minute,
      int32_t second, int32_t msec) {
    bool isBeforeNeg = false;
    bool isZero = true;
    std::string ret = "";
    ret += addPostgresIntervalPart(year, "year", &isZero, &isBeforeNeg);
    ret += addPostgresIntervalPart(month, "month", &isZero, &isBeforeNeg);
    ret += addPostgresIntervalPart(day, "day", &isZero, &isBeforeNeg);

    if (isZero || hour != 0 || minute != 0 || second != 0 || msec != 0) {
      if (!isZero) ret += " ";
      if (hour < 0 || minute < 0 || second < 0 || msec < 0)
        ret += "-";
      else
        ret += (isBeforeNeg ? "+" : "");
      ret += TimeType::timeToString(std::abs(hour), std::abs(minute),
                                    std::abs(second), std::abs(msec));
    }

    return ret;
  }

  static inline std::string toString(
      int64_t timeOffset, int32_t day, int32_t month,
      IntervalStyle intStyle = INTSTYLE_POSTGRES) {
    int32_t iyear, imonth, iday;
    iyear = month / MONTHS_PER_YEAR;
    imonth = month % MONTHS_PER_YEAR;
    iday = day;

    int32_t ihour, iminute, isecond, imsecond;
    ihour = timeOffset / USECS_PER_HOUR;
    timeOffset -= ihour * USECS_PER_HOUR;
    iminute = timeOffset / USECS_PER_MINUTE;
    timeOffset -= iminute * USECS_PER_MINUTE;
    isecond = timeOffset / USECS_PER_SEC;
    imsecond = timeOffset - (isecond * USECS_PER_SEC);

    switch (intStyle) {
      case INTSTYLE_POSTGRES:
        return postgresIntervalToString(iyear, imonth, iday, ihour, iminute,
                                        isecond, imsecond);
      case INTSTYLE_SQL_STANDARD:
        return "";
      case INTSTYLE_ISO_8601:
        return isoIntervalToString(iyear, imonth, iday, ihour, iminute, isecond,
                                   imsecond);
      case INTSTYLE_POSTGRES_VERBOSE:
      default:
        return "";
    }
  }

  //  Parse the formatted interval string into an IntervalVar.
  //  Note: The input parameter to be formatted to month-day-timeoffset
  //  style. This function is particularly used to parse interval constants.
  //  @param str the input interval string
  //  @return the IntervalVar result
  static inline IntervalVar fromString(std::string str) {
    int32_t month, day;
    int64_t timeOffset;
    size_t dayIdx = str.find_first_of(':');
    if (dayIdx == std::string::npos)
      LOG_ERROR(ERRCODE_INVALID_TEXT_REPRESENTATION,
                "invalid input syntax for interval: \"%s\"", str.c_str());
    size_t timeIdx = str.find_first_of(':', dayIdx + 1);
    if (timeIdx == std::string::npos)
      LOG_ERROR(ERRCODE_INVALID_TEXT_REPRESENTATION,
                "invalid input syntax for interval: \"%s\"", str.c_str());
    month = std::stoi(str.substr(0, dayIdx));
    day = std::stoi(str.substr(dayIdx + 1, timeIdx - dayIdx - 1));
    timeOffset = std::strtoll(str.substr(timeIdx + 1).c_str(), NULL, 10);
    IntervalVar ret = {timeOffset, day, month};
    return ret;
  }

  int compare(const Datum& a, const Datum& b) const override { return 0; }

  int compare(const char* str1, uint64_t len1, const char* str2,
              uint64_t len2) const override {
    return 0;
  }

 private:
  static inline std::string addPostgresIntervalPart(int32_t value,
                                                    std::string field,
                                                    bool* iszero, bool* isneg) {
    if (value == 0) return "";
    std::string ret = "";
    if (!(*iszero)) ret += " ";
    *iszero = false;
    if (*isneg && value > 0) ret += "+";
    *isneg = (value < 0);
    ret += std::to_string(value) + " ";
    ret += (value == 1 ? field : (field + "s"));
    return ret;
  }
};

}  // namespace dbcommon

#endif  // DBCOMMON_SRC_DBCOMMON_TYPE_INTERVAL_H_
