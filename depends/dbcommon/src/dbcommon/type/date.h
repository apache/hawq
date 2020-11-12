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

#ifndef DBCOMMON_SRC_DBCOMMON_TYPE_DATE_H_
#define DBCOMMON_SRC_DBCOMMON_TYPE_DATE_H_

#include <cerrno>
#include <climits>
#include <cstdint>
#include <limits>
#include <memory>
#include <string>
#include <utility>

#include "dbcommon/log/logger.h"
#include "dbcommon/type/typebase.h"
#include "dbcommon/utils/timezone-util.h"

/* Julian-date equivalents of Day 0 in Unix and Postgres reckoning */
#define UNIX_EPOCH_JDATE 2440588          /* == date2j(1970, 1, 1) */
#define POSTGRES_EPOCH_JDATE 2451545      /* == date2j(2000, 1, 1) */
#define ORC_TIMESTAMP_EPOCH_JDATE 2457024 /* == date2j(2015, 1, 1) */
#define AD_EPOCH_JDATE 719162             /* == date2j(0001, 1, 1) */
#define TIMEZONE_ADJUST \
  -1325491552 /* == date_part('epoch','1927-12-31 23:54:08')*/

const int64_t SECONDS_PER_HOUR = 60 * 60;
const int64_t SECONDS_PER_DAY = SECONDS_PER_HOUR * 24;
const int32_t MONTHS_PER_YEAR = 12;
const int32_t INTERVAL_DAYS_PER_MONTH = 30;
const int32_t HOURS_PER_DAY = 24;

const int64_t USECS_PER_DAY = SECONDS_PER_DAY * 1000000;
const int64_t USECS_PER_HOUR = SECONDS_PER_HOUR * 1000000;
const int64_t USECS_PER_MINUTE = 60000000;
const int64_t USECS_PER_SEC = 1000000;
static const char *Month_Name[] = {"Jan", "Feb", "Mar", "Apr", "May", "Jun",
                                   "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"};
const int32_t PG_UNIX_EPOCH_JDATE = POSTGRES_EPOCH_JDATE - UNIX_EPOCH_JDATE;
const int64_t TIMESTAMP_EPOCH_JDATE = PG_UNIX_EPOCH_JDATE * SECONDS_PER_DAY;
const int64_t TIMESTAMP_INFINITY = 0x7fffffffffffffff;
const int64_t TIMESTAMP_NEG_INFINITY = -(0x7fffffffffffffff) - 1;
const int32_t TIMESTAMP_FIELD_MAXLEN = 10;

/*
 * Date/Time Configuration
 *
 * DateStyle defines the output formatting choice for date/time types:
 *	USE_POSTGRES_DATES specifies traditional Postgres format
 *	USE_ISO_DATES specifies ISO-compliant format
 *	USE_SQL_DATES specifies Oracle/Ingres-compliant format
 *	USE_GERMAN_DATES specifies German-style dd.mm/yyyy
 *
 * DateOrder defines the field order to be assumed when reading an
 * ambiguous date (anything not in YYYY-MM-DD format, with a four-digit
 * year field first, is taken to be ambiguous):
 *	DATEORDER_YMD specifies field order yy-mm-dd
 *	DATEORDER_DMY specifies field order dd-mm-yy ("European" convention)
 *	DATEORDER_MDY specifies field order mm-dd-yy ("US" convention)
 *
 * In the Postgres and SQL DateStyles, DateOrder also selects output field
 * order: day comes before month in DMY style, else month comes before day.
 *
 * The user-visible "DateStyle" run-time parameter subsumes both of these.
 */

/* valid DateStyle values */
#define USE_POSTGRES_DATES 0
#define USE_ISO_DATES 1
#define USE_SQL_DATES 2
#define USE_GERMAN_DATES 3
#define USE_XSD_DATES 4

/* valid DateOrder values */
#define DATEORDER_YMD 0
#define DATEORDER_DMY 1
#define DATEORDER_MDY 2

namespace dbcommon {

template <typename T>
class DateTypeBase : public FixedSizeTypeBase {
 public:
  typedef T base_type;

  static std::string dateToString(int32_t year, int32_t month, int32_t day) {
    std::string datestr = "";
    if (year < 1000) datestr += '0';
    if (year < 100) datestr += '0';
    if (year < 10) datestr += '0';
    datestr += std::to_string(year) + '-';
    if (month < 10) datestr += '0';
    datestr += std::to_string(month) + '-';
    if (day < 10) datestr += '0';
    datestr += std::to_string(day);
    return datestr;
  }

  static std::string timeToString(int64_t hour, int64_t minute, int64_t second,
                                  int64_t precision) {
    std::string timestr = "";
    if (hour < 10) timestr += '0';
    timestr += std::to_string(hour) + ':';
    if (minute < 10) timestr += '0';
    timestr += std::to_string(minute) + ':';
    if (second < 10) timestr += '0';
    timestr += std::to_string(second);
    if (precision > 0) {
      int64_t power_of_ten = 100000;
      timestr += '.';
      while (precision / power_of_ten == 0) {
        timestr += '0';
        power_of_ten /= 10;
      }
      while (precision % 10 == 0) {
        precision /= 10;
      }
      timestr += std::to_string(precision);
    }
    return timestr;
  }

  static std::string nanoToString(int64_t nanoseconds) {
    std::string result = "";
    if (nanoseconds > 0) {
      result += ".";
      for (int32_t i = 0; i < 9 - std::to_string(nanoseconds).length(); i++)
        result += "0";
      while (nanoseconds % 10 == 0) {
        nanoseconds /= 10;
      }
      result += std::to_string(nanoseconds);
    }
    return result;
  }

  static std::string toWeekName(int32_t days) {
    int32_t week = days % 7;
    if (week < 0) week += 7;
    switch (week) {
      case 0:
        return "Thu";
      case 1:
        return "Fri";
      case 2:
        return "Sat";
      case 3:
        return "Sun";
      case 4:
        return "Mon";
      case 5:
        return "Tue";
      case 6:
        return "Wed";
    }

    return "";
  }

  static std::string toMonthName(std::string month) {
    int64_t m = std::strtoll(month.c_str(), NULL, 10);
    if (m > 0 && m < 13)
      return Month_Name[m - 1];
    else
      LOG_ERROR(ERRCODE_INVALID_TEXT_REPRESENTATION,
                "invalid input syntax for month: \"%s\"", month.c_str());
  }

  static std::string toMonth(std::string monthName) {
    if (monthName.compare("Jan") == 0)
      return "01";
    else if (monthName.compare("Feb") == 0)
      return "02";
    else if (monthName.compare("Mar") == 0)
      return "03";
    else if (monthName.compare("Apr") == 0)
      return "04";
    else if (monthName.compare("May") == 0)
      return "05";
    else if (monthName.compare("Jun") == 0)
      return "06";
    else if (monthName.compare("Jul") == 0)
      return "07";
    else if (monthName.compare("Aug") == 0)
      return "08";
    else if (monthName.compare("Sep") == 0)
      return "09";
    else if (monthName.compare("Oct") == 0)
      return "10";
    else if (monthName.compare("Nov") == 0)
      return "11";
    else if (monthName.compare("Dec") == 0)
      return "12";
    else
      LOG_ERROR(ERRCODE_INVALID_TEXT_REPRESENTATION,
                "invalid input syntax for month name: \"%s\"",
                monthName.c_str());
  }

  /*
   * Calendar time to Julian date conversions.
   * Julian date is commonly used in astronomical applications,
   *	since it is numerically accurate and computationally simple.
   * The algorithms here will accurately convert between Julian day
   *	and calendar date for all non-negative Julian days
   *	(i.e. from Nov 24, -4713 on).
   *
   * These routines will be used by other date/time packages
   * - thomas 97/02/25
   *
   * Rewritten to eliminate overflow problems. This now allows the
   * routines to work correctly for all Julian day counts from
   * 0 to 2147483647	(Nov 24, -4713 to Jun 3, 5874898) assuming
   * a 32-bit integer. Longer types should also work to the limits
   * of their precision.
   */
  static int32_t date2j(int32_t y, int32_t m, int32_t d) {
    int32_t julian;
    int32_t century;

    if (m > 2) {
      m += 1;
      y += 4800;
    } else {
      m += 13;
      y += 4799;
    }

    century = y / 100;
    julian = y * 365 - 32167;
    julian += y / 4 - century + century / 4;
    julian += 7834 * m / 256 + d;

    return julian;
  } /* date2j() */

  static void j2date(int32_t jd, int32_t *year, int32_t *month, int32_t *day) {
    unsigned int julian;
    unsigned int quad;
    unsigned int extra;
    int y;

    julian = jd;
    julian += 32044;
    quad = julian / 146097;
    extra = (julian - quad * 146097) * 4 + 3;
    julian += 60 + quad * 3 + extra / 146097;
    quad = julian / 1461;
    julian -= quad * 1461;
    y = julian * 4 / 1461;
    julian = ((y != 0) ? ((julian + 305) % 365) : ((julian + 306) % 366)) + 123;
    y += quad * 4;
    *year = y - 4800;
    quad = julian * 2141 / 65536;
    *day = julian - 7834 * quad / 256;
    *month = (quad + 10) % 12 + 1;

    return;
  } /* j2date() */

  uint64_t getTypeWidth() const override { return kWidth; }

  std::string DatumToBinary(const Datum &d) const override {
    auto v = DatumGetValue<base_type>(d);
    return std::string(reinterpret_cast<char *>(&v), kWidth);
  }

  int compare(const Datum &a, const Datum &b) const override {
    auto v1 = DatumGetValue<base_type>(a);
    auto v2 = DatumGetValue<base_type>(b);

    if (v1 == v2) {
      return 0;
    }

    return v1 < v2 ? -1 : 1;
  }

  int compare(const char *str1, uint64_t len1, const char *str2,
              uint64_t len2) const override {
    LOG_ERROR(ERRCODE_FEATURE_NOT_SUPPORTED,
              "date type compare not supported yet");
  }

  static const uint64_t kWidth;
};

template <typename T>
const uint64_t DateTypeBase<T>::kWidth = sizeof(DateTypeBase<T>::base_type);

class DateType : public DateTypeBase<int32_t> {
 public:
  DateType() { this->typeKind = DATEID; }

  Datum getDatum(const char *str) const override {
    auto ret = CreateDatum<int32_t>(fromString(str));
    return ret;
  }

  std::string DatumToString(const Datum &d) const override {
    auto v = DatumGetValue<int32_t>(d);
    return toString(v);
  }

  // For it has been parsed by HAWQ, and the format is yyyy-mm-dd
  static inline int32_t fromString(const std::string &str) {
    bool formatErr = false;
    if (str.length() != 10 && str.length() != 13) goto invalidFormat;
    for (int i = 0; i < 10; i++)
      if (i == 4 || i == 7) {
        if (str[i] != '-') goto invalidFormat;
      } else {
        if (str[i] < '0' || str[i] > '9') goto invalidFormat;
      }

    {
      int32_t year = (str[0] - '0') * 1000 + (str[1] - '0') * 100 +
                     (str[2] - '0') * 10 + str[3] - '0';
      int32_t month = (str[5] - '0') * 10 + str[6] - '0';
      int32_t day = (str[8] - '0') * 10 + str[9] - '0';
      if (str.length() == 13)
        return date2j(-year + 1, month, day) - UNIX_EPOCH_JDATE;
      else
        return date2j(year, month, day) - UNIX_EPOCH_JDATE;
    }

  invalidFormat:
    char *end = NULL;
    errno = 0;
    long long val = std::strtoll(str.c_str(), &end, 10);  // NOLINT

    if (end == str.c_str()) {
      LOG_ERROR(ERRCODE_INVALID_TEXT_REPRESENTATION,
                "invalid input syntax for date: \"%s\"", str.c_str());
    } else if (((val == LLONG_MAX || val == LLONG_MIN) && errno == ERANGE) ||
               val > std::numeric_limits<int32_t>::max() ||
               val < std::numeric_limits<int32_t>::min()) {
      LOG_ERROR(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE,
                "value \"%s\" is out of range", str.c_str());
    }

    // return reinterpret_cast<int32_t>(val);
    return val;
  }

  static inline std::string toString(int32_t dateval) {
    int32_t year;
    int32_t month;
    int32_t day;
    if (dateval + AD_EPOCH_JDATE < 0) {
      j2date(dateval + UNIX_EPOCH_JDATE, &year, &month, &day);
      return dateToString(-year + 1, month, day) + " BC";
    } else {
      j2date(dateval + UNIX_EPOCH_JDATE, &year, &month, &day);
      return dateToString(year, month, day);
    }
  }

  static std::string EncodeDate(std::string str, int8_t dateStyle,
                                int8_t dateOrder) {
    if (str == "NULL") return "";
    std::string year = str.substr(0, 4);
    std::string month = str.substr(5, 2);
    std::string day = str.substr(8, 2);
    if (dateStyle == USE_POSTGRES_DATES || dateStyle == USE_SQL_DATES) {
      if (dateOrder == DATEORDER_YMD || dateOrder == DATEORDER_MDY) {
        str.replace(0, 2, month);
        str.replace(3, 2, day);
        str.replace(6, 4, year);
      } else {
        str.replace(0, 2, day);
        str.replace(3, 2, month);
        str.replace(6, 4, year);
      }
      if (dateStyle == USE_POSTGRES_DATES) {
        str[2] = str[5] = '-';
      } else {
        str[2] = str[5] = '/';
      }
    } else if (dateStyle == USE_GERMAN_DATES) {
      str.replace(0, 2, day);
      str.replace(3, 2, month);
      str.replace(6, 4, year);
      str[2] = str[5] = '.';
    }

    return std::move(str);
  }

  static std::string DecodeDate(std::string str, int8_t dateStyle,
                                int8_t dateOrder) {
    std::string year;
    std::string month;
    std::string day;
    bool doExchange = false;
    if (dateStyle == USE_POSTGRES_DATES || dateStyle == USE_SQL_DATES) {
      if (dateOrder == DATEORDER_YMD || dateOrder == DATEORDER_MDY) {
        month = str.substr(0, 2);
        day = str.substr(3, 2);
        year = str.substr(6, 4);
      } else {
        day = str.substr(0, 2);
        month = str.substr(3, 2);
        year = str.substr(6, 4);
      }
      doExchange = true;
    } else if (dateStyle == USE_GERMAN_DATES) {
      day = str.substr(0, 2);
      month = str.substr(3, 2);
      year = str.substr(6, 4);
      doExchange = true;
    }
    if (doExchange) {
      str.replace(0, 4, year);
      str.replace(5, 2, month);
      str.replace(8, 2, day);
      str[4] = str[7] = '-';
    }

    return std::move(str);
  }
};

class TimeType : public DateTypeBase<int64_t> {
 public:
  TimeType() { this->typeKind = TIMEID; }

  Datum getDatum(const char *str) const override {
    auto ret = CreateDatum<int64_t>(fromString(str));
    return ret;
  }

  std::string DatumToString(const Datum &d) const override {
    auto v = DatumGetValue<int64_t>(d);
    return toString(v);
  }

  // For it has been parsed by HAWQ, and the format is hh:mm:ss.p
  static inline int64_t fromString(const std::string &str) {
    int64_t hour;
    int64_t minute;
    int64_t second;
    int64_t precision = 0;
    int64_t result;
    int8_t i;
    if (str.length() < 8 || str.length() == 9 || str.length() > 15)
      goto invalidFormat;
    for (i = 0; i < 8; i++)
      if (i == 2 || i == 5) {
        if (str[i] != ':') goto invalidFormat;
      } else {
        if (str[i] < '0' || str[i] > '9') goto invalidFormat;
      }
    hour = (str[0] - '0') * 10 + str[1] - '0';
    minute = (str[3] - '0') * 10 + str[4] - '0';
    second = (str[6] - '0') * 10 + str[7] - '0';
    result = (hour * 60 + minute) * 60 + second;
    if (str.length() > 8) {
      if (str[8] != '.') goto invalidFormat;
      for (i = 9; i < str.length(); i++) {
        if (str[i] < '0' || str[i] > '9') goto invalidFormat;
        precision = precision * 10 + str[i] - '0';
      }
      for (i = str.length(); i < 15; i++) {
        precision *= 10;
      }
    }
    result = result * 1000000 + precision;
    return result;

  invalidFormat:
    char *end = NULL;
    errno = 0;
    long long val = std::strtoll(str.c_str(), &end, 10);  // NOLINT

    if (strlen(end) != 0) {
      LOG_ERROR(ERRCODE_INVALID_TEXT_REPRESENTATION,
                "invalid input syntax for time: \"%s\"", str.c_str());
    } else if (((val == LLONG_MAX || val == LLONG_MIN) && errno == ERANGE) ||
               val > std::numeric_limits<int64_t>::max() ||
               val < std::numeric_limits<int64_t>::min()) {
      LOG_ERROR(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE,
                "value \"%s\" is out of range", str.c_str());
    }

    return val;
  }

  static inline std::string toString(int64_t timeval) {
    int64_t precision = timeval % 1000000;
    timeval /= 1000000;
    int64_t second = timeval % 60;
    timeval /= 60;
    int64_t minute = timeval % 60;
    int64_t hour = timeval / 60;
    return timeToString(hour, minute, second, precision);
  }
};

class TimestampType : public DateTypeBase<int64_t> {
 public:
  TimestampType() { this->typeKind = TIMESTAMPID; }

  uint64_t getTypeWidth() const override { return sizeof(Timestamp); }

  Datum getDatum(const char *str) const override { return Datum(0); }

  Datum getDatum(const char *str, Timestamp *timestamp) {
    *timestamp = fromString(str);
    auto ret = CreateDatum(timestamp);
    return ret;
  }

  std::string DatumToString(const Datum &d) const override {
    Timestamp *value = DatumGetValue<Timestamp *>(d);
    int64_t second = value->second;
    int64_t nanosecond = value->nanosecond;
    return toString(second, nanosecond);
  }

  // For it has been parsed by HAWQ, and the format is yyyy-mm-dd hh:mm:ss.p
  static inline Timestamp fromString(const std::string &str) {
    Timestamp result;
    if (str == "infinity" || str == "-infinity") {
      result.second = TIMESTAMP_INFINITY / 1000000 + TIMESTAMP_EPOCH_JDATE;
      result.nanosecond = TIMESTAMP_INFINITY % 1000000 * 1000;
      return result;
    }

    std::size_t dateEnd = str.find_first_of(' ');
    if (dateEnd > str.length())
      LOG_ERROR(ERRCODE_INVALID_TEXT_REPRESENTATION,
                "invalid input syntax for timestamp: \"%s\"", str.c_str());
    std::size_t secondEnd = str.find_first_of('.');
    if (secondEnd > str.length()) secondEnd = str.length();
    std::size_t nanoEnd = str.length() - 1;
    std::string date;
    if (str[str.length() - 2] == 'B' && str[str.length() - 1] == 'C') {
      if (secondEnd == str.length()) secondEnd -= 3;
      nanoEnd -= 3;
      date = str.substr(0, dateEnd) + " BC";
    } else {
      date = str.substr(0, dateEnd);
    }
    std::string second = str.substr(dateEnd + 1, secondEnd - dateEnd - 1);
    std::string nanosecond;
    if (secondEnd == str.length())
      nanosecond = "";
    else
      nanosecond = str.substr(secondEnd + 1, nanoEnd - secondEnd);
    result.second =
        (int64_t)(DateType::fromString(date) - 0) * SECONDS_PER_DAY +
        TimeType::fromString(second) / 1000000;
    result.nanosecond = std::strtoll(nanosecond.c_str(), NULL, 10);
    for (int32_t i = 0; i < 9 - nanosecond.length(); i++)
      result.nanosecond *= 10;
    return result;
  }

  static inline std::string toString(int64_t second, int64_t nanosecond) {
    Timestamp ts = Timestamp(second, nanosecond);
    if (TimestampType::IsTimestampPosFinite(ts)) return "infinity";
    if (TimestampType::IsTimestampNegFinite(ts)) return "-infinity";
    int32_t days = (int32_t)(second / SECONDS_PER_DAY);
    int64_t seconds = second % SECONDS_PER_DAY * 1000000;
    if (seconds < 0) {
      days -= 1;
      seconds += SECONDS_PER_DAY * 1000000;
    }
    if (nanosecond < 0) {
      seconds -= 1000000;
      nanosecond += 1000000000;
    }
    std::string date = DateType::toString(days);
    std::string time = TimeType::toString(seconds);
    std::string nanoseconds = nanoToString(nanosecond);
    if (date[date.length() - 2] == 'B' && date[date.length() - 1] == 'C') {
      return date.substr(0, date.length() - 2) + time + nanoseconds + " BC";
    } else {
      return date + " " + time + nanoseconds;
    }
  }

  std::string DatumToBinary(const Datum &d) const override {
    Timestamp *v = DatumGetValue<Timestamp *>(d);
    return std::string(reinterpret_cast<char *>(v), sizeof(Timestamp));
  }

  int compare(const Datum &a, const Datum &b) const override {
    Timestamp *v1 = DatumGetValue<Timestamp *>(a);
    Timestamp *v2 = DatumGetValue<Timestamp *>(b);

    if (v1->second == v2->second) {
      if (v1->nanosecond == v2->nanosecond) {
        return 0;
      } else {
        return v1->nanosecond < v2->nanosecond ? -1 : 1;
      }
    }

    return v1->second < v2->second ? -1 : 1;
  }

  static inline bool IsTimestampPosFinite(Timestamp val) {
    int64_t ts =
        (val.second - TIMESTAMP_EPOCH_JDATE) * 1000000 + val.nanosecond / 1000;
    if (ts == TIMESTAMP_INFINITY) return true;
    return false;
  }

  static inline bool IsTimestampNegFinite(Timestamp val) {
    int64_t ts =
        (val.second - TIMESTAMP_EPOCH_JDATE) * 1000000 + val.nanosecond / 1000;
    if (ts == TIMESTAMP_NEG_INFINITY) return true;
    return false;
  }

  static std::string EncodeTimestamp(std::string str, int8_t dateStyle,
                                     int8_t dateOrder) {
    if (str == "NULL") return "";
    if (str == "infinity" || str == "-infinity") return str;
    std::string date = str.substr(0, 10);
    if (dateStyle == USE_POSTGRES_DATES) {
      uint64_t found = str.find("BC");
      std::string year = date.substr(0, 4);
      if (found != std::string::npos) {
        date.append(" BC");
        str.replace(found, 7, year + " BC");
      } else {
        str.append(" " + year);
      }
      int32_t days = DateType::fromString(date);
      std::string weekName = toWeekName(days);
      std::string month = date.substr(5, 2);
      std::string monthName = toMonthName(month);
      std::string day = date.substr(8, 2);
      if (dateOrder == DATEORDER_YMD || dateOrder == DATEORDER_MDY) {
        str.replace(0, 3, weekName);
        str.replace(4, 3, monthName);
        str.replace(8, 2, day);
        str[3] = str[7] = ' ';
      } else {
        str.replace(0, 3, weekName);
        str.replace(4, 2, day);
        str.replace(7, 3, monthName);
        str[3] = str[6] = ' ';
      }
    } else {
      date = DateType::EncodeDate(date, dateStyle, dateOrder);
      str.replace(0, 10, date);
    }

    return std::move(str);
  }

  static std::string DecodeTimestamp(std::string str, int8_t dateStyle,
                                     int8_t dateOrder) {
    if (str == "infinity" || str == "-infinity") return str;
    std::string date = str.substr(0, 10);
    if (dateStyle == USE_POSTGRES_DATES) {
      uint64_t found = str.find(" BC");
      std::string year;
      if (found != std::string::npos) {
        year = str.substr(str.length() - 7, 4);
        str.erase(str.length() - 7, 7);
        str.append("BC");
      } else {
        year = str.substr(str.length() - 4, 4);
        str.erase(str.length() - 5, 5);
      }
      std::string month;
      std::string monthName;
      std::string day;
      if (dateOrder == DATEORDER_YMD || dateOrder == DATEORDER_MDY) {
        monthName = str.substr(4, 3);
        day = str.substr(8, 2);
      } else {
        day = str.substr(4, 2);
        monthName = str.substr(7, 3);
      }
      month = toMonth(monthName);
      str.replace(0, 4, year);
      str.replace(5, 2, month);
      str.replace(8, 2, day);
      str[4] = str[7] = '-';
    } else {
      date = DateType::DecodeDate(date, dateStyle, dateOrder);
      str.replace(0, 10, date);
    }

    return std::move(str);
  }

  int compare(const char *str1, uint64_t len1, const char *str2,
              uint64_t len2) const override {
    assert(len1 = sizeof(Timestamp));
    assert(len2 = sizeof(Timestamp));
    const Timestamp *ts1 = reinterpret_cast<const Timestamp *>(str1);
    const Timestamp *ts2 = reinterpret_cast<const Timestamp *>(str2);
    if (ts1->second == ts2->second) {
      if (ts1->nanosecond == ts2->nanosecond) return 0;
      return ts1->nanosecond < ts2->nanosecond ? -1 : 1;
    }
    return ts1->second < ts2->second ? -1 : 1;
  }

  static void extractDate(int64_t second, int32_t *year, int32_t *month,
                          int32_t *day);
  static void extractTime(int64_t second, int32_t *hour2, int32_t *minute2,
                          int32_t *second2);
  static void timestamp_trunc(Timestamp *ret, int32_t year, int32_t month,
                              int32_t day, int32_t hour, int32_t minute,
                              int32_t second, int64_t nanosecond);
  static void isoweek2date(int32_t woy, int32_t *year, int32_t *month,
                           int32_t *day);
  static double extractYear(int64_t second);
  static double extractMonth(int64_t second);
  static double extractDay(int64_t second);
  static double extractCentury(int64_t second);
  static double extractDecade(int64_t second);
  static double countWeek(int32_t year, int32_t month, int32_t day);
  static double extractDow(int64_t second);
  static double extractDoy(int64_t second);
  static double extractEpoch(int64_t second, int64_t nanosecond);
  static double extractHour(int64_t second);
  static double extractMicroseconds(int64_t second, int64_t nanosecond);
  static double extractMillennium(int64_t second);
  static double extractMilliseconds(int64_t second, int64_t nanosecond);
  static double extractMinute(int64_t second);
  static double extractQuarter(int64_t second);
  static double extractSecond(int64_t second, int64_t nanosecond);
  static double extractWeek(int64_t second);
  static Timestamp truncMillennium(int64_t second);
  static Timestamp truncCentury(int64_t second);
  static Timestamp truncDecade(int64_t second);
  static Timestamp truncYear(int64_t second);
  static Timestamp truncQuarter(int64_t second);
  static Timestamp truncMonth(int64_t second);
  static Timestamp truncWeek(int64_t second);
  static Timestamp truncDay(int64_t second);
  static Timestamp truncHour(int64_t second);
  static Timestamp truncMinute(int64_t second);
  static Timestamp truncSecond(int64_t second);
  static Timestamp truncMilliseconds(int64_t second, int64_t nanosecond);
  static Timestamp truncMicroseconds(int64_t second, int64_t nanosecond);
  static std::string timestamp2string(int64_t second, int64_t nanosecond);
  template <typename T>
  static void int2string(T value, char *string, int32_t *length);
};

class TimestamptzType : public TimestampType {
 public:
  TimestamptzType() { this->typeKind = TIMESTAMPTZID; }

  Datum getDatum(const char *str, Timestamp *timestamp) {
    *timestamp = fromString(str);
    auto ret = CreateDatum(timestamp);
    return ret;
  }

  static std::string timezoneToString(int64_t offset) {
    int timezone = offset / SECONDS_PER_HOUR;
    int time = offset - timezone * SECONDS_PER_HOUR;
    int t_minute = time / 60;
    int t_second = time % 60;
    std::string result = "";
    if (timezone < 0) {
      result += "-";
    } else {
      result += "+";
    }
    if (timezone < 10) result += "0";
    result += std::to_string(std::abs(timezone));
    if (time) {
      result += ":";
      if (t_minute > 0 && t_minute < 10) result += "0";
      result += std::to_string(std::abs(t_minute));
      result += ":";
      if (t_second > 0 && t_second < 10) result += "0";
      result += std::to_string(std::abs(t_second));
    }
    return result;
  }

  static inline Timestamp fromString(const std::string &str) {
    Timestamp result;
    if (str == "infinity" || str == "-infinity") {
      result.second = TIMESTAMP_INFINITY / 1000000 + TIMESTAMP_EPOCH_JDATE;
      result.nanosecond = TIMESTAMP_INFINITY % 1000000 * 1000;
      return result;
    }

    size_t len = str.length();
    std::string date, timetz;
    std::size_t dateEnd = str.find_first_of(' ');
    if (dateEnd > str.length())
      LOG_ERROR(ERRCODE_INVALID_TEXT_REPRESENTATION,
                "invalid input syntax for timestamp: \"%s\"", str.c_str());
    if (str[len - 2] == 'B' && str[len - 1] == 'C') {
      timetz = str.substr(dateEnd + 1, len - 3 - dateEnd - 1);
      date = str.substr(0, dateEnd) + " BC";
    } else {
      timetz = str.substr(dateEnd + 1);
      date = str.substr(0, dateEnd);
    }

    double timezone = 0;
    std::string time(timetz);
    std::size_t index = timetz.find_first_of("+-");
    if (index != std::string::npos) {
      timezone = parseTimezone(timetz.substr(index));
      time = timetz.substr(0, index);
    }

    std::string second(time), nanosecond;
    std::size_t pointIdx = time.find_first_of('.');
    if (pointIdx != std::string::npos) {
      second = time.substr(0, pointIdx);
      nanosecond = time.substr(pointIdx + 1);
    }

    result.second =
        (int64_t)(DateType::fromString(date) - 0) * SECONDS_PER_DAY +
        TimeType::fromString(second) / 1000000 -
        static_cast<int64_t>(timezone * SECONDS_PER_HOUR);
    result.nanosecond = std::strtoll(nanosecond.c_str(), NULL, 10);
    for (int32_t i = 0; i < 9 - nanosecond.length(); i++)
      result.nanosecond *= 10;
    return result;
  }

  static inline std::string toString(int64_t second, int64_t nanosecond) {
    int64_t val =
        (second - TIMESTAMP_EPOCH_JDATE) * 1000000 + nanosecond / 1000;
    if (val == TIMESTAMP_INFINITY) return "infinity";
    if (val == TIMESTAMP_NEG_INFINITY) return "-infinity";

    int32_t timezoneOffset = TimezoneUtil::getGMTOffset(second);
    second += timezoneOffset;
    if (second < timezoneOffset + TIMEZONE_ADJUST) {
      timezoneOffset += 352;
      second += 352;
    }
    int32_t days = (int32_t)(second / SECONDS_PER_DAY);
    int64_t seconds = second % SECONDS_PER_DAY * 1000000;
    if (seconds < 0) {
      days -= 1;
      seconds += SECONDS_PER_DAY * 1000000;
    }
    if (nanosecond < 0) {
      seconds -= 1000000;
      nanosecond += 1000000000;
    }
    std::string date = DateType::toString(days);
    std::string time = TimeType::toString(seconds);
    std::string nanoseconds = nanoToString(nanosecond);
    std::string timezone = timezoneToString(timezoneOffset);
    if (date[date.length() - 2] == 'B' && date[date.length() - 1] == 'C') {
      return date.substr(0, date.length() - 2) + time + nanoseconds + timezone +
             " BC";
    } else {
      return date + " " + time + nanoseconds + timezone;
    }
  }

  static std::string EncodeTimestamptz(std::string str, int8_t dateStyle,
                                       int8_t dateOrder) {
    if (str == "NULL") return "";
    if (str == "infinity" || str == "-infinity") return str;
    std::string date = str.substr(0, 10);
    if (dateStyle == USE_ISO_DATES) {
      date = DateType::EncodeDate(date, dateStyle, dateOrder);
      str.replace(0, 10, date);
    } else {
      LOG_ERROR(
          ERRCODE_FEATURE_NOT_SUPPORTED,
          "Date style is not supported for timestamptz type in new executor");
    }

    return std::move(str);
  }

  static std::string DecodeTimestamptz(std::string str, int8_t dateStyle,
                                       int8_t dateOrder) {
    if (str == "infinity" || str == "-infinity") return str;
    std::string date = str.substr(0, 10);
    if (dateStyle == USE_ISO_DATES) {
      date = DateType::DecodeDate(date, dateStyle, dateOrder);
      str.replace(0, 10, date);
    } else {
      LOG_ERROR(
          ERRCODE_FEATURE_NOT_SUPPORTED,
          "Date style is not supported for timestamptz type in new executor");
    }

    return std::move(str);
  }

 private:
  static inline double parseTimezone(const std::string &timezoneStr) {
    std::string str = timezoneStr.substr(1);
    double result = std::stod(str);
    if (timezoneStr[0] == '-') result = -result;
    return result;
  }
};

}  // namespace dbcommon

#endif  // DBCOMMON_SRC_DBCOMMON_TYPE_DATE_H_
