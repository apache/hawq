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

#include "dbcommon/type/date.h"

#include <cstdlib>
#include <limits>
#include <memory>
#include <utility>

#include "dbcommon/common/vector/fixed-length-vector.h"
#include "dbcommon/common/vector/timestamp-vector.h"
#include "dbcommon/utils/time-util.h"

namespace dbcommon {

void TimestampType::extractDate(int64_t second, int32_t *year, int32_t *month,
                                int32_t *day) {
  int32_t days = (int32_t)(second / SECONDS_PER_DAY);
  int64_t seconds = second % SECONDS_PER_DAY * 1000000;
  if (seconds < 0) {
    days -= 1;
    seconds += SECONDS_PER_DAY * 1000000;
  }
  j2date(days + UNIX_EPOCH_JDATE, year, month, day);
}

double TimestampType::extractYear(int64_t second) {
  int32_t year, month, day;
  extractDate(second, &year, &month, &day);
  if (year <= 0) year--;
  return static_cast<double>(year);
}

double TimestampType::extractMonth(int64_t second) {
  int32_t year, month, day;
  extractDate(second, &year, &month, &day);
  return static_cast<double>(month);
}

double TimestampType::extractDay(int64_t second) {
  int32_t year, month, day;
  extractDate(second, &year, &month, &day);
  return static_cast<double>(day);
}

double TimestampType::extractCentury(int64_t second) {
  int32_t year, month, day, century;
  extractDate(second, &year, &month, &day);
  if (year > 0)
    century = (year + 99) / 100;
  else
    century = -((99 - (year - 1)) / 100);
  return static_cast<double>(century);
}

double TimestampType::extractDecade(int64_t second) {
  int32_t year, month, day, decade;
  extractDate(second, &year, &month, &day);
  if (year >= 0)
    decade = year / 10;
  else
    decade = -((8 - (year - 1)) / 10);

  return static_cast<double>(decade);
}
double TimestampType::extractDow(int64_t second) {
  int32_t year, month, day, dow;
  extractDate(second, &year, &month, &day);
  dow = (date2j(year, month, day) + 1) % 7;
  return static_cast<double>(dow);
}

double TimestampType::extractDoy(int64_t second) {
  int32_t year, month, day, doy;
  extractDate(second, &year, &month, &day);
  doy = date2j(year, month, day) - date2j(year, 1, 1) + 1;
  return static_cast<double>(doy);
}

double TimestampType::extractEpoch(int64_t second, int64_t nanosecond) {
  TimezoneUtil::setGMTOffset("PRC");
  int32_t timezoneOffset = TimezoneUtil::getGMTOffset(second);
  second -= timezoneOffset;
  double second_new = static_cast<double>(second) +
                      static_cast<double>(nanosecond) / 1000000000.0;
  if (second_new < -1325491552) second_new -= 352;
  return static_cast<double>(second_new);
}

void TimestampType::extractTime(int64_t second, int32_t *hour2,
                                int32_t *minute2, int32_t *second2) {
  int32_t second_rest = (int32_t)(second % SECONDS_PER_DAY);
  *hour2 = (int32_t)(second_rest / 3600);
  *minute2 = (int32_t)((second_rest - (*hour2) * 3600) / 60);
  *second2 = (int32_t)(second_rest - (*hour2) * 3600 - (*minute2) * 60);
  if (*hour2 < 0) *hour2 += 23;
  if (*minute2 < 0) *minute2 += 59;
  if (*second2 < 0) *second2 += 60;
}

double TimestampType::extractHour(int64_t second) {
  int32_t hour2, minute2, second2;
  extractTime(second, &hour2, &minute2, &second2);
  return static_cast<double>(hour2);
}

double TimestampType::extractMinute(int64_t second) {
  int32_t hour2, minute2, second2;
  extractTime(second, &hour2, &minute2, &second2);
  return static_cast<double>(minute2);
}

double TimestampType::extractSecond(int64_t second, int64_t nanosecond) {
  int32_t hour2, minute2, second2;
  extractTime(second, &hour2, &minute2, &second2);
  double second_new = static_cast<double>(second2) +
                      static_cast<double>(nanosecond) / 1000000000.0;
  return static_cast<double>(second_new);
}

double TimestampType::extractMicroseconds(int64_t second, int64_t nanosecond) {
  int32_t hour2, minute2, second2;
  extractTime(second, &hour2, &minute2, &second2);
  double second_new = static_cast<double>(second2) * 1000000 +
                      static_cast<double>(nanosecond) / 1000.0;
  return static_cast<double>(second_new);
}

double TimestampType::extractMillennium(int64_t second) {
  int32_t year, month, day, millennium;
  extractDate(second, &year, &month, &day);
  if (year > 0)
    millennium = (year + 999) / 1000;
  else
    millennium = -((999 - (year - 1)) / 1000);
  return static_cast<double>(millennium);
}

double TimestampType::extractMilliseconds(int64_t second, int64_t nanosecond) {
  int32_t hour2, minute2, second2;
  extractTime(second, &hour2, &minute2, &second2);
  double second_new = static_cast<double>(second2) * 1000 +
                      static_cast<double>(nanosecond) / 1000000.0;
  return static_cast<double>(second_new);
}

double TimestampType::extractQuarter(int64_t second) {
  int32_t year, month, day, quarter;
  extractDate(second, &year, &month, &day);
  quarter = (month - 1) / 3 + 1;
  return static_cast<double>(quarter);
}
double TimestampType::countWeek(int32_t year, int32_t month, int32_t day) {
  int32_t day0, day4, dayn, week;

  dayn = date2j(year, month, day);
  day4 = date2j(year, 1, 4);
  day0 = day4 % 7;

  if (dayn < day4 - day0) {
    day4 = date2j(year - 1, 1, 4);
    day0 = day4 % 7;
  }
  week = (dayn - (day4 - day0)) / 7 + 1;
  if (week >= 52) {
    day4 = date2j(year + 1, 1, 4);
    day0 = day4 % 7;
    if (dayn >= day4 - day0) week = (dayn - (day4 - day0)) / 7 + 1;
  }
  return static_cast<double>(week);
}
double TimestampType::extractWeek(int64_t second) {
  int32_t year, month, day, week;
  extractDate(second, &year, &month, &day);
  week = (int32_t)countWeek(year, month, day);
  return static_cast<double>(week);
}

void TimestampType::timestamp_trunc(Timestamp *ret, int32_t year, int32_t month,
                                    int32_t day, int32_t hour, int32_t minute,
                                    int32_t second, int64_t nanosecond) {
  int32_t date, time;
  date = date2j(year, month, day) - UNIX_EPOCH_JDATE;
  time = (((hour * 60) + minute) * 60) + second;
  ret->second = date * SECONDS_PER_DAY + time;
  ret->nanosecond = nanosecond;
}
Timestamp TimestampType::truncMillennium(int64_t second) {
  int32_t year, month, day;
  Timestamp ret;
  extractDate(second, &year, &month, &day);
  if (year > 0)
    year = ((year + 999) / 1000) * 1000 - 999;
  else
    year = -((999 - (year - 1)) / 1000) * 1000 + 1;
  timestamp_trunc(&ret, year, 1, 1, 0, 0, 0, 0);
  return ret;
}
Timestamp TimestampType::truncCentury(int64_t second) {
  int32_t year, month, day;
  Timestamp ret;
  extractDate(second, &year, &month, &day);
  if (year > 0)
    year = ((year + 99) / 100) * 100 - 99;
  else
    year = -((99 - (year - 1)) / 100) * 100 + 1;
  timestamp_trunc(&ret, year, 1, 1, 0, 0, 0, 0);
  return ret;
}
Timestamp TimestampType::truncDecade(int64_t second) {
  int32_t year, month, day;
  Timestamp ret;
  extractDate(second, &year, &month, &day);
  if (year > 0)
    year = (year / 10) * 10;
  else
    year = -((8 - (year - 1)) / 10) * 10;
  timestamp_trunc(&ret, year, 1, 1, 0, 0, 0, 0);
  return ret;
}
Timestamp TimestampType::truncYear(int64_t second) {
  int32_t year, month, day;
  Timestamp ret;
  extractDate(second, &year, &month, &day);
  timestamp_trunc(&ret, year, 1, 1, 0, 0, 0, 0);
  return ret;
}
Timestamp TimestampType::truncQuarter(int64_t second) {
  int32_t year, month, day;
  Timestamp ret;
  extractDate(second, &year, &month, &day);
  month = (3 * ((month - 1) / 3)) + 1;
  timestamp_trunc(&ret, year, month, 1, 0, 0, 0, 0);
  return ret;
}
Timestamp TimestampType::truncMonth(int64_t second) {
  int32_t year, month, day;
  Timestamp ret;
  extractDate(second, &year, &month, &day);
  timestamp_trunc(&ret, year, month, 1, 0, 0, 0, 0);
  return ret;
}
Timestamp TimestampType::truncDay(int64_t second) {
  int32_t year, month, day;
  Timestamp ret;
  extractDate(second, &year, &month, &day);
  timestamp_trunc(&ret, year, month, day, 0, 0, 0, 0);
  return ret;
}
Timestamp TimestampType::truncHour(int64_t second) {
  int32_t year, month, day, hour2, minute2, second2;
  Timestamp ret;
  extractDate(second, &year, &month, &day);
  extractTime(second, &hour2, &minute2, &second2);
  timestamp_trunc(&ret, year, month, day, hour2, 0, 0, 0);
  return ret;
}
Timestamp TimestampType::truncMinute(int64_t second) {
  int32_t year, month, day, hour2, minute2, second2;
  Timestamp ret;
  extractDate(second, &year, &month, &day);
  extractTime(second, &hour2, &minute2, &second2);
  timestamp_trunc(&ret, year, month, day, hour2, minute2, 0, 0);
  return ret;
}
Timestamp TimestampType::truncSecond(int64_t second) {
  int32_t year, month, day, hour2, minute2, second2;
  Timestamp ret;
  extractDate(second, &year, &month, &day);
  extractTime(second, &hour2, &minute2, &second2);
  timestamp_trunc(&ret, year, month, day, hour2, minute2, second2, 0);
  return ret;
}
Timestamp TimestampType::truncMilliseconds(int64_t second, int64_t nanosecond) {
  int32_t year, month, day, hour2, minute2, second2;
  int64_t nano;
  Timestamp ret;
  extractDate(second, &year, &month, &day);
  extractTime(second, &hour2, &minute2, &second2);
  nano = (nanosecond / 1000000) * 1000000;
  timestamp_trunc(&ret, year, month, day, hour2, minute2, second2, nano);
  return ret;
}
Timestamp TimestampType::truncMicroseconds(int64_t second, int64_t nanosecond) {
  int32_t year, month, day, hour2, minute2, second2;
  int64_t nano;
  Timestamp ret;
  extractDate(second, &year, &month, &day);
  extractTime(second, &hour2, &minute2, &second2);
  nano = (nanosecond / 1000) * 1000;
  timestamp_trunc(&ret, year, month, day, hour2, minute2, second2, nano);
  return ret;
}
void TimestampType::isoweek2date(int32_t woy, int32_t *year, int32_t *month,
                                 int32_t *day) {
  int32_t day0, day4, jd;
  day4 = date2j(*year, 1, 4);
  day0 = day4 % 7;
  jd = ((woy - 1) * 7) + (day4 - day0);
  j2date(jd, year, month, day);
}

Timestamp TimestampType::truncWeek(int64_t second) {
  int32_t year, month, day, woy;
  Timestamp ret;
  extractDate(second, &year, &month, &day);
  woy = (int32_t)countWeek(year, month, day);
  if (woy >= 52 && month == 1) --year;
  if (woy <= 1 && month == 12) ++year;
  isoweek2date(woy, &year, &month, &day);
  timestamp_trunc(&ret, year, month, day, 0, 0, 0, 0);
  return ret;
}
template <typename T>
void TimestampType::int2string(T value, char *string, int32_t *length) {
  char data[11] = "0123456789";
  char a[10] = {0};
  char *str = string;
  T sum = value;
  int32_t i = 0;
  while (sum > 0) {
    a[i++] = data[sum % 10];
    sum /= 10;
  }
  for (int32_t j = i - 1; j >= 0; j--) {
    *str++ = a[j];
    (*length)++;
  }
}
std::string TimestampType::timestamp2string(int64_t second,
                                            int64_t nanosecond) {
  int64_t val = (second - TIMESTAMP_EPOCH_JDATE) * 1000000 + nanosecond / 1000;
  if (val == TIMESTAMP_INFINITY) return "infinity";
  if (val == TIMESTAMP_NEG_INFINITY) return "-infinity";
  int32_t days = (int32_t)(second / SECONDS_PER_DAY);
  int64_t seconds = second % SECONDS_PER_DAY;
  if (seconds < 0) {
    days -= 1;
    seconds += SECONDS_PER_DAY;
  }
  if (nanosecond < 0) {
    seconds -= 1000000;
    nanosecond += 1000000000;
  }
  char result[30];
  int32_t length = 0;
  int32_t year, month, day;
  bool is_bc = false;
  int32_t second_real = seconds % 60;
  seconds /= 60;
  int32_t minute = seconds % 60;
  int32_t hour = seconds / 60;
  int32_t nano_length = 1;

  j2date(days + UNIX_EPOCH_JDATE, &year, &month, &day);
  if (days + AD_EPOCH_JDATE < 0) {
    year = -year + 1;
    is_bc = true;
  }

  if (year < 1000) result[length++] = '0';
  if (year < 100) result[length++] = '0';
  if (year < 10) result[length++] = '0';
  int2string(year, &result[length], &length);
  result[length++] = '-';
  if (month < 10) result[length++] = '0';
  int2string(month, &result[length], &length);
  result[length++] = '-';
  if (day < 10) result[length++] = '0';
  int2string(day, &result[length], &length);
  result[length++] = ' ';

  if (hour < 10) result[length++] = '0';
  int2string(hour, &result[length], &length);
  result[length++] = ':';
  if (minute < 10) result[length++] = '0';
  int2string(minute, &result[length], &length);
  result[length++] = ':';
  if (second_real < 10) result[length++] = '0';
  int2string(second_real, &result[length], &length);

  if (nanosecond > 0) {
    result[length++] = '.';
    for (int64_t i = nanosecond; i != 0; i /= 10)
      if (i / 10 != 0) nano_length++;
    for (int32_t i = 0; i < 9 - nano_length; i++) result[length++] = '0';

    while (nanosecond % 10 == 0) {
      nanosecond /= 10;
    }
    int2string(nanosecond, &result[length], &length);
  }
  if (is_bc) {
    result[length++] = ' ';
    result[length++] = 'B';
    result[length++] = 'C';
  }
  result[length] = '\0';
  std::string str(result, length);
  return str;
}

}  // namespace dbcommon
