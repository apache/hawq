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

#include "dbcommon/function/date-function.h"

#include <algorithm>
#include <string>
#include <tuple>
#include <utility>

#include "dbcommon/common/vector-transformer.h"
#include "dbcommon/common/vector.h"
#include "dbcommon/common/vector/fixed-length-vector.h"
#include "dbcommon/common/vector/timestamp-vector.h"
#include "dbcommon/function/arithmetic-function.h"
#include "dbcommon/function/function.h"
#include "dbcommon/type/date.h"
#include "dbcommon/type/interval.h"

namespace dbcommon {

const DatetimeAliasMap DatetimeTable::mapTable =
    DatetimeTable::createDatetimeMap();

const DatetimeAliasMap DatetimeTable::createDatetimeMap() {
  DatetimeAliasMap dtMap;
  dtMap.insert(std::make_pair("epoch", EPOCH));
  dtMap.insert(std::make_pair("mil", MILLENNIUM));
  dtMap.insert(std::make_pair("millennia", MILLENNIUM));
  dtMap.insert(std::make_pair("millennium", MILLENNIUM));
  dtMap.insert(std::make_pair("mils", MILLENNIUM));
  dtMap.insert(std::make_pair("c", CENTURY));
  dtMap.insert(std::make_pair("cent", CENTURY));
  dtMap.insert(std::make_pair("centuries", CENTURY));
  dtMap.insert(std::make_pair("century", CENTURY));
  dtMap.insert(std::make_pair("dec", DECADE));
  dtMap.insert(std::make_pair("decade", DECADE));
  dtMap.insert(std::make_pair("descades", DECADE));
  dtMap.insert(std::make_pair("decs", DECADE));
  dtMap.insert(std::make_pair("y", YEAR));
  dtMap.insert(std::make_pair("year", YEAR));
  dtMap.insert(std::make_pair("years", YEAR));
  dtMap.insert(std::make_pair("yr", YEAR));
  dtMap.insert(std::make_pair("yrs", YEAR));
  dtMap.insert(std::make_pair("qtr", QUARTER));
  dtMap.insert(std::make_pair("quarter", QUARTER));
  dtMap.insert(std::make_pair("mon", MONTH));
  dtMap.insert(std::make_pair("mons", MONTH));
  dtMap.insert(std::make_pair("month", MONTH));
  dtMap.insert(std::make_pair("months", MONTH));
  dtMap.insert(std::make_pair("w", WEEK));
  dtMap.insert(std::make_pair("week", WEEK));
  dtMap.insert(std::make_pair("weeks", WEEK));
  dtMap.insert(std::make_pair("d", DAY));
  dtMap.insert(std::make_pair("day", DAY));
  dtMap.insert(std::make_pair("days", DAY));
  dtMap.insert(std::make_pair("dow", DOW));
  dtMap.insert(std::make_pair("doy", DOY));
  dtMap.insert(std::make_pair("h", HOUR));
  dtMap.insert(std::make_pair("hour", HOUR));
  dtMap.insert(std::make_pair("hours", HOUR));
  dtMap.insert(std::make_pair("hr", HOUR));
  dtMap.insert(std::make_pair("hrs", HOUR));
  dtMap.insert(std::make_pair("m", MINUTE));
  dtMap.insert(std::make_pair("min", MINUTE));
  dtMap.insert(std::make_pair("mins", MINUTE));
  dtMap.insert(std::make_pair("minute", MINUTE));
  dtMap.insert(std::make_pair("minutes", MINUTE));
  dtMap.insert(std::make_pair("s", SECOND));
  dtMap.insert(std::make_pair("sec", SECOND));
  dtMap.insert(std::make_pair("second", SECOND));
  dtMap.insert(std::make_pair("seconds", SECOND));
  dtMap.insert(std::make_pair("secs", SECOND));
  dtMap.insert(std::make_pair("millisecon", MILLISEC));
  dtMap.insert(std::make_pair("ms", MILLISEC));
  dtMap.insert(std::make_pair("msec", MILLISEC));
  dtMap.insert(std::make_pair("msecond", MILLISEC));
  dtMap.insert(std::make_pair("mseconds", MILLISEC));
  dtMap.insert(std::make_pair("msecs", MILLISEC));
  dtMap.insert(std::make_pair("microsecon", MICROSEC));
  dtMap.insert(std::make_pair("us", MICROSEC));
  dtMap.insert(std::make_pair("usec", MICROSEC));
  dtMap.insert(std::make_pair("usecond", MICROSEC));
  dtMap.insert(std::make_pair("useconds", MICROSEC));
  dtMap.insert(std::make_pair("usecs", MICROSEC));

  return dtMap;
}

Datum date_to_timestamp(Datum *params, uint64_t size) {
  assert(size == 2);
  Object *para = DatumGetValue<Object *>(params[1]);
  if (dynamic_cast<Vector *>(para)) {
    Vector *retVector = params[0];
    Vector *srcVector = params[1];

    FixedSizeTypeVectorRawData<int32_t> src(srcVector);
    retVector->resize(src.plainSize, src.sel, src.nulls);
    TimestampVectorRawData ret(retVector);

    auto cast = [&](uint64_t plainIdx) {
      ret.seconds[plainIdx] =
          static_cast<int64_t>(src.values[plainIdx] * SECONDS_PER_DAY);
    };
    dbcommon::transformVector(ret.plainSize, ret.sel, ret.nulls, cast);
    memset(ret.nanoseconds, 0, sizeof(int64_t) * ret.plainSize);

  } else {
    Scalar *retScalar = params[0];
    Scalar *srcScalar = params[1];

    if (srcScalar->isnull) {
      retScalar->isnull = true;
    } else {
      retScalar->isnull = false;
      Timestamp *ret = retScalar->allocateValue<Timestamp>();
      ret->second = static_cast<int64_t>(
          DatumGetValue<int32_t>(srcScalar->value) * SECONDS_PER_DAY);
      ret->nanosecond = 0;
    }
  }
  return params[0];
}

Datum timestamp_to_date(Datum *params, uint64_t size) {
  assert(size == 2);
  Object *para = DatumGetValue<Object *>(params[1]);
  if (dynamic_cast<Vector *>(para)) {
    Vector *retVector = params[0];
    Vector *srcVector = params[1];

    TimestampVectorRawData src(srcVector);
    retVector->resize(src.plainSize, src.sel, src.nulls);
    FixedSizeTypeVectorRawData<int32_t> ret(retVector);

    auto cast = [&](uint64_t plainIdx) {
      int64_t jsecond =
          src.seconds[plainIdx] + SECONDS_PER_DAY * UNIX_EPOCH_JDATE;
      ret.values[plainIdx] =
          static_cast<int32_t>(jsecond / SECONDS_PER_DAY) - UNIX_EPOCH_JDATE;
    };
    dbcommon::transformVector(ret.plainSize, ret.sel, ret.nulls, cast);
  } else {
    Scalar *retScalar = params[0];
    Scalar *srcScalar = params[1];
    if (srcScalar->isnull) {
      retScalar->isnull = true;
    } else {
      retScalar->isnull = false;
      Timestamp *srcPtr = DatumGetValue<Timestamp *>(srcScalar->value);
      int32_t ret = static_cast<int32_t>(srcPtr->second / SECONDS_PER_DAY);
      retScalar->value = CreateDatum<int32_t>(ret);
    }
  }
  return params[0];
}

Datum is_timestamp_finite(Datum *params, uint64_t size) {
  assert(size == 2);
  Object *para = DatumGetValue<Object *>(params[1]);
  if (dynamic_cast<Vector *>(para)) {
    SelectList *retSelectlist = params[0];
    Vector *srcVector = params[1];

    TimestampVectorRawData src(srcVector);

    uint64_t counter = 0;
    retSelectlist->setNulls(src.plainSize, src.sel, retSelectlist->getNulls(),
                            src.nulls);
    SelectList::value_type *ret = retSelectlist->begin();
    auto cast = [&](uint64_t plainIdx) {
      int64_t val = (src.seconds[plainIdx] - TIMESTAMP_EPOCH_JDATE) * 1000000 +
                    src.nanoseconds[plainIdx] / 1000;
      if (val != TIMESTAMP_INFINITY && val != TIMESTAMP_NEG_INFINITY)
        ret[counter++] = plainIdx;
    };
    dbcommon::transformVector(src.plainSize, src.sel, src.nulls, cast);
    retSelectlist->resize(counter);
  } else {
    Scalar *retScalar = params[0];
    Scalar *srcScalar = params[1];
    retScalar->clear();
    if (srcScalar->isnull) {
      retScalar->isnull = true;
    } else {
      Timestamp *src = srcScalar->value;
      bool ret = false;
      int64_t val = (src->second - TIMESTAMP_EPOCH_JDATE) * 1000000 +
                    src->nanosecond / 1000;
      if (val != TIMESTAMP_INFINITY && val != TIMESTAMP_NEG_INFINITY)
        ret = true;

      retScalar->isnull = false;
      retScalar->value = CreateDatum<bool>(ret);
    }
  }
  return params[0];
}

double inline timestamp_extract(Timestamp *timestamp, const char *strVal,
                                uint32_t length) {
  double ret;
  uint32_t validLen =
      length <= TIMESTAMP_FIELD_MAXLEN ? length : TIMESTAMP_FIELD_MAXLEN;
  std::string field(strVal, validLen);

  switch (DatetimeTable::getDatetimeKindByName(field)) {
    case EPOCH:
      ret =
          TimestampType::extractEpoch(timestamp->second, timestamp->nanosecond);
      break;
    case MILLENNIUM:
      ret = TimestampType::extractMillennium(timestamp->second);
      break;
    case CENTURY:
      ret = TimestampType::extractCentury(timestamp->second);
      break;
    case DECADE:
      ret = TimestampType::extractDecade(timestamp->second);
      break;
    case YEAR:
      ret = TimestampType::extractYear(timestamp->second);
      break;
    case QUARTER:
      ret = TimestampType::extractQuarter(timestamp->second);
      break;
    case MONTH:
      ret = TimestampType::extractMonth(timestamp->second);
      break;
    case WEEK:
      ret = TimestampType::extractWeek(timestamp->second);
      break;
    case DAY:
      ret = TimestampType::extractDay(timestamp->second);
      break;
    case DOW:
      ret = TimestampType::extractDow(timestamp->second);
      break;
    case DOY:
      ret = TimestampType::extractDoy(timestamp->second);
      break;
    case HOUR:
      ret = TimestampType::extractHour(timestamp->second);
      break;
    case MINUTE:
      ret = TimestampType::extractMinute(timestamp->second);
      break;
    case SECOND:
      ret = TimestampType::extractSecond(timestamp->second,
                                         timestamp->nanosecond);
      break;
    case MILLISEC:
      ret = TimestampType::extractMilliseconds(timestamp->second,
                                               timestamp->nanosecond);
      break;
    case MICROSEC:
      ret = TimestampType::extractMicroseconds(timestamp->second,
                                               timestamp->nanosecond);
      break;
    default:
      LOG_ERROR(ERRCODE_FEATURE_NOT_SUPPORTED,
                "timestamp units \"%s\" not recognized", field.c_str());
  }
  return ret;
}

Datum extract_subfield_vec_timestamp_vec(Datum *params, uint64_t size) {
  Vector *retVector = params[0];
  Vector *subfieldVector = params[1];
  Vector *timestampVector = params[2];

  subfieldVector->trim();
  TimestampVectorRawData timestamp(timestampVector);
  VariableSizeTypeVectorRawData subfield(subfieldVector);
  retVector->resize(timestamp.plainSize, timestamp.sel, timestamp.nulls,
                    subfield.nulls);
  FixedSizeTypeVectorRawData<double> ret(retVector);

  uint64_t bufferSize = 0;
  auto extract = [&](uint64_t plainIdx) {
    Timestamp ts(timestamp.seconds[plainIdx], timestamp.nanoseconds[plainIdx]);
    ret.values[plainIdx] = timestamp_extract(&ts, subfield.valptrs[plainIdx],
                                             subfield.lengths[plainIdx]);
  };
  dbcommon::transformVector(ret.plainSize, ret.sel, ret.nulls, extract);

  return params[0];
}

Datum extract_subfield_vec_timestamp_val(Datum *params, uint64_t size) {
  Vector *retVector = params[0];
  Vector *subfieldVector = params[1];
  Scalar *timestampScalar = params[2];

  if (timestampScalar->isnull) {
    subfieldVector->trim();
    VariableSizeTypeVectorRawData subfield(subfieldVector);
    retVector->resize(subfield.plainSize, subfield.sel, true);
  } else {
    subfieldVector->trim();
    VariableSizeTypeVectorRawData subfield(subfieldVector);
    retVector->resize(subfield.plainSize, subfield.sel, subfield.nulls);
    FixedSizeTypeVectorRawData<double> ret(retVector);

    uint64_t bufferSize = 0;
    auto extract = [&](uint64_t plainIdx) {
      ret.values[plainIdx] =
          timestamp_extract(timestampScalar->value, subfield.valptrs[plainIdx],
                            subfield.lengths[plainIdx]);
    };
    dbcommon::transformVector(ret.plainSize, ret.sel, ret.nulls, extract);
  }
  return params[0];
}

Datum extract_subfield_val_timestamp_vec(Datum *params, uint64_t size) {
  Vector *retVector = params[0];
  Scalar *subfieldScalar = params[1];
  Vector *timestampVector = params[2];

  TimestampVectorRawData timestamp(timestampVector);
  retVector->resize(timestamp.plainSize, timestamp.sel, timestamp.nulls);
  FixedSizeTypeVectorRawData<double> ret(retVector);

  uint32_t validLen = subfieldScalar->length <= TIMESTAMP_FIELD_MAXLEN
                          ? subfieldScalar->length
                          : TIMESTAMP_FIELD_MAXLEN;
  std::string field =
      std::string(DatumGetValue<const char *>(subfieldScalar->value), validLen);

  switch (DatetimeTable::getDatetimeKindByName(field)) {
    case EPOCH: {
      auto doExtractEpoch = [&](uint64_t plainIdx) {
        ret.values[plainIdx] = TimestampType::extractEpoch(
            timestamp.seconds[plainIdx], timestamp.nanoseconds[plainIdx]);
      };
      dbcommon::transformVector(ret.plainSize, ret.sel, ret.nulls,
                                doExtractEpoch);
      break;
    }
    case MILLENNIUM: {
      auto doExtractMillennium = [&](uint64_t plainIdx) {
        int32_t year, month, day, millennium;
        TimestampType::extractDate(timestamp.seconds[plainIdx], &year, &month,
                                   &day);
        if (year > 0)
          millennium = (year + 999) / 1000;
        else
          millennium = -((999 - (year - 1)) / 1000);
        ret.values[plainIdx] = static_cast<double>(millennium);
      };
      dbcommon::transformVector(ret.plainSize, ret.sel, ret.nulls,
                                doExtractMillennium);
      break;
    }
    case CENTURY: {
      auto doExtractCentury = [&](uint64_t plainIdx) {
        int32_t year, month, day, century;
        TimestampType::extractDate(timestamp.seconds[plainIdx], &year, &month,
                                   &day);
        if (year > 0)
          century = (year + 99) / 100;
        else
          century = -((99 - (year - 1)) / 100);
        ret.values[plainIdx] = static_cast<double>(century);
      };
      dbcommon::transformVector(ret.plainSize, ret.sel, ret.nulls,
                                doExtractCentury);
      break;
    }
    case DECADE: {
      auto doExtractDecade = [&](uint64_t plainIdx) {
        int32_t year, month, day, decade;
        TimestampType::extractDate(timestamp.seconds[plainIdx], &year, &month,
                                   &day);
        if (year >= 0)
          decade = year / 10;
        else
          decade = -((8 - (year - 1)) / 10);
        ret.values[plainIdx] = static_cast<double>(decade);
      };
      dbcommon::transformVector(ret.plainSize, ret.sel, ret.nulls,
                                doExtractDecade);
      break;
    }
    case YEAR: {
      auto doExtractYear = [&](uint64_t plainIdx) {
        int32_t year, month, day;
        TimestampType::extractDate(timestamp.seconds[plainIdx], &year, &month,
                                   &day);
        if (year <= 0) year--;
        ret.values[plainIdx] = static_cast<double>(year);
      };
      dbcommon::transformVector(ret.plainSize, ret.sel, ret.nulls,
                                doExtractYear);
      break;
    }
    case QUARTER: {
      auto doExtractQuarter = [&](uint64_t plainIdx) {
        int32_t year, month, day, quarter;
        TimestampType::extractDate(timestamp.seconds[plainIdx], &year, &month,
                                   &day);
        quarter = (month - 1) / 3 + 1;
        ret.values[plainIdx] = static_cast<double>(quarter);
      };
      dbcommon::transformVector(ret.plainSize, ret.sel, ret.nulls,
                                doExtractQuarter);
      break;
    }
    case MONTH: {
      auto doExtractMonth = [&](uint64_t plainIdx) {
        int32_t year, month, day;
        TimestampType::extractDate(timestamp.seconds[plainIdx], &year, &month,
                                   &day);
        ret.values[plainIdx] = static_cast<double>(month);
      };
      dbcommon::transformVector(ret.plainSize, ret.sel, ret.nulls,
                                doExtractMonth);
      break;
    }
    case WEEK: {
      auto doExtractWeek = [&](uint64_t plainIdx) {
        int32_t year, month, day;
        TimestampType::extractDate(timestamp.seconds[plainIdx], &year, &month,
                                   &day);
        if (year <= 0) year--;
        double week = TimestampType::countWeek(year, month, day);
        ret.values[plainIdx] = static_cast<double>(week);
      };
      dbcommon::transformVector(ret.plainSize, ret.sel, ret.nulls,
                                doExtractWeek);
      break;
    }
    case DAY: {
      auto doExtractDay = [&](uint64_t plainIdx) {
        int32_t year, month, day;
        TimestampType::extractDate(timestamp.seconds[plainIdx], &year, &month,
                                   &day);
        ret.values[plainIdx] = static_cast<double>(day);
      };
      dbcommon::transformVector(ret.plainSize, ret.sel, ret.nulls,
                                doExtractDay);
      break;
    }
    case DOW: {
      auto doExtractDow = [&](uint64_t plainIdx) {
        ret.values[plainIdx] =
            TimestampType::extractDow(timestamp.seconds[plainIdx]);
      };
      dbcommon::transformVector(ret.plainSize, ret.sel, ret.nulls,
                                doExtractDow);
      break;
    }
    case DOY: {
      auto doExtractDoy = [&](uint64_t plainIdx) {
        ret.values[plainIdx] =
            TimestampType::extractDoy(timestamp.seconds[plainIdx]);
      };
      dbcommon::transformVector(ret.plainSize, ret.sel, ret.nulls,
                                doExtractDoy);
      break;
    }
    case HOUR: {
      auto doExtractHour = [&](uint64_t plainIdx) {
        int32_t hour2, minute2, second2;
        TimestampType::extractTime(timestamp.seconds[plainIdx], &hour2,
                                   &minute2, &second2);
        ret.values[plainIdx] = static_cast<double>(hour2);
      };
      dbcommon::transformVector(ret.plainSize, ret.sel, ret.nulls,
                                doExtractHour);
      break;
    }
    case MINUTE: {
      auto doExtractMinute = [&](uint64_t plainIdx) {
        int32_t hour2, minute2, second2;
        TimestampType::extractTime(timestamp.seconds[plainIdx], &hour2,
                                   &minute2, &second2);
        ret.values[plainIdx] = static_cast<double>(minute2);
      };
      dbcommon::transformVector(ret.plainSize, ret.sel, ret.nulls,
                                doExtractMinute);
      break;
    }
    case SECOND: {
      auto doExtractSecond = [&](uint64_t plainIdx) {
        int32_t hour2, minute2, second2;
        TimestampType::extractTime(timestamp.seconds[plainIdx], &hour2,
                                   &minute2, &second2);
        double second_new =
            static_cast<double>(second2) +
            static_cast<double>(timestamp.nanoseconds[plainIdx]) / 1000000000.0;
        ret.values[plainIdx] = static_cast<double>(second_new);
      };
      dbcommon::transformVector(ret.plainSize, ret.sel, ret.nulls,
                                doExtractSecond);
      break;
    }
    case MILLISEC: {
      auto doExtractMillisec = [&](uint64_t plainIdx) {
        int32_t hour2, minute2, second2;
        TimestampType::extractTime(timestamp.seconds[plainIdx], &hour2,
                                   &minute2, &second2);
        double milliseconds =
            static_cast<double>(second2) * 1000 +
            static_cast<double>(timestamp.nanoseconds[plainIdx]) / 1000000.0;
        ret.values[plainIdx] = static_cast<double>(milliseconds);
      };
      dbcommon::transformVector(ret.plainSize, ret.sel, ret.nulls,
                                doExtractMillisec);
      break;
    }
    case MICROSEC: {
      auto doExtractMicrosec = [&](uint64_t plainIdx) {
        int32_t hour2, minute2, second2;
        TimestampType::extractTime(timestamp.seconds[plainIdx], &hour2,
                                   &minute2, &second2);
        double microseconds =
            static_cast<double>(second2) * 1000000 +
            static_cast<double>(timestamp.nanoseconds[plainIdx]) / 1000.0;
        ret.values[plainIdx] = static_cast<double>(microseconds);
      };
      dbcommon::transformVector(ret.plainSize, ret.sel, ret.nulls,
                                doExtractMicrosec);
      break;
    }
    default:
      LOG_ERROR(ERRCODE_FEATURE_NOT_SUPPORTED,
                "timestamp units \"%s\" not recognized", field.c_str());
  }

  return params[0];
}

Datum extract_subfield_val_timestamp_val(Datum *params, uint64_t size) {
  Scalar *retScalar = params[0];
  Scalar *subfieldScalar = params[1];
  Scalar *timestampScalar = params[2];

  if (timestampScalar->isnull || subfieldScalar->isnull) {
    retScalar->isnull = true;
  } else {
    retScalar->isnull = false;
    retScalar->value = CreateDatum<double>(timestamp_extract(
        timestampScalar->value, subfieldScalar->value, subfieldScalar->length));
  }
  return params[0];
}

Datum timestamp_date_part(Datum *params, uint64_t size) {
  assert(size == 3);

  return type1_op_type2_bind<
      extract_subfield_vec_timestamp_vec, extract_subfield_vec_timestamp_val,
      extract_subfield_val_timestamp_vec, extract_subfield_val_timestamp_val>(
      params, size);
}

Timestamp inline timestamp_trunc(Timestamp *timestamp, const char *strVal,
                                 uint32_t length) {
  Timestamp ret;
  uint32_t validLen =
      length <= TIMESTAMP_FIELD_MAXLEN ? length : TIMESTAMP_FIELD_MAXLEN;
  std::string field(strVal, validLen);

  switch (DatetimeTable::getDatetimeKindByName(field)) {
    case MILLENNIUM:
      ret = TimestampType::truncMillennium(timestamp->second);
      break;
    case CENTURY:
      ret = TimestampType::truncCentury(timestamp->second);
      break;
    case DECADE:
      ret = TimestampType::truncDecade(timestamp->second);
      break;
    case YEAR:
      ret = TimestampType::truncYear(timestamp->second);
      break;
    case QUARTER:
      ret = TimestampType::truncQuarter(timestamp->second);
      break;
    case MONTH:
      ret = TimestampType::truncMonth(timestamp->second);
      break;
    case WEEK:
      ret = TimestampType::truncWeek(timestamp->second);
      break;
    case DAY:
      ret = TimestampType::truncDay(timestamp->second);
      break;
    case HOUR:
      ret = TimestampType::truncHour(timestamp->second);
      break;
    case MINUTE:
      ret = TimestampType::truncMinute(timestamp->second);
      break;
    case SECOND:
      ret = TimestampType::truncSecond(timestamp->second);
      break;
    case MILLISEC:
      ret = TimestampType::truncMilliseconds(timestamp->second,
                                             timestamp->nanosecond);
      break;
    case MICROSEC:
      ret = TimestampType::truncMicroseconds(timestamp->second,
                                             timestamp->nanosecond);
      break;
    default:
      LOG_ERROR(ERRCODE_FEATURE_NOT_SUPPORTED,
                "timestamp units \"%s\" not recognized", field.c_str());
  }
  return ret;
}
inline int32_t truncYear(int64_t second) {
  int32_t days = (int32_t)(second / SECONDS_PER_DAY);
  if (second < 0) days -= 1;
  int32_t jd = days + UNIX_EPOCH_JDATE;
  uint32_t julian;
  uint32_t quad;
  uint32_t extra;
  int32_t y, year;

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
  year = y - 4800;
  return year;
}

inline std::tuple<int32_t, int32_t> truncMonth(int64_t second) {
  int32_t days = (int32_t)(second / SECONDS_PER_DAY);
  if (second < 0) days -= 1;
  int32_t jd = days + UNIX_EPOCH_JDATE;
  uint32_t julian;
  uint32_t quad;
  uint32_t extra;
  int32_t y, year, month;

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
  year = y - 4800;
  quad = julian * 2141 / 65536;
  month = (quad + 10) % 12 + 1;
  return std::make_tuple(year, month);
}
inline std::tuple<int32_t, int32_t, int32_t> truncDay(int64_t second) {
  int32_t days = (int32_t)(second / SECONDS_PER_DAY);
  if (second < 0) days -= 1;
  int32_t jd = days + UNIX_EPOCH_JDATE;
  uint32_t julian;
  uint32_t quad;
  uint32_t extra;
  int32_t y, year, month, day;

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
  year = y - 4800;
  quad = julian * 2141 / 65536;
  month = (quad + 10) % 12 + 1;
  day = julian - 7834 * quad / 256;
  return std::make_tuple(year, month, day);
}
inline int32_t truncHour(int64_t second) {
  int32_t hour;
  int32_t second_rest = (int32_t)(second % SECONDS_PER_DAY);
  hour = second_rest / 3600;
  if (hour < 0) hour += 23;
  return hour;
}

inline std::tuple<int32_t, int32_t> truncMinute(int64_t second) {
  int32_t hour, minute;
  int32_t second_rest = (int32_t)(second % SECONDS_PER_DAY);
  hour = second_rest / 3600;
  minute = (second_rest - (hour)*3600) / 60;
  if (minute < 0) minute += 59;
  if (hour < 0) hour += 23;
  return std::make_tuple(hour, minute);
}

inline std::tuple<int32_t, int32_t, int32_t> truncSecond(int64_t second) {
  int32_t hour, minute, second2;
  int32_t second_rest = (int32_t)(second % SECONDS_PER_DAY);
  hour = second_rest / 3600;
  minute = (second_rest - (hour)*3600) / 60;
  second2 = second_rest - (hour)*3600 - (minute)*60;
  if (minute < 0) minute += 59;
  if (hour < 0) hour += 23;
  if (second2 < 0) second2 += 60;
  return std::make_tuple(hour, minute, second2);
}

Datum trunc_subfield_vec_timestamp_vec(Datum *params, uint64_t size) {
  Vector *retVector = params[0];
  Vector *subfieldVector = params[1];
  Vector *timestampVector = params[2];

  subfieldVector->trim();
  TimestampVectorRawData timestamp(timestampVector);
  VariableSizeTypeVectorRawData subfield(subfieldVector);
  retVector->resize(timestamp.plainSize, timestamp.sel, timestamp.nulls,
                    subfield.nulls);
  TimestampVectorRawData ret(retVector);

  uint64_t bufferSize = 0;
  auto trunc = [&](uint64_t plainIdx) {
    Timestamp ts(timestamp.seconds[plainIdx], timestamp.nanoseconds[plainIdx]);
    Timestamp result = timestamp_trunc(&ts, subfield.valptrs[plainIdx],
                                       subfield.lengths[plainIdx]);
    ret.seconds[plainIdx] = result.second;
    ret.nanoseconds[plainIdx] = result.nanosecond;
  };
  dbcommon::transformVector(ret.plainSize, ret.sel, ret.nulls, trunc);

  return params[0];
}

Datum trunc_subfield_vec_timestamp_val(Datum *params, uint64_t size) {
  Vector *retVector = params[0];
  Vector *subfieldVector = params[1];
  Scalar *timestampScalar = params[2];

  if (timestampScalar->isnull) {
    subfieldVector->trim();
    VariableSizeTypeVectorRawData subfield(subfieldVector);
    retVector->resize(subfield.plainSize, subfield.sel, true);
  } else {
    subfieldVector->trim();
    VariableSizeTypeVectorRawData subfield(subfieldVector);
    retVector->resize(subfield.plainSize, subfield.sel, subfield.nulls);
    TimestampVectorRawData ret(retVector);

    uint64_t bufferSize = 0;
    auto trunc = [&](uint64_t plainIdx) {
      Timestamp result =
          timestamp_trunc(timestampScalar->value, subfield.valptrs[plainIdx],
                          subfield.lengths[plainIdx]);
      ret.seconds[plainIdx] = result.second;
      ret.nanoseconds[plainIdx] = result.nanosecond;
    };
    dbcommon::transformVector(ret.plainSize, ret.sel, ret.nulls, trunc);
  }
  return params[0];
}

Datum trunc_subfield_val_timestamp_vec(Datum *params, uint64_t size) {
  Vector *retVector = params[0];
  Scalar *subfieldScalar = params[1];
  Vector *timestampVector = params[2];

  TimestampVectorRawData timestamp(timestampVector);
  retVector->resize(timestamp.plainSize, timestamp.sel, timestamp.nulls);
  TimestampVectorRawData ret(retVector);

  uint32_t validLen = subfieldScalar->length <= TIMESTAMP_FIELD_MAXLEN
                          ? subfieldScalar->length
                          : TIMESTAMP_FIELD_MAXLEN;
  std::string field =
      std::string(DatumGetValue<const char *>(subfieldScalar->value), validLen);

  switch (DatetimeTable::getDatetimeKindByName(field)) {
    case MILLENNIUM: {
      auto trunc = [&](uint64_t plainIdx) {
        int32_t year;
        Timestamp result;
        year = truncYear(timestamp.seconds[plainIdx]);
        if (year > 0)
          year = ((year + 999) / 1000) * 1000 - 999;
        else
          year = -((999 - (year - 1)) / 1000) * 1000 + 1;
        TimestampType::timestamp_trunc(&result, year, 1, 1, 0, 0, 0, 0);
        ret.seconds[plainIdx] = result.second;
        ret.nanoseconds[plainIdx] = result.nanosecond;
      };
      dbcommon::transformVector(ret.plainSize, ret.sel, ret.nulls, trunc);
      break;
    }
    case CENTURY: {
      auto trunc = [&](uint64_t plainIdx) {
        int32_t year;
        Timestamp result;
        year = truncYear(timestamp.seconds[plainIdx]);
        if (year > 0)
          year = ((year + 99) / 100) * 100 - 99;
        else
          year = -((99 - (year - 1)) / 100) * 100 + 1;
        TimestampType::timestamp_trunc(&result, year, 1, 1, 0, 0, 0, 0);
        ret.seconds[plainIdx] = result.second;
        ret.nanoseconds[plainIdx] = result.nanosecond;
      };
      dbcommon::transformVector(ret.plainSize, ret.sel, ret.nulls, trunc);
      break;
    }
    case DECADE: {
      auto trunc = [&](uint64_t plainIdx) {
        int32_t year;
        Timestamp result;
        year = truncYear(timestamp.seconds[plainIdx]);
        if (year > 0)
          year = (year / 10) * 10;
        else
          year = -((8 - (year - 1)) / 10) * 10;
        TimestampType::timestamp_trunc(&result, year, 1, 1, 0, 0, 0, 0);
        ret.seconds[plainIdx] = result.second;
        ret.nanoseconds[plainIdx] = result.nanosecond;
      };
      dbcommon::transformVector(ret.plainSize, ret.sel, ret.nulls, trunc);
      break;
    }
    case YEAR: {
      auto trunc = [&](uint64_t plainIdx) {
        int32_t year;
        Timestamp result;
        year = truncYear(timestamp.seconds[plainIdx]);
        TimestampType::timestamp_trunc(&result, year, 1, 1, 0, 0, 0, 0);
        ret.seconds[plainIdx] = result.second;
        ret.nanoseconds[plainIdx] = result.nanosecond;
      };
      dbcommon::transformVector(ret.plainSize, ret.sel, ret.nulls, trunc);
      break;
    }
    case QUARTER: {
      auto trunc = [&](uint64_t plainIdx) {
        int32_t year, month;
        Timestamp result;
        std::tie(year, month) = truncMonth(timestamp.seconds[plainIdx]);
        month = (3 * ((month - 1) / 3)) + 1;
        TimestampType::timestamp_trunc(&result, year, month, 1, 0, 0, 0, 0);
        ret.seconds[plainIdx] = result.second;
        ret.nanoseconds[plainIdx] = result.nanosecond;
      };
      dbcommon::transformVector(ret.plainSize, ret.sel, ret.nulls, trunc);
      break;
    }
    case MONTH: {
      auto trunc = [&](uint64_t plainIdx) {
        int32_t year, month;
        Timestamp result;
        std::tie(year, month) = truncMonth(timestamp.seconds[plainIdx]);
        TimestampType::timestamp_trunc(&result, year, month, 1, 0, 0, 0, 0);
        ret.seconds[plainIdx] = result.second;
        ret.nanoseconds[plainIdx] = result.nanosecond;
      };
      dbcommon::transformVector(ret.plainSize, ret.sel, ret.nulls, trunc);
      break;
    }
    case WEEK: {
      auto trunc = [&](uint64_t plainIdx) {
        Timestamp result;
        result = TimestampType::truncWeek(timestamp.seconds[plainIdx]);
        ret.seconds[plainIdx] = result.second;
        ret.nanoseconds[plainIdx] = result.nanosecond;
      };
      dbcommon::transformVector(ret.plainSize, ret.sel, ret.nulls, trunc);
      break;
    }
    case DAY: {
      auto trunc = [&](uint64_t plainIdx) {
        int32_t year, month, day;
        Timestamp result;
        std::tie(year, month, day) = truncDay(timestamp.seconds[plainIdx]);
        TimestampType::timestamp_trunc(&result, year, month, day, 0, 0, 0, 0);
        ret.seconds[plainIdx] = result.second;
        ret.nanoseconds[plainIdx] = result.nanosecond;
      };
      dbcommon::transformVector(ret.plainSize, ret.sel, ret.nulls, trunc);
      break;
    }
    case HOUR: {
      auto trunc = [&](uint64_t plainIdx) {
        int32_t year, month, day, hour2;
        Timestamp result;
        std::tie(year, month, day) = truncDay(timestamp.seconds[plainIdx]);
        hour2 = truncHour(timestamp.seconds[plainIdx]);
        TimestampType::timestamp_trunc(&result, year, month, day, hour2, 0, 0,
                                       0);
        ret.seconds[plainIdx] = result.second;
        ret.nanoseconds[plainIdx] = result.nanosecond;
      };
      dbcommon::transformVector(ret.plainSize, ret.sel, ret.nulls, trunc);
      break;
    }
    case MINUTE: {
      auto trunc = [&](uint64_t plainIdx) {
        int32_t year, month, day, hour2, minute2;
        Timestamp result;
        std::tie(year, month, day) = truncDay(timestamp.seconds[plainIdx]);
        std::tie(hour2, minute2) = truncMinute(timestamp.seconds[plainIdx]);
        TimestampType::timestamp_trunc(&result, year, month, day, hour2,
                                       minute2, 0, 0);
        ret.seconds[plainIdx] = result.second;
        ret.nanoseconds[plainIdx] = result.nanosecond;
      };
      dbcommon::transformVector(ret.plainSize, ret.sel, ret.nulls, trunc);
      break;
    }
    case SECOND: {
      auto trunc = [&](uint64_t plainIdx) {
        int32_t year, month, day, hour2, minute2, second2;
        Timestamp result;
        std::tie(year, month, day) = truncDay(timestamp.seconds[plainIdx]);
        std::tie(hour2, minute2, second2) =
            truncSecond(timestamp.seconds[plainIdx]);
        TimestampType::timestamp_trunc(&result, year, month, day, hour2,
                                       minute2, second2, 0);
        ret.seconds[plainIdx] = result.second;
        ret.nanoseconds[plainIdx] = result.nanosecond;
      };
      dbcommon::transformVector(ret.plainSize, ret.sel, ret.nulls, trunc);
      break;
    }
    case MILLISEC: {
      auto trunc = [&](uint64_t plainIdx) {
        int32_t year, month, day, hour2, minute2, second2;
        int64_t nano;
        Timestamp result;
        std::tie(year, month, day) = truncDay(timestamp.seconds[plainIdx]);
        std::tie(hour2, minute2, second2) =
            truncSecond(timestamp.seconds[plainIdx]);
        nano = (timestamp.nanoseconds[plainIdx] / 1000000) * 1000000;
        TimestampType::timestamp_trunc(&result, year, month, day, hour2,
                                       minute2, second2, nano);
        ret.seconds[plainIdx] = result.second;
        ret.nanoseconds[plainIdx] = result.nanosecond;
      };
      dbcommon::transformVector(ret.plainSize, ret.sel, ret.nulls, trunc);
      break;
    }
    case MICROSEC: {
      auto trunc = [&](uint64_t plainIdx) {
        int32_t year, month, day, hour2, minute2, second2;
        int64_t nano;
        Timestamp result;
        std::tie(year, month, day) = truncDay(timestamp.seconds[plainIdx]);
        std::tie(hour2, minute2, second2) =
            truncSecond(timestamp.seconds[plainIdx]);
        nano = (timestamp.nanoseconds[plainIdx] / 1000) * 1000;
        TimestampType::timestamp_trunc(&result, year, month, day, hour2,
                                       minute2, second2, nano);
        ret.seconds[plainIdx] = result.second;
        ret.nanoseconds[plainIdx] = result.nanosecond;
      };
      dbcommon::transformVector(ret.plainSize, ret.sel, ret.nulls, trunc);
      break;
    }
    default:
      LOG_ERROR(ERRCODE_FEATURE_NOT_SUPPORTED,
                "timestamp units \"%s\" not recognized", field.c_str());
  }
  return params[0];
}
Datum trunc_subfield_val_timestamp_val(Datum *params, uint64_t size) {
  Scalar *retScalar = params[0];
  Scalar *subfieldScalar = params[1];
  Scalar *timestampScalar = params[2];

  if (timestampScalar->isnull || subfieldScalar->isnull) {
    retScalar->isnull = true;
  } else {
    retScalar->isnull = false;
    Timestamp *ret = retScalar->allocateValue<Timestamp>();
    Timestamp result = timestamp_trunc(
        timestampScalar->value, subfieldScalar->value, subfieldScalar->length);
    ret->second = static_cast<int64_t>(result.second);
    ret->nanosecond = static_cast<int64_t>(result.nanosecond);
  }
  return params[0];
}

Datum timestamp_date_trunc(Datum *params, uint64_t size) {
  assert(size == 3);

  return type1_op_type2_bind<
      trunc_subfield_vec_timestamp_vec, trunc_subfield_vec_timestamp_val,
      trunc_subfield_val_timestamp_vec, trunc_subfield_val_timestamp_val>(
      params, size);
}

Datum timestamp_to_text(Datum *params, uint64_t size) {
  assert(size == 2);
  Object *para = params[1];
  if (dynamic_cast<Vector *>(para)) {
    Vector *retVector = params[0];
    Vector *srcVector = params[1];

    srcVector->trim();
    TimestampVectorRawData src(srcVector);
    retVector->resize(src.plainSize, src.sel, src.nulls);
    VariableSizeTypeVectorRawData ret(retVector);

    auto retBuffer = retVector->getValueBuffer();
    auto text = [&](uint64_t plainIdx) {
      std::string str = TimestampType::timestamp2string(
          src.seconds[plainIdx], src.nanoseconds[plainIdx]);
      ret.lengths[plainIdx] = str.length();
      retBuffer->append(str.data(), ret.lengths[plainIdx]);
    };
    dbcommon::transformVector(ret.plainSize, ret.sel, ret.nulls, text);
    retVector->computeValPtrs();
  } else {
    Scalar *retScalar = params[0];
    Scalar *srcScalar = params[1];
    if (srcScalar->isnull) {
      retScalar->isnull = true;
    } else {
      retScalar->isnull = false;
      Timestamp *src = srcScalar->value;
      std::string str =
          TimestampType::timestamp2string(src->second, src->nanosecond);
      auto ret_length = str.length();
      char *ret = retScalar->allocateValue(ret_length);
      strncpy(ret, str.data(), ret_length);
    }
  }
  return params[0];
}

Timestamp timestampAddDay(Timestamp timestamp, int32_t day) {
  Timestamp ret = Timestamp(0, 0);
  if (day == 0) return ret;

  int32_t tsYear, tsMonth, tsDay;
  TimestampType::extractDate(timestamp.second, &tsYear, &tsMonth, &tsDay);
  int64_t secRest = timestamp.second % SECONDS_PER_DAY;
  int32_t jDate = TimestampType::date2j(tsYear, tsMonth, tsDay) + day;
  ret.second = (jDate - UNIX_EPOCH_JDATE) * SECONDS_PER_DAY;
  ret.second += secRest < 0 ? SECONDS_PER_DAY + secRest : secRest;
  ret.nanosecond = timestamp.nanosecond;
  return ret;
}

Timestamp timestampAddMonth(Timestamp timestamp, int32_t month) {
  Timestamp ret = Timestamp(0, 0);
  if (month == 0) return ret;

  int32_t tsYear, tsMonth, tsDay;
  TimestampType::extractDate(timestamp.second, &tsYear, &tsMonth, &tsDay);
  int64_t secondRest = timestamp.second % SECONDS_PER_DAY;
  tsMonth += month;
  if (tsMonth > MONTHS_PER_YEAR) {
    tsYear += (tsMonth - 1) / MONTHS_PER_YEAR;
    tsMonth = ((tsMonth - 1) % MONTHS_PER_YEAR) + 1;
  } else if (tsMonth < 1) {
    tsYear += tsMonth / MONTHS_PER_YEAR - 1;
    tsMonth = tsMonth % MONTHS_PER_YEAR + MONTHS_PER_YEAR;
  }

  if (tsDay > day_tab[isLeapYear(tsYear)][tsMonth - 1])
    tsDay = (day_tab[isLeapYear(tsYear)][tsMonth - 1]);

  int32_t retDate =
      TimestampType::date2j(tsYear, tsMonth, tsDay) - UNIX_EPOCH_JDATE;
  ret.second += retDate * SECONDS_PER_DAY;
  ret.second += secondRest < 0 ? SECONDS_PER_DAY + secondRest : secondRest;
  // TODO(wshao) : add overflow check here.
  ret.nanosecond = timestamp.nanosecond;
  return ret;
}

IntervalVar intervalJustifyDays(IntervalVar interval) {
  IntervalVar ret = interval;
  int32_t monthNum = interval.day / INTERVAL_DAYS_PER_MONTH;
  ret.day -= monthNum * INTERVAL_DAYS_PER_MONTH;
  ret.month += monthNum;
  if (ret.month > 0 && ret.day < 0) {
    ret.day += INTERVAL_DAYS_PER_MONTH;
    ret.month--;
  } else if (ret.month < 0 && ret.day > 0) {
    ret.day -= INTERVAL_DAYS_PER_MONTH;
    ret.month++;
  }

  return ret;
}

IntervalVar intervalJustifyHours(IntervalVar interval) {
  IntervalVar ret = interval;
  int64_t dayNum = interval.timeOffset / USECS_PER_DAY;
  if (dayNum != 0) ret.timeOffset -= dayNum * USECS_PER_DAY;
  ret.day += dayNum;

  if (ret.day > 0 && ret.timeOffset < 0) {
    ret.timeOffset += USECS_PER_DAY;
    ret.day--;
  } else if (ret.day < 0 && ret.timeOffset > 0) {
    ret.timeOffset -= USECS_PER_DAY;
    ret.day++;
  }

  return ret;
}

Datum interval_add_interval(Datum *params, uint64_t size) {
  auto itvPlusItvFunc = [](ByteBuffer &buf, IntervalVar val1,
                           IntervalVar val2) { return val1 + val2; };
  return two_params_bind<IntervalVar, IntervalVar, IntervalVar>(params, size,
                                                                itvPlusItvFunc);
}

Datum interval_sub_interval(Datum *params, uint64_t size) {
  auto itvMinusItvFunc = [](ByteBuffer &buf, IntervalVar val1,
                            IntervalVar val2) { return val1 - val2; };
  return two_params_bind<IntervalVar, IntervalVar, IntervalVar>(
      params, size, itvMinusItvFunc);
}

Datum interval_mul_interval(Datum *params, uint64_t size) {}

Datum interval_div_interval(Datum *params, uint64_t size) {}

Datum time_sub_time(Datum *params, uint64_t size) {
  auto tmMinusTmFunc = [](ByteBuffer &buf, int64_t val1, int64_t val2) {
    IntervalVar ret = IntervalVar(val1 - val2, 0, 0);
    return ret;
  };

  return two_params_bind<IntervalVar, int64_t, int64_t>(params, size,
                                                        tmMinusTmFunc);
}

Datum timestamp_sub_timestamp(Datum *params, uint64_t size) {
  auto tsMinusTsFunc = [](ByteBuffer &buf, Timestamp val1, Timestamp val2) {
    if (TimestampType::IsTimestampPosFinite(val1) ||
        TimestampType::IsTimestampNegFinite(val1) ||
        TimestampType::IsTimestampPosFinite(val2) ||
        TimestampType::IsTimestampNegFinite(val2))
      LOG_ERROR(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE,
                "cannot subtract infinite timestamps");

    int64_t microsecRet = (val1.second - val2.second) * USECS_PER_SEC;
    microsecRet += (val1.nanosecond - val2.nanosecond) / 1000;
    IntervalVar interval = IntervalVar(microsecRet, 0, 0);

    return intervalJustifyHours(interval);
  };

  return two_params_bind<IntervalVar, Timestamp, Timestamp>(params, size,
                                                            tsMinusTsFunc);
}

Datum date_add_interval(Datum *params, uint64_t size) {
  auto datePlusItvFunc = [](ByteBuffer &buf, int32_t val1, IntervalVar val2) {
    Timestamp ret = Timestamp(val1 * SECONDS_PER_DAY, 0);
    if (val2.month != 0) {
      ret = timestampAddMonth(ret, val2.month);
    }
    if (val2.day != 0) {
      ret = timestampAddDay(ret, val2.day);
    }
    int64_t secondPart = val2.timeOffset / USECS_PER_SEC;
    int64_t microPart = val2.timeOffset % USECS_PER_SEC;
    ret.second += secondPart;
    ret.nanosecond += microPart * 1000;
    return ret;
  };

  return two_params_bind<Timestamp, int32_t, IntervalVar>(params, size,
                                                          datePlusItvFunc);
}

Datum date_sub_interval(Datum *params, uint64_t size) {
  auto dateMinusItvFunc = [](ByteBuffer &buf, int32_t val1, IntervalVar val2) {
    Timestamp ret = Timestamp(val1 * SECONDS_PER_DAY, 0);
    IntervalVar nval(-val2.timeOffset, -val2.day, -val2.month);
    if (nval.month != 0) {
      ret = timestampAddMonth(ret, nval.month);
    }
    if (nval.day != 0) {
      ret = timestampAddDay(ret, nval.day);
    }
    int64_t secondPart = nval.timeOffset / USECS_PER_SEC;
    int64_t microPart = nval.timeOffset % USECS_PER_SEC;
    ret.second += secondPart;
    ret.nanosecond += microPart * 1000;
    return ret;
  };

  return two_params_bind<Timestamp, int32_t, IntervalVar>(params, size,
                                                          dateMinusItvFunc);
}

Datum time_add_interval(Datum *params, uint64_t size) {
  auto tmPlusItvFunc = [](ByteBuffer &buf, int64_t val1, IntervalVar val2) {
    int64_t ret = val1 + val2.timeOffset;
    ret -= ret / USECS_PER_DAY * USECS_PER_DAY;
    if (ret < 0) ret += USECS_PER_DAY;
    return ret;
  };
  return two_params_bind<int64_t, int64_t, IntervalVar>(params, size,
                                                        tmPlusItvFunc);
}

Datum time_sub_interval(Datum *params, uint64_t size) {
  auto tmMinusItvFunc = [](ByteBuffer &buf, int64_t val1, IntervalVar val2) {
    int64_t ret = val1 - val2.timeOffset;
    ret -= ret / USECS_PER_DAY * USECS_PER_DAY;
    if (ret < 0) ret += USECS_PER_DAY;
    return ret;
  };
  return two_params_bind<int64_t, int64_t, IntervalVar>(params, size,
                                                        tmMinusItvFunc);
}

Datum timestamp_add_interval(Datum *params, uint64_t size) {
  auto tsPlusItvFunc = [](ByteBuffer &buf, Timestamp val1, IntervalVar val2) {
    Timestamp ret = Timestamp(val1.second, val1.nanosecond);
    if (TimestampType::IsTimestampPosFinite(val1) ||
        TimestampType::IsTimestampNegFinite(val1)) {
      return ret;
    }
    if (val2.month != 0) {
      ret = timestampAddMonth(ret, val2.month);
    }
    if (val2.day != 0) {
      ret = timestampAddDay(ret, val2.day);
    }
    int64_t secondPart = val2.timeOffset / USECS_PER_SEC;
    int64_t microPart = val2.timeOffset % USECS_PER_SEC;
    ret.second += secondPart;
    ret.nanosecond += microPart * 1000;
    return ret;
  };

  return two_params_bind<Timestamp, Timestamp, IntervalVar>(params, size,
                                                            tsPlusItvFunc);
}

Datum timestamp_sub_interval(Datum *params, uint64_t size) {
  auto tsMinusItvFunc = [](ByteBuffer &buf, Timestamp val1, IntervalVar val2) {
    Timestamp ret = Timestamp(val1.second, val1.nanosecond);
    if (TimestampType::IsTimestampPosFinite(val1) ||
        TimestampType::IsTimestampNegFinite(val1)) {
      return ret;
    }
    IntervalVar nval(-val2.timeOffset, -val2.day, -val2.month);
    if (nval.month != 0) {
      ret = timestampAddMonth(ret, nval.month);
    }
    if (nval.day != 0) {
      ret = timestampAddDay(ret, nval.day);
    }
    int64_t secondPart = nval.timeOffset / USECS_PER_SEC;
    int64_t microPart = nval.timeOffset % USECS_PER_SEC;
    ret.second += secondPart;
    ret.nanosecond += microPart * 1000;
    return ret;
  };

  return two_params_bind<Timestamp, Timestamp, IntervalVar>(params, size,
                                                            tsMinusItvFunc);
}

}  // namespace dbcommon
