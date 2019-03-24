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

#include "storage/format/orc/orc-predicates.h"

#include <memory>
#include <sstream>
#include <utility>

#include "dbcommon/log/logger.h"
#include "dbcommon/type/decimal.h"
#include "dbcommon/utils/string-util.h"
#include "univplan/common/plannode-walker.h"

#include "storage/common/bloom-filter.h"
#include "storage/format/orc/reader.h"

namespace orc {

bool OrcPredicates::hasAllNull(int32_t colId) const {
  disableInvalidColId(colId);
  const Type& child = *reader->getType().getSubtype(colId - 1);
  return stripeStats->getColumnStatistics(child.getColumnId())
                 ->getNumberOfValues() == 0 &&
         stripeStats->getColumnStatistics(child.getColumnId())->hasNull();
}

bool OrcPredicates::hasNull(int32_t colId) const {
  disableInvalidColId(colId);
  const Type& child = *reader->getType().getSubtype(colId - 1);
  return stripeStats->getColumnStatistics(child.getColumnId())->hasNull();
}

univplan::PredicateStats OrcPredicates::getMinMax(int32_t colId) const {
  disableInvalidColId(colId);
  dbcommon::Timestamp ts1, ts2;
  return getMinMax(colId, &ts1, &ts2);
}

univplan::PredicateStats OrcPredicates::getMinMax(
    int32_t colId, dbcommon::Timestamp* minTimestamp,
    dbcommon::Timestamp* maxTimestamp) const {
  disableInvalidColId(colId);
  const Type& child = *reader->getType().getSubtype(colId - 1);
  const univplan::ColumnStatistics* stats =
      stripeStats->getColumnStatistics(child.getColumnId());
  dbcommon::TypeKind type = td->getColumnType(colId - 1);
  univplan::PredicateStats ret;
  ret.hasMinMax = true;
  switch (type) {
    case dbcommon::TypeKind::SMALLINTID:
    case dbcommon::TypeKind::INTID:
    case dbcommon::TypeKind::BIGINTID:
    case dbcommon::TypeKind::TIMEID: {
      const IntegerColumnStatisticsImpl* iStat =
          dynamic_cast<const IntegerColumnStatisticsImpl*>(stats);
      if (type == dbcommon::TypeKind::SMALLINTID) {
        ret.minValue = dbcommon::Scalar(
            dbcommon::CreateDatum(static_cast<int16_t>(iStat->getMinimum())),
            false);
        ret.maxValue = dbcommon::Scalar(
            dbcommon::CreateDatum(static_cast<int16_t>(iStat->getMaximum())),
            false);
      } else if (type == dbcommon::TypeKind::INTID ||
                 type == dbcommon::TypeKind::DATEID) {
        ret.minValue = dbcommon::Scalar(
            dbcommon::CreateDatum(static_cast<int32_t>(iStat->getMinimum())),
            false);
        ret.maxValue = dbcommon::Scalar(
            dbcommon::CreateDatum(static_cast<int32_t>(iStat->getMaximum())),
            false);
      } else {
        ret.minValue =
            dbcommon::Scalar(dbcommon::CreateDatum(iStat->getMinimum()), false);
        ret.maxValue =
            dbcommon::Scalar(dbcommon::CreateDatum(iStat->getMaximum()), false);
      }
      break;
    }
    case dbcommon::TypeKind::FLOATID:
    case dbcommon::TypeKind::DOUBLEID: {
      const DoubleColumnStatisticsImpl* dStat =
          dynamic_cast<const DoubleColumnStatisticsImpl*>(stats);
      if (type == dbcommon::TypeKind::FLOATID) {
        ret.minValue = dbcommon::Scalar(
            dbcommon::CreateDatum(static_cast<float>(dStat->getMinimum())),
            false);
        ret.maxValue = dbcommon::Scalar(
            dbcommon::CreateDatum(static_cast<float>(dStat->getMaximum())),
            false);
      } else {
        ret.minValue =
            dbcommon::Scalar(dbcommon::CreateDatum(dStat->getMinimum()), false);
        ret.maxValue =
            dbcommon::Scalar(dbcommon::CreateDatum(dStat->getMaximum()), false);
      }
      break;
    }
    case dbcommon::TypeKind::CHARID: {
      // we need to trim here
      const StringColumnStatisticsImpl* sStat =
          dynamic_cast<const StringColumnStatisticsImpl*>(stats);
      ret.minValue =
          dbcommon::Scalar(dbcommon::CreateDatum(sStat->getMinimum()), false);
      const char* s = sStat->getMinimum();
      uint32_t len = strlen(s);
      while (len != 0 && s[len - 1] == ' ') --len;
      ret.minValue.length = len;
      ret.maxValue =
          dbcommon::Scalar(dbcommon::CreateDatum(sStat->getMaximum()), false);
      s = sStat->getMaximum();
      len = strlen(s);
      while (len != 0 && s[len - 1] == ' ') --len;
      ret.maxValue.length = len;
      break;
    }
    case dbcommon::TypeKind::VARCHARID:
    case dbcommon::TypeKind::STRINGID: {
      const StringColumnStatisticsImpl* sStat =
          dynamic_cast<const StringColumnStatisticsImpl*>(stats);
      ret.minValue =
          dbcommon::Scalar(dbcommon::CreateDatum(sStat->getMinimum()), false);
      ret.minValue.length = strlen(sStat->getMinimum());
      ret.maxValue =
          dbcommon::Scalar(dbcommon::CreateDatum(sStat->getMaximum()), false);
      ret.maxValue.length = strlen(sStat->getMaximum());
      break;
    }
    case dbcommon::TypeKind::BOOLEANID: {
      const BooleanColumnStatisticsImpl* bStat =
          dynamic_cast<const BooleanColumnStatisticsImpl*>(stats);
      ret.minValue = dbcommon::Scalar(
          dbcommon::CreateDatum(bStat->getFalseCount() == 0), false);
      ret.maxValue = dbcommon::Scalar(
          dbcommon::CreateDatum(bStat->getTrueCount() > 0), false);
      break;
    }
    case dbcommon::TypeKind::DATEID: {
      const DateColumnStatisticsImpl* dStat =
          dynamic_cast<const DateColumnStatisticsImpl*>(stats);
      ret.minValue = dbcommon::Scalar(
          dbcommon::CreateDatum(static_cast<int32_t>(dStat->getMinimum())),
          false);
      ret.maxValue = dbcommon::Scalar(
          dbcommon::CreateDatum(static_cast<int32_t>(dStat->getMaximum())),
          false);
      break;
    }
    case dbcommon::TypeKind::TIMESTAMPID:
    case dbcommon::TypeKind::TIMESTAMPTZID: {
      const TimestampColumnStatisticsImpl* tStat =
          dynamic_cast<const TimestampColumnStatisticsImpl*>(stats);
      minTimestamp->second = tStat->getMinimum() / 1000;
      minTimestamp->nanosecond = (tStat->getMinimum() % 1000) * 1000000;
      maxTimestamp->second = tStat->getMaximum() / 1000;
      maxTimestamp->nanosecond =
          (tStat->getMaximum() % 1000) * 1000000 + 999999;
      ret.minValue =
          dbcommon::Scalar(dbcommon::CreateDatum(minTimestamp), false);
      ret.minValue.length = sizeof(dbcommon::Timestamp);
      ret.maxValue =
          dbcommon::Scalar(dbcommon::CreateDatum(maxTimestamp), false);
      ret.maxValue.length = sizeof(dbcommon::Timestamp);
      break;
    }
    case dbcommon::TypeKind::DECIMALID: {
      const DecimalColumnStatisticsImpl* dStat =
          dynamic_cast<const DecimalColumnStatisticsImpl*>(stats);
      ret.minValue = dbcommon::Scalar(
          dbcommon::CreateDatum(dStat->getMinimumStr()), false);
      ret.minValue.length = sizeof(dbcommon::DecimalVar);
      ret.maxValue = dbcommon::Scalar(
          dbcommon::CreateDatum(dStat->getMaximumStr()), false);
      ret.maxValue.length = sizeof(dbcommon::DecimalVar);
      break;
    }
    default: {
      ret.hasMinMax = false;
    }
  }
  return ret;
}

bool OrcPredicates::canDropByBloomFilter(int32_t colId,
                                         univplan::PredicateStats* stat,
                                         dbcommon::TypeKind type) const {
  disableInvalidColId(colId);
  const Type& child = *reader->getType().getSubtype(colId - 1);
  proto::BloomFilterIndex bloomFilterIndexProto =
      reader->rebuildBloomFilter(child.getColumnId());
  if (bloomFilterIndexProto.bloomfilter_size() == 0) return false;

  for (int32_t i = 0; i < bloomFilterIndexProto.bloomfilter_size(); ++i) {
    const proto::BloomFilter& bloomFilterProto =
        bloomFilterIndexProto.bloomfilter(i);
    std::vector<uint64_t> data;
    for (int32_t j = 0; j < bloomFilterProto.bitset_size(); ++j)
      data.push_back(bloomFilterProto.bitset(j));
    storage::BloomFilter::uptr bf(new storage::BloomFilter(
        data.data(), data.size(), bloomFilterProto.numhashfunctions()));
    switch (type) {
      case dbcommon::TypeKind::SMALLINTID: {
        if (bf->testInt(dbcommon::DatumGetValue<int16_t>(stat->maxValue.value)))
          return false;
        break;
      }
      case dbcommon::TypeKind::INTID:
      case dbcommon::TypeKind::DATEID: {
        if (bf->testInt(dbcommon::DatumGetValue<int32_t>(stat->maxValue.value)))
          return false;
        break;
      }
      case dbcommon::TypeKind::BIGINTID:
      case dbcommon::TypeKind::TIMEID: {
        if (bf->testInt(dbcommon::DatumGetValue<int64_t>(stat->maxValue.value)))
          return false;
        break;
      }
      case dbcommon::TypeKind::FLOATID: {
        if (bf->testDouble(
                dbcommon::DatumGetValue<float>(stat->maxValue.value)))
          return false;
        break;
      }
      case dbcommon::TypeKind::DOUBLEID: {
        if (bf->testDouble(
                dbcommon::DatumGetValue<double>(stat->maxValue.value)))
          return false;
        break;
      }
      case dbcommon::TypeKind::CHARID:
      case dbcommon::TypeKind::VARCHARID:
      case dbcommon::TypeKind::STRINGID: {
        const char* str =
            dbcommon::DatumGetValue<const char*>(stat->maxValue.value);
        if (bf->testString(str, strlen(str))) return false;
        break;
      }
      case dbcommon::TypeKind::TIMESTAMPID:
      case dbcommon::TypeKind::TIMESTAMPTZID: {
        dbcommon::Timestamp* ts =
            dbcommon::DatumGetValue<dbcommon::Timestamp*>(stat->maxValue.value);
        if (bf->testInt(ts->second * 1000 + ts->nanosecond / 1000000))
          return false;
        break;
      }
      default: {
        LOG_ERROR(
            ERRCODE_FEATURE_NOT_SUPPORTED,
            "not supported type %d in OrcPredicates::canDropByBloomFilter",
            type);
      }
    }
  }

  return true;
}

void OrcPredicates::disableInvalidColId(int32_t colId) const {
  if (colId < 0)
    LOG_ERROR(ERRCODE_FEATURE_NOT_SUPPORTED,
              "hidden column doesn't support predicate");
}

}  // end of namespace orc
