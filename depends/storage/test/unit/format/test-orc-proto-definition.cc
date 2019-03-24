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

#include <errno.h>
#include <stdlib.h>

#include "dbcommon/log/exception.h"
#include "dbcommon/log/logger.h"
#include "gtest/gtest.h"
#include "storage/format/orc/orc-proto-definition.h"
#include "storage/format/orc/type-impl.h"
#include "storage/format/orc/vector.h"
#include "storage/testutil/file-utils.h"

namespace orc {

TEST(TESTOrcProtoDefinition, TestColumnStatistics) {
  proto::ColumnStatistics stats;
  ColumnStatisticsImpl cs(stats);
  EXPECT_EQ(cs.getNumberOfValues(), stats.numberofvalues());
}

TEST(TESTOrcProtoDefinition, TestBinaryColumnStatistics) {
  proto::ColumnStatistics stats;
  BinaryColumnStatisticsImpl bcs(stats, true);

  EXPECT_EQ(bcs.getNumberOfValues(), stats.numberofvalues());
  EXPECT_THROW(bcs.getTotalLength(), dbcommon::TransactionAbortException);
  EXPECT_EQ(bcs.hasTotalLength(), false);

  proto::ColumnStatistics stats1;
  BinaryColumnStatisticsImpl bcs1(stats1, false);
  EXPECT_EQ(bcs1.getNumberOfValues(), stats1.numberofvalues());
  EXPECT_THROW(bcs.getTotalLength(), dbcommon::TransactionAbortException);
  EXPECT_EQ(bcs1.hasTotalLength(), false);

  proto::ColumnStatistics stats2;
  stats2.mutable_binarystatistics()->set_sum(1000);
  BinaryColumnStatisticsImpl bcs2(stats2, true);

  EXPECT_EQ(bcs2.getNumberOfValues(), stats2.numberofvalues());
  EXPECT_EQ(bcs2.getTotalLength(), 1000);
  EXPECT_EQ(bcs2.hasTotalLength(), true);

  proto::ColumnStatistics stats3;
  stats3.mutable_binarystatistics()->set_sum(2000);
  BinaryColumnStatisticsImpl bcs3(stats3, false);

  EXPECT_EQ(bcs3.getNumberOfValues(), stats3.numberofvalues());
  EXPECT_THROW(bcs3.getTotalLength(), dbcommon::TransactionAbortException);
  EXPECT_EQ(bcs3.hasTotalLength(), false);
}

TEST(TESTOrcProtoDefinition, TestBoolearColumnStatistics) {
  proto::ColumnStatistics stats;
  BooleanColumnStatisticsImpl bcs(stats, true);

  EXPECT_EQ(bcs.getNumberOfValues(), stats.numberofvalues());
  EXPECT_THROW(bcs.getFalseCount(), dbcommon::TransactionAbortException);
  EXPECT_THROW(bcs.getTrueCount(), dbcommon::TransactionAbortException);

  proto::ColumnStatistics stats1;
  BooleanColumnStatisticsImpl bcs1(stats1, false);
  EXPECT_EQ(bcs1.getNumberOfValues(), stats1.numberofvalues());
  EXPECT_THROW(bcs1.getFalseCount(), dbcommon::TransactionAbortException);
  EXPECT_THROW(bcs1.getTrueCount(), dbcommon::TransactionAbortException);

  proto::ColumnStatistics stats2;
  stats2.mutable_bucketstatistics()->add_count(1000);
  BooleanColumnStatisticsImpl bcs2(stats2, true);

  EXPECT_EQ(bcs2.getNumberOfValues(), stats2.numberofvalues());
  EXPECT_EQ(bcs2.getFalseCount(),
            bcs2.getNumberOfValues() - bcs2.getTrueCount());
  EXPECT_EQ(bcs2.getTrueCount(), 1000);
  EXPECT_EQ(bcs2.hasCount(), true);

  proto::ColumnStatistics stats3;
  stats3.mutable_bucketstatistics()->add_count(2000);
  BooleanColumnStatisticsImpl bcs3(stats3, false);

  EXPECT_EQ(bcs3.getNumberOfValues(), stats3.numberofvalues());
  EXPECT_THROW(bcs3.getFalseCount(), dbcommon::TransactionAbortException);
  EXPECT_THROW(bcs3.getTrueCount(), dbcommon::TransactionAbortException);
}

TEST(TESTOrcProtoDefinition, TestDateColumnStatistics) {
  proto::ColumnStatistics stats;
  DateColumnStatisticsImpl dcs(stats, true);

  EXPECT_EQ(dcs.getNumberOfValues(), stats.numberofvalues());
  EXPECT_THROW(dcs.getMaximum(), dbcommon::TransactionAbortException);
  EXPECT_THROW(dcs.getMinimum(), dbcommon::TransactionAbortException);

  proto::ColumnStatistics stats1;
  DateColumnStatisticsImpl dcs1(stats1, false);
  EXPECT_EQ(dcs1.getNumberOfValues(), stats1.numberofvalues());
  EXPECT_THROW(dcs1.getMaximum(), dbcommon::TransactionAbortException);
  EXPECT_THROW(dcs1.getMinimum(), dbcommon::TransactionAbortException);

  proto::ColumnStatistics stats2;
  stats2.mutable_datestatistics()->set_maximum(10000);
  stats2.mutable_datestatistics()->set_minimum(1);
  DateColumnStatisticsImpl dcs2(stats2, true);

  EXPECT_EQ(dcs2.getNumberOfValues(), stats2.numberofvalues());
  EXPECT_EQ(dcs2.getMaximum(), 10000);
  EXPECT_EQ(dcs2.getMinimum(), 1);
  EXPECT_EQ(dcs2.hasMaximum(), true);
  EXPECT_EQ(dcs2.hasMinimum(), true);

  proto::ColumnStatistics stats3;
  stats3.mutable_datestatistics()->set_maximum(20000);
  stats3.mutable_datestatistics()->set_minimum(2);
  DateColumnStatisticsImpl dcs3(stats3, false);

  EXPECT_EQ(dcs3.getNumberOfValues(), stats3.numberofvalues());
  EXPECT_THROW(dcs3.getMaximum(), dbcommon::TransactionAbortException);
  EXPECT_THROW(dcs3.getMinimum(), dbcommon::TransactionAbortException);
}

TEST(TESTOrcProtoDefinition, DecimalColumnStatisticsImpl) {
  proto::ColumnStatistics stats;
  DecimalColumnStatisticsImpl dcs(stats, true);

  EXPECT_EQ(dcs.getNumberOfValues(), stats.numberofvalues());
  EXPECT_THROW(dcs.getMaximum(), dbcommon::TransactionAbortException);
  EXPECT_THROW(dcs.getMinimum(), dbcommon::TransactionAbortException);
  EXPECT_THROW(dcs.getSum(), dbcommon::TransactionAbortException);

  proto::ColumnStatistics stats1;
  DecimalColumnStatisticsImpl dcs1(stats1, false);
  EXPECT_EQ(dcs1.getNumberOfValues(), stats1.numberofvalues());
  EXPECT_THROW(dcs1.getMaximum(), dbcommon::TransactionAbortException);
  EXPECT_THROW(dcs1.getMinimum(), dbcommon::TransactionAbortException);
  EXPECT_THROW(dcs1.getSum(), dbcommon::TransactionAbortException);

  proto::ColumnStatistics stats2;
  stats2.mutable_decimalstatistics()->set_maximum("1000.0");
  stats2.mutable_decimalstatistics()->set_minimum("1.0");
  stats2.mutable_decimalstatistics()->set_sum("1000000.0");
  DecimalColumnStatisticsImpl dcs2(stats2, true);

  EXPECT_EQ(dcs2.getNumberOfValues(), stats2.numberofvalues());
  EXPECT_EQ(dcs2.getMaximum().toString(), "1000.0");
  EXPECT_EQ(dcs2.getMinimum().toString(), "1.0");
  EXPECT_EQ(dcs2.getSum().toString(), "1000000.0");
  EXPECT_EQ(dcs2.hasMaximum(), true);
  EXPECT_EQ(dcs2.hasMinimum(), true);
  EXPECT_EQ(dcs2.hasSum(), true);

  proto::ColumnStatistics stats3;
  stats3.mutable_decimalstatistics()->set_maximum("1000.0");
  stats3.mutable_decimalstatistics()->set_minimum("1.0");
  stats3.mutable_decimalstatistics()->set_sum("1000000.0");
  DecimalColumnStatisticsImpl dcs3(stats3, false);

  EXPECT_EQ(dcs3.getNumberOfValues(), stats3.numberofvalues());
  EXPECT_THROW(dcs3.getMaximum(), dbcommon::TransactionAbortException);
  EXPECT_THROW(dcs3.getMinimum(), dbcommon::TransactionAbortException);
  EXPECT_THROW(dcs3.getSum(), dbcommon::TransactionAbortException);
}

TEST(TESTOrcProtoDefinition, TestDecimal) {
  Decimal d("1000.0");
  std::string s = d.toString();
  EXPECT_EQ(s, "1000.0");
}

TEST(TESTOrcProtoDefinition, DoubleColumnStatisticsImpl) {
  proto::ColumnStatistics stats;
  DoubleColumnStatisticsImpl dcs(stats);

  EXPECT_EQ(dcs.getNumberOfValues(), stats.numberofvalues());
  EXPECT_THROW(dcs.getMaximum(), dbcommon::TransactionAbortException);
  EXPECT_THROW(dcs.getMinimum(), dbcommon::TransactionAbortException);
  EXPECT_THROW(dcs.getSum(), dbcommon::TransactionAbortException);

  proto::ColumnStatistics stats2;
  stats2.mutable_doublestatistics()->set_maximum(1000.0);
  stats2.mutable_doublestatistics()->set_minimum(1.0);
  stats2.mutable_doublestatistics()->set_sum(1000000.0);
  DoubleColumnStatisticsImpl dcs2(stats2);

  EXPECT_EQ(dcs2.getNumberOfValues(), stats2.numberofvalues());
  EXPECT_EQ(dcs2.getMaximum(), 1000.0);
  EXPECT_EQ(dcs2.getMinimum(), 1.0);
  EXPECT_EQ(dcs2.getSum(), 1000000.0);
  EXPECT_EQ(dcs2.hasMaximum(), true);
  EXPECT_EQ(dcs2.hasMinimum(), true);
  EXPECT_EQ(dcs2.hasSum(), true);
}

TEST(TESTOrcProtoDefinition, IntegerColumnStatisticsImpl) {
  proto::ColumnStatistics stats;
  IntegerColumnStatisticsImpl ics(stats);

  EXPECT_EQ(ics.getNumberOfValues(), stats.numberofvalues());
  EXPECT_THROW(ics.getMaximum(), dbcommon::TransactionAbortException);
  EXPECT_THROW(ics.getMinimum(), dbcommon::TransactionAbortException);
  EXPECT_THROW(ics.getSum(), dbcommon::TransactionAbortException);

  proto::ColumnStatistics stats2;
  stats2.mutable_intstatistics()->set_maximum(1000);
  stats2.mutable_intstatistics()->set_minimum(1);
  stats2.mutable_intstatistics()->set_sum(1000000);
  IntegerColumnStatisticsImpl ics2(stats2);

  EXPECT_EQ(ics2.getNumberOfValues(), stats2.numberofvalues());
  EXPECT_EQ(ics2.getMaximum(), 1000);
  EXPECT_EQ(ics2.getMinimum(), 1);
  EXPECT_EQ(ics2.getSum(), 1000000);
  EXPECT_EQ(ics2.hasMaximum(), true);
  EXPECT_EQ(ics2.hasMinimum(), true);
  EXPECT_EQ(ics2.hasSum(), true);
}

TEST(TESTOrcProtoDefinition, StringColumnStatisticsImpl) {
  proto::ColumnStatistics stats;
  StringColumnStatisticsImpl scs(stats, true);

  EXPECT_EQ(scs.getNumberOfValues(), stats.numberofvalues());
  EXPECT_THROW(scs.getMaximum(), dbcommon::TransactionAbortException);
  EXPECT_THROW(scs.getMinimum(), dbcommon::TransactionAbortException);
  EXPECT_THROW(scs.getTotalLength(), dbcommon::TransactionAbortException);

  proto::ColumnStatistics stats1;
  StringColumnStatisticsImpl scs1(stats, false);

  EXPECT_EQ(scs1.getNumberOfValues(), stats1.numberofvalues());
  EXPECT_THROW(scs1.getMaximum(), dbcommon::TransactionAbortException);
  EXPECT_THROW(scs1.getMinimum(), dbcommon::TransactionAbortException);
  EXPECT_THROW(scs1.getTotalLength(), dbcommon::TransactionAbortException);

  proto::ColumnStatistics stats2;
  stats2.mutable_stringstatistics()->set_maximum("1000");
  stats2.mutable_stringstatistics()->set_minimum("1");
  stats2.mutable_stringstatistics()->set_sum(1000000);
  StringColumnStatisticsImpl scs2(stats2, true);

  EXPECT_EQ(scs2.getNumberOfValues(), stats2.numberofvalues());
  EXPECT_STREQ(scs2.getMaximum(), "1000");
  EXPECT_STREQ(scs2.getMinimum(), "1");
  EXPECT_EQ(scs2.getTotalLength(), 1000000);
  EXPECT_EQ(scs2.hasMaximum(), true);
  EXPECT_EQ(scs2.hasMinimum(), true);
  EXPECT_EQ(scs2.hasTotalLength(), true);

  proto::ColumnStatistics stats3;
  stats3.mutable_stringstatistics()->set_maximum("1000");
  stats3.mutable_stringstatistics()->set_minimum("1");
  stats3.mutable_stringstatistics()->set_sum(1000000);
  StringColumnStatisticsImpl scs3(stats3, false);

  EXPECT_EQ(scs3.getNumberOfValues(), stats3.numberofvalues());
  EXPECT_THROW(scs3.getMaximum(), dbcommon::TransactionAbortException);
  EXPECT_THROW(scs3.getMinimum(), dbcommon::TransactionAbortException);
  EXPECT_THROW(scs3.getTotalLength(), dbcommon::TransactionAbortException);
}

TEST(TESTOrcProtoDefinition, TimestampColumnStatisticsImpl) {
  proto::ColumnStatistics stats;
  TimestampColumnStatisticsImpl tcs(stats, true);

  EXPECT_EQ(tcs.getNumberOfValues(), stats.numberofvalues());
  EXPECT_THROW(tcs.getMaximum(), dbcommon::TransactionAbortException);
  EXPECT_THROW(tcs.getMinimum(), dbcommon::TransactionAbortException);

  proto::ColumnStatistics stats1;
  TimestampColumnStatisticsImpl tcs1(stats, false);

  EXPECT_EQ(tcs1.getNumberOfValues(), stats1.numberofvalues());
  EXPECT_THROW(tcs1.getMaximum(), dbcommon::TransactionAbortException);
  EXPECT_THROW(tcs1.getMinimum(), dbcommon::TransactionAbortException);

  proto::ColumnStatistics stats2;
  stats2.mutable_timestampstatistics()->set_maximum(1000);
  stats2.mutable_timestampstatistics()->set_minimum(1);
  TimestampColumnStatisticsImpl tcs2(stats2, true);

  EXPECT_EQ(tcs2.getNumberOfValues(), stats2.numberofvalues());
  EXPECT_EQ(tcs2.getMaximum(), 1000);
  EXPECT_EQ(tcs2.getMinimum(), 1);
  EXPECT_EQ(tcs2.hasMaximum(), true);
  EXPECT_EQ(tcs2.hasMinimum(), true);

  proto::ColumnStatistics stats3;
  stats3.mutable_timestampstatistics()->set_maximum(1000);
  stats3.mutable_timestampstatistics()->set_minimum(1);
  TimestampColumnStatisticsImpl tcs3(stats3, false);

  EXPECT_EQ(tcs3.getNumberOfValues(), stats3.numberofvalues());
  EXPECT_THROW(tcs3.getMaximum(), dbcommon::TransactionAbortException);
  EXPECT_THROW(tcs3.getMinimum(), dbcommon::TransactionAbortException);
}

TEST(TESTOrcProtoDefinition, StreamInformationImpl) {
  proto::Stream stream;
  stream.set_column(0);
  stream.set_kind(orc::proto::Stream_Kind::Stream_Kind_DATA);
  stream.set_length(100);
  StreamInformationImpl si(80, stream);

  EXPECT_EQ(si.getColumnId(), 0);
  EXPECT_EQ(si.getKind(), StreamKind::StreamKind_DATA);
  EXPECT_EQ(si.getLength(), 100);
  EXPECT_EQ(si.getOffset(), 80);
}

TEST(TESTOrcProtoDefinition, StripeInformationImpl) {
  InputStream *input = nullptr;
  dbcommon::MemoryPool *pool = dbcommon::getDefaultPool();
  StripeInformationImpl si(80, 0, 1, 2, 3, input, *pool,
                           CompressionKind::CompressionKind_LZ4, 88);

  EXPECT_EQ(si.getOffset(), 80);
  EXPECT_EQ(si.getIndexLength(), 0);
  EXPECT_EQ(si.getDataLength(), 1);
  EXPECT_EQ(si.getFooterLength(), 2);
  EXPECT_EQ(si.getNumberOfRows(), 3);
}

TEST(TESTOrcProtoDefinition, StatisticsImpl) {
  proto::StripeStatistics stripeStats;
  orc::proto::ColumnStatistics *cs = stripeStats.add_colstats();
  cs->set_hasnull(true);
  cs->set_numberofvalues(100);

  StatisticsImpl si(stripeStats, true);

  EXPECT_EQ(si.getNumberOfColumns(), 1);
  EXPECT_EQ(si.getColumnStatistics(0)->getNumberOfValues(), 100);
}

}  // namespace orc
