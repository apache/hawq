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

#include "dbcommon/common/tuple-batch.h"
#include "dbcommon/common/tuple-desc.h"
#include "dbcommon/filesystem/file-system-manager.h"
#include "dbcommon/filesystem/file-system.h"
#include "dbcommon/filesystem/local/local-file-system.h"
#include "dbcommon/log/logger.h"
#include "dbcommon/testutil/tuple-batch-utils.h"
#include "dbcommon/utils/parameters.h"

#include "storage/format/orc/orc-format.h"
#include "storage/testutil/format-util.h"

using namespace testing;  // NOLINT

namespace storage {

TEST(TestORCFormat, TestORCFormatReadWrite_1IntColumn1Row) {
  dbcommon::TupleBatchUtility tbu;
  dbcommon::TupleDesc::uptr desc = tbu.generateTupleDesc("thil");
  dbcommon::TupleBatch::uptr tb = tbu.generateTupleBatch(*desc, 0, 1);
  FormatUtility fmtu;

  fmtu.writeThenReadCompare("orc", desc.get(), std::move(tb),
                            "/tmp/TestORCFormatReadWrite_1IntColumn1Row");
}

TEST(TestORCFormat, TestORCFormatReadWrite_NullValue) {
  dbcommon::TupleBatchUtility tbu;
  FormatUtility fmtu;
  {
    dbcommon::TupleDesc::uptr desc = tbu.generateTupleDesc("il");
    dbcommon::TupleBatch::uptr tb =
        tbu.generateTupleBatchRandom(*desc, 666, 7, true);
    fmtu.writeThenReadCompare("orc", desc.get(), std::move(tb),
                              "/tmp/TestORCFormatReadWrite_NullValue");
    LOG_INFO("OK with noavx");
  }
}

TEST(TestORCFormat, TestORCFormatReadWrite_1IntColumnDirect) {
  dbcommon::TupleBatchUtility tbu;
  FormatUtility fmtu;
  {
    dbcommon::TupleDesc::uptr desc = tbu.generateTupleDesc("il");
    dbcommon::TupleBatch::uptr tb =
        tbu.generateTupleBatchRandom(*desc, 0, 2005, false);
    fmtu.writeThenReadCompare("orc", desc.get(), std::move(tb),
                              "/tmp/TestORCFormatReadWrite_1IntColumnDirect");
    LOG_INFO("OK without nulls");
  }
  {
    dbcommon::TupleDesc::uptr desc = tbu.generateTupleDesc("il");
    dbcommon::TupleBatch::uptr tb =
        tbu.generateTupleBatchRandom(*desc, 6666666, 2003, true);
    fmtu.writeThenReadCompare("orc", desc.get(), std::move(tb),
                              "/tmp/TestORCFormatReadWrite_1IntColumnDirect");
    LOG_INFO("OK with nulls");
  }
}

TEST(TestORCFormat, TestORCFormatReadWrite_1IntColumnRepeat) {
  dbcommon::TupleBatchUtility tbu;
  FormatUtility fmtu;
  {
    dbcommon::TupleDesc::uptr desc = tbu.generateTupleDesc("hil");
    dbcommon::TupleBatch::uptr tb =
        tbu.generateTupleBatchDuplicate(*desc, 0, 2005, false);
    fmtu.writeThenReadCompare("orc", desc.get(), std::move(tb),
                              "/tmp/TestORCFormatReadWrite_1IntColumnRepeat");
    LOG_INFO("OK without nulls");
  }
  {
    dbcommon::TupleDesc::uptr desc = tbu.generateTupleDesc("il");
    dbcommon::TupleBatch::uptr tb =
        tbu.generateTupleBatchDuplicate(*desc, 6666666, 2003, true);
    fmtu.writeThenReadCompare("orc", desc.get(), std::move(tb),
                              "/tmp/TestORCFormatReadWrite_1IntColumnRepeat");
    LOG_INFO("OK with nulls");
  }
}

TEST(TestORCFormat, TestORCFormatReadWrite_1IntColumnDelta) {
  dbcommon::TupleBatchUtility tbu;
  FormatUtility fmtu;
  {
    dbcommon::TupleDesc::uptr desc = tbu.generateTupleDesc("hil");
    dbcommon::TupleBatch::uptr tb =
        tbu.generateTupleBatch(*desc, 666, 2005, false);
    fmtu.writeThenReadCompare("orc", desc.get(), std::move(tb),
                              "/tmp/TestORCFormatReadWrite_1IntColumnDelta");
    LOG_INFO("OK without nulls");
  }
  {
    dbcommon::TupleDesc::uptr desc = tbu.generateTupleDesc("il");
    dbcommon::TupleBatch::uptr tb =
        tbu.generateTupleBatch(*desc, 6666666, 2003, true);
    fmtu.writeThenReadCompare("orc", desc.get(), std::move(tb),
                              "/tmp/TestORCFormatReadWrite_1IntColumnDelta");
    LOG_INFO("OK with nulls");
  }
}

TEST(TestORCFormat, TestORCFormatReadWrite_1IntColumnPatched) {
  dbcommon::TupleBatchUtility tbu;
  FormatUtility fmtu;
  {
    dbcommon::TupleDesc::uptr desc = tbu.generateTupleDesc("il");
    dbcommon::TupleBatch::uptr tb =
        tbu.generateTupleBatchPatch(*desc, 65536, 200, false);
    fmtu.writeThenReadCompare("orc", desc.get(), std::move(tb),
                              "/tmp/TestORCFormatReadWrite_1IntColumnPatched");
    LOG_INFO("OK without nulls");
  }
  {
    dbcommon::TupleDesc::uptr desc = tbu.generateTupleDesc("il");
    dbcommon::TupleBatch::uptr tb =
        tbu.generateTupleBatchPatch(*desc, 65536, 200, true);
    fmtu.writeThenReadCompare("orc", desc.get(), std::move(tb),
                              "/tmp/TestORCFormatReadWrite_1IntColumnPatched");
    LOG_INFO("OK with nulls");
  }
}

TEST(TestORCFormat, TestORCFormatReadWrite_1SmallIntColumnMaxValueRow) {
  dbcommon::TupleBatchUtility tbu;
  dbcommon::TupleDesc::uptr desc = tbu.generateTupleDesc("h");
  dbcommon::TupleBatch::uptr tb = tbu.generateTupleBatch(*desc, 32767, 1);
  FormatUtility fmtu;

  fmtu.writeThenReadCompare(
      "orc", desc.get(), std::move(tb),
      "/tmp/TestORCFormatReadWrite_1SmallIntColumnMaxValueRow");
}

TEST(TestORCFormat, TestORCFormatReadWrite_1DoubleColumn1Row) {
  dbcommon::TupleBatchUtility tbu;
  dbcommon::TupleDesc::uptr desc = tbu.generateTupleDesc("fd");
  dbcommon::TupleBatch::uptr tb = tbu.generateTupleBatch(*desc, 0, 1);
  FormatUtility fmtu;

  fmtu.writeThenReadCompare("orc", desc.get(), std::move(tb),
                            "/tmp/TestORCFormatReadWrite_1DoubleColumn1Row");
}

TEST(TestORCFormat, TestORCFormatReadWrite_1DoubleColumn1024Row) {
  {
    dbcommon::TupleBatchUtility tbu;
    dbcommon::TupleDesc::uptr desc = tbu.generateTupleDesc("fd");
    dbcommon::TupleBatch::uptr tb = tbu.generateTupleBatch(*desc, 0, 2003);
    FormatUtility fmtu;

    fmtu.writeThenReadCompare(
        "orc", desc.get(), std::move(tb),
        "/tmp/TestORCFormatReadWrite_1DoubleColumn1024Row");
  }
  {
    dbcommon::TupleBatchUtility tbu;
    dbcommon::TupleDesc::uptr desc = tbu.generateTupleDesc("fd");
    dbcommon::TupleBatch::uptr tb =
        tbu.generateTupleBatchPatch(*desc, 0, 2003, true);
    FormatUtility fmtu;

    fmtu.writeThenReadCompare(
        "orc", desc.get(), std::move(tb),
        "/tmp/TestORCFormatReadWrite_1DoubleColumn1024Row");
  }
}

TEST(TestORCFormat, TestORCFormatReadWrite_1StringColumn1Row) {
  dbcommon::TupleBatchUtility tbu;
  dbcommon::TupleDesc::uptr desc = tbu.generateTupleDesc("s");
  dbcommon::TupleBatch::uptr tb = tbu.generateTupleBatch(*desc, 0, 1);
  FormatUtility fmtu;

  fmtu.writeThenReadCompare("orc", desc.get(), std::move(tb),
                            "/tmp/TestORCFormatReadWrite_1StringColumn1Row");
}

TEST(TestORCFormat, TestORCFormatReadWrite_1StringColumnDirectEnc1024Row) {
  dbcommon::TupleBatchUtility tbu;
  FormatUtility fmtu;
  orc::SeekableOutputStream::COMPRESS_BLOCK_SIZE = 1024;
  dbcommon::TupleDesc::uptr desc;
  dbcommon::TupleBatch::uptr tb;
  {  // direct encoding
    desc = tbu.generateTupleDesc("s");
    tb = tbu.generateTupleBatch(*desc, 0, 1024);
    fmtu.writeThenReadCompare(
        "orc", desc.get(), std::move(tb),
        "/tmp/TestORCFormatReadWrite_1StringColumnDirectEnc1024Row",
        "{\"dicthreshold\":\"1\"}");
    LOG_INFO("OK without nulls");

    desc = tbu.generateTupleDesc("s");
    tb = tbu.generateTupleBatch(*desc, 0, 1024, true);
    fmtu.writeThenReadCompare(
        "orc", desc.get(), std::move(tb),
        "/tmp/TestORCFormatReadWrite_1StringColumnDirectEnc1024Row",
        "{\"dicthreshold\":\"1\"}");
    LOG_INFO("OK with nulls");
  }
}

TEST(TestORCFormat, TestORCFormatReadWrite_1StringColumnDictEnc1024Row) {
  dbcommon::TupleBatchUtility tbu;
  FormatUtility fmtu;
  orc::SeekableOutputStream::COMPRESS_BLOCK_SIZE = 1024;
  dbcommon::TupleDesc::uptr desc;
  dbcommon::TupleBatch::uptr tb;
  {  // dictionary encoding
    desc = tbu.generateTupleDesc("s");
    tb = tbu.generateTupleBatchDuplicate(*desc, 0, 1024, false);
    fmtu.writeThenReadCompare(
        "orc", desc.get(), std::move(tb),
        "/tmp/TestORCFormatReadWrite_1StringColumn1024Row_dictEnc_noNulls",
        "{\"dicthreshold\":\"0\"}");
    LOG_INFO("OK without nulls");

    desc = tbu.generateTupleDesc("s");
    tb = tbu.generateTupleBatchDuplicate(*desc, 0, 1024, true);
    fmtu.writeThenReadCompare(
        "orc", desc.get(), std::move(tb),
        "/tmp/TestORCFormatReadWrite_1StringColumn1024Row_dictEnc_withNulls",
        "{\"dicthreshold\":\"0\"}");
    LOG_INFO("OK with nulls");
  }
}

TEST(TestORCFormat, TestORCFormatReadWrite_MixColumn1024Row) {
  dbcommon::TupleBatchUtility tbu;
  dbcommon::TupleDesc::uptr desc = tbu.generateTupleDesc("btilhdfsvc");
  dbcommon::TupleBatch::uptr tb =
      tbu.generateTupleBatchRandom(*desc, 0, 2003, true);
  FormatUtility fmtu;

  fmtu.writeThenReadCompare("orc", desc.get(), std::move(tb),
                            "/tmp/TestORCFormatReadWrite_MixColumn1024Row");
}

TEST(
    TestORCFormat,
    DISABLED_TestORCFormatReadWrite_MultiBlock_1i_start0_step8_num256_nt8_align128) {  // NOLINT
  FormatUtility fmtu;
  fmtu.multiBlockTest(
      "orc", "i", 0, 8, 256, 8, 128,
      "/tmp/"
      "TestORCFormatReadWrite_MultiBlock_1i_start0_step8_num256_"
      "nt8_align128");
}

TEST(
    TestORCFormat,
    DISABLED_TestORCFormatReadWrite_MultiBlock_1i_start0_step1024_num1024_nt8_align128) {  // NOLINT
  FormatUtility fmtu;

  // throw too big tuple batch exception
  EXPECT_THROW(fmtu.multiBlockTest(
                   "orc", "i", 0, 1024, 1024, 8, 128,
                   "/tmp/"
                   "TestORCFormatReadWrite_MultiBlock_1i_start0_step8_num256_"
                   "nt8_align128"),
               dbcommon::TransactionAbortException);
}

TEST(
    TestORCFormat,
    DISABLED_TestORCFormatReadWrite_MultiBlock_1s_start0_step8_num256_nt8_align256) {  // NOLINT
  FormatUtility fmtu;
  fmtu.multiBlockTest(
      "orc", "s", 0, 8, 18, 8, 256,
      "/tmp/"
      "TestORCFormatReadWrite_MultiBlock_1s_start0_step8_num256_"
      "nt8_align256");
}

TEST(
    TestORCFormat,
    DISABLED_TestORCFormatReadWrite_MultiBlock_1i_start0_step8_num256_nt8_align256) {  // NOLINT
  FormatUtility fmtu;

  fmtu.multiBlockTest(
      "orc", "i", 0, 8, 256, 8, 256,
      "/tmp/"
      "TestORCFormatReadWrite_MultiBlock_1i_start0_step8_num256_"
      "nt8_align256");
}

TEST(
    TestORCFormat,
    DISABLED_TestORCFormatReadWrite_MultiBlock_1i_start0_step1_num256_nt8_align128) {  // NOLINT
  FormatUtility fmtu;
  fmtu.multiBlockTest(
      "orc", "i", 0, 1, 256, 8, 128,
      "/tmp/"
      "TestORCFormatReadWrite_MultiBlock_1i_start0_step1_num256_"
      "nt8_align128");
}

TEST(
    TestORCFormat,
    DISABLED_TestORCFormatReadWrite_MultiBlock_1s_start0_step1_num256_nt8_align128) {  // NOLINT
  FormatUtility fmtu;
  fmtu.multiBlockTest(
      "orc", "s", 0, 1, 256, 8, 128,
      "/tmp/"
      "TestORCFormatReadWrite_MultiBlock_1s_start0_step1_num256_"
      "nt8_align128");
}

TEST(
    TestORCFormat,
    DISABLED_TestORCFormatReadWrite_MultiBlock_1s_start0_step2_num256_nt8_align128) {  // NOLINT
  FormatUtility fmtu;
  fmtu.multiBlockTest(
      "orc", "s", 0, 2, 256, 8, 128,
      "/tmp/"
      "TestORCFormatReadWrite_MultiBlock_1s_start0_step2_num256_"
      "nt8_align128");
}

TEST(
    TestORCFormat,
    DISABLED_TestORCFormatReadWrite_MultiBlock_1s_start0_step3_num256_nt8_align128) {  // NOLINT
  FormatUtility fmtu;
  fmtu.multiBlockTest(
      "orc", "s", 0, 3, 256, 8, 128,
      "/tmp/"
      "TestORCFormatReadWrite_MultiBlock_1s_start0_step3_num256_"
      "nt8_align128");
}

TEST(
    TestORCFormat,
    DISABLED_TestORCFormatReadWrite_MultiBlock_1s_start0_step4_num256_nt8_align128) {  // NOLINT
  FormatUtility fmtu;
  fmtu.multiBlockTest(
      "orc", "s", 0, 4, 256, 8, 128,
      "/tmp/"
      "TestORCFormatReadWrite_MultiBlock_1s_start0_step4_num256_"
      "nt8_align128");
}

TEST(
    TestORCFormat,
    DISABLED_TestORCFormatReadWrite_MultiBlock_1s_start0_step5_num256_nt8_align128) {  // NOLINT
  FormatUtility fmtu;
  fmtu.multiBlockTest(
      "orc", "s", 0, 5, 256, 8, 128,
      "/tmp/"
      "TestORCFormatReadWrite_MultiBlock_1s_start0_step5_num256_"
      "nt8_align128");
}

TEST(
    TestORCFormat,
    DISABLED_TestORCFormatReadWrite_MultiBlock_1s_start0_step6_num256_nt8_align128) {  // NOLINT
  FormatUtility fmtu;
  EXPECT_THROW(fmtu.multiBlockTest(
                   "orc", "s", 0, 6, 256, 8, 128,
                   "/tmp/"
                   "TestORCFormatReadWrite_MultiBlock_1s_start0_step9_num256_"
                   "nt8_align128"),
               dbcommon::TransactionAbortException);
}

TEST(
    TestORCFormat,
    DISABLED_TestORCFormatReadWrite_MultiBlock_1s_start0_step7_num256_nt8_align128) {  // NOLINT
  FormatUtility fmtu;
  EXPECT_THROW(fmtu.multiBlockTest(
                   "orc", "s", 0, 7, 256, 8, 128,
                   "/tmp/"
                   "TestORCFormatReadWrite_MultiBlock_1s_start0_step10_num256_"
                   "nt8_align128"),
               dbcommon::TransactionAbortException);
}

TEST(
    TestORCFormat,
    DISABLED_TestORCFormatReadWrite_MultiBlock_1s_start0_step13_num256_nt8_align128) {  // NOLINT
  FormatUtility fmtu;
  EXPECT_THROW(
      fmtu.multiBlockTest("orc", "s", 0, 13, 256, 8, 128,
                          "/tmp/"
                          "TestORCFormatReadWrite_MultiBlock_1s_start0_"
                          "step13_num256_nt8_align128"),
      dbcommon::TransactionAbortException);
}

TEST(
    TestORCFormat,
    DISABLED_TestORCFormatReadWrite_MultiBlock_1i1s_start0_step8_num256_nt8_align128) {  // NOLINT
  FormatUtility fmtu;
  EXPECT_THROW(
      fmtu.multiBlockTest("orc", "is", 0, 8, 256, 8, 128,
                          "/tmp/"
                          "TestORCFormatReadWrite_MultiBlock_1i1s_start0_"
                          "step8_num256_nt8_align128"),
      dbcommon::TransactionAbortException);
}

TEST(
    TestORCFormat,
    DISABLED_TestORCFormatReadWrite_MultiBlock_3i3s_start0_step1_num1024_nt16_align512) {  // NOLINT
  FormatUtility fmtu;
  fmtu.multiBlockTest(
      "orc", "iiisss", 0, 1, 1024, 16, 512,
      "/tmp/"
      "TestORCFormatReadWrite_MultiBlock_3i3s_start0_step1_num1024_"
      "nt16_align512");
}

TEST(
    TestORCFormat,
    DISABLED_TestORCFormatReadWrite_MultiBlock_3i3s_start0_step1_num1024_nt32_align512) {  // NOLINT
  FormatUtility fmtu;
  fmtu.multiBlockTest(
      "orc", "iiisss", 0, 1, 1024, 32, 512,
      "/tmp/"
      "TestORCFormatReadWrite_MultiBlock_3i3s_start0_step1_num1024_"
      "nt32_align512");
}

TEST(
    TestORCFormat,
    DISABLED_TestORCFormatReadWrite_MultiBlock_3i3s_start0_step1_num2048_nt512_align2048) {  // NOLINT
  FormatUtility fmtu;
  fmtu.multiBlockTest(
      "orc", "iiisss", 0, 1, 2048, 512, 2048,
      "/tmp/"
      "TestORCFormatReadWrite_MultiBlock_3i3s_start0_step1_num2048_"
      "nt512_align2048");
}

TEST(
    TestORCFormat,
    DISABLED_TestORCFormatReadWrite_MultiBlock_btilhdfsvc_start0_step1_num127_nt8_align512) {  // NOLINT
  FormatUtility fmtu;
  fmtu.multiBlockTest(
      "orc", "btilhdfsvc", 0, 1, 127, 8, 512,
      "/tmp/"
      "TestORCFormatReadWrite_MultiBlock_btilhdfsvc_start0_step1_num127_"
      "nt8_align512");
}

TEST(TestORCFormat, TestORCFormatReadWrite_NegativeCase) {
  FormatUtility fu;

  std::unique_ptr<Format> format = fu.createFormat("orc");
  format->beginScan(nullptr, nullptr, nullptr, nullptr, nullptr, false);
  EXPECT_EQ(format->next(), nullptr);

  EXPECT_THROW(format->doUpdate(nullptr), dbcommon::TransactionAbortException);
  EXPECT_THROW(format->doDelete(nullptr), dbcommon::TransactionAbortException);

  dbcommon::Parameters params1;
  params1.set("number.tuples.per.batch", std::to_string(17));
  EXPECT_THROW(fu.createFormat("orc", &params1),
               dbcommon::TransactionAbortException);

  dbcommon::Parameters params2;
  params2.set("format.block.align.size", std::to_string(17));
  EXPECT_THROW(fu.createFormat("orc", &params2),
               dbcommon::TransactionAbortException);
}

TEST(TestORCFormat, TestORCFormatReadWrite_BooleanType) {
  dbcommon::TupleBatchUtility tbu;
  dbcommon::TupleDesc::uptr desc = tbu.generateTupleDesc("B");
  dbcommon::TupleBatch::uptr batch(new dbcommon::TupleBatch(*desc, true));
  dbcommon::TupleBatchWriter& writer = batch->getTupleBatchWriter();
  writer[0]->append("t", false);
  writer[0]->append("t", true);
  writer[0]->append("f", false);
  batch->incNumOfRows(3);
  FormatUtility fmtu;

  fmtu.writeThenReadCompare("orc", desc.get(), std::move(batch),
                            "/tmp/TestORCFormatReadWrite_BooleanType");
}

TEST(TestORCFormat, TestORCFormatReadWrite_DateType) {
  dbcommon::TupleBatchUtility tbu;
  dbcommon::TupleDesc::uptr desc = tbu.generateTupleDesc("D");
  dbcommon::TupleBatch::uptr batch(new dbcommon::TupleBatch(*desc, true));
  dbcommon::TupleBatchWriter& writer = batch->getTupleBatchWriter();
  writer[0]->append("2014-01-01", false);
  writer[0]->append("1900-12-22", true);
  writer[0]->append("1960-10-12 BC", false);
  batch->incNumOfRows(3);
  FormatUtility fmtu;

  fmtu.writeThenReadCompare("orc", desc.get(), std::move(batch),
                            "/tmp/TestORCFormatReadWrite_DateType");
}

TEST(TestORCFormat, TestORCFormatReadWrite_Binary) {
  dbcommon::TupleBatchUtility tbu;
  dbcommon::TupleDesc::uptr desc = tbu.generateTupleDesc("b");
  dbcommon::TupleBatch::uptr tb =
      tbu.generateTupleBatchRandom(*desc, 0, 2048, true);
  FormatUtility fmtu;

  fmtu.writeThenReadCompare("orc", desc.get(), std::move(tb),
                            "/tmp/TestORCFormatReadWrite_Binary");
}

}  // namespace storage
