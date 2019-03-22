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

#include "dbcommon/common/vector.h"
#include "dbcommon/type/decimal.h"
#include "dbcommon/type/type-util.h"
#include "dbcommon/type/typebase.h"
#include "gtest/gtest.h"

namespace dbcommon {
class TestType : public ::testing::Test {
 public:
  TestType() {
    bool1 = TypeUtil::instance()->getTypeEntryById(BOOLEANID)->type.get();
    int1 = TypeUtil::instance()->getTypeEntryById(TINYINTID)->type.get();
    int2 = TypeUtil::instance()->getTypeEntryById(SMALLINTID)->type.get();
    int4 = TypeUtil::instance()->getTypeEntryById(INTID)->type.get();
    int8 = TypeUtil::instance()->getTypeEntryById(BIGINTID)->type.get();
    flp4 = TypeUtil::instance()->getTypeEntryById(FLOATID)->type.get();
    flp8 = TypeUtil::instance()->getTypeEntryById(DOUBLEID)->type.get();
    date4 = TypeUtil::instance()->getTypeEntryById(DATEID)->type.get();
    time8 = TypeUtil::instance()->getTypeEntryById(TIMEID)->type.get();
    timestamp16 =
        TypeUtil::instance()->getTypeEntryById(TIMESTAMPID)->type.get();
    timestamptz16 =
        TypeUtil::instance()->getTypeEntryById(TIMESTAMPTZID)->type.get();

    numeric0 = TypeUtil::instance()->getTypeEntryById(DECIMALID)->type.get();
    str0 = TypeUtil::instance()->getTypeEntryById(STRINGID)->type.get();
    numeric0 = TypeUtil::instance()->getTypeEntryById(DECIMALID)->type.get();
    decimal24 =
        TypeUtil::instance()->getTypeEntryById(DECIMALNEWID)->type.get();
    interval16 = TypeUtil::instance()->getTypeEntryById(INTERVALID)->type.get();

    struct0 = new StructType();
    EXPECT_EQ(0, struct0->getSubtypeCount());
    EXPECT_EQ(BOOLEANID, struct0->addStructField("bool", bool1)->getTypeKind());
    EXPECT_EQ(1, struct0->getSubtypeCount());

    structEx = TypeUtil::instance()->getTypeEntryById(STRUCTEXID)->type.get();
    if (structEx->getSubtypeCount() < 1) {
      EXPECT_EQ(BOOLEANID,
                structEx->addStructField("bool", bool1)->getTypeKind());
      EXPECT_EQ(1, structEx->getSubtypeCount());
    }

    iobaseType =
        TypeUtil::instance()->getTypeEntryById(IOBASETYPEID)->type.get();
  }

  // name convention: type_width
  TypeBase *bool1, *int1, *int2, *int4, *int8, *flp4, *flp8, *str0, *struct0,
      *date4, *time8, *timestamp16, *timestamptz16, *structEx, *iobaseType,
      *numeric0, *decimal24, *interval16;
};
TEST_F(TestType, isFixedSizeType) {
  EXPECT_EQ(true, bool1->isFixedSizeType());
  EXPECT_EQ(true, int1->isFixedSizeType());
  EXPECT_EQ(true, int2->isFixedSizeType());
  EXPECT_EQ(true, int4->isFixedSizeType());
  EXPECT_EQ(true, int8->isFixedSizeType());
  EXPECT_EQ(true, flp4->isFixedSizeType());
  EXPECT_EQ(true, flp8->isFixedSizeType());
  EXPECT_EQ(true, date4->isFixedSizeType());
  EXPECT_EQ(true, time8->isFixedSizeType());
  EXPECT_EQ(true, timestamp16->isFixedSizeType());
  EXPECT_EQ(true, timestamptz16->isFixedSizeType());
  EXPECT_EQ(false, str0->isFixedSizeType());
  EXPECT_EQ(false, numeric0->isFixedSizeType());
  EXPECT_EQ(true, decimal24->isFixedSizeType());
  EXPECT_EQ(true, interval16->isFixedSizeType());
  EXPECT_THROW(struct0->isFixedSizeType(), TransactionAbortException);
  EXPECT_EQ(false, structEx->isFixedSizeType());
  EXPECT_EQ(false, iobaseType->isFixedSizeType());
}

TEST_F(TestType, getTypeWidth) {
  EXPECT_EQ(1, bool1->getTypeWidth());
  EXPECT_EQ(1, int1->getTypeWidth());
  EXPECT_EQ(2, int2->getTypeWidth());
  EXPECT_EQ(4, int4->getTypeWidth());
  EXPECT_EQ(8, int8->getTypeWidth());
  EXPECT_EQ(4, flp4->getTypeWidth());
  EXPECT_EQ(8, flp8->getTypeWidth());
  EXPECT_EQ(4, date4->getTypeWidth());
  EXPECT_EQ(8, time8->getTypeWidth());
  EXPECT_EQ(16, timestamp16->getTypeWidth());
  EXPECT_EQ(16, timestamptz16->getTypeWidth());
  EXPECT_EQ(-1, str0->getTypeWidth());
  EXPECT_EQ(-1, numeric0->getTypeWidth());
  EXPECT_EQ(24, decimal24->getTypeWidth());
  EXPECT_EQ(16, interval16->getTypeWidth());
  EXPECT_THROW(struct0->getTypeWidth(), TransactionAbortException);
  EXPECT_EQ(VAR_TYPE_LENGTH, structEx->getTypeWidth());
  EXPECT_EQ(VAR_TYPE_LENGTH, iobaseType->getTypeWidth());
}
TEST_F(TestType, getTypeKind) {
  EXPECT_EQ(BOOLEANID, bool1->getTypeKind());
  EXPECT_EQ(TINYINTID, int1->getTypeKind());
  EXPECT_EQ(SMALLINTID, int2->getTypeKind());
  EXPECT_EQ(INTID, int4->getTypeKind());
  EXPECT_EQ(BIGINTID, int8->getTypeKind());
  EXPECT_EQ(FLOATID, flp4->getTypeKind());
  EXPECT_EQ(DOUBLEID, flp8->getTypeKind());
  EXPECT_EQ(DATEID, date4->getTypeKind());
  EXPECT_EQ(TIMEID, time8->getTypeKind());
  EXPECT_EQ(TIMESTAMPID, timestamp16->getTypeKind());
  EXPECT_EQ(TIMESTAMPTZID, timestamptz16->getTypeKind());
  EXPECT_EQ(STRINGID, str0->getTypeKind());
  EXPECT_EQ(STRUCTID, struct0->getTypeKind());
  EXPECT_EQ(DECIMALID, numeric0->getTypeKind());
  EXPECT_EQ(STRUCTEXID, structEx->getTypeKind());
  EXPECT_EQ(IOBASETYPEID, iobaseType->getTypeKind());
  EXPECT_EQ(DECIMALID, numeric0->getTypeKind());
  EXPECT_EQ(DECIMALNEWID, decimal24->getTypeKind());
  EXPECT_EQ(INTERVALID, interval16->getTypeKind());
}
TEST_F(TestType, getVector) {
  EXPECT_EQ(BOOLEANID, bool1->getVector(false)->getTypeKind());
  EXPECT_EQ(TINYINTID, int1->getVector(false)->getTypeKind());
  EXPECT_EQ(SMALLINTID, int2->getVector(false)->getTypeKind());
  EXPECT_EQ(INTID, int4->getVector(false)->getTypeKind());
  EXPECT_EQ(BIGINTID, int8->getVector(false)->getTypeKind());
  EXPECT_EQ(FLOATID, flp4->getVector(false)->getTypeKind());
  EXPECT_EQ(DOUBLEID, flp8->getVector(false)->getTypeKind());
  EXPECT_EQ(DATEID, date4->getVector(false)->getTypeKind());
  EXPECT_EQ(TIMEID, time8->getVector(false)->getTypeKind());
  EXPECT_EQ(TIMESTAMPID, timestamp16->getVector(false)->getTypeKind());
  EXPECT_EQ(TIMESTAMPTZID, timestamptz16->getVector(false)->getTypeKind());
  EXPECT_EQ(STRINGID, str0->getVector(false)->getTypeKind());
  EXPECT_EQ(STRUCTID, struct0->getVector(false)->getTypeKind());
  EXPECT_EQ(DECIMALID, numeric0->getVector(false)->getTypeKind());
  EXPECT_EQ(STRUCTEXID, structEx->getVector(false)->getTypeKind());
  EXPECT_EQ(IOBASETYPEID, iobaseType->getVector(false)->getTypeKind());
  EXPECT_EQ(DECIMALID, numeric0->getVector(false)->getTypeKind());
  EXPECT_EQ(DECIMALNEWID, decimal24->getVector(false)->getTypeKind());
  EXPECT_EQ(INTERVALID, interval16->getVector(false)->getTypeKind());
}
TEST_F(TestType, getDatum) {
  {
    LOG_INFO("Testing invalid str");
    EXPECT_THROW(int1->getDatum("b"), TransactionAbortException);
    EXPECT_THROW(int2->getDatum("c"), TransactionAbortException);
    EXPECT_THROW(int4->getDatum("d"), TransactionAbortException);
    EXPECT_THROW(int8->getDatum("e"), TransactionAbortException);
    EXPECT_THROW(flp4->getDatum("f"), TransactionAbortException);
    EXPECT_THROW(flp8->getDatum("g"), TransactionAbortException);
    EXPECT_THROW(date4->getDatum("h"), TransactionAbortException);
    EXPECT_THROW(time8->getDatum("i"), TransactionAbortException);
    Timestamp ts;
    EXPECT_THROW(
        (dynamic_cast<TimestampType *>(timestamp16))->getDatum("j", &ts),
        TransactionAbortException);
    EXPECT_THROW(
        (dynamic_cast<TimestamptzType *>(timestamptz16))->getDatum("abc", &ts),
        TransactionAbortException);
    IntervalVar itv;
    EXPECT_THROW(
        (dynamic_cast<IntervalType *>(interval16))->getDatum("abc", &itv),
        TransactionAbortException);
  }
  {
    LOG_INFO("Testing out of range");
    EXPECT_THROW(int1->getDatum("256"), TransactionAbortException);
    EXPECT_THROW(int2->getDatum("65536"), TransactionAbortException);
    EXPECT_THROW(int4->getDatum("4294967296"), TransactionAbortException);
    EXPECT_THROW(int8->getDatum("18446744073709551616"),
                 TransactionAbortException);
    EXPECT_THROW(flp4->getDatum("1e39"), TransactionAbortException);
    EXPECT_THROW(flp4->getDatum("1e-39"), TransactionAbortException);
    EXPECT_THROW(flp8->getDatum("1e-309"), TransactionAbortException);
    EXPECT_THROW(date4->getDatum("18446744073709551616"),
                 TransactionAbortException);
    EXPECT_THROW(time8->getDatum("18446744073709551616-1-1"),
                 TransactionAbortException);
    Timestamp ts;
    EXPECT_THROW((dynamic_cast<TimestampType *>(timestamp16))
                     ->getDatum("18446744073709551616-1-1 00:00:00", &ts),
                 TransactionAbortException);
    EXPECT_THROW((dynamic_cast<TimestamptzType *>(timestamptz16))
                     ->getDatum("18446744073709551616-1-1 00:00:00", &ts),
                 TransactionAbortException);
    IntervalVar itv;
    EXPECT_THROW((dynamic_cast<IntervalType *>(interval16))
                     ->getDatum("100000000000000000000-1-0", &itv),
                 TransactionAbortException);
  }

  EXPECT_THROW(struct0->getDatum(""), TransactionAbortException);
  EXPECT_THROW(structEx->getDatum(""), TransactionAbortException);
  EXPECT_THROW(iobaseType->getDatum(""), TransactionAbortException);
}
TEST_F(TestType, DatumToString) {
  EXPECT_EQ("t", bool1->DatumToString(bool1->getDatum("t")));
  EXPECT_EQ("f", bool1->DatumToString(bool1->getDatum("f")));
  EXPECT_EQ("23", int1->DatumToString(int1->getDatum("23")));
  EXPECT_EQ("233", int2->DatumToString(int2->getDatum("233")));
  EXPECT_EQ("233", int4->DatumToString(int4->getDatum("233")));
  EXPECT_EQ("233", int8->DatumToString(int8->getDatum("233")));
  EXPECT_EQ("233.000000", flp4->DatumToString(flp4->getDatum("233")));
  EXPECT_EQ("233.000000", flp8->DatumToString(flp8->getDatum("233")));
  EXPECT_EQ("1970-08-22", date4->DatumToString(date4->getDatum("1970-08-22")));
  EXPECT_EQ("1970-01-22 BC",
            date4->DatumToString(date4->getDatum("1970-01-22 BC")));
  EXPECT_EQ("15:12:21.345",
            time8->DatumToString(time8->getDatum("15:12:21.345")));
  EXPECT_EQ("73120233",
            numeric0->DatumToString(numeric0->getDatum("73120233")));
  Timestamp ts;
  EXPECT_EQ("2018-01-16 15:24:21.345",
            timestamp16->DatumToString(
                (dynamic_cast<TimestampType *>(timestamp16))
                    ->getDatum("2018-01-16 15:24:21.345", &ts)));
  EXPECT_EQ("2018-01-16 15:24:21.345 BC",
            timestamp16->DatumToString(
                (dynamic_cast<TimestampType *>(timestamp16))
                    ->getDatum("2018-01-16 15:24:21.345 BC", &ts)));
  DecimalVar dec;
  EXPECT_EQ("7683512355.55112",
            decimal24->DatumToString((dynamic_cast<DecimalType *>(decimal24))
                                         ->getDatum("7683512355.55112", &dec)));
  EXPECT_EQ("233", str0->DatumToString(str0->getDatum("233")));
  EXPECT_THROW(struct0->DatumToString(CreateDatum(0)),
               TransactionAbortException);

  EXPECT_THROW(structEx->DatumToString(CreateDatum(0)),
               TransactionAbortException);
  EXPECT_THROW(iobaseType->DatumToString(CreateDatum(0)),
               TransactionAbortException);
}
TEST_F(TestType, DatumToBinary) {
  EXPECT_EQ(true, *reinterpret_cast<const bool *>(
                      bool1->DatumToBinary(bool1->getDatum("t")).c_str()));
  EXPECT_EQ(false, *reinterpret_cast<const bool *>(
                       bool1->DatumToBinary(bool1->getDatum("f")).c_str()));
  EXPECT_EQ(23, *reinterpret_cast<const int8_t *>(
                    int1->DatumToBinary(int1->getDatum("23")).c_str()));
  EXPECT_EQ(233, *reinterpret_cast<const int16_t *>(
                     int2->DatumToBinary(int2->getDatum("233")).c_str()));
  EXPECT_EQ(233, *reinterpret_cast<const int32_t *>(
                     int4->DatumToBinary(int4->getDatum("233")).c_str()));
  EXPECT_EQ(233, *reinterpret_cast<const int64_t *>(
                     int8->DatumToBinary(int8->getDatum("233")).c_str()));
  EXPECT_EQ(233, *reinterpret_cast<const float *>(
                     flp4->DatumToBinary(flp4->getDatum("233")).c_str()));
  EXPECT_EQ(233, *reinterpret_cast<const double *>(
                     flp8->DatumToBinary(flp8->getDatum("233")).c_str()));
  EXPECT_EQ(233,
            *reinterpret_cast<const int32_t *>(
                date4->DatumToBinary(date4->getDatum("1970-08-22")).c_str()));
  EXPECT_EQ(
      233,
      *reinterpret_cast<const int64_t *>(
          time8->DatumToBinary(time8->getDatum("00:00:00.000233")).c_str()));
  Timestamp ts;
  std::string str = timestamp16->DatumToBinary(
      (dynamic_cast<TimestampType *>(timestamp16))
          ->getDatum("1970-01-01 00:03:53.000000233", &ts));
  const Timestamp *tsnew = reinterpret_cast<const Timestamp *>(str.c_str());
  EXPECT_EQ(233, tsnew->second);
  EXPECT_EQ(233, tsnew->nanosecond);

  DecimalVar dec;
  std::string decStr = decimal24->DatumToBinary(
      (dynamic_cast<DecimalType *>(decimal24))
          ->getDatum("1080570562111159677.5332", &dec));
  const DecimalVar *decnew =
      reinterpret_cast<const DecimalVar *>(decStr.c_str());
  EXPECT_EQ(14360337991509079972, decnew->lowbits);
  EXPECT_EQ(585, decnew->highbits);
  EXPECT_EQ(4, decnew->scale);
  EXPECT_EQ("233", str0->DatumToBinary(str0->getDatum("233")));
  EXPECT_THROW(struct0->DatumToBinary(CreateDatum(0)),
               TransactionAbortException);

  EXPECT_THROW(structEx->DatumToBinary(CreateDatum(0)),
               TransactionAbortException);
}
TEST_F(TestType, compareEqual) {
  {  // equal
    EXPECT_EQ(0, bool1->compare(bool1->getDatum("t"), bool1->getDatum("t")));
    EXPECT_EQ(0, int1->compare(int1->getDatum("23"), int1->getDatum("23")));
    EXPECT_EQ(0, int2->compare(int2->getDatum("233"), int2->getDatum("233")));
    EXPECT_EQ(0, int4->compare(int4->getDatum("233"), int4->getDatum("233")));
    EXPECT_EQ(0, int8->compare(int8->getDatum("233"), int8->getDatum("233")));
    EXPECT_EQ(0, flp4->compare(flp4->getDatum("233"), flp4->getDatum("233")));
    EXPECT_EQ(0, flp8->compare(flp8->getDatum("233"), flp8->getDatum("233")));
    EXPECT_EQ(0, date4->compare(date4->getDatum("1970-08-22"),
                                date4->getDatum("1970-08-22")));
    EXPECT_EQ(0, time8->compare(time8->getDatum("15:12:21.345"),
                                time8->getDatum("15:12:21.345")));
    Timestamp ts1, ts2;
    EXPECT_EQ(0, timestamp16->compare(
                     (dynamic_cast<TimestampType *>(timestamp16))
                         ->getDatum("2018-01-16 16:51:12.123", &ts1),
                     (dynamic_cast<TimestampType *>(timestamp16))
                         ->getDatum("2018-01-16 16:51:12.123", &ts2)));
    EXPECT_EQ(0, str0->compare(str0->getDatum("233"), str0->getDatum("233")));
  }
  {  // less
    EXPECT_GT(0, bool1->compare(bool1->getDatum("f"), bool1->getDatum("t")));
    EXPECT_GT(0, int1->compare(int1->getDatum("2"), int1->getDatum("23")));
    EXPECT_GT(0, int2->compare(int2->getDatum("23"), int2->getDatum("233")));
    EXPECT_GT(0, int4->compare(int4->getDatum("23"), int4->getDatum("233")));
    EXPECT_GT(0, int8->compare(int8->getDatum("23"), int8->getDatum("233")));
    EXPECT_GT(0, flp4->compare(flp4->getDatum("23"), flp4->getDatum("233")));
    EXPECT_GT(0, date4->compare(date4->getDatum("1970-08-02"),
                                date4->getDatum("1970-08-22")));
    EXPECT_GT(0, time8->compare(time8->getDatum("15:12:21"),
                                time8->getDatum("15:12:21.345")));
    Timestamp ts1, ts2;
    EXPECT_GT(0, timestamp16->compare(
                     (dynamic_cast<TimestampType *>(timestamp16))
                         ->getDatum("2018-01-16 16:51:12", &ts1),
                     (dynamic_cast<TimestampType *>(timestamp16))
                         ->getDatum("2018-01-16 16:51:12.123", &ts2)));
    EXPECT_GT(0, flp8->compare(flp8->getDatum("23"), flp8->getDatum("233")));
    EXPECT_GT(0, str0->compare(str0->getDatum("23"), str0->getDatum("233")));
  }
  {  // greater
    EXPECT_LT(0, bool1->compare(bool1->getDatum("t"), bool1->getDatum("f")));
    EXPECT_LT(0, int1->compare(int1->getDatum("23"), int1->getDatum("2")));
    EXPECT_LT(0, int2->compare(int2->getDatum("233"), int2->getDatum("23")));
    EXPECT_LT(0, int4->compare(int4->getDatum("233"), int4->getDatum("23")));
    EXPECT_LT(0, int8->compare(int8->getDatum("233"), int8->getDatum("23")));
    EXPECT_LT(0, flp4->compare(flp4->getDatum("233"), flp4->getDatum("23")));
    EXPECT_LT(0, flp8->compare(flp8->getDatum("233"), flp8->getDatum("23")));
    EXPECT_LT(0, date4->compare(date4->getDatum("1970-08-22"),
                                date4->getDatum("1970-08-02")));
    EXPECT_LT(0, time8->compare(time8->getDatum("15:12:21.345"),
                                time8->getDatum("15:12:21")));
    Timestamp ts1, ts2;
    EXPECT_LT(
        0, timestamp16->compare((dynamic_cast<TimestampType *>(timestamp16))
                                    ->getDatum("2018-01-16 16:51:12.123", &ts1),
                                (dynamic_cast<TimestampType *>(timestamp16))
                                    ->getDatum("2018-01-16 16:51:12", &ts2)));
    EXPECT_LT(0, str0->compare(str0->getDatum("233"), str0->getDatum("23")));
  }
  EXPECT_THROW(struct0->compare(CreateDatum(0), CreateDatum(0)),
               TransactionAbortException);

  EXPECT_THROW(structEx->compare(CreateDatum(0), CreateDatum(0)),
               TransactionAbortException);

  EXPECT_THROW(iobaseType->compare(CreateDatum(0), CreateDatum(0)),
               TransactionAbortException);
}

TEST_F(TestType, getSubtypeCount) {
  EXPECT_THROW(bool1->getSubtypeCount(), TransactionAbortException);
  EXPECT_THROW(int1->getSubtypeCount(), TransactionAbortException);
  EXPECT_THROW(int2->getSubtypeCount(), TransactionAbortException);
  EXPECT_THROW(int4->getSubtypeCount(), TransactionAbortException);
  EXPECT_THROW(int8->getSubtypeCount(), TransactionAbortException);
  EXPECT_THROW(flp4->getSubtypeCount(), TransactionAbortException);
  EXPECT_THROW(flp8->getSubtypeCount(), TransactionAbortException);
  EXPECT_THROW(date4->getSubtypeCount(), TransactionAbortException);
  EXPECT_THROW(time8->getSubtypeCount(), TransactionAbortException);
  EXPECT_THROW(str0->getSubtypeCount(), TransactionAbortException);

  EXPECT_EQ(structEx->getSubtypeCount(), 1);
  EXPECT_THROW(iobaseType->getSubtypeCount(), TransactionAbortException);
}
TEST_F(TestType, getSubtype) {
  EXPECT_THROW(bool1->getSubtype(0), TransactionAbortException);
  EXPECT_THROW(int1->getSubtype(0), TransactionAbortException);
  EXPECT_THROW(int2->getSubtype(0), TransactionAbortException);
  EXPECT_THROW(int4->getSubtype(0), TransactionAbortException);
  EXPECT_THROW(int8->getSubtype(0), TransactionAbortException);
  EXPECT_THROW(flp4->getSubtype(0), TransactionAbortException);
  EXPECT_THROW(flp8->getSubtype(0), TransactionAbortException);
  EXPECT_THROW(date4->getSubtype(0), TransactionAbortException);
  EXPECT_THROW(time8->getSubtype(0), TransactionAbortException);
  EXPECT_THROW(timestamp16->getSubtype(0), TransactionAbortException);
  EXPECT_EQ(BOOLEANID, struct0->getSubtype(0)->getTypeKind());

  EXPECT_EQ(BOOLEANID, structEx->getSubtype(0)->getTypeKind());
  EXPECT_THROW(iobaseType->getSubtype(0), TransactionAbortException);
}
TEST_F(TestType, getFieldName) {
  EXPECT_THROW(bool1->getFieldName(0), TransactionAbortException);
  EXPECT_THROW(int1->getFieldName(0), TransactionAbortException);
  EXPECT_THROW(int2->getFieldName(0), TransactionAbortException);
  EXPECT_THROW(int4->getFieldName(0), TransactionAbortException);
  EXPECT_THROW(int8->getFieldName(0), TransactionAbortException);
  EXPECT_THROW(flp4->getFieldName(0), TransactionAbortException);
  EXPECT_THROW(flp8->getFieldName(0), TransactionAbortException);
  EXPECT_THROW(date4->getFieldName(0), TransactionAbortException);
  EXPECT_THROW(time8->getFieldName(0), TransactionAbortException);
  EXPECT_THROW(timestamp16->getFieldName(0), TransactionAbortException);
  EXPECT_THROW(str0->getFieldName(0), TransactionAbortException);
  EXPECT_EQ("bool", struct0->getFieldName(0));
  EXPECT_THROW(iobaseType->getFieldName(0), TransactionAbortException);
  EXPECT_EQ("bool", structEx->getFieldName(0));
}
TEST_F(TestType, addStructField) {
  EXPECT_THROW(bool1->addStructField("dummy", int1), TransactionAbortException);
  EXPECT_THROW(int1->addStructField("dummy", int1), TransactionAbortException);
  EXPECT_THROW(int2->addStructField("dummy", int2), TransactionAbortException);
  EXPECT_THROW(int4->addStructField("dummy", int4), TransactionAbortException);
  EXPECT_THROW(int8->addStructField("dummy", int8), TransactionAbortException);
  EXPECT_THROW(flp4->addStructField("dummy", flp4), TransactionAbortException);
  EXPECT_THROW(flp8->addStructField("dummy", flp8), TransactionAbortException);
  EXPECT_THROW(date4->addStructField("dummy", date4),
               TransactionAbortException);
  EXPECT_THROW(time8->addStructField("dummy", time8),
               TransactionAbortException);
  EXPECT_THROW(timestamp16->addStructField("dummy", timestamp16),
               TransactionAbortException);
  EXPECT_THROW(str0->addStructField("dummy", str0), TransactionAbortException);
  EXPECT_THROW(iobaseType->addStructField("dummy", str0),
               TransactionAbortException);
}
TEST_F(TestType, unsupported) {
  std::vector<TypeBase *> unsupportedTypes = {new UnionType(), new AnyType(),
                                              new UnknownType()};
  for (auto type : unsupportedTypes) {
    LOG_INFO("Testing %d", type->getTypeKind());
    EXPECT_THROW(type->isFixedSizeType(), TransactionAbortException);
    EXPECT_THROW(type->getTypeWidth(), TransactionAbortException);
    EXPECT_THROW(type->getVector(false), TransactionAbortException);
    EXPECT_THROW(type->getDatum("t"), TransactionAbortException);
    EXPECT_THROW(type->getVector(false), TransactionAbortException);
    EXPECT_THROW(type->DatumToBinary(CreateDatum(0)),
                 TransactionAbortException);
    EXPECT_THROW(type->DatumToString(CreateDatum(0)),
                 TransactionAbortException);
    EXPECT_THROW(type->compare(CreateDatum(0), CreateDatum(0)),
                 TransactionAbortException);
    EXPECT_THROW(type->getSubtypeCount(), TransactionAbortException);
    EXPECT_THROW(type->getSubtype(0), TransactionAbortException);
    EXPECT_THROW(type->getFieldName(0), TransactionAbortException);
    EXPECT_THROW(type->addStructField("dummy", type),
                 TransactionAbortException);
  }
}
}  // namespace dbcommon
