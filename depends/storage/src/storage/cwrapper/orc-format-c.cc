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

#include "storage/cwrapper/orc-format-c.h"

#include <uuid/uuid.h>

#include <memory>
#include <string>
#include <vector>

#include "dbcommon/common/vector-transformer.h"
#include "dbcommon/common/vector/decimal-vector.h"
#include "dbcommon/common/vector/timestamp-vector.h"
#include "dbcommon/filesystem/file-system.h"
#include "dbcommon/function/decimal-function.h"
#include "dbcommon/function/typecast-func.cg.h"
#include "dbcommon/type/date.h"
#include "dbcommon/type/decimal.h"
#include "dbcommon/type/type-kind.h"
#include "dbcommon/utils/global.h"
#include "dbcommon/utils/url.h"

#include "storage/format/format.h"
#include "storage/format/orc/orc-format.h"

#include "univplan/univplanbuilder/univplanbuilder-scan-task.h"

#define NUMERIC_POS 0x0000
#define NUMERIC_NEG 0x4000
#define DEC_DIGITS 4
#define NUMERIC_DSCALE_MASK 0x3FF
#define NUMERIC_HDRSZ (sizeof(int32_t) + sizeof(uint16_t) + sizeof(int16_t))

#ifdef __cplusplus
extern "C" {
#endif

static void ORCFormatSetErrorORCFormatC(ORCFormatCatchedError *ce, int errCode,
                                        const char *errMsg);

typedef struct OrcColumnReader {
  dbcommon::TypeKind type;
  const char *value;
  const bool *nulls;
  const uint64_t *lens;
  std::unique_ptr<dbcommon::ByteBuffer> valBuffer;
} OrcColumnReader;

struct ORCFormatC {
  std::unique_ptr<storage::ORCFormat> orcFormat;  // NOLINT
  dbcommon::URL::uptr url;
  dbcommon::Parameters params;
  dbcommon::TupleDesc desc;
  ORCFormatCatchedError error;
  std::vector<bool> columnsToRead;
  univplan::UnivPlanScanFileSplitListList splits;
  dbcommon::TupleBatch::uptr tb;
  std::string insertFileName;

  bool needNewTupleBatch;
  uint64_t rowRead;
  uint64_t rowCount;
  std::vector<std::unique_ptr<OrcColumnReader>> columnReaders;
  std::vector<uint32_t> colToReadIds;
};

typedef struct NumericTransData {
  int32_t varlen;        // total size counted in byte
  int16_t weight;        // size of integral part, counted in int16_t
  uint16_t sign_dscale;  // sign and scale
  int16_t digits[0];
} NumericTransData;

ORCFormatC *ORCFormatNewORCFormatC(const char *tableOptions, int segno) {
  ORCFormatC *instance = new ORCFormatC();
  univplan::UNIVPLANFORMATTYPE type = univplan::UNIVPLANFORMATTYPE::ORC_FORMAT;

  instance->params.set("table.options", tableOptions);
  instance->orcFormat.reset(new storage::ORCFormat(&(instance->params)));
  instance->orcFormat->setFileSystemManager(&FSManager);
  instance->tb = nullptr;
  instance->url = nullptr;
  instance->error.errCode = ERRCODE_SUCCESSFUL_COMPLETION;
  instance->insertFileName = "/" + std::to_string(segno + 1) + "_";
  return instance;
}

void ORCFormatBeginORCFormatC(ORCFormatC *fmt, ORCFormatFileSplit *splits,
                              int numSplits, bool *columnsToRead,
                              char **columnName, int *columnDatatype,
                              uint64_t *columnDatatypeMod, int numColumns) {
  try {
    fmt->tb = nullptr;
    fmt->needNewTupleBatch = true;
    for (int i = 0; i < numColumns; ++i) {
      fmt->columnsToRead.push_back(columnsToRead[i]);
      fmt->desc.add(columnName[i],
                    (static_cast<dbcommon::TypeKind>(columnDatatype[i])),
                    columnDatatypeMod[i]);
      if (columnsToRead[i]) {
        std::unique_ptr<OrcColumnReader> columnReader(new OrcColumnReader);
        columnReader->type = static_cast<dbcommon::TypeKind>(columnDatatype[i]);
        switch (columnReader->type) {
          case dbcommon::TypeKind::STRINGID:
          case dbcommon::TypeKind::CHARID:
          case dbcommon::TypeKind::VARCHARID:
          case dbcommon::TypeKind::BINARYID:
          case dbcommon::TypeKind::TIMESTAMPID:
          case dbcommon::TypeKind::TIMESTAMPTZID:
          case dbcommon::TypeKind::DECIMALID:
            columnReader->valBuffer.reset(new dbcommon::ByteBuffer(true));
            columnReader->valBuffer->reserve(DEFAULT_RESERVED_SIZE_OF_STRING *
                                             DEFAULT_NUMBER_TUPLES_PER_BATCH);
            break;
          default:
            columnReader->valBuffer = nullptr;
            break;
        }
        fmt->columnReaders.push_back(std::move(columnReader));
        fmt->colToReadIds.push_back(i);
      }
    }

    // create one scan task to contain all splits
    univplan::UnivPlanBuilderScanTask scanTaskBld;
    // add all splits into scan task
    for (int j = 0; j < numSplits; ++j) {
      scanTaskBld.addScanFileSplit(splits[j].fileName, splits[j].start,
                                   splits[j].len, -1, -1);  // no rangeid, rgid
    }
    // build scan task by transfering tb from this builder to fmt instance
    std::unique_ptr<univplan::UnivPlanScanFileSplitListTb> newScanTask(
        new univplan::UnivPlanScanFileSplitListTb(
            std::move(scanTaskBld.releaseSplitsTb())));
    fmt->splits.push_back(std::move(newScanTask));

    fmt->orcFormat->beginScan(&(fmt->splits), &(fmt->desc),
                              &(fmt->columnsToRead), nullptr, nullptr, false);
  } catch (dbcommon::TransactionAbortException &e) {
    ORCFormatSetErrorORCFormatC(&(fmt->error), e.errCode(), e.what());
  }
}

void ORCFormatRescanORCFormatC(ORCFormatC *fmt) {
  try {
    fmt->orcFormat->reScan();
  } catch (dbcommon::TransactionAbortException &e) {
    ORCFormatSetErrorORCFormatC(&(fmt->error), e.errCode(), e.what());
  }
}

void ORCFormatEndORCFormatC(ORCFormatC *fmt) {
  try {
    fmt->orcFormat->endScan();
  } catch (dbcommon::TransactionAbortException &e) {
    ORCFormatSetErrorORCFormatC(&(fmt->error), e.errCode(), e.what());
  }
}

void ORCFormatBeginInsertORCFormatC(ORCFormatC *fmt, const char *dirFullPath,
                                    char **columnName, int *columnDatatype,
                                    uint64_t *columnDatatypeMod,
                                    int numColumns) {
  try {
    fmt->tb = nullptr;
    for (int i = 0; i < numColumns; ++i) {
      fmt->desc.add(columnName[i],
                    (static_cast<dbcommon::TypeKind>(columnDatatype[i])),
                    columnDatatypeMod[i]);
    }

    std::string dirFullInsertPath(dirFullPath);
    dirFullInsertPath += INSERT_HIDDEN_DIR;
    fmt->url.reset(new dbcommon::URL(dirFullInsertPath));
    dbcommon::FileSystem *fs = FSManager.get(dirFullPath);
    std::string targetPath = fmt->url->getPath();
    std::string targetRawPath = fmt->url->getRawString();
    if (!fs->exists(targetPath.c_str())) {
      LOG_ERROR(ERRCODE_DATA_EXCEPTION, "no data directory found: %s",
                targetPath.c_str());
    }
    // Generate filename for current insertion.
    uuid_t uuid;
    char buf[1024];
    uuid_generate_time(uuid);
    uuid_unparse(uuid, buf);
    fmt->insertFileName.append(buf, strlen(buf));

    fmt->orcFormat->beginInsert(targetRawPath + fmt->insertFileName, fmt->desc);
  } catch (dbcommon::TransactionAbortException &e) {
    ORCFormatSetErrorORCFormatC(&(fmt->error), e.errCode(), e.what());
  }
}

void ORCFormatInsertORCFormatC(ORCFormatC *fmt, int *datatypes, char **values,
                               uint64_t *lens, unsigned char **nullBitmap,
                               int32_t **dims, bool *isNull) {
  try {
    if (fmt->tb == nullptr)
      fmt->tb.reset(new dbcommon::TupleBatch(fmt->desc, true));

    dbcommon::TupleBatchWriter &writers = fmt->tb->getTupleBatchWriter();
    int natts = fmt->desc.getNumOfColumns();

    for (int i = 0; i < natts; ++i) {
      dbcommon::TypeKind datatype =
          (static_cast<dbcommon::TypeKind>(datatypes[i]));
      switch (datatype) {
        case dbcommon::TypeKind::BOOLEANID:
          writers[i]->append(reinterpret_cast<char *>(values[i]), sizeof(bool),
                             isNull[i]);
          break;

        case dbcommon::TypeKind::TINYINTID:
          writers[i]->append(reinterpret_cast<char *>(values[i]),
                             sizeof(int8_t), isNull[i]);
          break;

        case dbcommon::TypeKind::SMALLINTID:
          writers[i]->append(reinterpret_cast<char *>(values[i]),
                             sizeof(int16_t), isNull[i]);
          break;

        case dbcommon::TypeKind::INTID:
        case dbcommon::TypeKind::DATEID:
          writers[i]->append(reinterpret_cast<char *>(values[i]),
                             sizeof(int32_t), isNull[i]);
          break;

        case dbcommon::TypeKind::BIGINTID:
        case dbcommon::TypeKind::TIMEID:
          writers[i]->append(reinterpret_cast<char *>(values[i]),
                             sizeof(int64_t), isNull[i]);
          break;

        case dbcommon::TypeKind::FLOATID:
          writers[i]->append(reinterpret_cast<char *>(values[i]), sizeof(float),
                             isNull[i]);
          break;

        case dbcommon::TypeKind::DOUBLEID:
          writers[i]->append(reinterpret_cast<char *>(values[i]),
                             sizeof(double), isNull[i]);
          break;

        case dbcommon::TypeKind::CHARID:
        case dbcommon::TypeKind::VARCHARID:
        case dbcommon::TypeKind::STRINGID:
        case dbcommon::TypeKind::BINARYID:
        case dbcommon::TypeKind::DECIMALID:
          writers[i]->append(reinterpret_cast<char *>(values[i]), isNull[i]);
          break;

        case dbcommon::TypeKind::TIMESTAMPID:
        case dbcommon::TypeKind::TIMESTAMPTZID:
          writers[i]->append(reinterpret_cast<char *>(values[i]),
                             sizeof(int64_t) + sizeof(int64_t), isNull[i]);
          break;

        case dbcommon::TypeKind::SMALLINTARRAYID:
        case dbcommon::TypeKind::INTARRAYID:
        case dbcommon::TypeKind::BIGINTARRAYID:
        case dbcommon::TypeKind::FLOATARRAYID:
        case dbcommon::TypeKind::DOUBLEARRAYID: {
          dbcommon::ListVector *lwriter =
              reinterpret_cast<dbcommon::ListVector *>(writers[i].get());
          lwriter->append(reinterpret_cast<char *>(values[i]), lens[i],
                          nullBitmap[i], dims[i], isNull[i], true);
          break;
        }
        case dbcommon::TypeKind::INVALIDTYPEID:
          LOG_ERROR(ERRCODE_DATA_EXCEPTION, "data type with id %d is invalid",
                    static_cast<int>(datatype));

        default:
          LOG_ERROR(ERRCODE_DATA_EXCEPTION,
                    "data type with id %d is not supported yet",
                    static_cast<int>(datatype));
          break;
      }
    }

    fmt->tb->incNumOfRows(1);
    if (fmt->tb->getNumOfRows() >= storage::Format::kTuplesPerBatch) {
      fmt->orcFormat->doInsert(std::move(fmt->tb));
      fmt->tb = nullptr;
    }
  } catch (dbcommon::TransactionAbortException &e) {
    ORCFormatSetErrorORCFormatC(&(fmt->error), e.errCode(), e.what());
  }
}

void ORCFormatEndInsertORCFormatC(ORCFormatC *fmt) {
  try {
    if (fmt->tb) fmt->orcFormat->doInsert(std::move(fmt->tb));  // NOLINT
    fmt->orcFormat->endInsert();
    dbcommon::FileSystem *fs = FSManager.get(fmt->url->getRawString());
    fs->rename((fmt->url->getPath() + fmt->insertFileName).c_str(),
               (fmt->url->getPath() + "/.." + fmt->insertFileName).c_str());
  } catch (dbcommon::TransactionAbortException &e) {
    ORCFormatSetErrorORCFormatC(&(fmt->error), e.errCode(), e.what());
  }
}

void ORCFormatFreeORCFormatC(ORCFormatC **fmt) {
  if (*fmt == nullptr) return;
  delete *fmt;
  *fmt = nullptr;
}

ORCFormatCatchedError *ORCFormatGetErrorORCFormatC(ORCFormatC *fmt) {
  return &(fmt->error);
}

void ORCFormatSetErrorORCFormatC(ORCFormatCatchedError *ce, int errCode,
                                 const char *errMsg) {
  assert(ce != nullptr);
  ce->errCode = errCode;
  snprintf(ce->errMessage, strlen(errMsg) + 1, "%s", errMsg);
}

static void textRelatedGetValueBuffer(ORCFormatC *fmt, dbcommon::BytesVector *v,
                                      OrcColumnReader *reader) {
  bool hasNull = v->hasNullValue();
  const uint64_t *lens = v->getLengths();
  const char **valPtrs = v->getValPtrs();
  reader->valBuffer->clear();
  reader->lens = lens;
  if (hasNull) {
    const bool *nulls = v->getNullBuffer()->getBools();
    for (uint64_t i = 0; i < fmt->rowCount; ++i) {
      if (!nulls[i]) {
        uint32_t len = lens[i];
        reader->valBuffer->append(len);
        reader->valBuffer->append(valPtrs[i], len);
      }
    }
    reader->nulls = nulls;
  } else {
    for (uint64_t i = 0; i < fmt->rowCount; ++i) {
      uint32_t len = lens[i];
      reader->valBuffer->append(len);
      reader->valBuffer->append(valPtrs[i], len);
    }
    reader->nulls = nullptr;
  }
  reader->value = reader->valBuffer->data();
}

static void timestampGetValueBuffer(ORCFormatC *fmt,
                                    dbcommon::TimestampVector *v,
                                    OrcColumnReader *reader) {
  bool hasNull = v->hasNullValue();
  const char **valPtrs = v->getValPtrs();
  const int64_t *second = reinterpret_cast<const int64_t *>(v->getValue());
  const int64_t *nanosecond =
      reinterpret_cast<const int64_t *>(v->getNanoseconds());
  reader->valBuffer->clear();
  if (hasNull) {
    const bool *nulls = v->getNullBuffer()->getBools();
    for (uint64_t i = 0; i < fmt->rowCount; ++i) {
      if (!nulls[i]) {
        int64_t val = (second[i] - TIMESTAMP_EPOCH_JDATE) * 1000000 +
                      nanosecond[i] / 1000;
        reader->valBuffer->append(val);
      }
    }
    reader->nulls = nulls;
  } else {
    for (uint64_t i = 0; i < fmt->rowCount; ++i) {
      int64_t val =
          (second[i] - TIMESTAMP_EPOCH_JDATE) * 1000000 + nanosecond[i] / 1000;
      reader->valBuffer->append(val);
    }
    reader->nulls = nullptr;
  }
  reader->value = reader->valBuffer->data();
}

static void decimalGetValueBuffer(dbcommon::DecimalVector *srcVector,
                                  OrcColumnReader *reader) {
  dbcommon::DecimalVectorRawData src(srcVector);

  auto convertNumericTranData = [&](uint64_t plainIdx) {
    NumericTransData numeric;
    dbcommon::Int128 data(src.hightbits[plainIdx], src.lowbits[plainIdx]);

    numeric.sign_dscale = NUMERIC_POS;
    if (data.isNegative()) {
      numeric.sign_dscale = NUMERIC_NEG;
      data = data.negate();
    }

    // Pad zero for fractional part in order to make it counted by int16_t
    int16_t scaleDigitCount = src.scales[plainIdx];
    int16_t paddingDigitCount =
        (DEC_DIGITS - scaleDigitCount % DEC_DIGITS) % DEC_DIGITS;
    int16_t significantDigitCount = data.getNumOfDigit();

    bool isPaddingSuffix = significantDigitCount > scaleDigitCount;
    int16_t totalDigitCount = isPaddingSuffix
                                  ? significantDigitCount + paddingDigitCount
                                  : scaleDigitCount + paddingDigitCount;

    numeric.sign_dscale |= (scaleDigitCount & NUMERIC_DSCALE_MASK);
    numeric.weight = isPaddingSuffix ? (totalDigitCount - scaleDigitCount -
                                        paddingDigitCount + (DEC_DIGITS - 1)) /
                                               DEC_DIGITS -
                                           1
                                     : -1;

    // In particular, if the value is zero, there will be no digits at all
    if (significantDigitCount == 0) totalDigitCount = 0;

    numeric.varlen =
        NUMERIC_HDRSZ +
        ((totalDigitCount + DEC_DIGITS - 1) / DEC_DIGITS) * sizeof(int16_t);

    // Reserver buffer
    reader->valBuffer->resize(reader->valBuffer->size() + numeric.varlen);

    // Fill header
    *reinterpret_cast<NumericTransData *>(reader->valBuffer->tail() -
                                          numeric.varlen) = numeric;

    //  Fill digits
    __int128_t dividend =
        (__int128_t(data.getHighBits()) << 64) + __int128_t(data.getLowBits());
    for (int i = 0; i < paddingDigitCount; i++) dividend *= 10;
    int16_t *ptr = reinterpret_cast<int16_t *>(reader->valBuffer->tail());
    for (int i = 0; i < (numeric.varlen - NUMERIC_HDRSZ) / sizeof(int16_t);
         i++) {
      int16_t remainder = dividend % 10000;
      *--ptr = remainder;
      dividend /= 10000;
    }
    assert(reinterpret_cast<char *>(ptr) ==
           reader->valBuffer->tail() - numeric.varlen + NUMERIC_HDRSZ);
  };
  reader->valBuffer->clear();
  dbcommon::transformVector(src.plainSize, src.sel, src.nulls,
                            convertNumericTranData);

  reader->nulls = srcVector->getNulls();
  reader->value = reader->valBuffer->data();
}

static void columnReadGetContent(ORCFormatC *fmt) {
  const dbcommon::TupleBatchReader &tbReader = fmt->tb->getTupleBatchReader();
  int32_t colIndex = 0;
  for (auto plainColIndex : fmt->colToReadIds) {
    OrcColumnReader *colReader = fmt->columnReaders[colIndex++].get();
    switch (colReader->type) {
      case dbcommon::TypeKind::STRINGID:
      case dbcommon::TypeKind::CHARID:
      case dbcommon::TypeKind::VARCHARID:
      case dbcommon::TypeKind::BINARYID: {
        dbcommon::BytesVector *v = dynamic_cast<dbcommon::BytesVector *>(
            tbReader[plainColIndex].get());
        textRelatedGetValueBuffer(fmt, v, colReader);
        break;
      }
      case dbcommon::TypeKind::TIMESTAMPID:
      case dbcommon::TypeKind::TIMESTAMPTZID: {
        dbcommon::TimestampVector *v =
            dynamic_cast<dbcommon::TimestampVector *>(
                tbReader[plainColIndex].get());
        timestampGetValueBuffer(fmt, v, colReader);
        break;
      }
      case dbcommon::TypeKind::DECIMALID: {
        dbcommon::DecimalVector *v = dynamic_cast<dbcommon::DecimalVector *>(
            tbReader[plainColIndex].get());
        decimalGetValueBuffer(v, colReader);
        break;
      }
      case dbcommon::TypeKind::BOOLEANID:
      case dbcommon::TypeKind::SMALLINTID:
      case dbcommon::TypeKind::INTID:
      case dbcommon::TypeKind::BIGINTID:
      case dbcommon::TypeKind::FLOATID:
      case dbcommon::TypeKind::DOUBLEID:
      case dbcommon::TypeKind::DATEID:
      case dbcommon::TypeKind::TIMEID: {
        dbcommon::Vector *v =
            dynamic_cast<dbcommon::Vector *>(tbReader[plainColIndex].get());
        if (v->hasNullValue()) {
          colReader->nulls = v->getNullBuffer()->getBools();
        } else {
          colReader->nulls = nullptr;
        }
        colReader->value = v->getValue();
        break;
      }
      default: {
        LOG_ERROR(ERRCODE_DATA_EXCEPTION, "not supported yet");
        break;
      }
    }
  }
}

bool ORCFormatNextORCFormatC(ORCFormatC *fmt, const char **values,
                             uint64_t *lens, bool *nulls) {
  try {
  begin:
    if (fmt->needNewTupleBatch) {
      fmt->tb = fmt->orcFormat->next();
      if (fmt->tb == nullptr) {
        return false;
      }
      fmt->needNewTupleBatch = false;
      fmt->rowRead = 0;
      fmt->rowCount = fmt->tb->getNumOfRows();
      if (fmt->rowCount > 0) columnReadGetContent(fmt);
    }

    if (fmt->rowRead < fmt->rowCount) {
      int32_t colIndex = 0;
      for (auto plainColIndex : fmt->colToReadIds) {
        OrcColumnReader *reader = fmt->columnReaders[colIndex++].get();
        switch (reader->type) {
          case dbcommon::TypeKind::STRINGID:
          case dbcommon::TypeKind::CHARID:
          case dbcommon::TypeKind::VARCHARID:
          case dbcommon::TypeKind::BINARYID: {
            if (reader->nulls && reader->nulls[fmt->rowRead]) {
              nulls[plainColIndex] = true;
            } else {
              nulls[plainColIndex] = false;
              values[plainColIndex] = reader->value;
              lens[plainColIndex] = reader->lens[fmt->rowRead] + 4;
              reader->value += lens[plainColIndex];
            }
            break;
          }
          case dbcommon::TypeKind::BOOLEANID: {
            if (reader->nulls && reader->nulls[fmt->rowRead]) {
              nulls[plainColIndex] = true;
            } else {
              nulls[plainColIndex] = false;
              values[plainColIndex] = reader->value;
            }
            reader->value += 1;
            break;
          }
          case dbcommon::TypeKind::SMALLINTID: {
            if (reader->nulls && reader->nulls[fmt->rowRead]) {
              nulls[plainColIndex] = true;
            } else {
              nulls[plainColIndex] = false;
              values[plainColIndex] = reader->value;
            }
            reader->value += 2;
            break;
          }
          case dbcommon::TypeKind::INTID:
          case dbcommon::TypeKind::FLOATID:
          case dbcommon::TypeKind::DATEID: {
            if (reader->nulls && reader->nulls[fmt->rowRead]) {
              nulls[plainColIndex] = true;
            } else {
              nulls[plainColIndex] = false;
              values[plainColIndex] = reader->value;
            }
            reader->value += 4;
            break;
          }
          case dbcommon::TypeKind::BIGINTID:
          case dbcommon::TypeKind::DOUBLEID:
          case dbcommon::TypeKind::TIMEID: {
            if (reader->nulls && reader->nulls[fmt->rowRead]) {
              nulls[plainColIndex] = true;
            } else {
              nulls[plainColIndex] = false;
              values[plainColIndex] = reader->value;
            }
            reader->value += 8;
            break;
          }
          case dbcommon::TypeKind::TIMESTAMPID:
          case dbcommon::TypeKind::TIMESTAMPTZID: {
            if (reader->nulls && reader->nulls[fmt->rowRead]) {
              nulls[plainColIndex] = true;
            } else {
              nulls[plainColIndex] = false;
              values[plainColIndex] = reader->value;
              reader->value += 8;
            }
            break;
          }
          case dbcommon::TypeKind::DECIMALID: {
            if (reader->nulls && reader->nulls[fmt->rowRead]) {
              nulls[plainColIndex] = true;
            } else {
              nulls[plainColIndex] = false;
              values[plainColIndex] = reader->value;
              lens[plainColIndex] =
                  (reinterpret_cast<const NumericTransData *>(reader->value))
                      ->varlen;
              reader->value += lens[plainColIndex];
            }
            break;
          }
          default: {
            LOG_ERROR(ERRCODE_DATA_EXCEPTION, "not supported yet");
            break;
          }
        }
      }
      ++fmt->rowRead;
    } else {
      fmt->needNewTupleBatch = true;
      goto begin;
    }
    return true;
  } catch (dbcommon::TransactionAbortException &e) {
    ORCFormatSetErrorORCFormatC(&(fmt->error), e.errCode(), e.what());
    return false;
  }
}

#ifdef __cplusplus
}
#endif
