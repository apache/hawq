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

#include "storage/format/orc/orc-format-reader.h"

#include <list>
#include <memory>
#include <utility>

#include "dbcommon/common/vector.h"
#include "dbcommon/common/vector/decimal-vector.h"
#include "dbcommon/common/vector/list-vector.h"
#include "dbcommon/common/vector/struct-vector.h"
#include "dbcommon/common/vector/variable-length-vector.h"
#include "dbcommon/utils/global.h"
#include "dbcommon/utils/url.h"

#include "storage/format/orc/input-stream.h"

namespace storage {

void ORCFormatReader::beginRead(
    dbcommon::FileSystemManagerInterface *fsManager,
    const univplan::UnivPlanScanFileSplitListList *splits,
    std::vector<bool> *columnsToRead, uint32_t nTuplesPerBatch,
    const univplan::UnivPlanExprPolyList *predicateExprs,
    const dbcommon::TupleDesc *td, bool readStatsOnly) {
  assert(fsManager != nullptr && splits != nullptr);

  this->fsManager = fsManager;
  this->splits = splits;
  this->nTuplesPerBatch = nTuplesPerBatch;
  this->columnsToRead = columnsToRead;

  if (columnsToRead != nullptr) {
    std::list<uint64_t> toRead;
    for (uint32_t i = 0; i < columnsToRead->size(); i++) {
      if ((*columnsToRead)[i]) {
        toRead.push_back(i);
      }
    }

    opts.include(toRead);
  }

  opts.setPredicateExprs(predicateExprs);
  opts.setTupleDesc(td);
  opts.setReadStatsOnlyFlag(readStatsOnly);
}

void ORCFormatReader::startNewSplit() {
  assert(currentSplitIdx < splits->front()->splits_size());

  std::string splitFilename;
  splits->front()->splits_filename(currentSplitIdx, &splitFilename);
  bool reuseInputStream = false;
  if (currentSplitIdx > 0) {
    std::string splitFilenamePrev;
    splits->front()->splits_filename(currentSplitIdx - 1, &splitFilenamePrev);
    if (splitFilename == splitFilenamePrev) {
      reuseInputStream = true;
    }
  }
  std::unique_ptr<orc::InputStream> inputStream;
  if (reuseInputStream) {
    inputStream = orcReader->ownInputStream();
  } else {
    dbcommon::URL url(splitFilename);
    if (orcReader) {
      inputStream = orcReader->ownInputStream();
      inputStream.reset(nullptr);
    }
    inputStream = orc::readFile(fsManager->get(url.getNormalizedServiceName()),
                                url.getPath());
  }
  opts.range(splits->front()->splits_start(currentSplitIdx),
             splits->front()->splits_len(currentSplitIdx));
  orcReader = orc::createReader(std::move(inputStream), opts);

  if (batch == nullptr) {
    batch = orcReader->createRowBatch(this->nTuplesPerBatch);
  }
}

bool ORCFormatReader::hasSomethingToRead() {
  if (startAnotherSplit) {
    startAnotherSplit = false;
    currentSplitIdx++;
    while (true) {
      if (currentSplitIdx >= splits->front()->splits_size()) {
        return false;
      }
      // skip empty split
      if (splits->front()->splits_len(currentSplitIdx) > 0) {
        startNewSplit();
        return true;
      }
      currentSplitIdx++;
    }
  }
  return true;
}

dbcommon::TupleBatch::uptr ORCFormatReader::read() {
  while (hasSomethingToRead()) {
    if (batch && orcReader->next(*batch)) {
      return createTupleBatch(batch.get());
    } else {
      startAnotherSplit = true;
      if (orcReader && opts.getPredicateExprs())
        orcReader->collectPredicateStats(&scannedStripe, &skippedStripe);
    }
  }

  orcReader.reset(nullptr);
  if (batch) batch.reset(nullptr);
  return dbcommon::TupleBatch::uptr(nullptr);
}

void ORCFormatReader::endRead() {
  if (opts.getPredicateExprs())
    LOG_INFO("Predicate Info: current qe scan %u stripes, skip %u stripes",
             scannedStripe, skippedStripe);
}

void ORCFormatReader::reset() {
  startAnotherSplit = true;
  currentSplitIdx = -1;
  skippedStripe = 0;
  scannedStripe = 0;
}

dbcommon::TupleBatch::uptr ORCFormatReader::createTupleBatch(
    orc::ColumnVectorBatch *batch) {
  orc::StructVectorBatch *structBatch =
      dynamic_cast<orc::StructVectorBatch *>(batch);
  assert(structBatch != nullptr);

  uint32_t nCols = columnsToRead != nullptr ? columnsToRead->size()
                                            : structBatch->fields.size();
  dbcommon::TupleBatch::uptr tbatch(new dbcommon::TupleBatch(nCols));

  std::vector<orc::ColumnVectorBatch *>::iterator it =
      structBatch->fields.begin();

  tbatch->setNumOfRows(structBatch->numElements);

  for (uint32_t colIdx = 0; colIdx < nCols; colIdx++) {
    if (columnsToRead && !columnsToRead->at(colIdx)) {
      continue;
    }

    orc::ColumnVectorBatch *b = *it++;

    std::unique_ptr<dbcommon::Vector> v;
    if (b->getType() == orc::ORCTypeKind::LIST) {
      orc::ListVectorBatch *lb = dynamic_cast<orc::ListVectorBatch *>(b);
      v = lb->buildVector(lb->elements->getType());
    } else if (b->getType() == orc::ORCTypeKind::DECIMAL) {
      v = b->buildVector((opts.getTupleDesc())->getColumnType(colIdx));
    } else if (b->getType() == orc::ORCTypeKind::TIMESTAMP) {
      v = b->buildVector((opts.getTupleDesc())->getColumnType(colIdx));
    } else {
      v = b->buildVector();
    }

    if (b->hasStats && b->getType() != orc::ORCTypeKind::TIMESTAMP) {
      v->setVectorStatistics(b->stats);
      // append one dummy item
      v->append("1", false);
      tbatch->setColumn(colIdx, std::move(v), false);
      tbatch->setNumOfRows(1);
      continue;
    }

    switch (b->getType()) {
      case orc::ORCTypeKind::BOOLEAN:
      case orc::ORCTypeKind::BYTE:
      case orc::ORCTypeKind::SHORT:
      case orc::ORCTypeKind::INT:
      case orc::ORCTypeKind::LONG:
      case orc::ORCTypeKind::FLOAT:
      case orc::ORCTypeKind::DOUBLE:
      case orc::ORCTypeKind::DATE:
      case orc::ORCTypeKind::TIME: {
        v->setValue(b->getData(), b->numElements * b->getWidth());
        v->setHasNull(b->hasNulls);
        if (b->hasNulls) v->setNotNulls(b->getNotNull(), b->numElements);
        assert(v->isValid());
        break;
      }
      case orc::ORCTypeKind::TIMESTAMP: {
        v->setValue(b->getData(), b->numElements * b->getWidth() / 2);
        v->setNanoseconds(b->getNanoseconds(),
                          b->numElements * b->getWidth() / 2);
        v->setHasNull(b->hasNulls);
        if (b->hasNulls) v->setNotNulls(b->getNotNull(), b->numElements);
        assert(v->isValid());
        break;
      }
      case orc::ORCTypeKind::DECIMAL: {
        assert(dynamic_cast<dbcommon::DecimalVector *>(v.get()));
        uint64_t count = b->numElements;
        v->setAuxiliaryValue(b->getAuxiliaryData(),
                             b->numElements * b->getWidth() / 3);
        v->setValue(b->getData(), b->numElements * b->getWidth() / 3);
        v->setScaleValue(b->getScaleData(), b->numElements * b->getWidth() / 3);
        v->setHasNull(b->hasNulls);
        if (b->hasNulls) v->setNotNulls(b->getNotNull(), b->numElements);
        assert(v->isValid());
        break;
      }
      case orc::ORCTypeKind::CHAR:
      case orc::ORCTypeKind::VARCHAR:
      case orc::ORCTypeKind::STRING:
      case orc::ORCTypeKind::BINARY: {
        orc::BytesVectorBatch *sb = dynamic_cast<orc::BytesVectorBatch *>(b);
        v->setLengths(reinterpret_cast<uint64_t *>(sb->length.data()),
                      sb->numElements);
        // todo: memory leak? when is the ownership of values
        v->setValPtrs((const char **)sb->data.data(), sb->numElements);
        v->setHasNull(b->hasNulls);
        if (b->hasNulls) v->setNotNulls(b->getNotNull(), b->numElements);
        reinterpret_cast<dbcommon::StringVector *>(v.get())->setDirectEncoding(
            sb->isDirectEncoding);

        assert(v->isValid());
        break;
      }
      case orc::ORCTypeKind::LIST: {
        orc::ListVectorBatch *lb = dynamic_cast<orc::ListVectorBatch *>(b);
        dbcommon::ListVector *lv =
            dynamic_cast<dbcommon::ListVector *>(v.get());
        lv->setOffsets(reinterpret_cast<uint64_t *>(lb->offsets.data()),
                       lb->numElements + 1);
        orc::ColumnVectorBatch *clb = lb->elements.get();
        std::unique_ptr<dbcommon::Vector> clv = clb->buildVector();
        // Now only support fixed-length type list
        clv->setValue(clb->getData(), clb->numElements * clb->getWidth());
        clv->setHasNull(clb->hasNulls);
        if (clb->hasNulls)
          clv->setNotNulls(clb->getNotNull(), clb->numElements);
        assert(clv->isValid());
        lv->addChildVector(std::move(clv));
        lv->setHasNull(lb->hasNulls);
        if (lb->hasNulls) lv->setNotNulls(lb->getNotNull(), lb->numElements);
        assert(lv->isValid());
        break;
      }
      case orc::ORCTypeKind::STRUCT: {
        // XXX(chiyang): support struct vector only for aggregate intermediate
        // output, as a result of which ORC file serves as workfile
        orc::StructVectorBatch *structBatch =
            dynamic_cast<orc::StructVectorBatch *>(b);
        assert(structBatch->fields.size() == 2);
        assert(structBatch->fields[0]->getType() == orc::ORCTypeKind::DOUBLE ||
               structBatch->fields[0]->getType() == orc::ORCTypeKind::DECIMAL);
        assert(structBatch->fields[1]->getType() == orc::ORCTypeKind::LONG);

        bool isDecimal =
            structBatch->fields[0]->getType() == orc::ORCTypeKind::DECIMAL;
        auto vecSum = dbcommon::Vector::BuildVector(
            isDecimal ? dbcommon::TypeKind::DECIMALNEWID
                      : dbcommon::TypeKind::DOUBLEID,
            false);
        auto vecCount =
            dbcommon::Vector::BuildVector(dbcommon::TypeKind::BIGINTID, false);
        {
          auto b0 = structBatch->fields[0];
          if (isDecimal) {
            vecSum->setValue(b0->getData(),
                             b0->numElements * b0->getWidth() / 3);
            vecSum->setAuxiliaryValue(b0->getAuxiliaryData(),
                                      b0->numElements * b0->getWidth() / 3);
            vecSum->setScaleValue(b0->getScaleData(),
                                  b0->numElements * b0->getWidth() / 3);
          } else {
            vecSum->setValue(b0->getData(), b0->numElements * b0->getWidth());
          }
          vecSum->setHasNull(b0->hasNulls);
          if (b0->hasNulls)
            vecSum->setNotNulls(b0->getNotNull(), b0->numElements);
        }
        {
          auto b1 = structBatch->fields[1];
          vecCount->setValue(b1->getData(), b1->numElements * b1->getWidth());
          vecCount->setHasNull(b1->hasNulls);
          if (b1->hasNulls)
            vecCount->setNotNulls(b1->getNotNull(), b1->numElements);
        }

        v->addChildVector(std::move(vecSum));
        v->addChildVector(std::move(vecCount));
        v->setTypeKind(isDecimal
                           ? dbcommon::TypeKind::AVG_DECIMAL_TRANS_DATA_ID
                           : dbcommon::TypeKind::AVG_DOUBLE_TRANS_DATA_ID);
        break;
      }
      default:
        LOG_ERROR(ERRCODE_FEATURE_NOT_SUPPORTED, "type %d not supported yet",
                  b->getType());
        break;
    }

    tbatch->setColumn(colIdx, std::move(v), false);
  }

  return std::move(tbatch);
}

}  // namespace storage
