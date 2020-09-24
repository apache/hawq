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

#include "dbcommon/common/tuple-batch.h"
#include "dbcommon/common/vector/decimal-vector.h"
#include "dbcommon/common/vector/fixed-length-vector.h"
#include "dbcommon/common/vector/interval-vector.h"
#include "dbcommon/common/vector/list-vector.h"
#include "dbcommon/common/vector/struct-vector.h"
#include "dbcommon/common/vector/timestamp-vector.h"
#include "dbcommon/common/vector/variable-length-vector.h"
#include "dbcommon/type/type-util.h"
#include "dbcommon/utils/macro.h"

namespace dbcommon {

std::unique_ptr<Vector> Vector::BuildVector(TypeKind type, bool ownData,
                                            int64_t typeModifier) {
  std::unique_ptr<Vector> ret;
  switch (type) {
    case TINYINTID:
      ret = std::unique_ptr<Vector>(new TinyIntVector(ownData));
      break;
    case SMALLINTID:
      ret = std::unique_ptr<Vector>(new SmallIntVector(ownData));
      break;
    case INTID:
      ret = std::unique_ptr<Vector>(new IntVector(ownData));
      break;
    case BIGINTID:
      ret = std::unique_ptr<Vector>(new BigIntVector(ownData));
      break;
    case FLOATID:
      ret = std::unique_ptr<Vector>(new FloatVector(ownData));
      break;
    case DOUBLEID:
      ret = std::unique_ptr<Vector>(new DoubleVector(ownData));
      break;
    case TIMESTAMPID:
      ret = std::unique_ptr<Vector>(new TimestampVector(ownData));
      break;
    case TIMESTAMPTZID:
      ret = std::unique_ptr<Vector>(new TimestamptzVector(ownData));
      break;
    case DATEID:
      ret = std::unique_ptr<Vector>(new DateVector(ownData));
      break;
    case TIMEID:
      ret = std::unique_ptr<Vector>(new TimeVector(ownData));
      break;
    case INTERVALID:
      ret = std::unique_ptr<Vector>(new IntervalVector(ownData));
      break;
    case STRINGID:
      ret = std::unique_ptr<Vector>(new StringVector(ownData));
      break;
    case DECIMALID:
      ret = std::unique_ptr<Vector>(new NumericVector(ownData));
      break;
    case DECIMALNEWID:
      ret = std::unique_ptr<Vector>(new DecimalVector(ownData));
      break;
    case VARCHARID:
      if (typeModifier == -1)
        ret = std::unique_ptr<Vector>(new StringVector(ownData));
      else
        ret = std::unique_ptr<Vector>(
            new VaryingCharVector(ownData, typeModifier));
      break;
    case CHARID:
      if (typeModifier == -1)
        ret = std::unique_ptr<Vector>(new StringVector(ownData));
      else
        ret = std::unique_ptr<Vector>(
            new BlankPaddedCharVector(ownData, typeModifier));
      break;
    case BOOLEANID:
      ret = std::unique_ptr<Vector>(new BooleanVector(ownData));
      break;
    case BINARYID:
      ret = std::unique_ptr<Vector>(new BinaryVector(ownData));
      break;
    case STRUCTID:
      ret = std::unique_ptr<Vector>(new StructVector(ownData));
      break;
    case SMALLINTARRAYID:
      ret = std::unique_ptr<Vector>(new SmallIntArrayVector(ownData));
      break;
    case INTARRAYID:
      ret = std::unique_ptr<Vector>(new IntArrayVector(ownData));
      break;
    case BIGINTARRAYID:
      ret = std::unique_ptr<Vector>(new BigIntArrayVector(ownData));
      break;
    case FLOATARRAYID:
      ret = std::unique_ptr<Vector>(new FloatArrayVector(ownData));
      break;
    case DOUBLEARRAYID:
      ret = std::unique_ptr<Vector>(new DoubleArrayVector(ownData));
      break;
    case IOBASETYPEID:
      ret = std::unique_ptr<Vector>(new IOBaseTypeVector(ownData));
      break;
    case STRUCTEXID:
      ret = std::unique_ptr<Vector>(new StructExTypeVector(ownData));
      break;
    case MAGMATID:
      ret = std::unique_ptr<Vector>(new MagmaTidVector(ownData));
      break;
    case AVG_DOUBLE_TRANS_DATA_ID:
      ret = std::unique_ptr<Vector>(new StructVector(ownData));
      ret->setTypeKind(AVG_DOUBLE_TRANS_DATA_ID);
      break;
    case AVG_DECIMAL_TRANS_DATA_ID:
      ret = std::unique_ptr<Vector>(new StructVector(ownData));
      ret->setTypeKind(AVG_DECIMAL_TRANS_DATA_ID);
      break;
    default:
      LOG_ERROR(ERRCODE_INTERNAL_ERROR, "Not supported type %d", type);
  }
  return std::move(ret);
}

bool Vector::isValid() const {
  if (this->getParentTupleBatch() != nullptr) {
    assert(getNumOfRows() == this->getParentTupleBatch()->getNumOfRows());
    if (selectListOwner == nullptr) {
      // means no selected list, for example, just cloned from another vector
      assert(this->selected == nullptr ||
             this->selected == belongingTupleBatch->getSelected());
    }
  }

  return true;
}

void Vector::setSelected(const SelectList *selected, bool fromBatch) {
  if (fromBatch) {
    assert(selectListOwner == nullptr && "expect plain vector");
    this->selected = const_cast<SelectList *>(selected);
  } else {
    if (selectListOwner) {
      assert(this->selected == selectListOwner.get());
      selectListOwner->resize(0);
    } else {
      selectListOwner.reset(new SelectList);
      this->selected = selectListOwner.get();
    }
    *(this->selected) = (*selected);
  }
  if (typeKind == TypeKind::STRUCTID) {
    for (auto &child : childs) {
      child->setSelected(selected, fromBatch);
    }
  }
}

bool Vector::equal(const Vector &a, const Vector &b) {
  if (a.getTypeKind() != b.getTypeKind() ||
      a.getNumOfRows() != b.getNumOfRows()) {
    return false;
  }

  uint64_t nA = a.getNumOfRows();
  for (uint64_t i = 0; i < nA; i++) {
    if (!equal(a, i, b, i)) return false;
  }

  return true;
}

bool Vector::equal(const Vector &a, uint64_t index1, const Vector &b,
                   uint64_t index2) {
  if (a.getTypeKind() != b.getTypeKind()) {
    return false;
  }

  uint64_t l1, l2;
  bool n1, n2;

  auto *d1 = a.read(index1, &l1, &n1);
  auto *d2 = b.read(index2, &l2, &n2);

  if (n1 != n2 || l1 != l2) {
    return false;
  }

  if (n1 || 0 == memcmp(d1, d2, l1)) {
    return true;
  }

  return false;
}

std::string Vector::toString() const {
  std::stringstream ss;

  // FIXME(chiyang): Compromise decimal with double
  if (TypeKind::DECIMALNEWID == typeKind) {
    for (auto idx = 0; idx < this->getNumOfRows(); idx++) {
      bool isNull;
      std::string tmp = this->read(idx, &isNull);
      if (!isNull) {
        tmp = std::to_string(std::stod(tmp));
      }
      ss << tmp << " ";
    }
    return ss.str();
  }

  for (auto idx = 0; idx < this->getNumOfRows(); idx++) {
    bool isNull;
    std::string tmp = this->read(idx, &isNull);
    ss << tmp << " ";
  }
  return ss.str();
}

void Vector::resize(size_t plainSize, const SelectList *sel,
                    const bool *__restrict__ nulls1,
                    const bool *__restrict__ nulls2,
                    const bool *__restrict__ nulls3) {
  assert(this->getOwnData());
  if (sel) this->setSelected(sel, true);

  if (nulls1 || nulls2 || nulls3) {
    this->setHasNull(true);
    this->nulls.reset(plainSize, sel, nulls1, nulls2, nulls3);
  } else {
    this->setHasNull(false);
  }

  resizeInternal(plainSize);
}

void Vector::resize(size_t plainSize, const SelectList *sel, bool null) {
  assert(this->getOwnData());
  if (sel) this->setSelected(sel, true);

  if (null) {
    this->setHasNull(true);
    this->nulls.reset(plainSize, sel, null);
  } else {
    this->setHasNull(false);
  }

  resizeInternal(plainSize);
}

}  // namespace dbcommon
