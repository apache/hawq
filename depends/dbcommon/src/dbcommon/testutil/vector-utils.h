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

#ifndef DBCOMMON_SRC_DBCOMMON_TESTUTIL_VECTOR_UTILS_H_
#define DBCOMMON_SRC_DBCOMMON_TESTUTIL_VECTOR_UTILS_H_

#include <algorithm>
#include <cassert>
#include <memory>
#include <set>
#include <string>
#include <utility>
#include <vector>

#include "dbcommon/common/vector.h"
#include "dbcommon/common/vector/decimal-vector.h"
#include "dbcommon/common/vector/list-vector.h"
#include "dbcommon/common/vector/struct-vector.h"
#include "dbcommon/type/type-modifier.h"
#include "dbcommon/type/type-util.h"
#include "dbcommon/utils/string-util.h"

namespace dbcommon {

class VectorUtility {
 public:
  VectorUtility() {}

  explicit VectorUtility(TypeKind typekind, int64_t typemod = -1)
      : typekind_(typekind) {
    typemod_ =
        (typemod == -1 && std::set<TypeKind>{CHARID, VARCHARID}.count(typekind_)
             ? TypeModifierUtil::getTypeModifierFromMaxLength(23)
             : typemod);
  }

  ~VectorUtility() {}

 public:
  template <class T>
  static std::unique_ptr<Vector> generateVector(
      TypeKind type, const std::vector<T> &vals,
      const std::vector<bool> *nullsPtr) {
    std::unique_ptr<Vector> vec = Vector::BuildVector(type, true);
    bool hasNull = (nullsPtr != nullptr);
    if (hasNull) {
      const std::vector<bool> nulls = *nullsPtr;
      assert(vals.size() == nulls.size());

      for (size_t s = 0; s < vals.size(); s++) {
        if (type == DECIMALNEWID) {
          std::stringstream ss;
          ss << vals[s];
          vec->append(nulls[s] ? "" : ss.str(), nulls[s]);
        } else {
          Datum d = CreateDatum<T>(vals[s]);
          vec->append(d, nulls[s]);
        }
      }
    } else {
      for (size_t s = 0; s < vals.size(); s++) {
        if (type == DECIMALNEWID) {
          std::stringstream ss;
          ss << vals[s];
          vec->append(ss.str(), false);
        } else {
          Datum d = CreateDatum<T>(vals[s]);
          vec->append(d, false);
        }
      }
    }
    vec->setHasNull(hasNull);
    return std::move(vec);
  }

  TypeKind getTypeKind() { return typekind_; }

  static std::unique_ptr<Vector> generateTimestampVector(
      TypeKind type, const std::vector<std::string> &valStrs,
      std::vector<Timestamp> *vals, const std::vector<bool> *nullsPtr) {
    std::unique_ptr<Vector> vec = Vector::BuildVector(type, true);
    bool hasNull = (nullsPtr != nullptr);
    if (hasNull) {
      const std::vector<bool> nulls = *nullsPtr;
      assert(vals->size() == nulls.size());

      for (size_t s = 0; s < vals->size(); s++) {
        Datum d = CreateDatum(valStrs[s].c_str(), &(*vals)[s], TIMESTAMPID);
        vec->append(d, nulls[s]);
      }
    } else {
      for (size_t s = 0; s < vals->size(); s++) {
        Datum d = CreateDatum(valStrs[s].c_str(), &(*vals)[s], TIMESTAMPID);
        vec->append(d, false);
      }
    }
    vec->setHasNull(hasNull);
    return std::move(vec);
  }

  template <class T>
  static std::unique_ptr<Vector> generateListVector(
      TypeKind type, TypeKind childType, const std::vector<T> &vals,
      const std::vector<uint64_t> &offsets, const std::vector<bool> *nullsPtr,
      const std::vector<bool> *valueNullsPtr) {
    std::unique_ptr<Vector> vec = Vector::BuildVector(type, false);
    ListVector *lvec = reinterpret_cast<ListVector *>(vec.get());

    std::unique_ptr<dbcommon::Vector> childVec =
        generateVector(childType, vals, valueNullsPtr);
    lvec->addChildVector(std::move(childVec));

    bool hasNull = (nullsPtr != nullptr);
    lvec->setHasNull(hasNull);
    if (hasNull) {
      const std::vector<bool> nulls = *nullsPtr;
      assert(offsets.size() == nulls.size() + 1);

      lvec->setNullBits(nullptr, 0);
      for (size_t i = 0; i < nulls.size(); i++) {
        lvec->appendNull(nulls[i]);
      }
    }

    lvec->setOffsets(offsets.data(), offsets.size());

    return std::move(vec);
  }

  template <class T>
  static std::unique_ptr<Vector> generateSelectVector(
      TypeKind type, const std::vector<T> &vect, const std::vector<bool> *nulls,
      SelectList *sel) {
    std::unique_ptr<Vector> result = generateVector(type, vect, nulls);

    if (sel) result->setSelected(sel, false);

    return std::move(result);
  }

  static std::unique_ptr<Vector> generateSelectTimestampVector(
      TypeKind type, const std::vector<std::string> &vectStr,
      std::vector<Timestamp> *vect, const std::vector<bool> *nulls,
      SelectList *sel) {
    std::unique_ptr<Vector> result =
        generateTimestampVector(type, vectStr, vect, nulls);

    if (sel) result->setSelected(sel, false);

    return std::move(result);
  }

  template <class T>
  static std::unique_ptr<Vector> generateSelectListVector(
      TypeKind type, TypeKind childType, const std::vector<T> &vect,
      const std::vector<uint64_t> &offsets, const std::vector<bool> *nulls,
      const std::vector<bool> *valueNulls, SelectList *sel) {
    std::unique_ptr<Vector> result =
        generateListVector(type, childType, vect, offsets, nulls, valueNulls);

    if (sel) result->setSelected(sel, false);

    return std::move(result);
  }

  static std::unique_ptr<Vector> generateSelectStructVector(
      std::vector<std::unique_ptr<Vector>> &vecs,  // NOLINT
      const std::vector<bool> *nulls, SelectList *sel, TypeKind typekind) {
    std::unique_ptr<dbcommon::Vector> result(new dbcommon::StructVector(false));
    result->setTypeKind(typekind);
    for (int i = 0; i < vecs.size(); i++) {
      result->addChildVector(std::move(vecs[i]));
    }
    if (nulls == nullptr) {
      result->setHasNull(false);
    } else {
      result->setHasNull(true);
      result->getNullBuffer()->resize(0);
      for (int i = 0; i < nulls->size(); i++) result->appendNull(((*nulls)[i]));
    }
    if (sel) result->setSelected(sel, false);
    return std::move(result);
  }

  // Generate a dbcommon::Vector for unittest
  static Vector::uptr generateVectorRandom(TypeKind typeKind, int numOfElement,
                                           bool hasNull, bool hasSel,
                                           bool ownData) {
    Vector::uptr vec = Vector::BuildVector(
        typeKind, true,
        (std::set<TypeKind>{CHARID, VARCHARID}.count(typeKind)
             ? TypeModifierUtil::getTypeModifierFromMaxLength(5)
             : -1));

    SelectList sel;

    for (auto i = 0; i < numOfElement; i++) {
      bool null = hasNull && (std::rand() % 2 == 0);
      vec->append(convertIntToString(typeKind, std::rand(), null), null);
      if (hasSel && (std::rand() % 2 == 0)) sel.push_back(i);
    }

    vec->setHasNull(hasNull);

    if (hasSel) {
      if (sel.size() < numOfElement) {
        int numOfRemainedElement = numOfElement - sel.size();
        for (auto i = 0; i < numOfRemainedElement; i++) {
          bool null = hasNull && (std::rand() % 2 == 0);
          vec->append(convertIntToString(typeKind, std::rand(), null), null);
          sel.push_back(i + numOfElement);
        }
      }
      vec->setSelected(&sel, false);
    }

    if (!ownData) {
      // XXX(chiyang): this memory leak doesn't matter in unittest
      Vector *tmp = vec.release();
      vec = Vector::BuildVector(typeKind, false, tmp->getTypeModifier());
      vec->setHasNull(tmp->hasNullValue());
      if (vec->hasNullValue()) {
        vec->getNullBuffer()->setBools(
            reinterpret_cast<const char *>(tmp->getNullBuffer()->data()),
            tmp->getNullBuffer()->size());
      }
      if (tmp->getLengths()) {
        vec->setLengths(tmp->getLengths(), tmp->getNumOfRowsPlain());
      }
      if (tmp->getValue()) {
        vec->setValue(tmp->getValueBuffer()->data(),
                      tmp->getValueBuffer()->size());
      }
      if (std::set<TypeKind>{CHARID, VARCHARID, STRINGID, BINARYID, DECIMALID}
              .count(typeKind)) {
        vec->computeValPtrs();
      } else if (TIMESTAMPID == typeKind) {
        vec->setNanoseconds(tmp->getNanosecondsBuffer()->data(),
                            tmp->getNanosecondsBuffer()->size());
      } else if (INTERVALID == typeKind) {
        vec->setDayValue(tmp->getDayBuffer()->data(),
                         tmp->getDayBuffer()->size());
        vec->setMonthValue(tmp->getMonthBuffer()->data(),
                           tmp->getMonthBuffer()->size());
      } else if (DECIMALNEWID == typeKind) {
        vec->setAuxiliaryValue(tmp->getAuxiliaryValueBuffer()->data(),
                               tmp->getAuxiliaryValueBuffer()->size());
        vec->setScaleValue(tmp->getScaleValueBuffer()->data(),
                           tmp->getScaleValueBuffer()->size());
      }
    }
    return std::move(vec);
  }

  static std::string convertIntToString(TypeKind type, int input,
                                        bool isNull = false) {
    if (isNull) {
      return "";
    }
    switch (type) {
      case TypeKind::TINYINTID:
        return (std::to_string(input & 127));
      case TypeKind::SMALLINTID:
        return (std::to_string(input & 32767));
      case TypeKind::TIMESTAMPID:
        return ("2018-01-" + std::to_string((input + 4) / 5 + 10) +
                " 15:16:01.123");
      case TypeKind::DATEID:
        return ("2018-01-" + std::to_string((input + 4) / 5 + 10));
      case TypeKind::TIMEID:
        return ("00:00:00." + std::to_string(input & 32767));
      case TypeKind::BOOLEANID:
        return (input % 2 == 0 ? "t" : "f");
      case TypeKind::BINARYID:
        return StringUtil::toOct(reinterpret_cast<char *>(&input),
                                 sizeof(input));
      case TypeKind::INTERVALID:
        return ("3:10:" + std::to_string(input & 32767));
      default:
        return (std::to_string(input));
    }
  }

  // Generate Vector from std::string representation.
  //
  // (space character) as DELIMITER
  // ("NULL") as NULL value
  // @return Vector without SelectList
  Vector::uptr generateVector(const std::string &vecStr, char delimiter = ' ') {
    Vector::uptr vec = Vector::BuildVector(typekind_, true, typemod_);
    bool hasnull = false;
    auto fields = StringUtil::split(vecStr, delimiter);
    int idx = 0;
    for (auto i = 0; i < fields.size(); i++) {
      std::string field = fields[i];
      if (typekind_ == TypeKind::TIMESTAMPID && field != "NULL") {
        i++;
        field = field + " " + fields[i];
      }
      if (typekind_ == TypeKind::CHARID && field != "NULL") {
        field = newBlankPaddedChar(field.data(), field.length(),
                                   TypeModifierUtil::getMaxLen(typemod_));
      }
      hasnull |= (field == "NULL");
      vec->append(field == "NULL" ? "" : field, field == "NULL");
      idx++;
    }
    vec->setHasNull(hasnull);
    assert(idx == vec->getNumOfRows());
    return std::move(vec);
  }

 private:
  TypeKind typekind_ = INVALIDTYPEID;
  int64_t typemod_ = -1;
};

}  // namespace dbcommon

#endif  // DBCOMMON_SRC_DBCOMMON_TESTUTIL_VECTOR_UTILS_H_
