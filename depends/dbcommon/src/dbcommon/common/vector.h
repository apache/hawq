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

#ifndef DBCOMMON_SRC_DBCOMMON_COMMON_VECTOR_H_
#define DBCOMMON_SRC_DBCOMMON_COMMON_VECTOR_H_

#include <algorithm>
#include <cassert>
#include <memory>
#include <set>
#include <string>
#include <utility>
#include <vector>

#include "dbcommon/common/node-deserializer.h"
#include "dbcommon/common/node-serializer.h"
#include "dbcommon/hash/cdb-hash.h"
#include "dbcommon/hash/fast-hash.h"
#include "dbcommon/nodes/datum.h"
#include "dbcommon/nodes/scalar.h"
#include "dbcommon/nodes/select-list.h"
#include "dbcommon/type/bool.h"
#include "dbcommon/type/date.h"
#include "dbcommon/type/float.h"
#include "dbcommon/type/integer.h"
#include "dbcommon/type/interval.h"
#include "dbcommon/type/type-modifier.h"
#include "dbcommon/type/varlen.h"
#include "dbcommon/utils/bool-buffer.h"
#include "dbcommon/utils/byte-buffer.h"
#include "dbcommon/utils/macro.h"
#include "dbcommon/utils/mb/mb-converter.h"

namespace dbcommon {

typedef struct VectorStatistics {
  Datum minimum;
  Datum maximum;
  Datum sum;
  uint64_t valueCount;
  bool hasMinMaxStats;
} VectorStatistics;

class TupleBatch;
class GroupKeys;

// A vector typically represents a column.
// It can own the data, or only reference the external data.
//
// A vector can be a "plain vector" or a "selected vector". A "plain vector"
// is a vector without a "selected array".
// A "selected" vector is a vector with a "selected array".
// A "selected array" is used to accelerate
// selection/filter operations. For example, if there is a filter "x > 3"
// on column x, if there is only a small number of elements
// satisfying the condition, then we can use the original vector
// with a selected array which store the indexes of result elements,
// instead of generating a new result vector.
//
// Now we distinguish two operations: "materialize" and "plainize".
// "materialize" operation on a vector that does not own the data will make a
// copy of the data, and it also make a "selected vector" as a "plain vector".
//
// "plainizing" a vector only convert a "selected vector" to a "plain vector".
// So "plainizing" a plain vector will do nothing.
// but "materializing" the vector will make it "materialized"
//
class Vector : public Object {
 public:
  typedef std::unique_ptr<Vector> uptr;

  explicit Vector(bool ownData) : nulls(ownData), values(ownData) {
    if (ownData) nulls.reserve(DEFAULT_NUMBER_TUPLES_PER_BATCH);
  }

  virtual ~Vector() {}

  virtual void convert(const dbcommon::SupportedEncodingSet inputEncoding,
                       const dbcommon::SupportedEncodingSet outputEncoding) {}

  // Is the vector owning the data or just reference the real data?
  // @return A bool value indicating whether the vector owns the data
  // or not
  bool getOwnData() const { return values.getOwnData(); }

  // Get the element type of this vector
  // @return The element type
  TypeKind getTypeKind() const { return typeKind; }

  // Set the element type of this vector
  // @param typeKind The type for the vector
  // @return void
  void setTypeKind(TypeKind typeKind) { this->typeKind = typeKind; }

  bool hasNullValue() const { return hasNulls; }
  void setHasNull(bool hasNull) { this->hasNulls = hasNull; }

  virtual bool checkZeroValue() const { return false; }

  virtual bool checkLessZero() const { return false; }

  virtual bool checkLogarithmAgrumentError() const { return false; }

  virtual bool checkExponentialArgumentError() const { return false; }

  // Is the element at "index" a null?
  // @param index The position of the element
  // @return A bool value indicating whether the element is null?
  bool isNull(uint64_t index) const {
    return isNullPlain(getPlainIndex(index));
  }

  // Is the element at the given plain "index" a null?
  // @param index The plain index
  // @return A bool value indicating whether the element is null?
  bool isNullPlain(uint64_t index) const {
    return hasNulls ? nulls.get(index) : false;
  }

  void setNull(uint64_t index) { nulls.set(getPlainIndex(index), true); }
  void setNullPlain(uint64_t index) { nulls.set(index, true); }

  // Append a null value to the "nulls" array
  // @param null The input value
  // @return void
  inline void appendNull(bool null) {
    nulls.append(null);
    if (selectListOwner) {
      selectListOwner->push_back(this->getNumOfRowsPlain() - 1);
    }
  }

  // Get the size (in BYTEs) of bitmap array for nulls.
  // A null value occupies only one bit.
  // For example, 9 values use 2 bytes
  // @return The size of the bitmap array for nulls.
  uint64_t getNullBitMapNumBytesPlain() const {
    return nulls.bitDataSizeInBytes();
  }

  // Get the number of elements in the null bitmap
  // For example, having 9 values will return 9
  // @return The number of elements in the null bitmap
  uint64_t getNullsSize() const { return nulls.size(); }

  const bool *getNulls() const { return hasNulls ? nulls.data() : nullptr; }

  // Get the bitmap array for nulls. A null value occupies only one bit.
  // @return The bitmap array for nulls.
  const char *getNullBits() const { return nulls.getBitData(); }

  // Set the null bitmap
  // @param data The bitmap in byte array
  // @param size The number of bool values in the array.
  // @return void
  void setNullBits(const char *nulls, uint64_t size) {
    assert(!getOwnData());
    this->nulls.setBitData(nulls, size);
  }

  void setNulls(const bool *nulls, uint64_t size) {
    this->nulls.setBools(reinterpret_cast<const char *>(nulls),
                         size * sizeof(bool));
  }

  void setNotNulls(const char *notNulls, uint64_t size) {
    this->nulls.setReserseBools(notNulls, size);
  }

  // Get the number of rows/elements in the vector
  // @return The number of rows/elements in the vector
  virtual uint64_t getNumOfRows() const {
    return selected ? selected->size() : getNumOfRowsPlain();
  }

  // Get the number of rows/elements in the vector
  // For a plain vector, return the number of rows.
  // For a selected vector, does not consider the select vector
  // @return The number of rows/elements in the vector
  virtual uint64_t getNumOfRowsPlain() const = 0;

  // Returns whether the Vector is empty (i.e. whether its size is 0).
  bool empty() const { return getNumOfRows() == 0; }

  // Whether all elements are selected.
  // @return A bool value indicating whether all elements are selected
  bool allSelected() const {
    return !selected || selected->size() == getNumOfRowsPlain();
  }

  // Get the select list
  // @return The selected list
  SelectList *getSelected() { return selected; }
  const SelectList *getSelected() const { return selected; }

  // Set the selected array. If it is from belonging tupleBatch,
  // this vector only keeps a reference to the selected array.
  // If it is not from belonging tupleBatch, this vector will
  // swap the content of a local select array with the input select array.
  // @param selected The selected array
  // @param fromBatch Indicating whether it is from beloning tupleBatch
  // @return void
  void setSelected(const SelectList *selected, bool fromBatch);

  // Get the value array. Please note that, if the vector
  // is a "selected vector", then the values might contain
  // some unnecessary data.
  // @return The value array
  const char *getValue() const { return values.data(); }

  // Get the nanosecond value array. Only used in TimestampVector
  // @return The nanosecode value array
  virtual const char *getNanoseconds() const { return nullptr; }

  // Get the auxiliary value array. Only used in DecimalVector
  // @return The auxiliary value array
  virtual const char *getAuxiliaryValue() const { return nullptr; }

  // Get the scale value array. Only used in DecimalVector
  // @return The scale value array
  virtual const char *getScaleValue() const { return nullptr; }

  // Get the day value array. Only used in IntervalVector
  // @return The day value array
  virtual const char *getDayValue() const { return nullptr; }

  // Get the month value array. Only used in IntervalVector
  // @return The month value array
  virtual const char *getMonthValue() const { return nullptr; }

  // Set the value array. This vector does not own the data.
  // @param value The value array
  // @param size The number of bytes in the array.
  // @return void
  virtual void setValue(const char *value, uint64_t size) = 0;

  // Set the nanosecond value array. This vector does not own the data.
  // Only used in TimestampVector
  // @param value The value array
  // @param size The number of bytes in the array.
  // @return void
  virtual void setNanoseconds(const char *value, uint64_t size) {}

  // Set the auxiliary value array. This vector does not own the data.
  // Only used in DecimalVector
  // @param value The auxiliary value array
  // @param size The number of bytes in the array.
  // @return void
  virtual void setAuxiliaryValue(const char *value, uint64_t size) {}
  // Set the scale value array. This vector does not own the data.
  // Only used in DecimalVector
  // @param value The scale value array
  // @param size The number of bytes in the array.
  // @return void
  virtual void setScaleValue(const char *value, uint64_t size) {}
  // Set the day value array. This vector does not own the data.
  // Only used in IntervalVector
  // @param value The day value array
  // @param size The number of bytes in the array.
  // @return void
  virtual void setDayValue(const char *value, uint64_t size) {}
  // Set the month value array. This vector does not own the data.
  // Only used in IntervalVector
  // @param value The day value array
  // @param size The number of bytes in the array.
  // @return void
  virtual void setMonthValue(const char *value, uint64_t size) {}

  // Get the offset array, Please note that, if the vector
  // is a "selected vector", then the data might contain
  // some unnecessary data.
  // @return the length array
  virtual const uint64_t *getLengths() const { return nullptr; }

  // Set the length array.
  // @param lengthsPtr The length array
  // @param size The number of elements (uint64_t) in the array.
  // @return void
  virtual void setLengths(const uint64_t *lengthsPtr, uint64_t size) {}

  // Get the offset array
  // @return The the offset array.
  virtual const char **getValPtrs() const { return nullptr; }

  // Set the value pointer array
  // @param valPtrs The value pointer array
  // @param size The number of elements in the value pointer array
  // @return void
  virtual void setValPtrs(const char **valPtrs, uint64_t size) {}

  // Compute value pointers from length array and value array
  // If valPtrs does not own the data, it will be changed to own the data
  // @return void
  virtual void computeValPtrs() {}

  // Read a value at index of row vector and return as binary.
  // @param index The row vector index at which position the value will be read.
  // @param len The output variable which will be set to the returned number of
  // bytes.
  // @param isNull The output variable
  //               which will be set to true if the value is null.
  // @return Return a constant buffer pointer to the data.
  virtual const char *read(uint64_t index, uint64_t *len, bool *null) const = 0;

  // Read a value at index of row vector and convert to sting
  // @param index The row vector index at which position the value will be read.
  // @param isNull The output variable
  //               which will be set to true if the value is null.
  // @return Return the string representation of data at index.
  std::string read(uint64_t index, bool *null) const {
    return readPlain(getPlainIndex(index), null);
  }

  // Read a value at plain index of row vector and convert to sting
  virtual std::string readPlain(uint64_t index, bool *null) const = 0;

  // Print debug string
  std::string toString() const;

  // Read a value at plain index of row vector and convert to dbcommon::Scalar
  // @param[in] index
  // @param[out] scalar
  virtual void readPlainScalar(uint64_t index, Scalar *scalar) const = 0;

  // Append a value to the vector end.
  // @param v The string representation of the value.
  // @param v If appended value is null.
  // @return void
  virtual void append(const std::string &strValue, bool null) = 0;

  // Append a value in binary format to the vector end.
  // @param v The value to append.
  // @param valueLen The size of the value
  // @param isNull Indicate whether it is a null.
  // @return void
  virtual void append(const char *v, uint64_t valueLen, bool null) = 0;

  // Append a datum to vector end.
  // @param datum The input datum
  // @param null The bool value indicating whether it is null.
  // @return void
  virtual void append(const Datum &datum, bool null) = 0;
  virtual void append(const Datum &valDatum, const Datum &nanoDatum,
                      bool null) {}

  // Append a Scalar to Vector.
  virtual void append(const Scalar *scalar) = 0;

  virtual void append(Vector *src, uint64_t index) = 0;

  // Append the input vector
  // All child class should call Vector::append too.
  // @return void
  virtual void append(Vector *vect) = 0;

  // Append the subset of vector according to selected
  // @param vector The source vector
  // @param selected The list of rows selected to be appended.
  // @return void
  virtual void append(Vector *vect, SelectList *sel) {
    assert(vect != nullptr);
    assert(selected == nullptr);

    int oldSize = this->getNumOfRowsPlain();

    if (vect->hasNullValue()) {
      if (!hasNullValue()) nulls.setReserseBools(false, oldSize);
      setHasNull(true);

      const dbcommon::BoolBuffer *inputNulls = vect->getNullBuffer();
      if (!sel || sel->size() == vect->getNumOfRowsPlain()) {
        nulls.append(inputNulls->data(), inputNulls->size());
      } else {
        for (auto i : *sel) {
          nulls.append(inputNulls->get(i));
        }
      }

    } else if (hasNullValue()) {
      uint64_t newSize = sel ? sel->size() : vect->getNumOfRowsPlain();
      for (uint64_t i = 0; i < newSize; i++) {
        nulls.append(false);
      }
    }

    if (selected && selectListOwner) {
      assert(belongingTupleBatch == nullptr);
      int numAppend = sel ? sel->size() : vect->getNumOfRowsPlain();
      for (int i = 0; i < numAppend; i++) {
        this->selected->push_back(oldSize++);
      }
    }
  }

  // Resize the Vector for subsequent operation that requires sufficient
  // allocated buffer space and fill the NullVector according to the input
  // NullVector.
  void resize(size_t plainSize, const SelectList *sel,
              const bool *__restrict__ nulls1,
              const bool *__restrict__ nulls2 = nullptr,
              const bool *__restrict__ nulls3 = nullptr);

  // Resize the Vector for subsequent operation that requires sufficient
  // allocated buffer space and fill the NullVector according to the input
  // NullValue.
  void resize(size_t plainSize, const SelectList *sel, bool null);

  // Reset the vector to an empty Vector.
  // All child class should call Vector::reset too.
  // @param resetBelongingTupleBatch Bool value indicating whether
  //        resetting belonging tuple batch
  // @return void
  virtual void reset(bool resetBelongingTupleBatch = false) {
    if (hasNulls) nulls.resize(0);

    selected = nullptr;
    selectListOwner.reset();

    if (resetBelongingTupleBatch) this->belongingTupleBatch = nullptr;
  }

  void clear() override { this->reset(true); }

  // Materialize the vector. Materialization means if the vector
  // does not own data, it will make a copy of the data. And
  // if the vector is a "selected vector", it will make it a "plain vector".
  // @return void
  virtual void materialize() = 0;

  // Clone a vector, the result vector is a plain vector regardless
  // of whether the input vector is a plain vector, or selected vector
  // @return The vector cloned
  std::unique_ptr<Vector> clone() const { return cloneSelected(selected); }

  // Clone a plain vector according to the input select list(plain index).
  // The result vector is a plain vector
  // @param sel The input select list, plain index
  // @return The vector cloned
  virtual std::unique_ptr<Vector> cloneSelected(
      const SelectList *sel) const = 0;

  // Clone a plain vector according to the input select list in some range
  // The result vector is a plain vector
  // @param sel The input select list, plain index
  // @param start The start input plain index
  // @param end The end input plain index (not included)
  // @return The vector cloned
  virtual std::unique_ptr<Vector> extract(const SelectList *sel, uint64_t start,
                                          uint64_t end) const = 0;

  // Replicate the specified element at given index for given times
  // @param index The element index
  // @param replicateNum The number of replicates
  // @return The replicated vector
  virtual std::unique_ptr<Vector> replicateItemToVec(
      uint64_t index, int replicateNum) const = 0;

  // Materialize several selected vectors. Dedicated for CASE expression.
  // @param[in] data Array of Vector*, SelectList* or Scalar*
  // @param[in] sels Array of SelectList*, nullptr indicate no match
  virtual void merge(size_t plainSize, const std::vector<Datum> &data,
                     const std::vector<SelectList *> &sels) {
    LOG_ERROR(ERRCODE_FEATURE_NOT_SUPPORTED, "not implemented");
  }

  // Calculate hash value for each item
  // @return The vector with the hash values
  virtual void hash(std::vector<uint64_t> &retval) const = 0;  // NOLINT

  // Calculate hash value for each item (used for shuffle)
  // @return The vector with the hash values
  virtual void cdbHash(std::vector<uint64_t> &retval) const = 0;  // NOLINT

  // Get the memory size in Bytes for this vector
  // It only includes the number of bytes for data, offsets and bitmap.
  // Note that this only works for "plain vector"
  // @return The data size
  virtual uint64_t getMemorySizePlain() const {
    return getSerializeSizeInternal();
  }

  // Get the containing tuple batch
  // @return The containing tuple batch
  virtual TupleBatch *getParentTupleBatch() const {
    return this->belongingTupleBatch;
  }

  // Set the containing tuple batch for this vector
  // @param parent The parent tuple batch
  // @return void
  virtual void setParentTupleBatch(TupleBatch *parent) {
    this->belongingTupleBatch = parent;
  }

  // Get the plain index of an element.
  // For example, a tuple batch has 10 tuples in its data array,
  // and it has a select list of 3 tuples in position 0, 3, 5 respectively.
  // So it means it has only 3 tuples.
  // So if we want to get the real index of tuple 1,
  // this function should return 3.
  // @param The input index
  // @return The real index
  uint64_t getPlainIndex(uint64_t index) const {
    assert(index >= 0 && index < getNumOfRows() && "invalid input");
    return selected ? (*selected)[index] : index;
  }

  // Get the null buffer
  // @return The null buffer. If this vector is not nullable, return nullptr
  const dbcommon::BoolBuffer *getNullBuffer() const {
    assert(hasNulls);
    return &nulls;
  }

  // Get the null buffer
  // @return The null buffer. If this vector is not nullable, return nullptr
  dbcommon::BoolBuffer *getNullBuffer() {
    assert(hasNulls);
    return &nulls;
  }

  // Get the value buffer of this vector
  // @return The value buffer
  virtual dbcommon::ByteBuffer *getValueBuffer() = 0;

  // Get the nanosecond value buffer. Only used in TimestampVector
  // @return The nanosecond value buffer
  virtual dbcommon::ByteBuffer *getNanosecondsBuffer() { return nullptr; }

  // Get the auxiliary value buffer. Only used in DecimalVector
  // to store the high bits of Int128 data
  // @return The auxiliary value buffer
  virtual dbcommon::ByteBuffer *getAuxiliaryValueBuffer() { return nullptr; }

  // Get the scale value buffer. Only used in DecimalVector
  // @return The scale value buffer
  virtual dbcommon::ByteBuffer *getScaleValueBuffer() { return nullptr; }

  // Get the day value buffer. Only used in IntervalVector
  // @return The day value buffer
  virtual dbcommon::ByteBuffer *getDayBuffer() { return nullptr; }

  // Get the month value buffer. Only used in IntervalVector
  // @return The month value buffer
  virtual dbcommon::ByteBuffer *getMonthBuffer() { return nullptr; }

  // Return true if the vector is valid, used only for validity check
  // to find potential bugs
  // @return True if the vector is valid, False otherwise
  virtual bool isValid() const;

  // Build a vector in given type
  // @param type The vector element type
  // @param ownData The bool value indicating whether the vector owns data
  // @return The vector built
  static std::unique_ptr<Vector> BuildVector(TypeKind type, bool ownData,
                                             int64_t typeModifier = -1);

  // Check whether the elements at the specified index are equal
  // @param v1 The first input vector
  // @param index1 The first index
  // @param v2 The second input vector
  // @param index2 The second index
  // @return True if the two elements are equal, False if not
  static bool equal(const Vector &v1, uint64_t index1, const Vector &v2,
                    uint64_t index2);

  // Check whether the two vectors are equal
  // @param v1 The first input vector
  // @param v2 The second input vector
  // @return True if the two vectors are equal, False if not
  static bool equal(const Vector &v1, const Vector &v2);

  // Swap the content of the two vectors
  // @param The input vector to swap with this vector
  // @return void
  virtual void swap(Vector &vect) {
    this->nulls.swap(vect.nulls);
    std::swap(this->selectListOwner, vect.selectListOwner);
    std::swap(this->selected, vect.selected);
    std::swap(this->belongingTupleBatch, vect.belongingTupleBatch);
    std::swap(this->typeKind, vect.typeKind);
  }

  // Check for group by, it will change the content of dataPtrSrc.
  // dataPtrSrc will be set to point to next column if matched, otherwise
  // nullptr.
  // xxx Need to be familiar with the implementation of groupKeys in executor
  virtual void checkGroupByKeys(
      std::vector<const void *> &dataPtrSrc) const {  // NOLINT
    LOG_ERROR(ERRCODE_FEATURE_NOT_SUPPORTED, "not implemented");
  }

  // This function is for comparing the data at specific position.
  // @param dataPtr points to the data with the same data type of the vector
  // @return The length that dataPtr have moved forward if matched, otherwise 0
  // the data that dataPtr points to is organized as
  // Fixed length column:
  // |isNull(with padding to the size of the type) |data
  // Variable length column:
  // |[length(8 byte)] |isNull(1 byte without padding)|data
  virtual uint64_t checkGroupByKey(uint64_t index, const char *dataPtr) const {
    LOG_ERROR(ERRCODE_FEATURE_NOT_SUPPORTED, "not implemented");
  }

  virtual void addChildVector(std::unique_ptr<Vector> vec) {
    if (childs.size() > 0) {
      assert(vec->getNumOfRows() == childs[0]->getNumOfRows());
    }
    childs.push_back(std::move(vec));
  }

  virtual Vector *getChildVector(int index) { return childs[index].get(); }

  virtual uint64_t getChildSize() const { return childs.size(); }

  uint64_t getSerializeSize() { return getSerializeSizeInternal(); }

  void serialize(NodeSerializer *serializer) { serializeInternal(serializer); }

  static std::unique_ptr<Vector> deserialize(NodeDeserializer *deserializer) {
    TypeKind type = deserializer->read<TypeKind>();
    uint64_t typeModifier = deserializer->read<uint64_t>();
    std::unique_ptr<Vector> vect =
        Vector::BuildVector(type, false, typeModifier);
    vect->deserializeInternal(deserializer);
    return std::move(vect);
  }

  // Get estimated size of serialized bytes.
  virtual uint64_t getSerializeSizeInternal() const {
    uint64_t sSize = sizeof(typeKind) + sizeof(typeModifier) +
                     sizeof(uint64_t) + sizeof(hasNulls);
    if (hasNulls) {
      sSize += sizeof(uint64_t) + getNullBitMapNumBytesPlain();
    }
    return sSize;
  }

  // Serialize this vector
  // @param serializer The serializer
  // @return void
  virtual void serializeInternal(NodeSerializer *serializer) {
    // write column type
    serializer->write<TypeKind>(getTypeKind());
    serializer->write<uint64_t>(typeModifier);
    assert(typeKind != CHARID || typeModifier != -1);

    // write the number of rows
    serializer->write<uint64_t>(getNumOfRows());

    // write the bitmap
    serializer->write<bool>(hasNulls);
    if (hasNulls) {
      if (allSelected()) {
        uint64_t bsz = nulls.bitDataSizeInBytes();
        serializer->write<uint64_t>(bsz);
        serializer->writeBytes(getNullBits(), bsz);
      } else {
        dbcommon::BoolBuffer buf(true);
        for (auto idx : *selected) buf.append(nulls.get(idx));
        uint64_t bsz = buf.bitDataSizeInBytes();
        serializer->write<uint64_t>(bsz);
        serializer->writeBytes(buf.getBitData(), bsz);
      }
    }
  }

  // Deserialize this vector
  // @param deserializer The deserializer
  // @return void
  virtual void deserializeInternal(NodeDeserializer *deserializer) {
    uint64_t nRows = deserializer->read<uint64_t>();

    this->hasNulls = deserializer->read<bool>();
    if (this->hasNulls) {
      uint64_t bsz = deserializer->read<uint64_t>();
      assert((nRows + 7) / 8 == bsz);
      if (bsz > 0) {
        const char *bstr = deserializer->readBytes(bsz);
        this->setNullBits(bstr, nRows);
      }
    }
  }

  void setVectorStatistics(const VectorStatistics &s) {
    hasStats = true;
    stats = s;
  }

  const VectorStatistics *getVectorStatistics() {
    if (hasStats)
      return &stats;
    else
      return nullptr;
  }

  int64_t getTypeModifier() { return typeModifier; }
  void setTypeModifier(int64_t typeModifier) {
    this->typeModifier = typeModifier;
  }

  virtual void setDirectEncoding(bool isDirectEncoding) {}

  virtual void trim() {}

  // Select the elements that "distinct from" the element stored in scalar
  // "Distinct from" operator differs from "Not equal" in cases when either
  // operand is NULL.
  // @param selected The SelectList to be returned
  // @param scalar The Scalar to be compared
  // @return void
  virtual void selectDistinct(SelectList *selected, Scalar *scalar) = 0;

  // Select the elements that "distinct from" the elements
  // at the same row in vect. "Distinct from" operator differs from "Not equal"
  // in cases when either operand is NULL.
  // @param selected The SelectList to be returned
  // @param vect The second Vector to be compared
  // @return void
  virtual void selectDistinct(SelectList *selected, Vector *vect) = 0;

 protected:
  // Resize the Vector for subsequent operation that requires sufficient
  // allocated buffer space.
  virtual void resizeInternal(size_t plainSize) = 0;

  TupleBatch *belongingTupleBatch = nullptr;

  SelectList *selected = nullptr;
  std::unique_ptr<SelectList> selectListOwner;

  int64_t typeModifier = -1;  // -1 for no type modifier

  // whether we only hava stripe statistics
  bool hasStats = false;
  VectorStatistics stats;

  // whether there are any null values
  bool hasNulls = true;
  dbcommon::BoolBuffer nulls;

  dbcommon::ByteBuffer values;
  TypeKind typeKind = INVALIDTYPEID;
  std::vector<std::unique_ptr<Vector>> childs;
};

}  // namespace dbcommon

#endif  // DBCOMMON_SRC_DBCOMMON_COMMON_VECTOR_H_
