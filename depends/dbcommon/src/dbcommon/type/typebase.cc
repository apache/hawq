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

#include <utility>

#include "dbcommon/type/typebase.h"

#include "dbcommon/common/vector.h"
#include "dbcommon/common/vector/struct-vector.h"

namespace dbcommon {

std::unique_ptr<Vector> TypeBase::getVector(bool ownData) {
  return Vector::BuildVector(typeKind, true);
}

bool StructType::isFixedSizeType() const {
  LOG_ERROR(ERRCODE_FEATURE_NOT_SUPPORTED, "struct type not supported yet");
}

uint64_t StructType::getTypeWidth() const {
  LOG_ERROR(ERRCODE_FEATURE_NOT_SUPPORTED, "struct type not supported yet");
}

Datum StructType::getDatum(const char* str) const {
  LOG_ERROR(ERRCODE_FEATURE_NOT_SUPPORTED, "struct type not supported yet");
}

std::string StructType::DatumToString(const Datum& d) const {
  LOG_ERROR(ERRCODE_FEATURE_NOT_SUPPORTED, "struct type not supported yet");
}

std::string StructType::DatumToBinary(const Datum& d) const {
  LOG_ERROR(ERRCODE_FEATURE_NOT_SUPPORTED, "struct type not supported yet");
}

int StructType::compare(const Datum& a, const Datum& b) const {
  LOG_ERROR(ERRCODE_FEATURE_NOT_SUPPORTED, "struct type not supported yet");
}

int StructType::compare(const char* str1, uint64_t len1, const char* str2,
                        uint64_t len2) const {
  LOG_ERROR(ERRCODE_FEATURE_NOT_SUPPORTED, "struct type not supported yet");
}

bool MapType::isFixedSizeType() const {
  LOG_ERROR(ERRCODE_FEATURE_NOT_SUPPORTED, "map type not supported yet");
}

uint64_t MapType::getTypeWidth() const {
  LOG_ERROR(ERRCODE_FEATURE_NOT_SUPPORTED, "map type not supported yet");
}

Datum MapType::getDatum(const char* str) const {
  LOG_ERROR(ERRCODE_FEATURE_NOT_SUPPORTED, "map type not supported yet");
}

std::string MapType::DatumToString(const Datum& d) const {
  LOG_ERROR(ERRCODE_FEATURE_NOT_SUPPORTED, "map type not supported yet");
}

std::string MapType::DatumToBinary(const Datum& d) const {
  LOG_ERROR(ERRCODE_FEATURE_NOT_SUPPORTED, "map type not supported yet");
}

int MapType::compare(const Datum& a, const Datum& b) const {
  LOG_ERROR(ERRCODE_FEATURE_NOT_SUPPORTED, "map type not supported yet");
}

int MapType::compare(const char* str1, uint64_t len1, const char* str2,
                     uint64_t len2) const {
  LOG_ERROR(ERRCODE_FEATURE_NOT_SUPPORTED, "map type not supported yet");
}

bool UnionType::isFixedSizeType() const {
  LOG_ERROR(ERRCODE_FEATURE_NOT_SUPPORTED, "union type not supported yet");
}

uint64_t UnionType::getTypeWidth() const {
  LOG_ERROR(ERRCODE_FEATURE_NOT_SUPPORTED, "union type not supported yet");
}

Datum UnionType::getDatum(const char* str) const {
  LOG_ERROR(ERRCODE_FEATURE_NOT_SUPPORTED, "union type not supported yet");
}

std::string UnionType::DatumToString(const Datum& d) const {
  LOG_ERROR(ERRCODE_FEATURE_NOT_SUPPORTED, "union type not supported yet");
}

std::string UnionType::DatumToBinary(const Datum& d) const {
  LOG_ERROR(ERRCODE_FEATURE_NOT_SUPPORTED, "union type not supported yet");
}

int UnionType::compare(const Datum& a, const Datum& b) const {
  LOG_ERROR(ERRCODE_FEATURE_NOT_SUPPORTED, "union type not supported yet");
}

int UnionType::compare(const char* str1, uint64_t len1, const char* str2,
                       uint64_t len2) const {
  LOG_ERROR(ERRCODE_FEATURE_NOT_SUPPORTED, "union type not supported yet");
}

bool AnyType::isFixedSizeType() const {
  LOG_ERROR(ERRCODE_FEATURE_NOT_SUPPORTED, "any type not supported yet");
}

uint64_t AnyType::getTypeWidth() const {
  LOG_ERROR(ERRCODE_FEATURE_NOT_SUPPORTED, "any type not supported yet");
}

Datum AnyType::getDatum(const char* str) const {
  LOG_ERROR(ERRCODE_FEATURE_NOT_SUPPORTED, "any type not supported yet");
}

std::string AnyType::DatumToString(const Datum& d) const {
  LOG_ERROR(ERRCODE_FEATURE_NOT_SUPPORTED, "any type not supported yet");
}

std::string AnyType::DatumToBinary(const Datum& d) const {
  LOG_ERROR(ERRCODE_FEATURE_NOT_SUPPORTED, "any type not supported yet");
}

int AnyType::compare(const Datum& a, const Datum& b) const {
  LOG_ERROR(ERRCODE_FEATURE_NOT_SUPPORTED, "any type not supported yet");
}

int AnyType::compare(const char* str1, uint64_t len1, const char* str2,
                     uint64_t len2) const {
  LOG_ERROR(ERRCODE_FEATURE_NOT_SUPPORTED, "any type not supported yet");
}

bool UnknownType::isFixedSizeType() const {
  LOG_ERROR(ERRCODE_INTERNAL_ERROR, "should not go here");
}

uint64_t UnknownType::getTypeWidth() const {
  LOG_ERROR(ERRCODE_INTERNAL_ERROR, "should not go here");
}

Datum UnknownType::getDatum(const char* str) const {
  LOG_ERROR(ERRCODE_INTERNAL_ERROR, "should not go here");
}

std::string UnknownType::DatumToString(const Datum& d) const {
  LOG_ERROR(ERRCODE_INTERNAL_ERROR, "should not go here");
}

std::string UnknownType::DatumToBinary(const Datum& d) const {
  LOG_ERROR(ERRCODE_INTERNAL_ERROR, "should not go here");
}

int UnknownType::compare(const Datum& a, const Datum& b) const {
  LOG_ERROR(ERRCODE_INTERNAL_ERROR, "should not go here");
}

int UnknownType::compare(const char* str1, uint64_t len1, const char* str2,
                         uint64_t len2) const {
  LOG_ERROR(ERRCODE_INTERNAL_ERROR, "should not go here");
}

}  // namespace dbcommon
