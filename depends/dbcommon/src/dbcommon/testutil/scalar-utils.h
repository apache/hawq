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

#ifndef DBCOMMON_SRC_DBCOMMON_TESTUTIL_SCALAR_UTILS_H_
#define DBCOMMON_SRC_DBCOMMON_TESTUTIL_SCALAR_UTILS_H_

#include <set>
#include <string>
#include <utility>

#include "dbcommon/common/vector.h"

namespace dbcommon {

class ScalarUtility {
 public:
  explicit ScalarUtility(TypeKind typekind, int64_t typemod = -1)
      : typekind_(typekind) {
    typemod_ =
        (typemod == -1 && std::set<TypeKind>{CHARID, VARCHARID}.count(typekind_)
             ? TypeModifierUtil::getTypeModifierFromMaxLength(23)
             : typemod);
    vec_ = Vector::BuildVector(typekind_, true, typemod_);
  }

  ScalarUtility(ScalarUtility &&x)
      : typekind_(x.typekind_), typemod_(x.typemod_), vec_(std::move(x.vec_)) {}

  ~ScalarUtility() {}

  Scalar generateScalar(const std::string &scalarStr) {
    Scalar ret;
    vec_->append(scalarStr == "NULL" ? "" : scalarStr, scalarStr == "NULL");
    vec_->readPlainScalar(vec_->getNumOfRowsPlain() - 1, &ret);
    return ret;
  }

 private:
  TypeKind typekind_ = INVALIDTYPEID;
  int64_t typemod_ = -1;
  Vector::uptr vec_;
};

}  // namespace dbcommon

#endif  // DBCOMMON_SRC_DBCOMMON_TESTUTIL_SCALAR_UTILS_H_
