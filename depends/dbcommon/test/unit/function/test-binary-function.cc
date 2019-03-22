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

#include "dbcommon/function/string-binary-function.h"

#include "dbcommon/testutil/function-utils.h"
#include "dbcommon/testutil/scalar-utils.h"
#include "dbcommon/testutil/vector-utils.h"

namespace dbcommon {

TEST(TestFunction, binary_octet_length) {
  FunctionUtility fu(FuncKind::BINARY_OCTET_LENGTH);
  VectorUtility vuBinary(TypeKind::BINARYID);
  VectorUtility vuInteger(TypeKind::INTID);

  auto strs = vuBinary.generateVector(
      "\\000\\001\\002 black 我是中国人 NULL NULL I am Chinese");
  auto lens = vuInteger.generateVector("3 5 15 NULL NULL 1 2 7");
  auto ret = Vector::BuildVector(TypeKind::INTID, true);
  std::vector<Datum> params{CreateDatum(ret.get()), CreateDatum(strs.get())};
  fu.test(params.data(), params.size(), CreateDatum(lens.get()));
}

}  // namespace dbcommon
