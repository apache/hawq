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

#include "dbcommon/common/vector.h"
#include "dbcommon/function/func-kind.cg.h"
#include "dbcommon/function/func.h"
#include "dbcommon/function/invoker.h"

namespace dbcommon {

TEST(TestFunction, TestBasic) {
  std::string a = "1";
  std::string b = "2";
  Scalar retval;
  Scalar val1;
  Scalar val2;

  FuncKind func = TINYINT_LESS_THAN_TINYINT;
  std::unique_ptr<SelectList> selected(new SelectList());
  Datum varDatum1 = CreateDatum(a.c_str(), TINYINTID);
  Datum varDatum2 = CreateDatum(b.c_str(), TINYINTID);
  val1.value = varDatum1;
  val2.value = varDatum2;
  Invoker(func, 10)
      .addParam(CreateDatum<Scalar *>(&retval))
      .addParam(CreateDatum<Scalar *>(&val1))
      .addParam(CreateDatum<Scalar *>(&val2))
      .invoke();
  std::string opstr = "LESS_THAN";
  if (opstr == "LESS_THAN" || opstr == "LESS_EQ" || opstr == "NOT_EQUAL") {
    ASSERT_EQ(true, DatumGetValue<bool>(retval.value));
  } else {
    ASSERT_EQ(false, DatumGetValue<bool>(retval.value));
  }
}
}  // namespace dbcommon
