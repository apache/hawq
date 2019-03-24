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

#ifndef UNIVPLAN_TEST_UNIT_TEST_UNIVPLAN_H_
#define UNIVPLAN_TEST_UNIT_TEST_UNIVPLAN_H_

#include <fstream>
#include <iostream>
#include <sstream>
#include <string>

#include "gtest/gtest.h"

namespace univplan {
class UnivPlanPlanString {
 public:
  UnivPlanPlanString() {}
  ~UnivPlanPlanString() { planString.clear(); }

  void readFromFile(const char *fileName) {
    std::ifstream in;
    std::string filePath(DATA_DIR);
    filePath.append(fileName);
    in.open(filePath);
    ASSERT_TRUE(!!in);
    std::string str;
    planString.clear();
    while (getline(in, str)) {
      planString.append(str);
      planString.append("\n");
    }
    in.close();
  }

  // only used for creating univplan ans file
  void writeIntoFile(const char *fileName, const char *str) {
    std::ofstream out;
    std::string filePath(DATA_DIR);
    filePath.append(fileName);
    out.open(filePath);
    out << str;
    out.close();
  }

  const char *getPlanString() { return planString.c_str(); }

 private:
  std::string planString;
};
}  // namespace univplan
#endif  // UNIVPLAN_TEST_UNIT_TEST_UNIVPLAN_H_
