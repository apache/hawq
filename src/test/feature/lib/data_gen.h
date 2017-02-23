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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/*
 * data_gen.h
 *
 *  Created on: May 11, 2016
 *      Author: malili
 */

#ifndef HAWQ_SRC_TEST_FEATURE_LIB_DATA_GEN_H_
#define HAWQ_SRC_TEST_FEATURE_LIB_DATA_GEN_H_

#include <string>

#include "gtest/gtest.h"
#include "sql_util.h"

namespace hawq {
namespace test {

class DataGenerator {
 public:
  explicit DataGenerator(hawq::test::SQLUtility *sqlUtil_) : sqlUtil(sqlUtil_) {}

  ~DataGenerator() {}

  void genSimpleTable(std::string tableName,
                      bool appendonly = true,
                      std::string orientation = "row",
                      std::string compresstype = "none",
                      int compresslevel = -1);

  void genAggregateTable(std::string tableName,
                         bool appendonly = true,
                         std::string orientation = "row",
                         std::string compresstype = "none",
                         int compresslevel = -1);

  void genTableWithFullTypes(std::string tableName,
                             bool appendonly = true,
                             std::string orientation = "row",
                             std::string compresstype = "none",
                             int compresslevel = -1);

  void genTableWithSeries(std::string tableName,
                          bool appendonly = true,
                          std::string orientation = "row",
                          std::string compresstype = "none",
                          int compresslevel = -1);

  void genTableWithNull(std::string tableName,
                        bool appendonly = true,
                        std::string orientation = "row",
                        std::string compresstype = "none",
                        int compresslevel = -1);

 private:
  std::string genTableDesc(bool appendonly,
                           std::string orientation,
                           std::string compresstype,
                           int compresslevel);

 private:
  hawq::test::SQLUtility *sqlUtil = nullptr;
};

} // namespace test
} // namespace hawq

#endif
