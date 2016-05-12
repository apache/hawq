/*
 * data-gen.h
 *
 *  Created on: May 11, 2016
 *      Author: malili
 */

#ifndef SRC_TEST_FEATURE_LIB_DATA_GEN_H_
#define SRC_TEST_FEATURE_LIB_DATA_GEN_H_

#include <string>

#include "gtest/gtest.h"
#include "sql-util.h"

class DataGenerator {
 public:
  explicit DataGenerator(SQLUtility *sqlUtil) : sqlUtil(sqlUtil) {}

  ~DataGenerator() {}

  void genSimpleTable(std::string tableName, bool appendonly = true,
                      std::string orientation = "row",
                      std::string compresstype = "none", int compresslevel = 0);

  void genTableWithFullTypes(std::string tableName, bool appendonly = true,
                             std::string orientation = "row",
                             std::string compresstype = "none",
                             int compresslevel = 0);

  void genTableWithSeries(std::string tableName, bool appendonly = true,
                          std::string orientation = "row",
                          std::string compresstype = "none",
                          int compresslevel = 0);

  void genTableWithNull(std::string tableName, bool appendonly = true,
                        std::string orientation = "row",
                        std::string compresstype = "none",
                        int compresslevel = 0);

 private:
  std::string genTableDesc(bool appendonly, std::string orientation,
                           std::string compresstype, int compresslevel);

 private:
  SQLUtility *sqlUtil = nullptr;
};

#endif /* SRC_TEST_FEATURE_LIB_DATA_GEN_H_ */
