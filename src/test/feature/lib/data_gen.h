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
