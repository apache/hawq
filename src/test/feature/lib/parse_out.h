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

#ifndef HAWQ_SRC_TEST_FEATURE_PARSE_OUT_H_
#define HAWQ_SRC_TEST_FEATURE_PARSE_OUT_H_

#include "lib/sqlfile-parsebase.h"

namespace hawq {
namespace test {

struct SqlOut {
  SqlOut(bool is_ignore, double time, std::string &&sql)
      : is_ignore(is_ignore), time(time), sql(std::move(sql)) {}

  bool is_ignore;
  double time;
  std::string sql;
};

class Sqlout_parser : public SqlFileParseBase {
 public:
  Sqlout_parser() : SqlFileParseBase("parse_out") {}
  bool parse_sql_time(
      const std::string &input_sqloutfile,
      std::vector<std::pair<std::string, double>> &output_sqloutinfo);

  static inline std::string reportSql(const std::string &oneSql) {
    return sql_start_tag + "\n" + oneSql + out_start_tag + "\n" + sql_end_tag;
  }

  static inline std::string reportIgnoreSql(const std::string oneSql) {
    return ignore_start_tag + "\n" + reportSql(oneSql) + "\n" + ignore_end_tag;
  }

 private:
  bool __parse_sql_time(const std::string &outfile, std::vector<SqlOut> &ouf);

  bool __parse_sql_time_imple(const std::vector<std::string> &inf,
                              std::vector<SqlOut> &ouf);

  bool __parse_ignore(const std::vector<std::string> &inf,
                      std::vector<SqlOut> &ouf, size_t &start_index,
                      const size_t end_index, bool ignore);

  bool __parse_sqlout(const std::vector<std::string> &inf,
                      std::vector<SqlOut> &ouf, size_t &start_index,
                      const size_t end_index, bool ignore);

  bool __parse_sqlout_imple(const std::vector<std::string> &inf,
                            std::vector<SqlOut> &ouf, size_t &start_index,
                            const size_t end_index, const std::string &end_tag,
                            bool ignore);

  bool __parse_time(const std::string &line, double &outtime);

 private:
  const std::string time_tag = "Time: ";

 private:
  static const std::string ignore_start_tag;
  static const std::string ignore_end_tag;
  static const std::string oneline_sql_start_tag;
  static const std::string oneline_sql_end_tag;
  static const std::string sql_start_tag;
  static const std::string sql_end_tag;
  static const std::string out_start_tag;
};
}  // namespace test
}  // namespace hawq
#endif
