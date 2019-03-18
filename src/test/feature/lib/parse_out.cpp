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

#include "parse_out.h"

namespace hawq {
namespace test {
using namespace std;

const std::string Sqlout_parser::ignore_start_tag = "--ignore_start";
const std::string Sqlout_parser::ignore_end_tag = "--ignore_end";
const std::string Sqlout_parser::oneline_sql_start_tag = "--oneline_sql_start";
const std::string Sqlout_parser::oneline_sql_end_tag = "--oneline_sql_end";
const std::string Sqlout_parser::sql_start_tag = "--sql_start";
const std::string Sqlout_parser::sql_end_tag = "--sql_end";
const std::string Sqlout_parser::out_start_tag = " --out_start";

bool Sqlout_parser::parse_sql_time(
    const string &input_sqloutfile,
    vector<pair<string, double>> &output_sqloutinfo) {
  vector<SqlOut> sqloutinfo;
  vector<pair<string, double>> temp_output;
  if (__parse_sql_time(input_sqloutfile, sqloutinfo) == false) return false;
  for (size_t i = 0; i != sqloutinfo.size(); ++i) {
    if (!sqloutinfo[i].is_ignore) {
      temp_output.emplace_back(std::move(sqloutinfo[i].sql),
                               sqloutinfo[i].time);
    }
  }
  output_sqloutinfo.swap(temp_output);
  return true;
}

bool Sqlout_parser::__parse_sql_time(const string &outfile,
                                     vector<SqlOut> &ouf) {
  vector<string> v_inf;
  if (__trans_file_to_vector(outfile, v_inf) == false) return false;
  if (__parse_sql_time_imple(v_inf, ouf) == false) return false;
  return true;
}

bool Sqlout_parser::__parse_sql_time_imple(const vector<string> &inf,
                                           vector<SqlOut> &ouf) {
  size_t module_start_index = 0, module_end_index = 0;
  for (;;) {
    if (__is_tag(inf[module_end_index], ignore_start_tag) == true) {
      if (__parse_ignore(inf, ouf, module_start_index, module_end_index,
                         false) == false)
        return false;
      module_start_index = module_end_index + 1;
      for (; __is_tag(inf[module_end_index], ignore_end_tag) == false;) {
        if (++module_end_index == inf.size())
          return __not_found_tag_error(module_start_index, ignore_end_tag);
      }
      if (__parse_ignore(inf, ouf, module_start_index, module_end_index,
                         true) == false)
        return false;
    } else if (++module_end_index == inf.size()) {
      if (__parse_ignore(inf, ouf, module_start_index, module_end_index,
                         false) == false)
        return false;
      return true;
    } else {
    }
  }
  return false;
}

bool Sqlout_parser::__parse_ignore(const vector<string> &inf,
                                   vector<SqlOut> &ouf, size_t &start_index,
                                   const size_t end_index, bool ignore) {
  while (start_index != end_index) {
    if (__parse_sqlout(inf, ouf, start_index, end_index, ignore) == false)
      return false;
  }
  return true;
}

bool Sqlout_parser::__parse_sqlout(const vector<string> &inf,
                                   vector<SqlOut> &ouf, size_t &start_index,
                                   const size_t end_index, bool ignore) {
  for (; start_index != end_index;) {
    const string &line = inf[start_index++];
    if (__is_tag(line, oneline_sql_start_tag) == true) {
      if (__parse_sqlout_imple(inf, ouf, start_index, end_index,
                               oneline_sql_end_tag, ignore) == false)
        return false;
    } else if (__is_tag(line, sql_start_tag) == true) {
      if (__parse_sqlout_imple(inf, ouf, start_index, end_index, sql_end_tag,
                               ignore) == false)
        return false;
    } else {
    }
  }
  return true;
}

bool Sqlout_parser::__parse_sqlout_imple(const vector<string> &inf,
                                         vector<SqlOut> &ouf,
                                         size_t &start_index,
                                         const size_t end_index,
                                         const string &end_tag, bool ignore) {
  string sql;
  size_t sql_start_index = start_index;
  size_t out_pos;
  if (end_tag == oneline_sql_end_tag) {
    if ((out_pos = inf[start_index].find(out_start_tag)) == string::npos)
      return __not_found_tag_error(start_index, out_start_tag);
    sql = inf[start_index].substr(0, out_pos);
  } else {
    if ((out_pos = inf[start_index].find(out_start_tag)) != string::npos) {
      sql = inf[start_index].substr(0, out_pos);  // if sql is empty or oneline
    } else {
      sql = inf[start_index];
      for (++start_index; start_index != end_index; ++start_index) {
        const string &line = inf[start_index];
        if ((out_pos = line.find(out_start_tag)) != string::npos) {
          sql.append("\n").append(line.substr(0, out_pos));
          break;
        } else {
          sql.append("\n").append(line);
        }
      }
    }

    if (start_index == end_index)
      return __not_found_tag_error(start_index, out_start_tag);
  }
  double outtime = -1.0;
  for (++start_index; start_index != end_index; ++start_index) {
    const string &line = inf[start_index];
    __parse_time(line, outtime);
    if (__sub_equal(line, end_tag) == true) {
      ouf.emplace_back(ignore, outtime, std::move(sql));
      return true;
    }
  }
  return __not_found_tag_error(sql_start_index, end_tag);
}

bool Sqlout_parser::__parse_time(const string &line, double &outtime) {
  if (__sub_equal(line, 0, time_tag)) {
    size_t start_pos = time_tag.size();
    size_t end_pos = line.find(' ', start_pos);
    outtime = stod(line.substr(start_pos, end_pos - start_pos));
    return true;
  }
  return false;
}

}  // namespace test
}  // namespace hawq