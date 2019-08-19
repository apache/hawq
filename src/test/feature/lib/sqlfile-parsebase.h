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

#ifndef HAWQ_SRC_TEST_FEATURE_SQLFILE_PARSEBASE_OUT_H_
#define HAWQ_SRC_TEST_FEATURE_SQLFILE_PARSEBASE_OUT_H_

#include <fstream>
#include <iostream>
#include <string>
#include <vector>

class SqlFileParseBase {
 public:
  SqlFileParseBase(const std::string &parseName) : parseName_(parseName) {}

  static std::string trim(std::string str);

 protected:
  size_t __find_is_not_space(const std::string &line);

  bool __sub_equal(const std::string &target, size_t pos,
                   const std::string &tag);

  bool __sub_equal(const std::string &target, std::string::const_iterator pos,
                   const std::string &tag);

  bool __sub_equal(const std::string &target, const std::string &tag);

  bool __error(const std::string &error_message);

  bool __open_error(const std::string &filename);

  bool __not_found_tag_error(const size_t index, const std::string &tag);

  bool __trans_file_to_vector(const std::string &infile,
                              std::vector<std::string> &v_inf);

  bool __is_tag(const std::string &line, const std::string &tag);

  bool __is_comment(const std::string &line) { return __is_tag(line, "--"); }

 private:
  const std::string parseName_;
};

#endif
