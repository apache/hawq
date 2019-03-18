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

#include <algorithm>

#include "sqlfile-parsebase.h"

std::string SqlFileParseBase::trim(std::string str) {
  size_t beginNotSpace = 0;
  for (; beginNotSpace != str.size() && isspace(str[beginNotSpace]);
       ++beginNotSpace)
    ;
  str.erase(0, beginNotSpace);
  if (str.empty()) return str;
  size_t endNotSpace = str.size() - 1;
  for (; isspace(str[endNotSpace]); --endNotSpace)
    ;
  str.erase(endNotSpace + 1);
  return str;
}

size_t SqlFileParseBase::__find_is_not_space(const std::string &line) {
  std::string::const_iterator pos = std::find_if_not(
      line.cbegin(), line.cend(), [](const char c) { return isspace(c); });
  return (pos - line.cbegin());
}

bool SqlFileParseBase::__sub_equal(const std::string &target, size_t pos,
                                   const std::string &tag) {
  return __sub_equal(target.substr(pos), tag);
}

bool SqlFileParseBase::__sub_equal(const std::string &target,
                                   std::string::const_iterator pos,
                                   const std::string &tag) {
  return __sub_equal(target.substr(pos - target.cbegin()), tag);
}

bool SqlFileParseBase::__sub_equal(const std::string &target,
                                   const std::string &tag) {
  return target.size() >= tag.size() && target.substr(0, tag.size()) == tag;
}

bool SqlFileParseBase::__open_error(const std::string &filename) {
  return __error(filename + " open error");
}

bool SqlFileParseBase::__not_found_tag_error(const size_t index,
                                             const std::string &tag) {
  return SqlFileParseBase::__error("line " + std::to_string(index) +
                                   " not found " + tag);
}

bool SqlFileParseBase::__trans_file_to_vector(const std::string &infile,
                                              std::vector<std::string> &v_inf) {
  std::ifstream inf(infile);
  if (!inf) return __open_error(infile);
  std::string lineBuf;
  std::vector<std::string> v_temp_inf;
  for (; getline(inf, lineBuf);) v_temp_inf.push_back(std::move(lineBuf));
  v_inf.swap(v_temp_inf);
  inf.close();
  return true;
}

bool SqlFileParseBase::__is_tag(const std::string &line,
                                const std::string &tag) {
  return __sub_equal(line, __find_is_not_space(line), tag);
}

bool SqlFileParseBase::__error(const std::string &error_message) {
  std::cerr << parseName_ << ": error: " << error_message << std::endl;
  return false;
}