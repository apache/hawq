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

#ifndef HAWQ_SRC_TEST_FEATURE_LIB_STRING_UTIL_H_
#define HAWQ_SRC_TEST_FEATURE_LIB_STRING_UTIL_H_

#include <string>
#include <vector>
#include <cstdio>

namespace hawq {
namespace test {

bool iequals(const std::string &, const std::string &);

void replace(std::string &, const std::string &, const std::string &);

void toLower(std::string &);

std::string lower(const std::string &);

std::string &trim(std::string &);

std::string &trimNewLine(std::string &);

std::vector<std::string> split(const std::string &, char);

std::string regexReplace(std::string &, const std::string &, const std::string &);

bool startsWith(const std::string &, const std::string &);

bool endsWith(const std::string &, const std::string &);

template <typename... T>
std::string stringFormat(const std::string &fmt, T... vs) {
  char b;
  unsigned required = std::snprintf(&b, 0, fmt.c_str(), vs...) + 1;
  char bytes[required];
  std::snprintf(bytes, required, fmt.c_str(), vs...);
  return std::string(bytes);
}

} // namespace test
} // namespace hawq 

#endif
