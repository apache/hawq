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

#include <cassert>
#include <vector>
#include <string>
#include <regex>
#include <algorithm>

#include "string_util.h"

using std::vector;
using std::string;

namespace hawq {
namespace test {

bool iequals(const string & str1, const string & str2) {
  if (str1.size() != str2.size()) {
    return false;
  }
  for(size_t k = 0; k < str1.size(); ++k) {
    if (tolower(str1[k]) != tolower(str2[k])) {
      return false;
    }
  }
  return true;
}

void replace(string & str,
             const string & search,
             const string & substitute) {
  size_t pos = 0;
  while ((pos = str.find(search, pos)) != string::npos) {
    str.replace(pos, search.size(), substitute);
    pos += substitute.size();
  }
}

void toLower(string & str) {
  std::transform(str.begin(), str.end(), str.begin(), ::tolower);
}

string lower(const string & str) {
  string ss(str);
  std::transform(ss.begin(), ss.end(), ss.begin(), ::tolower);
  return ss;
}

string &trim(string & str) { // NOLINT
  if (str.empty()) {
    return str;
  }
  str.erase(0, str.find_first_not_of(" "));
  str.erase(str.find_last_not_of(" ") + 1);
  return str;
}

string &trimNewLine(string & str) { // NOLINT
  str.erase(std::remove(str.begin(), str.end(), '\n'), str.end());
  return str;
}

vector<string> split(const string & str, char delimiter) {
  vector<string> res;
  size_t st = 0, en = 0;
  while(1) {
    en = str.find(delimiter, st);
    auto s = str.substr(st, en - st);
    if(s.size()) res.push_back(std::move(s));
    if(en == string::npos) break;
    st = en + 1;
  }
  return res;
}

string regexReplace(string & str,
                    const string & re,
                    const string & fmt) {
  const std::regex rexp(re);
  return std::regex_replace(str, rexp, fmt);
}

bool startsWith(const string & str, const string & key) {
  return str.find(key) == 0;
}

bool endsWith(const string & str, const string & key) {
  if(str.size() < key.size()) {
    return false;
  }
  return str.rfind(key) == (str.size() - key.size());
}

}
} // namespace hawq
