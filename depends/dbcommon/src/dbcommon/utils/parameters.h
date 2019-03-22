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

#ifndef DBCOMMON_SRC_DBCOMMON_UTILS_PARAMETERS_H_
#define DBCOMMON_SRC_DBCOMMON_UTILS_PARAMETERS_H_

#include <string>
#include <unordered_map>
#include <vector>

#include "dbcommon/log/logger.h"
#include "dbcommon/utils/string-util.h"

namespace dbcommon {

typedef std::unordered_map<std::string, std::string> KeyValueMap;

// Case insensitive key-value parameters
class Parameters {
 public:
  Parameters() {}

  ~Parameters() {}

  Parameters(const Parameters &other) { parameters = other.parameters; }

  Parameters &operator=(const Parameters &other) {
    parameters = other.parameters;
    return *this;
  }
  Parameters(Parameters &&other) = delete;
  Parameters operator=(Parameters &&other) = delete;

  void set(const std::string &key, const std::string &value) {
    parameters[dbcommon::StringUtil::lower(key)] = value;
  }

  std::string get(const std::string &key) const { return getInternal(key); }

  std::string get(const std::string &key, const std::string &defValue) const {
    return exists(key) ? getInternal(key) : defValue;
  }

  int32_t getAsInt32(const std::string &key) const {
    return atoi(getInternal(key).c_str());
  }

  int32_t getAsInt32(const std::string &key, int32_t defValue) const {
    return exists(key) ? atoi(getInternal(key).c_str()) : defValue;
  }

  double getAsDouble(const std::string &key) const {
    return atof(getInternal(key).c_str());
  }

  double getAsDouble(const std::string &key, double defValue) const {
    return exists(key) ? atof(getInternal(key).c_str()) : defValue;
  }

  bool getAsBool(const std::string &key) const {
    return parseBool(getInternal(key).c_str());
  }

  bool getAsBool(const std::string &key, bool defValue) const {
    return exists(key) ? parseBool(getInternal(key).c_str()) : defValue;
  }

  bool exists(const std::string &key) const {
    return parameters.count(dbcommon::StringUtil::lower(key));
  }

  void getAllKVEntries(std::vector<std::string> *keys,
                       std::vector<std::string> *values) const {
    if (keys != nullptr) {
      keys->clear();
    }
    if (values != nullptr) {
      values->clear();
    }

    for (auto iter = parameters.begin(); iter != parameters.end(); ++iter) {
      if (keys != nullptr) {
        keys->push_back(iter->first);
      }
      if (values != nullptr) {
        values->push_back(iter->second);
      }
    }
  }

 private:
  const std::string &getInternal(const std::string &key) const {
    KeyValueMap::const_iterator iter =
        parameters.find(dbcommon::StringUtil::lower(key));
    if (iter == parameters.end()) {
      LOG_ERROR(ERRCODE_INVALID_PARAMETER_VALUE, "key not found: %s",
                key.c_str());
    } else {
      return iter->second;
    }
  }

  bool parseBool(const std::string &str) const {
    std::string lowerString = dbcommon::StringUtil::lower(str);
    return str == "true";
  }

 private:
  KeyValueMap parameters;
};

}  // namespace dbcommon

#endif  // DBCOMMON_SRC_DBCOMMON_UTILS_PARAMETERS_H_
