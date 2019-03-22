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

#ifndef DBCOMMON_SRC_DBCOMMON_UTILS_URL_H_
#define DBCOMMON_SRC_DBCOMMON_UTILS_URL_H_

#include <memory>
#include <string>
#include <utility>

namespace dbcommon {

class URL {
 public:
  explicit URL(const std::string &urlString) {
    rawString = urlString;
    parse(urlString);
  }
  ~URL() {}
  URL(const URL &other) = delete;
  URL operator=(const URL &other) = delete;
  URL(const URL &&other) = delete;
  URL operator=(const URL &&other) = delete;

  typedef std::unique_ptr<URL> uptr;

  // Return a normalized url in format (lower case): protocol://host:port/
  // @return Normalized url with three parts
  std::string getNormalizedServiceName() {
    std::string ret(protocol);
    ret.append("://").append(host);
    if (port != INVALID_PORT && protocol == "hdfs") {
      ret.append(":").append(std::to_string(port));
    }
    ret.append("/");
    return std::move(ret);
  }

  static std::string generateNormalizedServiceName(std::string protocol,
                                                   std::string host, int port) {
    std::string ret(protocol);
    ret.append("://").append(host);
    if (port != INVALID_PORT && protocol == "hdfs") {
      ret.append(":").append(std::to_string(port));
    }
    ret.append("/");
    return std::move(ret);
  }
  // host is returned in lower case
  const std::string &getHost() { return host; }
  const int getPort() { return port; }
  // protocol is returned in lower case
  const std::string &getProtocol() { return protocol; }
  const std::string &getPath() { return path; }
  const std::string &getQuery() { return query; }
  const std::string getRawPathAndQuery();
  const std::string &getRawString() { return rawString; }

  static const int INVALID_PORT;
  static const int EMPTY_PORT;

 private:
  void parse(const std::string &path);
  std::string rawString;
  std::string protocol;
  std::string path;
  std::string query;
  std::string host;
  int port = INVALID_PORT;
};

}  // end of namespace dbcommon
#endif  // DBCOMMON_SRC_DBCOMMON_UTILS_URL_H_
