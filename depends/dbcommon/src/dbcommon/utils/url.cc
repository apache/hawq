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

#include "dbcommon/utils/url.h"

#include <algorithm>
#include <cassert>
#include <cctype>
#include <functional>
#include <string>

#include "dbcommon/log/logger.h"

namespace dbcommon {

const int URL::INVALID_PORT = -1;
const int URL::EMPTY_PORT = 0;

const std::string URL::getRawPathAndQuery() {
  const std::string protocolEnd("://");
  std::string::iterator protocolIter =
      std::search(rawString.begin(), rawString.end(), protocolEnd.begin(),
                  protocolEnd.end());
  int protocolLen = std::distance(rawString.begin(), protocolIter);
  assert(protocolLen > 0);
  return rawString.substr(protocolLen + 3, std::string::npos);
}

void URL::parse(const std::string& urlString) {
  const std::string protocolEnd("://");
  std::string::const_iterator protocolIter =
      std::search(urlString.begin(), urlString.end(), protocolEnd.begin(),
                  protocolEnd.end());

  if (protocolIter == urlString.end())
    LOG_ERROR(ERRCODE_INVALID_PARAMETER_VALUE,
              "wrong URL format, the path does not contain '://', path [%s]",
              urlString.c_str());

  int protocolLen = std::distance(urlString.begin(), protocolIter);
  if (protocolLen == 0)
    LOG_ERROR(ERRCODE_INVALID_PARAMETER_VALUE,
              "wrong URL format, no protocol found, path [%s]",
              urlString.c_str());

  // change protocol to lower case
  protocol.reserve(protocolLen);
  std::transform(urlString.begin(), protocolIter, back_inserter(protocol),
                 std::ptr_fun<int, int>(tolower));

  std::advance(protocolIter, protocolEnd.length());
  std::string::const_iterator hostIter =
      std::find(protocolIter, urlString.end(), '/');
  int hostAndPortLen = std::distance(protocolIter, hostIter);

  if (hostIter == urlString.end() && hostAndPortLen == 0) {
    LOG_ERROR(ERRCODE_INVALID_PARAMETER_VALUE,
              "wrong URL format, no host and path found, path [%s]",
              urlString.c_str());
  }

  std::string hostAndPort;

  if (hostAndPortLen != 0) {
    hostAndPort.reserve(hostAndPortLen);
    // change host:port pair to lower case
    std::transform(protocolIter, hostIter, back_inserter(hostAndPort),
                   std::ptr_fun<int, int>(tolower));

    std::string::iterator portIter =
        std::find(hostAndPort.begin(), hostAndPort.end(), ':');

    int hostLen = std::distance(hostAndPort.begin(), portIter);
    if (hostLen == 0) {
      LOG_ERROR(ERRCODE_INVALID_PARAMETER_VALUE,
                "wrong URL format, no host found, path [%s]",
                urlString.c_str());
    }
    std::transform(hostAndPort.begin(), portIter, back_inserter(host),
                   std::ptr_fun<int, int>(tolower));
    // host.append(hostAndPort.begin(), portIter);

    if (portIter == hostAndPort.end()) {
      port = EMPTY_PORT;
    } else {
      portIter++;
      int portLen = std::distance(portIter, hostAndPort.end());
      if (portLen == 0) {
        LOG_ERROR(ERRCODE_INVALID_PARAMETER_VALUE,
                  "wrong URL format, no port found, path [%s]",
                  urlString.c_str());
      }
      std::string portString(portIter, hostAndPort.end());
      port = std::atoi(portString.c_str());
    }
    // hdfs nameservice is case-sensitive
    if (protocol == "hdfs" && port == EMPTY_PORT)
      host = urlString.substr(protocolLen + protocolEnd.length(), hostLen);
  }

  if (hostIter == urlString.end()) path.append("/");
  std::string::const_iterator queryIter = find(hostIter, urlString.end(), '?');
  path.append(hostIter, queryIter);

  if (queryIter != urlString.end()) ++queryIter;
  query.assign(queryIter, urlString.end());
}

}  // end of namespace dbcommon
