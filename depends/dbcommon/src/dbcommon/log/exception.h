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

#ifndef DBCOMMON_SRC_DBCOMMON_LOG_EXCEPTION_H_
#define DBCOMMON_SRC_DBCOMMON_LOG_EXCEPTION_H_

#include <stdexcept>
#include <string>

namespace dbcommon {

//
// Base exception class
//
class Exception : public std::runtime_error {
 public:
  explicit Exception(const std::string& reason) : std::runtime_error(reason) {}
};

class TransactionAbortException : public Exception {
 public:
  explicit TransactionAbortException(const std::string& reason,
                                     const int errCode = 0, bool retry = false)
      : Exception(reason) {
    _errCode = errCode;
    _retry = retry;
  }

  int errCode() { return _errCode; }
  bool retry() { return _retry; }

 private:
  int _errCode;
  bool _retry;
};
}  // namespace dbcommon

#endif  // DBCOMMON_SRC_DBCOMMON_LOG_EXCEPTION_H_
