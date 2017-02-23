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

#ifndef HAWQ_SRC_TEST_FEATURE_LIB_COMMAND_H_
#define HAWQ_SRC_TEST_FEATURE_LIB_COMMAND_H_

#include <string>

namespace hawq {
namespace test {

class Command {
 public:
  Command() : _result_status(-1) {}
  explicit Command(const std::string& cmd)
      : _cmd_string(cmd), _result_status(-1) {}
  Command(const std::string& cmd, const std::string& out)
      : _cmd_string(cmd), _output_file(out), _result_status(-1) {}
  virtual ~Command() {}

  Command& run();
  Command& setCommand(const std::string&);
  Command& setOutputFile(const std::string&);
  const std::string& getCommand() const;
  const std::string& getResultOutput() const;
  int getResultStatus() const;

  static const std::string getCommandOutput(const std::string& cmd);
  static const std::string getCommandOutput(const std::string& cmd, const std::string& out);
  static int getCommandStatus(const std::string& cmd);
  static int getCommandStatus(const std::string& cmd, const std::string& out);

 private:
  Command(const Command&); 
  const Command& operator=(const Command&);

  bool _isCommandValid() const;
  void _saveToFile();

 private:
  std::string _cmd_string;
  std::string _output_file;
  std::string _result_output;
  int _result_status;

  static const int MAX_BUFFER_SIZE = 1024;
};

} // namespace test
} // namespace hawq

#endif
