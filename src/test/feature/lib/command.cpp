#include <fstream>
#include "command.h"

using std::string;

namespace hawq {
namespace test {

Command& Command::run() {
  FILE *fp = NULL;
  char result[MAX_BUFFER_SIZE] = {0};
  if (!this->_isCommandValid()) {
    goto error;
  }
  fp = popen(this->_cmd_string.append(" 2>&1").c_str(), "r");
  if (NULL == fp) {
    goto error;
  }
  this->_result_output.clear();
  while(fgets(result, sizeof(result), fp) != NULL) {
    this->_result_output.append(result);
  }

  this->_result_status = pclose(fp);
  if (-1 == this->_result_status) {
    goto error;
  }
  this->_result_status = WEXITSTATUS(this->_result_status);

  if (this->_output_file.length() > 0) {
    this->_saveToFile(); 
  }
  return *this;

error:
  this->_result_status = -1;
  return *this;
}

Command& Command::setCommand(const string& cmd) {
  this->_cmd_string = cmd;
  return *this;
}

Command& Command::setOutputFile(const string& out) {
  this->_output_file = out;
  return *this;
}

const string& Command::getCommand() const {
  return this->_cmd_string;
}

const string& Command::getResultOutput() const {
  return this->_result_output;
}

int Command::getResultStatus() const {
  return this->_result_status;
}

bool Command::_isCommandValid() const {
  if (this->_cmd_string.length() > 0) {
    return true;
  } else {
    return false;
  }
}

void Command::_saveToFile() {
  std::ofstream out(this->_output_file, std::ofstream::out);
  out << this->_result_output;
  out.close(); 
}

const string Command::getCommandOutput(const string& cmd) {
  return Command()
      .setCommand(cmd)
      .run()
      .getResultOutput();
}

const string Command::getCommandOutput(const string& cmd,
                                        const string& out) {
  return Command()
      .setCommand(cmd)
      .setOutputFile(out)
      .run()
      .getResultOutput();
}

int Command::getCommandStatus(const string& cmd) {
  return Command()
      .setCommand(cmd)
      .run()
      .getResultStatus();
}

int Command::getCommandStatus(const string& cmd,
                              const string& out) {
  return Command()
      .setCommand(cmd)
      .setOutputFile(out)
      .run()
      .getResultStatus();
}

} // namespace test
} // namespace hawq
