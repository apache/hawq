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

#include <pwd.h>
#include <string>
#include <vector>

#include "command.h"
#include "compent_config.h"
#include "string_util.h"
#include "xml_parser.h"

using std::string;

namespace hawq {
namespace test {

CompentConfig::CompentConfig() {
  iscloud = getenv("CLOUD_CLUSTER_ENV") ? true : false;
  slavesPhysicalHosts.clear();
  masterPhysicalHosts.clear();
  activeMasterPos = true;
  isdemo = false;
  compentPath = "";
  conn.reset(new hawq::test::PSQL(HAWQ_DB, HAWQ_HOST, HAWQ_PORT,
                                  this->getHawqUser(), HAWQ_PASSWORD));
}

bool CompentConfig::__fetchKubeCluster() {
  if (!iscloud)
    return false;

  string cmd, result;
  bool status;
  if (masterPhysicalHosts.size() == 0) {
    cmd = "kubectl get nodes | grep -v NAME | wc -l ";
    status = this->runCommand(KUBENET_MASTER, cmd, "", result, OS_COMMAND);
    if (!status)
      return false;
    int nodenum = std::atoi(result.c_str());
    if (nodenum > 1) {
      isdemo = false;
      cmd = "kubectl get nodes -a --show-labels=true | grep hadoopmaster | cut "
            "-d ' ' -f 1";
      status = this->runCommand(KUBENET_MASTER, cmd, "", result, OS_COMMAND);
      auto masterlines = hawq::test::split(result, '\n');
      for (size_t i = 0; i < masterlines.size(); i++) {
        masterPhysicalHosts.push_back(hawq::test::trim(masterlines[i]));
      }

      slavesPhysicalHosts.clear();
      cmd = "kubectl get nodes -a --show-labels=true | grep hadoopslave | cut "
            "-d ' ' -f 1";
      status =
          this->runCommand(masterPhysicalHosts[0], cmd, "", result, OS_COMMAND);
      auto slavelines = hawq::test::split(result, '\n');
      for (size_t i = 0; i < slavelines.size(); i++) {
        slavesPhysicalHosts.push_back(hawq::test::trim(slavelines[i]));
      }
    } else {
      return false;
    }
  }
  return true;
}

void CompentConfig::__copyCluster(std::vector<string> &masterhosts,
                                  std::vector<string> &slavehost) {
  masterhosts.clear();
  for (uint16_t i = 0; i < masterPhysicalHosts.size(); i++)
    masterhosts.push_back(masterPhysicalHosts[i]);

  slavehost.clear();
  for (uint16_t i = 0; i < slavesPhysicalHosts.size(); i++)
    slavehost.push_back(slavesPhysicalHosts[i]);
}

string CompentConfig::getHawqUser() {
  string user = HAWQ_USER;
  if (user.empty()) {
    struct passwd *pw;
    uid_t uid = geteuid();
    pw = getpwuid(uid);
    user.assign(pw->pw_name);
  }
  return user;
}

bool CompentConfig::runCommand(const std::string hostname,
                               const std::string &commandstr,
                               const std::string user, std::string &result,
                               CommandType cmdType) {
  string cmd = "ssh -o StrictHostKeyChecking=no ";
  cmd.append(hostname);
  CommandType refcmdType = cmdType;

  if ((refcmdType == YARN_COMMAND || refcmdType == HAWQ_OS_COMMAND) && !iscloud)
    refcmdType = OS_COMMAND;

  switch (refcmdType) {
  case OS_COMMAND: {
    cmd.append(" \"");
    if (user.size() > 0)
      cmd.append("sudo -u ").append(user).append(" env PATH=$PATH ");
    cmd.append(commandstr).append(" \"");
    break;
  }
  case HAWQ_COMMAND: {
    if (iscloud) {
      std::cout << "HAWQ command is not supported in cluod platform\n";
      EXPECT_TRUE(false);
      return false;
    }
    cmd.append(" \"source ");
    cmd.append(this->compentPath).append("/greenplum_path.sh;");
    cmd.append(commandstr).append(" \"");
    break;
  }
  case HAWQ_OS_COMMAND: {
    EXPECT_TRUE(iscloud == iscloud);
    cmd.append(" \"sudo docker exec `sudo docker ps | grep k8s_hawq-master | "
               "cut -d ' ' -f 1` sh -c '");
    cmd.append(commandstr).append("'\"");
    break;
  }
  case HDFS_COMMAND: {
    if (iscloud) {
      cmd.append(" \"sudo docker exec `sudo docker ps | grep k8s_hdfs-nn | cut "
                 "-d ' ' -f 1` sh -c '");
      cmd.append(commandstr).append("'\"");
    } else {
      cmd.append(" \"");
      if (user.size() > 0)
        cmd.append("sudo -u ").append(user).append(" env PATH=$PATH ");
      cmd.append(commandstr).append(" \"");
    }

    break;
  }
  case YARN_COMMAND: {
    std::cout << "HAWQ command is not supported in cluod platform\n";
    EXPECT_TRUE(false);
    return false;
  }
  }
  Command c(cmd);
  std::cout << cmd << "\n";
  result = c.run().getResultOutput();
  int status = c.getResultStatus();
  return (status == 0);
}

bool CompentConfig::runCommand(const std::string &commandstr,
                               const std::string user, std::string &result,
                               CommandType cmdType) {
  return this->runCommand(masterPhysicalHosts[MASTERPOS], commandstr, user,
                          result, cmdType);
}
bool CompentConfig::runCommandAndFind(const std::string &command,
                                      const std::string user,
                                      const std::string &findstring,
                                      CommandType cmdType) {
  string result = "";
  bool status = this->runCommand(command, user, result, cmdType);
  if (!status)
    return false;
  auto lines = hawq::test::split(result, '\n');
  for (size_t i = 0; i < lines.size(); i++) {
    string valueLine = lines[i];
    int find = valueLine.find(findstring);
    if (find >= 0) {
      return true;
    }
  }
  return false;
}

void CompentConfig::runCommandAndGetNodesPorts(
    const std::string &command, const std::string user,
    std::vector<string> &datanodelist, std::vector<int> &port,
    CommandType cmdType) {
  datanodelist.clear();
  port.clear();
  string result = "";
  bool status = this->runCommand(command, user, result, cmdType);
  if (status) {
    auto lines = hawq::test::split(result, '\n');
    for (size_t i = 0; i < lines.size(); i++) {
      string valueLine = lines[i];
      if (valueLine.find("ssh:") != string::npos)
        continue;
      if (valueLine.find("sudo:") != string::npos)
        continue;
      if (valueLine.find("WARNING") != string::npos)
        continue;
      auto datanodeInfo = hawq::test::split(valueLine, ':');
      if (datanodeInfo.size() == 3) {
        int portStart = datanodeInfo[2].find_first_of('(');
        int portEnd = datanodeInfo[2].find_first_of(')');
        string datanodePort = datanodeInfo[2].substr(0, portStart);
        string datanodeHost =
            datanodeInfo[2].substr(portStart + 1, portEnd - portStart - 1);
        datanodelist.push_back(hawq::test::trim(datanodeHost));
        port.push_back(std::stoi(hawq::test::trim(datanodePort)));
      }
    }
  }
}

bool CompentConfig::checkRemainedProcess() {
  string postcheckcmd = "ps -ef | grep postgres | grep con | grep -v idle | "
                        "grep -v gpsyncagent | grep -v grep | wc -l";
  string postshowcmd = "ps -ef | grep postgres | grep con | grep -v idle | "
                       "grep -v gpsyncagent | grep -v grep";
  string result;
  bool status;
  uint16_t postnum;

  for (uint16_t i = 0; i < masterPhysicalHosts.size(); i++) {
    uint16_t j;
    for (j = 0; j < MAX_RETRY_NUM; j++) {
      status = this->runCommand(masterPhysicalHosts[i], postcheckcmd, "",
                                result, OS_COMMAND);
      if (status) {
        postnum = atoi(result.c_str());
        if (postnum == 0)
          break;
        std::cout << "Checking remained postgres process and there  is "
                  << postnum << " remaind\n";
        status = this->runCommand(masterPhysicalHosts[i], postshowcmd, "",
                                  result, OS_COMMAND);
        std::cout << "remained process is:\n" << result << std::endl;
      }
      sleep(5);
    }
    if (j == MAX_RETRY_NUM) {
      status = this->runCommand(masterPhysicalHosts[i], postcheckcmd, "",
                                result, OS_COMMAND);
      std::cout << "Checking remained postgres process and there  is "
                << result;
      return false;
    }
  }

  for (uint16_t i = 0; i < slavesPhysicalHosts.size(); i++) {
    uint16_t j;
    for (j = 0; j < MAX_RETRY_NUM; j++) {
      status = this->runCommand(slavesPhysicalHosts[i], postcheckcmd, "",
                                result, OS_COMMAND);
      if (status) {
        postnum = atoi(result.c_str());
        if (postnum == 0)
          break;
        std::cout << "Checking remained postgres process and there is "
                  << postnum << " remained\n";
        status = this->runCommand(slavesPhysicalHosts[i], postshowcmd, "",
                                  result, OS_COMMAND);
        std::cout << "remained process is:\n" << result << std::endl;
      }
      sleep(10);
    }
    if (j == MAX_RETRY_NUM) {
      status = this->runCommand(slavesPhysicalHosts[i], postcheckcmd, "",
                                result, OS_COMMAND);
      std::cout << "Checking remained postgres process and there  is "
                << result;
      return false;
    }
  }
  return true;
}

bool CompentConfig::killRemainedProcess(int32_t segmentNum) {
  string hawqprocess = "ps -ef | grep postgres | grep con | grep -v idle | "
                       "grep -v grep | awk '{print \\$2}' ";
  string cmdkillstr = hawqprocess + " | sudo xargs kill -9 ";
  string cmdoutput;
  bool status;

  if (segmentNum == 0) {
    for (uint16_t i = 0; i < masterPhysicalHosts.size(); i++) {
      status = this->runCommand(masterPhysicalHosts[i], hawqprocess, "",
                                cmdoutput, OS_COMMAND);
      if (cmdoutput.size() != 0) {
        std::cout << "Kill process on " << masterPhysicalHosts[i]
                  << " and pid is " << cmdoutput;
        status = this->runCommand(masterPhysicalHosts[i], cmdkillstr, "",
                                  cmdoutput, OS_COMMAND);
        if (!status)
          std::cout << "Kill process on " << masterPhysicalHosts[i]
                    << " failed!\n"
                    << cmdoutput;
      }
    }
  }
  int32_t slavesize =
      (segmentNum == 0) ? slavesPhysicalHosts.size() : segmentNum;
  for (uint16_t i = 0; i < slavesize; i++) {
    status = this->runCommand(slavesPhysicalHosts[i], hawqprocess, "",
                              cmdoutput, OS_COMMAND);
    if (cmdoutput.size() != 0) {
      std::cout << "Kill process on " << slavesPhysicalHosts[i]
                << " and pid is " << cmdoutput;
      status = this->runCommand(slavesPhysicalHosts[i], cmdkillstr, "",
                                cmdoutput, OS_COMMAND);
      if (!status)
        std::cout << "Kill process on " << masterPhysicalHosts[i]
                  << " failed!\n"
                  << cmdoutput;
    }
  }
  return true;
}

} // namespace test
} // namespace hawq
