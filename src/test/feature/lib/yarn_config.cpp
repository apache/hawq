#include <fstream>
#include <string>
#include <vector>
#include <unordered_set>

#include "yarn_config.h"
#include "command.h"
#include "psql.h"
#include "xml_parser.h"
#include "string_util.h"

using std::string;

namespace hawq {
namespace test {

void YarnConfig::runCommand(const string &command, 
                            bool isyarnuser, 
                            string &result) {
  string cmd = "";
  if (isyarnuser) {
    cmd = "/usr/bin/sudo -Eu ";
    cmd.append(getYarnUser());
    cmd.append(" env \"PATH=$PATH\" ");
    cmd.append(command);
  } else {
    cmd = command;
  }
  Command c(cmd);
  result = c.run().getResultOutput();
}

bool YarnConfig::runCommandAndFind(const string &command, 
                                   bool isyarnuser, 
                                   const string &findstring) {
  string result = "";
  runCommand(command, isyarnuser, result);
  auto lines = hawq::test::split(result, '\n');
  for (size_t i=0; i<lines.size(); i++) {
    string valueLine = lines[i];
    int find = valueLine.find(findstring);
    if (find >= 0) {
        return true;
    }
  }
  return false;
}

void YarnConfig::runCommandAndGetNodesPorts(const string &command,
                                            bool isyarnuser,
                                            std::vector<string> &nodemanagers,
                                            std::vector<int> &port) {
  string result = "";
  runCommand(command, isyarnuser, result);
  auto lines = hawq::test::split(result, '\n');
  bool begin = false;
  for (size_t i=0; i<lines.size(); i++) {
    if (!begin) {
      if (lines[i].find("Node-Id") != string::npos) {
        begin = true;
      }
    } else {
      string values = hawq::test::split(lines[i], '\t')[0];
      nodemanagers.push_back(hawq::test::trim(hawq::test::split(values, ':')[0]));
      port.push_back(std::stoi(hawq::test::trim(hawq::test::split(values, ':')[1])));
    }
  }
}

string YarnConfig::getYarnUser() {
  string result = "";
  runCommand("ps aux|grep yarn.server|grep -v grep", false, result);
  auto lines = hawq::test::split(result, '\n');
  if (lines.size() >= 1) {
    return hawq::test::trim(hawq::test::split(lines[lines.size()-1], ' ')[0]);
  } 
  return "yarn";
}

bool YarnConfig::LoadFromHawqConfigFile() {
  if (isLoadFromHawqConfigFile) {
    return true;
  }
  const char *env = getenv("GPHOME");
  string confPath = env ? env : "";
  if (confPath != "") {
    confPath.append("/etc/yarn-client.xml");
  } else {
    return false;
  }

  hawqxmlconf.reset(new XmlConfig(confPath));
  if (!hawqxmlconf->parse())
    return false;
  return true;
}

bool YarnConfig::LoadFromYarnConfigFile() {
  if (isLoadFromYarnConfigFile) {
    return true;
  }
  string confPath=getHadoopHome();
  if (confPath == "") {
    return false;
  }
  confPath.append("/etc/hadoop/yarn-site.xml");
  yarnxmlconf.reset(new XmlConfig(confPath));
  if (!yarnxmlconf->parse())
    return false;
  return true;
}

int YarnConfig::isConfigYarn() {
  bool ret = LoadFromYarnConfigFile();
  if (!ret) {
    return -1;
  }
  string rm = yarnxmlconf->getString("yarn.resourcemanager.address.rm1");
  if (rm == "") {
    return 0;
  }
  return 1;
}

int YarnConfig::isHA() {
  const hawq::test::PSQLQueryResult &result = psql.getQueryResult(
       "SELECT substring(fselocation from length('hdfs:// ') for (position('/' in substring(fselocation from length('hdfs:// ')))-1)::int) "
       "FROM pg_filespace pgfs, pg_filespace_entry pgfse "
       "WHERE pgfs.fsname = 'dfs_system' AND pgfse.fsefsoid=pgfs.oid ;");
  std::vector<std::vector<string>> table = result.getRows();
  if (table.size() > 0) {
    int find = table[0][0].find(":");
    if (find < 0) {
      return 1;
    } else {
      return 0;
    }
  }
  return -1;
}

int YarnConfig::isConfigKerberos() {
  bool ret = LoadFromHawqConfigFile();
  if (!ret) {
    return -1;
  }
  string authentication = hawqxmlconf->getString("hadoop.security.authentication");
  if (authentication == "kerberos") {
    return 1;
  } else {
    return 0;
  }
}

string YarnConfig::getHadoopHome() {
  string result = "";
  runCommand("ps -ef|grep hadoop", false, result);
  string hadoopHome = "";
  auto lines = hawq::test::split(result, '\n');
  for (size_t i=0; i<lines.size()-1; i++) {
    string valueLine = lines[i];
    int pos = valueLine.find("-Dhadoop.home.dir=");
    if (pos >=0 ) {
      string valueTmp = valueLine.substr(pos+18); 
      int valueEnd = valueTmp.find_first_of(" ");
      string value = valueTmp.substr(0, valueEnd);
      hadoopHome = hawq::test::trim(value);
      return hadoopHome;
    }
  }
  return hadoopHome;
}

bool YarnConfig::getActiveRM(string &activeRM,
                                   int &port) {
    return getHARM("active", activeRM, port);
}

bool YarnConfig::getStandbyRM(string &standbyRM,
                                    int &port) {
    return getHARM("standby", standbyRM, port);
}

bool YarnConfig::getHARM(const string &RMtype,
                               string &RM,
                               int &port) {
  if (isHA() <= 0) {
    return false;
  }
  string RMService = "";
  string haRMValue = "rm1,rm2";
  auto haRMs = hawq::test::split(haRMValue, ',');
  for (size_t i = 0; i < haRMs.size(); i++) {
    string haRM = hawq::test::trim(haRMs[i]);
    string cmd = "yarn rmadmin -getServiceState ";
    cmd.append(haRM);
    if (runCommandAndFind(cmd, true, RMtype)) {
      RMService = haRM;
      break;
    }
  }
  bool ret = LoadFromYarnConfigFile();
  if (!ret) {
    return false;
  }
  string rpcAddressName = "yarn.resourcemanager.address.";
  rpcAddressName.append(RMService);
  string rpcAddressValue = yarnxmlconf->getString(rpcAddressName);
  auto RMInfo = hawq::test::split(rpcAddressValue, ':');
  RM = hawq::test::trim(RMInfo[0]);
  port = std::stoi(hawq::test::trim(RMInfo[1]));
  return true;
}

bool YarnConfig::getRMList(std::vector<string> &RMList,
                           std::vector<int> &ports){
  string RM = "";
  int port;
  if (isHA()) {
    getActiveRM(RM, port);
    RMList.push_back(RM);
    ports.push_back(port);
    getStandbyRM(RM, port);
    RMList.push_back(RM);
    ports.push_back(port);
    return true;
  }

  bool ret = LoadFromYarnConfigFile();
  if (!ret) {
    return false;
  }

  string RMAddressName = "yarn.resourcemanager.address";
  string RMAddressValue = yarnxmlconf->getString(RMAddressName);
  if (RMAddressValue == "") {
    return false;
  }
  auto RMInfo = hawq::test::split(RMAddressValue, ':');
  RM = hawq::test::trim(RMInfo[0]);
  port = std::stoi(hawq::test::trim(RMInfo[1]));
  RMList.push_back(RM);
  ports.push_back(port);
  return true;
}

void YarnConfig::getNodeManagers(std::vector<string> &nodemanagers,
                                 std::vector<int> &port) {
  runCommandAndGetNodesPorts("yarn node -list -all", true, nodemanagers, port);
}

void YarnConfig::getActiveNodeManagers(std::vector<string> &nodemanagers,
                                 std::vector<int> &port) {
  runCommandAndGetNodesPorts("yarn node -list -states RUNNING", true, nodemanagers, port);
}


string YarnConfig::getParameterValue(const string &parameterName) {
  bool ret = LoadFromYarnConfigFile();
  if (!ret) {
    return "Error: failed to load from YARN configuration file";
  }

  return yarnxmlconf->getString(parameterName);
}

string YarnConfig::getParameterValue(const string &parameterName,
                                     const string &conftype) {
  if (hawq::test::lower(conftype) == "yarn") {
    return getParameterValue(parameterName);
  }
  bool ret = LoadFromHawqConfigFile();
  if (!ret) {
    return "Error: failed to load from HAWQ configuration file";
  }

  return hawqxmlconf->getString(parameterName);
}

bool YarnConfig::setParameterValue(const string &parameterName,
                                   const string &parameterValue) {
  bool ret = LoadFromYarnConfigFile();
  if (!ret) {
    return false;
  }

  return yarnxmlconf->setString(parameterName, parameterValue);
}

bool YarnConfig::setParameterValue(const string &parameterName,
                                   const string &parameterValue,
                                   const string &conftype) {
  if (hawq::test::lower(conftype) == "yarn") {
    return setParameterValue(parameterName, parameterValue);
  }
  bool ret = LoadFromHawqConfigFile();
  if (!ret) {
    return false;
  }

  return hawqxmlconf->setString(parameterName, parameterValue);
}

} // namespace test
} // namespace hawq
