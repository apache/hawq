#include <fstream>
#include <string>
#include <vector>
#include <unordered_set>

#include "hdfs_config.h"
#include "command.h"
#include "psql.h"
#include "xml_parser.h"
#include "string_util.h"

using std::string;

namespace hawq {
namespace test {

void HdfsConfig::runCommand(const string &command, 
                            bool ishdfsuser, 
                            string &result) {
  string cmd = "";
  if (ishdfsuser) {
    cmd = "/usr/bin/sudo -Eu ";
    cmd.append(getHdfsUser());
    cmd.append(" env \"PATH=$PATH\" ");
    cmd.append(command);
  } else {
    cmd = command;
  }
  Command c(cmd);
  result = c.run().getResultOutput();
}

bool HdfsConfig::runCommandAndFind(const string &command, 
                                   bool ishdfsuser, 
                                   const string &findstring) {
  string result = "";
  runCommand(command, ishdfsuser, result);
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

void HdfsConfig::runCommandAndGetNodesPorts(const string &command, 
                                            std::vector<string> &datanodelist,
                                            std::vector<int> &port) {
  string result = "";
  runCommand(command, true, result);
  auto lines = hawq::test::split(result, '\n');
  for (size_t i = 0; i < lines.size(); i++) {
    string valueLine = lines[i];
    auto datanodeInfo = hawq::test::split(valueLine, ':');
    if (datanodeInfo.size() == 3) {
      int portStart = datanodeInfo[2].find_first_of('(');
      int portEnd = datanodeInfo[2].find_first_of(')');
      string datanodePort = datanodeInfo[2].substr(0, portStart);
      string datanodeHost = datanodeInfo[2].substr(portStart+1, portEnd-portStart-1);
      datanodelist.push_back(hawq::test::trim(datanodeHost));
      port.push_back(std::stoi(hawq::test::trim(datanodePort)));
    }
  }
}

string HdfsConfig::getHdfsUser() {
  string cmd = "ps aux|grep hdfs.server|grep -v grep";
  Command c(cmd);
  string result = c.run().getResultOutput();
  auto lines = hawq::test::split(result, '\n');
  if (lines.size() >= 1) {
    return hawq::test::trim(hawq::test::split(lines[lines.size()-1], ' ')[0]);
  } 
  return "hdfs";
}

bool HdfsConfig::LoadFromHawqConfigFile() {
  if (isLoadFromHawqConfigFile) {
    return true;
  }
  const char *env = getenv("GPHOME");
  string confPath = env ? env : "";
  if (confPath != "") {
    confPath.append("/etc/hdfs-client.xml");
  } else {
    return false;
  }

  hawqxmlconf.reset(new XmlConfig(confPath));
  if (!hawqxmlconf->parse())
    return false;

  isLoadFromHawqConfigFile = true;
  return true;
}

bool HdfsConfig::LoadFromHdfsConfigFile() {
  if (isLoadFromHdfsConfigFile) {
    return true;
  }
  string confPath=getHadoopHome();
  if (confPath == "") {
    return false;
  }
  confPath.append("/etc/hadoop/hdfs-site.xml");
  hdfsxmlconf.reset(new XmlConfig(confPath));
  if (!hdfsxmlconf->parse())
    return false;
  
  isLoadFromHdfsConfigFile = true;
  return true;
}

int HdfsConfig::isHA() {
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

int HdfsConfig::isConfigKerberos() {
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

int HdfsConfig::isTruncate() {
  if (runCommandAndFind("hadoop fs -truncate", false, "-truncate: Unknown command")) {
    return 0;
  } else {
    return 1;
  }
}

string HdfsConfig::getHadoopHome() {
  string result = "";
  runCommand("ps -ef|grep hadoop", false, result);
  string hadoopHome = "";
  auto lines = hawq::test::split(result, '\n');
  for (size_t i=0; i<lines.size()-1; i++) {
    string valueLine = lines[i];
    string findstring = "-Dhadoop.home.dir=";
    int pos = valueLine.find(findstring);
    if (pos >=0 ) {
      string valueTmp = valueLine.substr(pos+findstring.size()); 
      int valueEnd = valueTmp.find_first_of(" ");
      string value = valueTmp.substr(0, valueEnd);
      hadoopHome = hawq::test::trim(value);
      return hadoopHome;
    }
  }
  return hadoopHome;
}

bool HdfsConfig::getActiveNamenode(string &activenamenode,
                                   int &port) {
    return getHANamenode("active", activenamenode, port);
}

bool HdfsConfig::getStandbyNamenode(string &standbynamenode,
                                    int &port) {
    return getHANamenode("standby", standbynamenode, port);
}

bool HdfsConfig::getHANamenode(const string &namenodetype,
                               string &namenode,
                               int &port) {
  if (isHA() <= 0) {
    return false;
  }
  string namenodeService = "";
  string nameServiceValue = hawqxmlconf->getString("dfs.nameservices");
  string haNamenodesName = "dfs.ha.namenodes.";
  haNamenodesName.append(hawq::test::trim(nameServiceValue));
  string haNamenodesValue = hawqxmlconf->getString(haNamenodesName);
  auto haNamenodes = hawq::test::split(haNamenodesValue, ',');
  for (size_t i = 0; i < haNamenodes.size(); i++) {
    string haNamenode = hawq::test::trim(haNamenodes[i]);
    string cmd = "hdfs haadmin -getServiceState ";
    cmd.append(haNamenode);
    if (runCommandAndFind(cmd, true, namenodetype)) {
      namenodeService = haNamenode;
      break;
    }
  }
  string rpcAddressName = "dfs.namenode.rpc-address.gphd-cluster.";
  rpcAddressName.append(namenodeService);
  string rpcAddressValue = hawqxmlconf->getString(rpcAddressName);
  auto namenodeInfo = hawq::test::split(rpcAddressValue, ':');
  namenode = hawq::test::trim(namenodeInfo[0]);
  port = std::stoi(hawq::test::trim(namenodeInfo[1]));
  return true;
}

void HdfsConfig::getNamenodes(std::vector<string> &namenodes,
                              std::vector<int> &port) {
  string result = "";
  runCommand("hdfs getconf -nnRpcAddresses", true, result);
  auto lines = hawq::test::split(result, '\n');
  for (size_t i = 0; i < lines.size(); i++) {
    string valueLine = lines[i];
    auto namenodeInfo = hawq::test::split(valueLine, ':');
    if (namenodeInfo.size() == 2) {
      namenodes.push_back(hawq::test::trim(namenodeInfo[0]));
      port.push_back(std::stoi(hawq::test::trim(namenodeInfo[1])));
    }
  }
}

void HdfsConfig::getDatanodelist(std::vector<string> &datanodelist,
                                 std::vector<int> &port) {
  runCommandAndGetNodesPorts("hdfs dfsadmin -report | grep Name", datanodelist, port);
}

void HdfsConfig::getActiveDatanodes(std::vector<string> &activedatanodes,
                                    std::vector<int> &port) {
  runCommandAndGetNodesPorts("hdfs dfsadmin -report -live | grep Name", activedatanodes, port);
}

int HdfsConfig::isSafemode() {
  if (runCommandAndFind("hadoop fs -mkdir /tmp_hawq_test", false, "Name node is in safe mode.")) {
    return 1;
  }
  string cmd = "hadoop fs -rm -r /tmp_hawq_test";
  Command c_teardown(cmd);
  string result = c_teardown.run().getResultOutput();
  return 0;
}

string HdfsConfig::getParameterValue(const string &parameterName) {
  bool ret = LoadFromHdfsConfigFile();
  if (!ret) {
    return "Error: failed to load from HDFS configuration file";
  }

  return hdfsxmlconf->getString(parameterName);
}

string HdfsConfig::getParameterValue(const string &parameterName,
                                     const string &conftype) {
  if (hawq::test::lower(conftype) == "hdfs") {
    return getParameterValue(parameterName);
  }
  bool ret = LoadFromHawqConfigFile();
  if (!ret) {
    return "Error: failed to load from HAWQ configuration file";
  }

  return hawqxmlconf->getString(parameterName);
}

bool HdfsConfig::setParameterValue(const string &parameterName,
                                   const string &parameterValue) { 
  bool ret = LoadFromHdfsConfigFile();
  if (!ret) {
    return false;
  }

  return hdfsxmlconf->setString(parameterName, parameterValue);
}

bool HdfsConfig::setParameterValue(const string &parameterName,
                                   const string &parameterValue,
                                   const string &conftype) {
  if (hawq::test::lower(conftype) == "hdfs") {
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
