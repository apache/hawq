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
  const char *env = getenv("GPHOME");
  string confPath = env ? env : "";
  if (!confPath.empty()) {
    confPath.append("/etc/hdfs-client.xml");
  } else {
    return false;
  }

  hawqxmlconf.reset(new XmlConfig(confPath));
  hawqxmlconf->parse();
  return true;
}

bool HdfsConfig::LoadFromHdfsConfigFile() {
  string confPath=getHadoopHome();
  if (confPath == "")
    return false;
  confPath.append("/etc/hadoop/hdfs-site.xml");
  hdfsxmlconf.reset(new XmlConfig(confPath));
  hdfsxmlconf->parse();
  return true;
}

bool HdfsConfig::isHA() {
  bool ret = LoadFromHawqConfigFile();
  if (!ret) {
    return false;
  }
  string nameservice = hawqxmlconf->getString("dfs.nameservices");
  if (nameservice.length() > 0) {
    return true;
  } else {
    return false;
  }
}

bool HdfsConfig::isKerbos() {
  bool ret = LoadFromHawqConfigFile();
  if (!ret) {
    return false;
  }
  string authentication = hawqxmlconf->getString("hadoop.security.authentication");
  if (authentication == "kerberos") {
    return true;
  } else {
    return false;
  }
}

bool HdfsConfig::isTruncate() {
  string cmd = "hadoop fs -truncate";
  Command c(cmd);
  string result = c.run().getResultOutput();
  auto lines = hawq::test::split(result, '\n');
  if (lines.size() >= 1) {
    string valueLine = lines[0];
    int find = valueLine.find("-truncate: Unknown command");
    if (find < 0)
      return true;
  }
  return false;
}

string HdfsConfig::getHadoopHome() {
  string cmd = "ps -ef|grep hadoop";
  Command c(cmd);
  string result = c.run().getResultOutput();
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
  if (!isHA())
    return false;
  string namenodeService = "";
  string nameServiceValue = hawqxmlconf->getString("dfs.nameservices");
  string haNamenodesName = "dfs.ha.namenodes.";
  haNamenodesName.append(hawq::test::trim(nameServiceValue));
  string haNamenodesValue = hawqxmlconf->getString(haNamenodesName);
  auto haNamenodes = hawq::test::split(haNamenodesValue, ',');
  for (size_t i = 0; i < haNamenodes.size(); i++) {
    string haNamenode = hawq::test::trim(haNamenodes[i]);
    string cmd = "sudo -u ";
    cmd.append(getHdfsUser());
    cmd.append(" hdfs haadmin -getServiceState ");
    cmd.append(haNamenode);
    Command c(cmd);
    string result = c.run().getResultOutput();
    auto lines = hawq::test::split(result, '\n');
    if (lines.size() >= 1) {
      string valueLine = lines[0];
      if (valueLine == namenodetype) {
        namenodeService = haNamenode;
        break;
      }
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
  string cmd = "sudo -u ";
  cmd.append(getHdfsUser());
  cmd.append(" hdfs getconf -nnRpcAddresses");
  Command c(cmd);
  string result = c.run().getResultOutput();
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
  string cmd = "sudo -u ";
  cmd.append(getHdfsUser());
  cmd.append(" hdfs dfsadmin -report | grep Name");
  Command c(cmd);
  string result = c.run().getResultOutput();
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

void HdfsConfig::getActiveDatanodes(std::vector<string> &activedatanodes,
                                    std::vector<int> &port) {
  string cmd = "sudo -u ";
  cmd.append(getHdfsUser());
  cmd.append(" hdfs dfsadmin -report -live | grep Name");
  Command c(cmd);
  string result = c.run().getResultOutput();
  auto lines = hawq::test::split(result, '\n');
  for (size_t i = 0; i < lines.size(); i++) {
    string valueLine = lines[i];
    auto datanodeInfo = hawq::test::split(valueLine, ':');
    if (datanodeInfo.size() == 3) {
      int portStart = datanodeInfo[2].find_first_of('(');
      int portEnd = datanodeInfo[2].find_first_of(')');
      string datanodePort = datanodeInfo[2].substr(0, portStart);
      string datanodeHost = datanodeInfo[2].substr(portStart+1, portEnd-portStart-1);
      activedatanodes.push_back(hawq::test::trim(datanodeHost));
      port.push_back(std::stoi(hawq::test::trim(datanodePort)));
    }
  }
}


bool HdfsConfig::isSafemode() {
  string cmd = "hadoop fs -mkdir /tmp_hawq_test";
  Command c(cmd);
  string result = c.run().getResultOutput();
  auto lines = hawq::test::split(result, '\n');
  if (lines.size() >= 1) {
    string valueLine = lines[0];
    int find = valueLine.find("Name node is in safe mode.");
    if (find >= 0)
      return true;
  }
  cmd = "hadoop fs -rm -r /tmp_hawq_test";
  Command c_teardown(cmd);
  result = c_teardown.run().getResultOutput();
  return false;
}

string HdfsConfig::getParameterValue(const string &parameterName) {
  bool ret = LoadFromHdfsConfigFile();
  if (!ret) {
    return NULL;
  }

  return hdfsxmlconf->getString(parameterName);
}

string HdfsConfig::getParameterValue(const string &parameterName,
                                     const string &conftype) {
  if (conftype == "hdfs" || conftype == "HDFS")
    return getParameterValue(parameterName);
  bool ret = LoadFromHawqConfigFile();
  if (!ret) {
    return NULL;
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
  if (conftype == "hdfs" || conftype == "HDFS")
    return setParameterValue(parameterName, parameterValue);
  bool ret = LoadFromHawqConfigFile();
  if (!ret) {
    return false;
  }

  return hawqxmlconf->setString(parameterName, parameterValue);
}

} // namespace test
} // namespace hawq
