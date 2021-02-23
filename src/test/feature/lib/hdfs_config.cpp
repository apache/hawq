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

#include <fstream>
#include <string>
#include <vector>
#include <unordered_set>
#include <pwd.h>

#include "hdfs_config.h"
#include "command.h"
#include "xml_parser.h"
#include "string_util.h"

using std::string;

namespace hawq {
namespace test {

HdfsConfig::HdfsConfig() {
    isLoadFromHawqConfigFile = false;
    isLoadFromHdfsConfigFile = false;
    isHACluster = this->__isHA();
    this->getCluster();
}

bool HdfsConfig::__isHA() {
    const hawq::test::PSQLQueryResult &result = conn->getQueryResult(
       "SELECT substring(fselocation from length('hdfs:// ') for (position('/' in substring(fselocation from length('hdfs:// ')))-1)::int) "
       "FROM pg_filespace pgfs, pg_filespace_entry pgfse "
       "WHERE pgfs.fsname = 'dfs_system' AND pgfse.fsefsoid=pgfs.oid ;");
    std::vector<std::vector<string>> table = result.getRows();
    if (table.size() > 0) {
        int find = table[0][0].find(":");
        if (find < 0) {
            return true;
        } else {
            return false;
        }
    }
    return false;
}

// Return kubenet cluster information. It is only valid when iscloud is true
bool HdfsConfig::getCluster()
{
    bool status;
    if (masterPhysicalHosts.size() == 0 )
    {
        if (iscloud)
        {
            status = this->__fetchKubeCluster();
            if (!status)
                 return false;
            this->getHadoopHome();
            hdfsuser = this->getHdfsUser();
            if (hdfsuser.size() == 0)
                return false;
        }
        else
        {
            if (isHACluster)
            {
                string hostname = HAWQ_HOST;
                masterPhysicalHosts.push_back(hostname);
                this->getHadoopHome();
                hdfsuser = this->getHdfsUser();
                std::vector<string> masterhosts;
                std::vector<int>  namenodePort;
                this->getNamenodes(masterhosts, namenodePort);
                masterPhysicalHosts.clear();
                for (uint16_t i =0; i < masterhosts.size(); i++)
                    masterPhysicalHosts.push_back(masterhosts[i]);
            }
            else
            {
                 string nameportstring;
                 this->getNamenodeHost(nameportstring);
                 int pos = nameportstring.find(":");
                 masterPhysicalHosts.push_back(nameportstring.substr(0, pos));
                 this->getHadoopHome();
                 hdfsuser = this->getHdfsUser();
             }
             std::vector<int> datanodePort;
             this->getDatanodelist(slavesPhysicalHosts, datanodePort);
        }
    }
    return true;
}


string HdfsConfig::getHdfsUser() {
    if (this->hdfsuser.size() == 0 )
    {
        string command = "ps aux|grep hdfs.server|grep -v grep";
        string result;
        // NOTE: It should be HDFS_COMMAND since we need to login the hdfs container to get user
        bool status = this->runCommand(masterPhysicalHosts[MASTERPOS], command,
                      "", result, HDFS_COMMAND);
        if (!status)
            return "";
        auto lines = hawq::test::split(result, '\n');
        if (lines.size() >= 1) {
            return hawq::test::trim(hawq::test::split(lines[lines.size()-1], ' ')[0]);
        }
    }
    return hdfsuser;
}

bool HdfsConfig::LoadFromHawqConfigFile() {
    if (!isLoadFromHawqConfigFile)
    {
        string confPath = iscloud ? getenv("CLOUD_CLUSTER_ENV") : getenv("GPHOME") ;
        if (confPath.empty() || isdemo)
            return false;
        if (iscloud)
        {
            confPath.append("/hawq/hdfs-client.xml");
             }
        else
        {
            confPath.append("/etc/hdfs-client.xml");
        }

        hawqxmlconf.reset(new XmlConfig(confPath));
        if (!hawqxmlconf->parse())
            return false;
        isLoadFromHawqConfigFile = true;
    }
    return true;
}

bool HdfsConfig::LoadFromHdfsConfigFile() {
    if (!isLoadFromHdfsConfigFile)
    {
        string confPath = iscloud ? getenv("CLOUD_CLUSTER_ENV") : this->getHadoopHome() ;
        if (confPath.empty() || isdemo)
            return false;
        if (iscloud)
        {
            confPath.append("/hadoop/demo/hdfs-site.xml");
        }
        else
        {
            confPath.append("/etc/hadoop/hdfs-site.xml");
        }

         hdfsxmlconf.reset(new XmlConfig(confPath));
         if (!hdfsxmlconf->parse())
            return false;

         isLoadFromHdfsConfigFile = true;
    }
    return true;
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
  if (this->runCommandAndFind("hadoop fs -truncate",
          hdfsuser, "-truncate: Unknown command", HDFS_COMMAND)) {
    return 0;
  } else {
    return 1;
  }
}

string HdfsConfig::getHadoopHome() {
    if (this->compentPath.size () == 0)
    {
        string result = "";
        bool status = this->runCommand(masterPhysicalHosts[MASTERPOS], "ps -ef|grep hadoop",
                "", result, OS_COMMAND );
        if (!status)
                return "";
         auto lines = hawq::test::split(result, '\n');
         for (size_t i=0; i<lines.size()-1; i++) {
                string valueLine = lines[i];
                string findstring = "-Dhadoop.home.dir=";
                int pos = valueLine.find(findstring);
                if (pos >=0 ) {
                   string valueTmp = valueLine.substr(pos+findstring.size());
                   int valueEnd = valueTmp.find_first_of(" ");
                   string value = valueTmp.substr(0, valueEnd);
                   compentPath = hawq::test::trim(value);
             }
         }
    }
    return this->compentPath;
}

bool HdfsConfig::getNamenodeHost(string &namenodehost) {
  const hawq::test::PSQLQueryResult &result = conn->getQueryResult(
       "SELECT substring(fselocation from length('hdfs:// ') for (position('/' in substring(fselocation from length('hdfs:// ')))-1)::int) "
       "FROM pg_filespace pgfs, pg_filespace_entry pgfse "
       "WHERE pgfs.fsname = 'dfs_system' AND pgfse.fsefsoid=pgfs.oid ;");
  std::vector<std::vector<string>> table = result.getRows();
  if (table.size() > 0) {
    namenodehost = table[0][0];
    return true;
  }

  return false;
}

bool HdfsConfig::getActiveNamenode(string &activenamenode,
                                   int &port) {
    return getHANamenode("active", activenamenode, port);
}

bool HdfsConfig::getStandbyNamenode(string &standbynamenode,
                                    int &port) {
    return getHANamenode("standby", standbynamenode, port);
}

bool HdfsConfig::checkNamenodesHealth() {
  if (isHA() <= 0) {
    return false;
  }
  string namenodeService = "";
  string nameServiceValue = getParameterValue("dfs.nameservices");
  string haNamenodesName = "dfs.ha.namenodes.";
  haNamenodesName.append(hawq::test::trim(nameServiceValue));
  string haNamenodesValue = getParameterValue(haNamenodesName);
  auto haNamenodes = hawq::test::split(haNamenodesValue, ',');
  for (size_t i = 0; i < haNamenodes.size(); i++) {
    string haNamenode = hawq::test::trim(haNamenodes[i]);
    string cmd = "hdfs haadmin -checkHealth ";
    cmd.append(haNamenode);
    string checkResult;
    bool status = this->runCommand(cmd,
            hdfsuser, checkResult, HDFS_COMMAND );
    if (!status)
        return false;
    if (checkResult.size() > 0)
       return false;
  }

  return true;
}

bool HdfsConfig::getHANamenode(const string &namenodetype,
                               string &namenode,
                               int &port) {
  if (isHA() <= 0) {
    return false;
  }
  string namenodeService = "";
  string nameServiceValue = getParameterValue("dfs.nameservices");
  string haNamenodesName = "dfs.ha.namenodes.";
  haNamenodesName.append(hawq::test::trim(nameServiceValue));
  string haNamenodesValue = getParameterValue(haNamenodesName);
  auto haNamenodes = hawq::test::split(haNamenodesValue, ',');
  size_t i;
  string result;
  for ( i = 0; i < haNamenodes.size(); i++) {
    string haNamenode = hawq::test::trim(haNamenodes[i]);
    string cmd = "hdfs haadmin -getServiceState ";
    cmd.append(haNamenode);
    bool status = this->runCommandAndFind(cmd, hdfsuser, namenodetype, HDFS_COMMAND );
    if (status) {
      namenodeService = haNamenode;
      break;
    }
  }

  if (i == haNamenodes.size())
      return false;

  string rpcAddressName = "dfs.namenode.rpc-address.";
  rpcAddressName.append(nameServiceValue).append(".").append(namenodeService);
  string rpcAddressValue = getParameterValue(rpcAddressName);
  auto namenodeInfo = hawq::test::split(rpcAddressValue, ':');
  namenode = hawq::test::trim(namenodeInfo[0]);
  port = std::stoi(hawq::test::trim(namenodeInfo[1]));
  return true;
}

void HdfsConfig::getNamenodes(std::vector<string> &namenodes,
                              std::vector<int> &port)
{
    namenodes.clear();
    port.clear();
    string result = "";
    bool status = this->runCommand("hdfs getconf -nnRpcAddresses",
          hdfsuser, result, HDFS_COMMAND );
    if (!status)
        return;
    auto lines = hawq::test::split(result, '\n');
    for (size_t i = 0; i < lines.size(); i++) {
        string valueLine = lines[i];
        if (valueLine.find("ssh:") != string::npos ||
            valueLine.find("Warning:") != string::npos)
          continue;
        auto namenodeInfo = hawq::test::split(valueLine, ':');
        if (namenodeInfo.size() == 2) {
            namenodes.push_back(hawq::test::trim(namenodeInfo[0]));
            port.push_back(std::stoi(hawq::test::trim(namenodeInfo[1])));
        }
  }
}

void HdfsConfig::getDatanodelist(std::vector<string> &datanodelist,
                                 std::vector<int> &port) {
  this->runCommandAndGetNodesPorts("hdfs dfsadmin -report | grep Name",
                                   hdfsuser, datanodelist, port,
                                   HDFS_COMMAND);
}

void HdfsConfig::getActiveDatanodes(std::vector<string> &activedatanodes,
                                    std::vector<int> &port) {
     this->runCommandAndGetNodesPorts("hdfs dfsadmin -report -live | grep Name",
                                      hdfsuser, activedatanodes, port,
                                      HDFS_COMMAND);
}


int HdfsConfig::getActiveDatanodesNum() {
    string resultnum;
    bool status = this->runCommand(
            "hdfs dfsadmin -report -live | grep Name | wc -l",
            hdfsuser, resultnum, HDFS_COMMAND );
    if (!status)
        return -1;
    auto lines = hawq::test::split(resultnum, '\n');
    for (size_t i = 0; i < lines.size(); i++) {

        int pos = lines[i].find(" WARN ");
        if (pos > 0)
            continue;
        else
            return std::atoi(lines[i].c_str());
    }
    return -1;

}

int HdfsConfig::isSafemode() {
    bool status = this->runCommandAndFind(
            "hadoop fs -mkdir /tmp_hawq_test",
            hdfsuser, "Name node is in safe mode.", HDFS_COMMAND );
    if (status)
        return 1;
    string cmd = "hadoop fs -rm -r /tmp_hawq_test";
    string result;
    status = this->runCommand(cmd, hdfsuser, result, HDFS_COMMAND );
    if (status)
        return 0;
    return -1;
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
