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

#include "hawq_config.h"
#include "command.h"
#include "psql.h"
#include "xml_parser.h"
#include "string_util.h"

using std::string;

namespace hawq {
namespace test {

bool HawqConfig::LoadFromConfigFile() {
  const char *env = getenv("GPHOME");
  string confPath = env ? env : "";
  if (!confPath.empty()) {
    confPath.append("/etc/hawq-site.xml");
  } else {
    return false;
  }

  xmlconf.reset(new XmlConfig(confPath));
  xmlconf->parse();
  return true;
}

bool HawqConfig::getMaster(string &hostname, int &port) {
  bool ret = LoadFromConfigFile();
  if (!ret) {
    return false;
  }
  hostname = xmlconf->getString("hawq_master_address_host");
  port = xmlconf->getInt32("hawq_master_address_port");
  return true;
}

void HawqConfig::getStandbyMaster(string &hostname, int &port) {
  const hawq::test::PSQLQueryResult &result = psql.getQueryResult(
      "select hostname, port from gp_segment_configuration where role ='s'");
  std::vector<std::vector<string> > table = result.getRows();
  if (table.size() > 0) {
    hostname = table[0][0];
    port = std::stoi(table[0][1]);
  }
}

void HawqConfig::getTotalSegments(std::vector<string> &hostname,
                                  std::vector<int> &port) {
  hostname.resize(0); port.resize(0);
  const hawq::test::PSQLQueryResult &result = psql.getQueryResult(
      "select hostname, port from gp_segment_configuration where role ='p'");
  std::vector<std::vector<string> > table = result.getRows();
  for (size_t i = 0; i < table.size(); ++i) {
    hostname.push_back(table[i][0]);
    port.push_back(std::stoi(table[i][1]));
  }
}

void HawqConfig::getSlaves(std::vector<string> &hostname) {
  hostname.resize(0);
  std::ifstream inFile;
  char *GPHOME = getenv("GPHOME");
  if (GPHOME == nullptr) {
    return;
  }
  string slaveFile(GPHOME);
  slaveFile.append("/etc/slaves");
  inFile.open(slaveFile.c_str());
  string line;
  while (std::getline(inFile, line)) {
    hostname.push_back(line);
  }
  inFile.close();
}

void HawqConfig::getUpSegments(std::vector<string> &hostname,
                               std::vector<int> &port) {
  hostname.resize(0); port.resize(0);
  const hawq::test::PSQLQueryResult &result = psql.getQueryResult(
      "select hostname, port from gp_segment_configuration where role = 'p' "
      "and status = 'u'");
  std::vector<std::vector<string> > table = result.getRows();
  for (size_t i = 0; i < table.size(); ++i) {
    hostname.push_back(table[i][0]);
    port.push_back(std::stoi(table[i][1]));
  }
}

void HawqConfig::getDownSegments(std::vector<string> &hostname,
                                 std::vector<int> &port) {
  const hawq::test::PSQLQueryResult &result = psql.getQueryResult(
      "select hostname, port from gp_segment_configuration where role = 'p' "
      "and status != 'u'");
  std::vector<std::vector<string> > table = result.getRows();

  for (size_t i = 0; i < table.size(); ++i) {
    hostname.push_back(table[i][0]);
    port.push_back(std::stoi(table[i][1]));
  }
}

string HawqConfig::getGucValue(const string & gucName) {
  string cmd = "hawq config -s ";
  cmd.append(gucName);
  Command c(cmd);
  string result = c.run().getResultOutput();
  string gucValue = "";
  auto lines = hawq::test::split(result, '\n');
  // second line is value output.
  if (lines.size() >= 2) {
    string valueLine = lines[1];
    int pos = valueLine.find_first_of(':');
    string value = valueLine.substr(pos + 1);
    gucValue = hawq::test::trim(value);
  }
  return gucValue;
}

string HawqConfig::setGucValue(const string &gucName,
                               const string &gucValue) {
  string cmd = "hawq config -c ";
  cmd.append(gucName);
  cmd.append(" -v ");
  cmd.append(gucValue);
  Command c(cmd);
  string ret = c.run().getResultOutput();
  return ret;
}

bool HawqConfig::isMasterMirrorSynchronized() {
  const hawq::test::PSQLQueryResult &result =
      psql.getQueryResult("select summary_state from gp_master_mirroring");
  if (result.getRows().size() > 0) {
    string syncInfo = result.getData(0, 0);
    syncInfo = hawq::test::trim(syncInfo);
    if (syncInfo == "Synchronized") {
      return true;
    } else {
      return false;
    }
  }
  return false;
}

bool HawqConfig::isMultinodeMode() {
  const hawq::test::PSQLQueryResult &result =
      psql.getQueryResult("select hostname from gp_segment_configuration");
  std::vector<std::vector<string> > table = result.getRows();
  std::unordered_set<string> hostnameMap;
  for (unsigned int i = 0; i < table.size(); i++) {
    string hostname2 = table[i][0];
    if (hostname2 == "localhost") {
      char hostnamestr[256];
      gethostname(hostnamestr, 255);
      hostname2.assign(hostnamestr);
    }
    hostnameMap.insert(hostname2);
  }
  if (hostnameMap.size() <= 1) {
    return false;
  }
  return true;
}

} // namespace test
} // namespace hawq
