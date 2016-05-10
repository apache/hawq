#include "hawq-config.h"

#include <fstream>
#include <iostream>
#include <set>
#include <string>
#include <vector>

#include "command.h"
#include "psql.h"
#include "string-util.h"
#include "xml-parser.h"

bool HawqConfig::LoadFromConfigFile() {
  const char *env = getenv("GPHOME");
  std::string confPath = env ? env : "";
  if (!confPath.empty()) {
    confPath.append("/etc/hawq-site.xml");
  } else {
    return false;
  }

  xmlconf.reset(new XmlConfig(confPath.c_str()));
  xmlconf->parse();
  return true;
}

bool HawqConfig::getMaster(std::string &hostname, int &port) {
  bool ret = LoadFromConfigFile();
  if(!ret){
    return false;
  }
  hostname = xmlconf->getString("hawq_master_address_host");
  port = xmlconf->getInt32("hawq_master_address_port");
  return true;
}

void HawqConfig::getStandbyMaster(std::string &hostname, int &port) {
  PSQLQueryResult result = psql.getQueryResult(
      "select hostname, port from gp_segment_configuration where role ='s'");
  std::vector<std::vector<std::string> > table = result.getRows();
  if (table.size() > 0) {
    hostname = table[0][0];
    std::string portStr = table[0][1];
    port = std::stoi(portStr);
  }
}

void HawqConfig::getTotalSegments(std::vector<std::string> &hostname,
    std::vector<int> &port) {
  PSQLQueryResult result = psql.getQueryResult(
      "select hostname, port from gp_segment_configuration where role ='p'");
  std::vector<std::vector<std::string> > table = result.getRows();
  for (int i = 0; i < table.size(); i++) {
    hostname.push_back(table[i][0]);
    std::string portStr = table[i][1];
    port.push_back(std::stoi(portStr));
  }
}

void HawqConfig::getSlaves(std::vector<std::string> &hostname) {

  std::ifstream inFile;
  char* GPHOME = getenv("GPHOME");
  if (GPHOME == nullptr) {
    return;
  }
  std::string slaveFile(GPHOME);
  slaveFile.append("/etc/slaves");
  inFile.open(slaveFile.c_str());
  std::string line;
  while (std::getline(inFile, line)) {
    hostname.push_back(line);
  }
  inFile.close();
}

void HawqConfig::getUpSegments(std::vector<std::string> &hostname,
    std::vector<int> &port) {
  PSQLQueryResult result =
      psql.getQueryResult(
          "select hostname, port from gp_segment_configuration where role = 'p' and status = 'u'");
  std::vector<std::vector<std::string> > table = result.getRows();

  if (table.size() > 0) {
    hostname.push_back(table[0][0]);
    std::string portStr = table[0][1];
    port.push_back(std::stoi(portStr));
  }
}

void HawqConfig::getDownSegments(std::vector<std::string> &hostname,
    std::vector<int> &port) {
  PSQLQueryResult result =
      psql.getQueryResult(
          "select hostname, port from gp_segment_configuration where role = 'p' and status != 'u'");
  std::vector<std::vector<std::string> > table = result.getRows();

  if (table.size() > 0) {
    hostname.push_back(table[0][0]);
    std::string portStr = table[0][1];
    port.push_back(std::stoi(portStr));
  }
}

std::string HawqConfig::getGucValue(std::string gucName) {
  std::string cmd = "hawq config -s ";
  cmd.append(gucName);
  Command c(cmd);
  std::string result = c.run().getResultOutput();
  std::string gucValue = "";
  std::vector<std::string> lines = StringUtil::split(result, '\n');
  // second line is value output.
  if (lines.size() >= 2) {
    std::string valueLine = lines[1];
    int pos = valueLine.find_first_of(':');
    std::string value = valueLine.substr(pos + 1);
    gucValue = StringUtil::trim(value);
  }
  return gucValue;
}

std::string HawqConfig::setGucValue(std::string gucName, std::string gucValue) {
  std::string cmd = "hawq config -c ";
  cmd.append(gucName);
  cmd.append(" -v ");
  cmd.append(gucValue);
  Command c(cmd);
  std::string ret = c.run().getResultOutput();
  return ret;
}

bool HawqConfig::isMasterMirrorSynchronized() {
  PSQLQueryResult result = psql.getQueryResult(
      "select summary_state from gp_master_mirroring");
  if (result.getRows().size() > 0) {
    std::string syncInfo = result.getData(0, 0);
    syncInfo = StringUtil::trim(syncInfo);
    if (syncInfo == "Synchronized") {
      return true;
    } else {
      return false;
    }
  }
  return false;
}

bool HawqConfig::isMultinodeMode() {
  PSQLQueryResult result = psql.getQueryResult(
      "select hostname from gp_segment_configuration");
  std::vector<std::vector<std::string> > table = result.getRows();

  std::set<std::string> hostnameMap;
  for (int i = 0; i < table.size(); i++) {
    std::string hostname2 = table[i][0];
    if (hostname2 == "localhost") {
      char hostnamestr[256];
      gethostname(hostnamestr, 255);
      hostname2.assign(hostnamestr);
    }
    if (hostnameMap.find(hostname2) == hostnameMap.end()) {
      hostnameMap.insert(hostname2);
    }
  }
  if (hostnameMap.size() <= 1) {
    return false;
  } else {
    return true;
  }
}
