#ifndef SRC_TEST_FEATURE_LIB_HAWQ_CONFIG_H_
#define SRC_TEST_FEATURE_LIB_HAWQ_CONFIG_H_

#include "psql.h"
#include "xml-parser.h"

class HawqConfig {
  public:
    HawqConfig(const std::string& user = "gpadmin",
        const std::string& password = "", const std::string& db = "postgres",
        const std::string& host = "localhost", const std::string& port = "5432") :
        psql(db, host, port, user, password) {
      std::string masterHostname = "";
      int masterPort = 0;
      bool ret = getMaster(masterHostname, masterPort);
      if (ret) {
        std::string masterPortStr = std::to_string(masterPort);
        psql.setHost(masterHostname);
        psql.setPort(masterPortStr);
      }
    }
    ~HawqConfig() {
    }

    bool LoadFromConfigFile();
    bool getMaster(std::string &hostname, int &port);
    void getStandbyMaster(std::string &hostname, int &port);
    void getTotalSegments(std::vector<std::string> &hostname,
        std::vector<int> &port);
    void getSlaves(std::vector<std::string> &hostname);
    void getUpSegments(std::vector<std::string> &hostname,
        std::vector<int> &port);
    void getDownSegments(std::vector<std::string> &hostname,
        std::vector<int> &port);
    std::string getGucValue(std::string gucName);
    std::string setGucValue(std::string gucName, std::string gucValue);
    bool isMasterMirrorSynchronized();
    bool isMultinodeMode();
  private:
    std::unique_ptr<XmlConfig> xmlconf;
    PSQL psql;
};

#endif /* SRC_TEST_FEATURE_LIB_HAWQ_CONFIG_H_ */
