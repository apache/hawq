#ifndef HAWQ_SRC_TEST_FEATURE_LIB_YARN_CONFIG_H_
#define HAWQ_SRC_TEST_FEATURE_LIB_YARN_CONFIG_H_

#include <string>
#include <vector>

#include "psql.h"
#include "sql_util.h"
#include "xml_parser.h"

namespace hawq {
namespace test {

/**
 * YarnConfig common libray. Get detailed information about YARN
 * including checking state of resourcemanagers and nodemanagers, get parameter value
 * @author Chunling Wang
 */
class YarnConfig {
  public:
    /**
      * YarnConfig constructor
      */
    YarnConfig(): psql(HAWQ_DB, HAWQ_HOST, HAWQ_PORT, HAWQ_USER, HAWQ_PASSWORD) {
      isLoadFromHawqConfigFile = false;
      isLoadFromYarnConfigFile = false;
    }

    /**
      * YarnConfig destructor
      */
    ~YarnConfig()  {}

    /**
     * whether YARN is configured
     * @return 1 if YARN is configured; 0 and following functions should not be called; -1 if failed to load from YARN configuration file
     */
    int isConfigYarn();

    /**
     * whether YARN is in HA mode
     * @return 1 if YARN is HA, 0 if YARN is not HA, -1 if there is an error
     */
    int isHA();

    /**
     * whether YARN is kerberos
     * @return 1 if YARN is kerberos, 0 if YARN is not kerberos, -1 if failed to load from HAWQ configuration file 
     */
    int isConfigKerberos();

    /**
     * get HADOOP working directory
     * @return HADOOP working directory
     */
    std::string getHadoopHome();

    /**
     * get YARN active resourcemanager's hostname and port information
     * @param activeresourcemanager, active resourcemanager hostname reference which will be set
     * @param port, active resourcemanager port reference which will be set
     * @return true if getActiveRM succeeded
     */
    bool getActiveRM(std::string &activeRM, int &port);

    /**
     * get YARN standby resourcemanager's hostname and port information
     * @param standbyRM, standby resourcemanager hostname reference which will be set
     * @param port, standby resourcemanager port reference which will be set
     * @return true if getStandbyRM succeeded
     */
    bool getStandbyRM(std::string &standbyRM, int &port);

    /**
     * get YARN resourcemanager(s) information
     * @param RMList, resourcemanagers' hostnames reference which will be set
     * @param port, resourcemanagers' ports reference which will be set
     */
    bool getRMList(std::vector<std::string> &RMList, std::vector<int> &port);

    /**
     * get YARN nodemanagers information
     * @param nodemanagers, nodemanagers' hostnames reference which will be set
     * @param port, nodemanagers' ports reference which will be set
     */
    void getNodeManagers(std::vector<std::string> &nodemanagers, std::vector<int> &port);

    /**
     * get YARN active nodemanagers information
     * @param nodemanagers, active nodemanagers' hostnames reference which will be set
     * @param port, active nodemanagers' ports reference which will be set
     */
    void getActiveNodeManagers(std::vector<std::string> &nodemanagers, std::vector<int> &port);

    /**
     * get parameter value in ./etc/yarn-client.xml or ./etc/hadoop/yarn-site.xml according to parameter name
     * @param parameterName, used to get parameter value
     * @param conftype, get parameter value, 'yarn' or 'YARN' from ./etc/yarn-client.xml, others from ./etc/hadoop/yarn-site.xml
     * @return parameter value
     */
    std::string getParameterValue(const std::string &parameterName);

    /**
     * get parameter value in ./etc/hadoop/yarn-site.xml according to parameter name
     * @param parameterName, used to get parameter value
     * @return parameter value
     */
    std::string getParameterValue(const std::string &parameterName, const std::string &conftype);

    /**
     * set parameter value in ./etc/hdfs-client.xml or ./etc/hadoop/hdfs-site.xml according to parameter name
     * @param parameterName, parameter name which used to set parameter value
     * @param parameterValue, parameter value which to be set
     * @param conftype, get parameter value, 'yarn' or 'YARN' from ./etc/yarn-client.xml, others from ./etc/hadoop/yarn-site.xml
     * @return true if succeeded
     */
    bool setParameterValue(const std::string &parameterName, const std::string &parameterValue);

    /**
     * set parameter value in ./etc/hadoop/hdfs-site.xml according to parameter name
     * @param parameterName, parameter name which used to set parameter value
     * @param parameterValue, parameter value which to be set
     * @return true if succeeded
     */
    bool setParameterValue(const std::string &parameterName, const std::string &parameterValue, const std::string &conftype);

  private:
    void runCommand(const std::string &command, bool ishdfsuser, std::string &result);
    
    bool runCommandAndFind(const std::string &command, bool ishdfsuser, const std::string &findstring);
    
    void runCommandAndGetNodesPorts(const std::string &command, bool isyarnuser, std::vector<std::string> &nodemanagers, std::vector<int> &port);
    
    /**
     * @return yarn user
     */
    std::string getYarnUser();
    
    /**
     * load key-value parameters in ./etc/yarn-client.xml
     * @return true if succeeded
     */
    bool LoadFromHawqConfigFile();

    /**
     * load key-value parameters in ./etc/hadoop/yarn-site.xml
     * @return true if succeeded
     */
    bool LoadFromYarnConfigFile();

    /**
     * get Yarn active or standby resourcemanager information in HA mode according to the RMtype
     * @param RMtype, used to specify active or standby resourcemanager information
     * @param RM, resourcemanager hostname reference which will be set
     * @param port, resourcemanager port reference which will be set
     * @return true if getHARM succeeded
     */
    bool getHARM(const std::string &RMtype, std::string &RMnode, int &port);

  private:
    std::unique_ptr<XmlConfig> hawqxmlconf;
    std::unique_ptr<XmlConfig> yarnxmlconf;
    bool isLoadFromHawqConfigFile;
    bool isLoadFromYarnConfigFile;
    hawq::test::PSQL psql;
};

} // namespace test
} // namespace hawq

#endif /* HAWQ_SRC_TEST_FEATURE_LIB_YARN_CONFIG_H_ */
