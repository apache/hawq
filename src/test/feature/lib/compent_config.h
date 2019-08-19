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

#ifndef HAWQ_SRC_TEST_FEATURE_LIB_COMPENT_CONFIG_H_
#define HAWQ_SRC_TEST_FEATURE_LIB_COMPENT_CONFIG_H_

#include <string>
#include <vector>

#include "sql_util.h"
#include "xml_parser.h"

enum CommandType {
    OS_COMMAND,
    HAWQ_COMMAND,
    HDFS_COMMAND,
    YARN_COMMAND,
    HAWQ_OS_COMMAND
};

namespace hawq {
namespace test {

#define MASTERPOS 0
#define STANDBYPOS 1
#define MAX_RETRY_NUM 10

/**
 *  CompentConfig common library.
 *  Get information of cloud cluster and run command to physical cluster and cloud cluster

 */
class CompentConfig {
  public:
    /**
     * CompentConfig constructor
     */
    CompentConfig();

    /**
     * CompentConfig destructor
     */
    virtual ~CompentConfig() {}

    //return hawq user
    std::string getHawqUser();

    //Run command on specific host
    //@param hostname: hostname the host which comand should run
    //@param command: the running command
    //@param user: user the specific user
    //@param result: the command output
    //@param cmdType: the command type

    bool runCommand(const std::string hostname,
                    const std::string &command,
                    const std::string user,
                    std::string &result,
                    CommandType cmdType = OS_COMMAND);

    //Run command on specific host
    //@param command: the running command
    //@param user: user the specific user
    //@param result: the command output
    //@param cmdType: the command type
    bool runCommand(const std::string &command,
                    const std::string user,
                    std::string &result,
                    CommandType cmdType = OS_COMMAND);

    //Run command on specific host and find the specific string
    //@param command: the running command
    //@param user: user the specific user
    //@param findstring: the find string
    //@param cmdtype: the command type
    bool runCommandAndFind(const std::string &command,
                           const std::string user,
                           const std::string &findstring,
                           CommandType cmdType = OS_COMMAND);

    //Run command on specific host and generate the host and port
    //@param command: the running command
    //@param user: user the specific user
    //@param datanodelist:
    //@param port: the command type
    void runCommandAndGetNodesPorts(const std::string &command,
                                    const std::string user,
                                    std::vector<std::string> &datanodelist,
                                    std::vector<int> &port,
                                    CommandType cmdType = OS_COMMAND);

    // Return kubnet cluster information. It is only valid when iscloud is true
    virtual bool getCluster() { return false;}

    bool isCloudCluster() { return iscloud; }

    bool checkRemainedProcess();
    bool killRemainedProcess(int32_t segmentNum = 0 );


  protected:
    // Get kubenet cluster information. It is only valid when iscloud is true
    bool __fetchKubeCluster();

    // Get kubenet cluster information. It is only valid when iscloud is true
    void __copyCluster(std::vector<std::string> & masterhosts, std::vector<std::string> &slavehost);

    //whether it is single node cluster just for demo
    bool isdemo;
  protected:
    std::vector<std::string> masterPhysicalHosts;
    std::vector<std::string> slavesPhysicalHosts;
    //inidicate whether it is cloud cluster or physical cluster
    bool iscloud;
    //indicate which active master
    bool activeMasterPos;
    std::string compentPath;
    std::unique_ptr<hawq::test::PSQL> conn;
};

} // namespace test
} // namespace hawq

#endif /* HAWQ_SRC_TEST_FEATURE_LIB_COMPENT_CONFIG_H_ */
