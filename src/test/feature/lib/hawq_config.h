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

#ifndef HAWQ_SRC_TEST_FEATURE_LIB_HAWQ_CONFIG_H_
#define HAWQ_SRC_TEST_FEATURE_LIB_HAWQ_CONFIG_H_

#include <string>
#include <vector>

#include "psql.h"
#include "sql_util.h"
#include "xml_parser.h"

namespace hawq {
namespace test {

/**
 *  HawqConfig common library. Get detailed infomation about HAWQ cluster
 *  including checking state of master and slaves, setting GUC and checking standby.
 *  @author hubert zhang
 */
class HawqConfig {
  public:
    /**
     * HawqConfig constructor
     */
    HawqConfig(): psql(HAWQ_DB, HAWQ_HOST, HAWQ_PORT, HAWQ_USER, HAWQ_PASSWORD) {}

    /**
     * HawqConfig destructor
     */
    ~HawqConfig() {}

    /**
     * get hawq master's hostname and port information
     * @param hostname master hostname reference which will be set
     * @param port master port number reference which will be set
     * @return true if getMaster succeeded
     */
    bool getMaster(std::string &hostname, int &port);


    /**
     * get hawq standby master's hostname and port information
     * @param hostname master hostname reference which will be set
     * @param port master port number reference which will be set
     * @return true if hawq getStandbyMaster succeeded
     */
    void getStandbyMaster(std::string &hostname, int &port);


    /**
     * get the list of all of the hawq segments
     * @param hostname hostname list of all of the hawq segments
     * @param port port list of all of the hawq segments
     */
    void getTotalSegments(std::vector<std::string> &hostname,
                          std::vector<int> &port);

    /**
     * get list of hostnames in slave file
     * @param hostname list of hostnames in slave file.
     */
    void getSlaves(std::vector<std::string> &hostname);

    /**
     * get hawq segment list whose state is up(not down).
     * @param hostname hostname list of all of the up segments
     * @param port port list of all of the up segments
     * @see getDownSegments
     */
    void getUpSegments(std::vector<std::string> &hostname,
                       std::vector<int> &port);

    /**
     * get hawq segment list whose state is down.
     * @param hostname hostname list of all of the down segments
     * @param port port list of all of the down segments
     */
    void getDownSegments(std::vector<std::string> &hostname,
                         std::vector<int> &port);

    /**
     * get the guc value by name
     * @param gucName the guc name to be retrived
     * @return guc value
     */
    std::string getGucValue(const std::string & gucName);

    /**
     * set guc value by hawq config
     * @param gucName guc name to be set
     * @param gucValue the value to be set
     * @return hawq config return information
     */
    std::string setGucValue(const std::string & gucName,
                            const std::string & gucValue);

    /**
     * whether hawq master and mirror is synchronized by checking gp_master_mirroring.
     * @return true if Synchronized
     */
    bool isMasterMirrorSynchronized();

    /**
     * whether hawq is in multi-node mode.
     * @return true if hawq is multi-node
     */
    bool isMultinodeMode();

  private:
    /**
     * load key-value parameters in hawq-site.xml
     * @return true if succeeded
     */
    bool LoadFromConfigFile();

  private:
    std::unique_ptr<XmlConfig> xmlconf;
    hawq::test::PSQL psql;
};

} // namespace test
} // namespace hawq

#endif /* SRC_TEST_FEATURE_LIB_HAWQ_CONFIG_H_ */
