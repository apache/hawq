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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/*
 * MockApplicationClientProtocol.h
 *
 *  Created on: Mar 5, 2015
 *      Author: weikui
 */

#ifndef MOCKAPPLICATIONCLIENTPROTOCOL_H_
#define MOCKAPPLICATIONCLIENTPROTOCOL_H_

#include "gmock/gmock.h"
#include "libyarnserver/ApplicationClientProtocol.h"

using namespace libyarn;
using std::string; using std::list;

namespace Mock{
class MockApplicationClientProtocol : public ApplicationClientProtocol {
public:
    MockApplicationClientProtocol(const string & user, const string & rmHost,
        const string & rmPort, const string & tokenService,
        const SessionConfig & c):ApplicationClientProtocol(user,rmHost,rmPort,tokenService, c){
    }
    ~MockApplicationClientProtocol(){
    }
    MOCK_METHOD1(getNewApplication, GetNewApplicationResponse(GetNewApplicationRequest &request));
    MOCK_METHOD1(submitApplication, void (SubmitApplicationRequest &request));
    MOCK_METHOD1(getApplicationReport, GetApplicationReportResponse (GetApplicationReportRequest &request));
    MOCK_METHOD1(getContainers, GetContainersResponse (GetContainersRequest &request));
    MOCK_METHOD1(getClusterNodes, GetClusterNodesResponse (GetClusterNodesRequest &request));
    MOCK_METHOD1(getQueueInfo, GetQueueInfoResponse (GetQueueInfoRequest &request));
    MOCK_METHOD1(forceKillApplication, KillApplicationResponse (KillApplicationRequest &request));
    MOCK_METHOD1(getClusterMetrics, GetClusterMetricsResponse (GetClusterMetricsRequest &request));
    MOCK_METHOD1(getApplications, GetApplicationsResponse (GetApplicationsRequest &request));
    MOCK_METHOD1(getQueueAclsInfo, GetQueueUserAclsInfoResponse (GetQueueUserAclsInfoRequest &request));
};
}

#endif /* MOCKAPPLICATIONCLIENTPROTOCOL_H_ */
