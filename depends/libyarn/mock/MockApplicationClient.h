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
 * MockApplicationClient.h
 *
 *  Created on: Mar 11, 2015
 *      Author: weikui
 */

#ifndef MOCKAPPLICATIONCLIENT_H_
#define MOCKAPPLICATIONCLIENT_H_

#include "gmock/gmock.h"
#include "libyarnclient/ApplicationClient.h"

using namespace libyarn;
using namespace std;

namespace Mock{
class MockApplicationClient : public ApplicationClient {
public:
	MockApplicationClient(string &host, string &port):ApplicationClient(host,port){
	}
	~MockApplicationClient(){
	}
	MOCK_METHOD0(getNewApplication, ApplicationID ());
	MOCK_METHOD1(submitApplication, void (ApplicationSubmissionContext &appContext));
	MOCK_METHOD1(getApplicationReport, ApplicationReport (ApplicationID &appId));
	MOCK_METHOD1(getContainers, list<ContainerReport> (ApplicationAttemptId &appAttempId));

	MOCK_METHOD1(getClusterNodes, list<NodeReport> (list<NodeState> &state));
	MOCK_METHOD4(getQueueInfo, QueueInfo (string &queue, bool includeApps,bool includeChildQueues, bool recursive));
	MOCK_METHOD1(forceKillApplication, void (ApplicationID &appId));
	MOCK_METHOD0(getClusterMetrics, YarnClusterMetrics ());
	MOCK_METHOD2(getApplications, list<ApplicationReport> (list<string> &applicationTypes,list<YarnApplicationState> &applicationStates));
	MOCK_METHOD0(getQueueAclsInfo, list<QueueUserACLInfo> ());
};
}

#endif /* MOCKAPPLICATIONCLIENT_H_ */
