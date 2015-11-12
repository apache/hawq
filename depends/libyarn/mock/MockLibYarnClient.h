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
 * MockLibYarnClient.h
 *
 *  Created on: Mar 3, 2015
 *      Author: weikui
 */

#ifndef MOCKLIBYARNCLIENT_H_
#define MOCKLIBYARNCLIENT_H_
#include "gmock/gmock.h"
#include "libyarnclient/LibYarnClient.h"

using namespace std;
using namespace libyarn;

namespace Mock {
class MockLibYarnClient: public LibYarnClient {
public:
	MockLibYarnClient(string &rmHost, string &rmPort, string &schedHost,
				string &schedPort, string &amHost, int32_t amPort,
				string &am_tracking_url, int heartbeatInterval):
				LibYarnClient(rmHost,rmPort,schedHost,schedPort,amHost,amPort,am_tracking_url,heartbeatInterval){
	}
	MOCK_METHOD3(createJob, int (string &jobName, string &queue, string &jobId));
	MOCK_METHOD6(allocateResources, int (string &jobId, ResourceRequest &resRequest,
			list<string> &blackListAdditions, list<string> &blackListRemovals,
			list<Container> &allocatedContainers, int retryLimit));
	MOCK_METHOD3(activeResources, int (string &jobId, int releaseContainerIds[],int releaseContainerSize));
	MOCK_METHOD3(releaseResources, int (string &jobId, int releaseContainerIds[],int releaseContainerSize));
	MOCK_METHOD2(finishJob, int (string &jobId, FinalApplicationStatus finalStatus));
	MOCK_METHOD2(getApplicationReport, int (string &jobId, ApplicationReport &report));
	MOCK_METHOD2(getContainerReports, int (string &jobId,list<ContainerReport> &containerReports));
	MOCK_METHOD4(getContainerStatuses, int (string &jobId, int32_t containerIds[],
			int containerSize, list<ContainerStatus> &containerStatues));
	MOCK_METHOD5(getQueueInfo, int (string &queue, bool includeApps, bool includeChildQueues,
			bool recursive, QueueInfo &queueInfo));
	MOCK_METHOD2(getClusterNodes, int (list<NodeState> &states, list<NodeReport> &nodeReports));
	MOCK_METHOD1(getActiveFailContainerIds, int (set<int> &activeFailIds));

};
}

#endif /* MOCKLIBYARNCLIENT_H_ */
