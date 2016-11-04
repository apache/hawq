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

#ifndef LIBYARNCLIENT_H_
#define LIBYARNCLIENT_H_

#include <map>
#include <list>
#include <set>
#include <exception>
#include <pthread.h>

#include "LibYarnConstants.h"
#include "records/ResourceRequest.h"
#include "records/Container.h"
#include "records/FinalApplicationStatus.h"
#include "records/NodeReport.h"
#include "records/ContainerReport.h"
#include "records/ContainerStatus.h"
#include "records/NodeState.h"
#include "records/QueueInfo.h"
#include "records/FunctionResult.h"

#ifdef MOCKTEST
#include "TestLibYarnClientStub.h"
#endif

using std::string;
using std::list;
using std::map;
using std::set;

namespace libyarn {
	class LibYarnClient {
	public:
		LibYarnClient(string &user, string &rmHost, string &rmPort, string &schedHost,
				string &schedPort, string &amHost, int32_t amPort,
				string &am_tracking_url, int heartbeatInterval);

#ifdef MOCKTEST
		LibYarnClient(string &user, string &rmHost, string &rmPort, string &schedHost,
				string &schedPort, string &amHost, int32_t amPort,
				string &am_tracking_url, int heartbeatInterval,Mock::TestLibYarnClientStub *stub);
#endif

		virtual ~LibYarnClient();

		virtual int createJob(string &jobName, string &queue, string &jobId);

		virtual int forceKillJob(string &jobId);

		virtual void addResourceRequest(Resource capability, int32_t num_containers,
				   string host, int32_t priority, bool relax_locality);

		virtual int addContainerRequests(string &jobId,
										Resource &capability, int32_t num_containers,
										list<struct LibYarnNodeInfo> &preferred,
										int32_t priority, bool relax_locality);

		virtual int allocateResources(string &jobId,
									  list<string> &blackListAdditions, list<string> &blackListRemovals,
									  list<Container> &allocatedContainers, int32_t num_containers);

		virtual int activeResources(string &jobId, int64_t activeContainerIds[],
				int activeContainerSize);

		virtual int releaseResources(string &jobId, int64_t releaseContainerIds[],
				int releaseContainerSize);

		virtual int finishJob(string &jobId, FinalApplicationStatus finalStatus);

		virtual int getApplicationReport(string &jobId, ApplicationReport &report);

		virtual int getContainerReports(string &jobId,
				list<ContainerReport> &containerReports);

		virtual int getContainerStatuses(string &jobId, int64_t containerIds[],
				int containerSize, list<ContainerStatus> &containerStatues);

		virtual int getQueueInfo(string &queue, bool includeApps, bool includeChildQueues,
				bool recursive, QueueInfo &queueInfo);

		virtual int getClusterNodes(list<NodeState> &states, list<NodeReport> &nodeReports);

		virtual int getActiveFailContainerIds(set<int64_t> &activeFailIds);

		friend void* heartbeatFunc(void* args);

		void setErrorMessage(string errorMsg);

		string getErrorMessage();

		bool isJobHealthy();

		list<ResourceRequest>& getAskRequests();

		void clearAskRequests();

	private:
		void dummyAllocate();
	private:
		string errorMessage;

		//ApplicationClient
		void *appClient;
		//ApplicationMaster
		void *amrmClient;
		//ContainerManagement
		void *nmClient;

		ApplicationId clientAppId;
		ApplicationAttemptId clientAppAttempId;

		// the user of running AM, default is postgres
		string amUser;
		string schedHost;
		string schedPort;

		string amHost;
		int32_t amPort;
		string am_tracking_url;
		//unit: ms
		int heartbeatInterval;
		pthread_t heartbeatThread;
		pthread_mutex_t heartbeatLock;

		int32_t response_id;
		string clientJobId;

		map<int64_t, Container*> jobIdContainers;
		map<string, Token> nmTokenCache;
		set<int64_t> activeFailContainerIds;
		list<ResourceRequest> askRequests;

		volatile bool keepRun;
		bool needHeartbeatAlive;
#ifdef MOCKTEST
	private:
    /*
     * for test
     */
	Mock::TestLibYarnClientStub* libyarnStub;
#endif
	};

	class LibYarnNodeInfo {
		public:
			LibYarnNodeInfo(const string &host, const string &rack, int32_t cnt)
							:hostname(host), num_containers(cnt)
			{ if(rack == "") rackname = DEFAULT_RACK; else rackname = rack;}

			string getHost() { return hostname; }
			string getRack() { return rackname; }
			int32_t getContainerNum() { return num_containers; }
			void setContainerNum(int32_t num) { num_containers = num; }

		protected:
			string  hostname;
			string  rackname;
			int32_t num_containers;
	};
}
#endif /* LIBYARNCLIENT_H_ */
