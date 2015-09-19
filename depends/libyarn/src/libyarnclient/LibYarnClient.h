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

namespace libyarn {
	class LibYarnClient {
	public:
		LibYarnClient(string &rmHost, string &rmPort, string &schedHost,
				string &schedPort, string &amHost, int32_t amPort,
				string &am_tracking_url, int heartbeatInterval);

#ifdef MOCKTEST
		LibYarnClient(string &rmHost, string &rmPort, string &schedHost,
				string &schedPort, string &amHost, int32_t amPort,
				string &am_tracking_url, int heartbeatInterval,Mock::TestLibYarnClientStub *stub);
#endif

		virtual ~LibYarnClient();

		virtual int createJob(string &jobName, string &queue, string &jobId);

		virtual void addResourceRequest(Resource capability, int32_t num_containers,
				   string host, int32_t priority, bool relax_locality);

		virtual int addContainerRequests(string &jobId,
										Resource &capability, int32_t num_containers,
										list<struct LibYarnNodeInfo> &preferred,
										int32_t priority, bool relax_locality);

		virtual int allocateResources(string &jobId,
									  list<string> &blackListAdditions, list<string> &blackListRemovals,
									  list<Container> &allocatedContainers, int32_t num_containers);

		virtual int activeResources(string &jobId, int activeContainerIds[],
				int activeContainerSize);

		virtual int releaseResources(string &jobId, int releaseContainerIds[],
				int releaseContainerSize);

		virtual int finishJob(string &jobId, FinalApplicationStatus finalStatus);

		virtual int getApplicationReport(string &jobId, ApplicationReport &report);

		virtual int getContainerReports(string &jobId,
				list<ContainerReport> &containerReports);

		virtual int getContainerStatuses(string &jobId, int32_t containerIds[],
				int containerSize, list<ContainerStatus> &containerStatues);

		virtual int getQueueInfo(string &queue, bool includeApps, bool includeChildQueues,
				bool recursive, QueueInfo &queueInfo);

		virtual int getClusterNodes(list<NodeState> &states, list<NodeReport> &nodeReports);

		virtual int getActiveFailContainerIds(set<int> &activeFailIds);

		friend void* heartbeatFunc(void* args);

		void setErrorMessage(string errorMsg);

		string getErrorMessage();

		bool isJobHealthy();

		list<ResourceRequest> getAskRequests();

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

		ApplicationID clientAppId;
		ApplicationAttemptId clientAppAttempId;

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

		map<int, Container*> jobIdContainers;
		map<string, Token> nmTokenCache;
		set<int> activeFailContainerIds;
		list<ResourceRequest> askRequests;

		volatile bool keepRun;
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
			LibYarnNodeInfo(char *host, char* rack, int32_t cnt)
							:hostname(host), num_containers(cnt)
			{ if(rack == NULL) rackname = DEFAULT_RACK; else rackname = string(rack);}

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
