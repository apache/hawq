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
