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
using namespace std;

namespace Mock{
class MockApplicationClientProtocol : public ApplicationClientProtocol {
public:
	MockApplicationClientProtocol(const string & rmHost,
			const string & rmPort, const string & tokenService,
			const SessionConfig & c, const RpcAuth & a):
			ApplicationClientProtocol(rmHost,rmPort,tokenService, c,a){
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
