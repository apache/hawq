/*
 * MockContainerManagementProtocol.h
 *
 *  Created on: Mar 9, 2015
 *      Author: weikui
 */

#ifndef MOCKCONTAINERMANAGEMENTPROTOCOL_H_
#define MOCKCONTAINERMANAGEMENTPROTOCOL_H_
#include "gmock/gmock.h"
#include "libyarnserver/ContainerManagementProtocol.h"

using namespace libyarn;
using namespace std;

namespace Mock{
class MockContainerManagementProtocol : public ContainerManagementProtocol {
public:
	MockContainerManagementProtocol(string & nmHost, string & nmPort,
			const string & tokenService, const SessionConfig & c,
			const RpcAuth & a):
			ContainerManagementProtocol(nmHost,nmPort,tokenService, c,a){
	}
	~MockContainerManagementProtocol(){
	}

	MOCK_METHOD1(startContainers, StartContainersResponse(StartContainersRequest &request));
	MOCK_METHOD1(stopContainers, StopContainersResponse(StopContainersRequest &request));
	MOCK_METHOD1(getContainerStatuses, GetContainerStatusesResponse(GetContainerStatusesRequest &request));

};
}
#endif /* MOCKCONTAINERMANAGEMENTPROTOCOL_H_ */
