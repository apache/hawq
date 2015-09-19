/*
 * ContainerManagement.h
 *
 *  Created on: Jun 25, 2014
 *      Author: bwang
 */

#ifndef CONTAINERMANAGEMENT_H_
#define CONTAINERMANAGEMENT_H_

#include <cstdint>
#include <iostream>
#include <list>

#include "libyarncommon/UserInfo.h"
#include "libyarncommon/Token.h"
#include "libyarnserver/ContainerManagementProtocol.h"
#include "common/SessionConfig.h"
#include "records/Container.h"
#include "records/ContainerLaunchContext.h"
#include "records/ContainerId.h"
#include "records/NodeId.h"
#include "records/ContainerStatus.h"

#include "protocolrecords/StartContainerResponse.h"
#include "protocolrecords/StartContainerRequest.h"

#include "protocolrecords/StopContainersRequest.h"
#include "protocolrecords/StopContainersResponse.h"

#include "protocolrecords/GetContainerStatusesRequest.h"
#include "protocolrecords/GetContainerStatusesResponse.h"

using namespace std;

#ifdef MOCKTEST
#include "TestContainerManagementStub.h"
#endif

namespace libyarn {

class ContainerManagement {
public:
	ContainerManagement();

	virtual ~ContainerManagement();

	virtual StartContainerResponse startContainer(Container &container,
			StartContainerRequest &request, Token &nmToken);

	virtual void stopContainer(Container &container, Token &nmToken);

	virtual ContainerStatus getContainerStatus(Container &container, Token &nmToken);

#ifdef MOCKTEST
private:
    /*
     * for test
     */
    Mock::TestContainerManagementStub* stub;
#endif
};

} /* namespace libyarn */

#endif /* CONTAINERMANAGEMENT_H_ */
