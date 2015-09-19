/*
 * MockApplicationMaster.h
 *
 *  Created on: Mar 11, 2015
 *      Author: weikui
 */

#ifndef MOCKAPPLICATIONMASTER_H_
#define MOCKAPPLICATIONMASTER_H_

#include "gmock/gmock.h"
#include "libyarnclient/ApplicationMaster.h"

using namespace libyarn;
using namespace std;

namespace Mock{
class MockApplicationMaster : public ApplicationMaster {
public:
	MockApplicationMaster(string &schedHost, string &schedPort,
			UserInfo &user, const string &tokenService):
			ApplicationMaster(schedHost,schedPort,user,tokenService){
	}
	~MockApplicationMaster(){
	}

	MOCK_METHOD3(registerApplicationMaster, RegisterApplicationMasterResponse(string &amHost,int32_t amPort, string &am_tracking_url));
	MOCK_METHOD5(allocate, AllocateResponse(list<ResourceRequest> &asks,list<ContainerId> &releases,
			ResourceBlacklistRequest &blacklistRequest, int32_t responseId,float progress));
	MOCK_METHOD3(finishApplicationMaster, bool (string &diagnostics, string &trackingUrl,FinalApplicationStatus finalstatus));

};
}

#endif /* MOCKAPPLICATIONMASTER_H_ */
