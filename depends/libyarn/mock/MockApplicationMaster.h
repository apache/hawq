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
using std::string; using std::list;

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
