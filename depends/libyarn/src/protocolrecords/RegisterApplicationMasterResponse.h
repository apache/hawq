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

#ifndef REGISTERAPPLICATIONMASTERRESPONSE_H_
#define REGISTERAPPLICATIONMASTERRESPONSE_H_

#include <list>
#include "records/ApplicationACLMap.h"
#include "YARN_yarn_service_protos.pb.h"
#include "records/Resource.h"

using std::list;
using namespace hadoop::yarn;

namespace libyarn {
/*
message RegisterApplicationMasterResponseProto {
  optional ResourceProto maximumCapability = 1;
  optional bytes client_to_am_token_master_key = 2;
  repeated ApplicationACLMapProto application_ACLs = 3;
}
 */
class RegisterApplicationMasterResponse {
public:
	RegisterApplicationMasterResponse();
	RegisterApplicationMasterResponse(const RegisterApplicationMasterResponseProto &proto);
	virtual ~RegisterApplicationMasterResponse();

	RegisterApplicationMasterResponseProto& getProto();

	void setMaximumResourceCapability(Resource &capability);
	Resource getMaximumResourceCapability();

	void setClientToAMTokenMasterKey(string &key);
	string getClientToAMTokenMasterKey();

	void setApplicationACLs(list<ApplicationACLMap> &aclMapList);
	list<ApplicationACLMap> getApplicationACLs();

private:
	RegisterApplicationMasterResponseProto responseProto;
};

} /* namespace libyarn */

#endif /* REGISTERAPPLICATIONMASTERRESPONSE_H_ */
