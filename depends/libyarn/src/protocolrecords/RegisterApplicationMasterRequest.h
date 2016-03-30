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

#ifndef REGISTERAPPLICATIONMASTERREQUEST_H_
#define REGISTERAPPLICATIONMASTERREQUEST_H_

#include "YARN_yarn_service_protos.pb.h"

using std::string;
using namespace hadoop::yarn;

namespace libyarn {

/*
message RegisterApplicationMasterRequestProto {
  optional string host = 1;
  optional int32 rpc_port = 2;
  optional string tracking_url = 3;
}
 */

class RegisterApplicationMasterRequest {
public:
	RegisterApplicationMasterRequest();
	RegisterApplicationMasterRequest(const RegisterApplicationMasterRequestProto &proto);
	virtual ~RegisterApplicationMasterRequest();

	RegisterApplicationMasterRequestProto& getProto();

	void setHost(string &host);
	string getHost();

	void setRpcPort(int32_t port);
	int32_t getRpcPort();

	void setTrackingUrl(string &url);
	string getTrackingUrl();

private:
 	RegisterApplicationMasterRequestProto requestProto;
};

} /* namespace libyarn */

#endif /* REGISTERAPPLICATIONMASTERREQUEST_H_ */
