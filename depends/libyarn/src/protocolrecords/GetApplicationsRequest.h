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


#ifndef GETAPPLICATIONSREQUEST_H_
#define GETAPPLICATIONSREQUEST_H_

#include <iostream>
#include <list>

#include "YARN_yarn_service_protos.pb.h"

#include "records/YarnApplicationState.h"

using std::string; using std::list;
using namespace hadoop::yarn;

namespace libyarn {

//message GetApplicationsRequestProto {
//  repeated string application_types = 1;
//  repeated YarnApplicationStateProto application_states = 2;
//}

class GetApplicationsRequest {
public:
	GetApplicationsRequest();
	GetApplicationsRequest(const GetApplicationsRequestProto &proto);
	virtual ~GetApplicationsRequest();

	GetApplicationsRequestProto& getProto();

	void setApplicationTypes(list<string> &applicationTypes);
	list<string> getApplicationTypes();

	void setApplicationStates(list<YarnApplicationState> &applicationStates);
	list<YarnApplicationState> getApplicationStates();

private:
	GetApplicationsRequestProto requestProto;
};

} /* namespace libyarn */

#endif /* GETAPPLICATIONSREQUEST_H_ */

