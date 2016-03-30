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

#ifndef CONTAINERLAUNCHCONTEXT_H_
#define CONTAINERLAUNCHCONTEXT_H_

#include <iostream>
#include <list>
#include <map>
#include "YARN_yarn_protos.pb.h"

#include "StringLocalResourceMap.h"
#include "StringBytesMap.h"
#include "StringStringMap.h"
#include "ApplicationACLMap.h"
#include "LocalResource.h"
#include "StringStringMap.h"

using std::string; using std::list;
using namespace hadoop::yarn;

namespace libyarn {
/*
message ContainerLaunchContextProto {
  repeated StringLocalResourceMapProto localResources = 1;   //TODO:
  optional bytes tokens = 2;																//TODO:
  repeated StringBytesMapProto service_data = 3;						//TODO:
  repeated StringStringMapProto environment = 4;						//TODO:
  repeated string command = 5;
  repeated ApplicationACLMapProto application_ACLs = 6;     //TODO:
}
*/
class ContainerLaunchContext {
public:
	ContainerLaunchContext();
	ContainerLaunchContext(const ContainerLaunchContextProto &proto);
	virtual ~ContainerLaunchContext();

	ContainerLaunchContextProto& getProto();

	list<StringLocalResourceMap> getLocalResources();
	void setLocalResources(list<StringLocalResourceMap> &resourcesList);

	list<StringBytesMap> getServiceData();
	void setServiceData(list<StringBytesMap> &serviceDataList);

	list<StringStringMap> getEnvironment();
	void setEnvironment(list<StringStringMap> &envList);

	list<ApplicationACLMap> getApplicationACLs();
	void setApplicationACLs(list<ApplicationACLMap> &aclList);

	string getTokens();
	void setTokens(string &tokens);

	void setCommand(list<string> &commands);
	list<string> getCommand();

private:
	ContainerLaunchContextProto containerLaunchCtxProto;
};

} /* namespace libyarn */

#endif /* CONTAINERLAUNCHCONTEXT_H_ */
