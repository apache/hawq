/*
 * ContainerLaunchContext.h
 *
 *  Created on: Jul 14, 2014
 *      Author: bwang
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

using namespace std;
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
