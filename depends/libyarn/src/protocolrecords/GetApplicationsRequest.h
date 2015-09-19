/*
 * GetApplicationsRequest.h
 *
 *  Created on: Jul 28, 2014
 *      Author: bwang
 */

#ifndef GETAPPLICATIONSREQUEST_H_
#define GETAPPLICATIONSREQUEST_H_

#include <iostream>
#include <list>

#include "YARN_yarn_service_protos.pb.h"

#include "records/YarnApplicationState.h"

using namespace std;
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

