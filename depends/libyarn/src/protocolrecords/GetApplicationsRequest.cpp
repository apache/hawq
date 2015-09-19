/*
 * GetApplicationsRequest.cpp
 *
 *  Created on: Jul 28, 2014
 *      Author: bwang
 */

#include "GetApplicationsRequest.h"

namespace libyarn {

GetApplicationsRequest::GetApplicationsRequest() {
	requestProto = GetApplicationsRequestProto::default_instance();
}

GetApplicationsRequest::GetApplicationsRequest(
		const GetApplicationsRequestProto &proto) :
		requestProto(proto) {
}

GetApplicationsRequest::~GetApplicationsRequest() {
}

GetApplicationsRequestProto& GetApplicationsRequest::getProto() {
	return requestProto;
}

void GetApplicationsRequest::setApplicationTypes(
		list<string> &applicationTypes) {
	list<string>::iterator it = applicationTypes.begin();
	for (; it != applicationTypes.end(); it++) {
		requestProto.add_application_types(*it);
	}
}

list<string> GetApplicationsRequest::getApplicationTypes() {
	list<string> typsesList;
	for (int i = 0; i < requestProto.application_types_size(); i++) {
		typsesList.push_back(requestProto.application_types(i));
	}
	return typsesList;
}

void GetApplicationsRequest::setApplicationStates(
		list<YarnApplicationState> &applicationStates) {
	list<YarnApplicationState>::iterator it = applicationStates.begin();
	for (; it != applicationStates.end(); it++) {
		requestProto.add_application_states((YarnApplicationStateProto) *it);
	}
}

list<YarnApplicationState> GetApplicationsRequest::getApplicationStates() {
	list<YarnApplicationState> stateList;
	for (int i = 0; i < requestProto.application_states_size(); i++) {
		stateList.push_back(
				(YarnApplicationState) requestProto.application_states(i));
	}
	return stateList;
}

} /* namespace libyarn */
