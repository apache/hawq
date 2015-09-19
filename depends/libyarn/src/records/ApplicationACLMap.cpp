/*
 * ApplicationACLMap.cpp
 *
 *  Created on: Jul 30, 2014
 *      Author: bwang
 */

#include "ApplicationACLMap.h"

namespace libyarn {

ApplicationACLMap::ApplicationACLMap() {
	appAclProto = ApplicationACLMapProto::default_instance();
}

ApplicationACLMap::ApplicationACLMap(const ApplicationACLMapProto &proto) :
		appAclProto(proto) {
}

ApplicationACLMap::~ApplicationACLMap() {
}

ApplicationACLMapProto& ApplicationACLMap::getProto() {
	return appAclProto;
}

void ApplicationACLMap::setAccessType(ApplicationAccessType &accessType) {
	appAclProto.set_accesstype((ApplicationAccessTypeProto) accessType);
}

ApplicationAccessType ApplicationACLMap::getAccessType() {
	return (ApplicationAccessType) appAclProto.accesstype();
}

void ApplicationACLMap::setAcl(string &acl) {
	appAclProto.set_acl(acl);
}

string ApplicationACLMap::getAcl() {
	return appAclProto.acl();
}

} /* namespace libyarn */
