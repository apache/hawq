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

void ApplicationACLMap::setAccessType(ApplicationAccessType accessType) {
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
