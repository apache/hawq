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

#include "QueueUserACLInfo.h"

namespace libyarn {

QueueUserACLInfo::QueueUserACLInfo() {
	infoProto = QueueUserACLInfoProto::default_instance();
}

QueueUserACLInfo::QueueUserACLInfo(const QueueUserACLInfoProto &proto) :
		infoProto(proto) {
}

QueueUserACLInfo::~QueueUserACLInfo() {
}

QueueUserACLInfoProto& QueueUserACLInfo::getProto() {
	return infoProto;
}

string QueueUserACLInfo::getQueueName() {
	return infoProto.queuename();
}

void QueueUserACLInfo::setQueueName(string &queueName) {
	infoProto.set_queuename(queueName);
}

list<QueueACL> QueueUserACLInfo::getUserAcls() {
	list<QueueACL> aclList;
	for (int i = 0; i < infoProto.useracls_size(); i++) {
		aclList.push_back((QueueACL) infoProto.useracls(i));
	}
	return aclList;
}

void QueueUserACLInfo::setUserAcls(list<QueueACL> &userAclsList) {
	list<QueueACL>::iterator it = userAclsList.begin();
	for (; it != userAclsList.end(); it++) {
		infoProto.add_useracls((QueueACLProto) *it);
	}
}

} /* namespace libyarn */
