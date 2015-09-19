/*
 * QueueUserACLInfo.cpp
 *
 *  Created on: Jul 29, 2014
 *      Author: bwang
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
