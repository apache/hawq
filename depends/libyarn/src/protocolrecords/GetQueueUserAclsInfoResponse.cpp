/*
 * GetQueueUserAclsInfoResponse.cpp
 *
 *  Created on: Jul 29, 2014
 *      Author: bwang
 */

#include "GetQueueUserAclsInfoResponse.h"

namespace libyarn {

GetQueueUserAclsInfoResponse::GetQueueUserAclsInfoResponse() {
	responseProto = GetQueueUserAclsInfoResponseProto::default_instance();
}

GetQueueUserAclsInfoResponse::GetQueueUserAclsInfoResponse(
		const GetQueueUserAclsInfoResponseProto &proto) :
		responseProto(proto) {
}

GetQueueUserAclsInfoResponse::~GetQueueUserAclsInfoResponse() {
}

GetQueueUserAclsInfoResponseProto& GetQueueUserAclsInfoResponse::getProto() {
	return responseProto;
}

list<QueueUserACLInfo> GetQueueUserAclsInfoResponse::getUserAclsInfoList() {
	list<QueueUserACLInfo> aclInfoList;
	for (int i = 0; i < responseProto.queueuseracls_size(); i++) {
		aclInfoList.push_back(QueueUserACLInfo(responseProto.queueuseracls(i)));
	}
	return aclInfoList;
}

void GetQueueUserAclsInfoResponse::setUserAclsInfoList(
		list<QueueUserACLInfo> queueUserAclsList) {
	list<QueueUserACLInfo>::iterator it = queueUserAclsList.begin();
	for (; it != queueUserAclsList.end(); it++) {
		QueueUserACLInfoProto* infoProto = responseProto.add_queueuseracls();
		infoProto->CopyFrom((*it).getProto());
	}
}

} /* namespace libyarn */
