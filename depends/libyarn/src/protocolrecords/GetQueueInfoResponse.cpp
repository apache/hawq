/*
 * GetQueueInfoResponse.cpp
 *
 *  Created on: Jul 22, 2014
 *      Author: bwang
 */

#include "GetQueueInfoResponse.h"

namespace libyarn {

GetQueueInfoResponse::GetQueueInfoResponse() {
	responseProto = GetQueueInfoResponseProto::default_instance();
}

GetQueueInfoResponse::GetQueueInfoResponse(const GetQueueInfoResponseProto &proto) :
		responseProto(proto) {
}

GetQueueInfoResponse::~GetQueueInfoResponse() {
}

GetQueueInfoResponseProto& GetQueueInfoResponse::getProto(){
	return responseProto;

}

void GetQueueInfoResponse::setQueueInfo(QueueInfo &info) {
	QueueInfoProto *infoProto = new QueueInfoProto();
	infoProto->CopyFrom(info.getProto());
	responseProto.set_allocated_queueinfo(infoProto);
}

QueueInfo GetQueueInfoResponse::getQueueInfo(){
	return QueueInfo(responseProto.queueinfo());
}

} /* namespace libyarn */
