/*
 * GetQueueUserAclsInfoRequest.h
 *
 *  Created on: Jul 29, 2014
 *      Author: bwang
 */

#ifndef GETQUEUEUSERACLSINFOREQUEST_H_
#define GETQUEUEUSERACLSINFOREQUEST_H_

#include "YARN_yarn_service_protos.pb.h"

using namespace hadoop::yarn;

namespace libyarn {

//message GetQueueUserAclsInfoRequestProto {
//}

class GetQueueUserAclsInfoRequest {
public:
	GetQueueUserAclsInfoRequest();
	GetQueueUserAclsInfoRequest(const GetQueueUserAclsInfoRequestProto &proto);
	virtual ~GetQueueUserAclsInfoRequest();

	GetQueueUserAclsInfoRequestProto& getProto();
private:
	GetQueueUserAclsInfoRequestProto requestProto;
};

} /* namespace libyarn */

#endif /* GETQUEUEUSERACLSINFOREQUEST_H_ */
