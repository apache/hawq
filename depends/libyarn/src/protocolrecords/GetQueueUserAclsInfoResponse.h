/*
 * GetQueueUserAclsInfoResponse.h
 *
 *  Created on: Jul 29, 2014
 *      Author: bwang
 */

#ifndef GETQUEUEUSERACLSINFORESPONSE_H_
#define GETQUEUEUSERACLSINFORESPONSE_H_

#include <list>
#include "YARN_yarn_service_protos.pb.h"
#include "records/QueueUserACLInfo.h"

using namespace std;
using namespace hadoop::yarn;

namespace libyarn {

//message GetQueueUserAclsInfoResponseProto {
//  repeated QueueUserACLInfoProto queueUserAcls = 1;
//}

class GetQueueUserAclsInfoResponse {
public:
	GetQueueUserAclsInfoResponse();
	GetQueueUserAclsInfoResponse(const GetQueueUserAclsInfoResponseProto &proto);
	virtual ~GetQueueUserAclsInfoResponse();

	GetQueueUserAclsInfoResponseProto& getProto();
	list<QueueUserACLInfo> getUserAclsInfoList();
	void setUserAclsInfoList(list<QueueUserACLInfo> queueUserAclsList);

private:
	GetQueueUserAclsInfoResponseProto responseProto;

};

} /* namespace libyarn */

#endif /* GETQUEUEUSERACLSINFORESPONSE_H_ */
