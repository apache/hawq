/*
 * GetQueueInfoResponse.h
 *
 *  Created on: Jul 22, 2014
 *      Author: bwang
 */

#ifndef GETQUEUEINFORESPONSE_H_
#define GETQUEUEINFORESPONSE_H_

#include "YARN_yarn_protos.pb.h"
#include "YARN_yarn_service_protos.pb.h"
#include "records/QueueInfo.h"

using namespace hadoop::yarn;

namespace libyarn {

/*
message GetQueueInfoResponseProto {
  optional QueueInfoProto queueInfo = 1;
}
*/

class GetQueueInfoResponse {
public:
	GetQueueInfoResponse();
	GetQueueInfoResponse(const GetQueueInfoResponseProto &proto);
	virtual ~GetQueueInfoResponse();

	GetQueueInfoResponseProto& getProto();

	void setQueueInfo(QueueInfo &info);
	QueueInfo getQueueInfo();

private:
	GetQueueInfoResponseProto responseProto;
};

} /* namespace libyarn */

#endif /* GETQUEUEINFORESPONSE_H_ */
