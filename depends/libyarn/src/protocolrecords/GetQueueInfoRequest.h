/*
 * GetQueueInfoRequest.h
 *
 *  Created on: Jul 22, 2014
 *      Author: bwang
 */

#ifndef GETQUEUEINFOREQUEST_H_
#define GETQUEUEINFOREQUEST_H_

#include "YARN_yarn_service_protos.pb.h"

using namespace std;
using namespace hadoop::yarn;

namespace libyarn {

/*
message GetQueueInfoRequestProto {
  optional string queueName = 1;
  optional bool includeApplications = 2;
  optional bool includeChildQueues = 3;
  optional bool recursive = 4;
}
*/

class GetQueueInfoRequest {
public:
	GetQueueInfoRequest();
	GetQueueInfoRequest(const GetQueueInfoRequestProto &proto);
	virtual ~GetQueueInfoRequest();

	GetQueueInfoRequestProto& getProto();

	void setQueueName(string &name);
	string getQueueName();

	void setIncludeApplications(bool includeApplications);
	bool getIncludeApplications();

	void setIncludeChildQueues(bool includeChildQueues);
	bool getIncludeChildQueues();

	void setRecursive(bool recursive);
	bool getRecursive();

private:
	GetQueueInfoRequestProto requestProto;
};

} /* namespace libyarn */

#endif /* GETQUEUEINFOREQUEST_H_ */
