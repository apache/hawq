/*
 * AllocateRequest.h
 *
 *  Created on: Jul 15, 2014
 *      Author: bwang
 */

#ifndef ALLOCATEREQUEST_H_
#define ALLOCATEREQUEST_H_

#include <list>

#include "YARN_yarn_service_protos.pb.h"
#include "YARN_yarn_protos.pb.h"

#include "records/ResourceRequest.h"
#include "records/ContainerId.h"
#include "records/ResourceBlacklistRequest.h"

using namespace std;
using namespace hadoop::yarn;

namespace libyarn {

/*
message AllocateRequestProto {
  repeated ResourceRequestProto ask = 1;
  repeated ContainerIdProto release = 2;
  optional ResourceBlacklistRequestProto blacklist_request = 3;
  optional int32 response_id = 4;
  optional float progress = 5;
}
*/

class AllocateRequest {
public:
	AllocateRequest();
	AllocateRequest(const AllocateRequestProto &proto);
	virtual ~AllocateRequest();

	AllocateRequestProto& getProto();

	void addAsk(ResourceRequest &ask);
	void setAsks(list<ResourceRequest> &asks);
	list<ResourceRequest> getAsks();

	void addRelease(ContainerId &release);
	void setReleases(list<ContainerId> &releases);
	list<ContainerId> getReleases();

	void setBlacklistRequest(ResourceBlacklistRequest &request);
	ResourceBlacklistRequest getBlacklistRequest();

	void setResponseId(int32_t responseId);
	int32_t getResponseId();

	void setProgress(float progress);
	float getProgress();

private:
	AllocateRequestProto requestProto;
};

} /* namespace libyarn */

#endif /* ALLOCATEREQUEST_H_ */
