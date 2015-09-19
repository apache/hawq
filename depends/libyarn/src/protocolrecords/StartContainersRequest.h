/*
 * StartContainersRequest.h
 *
 *  Created on: Jul 17, 2014
 *      Author: bwang
 */

#ifndef STARTCONTAINERSREQUEST_H_
#define STARTCONTAINERSREQUEST_H_

#include <list>
#include "YARN_yarn_service_protos.pb.h"
#include "StartContainerRequest.h"
#include "records/Token.h"

using namespace std;
using namespace hadoop::yarn;

namespace libyarn {
/*
message StartContainersRequestProto {
  repeated StartContainerRequestProto start_container_request = 1;
}
*/
class StartContainersRequest {
public:
	StartContainersRequest();
	StartContainersRequest(const StartContainersRequestProto proto);
	virtual ~StartContainersRequest();

	StartContainersRequestProto& getProto();

	void setStartContainerRequests(list<StartContainerRequest> &requests);
	list<StartContainerRequest> getStartContainerRequests();

private:
	StartContainersRequestProto requestProto;
};

} /* namespace libyarn */

#endif /* STARTCONTAINERSREQUEST_H_ */
