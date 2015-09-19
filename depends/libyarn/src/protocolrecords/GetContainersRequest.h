/*
 * GetContainersRequest.h
 *
 *  Created on: Jan 5, 2015
 *      Author: weikui
 */

#ifndef GETCONTAINERSREQUEST_H_
#define GETCONTAINERSREQUEST_H_
#include <iostream>
#include <list>

#include "YARN_yarn_service_protos.pb.h"
#include "YARN_yarn_protos.pb.h"
#include "records/ApplicationAttemptId.h"

using namespace std;
using namespace hadoop::yarn;

namespace libyarn {

/*
message GetContainersRequestProto {
  optional ApplicationIdProto application_id = 1;
}
*/

class GetContainersRequest {
public:
	GetContainersRequest();
	GetContainersRequest(const GetContainersRequestProto &proto);
	virtual ~GetContainersRequest();

	GetContainersRequestProto& getProto();

	void setApplicationAttemptId(ApplicationAttemptId &appAttemptId);

	ApplicationAttemptId getApplicationAttemptId();

private:
	GetContainersRequestProto requestProto;
};

} /* namespace libyarn */

#endif /* GETCONTAINERSREQUEST_H_ */
