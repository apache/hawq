/*
 * KillApplicationRequest.h
 *
 *  Created on: Jul 28, 2014
 *      Author: bwang
 */

#ifndef KILLAPPLICATIONREQUEST_H_
#define KILLAPPLICATIONREQUEST_H_

#include "YARN_yarn_service_protos.pb.h"
#include "YARN_yarn_protos.pb.h"
#include "records/ApplicationID.h"

using namespace hadoop::yarn;

namespace libyarn {

//message KillApplicationRequestProto {
//  optional ApplicationIdProto application_id = 1;
//}

class KillApplicationRequest {
public:
	KillApplicationRequest();
	KillApplicationRequest(const KillApplicationRequestProto &proto);
	virtual ~KillApplicationRequest();

	KillApplicationRequestProto& getProto();
	void setApplicationId(ApplicationID &applicationId);

private:
	KillApplicationRequestProto requestProto;
};

} /* namespace libyarn */

#endif /* KILLAPPLICATIONREQUEST_H_ */
