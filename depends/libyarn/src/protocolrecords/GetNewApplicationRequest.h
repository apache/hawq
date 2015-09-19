/*
 * GetNewApplicationRequest.h
 *
 *  Created on: Jul 10, 2014
 *      Author: bwang
 */

#ifndef GETNEWAPPLICATIONREQUEST_H_
#define GETNEWAPPLICATIONREQUEST_H_

#include "YARN_yarn_protos.pb.h"
#include "YARN_yarn_service_protos.pb.h"

using namespace hadoop::yarn;

namespace libyarn {

/*
message GetNewApplicationRequestProto {
}
 */

class GetNewApplicationRequest {
public:
	GetNewApplicationRequest();
	GetNewApplicationRequest(const GetNewApplicationRequestProto &proto);
	virtual ~GetNewApplicationRequest();

	GetNewApplicationRequestProto& getProto();

private:
	GetNewApplicationRequestProto requestProto;
};

} /* namespace libyarn */

#endif /* GETNEWAPPLICATIONREQUEST_H_ */
