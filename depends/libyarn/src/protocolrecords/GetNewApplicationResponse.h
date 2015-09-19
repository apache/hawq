/*
 * GetNewApplicationResponse.h
 *
 *  Created on: Jul 10, 2014
 *      Author: bwang
 */

#ifndef GETNEWAPPLICATIONRESPONSE_H_
#define GETNEWAPPLICATIONRESPONSE_H_

#include "YARN_yarn_protos.pb.h"
#include "YARN_yarn_service_protos.pb.h"
#include "records/ApplicationID.h"
#include "records/Resource.h"

using namespace hadoop::yarn;

namespace libyarn {
/*
message GetNewApplicationResponseProto {
  optional ApplicationIdProto application_id = 1;
  optional ResourceProto maximumCapability = 2;
}
*/
class GetNewApplicationResponse {
public:
	GetNewApplicationResponse();
	GetNewApplicationResponse(const GetNewApplicationResponseProto &proto);
	virtual ~GetNewApplicationResponse();

	void setApplicationId(ApplicationID &appId);
	ApplicationID getApplicationId();

	void setResource(Resource &resource);
	Resource getResource();

private:
	GetNewApplicationResponseProto responseProto;
};

} /* namespace libyarn */

#endif /* GETNEWAPPLICATIONRESPONSE_H_ */
