/*
 * GetApplicationReportRequest.h
 *
 *  Created on: Jul 14, 2014
 *      Author: bwang
 */

#ifndef GETAPPLICATIONREPORTREQUEST_H_
#define GETAPPLICATIONREPORTREQUEST_H_

#include "YARN_yarn_service_protos.pb.h"
#include "YARN_yarn_protos.pb.h"
#include "records/ApplicationID.h"

using namespace hadoop::yarn;

namespace libyarn {

/*
message GetApplicationReportRequestProto {
  optional ApplicationIdProto application_id = 1;
}
*/

class GetApplicationReportRequest {
public:
	GetApplicationReportRequest();
	GetApplicationReportRequest(const GetApplicationReportRequestProto &proto);
	virtual ~GetApplicationReportRequest();

	GetApplicationReportRequestProto& getProto();

	void setApplicationId(ApplicationID &appId);
	ApplicationID getApplicationId();

private:
	GetApplicationReportRequestProto requestProto;
};

} /* namespace libyarn */

#endif /* GETAPPLICATIONREPORTREQUEST_H_ */
