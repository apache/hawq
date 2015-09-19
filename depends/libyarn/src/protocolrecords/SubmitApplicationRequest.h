/*
 * SubmitApplicationRequest.h
 *
 *  Created on: Jul 14, 2014
 *      Author: bwang
 */

#ifndef SUBMITAPPLICATIONREQUEST_H_
#define SUBMITAPPLICATIONREQUEST_H_

#include "YARN_yarn_service_protos.pb.h"
#include "YARN_yarn_protos.pb.h"
#include "records/ApplicationSubmissionContext.h"


using namespace hadoop::yarn;

namespace libyarn {

/*
message SubmitApplicationRequestProto {
  optional ApplicationSubmissionContextProto application_submission_context= 1;
}
*/

class SubmitApplicationRequest {
public:
	SubmitApplicationRequest();
	SubmitApplicationRequest(const SubmitApplicationRequestProto &proto);
	virtual ~SubmitApplicationRequest();

	SubmitApplicationRequestProto& getProto();

	void setApplicationSubmissionContext(ApplicationSubmissionContext &appCtx);

	ApplicationSubmissionContext getApplicationSubmissionContext();

private:
	SubmitApplicationRequestProto requestProto;
};

} /* namespace libyarn */

#endif /* SUBMITAPPLICATIONREQUEST_H_ */
