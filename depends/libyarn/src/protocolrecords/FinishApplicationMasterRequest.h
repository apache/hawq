/*
 * FinishApplicationMasterRequest.h
 *
 *  Created on: Jul 20, 2014
 *      Author: bwang
 */

#ifndef FINISHAPPLICATIONMASTERREQUEST_H_
#define FINISHAPPLICATIONMASTERREQUEST_H_

#include <iostream>

#include "YARN_yarn_service_protos.pb.h"

#include "records/FinalApplicationStatus.h"

using namespace std;
using namespace hadoop::yarn;

namespace libyarn {

/*
message FinishApplicationMasterRequestProto {
  optional string diagnostics = 1;
  optional string tracking_url = 2;
  optional FinalApplicationStatusProto final_application_status = 3;
}
*/

class FinishApplicationMasterRequest {
public:
	FinishApplicationMasterRequest();
	FinishApplicationMasterRequest(const FinishApplicationMasterRequestProto &proto);
	virtual ~FinishApplicationMasterRequest();

	FinishApplicationMasterRequestProto& getProto();

	void setDiagnostics(string &diagnostics);
	string getDiagnostics();

	void setTrackingUrl(string &url);
	string getTrackingUrl();

	void setFinalApplicationStatus(FinalApplicationStatus finalState);
	FinalApplicationStatus getFinalApplicationStatus();

private:
	FinishApplicationMasterRequestProto requestProto;
};

} /* namespace libyarn */

#endif /* FINISHAPPLICATIONMASTERREQUEST_H_ */
