/*
 * FinishApplicationMasterResponse.h
 *
 *  Created on: Jul 20, 2014
 *      Author: bwang
 */

#ifndef FINISHAPPLICATIONMASTERRESPONSE_H_
#define FINISHAPPLICATIONMASTERRESPONSE_H_

#include "YARN_yarn_service_protos.pb.h"

using namespace hadoop;
using namespace yarn;
namespace libyarn {

/*
message FinishApplicationMasterResponseProto {
  optional bool isUnregistered = 1 [default = false];
}
*/

class FinishApplicationMasterResponse {
public:
	FinishApplicationMasterResponse();
	FinishApplicationMasterResponse(const FinishApplicationMasterResponseProto &proto);
	virtual ~FinishApplicationMasterResponse();

	FinishApplicationMasterResponseProto& getProto();

	void setIsUnregistered(bool isUnregistered);
	bool getIsUnregistered();

private:
	FinishApplicationMasterResponseProto responseProto;
};

} /* namespace libyarn */

#endif /* FINISHAPPLICATIONMASTERRESPONSE_H_ */
