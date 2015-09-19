/*
 * SubmitApplicationResponse.h
 *
 *  Created on: Jul 14, 2014
 *      Author: bwang
 */

#ifndef SUBMITAPPLICATIONRESPONSE_H_
#define SUBMITAPPLICATIONRESPONSE_H_

#include "YARN_yarn_service_protos.pb.h"

using namespace hadoop::yarn;

namespace libyarn {
/*
message SubmitApplicationResponseProto {
}
*/
class SubmitApplicationResponse {
public:
	SubmitApplicationResponse();
	SubmitApplicationResponse(const SubmitApplicationResponseProto &proto);
	virtual ~SubmitApplicationResponse();

	SubmitApplicationResponseProto& getProto();

private:
	SubmitApplicationResponseProto responseProto;
};

} /* namespace libyarn */

#endif /* SUBMITAPPLICATIONRESPONSE_H_ */
