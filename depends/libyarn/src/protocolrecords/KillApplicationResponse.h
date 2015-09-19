/*
 * KillApplicationResponse.h
 *
 *  Created on: Jul 28, 2014
 *      Author: bwang
 */

#ifndef KILLAPPLICATIONRESPONSE_H_
#define KILLAPPLICATIONRESPONSE_H_

#include "YARN_yarn_service_protos.pb.h"

using namespace hadoop::yarn;

namespace libyarn {

//message KillApplicationResponseProto {
//}

class KillApplicationResponse {
public:
	KillApplicationResponse();
	KillApplicationResponse(const KillApplicationResponseProto &proto);
	virtual ~KillApplicationResponse();

	KillApplicationResponseProto& getProto();

private:
	KillApplicationResponseProto responseProto;
};

} /* namespace libyarn */

#endif /* KILLAPPLICATIONRESPONSE_H_ */
