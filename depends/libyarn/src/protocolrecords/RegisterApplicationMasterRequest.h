/*
 * RegisterApplicationMasterRequest.h
 *
 *  Created on: Jul 15, 2014
 *      Author: bwang
 */

#ifndef REGISTERAPPLICATIONMASTERREQUEST_H_
#define REGISTERAPPLICATIONMASTERREQUEST_H_

#include "YARN_yarn_service_protos.pb.h"

using namespace std;
using namespace hadoop::yarn;

namespace libyarn {

/*
message RegisterApplicationMasterRequestProto {
  optional string host = 1;
  optional int32 rpc_port = 2;
  optional string tracking_url = 3;
}
 */

class RegisterApplicationMasterRequest {
public:
	RegisterApplicationMasterRequest();
	RegisterApplicationMasterRequest(const RegisterApplicationMasterRequestProto &proto);
	virtual ~RegisterApplicationMasterRequest();

	RegisterApplicationMasterRequestProto& getProto();

	void setHost(string &host);
	string getHost();

	void setRpcPort(int32_t port);
	int32_t getRpcPort();

	void setTrackingUrl(string &url);
	string getTrackingUrl();

private:
 	RegisterApplicationMasterRequestProto requestProto;
};

} /* namespace libyarn */

#endif /* REGISTERAPPLICATIONMASTERREQUEST_H_ */
