/*
 * RegisterApplicationResponse.h
 *
 *  Created on: Jul 15, 2014
 *      Author: bwang
 */

#ifndef REGISTERAPPLICATIONMASTERRESPONSE_H_
#define REGISTERAPPLICATIONMASTERRESPONSE_H_

#include <list>
#include "records/ApplicationACLMap.h"
#include "YARN_yarn_service_protos.pb.h"
#include "records/Resource.h"


using namespace hadoop::yarn;

namespace libyarn {
/*
message RegisterApplicationMasterResponseProto {
  optional ResourceProto maximumCapability = 1;
  optional bytes client_to_am_token_master_key = 2;
  repeated ApplicationACLMapProto application_ACLs = 3;
}
 */
class RegisterApplicationMasterResponse {
public:
	RegisterApplicationMasterResponse();
	RegisterApplicationMasterResponse(const RegisterApplicationMasterResponseProto &proto);
	virtual ~RegisterApplicationMasterResponse();

	RegisterApplicationMasterResponseProto& getProto();

	void setMaximumResourceCapability(Resource &capability);
	Resource getMaximumResourceCapability();

	void setClientToAMTokenMasterKey(string &key);
	string getClientToAMTokenMasterKey();

	void setApplicationACLs(list<ApplicationACLMap> &aclMapList);
	list<ApplicationACLMap> getApplicationACLs();

private:
	RegisterApplicationMasterResponseProto responseProto;
};

} /* namespace libyarn */

#endif /* REGISTERAPPLICATIONMASTERRESPONSE_H_ */
