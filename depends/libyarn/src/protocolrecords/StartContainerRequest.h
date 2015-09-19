/*
 * StartContainerRequest.h
 *
 *  Created on: Jul 17, 2014
 *      Author: bwang
 */

#ifndef STARTCONTAINERREQUEST_H_
#define STARTCONTAINERREQUEST_H_

#include "YARN_yarn_service_protos.pb.h"
#include "YARNSecurity.pb.h"

#include "records/ContainerLaunchContext.h"
#include "records/Token.h"

using namespace std;
using namespace hadoop::yarn;
using namespace hadoop::common;

namespace libyarn {
/*
message StartContainerRequestProto {
  optional ContainerLaunchContextProto container_launch_context = 1;
  optional hadoop.common.TokenProto container_token = 2;
}
*/
class StartContainerRequest {
public:
	StartContainerRequest();
	StartContainerRequest(const StartContainerRequestProto &proto);
	virtual ~StartContainerRequest();

	StartContainerRequestProto& getProto();

	void setContainerLaunchCtx(ContainerLaunchContext &containerLaunchCtx);
	ContainerLaunchContext getContainerLaunchCtx();

	void setContainerToken(Token &token);
	Token getContainerToken();

private:
	StartContainerRequestProto requestProto;
};

} /* namespace libyarn */

#endif /* STARTCONTAINERREQUEST_H_ */


