/*
 * ApplicationSubmissionContext.h
 *
 *  Created on: Jul 14, 2014
 *      Author: bwang
 */

#ifndef APPLICATIONSUBMISSIONCONTEXT_H_
#define APPLICATIONSUBMISSIONCONTEXT_H_

#include <iostream>
#include "ContainerLaunchContext.h"
#include "YARN_yarn_protos.pb.h"
#include "ApplicationID.h"
#include "Priority.h"
#include "Resource.h"

using namespace std;
using namespace hadoop::yarn;

namespace libyarn {
/*
message ApplicationSubmissionContextProto {
 optional ApplicationIdProto application_id = 1;
 optional string application_name = 2 [default = "N/A"];
 optional string queue = 3 [default = "default"];
 optional PriorityProto priority = 4;
 optional ContainerLaunchContextProto am_container_spec = 5;
 optional bool cancel_tokens_when_complete = 6 [default = true];
 optional bool unmanaged_am = 7 [default = false];
 optional int32 maxAppAttempts = 8 [default = 0];
 optional ResourceProto resource = 9;
 optional string applicationType = 10 [default = "YARN"];
 }
*/
class ApplicationSubmissionContext {
public:
	ApplicationSubmissionContext();
	ApplicationSubmissionContext(const ApplicationSubmissionContextProto &proto);
	virtual ~ApplicationSubmissionContext();

	ApplicationSubmissionContextProto& getProto();

	void setApplicationId(ApplicationID &appId);
	ApplicationID getApplicationId();

	void setApplicationName(string &applicationName);
	string getApplicationName();

	void setQueue(string &queue);
	string getQueue();

	void setPriority(Priority &priority);
	Priority getPriority();

	void setAMContainerSpec(ContainerLaunchContext &ctx);
	ContainerLaunchContext getAMContainerSpec();

	void setCancelTokensWhenComplete(bool flag);
	bool getCancelTokensWhenComplete();

	void setUnmanagedAM(bool flag);
	bool getUnmanagedAM();

	void setMaxAppAttempts(int32_t max);
	int32_t getMaxAppAttempts();

	void setResource(Resource &resource);
	Resource getResource();

	void setApplicationType(string &type);
	string getApplicationType();

private:
	ApplicationSubmissionContextProto submitCtxProto;
};

} /* namespace libyarn */

#endif /* APPLICATIONSUBMISSIONCONTEXT_H_ */
