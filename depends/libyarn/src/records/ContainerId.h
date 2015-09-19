/*
 * AMCommand.h
 *
 *  Created on: Jul 16, 2014
 *      Author: bwang
 */
#ifndef CONTAINERID_H_
#define CONTAINERID_H_

#include "YARN_yarn_protos.pb.h"

#include "ApplicationID.h"
#include "ApplicationAttemptId.h"

using namespace hadoop::yarn;

namespace libyarn {

/*
message ContainerIdProto {
  optional ApplicationIdProto app_id = 1;
  optional ApplicationAttemptIdProto app_attempt_id = 2;
  optional int32 id = 3;
}
*/

class ContainerId {
public:
	ContainerId();
	ContainerId(const ContainerIdProto &proto);
	virtual ~ContainerId();

	ContainerIdProto& getProto();

	void setApplicationId(ApplicationID &appId);
	ApplicationID getApplicationId();

	void setApplicationAttemptId(ApplicationAttemptId &appAttemptId);
	ApplicationAttemptId getApplicationAttemptId();

	void setId(int32_t id);
	int32_t getId();

private:
	ContainerIdProto containerIdProto;
};

} /* namespace libyarn */

#endif /* CONTAINERID_H_ */
