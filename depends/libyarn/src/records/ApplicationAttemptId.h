/*
 * AMCommand.h
 *
 *  Created on: Jul 16, 2014
 *      Author: bwang
 */

#ifndef APPLICATIONATTEMPTID_H_
#define APPLICATIONATTEMPTID_H_

#include "YARN_yarn_protos.pb.h"
#include "ApplicationID.h"

using namespace hadoop::yarn;

namespace libyarn {
/*
message ApplicationAttemptIdProto {
  optional ApplicationIdProto application_id = 1;
  optional int32 attemptId = 2;
}
*/
class ApplicationAttemptId {
public:
	ApplicationAttemptId();
	ApplicationAttemptId(const ApplicationAttemptIdProto &proto);
	virtual ~ApplicationAttemptId();

	ApplicationAttemptIdProto& getProto();

	void setApplicationId(ApplicationID &appId);
	ApplicationID getApplicationId();

	void setAttemptId(int32_t attemptId);
	int32_t getAttemptId();

private:
	ApplicationAttemptIdProto attemptIdProto;
};

} /* namespace libyarn */

#endif /* APPLICATIONATTEMPTID_H_ */
