/*
 * AMCommand.h
 *
 *  Created on: Jul 16, 2014
 *      Author: bwang
 */
#ifndef APPLICATIONIDS_H_
#define APPLICATIONIDS_H_

#include "YARN_yarn_protos.pb.h"

using namespace hadoop::yarn;

namespace libyarn {

/*
message ApplicationIdProto {
  optional int32 id = 1;
  optional int64 cluster_timestamp = 2;
}
 */

class ApplicationID {
public:
	ApplicationID();
	ApplicationID(const ApplicationIdProto &proto);
	ApplicationID(const ApplicationID &applicationId);
	virtual ~ApplicationID();

	ApplicationIdProto& getProto();

	void setId(int32_t id);
	int32_t getId();

	void setClusterTimestamp(int64_t timestamp);
	int64_t getClusterTimestamp();

private:
	ApplicationIdProto appIdProto;
};

} /* namespace libyarn */
#endif /* APPLICATIONIDS_H_ */
