/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

#ifndef CONTAINERID_H_
#define CONTAINERID_H_

#include "YARN_yarn_protos.pb.h"

#include "ApplicationId.h"
#include "ApplicationAttemptId.h"

using namespace hadoop::yarn;

namespace libyarn {

/*
message ContainerIdProto {
  optional ApplicationIdProto app_id = 1;
  optional ApplicationAttemptIdProto app_attempt_id = 2;
  optional int64 id = 3;
}
*/

class ContainerId {
public:
	ContainerId();
	ContainerId(const ContainerIdProto &proto);
	virtual ~ContainerId();

	ContainerIdProto& getProto();

	void setApplicationId(ApplicationId &appId);
	ApplicationId getApplicationId();

	void setApplicationAttemptId(ApplicationAttemptId &appAttemptId);
	ApplicationAttemptId getApplicationAttemptId();

	void setId(int64_t id);
	int64_t getId();

private:
	ContainerIdProto containerIdProto;
};

} /* namespace libyarn */

#endif /* CONTAINERID_H_ */
