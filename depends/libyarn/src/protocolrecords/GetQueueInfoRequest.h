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

#ifndef GETQUEUEINFOREQUEST_H_
#define GETQUEUEINFOREQUEST_H_

#include "YARN_yarn_service_protos.pb.h"

using std::string;
using namespace hadoop::yarn;

namespace libyarn {

/*
message GetQueueInfoRequestProto {
  optional string queueName = 1;
  optional bool includeApplications = 2;
  optional bool includeChildQueues = 3;
  optional bool recursive = 4;
}
*/

class GetQueueInfoRequest {
public:
	GetQueueInfoRequest();
	GetQueueInfoRequest(const GetQueueInfoRequestProto &proto);
	virtual ~GetQueueInfoRequest();

	GetQueueInfoRequestProto& getProto();

	void setQueueName(string &name);
	string getQueueName();

	void setIncludeApplications(bool includeApplications);
	bool getIncludeApplications();

	void setIncludeChildQueues(bool includeChildQueues);
	bool getIncludeChildQueues();

	void setRecursive(bool recursive);
	bool getRecursive();

private:
	GetQueueInfoRequestProto requestProto;
};

} /* namespace libyarn */

#endif /* GETQUEUEINFOREQUEST_H_ */
