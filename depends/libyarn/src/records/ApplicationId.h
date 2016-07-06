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

#ifndef APPLICATIONIdS_H_
#define APPLICATIONIdS_H_

#include "YARN_yarn_protos.pb.h"

using namespace hadoop::yarn;

namespace libyarn {

/*
message ApplicationIdProto {
  optional int32 id = 1;
  optional int64 cluster_timestamp = 2;
}
 */

class ApplicationId {
public:
	ApplicationId();
	ApplicationId(const ApplicationIdProto &proto);
	ApplicationId(const ApplicationId &applicationId);
	virtual ~ApplicationId();

	ApplicationIdProto& getProto();

	void setId(int32_t id);
	int32_t getId();

	void setClusterTimestamp(int64_t timestamp);
	int64_t getClusterTimestamp();

private:
	ApplicationIdProto appIdProto;
};

} /* namespace libyarn */
#endif /* APPLICATIONIdS_H_ */
