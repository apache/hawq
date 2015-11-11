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

#ifndef GETNEWAPPLICATIONRESPONSE_H_
#define GETNEWAPPLICATIONRESPONSE_H_

#include "YARN_yarn_protos.pb.h"
#include "YARN_yarn_service_protos.pb.h"
#include "records/ApplicationID.h"
#include "records/Resource.h"

using namespace hadoop::yarn;

namespace libyarn {
/*
message GetNewApplicationResponseProto {
  optional ApplicationIdProto application_id = 1;
  optional ResourceProto maximumCapability = 2;
}
*/
class GetNewApplicationResponse {
public:
	GetNewApplicationResponse();
	GetNewApplicationResponse(const GetNewApplicationResponseProto &proto);
	virtual ~GetNewApplicationResponse();

	void setApplicationId(ApplicationID &appId);
	ApplicationID getApplicationId();

	void setResource(Resource &resource);
	Resource getResource();

private:
	GetNewApplicationResponseProto responseProto;
};

} /* namespace libyarn */

#endif /* GETNEWAPPLICATIONRESPONSE_H_ */
