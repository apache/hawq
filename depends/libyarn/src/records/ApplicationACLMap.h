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

#ifndef APPLICATIONACLMAP_H_
#define APPLICATIONACLMAP_H_

#include <iostream>
#include "YARN_yarn_protos.pb.h"
#include "ApplicationAccessType.h"

using std::string; 
using namespace hadoop::yarn;

namespace libyarn {

//message ApplicationACLMapProto {
//  optional ApplicationAccessTypeProto accessType = 1;
//  optional string acl = 2 [default = " "];
//}

class ApplicationACLMap {
public:
	ApplicationACLMap();
	ApplicationACLMap(const ApplicationACLMapProto &proto);
	virtual ~ApplicationACLMap();

	ApplicationACLMapProto& getProto();

	void setAccessType(ApplicationAccessType accessType);
	ApplicationAccessType getAccessType();

	void setAcl(string &acl);
	string getAcl();

private:
	ApplicationACLMapProto appAclProto;
};

} /* namespace libyarn */

#endif /* APPLICATIONACLMAP_H_ */
