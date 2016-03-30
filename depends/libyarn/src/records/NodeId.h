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

#ifndef NODEID_H_
#define NODEID_H_

#include "YARN_yarn_protos.pb.h"

using std::string; 

using namespace hadoop::yarn;

namespace libyarn {
/*
message NodeIdProto {
  optional string host = 1;
  optional int32 port = 2;
}
*/
class NodeId {
public:
	NodeId();
	NodeId(const NodeIdProto &proto);
	virtual ~NodeId();

	NodeIdProto& getProto();

	void setHost(string &host);
	string getHost();

	void setPort(int32_t port);
	int32_t getPort();

private:
	NodeIdProto nodeIdProto;
};

} /* namespace libyarn */

#endif /* NODEID_H_ */
