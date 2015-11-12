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

#ifndef NMTOKEN_H_
#define NMTOKEN_H_

#include "YARN_yarn_protos.pb.h"
#include "YARN_yarn_service_protos.pb.h"

#include "NodeId.h"
#include "Token.h"

using namespace hadoop::yarn;
using namespace hadoop::common;

namespace libyarn {

/*
message NMTokenProto {
  optional NodeIdProto nodeId = 1;
  optional hadoop.common.TokenProto token = 2;
}
*/

class NMToken {
public:
	NMToken();
	NMToken(const NMTokenProto &proto);
	virtual ~NMToken();

	NMTokenProto& getProto();

	void setNodeId(NodeId &nodeId);
	NodeId getNodeId();

	void setToken(Token token);
	Token getToken();

private:
	NMTokenProto nmTokenProto;
};

} /* namespace libyarn */

#endif /* NMTOKEN_H_ */
