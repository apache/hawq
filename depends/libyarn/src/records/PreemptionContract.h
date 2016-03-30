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

#ifndef PREEMPTIONCONTRACT_H_
#define PREEMPTIONCONTRACT_H_

#include <set>
#include <list>

#include "YARN_yarn_protos.pb.h"

#include "PreemptionResourceRequest.h"
#include "PreemptionContainer.h"

using std::list;
using std::set;
using namespace hadoop;
using namespace yarn;
namespace libyarn {

class PreemptionContract {
public:
	PreemptionContract();
	PreemptionContract(const PreemptionContractProto &proto);
	virtual ~PreemptionContract();

	PreemptionContractProto& getProto();
	set<PreemptionContainer> getContainers();
	void setContainers(set<PreemptionContainer> &containers);
	list<PreemptionResourceRequest> getResourceRequest();
	void setResourceRequest(list<PreemptionResourceRequest> &req);
private:
	PreemptionContractProto contractProto;
};

} /* namespace libyarn */

#endif /* PREEMPTIONCONTRACT_H_ */

//message PreemptionContractProto {
//  repeated PreemptionResourceRequestProto resource = 1;
//  repeated PreemptionContainerProto container = 2;
//}
