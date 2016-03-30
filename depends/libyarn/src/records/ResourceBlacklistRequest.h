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

#ifndef RESOURCEBLACKLISTREQUEST_H_
#define RESOURCEBLACKLISTREQUEST_H_

#include <list>
#include "YARN_yarn_protos.pb.h"

using std::string; using std::list;
using namespace hadoop::yarn;

namespace libyarn {

/*
message ResourceBlacklistRequestProto {
  repeated string blacklist_additions = 1;
  repeated string blacklist_removals = 2;
}
*/

class ResourceBlacklistRequest {
public:
	ResourceBlacklistRequest();
	ResourceBlacklistRequest(const ResourceBlacklistRequestProto &proto);
	virtual ~ResourceBlacklistRequest();

	ResourceBlacklistRequestProto& getProto();

	void setBlacklistAdditions(list<string> &additions);
	void addBlacklistAddition(string &addition);
	list<string> getBlacklistAdditions();

	void setBlacklistRemovals(list<string> &removals);
	void addBlacklistRemoval(string &removal);
	list<string> getBlacklistRemovals();

private:
	ResourceBlacklistRequestProto requestProto;
};

} /* namespace libyarn */

#endif /* RESOURCEBLACKLISTREQUEST_H_ */
