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

#ifndef STRINGSTRINGMAP_H_
#define STRINGSTRINGMAP_H_

#include <iostream>
#include "YARN_yarn_protos.pb.h"

using std::string;
using namespace hadoop::yarn;

namespace libyarn {

//message StringStringMapProto {
//  optional string key = 1;
//  optional string value = 2;
//}

class StringStringMap {
public:
	StringStringMap();
	StringStringMap(const StringStringMapProto &proto);
	virtual ~StringStringMap();

	StringStringMapProto& getProto();

	void setKey(string &key);
	string getKey();

	void setValue(string &value);
	string getValue();

private:
	StringStringMapProto ssMapProto;
};

} /* namespace libyarn */

#endif /* STRINGSTRINGMAP_H_ */
