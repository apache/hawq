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

#ifndef URL_H_
#define URL_H_

#include <iostream>
#include "YARN_yarn_protos.pb.h"

using std::string;
using namespace hadoop::yarn;
namespace libyarn {

//message URLProto {
//  optional string scheme = 1;
//  optional string host = 2;
//  optional int32 port = 3;
//  optional string file = 4;
//  optional string userInfo = 5;
//}

class URL {
public:
	URL();
	URL(const URLProto &proto);
	virtual ~URL();

	URLProto& getProto();

	string getScheme();
	void setScheme(string &scheme);

	string getHost();
	void setHost(string &host);

	int32_t getPort();
	void setPort(int32_t port);

	string getFile();
	void setFile(string &file);

	string getUserInfo();
	void setUserInfo(string &userInfo);

private:
	URLProto urlProto;
};

} /* namespace libyarn */

#endif /* URL_H_ */
