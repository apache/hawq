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

#ifndef SERIALIZEDEXCEPTION_H_
#define SERIALIZEDEXCEPTION_H_

#include <iostream>
#include "YARN_yarn_protos.pb.h"

using std::string;
using namespace hadoop::yarn;
namespace libyarn {

//message SerializedExceptionProto {
//  optional string message = 1;
//  optional string trace = 2;
//  optional string class_name = 3;
//  optional SerializedExceptionProto cause = 4;
//}

class SerializedException {
public:
	SerializedException();
	SerializedException(const SerializedExceptionProto &proto);
	virtual ~SerializedException();

	SerializedExceptionProto& getProto();

	void setMessage(string &message);
	string getMessage();

	void setTrace(string &trace);
	string getTrace();

	void setClassName(string &name);
	string getClassName();

	void setCause(SerializedException &cause);
	SerializedException getCause();

private:
	SerializedExceptionProto exceptionProto;
};

} /* namespace libyarn */

#endif /* SERIALIZEDEXCEPTION_H_ */
