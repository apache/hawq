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

#ifndef LOCALRESOURCE_H_
#define LOCALRESOURCE_H_

#include <iostream>
#include "YARN_yarn_protos.pb.h"
#include "URL.h"
#include "LocalResourceType.h"
#include "LocalResourceVisibility.h"

using std::string;
using namespace hadoop::yarn;
namespace libyarn {

class LocalResource {

//	message LocalResourceProto {
//	  optional URLProto resource = 1;
//	  optional int64 size = 2;
//	  optional int64 timestamp = 3;
//	  optional LocalResourceTypeProto type = 4;
//	  optional LocalResourceVisibilityProto visibility = 5;
//	  optional string pattern = 6;
//	}

public:
	LocalResource();
	LocalResource(const LocalResourceProto &proto);
	virtual ~LocalResource();

	LocalResourceProto& getProto();

	URL getResource();
	void setResource(URL &resource);

	long getSize();
	void setSize(long size);

	long getTimestamp();
	void setTimestamp(long timestamp);

	LocalResourceType getType();
	void setType(LocalResourceType &type);

	LocalResourceVisibility getVisibility();
	void setVisibility(LocalResourceVisibility &visibility);

	string getPattern();
	void setPattern(string &pattern);

private:
	LocalResourceProto localReProto;
};

} /* namespace libyarn */

#endif /* LOCALRESOURCE_H_ */
