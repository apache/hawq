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

/*
 * MockContainerManagement.h
 *
 *  Created on: Mar 11, 2015
 *      Author: weikui
 */

#ifndef MOCKCONTAINERMANAGEMENT_H_
#define MOCKCONTAINERMANAGEMENT_H_

#include "gmock/gmock.h"
#include "libyarnclient/ContainerManagement.h"

using namespace libyarn;
using std::string; using std::list;

namespace Mock{
class MockContainerManagement : public ContainerManagement {
public:
	~MockContainerManagement(){
	}
	MOCK_METHOD3(startContainer, StartContainerResponse (Container &container,StartContainerRequest &request, libyarn::Token &nmToken));
	MOCK_METHOD2(stopContainer, void (Container &container, libyarn::Token &nmToken));
	MOCK_METHOD2(getContainerStatus, ContainerStatus (Container &container, libyarn::Token &nmToken));
};
}




#endif /* MOCKCONTAINERMANAGEMENT_H_ */
