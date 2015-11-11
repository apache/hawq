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
 * TestLibYarnClientStub.h
 *
 *  Created on: Mar 11, 2015
 *      Author: weikui
 */

#ifndef TESTLIBYARNCLIENTSTUB_H_
#define TESTLIBYARNCLIENTSTUB_H_

#include "libyarnclient/ApplicationClient.h"
#include "libyarnclient/ApplicationMaster.h"
#include "libyarnclient/ContainerManagement.h"

using namespace libyarn;
namespace Mock {
class TestLibYarnClientStub {
public:
	virtual ~TestLibYarnClientStub() {}

	virtual ApplicationClient* getApplicationClient() = 0;
	virtual ApplicationMaster* getApplicationMaster() = 0;
	virtual ContainerManagement* getContainerManagement() = 0;
};
}
#endif /* TESTLIBYARNCLIENTSTUB_H_ */
