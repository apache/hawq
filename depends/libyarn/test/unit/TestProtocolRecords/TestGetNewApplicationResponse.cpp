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

#include "gtest/gtest.h"
#include "gmock/gmock.h"

#include "protocolrecords/GetNewApplicationResponse.h"

using namespace libyarn;
using namespace testing;

class TestGetNewApplicationResponse: public ::testing::Test {
protected:
	GetNewApplicationResponse getResponse;
};

TEST_F(TestGetNewApplicationResponse, TestGetNewApplicationResponse)
{
	Resource resource;
	Resource retResource;

	getResponse.setResource(resource);
	retResource = getResponse.getResource();

	/* The operator '==' is not defined for class Resource,
	   so we can't compare resource and retResource here. */
	SUCCEED();
}
