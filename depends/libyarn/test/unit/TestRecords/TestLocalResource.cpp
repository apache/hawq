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

#include <string>

#include "gtest/gtest.h"
#include "gmock/gmock.h"

#include "records/LocalResource.h"

using std::string;
using namespace libyarn;
using namespace testing;

class TestLocalResource: public ::testing::Test {
protected:
	LocalResource localResource;
};

TEST_F(TestLocalResource, TestLocalResource)
{
	LocalResourceProto proto;
	localResource = LocalResource(proto);

	SUCCEED();
}

TEST_F(TestLocalResource, TestLocalGetProto)
{
	LocalResourceProto proto;
	proto = localResource.getProto();

	SUCCEED();
}

TEST_F(TestLocalResource, TestGetResource)
{
	URL resource;
	URL retResource;

	localResource.setResource(resource);
	retResource = localResource.getResource();

	/* The operator '==' is not defined for class URL,
	   so we can't compare resource and retResource here. */

	SUCCEED();
}

TEST_F(TestLocalResource, TestGetSize)
{
	long size = -1;
	long retSize;

	localResource.setSize(size);
	retSize = localResource.getSize();

	EXPECT_EQ(size, retSize);
}

TEST_F(TestLocalResource, TestGetTimeStamp)
{
	long timestamp = -1;
	long retTimestamp;

	localResource.setTimestamp(timestamp);
	retTimestamp = localResource.getTimestamp();

	EXPECT_EQ(timestamp, retTimestamp);
}

TEST_F(TestLocalResource, TestGetLocalResourceType)
{
	LocalResourceType type = LocalResourceType::ARCHIVE;
	LocalResourceType retType;

	localResource.setType(type);
	retType = localResource.getType();

	EXPECT_EQ(type, retType);
}

TEST_F(TestLocalResource, TestGetLocalResourceVisibility)
{
	LocalResourceVisibility visibility = LocalResourceVisibility::PUBLIC;
	LocalResourceVisibility retVisibility;

	localResource.setVisibility(visibility);
	retVisibility = localResource.getVisibility();

	EXPECT_EQ(visibility, retVisibility);
}

TEST_F(TestLocalResource, TestGetPattern)
{
	string pattern("pattern");
	string retPattern;

	localResource.setPattern(pattern);
	retPattern = localResource.getPattern();

	EXPECT_EQ(pattern, retPattern);
}
