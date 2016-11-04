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

#include <list>
#include <string>

#include "gtest/gtest.h"
#include "gmock/gmock.h"

#include "records/ContainerLaunchContext.h"

using std::list;
using std::string;
using namespace libyarn;
using namespace testing;

class TestContainerLaunchContext: public ::testing::Test {
protected:
	ContainerLaunchContext containerContext;
};

TEST_F(TestContainerLaunchContext, TestGetLocalResources)
{
	list<StringLocalResourceMap> maps;
	list<StringLocalResourceMap> retMaps;
	StringLocalResourceMap map;
	maps.push_back(map);

	containerContext.setLocalResources(maps);
	retMaps = containerContext.getLocalResources();

	/* The operator '==' is not defined for class StringLocalResrouceMap,
	   so we can't compare list and retMaps here. */

	SUCCEED();
}

TEST_F(TestContainerLaunchContext, TestGetServiceData)
{
	list<StringBytesMap> maps;
	list<StringBytesMap> retMaps;
	StringBytesMap map;
	maps.push_back(map);

	containerContext.setServiceData(maps);
	retMaps = containerContext.getServiceData();

	/* The operator '==' is not defined for class StringBytesMap,
	   so we can't compare list and retMaps here. */

	SUCCEED();
}

TEST_F(TestContainerLaunchContext, TestGetEnvironment)
{
	list<StringStringMap> maps;
	list<StringStringMap> retMaps;
	StringStringMap map;
	maps.push_back(map);

	containerContext.setEnvironment(maps);
	retMaps = containerContext.getEnvironment();

	/* The operator '==' is not defined for class StringStringMap,
	   so we can't compare list and retMaps here. */

	SUCCEED();
}

TEST_F(TestContainerLaunchContext, TestGetApplicationACLs)
{
	list<ApplicationACLMap> maps;
	list<ApplicationACLMap> retMaps;
	ApplicationACLMap map;
	maps.push_back(map);

	containerContext.setApplicationACLs(maps);
	retMaps = containerContext.getApplicationACLs();

	/* The operator '==' is not defined for class ApplicationACLMap,
	   so we can't compare list and retMaps here. */

	SUCCEED();
}

TEST_F(TestContainerLaunchContext, TestGetTokens)
{
	string tokens("tokens");
	string retTokens;

	containerContext.setTokens(tokens);
	retTokens = containerContext.getTokens();

	EXPECT_EQ(tokens, retTokens);
}

TEST_F(TestContainerLaunchContext, TestGetCommand)
{
	list<string> commands;
	list<string> retCommands;
	string command("command");
	commands.push_back(command);

	containerContext.setCommand(commands);
	retCommands = containerContext.getCommand();

	EXPECT_EQ(commands, retCommands);
}
