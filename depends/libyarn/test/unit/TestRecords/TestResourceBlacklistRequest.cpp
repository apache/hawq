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

#include "records/ResourceBlacklistRequest.h"

using std::list;
using std::string;
using namespace libyarn;
using namespace testing;

class TestResourceBlacklistRequest: public ::testing::Test {
protected:
	ResourceBlacklistRequest blacklistRequest;
};

TEST_F(TestResourceBlacklistRequest, TestSetBlacklistAdditions)
{
	list<string> additions;
	list<string> retAdditions;
	string addition("addition");
	additions.push_back(addition);

	blacklistRequest.setBlacklistAdditions(additions);
	retAdditions = blacklistRequest.getBlacklistAdditions();

	EXPECT_EQ(additions, retAdditions);
}

TEST_F(TestResourceBlacklistRequest, TestAddBlacklistAdditions)
{
	list<string> additions;
	list<string> retAdditions;
	string addition("addition");
	additions.push_back(addition);

	blacklistRequest.addBlacklistAddition(addition);
	retAdditions = blacklistRequest.getBlacklistAdditions();

	EXPECT_EQ(additions, retAdditions);
}

TEST_F(TestResourceBlacklistRequest, TestSetBlacklistRemovals)
{
	list<string> removals;
	list<string> retRemovals;
	string removal("removal");
	removals.push_back(removal);

	blacklistRequest.setBlacklistRemovals(removals);
	retRemovals = blacklistRequest.getBlacklistRemovals();

	EXPECT_EQ(removals, retRemovals);
}

TEST_F(TestResourceBlacklistRequest, TestAddBlacklistRemovals)
{
	list<string> removals;
	list<string> retRemovals;
	string removal("removal");
	removals.push_back(removal);

	blacklistRequest.addBlacklistRemoval(removal);
	retRemovals = blacklistRequest.getBlacklistRemovals();

	EXPECT_EQ(removals, retRemovals);
}
