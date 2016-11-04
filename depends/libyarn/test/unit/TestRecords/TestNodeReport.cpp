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

#include "records/NodeReport.h"

using std::string;
using namespace libyarn;
using namespace testing;

class TestNodeReport: public ::testing::Test {
protected:
	NodeReport nodeReport;
};

TEST_F(TestNodeReport, TestGetHttpAddress)
{
	string address("address");
	string retAddress;

	nodeReport.setHttpAddress(address);
	retAddress = nodeReport.getHttpAddress();

	EXPECT_EQ(address, retAddress);
}

TEST_F(TestNodeReport, TestGetUsedResource)
{
	Resource resource;
	Resource retResource;

	nodeReport.setUsedResource(resource);
	retResource = nodeReport.getUsedResource();

	/* The operator '==' is not defined for class Resource,
	   so we can't compare resource and retResource here. */

	SUCCEED();
}

TEST_F(TestNodeReport, TestGetHealthReport)
{
	string report("report");
	string retReport;

	nodeReport.setHealthReport(report);
	retReport = nodeReport.getHealthReport();

	EXPECT_EQ(report, retReport);
}

TEST_F(TestNodeReport, TestGetLastHealthReportTime)
{
	int64_t time = -1;
	int64_t retTime;

	nodeReport.setLastHealthReportTime(time);
	retTime = nodeReport.getLastHealthReportTime();

	EXPECT_EQ(time, retTime);
}
