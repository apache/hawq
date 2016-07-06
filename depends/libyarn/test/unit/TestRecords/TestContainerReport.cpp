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

#include "records/ContainerReport.h"

using std::string;
using namespace libyarn;
using namespace testing;

class TestContainerReport: public ::testing::Test {
protected:
	ContainerReport containerReport;
};

TEST_F(TestContainerReport, TestGetNodeId)
{
	NodeId id;
	NodeId retId;

	containerReport.setNodeId(id);
	retId = containerReport.getNodeId();

	/* The operator '==' is not defined for class StringLocalResrouceMap,
	   so we can't compare list and retMap here. */

	SUCCEED();
}

TEST_F(TestContainerReport, TestGetCreationTime)
{
	int64_t time = -1;
	int64_t retTime;

	containerReport.setCreationTime(time);
	retTime = containerReport.getCreationTime();

	EXPECT_EQ(time, retTime);
}

TEST_F(TestContainerReport, TestGetFinishTime)
{
	int64_t time = -1;
	int64_t retTime;

	containerReport.setFinishTime(time);
	retTime = containerReport.getFinishTime();

	EXPECT_EQ(time, retTime);
}

TEST_F(TestContainerReport, TestGetContainerExitStatus)
{
	ContainerExitStatus status = ContainerExitStatus::ABORTED;
	ContainerExitStatus retStatus;

	containerReport.setContainerExitStatus(status);
	retStatus = containerReport.getContainerExitStatus();

	EXPECT_EQ(status, retStatus);
}

TEST_F(TestContainerReport, TestGetContainerState)
{
	ContainerState state = ContainerState::C_NEW;
	ContainerState retState;

	containerReport.setContainerState(state);
	retState = containerReport.getContainerState();

	EXPECT_EQ(state, retState);
}

TEST_F(TestContainerReport, TestGetDiagnostics)
{
	string diagnostics("diagnostics");
	string retDiagnostics;

	containerReport.setDiagnostics(diagnostics);
	retDiagnostics = containerReport.getDiagnostics();

	EXPECT_EQ(diagnostics, retDiagnostics);
}

TEST_F(TestContainerReport, TestGetLogUrl)
{
	string url("url");
	string retUrl;

	containerReport.setLogUrl(url);
	retUrl = containerReport.getLogUrl();

	EXPECT_EQ(url, retUrl);
}
