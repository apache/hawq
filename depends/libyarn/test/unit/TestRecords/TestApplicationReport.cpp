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

#include "records/ApplicationReport.h"

using std::string;
using namespace libyarn;
using namespace testing;

class TestApplicationReport: public ::testing::Test {
protected:
	ApplicationReport applicationReport;
};

TEST_F(TestApplicationReport, TestGetClientToAMToken)
{
	Token token;
	Token retToken;

	applicationReport.setClientToAMToken(token);
	retToken = applicationReport.getClientToAMToken();

	/* The operator '==' is not defined for class Token,
	   so we can't compare token and retToken here. */

	SUCCEED();
}

TEST_F(TestApplicationReport, TestGetTrackingUrl)
{
	string url("url");
	string retUrl;

	applicationReport.setTrackingUrl(url);
	retUrl = applicationReport.getTrackingUrl();

	EXPECT_EQ(url, retUrl);
}

TEST_F(TestApplicationReport, TestGetDiagnostics)
{
	string diagnostics("diagnostics");
	string retDiagnostics;

	applicationReport.setDiagnostics(diagnostics);
	retDiagnostics = applicationReport.getDiagnostics();

	EXPECT_EQ(diagnostics, retDiagnostics);
}

TEST_F(TestApplicationReport, TestGetStartTime)
{
	int64_t time = -1;
	int64_t retTime;

	applicationReport.setStartTime(time);
	retTime = applicationReport.getStartTime();

	EXPECT_EQ(time, retTime);
}

TEST_F(TestApplicationReport, TestGetFinishTime)
{
	int64_t time = -1;
	int64_t retTime;

	applicationReport.setFinishTime(time);
	retTime = applicationReport.getFinishTime();

	EXPECT_EQ(time, retTime);
}

TEST_F(TestApplicationReport, TestGetFinalApplicationStatus)
{
	FinalApplicationStatus status = FinalApplicationStatus::APP_SUCCEEDED;
	FinalApplicationStatus retStatus;

	applicationReport.setFinalApplicationStatus(status);
	retStatus = applicationReport.getFinalApplicationStatus();

	EXPECT_EQ(status, retStatus);
}

TEST_F(TestApplicationReport, TestGetApplicationResourceUsage)
{
	ApplicationResourceUsageReport usage;
	ApplicationResourceUsageReport retUsage;

	applicationReport.setAppResourceUsage(usage);
	retUsage = applicationReport.getAppResourceUsage();

	/* The operator '==' is not defined for class ApplicationResourceUsageReport,
	   so we can't compare usage and retUsage here. */

	SUCCEED();
}

TEST_F(TestApplicationReport, TestGetOriginalTrackingUrl)
{
	string url("url");
	string retUrl;

	applicationReport.setOriginalTrackingUrl(url);
	retUrl = applicationReport.getOriginalTrackingUrl();

	EXPECT_EQ(url, retUrl);
}

TEST_F(TestApplicationReport, TestGetCurrentAppAttemptId)
{
	ApplicationAttemptId id;
	ApplicationAttemptId retId;

	applicationReport.setCurrentAppAttemptId(id);
	retId = applicationReport.getCurrentAppAttemptId();

	/* The operator '==' is not defined for class ApplicationAttemptId,
	   so we can't compare id and retId here. */

	SUCCEED();
}

TEST_F(TestApplicationReport, TestGetApplicationType)
{
	string type("type");
	string retType;

	applicationReport.setApplicationType(type);
	retType = applicationReport.getApplicationType();

	EXPECT_EQ(type, retType);
}
