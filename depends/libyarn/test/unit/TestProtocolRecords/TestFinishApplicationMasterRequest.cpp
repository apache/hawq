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

#include "protocolrecords/FinishApplicationMasterRequest.h"

using std::string;
using namespace libyarn;
using namespace testing;

class TestFinishApplicationMasterRequest: public ::testing::Test {
protected:
	FinishApplicationMasterRequest finishRequest;
};

TEST_F(TestFinishApplicationMasterRequest, TestFinishApplicationMasterRequest)
{
	FinishApplicationMasterRequestProto finishRequestProto;
	finishRequest = FinishApplicationMasterRequest(finishRequestProto);

	SUCCEED();
}

TEST_F(TestFinishApplicationMasterRequest, TestGetDiagnostics)
{
	string diagnostics("diagnostics");
	string retDiagnostics;
	finishRequest.setDiagnostics(diagnostics);
	retDiagnostics = finishRequest.getDiagnostics();

	EXPECT_EQ(diagnostics, retDiagnostics);
}

TEST_F(TestFinishApplicationMasterRequest, TestGetTrackingUrl)
{
	string trackingUrl("trackingUrl");
	string retTrackingUrl;
	finishRequest.setTrackingUrl(trackingUrl);
	retTrackingUrl = finishRequest.getTrackingUrl();

	EXPECT_EQ(trackingUrl, retTrackingUrl);
}

TEST_F(TestFinishApplicationMasterRequest, TestFinalApplicationStatus)
{
	FinalApplicationStatus finalStatus;
	finalStatus = finishRequest.getFinalApplicationStatus();

	SUCCEED();
}
