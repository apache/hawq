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

#include "records/ApplicationResourceUsageReport.h"

using namespace libyarn;
using namespace testing;

class TestApplicationResourceUsageReport: public ::testing::Test {
protected:
	ApplicationResourceUsageReport applicationReport;
};

TEST_F(TestApplicationResourceUsageReport, TestGetNumUsedContainers)
{
	int32_t num = -1;
	int32_t retNum;

	applicationReport.setNumUsedContainers(num);
	retNum = applicationReport.getNumUsedContainers();

	EXPECT_EQ(num, retNum);
}

TEST_F(TestApplicationResourceUsageReport, TestGetNumReservedContainers)
{
	int32_t num = -1;
	int32_t retNum;

	applicationReport.setNumReservedContainers(num);
	retNum = applicationReport.getNumReservedContainers();

	EXPECT_EQ(num, retNum);
}

TEST_F(TestApplicationResourceUsageReport, TestGetUsedResources)
{
	Resource resource;
	Resource retResource;

	applicationReport.setUsedResources(resource);
	retResource = applicationReport.getUsedResources();


	/* The operator '==' is not defined for class Resource,
	   so we can't compare resource and retResource here. */

	SUCCEED();
}

TEST_F(TestApplicationResourceUsageReport, TestGetReservedResources)
{
	Resource resource;
	Resource retResource;

	applicationReport.setReservedResources(resource);
	retResource = applicationReport.getReservedResources();


	/* The operator '==' is not defined for class Resource,
	   so we can't compare resource and retResource here. */

	SUCCEED();
}

TEST_F(TestApplicationResourceUsageReport, TestGetNeededResources)
{
	Resource resource;
	Resource retResource;

	applicationReport.setNeededResources(resource);
	retResource = applicationReport.getNeededResources();


	/* The operator '==' is not defined for class Resource,
	   so we can't compare resource and retResource here. */

	SUCCEED();
}
