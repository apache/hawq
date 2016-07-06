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

#include "records/URL.h"

using std::string;
using namespace libyarn;
using namespace testing;

class TestURL: public ::testing::Test {
protected:
	URL url;
};

TEST_F(TestURL, TestGetScheme)
{
	string scheme("scheme");
	string retScheme;

	url.setScheme(scheme);
	retScheme = url.getScheme();

	EXPECT_EQ(scheme, retScheme);
}

TEST_F(TestURL, TestGetHost)
{
	string host("host");
	string retHost;

	url.setHost(host);
	retHost = url.getHost();

	EXPECT_EQ(host, retHost);
}

TEST_F(TestURL, TestGetPort)
{
	int32_t port = -1;
	int32_t retPort;

	url.setPort(port);
	retPort = url.getPort();

	EXPECT_EQ(port, retPort);
}

TEST_F(TestURL, TestGetFile)
{
	string file("file");
	string retFile;

	url.setFile(file);
	retFile = url.getFile();

	EXPECT_EQ(file, retFile);
}

TEST_F(TestURL, TestGetUserInfo)
{
	string userInfo("userInfo");
	string retUserInfo;

	url.setUserInfo(userInfo);
	retUserInfo = url.getUserInfo();

	EXPECT_EQ(userInfo, retUserInfo);
}
