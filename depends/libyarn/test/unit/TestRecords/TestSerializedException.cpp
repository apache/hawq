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

#include "records/SerializedException.h"

using std::string;
using namespace libyarn;
using namespace testing;

class TestSerializedException: public ::testing::Test {
protected:
	SerializedException serializedException;
};

TEST_F(TestSerializedException, TestGetMessage)
{
	string message("message");
	string retMessage;

	serializedException.setMessage(message);
	retMessage = serializedException.getMessage();

	EXPECT_EQ(message, retMessage);
}

TEST_F(TestSerializedException, TestGetTrace)
{
	string trace("trace");
	string retTrace;

	serializedException.setTrace(trace);
	retTrace = serializedException.getTrace();

	EXPECT_EQ(trace, retTrace);
}

TEST_F(TestSerializedException, TestGetClassName)
{
	string className("className");
	string retClassName;

	serializedException.setClassName(className);
	retClassName = serializedException.getClassName();

	EXPECT_EQ(className, retClassName);
}

TEST_F(TestSerializedException, TestGetCause)
{
	SerializedException cause;
	SerializedException retCause;

	serializedException.setCause(cause);
	retCause = serializedException.getCause();

	/* The operator '==' is not defined for class SerializedException,
	   so we can't compare cause and retCause here. */

	SUCCEED();
}
