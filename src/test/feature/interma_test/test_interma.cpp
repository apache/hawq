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
#include <iostream>
#include <unistd.h>
#include <fcntl.h>
#include <unistd.h>
#include "gtest/gtest.h"

//interma test
using namespace std;

class TestInterma : public ::testing::Test {
public:
	TestInterma() 
	{}

	string get_test_schema_name() 
	{
		const ::testing::TestInfo *const test_info = ::testing::UnitTest::GetInstance()->current_test_info();
		return string(test_info->test_case_name()) + "_" + test_info->name();
	}
};

void static print_msg(const string &msg)
{
	const char * msg_str = msg.c_str();
	int fd = open("/tmp/feature-test.log", O_CREAT|O_WRONLY|O_APPEND);
	write(fd, msg_str, strlen(msg_str));
	close (fd);
	cerr<<msg<<endl;
}

//test S & P order
//RESULT: S1 beginS1 endS2 beginS2 endP1 beginP2 beginP2 endP1 end
TEST_F(TestInterma, S1Test) {
	print_msg("S1 begin");
	sleep(10);
	print_msg("S1 end");
}
TEST_F(TestInterma, S2Test) {
	print_msg("S2 begin");
	sleep(20);
	print_msg("S2 end");
}
TEST_F(TestInterma, P1Test) {
	print_msg("P1 begin");
	sleep(15);
	print_msg("P1 end");
}
TEST_F(TestInterma, P2Test) {
	print_msg("P2 begin");
	sleep(10);
	print_msg("P2 end");
}


TEST_F(TestInterma, SchemaNameTest) {
	string data = get_test_schema_name();
	std::transform(data.begin(), data.end(), data.begin(), ::tolower);
	print_msg(data);
}

