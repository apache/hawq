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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#ifndef SRC_TEST_FEATURE_LIB_HAWQ_SCP_H_
#define SRC_TEST_FEATURE_LIB_HAWQ_SCP_H_

#include <string>

namespace hawq {
namespace test {

class HAWQScp
{
public:
	HAWQScp() = default;
	~HAWQScp() = default;
	HAWQScp(const HAWQScp&) = delete;
	HAWQScp& operator=(const HAWQScp&) = delete;

	bool copy(const std::string& host_list, const std::string& src_file, const std::string& dst_dir);
};

} // namespace test
} // namespace hawq

#endif   // SRC_TEST_FEATURE_LIB_HAWQ_SCP_H_
