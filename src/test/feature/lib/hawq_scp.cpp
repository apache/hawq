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

#include "command.h"
#include "sql_util.h"
#include "hawq_scp.h"

using std::string;

namespace hawq {
namespace test {

bool HAWQScp::copy(const string& host_list, const string& src_file, const string& dst_dir)
{
	Command cmd("hawq scp " + host_list + " " + src_file + " =:" + dst_dir);
	if (cmd.run().getResultStatus() == 0)
	{
		return true;
	}
	else
	{
		return false;
	}
}

} // namespace test
} // namespace hawq
