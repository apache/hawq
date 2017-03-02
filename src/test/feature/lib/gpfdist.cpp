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

#include <fstream>
#include "gpfdist.h"

using std::string;

namespace hawq {
namespace test {
void GPfdist::init_gpfdist() {
	auto sql =
			"CREATE EXTERNAL WEB TABLE gpfdist_status (x text) "
					"execute E'( python %s/bin/lib/gppinggpfdist.py localhost:7070 2>&1 || echo) ' "
					"on SEGMENT 0 "
					"FORMAT 'text' (delimiter '|');";
	auto GPHOME = getenv("GPHOME");
	util->execute(hawq::test::stringFormat(sql, GPHOME));

	sql =
			"CREATE EXTERNAL WEB TABLE gpfdist_start (x text) "
					"execute E'((%s/bin/gpfdist -p 7070 -d %s  </dev/null >/dev/null 2>&1 &); sleep 2; echo \"starting\"...) ' "
					"on SEGMENT 0 "
					"FORMAT 'text' (delimiter '|');";
	std::string path = util->getTestRootPath() + "/ExternalSource/data";
	util->execute(hawq::test::stringFormat(sql, GPHOME, path.c_str()));

	util->execute(
			"CREATE EXTERNAL WEB TABLE gpfdist_stop (x text) "
					"execute E'(pkill gpfdist || killall gpfdist) > /dev/null 2>&1; echo stopping...' "
					"on SEGMENT 0 "
					"FORMAT 'text' (delimiter '|');");
	util->execute("select * from gpfdist_stop;");
	util->execute("select * from gpfdist_status;");
	util->execute("select * from gpfdist_start;");
	util->execute("select * from gpfdist_status;");
}

void GPfdist::finalize_gpfdist() {
	util->execute("select * from gpfdist_stop;");
	util->execute("select * from gpfdist_status;");
	util->execute("drop external table gpfdist_status;");
	util->execute("drop external table gpfdist_start;");
	util->execute("drop external table gpfdist_stop;");

}

} // namespace test
} // namespace hawq
