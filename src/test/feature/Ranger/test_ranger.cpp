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

#include "test_ranger.h"

#include <string>
#include <pwd.h>

#include "lib/command.h"
#include "lib/gpfdist.h"
#include "lib/sql_util.h"
#include "lib/string_util.h"

using std::vector;
using std::string;
using hawq::test::SQLUtility;
using hawq::test::Command;

TEST_F(TestHawqRanger, BasicTest) {
    SQLUtility util;

    if (util.getGUCValue("hawq_acl_type") == "ranger")
    {
		hawq::test::GPfdist gpdfist(&util);
		gpdfist.init_gpfdist();

		string rootPath(util.getTestRootPath());
		string initfile = hawq::test::stringFormat("Ranger/sql/init_file");
		auto cmd = hawq::test::stringFormat("ls -l %s/Ranger/sql/normal/*.sql 2>/dev/null | wc -l", rootPath.c_str());
		int sql_num = std::atoi(Command::getCommandOutput(cmd).c_str());
		int writableTableCase = 28;
		string rangerHost = RANGER_HOST;
		cmd = hawq::test::stringFormat("cp %s/Ranger/data/copydata.txt /tmp/a.txt", rootPath.c_str());
		Command::getCommandStatus(cmd);

		// clear environment
		for (int i = 1; i <= sql_num; i++) {
			// delete user_num
			std::string normalusername = hawq::test::stringFormat("usertest%d", i);
			std::string superusername = hawq::test::stringFormat("usersuper%d", i);
			util.execute(hawq::test::stringFormat("drop role %s;",normalusername.c_str()), false);
			util.execute(hawq::test::stringFormat("drop role %s;",superusername.c_str()), false);
			// delete policy
			std::string cmd = hawq::test::stringFormat("ls -l %s/Ranger/policy/%d/ 2>/dev/null| grep \"^-\" | wc -l ", rootPath.c_str(), i);
			int policy_num = std::atoi(Command::getCommandOutput(cmd).c_str());
			for (int j = 1; j <= policy_num; j++) {
				cmd = hawq::test::stringFormat("python %s/Ranger/rangerpolicy.py -h %s -d policy%d-%d", rootPath.c_str(), rangerHost.c_str(), i, j);
				Command::getCommandStatus(cmd);
			}
		}

		for (int i = 1; i <= sql_num; i++) {
			// create user_num
			std::string superusername = hawq::test::stringFormat("usersuper%d", i);;
			std::string normalusername = hawq::test::stringFormat("usertest%d", i);;
			util.execute(hawq::test::stringFormat("create role %s with login createdb superuser;", superusername.c_str()),true);
			if(i == writableTableCase) { //for writable external table
				util.execute(hawq::test::stringFormat("create role %s with login createdb CREATEEXTTABLE(type='writable') CREATEROLE;", normalusername.c_str()),true);
			}
			else {
				util.execute(hawq::test::stringFormat("create role %s with login createdb CREATEEXTTABLE CREATEROLE;", normalusername.c_str()),true);
			}
			cmd = hawq::test::stringFormat("python %s/Ranger/rangeruser.py -h %s -u %s,%s", rootPath.c_str(),
					rangerHost.c_str(),normalusername.c_str(), superusername.c_str());
			Command::getCommandStatus(cmd);

			//run sql by different users
			string normal_sqlfile = hawq::test::stringFormat("Ranger/sql/normal/%d.sql", i);
			string super_sqlfile = hawq::test::stringFormat("Ranger/sql/super/%d.sql", i);
			string admin_sqlfile = hawq::test::stringFormat("Ranger/sql/admin/%d.sql", i);
			string normal_ansfile_fail = hawq::test::stringFormat("Ranger/ans/normal%d_fail.ans", i);
			string super_ansfile_fail = hawq::test::stringFormat("Ranger/ans/super%d_fail.ans", i);
			string admin_ansfile = hawq::test::stringFormat("Ranger/ans/adminfirst%d.ans", i);

			cmd = hawq::test::stringFormat("ls -l %s/Ranger/policy/%d/ 2>/dev/null| wc -l", rootPath.c_str(), i);
			int policy_num = std::atoi(Command::getCommandOutput(cmd).c_str());

			cmd = hawq::test::stringFormat("ls -l %s/Ranger/sql/super/%d.sql 2>/dev/null| wc -l", rootPath.c_str(), i);
			int supersqlexist = std::atoi(Command::getCommandOutput(cmd).c_str());

			if (policy_num > 0){
				if (supersqlexist) {
					util.execSQLFile(super_sqlfile, super_ansfile_fail, initfile, true, true);
				}
				else {
					util.execSQLFile(normal_sqlfile, normal_ansfile_fail, initfile, true, true);
				}
			}

			util.execSQLFile(admin_sqlfile, admin_ansfile, initfile, true, true);


			for (int j = 1; j <= policy_num; j++) {
				cmd = hawq::test::stringFormat("python %s/Ranger/rangerpolicy.py -h %s -a %s/Ranger/policy/%d/%d.json", rootPath.c_str(), rangerHost.c_str(), rootPath.c_str(), i, j);
				Command::getCommandStatus(cmd);
			}
		}
		sleep(60);


		for (int i = 1; i <= sql_num; i++) {
			//run sql by different users
			string normal_sqlfile = hawq::test::stringFormat("Ranger/sql/normal/%d.sql", i);
			string super_sqlfile = hawq::test::stringFormat("Ranger/sql/super/%d.sql", i);
			string normal_ansfile_success = hawq::test::stringFormat("Ranger/ans/normal%d_success.ans", i);
			string super_ansfile_success = hawq::test::stringFormat("Ranger/ans/super%d_success.ans", i);


			cmd = hawq::test::stringFormat("ls -l %s/Ranger/policy/%d/ 2>/dev/null| wc -l", rootPath.c_str(), i);
			int policy_num = std::atoi(Command::getCommandOutput(cmd).c_str());
			cmd = hawq::test::stringFormat("ls -l %s/Ranger/sql/super/%d.sql 2>/dev/null | wc -l", rootPath.c_str(), i);
			int supersqlexist = std::atoi(Command::getCommandOutput(cmd).c_str());
			util.execSQLFile(normal_sqlfile, normal_ansfile_success, initfile, true, true);
			if (supersqlexist) {
				util.execSQLFile(super_sqlfile, super_ansfile_success, initfile, true, true);
			}
		}

		//using gpadmin to clear database environment.
		for (int i = 1; i <= sql_num; i++) {
			string admin_sqlfile = hawq::test::stringFormat("Ranger/sql/admin/%d.sql", i);
			string admin_ansfile = hawq::test::stringFormat("Ranger/ans/adminsecond%d.ans", i);
			util.execSQLFile(admin_sqlfile, admin_ansfile, initfile, true, true);
		}

		for (int i = 1; i <= sql_num; i++) {
			// delete user_num
			std::string normalusername = hawq::test::stringFormat("usertest%d", i);
			std::string superusername = hawq::test::stringFormat("usersuper%d", i);
			util.execute(hawq::test::stringFormat("drop role %s;",normalusername.c_str()), false);
			util.execute(hawq::test::stringFormat("drop role %s;",superusername.c_str()), false);
			// delete policy
			std::string cmd = hawq::test::stringFormat("ls -l %s/Ranger/policy/%d/ 2>/dev/null | wc -l", rootPath.c_str(), i);
			int policy_num = std::atoi(Command::getCommandOutput(cmd).c_str());
			for (int j = 1; j <= policy_num; j++) {
				cmd = hawq::test::stringFormat("python %s/Ranger/rangerpolicy.py -h %s -d policy%d-%d", rootPath.c_str(), rangerHost.c_str(), i, j);
				Command::getCommandStatus(cmd);
			}
		}

		gpdfist.finalize_gpfdist();
    }
}
