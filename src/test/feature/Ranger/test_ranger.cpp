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
#include "lib/string_util.h"

using std::vector;
using std::string;
using hawq::test::SQLUtility;
using hawq::test::Command;

TestHawqRanger::TestHawqRanger()
{
	initfile = hawq::test::stringFormat("Ranger/sql/init_file");
	rangerHost = RANGER_HOST;
}

TEST_F(TestHawqRanger, BasicTest) {
	SQLUtility util;

	if (util.getGUCValue("hawq_acl_type") == "ranger")
	{
		hawq::test::GPfdist gpdfist(&util);
		gpdfist.init_gpfdist();

		string rootPath(util.getTestRootPath());
		auto cmd = hawq::test::stringFormat("ls -l %s/Ranger/sql/normal/*.sql 2>/dev/null | grep \"^-\" | wc -l", rootPath.c_str());
		int sql_num = std::atoi(Command::getCommandOutput(cmd).c_str());
		int writableTableCase = 28;
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

			cmd = hawq::test::stringFormat("ls -l %s/Ranger/policy/%d/ 2>/dev/null| grep \"^-\" | wc -l", rootPath.c_str(), i);
			int policy_num = std::atoi(Command::getCommandOutput(cmd).c_str());

			cmd = hawq::test::stringFormat("ls -l %s/Ranger/sql/super/%d.sql 2>/dev/null| grep \"^-\" | wc -l", rootPath.c_str(), i);
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

			cmd = hawq::test::stringFormat("ls -l %s/Ranger/policy/%d/ 2>/dev/null| grep \"^-\" | wc -l", rootPath.c_str(), i);
			int policy_num = std::atoi(Command::getCommandOutput(cmd).c_str());

			cmd = hawq::test::stringFormat("ls -l %s/Ranger/sql/super/%d.sql 2>/dev/null | grep \"^-\" | wc -l", rootPath.c_str(), i);
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
			std::string cmd = hawq::test::stringFormat("ls -l %s/Ranger/policy/%d/ 2>/dev/null | grep \"^-\" | wc -l", rootPath.c_str(), i);
			int policy_num = std::atoi(Command::getCommandOutput(cmd).c_str());
			for (int j = 1; j <= policy_num; j++) {
				cmd = hawq::test::stringFormat("python %s/Ranger/rangerpolicy.py -h %s -d policy%d-%d", rootPath.c_str(), rangerHost.c_str(), i, j);
				Command::getCommandStatus(cmd);
			}
		}

		gpdfist.finalize_gpfdist();
	}
}

static void clear_env(SQLUtility &util, int sql_id, string rootPath, string rangerHost)
{
	int i = sql_id;
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

TEST_F(TestHawqRanger, FallbackTest) {
	SQLUtility util;

	if (util.getGUCValue("hawq_acl_type") == "ranger")
	{
		string rootPath(util.getTestRootPath());
		string rangerHost = RANGER_HOST;
		string initfile = "Ranger/sql/init_file";
		string cmd;
		int FallbackCase = 10000;
		int i = FallbackCase;

		// clear environment
		clear_env(util, i, rootPath, rangerHost);

		// create user_num
		std::string normalusername = hawq::test::stringFormat("usertest%d", i);;
		std::string superusername = hawq::test::stringFormat("usersuper%d", i);;
		util.execute(hawq::test::stringFormat("create role %s with login createdb;", normalusername.c_str()),true);
		util.execute(hawq::test::stringFormat("create role %s with login createdb superuser;", superusername.c_str()),true);
		// add user
		cmd = hawq::test::stringFormat("python %s/Ranger/rangeruser.py -h %s -u %s,%s", rootPath.c_str(),
			rangerHost.c_str(), normalusername.c_str(), superusername.c_str());
		Command::getCommandStatus(cmd);
		// add policy
		cmd = hawq::test::stringFormat("python %s/Ranger/rangerpolicy.py -h %s -a %s/Ranger/policy/%d/%d.json", rootPath.c_str(), rangerHost.c_str(), rootPath.c_str(), i, 1);
		Command::getCommandStatus(cmd);
		sleep(60);

		// run sql test
		string normal_sqlfile = hawq::test::stringFormat("Ranger/sql/manual/%d.sql", i);
		string normal_ansfile_success = hawq::test::stringFormat("Ranger/ans/normal%d_success.ans", i);
		util.execSQLFile(normal_sqlfile, normal_ansfile_success, initfile, true, true);

		clear_env(util, i, rootPath, rangerHost);
    }
}

TEST_F(TestHawqRanger, DenyTest) {
	SQLUtility util;
	if (util.getGUCValue("hawq_acl_type") == "ranger")
	{
		clearEnv(&util, "deny", 1);
		util.execute("create table a(i int);");
		addUser(&util, "deny", 1, true);
		runSQLFile(&util, "deny", "succeed", 1);
		addPolicy(&util, "deny", 1);
		runSQLFile(&util, "deny", "fail", 1);
	}
}


TEST_F(TestHawqRanger, DenyExcludeTest) {
	SQLUtility util;
	if (util.getGUCValue("hawq_acl_type") == "ranger")
	{
		clearEnv(&util, "denyexclude", 2);
		clearEnv(&util, "deny", 2);
		util.execute("create table a(i int);");
		addUser(&util, "denyexclude", 2, true);
		runSQLFile(&util, "denyexclude", "succeed", 2);
		addPolicy(&util, "deny", 2);
		runSQLFile(&util, "denyexclude", "fail", 2);
		addPolicy(&util, "denyexclude", 2);
		runSQLFile(&util, "denyexclude", "succeed2", 2);
	}
}

TEST_F(TestHawqRanger, AllowExcludeTest) {
	SQLUtility util;
	if (util.getGUCValue("hawq_acl_type") == "ranger")
	{
		clearEnv(&util, "allowexclude", 3);
		clearEnv(&util, "allow", 3);
		util.execute("create table a(i int);");
		addUser(&util, "allowexclude", 3, false);

		addPolicy(&util, "allow", 3);
		runSQLFile(&util, "allowexclude", "succeed", 3);

		addPolicy(&util, "allowexclude", 3);
		runSQLFile(&util, "allowexclude", "fail", 3);
	}
}

TEST_F(TestHawqRanger, ResourceExcludeTest) {
	SQLUtility util;
	if (util.getGUCValue("hawq_acl_type") == "ranger")
	{
		clearEnv(&util, "resourceexclude", 4);
		clearEnv(&util, "allow", 4);
		util.execute("create table a(i int);");
		util.execute("create table b(i int);");
		addUser(&util, "resourceexclude", 4, false);

		addPolicy(&util, "resourceexclude", 4);
		// select a fail, select b succeed
		runSQLFile(&util, "resourceexclude", "fail", 4);

		//add usage-schema to public
		addPolicy(&util, "allow", 4);
		runSQLFile(&util, "resourceexclude", "succeed", 4);
	}
}

TEST_F(TestHawqRanger, ResourceExcludeStarTest) {
	SQLUtility util;
	if (util.getGUCValue("hawq_acl_type") == "ranger")
	{
		clearEnv(&util, "resourceexclude", 5);
		clearEnv(&util, "allow", 5);
		util.execute("create table a(i int);");
		addUser(&util, "resourceexclude", 5, false);

		addPolicy(&util, "resourceexclude", 5);
		// fail in select table a , succeed in select table b
		runSQLFile(&util, "resourceexclude", "fail", 5);

		//add usage-schema to public
		addPolicy(&util, "allow", 5);
		runSQLFile(&util, "resourceexclude", "fail2", 5);
	}
}

TEST_F(TestHawqRanger, ResourceIncludeATest) {
	SQLUtility util;
	if (util.getGUCValue("hawq_acl_type") == "ranger")
	{
		clearEnv(&util, "allow", 6);
		util.execute("create table a(i int);");
		addUser(&util, "allow", 6, false);

		addPolicy(&util, "allow", 6);
		runSQLFile(&util, "allow", "fail", 6);
	}
}

TEST_F(TestHawqRanger, HcatalogTest) {
	SQLUtility util;
	if (util.getGUCValue("hawq_acl_type") == "ranger")
	{
		/*
		 * create a table in hive and populate some rows
		 */
		clearEnv(&util, "pxf", 1);
		clearEnv(&util, "pxf", 2);
		clearEnv(&util, "pxf", 3);
		string rootPath(util.getTestRootPath());
		string sqlPath = rootPath + "/Ranger/data/testhive.sql";
		auto cmd =  hawq::test::stringFormat("hive -f %s", sqlPath.c_str());
		Command::getCommandStatus(cmd);

		/*
		 * create a user and query this table, fail.
		 */
		addUser(&util, "pxf", 1, false);
		runSQLFile(&util, "pxf", "fail", 1);

		/*
		 * add allow policies for this user and query again, succeed.
		 */
		addPolicy(&util, "pxf", 1); // usage of default
		addPolicy(&util, "pxf", 2); // select of table
		addPolicy(&util, "pxf", 3); // usage of current schema(e.g.testhawqranger_hcatalogtest)
		runSQLFile(&util, "pxf", "success", 1);
	}
}

void TestHawqRanger::addUser(hawq::test::SQLUtility* util, std::string case_name, int user_index, bool full_policy, int writable_index)
{
	string rootPath = util->getTestRootPath();
	string cmd = "";
	if (user_index == -1)
	{
		cmd = hawq::test::stringFormat("ls -l %s/Ranger/sql/%s/*.sql 2>/dev/null| grep \"^-\" | wc -l", rootPath.c_str(), case_name.c_str());
		int sql_num = std::atoi(Command::getCommandOutput(cmd).c_str());
		for (int i = 1; i <= sql_num; i++) {
			// create user_num
			std::string denyusername = hawq::test::stringFormat("user%s%d", case_name.c_str() ,i);
			util->execute(hawq::test::stringFormat("create role %s with login createdb CREATEEXTTABLE CREATEROLE;", denyusername.c_str()),true);
			if (full_policy)
			{
				cmd = hawq::test::stringFormat("python %s/Ranger/rangeruser.py -h %s -u %s -f True", rootPath.c_str(),
								rangerHost.c_str(),denyusername.c_str());
			} else {
				cmd = hawq::test::stringFormat("python %s/Ranger/rangeruser.py -h %s -u %s", rootPath.c_str(),
								rangerHost.c_str(),denyusername.c_str());
			}
			Command::getCommandStatus(cmd);
			if (full_policy)
			{
				sleep(60);
			}
		}
	} else {
		std::string denyusername = hawq::test::stringFormat("user%s%d", case_name.c_str() ,user_index);
		util->execute(hawq::test::stringFormat("create role %s with login createdb CREATEEXTTABLE CREATEROLE;", denyusername.c_str()),true);
		if (full_policy)
		{
			cmd = hawq::test::stringFormat("python %s/Ranger/rangeruser.py -h %s -u %s -f True", rootPath.c_str(),
							rangerHost.c_str(),denyusername.c_str());
		} else
		{
			cmd = hawq::test::stringFormat("python %s/Ranger/rangeruser.py -h %s -u %s", rootPath.c_str(),
							rangerHost.c_str(),denyusername.c_str());
		}
		Command::getCommandStatus(cmd);
		if (full_policy)
		{
			sleep(60);
		}
	}
}

void TestHawqRanger::clearEnv(hawq::test::SQLUtility* util, std::string case_name, int user_index)
{
	string rootPath = util->getTestRootPath();
	string cmd = "";
	if (user_index == -1)
	{
		cmd = hawq::test::stringFormat("ls -l %s/Ranger/sql/%s/*.sql 2>/dev/null| grep \"^-\" | wc -l", rootPath.c_str(), case_name.c_str());
		int sql_num = std::atoi(Command::getCommandOutput(cmd).c_str());
		for (int i = 1; i <= sql_num; i++) {
			// delete user_num
			std::string denyusername = hawq::test::stringFormat("user%s%d", case_name.c_str(), i);
			util->execute(hawq::test::stringFormat("drop role %s;",denyusername.c_str()), false);
			// delete policy
			std::string cmd = hawq::test::stringFormat("ls -l %s/Ranger/%spolicy/%d/ 2>/dev/null| grep \"^-\" | wc -l ", rootPath.c_str(), case_name.c_str(), i);
			int policy_num = std::atoi(Command::getCommandOutput(cmd).c_str());
			for (int j = 1; j <= policy_num; j++) {
				cmd = hawq::test::stringFormat("python %s/Ranger/rangerpolicy.py -h %s -d %spolicy%d-%d", rootPath.c_str(), rangerHost.c_str(), case_name.c_str(), i, j);
				Command::getCommandStatus(cmd);
			}
		}
	} else {
		// delete user_num
		std::string denyusername = hawq::test::stringFormat("user%s%d", case_name.c_str(), user_index);
		util->execute(hawq::test::stringFormat("drop role %s;",denyusername.c_str()), false);
		// delete policy
		std::string cmd = hawq::test::stringFormat("ls -l %s/Ranger/%spolicy/%d/ 2>/dev/null| grep \"^-\" | wc -l ", rootPath.c_str(), case_name.c_str(), user_index);
		int policy_num = std::atoi(Command::getCommandOutput(cmd).c_str());
		for (int j = 1; j <= policy_num; j++) {
			cmd = hawq::test::stringFormat("python %s/Ranger/rangerpolicy.py -h %s -d %spolicy%d-%d", rootPath.c_str(), rangerHost.c_str(), case_name.c_str(), user_index, j);
			Command::getCommandStatus(cmd);
		}
	}
}

void TestHawqRanger::runSQLFile(hawq::test::SQLUtility* util, std::string case_name, std::string ans_suffix, int sql_index)
{
	string rootPath = util->getTestRootPath();
	auto cmd = hawq::test::stringFormat("ls -l %s/Ranger/sql/%s/*.sql 2>/dev/null| grep \"^-\" | wc -l", rootPath.c_str(), case_name.c_str());

	// run all the sql files in folder.
	if(sql_index == -1)
	{
		int sql_num = std::atoi(Command::getCommandOutput(cmd).c_str());
		for (int i = 1; i <= sql_num; i++) {
			string deny_sqlfile = hawq::test::stringFormat("Ranger/sql/%s/%d.sql", i, case_name.c_str());
			string deny_ansfile_succeed = hawq::test::stringFormat("Ranger/ans/%s%d_%s.ans", case_name.c_str(), i, ans_suffix.c_str());

			auto cmd = hawq::test::stringFormat("ls -l %s/Ranger/%spolicy/%d/ 2>/dev/null| grep \"^-\"| wc -l", rootPath.c_str(), case_name.c_str(), i);
			int policy_num = std::atoi(Command::getCommandOutput(cmd).c_str());

			if (policy_num > 0){
				util->execSQLFile(deny_sqlfile, deny_ansfile_succeed, initfile);
			}
		}
	} else {
		string deny_sqlfile = hawq::test::stringFormat("Ranger/sql/%s/%d.sql", case_name.c_str(), sql_index);
		string deny_ansfile_succeed = hawq::test::stringFormat("Ranger/ans/%s%d_%s.ans", case_name.c_str(), sql_index, ans_suffix.c_str());

		auto cmd = hawq::test::stringFormat("ls -l %s/Ranger/%spolicy/%d/ 2>/dev/null| grep \"^-\"| wc -l", rootPath.c_str(), case_name.c_str(), sql_index);
		int policy_num = std::atoi(Command::getCommandOutput(cmd).c_str());

		if (policy_num > 0){
			util->execSQLFile(deny_sqlfile, deny_ansfile_succeed, initfile);
		}
	}
}

void TestHawqRanger::addPolicy(hawq::test::SQLUtility* util, std::string case_name, int policy_index)
{
	string rootPath = util->getTestRootPath();
	auto cmd = hawq::test::stringFormat("ls -l %s/Ranger/sql/%s/*.sql 2>/dev/null| grep \"^-\" | wc -l", rootPath.c_str(), case_name.c_str());

	if (policy_index == -1)
	{
		int sql_num = std::atoi(Command::getCommandOutput(cmd).c_str());
		for (int i = 1; i <= sql_num; i++) {
			auto cmd = hawq::test::stringFormat("ls -l %s/Ranger/%spolicy/%d/ 2>/dev/null| grep \"^-\"| wc -l", rootPath.c_str(), case_name.c_str(), i);
			int policy_num = std::atoi(Command::getCommandOutput(cmd).c_str());

			for (int j = 1; j <= policy_num; j++) {
				cmd = hawq::test::stringFormat("python %s/Ranger/rangerpolicy.py -h %s -a %s/Ranger/%spolicy/%d/%d.json", rootPath.c_str(), rangerHost.c_str(), rootPath.c_str(), case_name.c_str(), i, j);
				Command::getCommandStatus(cmd);
			}
		}
	} else {
		auto cmd = hawq::test::stringFormat("ls -l %s/Ranger/%spolicy/%d/ 2>/dev/null| grep \"^-\"| wc -l", rootPath.c_str(), case_name.c_str(), policy_index);
		int policy_num = std::atoi(Command::getCommandOutput(cmd).c_str());
		for (int j = 1; j <= policy_num; j++) {
			cmd = hawq::test::stringFormat("python %s/Ranger/rangerpolicy.py -h %s -a %s/Ranger/%spolicy/%d/%d.json", rootPath.c_str(), rangerHost.c_str(), rootPath.c_str(), case_name.c_str(), policy_index, j);
			Command::getCommandStatus(cmd);
		}
	}
	sleep(60);
}
