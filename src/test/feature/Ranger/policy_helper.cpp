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
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <fcntl.h>
#include "policy_helper.h"
#include "lib/command.h"
#include "lib/string_util.h"

using namespace std;

#define TMP_FOLDER "/tmp/ranger_test/"


PolicyHelper::PolicyHelper (string root_path, string ranger_host): _root_path (root_path), _rangerhost(ranger_host)
{}

PolicyHelper::~PolicyHelper ()
{}

void PolicyHelper::Reset() 
{
	_policys.clear();
}

int PolicyHelper::add_common_tsf(string policy_name, string user, 
		string database, string schema, string tsf_field, string tsf_value, 				
		vector<string> accesses)
{

	const char * policy_template = 
	"{\
		\"allowExceptions\": [ ], \
		\"denyExceptions\": [ ], \
		\"denyPolicyItems\": [ ], \
		\"description\": \"no description\", \
		\"isAuditEnabled\": true, \
		\"isEnabled\": true, \
		\"name\": \"%s\", \
		\"policyItems\": [\
			{\
				\"accesses\": [ %s ], \
				\"conditions\": [ ], \
				\"delegateAdmin\": true, \
				\"groups\": null, \
				\"users\": [\"%s\"]\
			}\
		], \
		\"resources\": {\
			\"database\": {\
				\"isExcludes\": false, \
				\"isRecursive\": false, \
				\"values\": [\"%s\"]\
			}, \
			\"schema\": {\
				\"isExcludes\": false, \
				\"isRecursive\": false, \
				\"values\": [\"%s\"]\
			}, \
			\"%s\": {\
				\"isExcludes\": false, \
				\"isRecursive\": false, \
				\"values\": [\"%s\"]\
			}\
		}, \
		\"service\": \"hawq\", \
		\"version\": 3\
	}";

	string accesses_str;
	for (auto access : accesses)
	{
		const char * access_template = "{\"isAllowed\": true, \"type\": \"%s\"}";
		string acc_str = hawq::test::stringFormat(access_template, access.c_str());
		if (accesses_str != "")
		{
			accesses_str += ",";
		}
		accesses_str += acc_str;
	}
	
	string policy_str = hawq::test::stringFormat(policy_template, 
		policy_name.c_str(), accesses_str.c_str(), user.c_str(), 
		database.c_str(), schema.c_str(), tsf_field.c_str(), tsf_value.c_str());
	
	_policys.push_back(policy_str);
	//cout<<policy_str<<endl;

	return 0;
}

int PolicyHelper::add_common_sp(std::string policy_name, std::string user, 
		std::string tsp_field, std::string tsp_value, 				
		std::vector<std::string> accesses)
{
	const char * policy_template = 
	"{\
		\"allowExceptions\": [ ], \
		\"denyExceptions\": [ ], \
		\"denyPolicyItems\": [ ], \
		\"description\": \"no description\", \
		\"isAuditEnabled\": true, \
		\"isEnabled\": true, \
		\"name\": \"%s\", \
		\"policyItems\": [\
			{\
				\"accesses\": [ %s ], \
				\"conditions\": [ ], \
				\"delegateAdmin\": true, \
				\"groups\": null, \
				\"users\": [\"%s\"]\
			}\
		], \
		\"resources\": {\
			\"%s\": {\
				\"isExcludes\": false, \
				\"isRecursive\": false, \
				\"values\": [\"%s\"]\
			}\
		}, \
		\"service\": \"hawq\", \
		\"version\": 3\
	}";

	string accesses_str;
	for (auto access : accesses)
	{
		const char * access_template = "{\"isAllowed\": true, \"type\": \"%s\"}";
		string acc_str = hawq::test::stringFormat(access_template, access.c_str());
		if (accesses_str != "")
		{
			accesses_str += ",";
		}
		accesses_str += acc_str;
	}
	
	string policy_str = hawq::test::stringFormat(policy_template, 
		policy_name.c_str(), accesses_str.c_str(), user.c_str(), tsp_field.c_str(), tsp_value.c_str());
	
	_policys.push_back(policy_str);
	return 0;
}

string PolicyHelper::write_tmpfile(std::string content)
{
	char temp[] = TMP_FOLDER "fileXXXXXX"; 
	const char *file_name = mktemp(temp);
	
	int fd = open(file_name, O_CREAT | O_TRUNC | O_WRONLY);
	if (fd <= 0)
		return "";
	write(fd, content.c_str(), content.length());
	close(fd);
	return string(file_name);
}

int PolicyHelper::ActivateAllPoliciesOnRanger()
{
	auto cmd = "mkdir -p " TMP_FOLDER;
	hawq::test::Command::getCommandStatus(cmd);
	for (auto policy : _policys) 
	{
		string file_path= write_tmpfile(policy);	
		if (file_path == "")
			return -1;
		auto cmd = hawq::test::stringFormat("python %s/Ranger/rangerpolicy.py -h %s -a %s", 
			_root_path.c_str(), _rangerhost.c_str(), file_path.c_str() );
		int ret = hawq::test::Command::getCommandStatus(cmd);
		if (ret != 0)
			return -1;
	}		
	sleep(60);
	return 0;
}

int PolicyHelper::DeletePolicyOnRanger(std::string policy_name)
{
	auto cmd = hawq::test::stringFormat("python %s/Ranger/rangerpolicy.py -h %s -d %s", 
		_root_path.c_str(), _rangerhost.c_str(), policy_name.c_str() );
	int ret = hawq::test::Command::getCommandStatus(cmd);
	if (ret != 0)
		return -1;
	return 0;
}

