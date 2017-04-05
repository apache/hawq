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

#ifndef _POLICY_HELPER_H_
#define _POLICY_HELPER_H_

#include <string>
#include <vector>


/**
 * A helper class for ranger policy add/del, using rangerpolicy.py internally
 * Usage see test_policyhelper.cpp
 */ 
class PolicyHelper{
public:
	PolicyHelper (std::string root_path, std::string ranger_host);
	~PolicyHelper ();
	void Reset();

	/*
	 * send all policies to ranger and sleep 60s for taking effect
	 * @returns: 0 success; -1 failed
	 */ 
	int ActivateAllPoliciesOnRanger();

	/*
	Resources hierarchy:
	|--database
	|         |--schema
	|         |       |--table
	|         |       |--sequence
	|         |       |--function
	|         |
	|         |--language
	|         
	|--tablespace
	|--protocol  
	*/
	int AddSchemaPolicy(std::string policy_name, std::string user, 
		std::string database, std::string schema, std::vector<std::string> accesses) 
	{
		return add_common_tsf(policy_name, user, database, schema, "table", "*", accesses);
	}
	
	int AddTablePolicy(std::string policy_name, std::string user, 
		std::string database, std::string schema, std::string table, std::vector<std::string> accesses) 
	{
		return add_common_tsf(policy_name, user, database, schema, "table", table, accesses);
	}
	
	int AddProtocolPolicy(std::string policy_name, std::string user, 
		std::string protocol, std::vector<std::string> accesses) 
	{
		return add_common_sp(policy_name, user, "protocol", protocol, accesses);
	}

	//TODO other AddXXXPolicy function
	
	int DeletePolicyOnRanger(std::string user);

private:
	std::string _rangerhost;
	std::string _root_path;

	std::vector<std::string> _policys;

	std::string write_tmpfile(std::string content);
	int add_common_t(std::string policy_name, std::string user, 
		std::string database, std::string schema, std::string tsf_field, std::string tsf_value, 				
		std::vector<std::string> accesses);
	
	/**
	 * tsf means:
	 *	t: table
	 *	s: schema
	 *	f: function
	 */ 
	int add_common_tsf(std::string policy_name, std::string user, 
		std::string database, std::string schema, std::string tsf_field, std::string tsf_value, 				
		std::vector<std::string> accesses);
	
	/**
	 * sp means:
	 *	s: tablespace
	 *	p: protocol
	 */ 
	int add_common_sp(std::string policy_name, std::string user, 
		std::string tsp_field, std::string tsp_value, 				
		std::vector<std::string> accesses);
};

#endif

