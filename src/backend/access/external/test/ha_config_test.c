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

#include <stdarg.h>
#include <stddef.h>
#include <setjmp.h>
#include "cmockery.h"

#include "c.h"
#include "../ha_config.c"
#include "ha_config_mock.c"
#include "lib/stringinfo.h"
#include "utils/builtins.h"

/* Helper functions signature */
#define format_string_release(p) (pfree(p))

static char *format_string_create(char *f,...);
static void handle_pg_excp(char *msg, int errcode);

/*
 * Unitest for GPHD_HA_load_nodes() in ../access/external/ha_config.c
 * GPHD_HA_load_nodes() discovers the active Namnode from an HA Namenodes pair.
 * It does this by interacting with the API exposed by hdfs.h, from which it uses 
 * 2 functions:
 * a. Namenode * hdfsGetHANamenodes(const char * nameservice, int * size);
 * b. void hdfsFreeNamenodeInformation(Namenode * namenodes, int size);
 * This unitest verifies the correct interaction between GPHD_HA_load_nodes() implementation
 * and the 2 hdfs.h APIs. It looks at the standard flows with expected input configuration
 * and also at limit cases with corrupted input configuration.
 * The mock functions for the two(2) hdfs.h APIs are in ha_config_mock.c.
 */

/*
 * SUT function: NNHAConf* GPHD_HA_load_nodes(const char *nameservice);
 * Mock function: Namenode * hdfsGetHANamenodes(const char * nameservice, int * size) 
 * Negative test: GPHD_HA_load_nodes() receives an unexistent namesrvice
 */
void 
test__GPHD_HA_load_nodes__UnknownNameservice(void **state)
{
	/* 
	 * In case it receives an unknown nameservice string, the real function hdfsGetHANamenodes()
	 * will return NULL. We instruct our mock function to return NULL. In this way we simulate
	 * an unknown_service scenario and verify that our SUT function GPHD_HA_load_nodes() handles
	 * correctly the NULL returned by hdfsGetHANamenodes.
	 */
	will_return(hdfsGetHANamenodes, NULL);
	
	PG_TRY();
	{
		NNHAConf *hac = GPHD_HA_load_nodes("UNKNOWN_SERVICE");
	}
	PG_CATCH();
	{
		char *msg = "nameservice UNKNOWN_SERVICE not found in client configuration. No HA namenodes provided";
		handle_pg_excp(msg, ERRCODE_SYNTAX_ERROR);
		return;
	}
	PG_END_TRY();
	
	assert_true(false);
}

/*
 * SUT function: NNHAConf* GPHD_HA_load_nodes(const char *nameservice);
 * Mock function: Namenode * hdfsGetHANamenodes(const char * nameservice, int * size) 
 * Negative test: hdfsGetHANamenodes() returns just one namenode. 
 * This is not an HA sustainable.
 */
void 
test__GPHD_HA_load_nodes__OneNN(void **state)
{
	unsigned int numn = 1;
	Namenode nns[1];
	
	will_return(hdfsGetHANamenodes, nns);
	will_assign_value(hdfsGetHANamenodes, size, numn);
	

	PG_TRY();
	{
		NNHAConf *hac = GPHD_HA_load_nodes("NAMESERVICE");
	}
	PG_CATCH();
	{
		char *msg = format_string_create("High availability for nameservice %s was configured with only one node. A high availability scheme requires at least two nodes ",
										 "NAMESERVICE");
		handle_pg_excp(msg, ERRCODE_INTERNAL_ERROR);
		format_string_release(msg); /* if we trip on assert_string_equal we don't free but it doesen't matter because process stops*/
		return;
	}
	PG_END_TRY();
	
	assert_true(false);
	
}
										 																							
/*
 * SUT function: NNHAConf* GPHD_HA_load_nodes(const char *nameservice);
 * Mock function: Namenode * hdfsGetHANamenodes(const char * nameservice, int * size) 
 * Negative test: hdfsGetHANamenodes() returns a Namenode.rpc_address field without ":"
 * the host:port delimiter.
 */
void 
test__GPHD_HA_load_nodes__RpcDelimMissing(void **state)
{
	unsigned int numn = 2;
	Namenode nns[] = { {"mdw2080", "mdw:50070"}, {"smdw:2080", "smdw:50070"}};	
	
	will_return(hdfsGetHANamenodes, nns);
	will_assign_value(hdfsGetHANamenodes, size, numn);
		
	PG_TRY();
	{
		NNHAConf *hac = GPHD_HA_load_nodes("NAMESERVICE");
	}
	PG_CATCH();
	{
		char *msg = "dfs.namenode.rpc-address was set incorrectly in the configuration. ':' missing";
		handle_pg_excp(msg, ERRCODE_SYNTAX_ERROR);
		return;
	}
	PG_END_TRY();
	
	assert_true(false);
	
}

/*
 * SUT function: NNHAConf* GPHD_HA_load_nodes(const char *nameservice)
 * Mock functions: Namenode * hdfsGetHANamenodes(const char * nameservice, int * size)
 *                 void port_to_str(char **port, int new_port)
 * Positive test: port_to_str() assigns pxf_service_port correctly
 */
void 
test__GPHD_HA_load_nodes__PxfServicePortIsAssigned(void **state)
{
	unsigned int numn = 2;
	Namenode nns[] = { {"mdw:2080", "mdw:50070"}, {"smdw:2080", "smdw:50070"}};
	char strPort[INT32_CHAR_SIZE] = {0};
	pg_ltoa(pxf_service_port, strPort);
	
	will_return(hdfsGetHANamenodes, nns);
	will_assign_value(hdfsGetHANamenodes, size, numn);

	will_be_called(port_to_str);
	expect_not_value(port_to_str, port, NULL);
	expect_value(port_to_str, new_port, pxf_service_port);
	will_assign_string(port_to_str, port, strPort);

	will_be_called(port_to_str);
	expect_not_value(port_to_str, port, NULL);
	expect_value(port_to_str, new_port, pxf_service_port);
	will_assign_string(port_to_str, port, strPort);

	will_be_called(hdfsFreeNamenodeInformation);
		

	NNHAConf *hac = GPHD_HA_load_nodes("NAMESERVICE");
}

/*
 * SUT function: NNHAConf* GPHD_HA_load_nodes(const char *nameservice)
 * Mock functions: Namenode * hdfsGetHANamenodes(const char * nameservice, int * size)
 *                 void port_to_str(char **port, int new_port)
 * Negative test: hdfsGetHANamenodes() returns a Namenode.http_address field without 
 * the host - ":port".
 */
void 
test__GPHD_HA_load_nodes__HostMissing(void **state)
{
	unsigned int numn = 2;
	Namenode nns[] = { {":2080", "mdw:50070"}, {"smdw:2080", "smdw:50070"}};
	char strPort[INT32_CHAR_SIZE] = {0};
	pg_ltoa(pxf_service_port, strPort);
	
	will_return(hdfsGetHANamenodes, nns);
	will_assign_value(hdfsGetHANamenodes, size, numn);
	
	will_be_called(port_to_str);
	expect_not_value(port_to_str, port, NULL);
	expect_value(port_to_str, new_port, pxf_service_port);
	will_assign_string(port_to_str, port, strPort);

	will_be_called(port_to_str);
	expect_not_value(port_to_str, port, NULL);
	expect_value(port_to_str, new_port, pxf_service_port);
	will_assign_string(port_to_str, port, strPort);

	will_be_called(hdfsFreeNamenodeInformation);

	PG_TRY();
	{
		NNHAConf *hac = GPHD_HA_load_nodes("NAMESERVICE");
	}
	PG_CATCH();
	{
		char *msg = "HA Namenode host number 1 is NULL value";
		handle_pg_excp(msg, ERRCODE_SYNTAX_ERROR);
		return;
	}
	PG_END_TRY();
}

/*
 * SUT function: NNHAConf* GPHD_HA_load_nodes(const char *nameservice)
 * Mock functions: Namenode * hdfsGetHANamenodes(const char * nameservice, int * size)
 *                 void port_to_str(char **port, int new_port)
 * Negative test: port_to_str() does not set the port
 * the port - "host:".
 */
void 
test__GPHD_HA_load_nodes__PortMissing(void **state)
{
	unsigned int numn = 2;
	Namenode nns[] = { {"mdw:", "mdw:50070"}, {"smdw:2080", "smdw:50070"}};	
	
	will_return(hdfsGetHANamenodes, nns);
	will_assign_value(hdfsGetHANamenodes, size, numn);

	will_be_called(port_to_str);
	expect_not_value(port_to_str, port, NULL);
	expect_value(port_to_str, new_port, pxf_service_port);

	will_be_called(port_to_str);
	expect_not_value(port_to_str, port, NULL);
	expect_value(port_to_str, new_port, pxf_service_port);

	will_be_called(hdfsFreeNamenodeInformation);
	
	PG_TRY();
	{
		NNHAConf *hac = GPHD_HA_load_nodes("NAMESERVICE");
	}
	PG_CATCH();
	{
		char *msg = "HA Namenode RPC port number 1 is NULL value";
		handle_pg_excp(msg, ERRCODE_SYNTAX_ERROR);
		return;
	}
	PG_END_TRY();
}

/*
 * SUT function: NNHAConf* GPHD_HA_load_nodes(const char *nameservice)
 * Mock functions: Namenode * hdfsGetHANamenodes(const char * nameservice, int * size)
 *                 void port_to_str(char **port, int new_port)
 * Negative test: port_to_str() returns a port outside the valid range
 *  - a number higher than 65535
 */
void 
test__GPHD_HA_load_nodes__PortIsInvalidNumber(void **state)
{
	unsigned int numn = 2;
	Namenode nns[] = { {"mdw:2080", "mdw:65550"}, {"smdw:2080", "smdw:50070"}};	
	
	will_return(hdfsGetHANamenodes, nns);
	will_assign_value(hdfsGetHANamenodes, size, numn);

	will_be_called(port_to_str);
	expect_not_value(port_to_str, port, NULL);
	expect_value(port_to_str, new_port, pxf_service_port);
	will_assign_string(port_to_str, port, "65550");

	will_be_called(port_to_str);
	expect_not_value(port_to_str, port, NULL);
	expect_value(port_to_str, new_port, pxf_service_port);
	will_assign_string(port_to_str, port, "65550");

	will_be_called(hdfsFreeNamenodeInformation);
	
	PG_TRY();
	{
		NNHAConf *hac = GPHD_HA_load_nodes("NAMESERVICE");
	}
	PG_CATCH();
	{
		char *msg = "Invalid port <65550> detected in nameservice configuration";				
		handle_pg_excp(msg, ERRCODE_SYNTAX_ERROR);		
		return;
	}
	PG_END_TRY();
}

/*
 * SUT function: NNHAConf* GPHD_HA_load_nodes(const char *nameservice)
 * Mock functions: Namenode * hdfsGetHANamenodes(const char * nameservice, int * size)
 *                 void port_to_str(char **port, int new_port)
 * Negative test: port_to_str() returns a port that is not a number
 */
void 
test__GPHD_HA_load_nodes__PortIsNotNumber_TakeOne(void **state)
{
	NNHAConf *hac;
	unsigned int numn = 2;
	Namenode nns[] = { {"mdw:2080", "mdw:50070"}, {"smdw:2080", "smdw:50070"}};
	
	will_return(hdfsGetHANamenodes, nns);
	will_assign_value(hdfsGetHANamenodes, size, numn);

	will_be_called(port_to_str);
	expect_not_value(port_to_str, port, NULL);
	expect_value(port_to_str, new_port, pxf_service_port);
	will_assign_string(port_to_str, port, "melon");

	will_be_called(port_to_str);
	expect_not_value(port_to_str, port, NULL);
	expect_value(port_to_str, new_port, pxf_service_port);

	will_be_called(hdfsFreeNamenodeInformation);
	
	PG_TRY();
	{
		hac = GPHD_HA_load_nodes("NAMESERVICE");
	}
	PG_CATCH();
	{
		char *msg = "Invalid port <melon> detected in nameservice configuration";				
		handle_pg_excp(msg, ERRCODE_SYNTAX_ERROR);		
		return;
	}
	PG_END_TRY();
}

/*
 * SUT function: NNHAConf* GPHD_HA_load_nodes(const char *nameservice)
 * Mock functions: Namenode * hdfsGetHANamenodes(const char * nameservice, int * size)
 *                 void port_to_str(char **port, int new_port)
 * Negative test: port_to_str() returns a port that is not a number
 */
void 
test__GPHD_HA_load_nodes__PortIsNotNumber_TakeTwo(void **state)
{
	NNHAConf *hac;
	unsigned int numn = 2;
	Namenode nns[] = { {"mdw:2080", "mdw:50070"}, {"smdw:2080", "smdw:50070"}};
	
	will_return(hdfsGetHANamenodes, nns);
	will_assign_value(hdfsGetHANamenodes, size, numn);

	will_be_called(port_to_str);
	expect_not_value(port_to_str, port, NULL);
	expect_value(port_to_str, new_port, pxf_service_port);
	will_assign_string(port_to_str, port, "100ab");

	will_be_called(port_to_str);
	expect_not_value(port_to_str, port, NULL);
	expect_value(port_to_str, new_port, pxf_service_port);

	will_be_called(hdfsFreeNamenodeInformation);
	
	PG_TRY();
	{
		hac = GPHD_HA_load_nodes("NAMESERVICE");
	}
	PG_CATCH();
	{
		char *msg = "Invalid port <100ab> detected in nameservice configuration";				
		handle_pg_excp(msg, ERRCODE_SYNTAX_ERROR);		
		return;
	}
	PG_END_TRY();
}

int 
main(int argc, char *argv[]) 
{
	cmockery_parse_arguments(argc, argv);

	const UnitTest tests[] = {
		    unit_test(test__GPHD_HA_load_nodes__UnknownNameservice),
            unit_test(test__GPHD_HA_load_nodes__OneNN),
		    unit_test(test__GPHD_HA_load_nodes__RpcDelimMissing),
		    unit_test(test__GPHD_HA_load_nodes__PxfServicePortIsAssigned),
		    unit_test(test__GPHD_HA_load_nodes__HostMissing),
		    unit_test(test__GPHD_HA_load_nodes__PortMissing),
		    unit_test(test__GPHD_HA_load_nodes__PortIsInvalidNumber),
		    unit_test(test__GPHD_HA_load_nodes__PortIsNotNumber_TakeOne),
		    unit_test(test__GPHD_HA_load_nodes__PortIsNotNumber_TakeTwo)
	};
	return run_tests(tests);
}

/*
 * Helper function to format strings that need to be passed to assert macros
 */
static char*
format_string_create(char *f,...)
{
	StringInfoData s;
	va_list vl;
	
	initStringInfo(&s);
	
	va_start(vl,f);
	appendStringInfoVA(&s, f, vl);
	va_end(vl);
	
	return s.data;
}

/*
 * Encapsulates exception unpackaging
 */
static void
handle_pg_excp(char *msg, int errcode)
{
	CurrentMemoryContext = 1;
	ErrorData *edata = CopyErrorData();
	
	assert_true(edata->sqlerrcode == errcode);
	assert_true(edata->elevel == ERROR);
	assert_string_equal(edata->message, msg);
	
	/* Clean the internal error data stack. Otherwise errordata_stack_depth in elog.c,
	 * keeps growing from test to test with each ereport we issue in our SUT function
	 * until we reach errordata_stack_depth >= ERRORDATA_STACK_SIZE and our tests
	 * start failing
	 */
	elog_dismiss(INFO);
}
