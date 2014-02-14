#include <stdarg.h>
#include <stddef.h>
#include <setjmp.h>
#include "cmockery.h"

#include "c.h"
#include "../ha_config.c"
#include "ha_config_mock.c"
#include "lib/stringinfo.h"
#include <stdarg.h>

/* Helper functions signature */
#define format_string_release(p) (pfree(p))

static char *format_string_create(char *f,...);
static void handle_pg_excp(char *msg, int errcode);

/*
 * Unitest for load_nn_ha_config() in ../access/external/ha_config.c
 * load_nn_ha_config() discovers the active Namnode from an HA Namenodes pair.
 * It does this by interacting with the API exposed by hdfs.h, from which it uses 
 * 2 functions:
 * a. Namenode * hdfsGetHANamenodes(const char * nameservice, int * size);
 * b. void hdfsFreeNamenodeInformation(Namenode * namenodes, int size);
 * This unitest verifies the correct interaction between load_nn_ha_config() implementation
 * and the 2 hdfs.h APIs. It looks at the standard flows with expected input configuration
 * and also at limit cases with corrupted input configuration.
 * The mock functions for the two(2) hdfs.h APIs are in ha_config_mock.c.
 */

/*
 * SUT function: NNHAConf* load_nn_ha_config(const char *nameservice);
 * Mock function: Namenode * hdfsGetHANamenodes(const char * nameservice, int * size) 
 * Negative test: load_nn_ha_config() receives an unexistent namesrvice
 */
void 
test__load_nn_ha_config__UnknownNameservice(void **state)
{
	/* 
	 * In case it receives an unknown nameservice string, the real function hdfsGetHANamenodes()
	 * will return NULL. We instruct our mock function to return NULL. In this way we simulate
	 * an unknown_service scenario and verify that our SUT function load_nn_ha_config() handles
	 * correctly the NULL returned by hdfsGetHANamenodes.
	 */
	will_return(hdfsGetHANamenodes, NULL);
	
	PG_TRY();
	{
		NNHAConf *hac = load_nn_ha_config("UNKNOWN_SERVICE");
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
 * SUT function: NNHAConf* load_nn_ha_config(const char *nameservice);
 * Mock function: Namenode * hdfsGetHANamenodes(const char * nameservice, int * size) 
 * Negative test: hdfsGetHANamenodes() returns just one namenode. 
 * This is not an HA sustainable.
 */
void 
test__load_nn_ha_config__OneNN(void **state)
{
	unsigned int numn = 1;
	Namenode nns[1];
	
	will_return(hdfsGetHANamenodes, nns);
	will_assign_value(hdfsGetHANamenodes, size, numn);
	

	PG_TRY();
	{
		NNHAConf *hac = load_nn_ha_config("NAMESERVICE");
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
 * SUT function: NNHAConf* load_nn_ha_config(const char *nameservice);
 * Mock functions: 
 *         Namenode * hdfsGetHANamenodes(const char * nameservice, int * size);
 *         void hdfsFreeNamenodeInformation(Namenode * namenodes, int size);
 *         bool ping(char* host, char *port);
 * hdfsGetHANamenodes() returns a valid Array of Namenodes and ping() discovers
 * that the second namenode is active
 */
void 
test__load_nn_ha_config__SecondNNFound(void **state)
{
	unsigned int numn = 2;
	Namenode nns[] = { {"mdw:2080", "mdw:50070"}, {"smdw:2080", "smdw:50070"}};
	
	will_return(hdfsGetHANamenodes, nns);
	will_assign_value(hdfsGetHANamenodes, size, numn);
	will_be_called(hdfsFreeNamenodeInformation);
	expect_string(ping, host, "mdw");
	expect_string(ping, port, "2080");
	will_return(ping, false);
	expect_string(ping, host, "smdw");
	expect_string(ping, port, "2080");
	will_return(ping, true);

	NNHAConf *hac = load_nn_ha_config("NAMESERVICE");

	assert_int_equal(hac->active, 1);
	assert_int_equal(hac->numn, 2);
	assert_string_equal(hac->rpcports[0], "2080");
	assert_string_equal(hac->restports[0], "50070");
	assert_string_equal(hac->nodes[0], "mdw");
	assert_string_equal(hac->rpcports[1], "2080");
	assert_string_equal(hac->restports[1], "50070");
	assert_string_equal(hac->nodes[1], "smdw");
}

/*
 * SUT function: NNHAConf* load_nn_ha_config(const char *nameservice);
 * Mock functions: 
 *         Namenode * hdfsGetHANamenodes(const char * nameservice, int * size);
 *         void hdfsFreeNamenodeInformation(Namenode * namenodes, int size);
 *         bool ping(char* host, char *port);
 * hdfsGetHANamenodes() returns a valid Array of Namenodes and ping() discovers
 * that the first namenode is active
 */
void 
test__load_nn_ha_config__FirstNNFound(void **state)
{
	unsigned int numn = 2;
	Namenode nns[] = { {"mdw:2080", "mdw:50070"}, {"smdw:2080", "smdw:50070"}};
	
	will_return(hdfsGetHANamenodes, nns);
	will_assign_value(hdfsGetHANamenodes, size, numn);
	will_be_called(hdfsFreeNamenodeInformation);
	expect_string(ping, host, "mdw");
	expect_string(ping, port, "2080");	
	will_return(ping, true); 

	NNHAConf *hac = load_nn_ha_config("NAMESERVICE");

	assert_int_equal(hac->active, 0);
	assert_int_equal(hac->numn, 2);
	assert_string_equal(hac->rpcports[0], "2080");
	assert_string_equal(hac->restports[0], "50070");
	assert_string_equal(hac->nodes[0], "mdw");
	assert_string_equal(hac->rpcports[1], "2080");
	assert_string_equal(hac->restports[1], "50070");
	assert_string_equal(hac->nodes[1], "smdw");     
}

/*
 * SUT function: NNHAConf* load_nn_ha_config(const char *nameservice);
 * Mock functions: 
 *         Namenode * hdfsGetHANamenodes(const char * nameservice, int * size);
 *         void hdfsFreeNamenodeInformation(Namenode * namenodes, int size);
 *         bool ping(char* host, char *port);
 * hdfsGetHANamenodes() returns a valid Array of 3 Namenodes and ping() discovers
 * that the third namenode is active
 */
void 
test__load_nn_ha_config__ThirdNNFound(void **state)
{
	unsigned int numn = 3;
	Namenode nns[] = { {"mdw:2080", "mdw:50070"}, {"smdw:2080", "smdw:50070"}, {"tmdw:2080", "tmdw:50070"} };
        
	will_return(hdfsGetHANamenodes, nns);
	will_assign_value(hdfsGetHANamenodes, size, numn);
	will_be_called(hdfsFreeNamenodeInformation);
	expect_string(ping, host, "mdw");
	expect_string(ping, port, "2080");
	will_return(ping, false);
	expect_string(ping, host, "smdw");
	expect_string(ping, port, "2080");	
	will_return(ping, false);
	expect_string(ping, host, "tmdw");
	expect_string(ping, port, "2080");	
	will_return(ping, true); 
	
	NNHAConf *hac = load_nn_ha_config("NAMESERVICE");
	
	assert_int_equal(hac->active, 2);
	assert_int_equal(hac->numn, 3);
	assert_string_equal(hac->rpcports[0], "2080");
	assert_string_equal(hac->restports[0], "50070");
	assert_string_equal(hac->nodes[0], "mdw");
	assert_string_equal(hac->rpcports[1], "2080");
	assert_string_equal(hac->restports[1], "50070");
	assert_string_equal(hac->nodes[1], "smdw");
	assert_string_equal(hac->rpcports[2], "2080");
	assert_string_equal(hac->restports[2], "50070");
	assert_string_equal(hac->nodes[2], "tmdw");
}

/*
 * SUT function: NNHAConf* load_nn_ha_config(const char *nameservice)
 * Mock functions: 
 *         Namenode * hdfsGetHANamenodes(const char * nameservice, int * size);
 *         void hdfsFreeNamenodeInformation(Namenode * namenodes, int size);
 *         bool ping(char* host, char *port);
 * Negative test: hdfsGetHANamenodes() returns a valid Array of namenodes and 
 * ping() does not discover an active namenode
 */
void 
test__load_nn_ha_config__NoneFound(void **state)
{
	unsigned int numn = 2;
	Namenode nns[] = { {"mdw:2080", "mdw:50070"}, {"smdw:2080", "smdw:50070"}};
	
	will_return(hdfsGetHANamenodes, nns);
	will_assign_value(hdfsGetHANamenodes, size, numn);
	will_be_called(hdfsFreeNamenodeInformation);
	expect_string(ping, host, "mdw");
	expect_string(ping, port, "2080");	
	will_return(ping, false);
	expect_string(ping, host, "smdw");
	expect_string(ping, port, "2080");	
	will_return(ping, false);
	
	PG_TRY();
	{
		NNHAConf *hac = load_nn_ha_config("NAMESERVICE");}
	
	PG_CATCH();
	{
		char *msg = format_string_create("No HA active namenode found for nameservice %s", "NAMESERVICE");
		handle_pg_excp(msg, ERRCODE_CONNECTION_FAILURE);                
		format_string_release(msg); 
		return;
	}
	PG_END_TRY();

	assert_true(false);
}
										 																							
/*
 * SUT function: NNHAConf* load_nn_ha_config(const char *nameservice);
 * Mock function: Namenode * hdfsGetHANamenodes(const char * nameservice, int * size) 
 * Negative test: hdfsGetHANamenodes() returns a Namenode.rpc_address field without ":"
 * the host:port delimiter.
 */
void 
test__load_nn_ha_config__RpcDelimMissing(void **state)
{
	unsigned int numn = 2;
	Namenode nns[] = { {"mdw2080", "mdw:50070"}, {"smdw:2080", "smdw:50070"}};	
	
	will_return(hdfsGetHANamenodes, nns);
	will_assign_value(hdfsGetHANamenodes, size, numn);
		
	PG_TRY();
	{
		NNHAConf *hac = load_nn_ha_config("NAMESERVICE");
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
 * SUT function: NNHAConf* load_nn_ha_config(const char *nameservice)
 * Mock function: Namenode * hdfsGetHANamenodes(const char * nameservice, int * size) 
 * Negative test: hdfsGetHANamenodes() returns a Namenode.http_address field without ":"
 * the host:port delimiter.
 */
void 
test__load_nn_ha_config__HttpDelimMissing(void **state)
{
	unsigned int numn = 2;
	Namenode nns[] = { {"mdw:2080", "mdw:50070"}, {"smdw:2080", "smdw50070"}};	
	
	will_return(hdfsGetHANamenodes, nns);
	will_assign_value(hdfsGetHANamenodes, size, numn);
		
	PG_TRY();
	{
		NNHAConf *hac = load_nn_ha_config("NAMESERVICE");
	}
	PG_CATCH();
	{
		char *msg = "dfs.namenode.http-address was set incorrectly in the configuration. ':' missing";
		handle_pg_excp(msg, ERRCODE_SYNTAX_ERROR);
		return;
	}
	PG_END_TRY();
	
	assert_true(false);
	
}

/*
 * SUT function: NNHAConf* load_nn_ha_config(const char *nameservice)
 * Mock function: Namenode * hdfsGetHANamenodes(const char * nameservice, int * size) 
 * Negative test: hdfsGetHANamenodes() returns a Namenode.http_address field without 
 * the host - ":port".
 */
void 
test__load_nn_ha_config__HostMissing(void **state)
{
	unsigned int numn = 2;
	Namenode nns[] = { {":2080", "mdw:50070"}, {"smdw:2080", "smdw:50070"}};	
	
	will_return(hdfsGetHANamenodes, nns);
	will_assign_value(hdfsGetHANamenodes, size, numn);
	will_be_called(hdfsFreeNamenodeInformation);
	
	PG_TRY();
	{
		NNHAConf *hac = load_nn_ha_config("NAMESERVICE");
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
 * SUT function: NNHAConf* load_nn_ha_config(const char *nameservice)
 * Mock function: Namenode * hdfsGetHANamenodes(const char * nameservice, int * size) 
 * Negative test: hdfsGetHANamenodes() returns a Namenode.http_address field without 
 * the port - "host:".
 */
void 
test__load_nn_ha_config__PortMissing(void **state)
{
	unsigned int numn = 2;
	Namenode nns[] = { {"mdw:", "mdw:50070"}, {"smdw:2080", "smdw:50070"}};	
	
	will_return(hdfsGetHANamenodes, nns);
	will_assign_value(hdfsGetHANamenodes, size, numn);
	will_be_called(hdfsFreeNamenodeInformation);
	
	PG_TRY();
	{
		NNHAConf *hac = load_nn_ha_config("NAMESERVICE");
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
 * SUT function: NNHAConf* load_nn_ha_config(const char *nameservice)
 * Mock function: Namenode * hdfsGetHANamenodes(const char * nameservice, int * size) 
 * Negative test: hdfsGetHANamenodes() returns an empty Namenode address string.
 */
void 
test__load_nn_ha_config__EmptyAddress(void **state)
{
	unsigned int numn = 2;
	Namenode nns[] = { {"", "mdw:50070"}, {"smdw:2080", "smdw:50070"}};	
	
	will_return(hdfsGetHANamenodes, nns);
	will_assign_value(hdfsGetHANamenodes, size, numn);
	
	PG_TRY();
	{
		NNHAConf *hac = load_nn_ha_config("NAMESERVICE");
	}
	PG_CATCH();
	{
		char *msg = "In configuration Namenode.rpc_address number 1 is null or empty";
		handle_pg_excp(msg, ERRCODE_SYNTAX_ERROR);
		return;
	}
	PG_END_TRY();
}

/*
 * SUT function: NNHAConf* load_nn_ha_config(const char *nameservice)
 * Mock function: Namenode * hdfsGetHANamenodes(const char * nameservice, int * size) 
 * Negative test: hdfsGetHANamenodes() returns a port outside the valid range
 *  - a number higher than 65535
 */
void 
test__load_nn_ha_config__PortIsInvalidNumber(void **state)
{
	unsigned int numn = 2;
	Namenode nns[] = { {"mdw:2080", "mdw:65550"}, {"smdw:2080", "smdw:50070"}};	
	
	will_return(hdfsGetHANamenodes, nns);
	will_assign_value(hdfsGetHANamenodes, size, numn);
	will_be_called(hdfsFreeNamenodeInformation);
	
	PG_TRY();
	{
		NNHAConf *hac = load_nn_ha_config("NAMESERVICE");
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
 * SUT function: NNHAConf* load_nn_ha_config(const char *nameservice)
 * Mock function: Namenode * hdfsGetHANamenodes(const char * nameservice, int * size) 
 * Negative test: hdfsGetHANamenodes() returns a port that is not a numeric string
 */
void 
test__load_nn_ha_config__PortIsNotNumber_TakeOne(void **state)
{
	NNHAConf *hac;
	unsigned int numn = 2;
	Namenode nns[] = { {"mdw:melon", "mdw:50070"}, {"smdw:2080", "smdw:50070"}};	
	
	will_return(hdfsGetHANamenodes, nns);
	will_assign_value(hdfsGetHANamenodes, size, numn);
	will_be_called(hdfsFreeNamenodeInformation);
	
	PG_TRY();
	{
		hac = load_nn_ha_config("NAMESERVICE");
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
 * SUT function: NNHAConf* load_nn_ha_config(const char *nameservice)
 * Mock function: Namenode * hdfsGetHANamenodes(const char * nameservice, int * size) 
 * Negative test: hdfsGetHANamenodes() returns a port that is not a numeric string
 */
void 
test__load_nn_ha_config__PortIsNotNumber_TakeTwo(void **state)
{
	NNHAConf *hac;
	unsigned int numn = 2;
	Namenode nns[] = { {"mdw:100ab", "mdw:50070"}, {"smdw:2080", "smdw:50070"}};	
	
	will_return(hdfsGetHANamenodes, nns);
	will_assign_value(hdfsGetHANamenodes, size, numn);
	will_be_called(hdfsFreeNamenodeInformation);
	
	PG_TRY();
	{
		hac = load_nn_ha_config("NAMESERVICE");
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
		    unit_test(test__load_nn_ha_config__UnknownNameservice),
            unit_test(test__load_nn_ha_config__OneNN), 
		    unit_test(test__load_nn_ha_config__SecondNNFound),
		    unit_test(test__load_nn_ha_config__FirstNNFound),
		    unit_test(test__load_nn_ha_config__ThirdNNFound),
		    unit_test(test__load_nn_ha_config__NoneFound),
		    unit_test(test__load_nn_ha_config__RpcDelimMissing),
		    unit_test(test__load_nn_ha_config__HttpDelimMissing),
		    unit_test(test__load_nn_ha_config__HostMissing), 
		    unit_test(test__load_nn_ha_config__PortMissing),
		    unit_test(test__load_nn_ha_config__EmptyAddress),
		    unit_test(test__load_nn_ha_config__PortIsInvalidNumber),		
		    unit_test(test__load_nn_ha_config__PortIsNotNumber_TakeOne),
		    unit_test(test__load_nn_ha_config__PortIsNotNumber_TakeTwo)
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
