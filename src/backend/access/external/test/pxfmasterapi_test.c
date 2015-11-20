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
#include "../pxfmasterapi.c"
#include "lib/stringinfo.h"
#include "utils/elog.h"
#include <stdarg.h>

/*
 * Tests for the HA failover mechanism
 * pxfmasterapi.c contains several methods that issue REST calls to the PXF agent:
 * get_data_fragment_list(), get_datanode_rest_servers() and get_data_statistics()
 * All these methods need to support the case where PXF is deployed on a HA HDFS namenode cluster.
 * In such a situation a namenode machine is actually represented by two machines and
 * one of them could become inactive.
 * For this reason the REST logic was encapsulated into a failover logic in function rest_request().
 * All REST calls mentioned above will use rest_request(). The tests bellow will validate rest_request().
 */

/*
 * Trigger the first exception thrown from rest_request
 * Function is used with will_be_called_with_sideeffect
 */
void
FirstException( )
{
	elog(ERROR, "first exception");
}

/*
 * Trigger the second exception thrown from rest_request
 * Function is used with will_be_called_with_sideeffect
 */
void
SecondException( )
{
	elog(ERROR, "second exception");
}

/*
 * SUT: rest_request
 * call_rest throws an error while not in HA mode
 */
void
test__rest_request__callRestThrowsNoHA(void **state)
{
	GPHDUri *hadoop_uri = (GPHDUri*)  palloc0(sizeof(GPHDUri));
	hadoop_uri->host = pstrdup("host1");
	hadoop_uri->port = pstrdup("port1");
	ClientContext* client_context =  (ClientContext*)  palloc0(sizeof(ClientContext));
	char *restMsg = "empty message";

	expect_any(call_rest, hadoop_uri);
	expect_any(call_rest, client_context);
	expect_any(call_rest, rest_msg);
	will_be_called_with_sideeffect(call_rest, &FirstException, NULL);

	/* test */
	PG_TRY();
	{
		rest_request(hadoop_uri, client_context, restMsg);
	}
	PG_CATCH();
	{
		pfree(hadoop_uri->host);
		pfree(hadoop_uri->port);
		pfree(hadoop_uri);
		pfree(client_context);

		CurrentMemoryContext = 1;
		ErrorData *edata = CopyErrorData();

		/*Validate the type of expected error */
		assert_string_equal(edata->message, "first exception");
		return;
	}
	PG_END_TRY();

	assert_true(false);
}

/*
 * SUT: rest_request
 * call_rest throws an error while in HA mode
 * and the failover method finds an active IP so the second
 * call to call_rest does  no throw an exception
 */
void
test__rest_request__callRestThrowsHAFirstTime(void **state)
{
	GPHDUri *hadoop_uri = (GPHDUri*)  palloc0(sizeof(GPHDUri));
	hadoop_uri->host = pstrdup("host1");
	hadoop_uri->port = pstrdup("port1");
	NNHAConf *ha_nodes = (NNHAConf*)  palloc0(sizeof(NNHAConf));
    hadoop_uri->ha_nodes = ha_nodes;
	ha_nodes->nodes = (char *[]){"host1", "host2"};
	ha_nodes->restports = (char *[]){"port1", "port2"};
	ha_nodes->numn = 2;

	ClientContext* client_context =  (ClientContext*)  palloc0(sizeof(ClientContext));
	char *restMsg = "empty message";

	expect_any(call_rest, hadoop_uri);
	expect_any(call_rest, client_context);
	expect_any(call_rest, rest_msg);
	will_be_called_with_sideeffect(call_rest, &FirstException, NULL);

	/* the second call from ha_failover */
	expect_any(call_rest, hadoop_uri);
	expect_any(call_rest, client_context);
	expect_any(call_rest, rest_msg);
	will_be_called(call_rest);


	/* test */
	rest_request(hadoop_uri, client_context, restMsg);

	pfree(hadoop_uri);
	pfree(client_context);
}

/*
 * SUT: rest_request
 * call_rest throws an error while in HA mode
 * and the failover method finds an an active IP so the second
 * call to call_rest is issued on the second IP. This call also throws
 * an exception - but this time the exception is not caught.
 */
void
test__rest_request__callRestThrowsHASecondTime(void **state)
{
	GPHDUri *hadoop_uri = (GPHDUri*)  palloc0(sizeof(GPHDUri));
	hadoop_uri->host = pstrdup("host1");
	hadoop_uri->port = pstrdup("port1");
	NNHAConf *ha_nodes = (NNHAConf*)  palloc0(sizeof(NNHAConf));
    hadoop_uri->ha_nodes = ha_nodes;
	ha_nodes->nodes = (char *[]){"host1", "host2"};
	ha_nodes->restports = (char *[]){"port1", "port2"};
	ha_nodes->numn = 2;

	ClientContext* client_context =  (ClientContext*)  palloc0(sizeof(ClientContext));
	char *restMsg = "empty message";

	expect_any(call_rest, hadoop_uri);
	expect_any(call_rest, client_context);
	expect_any(call_rest, rest_msg);
	will_be_called_with_sideeffect(call_rest, &FirstException, NULL);

	/* the second call from ha_failover */
	expect_any(call_rest, hadoop_uri);
	expect_any(call_rest, client_context);
	expect_any(call_rest, rest_msg);
	will_be_called_with_sideeffect(call_rest, &SecondException, NULL);


	/* test */
	PG_TRY();
	{
		rest_request(hadoop_uri, client_context, restMsg);
	}
	PG_CATCH();
	{
		pfree(hadoop_uri->host);
		pfree(hadoop_uri->port);
		pfree(hadoop_uri);
		pfree(client_context);

		CurrentMemoryContext = 1;
		ErrorData *edata = CopyErrorData();

		/*Validate the type of expected error */
		assert_string_equal(edata->message, "second exception");
		/* the first exception was caught by rest_request() */
		return;
	}
	PG_END_TRY();

	assert_true(false);
}

/*
 * SUT: rest_request
 * the first time call_rest is called we succeed, since the first IP is valid
 * No exceptions are thrown
 */
void
test__rest_request__callRestHASuccessFromTheFirstCall(void **state)
{
	GPHDUri *hadoop_uri = (GPHDUri*)  palloc0(sizeof(GPHDUri));
	hadoop_uri->host = pstrdup("host1");
	hadoop_uri->port = pstrdup("port1");
	NNHAConf *ha_nodes = (NNHAConf*)  palloc0(sizeof(NNHAConf));
    hadoop_uri->ha_nodes = ha_nodes;
	ha_nodes->nodes = (char *[]){"host1", "host2"};
	ha_nodes->restports = (char *[]){"port1", "port2"};
	ha_nodes->numn = 2;

	ClientContext* client_context =  (ClientContext*)  palloc0(sizeof(ClientContext));
	char *restMsg = "empty message";

	expect_any(call_rest, hadoop_uri);
	expect_any(call_rest, client_context);
	expect_any(call_rest, rest_msg);
	will_be_called(call_rest);

	/* test */
	rest_request(hadoop_uri, client_context, restMsg);

	pfree(hadoop_uri->host);
	pfree(hadoop_uri->port);
	pfree(hadoop_uri);
	pfree(client_context);
}

void
test__normalize_size(void **state)
{
	float4 result = normalize_size(10000000, "B");
	assert_int_equal(result, 10000000);

	result = normalize_size(10000000, "KB");
	assert_int_equal(result, 10240000000);

	result = normalize_size(500, "MB");
	assert_int_equal(result, 524288000);

	result = normalize_size(10, "GB");
	assert_int_equal(result, 10737418240);

	result = normalize_size(10000, "TB");
	assert_int_equal(result, 10995116277760000);
}

int 
main(int argc, char *argv[]) 
{
	cmockery_parse_arguments(argc, argv);

	const UnitTest tests[] = {
		    unit_test(test__rest_request__callRestThrowsNoHA),
		    unit_test(test__rest_request__callRestThrowsHAFirstTime),
		    unit_test(test__rest_request__callRestThrowsHASecondTime),
		    unit_test(test__rest_request__callRestHASuccessFromTheFirstCall),
			unit_test(test__normalize_size)
	};
	return run_tests(tests);
}
