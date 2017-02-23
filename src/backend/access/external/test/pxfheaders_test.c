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
#include "../pxfheaders.c"

static GPHDUri *gphd_uri = NULL;
static PxfInputData *input_data = NULL;
static extvar_t *mock_extvar = NULL;

static char *old_pxf_remote_service_login = NULL;
static char *old_pxf_remote_service_secret = NULL;

void expect_churl_headers(const char *key, const char *value);
void expect_churl_headers_alignment();
void store_gucs();
void setup_gphd_uri();
void setup_input_data();
void setup_external_vars();
void expect_external_vars();
void restore_gucs();

void
test__build_http_header__remote_login_is_null(void **state)
{
	expect_external_vars();

	expect_churl_headers("X-GP-SEGMENT-ID", mock_extvar->GP_SEGMENT_ID);
	expect_churl_headers("X-GP-SEGMENT-COUNT", mock_extvar->GP_SEGMENT_COUNT);
	expect_churl_headers("X-GP-XID", mock_extvar->GP_XID);
	expect_churl_headers_alignment();
	expect_churl_headers("X-GP-URL-HOST", gphd_uri->host);
	expect_churl_headers("X-GP-URL-PORT", gphd_uri->port);
	expect_churl_headers("X-GP-DATA-DIR", gphd_uri->data);
	expect_churl_headers("X-GP-URI", gphd_uri->uri);
	expect_churl_headers("X-GP-HAS-FILTER", "0");

	build_http_header(input_data);
}

void
test__build_http_header__remote_login_is_not_null(void **state)
{
	expect_external_vars();

	expect_churl_headers("X-GP-SEGMENT-ID", mock_extvar->GP_SEGMENT_ID);
	expect_churl_headers("X-GP-SEGMENT-COUNT", mock_extvar->GP_SEGMENT_COUNT);
	expect_churl_headers("X-GP-XID", mock_extvar->GP_XID);
	expect_churl_headers_alignment();
	expect_churl_headers("X-GP-URL-HOST", gphd_uri->host);
	expect_churl_headers("X-GP-URL-PORT", gphd_uri->port);
	expect_churl_headers("X-GP-DATA-DIR", gphd_uri->data);
	expect_churl_headers("X-GP-URI", gphd_uri->uri);
	expect_churl_headers("X-GP-HAS-FILTER", "0");

	pxf_remote_service_login = "not a valid login";
	expect_churl_headers("X-GP-REMOTE-USER", pxf_remote_service_login);

	build_http_header(input_data);
}

void
test__build_http_header__remote_secret_is_not_null(void **state)
{
	expect_external_vars();

	expect_churl_headers("X-GP-SEGMENT-ID", mock_extvar->GP_SEGMENT_ID);
	expect_churl_headers("X-GP-SEGMENT-COUNT", mock_extvar->GP_SEGMENT_COUNT);
	expect_churl_headers("X-GP-XID", mock_extvar->GP_XID);
	expect_churl_headers_alignment();
	expect_churl_headers("X-GP-URL-HOST", gphd_uri->host);
	expect_churl_headers("X-GP-URL-PORT", gphd_uri->port);
	expect_churl_headers("X-GP-DATA-DIR", gphd_uri->data);
	expect_churl_headers("X-GP-URI", gphd_uri->uri);
	expect_churl_headers("X-GP-HAS-FILTER", "0");

	pxf_remote_service_secret = "password";
	expect_churl_headers("X-GP-REMOTE-PASS", pxf_remote_service_secret);

	build_http_header(input_data);
}

void
test__build_http_header__remote_credentials_are_not_null(void **state)
{
	expect_external_vars();

	expect_churl_headers("X-GP-SEGMENT-ID", mock_extvar->GP_SEGMENT_ID);
	expect_churl_headers("X-GP-SEGMENT-COUNT", mock_extvar->GP_SEGMENT_COUNT);
	expect_churl_headers("X-GP-XID", mock_extvar->GP_XID);
	expect_churl_headers_alignment();
	expect_churl_headers("X-GP-URL-HOST", gphd_uri->host);
	expect_churl_headers("X-GP-URL-PORT", gphd_uri->port);
	expect_churl_headers("X-GP-DATA-DIR", gphd_uri->data);
	expect_churl_headers("X-GP-URI", gphd_uri->uri);
	expect_churl_headers("X-GP-HAS-FILTER", "0");

	pxf_remote_service_login = "not a valid login";
	expect_churl_headers("X-GP-REMOTE-USER", pxf_remote_service_login);

	pxf_remote_service_secret = "password";
	expect_churl_headers("X-GP-REMOTE-PASS", pxf_remote_service_secret);

	build_http_header(input_data);
}

void
test__get_format_name(void **state)
{
	char fmtcode = 't';
	char *formatName = get_format_name(fmtcode);
	assert_string_equal(TextFormatName, formatName);

	fmtcode = 'c';
	formatName = get_format_name(fmtcode);
	assert_string_equal(TextFormatName, formatName);

	fmtcode = 'b';
	formatName = get_format_name(fmtcode);
	assert_string_equal(GpdbWritableFormatName, formatName);
}

/*
 * Add an expect clause on a churl_headers_append with given
 * key and value
 */
void
expect_churl_headers(const char *key, const char *value)
{
	expect_value(churl_headers_append, headers, input_data->headers);
	expect_string(churl_headers_append, key, key);
	expect_string(churl_headers_append, value, value);
	will_be_called(churl_headers_append);
}

/*
 * Add the X-GP-ALIGNMENT header
 * As I don't want to copy-paste the logic in the 
 * production code I just support two cases.
 * Anything other than that requires special attention
 */
void 
expect_churl_headers_alignment()
{
	if (sizeof(char*) == 4)
		expect_churl_headers("X-GP-ALIGNMENT", "4");
	else if (sizeof(char*) == 8)
		expect_churl_headers("X-GP-ALIGNMENT", "8");
	else
		assert_false(true);
}

void
common_setup (void** state)
{
	store_gucs();
	setup_gphd_uri();
	setup_input_data();
	setup_external_vars();
}

void
store_gucs()
{
	old_pxf_remote_service_login = pxf_remote_service_login;
	old_pxf_remote_service_secret = pxf_remote_service_secret;
}

void
setup_gphd_uri()
{
	gphd_uri = palloc0(sizeof(GPHDUri));
	gphd_uri->host = "there's a place you're always welcome";
	gphd_uri->port = "it's as nice as it can be";
	gphd_uri->data = "everyone can get in";
	gphd_uri->uri = "'cos it's absolutely free";
}

void
setup_input_data()
{
	input_data = palloc0(sizeof(PxfInputData));
	input_data->gphduri = gphd_uri;
	input_data->headers = 0xBAADF00D;
}

void
setup_external_vars()
{
	mock_extvar = palloc0(sizeof(extvar_t));

	snprintf(mock_extvar->GP_SEGMENT_ID, sizeof(mock_extvar->GP_SEGMENT_ID), "badID");
	snprintf(mock_extvar->GP_SEGMENT_COUNT, sizeof(mock_extvar->GP_SEGMENT_COUNT), "lots");
	snprintf(mock_extvar->GP_XID, sizeof(mock_extvar->GP_XID), "badXID");
}

void expect_external_vars()
{
	expect_any(external_set_env_vars, extvar);
	expect_string(external_set_env_vars, uri, gphd_uri->uri);
	expect_value(external_set_env_vars, csv, false);
	expect_value(external_set_env_vars, escape, NULL);
	expect_value(external_set_env_vars, quote, NULL);
	expect_value(external_set_env_vars, header, false);
	expect_value(external_set_env_vars, scancounter, 0);
	will_assign_memory(external_set_env_vars, extvar, mock_extvar, sizeof(extvar_t));
	will_be_called(external_set_env_vars);
}

/*
 * Common resource cleanup
 */
void 
common_teardown (void** state)
{
	pfree(mock_extvar);
	pfree(input_data);
	pfree(gphd_uri);

	// Reset GUCs so tests won't have to
	restore_gucs();
}

void restore_gucs()
{
	pxf_remote_service_login = old_pxf_remote_service_login;
	pxf_remote_service_secret = old_pxf_remote_service_secret;
}

int 
main(int argc, char* argv[]) 
{
	cmockery_parse_arguments(argc, argv);

	const UnitTest tests[] = 
	{
		unit_test_setup_teardown(test__build_http_header__remote_login_is_null, 
								 common_setup, common_teardown),
		unit_test_setup_teardown(test__build_http_header__remote_login_is_not_null, 
								 common_setup, common_teardown),
		unit_test_setup_teardown(test__build_http_header__remote_credentials_are_not_null, 
								 common_setup, common_teardown),
		unit_test_setup_teardown(test__get_format_name,
								 common_setup, common_teardown)
	};

	return run_tests(tests);
}
