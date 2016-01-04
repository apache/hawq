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
 *
 */

#include "common.h"
#include "access/pxfuriparser.h"

PG_FUNCTION_INFO_V1(pxfprotocol_export);
PG_FUNCTION_INFO_V1(pxfprotocol_import);
PG_FUNCTION_INFO_V1(pxfprotocol_validate_urls);

Datum pxfprotocol_export(PG_FUNCTION_ARGS);
Datum pxfprotocol_import(PG_FUNCTION_ARGS);
Datum pxfprotocol_validate_urls(PG_FUNCTION_ARGS);

Datum
pxfprotocol_export(PG_FUNCTION_ARGS)
{
	return gpbridge_export(fcinfo);
}

Datum
pxfprotocol_import(PG_FUNCTION_ARGS)
{
	return gpbridge_import(fcinfo);
}

/*
 * Validate the user-specified pxf URI supported functionality
 */
Datum
pxfprotocol_validate_urls(PG_FUNCTION_ARGS)
{
	GPHDUri	*uri;
	char *option;
	bool is_writable = EXTPROTOCOL_VALIDATOR_GET_DIRECTION(fcinfo) == EXT_VALIDATE_WRITE;

	/* Must be called via the external table format manager */
	if (!CALLED_AS_EXTPROTOCOL_VALIDATOR(fcinfo))
		elog(ERROR, "cannot execute pxfprotocol_validate_urls outside protocol manager");

	/*
	 * Condition 1: there must be only ONE url.
	 */
	if (EXTPROTOCOL_VALIDATOR_GET_NUM_URLS(fcinfo) != 1)
		ereport(ERROR,
				(errcode(ERRCODE_PROTOCOL_VIOLATION),
				 errmsg("number of URLs must be one")));

	/*
	 * Condition 2: url formatting of extra options.
	 */
	uri = parseGPHDUri(EXTPROTOCOL_VALIDATOR_GET_NTH_URL(fcinfo, 1));

	/*
	 * Condition 3: No duplicate options.
	 */
	GPHDUri_verify_no_duplicate_options(uri);

	/*
	 * Condition 4: existence of core options if profile wasn't supplied
	 */
	if (GPHDUri_get_value_for_opt(uri, "profile", &option, false) < 0)
	{
		List *coreOptions = list_make2("ACCESSOR", "RESOLVER");
		if (!is_writable)
		{
			coreOptions = lcons("FRAGMENTER", coreOptions);
		}
		GPHDUri_verify_core_options_exist(uri, coreOptions);
		list_free(coreOptions);
	}

	/* Temp: Uncomment for printing a NOTICE with parsed parameters */
	/*   GPHDUri_debug_print(uri); */

	/* if we're here - the URI is valid. Don't need it no more */
	freeGPHDUri(uri);

	/* HEADER option is not allowed */
	List* format_opts = EXTPROTOCOL_VALIDATOR_GET_FMT_OPT_LIST(fcinfo);
	ListCell   *format_option;
	foreach(format_option, format_opts)
	{
		DefElem    *defel = (DefElem *) lfirst(format_option);
		if (strcmp(defel->defname, "header") == 0)
		{
			ereport(ERROR,
					(errcode(ERRCODE_PROTOCOL_VIOLATION),
							errmsg("HEADER option is not allowed in a PXF external table")));

		}
	}

	PG_RETURN_VOID();
}
