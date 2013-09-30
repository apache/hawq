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
	bool	is_writable = EXTPROTOCOL_VALIDATOR_GET_DIRECTION(fcinfo) == EXT_VALIDATE_WRITE;

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
	 * Condition 3: existence of core options if profile wasn't supplied
	 */
	if (!is_writable)
	{
		char *option;

		if(GPHDUri_get_value_for_opt(uri, "profile", &option, false) < 0)
		{
			StringInfoData err_msg;
			initStringInfo(&err_msg);

			if(GPHDUri_get_value_for_opt(uri, "fragmenter", &option, false) < 0)
			{
				appendStringInfo(&err_msg, "FRAGMENTER and ");
			}
			if(GPHDUri_get_value_for_opt(uri, "accessor", &option, false) < 0)
			{
				appendStringInfo(&err_msg, "ACCESSOR and ");
			}
			if(GPHDUri_get_value_for_opt(uri, "resolver", &option, false) < 0)
			{
				appendStringInfo(&err_msg, "RESOLVER and ");
			}
			if(err_msg.len > 0)
			{
				truncateStringInfo(&err_msg, err_msg.len - strlen(" and ")); //omit trailing 'and'
				ereport(ERROR,
				(errcode(ERRCODE_SYNTAX_ERROR),
				 errmsg("Invalid URI %s: PROFILE or %s option(s) missing", uri->uri, err_msg.data)));
			}
			pfree(err_msg.data);
		}	
	}

	/* Temp: Uncomment for printing a NOTICE with parsed parameters */
	/* GPHDUri_debug_print(uri); */

	/* if we're here - the URI is valid. Don't need it no more */
	freeGPHDUri(uri);

	PG_RETURN_VOID();
}
