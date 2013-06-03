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
	ereport(ERROR,
			(errcode(ERRCODE_GP_FEATURE_NOT_YET),
			 errmsg("pxf does not yet support writable external tables")));
	PG_RETURN_VOID();
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
	 * Condition 2: write not supported
	 */
	if (EXTPROTOCOL_VALIDATOR_GET_DIRECTION(fcinfo) == EXT_VALIDATE_WRITE)
            ereport(ERROR,
                    (errcode(ERRCODE_GP_FEATURE_NOT_YET),
                     errmsg("pxf does not yet support writable external tables")));

	/*
	 * Condition 3: url formatting of extra options.
	 */
	uri = parseGPHDUri(EXTPROTOCOL_VALIDATOR_GET_NTH_URL(fcinfo, 1));

	/* Temp: Uncomment for printing a NOTICE with parsed parameters */
	/* GPHDUri_debug_print(uri); */

	/* if we're here - the URI is valid. Don't need it no more */
	freeGPHDUri(uri);

	PG_RETURN_VOID();
}
