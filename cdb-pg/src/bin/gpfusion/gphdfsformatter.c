/*-------------------------------------------------------------------------
 *
 * gphdfsformatter.c
 *
 * gphdfsformatter_* functions are now called gpdbwritableformatter_*, as
 * they parse and form GPDBWritable serializable objects, and the caller
 * may not necessarily be a gphdfs user.
 *
 * This file is a place holder for these old function names, as formatter
 * FUNCTION may already exist that uses these function names, and we want
 * to preserve backward compatibility.
 *
 * Copyright (c) 2011, Greenplum inc
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"
#include "fmgr.h"
#include "funcapi.h"
#include "common.h"

PG_FUNCTION_INFO_V1(gphdfsformatter_export);
PG_FUNCTION_INFO_V1(gphdfsformatter_import);

Datum gphdfsformatter_export(PG_FUNCTION_ARGS);
Datum gphdfsformatter_import(PG_FUNCTION_ARGS);

Datum
gphdfsformatter_export(PG_FUNCTION_ARGS)
{
	return gpdbwritableformatter_export(fcinfo);
}

Datum
gphdfsformatter_import(PG_FUNCTION_ARGS)
{
	return gpdbwritableformatter_import(fcinfo);
}
