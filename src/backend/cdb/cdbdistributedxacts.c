/*-------------------------------------------------------------------------
 *
 * cdbdistributedxacts.c
 *		Set-returning function to view gp_distributed_xacts table.
 *
 * IDENTIFICATION
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "funcapi.h"
#include "access/heapam.h"
#include "catalog/pg_type.h"
#include "utils/builtins.h"
#include "cdb/cdbutil.h"

Datum		gp_distributed_xacts__(PG_FUNCTION_ARGS);

PG_FUNCTION_INFO_V1(gp_distributed_xacts__);
/*
 * pgdatabasev - produce a view of gp_distributed_xacts to include transient state
 */
Datum
gp_distributed_xacts__(PG_FUNCTION_ARGS)
{
	PG_RETURN_NULL();
}

