/*-------------------------------------------------------------------------
 *
 * gp_distributed_log.c
 *		Set-returning function to view gp_distributed_log table.
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
#include "access/clog.h"
#include "access/transam.h"
#include "cdb/cdbvars.h"                /* Gp_segment */

Datum		gp_distributed_log(PG_FUNCTION_ARGS);

/*
 * pgdatabasev - produce a view of gp_distributed_log that combines 
 * information from the local clog and the distributed log.
 */
Datum
gp_distributed_log(PG_FUNCTION_ARGS)
{
	PG_RETURN_NULL();
}

