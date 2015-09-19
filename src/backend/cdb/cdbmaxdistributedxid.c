/*-------------------------------------------------------------------------
 *
 * cdbmaxdistributedxid.c
 *		Function to return maximum distributed transaction id.
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

Datum		gp_max_distributed_xid(PG_FUNCTION_ARGS);

PG_FUNCTION_INFO_V1(gp_max_distributed_xid);

Datum
gp_max_distributed_xid(PG_FUNCTION_ARGS __attribute__((unused)) )
{
	PG_RETURN_NULL();
}
