/*
 * Copyright (c) 2011 EMC Corporation All Rights Reserved
 *
 * This software is protected, without limitation, by copyright law
 * and international treaties. Use of this software and the intellectual
 * property contained therein is expressly limited to the terms and
 * conditions of the License Agreement under which it is provided by
 * or on behalf of EMC.
 */

/* ---------------------------------------------------------------------
 *
 * Interface to gp_ao_co_diagnostic functions.
 *
 * This file contains functions that are wrappers around their corresponding GP
 * internal functions located in the postgres backend executable. The GP
 * internal functions can not be called directly from outside
 *
 * Internal functions can not be called directly from outside the postrgres
 * backend executable without defining them in the catalog tables. The wrapper
 * functions defined in this file are compiled and linked into an library, which
 * can then be called as a user defined function. The wrapper functions will
 * call the actual internal functions.
 *
 * ---------------------------------------------------------------------
 */

#include "postgres.h"
#include <string.h>
#include "fmgr.h"
#include "funcapi.h"

#ifdef PG_MODULE_MAGIC
PG_MODULE_MAGIC;
#endif

extern Datum
gp_aoseg_history(PG_FUNCTION_ARGS);

extern Datum
gp_aocsseg(PG_FUNCTION_ARGS);

extern Datum
gp_aocsseg_history(PG_FUNCTION_ARGS);

PG_FUNCTION_INFO_V1(gp_aoseg_history_wrapper);
PG_FUNCTION_INFO_V1(gp_aocsseg_wrapper);
PG_FUNCTION_INFO_V1(gp_aocsseg_history_wrapper);

extern Datum
gp_aoseg_history_wrapper(PG_FUNCTION_ARGS);

extern Datum
gp_aocsseg_wrapper(PG_FUNCTION_ARGS);

extern Datum
gp_aocsseg_history_wrapper(PG_FUNCTION_ARGS);



/* ---------------------------------------------------------------------
 * Interface to gp_aoseg_history_wrapper function.
 *
 * The gp_aoseg_history_wrapper function is a wrapper around the gp_aoseg_history
 * function. It can be invoked by creating a function via psql that references it.
 * For example,
 *
 * CREATE FUNCTION get_gp_aoseg_history(oid)
 *   RETURNS TABLE ( gp_tid tid
 *                 , gp_xmin integer
 *                 , gp_xmin_status text
 *                 , gp_xmin_commit_distrib_id text
 *                 , gp_xmax integer
 *                 , gp_xmax_status text
 *                 , gp_xmax_commit_distrib_id text
 *                 , gp_command_id integer
 *                 , gp_infomask text
 *                 , gp_update_tid tid
 *                 , gp_visibility text
 *                 , segno integer
 *                 , tupcount bigint
 *                 , eof bigint
 *                 , eof_uncompressed bigint
 *                 )
 *   AS '$GPHOME/lib/gp_ao_co_diagnostics', 'gp_aoseg_history_wrapper' LANGUAGE C STRICT;
 *
 */
Datum
gp_aoseg_history_wrapper(PG_FUNCTION_ARGS)
{
  Datum returnValue = gp_aoseg_history(fcinfo);

  PG_RETURN_DATUM(returnValue);
}


/* ---------------------------------------------------------------------
 * Interface to gp_aocsseg_wrapper function.
 *
 * The gp_aocsseg_wrapper function is a wrapper around the gp_aocsseg function.
 * It can be invoked by creating a function via psql that references it.
 * For example,
 *
 * CREATE FUNCTION get_gp_aocsseg(oid)
 *   RETURNS TABLE ( gp_tid tid
 *                 , segno integer
 *                 , column_num smallint
 *                 , physical_segno integer
 *                 , tupcount bigint
 *                 , eof bigint
 *                 , eof_uncompressed bigint
 *                 )
 *   AS '$GPHOME/lib/gp_ao_co_diagnostics', 'gp_aocsseg_wrapper' LANGUAGE C STRICT;
 *
 */
Datum
gp_aocsseg_wrapper(PG_FUNCTION_ARGS)
{
  Datum returnValue = gp_aocsseg(fcinfo);

  PG_RETURN_DATUM(returnValue);
}


/* ---------------------------------------------------------------------
 * Interface to gp_aocsseg_history_wrapper function.
 *
 * The gp_aocsseg_history_wrapper function is a wrapper around the gp_aocsseg_history
 * function. It can be invoked by creating a function via psql that references it.
 * For example,
 *
 * CREATE FUNCTION get_gp_aocsseg_history(oid)
 *   RETURNS TABLE ( gp_tid tid
 *                 , gp_xmin integer
 *                 , gp_xmin_status text
 *                 , gp_xmin_distrib_id text
 *                 , gp_xmax integer
 *                 , gp_xmax_status text
 *                 , gp_xmax_distrib_id text
 *                 , gp_command_id integer
 *                 , gp_infomask text
 *                 , gp_update_tid tid
 *                 , gp_visibility text
 *                 , segno integer
 *                 , column_num smallint
 *                 , physical_segno integer
 *                 , tupcount bigint
 *                 , eof bigint
 *                 , eof_uncompressed bigint
 *                 )
 *   AS '$GPHOME/lib/gp_ao_co_diagnostics', 'gp_aocsseg_history_wrapper' LANGUAGE C STRICT;
 *
 */
Datum
gp_aocsseg_history_wrapper(PG_FUNCTION_ARGS)
{
  Datum returnValue = gp_aocsseg_history(fcinfo);

  PG_RETURN_DATUM(returnValue);
}




