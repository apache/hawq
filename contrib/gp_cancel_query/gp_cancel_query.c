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

/*
 * Interface to gp_cancel_query_wrapper function.
 *
 * The gp_cancel_query_wrapper function is a wrapper around the gp_cancel_query
 * function located in the postgres backend executable. Since we can not call the
 * gp_cancel_query function directly without a change to the catalog tables, this
 * wrapper will allow a user to create a function using this wrapper and indirectly
 * call gp_cancel_query.
 *
 * The dynamicly linked library created from this source can be reference by
 * creating a function in psql that references it. For example,
 *
 * CREATE OR REPLACE FUNCTION gp_cancel_query(int, int)
 *   RETURNS bool
 *   AS '$libdir/gp_cancel_query.so', 'gp_cancel_query_wrapper' LANGUAGE C STRICT;
 *
 */

#include "postgres.h"
#include <string.h>
#include "fmgr.h"
#include "funcapi.h"
#include "utils/builtins.h"

#ifdef PG_MODULE_MAGIC
PG_MODULE_MAGIC;
#endif

extern Datum gp_cancel_query(PG_FUNCTION_ARGS);

PG_FUNCTION_INFO_V1(gp_cancel_query_wrapper);

extern Datum gp_cancel_query_wrapper(PG_FUNCTION_ARGS);

/*
 * gp_cancel_query_wrapper
 *    The wrapper for creating a user-defined C function for gp_cancel_query.
 */
Datum
gp_cancel_query_wrapper(PG_FUNCTION_ARGS)
{
	return gp_cancel_query(fcinfo);
}

