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
 * The dynamically linked library created from this source can be reference by
 * creating a function in psql that references it. For example,
 *
 * CREATE OR REPLACE FUNCTION gp_workfile_cache_test()
 *   RETURNS bool
 *   AS '$libdir/gp_workfile_mgr.so', 'gp_workfile_manager_test_harness_wrapper' LANGUAGE C STRICT;
 *
 */

#include "postgres.h"
#include <string.h>
#include "fmgr.h"
#include "funcapi.h"
#include "utils/builtins.h"

Datum gp_workfile_mgr_test_harness(PG_FUNCTION_ARGS);

PG_FUNCTION_INFO_V1(gp_workfile_mgr_test_harness_wrapper);

extern Datum gp_workfile_mgr_test_harness_wrapper(PG_FUNCTION_ARGS);

/*
 * gp_workfile_manager_clear_cache_wrapper
 *    The wrapper for creating a user-defined C function for gp_workfile_manager_clear_cache.
 */
Datum
gp_workfile_mgr_test_harness_wrapper(PG_FUNCTION_ARGS)
{
	return gp_workfile_mgr_test_harness(fcinfo);
}

