/*-------------------------------------------------------------------------
 *
 * deprecated.c
 *
 *   Utility function to support deprecating functions from one release
 * to the next.  Deleting the function outright as part of upgrade is
 * not good because it could theoretically cascade to user views, so 
 * instead the definition of a deprecated function should be changed
 * to point to this function instead which will provide an error message
 * when the function is called.
 *
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
 *-------------------------------------------------------------------------
 */

#include "postgres.h"
#include "funcapi.h"

#include "utils/builtins.h"

Datum
gp_deprecated(PG_FUNCTION_ARGS)
{
	/* Lookup the function that was called in the catalog */
	Oid   procOid  = fcinfo->flinfo->fn_oid;
	char *procName = format_procedure(procOid);

	/* Return error that the function is deprecated */
	ereport(ERROR,
			(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
			 errmsg("function %s has been deprecated", procName)));

	/* unreachable */
	PG_RETURN_NULL();
}
