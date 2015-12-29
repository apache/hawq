/*
 * gp_optimizer_functions.c
 *    Defines builtin transformation functions for the optimizer.
 *
 * enable_xform: This function wraps EnableXform.
 *
 * disable_xform: This function wraps DisableXform.
 *
 * gp_opt_version: This function wraps LibraryVersion. 
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
 */

#include "postgres.h"

#include "funcapi.h"
#include "utils/builtins.h"

extern Datum EnableXform(PG_FUNCTION_ARGS);

/*
* Enables transformations in the optimizer.
*/
Datum
enable_xform(PG_FUNCTION_ARGS)
{
#ifdef USE_ORCA
	return EnableXform(fcinfo);
#else
	return CStringGetTextDatum("Pivotal Query Optimizer not supported");
#endif
}

extern Datum DisableXform(PG_FUNCTION_ARGS);

/* 
* Disables transformations in the optimizer.
*/
Datum
disable_xform(PG_FUNCTION_ARGS)
{
#ifdef USE_ORCA
	return DisableXform(fcinfo);
#else
	return CStringGetTextDatum("Pivotal Query Optimizer not supported");
#endif
}

extern Datum LibraryVersion();
	
/*
* Returns the optimizer and gpos library versions.
*/
Datum
gp_opt_version(PG_FUNCTION_ARGS __attribute__((unused)))
{
#ifdef USE_ORCA
	return LibraryVersion();
#else
	return CStringGetTextDatum("Pivotal Query Optimizer not supported");
#endif
}
