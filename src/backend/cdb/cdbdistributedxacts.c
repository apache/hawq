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

