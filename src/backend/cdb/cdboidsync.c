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
 * cdboidsync.c
 *
 * Make sure we don't re-use oids already used on the segment databases
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <sys/types.h>
#include <sys/stat.h>

#include "access/heapam.h"
#include "access/transam.h"
#include "catalog/catalog.h"
#include "catalog/namespace.h"
#include "catalog/pg_tablespace.h"
#include "commands/tablespace.h"
#include "miscadmin.h"
#include "storage/fd.h"
#include "utils/builtins.h"
#include "utils/syscache.h"
#include "utils/relcache.h"
#include "access/subtrans.h"
#include "access/transam.h"
#include "miscadmin.h"
#include "storage/proc.h"
#include "utils/builtins.h"

#include "gp-libpq-fe.h"
#include "lib/stringinfo.h"
#include "cdb/cdbvars.h"
#include "cdb/cdbdisp.h"
#include "utils/int8.h"
#include "utils/lsyscache.h"
#include "cdb/cdboidsync.h"
#include "cdb/dispatcher.h"

Datum
pg_highest_oid(PG_FUNCTION_ARGS __attribute__((unused)) )
{
	Oid			result;

	result = ShmemVariableCache->nextOid;

	PG_RETURN_OID(result);

}

void
cdb_sync_oid_to_segments(void)
{
	if (Gp_role == GP_ROLE_DISPATCH && IsNormalProcessingMode())
	{
		int 	i;
		
		/* Burn a few extra just for safety */
		for (i=0;i<10;i++)
			GetNewObjectId();
	}
	
}

