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
 * CREATE OR REPLACE FUNCTION gp_mdver_cache_entries()
 *   RETURNS RECORD
 *   AS '$libdir/gp_mdver.so', 'gp_mdver_cache_entries'
 *   LANGUAGE C;
 *
 */

#include "postgres.h"
#include "funcapi.h"
#include "utils/mdver.h"

#ifdef PG_MODULE_MAGIC
PG_MODULE_MAGIC;
#endif

/* Forward declarations */
Datum gp_mdver_cache_entries(PG_FUNCTION_ARGS);

/* Shared library Postgres module magic */
PG_FUNCTION_INFO_V1(gp_mdver_cache_entries);

/* Number of columns in the tuple holding a mdver entry */
#define NUM_MDVER_ENTRIES_COL 3

/*
 * Function returning all workfile cache entries for one segment
 */
Datum
gp_mdver_cache_entries(PG_FUNCTION_ARGS)
{
	FuncCallContext *funcctx = NULL;
	int32 *crtIndexPtr = NULL;

	if (SRF_IS_FIRSTCALL())
	{
		/* create a function context for cross-call persistence */
		funcctx = SRF_FIRSTCALL_INIT();

		/* Switch to memory context appropriate for multiple function calls */
		MemoryContext oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

		/*
		 * Build a tuple descriptor for our result type
		 * The number and type of attributes have to match the definition of the
		 * view gp_mdver_cache_entries
		 */
		TupleDesc tupdesc = CreateTemplateTupleDesc(NUM_MDVER_ENTRIES_COL, false);

		TupleDescInitEntry(tupdesc, (AttrNumber) 1, "oid",
				OIDOID, -1 /* typmod */, 0 /* attdim */);
		TupleDescInitEntry(tupdesc, (AttrNumber) 2, "ddl_version",
				INT8OID, -1 /* typmod */, 0 /* attdim */);
		TupleDescInitEntry(tupdesc, (AttrNumber) 3, "dml_version",
				INT8OID, -1 /* typmod */, 0 /* attdim */);

		Assert(NUM_MDVER_ENTRIES_COL == 3);

		funcctx->tuple_desc = BlessTupleDesc(tupdesc);

		crtIndexPtr = (int32 *) palloc(sizeof(*crtIndexPtr));
		*crtIndexPtr = 0;
		funcctx->user_fctx = crtIndexPtr;
		MemoryContextSwitchTo(oldcontext);

	}

	Cache *cache = mdver_get_glob_mdvsn();
	funcctx = SRF_PERCALL_SETUP();
	crtIndexPtr = (int32 *) funcctx->user_fctx;


	while (true)
	{

		CacheEntry *crtEntry = Cache_NextEntryToList(cache, crtIndexPtr);

		if (!crtEntry)
		{
			/* Reached the end of the entry array, we're done */
			SRF_RETURN_DONE(funcctx);
		}

		Datum		values[NUM_MDVER_ENTRIES_COL];
		bool		nulls[NUM_MDVER_ENTRIES_COL];
		MemSet(nulls, 0, sizeof(nulls));

		mdver_entry *mdver = CACHE_ENTRY_PAYLOAD(crtEntry);

		/*
		 * Lock entry in order to read its payload
		 * Don't call any functions that can get interrupted or
		 * that palloc memory while holding this lock.
		 */
		Cache_LockEntry(cache, crtEntry);

		if (!Cache_ShouldListEntry(crtEntry))
		{
			Cache_UnlockEntry(cache, crtEntry);
			continue;
		}

		values[0] = ObjectIdGetDatum(mdver->key);
		values[1] = UInt64GetDatum(mdver->ddl_version);
		values[2] = UInt64GetDatum(mdver->dml_version);

		/* Done reading from the payload of the entry, release lock */
		Cache_UnlockEntry(cache, crtEntry);

		HeapTuple tuple = heap_form_tuple(funcctx->tuple_desc, values, nulls);
		Datum result = HeapTupleGetDatum(tuple);
		SRF_RETURN_NEXT(funcctx, result);
	}
}


/* EOF */
