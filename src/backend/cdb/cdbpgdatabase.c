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
 * cdbpgdatabase.c
 *		Set-returning function to view pgdatabase table.
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
#include "cdb/cdbfts.h"

Datum		gp_pgdatabase__(PG_FUNCTION_ARGS);

/* Working status for pg_prepared_xact */
typedef struct
{
	Segment		*master;
	Segment		*standby;
	List		*segments;
	int			idx;
}	Working_State;

PG_FUNCTION_INFO_V1(gp_pgdatabase__);

/*
 * pgdatabasev - produce a view of pgdatabase to include transient state
 */
Datum
gp_pgdatabase__(PG_FUNCTION_ARGS)
{
	FuncCallContext *funcctx;
	Working_State *mystatus;

	if (SRF_IS_FIRSTCALL())
	{
		TupleDesc	tupdesc;
		MemoryContext oldcontext;

		/* create a function context for cross-call persistence */
		funcctx = SRF_FIRSTCALL_INIT();

		/*
		 * switch to memory context appropriate for multiple function
		 * calls
		 */
		oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

		/* build tupdesc for result tuples */
		/* this had better match pg_prepared_xacts view in	system_views.sql */
		tupdesc = CreateTemplateTupleDesc(5, false);
		TupleDescInitEntry(tupdesc, (AttrNumber) 1, "dbid",
						   INT2OID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 2, "isprimary",
						   BOOLOID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 3, "content",
						   INT2OID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 4, "valid",
						   BOOLOID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 5, "definedprimary",
						   BOOLOID, -1, 0);

		funcctx->tuple_desc = BlessTupleDesc(tupdesc);

		/*
		 * Collect all the locking information that we will format and
		 * send out as a result set.
		 */
		mystatus = (Working_State *) palloc(sizeof(Working_State));
		funcctx->user_fctx = (void *) mystatus;

		mystatus->master = GetMasterSegment();
		mystatus->standby = GetStandbySegment();
		mystatus->segments = GetSegmentList();
		mystatus->idx = 0;

		MemoryContextSwitchTo(oldcontext);
	}

	funcctx = SRF_PERCALL_SETUP();
	mystatus = (Working_State *) funcctx->user_fctx;

	while (mystatus->master || mystatus->standby || (mystatus->idx < list_length(mystatus->segments)))
	{
		Datum		values[6];
		bool		nulls[6];
		HeapTuple	tuple;
		Datum		result;
		Segment 	*current = NULL;

		if (mystatus->master)
		{
			current = mystatus->master;
			mystatus->master = NULL;
		}
		else if (mystatus->standby)
		{
			current = mystatus->standby;
			mystatus->standby = NULL;
		}
		else
		{
			current = list_nth(mystatus->segments, mystatus->idx);
			mystatus->idx++;
		}

		/*
		 * Form tuple with appropriate data.
		 */
		MemSet(values, 0, sizeof(values));
		MemSet(nulls, false, sizeof(nulls));

		//values[0] = UInt16GetDatum(current->dbid);
		values[1] = current->standby ? false : true;;
		values[2] = UInt16GetDatum(current->segindex);

		values[3] = BoolGetDatum(true);
		values[4] = values[1];

		tuple = heap_form_tuple(funcctx->tuple_desc, values, nulls);
		result = HeapTupleGetDatum(tuple);
		SRF_RETURN_NEXT(funcctx, result);
	}

	SRF_RETURN_DONE(funcctx);
}
