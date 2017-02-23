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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/*-------------------------------------------------------------------------
 *
 * mppexecplan.c
 *
 *
 *     Copyright (c) 2001-2003, PostgreSQL Global Development Group
 *     ALL RIGHTS RESERVED;
 *
 *     Permission to use, copy, modify, and distribute this software and its
 *     documentation for any purpose, without fee, and without a written agreement
 *     is hereby granted, provided that the above copyright notice and this
 *     paragraph and the following two paragraphs appear in all copies.
 *
 *     IN NO EVENT SHALL THE AUTHOR OR DISTRIBUTORS BE LIABLE TO ANY PARTY FOR
 *     DIRECT, INDIRECT, SPECIAL, INCIDENTAL, OR CONSEQUENTIAL DAMAGES, INCLUDING
 *     LOST PROFITS, ARISING OUT OF THE USE OF THIS SOFTWARE AND ITS
 *     DOCUMENTATION, EVEN IF THE AUTHOR OR DISTRIBUTORS HAVE BEEN ADVISED OF THE
 *     POSSIBILITY OF SUCH DAMAGE.
 *
 *     THE AUTHOR AND DISTRIBUTORS SPECIFICALLY DISCLAIMS ANY WARRANTIES,
 *     INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY
 *     AND FITNESS FOR A PARTICULAR PURPOSE.  THE SOFTWARE PROVIDED HEREUNDER IS
 *     ON AN "AS IS" BASIS, AND THE AUTHOR AND DISTRIBUTORS HAS NO OBLIGATIONS TO
 *     PROVIDE MAINTENANCE, SUPPORT, UPDATES, ENHANCEMENTS, OR MODIFICATIONS.
 *
 */
#include "postgres.h"

#include <ctype.h>
#include <assert.h>

#include "libpq-fe.h"
#include "libpq-int.h"
#include "fmgr.h"
#include "funcapi.h"
#include "miscadmin.h"
#include "access/tupdesc.h"
#include "commands/async.h"
#include "catalog/pg_type.h"
#include "lib/stringinfo.h"
#include "storage/ipc.h"
#include "tcop/tcopprot.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "executor/spi.h"
#include "executor/spi_priv.h"
#include "nodes/print.h"
#include "cdb/cdbsrlz.h"

#include "cdbexecplan.h"

/* general utility */
#define GET_STR(textp) DatumGetCString(DirectFunctionCall1(textout, PointerGetDatum(textp)))

/*
 * cdb_serialize_plan takes 1 argument:
 *  1) an SQL statement
 * And returns the serialized plan tree.
 */
PG_FUNCTION_INFO_V1(cdb_serialize_plan);
Datum
cdb_serialize_plan(PG_FUNCTION_ARGS)
{
	char* pszSQL = NULL;
	char* pszOutput = NULL;
	bool bPretty = true;

	/*
	 * Grab the current memory context.  So we can create the node string in it,
	 * rather than in the spi context that will go away when we call 
	 * SPI_finish
	 */
	MemoryContext myContext = CurrentMemoryContext;
	MemoryContext oldContext;
	
	pszSQL	= GET_STR(PG_GETARG_TEXT_P(0));
	bPretty	= PG_GETARG_BOOL(1);

	if ( SPI_OK_CONNECT != SPI_connect() )
	{
		ereport(ERROR,
				(errcode(ERRCODE_EXTERNAL_ROUTINE_EXCEPTION),
				errmsg("SPI_connect failed in cdb_serialize_plan" )));
	}
	
	_SPI_plan*	thePlan = (_SPI_plan *)SPI_prepare(pszSQL, 0, NULL);
	if ( thePlan == NULL )
	{
		ereport(ERROR,
				(errcode(ERRCODE_EXTERNAL_ROUTINE_EXCEPTION),
				errmsg("SPI_prepare failed for sql %s", pszSQL )));
	}
		
	if ( list_length(thePlan->ptlist) != 1 )
	{
		ereport(ERROR,
				(errcode(ERRCODE_EXTERNAL_ROUTINE_EXCEPTION),
				errmsg("cdb_serialize_plan can only handle 1 sql statement, but there were %d", list_length(thePlan->ptlist) )));
	}
	
	ListCell   *plan_list_item = list_head(thePlan->ptlist);

	oldContext = MemoryContextSwitchTo(myContext);
	pszOutput = serializeNode(lfirst(plan_list_item), NULL, NULL /*uncompressed_size*/);
	MemoryContextSwitchTo(oldContext);

	if ( pszOutput == NULL )
	{
		ereport(ERROR,
				(errcode(ERRCODE_EXTERNAL_ROUTINE_EXCEPTION),
				errmsg("TestPlanSerialization failed for sql %s", pszSQL )));
	}
	
	SPI_freeplan(thePlan);
	SPI_finish();

	char *f;
	if ( bPretty )
		f = pretty_format_node_dump(pszOutput);
	else
		f = pstrdup(pszOutput);

	int lenOutput = strlen(f);
	int len = lenOutput + VARHDRSZ;
	text* result = (text *) palloc(len);
	if ( result == NULL )
	{
		ereport(ERROR,
				(errcode(ERRCODE_OUT_OF_MEMORY),
				 errmsg("out of memory")));
	}

	/* Set size of result string... */
	SET_VARSIZE(result, len);

	/* Fill data field of result string... */
	char* ptr = VARDATA(result);
	memcpy(ptr, f, lenOutput);

	pfree(pszOutput);
	pfree(f);
	pfree(pszSQL);

	PG_RETURN_TEXT_P(result);
}

/*
 * cdb_serialize_query takes 1 argument:
 *  1) an SQL statement
 * And returns the serialized query tree.
 */
PG_FUNCTION_INFO_V1(cdb_serialize_query);
Datum
cdb_serialize_query(PG_FUNCTION_ARGS)
{
	char* pszOutput = NULL;
	char* pszSQL = NULL;
	bool bPretty = true;
	
	/*
	 * Grab the current memory context.  So we can create the node string in it,
	 * rather than in the spi context that will go away when we call 
	 * SPI_finish
	 */
	MemoryContext myContext = CurrentMemoryContext;
	MemoryContext oldContext;

	pszSQL = GET_STR(PG_GETARG_TEXT_P(0));
	bPretty	= PG_GETARG_BOOL(1);

	if ( SPI_OK_CONNECT != SPI_connect() )
	{
		ereport(ERROR,
				(errcode(ERRCODE_EXTERNAL_ROUTINE_EXCEPTION),
				errmsg("SPI_connect failed in cdb_serialize_query" )));
	}
	
	_SPI_plan*	thePlan = (_SPI_plan *)SPI_prepare(pszSQL, 0, NULL);
	if ( thePlan == NULL )
	{
		ereport(ERROR,
				(errcode(ERRCODE_EXTERNAL_ROUTINE_EXCEPTION),
				errmsg("SPI_prepare failed for sql %s", pszSQL )));
	}
		
	List	   *query_list_list = thePlan->qtlist;
	if ( list_length(query_list_list) != 1 )
	{
		ereport(ERROR,
				(errcode(ERRCODE_EXTERNAL_ROUTINE_EXCEPTION),
				errmsg("cdb_serialize_query can only handle 1 sql statement, but there were %d", list_length(query_list_list) )));
	}
	
	ListCell   *query_list_list_item;
	foreach(query_list_list_item, query_list_list)
	{
		List	   *query_list = lfirst(query_list_list_item);
		ListCell   *query_list_item;
		
		if ( list_length(query_list) != 1 )
		{
			ereport(ERROR,
					(errcode(ERRCODE_EXTERNAL_ROUTINE_EXCEPTION),
					errmsg("cdb_serialize_query can only handle 1 sql statement, but there were %d", list_length(query_list) )));
		}
		
		foreach(query_list_item, query_list)
		{
			oldContext = MemoryContextSwitchTo(myContext);
			pszOutput = serializeNode(lfirst(query_list_item), NULL, NULL /*uncompressed_size*/);
			MemoryContextSwitchTo(oldContext);

			if ( pszOutput == NULL )
			{
				ereport(ERROR,
						(errcode(ERRCODE_EXTERNAL_ROUTINE_EXCEPTION),
						errmsg("TestQuerySerialization failed for sql %s", pszSQL )));
			}
			break;
		}
		break;
	}
		
	SPI_freeplan(thePlan);
	SPI_finish();

	char *f;
	if ( bPretty )
		f = pretty_format_node_dump(pszOutput);
	else
		f = pstrdup(pszOutput);

	int lenOutput = strlen(f);
	int len = lenOutput + VARHDRSZ;
	text* result = (text *) palloc(len);
	if ( result == NULL )
	{
		ereport(ERROR,
				(errcode(ERRCODE_OUT_OF_MEMORY),
				 errmsg("out of memory")));
	}

	/* Set size of result string... */
	SET_VARSIZE(result, len);

	/* Fill data field of result string... */
	char* ptr = VARDATA(result);
	memcpy(ptr, f, lenOutput);

	pfree(pszOutput);
	pfree(f);
	pfree(pszSQL);

	PG_RETURN_TEXT_P(result);
}


typedef struct 
{
	int index;
	int rows;
} QueryInfo;

#if 0  /* Retired for 3.4 -- must be updated w.r.t. PlannedStmt in order to revive. */
/*
 * cdb_exec_indirect takes 1 argument:
 *  1) an SQL statement
 * And returns a rowset of the query results
 * resulting from
 *	a) preparing the statment, and grabbing the plan and query trees
 *		from this.
 *	b) serializing both the plan and query trees.
 *  c) deserializing the plan and query tree strings into plan and query nodes.
 *  d) executing the query using these plan and query nodes.
 * This can be compared against the rowset resulting from executing the query directly.
 * If they match, the serialization may be correct.  If they don't, it is definitely wrong.
 */
PG_FUNCTION_INFO_V1(cdb_exec_indirect);
Datum
cdb_exec_indirect(PG_FUNCTION_ARGS)
{
    FuncCallContext *funcctx;
    MemoryContext oldcontext;

	char* pszSQL = NULL;
	char* pszPlan = NULL;
	char* pszQuery = NULL;
	
    /* stuff done only on the first call of the function */
    if (SRF_IS_FIRSTCALL())
    {
        /* create a function context for cross-call persistence */
        funcctx = SRF_FIRSTCALL_INIT();

		pszSQL = GET_STR(PG_GETARG_TEXT_P(0));

		if ( SPI_OK_CONNECT != SPI_connect() )
		{
			ereport(ERROR,
					(errcode(ERRCODE_EXTERNAL_ROUTINE_EXCEPTION),
					errmsg("SPI_connect failed in cdb_exec_indirect" )));
		}

		/*
		 * Prepare the SQL using SPI
		 */
	
		_SPI_plan*	thePlan = (_SPI_plan *)SPI_prepare(pszSQL, 0, NULL);
		if ( thePlan == NULL )
		{
			ereport(ERROR,
					(errcode(ERRCODE_EXTERNAL_ROUTINE_EXCEPTION),
					errmsg("SPI_prepare failed for sql %s", pszSQL )));
		}
		
		/*
		 * Grab the plan tree from the _SPI_plan struct.
		 * Fail if there is more than 1.
		 */
		if ( list_length(thePlan->ptlist) != 1 )
		{
			ereport(ERROR,
					(errcode(ERRCODE_EXTERNAL_ROUTINE_EXCEPTION),
					errmsg("cdb_exec_indirect can only handle 1 sql statement, but there were %d", list_length(thePlan->ptlist) )));
		}
	
		ListCell   *plan_list_item = list_head(thePlan->ptlist);

		/*
		 * Serialize the plan tree to a string
		 */
		pszPlan = serializeNode(lfirst(plan_list_item), NULL);

		if ( pszPlan == NULL )
		{
			ereport(ERROR,
					(errcode(ERRCODE_EXTERNAL_ROUTINE_EXCEPTION),
					errmsg("serializeNode failed for sql %s", pszSQL )));
		}
		
		/*
		 * Grab the query tree from the _SPI_plan struct.
		 * Fail if there is more than 1.
		 */
		List	   *query_list_list = thePlan->qtlist;
		if ( list_length(query_list_list) != 1 )
		{
			ereport(ERROR,
					(errcode(ERRCODE_EXTERNAL_ROUTINE_EXCEPTION),
					errmsg("cdb_exec_indirect can only handle 1 sql statement, but there were %d", list_length(query_list_list) )));
		}
	
		ListCell   *query_list_list_item;
		foreach(query_list_list_item, query_list_list)
		{
			List	   *query_list = lfirst(query_list_list_item);
			ListCell   *query_list_item;
		
			if ( list_length(query_list) != 1 )
			{
				ereport(ERROR,
						(errcode(ERRCODE_EXTERNAL_ROUTINE_EXCEPTION),
						errmsg("cdb_exec_indirect can only handle 1 sql statement, but there were %d", list_length(query_list) )));
			}
		
			foreach(query_list_item, query_list)
			{
				/*
				* Serialize the query tree to a string
				*/
				pszQuery = serializeNode(lfirst(query_list_item), NULL);

				if ( pszQuery == NULL )
				{
					ereport(ERROR,
							(errcode(ERRCODE_EXTERNAL_ROUTINE_EXCEPTION),
							errmsg("serializeNode failed for sql %s", pszSQL )));
				}
				break;
			}
			break;
		}

		SPI_freeplan(thePlan);
	
		/*
		 * Now reverse the process: deserialize the plan and query tree strings
		 * back into nodes.
		 */ 
		Plan *pNewPlan = (Plan *)deserializeNode( pszPlan );
		Query *pNewQuery = (Query *)deserializeNode( pszQuery );
		if ( pNewPlan == NULL || pNewQuery == NULL )
		{
			ereport(ERROR,
					(errcode(ERRCODE_EXTERNAL_ROUTINE_EXCEPTION),
					errmsg("Plan or Query XML deserialization failed in cdb_exec_indirect" )));
		}

		/*
		 * Execute through our cdb version of SPI_execute
		 */ 
		if ( SPI_OK_SELECT != SPI_execute_cdb( pszSQL, pNewPlan, pNewQuery, false, 0 ) )
		{
			ereport(ERROR,
					(errcode(ERRCODE_EXTERNAL_ROUTINE_EXCEPTION),
					errmsg("SPI_execute_cdb failed in cdb_exec_plan" )));
		}


        /* switch to memory context appropriate for multiple function calls */
        oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

        funcctx->tuple_desc = BlessTupleDesc(SPI_tuptable->tupdesc);


		/* Allocate cross-call state, so that we can keep track of where we're
         * at in the processing.
         */
        QueryInfo *query_block = (QueryInfo *) palloc0( sizeof(QueryInfo) );
		funcctx->user_fctx = (int *)query_block;
		
		query_block->index	= 0;
		query_block->rows	= SPI_processed;
		MemoryContextSwitchTo(oldcontext);
	}

	/*---------------------*
     * Per-call operations
     */
    
    funcctx = SRF_PERCALL_SETUP();

	QueryInfo *query_block = (QueryInfo *)funcctx->user_fctx;
	if ( query_block->index < query_block->rows )
	{
		/*
		 * Get heaptuple from SPI, then deform it, and reform it using
		 * our tuple desc.
		 * If we don't do this, but rather try to pass the tuple from SPI
		 * directly back, we get an error because
		 * the tuple desc that is associated with the SPI call
		 * has not been blessed.
		 */
		HeapTuple tuple = SPI_tuptable->vals[query_block->index++];
		TupleDesc tupleDesc = funcctx->tuple_desc;

		Datum *values = (Datum *) palloc(tupleDesc->natts * sizeof(Datum));
		char *nulls = (char *) palloc(tupleDesc->natts * sizeof(char));

		heap_deformtuple(tuple, tupleDesc, values, nulls);

		HeapTuple res = heap_formtuple(tupleDesc, values, nulls );

		pfree(values);
		pfree(nulls);
		
		/* make the tuple into a datum */
		Datum result  = HeapTupleGetDatum(res);

		SRF_RETURN_NEXT(funcctx, result);
	}
	
	/* 
	 * do when there is no more left 
	 */

	pfree(query_block);

	SPI_finish();
	
	funcctx->user_fctx = NULL;
	
	SRF_RETURN_DONE(funcctx);
}
#endif

PG_FUNCTION_INFO_V1(cdb_test_mppexec);
Datum
cdb_test_mppexec(PG_FUNCTION_ARGS)
{
	char* pszSQL = NULL;
	char* pszPlan = NULL;
	char* pszQuery = NULL;
	
	pszSQL = GET_STR(PG_GETARG_TEXT_P(0));

	if ( SPI_OK_CONNECT != SPI_connect() )
	{
		ereport(ERROR,
				(errcode(ERRCODE_EXTERNAL_ROUTINE_EXCEPTION),
				errmsg("SPI_connect failed in cdb_test_mppexec" )));
	}

	/*
	* Prepare the SQL using SPI
	*/
	_SPI_plan*	thePlan = (_SPI_plan *)SPI_prepare(pszSQL, 0, NULL);
	if ( thePlan == NULL )
	{
		ereport(ERROR,
				(errcode(ERRCODE_EXTERNAL_ROUTINE_EXCEPTION),
				errmsg("SPI_prepare failed for sql %s", pszSQL )));
	}
		
	/*
	 * Grab the plan tree from the _SPI_plan struct.
	 * Fail if there is more than 1.
	 */
	if ( list_length(thePlan->ptlist) != 1 )
	{
		ereport(ERROR,
				(errcode(ERRCODE_EXTERNAL_ROUTINE_EXCEPTION),
				errmsg("cdb_test_mppexec can only handle 1 sql statement, but there were %d", list_length(thePlan->ptlist) )));
	}
	
	ListCell   *plan_list_item = list_head(thePlan->ptlist);

	/*
	* Serialize the plan tree to a string
	*/
	pszPlan = serializeNode(lfirst(plan_list_item), NULL, NULL /*uncompressed_size*/);

	if ( pszPlan == NULL )
	{
		ereport(ERROR,
				(errcode(ERRCODE_EXTERNAL_ROUTINE_EXCEPTION),
				errmsg("serializeNode failed for sql %s", pszSQL )));
	}
		
	/*
	* Grab the query tree from the _SPI_plan struct.
	* Fail if there is more than 1.
	*/
	List	   *query_list_list = thePlan->qtlist;
	if ( list_length(query_list_list) != 1 )
	{
		ereport(ERROR,
				(errcode(ERRCODE_EXTERNAL_ROUTINE_EXCEPTION),
				errmsg("cdb_test_mppexec can only handle 1 sql statement, but there were %d", list_length(query_list_list) )));
	}
	
	ListCell   *query_list_list_item;
	foreach(query_list_list_item, query_list_list)
	{
		List	   *query_list = lfirst(query_list_list_item);
		ListCell   *query_list_item;
	
		if ( list_length(query_list) != 1 )
		{
			ereport(ERROR,
					(errcode(ERRCODE_EXTERNAL_ROUTINE_EXCEPTION),
					errmsg("cdb_test_mppexec can only handle 1 sql statement, but there were %d", list_length(query_list) )));
		}
		
		foreach(query_list_item, query_list)
		{
			/*
			* Serialize the query tree to a string
			*/
			pszQuery = serializeNode(lfirst(query_list_item), NULL, NULL /*uncompressed_size*/);

			if ( pszQuery == NULL )
			{
				ereport(ERROR,
						(errcode(ERRCODE_EXTERNAL_ROUTINE_EXCEPTION),
						errmsg("serializeNode failed for sql %s", pszSQL )));
			}
			break;
		}
		break;
	}

	int x;

	StringInfoData buffer;
	initStringInfo( &buffer );

	appendStringInfo( &buffer, "mppexec '%s' '%s'", pszQuery, pszPlan);

	if ( SPI_OK_SELECT != SPI_execute( buffer.data, false, 0 ) )
	{
		ereport(ERROR,
				(errcode(ERRCODE_EXTERNAL_ROUTINE_EXCEPTION),
				errmsg("SPI_execute failed in cdb_test_mppexec" )));
	}

	bool bRtn = true;	
				
	x = SPI_processed;
	elog( INFO, "Rows returned in SPI_execute_plan were %d", x );

	SPI_finish();
	
	pfree(pszSQL);
	
	PG_RETURN_BOOL( bRtn );
	
}

