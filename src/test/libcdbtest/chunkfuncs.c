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
 * chunkfuncs.c
 *     Test functions for tuple chunk API.  These are to be called from SQL
 *     for testing purposes.
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
#include "libpq-fe.h"
#include "access/tupdesc.h"
#include "cdb/cdbvars.h"
#include "cdb/tupchunk.h"
#include "cdb/tupchunklist.h"
#include "cdb/tupser.h"
#include "executor/spi.h"
#include "libpq/pqformat.h"
#include "utils/builtins.h"

#include "cdbtest.h"

#define GET_STR(textp) DatumGetCString(DirectFunctionCall1(textout, PointerGetDatum(textp)))

extern int Gp_max_tuple_chunk_size;

typedef struct ChunkedRowState
{
    int maxChunkDataSize;
    uint16 srcSegIdx;
    uint16 tgtSegIdx;
    uint16 mnid;

	/* The SerTupInfo with cached info in it for serialization. */
	SerTupInfo serInfo;
	
    /* Index of row we are on. */
    int rowIdx;

    /* Count of which chunk in the current row we are on. */
    int chunkNum;

    /* List of tuple-chunks for the current row. */
    TupleChunkList rowChunks;
    TupleChunkListItem nextChunk;
} ChunkedRowState;

/* Static state for GetRowsAsChunks function. */
static ChunkedRowState *s_rac_state;

typedef struct DechunkedRowState
{
	/* The SerTupInfo with cached info in it for serialization. */
	SerTupInfo serInfo;
	
    /* Index of row we are on. */
    int rowIdx;
} DechunkedRowState;

/* Static state for GetDechunkedRows function. */
static DechunkedRowState *s_dcr_state;


typedef struct DeserializedRowState
{
	/* The SerTupInfo with cached info in it for serialization. */
	SerTupInfo serInfo;
	
    /* Index of row we are on. */
    int rowIdx;
} DeserializedRowState;

/* Static state for GetDeserializedRows function. */
static DeserializedRowState *s_dr_state;


/* This function executes a SQL "SELECT" statement and returns the results as
 * a collection of tuple chunks.  The returned rows have the following format:
 *
 * +------------------+--------------------+---------------------+-------------------+
 * | rownum (INTEGER) | chunknum (INTEGER) | chunksize (INTEGER) | chunkdata (BYTEA) |
 * +------------------+--------------------+---------------------+-------------------+
 *
 * This is defined as the "__getchunkedrows" data-type.
 *
 * Both rownum and chunknum start at 1.
 */
PG_FUNCTION_INFO_V1(GetChunkedRows__text1);
Datum GetChunkedRows__text1(PG_FUNCTION_ARGS)
{
    FuncCallContext *funcctx;
    MemoryContext oldcontext;

    char *pszSQL;
    int rc;

    TupleDesc tupdesc;
    HeapTuple htup;

    Datum resultValues[4];          /* Parts of the result set. */
    char *resultNulls = "    ";     /* No NULLs in result set. */
    Datum resultTuple;              /* The full result. */


    /* stuff done only on the first call of the function */
    if (SRF_IS_FIRSTCALL())
    {
        /* create a function context for cross-call persistence */
        funcctx = SRF_FIRSTCALL_INIT();

        /* switch to memory context appropriate for multiple function calls */
        oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

        /*---------------------*
         * One-time operations
         */
        
        /* Get the SQL to execute, and run it. */
        pszSQL = GET_STR(PG_GETARG_TEXT_P(0));
        rc = SPI_connect();
        if (rc != SPI_OK_CONNECT)
            elog(ERROR, "SPI_connect() reported error.");
        
        /* Execute read-only SQL.  The 0 means "no limit on results." */
        rc = SPI_execute(pszSQL, true, 0);
        if (rc != SPI_OK_SELECT)
            elog(ERROR, "This function requires a SELECT statement.");
        
        /* Allocate cross-call state, so that we can keep track of where we're
         * at in the processing.
         */
        
        s_rac_state = (ChunkedRowState *) palloc0(sizeof(ChunkedRowState));
        s_rac_state->rowIdx = 0;
        s_rac_state->rowChunks = NULL;
        s_rac_state->chunkNum = 0;
        
        s_rac_state->maxChunkDataSize = Gp_max_tuple_chunk_size;
        
		InitSerTupInfo(SPI_tuptable->tupdesc, &s_rac_state->serInfo);
		
        /* Generate return-type information.  This is derived from the user
         * datatype "__getchunkedrows".
         */
        
        tupdesc = RelationNameGetTupleDesc("__getchunkedrows");
        funcctx->tuple_desc = BlessTupleDesc(tupdesc);
    }
    
    
    /*---------------------*
     * Per-call operations
     */
    
    funcctx = SRF_PERCALL_SETUP();
    
    if (s_rac_state->rowChunks == NULL || s_rac_state->nextChunk == NULL)
    {
        /* The chunk list is empty, or we have traversed all chunks in the
         * current row.  Go to the next row, and generate a new list of chunks
         * for that row.
         */

        if (s_rac_state->rowIdx < SPI_processed)
        {
            s_rac_state->rowChunks =
				SerializeTupleIntoChunks(SPI_tuptable->vals[s_rac_state->rowIdx],
								   &s_rac_state->serInfo,
								   Gp_max_tuple_chunk_size);

            s_rac_state->nextChunk = s_rac_state->rowChunks->p_first;
            s_rac_state->chunkNum = 0;
        }
        else
        {
            /* No more rows. */
            
            s_rac_state->rowChunks = NULL;
            s_rac_state->nextChunk = NULL;
            s_rac_state->chunkNum = 0;
        }
    }
    
    
    if (s_rac_state->rowChunks == NULL ||
        s_rac_state->rowChunks->p_first == NULL)
    {
        /* There are no more tuples and no more chunks.  We are done. */

        /* Do final cleanup. */
        SPI_finish();
        s_rac_state = NULL;      /* Freed when mem-ctx is cleaned up. */

        SRF_RETURN_DONE(funcctx);
    }
    else
    {
        /* There are more chunks.  Return the next chunk as a row. */

        StringInfoData serChunkData;
        bytea *chunkData;

        resultValues[0] = Int32GetDatum(s_rac_state->rowIdx + 1);
        resultValues[1] = Int32GetDatum(s_rac_state->chunkNum + 1);
        resultValues[2] = Int32GetDatum(s_rac_state->nextChunk->chunk_length);

        pq_begintypsend(&serChunkData);   /* Does initStringInfo() call. */
        pq_sendbytes(&serChunkData, (const char *)s_rac_state->nextChunk->chunk_data,
					 s_rac_state->nextChunk->chunk_length);
        chunkData = pq_endtypsend(&serChunkData);

        resultValues[3] = PointerGetDatum(chunkData);

        /* Form the tuple. */
        htup = heap_formtuple(funcctx->tuple_desc, resultValues, resultNulls);
        resultTuple = HeapTupleGetDatum(htup);

        /* Go to the next chunk, for the next call to this function. */

        s_rac_state->chunkNum++;

        s_rac_state->nextChunk = s_rac_state->nextChunk->p_next;
        if (s_rac_state->nextChunk == NULL)
            s_rac_state->rowIdx++;
        
        /* Return our result! */
        SRF_RETURN_NEXT(funcctx, resultTuple);
    }
}



/* This function executes a SQL "SELECT" statement that returns rows of
 * serialized (but not chunked) tuple data, and returns the results as
 * deserialized rows.  The input query should produce results of type bytea.
 *
 * This function returns the RECORD data-type; thus, the actual result format
 * must be specified in the SELECT statement, like this:
 *
 *     SELECT * FROM getdeserializedrows('SELECT serialized FROM ser_rows')
 *         AS r (msgid INTEGER, msgread BOOL, msgtext VARCHAR(2000));
 *
 *     select * from getdeserializedrows(
 *         'select serialized from test_ser_rows',
 *         'foo'::regtype)
 *         as foo ( ident integer,
 *                  boolval boolean,
 *                  score integer,
 *                  notes varchar);
 *
 * This way, the function knows what the TupleDesc should be for the tuple it
 * creates.
 */
PG_FUNCTION_INFO_V1(GetDeserializedRows__text1_regtype1);
Datum GetDeserializedRows__text1_regtype1(PG_FUNCTION_ARGS)
{
    FuncCallContext *funcctx;
    MemoryContext oldcontext;

    Oid typeOid;
    char *pszSQL;
    int rc;

    HeapTuple htup;

    Datum resultTuple;              /* The full result. */

    /* stuff done only on the first call of the function */
    if (SRF_IS_FIRSTCALL())
    {
        /* create a function context for cross-call persistence */
        funcctx = SRF_FIRSTCALL_INIT();

        /* switch to memory context appropriate for multiple function calls */
        oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

        /*---------------------*
         * One-time operations
         */

        /* Get the SQL to execute, and run it. */
        pszSQL = GET_STR(PG_GETARG_TEXT_P(0));
        rc = SPI_connect();
        if (rc != SPI_OK_CONNECT)
            elog(ERROR, "SPI_connect() reported error.");

        /* Execute read-only SQL.  The 0 means "no limit on results." */
        rc = SPI_execute(pszSQL, true, 0);
        if (rc != SPI_OK_SELECT)
            elog(ERROR, "This function requires a SELECT statement.");

        /* The SQL must return rows with a single column of type BYTEA. */

        if (SPI_tuptable->tupdesc->natts != 1 ||
            SPI_tuptable->tupdesc->attrs[0]->atttypid != BYTEAOID)
        {
            elog(ERROR, "The SELECT statement result must have exactly one"
                " column of type BYTEA.");
        }

        /* Return-type information must be specified by the caller. */

        typeOid = PG_GETARG_OID(1);
        funcctx->tuple_desc = TypeGetTupleDesc(typeOid, NULL);

        /* Allocate cross-call state, so that we can keep track of where we're
         * at in the processing.
         */

        s_dr_state = (DeserializedRowState *) palloc0(sizeof(DeserializedRowState));
		InitSerTupInfo(funcctx->tuple_desc, &s_dr_state->serInfo);
        s_dr_state->rowIdx = 0;
    }
    
    
    /*---------------------*
     * Per-call operations
     */
    
    funcctx = SRF_PERCALL_SETUP();
    
    if (s_dr_state->rowIdx < SPI_processed)
    {
        /* There are more rows.  Deserialize the next row and return its
         * contents.
         */

        Datum datumSerTup;
        bytea *baSerTup;
        StringInfoData strInfoTup;
        bool isNull;

        datumSerTup = fastgetattr(SPI_tuptable->vals[s_dr_state->rowIdx], 1,
            SPI_tuptable->tupdesc, &isNull);
        baSerTup = DatumGetByteaP(datumSerTup);

        initStringInfo(&strInfoTup);
        appendBinaryStringInfo(&strInfoTup, VARDATA(baSerTup),
            VARSIZE(baSerTup) - VARHDRSZ);

        /* Deserialize the data stream into a tuple. */

        htup = DeserializeTuple(&s_dr_state->serInfo, &strInfoTup);
        resultTuple = HeapTupleGetDatum(htup);

        /* Move on to the next row in the list, for next iteration. */
        s_dr_state->rowIdx++;

        /* Return our result! */
        SRF_RETURN_NEXT(funcctx, resultTuple);
    }
    else
    {
        /* There are no more tuples.  We are done. */

        /* Do final cleanup. */
        SPI_finish();
        s_dr_state = NULL;      /* Freed when mem-ctx is cleaned up. */

        SRF_RETURN_DONE(funcctx);
    }
}



/* This function executes a SQL "SELECT" statement that returns rows of
 * chunked tuple data, and returns the results as dechunked and deserialized
 * rows.  The input query should produce results of type bytea.
 *
 * This function returns the RECORD data-type; thus, the actual result format
 * must be specified in the SELECT statement, like this:
 *
 *     SELECT * FROM getdechunkedrows(
 *         'SELECT serialized FROM test_ser_rows', 'foo'::regtype)
 *         AS foo ( ident INTEGER, boolval BOOLEAN,
 *                  score INTEGER, notes VARCHAR    );
 *
 * This way, the function knows what the TupleDesc should be for the tuple it
 * creates.
 */
PG_FUNCTION_INFO_V1(GetDechunkedRows__text1_regtype1);
Datum GetDechunkedRows__text1_regtype1(PG_FUNCTION_ARGS)
{
    FuncCallContext *funcctx;
    MemoryContext oldcontext;

    Oid typeOid;
    char *pszSQL;
    int rc;

    HeapTuple htup;

    Datum resultTuple;              /* The full result. */

    /* stuff done only on the first call of the function */
    if (SRF_IS_FIRSTCALL())
    {
        /* create a function context for cross-call persistence */
        funcctx = SRF_FIRSTCALL_INIT();

        /* switch to memory context appropriate for multiple function calls */
        oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

        /*---------------------*
         * One-time operations
         */

        /* Get the SQL to execute, and run it. */
        pszSQL = GET_STR(PG_GETARG_TEXT_P(0));
        rc = SPI_connect();
        if (rc != SPI_OK_CONNECT)
            elog(ERROR, "SPI_connect() reported error.");

        /* Execute read-only SQL.  The 0 means "no limit on results." */
        rc = SPI_execute(pszSQL, true, 0);
        if (rc != SPI_OK_SELECT)
            elog(ERROR, "This function requires a SELECT statement.");

        /* The SQL must return rows with a single column of type BYTEA. */

        if (SPI_tuptable->tupdesc->natts != 1 ||
            SPI_tuptable->tupdesc->attrs[0]->atttypid != BYTEAOID)
        {
            elog(ERROR, "The SELECT statement result must have exactly one"
                " column of type BYTEA.");
        }

        /* Return-type information must be specified by the caller. */

        typeOid = PG_GETARG_OID(1);
        funcctx->tuple_desc = TypeGetTupleDesc(typeOid, NULL);

        /* Allocate cross-call state, so that we can keep track of where we're
         * at in the processing.
         */

        s_dcr_state = (DechunkedRowState *) palloc0(sizeof(DechunkedRowState));
        s_dcr_state->rowIdx = 0;
		InitSerTupInfo(funcctx->tuple_desc, &s_dcr_state->serInfo);
    }


    /*---------------------*
     * Per-call operations
     */

    funcctx = SRF_PERCALL_SETUP();

    if (s_dcr_state->rowIdx < SPI_processed)
    {
        /* There are more rows.  Gather all the chunks for the next row and
         * dechunk them, then the row's contents.
         */

        Datum datumChunk;
        bytea *baChunk;
        bool isNull;
        
        TupleChunkListData tcListData;
        TupleChunkListItem tcItem;
        TupleChunkType lastChunkType;

        /* Initialize the StringInfo to take in the input chunks. */

        tcListData.p_first = NULL;
        tcListData.p_last = NULL;
        tcListData.num_chunks = 0;

        do
        {
            datumChunk = fastgetattr(SPI_tuptable->vals[s_dcr_state->rowIdx],
                1, SPI_tuptable->tupdesc, &isNull);
            baChunk = DatumGetByteaP(datumChunk);

            /* Create a new tuple chunk list item. */

            tcItem = palloc0(sizeof(TupleChunkListItemData) +
                VARSIZE(baChunk) - VARHDRSZ);

            tcItem->chunk_length = VARSIZE(baChunk) - VARHDRSZ;

            memcpy(tcItem->chunk_data, VARDATA(baChunk),
                tcItem->chunk_length);

            appendChunkToTCList(&tcListData, tcItem);

            GetChunkType(tcItem, &lastChunkType);

            s_dcr_state->rowIdx++;
        }
        while (lastChunkType != TC_WHOLE && lastChunkType != TC_PARTIAL_END &&
               s_dcr_state->rowIdx < SPI_processed);

        /* Deserialize the chunk sequence into a tuple. */
        htup = CvtChunksToHeapTup(&tcListData, &s_dcr_state->serInfo);
        resultTuple = HeapTupleGetDatum(htup);

        /* Return our result! */
        SRF_RETURN_NEXT(funcctx, resultTuple);
    }
    else
    {
        /* There are no more tuples.  We are done. */

        /* Do final cleanup. */
        SPI_finish();
        s_dcr_state = NULL;      /* Freed when mem-ctx is cleaned up. */

        SRF_RETURN_DONE(funcctx);
    }
}

