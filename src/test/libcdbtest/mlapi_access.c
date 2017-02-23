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
 * mlapi_access.c
 *     Test functions for accessing the Motion Layer API.
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


#include <sys/time.h>

#include "funcapi.h"

#include "access/heapam.h"
#include "cdb/cdblink.h"
#include "cdb/cdbmotion.h"
#include "executor/spi.h"
#include "utils/memutils.h"

#include "cdbtest.h"


#define GET_STR(textp) DatumGetCString(DirectFunctionCall1(textout, PointerGetDatum(textp)))

/*
 * For now, since Interconnect doesn't support inter-machine operations, we
 * use hard-coded values for certain fields.
 */

#define TEST_PROC_GROUP_ID  0x55AA
#define TEST_HOST_ID        0xAA55


/* Some static variables that are necessary across calls to the send/recv
 * test functions.
 */

static bool s_sendInProgress = false;
static TupleSendContext s_tupSendCtxt;

static ReceiveReturnCode s_last_recvRC = -1;


long getCurrTimestamp(void);


/* Initialize a motion layer node. */
PG_FUNCTION_INFO_V1(MLAPI_InitMLNode__int1_bool1_regtype1);
Datum MLAPI_InitMLNode__int1_bool1_regtype1(PG_FUNCTION_ARGS)
{
    int mnid;
    Oid typeOid;
    TupleDesc tupDesc;
    bool preserveOrder;

    /* Motion node ID */
    mnid = PG_GETARG_INT32(0);

    /* Order-preserving flag */
    preserveOrder = PG_GETARG_BOOL(1);

    /* Tuple-descriptor describing type of all tuples sent and received by
     * this motion node.
     */
    typeOid = PG_GETARG_OID(2);
    tupDesc = TypeGetTupleDesc(typeOid, NULL);

    InitMotionLayerNode(mnid, /*include-backup-segs*/ false, preserveOrder,
        tupDesc);

    PG_RETURN_INT32(mnid);
}


/* Clean up a motion layer node. */
PG_FUNCTION_INFO_V1(MLAPI_EndMLNode__int1);
Datum MLAPI_EndMLNode__int1(PG_FUNCTION_ARGS)
{
    int mnid;

    /* Motion node ID */
    mnid = PG_GETARG_INT32(0);

    EndMotionLayerNode(mnid, /* flush-comm-layer */ false);

    PG_RETURN_INT32(mnid);
}


PG_FUNCTION_INFO_V1(MLAPI_SendTuple__int1_text1);
Datum MLAPI_SendTuple__int1_text1(PG_FUNCTION_ARGS)
{
    int mnid, mySegIdx, rc;
    char *pszSQL;
    SendReturnCode sendRC;
    MemoryContext oldCtxt;

    if (s_sendInProgress)
        elog(ERROR, "An incomplete send-tuple is in progress.");

    /*---------------------*
     * Arguments
     */

    /* Motion node ID */
    mnid = PG_GETARG_INT32(0);

    /* Get the SQL that will generate the tuple to send. */
    pszSQL = GET_STR(PG_GETARG_TEXT_P(1));

    /*---------------------------------*
     * Run the SQL and get a row back.
     */
    
    rc = SPI_connect();
    if (rc != SPI_OK_CONNECT)
        elog(ERROR, "SPI_connect() reported error.");

    /* Execute read-only SQL, and get only the first tuple. */
    rc = SPI_execute(pszSQL, true, 1);
    if (rc != SPI_OK_SELECT)
        elog(ERROR, "This function requires a SELECT statement.");

    /*-----------------------------*
     * Set up motion node info.
     */

    oldCtxt = MemoryContextSwitchTo(CacheMemoryContext);

    getGpIdentity(&mySegIdx, NULL);

    /*-----------------------------*
     * Set up tuple send context.
     */

    bzero(&s_tupSendCtxt, sizeof(s_tupSendCtxt));

    s_tupSendCtxt.heap_tuple = heap_copytuple(SPI_tuptable->vals[0]);
    s_tupSendCtxt.target_segment = mySegIdx;
    s_tupSendCtxt.flags = 0;

    MemoryContextSwitchTo(oldCtxt);

    /*-----------------------------*
     * Do the send!
     */
    
    sendRC = SendTuple(mnid, &s_tupSendCtxt);
    s_sendInProgress = (sendRC == SEND_INCOMPLETE);

    if (sendRC == SEND_COMPLETE)
    {
        /* Time to clean up our intermediate state. */

        heap_freetuple(s_tupSendCtxt.heap_tuple);
        clearTCList(s_tupSendCtxt.tuple_chunk_list);
        bzero(&s_tupSendCtxt, sizeof(s_tupSendCtxt));
    }

    SPI_finish();
    PG_RETURN_INT32(sendRC);
}


PG_FUNCTION_INFO_V1(MLAPI_FinishSendTuple__int1);
Datum MLAPI_FinishSendTuple__int1(PG_FUNCTION_ARGS)
{
    int mnid;
    SendReturnCode sendRC;

    if (!s_sendInProgress)
        elog(ERROR, "No incomplete send-tuple in progress.");

    /*---------------------*
     * Arguments
     */

    /* Motion node ID */
    mnid = PG_GETARG_INT32(0);

    /*-----------------------------*
     * Continue the send!
     */

    sendRC = SendTuple(mnid, &s_tupSendCtxt);
    s_sendInProgress = (sendRC == SEND_INCOMPLETE);

    if (sendRC == SEND_COMPLETE)
    {
        /* Time to clean up our intermediate state. */

        heap_freetuple(s_tupSendCtxt.heap_tuple);
        clearTCList(s_tupSendCtxt.tuple_chunk_list);
        bzero(&s_tupSendCtxt, sizeof(s_tupSendCtxt));
    }

    PG_RETURN_INT32(sendRC);
}


PG_FUNCTION_INFO_V1(MLAPI_RecvTuple__int1);
Datum MLAPI_RecvTuple__int1(PG_FUNCTION_ARGS)
{
    int mnid;
    HeapTuple htup;

    /*---------------------*
     * Arguments
     */

    /* Motion node ID */
    mnid = PG_GETARG_INT32(0);

    /*---------------------*
     * Perform the receive.
     */

    /* Try to receive a tuple. */

    htup = NULL;
    s_last_recvRC = RecvTuple(mnid, &htup);

    if (s_last_recvRC == GOT_TUPLE)
    {
        /* Return the tuple we got. */
        return HeapTupleGetDatum(htup);
    }
    else
    {
        /* Return NULL. */
        PG_RETURN_NULL();
    }
}


PG_FUNCTION_INFO_V1(MLAPI_GetLastRecvCode);
Datum MLAPI_GetLastRecvCode(PG_FUNCTION_ARGS)
{
    PG_RETURN_INT32(s_last_recvRC);
}


/* This SQL function performs a benchmark against the Motion Layer API, and
 * then returns the performance results as a compound-type.
 *
 * Arguments to SQL function:
 *     Arg 0:  Motion-node ID to use.
 *     Arg 1:  SQL to execute, to get the results to move back and forth.
 *
 * Results:
 *     The results are packaged as the following compound-type:
 *         total_tups
 *         total_bytes
 *         tuple_bytes
 *         send_start
 *         send_end
 *         send_ops
 *         recv_start
 *         recv_end
 *         recv_ops
 */
PG_FUNCTION_INFO_V1(MLAPI_SendRecvPerf__int1_text1);
Datum MLAPI_SendRecvPerf__int1_text1(PG_FUNCTION_ARGS)
{
    int mnid, mySegIdx, rc;
    char *pszSQL;
    TupleDesc tupdesc;
    HeapTuple htup;

    TupleSendContext tup_send_ctxt;

    SendReturnCode sendRC;
    ReceiveReturnCode recvRC;

    int tups_sent = 0;      /* Tracks count of tuples sent. */
    int tups_recvd = 0;     /* Tracks count of tuples received. */
    int total_tups;
    
    long recv_ops = 0;      /* Number of times RecvTuple() was called. */
    long send_ops = 0;      /* Number of times SendTuple() was called. */

    /* These vars track amount of bytes sent. */
    long tuple_bytes_sent = 0, total_bytes_sent = 0;
    
    long recvStartTime = 0, recvEndTime = 0;    /* Start/end times, for     */
    long sendStartTime = 0, sendEndTime = 0;    /* bandwidth/latency calcs. */
    
    Datum resultValues[9];              /* Parts of the result set. */
    char *resultNulls = "         ";    /* No NULLs in result set. */
    Datum resultTuple;                  /* The full result. */

    /*---------------------------------
     * Arguments
     */
    
    /* Get the motion-node ID. */
    mnid = PG_GETARG_INT32(0);

    /* Get the SQL to execute. */
    pszSQL = GET_STR(PG_GETARG_TEXT_P(1));

    /* Generate return-type information.  This is derived from the user
     * datatype "__mlapi_sendrecv_perf".
     */
    tupdesc = RelationNameGetTupleDesc("__mlapi_sendrecv_perf");
    tupdesc = BlessTupleDesc(tupdesc);

    /* Execute read-only SQL.  The 0 means "no limit on results." */

    rc = SPI_connect();
    if (rc != SPI_OK_CONNECT)
        elog(ERROR, "SPI_connect() reported error.");

    rc = SPI_execute(pszSQL, true, 0);
    if (rc != SPI_OK_SELECT)
        elog(ERROR, "This function requires a SELECT statement.");

    if (SPI_processed == 0)
        elog(ERROR, "The input SELECT statement produced zero rows.");

    elog(NOTICE, "Performing Motion Layer API benchmark with %d rows.\n\t%s\n",
        SPI_processed, (SPI_processed < 1000 ?
            "(That's not very many.  Are you scared or something?)" :
            "Please wait patiently..."));

    total_tups = SPI_processed;

    /* Start sending and receiving tuple data!  Track stats, too. */

    getGpIdentity(&mySegIdx, NULL);

    bzero(&tup_send_ctxt, sizeof(tup_send_ctxt));
    tup_send_ctxt.heap_tuple = SPI_tuptable->vals[0];
    tup_send_ctxt.target_segment = mySegIdx;

    /* Record current timestamp as send start-time, since we are basically
     * starting to send right away.
     */
    sendStartTime = getCurrTimestamp();
    
    do
    {
        /* Try to do a receive now. */
        if (tups_recvd < SPI_processed)
        {
            recvRC = RecvTuple(mnid, &htup);
            recv_ops++;
            
            switch (recvRC)
            {
            case GOT_TUPLE:
                if (tups_recvd == 0)
                    recvStartTime = getCurrTimestamp();
                
                tups_recvd++;
                
                if (tups_recvd == SPI_processed)
                    recvEndTime = getCurrTimestamp();
                
                /* We don't need the actual tuple, so just free it. */
                heap_freetuple(htup);
                
                break;
                
            case NO_TUPLE:
                /* Nothing to do here.  We already count recv-ops earlier. */
                break;
                
            case END_OF_STREAM:
                elog(ERROR, "Received unexpected end-of-stream!");
                /* The elog() transfers control, so this won't fall thru. */
                
            default:
                elog(ERROR, "Unrecognized result-code from RecvTuple().");
            }
        }

        /* Try to do a send now. */
        if (tups_sent < SPI_processed)
        {
            sendRC = SendTuple(mnid, &tup_send_ctxt);
            send_ops++;

            switch (sendRC)
            {
            case SEND_COMPLETE:
                /* Record statistics for this tuple. */
                
                tups_sent++;

                if (tups_sent == SPI_processed)
                    sendEndTime = getCurrTimestamp();

                tuple_bytes_sent += tup_send_ctxt.tuple_bytes_sent;
                total_bytes_sent += tup_send_ctxt.total_bytes_sent;

                /* Clean up send-context, in preparation for next tuple. */

                clearTCList(tup_send_ctxt.tuple_chunk_list);
                bzero(&tup_send_ctxt, sizeof(tup_send_ctxt));

                /* Set up for the next tuple. */

                tup_send_ctxt.heap_tuple = SPI_tuptable->vals[tups_sent];
                tup_send_ctxt.target_segment = mySegIdx;

                break;

            case SEND_INCOMPLETE:
                /* Nothing to do here.  We already count send-ops earlier. */
                break;

            default:
                elog(ERROR, "Unrecognized result-code from SendTuple().");
            }
        }
    }
    while (tups_sent < total_tups || tups_recvd < total_tups);

    SPI_finish();

    /* Finally, return the results for the performance-test. */

    resultValues[0] = Int32GetDatum(total_tups);
    resultValues[1] = Int64GetDatum(total_bytes_sent);
    resultValues[2] = Int64GetDatum(tuple_bytes_sent);
    resultValues[3] = Int64GetDatum(sendStartTime);
    resultValues[4] = Int64GetDatum(sendEndTime);
    resultValues[5] = Int64GetDatum(send_ops);
    resultValues[6] = Int64GetDatum(recvStartTime);
    resultValues[7] = Int64GetDatum(recvEndTime);
    resultValues[8] = Int64GetDatum(recv_ops);

    /* Form the tuple. */
    htup = heap_formtuple(tupdesc, resultValues, resultNulls);
    resultTuple = HeapTupleGetDatum(htup);

    return resultTuple;
}


/* Helper function to return the current milliseconds timestamp of the system. */
long getCurrTimestamp(void)
{
    struct timeval currTime;
    
    if (gettimeofday(&currTime, NULL) < 0)
        elog(ERROR, "gettimeofday() error!");
    
    /* Combine seconds and microseconds into a milliseconds value. */
    return currTime.tv_sec * 1000 + currTime.tv_usec / 1000;
}
