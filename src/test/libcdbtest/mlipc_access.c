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
 * mlipc_access.c
 *     Test functions for Motion Layer IPC API.  These are to be called from SQL
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
#include "cdb/cdbmotion.h"
#include "cdb/cdblink.h"
#include "cdb/cdbutil.h"
#include "cdb/ml_ipc.h"
#include "executor/spi.h"
#include "libpq/pqformat.h"
#include "utils/builtins.h"

#include "cdbtest.h"

#include <sys/time.h>


#define GET_STR(textp) DatumGetCString(DirectFunctionCall1(textout, PointerGetDatum(textp)))


/* 
 * NOTE: This function assumes/requires the following to be true:
 *    (1) This function MUST be called in utility mode.!
 *
 *    (2) This function assumes that Motion Layer setup and cdblink setup has
 *        already occured magically.
 *
 *    (3) 
 * 
 */
PG_FUNCTION_INFO_V1(ml_ipc_bench);
Datum ml_ipc_bench(PG_FUNCTION_ARGS)
{
    FuncCallContext     *funcctx;
    MemoryContext   oldcontext;
    
    int                  call_cntr;
    int                  max_calls;
    TupleDesc            tupdesc;
    AttInMetadata       *attinmeta;
    uint32 num_msgs;
    uint32 data_size;
    int i;
	int mySegIndex;
	bool isPrimary;
	
	int msgsReceived = 0;
	int msgsSent = 0;
	char *msg;
	uint64 totalMsec;
	
	GpRoleValue oldRole = Gp_role;
	
	/* temporarily disable this routine. see TODO item below. */
	ereport(ERROR, (errcode(ERRCODE_GP_FEATURE_NOT_YET),
					errmsg("Interconnect testing through libcdbtest is currently disabled")
					));
	
    if( SRF_IS_FIRSTCALL() )
    {
        
        /* create a function context for cross-call persistence */
        funcctx = SRF_FIRSTCALL_INIT();

        /* switch to memory context appropriate for multiple function calls */
        oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

        /* total number of tuples to be returned */
        funcctx->max_calls = 1;

        /* Build a tuple description for a __testpassbyval tuple */
        tupdesc = RelationNameGetTupleDesc("__ml_ipc_bench_results");
        funcctx->tuple_desc = BlessTupleDesc(tupdesc);
    
        /*
         * generate attribute metadata needed later to produce tuples from raw
         * C strings
         */
        attinmeta = TupleDescGetAttInMetadata(tupdesc);
        funcctx->attinmeta = attinmeta;

        //read in args
        num_msgs = PG_GETARG_UINT32(0);
	    data_size = PG_GETARG_UINT32(1); 


		
        //set things up
	    getGpIdentity( &mySegIndex, &isPrimary );

        //create our test message
        msg = palloc(data_size);
        for( i=0; i < data_size; i++ )
        {
            //TODO: create better test messages.
            *(msg+i) = 'a';
        }
        
        
        /* Form a tuplechunk with the message as a payload */
        TupleChunkListItem tcItem = (TupleChunkListItem) 
                                        palloc0(sizeof(TupleChunkListData) + 
                                        TUPLE_CHUNK_HEADER_SIZE +
                                        strlen(msg));
    
        tcItem->chunk_length = strlen(msg) + TUPLE_CHUNK_HEADER_SIZE;
        
		
		
        SetTargetSegIdx(tcItem->chunk_data, mySegIndex);
        SetQueryGroupID(tcItem->chunk_data, 1);
        SetSourceSegIdx(tcItem->chunk_data, mySegIndex);
        SetMotionNodeID(tcItem->chunk_data, 1);
        SetChunkDataSize(tcItem->chunk_data, strlen(msg) );
        SetChunkType(tcItem->chunk_data, TC_WHOLE );
    
        memcpy( tcItem->chunk_data + TUPLE_CHUNK_HEADER_SIZE, msg, strlen(msg) );
    
        bool done = false;
        TupleChunkListItem recvTcItem;
		TupleChunkListItem nextTcItem;
        TupleChunkListItem lastTcItem;
        
        //stat variables
        struct timeval startTime;
        struct timeval stopTime;
    
        gettimeofday(&startTime, NULL);
        
        
        while( ! done )
        {
           
           //try and send out
           if( msgsSent != num_msgs)
           {	
			   /* TODO: we need to enable this call again in order to support this test routine!
               if( SendTupleChunkToAMS( tcItem ) ){
				   msgsSent++;                   
               }
			   */
           }
           
           
           //try and receive
           if( (recvTcItem = RecvTupleChunkFromAMS() ) != NULL )
           {
			   nextTcItem = recvTcItem;
               lastTcItem = recvTcItem;
               //need to go through recvTcItem and count the items
               while( nextTcItem != NULL )
               {                
                   msgsReceived++;	                
				   lastTcItem = nextTcItem;
				   nextTcItem = lastTcItem->p_next;
				   
				   if( msgsReceived % 1000 == 0 )
					   elog( NOTICE, "recvd: %d", msgsReceived );
               }  
			   
			   clearTCList(recvTcItem);
           }
           
           //test for exit
           if( msgsSent == num_msgs && msgsReceived == num_msgs )
               done = true;
        
        }
                
        gettimeofday(&stopTime, NULL);
        
		
        
        totalMsec = ( (stopTime.tv_sec - startTime.tv_sec) * 1000) + 
                    ( (stopTime.tv_usec - startTime.tv_usec) / 1000 );
	    
	    pfree(tcItem);
	    pfree(msg);
        
        MemoryContextSwitchTo(oldcontext);
    
    }
    
    
    /* stuff done on every call of the function */
    funcctx = SRF_PERCALL_SETUP();

    call_cntr = funcctx->call_cntr;
    max_calls = funcctx->max_calls;
    attinmeta = funcctx->attinmeta;
	
	
	if (call_cntr < max_calls)    /* do when there is more left to send */
    {
        
        
        Datum result;
        HeapTuple tuple;
        
        Datum resultValues[3];          /* Parts of the result set. */
        char *resultNulls = "    ";     /* No NULLs in result set. */
        
        int64 bytes = num_msgs * data_size;
                
        double MBs = (double)bytes / (1024*1024);
        double Secs = (double)totalMsec / 1000;        
        double bw = (double)MBs / Secs;
        
        resultValues[0] = Int64GetDatum( totalMsec );
        resultValues[1] = Float64GetDatum( &MBs );
        resultValues[2] = Float64GetDatum( &bw );
        
        
        tuple = heap_formtuple(funcctx->tuple_desc, resultValues, resultNulls);
        
        /* make the tuple into a datum */
        result = HeapTupleGetDatum(tuple);
	
	
        SRF_RETURN_NEXT(funcctx, result);
    }
    else    /* do when there is no more left */
    {
        SRF_RETURN_DONE(funcctx);
    }
    
		
}
