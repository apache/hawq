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
#include "nodeVMotion.h"
#include "tuplebatch.h"
#include "execVQual.h"
/*=========================================================================
 * FUNCTIONS PROTOTYPES
 */
static TupleTableSlot *execVMotionSender(MotionState * node);
static TupleTableSlot *execVMotionUnsortedReceiver(MotionState * node);

TupleTableSlot *ExecVMotion(MotionState * node);
static void doSendTupleBatch(Motion * motion, MotionState * node, TupleTableSlot *outerTupleSlot);
static SendReturnCode
SendTupleBatch(MotionLayerState *mlStates, ChunkTransportState *transportStates,
               int16 motNodeID, TupleBatch tuplebatch, int16 targetRoute);
/*=========================================================================
 */

TupleTableSlot *
ExecVMotionVirtualLayer(MotionState *node)
{
    if(node->mstype == MOTIONSTATE_SEND)
        return ExecVMotion(node);
    else if(node->mstype == MOTIONSTATE_RECV)
    {
        TupleTableSlot* slot = node->ps.ps_ResultTupleSlot;
        while(1)
        {
            bool succ = VirtualNodeProc(slot);
            if(!succ)
            {
                slot = ExecVMotion(node);
                if(TupIsNull(slot))
                    break;
                else
                    continue;
            }

            break;
        }
        return slot;
    }
}

/* ----------------------------------------------------------------
 *		ExecVMotion
 * ----------------------------------------------------------------
 */
TupleTableSlot *
ExecVMotion(MotionState * node)
{
    Motion	   *motion = (Motion *) node->ps.plan;

    /*
     * at the top here we basically decide: -- SENDER vs. RECEIVER and --
     * SORTED vs. UNSORTED
     */
    if (node->mstype == MOTIONSTATE_RECV)
    {
        TupleTableSlot *tuple;

        if (node->ps.state->active_recv_id >= 0)
        {
            if (node->ps.state->active_recv_id != motion->motionID)
            {
                elog(LOG, "DEADLOCK HAZARD: Updating active_motion_id from %d to %d",
                     node->ps.state->active_recv_id, motion->motionID);
                node->ps.state->active_recv_id = motion->motionID;
            }
        } else
            node->ps.state->active_recv_id = motion->motionID;

        /* Running in diagnostic mode ? */
        if (Gp_interconnect_type == INTERCONNECT_TYPE_NIL)
        {
            node->ps.state->active_recv_id = -1;
            return NULL;
        }

        Assert(!motion->sendSorted);

        tuple = execVMotionUnsortedReceiver(node);

        if (tuple == NULL)
            node->ps.state->active_recv_id = -1;
        else
        {
            Gpmon_M_Incr(GpmonPktFromMotionState(node), GPMON_QEXEC_M_ROWSIN);
            Gpmon_M_Incr_Rows_Out(GpmonPktFromMotionState(node));
            setMotionStatsForGpmon(node);
        }
#ifdef MEASURE_MOTION_TIME
        gettimeofday(&stopTime, NULL);

		node->motionTime.tv_sec += stopTime.tv_sec - startTime.tv_sec;
		node->motionTime.tv_usec += stopTime.tv_usec - startTime.tv_usec;

		while (node->motionTime.tv_usec < 0)
		{
			node->motionTime.tv_usec += 1000000;
			node->motionTime.tv_sec--;
		}

		while (node->motionTime.tv_usec >= 1000000)
		{
			node->motionTime.tv_usec -= 1000000;
			node->motionTime.tv_sec++;
		}
#endif
        CheckSendPlanStateGpmonPkt(&node->ps);
        return tuple;
    }
    else if(node->mstype == MOTIONSTATE_SEND)
    {
        return execVMotionSender(node);
    }

    Assert(!"Non-active motion is executed");
    return NULL;
}

static TupleTableSlot *
execVMotionSender(MotionState * node)
{
    /* SENDER LOGIC */
    TupleTableSlot *outerTupleSlot;
    PlanState  *outerNode;
    Motion	   *motion = (Motion *) node->ps.plan;
    bool		done = false;


#ifdef MEASURE_MOTION_TIME
    struct timeval time1;
	struct timeval time2;

	gettimeofday(&time1, NULL);
#endif

    AssertState(motion->motionType == MOTIONTYPE_HASH ||
                (motion->motionType == MOTIONTYPE_EXPLICIT && motion->segidColIdx > 0) ||
                (motion->motionType == MOTIONTYPE_FIXED && motion->numOutputSegs <= 1));
    Assert(node->ps.state->interconnect_context);

    while (!done)
    {
        /* grab TupleTableSlot from our child. */
        outerNode = outerPlanState(node);
        outerTupleSlot = ExecProcNode(outerNode);

        /* Running in diagnostic mode, we just drop all tuples. */
        if (Gp_interconnect_type == INTERCONNECT_TYPE_NIL)
        {
            if (!TupIsNull(outerTupleSlot))
                continue;

            return NULL;
        }

        if (done || TupIsNull(outerTupleSlot))
        {
            doSendEndOfStream(motion, node);
            done = true;
        }
        else
        {
            doSendTupleBatch(motion, node, outerTupleSlot);

            Gpmon_M_Incr_Rows_Out(GpmonPktFromMotionState(node));
            setMotionStatsForGpmon(node);
            CheckSendPlanStateGpmonPkt(&node->ps);

            if (node->stopRequested)
            {
                elog(gp_workfile_caching_loglevel, "Motion initiating Squelch walker");
                /* propagate stop notification to our children */
                ExecSquelchNode(outerNode);
                done = true;
            }
        }
#ifdef MEASURE_MOTION_TIME
        gettimeofday(&time1, NULL);

		node->motionTime.tv_sec += time1.tv_sec - time2.tv_sec;
		node->motionTime.tv_usec += time1.tv_usec - time2.tv_usec;

		while (node->motionTime.tv_usec < 0)
		{
			node->motionTime.tv_usec += 1000000;
			node->motionTime.tv_sec--;
		}

		while (node->motionTime.tv_usec >= 1000000)
		{
			node->motionTime.tv_usec -= 1000000;
			node->motionTime.tv_sec++;
		}
#endif
    }

    Assert(node->stopRequested || node->numTuplesFromChild == node->numTuplesToAMS);

    /* nothing else to send out, so we return NULL up the tree. */
    return NULL;
}

void
doSendTupleBatch(Motion * motion, MotionState * node, TupleTableSlot *outerTupleSlot)
{
    int16		    targetRoute;
    HeapTuple       tuple;
    SendReturnCode  sendRC;
    ExprContext    *econtext = node->ps.ps_ExprContext;

    /* We got a tuple from the child-plan. */
    node->numTuplesFromChild++;

    if (motion->motionType == MOTIONTYPE_FIXED)
    {
        if (motion->numOutputSegs == 0) /* Broadcast */
        {
            targetRoute = BROADCAST_SEGIDX;
        }
        else /* Fixed Motion. */
        {
            Assert(motion->numOutputSegs == 1);
            /*
             * Actually, since we can only send to a single output segment
             * here, we are guaranteed that we only have a single
             * targetRoute setup that we could possibly send to.  So we
             * can cheat and just fix the targetRoute to 0 (the 1st
             * route).
             */
            targetRoute = 0;
        }
    }
    else if (motion->motionType == MOTIONTYPE_HASH) /* Redistribute */
    {
        //TODO:: Implement later
    }
    else /* ExplicitRedistribute */
    {
        Datum segidColIdxDatum;

        Assert(motion->segidColIdx > 0 && motion->segidColIdx <= list_length((motion->plan).targetlist));
        bool is_null = false;
        segidColIdxDatum = slot_getattr(outerTupleSlot, motion->segidColIdx, &is_null);
        targetRoute = Int32GetDatum(segidColIdxDatum);
        Assert(!is_null);
    }

    /* send the tuple out. */
    sendRC = SendTupleBatch(node->ps.state->motionlayer_context,
                            node->ps.state->interconnect_context,
                            motion->motionID,
                            outerTupleSlot->PRIVATE_tb,
                            targetRoute);

    Assert(sendRC == SEND_COMPLETE || sendRC == STOP_SENDING);
    if (sendRC == SEND_COMPLETE)
        node->numTuplesToAMS++;
    else
        node->stopRequested = true;
}

TupleTableSlot *execVMotionUnsortedReceiver(MotionState * node)
{
    /* RECEIVER LOGIC */
    TupleTableSlot *slot;
    HeapTuple	tuple;
    Motion	   *motion = (Motion *) node->ps.plan;
    ReceiveReturnCode recvRC;

    AssertState(motion->motionType == MOTIONTYPE_HASH ||
                (motion->motionType == MOTIONTYPE_EXPLICIT && motion->segidColIdx > 0) ||
                (motion->motionType == MOTIONTYPE_FIXED && motion->numOutputSegs <= 1));

    Assert(node->ps.state->motionlayer_context);
    Assert(node->ps.state->interconnect_context);

    if (node->stopRequested)
    {
        SendStopMessage(node->ps.state->motionlayer_context,
                        node->ps.state->interconnect_context,
                        motion->motionID);
        return NULL;
    }

    recvRC = RecvTupleFrom(node->ps.state->motionlayer_context,
                           node->ps.state->interconnect_context,
                           motion->motionID, &tuple, ANY_ROUTE);

    if (recvRC == END_OF_STREAM)
    {
        Assert(node->numTuplesFromAMS == node->numTuplesToParent);
        Assert(node->numTuplesFromChild == 0);
        Assert(node->numTuplesToAMS == 0);
        return NULL;
    }

    node->numTuplesFromAMS++;
    node->numTuplesToParent++;

    /* store it in our result slot and return this. */
    slot = node->ps.ps_ResultTupleSlot;
    bool succ = tbDeserialization(((MemTuple)tuple)->PRIVATE_mt_bits,&slot->PRIVATE_tb);

    if(!succ)
        elog(ERROR,"Deserialization process Failed");

    TupSetVirtualTupleNValid(slot, ((TupleBatch)slot->PRIVATE_tb)->ncols);
    return slot;
}

/*
 * Function:  SendTupleBatch - Sends a batch of tuples to the AMS layer.
 */
SendReturnCode
SendTupleBatch(MotionLayerState *mlStates,
               ChunkTransportState *transportStates,
               int16 motNodeID,
               TupleBatch tuplebatch,
               int16 targetRoute)
{
    MotionNodeEntry *pMNEntry;
    TupleChunkListData tcList;
    MemoryContext oldCtxt;
    SendReturnCode rc;

    /*
     * Analyze tools.  Do not send any thing if this slice is in the bit mask
     */
    if (gp_motion_slice_noop != 0 && (gp_motion_slice_noop & (1 << currentSliceId)) != 0)
        return SEND_COMPLETE;

    /*
     * Pull up the motion node entry with the node's details.  This includes
     * details that affect sending, such as whether the motion node needs to
     * include backup segment-dbs.
     */
    pMNEntry = getMotionNodeEntry(mlStates, motNodeID, "SendTuple");

    MemTuple tuple = tbSerialization(tuplebatch);

    if (targetRoute != BROADCAST_SEGIDX)
    {
        struct directTransportBuffer b;

        getTransportDirectBuffer(transportStates, motNodeID, targetRoute, &b);

        if (b.pri != NULL && b.prilen > TUPLE_CHUNK_HEADER_SIZE)
        {
            int sent = 0;

            sent = SerializeTupleDirect(tuple, &pMNEntry->ser_tup_info, &b);
            if (sent > 0)
            {
                putTransportDirectBuffer(transportStates, motNodeID, targetRoute, sent);

                tcList.num_chunks = 1;
                tcList.serialized_data_length = sent;

                statSendTuple(mlStates, pMNEntry, &tcList);

                return SEND_COMPLETE;
            }
        }
    }

    /* Create and store the serialized form, and some stats about it. */
    oldCtxt = MemoryContextSwitchTo(mlStates->motion_layer_mctx);


    SerializeTupleIntoChunks(tuple, &pMNEntry->ser_tup_info, &tcList);
    pfree(tuple);

    MemoryContextSwitchTo(oldCtxt);

    /* do the send. */
    if (!SendTupleChunkToAMS(mlStates, transportStates, motNodeID, targetRoute, tcList.p_first))
    {
        pMNEntry->stopped = true;
        rc = STOP_SENDING;
    }
    else
    {
        /* update stats */
        statSendTuple(mlStates, pMNEntry, &tcList);

        rc = SEND_COMPLETE;
    }

    /* cleanup */
    clearTCList(&pMNEntry->ser_tup_info.chunkCache, &tcList);

    return rc;
}
