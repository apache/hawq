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
 * cdbmotion.c
 *		Access into the motion-layer in order to send and receive tuples
 *		within a motion node ID within a particular process group id.
 *
 * Reviewers: jzhang, tkordas
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "miscadmin.h"
#include "access/heapam.h"
#include "cdb/cdbconn.h"
#include "nodes/execnodes.h" //SliceTable
#include "cdb/cdbmotion.h"
#include "cdb/cdbvars.h"
#include "cdb/htupfifo.h"
#include "cdb/ml_ipc.h"
#include "cdb/tupser.h"
#include "libpq/pqformat.h"
#include "utils/hsearch.h"
#include "utils/memutils.h"


/*
 * MOTION NODE INFO DATA STRUCTURES
 */
int			Gp_max_tuple_chunk_size;

/*
 * STATIC STATE VARS
 *
 * Note, the alignment here isn't quite right.
 * GCC4.0 doesn't like our earlier initializer method of declaration.
 *
 * static TupleChunkListItemData s_eos_chunk_data = {NULL, TUPLE_CHUNK_HEADER_SIZE, NULL, "                "};
 */
static uint8 s_eos_buffer[sizeof(TupleChunkListItemData) + 8];
static TupleChunkListItem s_eos_chunk_data = (TupleChunkListItem)s_eos_buffer;

/*
 * HELPER FUNCTION DECLARATIONS
 */
static ChunkSorterEntry *getChunkSorterEntry(MotionLayerState *mlStates,
											 MotionNodeEntry * motNodeEntry,
											 int16 srcRoute);
static bool addChunkToSorter(MotionLayerState *mlStates,
							 ChunkTransportState *transportStates,
							 MotionNodeEntry * pMNEntry,
							 TupleChunkListItem tcItem,
							 int16 motNodeID,
							 int16 srcRoute);

static void processIncomingChunks(MotionLayerState *mlStates,
								  ChunkTransportState *transportStates,
								  MotionNodeEntry * pMNEntry,
								  int16 motNodeID,
								  int16 srcRoute);

static inline void reconstructTuple(MotionNodeEntry * pMNEntry, ChunkSorterEntry * pCSEntry);

/* Stats-function declarations. */
static void statSendTuple(MotionLayerState *mlStates, MotionNodeEntry * pMNEntry, TupleChunkList tcList);
static void statSendEOS(MotionLayerState *mlStates, MotionNodeEntry * pMNEntry);
static void statChunksProcessed(MotionLayerState *mlStates, MotionNodeEntry * pMNEntry, int chunksProcessed, int chunkBytes, int tupleBytes);
static void statNewTupleArrived(MotionNodeEntry * pMNEntry, ChunkSorterEntry * pCSEntry);
static void statRecvTuple(MotionNodeEntry * pMNEntry,
						  ChunkSorterEntry * pCSEntry,
						  ReceiveReturnCode recvRC);



/* Helper function to perform the operations necessary to reconstruct a
 * HeapTuple from a list of tuple-chunks, and then update the Motion Layer
 * state appropriately.  This includes storing the tuple, cleaning out the
 * tuple-chunk list, and recording statistics about the newly formed tuple.
 */
static inline void
reconstructTuple(MotionNodeEntry * pMNEntry, ChunkSorterEntry * pCSEntry)
{
	HeapTuple	htup;

	/*
	 * Convert the list of chunks into a tuple, then stow it away. This frees
	 * our TCList as a side-effect
	 */
	htup = CvtChunksToHeapTup(&pCSEntry->chunk_list, &pMNEntry->ser_tup_info);

	htfifo_addtuple(pCSEntry->ready_tuples, htup);

	/* Stats */
	statNewTupleArrived(pMNEntry, pCSEntry);
}

/*
 * FUNCTION DEFINITIONS
 */

/*
 * This function deletes the Motion Layer state at the end of a query
 * execution.
 */
void
RemoveMotionLayer(MotionLayerState *mlStates, bool flushCommLayer  __attribute__((unused)))
{

	if (Gp_role == GP_ROLE_UTILITY)
		return;

#ifdef AMS_VERBOSE_LOGGING
	/* Emit statistics to log */
	if (gp_log_interconnect >= GPVARS_VERBOSITY_VERBOSE)
		elog(LOG, "RemoveMotionLayer(): dumping stats\n"
			 "      Sent: %9u chunks %9u total bytes %9u tuple bytes\n"
			 "  Received: %9u chunks %9u total bytes %9u tuple bytes; "
			 "%9u chunkproc calls\n",
			 mlStates->stat_total_chunks_sent,
			 mlStates->stat_total_bytes_sent,
			 mlStates->stat_tuple_bytes_sent,
			 mlStates->stat_total_chunks_recvd,
			 mlStates->stat_total_bytes_recvd,
			 mlStates->stat_tuple_bytes_recvd,
			 mlStates->stat_total_chunkproc_calls);
#endif

	/*
	 * free our some resources to be safe. The rest will get freed with
	 * MemoryContextDelete()
	 */
	if (mlStates->mnEntries != NULL)
		pfree(mlStates->mnEntries);
	
	mlStates->mnEntries = NULL;
	mlStates->mneCount = 0;

	/*
	 * Free all memory used by the Motion Layer in the processing of this query.
	 */
	if (mlStates->motion_layer_mctx != NULL)
		MemoryContextDelete(mlStates->motion_layer_mctx);
}


void
initMotionLayerStructs(MotionLayerState **mlStates)
{
	MemoryContext oldCtxt;
	MemoryContext ml_mctx;
	uint8	   *pData;

	if (Gp_role == GP_ROLE_UTILITY)
		return;

	if (Gp_interconnect_type == INTERCONNECT_TYPE_UDP)
		Gp_max_tuple_chunk_size = Gp_max_packet_size - sizeof(struct icpkthdr) - TUPLE_CHUNK_HEADER_SIZE;
	else
		Gp_max_tuple_chunk_size = Gp_max_packet_size - PACKET_HEADER_SIZE - TUPLE_CHUNK_HEADER_SIZE;		

	/*
	 * Use the statically allocated chunk that is intended for sending end-of-
	 * stream messages so that we don't incur allocation and deallocation
	 * overheads.
	 */
	s_eos_chunk_data->p_next = NULL;
	s_eos_chunk_data->inplace = NULL;
	s_eos_chunk_data->chunk_length = TUPLE_CHUNK_HEADER_SIZE;

	pData = s_eos_chunk_data->chunk_data;

	SetChunkDataSize(pData, 0);
	SetChunkType(pData, TC_END_OF_STREAM);
	
	/*
	 * Create the memory-contexts that we will use within the Motion Layer.
	 *
	 * We make the Motion Layer memory-context a child of the ExecutorState
	 * Context, as it lives inside of the estate of a specific query and needs
	 * to get freed when the query is finished.
	 *
	 * The tuple-serial memory-context is a child of the Motion Layer
	 * memory-context
	 *
	 * NOTE: we need to be sure the caller is in ExecutorState memory context
	 * (estate->es_query_cxt) before calling us .
	 */
	ml_mctx =
		AllocSetContextCreate(CurrentMemoryContext, "MotionLayerMemCtxt",
							  ALLOCSET_SMALL_MINSIZE,
							  ALLOCSET_SMALL_INITSIZE,
							  ALLOCSET_DEFAULT_MAXSIZE); /* use a setting bigger than "small" */
							
	
	/*
	 * Switch to the Motion Layer memory context, so that we can clean things
	 * up easily.
	 */
	oldCtxt = MemoryContextSwitchTo(ml_mctx);

	Assert(*mlStates == NULL);
	*mlStates = palloc0(sizeof(MotionLayerState));

	(*mlStates)->mnEntries = palloc0(MNE_INITIAL_COUNT * sizeof(MotionNodeEntry));
	(*mlStates)->mneCount = MNE_INITIAL_COUNT;

	/* Allocation is done.	Go back to caller memory-context. */
	MemoryContextSwitchTo(oldCtxt);

	/*
	 * Keep our motion layer memory context in our newly created motion layer.
	 */
	(*mlStates)->motion_layer_mctx = ml_mctx;
}

static void
AddMotionLayerNode(MotionLayerState *mlStates, int16 motNodeID)
{
	/* increase size of our table */
	MotionNodeEntry *newTable;

	newTable = repalloc(mlStates->mnEntries, motNodeID * sizeof(MotionNodeEntry));
	mlStates->mnEntries = newTable;
	/* zero-out the new piece at the end */
	memset(&mlStates->mnEntries[mlStates->mneCount],
		   0,
		   (motNodeID - mlStates->mneCount) * sizeof(MotionNodeEntry));
	mlStates->mneCount = motNodeID;
}

/*
 * Initialize a single motion node.  This is called by the executor
 * when a to set up a placeholder which will be filled in during plan
 * initialization.
 *
 * This function is called from:  executorStart()
 */
void
InitMotionLayerNode(MotionLayerState *mlStates, int16 motNodeID)
{
	MemoryContext oldCtxt;
	MotionNodeEntry *pEntry;

	/*
	 * Switch to the Motion Layer's memory-context, so that the motion node
	 * can be reset later.
	 */
	oldCtxt = MemoryContextSwitchTo(mlStates->motion_layer_mctx);

	if (motNodeID > mlStates->mneCount)
	{
		AddMotionLayerNode(mlStates, motNodeID);
	}
	do
	{
		pEntry = &mlStates->mnEntries[motNodeID - 1];

		if (pEntry->valid)
		{
			/*
			 * An entry for this motion node ID already existed.  Initializing
			 * twice for a given motion node is not allowed.  Having two nodes
			 * with the same ID is also not allowed.  So, this is pretty
			 * likely a serious problem.
			 */
			EndMotionLayerNode(mlStates, motNodeID, false);
		}
	} while (pEntry->valid);

	pEntry->motion_node_id = motNodeID;

	pEntry->valid = true;

	/*
	 * we'll just set this to 0.  later, ml_ipc will call
	 * setExpectedReceivers() to set this if we are a "Receiving" motion node.
	 */
	pEntry->num_senders = 0;

	/* All done!  Go back to caller memory-context. */
	MemoryContextSwitchTo(oldCtxt);
}

/*
 * Initialize a single motion node.  This is called by the executor when a
 * motion node in the plan tree is being initialized.
 *
 * This function is called from:  ExecInitMotion()
 */
void
UpdateMotionLayerNode(MotionLayerState *mlStates, int16 motNodeID, bool preserveOrder, TupleDesc tupDesc, uint64 operatorMemKB)
{
	MemoryContext oldCtxt;
	MotionNodeEntry *pEntry;

	AssertArg(tupDesc != NULL);

	/*
	 * Switch to the Motion Layer's memory-context, so that the motion node
	 * can be reset later.
	 */
	oldCtxt = MemoryContextSwitchTo(mlStates->motion_layer_mctx);
	
	if (motNodeID > mlStates->mneCount)
	{
		AddMotionLayerNode(mlStates, motNodeID);
	}

	pEntry = &mlStates->mnEntries[motNodeID - 1];

	if (!pEntry->valid)
	{
		/*
		 * we'll just set this to 0.  later, ml_ipc will call
		 * setExpectedReceivers() to set this if we are a "Receiving"
		 * motion node.
		 */
		pEntry->num_senders = 0;
	}

	pEntry->motion_node_id = motNodeID;

	pEntry->valid = true;

	/* Finish up initialization of the motion node entry. */
	pEntry->preserve_order = preserveOrder;
	pEntry->tuple_desc = CreateTupleDescCopy(tupDesc);
	InitSerTupInfo(pEntry->tuple_desc, &pEntry->ser_tup_info);

	pEntry->memKB = operatorMemKB;

	if (!preserveOrder)
	{
		Assert(pEntry->memKB > 0);
		
		/* Create a tuple-store for the motion node's incoming tuples. */
		pEntry->ready_tuples = htfifo_create(pEntry->memKB);
	}
	else
		pEntry->ready_tuples = NULL;


	pEntry->num_stream_ends_recvd = 0;

	/* Initialize statistics counters. */
	pEntry->stat_total_chunks_sent = 0;
	pEntry->stat_total_bytes_sent = 0;
	pEntry->stat_tuple_bytes_sent = 0;
	pEntry->stat_total_sends = 0;
	pEntry->stat_total_recvs = 0;
	pEntry->stat_tuples_available = 0;
	pEntry->stat_tuples_available_hwm = 0;
	pEntry->stat_total_chunks_recvd = 0;
	pEntry->stat_total_bytes_recvd = 0;
	pEntry->stat_tuple_bytes_recvd = 0;
	pEntry->sel_rd_wait = 0;
	pEntry->sel_wr_wait = 0;

	pEntry->cleanedUp = false;
	pEntry->stopped = false;
	pEntry->moreNetWork = true;


	/* All done!  Go back to caller memory-context. */
	MemoryContextSwitchTo(oldCtxt);
}

void
setExpectedReceivers(MotionLayerState *mlStates, int16 motNodeID, int expectedReceivers)
{
	MotionNodeEntry *pEntry = getMotionNodeEntry(mlStates, motNodeID, "setExpectedReceivers");

	pEntry->num_senders = expectedReceivers;
}

void
SendStopMessage(MotionLayerState *mlStates,
				ChunkTransportState *transportStates,
				int16 motNodeID)
{
	MotionNodeEntry *pEntry = getMotionNodeEntry(mlStates, motNodeID, "SendStopMessage");

	pEntry->stopped = true;
	if (transportStates != NULL && transportStates->doSendStopMessage != NULL)
		transportStates->doSendStopMessage(transportStates, motNodeID);
}

/*
 * Function:  SendTuple - Sends a portion or whole tuple to the AMS layer.
 */
SendReturnCode
SendTuple(MotionLayerState *mlStates,
		  ChunkTransportState *transportStates,
		  int16 motNodeID,
		  HeapTuple tuple,
		  int16 targetRoute)
{
	MotionNodeEntry *pMNEntry;
	TupleChunkListData tcList;
	MemoryContext oldCtxt;
	SendReturnCode rc;

	AssertArg(tuple != NULL);
		
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

#ifdef AMS_VERBOSE_LOGGING
	elog(DEBUG5, "Serializing HeapTuple for sending.");
#endif

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

				/* fill-in tcList fields to update stats */
				tcList.num_chunks = 1;
				tcList.serialized_data_length = sent;
			
				/* update stats */
				statSendTuple(mlStates, pMNEntry, &tcList);

				return SEND_COMPLETE;
			}
		}
		/* Otherwise fall-through */
	}

	/* Create and store the serialized form, and some stats about it. */
	oldCtxt = MemoryContextSwitchTo(mlStates->motion_layer_mctx);

	SerializeTupleIntoChunks(tuple, &pMNEntry->ser_tup_info, &tcList);

	MemoryContextSwitchTo(oldCtxt);

#ifdef AMS_VERBOSE_LOGGING
	elog(DEBUG5, "Serialized HeapTuple for sending:\n"
		 "\ttarget-route %d \n"
		 "\t%d bytes in serial form\n"
		 "\tbroken into %d chunks",
		 targetRoute,
		 tcList.serialized_data_length,
		 tcList.num_chunks);
#endif

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

TupleChunkListItem
get_eos_tuplechunklist(void)
{
	return s_eos_chunk_data;
}

/*
 * Sends a token to all peer Motion Nodes, indicating that this motion
 * node has no more tuples to send out.
 */
void
SendEndOfStream(MotionLayerState       *mlStates,
                ChunkTransportState    *transportStates,
                int                     motNodeID)
{
	MotionNodeEntry *pMNEntry;

	/*
	 * Pull up the motion node entry with the node's details.  This includes
	 * details that affect sending, such as whether the motion node needs to
	 * include backup segment-dbs.
	 */
	pMNEntry = getMotionNodeEntry(mlStates, motNodeID, "SendEndOfStream");

	transportStates->SendEos(mlStates, transportStates, motNodeID, s_eos_chunk_data);

	/*
	 * We increment our own "stream-ends received" count when we send our own,
	 * as well as when we receive one.
	 */
	pMNEntry->num_stream_ends_recvd++;

	/* We record EOS as if a tuple were sent. */
	statSendEOS(mlStates, pMNEntry);
}

/* An unordered receiver will call this with srcRoute == ANY_ROUTE */
ReceiveReturnCode
RecvTupleFrom(MotionLayerState *mlStates,
			  ChunkTransportState *transportStates,
			  int16 motNodeID,
			  HeapTuple *tup_i,
			  int16 srcRoute)
{
	MotionNodeEntry *pMNEntry;
	ChunkSorterEntry *pCSEntry;
	ReceiveReturnCode recvRC = END_OF_STREAM;
	htup_fifo ReadyList;

#ifdef AMS_VERBOSE_LOGGING
	elog(DEBUG5, "RecvTupleFrom( motNodeID = %d, srcRoute = %d )", motNodeID, srcRoute);
#endif

	pMNEntry = getMotionNodeEntry(mlStates, motNodeID, "RecvTupleFrom");

	if (srcRoute == ANY_ROUTE)
	{
		Assert(pMNEntry->preserve_order == 0);
		pCSEntry = NULL;

		ReadyList = pMNEntry->ready_tuples;
	}
	else
	{
		Assert(pMNEntry->preserve_order != 0);
		/*
		 * Pull up the chunk-sorter entry for the specified sender, and get the
		 * tuple-store we should use.
		 */
		pCSEntry = getChunkSorterEntry(mlStates, pMNEntry, srcRoute);
		ReadyList = pCSEntry->ready_tuples;
	}

	/* Get the next HeapTuple, if one is available! */
	*tup_i = htfifo_gettuple(ReadyList);

	if (*tup_i != NULL)
	{
		recvRC = GOT_TUPLE;
		statRecvTuple(pMNEntry, pCSEntry, recvRC);
		return recvRC;
	}

	/* We need to get more chunks before we have a full tuple to return */
	do
	{
		if (srcRoute == ANY_ROUTE)
		{
			if (!pMNEntry->moreNetWork)
			{
				recvRC = END_OF_STREAM;
				statRecvTuple(pMNEntry, pCSEntry, recvRC);
				return recvRC;
			}
		}
		else
		{
			if  (pCSEntry->end_of_stream)
			{
				recvRC = END_OF_STREAM;
				statRecvTuple(pMNEntry, pCSEntry, recvRC);
				return recvRC;
			}
		}

		processIncomingChunks(mlStates, transportStates, pMNEntry, motNodeID, srcRoute);

		if (srcRoute == ANY_ROUTE)
			*tup_i = htfifo_gettuple(pMNEntry->ready_tuples);
		else
			*tup_i = htfifo_gettuple(pCSEntry->ready_tuples);

		if (*tup_i != NULL)
		{
			/* We got a tuple. */
			recvRC = GOT_TUPLE;
		}
		else if ((srcRoute == ANY_ROUTE && !pMNEntry->moreNetWork) ||
				 (srcRoute != ANY_ROUTE && pCSEntry->end_of_stream))
		{
			/*
			 * No tuple was available (tuple-store was at EOF), and end-of-stream
			 * has been marked.  No more tuples are going to show up.
			 */
			recvRC = END_OF_STREAM;
		}
		else
		{
			/*
			 * No tuple was available (tuple-store was at EOF), but we
			 * have not been sent an end-of-stream. We need to loop so
			 * that we process more chunks to assemble a complete
			 * tuple.
			 */
			recvRC = NO_TUPLE;
		}
	}
	while (recvRC == NO_TUPLE);

	/* Stats */
	statRecvTuple(pMNEntry, pCSEntry, recvRC);

	return recvRC;
}


/*
 * This helper function is the receive-tuple workhorse.  It pulls
 * tuple chunks from the AMS, and pushes them to the chunk-sorter
 * where they can be sorted based on sender, and reconstituted into
 * whole HeapTuples.  Functions like RecvTuple() and RecvTupleFrom()
 * should definitely call this before trying to get a HeapTuple to
 * return.  It can also be called during other operations if that
 * seems like a good idea.  For example, it can be called sometime
 * during send-tuple operations as well.
 */
static void
processIncomingChunks(MotionLayerState *mlStates,
					  ChunkTransportState *transportStates,
					  MotionNodeEntry * pMNEntry,
					  int16 motNodeID,
					  int16 srcRoute)
{
	TupleChunkListItem tcItem,
				tcNext;
	MemoryContext oldCtxt;

	/* Keep track of processed chunk stats. */
	int			numChunks,
				chunkBytes,
				tupleBytes;

	oldCtxt = MemoryContextSwitchTo(mlStates->motion_layer_mctx);

	/*
	 * Get all of the currently available tuple-chunks, and push each one into
	 * the chunk-sorter.
	 */
	if (srcRoute == ANY_ROUTE)
		tcItem = transportStates->RecvTupleChunkFromAny(mlStates, transportStates, motNodeID, &srcRoute);
	else
		tcItem = transportStates->RecvTupleChunkFrom(transportStates, motNodeID, srcRoute);

	numChunks = 0;
	chunkBytes = 0;
	tupleBytes = 0;

	while (tcItem != NULL)
	{
		numChunks++;

		/* Detach the current chunk off of the front of the list. */
		tcNext = tcItem->p_next;
		tcItem->p_next = NULL;

		/* Track stats. */
		chunkBytes += tcItem->chunk_length;
		if (tcItem->chunk_length >= TUPLE_CHUNK_HEADER_SIZE)
		{
			tupleBytes += tcItem->chunk_length - TUPLE_CHUNK_HEADER_SIZE;
		}
		else
		{
			elog(ERROR, "Received tuple-chunk of size %u; smaller than"
				 " chunk header size %d!", tcItem->chunk_length,
				 TUPLE_CHUNK_HEADER_SIZE);
		}

		/* Stick the chunk into the sorter. */
		addChunkToSorter(mlStates, transportStates, pMNEntry, tcItem, motNodeID, srcRoute);

		tcItem = tcNext;
	}

	/* The chunk list we just processed freed-up our rx-buffer space. */
	if (Gp_interconnect_type == INTERCONNECT_TYPE_UDP)
		MlPutRxBuffer(transportStates, motNodeID, srcRoute);

	/* Stats */
	statChunksProcessed(mlStates, pMNEntry, numChunks, chunkBytes, tupleBytes);

	MemoryContextSwitchTo(oldCtxt);
}

void
EndMotionLayerNode(MotionLayerState *mlStates, int16 motNodeID, bool flushCommLayer)
{
	MotionNodeEntry *pMNEntry;
	ChunkSorterEntry *pCSEntry;
	int i;

	pMNEntry = getMotionNodeEntry(mlStates, motNodeID, "EndMotionLayerNode");

#ifdef AMS_VERBOSE_LOGGING
	elog(DEBUG5, "Cleaning up Motion Layer details for motion node %d.",
		 motNodeID);
#endif

	/*
	 * Iterate through all entries in the motion layer's chunk-sort map, to
	 * see if we have gotten end-of-stream from all senders.
	 */
	if (pMNEntry->preserve_order && pMNEntry->ready_tuple_lists != NULL)
	{
		for (i=0; i < GetQEGangNum(); i++)
		{
			pCSEntry = &pMNEntry->ready_tuple_lists[i];

			if (pMNEntry->preserve_order &&
				gp_log_interconnect >= GPVARS_VERBOSITY_DEBUG)
			{
				/* Print chunk-sorter entry statistics. */
				elog(DEBUG4, "Chunk-sorter entry [route=%d,node=%d] statistics:\n"
					 "\tAvailable Tuples High-Watermark: " UINT64_FORMAT,
					 i, pMNEntry->motion_node_id,
					 pMNEntry->stat_tuples_available_hwm);
			}

			if (!pMNEntry->stopped && !pCSEntry->end_of_stream)
			{
				if (flushCommLayer)
				{
					elog(FATAL, "Motion layer node %d cleanup - did not receive"
						 " end-of-stream from sender %d.", motNodeID, i);

					/*** TODO - get chunks until end-of-stream comes in. ***/
				}
				else
				{
					elog(LOG, "Motion layer node %d cleanup - did not receive"
						 " end-of-stream from sender %d.", motNodeID, i);
				}
			}
			else
			{
				/* End-of-stream is marked for this entry. */

				/*** TODO - do more than just complain! ***/

				if (pCSEntry->chunk_list.num_chunks > 0)
				{
					elog(LOG, "Motion layer node %d cleanup - there are still"
						 " %d chunks enqueued from sender %d.", motNodeID,
						 pCSEntry->chunk_list.num_chunks, i );
				}

				/***
					TODO - Make sure there are no outstanding tuples in the
					tuple-store.
				***/
			}

			/*
			 * Clean up the chunk-sorter entry, then remove it from the hash
			 * table.
			 */
			clearTCList(&pMNEntry->ser_tup_info.chunkCache, &pCSEntry->chunk_list);
			if (pMNEntry->preserve_order)	/* Clean up the tuple-store. */
				htfifo_destroy(pCSEntry->ready_tuples);
		}
	}
	pMNEntry->cleanedUp = true;

	/* Clean up the motion-node entry, then remove it from the hash table. */
	if (gp_log_interconnect >= GPVARS_VERBOSITY_VERBOSE)
    {
        if (pMNEntry->stat_total_bytes_sent > 0 ||
            pMNEntry->sel_wr_wait > 0)
        {
            elog(LOG, "Interconnect seg%d slice%d sent " UINT64_FORMAT " tuples, "
                 UINT64_FORMAT " total bytes, " UINT64_FORMAT " tuple bytes, "
				 UINT64_FORMAT " chunks; waited " UINT64_FORMAT " usec.",
		         GetQEIndex(),
                 currentSliceId,
		         pMNEntry->stat_total_sends,
		         pMNEntry->stat_total_bytes_sent,
		         pMNEntry->stat_tuple_bytes_sent,
		         pMNEntry->stat_total_chunks_sent,
		         pMNEntry->sel_wr_wait
		        );
        }
        if (pMNEntry->stat_total_bytes_recvd > 0 ||
            pMNEntry->sel_rd_wait > 0)
        {
            elog(LOG, "Interconnect seg%d slice%d received from slice%d: " UINT64_FORMAT " tuples, "
                 UINT64_FORMAT " total bytes, " UINT64_FORMAT " tuple bytes, " 
				 UINT64_FORMAT " chunks; waited " UINT64_FORMAT " usec.",
                 GetQEIndex(),
                 currentSliceId,
		         motNodeID,
		         pMNEntry->stat_total_recvs,
		         pMNEntry->stat_total_bytes_recvd,
		         pMNEntry->stat_tuple_bytes_recvd,
		         pMNEntry->stat_total_chunks_recvd,
		         pMNEntry->sel_rd_wait
		        );
        }
    }

	CleanupSerTupInfo(&pMNEntry->ser_tup_info);
	FreeTupleDesc(pMNEntry->tuple_desc);
	if (!pMNEntry->preserve_order)
		htfifo_destroy(pMNEntry->ready_tuples);

	pMNEntry->valid = false;
}

/*
 * Helper function to get the motion node entry for a given ID.  NULL
 * is returned if the ID is unrecognized.
 */
MotionNodeEntry *
getMotionNodeEntry(MotionLayerState *mlStates, int16 motNodeID, char *errString __attribute__((unused)) )
{
	MotionNodeEntry *pMNEntry = NULL;

	if (motNodeID > mlStates->mneCount ||
		!mlStates->mnEntries[motNodeID - 1].valid)
	{
		ereport(ERROR, (errcode(ERRCODE_GP_INTERCONNECTION_ERROR),
		errmsg("Interconnect Error: Unexpected Motion Node Id: %d.  This means"
			   " a motion node that wasn't setup is requesting interconnect"
			   " resources.", motNodeID)));
	}
	else
		pMNEntry = &mlStates->mnEntries[motNodeID - 1];

	if (pMNEntry != NULL)
		Assert(pMNEntry->motion_node_id == motNodeID);

	return pMNEntry;
}

/*
 * Retrieve the chunk-sorter entry for the specified motion-node/source pair.
 * If one doesn't exist, it is created and initialized.
 *
 * It might not seem obvious why the MotionNodeEntry is required as an
 * argument.  It's in there to ensure that only valid motion nodes are
 * represented in the chunk-sorter.  The motion-node's entry is typically
 * retrieved using getMotionNodeEntry() before this function is called.
 */
ChunkSorterEntry *
getChunkSorterEntry(MotionLayerState *mlStates,
					MotionNodeEntry * motNodeEntry,
					int16 srcRoute)
{
	int16		motNodeID;
	MemoryContext oldCtxt;
	ChunkSorterEntry *chunkSorterEntry=NULL;

	AssertArg(motNodeEntry != NULL);

	Assert(srcRoute >= 0);
	Assert(srcRoute < GetQEGangNum());

	motNodeID = motNodeEntry->motion_node_id;

	/* Do we have a sorter initialized ? */
	if (motNodeEntry->ready_tuple_lists != NULL)
	{
		if (motNodeEntry->ready_tuple_lists[srcRoute].init)
			return &motNodeEntry->ready_tuple_lists[srcRoute];
	}

	/* We have to create an entry */
	oldCtxt = MemoryContextSwitchTo(mlStates->motion_layer_mctx);

	if (motNodeEntry->ready_tuple_lists == NULL)
		motNodeEntry->ready_tuple_lists = (ChunkSorterEntry *)palloc0(GetQEGangNum() * sizeof(ChunkSorterEntry));

	chunkSorterEntry = &motNodeEntry->ready_tuple_lists[srcRoute];

	if (chunkSorterEntry == NULL)
	{
		ereport(ERROR, (errcode(ERRCODE_OUT_OF_MEMORY),
						errmsg("Could not allocate entry for tuple chunk sorter.")));
	}

	chunkSorterEntry->chunk_list.serialized_data_length = 0;
	chunkSorterEntry->chunk_list.max_chunk_length = Gp_max_tuple_chunk_size;
	chunkSorterEntry->chunk_list.num_chunks = 0;
	chunkSorterEntry->chunk_list.p_first = NULL;
	chunkSorterEntry->chunk_list.p_last = NULL;
	chunkSorterEntry->end_of_stream = false;
	chunkSorterEntry->init = true;

	/*
	 * If motion node is not order-preserving, then all chunk-sorter
	 * entries share one global tuple-store.  If motion node is order-
	 * preserving then there is a tuple-store per sender per motion node.
	 */
	if (motNodeEntry->preserve_order)
	{
		Assert(motNodeEntry->memKB > 0);
		chunkSorterEntry->ready_tuples = htfifo_create(motNodeEntry->memKB);

#ifdef AMS_VERBOSE_LOGGING
		elog(DEBUG5, "Motion node %d is order-preserving.  Creating"
			 " tuple-store for entry [src=%d,mn=%d].", motNodeID,
			 srcRoute, motNodeID);
#endif
	}
	else
	{
		chunkSorterEntry->ready_tuples = motNodeEntry->ready_tuples;

#ifdef AMS_VERBOSE_LOGGING
		elog(DEBUG5, "Motion node %d is not order-preserving.  Using"
			 " shared tuple-store for entry [src=%d,mn=%d].", motNodeID,
			 srcRoute, motNodeID);
#endif

		/* Sanity-check: */
		Assert(motNodeEntry->ready_tuples != NULL);
	}

	MemoryContextSwitchTo(oldCtxt);

	Assert(chunkSorterEntry != NULL);
	return chunkSorterEntry;
}

/*
 * Helper function for converting chunks from the receive state (where
 * they point into a share buffer) into the transient state (where they
 * have their own storage). We need to do this if we didn't receive enough
 * information in one chunk to reconstruct a tuple.
 */
static void
materializeChunk(TupleChunkListItem * tcItem)
{
	TupleChunkListItem newItem;

	/*
	 * This chunk needs to be converted from pointing to global receive buffer
	 * store to having its own storage
	 */
	Assert(tcItem != NULL);
	Assert(*tcItem != NULL);

	newItem = repalloc(*tcItem, sizeof(TupleChunkListItemData) + (*tcItem)->chunk_length);

	memcpy(newItem->chunk_data, newItem->inplace, newItem->chunk_length);
	newItem->inplace = NULL;	/* no need to free, someone else owns it */

	*tcItem = newItem;

	return;
}

/*
 * Add another tuple-chunk to the chunk sorter.  If the new chunk
 * completes another HeapTuple, that tuple will be deserialized and
 * stored into a tuple-store.  If not, the chunk is added to the
 * appropriate list of chunks.
 *
 * Return Values:
 *	 true  - if another HeapTuple is completed by this chunk.
 *	 false - if the chunk does not complete a HeapTuple.
 */
static bool
addChunkToSorter(MotionLayerState *mlStates,
				 ChunkTransportState *transportStates,
				 MotionNodeEntry * pMNEntry,
				 TupleChunkListItem tcItem,
				 int16 motNodeID,
				 int16 srcRoute)
{
	MemoryContext oldCtxt;
	ChunkSorterEntry *chunkSorterEntry;
	bool		tupleCompleted = false;
	TupleChunkType tcType;

	AssertArg(tcItem != NULL);

	oldCtxt = MemoryContextSwitchTo(mlStates->motion_layer_mctx);

	chunkSorterEntry = getChunkSorterEntry(mlStates, pMNEntry, srcRoute);

	/* Look at the chunk's type, to figure out what to do with it. */

	GetChunkType(tcItem, &tcType);

	switch (tcType)
	{
		case TC_WHOLE:
		case TC_EMPTY:
			/* There shouldn't be any partial tuple data in the list! */
			if (chunkSorterEntry->chunk_list.num_chunks != 0)
			{
				ereport(ERROR, (errcode(ERRCODE_GP_INTERCONNECTION_ERROR),
				   errmsg("Received TC_WHOLE chunk from [src=%d,mn=%d] after"
						  " partial tuple data.", srcRoute, motNodeID)));
			}

			/* Put this chunk into the list, then turn it into a HeapTuple! */
			appendChunkToTCList(&chunkSorterEntry->chunk_list, tcItem);
			reconstructTuple(pMNEntry, chunkSorterEntry);
			tupleCompleted = true;

			break;

		case TC_PARTIAL_START:

			/* There shouldn't be any partial tuple data in the list! */
			if (chunkSorterEntry->chunk_list.num_chunks != 0)
			{
				ereport(ERROR, (errcode(ERRCODE_GP_INTERCONNECTION_ERROR),
				 errmsg("Received TC_PARTIAL_START chunk from [src=%d,mn=%d]"
						" after partial tuple data.", srcRoute, motNodeID)));
			}

			/*
			 * we don't have enough to reconstruct the tuple, we need to copy
			 * the chunk data out of our shared buffer
			 */
			materializeChunk(&tcItem);

			/* Put this chunk into the list. */
			appendChunkToTCList(&chunkSorterEntry->chunk_list, tcItem);

			break;

		case TC_PARTIAL_MID:

			/* There should be partial tuple data in the list. */
			if (chunkSorterEntry->chunk_list.num_chunks <= 0)
			{
				ereport(ERROR, (errcode(ERRCODE_GP_INTERCONNECTION_ERROR),
				   errmsg("Received TC_PARTIAL_MID chunk from [src=%d,mn=%d]"
				  " without any leading tuple data.", srcRoute, motNodeID)));
			}

			/*
			 * we don't have enough to reconstruct the tuple, we need to copy
			 * the chunk data out of our shared buffer
			 */
			materializeChunk(&tcItem);

			/* Append this chunk to the list. */
			appendChunkToTCList(&chunkSorterEntry->chunk_list, tcItem);

			break;

		case TC_PARTIAL_END:

			/* There should be partial tuple data in the list. */
			if (chunkSorterEntry->chunk_list.num_chunks <= 0)
			{
				ereport(ERROR, (errcode(ERRCODE_GP_INTERCONNECTION_ERROR),
				   errmsg("Received TC_PARTIAL_END chunk from [src=%d,mn=%d]"
				  " without any leading tuple data.", srcRoute, motNodeID)));
			}

			/* Put this chunk into the list, then turn it into a HeapTuple! */
			appendChunkToTCList(&chunkSorterEntry->chunk_list, tcItem);
			reconstructTuple(pMNEntry, chunkSorterEntry);
			tupleCompleted = true;

			break;

		case TC_END_OF_STREAM:
#ifdef AMS_VERBOSE_LOGGING
			elog(LOG, "Got end-of-stream. motnode %d route %d", motNodeID, srcRoute);
#endif
			/* There shouldn't be any partial tuple data in the list! */
			if (chunkSorterEntry->chunk_list.num_chunks != 0)
			{
				ereport(ERROR, (errcode(ERRCODE_GP_INTERCONNECTION_ERROR),
				 errmsg("Received TC_END_OF_STREAM chunk from [src=%d,mn=%d]"
						" after partial tuple data.", srcRoute, motNodeID)));
			}

			/* Make sure that we haven't already received end-of-stream! */

			if (chunkSorterEntry->end_of_stream)
			{
				ereport(ERROR, (errcode(ERRCODE_GP_INTERCONNECTION_ERROR),
								errmsg("Received end-of-stream chunk from"
				  " [src=%d,mn=%d] when already marked as at end-of-stream.",
									   srcRoute, motNodeID)));
			}

			/* Mark the state as "end of stream." */
			chunkSorterEntry->end_of_stream = true;
			pMNEntry->num_stream_ends_recvd++;

			if (pMNEntry->num_stream_ends_recvd == pMNEntry->num_senders)
				pMNEntry->moreNetWork = false;

			/*
			 * Since we received an end-of-stream.	Then we no longer need
			 * read interest in the interconnect.
			 */
			DeregisterReadInterest(transportStates, motNodeID, srcRoute,
                                   "end of stream");
			break;

		default:
			ereport(ERROR, (errcode(ERRCODE_GP_INTERCONNECTION_ERROR),
			   errmsg("Received tuple chunk of unrecognized type %d (len %d)"
					  " from [src=%d,mn=%d].",
					  tcType, tcItem->chunk_length, srcRoute, motNodeID)));
	}

	MemoryContextSwitchTo(oldCtxt);

	return tupleCompleted;
}



/*
 * STATISTICS HELPER-FUNCTIONS
 *
 * NOTE: the only fields that are required to be valid are
 * tcList->num_chunks and tcList->serialized_data_length, and
 * SerializeTupleDirect() only fills those fields out.
 */
static void
statSendTuple(MotionLayerState *mlStates, MotionNodeEntry * pMNEntry, TupleChunkList tcList)
{
	int			headerOverhead;

	AssertArg(pMNEntry != NULL);

	headerOverhead = TUPLE_CHUNK_HEADER_SIZE * tcList->num_chunks;

	/* per motion-node stats. */
	pMNEntry->stat_total_sends++;
	pMNEntry->stat_total_chunks_sent += tcList->num_chunks;
	pMNEntry->stat_total_bytes_sent += tcList->serialized_data_length + headerOverhead;
	pMNEntry->stat_tuple_bytes_sent += tcList->serialized_data_length;

	/* Update global motion-layer statistics. */
	mlStates->stat_total_chunks_sent += tcList->num_chunks;

	mlStates->stat_total_bytes_sent +=
		tcList->serialized_data_length + headerOverhead;

	mlStates->stat_tuple_bytes_sent += tcList->serialized_data_length;

}

static void
statSendEOS(MotionLayerState *mlStates, MotionNodeEntry * pMNEntry)
{
	AssertArg(pMNEntry != NULL);

	/* Update motion node statistics. */
	pMNEntry->stat_total_chunks_sent++;
	pMNEntry->stat_total_bytes_sent += TUPLE_CHUNK_HEADER_SIZE;

	/* Update global motion-layer statistics. */
	mlStates->stat_total_chunks_sent++;
	mlStates->stat_total_bytes_sent += TUPLE_CHUNK_HEADER_SIZE;
}

static void
statChunksProcessed(MotionLayerState *mlStates, MotionNodeEntry * pMNEntry, int chunksProcessed, int chunkBytes, int tupleBytes)
{
	AssertArg(chunksProcessed >= 0);
	AssertArg(chunkBytes >= 0);
	AssertArg(tupleBytes >= 0);

	/* Update Global Motion Layer Stats. */
	mlStates->stat_total_chunks_recvd += chunksProcessed;
	mlStates->stat_total_chunkproc_calls++;
	mlStates->stat_total_bytes_recvd += chunkBytes;
	mlStates->stat_tuple_bytes_recvd += tupleBytes;

	/* Update Motion-node stats. */
	pMNEntry->stat_total_chunks_recvd += chunksProcessed;
	pMNEntry->stat_total_bytes_recvd += chunkBytes;
	pMNEntry->stat_tuple_bytes_recvd += tupleBytes;
}

static void
statNewTupleArrived(MotionNodeEntry * pMNEntry, ChunkSorterEntry * pCSEntry)
{
	uint32		tupsAvail;

	AssertArg(pMNEntry != NULL);
	AssertArg(pCSEntry != NULL);

	/*
	 * High-watermarks:  We track the number of tuples available to receive,
	 * but that haven't yet been received.  The high-watermark is recorded.
	 *
	 * Also, if the motion node is order-preserving, we track a per-sender
	 * high-watermark as well.
	 */
	tupsAvail = ++(pMNEntry->stat_tuples_available);
	if (pMNEntry->stat_tuples_available_hwm < tupsAvail)
	{
		/* New high-watermark! */
		pMNEntry->stat_tuples_available_hwm = tupsAvail;
	}

	if (pMNEntry->preserve_order)
	{
		/* Track per-sender high watermark. */
		tupsAvail = ++(pCSEntry->stat_tuples_available);
		if (pCSEntry->stat_tuples_available_hwm < tupsAvail)
		{
			/* New high-watermark! */
			pCSEntry->stat_tuples_available_hwm = tupsAvail;
		}
	}
}

static void
statRecvTuple(MotionNodeEntry * pMNEntry, ChunkSorterEntry * pCSEntry,
			  ReceiveReturnCode recvRC)
{
	AssertArg(pMNEntry != NULL);
	AssertArg(pCSEntry != NULL || !pMNEntry->preserve_order);

	if (recvRC == GOT_TUPLE)
	{
	    /* Count tuples received. */
	    pMNEntry->stat_total_recvs++;

	    /* Update "tuples available" counts for high watermark stats. */
		pMNEntry->stat_tuples_available--;

		if (pMNEntry->preserve_order)
			pCSEntry->stat_tuples_available--;
	}
}

