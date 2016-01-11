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
 * cdbexplain.c
 *    Functions supporting the Greenplum EXPLAIN ANALYZE command
 *
 *-------------------------------------------------------------------------
 */

#include <unistd.h>

#include "postgres.h"
#include "portability/instr_time.h"

#include "cdb/cdbconn.h"                /* SegmentDatabaseDescriptor */
#include "cdb/cdbdispatchresult.h"      /* CdbDispatchResults */
#include "cdb/cdbexplain.h"             /* me */
#include "cdb/cdbpartition.h"
#include "cdb/cdbvars.h"                /* Gp_segment */
#include "executor/executor.h"          /* ExecStateTreeWalker */
#include "executor/instrument.h"        /* Instrumentation */
#include "lib/stringinfo.h"             /* StringInfo */
#include "gp-libpq-fe.h"                   /* PGresult; prereq for libpq-int.h */
#include "gp-libpq-int.h"                  /* pg_result */
#include "libpq/pqformat.h"             /* pq_beginmessage() etc. */
#include "utils/memutils.h"             /* MemoryContextGetPeakSpace() */
#include "cdb/memquota.h"
#include "cdb/cdbgang.h"
#include "inttypes.h"
#include "parser/parsetree.h"
#include "utils/vmem_tracker.h"

/* EXPLAIN ANALYZE statistics for one plan node of a slice */
typedef struct CdbExplain_StatInst
{
    NodeTag     pstype;         /* PlanState node type */
	bool        running;        /* True if we've completed first tuple */
	instr_time	starttime;		/* Start time of current iteration of node */
	instr_time	counter;		/* Accumulated runtime for this node */
	double		firsttuple;		/* Time for first tuple of this cycle */
    double      startup;        /* Total startup time (in seconds) */
    double      total;          /* Total total time (in seconds) */
    double      ntuples;        /* Total tuples produced */
    double      nloops;         /* # of run cycles for this node */
    double      execmemused;    /* executor memory used (bytes) */
    double      workmemused;    /* work_mem actually used (bytes) */
    double      workmemwanted;  /* work_mem to avoid workfile i/o (bytes) */
	bool        workfileReused; /* workfile reused in this node */
	bool        workfileCreated;/* workfile created in this node */
	instr_time	firststart;		/* Start time of first iteration of node */
	double		peakMemBalance; /* Max mem account balance */
	int		numPartScanned; /* Number of part tables scanned */
    int         bnotes;         /* Offset to beginning of node's extra text */
    int         enotes;         /* Offset to end of node's extra text */
} CdbExplain_StatInst;


/* EXPLAIN ANALYZE statistics for one process working on one slice */
typedef struct CdbExplain_SliceWorker
{
    double      peakmemused;    /* bytes alloc in per-query mem context tree */
    double		vmem_reserved;	/* vmem reserved by a QE */
    double		memory_accounting_global_peak;	/* peak memory observed during memory accounting */
} CdbExplain_SliceWorker;


/* Header of EXPLAIN ANALYZE statistics message sent from qExec to qDisp */
typedef struct CdbExplain_StatHdr
{
    NodeTag     type;           /* T_CdbExplain_StatHdr */
    int         segindex;       /* segment id */
    char       hostname[SEGMENT_IDENTITY_NAME_LENGTH];       /* segment hostname */
    int         nInst;          /* num of StatInst entries following StatHdr */
    int         bnotes;         /* offset to extra text area */
    int         enotes;         /* offset to end of extra text area */

    int			memAccountTreeNodeCount;     /* How many mem account we serialized */
    int			memAccountTreeStartOffset; /* Where in the header our mem account tree is serialized */

    CdbExplain_SliceWorker  worker;     /* qExec's overall stats for slice */

    /*
     * During serialization, we use this as a temporary StatInst and save "one-at-a-time"
     * StatInst into this variable. We then write this variable into buffer (serialize it)
     * and then "recycle" the same inst for next plan node's StatInst.
     * During deserialization, an Array [0..nInst-1] of StatInst entries is appended starting here.
     */
    CdbExplain_StatInst inst[1];

    /* extra text is appended after that */
} CdbExplain_StatHdr;


/* Dispatch status summarized over workers in a slice */
typedef struct CdbExplain_DispatchSummary
{
    int         nResult;
    int         nOk;
    int         nError;
    int         nCanceled;
    int         nNotDispatched;
    int         nIgnorableError;
} CdbExplain_DispatchSummary;


/* One node's EXPLAIN ANALYZE statistics for all the workers of its segworker group */
typedef struct CdbExplain_NodeSummary
{
    /* Summary over all the node's workers */
    CdbExplain_Agg  ntuples;
    CdbExplain_Agg  execmemused;
    CdbExplain_Agg  workmemused;
    CdbExplain_Agg  workmemwanted;
	CdbExplain_Agg  totalWorkfileReused;
	CdbExplain_Agg  totalWorkfileCreated;
    CdbExplain_Agg  peakMemBalance;
    /* Used for DynamicTableScan, DynamicIndexScan and DynamicBitmapTableScan */
    CdbExplain_Agg  totalPartTableScanned;

    /* insts array info */
    int             segindex0;      /* segment id of insts[0] */
    int             ninst;          /* num of StatInst entries in inst array */

    /* Array [0..ninst-1] of StatInst entries is appended starting here */
    CdbExplain_StatInst insts[1];   /* variable size - must be last */
} CdbExplain_NodeSummary;


/* One slice's statistics for all the workers of its segworker group */
typedef struct CdbExplain_SliceSummary
{
    Slice          *slice;

    /* worker array */
    int             nworker;        /* num of SliceWorker slots in worker array */
    int             segindex0;      /* segment id of workers[0] */
    CdbExplain_SliceWorker *workers;    /* -> array [0..nworker-1] of SliceWorker */

    SerializedMemoryAccount **memoryTreeRoots; /* Array of pointers to pseudo roots [0...nworker-1] */

    CdbExplain_Agg  peakmemused; /* Summary of SliceWorker stats over all of the slice's workers */

    CdbExplain_Agg	vmem_reserved; /* vmem reserved by QEs */

    CdbExplain_Agg	memory_accounting_global_peak; /* Peak memory accounting balance by QEs */

    /* Rollup of per-node stats over all of the slice's workers and nodes */
    double          workmemused_max;
    double          workmemwanted_max;

    /* How many workers were dispatched and returned results? (0 if local) */
    CdbExplain_DispatchSummary  dispatchSummary;
} CdbExplain_SliceSummary;


/* State for cdbexplain_showExecStats() */
typedef struct CdbExplain_ShowStatCtx
{
    MemoryContext   explaincxt;         /* alloc all our buffers from this */
    StringInfoData  extratextbuf;
    instr_time      querystarttime;

    /* Rollup of per-node stats over the entire query plan */
    double          workmemused_max;
    double          workmemwanted_max;

    /* Per-slice statistics are deposited in this SliceSummary array */
    int             nslice;             /* num of slots in slices array */
    CdbExplain_SliceSummary   *slices;  /* -> array[0..nslice-1] of SliceSummary */
} CdbExplain_ShowStatCtx;


/* State for cdbexplain_sendStatWalker() and cdbexplain_collectStatsFromNode() */
typedef struct CdbExplain_SendStatCtx
{
    StringInfoData         *notebuf;
    StringInfoData          buf;
    CdbExplain_StatHdr      hdr;
} CdbExplain_SendStatCtx;


/* State for cdbexplain_recvStatWalker() and cdbexplain_depositStatsToNode() */
typedef struct CdbExplain_RecvStatCtx
{
	/*
	 * iStatInst is the current StatInst serial during the depositing process for a slice.
	 * We walk the plan tree, and for each node we deposit stat from all the QEs
	 * of the segworker group for current slice. After we finish one node, we increase
	 * iStatInst, which means we are done with one plan node's stat across
	 * all segments and now moving forward to the next one. Once we are done
	 * processing all the plan node of a PARTICULAR slice, then we switch to the
	 * next slice, read the messages from all the QEs of the next slice (another
	 * segworker group) store them in the msgptrs, reset the iStatInst and then start
	 * parsing these messages and depositing them in the nodes of the new slice.
	 */
    int                     iStatInst;
    /*
     * nStatInst is the total number of StatInst for current slice. Typically this is
     * the number of plan nodes in the current slice.
     */
    int                     nStatInst;
    /* segIndexMin is the min of segment index from which we collected message (i.e., saved msgptrs) */
    int                     segindexMin;
    /* segIndexMax is the max of segment index from which we collected message (i.e., saved msgptrs) */
    int                     segindexMax;
    /* We deposit stat for one slice at a time. sliceIndex saves the current slice */
    int                     sliceIndex;
	int						segmentNum;
    /*
     * The number of msgptrs that we have saved for current slice. This is
     * typically the number of QE processes
     */
    int                     nmsgptr;
    /* The actual messages. Contains an array of StatInst too */
    CdbExplain_StatHdr    **msgptrs;
    CdbDispatchResults     *dispatchResults;
    StringInfoData         *extratextbuf;
    CdbExplain_ShowStatCtx *showstatctx;

    /* Rollup of per-node stats over all of the slice's workers and nodes */
    double                  workmemused_max;
    double                  workmemwanted_max;
} CdbExplain_RecvStatCtx;


/* State for cdbexplain_localStatWalker() */
typedef struct CdbExplain_LocalStatCtx
{
    CdbExplain_SendStatCtx  send;
    CdbExplain_RecvStatCtx  recv;
    CdbExplain_StatHdr     *msgptrs[1];
} CdbExplain_LocalStatCtx;


static CdbVisitOpt
cdbexplain_localStatWalker(PlanState *planstate, void *context);
static CdbVisitOpt
cdbexplain_sendStatWalker(PlanState *planstate, void *context);
static CdbVisitOpt
cdbexplain_recvStatWalker(PlanState *planstate, void *context);

static void
cdbexplain_collectSliceStats(PlanState                 *planstate,
                             CdbExplain_SliceWorker    *out_worker);
static void
cdbexplain_depositSliceStats(CdbExplain_StatHdr        *hdr,
                             CdbExplain_RecvStatCtx    *recvstatctx);
static void
cdbexplain_collectStatsFromNode(PlanState *planstate, CdbExplain_SendStatCtx *ctx);
static void
cdbexplain_depositStatsToNode(PlanState *planstate, CdbExplain_RecvStatCtx *ctx);
static int
cdbexplain_collectExtraText(PlanState *planstate, StringInfo notebuf);
static int
cdbexplain_countLeafPartTables(PlanState *planstate);


/*
 * cdbexplain_localExecStats
 *    Called by qDisp to build NodeSummary and SliceSummary blocks
 *    containing EXPLAIN ANALYZE statistics for a root slice that
 *    has been executed locally in the qDisp process.  Attaches these
 *    structures to the PlanState nodes' Instrumentation objects for
 *    later use by cdbexplain_showExecStats().
 *
 * 'planstate' is the top PlanState node of the slice.
 * 'showstatctx' is a CdbExplain_ShowStatCtx object which was created by
 *      calling cdbexplain_showExecStatsBegin().
 */
void
cdbexplain_localExecStats(struct PlanState                 *planstate,
                          struct CdbExplain_ShowStatCtx    *showstatctx)
{
    CdbExplain_LocalStatCtx ctx;
    MemoryContext           oldcxt;

    Assert(Gp_role != GP_ROLE_EXECUTE);

    Insist(planstate && planstate->instrument && showstatctx);

    /* Switch to the EXPLAIN memory context. */
    oldcxt = MemoryContextSwitchTo(showstatctx->explaincxt);

    memset(&ctx, 0, sizeof(ctx));

    /* Set up send context area. */
    ctx.send.notebuf = &showstatctx->extratextbuf;

    /* Set up a temporary StatHdr for both collecting and depositing stats. */
    ctx.msgptrs[0] = &ctx.send.hdr;
    ctx.send.hdr.segindex = GetQEIndex();
    ctx.send.hdr.nInst = 1;

    /* Set up receive context area referencing our temp StatHdr. */
    ctx.recv.nStatInst = ctx.send.hdr.nInst;
    ctx.recv.segindexMin = ctx.recv.segindexMax = ctx.send.hdr.segindex;

    ctx.recv.sliceIndex = LocallyExecutingSliceIndex(planstate->state);
    ctx.recv.msgptrs = ctx.msgptrs;
    ctx.recv.nmsgptr = 1;
    ctx.recv.dispatchResults = NULL;
    ctx.recv.extratextbuf = NULL;
    ctx.recv.showstatctx = showstatctx;

    /*
     * Collect and redeposit statistics from each PlanState node in this slice.
     * Any extra message text will be appended directly to extratextbuf.
     */
    planstate_walk_node(planstate, cdbexplain_localStatWalker, &ctx);

    /* Obtain per-slice stats and put them in SliceSummary. */
    cdbexplain_collectSliceStats(planstate, &ctx.send.hdr.worker);
    cdbexplain_depositSliceStats(&ctx.send.hdr, &ctx.recv);

    /* Restore caller's memory context. */
    MemoryContextSwitchTo(oldcxt);
}                               /* cdbexplain_localExecStats */


/*
 * cdbexplain_localStatWalker
 */
CdbVisitOpt
cdbexplain_localStatWalker(PlanState *planstate, void *context)
{
    CdbExplain_LocalStatCtx    *ctx = (CdbExplain_LocalStatCtx *)context;

    /* Collect stats into our temporary StatInst and caller's extratextbuf. */
    cdbexplain_collectStatsFromNode(planstate, &ctx->send);

    /* Redeposit stats back into Instrumentation, and attach a NodeSummary. */
    cdbexplain_depositStatsToNode(planstate, &ctx->recv);

    /* Don't descend across a slice boundary. */
    if (IsA(planstate, MotionState))
        return CdbVisit_Skip;

    return CdbVisit_Walk;
}                               /* cdbexplain_localStatWalker */


/*
 * cdbexplain_sendExecStats
 *    Called by qExec process to send EXPLAIN ANALYZE statistics to qDisp.
 *    On the qDisp, libpq will recognize our special message type ('Y') and
 *    attach the message to the current command's PGresult object.
 */
void
cdbexplain_sendExecStats(QueryDesc *queryDesc)
{
    EState                 *estate;
    PlanState              *planstate;
    CdbExplain_SendStatCtx  ctx;
    StringInfoData          notebuf;
    StringInfoData			memoryAccountTreeBuffer;

    /* Header offset (where header begins in the message buffer) */
    int                     hoff;

    Assert(Gp_role == GP_ROLE_EXECUTE);

    if (!queryDesc ||
        !queryDesc->estate)
        return;

    /* If executing a root slice (UPD/DEL/INS), start at top of plan tree. */
    estate = queryDesc->estate;
    if (LocallyExecutingSliceIndex(estate) == RootSliceIndex(estate))
        planstate = queryDesc->planstate;

    /* Non-root slice: Start at child of our sending Motion node. */
    else
    {
    	planstate = &(getMotionState(queryDesc->planstate, LocallyExecutingSliceIndex(estate))->ps);
        Assert(planstate &&
               IsA(planstate, MotionState) &&
               planstate->lefttree);
        planstate = planstate->lefttree;
    }

	if (planstate == NULL)
		return;

    /* Start building the message header in our context area. */
    memset(&ctx, 0, sizeof(ctx));
    ctx.hdr.type = T_CdbExplain_StatHdr;
    ctx.hdr.segindex = GetQEIndex();
    //ctx.hdr.hostname =
    ctx.hdr.nInst = 0;
    gethostname(ctx.hdr.hostname,SEGMENT_IDENTITY_NAME_LENGTH);
   // strncpy(ctx.hdr.hostname, estate->ctx->name,SEGMENT_IDENTITY_NAME_LENGTH-1);

    /* Allocate a separate buffer where nodes can append extra message text. */
    initStringInfo(&notebuf);
    ctx.notebuf = &notebuf;

    /* Reserve buffer space for the message header (excluding 'inst' array). */
    pq_beginmessage(&ctx.buf, 'Y');

    /* Where the actual StatHdr begins*/
    hoff = ctx.buf.len;

    /* Write everything until inst member including "CdbExplain_SliceWorker worker" */
    appendBinaryStringInfo(&ctx.buf, (char *)&ctx.hdr, sizeof(ctx.hdr) - sizeof(ctx.hdr.inst));

    /* Append statistics from each PlanState node in this slice. */
    planstate_walk_node(planstate, cdbexplain_sendStatWalker, &ctx);

    /* Obtain per-slice stats and put them in StatHdr. */
    cdbexplain_collectSliceStats(planstate, &ctx.hdr.worker);

    /* Append MemoryAccount Tree */
    ctx.hdr.memAccountTreeStartOffset = ctx.buf.len - hoff;
    initStringInfo(&memoryAccountTreeBuffer);
    uint totalSerialized = MemoryAccounting_Serialize(&memoryAccountTreeBuffer);

    ctx.hdr.memAccountTreeNodeCount = totalSerialized;
    appendBinaryStringInfo(&ctx.buf, memoryAccountTreeBuffer.data, memoryAccountTreeBuffer.len);
    pfree(memoryAccountTreeBuffer.data);

    /* Append the extra message text. */
    ctx.hdr.bnotes = ctx.buf.len - hoff;
    appendBinaryStringInfo(&ctx.buf, notebuf.data, notebuf.len);
    ctx.hdr.enotes = ctx.buf.len - hoff;
    pfree(notebuf.data);

    /*
     * Move the message header into the buffer. Rewrite the updated header (with bnotes, enotes, nInst etc.)
     * Note: this is the second time we are writing the header. The first write merely reserves space for
     * the header
     */
    memcpy(ctx.buf.data+hoff, (char *)&ctx.hdr, sizeof(ctx.hdr) - sizeof(ctx.hdr.inst));

    /* Send message to qDisp process. */
    pq_endmessage(&ctx.buf);
}                               /* cdbexplain_sendExecStats */


/*
 * cdbexplain_sendStatWalker
 */
CdbVisitOpt
cdbexplain_sendStatWalker(PlanState *planstate, void *context)
{
    CdbExplain_SendStatCtx *ctx = (CdbExplain_SendStatCtx *)context;
    CdbExplain_StatInst    *si = &ctx->hdr.inst[0];

    /* Stuff stats into our temporary StatInst.  Add extra text to notebuf. */
    cdbexplain_collectStatsFromNode(planstate, ctx);

    /* Append StatInst instance to message. */
    appendBinaryStringInfo(&ctx->buf, (char *)si, sizeof(*si));
    ctx->hdr.nInst++;

    /* Don't descend across a slice boundary. */
    if (IsA(planstate, MotionState))
        return CdbVisit_Skip;

    return CdbVisit_Walk;
}                               /* cdbexplain_sendStatWalker */


/*
 * cdbexplain_recvExecStats
 *    Called by qDisp to transfer a slice's EXPLAIN ANALYZE statistics
 *    from the CdbDispatchResults structures to the PlanState tree.
 *    Recursively does the same for slices that are descendants of the
 *    one specified.
 *
 * 'showstatctx' is a CdbExplain_ShowStatCtx object which was created by
 *      calling cdbexplain_showExecStatsBegin().
 */
void
cdbexplain_recvExecStats(struct PlanState              *planstate,
                         struct CdbDispatchResults     *dispatchResults,
                         int                            sliceIndex,
                         struct CdbExplain_ShowStatCtx *showstatctx,
                         int							segmentNum)
{
    CdbDispatchResult          *dispatchResultBeg;
    CdbDispatchResult          *dispatchResultEnd;
    CdbExplain_RecvStatCtx      ctx;
    CdbExplain_DispatchSummary  ds;
    MemoryContext   oldcxt;
    int             iDispatch;
    int             nDispatch;
    int             imsgptr;

    if (!planstate ||
        !planstate->instrument ||
        !showstatctx)
        return;

    /*
     * Note that the caller may free the CdbDispatchResults upon return, maybe
     * before EXPLAIN ANALYZE examines the PlanState tree.  Consequently we
     * must not return ptrs into the dispatch result buffers, but must copy any
     * needed information into a sufficiently long-lived memory context.
     */
    oldcxt = MemoryContextSwitchTo(showstatctx->explaincxt);

    /* Initialize treewalk context. */
    memset(&ctx, 0, sizeof(ctx));
    ctx.dispatchResults = dispatchResults;
    ctx.extratextbuf = &showstatctx->extratextbuf;
    ctx.showstatctx = showstatctx;
    ctx.sliceIndex = sliceIndex;
	ctx.segmentNum = segmentNum;

    /* Find the slice's CdbDispatchResult objects. */
    dispatchResultBeg = cdbdisp_resultBegin(dispatchResults, sliceIndex);
    dispatchResultEnd = cdbdisp_resultEnd(dispatchResults, sliceIndex);
    nDispatch = dispatchResultEnd - dispatchResultBeg;

    /* Initialize worker counts. */
    memset(&ds, 0, sizeof(ds));
    ds.nResult = nDispatch;

    /* Find and validate the statistics returned from each qExec. */
    if (nDispatch > 0)
        ctx.msgptrs = (CdbExplain_StatHdr **)palloc0(nDispatch * sizeof(ctx.msgptrs[0]));
    for (iDispatch = 0; iDispatch < nDispatch; iDispatch++)
    {
        CdbDispatchResult  *dispatchResult = &dispatchResultBeg[iDispatch];
        PGresult           *pgresult;
        CdbExplain_StatHdr *hdr;
        pgCdbStatCell      *statcell;

        /* Update worker counts. */
        if (!dispatchResult->hasDispatched)
            ds.nNotDispatched++;
        else if (dispatchResult->wasCanceled)
            ds.nCanceled++;
        else if (dispatchResult->errcode)
            ds.nError++;
        else if (dispatchResult->okindex >= 0)
            ds.nOk++;               /* qExec returned successful completion */
        else
            ds.nIgnorableError++;   /* qExec returned an error that's likely a
                                     * side-effect of another qExec's failure,
                                     * e.g. an interconnect error
                                     */

        /* Find this qExec's last PGresult.  If none, skip to next qExec. */
        pgresult = cdbdisp_getPGresult(dispatchResult, -1);
        if (!pgresult)
            continue;

        /* Find our statistics in list of response messages.  If none, skip. */
        for (statcell = pgresult->cdbstats; statcell; statcell = statcell->next)
        {
            if (IsA((Node *)statcell->data, CdbExplain_StatHdr))
                break;
        }
        if (!statcell)
            continue;

        /* Validate the message header. */
        hdr = (CdbExplain_StatHdr *)statcell->data;
        if ((size_t)statcell->len < sizeof(*hdr) ||
            (size_t)statcell->len != (sizeof(*hdr) - sizeof(hdr->inst) +
            							hdr->nInst * sizeof(hdr->inst) +
            							hdr->memAccountTreeNodeCount * sizeof(SerializedMemoryAccount) +
            							hdr->enotes - hdr->bnotes) ||
            statcell->len != hdr->enotes ||
            hdr->segindex < -1 ||
            hdr->segindex >= segmentNum)
        {
            ereport(ERROR, (errcode(ERRCODE_GP_INTERCONNECTION_ERROR),
                            errmsg_internal("Invalid execution statistics "
                                            "response returned from seg%d.  "
                                            "length=%d",
                                            hdr->segindex,
                                            statcell->len),
                            errhint("Please verify that all instances are using "
                                    "the correct %s software version.",
                                    PACKAGE_NAME)
                    ));
        }

        /* Slice should have same number of plan nodes on every qExec. */
        if (iDispatch == 0)
            ctx.nStatInst = hdr->nInst;
        else
		{
			/* MPP-2140: what causes this ? */
			if (ctx.nStatInst != hdr->nInst)
				ereport(ERROR, (errcode(ERRCODE_GP_INTERCONNECTION_ERROR),
								errmsg("Invalid execution statistics "
									   "received stats node-count mismatch: cdbexplain_recvExecStats() ctx.nStatInst %d hdr->nInst %d", ctx.nStatInst, hdr->nInst),
								errhint("Please verify that all instances are using "
										"the correct %s software version.",
										PACKAGE_NAME)));

            Insist(ctx.nStatInst == hdr->nInst);
		}

        /* Save lowest and highest segment id for which we have stats. */
        if (iDispatch == 0)
            ctx.segindexMin = ctx.segindexMax = hdr->segindex;
        else if (ctx.segindexMax < hdr->segindex)
            ctx.segindexMax = hdr->segindex;
        else if (ctx.segindexMin > hdr->segindex)
            ctx.segindexMin = hdr->segindex;

        /* Save message ptr for easy reference. */
        ctx.msgptrs[ctx.nmsgptr] = hdr;
        ctx.nmsgptr++;
    }

    /* Attach NodeSummary to each PlanState node's Instrumentation node. */
    planstate_walk_node(planstate, cdbexplain_recvStatWalker, &ctx);

    /* Make sure we visited the right number of PlanState nodes. */
    Insist(ctx.iStatInst == ctx.nStatInst);

    /* Transfer per-slice stats from message headers to the SliceSummary. */
    for (imsgptr = 0; imsgptr < ctx.nmsgptr; imsgptr++)
        cdbexplain_depositSliceStats(ctx.msgptrs[imsgptr], &ctx);

    /* Transfer worker counts to SliceSummary. */
    showstatctx->slices[sliceIndex].dispatchSummary = ds;

    /* Restore caller's memory context. */
    MemoryContextSwitchTo(oldcxt);

    /* Clean up. */
    if (ctx.msgptrs)
        pfree(ctx.msgptrs);
}                               /* cdbexplain_recvExecStats */


/*
 * cdbexplain_recvStatWalker
 *    Update the given PlanState node's Instrument node with statistics
 *    received from qExecs.  Attach a CdbExplain_NodeSummary block to
 *    the Instrument node.  At a MotionState node, descend to child slice.
 */
CdbVisitOpt
cdbexplain_recvStatWalker(PlanState *planstate, void *context)
{
    CdbExplain_RecvStatCtx *ctx = (CdbExplain_RecvStatCtx *)context;

    /* If slice was dispatched to qExecs, and stats came back, grab 'em. */
    if (ctx->nmsgptr > 0)
    {
        /* Transfer received stats to Instrumentation, NodeSummary, etc. */
        cdbexplain_depositStatsToNode(planstate, ctx);

        /* Advance to next node's entry in all of the StatInst arrays. */
        ctx->iStatInst++;
    }

    /* Motion operator?  Descend to next slice. */
    if (IsA(planstate, MotionState))
    {
        cdbexplain_recvExecStats(planstate->lefttree,
                                 ctx->dispatchResults,
                                 ((Motion *)planstate->plan)->motionID,
                                 ctx->showstatctx,
                                 ctx->segmentNum);
        return CdbVisit_Skip;
    }

    return CdbVisit_Walk;
}                               /* cdbexplain_recvStatWalker */


/*
 * cdbexplain_collectSliceStats
 *    Obtain per-slice statistical observations from the current slice
 *    (which has just completed execution in the current process) and
 *    store the information in the given SliceWorker struct.
 *
 * 'planstate' is the top PlanState node of the current slice.
 */
static void
cdbexplain_collectSliceStats(PlanState                 *planstate,
                             CdbExplain_SliceWorker    *out_worker)
{
    EState     *estate = planstate->state;

    /* Max bytes malloc'ed under executor's per-query memory context. */
    out_worker->peakmemused =
        (double)MemoryContextGetPeakSpace(estate->es_query_cxt);

    out_worker->vmem_reserved = (double) VmemTracker_GetMaxReservedVmemBytes();

    out_worker->memory_accounting_global_peak = (double) MemoryAccountingPeakBalance;

}                               /* cdbexplain_collectSliceStats */


/*
 * cdbexplain_depositSliceStats
 *    Transfer a worker's per-slice stats contribution from StatHdr into the
 *    SliceSummary array in the ShowStatCtx.  Transfer the rollup of per-node
 *    stats from the RecvStatCtx into the SliceSummary.
 *
 * Kludge: In a non-parallel plan, slice numbers haven't been assigned, so we
 * may be called more than once with sliceIndex == 0: once for the outermost
 * query and once for each InitPlan subquery.  In this case we dynamically
 * expand the SliceSummary array.  CDB TODO: Always assign proper root slice
 * ids (in qDispSliceId field of SubPlan node); then remove this kludge.
 */
static void
cdbexplain_depositSliceStats(CdbExplain_StatHdr        *hdr,
                             CdbExplain_RecvStatCtx    *recvstatctx)
{
    int                         sliceIndex = recvstatctx->sliceIndex;
    CdbExplain_ShowStatCtx     *showstatctx = recvstatctx->showstatctx;
    CdbExplain_SliceSummary    *ss = &showstatctx->slices[sliceIndex];
    CdbExplain_SliceWorker     *ssw;
    int                         iworker;

    Insist(sliceIndex >= 0 &&
           sliceIndex < showstatctx->nslice);

    /* Kludge:  QD can have more than one 'Slice 0' if plan is non-parallel. */
    if (sliceIndex == 0 &&
        recvstatctx->dispatchResults == NULL &&
        ss->workers)
    {
        Assert(ss->nworker == 1 &&
               recvstatctx->segindexMin == hdr->segindex &&
               recvstatctx->segindexMax == hdr->segindex);

        /* Expand the SliceSummary array to make room for InitPlan subquery. */
        sliceIndex = showstatctx->nslice++;
        showstatctx->slices = (CdbExplain_SliceSummary *)
                repalloc(showstatctx->slices, showstatctx->nslice * sizeof(showstatctx->slices[0]));
        ss = &showstatctx->slices[sliceIndex];
        memset(ss, 0, sizeof(*ss));
    }    

    /* Slice's first worker? */
    if (!ss->workers)
    {
        /* Caller already switched to EXPLAIN context. */
        Assert(CurrentMemoryContext == showstatctx->explaincxt);

        /* Allocate SliceWorker array and attach it to the SliceSummary. */
        ss->segindex0 = recvstatctx->segindexMin;
        ss->nworker = recvstatctx->segindexMax + 1 - ss->segindex0;
        ss->workers = (CdbExplain_SliceWorker *)palloc0(ss->nworker * sizeof(ss->workers[0]));
        ss->memoryTreeRoots = (SerializedMemoryAccount **)palloc0(ss->nworker * sizeof(ss->memoryTreeRoots[0]));
    }

    /* Save a copy of this SliceWorker instance in the worker array. */
    iworker = hdr->segindex - ss->segindex0;
    ssw = &ss->workers[iworker];
    Insist(iworker >= 0 && iworker < ss->nworker);
    Insist(ssw->peakmemused == 0);  /* each worker should be seen just once */
    *ssw = hdr->worker;

    const char *originalSerializedMemoryAccountingStartAddress = ((const char*) hdr) +
    		hdr->memAccountTreeStartOffset;

    size_t bitCount = sizeof(SerializedMemoryAccount) * hdr->memAccountTreeNodeCount;
    /*
     * We need to copy of the serialized bits. These bits have shorter lifespan
     * and can get out of scope before we finish explain analyze.
     */
    void *copiedSerializedMemoryAccountingStartAddress = palloc(bitCount);
    memcpy(copiedSerializedMemoryAccountingStartAddress, originalSerializedMemoryAccountingStartAddress, bitCount);

    ss->memoryTreeRoots[iworker] = MemoryAccounting_Deserialize(copiedSerializedMemoryAccountingStartAddress,
    		hdr->memAccountTreeNodeCount);

    /* Rollup of per-worker stats into SliceSummary */
    cdbexplain_agg_upd(&ss->peakmemused, hdr->worker.peakmemused, hdr->segindex, hdr->hostname);
    cdbexplain_agg_upd(&ss->vmem_reserved, hdr->worker.vmem_reserved, hdr->segindex, hdr->hostname);
    cdbexplain_agg_upd(&ss->memory_accounting_global_peak, hdr->worker.memory_accounting_global_peak, hdr->segindex, hdr->hostname);

    /* Rollup of per-node stats over all nodes of the slice into SliceSummary */
    ss->workmemused_max = recvstatctx->workmemused_max;
    ss->workmemwanted_max = recvstatctx->workmemwanted_max;

    /* Rollup of per-node stats over the whole query into ShowStatCtx. */
    showstatctx->workmemused_max = Max(showstatctx->workmemused_max, recvstatctx->workmemused_max);
    showstatctx->workmemwanted_max = Max(showstatctx->workmemwanted_max, recvstatctx->workmemwanted_max);
}                               /* cdbexplain_depositSliceStats */


/*
 * cdbexplain_collectStatsFromNode
 *
 * Called by sendStatWalker and localStatWalker to obtain a node's statistics
 * and transfer them into the temporary StatHdr and StatInst in the SendStatCtx.
 * Also obtains the node's extra message text, which it appends to the caller's
 * cxt->nodebuf.
 */
static void
cdbexplain_collectStatsFromNode(PlanState *planstate, CdbExplain_SendStatCtx *ctx)
{
    CdbExplain_StatInst    *si = &ctx->hdr.inst[0];
    Instrumentation        *instr = planstate->instrument;

    Insist(instr);

	/* Save the state whether this node is completed the first tuple */
	bool running = instr->running;

    /* We have to finalize statistics, since ExecutorEnd hasn't been called. */
    InstrEndLoop(instr);

    if (Debug_print_execution_detail) {
      instr_time time;
      INSTR_TIME_SET_CURRENT(time);
      elog(DEBUG1,"The time after processing node %d : %.3f ms",
           planstate->type, 1000.0 * INSTR_TIME_GET_DOUBLE(time));
    }

    /* Initialize the StatInst slot in the temporary StatHdr. */
    memset(si, 0, sizeof(*si));
    si->pstype = planstate->type;

    /* Add this node's extra message text to notebuf.  Store final stats. */
    si->bnotes = cdbexplain_collectExtraText(planstate, ctx->notebuf);
    si->enotes = ctx->notebuf->len;

    /* Make sure there is a '\0' between this node's message and the next. */
    if (si->bnotes < si->enotes)
        appendStringInfoChar(ctx->notebuf, '\0');

    /* Transfer this node's statistics from Instrumentation into StatInst. */
	si->running         = running;
	si->starttime       = instr->starttime;
	si->counter         = instr->counter;
	si->firsttuple      = instr->firsttuple;
    si->startup         = instr->startup;
    si->total           = instr->total;
    si->ntuples         = instr->ntuples;
    si->nloops          = instr->nloops;
    si->execmemused     = instr->execmemused;
    si->workmemused     = instr->workmemused;
    si->workmemwanted   = instr->workmemwanted;
    si->workfileReused   = instr->workfileReused;
    si->workfileCreated  = instr->workfileCreated;
	si->peakMemBalance	 = MemoryAccounting_GetPeak(planstate->plan->memoryAccount);
	si->firststart      = instr->firststart;
	si->numPartScanned = instr->numPartScanned;
}                               /* cdbexplain_collectStatsFromNode */


/*
 * CdbExplain_DepStatAcc
 *    Segment statistic accumulator used by cdbexplain_depositStatsToNode().
 */
typedef struct CdbExplain_DepStatAcc
{
	/* vmax, vsum, vcnt, segmax */
    CdbExplain_Agg          agg;

    /* max's received StatHdr */
    CdbExplain_StatHdr     *rshmax;
    /* max's received inst in StatHdr */
    CdbExplain_StatInst    *rsimax;
    /* max's inst in NodeSummary */
    CdbExplain_StatInst    *nsimax;

    /* max's received StatHdr */
    CdbExplain_StatHdr     *rshLast;
    /* max's received inst in StatHdr */
    CdbExplain_StatInst    *rsiLast;
    /* max's inst in NodeSummary */
    CdbExplain_StatInst    *nsiLast;

    /* max run-time of all the segments */
    double                  max_total;
    /* start time of the first iteration for node with maximum runtime */
    instr_time              firststart_of_max_total;
} CdbExplain_DepStatAcc;

static void
cdbexplain_depStatAcc_init0(CdbExplain_DepStatAcc *acc)
{
    cdbexplain_agg_init0(&acc->agg);
    acc->rshmax = NULL;
    acc->rsimax = NULL;
    acc->nsimax = NULL;
    acc->rshLast = NULL;
    acc->rsiLast = NULL;
    acc->nsiLast = NULL;
    acc->max_total = 0;
    INSTR_TIME_SET_ZERO(acc->firststart_of_max_total);
}                               /* cdbexplain_depStatAcc_init0 */

static inline void
cdbexplain_depStatAcc_upd(CdbExplain_DepStatAcc    *acc,
                          double                    v,
                          CdbExplain_StatHdr       *rsh,
                          CdbExplain_StatInst      *rsi,
                          CdbExplain_StatInst      *nsi)
{
    if (cdbexplain_agg_upd(&acc->agg, v, rsh->segindex,rsh->hostname))
    {
        acc->rshmax = rsh;
        acc->rsimax = rsi;
        acc->nsimax = nsi;
    }

    if (acc->max_total < nsi->total)
    {
		acc->rshLast = rsh;
		acc->rsiLast = rsi;
		acc->nsiLast = nsi;
		acc->agg.ilast = rsh->segindex;
		strncpy(acc->agg.hostnamelast, rsh->hostname,SEGMENT_IDENTITY_NAME_LENGTH-1);
		acc->agg.vlast = v;
		acc->max_total = nsi->total;
		INSTR_TIME_ASSIGN(acc->firststart_of_max_total, nsi->firststart);
    }
}                               /* cdbexplain_depStatAcc_upd */

static void
cdbexplain_depStatAcc_saveText(CdbExplain_DepStatAcc   *acc,
                               StringInfoData          *extratextbuf,
                               bool                    *saved_inout)
{
    CdbExplain_StatHdr     *rsh = acc->rshmax;
    CdbExplain_StatInst    *rsi = acc->rsimax;
    CdbExplain_StatInst    *nsi = acc->nsimax;

    if (acc->agg.vcnt > 0 &&
        nsi->bnotes == nsi->enotes &&
        rsi->bnotes < rsi->enotes)
    {
        /* Locate extra message text in dispatch result buffer. */
        int         notelen = rsi->enotes - rsi->bnotes;
        const char *notes = (const char *)rsh + rsh->bnotes + rsi->bnotes;

        Insist(rsh->bnotes + rsi->enotes < rsh->enotes &&
               notes[notelen] == '\0');

        /* Append to extratextbuf. */
        nsi->bnotes = extratextbuf->len;
        appendBinaryStringInfo(extratextbuf, notes, notelen);
        nsi->enotes = extratextbuf->len;

        /* Tell caller that some extra text has been saved. */
        if (saved_inout)
            *saved_inout = true;
    }
}                               /* cdbexplain_depStatAcc_saveText */


/*
 * cdbexplain_depositStatsToNode
 *
 * Called by recvStatWalker and localStatWalker to update the given
 * PlanState node's Instrument node with statistics received from
 * workers or collected locally.  Attaches a CdbExplain_NodeSummary
 * block to the Instrument node.  If top node of slice, per-slice
 * statistics are transferred from the StatHdr to the SliceSummary.
 */
static void
cdbexplain_depositStatsToNode(PlanState *planstate, CdbExplain_RecvStatCtx *ctx)
{
    Instrumentation            *instr = planstate->instrument;
    CdbExplain_StatHdr         *rsh; /* The header (which includes StatInst) */
    CdbExplain_StatInst        *rsi; /* The current StatInst */

    /*
     * Points to the insts array of node summary (CdbExplain_NodeSummary). Used
     * for saving every rsi in the node summary (in addition to saving the max/avg).
     */
    CdbExplain_StatInst        *nsi;

    /*
     * ns is the node summary across all QEs of the segworker group. It also contains detailed "unsummarized"
     * raw stat for a node across all QEs in current segworker group (in the insts array)
     */
    CdbExplain_NodeSummary     *ns;
    CdbExplain_DepStatAcc       ntuples;
    CdbExplain_DepStatAcc       execmemused;
    CdbExplain_DepStatAcc       workmemused;
    CdbExplain_DepStatAcc       workmemwanted;
    CdbExplain_DepStatAcc       totalWorkfileReused;
    CdbExplain_DepStatAcc       totalWorkfileCreated;
    CdbExplain_DepStatAcc       peakmemused;
    CdbExplain_DepStatAcc		vmem_reserved;
    CdbExplain_DepStatAcc		memory_accounting_global_peak;
    CdbExplain_DepStatAcc       peakMemBalance;
    CdbExplain_DepStatAcc       totalPartTableScanned;
    int                         imsgptr;
    int                         nInst;

    Insist(instr &&
           ctx->iStatInst < ctx->nStatInst);

    /* Caller already switched to EXPLAIN context. */
    Assert(CurrentMemoryContext == ctx->showstatctx->explaincxt);

    /* Allocate NodeSummary block. */
    nInst = ctx->segindexMax + 1 - ctx->segindexMin;
    ns = (CdbExplain_NodeSummary *)palloc0(sizeof(*ns) - sizeof(ns->insts) +
                                           nInst*sizeof(ns->insts[0]));
    ns->segindex0 = ctx->segindexMin;
    ns->ninst = nInst;

    /* Attach our new NodeSummary to the Instrumentation node. */
    instr->cdbNodeSummary = ns;

    /* Initialize per-node accumulators. */
    cdbexplain_depStatAcc_init0(&ntuples);
    cdbexplain_depStatAcc_init0(&execmemused);
    cdbexplain_depStatAcc_init0(&workmemused);
    cdbexplain_depStatAcc_init0(&workmemwanted);
	cdbexplain_depStatAcc_init0(&totalWorkfileReused);
	cdbexplain_depStatAcc_init0(&totalWorkfileCreated);
    cdbexplain_depStatAcc_init0(&peakMemBalance);
    cdbexplain_depStatAcc_init0(&totalPartTableScanned);

    /* Initialize per-slice accumulators. */
    cdbexplain_depStatAcc_init0(&peakmemused);
    cdbexplain_depStatAcc_init0(&vmem_reserved);
    cdbexplain_depStatAcc_init0(&memory_accounting_global_peak);

    bool isRunning = false;

    /* Examine the statistics from each qExec. */
    for (imsgptr = 0; imsgptr < ctx->nmsgptr; imsgptr++)
    {
        /* Locate PlanState node's StatInst received from this qExec. */
        rsh = ctx->msgptrs[imsgptr];
        // char* hostname = ctx->dispatchResults->resultArray[imsgptr].segdbDesc->segment->hostname;
        rsi = &rsh->inst[ctx->iStatInst];

        Insist(rsi->pstype == planstate->type &&
               ns->segindex0 <= rsh->segindex &&
               rsh->segindex < ns->segindex0 + ns->ninst);

        /* Locate this qExec's StatInst slot in node's NodeSummary block. */
        nsi = &ns->insts[rsh->segindex - ns->segindex0];

        /* Copy the StatInst to NodeSummary from dispatch result buffer. */
        *nsi = *rsi;


        if (Debug_print_execution_detail) {
          elog(DEBUG1,"The time information for node %d on executor %d: "
              "starttime: %.3f ms, counter: %.3f ms, firsttuple: %.3f ms, "
              "startup: %.3f ms, total: %.3f ms, ntuples: %.3f, "
              "nloops: %.3f",
               planstate->type, rsh->segindex,
               1000.0 * INSTR_TIME_GET_DOUBLE(nsi->starttime),
               1000.0 * INSTR_TIME_GET_DOUBLE(nsi->counter),
               1000.0 * nsi->firsttuple, 1000.0 * nsi->startup, 1000.0 * nsi->total,
               nsi->ntuples, nsi->nloops
          );
        }

        /*
         * Drop qExec's extra text.  We rescue it below if qExec is a winner.
         * For local qDisp slice, ctx->extratextbuf is NULL, which tells us to
         * leave the extra text undisturbed in its existing buffer.
         */
        if (ctx->extratextbuf)
            nsi->bnotes = nsi->enotes = 0;

        if (nsi->running)
        {
        	  isRunning = true;
        }

        /* Update per-node accumulators. */
        cdbexplain_depStatAcc_upd(&ntuples, rsi->ntuples, rsh, rsi, nsi);
        cdbexplain_depStatAcc_upd(&execmemused, rsi->execmemused, rsh, rsi, nsi);
        cdbexplain_depStatAcc_upd(&workmemused, rsi->workmemused, rsh, rsi, nsi);
        cdbexplain_depStatAcc_upd(&workmemwanted, rsi->workmemwanted, rsh, rsi, nsi);
        cdbexplain_depStatAcc_upd(&totalWorkfileReused, (rsi->workfileReused ? 1 : 0), rsh, rsi, nsi);
        cdbexplain_depStatAcc_upd(&totalWorkfileCreated, (rsi->workfileCreated ? 1 : 0), rsh, rsi, nsi);
        cdbexplain_depStatAcc_upd(&peakMemBalance, rsi->peakMemBalance, rsh, rsi, nsi);
        cdbexplain_depStatAcc_upd(&totalPartTableScanned, rsi->numPartScanned, rsh, rsi, nsi);

        /* Update per-slice accumulators. */
        cdbexplain_depStatAcc_upd(&peakmemused, rsh->worker.peakmemused, rsh, rsi, nsi);
        cdbexplain_depStatAcc_upd(&vmem_reserved, rsh->worker.vmem_reserved, rsh, rsi, nsi);
        cdbexplain_depStatAcc_upd(&memory_accounting_global_peak, rsh->worker.memory_accounting_global_peak, rsh, rsi, nsi);
    }

    if (Debug_print_execution_detail) {
      if (ntuples.nsimax)
      {
        elog(DEBUG1,"The time information for node %d on executor: RSIMAX"
            "starttime: %.3f ms, counter: %.3f ms, firsttuple: %.3f ms, "
            "startup: %.3f ms, total: %.3f ms, ntuples: %.3f, "
            "nloops: %.3f",
            planstate->type, 1000.0 * INSTR_TIME_GET_DOUBLE(ntuples.nsimax->starttime),
            1000.0 * INSTR_TIME_GET_DOUBLE(ntuples.nsimax->counter),
            1000.0 * ntuples.nsimax->firsttuple, 1000.0 * ntuples.nsimax->startup,
            1000.0 * ntuples.nsimax->total,
            ntuples.nsimax->ntuples, ntuples.nsimax->nloops
        );
      }
      else
      {
        elog(DEBUG1, "The time information for node %d on executor: RSIMAX",
            planstate->type);
      }
    }

    /* Save per-node accumulated stats in NodeSummary. */
    ns->ntuples = ntuples.agg;
    ns->execmemused = execmemused.agg;
    ns->workmemused = workmemused.agg;
    ns->workmemwanted = workmemwanted.agg;
    ns->totalWorkfileReused = totalWorkfileReused.agg;
    ns->totalWorkfileCreated = totalWorkfileCreated.agg;
    ns->peakMemBalance = peakMemBalance.agg;
    ns->totalPartTableScanned = totalPartTableScanned.agg;

    /* Roll up summary over all nodes of slice into RecvStatCtx. */
    ctx->workmemused_max = Max(ctx->workmemused_max, workmemused.agg.vmax);
    ctx->workmemwanted_max = Max(ctx->workmemwanted_max, workmemwanted.agg.vmax);

    instr->running = isRunning;
    instr->total = ntuples.max_total;
    instr->totalLast = ntuples.max_total;
    INSTR_TIME_ASSIGN(instr->firststart, ntuples.firststart_of_max_total);

    /* Put winner's stats into qDisp PlanState's Instrument node. */
    if (ntuples.agg.vcnt > 0)
    {
      instr->running          = ntuples.nsimax->running;
      instr->starttime        = ntuples.nsimax->starttime;
      instr->counter          = ntuples.nsimax->counter;
      instr->firsttuple       = ntuples.nsimax->firsttuple;
      instr->startup          = ntuples.nsimax->startup;
      instr->total            = ntuples.nsimax->total;
      instr->startupLast      = ntuples.nsiLast->startup;
      instr->totalLast        = ntuples.nsiLast->total;
      instr->ntuples          = ntuples.nsimax->ntuples;
      instr->nloops           = ntuples.nsimax->nloops;
      instr->execmemused      = ntuples.nsimax->execmemused;
      instr->workmemused      = ntuples.nsimax->workmemused;
      instr->workmemwanted    = ntuples.nsimax->workmemwanted;
      instr->workfileReused   = ntuples.nsimax->workfileReused;
      instr->workfileCreated  = ntuples.nsimax->workfileCreated;
      instr->firststart       = ntuples.nsimax->firststart;
      instr->firststartLast       = ntuples.nsiLast->firststart;
    }

    /* Save extra message text for the most interesting winning qExecs. */
    if (ctx->extratextbuf)
    {
      bool    saved = false;

      /* One worker which used or wanted the most work_mem */
      if (workmemwanted.agg.vmax >= workmemused.agg.vmax)
          cdbexplain_depStatAcc_saveText(&workmemwanted, ctx->extratextbuf, &saved);
      else if (workmemused.agg.vmax > 1.05 * cdbexplain_agg_avg(&workmemused.agg))
          cdbexplain_depStatAcc_saveText(&workmemused, ctx->extratextbuf, &saved);

      /* Worker which used the most executor memory (this node's usage) */
      if (execmemused.agg.vmax > 1.05 * cdbexplain_agg_avg(&execmemused.agg))
          cdbexplain_depStatAcc_saveText(&execmemused, ctx->extratextbuf, &saved);

      /*
       * For the worker which had the highest peak executor memory usage
       * overall across the whole slice, we'll report the extra message
       * text from all of the nodes in the slice.  But only if that worker
       * stands out more than 5% above the average.
       */
      if (peakmemused.agg.vmax > 1.05 * cdbexplain_agg_avg(&peakmemused.agg))
          cdbexplain_depStatAcc_saveText(&peakmemused, ctx->extratextbuf, &saved);

      /*
       * One worker that used cached workfiles.
       */
      if (totalWorkfileReused.agg.vcnt > 0)
      {
        cdbexplain_depStatAcc_saveText(&totalWorkfileReused, ctx->extratextbuf, &saved);
      }

      /*
       * One worker which produced the greatest number of output rows.
       * (Always give at least one node a chance to have its extra message
       * text seen.  In case no node stood out above the others, make a
       * repeatable choice based on the number of output rows.)
       */
      if (!saved ||
          ntuples.agg.vmax > 1.05 * cdbexplain_agg_avg(&ntuples.agg))
          cdbexplain_depStatAcc_saveText(&ntuples, ctx->extratextbuf, &saved);
   }
}                               /* cdbexplain_depositStatsToNode */


/*
 * cdbexplain_collectExtraText
 *    Allow a node to supply additional text for its EXPLAIN ANALYZE report.
 *
 * Returns the starting offset of the extra message text from notebuf->data.
 * The caller can compute the length as notebuf->len minus the starting offset.
 * If the node did not provide any extra message text, the length will be 0.
 */
int
cdbexplain_collectExtraText(PlanState *planstate, StringInfo notebuf)
{
    int     bnotes = notebuf->len;

    /*
     * Invoke node's callback.  It may append to our notebuf and/or its own
     * cdbexplainbuf; and store final statistics in its Instrumentation node.
     */
    if (planstate->cdbexplainfun)
        planstate->cdbexplainfun(planstate, notebuf);

    /*
     * Append contents of node's extra message buffer.  This allows nodes to
     * contribute EXPLAIN ANALYZE info without having to set up a callback.
     */
    if (planstate->cdbexplainbuf &&
        planstate->cdbexplainbuf->len > 0)
    {
        /* If callback added to notebuf, make sure text ends with a newline. */
        if (bnotes < notebuf->len &&
            notebuf->data[notebuf->len-1] != '\n')
            appendStringInfoChar(notebuf, '\n');

        appendBinaryStringInfo(notebuf, planstate->cdbexplainbuf->data,
                               planstate->cdbexplainbuf->len);

        truncateStringInfo(planstate->cdbexplainbuf, 0);
    }

    return bnotes;
}                               /* cdbexplain_collectExtraText */


/*
 * cdbexplain_formatExtraText
 *    Format extra message text into the EXPLAIN output buffer.
 */
static void
cdbexplain_formatExtraText(StringInfo   str,
                           int          indent,
                           int          segindex,
						   bool         workfileReuse,
                           const char  *notes,
                           int          notelen)
{
    const char *cp = notes;
    const char *ep = notes + notelen;
	const char *reuse = "";
	if (workfileReuse)
	{
		reuse = " reuse";
	}

    /* Could be more than one line... */
    while (cp < ep)
    {
        const char *nlp = strchr(cp, '\n');
        const char *dp = nlp ? nlp : ep;

        /* Strip trailing whitespace. */
        while (cp < dp &&
               isspace(dp[-1]))
            dp--;

        /* Add to output buffer. */
        if (cp < dp)
        {
            appendStringInfoFill(str, 2*indent, ' ');
            if (segindex >= 0)
            {
                appendStringInfo(str, "(seg%d%s) ", segindex, reuse);
                if (segindex < 10)
                    appendStringInfoChar(str, ' ');
                if (segindex < 100)
                    appendStringInfoChar(str, ' ');
            }
            appendBinaryStringInfo(str, cp, dp-cp);
            appendStringInfoChar(str, '\n');
        }

        if (!nlp)
            break;
        cp = nlp+1;
    }
}                               /* cdbexplain_formatExtraText */



/*
 * cdbexplain_formatMemory
 *    Convert memory size to string from (double) bytes.
 *
 * 	  	outbuf:  [output] pointer to a char buffer to be filled
 * 		bufsize: [input] maximum number of characters to write to outbuf (must be set by the caller)
 * 		bytes:   [input] a value representing memory size in bytes to be written to outbuf
 */
static void
cdbexplain_formatMemory(char *outbuf, int bufsize, double bytes)
{
	Assert(outbuf != NULL &&  "CDBEXPLAIN: char buffer is null");
	Assert(bufsize > 0 &&  "CDBEXPLAIN: size of char buffer is zero");
	/*check if truncation occurs */
#ifdef USE_ASSERT_CHECKING
	int nchars_written =
#endif /* USE_ASSERT_CHECKING */
	snprintf(outbuf, bufsize, "%.0fK bytes", floor((bytes + 1023.0)/1024.0));
	Assert(nchars_written < bufsize &&
		   "CDBEXPLAIN:  size of char buffer is smaller than the required number of chars");
}                              /* cdbexplain_formatMemory */



/*
 * cdbexplain_formatSeconds
 *    Convert time in seconds to readable string
 *
 *   	outbuf:  [output] pointer to a char buffer to be filled
 * 		bufsize: [input] maximum number of characters to write to outbuf (must be set by the caller)
 * 		seconds: [input] a value representing no. of seconds to be written to outbuf
 */
static void
cdbexplain_formatSeconds(char* outbuf, int bufsize, double seconds)
{
	Assert(outbuf != NULL &&  "CDBEXPLAIN: char buffer is null");
	Assert(bufsize > 0 &&  "CDBEXPLAIN: size of char buffer is zero");
	double ms = seconds * 1000.0;
	/*check if truncation occurs */
#ifdef USE_ASSERT_CHECKING
	int nchars_written =
#endif /* USE_ASSERT_CHECKING */
	snprintf(outbuf, bufsize, "%.*f ms",
            (ms < 10.0 && ms != 0.0 && ms > -10.0) ? 3 : 0,
            ms);
	Assert(nchars_written < bufsize &&
		   "CDBEXPLAIN:  size of char buffer is smaller than the required number of chars");
}                               /* cdbexplain_formatSeconds */

/*
 * cdbexplain_formatSeconds
 *    Convert time in seconds to readable string
 *
 *   	outbuf:  [output] pointer to a char buffer to be filled
 * 		bufsize: [input] maximum number of characters to write to outbuf (must be set by the caller)
 * 		seconds: [input] a value representing no. of seconds to be written to outbuf
 */
static void
cdbexplain_formatPairSeconds(char* outbuf, int bufsize, double seconds,double secondsLast)
{
	Assert(outbuf != NULL &&  "CDBEXPLAIN: char buffer is null");
	Assert(bufsize > 0 &&  "CDBEXPLAIN: size of char buffer is zero");
	double ms = seconds * 1000.0;
	double mslast = secondsLast * 1000.0;
	/*check if truncation occurs */
#ifdef USE_ASSERT_CHECKING
	int nchars_written =
#endif /* USE_ASSERT_CHECKING */
	snprintf(outbuf, bufsize, "%.*f/%.*f ms",
            (ms < 10.0 && ms != 0.0 && ms > -10.0) ? 3 : 0,
            ms,(mslast < 10.0 && mslast != 0.0 && mslast > -10.0) ? 3 : 0, mslast);
	Assert(nchars_written < bufsize &&
		   "CDBEXPLAIN:  size of char buffer is smaller than the required number of chars");
}

/*
 * cdbexplain_formatSeg
 *    Convert segment id to string. 
 *
 *    	outbuf:  [output] pointer to a char buffer to be filled
 * 		bufsize: [input] maximum number of characters to write to outbuf (must be set by the caller)
 * 		segindex:[input] a value representing segment index to be written to outbuf
 * 		nInst:   [input] no. of stat instances
 */
static void
cdbexplain_formatSeg(char *outbuf, int bufsize, int segindex, int nInst,char* hostname)
{
	Assert(outbuf != NULL &&  "CDBEXPLAIN: char buffer is null");
	Assert(bufsize > 0 &&  "CDBEXPLAIN: size of char buffer is zero");

    if ( nInst > 1 && segindex >= 0){
    	/*check if truncation occurs */
#ifdef USE_ASSERT_CHECKING
    	int nchars_written =
#endif /* USE_ASSERT_CHECKING */
        snprintf(outbuf, bufsize, " (seg%d:%s)", segindex,hostname);
    	Assert(nchars_written < bufsize &&
    		   "CDBEXPLAIN:  size of char buffer is smaller than the required number of chars");
    }
    else{
        outbuf[0] = '\0';
    }
}                               /* cdbexplain_formatSeg */

/*
 * cdbexplain_formatSeg
 *    Convert segment id to string.
 *
 *    	outbuf:  [output] pointer to a char buffer to be filled
 * 		bufsize: [input] maximum number of characters to write to outbuf (must be set by the caller)
 * 		segindex:[input] a value representing segment index to be written to outbuf
 * 		nInst:   [input] no. of stat instances
 */
static void
cdbexplain_formatSegNoParenthesis(char *outbuf, int bufsize, int segindex, int nInst,char* hostname)
{
	Assert(outbuf != NULL &&  "CDBEXPLAIN: char buffer is null");
	Assert(bufsize > 0 &&  "CDBEXPLAIN: size of char buffer is zero");

    if ( nInst > 1 && segindex >= 0){
    	/*check if truncation occurs */
#ifdef USE_ASSERT_CHECKING
    	int nchars_written =
#endif /* USE_ASSERT_CHECKING */
        snprintf(outbuf, bufsize, "seg%d:%s", segindex,hostname);
    	Assert(nchars_written < bufsize &&
    		   "CDBEXPLAIN:  size of char buffer is smaller than the required number of chars");
    }
    else{
        outbuf[0] = '\0';
    }
}                               /* cdbexplain_formatSeg */


/*
 * cdbexplain_showExecStatsBegin
 *    Called by qDisp process to create a CdbExplain_ShowStatCtx structure
 *    in which to accumulate overall statistics for a query.
 *
 * 'querystarttime' is the timestamp of the start of the query, in a
 *      platform-dependent format.
 * 'explaincxt' is a MemoryContext from which to allocate the ShowStatCtx as
 *      well as any needed buffers and the like.  The explaincxt ptr is saved
 *      in the ShowStatCtx.  The caller is expected to reset or destroy the
 *      explaincxt not too long after calling cdbexplain_showExecStatsEnd(); so
 *      we don't bother to pfree() memory that we allocate from this context.
 *
 * Note this function is called before ExecutorStart(), so there is no EState
 * or SliceTable yet.
 */
struct CdbExplain_ShowStatCtx *
cdbexplain_showExecStatsBegin(struct QueryDesc *queryDesc,
                              MemoryContext     explaincxt,
                              instr_time        querystarttime)
{
    MemoryContext           oldcontext;
    CdbExplain_ShowStatCtx *ctx;
    int                     nslice;

    Assert(Gp_role != GP_ROLE_EXECUTE);

    /* Switch to EXPLAIN memory context. */
    oldcontext = MemoryContextSwitchTo(explaincxt);

    /* Allocate and zero the ShowStatCtx */
    ctx = (CdbExplain_ShowStatCtx *)palloc0(sizeof(*ctx));

    ctx->explaincxt = explaincxt;
    ctx->querystarttime = querystarttime;

    /* Determine number of slices.  (SliceTable hasn't been built yet.) */
    nslice = 1 + queryDesc->plannedstmt->planTree->nMotionNodes + queryDesc->plannedstmt->planTree->nInitPlans;

    /* Allocate and zero the SliceSummary array. */
    ctx->nslice = nslice;
    ctx->slices = (CdbExplain_SliceSummary *)palloc0(nslice * sizeof(ctx->slices[0]));

    /* Allocate a buffer in which we can collect any extra message text. */
    initStringInfoOfSize(&ctx->extratextbuf, 4000);

    /* Restore caller's MemoryContext. */
    MemoryContextSwitchTo(oldcontext);
    return ctx;
}                               /* cdbexplain_showExecStatsBegin */

/*
 * nodeSupportWorkfileCaching
 *   Return true if a given node supports workfile caching.
 */
static bool
nodeSupportWorkfileCaching(PlanState *planstate)
{
	return (IsA(planstate, SortState) ||
			IsA(planstate, HashJoinState) ||
			(IsA(planstate, AggState) && ((Agg*)planstate->plan)->aggstrategy == AGG_HASHED) ||
			IsA(planstate, MaterialState));
}

/*
 * cdbexplain_showExecStats
 *    Called by qDisp process to format a node's EXPLAIN ANALYZE statistics.
 *
 * 'planstate' is the node whose statistics are to be displayed.
 * 'str' is the output buffer.
 * 'indent' is the root indentation for all the text generated for explain output
 * 'ctx' is a CdbExplain_ShowStatCtx object which was created by a call to
 *      cdbexplain_showExecStatsBegin().
 */
void
cdbexplain_showExecStats(struct PlanState              *planstate,
                         struct StringInfoData         *str,
                         int                            indent,
                         struct CdbExplain_ShowStatCtx *ctx)
{
    Instrumentation            *instr = planstate->instrument;
    CdbExplain_NodeSummary     *ns = instr->cdbNodeSummary;
    instr_time                  timediff;
    instr_time                  timediffLast;
    double                      ntuples_avg;
    int                         i;

    const char *s_row = " row";
    const char *s_rows = " rows";
    char        firstbuf[50];
    char        totalbuf[50];
    char        avgbuf[50];
    char        maxbuf[50];
    char        segbufWithParenthese[50];
    char        segbuf[50];
    char        segbufLast[50];
    char        startbuf[50];		

    /* Might not have received stats from qExecs if they hit errors. */
    if (!ns)
        return;

	Assert(instr != NULL);
	const char *noRowRequested = "";
	if (!instr->running)
	{
		noRowRequested = "(No row requested) ";
	}

    /*
     * Row counts.  Also, timings from the worker with the most output rows.
     */
    appendStringInfoFill(str, 2 * indent, ' ');
    cdbexplain_formatSeg(segbufWithParenthese, sizeof(segbufWithParenthese), ns->ntuples.imax, ns->ninst,ns->ntuples.hostnamemax);
    cdbexplain_formatSegNoParenthesis(segbuf, sizeof(segbuf), ns->ntuples.imax, ns->ninst,ns->ntuples.hostnamemax);
    cdbexplain_formatSegNoParenthesis(segbufLast, sizeof(segbufLast), ns->ntuples.ilast, ns->ninst,ns->ntuples.hostnamelast);
    ntuples_avg = cdbexplain_agg_avg(&ns->ntuples);
    bool containMaxRowAndLast=false;
    switch (planstate->type)
    {
        case T_BitmapAndState:
        case T_BitmapOrState:
        case T_BitmapIndexScanState:
            s_row = "";
            s_rows = "";
            if (ns->ntuples.vcnt > 1){
                appendStringInfo(str,
                                 "Bitmaps out:  Avg %.1f x %d workers."
                                 "  Max/Last(%s/%s) %.0f/%.0f rows",
                                 ntuples_avg,
								ns->ntuples.vcnt,
								segbuf,segbufLast,
								ns->ntuples.vmax,ns->ntuples.vlast);
                containMaxRowAndLast = true;
            }
            else
                appendStringInfo(str,
                                 "Bitmaps out:  %s%.0f%s",
								 noRowRequested,
                                 ns->ntuples.vmax,
								 segbufWithParenthese);
            break;
        case T_HashState:
            if (ns->ntuples.vcnt > 1){
                appendStringInfo(str,
                                 "Rows in:  Avg %.1f rows x %d workers."
                                 "  Max/Last(%s/%s) %.0f/%.0f rows",
                                 ntuples_avg,
								ns->ntuples.vcnt,
								segbuf,segbufLast,
								ns->ntuples.vmax,ns->ntuples.vlast);
                containMaxRowAndLast = true;
            }
            else
                appendStringInfo(str,
                                 "Rows in:  %s%.0f rows%s",
								 noRowRequested,
                                 ns->ntuples.vmax,
								segbufWithParenthese);
            break;
        case T_MotionState:
            if (ns->ntuples.vcnt > 1){
                appendStringInfo(str,
                                 "Rows out:  Avg %.1f rows x %d workers"
                                 " at destination.  Max/Last(%s/%s) %.0f/%.0f rows",
                                 ntuples_avg,
								ns->ntuples.vcnt,
								segbuf,segbufLast,
								ns->ntuples.vmax,ns->ntuples.vlast);
                containMaxRowAndLast = true;
            }
            else
                appendStringInfo(str,
                                 "Rows out:  %s%.0f rows at destination%s",
								 noRowRequested,
                                 ns->ntuples.vmax,
								segbufWithParenthese);
            break;
        default:
            if (ns->ntuples.vcnt > 1){
                appendStringInfo(str,
                                 "Rows out:  Avg %.1f rows x %d workers."
                                 "  Max/Last(%s/%s) %.0f/%.0f rows",
                                 ntuples_avg,
                                 ns->ntuples.vcnt,
								segbuf,segbufLast,
                                 ns->ntuples.vmax,ns->ntuples.vlast);
                containMaxRowAndLast = true;
            }
            else
                appendStringInfo(str,
                                 "Rows out:  %s%.0f rows%s",
								 noRowRequested,
                                 ns->ntuples.vmax,
								segbufWithParenthese);
    }

    if( containMaxRowAndLast ){
    		/* Time from this worker's first InstrStartNode() to its first result row */
    		cdbexplain_formatPairSeconds(firstbuf, sizeof(firstbuf), instr->startup, instr->startupLast);

    		/* Time from this worker's first InstrStartNode() to end of its results */
    		cdbexplain_formatPairSeconds(totalbuf, sizeof(totalbuf), instr->total, instr->totalLast);
    }
    else
    {
    		/* Time from this worker's first InstrStartNode() to its first result row */
    	    	cdbexplain_formatSeconds(firstbuf, sizeof(firstbuf), instr->startup);

    	    /* Time from this worker's first InstrStartNode() to end of its results */
    	    cdbexplain_formatSeconds(totalbuf, sizeof(totalbuf), instr->total);
    }

    /*
     * Show elapsed time just once if they are the same or if we don't have
     * any valid elapsed time for first tuple.
     */
    if ((instr->ntuples > 0) && (strcmp(firstbuf, totalbuf) != 0))
        appendStringInfo(str,
                         " with %s to first%s, %s to end",
						 firstbuf,
                         s_row,
						 totalbuf);
    else
        appendStringInfo(str,
                         " with %s to end",
						 totalbuf);

    /* Number of rescans */
    if (instr->nloops > 1)
        appendStringInfo(str, " of %.0f scans", instr->nloops);
	
	/* Time from start of query on qDisp to this worker's first result row */
	if (!(INSTR_TIME_IS_ZERO(instr->firststart)) && !(INSTR_TIME_IS_ZERO(instr->firststartLast)))
	{
		INSTR_TIME_SET_ZERO(timediff);
		INSTR_TIME_ACCUM_DIFF(timediff, instr->firststart, ctx->querystarttime);
		INSTR_TIME_SET_ZERO(timediffLast);
		INSTR_TIME_ACCUM_DIFF(timediffLast, instr->firststartLast, ctx->querystarttime);
		cdbexplain_formatPairSeconds(startbuf, sizeof(startbuf), INSTR_TIME_GET_DOUBLE(timediff), INSTR_TIME_GET_DOUBLE(timediffLast));
		appendStringInfo(str, ", start offset by %s", startbuf);
	}

    appendStringInfoString(str, ".\n");

	if ((EXPLAIN_MEMORY_VERBOSITY_DETAIL <= explain_memory_verbosity)
			&& planstate->type == T_MotionState)
    {
		Motion	   *pMotion = (Motion *) planstate->plan;
		int curSliceId = pMotion->motionID;

    	for (int iWorker = 0; iWorker < ctx->slices[curSliceId].nworker; iWorker++)
    	{
    	    appendStringInfoFill(str, 2*indent, ' ');
    		appendStringInfo(str, "slice %d, seg %d\n", curSliceId, iWorker);

    		MemoryAccounting_ToString(&(ctx->slices[curSliceId].memoryTreeRoots[iWorker]->memoryAccount), str, indent + 1);
    	}
    }


    /*
     * Executor memory used by this individual node, if it allocates from a
     * memory context of its own instead of sharing the per-query context.
     */
    if (ns->execmemused.vcnt > 0)
    {
        appendStringInfoFill(str, 2*indent, ' ');
        cdbexplain_formatMemory(maxbuf, sizeof(maxbuf), ns->execmemused.vmax);
        if (ns->execmemused.vcnt == 1)
            appendStringInfo(str,
                             "Executor memory:  %s.\n",
                             maxbuf);
        else
        {
            cdbexplain_formatSeg(segbuf, sizeof(segbuf), ns->execmemused.imax, ns->ninst,ns->ntuples.hostnamemax);
            cdbexplain_formatMemory(avgbuf, sizeof(avgbuf), cdbexplain_agg_avg(&ns->execmemused));
            appendStringInfo(str,
                             "Executor memory:  %s avg, %s max%s.\n",
                             avgbuf,
                             maxbuf,
                             segbuf);
        }
    }

    /*
     * Actual work_mem used.
     */
    if (ns->workmemused.vcnt > 0)
    {
        appendStringInfoFill(str, 2*indent, ' ');
        cdbexplain_formatMemory(maxbuf, sizeof(maxbuf), ns->workmemused.vmax);
        if (ns->workmemused.vcnt == 1)
            appendStringInfo(str,
                             "Work_mem used:  %s.",
                             maxbuf);
        else
        {
            cdbexplain_formatSeg(segbuf, sizeof(segbuf), ns->workmemused.imax, ns->ninst,ns->ntuples.hostnamemax);
            cdbexplain_formatMemory(avgbuf, sizeof(avgbuf), cdbexplain_agg_avg(&ns->workmemused));
            appendStringInfo(str,
                             "Work_mem used:  %s avg, %s max%s.",
                             avgbuf,
                             maxbuf,
                             segbuf);
        }
    	/*
    	 * Total number of segments in which this node reuses cached or creates workfiles.
    	 */
    	if (nodeSupportWorkfileCaching(planstate))
    	{
    		appendStringInfo(str,
    						 " Workfile: (%d spilling, %d reused)",
    						 ns->totalWorkfileCreated.vcnt,
    						 ns->totalWorkfileReused.vcnt);
    	}

    	appendStringInfo(str,"\n");

    }

    if (EXPLAIN_MEMORY_VERBOSITY_SUPPRESS < explain_memory_verbosity)
    {
		/*
		 * Memory account balance without overhead
		 */
		appendStringInfoFill(str, 2*indent, ' ');
		cdbexplain_formatMemory(maxbuf, sizeof(maxbuf), ns->peakMemBalance.vmax);
		if (ns->peakMemBalance.vcnt == 1)
		{
			appendStringInfo(str,
							 "Memory:  %s.\n",
							 maxbuf);
		}
		else
		{
			cdbexplain_formatSeg(segbuf, sizeof(segbuf), ns->peakMemBalance.imax, ns->ninst,ns->ntuples.hostnamemax);
			cdbexplain_formatMemory(avgbuf, sizeof(avgbuf), cdbexplain_agg_avg(&ns->peakMemBalance));
			appendStringInfo(str,
							 "Memory:  %s avg, %s max%s.\n",
							 avgbuf,
							 maxbuf,
							 segbuf);
		}
    }

    /*
     * What value of work_mem would suffice to eliminate workfile I/O?
     */
    if (ns->workmemwanted.vcnt > 0)
    {
        appendStringInfoFill(str, 2*indent, ' ');
        cdbexplain_formatMemory(maxbuf, sizeof(maxbuf), ns->workmemwanted.vmax);
        if (ns->ninst == 1)
        {
            appendStringInfo(str,
                "Work_mem wanted: %s to lessen workfile I/O.\n",
                maxbuf);
        }
        else
        {
            cdbexplain_formatMemory(avgbuf, sizeof(avgbuf), cdbexplain_agg_avg(&ns->workmemwanted));
            cdbexplain_formatSeg(segbuf, sizeof(segbuf), ns->workmemwanted.imax, ns->ninst,ns->ntuples.hostnamemax);
            appendStringInfo(str,
                             "Work_mem wanted: %s avg, %s max%s"
                             " to lessen workfile I/O affecting %d workers.\n",
                             avgbuf,
                             maxbuf,
                             segbuf,
                             ns->workmemwanted.vcnt);
        }
    }
	
    /*
     * Print number of partitioned tables scanned for dynamic scans.
     */
    if (0 <= ns->totalPartTableScanned.vcnt && (T_BitmapTableScanState == planstate->type
    	|| T_DynamicTableScanState == planstate->type
    	|| T_DynamicIndexScanState == planstate->type))
    {
    	double nPartTableScanned_avg = cdbexplain_agg_avg(&ns->totalPartTableScanned);
    	if (0 == nPartTableScanned_avg)
    	{
    		bool displayPartitionScanned = true;
    		if (T_BitmapTableScanState == planstate->type)
    		{
    			ScanState *scanState = (ScanState *) planstate;
    			if (!isDynamicScan((Scan *)scanState->ps.plan))
    			{
    				displayPartitionScanned = false;
    			}
    		}

    		if (displayPartitionScanned)
    		{
    			int numTotalLeafParts = cdbexplain_countLeafPartTables(planstate);
    			appendStringInfoFill(str, 2 * indent, ' ');
    			appendStringInfo(str,
    					"Partitions scanned:  0 (out of %d).\n",
    					numTotalLeafParts);
    		}
    	}
    	else
    	{
    		cdbexplain_formatSeg(segbuf, sizeof(segbuf), ns->totalPartTableScanned.imax, ns->ninst,ns->ntuples.hostnamemax);
    		int numTotalLeafParts = cdbexplain_countLeafPartTables(planstate);
    		appendStringInfoFill(str, 2 * indent, ' ');

    		/* only 1 segment scans partitions */
    		if (1 == ns->totalPartTableScanned.vcnt)
    		{
    			/* rescan */
    			if (1 < instr->nloops)
    			{
    				double totalPartTableScannedPerRescan = ns->totalPartTableScanned.vmax / instr->nloops;
    				appendStringInfo(str,
    						"Partitions scanned:  %.0f (out of %d) %s of %.0f scans.\n",
    						totalPartTableScannedPerRescan,
    						numTotalLeafParts,
    						segbuf,
    						instr->nloops);
    			}
    			else
    			{
    				appendStringInfo(str,
    						"Partitions scanned:  %.0f (out of %d) %s.\n",
    						ns->totalPartTableScanned.vmax,
    						numTotalLeafParts,
    						segbuf);
    			}
    		}
    		else
    		{
    			/* rescan */
    			if (1 < instr->nloops)
    			{
    				double totalPartTableScannedPerRescan = nPartTableScanned_avg / instr->nloops;
    				double maxPartTableScannedPerRescan = ns->totalPartTableScanned.vmax / instr->nloops;
    				appendStringInfo(str,
    						"Partitions scanned:  Avg %.1f (out of %d) x %d workers of %.0f scans."
    						"  Max %.0f parts%s.\n",
    						totalPartTableScannedPerRescan,
    						numTotalLeafParts,
    						ns->totalPartTableScanned.vcnt,
    						instr->nloops,
    						maxPartTableScannedPerRescan,
    						segbuf
    						);
    			}
    			else
    			{
    				appendStringInfo(str,
    						"Partitions scanned:  Avg %.1f (out of %d) x %d workers."
    						"  Max %.0f parts%s.\n",
    						nPartTableScanned_avg,
    						numTotalLeafParts,
    						ns->totalPartTableScanned.vcnt,
    						ns->totalPartTableScanned.vmax,
    						segbuf);
    			}
    		}
    	}
    }
	
    /*
     * Extra message text.
     */
    for (i = 0; i < ns->ninst; i++)
    {
        CdbExplain_StatInst    *nsi = &ns->insts[i];

        if (nsi->bnotes < nsi->enotes)
        {
            cdbexplain_formatExtraText(str,
                                       indent,
                                       (ns->ninst == 1) ? -1
                                                        : ns->segindex0 + i,
									   nsi->workfileReused,
                                       ctx->extratextbuf.data + nsi->bnotes,
                                       nsi->enotes - nsi->bnotes);
        }
    }

    /*
     * Dump stats for all workers.
     */
	if (gp_enable_explain_allstat &&
        ns->segindex0 >= 0 &&
        ns->ninst > 0)
	{
		/* create a header for all stats: separate each individual
		 * stat by an underscore, separate the grouped stats for each
		 * node by a slash
		 */
        appendStringInfoFill(str, 2*indent, ' ');
		appendStringInfoString(str,
                               "allstat: "
        /*	 "seg_starttime_firststart_counter_firsttuple_startup_total_ntuples_nloops" */
						       "seg_firststart_total_ntuples");

        for (i = 0; i < ns->ninst; i++)
		{
            CdbExplain_StatInst    *nsi = &ns->insts[i];

	        if (INSTR_TIME_IS_ZERO(nsi->firststart))
	        {
                continue;
	        }

	        /* Time from start of query on qDisp to worker's first result row */
			INSTR_TIME_SET_ZERO(timediff);
	        INSTR_TIME_ACCUM_DIFF(timediff, nsi->firststart, ctx->querystarttime);
	        cdbexplain_formatSeconds(startbuf, sizeof(startbuf), INSTR_TIME_GET_DOUBLE(timediff));
			cdbexplain_formatSeconds(totalbuf, sizeof(totalbuf), nsi->total);

			appendStringInfo(str,
                             "/seg%d_%s_%s_%.0f",
                             ns->segindex0 + i,
							 startbuf,
							 totalbuf,
                             nsi->ntuples);
		}
		appendStringInfoString(str, "//end\n");
	}
}                               /* cdbexplain_showExecStats */


/*
 * cdbexplain_showExecStatsEnd
 *    Called by qDisp process to format the overall statistics for a query
 *    into the caller's buffer.
 *
 * 'ctx' is the CdbExplain_ShowStatCtx object which was created by a call to
 *      cdbexplain_showExecStatsBegin() and contains statistics which have
 *      been accumulated over a series of calls to cdbexplain_showExecStats().
 *      Invalid on return (it is freed).
 * 'str' is the output buffer.
 *
 * This doesn't free the CdbExplain_ShowStatCtx object or buffers, because
 * shortly afterwards the caller is expected to destroy the 'explaincxt'
 * MemoryContext which was passed to cdbexplain_showExecStatsBegin(), thus
 * freeing all at once.
 */
void
cdbexplain_showExecStatsEnd(struct PlannedStmt *stmt,
							struct CdbExplain_ShowStatCtx  *showstatctx,
							struct StringInfoData          *str,
							struct EState                  *estate)
{
    Slice  *slice;
    int     sliceIndex;
    int     startlen;
    int     tab;
    int     flag;

    char    avgbuf[50];
    char    maxbuf[50];
    char    segbuf[50];

    /*
     * Summary by slice
     */
    if (showstatctx->nslice > 0)
        appendStringInfoString(str, "Slice statistics:\n");

    for (sliceIndex = 0; sliceIndex < showstatctx->nslice; sliceIndex++)
    {
        CdbExplain_SliceSummary    *ss = &showstatctx->slices[sliceIndex];
        CdbExplain_DispatchSummary *ds = &ss->dispatchSummary;

        startlen = str->len;
        appendStringInfo(str, "  (slice%d) ", sliceIndex);
        if (sliceIndex < 10)
            appendStringInfoChar(str, ' ');

        flag = str->len;
        appendStringInfoString(str, "  ");
        tab = str->len - startlen;

        /* Worker counts */
        slice = getCurrentSlice(estate, sliceIndex);
        if (slice &&
            slice->numGangMembersToBeActive > 0 &&
            slice->numGangMembersToBeActive != ss->dispatchSummary.nOk)
        {
            int nNotDispatched = slice->numGangMembersToBeActive - ds->nResult + ds->nNotDispatched;

            str->data[flag] = (ss->dispatchSummary.nError > 0) ? 'X' : '_';

            appendStringInfoString(str, "Workers:");
            if (ds->nError == 1)
            {
                appendStringInfo(str,
                                 " %d error;",
                                 ds->nError);
            }
            else if (ds->nError > 1)
            {
                appendStringInfo(str,
                                 " %d errors;",
                                 ds->nError);
            }
            if (ds->nCanceled > 0)
            {
                appendStringInfo(str,
                                 " %d canceled;",
                                 ds->nCanceled);
            }
            if (nNotDispatched > 0)
            {
                appendStringInfo(str,
                                 " %d not dispatched;",
                                 nNotDispatched);
            }
            if (ds->nIgnorableError > 0)
            {
                appendStringInfo(str,
                                 " %d aborted;",
                                 ds->nIgnorableError);
            }
            if (ds->nOk > 0)
            {
                appendStringInfo(str,
                                 " %d ok;",
                                 ds->nOk);
            }
            str->len--;
            appendStringInfoString(str, ".  ");
        }

        /* Executor memory high-water mark */
        cdbexplain_formatMemory(maxbuf, sizeof(maxbuf), ss->peakmemused.vmax);
        if (ss->peakmemused.vcnt == 1)
        {
            const char *seg = segbuf;

            if (ss->peakmemused.imax >= 0)
            {
                cdbexplain_formatSeg(segbuf, sizeof(segbuf), ss->peakmemused.imax, 999,ss->peakmemused.hostnamemax);
            }
            else if (slice &&
                     slice->gangSize > 0)
            {
                seg = " (entry db)";
            }
            else
            {
                seg = "";
            }
            appendStringInfo(str,
                             "Executor memory: %s%s.",
                             maxbuf,
                             seg);
        }
        else if (ss->peakmemused.vcnt > 1)
        {
            cdbexplain_formatMemory(avgbuf, sizeof(avgbuf), cdbexplain_agg_avg(&ss->peakmemused));
            cdbexplain_formatSeg(segbuf, sizeof(segbuf), ss->peakmemused.imax, ss->nworker,ss->peakmemused.hostnamemax);
            appendStringInfo(str,
                             "Executor memory: %s avg x %d workers, %s max%s.",
                             avgbuf,
                             ss->peakmemused.vcnt,
                             maxbuf,
                             segbuf);
        }

        if (EXPLAIN_MEMORY_VERBOSITY_SUPPRESS < explain_memory_verbosity)
        {
			/* Memory accounting global peak memory usage */
			cdbexplain_formatMemory(maxbuf, sizeof(maxbuf), ss->memory_accounting_global_peak.vmax);
			if (ss->memory_accounting_global_peak.vcnt == 1)
			{
				const char *seg = segbuf;

				if (ss->memory_accounting_global_peak.imax >= 0)
				{
					cdbexplain_formatSeg(segbuf, sizeof(segbuf), ss->memory_accounting_global_peak.imax, 999,ss->memory_accounting_global_peak.hostnamemax);
				}
				else if (slice &&
						 slice->gangSize > 0)
				{
					seg = " (entry db)";
				}
				else
				{
					seg = "";
				}
				appendStringInfo(str,
								 "  Peak memory: %s%s.",
								 maxbuf,
								 seg);
			}
			else if (ss->memory_accounting_global_peak.vcnt > 1)
			{
				cdbexplain_formatMemory(avgbuf, sizeof(avgbuf), cdbexplain_agg_avg(&ss->memory_accounting_global_peak));
				cdbexplain_formatSeg(segbuf, sizeof(segbuf), ss->memory_accounting_global_peak.imax, ss->nworker,ss->memory_accounting_global_peak.hostnamemax);
				appendStringInfo(str,
								 "  Peak memory: %s avg x %d workers, %s max%s.",
								 avgbuf,
								 ss->memory_accounting_global_peak.vcnt,
								 maxbuf,
								 segbuf);
			}

			/* Vmem reserved by QEs */
			cdbexplain_formatMemory(maxbuf, sizeof(maxbuf), ss->vmem_reserved.vmax);
			if (ss->vmem_reserved.vcnt == 1)
			{
				const char *seg = segbuf;

				if (ss->vmem_reserved.imax >= 0)
				{
					cdbexplain_formatSeg(segbuf, sizeof(segbuf), ss->vmem_reserved.imax, 999,ss->vmem_reserved.hostnamemax);
				}
				else if (slice &&
						 slice->gangSize > 0)
				{
					seg = " (entry db)";
				}
				else
				{
					seg = "";
				}
				appendStringInfo(str,
								 "  Vmem reserved: %s%s.",
								 maxbuf,
								 seg);
			}
			else if (ss->vmem_reserved.vcnt > 1)
			{
				cdbexplain_formatMemory(avgbuf, sizeof(avgbuf), cdbexplain_agg_avg(&ss->vmem_reserved));
				cdbexplain_formatSeg(segbuf, sizeof(segbuf), ss->vmem_reserved.imax, ss->nworker,ss->vmem_reserved.hostnamemax);
				appendStringInfo(str,
								 "  Vmem reserved: %s avg x %d workers, %s max%s.",
								 avgbuf,
								 ss->vmem_reserved.vcnt,
								 maxbuf,
								 segbuf);
			}
        }

        /* Work_mem used/wanted (max over all nodes and workers of slice) */
        if (ss->workmemused_max + ss->workmemwanted_max > 0)
        {
            cdbexplain_formatMemory(maxbuf, sizeof(maxbuf), ss->workmemused_max);
            appendStringInfo(str, "  Work_mem: %s max", maxbuf);
            if (ss->workmemwanted_max > 0)
            {
                str->data[flag] = '*';  /* draw attention to this slice */
                cdbexplain_formatMemory(maxbuf, sizeof(maxbuf), ss->workmemwanted_max);
                appendStringInfo(str, ", %s wanted", maxbuf);
            }
            appendStringInfoChar(str, '.');
       }

       appendStringInfoChar(str, '\n');
    }
    
    appendStringInfoString(str, "Statement statistics:\n");
    appendStringInfo(str, "  Memory used: %.0fK bytes", ceil((double) stmt->query_mem / 1024.0));
        
    if (showstatctx->workmemwanted_max > 0)
	{
		appendStringInfo(str, "\n  Memory wanted: %.0fK bytes",
				(double) StatementMemForNoSpillKB(stmt, (uint64) showstatctx->workmemwanted_max / 1024L));
	}

	appendStringInfoChar(str, '\n');

}                               /* cdbexplain_showExecStatsEnd */

static int
cdbexplain_countLeafPartTables(PlanState *planstate)
{
	Assert (IsA(planstate, DynamicTableScanState) || IsA(planstate, DynamicIndexScanState)
	        || IsA(planstate, BitmapTableScanState));
	Scan *scan = (Scan *) planstate->plan;

	Oid root_oid = getrelid(scan->scanrelid, planstate->state->es_range_table);
	return countLeafPartTables(root_oid);
}
