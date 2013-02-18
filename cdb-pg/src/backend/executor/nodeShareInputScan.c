/*-------------------------------------------------------------------------
 *
 * nodeShareInputScan.c
 *
 * Portions Copyright (c) 2007-2008, Greenplum inc
 * Portions Copyright (c) 1996-2008, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *-------------------------------------------------------------------------
 */

/*
 * INTERFACE ROUNTINES
 * 	ExecCountSlotsShareInputScan
 *	ExecInitShareInputScan
 * 	ExecShareInputScan
 * 	ExecEndShareInputScan
 * 	ExecShareInputMarkPosScan
 * 	ExecShareInputRestrPosScan
 * 	ExecShareInputReScanScanv
 */

#include "postgres.h"

#include "cdb/cdbvars.h"
#include "executor/executor.h"
#include "executor/nodeShareInputScan.h"

#include "utils/tuplestorenew.h"
#include "miscadmin.h"

#include "utils/debugbreak.h"
#include "utils/tuplesort.h"
#include "postmaster/primary_mirror_mode.h"

static TupleTableSlot *ShareInputNext(ShareInputScanState *node);

/* ------------------------------------------------------------------
 * 	ExecShareInputScan 
 * ------------------------------------------------------------------
 */
TupleTableSlot *ExecShareInputScan(ShareInputScanState *node)
{
	return ExecScan(&node->ss, (ExecScanAccessMtd) ShareInputNext);
}

/*
 * init_tuplestore_state
 *    Initialize the tuplestore state for the Shared node if the state
 *    is not initialized.
 */
static void
init_tuplestore_state(ShareInputScanState *node)
{
	Assert(node->ts_state == NULL);
	
	EState *estate = node->ss.ps.state;
	ShareInputScan *sisc = (ShareInputScan *)node->ss.ps.plan;
	ShareNodeEntry *snEntry = ExecGetShareNodeEntry(estate, sisc->share_id, false);
	PlanState *snState = NULL;

	ShareType share_type = sisc->share_type;

	if(snEntry)
	{
		snState = (PlanState *) snEntry->shareState;
		if(snState)
		{
			ExecProcNode(snState);
		}
		
		else
		{
			Assert(share_type == SHARE_MATERIAL_XSLICE || share_type == SHARE_SORT_XSLICE);
		}
	}

	if(share_type == SHARE_MATERIAL_XSLICE)
	{
		char rwfile_prefix[100];
		shareinput_create_bufname_prefix(rwfile_prefix, sizeof(rwfile_prefix), sisc->share_id);
	
		node->ts_state = (void *) ntuplestore_create_readerwriter(rwfile_prefix, 0, false);
		node->ts_pos = (void *) ntuplestore_create_accessor((NTupleStore *) node->ts_state, false);
		ntuplestore_acc_seek_bof((NTupleStoreAccessor *)node->ts_pos);
	}
	else if(share_type == SHARE_MATERIAL)
	{
		node->ts_state = ((MaterialState *)snState)->ts_state;
		node->ts_pos = (void *) ntuplestore_create_accessor((NTupleStore *) node->ts_state, false);
		ntuplestore_acc_seek_bof((NTupleStoreAccessor *)node->ts_pos);
	}
	else if(share_type == SHARE_SORT_XSLICE)
	{
		char rwfile_prefix[100];
		shareinput_create_bufname_prefix(rwfile_prefix, sizeof(rwfile_prefix), sisc->share_id);
			
		if(gp_enable_mk_sort)
		{
			node->ts_state = tuplesort_begin_heap_file_readerwriter_mk(
				rwfile_prefix, false,
				NULL, 0, NULL, NULL, PlanStateOperatorMemKB((PlanState *) node), true);

			tuplesort_begin_pos_mk((Tuplesortstate_mk *) node->ts_state, (TuplesortPos_mk **)(&node->ts_pos));
			tuplesort_rescan_pos_mk((Tuplesortstate_mk *) node->ts_state, (TuplesortPos_mk *)node->ts_pos); 
		}
		else
		{
			node->ts_state = tuplesort_begin_heap_file_readerwriter(
				rwfile_prefix, false,
				NULL, 0, NULL, NULL, PlanStateOperatorMemKB((PlanState *) node), true);

			tuplesort_begin_pos((Tuplesortstate *) node->ts_state, (TuplesortPos **)(&node->ts_pos));
			tuplesort_rescan_pos((Tuplesortstate *) node->ts_state, (TuplesortPos *)node->ts_pos); 
		}
	}
	else 
	{
		Assert(sisc->share_type == SHARE_SORT);
		Assert(snState != NULL);

		if(gp_enable_mk_sort)
		{
			node->ts_state = ((SortState *)snState)->tuplesortstate;
			tuplesort_begin_pos_mk((Tuplesortstate_mk *) node->ts_state, (TuplesortPos_mk **)(&node->ts_pos));
			tuplesort_rescan_pos_mk((Tuplesortstate_mk *) node->ts_state, (TuplesortPos_mk *)node->ts_pos);
		}
		else
		{
			node->ts_state = ((SortState *)snState)->tuplesortstate;
			tuplesort_begin_pos((Tuplesortstate *) node->ts_state, (TuplesortPos **)(&node->ts_pos));
			tuplesort_rescan_pos((Tuplesortstate *) node->ts_state, (TuplesortPos *)node->ts_pos);
		}
	}
}


/* ------------------------------------------------------------------
 * ShareInputNext
 * 	Retrieve a tuple from the ShareInputScan
 * ------------------------------------------------------------------
 */
TupleTableSlot * 
ShareInputNext(ShareInputScanState *node)
{
	EState *estate;
	ScanDirection dir;
	bool forward;
	TupleTableSlot *slot;

	ShareInputScan * sisc = (ShareInputScan *) node->ss.ps.plan;

	ShareType share_type = sisc->share_type;

	/* 
	 * get state info from node
	 */
	estate = node->ss.ps.state;
	dir = estate->es_direction;
	forward = ScanDirectionIsForward(dir);


	/* if first time call, need to initialize the tuplestore state.  */
	if(node->ts_state == NULL)
	{
		init_tuplestore_state(node);
	}

	slot = node->ss.ps.ps_ResultTupleSlot;

	while(1)
	{
		bool gotOK = false;

		if(share_type == SHARE_MATERIAL || share_type == SHARE_MATERIAL_XSLICE) 
		{
			ntuplestore_acc_advance((NTupleStoreAccessor *) node->ts_pos, forward ? 1 : -1);
			gotOK = ntuplestore_acc_current_tupleslot((NTupleStoreAccessor *) node->ts_pos, slot);
		}
		else
		{
			if(gp_enable_mk_sort)
				gotOK = tuplesort_gettupleslot_pos_mk((Tuplesortstate_mk *)node->ts_state, (TuplesortPos_mk *)node->ts_pos, forward, slot);
			else
				gotOK = tuplesort_gettupleslot_pos((Tuplesortstate *)node->ts_state, (TuplesortPos *)node->ts_pos, forward, slot);
		}

		if(!gotOK)
			return NULL;

		Gpmon_M_Incr_Rows_Out(GpmonPktFromShareInputState(node)); 
		CheckSendPlanStateGpmonPkt(&node->ss.ps);

		return slot;
	}

	Assert(!"should not be here");
	return NULL;
}

/*  ------------------------------------------------------------------
 * 	ExecInitShareInputScan 
 * ------------------------------------------------------------------
 */
ShareInputScanState *
ExecInitShareInputScan(ShareInputScan *node, EState *estate, int eflags)
{
	ShareInputScanState *sisstate;
	Plan *outerPlan;
	TupleDesc tupDesc;

	Assert(innerPlan(node) == NULL);
	
	/* create state data structure */
	sisstate = makeNode(ShareInputScanState);
	sisstate->ss.ps.plan = (Plan *) node;
	sisstate->ss.ps.state = estate;
	
	sisstate->ts_state = NULL;
	sisstate->ts_pos = NULL;
	sisstate->ts_markpos = NULL;

	sisstate->share_lk_ctxt = NULL;

	/* 
	 * init child node.  
	 * if outerPlan is NULL, this is no-op (so that the ShareInput node will be 
	 * only init-ed once).
	 */
	outerPlan = outerPlan(node);
	outerPlanState(sisstate) = ExecInitNode(outerPlan, estate, eflags);

	sisstate->ss.ps.targetlist = (List *) 
		ExecInitExpr((Expr *) node->plan.targetlist, (PlanState *) sisstate);
	Assert(node->plan.qual == NULL);
	sisstate->ss.ps.qual = NULL;

	/* Misc initialization 
	 * 
	 * Create expression context 
	 */
	ExecAssignExprContext(estate, &sisstate->ss.ps);

	/* tuple table init */
	ExecInitResultTupleSlot(estate, &sisstate->ss.ps);
	sisstate->ss.ss_ScanTupleSlot = ExecInitExtraTupleSlot(estate);

	/* 
	 * init tuple type.
	 */
	ExecAssignResultTypeFromTL(&sisstate->ss.ps);

	{
		bool hasoid;
		if (!ExecContextForcesOids(&sisstate->ss.ps, &hasoid))
			hasoid = false;

		tupDesc = ExecTypeFromTL(node->plan.targetlist, hasoid);
	}
		
	ExecAssignScanType(&sisstate->ss, tupDesc);

	sisstate->ss.ps.ps_ProjInfo = NULL;
	
	initGpmonPktForShareInputScan((Plan *)node, &sisstate->ss.ps.gpmon_pkt, estate);
	
	return sisstate;
}

void
ExecSliceDependencyShareInputScan(ShareInputScanState *node)
{
	ShareInputScan * sisc = (ShareInputScan *) node->ss.ps.plan;

	elog(DEBUG1, "SISC exec dependency on slice %d, driver_slice is %d", currentSliceId, sisc->driver_slice);

	if(sisc->driver_slice >= 0 && sisc->driver_slice == currentSliceId)
		node->share_lk_ctxt = shareinput_reader_waitready(sisc->share_id);
}

/* ------------------------------------------------------------------
 * 	ExecCountSlotsShareInputScan 
 * ------------------------------------------------------------------
 */
int 
ExecCountSlotsShareInputScan(ShareInputScan* node)
{
#define SHAREINPUT_NSLOTS 2
	return ExecCountSlotsNode(outerPlan((Plan *) node)) 
		+ SHAREINPUT_NSLOTS;
}

/* ------------------------------------------------------------------
 * 	ExecEndShareInputScan
 * ------------------------------------------------------------------
 */
void ExecEndShareInputScan(ShareInputScanState *node)
{
	ShareInputScan * sisc = (ShareInputScan *) node->ss.ps.plan;

	/* clean up tuple table */
	ExecClearTuple(node->ss.ss_ScanTupleSlot);
	ExecClearTuple(node->ss.ps.ps_ResultTupleSlot);

	/* 
	 * no need to call tuplestore end.  Underlying ShareInput will take
	 * care of releasing tuplestore resources 
	 */
	/*
	 * XXX Do we need to pfree the tuplestore_state and pos? 
	 * XXX nodeMaterial.c does not, need to find out why
	 */
	
	if(node->share_lk_ctxt)
		shareinput_reader_notifydone(node->share_lk_ctxt, sisc->share_id);

	if(sisc->share_type == SHARE_MATERIAL || sisc->share_type == SHARE_MATERIAL_XSLICE) 
	{
		if(node->ts_pos != NULL)
			ntuplestore_destroy_accessor((NTupleStoreAccessor *) node->ts_pos);
		if(node->ts_markpos != NULL)
			pfree(node->ts_markpos);

		if(node->ts_state != NULL)
		{
			if(ntuplestore_is_readerwriter_reader((NTupleStore *) node->ts_state))
				ntuplestore_destroy((NTupleStore *) node->ts_state);
		}
	}

	node->ts_state = NULL;
	node->ts_pos = NULL;
	node->ts_markpos = NULL;

	/* 
	 * shutdown subplan.  First scanner of underlying share input will
	 * do the shutdown, all other scanners are no-op because outerPlanState
	 * is NULL
	 */
	ExecEndNode(outerPlanState(node));

	EndPlanStateGpmonPkt(&node->ss.ps);
}

/* ------------------------------------------------------------------
 * 	ExecShareInputMarkPosScan
 * ------------------------------------------------------------------
 */
void ExecShareInputScanMarkPos(ShareInputScanState *node)
{
	ShareInputScan *sisc = (ShareInputScan *) node->ss.ps.plan;
	Assert(node->ts_state); 

	if(sisc->share_type == SHARE_MATERIAL || sisc->share_type == SHARE_MATERIAL_XSLICE)
	{
		Assert(node->ts_pos);

		if(node->ts_markpos == NULL)
			node->ts_markpos = palloc(sizeof(NTupleStorePos));

		ntuplestore_acc_tell((NTupleStoreAccessor *)node->ts_pos, (NTupleStorePos *) node->ts_markpos);
	}
	else if(sisc->share_type == SHARE_SORT || sisc->share_type == SHARE_SORT_XSLICE) 
	{
		if(gp_enable_mk_sort)
			tuplesort_markpos_pos_mk((Tuplesortstate_mk *)node->ts_state, (TuplesortPos_mk *) node->ts_pos);
		else
			tuplesort_markpos_pos((Tuplesortstate *)node->ts_state, (TuplesortPos *) node->ts_pos);
	}
	else
		Assert(!"ExecShareInputScanMarkPos: invalid share type");
}

/* ------------------------------------------------------------------
 * 	ExecShareInputRestrPosScan
 * ------------------------------------------------------------------
 */
void ExecShareInputScanRestrPos(ShareInputScanState *node)
{
	ShareInputScan *sisc = (ShareInputScan *) node->ss.ps.plan;
	Assert(node->ts_state); 

	if(sisc->share_type == SHARE_MATERIAL || sisc->share_type == SHARE_MATERIAL_XSLICE)
	{
		Assert(node->ts_pos && node->ts_markpos);
		ntuplestore_acc_seek((NTupleStoreAccessor *) node->ts_pos, (NTupleStorePos *) node->ts_markpos);
	}
	else if(sisc->share_type == SHARE_SORT || sisc->share_type == SHARE_SORT_XSLICE) 
	{
		if(gp_enable_mk_sort)
			tuplesort_restorepos_pos_mk((Tuplesortstate_mk *) node->ts_state, (TuplesortPos_mk *) node->ts_pos);
		else
			tuplesort_restorepos_pos((Tuplesortstate *) node->ts_state, (TuplesortPos *) node->ts_pos);
	}
	else
		Assert(!"ExecShareInputScanRestrPos: invalid share type");

	Gpmon_M_Incr(GpmonPktFromShareInputState(node), GPMON_SHAREINPUT_RESTOREPOS); 
	CheckSendPlanStateGpmonPkt(&node->ss.ps);
}

/* ------------------------------------------------------------------
 * 	ExecShareInputScanReScan
 * ------------------------------------------------------------------
 */
void ExecShareInputScanReScan(ShareInputScanState *node, ExprContext *exprCtxt)
{
	/* if first time call, need to initialize the tuplestore state */
	if(node->ts_state == NULL)
	{
		init_tuplestore_state(node);
	}

	ShareInputScan *sisc = (ShareInputScan *) node->ss.ps.plan;

	ExecClearTuple(node->ss.ps.ps_ResultTupleSlot);
	if(sisc->share_type == SHARE_MATERIAL || sisc->share_type == SHARE_MATERIAL_XSLICE)
	{
		Assert(node->ts_pos);
		ntuplestore_acc_seek_bof((NTupleStoreAccessor *) node->ts_pos);
	}
	else if (sisc->share_type == SHARE_SORT || sisc->share_type == SHARE_SORT_XSLICE)
	{
		if(gp_enable_mk_sort)
			tuplesort_rescan_pos_mk((Tuplesortstate_mk *)node->ts_state, (TuplesortPos_mk *) node->ts_pos);
		else
			tuplesort_rescan_pos((Tuplesortstate *)node->ts_state, (TuplesortPos *) node->ts_pos);
	}
	else
		Assert(!"ExecShareInputScanReScan: invalid share type ");

	Gpmon_M_Incr(GpmonPktFromShareInputState(node), GPMON_SHAREINPUT_RESCAN); 
	CheckSendPlanStateGpmonPkt(&node->ss.ps);
}

/*************************************************************************
 * XXX 
 * we need some IPC mechanism for shareinptu_read_wait/writer_notify.  Semaphore is
 * the first thing come to mind but it turns out postgres is very picky about
 * how to use semaphore and we do not want to mess up with it.
 *
 * Here we used FIFO (named pipe).  mkfifo is Posix.1 and should be available on any
 * reasonable Unix like system.
 *
 * When we open fifo, we open it with O_RDWR.  So the fifo has both reader and writer.
 * That also means, for write, it will not block, but reader will until writer writes
 * something.
 *
 * At first, I used postgres File to manage the FIFO.  It turns out this is not
 * correct because when postgres run out of file descriptors, it will try to close
 * some file descriptors using an LRU algorithm.  Later when the File is used again,
 * postgres will reopen it. The FIFO here is used for synchronization so it is simply 
 * wrong.  Here we use the file descriptor directly, and use a XCallBack to cleanup
 * the resource at the end of transaction (commit or abort).
 * 
 * XXX However, it is always better to have this kind of stuff abstracted out
 * by the system.  
 **************************************************************************/

#include "fcntl.h"
#include "unistd.h"
#include "sys/types.h"
#include "sys/stat.h"
#include "storage/fd.h"
#include "cdb/cdbselect.h"

void shareinput_create_bufname_prefix(char* p, int size, int share_id)
{
	snprintf(p, size, "%s_SIRW_%d_%d_%d_%d", 
            PG_TEMP_FILE_PREFIX, 
            Gp_segment, gp_session_id, gp_command_count, share_id);
}

/* Here we use the absolute path name as the lock name.  See fd.c 
 * for how the name is created (GP_TEMP_FILE_DIR and make_database_relative).
 */
static void sisc_lockname(char* p, int size, int share_id, const char* name)
{
	if (snprintf(p, size,
			"%s/%s/%s_gpcdb2.sisc_%d_%d_%d_%d_%s",
			getCurrentTempFilePath, PG_TEMP_FILES_DIR, PG_TEMP_FILE_PREFIX, 
			Gp_segment, gp_session_id, gp_command_count, share_id, name
			) > size)
	{
		ereport(ERROR, (errmsg("cannot generate path %s/%s/%s_gpcdb2.sisc_%d_%d_%d_%d_%s",
                        getCurrentTempFilePath, PG_TEMP_FILES_DIR, PG_TEMP_FILE_PREFIX,
                        Gp_segment, gp_session_id, gp_command_count, share_id, name)));
	}
}

typedef struct ShareInput_Lk_Context 
{
	int readyfd;
	int donefd;
	int  zcnt;
	bool del_ready; 
	bool del_done;
	char lkname_ready[MAXPGPATH];
	char lkname_done[MAXPGPATH];
} ShareInput_Lk_Context;

static void shareinput_clean_lk_ctxt(ShareInput_Lk_Context *lk_ctxt)
{
	int err;

	elog(LOG, "shareinput_clean_lk_ctxt cleanup lk ctxt %p", lk_ctxt);

	if(lk_ctxt->readyfd >= 0)
	{
		err = gp_retry_close(lk_ctxt->readyfd);
		insist_log(!err, "shareinput_clean_lk_ctxt cannot close readyfd: %m");

		lk_ctxt->readyfd = -1;
	}

	if(lk_ctxt->donefd >= 0)
	{
		err = gp_retry_close(lk_ctxt->donefd);
		insist_log(!err, "shareinput_clean_lk_ctxt cannot close donefd: %m");

		lk_ctxt->donefd = -1;
	}

	if(lk_ctxt->del_ready && lk_ctxt->lkname_ready[0])
	{
		err = unlink(lk_ctxt->lkname_ready);
		insist_log(!err, "shareinput_clean_lk_ctxt cannot unlink \"%s\": %m", lk_ctxt->lkname_ready);

		lk_ctxt->del_ready = false;
	}

	if(lk_ctxt->del_done && lk_ctxt->lkname_done[0])
	{
		err = unlink(lk_ctxt->lkname_done);
		insist_log(!err, "shareinput_clean_lk_ctxt cannot unline \"%s\": %m", lk_ctxt->lkname_done);

		lk_ctxt->del_done = false;
	}

	gp_free2 (lk_ctxt, sizeof(ShareInput_Lk_Context));
}

static void XCallBack_ShareInput_FIFO(XactEvent ev, void* vp)
{
	ShareInput_Lk_Context *lk_ctxt = (ShareInput_Lk_Context *) vp; 
	shareinput_clean_lk_ctxt(lk_ctxt);
}

static void create_tmp_fifo(const char *fifoname)
{
#ifdef WIN32
	elog(ERROR, "mkfifo not supported on win32");
#else
	int err = mkfifo(fifoname, 0600);
	if(err < 0)
	{
		/* first try may be due to pgsql_tmp dir is not created yet. */
		char tmpdir[MAXPGPATH];
		if (snprintf(tmpdir, MAXPGPATH, "%s/%s", getCurrentTempFilePath, PG_TEMP_FILES_DIR) > MAXPGPATH)
		{
			ereport(ERROR, (errmsg("cannot create dir path %s/%s", getCurrentTempFilePath, PG_TEMP_FILES_DIR)));
		}
		mkdir(tmpdir, S_IRWXU);

		/* then try it again */
		err = mkfifo(fifoname, 0600);

		if(err < 0 && errno != EEXIST)
			elog(ERROR, "could not create temporary fifo \"%s\": %m", fifoname);
	}
#endif
}

/* 
 * As all other read/write in postgres, we may be interrupted so retry is needed.
 */
static int retry_read(int fd, char *buf, int rsize)
{
	int sz;
	Assert(rsize > 0);

read_retry:
	sz = read(fd, buf, rsize);
	if (sz > 0)
		return sz;
	else if(sz == 0 || errno == EINTR)
		goto read_retry;
	else
		elog(ERROR, "could not read from fifo: %m");

	Assert(!"Never be here");
	return 0;
}

static int retry_write(int fd, char *buf, int wsize)
{
	int sz;
	Assert(wsize > 0);

write_retry:
	sz = write(fd, buf, wsize);
	if(sz > 0)
		return sz;
	else if(sz == 0 || errno == EINTR)
		goto write_retry;
	else
		elog(ERROR, "could not write to fifo: %m");

	Assert(!"Never be here");
	return 0;
}

/* 
 * Readiness (a) synchronization.
 *
 * For readiness, the shared node will write xslice of 'a' into the pipe.
 * For each share, there is just one ready writer.  Once sharer starts write
 * it need to write all xslice copies of 'a', even if we are interrupted, that
 * is, we should not call CHECK_FOR_INTERRUPTS.
 *
 * For sharer, it need to check for ready to read (using select), because read 
 * is blocking.  Otherwise if shared is cancelled before write, then we will be
 * blocked here forever.  Once shared has write at least one 'a', it will write
 * all xslice of 'a', so once select succeed, read will eventually succeed.  Once
 * sharer got 'a', it write 'b' back to shared.
 *
 * Done (b and z) synchronization.
 * For done, the shared is the only reader.  sharer will not block for writing, 
 * but shared may block for read, therefore, we much call select before shared 
 * calling read.  Because there is only one shared, nobody can steal char from
 * the pipe, therefore, if select succeed, read will not block forever.
 *
 * One thing to note is that some 'z' may comeback before all 'b' come back.
 * So, need to handle this in notifyready.
 */
void *shareinput_reader_waitready(int share_id)
{
	mpp_fd_set rset;
	struct timeval tval;
	int n;
	char a;
	int rwsize;

	ShareInput_Lk_Context *pctxt = gp_malloc(sizeof(ShareInput_Lk_Context));

	if(!pctxt)
		ereport(ERROR, (errcode(ERRCODE_OUT_OF_MEMORY),
			errmsg("Share input reader failed: out of memory")));

	pctxt->readyfd = -1;
	pctxt->donefd = -1;
	pctxt->zcnt = 0;
	pctxt->del_ready = false;
	pctxt->del_done = false;
	pctxt->lkname_ready[0] = '\0';
	pctxt->lkname_done[0] = '\0';

	RegisterXactCallbackOnce(XCallBack_ShareInput_FIFO, pctxt);

	sisc_lockname(pctxt->lkname_ready, MAXPGPATH, share_id, "ready");
	create_tmp_fifo(pctxt->lkname_ready);
	pctxt->readyfd = open(pctxt->lkname_ready, O_RDWR, 0600); 
	if(pctxt->readyfd < 0)
		elog(ERROR, "could not open fifo \"%s\": %m", pctxt->lkname_ready);
	
	sisc_lockname(pctxt->lkname_done, MAXPGPATH, share_id, "done");
	create_tmp_fifo(pctxt->lkname_done);
	pctxt->donefd = open(pctxt->lkname_done, O_RDWR, 0600);
	if(pctxt->donefd < 0)
		elog(ERROR, "could not open fifo \"%s\": %m", pctxt->lkname_done);

	while(1)
	{
		CHECK_FOR_INTERRUPTS();

		MPP_FD_ZERO(&rset);
		MPP_FD_SET(pctxt->readyfd, &rset);

		tval.tv_sec = 1;
		tval.tv_usec = 0;

		n = select(pctxt->readyfd+1, (fd_set *) &rset, NULL, NULL, &tval);

		if(n==1)
		{
			rwsize = retry_read(pctxt->readyfd, &a, 1);
			Assert(rwsize == 1 && a == 'a');

			elog(DEBUG1, "SISC: Wait ready get a, now write b");
			rwsize = retry_write(pctxt->donefd, "b", 1);
			Assert(rwsize == 1);

			break;
		}
		else if(n==0)
		{
			elog(DEBUG1, "SISC: Wait ready time out once");
		}
		else
		{
			int save_errno = errno;
			elog(LOG, "SISC: Wait ready try again, errno %d ... ", save_errno);
		}
	}
	return (void *) pctxt;
}

void *shareinput_writer_notifyready(int share_id, int xslice)
{
	mpp_fd_set rset;
	struct timeval tval;
	int n;
	char b;
	int rwsize;

	ShareInput_Lk_Context *pctxt = gp_malloc(sizeof(ShareInput_Lk_Context));

	if(!pctxt)
		ereport(ERROR, (errcode(ERRCODE_OUT_OF_MEMORY),
			errmsg("Shareinput Writer failed: out of memory")));

	pctxt->readyfd = -1;
	pctxt->donefd = -1;
	pctxt->zcnt = 0;
	pctxt->del_ready = false;
	pctxt->del_done = false;
	pctxt->lkname_ready[0] = '\0';
	pctxt->lkname_done[0] = '\0';

	RegisterXactCallbackOnce(XCallBack_ShareInput_FIFO, pctxt);

	sisc_lockname(pctxt->lkname_ready, MAXPGPATH, share_id, "ready");
	create_tmp_fifo(pctxt->lkname_ready);
	pctxt->del_ready = true;
	pctxt->readyfd = open(pctxt->lkname_ready, O_RDWR, 0600); 
	if(pctxt->readyfd < 0)
	
		elog(ERROR, "could not open fifo \"%s\": %m", pctxt->lkname_ready);
	sisc_lockname(pctxt->lkname_done, MAXPGPATH, share_id, "done");
	create_tmp_fifo(pctxt->lkname_done);
	pctxt->del_done = true;
	pctxt->donefd = open(pctxt->lkname_done, O_RDWR, 0600);
	if(pctxt->donefd < 0)
		elog(ERROR, "could not open fifo \"%s\": %m", pctxt->lkname_done);

	for(n=0; n<xslice; ++n)
	{
		rwsize = retry_write(pctxt->readyfd, "a", 1);
		Assert(rwsize == 1);
	}
	
	while(xslice > 0)
	{
		CHECK_FOR_INTERRUPTS();

		MPP_FD_ZERO(&rset);
		MPP_FD_SET(pctxt->donefd, &rset);

		tval.tv_sec = 1;
		tval.tv_usec = 0;
		n = select(pctxt->donefd+1, (fd_set *) &rset, NULL, NULL, &tval);

		if(n==1)
		{
			rwsize = retry_read(pctxt->donefd, &b, 1);
			Assert(rwsize == 1);

			if(b == 'z')
				++pctxt->zcnt;
			else
			{
				Assert(b == 'b');
				--xslice;
				elog(DEBUG1, "SISC: notify ready succeed 1, xslice remaining %d", xslice);
			}
		}
		else if(n==0)
		{
			elog(DEBUG1, "SISC: Notify ready time out once ... ");
		}
		else
		{
			int save_errno = errno;
			elog(LOG, "SISC: notify still wait for an answer, errno %d", save_errno);
		}
	}
	return (void *) pctxt;
}

void shareinput_reader_notifydone(void *ctxt, int share_id)
{
	ShareInput_Lk_Context *pctxt = (ShareInput_Lk_Context *) ctxt;
	int rwsize;

	rwsize = retry_write(pctxt->donefd, "z", 1);
	Assert(rwsize == 1);

	shareinput_clean_lk_ctxt(pctxt);
	UnregisterXactCallbackOnce(XCallBack_ShareInput_FIFO, (void *) ctxt);
}

void shareinput_writer_waitdone(void *ctxt, int share_id, int nsharer_xslice)
{
	ShareInput_Lk_Context *pctxt = (ShareInput_Lk_Context *) ctxt;
	mpp_fd_set rset;
	struct timeval tval;
	int n;
	char z;
	int rwsize;

	nsharer_xslice -= pctxt->zcnt;

	while(nsharer_xslice > 0)
	{
		CHECK_FOR_INTERRUPTS();
	
		MPP_FD_ZERO(&rset);
		MPP_FD_SET(pctxt->donefd, &rset);

		tval.tv_sec = 1;
		tval.tv_usec = 0;
		n = select(pctxt->donefd+1, (fd_set *) &rset, NULL, NULL, &tval);
	
		if(n==1)
		{
			rwsize = retry_read(pctxt->donefd, &z, 1); 
			Assert(rwsize == 1 && z == 'z');

			elog(DEBUG1, "SISC: wait done get 1 notification");
			--nsharer_xslice;
		}
		else if(n==0)
		{
			elog(DEBUG1, "SISC: wait done timeout onece");
		}
		else 
		{
			int save_errno = errno;
			elog(LOG, "SISC: wait done time out once, errno %d", save_errno);
		}
	}

	elog(DEBUG1, "SISC: Writer have got all reader done notification");
	shareinput_clean_lk_ctxt(ctxt);
	UnregisterXactCallbackOnce(XCallBack_ShareInput_FIFO, (void *) ctxt);
}

void
initGpmonPktForShareInputScan(Plan *planNode, gpmon_packet_t *gpmon_pkt, EState *estate)
{
	Assert(planNode != NULL && gpmon_pkt != NULL && IsA(planNode, ShareInputScan));

	{
		Assert(GPMON_SHAREINPUT_TOTAL <= (int)GPMON_QEXEC_M_COUNT);
		InitPlanNodeGpmonPkt(planNode, gpmon_pkt, estate, PMNT_SharedScan,
							 (int64)planNode->plan_rows, 
							 NULL);
	}
}
