/*-------------------------------------------------------------------------
 *
 * heapam.c
 *	  heap access method code
 *
 * Portions Copyright (c) 1996-2009, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  $PostgreSQL: pgsql/src/backend/access/heap/heapam.c,v 1.223 2007/01/05 22:19:22 momjian Exp $
 *
 *
 * INTERFACE ROUTINES
 *		relation_open	- open any relation by relation OID
 *		relation_openrv - open any relation specified by a RangeVar
 *		relation_close	- close any relation
 *		heap_open		- open a heap relation by relation OID
 *		heap_openrv		- open a heap relation specified by a RangeVar
 *		heap_close		- (now just a macro for relation_close)
 *		heap_beginscan	- begin relation scan
 *		heap_rescan		- restart a relation scan
 *		heap_endscan	- end relation scan
 *		heap_getnext	- retrieve next tuple in scan
 *		heap_fetch		- retrieve tuple with given tid
 *		heap_insert		- insert tuple into a relation
 *		heap_delete		- delete a tuple from a relation
 *		heap_update		- replace a tuple in a relation with another tuple
 *		heap_markpos	- mark scan position
 *		heap_restrpos	- restore position to marked location
 *
 * NOTES
 *	  This file contains the heap_ routines which implement
 *	  the POSTGRES heap access method used for all POSTGRES
 *	  relations.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/heapam.h"
#include "access/hio.h"
#include "access/multixact.h"
#include "access/relscan.h"
#include "access/sysattr.h"
#include "access/transam.h"
#include "access/tuptoaster.h"
#include "access/valid.h"
#include "access/xact.h"
#include "catalog/catalog.h"
#include "catalog/catquery.h"
#include "catalog/gp_policy.h"
#include "catalog/namespace.h"
#include "miscadmin.h"
#include "pgstat.h"
#include "storage/procarray.h"
#include "utils/inval.h"
#include "utils/lsyscache.h"
#include "utils/relcache.h"
#include "utils/syscache.h"
#include "utils/faultinjector.h"
#include "storage/freespace.h"
#include "utils/guc.h"
#include "cdb/cdbpersistentrecovery.h"
#include "cdb/cdbinmemheapam.h"
#include "cdb/cdbvars.h"
#include "miscadmin.h"

static XLogRecPtr log_heap_update(Relation reln, Buffer oldbuf,
		   ItemPointerData from, Buffer newbuf, HeapTuple newtup, bool move);


/* ----------------------------------------------------------------
 *						 heap support routines
 * ----------------------------------------------------------------
 */

/* ----------------
 *		initscan - scan code common to heap_beginscan and heap_rescan
 * ----------------
 */
static void
initscan(HeapScanDesc scan, ScanKey key)
{
	int i;

	/*
	 * Determine the number of blocks we have to scan.
	 *
	 * It is sufficient to do this once at scan start, since any tuples added
	 * while the scan is in progress will be invisible to my transaction
	 * anyway...
	 */
	scan->rs_nblocks = RelationGetNumberOfBlocks(scan->rs_rd);

	scan->rs_inited = false;
	scan->rs_ctup.t_data = NULL;
	ItemPointerSetInvalid(&scan->rs_ctup.t_self);
	scan->rs_cbuf = InvalidBuffer;
	scan->rs_cblock = InvalidBlockNumber;
	for (i = 0; i < lengthof(scan->rs_rahead); i++)
	{
		scan->rs_rahead[i].buffer = InvalidBuffer;
		scan->rs_rahead[i].block = InvalidBlockNumber;
	}

	/* we don't have a marked position... */
	ItemPointerSetInvalid(&(scan->rs_mctid));

	/* page-at-a-time fields are always invalid when not rs_inited */

	/*
	 * copy the scan key, if appropriate
	 */
	if (key != NULL)
		memcpy(scan->rs_key, key, scan->rs_nkeys * sizeof(ScanKeyData));

	/*
	 * Currently, we don't have a stats counter for bitmap heap scans (but the
	 * underlying bitmap index scans will be counted).
	 */
	//if (!scan->rs_bitmapscan)
		pgstat_count_heap_scan(scan->rs_rd);
}

/*
 * heapgetpage - subroutine for heapgettup()
 *
 * This routine reads and pins the specified page of the relation.
 * In page-at-a-time mode it performs additional work, namely determining
 * which tuples on the page are visible.
 */
static void
heapgetpage(HeapScanDesc scan, BlockNumber page, bool backward)
{
	Buffer		buffer;
	Snapshot	snapshot;
	Page		dp;
	int			lines;
	int			ntup;
	OffsetNumber lineoff;
	ItemId		lpp;
	TransactionId t_xmin, t_xmax;
	CommandId     t_cid;

	MIRROREDLOCK_BUFMGR_MUST_ALREADY_BE_HELD;

	/* don't run prefetching code if the table is small. This has the
	   side effect of using the full buffer pool to cache the table */
	bool        is_small = (scan->rs_nblocks <= (NBuffers * 60 / 100));

	Assert(page < scan->rs_nblocks);

	if (! scan->rs_pageatatime)
	{
		scan->rs_cbuf = ReleaseAndReadBuffer(scan->rs_cbuf,
											 scan->rs_rd,
											 page);
		scan->rs_cblock = page;
		return;
	}

	if (is_small)
	{
		scan->rs_cbuf = ReleaseAndReadBuffer(scan->rs_cbuf, scan->rs_rd, page);
		scan->rs_cblock = page;
	}
	else
	{
		int         ri, i;
		const int   incr = backward ? -1 : 1;
		const int   rimax = lengthof(scan->rs_rahead);

		/*
		 *  This will fill up to the first 2 quarters of read ahead
		 */
		for (i = 0; i < rimax / 2; i++)
		{
			const int pg = page + incr * i;
			if (! (0 <= pg && pg < scan->rs_nblocks))
				break;
			ri = pg % rimax;
			if (scan->rs_rahead[ri].block == pg)
				continue;
			scan->rs_rahead[ri].block = pg;
			scan->rs_rahead[ri].buffer
				= KillAndReadBuffer(scan->rs_rahead[ri].buffer, scan->rs_rd, pg);
			/*elog(LOG, "heapgetpage: 1 [%d] read %d at %x", ri, pg, scan->rs_rahead[ri].buffer);*/
		}
		/* THIS MAY ONLY BE HELPFUL WITH AIO */
		/*
		 *  This will fill the 3rd quarter of read ahead provided that it is not filled
		 */
		for (i = rimax / 2; i < rimax * 3 / 4; i++)
		{
			const int pg = page + incr * i;
			if (! (0 <= pg && pg < scan->rs_nblocks))
				break;
			ri = pg % rimax;
			if (scan->rs_rahead[ri].block == pg)
				break;
			scan->rs_rahead[ri].block = pg;
			scan->rs_rahead[ri].buffer
				= KillAndReadBuffer(scan->rs_rahead[ri].buffer, scan->rs_rd, pg);
			/*elog(LOG, "heapgetpage: 2 [%d] read %d at %x", ri, pg, scan->rs_rahead[ri].buffer);*/
		}

		ri = page % rimax;
		if (scan->rs_rahead[ri].block != page)
			ereport(ERROR,
					(errcode(ERRCODE_INTERNAL_ERROR),
					 errmsg("page number mismatch, expect %d, but got %d",
							page, scan->rs_rahead[ri].block)));

		if (BufferIsValid(scan->rs_cbuf))
			ReleaseBuffer(scan->rs_cbuf);
		scan->rs_cbuf = scan->rs_rahead[ri].buffer;
		scan->rs_cblock = page;
		IncrBufferRefCount(scan->rs_cbuf);
	}

	buffer = scan->rs_cbuf;
	snapshot = scan->rs_snapshot;

	/*
	 * We must hold share lock on the buffer content while examining tuple
	 * visibility.	Afterwards, however, the tuples we have found to be
	 * visible are guaranteed good as long as we hold the buffer pin.
	 */
	LockBuffer(buffer, BUFFER_LOCK_SHARE);

	dp = (Page) BufferGetPage(buffer);
	lines = PageGetMaxOffsetNumber(dp);
	ntup = 0;

	t_xmin = t_xmax = 0;
	t_cid = 0;

	for (lineoff = FirstOffsetNumber, lpp = PageGetItemId(dp, lineoff);
		 lineoff <= lines;
		 lineoff++, lpp++)
	{
		if (ItemIdIsUsed(lpp))
		{

			bool		valid;
			HeapTupleHeader theader = (HeapTupleHeader) PageGetItem((Page) dp, lpp);

			valid = (t_xmax == HeapTupleHeaderGetXmax(theader) &&
					 t_xmin == HeapTupleHeaderGetXmin(theader) &&
					 t_cid == HeapTupleHeaderGetRawCommandId(theader));

			if(!valid)
			{
				HeapTupleData loctup;

				loctup.t_data = theader;
				loctup.t_len = ItemIdGetLength(lpp);
				ItemPointerSet(&(loctup.t_self), page, lineoff);

				valid = HeapTupleSatisfiesVisibility(scan->rs_rd, &loctup, snapshot, buffer);
#ifdef WATCH_VISIBILITY_IN_ACTION
				elog(LOG, "VISIBILITY(%s) %s %s",
				     (valid ? "true" : "false"),
	     		     RelationGetRelationName(scan->rs_rd),
				     WatchVisibilityInActionString(
				     			page,
				     			lineoff,
				     			&loctup,
				     			snapshot));
#endif
				if (valid)
				{
					t_xmax = HeapTupleHeaderGetXmax(loctup.t_data);
					t_xmin = HeapTupleHeaderGetXmin(loctup.t_data);
					t_cid = HeapTupleHeaderGetRawCommandId(loctup.t_data);
					scan->rs_vistuples[ntup++] = lineoff;
				}
			}
			else
				scan->rs_vistuples[ntup++] = lineoff;
		}
	}

	LockBuffer(buffer, BUFFER_LOCK_UNLOCK);

	Assert(ntup <= MaxHeapTuplesPerPage);
	scan->rs_ntuples = ntup;
}

/* ----------------
 *		heapgettup - fetch next heap tuple
 *
 *		Initialize the scan if not already done; then advance to the next
 *		tuple as indicated by "dir"; return the next tuple in scan->rs_ctup,
 *		or set scan->rs_ctup.t_data = NULL if no more tuples.
 *
 * dir == NoMovementScanDirection means "re-fetch the tuple indicated
 * by scan->rs_ctup".
 *
 * Note: the reason nkeys/key are passed separately, even though they are
 * kept in the scan descriptor, is that the caller may not want us to check
 * the scankeys.
 *
 * Note: when we fall off the end of the scan in either direction, we
 * reset rs_inited.  This means that a further request with the same
 * scan direction will restart the scan, which is a bit odd, but a
 * request with the opposite scan direction will start a fresh scan
 * in the proper direction.  The latter is required behavior for cursors,
 * while the former case is generally undefined behavior in Postgres
 * so we don't care too much.
 * ----------------
 */
static void
heapgettup(HeapScanDesc scan,
		   ScanDirection dir,
		   int nkeys,
		   ScanKey key)
{
	MIRROREDLOCK_BUFMGR_DECLARE;

	HeapTuple	tuple = &(scan->rs_ctup);
	Snapshot	snapshot = scan->rs_snapshot;
	bool		backward = ScanDirectionIsBackward(dir);
	BlockNumber page;
	Page		dp;
	int			lines;
	OffsetNumber lineoff;
	int			linesleft;
	ItemId		lpp;

	/*
	 * calculate next starting lineoff, given scan direction
	 */
	
	// -------- MirroredLock ----------
	MIRROREDLOCK_BUFMGR_LOCK;
	
	if (ScanDirectionIsForward(dir))
	{
		if (!scan->rs_inited)
		{
			/*
			 * return null immediately if relation is empty
			 */
			if (scan->rs_nblocks == 0)
			{
				
				MIRROREDLOCK_BUFMGR_UNLOCK;
				// -------- MirroredLock ----------
				
				Assert(!BufferIsValid(scan->rs_cbuf));
				tuple->t_data = NULL;
				return;
			}
			page = 0;			/* first page */
			heapgetpage(scan, page, ScanDirectionIsBackward(dir));
			lineoff = FirstOffsetNumber;		/* first offnum */
			scan->rs_inited = true;
		}
		else
		{
			/* continue from previously returned page/tuple */
			page = scan->rs_cblock;		/* current page */
			lineoff =			/* next offnum */
				OffsetNumberNext(ItemPointerGetOffsetNumber(&(tuple->t_self)));
		}

		LockBuffer(scan->rs_cbuf, BUFFER_LOCK_SHARE);

		dp = (Page) BufferGetPage(scan->rs_cbuf);
		lines = PageGetMaxOffsetNumber(dp);
		/* page and lineoff now reference the physically next tid */

		linesleft = lines - lineoff + 1;
	}
	else if (backward)
	{
		if (!scan->rs_inited)
		{
			/*
			 * return null immediately if relation is empty
			 */
			if (scan->rs_nblocks == 0)
			{
				
				MIRROREDLOCK_BUFMGR_UNLOCK;
				// -------- MirroredLock ----------
				
				Assert(!BufferIsValid(scan->rs_cbuf));
				tuple->t_data = NULL;
				return;
			}
			page = scan->rs_nblocks - 1;		/* final page */
			heapgetpage(scan, page, ScanDirectionIsBackward(dir));
		}
		else
		{
			/* continue from previously returned page/tuple */
			page = scan->rs_cblock;		/* current page */
		}

		LockBuffer(scan->rs_cbuf, BUFFER_LOCK_SHARE);

		dp = (Page) BufferGetPage(scan->rs_cbuf);
		lines = PageGetMaxOffsetNumber(dp);

		if (!scan->rs_inited)
		{
			lineoff = lines;	/* final offnum */
			scan->rs_inited = true;
		}
		else
		{
			lineoff =			/* previous offnum */
				OffsetNumberPrev(ItemPointerGetOffsetNumber(&(tuple->t_self)));
		}
		/* page and lineoff now reference the physically previous tid */

		linesleft = lineoff;
	}
	else
	{
		/*
		 * ``no movement'' scan direction: refetch prior tuple
		 */
		if (!scan->rs_inited)
		{

			MIRROREDLOCK_BUFMGR_UNLOCK;
			// -------- MirroredLock ----------

			Assert(!BufferIsValid(scan->rs_cbuf));
			tuple->t_data = NULL;
			return;
		}

		page = ItemPointerGetBlockNumber(&(tuple->t_self));
		if (page != scan->rs_cblock)
			heapgetpage(scan, page, ScanDirectionIsBackward(dir));

		/* Since the tuple was previously fetched, needn't lock page here */
		dp = (Page) BufferGetPage(scan->rs_cbuf);
		lineoff = ItemPointerGetOffsetNumber(&(tuple->t_self));
		lpp = PageGetItemId(dp, lineoff);
		Assert(ItemIdIsNormal(lpp));

		tuple->t_data = (HeapTupleHeader) PageGetItem((Page) dp, lpp);
		tuple->t_len = ItemIdGetLength(lpp);
		
		MIRROREDLOCK_BUFMGR_UNLOCK;
		// -------- MirroredLock ----------
		
		return;
	}

	/*
	 * advance the scan until we find a qualifying tuple or run out of stuff
	 * to scan
	 */
	lpp = PageGetItemId(dp, lineoff);
	for (;;)
	{
        CHECK_FOR_INTERRUPTS();

		while (linesleft > 0)
		{
			if (ItemIdIsUsed(lpp))
			{
				bool		valid;

				tuple->t_data = (HeapTupleHeader) PageGetItem((Page) dp, lpp);
				tuple->t_len = ItemIdGetLength(lpp);
				ItemPointerSet(&(tuple->t_self), page, lineoff);

				/*
				 * if current tuple qualifies, return it.
				 */
				valid = HeapTupleSatisfiesVisibility(scan->rs_rd,
													 tuple,
													 snapshot,
													 scan->rs_cbuf);
#ifdef WATCH_VISIBILITY_IN_ACTION
//				if (gp_watch_visibility_in_action)
				{
					bool keyMatches = true;

					/*
					 * Always test the key reguardless if it is visible.
					 */
					if (key != NULL)
						HeapKeyTest(tuple, RelationGetDescr(scan->rs_rd),
									nkeys, key, keyMatches);

					/*
					 * Log when the key matches.
					 */
					if (keyMatches)
					{
						elog(LOG, "VISIBILITY(%s) %s %s",
						     (valid ? "true" : "false"),
			     		     RelationGetRelationName(scan->rs_rd),
						     WatchVisibilityInActionString(page,lineoff,tuple,snapshot));
					}

					if (valid && !keyMatches)
						valid = false;
				}
//				else
//				{
//					/*
//					 * Normal path copied from below...
//					 */
//					if (valid && key != NULL)
//						HeapKeyTest(tuple, RelationGetDescr(scan->rs_rd),
//									nkeys, key, valid);
//				}
#else
				if (valid && key != NULL)
					HeapKeyTest(tuple, RelationGetDescr(scan->rs_rd),
								nkeys, key, &valid);
#endif

				if (valid)
				{
					LockBuffer(scan->rs_cbuf, BUFFER_LOCK_UNLOCK);
					
					MIRROREDLOCK_BUFMGR_UNLOCK;
					// -------- MirroredLock ----------
					
					return;
				}
			}

			/*
			 * otherwise move to the next item on the page
			 */
			--linesleft;
			if (backward)
			{
				--lpp;			/* move back in this page's ItemId array */
				--lineoff;
			}
			else
			{
				++lpp;			/* move forward in this page's ItemId array */
				++lineoff;
			}
		}

		/*
		 * if we get here, it means we've exhausted the items on this page and
		 * it's time to move to the next.
		 */
		LockBuffer(scan->rs_cbuf, BUFFER_LOCK_UNLOCK);
		
		MIRROREDLOCK_BUFMGR_UNLOCK;
		// -------- MirroredLock ----------
		
		/*
		 * return NULL if we've exhausted all the pages
		 */
		if (backward ? (page == 0) : (page + 1 >= scan->rs_nblocks))
		{
			if (BufferIsValid(scan->rs_cbuf))
				ReleaseBuffer(scan->rs_cbuf);
			scan->rs_cbuf = InvalidBuffer;
			scan->rs_cblock = InvalidBlockNumber;
			tuple->t_data = NULL;
			scan->rs_inited = false;
			return;
		}

		page = backward ? (page - 1) : (page + 1);
		
		// -------- MirroredLock ----------
		MIRROREDLOCK_BUFMGR_LOCK;
		
		heapgetpage(scan, page, ScanDirectionIsBackward(dir));

		LockBuffer(scan->rs_cbuf, BUFFER_LOCK_SHARE);

		dp = (Page) BufferGetPage(scan->rs_cbuf);
		lines = PageGetMaxOffsetNumber((Page) dp);
		linesleft = lines;
		if (backward)
		{
			lineoff = lines;
			lpp = PageGetItemId(dp, lines);
		}
		else
		{
			lineoff = FirstOffsetNumber;
			lpp = PageGetItemId(dp, FirstOffsetNumber);
		}
	}
}

/* ----------------
 *		heapgettup_pagemode - fetch next heap tuple in page-at-a-time mode
 *
 *		Same API as heapgettup, but used in page-at-a-time mode
 *
 * The internal logic is much the same as heapgettup's too, but there are some
 * differences: we do not take the buffer content lock (that only needs to
 * happen inside heapgetpage), and we iterate through just the tuples listed
 * in rs_vistuples[] rather than all tuples on the page.  Notice that
 * lineindex is 0-based, where the corresponding loop variable lineoff in
 * heapgettup is 1-based.
 * ----------------
 */
static void
heapgettup_pagemode(HeapScanDesc scan,
					ScanDirection dir,
					int nkeys,
					ScanKey key)
{
	MIRROREDLOCK_BUFMGR_DECLARE;

	HeapTuple	tuple = &(scan->rs_ctup);
	bool		backward = ScanDirectionIsBackward(dir);
	BlockNumber page;
	Page		dp;
	int			lines;
	int			lineindex;
	OffsetNumber lineoff;
	int			linesleft;
	ItemId		lpp;

	
	// -------- MirroredLock ----------
	MIRROREDLOCK_BUFMGR_LOCK;
	
	/*
	 * calculate next starting lineindex, given scan direction
	 */
	if (ScanDirectionIsForward(dir))
	{
		if (!scan->rs_inited)
		{
			/*
			 * return null immediately if relation is empty
			 */
			if (scan->rs_nblocks == 0)
			{
				
				MIRROREDLOCK_BUFMGR_UNLOCK;
				// -------- MirroredLock ----------
				
				Assert(!BufferIsValid(scan->rs_cbuf));
				tuple->t_data = NULL;
				return;
			}
			page = 0;			/* first page */
			heapgetpage(scan, page, ScanDirectionIsBackward(dir));
			lineindex = 0;
			scan->rs_inited = true;
		}
		else
		{
			/* continue from previously returned page/tuple */
			page = scan->rs_cblock;		/* current page */
			lineindex = scan->rs_cindex + 1;
		}

		dp = (Page) BufferGetPage(scan->rs_cbuf);
		lines = scan->rs_ntuples;
		/* page and lineindex now reference the next visible tid */

		linesleft = lines - lineindex;
	}
	else if (backward)
	{
		if (!scan->rs_inited)
		{
			/*
			 * return null immediately if relation is empty
			 */
			if (scan->rs_nblocks == 0)
			{
				
				MIRROREDLOCK_BUFMGR_UNLOCK;
				// -------- MirroredLock ----------
				
				Assert(!BufferIsValid(scan->rs_cbuf));
				tuple->t_data = NULL;
				return;
			}
			page = scan->rs_nblocks - 1;		/* final page */
			heapgetpage(scan, page, ScanDirectionIsBackward(dir));
		}
		else
		{
			/* continue from previously returned page/tuple */
			page = scan->rs_cblock;		/* current page */
		}

		dp = (Page) BufferGetPage(scan->rs_cbuf);
		lines = scan->rs_ntuples;

		if (!scan->rs_inited)
		{
			lineindex = lines - 1;
			scan->rs_inited = true;
		}
		else
		{
			lineindex = scan->rs_cindex - 1;
		}
		/* page and lineindex now reference the previous visible tid */

		linesleft = lineindex + 1;
	}
	else
	{
		/*
		 * ``no movement'' scan direction: refetch prior tuple
		 */
		if (!scan->rs_inited)
		{

			MIRROREDLOCK_BUFMGR_UNLOCK;
			// -------- MirroredLock ----------

			Assert(!BufferIsValid(scan->rs_cbuf));
			tuple->t_data = NULL;
			return;
		}

		page = ItemPointerGetBlockNumber(&(tuple->t_self));
		if (page != scan->rs_cblock)
			heapgetpage(scan, page, ScanDirectionIsBackward(dir));

		/* Since the tuple was previously fetched, needn't lock page here */
		
		MIRROREDLOCK_BUFMGR_UNLOCK;
		// -------- MirroredLock ----------
		
		dp = (Page) BufferGetPage(scan->rs_cbuf);
		lineoff = ItemPointerGetOffsetNumber(&(tuple->t_self));
		lpp = PageGetItemId(dp, lineoff);
		Assert(ItemIdIsNormal(lpp));

		tuple->t_data = (HeapTupleHeader) PageGetItem((Page) dp, lpp);
		tuple->t_len = ItemIdGetLength(lpp);

		/* check that rs_cindex is in sync */
		Assert(scan->rs_cindex < scan->rs_ntuples);
		Assert(lineoff == scan->rs_vistuples[scan->rs_cindex]);

		return;
	}

	/*
	 * advance the scan until we find a qualifying tuple or run out of stuff
	 * to scan
	 */
	for (;;)
	{
        CHECK_FOR_INTERRUPTS();

		while (linesleft > 0)
		{
			lineoff = scan->rs_vistuples[lineindex];
			lpp = PageGetItemId(dp, lineoff);
			Assert(ItemIdIsUsed(lpp));

			tuple->t_data = (HeapTupleHeader) PageGetItem((Page) dp, lpp);
			tuple->t_len = ItemIdGetLength(lpp);
			ItemPointerSet(&(tuple->t_self), page, lineoff);

			/*
			 * if current tuple qualifies, return it.
			 */
			if (key != NULL)
			{
				bool		valid;

				HeapKeyTest(tuple, RelationGetDescr(scan->rs_rd),
							nkeys, key, &valid);
				if (valid)
				{
					
					MIRROREDLOCK_BUFMGR_UNLOCK;
					// -------- MirroredLock ----------
					
					scan->rs_cindex = lineindex;
					return;
				}
			}
			else
			{
				
				MIRROREDLOCK_BUFMGR_UNLOCK;
				// -------- MirroredLock ----------
				
				scan->rs_cindex = lineindex;
				return;
			}

			/*
			 * otherwise move to the next item on the page
			 */
			--linesleft;
			if (backward)
				--lineindex;
			else
				++lineindex;
		}

		/*
		 * if we get here, it means we've exhausted the items on this page and
		 * it's time to move to the next.
		 */

		MIRROREDLOCK_BUFMGR_UNLOCK;
		// -------- MirroredLock ----------

		/*
		 * return NULL if we've exhausted all the pages
		 */
		if (backward ? (page == 0) : (page + 1 >= scan->rs_nblocks))
		{
			if (BufferIsValid(scan->rs_cbuf))
				ReleaseBuffer(scan->rs_cbuf);
			scan->rs_cbuf = InvalidBuffer;
			scan->rs_cblock = InvalidBlockNumber;
			tuple->t_data = NULL;
			scan->rs_inited = false;
			return;
		}

		page = backward ? (page - 1) : (page + 1);
		
		// -------- MirroredLock ----------
		MIRROREDLOCK_BUFMGR_LOCK;
		
		heapgetpage(scan, page, ScanDirectionIsBackward(dir));

		dp = (Page) BufferGetPage(scan->rs_cbuf);
		lines = scan->rs_ntuples;
		linesleft = lines;
		if (backward)
			lineindex = lines - 1;
		else
			lineindex = 0;
	}
}


/* ----------------------------------------------------------------
 *					 heap access method interface
 * ----------------------------------------------------------------
 */

/* ----------------
 *		relation_open - open any relation by relation OID
 *
 *		If lockmode is not "NoLock", the specified kind of lock is
 *		obtained on the relation.  (Generally, NoLock should only be
 *		used if the caller knows it has some appropriate lock on the
 *		relation already.)
 *
 *		An error is raised if the relation does not exist.
 *
 *		NB: a "relation" is anything with a pg_class entry.  The caller is
 *		expected to check whether the relkind is something it can handle.
 * ----------------
 */
Relation
relation_open(Oid relationId, LOCKMODE lockmode)
{
	Relation	r;

	Assert(lockmode >= NoLock && lockmode < MAX_LOCKMODES);

	/* Get the lock before trying to open the relcache entry */
	if (lockmode != NoLock)
		LockRelationOid(relationId, lockmode);

	/* The relcache does all the real work... */
	r = RelationIdGetRelation(relationId);

	if (!RelationIsValid(r))
		elog(ERROR, "could not open relation with OID %u", relationId);

	pgstat_initstats(r);
	
	return r;
}

/* ----------------
 *		try_relation_open - open any relation by relation OID
 *
 *		Same as relation_open, except return NULL instead of failing
 *		if the relation does not exist.
 * ----------------
 */
Relation
try_relation_open(Oid relationId, LOCKMODE lockmode, bool noWait)
{
	Relation	r;

	Assert(lockmode >= NoLock && lockmode < MAX_LOCKMODES);

	/* Get the lock first */
	if (lockmode != NoLock)
	{
		if (!noWait)
			LockRelationOid(relationId, lockmode);
		else
		{
			/*
			 * noWait is a Greenplum addition to the open_relation code
			 * basically to support INSERT ... FOR UPDATE NOWAIT.  Our NoWait
			 * handling needs to be more tollerant of failed locks than standard
			 * postgres largely due to the fact that we have to promote certain
			 * update locks in order to handle distributed updates.
			 */
			if (!ConditionalLockRelationOid(relationId, lockmode))
			{
				/* Get the name for error reporting */
				char *relname = get_rel_name(relationId);

				/* 
				 * If the relation was dropped then accept the lock 
				 * failure and return null.
				 */
				if (relname == NULL)
					return NULL;
				
				/* Otherwise report the failed lock */
				ereport(ERROR, 
						(errcode(ERRCODE_LOCK_NOT_AVAILABLE),
						 errmsg("could not obtain lock on relation \"%s\"",
								relname)));
			}
		}
	}

	/*
	 * Now that we have the lock, probe to see if the relation really exists
	 * or not.
	 */
	if (0 == caql_getcount(
				NULL,
				cql("SELECT COUNT(*) FROM pg_class "
					" WHERE oid = :1 ",
					ObjectIdGetDatum(relationId))))
	{
		/* Release useless lock */
		if (lockmode != NoLock)
			UnlockRelationOid(relationId, lockmode);

		return NULL;
	}

	/* Should be safe to do a relcache load */
	r = RelationIdGetRelation(relationId);

	if (!RelationIsValid(r))
		elog(ERROR, "could not open relation with OID %u", relationId);

	pgstat_initstats(r);

	return r;
}



/*
 * CdbTryOpenRelation -- Opens a relation with a specified lock mode.
 *
 * CDB: Like try_open_relation, except that it will upgrade the lock when needed
 * for distributed tables.
 */
Relation
CdbTryOpenRelation(Oid relid, LOCKMODE reqmode, bool noWait, bool *lockUpgraded)
{
    LOCKMODE    lockmode = reqmode;
	Relation    rel;

	if (lockUpgraded != NULL)
		*lockUpgraded = false;

    /*
	 * CDB: We need to upgrade the requested lock from RowExclusiveLock
	 * to Exclusive lock (locking the whole relation) when doing updates
	 * on distributed relations.
	 *
	 * Historically the need for this was do to limitations caused by lack of
	 * distributed deadlock detection (which we now have) and due to our use
	 * of logical mirroring (which has been replaced with physical replication).
	 * However the need for this remains due to our current implementation
	 * of partitioning.
	 *
	 * Specifically in the case of INSERT SELECT we do not know which partitions
	 * we may be updating.  So we need to serialize UPDATE and DELETE operations
	 * to the same partitioned table.  
	 *
	 * Note: This code could be improved substantiatally.
	 */
	if (lockmode == RowExclusiveLock)
	{
		rel = try_heap_open(relid, NoLock, noWait);
		if (!rel)
			return NULL;
		
		if (rel->rd_cdbpolicy &&
			rel->rd_cdbpolicy->ptype == POLICYTYPE_PARTITIONED)
		{
			lockmode = ExclusiveLock;
			if (lockUpgraded != NULL)
				*lockUpgraded = true;
		}
		relation_close(rel, NoLock);
    }

	rel = try_heap_open(relid, lockmode, noWait);
	if (!RelationIsValid(rel))
		return NULL;

	/* 
	 * There is a slim chance that ALTER TABLE SET DISTRIBUTED BY may
	 * have altered the distribution policy between the time that we
	 * decided to upgrade the lock and the time we opened the relation
	 * with the lock.  Double check that our chosen lock mode is still
	 * okay.
	 */
	if (lockmode == RowExclusiveLock &&
		rel->rd_cdbpolicy &&
		rel->rd_cdbpolicy->ptype == POLICYTYPE_PARTITIONED)
	{
		elog(ERROR, "relation \"%s\" concurrently updated", 
			 RelationGetRelationName(rel));
	}

	return rel;
}                                       /* CdbOpenRelation */

/*
 * CdbOpenRelation -- Opens a relation with a specified lock mode.
 *
 * CDB: Like CdbTryOpenRelation, except that it guarantees either
 * an error or a valid opened relation returned.
 */
Relation
CdbOpenRelation(Oid relid, LOCKMODE reqmode, bool noWait, bool *lockUpgraded)
{
	Relation rel;

	rel = CdbTryOpenRelation(relid, reqmode, noWait, lockUpgraded);

	if (!RelationIsValid(rel))
		elog(ERROR, "could not open relation with OID %u", relid);

	return rel;

}                                       /* CdbOpenRelation */

/*
 * CdbOpenRelationRv -- Opens a relation with a specified lock mode.
 *
 * CDB: Like CdbTryOpenRelation, except that it guarantees either
 * an error or a valid opened relation returned.
 */
Relation
CdbOpenRelationRv(const RangeVar *relation, LOCKMODE reqmode, bool noWait, 
				  bool *lockUpgraded)
{
	Oid			relid;
	Relation	rel;

	/* Look up the appropriate relation using namespace search */
	relid = RangeVarGetRelid(relation, false, true /*allowHcatalog*/);
	rel = CdbTryOpenRelation(relid, reqmode, noWait, lockUpgraded);

	if (!RelationIsValid(rel))
	{
		if (relation->schemaname)
		{
			ereport(ERROR,
					(errcode(ERRCODE_UNDEFINED_TABLE),
					 errmsg("relation \"%s.%s\" does not exist",
							relation->schemaname, relation->relname),
					 errOmitLocation(true)));
		}
		else
		{
			ereport(ERROR,
					(errcode(ERRCODE_UNDEFINED_TABLE),
					 errmsg("relation \"%s\" does not exist",
							relation->relname),
					 errOmitLocation(true)));
		}
	}

	return rel;

}                                       /* CdbOpenRelation */



/* ----------------
 *		relation_open_nowait - open but don't wait for lock
 *
 *		Same as relation_open, except throw an error instead of waiting
 *		when the requested lock is not immediately obtainable.
 * ----------------
 */
Relation
relation_open_nowait(Oid relationId, LOCKMODE lockmode)
{
	Relation	r;

	Assert(lockmode >= NoLock && lockmode < MAX_LOCKMODES);

	/* Get the lock before trying to open the relcache entry */
	if (lockmode != NoLock)
	{
		if (!ConditionalLockRelationOid(relationId, lockmode))
		{
			/* try to throw error by name; relation could be deleted... */
			char	   *relname = get_rel_name(relationId);

			if (relname)
				ereport(ERROR,
						(errcode(ERRCODE_LOCK_NOT_AVAILABLE),
						 errmsg("could not obtain lock on relation \"%s\"",
								relname)));
			else
				ereport(ERROR,
						(errcode(ERRCODE_LOCK_NOT_AVAILABLE),
					  errmsg("could not obtain lock on relation with OID %u",
							 relationId)));
		}
	}

	/* The relcache does all the real work... */
	r = RelationIdGetRelation(relationId);

	if (!RelationIsValid(r))
		elog(ERROR, "could not open relation with OID %u", relationId);

	return r;
}

/* ----------------
 *		relation_openrv - open any relation specified by a RangeVar
 *
 *		Same as relation_open, but the relation is specified by a RangeVar.
 * ----------------
 */
Relation
relation_openrv(const RangeVar *relation, LOCKMODE lockmode)
{
	Oid			relOid;
	Relation    rel;

	/*
	 * Check for shared-cache-inval messages before trying to open the
	 * relation.  This is needed to cover the case where the name identifies a
	 * rel that has been dropped and recreated since the start of our
	 * transaction: if we don't flush the old syscache entry then we'll latch
	 * onto that entry and suffer an error when we do RelationIdGetRelation.
	 * Note that relation_open does not need to do this, since a relation's
	 * OID never changes.
	 *
	 * We skip this if asked for NoLock, on the assumption that the caller has
	 * already ensured some appropriate lock is held.
	 */
	if (lockmode != NoLock)
		AcceptInvalidationMessages();

	/* Look up the appropriate relation using namespace search */
	relOid = RangeVarGetRelid(relation, false, true /*allowHcatalog*/);

	/* 
	 * use try_relation_open instead of relation_open so that we can
	 * throw a more graceful error message if the relation was dropped
	 * between the RangeVarGetRelid and when we try to open the relation.
	 */
	rel = try_relation_open(relOid, lockmode, false);

	if (!RelationIsValid(rel))
	{
		if (relation->schemaname)
		{
			ereport(ERROR,
					(errcode(ERRCODE_UNDEFINED_TABLE),
					 errmsg("relation \"%s.%s\" does not exist",
							relation->schemaname, relation->relname),
					 errOmitLocation(true)));
		}
		else
		{
			ereport(ERROR,
					(errcode(ERRCODE_UNDEFINED_TABLE),
					 errmsg("relation \"%s\" does not exist",
							relation->relname),
					 errOmitLocation(true)));
		}
	}

	return rel;
}

/* ----------------
 *		try_relation_openrv - open any relation specified by a RangeVar
 *
 *		Same as relation_openrv, but return NULL instead of failing for
 *		relation-not-found.  (Note that some other causes, such as
 *		permissions problems, will still result in an ereport.)
 * ----------------
 */
Relation
try_relation_openrv(const RangeVar *relation, LOCKMODE lockmode, bool noWait)
{
	Oid			relOid;

	/*
	 * Check for shared-cache-inval messages before trying to open the
	 * relation.  This is needed to cover the case where the name identifies a
	 * rel that has been dropped and recreated since the start of our
	 * transaction: if we don't flush the old syscache entry then we'll latch
	 * onto that entry and suffer an error when we do RelationIdGetRelation.
	 * Note that relation_open does not need to do this, since a relation's
	 * OID never changes.
	 *
	 * We skip this if asked for NoLock, on the assumption that the caller has
	 * already ensured some appropriate lock is held.
	 */
	if (lockmode != NoLock)
		AcceptInvalidationMessages();

	/* Look up the appropriate relation using namespace search */
	relOid = RangeVarGetRelid(relation, true, true /*allowHcatalog*/);

	/* Return NULL on not-found */
	if (!OidIsValid(relOid))
		return NULL;

	/* Let relation_open do the rest */
	return try_relation_open(relOid, lockmode, noWait);
}


/* ----------------
 *		relation_close - close any relation
 *
 *		If lockmode is not "NoLock", we then release the specified lock.
 *
 *		Note that it is often sensible to hold a lock beyond relation_close;
 *		in that case, the lock is released automatically at xact end.
 * ----------------
 */
void
relation_close(Relation relation, LOCKMODE lockmode)
{
	LockRelId	relid = relation->rd_lockInfo.lockRelId;

	Assert(lockmode >= NoLock && lockmode < MAX_LOCKMODES);

	/* The relcache does the real work... */
	RelationClose(relation);

	if (lockmode != NoLock)
		UnlockRelationId(&relid, lockmode);
}


/* ----------------
 *		heap_open - open a heap relation by relation OID
 *
 *		This is essentially relation_open plus check that the relation
 *		is not an index nor a composite type.  (The caller should also
 *		check that it's not a view before assuming it has storage.)
 * ----------------
 */
Relation
heap_open(Oid relationId, LOCKMODE lockmode)
{
	Relation	r;

	r = relation_open(relationId, lockmode);

	if (r->rd_rel->relkind == RELKIND_INDEX)
		ereport(ERROR,
				(errcode(ERRCODE_WRONG_OBJECT_TYPE),
				 errmsg("\"%s\" is an index",
						RelationGetRelationName(r))));
	else if (r->rd_rel->relkind == RELKIND_COMPOSITE_TYPE)
		ereport(ERROR,
				(errcode(ERRCODE_WRONG_OBJECT_TYPE),
				 errmsg("\"%s\" is a composite type",
						RelationGetRelationName(r))));

	return r;
}

/* ----------------
 *		try_heap_open - open a heap relation by relation OID
 *
 *		As above, but relation return NULL for relation-not-found
 * ----------------
 */
Relation
try_heap_open(Oid relationId, LOCKMODE lockmode, bool noWait)
{
	Relation	r;

	r = try_relation_open(relationId, lockmode, noWait);

	if (!RelationIsValid(r))
		return NULL;

	if (r->rd_rel->relkind == RELKIND_INDEX)
		ereport(ERROR,
				(errcode(ERRCODE_WRONG_OBJECT_TYPE),
				 errmsg("\"%s\" is an index",
						RelationGetRelationName(r))));
	else if (r->rd_rel->relkind == RELKIND_COMPOSITE_TYPE)
		ereport(ERROR,
				(errcode(ERRCODE_WRONG_OBJECT_TYPE),
				 errmsg("\"%s\" is a composite type",
						RelationGetRelationName(r))));

	return r;
}

/* ----------------
 *		heap_openrv - open a heap relation specified
 *		by a RangeVar node
 *
 *		As above, but relation is specified by a RangeVar.
 * ----------------
 */
Relation
heap_openrv(const RangeVar *relation, LOCKMODE lockmode)
{
	Relation	r;

	r = relation_openrv(relation, lockmode);

	if (r->rd_rel->relkind == RELKIND_INDEX)
		ereport(ERROR,
				(errcode(ERRCODE_WRONG_OBJECT_TYPE),
				 errmsg("\"%s\" is an index",
						RelationGetRelationName(r))));
	else if (r->rd_rel->relkind == RELKIND_COMPOSITE_TYPE)
		ereport(ERROR,
				(errcode(ERRCODE_WRONG_OBJECT_TYPE),
				 errmsg("\"%s\" is a composite type",
						RelationGetRelationName(r))));

	return r;
}

/* ----------------
 *      try_heap_openrv - open a heap relation specified
 *      by a RangeVar node
 *
 *      As above, but return NULL instead of failing for relation-not-found.
 * ----------------
 */
Relation
try_heap_openrv(const RangeVar *relation, LOCKMODE lockmode, bool noWait)
{
    Relation    r;

    r = try_relation_openrv(relation, lockmode, noWait);

    if (r)
    {
        if (r->rd_rel->relkind == RELKIND_INDEX)
            ereport(ERROR,
                    (errcode(ERRCODE_WRONG_OBJECT_TYPE),
                     errmsg("\"%s\" is an index",
                            RelationGetRelationName(r))));
        else if (r->rd_rel->relkind == RELKIND_COMPOSITE_TYPE)
            ereport(ERROR,
                    (errcode(ERRCODE_WRONG_OBJECT_TYPE),
                     errmsg("\"%s\" is a composite type",
                            RelationGetRelationName(r))));
    }
    return r;
}
           


/* ----------------
 *		heap_beginscan	- begin relation scan
 * ----------------
 */
HeapScanDesc
heap_beginscan(Relation relation, Snapshot snapshot,
			   int nkeys, ScanKey key)
{
	HeapScanDesc scan;

	/*
	 * increment relation ref count while scanning relation
	 *
	 * This is just to make really sure the relcache entry won't go away while
	 * the scan has a pointer to it.  Caller should be holding the rel open
	 * anyway, so this is redundant in all normal scenarios...
	 */
	RelationIncrementReferenceCount(relation);

	/*
	 * allocate and initialize scan descriptor
	 */
	scan = (HeapScanDesc) palloc(sizeof(HeapScanDescData));

	scan->rs_rd = relation;
	scan->rs_snapshot = snapshot;
	scan->rs_nkeys = nkeys;

	/*
	 * we can use page-at-a-time mode if it's an MVCC-safe snapshot
	 */
	scan->rs_pageatatime = IsMVCCSnapshot(snapshot);

	/*
	 * we do this here instead of in initscan() because heap_rescan also calls
	 * initscan() and we don't want to allocate memory again
	 */
	if (nkeys > 0)
		scan->rs_key = (ScanKey) palloc(sizeof(ScanKeyData) * nkeys);
	else
		scan->rs_key = NULL;


	initscan(scan, key);

	return scan;
}

static void release_all_buffers(HeapScanDesc scan)
{
	int i;
	/*
	 * unpin scan buffers
	 */
	if (BufferIsValid(scan->rs_cbuf)) {
		ReleaseBuffer(scan->rs_cbuf);
		scan->rs_cbuf = InvalidBuffer;
	}
	for (i = 0; i < lengthof(scan->rs_rahead); i++)  {
		if (BufferIsValid(scan->rs_rahead[i].buffer)) {
			ReleaseBuffer(scan->rs_rahead[i].buffer);
			scan->rs_rahead[i].buffer = InvalidBuffer;
		}
	}
}



/* ----------------
 *		heap_rescan		- restart a relation scan
 * ----------------
 */
void
heap_rescan(HeapScanDesc scan,
			ScanKey key)
{
	/*
	 * unpin scan buffers
	 */
	release_all_buffers(scan);

	/*
	 * reinitialize scan descriptor
	 */
	initscan(scan, key);
}

/* ----------------
 *		heap_endscan	- end relation scan
 *
 *		See how to integrate with index scans.
 *		Check handling if reldesc caching.
 * ----------------
 */
void
heap_endscan(HeapScanDesc scan)
{
	/* Note: no locking manipulations needed */

	/*
	 * unpin scan buffers
	 */
	release_all_buffers(scan);
	/*
	 * decrement relation reference count and free scan descriptor storage
	 */
	RelationDecrementReferenceCount(scan->rs_rd);

	if (scan->rs_key)
		pfree(scan->rs_key);

	pfree(scan);
}

/* ----------------
 *		heap_getnext	- retrieve next tuple in scan
 *
 *		Fix to work with index relations.
 *		We don't return the buffer anymore, but you can get it from the
 *		returned HeapTuple.
 * ----------------
 */

#ifdef HEAPDEBUGALL
#define HEAPDEBUG_1 \
	elog(DEBUG2, "heap_getnext([%s,nkeys=%d],dir=%d) called", \
		 RelationGetRelationName(scan->rs_rd), scan->rs_nkeys, (int) direction)
#define HEAPDEBUG_2 \
	elog(DEBUG2, "heap_getnext returning EOS")
#define HEAPDEBUG_3 \
	elog(DEBUG2, "heap_getnext returning tuple")
#else
#define HEAPDEBUG_1
#define HEAPDEBUG_2
#define HEAPDEBUG_3
#endif   /* !defined(HEAPDEBUGALL) */




HeapTuple
heap_getnext(HeapScanDesc scan, ScanDirection direction)
{
	MIRROREDLOCK_BUFMGR_VERIFY_NO_LOCK_LEAK_DECLARE;

	/* Note: no locking manipulations needed */

	MIRROREDLOCK_BUFMGR_VERIFY_NO_LOCK_LEAK_ENTER;

	HEAPDEBUG_1;				/* heap_getnext( info ) */

	if (scan->rs_pageatatime)
		heapgettup_pagemode(scan, direction,
							scan->rs_nkeys, scan->rs_key);
	else
		heapgettup(scan, direction, scan->rs_nkeys, scan->rs_key);

	if (scan->rs_ctup.t_data == NULL)
	{
		release_all_buffers(scan);
		HEAPDEBUG_2;			/* heap_getnext returning EOS */

		MIRROREDLOCK_BUFMGR_VERIFY_NO_LOCK_LEAK_EXIT;
		
		return NULL;
	}

	/*
	 * if we get here it means we have a new current scan tuple, so point to
	 * the proper return buffer and return the tuple.
	 */
	HEAPDEBUG_3;				/* heap_getnext returning tuple */

	pgstat_count_heap_getnext(scan->rs_rd);

	MIRROREDLOCK_BUFMGR_VERIFY_NO_LOCK_LEAK_EXIT;

	return &(scan->rs_ctup);
}


/*
 *  heap_getnextx  - fill up tdata[] up to tdatacnt
 *
 *  Take all tuples from a page, and cram as much as possible into tdata[].
 *  tdatacnt - IN: size of tdata[]. OUT: number of elements filled in tdata[].
 *  EOS      - OUT: non-zero if end of stream reached.
 */
void
heap_getnextx(HeapScanDesc scan, ScanDirection dir,
			  HeapTupleData tdata[], int *tdatacnt,
			  int *seen_EOS)
{
	MIRROREDLOCK_BUFMGR_VERIFY_NO_LOCK_LEAK_DECLARE;

	int maxcnt = *tdatacnt;
	int retcnt = 0;
	HeapTuple t = NULL;
	bool backward = ScanDirectionIsBackward(dir);
	bool forward = ScanDirectionIsForward(dir);

	MIRROREDLOCK_BUFMGR_VERIFY_NO_LOCK_LEAK_ENTER;

	/*
	 *  go slow if 1. scan current again, or 2. not doing page at a time,
	 *    or 3. we have to qualify keys.
	 */
	bool onebyone = (!backward && !forward) || !scan->rs_pageatatime || scan->rs_nkeys;

	if (maxcnt <= 0)
	{
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("illegal value in tdatacnt")));
	}

	if (scan->rs_pageatatime)
		heapgettup_pagemode(scan, dir,
							scan->rs_nkeys, scan->rs_key);
	else
		heapgettup(scan, dir, scan->rs_nkeys, scan->rs_key);

	if (NULL == (t = scan->rs_ctup.t_data ? &scan->rs_ctup : 0))
	{
		release_all_buffers(scan);
		*tdatacnt = 0, *seen_EOS = 1;
		
		MIRROREDLOCK_BUFMGR_VERIFY_NO_LOCK_LEAK_EXIT;
		
		return;
	}

	tdata[retcnt++] = *t;
	if (!onebyone && retcnt < maxcnt)
	{
		HeapTuple tuple = &(scan->rs_ctup);
		int lineindex, linesleft;
		BlockNumber page = scan->rs_cblock;
		Page dp = (Page) BufferGetPage(scan->rs_cbuf);
		int lines = scan->rs_ntuples;

		/* go through all tuples in this page */
		if (forward)
		{
			lineindex = scan->rs_cindex + 1;
			linesleft = lines - lineindex;
		}
		else
		{
			lineindex = scan->rs_cindex - 1;
			linesleft = lineindex + 1;
		}

		while (linesleft > 0 && retcnt < maxcnt)
		{
			OffsetNumber lineoff = scan->rs_vistuples[lineindex];
			ItemId lpp = PageGetItemId(dp, lineoff);
			Assert(ItemIdIsUsed(lpp));

			tuple->t_data = (HeapTupleHeader) PageGetItem((Page) dp, lpp);
			tuple->t_len = ItemIdGetLength(lpp);
			ItemPointerSet(&(tuple->t_self), page, lineoff);
			scan->rs_cindex = lineindex;

			tdata[retcnt++] = *tuple;

			--linesleft;
			if (backward)
				--lineindex;
			else
				++lineindex;
		}

		/* if we get here, we processed all tuples on the page or we filled up
		 * tdata[].
		 */
	}

	*tdatacnt = retcnt, *seen_EOS = 0;

	MIRROREDLOCK_BUFMGR_VERIFY_NO_LOCK_LEAK_EXIT;

}


/*
 *	heap_fetch		- retrieve tuple with given tid
 *
 * On entry, tuple->t_self is the TID to fetch.  We pin the buffer holding
 * the tuple, fill in the remaining fields of *tuple, and check the tuple
 * against the specified snapshot.
 *
 * If successful (tuple found and passes snapshot time qual), then *userbuf
 * is set to the buffer holding the tuple and TRUE is returned.  The caller
 * must unpin the buffer when done with the tuple.
 *
 * If the tuple is not found (ie, item number references a deleted slot),
 * then tuple->t_data is set to NULL and FALSE is returned.
 *
 * If the tuple is found but fails the time qual check, then FALSE is returned
 * but tuple->t_data is left pointing to the tuple.
 *
 * keep_buf determines what is done with the buffer in the FALSE-result cases.
 * When the caller specifies keep_buf = true, we retain the pin on the buffer
 * and return it in *userbuf (so the caller must eventually unpin it); when
 * keep_buf = false, the pin is released and *userbuf is set to InvalidBuffer.
 *
 * It is somewhat inconsistent that we ereport() on invalid block number but
 * return false on invalid item number.  There are a couple of reasons though.
 * One is that the caller can relatively easily check the block number for
 * validity, but cannot check the item number without reading the page
 * himself.  Another is that when we are following a t_ctid link, we can be
 * reasonably confident that the page number is valid (since VACUUM shouldn't
 * truncate off the destination page without having killed the referencing
 * tuple first), but the item number might well not be good.
 */
bool
heap_fetch(Relation relation,
		   Snapshot snapshot,
		   HeapTuple tuple,
		   Buffer *userbuf,
		   bool keep_buf,
		   Relation stats_relation)
{
	MIRROREDLOCK_BUFMGR_VERIFY_NO_LOCK_LEAK_DECLARE;

	bool result;

	MIRROREDLOCK_BUFMGR_VERIFY_NO_LOCK_LEAK_ENTER;

	/* Assume *userbuf is undefined on entry */
	*userbuf = InvalidBuffer;
	result = heap_release_fetch(relation, snapshot, tuple,
							  userbuf, keep_buf, stats_relation);


	MIRROREDLOCK_BUFMGR_VERIFY_NO_LOCK_LEAK_EXIT;

	return result;
}

/*
 *	heap_release_fetch		- retrieve tuple with given tid
 *
 * This has the same API as heap_fetch except that if *userbuf is not
 * InvalidBuffer on entry, that buffer will be released before reading
 * the new page.  This saves a separate ReleaseBuffer step and hence
 * one entry into the bufmgr when looping through multiple fetches.
 * Also, if *userbuf is the same buffer that holds the target tuple,
 * we avoid bufmgr manipulation altogether.
 */
bool
heap_release_fetch(Relation relation,
				   Snapshot snapshot,
				   HeapTuple tuple,
				   Buffer *userbuf,
				   bool keep_buf,
				   Relation stats_relation)
{
	MIRROREDLOCK_BUFMGR_DECLARE;

	ItemPointer tid = &(tuple->t_self);
	ItemId		lp;
	Buffer		buffer;
	PageHeader	dp;
	OffsetNumber offnum;
	bool		valid;

	/*
	 * get the buffer from the relation descriptor. Note that this does a
	 * buffer pin, and releases the old *userbuf if not InvalidBuffer.
	 */
	
	// -------- MirroredLock ----------
	MIRROREDLOCK_BUFMGR_LOCK;
	
	buffer = ReleaseAndReadBuffer(*userbuf, relation,
								  ItemPointerGetBlockNumber(tid));

	/*
	 * Need share lock on buffer to examine tuple commit status.
	 */
	LockBuffer(buffer, BUFFER_LOCK_SHARE);
	dp = (PageHeader) BufferGetPage(buffer);

	/*
	 * We'd better check for out-of-range offnum in case of VACUUM since the
	 * TID was obtained.
	 */
	offnum = ItemPointerGetOffsetNumber(tid);
	if (offnum < FirstOffsetNumber || offnum > PageGetMaxOffsetNumber(dp))
	{
		LockBuffer(buffer, BUFFER_LOCK_UNLOCK);
		
		MIRROREDLOCK_BUFMGR_UNLOCK;
		// -------- MirroredLock ----------
		
		if (keep_buf)
			*userbuf = buffer;
		else
		{
			ReleaseBuffer(buffer);
			*userbuf = InvalidBuffer;
		}
		tuple->t_data = NULL;
		return false;
	}

	/*
	 * get the item line pointer corresponding to the requested tid
	 */
	lp = PageGetItemId(dp, offnum);

	/*
	 * Must check for deleted tuple.
	 */
	if (!ItemIdIsUsed(lp))
	{
		LockBuffer(buffer, BUFFER_LOCK_UNLOCK);
		
		MIRROREDLOCK_BUFMGR_UNLOCK;
		// -------- MirroredLock ----------
		
		if (keep_buf)
			*userbuf = buffer;
		else
		{
			ReleaseBuffer(buffer);
			*userbuf = InvalidBuffer;
		}
		tuple->t_data = NULL;
		return false;
	}

	/*
	 * fill in *tuple fields
	 */
	tuple->t_data = (HeapTupleHeader) PageGetItem((Page) dp, lp);
	tuple->t_len = ItemIdGetLength(lp);

	/*
	 * check time qualification of tuple, then release lock
	 */
	valid = HeapTupleSatisfiesVisibility(relation, tuple, snapshot, buffer);

#ifdef WATCH_VISIBILITY_IN_ACTION
	elog(LOG, "VISIBILITY(%s) %s %s",
	     (valid ? "true" : "false"),
	     RelationGetRelationName(relation),
	     WatchVisibilityInActionString(
	     			ItemPointerGetBlockNumber(tid),
	     			offnum,
	     			tuple,
	     			snapshot));
#endif

	LockBuffer(buffer, BUFFER_LOCK_UNLOCK);
	
	MIRROREDLOCK_BUFMGR_UNLOCK;
	// -------- MirroredLock ----------
	
	if (valid)
	{
		/*
		 * All checks passed, so return the tuple as valid. Caller is now
		 * responsible for releasing the buffer.
		 */
		*userbuf = buffer;

		/* Count the successful fetch against appropriate rel, if any */
		if (stats_relation != NULL)
			pgstat_count_heap_fetch(stats_relation);

		return true;
	}

	/* Tuple failed time qual, but maybe caller wants to see it anyway. */
	if (keep_buf)
		*userbuf = buffer;
	else
	{
		ReleaseBuffer(buffer);
		*userbuf = InvalidBuffer;
	}

	return false;
}

/*
 *	heap_get_latest_tid -  get the latest tid of a specified tuple
 *
 * Actually, this gets the latest version that is visible according to
 * the passed snapshot.  You can pass SnapshotDirty to get the very latest,
 * possibly uncommitted version.
 *
 * *tid is both an input and an output parameter: it is updated to
 * show the latest version of the row.	Note that it will not be changed
 * if no version of the row passes the snapshot test.
 */
void
heap_get_latest_tid(Relation relation,
					Snapshot snapshot,
					ItemPointer tid)
{
	MIRROREDLOCK_BUFMGR_DECLARE;

	BlockNumber blk;
	ItemPointerData ctid;
	TransactionId priorXmax;

	/* this is to avoid Assert failures on bad input */
	if (!ItemPointerIsValid(tid))
		return;

	/*
	 * Since this can be called with user-supplied TID, don't trust the input
	 * too much.  (RelationGetNumberOfBlocks is an expensive check, so we
	 * don't check t_ctid links again this way.  Note that it would not do to
	 * call it just once and save the result, either.)
	 */
	blk = ItemPointerGetBlockNumber(tid);
	if (blk >= RelationGetNumberOfBlocks(relation))
		elog(ERROR, "block number %u is out of range for relation \"%s\"",
			 blk, RelationGetRelationName(relation));

	/*
	 * Loop to chase down t_ctid links.  At top of loop, ctid is the tuple we
	 * need to examine, and *tid is the TID we will return if ctid turns out
	 * to be bogus.
	 *
	 * Note that we will loop until we reach the end of the t_ctid chain.
	 * Depending on the snapshot passed, there might be at most one visible
	 * version of the row, but we don't try to optimize for that.
	 */
	ctid = *tid;
	priorXmax = InvalidTransactionId;	/* cannot check first XMIN */
	for (;;)
	{
		Buffer		buffer;
		PageHeader	dp;
		OffsetNumber offnum;
		ItemId		lp;
		HeapTupleData tp;
		bool		valid;

		/*
		 * Read, pin, and lock the page.
		 */

		// -------- MirroredLock ----------
		MIRROREDLOCK_BUFMGR_LOCK;

		buffer = ReadBuffer(relation, ItemPointerGetBlockNumber(&ctid));
		LockBuffer(buffer, BUFFER_LOCK_SHARE);
		dp = (PageHeader) BufferGetPage(buffer);

		/*
		 * Check for bogus item number.  This is not treated as an error
		 * condition because it can happen while following a t_ctid link. We
		 * just assume that the prior tid is OK and return it unchanged.
		 */
		offnum = ItemPointerGetOffsetNumber(&ctid);
		if (offnum < FirstOffsetNumber || offnum > PageGetMaxOffsetNumber(dp))
		{
			UnlockReleaseBuffer(buffer);

			MIRROREDLOCK_BUFMGR_UNLOCK;
			// -------- MirroredLock ----------

			break;
		}
		lp = PageGetItemId(dp, offnum);
		if (!ItemIdIsUsed(lp))
		{
			UnlockReleaseBuffer(buffer);

			MIRROREDLOCK_BUFMGR_UNLOCK;
			// -------- MirroredLock ----------

			break;
		}

		/* OK to access the tuple */
		tp.t_self = ctid;
		tp.t_data = (HeapTupleHeader) PageGetItem(dp, lp);
		tp.t_len = ItemIdGetLength(lp);

		/*
		 * After following a t_ctid link, we might arrive at an unrelated
		 * tuple.  Check for XMIN match.
		 */
		if (TransactionIdIsValid(priorXmax) &&
		  !TransactionIdEquals(priorXmax, HeapTupleHeaderGetXmin(tp.t_data)))
		{
			UnlockReleaseBuffer(buffer);

			MIRROREDLOCK_BUFMGR_UNLOCK;
			// -------- MirroredLock ----------

			break;
		}

		/*
		 * Check time qualification of tuple; if visible, set it as the new
		 * result candidate.
		 */
		valid = HeapTupleSatisfiesVisibility(relation, &tp, snapshot, buffer);
#ifdef WATCH_VISIBILITY_IN_ACTION
		elog(LOG, "VISIBILITY(%s) %s %s",
		     (valid ? "true" : "false"),
		     RelationGetRelationName(relation),
		     WatchVisibilityInActionString(
		     			ItemPointerGetBlockNumber(&ctid),
		     			offnum,
		     			&tp,
		     			snapshot));
#endif
		if (valid)
			*tid = ctid;

		/*
		 * If there's a valid t_ctid link, follow it, else we're done.
		 */
		if ((tp.t_data->t_infomask & (HEAP_XMAX_INVALID | HEAP_IS_LOCKED)) ||
			ItemPointerEquals(&tp.t_self, &tp.t_data->t_ctid))
		{
			UnlockReleaseBuffer(buffer);

			MIRROREDLOCK_BUFMGR_UNLOCK;
			// -------- MirroredLock ----------

			break;
		}

		ctid = tp.t_data->t_ctid;
		priorXmax = HeapTupleHeaderGetXmax(tp.t_data);
		UnlockReleaseBuffer(buffer);
		
		MIRROREDLOCK_BUFMGR_UNLOCK;
		// -------- MirroredLock ----------
		
	}							/* end of loop */
}

/*
 * MUST BE CALLED IN CRITICAL SECTION!
 */
static void
heap_log_tuple_insert(Relation rel, Buffer buffer, HeapTuple tup, bool isFrozen)
{
	xl_heap_insert xlrec;
	xl_heap_header xlhdr;
	XLogRecPtr	recptr;
	XLogRecData rdata[3];
	Page		page = BufferGetPage(buffer);
	uint8		info = XLOG_HEAP_INSERT;

	Assert(CritSectionCount > 0);

	xl_heaptid_set(&xlrec.target, rel, &tup->t_self);
	rdata[0].data = (char *) &xlrec;
	rdata[0].len = SizeOfHeapInsert;
	rdata[0].buffer = InvalidBuffer;
	rdata[0].next = &(rdata[1]);

	xlhdr.t_infomask2 = tup->t_data->t_infomask2;
	xlhdr.t_infomask = tup->t_data->t_infomask;
	xlhdr.t_hoff = tup->t_data->t_hoff;

	/*
	 * note we mark rdata[1] as belonging to buffer; if XLogInsert decides
	 * to write the whole page to the xlog, we don't need to store
	 * xl_heap_header in the xlog.
	 */
	rdata[1].data = (char *) &xlhdr;
	rdata[1].len = SizeOfHeapHeader;
	rdata[1].buffer = buffer;
	rdata[1].buffer_std = true;
	rdata[1].next = &(rdata[2]);

	/* PG73FORMAT: write bitmap [+ padding] [+ oid] + data */
	rdata[2].data = (char *) tup->t_data + offsetof(HeapTupleHeaderData, t_bits);
	rdata[2].len = tup->t_len - offsetof(HeapTupleHeaderData, t_bits);
	rdata[2].buffer = buffer;
	rdata[2].buffer_std = true;
	rdata[2].next = NULL;

	/*
	 * If this is the single and first tuple on page, we can reinit the
	 * page instead of restoring the whole thing.  Set flag, and hide
	 * buffer references from XLogInsert.
	 */
	if (ItemPointerGetOffsetNumber(&(tup->t_self)) == FirstOffsetNumber &&
		PageGetMaxOffsetNumber(page) == FirstOffsetNumber)
	{
		info |= XLOG_HEAP_INIT_PAGE;
		rdata[1].buffer = rdata[2].buffer = InvalidBuffer;
	}

	if (!isFrozen)
		recptr = XLogInsert(RM_HEAP_ID, info, rdata);
	else
		recptr = XLogInsert_OverrideXid(RM_HEAP_ID, info, rdata, FrozenTransactionId);

	PageSetLSN(page, recptr);
	PageSetTLI(page, ThisTimeLineID);
}


/*
 *	heap_insert		- insert tuple into a heap
 *
 * The new tuple is stamped with current transaction ID and the specified
 * command ID.
 *
 * If use_wal is false, the new tuple is not logged in WAL, even for a
 * non-temp relation.  Safe usage of this behavior requires that we arrange
 * that all new tuples go into new pages not containing any tuples from other
 * transactions, that the relation gets fsync'd before commit, and that the
 * transaction emits at least one WAL record to ensure RecordTransactionCommit
 * will decide to WAL-log the commit.
 *
 * use_fsm is passed directly to RelationGetBufferForTuple, which see for
 * more info.
 *
 * The return value is the OID assigned to the tuple (either here or by the
 * caller), or InvalidOid if no OID.  The header fields of *tup are updated
 * to match the stored tuple; in particular tup->t_self receives the actual
 * TID where the tuple was stored.	But note that any toasting of fields
 * within the tuple data is NOT reflected into *tup.
 */
Oid
heap_insert(Relation relation, HeapTuple tup, CommandId cid,
			bool use_wal, bool use_fsm, TransactionId xid)
{

	if (Gp_role == GP_ROLE_EXECUTE && !gp_upgrade_mode)
		elog(ERROR, "heap_insert() is not allowed on segments");

	MIRROREDLOCK_BUFMGR_DECLARE;

	bool		isFrozen = (xid == FrozenTransactionId);
	HeapTuple	heaptup;
	Buffer		buffer;

	Insist(RelationIsHeap(relation));

	// Fetch gp_persistent_relation_node information that will be added to XLOG record.
	RelationFetchGpRelationNodeForXLog(relation);

	if (relation->rd_rel->relhasoids)
	{
#ifdef NOT_USED
		/* this is redundant with an Assert in HeapTupleSetOid */
		Assert(tup->t_data->t_infomask & HEAP_HASOID);
#endif

		/*
		 * If the object id of this tuple has already been assigned, trust the
		 * caller.	There are a couple of ways this can happen.  At initial db
		 * creation, the backend program sets oids for tuples. When we define
		 * an index, we set the oid.  Finally, in the future, we may allow
		 * users to set their own object ids in order to support a persistent
		 * object store (objects need to contain pointers to one another).
		 */
		if (!OidIsValid(HeapTupleGetOid(tup)))
			HeapTupleSetOid(tup, GetNewOid(relation));
	}
	else
	{
 		/* Check there is not space for an OID, since pgclass.relhasoids says
 		 * there shouldn't be one.  The hidden "escape hatch" GUC is here so 
 		 * that we can revert to the old (no error) behavior in the unlikely
 		 * event of an emergency in the field.
		 */
 		if ( tup->t_data->t_infomask & HEAP_HASOID && gp_heap_require_relhasoids_match )
 		{
 			elog(ERROR, "tuple has oid, but schema does not");
 		}
	}

	tup->t_data->t_infomask &= ~(HEAP_XACT_MASK);
	if (isFrozen)
		tup->t_data->t_infomask |= HEAP_XMIN_COMMITTED;
	tup->t_data->t_infomask |= HEAP_XMAX_INVALID;
	HeapTupleHeaderSetXmin(tup->t_data, xid);
	HeapTupleHeaderSetCmin(tup->t_data, cid);
	HeapTupleHeaderSetXmax(tup->t_data, 0);		/* for cleanliness */

	/*
	 * If the new tuple is too big for storage or contains already toasted
	 * out-of-line attributes from some other relation, invoke the toaster.
	 *
	 * Note: below this point, heaptup is the data we actually intend to store
	 * into the relation; tup is the caller's original untoasted data.
	 */
	if (HeapTupleHasExternal(tup) ||
		(MAXALIGN(tup->t_len) > TOAST_TUPLE_THRESHOLD))
		heaptup = toast_insert_or_update(relation, tup, NULL, NULL, TOAST_TUPLE_TARGET, isFrozen);
	else
		heaptup = tup;
	
	// -------- MirroredLock ----------
	MIRROREDLOCK_BUFMGR_LOCK;
	
	/* Find buffer to insert this tuple into */
	buffer = RelationGetBufferForTuple(relation, heaptup->t_len,
									   InvalidBuffer, use_fsm);
	START_CRIT_SECTION();

	RelationPutHeapTuple(relation, buffer, heaptup);

	MarkBufferDirty(buffer);

	/* XLOG stuff */
	if (relation->rd_istemp)
	{
		/* No XLOG record, but still need to flag that XID exists on disk */
		MyXactMadeTempRelUpdate = true;
	}
	else if (use_wal)
	{
		heap_log_tuple_insert(relation, buffer, heaptup, isFrozen);
	}

	END_CRIT_SECTION();

	UnlockReleaseBuffer(buffer);
	
	MIRROREDLOCK_BUFMGR_UNLOCK;
	// -------- MirroredLock ----------
	
	/*
	 * If tuple is cachable, mark it for invalidation from the caches in case
	 * we abort.  Note it is OK to do this after releasing the buffer, because
	 * the heaptup data structure is all in local memory, not in the shared
	 * buffer.
	 */
	CacheInvalidateHeapTuple(relation, heaptup, SysCacheInvalidate_Insert);

	pgstat_count_heap_insert(relation);

	/*
	 * If heaptup is a private copy, release it.  Don't forget to copy t_self
	 * back to the caller's image, too.
	 */
	if (heaptup != tup)
	{
		tup->t_self = heaptup->t_self;
		heap_freetuple(heaptup);
	}

	return HeapTupleGetOid(tup);
}

/*
 *	simple_heap_insert - insert a tuple
 *
 * Currently, this routine differs from heap_insert only in supplying
 * a default command ID.  But it should be used rather than using
 * heap_insert directly in most places where we are modifying system catalogs.
 */
Oid
simple_heap_insert(Relation relation, HeapTuple tup)
{
	MIRROREDLOCK_BUFMGR_VERIFY_NO_LOCK_LEAK_DECLARE;

	Oid result;

	MIRROREDLOCK_BUFMGR_VERIFY_NO_LOCK_LEAK_ENTER;

	result = heap_insert(relation, tup, GetCurrentCommandId(),
					   true, true, GetCurrentTransactionId());

	MIRROREDLOCK_BUFMGR_VERIFY_NO_LOCK_LEAK_EXIT;

	return result;
}

/*
 *	frozen_heap_insert - insert a tuple and freeze it (always visible).
 *
 * Currently, this routine differs from heap_insert in supplying
 * a default command ID and a frozen transaction id. Also, the committed
 * bit of the tuple is set. This function is currently used for data that
 * goes into error tables and need to stay there even if transaction
 * aborts.
 */
Oid
frozen_heap_insert(Relation relation, HeapTuple tup)
{
	if (Gp_role == GP_ROLE_EXECUTE && !gp_upgrade_mode)
		elog(ERROR, "frozen_heap_insert() is not allowed on segments");

	MIRROREDLOCK_BUFMGR_VERIFY_NO_LOCK_LEAK_DECLARE;

	Oid result;
	
	MIRROREDLOCK_BUFMGR_VERIFY_NO_LOCK_LEAK_ENTER;

	result = heap_insert(relation, tup, GetCurrentCommandId(),
					   true, true, FrozenTransactionId);

	MIRROREDLOCK_BUFMGR_VERIFY_NO_LOCK_LEAK_EXIT;

	return result;
}

Oid
frozen_heap_insert_directed(
	Relation 		relation, 
	HeapTuple 		tup, 
	BlockNumber 	blockNum)
{
	if (Gp_role == GP_ROLE_EXECUTE && !gp_upgrade_mode)
		elog(ERROR, "frozen_heap_insert_directed() is not allowed on segments");

	MIRROREDLOCK_BUFMGR_VERIFY_NO_LOCK_LEAK_DECLARE;

	Oid result;

	MIRROREDLOCK_BUFMGR_VERIFY_NO_LOCK_LEAK_ENTER;

	relation->rd_targblock = blockNum;
	result = heap_insert(relation, tup, GetCurrentCommandId(),
					   /* use_wal */ true, /* use_fsm */ false, 
					   FrozenTransactionId);

	MIRROREDLOCK_BUFMGR_VERIFY_NO_LOCK_LEAK_EXIT;

	return result;
}

/*
 * heap_trace_current_tuple
 *
 * Log a line showing the current tuple MVCC information.
 */
static void
heap_trace_current_tuple(
	char		*caller,

	HeapTuple	tuple)
{
	TupleVisibilitySummary tupleVisibilitySummary;
	char *summary;

	GetTupleVisibilitySummary(
							tuple,
							&tupleVisibilitySummary);
	summary = GetTupleVisibilitySummaryString(&tupleVisibilitySummary);

	elog(LOG, "Current tuple for %s: %s",
		 caller,
		 summary);

	pfree(summary);
}

/*
 *	heap_delete - delete a tuple
 *
 * NB: do not call this directly unless you are prepared to deal with
 * concurrent-update conditions.  Use simple_heap_delete instead.
 *
 *	relation - table to be modified (caller must hold suitable lock)
 *	tid - TID of tuple to be deleted
 *	ctid - output parameter, used only for failure case (see below)
 *	update_xmax - output parameter, used only for failure case (see below)
 *	cid - delete command ID (used for visibility test, and stored into
 *		cmax if successful)
 *	crosscheck - if not InvalidSnapshot, also check tuple against this
 *	wait - true if should wait for any conflicting update to commit/abort
 *
 * Normal, successful return value is HeapTupleMayBeUpdated, which
 * actually means we did delete it.  Failure return codes are
 * HeapTupleSelfUpdated, HeapTupleUpdated, or HeapTupleBeingUpdated
 * (the last only possible if wait == false).
 *
 * In the failure cases, the routine returns the tuple's t_ctid and t_xmax.
 * If t_ctid is the same as tid, the tuple was deleted; if different, the
 * tuple was updated, and t_ctid is the location of the replacement tuple.
 * (t_xmax is needed to verify that the replacement tuple matches.)
 */
HTSU_Result
heap_delete(Relation relation, ItemPointer tid,
			ItemPointer ctid, TransactionId *update_xmax,
			CommandId cid, Snapshot crosscheck, bool wait)
{

	/*
	 * in hawq, heap table only used for catalog on master.
	 */
	if (Gp_role == GP_ROLE_EXECUTE && !gp_upgrade_mode )
		elog(ERROR, "heap_delete() is not allowed on segments");

	MIRROREDLOCK_BUFMGR_DECLARE;

	HTSU_Result result;
	TransactionId xid = GetCurrentTransactionId();
	ItemId		lp;
	HeapTupleData tp;
	PageHeader	dp;
	Buffer		buffer;
	bool		have_tuple_lock = false;
	bool		iscombo;

	Assert(ItemPointerIsValid(tid));

	if (RelationIsAoRows(relation) || RelationIsParquet(relation))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("Append-only tables are not updatable. Operation not permitted."),
				 errOmitLocation(true)));

	// Fetch gp_persistent_relation_node information that will be added to XLOG record.
	RelationFetchGpRelationNodeForXLog(relation);
	
	// -------- MirroredLock ----------
	MIRROREDLOCK_BUFMGR_LOCK;
	
	buffer = ReadBuffer(relation, ItemPointerGetBlockNumber(tid));
	LockBuffer(buffer, BUFFER_LOCK_EXCLUSIVE);

	dp = (PageHeader) BufferGetPage(buffer);
	lp = PageGetItemId(dp, ItemPointerGetOffsetNumber(tid));

	tp.t_data = (HeapTupleHeader) PageGetItem(dp, lp);
	tp.t_len = ItemIdGetLength(lp);
	tp.t_self = *tid;

l1:
	result = HeapTupleSatisfiesUpdate(relation, tp.t_data, cid, buffer);

	if (result == HeapTupleInvisible)
	{
		UnlockReleaseBuffer(buffer);
		
		MIRROREDLOCK_BUFMGR_UNLOCK;
		// -------- MirroredLock ----------
		
		elog(ERROR, "attempted to delete invisible tuple");
	}
	else if (result == HeapTupleBeingUpdated && wait)
	{
		TransactionId xwait;
		uint16		infomask;

		/* must copy state data before unlocking buffer */
		xwait = HeapTupleHeaderGetXmax(tp.t_data);
		Assert(TransactionIdIsValid(xid));

		infomask = tp.t_data->t_infomask;

		LockBuffer(buffer, BUFFER_LOCK_UNLOCK);
		
		MIRROREDLOCK_BUFMGR_UNLOCK;
		// -------- MirroredLock ----------
		
		/*
		 * Acquire tuple lock to establish our priority for the tuple (see
		 * heap_lock_tuple).  LockTuple will release us when we are
		 * next-in-line for the tuple.
		 *
		 * If we are forced to "start over" below, we keep the tuple lock;
		 * this arranges that we stay at the head of the line while rechecking
		 * tuple state.
		 */
		if (!have_tuple_lock)
		{
			LockTuple(relation, &(tp.t_self), ExclusiveLock);
			have_tuple_lock = true;
		}

		/*
		 * Sleep until concurrent transaction ends.  Note that we don't care
		 * if the locker has an exclusive or shared lock, because we need
		 * exclusive.
		 */

		if (infomask & HEAP_XMAX_IS_MULTI)
		{
			/* wait for multixact */
			MultiXactIdWait((MultiXactId) xwait);
			
			// -------- MirroredLock ----------
			MIRROREDLOCK_BUFMGR_LOCK;
			
			LockBuffer(buffer, BUFFER_LOCK_EXCLUSIVE);

			/*
			 * If xwait had just locked the tuple then some other xact could
			 * update this tuple before we get to this point.  Check for xmax
			 * change, and start over if so.
			 */
			if (!(tp.t_data->t_infomask & HEAP_XMAX_IS_MULTI) ||
				!TransactionIdEquals(HeapTupleHeaderGetXmax(tp.t_data),
									 xwait))
				goto l1;

			/*
			 * You might think the multixact is necessarily done here, but not
			 * so: it could have surviving members, namely our own xact or
			 * other subxacts of this backend.	It is legal for us to delete
			 * the tuple in either case, however (the latter case is
			 * essentially a situation of upgrading our former shared lock to
			 * exclusive).	We don't bother changing the on-disk hint bits
			 * since we are about to overwrite the xmax altogether.
			 */
		}
		else
		{
			/* wait for regular transaction to end */
			XactLockTableWait(xwait);
			
			// -------- MirroredLock ----------
			MIRROREDLOCK_BUFMGR_LOCK;
			
			LockBuffer(buffer, BUFFER_LOCK_EXCLUSIVE);

			/*
			 * xwait is done, but if xwait had just locked the tuple then some
			 * other xact could update this tuple before we get to this point.
			 * Check for xmax change, and start over if so.
			 */
			if ((tp.t_data->t_infomask & HEAP_XMAX_IS_MULTI) ||
				!TransactionIdEquals(HeapTupleHeaderGetXmax(tp.t_data),
									 xwait))
				goto l1;

			/* Otherwise we can mark it committed or aborted */
			if (!(tp.t_data->t_infomask & (HEAP_XMAX_COMMITTED |
										   HEAP_XMAX_INVALID)))
			{
				if (TransactionIdDidCommit(xwait))
					tp.t_data->t_infomask |= HEAP_XMAX_COMMITTED;
				else
					tp.t_data->t_infomask |= HEAP_XMAX_INVALID;
				SetBufferCommitInfoNeedsSave(buffer);
			}
		}

		/*
		 * We may overwrite if previous xmax aborted, or if it committed but
		 * only locked the tuple without updating it.
		 */
		if (tp.t_data->t_infomask & (HEAP_XMAX_INVALID |
									 HEAP_IS_LOCKED))
			result = HeapTupleMayBeUpdated;
		else
			result = HeapTupleUpdated;
	}

	if (crosscheck != InvalidSnapshot && result == HeapTupleMayBeUpdated)
	{
		/* Perform additional check for serializable RI updates */
		if (!HeapTupleSatisfiesSnapshot(relation, tp.t_data, crosscheck, buffer))
			result = HeapTupleUpdated;
	}

	if (result != HeapTupleMayBeUpdated)
	{
		Assert(result == HeapTupleSelfUpdated ||
			   result == HeapTupleUpdated ||
			   result == HeapTupleBeingUpdated);
		Assert(!(tp.t_data->t_infomask & HEAP_XMAX_INVALID));
		*ctid = tp.t_data->t_ctid;
		*update_xmax = HeapTupleHeaderGetXmax(tp.t_data);
		UnlockReleaseBuffer(buffer);
		
		MIRROREDLOCK_BUFMGR_UNLOCK;
		// -------- MirroredLock ----------
		
		if (have_tuple_lock)
			UnlockTuple(relation, &(tp.t_self), ExclusiveLock);
		return result;
	}

	/* replace cid with a combo cid if necessary */
	HeapTupleHeaderAdjustCmax(tp.t_data, &cid, &iscombo);

	START_CRIT_SECTION();

	/* store transaction information of xact deleting the tuple */
	tp.t_data->t_infomask &= ~(HEAP_XMAX_COMMITTED |
							   HEAP_XMAX_INVALID |
							   HEAP_XMAX_IS_MULTI |
							   HEAP_IS_LOCKED |
							   HEAP_MOVED);
	HeapTupleHeaderSetXmax(tp.t_data, xid);
	HeapTupleHeaderSetCmax(tp.t_data, cid, iscombo);
	/* Make sure there is no forward chain link in t_ctid */
	tp.t_data->t_ctid = tp.t_self;

	MarkBufferDirty(buffer);

	/* XLOG stuff */
	if (!relation->rd_istemp)
	{
		xl_heap_delete xlrec;
		XLogRecPtr	recptr;
		XLogRecData rdata[2];

		xl_heaptid_set(&xlrec.target, relation, &tp.t_self);
		rdata[0].data = (char *) &xlrec;
		rdata[0].len = SizeOfHeapDelete;
		rdata[0].buffer = InvalidBuffer;
		rdata[0].next = &(rdata[1]);

		rdata[1].data = NULL;
		rdata[1].len = 0;
		rdata[1].buffer = buffer;
		rdata[1].buffer_std = true;
		rdata[1].next = NULL;

		recptr = XLogInsert(RM_HEAP_ID, XLOG_HEAP_DELETE, rdata);

		PageSetLSN(dp, recptr);
		PageSetTLI(dp, ThisTimeLineID);
	}
	else
	{
		/* No XLOG record, but still need to flag that XID exists on disk */
		MyXactMadeTempRelUpdate = true;
	}

	END_CRIT_SECTION();

	LockBuffer(buffer, BUFFER_LOCK_UNLOCK);
	
	MIRROREDLOCK_BUFMGR_UNLOCK;
	// -------- MirroredLock ----------
	
	/*
	 * If the tuple has toasted out-of-line attributes, we need to delete
	 * those items too.  We have to do this before releasing the buffer
	 * because we need to look at the contents of the tuple, but it's OK to
	 * release the content lock on the buffer first.
	 */
	if (HeapTupleHasExternal(&tp))
		toast_delete(relation, &tp);

	/*
	 * Mark tuple for invalidation from system caches at next command
	 * boundary. We have to do this before releasing the buffer because we
	 * need to look at the contents of the tuple.
	 */
	CacheInvalidateHeapTuple(relation, &tp, SysCacheInvalidate_Delete);

	/* Now we can release the buffer */
	ReleaseBuffer(buffer);

	/*
	 * Release the lmgr tuple lock, if we had it.
	 */
	if (have_tuple_lock)
		UnlockTuple(relation, &(tp.t_self), ExclusiveLock);

	pgstat_count_heap_delete(relation);

	return HeapTupleMayBeUpdated;
}

/*
 *	simple_heap_delete - delete a tuple
 *
 * This routine may be used to delete a tuple when concurrent updates of
 * the target tuple are not expected (for example, because we have a lock
 * on the relation associated with the tuple).	Any failure is reported
 * via ereport().
 */
void
simple_heap_delete(Relation relation, ItemPointer tid)
{
	MIRROREDLOCK_BUFMGR_VERIFY_NO_LOCK_LEAK_DECLARE;

	HTSU_Result result;
	ItemPointerData update_ctid;
	TransactionId update_xmax;

	MIRROREDLOCK_BUFMGR_VERIFY_NO_LOCK_LEAK_ENTER;

	result = heap_delete(relation, tid,
						 &update_ctid, &update_xmax,
						 GetCurrentCommandId(), InvalidSnapshot,
						 true /* wait for commit */ );
	switch (result)
	{
		case HeapTupleSelfUpdated:
			/* Tuple was already updated in current command? */
			elog(ERROR, "tuple already updated by self");
			break;

		case HeapTupleMayBeUpdated:
			/* done successfully */
			break;

		case HeapTupleUpdated:
			elog(ERROR, "tuple concurrently updated");
			break;

		default:
			elog(ERROR, "unrecognized heap_delete status: %u", result);
			break;
	}
	
	MIRROREDLOCK_BUFMGR_VERIFY_NO_LOCK_LEAK_EXIT;

}

static HTSU_Result
heap_update_internal(Relation relation, ItemPointer otid, HeapTuple newtup,
			ItemPointer ctid, TransactionId *update_xmax,
			CommandId cid, Snapshot crosscheck, bool wait, bool simple)
{

	if (Gp_role == GP_ROLE_EXECUTE && !gp_upgrade_mode)
		elog(ERROR, "heap_update_internal() is not allowed on segments");

	MIRROREDLOCK_BUFMGR_DECLARE;

	HTSU_Result result;
	TransactionId xid = GetCurrentTransactionId();
	ItemId		lp;
	HeapTupleData oldtup;
	HeapTuple	heaptup;
	PageHeader	dp;
	Buffer		buffer,
				newbuf;
	bool		need_toast,
				already_marked;
	Size		newtupsize,
				pagefree;
	bool		have_tuple_lock = false;
	bool		iscombo;

	// Fetch gp_persistent_relation_node information that will be added to XLOG record.
	RelationFetchGpRelationNodeForXLog(relation);

	Assert(ItemPointerIsValid(otid));

	if (RelationIsAoRows(relation) || RelationIsParquet(relation))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("Append-only tables are not updatable. Operation not permitted."),
				 errOmitLocation(true)));
	
	// -------- MirroredLock ----------
	MIRROREDLOCK_BUFMGR_LOCK;
	
	buffer = ReadBuffer(relation, ItemPointerGetBlockNumber(otid));
	LockBuffer(buffer, BUFFER_LOCK_EXCLUSIVE);

	dp = (PageHeader) BufferGetPage(buffer);
	lp = PageGetItemId(dp, ItemPointerGetOffsetNumber(otid));

	oldtup.t_data = (HeapTupleHeader) PageGetItem(dp, lp);
	oldtup.t_len = ItemIdGetLength(lp);
	oldtup.t_self = *otid;

	/*
	 * Note: beyond this point, use oldtup not otid to refer to old tuple.
	 * otid may very well point at newtup->t_self, which we will overwrite
	 * with the new tuple's location, so there's great risk of confusion if we
	 * use otid anymore.
	 */

l2:
	result = HeapTupleSatisfiesUpdate(relation, oldtup.t_data, cid, buffer);

	if (result == HeapTupleInvisible)
	{
		/* Trace current tuple information before we unlock the buffer */
		heap_trace_current_tuple("heap_update", &oldtup);
		
		UnlockReleaseBuffer(buffer);
		elog(ERROR, "attempted to update invisible tuple");
	}
	else if (result == HeapTupleBeingUpdated && wait)
	{
		TransactionId xwait;
		uint16		infomask;

		/* must copy state data before unlocking buffer */
		xwait = HeapTupleHeaderGetXmax(oldtup.t_data);
		Assert(TransactionIdIsValid(xid));

		infomask = oldtup.t_data->t_infomask;

		LockBuffer(buffer, BUFFER_LOCK_UNLOCK);
		
		MIRROREDLOCK_BUFMGR_UNLOCK;
		// -------- MirroredLock ----------
		
		/*
		 * Acquire tuple lock to establish our priority for the tuple (see
		 * heap_lock_tuple).  LockTuple will release us when we are
		 * next-in-line for the tuple.
		 *
		 * If we are forced to "start over" below, we keep the tuple lock;
		 * this arranges that we stay at the head of the line while rechecking
		 * tuple state.
		 */
		if (!have_tuple_lock)
		{
			LockTuple(relation, &(oldtup.t_self), ExclusiveLock);
			have_tuple_lock = true;
		}

		/*
		 * Sleep until concurrent transaction ends.  Note that we don't care
		 * if the locker has an exclusive or shared lock, because we need
		 * exclusive.
		 */

		if (infomask & HEAP_XMAX_IS_MULTI)
		{
			/* wait for multixact */
			MultiXactIdWait((MultiXactId) xwait);
			
			// -------- MirroredLock ----------
			MIRROREDLOCK_BUFMGR_LOCK;
			
			LockBuffer(buffer, BUFFER_LOCK_EXCLUSIVE);

			/*
			 * If xwait had just locked the tuple then some other xact could
			 * update this tuple before we get to this point.  Check for xmax
			 * change, and start over if so.
			 */
			if (!(oldtup.t_data->t_infomask & HEAP_XMAX_IS_MULTI) ||
				!TransactionIdEquals(HeapTupleHeaderGetXmax(oldtup.t_data),
									 xwait))
				goto l2;

			/*
			 * You might think the multixact is necessarily done here, but not
			 * so: it could have surviving members, namely our own xact or
			 * other subxacts of this backend.	It is legal for us to update
			 * the tuple in either case, however (the latter case is
			 * essentially a situation of upgrading our former shared lock to
			 * exclusive).	We don't bother changing the on-disk hint bits
			 * since we are about to overwrite the xmax altogether.
			 */
		}
		else
		{
			/* wait for regular transaction to end */
			XactLockTableWait(xwait);
			
			// -------- MirroredLock ----------
			MIRROREDLOCK_BUFMGR_LOCK;
			
			LockBuffer(buffer, BUFFER_LOCK_EXCLUSIVE);

			/*
			 * xwait is done, but if xwait had just locked the tuple then some
			 * other xact could update this tuple before we get to this point.
			 * Check for xmax change, and start over if so.
			 */
			if ((oldtup.t_data->t_infomask & HEAP_XMAX_IS_MULTI) ||
				!TransactionIdEquals(HeapTupleHeaderGetXmax(oldtup.t_data),
									 xwait))
				goto l2;

			/* Otherwise we can mark it committed or aborted */
			if (!(oldtup.t_data->t_infomask & (HEAP_XMAX_COMMITTED |
											   HEAP_XMAX_INVALID)))
			{
				if (TransactionIdDidCommit(xwait))
					oldtup.t_data->t_infomask |= HEAP_XMAX_COMMITTED;
				else
					oldtup.t_data->t_infomask |= HEAP_XMAX_INVALID;
				SetBufferCommitInfoNeedsSave(buffer);
			}
		}

		/*
		 * We may overwrite if previous xmax aborted, or if it committed but
		 * only locked the tuple without updating it.
		 */
		if (oldtup.t_data->t_infomask & (HEAP_XMAX_INVALID |
										 HEAP_IS_LOCKED))
			result = HeapTupleMayBeUpdated;
		else
			result = HeapTupleUpdated;
	}

	if (crosscheck != InvalidSnapshot && result == HeapTupleMayBeUpdated)
	{
		/* Perform additional check for serializable RI updates */
		if (!HeapTupleSatisfiesSnapshot(relation, oldtup.t_data, crosscheck, buffer))
			result = HeapTupleUpdated;
	}

	if (result != HeapTupleMayBeUpdated)
	{
		Assert(result == HeapTupleSelfUpdated ||
			   result == HeapTupleUpdated ||
			   result == HeapTupleBeingUpdated);
		Assert(!(oldtup.t_data->t_infomask & HEAP_XMAX_INVALID));
		*ctid = oldtup.t_data->t_ctid;
		*update_xmax = HeapTupleHeaderGetXmax(oldtup.t_data);

		if (simple)
		{
			/* Trace current tuple information before we unlock the buffer */
			heap_trace_current_tuple("heap_update", &oldtup);
		}
		UnlockReleaseBuffer(buffer);
		
		MIRROREDLOCK_BUFMGR_UNLOCK;
		// -------- MirroredLock ----------
		
		if (have_tuple_lock)
			UnlockTuple(relation, &(oldtup.t_self), ExclusiveLock);
		return result;
	}

	/* Fill in OID and transaction status data for newtup */
	if (relation->rd_rel->relhasoids)
	{
#ifdef NOT_USED
		/* this is redundant with an Assert in HeapTupleSetOid */
		Assert(newtup->t_data->t_infomask & HEAP_HASOID);
#endif
		HeapTupleSetOid(newtup, HeapTupleGetOid(&oldtup));
	}
	else
	{
		/* check there is not space for an OID */
		Assert(!(newtup->t_data->t_infomask & HEAP_HASOID));
	}

	newtup->t_data->t_infomask &= ~(HEAP_XACT_MASK);
	newtup->t_data->t_infomask |= (HEAP_XMAX_INVALID | HEAP_UPDATED);
	HeapTupleHeaderSetXmin(newtup->t_data, xid);
	HeapTupleHeaderSetCmin(newtup->t_data, cid);
	HeapTupleHeaderSetXmax(newtup->t_data, 0);	/* for cleanliness */
	//newtup->t_tableOid = RelationGetRelid(relation);

	/*
	 * Replace cid with a combo cid if necessary.  Note that we already put
	 * the plain cid into the new tuple.
	 */
	HeapTupleHeaderAdjustCmax(oldtup.t_data, &cid, &iscombo);

	/*
	 * If the toaster needs to be activated, OR if the new tuple will not fit
	 * on the same page as the old, then we need to release the content lock
	 * (but not the pin!) on the old tuple's buffer while we are off doing
	 * TOAST and/or table-file-extension work.	We must mark the old tuple to
	 * show that it's already being updated, else other processes may try to
	 * update it themselves.
	 *
	 * We need to invoke the toaster if there are already any out-of-line
	 * toasted values present, or if the new tuple is over-threshold.
	 */
	newtupsize = MAXALIGN(newtup->t_len);

	need_toast = (HeapTupleHasExternal(&oldtup) ||
				  HeapTupleHasExternal(newtup) ||
				  newtupsize > TOAST_TUPLE_THRESHOLD);

	pagefree = PageGetFreeSpace((Page) dp);

	if (need_toast || newtupsize > pagefree)
	{
		oldtup.t_data->t_infomask &= ~(HEAP_XMAX_COMMITTED |
									   HEAP_XMAX_INVALID |
									   HEAP_XMAX_IS_MULTI |
									   HEAP_IS_LOCKED |
									   HEAP_MOVED);
		HeapTupleHeaderSetXmax(oldtup.t_data, xid);
		HeapTupleHeaderSetCmax(oldtup.t_data, cid, iscombo);
		/* temporarily make it look not-updated */
		oldtup.t_data->t_ctid = oldtup.t_self;
		already_marked = true;
		LockBuffer(buffer, BUFFER_LOCK_UNLOCK);
		
		MIRROREDLOCK_BUFMGR_UNLOCK;
		// -------- MirroredLock ----------
		
		/*
		 * Let the toaster do its thing, if needed.
		 *
		 * Note: below this point, heaptup is the data we actually intend to
		 * store into the relation; newtup is the caller's original untoasted
		 * data.
		 */
		if (need_toast)
		{
			heaptup = toast_insert_or_update(relation, newtup, &oldtup, NULL, TOAST_TUPLE_TARGET, false);
			newtupsize = MAXALIGN(heaptup->t_len);
		}
		else
			heaptup = newtup;

		/*
		 * Now, do we need a new page for the tuple, or not?  This is a bit
		 * tricky since someone else could have added tuples to the page while
		 * we weren't looking.  We have to recheck the available space after
		 * reacquiring the buffer lock.  But don't bother to do that if the
		 * former amount of free space is still not enough; it's unlikely
		 * there's more free now than before.
		 *
		 * What's more, if we need to get a new page, we will need to acquire
		 * buffer locks on both old and new pages.	To avoid deadlock against
		 * some other backend trying to get the same two locks in the other
		 * order, we must be consistent about the order we get the locks in.
		 * We use the rule "lock the lower-numbered page of the relation
		 * first".  To implement this, we must do RelationGetBufferForTuple
		 * while not holding the lock on the old page, and we must rely on it
		 * to get the locks on both pages in the correct order.
		 */
		
		// -------- MirroredLock ----------
		MIRROREDLOCK_BUFMGR_LOCK;
		
		if (newtupsize > pagefree)
		{
			/* Assume there's no chance to put heaptup on same page. */
			newbuf = RelationGetBufferForTuple(relation, heaptup->t_len,
											   buffer, true);
		}
		else
		{
			/* Re-acquire the lock on the old tuple's page. */
			LockBuffer(buffer, BUFFER_LOCK_EXCLUSIVE);
			/* Re-check using the up-to-date free space */
			pagefree = PageGetFreeSpace((Page) dp);
			if (newtupsize > pagefree)
			{
				/*
				 * Rats, it doesn't fit anymore.  We must now unlock and
				 * relock to avoid deadlock.  Fortunately, this path should
				 * seldom be taken.
				 */
				LockBuffer(buffer, BUFFER_LOCK_UNLOCK);
				newbuf = RelationGetBufferForTuple(relation, heaptup->t_len,
												   buffer, true);
			}
			else
			{
				/* OK, it fits here, so we're done. */
				newbuf = buffer;
			}
		}
	}
	else
	{
		/* No TOAST work needed, and it'll fit on same page */
		already_marked = false;
		newbuf = buffer;
		heaptup = newtup;
	}

	/*
	 * At this point newbuf and buffer are both pinned and locked, and newbuf
	 * has enough space for the new tuple.	If they are the same buffer, only
	 * one pin is held.
	 */

	/* NO EREPORT(ERROR) from here till changes are logged */
	START_CRIT_SECTION();

	RelationPutHeapTuple(relation, newbuf, heaptup);	/* insert new tuple */

	if (!already_marked)
	{
		oldtup.t_data->t_infomask &= ~(HEAP_XMAX_COMMITTED |
									   HEAP_XMAX_INVALID |
									   HEAP_XMAX_IS_MULTI |
									   HEAP_IS_LOCKED |
									   HEAP_MOVED);
		HeapTupleHeaderSetXmax(oldtup.t_data, xid);
		HeapTupleHeaderSetCmax(oldtup.t_data, cid, iscombo);
	}

	/* record address of new tuple in t_ctid of old one */
	oldtup.t_data->t_ctid = heaptup->t_self;

	if (newbuf != buffer)
		MarkBufferDirty(newbuf);
	MarkBufferDirty(buffer);

	/* XLOG stuff */
	if (!relation->rd_istemp)
	{
		XLogRecPtr	recptr = log_heap_update(relation, buffer, oldtup.t_self,
											 newbuf, heaptup, false);

		if (newbuf != buffer)
		{
			PageSetLSN(BufferGetPage(newbuf), recptr);
			PageSetTLI(BufferGetPage(newbuf), ThisTimeLineID);
		}
		PageSetLSN(BufferGetPage(buffer), recptr);
		PageSetTLI(BufferGetPage(buffer), ThisTimeLineID);
	}
	else
	{
		/* No XLOG record, but still need to flag that XID exists on disk */
		MyXactMadeTempRelUpdate = true;
	}

	END_CRIT_SECTION();

	if (newbuf != buffer)
		LockBuffer(newbuf, BUFFER_LOCK_UNLOCK);
	LockBuffer(buffer, BUFFER_LOCK_UNLOCK);
	
	MIRROREDLOCK_BUFMGR_UNLOCK;
	// -------- MirroredLock ----------
	
	/*
	 * Mark old tuple for invalidation from system caches at next command
	 * boundary. We have to do this before releasing the buffer because we
	 * need to look at the contents of the tuple.
	 */
	CacheInvalidateHeapTuple(relation, &oldtup, SysCacheInvalidate_Update_OldTup);

	/* Now we can release the buffer(s) */
	if (newbuf != buffer)
		ReleaseBuffer(newbuf);
	ReleaseBuffer(buffer);

	/*
	 * If new tuple is cachable, mark it for invalidation from the caches in
	 * case we abort.  Note it is OK to do this after releasing the buffer,
	 * because the heaptup data structure is all in local memory, not in the
	 * shared buffer.
	 */

	CacheInvalidateHeapTuple(relation, heaptup, SysCacheInvalidate_Update_NewTup);

	/*
	 * Release the lmgr tuple lock, if we had it.
	 */
	if (have_tuple_lock)
		UnlockTuple(relation, &(oldtup.t_self), ExclusiveLock);

	pgstat_count_heap_update(relation, false);

	/*
	 * If heaptup is a private copy, release it.  Don't forget to copy t_self
	 * back to the caller's image, too.
	 */
	if (heaptup != newtup)
	{
		newtup->t_self = heaptup->t_self;
		heap_freetuple(heaptup);
	}

	return HeapTupleMayBeUpdated;
}

/*
 *	heap_update - replace a tuple
 *
 * NB: do not call this directly unless you are prepared to deal with
 * concurrent-update conditions.  Use simple_heap_update instead.
 *
 *	relation - table to be modified (caller must hold suitable lock)
 *	otid - TID of old tuple to be replaced
 *	newtup - newly constructed tuple data to store
 *	ctid - output parameter, used only for failure case (see below)
 *	update_xmax - output parameter, used only for failure case (see below)
 *	cid - update command ID (used for visibility test, and stored into
 *		cmax/cmin if successful)
 *	crosscheck - if not InvalidSnapshot, also check old tuple against this
 *	wait - true if should wait for any conflicting update to commit/abort
 *
 * Normal, successful return value is HeapTupleMayBeUpdated, which
 * actually means we *did* update it.  Failure return codes are
 * HeapTupleSelfUpdated, HeapTupleUpdated, or HeapTupleBeingUpdated
 * (the last only possible if wait == false).
 *
 * On success, the header fields of *newtup are updated to match the new
 * stored tuple; in particular, newtup->t_self is set to the TID where the
 * new tuple was inserted.	However, any TOAST changes in the new tuple's
 * data are not reflected into *newtup.
 *
 * In the failure cases, the routine returns the tuple's t_ctid and t_xmax.
 * If t_ctid is the same as otid, the tuple was deleted; if different, the
 * tuple was updated, and t_ctid is the location of the replacement tuple.
 * (t_xmax is needed to verify that the replacement tuple matches.)
 */
HTSU_Result
heap_update(Relation relation, ItemPointer otid, HeapTuple newtup,
			ItemPointer ctid, TransactionId *update_xmax,
			CommandId cid, Snapshot crosscheck, bool wait)
{
	MIRROREDLOCK_BUFMGR_VERIFY_NO_LOCK_LEAK_DECLARE;

	HTSU_Result result;

	MIRROREDLOCK_BUFMGR_VERIFY_NO_LOCK_LEAK_ENTER;

	result = heap_update_internal(
					relation, otid, newtup, ctid, update_xmax,
					cid, crosscheck, wait, /* simple */ false);

	MIRROREDLOCK_BUFMGR_VERIFY_NO_LOCK_LEAK_EXIT;

	return result;
}

/*
 *	simple_heap_update - replace a tuple
 *
 * This routine may be used to update a tuple when concurrent updates of
 * the target tuple are not expected (for example, because we have a lock
 * on the relation associated with the tuple).	Any failure is reported
 * via ereport().
 */
void
simple_heap_update(Relation relation, ItemPointer otid, HeapTuple tup)
{

	if (Gp_role == GP_ROLE_EXECUTE && !gp_upgrade_mode)
		elog(ERROR, "simple_heap_update() is not allowed on segments");

	MIRROREDLOCK_BUFMGR_VERIFY_NO_LOCK_LEAK_DECLARE;

	HTSU_Result result;
	ItemPointerData update_ctid;
	TransactionId update_xmax;

	MIRROREDLOCK_BUFMGR_VERIFY_NO_LOCK_LEAK_ENTER;

	result = heap_update_internal(
						 relation, otid, tup,
						 &update_ctid, &update_xmax,
						 GetCurrentCommandId(), InvalidSnapshot,
						 true /* wait for commit */,
						 /* simple */ true);
	switch (result)
	{
		case HeapTupleSelfUpdated:
			/* Tuple was already updated in current command? */
			elog(ERROR, "tuple already updated by self");
			break;

		case HeapTupleMayBeUpdated:
			/* done successfully */
			break;

		case HeapTupleUpdated:
			elog(ERROR, "tuple concurrently updated");
			break;

		default:
			elog(ERROR, "unrecognized heap_update status: %u", result);
			break;
	}

	MIRROREDLOCK_BUFMGR_VERIFY_NO_LOCK_LEAK_EXIT;

}

/*
 *	heap_lock_tuple - lock a tuple in shared or exclusive mode
 *
 * Note that this acquires a buffer pin, which the caller must release.
 *
 * Input parameters:
 *	relation: relation containing tuple (caller must hold suitable lock)
 *	tuple->t_self: TID of tuple to lock (rest of struct need not be valid)
 *	cid: current command ID (used for visibility test, and stored into
 *		tuple's cmax if lock is successful)
 *	mode: indicates if shared or exclusive tuple lock is desired
 *	waittype:
 *		if LockTupleWait, wait for lock until it's acquired (normal behavior)
 *		if LockTupleNoWait,	if can't get lock right away, report error.
 *		if LockTupleIfNotLocked, if can't get lock right away, give up. no error
 *
 * Output parameters:
 *	*tuple: all fields filled in
 *	*buffer: set to buffer holding tuple (pinned but not locked at exit)
 *	*ctid: set to tuple's t_ctid, but only in failure cases
 *	*update_xmax: set to tuple's xmax, but only in failure cases
 *
 * Function result may be:
 *	HeapTupleMayBeUpdated: lock was successfully acquired
 *	HeapTupleSelfUpdated: lock failed because tuple updated by self
 *	HeapTupleUpdated: lock failed because tuple updated by other xact
 *
 * In the failure cases, the routine returns the tuple's t_ctid and t_xmax.
 * If t_ctid is the same as t_self, the tuple was deleted; if different, the
 * tuple was updated, and t_ctid is the location of the replacement tuple.
 * (t_xmax is needed to verify that the replacement tuple matches.)
 *
 *
 * NOTES: because the shared-memory lock table is of finite size, but users
 * could reasonably want to lock large numbers of tuples, we do not rely on
 * the standard lock manager to store tuple-level locks over the long term.
 * Instead, a tuple is marked as locked by setting the current transaction's
 * XID as its XMAX, and setting additional infomask bits to distinguish this
 * usage from the more normal case of having deleted the tuple.  When
 * multiple transactions concurrently share-lock a tuple, the first locker's
 * XID is replaced in XMAX with a MultiTransactionId representing the set of
 * XIDs currently holding share-locks.
 *
 * When it is necessary to wait for a tuple-level lock to be released, the
 * basic delay is provided by XactLockTableWait or MultiXactIdWait on the
 * contents of the tuple's XMAX.  However, that mechanism will release all
 * waiters concurrently, so there would be a race condition as to which
 * waiter gets the tuple, potentially leading to indefinite starvation of
 * some waiters.  The possibility of share-locking makes the problem much
 * worse --- a steady stream of share-lockers can easily block an exclusive
 * locker forever.	To provide more reliable semantics about who gets a
 * tuple-level lock first, we use the standard lock manager.  The protocol
 * for waiting for a tuple-level lock is really
 *		LockTuple()
 *		XactLockTableWait()
 *		mark tuple as locked by me
 *		UnlockTuple()
 * When there are multiple waiters, arbitration of who is to get the lock next
 * is provided by LockTuple().	However, at most one tuple-level lock will
 * be held or awaited per backend at any time, so we don't risk overflow
 * of the lock table.  Note that incoming share-lockers are required to
 * do LockTuple as well, if there is any conflict, to ensure that they don't
 * starve out waiting exclusive-lockers.  However, if there is not any active
 * conflict for a tuple, we don't incur any extra overhead.
 */
HTSU_Result
heap_lock_tuple(Relation relation, HeapTuple tuple, Buffer *buffer,
				ItemPointer ctid, TransactionId *update_xmax,
				CommandId cid, LockTupleMode mode, LockTupleWaitType waittype)
{
	MIRROREDLOCK_BUFMGR_DECLARE;

	HTSU_Result result;
	ItemPointer tid = &(tuple->t_self);
	ItemId		lp;
	PageHeader	dp;
	TransactionId xid;
	TransactionId xmax;
	uint16		old_infomask;
	uint16		new_infomask;
	LOCKMODE	tuple_lock_type;
	bool		have_tuple_lock = false;

	// Fetch gp_persistent_relation_node information that will be added to XLOG record.
	RelationFetchGpRelationNodeForXLog(relation);

	tuple_lock_type = (mode == LockTupleShared) ? ShareLock : ExclusiveLock;
	
	// -------- MirroredLock ----------
	MIRROREDLOCK_BUFMGR_LOCK;
	
	*buffer = ReadBuffer(relation, ItemPointerGetBlockNumber(tid));
	LockBuffer(*buffer, BUFFER_LOCK_EXCLUSIVE);

	dp = (PageHeader) BufferGetPage(*buffer);
	lp = PageGetItemId(dp, ItemPointerGetOffsetNumber(tid));
	Assert(ItemIdIsUsed(lp));

	tuple->t_data = (HeapTupleHeader) PageGetItem((Page) dp, lp);
	tuple->t_len = ItemIdGetLength(lp);

l3:
	result = HeapTupleSatisfiesUpdate(relation, tuple->t_data, cid, *buffer);

	if (result == HeapTupleInvisible)
	{
		UnlockReleaseBuffer(*buffer);
		
		MIRROREDLOCK_BUFMGR_UNLOCK;
		// -------- MirroredLock ----------
		
		elog(ERROR, "attempted to lock invisible tuple");
	}
	else if (result == HeapTupleBeingUpdated)
	{
		TransactionId xwait;
		uint16		infomask;

		/* must copy state data before unlocking buffer */
		xwait = HeapTupleHeaderGetXmax(tuple->t_data);

		infomask = tuple->t_data->t_infomask;

		LockBuffer(*buffer, BUFFER_LOCK_UNLOCK);
		
		MIRROREDLOCK_BUFMGR_UNLOCK;
		// -------- MirroredLock ----------
		
		/*
		 * If we wish to acquire share lock, and the tuple is already
		 * share-locked by a multixact that includes any subtransaction of the
		 * current top transaction, then we effectively hold the desired lock
		 * already.  We *must* succeed without trying to take the tuple lock,
		 * else we will deadlock against anyone waiting to acquire exclusive
		 * lock.  We don't need to make any state changes in this case.
		 */
		if (mode == LockTupleShared &&
			(infomask & HEAP_XMAX_IS_MULTI) &&
			MultiXactIdIsCurrent((MultiXactId) xwait))
		{
			Assert(infomask & HEAP_XMAX_SHARED_LOCK);
			/* Probably can't hold tuple lock here, but may as well check */
			if (have_tuple_lock)
				UnlockTuple(relation, tid, tuple_lock_type);
			return HeapTupleMayBeUpdated;
		}

		/*
		 * Acquire tuple lock to establish our priority for the tuple.
		 * LockTuple will release us when we are next-in-line for the tuple.
		 * We must do this even if we are share-locking.
		 *
		 * If we are forced to "start over" below, we keep the tuple lock;
		 * this arranges that we stay at the head of the line while rechecking
		 * tuple state.
		 */
		if (!have_tuple_lock)
		{
			if (waittype == LockTupleIfNotLocked)
			{
				if (!ConditionalLockTuple(relation, tid, tuple_lock_type))
					return HeapTupleBeingUpdated; /* return without a lock */
			}
			else if (waittype == LockTupleNoWait)
			{
				if (!ConditionalLockTuple(relation, tid, tuple_lock_type))
					ereport(ERROR,
							(errcode(ERRCODE_LOCK_NOT_AVAILABLE),
					errmsg("could not obtain lock on row in relation \"%s\"",
						   RelationGetRelationName(relation))));
			}
			else
				LockTuple(relation, tid, tuple_lock_type);
			have_tuple_lock = true;
		}
		
		if (mode == LockTupleShared && (infomask & HEAP_XMAX_SHARED_LOCK))
		{
			/*
			 * Acquiring sharelock when there's at least one sharelocker
			 * already.  We need not wait for him/them to complete.
			 */
			
			// -------- MirroredLock ----------
			MIRROREDLOCK_BUFMGR_LOCK;

			LockBuffer(*buffer, BUFFER_LOCK_EXCLUSIVE);

			/*
			 * Make sure it's still a shared lock, else start over.  (It's OK
			 * if the ownership of the shared lock has changed, though.)
			 */
			if (!(tuple->t_data->t_infomask & HEAP_XMAX_SHARED_LOCK))
				goto l3;
		}
		else if (infomask & HEAP_XMAX_IS_MULTI)
		{
			/* wait for multixact to end */
			if (waittype == LockTupleIfNotLocked)
			{
				if (!ConditionalMultiXactIdWait((MultiXactId) xwait))
					return HeapTupleBeingUpdated; /* return without a lock */
			}
			else if (waittype == LockTupleNoWait)
			{
				if (!ConditionalMultiXactIdWait((MultiXactId) xwait))
					ereport(ERROR,
							(errcode(ERRCODE_LOCK_NOT_AVAILABLE),
					errmsg("could not obtain lock on row in relation \"%s\"",
						   RelationGetRelationName(relation))));
			}
			else
				MultiXactIdWait((MultiXactId) xwait);
			
			// -------- MirroredLock ----------
			MIRROREDLOCK_BUFMGR_LOCK;

			LockBuffer(*buffer, BUFFER_LOCK_EXCLUSIVE);

			/*
			 * If xwait had just locked the tuple then some other xact could
			 * update this tuple before we get to this point. Check for xmax
			 * change, and start over if so.
			 */
			if (!(tuple->t_data->t_infomask & HEAP_XMAX_IS_MULTI) ||
				!TransactionIdEquals(HeapTupleHeaderGetXmax(tuple->t_data),
									 xwait))
				goto l3;

			/*
			 * You might think the multixact is necessarily done here, but not
			 * so: it could have surviving members, namely our own xact or
			 * other subxacts of this backend.	It is legal for us to lock the
			 * tuple in either case, however.  We don't bother changing the
			 * on-disk hint bits since we are about to overwrite the xmax
			 * altogether.
			 */
		}
		else
		{
			/* wait for regular transaction to end */
			if (waittype == LockTupleIfNotLocked)
			{
				if (!ConditionalXactLockTableWait(xwait))
					return HeapTupleBeingUpdated; /* return without a lock */
			}
			else if (waittype == LockTupleNoWait)
			{
				if (!ConditionalXactLockTableWait(xwait))
					ereport(ERROR,
							(errcode(ERRCODE_LOCK_NOT_AVAILABLE),
					errmsg("could not obtain lock on row in relation \"%s\"",
						   RelationGetRelationName(relation))));
			}
			else
				XactLockTableWait(xwait);
			
			// -------- MirroredLock ----------
			MIRROREDLOCK_BUFMGR_LOCK;

			LockBuffer(*buffer, BUFFER_LOCK_EXCLUSIVE);

			/*
			 * xwait is done, but if xwait had just locked the tuple then some
			 * other xact could update this tuple before we get to this point.
			 * Check for xmax change, and start over if so.
			 */
			if ((tuple->t_data->t_infomask & HEAP_XMAX_IS_MULTI) ||
				!TransactionIdEquals(HeapTupleHeaderGetXmax(tuple->t_data),
									 xwait))
				goto l3;

			/* Otherwise we can mark it committed or aborted */
			if (!(tuple->t_data->t_infomask & (HEAP_XMAX_COMMITTED |
											   HEAP_XMAX_INVALID)))
			{
				if (TransactionIdDidCommit(xwait))
					tuple->t_data->t_infomask |= HEAP_XMAX_COMMITTED;
				else
					tuple->t_data->t_infomask |= HEAP_XMAX_INVALID;
				SetBufferCommitInfoNeedsSave(*buffer);
			}
		}

		/*
		 * We may lock if previous xmax aborted, or if it committed but only
		 * locked the tuple without updating it.  The case where we didn't
		 * wait because we are joining an existing shared lock is correctly
		 * handled, too.
		 */
		if (tuple->t_data->t_infomask & (HEAP_XMAX_INVALID |
										 HEAP_IS_LOCKED))
			result = HeapTupleMayBeUpdated;
		else
			result = HeapTupleUpdated;
	}

	if (result != HeapTupleMayBeUpdated)
	{
		Assert(result == HeapTupleSelfUpdated || result == HeapTupleUpdated);
		Assert(!(tuple->t_data->t_infomask & HEAP_XMAX_INVALID));
		*ctid = tuple->t_data->t_ctid;
		*update_xmax = HeapTupleHeaderGetXmax(tuple->t_data);
		LockBuffer(*buffer, BUFFER_LOCK_UNLOCK);
		
		MIRROREDLOCK_BUFMGR_UNLOCK;
		// -------- MirroredLock ----------
		
		if (have_tuple_lock)
			UnlockTuple(relation, tid, tuple_lock_type);
		return result;
	}

	/*
	 * We might already hold the desired lock (or stronger), possibly under a
	 * different subtransaction of the current top transaction.  If so, there
	 * is no need to change state or issue a WAL record.  We already handled
	 * the case where this is true for xmax being a MultiXactId, so now check
	 * for cases where it is a plain TransactionId.
	 *
	 * Note in particular that this covers the case where we already hold
	 * exclusive lock on the tuple and the caller only wants shared lock. It
	 * would certainly not do to give up the exclusive lock.
	 */
	xmax = HeapTupleHeaderGetXmax(tuple->t_data);
	old_infomask = tuple->t_data->t_infomask;

	if (!(old_infomask & (HEAP_XMAX_INVALID |
						  HEAP_XMAX_COMMITTED |
						  HEAP_XMAX_IS_MULTI)) &&
		(mode == LockTupleShared ?
		 (old_infomask & HEAP_IS_LOCKED) :
		 (old_infomask & HEAP_XMAX_EXCL_LOCK)) &&
		TransactionIdIsCurrentTransactionId(xmax))
	{
		LockBuffer(*buffer, BUFFER_LOCK_UNLOCK);
		
		MIRROREDLOCK_BUFMGR_UNLOCK;
		// -------- MirroredLock ----------
		
		/* Probably can't hold tuple lock here, but may as well check */
		if (have_tuple_lock)
			UnlockTuple(relation, tid, tuple_lock_type);
		return HeapTupleMayBeUpdated;
	}

	/*
	 * Compute the new xmax and infomask to store into the tuple.  Note we do
	 * not modify the tuple just yet, because that would leave it in the wrong
	 * state if multixact.c elogs.
	 */
	xid = GetCurrentTransactionId();

	new_infomask = old_infomask & ~(HEAP_XMAX_COMMITTED |
									HEAP_XMAX_INVALID |
									HEAP_XMAX_IS_MULTI |
									HEAP_IS_LOCKED |
									HEAP_MOVED);

	if (mode == LockTupleShared)
	{
		/*
		 * If this is the first acquisition of a shared lock in the current
		 * transaction, set my per-backend OldestMemberMXactId setting. We can
		 * be certain that the transaction will never become a member of any
		 * older MultiXactIds than that.  (We have to do this even if we end
		 * up just using our own TransactionId below, since some other backend
		 * could incorporate our XID into a MultiXact immediately afterwards.)
		 */
		MultiXactIdSetOldestMember();

		new_infomask |= HEAP_XMAX_SHARED_LOCK;

		/*
		 * Check to see if we need a MultiXactId because there are multiple
		 * lockers.
		 *
		 * HeapTupleSatisfiesUpdate will have set the HEAP_XMAX_INVALID bit if
		 * the xmax was a MultiXactId but it was not running anymore. There is
		 * a race condition, which is that the MultiXactId may have finished
		 * since then, but that uncommon case is handled within
		 * MultiXactIdExpand.
		 *
		 * There is a similar race condition possible when the old xmax was a
		 * regular TransactionId.  We test TransactionIdIsInProgress again
		 * just to narrow the window, but it's still possible to end up
		 * creating an unnecessary MultiXactId.  Fortunately this is harmless.
		 */
		if (!(old_infomask & (HEAP_XMAX_INVALID | HEAP_XMAX_COMMITTED)))
		{
			if (old_infomask & HEAP_XMAX_IS_MULTI)
			{
				/*
				 * If the XMAX is already a MultiXactId, then we need to
				 * expand it to include our own TransactionId.
				 */
				xid = MultiXactIdExpand((MultiXactId) xmax, xid);
				new_infomask |= HEAP_XMAX_IS_MULTI;
			}
			else if (TransactionIdIsInProgress(xmax))
			{
				/*
				 * If the XMAX is a valid TransactionId, then we need to
				 * create a new MultiXactId that includes both the old locker
				 * and our own TransactionId.
				 */
				xid = MultiXactIdCreate(xmax, xid);
				new_infomask |= HEAP_XMAX_IS_MULTI;
			}
			else
			{
				/*
				 * Can get here iff HeapTupleSatisfiesUpdate saw the old xmax
				 * as running, but it finished before
				 * TransactionIdIsInProgress() got to run.	Treat it like
				 * there's no locker in the tuple.
				 */
			}
		}
		else
		{
			/*
			 * There was no previous locker, so just insert our own
			 * TransactionId.
			 */
		}
	}
	else
	{
		/* We want an exclusive lock on the tuple */
		new_infomask |= HEAP_XMAX_EXCL_LOCK;
	}

	START_CRIT_SECTION();

	/*
	 * Store transaction information of xact locking the tuple.
	 *
	 * Note: Cmax is meaningless in this context, so don't set it; this avoids
	 * possibly generating a useless combo CID.
	 */
	tuple->t_data->t_infomask = new_infomask;
	HeapTupleHeaderSetXmax(tuple->t_data, xid);
	/* Make sure there is no forward chain link in t_ctid */
	tuple->t_data->t_ctid = *tid;

	MarkBufferDirty(*buffer);

	/*
	 * XLOG stuff.	You might think that we don't need an XLOG record because
	 * there is no state change worth restoring after a crash.	You would be
	 * wrong however: we have just written either a TransactionId or a
	 * MultiXactId that may never have been seen on disk before, and we need
	 * to make sure that there are XLOG entries covering those ID numbers.
	 * Else the same IDs might be re-used after a crash, which would be
	 * disastrous if this page made it to disk before the crash.  Essentially
	 * we have to enforce the WAL log-before-data rule even in this case.
	 * (Also, in a PITR log-shipping or 2PC environment, we have to have XLOG
	 * entries for everything anyway.)
	 */
	if (!relation->rd_istemp)
	{
		xl_heap_lock xlrec;
		XLogRecPtr	recptr;
		XLogRecData rdata[2];

		xl_heaptid_set(&xlrec.target, relation, &tuple->t_self);
		xlrec.locking_xid = xid;
		xlrec.xid_is_mxact = ((new_infomask & HEAP_XMAX_IS_MULTI) != 0);
		xlrec.shared_lock = (mode == LockTupleShared);
		rdata[0].data = (char *) &xlrec;
		rdata[0].len = SizeOfHeapLock;
		rdata[0].buffer = InvalidBuffer;
		rdata[0].next = &(rdata[1]);

		rdata[1].data = NULL;
		rdata[1].len = 0;
		rdata[1].buffer = *buffer;
		rdata[1].buffer_std = true;
		rdata[1].next = NULL;

		recptr = XLogInsert(RM_HEAP_ID, XLOG_HEAP_LOCK, rdata);

		PageSetLSN(dp, recptr);
		PageSetTLI(dp, ThisTimeLineID);
	}
	else
	{
		/* No XLOG record, but still need to flag that XID exists on disk */
		MyXactMadeTempRelUpdate = true;
	}

	END_CRIT_SECTION();

	LockBuffer(*buffer, BUFFER_LOCK_UNLOCK);
	
	MIRROREDLOCK_BUFMGR_UNLOCK;
	// -------- MirroredLock ----------
	
	/*
	 * Now that we have successfully marked the tuple as locked, we can
	 * release the lmgr tuple lock, if we had it.
	 */
	if (have_tuple_lock)
		UnlockTuple(relation, tid, tuple_lock_type);

	return HeapTupleMayBeUpdated;
}


/*
 * heap_inplace_update - update a tuple "in place" (ie, overwrite it)
 *
 * Overwriting violates both MVCC and transactional safety, so the uses
 * of this function in Postgres are extremely limited.	Nonetheless we
 * find some places to use it.
 *
 * The tuple cannot change size, and therefore it's reasonable to assume
 * that its null bitmap (if any) doesn't change either.  So we just
 * overwrite the data portion of the tuple without touching the null
 * bitmap or any of the header fields.
 *
 * tuple is an in-memory tuple structure containing the data to be written
 * over the target tuple.  Also, tuple->t_self identifies the target tuple.
 */
static void
heap_inplace_update_internal(Relation relation, HeapTuple tuple, TransactionId xid)
{
	MIRROREDLOCK_BUFMGR_DECLARE;

	bool		isFrozen = (xid == FrozenTransactionId);
	Buffer		buffer;
	Page		page;
	OffsetNumber offnum;
	ItemId		lp = NULL;
	HeapTupleHeader htup;
	uint32		oldlen;
	uint32		newlen;

	/* 
	 * Fetch gp_persistent_relation_node information that will be added
	 * to XLOG record.
	 */
	RelationFetchGpRelationNodeForXLog(relation);

	// -------- MirroredLock ----------
	MIRROREDLOCK_BUFMGR_LOCK;
	
	buffer = ReadBuffer(relation, ItemPointerGetBlockNumber(&(tuple->t_self)));
	LockBuffer(buffer, BUFFER_LOCK_EXCLUSIVE);
	page = (Page) BufferGetPage(buffer);

	offnum = ItemPointerGetOffsetNumber(&(tuple->t_self));
	if (PageGetMaxOffsetNumber(page) >= offnum)
		lp = PageGetItemId(page, offnum);

	if (PageGetMaxOffsetNumber(page) < offnum || !ItemIdIsUsed(lp))
		elog(ERROR, "heap_inplace_update: invalid lp");

	htup = (HeapTupleHeader) PageGetItem(page, lp);

	oldlen = ItemIdGetLength(lp) - htup->t_hoff;
	newlen = tuple->t_len - tuple->t_data->t_hoff;
	if (oldlen != newlen || htup->t_hoff != tuple->t_data->t_hoff)
		elog(ERROR, "heap_inplace_update: wrong tuple length");

	/* NO EREPORT(ERROR) from here till changes are logged */
	START_CRIT_SECTION();

	memcpy((char *) htup + htup->t_hoff,
		   (char *) tuple->t_data + tuple->t_data->t_hoff,
		   newlen);

	MarkBufferDirty(buffer);

#ifdef FAULT_INJECTOR
	if (FaultInjector_InjectFaultIfSet(
									XLOGInsert,
									DDLNotSpecified,
									"" /* databaseName */,
									"" /* tableName */) == FaultInjectorTypeSkip)
	{
		END_CRIT_SECTION();

		UnlockReleaseBuffer(buffer);

		MIRROREDLOCK_BUFMGR_UNLOCK;
		// -------- MirroredLock ----------
		
		/* Send out shared cache inval if necessary */
		if (!IsBootstrapProcessingMode())
		{
			CacheInvalidateHeapTuple(relation, tuple, SysCacheInvalidate_Update_InPlace);
		}
		return;
	}
#endif

	/* XLOG stuff */
	if (!relation->rd_istemp)
	{
		xl_heap_inplace xlrec;
		XLogRecPtr	recptr;
		XLogRecData rdata[2];

		xl_heaptid_set(&xlrec.target, relation, &tuple->t_self);

		rdata[0].data = (char *) &xlrec;
		rdata[0].len = SizeOfHeapInplace;
		rdata[0].buffer = InvalidBuffer;
		rdata[0].next = &(rdata[1]);

		rdata[1].data = (char *) htup + htup->t_hoff;
		rdata[1].len = newlen;
		rdata[1].buffer = buffer;
		rdata[1].buffer_std = true;
		rdata[1].next = NULL;

		if (!isFrozen)
			recptr = XLogInsert(RM_HEAP_ID, XLOG_HEAP_INPLACE, rdata);
		else
			recptr = XLogInsert_OverrideXid(RM_HEAP_ID, XLOG_HEAP_INPLACE, rdata, FrozenTransactionId);

		PageSetLSN(page, recptr);
		PageSetTLI(page, ThisTimeLineID);
	}

	END_CRIT_SECTION();

	UnlockReleaseBuffer(buffer);
	
	MIRROREDLOCK_BUFMGR_UNLOCK;
	// -------- MirroredLock ----------
	
	/* Send out shared cache inval if necessary */
	if (!IsBootstrapProcessingMode())
		CacheInvalidateHeapTuple(relation, tuple, SysCacheInvalidate_Update_InPlace);
}

/*
 * heap_inplace_frozen_delete_internal - frozen delete a tuple "in place" 
 * by modifying its MVCC header. Overwriting MVCC header violates both MVCC
 * and transactional safety, so the uses of this function in Postgres are 
 * extremely limited. Nonetheless we find some places to use it.
 *
 * Modified from heap_inplace_update_internal.
 */
static void
heap_inplace_frozen_delete_internal(Relation relation, HeapTuple tuple, TransactionId xid)
{
	MIRROREDLOCK_BUFMGR_DECLARE;

	bool		isFrozen = (xid == FrozenTransactionId);
	Buffer		buffer;
	Page		page;
	OffsetNumber offnum;
	ItemId		lp = NULL;
	HeapTupleHeader htup;
	uint32		oldlen;
	uint32		newlen;

	/* 
	 * Fetch gp_persistent_relation_node information that will be added
	 * to XLOG record.
	 */
	RelationFetchGpRelationNodeForXLog(relation);
	
	// -------- MirroredLock ----------
	MIRROREDLOCK_BUFMGR_LOCK;
	
	buffer = ReadBuffer(relation, ItemPointerGetBlockNumber(&(tuple->t_self)));
	LockBuffer(buffer, BUFFER_LOCK_EXCLUSIVE);
	page = (Page) BufferGetPage(buffer);

	offnum = ItemPointerGetOffsetNumber(&(tuple->t_self));
	if (PageGetMaxOffsetNumber(page) >= offnum)
		lp = PageGetItemId(page, offnum);

	if (PageGetMaxOffsetNumber(page) < offnum || !ItemIdIsUsed(lp))
		elog(ERROR, "heap_inplace_frozen_delete_internal: invalid lp");

	htup = (HeapTupleHeader) PageGetItem(page, lp);

	oldlen = ItemIdGetLength(lp) - htup->t_hoff;
	newlen = tuple->t_len - tuple->t_data->t_hoff;
	if (oldlen != newlen || htup->t_hoff != tuple->t_data->t_hoff)
		elog(ERROR, "heap_inplace_frozen_delete_internal: wrong tuple length");

	/* NO EREPORT(ERROR) from here till changes are logged */
	START_CRIT_SECTION();

	htup->t_infomask &= 0;
	htup->t_infomask |= HEAP_XMIN_INVALID;
	HeapTupleHeaderSetXmin(htup, 0);
	HeapTupleHeaderSetXmax(htup, FrozenTransactionId);

	memcpy((char *) htup + htup->t_hoff,
		   (char *) tuple->t_data + tuple->t_data->t_hoff,
		   newlen);

	MarkBufferDirty(buffer);

	/* XLOG stuff */
	if (!relation->rd_istemp)
	{
		xl_heap_inplace xlrec;
		XLogRecPtr	recptr;
		XLogRecData rdata[2];

		xl_heaptid_set(&xlrec.target, relation, &tuple->t_self);

		rdata[0].data = (char *) &xlrec;
		rdata[0].len = SizeOfHeapInplace;
		rdata[0].buffer = InvalidBuffer;
		rdata[0].next = &(rdata[1]);

		rdata[1].data = (char *) htup + htup->t_hoff;
		rdata[1].len = newlen;
		rdata[1].buffer = buffer;
		rdata[1].buffer_std = true;
		rdata[1].next = NULL;

		if (!isFrozen)
			recptr = XLogInsert(RM_HEAP_ID, XLOG_HEAP_INPLACE, rdata);
		else
			recptr = XLogInsert_OverrideXid(RM_HEAP_ID, XLOG_HEAP_INPLACE, rdata, FrozenTransactionId);

		PageSetLSN(page, recptr);
		PageSetTLI(page, ThisTimeLineID);
	}

	END_CRIT_SECTION();

	UnlockReleaseBuffer(buffer);
	
	MIRROREDLOCK_BUFMGR_UNLOCK;
	// -------- MirroredLock ----------
	
	/* Send out shared cache inval if necessary */
	if (!IsBootstrapProcessingMode())
		CacheInvalidateHeapTuple(relation, tuple, SysCacheInvalidate_Delete);
}

void
frozen_heap_inplace_update(Relation relation, HeapTuple tuple)
{
	if (Gp_role == GP_ROLE_EXECUTE && !gp_upgrade_mode)
		elog(ERROR, "frozen_heap_inplace_update() is not allowed on segments");

	MIRROREDLOCK_BUFMGR_VERIFY_NO_LOCK_LEAK_DECLARE;

	MIRROREDLOCK_BUFMGR_VERIFY_NO_LOCK_LEAK_ENTER;

	heap_inplace_update_internal(relation, tuple, FrozenTransactionId);

	MIRROREDLOCK_BUFMGR_VERIFY_NO_LOCK_LEAK_EXIT;
}

void
frozen_heap_inplace_delete(Relation relation, HeapTuple tuple)
{
	if (Gp_role == GP_ROLE_EXECUTE && !gp_upgrade_mode)
		elog(ERROR, "frozen_heap_inplace_delete() is not allowed on segments");

	MIRROREDLOCK_BUFMGR_VERIFY_NO_LOCK_LEAK_DECLARE;

	MIRROREDLOCK_BUFMGR_VERIFY_NO_LOCK_LEAK_ENTER;

	heap_inplace_frozen_delete_internal(relation, tuple, FrozenTransactionId);

	MIRROREDLOCK_BUFMGR_VERIFY_NO_LOCK_LEAK_EXIT;
}

void
heap_inplace_update(Relation relation, HeapTuple tuple)
{
	if (Gp_role == GP_ROLE_EXECUTE && !gp_upgrade_mode)
		elog(ERROR, "heap_inplace_update() is not allowed on segments");

	MIRROREDLOCK_BUFMGR_VERIFY_NO_LOCK_LEAK_DECLARE;

	MIRROREDLOCK_BUFMGR_VERIFY_NO_LOCK_LEAK_ENTER;

	heap_inplace_update_internal(relation, tuple, GetCurrentTransactionId());
	
	MIRROREDLOCK_BUFMGR_VERIFY_NO_LOCK_LEAK_EXIT;
}

/*
 * heap_freeze_tuple
 *
 * Check to see whether any of the XID fields of a tuple (xmin, xmax, xvac)
 * are older than the specified cutoff XID.  If so, replace them with
 * FrozenTransactionId or InvalidTransactionId as appropriate, and return
 * TRUE.  Return FALSE if nothing was changed.
 *
 * It is assumed that the caller has checked the tuple with
 * HeapTupleSatisfiesVacuum() and determined that it is not HEAPTUPLE_DEAD
 * (else we should be removing the tuple, not freezing it).
 *
 * NB: cutoff_xid *must* be <= the current global xmin, to ensure that any
 * XID older than it could neither be running nor seen as running by any
 * open transaction.  This ensures that the replacement will not change
 * anyone's idea of the tuple state.  Also, since we assume the tuple is
 * not HEAPTUPLE_DEAD, the fact that an XID is not still running allows us
 * to assume that it is either committed good or aborted, as appropriate;
 * so we need no external state checks to decide what to do.  (This is good
 * because this function is applied during WAL recovery, when we don't have
 * access to any such state, and can't depend on the hint bits to be set.)
 *
 * In lazy VACUUM, we call this while initially holding only a shared lock
 * on the tuple's buffer.  If any change is needed, we trade that in for an
 * exclusive lock before making the change.  Caller should pass the buffer ID
 * if shared lock is held, InvalidBuffer if exclusive lock is already held.
 *
 * Note: it might seem we could make the changes without exclusive lock, since
 * TransactionId read/write is assumed atomic anyway.  However there is a race
 * condition: someone who just fetched an old XID that we overwrite here could
 * conceivably not finish checking the XID against pg_clog before we finish
 * the VACUUM and perhaps truncate off the part of pg_clog he needs.  Getting
 * exclusive lock ensures no other backend is in process of checking the
 * tuple status.  Also, getting exclusive lock makes it safe to adjust the
 * infomask bits.
 */
bool
heap_freeze_tuple(HeapTupleHeader tuple, TransactionId cutoff_xid,
				  Buffer buf)
{
	bool		changed = false;
	TransactionId xid;

	MIRROREDLOCK_BUFMGR_MUST_ALREADY_BE_HELD;

	xid = HeapTupleHeaderGetXmin(tuple);
	if (TransactionIdIsNormal(xid) &&
		TransactionIdPrecedes(xid, cutoff_xid))
	{
		if (buf != InvalidBuffer)
		{
			/* trade in share lock for exclusive lock */
			LockBuffer(buf, BUFFER_LOCK_UNLOCK);
			LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE);
			buf = InvalidBuffer;
		}
		HeapTupleHeaderSetXmin(tuple, FrozenTransactionId);
		/*
		 * Might as well fix the hint bits too; usually XMIN_COMMITTED will
		 * already be set here, but there's a small chance not.
		 */
		Assert(!(tuple->t_infomask & HEAP_XMIN_INVALID));
		tuple->t_infomask |= HEAP_XMIN_COMMITTED;
		changed = true;
	}

	/*
	 * When we release shared lock, it's possible for someone else to change
	 * xmax before we get the lock back, so repeat the check after acquiring
	 * exclusive lock.  (We don't need this pushup for xmin, because only
	 * VACUUM could be interested in changing an existing tuple's xmin, and
	 * there's only one VACUUM allowed on a table at a time.)
	 */
recheck_xmax:
	if (!(tuple->t_infomask & HEAP_XMAX_IS_MULTI))
	{
		xid = HeapTupleHeaderGetXmax(tuple);
		if (TransactionIdIsNormal(xid) &&
			TransactionIdPrecedes(xid, cutoff_xid))
		{
			if (buf != InvalidBuffer)
			{
				/* trade in share lock for exclusive lock */
				LockBuffer(buf, BUFFER_LOCK_UNLOCK);
				LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE);
				buf = InvalidBuffer;
				goto recheck_xmax;			/* see comment above */
			}
			HeapTupleHeaderSetXmax(tuple, InvalidTransactionId);
			/*
			 * The tuple might be marked either XMAX_INVALID or XMAX_COMMITTED
			 * + LOCKED.  Normalize to INVALID just to be sure no one gets
			 * confused.
			 */
			tuple->t_infomask &= ~HEAP_XMAX_COMMITTED;
			tuple->t_infomask |= HEAP_XMAX_INVALID;
			changed = true;
		}
	}
	else
	{
		/*----------
		 * XXX perhaps someday we should zero out very old MultiXactIds here?
		 *
		 * The only way a stale MultiXactId could pose a problem is if a
		 * tuple, having once been multiply-share-locked, is not touched by
		 * any vacuum or attempted lock or deletion for just over 4G MultiXact
		 * creations, and then in the probably-narrow window where its xmax
		 * is again a live MultiXactId, someone tries to lock or delete it.
		 * Even then, another share-lock attempt would work fine.  An
		 * exclusive-lock or delete attempt would face unexpected delay, or
		 * in the very worst case get a deadlock error.  This seems an
		 * extremely low-probability scenario with minimal downside even if
		 * it does happen, so for now we don't do the extra bookkeeping that
		 * would be needed to clean out MultiXactIds.
		 *----------
		 */
	}

	/*
	 * Although xvac per se could only be set by VACUUM, it shares physical
	 * storage space with cmax, and so could be wiped out by someone setting
	 * xmax.  Hence recheck after changing lock, same as for xmax itself.
	 */
recheck_xvac:
	if (tuple->t_infomask & HEAP_MOVED)
	{
		xid = HeapTupleHeaderGetXvac(tuple);
		if (TransactionIdIsNormal(xid) &&
			TransactionIdPrecedes(xid, cutoff_xid))
		{
			if (buf != InvalidBuffer)
			{
				/* trade in share lock for exclusive lock */
				LockBuffer(buf, BUFFER_LOCK_UNLOCK);
				LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE);
				buf = InvalidBuffer;
				goto recheck_xvac;			/* see comment above */
			}
			/*
			 * If a MOVED_OFF tuple is not dead, the xvac transaction must
			 * have failed; whereas a non-dead MOVED_IN tuple must mean the
			 * xvac transaction succeeded.
			 */
			if (tuple->t_infomask & HEAP_MOVED_OFF)
				HeapTupleHeaderSetXvac(tuple, InvalidTransactionId);
			else
				HeapTupleHeaderSetXvac(tuple, FrozenTransactionId);
			/*
			 * Might as well fix the hint bits too; usually XMIN_COMMITTED
			 * will already be set here, but there's a small chance not.
			 */
			Assert(!(tuple->t_infomask & HEAP_XMIN_INVALID));
			tuple->t_infomask |= HEAP_XMIN_COMMITTED;
			changed = true;
		}
	}

	return changed;
}


/* ----------------
 *		heap_markpos	- mark scan position
 * ----------------
 */
void
heap_markpos(HeapScanDesc scan)
{
	/* Note: no locking manipulations needed */

	if (scan->rs_ctup.t_data != NULL)
	{
		scan->rs_mctid = scan->rs_ctup.t_self;
		if (scan->rs_pageatatime)
			scan->rs_mindex = scan->rs_cindex;
	}
	else
		ItemPointerSetInvalid(&scan->rs_mctid);
}


void
heap_markposx(HeapScanDesc scan, HeapTuple tuple)
{
	if (tuple && tuple->t_data)
	{
		Assert(ItemPointerIsValid(&tuple->t_self));
		scan->rs_mctid = tuple->t_self;
		if (scan->rs_pageatatime)
			scan->rs_mindex = ItemPointerGetOffsetNumber(&tuple->t_self);
	}
	else
	{
		ItemPointerSetInvalid(&scan->rs_mctid);
	}
}


/* ----------------
 *		heap_restrpos	- restore position to marked location
 * ----------------
 */
void
heap_restrpos(HeapScanDesc scan)
{
	/* XXX no amrestrpos checking that ammarkpos called */

	if (!ItemPointerIsValid(&scan->rs_mctid))
	{
		scan->rs_ctup.t_data = NULL;

		/*
		 * unpin scan buffers
		 */
		release_all_buffers(scan);
		scan->rs_cbuf = InvalidBuffer;
		scan->rs_cblock = InvalidBlockNumber;
		scan->rs_inited = false;
	}
	else
	{
		/*
		 * If we reached end of scan, rs_inited will now be false.	We must
		 * reset it to true to keep heapgettup from doing the wrong thing.
		 */
		scan->rs_inited = true;
		scan->rs_ctup.t_self = scan->rs_mctid;
		if (scan->rs_pageatatime)
		{
			scan->rs_cindex = scan->rs_mindex;
			heapgettup_pagemode(scan,
								NoMovementScanDirection,
								0,		/* needn't recheck scan keys */
								NULL);
		}
		else
			heapgettup(scan,
					   NoMovementScanDirection,
					   0,		/* needn't recheck scan keys */
					   NULL);
	}
}

/*
 * Perform XLogInsert for a new heap page operation
 */
void
log_heap_newpage(Relation rel, 
				 Page page,
				 BlockNumber bno)
{
	xl_heap_newpage xlrec;
	XLogRecPtr recptr;
	XLogRecData rdata[2];

	START_CRIT_SECTION();

	xl_heapnode_set(&xlrec.heapnode, rel);
	xlrec.blkno = bno;

	rdata[0].data = (char*) &xlrec;
	rdata[0].len = SizeOfHeapNewpage;
	rdata[0].buffer = InvalidBuffer;
	rdata[0].next = &rdata[1];

	rdata[1].data = (char*) page;
	rdata[1].len = BLCKSZ;
	rdata[1].buffer = InvalidBuffer;
	rdata[1].next = NULL;

	recptr = XLogInsert(RM_HEAP_ID, XLOG_HEAP_NEWPAGE, rdata);
	PageSetLSN(page, recptr);
	PageSetTLI(page, ThisTimeLineID);

	END_CRIT_SECTION();
}

/*
 * Perform XLogInsert for a heap-clean operation.  Caller must already
 * have modified the buffer and marked it dirty.
 *
 * Note: for historical reasons, the entries in the unused[] array should
 * be zero-based tuple indexes, not one-based.
 * 
 * Note: prior to Postgres 8.3, the entries in the nowunused[] array were
 * zero-based tuple indexes.  Now they are one-based like other uses
 * of OffsetNumber.
 */
XLogRecPtr
log_heap_clean(Relation reln, Buffer buffer, OffsetNumber *unused, int uncnt)
{
	xl_heap_clean xlrec;
	XLogRecPtr	recptr;
	XLogRecData rdata[2];

	/* Caller should not call me on a temp relation */
	Assert(!reln->rd_istemp);

	xl_heapnode_set(&xlrec.heapnode, reln);
	xlrec.block = BufferGetBlockNumber(buffer);

	rdata[0].data = (char *) &xlrec;
	rdata[0].len = SizeOfHeapClean;
	rdata[0].buffer = InvalidBuffer;
	rdata[0].next = &(rdata[1]);

	/*
	 * The unused-offsets array is not actually in the buffer, but pretend
	 * that it is.	When XLogInsert stores the whole buffer, the offsets array
	 * need not be stored too.
	 */
	if (uncnt > 0)
	{
		rdata[1].data = (char *) unused;
		rdata[1].len = uncnt * sizeof(OffsetNumber);
	}
	else
	{
		rdata[1].data = NULL;
		rdata[1].len = 0;
	}
	rdata[1].buffer = buffer;
	rdata[1].buffer_std = true;
	rdata[1].next = NULL;

	recptr = XLogInsert(RM_HEAP_ID, XLOG_HEAP_CLEAN, rdata);

	return recptr;
}

/*
 * Perform XLogInsert for a heap-freeze operation.  Caller must already
 * have modified the buffer and marked it dirty.
 *
 * Unlike log_heap_clean(), the offsets[] entries are one-based.
 */
XLogRecPtr
log_heap_freeze(Relation reln, Buffer buffer,
				TransactionId cutoff_xid,
				OffsetNumber *offsets, int offcnt)
{
	xl_heap_freeze xlrec;
	XLogRecPtr	recptr;
	XLogRecData rdata[2];

	/* Caller should not call me on a temp relation */
	Assert(!reln->rd_istemp);

	xl_heapnode_set(&xlrec.heapnode, reln);
	xlrec.block = BufferGetBlockNumber(buffer);
	xlrec.cutoff_xid = cutoff_xid;

	rdata[0].data = (char *) &xlrec;
	rdata[0].len = SizeOfHeapFreeze;
	rdata[0].buffer = InvalidBuffer;
	rdata[0].next = &(rdata[1]);

	/*
	 * The tuple-offsets array is not actually in the buffer, but pretend
	 * that it is.	When XLogInsert stores the whole buffer, the offsets array
	 * need not be stored too.
	 */
	if (offcnt > 0)
	{
		rdata[1].data = (char *) offsets;
		rdata[1].len = offcnt * sizeof(OffsetNumber);
	}
	else
	{
		rdata[1].data = NULL;
		rdata[1].len = 0;
	}
	rdata[1].buffer = buffer;
	rdata[1].buffer_std = true;
	rdata[1].next = NULL;

	recptr = XLogInsert(RM_HEAP2_ID, XLOG_HEAP2_FREEZE, rdata);

	return recptr;
}

/*
 * Perform XLogInsert for a heap-update operation.	Caller must already
 * have modified the buffer(s) and marked them dirty.
 */
static XLogRecPtr
log_heap_update(Relation reln, Buffer oldbuf, ItemPointerData from,
				Buffer newbuf, HeapTuple newtup, bool move)
{
	/*
	 * Note: xlhdr is declared to have adequate size and correct alignment for
	 * an xl_heap_header.  However the two tids, if present at all, will be
	 * packed in with no wasted space after the xl_heap_header; they aren't
	 * necessarily aligned as implied by this struct declaration.
	 */
	struct
	{
		xl_heap_header hdr;
		TransactionId tid1;
		TransactionId tid2;
	}			xlhdr;
	int			hsize = SizeOfHeapHeader;
	xl_heap_update xlrec;
	XLogRecPtr	recptr;
	XLogRecData rdata[4];
	Page		page = BufferGetPage(newbuf);
	uint8		info = (move) ? XLOG_HEAP_MOVE : XLOG_HEAP_UPDATE;

	/* Caller should not call me on a temp relation */
	Assert(!reln->rd_istemp);

	xl_heaptid_set(&xlrec.target, reln, &from);
	xlrec.newtid = newtup->t_self;
	rdata[0].data = (char *) &xlrec;
	rdata[0].len = SizeOfHeapUpdate;
	rdata[0].buffer = InvalidBuffer;
	rdata[0].next = &(rdata[1]);

	rdata[1].data = NULL;
	rdata[1].len = 0;
	rdata[1].buffer = oldbuf;
	rdata[1].buffer_std = true;
	rdata[1].next = &(rdata[2]);

	xlhdr.hdr.t_infomask2 = newtup->t_data->t_infomask2;
	xlhdr.hdr.t_infomask = newtup->t_data->t_infomask;
	xlhdr.hdr.t_hoff = newtup->t_data->t_hoff;
	if (move)					/* remember xmax & xmin */
	{
		TransactionId xid[2];	/* xmax, xmin */

		if (newtup->t_data->t_infomask & (HEAP_XMAX_INVALID | HEAP_IS_LOCKED))
			xid[0] = InvalidTransactionId;
		else
			xid[0] = HeapTupleHeaderGetXmax(newtup->t_data);
		xid[1] = HeapTupleHeaderGetXmin(newtup->t_data);
		memcpy((char *) &xlhdr + hsize,
			   (char *) xid,
			   2 * sizeof(TransactionId));
		hsize += 2 * sizeof(TransactionId);
	}

	/*
	 * As with insert records, we need not store the rdata[2] segment if we
	 * decide to store the whole buffer instead.
	 */
	rdata[2].data = (char *) &xlhdr;
	rdata[2].len = hsize;
	rdata[2].buffer = newbuf;
	rdata[2].buffer_std = true;
	rdata[2].next = &(rdata[3]);

	/* PG73FORMAT: write bitmap [+ padding] [+ oid] + data */
	rdata[3].data = (char *) newtup->t_data + offsetof(HeapTupleHeaderData, t_bits);
	rdata[3].len = newtup->t_len - offsetof(HeapTupleHeaderData, t_bits);
	rdata[3].buffer = newbuf;
	rdata[3].buffer_std = true;
	rdata[3].next = NULL;

	/* If new tuple is the single and first tuple on page... */
	if (ItemPointerGetOffsetNumber(&(newtup->t_self)) == FirstOffsetNumber &&
		PageGetMaxOffsetNumber(page) == FirstOffsetNumber)
	{
		info |= XLOG_HEAP_INIT_PAGE;
		rdata[2].buffer = rdata[3].buffer = InvalidBuffer;
	}

	recptr = XLogInsert(RM_HEAP_ID, info, rdata);

	return recptr;
}

/*
 * Perform XLogInsert for a heap-move operation.  Caller must already
 * have modified the buffers and marked them dirty.
 */
XLogRecPtr
log_heap_move(Relation reln, Buffer oldbuf, ItemPointerData from,
			  Buffer newbuf, HeapTuple newtup)
{
	return log_heap_update(reln, oldbuf, from, newbuf, newtup, true);
}

static void
heap_xlog_clean(XLogRecPtr lsn, XLogRecord *record)
{
	MIRROREDLOCK_BUFMGR_DECLARE;

	xl_heap_clean *xlrec = (xl_heap_clean *) XLogRecGetData(record);
	Relation	reln;
	Buffer		buffer;
	Page		page;

	if (record->xl_info & XLR_BKP_BLOCK_1)
		return;

	reln = XLogOpenRelation(xlrec->heapnode.node);
	
	// -------- MirroredLock ----------
	MIRROREDLOCK_BUFMGR_LOCK;
	
	buffer = XLogReadBuffer(reln, xlrec->block, false);
	REDO_PRINT_READ_BUFFER_NOT_FOUND(reln, xlrec->block, buffer, lsn);
	if (!BufferIsValid(buffer))
	{
		
		MIRROREDLOCK_BUFMGR_UNLOCK;
		// -------- MirroredLock ----------
		
		return;
	}
	page = (Page) BufferGetPage(buffer);

	REDO_PRINT_LSN_APPLICATION(reln, xlrec->block, page, lsn);
	if (XLByteLE(lsn, PageGetLSN(page)))
	{
		UnlockReleaseBuffer(buffer);
		
		MIRROREDLOCK_BUFMGR_UNLOCK;
		// -------- MirroredLock ----------
		
		return;
	}

	if (record->xl_len > SizeOfHeapClean)
	{
		OffsetNumber *unused;
		OffsetNumber *unend;
		ItemId		lp;

		unused = (OffsetNumber *) ((char *) xlrec + SizeOfHeapClean);
		unend = (OffsetNumber *) ((char *) xlrec + record->xl_len);

		while (unused < unend)
		{
			/* unused[] entries are zero-based */
			lp = PageGetItemId(page, *unused + 1);
			lp->lp_flags &= ~LP_USED;
			unused++;
		}
	}

	PageRepairFragmentation(page, NULL);

	PageSetLSN(page, lsn);
	PageSetTLI(page, ThisTimeLineID);
	MarkBufferDirty(buffer);
	UnlockReleaseBuffer(buffer);
	
	MIRROREDLOCK_BUFMGR_UNLOCK;
	// -------- MirroredLock ----------
	
}

static void
heap_xlog_freeze(XLogRecPtr lsn, XLogRecord *record)
{
	MIRROREDLOCK_BUFMGR_DECLARE;

	xl_heap_freeze *xlrec = (xl_heap_freeze *) XLogRecGetData(record);
	TransactionId cutoff_xid = xlrec->cutoff_xid;
	Relation	reln;
	Buffer		buffer;
	Page		page;

	if (record->xl_info & XLR_BKP_BLOCK_1)
		return;

	reln = XLogOpenRelation(xlrec->heapnode.node);
	
	// -------- MirroredLock ----------
	MIRROREDLOCK_BUFMGR_LOCK;
	
	buffer = XLogReadBuffer(reln, xlrec->block, false);
	REDO_PRINT_READ_BUFFER_NOT_FOUND(reln, xlrec->block, buffer, lsn);
	if (!BufferIsValid(buffer))
	{
		
		MIRROREDLOCK_BUFMGR_UNLOCK;
		// -------- MirroredLock ----------
		
		return;
	}
	page = (Page) BufferGetPage(buffer);

	REDO_PRINT_LSN_APPLICATION(reln, xlrec->block, page, lsn);
	if (XLByteLE(lsn, PageGetLSN(page)))
	{
		UnlockReleaseBuffer(buffer);
		
		MIRROREDLOCK_BUFMGR_UNLOCK;
		// -------- MirroredLock ----------
		
		return;
	}

	if (record->xl_len > SizeOfHeapFreeze)
	{
		OffsetNumber *offsets;
		OffsetNumber *offsets_end;

		offsets = (OffsetNumber *) ((char *) xlrec + SizeOfHeapFreeze);
		offsets_end = (OffsetNumber *) ((char *) xlrec + record->xl_len);

		while (offsets < offsets_end)
		{
			/* offsets[] entries are one-based */
			ItemId		lp = PageGetItemId(page, *offsets);
			HeapTupleHeader tuple = (HeapTupleHeader) PageGetItem(page, lp);

			(void) heap_freeze_tuple(tuple, cutoff_xid, InvalidBuffer);
			offsets++;
		}
	}

	PageSetLSN(page, lsn);
	PageSetTLI(page, ThisTimeLineID);
	MarkBufferDirty(buffer);
	UnlockReleaseBuffer(buffer);
	
	MIRROREDLOCK_BUFMGR_UNLOCK;
	// -------- MirroredLock ----------
	
}

static void
heap_xlog_newpage(XLogRecPtr lsn, XLogRecord *record)
{
	MIRROREDLOCK_BUFMGR_DECLARE;

	xl_heap_newpage *xlrec = (xl_heap_newpage *) XLogRecGetData(record);
	Relation	reln;
	Buffer		buffer;
	Page		page;

	/*
	 * Note: the NEWPAGE log record is used for both heaps and indexes, so do
	 * not do anything that assumes we are touching a heap.
	 */
	reln = XLogOpenRelation(xlrec->heapnode.node);
	
	// -------- MirroredLock ----------
	MIRROREDLOCK_BUFMGR_LOCK;
	
	buffer = XLogReadBuffer(reln, xlrec->blkno, true);
	Assert(BufferIsValid(buffer));
	page = (Page) BufferGetPage(buffer);

	Assert(record->xl_len == SizeOfHeapNewpage + BLCKSZ);
	memcpy(page, (char *) xlrec + SizeOfHeapNewpage, BLCKSZ);

	PageSetLSN(page, lsn);
	PageSetTLI(page, ThisTimeLineID);
	MarkBufferDirty(buffer);
	UnlockReleaseBuffer(buffer);
	
	MIRROREDLOCK_BUFMGR_UNLOCK;
	// -------- MirroredLock ----------
	
}

static void
heap_xlog_delete(XLogRecPtr lsn, XLogRecord *record)
{
	MIRROREDLOCK_BUFMGR_DECLARE;

	xl_heap_delete *xlrec = (xl_heap_delete *) XLogRecGetData(record);
	Relation	reln;
	Buffer		buffer;
	Page		page;
	OffsetNumber offnum;
	ItemId		lp = NULL;
	HeapTupleHeader htup;

	if (record->xl_info & XLR_BKP_BLOCK_1)
		return;

	reln = XLogOpenRelation(xlrec->target.node);
	
	// -------- MirroredLock ----------
	MIRROREDLOCK_BUFMGR_LOCK;
	
	buffer = XLogReadBuffer(reln,
							ItemPointerGetBlockNumber(&(xlrec->target.tid)),
							false);
	REDO_PRINT_READ_BUFFER_NOT_FOUND(reln, ItemPointerGetBlockNumber(&(xlrec->target.tid)), buffer, lsn);
	if (!BufferIsValid(buffer))
	{
		
		MIRROREDLOCK_BUFMGR_UNLOCK;
		// -------- MirroredLock ----------
		
		return;
	}
	page = (Page) BufferGetPage(buffer);

	REDO_PRINT_LSN_APPLICATION(reln, ItemPointerGetBlockNumber(&(xlrec->target.tid)), page, lsn);

	if (Debug_print_qd_mirroring)
		elog(LOG, "heap_xlog_delete: page lsn = (%X,%X)",
			 PageGetLSN(page).xlogid, PageGetLSN(page).xrecoff);

	if (XLByteLE(lsn, PageGetLSN(page)))		/* changes are applied */
	{
		UnlockReleaseBuffer(buffer);
		
		MIRROREDLOCK_BUFMGR_UNLOCK;
		// -------- MirroredLock ----------
		
		if (Debug_print_qd_mirroring)
		{
			elog(LOG, "delete already appplied: lsn (%X,%X), page (%X,%X)",
				 lsn.xlogid,
				 lsn.xrecoff,
				 PageGetLSN(page).xlogid,
				 PageGetLSN(page).xrecoff);
		}
		return;
	}

	offnum = ItemPointerGetOffsetNumber(&(xlrec->target.tid));
	if (PageGetMaxOffsetNumber(page) >= offnum)
		lp = PageGetItemId(page, offnum);

	if (PageGetMaxOffsetNumber(page) < offnum || !ItemIdIsUsed(lp))
		elog(PANIC, "heap_delete_redo: invalid lp");

	htup = (HeapTupleHeader) PageGetItem(page, lp);

	htup->t_infomask &= ~(HEAP_XMAX_COMMITTED |
						  HEAP_XMAX_INVALID |
						  HEAP_XMAX_IS_MULTI |
						  HEAP_IS_LOCKED |
						  HEAP_MOVED);
	HeapTupleHeaderSetXmax(htup, record->xl_xid);
	HeapTupleHeaderSetCmax(htup, FirstCommandId, false);
	/* Make sure there is no forward chain link in t_ctid */
	htup->t_ctid = xlrec->target.tid;
	PageSetLSN(page, lsn);
	PageSetTLI(page, ThisTimeLineID);

	MarkBufferDirty(buffer);

	UnlockReleaseBuffer(buffer);
	
	MIRROREDLOCK_BUFMGR_UNLOCK;
	// -------- MirroredLock ----------
	
}

static void
heap_xlog_insert(XLogRecPtr lsn, XLogRecord *record)
{
	MIRROREDLOCK_BUFMGR_DECLARE;

	xl_heap_insert *xlrec = (xl_heap_insert *) XLogRecGetData(record);
	Relation	reln;
	Buffer		buffer;
	Page		page;
	OffsetNumber offnum;
	struct
	{
		HeapTupleHeaderData hdr;
		char		data[MaxHeapTupleSize];
	}			tbuf;
	HeapTupleHeader htup;
	xl_heap_header xlhdr;
	uint32		newlen;

	if (record->xl_info & XLR_BKP_BLOCK_1)
		return;

	reln = XLogOpenRelation(xlrec->target.node);
	
	// -------- MirroredLock ----------
	MIRROREDLOCK_BUFMGR_LOCK;
	
	if (record->xl_info & XLOG_HEAP_INIT_PAGE)
	{
		buffer = XLogReadBuffer(reln,
							 ItemPointerGetBlockNumber(&(xlrec->target.tid)),
								true);
		Assert(BufferIsValid(buffer));
		page = (Page) BufferGetPage(buffer);

		PageInit(page, BufferGetPageSize(buffer), 0);
	}
	else
	{
		buffer = XLogReadBuffer(reln,
							 ItemPointerGetBlockNumber(&(xlrec->target.tid)),
								false);
		REDO_PRINT_READ_BUFFER_NOT_FOUND(reln, ItemPointerGetBlockNumber(&(xlrec->target.tid)), buffer, lsn);
		if (!BufferIsValid(buffer))
		{

			MIRROREDLOCK_BUFMGR_UNLOCK;
			// -------- MirroredLock ----------

			return;
		}

		page = (Page) BufferGetPage(buffer);

		REDO_PRINT_LSN_APPLICATION(reln,
					ItemPointerGetBlockNumber(&(xlrec->target.tid)), page, lsn);

		if (XLByteLE(lsn, PageGetLSN(page)))	/* changes are applied */
		{
			if (Debug_print_qd_mirroring)
			{
				elog(LOG, "insert already appplied: lsn (%X,%X), page (%X,%X)",
					 lsn.xlogid,
					 lsn.xrecoff,
					 PageGetLSN(page).xlogid,
					 PageGetLSN(page).xrecoff);
			}
			UnlockReleaseBuffer(buffer);

			MIRROREDLOCK_BUFMGR_UNLOCK;
			// -------- MirroredLock ----------

			return;
		}
	}

	offnum = ItemPointerGetOffsetNumber(&(xlrec->target.tid));
	if (PageGetMaxOffsetNumber(page) + 1 < offnum)
		elog(PANIC, "heap_insert_redo: invalid max offset number: "
			 "%u, expected %u", offnum, (OffsetNumber)PageGetMaxOffsetNumber(page) + 1);
	newlen = record->xl_len - SizeOfHeapInsert - SizeOfHeapHeader;
	Assert(newlen <= MaxHeapTupleSize);
	memcpy((char *) &xlhdr,
		   (char *) xlrec + SizeOfHeapInsert,
		   SizeOfHeapHeader);
	htup = &tbuf.hdr;
	MemSet((char *) htup, 0, sizeof(HeapTupleHeaderData));
	/* PG73FORMAT: get bitmap [+ padding] [+ oid] + data */
	memcpy((char *) htup + offsetof(HeapTupleHeaderData, t_bits),
		   (char *) xlrec + SizeOfHeapInsert + SizeOfHeapHeader,
		   newlen);
	newlen += offsetof(HeapTupleHeaderData, t_bits);
	htup->t_infomask2 = xlhdr.t_infomask2;
	htup->t_infomask = xlhdr.t_infomask;
	htup->t_hoff = xlhdr.t_hoff;
	HeapTupleHeaderSetXmin(htup, record->xl_xid);
	HeapTupleHeaderSetCmin(htup, FirstCommandId);
	htup->t_ctid = xlrec->target.tid;

	offnum = PageAddItem(page, (Item) htup, newlen, offnum,
						 LP_USED | OverwritePageMode);
	if (offnum == InvalidOffsetNumber)
		elog(PANIC, "heap_insert_redo: failed to add tuple");

	PageSetLSN(page, lsn);
	PageSetTLI(page, ThisTimeLineID);
	MarkBufferDirty(buffer);
	UnlockReleaseBuffer(buffer);
	
	MIRROREDLOCK_BUFMGR_UNLOCK;
	// -------- MirroredLock ----------
	
}

/*
 * Handles UPDATE & MOVE
 */
static void
heap_xlog_update(XLogRecPtr lsn, XLogRecord *record, bool move)
{
	MIRROREDLOCK_BUFMGR_DECLARE;

	xl_heap_update *xlrec = (xl_heap_update *) XLogRecGetData(record);
	Relation	reln = XLogOpenRelation(xlrec->target.node);
	Buffer		buffer;
	bool		samepage = (ItemPointerGetBlockNumber(&(xlrec->newtid)) ==
							ItemPointerGetBlockNumber(&(xlrec->target.tid)));
	Page		page;
	OffsetNumber offnum;
	ItemId		lp = NULL;
	HeapTupleHeader htup;
	struct
	{
		HeapTupleHeaderData hdr;
		char		data[MaxHeapTupleSize];
	}			tbuf;
	xl_heap_header xlhdr;
	int			hsize;
	uint32		newlen;

	if (record->xl_info & XLR_BKP_BLOCK_1)
	{
		if (samepage)
			return;				/* backup block covered both changes */
		
		// -------- MirroredLock ----------
		MIRROREDLOCK_BUFMGR_LOCK;
		
		goto newt;
	}

	/* Deal with old tuple version */
	
	// -------- MirroredLock ----------
	MIRROREDLOCK_BUFMGR_LOCK;
	
	buffer = XLogReadBuffer(reln,
							ItemPointerGetBlockNumber(&(xlrec->target.tid)),
							false);
	REDO_PRINT_READ_BUFFER_NOT_FOUND(reln, ItemPointerGetBlockNumber(&(xlrec->target.tid)), buffer, lsn);
	if (!BufferIsValid(buffer))
		goto newt;
	page = (Page) BufferGetPage(buffer);

	REDO_PRINT_LSN_APPLICATION(reln, ItemPointerGetBlockNumber(&(xlrec->target.tid)), page, lsn);
	if (XLByteLE(lsn, PageGetLSN(page)))		/* changes are applied */
	{
		UnlockReleaseBuffer(buffer);
		if (samepage)
		{

			MIRROREDLOCK_BUFMGR_UNLOCK;
			// -------- MirroredLock ----------

			return;
		}
		goto newt;
	}

	offnum = ItemPointerGetOffsetNumber(&(xlrec->target.tid));
	if (PageGetMaxOffsetNumber(page) >= offnum)
		lp = PageGetItemId(page, offnum);

	if (PageGetMaxOffsetNumber(page) < offnum || !ItemIdIsUsed(lp))
		elog(PANIC, "heap_update_redo: invalid lp");

	htup = (HeapTupleHeader) PageGetItem(page, lp);

	if (move)
	{
		htup->t_infomask &= ~(HEAP_XMIN_COMMITTED |
							  HEAP_XMIN_INVALID |
							  HEAP_MOVED_IN);
		htup->t_infomask |= HEAP_MOVED_OFF;
		HeapTupleHeaderSetXvac(htup, record->xl_xid);
		/* Make sure there is no forward chain link in t_ctid */
		htup->t_ctid = xlrec->target.tid;
	}
	else
	{
		htup->t_infomask &= ~(HEAP_XMAX_COMMITTED |
							  HEAP_XMAX_INVALID |
							  HEAP_XMAX_IS_MULTI |
							  HEAP_IS_LOCKED |
							  HEAP_MOVED);
		HeapTupleHeaderSetXmax(htup, record->xl_xid);
		HeapTupleHeaderSetCmax(htup, FirstCommandId, false);
		/* Set forward chain link in t_ctid */
		htup->t_ctid = xlrec->newtid;
	}

	/*
	 * this test is ugly, but necessary to avoid thinking that insert change
	 * is already applied
	 */
	if (samepage)
		goto newsame;
	PageSetLSN(page, lsn);
	PageSetTLI(page, ThisTimeLineID);
	MarkBufferDirty(buffer);
	UnlockReleaseBuffer(buffer);

	/* Deal with new tuple */

newt:;

	if (record->xl_info & XLR_BKP_BLOCK_2)
	{		
		MIRROREDLOCK_BUFMGR_UNLOCK;
		// -------- MirroredLock ----------
		
		return;
	}

	if (record->xl_info & XLOG_HEAP_INIT_PAGE)
	{
		buffer = XLogReadBuffer(reln,
								ItemPointerGetBlockNumber(&(xlrec->newtid)),
								true);
		Assert(BufferIsValid(buffer));
		page = (Page) BufferGetPage(buffer);

		PageInit(page, BufferGetPageSize(buffer), 0);
	}
	else
	{
		buffer = XLogReadBuffer(reln,
								ItemPointerGetBlockNumber(&(xlrec->newtid)),
								false);
		REDO_PRINT_READ_BUFFER_NOT_FOUND(reln, ItemPointerGetBlockNumber(&(xlrec->newtid)), buffer, lsn);
		if (!BufferIsValid(buffer))
		{
			MIRROREDLOCK_BUFMGR_UNLOCK;
			// -------- MirroredLock ----------
			
			return;
		}
		page = (Page) BufferGetPage(buffer);

		REDO_PRINT_LSN_APPLICATION(reln, ItemPointerGetBlockNumber(&(xlrec->newtid)), page, lsn);
		if (XLByteLE(lsn, PageGetLSN(page)))	/* changes are applied */
		{
			UnlockReleaseBuffer(buffer);

			MIRROREDLOCK_BUFMGR_UNLOCK;
			// -------- MirroredLock ----------

			return;
		}
	}

newsame:;

	offnum = ItemPointerGetOffsetNumber(&(xlrec->newtid));
	if (PageGetMaxOffsetNumber(page) + 1 < offnum)
		elog(PANIC, "heap_update_redo: invalid max offset number");

	hsize = SizeOfHeapUpdate + SizeOfHeapHeader;
	if (move)
		hsize += (2 * sizeof(TransactionId));

	newlen = record->xl_len - hsize;
	Assert(newlen <= MaxHeapTupleSize);
	memcpy((char *) &xlhdr,
		   (char *) xlrec + SizeOfHeapUpdate,
		   SizeOfHeapHeader);
	htup = &tbuf.hdr;
	MemSet((char *) htup, 0, sizeof(HeapTupleHeaderData));
	/* PG73FORMAT: get bitmap [+ padding] [+ oid] + data */
	memcpy((char *) htup + offsetof(HeapTupleHeaderData, t_bits),
		   (char *) xlrec + hsize,
		   newlen);
	newlen += offsetof(HeapTupleHeaderData, t_bits);
	htup->t_infomask2 = xlhdr.t_infomask2;
	htup->t_infomask = xlhdr.t_infomask;
	htup->t_hoff = xlhdr.t_hoff;

	if (move)
	{
		TransactionId xid[2];	/* xmax, xmin */

		memcpy((char *) xid,
			   (char *) xlrec + SizeOfHeapUpdate + SizeOfHeapHeader,
			   2 * sizeof(TransactionId));
		HeapTupleHeaderSetXmin(htup, xid[1]);
		HeapTupleHeaderSetXmax(htup, xid[0]);
		HeapTupleHeaderSetXvac(htup, record->xl_xid);
	}
	else
	{
		HeapTupleHeaderSetXmin(htup, record->xl_xid);
		HeapTupleHeaderSetCmin(htup, FirstCommandId);
	}
	/* Make sure there is no forward chain link in t_ctid */
	htup->t_ctid = xlrec->newtid;

	offnum = PageAddItem(page, (Item) htup, newlen, offnum,
						 LP_USED | OverwritePageMode);
	if (offnum == InvalidOffsetNumber)
		elog(PANIC, "heap_update_redo: failed to add tuple");
	PageSetLSN(page, lsn);
	PageSetTLI(page, ThisTimeLineID);
	MarkBufferDirty(buffer);
	UnlockReleaseBuffer(buffer);
	
	MIRROREDLOCK_BUFMGR_UNLOCK;
	// -------- MirroredLock ----------
	
}

static void
heap_xlog_lock(XLogRecPtr lsn, XLogRecord *record)
{
	MIRROREDLOCK_BUFMGR_DECLARE;

	xl_heap_lock *xlrec = (xl_heap_lock *) XLogRecGetData(record);
	Relation	reln;
	Buffer		buffer;
	Page		page;
	OffsetNumber offnum;
	ItemId		lp = NULL;
	HeapTupleHeader htup;

	if (record->xl_info & XLR_BKP_BLOCK_1)
		return;

	reln = XLogOpenRelation(xlrec->target.node);
	
	// -------- MirroredLock ----------
	MIRROREDLOCK_BUFMGR_LOCK;
	
	buffer = XLogReadBuffer(reln,
							ItemPointerGetBlockNumber(&(xlrec->target.tid)),
							false);
	REDO_PRINT_READ_BUFFER_NOT_FOUND(reln, ItemPointerGetBlockNumber(&(xlrec->target.tid)), buffer, lsn);
	if (!BufferIsValid(buffer))
	{
		
		MIRROREDLOCK_BUFMGR_UNLOCK;
		// -------- MirroredLock ----------
		
		return;
	}
	page = (Page) BufferGetPage(buffer);

	REDO_PRINT_LSN_APPLICATION(reln, ItemPointerGetBlockNumber(&(xlrec->target.tid)), page, lsn);
	if (XLByteLE(lsn, PageGetLSN(page)))		/* changes are applied */
	{
		UnlockReleaseBuffer(buffer);
		
		MIRROREDLOCK_BUFMGR_UNLOCK;
		// -------- MirroredLock ----------
		
		return;
	}

	offnum = ItemPointerGetOffsetNumber(&(xlrec->target.tid));
	if (PageGetMaxOffsetNumber(page) >= offnum)
		lp = PageGetItemId(page, offnum);

	if (PageGetMaxOffsetNumber(page) < offnum || !ItemIdIsUsed(lp))
		elog(PANIC, "heap_lock_redo: invalid lp");

	htup = (HeapTupleHeader) PageGetItem(page, lp);

	htup->t_infomask &= ~(HEAP_XMAX_COMMITTED |
						  HEAP_XMAX_INVALID |
						  HEAP_XMAX_IS_MULTI |
						  HEAP_IS_LOCKED |
						  HEAP_MOVED);
	if (xlrec->xid_is_mxact)
		htup->t_infomask |= HEAP_XMAX_IS_MULTI;
	if (xlrec->shared_lock)
		htup->t_infomask |= HEAP_XMAX_SHARED_LOCK;
	else
		htup->t_infomask |= HEAP_XMAX_EXCL_LOCK;
	HeapTupleHeaderSetXmax(htup, xlrec->locking_xid);
	HeapTupleHeaderSetCmax(htup, FirstCommandId, false);
	/* Make sure there is no forward chain link in t_ctid */
	htup->t_ctid = xlrec->target.tid;
	PageSetLSN(page, lsn);
	PageSetTLI(page, ThisTimeLineID);
	MarkBufferDirty(buffer);
	UnlockReleaseBuffer(buffer);
	
	MIRROREDLOCK_BUFMGR_UNLOCK;
	// -------- MirroredLock ----------
	
}

static void
heap_xlog_inplace(XLogRecPtr lsn, XLogRecord *record)
{
	MIRROREDLOCK_BUFMGR_DECLARE;

	xl_heap_inplace *xlrec = (xl_heap_inplace *) XLogRecGetData(record);
	Relation	reln = XLogOpenRelation(xlrec->target.node);
	Buffer		buffer;
	Page		page;
	OffsetNumber offnum;
	ItemId		lp = NULL;
	HeapTupleHeader htup;
	uint32		oldlen;
	uint32		newlen;

	if (record->xl_info & XLR_BKP_BLOCK_1)
		return;
	
	// -------- MirroredLock ----------
	MIRROREDLOCK_BUFMGR_LOCK;
	
	buffer = XLogReadBuffer(reln,
							ItemPointerGetBlockNumber(&(xlrec->target.tid)),
							false);
	REDO_PRINT_READ_BUFFER_NOT_FOUND(reln, ItemPointerGetBlockNumber(&(xlrec->target.tid)), buffer, lsn);
	if (!BufferIsValid(buffer))
	{
		
		MIRROREDLOCK_BUFMGR_UNLOCK;
		// -------- MirroredLock ----------
		
		return;
	}
	page = (Page) BufferGetPage(buffer);

	REDO_PRINT_LSN_APPLICATION(reln, ItemPointerGetBlockNumber(&(xlrec->target.tid)), page, lsn);
	if (XLByteLE(lsn, PageGetLSN(page)))		/* changes are applied */
	{
		UnlockReleaseBuffer(buffer);
		
		MIRROREDLOCK_BUFMGR_UNLOCK;
		// -------- MirroredLock ----------
		
		return;
	}

	offnum = ItemPointerGetOffsetNumber(&(xlrec->target.tid));
	if (PageGetMaxOffsetNumber(page) >= offnum)
		lp = PageGetItemId(page, offnum);

	if (PageGetMaxOffsetNumber(page) < offnum || !ItemIdIsUsed(lp))
		elog(PANIC, "heap_inplace_redo: invalid lp");

	htup = (HeapTupleHeader) PageGetItem(page, lp);

	oldlen = ItemIdGetLength(lp) - htup->t_hoff;
	newlen = record->xl_len - SizeOfHeapInplace;
	if (oldlen != newlen)
		elog(PANIC, "heap_inplace_redo: wrong tuple length");

	memcpy((char *) htup + htup->t_hoff,
		   (char *) xlrec + SizeOfHeapInplace,
		   newlen);

	PageSetLSN(page, lsn);
	PageSetTLI(page, ThisTimeLineID);
	MarkBufferDirty(buffer);
	UnlockReleaseBuffer(buffer);
	
	MIRROREDLOCK_BUFMGR_UNLOCK;
	// -------- MirroredLock ----------
	
}

void
heap_redo(XLogRecPtr beginLoc, XLogRecPtr lsn, XLogRecord *record)
{
	uint8		info = record->xl_info & ~XLR_INFO_MASK;

	info &= XLOG_HEAP_OPMASK;
	if (info == XLOG_HEAP_INSERT)
		heap_xlog_insert(lsn, record);
	else if (info == XLOG_HEAP_DELETE)
		heap_xlog_delete(lsn, record);
	else if (info == XLOG_HEAP_UPDATE)
		heap_xlog_update(lsn, record, false);
	else if (info == XLOG_HEAP_MOVE)
		heap_xlog_update(lsn, record, true);
	else if (info == XLOG_HEAP_CLEAN)
		heap_xlog_clean(lsn, record);
	else if (info == XLOG_HEAP_NEWPAGE)
		heap_xlog_newpage(lsn, record);
	else if (info == XLOG_HEAP_LOCK)
		heap_xlog_lock(lsn, record);
	else if (info == XLOG_HEAP_INPLACE)
		heap_xlog_inplace(lsn, record);
	else
		elog(PANIC, "heap_redo: unknown op code %u", info);
}

void
heap2_redo(XLogRecPtr beginLoc, XLogRecPtr lsn, XLogRecord *record)
{
	uint8		info = record->xl_info & ~XLR_INFO_MASK;

	info &= XLOG_HEAP_OPMASK;
	if (info == XLOG_HEAP2_FREEZE)
		heap_xlog_freeze(lsn, record);
	else
		elog(PANIC, "heap2_redo: unknown op code %u", info);
}

static void
out_target(StringInfo buf, xl_heaptid *target)
{
	appendStringInfo(buf, "rel %u/%u/%u; tid %u/%u",
			 target->node.spcNode, target->node.dbNode, target->node.relNode,
					 ItemPointerGetBlockNumber(&(target->tid)),
					 ItemPointerGetOffsetNumber(&(target->tid)));
}

void
heap_desc(StringInfo buf, XLogRecPtr beginLoc, XLogRecord *record)
{
	uint8		info = record->xl_info & ~XLR_INFO_MASK;
	char		*rec = XLogRecGetData(record);

	info &= XLOG_HEAP_OPMASK;
	if (info == XLOG_HEAP_INSERT)
	{
		xl_heap_insert *xlrec = (xl_heap_insert *) rec;

		if (record->xl_info & XLOG_HEAP_INIT_PAGE)
			appendStringInfo(buf, "insert(init): ");
		else
			appendStringInfo(buf, "insert: ");
		out_target(buf, &(xlrec->target));
	}
	else if (info == XLOG_HEAP_DELETE)
	{
		xl_heap_delete *xlrec = (xl_heap_delete *) rec;

		appendStringInfo(buf, "delete: ");
		out_target(buf, &(xlrec->target));
	}
	else if (info == XLOG_HEAP_UPDATE)
	{
		xl_heap_update *xlrec = (xl_heap_update *) rec;

		if (record->xl_info & XLOG_HEAP_INIT_PAGE)
			appendStringInfo(buf, "update(init): ");
		else
			appendStringInfo(buf, "update: ");
		out_target(buf, &(xlrec->target));
		appendStringInfo(buf, "; new %s",
						 ItemPointerToString(&(xlrec->newtid)));
	}
	else if (info == XLOG_HEAP_MOVE)
	{
		xl_heap_update *xlrec = (xl_heap_update *) rec;

		if (record->xl_info & XLOG_HEAP_INIT_PAGE)
			appendStringInfo(buf, "move(init): ");
		else
			appendStringInfo(buf, "move: ");
		out_target(buf, &(xlrec->target));
		appendStringInfo(buf, "; new %s",
						 ItemPointerToString(&(xlrec->newtid)));
	}
	else if (info == XLOG_HEAP_CLEAN)
	{
		xl_heap_clean *xlrec = (xl_heap_clean *) rec;

		appendStringInfo(buf, "clean: rel %u/%u/%u; blk %u",
						 xlrec->heapnode.node.spcNode, xlrec->heapnode.node.dbNode,
						 xlrec->heapnode.node.relNode, xlrec->block);
	}
	else if (info == XLOG_HEAP_NEWPAGE)
	{
		xl_heap_newpage *xlrec = (xl_heap_newpage *) rec;

		appendStringInfo(buf, "newpage: rel %u/%u/%u; blk %u",
						 xlrec->heapnode.node.spcNode, xlrec->heapnode.node.dbNode,
						 xlrec->heapnode.node.relNode, xlrec->blkno);
	}
	else if (info == XLOG_HEAP_LOCK)
	{
		xl_heap_lock *xlrec = (xl_heap_lock *) rec;

		if (xlrec->shared_lock)
			appendStringInfo(buf, "shared_lock: ");
		else
			appendStringInfo(buf, "exclusive_lock: ");
		if (xlrec->xid_is_mxact)
			appendStringInfo(buf, "mxid ");
		else
			appendStringInfo(buf, "xid ");
		appendStringInfo(buf, "%u ", xlrec->locking_xid);
		out_target(buf, &(xlrec->target));
	}
	else if (info == XLOG_HEAP_INPLACE)
	{
		xl_heap_inplace *xlrec = (xl_heap_inplace *) rec;

		appendStringInfo(buf, "inplace: ");
		out_target(buf, &(xlrec->target));
	}
	else
		appendStringInfo(buf, "UNKNOWN");
}

bool heap_getrelfilenode(
	XLogRecord 		*record,
	RelFileNode		*relFileNode)
{
	uint8		info = record->xl_info & ~XLR_INFO_MASK;
	void*		data = XLogRecGetData(record);

	info &= XLOG_HEAP_OPMASK;
	if (info == XLOG_HEAP_INSERT)
	{
		xl_heap_insert *xlrec = (xl_heap_insert *) data;

		*relFileNode = xlrec->target.node;
	}
	else if (info == XLOG_HEAP_DELETE)
	{
		xl_heap_delete *xlrec = (xl_heap_delete *) data;

		*relFileNode = xlrec->target.node;
	}
	else if (info == XLOG_HEAP_UPDATE)
	{
		xl_heap_update *xlrec = (xl_heap_update *) data;

		*relFileNode = xlrec->target.node;
	}
	else if (info == XLOG_HEAP_MOVE)
	{
		xl_heap_update *xlrec = (xl_heap_update *) data;

		*relFileNode = xlrec->target.node;
	}
	else if (info == XLOG_HEAP_CLEAN)
	{
		xl_heap_clean *xlrec = (xl_heap_clean *) data;

		*relFileNode = xlrec->heapnode.node;
	}
	else if (info == XLOG_HEAP_NEWPAGE)
	{
		xl_heap_newpage *xlrec = (xl_heap_newpage *) data;

		*relFileNode = xlrec->heapnode.node;
	}
	else if (info == XLOG_HEAP_LOCK)
	{
		xl_heap_lock *xlrec = (xl_heap_lock *) data;

		*relFileNode = xlrec->target.node;
	}
	else if (info == XLOG_HEAP_INPLACE)
	{
		xl_heap_inplace *xlrec = (xl_heap_inplace *) data;

		*relFileNode = xlrec->target.node;
	}
	else
		elog(PANIC, "heap_getrelfilenode: unknown op code %u", info);

	return true;
}

void
heap2_desc(StringInfo buf, XLogRecPtr beginLoc, XLogRecord *record)
{
	uint8		info = record->xl_info & ~XLR_INFO_MASK;
	char		*rec = XLogRecGetData(record);

	info &= XLOG_HEAP_OPMASK;
	if (info == XLOG_HEAP2_FREEZE)
	{
		xl_heap_freeze *xlrec = (xl_heap_freeze *) rec;

		appendStringInfo(buf, "freeze: rel %u/%u/%u; blk %u; cutoff %u",
						 xlrec->heapnode.node.spcNode, xlrec->heapnode.node.dbNode,
						 xlrec->heapnode.node.relNode, xlrec->block,
						 xlrec->cutoff_xid);
	}
	else
		appendStringInfo(buf, "UNKNOWN");
}
