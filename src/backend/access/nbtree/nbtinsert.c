/*-------------------------------------------------------------------------
 *
 * nbtinsert.c
 *	  Item insertion in Lehman and Yao btrees for Postgres.
 *
 * Portions Copyright (c) 1996-2008, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  $PostgreSQL: pgsql/src/backend/access/nbtree/nbtinsert.c,v 1.146.2.2 2007/12/31 04:52:20 tgl Exp $
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "access/heapam.h"
#include "access/nbtree.h"
#include "access/transam.h"
#include "cdb/cdbappendonlyam.h"
#include "cdb/cdbvars.h"  /*Gp_is_primary*/
#include "miscadmin.h"
#include "utils/inval.h"


typedef struct
{
	/* context data for _bt_checksplitloc */
	Size		newitemsz;		/* size of new item to be inserted */
	int			fillfactor;		/* needed when splitting rightmost page */
	bool		is_leaf;		/* T if splitting a leaf page */
	bool		is_rightmost;	/* T if splitting a rightmost page */

	bool		have_split;		/* found a valid split? */

	/* these fields valid only if have_split is true */
	bool		newitemonleft;	/* new item on left or right of best split */
	OffsetNumber firstright;	/* best split point */
	int			best_delta;		/* best size delta so far */
} FindSplitData;


static Buffer _bt_newroot(Relation rel, Buffer lbuf, Buffer rbuf);

static TransactionId _bt_check_unique(Relation rel, IndexTuple itup,
				 Relation heapRel, Buffer buf,
				 ScanKey itup_scankey);
static void _bt_insertonpg(Relation rel, Buffer buf,
			   BTStack stack,
			   int keysz, ScanKey scankey,
			   IndexTuple itup,
			   OffsetNumber afteritem,
			   bool split_only_page);
static Buffer _bt_split(Relation rel, Buffer buf, OffsetNumber firstright,
		  OffsetNumber newitemoff, Size newitemsz,
		  IndexTuple newitem, bool newitemonleft);
static OffsetNumber _bt_findsplitloc(Relation rel, Page page,
				 OffsetNumber newitemoff,
				 Size newitemsz,
				 bool *newitemonleft);
static void _bt_checksplitloc(FindSplitData *state, OffsetNumber firstright,
				  int leftfree, int rightfree,
				  bool newitemonleft, Size firstrightitemsz);
static void _bt_pgaddtup(Relation rel, Page page,
			 Size itemsize, IndexTuple itup,
			 OffsetNumber itup_off, const char *where);
static bool _bt_isequal(TupleDesc itupdesc, Page page, OffsetNumber offnum,
			int keysz, ScanKey scankey);
static void _bt_vacuum_one_page(Relation rel, Buffer buffer);


/*
 *	_bt_doinsert() -- Handle insertion of a single index tuple in the tree.
 *
 *		This routine is called by the public interface routines, btbuild
 *		and btinsert.  By here, itup is filled in, including the TID.
 */
void
_bt_doinsert(Relation rel, IndexTuple itup,
			 bool index_is_unique, Relation heapRel)
{
	MIRROREDLOCK_BUFMGR_DECLARE;

	int			natts = rel->rd_rel->relnatts;
	ScanKey		itup_scankey;
	BTStack		stack;
	Buffer		buf;

	/* we need an insertion scan key to do our search, so build one */
	itup_scankey = _bt_mkscankey(rel, itup);
	
top:
	// -------- MirroredLock ----------
	MIRROREDLOCK_BUFMGR_LOCK;
	
	/* find the first page containing this key */
	stack = _bt_search(rel, natts, itup_scankey, false, &buf, BT_WRITE);

	/* trade in our read lock for a write lock */
	LockBuffer(buf, BUFFER_LOCK_UNLOCK);
	LockBuffer(buf, BT_WRITE);

	/*
	 * If the page was split between the time that we surrendered our read
	 * lock and acquired our write lock, then this page may no longer be the
	 * right place for the key we want to insert.  In this case, we need to
	 * move right in the tree.	See Lehman and Yao for an excruciatingly
	 * precise description.
	 */
	buf = _bt_moveright(rel, buf, natts, itup_scankey, false, BT_WRITE);

	/*
	 * If we're not allowing duplicates, make sure the key isn't already in
	 * the index.
	 *
	 * NOTE: obviously, _bt_check_unique can only detect keys that are already
	 * in the index; so it cannot defend against concurrent insertions of the
	 * same key.  We protect against that by means of holding a write lock on
	 * the target page.  Any other would-be inserter of the same key must
	 * acquire a write lock on the same target page, so only one would-be
	 * inserter can be making the check at one time.  Furthermore, once we are
	 * past the check we hold write locks continuously until we have performed
	 * our insertion, so no later inserter can fail to see our insertion.
	 * (This requires some care in _bt_insertonpg.)
	 *
	 * If we must wait for another xact, we release the lock while waiting,
	 * and then must start over completely.
	 */
	if (index_is_unique)
	{
		TransactionId xwait;

		xwait = _bt_check_unique(rel, itup, heapRel, buf, itup_scankey);

		if (TransactionIdIsValid(xwait) )
		{
			/* Have to wait for the other guy ... */
			_bt_relbuf(rel, buf);
			/*
			 * We have to unlock it to resume interrupts.  In case we wait for
			 * the lock in XactLockTableWait and a cancellation is requested,
			 * we should be able to respond it.
			 */
			MIRROREDLOCK_BUFMGR_UNLOCK;
			XactLockTableWait(xwait);
			/* start over... */
			_bt_freestack(stack);
			goto top;
		}
	}

	/* do the insertion */
	_bt_insertonpg(rel, buf, stack, natts, itup_scankey, itup, 0, false);
	
	MIRROREDLOCK_BUFMGR_UNLOCK;
	// -------- MirroredLock ----------
	
	/* be tidy */
	_bt_freestack(stack);
	_bt_freeskey(itup_scankey);
}

/*
 *	_bt_check_unique() -- Check for violation of unique index constraint
 *
 * Returns InvalidTransactionId if there is no conflict, else an xact ID
 * we must wait for to see if it commits a conflicting tuple.	If an actual
 * conflict is detected, no return --- just ereport().
 */
static TransactionId
_bt_check_unique(Relation rel, IndexTuple itup, Relation heapRel,
				 Buffer buf, ScanKey itup_scankey)
{
	TupleDesc	itupdesc = RelationGetDescr(rel);
	int			natts = rel->rd_rel->relnatts;
	OffsetNumber offset,
				maxoff;
	Page		page;
	BTPageOpaque opaque;
	Buffer		nbuf = InvalidBuffer;

	MIRROREDLOCK_BUFMGR_MUST_ALREADY_BE_HELD;

	page = BufferGetPage(buf);
	opaque = (BTPageOpaque) PageGetSpecialPointer(page);
	maxoff = PageGetMaxOffsetNumber(page);

	/*
	 * Find first item >= proposed new item.  Note we could also get a pointer
	 * to end-of-page here.
	 */
	offset = _bt_binsrch(rel, buf, natts, itup_scankey, false);

	/*
	 * Scan over all equal tuples, looking for live conflicts.
	 */
	for (;;)
	{
		HeapTupleData htup;
		Buffer		hbuffer;
		ItemId		curitemid;
		IndexTuple	curitup;
		BlockNumber nblkno;

		/*
		 * make sure the offset points to an actual item before trying to
		 * examine it...
		 */
		if (offset <= maxoff)
		{
			curitemid = PageGetItemId(page, offset);

			/*
			 * We can skip items that are marked killed.
			 *
			 * Formerly, we applied _bt_isequal() before checking the kill
			 * flag, so as to fall out of the item loop as soon as possible.
			 * However, in the presence of heavy update activity an index may
			 * contain many killed items with the same key; running
			 * _bt_isequal() on each killed item gets expensive. Furthermore
			 * it is likely that the non-killed version of each key appears
			 * first, so that we didn't actually get to exit any sooner
			 * anyway. So now we just advance over killed items as quickly as
			 * we can. We only apply _bt_isequal() when we get to a non-killed
			 * item or the end of the page.
			 */
			if (!ItemIdDeleted(curitemid))
			{
				/*
				 * _bt_compare returns 0 for (1,NULL) and (1,NULL) - this's
				 * how we handling NULLs - and so we must not use _bt_compare
				 * in real comparison, but only for ordering/finding items on
				 * pages. - vadim 03/24/97
				 */
				if (!_bt_isequal(itupdesc, page, offset, natts, itup_scankey))
					break;		/* we're past all the equal tuples */

				/* okay, we gotta fetch the heap tuple ... */
				curitup = (IndexTuple) PageGetItem(page, curitemid);

				/*
				 * If the parent relation is an AO/CO table, we have to find out
				 * if this tuple is actually in the table.
				 */
				{
					htup.t_self = curitup->t_tid;
					if (heap_fetch(heapRel, SnapshotDirty, &htup, &hbuffer,
								   true, NULL))
					{
						/* it is a duplicate */
						TransactionId xwait =
							(TransactionIdIsValid(SnapshotDirty->xmin)) ?
							SnapshotDirty->xmin : SnapshotDirty->xmax;

						ReleaseBuffer(hbuffer);
						
						/*
						 * If this tuple is being updated by other transaction
						 * then we have to wait for its commit/abort.
						 */
						if (TransactionIdIsValid(xwait))
						{
							if (nbuf != InvalidBuffer)
								_bt_relbuf(rel, nbuf);
							/* Tell _bt_doinsert to wait... */
							return xwait;
						}
						
						/*
						 * Otherwise we have a definite conflict.  But before
						 * complaining, look to see if the tuple we want to insert
						 * is itself now committed dead --- if so, don't complain.
						 * This is a waste of time in normal scenarios but we must
						 * do it to support CREATE INDEX CONCURRENTLY.
						 */
						htup.t_self = itup->t_tid;
						if (heap_fetch(heapRel, SnapshotSelf, &htup, &hbuffer,
									   false, NULL))
						{
							/* Normal case --- it's still live */
							ReleaseBuffer(hbuffer);
						}
						else if (htup.t_data != NULL)
						{
							/*
							 * It's been deleted, so no error, and no need to
							 * continue searching
							 */
							break;
						}
						else
						{
							/* couldn't find the tuple?? */
							elog(ERROR, "failed to fetch tuple being inserted");
						}
						
						ereport(ERROR,
								(errcode(ERRCODE_UNIQUE_VIOLATION),
								 errmsg("duplicate key violates unique constraint \"%s\"",
										RelationGetRelationName(rel)),
								 errOmitLocation(true)));
					}
					else if (htup.t_data != NULL)
					{
						/*
						 * Hmm, if we can't see the tuple, maybe it can be marked
						 * killed.	This logic should match index_getnext and
						 * btgettuple.
						 */
						LockBuffer(hbuffer, BUFFER_LOCK_SHARE);
						if (HeapTupleSatisfiesVacuum(htup.t_data, RecentGlobalXmin,
													 hbuffer, true) == HEAPTUPLE_DEAD)
						{
							curitemid->lp_flags |= LP_DELETE;
							opaque->btpo_flags |= BTP_HAS_GARBAGE;
							/* be sure to mark the proper buffer dirty... */
							if (nbuf != InvalidBuffer)
								SetBufferCommitInfoNeedsSave(nbuf);
							else
								SetBufferCommitInfoNeedsSave(buf);
						}
						LockBuffer(hbuffer, BUFFER_LOCK_UNLOCK);
					}
					ReleaseBuffer(hbuffer);
				}
			}
		}

		/*
		 * Advance to next tuple to continue checking.
		 */
		if (offset < maxoff)
			offset = OffsetNumberNext(offset);
		else
		{
			/* If scankey == hikey we gotta check the next page too */
			if (P_RIGHTMOST(opaque))
				break;
			if (!_bt_isequal(itupdesc, page, P_HIKEY,
							 natts, itup_scankey))
				break;
			/* Advance to next non-dead page --- there must be one */
			for (;;)
			{
				nblkno = opaque->btpo_next;
				nbuf = _bt_relandgetbuf(rel, nbuf, nblkno, BT_READ);
				page = BufferGetPage(nbuf);
				opaque = (BTPageOpaque) PageGetSpecialPointer(page);
				if (!P_IGNORE(opaque))
					break;
				if (P_RIGHTMOST(opaque))
					elog(ERROR, "fell off the end of index \"%s\"",
						 RelationGetRelationName(rel));
			}
			maxoff = PageGetMaxOffsetNumber(page);
			offset = P_FIRSTDATAKEY(opaque);
		}
	}

	if (nbuf != InvalidBuffer)
		_bt_relbuf(rel, nbuf);

	return InvalidTransactionId;
}

/*----------
 *	_bt_insertonpg() -- Insert a tuple on a particular page in the index.
 *
 *		This recursive procedure does the following things:
 *
 *			+  finds the right place to insert the tuple.
 *			+  if necessary, splits the target page (making sure that the
 *			   split is equitable as far as post-insert free space goes).
 *			+  inserts the tuple.
 *			+  if the page was split, pops the parent stack, and finds the
 *			   right place to insert the new child pointer (by walking
 *			   right using information stored in the parent stack).
 *			+  invokes itself with the appropriate tuple for the right
 *			   child page on the parent.
 *			+  updates the metapage if a true root or fast root is split.
 *
 *		On entry, we must have the right buffer in which to do the
 *		insertion, and the buffer must be pinned and write-locked.	On return,
 *		we will have dropped both the pin and the lock on the buffer.
 *
 *		If 'afteritem' is >0 then the new tuple must be inserted after the
 *		existing item of that number, noplace else.  If 'afteritem' is 0
 *		then the procedure finds the exact spot to insert it by searching.
 *		(keysz and scankey parameters are used ONLY if afteritem == 0.
 *		The scankey must be an insertion-type scankey.)
 *
 *		NOTE: if the new key is equal to one or more existing keys, we can
 *		legitimately place it anywhere in the series of equal keys --- in fact,
 *		if the new key is equal to the page's "high key" we can place it on
 *		the next page.	If it is equal to the high key, and there's not room
 *		to insert the new tuple on the current page without splitting, then
 *		we can move right hoping to find more free space and avoid a split.
 *		(We should not move right indefinitely, however, since that leads to
 *		O(N^2) insertion behavior in the presence of many equal keys.)
 *		Once we have chosen the page to put the key on, we'll insert it before
 *		any existing equal keys because of the way _bt_binsrch() works.
 *
 *		The locking interactions in this code are critical.  You should
 *		grok Lehman and Yao's paper before making any changes.  In addition,
 *		you need to understand how we disambiguate duplicate keys in this
 *		implementation, in order to be able to find our location using
 *		L&Y "move right" operations.  Since we may insert duplicate user
 *		keys, and since these dups may propagate up the tree, we use the
 *		'afteritem' parameter to position ourselves correctly for the
 *		insertion on internal pages.
 *----------
 */
static void
_bt_insertonpg(Relation rel,
			   Buffer buf,
			   BTStack stack,
			   int keysz,
			   ScanKey scankey,
			   IndexTuple itup,
			   OffsetNumber afteritem,
			   bool split_only_page)
{
	Page		page;
	BTPageOpaque lpageop;
	OffsetNumber newitemoff;
	OffsetNumber firstright = InvalidOffsetNumber;
	Size		itemsz;

	MIRROREDLOCK_BUFMGR_MUST_ALREADY_BE_HELD;

	// Fetch gp_persistent_relation_node information that will be added to XLOG record.
	RelationFetchGpRelationNodeForXLog(rel);

	page = BufferGetPage(buf);
	lpageop = (BTPageOpaque) PageGetSpecialPointer(page);

	itemsz = IndexTupleDSize(*itup);
	itemsz = MAXALIGN(itemsz);	/* be safe, PageAddItem will do this but we
								 * need to be consistent */

	/*
	 * Check whether the item can fit on a btree page at all. (Eventually, we
	 * ought to try to apply TOAST methods if not.) We actually need to be
	 * able to fit three items on every page, so restrict any one item to 1/3
	 * the per-page available space. Note that at this point, itemsz doesn't
	 * include the ItemId.
	 */
	if (itemsz > BTMaxItemSize(page))
		ereport(ERROR,
				(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
				 errmsg("index row size %lu exceeds btree maximum, %lu",
						(unsigned long) itemsz,
						(unsigned long) BTMaxItemSize(page)),
		errhint("Values larger than 1/3 of a buffer page cannot be indexed.\n"
				"Consider a function index of an MD5 hash of the value, "
				"or use full text indexing.")));

	/*
	 * Determine exactly where new item will go.
	 */
	if (afteritem > 0)
		newitemoff = afteritem + 1;
	else
	{
		/*----------
		 * If we will need to split the page to put the item here,
		 * check whether we can put the tuple somewhere to the right,
		 * instead.  Keep scanning right until we
		 *		(a) find a page with enough free space,
		 *		(b) reach the last page where the tuple can legally go, or
		 *		(c) get tired of searching.
		 * (c) is not flippant; it is important because if there are many
		 * pages' worth of equal keys, it's better to split one of the early
		 * pages than to scan all the way to the end of the run of equal keys
		 * on every insert.  We implement "get tired" as a random choice,
		 * since stopping after scanning a fixed number of pages wouldn't work
		 * well (we'd never reach the right-hand side of previously split
		 * pages).	Currently the probability of moving right is set at 0.99,
		 * which may seem too high to change the behavior much, but it does an
		 * excellent job of preventing O(N^2) behavior with many equal keys.
		 *----------
		 */
		bool		movedright = false;

		while (PageGetFreeSpace(page) < itemsz)
		{
			Buffer		rbuf;

			/*
			 * before considering moving right, see if we can obtain enough
			 * space by erasing LP_DELETE items
			 */
			if (P_ISLEAF(lpageop) && P_HAS_GARBAGE(lpageop))
			{
				_bt_vacuum_one_page(rel, buf);
				if (PageGetFreeSpace(page) >= itemsz)
					break;		/* OK, now we have enough space */
			}

			/*
			 * nope, so check conditions (b) and (c) enumerated above
			 */
			if (P_RIGHTMOST(lpageop) ||
				_bt_compare(rel, keysz, scankey, page, P_HIKEY) != 0 ||
				random() <= (MAX_RANDOM_VALUE / 100))
				break;

			/*
			 * step right to next non-dead page
			 *
			 * must write-lock that page before releasing write lock on
			 * current page; else someone else's _bt_check_unique scan could
			 * fail to see our insertion.  write locks on intermediate dead
			 * pages won't do because we don't know when they will get
			 * de-linked from the tree.
			 */
			rbuf = InvalidBuffer;

			for (;;)
			{
				BlockNumber rblkno = lpageop->btpo_next;

				rbuf = _bt_relandgetbuf(rel, rbuf, rblkno, BT_WRITE);
				page = BufferGetPage(rbuf);
				lpageop = (BTPageOpaque) PageGetSpecialPointer(page);
				if (!P_IGNORE(lpageop))
					break;
				if (P_RIGHTMOST(lpageop))
					elog(ERROR, "fell off the end of index \"%s\"",
						 RelationGetRelationName(rel));
			}
			_bt_relbuf(rel, buf);
			buf = rbuf;
			movedright = true;
		}

		/*
		 * Now we are on the right page, so find the insert position. If we
		 * moved right at all, we know we should insert at the start of the
		 * page, else must find the position by searching.
		 */
		if (movedright)
			newitemoff = P_FIRSTDATAKEY(lpageop);
		else
			newitemoff = _bt_binsrch(rel, buf, keysz, scankey, false);
	}

	/*
	 * Do we need to split the page to fit the item on it?
	 *
	 * Note: PageGetFreeSpace() subtracts sizeof(ItemIdData) from its result,
	 * so this comparison is correct even though we appear to be accounting
	 * only for the item and not for its line pointer.
	 */
	if (PageGetFreeSpace(page) < itemsz)
	{
		bool		is_root = P_ISROOT(lpageop);
		bool		is_only = P_LEFTMOST(lpageop) && P_RIGHTMOST(lpageop);
		bool		newitemonleft;
		Buffer		rbuf;

		/* Choose the split point */
		firstright = _bt_findsplitloc(rel, page,
									  newitemoff, itemsz,
									  &newitemonleft);

		/* split the buffer into left and right halves */
		rbuf = _bt_split(rel, buf, firstright,
						 newitemoff, itemsz, itup, newitemonleft);

		/*----------
		 * By here,
		 *
		 *		+  our target page has been split;
		 *		+  the original tuple has been inserted;
		 *		+  we have write locks on both the old (left half)
		 *		   and new (right half) buffers, after the split; and
		 *		+  we know the key we want to insert into the parent
		 *		   (it's the "high key" on the left child page).
		 *
		 * We're ready to do the parent insertion.  We need to hold onto the
		 * locks for the child pages until we locate the parent, but we can
		 * release them before doing the actual insertion (see Lehman and Yao
		 * for the reasoning).
		 *----------
		 */
		_bt_insert_parent(rel, buf, rbuf, stack, is_root, is_only);
	}
	else
	{
		Buffer		metabuf = InvalidBuffer;
		Page		metapg = NULL;
		BTMetaPageData *metad = NULL;
		OffsetNumber itup_off;
		BlockNumber itup_blkno;

		itup_off = newitemoff;
		itup_blkno = BufferGetBlockNumber(buf);

		/*
		 * If we are doing this insert because we split a page that was the
		 * only one on its tree level, but was not the root, it may have been
		 * the "fast root".  We need to ensure that the fast root link points
		 * at or above the current page.  We can safely acquire a lock on the
		 * metapage here --- see comments for _bt_newroot().
		 */
		if (split_only_page)
		{
			Assert(!P_ISLEAF(lpageop));

			metabuf = _bt_getbuf(rel, BTREE_METAPAGE, BT_WRITE);
			metapg = BufferGetPage(metabuf);
			metad = BTPageGetMeta(metapg);

			if (metad->btm_fastlevel >= lpageop->btpo.level)
			{
				/* no update wanted */
				_bt_relbuf(rel, metabuf);
				metabuf = InvalidBuffer;
			}
		}

		/* Do the update.  No ereport(ERROR) until changes are logged */
		START_CRIT_SECTION();

		_bt_pgaddtup(rel, page, itemsz, itup, newitemoff, "page");

		MarkBufferDirty(buf);

		if (BufferIsValid(metabuf))
		{
			metad->btm_fastroot = itup_blkno;
			metad->btm_fastlevel = lpageop->btpo.level;
			MarkBufferDirty(metabuf);
		}

		/* XLOG stuff */
		if (!rel->rd_istemp)
		{
			xl_btree_insert xlrec;
			BlockNumber xldownlink;
			xl_btree_metadata xlmeta;
			uint8		xlinfo;
			XLogRecPtr	recptr;
			XLogRecData rdata[4];
			XLogRecData *nextrdata;
			IndexTupleData trunctuple;

			xl_btreetid_set(&(xlrec.target), rel, itup_blkno, itup_off);

			rdata[0].data = (char *) &xlrec;
			rdata[0].len = SizeOfBtreeInsert;
			rdata[0].buffer = InvalidBuffer;
			rdata[0].next = nextrdata = &(rdata[1]);

			if (P_ISLEAF(lpageop))
				xlinfo = XLOG_BTREE_INSERT_LEAF;
			else
			{
				xldownlink = ItemPointerGetBlockNumber(&(itup->t_tid));
				Assert(ItemPointerGetOffsetNumber(&(itup->t_tid)) == P_HIKEY);

				nextrdata->data = (char *) &xldownlink;
				nextrdata->len = sizeof(BlockNumber);
				nextrdata->buffer = InvalidBuffer;
				nextrdata->next = nextrdata + 1;
				nextrdata++;

				xlinfo = XLOG_BTREE_INSERT_UPPER;
			}

			if (BufferIsValid(metabuf))
			{
				xlmeta.root = metad->btm_root;
				xlmeta.level = metad->btm_level;
				xlmeta.fastroot = metad->btm_fastroot;
				xlmeta.fastlevel = metad->btm_fastlevel;

				nextrdata->data = (char *) &xlmeta;
				nextrdata->len = sizeof(xl_btree_metadata);
				nextrdata->buffer = InvalidBuffer;
				nextrdata->next = nextrdata + 1;
				nextrdata++;

				xlinfo = XLOG_BTREE_INSERT_META;
			}

			/* Read comments in _bt_pgaddtup */
			if (!P_ISLEAF(lpageop) && newitemoff == P_FIRSTDATAKEY(lpageop))
			{
				trunctuple = *itup;
				trunctuple.t_info = sizeof(IndexTupleData);
				nextrdata->data = (char *) &trunctuple;
				nextrdata->len = sizeof(IndexTupleData);
			}
			else
			{
				nextrdata->data = (char *) itup;
				nextrdata->len = IndexTupleDSize(*itup);
			}
			nextrdata->buffer = buf;
			nextrdata->buffer_std = true;
			nextrdata->next = NULL;

			recptr = XLogInsert(RM_BTREE_ID, xlinfo, rdata);

			if (BufferIsValid(metabuf))
			{
				PageSetLSN(metapg, recptr);
				PageSetTLI(metapg, ThisTimeLineID);
			}

			PageSetLSN(page, recptr);
			PageSetTLI(page, ThisTimeLineID);
		}

		END_CRIT_SECTION();

		/* release buffers; send out relcache inval if metapage changed */
		if (BufferIsValid(metabuf))
		{
			if (!InRecovery)
				CacheInvalidateRelcache(rel);
			_bt_relbuf(rel, metabuf);
		}

		_bt_relbuf(rel, buf);
	}
}

/*
 *	_bt_split() -- split a page in the btree.
 *
 *		On entry, buf is the page to split, and is pinned and write-locked.
 *		firstright is the item index of the first item to be moved to the
 *		new right page.  newitemoff etc. tell us about the new item that
 *		must be inserted along with the data from the old page.
 *
 *		Returns the new right sibling of buf, pinned and write-locked.
 *		The pin and lock on buf are maintained.
 */
static Buffer
_bt_split(Relation rel, Buffer buf, OffsetNumber firstright,
		  OffsetNumber newitemoff, Size newitemsz, IndexTuple newitem,
		  bool newitemonleft)
{
	Buffer		rbuf;
	Page		origpage;
	Page		leftpage,
				rightpage;
	BTPageOpaque ropaque,
				lopaque,
				oopaque;
	Buffer		sbuf = InvalidBuffer;
	Page		spage = NULL;
	BTPageOpaque sopaque = NULL;
	OffsetNumber itup_off = 0;
	BlockNumber itup_blkno = 0;
	Size		itemsz;
	ItemId		itemid;
	IndexTuple	item;
	OffsetNumber leftoff,
				rightoff;
	OffsetNumber maxoff;
	OffsetNumber i;

	MIRROREDLOCK_BUFMGR_MUST_ALREADY_BE_HELD;

	// Fetch gp_persistent_relation_node information that will be added to XLOG record.
	RelationFetchGpRelationNodeForXLog(rel);

	rbuf = _bt_getbuf(rel, P_NEW, BT_WRITE);
	origpage = BufferGetPage(buf);
	leftpage = PageGetTempPage(origpage, sizeof(BTPageOpaqueData));
	rightpage = BufferGetPage(rbuf);

	_bt_pageinit(leftpage, BufferGetPageSize(buf));
	/* rightpage was already initialized by _bt_getbuf */

	/* init btree private data */
	oopaque = (BTPageOpaque) PageGetSpecialPointer(origpage);
	lopaque = (BTPageOpaque) PageGetSpecialPointer(leftpage);
	ropaque = (BTPageOpaque) PageGetSpecialPointer(rightpage);

	/* if we're splitting this page, it won't be the root when we're done */
	/* also, clear the SPLIT_END and HAS_GARBAGE flags in both pages */
	lopaque->btpo_flags = oopaque->btpo_flags;
	lopaque->btpo_flags &= ~(BTP_ROOT | BTP_SPLIT_END | BTP_HAS_GARBAGE);
	ropaque->btpo_flags = lopaque->btpo_flags;
	lopaque->btpo_prev = oopaque->btpo_prev;
	lopaque->btpo_next = BufferGetBlockNumber(rbuf);
	ropaque->btpo_prev = BufferGetBlockNumber(buf);
	ropaque->btpo_next = oopaque->btpo_next;
	lopaque->btpo.level = ropaque->btpo.level = oopaque->btpo.level;
	/* Since we already have write-lock on both pages, ok to read cycleid */
	lopaque->btpo_cycleid = _bt_vacuum_cycleid(rel);
	ropaque->btpo_cycleid = lopaque->btpo_cycleid;

	/*
	 * If the page we're splitting is not the rightmost page at its level in
	 * the tree, then the first entry on the page is the high key for the
	 * page.  We need to copy that to the right half.  Otherwise (meaning the
	 * rightmost page case), all the items on the right half will be user
	 * data.
	 */
	rightoff = P_HIKEY;

	if (!P_RIGHTMOST(oopaque))
	{
		itemid = PageGetItemId(origpage, P_HIKEY);
		itemsz = ItemIdGetLength(itemid);
		item = (IndexTuple) PageGetItem(origpage, itemid);
		if (PageAddItem(rightpage, (Item) item, itemsz, rightoff,
						LP_USED) == InvalidOffsetNumber)
			elog(PANIC, "failed to add hikey to the right sibling"
				 " while splitting block %u of index \"%s\"",
				 BufferGetBlockNumber(buf), RelationGetRelationName(rel));
		rightoff = OffsetNumberNext(rightoff);
	}

	/*
	 * The "high key" for the new left page will be the first key that's going
	 * to go into the new right page.  This might be either the existing data
	 * item at position firstright, or the incoming tuple.
	 */
	leftoff = P_HIKEY;
	if (!newitemonleft && newitemoff == firstright)
	{
		/* incoming tuple will become first on right page */
		itemsz = newitemsz;
		item = newitem;
	}
	else
	{
		/* existing item at firstright will become first on right page */
		itemid = PageGetItemId(origpage, firstright);
		itemsz = ItemIdGetLength(itemid);
		item = (IndexTuple) PageGetItem(origpage, itemid);
	}
	if (PageAddItem(leftpage, (Item) item, itemsz, leftoff,
					LP_USED) == InvalidOffsetNumber)
		elog(PANIC, "failed to add hikey to the left sibling"
			 " while splitting block %u of index \"%s\"",
			 BufferGetBlockNumber(buf), RelationGetRelationName(rel));
	leftoff = OffsetNumberNext(leftoff);

	/*
	 * Now transfer all the data items to the appropriate page
	 */
	maxoff = PageGetMaxOffsetNumber(origpage);

	for (i = P_FIRSTDATAKEY(oopaque); i <= maxoff; i = OffsetNumberNext(i))
	{
		itemid = PageGetItemId(origpage, i);
		itemsz = ItemIdGetLength(itemid);
		item = (IndexTuple) PageGetItem(origpage, itemid);

		/* does new item belong before this one? */
		if (i == newitemoff)
		{
			if (newitemonleft)
			{
				_bt_pgaddtup(rel, leftpage, newitemsz, newitem, leftoff,
							 "left sibling");
				itup_off = leftoff;
				itup_blkno = BufferGetBlockNumber(buf);
				leftoff = OffsetNumberNext(leftoff);
			}
			else
			{
				_bt_pgaddtup(rel, rightpage, newitemsz, newitem, rightoff,
							 "right sibling");
				itup_off = rightoff;
				itup_blkno = BufferGetBlockNumber(rbuf);
				rightoff = OffsetNumberNext(rightoff);
			}
		}

		/* decide which page to put it on */
		if (i < firstright)
		{
			_bt_pgaddtup(rel, leftpage, itemsz, item, leftoff,
						 "left sibling");
			leftoff = OffsetNumberNext(leftoff);
		}
		else
		{
			_bt_pgaddtup(rel, rightpage, itemsz, item, rightoff,
						 "right sibling");
			rightoff = OffsetNumberNext(rightoff);
		}
	}

	/* cope with possibility that newitem goes at the end */
	if (i <= newitemoff)
	{
		if (newitemonleft)
		{
			_bt_pgaddtup(rel, leftpage, newitemsz, newitem, leftoff,
						 "left sibling");
			itup_off = leftoff;
			itup_blkno = BufferGetBlockNumber(buf);
			leftoff = OffsetNumberNext(leftoff);
		}
		else
		{
			_bt_pgaddtup(rel, rightpage, newitemsz, newitem, rightoff,
						 "right sibling");
			itup_off = rightoff;
			itup_blkno = BufferGetBlockNumber(rbuf);
			rightoff = OffsetNumberNext(rightoff);
		}
	}

	/*
	 * We have to grab the right sibling (if any) and fix the prev pointer
	 * there. We are guaranteed that this is deadlock-free since no other
	 * writer will be holding a lock on that page and trying to move left, and
	 * all readers release locks on a page before trying to fetch its
	 * neighbors.
	 */

	if (!P_RIGHTMOST(ropaque))
	{
		sbuf = _bt_getbuf(rel, ropaque->btpo_next, BT_WRITE);
		spage = BufferGetPage(sbuf);
		sopaque = (BTPageOpaque) PageGetSpecialPointer(spage);
		if (sopaque->btpo_prev != ropaque->btpo_prev)
			elog(PANIC, "right sibling's left-link doesn't match: "
				 "block %u links to %u instead of expected %u in index \"%s\"",
				 ropaque->btpo_next, sopaque->btpo_prev, ropaque->btpo_prev,
				 RelationGetRelationName(rel));

		/*
		 * Check to see if we can set the SPLIT_END flag in the right-hand
		 * split page; this can save some I/O for vacuum since it need not
		 * proceed to the right sibling.  We can set the flag if the right
		 * sibling has a different cycleid: that means it could not be part of
		 * a group of pages that were all split off from the same ancestor
		 * page.  If you're confused, imagine that page A splits to A B and
		 * then again, yielding A C B, while vacuum is in progress.  Tuples
		 * originally in A could now be in either B or C, hence vacuum must
		 * examine both pages.	But if D, our right sibling, has a different
		 * cycleid then it could not contain any tuples that were in A when
		 * the vacuum started.
		 */
		if (sopaque->btpo_cycleid != ropaque->btpo_cycleid)
			ropaque->btpo_flags |= BTP_SPLIT_END;
	}

	/*
	 * Right sibling is locked, new siblings are prepared, but original page
	 * is not updated yet. Log changes before continuing.
	 *
	 * NO EREPORT(ERROR) till right sibling is updated.  We can get away with
	 * not starting the critical section till here because we haven't been
	 * scribbling on the original page yet, and we don't care about the new
	 * sibling until it's linked into the btree.
	 */
	START_CRIT_SECTION();

	MarkBufferDirty(buf);
	MarkBufferDirty(rbuf);

	if (!P_RIGHTMOST(ropaque))
	{
		sopaque->btpo_prev = BufferGetBlockNumber(rbuf);
		MarkBufferDirty(sbuf);
	}

	/* XLOG stuff */
	if (!rel->rd_istemp)
	{
		xl_btree_split xlrec;
		uint8		xlinfo;
		XLogRecPtr	recptr;
		XLogRecData rdata[4];

		xl_btreetid_set(&(xlrec.target), rel, itup_blkno, itup_off);
		if (newitemonleft)
			xlrec.otherblk = BufferGetBlockNumber(rbuf);
		else
			xlrec.otherblk = BufferGetBlockNumber(buf);
		xlrec.leftblk = lopaque->btpo_prev;
		xlrec.rightblk = ropaque->btpo_next;
		xlrec.level = lopaque->btpo.level;

		/*
		 * Direct access to page is not good but faster - we should implement
		 * some new func in page API.  Note we only store the tuples
		 * themselves, knowing that the item pointers are in the same order
		 * and can be reconstructed by scanning the tuples.  See comments for
		 * _bt_restore_page().
		 */
		xlrec.leftlen = ((PageHeader) leftpage)->pd_special -
			((PageHeader) leftpage)->pd_upper;

		rdata[0].data = (char *) &xlrec;
		rdata[0].len = SizeOfBtreeSplit;
		rdata[0].buffer = InvalidBuffer;
		rdata[0].next = &(rdata[1]);

		rdata[1].data = (char *) leftpage + ((PageHeader) leftpage)->pd_upper;
		rdata[1].len = xlrec.leftlen;
		rdata[1].buffer = InvalidBuffer;
		rdata[1].next = &(rdata[2]);

		rdata[2].data = (char *) rightpage + ((PageHeader) rightpage)->pd_upper;
		rdata[2].len = ((PageHeader) rightpage)->pd_special -
			((PageHeader) rightpage)->pd_upper;
		rdata[2].buffer = InvalidBuffer;
		rdata[2].next = NULL;

		if (!P_RIGHTMOST(ropaque))
		{
			rdata[2].next = &(rdata[3]);
			rdata[3].data = NULL;
			rdata[3].len = 0;
			rdata[3].buffer = sbuf;
			rdata[3].buffer_std = true;
			rdata[3].next = NULL;
		}

		if (P_ISROOT(oopaque))
			xlinfo = newitemonleft ? XLOG_BTREE_SPLIT_L_ROOT : XLOG_BTREE_SPLIT_R_ROOT;
		else
			xlinfo = newitemonleft ? XLOG_BTREE_SPLIT_L : XLOG_BTREE_SPLIT_R;

		recptr = XLogInsert(RM_BTREE_ID, xlinfo, rdata);

		PageSetLSN(leftpage, recptr);
		PageSetTLI(leftpage, ThisTimeLineID);
		PageSetLSN(rightpage, recptr);
		PageSetTLI(rightpage, ThisTimeLineID);
		if (!P_RIGHTMOST(ropaque))
		{
			PageSetLSN(spage, recptr);
			PageSetTLI(spage, ThisTimeLineID);
		}
	}

	/*
	 * By here, the original data page has been split into two new halves, and
	 * these are correct.  The algorithm requires that the left page never
	 * move during a split, so we copy the new left page back on top of the
	 * original.  Note that this is not a waste of time, since we also require
	 * (in the page management code) that the center of a page always be
	 * clean, and the most efficient way to guarantee this is just to compact
	 * the data by reinserting it into a new left page.  (XXX the latter
	 * comment is probably obsolete.)
	 *
	 * It's a bit weird that we don't fill in the left page till after writing
	 * the XLOG entry, but not really worth changing.  Note that we use the
	 * origpage data (specifically its BTP_ROOT bit) while preparing the XLOG
	 * entry, so simply reshuffling the code won't do.
	 */

	PageRestoreTempPage(leftpage, origpage);

	END_CRIT_SECTION();

	/* release the old right sibling */
	if (!P_RIGHTMOST(ropaque))
		_bt_relbuf(rel, sbuf);

	/* split's done */
	return rbuf;
}

/*
 *	_bt_findsplitloc() -- find an appropriate place to split a page.
 *
 * The idea here is to equalize the free space that will be on each split
 * page, *after accounting for the inserted tuple*.  (If we fail to account
 * for it, we might find ourselves with too little room on the page that
 * it needs to go into!)
 *
 * If the page is the rightmost page on its level, we instead try to arrange
 * to leave the left split page fillfactor% full.  In this way, when we are
 * inserting successively increasing keys (consider sequences, timestamps,
 * etc) we will end up with a tree whose pages are about fillfactor% full,
 * instead of the 50% full result that we'd get without this special case.
 * This is the same as nbtsort.c produces for a newly-created tree.  Note
 * that leaf and nonleaf pages use different fillfactors.
 *
 * We are passed the intended insert position of the new tuple, expressed as
 * the offsetnumber of the tuple it must go in front of.  (This could be
 * maxoff+1 if the tuple is to go at the end.)
 *
 * We return the index of the first existing tuple that should go on the
 * righthand page, plus a boolean indicating whether the new tuple goes on
 * the left or right page.	The bool is necessary to disambiguate the case
 * where firstright == newitemoff.
 */
static OffsetNumber
_bt_findsplitloc(Relation rel,
				 Page page,
				 OffsetNumber newitemoff,
				 Size newitemsz,
				 bool *newitemonleft)
{
	BTPageOpaque opaque;
	OffsetNumber offnum;
	OffsetNumber maxoff;
	ItemId		itemid;
	FindSplitData state;
	int			leftspace,
				rightspace,
				goodenough,
				dataitemtotal,
				dataitemstoleft;

	opaque = (BTPageOpaque) PageGetSpecialPointer(page);

	/* Passed-in newitemsz is MAXALIGNED but does not include line pointer */
	newitemsz += sizeof(ItemIdData);
	state.newitemsz = newitemsz;
	state.is_leaf = P_ISLEAF(opaque);
	state.is_rightmost = P_RIGHTMOST(opaque);
	state.have_split = false;
	if (state.is_leaf)
		state.fillfactor = RelationGetFillFactor(rel,
												 BTREE_DEFAULT_FILLFACTOR);
	else
		state.fillfactor = BTREE_NONLEAF_FILLFACTOR;
	state.newitemonleft = false;	/* these just to keep compiler quiet */
	state.firstright = 0;
	state.best_delta = 0;

	/* Total free space available on a btree page, after fixed overhead */
	leftspace = rightspace =
		PageGetPageSize(page) - SizeOfPageHeaderData -
		MAXALIGN(sizeof(BTPageOpaqueData));

	/*
	 * Finding the best possible split would require checking all the possible
	 * split points, because of the high-key and left-key special cases.
	 * That's probably more work than it's worth; instead, stop as soon as we
	 * find a "good-enough" split, where good-enough is defined as an
	 * imbalance in free space of no more than pagesize/16 (arbitrary...) This
	 * should let us stop near the middle on most pages, instead of plowing to
	 * the end.
	 */
	goodenough = leftspace / 16;

	/* The right page will have the same high key as the old page */
	if (!P_RIGHTMOST(opaque))
	{
		itemid = PageGetItemId(page, P_HIKEY);
		rightspace -= (int) (MAXALIGN(ItemIdGetLength(itemid)) +
							 sizeof(ItemIdData));
	}

	/* Count up total space in data items without actually scanning 'em */
	dataitemtotal = rightspace - (int) PageGetFreeSpace(page);

	/*
	 * Scan through the data items and calculate space usage for a split at
	 * each possible position.
	 */
	dataitemstoleft = 0;
	maxoff = PageGetMaxOffsetNumber(page);

	for (offnum = P_FIRSTDATAKEY(opaque);
		 offnum <= maxoff;
		 offnum = OffsetNumberNext(offnum))
	{
		Size		itemsz;
		int			leftfree,
					rightfree;

		itemid = PageGetItemId(page, offnum);
		itemsz = MAXALIGN(ItemIdGetLength(itemid)) + sizeof(ItemIdData);

		/*
		 * We have to allow for the current item becoming the high key of the
		 * left page; therefore it counts against left space as well as right
		 * space.
		 */
		leftfree = leftspace - dataitemstoleft - (int) itemsz;
		rightfree = rightspace - (dataitemtotal - dataitemstoleft);

		/*
		 * Will the new item go to left or right of split?
		 */
		if (offnum > newitemoff)
			_bt_checksplitloc(&state, offnum, leftfree, rightfree,
							  true, itemsz);
		else if (offnum < newitemoff)
			_bt_checksplitloc(&state, offnum, leftfree, rightfree,
							  false, itemsz);
		else
		{
			/* need to try it both ways! */
			_bt_checksplitloc(&state, offnum, leftfree, rightfree,
							  true, itemsz);
			/*
			 * Here we are contemplating newitem as first on right.  In this
			 * case it, not the current item, will become the high key of the
			 * left page, and so we have to correct the allowance made above.
			 */
			leftfree += (int) itemsz - (int) newitemsz;
			_bt_checksplitloc(&state, offnum, leftfree, rightfree,
							  false, newitemsz);
		}

		/* Abort scan once we find a good-enough choice */
		if (state.have_split && state.best_delta <= goodenough)
			break;

		dataitemstoleft += itemsz;
	}

	/*
	 * I believe it is not possible to fail to find a feasible split, but just
	 * in case ...
	 */
	if (!state.have_split)
		elog(ERROR, "could not find a feasible split point for index \"%s\"",
			 RelationGetRelationName(rel));

	*newitemonleft = state.newitemonleft;
	return state.firstright;
}

/*
 * Subroutine to analyze a particular possible split choice (ie, firstright
 * and newitemonleft settings), and record the best split so far in *state.
 */
static void
_bt_checksplitloc(FindSplitData *state, OffsetNumber firstright,
				  int leftfree, int rightfree,
				  bool newitemonleft, Size firstrightitemsz)
{
	/*
	 * Account for the new item on whichever side it is to be put.
	 */
	if (newitemonleft)
		leftfree -= (int) state->newitemsz;
	else
		rightfree -= (int) state->newitemsz;

	/*
	 * If we are not on the leaf level, we will be able to discard the key
	 * data from the first item that winds up on the right page.
	 */
	if (!state->is_leaf)
		rightfree += (int) firstrightitemsz -
			(int) (MAXALIGN(sizeof(IndexTupleData)) + sizeof(ItemIdData));

	/*
	 * If feasible split point, remember best delta.
	 */
	if (leftfree >= 0 && rightfree >= 0)
	{
		int			delta;

		if (state->is_rightmost)
		{
			/*
			 * If splitting a rightmost page, try to put (100-fillfactor)% of
			 * free space on left page. See comments for _bt_findsplitloc.
			 */
			delta = (state->fillfactor * leftfree)
				- ((100 - state->fillfactor) * rightfree);
		}
		else
		{
			/* Otherwise, aim for equal free space on both sides */
			delta = leftfree - rightfree;
		}

		if (delta < 0)
			delta = -delta;
		if (!state->have_split || delta < state->best_delta)
		{
			state->have_split = true;
			state->newitemonleft = newitemonleft;
			state->firstright = firstright;
			state->best_delta = delta;
		}
	}
}

/*
 * _bt_insert_parent() -- Insert downlink into parent after a page split.
 *
 * On entry, buf and rbuf are the left and right split pages, which we
 * still hold write locks on per the L&Y algorithm.  We release the
 * write locks once we have write lock on the parent page.	(Any sooner,
 * and it'd be possible for some other process to try to split or delete
 * one of these pages, and get confused because it cannot find the downlink.)
 *
 * stack - stack showing how we got here.  May be NULL in cases that don't
 *			have to be efficient (concurrent ROOT split, WAL recovery)
 * is_root - we split the true root
 * is_only - we split a page alone on its level (might have been fast root)
 *
 * This is exported so it can be called by nbtxlog.c.
 */
void
_bt_insert_parent(Relation rel,
				  Buffer buf,
				  Buffer rbuf,
				  BTStack stack,
				  bool is_root,
				  bool is_only)
{
	MIRROREDLOCK_BUFMGR_MUST_ALREADY_BE_HELD;

	/*
	 * Here we have to do something Lehman and Yao don't talk about: deal with
	 * a root split and construction of a new root.  If our stack is empty
	 * then we have just split a node on what had been the root level when we
	 * descended the tree.	If it was still the root then we perform a
	 * new-root construction.  If it *wasn't* the root anymore, search to find
	 * the next higher level that someone constructed meanwhile, and find the
	 * right place to insert as for the normal case.
	 *
	 * If we have to search for the parent level, we do so by re-descending
	 * from the root.  This is not super-efficient, but it's rare enough not
	 * to matter.  (This path is also taken when called from WAL recovery ---
	 * we have no stack in that case.)
	 */
	if (is_root)
	{
		Buffer		rootbuf;

		Assert(stack == NULL);
		Assert(is_only);
		/* create a new root node and update the metapage */
		rootbuf = _bt_newroot(rel, buf, rbuf);
		/* release the split buffers */
		_bt_relbuf(rel, rootbuf);
		_bt_relbuf(rel, rbuf);
		_bt_relbuf(rel, buf);
	}
	else
	{
		BlockNumber bknum = BufferGetBlockNumber(buf);
		BlockNumber rbknum = BufferGetBlockNumber(rbuf);
		Page		page = BufferGetPage(buf);
		IndexTuple	new_item;
		BTStackData fakestack;
		IndexTuple	ritem;
		Buffer		pbuf;

		if (stack == NULL)
		{
			BTPageOpaque lpageop;

			if (!InRecovery)
				elog(DEBUG2, "concurrent ROOT page split");
			lpageop = (BTPageOpaque) PageGetSpecialPointer(page);
			/* Find the leftmost page at the next level up */
			pbuf = _bt_get_endpoint(rel, lpageop->btpo.level + 1, false);
			/* Set up a phony stack entry pointing there */
			stack = &fakestack;
			stack->bts_blkno = BufferGetBlockNumber(pbuf);
			stack->bts_offset = InvalidOffsetNumber;
			/* bts_btentry will be initialized below */
			stack->bts_parent = NULL;
			_bt_relbuf(rel, pbuf);
		}

		/* get high key from left page == lowest key on new right page */
		ritem = (IndexTuple) PageGetItem(page,
										 PageGetItemId(page, P_HIKEY));

		/* form an index tuple that points at the new right page */
		new_item = CopyIndexTuple(ritem);
		ItemPointerSet(&(new_item->t_tid), rbknum, P_HIKEY);

		/*
		 * Find the parent buffer and get the parent page.
		 *
		 * Oops - if we were moved right then we need to change stack item! We
		 * want to find parent pointing to where we are, right ?	- vadim
		 * 05/27/97
		 */
		ItemPointerSet(&(stack->bts_btentry.t_tid), bknum, P_HIKEY);

		pbuf = _bt_getstackbuf(rel, stack, BT_WRITE);

		/* Now we can unlock the children */
		_bt_relbuf(rel, rbuf);
		_bt_relbuf(rel, buf);

		/* Check for error only after writing children */
		if (pbuf == InvalidBuffer)
			elog(ERROR, "failed to re-find parent key in index \"%s\" for split pages %u/%u",
				 RelationGetRelationName(rel), bknum, rbknum);

		/* Recursively update the parent */
		_bt_insertonpg(rel, pbuf, stack->bts_parent,
					   0, NULL, new_item, stack->bts_offset,
					   is_only);

		/* be tidy */
		pfree(new_item);
	}
}

/*
 *	_bt_getstackbuf() -- Walk back up the tree one step, and find the item
 *						 we last looked at in the parent.
 *
 *		This is possible because we save the downlink from the parent item,
 *		which is enough to uniquely identify it.  Insertions into the parent
 *		level could cause the item to move right; deletions could cause it
 *		to move left, but not left of the page we previously found it in.
 *
 *		Adjusts bts_blkno & bts_offset if changed.
 *
 *		Returns InvalidBuffer if item not found (should not happen).
 */
Buffer
_bt_getstackbuf(Relation rel, BTStack stack, int access)
{
	BlockNumber blkno;
	OffsetNumber start;

	MIRROREDLOCK_BUFMGR_MUST_ALREADY_BE_HELD;

	blkno = stack->bts_blkno;
	start = stack->bts_offset;

	for (;;)
	{
		Buffer		buf;
		Page		page;
		BTPageOpaque opaque;

		buf = _bt_getbuf(rel, blkno, access);
		page = BufferGetPage(buf);
		opaque = (BTPageOpaque) PageGetSpecialPointer(page);

		if (!P_IGNORE(opaque))
		{
			OffsetNumber offnum,
						minoff,
						maxoff;
			ItemId		itemid;
			IndexTuple	item;

			minoff = P_FIRSTDATAKEY(opaque);
			maxoff = PageGetMaxOffsetNumber(page);

			/*
			 * start = InvalidOffsetNumber means "search the whole page". We
			 * need this test anyway due to possibility that page has a high
			 * key now when it didn't before.
			 */
			if (start < minoff)
				start = minoff;

			/*
			 * Need this check too, to guard against possibility that page
			 * split since we visited it originally.
			 */
			if (start > maxoff)
				start = OffsetNumberNext(maxoff);

			/*
			 * These loops will check every item on the page --- but in an
			 * order that's attuned to the probability of where it actually
			 * is.	Scan to the right first, then to the left.
			 */
			for (offnum = start;
				 offnum <= maxoff;
				 offnum = OffsetNumberNext(offnum))
			{
				itemid = PageGetItemId(page, offnum);
				item = (IndexTuple) PageGetItem(page, itemid);
				if (BTEntrySame(item, &stack->bts_btentry))
				{
					/* Return accurate pointer to where link is now */
					stack->bts_blkno = blkno;
					stack->bts_offset = offnum;
					return buf;
				}
			}

			for (offnum = OffsetNumberPrev(start);
				 offnum >= minoff;
				 offnum = OffsetNumberPrev(offnum))
			{
				itemid = PageGetItemId(page, offnum);
				item = (IndexTuple) PageGetItem(page, itemid);
				if (BTEntrySame(item, &stack->bts_btentry))
				{
					/* Return accurate pointer to where link is now */
					stack->bts_blkno = blkno;
					stack->bts_offset = offnum;
					return buf;
				}
			}
		}

		/*
		 * The item we're looking for moved right at least one page.
		 */
		if (P_RIGHTMOST(opaque))
		{
			_bt_relbuf(rel, buf);
			return InvalidBuffer;
		}
		blkno = opaque->btpo_next;
		start = InvalidOffsetNumber;
		_bt_relbuf(rel, buf);
	}
}

/*
 *	_bt_newroot() -- Create a new root page for the index.
 *
 *		We've just split the old root page and need to create a new one.
 *		In order to do this, we add a new root page to the file, then lock
 *		the metadata page and update it.  This is guaranteed to be deadlock-
 *		free, because all readers release their locks on the metadata page
 *		before trying to lock the root, and all writers lock the root before
 *		trying to lock the metadata page.  We have a write lock on the old
 *		root page, so we have not introduced any cycles into the waits-for
 *		graph.
 *
 *		On entry, lbuf (the old root) and rbuf (its new peer) are write-
 *		locked. On exit, a new root page exists with entries for the
 *		two new children, metapage is updated and unlocked/unpinned.
 *		The new root buffer is returned to caller which has to unlock/unpin
 *		lbuf, rbuf & rootbuf.
 */
static Buffer
_bt_newroot(Relation rel, Buffer lbuf, Buffer rbuf)
{
	Buffer		rootbuf;
	Page		lpage,
				rootpage;
	BlockNumber lbkno,
				rbkno;
	BlockNumber rootblknum;
	BTPageOpaque rootopaque;
	ItemId		itemid;
	IndexTuple	item;
	Size		itemsz;
	IndexTuple	new_item;
	Buffer		metabuf;
	Page		metapg;
	BTMetaPageData *metad;

	MIRROREDLOCK_BUFMGR_MUST_ALREADY_BE_HELD;

	// Fetch gp_persistent_relation_node information that will be added to XLOG record.
	RelationFetchGpRelationNodeForXLog(rel);

	lbkno = BufferGetBlockNumber(lbuf);
	rbkno = BufferGetBlockNumber(rbuf);
	lpage = BufferGetPage(lbuf);

	/* get a new root page */
	rootbuf = _bt_getbuf(rel, P_NEW, BT_WRITE);
	rootpage = BufferGetPage(rootbuf);
	rootblknum = BufferGetBlockNumber(rootbuf);

	/* acquire lock on the metapage */
	metabuf = _bt_getbuf(rel, BTREE_METAPAGE, BT_WRITE);
	metapg = BufferGetPage(metabuf);
	metad = BTPageGetMeta(metapg);

	/* NO EREPORT(ERROR) from here till newroot op is logged */
	START_CRIT_SECTION();

	/* set btree special data */
	rootopaque = (BTPageOpaque) PageGetSpecialPointer(rootpage);
	rootopaque->btpo_prev = rootopaque->btpo_next = P_NONE;
	rootopaque->btpo_flags = BTP_ROOT;
	rootopaque->btpo.level =
		((BTPageOpaque) PageGetSpecialPointer(lpage))->btpo.level + 1;
	rootopaque->btpo_cycleid = 0;

	/* update metapage data */
	metad->btm_root = rootblknum;
	metad->btm_level = rootopaque->btpo.level;
	metad->btm_fastroot = rootblknum;
	metad->btm_fastlevel = rootopaque->btpo.level;

	/*
	 * Create downlink item for left page (old root).  Since this will be the
	 * first item in a non-leaf page, it implicitly has minus-infinity key
	 * value, so we need not store any actual key in it.
	 */
	itemsz = sizeof(IndexTupleData);
	new_item = (IndexTuple) palloc(itemsz);
	new_item->t_info = itemsz;
	ItemPointerSet(&(new_item->t_tid), lbkno, P_HIKEY);

	/*
	 * Insert the left page pointer into the new root page.  The root page is
	 * the rightmost page on its level so there is no "high key" in it; the
	 * two items will go into positions P_HIKEY and P_FIRSTKEY.
	 *
	 * Note: we *must* insert the two items in item-number order, for the
	 * benefit of _bt_restore_page().
	 */
	if (PageAddItem(rootpage, (Item) new_item, itemsz, P_HIKEY, LP_USED) == InvalidOffsetNumber)
		elog(PANIC, "failed to add leftkey to new root page"
			 " while splitting block %u of index \"%s\"",
			 BufferGetBlockNumber(lbuf), RelationGetRelationName(rel));
	pfree(new_item);

	/*
	 * Create downlink item for right page.  The key for it is obtained from
	 * the "high key" position in the left page.
	 */
	itemid = PageGetItemId(lpage, P_HIKEY);
	itemsz = ItemIdGetLength(itemid);
	item = (IndexTuple) PageGetItem(lpage, itemid);
	new_item = CopyIndexTuple(item);
	ItemPointerSet(&(new_item->t_tid), rbkno, P_HIKEY);

	/*
	 * insert the right page pointer into the new root page.
	 */
	if (PageAddItem(rootpage, (Item) new_item, itemsz, P_FIRSTKEY, LP_USED) == InvalidOffsetNumber)
		elog(PANIC, "failed to add rightkey to new root page"
			 " while splitting block %u of index \"%s\"",
			 BufferGetBlockNumber(lbuf), RelationGetRelationName(rel));
	pfree(new_item);

	MarkBufferDirty(rootbuf);
	MarkBufferDirty(metabuf);

	/* XLOG stuff */
	if (!rel->rd_istemp)
	{
		xl_btree_newroot xlrec;
		XLogRecPtr	recptr;
		XLogRecData rdata[2];

		xl_btreenode_set(&(xlrec.btreenode), rel);
		xlrec.rootblk = rootblknum;
		xlrec.level = metad->btm_level;

		rdata[0].data = (char *) &xlrec;
		rdata[0].len = SizeOfBtreeNewroot;
		rdata[0].buffer = InvalidBuffer;
		rdata[0].next = &(rdata[1]);

		/*
		 * Direct access to page is not good but faster - we should implement
		 * some new func in page API.
		 */
		rdata[1].data = (char *) rootpage + ((PageHeader) rootpage)->pd_upper;
		rdata[1].len = ((PageHeader) rootpage)->pd_special -
			((PageHeader) rootpage)->pd_upper;
		rdata[1].buffer = InvalidBuffer;
		rdata[1].next = NULL;

		recptr = XLogInsert(RM_BTREE_ID, XLOG_BTREE_NEWROOT, rdata);

		PageSetLSN(rootpage, recptr);
		PageSetTLI(rootpage, ThisTimeLineID);
		PageSetLSN(metapg, recptr);
		PageSetTLI(metapg, ThisTimeLineID);
	}

	END_CRIT_SECTION();

	/* send out relcache inval for metapage change */
	if (!InRecovery)
		CacheInvalidateRelcache(rel);

	/* done with metapage */
	_bt_relbuf(rel, metabuf);

	return rootbuf;
}

/*
 *	_bt_pgaddtup() -- add a tuple to a particular page in the index.
 *
 *		This routine adds the tuple to the page as requested.  It does
 *		not affect pin/lock status, but you'd better have a write lock
 *		and pin on the target buffer!  Don't forget to write and release
 *		the buffer afterwards, either.
 *
 *		The main difference between this routine and a bare PageAddItem call
 *		is that this code knows that the leftmost index tuple on a non-leaf
 *		btree page doesn't need to have a key.  Therefore, it strips such
 *		tuples down to just the tuple header.  CAUTION: this works ONLY if
 *		we insert the tuples in order, so that the given itup_off does
 *		represent the final position of the tuple!
 */
static void
_bt_pgaddtup(Relation rel,
			 Page page,
			 Size itemsize,
			 IndexTuple itup,
			 OffsetNumber itup_off,
			 const char *where)
{
	BTPageOpaque opaque = (BTPageOpaque) PageGetSpecialPointer(page);
	IndexTupleData trunctuple;

	if (!P_ISLEAF(opaque) && itup_off == P_FIRSTDATAKEY(opaque))
	{
		trunctuple = *itup;
		trunctuple.t_info = sizeof(IndexTupleData);
		itup = &trunctuple;
		itemsize = sizeof(IndexTupleData);
	}

	if (PageAddItem(page, (Item) itup, itemsize, itup_off,
					LP_USED) == InvalidOffsetNumber)
		elog(PANIC, "failed to add item to the %s in index \"%s\"",
			 where, RelationGetRelationName(rel));
}

/*
 * _bt_isequal - used in _bt_doinsert in check for duplicates.
 *
 * This is very similar to _bt_compare, except for NULL handling.
 * Rule is simple: NOT_NULL not equal NULL, NULL not equal NULL too.
 */
static bool
_bt_isequal(TupleDesc itupdesc, Page page, OffsetNumber offnum,
			int keysz, ScanKey scankey)
{
	IndexTuple	itup;
	int			i;

	/* Better be comparing to a leaf item */
	Assert(P_ISLEAF((BTPageOpaque) PageGetSpecialPointer(page)));

	itup = (IndexTuple) PageGetItem(page, PageGetItemId(page, offnum));

	for (i = 1; i <= keysz; i++)
	{
		AttrNumber	attno;
		Datum		datum;
		bool		isNull;
		int32		result;

		attno = scankey->sk_attno;
		Assert(attno == i);
		datum = index_getattr(itup, attno, itupdesc, &isNull);

		/* NULLs are never equal to anything */
		if (isNull || (scankey->sk_flags & SK_ISNULL))
			return false;

		result = DatumGetInt32(FunctionCall2(&scankey->sk_func,
											 datum,
											 scankey->sk_argument));

		if (result != 0)
			return false;

		scankey++;
	}

	/* if we get here, the keys are equal */
	return true;
}

/*
 * _bt_vacuum_one_page - vacuum just one index page.
 *
 * Try to remove LP_DELETE items from the given page.  The passed buffer
 * must be exclusive-locked, but unlike a real VACUUM, we don't need a
 * super-exclusive "cleanup" lock (see nbtree/README).
 */
static void
_bt_vacuum_one_page(Relation rel, Buffer buffer)
{
	OffsetNumber deletable[MaxOffsetNumber];
	int			ndeletable = 0;
	OffsetNumber offnum,
				minoff,
				maxoff;
	Page		page = BufferGetPage(buffer);
	BTPageOpaque opaque = (BTPageOpaque) PageGetSpecialPointer(page);

	/*
	 * Scan over all items to see which ones need deleted according to
	 * LP_DELETE flags.
	 */
	minoff = P_FIRSTDATAKEY(opaque);
	maxoff = PageGetMaxOffsetNumber(page);
	for (offnum = minoff;
		 offnum <= maxoff;
		 offnum = OffsetNumberNext(offnum))
	{
		ItemId		itemId = PageGetItemId(page, offnum);

		if (ItemIdDeleted(itemId))
			deletable[ndeletable++] = offnum;
	}

	if (ndeletable > 0)
		_bt_delitems(rel, buffer, deletable, ndeletable);

	/*
	 * Note: if we didn't find any LP_DELETE items, then the page's
	 * BTP_HAS_GARBAGE hint bit is falsely set.  We do not bother expending a
	 * separate write to clear it, however.  We will clear it when we split
	 * the page.
	 */
}
