/*-------------------------------------------------------------------------
 *
 * genam.c
 *	  general index access method routines
 *
 * Portions Copyright (c) 1996-2009, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  $PostgreSQL: pgsql/src/backend/access/index/genam.c,v 1.59 2006/10/04 00:29:48 momjian Exp $
 *
 * NOTES
 *	  many of the old access method routines have been turned into
 *	  macros and moved to genam.h -cim 4/30/91
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "access/genam.h"
#include "access/heapam.h"
#include "access/relscan.h"
#include "access/transam.h"
#include "miscadmin.h"
#include "pgstat.h"
#include "storage/bufmgr.h"
#include "utils/rel.h"
#include "utils/tqual.h"

#include "cdb/cdbvars.h"

#include "cdb/cdbinmemheapam.h"
#include "catalog/pg_namespace.h"


/* ----------------------------------------------------------------
 *		general access method routines
 *
 *		All indexed access methods use an identical scan structure.
 *		We don't know how the various AMs do locking, however, so we don't
 *		do anything about that here.
 *
 *		The intent is that an AM implementor will define a beginscan routine
 *		that calls RelationGetIndexScan, to fill in the scan, and then does
 *		whatever kind of locking he wants.
 *
 *		At the end of a scan, the AM's endscan routine undoes the locking,
 *		but does *not* call IndexScanEnd --- the higher-level index_endscan
 *		routine does that.	(We can't do it in the AM because index_endscan
 *		still needs to touch the IndexScanDesc after calling the AM.)
 *
 *		Because of this, the AM does not have a choice whether to call
 *		RelationGetIndexScan or not; its beginscan routine must return an
 *		object made by RelationGetIndexScan.  This is kinda ugly but not
 *		worth cleaning up now.
 * ----------------------------------------------------------------
 */

static HeapTuple
systable_getnext_or_prev(SysScanDesc sysscan, ScanDirection direction);


/* ----------------
 *	RelationGetIndexScan -- Create and fill an IndexScanDesc.
 *
 *		This routine creates an index scan structure and sets its contents
 *		up correctly. This routine calls AMrescan to set up the scan with
 *		the passed key.
 *
 *		Parameters:
 *				indexRelation -- index relation for scan.
 *				nkeys -- count of scan keys.
 *				key -- array of scan keys to restrict the index scan.
 *
 *		Returns:
 *				An initialized IndexScanDesc.
 * ----------------
 */
IndexScanDesc
RelationGetIndexScan(Relation indexRelation,
					 int nkeys, ScanKey key)
{
	IndexScanDesc scan;

	scan = (IndexScanDesc) palloc(sizeof(IndexScanDescData));

	scan->heapRelation = NULL;	/* may be set later */
	scan->indexRelation = indexRelation;
	scan->xs_snapshot = SnapshotNow;	/* may be set later */
	scan->numberOfKeys = nkeys;

	/*
	 * We allocate the key space here, but the AM is responsible for actually
	 * filling it from the passed key array.
	 */
	if (nkeys > 0)
	{
		scan->keyData = (ScanKey) palloc(sizeof(ScanKeyData) * nkeys);
	}
	else
	{
		scan->keyData = NULL;
	}

	scan->is_multiscan = false; /* caller may change this */
	scan->kill_prior_tuple = false;
	scan->ignore_killed_tuples = true;	/* default setting */

	scan->opaque = NULL;

	ItemPointerSetInvalid(&scan->currentItemData);
	ItemPointerSetInvalid(&scan->currentMarkData);

	ItemPointerSetInvalid(&scan->xs_ctup.t_self);
	scan->xs_ctup.t_data = NULL;
	scan->xs_cbuf = InvalidBuffer;

	/*
	 * Let the AM fill in the key and any opaque data it wants.
	 */
	index_rescan(scan, key);

	return scan;
}

/* ----------------
 *	IndexScanEnd -- End an index scan.
 *
 *		This routine just releases the storage acquired by
 *		RelationGetIndexScan().  Any AM-level resources are
 *		assumed to already have been released by the AM's
 *		endscan routine.
 *
 *	Returns:
 *		None.
 * ----------------
 */
void
IndexScanEnd(IndexScanDesc scan)
{
	if (NULL != scan->keyData)
	{
		pfree(scan->keyData);
	}

	pfree(scan);
}


/* ----------------------------------------------------------------
 *		heap-or-index-scan access to system catalogs
 *
 *		These functions support system catalog accesses that normally use
 *		an index but need to be capable of being switched to heap scans
 *		if the system indexes are unavailable.
 *
 *		The specified scan keys must be compatible with the named index.
 *		Generally this means that they must constrain either all columns
 *		of the index, or the first K columns of an N-column index.
 *
 *		These routines could work with non-system tables, actually,
 *		but they're only useful when there is a known index to use with
 *		the given scan keys; so in practice they're only good for
 *		predetermined types of scans of system catalogs.
 * ----------------------------------------------------------------
 */

/*
 * systable_beginscan --- set up for heap-or-index scan
 *
 *	rel: catalog to scan, already opened and suitably locked
 *	indexId: OID of index to conditionally use
 *	indexOK: if false, forces a heap scan (see notes below)
 *	snapshot: time qual to use (usually should be SnapshotNow)
 *	nkeys, key: scan keys
 *
 * The attribute numbers in the scan key should be set for the heap case.
 * If we choose to index, we reset them to 1..n to reference the index
 * columns.  Note this means there must be one scankey qualification per
 * index column!  This is checked by the Asserts in the normal, index-using
 * case, but won't be checked if the heapscan path is taken.
 *
 * The routine checks the normal cases for whether an indexscan is safe,
 * but caller can make additional checks and pass indexOK=false if needed.
 * In standard case indexOK can simply be constant TRUE.
 */
SysScanDesc
systable_beginscan(Relation heapRelation,
				   Oid indexId,
				   bool indexOK,
				   Snapshot snapshot,
				   int nkeys, ScanKey key)
{
	SysScanDesc sysscan;
	Relation	irel = NULL;
	InMemHeapRelation memheap = NULL;

	bool inmemonly = FALSE;

	if (PG_AOSEGMENT_NAMESPACE == heapRelation->rd_rel->relnamespace
			&& GP_ROLE_EXECUTE == Gp_role)
	{
		inmemonly = TRUE;
	}

	sysscan = (SysScanDesc) palloc0(sizeof(SysScanDescData));

	sysscan->heap_rel = heapRelation;
	sysscan->irel = NULL;
	sysscan->inmem_started = FALSE;

	memheap = OidGetInMemHeapRelation(heapRelation->rd_id, INMEM_HEAP_MAPPING);
	if ((NULL != memheap) && (GP_ROLE_EXECUTE == Gp_role))
	{
		sysscan->inmemscan = InMemHeap_BeginScan(memheap, nkeys, key, NULL /* orig_attnos */, inmemonly);
	}
	else
	{
		sysscan->inmemscan = NULL;
	}

	if (inmemonly && NULL == sysscan->inmemscan)
		elog(ERROR, "initialize an in-memory only system catalog scan of %s relid %u "
			"but in-memory table cannot be found.",
			heapRelation->rd_rel->relname.data, heapRelation->rd_id);

	if (inmemonly || sysscan->inmemscan)
		return sysscan;

	if (indexOK &&
		!IgnoreSystemIndexes &&
		!ReindexIsProcessingIndex(indexId))
	{
		irel = index_open(indexId, AccessShareLock);
	}
	else
	{
		irel = NULL;
	}

	AttrNumber *orig_attnos = NULL;
	if (NULL != irel)
	{
		int			i;

		if (!IsBootstrapProcessingMode())
		{
			Insist(RelationGetRelid(heapRelation) == irel->rd_index->indrelid);
		}
		
		/* save original key attribute numbers in case we need them for an in-memory scan in addition to the index scan */
		orig_attnos = palloc(nkeys * sizeof(AttrNumber));
		/* Change attribute numbers to be index column numbers. */
		for (i = 0; i < nkeys; i++)
		{
			Assert(key[i].sk_attno == irel->rd_index->indkey.values[i]);
			orig_attnos[i] = key[i].sk_attno;
			key[i].sk_attno = i + 1;
		}

		sysscan->iscan = index_beginscan(heapRelation, irel,
										 snapshot, nkeys, key);
		sysscan->scan = NULL;
	}
	else
	{
		sysscan->scan = heap_beginscan(heapRelation, snapshot, nkeys, key);
		sysscan->iscan = NULL;
	}

	sysscan->irel = irel;

	/*
	 * Check if there is in-memory-only tuples.
	 * In the case of in-memory tuples, there is no need to check for
	 * this mapping because it would be passed to the segments together
	 * with the heap tuples.
	 */
	memheap = OidGetInMemHeapRelation(heapRelation->rd_id, INMEM_ONLY_MAPPING);
	if (NULL != memheap)
	{
		sysscan->inmemonlyscan = InMemHeap_BeginScan(memheap, nkeys, key, orig_attnos,
				TRUE /* is memory only - don't scan relation from disk */);
	}

	return sysscan;
}

/*
 * systable_getnext --- get next tuple in a heap-or-index scan
 *
 * Returns NULL if no more tuples available.
 *
 * Note that returned tuple is a reference to data in a disk buffer;
 * it must not be modified, and should be presumed inaccessible after
 * next getnext() or endscan() call.
 */
HeapTuple
systable_getnext(SysScanDesc sysscan)
{
	HeapTuple	htup;

	if (sysscan->inmemscan && (GP_ROLE_EXECUTE == Gp_role))
	{
		htup = InMemHeap_GetNext(sysscan->inmemscan, ForwardScanDirection);
	}
	else
	{
		htup = systable_getnext_or_prev(sysscan, ForwardScanDirection);
	}

	return htup;
}

HeapTuple
systable_getprev(SysScanDesc sysscan)
{
	if (sysscan->inmemscan && (GP_ROLE_EXECUTE == Gp_role))
	{
		elog(ERROR, "systable_getprev() is not allowed on segments");
	}

	return systable_getnext_or_prev(sysscan, BackwardScanDirection);
}

/*
 * systable_getnext_or_prev --- get next or previous tuple
 * depends on the given direction.
 */
static HeapTuple
systable_getnext_or_prev(SysScanDesc sysscan, ScanDirection direction)
{
	HeapTuple htup = NULL;

	if (!sysscan->inmem_started)
	{
		if (NULL != sysscan->irel)
		{
			htup = index_getnext(sysscan->iscan, direction);
		}
		else
		{
			htup = heap_getnext(sysscan->scan, direction);
		}

		if ((NULL == htup) && (NULL != sysscan->inmemonlyscan))
		{
			sysscan->inmem_started = TRUE;
		}
	}

	if (sysscan->inmem_started)
	{
		htup = InMemHeap_GetNext(sysscan->inmemonlyscan, direction);
	}

	return htup;
}

/*
 * systable_endscan --- close scan, release resources
 *
 * Note that it's still up to the caller to close the heap relation.
 */
void
systable_endscan(SysScanDesc sysscan)
{
	if ((NULL != sysscan->inmemscan) && (GP_ROLE_EXECUTE == Gp_role))
	{
		InMemHeap_EndScan(sysscan->inmemscan);
	}
	if (NULL != sysscan->irel)
	{
		index_endscan(sysscan->iscan);
		index_close(sysscan->irel, AccessShareLock);
	}
	if (NULL != sysscan->scan)
	{
		heap_endscan(sysscan->scan);
	}
	if (NULL != sysscan->inmemonlyscan)
	{
		InMemHeap_EndScan(sysscan->inmemonlyscan);
	}

	pfree(sysscan);
}


/*
 * systable_beginscan_ordered --- set up for ordered catalog scan
 *
 * These routines have essentially the same API as systable_beginscan etc,
 * except that they guarantee to return multiple matching tuples in
 * index order.  Also, for largely historical reasons, the index to use
 * is opened and locked by the caller, not here.
 *
 * Currently we do not support non-index-based scans here.	(In principle
 * we could do a heapscan and sort, but the uses are in places that
 * probably don't need to still work with corrupted catalog indexes.)
 * For the moment, therefore, these functions are merely the thinnest of
 * wrappers around index_beginscan/index_getnext.  The main reason for their
 * existence is to centralize possible future support of lossy operators
 * in catalog scans.
 */
SysScanDesc
systable_beginscan_ordered(Relation heapRelation,
						   Relation indexRelation,
						   Snapshot snapshot,
						   int nkeys, ScanKey key)
{
	SysScanDesc sysscan;
	int			i;

	/* REINDEX can probably be a hard error here ... */
	if (ReindexIsProcessingIndex(RelationGetRelid(indexRelation)))
	{
		elog(ERROR, "cannot do ordered scan on index \"%s\", because it is the current REINDEX target",
			 RelationGetRelationName(indexRelation));
	}
	/* ... but we only throw a warning about violating IgnoreSystemIndexes */
	if (IgnoreSystemIndexes)
	{
		elog(WARNING, "using index \"%s\" despite IgnoreSystemIndexes",
			 RelationGetRelationName(indexRelation));
	}

	sysscan = (SysScanDesc) palloc(sizeof(SysScanDescData));

	sysscan->heap_rel = heapRelation;
	sysscan->irel = indexRelation;

	/* Change attribute numbers to be index column numbers. */
	for (i = 0; i < nkeys; i++)
	{
		int			j;

		for (j = 0; j < indexRelation->rd_index->indnatts; j++)
		{
			if (key[i].sk_attno == indexRelation->rd_index->indkey.values[j])
			{
				key[i].sk_attno = j + 1;
				break;
			}
		}
		if (j == indexRelation->rd_index->indnatts)
		{
			elog(ERROR, "column is not in index");
		}
	}

	sysscan->iscan = index_beginscan(heapRelation, indexRelation,
									 snapshot, nkeys, key);
	sysscan->scan = NULL;

	return sysscan;
}

/*
 * systable_getnext_ordered --- get next tuple in an ordered catalog scan
 */
HeapTuple
systable_getnext_ordered(SysScanDesc sysscan, ScanDirection direction)
{
	HeapTuple	htup;

	Assert(NULL != sysscan->irel);
	htup = index_getnext(sysscan->iscan, direction);
	/* See notes in systable_getnext */
	//if (htup && sysscan->iscan->xs_recheck)
	//	elog(ERROR, "system catalog scans with lossy index conditions are not implemented");

	return htup;
}

/*
 * systable_endscan_ordered --- close scan, release resources
 */
void
systable_endscan_ordered(SysScanDesc sysscan)
{
	Assert(NULL != sysscan->irel);
	index_endscan(sysscan->iscan);
	pfree(sysscan);
}
