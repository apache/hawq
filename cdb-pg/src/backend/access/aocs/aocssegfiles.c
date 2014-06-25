/* 
 * AOCS Segment files.
 *
 * Copyright (c) 2009-2013, Greenplum Inc.
 */

#include "postgres.h"

#include "cdb/cdbappendonlystorage.h"
#include "access/aomd.h"
#include "access/heapam.h"
#include "access/genam.h"
#include "access/hio.h"
#include "access/multixact.h"
#include "access/transam.h"
#include "access/tuptoaster.h"
#include "access/valid.h"
#include "access/xact.h"
#include "catalog/catalog.h"
#include "catalog/pg_appendonly.h"
#include "catalog/namespace.h"
#include "catalog/indexing.h"
#include "catalog/gp_fastsequence.h"
#include "catalog/aoseg.h"
#include "cdb/cdbvars.h"
#include "executor/spi.h"
#include "nodes/makefuncs.h"
#include "utils/acl.h"
#include "utils/syscache.h"
#include "utils/lsyscache.h"
#include "utils/numeric.h"
#include "cdb/cdbappendonlyblockdirectory.h"
#include "cdb/cdbappendonlystoragelayer.h"
#include "cdb/cdbappendonlystorageread.h"
#include "cdb/cdbappendonlystoragewrite.h"
#include "utils/datumstream.h"
#include "utils/fmgroids.h"
#include "access/aocssegfiles.h"
#include "access/aosegfiles.h"
#include "access/appendonlywriter.h"
#include "cdb/cdbaocsam.h"
#include "executor/executor.h"

#include "utils/debugbreak.h"
#include "funcapi.h"
#include "access/heapam.h"
#include "catalog/pg_type.h"
#include "utils/builtins.h"



AOCSFileSegInfo *
NewAOCSFileSegInfo(int4 segno, int4 nvp)
{
	AOCSFileSegInfo *seginfo;

	seginfo = (AOCSFileSegInfo *) palloc0(aocsfileseginfo_size(nvp));
	seginfo->segno = segno;
	seginfo->vpinfo.nEntry = nvp;

	return seginfo;
}

void InsertInitialAOCSFileSegInfo(Oid segrelid, int4 segno, int4 contentid, int4 nvp)
{
    bool *nulls = palloc0(sizeof(bool) * Natts_pg_aocsseg);
    Datum *values = palloc0(sizeof(Datum) * Natts_pg_aocsseg);
    AOCSVPInfo *vpinfo = create_aocs_vpinfo(nvp);
    HeapTuple segtup;
	ItemPointerData tid;
	Relation segrel;

    segrel = heap_open(segrelid, RowExclusiveLock);

	InsertFastSequenceEntry(segrelid,
							(int64)segno,
							0,
							contentid,
							&tid);

	values[Anum_pg_aocs_segno-1]		 = Int32GetDatum(segno);
	values[Anum_pg_aocs_tupcount-1]		 = Int64GetDatum(0);
	values[Anum_pg_aocs_varblockcount-1] = Int64GetDatum(0);
	values[Anum_pg_aocs_vpinfo-1]		 = PointerGetDatum(vpinfo);
	values[Anum_pg_aocs_content-1]		 = Int32GetDatum(contentid);

    segtup = heap_form_tuple(RelationGetDescr(segrel), values, nulls);

    frozen_heap_insert(segrel, segtup);

    if (GP_ROLE_EXECUTE != Gp_role)
    {
        CatalogUpdateIndexes(segrel, segtup);
    }

    heap_freetuple(segtup);
    heap_close(segrel, RowExclusiveLock);

    pfree(vpinfo);
    pfree(nulls);
    pfree(values);
}

/* 
 * GetAOCSFileSegInfo.
 * 
 * Get the catalog entry for an appendonly (column-oriented) relation from the
 * pg_aocsseg_* relation that belongs to the currently used
 * AppendOnly table.
 *
 * If a caller intends to append to this (logical) file segment entry they must
 * already hold a relation Append-Only segment file (transaction-scope) lock (tag 
 * LOCKTAG_RELATION_APPENDONLY_SEGMENT_FILE) in order to guarantee
 * stability of the pg_aoseg information on this segment file and exclusive right
 * to append data to the segment file.
 */
AOCSFileSegInfo *
GetAOCSFileSegInfo(
	Relation 			prel,
	AppendOnlyEntry 	*aoEntry,
	Snapshot 			appendOnlyMetaDataSnapshot,
	int32 				segno,
	int32 contentid)
{
	TupleDesc	tupdesc = RelationGetDescr(prel);
	int32 nvp = tupdesc->natts;

    Relation segrel;
    TupleDesc segdsc;
    ScanKeyData scankey[2];
    SysScanDesc aocsscan;
    HeapTuple segtup;

    AOCSFileSegInfo *seginfo;
    Datum *d;
    bool *null;
    bool indexOK;
    Oid indexid;

    segrel = heap_open(aoEntry->segrelid, AccessShareLock);
    segdsc = RelationGetDescr(segrel);

    if (GP_ROLE_EXECUTE == Gp_role)
    {
        indexOK = FALSE;
        indexid = InvalidOid;
    }
    else
    {
        indexOK = TRUE;
        indexid = aoEntry->segidxid;
    }

    ScanKeyInit(&scankey[0], (AttrNumber)Anum_pg_aocs_segno, BTEqualStrategyNumber,
    		F_INT4EQ, Int32GetDatum(segno));
    ScanKeyInit(&scankey[1], (AttrNumber)Anum_pg_aocs_content, BTEqualStrategyNumber,
    		F_INT4EQ, Int32GetDatum(contentid));

    aocsscan = systable_beginscan(segrel, indexid, indexOK, SnapshotNow, 2, &scankey[0]);

    segtup = systable_getnext(aocsscan);

    if(!HeapTupleIsValid(segtup))
    {
        /* This segment file does not have an entry. */
        systable_endscan(aocsscan);
        heap_close(segrel, AccessShareLock);
        return NULL;
    }

	/* Close the index */
    segtup = heap_copytuple(segtup);
	
	systable_endscan(aocsscan);

	Assert(HeapTupleIsValid(segtup));

    seginfo = (AOCSFileSegInfo *) palloc0(aocsfileseginfo_size(nvp));

    d = (Datum *) palloc(sizeof(Datum) * Natts_pg_aocsseg);
    null = (bool *) palloc(sizeof(bool) * Natts_pg_aocsseg); 

    heap_deform_tuple(segtup, segdsc, d, null);

    Assert(!null[Anum_pg_aocs_segno - 1]);
    Assert(DatumGetInt32(d[Anum_pg_aocs_segno - 1] == segno));
    seginfo->segno = segno;

    Assert(!null[Anum_pg_aocs_tupcount - 1]);
    seginfo->tupcount = DatumGetInt64(d[Anum_pg_aocs_tupcount - 1]);

    Assert(!null[Anum_pg_aocs_varblockcount - 1]);
    seginfo->varblockcount = DatumGetInt64(d[Anum_pg_aocs_varblockcount - 1]);

	ItemPointerSetInvalid(&seginfo->sequence_tid);

    Assert(!null[Anum_pg_aocs_vpinfo - 1]);
    {
        struct varlena *v = (struct varlena *) DatumGetPointer(d[Anum_pg_aocs_vpinfo - 1]);
        struct varlena *dv = pg_detoast_datum(v);

        Assert(VARSIZE(dv) == aocs_vpinfo_size(nvp));
        memcpy(&seginfo->vpinfo, dv, aocs_vpinfo_size(nvp));
        if(dv!=v)
            pfree(dv);
    }

    seginfo->content = GpIdentity.segindex;

    pfree(d);
    pfree(null);

    heap_close(segrel, AccessShareLock);

    return seginfo;
}

AOCSFileSegInfo **GetAllAOCSFileSegInfo(Relation prel,
										AppendOnlyEntry *aoEntry,
										Snapshot appendOnlyMetaDataSnapshot,
										bool returnAllSegfiles,
										int32 *totalseg)
{
    Relation 			pg_aocsseg_rel;

	AOCSFileSegInfo		**results;
	TupleDesc	tupdesc = RelationGetDescr(prel);
	
	pg_aocsseg_rel = relation_open(aoEntry->segrelid, AccessShareLock);

	results = GetAllAOCSFileSegInfo_pg_aocsseg_rel(
											tupdesc->natts,
											RelationGetRelationName(prel),
											aoEntry,
											pg_aocsseg_rel,
											appendOnlyMetaDataSnapshot,
											returnAllSegfiles,
											totalseg);

    heap_close(pg_aocsseg_rel, AccessShareLock);

	return results;
}

/*
 * The comparison routine that sorts an array of AOCSFileSegInfos
 * in the ascending order of the segment number.
 */
static int
aocsFileSegInfoCmp(const void *left, const void *right)
{
	AOCSFileSegInfo *leftSegInfo = *((AOCSFileSegInfo **)left);
	AOCSFileSegInfo *rightSegInfo = *((AOCSFileSegInfo **)right);
	
	if (leftSegInfo->content < rightSegInfo->content)
	{
		return -1;
	}

	if (leftSegInfo->content > rightSegInfo->content)
	{
		return 1;
	}

	if (leftSegInfo->segno < rightSegInfo->segno)
	{
		return -1;
	}
	
	if (leftSegInfo->segno > rightSegInfo->segno)
	{
		return 1;
	}
	
	return 0;
}

AOCSFileSegInfo **GetAllAOCSFileSegInfo_pg_aocsseg_rel(
										int numOfColumns,
										char *relationName,
										AppendOnlyEntry *aoEntry,
									    Relation pg_aocsseg_rel,
										Snapshot snapshot,
										bool returnAllSegmentsFiles,
										int32 *totalseg)
{

    int32 nvp = numOfColumns;

    SysScanDesc scan;
    HeapTuple tup;
    ScanKeyData key[1];
    AOCSFileSegInfo **allseg;
    AOCSFileSegInfo *seginfo;
    int cur_seg;
    Datum *d;
    bool *null;
	int seginfo_slot_no = AO_FILESEGINFO_ARRAY_SIZE;

	Assert(aoEntry != NULL);

	/* MPP-16407:
	 * Initialize the segment file information array, we first allocate 8 slot for the array,
	 * then array will be dynamically expanded later if necessary.
	 */
    allseg = (AOCSFileSegInfo **) palloc0(sizeof(AOCSFileSegInfo*) * seginfo_slot_no);
    d = (Datum *) palloc(sizeof(Datum) * Natts_pg_aocsseg);
    null = (bool *) palloc(sizeof(bool) * Natts_pg_aocsseg);

    cur_seg = 0;

    ScanKeyInit(&key[0], (AttrNumber)Anum_pg_aocs_content,
    		BTEqualStrategyNumber, F_INT4EQ, Int32GetDatum(GpIdentity.segindex));

    if (returnAllSegmentsFiles)
    {
        scan = systable_beginscan(pg_aocsseg_rel, InvalidOid, FALSE, snapshot, 0, NULL);
    }
    else
    {
        scan = systable_beginscan(pg_aocsseg_rel, InvalidOid, FALSE, snapshot, 1, &key[0]);
    }
	while (HeapTupleIsValid(tup = systable_getnext(scan)))
    {
		/* dynamically expand space for AOCSFileSegInfo* array */
		if (cur_seg >= seginfo_slot_no)
		{
			seginfo_slot_no *= 2;
			allseg = (AOCSFileSegInfo **) repalloc(allseg, sizeof(AOCSFileSegInfo*) * seginfo_slot_no);
		}
		
        seginfo = (AOCSFileSegInfo *) palloc0(aocsfileseginfo_size(nvp));

        allseg[cur_seg] = seginfo;

        if (Gp_role == GP_ROLE_DISPATCH)
		{
        	GetTupleVisibilitySummary(
								tup,
								&seginfo->tupleVisibilitySummary);
		}

        heap_deform_tuple(tup, RelationGetDescr(pg_aocsseg_rel), d, null);

        Assert(!null[Anum_pg_aocs_segno - 1]);
        seginfo->segno = DatumGetInt32(d[Anum_pg_aocs_segno - 1]);

        Assert(!null[Anum_pg_aocs_tupcount - 1]);
        seginfo->tupcount = DatumGetInt64(d[Anum_pg_aocs_tupcount - 1]);

        Assert(!null[Anum_pg_aocs_varblockcount - 1]);
        seginfo->varblockcount = DatumGetInt64(d[Anum_pg_aocs_varblockcount - 1]);

		ItemPointerSetInvalid(&seginfo->sequence_tid);

       Assert(!null[Anum_pg_aocs_vpinfo - 1]);
        {
            struct varlena *v = (struct varlena *) DatumGetPointer(d[Anum_pg_aocs_vpinfo - 1]);
            struct varlena *dv = pg_detoast_datum(v);

            /* 
             * VARSIZE(dv) may be less than aocs_vpinfo_size, in case of
             * add column, we try to do a ctas from old table to new table.
             */
            Assert(VARSIZE(dv) <= aocs_vpinfo_size(nvp));

            memcpy(&seginfo->vpinfo, dv, VARSIZE(dv));
            if(dv!=v)
                pfree(dv);
        }

        seginfo->content = DatumGetInt32(d[Anum_pg_aocs_content - 1]);
        ++cur_seg;
    }

    pfree(d);
    pfree(null);

    systable_endscan(scan);

	*totalseg = cur_seg;

	if (*totalseg == 0)
	{
		pfree(allseg);
		
		return NULL;
	}
	
	/*
	 * Sort allseg by the order of segment file number.
	 *
	 * Currently this is only needed when building a bitmap index to guarantee the tids
	 * are in the ascending order. But since this array is pretty small, we just sort
	 * the array for all cases.
	 */
	qsort((char *)allseg, *totalseg, sizeof(AOCSFileSegInfo *), aocsFileSegInfoCmp);

    return allseg;
}

/*
 * GetAOCSTotalBytes
 *
 * Get the total bytes for a specific AOCS table from the pg_aocsseg table on master.
 *
 * In hawq, master keep all segfile info in pg_aocsseg table,
 * therefore it get the whole table size.
 */
int64 GetAOCSTotalBytes(Relation parentrel, Snapshot appendOnlyMetaDataSnapshot)
{
	AOCSFileSegInfo **allseg;
	int totalseg;
	int64 result;
	int s;
	AOCSVPInfo *vpinfo;
	AppendOnlyEntry *aoEntry = NULL;

	aoEntry = GetAppendOnlyEntry(RelationGetRelid(parentrel), appendOnlyMetaDataSnapshot);
	Assert(aoEntry != NULL);

	Assert(GP_ROLE_EXECUTE != Gp_role);

	result = 0;
	allseg = GetAllAOCSFileSegInfo(parentrel, aoEntry, appendOnlyMetaDataSnapshot, true, &totalseg);
	for (s = 0; s < totalseg; s++)
	{
		int32 nEntry;
		int e;
		
		vpinfo = &((allseg[s])->vpinfo);
		nEntry = vpinfo->nEntry;

		for (e = 0; e < nEntry; e++)
			result += vpinfo->entry[e].eof;

		pfree(allseg[s]);
	}

	pfree(aoEntry);
	if (allseg)
	{
		pfree(allseg);
	}

	return result;
}

void UpdateAOCSFileSegInfoOnMaster(AOCSInsertDesc idesc)
{
	int i;
	int64 *eof, *uncomp_eof;
	int nattr = idesc->aoi_rel->rd_att->natts;

	eof = palloc(nattr * sizeof(int64));
	uncomp_eof = palloc(nattr * sizeof(int64));

	for (i = 0; i < nattr; ++i)
	{
		eof[i] = idesc->ds[i]->eof;
		uncomp_eof[i] = idesc->ds[i]->eofUncompress;
	}

	UpdateAOCSFileSegInfo(idesc->aoi_rel, idesc->aoEntry, idesc->cur_segno,
			idesc->insertCount, idesc->varblockCount, eof, uncomp_eof,
			GpIdentity.segindex);

	pfree(eof);
	pfree(uncomp_eof);
}

void UpdateAOCSFileSegInfo(Relation prel, AppendOnlyEntry *aoEntry,
		int32 segno, int64 insertCount, int64 varblockCount,
		int64 *eof, int64 *uncomp_eof, int32 contentid)
{
	LockAcquireResult acquireResult;
	
    Relation segrel;

    ScanKeyData key[2];
    SysScanDesc scan;

    HeapTuple oldtup;
    HeapTuple newtup;
    Datum d[Natts_pg_aocsseg];
    bool null[Natts_pg_aocsseg] = {0, };
    bool repl[Natts_pg_aocsseg] = {0, };

	TupleDesc tupdesc = RelationGetDescr(prel);
	int nvp = tupdesc->natts;
    int i;
    AOCSVPInfo *vpinfo = create_aocs_vpinfo(nvp);

	Insist(GP_ROLE_EXECUTE != Gp_role);

	/*
	 * Since we have the segment-file entry under lock (with LockRelationAppendOnlySegmentFile)
	 * we can use SnapshotNow.
	 */
    Snapshot usesnapshot = SnapshotNow;

	Assert(aoEntry != NULL);

	if (Gp_role != GP_ROLE_DISPATCH)
	{
		/*
		 * Verify we already have the write-lock!
		 */
		acquireResult = LockRelationAppendOnlySegmentFile(
													&prel->rd_node,
													segno,
													AccessExclusiveLock,
													/* dontWait */ false,
													contentid);
		if (acquireResult != LOCKACQUIRE_ALREADY_HELD)
		{
			elog(ERROR, "Should already have the (transaction-scope) write-lock on Append-Only segment file #%d, "
						 "relation %s", segno,
				 RelationGetRelationName(prel));
		}
	}

    segrel = heap_open(aoEntry->segrelid, RowExclusiveLock);
    tupdesc = RelationGetDescr(segrel);

    /* Setup a scan key to fetch from the indexed by segno and content id */
    ScanKeyInit(&key[0],
    		(AttrNumber)Anum_pg_aocs_segno,
    		BTEqualStrategyNumber,
    		F_INT4EQ,
    		Int32GetDatum(segno));

    ScanKeyInit(&key[1],
    		(AttrNumber)Anum_pg_aocs_content,
    		BTEqualStrategyNumber,
    		F_INT4EQ,
    		Int32GetDatum(contentid));

    scan = systable_beginscan(segrel, aoEntry->segidxid, TRUE, usesnapshot, 2, &key[0]);

    oldtup = systable_getnext(scan);

    if(!HeapTupleIsValid(oldtup))
        ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT),
                    errmsg("AOCS table \"%s\" file segment \"%d\" does not exist",
                        RelationGetRelationName(prel), segno)
                    ));

#ifdef USE_ASSERT_CHECKING
	d[Anum_pg_aocs_segno - 1] =
			fastgetattr(oldtup, Anum_pg_aocs_segno, tupdesc, &null[Anum_pg_aocs_segno - 1]);
	Assert(!null[Anum_pg_aocs_segno - 1]);
	Assert(DatumGetInt32(d[Anum_pg_aocs_segno - 1]) == segno);
#endif

	d[Anum_pg_aocs_tupcount - 1] =
			fastgetattr(oldtup, Anum_pg_aocs_tupcount, tupdesc, &null[Anum_pg_aocs_tupcount - 1]);
	Assert(!null[Anum_pg_aocs_tupcount -1]);

	d[Anum_pg_aocs_tupcount - 1] += insertCount;
	repl[Anum_pg_aocs_tupcount - 1] = true;

	d[Anum_pg_aocs_varblockcount - 1] =
			fastgetattr(oldtup, Anum_pg_aocs_varblockcount, tupdesc, &null[Anum_pg_aocs_varblockcount - 1]);
	Assert(!null[Anum_pg_aocs_varblockcount - 1]);
	d[Anum_pg_aocs_varblockcount - 1] += varblockCount;
	repl[Anum_pg_aocs_varblockcount - 1] = true;

    for(i=0; i<nvp; ++i)
    {
        vpinfo->entry[i].eof = eof[i];
        vpinfo->entry[i].eof_uncompressed = uncomp_eof[i];
    }
    d[Anum_pg_aocs_vpinfo - 1] = PointerGetDatum(vpinfo);
    null[Anum_pg_aocs_vpinfo - 1] = false;
    repl[Anum_pg_aocs_vpinfo - 1] = true;

    newtup = heap_modify_tuple(oldtup, tupdesc, d, null, repl);

    simple_heap_update(segrel, &oldtup->t_self, newtup);

    CatalogUpdateIndexes(segrel, newtup);

    pfree(newtup);
    pfree(vpinfo);

    systable_endscan(scan);
    heap_close(segrel, RowExclusiveLock);
}

void AOCSFileSegInfoAddCount(Relation prel, AppendOnlyEntry *aoEntry, int32 segno, int64 tupadded, int64 varblockadded)
{
	LockAcquireResult acquireResult;
	
    Relation segrel;

    ScanKeyData key[2];
    SysScanDesc scan;

    HeapTuple oldtup;
    HeapTuple newtup;
    Datum d[Natts_pg_aocsseg];
    bool null[Natts_pg_aocsseg] = {0, };
    bool repl[Natts_pg_aocsseg] = {0, };
    bool indexOK;
    Oid indexid;

    TupleDesc tupdesc; 

	/*
	 * Since we have the segment-file entry under lock (with LockRelationAppendOnlySegmentFile)
	 * we can use SnapshotNow.
	 */
    Snapshot usesnapshot = SnapshotNow;

	Assert(aoEntry != NULL);

	/*
	 * Verify we already have the write-lock!
	 */
	acquireResult = LockRelationAppendOnlySegmentFile(
												&prel->rd_node,
												segno,
												AccessExclusiveLock,
												/* dontWait */ false,
												GpIdentity.segindex);
	if (acquireResult != LOCKACQUIRE_ALREADY_HELD)
	{
		elog(ERROR, "Should already have the (transaction-scope) write-lock on Append-Only segment file #%d, "
			 "relation %s", segno, RelationGetRelationName(prel));
	}

    segrel = heap_open(aoEntry->segrelid, RowExclusiveLock);
    tupdesc = segrel->rd_att;

    /* In GP-SQL, index only exists on master*/
    if (GP_ROLE_EXECUTE == Gp_role)
    {
        indexOK = FALSE;
        indexid = InvalidOid;
    }
    else
    {
        indexOK = TRUE;
        indexid = aoEntry->segidxid;
    }

    /* setup the scan key */
    ScanKeyInit(&key[0], (AttrNumber) Anum_pg_aocs_segno,
    		BTEqualStrategyNumber, F_INT4EQ, Int32GetDatum(segno));
    ScanKeyInit(&key[1], (AttrNumber) Anum_pg_aocs_content,
    		BTEqualStrategyNumber, F_INT4EQ, Int32GetDatum(GpIdentity.segindex));

    scan = systable_beginscan(segrel, indexid, indexOK, usesnapshot, 2, &key[0]);

    oldtup = systable_getnext(scan);

    if(!HeapTupleIsValid(oldtup))
        ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT),
                    errmsg("AOCS table \"%s\" file segment \"%d\" does not exist",
                        RelationGetRelationName(prel), segno)
                    ));

#ifdef USE_ASSERT_CHECKING
    d[Anum_pg_aocs_segno-1] = fastgetattr(oldtup, Anum_pg_aocs_segno, tupdesc, &null[Anum_pg_aocs_segno-1]);
    Assert(!null[Anum_pg_aocs_segno-1]);
    Assert(DatumGetInt32(d[Anum_pg_aocs_segno-1]) == segno);
#endif

    d[Anum_pg_aocs_tupcount-1] = fastgetattr(oldtup, Anum_pg_aocs_tupcount, tupdesc, &null[Anum_pg_aocs_tupcount-1]);
    Assert(!null[Anum_pg_aocs_tupcount-1]);

    d[Anum_pg_aocs_tupcount-1] += tupadded;
    repl[Anum_pg_aocs_tupcount-1] = true;

    d[Anum_pg_aocs_varblockcount-1] = fastgetattr(oldtup, Anum_pg_aocs_varblockcount, tupdesc, &null[Anum_pg_aocs_varblockcount-1]);
    Assert(!null[Anum_pg_aocs_varblockcount-1]);
    d[Anum_pg_aocs_varblockcount-1] += varblockadded;
    repl[Anum_pg_aocs_tupcount-1] = true;

    newtup = heap_modify_tuple(oldtup, tupdesc, d, null, repl);

    simple_heap_update(segrel, &oldtup->t_self, newtup);
    if (GP_ROLE_EXECUTE != Gp_role)
    {
        CatalogUpdateIndexes(segrel, newtup);
    }

    heap_freetuple(newtup);

    systable_endscan(scan);
    heap_close(segrel, RowExclusiveLock);
}

extern Datum aocsvpinfo_decode(PG_FUNCTION_ARGS);
Datum aocsvpinfo_decode(PG_FUNCTION_ARGS)
{
    AOCSVPInfo *vpinfo = (AOCSVPInfo *) PG_GETARG_BYTEA_P(0);
    int i = PG_GETARG_INT32(1);
    int j = PG_GETARG_INT32(2);
    int64 result;

    if (i<0 || i>=vpinfo->nEntry)
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                        errmsg("invalid entry for decoding aocsvpinfo")
                    ));

    if (j<0 || j>1)
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                        errmsg("invalid entry for decoding aocsvpinfo")
                    ));

    if (j==0)
        result = vpinfo->entry[i].eof;
    else
        result = vpinfo->entry[i].eof_uncompressed;

    PG_RETURN_INT64(result);
}


PG_MODULE_MAGIC;

PG_FUNCTION_INFO_V1(gp_aocsseg);

extern Datum
gp_aocsseg(PG_FUNCTION_ARGS);

Datum
gp_aocsseg(PG_FUNCTION_ARGS)
{
    int aocsRelOid = PG_GETARG_OID(0);

	typedef struct Context
	{
		Oid		aocsRelOid;

		int		relnatts;
		
		struct AOCSFileSegInfo **aocsSegfileArray;

		int 	totalAocsSegFiles;

		int		segfileArrayIndex;

		int		columnNum;
						// 0-based index into columns.
	} Context;
	
	FuncCallContext *funcctx;
	Context *context;

	if (Gp_role != GP_ROLE_DISPATCH)
	{
		ereport(ERROR,
				(errcode(ERRCODE_GP_COMMAND_ERROR),
				errmsg("gp_aocsseg cannot be called on segments.")));
	}

	if (SRF_IS_FIRSTCALL())
	{
		TupleDesc	tupdesc;
		MemoryContext oldcontext;
		Relation aocsRel;
		AppendOnlyEntry *aoEntry;
		Relation pg_aocsseg_rel;

		/* create a function context for cross-call persistence */
		funcctx = SRF_FIRSTCALL_INIT();

		/*
		 * switch to memory context appropriate for multiple function
		 * calls
		 */
		oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

		/* build tupdesc for result tuples */
		tupdesc = CreateTemplateTupleDesc(7, false);
		TupleDescInitEntry(tupdesc, (AttrNumber) 1, "gp_tid",
						   TIDOID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 2, "segno",
						   INT4OID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 3, "column_num",
						   INT2OID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 4, "physical_segno",
						   INT4OID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 5, "tupcount",
						   INT8OID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 6, "eof",
						   INT8OID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 7, "eof_uncompressed",
						   INT8OID, -1, 0);

		funcctx->tuple_desc = BlessTupleDesc(tupdesc);

		/*
		 * Collect all the locking information that we will format and send
		 * out as a result set.
		 */
		context = (Context *) palloc(sizeof(Context));
		funcctx->user_fctx = (void *) context;

		context->aocsRelOid = aocsRelOid;

		aocsRel = heap_open(aocsRelOid, NoLock);
		if(!RelationIsAoCols(aocsRel))
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					errmsg("'%s' is not an append-only columnar relation",
							RelationGetRelationName(aocsRel))));

		// Remember the number of columns.
		context->relnatts = RelationGetNumberOfAttributes(aocsRel);

		aoEntry = GetAppendOnlyEntry(aocsRelOid, SnapshotNow);
		
		pg_aocsseg_rel = heap_open(aoEntry->segrelid, NoLock);
		
		context->aocsSegfileArray = GetAllAOCSFileSegInfo_pg_aocsseg_rel(
														RelationGetNumberOfAttributes(aocsRel),
														RelationGetRelationName(aocsRel),
														aoEntry, 
														pg_aocsseg_rel,
														SnapshotNow,
														true,
														&context->totalAocsSegFiles);

		heap_close(pg_aocsseg_rel, NoLock);
		heap_close(aocsRel, NoLock);

		// Iteration positions.
		context->segfileArrayIndex = 0;
		context->columnNum = 0;

		funcctx->user_fctx = (void *) context;

		MemoryContextSwitchTo(oldcontext);
	}

	funcctx = SRF_PERCALL_SETUP();
	context = (Context *) funcctx->user_fctx;

	/*
	 * Process each column for each segment file.
	 */
	while (true)
	{
		Datum		values[7];
		bool		nulls[7];
		HeapTuple	tuple;
		Datum		result;

		struct AOCSFileSegInfo *aocsSegfile;

		AOCSVPInfoEntry *entry;
		
		if (context->segfileArrayIndex >= context->totalAocsSegFiles)
		{
			break;
		}

		if (context->columnNum >= context->relnatts)
		{
			/*
			 * Finished with the current segment file.
			 */
			context->segfileArrayIndex++;
			if (context->segfileArrayIndex >= context->totalAocsSegFiles)
			{
				continue;
			}

			context->columnNum = 0;
		}
		
		/*
		 * Form tuple with appropriate data.
		 */
		MemSet(values, 0, sizeof(values));
		MemSet(nulls, false, sizeof(nulls));

		aocsSegfile = context->aocsSegfileArray[context->segfileArrayIndex];

		entry = getAOCSVPEntry(aocsSegfile, context->columnNum);

		values[0] = ItemPointerGetDatum(&aocsSegfile->tupleVisibilitySummary.tid);
		values[1] = Int32GetDatum(aocsSegfile->segno);
		values[2] = Int16GetDatum(context->columnNum);
		values[3] = Int32GetDatum(context->columnNum * AOTupleId_MultiplierSegmentFileNum + aocsSegfile->segno);
		values[4] = Int64GetDatum(aocsSegfile->tupcount);
		values[5] = Int64GetDatum(entry->eof);
		values[6] = Int64GetDatum(entry->eof_uncompressed);

		tuple = heap_form_tuple(funcctx->tuple_desc, values, nulls);
		result = HeapTupleGetDatum(tuple);

		// Indicate we emitted one column.
		context->columnNum++;

		SRF_RETURN_NEXT(funcctx, result);
	}

	SRF_RETURN_DONE(funcctx);
}

PG_FUNCTION_INFO_V1(gp_aocsseg_history);

extern Datum
gp_aocsseg_history(PG_FUNCTION_ARGS);

Datum
gp_aocsseg_history(PG_FUNCTION_ARGS)
{
    int aocsRelOid = PG_GETARG_OID(0);

	typedef struct Context
	{
		Oid		aocsRelOid;

		int		relnatts;
		
		struct AOCSFileSegInfo **aocsSegfileArray;

		int 	totalAocsSegFiles;

		int		segfileArrayIndex;

		int		columnNum;
						// 0-based index into columns.
	} Context;
	
	FuncCallContext *funcctx;
	Context *context;

	if (Gp_role != GP_ROLE_DISPATCH)
	{
		ereport(ERROR,
				(errcode(ERRCODE_GP_COMMAND_ERROR),
				errmsg("gp_aocsseg_history cannot be called on segments.")));
	}

	if (SRF_IS_FIRSTCALL())
	{
		TupleDesc	tupdesc;
		MemoryContext oldcontext;
		Relation aocsRel;
		AppendOnlyEntry *aoEntry;
		Relation pg_aocsseg_rel;

		/* create a function context for cross-call persistence */
		funcctx = SRF_FIRSTCALL_INIT();

		/*
		 * switch to memory context appropriate for multiple function
		 * calls
		 */
		oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

		/* build tupdesc for result tuples */
		tupdesc = CreateTemplateTupleDesc(17, false);
		TupleDescInitEntry(tupdesc, (AttrNumber) 1, "gp_tid",
						   TIDOID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 2, "gp_xmin",
						   INT4OID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 3, "gp_xmin_status",
						   TEXTOID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 4, "gp_xmin_distrib_id",
						   TEXTOID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 5, "gp_xmax",
						   INT4OID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 6, "gp_xmax_status",
						   TEXTOID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 7, "gp_xmax_distrib_id",
						   TEXTOID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 8, "gp_command_id",
						   INT4OID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 9, "gp_infomask",
						   TEXTOID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 10, "gp_update_tid",
						   TIDOID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 11, "gp_visibility",
						   TEXTOID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 12, "segno",
						   INT4OID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 13, "column_num",
						   INT2OID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 14, "physical_segno",
						   INT4OID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 15, "tupcount",
						   INT8OID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 16, "eof",
						   INT8OID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 17, "eof_uncompressed",
						   INT8OID, -1, 0);

		funcctx->tuple_desc = BlessTupleDesc(tupdesc);

		/*
		 * Collect all the locking information that we will format and send
		 * out as a result set.
		 */
		context = (Context *) palloc(sizeof(Context));
		funcctx->user_fctx = (void *) context;

		context->aocsRelOid = aocsRelOid;

		aocsRel = heap_open(aocsRelOid, NoLock);
		if(!RelationIsAoCols(aocsRel))
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					errmsg("'%s' is not an append-only columnar relation",
							RelationGetRelationName(aocsRel))));

		// Remember the number of columns.
		context->relnatts = RelationGetNumberOfAttributes(aocsRel);

		aoEntry = GetAppendOnlyEntry(aocsRelOid, SnapshotNow);
		
		pg_aocsseg_rel = heap_open(aoEntry->segrelid, NoLock);
		
		context->aocsSegfileArray = GetAllAOCSFileSegInfo_pg_aocsseg_rel(
														RelationGetNumberOfAttributes(aocsRel),
														RelationGetRelationName(aocsRel),
														aoEntry, 
														pg_aocsseg_rel,
														SnapshotAny,	// Get ALL tuples from pg_aocsseg_% including aborted and in-progress ones. 
														true,
														&context->totalAocsSegFiles);

		heap_close(pg_aocsseg_rel, NoLock);
		heap_close(aocsRel, NoLock);

		// Iteration positions.
		context->segfileArrayIndex = 0;
		context->columnNum = 0;

		funcctx->user_fctx = (void *) context;

		MemoryContextSwitchTo(oldcontext);
	}

	funcctx = SRF_PERCALL_SETUP();
	context = (Context *) funcctx->user_fctx;

	/*
	 * Process each column for each segment file.
	 */
	while (true)
	{
		Datum		values[17];
		bool		nulls[17];
		HeapTuple	tuple;
		Datum		result;

		struct AOCSFileSegInfo *aocsSegfile;

		AOCSVPInfoEntry *entry;
		
		if (context->segfileArrayIndex >= context->totalAocsSegFiles)
		{
			break;
		}

		if (context->columnNum >= context->relnatts)
		{
			/*
			 * Finished with the current segment file.
			 */
			context->segfileArrayIndex++;
			if (context->segfileArrayIndex >= context->totalAocsSegFiles)
			{
				continue;
			}

			context->columnNum = 0;
		}
		
		/*
		 * Form tuple with appropriate data.
		 */
		MemSet(values, 0, sizeof(values));
		MemSet(nulls, false, sizeof(nulls));

		aocsSegfile = context->aocsSegfileArray[context->segfileArrayIndex];

		entry = getAOCSVPEntry(aocsSegfile, context->columnNum);

		GetTupleVisibilitySummaryDatums(
								&values[0],
								&nulls[0],
								&aocsSegfile->tupleVisibilitySummary);

		values[11] = Int32GetDatum(aocsSegfile->segno);
		values[12] = Int16GetDatum(context->columnNum);
		values[13] = Int32GetDatum(context->columnNum * AOTupleId_MultiplierSegmentFileNum + aocsSegfile->segno);
		values[14] = Int64GetDatum(aocsSegfile->tupcount);
		values[15] = Int64GetDatum(entry->eof);
		values[16] = Int64GetDatum(entry->eof_uncompressed);

		tuple = heap_form_tuple(funcctx->tuple_desc, values, nulls);
		result = HeapTupleGetDatum(tuple);

		// Indicate we emitted one column.
		context->columnNum++;

		SRF_RETURN_NEXT(funcctx, result);
	}

	SRF_RETURN_DONE(funcctx);
}

Datum
gp_update_aocol_master_stats_internal(Relation parentrel, Snapshot appendOnlyMetaDataSnapshot)
{
	StringInfoData	sqlstmt;
	bool			connected = false;
	char		   *aoseg_relname;
	int				proc;
	int				ret;
	float8			total_count = 0;
	MemoryContext 	oldcontext = CurrentMemoryContext;
	TupleDesc		tupdesc = RelationGetDescr(parentrel);
	int32			nvp = tupdesc->natts;
	AppendOnlyEntry *aoEntry = GetAppendOnlyEntry(RelationGetRelid(parentrel), appendOnlyMetaDataSnapshot);

	Assert(aoEntry != NULL);
	
	/*
	 * get the name of the aoseg relation
	 */
	aoseg_relname = get_rel_name(aoEntry->segrelid);
	if (NULL == aoseg_relname)
		elog(ERROR, "failed to get relname for AO file segment");

	/*
	 * assemble our query string
	 */
	initStringInfo(&sqlstmt);
	appendStringInfo(&sqlstmt, "select segno,sum(tupcount) "
					"from gp_dist_random('pg_aoseg.%s') "
					"group by (segno)", aoseg_relname);


	PG_TRY();
	{

		if (SPI_OK_CONNECT != SPI_connect())
		{
			ereport(ERROR, (errcode(ERRCODE_CDB_INTERNAL_ERROR),
							errmsg("Unable to obtain AO relation information from segment databases."),
							errdetail("SPI_connect failed in gp_update_ao_master_stats")));
		}
		connected = true;

		/* Do the query. */
		ret = SPI_execute(sqlstmt.data, false, 0);
		proc = SPI_processed;


		if (ret > 0 && SPI_tuptable != NULL)
		{
			TupleDesc tupdesc = SPI_tuptable->tupdesc;
			SPITupleTable *tuptable = SPI_tuptable;
			int i;

			/*
			 * Iterate through each result tuple
			 */
			for (i = 0; i < proc; i++)
			{
				HeapTuple 	tuple = tuptable->vals[i];
				AOCSFileSegInfo *aocsfsinfo = NULL;
				int			qe_segno;
				int64 		qe_tupcount;
				char 		*val_segno;
				char 		*val_tupcount;
				MemoryContext 	cxt_save;

				/*
				 * Get totals from QE's for a specific segment
				 */
				val_segno = SPI_getvalue(tuple, tupdesc, 1);
				val_tupcount = SPI_getvalue(tuple, tupdesc, 2);

				/* use our own context so that SPI won't free our stuff later */
				cxt_save = MemoryContextSwitchTo(oldcontext);

				/*
				 * Convert to desired data type
				 */
				qe_segno = pg_atoi(val_segno, sizeof(int32), 0);
				qe_tupcount = DatumGetInt64(DirectFunctionCall1(int8in,
																CStringGetDatum(val_tupcount)));

				total_count += qe_tupcount;

				/*
				 * Get the numbers on the QD for this segment
				 */
					
				
				// CONSIDER: For integrity, we should lock ALL segment files first before 
				// executing the query.  And, the query of the segments (the SPI_execute)
				// and the update (UpdateFileSegInfo) should be in the same transaction.
				//
				// If there are concurrent Append-Only inserts, we can end up with
				// the wrong answer...
				//
				// NOTE: This is a transaction scope lock that must be held until commit / abort.
				//
				LockRelationAppendOnlySegmentFile(
												&parentrel->rd_node,
												qe_segno,
												AccessExclusiveLock,
												/* dontWait */ false,
												GpIdentity.segindex);
					
				aocsfsinfo = GetAOCSFileSegInfo(parentrel, aoEntry, appendOnlyMetaDataSnapshot, qe_segno, GpIdentity.segindex);
				if (aocsfsinfo == NULL)
				{
					Assert(!"in GP-SQL, master should dispatch seginfo to all QEs");

					InsertInitialAOCSFileSegInfo(aoEntry->segrelid, qe_segno, GpIdentity.segindex, nvp);

					aocsfsinfo = NewAOCSFileSegInfo(qe_segno, nvp);
				}

				/*
				 * check if numbers match.
				 * NOTE: proper way is to use int8eq() but since we
				 * don't expect any NAN's in here better do it directly
				 */
				if(aocsfsinfo->tupcount != qe_tupcount)
				{
					int64 	diff = qe_tupcount - aocsfsinfo->tupcount;

					elog(DEBUG3, "gp_update_ao_master_stats: updating "
						"segno %d with tupcount %d", qe_segno,
						(int)qe_tupcount);

					/*
					 * QD tup count !=  QE tup count. update QD count by
					 * passing in the diff (may be negative sometimes).
					 */
					AOCSFileSegInfoAddCount(parentrel, aoEntry, qe_segno, diff, 0);
				}
				else
					elog(DEBUG3, "gp_update_ao_master_stats: no need to "
						"update segno %d. it is synced", qe_segno);

				pfree(aocsfsinfo);

				MemoryContextSwitchTo(cxt_save);

				/*
				 * TODO: if an entry exists for this rel in the AO hash table
				 * need to also update that entry in shared memory. Need to
				 * figure out how to do this safely when concurrent operations
				 * are in progress. note that if no entry exists we are ok.
				 *
				 * At this point this doesn't seem too urgent as we generally
				 * only expect this function to update segno 0 only and the QD
				 * never cares about segment 0 anyway.
				 */
			}
		}

		connected = false;

		SPI_finish();
	}

	/* Clean up in case of error. */
	PG_CATCH();
	{
		if (connected)
			SPI_finish();

		/* Carry on with error handling. */
		PG_RE_THROW();
	}
	PG_END_TRY();

	pfree(aoEntry);
		
	pfree(sqlstmt.data);

	PG_RETURN_FLOAT8(total_count);
}

Datum
aocol_compression_ratio_internal(Relation parentrel)
{
	Relation pg_aocsseg_rel;
	TupleDesc pg_aocsseg_dsc;
	HeapTuple tuple;
	HeapScanDesc aocsscan;
	Datum vpinfo;
	float8 total_eof = 0;
	float8 total_eof_uncompressed = 0;
	bool isNull;
	AppendOnlyEntry *aoEntry = NULL;
	float8 compress_ratio = -1; /* the default, meaning "not available" */

	Assert(GpIdentity.segindex == -1); /* Make sure this function is called in master */

	aoEntry = GetAppendOnlyEntry(RelationGetRelid(parentrel), SnapshotNow);

	Assert(aoEntry != NULL);

	pg_aocsseg_rel = heap_open(aoEntry->segrelid, AccessShareLock);
	pg_aocsseg_dsc = RelationGetDescr(pg_aocsseg_rel);

	aocsscan = heap_beginscan(pg_aocsseg_rel, SnapshotNow, 0, NULL);

	while (HeapTupleIsValid(tuple = heap_getnext(aocsscan, ForwardScanDirection)))
	{
		vpinfo = fastgetattr(tuple, Anum_pg_aocs_vpinfo, pg_aocsseg_dsc, &isNull);
		Assert(!isNull);

		{
			struct varlena *v = (struct varlena *)DatumGetPointer(vpinfo);
			struct varlena *dv = pg_detoast_datum(v);
			AOCSVPInfo *vpinfo = (AOCSVPInfo *)dv;
			int i;
			for (i = 0; i < vpinfo->nEntry; i++)
			{
				total_eof += vpinfo->entry[i].eof;
				total_eof_uncompressed += vpinfo->entry[i].eof_uncompressed;
			}
			if(dv != v)
			{
				pfree(dv);
			}
		}

		CHECK_FOR_INTERRUPTS();
	}

	heap_endscan(aocsscan);
	heap_close(pg_aocsseg_rel, AccessShareLock);
	pfree(aoEntry);

	if (total_eof > 0)
	{
		char  buf[8];

		/* calculate the compression ratio */
		float8 compress_ratio_raw = DatumGetFloat8(DirectFunctionCall2(float8div,
																	Float8GetDatum(total_eof_uncompressed),
																	Float8GetDatum(total_eof)));

		/* format to 2 digits past the decimal point */
		snprintf(buf, 8, "%.2f", compress_ratio_raw);

		/* format to 2 digit decimal precision */
		compress_ratio = DatumGetFloat8(DirectFunctionCall1(float8in,
										CStringGetDatum(buf)));

	}

	PG_RETURN_FLOAT8(compress_ratio);
}

/**
 * Free up seginfo array.
 */
void
FreeAllAOCSSegFileInfo(AOCSFileSegInfo **allAOCSSegInfo, int totalSegFiles)
{
	Assert(allAOCSSegInfo);

	for(int file_no = 0; file_no < totalSegFiles; file_no++)
	{
		Assert(allAOCSSegInfo[file_no] != NULL);

		pfree(allAOCSSegInfo[file_no]);
	}
}

