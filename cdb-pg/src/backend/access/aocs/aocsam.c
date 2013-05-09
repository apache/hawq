/*
 * Append only columnar access methods
 *
 *	Copyright (c), 2009-2013, Greenplum Inc.
 */

#include "postgres.h"

#include "fmgr.h"
#include "access/appendonlytid.h"
#include "access/aomd.h"
#include "access/heapam.h"
#include "access/hio.h"
#include "access/multixact.h"
#include "access/transam.h"
#include "access/tuptoaster.h"
#include "access/valid.h"
#include "access/xact.h"
#include "access/appendonlywriter.h"
#include "catalog/catalog.h"
#include "catalog/pg_appendonly.h"
#include "catalog/pg_attribute_encoding.h"
#include "catalog/namespace.h"
#include "catalog/gp_fastsequence.h"
#include "cdb/cdbvars.h"
#include "cdb/cdbappendonlyblockdirectory.h"
#include "cdb/cdbappendonlystoragelayer.h"
#include "cdb/cdbappendonlystorageread.h"
#include "cdb/cdbappendonlystoragewrite.h"
#include "utils/datumstream.h"
#include "access/aocssegfiles.h"
#include "cdb/cdbaocsam.h"
#include "cdb/cdbappendonlyam.h"
#include "miscadmin.h"
#include "pgstat.h"
#include "storage/procarray.h"
#include "utils/inval.h"
#include "utils/lsyscache.h"
#include "utils/relcache.h"
#include "utils/syscache.h"
#include "storage/freespace.h"
#include "storage/smgr.h"
#include "storage/quicklz1.h"
#include "storage/quicklz3.h"
#include "utils/guc.h"

#include "utils/debugbreak.h"

/*
 * Open the segment file for a specified column
 * associated with the datum stream.
 */
static void open_datumstreamread_segfile(
									 char *basepath, RelFileNode node,
									 AOCSFileSegInfo *segInfo,
									 DatumStreamRead *ds,
									 int colNo)
{
	int segNo = segInfo->segno;
	char fn[MAXPGPATH];
	int32	fileSegNo;

	AOCSVPInfoEntry *e = getAOCSVPEntry(segInfo, colNo);

	FormatAOSegmentFileName(basepath, segNo, colNo, &fileSegNo, fn);
	Assert(strlen(fn) + 1 <= MAXPGPATH);

	Assert(ds);
	datumstreamread_open_file(ds, fn, e->eof, e->eof_uncompressed, node, fileSegNo);
}

/*
 * Open all segment files associted with the datum stream.
 *
 * Currently, there is one segment file for each column. This function
 * only opens files for those columns which are in the projection.
 *
 * If blockDirectory is not NULL, the first block info is written to
 * the block directory.
 */
static void open_all_datumstreamread_segfiles(Relation rel,
										  AOCSFileSegInfo *segInfo,
										  DatumStreamRead **ds,
										  bool *proj,
										  int nvp,
										  AppendOnlyBlockDirectory *blockDirectory)
{
    char *basepath = relpath(rel->rd_node);

    int i;

    Assert(proj);

    for(i=0; i<nvp; ++i)
    {
        if (proj[i])
	{
			open_datumstreamread_segfile(basepath, rel->rd_node, segInfo, ds[i], i);
			datumstreamread_block(ds[i]);

			if (blockDirectory != NULL)
			{
				AppendOnlyBlockDirectory_InsertEntry(blockDirectory,
													 i,
													 ds[i]->blockFirstRowNum,
													 ds[i]->blockFileOffset,
													 ds[i]->blockRowCount);
			}
		}
    }

    pfree(basepath);
}

/*
 * Initialise data streams for every column used in this query. For writes, this
 * means all columns.
 */
static void
open_ds_write(Relation rel,
        DatumStreamWrite **ds, TupleDesc relationTupleDesc, bool *proj,
		AORelationVersion version)
{
	int		nvp = relationTupleDesc->natts;
	StdRdOptions 	**opts = RelationGetAttributeOptions(rel);

	/* open datum streams.  It will open segment file underneath */
	for (int i = 0; i < nvp; ++i)
	{
		Form_pg_attribute attr = relationTupleDesc->attrs[i];
		char *ct;
		int32 clvl;
		int32 blksz;
		bool checksum;

		StringInfoData titleBuf;

		// UNDONE: Need to track and dispose of this storage...
		initStringInfo(&titleBuf);
		appendStringInfo(&titleBuf, "Write of Append-Only Column-Oriented relation '%s', column #%d '%s'",
						 RelationGetRelationName(rel),
						 i + 1,
						 NameStr(attr->attname));
		
		/*
		 * If the column has column specific encoding, we use those. Note
		 * that even if the user specified no encoding for this column,
		 * we will have represented that as compresstype=none.
		 */
		if (opts[i])
		{
			ct = opts[i]->compresstype;
			clvl = opts[i]->compresslevel;
			blksz = opts[i]->blocksize;
			checksum = opts[i]->checksum;
		}
		else
		{
			ct = "none";
			clvl = 0;
			blksz = DEFAULT_APPENDONLY_BLOCK_SIZE;
			checksum = false;
		}

		ds[i] = create_datumstreamwrite(
									ct, 
									clvl, 
									checksum,
									/* safeFSWriteSize */ 0,	// UNDONE: Need to wire down pg_appendonly column?
									blksz, 
									version, 
									attr,
									RelationGetRelationName(rel),
									/* title */ titleBuf.data);

	}
}

/*
 * Initialise data streams for every column used in this query. For writes, this
 * means all columns.
 */
static void
open_ds_read(Relation rel,
        DatumStreamRead **ds, TupleDesc relationTupleDesc, bool *proj,
		AORelationVersion version)
{
	int		nvp = relationTupleDesc->natts;
	StdRdOptions 	**opts = RelationGetAttributeOptions(rel);

	/* open datum streams.  It will open segment file underneath */
	for (int i = 0; i < nvp; ++i)
	{
		Form_pg_attribute attr = relationTupleDesc->attrs[i];
		char *ct;
		int32 clvl;
		int32 blksz;
		bool checksum;

		/*
		 * If the column has column specific encoding, we use those. Note
		 * that even if the user specified no encoding for this column,
		 * we will have represented that as compresstype=none.
		 */
		if (opts[i])
		{
			ct = opts[i]->compresstype;
			clvl = opts[i]->compresslevel;
			blksz = opts[i]->blocksize;
			checksum = opts[i]->checksum;
		}
		else
		{
			ct = "none";
			clvl = 0;
			blksz = DEFAULT_APPENDONLY_BLOCK_SIZE;
			checksum = false;
		}

		if (proj[i])
		{
			StringInfoData titleBuf;
			
			// UNDONE: Need to track and dispose of this storage...
			initStringInfo(&titleBuf);
			appendStringInfo(&titleBuf, "Scan of Append-Only Column-Oriented relation '%s', column #%d '%s'",
							 RelationGetRelationName(rel),
							 i + 1,
							 NameStr(attr->attname));

			ds[i] = create_datumstreamread(
										ct, 
										clvl,
										checksum,
										/* safeFSWriteSize */ false,	// UNDONE: Need to wire down pg_appendonly column
										blksz, 
										version, 
										attr,
										RelationGetRelationName(rel),
										/* title */ titleBuf.data);

		}
		else
			/* We aren't projecting this column, so nothing to do */
			ds[i] = NULL;
	}
}

static void close_ds_read(DatumStreamRead **ds, int nvp)
{
    int i;
    for(i=0; i<nvp; ++i)
    {
        if(ds[i])
        {
            destroy_datumstreamread(ds[i]);
            ds[i] = NULL;
        }
    }
}

static void close_ds_write(DatumStreamWrite **ds, int nvp)
{
    int i;
    for(i=0; i<nvp; ++i)
    {
        if(ds[i])
        {
            destroy_datumstreamwrite(ds[i]);
            ds[i] = NULL;
        }
    }
}


static void aocs_initscan(AOCSScanDesc scan)
{
    scan->cur_seg = -1;

    ItemPointerSet(&scan->cdb_fake_ctid, 0, 0);
    scan->cur_seg_row = 0;

    open_ds_read(scan->aos_rel,
    		scan->ds, scan->relationTupleDesc, scan->proj, scan->aoEntry->version);

    pgstat_count_heap_scan(scan->aos_rel);
}

static int open_next_scan_seg(AOCSScanDesc scan)
{
    int nvp = scan->relationTupleDesc->natts;

    while(++scan->cur_seg < scan->total_seg)
    {
        AOCSFileSegInfo * curSegInfo = scan->seginfo[scan->cur_seg];

        if(curSegInfo->tupcount > 0)
        {
            int i = 0;
            bool emptySeg = false;
            for(; i<nvp; ++i)
            {
                if (scan->proj[i])
                {
                    AOCSVPInfoEntry *e = getAOCSVPEntry(curSegInfo, i);
                    if (e->eof == 0)
                        emptySeg = true;

                    break;
                }
            }

            if (!emptySeg)
            {

				/* If the scan also builds the block directory, initialize it here. */
				if (scan->buildBlockDirectory)
				{
					ItemPointerData tid;

					Assert(scan->blockDirectory != NULL);
					Assert(scan->aoEntry != NULL);

                    /*
                     * if building the block directory, we need to make sure the sequence
                     *   starts higher than our highest tuple's rownum.  In the case of upgraded blocks,
                     *   the highest tuple will have tupCount as its row num
                     * for non-upgrade cases, which use the sequence, it will be enough to start
                     *   off the end of the sequence; note that this is not ideal -- if we are at
                     *   least curSegInfo->tupcount + 1 then we don't even need to update
                     *   the sequence value
                     */
                    int64 firstSequence =
                        GetFastSequences(scan->aoEntry->segrelid,
                                         curSegInfo->segno,
                                         curSegInfo->tupcount + 1,
                                         NUM_FAST_SEQUENCES,
                                         &tid);

					AppendOnlyBlockDirectory_Init_forInsert(scan->blockDirectory,
															scan->aoEntry,
															scan->appendOnlyMetaDataSnapshot,
															(FileSegInfo *) curSegInfo,
															0 /* lastSequence */,
															scan->aos_rel,
															curSegInfo->segno,
															nvp,
															true);

					Assert(!"need contentid here");
                    InsertFastSequenceEntry(scan->aoEntry->segrelid,
											curSegInfo->segno,
											firstSequence,
											/*TODO, need change in hawq*/
											-1,
											&tid);
				}

				open_all_datumstreamread_segfiles(
											  scan->aos_rel,
											  curSegInfo,
											  scan->ds,
											  scan->proj,
											  nvp,
											  scan->blockDirectory);

				return scan->cur_seg;
			}
		}
    }

    return -1;
}

static void close_cur_scan_seg(AOCSScanDesc scan)
{
    int nvp = scan->relationTupleDesc->natts;

	if (scan->cur_seg < 0)
		return;

    for(int i=0; i<nvp; ++i)
    {
        if(scan->ds[i])
        {
            datumstreamread_close_file(scan->ds[i]);
        }
    }

	if (scan->buildBlockDirectory)
	{
		Assert(scan->blockDirectory != NULL);
		AppendOnlyBlockDirectory_End_forInsert(scan->blockDirectory);
	}
}

/**
 * begin the scan over the given relation.
 *
 * @param relationTupleDesc if NULL, then this function will simply use relation->rd_att.  This is the typical use-case.
 *               Passing in a separate tuple descriptor is only needed for cases for the caller has changed
 *               relation->rd_att without updating the underlying relation files yet (that is, the caller is doing
 *               an alter and relation->rd_att will be the relation's new form but relationTupleDesc is the old form)
 */
AOCSScanDesc
aocs_beginscan(Relation relation, Snapshot appendOnlyMetaDataSnapshot, TupleDesc relationTupleDesc, bool *proj)
{
    AOCSScanDesc scan;
    AppendOnlyEntry *aoentry;

	ValidateAppendOnlyMetaDataSnapshot(&appendOnlyMetaDataSnapshot);
	
    if (!relationTupleDesc)
		relationTupleDesc = RelationGetDescr(relation);

    int nvp = relationTupleDesc->natts;

    RelationIncrementReferenceCount(relation);
    scan = (AOCSScanDesc) palloc0(sizeof(AOCSScanDescData));
    scan->aos_rel = relation;
	scan->appendOnlyMetaDataSnapshot = appendOnlyMetaDataSnapshot;

    aoentry = GetAppendOnlyEntry(RelationGetRelid(relation), appendOnlyMetaDataSnapshot);
    scan->aoEntry = aoentry;
    Assert(aoentry->majorversion == 1 && aoentry->minorversion == 0);

    scan->compLevel = aoentry->compresslevel;
    scan->compType = aoentry->compresstype;
    scan->blocksz = aoentry->blocksize;

    scan->seginfo = GetAllAOCSFileSegInfo(relation, aoentry, appendOnlyMetaDataSnapshot, &scan->total_seg);

    scan->relationTupleDesc = relationTupleDesc;

    Assert(proj);
    scan->proj = proj;

    scan->ds = (DatumStreamRead **) palloc0(sizeof(DatumStreamRead *) * nvp);

    aocs_initscan(scan);

	scan->buildBlockDirectory = false;
	scan->blockDirectory = NULL;

    return scan;
}

void aocs_rescan(AOCSScanDesc scan)
{
    close_cur_scan_seg(scan);
    close_ds_read(scan->ds, scan->relationTupleDesc->natts);
    aocs_initscan(scan);
}

void aocs_endscan(AOCSScanDesc scan)
{
    int i;

    RelationDecrementReferenceCount(scan->aos_rel);

	close_cur_scan_seg(scan);
    close_ds_read(scan->ds, scan->relationTupleDesc->natts);

    pfree(scan->ds);

    for(i=0; i<scan->total_seg; ++i)
    {
        if(scan->seginfo[i])
        {
            pfree(scan->seginfo[i]);
            scan->seginfo[i] = NULL;
        }
    }
    if (scan->seginfo)
        pfree(scan->seginfo);

	pfree(scan->aoEntry);
    pfree(scan);
}

void aocs_getnext(AOCSScanDesc scan, ScanDirection direction, TupleTableSlot *slot)
{
    int ncol;
    Datum *d = slot_get_values(slot);
    bool *null = slot_get_isnull(slot);
	AOTupleId aoTupleId;
	int64 rowNum = INT64CONST(-1);

    int err = 0;
    int i;

    Assert(ScanDirectionIsForward(direction));

    ncol = slot->tts_tupleDescriptor->natts;
    Assert(ncol <= scan->relationTupleDesc->natts);

    while(1)
    {
ReadNext:
        /* If necessary, open next seg */
        if(scan->cur_seg < 0 || err < 0)
        {
            err = open_next_scan_seg(scan);
            if(err < 0)
            {
                /* No more seg, we are at the end */
                ExecClearTuple(slot);
				scan->cur_seg = -1;
                return;
            }
			scan->cur_seg_row = 0;
        }

        Assert(scan->cur_seg >= 0);

        /* Read from cur_seg */
        for(i=0; i<ncol; ++i)
        {
            if(scan->proj[i])
            {
                err = datumstreamread_advance(scan->ds[i]);
                Assert(err >= 0);
                if(err == 0)
                {
                    err = datumstreamread_block(scan->ds[i]);
                    if(err < 0)
                    {
                        /* Ha, cannot read next block,
		                           * we need to go to next seg
		                           */
						close_cur_scan_seg(scan);
                        goto ReadNext;
                    }

					if (scan->buildBlockDirectory)
					{
						Assert(scan->blockDirectory != NULL);

						AppendOnlyBlockDirectory_InsertEntry(scan->blockDirectory,
															 i,
															 scan->ds[i]->blockFirstRowNum,
															 scan->ds[i]->blockFileOffset,
															 scan->ds[i]->blockRowCount);
					}

                    err = datumstreamread_advance(scan->ds[i]);
                    Assert(err > 0);
                }

				/*
				 * Get the column's datum right here since the data structures should still
				 * be hot in CPU data cache memory.
				 */
                datumstreamread_get(scan->ds[i], &d[i], &null[i]);

				if (rowNum == INT64CONST(-1) &&
					scan->ds[i]->blockFirstRowNum != INT64CONST(-1))
				{
					Assert(scan->ds[i]->blockFirstRowNum > 0);
					rowNum = scan->ds[i]->blockFirstRowNum +
						datumstreamread_nth(scan->ds[i]);

				}
            }
        }

		AOTupleIdInit_Init(&aoTupleId);
		AOTupleIdInit_segmentFileNum(&aoTupleId,
									 scan->seginfo[scan->cur_seg]->segno);

		scan->cur_seg_row++;
		if (rowNum == INT64CONST(-1))
		{
			AOTupleIdInit_rowNum(&aoTupleId, scan->cur_seg_row);
		}
		else
		{
			AOTupleIdInit_rowNum(&aoTupleId, rowNum);
		}

		scan->cdb_fake_ctid = *((ItemPointer)&aoTupleId);

        TupSetVirtualTupleNValid(slot, ncol);
        slot_set_ctid(slot, &(scan->cdb_fake_ctid));
        return;
    }

    Assert(!"Never here");
    return;
}


/* Open next file segment for write.  See SetCurrentFileSegForWrite */
/* XXX Right now, we put each column to different files */
static void OpenAOCSDatumStreams(AOCSInsertDesc desc)
{
    char *basepath = relpath(desc->aoi_rel->rd_node);
    char fn[MAXPGPATH];
	int32	fileSegNo;

    AOCSFileSegInfo *seginfo;

    TupleDesc tupdesc = RelationGetDescr(desc->aoi_rel);
    int nvp = tupdesc->natts;

    desc->ds = (DatumStreamWrite **) palloc0(sizeof(DatumStreamWrite *) * nvp);

	/*
	* In order to append to this file segment entry we must first
	* acquire the relation Append-Only segment file (transaction-scope) lock (tag
	* LOCKTAG_RELATION_APPENDONLY_SEGMENT_FILE) in order to guarantee
	* stability of the pg_aoseg information on this segment file and exclusive right
	* to append data to the segment file.
	*
	* NOTE: This is a transaction scope lock that must be held until commit / abort.
	*/
	LockRelationAppendOnlySegmentFile(
								&desc->aoi_rel->rd_node,
								desc->cur_segno,
								AccessExclusiveLock,
								/* dontWait */ false,
								GpIdentity.segindex);

    open_ds_write(desc->aoi_rel,
    		desc->ds, tupdesc, NULL, desc->aoEntry->version);

    /* Now open seg info file and get eof mark. */
    seginfo = GetAOCSFileSegInfo(
    					desc->aoi_rel, 
    					desc->aoEntry, 
    					desc->appendOnlyMetaDataSnapshot, 
    					desc->cur_segno,
    					GpIdentity.segindex);
	if (seginfo == NULL)
	{
		InsertInitialAOCSFileSegInfo(desc->aoEntry->segrelid, desc->cur_segno, GpIdentity.segindex, nvp);

		seginfo = NewAOCSFileSegInfo(desc->cur_segno, nvp);
	}
	desc->fsInfo = seginfo;
	desc->rowCount = seginfo->tupcount;

    for(int i=0; i<nvp; ++i)
    {
        AOCSVPInfoEntry *e = getAOCSVPEntry(seginfo, i);

		FormatAOSegmentFileName(basepath, seginfo->segno, i, &fileSegNo, fn);
		Assert(strlen(fn) + 1 <= MAXPGPATH);

        datumstreamwrite_open_file(desc->ds[i], fn, e->eof, e->eof_uncompressed,
				desc->aoi_rel->rd_node,
				fileSegNo);
    }

    pfree(basepath);
}

static inline void
SetBlockFirstRowNums(DatumStreamWrite **datumStreams,
					 int numDatumStreams,
					 int64 blockFirstRowNum)
{
	int i;

	Assert(datumStreams != NULL);

	for (i=0; i<numDatumStreams; i++)
	{
		Assert(datumStreams[i] != NULL);

		datumStreams[i]->blockFirstRowNum =	blockFirstRowNum;
	}
}


AOCSInsertDesc aocs_insert_init(Relation rel, int segno)
{
	AOCSInsertDesc desc;
	AppendOnlyEntry *aoentry;
	TupleDesc tupleDesc;
	int64 firstSequence = 0;

    aoentry = GetAppendOnlyEntry(RelationGetRelid(rel), SnapshotNow);
    Assert(aoentry->majorversion == 1 && aoentry->minorversion == 0);


    desc = (AOCSInsertDesc) palloc0(sizeof(AOCSInsertDescData));
    desc->aoi_rel = rel;
	desc->appendOnlyMetaDataSnapshot = SnapshotNow;
							// Writers uses this since they have exclusive access to the lock acquired with
							// LockRelationAppendOnlySegmentFile for the segment-file.
							

	tupleDesc = RelationGetDescr(desc->aoi_rel);

	Assert(segno >= 0);
	desc->cur_segno = segno;
	desc->aoEntry = aoentry;

	desc->compLevel = aoentry->compresslevel;
	desc->compType = aoentry->compresstype;
	desc->blocksz = aoentry->blocksize;

	OpenAOCSDatumStreams(desc);

	/*
	 * Obtain the next list of fast sequences for this relation.
	 *
	 * Even in the case of no indexes, we need to update the fast
	 * sequences, since the table may contain indexes at some
	 * point of time.
	 */
	desc->numSequences = 0;

	firstSequence =
		GetFastSequences(desc->aoEntry->segrelid,
						 segno,
						 desc->rowCount + 1,
						 NUM_FAST_SEQUENCES,
						 &desc->fsInfo->sequence_tid);
	desc->numSequences = NUM_FAST_SEQUENCES;

	/* Set last_sequence value */
	Assert(firstSequence > desc->rowCount);
	desc->lastSequence = firstSequence - 1;

	SetBlockFirstRowNums(desc->ds, tupleDesc->natts, desc->lastSequence + 1);

	/* Initialize the block directory. */
	tupleDesc = RelationGetDescr(rel);
	AppendOnlyBlockDirectory_Init_forInsert(
		&(desc->blockDirectory), 
		aoentry, 
		desc->appendOnlyMetaDataSnapshot,		// CONCERN: Safe to assume all block directory entries for segment are "covered" by same exclusive lock.
		(FileSegInfo *)desc->fsInfo, desc->lastSequence,
		rel, segno, tupleDesc->natts, true);

    return desc;
}


Oid aocs_insert_values(AOCSInsertDesc idesc, Datum *d, bool * null, AOTupleId *aoTupleId)
{
    Relation rel = idesc->aoi_rel;
    TupleDesc tupdesc = RelationGetDescr(rel);
    int i;

    if (rel->rd_rel->relhasoids)
		ereport(ERROR,
				(errcode(ERRCODE_GP_FEATURE_NOT_SUPPORTED),
				 errmsg("append-only column-oriented tables do not support rows with OIDs")));

    /* As usual, at this moment, we assume one col per vp */
    for(i=0; i<tupdesc->natts; ++i)
    {
    	void *toFree1;
		Datum datum;

		datum = d[i];
        int err = datumstreamwrite_put(idesc->ds[i], datum, null[i], &toFree1);
		if (toFree1 != NULL)
		{
			/*
			 * Use the de-toasted and/or de-compressed as datum instead.
			 */
			datum = PointerGetDatum(toFree1);
		}
        if(err < 0)
        {
			int itemCount = datumstreamwrite_nth(idesc->ds[i]);
			void *toFree2;

			/* write the block up to this one */
			datumstreamwrite_block(idesc->ds[i]);
			if (itemCount > 0)
			{
				/* Insert an entry to the block directory */
				AppendOnlyBlockDirectory_InsertEntry(
					&idesc->blockDirectory,
					i,
					idesc->ds[i]->blockFirstRowNum,
					AppendOnlyStorageWrite_LastWriteBeginPosition(&idesc->ds[i]->ao_write),
					itemCount);

    			/* since we have written all up to the new tuple,
	    		  *   the new blockFirstRowNum is the inserted tuple's row number */
                idesc->ds[i]->blockFirstRowNum = idesc->lastSequence + 1;
			}

			Assert(idesc->ds[i]->blockFirstRowNum == idesc->lastSequence + 1);


            /* now write this new item to the new block */
            err = datumstreamwrite_put(idesc->ds[i], datum, null[i], &toFree2);
			Assert(toFree2 == NULL);
            if (err < 0)
            {
                Assert(!null[i]);
				/*
				 * rle_type is running on a block stream, if an object spans multiple
				 * blocks than data will not be compressed (if rle_type is set).
				 */
				if ((idesc->compType != NULL) && (pg_strcasecmp(idesc->compType, "rle_type") == 0))
				{
					idesc->ds[i]->ao_write.storageAttributes.compress = FALSE;
				}

                err = datumstreamwrite_lob(idesc->ds[i], datum);
                Assert(err >= 0);

                /* Insert an entry to the block directory */
                AppendOnlyBlockDirectory_InsertEntry(
                    &idesc->blockDirectory,
                    i,
                    idesc->ds[i]->blockFirstRowNum,
                    AppendOnlyStorageWrite_LastWriteBeginPosition(&idesc->ds[i]->ao_write),
                    1 /*itemCount -- always just the lob just inserted */
                );


				/*
				 * A lob will live by itself in the block so
				 * this assignment is for the block that contains tuples
				 * AFTER the one we are inserting
				 */
				idesc->ds[i]->blockFirstRowNum = idesc->lastSequence + 2;
            }
        }

		if (toFree1 != NULL)
		{
			pfree(toFree1);
		}
    }

    idesc->insertCount++;
	idesc->lastSequence++;
	if (idesc->numSequences > 0)
		(idesc->numSequences)--;

	Assert(idesc->numSequences >= 0);

	AOTupleIdInit_Init(aoTupleId);
	AOTupleIdInit_segmentFileNum(aoTupleId, idesc->cur_segno);
	AOTupleIdInit_rowNum(aoTupleId, idesc->lastSequence);

	/*
	 * If the allocated fast sequence numbers are used up, we request for
	 * a next list of fast sequence numbers.
	 */
	if (idesc->numSequences == 0)
	{
		int64 firstSequence;

		/*
		 * in GP-SQL, catalog is in memory heap table,
		 * ItemPointer of tuple is invalid.
		 */
		if (GP_ROLE_EXECUTE == Gp_role)
		{
			firstSequence = GetFastSequences(idesc->aoEntry->segrelid,
					idesc->cur_segno, idesc->lastSequence + 1,
					NUM_FAST_SEQUENCES, &idesc->fsInfo->sequence_tid);
		}
		else
		{
			Assert(ItemPointerIsValid(&idesc->fsInfo->sequence_tid));

			firstSequence = GetFastSequencesByTid(&idesc->fsInfo->sequence_tid,
							idesc->lastSequence + 1,
							NUM_FAST_SEQUENCES);
		}
		Assert(firstSequence == idesc->lastSequence + 1);
		idesc->numSequences = NUM_FAST_SEQUENCES;
	}

    return InvalidOid;
}

void aocs_insert_finish(AOCSInsertDesc idesc)
{
    Relation rel = idesc->aoi_rel;
    TupleDesc tupdesc = RelationGetDescr(rel);
    int i;

    idesc->sendback->segno = idesc->cur_segno;
    idesc->sendback->varblock = idesc->varblockCount;
    idesc->sendback->insertCount = idesc->insertCount;

    idesc->sendback->numfiles = rel->rd_att->natts;

    idesc->sendback->eof = palloc(rel->rd_att->natts * sizeof(int64));
	idesc->sendback->uncompressed_eof = palloc(rel->rd_att->natts * sizeof(int64));

	idesc->sendback->nextFastSequence = idesc->lastSequence + idesc->numSequences - 1;

    for(i=0; i<tupdesc->natts; ++i)
    {
		int itemCount = datumstreamwrite_nth(idesc->ds[i]);

        datumstreamwrite_block(idesc->ds[i]);

		AppendOnlyBlockDirectory_InsertEntry(
			&idesc->blockDirectory,
			i,
			idesc->ds[i]->blockFirstRowNum,
			AppendOnlyStorageWrite_LastWriteBeginPosition(&idesc->ds[i]->ao_write),
			itemCount);

        datumstreamwrite_close_file(idesc->ds[i]);

		idesc->sendback->eof[i] = idesc->ds[i]->eof;
		idesc->sendback->uncompressed_eof[i] = idesc->ds[i]->eofUncompress;
    }

	AppendOnlyBlockDirectory_End_forInsert(&(idesc->blockDirectory));

	if (Gp_role != GP_ROLE_EXECUTE)
		UpdateAOCSFileSegInfoOnMaster(idesc);

	pfree(idesc->fsInfo);
    pfree(idesc->aoEntry);

	close_ds_write(idesc->ds, tupdesc->natts);
}

static void
positionFirstBlockOfRange(
	DatumStreamFetchDesc datumStreamFetchDesc)
{
	AppendOnlyBlockDirectoryEntry_GetBeginRange(
				&datumStreamFetchDesc->currentBlock.blockDirectoryEntry,
				&datumStreamFetchDesc->scanNextFileOffset,
				&datumStreamFetchDesc->scanNextRowNum);
}

static void
positionLimitToEndOfRange(
	DatumStreamFetchDesc datumStreamFetchDesc)
{
	AppendOnlyBlockDirectoryEntry_GetEndRange(
				&datumStreamFetchDesc->currentBlock.blockDirectoryEntry,
				&datumStreamFetchDesc->scanAfterFileOffset,
				&datumStreamFetchDesc->scanLastRowNum);
}


static void
positionSkipCurrentBlock(
	DatumStreamFetchDesc datumStreamFetchDesc)
{
	datumStreamFetchDesc->scanNextFileOffset =
		datumStreamFetchDesc->currentBlock.fileOffset +
		datumStreamFetchDesc->currentBlock.overallBlockLen;

	datumStreamFetchDesc->scanNextRowNum =
		datumStreamFetchDesc->currentBlock.lastRowNum + 1;
}

static void
fetchFromCurrentBlock(AOCSFetchDesc aocsFetchDesc,
					  int64 rowNum,
					  TupleTableSlot *slot,
					  int colno)
{
	DatumStreamFetchDesc datumStreamFetchDesc =
		aocsFetchDesc->datumStreamFetchDesc[colno];
	DatumStreamRead *datumStream = datumStreamFetchDesc->datumStream;
	Datum value;
	bool null;

	int rowNumInBlock = rowNum - datumStreamFetchDesc->currentBlock.firstRowNum;

	Assert(rowNumInBlock >= 0);

	/*
	 * MPP-17061: gotContents could be false in the case of aborted rows.
	 * As described in the repro in MPP-17061, if aocs_fetch is trying to
	 * fetch an invisible/aborted row, it could set the block header metadata
	 * of currentBlock to the next CO block, but without actually reading in
	 * next CO block's content.
	 */
	if (datumStreamFetchDesc->currentBlock.gotContents == false)
	{	
		datumstreamread_block_content(datumStream);
		datumStreamFetchDesc->currentBlock.gotContents = true;
	}

	datumstreamread_find(datumStream, rowNumInBlock);

	if (slot != NULL)
	{
		Datum *values = slot_get_values(slot);
		bool *nulls = slot_get_isnull(slot);

		datumstreamread_get(datumStream, &(values[colno]), &(nulls[colno]));
	}
	else
	{
		datumstreamread_get(datumStream, &value, &null);
	}
}

static bool
scanToFetchValue(AOCSFetchDesc aocsFetchDesc,
				 int64 rowNum,
				 TupleTableSlot *slot,
				 int colno)
{
	DatumStreamFetchDesc datumStreamFetchDesc =
		aocsFetchDesc->datumStreamFetchDesc[colno];
	DatumStreamRead *datumStream = datumStreamFetchDesc->datumStream;
	bool found;

	found = datumstreamread_find_block(datumStream,
								   datumStreamFetchDesc,
								   rowNum);
	if (found)
		fetchFromCurrentBlock(aocsFetchDesc, rowNum, slot, colno);

	return found;
}

static void
closeFetchSegmentFile(DatumStreamFetchDesc datumStreamFetchDesc)
{
	Assert(datumStreamFetchDesc->currentSegmentFile.isOpen);

	datumstreamread_close_file(datumStreamFetchDesc->datumStream);
	datumStreamFetchDesc->currentSegmentFile.isOpen = false;
}

static bool
openFetchSegmentFile(AOCSFetchDesc aocsFetchDesc,
					 int openSegmentFileNum,
					 int colNo)
{
	int		i;

	AOCSFileSegInfo	*fsInfo;
	int			segmentFileNum;
	int64		logicalEof;
	DatumStreamFetchDesc datumStreamFetchDesc =
		aocsFetchDesc->datumStreamFetchDesc[colNo];

	Assert(!datumStreamFetchDesc->currentSegmentFile.isOpen);

	i = 0;
	while (true)
	{
		if (i >= aocsFetchDesc->totalSegfiles)
			return false;	// Segment file not visible in catalog information.

		fsInfo = aocsFetchDesc->segmentFileInfo[i];
		segmentFileNum = fsInfo->segno;
		if (openSegmentFileNum == segmentFileNum)
		{
			AOCSVPInfoEntry *entry = getAOCSVPEntry(fsInfo, colNo);

			logicalEof = entry->eof;
			break;
		}
		i++;
	}

	/*
	 * Don't try to open a segment file when its EOF is 0, since the file may not
	 * exist. See MPP-8280.
	 */
	if (logicalEof == 0)
		return false;

	open_datumstreamread_segfile(
							 aocsFetchDesc->basepath, aocsFetchDesc->relation->rd_node,
							 fsInfo,
							 datumStreamFetchDesc->datumStream,
							 colNo);

	datumStreamFetchDesc->currentSegmentFile.num = openSegmentFileNum;
	datumStreamFetchDesc->currentSegmentFile.logicalEof = logicalEof;

	datumStreamFetchDesc->currentSegmentFile.isOpen = true;

	return true;
}

static void
resetCurrentBlockInfo(CurrentBlock *currentBlock)
{
	currentBlock->have = false;
	currentBlock->firstRowNum = 0;
	currentBlock->lastRowNum = 0;
}

/*
 * Initialize the fetch descriptor.
 */
AOCSFetchDesc
aocs_fetch_init(Relation relation,
				Snapshot appendOnlyMetaDataSnapshot,
				bool *proj)
{
	AOCSFetchDesc aocsFetchDesc;
	int colno;
	AppendOnlyEntry *aoentry;
	char *basePath = relpath(relation->rd_node);
	TupleDesc tupleDesc = RelationGetDescr(relation);
	StdRdOptions 	**opts = RelationGetAttributeOptions(relation);	
	
	ValidateAppendOnlyMetaDataSnapshot(&appendOnlyMetaDataSnapshot);


	/*
	 * increment relation ref count while scanning relation
	 *
	 * This is just to make really sure the relcache entry won't go away while
	 * the scan has a pointer to it.  Caller should be holding the rel open
	 * anyway, so this is redundant in all normal scenarios...
	 */
	RelationIncrementReferenceCount(relation);

	aocsFetchDesc = (AOCSFetchDesc) palloc0(sizeof(AOCSFetchDescData));
	aocsFetchDesc->relation = relation;
	
	aocsFetchDesc->appendOnlyMetaDataSnapshot = appendOnlyMetaDataSnapshot;
	

	aocsFetchDesc->initContext = CurrentMemoryContext;

	aocsFetchDesc->segmentFileNameMaxLen = AOSegmentFilePathNameLen(relation) + 1;
	aocsFetchDesc->segmentFileName =
						(char*)palloc(aocsFetchDesc->segmentFileNameMaxLen);
	aocsFetchDesc->segmentFileName[0] = '\0';
	aocsFetchDesc->basepath = basePath;

	Assert(proj);
	aocsFetchDesc->proj = proj;

	aoentry = GetAppendOnlyEntry(RelationGetRelid(relation), appendOnlyMetaDataSnapshot);
    Assert(aoentry->majorversion == 1 && aoentry->minorversion == 0);
	aocsFetchDesc->aoEntry = aoentry;

	aocsFetchDesc->segmentFileInfo =
		GetAllAOCSFileSegInfo(relation, aoentry, appendOnlyMetaDataSnapshot, &aocsFetchDesc->totalSegfiles);

	Assert(tupleDesc != NULL);

	AppendOnlyBlockDirectory_Init_forSearch(
		&aocsFetchDesc->blockDirectory,
		aoentry,
		appendOnlyMetaDataSnapshot,
		(FileSegInfo **)aocsFetchDesc->segmentFileInfo,
		aocsFetchDesc->totalSegfiles,
		aocsFetchDesc->relation,
		tupleDesc->natts,
		true);

	aocsFetchDesc->datumStreamFetchDesc = (DatumStreamFetchDesc*)
			palloc0(tupleDesc->natts * sizeof(DatumStreamFetchDesc));

	for (colno = 0; colno < tupleDesc->natts; colno++)
	{	
		aocsFetchDesc->datumStreamFetchDesc[colno] = NULL;
		if (proj[colno])
		{
			char *ct;
			int32 clvl;
			int32 blksz;
			bool checksum;
			
			StringInfoData titleBuf;

			/*
			 * If the column has column specific encoding, we use those. Note
			 * that even if the user specified no encoding for this column,
			 * we will have represented that as compresstype=none.
			 */
			if (opts[colno])
			{
				ct = opts[colno]->compresstype;
				clvl = opts[colno]->compresslevel;
				blksz = opts[colno]->blocksize;
				checksum = opts[colno]->checksum;
			}
			else
			{
				ct = "none";
				clvl = 0;
				blksz = DEFAULT_APPENDONLY_BLOCK_SIZE;
				checksum = false;
			}
			
			// UNDONE: Need to track and dispose of this storage...
			initStringInfo(&titleBuf);
			appendStringInfo(&titleBuf, "Fetch from Append-Only Column-Oriented relation '%s', column #%d '%s'",
							 RelationGetRelationName(relation),
							 colno + 1,
							 NameStr(tupleDesc->attrs[colno]->attname));

			aocsFetchDesc->datumStreamFetchDesc[colno] = (DatumStreamFetchDesc)
				palloc0(sizeof(DatumStreamFetchDescData));

			aocsFetchDesc->datumStreamFetchDesc[colno]->datumStream =
				create_datumstreamread(
								   ct,
								   clvl,
								   checksum,
									/* safeFSWriteSize */ false,	// UNDONE: Need to wire down pg_appendonly column
								   blksz,
								   aoentry->version,
								   tupleDesc->attrs[colno],
								   RelationGetRelationName(relation),
								   /* title */ titleBuf.data);

		}
	}

	return aocsFetchDesc;
}

/*
 * Fetch the tuple based on the given tuple id.
 *
 * If the 'slot' is not NULL, the tuple will be assigned to the slot.
 *
 * Return true if the tuple is found. Otherwise, return false.
 */
bool
aocs_fetch(AOCSFetchDesc aocsFetchDesc,
		   AOTupleId *aoTupleId,
		   TupleTableSlot *slot)
{
	int segmentFileNum = AOTupleIdGet_segmentFileNum(aoTupleId);
	int64 rowNum = AOTupleIdGet_rowNum(aoTupleId);
    TupleDesc tupdesc = RelationGetDescr(aocsFetchDesc->relation);
	int numCols = tupdesc->natts;
	int colno;
	bool found = true;

	Assert(numCols > 0);

	/*
	 * Go through columns one by one. Check if the current block
	 * has the requested tuple. If so, fetch it. Otherwise, read
	 * the block that contains the requested tuple.
	 */
	for(colno=0; colno<numCols; colno++)
	{
		DatumStreamFetchDesc datumStreamFetchDesc = aocsFetchDesc->datumStreamFetchDesc[colno];

		/* If this column does not need to be fetched, skip it. */
		if (datumStreamFetchDesc == NULL)
			continue;
		
		if (Debug_appendonly_print_datumstream)
			elog(LOG, 
				 "aocs_fetch filePathName %s segno %u rowNum  " INT64_FORMAT " firstRowNum " INT64_FORMAT " lastRowNum " INT64_FORMAT " ",
				 datumStreamFetchDesc->datumStream->ao_read.bufferedRead.filePathName,
				 datumStreamFetchDesc->currentSegmentFile.num,
				 rowNum,
				 datumStreamFetchDesc->currentBlock.firstRowNum,
				 datumStreamFetchDesc->currentBlock.lastRowNum);				
		
		/*
		 * If the current block has the requested tuple, read it.
		 */
		if (datumStreamFetchDesc->currentSegmentFile.isOpen &&
			datumStreamFetchDesc->currentSegmentFile.num == segmentFileNum &&
			aocsFetchDesc->blockDirectory.currentSegmentFileNum == segmentFileNum &&
			datumStreamFetchDesc->currentBlock.have)
		{			
			if (rowNum >= datumStreamFetchDesc->currentBlock.firstRowNum &&
				rowNum <= datumStreamFetchDesc->currentBlock.lastRowNum)
			{
				fetchFromCurrentBlock(aocsFetchDesc, rowNum, slot, colno);
				continue;
			}

			/*
			 * Otherwise, fetch the right block.
			 */
			if (AppendOnlyBlockDirectoryEntry_RangeHasRow(
					&(datumStreamFetchDesc->currentBlock.blockDirectoryEntry),
					rowNum))
			{
				/*
				 * The tuple is covered by the current Block Directory entry,
				 * but is it before or after our current block?
				 */
				if (rowNum < datumStreamFetchDesc->currentBlock.firstRowNum)
				{
					/*
					 * Set scan range to prior block
					 */
					positionFirstBlockOfRange(datumStreamFetchDesc);

					datumStreamFetchDesc->scanAfterFileOffset =
						datumStreamFetchDesc->currentBlock.fileOffset;
					datumStreamFetchDesc->scanLastRowNum =
						datumStreamFetchDesc->currentBlock.firstRowNum - 1;
				}
				else
				{
					/*
					 * Set scan range to following blocks.
					 */
					positionSkipCurrentBlock(datumStreamFetchDesc);
					positionLimitToEndOfRange(datumStreamFetchDesc);
				}

				if (!scanToFetchValue(aocsFetchDesc, rowNum, slot, colno))
				{
					found = false;
					break;
				}

				continue;
			}
		}

		/*
		 * Open or switch open, if necessary.
		 */
		if (datumStreamFetchDesc->currentSegmentFile.isOpen &&
			segmentFileNum != datumStreamFetchDesc->currentSegmentFile.num)
		{
			closeFetchSegmentFile(datumStreamFetchDesc);

			Assert(!datumStreamFetchDesc->currentSegmentFile.isOpen);
		}

		if (!datumStreamFetchDesc->currentSegmentFile.isOpen)
		{
			if (!openFetchSegmentFile(
					aocsFetchDesc,
					segmentFileNum,
					colno))
			{
				found = false;	// Segment file not in aoseg table..
						   		// Must be aborted or deleted and reclaimed.
				break;
			}

			/* Reset currentBlock info */
			resetCurrentBlockInfo(&(datumStreamFetchDesc->currentBlock));
		}

		/*
		 * Need to get the Block Directory entry that covers the TID.
		 */
		if (!AppendOnlyBlockDirectory_GetEntry(
				&aocsFetchDesc->blockDirectory,
				aoTupleId,
				colno,
				&datumStreamFetchDesc->currentBlock.blockDirectoryEntry))
		{
			found = false;	/* Row not represented in Block Directory. */
				   			/* Must be aborted or deleted and reclaimed. */
			break;
		}

		/*
		 * Set scan range covered by new Block Directory entry.
		 */
		positionFirstBlockOfRange(datumStreamFetchDesc);

		positionLimitToEndOfRange(datumStreamFetchDesc);

		if (!scanToFetchValue(aocsFetchDesc, rowNum, slot, colno))
		{
			found = false;
			break;
		}
	}

	if (found)
	{
		if (slot != NULL)
		{
			TupSetVirtualTupleNValid(slot, colno);
			slot_set_ctid(slot, (ItemPointer)aoTupleId);
		}
	}
	else
	{
		if (slot != NULL)
			slot = ExecClearTuple(slot);
	}

	return found;
}

void
aocs_fetch_finish(AOCSFetchDesc aocsFetchDesc)
{
	int colno;
	Relation relation = aocsFetchDesc->relation;

	Assert(relation != NULL);

    TupleDesc tupdesc = RelationGetDescr(relation);

	Assert(tupdesc != NULL);

	for (colno = 0; colno < tupdesc->natts; colno++)
	{
		DatumStreamFetchDesc datumStreamFetchDesc =
			aocsFetchDesc->datumStreamFetchDesc[colno];
		if (datumStreamFetchDesc != NULL)
		{
			Assert(datumStreamFetchDesc->datumStream != NULL);
			datumstreamread_close_file(datumStreamFetchDesc->datumStream);
			destroy_datumstreamread(datumStreamFetchDesc->datumStream);
			pfree(datumStreamFetchDesc);
		}
	}
	pfree(aocsFetchDesc->datumStreamFetchDesc);

	AppendOnlyBlockDirectory_End_forSearch(&aocsFetchDesc->blockDirectory);

	if (aocsFetchDesc->segmentFileInfo)
	{
		FreeAllAOCSSegFileInfo(aocsFetchDesc->segmentFileInfo, aocsFetchDesc->totalSegfiles);
		pfree(aocsFetchDesc->segmentFileInfo);
		aocsFetchDesc->segmentFileInfo = NULL;
	}

	RelationDecrementReferenceCount(aocsFetchDesc->relation);

	pfree(aocsFetchDesc->segmentFileName);
	pfree(aocsFetchDesc->basepath);

	pfree(aocsFetchDesc->aoEntry);
}
