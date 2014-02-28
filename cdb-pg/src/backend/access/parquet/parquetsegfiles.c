/*
 * parquetsegfiles.c
 *
 *  Created on: Jul 5, 2013
 *      Author: malili
 */
#include "postgres.h"

#include "funcapi.h"
#include "miscadmin.h"
#include "access/heapam.h"
#include "access/genam.h"
#include "catalog/pg_type.h"
#include "catalog/pg_proc.h"
#include "catalog/dependency.h"
#include "catalog/indexing.h"
#include "catalog/namespace.h"
#include "catalog/gp_fastsequence.h"
#include "catalog/pg_appendonly.h"
#include "cdb/cdbvars.h"
#include "executor/spi.h"
#include "nodes/makefuncs.h"
#include "utils/acl.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/syscache.h"
#include "utils/fmgroids.h"
#include "utils/numeric.h"
#include "access/parquetsegfiles.h"

static int
parquetFileSegInfoCmp(const void *left, const void *right);

/*
 * GetFileSegInfo
 *
 * Get the catalog entry for an appendonly (row-oriented) relation from the
 * pg_aoseg_* relation that belongs to the currently used
 * AppendOnly table.
 *
 * If a caller intends to append to this file segment entry they must
 * already hold a relation Append-Only segment file (transaction-scope) lock (tag
 * LOCKTAG_RELATION_APPENDONLY_SEGMENT_FILE) in order to guarantee
 * stability of the pg_aoseg information on this segment file and exclusive right
 * to append data to the segment file.
 */
ParquetFileSegInfo *
GetParquetFileSegInfo(Relation parentrel, AppendOnlyEntry *aoEntry,
		Snapshot parquetMetaDataSnapshot, int segno, int contentid) {

	Relation pg_parquetseg_rel;
	TupleDesc pg_parquetseg_dsc;
	HeapTuple tuple;
	ScanKeyData key[2];
	SysScanDesc parquetscan;
	Datum eof, eof_uncompressed, tupcount;
	bool isNull;
	bool indexOK;
	Oid indexid;
	ParquetFileSegInfo *fsinfo;

	/*
	 * Check the pg_paqseg relation to be certain the parquet table segment file
	 * is there.
	 */
	pg_parquetseg_rel = heap_open(aoEntry->segrelid, AccessShareLock);
	pg_parquetseg_dsc = RelationGetDescr(pg_parquetseg_rel);

	if (Gp_role == GP_ROLE_EXECUTE) {
		indexOK = FALSE;
		indexid = InvalidOid;
	} else {
		indexOK = TRUE;
		indexid = aoEntry->segidxid;
	}

	/*
	 * Setup a scan key to fetch from the index by segno.
	 */
	ScanKeyInit(&key[0], (AttrNumber) Anum_pg_parquetseg_segno,
			BTEqualStrategyNumber, F_INT4EQ, Int32GetDatum(segno));

	ScanKeyInit(&key[1], (AttrNumber) Anum_pg_parquetseg_content,
			BTEqualStrategyNumber, F_INT4EQ, Int32GetDatum(contentid));

	parquetscan = systable_beginscan(pg_parquetseg_rel, indexid, indexOK,
			SnapshotNow, 2, &key[0]);

	tuple = systable_getnext(parquetscan);

	if (!HeapTupleIsValid(tuple)) {
		/* This segment file does not have an entry. */
		systable_endscan(parquetscan);
		heap_close(pg_parquetseg_rel, AccessShareLock);
		return NULL ;
	}

	tuple = heap_copytuple(tuple);

	systable_endscan(parquetscan);

	Assert(HeapTupleIsValid(tuple));

	fsinfo = (ParquetFileSegInfo *) palloc0(sizeof(ParquetFileSegInfo));

	/* get the eof */
	eof = fastgetattr(tuple, Anum_pg_parquetseg_eof, pg_parquetseg_dsc,
			&isNull);

	if (isNull)
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_OBJECT), errmsg("got invalid eof value: NULL")));

	/* get the tupcount */
	tupcount = fastgetattr(tuple, Anum_pg_parquetseg_tupcount,
			pg_parquetseg_dsc, &isNull);

	if (isNull)
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_OBJECT), errmsg("got invalid tupcount value: NULL")));

	/* get the uncompressed eof */
	eof_uncompressed = fastgetattr(tuple, Anum_pg_parquetseg_eofuncompressed,
			pg_parquetseg_dsc, &isNull);
	/*
	 * Confusing: This eof_uncompressed variable is never used.  It appears we only
	 * call fastgetattr to get the isNull value.  this variable "eof_uncompressed" is
	 * not at all the same as fsinfo->eof_uncompressed.
	 */

	if (isNull) {
		/*
		 * NULL is allowed. Tables that were created before the release of the
		 * eof_uncompressed catalog column will have a NULL instead of a value.
		 */
		fsinfo->eof_uncompressed = InvalidUncompressedEof;
	} else {
		fsinfo->eof_uncompressed = (int64) DatumGetFloat8(eof_uncompressed);
	}

	fsinfo->segno = segno;
	fsinfo->eof = (int64) DatumGetFloat8(eof);
	fsinfo->tupcount = (int64) DatumGetFloat8(tupcount);
	fsinfo->content = GpIdentity.segindex;

	ItemPointerSetInvalid(&fsinfo->sequence_tid);

	if (fsinfo->eof < 0)
		ereport(ERROR,
				(errcode(ERRCODE_GP_INTERNAL_ERROR),
						errmsg("invalid eof " INT64_FORMAT " for relation %s",
								fsinfo->eof,
								RelationGetRelationName(parentrel))));

	/* Finish up scan and close appendonly catalog. */
	heap_close(pg_parquetseg_rel, AccessShareLock);

	return fsinfo;
}

/*
 * InsertFileSegInfo
 *
 * Adds an entry into the pg_paqseg_*  table for this Parquet
 * relation. Use use frozen_heap_insert so the tuple is
 * frozen on insert.
 *
 * Also insert a new entry to gp_fastsequence for this segment file.
 */
void InsertInitialParquetSegnoEntry(AppendOnlyEntry *aoEntry, int segno,
		int contentid) {
	Relation pg_parquetseg_rel;
	Relation pg_parquetseg_idx;
	TupleDesc pg_parquetseg_dsc;
	HeapTuple pg_parquetseg_tuple = NULL;
	int natts = 0;
	bool *nulls;
	Datum *values;
	ItemPointerData tid;

	Assert(aoEntry != NULL);

	InsertFastSequenceEntry(aoEntry->segrelid, (int64) segno, 0, contentid,
			&tid);

	pg_parquetseg_rel = heap_open(aoEntry->segrelid, RowExclusiveLock);

	pg_parquetseg_dsc = RelationGetDescr(pg_parquetseg_rel);
	natts = pg_parquetseg_dsc->natts;
	nulls = palloc(sizeof(bool) * natts);
	values = palloc0(sizeof(Datum) * natts);
	MemSet(nulls, 0, sizeof(char) * natts);

	if (Gp_role != GP_ROLE_EXECUTE)
		pg_parquetseg_idx = index_open(aoEntry->segidxid,
				RowExclusiveLock);
	else
		pg_parquetseg_idx = NULL;

	values[Anum_pg_parquetseg_segno - 1] = Int32GetDatum(segno);
	values[Anum_pg_parquetseg_tupcount - 1] = Float8GetDatum(0);
	values[Anum_pg_parquetseg_eof - 1] = Float8GetDatum(0);
	values[Anum_pg_parquetseg_eofuncompressed - 1] = Float8GetDatum(0);
	values[Anum_pg_parquetseg_content - 1] = Int32GetDatum(contentid);

	/*
	 * form the tuple and insert it
	 */
	pg_parquetseg_tuple = heap_form_tuple(pg_parquetseg_dsc, values, nulls);
	if (!HeapTupleIsValid(pg_parquetseg_tuple))
		elog(ERROR, "failed to build Parquet file segment tuple");

	frozen_heap_insert(pg_parquetseg_rel, pg_parquetseg_tuple);

	if (Gp_role != GP_ROLE_EXECUTE)
		CatalogUpdateIndexes(pg_parquetseg_rel, pg_parquetseg_tuple);

	heap_freetuple(pg_parquetseg_tuple);

	if (Gp_role != GP_ROLE_EXECUTE)
		index_close(pg_parquetseg_idx, RowExclusiveLock);
	heap_close(pg_parquetseg_rel, RowExclusiveLock);
}

/*
 * Update the eof and filetupcount of a parquet table.
 */
void
UpdateParquetFileSegInfo(Relation parentrel,
				  AppendOnlyEntry *aoEntry,
				  int segno,
				  int64 eof,
				  int64 eof_uncompressed,
				  int64 tuples_added,
				  int32 contentid)
{

	LockAcquireResult acquireResult;

	Relation			pg_parquetseg_rel;
	TupleDesc			pg_parquetseg_dsc;
	ScanKeyData			key[2];
	SysScanDesc			parquetscan;
	HeapTuple			tuple, new_tuple;
	Datum				filetupcount;
	Datum				new_tuple_count;
	Datum			   *new_record;
	bool			   *new_record_nulls;
	bool			   *new_record_repl;
	bool				isNull;

	/* overflow sanity checks. don't check the same for tuples_added,
	 * it may be coming as a negative diff from gp_update_ao_master_stats */
	Assert(eof >= 0);

	Insist(Gp_role != GP_ROLE_EXECUTE);

	elog(DEBUG3, "UpdateParquetFileSegInfo called. segno = %d", segno);

	if (Gp_role != GP_ROLE_DISPATCH)
	{
		/*
		 * Verify we already have the write-lock!
		 */
		acquireResult = LockRelationAppendOnlySegmentFile(
													&parentrel->rd_node,
													segno,
													AccessExclusiveLock,
													/* dontWait */ false,
													contentid);
		if (acquireResult != LOCKACQUIRE_ALREADY_HELD)
		{
			elog(ERROR, "Should already have the (transaction-scope) write-lock on Parquet segment file #%d, "
				 "relation %s", segno, RelationGetRelationName(parentrel));
		}
	}

	/*
	 * Open the aoseg relation and its index.
	 */
	pg_parquetseg_rel = heap_open(aoEntry->segrelid, RowExclusiveLock);
	pg_parquetseg_dsc = pg_parquetseg_rel->rd_att;

	/*
	 * Setup a scan key to fetch from the index by segno.
	 */
	ScanKeyInit(&key[0], (AttrNumber) Anum_pg_parquetseg_segno,
			BTEqualStrategyNumber, F_INT4EQ, Int32GetDatum(segno));

	ScanKeyInit(&key[1], (AttrNumber) Anum_pg_parquetseg_content,
			BTEqualStrategyNumber, F_INT4EQ, Int32GetDatum(contentid));

	parquetscan = systable_beginscan(pg_parquetseg_rel, aoEntry->segidxid, TRUE,
			SnapshotNow, 2, &key[0]);

	tuple = systable_getnext(parquetscan);

	if (!HeapTupleIsValid(tuple))
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_OBJECT),
				errmsg("parquet table \"%s\" file segment \"%d\" entry "
						"does not exist", RelationGetRelationName(parentrel),
						segno)));

	new_record = palloc0(sizeof(Datum) * pg_parquetseg_dsc->natts);
	new_record_nulls = palloc0(sizeof(bool) * pg_parquetseg_dsc->natts);
	new_record_repl = palloc0(sizeof(bool) * pg_parquetseg_dsc->natts);

	/* get the current tuple count so we can add to it */
	filetupcount = fastgetattr(tuple,
								Anum_pg_parquetseg_tupcount,
								pg_parquetseg_dsc,
								&isNull);

	if(isNull)
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_OBJECT),
				errmsg("got invalid pg_aoseg filetupcount value: NULL")));

	/* calculate the new tuple count */
	new_tuple_count = DirectFunctionCall2(float8pl,
										  filetupcount,
										  Float8GetDatum((float8)tuples_added));

	/*
	 * Build a tuple to update
	 */
	new_record[Anum_pg_parquetseg_eof - 1] = Float8GetDatum((float8)eof);
	new_record_repl[Anum_pg_parquetseg_eof - 1] = true;

	new_record[Anum_pg_parquetseg_tupcount - 1] = new_tuple_count;
	new_record_repl[Anum_pg_parquetseg_tupcount - 1] = true;

	new_record[Anum_pg_parquetseg_eofuncompressed - 1] = Float8GetDatum((float8)eof_uncompressed);
	new_record_repl[Anum_pg_parquetseg_eofuncompressed - 1] = true;

	/*
	 * update the tuple in the pg_aoseg table
	 */
	new_tuple = heap_modify_tuple(tuple, pg_parquetseg_dsc, new_record,
								new_record_nulls, new_record_repl);

	simple_heap_update(pg_parquetseg_rel, &tuple->t_self, new_tuple);

	CatalogUpdateIndexes(pg_parquetseg_rel, new_tuple);

	heap_freetuple(new_tuple);

	/* Finish up scan */
	systable_endscan(parquetscan);
	heap_close(pg_parquetseg_rel, RowExclusiveLock);

	pfree(new_record);
	pfree(new_record_nulls);
	pfree(new_record_repl);
}

/*
 * GetSegFilesTotals
 *
 * Get the total bytes and tuples for a specific parquet table
 * from the pg_aoseg table on this local segdb.
 */
ParquetFileSegTotals *GetParquetSegFilesTotals(Relation parentrel, Snapshot parquetMetaDataSnapshot, int32 contentid)
{

	Relation		pg_paqseg_rel;
	TupleDesc		pg_paqseg_dsc;
	HeapTuple		tuple;
	ScanKeyData		key[2];
	SysScanDesc		paqscan;
	ParquetFileSegTotals  *result;
	Datum			eof,
					eof_uncompressed,
					tupcount;
	bool			isNull;
	AppendOnlyEntry *aoEntry = NULL;

	Assert(RelationIsParquet(parentrel));

	aoEntry = GetAppendOnlyEntry(RelationGetRelid(parentrel), parquetMetaDataSnapshot);

	result = (ParquetFileSegTotals *) palloc0(sizeof(ParquetFileSegTotals));

	pg_paqseg_rel = heap_open(aoEntry->segrelid, AccessShareLock);
	pg_paqseg_dsc = RelationGetDescr(pg_paqseg_rel);

	ScanKeyInit(&key[0], (AttrNumber) Anum_pg_parquetseg_content,
			BTEqualStrategyNumber, F_INT4EQ, Int32GetDatum(contentid));

	paqscan = systable_beginscan(pg_paqseg_rel, InvalidOid, FALSE,
			parquetMetaDataSnapshot, 1, &key[0]);

	while (HeapTupleIsValid(tuple = systable_getnext(paqscan)))
	{
		eof = fastgetattr(tuple, Anum_pg_parquetseg_eof, pg_paqseg_dsc, &isNull);
		tupcount = fastgetattr(tuple, Anum_pg_parquetseg_tupcount, pg_paqseg_dsc, &isNull);
		eof_uncompressed = fastgetattr(tuple, Anum_pg_parquetseg_eofuncompressed, pg_paqseg_dsc, &isNull);

		if(isNull)
			result->totalbytesuncompressed = InvalidUncompressedEof;
		else
			result->totalbytesuncompressed += (int64)DatumGetFloat8(eof_uncompressed);

		result->totalbytes += (int64)DatumGetFloat8(eof);
		result->totaltuples += (int64)DatumGetFloat8(tupcount);
		result->totalfilesegs++;

		CHECK_FOR_INTERRUPTS();
	}

	systable_endscan(paqscan);
	heap_close(pg_paqseg_rel, AccessShareLock);

	pfree(aoEntry);

	return result;
}


/*
 * GetParquetTotalBytes
 *
 * Get the total bytes for a specific parquet table from the pg_aoseg table on master.
 *
 * In hawq, master keep all segfile info in pg_aoseg table,
 * therefore it get the whole table size.
 */
int64 GetParquetTotalBytes(Relation parentrel, Snapshot parquetMetaDataSnapshot)
{

	Relation		pg_paqseg_rel;
	TupleDesc		pg_paqseg_dsc;
	HeapTuple		tuple;
	SysScanDesc		parquetscan;
	int64		  	result;
	Datum			eof;
	bool			isNull;
	AppendOnlyEntry 	*aoEntry = NULL;

	aoEntry = GetAppendOnlyEntry(RelationGetRelid(parentrel), parquetMetaDataSnapshot);

	result = 0;

	pg_paqseg_rel = heap_open(aoEntry->segrelid, AccessShareLock);
	pg_paqseg_dsc = RelationGetDescr(pg_paqseg_rel);

	Assert (Gp_role != GP_ROLE_EXECUTE);

	parquetscan = systable_beginscan(pg_paqseg_rel, InvalidOid, FALSE,
			parquetMetaDataSnapshot, 0, NULL);

	while (HeapTupleIsValid(tuple = systable_getnext(parquetscan)))
	{
		eof = fastgetattr(tuple, Anum_pg_parquetseg_eof, pg_paqseg_dsc, &isNull);
		Assert(!isNull);

		result += (int64)DatumGetFloat8(eof);

		CHECK_FOR_INTERRUPTS();
	}

	systable_endscan(parquetscan);
	heap_close(pg_paqseg_rel, AccessShareLock);

	pfree(aoEntry);

	return result;
}

/*
 * GetAllFileSegInfo
 *
 * Get all catalog entries for an appendonly relation from the
 * pg_aoseg_* relation that belongs to the currently used
 * AppendOnly table. This is basically a physical snapshot that a
 * scanner can use to scan all the data in a local segment database.
 */
ParquetFileSegInfo **GetAllParquetFileSegInfo(Relation parentrel,
		AppendOnlyEntry *aoEntry, Snapshot parquetMetaDataSnapshot,
		int *totalsegs) {
	Relation pg_parquetseg_rel;

	ParquetFileSegInfo **result;

	pg_parquetseg_rel = heap_open(aoEntry->segrelid, AccessShareLock);

	result = GetAllParquetFileSegInfo_pg_paqseg_rel(RelationGetRelationName(parentrel),
			aoEntry, pg_parquetseg_rel, parquetMetaDataSnapshot,
			/* Accessing the files belong to this segment */false, totalsegs);

	heap_close(pg_parquetseg_rel, AccessShareLock);

	return result;
}

ParquetFileSegInfo **GetAllParquetFileSegInfo_pg_paqseg_rel(
								char *relationName,
								AppendOnlyEntry *aoEntry,
								Relation pg_parquetseg_rel,
								Snapshot parquetMetaDataSnapshot,
								bool returnAllSegmentsFiles,
								int *totalsegs)
{
	TupleDesc		pg_parquetseg_dsc;
	HeapTuple		tuple;
	SysScanDesc		parquetscan;
	ScanKeyData		key[1];
	ParquetFileSegInfo		**allseginfo;
	int				seginfo_no;
	int				seginfo_slot_no = AO_FILESEGINFO_ARRAY_SIZE;
	Datum			segno,
					eof,
					eof_uncompressed,
					tupcount,
					content;
	bool			isNull;

	pg_parquetseg_dsc = RelationGetDescr(pg_parquetseg_rel);

	/* MPP-16407:
	 * Initialize the segment file information array, we first allocate 8 slot for the array,
	 * then array will be dynamically expanded later if necessary.
	 */
	allseginfo = (ParquetFileSegInfo **) palloc0(sizeof(ParquetFileSegInfo*) * seginfo_slot_no);
	seginfo_no = 0;

	ScanKeyInit(&key[0], (AttrNumber) Anum_pg_parquetseg_content,
			BTEqualStrategyNumber, F_INT4EQ, Int32GetDatum(GpIdentity.segindex));

	/*
	 * Now get the actual segfile information
	 */
	if (returnAllSegmentsFiles)
		parquetscan = systable_beginscan(pg_parquetseg_rel, InvalidOid, FALSE,
				parquetMetaDataSnapshot, 0, NULL);
	else
		parquetscan = systable_beginscan(pg_parquetseg_rel, InvalidOid, FALSE,
				parquetMetaDataSnapshot, 1, &key[0]);

	while (HeapTupleIsValid(tuple = systable_getnext(parquetscan)))
	{
		/* dynamically expand space for FileSegInfo* array */
		if (seginfo_no >= seginfo_slot_no)
		{
			seginfo_slot_no *= 2;
			allseginfo = (ParquetFileSegInfo **) repalloc(allseginfo, sizeof(ParquetFileSegInfo*) * seginfo_slot_no);
		}

		ParquetFileSegInfo *oneseginfo;

		allseginfo[seginfo_no] = (ParquetFileSegInfo *)palloc0(sizeof(ParquetFileSegInfo));
		oneseginfo = allseginfo[seginfo_no];

		GetTupleVisibilitySummary(
								tuple,
								&oneseginfo->tupleVisibilitySummary);

		segno = fastgetattr(tuple, Anum_pg_parquetseg_segno, pg_parquetseg_dsc, &isNull);
		oneseginfo->segno = DatumGetInt32(segno);

		eof = fastgetattr(tuple, Anum_pg_parquetseg_eof, pg_parquetseg_dsc, &isNull);
		oneseginfo->eof = (int64)DatumGetFloat8(eof);

		tupcount = fastgetattr(tuple, Anum_pg_parquetseg_tupcount, pg_parquetseg_dsc, &isNull);
		oneseginfo->tupcount = (int64)DatumGetFloat8(tupcount);

		ItemPointerSetInvalid(&oneseginfo->sequence_tid);

		eof_uncompressed = fastgetattr(tuple, Anum_pg_parquetseg_eofuncompressed, pg_parquetseg_dsc, &isNull);

		if(isNull)
			oneseginfo->eof_uncompressed = InvalidUncompressedEof;
		else
			oneseginfo->eof_uncompressed = (int64)DatumGetFloat8(eof);

		content = fastgetattr(tuple, Anum_pg_parquetseg_content, pg_parquetseg_dsc, &isNull);
		oneseginfo->content = DatumGetInt32(content);

		if (Debug_appendonly_print_scan)
			elog(LOG,"Parquet found existing contentid %d segno %d with eof " INT64_FORMAT " for table '%s' out of total %d segs",
				oneseginfo->content,
				oneseginfo->segno,
				oneseginfo->eof,
				relationName,
				*totalsegs);

		seginfo_no++;

		CHECK_FOR_INTERRUPTS();
	}

	systable_endscan(parquetscan);

	*totalsegs = seginfo_no;

	if (*totalsegs == 0)
	{
		pfree(allseginfo);
		return NULL;
	}

	/*
	 * Sort allseginfo by the order of segment file number.
	 *
	 * Currently this is only needed when building a bitmap index to guarantee the tids
	 * are in the ascending order. But since this array is pretty small, we just sort
	 * the array for all cases.
	 */
	qsort((char *)allseginfo, *totalsegs, sizeof(ParquetFileSegInfo *), parquetFileSegInfoCmp);

	return allseginfo;
}

/*
 * The comparison routine that sorts an array of FileSegInfos
 * in the ascending order of the segment number.
 */
static int
parquetFileSegInfoCmp(const void *left, const void *right)
{
	ParquetFileSegInfo *leftSegInfo = *((ParquetFileSegInfo **)left);
	ParquetFileSegInfo *rightSegInfo = *((ParquetFileSegInfo **)right);

	if (leftSegInfo->content < rightSegInfo->content)
		return -1;

	if (leftSegInfo->content > rightSegInfo->content)
		return 1;

	if (leftSegInfo->segno < rightSegInfo->segno)
		return -1;

	if (leftSegInfo->segno > rightSegInfo->segno)
		return 1;

	return 0;
}
