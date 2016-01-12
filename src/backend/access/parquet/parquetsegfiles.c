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

/*
 * parquetsegfiles.c
 *
 *  Created on: Jul 5, 2013
 *      Author: malili
 */
#include "postgres.h"

#include "funcapi.h"
#include "miscadmin.h"
#include "access/filesplit.h"
#include "access/heapam.h"
#include "access/genam.h"
#include "catalog/pg_type.h"
#include "catalog/pg_proc.h"
#include "catalog/dependency.h"
#include "catalog/indexing.h"
#include "catalog/namespace.h"
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
		Snapshot parquetMetaDataSnapshot, int segno) {

	Relation pg_parquetseg_rel;
	TupleDesc pg_parquetseg_dsc;
	HeapTuple tuple;
	ScanKeyData key[1];
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

	parquetscan = systable_beginscan(pg_parquetseg_rel, indexid, indexOK,
			SnapshotNow, 1, &key[0]);

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
void InsertInitialParquetSegnoEntry(AppendOnlyEntry *aoEntry, int segno) {
	Relation pg_parquetseg_rel;
	Relation pg_parquetseg_idx;
	TupleDesc pg_parquetseg_dsc;
	HeapTuple pg_parquetseg_tuple = NULL;
	int natts = 0;
	bool *nulls;
	Datum *values;

	Assert(aoEntry != NULL);

	if (segno == 0)
	{
		return;
	}

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
				  int64 tuples_added)
{

	LockAcquireResult acquireResult;

	Relation			pg_parquetseg_rel;
	TupleDesc			pg_parquetseg_dsc;
	ScanKeyData			key[1];
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
													/* dontWait */ false);
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

	parquetscan = systable_beginscan(pg_parquetseg_rel, aoEntry->segidxid, TRUE,
			SnapshotNow, 1, &key[0]);

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
ParquetFileSegTotals *GetParquetSegFilesTotals(Relation parentrel, Snapshot parquetMetaDataSnapshot)
{

	Relation		pg_paqseg_rel;
	TupleDesc		pg_paqseg_dsc;
	HeapTuple		tuple;
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

	paqscan = systable_beginscan(pg_paqseg_rel, InvalidOid, FALSE,
			parquetMetaDataSnapshot, 0, NULL);

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
			-1, totalsegs);

	heap_close(pg_parquetseg_rel, AccessShareLock);

	return result;
}

ParquetFileSegInfo **GetAllParquetFileSegInfoWithSegno(Relation parentrel,
		AppendOnlyEntry *aoEntry, Snapshot parquetMetaDataSnapshot,
		int segno, int *totalsegs) {
	Relation pg_parquetseg_rel;

	ParquetFileSegInfo **result;

	pg_parquetseg_rel = heap_open(aoEntry->segrelid, AccessShareLock);

	result = GetAllParquetFileSegInfo_pg_paqseg_rel(RelationGetRelationName(parentrel),
			aoEntry, pg_parquetseg_rel, parquetMetaDataSnapshot,
			segno, totalsegs);

	heap_close(pg_parquetseg_rel, AccessShareLock);

	return result;
		
}

ParquetFileSegInfo **GetAllParquetFileSegInfo_pg_paqseg_rel(
								char *relationName,
								AppendOnlyEntry *aoEntry,
								Relation pg_parquetseg_rel,
								Snapshot parquetMetaDataSnapshot,
								int expectedSegno,
								int *totalsegs)
{
	TupleDesc		pg_parquetseg_dsc;
	HeapTuple		tuple;
	SysScanDesc		parquetscan;
	ScanKeyData		key[2];
	ParquetFileSegInfo		**allseginfo;
	int				seginfo_no, numOfKey=0;
	int				seginfo_slot_no = AO_FILESEGINFO_ARRAY_SIZE;
	Datum			segno,
					eof,
					eof_uncompressed,
					tupcount;
	bool			isNull;

	pg_parquetseg_dsc = RelationGetDescr(pg_parquetseg_rel);

	/* MPP-16407:
	 * Initialize the segment file information array, we first allocate 8 slot for the array,
	 * then array will be dynamically expanded later if necessary.
	 */
	allseginfo = (ParquetFileSegInfo **) palloc0(sizeof(ParquetFileSegInfo*) * seginfo_slot_no);
	seginfo_no = 0;

	if(expectedSegno>=0){
		ScanKeyInit(&key[numOfKey++], (AttrNumber) Anum_pg_parquetseg_segno,
			BTEqualStrategyNumber, F_INT4EQ, Int32GetDatum(expectedSegno));
	}	

	/*
	 * Now get the actual segfile information
	 */
	parquetscan = systable_beginscan(pg_parquetseg_rel, InvalidOid, FALSE,
			parquetMetaDataSnapshot, numOfKey, &key[0]);

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

        	if (Gp_role == GP_ROLE_DISPATCH)
		{
            		GetTupleVisibilitySummary(
								tuple,
								&oneseginfo->tupleVisibilitySummary);
        	}
            
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

		if (Debug_appendonly_print_scan)
			elog(LOG,"Parquet found existing segno %d with eof " INT64_FORMAT " for table '%s' out of total %d segs",
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

	if (leftSegInfo->segno < rightSegInfo->segno)
		return -1;

	if (leftSegInfo->segno > rightSegInfo->segno)
		return 1;

	return 0;
}

/*
 * Get all segment file splits associated with
 * one parquet table.
 */
List *
ParquetGetAllSegFileSplits(AppendOnlyEntry *aoEntry,
						Snapshot parquetMetaDataSnapshot)
{
	Relation pg_parquetseg_rel;
	TupleDesc pg_parquetseg_dsc;
	HeapTuple tuple;
	SysScanDesc parquetscan;
	List *splits = NIL;

	pg_parquetseg_rel = heap_open(aoEntry->segrelid, AccessShareLock);
	pg_parquetseg_dsc = RelationGetDescr(pg_parquetseg_rel);
	parquetscan = systable_beginscan(pg_parquetseg_rel, InvalidOid, FALSE,
								parquetMetaDataSnapshot, 0, NULL);
	while (HeapTupleIsValid (tuple = systable_getnext(parquetscan)))
	{
		FileSplit split = makeNode(FileSplitNode);
		split->segno = DatumGetInt32(fastgetattr(tuple, Anum_pg_parquetseg_segno, pg_parquetseg_dsc, NULL));
		split->offsets = 0;
		split->lengths = (int64)DatumGetFloat8(fastgetattr(tuple, Anum_pg_parquetseg_eof, pg_parquetseg_dsc, NULL));
		split->logiceof = split->lengths;
		splits = lappend(splits, split);
	}

	systable_endscan(parquetscan);
	heap_close(pg_parquetseg_rel, AccessShareLock);

	return splits;
}

void
ParquetFetchSegFileInfo(AppendOnlyEntry *aoEntry, List *segfileinfos, Snapshot parquetMetaDataSnapshot)
{
	Relation pg_parquetseg_rel;
	TupleDesc pg_parquetseg_dsc;
	HeapTuple tuple;
	SysScanDesc parquetscan;

	/*
	 * Since this function is called for insert operation,
	 * here we use RowExclusiveLock.
	 */
	pg_parquetseg_rel = heap_open(aoEntry->segrelid, RowExclusiveLock);
	pg_parquetseg_dsc = RelationGetDescr(pg_parquetseg_rel);
	parquetscan = systable_beginscan(pg_parquetseg_rel, InvalidOid, FALSE,
									parquetMetaDataSnapshot, 0, NULL);

	while (HeapTupleIsValid(tuple = systable_getnext(parquetscan)))
	{
		ListCell *lc;
		int segno = DatumGetInt32(fastgetattr(tuple, Anum_pg_parquetseg_segno, pg_parquetseg_dsc, NULL));
		foreach (lc, segfileinfos)
		{
			ResultRelSegFileInfo *segfileinfo = (ResultRelSegFileInfo *)lfirst(lc);
			Assert(segfileinfo != NULL);
			if (segfileinfo->segno == segno)
			{
				segfileinfo->numfiles = 1;
				segfileinfo->tupcount = (int64)DatumGetFloat8(fastgetattr(tuple, Anum_pg_parquetseg_tupcount, pg_parquetseg_dsc, NULL));
				segfileinfo->varblock = 0;
				segfileinfo->eof = (int64 *)palloc(sizeof(int64));
				segfileinfo->eof[0] = (int64)DatumGetFloat8(fastgetattr(tuple, Anum_pg_parquetseg_eof, pg_parquetseg_dsc, NULL));
				segfileinfo->uncompressed_eof = (int64 *)palloc(sizeof(int64));
				segfileinfo->uncompressed_eof[0] = (int64)DatumGetFloat8(fastgetattr(tuple, Anum_pg_parquetseg_eofuncompressed, pg_parquetseg_dsc, NULL));
				break;
			}
		}
	}

	systable_endscan(parquetscan);
	heap_close(pg_parquetseg_rel, RowExclusiveLock);
	return;
}

Datum
parquet_compression_ration_internal(Relation parentrel)
{
	Relation pg_parquetseg_rel;
	TupleDesc pg_parquetseg_dsc;
	HeapTuple tuple;
	HeapScanDesc parquetscan;
	Datum eof;
	Datum eof_uncompressed;
	float8 total_eof = 0;
	float8 total_eof_uncompressed = 0;
	bool isNull;
	AppendOnlyEntry *aoEntry = NULL;
	float8			compress_ratio = -1; /* the default, meaning "not available" */

	Assert(AmActiveMaster()); /* Make sure this function is called in master */

	aoEntry = GetAppendOnlyEntry(RelationGetRelid(parentrel), SnapshotNow);

	Assert(aoEntry != NULL);

	pg_parquetseg_rel = heap_open(aoEntry->segrelid, AccessShareLock);
	pg_parquetseg_dsc = RelationGetDescr(pg_parquetseg_rel);

	parquetscan = heap_beginscan(pg_parquetseg_rel, SnapshotNow, 0, NULL);

	while (HeapTupleIsValid(tuple = heap_getnext(parquetscan, ForwardScanDirection)))
	{
		eof = fastgetattr(tuple, Anum_pg_parquetseg_eof, pg_parquetseg_dsc, &isNull);
		Assert(!isNull);

		eof_uncompressed = fastgetattr(tuple, Anum_pg_parquetseg_eofuncompressed, pg_parquetseg_dsc, &isNull);
		Assert(!isNull);

		total_eof += DatumGetFloat8(eof);
		total_eof_uncompressed += DatumGetFloat8(eof_uncompressed);

		CHECK_FOR_INTERRUPTS();
	}

	heap_endscan(parquetscan);
	heap_close(pg_parquetseg_rel, AccessShareLock);

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
