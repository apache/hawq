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
 * parquetam.c
 *
 *  Created on: Jul 4, 2013
 *      Author: malili
 */

#include "postgres.h"
#include <fcntl.h>
#include "access/tupdesc.h"
#include "access/memtup.h"
#include "access/filesplit.h"
#include "access/sdir.h"
#include "utils/relcache.h"
#include "utils/tqual.h"
#include "utils/rel.h"
#include "utils/faultinjector.h"
#include "access/aomd.h"
#include "cdb/cdbvars.h"
#include "cdb/cdbparquetam.h"
#include "cdb/cdbparquetstoragewrite.h"
#include "catalog/pg_attribute_encoding.h"
#include "catalog/catquery.h"
#include "utils/lsyscache.h"
#include "utils/builtins.h"
#include "catalog/pg_statistic.h"
#include "cdb/cdbparquetfooterbuffer.h"
#include "cdb/cdbparquetfooterserializer.h"

/**For read*/
static void initscan(ParquetScanDesc scan);

/*get next segment file for read*/
static bool SetNextFileSegForRead(ParquetScanDesc scan);

/*get next row group to read*/
bool getNextRowGroup(ParquetScanDesc scan);

/*close current scanning file segments*/
static void CloseScannedFileSeg(ParquetScanDesc scan);

static bool ValidateParquetSegmentFile(
		TupleDesc hawqTupDescs,
		int *hawqAttrToParquetColChunks,
		ParquetMetadata parquetMetadata);

static int getColumnChunkNumForHawqAttr(struct FileField_4C* hawqAttr);


/*For write*/
static void SetCurrentFileSegForWrite(
		ParquetInsertDesc parquetInsertDesc,
		ResultRelSegFileInfo *segfileinfo);

static void OpenSegmentFile(
		MirroredAppendOnlyOpen *mirroredOpen,
		char *filePathName,
		int64 logicalEof,
		RelFileNode *relFileNode,
		int32 segmentFileNum,
		char *relname,
		File *parquet_file,
		File *parquet_file_previous,
		CompactProtocol **protocol_read,
		TupleDesc tableAttrs,
		ParquetMetadata *parquetMetadata,
		int64 *fileLen,
		int64 *fileLen_uncompressed,
		int *previous_rowgroupcnt);

static void CloseWritableFileSeg(ParquetInsertDesc parquetInsertDesc);

static void freeParquetInsertDesc(ParquetInsertDesc parquetInsertDesc);

static void TransactionFlushAndCloseFile(
		ParquetInsertDesc	parquetInsertDesc,
		int64				*newLogicalEof, /* The new EOF for the segment file. */
		int64				*fileLen_uncompressed);


/* ----------------
 *		initscan - scan code common to parquet_beginscan and parquet_rescan
 * ----------------
 */
static void initscan(ParquetScanDesc scan)
{
	scan->pqs_filenamepath[0] = '\0';
	scan->pqs_splits_processed = 0;
	scan->pqs_need_new_split = true; /* need to assign a file to be scanned */
	scan->pqs_done_all_splits = false;
	scan->bufferDone = true;
	scan->initedStorageRoutines = false;

	ItemPointerSet(&scan->cdb_fake_ctid, 0, 0);
	scan->cur_seg_row = 0;

	pgstat_count_heap_scan(scan->pqs_rd);
}

/**
 *begin scanning of a parquet relation
 */
ParquetScanDesc
parquet_beginscan(
		Relation relation,
		Snapshot parquetMetaDataSnapshot,
		TupleDesc relationTupleDesc,
		bool *proj)
{
	ParquetScanDesc 			scan;
	AppendOnlyEntry				*aoEntry;

	AppendOnlyStorageAttributes	*attr;

	/*
	 * increment relation ref count while scanning relation
	 *
	 * This is just to make really sure the relcache entry won't go away while
	 * the scan has a pointer to it.  Caller should be holding the rel open
	 * anyway, so this is redundant in all normal scenarios...
	 */
	RelationIncrementReferenceCount(relation);

	/* allocate scan descriptor */
	scan = (ParquetScanDescData *)palloc0(sizeof(ParquetScanDescData));

	/*
	 * Get the pg_appendonly information for this table
	 */
	aoEntry = GetAppendOnlyEntry(RelationGetRelid(relation), parquetMetaDataSnapshot);
	scan->aoEntry = aoEntry;
	Assert(aoEntry->majorversion == 1 && aoEntry->minorversion == 0);

#ifdef FAULT_INJECTOR
				FaultInjector_InjectFaultIfSet(
											   FailQeWhenBeginParquetScan,
											   DDLNotSpecified,
											   "",	// databaseName
											   ""); // tableName
#endif

	/*
	 * initialize the scan descriptor
	 */
	scan->pqs_filenamepath_maxlen = AOSegmentFilePathNameLen(relation) + 1;
	scan->pqs_filenamepath = (char*)palloc0(scan->pqs_filenamepath_maxlen);
	scan->pqs_rd = relation;
	scan->parquetScanInitContext = CurrentMemoryContext;

	/*
	 * Fill in Parquet Storage layer attributes.
	 */
	attr = &scan->storageAttributes;

	/*
	 * These attributes describe the AppendOnly format to be scanned.
	 */
	if (aoEntry->compresstype == NULL || pg_strcasecmp(aoEntry->compresstype, "none") == 0)
		attr->compress = false;
	else
		attr->compress = true;
	if (aoEntry->compresstype != NULL)
		attr->compressType = aoEntry->compresstype;
	else
		attr->compressType = "none";
	attr->compressLevel     = aoEntry->compresslevel;
	attr->checksum			= aoEntry->checksum;
	attr->safeFSWriteSize	= aoEntry->safefswritesize;
	attr->splitsize = aoEntry->splitsize;
	attr->version			= aoEntry->version;


	AORelationVersion_CheckValid(attr->version);

	scan->proj = proj;

	scan->pqs_tupDesc = (relationTupleDesc == NULL) ? RelationGetDescr(relation) : relationTupleDesc;

	scan->hawqAttrToParquetColChunks = (int*)palloc0(scan->pqs_tupDesc->natts * sizeof(int));

	initscan(scan);

	return scan ;
}

void parquet_rescan(ParquetScanDesc scan) {
	CloseScannedFileSeg(scan);
	scan->initedStorageRoutines = false;
	ParquetRowGroupReader_FinishedScanRowGroup(&(scan->rowGroupReader));
	ParquetStorageRead_FinishSession(&(scan->storageRead));
	initscan(scan);
}

void parquet_endscan(ParquetScanDesc scan) {
	ParquetRowGroupReader rowGroupReader = scan->rowGroupReader;

	RelationDecrementReferenceCount(scan->pqs_rd);

	CloseScannedFileSeg(scan);

	MemoryContext oldContext = MemoryContextSwitchTo(scan->parquetScanInitContext);

	/* Free the column readers information*/
	if(rowGroupReader.columnReaders != NULL)
	{
		for (int i = 0; i < rowGroupReader.columnReaderCount; ++i)
		{
			ParquetColumnReader *reader = &rowGroupReader.columnReaders[i];

			if (reader->dataBuffer != NULL)
			{
				pfree(reader->dataBuffer);
			}

			if (reader->pageBuffer != NULL)
			{
				pfree(reader->pageBuffer);
			}

			if (reader->geoval != NULL)
			{
				pfree(reader->geoval);
			}
			if (reader->columnMetadata != NULL) {
				pfree(reader->columnMetadata);
			}
		}
		pfree(rowGroupReader.columnReaders);
	}

	if(scan->hawqAttrToParquetColChunks != NULL){
		pfree(scan->hawqAttrToParquetColChunks);
	}

	if(scan->aoEntry != NULL){
		pfree(scan->aoEntry);
	}

	ParquetStorageRead_FinishSession(&(scan->storageRead));

	MemoryContextSwitchTo(oldContext);
}

void parquet_getnext(ParquetScanDesc scan, ScanDirection direction,
		TupleTableSlot *slot) {

	AOTupleId aoTupleId;
	Assert(ScanDirectionIsForward(direction));

#ifdef FAULT_INJECTOR
				FaultInjector_InjectFaultIfSet(
											   FailQeWhenParquetGetNext,
											   DDLNotSpecified,
											   "",	// databaseName
											   ""); // tableName
#endif

	for(;;)
	{
		if(scan->bufferDone)
		{
			/*
			 * Get the next row group. We call this function until we
			 * successfully get a block to process, or finished reading
			 * all the data (all 'segment' files) for this relation.
			 */
			while(!getNextRowGroup(scan))
			{
				/* have we read all this relation's data. done! */
				if(scan->pqs_done_all_splits)
				{
					ExecClearTuple(slot);
					return /*NULL*/;
				}
			}

			scan->bufferDone = false;
		}

		bool tupleExist = ParquetRowGroupReader_ScanNextTuple(
												scan->pqs_tupDesc,
												&scan->rowGroupReader,
												scan->hawqAttrToParquetColChunks,
												scan->proj,
												slot);

		if(tupleExist)
		{
			int segno = ((FileSplitNode *)list_nth(scan->splits, scan->pqs_splits_processed - 1))->segno;
			AOTupleIdInit_Init(&aoTupleId);
			AOTupleIdInit_segmentFileNum(&aoTupleId,
										 segno);

			scan->cur_seg_row++;
			AOTupleIdInit_rowNum(&aoTupleId, scan->cur_seg_row);

			scan->cdb_fake_ctid = *((ItemPointer)&aoTupleId);

			slot_set_ctid(slot, &(scan->cdb_fake_ctid));

			return;
		}
		/* no more items in the row group, get new buffer */
		scan->bufferDone = true;
	}
}

/*
 * You can think of this scan routine as get next "executor" Parquet rowGroup.
 */
bool getNextRowGroup(ParquetScanDesc scan)
{
	if (scan->pqs_need_new_split)
	{
		/*
		 * Need to open a new segment file.
		 */
		if(!SetNextFileSegForRead(scan))
			return false;

		scan->cur_seg_row = 0;
	}

	FileSplit split = (FileSplitNode *)list_nth(scan->splits, scan->pqs_splits_processed-1);

	if (ParquetRowGroupReader_GetRowGroupInfo(
												split,
												&scan->storageRead,
												&scan->rowGroupReader,
												scan->proj,
												scan->pqs_tupDesc,
												scan->hawqAttrToParquetColChunks,
												scan->toCloseFile)) {
		ParquetRowGroupReader_GetContents(&scan->rowGroupReader);
	}
	else {
		/* current split is finished */
		scan->pqs_need_new_split = true;
		/* if done with reading the segment file */
		if (scan->toCloseFile) {
			CloseScannedFileSeg(scan);
			return false;
		}
	}

	return true;
}

/*
 * Finished scanning this file segment. Close it.
 */
static void
CloseScannedFileSeg(ParquetScanDesc scan)
{
	ParquetStorageRead_CloseFile(&scan->storageRead);

	scan->pqs_need_new_split = true;
}

/*
 * Open the next file segment to scan and allocate all resources needed for it.
 */
static bool
SetNextFileSegForRead(ParquetScanDesc scan)
{
	Relation		reln = scan->pqs_rd;
	int				segno = -1;
	int64			eof = 0;
	bool			finished_all_splits = true; /* assume */
	int32			fileSegNo;
	bool			parquetMetadataCorrect;
	bool			toOpenFile = true; // by default need to open segment file to read

	Assert(scan->pqs_need_new_split);   /* only call me when last segfile completed */
	Assert(!scan->pqs_done_all_splits); /* don't call me if I told you to stop */

	/*
	 * There is no guarantee that the current memory context will be preserved between calls,
	 * so switch to a safe memory context for retrieving compression information.
	 */
	MemoryContext oldMemoryContext = MemoryContextSwitchTo(scan->parquetScanInitContext);
	if (!scan->initedStorageRoutines)
	{
		ParquetStorageRead_Init(
							&scan->storageRead,
							scan->parquetScanInitContext,
							NameStr(scan->pqs_rd->rd_rel->relname),
							&scan->storageAttributes);

		ParquetRowGroupReader_Init(
							&scan->rowGroupReader,
							scan->pqs_rd,
							&scan->storageRead);

		scan->bufferDone = true; /* so we read a new buffer right away */

		scan->initedStorageRoutines = true;
	}

	/*
	 * Do we have more segment files to read or are we done?
	 */
	while (scan->pqs_splits_processed < list_length(scan->splits)) {
	    /* still have more segment files to read. get info of the next one */
	    FileSplit split =
	        (FileSplitNode *)list_nth(scan->splits, scan->pqs_splits_processed);

	    /* For splits within the same segment file, no need to reopen file */
	    if (scan->pqs_splits_processed > 0) {
	      FileSplit lastSplit = (FileSplitNode *)list_nth(
	          scan->splits, scan->pqs_splits_processed - 1);
	      if (split->segno == lastSplit->segno) {
	    	  /*
	    	   * if all rowgroups already processed, omit the remaining splits
	    	   */
	    	  if (scan->storageRead.rowGroupCount == scan->storageRead.rowGroupProcessedCount) {
	    		scan->pqs_splits_processed++;
	    		continue;
	        } else {
	        	toOpenFile = false;
	        }
	      }
	    }

	    scan->toCloseFile = true;
	    if (scan->pqs_splits_processed + 1 < list_length(scan->splits)) {
	      FileSplit nextSplit = (FileSplitNode *)list_nth(
	          scan->splits, scan->pqs_splits_processed + 1);
	      if (split->segno == nextSplit->segno)
	        scan->toCloseFile = false;
	    }

	    segno = split->segno;
		eof = split->logiceof;

		scan->pqs_splits_processed++;

		/*
		 * special case: we are the QD reading from a parquet table in utility mode
		 * (gp_dump). We see entries in the parquetseg table but no files or data
		 * actually exist. If we try to open this file we'll get an error, so
		 * we must skip to the next. For now, we can test if the file exists by
		 * looking at the eof value - it's always 0 on the QD.
		 */
		if(eof > 0)
		{
			finished_all_splits = false;
			break;
		}
	}

	if(finished_all_splits)
	{
		/* finished reading all segment files */
		scan->pqs_need_new_split = false;
		scan->pqs_done_all_splits = true;
		return false;
	}

	MakeAOSegmentFileName(reln, segno, -1, &fileSegNo, scan->pqs_filenamepath);
	Assert(strlen(scan->pqs_filenamepath) + 1 <= scan->pqs_filenamepath_maxlen);

	Assert(scan->initedStorageRoutines);

	if (toOpenFile) {
		/**need open files here*/
		ParquetStorageRead_OpenFile(
							&scan->storageRead,
							scan->pqs_filenamepath,
							eof,
							scan->pqs_rd->rd_att);

		parquetMetadataCorrect = ValidateParquetSegmentFile(scan->pqs_tupDesc,
				scan->hawqAttrToParquetColChunks, scan->storageRead.parquetMetadata);

		if(!parquetMetadataCorrect){
			elog(ERROR, "parquet metadata information conflicts with hawq table information");
		}
	}

	scan->pqs_need_new_split = false;

	if (Debug_appendonly_print_scan)
		elog(LOG,"Parquet scan initialize for table '%s', %u/%u/%u, segment file %u, EOF " INT64_FORMAT ", "
			 "(compression = %s)",
			 NameStr(scan->pqs_rd->rd_rel->relname),
			 scan->pqs_rd->rd_node.spcNode,
			 scan->pqs_rd->rd_node.dbNode,
			 scan->pqs_rd->rd_node.relNode,
			 segno,
			 eof,
			 (scan->storageAttributes.compress ? "true" : "false"));

	/* Switch back to caller's memory context. */
	MemoryContextSwitchTo(oldMemoryContext);

	return true;
}

/**
 * Validate the correctness of parquet file, judge whether it corresponds to hawq table
 * @projs			the projection of the scan
 * @hawqTupDescs	the tuple description of hawq table
 * @parquetMetadata	parquet metadata read from parquet file
 */
static bool ValidateParquetSegmentFile(TupleDesc hawqTupDescs,
		int *hawqAttrToParquetColChunks, ParquetMetadata parquetMetadata)
{
	int numHawqAttrs = hawqTupDescs->natts;
	if(numHawqAttrs != parquetMetadata->fieldCount){
		return false;
	}

	for(int i = 0; i < numHawqAttrs; i++){
		hawqAttrToParquetColChunks[i] = getColumnChunkNumForHawqAttr(&(parquetMetadata->pfield[i]));
	}

	return true;
}

/**
 * get parquet column chunks number for a hawq attribute
 */
static int getColumnChunkNumForHawqAttr(struct FileField_4C* hawqAttr){
	int resultNum = 0;
	if(hawqAttr->num_children == 0){
		return 1;
	}
	else{
		for(int i = 0; i < hawqAttr->num_children; i++){
			resultNum += getColumnChunkNumForHawqAttr(&(hawqAttr->children[i]));
		}
		return resultNum;
	}
}

/**
 * Initialize and get ready for inserting values into the parquet table. If the 
 * parquet insert descriptor is not created, NULL is returned.
 *
 * @rel				the relation to insert
 * @segno			the segment number
 */
ParquetInsertDesc parquet_insert_init(Relation rel, ResultRelSegFileInfo *segfileinfo) {
	ParquetInsertDesc 	parquetInsertDesc 	= NULL;
	AppendOnlyEntry 	*aoentry 		= NULL;
	MemoryContext 		oldMemoryContext 	= NULL;
	StringInfoData 		titleBuf;
	int 				relNameLen 			= 0;

	/*
	 * Get the pg_appendonly information for this table
	 */
	aoentry = GetAppendOnlyEntry(RelationGetRelid(rel), SnapshotNow);
   
    /* The parquet entry must exist and is at right version. The version number
       is hard coded when the entry is created. */ 
    Assert(aoentry != NULL);
	Assert(aoentry->majorversion == 1 && aoentry->minorversion == 0);

	parquetInsertDesc = (ParquetInsertDesc) palloc0(sizeof(ParquetInsertDescData));

	parquetInsertDesc->memoryContext = CurrentMemoryContext;
	oldMemoryContext = MemoryContextSwitchTo(parquetInsertDesc->memoryContext);

	parquetInsertDesc->parquet_rel = rel;
	relNameLen = strlen(rel->rd_rel->relname.data);
	parquetInsertDesc->relname = (char*)palloc0(relNameLen + 1);
	memcpy(parquetInsertDesc->relname, rel->rd_rel->relname.data, relNameLen);
	parquetInsertDesc->parquetMetaDataSnapshot = SnapshotNow;

	parquetInsertDesc->parquet_file = -1;
	parquetInsertDesc->parquetFilePathNameMaxLen = AOSegmentFilePathNameLen(rel) + 1;
	parquetInsertDesc->parquetFilePathName =
			(char*) palloc0(parquetInsertDesc->parquetFilePathNameMaxLen);
	parquetInsertDesc->parquetFilePathName[0] = '\0';
	parquetInsertDesc->footerProtocol = NULL;

	Assert(segfileinfo->segno >= 0);
	parquetInsertDesc->cur_segno = segfileinfo->segno;
	parquetInsertDesc->aoEntry = aoentry;
	parquetInsertDesc->insertCount = 0;

	initStringInfo(&titleBuf);
	appendStringInfo(&titleBuf, "Write of Parquet relation '%s'",
			RelationGetRelationName(parquetInsertDesc->parquet_rel));
	parquetInsertDesc->title = titleBuf.data;

	parquetInsertDesc->mirroredOpen = (MirroredAppendOnlyOpen *)
			palloc0(sizeof(MirroredAppendOnlyOpen));
	parquetInsertDesc->mirroredOpen->isActive = FALSE;
	parquetInsertDesc->mirroredOpen->segmentFileNum = 0;
	parquetInsertDesc->mirroredOpen->primaryFile = -1;
	parquetInsertDesc->previous_rowgroupcnt = 0;

	/* open our current relation file segment for write */
	SetCurrentFileSegForWrite(parquetInsertDesc, segfileinfo);

	/* Allocation is done.	Go back to caller memory-context. */
	MemoryContextSwitchTo(oldMemoryContext);

	return parquetInsertDesc;
}


Oid parquet_insert(ParquetInsertDesc parquetInsertDesc, TupleTableSlot *slot)
{
	Oid oid;
	AOTupleId aotid;

	slot_getallattrs(slot);
	oid = parquet_insert_values(parquetInsertDesc, slot_get_values(slot), slot_get_isnull(slot), &aotid);
	slot_set_ctid(slot, (ItemPointer)&aotid);

	return oid;
}


Oid parquet_insert_values(ParquetInsertDesc parquetInsertDesc,
						  Datum *values, bool *nulls,
						  AOTupleId *aoTupleId)
{
	ParquetRowGroup rowgroup;
	MemoryContext oldMemoryContext;

	oldMemoryContext = MemoryContextSwitchTo(parquetInsertDesc->memoryContext);

	if (parquetInsertDesc->current_rowGroup == NULL)	/* TODO maybe create in insert_init phase */
	{
		parquetInsertDesc->current_rowGroup = addRowGroup(parquetInsertDesc->parquetMetadata,
														  parquetInsertDesc->parquet_rel->rd_att,
														  parquetInsertDesc->aoEntry,
														  parquetInsertDesc->parquet_file);
	}

	rowgroup = parquetInsertDesc->current_rowGroup;
	if (rowgroup->rowGroupMetadata->totalByteSize >= rowgroup->catalog->blocksize)
	{
		flushRowGroup(rowgroup,
					  parquetInsertDesc->parquetMetadata,
					  parquetInsertDesc->mirroredOpen,
					  parquetInsertDesc->footerProtocol,
					  &parquetInsertDesc->fileLen,
					  &parquetInsertDesc->fileLen_uncompressed);

		rowgroup = addRowGroup(parquetInsertDesc->parquetMetadata,
							   parquetInsertDesc->parquet_rel->rd_att,
							   parquetInsertDesc->aoEntry,
							   parquetInsertDesc->parquet_file);

		parquetInsertDesc->current_rowGroup = rowgroup;
	}

	Assert(parquetInsertDesc->parquet_file == rowgroup->parquetFile);

	appendRowValue(rowgroup,
				   parquetInsertDesc->parquetMetadata,
				   values, nulls);

	parquetInsertDesc->insertCount++;
	parquetInsertDesc->rowCount++;

	/* Allocation is done.	Go back to caller memory-context. */
	MemoryContextSwitchTo(oldMemoryContext);

	AOTupleIdInit_Init(aoTupleId);
	AOTupleIdInit_segmentFileNum(aoTupleId, parquetInsertDesc->cur_segno);
	AOTupleIdInit_rowNum(aoTupleId, parquetInsertDesc->rowCount);

	return InvalidOid;
}

void parquet_insert_finish(ParquetInsertDesc parquetInsertDesc) {
	MemoryContext oldMemoryContext;

	oldMemoryContext = MemoryContextSwitchTo(parquetInsertDesc->memoryContext);

	flushRowGroup(parquetInsertDesc->current_rowGroup,
				  parquetInsertDesc->parquetMetadata,
				  parquetInsertDesc->mirroredOpen,
				  parquetInsertDesc->footerProtocol,
				  &parquetInsertDesc->fileLen,
				  &parquetInsertDesc->fileLen_uncompressed);

	/*should add row group row number*/
	writeParquetFooter(parquetInsertDesc->parquet_file,
			parquetInsertDesc->parquetFilePathName,
			parquetInsertDesc->parquetMetadata, &parquetInsertDesc->fileLen,
			&parquetInsertDesc->fileLen_uncompressed,
			&(parquetInsertDesc->protocol_read),
			&(parquetInsertDesc->footerProtocol),
			parquetInsertDesc->previous_rowgroupcnt);

	CloseWritableFileSeg(parquetInsertDesc);

	/** free parquet insert desc*/
	freeParquetInsertDesc(parquetInsertDesc);

	/* Allocation is done.	Go back to caller memory-context. */
	MemoryContextSwitchTo(oldMemoryContext);

	pfree(parquetInsertDesc);
}

void
freeParquetInsertDesc(ParquetInsertDesc parquetInsertDesc)
{
	if(parquetInsertDesc->aoEntry != NULL)
	{
		pfree(parquetInsertDesc->aoEntry);
		parquetInsertDesc->aoEntry = NULL;
	}
	if(parquetInsertDesc->title != NULL)
	{
		pfree(parquetInsertDesc->title);
		parquetInsertDesc->title = NULL;
	}
	if(parquetInsertDesc->relname != NULL)
	{
		pfree(parquetInsertDesc->relname);
		parquetInsertDesc->relname = NULL;
	}
	if(parquetInsertDesc->parquetFilePathName != NULL)
	{
		pfree(parquetInsertDesc->parquetFilePathName);
		parquetInsertDesc->parquetFilePathName = NULL;
	}
	if(parquetInsertDesc->mirroredOpen != NULL)
	{
		pfree(parquetInsertDesc->mirroredOpen);
		parquetInsertDesc->mirroredOpen = NULL;
	}
	if(parquetInsertDesc->parquetMetadata != NULL)
	{
		freeParquetMetadata(parquetInsertDesc->parquetMetadata);
		parquetInsertDesc->parquetMetadata = NULL;
	}
}

/*
 * Finished writing to this file segment. Update catalog and close file.
 */
static void CloseWritableFileSeg(ParquetInsertDesc parquetInsertDesc) {

	int64	fileLen_uncompressed;
	int64	fileLen;

	TransactionFlushAndCloseFile(parquetInsertDesc, &fileLen, &fileLen_uncompressed);

	parquetInsertDesc->sendback->segno = parquetInsertDesc->cur_segno;
	parquetInsertDesc->sendback->insertCount = parquetInsertDesc->insertCount;

	parquetInsertDesc->sendback->numfiles = 1;
	parquetInsertDesc->sendback->eof[0] = fileLen;
	parquetInsertDesc->sendback->uncompressed_eof[0] = fileLen_uncompressed;

	/*
	 * Update the parquet segment info table with our new eof
	 */
	if (Gp_role != GP_ROLE_EXECUTE)
		UpdateParquetFileSegInfo(parquetInsertDesc->parquet_rel,
				parquetInsertDesc->aoEntry, parquetInsertDesc->cur_segno,
				fileLen, fileLen_uncompressed, parquetInsertDesc->insertCount);

	pfree(parquetInsertDesc->fsInfo);
	parquetInsertDesc->fsInfo = NULL;

	if (Debug_appendonly_print_insert)
		elog(LOG,
		"Parquet scan closed write file segment #%d for table %s "
		"(file length " INT64_FORMAT ", insert count %lld",
		parquetInsertDesc->cur_segno,
		NameStr(parquetInsertDesc->parquet_rel->rd_rel->relname),
		fileLen,
		(long long)parquetInsertDesc->insertCount);

}


/*
 * Flush and close the current segment file under a transaction.
 *
 * Handles mirror loss end transaction work.
 *
 * No error if the current is already closed.
 */
static void TransactionFlushAndCloseFile(
	ParquetInsertDesc	parquetInsertDesc,
	int64				*newLogicalEof, /* The new EOF for the segment file. */
	int64				*fileLen_uncompressed)
{
	Assert(parquetInsertDesc != NULL);
	if (parquetInsertDesc->parquet_file == -1)
	{
		*newLogicalEof = 0;
		*fileLen_uncompressed = 0;
		return;
	}

	/* Get Logical Eof and uncompressed length*/
	*newLogicalEof = FileNonVirtualTell(parquetInsertDesc->parquet_file);
	if (*newLogicalEof < 0){
		ereport(ERROR,
				(errcode_for_file_access(),
					errmsg("file tell position error in file '%s' for relation '%s': %s",
							parquetInsertDesc->parquetFilePathName,
							parquetInsertDesc->relname,
							strerror(errno))));
	}

	Assert(parquetInsertDesc->fileLen == *newLogicalEof);

	*fileLen_uncompressed = parquetInsertDesc->fileLen_uncompressed;

	int primaryError = 0;
	MirroredAppendOnly_FlushAndClose(parquetInsertDesc->mirroredOpen,
			&primaryError);
	if (primaryError != 0)
		ereport(ERROR,
				(errcode_for_file_access(),
						errmsg("file flush error when flushing (fsync) segment file '%s' to "
								"disk for relation '%s': %s",
								parquetInsertDesc->parquetFilePathName,
								parquetInsertDesc->relname,
								strerror(primaryError))));

	parquetInsertDesc->parquet_file = -1;

	if(parquetInsertDesc->file_previousmetadata == -1)
		return;

	FileClose(parquetInsertDesc->file_previousmetadata);
	parquetInsertDesc->file_previousmetadata = -1;

	/*assign newLogicalEof and fileLen_uncompressed*/

}

/*
 * Open the next file segment for write.
 */
static void SetCurrentFileSegForWrite(ParquetInsertDesc parquetInsertDesc, ResultRelSegFileInfo *segfileinfo) {
	ParquetFileSegInfo *fsinfo;
	int32 fileSegNo;

	/* Make the 'segment' file name */
	MakeAOSegmentFileName(parquetInsertDesc->parquet_rel,
			parquetInsertDesc->cur_segno, -1, &fileSegNo,
			parquetInsertDesc->parquetFilePathName);
	Assert(
			strlen(parquetInsertDesc->parquetFilePathName) + 1 <=
			parquetInsertDesc->parquetFilePathNameMaxLen);

	/*
	 * In order to append to this file segment entry we must first
	 * acquire the relation parquet segment file (transaction-scope) lock (tag
	 * LOCKTAG_RELATION_APPENDONLY_SEGMENT_FILE) in order to guarantee
	 * stability of the pg_aoseg information on this segment file and exclusive right
	 * to append data to the segment file.
	 *
	 * NOTE: This is a transaction scope lock that must be held until commit / abort.
	 */
	LockRelationAppendOnlySegmentFile(&parquetInsertDesc->parquet_rel->rd_node,
			parquetInsertDesc->cur_segno, AccessExclusiveLock,
			/* dontWait */false);

	/* Now, get the information for the file segment we are going to append to. */
	parquetInsertDesc->fsInfo = (ParquetFileSegInfo *) palloc0(sizeof(ParquetFileSegInfo));

	/*
	 * in hawq, we cannot insert a new catalog entry and then update,
	 * since we cannot get the tid of added tuple.
	 * we should add the new catalog entry on master and then dispatch it to segments for update.
	 */
	Assert(parquetInsertDesc->fsInfo != NULL);
	Assert(segfileinfo->numfiles == 1);
	fsinfo = parquetInsertDesc->fsInfo;
	fsinfo->segno = segfileinfo->segno;
	fsinfo->tupcount = segfileinfo->tupcount;
	fsinfo->eof = segfileinfo->eof[0];
	fsinfo->eof_uncompressed = segfileinfo->uncompressed_eof[0];

	parquetInsertDesc->fileLen = (int64)fsinfo->eof;
	parquetInsertDesc->fileLen_uncompressed = (int64)fsinfo->eof_uncompressed;
	parquetInsertDesc->rowCount = fsinfo->tupcount;

	/* Open the existing file for write.*/
	OpenSegmentFile(
			parquetInsertDesc->mirroredOpen,
			parquetInsertDesc->parquetFilePathName, fsinfo->eof,
			&parquetInsertDesc->parquet_rel->rd_node,
			parquetInsertDesc->cur_segno, parquetInsertDesc->relname,
			&parquetInsertDesc->parquet_file,
			&parquetInsertDesc->file_previousmetadata,
			&parquetInsertDesc->protocol_read,
			parquetInsertDesc->parquet_rel->rd_att,
			&parquetInsertDesc->parquetMetadata,
			&parquetInsertDesc->fileLen,
			&parquetInsertDesc->fileLen_uncompressed,
			&parquetInsertDesc->previous_rowgroupcnt);

	initSerializeFooter(&(parquetInsertDesc->footerProtocol), parquetInsertDesc->parquetFilePathName);

}

/*
 * Opens the next segment file to write.  The file must already exist.
 * This routine is responsible for seeking to the proper write location
 * given the logical EOF.
 *
 * @filePathName:	The name of the segment file to open.
 * @logicalEof:		The last committed write transaction's EOF
 * 					value to use as the end of the segment file.
 * @parquet_file		The file handler of segment file
 */
static void OpenSegmentFile(
		MirroredAppendOnlyOpen *mirroredOpen,
		char *filePathName,
		int64 logicalEof,
		RelFileNode *relFileNode,
		int32 segmentFileNum,
		char *relname,
		File *parquet_file,
		File *parquet_file_previous,
		CompactProtocol **protocol_read,
		TupleDesc tableAttrs,
		ParquetMetadata *parquetMetadata,
		int64 *fileLen,
		int64 *fileLen_uncompressed,
		int *previous_rowgroupcnt) {
	int primaryError;

	File file;

	int64 seekResult;

	Assert(filePathName != NULL);

	bool metadataExist = false;

	/*
	 * Open the file for metadata reading.
	 */
	MirroredAppendOnly_OpenReadWrite(mirroredOpen, relFileNode, segmentFileNum,
			relname, logicalEof, true, &primaryError);
	if (primaryError != 0)
		ereport(ERROR,
				(errcode_for_file_access(),
						errmsg("file open error when opening file "
								"'%s' for relation '%s': %s", filePathName, relname,
								strerror(primaryError))));

	*parquet_file_previous = mirroredOpen->primaryFile;

	int64 fileSize = FileSeek(*parquet_file_previous, 0, SEEK_END);
	if (fileSize < 0){
		ereport(ERROR,
				(errcode_for_file_access(),
						errmsg("file seek error in file '%s' for relation "
								"'%s'", filePathName, relname)));
	}
	if (logicalEof > fileSize) {
		ereport(ERROR,
				(errcode(ERRCODE_GP_INTERNAL_ERROR),
						errmsg("logical eof exceeds file size in file '%s' for relation '%s'",
								filePathName,
								relname)));
	}

	/*read parquet footer, get metadata information before rowgroup metadata*/
	metadataExist = readParquetFooter(*parquet_file_previous, parquetMetadata, protocol_read,
			logicalEof, filePathName);

	*previous_rowgroupcnt = (*parquetMetadata)->blockCount;

	/*
	 * Open the file for writing.
	 */
	MirroredAppendOnly_OpenReadWrite(mirroredOpen, relFileNode, segmentFileNum,
			relname, logicalEof, false, &primaryError);
	if (primaryError != 0)
		ereport(ERROR,
				(errcode_for_file_access(),
						errmsg("file open error when opening file '%s' "
								"for relation '%s': %s", filePathName, relname,
								strerror(primaryError))));

	file = mirroredOpen->primaryFile;

	seekResult = FileNonVirtualTell(file);
	if (seekResult != logicalEof) {
		/* previous transaction is aborted truncate file*/
		if (FileTruncate(file, logicalEof)) {

			MirroredAppendOnly_Close(mirroredOpen);
			ereport(ERROR,
					(errcode_for_file_access(),
						errmsg("file truncate error in file '%s' for relation "
								"'%s' to position " INT64_FORMAT ": %s",
								filePathName, relname, logicalEof,
								strerror(errno))));
		}
	}

	*parquet_file = file;

	/*if metadata not exist, should initialize the metadata, and write out file header*/
	if (metadataExist == false) {
		/* init parquet metadata information, init schema information using table attributes,
		 * and may get existing information from data file*/
		initparquetMetadata(*parquetMetadata, tableAttrs, *parquet_file);

		/*should judge whether file already exists, if a new file, should write header out*/
		writeParquetHeader(*parquet_file, filePathName, fileLen, fileLen_uncompressed);
	}
	else {
		if (!checkAndSyncMetadata(*parquetMetadata, tableAttrs))
		{
			ereport(ERROR,
					(errcode(ERRCODE_GP_INTERNAL_ERROR),
						errmsg("parquet storage write file's metadata incompatible "
								"with table's schema for relation '%s'.",
						relname)));
		}
	}
}

/**
 * Given parquet table oid, return the memory reserved for parquet table insert operator.
 * For uncompressed table, the whole rowgroup is stored in memory before written to disk,
 * so we can keep a memory quota of rowgroup size for it.
 * For compressed table, besides the compressed rowgroup, there's a page buffer for each column
 * storing the original uncompressed data, so the max memory consumption under worst case is
 * 2 times of rowgroup size.
 * @rel_oid		The oid of relation to be inserted
 * @return		The memory allocated for this table insert
 */
uint64 memReservedForParquetInsert(Oid rel_oid) {
	uint64 memReserved = 0;
	char *compresstype = NULL;

	AppendOnlyEntry *aoEntry = GetAppendOnlyEntry(rel_oid, SnapshotNow);
	memReserved = aoEntry->blocksize;
	compresstype = aoEntry->compresstype;

	if (compresstype && (strcmp(compresstype, "none") != 0)){
		memReserved *= 2;
	}
	pfree(aoEntry);
	return memReserved;
}

/**
 * Given parquet table oid, return the memory reserved for parquet table scan operator.
 *
 * 1) For Un-compressed parquet table. When scanning parquet table, just the required columns
 * are loaded into memory instead of the entire row group, so we can use
 * sum(columnwidth)/recordwidth * rowgroupsize to estimate memory occupation.
 *
 * 2) For compressed parquet table. When scanning the parquet table, besides loading required
 * columns(compressed data) into memory, each column chunk has a page buffer storing uncompressed
 * data for this page. Usually one column chunk has multiple column pages, and the worst case is
 * that one column chunk just has one column page, under which there are two copies of data for
 * each required column: compressed and uncompressed. Because the rowgroupsize is the uncompressed
 * rowgroup limit, we can estimate the worst case of memory consuming to be
 * (columnwidth)/recordwidth * rowgroupsize * 2.
 *
 * @rel_oid		The oid of relation to be inserted
 * @attr_list		The list of attributes to be scanned
 * @return		The memory allocated for this table insert
 */

uint64 memReservedForParquetScan(Oid rel_oid, List* attr_list) {
	uint64		rowgroupsize = 0;
	char		*compresstype = NULL;
	uint64		memReserved = 0;

	int 		attrNum = get_relnatts(rel_oid); /*Get the total attribute number of the relation*/
	uint64		attsWidth = 0;		/*the sum width of attributes to be scanned*/
	uint64		recordWidth = 0;	/*the average width of one record in the relation*/
	/* The width array for all the attributes in the relation*/
	int32		*attWidth = (int32*)palloc0(attrNum * sizeof(int32));

	/** The variables for traversing through attribute list*/
	ListCell	*cell;

	/* Get rowgroup size and compress type */
	AppendOnlyEntry *aoEntry = GetAppendOnlyEntry(rel_oid, SnapshotNow);
	rowgroupsize = aoEntry->blocksize;
	compresstype = aoEntry->compresstype;


	/** For each column in the relation, get the column width
	 * 1) Get the column width from pg_attribute, estimate column width for to-be-scanned columns:
	 * If fixed column width, the attlen is the column width; if not fixed, refer to typmod
	 * 2) Get the average column width for variable length type column from table pg_statistic, if the
	 * stawidth not equals 0, set it as the column width.
	 */
	for(int i = 0; i < attrNum; i++){
		int att_id = i + 1;
		HeapTuple attTuple = caql_getfirst(NULL, cql("SELECT * FROM pg_attribute"
				" WHERE attrelid = :1 "
				" AND attnum = :2 ",
				ObjectIdGetDatum(rel_oid),
				Int16GetDatum(att_id)));

		if (HeapTupleIsValid(attTuple)) {
			/*Step1: estimate attwidth according to pg_attributes*/
			Form_pg_attribute att = (Form_pg_attribute) GETSTRUCT(attTuple);
			estimateColumnWidth(attWidth, &i, att, false);
			i--;

			int32 stawidth = 0;
			/*Step2: adjust addwidth according to pg_statistic*/
			switch (att->atttypid)
			{
				case HAWQ_TYPE_VARCHAR:
				case HAWQ_TYPE_TEXT:
				case HAWQ_TYPE_XML:
				case HAWQ_TYPE_PATH:
				case HAWQ_TYPE_POLYGON:
					stawidth = get_attavgwidth(rel_oid, att_id);
					if(stawidth != 0)
						attWidth[i] = stawidth;
					break;
				case HAWQ_TYPE_VARBIT:
					stawidth = get_attavgwidth(rel_oid, att_id);
					if(stawidth != 0)
						attWidth[i] = stawidth + 4;
					break;
				default:
					break;
			}
		}
		recordWidth += attWidth[i];
	}

	/* Reverse through the to-be-scanned attribute list, sum up the width */
	Assert (1 <= list_length(attr_list));
	foreach(cell, attr_list)
	{
		AttrNumber att_id = lfirst_int(cell);
		Assert(1 <= att_id);
		Assert(att_id <= attrNum);
		attsWidth += attWidth[att_id - 1];	/*sum up the attribute width in the to-be-scanned list*/
	}

	pfree(attWidth);

	memReserved = (attsWidth * rowgroupsize) / recordWidth;
	if(compresstype != NULL && (strcmp(compresstype, "none") != 0))
	{
		memReserved *= 2;
	}
	pfree(aoEntry);
	/* Since memory allocated to parquet scan depends on the columns get scanned, it is possible
	 * to allocate very small chunk of memory if the scanned columns are very small in terms of the
	 * average table width. Therefore, we set minimum threshold of memory allocated as 100 KB to
	 * be consistent with the non-memory intensive operators.
	 *
	 */
	if (100 * 1024 > memReserved)
	{
		memReserved = 100 * 1024;
	}

	return memReserved;
}
