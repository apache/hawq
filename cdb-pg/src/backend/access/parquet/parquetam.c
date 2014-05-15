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
#include "access/sdir.h"
#include "utils/relcache.h"
#include "utils/tqual.h"
#include "utils/rel.h"
#include "access/aomd.h"
#include "cdb/cdbvars.h"
#include "cdb/cdbparquetam.h"
#include "cdb/cdbparquetstoragewrite.h"
#include "catalog/pg_attribute_encoding.h"


/**For read*/
static void initscan(ParquetScanDesc scan);

/*get next segment file for read*/
static bool SetNextFileSegForRead(ParquetScanDesc scan);

/*get next row group to read*/
static bool getNextRowGroup(ParquetScanDesc scan);

/*close current scanning file segments*/
static void CloseScannedFileSeg(ParquetScanDesc scan);

static bool ValidateParquetSegmentFile(
		TupleDesc hawqTupDescs,
		int *hawqAttrToParquetColChunks,
		ParquetMetadata parquetMetadata);

static int getColumnChunkNumForHawqAttr(struct FileField_4C* hawqAttr);


/*For write*/
static void SetCurrentFileSegForWrite(
		ParquetInsertDesc parquetInsertDesc);

static void OpenSegmentFile(
		MirroredAppendOnlyOpen *mirroredOpen,
		char *filePathName,
		int64 logicalEof,
		RelFileNode *relFileNode,
		int32 segmentFileNum,
		char *relname,
		int32 contentid,
		File *parquet_file,
		TupleDesc tableAttrs,
		ParquetMetadata *parquetMetadata,
		int64 *fileLen,
		int64 *fileLen_uncompressed);

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
	scan->pqs_segfiles_processed = 0;
	scan->pqs_need_new_segfile = true; /* need to assign a file to be scanned */
	scan->pqs_done_all_segfiles = false;
	scan->bufferDone = true;

	ItemPointerSet(&scan->cdb_fake_ctid, 0, 0);
	scan->cur_seg_row = 0;

	pgstat_count_heap_scan(scan->pqs_rd);
}

/**
 *begin scanning of a parquet relation
 */
ParquetScanDesc parquet_beginscan(Relation relation,
		Snapshot parquetMetaDataSnapshot, TupleDesc relationTupleDesc,
		bool *proj) {
	ParquetScanDesc 	scan;
	AppendOnlyEntry		*aoEntry;

	AppendOnlyStorageAttributes *attr;

	StringInfoData		titleBuf;
	/*
	 * increment relation ref count while scanning relation
	 *
	 * This is just to make really sure the relcache entry won't go away while
	 * the scan has a pointer to it.  Caller should be holding the rel open
	 * anyway, so this is redundant in all normal scenarios...
	 */
	RelationIncrementReferenceCount(relation);

	/*
	 * allocate scan descriptor
	 */
	scan = (ParquetScanDescData *)palloc0(sizeof(ParquetScanDescData));

	/*
	 * Get the pg_appendonly information for this table
	 */
	aoEntry = GetAppendOnlyEntry(RelationGetRelid(relation), parquetMetaDataSnapshot);
	scan->aoEntry = aoEntry;
	Assert(aoEntry->majorversion == 1 && aoEntry->minorversion == 0);

	/*
	 * initialize the scan descriptor
	 */
	scan->pqs_filenamepath_maxlen = AOSegmentFilePathNameLen(relation) + 1;
	scan->pqs_filenamepath = (char*)palloc(scan->pqs_filenamepath_maxlen);
	scan->pqs_filenamepath[0] = '\0';
	scan->pqs_rd = relation;
	scan->parquetScanInitContext = CurrentMemoryContext;
	initStringInfo(&titleBuf);
	appendStringInfo(&titleBuf, "Scan of Parquet relation '%s'",
					 RelationGetRelationName(relation));
	scan->title = titleBuf.data;

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
	attr->version			= aoEntry->version;


	AORelationVersion_CheckValid(attr->version);

	/*
	 * Get information about all the file segments we need to scan
	 */
	scan->pqs_segfile_arr = GetAllParquetFileSegInfo(relation, aoEntry,
			parquetMetaDataSnapshot, &scan->pqs_total_segfiles);

	scan->proj = proj;

	scan->pqs_tupDesc = (relationTupleDesc == NULL) ? RelationGetDescr(relation) : relationTupleDesc;

	scan->hawqAttrToParquetColChunks = (int*)palloc0(scan->pqs_tupDesc->natts * sizeof(int));

	initscan(scan);

	return scan ;
}

void parquet_rescan(ParquetScanDesc scan) {
    CloseScannedFileSeg(scan);
	scan->initedStorageRoutines = false;
	ParquetStorageRead_FinishSession(&(scan->storageRead));
	initscan(scan);
}

void parquet_endscan(ParquetScanDesc scan) {
	ParquetExecutorReadRowGroup readRowGroup = scan->executorReadRowGroup;

	RelationDecrementReferenceCount(scan->pqs_rd);

	if(readRowGroup.columnReaders != NULL)
	{
		for (int i = 0; i < readRowGroup.columnReaderCount; ++i)
		{
			ParquetColumnReader *reader = &readRowGroup.columnReaders[i];

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
		}
		pfree(readRowGroup.columnReaders);
	}
    
    CloseScannedFileSeg(scan);
	ParquetStorageRead_FinishSession(&(scan->storageRead));
	scan->initedStorageRoutines = false;

	if(scan->hawqAttrToParquetColChunks != NULL){
		pfree(scan->hawqAttrToParquetColChunks);
	}

	if(scan->aoEntry != NULL){
		pfree(scan->aoEntry);
	}
}

void parquet_getnext(ParquetScanDesc scan, ScanDirection direction,
		TupleTableSlot *slot) {

	AOTupleId aoTupleId;
	int64 rowNum = INT64CONST(-1);

	Assert(ScanDirectionIsForward(direction));

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
				if(scan->pqs_done_all_segfiles)
				{
					ExecClearTuple(slot);
					return /*NULL*/;
				}
			}

			scan->bufferDone = false;
		}

		bool tupleExist = ParquetExecutorReadRowGroup_ScanNextTuple(
												scan->pqs_tupDesc,
												&scan->executorReadRowGroup,
												scan->hawqAttrToParquetColChunks,
												scan->proj,
												slot);

		if(tupleExist){

			AOTupleIdInit_Init(&aoTupleId);
			AOTupleIdInit_segmentFileNum(&aoTupleId,
										 scan->pqs_segfile_arr[scan->pqs_segfiles_processed - 1]->segno);

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
static bool getNextRowGroup(ParquetScanDesc scan)
{
	if (scan->pqs_need_new_segfile)
	{
		/*
		 * Need to open a new segment file.
		 */
		if(!SetNextFileSegForRead(scan))
			return false;

		scan->cur_seg_row = 0;
	}

	if (!ParquetExecutorReadRowGroup_GetRowGroupInfo(
									&scan->storageRead,
									&scan->executorReadRowGroup,
									scan->proj,
									scan->pqs_tupDesc,
									scan->hawqAttrToParquetColChunks))
	{
		/* done reading the file */
		CloseScannedFileSeg(scan);

		return false;
	}

	ParquetExecutorReadRowGroup_GetContents(
									&scan->executorReadRowGroup,
									scan->storageRead.memoryContext);

	return true;
}

/*
 * Finished scanning this file segment. Close it.
 */
static void
CloseScannedFileSeg(ParquetScanDesc scan)
{
	ParquetStorageRead_CloseFile(&scan->storageRead);

	scan->pqs_need_new_segfile = true;
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
	bool			finished_all_files = true; /* assume */
	int32			fileSegNo;
	bool			parquetMetadataCorrect;

	Assert(scan->pqs_need_new_segfile);   /* only call me when last segfile completed */
	Assert(!scan->pqs_done_all_segfiles); /* don't call me if I told you to stop */

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
							scan->title,
							&scan->storageAttributes);

		/*
		 * There is no guarantee that the current memory context will be preserved between calls,
		 * so switch to a safe memory context for retrieving compression information.
		 */
		MemoryContext oldMemoryContext = MemoryContextSwitchTo(scan->parquetScanInitContext);

		/* Switch back to caller's memory context. */
		MemoryContextSwitchTo(oldMemoryContext);

		ParquetExecutorReadRowGroup_Init(
							&scan->executorReadRowGroup,
							scan->pqs_rd,
							scan->parquetScanInitContext,
							&scan->storageRead);

		scan->bufferDone = true; /* so we read a new buffer right away */

		scan->initedStorageRoutines = true;
	}

	/*
	 * Do we have more segment files to read or are we done?
	 */
	while(scan->pqs_segfiles_processed < scan->pqs_total_segfiles)
	{
		/* still have more segment files to read. get info of the next one */
		ParquetFileSegInfo *fsinfo = scan->pqs_segfile_arr[scan->pqs_segfiles_processed];
		segno = fsinfo->segno;
		eof = (int64)fsinfo->eof;

		scan->pqs_segfiles_processed++;

		/*
		 * special case: we are the QD reading from a parquet table in utility mode
		 * (gp_dump). We see entries in the parquetseg table but no files or data
		 * actually exist. If we try to open this file we'll get an error, so
		 * we must skip to the next. For now, we can test if the file exists by
		 * looking at the eof value - it's always 0 on the QD.
		 */
		if(eof > 0)
		{
			finished_all_files = false;
			break;
		}
	}

	if(finished_all_files)
	{
		/* finished reading all segment files */
		scan->pqs_need_new_segfile = false;
		scan->pqs_done_all_segfiles = true;
		return false;
	}

	MakeAOSegmentFileName(reln, segno, -1, &fileSegNo, scan->pqs_filenamepath);
	Assert(strlen(scan->pqs_filenamepath) + 1 <= scan->pqs_filenamepath_maxlen);

	Assert(scan->initedStorageRoutines);

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

	scan->pqs_need_new_segfile = false;

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
ParquetInsertDesc parquet_insert_init(Relation rel, int segno) {
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

	Assert(segno >= 0);
	parquetInsertDesc->cur_segno = segno;
	parquetInsertDesc->aoEntry = aoentry;
	parquetInsertDesc->insertCount = 0;

	parquetInsertDesc->parquetMetadata = (struct ParquetMetadata_4C *)
			palloc0(sizeof(struct ParquetMetadata_4C));
	/*SHOULD CALL OPEN METADATA FILE HERE, AND GET parquetMetadata INFO*/

	initStringInfo(&titleBuf);
	appendStringInfo(&titleBuf, "Write of Parquet relation '%s'",
			RelationGetRelationName(parquetInsertDesc->parquet_rel));
	parquetInsertDesc->title = titleBuf.data;

	parquetInsertDesc->mirroredOpen = (MirroredAppendOnlyOpen *)
			palloc0(sizeof(MirroredAppendOnlyOpen));
	parquetInsertDesc->mirroredOpen->isActive = FALSE;
	parquetInsertDesc->mirroredOpen->segmentFileNum = 0;
	parquetInsertDesc->mirroredOpen->primaryFile = -1;

	/* open our current relation file segment for write */
	SetCurrentFileSegForWrite(parquetInsertDesc);

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
				  &parquetInsertDesc->fileLen,
				  &parquetInsertDesc->fileLen_uncompressed);

	/*should add row group row number*/
	writeParquetFooter(parquetInsertDesc->parquet_file,
			parquetInsertDesc->parquetFilePathName,
			parquetInsertDesc->parquetMetadata, &parquetInsertDesc->fileLen,
			&parquetInsertDesc->fileLen_uncompressed);

	CloseWritableFileSeg(parquetInsertDesc);

	/** free parquet insert desc*/
	freeParquetInsertDesc(parquetInsertDesc);

	/* Allocation is done.	Go back to caller memory-context. */
	MemoryContextSwitchTo(oldMemoryContext);

	pfree(parquetInsertDesc);
}

void freeParquetInsertDesc(ParquetInsertDesc parquetInsertDesc) {
	pfree(parquetInsertDesc->aoEntry);
	pfree(parquetInsertDesc->title);
	pfree(parquetInsertDesc->relname);
	pfree(parquetInsertDesc->parquetFilePathName);
	pfree(parquetInsertDesc->mirroredOpen);

	pfree(parquetInsertDesc->parquetMetadata->hawqschemastr);
	for (int i = 0; i < parquetInsertDesc->parquetMetadata->blockCount; i++) {
		struct BlockMetadata_4C* block =
				parquetInsertDesc->parquetMetadata->pBlockMD[i];
		for (int j = 0; j < block->ColChunkCount; j++) {
			pfree(block->columns[j].colName);
			if(block->columns[j].pathInSchema != NULL){
				pfree(block->columns[j].pathInSchema);
			}
			pfree(block->columns[j].pEncodings);
		}
		pfree(block->columns);
	}
	pfree(parquetInsertDesc->parquetMetadata->pBlockMD);

	for (int i = 0; i < parquetInsertDesc->parquetMetadata->fieldCount; i++) {
		pfree(parquetInsertDesc->parquetMetadata->pfield[i].name);
	}
	pfree(parquetInsertDesc->parquetMetadata->pfield);
	pfree(parquetInsertDesc->parquetMetadata->estimateChunkSizes);
	pfree(parquetInsertDesc->parquetMetadata);
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
				fileLen, fileLen_uncompressed, parquetInsertDesc->insertCount,
				GpIdentity.segindex);

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
	if (*newLogicalEof < 0)
	{
		ereport(ERROR,
				(errcode_for_file_access(),
					errmsg("file tell position error in file '%s' for relation '%s': %s"
							, parquetInsertDesc->parquetFilePathName, parquetInsertDesc->relname, strerror(errno)),
					errdetail("%s", HdfsGetLastError())));
	}

	Assert(parquetInsertDesc->fileLen == *newLogicalEof);

	*fileLen_uncompressed = parquetInsertDesc->fileLen_uncompressed;

	int primaryError = 0;
	MirroredAppendOnly_FlushAndClose(parquetInsertDesc->mirroredOpen,
			&primaryError);
	if (primaryError != 0)
		ereport(ERROR,
				(errcode_for_file_access(),
						errmsg("file flush error when flushing (fsync) segment file '%s' to " "disk for relation '%s': %s"
								, parquetInsertDesc->parquetFilePathName, parquetInsertDesc->relname, strerror(primaryError)),
						errdetail("%s", HdfsGetLastError())));

	parquetInsertDesc->parquet_file = -1;

	/*assign newLogicalEof and fileLen_uncompressed*/

}

/*
 * Open the next file segment for write.
 */
static void SetCurrentFileSegForWrite(ParquetInsertDesc parquetInsertDesc) {
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
			/* dontWait */false, GpIdentity.segindex);

	/* Now, get the information for the file segment we are going to append to. */
	parquetInsertDesc->fsInfo = GetParquetFileSegInfo(
			parquetInsertDesc->parquet_rel, parquetInsertDesc->aoEntry,
			parquetInsertDesc->parquetMetaDataSnapshot,
			parquetInsertDesc->cur_segno, GpIdentity.segindex);

	/*
	 * in hawq, we cannot insert a new catalog entry and then update,
	 * since we cannot get the tid of added tuple.
	 * we should add the new catalog entry on master and then dispatch it to segments for update.
	 */
	Assert(parquetInsertDesc->fsInfo != NULL);
	fsinfo = parquetInsertDesc->fsInfo;
	Assert(fsinfo);
	parquetInsertDesc->fileLen = (int64)fsinfo->eof;
	parquetInsertDesc->fileLen_uncompressed = (int64)fsinfo->eof_uncompressed;
	parquetInsertDesc->rowCount = fsinfo->tupcount;

	/* Open the existing file for write.*/
	OpenSegmentFile(
			parquetInsertDesc->mirroredOpen,
			parquetInsertDesc->parquetFilePathName, fsinfo->eof,
			&parquetInsertDesc->parquet_rel->rd_node,
			parquetInsertDesc->cur_segno, parquetInsertDesc->relname,
			GpIdentity.segindex,
			&parquetInsertDesc->parquet_file,
			parquetInsertDesc->parquet_rel->rd_att,
			&parquetInsertDesc->parquetMetadata,
			&parquetInsertDesc->fileLen,
			&parquetInsertDesc->fileLen_uncompressed);
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
		int32 contentid,
		File *parquet_file,
		TupleDesc tableAttrs,
		ParquetMetadata *parquetMetadata,
		int64 *fileLen,
		int64 *fileLen_uncompressed) {
	int primaryError;

	File file;

	int64 seekResult;

	Assert(filePathName != NULL);

	bool metadataExist = false;

	/*
	 * Open or create the file for metadata reading.
	 */
	MirroredAppendOnly_OpenReadWrite(mirroredOpen, relFileNode, segmentFileNum,
			contentid, relname, logicalEof, true, &primaryError);
	if (primaryError != 0)
		ereport(ERROR,
				(errcode_for_file_access(), errmsg("file open error when opening file " "'%s' for relation '%s': %s"
						, filePathName, relname, strerror(primaryError)),
				errdetail("%s", HdfsGetLastError())));

	file = mirroredOpen->primaryFile;

	int64 fileSize = FileSeek(file, 0, SEEK_END);
	if (fileSize < 0)
	{
		ereport(ERROR,
				(errcode_for_file_access(),
						errmsg("file seek error in file '%s' for relation " "'%s'", filePathName, relname),
						errdetail("%s", HdfsGetLastError())));
	}
	if (logicalEof > fileSize) {
		ereport(ERROR,
				(errcode(ERRCODE_GP_INTERNAL_ERROR),
						errmsg("logical eof exceeds file size in file '%s' for relation '%s'",
								filePathName,
								relname)));
	}

	metadataExist = readParquetFooter(file, parquetMetadata, logicalEof, filePathName);

	MirroredAppendOnly_Close(mirroredOpen);

	/*
	 * Open or create the file for write.
	 */
	MirroredAppendOnly_OpenReadWrite(mirroredOpen, relFileNode, segmentFileNum,
			contentid, relname, logicalEof, false, &primaryError);
	if (primaryError != 0)
		ereport(ERROR,
				(errcode_for_file_access(),
						errmsg("file open error when opening file '%s' " "for relation '%s': %s"
								, filePathName, relname, strerror(primaryError)),
						errdetail("%s", HdfsGetLastError())));

	file = mirroredOpen->primaryFile;

	seekResult = FileNonVirtualTell(file);
	if (seekResult != logicalEof) {
		if (seekResult < 0)
		{
			ereport(ERROR,
					(errcode_for_file_access(),
						errmsg("file tell error in file '%s' for relation " "'%s' to position " INT64_FORMAT ": %s", filePathName, relname, logicalEof, strerror(errno)),
						errdetail("%s", HdfsGetLastError())));
		}

		/* previous transaction is aborted truncate file*/
		if (FileTruncate(file, logicalEof)) {

			char * msg = pstrdup(HdfsGetLastError());

			MirroredAppendOnly_Close(mirroredOpen);
			ereport(ERROR,
					(errcode_for_file_access(), errmsg("file truncate error in file '%s' for relation " "'%s' to position " INT64_FORMAT ": %s", filePathName, relname, logicalEof, strerror(errno)),
							errdetail("%s", msg)));
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
