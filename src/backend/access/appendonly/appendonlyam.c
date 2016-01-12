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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/*-------------------------------------------------------------------------
 *
 * appendonlyam.c
 *	  append-only relation access method code
 *
 * Portions Copyright (c) 1996-2006, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2008-2009, Greenplum Inc.
 *
 *
 * INTERFACE ROUTINES
 *		appendonly_beginscan		- begin relation scan
 *		appendonly_rescan			- restart a relation scan
 *		appendonly_endscan			- end relation scan
 *		appendonly_getnext			- retrieve next tuple in scan
 *		appendonly_insert_init		- initialize an insert operation
 *		appendonly_insert			- insert tuple into a relation
 *		appendonly_insert_finish	- finish an insert operation
 *
 * NOTES
 *	  This file contains the appendonly_ routines which implement
 *	  the access methods used for all append-only relations.
 *
 *            $Id: $
 *        $Change: $
 *      $DateTime: $
 *        $Author: $
 *-------------------------------------------------------------------------
 */
#include "postgres.h"
#include "miscadmin.h"
#ifndef WIN32
#include <fcntl.h>
#else
#include <io.h>
#endif
#include <sys/file.h>
#include <unistd.h>

#include "fmgr.h"
#include "access/tupdesc.h"
#include "access/appendonlytid.h"
#include "access/filesplit.h"
#include "cdb/cdbappendonlystorage.h"
#include "cdb/cdbappendonlystorageformat.h"
#include "cdb/cdbappendonlystoragelayer.h"
#include "access/aomd.h"
#include "access/heapam.h"
#include "access/hio.h"
#include "access/multixact.h"
#include "access/transam.h"
#include "access/tuptoaster.h"
#include "access/valid.h"
#include "access/xact.h"
#include "access/appendonlywriter.h"
#include "access/aosegfiles.h"
#include "catalog/catalog.h"
#include "catalog/pg_appendonly.h"
#include "catalog/pg_attribute_encoding.h"
#include "catalog/namespace.h"
#include "cdb/cdbvars.h"
#include "cdb/cdbappendonlyam.h"
#include "pgstat.h"
#include "storage/procarray.h"
#include "storage/gp_compress.h"
#include "utils/inval.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/relcache.h"
#include "utils/syscache.h"
#include "storage/freespace.h"
#include "storage/smgr.h"
#include "storage/gp_compress.h"
#include "utils/builtins.h"
#include "utils/guc.h"

#ifdef HAVE_LIBZ
#include <zlib.h>
#endif

#define SCANNED_SEGNO  (&scan->aos_segfile_arr[ \
					(scan->aos_segfiles_processed == 0 ? 0 : scan->aos_segfiles_processed - 1)])->segno

typedef enum AoExecutorBlockKind
{
	AoExecutorBlockKind_None = 0,
	AoExecutorBlockKind_VarBlock,
	AoExecutorBlockKind_SingleRow,
	MaxAoExecutorBlockKind /* must always be last */
} AoExecutorBlockKind;

static void
AppendOnlyExecutionReadBlock_SetSegmentFileNum(
	AppendOnlyExecutorReadBlock		*executorReadBlock,
	int								segmentFileNum);

static void
AppendOnlyExecutionReadBlock_SetPositionInfo(
	AppendOnlyExecutorReadBlock		*executorReadBlock,
	int64							blockFirstRowNum);

static void
AppendOnlyExecutorReadBlock_Init(
	AppendOnlyExecutorReadBlock		*executorReadBlock,
	Relation						relation,
	MemoryContext					memoryContext,
	AppendOnlyStorageRead			*storageRead,
	int32							usableBlockSize);

static void
AppendOnlyExecutorReadBlock_Finish(
	AppendOnlyExecutorReadBlock		*executorReadBlock);

static void
AppendOnlyExecutorReadBlock_ResetCounts(
	AppendOnlyExecutorReadBlock		*executorReadBlock);


/* ----------------
 *		initscan - scan code common to appendonly_beginscan and appendonly_rescan
 * ----------------
 */
static void
initscan(AppendOnlyScanDesc scan, ScanKey key)
{

	/*
	 * copy the scan key, if appropriate
	 */
	if (key != NULL)
		memcpy(scan->aos_key, key, scan->aos_nkeys * sizeof(ScanKeyData));

	scan->aos_filenamepath[0] = '\0';
	scan->aos_splits_processed = 0;
	scan->aos_need_new_split = true; /* need to assign a file to be scanned */
	scan->aos_done_all_splits = false;
	scan->bufferDone = true;

	if (scan->initedStorageRoutines)
		AppendOnlyExecutorReadBlock_ResetCounts(
								&scan->executorReadBlock);

	pgstat_count_heap_scan(scan->aos_rd);
}

/*
 * Open the next file segment to scan and allocate all resources needed for it.
 */
static bool
SetNextFileSegForRead(AppendOnlyScanDesc scan)
{
	Relation		reln = scan->aos_rd;
	int				segno = -1;
	int64 			eof = -1;
	int64			end_of_split = -1;
	int64 offset = 0;
	bool			finished_all_splits = true; /* assume */
	int32			fileSegNo;
	bool toOpenFile = true;

	Assert(scan->aos_need_new_split);   /* only call me when last segfile completed */
	Assert(!scan->aos_done_all_splits); /* don't call me if I told you to stop */


	if (!scan->initedStorageRoutines)
	{
		PGFunction *fns = NULL;

		AppendOnlyStorageRead_Init(
							&scan->storageRead,
							scan->aoScanInitContext,
							scan->usableBlockSize,
							NameStr(scan->aos_rd->rd_rel->relname),
							scan->title,
							&scan->storageAttributes);

		/*
		 * There is no guarantee that the current memory context will be preserved between calls,
		 * so switch to a safe memory context for retrieving compression information.
		 */
		MemoryContext oldMemoryContext = MemoryContextSwitchTo(scan->aoScanInitContext);

		/* Get the relation specific compression functions */
		fns = RelationGetRelationCompressionFuncs(reln);
		scan->storageRead.compression_functions = fns;

		if (scan->storageRead.compression_functions != NULL)
		{
			PGFunction cons = fns[COMPRESSION_CONSTRUCTOR];
			CompressionState *cs;
			StorageAttributes sa;

			sa.comptype = scan->storageAttributes.compressType;
			sa.complevel = scan->storageAttributes.compressLevel;
			sa.blocksize = scan->usableBlockSize;

			/*
			 * The relation's tuple descriptor allows the compression
			 * constructor to make decisions about how to compress or
			 * decompress the relation given it's structure.
			 */
			cs = callCompressionConstructor(cons,
											RelationGetDescr(reln),
											&sa,
											false /* decompress */);
			scan->storageRead.compressionState = cs;
		}

		/* Switch back to caller's memory context. */
		MemoryContextSwitchTo(oldMemoryContext);

		AppendOnlyExecutorReadBlock_Init(
							&scan->executorReadBlock,
							scan->aos_rd,
							scan->aoScanInitContext,
							&scan->storageRead,
							scan->usableBlockSize);

		scan->bufferDone = true; /* so we read a new buffer right away */

		scan->initedStorageRoutines = true;
	}

	/*
	 * Do we have more segment files to read or are we done?
	 */
	while(scan->aos_splits_processed < list_length(scan->splits))
	{
		/* still have more segment files to read. get info of the next one */
		FileSplit split = (FileSplitNode *)list_nth(scan->splits, scan->aos_splits_processed);
		// new random read code
		// splits in the same doesn't need to reopen file
		if (scan->aos_splits_processed > 0) {
			FileSplit lastSplit = (FileSplitNode *) list_nth(scan->splits,
					scan->aos_splits_processed - 1);
			if (split->segno == lastSplit->segno) {
				toOpenFile = false;
			}
		}

		scan->toCloseFile = true;
		if (scan->aos_splits_processed + 1 < list_length(scan->splits)) {
			FileSplit nextSplit = (FileSplitNode *) list_nth(scan->splits,
					scan->aos_splits_processed + 1);
			if (split->segno == nextSplit->segno) {
				scan->toCloseFile = false;
			}
		}


		// int continueLength = 0;
		segno = split->segno;
		end_of_split = split->offsets + split->lengths;
		offset = split->offsets;
		scan->aos_splits_processed++;

		Assert(eof==-1 || eof == split->logiceof);
		if(eof==-1)	eof = split->logiceof;

		//workaround for split->logiceof is not set 
		if( end_of_split >eof )
			eof = end_of_split;
		
		/* lirong has combine continue splits in optimizer
				 * so we don't need to combine again
		// combine continue splits
		while(scan->aos_splits_processed + continueLength < list_length(scan->splits)){
			FileSplit nextSplit = (FileSplitNode *) list_nth(scan->splits,
							scan->aos_splits_processed + continueLength);
			continueLength++;
			if (split->segno == nextSplit->segno && end_of_split == nextSplit->offsets) {
					end_of_split = end_of_split + nextSplit->lengths;
					scan->aos_splits_processed++;
			}else{
				break;
			}
		}
		*/


		/*
		 * special case: we are the QD reading from an AO table in utility mode
		 * (gp_dump). We see entries in the aoseg table but no files or data
		 * actually exist. If we try to open this file we'll get an error, so
		 * we must skip to the next. For now, we can test if the file exists by
		 * looking at the end_of_split value - it's always 0 on the QD.
		 */
		if(end_of_split > 0)
		{
			finished_all_splits = false;
			break;
		}
	}

	if(finished_all_splits)
	{
		/* finished reading all segment files */
		scan->aos_need_new_split = false;
		scan->aos_done_all_splits = true;

		return false;
	}

	MakeAOSegmentFileName(reln, segno, -1, &fileSegNo, scan->aos_filenamepath);
	Assert(strlen(scan->aos_filenamepath) + 1 <= scan->aos_filenamepath_maxlen);

	Assert(scan->initedStorageRoutines);

	/* old code
	AppendOnlyStorageRead_OpenFile(
						&scan->storageRead,
						scan->aos_filenamepath,
						end_of_split,
						eof,
						offset);
	*/
	/* lei's code
	if (toOpenFile) {
		AppendOnlyStorageRead_OpenFile(&scan->storageRead,
				scan->aos_filenamepath, end_of_split, eof, offset);
	} else {
		AppendOnlyStorageRead *storageRead = &scan->storageRead;
		storageRead->logicalEof = end_of_split;
		storageRead->bufferedRead.largeReadPosition = offset;

		BufferedReadSetFile(&storageRead->bufferedRead, storageRead->file,
				storageRead->segmentFileName, end_of_split);
	}
	*/
	AppendOnlyStorageRead_OpenFile(&scan->storageRead,
					scan->aos_filenamepath, end_of_split,eof, offset,toOpenFile);

	AppendOnlyExecutionReadBlock_SetSegmentFileNum(
								&scan->executorReadBlock,
								segno);

	AppendOnlyExecutionReadBlock_SetPositionInfo(
								&scan->executorReadBlock,
								/* blockFirstRowNum */ 1);

	/* ready to go! */
	scan->aos_need_new_split = false;


	if (Debug_appendonly_print_scan)
		elog(LOG,"Append-only scan initialize for table '%s', %u/%u/%u, segment file %u, END_OF_SPLIT" INT64_FORMAT ", EOF " INT64_FORMAT ", "
			 "(compression = %s, usable blocksize %d)",
			 NameStr(scan->aos_rd->rd_rel->relname),
			 scan->aos_rd->rd_node.spcNode,
			 scan->aos_rd->rd_node.dbNode,
			 scan->aos_rd->rd_node.relNode,
			 segno,
			 end_of_split,
			 eof,
			 (scan->storageAttributes.compress ? "true" : "false"),
		     scan->usableBlockSize);


	return true;
}

/*
 * errcontext_appendonly_insert_block_user_limit
 *
 * Add an errcontext() line showing the table name but little else because this is a user
 * caused error.
 */
static int
errcontext_appendonly_insert_block_user_limit(AppendOnlyInsertDesc aoInsertDesc)
{
	char	*relationName = NameStr(aoInsertDesc->aoi_rel->rd_rel->relname);

	errcontext(
		 "Append-Only table '%s'",
		 relationName);

	return 0;
}


/*
 * errcontext_appendonly_insert_block
 *
 * Add an errcontext() line showing the table, segment file, offset in file, block count of the block being inserted.
 */
static int
errcontext_appendonly_insert_block(AppendOnlyInsertDesc aoInsertDesc)
{
	char	*relationName = NameStr(aoInsertDesc->aoi_rel->rd_rel->relname);
	int		segmentFileNum = aoInsertDesc->cur_segno;
	int64	headerOffsetInFile = AppendOnlyStorageWrite_CurrentPosition(&aoInsertDesc->storageWrite);
	int64	bufferCount = aoInsertDesc->bufferCount;

	errcontext(
		 "Append-Only table '%s', segment file #%d, block header offset in file = " INT64_FORMAT ", "
		 ", bufferCount " INT64_FORMAT ")",
		 relationName,
		 segmentFileNum,
		 headerOffsetInFile,
		 bufferCount);

	return 0;
}

/*
 * errdetail_appendonly_insert_block_header
 *
 * Add an errdetail() line showing the Append-Only Storage block header for the block being inserted.
 */
static int
errdetail_appendonly_insert_block_header(AppendOnlyInsertDesc aoInsertDesc)
{
	uint8	*header;

	bool 	usingChecksum;

	header = AppendOnlyStorageWrite_GetCurrentInternalBuffer(&aoInsertDesc->storageWrite);

	usingChecksum = aoInsertDesc->usingChecksum;

	return errdetail_appendonly_storage_content_header(header, usingChecksum, aoInsertDesc->storageAttributes.version);
}

/*
 * Open the next file segment for write.
 */
static void
SetCurrentFileSegForWrite(AppendOnlyInsertDesc aoInsertDesc, ResultRelSegFileInfo *segfileinfo)
{
	FileSegInfo		*fsinfo;
	int64			eof;
	int64			eof_uncompressed;
	int64			varblockcount;
	int32			fileSegNo;

	/* Make the 'segment' file name */
	MakeAOSegmentFileName(aoInsertDesc->aoi_rel,
					  aoInsertDesc->cur_segno, -1,
					  &fileSegNo,
					  aoInsertDesc->appendFilePathName);
	Assert(strlen(aoInsertDesc->appendFilePathName) + 1 <= aoInsertDesc->appendFilePathNameMaxLen);

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
									&aoInsertDesc->aoi_rel->rd_node,
									aoInsertDesc->cur_segno,
									AccessExclusiveLock,
									/* dontWait */ false);

	/* Now, get the information for the file segment we are going to append to. */
	aoInsertDesc->fsInfo = (FileSegInfo *) palloc0(sizeof(FileSegInfo));

	/*
	 * in hawq, we cannot insert a new catalog entry and then update,
	 * since we cannot get the tid of added tuple.
	 * we should add the new catalog entry on master and then dispatch it to segments for update.
	 */
	fsinfo = aoInsertDesc->fsInfo;
	Assert(segfileinfo->numfiles == 1);
	fsinfo->segno = segfileinfo->segno;
	fsinfo->eof = segfileinfo->eof[0];
	fsinfo->eof_uncompressed = segfileinfo->uncompressed_eof[0];
	fsinfo->varblockcount = segfileinfo->varblock;
	fsinfo->tupcount = segfileinfo->tupcount;

	eof = (int64)fsinfo->eof;
	eof_uncompressed = (int64)fsinfo->eof_uncompressed;
	varblockcount = (int64)fsinfo->varblockcount;
	aoInsertDesc->rowCount = fsinfo->tupcount;

	/*
	 * Open the existing file for write.
	 */
	AppendOnlyStorageWrite_OpenFile(
							&aoInsertDesc->storageWrite,
							aoInsertDesc->appendFilePathName,
							eof,
							eof_uncompressed,
							&aoInsertDesc->aoi_rel->rd_node,
							aoInsertDesc->cur_segno);

	/* reset counts */
	aoInsertDesc->insertCount = 0;
	aoInsertDesc->varblockCount = 0;

	/*
	 * Use the current block count from the segfile info so our system log error messages are
	 * accurate.
	 */
	aoInsertDesc->bufferCount = varblockcount;
}

/*
 * Finished scanning this file segment. Close it.
 */
static void
CloseScannedFileSeg(AppendOnlyScanDesc scan)
{
	AppendOnlyStorageRead_CloseFile(&scan->storageRead);

	scan->aos_need_new_split = true;
}

/*
 * Finished writing to this file segment. Update catalog and close file.
 */
static void
CloseWritableFileSeg(AppendOnlyInsertDesc aoInsertDesc)
{
	int64	fileLen_uncompressed;
	int64	fileLen;

	AppendOnlyStorageWrite_TransactionFlushAndCloseFile(
											&aoInsertDesc->storageWrite,
											&fileLen,
											&fileLen_uncompressed);

	aoInsertDesc->sendback->segno = aoInsertDesc->cur_segno;
	aoInsertDesc->sendback->varblock = aoInsertDesc->varblockCount;
	aoInsertDesc->sendback->insertCount = aoInsertDesc->insertCount;

	aoInsertDesc->sendback->eof = palloc(sizeof(int64));
	aoInsertDesc->sendback->uncompressed_eof = palloc(sizeof(int64));

	aoInsertDesc->sendback->numfiles = 1;
	aoInsertDesc->sendback->eof[0] = fileLen;
	aoInsertDesc->sendback->uncompressed_eof[0] = fileLen_uncompressed;

	/*
	 * Update the AO segment info table with our new eof
	 */
	if (Gp_role != GP_ROLE_EXECUTE)
		UpdateFileSegInfo(aoInsertDesc->aoi_rel,
					  aoInsertDesc->aoEntry,
					  aoInsertDesc->cur_segno,
					  fileLen,
					  fileLen_uncompressed,
					  aoInsertDesc->insertCount,
					  aoInsertDesc->varblockCount);

	pfree(aoInsertDesc->fsInfo);
	aoInsertDesc->fsInfo = NULL;

	if (Debug_appendonly_print_insert)
		elog(LOG,
		     "Append-only scan closed write file segment #%d for table %s "
		     "(file length " INT64_FORMAT ", insert count %f, VarBlock count %f",
		     aoInsertDesc->cur_segno,
		     NameStr(aoInsertDesc->aoi_rel->rd_rel->relname),
		     fileLen,
		     aoInsertDesc->insertCount,
		     aoInsertDesc->varblockCount);

}

//------------------------------------------------------------------------------

static void
AppendOnlyExecutorReadBlock_GetContents(
	AppendOnlyExecutorReadBlock		*executorReadBlock)
{
	VarBlockCheckError varBlockCheckError;

	if (!executorReadBlock->isCompressed)
	{
		if (!executorReadBlock->isLarge)
		{
			/*
			 * Small content.
			 */
			executorReadBlock->dataBuffer =
					AppendOnlyStorageRead_GetBuffer(executorReadBlock->storageRead, true);

			if (Debug_appendonly_print_scan)
				elog(LOG,
					 "Append-only scan read small non-compressed block for table '%s' "
					 "(length = %d, segment file '%s', block offset in file = " INT64_FORMAT ")",
					 AppendOnlyStorageRead_RelationName(executorReadBlock->storageRead),
					 executorReadBlock->dataLen,
					 AppendOnlyStorageRead_SegmentFileName(executorReadBlock->storageRead),
					 executorReadBlock->headerOffsetInFile);
		}
		else
		{
			/*
			 * Large row.
			 */

			// UNDONE: Error out if NOTOAST isn't ON.

			// UNDONE: Error out if it is not a single row
			Assert(executorReadBlock->executorBlockKind == AoExecutorBlockKind_SingleRow);

			/*
			 * Enough room in our private buffer?
			 * UNDONE: Is there a way to avoid the 2nd copy later doProcessTuple?
			 */
			if (executorReadBlock->largeContentBufferLen < executorReadBlock->dataLen)
			{
				MemoryContext	oldMemoryContext;

				/*
				 * Buffer too small.
				 */
				oldMemoryContext =
							MemoryContextSwitchTo(executorReadBlock->memoryContext);

				if (executorReadBlock->largeContentBuffer != NULL)
				{
					/*
					 * Make sure we set the our pointer to NULL here in case
					 * the subsequent allocation fails.  Otherwise cleanup will
					 * get confused.
					 */
					pfree(executorReadBlock->largeContentBuffer);
					executorReadBlock->largeContentBuffer = NULL;
				}

				executorReadBlock->largeContentBuffer = (uint8*)palloc(executorReadBlock->dataLen);
				executorReadBlock->largeContentBufferLen = executorReadBlock->dataLen;

				/* Deallocation and allocation done.  Go back to caller memory-context. */
				MemoryContextSwitchTo(oldMemoryContext);
			}

			executorReadBlock->dataBuffer = executorReadBlock->largeContentBuffer;

			AppendOnlyStorageRead_Content(
									executorReadBlock->storageRead,
									executorReadBlock->dataBuffer,
									executorReadBlock->dataLen,
									false);

			if (Debug_appendonly_print_scan)
				elog(LOG,
					 "Append-only scan read large row for table '%s' "
					 "(length = %d, segment file '%s', "
					 "block offset in file = " INT64_FORMAT ")",
					 AppendOnlyStorageRead_RelationName(executorReadBlock->storageRead),
					 executorReadBlock->dataLen,
					 AppendOnlyStorageRead_SegmentFileName(executorReadBlock->storageRead),
					 executorReadBlock->headerOffsetInFile);
		}
	}
	else
	{
		int32 compressedLen =
			AppendOnlyStorageRead_CurrentCompressedLen(executorReadBlock->storageRead);

		// AppendOnlyStorageWrite does not report compressed for large content metadata.
		Assert(!executorReadBlock->isLarge);

		/*
		 * Decompress into our temporary buffer.
		 */
		executorReadBlock->dataBuffer = executorReadBlock->uncompressedBuffer;

		AppendOnlyStorageRead_Content(
								executorReadBlock->storageRead,
								executorReadBlock->dataBuffer,
								executorReadBlock->dataLen,
								true);

		if (Debug_appendonly_print_scan)
			elog(LOG,
				 "Append-only scan read decompressed block for table '%s' "
				 "(compressed length %d, length = %d, segment file '%s', "
				 "block offset in file = " INT64_FORMAT ")",
				 AppendOnlyStorageRead_RelationName(executorReadBlock->storageRead),
				 compressedLen,
				 executorReadBlock->dataLen,
				 AppendOnlyStorageRead_SegmentFileName(executorReadBlock->storageRead),
				 executorReadBlock->headerOffsetInFile);
	}

	/*
	 * The executorBlockKind value is what the executor -- i.e. the upper
	 * part of this appendonlyam module! -- has stored in the Append-Only
	 * Storage header.  We interpret it here.
	 */

	switch (executorReadBlock->executorBlockKind)
	{
	case AoExecutorBlockKind_VarBlock:
		varBlockCheckError = VarBlockIsValid(executorReadBlock->dataBuffer, executorReadBlock->dataLen);
		if (varBlockCheckError != VarBlockCheckOk)
			ereport(ERROR,
					(errcode(ERRCODE_GP_INTERNAL_ERROR),
					 errmsg("VarBlock  is not valid. "
					        "Valid block check error %d, detail '%s'",
							varBlockCheckError,
							VarBlockGetCheckErrorStr()),
					 errdetail_appendonly_read_storage_content_header(executorReadBlock->storageRead),
					 errcontext_appendonly_read_storage_block(executorReadBlock->storageRead)));

		/*
		 * Now use the VarBlock module to extract the items out.
		 */
		VarBlockReaderInit(&executorReadBlock->varBlockReader,
						   executorReadBlock->dataBuffer,
						   executorReadBlock->dataLen);

		executorReadBlock->readerItemCount = VarBlockReaderItemCount(&executorReadBlock->varBlockReader);

		executorReadBlock->currentItemCount = 0;

		if (executorReadBlock->rowCount != executorReadBlock->readerItemCount)
		{
			ereport(ERROR,
					(errcode(ERRCODE_GP_INTERNAL_ERROR),
					 errmsg("Row count %d in append-only storage header does not match VarBlock item count %d",
							executorReadBlock->rowCount,
							executorReadBlock->readerItemCount),
					 errdetail_appendonly_read_storage_content_header(executorReadBlock->storageRead),
					 errcontext_appendonly_read_storage_block(executorReadBlock->storageRead)));
		}

		if (Debug_appendonly_print_scan)
		{
			elog(LOG,"Append-only scan read VarBlock for table '%s' with %d items (block offset in file = " INT64_FORMAT ")",
			     AppendOnlyStorageRead_RelationName(executorReadBlock->storageRead),
			     executorReadBlock->readerItemCount,
			     executorReadBlock->headerOffsetInFile);
		}
		break;

	case AoExecutorBlockKind_SingleRow:
		if (executorReadBlock->rowCount != 1)
		{
			ereport(ERROR,
					(errcode(ERRCODE_GP_INTERNAL_ERROR),
					 errmsg("Row count %d in append-only storage header is not 1 for single row",
							executorReadBlock->rowCount),
					 errdetail_appendonly_read_storage_content_header(executorReadBlock->storageRead),
					 errcontext_appendonly_read_storage_block(executorReadBlock->storageRead)));
		}
		executorReadBlock->singleRow = executorReadBlock->dataBuffer;
		executorReadBlock->singleRowLen = executorReadBlock->dataLen;
		if (Debug_appendonly_print_scan)
		{
			elog(LOG,"Append-only scan read single row for table '%s' with length %d (block offset in file = " INT64_FORMAT ")",
			     AppendOnlyStorageRead_RelationName(executorReadBlock->storageRead),
			     executorReadBlock->singleRowLen,
			     executorReadBlock->headerOffsetInFile);
		}
		break;

	default:
		elog(ERROR, "Unrecognized append-only executor block kind: %d",
			executorReadBlock->executorBlockKind);
		break;
	}
}

static bool
AppendOnlyExecutorReadBlock_GetBlockInfo(
	AppendOnlyStorageRead			*storageRead,
	AppendOnlyExecutorReadBlock		*executorReadBlock,
	bool  						isUseSplitLen)
{
	int64 blockFirstRowNum = executorReadBlock->blockFirstRowNum;

	if (!AppendOnlyStorageRead_GetBlockInfo(
									storageRead,
									&executorReadBlock->dataLen,
									&executorReadBlock->executorBlockKind,
									&executorReadBlock->blockFirstRowNum,
									&executorReadBlock->rowCount,
									&executorReadBlock->isLarge,
									&executorReadBlock->isCompressed,
									isUseSplitLen))
	{
		return false;
	}

	/* If the firstRowNum is not stored in the AOBlock,
	 * executorReadBlock->blockFirstRowNum is set to -1.
	 * Since this is properly updated by calling functions
	 * AppendOnlyExecutionReadBlock_SetPositionInfo and
	 * AppendOnlyExecutionReadBlock_FinishedScanBlock,
	 * we restore the last value when the block does not
	 * contain firstRowNum.
	 */
	if (executorReadBlock->blockFirstRowNum < 0)
	{
		executorReadBlock->blockFirstRowNum = blockFirstRowNum;
	}

	executorReadBlock->headerOffsetInFile =
		AppendOnlyStorageRead_CurrentHeaderOffsetInFile(storageRead);

	// UNDONE: Check blockFirstRowNum

	return true;
}

static void
AppendOnlyExecutionReadBlock_SetSegmentFileNum(
	AppendOnlyExecutorReadBlock		*executorReadBlock,
	int								segmentFileNum)
{
	executorReadBlock->segmentFileNum = segmentFileNum;
}

static void
AppendOnlyExecutionReadBlock_SetPositionInfo(
	AppendOnlyExecutorReadBlock		*executorReadBlock,
	int64							blockFirstRowNum)
{
	executorReadBlock->blockFirstRowNum = blockFirstRowNum;
}

static void
AppendOnlyExecutionReadBlock_FinishedScanBlock(
	AppendOnlyExecutorReadBlock		*executorReadBlock)
{
	executorReadBlock->blockFirstRowNum += executorReadBlock->rowCount;
}

/*
 * Initialize the ExecutorReadBlock once.  Assumed to be zeroed out before the call.
 */
static void
AppendOnlyExecutorReadBlock_Init(
	AppendOnlyExecutorReadBlock		*executorReadBlock,
	Relation						relation,
	MemoryContext					memoryContext,
	AppendOnlyStorageRead			*storageRead,
	int32							usableBlockSize)
{
	MemoryContext	oldcontext;

	oldcontext = MemoryContextSwitchTo(memoryContext);
	executorReadBlock->uncompressedBuffer = (uint8 *) palloc(usableBlockSize * sizeof(uint8));

	executorReadBlock->mt_bind = create_memtuple_binding(RelationGetDescr(relation));

	ItemPointerSet(&executorReadBlock->cdb_fake_ctid, 0, 0);

	executorReadBlock->storageRead = storageRead;

	executorReadBlock->memoryContext = memoryContext;

	MemoryContextSwitchTo(oldcontext);

}

/*
 * Free the space allocated inside ExexcutorReadBlock.
 */
static void
AppendOnlyExecutorReadBlock_Finish(
	AppendOnlyExecutorReadBlock *executorReadBlock)
{
	if (executorReadBlock->uncompressedBuffer)
	{
		pfree(executorReadBlock->uncompressedBuffer);
		executorReadBlock->uncompressedBuffer = NULL;
	}

	if (executorReadBlock->mt_bind)
	{
		destroy_memtuple_binding(executorReadBlock->mt_bind);
		executorReadBlock->mt_bind = NULL;
	}
}

static void
AppendOnlyExecutorReadBlock_ResetCounts(
	AppendOnlyExecutorReadBlock		*executorReadBlock)
{
	executorReadBlock->totalRowsScannned = 0;
}

static bool
AppendOnlyExecutorReadBlock_ProcessTuple(
	AppendOnlyExecutorReadBlock		*executorReadBlock,
	int64							rowNum,
	MemTuple						tuple,
	int32							tupleLen,
	int 							nkeys,
	ScanKey 						key,
	TupleTableSlot 					*slot)
{
	bool	valid = true;	// Assume for HeapKeyTestUsingSlot define.
	AOTupleId *aoTupleId = (AOTupleId*)&executorReadBlock->cdb_fake_ctid;

	AOTupleIdInit_Init(aoTupleId);
	AOTupleIdInit_segmentFileNum(aoTupleId, executorReadBlock->segmentFileNum);
	AOTupleIdInit_rowNum(aoTupleId, rowNum);

	if(slot)
	{
		/*
		 * MPP-7372: If the AO table was created before the fix for this issue, it may
		 * contain tuples with misaligned bindings. Here we check if the stored memtuple
		 * is problematic and then create a clone of the tuple with properly aligned
		 * bindings to be used by the executor.
		 */
		if (!IsAOBlockAndMemtupleAlignmentFixed(executorReadBlock->storageRead->storageAttributes.version) &&
			memtuple_has_misaligned_attribute(tuple, slot->tts_mt_bind))
		{
			/*
			 * Create a properly aligned clone of the memtuple.
			 * We p'alloc memory for the clone, so the slot is
			 * responsible for releasing the allocated memory.
			 */
			tuple = memtuple_aligned_clone(tuple, slot->tts_mt_bind, true /* upgrade */);
			Assert(tuple);
			ExecStoreMemTuple(tuple, slot, true /* shouldFree */);
		}
		else
		{
			ExecStoreMemTuple(tuple, slot, false);
		}

		slot_set_ctid(slot, &(executorReadBlock->cdb_fake_ctid));
	}

	/* skip visibility test, all tuples are visible */

	if (key != NULL)
		HeapKeyTestUsingSlot(slot, nkeys, key, valid);

	if (Debug_appendonly_print_scan_tuple && valid)
		elog(LOG,"Append-only scan tuple for table '%s' "
		     "(AOTupleId %s, tuple length %d, memtuple length %d, block offset in file " INT64_FORMAT ")",
		     AppendOnlyStorageRead_RelationName(executorReadBlock->storageRead),
		     AOTupleIdToString(aoTupleId),
		     tupleLen,
		     memtuple_get_size(tuple, executorReadBlock->mt_bind),
		     executorReadBlock->headerOffsetInFile);

	return valid;
}

static MemTuple
AppendOnlyExecutorReadBlock_ScanNextTuple(
	AppendOnlyExecutorReadBlock		*executorReadBlock,
	int 							nkeys,
	ScanKey 						key,
	TupleTableSlot 					*slot)
{
	MemTuple	tuple;

	Assert(slot);

	switch (executorReadBlock->executorBlockKind)
	{
	case AoExecutorBlockKind_VarBlock:

		/*
		 * get the next item (tuple) from the varblock
		 */
		while (true)
		{
			int		itemLen = 0;
			uint8  *itemPtr;
			int64	rowNum;

			itemPtr = VarBlockReaderGetNextItemPtr(
								&executorReadBlock->varBlockReader,
								&itemLen);

			if (itemPtr == NULL)
			{
				/* no more items in the varblock, get new buffer */
				AppendOnlyExecutionReadBlock_FinishedScanBlock(
													executorReadBlock);
				return NULL;
			}

			executorReadBlock->currentItemCount++;

			executorReadBlock->totalRowsScannned++;

			if (itemLen > 0)
			{
				tuple = (MemTuple) itemPtr;

				rowNum = executorReadBlock->blockFirstRowNum +
					     executorReadBlock->currentItemCount - INT64CONST(1);

				if (AppendOnlyExecutorReadBlock_ProcessTuple(
												executorReadBlock,
												rowNum,
												tuple,
												itemLen,
												nkeys,
												key,
												slot))
					return TupGetMemTuple(slot);
			}

		}

		/* varblock sanity check */
		if (executorReadBlock->readerItemCount !=
			executorReadBlock->currentItemCount)
			elog(NOTICE, "Varblock mismatch: Reader count %d, found %d items\n",
						executorReadBlock->readerItemCount,
						executorReadBlock->currentItemCount);
		break;

	case AoExecutorBlockKind_SingleRow:
		{
			int32 singleRowLen;

			if (executorReadBlock->singleRow == NULL)
			{
				AppendOnlyExecutionReadBlock_FinishedScanBlock(
													executorReadBlock);
				return NULL;	// Force fetching new block.
			}

			Assert(executorReadBlock->singleRowLen != 0);

			tuple = (MemTuple) executorReadBlock->singleRow;
			singleRowLen = executorReadBlock->singleRowLen;

			/*
			 * Indicated used up for scan.
			 */
			executorReadBlock->singleRow = NULL;
			executorReadBlock->singleRowLen = 0;

			executorReadBlock->totalRowsScannned++;

			if (AppendOnlyExecutorReadBlock_ProcessTuple(
											executorReadBlock,
											executorReadBlock->blockFirstRowNum,
											tuple,
											singleRowLen,
											nkeys,
											key,
											slot))
				return TupGetMemTuple(slot);
		}
		break;

	default:
		elog(ERROR, "Unrecognized append-only executor block kind: %d",
			 executorReadBlock->executorBlockKind);
		break;
	}

	AppendOnlyExecutionReadBlock_FinishedScanBlock(
										executorReadBlock);
	return NULL;	// No match.
}

static bool
AppendOnlyExecutorReadBlock_FetchTuple(
	AppendOnlyExecutorReadBlock		*executorReadBlock,
	int64							rowNum,
	int 							nkeys,
	ScanKey 						key,
	TupleTableSlot 					*slot)
{
	MemTuple	tuple;
	int 		itemNum;

	Assert(rowNum >= executorReadBlock->blockFirstRowNum);
	Assert(rowNum <=
		   executorReadBlock->blockFirstRowNum +
		   executorReadBlock->rowCount - 1);

	/*
	 * Get 0-based index to tuple.
	 */
	itemNum =
		(int)(rowNum - executorReadBlock->blockFirstRowNum);

	switch (executorReadBlock->executorBlockKind)
	{
	case AoExecutorBlockKind_VarBlock:
		{
			uint8  *itemPtr;
			int		itemLen;

			itemPtr = VarBlockReaderGetItemPtr(
								&executorReadBlock->varBlockReader,
								itemNum,
								&itemLen);
			Assert(itemPtr != NULL);

			tuple = (MemTuple) itemPtr;

			if (AppendOnlyExecutorReadBlock_ProcessTuple(
												executorReadBlock,
												rowNum,
												tuple,
												itemLen,
												nkeys,
												key,
												slot))
				return true;
		}
		break;

	case AoExecutorBlockKind_SingleRow:
		{
			Assert(itemNum == 0);
			Assert (executorReadBlock->singleRow != NULL);
			Assert(executorReadBlock->singleRowLen != 0);

			tuple = (MemTuple) executorReadBlock->singleRow;

			if (AppendOnlyExecutorReadBlock_ProcessTuple(
												executorReadBlock,
												rowNum,
												tuple,
												executorReadBlock->singleRowLen,
												nkeys,
												key,
												slot))
				return true;
		}
		break;

	default:
		elog(ERROR, "Unrecognized append-only executor block kind: %d",
			 executorReadBlock->executorBlockKind);
		break;
	}

	return false;	// No match.
}

//------------------------------------------------------------------------------

/*
 * You can think of this scan routine as get next "executor" AO block.
 */
static bool
getNextBlock(
	AppendOnlyScanDesc 	scan)
{
LABEL_START_GETNEXTBLOCK:
	if (scan->aos_need_new_split)
	{
		/*
		 * Need to open a new segment file.
		 */
		if (!SetNextFileSegForRead(scan))
			return false;
	}

	if (!AppendOnlyExecutorReadBlock_GetBlockInfo(
									&scan->storageRead,
									&scan->executorReadBlock,
									true))
	{
		/* done reading the file */
		if(scan->toCloseFile){
			CloseScannedFileSeg(scan);
		}
		scan->aos_need_new_split = true;

		return false;
	}

  //skip invalid small content blocks
  if(!scan->executorReadBlock.isLarge
          && scan->executorReadBlock.executorBlockKind == AoExecutorBlockKind_SingleRow
          && scan->executorReadBlock.rowCount==0)
  {
      //skip current block
      AppendOnlyStorageRead_SkipCurrentBlock(&scan->storageRead, true);
      goto LABEL_START_GETNEXTBLOCK;
  }else{
      AppendOnlyExecutorReadBlock_GetContents(
                &scan->executorReadBlock);
  }
	return true;
}


/* ----------------
 *		appendonlygettup - fetch next heap tuple
 *
 *		Initialize the scan if not already done; then advance to the next
 *		tuple in forward direction; return the next tuple in scan->aos_ctup,
 *		or set scan->aos_ctup.t_data = NULL if no more tuples.
 *
 * Note: the reason nkeys/key are passed separately, even though they are
 * kept in the scan descriptor, is that the caller may not want us to check
 * the scankeys.
 * ----------------
 */
static MemTuple
appendonlygettup(AppendOnlyScanDesc scan,
				 ScanDirection dir __attribute__((unused)),
				 int nkeys,
				 ScanKey key,
				 TupleTableSlot *slot)
{
	MemTuple	tuple;

	Assert(ScanDirectionIsForward(dir));
	Assert(scan->usableBlockSize > 0);

	for(;;)
	{
		if(scan->bufferDone)
		{
			/*
			 * Get the next block. We call this function until we
			 * successfully get a block to process, or finished reading
			 * all the data (all 'segment' files) for this relation.
			 */
			while(!getNextBlock(scan))
			{
				/* have we read all this relation's data. done! */
				if(scan->aos_done_all_splits)
					return NULL;
			}

			scan->bufferDone = false;
		}

		tuple = AppendOnlyExecutorReadBlock_ScanNextTuple(
												&scan->executorReadBlock,
												nkeys,
												key,
												slot);
		if (tuple != NULL)
		{
			return tuple;
		}

		/* no more items in the varblock, get new buffer */
		scan->bufferDone = true;


	}

}

static void
cancelLastBuffer(AppendOnlyInsertDesc aoInsertDesc)
{
	if (aoInsertDesc->nonCompressedData != NULL)
	{
		Assert(AppendOnlyStorageWrite_IsBufferAllocated(&aoInsertDesc->storageWrite));
		AppendOnlyStorageWrite_CancelLastBuffer(&aoInsertDesc->storageWrite);
		aoInsertDesc->nonCompressedData = NULL;
	}
	else
		Assert(!AppendOnlyStorageWrite_IsBufferAllocated(&aoInsertDesc->storageWrite));
}

static void
setupNextWriteBlock(AppendOnlyInsertDesc aoInsertDesc)
{
	Assert(aoInsertDesc->nonCompressedData == NULL);
	Assert(!AppendOnlyStorageWrite_IsBufferAllocated(&aoInsertDesc->storageWrite));

	/*
	 * when trying to fetch the next write block, we first check whether
	 * this new write block would cross the boundary of split.
	 */
	AppendOnlyStorageWrite_PadOutForSplit(&aoInsertDesc->storageWrite, aoInsertDesc->usableBlockSize);

	if(!aoInsertDesc->shouldCompress)
	{
		aoInsertDesc->nonCompressedData =
			AppendOnlyStorageWrite_GetBuffer(
									&aoInsertDesc->storageWrite,
									AoHeaderKind_SmallContent);

		/*
		 * Prepare our VarBlock for items.  Leave room for the Append-Only
		 * Storage header.
		 */
		VarBlockMakerInit(&aoInsertDesc->varBlockMaker,
						  aoInsertDesc->nonCompressedData,
						  aoInsertDesc->maxDataLen,
						  aoInsertDesc->tempSpace,
						  aoInsertDesc->tempSpaceLen);

	}
	else
	{
		/*
		 * Block oriented compression.  We also restrict the size of the
		 * buffer to leave room for the Append-Only Storage header in case the
		 * block cannot be compressed by the compress library.
		 */
		VarBlockMakerInit(&aoInsertDesc->varBlockMaker,
						  aoInsertDesc->uncompressedBuffer,
						  aoInsertDesc->maxDataLen,
						  aoInsertDesc->tempSpace,
						  aoInsertDesc->tempSpaceLen);
	}

	aoInsertDesc->bufferCount++;
}


static void
finishWriteBlock(AppendOnlyInsertDesc aoInsertDesc)
{
	int		executorBlockKind;
	int		itemCount;
	int32 	dataLen;

	executorBlockKind = AoExecutorBlockKind_VarBlock;	// Assume.

	itemCount = VarBlockMakerItemCount(&aoInsertDesc->varBlockMaker);
	if (itemCount == 0)
	{
		/*
		 * "Cancel" the last block allocation, if one.
		 */
		cancelLastBuffer(aoInsertDesc);
		return;
	}

	dataLen = VarBlockMakerFinish(&aoInsertDesc->varBlockMaker);

	aoInsertDesc->varblockCount++;

	if(!aoInsertDesc->shouldCompress)
	{
		if (itemCount == 1)
		{
			dataLen = VarBlockCollapseToSingleItem(
						   /* target */ aoInsertDesc->nonCompressedData,
					       /* source */ aoInsertDesc->nonCompressedData,
					       /* sourceLen */ dataLen);
			executorBlockKind = AoExecutorBlockKind_SingleRow;
		}

		AppendOnlyStorageWrite_FinishBuffer(
							&aoInsertDesc->storageWrite,
							dataLen,
							executorBlockKind,
							itemCount);
		aoInsertDesc->nonCompressedData = NULL;
		Assert(!AppendOnlyStorageWrite_IsBufferAllocated(&aoInsertDesc->storageWrite));

		if (Debug_appendonly_print_insert)
			elog(LOG,
			     "Append-only insert finished uncompressed block for table '%s' "
			     "(length = %d, application specific %d, item count %d, block count " INT64_FORMAT ")",
			     NameStr(aoInsertDesc->aoi_rel->rd_rel->relname),
			     dataLen,
			     executorBlockKind,
			     itemCount,
			     aoInsertDesc->bufferCount);
	}
	else
	{
		if (itemCount == 1)
		{
			dataLen = VarBlockCollapseToSingleItem(
							   /* target */ aoInsertDesc->uncompressedBuffer,
						       /* source */ aoInsertDesc->uncompressedBuffer,
						       /* sourceLen */ dataLen);
			executorBlockKind = AoExecutorBlockKind_SingleRow;
		}
		else
		{
			Assert(executorBlockKind == AoExecutorBlockKind_VarBlock);

			/*
			  * Just before finishing the attempting to compress the VarBlock, let's verify the VarBlock has integrity, honor, etc.
			  */
			if (gp_appendonly_verify_write_block)
			{
				VarBlockCheckError varBlockCheckError;

				varBlockCheckError = VarBlockIsValid(aoInsertDesc->uncompressedBuffer, dataLen);
				if (varBlockCheckError != VarBlockCheckOk)
					ereport(ERROR,
							(errcode(ERRCODE_GP_INTERNAL_ERROR),
							 errmsg("Verify block during write found VarBlock is not valid. "
									"Valid block check error %d, detail '%s'",
									varBlockCheckError,
									VarBlockGetCheckErrorStr()),
							 errdetail_appendonly_insert_block_header(aoInsertDesc),
							 errcontext_appendonly_insert_block(aoInsertDesc)));
			}
		}

		AppendOnlyStorageWrite_Content(
							&aoInsertDesc->storageWrite,
							aoInsertDesc->uncompressedBuffer,
							dataLen,
							executorBlockKind,
							itemCount);
	}

	Assert(aoInsertDesc->nonCompressedData == NULL);
	Assert(!AppendOnlyStorageWrite_IsBufferAllocated(&aoInsertDesc->storageWrite));
}

/* ----------------------------------------------------------------
 *					 append-only access method interface
 * ----------------------------------------------------------------
 */


/* ----------------
 *		appendonly_beginscan	- begin relation scan
 * ----------------
 */
AppendOnlyScanDesc
appendonly_beginscan(Relation relation, Snapshot appendOnlyMetaDataSnapshot, int nkeys, ScanKey key)
{
	AppendOnlyScanDesc	scan;
	AppendOnlyEntry		*aoentry;
	char*				comptype;
	int					complevel;

	AppendOnlyStorageAttributes *attr;

	
	StringInfoData titleBuf;

	ValidateAppendOnlyMetaDataSnapshot(&appendOnlyMetaDataSnapshot);
	
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
	scan = (AppendOnlyScanDesc) palloc0(sizeof(AppendOnlyScanDescData));

	/*
	 * Get the pg_appendonly information for this table
	 */
	aoentry = GetAppendOnlyEntry(RelationGetRelid(relation), appendOnlyMetaDataSnapshot);
	scan->aoEntry = aoentry;
	complevel = aoentry->compresslevel;
	comptype = aoentry->compresstype;

	/*
	 * initialize the scan descriptor
	 */
	scan->aos_filenamepath_maxlen = AOSegmentFilePathNameLen(relation) + 1;
	scan->aos_filenamepath = (char*)palloc(scan->aos_filenamepath_maxlen);
	scan->aos_filenamepath[0] = '\0';
	scan->usableBlockSize = aoentry->blocksize; /*AppendOnlyStorage_GetUsableBlockSize(aoentry->blocksize); */
	scan->aos_rd = relation;
	scan->appendOnlyMetaDataSnapshot = appendOnlyMetaDataSnapshot;
	scan->aos_nkeys = nkeys;
	scan->aoScanInitContext = CurrentMemoryContext;

	initStringInfo(&titleBuf);
	appendStringInfo(&titleBuf, "Scan of Append-Only Row-Oriented relation '%s'",
					 RelationGetRelationName(relation));
	scan->title = titleBuf.data;


	/*
	 * Fill in Append-Only Storage layer attributes.
	 */
	attr = &scan->storageAttributes;

	/*
	 * These attributes describe the AppendOnly format to be scanned.
	 */
  if (aoentry->compresstype == NULL || pg_strcasecmp(aoentry->compresstype, "none") == 0)
		attr->compress = false;
	else
		attr->compress = true;
	if (aoentry->compresstype != NULL)
		attr->compressType = aoentry->compresstype;
	else
		attr->compressType = "none";
  attr->compressLevel     = aoentry->compresslevel;
	attr->checksum			= aoentry->checksum;
	attr->safeFSWriteSize	= aoentry->safefswritesize;
	attr->splitsize = aoentry->splitsize;
	attr->version			= aoentry->version;

	AORelationVersion_CheckValid(attr->version);

	/*
	 * Adding a NOTOAST table attribute in 3.3.3 would require a catalog change,
	 * so in the interim we will test this with a GUC.
	 *
	 * This GUC must have the same value on write and read.
	 */
//	scan->aos_notoast = aoentry->notoast;
	scan->aos_notoast = Debug_appendonly_use_no_toast;


	// UNDONE: We are calling the static header length routine here.
	scan->maxDataLen =
					scan->usableBlockSize -
	 				AppendOnlyStorageFormat_RegularHeaderLenNeeded(scan->storageAttributes.checksum);


	/*
	 * we do this here instead of in initscan() because appendonly_rescan also calls
	 * initscan() and we don't want to allocate memory again
	 */
	if (nkeys > 0)
		scan->aos_key = (ScanKey) palloc(sizeof(ScanKeyData) * nkeys);
	else
		scan->aos_key = NULL;

	//pgstat_initstats(relation);
	initscan(scan, key);

	return scan;
}

/* ----------------
 *		appendonly_rescan		- restart a relation scan
 *
 *
 * TODO: instead of freeing resources here and reallocating them in initscan
 * over and over see which of them can be refactored into appendonly_beginscan
 * and persist there until endscan is finally reached. For now this will do.
 * ----------------
 */
void
appendonly_rescan(AppendOnlyScanDesc scan,
				  ScanKey key)
{

	CloseScannedFileSeg(scan);

	AppendOnlyStorageRead_FinishSession(&scan->storageRead);

	scan->initedStorageRoutines = false;

	AppendOnlyExecutorReadBlock_Finish(&scan->executorReadBlock);

	scan->aos_need_new_split = true;

	/*
	 * reinitialize scan descriptor
	 */
	initscan(scan, key);
}

/* ----------------
 *		appendonly_endscan	- end relation scan
 * ----------------
 */
void
appendonly_endscan(AppendOnlyScanDesc scan)
{

	RelationDecrementReferenceCount(scan->aos_rd);

	if (scan->aos_key)
		pfree(scan->aos_key);

	CloseScannedFileSeg(scan);

	AppendOnlyStorageRead_FinishSession(&scan->storageRead);

	scan->initedStorageRoutines = false;

	AppendOnlyExecutorReadBlock_Finish(&scan->executorReadBlock);

	pfree(scan->aos_filenamepath);

	pfree(scan->aoEntry);

	pfree(scan->title);

	pfree(scan);
}

/* ----------------
 *		appendonly_getnext	- retrieve next tuple in scan
 * ----------------
 */
MemTuple
appendonly_getnext(AppendOnlyScanDesc scan, ScanDirection direction, TupleTableSlot *slot)
{
	MemTuple tup = appendonlygettup(scan, direction, scan->aos_nkeys, scan->aos_key, slot);

	if (tup == NULL)
	{
		if(slot)
			ExecClearTuple(slot);

		return NULL;
	}

	pgstat_count_heap_getnext(scan->aos_rd);

	return tup;
}

/*
 * appendonly_insert_init
 *
 * before using appendonly_insert() to insert tuples we need to call
 * this function to initialize our varblock and bufferedAppend structures
 * and memory for appending data into the relation file.
 *
 * see appendonly_insert() for more specifics about inserting tuples into
 * append only tables.
 */
AppendOnlyInsertDesc
appendonly_insert_init(Relation rel, ResultRelSegFileInfo *segfileinfo)
{
	AppendOnlyInsertDesc 	aoInsertDesc;
	AppendOnlyEntry				*aoentry;
	int 							maxtupsize;
	PGFunction 				*fns;
	int 					desiredOverflowBytes = 0;
	size_t 					(*desiredCompressionSize)(size_t input);

	AppendOnlyStorageAttributes *attr;

	StringInfoData titleBuf;

	/*
	 * Get the pg_appendonly information for this table
	 */
	aoentry = GetAppendOnlyEntry(RelationGetRelid(rel), SnapshotNow);

	/*
	 * allocate and initialize the insert descriptor
	 */
	aoInsertDesc  = (AppendOnlyInsertDesc) palloc0(sizeof(AppendOnlyInsertDescData));

	aoInsertDesc->aoi_rel = rel;
	aoInsertDesc->appendOnlyMetaDataSnapshot = SnapshotNow;		
								// Writers uses this since they have exclusive access to the lock acquired with
								// LockRelationAppendOnlySegmentFile for the segment-file.

	aoInsertDesc->mt_bind = create_memtuple_binding(RelationGetDescr(rel));

	aoInsertDesc->appendFile = -1;
	aoInsertDesc->appendFilePathNameMaxLen = AOSegmentFilePathNameLen(rel) + 1;
	aoInsertDesc->appendFilePathName = (char*)palloc(aoInsertDesc->appendFilePathNameMaxLen);
	aoInsertDesc->appendFilePathName[0] = '\0';

	aoInsertDesc->bufferCount = 0;
	aoInsertDesc->insertCount = 0;
	aoInsertDesc->varblockCount = 0;
	aoInsertDesc->rowCount = 0;

	Assert(segfileinfo->segno >= 0);
	aoInsertDesc->cur_segno = segfileinfo->segno;
	aoInsertDesc->aoEntry = aoentry;

	/*
	 * Adding a NOTOAST table attribute in 3.3.3 would require a catalog change,
	 * so in the interim we will test this with a GUC.
	 *
	 * This GUC must have the same value on write and read.
	 */
//	aoInsertDesc->useNoToast = aoentry->notoast;
	aoInsertDesc->useNoToast = Debug_appendonly_use_no_toast;

	aoInsertDesc->usableBlockSize = aoentry->blocksize; /* AppendOnlyStorage_GetUsableBlockSize(aoentry->blocksize); */

	attr = &aoInsertDesc->storageAttributes;

	/*
	 * These attributes describe the AppendOnly format to be scanned.
	 */
  if (aoentry->compresstype == NULL || pg_strcasecmp(aoentry->compresstype, "none") == 0)
		attr->compress = false;
	else
		attr->compress = true;
	if (aoentry->compresstype != NULL)
		attr->compressType	= aoentry->compresstype;
	else
		attr->compressType = "none";
	attr->compressLevel	= aoentry->compresslevel;
	attr->checksum			= aoentry->checksum;
	attr->safeFSWriteSize	= aoentry->safefswritesize;
	attr->splitsize = aoentry->splitsize;
	attr->version			= aoentry->version;

	AORelationVersion_CheckValid(attr->version);

	fns = RelationGetRelationCompressionFuncs(rel);

	CompressionState *cs = NULL;
	CompressionState *verifyCs = NULL;
	if (fns)
	{
		PGFunction cons = fns[COMPRESSION_CONSTRUCTOR];
		StorageAttributes sa;

		sa.comptype = aoentry->compresstype;
		sa.complevel = aoentry->compresslevel;
		sa.blocksize = aoentry->blocksize;

		cs = callCompressionConstructor(cons, RelationGetDescr(rel),
										&sa,
										true /* compress */);
		if (gp_appendonly_verify_write_block == true)
		{
			verifyCs = callCompressionConstructor(cons, RelationGetDescr(rel),
											&sa,
											false /* decompress */);
		}

		desiredCompressionSize = cs->desired_sz;
		if (desiredCompressionSize != NULL)
		{
			/*
			 * Call the compression's desired size function to find out what additional
			 * space it requires for our block size.
			 */
			desiredOverflowBytes =
				(int)(desiredCompressionSize)(aoInsertDesc->usableBlockSize)
					- aoInsertDesc->usableBlockSize;
			Assert(desiredOverflowBytes >= 0);
		}
	}

	aoInsertDesc->storageAttributes.overflowSize = desiredOverflowBytes;

	initStringInfo(&titleBuf);
	appendStringInfo(&titleBuf, "Write of Append-Only Row-Oriented relation '%s'",
					 RelationGetRelationName(aoInsertDesc->aoi_rel));
	aoInsertDesc->title = titleBuf.data;

	AppendOnlyStorageWrite_Init(
						&aoInsertDesc->storageWrite,
						NULL,
						aoInsertDesc->usableBlockSize,
						RelationGetRelationName(aoInsertDesc->aoi_rel),
						aoInsertDesc->title,
						&aoInsertDesc->storageAttributes);

	aoInsertDesc->storageWrite.compression_functions = fns;
	aoInsertDesc->storageWrite.compressionState = cs;
	aoInsertDesc->storageWrite.verifyWriteCompressionState = verifyCs;

	if (Debug_appendonly_print_insert)
		elog(LOG,"Append-only insert initialize for table '%s' segment file %u "
		     "(compression = %s, compression type %s, compression level %d)",
		     NameStr(aoInsertDesc->aoi_rel->rd_rel->relname),
		     aoInsertDesc->cur_segno,
		     (attr->compress ? "true" : "false"),
		     (aoentry->compresstype ? aoentry->compresstype : "<none>"),
		     attr->compressLevel);

	aoInsertDesc->completeHeaderLen =
					AppendOnlyStorageWrite_CompleteHeaderLen(
										&aoInsertDesc->storageWrite,
										AoHeaderKind_SmallContent);

	aoInsertDesc->maxDataLen =
						aoInsertDesc->usableBlockSize -
						aoInsertDesc->completeHeaderLen;

	aoInsertDesc->tempSpaceLen = aoInsertDesc->usableBlockSize / 8; /* TODO - come up with a more efficient calculation */
	aoInsertDesc->tempSpace = (uint8 *) palloc(aoInsertDesc->tempSpaceLen * sizeof(uint8));
	maxtupsize = aoInsertDesc->maxDataLen - VARBLOCK_HEADER_LEN - 4;
	aoInsertDesc->toast_tuple_threshold = maxtupsize / 4; /* see tuptoaster.h for more information */
	aoInsertDesc->toast_tuple_target = maxtupsize / 4;

	/* open our current relation file segment for write */
	SetCurrentFileSegForWrite(aoInsertDesc, segfileinfo);

	Assert(aoInsertDesc->tempSpaceLen > 0);

	/*
	 * Obtain the next list of fast sequences for this relation.
	 *
	 * Even in the case of no indexes, we need to update the fast
	 * sequences, since the table may contain indexes at some
	 * point of time.
	 */
	Assert(!ItemPointerIsValid(&aoInsertDesc->fsInfo->sequence_tid));
	Assert(aoInsertDesc->fsInfo->segno == segfileinfo->segno);

	setupNextWriteBlock(aoInsertDesc);

	return aoInsertDesc;
}


/*
 *	appendonly_insert		- insert tuple into a varblock
 *
 * Note the following major differences from heap_insert
 *
 * - wal is always bypassed here.
 * - transaction information is of no interest.
 * - tuples inserted into varblocks, not via the postgresql buf/page manager.
 * - no need to pin buffers.
 *
  * The output parameter tupleOid is the OID assigned to the tuple (either here or by the
  * caller), or InvalidOid if no OID.  The header fields of *tup are updated
  * to match the stored tuple;
  */
 void
 appendonly_insert(
	 AppendOnlyInsertDesc aoInsertDesc,
	 MemTuple instup,
	 Oid *tupleOid,
	 AOTupleId *aoTupleId)
 {
	Relation		 relation = aoInsertDesc->aoi_rel;
	VarBlockByteLen	 itemLen;
	uint8			*itemPtr;
	MemTuple 		 tup = NULL;
	bool			need_toast;
	bool			isLargeContent;

	Assert(aoInsertDesc->usableBlockSize > 0 && aoInsertDesc->tempSpaceLen > 0);
	Assert(aoInsertDesc->toast_tuple_threshold > 0 && aoInsertDesc->toast_tuple_target > 0);

	Insist(RelationIsAoRows(relation));
	if (relation->rd_rel->relhasoids)
	{

		/*
		 * If the object id of this tuple has already been assigned, trust the
		 * caller.	There are a couple of ways this can happen.  At initial db
		 * creation, the backend program sets oids for tuples. When we define
		 * an index, we set the oid.  Finally, in the future, we may allow
		 * users to set their own object ids in order to support a persistent
		 * object store (objects need to contain pointers to one another).
		 */
		if (!OidIsValid(MemTupleGetOid(instup, aoInsertDesc->mt_bind)))
			MemTupleSetOid(instup, aoInsertDesc->mt_bind, GetNewOid(relation));
	}
	else
	{
		/* check there is not space for an OID */
		MemTupleNoOidSpace(instup);
	}

	if (aoInsertDesc->useNoToast)
		need_toast = false;
	else
		need_toast = (MemTupleHasExternal(instup, aoInsertDesc->mt_bind) ||
					  memtuple_get_size(instup, aoInsertDesc->mt_bind) >
					  aoInsertDesc->toast_tuple_threshold);

	/*
	 * If the new tuple is too big for storage or contains already toasted
	 * out-of-line attributes from some other relation, invoke the toaster.
	 *
	 * Note: below this point, tup is the data we actually intend to store
	 * into the relation; instup is the caller's original untoasted data.
	 */
	if (need_toast)
		tup = (MemTuple) toast_insert_or_update(relation, (HeapTuple) instup,
												NULL, aoInsertDesc->mt_bind,
												aoInsertDesc->toast_tuple_target,
												false /* errtbl is never AO */);
	else
		tup = instup;

	/*
	 * MPP-7372: If the AO table was created before the fix for this issue, it may contain
	 * tuples with misaligned bindings. Here we check if the memtuple to be stored is
	 * problematic and then create a clone of the tuple with the old (misaligned) bindings
	 * to preserve consistency.
	 */
	if (!IsAOBlockAndMemtupleAlignmentFixed(aoInsertDesc->storageAttributes.version) &&
		memtuple_has_misaligned_attribute(tup, aoInsertDesc->mt_bind))
	{
		/* Create a clone of the memtuple using misaligned bindings. */
		MemTuple tuple = memtuple_aligned_clone(tup, aoInsertDesc->mt_bind, false /* downgrade */);
		Assert(tuple);
		if(tup != instup)
		{
			pfree(tup);
		}
		tup = tuple;
	}

	/*
	 * get space to insert our next item (tuple)
	 */
	itemLen = memtuple_get_size(tup, aoInsertDesc->mt_bind);
	isLargeContent = false;

	/*
	 * If we are at the limit for append-only storage header's row count,
	 * force this VarBlock to finish.
	 */
	if (VarBlockMakerItemCount(&aoInsertDesc->varBlockMaker) >= AOSmallContentHeader_MaxRowCount)
		itemPtr = NULL;
	else
		itemPtr = VarBlockMakerGetNextItemPtr(&aoInsertDesc->varBlockMaker, itemLen);

	/*
	 * If no more room to place items in the current varblock
	 * finish it and start inserting into the next one.
	 */
	if (itemPtr == NULL)
	{
		if (VarBlockMakerItemCount(&aoInsertDesc->varBlockMaker) == 0)
		{
			/*
			 * Case #1.  The entire tuple cannot fit within a VarBlock.  It is too large.
			 */
			if (aoInsertDesc->useNoToast)
			{
				/*
				 * Indicate we need to write the large tuple as a large content
				 * multiple-block set.
				 */
				isLargeContent = true;
			}
			else
			{
				/*
				 * Use a different errcontext when user input (tuple contents) cause the
				 * error.
				 */
				ereport(ERROR,
						(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
						 errmsg("Item too long (check #1): length %d, maxBufferLen %d",
						        itemLen, aoInsertDesc->varBlockMaker.maxBufferLen),
						 errcontext_appendonly_insert_block_user_limit(aoInsertDesc)));
			}
		}
		else
		{
			/*
			 * Write out the current VarBlock to make room.
			 */
			finishWriteBlock(aoInsertDesc);
			Assert(aoInsertDesc->nonCompressedData == NULL);
			Assert(!AppendOnlyStorageWrite_IsBufferAllocated(&aoInsertDesc->storageWrite));

			/*
			 * Setup a new VarBlock.
			 */
			setupNextWriteBlock(aoInsertDesc);

			itemPtr = VarBlockMakerGetNextItemPtr(&aoInsertDesc->varBlockMaker, itemLen);

			if (itemPtr == NULL)
			{
				/*
				 * Case #2.  The entire tuple cannot fit within a VarBlock.  It is too large.
				 */
				if (aoInsertDesc->useNoToast)
				{
					/*
					 * Indicate we need to write the large tuple as a large content
					 * multiple-block set.
					 */
					isLargeContent = true;
				}
				else
				{
					/*
					 * Use a different errcontext when user input (tuple contents) cause the
					 * error.
					 */
					ereport(ERROR,
							(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
							 errmsg("Item too long (check #2): length %d, maxBufferLen %d",
							        itemLen, aoInsertDesc->varBlockMaker.maxBufferLen),
							 errcontext_appendonly_insert_block_user_limit(aoInsertDesc)));
				}
			}
		}

	}

	if (!isLargeContent)
	{
		/*
		 * We have room in the current VarBlock for the new tuple.
		 */
		Assert(itemPtr != NULL);

		if (itemLen > 0)
			memcpy(itemPtr, tup, itemLen);
	}
	else
	{
		/*
		 * Write the large tuple as a large content multiple-block set.
		 */
		Assert(itemPtr == NULL);
		Assert(!need_toast);
		Assert(instup == tup);

		/*
		 * "Cancel" the last block allocation, if one.
		 */
		cancelLastBuffer(aoInsertDesc);
		Assert(aoInsertDesc->nonCompressedData == NULL);
		Assert(!AppendOnlyStorageWrite_IsBufferAllocated(&aoInsertDesc->storageWrite));

		/*
		 * Write large content.
		 */
		AppendOnlyStorageWrite_Content(
							&aoInsertDesc->storageWrite,
							(uint8*)tup,
							itemLen,
							AoExecutorBlockKind_SingleRow,
							/* rowCount */ 1);
		Assert(aoInsertDesc->nonCompressedData == NULL);
		Assert(!AppendOnlyStorageWrite_IsBufferAllocated(&aoInsertDesc->storageWrite));

		setupNextWriteBlock(aoInsertDesc);
	}

	aoInsertDesc->insertCount++;
	pgstat_count_heap_insert(relation);

	*tupleOid = MemTupleGetOid(tup, aoInsertDesc->mt_bind);

	AOTupleIdInit_Init(aoTupleId);
	AOTupleIdInit_segmentFileNum(aoTupleId, aoInsertDesc->cur_segno);

	if (Debug_appendonly_print_insert_tuple)
	{
		elog(LOG,"Append-only insert tuple for table '%s' "
		     "(AOTupleId %s, memtuple length %d, isLargeRow %s, block count " INT64_FORMAT ")",
		     NameStr(aoInsertDesc->aoi_rel->rd_rel->relname),
		     AOTupleIdToString(aoTupleId),
		     itemLen,
		     (isLargeContent ? "true" : "false"),
		     aoInsertDesc->bufferCount);
	}

	if(tup != instup)
		pfree(tup);
}

/*
 * appendonly_insert_finish
 *
 * when done inserting all the data via appendonly_insert() we need to call
 * this function to flush all remaining data in the buffer into the file.
 */
void
appendonly_insert_finish(AppendOnlyInsertDesc aoInsertDesc)
{
	/*
	 * Finish up that last varblock.
	 */
	finishWriteBlock(aoInsertDesc);

	CloseWritableFileSeg(aoInsertDesc);

	AppendOnlyStorageWrite_FinishSession(&aoInsertDesc->storageWrite);

	pfree(aoInsertDesc->aoEntry);
	pfree(aoInsertDesc->title);
	pfree(aoInsertDesc);
}

/*
 * RelationGuessNumberOfBlocks
 *
 * Has the same meaning as RelationGetNumberOfBlocks for heap relations
 * however uses an estimation since AO relations use variable len blocks
 * which are meaningless to the optimizer.
 *
 * This function, in other words, answers the following question - "If
 * I were a heap relation, about how many blocks would I have had?"
 */
BlockNumber
RelationGuessNumberOfBlocks(double totalbytes)
{
	/* for now it's very simple */
	return (BlockNumber)(totalbytes/BLCKSZ) + 1;
}

/*
 * AppendOnlyStorageWrite_PadOutForSplit		- padding zero to split boundary.
 */
void
AppendOnlyStorageWrite_PadOutForSplit(
		AppendOnlyStorageWrite *storageWrite,
		int32 varblocksize)
{
	int64 nextWritePosition;
	int64 nextBoundaryPosition;
	int32 splitWriteRemainder;
	bool crossSplitBoundary;

	uint8 *buffer;
	int32 safeWriteSplit = storageWrite->storageAttributes.splitsize;

	nextWritePosition = BufferedAppendNextBufferPosition(&storageWrite->bufferedAppend);
	crossSplitBoundary = ((nextWritePosition / safeWriteSplit) != ((nextWritePosition + varblocksize) / safeWriteSplit));
	if (!crossSplitBoundary)
	{
		return;
	}
	nextBoundaryPosition = ((nextWritePosition + safeWriteSplit - 1) / safeWriteSplit) * safeWriteSplit;
	splitWriteRemainder = (int32)(nextBoundaryPosition - nextWritePosition);
	if (splitWriteRemainder <= 0)
	{
		return;
	}

	/*
	 * Get buffer of the remainder to pad.
	 */
	buffer = BufferedAppendGetBuffer(&storageWrite->bufferedAppend, splitWriteRemainder);
	if (buffer == NULL)
	{
		ereport(ERROR,
				(errcode(ERRCODE_GP_INTERNAL_ERROR),
				errmsg("We do not expect files to have a maximum length")));
	}
	memset(buffer, 0, splitWriteRemainder);
	BufferedAppendFinishBuffer(&storageWrite->bufferedAppend,
								splitWriteRemainder,
								splitWriteRemainder);

	if (Debug_appendonly_print_insert)
	{
		elog(LOG, "Append-only insert zero padded splitWriteRemainder for table '%s' (nextWritePosition = " INT64_FORMAT ", splitWriteRemainder = %d)",
				storageWrite->relationName,
				nextBoundaryPosition,
				splitWriteRemainder);
	}
}
