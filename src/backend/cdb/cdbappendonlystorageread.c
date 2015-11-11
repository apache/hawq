/*-------------------------------------------------------------------------
 *
 * cdbappendonlystorageread.c
 *
 * Copyright (c) 2007-2009, Greenplum inc
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"
#ifndef WIN32
#include <sys/fcntl.h>
#else
#include <io.h>
#endif
#include <sys/file.h>
#include <unistd.h>

#include "access/filesplit.h"
#include "catalog/pg_compression.h"
#include "cdb/cdbappendonlystorage.h"
#include "cdb/cdbappendonlystoragelayer.h"
#include "cdb/cdbappendonlystorageformat.h"
#include "cdb/cdbappendonlystorageread.h"
#include "utils/guc.h"
#include "cdb/cdbvars.h"


// -----------------------------------------------------------------------------
// Initialization
// -----------------------------------------------------------------------------

/*
 * Initialize AppendOnlyStorageRead.
 *
 * The AppendOnlyStorageRead data structure is initialized
 * once for a read session and can be used to read
 * Append-Only Storage Blocks from 1 or more segment files.
 *
 * The current file to read to is opened with the
 * AppendOnlyStorageRead_OpenFile routine.
 */
void AppendOnlyStorageRead_Init(
	AppendOnlyStorageRead			*storageRead,
				/* The data structure to initialize. */

	MemoryContext 					memoryContext,
				/*
				 * The memory context to use for buffers and
				 * other memory needs.  When NULL, the
				 * current memory context is used.
				 */
    int32                			maxBufferLen,
				/*
				 * The maximum Append-Only Storage Block
				 * length including all storage headers.
				 */
	char							*relationName,
				/*
				 * Name of the relation to use in system
				 * logging and error messages.
				 */

	char							*title,
				/*
				 * A phrase that better describes the purpose of the this open.
				 *
				 * The caller manages the storage for this.
				 */

	AppendOnlyStorageAttributes		*storageAttributes)
				/*
				 * The Append-Only Storage Attributes
				 * from relation creation.
				 */
{
	int		relationNameLen;
	uint8	*memory;
	int32	memoryLen;
	MemoryContext	oldMemoryContext;

	Assert(storageRead != NULL);

	// UNDONE: Range check maxBufferLen

	Assert(relationName != NULL);
	Assert(storageAttributes != NULL);

	// UNDONE: Range check fields in storageAttributes

	MemSet(storageRead, 0, sizeof(AppendOnlyStorageRead));

	storageRead->maxBufferLen = maxBufferLen;

	if (memoryContext == NULL)
		storageRead->memoryContext = CurrentMemoryContext;
	else
		storageRead->memoryContext = memoryContext;

	oldMemoryContext = MemoryContextSwitchTo(storageRead->memoryContext);

	memcpy(
		&storageRead->storageAttributes,
		storageAttributes,
		sizeof(AppendOnlyStorageAttributes));

	relationNameLen = strlen(relationName);
	storageRead->relationName = (char *) palloc(relationNameLen + 1);
	memcpy(storageRead->relationName, relationName, relationNameLen + 1);

	storageRead->title = title;

	storageRead->minimumHeaderLen =
		AppendOnlyStorageFormat_RegularHeaderLenNeeded(
									storageRead->storageAttributes.checksum);

	/*
	 * Initialize BufferedRead.
	 */
	storageRead->largeReadLen = 2 * storageRead->maxBufferLen;

	memoryLen =
		BufferedReadMemoryLen(
					storageRead->maxBufferLen,
					storageRead->largeReadLen);

	Assert(CurrentMemoryContext == storageRead->memoryContext);
	memory = (uint8*)palloc(memoryLen);

	BufferedReadInit(&storageRead->bufferedRead,
					 memory,
					 memoryLen,
					 storageRead->maxBufferLen,
					 storageRead->largeReadLen,
					 relationName);

	if (Debug_appendonly_print_scan || Debug_appendonly_print_read_block)
		elog(LOG,"Append-Only Storage Read initialize for table '%s' "
		     "(compression = %s, compression level %d, maximum buffer length %d, large read length %d)",
		     storageRead->relationName,
		     (storageRead->storageAttributes.compress ? "true" : "false"),
		     storageRead->storageAttributes.compressLevel,
		     storageRead->maxBufferLen,
		     storageRead->largeReadLen);

	storageRead->file = -1;

	MemoryContextSwitchTo(oldMemoryContext);

	storageRead->isActive = true;

}

/*
 * Return (read-only) pointer to relation name.
 */
char *AppendOnlyStorageRead_RelationName(
	AppendOnlyStorageRead			*storageRead)
{
	Assert(storageRead != NULL);
	Assert(storageRead->isActive);

	return storageRead->relationName;
}

/*
 * Return (read-only) pointer to relation name.
 */
char *AppendOnlyStorageRead_SegmentFileName(
	AppendOnlyStorageRead			*storageRead)
{
	Assert(storageRead != NULL);
	Assert(storageRead->isActive);

	return storageRead->segmentFileName;
}

/*
 * Finish using the AppendOnlyStorageRead session created with ~Init.
 */
void AppendOnlyStorageRead_FinishSession(
	AppendOnlyStorageRead			*storageRead)
				/* The data structure to finish. */
{
	MemoryContext	oldMemoryContext;

	if(!storageRead->isActive)
		return;

	oldMemoryContext = MemoryContextSwitchTo(storageRead->memoryContext);

	// UNDONE: This expects the MemoryContext to be what was used for the 'memory' in ~Init
	BufferedReadFinish(&storageRead->bufferedRead);

	if (storageRead->relationName != NULL)
	{
		pfree(storageRead->relationName);
		storageRead->relationName = NULL;
	}

	if (storageRead->segmentFileName != NULL)
	{
		pfree(storageRead->segmentFileName);
		storageRead->segmentFileName = NULL;
	}

	if (storageRead->compression_functions != NULL)
	{
		callCompressionDestructor(storageRead->compression_functions[COMPRESSION_DESTRUCTOR], storageRead->compressionState);
		pfree(storageRead->compressionState);
	}

	/* Deallocation is done.	  Go back to caller memory-context. */
	MemoryContextSwitchTo(oldMemoryContext);

	storageRead->isActive = false;

}

// -----------------------------------------------------------------------------
// Open and Close
// -----------------------------------------------------------------------------

/*
 * Do open the next segment file to read, but don't do error processing.
 *
 * This routine is responsible for seeking to the proper
 * read location given the logical EOF.
 */
static File AppendOnlyStorageRead_DoOpenFile(
	AppendOnlyStorageRead		*storageRead,

    char				 		*filePathName)
				/* The name of the segment file to open. */
{
	int		fileFlags = O_RDONLY | PG_BINARY;
	int		fileMode = 0400;
						/* File mode is S_IRUSR 00400 user has read permission */

	File	file;

	Assert(storageRead != NULL);
	Assert(storageRead->isActive);
	Assert(filePathName != NULL);

	if (Debug_appendonly_print_read_block)
	{
		elog(LOG,
			 "Append-Only storage read: opening table '%s', segment file '%s', fileFlags 0x%x, fileMode 0x%x",
			 storageRead->relationName,
			 storageRead->segmentFileName,
			 fileFlags,
			 fileMode);
	}
	/*
	 * Open the file for read.
	 */
	file = PathNameOpenFile(filePathName, fileFlags, fileMode);
	return file;
}

/*
 * Finish the open by positioning the next read and saving information.
 */
static void AppendOnlyStorageRead_FinishOpenFile(
	AppendOnlyStorageRead		*storageRead,

	File						file,
				/* The open file. */

	char						*filePathName,
				/* The name of the segment file to open. */
	int64						splitLen,
	int64						logicalEof,
	int64 offset)
				/*
				 * The snapshot version of the EOF
				 * value to use as the read end of the segment
				 * file.
				 */
{
	int64	seekResult;

	MemoryContext	oldMemoryContext;

	int		segmentFileNameLen;

	seekResult = FileSeek(file, offset, SEEK_SET);
	if (seekResult != offset)
	{
		FileClose(file);
        ereport(ERROR,
				(errcode(ERRCODE_IO_ERROR),
			     errmsg("Append-only Storage Read error on segment file '%s' for relation '%s'.  FileSeek offset = " INT64_FORMAT ".  Error code = %d (%s)"
			    		 , filePathName, storageRead->relationName, offset, errno, strerror(errno)),
			     errdetail("%s", HdfsGetLastError())));
	}

	storageRead->file = file;

	/*
	 * When reading multiple segment files, we throw away the old segment file name strings.
	 */
	oldMemoryContext = MemoryContextSwitchTo(storageRead->memoryContext);

	if (storageRead->segmentFileName != NULL)
		pfree(storageRead->segmentFileName);

	segmentFileNameLen = strlen(filePathName);
	storageRead->segmentFileName = (char *) palloc(segmentFileNameLen + 1);
	memcpy(storageRead->segmentFileName, filePathName, segmentFileNameLen + 1);

	/* Allocation is done.	Go back to caller memory-context. */
	MemoryContextSwitchTo(oldMemoryContext);

	storageRead->logicalEof = logicalEof;
	storageRead->bufferedRead.largeReadPosition = offset;

	BufferedReadSetFile(
				&storageRead->bufferedRead,
				storageRead->file,
				storageRead->segmentFileName,
				splitLen,
				logicalEof);

}

/*
 * Open the next segment file to read.
 *
 * This routine is responsible for seeking to the proper
 * read location given the logical EOF.
 */
void AppendOnlyStorageRead_OpenFile(
	AppendOnlyStorageRead		*storageRead,

    char				 		*filePathName,
				/* The name of the segment file to open. */
	int64 						splitLen,
    int64                		logicalEof,
    int64 offset,
	bool toOpenFile)
				/*
				 * The snapshot version of the EOF
				 * value to use as the read end of the segment
				 * file.
				 */
{
	File	file;

	Assert(storageRead != NULL);
	Assert(storageRead->isActive);
	Assert(filePathName != NULL);

	/*
	 * The EOF must be be greater than 0, otherwise we risk transactionally created
	 * segment files from disappearing if a concurrent write transaction aborts.
	 */
	if (logicalEof <= 0 && splitLen <= 0)
		ereport(ERROR,
				(errcode(ERRCODE_GP_INTERNAL_ERROR),
				 errmsg("Append-only Storage Read segment file '%s' EOF must be > 0 for relation '%s'",
						filePathName,
						storageRead->relationName)));
	if(toOpenFile || storageRead->file < 0 ){
		if (debug_print_split_alloc_result) {
					elog(LOG, "reopen file");
		}
		file = AppendOnlyStorageRead_DoOpenFile(
									storageRead,
									filePathName);
	}
	else
	{
		if (debug_print_split_alloc_result) {
			elog(LOG, "avoid reopen file");
		}
		file = storageRead->file;
	}
	if(file < 0)
	{
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("Append-Only Storage Read could not open segment file '%s' for relation '%s'", filePathName, storageRead->relationName),
				 errdetail("%s", HdfsGetLastError())));
	}

	AppendOnlyStorageRead_FinishOpenFile(
									storageRead,
									file,
									filePathName,
									splitLen,
									logicalEof,
									offset);
}

/*
 * Try opening the next segment file to read.
 *
 * This routine is responsible for seeking to the proper
 * read location given the logical EOF.
 */
bool AppendOnlyStorageRead_TryOpenFile(
	AppendOnlyStorageRead		*storageRead,

    char				 		*filePathName,
				/* The name of the segment file to open. */
	int64						splitLen,
    int64                		logicalEof)
				/*
				 * The snapshot version of the EOF
				 * value to use as the read end of the segment
				 * file.
				 */
{
	File	file;

	Assert(storageRead != NULL);
	Assert(storageRead->isActive);
	Assert(filePathName != NULL);
	// UNDONE: Range check logicalEof

	file = AppendOnlyStorageRead_DoOpenFile(
									storageRead,
									filePathName);
	if(file < 0)
	{
		return false;
	}

	AppendOnlyStorageRead_FinishOpenFile(
									storageRead,
									file,
									filePathName,
									splitLen,
									logicalEof,
									0);

	return true;
}

/*
 * Set a temporary read range in the current open segment file.
 *
 * The beginFileOffset must be to the beginning of an Append-Only Storage block.
 *
 * The afterFileOffset serves as the temporary EOF.  It will cause ~_GetBlockInfo to return
 * false (no more blocks) when reached.  It must be at the end of an Append-Only Storage
 * block.
 *
 * When ~_GetBlockInfo returns false (no more blocks), the temporary read range is forgotten.
 */
void AppendOnlyStorageRead_SetTemporaryRange(
	AppendOnlyStorageRead		*storageRead,
	int64						beginFileOffset,
	int64						afterFileOffset)
{
	Assert(storageRead->isActive);
	Assert(storageRead->file != -1);
	Assert(beginFileOffset >= 0);
	Assert(beginFileOffset <= storageRead->logicalEof);
	Assert(afterFileOffset >= 0);
	Assert(afterFileOffset <= storageRead->logicalEof);

	BufferedReadSetTemporaryRange(&storageRead->bufferedRead,
								  beginFileOffset,
								  afterFileOffset);
}

/*
 * Close the current segment file.
 *
 * No error if the current is already closed.
 */
void AppendOnlyStorageRead_CloseFile(
	AppendOnlyStorageRead		*storageRead)
{
	if(!storageRead->isActive)
		return;

	if (storageRead->file == -1)
		return;

	FileClose(storageRead->file);

	storageRead->file = -1;

	storageRead->logicalEof = INT64CONST(0);

	if(storageRead->bufferedRead.file >= 0)
		BufferedReadCompleteFile(&storageRead->bufferedRead);
}

// -----------------------------------------------------------------------------
// Reading Content
// -----------------------------------------------------------------------------

/*
 * Skip zero padding to next page boundary, if necessary.
 *
 * This function is called when the file system block we are scanning has
 * no more valid data but instead is padded with zero's from the position
 * we are currently in until the end of the block. The function will skip
 * to the end of block if skipLen is -1 or skip skipLen bytes otherwise.
 */
static void
AppendOnlyStorageRead_DoSkipPadding(
	AppendOnlyStorageRead		*storageRead,
	int32 						skipLen,
    bool				isUseSplitLen)
{
	int64   nextReadPosition;
	int64   nextBoundaryPosition;
	int32   safeWriteRemainder;
	bool	doSkip;
	uint8  *buffer;
	int32	availableLen;
	int32   safewrite = storageRead->storageAttributes.safeFSWriteSize;

	/* early exit if no pad used */
	if(safewrite == 0)
		return;

	nextReadPosition = BufferedReadNextBufferPosition(&storageRead->bufferedRead);
	nextBoundaryPosition =
		((nextReadPosition + safewrite - 1)/safewrite)*safewrite;
	safeWriteRemainder = (int32)(nextBoundaryPosition - nextReadPosition);

	if (safeWriteRemainder <= 0)
		doSkip = false;
	else if (skipLen == -1)
	{
		/*
		 * Skip to end of page.
		 */
		doSkip = true;
		skipLen = safeWriteRemainder;
	}
	else
		doSkip = (safeWriteRemainder < skipLen);

	if (doSkip)
	{
		/*
		 * Read through the remainder.
		 */
		buffer = BufferedReadGetNextBuffer(&storageRead->bufferedRead,
										   safeWriteRemainder,
										   &availableLen,
										   isUseSplitLen);

		/*
		 * Since our file EOF should always be a multiple of the file-system
		 * page, we do not expect a short read here.
		 */
		if (buffer == NULL)
			availableLen = 0;
		if (buffer == NULL || safeWriteRemainder != availableLen)
		{
			ereport(ERROR,
					(errcode(ERRCODE_GP_INTERNAL_ERROR),
					 errmsg("Unexpected end of file.  Expected to read %d bytes after position " INT64_FORMAT " but found %d bytes (bufferCount  " INT64_FORMAT ")\n",
							safeWriteRemainder,
							nextReadPosition,
							availableLen,
							storageRead->bufferCount)));
		}

		// UNDONE: For verification purposes, we should verify the
		// UNDONE: reminder is all zeroes.

		if (Debug_appendonly_print_scan)
			elog(LOG,"Append-only scan skipping zero padded remainder for table '%s' (nextReadPosition = " INT64_FORMAT ", safeWriteRemainder = %d)",
			     storageRead->relationName,
			     nextReadPosition,
			     safeWriteRemainder);
	}
}

/*
 * Skip zero padding to next page boundary, if necessary.
 *
 * This function is called when the file system block we are scanning has
 * no more valid data but instead is padded with zero's from the position
 * we are currently in until the end of the block. The function will skip
 * to the end of block if skipLen is -1 or skip skipLen bytes otherwise.
 */
static bool
AppendOnlyStorageRead_PositionToNextBlock(
	AppendOnlyStorageRead		*storageRead,
	int64						*headerOffsetInFile,
	uint8						**header,
	int32						*blockLimitLen,
    bool				isUseSplitLen)
{
	int32		availableLen;
	int			i;
	int64		fileRemainderLen;

	Assert(storageRead != NULL);
	Assert(header != NULL);

	/*
	 * Peek ahead just enough so we can see the Append-Only storage
	 * header.
	 *
	 * However, we need to honor the file-system page boundaries here
	 * since we do not let the length information cross the boundary.
	 */
	AppendOnlyStorageRead_DoSkipPadding(storageRead, storageRead->minimumHeaderLen,isUseSplitLen);

	*headerOffsetInFile = BufferedReadNextBufferPosition(&storageRead->bufferedRead);

	*header = BufferedReadGetNextBuffer(&storageRead->bufferedRead,
									    storageRead->minimumHeaderLen,
									    &availableLen,
									    isUseSplitLen);

	if (*header == NULL)
	{
		/* done reading the file */
		return false;
	}

	storageRead->bufferCount++;

	if ((availableLen != storageRead->minimumHeaderLen)
	    && (storageRead->bufferedRead.largeReadPosition + storageRead->bufferedRead.largeReadLen != storageRead->bufferedRead.splitLen))
		ereport(ERROR,
				(errcode(ERRCODE_GP_INTERNAL_ERROR),
				 errmsg("Expected %d bytes and got %d bytes in table %s "
				        "(segment file '%s', header offset in file = " INT64_FORMAT ", bufferCount " INT64_FORMAT ")\n",
						storageRead->minimumHeaderLen,
						availableLen,
					    storageRead->relationName,
					    storageRead->segmentFileName,
						*headerOffsetInFile,
						storageRead->bufferCount)));

	/*
	 * First check for zero padded page remainder.
	 */
	i = 0;
	while (true)
	{
		if ((*header)[i] != 0)
			break;

		i++;
		if (i >= storageRead->minimumHeaderLen)
		{
			/*
			 * Skip over zero padding caused when the append command
			 * left a partially full page.
			 */
			AppendOnlyStorageRead_DoSkipPadding(storageRead, /* indicated till end of page */ -1, isUseSplitLen);

			/*
			 * Now try to get the peek data from the new page.
			 */
			*headerOffsetInFile = BufferedReadNextBufferPosition(&storageRead->bufferedRead);

			*header = BufferedReadGetNextBuffer(&storageRead->bufferedRead,
											    storageRead->minimumHeaderLen,
											    &availableLen,
											    isUseSplitLen);

			if (*header == NULL)
			{
				/* done reading the file */
				return false;
			}

			if ((availableLen != storageRead->minimumHeaderLen)
			    && (storageRead->bufferedRead.largeReadPosition + storageRead->bufferedRead.largeReadLen != storageRead->bufferedRead.splitLen))
				ereport(ERROR,
						(errcode(ERRCODE_GP_INTERNAL_ERROR),
						 errmsg("Expected %d bytes and found %d bytes in table %s "
						 		"(segment file '%s', header offset in file = " INT64_FORMAT ", bufferCount " INT64_FORMAT ")",
								storageRead->minimumHeaderLen,
								availableLen,
								storageRead->relationName,
							    storageRead->segmentFileName,
								*headerOffsetInFile,
								storageRead->bufferCount)));
			i = 0;
		}
	}

	/*
	 * Check for zero padded page remainder.
	 * If i >= storageRead->minimumHeaderLen (16 or 8), we skip storageRead->minimumHeaderLen zeros as mentioned above.
	 * If there are more zeros but less than storageRead->minimumHeaderLen, we skip them as well.
	 * But it must be a multiple of 8 in version 2, or a multiple of 4 in version 1. If not, we should align it.
	 */
	if (i > 0)
	{
	  if (storageRead->storageAttributes.version == AORelationVersion_Original)
	  {
	    i = i / 4 * 4;
	  }
	  else if (storageRead->storageAttributes.version == AORelationVersion_Aligned64bit)
	  {
	    i = i / 8 * 8;
	  }
	  *headerOffsetInFile += i;
	  *header += i;
	  storageRead->bufferedRead.bufferOffset += i;
	}

	/*
	 * Determine the maximum boundary of the block.
	 * UNDONE: When we have a block directory, we will tighten the limit down.
	 */
	fileRemainderLen = storageRead->bufferedRead.fileLen -
		               *headerOffsetInFile;
	if (storageRead->maxBufferLen > fileRemainderLen)
		*blockLimitLen = (int32)fileRemainderLen;
	else
		*blockLimitLen = storageRead->maxBufferLen;

	return (*blockLimitLen > 0);
}


char *
AppendOnlyStorageRead_ContextStr(AppendOnlyStorageRead *storageRead)
{
	StringInfoData buf;
	
	int64	headerOffsetInFile;

	headerOffsetInFile = BufferedReadCurrentPosition(&storageRead->bufferedRead);

	initStringInfo(&buf);
	appendStringInfo(
				&buf, 
				 "%s. Append-Only segment file '%s', block header offset in file = " INT64_FORMAT ", bufferCount " INT64_FORMAT,
				 storageRead->title,
				 storageRead->segmentFileName,
				 headerOffsetInFile,
				 storageRead->bufferCount);

	return buf.data;
}

/*
 * errcontext_appendonly_read_storage_block
 *
 * Add an errcontext() line showing the table, segment file, offset in file, block count of
 * the storage block being read.
 */
int
errcontext_appendonly_read_storage_block(AppendOnlyStorageRead *storageRead)
{
	char *str;

	str = AppendOnlyStorageRead_ContextStr(storageRead);

	errcontext("%s", str);

	pfree(str);

	return 0;
}

char *
AppendOnlyStorageRead_StorageContentHeaderStr(AppendOnlyStorageRead *storageRead)
{
	uint8	*header;

	header = BufferedReadGetCurrentBuffer(&storageRead->bufferedRead);

	return AppendOnlyStorageFormat_BlockHeaderStr(
												header, 
												storageRead->storageAttributes.checksum, 
												storageRead->storageAttributes.version);
}

/*
 * errdetail_appendonly_read_storage_content_header
 *
 * Add an errdetail() line showing the Append-Only Storage header being read.
 */
int
errdetail_appendonly_read_storage_content_header(AppendOnlyStorageRead *storageRead)
{
	char *str;

	str = AppendOnlyStorageRead_StorageContentHeaderStr(storageRead);

	errdetail("%s", str);

	pfree(str);

	return 0;
}

static void AppendOnlyStorageRead_LogBlockHeader(
	AppendOnlyStorageRead		*storageRead,
	uint8						*header)
{
	char *contextStr;
	char *blockHeaderStr;

	contextStr = AppendOnlyStorageRead_ContextStr(storageRead);

	blockHeaderStr = 
			AppendOnlyStorageFormat_SmallContentHeaderStr(
													header, 
													storageRead->storageAttributes.checksum, 
													storageRead->storageAttributes.version);
	ereport(LOG,
			(errmsg("%s. %s",
					contextStr,
					blockHeaderStr)));

	pfree(contextStr);
	pfree(blockHeaderStr);
}

/*
 * Get information on the next Append-Only Storage Block.
 *
 * Return true if another block was found.	Otherwise,
 * when we have reached the end of the current segment
 * file.
 */
static bool AppendOnlyStorageRead_InternalGetBlockInfo(
	AppendOnlyStorageRead		*storageRead,
	bool isUseSplitLen)
{
	uint8  				*header;
	AOHeaderCheckError	checkError;
	int32				blockLimitLen = 0;	// Shutup compiler.
	pg_crc32			storedChecksum;
	pg_crc32			computedChecksum;

	/*
	 * Reset current* variables.
	 */

	// For efficiency, zero out.  Comment out lines that set fields to 0.
	memset(&storageRead->current, 0, sizeof(AppendOnlyStorageReadCurrent));

//	storageRead->current.headerOffsetInFile = 0;
	storageRead->current.headerKind = AoHeaderKind_None;
//	storageRead->current.actualHeaderLen = 0;
//	storageRead->current.contentLen = 0;
//	storageRead->current.overallBlockLen = 0;
//	storageRead->current.contentOffset = 0;
//	storageRead->current.executorBlockKind = 0;
//	storageRead->current.hasFirstRowNum = false;
	storageRead->current.firstRowNum = INT64CONST(-1);
//	storageRead->current.rowCount = 0;
//	storageRead->current.isLarge = false;
//	storageRead->current.isCompressed = false;
//	storageRead->current.compressedLen = 0;

	if(Debug_appendonly_print_datumstream)
		elog(LOG, "before AppendOnlyStorageRead_PositionToNextBlock, storageRead->current.headerOffsetInFile is" INT64_FORMAT "storageRead->current.overallBlockLen is %d", storageRead->current.headerOffsetInFile, storageRead->current.overallBlockLen); 

	if (!AppendOnlyStorageRead_PositionToNextBlock(
											storageRead,
											&storageRead->current.headerOffsetInFile,
											&header,
											&blockLimitLen,
											isUseSplitLen))
	{
		/* Done reading the file */
		return false;
	}

	if(Debug_appendonly_print_datumstream)
		elog(LOG, "after AppendOnlyStorageRead_PositionToNextBlock, storageRead->current.headerOffsetInFile is" INT64_FORMAT "storageRead->current.overallBlockLen is %d", storageRead->current.headerOffsetInFile, storageRead->current.overallBlockLen); 

	/*
	 * Proceed very carefully:
	 * [ 1. Verify header checksum ]
	 *   2. Examine (basic) header.
	 *   3. Examine specific header.
	 * [ 4. Verify the block checksum ]
	 */
	if (storageRead->storageAttributes.checksum)
	{
		if (!AppendOnlyStorageFormat_VerifyHeaderChecksum(
													header,
													&storedChecksum,
													&computedChecksum))
			ereport(ERROR,
			        (errmsg("Header checksum does not match.  Expected 0x%X and found 0x%X headerOffsetInFile is" INT64_FORMAT " overallBlockLen is %d",
			     		    storedChecksum,
			        		computedChecksum,
			        		storageRead->current.headerOffsetInFile,
			        		storageRead->current.overallBlockLen),
			         errdetail_appendonly_read_storage_content_header(storageRead),
			         errcontext_appendonly_read_storage_block(storageRead)));
	}

	/*
	 * Check the (basic) header information.
	 */
	checkError = AppendOnlyStorageFormat_GetHeaderInfo(
												header,
												storageRead->storageAttributes.checksum,
												&storageRead->current.headerKind,
												&storageRead->current.actualHeaderLen);
	if (checkError != AOHeaderCheckOk)
		ereport(ERROR,
		 		(errmsg("Bad append-only storage header.  Header check error %d, detail '%s'",
						(int)checkError,
						AppendOnlyStorageFormat_GetHeaderCheckErrorStr()),
				 errdetail_appendonly_read_storage_content_header(storageRead),
				 errcontext_appendonly_read_storage_block(storageRead)));

	/*
	 * Get more header since AppendOnlyStorageRead_PositionToNextBlock only gets minimum.
	 */
	if (storageRead->minimumHeaderLen < storageRead->current.actualHeaderLen)
	{
		int32 availableLen;

		header = BufferedReadGrowBuffer(&storageRead->bufferedRead,
										storageRead->current.actualHeaderLen,
										&availableLen,
										isUseSplitLen);

		if (header == NULL ||
			availableLen != storageRead->current.actualHeaderLen)
			ereport(ERROR,
					(errcode(ERRCODE_GP_INTERNAL_ERROR),
					 errmsg("Expected %d bytes and found %d bytes in table %s "
							"(segment file '%s', header offset in file = " INT64_FORMAT ", bufferCount " INT64_FORMAT ")",
							storageRead->current.actualHeaderLen,
							availableLen,
							storageRead->relationName,
							storageRead->segmentFileName,
							storageRead->current.headerOffsetInFile,
							storageRead->bufferCount)));
	}

	/*
	 * Based on the kind of header, we either have small or large content.
	 */
	switch (storageRead->current.headerKind)
	{
	case AoHeaderKind_SmallContent:
		/*
		 * Check the SmallContent header information.
		 */
		checkError =
			AppendOnlyStorageFormat_GetSmallContentHeaderInfo(
								header,
								storageRead->current.actualHeaderLen,
								storageRead->storageAttributes.checksum,
								blockLimitLen,
								&storageRead->current.overallBlockLen,
								&storageRead->current.contentOffset,
								&storageRead->current.uncompressedLen,
								&storageRead->current.executorBlockKind,
								&storageRead->current.hasFirstRowNum,
								storageRead->storageAttributes.version,
								&storageRead->current.firstRowNum,
								&storageRead->current.rowCount,
								&storageRead->current.isCompressed,
								&storageRead->current.compressedLen);
		if (checkError != AOHeaderCheckOk)
			ereport(ERROR,
					(errmsg("Bad append-only storage header of type small content. Header check error %d, detail '%s'",
							(int)checkError,
							AppendOnlyStorageFormat_GetHeaderCheckErrorStr()),
					 errdetail_appendonly_read_storage_content_header(storageRead),
					 errcontext_appendonly_read_storage_block(storageRead)));
		break;

	case AoHeaderKind_LargeContent:
		/*
		 * Check the LargeContent metadata header information.
		 */
		checkError = AppendOnlyStorageFormat_GetLargeContentHeaderInfo(
								header,
								storageRead->current.actualHeaderLen,
								storageRead->storageAttributes.checksum,
								&storageRead->current.uncompressedLen,
								&storageRead->current.executorBlockKind,
								&storageRead->current.hasFirstRowNum,
								&storageRead->current.firstRowNum,
								&storageRead->current.rowCount);
		if (checkError != AOHeaderCheckOk)
			ereport(ERROR,
					(errmsg("Bad append-only storage header of type large content. Header check error %d, detail '%s'",
							(int)checkError,
							AppendOnlyStorageFormat_GetHeaderCheckErrorStr()),
					 errdetail_appendonly_read_storage_content_header(storageRead),
					 errcontext_appendonly_read_storage_block(storageRead)));
		storageRead->current.isLarge = true;
		break;

	case AoHeaderKind_NonBulkDenseContent:
		/*
		 * Check the NonBulkDense header information.
		 */
		checkError =
			AppendOnlyStorageFormat_GetNonBulkDenseContentHeaderInfo(
								header,
								storageRead->current.actualHeaderLen,
								storageRead->storageAttributes.checksum,
								blockLimitLen,
								&storageRead->current.overallBlockLen,
								&storageRead->current.contentOffset,
								&storageRead->current.uncompressedLen,
								&storageRead->current.executorBlockKind,
								&storageRead->current.hasFirstRowNum,
								storageRead->storageAttributes.version,
								&storageRead->current.firstRowNum,
								&storageRead->current.rowCount);
		if (checkError != AOHeaderCheckOk)
			ereport(ERROR,
					(errmsg("Bad append-only storage header of type non-bulk dense content. Header check error %d, detail '%s'",
							(int)checkError,
							AppendOnlyStorageFormat_GetHeaderCheckErrorStr()),
					 errdetail_appendonly_read_storage_content_header(storageRead),
					 errcontext_appendonly_read_storage_block(storageRead)));
		break;
	
	case AoHeaderKind_BulkDenseContent:
		/*
		 * Check the BulkDenseContent header information.
		 */
		checkError =
			AppendOnlyStorageFormat_GetBulkDenseContentHeaderInfo(
								header,
								storageRead->current.actualHeaderLen,
								storageRead->storageAttributes.checksum,
								blockLimitLen,
								&storageRead->current.overallBlockLen,
								&storageRead->current.contentOffset,
								&storageRead->current.uncompressedLen,
								&storageRead->current.executorBlockKind,
								&storageRead->current.hasFirstRowNum,
								storageRead->storageAttributes.version,
								&storageRead->current.firstRowNum,
								&storageRead->current.rowCount,
								&storageRead->current.isCompressed,
								&storageRead->current.compressedLen);
		if (checkError != AOHeaderCheckOk)
			ereport(ERROR,
					(errmsg("Bad append-only storage header of type bulk dense content. Header check error %d, detail '%s'",
							(int)checkError,
							AppendOnlyStorageFormat_GetHeaderCheckErrorStr()),
					 errdetail_appendonly_read_storage_content_header(storageRead),
					 errcontext_appendonly_read_storage_block(storageRead)));
		break;

	default:
		elog(ERROR, "Unexpected Append-Only header kind %d", 
			 storageRead->current.headerKind);
		break;
	}

	if (Debug_appendonly_print_storage_headers)
	{
		AppendOnlyStorageRead_LogBlockHeader(storageRead, header);
	}

	if (storageRead->current.hasFirstRowNum)
	{
		// UNDONE: Grow buffer and read the value into firstRowNum.
	}

	if (storageRead->current.headerKind == AoHeaderKind_LargeContent)
	{
		// UNDONE: Finish the read for the information only header.
	}

	return true;
}

/*
 * Get information on the next Append-Only Storage Block.
 *
 * Return true if another block was found.	Otherwise,
 * when we have reached the end of the current segment
 * file.
 */
bool AppendOnlyStorageRead_GetBlockInfo(
	AppendOnlyStorageRead		*storageRead,

	int32						*contentLen,
				/* The total byte length of the content. */

	int 						*executorBlockKind,
				/*
				 * The executor supplied value stored in the
				 * Append-Only Storage Block header.
				 */
	int64						*firstRowNum,
				/*
				 * When the first row number for this block
				 * was explicitly set, that value is
				 * returned here.  Otherwise, INT64CONST(-1)
				 * is returned.
				 */
	int 						*rowCount,
				/* The number of rows in the content. */

	bool						*isLarge,
				/*
				 * When true, the content was longer than the
				 * maxBufferLen (i.e. blocksize) minus
				 * Append-Only Storage Block header and had
				 * to be stored in more than one storage block.
				 */
	bool						*isCompressed,
				/*
				 * When true, the content is compressed and
				 * cannot be looked at directly in the buffer.
				 */
	bool  						isUseSplitLen)
{
	bool	isNext;

	Assert(storageRead != NULL);
	Assert(storageRead->isActive);

	/*
	 * If isUseSplitLen= true and readPosition>splitLen,then this block should not belong to this split.
	 * We should read segment file for a new split.
	 * This situation will occur when the previous block is the last block of a big tuple which is larger than read split  size(128MB).
	 * It also means the last tuple cross splits and the rest of this split should handle for other vSeg not this vSeg.
	 */
	if (isUseSplitLen && storageRead->bufferedRead.largeReadPosition >= storageRead->bufferedRead.splitLen)
	   return false;

	isNext = AppendOnlyStorageRead_InternalGetBlockInfo(storageRead, isUseSplitLen);

	/*
	 * The current* variables have good values even when there is no next block.
	 */
	*contentLen = storageRead->current.uncompressedLen;
	*executorBlockKind = storageRead->current.executorBlockKind;
	*firstRowNum = storageRead->current.firstRowNum;
	*rowCount = storageRead->current.rowCount;
	*isLarge = storageRead->current.isLarge;
	*isCompressed = storageRead->current.isCompressed;

	return isNext;
}

/*
 * Return the current Append-Only Storage Block buffer.
 */
uint8 *AppendOnlyStorageRead_CurrentBuffer(
	AppendOnlyStorageRead		*storageRead)
{
	Assert(storageRead != NULL);
	Assert(storageRead->isActive);

	return BufferedReadGetCurrentBuffer(&storageRead->bufferedRead);
}

/*
 * Return the file offset of the current Append-Only Storage Block.
 */
int64 AppendOnlyStorageRead_CurrentHeaderOffsetInFile(
	AppendOnlyStorageRead		*storageRead)
{
	Assert(storageRead != NULL);
	Assert(storageRead->isActive);

	return storageRead->current.headerOffsetInFile;
}

/*
 * Return the compressed length of the content of the current Append-Only Storage Block.
 */
int64 AppendOnlyStorageRead_CurrentCompressedLen(
	AppendOnlyStorageRead		*storageRead)
{
	Assert(storageRead != NULL);
	Assert(storageRead->isActive);

	return storageRead->current.compressedLen;
}

/*
 * Return the overall block length of the current Append-Only Storage Block.
 */
int64 AppendOnlyStorageRead_OverallBlockLen(
	AppendOnlyStorageRead		*storageRead)
{
	Assert(storageRead != NULL);
	Assert(storageRead->isActive);

	return storageRead->current.overallBlockLen;
}

/*
 * Internal routine to grow the BufferedRead buffer to be the whole current block and
 * to get header and content pointers of current block.
 *
 * Since we are growing the BufferedRead buffer to the whole block, old pointers to
 * the header must be abandoned.
 *
 * Header to current block was read and verified by AppendOnlyStorageRead_InternalGetBlockInfo.
 */
static void AppendOnlyStorageRead_InternalGetBuffer(
	AppendOnlyStorageRead		*storageRead,
	uint8						**header,
	uint8						**content,
    bool 				isUseSplitLen)
{
	int32				availableLen;
	pg_crc32			storedChecksum;
	pg_crc32			computedChecksum;

	/*
	 * Verify next block is type Block.
	 */
	Assert(storageRead->current.headerKind == AoHeaderKind_SmallContent ||
		   storageRead->current.headerKind == AoHeaderKind_NonBulkDenseContent ||
		   storageRead->current.headerKind == AoHeaderKind_BulkDenseContent);

	/*
	 * Grow the buffer to the full block length to avoid any
	 * unnecessary copying by BufferedRead.
	 *
	 * Since the BufferedRead module may have to copy information around,
	 * we do not save any pointers to the prior buffer call.  This why
	 * AppendOnlyStorageFormat_GetHeaderInfo passes back the offset to the data,
	 * not a pointer.
	 */
	*header = BufferedReadGrowBuffer(&storageRead->bufferedRead,
									 storageRead->current.overallBlockLen,
									 &availableLen,
									 isUseSplitLen);

	if (storageRead->current.overallBlockLen != availableLen)
		ereport(ERROR,
				(errcode(ERRCODE_GP_INTERNAL_ERROR),
				 errmsg("Wrong buffer length.  Expected %d byte length buffer and got %d ",
						storageRead->current.overallBlockLen,
						availableLen),
				 errdetail_appendonly_read_storage_content_header(storageRead),
				 errcontext_appendonly_read_storage_block(storageRead)));

	if (storageRead->storageAttributes.checksum &&
		gp_appendonly_verify_block_checksums)
	{
		/*
		 * Now that the header has been verified, verify the block checksum
		 * in the header with the checksum of the data portion.
		 */
		if (!AppendOnlyStorageFormat_VerifyBlockChecksum(
												*header,
												storageRead->current.overallBlockLen,
												&storedChecksum,
												&computedChecksum))
			ereport(ERROR,
			     	(errmsg("Block checksum does not match.  Expected 0x%X and found 0x%X",
						     storedChecksum,
						     computedChecksum),
					 errdetail_appendonly_read_storage_content_header(storageRead),
					 errcontext_appendonly_read_storage_block(storageRead)));
	}

	*content = &((*header)[storageRead->current.contentOffset]);
}

/*
 * Get a pointer to the small non-compressed content.
 *
 * This interface provides a pointer directly into the
 * read buffer for efficient data use.
 *
 */
uint8 *AppendOnlyStorageRead_GetBuffer(
	AppendOnlyStorageRead		*storageRead,
	bool isUseSplitLen)
{
	uint8  		*header;
	uint8		*content;

	Assert(storageRead != NULL);
	Assert(storageRead->isActive);

	/*
	 * Verify next block is a "small" non-compressed block.
	 */
	Assert(storageRead->current.headerKind == AoHeaderKind_SmallContent ||
		   storageRead->current.headerKind == AoHeaderKind_NonBulkDenseContent ||
		   storageRead->current.headerKind == AoHeaderKind_BulkDenseContent);
	Assert(!storageRead->current.isLarge);
	Assert(!storageRead->current.isCompressed);

	/*
	 * Fetch pointers to content.
	 */
	AppendOnlyStorageRead_InternalGetBuffer(
									storageRead,
									&header,
									&content,
									isUseSplitLen);

	return content;
}

/*
 * Copy the large and/or decompressed content out.
 *
 * The contentOutLen parameter value must match the contentLen
 * from the AppendOnlyStorageReadGetBlockInfo call.
 *
 * Note this routine will work for small non-compressed content, too.
 */
void AppendOnlyStorageRead_Content(
	AppendOnlyStorageRead		*storageRead,

	uint8						*contentOut,
			/* The memory to receive the contiguous content. */

	int32						contentOutLen,
			/* The byte length of the contentOut buffer. */
	bool 						isUseSplitLen)
{
	Assert(storageRead != NULL);
	Assert(storageRead->isActive);
	Assert(contentOutLen == storageRead->current.uncompressedLen);

	if (storageRead->current.isLarge)
	{
		int64		largeContentPosition;
						// Position of the large content metadata block.

		int32		largeContentLen;
						// Total length of the large content.

		int32		remainingLargeContentLen;
						// The remaining number of bytes to read for the large content.

		uint8		*contentNext;
						// Pointer inside the contentOut buffer to put the next byte.

		int32		regularBlockReadCount;
						// Number of regular blocks read after the metadata block.

		int32		regularContentLen;
						// Length of the current regular block's content.

		/*
		 * Large content.
		 *
		 * We have the LargeContent "metadata" AO block with the total length (already
		 * read) followed by N SmallContent blocks with the fragments of the large content.
		 */


		/*
		 * Save any values needed from the current* members since they will be modifed
		 * as we read the regular blocks.
		 */
		largeContentPosition = storageRead->current.headerOffsetInFile;
		largeContentLen = storageRead->current.uncompressedLen;

		/*
		 * Loop to read regular blocks.
		 */
		contentNext = contentOut;
		remainingLargeContentLen = largeContentLen;
		regularBlockReadCount = 0;
		while (true)
		{
			/*
			 * Read next regular block.
			 */
			regularBlockReadCount++;
			if (!AppendOnlyStorageRead_InternalGetBlockInfo(storageRead, false))
			{
				/*
				 * Unexpected end of file.
				 */
				ereport(ERROR,
						(errcode(ERRCODE_GP_INTERNAL_ERROR),
						 errmsg("Unexpected end of file trying to read block %d of large content in segment file '%s' of table '%s'.  "
						        "Large content metadata block is at position " INT64_FORMAT "  "
						        "Large content length %d",
								regularBlockReadCount,
								storageRead->segmentFileName,
								storageRead->relationName,
								largeContentPosition,
								largeContentLen)));
			}
			if (storageRead->current.headerKind != AoHeaderKind_SmallContent)
			{
				/*
				 * Unexpected headerKind.
				 */
				ereport(ERROR,
						(errcode(ERRCODE_GP_INTERNAL_ERROR),
						 errmsg("Expected header kind 'Block' for block %d of large content in segment file '%s' of table '%s'.  "
								"Large content metadata block is at position " INT64_FORMAT "  "
								"Large content length %d",
								regularBlockReadCount,
								storageRead->segmentFileName,
								storageRead->relationName,
								largeContentPosition,
								largeContentLen)));
			}
			Assert(!storageRead->current.isLarge);

			regularContentLen = storageRead->current.uncompressedLen;
		   	remainingLargeContentLen -= regularContentLen;
			if (remainingLargeContentLen < 0)
			{
				/*
				 * Too much data found???
				 */
				ereport(ERROR,
						(errcode(ERRCODE_GP_INTERNAL_ERROR),
						 errmsg("Too much data found after reading %d blocks for large content in segment file '%s' of table '%s'.  "
								"Large content metadata block is at position " INT64_FORMAT "  "
								"Large content length %d; extra data length %d",
								regularBlockReadCount,
								storageRead->segmentFileName,
								storageRead->relationName,
								largeContentPosition,
								largeContentLen,
								-remainingLargeContentLen)));
			}

			/*
			 * We can safely recurse one level here.
			 */
			AppendOnlyStorageRead_Content(
									storageRead,
									contentNext,
									regularContentLen,
									isUseSplitLen);

			if (remainingLargeContentLen == 0)
				break;

			/*
			 * Advance our pointer inside the contentOut buffer to put the next bytes.
			 */
			contentNext += regularContentLen;
		}
	}
	else
	{
		uint8		*header;
		uint8		*content;

		/*
		 * "Small" content in one regular block.
		 */

		/*
		 * Fetch pointers to content.
		 */
		AppendOnlyStorageRead_InternalGetBuffer(
										storageRead,
										&header,
										&content,
										isUseSplitLen);

		if (!storageRead->current.isCompressed)
		{
			/*
			 * Not compressed.
			 */
			memcpy(
				contentOut,
				content,
				storageRead->current.uncompressedLen);

			if (Debug_appendonly_print_scan)
				elog(LOG,
					 "Append-only Storage Read non-compressed block for table '%s' "
					 "(length = %d, segment file '%s', header offset in file = " INT64_FORMAT ", block count " INT64_FORMAT ")",
				     storageRead->relationName,
				     storageRead->current.uncompressedLen,
				     storageRead->segmentFileName,
				     storageRead->current.headerOffsetInFile,
				     storageRead->bufferCount);
		}
		else
		{
		  /*
		   * Compressed.
		   */

		  PGFunction	  decompressor;
		  PGFunction	 *cfns = storageRead->compression_functions;

			if (cfns == NULL)
				decompressor = NULL;
			else
				decompressor = cfns[COMPRESSION_DECOMPRESS];

		  gp_decompress_new(
		  		content,				// Compressed data in block.
		  		storageRead->current.compressedLen,
		  		contentOut,
		  		storageRead->current.uncompressedLen,
		  		decompressor,
		  		storageRead->compressionState,
		  		storageRead->bufferCount);

			if (Debug_appendonly_print_scan)
				elog(LOG,
				     "Append-only Storage Read decompressed block for table '%s' "
					 "(compressed length %d, uncompressed length = %d, segment file '%s', "
					 "header offset in file = " INT64_FORMAT ", block count " INT64_FORMAT ")",
				     storageRead->relationName,
				     AppendOnlyStorageFormat_GetCompressedLen(header),
				     storageRead->current.uncompressedLen,
				     storageRead->segmentFileName,
				     storageRead->current.headerOffsetInFile,
				     storageRead->bufferCount);
		}
	}


}

/*
 * Skip the current block found with ~_GetBlockInfo.
 *
 * Do not decompress the block contents.
 *
 * Call this routine instead of calling ~_GetBuffer or ~_Contents that look at contents. Useful
 * when the desired row(s) are not within the row range of the current block.
 *
 */
void AppendOnlyStorageRead_SkipCurrentBlock(
	AppendOnlyStorageRead		*storageRead, bool isUseSplitLen)
{
	Assert(storageRead != NULL);
	Assert(storageRead->isActive);

	if (storageRead->current.isLarge)
	{
		int64		largeContentPosition;
						// Position of the large content metadata block.

		int32		largeContentLen;
						// Total length of the large content.

		int32		remainingLargeContentLen;
						// The remaining number of bytes to read for the large content.

		int32		regularBlockReadCount;
						// Number of regular blocks read after the metadata block.

		int32		regularContentLen;
						// Length of the current regular block's content.

		/*
		 * Large content.
		 *
		 * We have the LargeContent "metadata" AO block with the total length (already
		 * read) followed by N SmallContent blocks with the fragments of the large content.
		 */


		/*
		 * Save any values needed from the current* members since they will be modifed
		 * as we read the regular blocks.
		 */
		largeContentPosition = storageRead->current.headerOffsetInFile;
		largeContentLen = storageRead->current.uncompressedLen;

		/*
		 * Loop to read regular blocks.
		 */
		remainingLargeContentLen = largeContentLen;
		regularBlockReadCount = 0;
		while (true)
		{
			/*
			 * Read next regular block.
			 */
			regularBlockReadCount++;
			if (!AppendOnlyStorageRead_InternalGetBlockInfo(storageRead, isUseSplitLen))
			{
				/*
				 * Unexpected end of file.
				 */
				ereport(ERROR,
						(errcode(ERRCODE_GP_INTERNAL_ERROR),
						 errmsg("Unexpected end of file trying to read block %d of large content in segment file '%s' of table '%s'.  "
								"Large content metadata block is at position " INT64_FORMAT "  "
								"Large content length %d",
								regularBlockReadCount,
								storageRead->segmentFileName,
								storageRead->relationName,
								largeContentPosition,
								largeContentLen)));
			}
			if (storageRead->current.headerKind != AoHeaderKind_SmallContent)
			{
				/*
				 * Unexpected headerKind.
				 */
				ereport(ERROR,
						(errcode(ERRCODE_GP_INTERNAL_ERROR),
						 errmsg("Expected header kind 'Block' for block %d of large content in segment file '%s' of table '%s'.  "
								"Large content metadata block is at position " INT64_FORMAT "  "
								"Large content length %d",
								regularBlockReadCount,
								storageRead->segmentFileName,
								storageRead->relationName,
								largeContentPosition,
								largeContentLen)));
			}
			Assert(!storageRead->current.isLarge);

			regularContentLen = storageRead->current.uncompressedLen;
			remainingLargeContentLen -= regularContentLen;
			if (remainingLargeContentLen < 0)
			{
				/*
				 * Too much data found???
				 */
				ereport(ERROR,
						(errcode(ERRCODE_GP_INTERNAL_ERROR),
						 errmsg("Too much data found after reading %d blocks for large content in segment file '%s' of table '%s'.	"
								"Large content metadata block is at position " INT64_FORMAT "  "
								"Large content length %d; extra data length %d",
								regularBlockReadCount,
								storageRead->segmentFileName,
								storageRead->relationName,
								largeContentPosition,
								largeContentLen,
								-remainingLargeContentLen)));
			}

			/*
			 * Since we are skipping, we do not use the compressed or uncompressed
			 * content.
			 */

			if (remainingLargeContentLen == 0)
				break;
		}
	}
	else
	{
		uint8		*header;
		uint8		*content;

		/*
		 * "Small" content in one regular block.
		 */

		/*
		 * Fetch pointers to content.
		 *
		 * Since we are skipping, we do not look at the content.
		 */
		AppendOnlyStorageRead_InternalGetBuffer(
										storageRead,
										&header,
										&content,
										true);
	}

}
