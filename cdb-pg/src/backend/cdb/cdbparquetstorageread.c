/*
 * cdbparquetstorageread.c
 *
 *  Created on: Sep 26, 2013
 *      Author: malili
 */
#include "cdb/cdbparquetstorageread.h"
#include "sys/fcntl.h"
#include "utils/guc.h"

static File ParquetStorageRead_DoOpenFile(ParquetStorageRead *storageRead,
		char *filePathName);

static void ParquetStorageRead_FinishOpenFile(ParquetStorageRead *storageRead,
	File file, char *filePathName, int64 logicalEof, TupleDesc tableAttrs);

/*
 * Open the next segment file to read.
 *
 * This routine is responsible for seeking to the proper
 * read location given the logical EOF.
 *
 * @filePathName:		The name of the segment file to open.
 * @logicalEof:			The snapshot version of the EOF  value to use as the read
 * 						end of the segment file.
 */
void ParquetStorageRead_OpenFile(ParquetStorageRead *storageRead,
		char *filePathName, int64 logicalEof, TupleDesc tableAttrs)
{
	File	file;

	Assert(storageRead != NULL);
	Assert(storageRead->isActive);
	Assert(filePathName != NULL);

	/*
	 * The EOF must be be greater than 0, otherwise we risk transactionally created
	 * segment files from disappearing if a concurrent write transaction aborts.
	 */
	if (logicalEof == 0)
		ereport(ERROR,
				(errcode(ERRCODE_GP_INTERNAL_ERROR),
				 errmsg("parquet storage read segment file '%s' eof must be > 0 for relation '%s'",
						filePathName,
						storageRead->relationName)));

	file = ParquetStorageRead_DoOpenFile(
									storageRead,
									filePathName);
	if(file < 0)
	{
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("file open error in file '%s' for relation '%s': %s"
						 , filePathName, storageRead->relationName, strerror(errno)),
				 errdetail("%s", HdfsGetLastError())));
	}

	ParquetStorageRead_FinishOpenFile(
									storageRead,
									file,
									filePathName,
									logicalEof,
									tableAttrs);
}


/*
 * Do open the next segment file to read, but don't do error processing.
 *
 */
static File ParquetStorageRead_DoOpenFile(
		ParquetStorageRead *storageRead,
		char *filePathName)
{
	int		fileFlags = O_RDONLY | PG_BINARY;
	int		fileMode = 0400; /* File mode is S_IRUSR 00400 user has read permission */

	File	file;

	Assert(storageRead != NULL);
	Assert(storageRead->isActive);
	Assert(filePathName != NULL);

	if (Debug_appendonly_print_read_block)
	{
		elog(LOG,
			 "Parquet storage read: opening table '%s', segment file '%s', fileFlags 0x%x, fileMode 0x%x",
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
 * @file			The open file
 * @filePathName	The name of the segment file to open
 * @logicalEof		The snapshot version of the EOF value to use as the read end of
 * 					the segment file
 */
static void ParquetStorageRead_FinishOpenFile(
	ParquetStorageRead		*storageRead,
	File					file,
	char					*filePathName,
	int64					logicalEof,
	TupleDesc 				tableAttrs)
{
	MemoryContext	oldMemoryContext;
	int				segmentFileNameLen;

	readParquetFooter(file, &(storageRead->parquetMetadata), logicalEof, filePathName);

	if(checkAndSyncMetadata(storageRead->parquetMetadata, tableAttrs) == false){
		ereport(ERROR,
				(errcode(ERRCODE_GP_INTERNAL_ERROR),
					errmsg("parquet file error: metadata not correct for relation '%s'",
					storageRead->relationName)));
	}

	storageRead->file = file;
	storageRead->rowGroupCount = storageRead->parquetMetadata->blockCount;
	storageRead->rowGroupProcessedCount = 0;

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
}

/*
 * Close the current segment file.
 *
 * No error if the current is already closed.
 */
void ParquetStorageRead_CloseFile(ParquetStorageRead *storageRead)
{
	if(!storageRead->isActive)
		return;

	if (storageRead->file == -1)
		return;

	FileClose(storageRead->file);

	storageRead->file = -1;

}


/*
 * Initialize ParquetStorageRead.
 *
 * The ParquetStorageRead data structure is initialized once for a read and can be used to read
 * Parquet Storage Blocks from 1 or more segment files.
 * The current file to read to is opened with the ParquetStorageRead_OpenFile routine.
 *
 * @storageRead			The data structure to initialize
 * @memoryContext		The memory context to use for buffers and other memory needs.  When NULL,
 * 						the current memory context is used.
 * @relationName		Name of the relation to use in system logging and error messages.
 * @title				A phrase that better describes the purpose of the this open. The caller
 * 						manages the storage for this.
 * @storageAttributes	The Parquet Storage Attributes from relation creation.
 */
void ParquetStorageRead_Init(
		ParquetStorageRead *storageRead,
		MemoryContext memoryContext,
		char *relationName,
		char *title,
		ParquetStorageAttributes *storageAttributes)
{
	int		relationNameLen;
	uint8	*memory;
	int32	memoryLen = 1024;
	MemoryContext	oldMemoryContext;

	Assert(storageRead != NULL);

	Assert(relationName != NULL);
	Assert(storageAttributes != NULL);

	MemSet(storageRead, 0, sizeof(ParquetStorageRead));

	if (memoryContext == NULL)
		storageRead->memoryContext = CurrentMemoryContext;
	else
		storageRead->memoryContext = memoryContext;

	oldMemoryContext = MemoryContextSwitchTo(storageRead->memoryContext);

	memcpy(
		&storageRead->storageAttributes,
		storageAttributes,
		sizeof(ParquetStorageAttributes));

	relationNameLen = strlen(relationName);
	storageRead->relationName = (char *) palloc(relationNameLen + 1);
	memcpy(storageRead->relationName, relationName, relationNameLen + 1);

	storageRead->title = title;

	Assert(CurrentMemoryContext == storageRead->memoryContext);
	memory = (uint8*)palloc(memoryLen);

	if (Debug_appendonly_print_scan || Debug_appendonly_print_read_block)
		elog(LOG,"Parquet Storage Read initialize for table '%s' "
		     "(compression = %s, compression level %d)",
		     storageRead->relationName,
		     (storageRead->storageAttributes.compress ? "true" : "false"),
		     storageRead->storageAttributes.compressLevel);

	storageRead->file = -1;

	MemoryContextSwitchTo(oldMemoryContext);

	storageRead->isActive = true;
}

