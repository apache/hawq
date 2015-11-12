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
 * cdbparquetstorageread.c
 *
 *  Created on: Sep 26, 2013
 *      Author: malili
 */
#include "cdb/cdbparquetstorageread.h"
#include "cdb/cdbparquetfooterserializer.h"
#include "sys/fcntl.h"
#include "utils/guc.h"

static File ParquetStorageRead_DoOpenFile(ParquetStorageRead *storageRead,
		char *filePathName);

static void ParquetStorageRead_FinishOpenFile(ParquetStorageRead *storageRead,
		File fileHandlerfordata, File fileHandlerforfooter, char *filePathName,
		int64 logicalEof, TupleDesc tableAttrs);

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
		char *filePathName, int64 logicalEof, TupleDesc tableAttrs) {
	File fileHandlerfordata;
	File fileHandlerforfooter;

	Assert(storageRead != NULL);
	Assert(storageRead->isActive);
	Assert(filePathName != NULL);

	/*
	 * The EOF must be be greater than 0, otherwise we risk transactionally created
	 * segment files from disappearing if a concurrent write transaction aborts.
	 */
	if (logicalEof == 0)
		ereport(ERROR,
				(errcode(ERRCODE_GP_INTERNAL_ERROR), errmsg("parquet storage read segment file '%s' eof must be > 0 for relation '%s'", filePathName, storageRead->relationName)));

	fileHandlerfordata = ParquetStorageRead_DoOpenFile(storageRead,
			filePathName);
	if (fileHandlerfordata < 0) {
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("file open error in file '%s' for relation '%s': %s"
						 , filePathName, storageRead->relationName, strerror(errno)),
				 errdetail("%s", HdfsGetLastError())));
	}

	fileHandlerforfooter = ParquetStorageRead_DoOpenFile(storageRead,
			filePathName);
	if (fileHandlerforfooter < 0) {
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("file open error for footer processing "
						 "in file '%s' for relation '%s': %s"
						 , filePathName, storageRead->relationName, strerror(errno)),
				 errdetail("%s", HdfsGetLastError())));
	}

	ParquetStorageRead_FinishOpenFile(storageRead, fileHandlerfordata,
			fileHandlerforfooter, filePathName, logicalEof, tableAttrs);
}

/*
 * Do open the next segment file to read, but don't do error processing.
 *
 */
static File ParquetStorageRead_DoOpenFile(ParquetStorageRead *storageRead,
		char *filePathName) {
	int fileFlags = O_RDONLY | PG_BINARY;
	int fileMode = 0400; /* File mode is S_IRUSR 00400 user has read permission */

	File file;

	Assert(storageRead != NULL);
	Assert(storageRead->isActive);
	Assert(filePathName != NULL);

	if (Debug_appendonly_print_read_block) {
		elog(
				LOG,
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
static void ParquetStorageRead_FinishOpenFile(ParquetStorageRead *storageRead,
		File fileHandlerfordata, File fileHandlerforfooter, char *filePathName,
		int64 logicalEof, TupleDesc tableAttrs) {
	MemoryContext oldMemoryContext;
	int segmentFileNameLen;

	oldMemoryContext = MemoryContextSwitchTo(storageRead->memoryContext);

	readParquetFooter(fileHandlerforfooter, &(storageRead->parquetMetadata),
			&(storageRead->footerProtocol), logicalEof, filePathName);

	if (checkAndSyncMetadata(storageRead->parquetMetadata, tableAttrs) == false) {
		ereport(ERROR,
				(errcode(ERRCODE_GP_INTERNAL_ERROR),
						errmsg("parquet file error: metadata not correct for relation '%s'",
						storageRead->relationName)));
	}

	storageRead->file = fileHandlerfordata;
	storageRead->fileHandlerForFooter = fileHandlerforfooter;
	storageRead->rowGroupCount = storageRead->parquetMetadata->blockCount;
	storageRead->rowGroupProcessedCount = 0;

	/*
	 * When reading multiple segment files, we throw away the old segment file name strings.
	 */
	if (storageRead->segmentFileName != NULL)
		pfree(storageRead->segmentFileName);

	segmentFileNameLen = strlen(filePathName);
	storageRead->segmentFileName = (char *) palloc0(segmentFileNameLen + 1);
	memcpy(storageRead->segmentFileName, filePathName, segmentFileNameLen + 1);

	/* Allocation is done.	Go back to caller memory-context. */
	MemoryContextSwitchTo(oldMemoryContext);
}

/*
 * Close the current segment file.
 *
 * No error if the current is already closed.
 */
void ParquetStorageRead_CloseFile(ParquetStorageRead *storageRead) {
	MemoryContext oldMemoryContext;
	if (!storageRead->isActive)
		return;

	if (storageRead->file == -1)
		return;

	FileClose(storageRead->file);

	storageRead->file = -1;

	Assert(storageRead->fileHandlerForFooter != -1);
	FileClose(storageRead->fileHandlerForFooter);
	storageRead->fileHandlerForFooter = -1;

	oldMemoryContext = MemoryContextSwitchTo(storageRead->memoryContext);

	/*free storageRead->parquetMetadata*/
	if (storageRead->parquetMetadata != NULL) {
		freeParquetMetadata(storageRead->parquetMetadata);
		storageRead->parquetMetadata = NULL;
	}

	storageRead->preRead = false;

	MemoryContextSwitchTo(oldMemoryContext);
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
void
ParquetStorageRead_Init(
		ParquetStorageRead *storageRead,
		MemoryContext memoryContext,
		char *relationName,
		AppendOnlyStorageAttributes *storageAttributes)
{
	int relationNameLen;
	MemoryContext oldMemoryContext;

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
		sizeof(AppendOnlyStorageAttributes));

	relationNameLen = strlen(relationName);
	storageRead->relationName = (char *) palloc0(relationNameLen + 1);
	strcpy(storageRead->relationName, relationName);

	Assert(CurrentMemoryContext == storageRead->memoryContext);

	if (Debug_appendonly_print_scan || Debug_appendonly_print_read_block)
		elog(LOG,"Parquet Storage Read initialize for table '%s' "
		"(compression = %s, compression level %d)",
		storageRead->relationName,
		(storageRead->storageAttributes.compress ? "true" : "false"),
		storageRead->storageAttributes.compressLevel);

	storageRead->file = -1;
	storageRead->fileHandlerForFooter = -1;

	storageRead->preRead = false;

	MemoryContextSwitchTo(oldMemoryContext);

	storageRead->isActive = true;
}

/**
 * Free the contents in storage read
 */
void
ParquetStorageRead_FinishSession(ParquetStorageRead *storageRead)
{
	MemoryContext oldContext;

	if(!storageRead->isActive)
		return;

	oldContext = MemoryContextSwitchTo(storageRead->memoryContext);

	if(storageRead->relationName != NULL)
	{
		pfree(storageRead->relationName);
		storageRead->relationName = NULL;
	}

	if (storageRead->segmentFileName != NULL)
	{
		pfree(storageRead->segmentFileName);
		storageRead->segmentFileName = NULL;
	}

	freeFooterProtocol(storageRead->footerProtocol);
	storageRead->footerProtocol = NULL;

	MemoryContextSwitchTo(oldContext);

	storageRead->isActive = false;
}
