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

/*-------------------------------------------------------------------------
 *
 * cdbmirroredbufferpool.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <fcntl.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#include <signal.h>

#include "access/xlogmm.h"
#include "cdb/cdbmirroredbufferpool.h"
#include "cdb/cdbpersistenttablespace.h"
#include "cdb/cdbpersistentstore.h"
#include "cdb/cdbvars.h"
#include "cdb/cdbutil.h"
#include "storage/bufmgr.h"
#include "storage/fd.h"
#include "storage/smgr.h"
#include "utils/guc.h"
#include "utils/memutils.h"
#include "utils/palloc.h"
#include "cdb/cdbpersistentfilesysobj.h"
#include "cdb/cdbpersistentrelfile.h"

static int32 writeBufLen = 0;
static char	*writeBuf = NULL;

/*
 * Open a relation for mirrored write.
 */
static void MirroredBufferPool_DoOpen(
	MirroredBufferPoolOpen 		*open,
				/* The resulting open struct. */

	RelFileNode 				*relFileNode,
				/* The tablespace, database, and relation OIDs for the open. */

	uint32						segmentFileNum,
				/* Which segment file. */

	char						*relationName,
				/* For tracing only.  Can be NULL in some execution paths. */
	bool						create,

	int 						*primaryError)
{
	int		fileFlags = O_RDWR | PG_BINARY;
	int		fileMode = 0600;
						/*
						 * File mode is S_IRUSR 00400 user has read permission
						 *               + S_IWUSR 00200 user has write permission
						 */

	char *filespaceLocation = NULL;

	Assert(open != NULL);
	
	*primaryError = 0;

	if (create)
		fileFlags = O_CREAT | O_RDWR | PG_BINARY;

	PersistentTablespace_GetFilespacePath(
										relFileNode->spcNode,
										FALSE,
										&filespaceLocation);
	
	if (Debug_persistent_print && create)
	{
		SUPPRESS_ERRCONTEXT_DECLARE;

		SUPPRESS_ERRCONTEXT_PUSH();

		elog(Persistent_DebugPrintLevel(), 
			 "MirroredBufferPool_DoOpen: Special open %u/%u/%u --> create %s, "
			 "primary filespace location %s ",
			 relFileNode->spcNode,
			 relFileNode->dbNode,
			 relFileNode->relNode,
			 (create ? "true" : "false"),
			 (filespaceLocation == NULL) ? "<null>" : filespaceLocation);

		SUPPRESS_ERRCONTEXT_POP();
	}
	
	MemSet(open, 0, sizeof(MirroredBufferPoolOpen));
	open->primaryFile = -1;
	
	open->relFileNode = *relFileNode;
	open->segmentFileNum = segmentFileNum;

	open->create = create;
	if (true)
	{
		char *path;

		path = (char*)palloc(MAXPGPATH + 1);

		/*
		 * Do the primary work first so we don't leave files on the mirror or have an
		 * open to clean up.
		 */
		FormRelfilePath(path,
						filespaceLocation,
						relFileNode,
						segmentFileNum);

		errno = 0;
		
		open->primaryFile = PathNameOpenFile(path, fileFlags, fileMode);
				
		if (open->primaryFile < 0)
		{
			*primaryError = errno;
		}

		pfree(path);
	}
	
	if (*primaryError != 0)
	{
		open->isActive = false;
	}
	else
	{
		open->isActive = true;
	}

	if (filespaceLocation != NULL)
		pfree(filespaceLocation);
}

void MirroredBufferPool_Open(
	MirroredBufferPoolOpen *open,
				/* The resulting open struct. */

	RelFileNode 	*relFileNode,
				/* The tablespace, database, and relation OIDs for the open. */

	uint32			segmentFileNum,
				/* Which segment file. */

	char			*relationName,
				/* For tracing only.  Can be NULL in some execution paths. */

	int 			*primaryError)

{
	MIRROREDLOCK_BUFMGR_DECLARE;

	// -------- MirroredLock ----------
	MIRROREDLOCK_BUFMGR_LOCK;

	MirroredBufferPool_DoOpen(
							open, 
							relFileNode,
							segmentFileNum,
							relationName,
							/* create */ false,
							primaryError);

	MIRROREDLOCK_BUFMGR_UNLOCK;
	// -------- MirroredLock ----------
}

void MirroredBufferPool_Create(
	MirroredBufferPoolOpen 		*open,
				/* The resulting open struct. */

	RelFileNode 				*relFileNode,
				/* The tablespace, database, and relation OIDs for the open. */

	uint32						segmentFileNum,
				/* Which segment file. */

	char						*relationName,
				/* For tracing only.  Can be NULL in some execution paths. */

	int 						*primaryError)

{
	MIRROREDLOCK_BUFMGR_DECLARE;

	*primaryError = 0;

	// -------- MirroredLock ----------
	MIRROREDLOCK_BUFMGR_LOCK;

	MirroredBufferPool_DoOpen(
					open, 
					relFileNode,
					segmentFileNum,
					relationName,
					/* create */ true,
					primaryError);

	MIRROREDLOCK_BUFMGR_UNLOCK;
	// -------- MirroredLock ----------
}

bool MirroredBufferPool_IsActive(
	MirroredBufferPoolOpen *open)
				/* The open struct. */
{
	return open->isActive;
}

/*
 * Flush a flat file.
 *
 */
bool MirroredBufferPool_Flush(
	MirroredBufferPoolOpen *open)
				/* The open struct. */	
{
	int ioError;

	Assert(open != NULL);
	Assert(open->isActive);

	ioError = 0;
	
	errno = 0;

	if (FileSync(open->primaryFile) < 0) 
		ioError = errno;

	errno = ioError;
	return (errno == 0);

}

/*
 * Close a bulk relation file.
 *
 */
void MirroredBufferPool_Close(
	MirroredBufferPoolOpen *open)
{
	Assert(open != NULL);
	Assert(open->isActive);
	errno = 0;

	FileClose(open->primaryFile);
	open->primaryFile = -1;
	open->isActive = false;	
}

/*
 * Write a mirrored flat file.
 */
bool MirroredBufferPool_Write(
	MirroredBufferPoolOpen *open,
				/* The open struct. */

	int32		position,
				/* The position to write the data. */

	void		*buffer,
				/* Pointer to the buffer. */

	int32		bufferLen)
				/* Byte length of buffer. */

{
	int64	seekResult;
	int	primaryError;

	Assert(open != NULL);
	Assert(buffer != NULL);
	Assert(open->isActive);

	primaryError = 0;
	
	if (bufferLen > writeBufLen)
	{
		if (writeBuf != NULL)
		{
			pfree(writeBuf);
			writeBuf = NULL;
			writeBufLen = 0;
		}
	}
	
	if (writeBuf == NULL)
	{
		writeBufLen = bufferLen;
		
		writeBuf = MemoryContextAlloc(TopMemoryContext, writeBufLen);
		if (writeBuf == NULL)
			ereport(ERROR, 
					(errcode(ERRCODE_OUT_OF_MEMORY),
					 (errmsg("could not allocate memory for mirrored alligned buffer"))));
	}

	/* 
	 * memcpy() is required since buffer is still changing, however filerep
	 * requires identical data to be written to primary and its mirror 
	 */
	memcpy(writeBuf, buffer, bufferLen);
	
	if (true)
	{
		errno = 0;
		
		seekResult = FileSeek(open->primaryFile, position, SEEK_SET);
		if (seekResult != position)
		{
			ereport(ERROR,
					(errcode_for_file_access(),
					 errmsg("could not seek in file to position %d in file '%s', segment file %d : %m", 
							position,
							relpath(open->relFileNode), 
							open->segmentFileNum)));
		}

		errno = 0;
		if ((int) FileWrite(open->primaryFile, writeBuf, bufferLen) != bufferLen)
		{
			/* if write didn't set errno, assume problem is no disk space */
			if (errno == 0)
				errno = ENOSPC;
			primaryError = errno;
		}
	}

	errno = primaryError;
	return (primaryError == 0);

}

/*
 * Read a mirrored buffer pool page.
 */
int MirroredBufferPool_Read(
	MirroredBufferPoolOpen *open,
				/* The open struct. */

	int32		position,
				/* The position to write the data. */

	void		*buffer,
				/* Pointer to the buffer. */

	int32		bufferLen)
				/* Byte length of buffer. */

{
	int64 seekResult;

	Assert(open != NULL);
	Assert(buffer != NULL);
	Assert(open->isActive);

	errno = 0;
	seekResult = FileSeek(open->primaryFile, position, SEEK_SET);
	if (seekResult != position)
	{
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not seek in file to position %d in file '%s', segment file %d: %m", 
				        position,
				        relpath(open->relFileNode), open->segmentFileNum)));
	}

	return FileRead(open->primaryFile, buffer, bufferLen);
}

int64 MirroredBufferPool_SeekSet(
	MirroredBufferPoolOpen *open,
				/* The open struct. */

	int32		position)
{
	Assert(open != NULL);
	Assert(open->isActive);

	errno = 0;
	return FileSeek(open->primaryFile, position, SEEK_SET);
}


int64 MirroredBufferPool_SeekEnd(
	MirroredBufferPoolOpen *open)
				/* The open struct. */
{
	Assert(open != NULL);
	Assert(open->isActive);

	errno = 0;
	return FileSeek(open->primaryFile, 0L, SEEK_END);
}



/*
 * Mirrored drop.
 */
static void MirroredBufferPool_DoDrop(
	RelFileNode 				*relFileNode,
				/* The tablespace, database, and relation OIDs for the open. */

	uint32						segmentFileNum,
				/* Which segment file. */

	char						*relationName,
				/* For tracing only.  Can be NULL in some execution paths. */

	int 						*primaryError)
{
	char *filespaceLocation = NULL;
	
	*primaryError = 0;

	PersistentTablespace_GetFilespacePath(
												   relFileNode->spcNode,
												   FALSE,
												   &filespaceLocation);	
	
	if (true)
	{
		char *path;

		path = (char*)palloc(MAXPGPATH + 1);

		FormRelfilePath(path,
						filespaceLocation,
						relFileNode,
						segmentFileNum);

		errno = 0;
		
		if (RemovePath(path, 0) < 0)
		{
			*primaryError = errno;
		}

		pfree(path);
	}
	
	if (filespaceLocation != NULL)
		pfree(filespaceLocation);
}

void MirroredBufferPool_Drop(
	RelFileNode					*relFileNode,
	 
	int32						segmentFileNum,

	char						*relationName,
					/* For tracing only.  Can be NULL in some execution paths. */
	
	bool						isRedo,

	int							*primaryError)
{	
	MirroredBufferPool_DoDrop(
						relFileNode,
						segmentFileNum,
						relationName,
						primaryError);
}

bool MirroredBufferPool_Truncate(
	MirroredBufferPoolOpen *open,
				/* The open struct. */

	int64		position)
				/* The position to cutoff the data. */
{
	int ioError = 0;

	errno = 0;		
	if (FileTruncate(open->primaryFile, position) < 0)
		ioError = errno;

	errno = ioError;
	return (ioError == 0);

}


