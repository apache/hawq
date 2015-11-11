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
 * cdbmirroredflatfile.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"
#include "miscadmin.h"

#include <fcntl.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#include <signal.h>

#include "access/twophase.h"
#include "access/slru.h"
#include "access/xlog_internal.h"
#include "utils/palloc.h"
#include "cdb/cdbmirroredflatfile.h"
#include "cdb/cdbpersistentdatabase.h"
#include "cdb/cdbpersistenttablespace.h"
#include "storage/fd.h"
#include "catalog/catalog.h"
#include "catalog/pg_tablespace.h"
#include "commands/tablespace.h"
#include "utils/memutils.h"

#define PGVERSION "PG_VERSION"

/*
 * Open a relation for mirrored write.
 */
int MirroredFlatFile_Open(
	MirroredFlatFileOpen *open,
				/* The resulting open struct. */

	char 			*subDirectory,
	
	char			*simpleFileName,
				/* The simple file name. */

	int 			fileFlags, 

	int 			fileMode,

	bool			suppressError,

	bool			atomic_op,

	bool			isMirrorRecovery) 
/* NOTE do we need to consider supressError on mirror */

{
	int save_errno = 0;
	
	char *dir = NULL, *mirrorDir = NULL;
	
	Assert(open != NULL);

	/* If the user wants an atomic write, they better be creating a file */
	Assert((atomic_op && (fileFlags & O_CREAT) == O_CREAT) ||
		   !atomic_op);
	
	MemSet(open, 0, sizeof(MirroredFlatFileOpen));

	open->atomic_op = atomic_op;

	if (atomic_op)
	{
		char *tmp_filename;

		tmp_filename = (char*)palloc(MAXPGPATH + 1);

		open->atomicSimpleFileName = MemoryContextStrdup(TopMemoryContext, simpleFileName);
		sprintf(tmp_filename, "%s.%i",
				 simpleFileName, MyProcPid);
		open->simpleFileName = MemoryContextStrdup(TopMemoryContext, tmp_filename);
		
		pfree(tmp_filename);
	}
	else
	{
		open->simpleFileName = MemoryContextStrdup(TopMemoryContext, simpleFileName);
	}

	open->isMirrorRecovery = isMirrorRecovery;
	open->usingDbDirNode = false;

	/* 
	 * Now that the transaction filespace is configurable, the subdirectory is not always
	 * relative to the data directory. If we have configured the transaction files to be on
	 * the non-default filespace, then we need the absolute path for the sub directory 
         */
	if (isTxnDir(subDirectory))
	{
		dir = makeRelativeToTxnFilespace(subDirectory);
		mirrorDir = makeRelativeToPeerTxnFilespace(subDirectory);
		open->subDirectory = MemoryContextStrdup(TopMemoryContext, mirrorDir);
	}
	else
	{
		/* Default case */
		dir = MemoryContextStrdup(TopMemoryContext, subDirectory);
		open->subDirectory = MemoryContextStrdup(TopMemoryContext, subDirectory);
	}

	open->path = MemoryContextAlloc(TopMemoryContext, strlen(dir) + 1 +
									strlen(open->simpleFileName) + 1);

	
	if (snprintf(open->path, MAXPGPATH, "%s/%s", dir, open->simpleFileName) > MAXPGPATH)
	{
		ereport(ERROR, (errmsg("cannot generate path %s/%s", dir, open->simpleFileName)));
	}

	errno = 0;
	
	if (!open->isMirrorRecovery) {
		open->primaryFile = PathNameOpenFile(open->path, fileFlags, fileMode);
		save_errno = errno;
		
		if (open->primaryFile < 0)
		{
			if (!suppressError)
			{
				ereport(ERROR,
						(errcode_for_file_access(),
						 errmsg("could not open file \"%s\": %m", open->path)));
			}
			
			pfree(open->subDirectory);
			open->subDirectory = NULL;
			
			pfree(open->simpleFileName);
			open->simpleFileName = NULL;
			
			pfree(open->path);
			open->path = NULL;
		
			open->isActive = false;
						
		}
		else
		{
			open->appendPosition = 0;
			
			open->isActive = true;		
		}
	} 
	else
	{
		open->isActive = true;
	}

	if (dir)	
		pfree(dir);

	if (mirrorDir)
		pfree(mirrorDir);

	return save_errno;
}

/*
 * Open a relation for mirrored write.
 */
int MirroredFlatFile_OpenInDbDir(
	MirroredFlatFileOpen *open,
				/* The resulting open struct. */

	DbDirNode 		*dbDirNode,
	
	char			*simpleFileName,
				/* The simple file name. */

	int 			fileFlags, 

	int 			fileMode,

	bool			suppressError)

{
	int save_errno = 0;
	
	char *filespaceLocation = NULL;
	char *subDirectory;

	Assert(open != NULL);
	
	MemSet(open, 0, sizeof(MirroredFlatFileOpen));

	open->usingDbDirNode = true;
	open->dbDirNode = *dbDirNode;

	subDirectory = (char*)palloc(MAXPGPATH + 1);
	
	PersistentTablespace_GetFilespacePath(
													   dbDirNode->tablespace,
													   FALSE,
													   &filespaceLocation);	
	FormDatabasePath(
					 subDirectory,
					 filespaceLocation,
					 dbDirNode->tablespace,
					 dbDirNode->database);	
	
	open->subDirectory = MemoryContextStrdup(TopMemoryContext, subDirectory);
	open->simpleFileName = MemoryContextStrdup(TopMemoryContext, simpleFileName);
	open->path = MemoryContextAlloc(TopMemoryContext, strlen(subDirectory) + 1 + strlen(simpleFileName) + 1);
	
	sprintf(open->path, "%s/%s", subDirectory, simpleFileName);
	
	errno = 0;
	open->primaryFile = PathNameOpenFile(open->path, fileFlags, fileMode);
	save_errno = errno;
	
	if (open->primaryFile < 0)
	{
		if (!suppressError)
		{
			ereport(ERROR,
					(errcode_for_file_access(),
					 errmsg("could not open file \"%s\": %m", open->path)));
		}
		
		pfree(open->subDirectory);
		open->subDirectory = NULL;
		
		pfree(open->simpleFileName);
		open->simpleFileName = NULL;
		
		pfree(open->path);
		open->path = NULL;
		
		open->isActive = false;		
	}
	else
	{
		open->appendPosition = 0;
		
		open->isActive = true;
	}

	pfree(subDirectory);

	if (filespaceLocation != NULL)
		pfree(filespaceLocation);
	
	return save_errno;
}


extern bool MirroredFlatFile_IsActive(
	MirroredFlatFileOpen *open)
				/* The open struct. */
{
	return open->isActive;
}

/*
 * Flush a flat file.
 *
 */
int MirroredFlatFile_Flush(
	MirroredFlatFileOpen *open,
				/* The open struct. */	

	bool				suppressError)

{
	int		save_errno = 0;
	
	Assert(open != NULL);
	Assert(open->isActive);

	if (! open->isMirrorRecovery) {
		int	ret;

		errno = 0;
		ret = FileSync(open->primaryFile);
		if (ret != 0)
			ereport(ERROR,
					(errcode_for_file_access(),
					 errmsg("could not fsync file \"%s\": %m", open->path)));

		save_errno = errno;
	}

	return save_errno;
}

/*
 * Close a bulk relation file.
 *
 */
void MirroredFlatFile_Close(
	MirroredFlatFileOpen *open)
				/* The open struct. */				

{
	Assert(open != NULL);
	Assert(open->isActive);

	if (! open->isMirrorRecovery) {
		errno = 0;
		FileClose(open->primaryFile);
	}
	
	if (open->atomic_op)
	{
		MirroredFlatFile_Rename(open->subDirectory,
								open->simpleFileName,
								open->atomicSimpleFileName,
								true,
								false);
	}

	Assert(open->subDirectory != NULL);
	pfree(open->subDirectory);
	open->subDirectory = NULL;
	Assert(open->simpleFileName != NULL);
	pfree(open->simpleFileName);
	open->simpleFileName = NULL;
	Assert(open->path != NULL);
	pfree(open->path);
	open->path = NULL;

	if (open->atomic_op)
		pfree(open->atomicSimpleFileName);

	open->isActive = false;
}

/*
 * Rename a flat file.
 *
 */
int MirroredFlatFile_Rename(
							char 			*subDirectory,
							
							char			*oldSimpleFileName,
								/* The simple file name. */
							
							char			*newSimpleFileName,
								/* The simple file name. */
						
							bool			new_can_exist,
							
							bool			isMirrorRecovery) 
					/* if TRUE then request is not performed on primary */
{
	char *oldPath;
	char *newPath;
	int	 save_errno = 0;
	struct stat buf;

	char *dir = NULL, *mirrorDir = NULL;

	oldPath = (char*)palloc(MAXPGPATH + 1);
	newPath = (char*)palloc(MAXPGPATH + 1);

	if (isTxnDir(subDirectory))
	{
		dir = makeRelativeToTxnFilespace(subDirectory);
		mirrorDir = makeRelativeToPeerTxnFilespace(subDirectory);
	}	
	else
	{
		dir = MemoryContextStrdup(TopMemoryContext, subDirectory);
		mirrorDir = MemoryContextStrdup(TopMemoryContext, subDirectory);
	}

	if (snprintf(oldPath, MAXPGPATH, "%s/%s", dir, oldSimpleFileName) > MAXPGPATH)
	{
		ereport(ERROR, (errmsg("cannot generate path %s/%s", dir, oldSimpleFileName)));
	}
	if (snprintf(newPath, MAXPGPATH, "%s/%s", dir, newSimpleFileName) > MAXPGPATH)
	{
		ereport(ERROR, (errmsg("cannot generate path %s/%s", dir, newSimpleFileName)));
	}
	
	if (!new_can_exist)
	{
		if (stat(newPath, &buf) == 0)
			elog(ERROR, "cannot rename flat file %s to %s: File exists",
				 oldPath,
				 newPath);
	}

	if (!isMirrorRecovery)
	{
		if (rename(oldPath, newPath) != 0)
			ereport(ERROR,
					(errcode_for_file_access(),
					 errmsg("could not rename file from \"%s\" to \"%s\": %m", 
							oldPath, newPath)));
		else
			errno = 0;
		save_errno = errno;
	}
	
	pfree(oldPath);
	pfree(newPath);

	pfree(dir);
	pfree(mirrorDir);

	return save_errno;
}


// -----------------------------------------------------------------------------
// Append 
// -----------------------------------------------------------------------------

/*
 * Write a mirrored flat file.
 */
int MirroredFlatFile_Append(
	MirroredFlatFileOpen *open,
				/* The open struct. */

	void		*buffer,
				/* Pointer to the buffer. */

	int32		bufferLen,
				/* Byte length of buffer. */

	bool		suppressError)

{
	int save_errno = 0;
	
	Assert(open != NULL);
	Assert(open->isActive);

	if (! open->isMirrorRecovery) {
		int	ret;

		errno = 0;
		ret = FileWrite(open->primaryFile, buffer, bufferLen);
		if (ret != bufferLen)
		{
			/* if write didn't set errno, assume problem is no disk space */
			if (errno == 0)
				errno = ENOSPC;

			if (!suppressError)
				ereport(ERROR,
						(errcode_for_file_access(),
						 errmsg("could not write to file \"%s\": %m", open->path)));
		}
		save_errno = errno;
	}
	
	open->appendPosition += bufferLen;

	return save_errno;
}

int32 MirroredFlatFile_GetAppendPosition(
	MirroredFlatFileOpen *open)
				/* The open struct. */
{
	return open->appendPosition;
}

/*
 * Write a mirrored flat file.
 */
int MirroredFlatFile_Write(
	MirroredFlatFileOpen *open,
				/* The open struct. */

	int32		position,
				/* The position to write the data. */

	void		*buffer,
				/* Pointer to the buffer. */

	int32		bufferLen,
				/* Byte length of buffer. */

	bool		suppressError)

{
	int64	seekResult;
	int		save_errno = 0;
	
	Assert(open != NULL);
	Assert(open->isActive);
	
	XLogInitMirroredAlignedBuffer(bufferLen);
	
	/* 
	 * memcpy() is required since buffer is still changing, however filerep
	 * requires identical data are written to primary and its mirror 
	 */	
	memcpy(writeBufAligned, buffer, bufferLen);
	
	if (! open->isMirrorRecovery) {
		errno = 0;
		seekResult = FileSeek(open->primaryFile, position, SEEK_SET);
		if (seekResult != position)
		{
			ereport(ERROR,
					(errcode_for_file_access(),
					 errmsg("could not seek in file to position '%d' in file '%s': %m", 
							position,
							open->path)));
		}

		if ((int) FileWrite(open->primaryFile, writeBufAligned, bufferLen) != bufferLen)
		{
			/* if write didn't set errno, assume problem is no disk space */
			if (errno == 0)
				errno = ENOSPC;
			if (!suppressError)
				ereport(ERROR,
						(errcode_for_file_access(),
						 errmsg("could not write to file \"%s\": %m", open->path)));
		}
		save_errno = errno;
	}
		
	return save_errno;
}

/*
 * Open a relation on primary
 */
int MirroredFlatFile_OpenPrimary(
						  MirroredFlatFileOpen *open,
						  /* The resulting open struct. */
						  
						  char 			*subDirectory,
						  
						  char			*simpleFileName,
						  /* The simple file name. */
						  
						  int 			fileFlags, 
						  
						  int 			fileMode,
								 
						  bool			suppressError) 
/* NOTE do we need to consider supressError on mirror */

{
	Assert(open != NULL);
	
	MemSet(open, 0, sizeof(MirroredFlatFileOpen));
	
	open->usingDbDirNode = false;
	open->subDirectory = MemoryContextStrdup(TopMemoryContext, subDirectory);
	open->simpleFileName = MemoryContextStrdup(TopMemoryContext, simpleFileName);
	open->path = MemoryContextAlloc(TopMemoryContext, strlen(subDirectory) + 1 + strlen(simpleFileName) + 1);
	
	sprintf(open->path, "%s/%s", subDirectory, simpleFileName);
	
	errno = 0;
	open->primaryFile = PathNameOpenFile(open->path, fileFlags, fileMode);
	if (open->primaryFile < 0)
	{
		if (!suppressError)
		{
			ereport(ERROR,
					(errcode_for_file_access(),
					 errmsg("could not open file \"%s\": %m", open->path)));
		}

		pfree(open->subDirectory);
		open->subDirectory = NULL;
		
		pfree(open->simpleFileName);
		open->simpleFileName = NULL;

		pfree(open->path);
		open->path = NULL;

		open->isActive = false;
	}
	else
	{
		open->isActive = true;
	
		open->appendPosition = 0;
	}
	
	return errno;
}

/*
 * Close a bulk relation file.
 *
 */
void MirroredFlatFile_ClosePrimary(
							MirroredFlatFileOpen *open)
/* The open struct. */				

{
	Assert(open != NULL);
	Assert(open->isActive);
	
	errno = 0;
	
	FileClose(open->primaryFile);
	
	Assert(open->subDirectory != NULL);
	pfree(open->subDirectory);
	open->subDirectory = NULL;
	Assert(open->simpleFileName != NULL);
	pfree(open->simpleFileName);
	open->simpleFileName = NULL;
	Assert(open->path != NULL);
	pfree(open->path);
	open->path = NULL;
	
	open->isActive = false;
}


/*
 * Read a local flat file.
 */
int MirroredFlatFile_Read(
	MirroredFlatFileOpen *open,
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
				 errmsg("could not seek in file to position %d in file \"%s\": %m", 
				        position,
				        open->path)));
	}

	return FileRead(open->primaryFile, buffer, bufferLen);
}

/*
 * Seek in local flat file
 */
int64 MirroredFlatFile_SeekSet(
	MirroredFlatFileOpen *open,
				/* The open struct. */

	int32		position)
{
	errno = 0;
	return FileSeek(open->primaryFile, position, SEEK_SET);
}

/*
 * Seek to the end of local flat file
 */
int64 MirroredFlatFile_SeekEnd(
							   MirroredFlatFileOpen *open)
							   /* The open struct. */
{
	errno = 0;
	return FileSeek(open->primaryFile, 0, SEEK_END);
}


/*
 * Mirrored drop.
 */
int MirroredFlatFile_Drop(
	char 			*subDirectory,
	
	char			*simpleFileName,
		/* The simple file name. */

	bool			suppressError,

	bool			isMirrorRecovery) 
		/* if TRUE then request is not performed on primary */
{
	char *path;
	int	 save_errno = 0;
	char *dir = NULL, *mirrorDir = NULL;
	
	if (isTxnDir(subDirectory))
	{
		dir = makeRelativeToTxnFilespace(subDirectory);
		mirrorDir = makeRelativeToPeerTxnFilespace(subDirectory);
	}
	else
	{
		dir = MemoryContextStrdup(TopMemoryContext, subDirectory);
		mirrorDir = MemoryContextStrdup(TopMemoryContext, subDirectory);
	}

	/* FIXME: mat3: Don't process mirror drop for hdfs... */
	if (strncmp(subDirectory, "hdfs:/", 6) == 0)
		return save_errno;

	path = (char*)palloc(MAXPGPATH + 1);
	
	if (snprintf(path, MAXPGPATH, "%s/%s", dir, simpleFileName) > MAXPGPATH)
	{
		ereport(ERROR, (errmsg("cannot generate path %s/%s", dir, simpleFileName)));
	}	
	
	if (! isMirrorRecovery) {
		errno = 0;
		if (unlink(path) < 0 && !suppressError)
			ereport(ERROR,
					(errcode_for_file_access(),
					 errmsg("could not unlink file \"%s\": %m", path)));
		save_errno = errno;
	}

	pfree(path);
	pfree(dir);
	pfree(mirrorDir);

	return save_errno;
}


/*
 * Reconcile xlog (WAL) EOF between primary and mirror.
 *		xlog gets truncated to the same EOF on primary and mirror.
 *		Truncated area is set to zero.
 */
int MirroredFlatFile_ReconcileXLogEof(	
							  char 			*subDirectory,
							  
							  char			*simpleFileName,
							  
							  XLogRecPtr	primaryXLogEof,
							  
							  XLogRecPtr	*mirrorXLogEof)
{
	int		status = STATUS_OK;
	
	return status;
}

/*
 *
 */
int
MirrorFlatFile(
			   char 		*subDirectory,
			   char			*simpleFileName)
{
	MirroredFlatFileOpen	mirroredOpen;
	MirroredFlatFileOpen	primaryOpen;
	char					*buffer = NULL;
	int32					endOffset = 0;
	int32					startOffset = 0;
	int32					bufferLen = 0;
	int						retval = 0;
	char 			*dir = NULL, *mirrorDir = NULL;
	
	errno = 0;

	if (isTxnDir(subDirectory))
	{
		dir = makeRelativeToTxnFilespace(subDirectory);
		mirrorDir = makeRelativeToPeerTxnFilespace(subDirectory);	
	}
	else
	{
		dir = MemoryContextStrdup(TopMemoryContext, subDirectory);
		mirrorDir = MemoryContextStrdup(TopMemoryContext, subDirectory);
	}
	
	while (1) {
		
		retval = MirroredFlatFile_OpenPrimary(
											  &primaryOpen,
											  dir,
											  simpleFileName,
											  O_RDONLY | PG_BINARY,
											  S_IRUSR | S_IWUSR,
											  /* suppressError */ false);
		if (retval != 0)
			break;
		
		/*
		 * Determine the end.
		 */
		endOffset = MirroredFlatFile_SeekEnd(&primaryOpen);
		if (endOffset < 0) {
			ereport(ERROR,
					(errcode_for_file_access(),
					 errmsg("could not seek to end of file \"%s\" : %m",
							primaryOpen.path)));
			break;
		}
		
		retval = MirroredFlatFile_Drop(
									   subDirectory,
									   simpleFileName,
									   /* suppressError */ false,
									   /*isMirrorRecovery */ TRUE);
		if (retval != 0)
			break;
		
		retval = MirroredFlatFile_Open(
									   &mirroredOpen,
									   subDirectory,
									   simpleFileName,
									   O_CREAT | O_EXCL | O_RDWR | PG_BINARY,
									   S_IRUSR | S_IWUSR,
									   /* suppressError */ false,
									   /* atomic write/ */ false,
									   /*isMirrorRecovery */ TRUE);
		
		if (retval != 0)
			break;
				
		bufferLen = (Size) Min(BLCKSZ, endOffset - startOffset);
		buffer = (char*) palloc(bufferLen);
		if (buffer == NULL)
			ereport(ERROR,
					(errcode(ERRCODE_OUT_OF_MEMORY),
					 (errmsg("Not enough shared memory for Mirroring."))));
		
		MemSet(buffer, 0, bufferLen);
				
		while (startOffset < endOffset) {
			
			retval = MirroredFlatFile_Read(	
										   &primaryOpen,
										   startOffset,
										   buffer,
										   bufferLen);
			
			if (retval != bufferLen) {
				ereport(ERROR,
						(errcode_for_file_access(),
						 errmsg("could not read from position:%d in file \"%s\" : %m",
								startOffset, primaryOpen.path)));
				
				break;
			}
			
			retval = MirroredFlatFile_Append(
											 &mirroredOpen,
											 buffer,
											 bufferLen,
											 /* suppressError */ false);
			
			if (retval != 0)
				break;
			
			startOffset += bufferLen;
			
			bufferLen = (Size) Min(BLCKSZ, endOffset - startOffset);
		} // while
		
		if (retval != 0)
			break;
		
		retval = MirroredFlatFile_Flush(
										&mirroredOpen,
										/* suppressError */ FALSE);
		if (retval != 0)
			break;
		
		break;
	} // while(1)
	
	if (buffer) {
		pfree(buffer);
		buffer = NULL;
	}
	
	if (MirroredFlatFile_IsActive(&mirroredOpen)) {
		MirroredFlatFile_Close(&mirroredOpen);
	}
	
	if (MirroredFlatFile_IsActive(&primaryOpen)) {
		MirroredFlatFile_ClosePrimary(&primaryOpen);
	}

	if (dir)
		pfree(dir);
	if (mirrorDir)
		pfree(mirrorDir);	

	return retval;
}

void
MirroredFlatFile_DropFilesFromDir(void)
{
	char *mirrorXLogDir = makeRelativeToPeerTxnFilespace(XLOGDIR);
	char *mirrorCLogDir = makeRelativeToPeerTxnFilespace(CLOG_DIR);
	char *mirrorDistributedLogDir = makeRelativeToPeerTxnFilespace(DISTRIBUTEDLOG_DIR);
	char *mirrorDistributedXidMapDir = makeRelativeToPeerTxnFilespace(DISTRIBUTEDXIDMAP_DIR);
	char *mirrorMultiXactMemberDir = makeRelativeToPeerTxnFilespace(MULTIXACT_MEMBERS_DIR);
	char *mirrorMultiXactOffsetDir = makeRelativeToPeerTxnFilespace(MULTIXACT_OFFSETS_DIR);
	char *mirrorSubxactDir = makeRelativeToPeerTxnFilespace(SUBTRANS_DIR);
		
	pfree(mirrorSubxactDir);
	pfree(mirrorMultiXactOffsetDir);
	pfree(mirrorMultiXactMemberDir);
	pfree(mirrorDistributedXidMapDir);
	pfree(mirrorDistributedLogDir);
	pfree(mirrorCLogDir);
	pfree(mirrorXLogDir);
	
}

/* 
 * Drop temporary files of pg_database, pg_auth, pg_auth_time_constraint
 */
void
MirroredFlatFile_DropTemporaryFiles(void)
{
	
	DIR             *global_dir;
	struct dirent   *de;
	char			path[MAXPGPATH+1];
	
	global_dir = AllocateDir("global");
	
	while ((de = ReadDir(global_dir, "global")) != NULL) 
	{
		if (strstr(de->d_name, "pg_database.") != NULL ||
			strstr(de->d_name, "pg_auth.") != NULL ||
			strstr(de->d_name, "pg_auth_time_constraint.") != NULL)
		{
			sprintf(path, "%s/%s", "global", de->d_name);

			errno = 0;
			if (unlink(path))
			{
				char	tmpBuf[1024];
								
				snprintf(tmpBuf, sizeof(tmpBuf), "unlink failed identifier '%s' errno '%d' ",
						 de->d_name, 
						 errno);
				/* TODO: ereport? */
			}
		}
	}
	
	if (global_dir)
	{
		FreeDir(global_dir);
	}	
}

