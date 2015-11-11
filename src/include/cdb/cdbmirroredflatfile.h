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
 * cdbmirroredflatfile.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef CDBMIRROREDFLATFILE_H
#define CDBMIRROREDFLATFILE_H

#include "utils/palloc.h"
#include "storage/fd.h"
#include "storage/dbdirnode.h"

/*
 * This module is for doing mirrored writes for system flat files being written
 * by multiple backend process.
 *
 * It is must manage a shared open to the mirror sender.
 */

/*
 * This structure contains write open information.  Consider the fields
 * inside to be private.
 */

/*
 * This structure contains write open information.  Consider the fields
 * inside to be private.
 */
typedef struct MirroredFlatFileOpen
{
	bool	isActive;

	bool	usingDbDirNode;

	DbDirNode 	dbDirNode;

	char 	*subDirectory;

	char    *simpleFileName;

	char 	*path;

	File	primaryFile;

	int32	appendPosition;
	
	bool	isMirrorRecovery;

	/* 
	 * Perform the entire operation atomically. We implement this by writing to
	 * a temporary file and then renaming.
	 */
	bool	atomic_op;
	char   *atomicSimpleFileName; /* original file that will be replaced with temporary file */

} MirroredFlatFileOpen;

#define MirroredFlatFileOpen_Init {false, false, {0, 0}, NULL, NULL, NULL, -1, 0, false, false, NULL}

// -----------------------------------------------------------------------------
// CleanMirrorDirectory, Create, Open, Flush, and Close 
// -----------------------------------------------------------------------------

#ifdef suppress
/*
 * Remove all files in the mirror's directory.
 */
extern void MirroredFlatFile_CleanMirrorDirectory(
	char 			*directorySimpleName);
				/* The simple name of the directory in the instance database directory. */
#endif

/*
 * Open a relation for mirrored write.
 */
extern int MirroredFlatFile_Open(
	MirroredFlatFileOpen *open,
				/* The resulting open struct. */

	char 			*subDirectory,
	
	char			*simpleFileName,
				/* The simple file name. */

	int 			fileFlags,

	int 			fileMode,

	bool			suppressError,

	bool			atomic_op,

	bool			isMirrorRecovery);

/*
 * Open a relation for mirrored write.
 */
extern int MirroredFlatFile_OpenInDbDir(
	MirroredFlatFileOpen *open,
				/* The resulting open struct. */

	DbDirNode 			*dbDirNode,
	
	char			*simpleFileName,
				/* The simple file name. */

	int 			fileFlags,

	int 			fileMode,

	bool			suppressError);


/*
 * Flush a flat file.
 */
extern int MirroredFlatFile_Flush(
	MirroredFlatFileOpen *open,
				/* The open struct. */

	bool				 suppressError);

				
extern bool MirroredFlatFile_IsActive(
	MirroredFlatFileOpen *open);
				/* The open struct. */
/*
 * Close a flat file.
 */
extern void MirroredFlatFile_Close(
	MirroredFlatFileOpen *open);
				/* The open struct. */
// -----------------------------------------------------------------------------
// Rename
// -----------------------------------------------------------------------------

extern int MirroredFlatFile_Rename(
							char 			*subDirectory,
							
							char			*oldSimpleFileName,
							/* The simple file name. */
							
							char			*newimpleFileName,
							/* The simple file name. */
							
							bool			suppressError,
							
							bool			isMirrorRecovery);
				
								
// -----------------------------------------------------------------------------
// Append and Write 
// -----------------------------------------------------------------------------

/*
* Append to a mirrored flat file.
*
* Assumed to start with an empty just created file at ~_Open.
*/
extern int MirroredFlatFile_Append(
	MirroredFlatFileOpen *open,
				/* The open struct. */

	void		*data,
				/* Pointer to the data. */

	int32		dataLen,
				/* The byte length of the data. */

	bool		suppressError);

/*
 * Get the current append position on the primary.
 */
extern int32 MirroredFlatFile_GetAppendPosition(
	MirroredFlatFileOpen *open);
				/* The open struct. */

/*
 * Write a mirrored flat file.
 */
extern int MirroredFlatFile_Write(
	MirroredFlatFileOpen *open,
				/* The open struct. */

	int32		position,
				/* The position to write the data. */

	void		*data,
				/* Pointer to the data. */

	int32		dataLen,
				/* The byte length of the data. */

	bool		suppressError);

extern int MirroredFlatFile_OpenPrimary(
				 MirroredFlatFileOpen *open,
				 /* The resulting open struct. */
				 
				 char 			*subDirectory,
				 
				 char			*simpleFileName,
				 /* The simple file name. */
				 
				 int 			fileFlags, 
				 
				 int 			fileMode,
				 
				 bool			suppressError);

extern void MirroredFlatFile_ClosePrimary(
								  MirroredFlatFileOpen *open);

extern int MirroredFlatFile_Read(
	MirroredFlatFileOpen *open,
				/* The open struct. */

	int32		position,

	void		*buffer,
				/* Pointer to the Buffer Pool page, properly protected by locks. */

	int32		bufferLen);

extern int64 MirroredFlatFile_SeekSet(
	MirroredFlatFileOpen *open,
				/* The open struct. */

	int32		position);

extern int64 MirroredFlatFile_SeekEnd(
									  MirroredFlatFileOpen *open);

// -----------------------------------------------------------------------------
// Drop 
// -----------------------------------------------------------------------------

/*
 * Mirrored drop.
 */
extern int MirroredFlatFile_Drop(
	char 			*subDirectory,
	
	char			*simpleFileName,
				/* The simple file name. */

	bool			suppressError,

	bool			isMirrorRecovery);


// -----------------------------------------------------------------------------
// Reconcile XLog Eof
// -----------------------------------------------------------------------------

/*
 * Mirrored reconcile.
 */
extern int MirroredFlatFile_ReconcileXLogEof(	
						  char 			*subDirectory,
						  
						  char			*simpleFileName,
						  
						  XLogRecPtr	primaryXLogEof,
						  
						  XLogRecPtr	*mirrorXLogEof);


extern int MirrorFlatFile(
			   char 		*subDirectory,
			   char			*simpleFileName);

extern void MirroredFlatFile_DropFilesFromDir(void);

extern void MirroredFlatFile_DropTemporaryFiles(void);

#endif   /* CDBMIRROREDFLATFILE_H */

