/*-------------------------------------------------------------------------
 *
 * cdbmirroredappendonly.c
 *
 * Copyright (c) 2009-2010, Greenplum inc
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <unistd.h>
#include <signal.h>
#include <fcntl.h>
#include <sys/file.h>

#include "access/xlogmm.h"
#include "utils/palloc.h"
#include "cdb/cdbfilerepprimary.h"
#include "cdb/cdbmirroredappendonly.h"
#include "storage/fd.h"
#include "catalog/catalog.h"
#include "cdb/cdbpersistenttablespace.h"
#include "cdb/cdbfilerepprimary.h"
#include "storage/smgr.h"
#include "storage/lwlock.h"
#include "utils/guc.h"
#include "cdb/cdbpersistentfilesysobj.h"
#include "cdb/cdbpersistentstore.h"
#include "cdb/cdbpersistentrecovery.h"
#include "cdb/cdbvars.h"

/*
 * Open a relation for mirrored write.
 */
static void MirroredAppendOnly_DoOpen(
	MirroredAppendOnlyOpen 		*open,
			/* The resulting open struct. */
							 
	RelFileNode					*relFileNode,
			/* The tablespace, database, and relation OIDs for the open. */
	 
	int32						segmentFileNum,
	
	int32						contentid,

	char						*relationName,
					/* For tracing only.  Can be NULL in some execution paths. */
	
	int64						logicalEof,
				/* The file name path. */

	bool						create,

	bool						readOnly,

	int 						*primaryError)
{
	int		fileFlags = O_RDWR | PG_BINARY;
	int		fileMode = 0600;
						/*
						 * File mode is S_IRUSR 00400 user has read permission
						 *               + S_IWUSR 00200 user has write permission
						 */

	char *primaryFilespaceLocation = NULL;
	char *mirrorFilespaceLocation = NULL;

	Assert(open != NULL);
	
	*primaryError = 0;

	if (create)
		fileFlags |= O_CREAT;

	MemSet(open, 0, sizeof(MirroredAppendOnlyOpen));

	open->relFileNode = *relFileNode;
	
	open->segmentFileNum = segmentFileNum;

	open->contentid = contentid;

	open->create = create;

	PersistentTablespace_GetPrimaryAndMirrorFilespaces(
										contentid,
										relFileNode->spcNode,
										TRUE,
										&primaryFilespaceLocation,
										&mirrorFilespaceLocation);

	char *dbPath;
	char *path;

	dbPath = (char*)palloc(MAXPGPATH + 1);
	path = (char*)palloc(MAXPGPATH + 1);

	/*
	 * Do the primary work first so we don't leave files on the mirror or have an
	 * open to clean up.
	 */
	FormDatabasePath(
				dbPath,
				primaryFilespaceLocation,
				relFileNode->spcNode,
				relFileNode->dbNode);
	
	if (segmentFileNum == 0)
		sprintf(path, "%s/%u", dbPath, relFileNode->relNode);
	else
		sprintf(path, "%s/%u.%u", dbPath, relFileNode->relNode, segmentFileNum);

	errno = 0;

	{
		fileFlags = 0;
		if (readOnly)
			fileFlags |= O_RDONLY;
		else
			fileFlags |= O_WRONLY;

		if (create)
			fileFlags |= O_CREAT;
		else
			fileFlags |= O_APPEND;
	}
	open->primaryFile = PathNameOpenFile(path, fileFlags, fileMode);

	if (open->primaryFile < 0)
	{
		*primaryError = errno;
	}

	pfree(dbPath);
	pfree(path);

	if (*primaryError != 0)
	{
		open->isActive = false;
	}
	else
	{
		open->isActive = true;
	}

	if (primaryFilespaceLocation != NULL)
		pfree(primaryFilespaceLocation);
	if (mirrorFilespaceLocation != NULL)
		pfree(mirrorFilespaceLocation);

}

/*
 * Call MirroredAppendOnly_Create with the MirroredLock already held.
 */
void MirroredAppendOnly_Create(
	RelFileNode 				*relFileNode,
				/* The tablespace, database, and relation OIDs for the open. */

	int32						segmentFileNum,
				/* Which segment file. */
	
	int32						contentid,

	char						*relationName,
					/* For tracing only.  Can be NULL in some execution paths. */
	
	int 						*primaryError)
{
	MirroredAppendOnlyOpen mirroredOpen;

	*primaryError = 0;

	MirroredAppendOnly_DoOpen(
					&mirroredOpen, 
					relFileNode,
					segmentFileNum,
					contentid,
					relationName,
					/* logicalEof */ 0,
					/* create */ true,
					/* readOnly */ false,
					primaryError);
	if (*primaryError != 0)
		return;

	MirroredAppendOnly_FlushAndClose(
							&mirroredOpen,
							primaryError);
}

/*
 * MirroredAppendOnly_OpenReadWrite will acquire and release the MirroredLock.
 *
 * Use aaa after writing data to determine if mirror loss occurred and
 * mirror catchup must be performed.
 */
void MirroredAppendOnly_OpenReadWrite(
	MirroredAppendOnlyOpen		*open,
				/* The resulting open struct. */

	RelFileNode 				*relFileNode,
				/* The tablespace, database, and relation OIDs for the open. */

	int32						segmentFileNum,
				/* Which segment file. */
	
	int32						contentid,

	char						*relationName,
					/* For tracing only.  Can be NULL in some execution paths. */
	
	int64						logicalEof,
				/* The logical EOF to begin appending the new data. */
	
	bool						readOnly,

	int 						*primaryError)
{
	*primaryError = 0;

	MirroredAppendOnly_DoOpen(
							open, 
							relFileNode,
							segmentFileNum,
							contentid,
							relationName,
							logicalEof,
							/* create */ false,
							/* readOnly */ readOnly,
							primaryError);
}

bool MirroredAppendOnly_IsActive(
	MirroredAppendOnlyOpen *open)
					/* The open struct. */
{
	return open->isActive;
}

/*
 * Flush and close a bulk relation file.
 *
 * If the flush is unable to complete on the mirror, then this relation will be marked in the
 * commit, distributed commit, distributed prepared and commit prepared records as having
 * un-mirrored bulk initial data.
 */
void MirroredAppendOnly_FlushAndClose(
	MirroredAppendOnlyOpen 	*open,
				/* The open struct. */		

	int						*primaryError)

{
	Assert(open != NULL);
	Assert(open->isActive);

	*primaryError = 0;

	{
		int		ret;

		errno = 0;

		ret = FileSync(open->primaryFile);
		if (ret != 0)
			*primaryError = errno;
	}

	FileClose(open->primaryFile);

	open->isActive = false;
	open->primaryFile = 0;
}

/*
 * Flush and close a bulk relation file.
 *
 * If the flush is unable to complete on the mirror, then this relation will be marked in the
 * commit, distributed commit, distributed prepared and commit prepared records as having
 * un-mirrored bulk initial data.
 */
void MirroredAppendOnly_Flush(
	MirroredAppendOnlyOpen 	*open,
				/* The open struct. */				

	int						*primaryError)
{
	Assert(open != NULL);
	Assert(open->isActive);
	
	int		ret;

	errno = 0;

	ret = FileSync(open->primaryFile);
	if (ret != 0)
	{
		*primaryError = errno;
	}
}

/*
 * Close a bulk relation file.
 *
 */
void MirroredAppendOnly_Close(
	MirroredAppendOnlyOpen 	*open
				/* The open struct. */)
{
	Assert(open != NULL);
	Assert(open->isActive);
	
	// No primary error to report.
	errno = 0;
	
	FileClose(open->primaryFile);

	open->isActive = false;
	open->primaryFile = 0;
}

static void MirroredAppendOnly_DoDrop(
	RelFileNode					*relFileNode,
	 
	int32						segmentFileNum,

	int32						contentid,

	char						*relationName,
					/* For tracing only.  Can be NULL in some execution paths. */

	int							*primaryError)
{
	char *primaryFilespaceLocation;
	char *mirrorFilespaceLocation;

	*primaryError = 0;

	PersistentTablespace_GetPrimaryAndMirrorFilespaces(
													   contentid,
													   relFileNode->spcNode,
													   TRUE,
													   &primaryFilespaceLocation,
													   &mirrorFilespaceLocation);	
	

	char *dbPath;
	char *path;

	dbPath = (char*)palloc(MAXPGPATH + 1);
	path = (char*)palloc(MAXPGPATH + 1);

	FormDatabasePath(
					 dbPath,
					 primaryFilespaceLocation,
					 relFileNode->spcNode,
					 relFileNode->dbNode);

	if (segmentFileNum == 0)
		sprintf(path, "%s/%u", dbPath, relFileNode->relNode);
	else
		sprintf(path, "%s/%u.%u", dbPath, relFileNode->relNode, segmentFileNum);

	errno = 0;
	
	if (RemovePath(path, 0) < 0)
	{
		*primaryError = errno;
	}

	pfree(dbPath);
	pfree(path);

	if (primaryFilespaceLocation != NULL)
		pfree(primaryFilespaceLocation);
	
	if (mirrorFilespaceLocation != NULL)
		pfree(mirrorFilespaceLocation);	
}

void MirroredAppendOnly_Drop(
	RelFileNode					*relFileNode,
	 
	int32						segmentFileNum,

	int32						contentid,

	char						*relationName,
					/* For tracing only.  Can be NULL in some execution paths. */
	
	int							*primaryError)
{
	MirroredAppendOnly_DoDrop(
						relFileNode,
						segmentFileNum,
						contentid,
						relationName,
						primaryError);
}

// -----------------------------------------------------------------------------
// Append 
// -----------------------------------------------------------------------------
				
/*
 * Write bulk mirrored.
 */
void MirroredAppendOnly_Append(
	MirroredAppendOnlyOpen *open,
				/* The open struct. */

	void					*buffer,
				/* Pointer to the buffer. */

	int32					bufferLen,
				/* Byte length of buffer. */

	int 					*primaryError)
{
	Assert(open != NULL);
	Assert(open->isActive);
	
	*primaryError = 0;
	/**mirrorDataLossOccurred = false;*/

	int	ret;
	errno = 0;

	ret = FileWrite(open->primaryFile, buffer, bufferLen);

	if (ret != bufferLen)
	{
		/* if write didn't set errno, assume problem is no disk space */
		if (errno == 0)
			errno = ENOSPC;
		*primaryError = errno;
	}
}

// -----------------------------------------------------------------------------
// Truncate
// ----------------------------------------------------------------------------
void MirroredAppendOnly_Truncate(
	MirroredAppendOnlyOpen *open,
				/* The open struct. */
	
	int64		position,
				/* The position to cutoff the data. */

	int 		*primaryError)
{
	*primaryError = 0;
		errno = 0;
		
	if (FileTruncate(open->primaryFile, position) < 0)
		*primaryError = errno;
}

/*
 * Read an append only file in sequential way.
 * The routine is used for Resync.
 */
int MirroredAppendOnly_Read(
	MirroredAppendOnlyOpen *open,
		/* The open struct. */
	
	void		*buffer,
		/* Pointer to the buffer. */
	
	int32		bufferLen)
		/* Byte length of buffer. */

{
	int	ret;

	Assert(open != NULL);
	Assert(buffer != NULL);
	Assert(open->isActive);
	
	errno = 0;

	ret = FileRead(open->primaryFile, buffer, bufferLen);

	return ret;
}
