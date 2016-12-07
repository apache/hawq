/*-------------------------------------------------------------------------
 *
 * md.c
 *	  This code manages relations that reside on magnetic disk.
 *
 * Portions Copyright (c) 1996-2009, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  $PostgreSQL: pgsql/src/backend/storage/smgr/md.c,v 1.123.2.2 2007/04/12 17:11:00 tgl Exp $
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <unistd.h>
#include <fcntl.h>
#include <sys/file.h>
#include <glob.h>
#include <dirent.h>
#include <sys/types.h>
#include <sys/stat.h>


#include "access/appendonlywriter.h" 
#include "catalog/catalog.h"
#include "miscadmin.h"
#include "postmaster/bgwriter.h"
#include "storage/fd.h"
#include "storage/smgr.h"
#include "utils/hsearch.h"
#include "utils/memutils.h"
#include "cdb/cdbmirroredbufferpool.h"
#include "cdb/cdbpersistenttablespace.h"
#include "cdb/cdbvars.h"
#include "utils/guc.h"
#include "catalog/pg_tablespace.h"


/* interval for calling AbsorbFsyncRequests in mdsync */
#define FSYNCS_PER_ABSORB		10

/* special values for the segno arg to RememberFsyncRequest */
#define FORGET_RELATION_FSYNC	(InvalidBlockNumber)
#define FORGET_DATABASE_FSYNC	(InvalidBlockNumber-1)

/*
 * On Windows, we have to interpret EACCES as possibly meaning the same as
 * ENOENT, because if a file is unlinked-but-not-yet-gone on that platform,
 * that's what you get.  Ugh.  This code is designed so that we don't
 * actually believe these cases are okay without further evidence (namely,
 * a pending fsync request getting revoked ... see mdsync).
 */
#ifndef WIN32
#define FILE_POSSIBLY_DELETED(err)  ((err) == ENOENT)
#else
#define FILE_POSSIBLY_DELETED(err)  ((err) == ENOENT || (err) == EACCES)
#endif

/*
 *	The magnetic disk storage manager keeps track of open file
 *	descriptors in its own descriptor pool.  This is done to make it
 *	easier to support relations that are larger than the operating
 *	system's file size limit (often 2GBytes).  In order to do that,
 *	we break relations up into "segment" files that are each shorter than
 *	the OS file size limit.  The segment size is set by the RELSEG_SIZE
 *	configuration constant in pg_config_manual.h.
 *
 *	On disk, a relation must consist of consecutively numbered segment
 *	files in the pattern
 *		-- Zero or more full segments of exactly RELSEG_SIZE blocks each
 *		-- Exactly one partial segment of size 0 <= size < RELSEG_SIZE blocks
 *		-- Optionally, any number of inactive segments of size 0 blocks.
 *	The full and partial segments are collectively the "active" segments.
 *	Inactive segments are those that once contained data but are currently
 *	not needed because of an mdtruncate() operation.  The reason for leaving
 *	them present at size zero, rather than unlinking them, is that other
 *	backends and/or the bgwriter might be holding open file references to
 *	such segments.  If the relation expands again after mdtruncate(), such
 *	that a deactivated segment becomes active again, it is important that
 *	such file references still be valid --- else data might get written
 *	out to an unlinked old copy of a segment file that will eventually
 *	disappear.
 *
 *	The file descriptor pointer (md_fd field) stored in the SMgrRelation
 *	cache is, therefore, just the head of a list of MdfdVec objects, one
 *	per segment.  But note the md_fd pointer can be NULL, indicating
 *	relation not open.
 *
 *	Also note that mdmir_chain == NULL does not necessarily mean the relation
 *	doesn't have another segment after this one; we may just not have
 *	opened the next segment yet.  (We could not have "all segments are
 *	in the chain" as an invariant anyway, since another backend could
 *	extend the relation when we weren't looking.)  We do not make chain
 *	entries for inactive segments, however; as soon as we find a partial
 *	segment, we assume that any subsequent segments are inactive.
 *
 *	All MdfdVec objects are palloc'd in the MdCxt memory context.
 *
 *	Defining LET_OS_MANAGE_FILESIZE disables the segmentation logic,
 *	for use on machines that support large files.  Beware that that
 *	code has not been tested in a long time and is probably bit-rotted.
 */

typedef struct _MdMirVec
{
	MirroredBufferPoolOpen		mdmir_open;

	BlockNumber mdmir_segno;		/* segment number, from 0 */
#ifndef LET_OS_MANAGE_FILESIZE	/* for large relations */
	struct _MdMirVec *mdmir_chain;	/* next segment, or NULL */
#endif
} MdMirVec;

static MemoryContext MdCxt;		/* context for all md.c allocations */


/*
 * In some contexts (currently, standalone backends and the bgwriter process)
 * we keep track of pending fsync operations: we need to remember all relation
 * segments that have been written since the last checkpoint, so that we can
 * fsync them down to disk before completing the next checkpoint.  This hash
 * table remembers the pending operations.	We use a hash table mostly as
 * a convenient way of eliminating duplicate requests.
 *
 * (Regular backends do not track pending operations locally, but forward
 * them to the bgwriter.)
 */
typedef struct
{
	RelFileNode rnode;			/* the targeted relation */
	BlockNumber segno;			/* which segment */
} PendingOperationTag;

typedef uint16 CycleCtr;		/* can be any convenient integer size */

typedef struct
{
	PendingOperationTag tag;	/* hash table key (must be first!) */
	bool		canceled;		/* T => request canceled, not yet removed */
	CycleCtr	cycle_ctr;		/* mdsync_cycle_ctr when request was made */
} PendingOperationEntry;

static HTAB *pendingOpsTable = NULL;

static CycleCtr mdsync_cycle_ctr = 0;


/* local routines */
static MdMirVec *mdopen(SMgrRelation reln, bool allowNotFound);
static bool register_dirty_segment(SMgrRelation reln, MdMirVec *seg);
static MdMirVec *_mirvec_alloc(void);

#ifndef LET_OS_MANAGE_FILESIZE
static MdMirVec *_mdmir_openseg(SMgrRelation reln, BlockNumber segno,
			  bool createIfDoesntExist);
#endif
static MdMirVec *_mdmir_getseg(SMgrRelation reln, BlockNumber blkno,
			 bool allowNotFound);
static BlockNumber _mdnblocks(MirroredBufferPoolOpen *mirroredOpen, Size blcksz);


/*
 *	mdinit() -- Initialize private state for magnetic disk storage manager.
 */
bool
mdinit(void)
{
	MdCxt = AllocSetContextCreate(TopMemoryContext,
								  "MdSmgr",
								  ALLOCSET_DEFAULT_MINSIZE,
								  ALLOCSET_DEFAULT_INITSIZE,
								  ALLOCSET_DEFAULT_MAXSIZE);

	/*
	 * Create pending-operations hashtable if we need it.  Currently, we need
	 * it if we are standalone (not under a postmaster) OR if we are a
	 * bootstrap-mode subprocess of a postmaster (that is, a startup or
	 * bgwriter process).
	 */
	if (!IsUnderPostmaster || IsBootstrapProcessingMode())
	{
		HASHCTL		hash_ctl;

		MemSet(&hash_ctl, 0, sizeof(hash_ctl));
		hash_ctl.keysize = sizeof(PendingOperationTag);
		hash_ctl.entrysize = sizeof(PendingOperationEntry);
		hash_ctl.hash = tag_hash;
		hash_ctl.hcxt = MdCxt;
		pendingOpsTable = hash_create("Pending Ops Table",
									  100L,
									  &hash_ctl,
								   HASH_ELEM | HASH_FUNCTION | HASH_CONTEXT);
	}

	return true;
}

static int errdetail_nonexistent_relation(
	int				error,

	RelFileNode		*relFileNode)
{
#define MAX_MSG_LEN MAXPGPATH + 100

	char *buffer;
	int snprintfResult;

	char *filespaceLocation = NULL;

	struct stat fst;

	if (!FILE_POSSIBLY_DELETED(error))
	{
		/*
		 * No call to errdetail.
		 */
		return 0;
	}

	buffer = (char*)palloc(MAX_MSG_LEN);

	if (IsBuiltinTablespace(relFileNode->spcNode))
	{
		/*
		 * Assume filespace and tablespace exists.
		 */
	}
	else
	{		
		char *tablespacePath;

		/*
		 * Investigate whether the containing directories exist to give more detail.
		 */
		PersistentTablespace_GetFilespacePath(
											relFileNode->spcNode,
											FALSE,
											&filespaceLocation);
		if (filespaceLocation == NULL ||
			strlen(filespaceLocation) == 0)
		{
			elog(ERROR, "Empty primary filespace directory location");
		}

		/*
		 * Does the filespace directory exist?
		 */
		if (stat(filespaceLocation, &fst) < 0)
		{
			int saved_err;

			saved_err = errno;

			if (FILE_POSSIBLY_DELETED(saved_err))
			{
				snprintfResult = 
					snprintf(
						buffer,
						MAX_MSG_LEN,
						"Filespace directory \"%s\" does not exist",
						filespaceLocation);
			}
			else
			{
				snprintfResult = 
					snprintf(
						buffer,
						MAX_MSG_LEN,
						"Filespace directory \"%s\" existence check: %s",
						filespaceLocation,
						strerror(saved_err));
			}
			
			Assert(snprintfResult >= 0);
			Assert(snprintfResult < MAX_MSG_LEN);

			/*
			 * Give DETAIL on the filespace directory!
			 */
			errdetail("%s", buffer);
			pfree(buffer);

			pfree(filespaceLocation);
			return 0;
		}
		else
		{
			if (Debug_persistent_recovery_print)
			{
				/*
				 * Since we are being invoked inside an elog, we don't want to
				 * recurse.  So, use errdetail instead!
				 */
				snprintfResult = 
					snprintf(
						buffer,
						MAX_MSG_LEN,
						"Filespace directory \"%s\" does exist",
						filespaceLocation);
				
				Assert(snprintfResult >= 0);
				Assert(snprintfResult < MAX_MSG_LEN);
				
				errdetail("%s",buffer);
			}
		}
	
		tablespacePath = (char*)palloc(MAXPGPATH + 1);

		FormTablespacePath(
					tablespacePath,
					filespaceLocation,
					relFileNode->spcNode);

		/*
		 * Does the tablespace directory exist?
		 */
		if (stat(tablespacePath, &fst) < 0)
		{
			int saved_err;

			saved_err = errno;

			if (FILE_POSSIBLY_DELETED(saved_err))
			{
				snprintfResult = 
					snprintf(
						buffer,
						MAX_MSG_LEN,
						"Tablespace directory \"%s\" does not exist",
						tablespacePath);
			}
			else
			{
				snprintfResult = 
					snprintf(
						buffer,
						MAX_MSG_LEN,
						"Tablespace directory \"%s\" existence check: %s",
						tablespacePath,
						strerror(saved_err));
			}
			
			Assert(snprintfResult >= 0);
			Assert(snprintfResult < MAX_MSG_LEN);

			/*
			 * Give DETAIL on the tablespace directory!
			 */
			errdetail("%s", buffer);
			pfree(buffer);

			if (filespaceLocation != NULL)
				pfree(filespaceLocation);

			pfree(tablespacePath);
			return 0;
		}
		else
		{
			if (Debug_persistent_recovery_print)
			{
				snprintfResult = 
					snprintf(
						buffer,
						MAX_MSG_LEN,
						"Tablespace directory \"%s\" does exist",
						tablespacePath);
				
				Assert(snprintfResult >= 0);
				Assert(snprintfResult < MAX_MSG_LEN);
				
				errdetail("%s", buffer);
			}
		}

		pfree(tablespacePath);
	}

	if (relFileNode->spcNode != GLOBALTABLESPACE_OID)
	{
		char *dbPath;

		dbPath = (char*)palloc(MAXPGPATH + 1);
		
		FormDatabasePath(
					dbPath,
					filespaceLocation,
					relFileNode->spcNode,
					relFileNode->dbNode);

		/*
		 * Does the database directory exist?
		 */
		if (stat(dbPath, &fst) < 0)
		{
			int saved_err;
		
			saved_err = errno;
		
			if (FILE_POSSIBLY_DELETED(saved_err))
			{
				snprintfResult = 
					snprintf(
						buffer,
						MAX_MSG_LEN,
						"Database directory \"%s\" does not exist",
						dbPath);
			}
			else
			{
				snprintfResult = 
					snprintf(
						buffer,
						MAX_MSG_LEN,
						"Database directory \"%s\" existence check: %s",
						dbPath,
						strerror(saved_err));
			}
			
			Assert(snprintfResult >= 0);
			Assert(snprintfResult < MAX_MSG_LEN);
		
			/*
			 * Give DETAIL on the database directory!
			 */
			errdetail("%s", buffer);
			pfree(buffer);
		
			if (filespaceLocation != NULL)
				pfree(filespaceLocation);

			pfree(dbPath);
			return 0;
		}
		else
		{
			if (Debug_persistent_recovery_print)
			{
				snprintfResult = 
					snprintf(
						buffer,
						MAX_MSG_LEN,
						"Database directory \"%s\" does exist",
						dbPath);
				
				Assert(snprintfResult >= 0);
				Assert(snprintfResult < MAX_MSG_LEN);
				
				errdetail("%s", buffer);
			}
		}

		pfree(dbPath);
	}
	
	pfree(buffer);
	if (filespaceLocation != NULL)
		pfree(filespaceLocation);

	/*
	 * No (normal) call to errdetail.
	 */
	return 0;
}

/*
 *	mdcreatefilespacedir() -- Create a new filespace directory on magnetic disk.
 *
 * If isRedo is true, it's okay for the filespace directory to exist already.
 */
void
mdcreatefilespacedir(
	char						*filespaceLocation,
	int 						*ioError)
{
	*ioError = 0;

	if (MakeDirectory(filespaceLocation, 0700) < 0)
		*ioError = errno;
}

/*
 *	mdcreatetablespacedir() -- Create a new tablespace directory on magnetic disk.
 *
 * If isRedo is true, it's okay for the tablespace directory to exist already.
 */
void
mdcreatetablespacedir(
	Oid							tablespaceOid,
	int							*ioError)
{
	char *filespaceLocation;
	char tablespacePath[MAXPGPATH];

	*ioError = 0;
	
	PersistentTablespace_GetFilespacePath(
										tablespaceOid,
										FALSE,
										&filespaceLocation);

	FormTablespacePath(
				tablespacePath,
				filespaceLocation,
				tablespaceOid);

	if (MakeDirectory(tablespacePath, 0700) < 0)
		*ioError = errno;

	if (filespaceLocation)
		pfree(filespaceLocation);
}

/*
 *	mdcreatedbdir() -- Create a new database directory on magnetic disk.
 *
 * If isRedo is true, it's okay for the database directory to exist already.
 */
void
mdcreatedbdir(
	DbDirNode					*dbDirNode,
	bool						ignoreAlreadyExists,
	int							*ioError)
{
	char *filespaceLocation;
	char dbPath[MAXPGPATH];

	*ioError = 0;

	PersistentTablespace_GetFilespacePath(
										dbDirNode->tablespace,
										FALSE,
										&filespaceLocation);
	
	FormDatabasePath(
					dbPath,
					filespaceLocation,
					dbDirNode->tablespace,
					dbDirNode->database);

	if (MakeDirectory(dbPath, 0700) < 0)
		if (ignoreAlreadyExists &&
			errno != EEXIST)
			*ioError = errno;
	
	if (filespaceLocation)
		pfree(filespaceLocation);
}

/*
 * mdcreaterelationdir() -- Create a new relation directory on magnetic disk.
 */
void mdcreaterelationdir(
	RelFileNode *relFileNode,
	int *ioError)
{
	char *filespaceLocation;
	char relationPath[MAXPGPATH];

	*ioError = 0;

	PersistentTablespace_GetFilespacePath(
								relFileNode->spcNode,
								FALSE,
								&filespaceLocation);
	FormRelationPath(relationPath, filespaceLocation, *relFileNode);

	if (MakeDirectory(relationPath, 0700) < 0)
	{
		*ioError = errno;
	}

	if (filespaceLocation)
	{
		pfree(filespaceLocation);
	}
}

/*
 *	mdcreate() -- Create a new relation file on magnetic disk.
 */
void
mdcreate(
	SMgrRelation 				reln,

	bool 						isLocalBuf, 

	char						*relationName,
	bool						ignoreAlreadyExists,

	int							*primaryError)
{
	MirroredBufferPoolOpen		mirroredOpen;

	*primaryError = 0;

	if (reln->md_mirvec != NULL)
		mdclose(reln);		// Don't assume it has been created -- make sure it gets created on both mirrors.

	Assert(reln->md_mirvec == NULL);
	
	MirroredBufferPool_Create(
					&mirroredOpen,
					&reln->smgr_rnode,
					/* segmentFileNum */ 0,
					relationName,
					primaryError);
	if (primaryError != 0)
	{
		int			savePrimaryError = *primaryError;

		/*
		 * During bootstrap, there are cases where a system relation will be
		 * accessed (by internal backend processes) before the bootstrap
		 * script nominally creates it.  Therefore, allow the file to exist
		 * already, even if ignoreAlreadyExists is not set.	(See also mdopen)
		 */
		if (ignoreAlreadyExists || IsBootstrapProcessingMode())
			MirroredBufferPool_Open(
						&mirroredOpen,
						&reln->smgr_rnode,
						/* segmentFileNum */ 0,
						relationName,
						primaryError);
		if (!MirroredBufferPool_IsActive(&mirroredOpen))
		{
			/* be sure to return the error reported by create, not open */
			*primaryError = savePrimaryError;
			return;
		}
		errno = 0;
	}

	reln->md_mirvec = _mirvec_alloc();

	reln->md_mirvec->mdmir_open = mirroredOpen;
	reln->md_mirvec->mdmir_segno = 0;
#ifndef LET_OS_MANAGE_FILESIZE
	reln->md_mirvec->mdmir_chain = NULL;
#endif
}

/*
 *	mdunlink() -- Unlink a relation.
 *
 * Note that we're passed a RelFileNode --- by the time this is called,
 * there won't be an SMgrRelation hashtable entry anymore.
 *
 * If isRedo is true, it's okay for the relation to be already gone.
 */
bool
mdunlink(
	RelFileNode 				rnode, 

	char						*relationName,
					/* For tracing only.  Can be NULL in some execution paths. */
	
	bool						isRedo,

	bool 						ignoreNonExistence)
{
	bool		 status		  = true;
	int			 primaryError = 0;
	char		 tmp[MAXPGPATH];
	char		*path;
	int			 segmentFileNum;

	/*
	 * We have to clean out any pending fsync requests for the doomed relation,
	 * else the next mdsync() will fail.
	 */
	ForgetRelationFsyncRequests(rnode);

	/* 
	 * Delete All segment file extensions
	 *
	 * This code used to be implemented via glob(), but globbing data is slow
	 * when there are many files in a directory, so using glob is to be avoided.
	 * Instead we perform point lookups for files and delete the ones we find.
	 * There are different rules for this depending on the type of table:
	 *
	 *   Heap Tables: contiguous extensions, no upper bound
	 *   AO Tables: non contiguous extensions [.1 - .127]
	 *   CO Tables: non contiguous extensions
	 *          [  .1 - .127] for first column
	 *          [.128 - .255] for second column
	 *          [.256 - .283] for third column
	 *          etc
	 *
	 * mdunlink is only called on Heap Tables, AO/CO tables are handled by a
	 * different code path.  The following logic assumes that the files are
	 * a single contiguous range of numbers.
	 *
	 * UNDONE: Probably should have mirror do pattern match too.
	 *    It is conceivably possible that the primary/mirror may have a 
	 *    different set of segment files, so doing the pattern match only
	 *    in one place is dangerous.
	 *
	 * UNDONE: This is broken for mirroring !!!
	 *    The fundamental problem is that if we drop a file on the mirror
	 *    then fail over before we have dropped the file on the primary
	 *    then the mirror is unaware that the file still needs to be dropped
	 *    on the old primary.  
	 *
	 * The above two issues are tracked in MPP-11724
	 */

	/* 
	 * We do this in two passes because it is safer to drop the files in reverse
	 * order so as to prevent the creation of holes, but we need to scan forward
	 * to know what files actually exist.
	 */
	path = relpath(rnode);
	for (segmentFileNum = 0; /* break in code */ ; segmentFileNum++)
	{
		struct stat sbuf;
		
		/* the zero segment file does not have the ".0" extension */
		if (segmentFileNum == 0)
			snprintf(tmp, sizeof(tmp), "%s", path);
		else
			snprintf(tmp, sizeof(tmp), "%s/%d", path, segmentFileNum);

		if (stat(tmp, &sbuf) < 0)
			break;  /* No such file, loop is done */
	}
	pfree(path);

	/* If the zero segment didn't exist raise an error if requested */
	if (segmentFileNum == 0 && !ignoreNonExistence)
	{
		primaryError = ENOENT;
		status = false;
	}
		
	/* second pass perform the drops in reverse order: important for REDO */
	for(segmentFileNum--; segmentFileNum >= 0; segmentFileNum--)
	{
		MirroredBufferPool_Drop(&rnode, segmentFileNum, relationName,
							isRedo, &primaryError);
		if (primaryError != 0)
		{
			status = false;
			break;
		}
	}

	errno = primaryError;
	return status;
}

/*
 *	mdrmfilespacedir() -- Remove a filespace directory on magnetic disk.
 *
 * If isRedo is true, it's okay for the filespace directory to exist already.
 */
bool
mdrmfilespacedir(
	Oid							filespaceOid,
	char						*filespaceLocation,
	bool 						ignoreNonExistence)
{
	return RemovePath(filespaceLocation, true);
}

/*
 *	mdrmtablespacedir() -- Remove a tablespace directory on magnetic disk.
 *
 * If isRedo is true, it's okay for the tablespace directory to exist already.
 */
bool
mdrmtablespacedir(
	Oid							tablespaceOid,
	bool 						ignoreNonExistence)
{

	char *filespaceLocation = NULL;
	char tablespacePath[MAXPGPATH];
	bool result;

	PersistentTablespace_GetFilespacePath(
										tablespaceOid,
										FALSE,
										&filespaceLocation);

	/*
	 * We've removed all relations, so all that is left are PG* files and work files.
	 */
	FormTablespacePath(
				tablespacePath,
				filespaceLocation,
				tablespaceOid);
	result = RemovePath(tablespacePath, true);

	if (filespaceLocation != NULL)
		pfree(filespaceLocation);
	return result;
}


/*
 *	mdrmbdir() -- Remove a database directory on magnetic disk.
 *
 * If isRedo is true, it's okay for the database directory to exist already.
 */
bool
mdrmdbdir(
	DbDirNode					*dbDirNode,
	
	bool 						ignoreNonExistence)
{
	char *filespaceLocation;
	char dbPath[MAXPGPATH];
	bool result;

	PersistentTablespace_GetFilespacePath(
										dbDirNode->tablespace,
										FALSE,
										&filespaceLocation);
	
	/*
	 * We've removed all relations, so all that is left are PG* files and work files.
	 */
	FormDatabasePath(
				dbPath,
				filespaceLocation,
				dbDirNode->tablespace,
				dbDirNode->database);
		
	result = RemovePath(dbPath, true);

	if (filespaceLocation != NULL)
		pfree(filespaceLocation);

	return result;
}

/*
 * mdrmrelationdir() -- Remove a physical relation directory on magnetic disk.
 *
 * If isRedo is true, it's OK for the relation directory to exist already.
 */
bool mdrmrelationdir(
	RelFileNode *relFileNode,
	bool ignoreNonExistence)
{
	char *filespaceLocation;
	char relationPath[MAXPGPATH];
	bool result;

	PersistentTablespace_GetFilespacePath(
										relFileNode->spcNode,
										FALSE,
										&filespaceLocation);

	/*
	 * We've removed all relation files.
	 */
	FormRelationPath(
					relationPath,
					filespaceLocation,
					*relFileNode);

	result = RemovePath(relationPath, true);

	if (filespaceLocation != NULL)
	{
		pfree(filespaceLocation);
	}

	return result;
}

/*
 *	mdextend() -- Add a block to the specified relation.
 *
 *		The semantics are basically the same as mdwrite(): write at the
 *		specified position.  However, we are expecting to extend the
 *		relation (ie, blocknum is >= the current EOF), and so in case of
 *		failure we clean up by truncating.
 *
 *		This routine returns true or false, with errno set as appropriate.
 */
bool
mdextend(SMgrRelation reln, BlockNumber blocknum, char *buffer, bool isTemp)
{
	long		seekpos;
#ifdef suppress
	int			nbytes;
#endif
	MdMirVec    *v;

	v = _mdmir_getseg(reln, blocknum, false);

#ifndef LET_OS_MANAGE_FILESIZE
	seekpos = (long) (BLCKSZ * (blocknum % ((BlockNumber) RELSEG_SIZE)));
	Assert(seekpos < BLCKSZ * RELSEG_SIZE);
#else
	seekpos = (long) (BLCKSZ * (blocknum));
#endif

	/*
	 * Note: because caller obtained blocknum by calling _mdnblocks, which did
	 * a seek(SEEK_END), this seek is often redundant and will be optimized
	 * away by fd.c.  It's not redundant, however, if there is a partial page
	 * at the end of the file.	In that case we want to try to overwrite the
	 * partial page with a full page.  It's also not redundant if bufmgr.c had
	 * to dump another buffer of the same file to make room for the new page's
	 * buffer.
	 */

	if (!MirroredBufferPool_Write(
							&v->mdmir_open,
							seekpos,
							buffer,
							BLCKSZ))
		return false;

#ifdef suppress
	// UNDONE: What do we do with this partial write / truncate back madness????
	if (FileSeek(v->mdfd_vfd, seekpos, SEEK_SET) != seekpos)
		return false;

	if ((nbytes = FileWrite(v->mdfd_vfd, buffer, BLCKSZ)) != BLCKSZ)
	{
		if (nbytes > 0)
		{
			int			save_errno = errno;

			/* Remove the partially-written page */
			FileTruncate(v->mdfd_vfd, seekpos);
			FileSeek(v->mdfd_vfd, seekpos, SEEK_SET);
			errno = save_errno;
		}
		return false;
	}
#endif

	if (!isTemp)
	{
		if (!register_dirty_segment(reln, v))
			return false;
	}

#ifndef LET_OS_MANAGE_FILESIZE
	Assert(_mdnblocks(&v->mdmir_open, BLCKSZ) <= ((BlockNumber) RELSEG_SIZE));
#endif

	return true;
}

/*
 *	mdopen() -- Open the specified relation.  ereport's on failure.
 *		(Optionally, can return NULL instead of ereport for ENOENT.)
 *
 * Note we only open the first segment, when there are multiple segments.
 */
static MdMirVec *
mdopen(SMgrRelation reln, bool allowNotFound)
{
	int	primaryError;

	MdMirVec    *v;
	MirroredBufferPoolOpen		mirroredOpen;

	/* No work if already open */
	if (reln->md_mirvec)
		return reln->md_mirvec;

	MirroredBufferPool_Open(
				&mirroredOpen,
				&reln->smgr_rnode,
				/* segmentFileNum */ 0,
				/* relationName */ NULL,		// Ok to be NULL -- we don't know the name here.
				&primaryError);
	if (primaryError != 0)
	{
		/*
		 * During bootstrap, there are cases where a system relation will be
		 * accessed (by internal backend processes) before the bootstrap
		 * script nominally creates it.  Therefore, accept mdopen() as a
		 * substitute for mdcreate() in bootstrap mode only. (See mdcreate)
		 */
		if (IsBootstrapProcessingMode())
		{
			
			MirroredBufferPool_Create(
							&mirroredOpen,
							&reln->smgr_rnode,
							/* segmentFileNum */ 0,
							/* relationName */ NULL,		// Ok to be NULL -- we don't know the name here.
							&primaryError);
		}
		if (!MirroredBufferPool_IsActive(&mirroredOpen))
		{
			int saved_err;

			if (allowNotFound && FILE_POSSIBLY_DELETED(errno))
				return NULL;

			saved_err = errno;
			ereport(ERROR,
					(errcode_for_file_access(),
					 errmsg("could not open relation %u/%u/%u: %s",
							reln->smgr_rnode.spcNode,
							reln->smgr_rnode.dbNode,
							reln->smgr_rnode.relNode,
							strerror(saved_err)),
					 errdetail_nonexistent_relation(saved_err, &reln->smgr_rnode)));
		}
	}

	reln->md_mirvec = v = _mirvec_alloc();

	v->mdmir_open = mirroredOpen;
	v->mdmir_segno = 0;
#ifndef LET_OS_MANAGE_FILESIZE
	v->mdmir_chain = NULL;
	Assert(_mdnblocks(&v->mdmir_open, BLCKSZ) <= ((BlockNumber) RELSEG_SIZE));
#endif

	return v;
}

/*
 *	mdclose() -- Close the specified relation, if it isn't closed already.
 *
 *		Returns true or false with errno set as appropriate.
 */
bool
mdclose(SMgrRelation reln)
{
	MdMirVec    *v = reln->md_mirvec;

	/* No work if already closed */
	if (v == NULL)
		return true;

	reln->md_mirvec = NULL;			/* prevent dangling pointer after error */

#ifndef LET_OS_MANAGE_FILESIZE
	while (v != NULL)
	{
		MdMirVec    *ov = v;

		/* if not closed already */
		if (MirroredBufferPool_IsActive(&v->mdmir_open))
			MirroredBufferPool_Close(&v->mdmir_open);
		/* Now free vector */
		v = v->mdmir_chain;
		pfree(ov);
	}
#else
	if (MirroredBufferPool_IsActive(&v->mdmir_open))
		MirroredBufferPool_Close(&v->mdmir_open);
	pfree(v);
#endif

	return true;
}

/*
 *	mdread() -- Read the specified block from a relation.
 */
bool
mdread(SMgrRelation reln, BlockNumber blocknum, char *buffer)
{
	bool		status;
	long		seekpos;
	int			nbytes;
	MdMirVec    *v;

	v = _mdmir_getseg(reln, blocknum, false);

#ifndef LET_OS_MANAGE_FILESIZE
	seekpos = (long) (BLCKSZ * (blocknum % ((BlockNumber) RELSEG_SIZE)));
	Assert(seekpos < BLCKSZ * RELSEG_SIZE);
#else
	seekpos = (long) (BLCKSZ * (blocknum));
#endif

	if (MirroredBufferPool_SeekSet(&v->mdmir_open, seekpos) != seekpos)
		return false;

	status = true;
	if ((nbytes = MirroredBufferPool_Read(
								&v->mdmir_open,
								seekpos,
								buffer, 
								BLCKSZ)) != BLCKSZ)
	{
		/*
		 * If we are at or past EOF, return zeroes without complaining. Also
		 * substitute zeroes if we found a partial block at EOF.
		 *
		 * XXX this is really ugly, bad design.  However the current
		 * implementation of hash indexes requires it, because hash index
		 * pages are initialized out-of-order.
		 */
		if (nbytes == 0 ||
			(nbytes > 0 && mdnblocks(reln) == blocknum))
			MemSet(buffer, 0, BLCKSZ);
		else
			status = false;
	}

	return status;
}

/*
 *	mdwrite() -- Write the supplied block at the appropriate location.
 */
bool
mdwrite(SMgrRelation reln, BlockNumber blocknum, char *buffer, bool isTemp)
{
	long		seekpos;
	MdMirVec    *v;

	v = _mdmir_getseg(reln, blocknum, false);

#ifndef LET_OS_MANAGE_FILESIZE
	seekpos = (long) (BLCKSZ * (blocknum % ((BlockNumber) RELSEG_SIZE)));
	Assert(seekpos < BLCKSZ * RELSEG_SIZE);
#else
	seekpos = (long) (BLCKSZ * (blocknum));
#endif

	if (!MirroredBufferPool_Write(
							&v->mdmir_open,
							seekpos,
							buffer,
							BLCKSZ))
		return false;

	if (!isTemp)
	{
		if (!register_dirty_segment(reln, v))
			return false;
	}

	return true;
}

/*
 *	mdnblocks() -- Get the number of blocks stored in a relation.
 *
 *		Important side effect: all active segments of the relation are opened
 *		and added to the mdmir_chain list.  If this routine has not been
 *		called, then only segments up to the last one actually touched
 *		are present in the chain.
 *
 *		Returns # of blocks, or InvalidBlockNumber on error.
 */
BlockNumber
mdnblocks(SMgrRelation reln)
{
	MdMirVec    *v = mdopen(reln, false);

#ifndef LET_OS_MANAGE_FILESIZE
	BlockNumber nblocks;
	BlockNumber segno = 0;

	/*
	 * Skip through any segments that aren't the last one, to avoid redundant
	 * seeks on them.  We have previously verified that these segments are
	 * exactly RELSEG_SIZE long, and it's useless to recheck that each time.
	 *
	 * NOTE: this assumption could only be wrong if another backend has
	 * truncated the relation.	We rely on higher code levels to handle that
	 * scenario by closing and re-opening the md fd, which is handled via
	 * relcache flush.  (Since the bgwriter doesn't participate in relcache
	 * flush, it could have segment chain entries for inactive segments;
	 * that's OK because the bgwriter never needs to compute relation size.)
	 */
	while (v->mdmir_chain != NULL)
	{
		segno++;
		v = v->mdmir_chain;
	}

	for (;;)
	{
		nblocks = _mdnblocks(&v->mdmir_open, BLCKSZ);
		if (nblocks > ((BlockNumber) RELSEG_SIZE))
			elog(FATAL, "segment too big");
		if (nblocks < ((BlockNumber) RELSEG_SIZE))
		{
			return (segno * ((BlockNumber) RELSEG_SIZE)) + nblocks;
        }

		/*
		 * If segment is exactly RELSEG_SIZE, advance to next one.
		 */
		segno++;

		if (v->mdmir_chain == NULL)
		{
			/*
			 * Because we pass true for createIfDoesntExist, we will create the next segment (with
			 * zero length) immediately, if the last segment is of length
			 * RELSEG_SIZE.  While perhaps not strictly necessary, this keeps
			 * the logic simple.
			 */
			v->mdmir_chain = _mdmir_openseg(reln, segno, true /* createIfDoesntExist */);
			if (v->mdmir_chain == NULL)
				return InvalidBlockNumber;		/* failed? */
		}

		v = v->mdmir_chain;
	}
#else
	return _mdnblocks(&v->mdmir_open, BLCKSZ);
#endif
}

/*
 *	mdtruncate() -- Truncate relation to specified number of blocks.
 *
 *		Returns # of blocks or InvalidBlockNumber on error.
 */
BlockNumber
mdtruncate(SMgrRelation reln, BlockNumber nblocks, bool isTemp)
{
	MdMirVec    *v;
	BlockNumber curnblk;

#ifndef LET_OS_MANAGE_FILESIZE
	BlockNumber priorblocks;
#endif

	/*
	 * NOTE: mdnblocks makes sure we have opened all active segments, so
	 * that truncation loop will get them all!
	 */
	curnblk = mdnblocks(reln);
	if (curnblk == InvalidBlockNumber)
		return InvalidBlockNumber;		/* mdnblocks failed */
	if (nblocks > curnblk)
		return InvalidBlockNumber;		/* bogus request */
	/*
	 * Resync issues truncate to mirror only. In that case on primary nblocks will be always identical to curnblock
	 * since nblocks is allocated while holding LockRelationForResyncExtension.
	 */
	if (nblocks == curnblk)
		return nblocks;			/* no work */

	v = mdopen(reln, false);

#ifndef LET_OS_MANAGE_FILESIZE
	priorblocks = 0;
	
	while (v != NULL)
	{
		MdMirVec    *ov = v;

		if (priorblocks > nblocks)
		{
			/*
			 * This segment is no longer active (and has already been
			 * unlinked from the mdmir_chain). We truncate the file, but do
			 * not delete it, for reasons explained in the header comments.
			 */
			if (!MirroredBufferPool_Truncate(&v->mdmir_open, 0))
				return InvalidBlockNumber;
			if (!isTemp)
			{
				if (!register_dirty_segment(reln, v))
					return InvalidBlockNumber;
			}
			v = v->mdmir_chain;
			Assert(ov != reln->md_mirvec);	/* we never drop the 1st segment */
			pfree(ov);
		}
		else if (priorblocks + ((BlockNumber) RELSEG_SIZE) > nblocks)
		{
			/*
			 * This is the last segment we want to keep. Truncate the file to
			 * the right length, and clear chain link that points to any
			 * remaining segments (which we shall zap). NOTE: if nblocks is
			 * exactly a multiple K of RELSEG_SIZE, we will truncate the K+1st
			 * segment to 0 length but keep it. This adheres to the invariant
			 * given in the header comments.
			 */
			BlockNumber lastsegblocks = nblocks - priorblocks;

			if (!MirroredBufferPool_Truncate(&v->mdmir_open, lastsegblocks * BLCKSZ))
				return InvalidBlockNumber;
			if (!isTemp)
			{
				if (!register_dirty_segment(reln, v))
					return InvalidBlockNumber;
			}
			v = v->mdmir_chain;
			ov->mdmir_chain = NULL;
		}
		else
		{
			/*
			 * We still need this segment and 0 or more blocks beyond it, so
			 * nothing to do here.
			 */
			v = v->mdmir_chain;
		}
		priorblocks += RELSEG_SIZE;
	}
#else
	if (!MirroredBufferPool_Truncate(&v->mdmir_open, nblocks * BLCKSZ))
		return InvalidBlockNumber;
	if (!isTemp)
	{
		if (!register_dirty_segment(reln, v))
			return InvalidBlockNumber;
	}
#endif

	return nblocks;
}

/*
 *	mdimmedsync() -- Immediately sync a relation to stable storage.
 *
 * Note that only writes already issued are synced; this routine knows
 * nothing of dirty buffers that may exist inside the buffer manager.
 */
bool
mdimmedsync(SMgrRelation reln)
{
	MdMirVec    *v;
	BlockNumber curnblk;

	/*
	 * NOTE: mdnblocks makes sure we have opened all active segments, so
	 * that fsync loop will get them all!
	 */
	curnblk = mdnblocks(reln);
	if (curnblk == InvalidBlockNumber)
		return false;			/* mdnblocks failed */

	v = mdopen(reln, false);

#ifndef LET_OS_MANAGE_FILESIZE
	while (v != NULL)
	{
		if (!MirroredBufferPool_Flush(&v->mdmir_open))
			return false;
		v = v->mdmir_chain;
	}
#else
	if (!MirroredBufferPool_Flush(&v->mdmir_open) < 0)
		return false;
#endif

	return true;
}

/*
 *	mdsync() -- Sync previous writes to stable storage.
 */
bool
mdsync(void)
{
	static bool mdsync_in_progress = false;

	HASH_SEQ_STATUS hstat;
	PendingOperationEntry *entry;
	int			absorb_counter;

	/*
	 * This is only called during checkpoints, and checkpoints should only
	 * occur in processes that have created a pendingOpsTable.
	 */
	if (!pendingOpsTable)
		return false;

	/*
	 * If we are in the bgwriter, the sync had better include all fsync
	 * requests that were queued by backends before the checkpoint REDO
	 * point was determined.  We go that a little better by accepting all
	 * requests queued up to the point where we start fsync'ing.
	 */
	AbsorbFsyncRequests();

	/*
	 * To avoid excess fsync'ing (in the worst case, maybe a never-terminating
	 * checkpoint), we want to ignore fsync requests that are entered into the
	 * hashtable after this point --- they should be processed next time,
	 * instead.  We use mdsync_cycle_ctr to tell old entries apart from new
	 * ones: new ones will have cycle_ctr equal to the incremented value of
	 * mdsync_cycle_ctr.
	 *
	 * In normal circumstances, all entries present in the table at this
	 * point will have cycle_ctr exactly equal to the current (about to be old)
	 * value of mdsync_cycle_ctr.  However, if we fail partway through the
	 * fsync'ing loop, then older values of cycle_ctr might remain when we
	 * come back here to try again.  Repeated checkpoint failures would
	 * eventually wrap the counter around to the point where an old entry
	 * might appear new, causing us to skip it, possibly allowing a checkpoint
	 * to succeed that should not have.  To forestall wraparound, any time
	 * the previous mdsync() failed to complete, run through the table and
	 * forcibly set cycle_ctr = mdsync_cycle_ctr.
	 *
	 * Think not to merge this loop with the main loop, as the problem is
	 * exactly that that loop may fail before having visited all the entries.
	 * From a performance point of view it doesn't matter anyway, as this
	 * path will never be taken in a system that's functioning normally.
	 */
	if (mdsync_in_progress)
	{
		/* prior try failed, so update any stale cycle_ctr values */
		hash_seq_init(&hstat, pendingOpsTable);
		while ((entry = (PendingOperationEntry *) hash_seq_search(&hstat)) != NULL)
		{
			entry->cycle_ctr = mdsync_cycle_ctr;
		}
	}

	/* Advance counter so that new hashtable entries are distinguishable */
	mdsync_cycle_ctr++;

	/* Set flag to detect failure if we don't reach the end of the loop */
	mdsync_in_progress = true;

	/* Now scan the hashtable for fsync requests to process */
	absorb_counter = FSYNCS_PER_ABSORB;
	hash_seq_init(&hstat, pendingOpsTable);
	while ((entry = (PendingOperationEntry *) hash_seq_search(&hstat)) != NULL)
	{
		/*
		 * If the entry is new then don't process it this time.  Note that
		 * "continue" bypasses the hash-remove call at the bottom of the loop.
		 */
		if (entry->cycle_ctr == mdsync_cycle_ctr)
			continue;

		/* Else assert we haven't missed it */
		Assert((CycleCtr) (entry->cycle_ctr + 1) == mdsync_cycle_ctr);

		/*
		 * If fsync is off then we don't have to bother opening the file
		 * at all.  (We delay checking until this point so that changing
		 * fsync on the fly behaves sensibly.)  Also, if the entry is
		 * marked canceled, fall through to delete it.
		 */
		if (enableFsync && !entry->canceled)
		{
			int			failures;

			/*
			 * If in bgwriter, we want to absorb pending requests every so
			 * often to prevent overflow of the fsync request queue.  It is
			 * unspecified whether newly-added entries will be visited by
			 * hash_seq_search, but we don't care since we don't need to
			 * process them anyway.
			 */
			if (--absorb_counter <= 0)
			{
				AbsorbFsyncRequests();
				absorb_counter = FSYNCS_PER_ABSORB;
			}

			/*
			 * The fsync table could contain requests to fsync segments that
			 * have been deleted (unlinked) by the time we get to them.
			 * Rather than just hoping an ENOENT (or EACCES on Windows) error
			 * can be ignored, what we do on error is absorb pending requests
			 * and then retry.  Since mdunlink() queues a "revoke" message
			 * before actually unlinking, the fsync request is guaranteed to
			 * be marked canceled after the absorb if it really was this case.
			 * DROP DATABASE likewise has to tell us to forget fsync requests
			 * before it starts deletions.
			 */
			for (failures = 0; ; failures++)	/* loop exits at "break" */
			{
				SMgrRelation reln;
				MdMirVec    *seg;

				/*
				 * Find or create an smgr hash entry for this relation. This
				 * may seem a bit unclean -- md calling smgr?  But it's really
				 * the best solution.  It ensures that the open file reference
				 * isn't permanently leaked if we get an error here. (You may
				 * say "but an unreferenced SMgrRelation is still a leak!" Not
				 * really, because the only case in which a checkpoint is done
				 * by a process that isn't about to shut down is in the
				 * bgwriter, and it will periodically do smgrcloseall(). This
				 * fact justifies our not closing the reln in the success path
				 * either, which is a good thing since in non-bgwriter cases
				 * we couldn't safely do that.)  Furthermore, in many cases
				 * the relation will have been dirtied through this same smgr
				 * relation, and so we can save a file open/close cycle.
				 */
				reln = smgropen(entry->tag.rnode);

				/*
				 * It is possible that the relation has been dropped or
				 * truncated since the fsync request was entered.  Therefore,
				 * allow ENOENT, but only if we didn't fail already on
				 * this file.  This applies both during _mdfd_getseg() and
				 * during FileSync, since fd.c might have closed the file
				 * behind our back.
				 */
				seg = _mdmir_getseg(reln,
								   entry->tag.segno * ((BlockNumber) RELSEG_SIZE),
								   true);
				if (seg != NULL &&
					MirroredBufferPool_Flush(&seg->mdmir_open))
					break;		/* success; break out of retry loop */

				/*
				 * XXX is there any point in allowing more than one retry?
				 * Don't see one at the moment, but easy to change the
				 * test here if so.
				 */
				if (!FILE_POSSIBLY_DELETED(errno) ||
					failures > 0)
				{
					ereport(LOG,
							(errcode_for_file_access(),
							 errmsg("could not fsync segment %u of relation %u/%u/%u: %m",
									entry->tag.segno,
									entry->tag.rnode.spcNode,
									entry->tag.rnode.dbNode,
									entry->tag.rnode.relNode)));
					hash_seq_term(&hstat);
					return false;
				}
				else
					ereport(DEBUG1,
							(errcode_for_file_access(),
							 errmsg("could not fsync segment %u of relation %u/%u/%u, but retrying: %m",
									entry->tag.segno,
									entry->tag.rnode.spcNode,
									entry->tag.rnode.dbNode,
									entry->tag.rnode.relNode)));

				/*
				 * Absorb incoming requests and check to see if canceled.
				 */
				AbsorbFsyncRequests();
				absorb_counter = FSYNCS_PER_ABSORB;	/* might as well... */

				if (entry->canceled)
					break;
			}	/* end retry loop */
		}

		/*
		 * If we get here, either we fsync'd successfully, or we don't have
		 * to because enableFsync is off, or the entry is (now) marked
		 * canceled.  Okay to delete it.
		 */
		if (hash_search(pendingOpsTable, &entry->tag,
						HASH_REMOVE, NULL) == NULL)
			elog(ERROR, "pendingOpsTable corrupted");
	}	/* end loop over hashtable entries */

	/* Flag successful completion of mdsync */
	mdsync_in_progress = false;

	return true;
}

/*
 * register_dirty_segment() -- Mark a relation segment as needing fsync
 *
 * If there is a local pending-ops table, just make an entry in it for
 * mdsync to process later.  Otherwise, try to pass off the fsync request
 * to the background writer process.  If that fails, just do the fsync
 * locally before returning (we expect this will not happen often enough
 * to be a performance problem).
 *
 * A false result implies I/O failure during local fsync.  errno will be
 * valid for error reporting.
 */
static bool
register_dirty_segment(SMgrRelation reln, MdMirVec *seg)
{
	if (pendingOpsTable)
	{
		/* push it into local pending-ops table */
		RememberFsyncRequest(reln->smgr_rnode, seg->mdmir_segno);
		return true;
	}
	else
	{
		if (ForwardFsyncRequest(reln->smgr_rnode, seg->mdmir_segno))
			return true;
	}

	if (!MirroredBufferPool_Flush(&seg->mdmir_open))
		return false;
	return true;
}

/*
 * RememberFsyncRequest() -- callback from bgwriter side of fsync request
 *
 * We stuff the fsync request into the local hash table for execution
 * during the bgwriter's next checkpoint.
 *
 * The range of possible segment numbers is way less than the range of
 * BlockNumber, so we can reserve high values of segno for special purposes.
 * We define two: FORGET_RELATION_FSYNC means to cancel pending fsyncs for
 * a relation, and FORGET_DATABASE_FSYNC means to cancel pending fsyncs for
 * a whole database.  (These are a tad slow because the hash table has to be
 * searched linearly, but it doesn't seem worth rethinking the table structure
 * for them.)
 */
void
RememberFsyncRequest(RelFileNode rnode, BlockNumber segno)
{
	Assert(pendingOpsTable);

	if (segno == FORGET_RELATION_FSYNC)
	{
		/* Remove any pending requests for the entire relation */
		HASH_SEQ_STATUS hstat;
		PendingOperationEntry *entry;

		hash_seq_init(&hstat, pendingOpsTable);
		while ((entry = (PendingOperationEntry *) hash_seq_search(&hstat)) != NULL)
		{
			if (RelFileNodeEquals(entry->tag.rnode, rnode))
			{
				/* Okay, cancel this entry */
				entry->canceled = true;
			}
		}
	}
	else if (segno == FORGET_DATABASE_FSYNC)
	{
		/* Remove any pending requests for the entire database */
		HASH_SEQ_STATUS hstat;
		PendingOperationEntry *entry;

		hash_seq_init(&hstat, pendingOpsTable);
		while ((entry = (PendingOperationEntry *) hash_seq_search(&hstat)) != NULL)
		{
			if (!OidIsValid(rnode.spcNode) ||
				entry->tag.rnode.spcNode == rnode.spcNode)
			{
				if (entry->tag.rnode.dbNode == rnode.dbNode)
				{
					/* Okay, cancel this entry */
					entry->canceled = true;
				}
			}
		}
	}
	else
	{
		/* Normal case: enter a request to fsync this segment */
		PendingOperationTag key;
		PendingOperationEntry *entry;
		bool		found;

		/* ensure any pad bytes in the hash key are zeroed */
		MemSet(&key, 0, sizeof(key));
		key.rnode = rnode;
		key.segno = segno;

		entry = (PendingOperationEntry *) hash_search(pendingOpsTable,
													  &key,
													  HASH_ENTER,
													  &found);
		/* if new or previously canceled entry, initialize it */
		if (!found || entry->canceled)
		{
			entry->canceled = false;
			entry->cycle_ctr = mdsync_cycle_ctr;
		}
		/*
		 * NB: it's intentional that we don't change cycle_ctr if the entry
		 * already exists.  The fsync request must be treated as old, even
		 * though the new request will be satisfied too by any subsequent
		 * fsync.
		 *
		 * However, if the entry is present but is marked canceled, we should
		 * act just as though it wasn't there.  The only case where this could
		 * happen would be if a file had been deleted, we received but did not
		 * yet act on the cancel request, and the same relfilenode was then
		 * assigned to a new file.  We mustn't lose the new request, but
		 * it should be considered new not old.
		 */
	}
}

/*
 * ForgetRelationFsyncRequests -- ensure any fsyncs for a rel are forgotten
 */
void
ForgetRelationFsyncRequests(RelFileNode rnode)
{
	if (pendingOpsTable)
	{
		/* standalone backend or startup process: fsync state is local */
		RememberFsyncRequest(rnode, FORGET_RELATION_FSYNC);
	}
	else if (IsUnderPostmaster)
	{
		/*
		 * Notify the bgwriter about it.  If we fail to queue the revoke
		 * message, we have to sleep and try again ... ugly, but hopefully
		 * won't happen often.
		 *
		 * XXX should we CHECK_FOR_INTERRUPTS in this loop?  Escaping with
		 * an error would leave the no-longer-used file still present on
		 * disk, which would be bad, so I'm inclined to assume that the
		 * bgwriter will always empty the queue soon.
		 */
		while (!ForwardFsyncRequest(rnode, FORGET_RELATION_FSYNC))
			pg_usleep(10000L);	/* 10 msec seems a good number */
		/*
		 * Note we don't wait for the bgwriter to actually absorb the
		 * revoke message; see mdsync() for the implications.
		 */
	}
}

/*
 * ForgetDatabaseFsyncRequests -- ensure any fsyncs for a DB are forgotten
 */
void
ForgetDatabaseFsyncRequests(Oid tblspc, Oid dbid)
{
	RelFileNode rnode;

	rnode.dbNode = dbid;
	rnode.spcNode = tblspc;
	rnode.relNode = 0;

	if (pendingOpsTable)
	{
		/* standalone backend or startup process: fsync state is local */
		RememberFsyncRequest(rnode, FORGET_DATABASE_FSYNC);
	}
	else if (IsUnderPostmaster)
	{
		/* see notes in ForgetRelationFsyncRequests */
		while (!ForwardFsyncRequest(rnode, FORGET_DATABASE_FSYNC))
			pg_usleep(10000L);	/* 10 msec seems a good number */
	}
}


/*
 *	_mirvec_alloc() -- Make a MdfdVec object.
 */
static MdMirVec *
_mirvec_alloc(void)
{
	return (MdMirVec *) MemoryContextAllocZero(MdCxt, sizeof(MdMirVec));
}

#ifndef LET_OS_MANAGE_FILESIZE

/*
 * Open the specified segment of the relation,
 * and make a MdfdVec object for it.  Returns NULL on failure.
 *
 * @param createIfDoesntExist if true then create the segment file if it doesn't already exist
 */
static MdMirVec *
_mdmir_openseg(SMgrRelation reln, BlockNumber segno, bool createIfDoesntExist)
{
	int	primaryError;

	MdMirVec    *v;

	MirroredBufferPoolOpen		mirroredOpen;

	/* open the file */
	MirroredBufferPool_Open(
					&mirroredOpen,
					&reln->smgr_rnode,
					segno,
					/* relationName */ NULL,		// Ok to be NULL -- we don't know the name here.
					&primaryError);

	if (!MirroredBufferPool_IsActive(&mirroredOpen))
	{
	    if ( createIfDoesntExist )
	    {
	        MirroredBufferPool_Create(
					&mirroredOpen,
					&reln->smgr_rnode,
					segno,
					/* relationName */ NULL,		// Ok to be NULL -- we don't know the name here.
					&primaryError);

        }

        /* check again now that we may have done create */
        if (!MirroredBufferPool_IsActive(&mirroredOpen))
        {
            return NULL;
        }
    }
	/* allocate an mdfdvec entry for it */
	v = _mirvec_alloc();

	/* fill the entry */
	v->mdmir_open = mirroredOpen;
	v->mdmir_segno = segno;
	v->mdmir_chain = NULL;
	Assert(_mdnblocks(&v->mdmir_open, BLCKSZ) <= ((BlockNumber) RELSEG_SIZE));

	/* all done */
	return v;
}
#endif   /* LET_OS_MANAGE_FILESIZE */

/*
 *	_mdfd_getseg() -- Find the segment of the relation holding the
 *		specified block.  ereport's on failure.
 *		(Optionally, can return NULL instead of ereport for ENOENT.)
 */
static MdMirVec *
_mdmir_getseg(SMgrRelation reln, BlockNumber blkno, bool allowNotFound)
{
	MdMirVec    *v = mdopen(reln, allowNotFound);

#ifndef LET_OS_MANAGE_FILESIZE
	BlockNumber segstogo;
	BlockNumber nextsegno;

	if (!v)
		return NULL;			/* only possible if allowNotFound */

	for (segstogo = blkno / ((BlockNumber) RELSEG_SIZE), nextsegno = 1;
		 segstogo > 0;
		 nextsegno++, segstogo--)
	{
		if (v->mdmir_chain == NULL)
		{
			/*
			 * We will create the next segment only if the target block is
			 * within it.  This prevents Sorcerer's Apprentice syndrome if a
			 * bug at higher levels causes us to be handed a ridiculously
			 * large blkno --- otherwise we could create many thousands of
			 * empty segment files before reaching the "target" block.	We
			 * should never need to create more than one new segment per call,
			 * so this restriction seems reasonable.
			 *
			 * BUT: when doing WAL recovery, disable this logic and create
			 * segments unconditionally.  In this case it seems better to
			 * assume the given blkno is good (it presumably came from a
			 * CRC-checked WAL record); furthermore this lets us cope in the
			 * case where we are replaying WAL data that has a write into a
			 * high-numbered segment of a relation that was later deleted.	We
			 * want to go ahead and create the segments so we can finish out
			 * the replay.
			 */
			bool createIfDoesntExist = segstogo == 1 || InRecovery;
			v->mdmir_chain = _mdmir_openseg(reln,
										  nextsegno,
								          createIfDoesntExist);
			if (v->mdmir_chain == NULL)
			{
				int saved_err;
				
				if (allowNotFound && FILE_POSSIBLY_DELETED(errno))
					return NULL;

				saved_err = errno;

				ereport(ERROR,
						(errcode_for_file_access(),
						 errmsg("could not open segment %u of relation %u/%u/%u (target block %u): %s",
								nextsegno,
								reln->smgr_rnode.spcNode,
								reln->smgr_rnode.dbNode,
								reln->smgr_rnode.relNode,
								blkno,
								strerror(saved_err)),
						 errdetail_nonexistent_relation(saved_err, &reln->smgr_rnode)));
			}
		}
		v = v->mdmir_chain;
	}
#endif

	return v;
}

/*
 * Get number of blocks present in a single disk file
 */
static BlockNumber
_mdnblocks(MirroredBufferPoolOpen *mirroredOpen, Size blcksz)
{
	long		len;

	Assert(mirroredOpen != NULL);
	Assert(MirroredBufferPool_IsActive(mirroredOpen));
	
	len = MirroredBufferPool_SeekEnd(mirroredOpen);
	if (len < 0)
		return 0;				/* on failure, assume file is empty */
	return (BlockNumber) (len / blcksz);
}
