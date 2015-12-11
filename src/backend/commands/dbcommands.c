/*-------------------------------------------------------------------------
 *
 * dbcommands.c
 *		Database management commands (create/drop database).
 *
 * Note: database creation/destruction commands use exclusive locks on
 * the database objects (as expressed by LockSharedObject()) to avoid
 * stepping on each others' toes.  Formerly we used table-level locks
 * on pg_database, but that's too coarse-grained.
 *
 * Portions Copyright (c) 2005-2010, Greenplum inc
 * Portions Copyright (c) 1996-2009, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  $PostgreSQL: pgsql/src/backend/commands/dbcommands.c,v 1.187.2.4 2010/03/25 14:45:21 alvherre Exp $
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <fcntl.h>
#include <locale.h>
#include <unistd.h>
#include <sys/stat.h>

#include "access/genam.h"
#include "access/heapam.h"
#include "catalog/heap.h"
#include "access/xact.h"
#include "access/transam.h"				/* InvalidTransactionId */
#include "catalog/catalog.h"
#include "catalog/catquery.h"
#include "catalog/dependency.h"
#include "catalog/indexing.h"
#include "catalog/pg_attribute.h"
#include "catalog/pg_authid.h"
#include "catalog/pg_class.h"
#include "catalog/pg_namespace.h"
#include "catalog/pg_database.h"
#include "catalog/pg_tablespace.h"
#include "commands/comment.h"
#include "commands/dbcommands.h"
#include "commands/filespace.h"
#include "commands/tablespace.h"
#include "mb/pg_wchar.h"
#include "miscadmin.h"
#include "pgstat.h"
#include "postmaster/checkpoint.h"
#include "storage/freespace.h"
#include "storage/ipc.h"
#include "storage/procarray.h"
#include "storage/smgr.h"
#include "utils/acl.h"
#include "utils/builtins.h"
#include "utils/flatfiles.h"
#include "utils/fmgroids.h"
#include "utils/guc.h"
#include "utils/lsyscache.h"
#include "utils/pg_locale.h"
#include "utils/syscache.h"

#include "cdb/cdbdisp.h"
#include "cdb/cdbdispatchresult.h"
#include "cdb/cdbsrlz.h"
#include "cdb/cdbvars.h"
#include "cdb/cdbcat.h"
#include "cdb/cdbpersistentdatabase.h"
#include "cdb/cdbpersistentrelfile.h"
#include "cdb/cdbmirroredfilesysobj.h"
#include "cdb/cdbmirroredappendonly.h"
#include "cdb/cdbmirroredbufferpool.h"
#include "cdb/cdbmirroredflatfile.h"
#include "cdb/cdbdatabaseinfo.h"
#include "cdb/cdbdirectopen.h"
#include "cdb/cdbpersistentfilesysobj.h"

#include "utils/pg_rusage.h"

typedef struct
{
	Oid			src_dboid;		/* source (template) DB */
	Oid			dest_dboid;		/* DB we are trying to create */
} createdb_failure_params;

/* non-export function prototypes */
static bool get_db_info(const char *name, LOCKMODE lockmode,
			Oid *dbIdP, Oid *ownerIdP,
			int *encodingP, bool *dbIsTemplateP, bool *dbAllowConnP,
			Oid *dbLastSysOidP, TransactionId *dbFrozenXidP,
			Oid *metaTablespace, Oid *dataTablespace);
static bool have_createdb_privilege(void);
static bool check_db_file_conflict(Oid db_id);

/*
 * Create target database directories (under transaction).
 */
static void create_target_directories(
	DatabaseInfo 			*info,

	Oid						sourceDefaultTablespace,

	Oid						destDefaultTablespace,

	Oid						destDatabase)
{
	int t;

	for (t = 0; t < info->tablespacesCount; t++)
	{
		ItemPointerData persistentTid;
		int64 persistentSerialNum;

		Oid tablespace = info->tablespaces[t];
		DbDirNode dbDirNode;
		
		CHECK_FOR_INTERRUPTS();
		
		if (tablespace == GLOBALTABLESPACE_OID)
			continue;

		/* The meta data tablespace. */
		if (tablespace == DEFAULTTABLESPACE_OID)
		{
			dbDirNode.tablespace = DEFAULTTABLESPACE_OID;
			dbDirNode.database = destDatabase;

			MirroredFileSysObj_TransactionCreateDbDir(
												&dbDirNode,
												&persistentTid,
												&persistentSerialNum);

			set_short_version(NULL, &dbDirNode, true);
			continue;
		}

		Assert(Gp_role == GP_ROLE_DISPATCH);
		/* This is the original data tablespace which we want to change. */
		if (tablespace == sourceDefaultTablespace)
			tablespace = destDefaultTablespace;

		dbDirNode.tablespace = tablespace;
		dbDirNode.database = destDatabase;

		MirroredFileSysObj_TransactionCreateDbDir(
										&dbDirNode,
										&persistentTid,
										&persistentSerialNum);
		set_short_version(NULL, &dbDirNode, true);
	}
}

static void make_dst_relfilenode(
	Oid						tablespace,

	Oid						relfilenode,

	Oid						srcDefaultTablespace,

	Oid						dstDefaultTablespace,

	Oid						dstDatabase,

	RelFileNode				*dstRelFileNode)
{
	if (tablespace == srcDefaultTablespace)
		tablespace = dstDefaultTablespace;
	
	dstRelFileNode->spcNode = tablespace;
	
	dstRelFileNode->dbNode = dstDatabase;
	
	dstRelFileNode->relNode = relfilenode;
}

typedef struct StoredRelationPersistentInfo
{
	ItemPointerData		tid;

	int32				serialNum;
} StoredRelationPersistentInfo;

static void update_gp_relation_node(
	Relation 				gpRelationNodeRel,
	Oid						dstDefaultTablespace,
	Oid						dstDatabase,
	ItemPointer				gpRelationNodeTid,
	Oid						relationNode,
	int32					segmentFileNum,
	ItemPointer				persistentTid,
	int64					persistentSerialNum)
{
	HeapTupleData	tuple;
	Buffer			buffer;

	bool			nulls[Natts_gp_relfile_node];
	Datum			values[Natts_gp_relfile_node];

	Oid				verifyRelationNode;
	int32			verifySegmentFileNum;
	
	Datum			repl_val[Natts_gp_relfile_node];
	bool			repl_null[Natts_gp_relfile_node];
	bool			repl_repl[Natts_gp_relfile_node];
	HeapTuple		newtuple;

	tuple.t_self = *gpRelationNodeTid;
	
	if (!heap_fetch(gpRelationNodeRel, SnapshotAny,
					&tuple, &buffer, false, NULL))
		elog(ERROR, "Failed to fetch gp_relation_node tuple at TID %s",
			 ItemPointerToString(&tuple.t_self));
	
	heap_deform_tuple(&tuple, RelationGetDescr(gpRelationNodeRel), values, nulls);
	
	Assert(!nulls[Anum_gp_relfile_node_relfilenode_oid - 1]);
	verifyRelationNode = DatumGetObjectId(values[Anum_gp_relfile_node_relfilenode_oid - 1]);

	Assert(verifyRelationNode == relationNode);

	Assert(!nulls[Anum_gp_relfile_node_segment_file_num - 1]);
	verifySegmentFileNum = DatumGetInt32(values[Anum_gp_relfile_node_segment_file_num - 1]);

	Assert(verifySegmentFileNum == segmentFileNum);

	memset(repl_val, 0, sizeof(repl_val));
	memset(repl_null, false, sizeof(repl_null));
	memset(repl_repl, 0, sizeof(repl_null));
	
	repl_val[Anum_gp_relfile_node_persistent_tid - 1] = PointerGetDatum(persistentTid);
	repl_repl[Anum_gp_relfile_node_persistent_tid - 1] = true;
	repl_val[Anum_gp_relfile_node_persistent_serial_num - 1] = Int64GetDatum(persistentSerialNum);
	repl_repl[Anum_gp_relfile_node_persistent_serial_num - 1] = true;
	
	newtuple = heap_modify_tuple(&tuple, RelationGetDescr(gpRelationNodeRel), repl_val, repl_null, repl_repl);
	
	heap_inplace_update(gpRelationNodeRel, newtuple);
	
	heap_freetuple(newtuple);

	ReleaseBuffer(buffer);
}

static void copy_buffer_pool_files(
	RelFileNode 	*srcRelFileNode,
	RelFileNode		*dstRelFileNode,
	char			*relationName,
	ItemPointer		persistentTid,
	int64			persistentSerialNum,
	char			*buffer,
	bool			useWal)
{
	SMgrRelation	srcrel;
	int32			nblocks;

	SMgrRelation	dstrel;

	int32		blkno;

	srcrel = smgropen(*srcRelFileNode);

	nblocks = smgrnblocks(srcrel);

	dstrel = smgropen(*dstRelFileNode);

	if (useWal)
	{
		if (Debug_persistent_print)
		{
			elog(Persistent_DebugPrintLevel(),
				 "copy_buffer_pool_files %u/%u/%u: not bypassing the WAL -- not using bulk load, persistent serial num " INT64_FORMAT ", TID %s",
				 dstRelFileNode->spcNode,
				 dstRelFileNode->dbNode,
				 dstRelFileNode->relNode,
				 persistentSerialNum,
				 ItemPointerToString(persistentTid));
		}
	}
	
	/*
	 * Do the data copying.
	 */
	for (blkno = 0; blkno < nblocks; blkno++)
	{
		xl_heap_newpage xlrec;
		XLogRecPtr	recptr;
		XLogRecData rdata[2];
		
		smgrread(srcrel, blkno, buffer);

		CHECK_FOR_INTERRUPTS();

		if (useWal)
		{
			/*
			 * We XLOG buffer pool relations for 2 reasons:
			 *
			 *   1) To support master mirroring which replays the XLOG to the
			 *       standby master.
			 *   2) Better CREATE DATABASE performance.  If we fsync each file
			 *       as we copy, it slows things down.
			 */
		
			/* NO ELOG(ERROR) from here till newpage op is logged */
			START_CRIT_SECTION();
		
			/* XXX consolidate with heap_logpage() */
			xlrec.heapnode.node = dstrel->smgr_rnode;
			xlrec.heapnode.persistentTid = *persistentTid;
			xlrec.heapnode.persistentSerialNum = persistentSerialNum;
			xlrec.blkno = blkno;
		
			rdata[0].data = (char *) &xlrec;
			rdata[0].len = SizeOfHeapNewpage;
			rdata[0].buffer = InvalidBuffer;
			rdata[0].next = &(rdata[1]);
		
			rdata[1].data = (char *) buffer;
			rdata[1].len = BLCKSZ;
			rdata[1].buffer = InvalidBuffer;
			rdata[1].next = NULL;
		
			recptr = XLogInsert(RM_HEAP_ID, XLOG_HEAP_NEWPAGE, rdata);
		
			PageSetLSN(buffer, recptr);
			PageSetTLI(buffer, ThisTimeLineID);
		
			END_CRIT_SECTION();

		}

		// -------- MirroredLock ----------
		LWLockAcquire(MirroredLock, LW_SHARED);

		smgrwrite(dstrel, blkno, buffer, false);

		LWLockRelease(MirroredLock);
		// -------- MirroredLock ----------
	}

	/*
	 * It's obvious that we must fsync this when not WAL-logging the copy. It's
	 * less obvious that we have to do it even if we did WAL-log the copied
	 * pages. The reason is that since we're copying outside shared buffers, a
	 * CHECKPOINT occurring during the copy has no way to flush the previously
	 * written data to disk (indeed it won't know the new rel even exists).  A
	 * crash later on would replay WAL from the checkpoint, therefore it
	 * wouldn't replay our earlier WAL entries. If we do not fsync those pages
	 * here, they might still not be on disk when the crash occurs.
	 */
	
	// -------- MirroredLock ----------
	LWLockAcquire(MirroredLock, LW_SHARED);
	
	smgrimmedsync(dstrel);
	
	LWLockRelease(MirroredLock);
	// -------- MirroredLock ----------

	smgrclose(dstrel);

	smgrclose(srcrel);
}

static void copy_append_only_segment_file(
	RelFileNode 	*srcRelFileNode,
	RelFileNode		*dstRelFileNode,
	int32			segmentFileNum,
	char			*relationName,
	int64			eof,
	ItemPointer		persistentTid,
	int64			persistentSerialNum,
	char			*buffer)
{
	char srcFileName[MAXPGPATH];
	char dstFileName[MAXPGPATH];
	char extension[12];

	File		srcFile;

	MirroredAppendOnlyOpen mirroredDstOpen;

	int64	endOffset;
	int64	readOffset;
	int32	bufferLen;
	int 	retval;

	int primaryError;

	Assert(eof > 0);

	if (Debug_persistent_print)
		elog(Persistent_DebugPrintLevel(), 
			 "copy_append_only_segment_file: Enter %u/%u/%u, segment file #%d, serial number " INT64_FORMAT ", TID %s, EOF " INT64_FORMAT,
			 dstRelFileNode->spcNode,
			 dstRelFileNode->dbNode,
			 dstRelFileNode->relNode,
			 segmentFileNum,
			 persistentSerialNum,
			 ItemPointerToString(persistentTid),
			 eof);

	if (segmentFileNum > 0)
	{
		sprintf(extension, "/%u", segmentFileNum);
	}
	else
		extension[0] = '\0';
	
	CopyRelPath(srcFileName, MAXPGPATH, *srcRelFileNode);
	if (segmentFileNum > 0)
	{
		strcat(srcFileName, extension);
	}

	/*
	 * Open the files
	 */
	srcFile = PathNameOpenFile(srcFileName, O_RDONLY | PG_BINARY, 0);
	if (srcFile < 0)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not open file \"%s\": %m", srcFileName),
				 errdetail("%s", HdfsGetLastError())));

	CopyRelPath(dstFileName, MAXPGPATH, *dstRelFileNode);
	if (segmentFileNum > 0)
	{
		strcat(dstFileName, extension);
	}

	MirroredAppendOnly_OpenReadWrite(
							&mirroredDstOpen,
							dstRelFileNode,
							segmentFileNum,
							relationName,
							/* logicalEof */ 0,	// NOTE: This is the START EOF.  Since we are copying, we start at 0.
							false,
							&primaryError);
	if (primaryError != 0)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not open file \"%s\": %s", dstFileName, strerror(primaryError)),
				 errdetail("%s", HdfsGetLastError())));

	/*
	 * Do the data copying.
	 */
	endOffset = eof;
	readOffset = 0;
	bufferLen = (Size) Min(2*BLCKSZ, endOffset);
	while (readOffset < endOffset)
	{
		CHECK_FOR_INTERRUPTS();
		
		retval = FileRead(srcFile, buffer, bufferLen);
		if (retval != bufferLen) 
		{
			ereport(ERROR,
					(errcode_for_file_access(),
					 errmsg("could not read from position: " INT64_FORMAT " in file '%s' : %m", readOffset, srcFileName),
					 errdetail("%s", HdfsGetLastError())));
			
			break;
		}						
		
		MirroredAppendOnly_Append(
							  &mirroredDstOpen,
							  buffer,
							  bufferLen,
							  &primaryError/*,
							  &mirrorDataLossOccurred*/);
		if (primaryError != 0)
			ereport(ERROR,
					(errcode_for_file_access(),
					 errmsg("could not write file \"%s\": %s", dstFileName, strerror(primaryError)),
					 errdetail("%s", HdfsGetLastError())));
		
		readOffset += bufferLen;
		
		bufferLen = (Size) Min(2*BLCKSZ, endOffset - readOffset);						
	}

	MirroredAppendOnly_FlushAndClose(
							&mirroredDstOpen,
							&primaryError);
	if (primaryError != 0)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not flush (fsync) file \"%s\": %s", dstFileName, strerror(primaryError)),
				 errdetail("%s", HdfsGetLastError())));

	FileClose(srcFile);
}

// -----------------------------------------------------------------------------

/*
 * CREATE DATABASE
 */
void
createdb(CreatedbStmt *stmt)
{
	Oid			src_dboid = InvalidOid;
	Oid			src_owner;
	int			src_encoding = -1;
	bool		src_istemplate = false;
	bool		src_allowconn;
	Oid			src_lastsysoid = InvalidOid;
	TransactionId src_frozenxid = InvalidTransactionId;
	Oid			src_deftablespace = InvalidOid;
	volatile Oid dst_deftablespace;
	Relation	pg_database_rel;
	HeapTuple	tuple;
	Datum		new_record[Natts_pg_database];
	bool		new_record_nulls[Natts_pg_database];
	Oid			dboid;
	Oid			datdba;
	ListCell   *option;
	DefElem    *dtablespacename = NULL;
	DefElem    *downer = NULL;
	DefElem    *dtemplate = NULL;
	DefElem    *dencoding = NULL;
	DefElem    *dconnlimit = NULL;
	char	   *dbname = stmt->dbname;
	char	   *dbowner = NULL;
	const char *dbtemplate = NULL;
	int			encoding = -1;
	int			dbconnlimit = -1;
	bool		store_meta_on_default_tablespace = false;

	bool		shouldDispatch = (Gp_role == GP_ROLE_DISPATCH);
	cqContext	cqc;
	cqContext  *pcqCtx;

	if (shouldDispatch)
		if (Persistent_BeforePersistenceWork())
			elog(NOTICE, " Create database dispatch before persistence work!");


	if (Gp_role != GP_ROLE_EXECUTE)
	{
		/*
		 * Don't allow master to call this in a transaction block. Segments
		 * are ok as distributed transaction participants. 
		 */
		PreventTransactionChain((void *) stmt, "CREATE DATABASE");
	}

	/* Extract options from the statement node tree */
	foreach(option, stmt->options)
	{
		DefElem    *defel = (DefElem *) lfirst(option);

		if (strcmp(defel->defname, "tablespace") == 0)
		{
			if (dtablespacename)
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("conflicting or redundant options")));
			dtablespacename = defel;
		}
		else if (strcmp(defel->defname, "owner") == 0)
		{
			if (downer)
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("conflicting or redundant options")));
			downer = defel;
		}
		else if (strcmp(defel->defname, "template") == 0)
		{
			if (dtemplate)
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("conflicting or redundant options")));
			dtemplate = defel;
		}
		else if (strcmp(defel->defname, "encoding") == 0)
		{
			if (dencoding)
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("conflicting or redundant options")));
			dencoding = defel;
		}
		else if (strcmp(defel->defname, "connectionlimit") == 0)
		{
			if (dconnlimit)
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("conflicting or redundant options")));
			dconnlimit = defel;
		}
		else if (strcmp(defel->defname, "location") == 0)
		{
			ereport(WARNING,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("LOCATION is not supported anymore"),
					 errhint("Consider using tablespaces instead.")));
		}
		else
			elog(ERROR, "option \"%s\" not recognized",
				 defel->defname);
	}

	if (downer && downer->arg)
		dbowner = strVal(downer->arg);
	if (dtemplate && dtemplate->arg)
		dbtemplate = strVal(dtemplate->arg);
	if (dencoding && dencoding->arg)
	{
		const char *encoding_name;

		if (IsA(dencoding->arg, Integer))
		{
			encoding = intVal(dencoding->arg);
			encoding_name = pg_encoding_to_char(encoding);
			if (strcmp(encoding_name, "") == 0 ||
				pg_valid_server_encoding(encoding_name) < 0)
				ereport(ERROR,
						(errcode(ERRCODE_UNDEFINED_OBJECT),
						 errmsg("%d is not a valid encoding code",
								encoding)));
		}
		else if (IsA(dencoding->arg, String))
		{
			encoding_name = strVal(dencoding->arg);
			if (pg_valid_server_encoding(encoding_name) < 0)
				ereport(ERROR,
						(errcode(ERRCODE_UNDEFINED_OBJECT),
						 errmsg("%s is not a valid encoding name",
								encoding_name)));
			encoding = pg_char_to_encoding(encoding_name);
		}
		else
			elog(ERROR, "unrecognized node type: %d",
				 nodeTag(dencoding->arg));

		if (encoding == PG_SQL_ASCII && shouldDispatch)
		{
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("server encoding 'SQL_ASCII' is not supported")));
		}
	}
	if (dconnlimit && dconnlimit->arg)
		dbconnlimit = intVal(dconnlimit->arg);

	/* obtain OID of proposed owner */
	if (dbowner)
		datdba = get_roleid_checked(dbowner);
	else
		datdba = GetUserId();

	/*
	 * To create a database, must have createdb privilege and must be able to
	 * become the target role (this does not imply that the target role itself
	 * must have createdb privilege).  The latter provision guards against
	 * "giveaway" attacks.	Note that a superuser will always have both of
	 * these privileges a fortiori.
	 */
	if (!have_createdb_privilege())
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
				 errmsg("permission denied to create database")));

	check_is_member_of_role(GetUserId(), datdba);

	/*
	 * Lookup database (template) to be cloned, and obtain share lock on it.
	 * ShareLock allows two CREATE DATABASEs to work from the same template
	 * concurrently, while ensuring no one is busy dropping it in parallel
	 * (which would be Very Bad since we'd likely get an incomplete copy
	 * without knowing it).  This also prevents any new connections from being
	 * made to the source until we finish copying it, so we can be sure it
	 * won't change underneath us.
	 */
	if (!dbtemplate)
		dbtemplate = "template1";		/* Default template database name */

	if (!get_db_info(dbtemplate, ShareLock,
					 &src_dboid, &src_owner, &src_encoding,
					 &src_istemplate, &src_allowconn, &src_lastsysoid,
					 &src_frozenxid, NULL, &src_deftablespace))
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_DATABASE),
				 errmsg("template database \"%s\" does not exist",
						dbtemplate)));

	/*
	 * Permission check: to copy a DB that's not marked datistemplate, you
	 * must be superuser or the owner thereof.
	 */
	if (!src_istemplate)
	{
		if (!pg_database_ownercheck(src_dboid, GetUserId()))
			ereport(ERROR,
					(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
					 errmsg("permission denied to copy database \"%s\"",
							dbtemplate)));
		ereport(ERROR,
				(errcode(ERRCODE_CDB_FEATURE_NOT_SUPPORTED),
				 errmsg("copy non template database is not supported")));
	}

	/*
	 * The source DB can't have any active backends, except this one
	 * (exception is to allow CREATE DB while connected to template1).
	 * Otherwise we might copy inconsistent data.
	 */
	if (src_dboid != TemplateDbOid && DatabaseHasActiveBackends(src_dboid, true))
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_IN_USE),
			errmsg("source database \"%s\" is being accessed by other users",
				   dbtemplate)));

	/* If encoding is defaulted, use source's encoding */
	if (encoding < 0)
		encoding = src_encoding;

	/* Some encodings are client only */
	if (!PG_VALID_BE_ENCODING(encoding))
		ereport(ERROR,
				(errcode(ERRCODE_WRONG_OBJECT_TYPE),
				 errmsg("invalid server encoding %d", encoding)));

	/* check whether encoding and locale are compatible - see also related check in initdb */
	if (gp_encoding_check_locale_compatibility)
	{
		char *lc_ctype = GetConfigOptionByName("lc_ctype", NULL);
		char *lc_collate = GetConfigOptionByName("lc_collate", NULL);
		if (!check_locale_encoding(lc_ctype, encoding))
		{
			ereport(ERROR,
					(errmsg("Encoding mismatch"),
					 errdetail("The encoding you selected (%s) and the encoding that the\n"
								"selected locale uses (%s) do not match.  This would lead to\n"
								"misbehavior in various character string processing functions.\n",
							pg_encoding_to_char(encoding),
							lc_ctype)));
		}
		if(!check_locale_encoding(lc_collate,encoding))
		{
			ereport(ERROR,
					(errmsg("Encoding mismatch"),
					 errdetail("The encoding you selected (%s) and the encoding that the\n"
								"selected locale uses (%s) do not match.  This would lead to\n"
								"misbehavior in various character string processing functions.\n",
							pg_encoding_to_char(encoding),
							lc_collate)));
		}
	}

	/* Resolve default tablespace for new database */
	if (dtablespacename && dtablespacename->arg)
	{
		char	   *tablespacename;
		AclResult	aclresult;

		tablespacename = strVal(dtablespacename->arg);
		dst_deftablespace = get_tablespace_oid(tablespacename);
		if (!OidIsValid(dst_deftablespace))
			ereport(ERROR,
					(errcode(ERRCODE_UNDEFINED_OBJECT),
					 errmsg("tablespace \"%s\" does not exist",
							tablespacename)));
		/* check permissions */
		aclresult = pg_tablespace_aclcheck(dst_deftablespace, GetUserId(),
										   ACL_CREATE);
		if (aclresult != ACLCHECK_OK)
			aclcheck_error(aclresult, ACL_KIND_TABLESPACE,
						   tablespacename);

		/* pg_global must never be the default tablespace */
		if (dst_deftablespace == GLOBALTABLESPACE_OID)
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				  errmsg("pg_global cannot be used as default tablespace")));

		/* don't allow user create database on pg_default*/
		if (dst_deftablespace == DEFAULTTABLESPACE_OID)
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					errmsg("Creating database on tablespace 'pg_default' is not allowed")));

		/*
		 * If we are trying to change the default tablespace of the template,
		 * we require that the template not have any files in the new default
		 * tablespace.	This is necessary because otherwise the copied
		 * database would contain pg_class rows that refer to its default
		 * tablespace both explicitly (by OID) and implicitly (as zero), which
		 * would cause problems.  For example another CREATE DATABASE using
		 * the copied database as template, and trying to change its default
		 * tablespace again, would yield outright incorrect results (it would
		 * improperly move tables to the new default tablespace that should
		 * stay in the same tablespace).
		 */
		if (dst_deftablespace != src_deftablespace)
		{
			char	   *srcpath;
			struct stat st;

			srcpath = GetDatabasePath(src_dboid, dst_deftablespace);

			if (stat(srcpath, &st) == 0 &&
				S_ISDIR(st.st_mode) &&
				!directory_is_empty(srcpath))
				ereport(ERROR,
						(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						 errmsg("cannot assign new default tablespace \"%s\"",
								tablespacename),
						 errdetail("There is a conflict because database \"%s\" already has some tables in this tablespace.",
								   dbtemplate)));
			pfree(srcpath);
		}
	}
	else
	{
		/* Use template database's default tablespace */
		dst_deftablespace = src_deftablespace;
		/* Note there is no additional permission check in this path */
	}

	if (!is_tablespace_updatable(dst_deftablespace))
		store_meta_on_default_tablespace = true;

	/*
	 * Check for db name conflict.	This is just to give a more friendly error
	 * message than "unique index violation".  There's a race condition but
	 * we're willing to accept the less friendly message in that case.
	 */
	if (OidIsValid(get_database_oid(dbname)))
		ereport(ERROR,
				(errcode(ERRCODE_DUPLICATE_DATABASE),
				 errmsg("database \"%s\" already exists", dbname)));

	/*
	 * Select an OID for the new database, checking that it doesn't have
	 * a filename conflict with anything already existing in the tablespace
	 * directories.
	 */
	if (src_dboid == TemplateDbOid)
		pg_database_rel = heap_open(DatabaseRelationId, AccessShareLock);
	else
		pg_database_rel = heap_open(DatabaseRelationId, RowExclusiveLock);
	pcqCtx = caql_beginscan(
							caql_addrel(cqclr(&cqc), pg_database_rel), 
							cql("INSERT INTO pg_database",
								NULL));
	if (Gp_role == GP_ROLE_EXECUTE && stmt->dbOid != 0)
		dboid = stmt->dbOid;
	else
	{
		do
		{
			dboid = GetNewOid(pg_database_rel);
		} while (check_db_file_conflict(dboid));
	}


	/* Remember this for dispatching to segDBs */
	stmt->dbOid = dboid;

	/*
	 * Insert a new tuple into pg_database.  This establishes our ownership of
	 * the new database name (anyone else trying to insert the same name will
	 * block on the unique index, and fail after we commit).
	 */

	/* Form tuple */
	MemSet(new_record, 0, sizeof(new_record));
	MemSet(new_record_nulls, false, sizeof(new_record_nulls));

	new_record[Anum_pg_database_datname - 1] =
		DirectFunctionCall1(namein, CStringGetDatum(dbname));
	new_record[Anum_pg_database_datdba - 1] = ObjectIdGetDatum(datdba);
	new_record[Anum_pg_database_encoding - 1] = Int32GetDatum(encoding);
	new_record[Anum_pg_database_datistemplate - 1] = BoolGetDatum(false);
	new_record[Anum_pg_database_datallowconn - 1] = BoolGetDatum(true);
	new_record[Anum_pg_database_datconnlimit - 1] = Int32GetDatum(dbconnlimit);
	new_record[Anum_pg_database_datlastsysoid - 1] = ObjectIdGetDatum(src_lastsysoid);
	new_record[Anum_pg_database_datfrozenxid - 1] = TransactionIdGetDatum(src_frozenxid);
	new_record[Anum_pg_database_dattablespace - 1] = ObjectIdGetDatum(store_meta_on_default_tablespace ? DEFAULTTABLESPACE_OID : dst_deftablespace);
	new_record[Anum_pg_database_dat2tablespace - 1] = ObjectIdGetDatum(dst_deftablespace);

	/*
	 * We deliberately set datconfig and datacl to defaults (NULL), rather
	 * than copying them from the template database.  Copying datacl would be
	 * a bad idea when the owner is not the same as the template's owner. It's
	 * more debatable whether datconfig should be copied.
	 */
	new_record_nulls[Anum_pg_database_datconfig - 1] = true;
	new_record_nulls[Anum_pg_database_datacl - 1] = true;

	tuple = caql_form_tuple(pcqCtx, 
							new_record, new_record_nulls);

	HeapTupleSetOid(tuple, dboid);

	caql_insert(pcqCtx, tuple); /* implicit update of index as well */

	/*
	 * Now generate additional catalog entries associated with the new DB
	 */

	/* Register owner dependency */
	recordDependencyOnOwner(DatabaseRelationId, dboid, datdba);

	/* Create pg_shdepend entries for objects within database */
	copyTemplateDependencies(src_dboid, dboid);

	/* MPP-6929: metadata tracking */
	if (Gp_role == GP_ROLE_DISPATCH)
		MetaTrackAddObject(DatabaseRelationId,
						   dboid,
						   GetUserId(),
						   "CREATE", "DATABASE"
				);

	
	CHECK_FOR_INTERRUPTS();
	
	/*
	 * Force dirty buffers out to disk, to ensure source database is
	 * up-to-date for the copy.  (We really only need to flush buffers for the
	 * source database, but bufmgr.c provides no API for that.)
	 */
	BufferSync();

	CHECK_FOR_INTERRUPTS();
	
	{
		PGRUsage	ru_start;
		DatabaseInfo *info;
		int r;
		char *buffer;
		ItemPointerData	gpRelationNodePersistentTid;
		int64 gpRelationNodePersistentSerialNum;
		Relation gp_relfile_node;

		MemSet(&gpRelationNodePersistentTid, 0, sizeof(ItemPointerData));
		gpRelationNodePersistentSerialNum = 0;

		/*
		 * Collect information of source database's relations.
		 */
		if (Debug_database_command_print)
			pg_rusage_init(&ru_start);

		info = DatabaseInfo_Collect(
							src_dboid, 
							DEFAULTTABLESPACE_OID,
							/* collectGpRelationNodeInfo */ true,
							/* collectAppendOnlyCatalogSegmentInfo */ true,
							/* scanFileSystem */ true);
		
		if (Debug_database_command_print)
			elog(NOTICE, "collect phase: %s",
				 pg_rusage_show(&ru_start));


		CHECK_FOR_INTERRUPTS();

//		if (Debug_database_command_print)
//			DatabaseInfo_Trace(info);

		/*
		 * Verify integrity of databae information.
		 */
		if (Debug_database_command_print)
			pg_rusage_init(&ru_start);

//		DatabaseInfo_Check(info);

		if (Debug_database_command_print)
			elog(NOTICE, "check phase: %s",
				 pg_rusage_show(&ru_start));

		if (Debug_database_command_print)
			pg_rusage_init(&ru_start);

		/*
		 * Create target database directories (under transaction).
		 * The meta data tablespace is local tablespace, and will be added to
		 * the info structure.
		 */
		create_target_directories(
								info,
								src_deftablespace,
								dst_deftablespace,
								dboid);

		if (Debug_database_command_print)
			elog(NOTICE, "created db dirs phase: %s",
				 pg_rusage_show(&ru_start));

		/*
		 * Create each physical destination relation segment file and copy the data.
		 */
		if (Debug_database_command_print)
			pg_rusage_init(&ru_start);
		
		/* Use palloc to ensure we get a maxaligned buffer */		
		buffer = palloc(2*BLCKSZ);

		for (r = 0; r < info->dbInfoRelArrayCount; r++)
		{
			DbInfoRel *dbInfoRel = &info->dbInfoRelArray[r];

			Oid tablespace;
			Oid relfilenode;

			RelFileNode srcRelFileNode;
			RelFileNode dstRelFileNode;

			PersistentFileSysRelStorageMgr relStorageMgr;

			tablespace = dbInfoRel->reltablespace;
			if (tablespace == GLOBALTABLESPACE_OID)
				continue;

			CHECK_FOR_INTERRUPTS();

			relfilenode = dbInfoRel->relfilenodeOid;
			
			srcRelFileNode.spcNode = tablespace;
			srcRelFileNode.dbNode = info->database;
			srcRelFileNode.relNode = relfilenode;
			
			make_dst_relfilenode(
							tablespace,
							relfilenode,
							src_deftablespace,
							dst_deftablespace,
							dboid,
							&dstRelFileNode);
			
			relStorageMgr = (
					 (dbInfoRel->relstorage == RELSTORAGE_AOROWS ||
					  dbInfoRel->relstorage == RELSTORAGE_PARQUET ) ?
									PersistentFileSysRelStorageMgr_AppendOnly :
									PersistentFileSysRelStorageMgr_BufferPool);

			if (relStorageMgr == PersistentFileSysRelStorageMgr_BufferPool)
			{
				bool useWal;
				
				PersistentFileSysRelStorageMgr localRelStorageMgr;
				PersistentFileSysRelBufpoolKind relBufpoolKind;
				
				useWal = !XLog_CanBypassWal();
				/*
				 * All of heap relations will store on default tablespace.
				 * These must be catalog.
				 */
				if (store_meta_on_default_tablespace)
					dstRelFileNode.spcNode = DEFAULTTABLESPACE_OID;
				GpPersistentRelfileNode_GetRelfileInfo(
													dbInfoRel->relkind,
													dbInfoRel->relstorage,
													dbInfoRel->relam,
													&localRelStorageMgr,
													&relBufpoolKind);
				Assert(localRelStorageMgr == PersistentFileSysRelStorageMgr_BufferPool);

				/*
				 * Generate new page XLOG records with the one persistent TID and
				 * serial number for the Buffer Pool managed relation.
				 */
				MirroredFileSysObj_TransactionCreateBufferPoolFile(
													smgropen(dstRelFileNode),
													relBufpoolKind,
													/* isLocalBuf */ false,
													dbInfoRel->relname,
													/* doJustInTimeDirCreate */ false,
													/* bufferPoolBulkLoad */ !useWal,
													&dbInfoRel->gpRelationNodes[0].persistentTid,		// OUTPUT
													&dbInfoRel->gpRelationNodes[0].persistentSerialNum);	// OUTPUT

				if (dstRelFileNode.relNode == GpRelfileNodeRelationId)
				{
					gpRelationNodePersistentTid = dbInfoRel->gpRelationNodes[0].persistentTid;
					gpRelationNodePersistentSerialNum = dbInfoRel->gpRelationNodes[0].persistentSerialNum;
				}

				copy_buffer_pool_files(
								&srcRelFileNode,
								&dstRelFileNode,
								dbInfoRel->relname,
								&dbInfoRel->gpRelationNodes[0].persistentTid,
								dbInfoRel->gpRelationNodes[0].persistentSerialNum,
								buffer,
								useWal);
			}
			else
			{
				int i;
				ItemPointerData persistentTid;
				int64 persistentSerialNum;
				//first create Relation Directory
				MirroredFileSysObj_TransactionCreateRelationDir(&dstRelFileNode,
						/* doJustInTimeDirCreate */ false,
						&persistentTid,			// OUTPUT
						&persistentSerialNum);	// OUTPUT

				DatabaseInfo_AlignAppendOnly(info, dbInfoRel);

				for (i = 0; i < dbInfoRel->gpRelationNodesCount; i++)
				{
					DbInfoGpRelationNode *dbInfoGpRelationNode = 
												&dbInfoRel->gpRelationNodes[i];

					MirroredFileSysObj_TransactionCreateAppendOnlyFile(
														&dstRelFileNode,
														dbInfoGpRelationNode->segmentFileNum,
														dbInfoRel->relname,
														/* doJustInTimeDirCreate */ false,
														&dbInfoGpRelationNode->persistentTid,			// OUTPUT
														&dbInfoGpRelationNode->persistentSerialNum); 	// OUTPUT


					if (dbInfoGpRelationNode->logicalEof > 0)
					{
						copy_append_only_segment_file(
										&srcRelFileNode,
										&dstRelFileNode,
										dbInfoGpRelationNode->segmentFileNum,
										dbInfoRel->relname,
										dbInfoGpRelationNode->logicalEof,
										&dbInfoGpRelationNode->persistentTid,
										dbInfoGpRelationNode->persistentSerialNum,
										buffer);
					}
				}
			}			
		}

		pfree(buffer);

		if (Debug_database_command_print)
			elog(NOTICE, "copy stored relations phase: %s",
				 pg_rusage_show(&ru_start));

		/*
		 * Use our direct open feature so we can set the persistent information.
		 *
		 * Note:  The index will not get inserts -- we'll rebuild afterwards.
		 * GOH NB: cdbdirectopen: tablespace parameter is used to construct
		 * the static variable for gp_relation_node!
		 */
		gp_relfile_node =
				DirectOpen_GpRelfileNodeOpen(
								store_meta_on_default_tablespace ? DEFAULTTABLESPACE_OID : dst_deftablespace, 
								dboid);

		/*
		 * Manually set the persistence information so it will get recorded in
		 * the insert XLOG records.
		 */
		gp_relfile_node->rd_relationnodeinfo.isPresent = true;
		gp_relfile_node->rd_relationnodeinfo.persistentTid = gpRelationNodePersistentTid;
		gp_relfile_node->rd_relationnodeinfo.persistentSerialNum = gpRelationNodePersistentSerialNum;

		for (r = 0; r < info->dbInfoRelArrayCount; r++)
		{
			DbInfoRel *dbInfoRel = &info->dbInfoRelArray[r];

			int g;
			
			for (g = 0; g < dbInfoRel->gpRelationNodesCount; g++)
			{
				DbInfoGpRelationNode *dbInfoGpRelationNode = 
											&dbInfoRel->gpRelationNodes[g];
			
				update_gp_relation_node(
							gp_relfile_node,
							dst_deftablespace,
							dboid,
							&dbInfoGpRelationNode->gpRelationNodeTid,
							dbInfoRel->relfilenodeOid,
							dbInfoGpRelationNode->segmentFileNum,
							&dbInfoGpRelationNode->persistentTid,		// INPUT
							dbInfoGpRelationNode->persistentSerialNum);	// INPUT
			}

		}

		DirectOpen_GpRelfileNodeClose(gp_relfile_node);
	}

	/*
	 * Close pg_database, but keep lock till commit (this is important to
	 * prevent any risk of deadlock failure while updating flat file)
	 */
	heap_close(pg_database_rel, NoLock);
	caql_endscan(pcqCtx);

	/*
	 * Set flag to update flat database file at commit.
	 */
	database_file_update_needed();
}

/*
 * DROP DATABASE
 */
void
dropdb(const char *dbname, bool missing_ok)
{
	Oid			db_id = InvalidOid;
	bool		db_istemplate = true;
	Oid			defaultTablespace = InvalidOid;
	Relation	pgdbrel;
	bool		sharedStorage;

	AssertArg(dbname);

	if (Gp_role != GP_ROLE_EXECUTE)
	{
		/*
		 * Don't allow master tp call this in a transaction block.  Segments are ok as
		 * distributed transaction participants. 
		 */
		PreventTransactionChain((void *) dbname, "DROP DATABASE");
	}

	if (strcmp(dbname, get_database_name(MyDatabaseId)) == 0)
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_IN_USE),
				 errmsg("cannot drop the currently open database")));

	/*
	 * Look up the target database's OID, and get exclusive lock on it. We
	 * need this to ensure that no new backend starts up in the target
	 * database while we are deleting it (see postinit.c), and that no one is
	 * using it as a CREATE DATABASE template or trying to delete it for
	 * themselves.
	 */
	pgdbrel = heap_open(DatabaseRelationId, RowExclusiveLock);

	if (!get_db_info(dbname, AccessExclusiveLock, &db_id, NULL, NULL,
					 &db_istemplate, NULL, NULL, NULL, &defaultTablespace, NULL))
	{
		if (!missing_ok)
		{
			ereport(ERROR,
					(errcode(ERRCODE_UNDEFINED_DATABASE),
					 errmsg("database \"%s\" does not exist", dbname)));
		}
		else
		{
			if ( missing_ok && Gp_role == GP_ROLE_EXECUTE )
			{
				/* This branch exists just to facilitate cleanup of a failed
				 * CREATE DATABASE.  Other callers will supply missing_ok as
				 * FALSE.  The role check is just insurance. 
				 */
				elog(DEBUG1, "ignored request to drop non-existent "
					 "database \"%s\"", dbname);
				
				heap_close(pgdbrel, RowExclusiveLock);
				return;
			}
			else
			{
				/* Release the lock, since we changed nothing */
				heap_close(pgdbrel, RowExclusiveLock);
				ereport(NOTICE,
						(errmsg("database \"%s\" does not exist, skipping",
								dbname)));
				return;
			}
		}
	}

	/*
	 * Permission checks
	 */
	if (!pg_database_ownercheck(db_id, GetUserId()))
		aclcheck_error(ACLCHECK_NOT_OWNER, ACL_KIND_DATABASE,
					   dbname);

	/*
	 * Disallow dropping a DB that is marked istemplate.  This is just to
	 * prevent people from accidentally dropping template0 or template1; they
	 * can do so if they're really determined ...
	 */
	if (db_istemplate)
		ereport(ERROR,
				(errcode(ERRCODE_WRONG_OBJECT_TYPE),
				 errmsg("cannot drop a template database")));

	/*
	 * Check for active backends in the target database.  (Because we hold the
	 * database lock, no new ones can start after this.)
	 */
	if (DatabaseHasActiveBackends(db_id, false))
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_IN_USE),
				 errmsg("database \"%s\" is being accessed by other users",
						dbname)));

	/* Am I on a shared storage? It will decide the mirror operation. */
	sharedStorage = is_tablespace_shared(get_database_dts(db_id));

	/*
	 * Remove the database's tuple from pg_database.
	 */

	cqContext	cqc2;

	if (0 == caql_getcount(
				caql_addrel(cqclr(&cqc2), pgdbrel),
				cql("DELETE FROM pg_database "
					" WHERE oid = :1 ",
					ObjectIdGetDatum(db_id))))
	{
		elog(ERROR, "cache lookup failed for database %u", db_id);
	}

	/*
	 * Delete any comments associated with the database.
	 */
	DeleteSharedComments(db_id, DatabaseRelationId);

	/*
	 * Remove shared dependency references for the database.
	 */
	dropDatabaseDependencies(db_id);

	/*
	 * Tell the stats collector to forget it immediately, too.
	 */
	pgstat_drop_database(db_id);
	
	/* MPP-6929: metadata tracking */
	MetaTrackDropObject(DatabaseRelationId, db_id);

	/*
	 * Also, clean out any entries in the shared free space map.
	 */
	FreeSpaceMapForgetDatabase(InvalidOid, db_id);

	/*
	 * Force a checkpoint to make sure the bgwriter to push all pages to disk.
	 */
	RequestCheckpoint(true, false);

	/*
	 * Collect information on the database's relations from pg_class and from scanning
	 * the file-system directories.
	 *
	 * Schedule the relation files and database directories for deletion at transaction
	 * commit.
	 */
	{
		DatabaseInfo *info;
		int r;

		DbDirNode dbDirNode;
		PersistentFileSysState state;
		
		ItemPointerData persistentTid;
		int64 persistentSerialNum;
		
		info = DatabaseInfo_Collect(
								db_id, 
								defaultTablespace,
								/* collectGpRelationNodeInfo */ true,
								/* collectAppendOnlyCatalogSegmentInfo */ false,
//								/* scanFileSystem */ true);
								/*
								 * wangz11: info->miscEntry and dbInfoRel->physicalSegmentFiles
								 * collected in DatabaseInfo_Collect are not used.
								 */
								/* scanFileSystem */ false);

//		if (Debug_database_command_print)
//			DatabaseInfo_Trace(info);
		
		if (info->parentlessGpRelationNodesCount > 0)
		{
//			DatabaseInfo_Trace(info);

//			pg_usleep(60 * 1000000L);
			
			elog(ERROR, "Found %d parentless gp_relation_node entries",
				 info->parentlessGpRelationNodesCount);
		}

		/*
		 * Verify the relation information from pg_class matches what we found on disk.
		 */
//		DatabaseInfo_Check(info);

		/*
		 * Schedule relation file deletes for transaction commit.
		 */
		for (r = 0; r < info->dbInfoRelArrayCount; r++)
		{
			DbInfoRel *dbInfoRel = &info->dbInfoRelArray[r];
			
			RelFileNode relFileNode;
			PersistentFileSysRelStorageMgr relStorageMgr;

			int g;
			if (dbInfoRel->reltablespace == GLOBALTABLESPACE_OID)
				continue;
			
			relFileNode.spcNode = dbInfoRel->reltablespace;
			relFileNode.dbNode = db_id;
			relFileNode.relNode = dbInfoRel->relfilenodeOid;

			CHECK_FOR_INTERRUPTS();

			/*
			 * Schedule the relation drop.
			 */
			relStorageMgr = (
					 (dbInfoRel->relstorage == RELSTORAGE_AOROWS ||
					  dbInfoRel->relstorage == RELSTORAGE_PARQUET  ) ?
									PersistentFileSysRelStorageMgr_AppendOnly :
									PersistentFileSysRelStorageMgr_BufferPool);

			if (relStorageMgr == PersistentFileSysRelStorageMgr_BufferPool)
			{
				DbInfoGpRelationNode *dbInfoGpRelationNode;
				
				dbInfoGpRelationNode = &dbInfoRel->gpRelationNodes[0];

				MirroredFileSysObj_ScheduleDropBufferPoolFile(
												&relFileNode,
												/* isLocalBuf */ false,
												dbInfoRel->relname,
												&dbInfoGpRelationNode->persistentTid,
												dbInfoGpRelationNode->persistentSerialNum);
			}
			else
			{
				for (g = 0; g < dbInfoRel->gpRelationNodesCount; g++)
				{
					DbInfoGpRelationNode *dbInfoGpRelationNode;
					
					dbInfoGpRelationNode = &dbInfoRel->gpRelationNodes[g];

					MirroredFileSysObj_ScheduleDropAppendOnlyFile(
											&relFileNode,
											dbInfoGpRelationNode->segmentFileNum,
											dbInfoRel->relname,
											&dbInfoGpRelationNode->persistentTid,
											dbInfoGpRelationNode->persistentSerialNum);
				}

				/* Schedule for dropping relation directory */
				MirroredFileSysObj_ScheduleDropRelationDir(&relFileNode, is_tablespace_shared(relFileNode.spcNode));
			}
		}

		/*
		 * Schedule all persistent database directory removals for transaction commit.
		 */
		PersistentDatabase_DirIterateInit();
		while (PersistentDatabase_DirIterateNext(
										&dbDirNode,
										&state,
										&persistentTid,
										&persistentSerialNum))
		{
			if (dbDirNode.database != db_id)
				continue;

			/*
			 * Database directory objects can linger in 'Drop Pending' state, etc,
			 * when the mirror is down and needs drop work.  So only pay attention
			 * to 'Created' objects.
			 */
			if (state != PersistentFileSysState_Created)
				continue;
	
			elog(LOG, "scheduling database drop of %u/%u",
				 dbDirNode.tablespace, dbDirNode.database);

			MirroredFileSysObj_ScheduleDropDbDir(
											&dbDirNode,
											&persistentTid,
											persistentSerialNum,
											sharedStorage);
		}
	}

	/*
	 * Close pg_database, but keep lock till commit (this is important to
	 * prevent any risk of deadlock failure while updating flat file)
	 */
	heap_close(pgdbrel, NoLock);

	/*
	 * Set flag to update flat database file at commit.
	 */
	database_file_update_needed();
}


/*
 * Rename database
 */
void
RenameDatabase(const char *oldname, const char *newname)
{
	Oid			db_id = InvalidOid;
	HeapTuple	newtup;
	Relation	rel;
	cqContext	cqc;
	cqContext  *pcqCtx;

	/*
	 * Look up the target database's OID, and get exclusive lock on it. We
	 * need this for the same reasons as DROP DATABASE.
	 */
	rel = heap_open(DatabaseRelationId, RowExclusiveLock);

	if (!get_db_info(oldname, AccessExclusiveLock, &db_id, NULL, NULL,
					 NULL, NULL, NULL, NULL, NULL, NULL))
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_DATABASE),
				 errmsg("database \"%s\" does not exist", oldname)));

	/*
	 * XXX Client applications probably store the current database somewhere,
	 * so renaming it could cause confusion.  On the other hand, there may not
	 * be an actual problem besides a little confusion, so think about this
	 * and decide.
	 */
	if (db_id == MyDatabaseId)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("current database may not be renamed")));

	/*
	 * Make sure the database does not have active sessions.  This is the same
	 * concern as above, but applied to other sessions.
	 */
	if (DatabaseHasActiveBackends(db_id, false))
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_IN_USE),
				 errmsg("database \"%s\" is being accessed by other users",
						oldname)));

	/* make sure the new name doesn't exist */
	if (OidIsValid(get_database_oid(newname)))
		ereport(ERROR,
				(errcode(ERRCODE_DUPLICATE_DATABASE),
				 errmsg("database \"%s\" already exists", newname)));

	/* must be owner */
	if (!pg_database_ownercheck(db_id, GetUserId()))
		aclcheck_error(ACLCHECK_NOT_OWNER, ACL_KIND_DATABASE,
					   oldname);

	/* must have createdb rights */
	if (!have_createdb_privilege())
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
				 errmsg("permission denied to rename database")));

	/* rename */

	pcqCtx = caql_addrel(cqclr(&cqc), rel);
	newtup = caql_getfirst(
			pcqCtx,
			cql("SELECT * FROM pg_database "
				" WHERE oid = :1 "
				" FOR UPDATE ",
				ObjectIdGetDatum(db_id)));

	if (!HeapTupleIsValid(newtup))
		elog(ERROR, "cache lookup failed for database %u", db_id);
	namestrcpy(&(((Form_pg_database) GETSTRUCT(newtup))->datname), newname);

	caql_update_current(pcqCtx, newtup); /* implicit update of index as well */

	/* MPP-6929: metadata tracking */
	if (Gp_role == GP_ROLE_DISPATCH)
		MetaTrackUpdObject(DatabaseRelationId,
						   db_id,
						   GetUserId(),
						   "ALTER", "RENAME"
				);

	/*
	 * Close pg_database, but keep lock till commit (this is important to
	 * prevent any risk of deadlock failure while updating flat file)
	 */
	heap_close(rel, NoLock);

	/*
	 * Set flag to update flat database file at commit.
	 */
	database_file_update_needed();
}


/*
 * ALTER DATABASE name ...
 */
void
AlterDatabase(AlterDatabaseStmt *stmt)
{
	Relation	rel;
	HeapTuple	tuple,
				newtuple;
	cqContext	cqc;
	cqContext  *pcqCtx;
	ListCell   *option;
	int			connlimit = -1;
	DefElem    *dconnlimit = NULL;
	Datum		new_record[Natts_pg_database];
	bool		new_record_nulls[Natts_pg_database];
	bool		new_record_repl[Natts_pg_database];
	Oid			dboid = InvalidOid;

	/* Extract options from the statement node tree */
	foreach(option, stmt->options)
	{
		DefElem    *defel = (DefElem *) lfirst(option);

		if (strcmp(defel->defname, "connectionlimit") == 0)
		{
			if (dconnlimit)
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("conflicting or redundant options")));
			dconnlimit = defel;
		}
		else
			elog(ERROR, "option \"%s\" not recognized",
				 defel->defname);
	}

	if (dconnlimit)
		connlimit = intVal(dconnlimit->arg);

	/*
	 * Get the old tuple.  We don't need a lock on the database per se,
	 * because we're not going to do anything that would mess up incoming
	 * connections.
	 */
	rel = heap_open(DatabaseRelationId, RowExclusiveLock);

	pcqCtx = caql_addrel(cqclr(&cqc), rel);

	tuple = caql_getfirst(
			pcqCtx,
			cql("SELECT * FROM pg_database" 
				" WHERE datname = :1 FOR UPDATE", 
				CStringGetDatum(stmt->dbname)));

	if (!HeapTupleIsValid(tuple))
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_DATABASE),
				 errmsg("database \"%s\" does not exist", stmt->dbname)));

	dboid = HeapTupleGetOid(tuple);

	if (!pg_database_ownercheck(dboid, GetUserId()))
		aclcheck_error(ACLCHECK_NOT_OWNER, ACL_KIND_DATABASE,
					   stmt->dbname);

	/*
	 * Build an updated tuple, perusing the information just obtained
	 */
	MemSet(new_record, 0, sizeof(new_record));
	MemSet(new_record_nulls, false, sizeof(new_record_nulls));
	MemSet(new_record_repl, false, sizeof(new_record_repl));

	if (dconnlimit)
	{
		new_record[Anum_pg_database_datconnlimit - 1] = Int32GetDatum(connlimit);
		new_record_repl[Anum_pg_database_datconnlimit - 1] = true;
	}

	newtuple = caql_modify_current(pcqCtx, new_record,
								   new_record_nulls, new_record_repl);
	caql_update_current(pcqCtx, newtuple); 
	/* and Update indexes (implicit) */

	/* MPP-6929: metadata tracking */
	if (Gp_role == GP_ROLE_DISPATCH)
		MetaTrackUpdObject(DatabaseRelationId,
						   dboid,
						   GetUserId(),
						   "ALTER", "CONNECTION LIMIT"
				);

	/* Close pg_database, but keep lock till commit */
	heap_close(rel, NoLock);

	/*
	 * We don't bother updating the flat file since the existing options for
	 * ALTER DATABASE don't affect it.
	 */
}


/*
 * ALTER DATABASE name SET ...
 */
void
AlterDatabaseSet(AlterDatabaseSetStmt *stmt)
{
	char	   *valuestr;
	HeapTuple	tuple,
		newtuple;
	Relation	rel;
	cqContext	cqc;
	cqContext  *pcqCtx;
	Datum		repl_val[Natts_pg_database];
	bool		repl_null[Natts_pg_database];
	bool		repl_repl[Natts_pg_database];
	Oid			dboid = InvalidOid;
	char	   *alter_subtype = "SET"; /* metadata tracking */

	valuestr = flatten_set_variable_args(stmt->variable, stmt->value);

	/*
	 * Get the old tuple.  We don't need a lock on the database per se,
	 * because we're not going to do anything that would mess up incoming
	 * connections.
	 */
	rel = heap_open(DatabaseRelationId, RowExclusiveLock);

	pcqCtx = caql_addrel(cqclr(&cqc), rel);

	tuple = caql_getfirst(
			pcqCtx,
			cql("SELECT * FROM pg_database" 
				" WHERE datname = :1 FOR UPDATE", 
				CStringGetDatum(stmt->dbname)));

	if (!HeapTupleIsValid(tuple))
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_DATABASE),
				 errmsg("database \"%s\" does not exist", stmt->dbname)));

	dboid = HeapTupleGetOid(tuple);

	if (!pg_database_ownercheck(dboid, GetUserId()))
		aclcheck_error(ACLCHECK_NOT_OWNER, ACL_KIND_DATABASE,
					   stmt->dbname);

	memset(repl_repl, false, sizeof(repl_repl));
	repl_repl[Anum_pg_database_datconfig - 1] = true;

	if (strcmp(stmt->variable, "all") == 0 && valuestr == NULL)
	{
		alter_subtype = "RESET ALL";

		ArrayType  *new = NULL;
		Datum		datum;
		bool		isnull;

		/*
		 * in RESET ALL, request GUC to reset the settings array; if none
		 * left, we can set datconfig to null; otherwise use the returned
		 * array
		 */
		datum = heap_getattr(tuple, Anum_pg_database_datconfig,
							 RelationGetDescr(rel), &isnull);
		if (!isnull)
			new = GUCArrayReset(DatumGetArrayTypeP(datum));
		if (new)
		{
			repl_val[Anum_pg_database_datconfig - 1] = PointerGetDatum(new);
			repl_null[Anum_pg_database_datconfig - 1] = false;
		}
		else
		{
			repl_null[Anum_pg_database_datconfig - 1] = true;
			repl_val[Anum_pg_database_datconfig - 1] = (Datum) 0;
		}
	}
	else
	{
		Datum		datum;
		bool		isnull;
		ArrayType  *a;

		repl_null[Anum_pg_database_datconfig - 1] = false;

		datum = heap_getattr(tuple, Anum_pg_database_datconfig,
							 RelationGetDescr(rel), &isnull);

		a = isnull ? NULL : DatumGetArrayTypeP(datum);

		if (valuestr)
			a = GUCArrayAdd(a, stmt->variable, valuestr);
		else
		{
			alter_subtype = "RESET";
			a = GUCArrayDelete(a, stmt->variable);
		}

		if (a)
			repl_val[Anum_pg_database_datconfig - 1] = PointerGetDatum(a);
		else
			repl_null[Anum_pg_database_datconfig - 1] = true;
	}

	newtuple = caql_modify_current(pcqCtx,
								   repl_val, repl_null, repl_repl);
	caql_update_current(pcqCtx, newtuple); 
	/* and Update indexes (implicit) */

	/* MPP-6929: metadata tracking */
	if (Gp_role == GP_ROLE_DISPATCH)
		MetaTrackUpdObject(DatabaseRelationId,
						   dboid,
						   GetUserId(),
						   "ALTER", alter_subtype
				);

	/* Close pg_database, but keep lock till commit */
	heap_close(rel, NoLock);

	/*
	 * We don't bother updating the flat file since ALTER DATABASE SET doesn't
	 * affect it.
	 */
}


/*
 * ALTER DATABASE name OWNER TO newowner
 */
void
AlterDatabaseOwner(const char *dbname, Oid newOwnerId)
{
	HeapTuple	tuple;
	Relation	rel;
	cqContext	cqc;
	cqContext  *pcqCtx;
	Form_pg_database datForm;
	Oid			dboid = InvalidOid;
	/*
	 * Get the old tuple.  We don't need a lock on the database per se,
	 * because we're not going to do anything that would mess up incoming
	 * connections.
	 */
	rel = heap_open(DatabaseRelationId, RowExclusiveLock);

	pcqCtx = caql_addrel(cqclr(&cqc), rel);

	tuple = caql_getfirst(
			pcqCtx,
			cql("SELECT * FROM pg_database" 
				" WHERE datname = :1 FOR UPDATE", 
				CStringGetDatum((char *)dbname)));

	if (!HeapTupleIsValid(tuple))
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_DATABASE),
				 errmsg("database \"%s\" does not exist", dbname)));

	datForm = (Form_pg_database) GETSTRUCT(tuple);

	dboid = HeapTupleGetOid(tuple);

	/*
	 * If the new owner is the same as the existing owner, consider the
	 * command to have succeeded.  This is to be consistent with other
	 * objects.
	 */
	if (datForm->datdba != newOwnerId)
	{
		Datum		repl_val[Natts_pg_database];
		bool		repl_null[Natts_pg_database];
		bool		repl_repl[Natts_pg_database];
		Acl		   *newAcl;
		Datum		aclDatum;
		bool		isNull;
		HeapTuple	newtuple;

		/* Otherwise, must be owner of the existing object */
		if (!pg_database_ownercheck(dboid, GetUserId()))
			aclcheck_error(ACLCHECK_NOT_OWNER, ACL_KIND_DATABASE,
						   dbname);

		/* Must be able to become new owner */
		check_is_member_of_role(GetUserId(), newOwnerId);

		/*
		 * must have createdb rights
		 *
		 * NOTE: This is different from other alter-owner checks in that the
		 * current user is checked for createdb privileges instead of the
		 * destination owner.  This is consistent with the CREATE case for
		 * databases.  Because superusers will always have this right, we need
		 * no special case for them.
		 */
		if (!have_createdb_privilege())
			ereport(ERROR,
					(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
				   errmsg("permission denied to change owner of database")));

		memset(repl_null, false, sizeof(repl_null));
		memset(repl_repl, false, sizeof(repl_repl));

		repl_repl[Anum_pg_database_datdba - 1] = true;
		repl_val[Anum_pg_database_datdba - 1] = ObjectIdGetDatum(newOwnerId);

		/*
		 * Determine the modified ACL for the new owner.  This is only
		 * necessary when the ACL is non-null.
		 */
		aclDatum = heap_getattr(tuple,
								Anum_pg_database_datacl,
								RelationGetDescr(rel),
								&isNull);
		if (!isNull)
		{
			newAcl = aclnewowner(DatumGetAclP(aclDatum),
								 datForm->datdba, newOwnerId);
			repl_repl[Anum_pg_database_datacl - 1] = true;
			repl_val[Anum_pg_database_datacl - 1] = PointerGetDatum(newAcl);
		}

		newtuple = caql_modify_current(pcqCtx,
									   repl_val, repl_null, repl_repl);
		caql_update_current(pcqCtx, newtuple); 
		/* and Update indexes (implicit) */

		heap_freetuple(newtuple);

		/* Update owner dependency reference */
		changeDependencyOnOwner(DatabaseRelationId, HeapTupleGetOid(tuple),
								newOwnerId);

		/* MPP-6929: metadata tracking */
		if (Gp_role == GP_ROLE_DISPATCH)
			MetaTrackUpdObject(DatabaseRelationId,
							   dboid,
							   GetUserId(),
							   "ALTER", "OWNER"
					);

	}

	/* Close pg_database, but keep lock till commit */
	heap_close(rel, NoLock);

	/*
	 * We don't bother updating the flat file since ALTER DATABASE OWNER
	 * doesn't affect it.
	 */
}


/*
 * Helper functions
 */

/*
 * Look up info about the database named "name".  If the database exists,
 * obtain the specified lock type on it, fill in any of the remaining
 * parameters that aren't NULL, and return TRUE.  If no such database,
 * return FALSE.
 */
static bool
get_db_info(const char *name, LOCKMODE lockmode,
			Oid *dbIdP, Oid *ownerIdP,
			int *encodingP, bool *dbIsTemplateP, bool *dbAllowConnP,
			Oid *dbLastSysOidP, TransactionId *dbFrozenXidP,
			Oid *metaTablespace, Oid *dataTablespace)
{
	bool		result = false;
	Relation	relation;

	AssertArg(name);

	/* Caller may wish to grab a better lock on pg_database beforehand... */
	relation = heap_open(DatabaseRelationId, AccessShareLock);
	/* XXX XXX: should this be RowExclusive, depending on lockmode? We
	 * try to get a lock later... */

	/*
	 * Loop covers the rare case where the database is renamed before we can
	 * lock it.  We try again just in case we can find a new one of the same
	 * name.
	 */
	for (;;)
	{
		cqContext	cqc;
		cqContext  *pcqCtx;
		HeapTuple	tuple;
		Oid			dbOid;
		int			fetchCount;

		/*
		 * there's no syscache for database-indexed-by-name, so must do it the
		 * hard way
		 */

		dbOid = caql_getoid_plus(
				caql_addrel(cqclr(&cqc), relation),
				&fetchCount,
				NULL,
				cql("SELECT oid FROM pg_database" 
					" WHERE datname = :1 ", 
					CStringGetDatum((char *) name)));

		if (!fetchCount)
		{
			/* definitely no database of that name */
			break;
		}

		/*
		 * Now that we have a database OID, we can try to lock the DB.
		 */
		if (lockmode != NoLock)
			LockSharedObject(DatabaseRelationId, dbOid, 0, lockmode);

		/*
		 * And now, re-fetch the tuple by OID.	If it's still there and still
		 * the same name, we win; else, drop the lock and loop back to try
		 * again.
		 */
		pcqCtx = caql_beginscan(
				caql_addrel(cqclr(&cqc), relation),
				cql("SELECT * FROM pg_database "
					" WHERE oid = :1 ",
					ObjectIdGetDatum(dbOid)));

		tuple = caql_getnext(pcqCtx);

		if (HeapTupleIsValid(tuple))
		{
			Form_pg_database dbform = (Form_pg_database) GETSTRUCT(tuple);

			if (strcmp(name, NameStr(dbform->datname)) == 0)
			{
				/* oid of the database */
				if (dbIdP)
					*dbIdP = dbOid;
				/* oid of the owner */
				if (ownerIdP)
					*ownerIdP = dbform->datdba;
				/* character encoding */
				if (encodingP)
					*encodingP = dbform->encoding;
				/* allowed as template? */
				if (dbIsTemplateP)
					*dbIsTemplateP = dbform->datistemplate;
				/* allowing connections? */
				if (dbAllowConnP)
					*dbAllowConnP = dbform->datallowconn;
				/* last system OID used in database */
				if (dbLastSysOidP)
					*dbLastSysOidP = dbform->datlastsysoid;
				/* limit of frozen XIDs */
				if (dbFrozenXidP)
					*dbFrozenXidP = dbform->datfrozenxid;
				/* default tablespace for this database */
				if (metaTablespace)
					*metaTablespace = dbform->dattablespace;
				if (dataTablespace)
					*dataTablespace = dbform->dat2tablespace;
				
				caql_endscan(pcqCtx);
				result = true;
				break;
			}
			/* can only get here if it was just renamed */

		}
		caql_endscan(pcqCtx);

		if (lockmode != NoLock)
			UnlockSharedObject(DatabaseRelationId, dbOid, 0, lockmode);
	}

	heap_close(relation, AccessShareLock);

	return result;
}

/* Check if current user has createdb privileges */
static bool
have_createdb_privilege(void)
{
	bool		result = false;
	HeapTuple	utup;
	cqContext  *pcqCtx;

	/* Superusers can always do everything */
	if (superuser())
		return true;

	pcqCtx = caql_beginscan(
			NULL,
			cql("SELECT * FROM pg_authid "
				" WHERE oid = :1 ",
				ObjectIdGetDatum(GetUserId())));

	utup = caql_getnext(pcqCtx);

	if (HeapTupleIsValid(utup))
		result = ((Form_pg_authid) GETSTRUCT(utup))->rolcreatedb;

	caql_endscan(pcqCtx);

	return result;
}

/*
 * Check for existing files that conflict with a proposed new DB OID;
 * return TRUE if there are any
 *
 * If there were a subdirectory in any tablespace matching the proposed new
 * OID, we'd get a create failure due to the duplicate name ... and then we'd
 * try to remove that already-existing subdirectory during the cleanup in
 * remove_dbtablespaces.  Nuking existing files seems like a bad idea, so
 * instead we make this extra check before settling on the OID of the new
 * database.  This exactly parallels what GetNewRelFileNode() does for table
 * relfilenode values.
 */
static bool
check_db_file_conflict(Oid db_id)
{
	bool		result = false;
	cqContext  *pcqCtx;
	HeapTuple	tuple;

	pcqCtx = caql_beginscan(
			NULL,
			cql("SELECT * FROM pg_tablespace ", NULL));

	while (HeapTupleIsValid(tuple = caql_getnext(pcqCtx)))
	{
		Oid			dsttablespace = HeapTupleGetOid(tuple);
		char	   *dstpath;
		struct stat st;

		/* Don't mess with the global tablespace */
		if (dsttablespace == GLOBALTABLESPACE_OID)
			continue;

		dstpath = GetDatabasePath(db_id, dsttablespace);

		if (lstat(dstpath, &st) == 0)
		{
			/* Found a conflicting file (or directory, whatever) */
			pfree(dstpath);
			result = true;
			break;
		}

		pfree(dstpath);
	}

	caql_endscan(pcqCtx);
	return result;
}

/*
 * get_database_oid - given a database name, look up the OID
 *
 * Returns InvalidOid if database name not found.
 */
Oid
get_database_oid(const char *dbname)
{
	Oid			oid;
	int			fetchCount;

	/*
	 * There's no syscache for pg_database indexed by name, so we must look
	 * the hard way.
	 */
	oid = caql_getoid_plus(
			NULL,
			&fetchCount,
			NULL,
			cql("SELECT oid FROM pg_database" 
				" WHERE datname = :1 ", 
				CStringGetDatum((char *) dbname)));

	/* We assume that there can be at most one matching tuple */
	if (!fetchCount)
		oid = InvalidOid;

	return oid;
}


/*
 * get_database_name - given a database OID, look up the name
 *
 * Returns a palloc'd string, or NULL if no such database.
 */
char *
get_database_name(Oid dbid)
{
	char	   *result;

	result = caql_getcstring(
			NULL,
			cql("SELECT datname FROM pg_database "
				" WHERE oid = :1 ",
				ObjectIdGetDatum(dbid)));

	return result;
}

/*
 * get_database_dts
 *	Return the database data tablespace OID.
 */
Oid
get_database_dts(Oid dbid)
{
	HeapTuple	dbtuple;
	Oid	   		result;

	if (IsBootstrapProcessingMode())
		return MyDatabaseTableSpace;

	dbtuple = SearchSysCache(DATABASEOID,
							 ObjectIdGetDatum(dbid),
							 0, 0, 0);
	if (HeapTupleIsValid(dbtuple))
	{
		result = ((Form_pg_database) GETSTRUCT(dbtuple))->dat2tablespace;
		ReleaseSysCache(dbtuple);
	}
	else
		result = InvalidOid;

	return result;
}

/*
 * Forget about a database
 */
void
xlog_prepare_database_drop(Oid tblspc, Oid dbid)
{
	/* Drop pages for this database that are in the shared buffer cache */
	DropDatabaseBuffers(tblspc, dbid);

	/* Also, clean out any entries in the shared free space map */
	FreeSpaceMapForgetDatabase(tblspc, dbid);

	/* Also, clean out any fsync requests that might be pending in md.c */
	ForgetDatabaseFsyncRequests(tblspc, dbid);

	/* Clean out the xlog relcache too */
	XLogDropDatabase(tblspc, dbid);

	elog(LOG, "dropped all buffers, freespace, fsyncs and pending pages "
		 "for database %u/%u", tblspc, dbid);
}

/*
 * DATABASE resource manager's routines
 */
void
dbase_redo(XLogRecPtr beginLoc  __attribute__((unused)), XLogRecPtr lsn  __attribute__((unused)), XLogRecord *record)
{
	uint8		info = record->xl_info & ~XLR_INFO_MASK;

	if (info == XLOG_DBASE_CREATE)
	{
		xl_dbase_create_rec *xlrec = (xl_dbase_create_rec *) XLogRecGetData(record);
		char	   *src_path;
		char	   *dst_path;
		struct stat st;

		src_path = GetDatabasePath(xlrec->src_db_id, xlrec->src_tablespace_id);
		dst_path = GetDatabasePath(xlrec->db_id, xlrec->tablespace_id);

		/*
		 * Our theory for replaying a CREATE is to forcibly drop the target
		 * subdirectory if present, then re-copy the source data. This may be
		 * more work than needed, but it is simple to implement.
		 */
		if (stat(dst_path, &st) == 0 && S_ISDIR(st.st_mode))
		{
			if (!rmtree(dst_path, true))
				ereport(WARNING,
						(errmsg("could not remove database directory \"%s\"",
								dst_path)));
		}

		/*
		 * Force dirty buffers out to disk, to ensure source database is
		 * up-to-date for the copy.  (We really only need to flush buffers for
		 * the source database, but bufmgr.c provides no API for that.)
		 */
		BufferSync();

		/*
		 * Copy this subdirectory to the new location
		 *
		 * We don't need to copy subdirectories
		 */
		copydir(src_path, dst_path, false);
	}
	else
		elog(PANIC, "dbase_redo: unknown op code %u", info);
}

void
dbase_desc(StringInfo buf, XLogRecPtr beginLoc, XLogRecord *record)
{
	uint8		info = record->xl_info & ~XLR_INFO_MASK;
	char		*rec = XLogRecGetData(record);

	if (info == XLOG_DBASE_CREATE)
	{
		xl_dbase_create_rec *xlrec = (xl_dbase_create_rec *) rec;

		appendStringInfo(buf, "create db: copy dir %u/%u to %u/%u",
						 xlrec->src_db_id, xlrec->src_tablespace_id,
						 xlrec->db_id, xlrec->tablespace_id);
	}
	else
		appendStringInfo(buf, "UNKNOWN");
}
