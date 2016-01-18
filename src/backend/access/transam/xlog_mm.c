/*-------------------------------------------------------------------------
 *
 * xlog_mm.c
 *
 * Special xlog handling for master mirroring.
 *
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
 *
 *-------------------------------------------------------------------------
 */
#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

#include "postgres.h"

#include "access/twophase.h"
#include "access/xlog.h"
#include "access/xlogmm.h"
#include "catalog/gp_segment_config.h"
#include "catalog/pg_filespace.h"
#include "catalog/pg_tablespace.h"
#include "cdb/cdbpersistentfilespace.h"
#include "cdb/cdbpersistenttablespace.h"
#include "cdb/cdbpersistentdatabase.h"
#include "cdb/cdbutil.h"
#include "cdb/cdbvars.h"
#include "commands/dbcommands.h"
#include "commands/filespace.h"
#include "commands/tablespace.h"
#include "postmaster/postmaster.h"
#include "storage/freespace.h"
#include "utils/builtins.h"
#include "utils/guc.h"
#include "cdb/cdbpersistentrecovery.h"
#include "cdb/cdbpersistentfilesysobj.h"

static void tblspc_get_filespace_paths(Oid tblspc, char **path);
static void add_filespace_map_entry(fspc_map *m, XLogRecPtr *beginLoc, char *caller);
static void add_tablespace_map_entry(tspc_map *m, XLogRecPtr *beginLoc, char *caller);

/*
 * Remember filespace and tablespace mappings, used so that we know
 * where to write data to during master mirror synchronisation. Note that
 * segment level replay is different, as since we're not streaming we can be
 * sure to get the persistent tables in shape and just used those.
 */
static HTAB *filespace_map_ht = NULL;
static HTAB *tablespace_map_ht = NULL;

/*
 * Unlink the object referenced by path. Tolerate it not existing but
 * do not tolerate any other error.
 *
 * For directories, we remove the whole tree underneath the directory and then
 * the directory itself.
 */
static void
unlink_obj(char *path, uint8 info)
{
	if (info == MMXLOG_REMOVE_DIR)
	{
		/* same behaviour as dropdb(), RemoveFileSpace(), RemoveTableSpace() */
		elog(DEBUG1, "removing directory, as requested %s", path);
		RemovePath(path, true);
	}
	else if (info == MMXLOG_REMOVE_FILE)
	{
		if (Debug_print_qd_mirroring)
			elog(DEBUG1, "unlinking file, as requested %s", path);

		if (RemovePath(path, false) < 0)
		{
			if (errno != ENOENT) /* allow it to already be removed */
				elog(WARNING, "could not unlink %s: %m", path);
		}
	}
	else
		Insist(false);
}

/* Actual replay code */
void
mmxlog_redo(XLogRecPtr beginLoc, XLogRecPtr lsn, XLogRecord *record)
{
	uint8			info = record->xl_info & ~XLR_INFO_MASK;
	xl_mm_fs_obj	*xlrec = (xl_mm_fs_obj *) XLogRecGetData(record);
	char			*path = xlrec->path;

	if (path == NULL)
	{
		/*
		 * A NULL path indicates a problem in looking up the path specified in the xlog record.
		 * The record contains both the master and master mirror dbids and corresponding object
		 * paths. If obj_get_path() returns a NULL pointer, it is most likely xlog record's
		 * master or master mirror dbid does not match the dbid of the current master
		 * (i.e. this process).
		 *
		 * Although this situation should never occur, it is possible that the dbid of the
		 * master and master mirror have changed over time due to adding and dropping a standby
		 * master before and after an expansion. Older xlog records would contain pre-expansion
		 * dbids for the standby master, and post expansion records would contain the new dbid
		 * for the standby master. If this process is the new standby master (i.e. after a
		 * master mirror takeover), then it will not recognize the older standby master dbid
		 * in the xlog record.
		 *
		 * This situation should never occur, because a standby takeover should never be
		 * in a situation where it needs to read the xlog before the more recent stand by
		 * master initialization (i.e. the latest standby master initialization would have
		 * generated a checkpoint with no active transaction before that checkpoint.
		 *
		 * Another possibility is that a user copied the xlog from another system (i.e. master),
		 * and placed it here. The user should never do this, but if they do, then the dbids
		 * would not match.
		 *
		 * The other possibility is that the xlog record contained a NULL path for the object.
		 * This should also never happen.
		 */
		elog(ERROR, "The object's path can not be constructed based on the xlog record. ");
	}

	/* Standby should only touch local storage. */
	if (GPStandby() && xlrec->shared)
	{
		elog(DEBUG1, "Standby redo skip none-local path: %s", path);
		return;
	}

	if (info == MMXLOG_CREATE_DIR)
	{
		bool dir_created = true;

		elog(DEBUG1, "got create directory request: %s", path);
		if (Debug_persistent_recovery_print)
		{
			elog(PersistentRecovery_DebugPrintLevel(),
				 "mmxlog_redo: create directory request %d: path \"%s\", filespace %u, path \"%s\"",
				 info,
				 path,
				 xlrec->filespace,
				 xlrec->path);
		}

		/* There is no new filespace and tablespace for a Standby. */
		if (MakeDirectory(path, 0700) == 0)
		{
			if (Debug_persistent_recovery_print)
			{
				elog(PersistentRecovery_DebugPrintLevel(),
					 "mmxlog_redo: Re-created directory \"%s\"",
					 path);
			}
		}
		else
		{
			/*
			 * Allowed to already exist.
			 */
			if (errno != EEXIST)
			{
				if (GPStandby())
				{
					elog(ERROR, "could not create directory \"%s\": %m",
						 path);
				}
				else
				{
					elog(LOG, "Note: unable a create directory \"%s\" from Master Mirroring redo: %m",
						 path);
				}
			}
			else
			{
				if (Debug_persistent_recovery_print)
				{
					elog(PersistentRecovery_DebugPrintLevel(),
						 "mmxlog_redo: Directory \"%s\" already exists",
						 path);
				}
			}
			// UNDONE: This isn't idempotent!  What if the directory create succeeds but the PG_VERSION create fails in a system crash?
			dir_created = false;

		}

		/* need to add PG_VERSION for newly created databases */
		if (xlrec->objtype == MM_OBJ_DATABASE && dir_created == true)
			set_short_version(path, NULL, false);
	}
	else if (info == MMXLOG_REMOVE_DIR)
	{
		if (Debug_persistent_recovery_print)
		{
			elog(PersistentRecovery_DebugPrintLevel(),
				 "mmxlog_redo: remove directory request %s: path \"%s\", filespace %u, path \"%s\"",
				 (xlrec->objtype == MM_OBJ_FILESPACE ? "filespace" :
				    (xlrec->objtype == MM_OBJ_TABLESPACE ? "tablespace" :
					   (xlrec->objtype == MM_OBJ_DATABASE ? "database" :
						   "unknown"))),
				 path,
				 xlrec->filespace,
				 xlrec->path);
		}

		/* tablespace and database should be fine */
		unlink_obj(path, info);
	}
	else if (info == MMXLOG_CREATE_FILE)
	{
		if (Debug_persistent_recovery_print)
		{
			elog(PersistentRecovery_DebugPrintLevel(),
				 "mmxlog_redo: create file request %d: path \"%s\", filespace %u, %u/%u/%u, path \"%s\"",
				 info,
				 path,
				 xlrec->filespace,
				 xlrec->tablespace,
				 xlrec->database,
				 xlrec->relfilenode,
				 xlrec->path);
		}

		/* Local file and hdfs file need different flags for open(). */
		int fd = -1;

		if (!xlrec->shared)
			fd = PathNameOpenFile(path, O_CREAT | O_EXCL | PG_BINARY, 0600);
		else
		{
			if (HdfsPathExist(path))
				errno = EEXIST;
			else
				fd = PathNameOpenFile(path, O_CREAT | O_WRONLY, 0600);
		}

		if (fd < 0)
		{
			/* tolerate existence */
			if (errno != EEXIST)
				elog(WARNING, "could open open file %s: %m", path);
		}

		if (fd >= 0)
			FileClose(fd);
	}
	else if (info == MMXLOG_REMOVE_FILE)
	{
		RelFileNode rnode;

		Insist(xlrec->objtype == MM_OBJ_RELFILENODE);

		if (Debug_persistent_recovery_print)
		{
			elog(PersistentRecovery_DebugPrintLevel(),
				 "mmxlog_redo: remove file request %d: path \"%s\", filespace %u, %u/%u/%u, path \"%s\"",
				 info,
				 path,
				 xlrec->filespace,
				 xlrec->tablespace,
				 xlrec->database,
				 xlrec->relfilenode,
				 xlrec->path);
		}

		rnode.spcNode = xlrec->tablespace;
		rnode.dbNode = xlrec->database;
		rnode.relNode = xlrec->relfilenode;

		XLogDropRelation(rnode);

		if (AmActiveMaster())
		{
			RC4PersistentTablespaceGetFilespaces tablespaceGetFilespaces;

			char *primaryFilespaceLocation;

			Oid filespaceOid;

			/*
			 * If we are re-doing Master Mirroring work on the Master and the tablespace
			 * doesn't exist in the shared-memory persistent hash-table, skip the unlink...
			 */
			tablespaceGetFilespaces =
				PersistentTablespace_TryGetFilespacePath(
															rnode.spcNode,
															&primaryFilespaceLocation,
															&filespaceOid);
			switch (tablespaceGetFilespaces)
			{
			case RC4PersistentTablespaceGetFilespaces_Ok:
				break;

			case RC4PersistentTablespaceGetFilespaces_TablespaceNotFound:
				elog(LOG, "Note: unable find tablespace %u from Master Mirroring redo",
					 rnode.spcNode);
				return;

			case RC4PersistentTablespaceGetFilespaces_FilespaceNotFound:
				elog(LOG, "Note: unable find filespace %u for tablespace %u for Master Mirroring redo",
					 filespaceOid, rnode.spcNode);
				return;

			default:
				elog(ERROR, "Unexpected tablespace filespace fetch result: %d",
					 tablespaceGetFilespaces);
			}
		}

		/*
		 * QD needs remove the other segments files.
		 */
		if (xlrec->shared)
		{
			RemovePath(path, false);
		}
		else
			smgrdounlink(
					&rnode,
					/* isLocalBuf */ false,
					/* relationName */ NULL,
					/* isRedo */ true,		// Don't generate Master Mirroring records...
					/* ignoreNonExistence */ true);
	}
	else
		elog(PANIC, "unknown mmxlog op code %u", info);
}

/*
 * For log output
 */
void
mmxlog_desc(StringInfo buf, XLogRecPtr beginLoc, XLogRecord *record)
{
	uint8		info = record->xl_info & ~XLR_INFO_MASK;
	char		*rec = XLogRecGetData(record);
	xl_mm_fs_obj *xlrec = (xl_mm_fs_obj *) rec;
	char *path = xlrec->path;

	if (path == NULL)
	{
		/*
		 * A NULL path indicates a problem in looking up the path specified in the xlog record.
		 * The record contains both the master and master mirror dbids and corresponding object
		 * paths. If obj_get_path() returns a NULL pointer, it is most likely xlog record's
		 * master or master mirror dbid does not match the dbid of the current master
		 * (i.e. this process).
		 *
		 * Although this situation should never occur, it is possible that the dbid of the
		 * master and master mirror have changed over time due to adding and dropping a standby
		 * master before and after an expansion. Older xlog records would contain pre-expansion
		 * dbids for the standby master, and post expansion records would contain the new dbid
		 * for the standby master. If this process is the new standby master (i.e. after a
		 * master mirror takeover), then it will not recognize the older standby master dbid
		 * in the xlog record.
		 *
		 * This situation should never occur, because a standby takeover should never be
		 * in a situation where it needs to read the xlog before the more recent stand by
		 * master initialization (i.e. the latest standby master initialization would have
		 * generated a checkpoint with no active transaction before that checkpoint.
		 *
		 * Another possibility is that a user copied the xlog from another system (i.e. master),
		 * and placed it here. The user should never do this, but if they do, then the dbids
		 * would not match.
		 *
		 * The other possibility is that the xlog record contained a NULL path for the object.
		 * This should also never happen.
		 */
		elog(ERROR, "The object's path can not be constructed based on the xlog record. ");
	}

	if (info == MMXLOG_CREATE_DIR)
		appendStringInfo(buf, "create directory: path \"%s\", filespace %u",
						 path,
						 xlrec->filespace);
	else if (info == MMXLOG_REMOVE_DIR)
		appendStringInfo(buf, "remove directory: path \"%s\", filespace %u",
						 path,
						 xlrec->filespace);
	else if (info == MMXLOG_CREATE_FILE)
		appendStringInfo(buf, "create file: path \"%s\", filespace %u",
						 path,
						 xlrec->filespace);
	else if (info == MMXLOG_REMOVE_FILE)
		appendStringInfo(buf, "remove file: path \"%s\", filespace %u",
						 path,
						 xlrec->filespace);
	else
		appendStringInfo(buf, "UNKNOWN");
}

/* convert and oid to a string */
static char *
oidtoa(Oid oid)
{
	char *tmp = palloc(11); /* maximum OID is UINT_MAX */

	pg_ltoa(oid, tmp);

	return tmp;
}

static char *
append_file_parts(mm_fs_obj_type type,
				  char *str, Oid tablespace, Oid database, Oid relfilenode,
				  uint32 segnum)
{
	if (type == MM_OBJ_FILESPACE)
		return str; /* already done in base path */

	/* first two are the special tablespaces */
	if (tablespace == DEFAULTTABLESPACE_OID)
		join_path_components(str, str, "base");
	else if (tablespace == GLOBALTABLESPACE_OID)
		join_path_components(str, str, "global");
	else
		join_path_components(str, str, oidtoa(tablespace));

	if (OidIsValid(database))
		join_path_components(str, str, oidtoa(database));

	if (OidIsValid(relfilenode))
	{
		join_path_components(str, str, oidtoa(relfilenode));

		/* Is this a segment > 0 ? If so, add the segment file number */
		if (segnum)
			sprintf(str + strlen(str), "/%u", segnum);
	}

	return str;
}

/*
 * Guts of logging for creation or destruction of filesystem objects on
 * the master.
 */
static bool
emit_mmxlog_fs_record(mm_fs_obj_type type, Oid filespace,
					  Oid tablespace, Oid database, Oid relfilenode,
					  ItemPointer persistentTid, int64 persistentSerialNum,
					  int32 segnum, uint8 flags, XLogRecPtr *beginLoc)
{
	XLogRecData		rdata;
	xl_mm_fs_obj	xlrec;
	char			*path;

	MemSet(beginLoc, 0, sizeof(XLogRecPtr));

	if (!CanEmitXLogRecords)
		return false;

	if (!AmActiveMaster())
		return false;

	Insist(Gp_role == GP_ROLE_DISPATCH ||
		   Gp_role == GP_ROLE_UTILITY);

	if (type == MM_OBJ_FILESPACE)
	{
		Insist(OidIsValid(filespace));
		PersistentFilespace_GetLocation(filespace, &path);
	}
	else
	{
		Insist(OidIsValid(tablespace));

		tblspc_get_filespace_paths(tablespace, &path);

		filespace = PersistentTablespace_GetFileSpaceOid(tablespace);
	}

	/*
	 * Make a non-transactional XLOG entry showing the file creation. It's
	 * non-transactional because we should replay it whether the transaction
	 * commits or not; if not, the file will be dropped at abort time.
	 */
	xlrec.objtype = type;
	xlrec.filespace = filespace;
	xlrec.tablespace = tablespace;
	xlrec.database = database;
	xlrec.relfilenode = relfilenode;
	xlrec.segnum = segnum;
	xlrec.shared = (SYSTEMFILESPACE_OID!=filespace); //cannot call is_filespace_shared(filespace); here, it will cause deadlock with PersistentObjLock
    xlrec.persistentTid = *persistentTid;
    xlrec.persistentSerialNum = persistentSerialNum;

	Insist(!path || strlen(path) <= MAXPGPATH);

	xlrec.path[0] = '\0';
	if (path)
		StrNCpy(xlrec.path, path, sizeof(xlrec.path));
	else
	{
		/*
		 * Allow relative paths if we didn't get anything when we looked up
		 * the filespace. We must allow this for the default filespace.
		 */
		xlrec.path[0] = '.';
		xlrec.path[1] = '\0';
	}

	append_file_parts(type, xlrec.path, tablespace, database,
					  relfilenode, segnum);

	if (Debug_print_qd_mirroring)
		elog(DEBUG1, "XLOG: type = %i, flags = %i, path = %s",
			 type, flags, xlrec.path);

	rdata.data = (char *) &xlrec;
	rdata.len = sizeof(xlrec);
	rdata.buffer = InvalidBuffer;
	rdata.next = NULL;

	XLogInsert(RM_MMXLOG_ID, flags | XLOG_NO_TRAN, &rdata);
	*beginLoc = XLogLastInsertBeginLoc();

	return true;
}

/* External interface to filespace removal logging */
void
mmxlog_log_remove_filespace(Oid filespace,ItemPointer persistentTid, int64 persistentSerialNum)
{
	bool emitted;
	XLogRecPtr beginLoc;

	if (Debug_print_qd_mirroring)
		elog(DEBUG1, "emitting drop filespace record for %u",
			 filespace);
	emitted =
		emit_mmxlog_fs_record(
						  MM_OBJ_FILESPACE,
						  filespace,
						  InvalidOid /* tablespace */,
						  InvalidOid /* database */,
						  InvalidOid /* relfilenode */,
						  persistentTid,
						  persistentSerialNum,
						  0 /* segnum */,
						  MMXLOG_REMOVE_DIR,
						  &beginLoc);
	if (Debug_persistent_recovery_print)
	{
		SUPPRESS_ERRCONTEXT_DECLARE;

		SUPPRESS_ERRCONTEXT_PUSH();

		elog(PersistentRecovery_DebugPrintLevel(),
			 "mmxlog_log_remove_filespace: delete filespace %u (emitted %s, beginLoc %s)",
			 filespace,
			 (emitted ? "true" : "false"),
			 XLogLocationToString(&beginLoc));

		SUPPRESS_ERRCONTEXT_POP();
	}
}

/* External interface to tablespace removal logging */
void
mmxlog_log_remove_tablespace(Oid tablespace,ItemPointer persistentTid, int64 persistentSerialNum)
{
	bool emitted;
	XLogRecPtr beginLoc;

	if (Debug_print_qd_mirroring)
		elog(DEBUG1, "emitting drop tablespace record for %u",
			 tablespace);
	emitted =
		emit_mmxlog_fs_record(
						  MM_OBJ_TABLESPACE,
						  InvalidOid /* filespace */,
						  tablespace,
						  InvalidOid /* database */,
						  InvalidOid /* relfilenode */,
						  persistentTid,
						  persistentSerialNum,
						  0 /* segnum */,
						  MMXLOG_REMOVE_DIR,
						  &beginLoc);
	if (Debug_persistent_recovery_print)
	{
		SUPPRESS_ERRCONTEXT_DECLARE;

		SUPPRESS_ERRCONTEXT_PUSH();

		elog(PersistentRecovery_DebugPrintLevel(),
			 "mmxlog_log_remove_tablespace: delete tablespace %u (emitted %s, beginLoc %s)",
			 tablespace,
			 (emitted ? "true" : "false"),
			 XLogLocationToString(&beginLoc));

		SUPPRESS_ERRCONTEXT_POP();
	}
}

/* External interface to database removal logging */
void
mmxlog_log_remove_database(Oid tablespace, Oid database,
		ItemPointer persistentTid, int64 persistentSerialNum)
{
	bool emitted;
	XLogRecPtr beginLoc;

	if (Debug_print_qd_mirroring)
		elog(DEBUG1, "emitting drop database record for %u/%u",
			 tablespace, database);
	emitted =
		emit_mmxlog_fs_record(
						  MM_OBJ_DATABASE,
						  InvalidOid /* filespace */,
						  tablespace,
						  database,
						  InvalidOid /* relfilenode */,
						  persistentTid,
						  persistentSerialNum,
						  0 /* segnum */,
						  MMXLOG_REMOVE_DIR,
						  &beginLoc);
	if (Debug_persistent_recovery_print)
	{
		SUPPRESS_ERRCONTEXT_DECLARE;

		SUPPRESS_ERRCONTEXT_PUSH();

		elog(PersistentRecovery_DebugPrintLevel(),
			 "mmxlog_log_remove_database: delete database directory %u/%u (emitted %s, beginLoc %s)",
			 tablespace,
			 database,
			 (emitted ? "true" : "false"),
			 XLogLocationToString(&beginLoc));

		SUPPRESS_ERRCONTEXT_POP();
	}
}

/* External interface to relation removal logging */
void
mmxlog_log_remove_relation(Oid tablespace, Oid database, Oid relfilenode,
		ItemPointer persistentTid, int64 persistentSerialNum)
{
	bool emitted;
	XLogRecPtr beginLoc;

	if (Debug_print_qd_mirroring)
	{
		elog(DEBUG1, "emitting drop relation record for %u/%u/%u",
			tablespace, database, relfilenode);
	}

	emitted = emit_mmxlog_fs_record(
								MM_OBJ_RELATION,
								InvalidOid /* fielspace */,
								tablespace,
								database,
								relfilenode,
								persistentTid,
								persistentSerialNum,
								0 /* segnum */,
								MMXLOG_REMOVE_DIR,
								&beginLoc);

	if (Debug_persistent_recovery_print)
	{
		SUPPRESS_ERRCONTEXT_DECLARE;
		SUPPRESS_ERRCONTEXT_PUSH();

		elog(PersistentRecovery_DebugPrintLevel(),
			"mmxlog_log_remove_relation: delete relation directory %u/%u/%u (emitted %s, beginLoc %s)",
			tablespace,
			database,
			relfilenode,
			(emitted ? "true" : "false"),
			XLogLocationToString(&beginLoc));

		SUPPRESS_ERRCONTEXT_POP();
	}
}

/* External interface to relfilenode removal logging */
void
mmxlog_log_remove_relfilenode(Oid tablespace, Oid database, Oid relfilenode,
							  int32 segnum,ItemPointer persistentTid, int64 persistentSerialNum)
{
	bool emitted;
	XLogRecPtr beginLoc;

	if (Debug_print_qd_mirroring)
		elog(DEBUG1, "emitting drop relfilenode record for %u/%u/%u",
			 tablespace, database, relfilenode);
	emitted =
		emit_mmxlog_fs_record(
						  MM_OBJ_RELFILENODE,
						  InvalidOid /* filespace */,
						  tablespace,
						  database,
						  relfilenode,
						  persistentTid,
						  persistentSerialNum,
						  segnum,
						  MMXLOG_REMOVE_FILE,
						  &beginLoc);
	if (Debug_persistent_recovery_print)
	{
		SUPPRESS_ERRCONTEXT_DECLARE;

		SUPPRESS_ERRCONTEXT_PUSH();

		elog(PersistentRecovery_DebugPrintLevel(),
			 "mmxlog_log_remove_relfilenode: delete relation %u/%u/%u, segment file #%d (emitted %s, beginLoc %s)",
			 tablespace,
			 database,
			 relfilenode,
			 segnum,
			 (emitted ? "true" : "false"),
			 XLogLocationToString(&beginLoc));

		SUPPRESS_ERRCONTEXT_POP();
	}
}

/* External interface to filespace creation logging */
void
mmxlog_log_create_filespace(Oid filespace,ItemPointer persistentTid, int64 persistentSerialNum)
{
	bool emitted;
	XLogRecPtr beginLoc;

	emitted =
		emit_mmxlog_fs_record(
						  MM_OBJ_FILESPACE,
						  filespace,
						  InvalidOid /* tablespace */,
						  InvalidOid /* database */,
						  InvalidOid /* relfilenode */,
						  persistentTid,
						  persistentSerialNum,
						  0 /* segnum */,
						  MMXLOG_CREATE_DIR,
						  &beginLoc);
	if (Debug_persistent_recovery_print)
	{
		SUPPRESS_ERRCONTEXT_DECLARE;

		SUPPRESS_ERRCONTEXT_PUSH();

		elog(PersistentRecovery_DebugPrintLevel(),
			 "mmxlog_log_create_filespace: create filespace %u (emitted %s, beginLoc %s)",
			 filespace,
			 (emitted ? "true" : "false"),
			 XLogLocationToString(&beginLoc));

		SUPPRESS_ERRCONTEXT_POP();
	}
}

/* External interface to tablespace creation logging */
void
mmxlog_log_create_tablespace(Oid filespace, Oid tablespace, ItemPointer persistentTid, int64 persistentSerialNum)
{
	bool emitted;
	XLogRecPtr beginLoc;

	emitted =
		emit_mmxlog_fs_record(
						  MM_OBJ_TABLESPACE,
						  filespace,
						  tablespace,
						  InvalidOid /* database */,
						  InvalidOid /* relfilenode */,
						  persistentTid,
						  persistentSerialNum,
						  0 /* segnum */,
						  MMXLOG_CREATE_DIR,
						  &beginLoc);
	if (Debug_persistent_recovery_print)
	{
		SUPPRESS_ERRCONTEXT_DECLARE;

		SUPPRESS_ERRCONTEXT_PUSH();

		elog(PersistentRecovery_DebugPrintLevel(),
			 "mmxlog_log_create_tablespace: create tablespace %u (filespace %u, emitted %s, beginLoc %s)",
			 tablespace,
			 filespace,
			 (emitted ? "true" : "false"),
			 XLogLocationToString(&beginLoc));

		SUPPRESS_ERRCONTEXT_POP();
	}
}

/* External interface to database creation logging */
void
mmxlog_log_create_database(Oid tablespace, Oid database,
		ItemPointer persistentTid, int64 persistentSerialNum)
{
	bool emitted;
	XLogRecPtr beginLoc;

	emitted =
		emit_mmxlog_fs_record(
						  MM_OBJ_DATABASE,
						  InvalidOid /* filespace */,
						  tablespace,
						  database,
						  InvalidOid /* relfilenode */,
						  persistentTid,
						  persistentSerialNum,
						  0 /* segnum */,
						  MMXLOG_CREATE_DIR,
						  &beginLoc);
	if (Debug_persistent_recovery_print)
	{
		SUPPRESS_ERRCONTEXT_DECLARE;

		SUPPRESS_ERRCONTEXT_PUSH();

		elog(PersistentRecovery_DebugPrintLevel(),
			 "mmxlog_log_create_database: create database directory %u/%u (emitted %s, beginLoc %s)",
			 tablespace,
			 database,
			 (emitted ? "true" : "false"),
			 XLogLocationToString(&beginLoc));

		SUPPRESS_ERRCONTEXT_POP();
	}
}

/*
 * External interface to relation create logging
 */
void
mmxlog_log_create_relation(Oid tablespace, Oid database, Oid relfilenode,
		ItemPointer persistentTid, int64 persistentSerialNum)
{
	bool emitted;
	XLogRecPtr beginLoc;

	emitted = emit_mmxlog_fs_record(
								MM_OBJ_RELATION,
								InvalidOid /* filespace */,
								tablespace,
								database,
								relfilenode,
								persistentTid,
								persistentSerialNum,
								0 /*segnum */,
								MMXLOG_CREATE_DIR,
								&beginLoc);

	if (Debug_persistent_recovery_print)
	{
		SUPPRESS_ERRCONTEXT_DECLARE;
		SUPPRESS_ERRCONTEXT_PUSH();

		elog(PersistentRecovery_DebugPrintLevel(),
			"mmxlog_log_create_relation: create relation directory %u/%u/%u (emitted %s, beginLoc %s)",
			tablespace,
			database,
			relfilenode,
			(emitted ? "true" : "false"),
			XLogLocationToString(&beginLoc));

		SUPPRESS_ERRCONTEXT_POP();
	}
}

/* External interface to relfilenode creation logging */
void
mmxlog_log_create_relfilenode(Oid tablespace, Oid database,
							  Oid relfilenode, int32 segnum,
							  ItemPointer persistentTid, int64 persistentSerialNum)
{
	bool emitted;
	XLogRecPtr beginLoc;

	emitted =
		emit_mmxlog_fs_record(
						  MM_OBJ_RELFILENODE,
						  InvalidOid /* filespace */,
						  tablespace,
						  database,
						  relfilenode,
						  persistentTid,
						  persistentSerialNum,
						  segnum,
						  MMXLOG_CREATE_FILE,
						  &beginLoc);
	if (Debug_persistent_recovery_print)
	{
		SUPPRESS_ERRCONTEXT_DECLARE;

		SUPPRESS_ERRCONTEXT_PUSH();

		elog(PersistentRecovery_DebugPrintLevel(),
			 "mmxlog_log_create_relfilenode: create relation %u/%u/%u, segment file #%d (emitted %s, beginLoc %s)",
			 tablespace,
			 database,
			 relfilenode,
			 segnum,
			 (emitted ? "true" : "false"),
			 XLogLocationToString(&beginLoc));

		SUPPRESS_ERRCONTEXT_POP();
	}
}

/*
 * We need to make sure the memory for these maps doesn't
 away.
 */
static void *
map_alloc(Size sz)
{
	/* XXX: might have to allocate in a higher memory context */
	return palloc(sz);
}

/*
 * Generic initialisation of hash table.
 */
static HTAB *
init_hash(const char *name, Size keysize, Size entrysize, int initialSize)
{
	HASHCTL ctl;

	ctl.keysize = keysize;
	ctl.entrysize = entrysize;
	ctl.alloc = map_alloc;
	return hash_create(name,
					   initialSize,
					   &ctl,
					   HASH_ELEM | HASH_ALLOC);
}

static void
mmxlog_empty_filespace_hashtable(char *caller)
{
	HASH_SEQ_STATUS iterateStatus;
	fspc_map *entry;
	int i;
	bool found;

	hash_seq_init(&iterateStatus, filespace_map_ht);

	i = 0;
	while (true)
	{
		entry =
			(fspc_map*)
					hash_seq_search(&iterateStatus);
		if (entry == NULL)
			break;

		if (Debug_persistent_recovery_print)
		{
			elog(PersistentRecovery_DebugPrintLevel(),
				 "mmxlog_empty_filespace_hashtable[%d]: filespace %u, dbid1 %d, dbid2 %d (caller '%s')",
				 i,
				 entry->filespaceoid,
				 entry->dbid1,
				 entry->dbid2,
				 caller);
		}
		entry = hash_search(filespace_map_ht,
							&(entry->filespaceoid),
							HASH_REMOVE,
							&found);
		if (entry == NULL)
			elog(ERROR, "Corrupted filespace hashtable");
		i++;
	}
	if (Debug_persistent_recovery_print)
	{
		elog(PersistentRecovery_DebugPrintLevel(),
			 "mmxlog_empty_filespace_hashtable: filespace remove count %d (caller '%s')",
			 i,
			 caller);
	}
}

static void
mmxlog_empty_tablespace_hashtable(char *caller)
{
	HASH_SEQ_STATUS iterateStatus;
	tspc_map *entry;
	int i;
	bool found;

	hash_seq_init(&iterateStatus, tablespace_map_ht);

	i = 0;
	while (true)
	{
		entry =
			(tspc_map*)
					hash_seq_search(&iterateStatus);
		if (entry == NULL)
			break;

		if (Debug_persistent_recovery_print)
		{
			elog(PersistentRecovery_DebugPrintLevel(),
				 "mmxlog_empty_tablespace_hashtable[%d]: tablespace %u, (filespace %u, caller '%s')",
				 i,
				 entry->tablespaceoid,
				 entry->filespaceoid,
				 caller);
		}
		entry = hash_search(tablespace_map_ht,
							&(entry->tablespaceoid),
							HASH_REMOVE,
							&found);
		if (entry == NULL)
			elog(ERROR, "Corrupted tablespace hashtable");
		i++;
	}
	if (Debug_persistent_recovery_print)
	{
		elog(PersistentRecovery_DebugPrintLevel(),
			 "mmxlog_empty_tablespace_hashtable: tablespace remove count %d (caller '%s')",
			 i,
			 caller);
	}
}

void
mmxlog_empty_hashtables(void)
{
	mmxlog_empty_filespace_hashtable("mmxlog_empty_hashtables");
	mmxlog_empty_tablespace_hashtable("mmxlog_empty_hashtables");
}


void
mmxlog_print_filespaces(int elevel, char *caller)
{
	HASH_SEQ_STATUS iterateStatus;
	fspc_map *entry;
	int i;

	hash_seq_init(&iterateStatus, filespace_map_ht);

	i = 0;
	while (true)
	{
		entry =
			(fspc_map*)
					hash_seq_search(&iterateStatus);
		if (entry == NULL)
			break;

		elog(elevel,
			 "mmxlog_print_filespaces[%d]: filespace %u (dbid1 %d, path1 \"%s\", dbid2 %d, path2 \"%s\", caller '%s')",
			 i,
			 entry->filespaceoid,
			 entry->dbid1,
			 entry->path1,
			 entry->dbid2,
			 entry->path2,
			 caller);
		i++;
	}
	elog(elevel,
		 "mmxlog_print_filespaces: filespace count %d (caller '%s')",
		 i,
		 caller);
}

void
mmxlog_print_tablespaces(int elevel, char *caller)
{
	HASH_SEQ_STATUS iterateStatus;
	tspc_map *entry;
	int i;

	hash_seq_init(&iterateStatus, tablespace_map_ht);

	i = 0;
	while (true)
	{
		entry =
			(tspc_map*)
					hash_seq_search(&iterateStatus);
		if (entry == NULL)
			break;

		elog(elevel,
			 "mmxlog_print_tablespaces[%d]: tablespace %u, (filespace %u, caller '%s')",
			 i,
			 entry->tablespaceoid,
			 entry->filespaceoid,
			 caller);
		i++;
	}
	elog(elevel,
		 "mmxlog_print_tablespaces: tablespace count %d (caller '%s')",
		 i,
		 caller);
}

/*
 * Add a new mapping to the filespace hash table. We do not support the
 * complementary filespace mapping removal function because we do not want to
 * get into situations where we've removed a filespace but still data to apply
 * to some file in the filespace. Unfortunately, WAL needs to just do what it is
 * told and it could be told to do this if we get the logic wrong on the other
 * end.
 */
static void
add_filespace_map_entry(fspc_map *m, XLogRecPtr *beginLoc, char *caller)
{
	void *entry;
	bool found;

	/*
	 * The table is lazily initialised.
	 */
	if (!filespace_map_ht)
		filespace_map_ht = init_hash("mmxlog filespace map",
									 sizeof(Oid), /* keysize */
									 sizeof(fspc_map),
									 gp_max_filespaces);

	entry = hash_search(filespace_map_ht,
						&(m->filespaceoid),
						HASH_ENTER,
						&found);
	/*
	 * If this is a new entry, we need to add the data, if we found
	 * an entry, we need to update it, so just copy our data
	 * right over the top.
	 */
	memcpy(entry, m, sizeof(fspc_map));

	if (Debug_persistent_recovery_print)
	{
		elog(PersistentRecovery_DebugPrintLevel(),
			 "add_filespace_map_entry: add filespace %u, dbid1 %d, dbid2 %d (beginLoc %s, caller '%s')",
			 m->filespaceoid,
			 m->dbid1,
			 m->dbid2,
			 XLogLocationToString(beginLoc),
			 caller);
	}
}

/*
 * Same as add_filespace_map_entry(), but for tablespaces.
 */
static void
add_tablespace_map_entry(tspc_map *m, XLogRecPtr *beginLoc, char *caller)
{
	void *entry;
	bool found;

	/*
	 * The table is lazily initialised.
	 */
	if (!tablespace_map_ht)
		tablespace_map_ht = init_hash("mmxlog tablespace map",
									 sizeof(Oid), /* keysize */
									 sizeof(tspc_map),
									 gp_max_tablespaces);

	entry = hash_search(tablespace_map_ht,
						&(m->tablespaceoid),
						HASH_ENTER,
						&found);
	/*
	 * See above for why we do this.
	 */
	memcpy(entry, m, sizeof(tspc_map));

	if (Debug_persistent_recovery_print)
	{
		elog(PersistentRecovery_DebugPrintLevel(),
			 "add_tablespace_map_entry: add tablespace %u (filespace %u, beginLoc %s, caller '%s')",
			 m->tablespaceoid,
			 m->filespaceoid,
			 XLogLocationToString(beginLoc),
			 caller);
	}
}

/*
 * Given a filespace oid, lookup that path to the filespace.
 *
 * Output parameter will be set to NULL if not found.
 */
bool
mmxlog_filespace_get_path(
	Oid fspcoid,

	char **filespacePath)
{
	bool found;
	fspc_map *m;
	char *path = NULL;

	Insist(fspcoid != SYSTEMFILESPACE_OID);
	*filespacePath = NULL;

	/*
	 * The table is lazily initialised.
	 */
	if (!filespace_map_ht)
		filespace_map_ht = init_hash("mmxlog filespace map",
									 sizeof(Oid), /* keysize */
									 sizeof(fspc_map),
									 gp_max_filespaces);

	m = hash_search(filespace_map_ht,
					&fspcoid,
					HASH_FIND,
					&found);

	if (!found)
	{
		if (Debug_persistent_recovery_print)
		{
			mmxlog_print_filespaces(
								PersistentRecovery_DebugPrintLevel(),
								"mmxlog_filespace_get_path");
		}
		return false;
	}

	path = m->path1;

	*filespacePath = pstrdup(path);

	return true;
}

/*
 * Given a tablespace oid, return that filespace for the tablespace.
 *
 * Output parameter will be set to InvalidOid if not found.
 */
bool
mmxlog_tablespace_get_filespace(
	Oid tspcoid,

	Oid *filespaceOid)
{
	tspc_map *m;
	bool found;

	elog(DEBUG1, "MMXLOG: looking for tspcoid %u", tspcoid);

	*filespaceOid = InvalidOid;

	/*
	 * The table is lazily initialised.
	 */
	if (!tablespace_map_ht)
		tablespace_map_ht = init_hash("mmxlog tablespace map",
									 sizeof(Oid), /* keysize */
									 sizeof(tspc_map),
									 gp_max_tablespaces);
	/*
	 * First, get the filespace that the tablespace resides in.
	 */
	m = hash_search(tablespace_map_ht, &tspcoid, HASH_FIND, &found);

	if (!found)
	{
		if (Debug_persistent_recovery_print)
		{
			mmxlog_print_tablespaces(
								PersistentRecovery_DebugPrintLevel(),
								"mmxlog_tablespace_get_filespace");
		}
		return false;
	}

	*filespaceOid = m->filespaceoid;

	return true;
}

void
mmxlog_add_filespace_init(
	fspc_agg_state **fas, int *maxCount)
{
	int len;

	Assert (*fas == NULL);

	*maxCount = 10;		// Start off with at least this much room.
	len = FSPC_CHECKPOINT_BYTES(*maxCount);
	*fas = (fspc_agg_state*)palloc0(len);
}

void
mmxlog_add_filespace(
	fspc_agg_state **fas, int *maxCount,
	Oid filespace,
	char *path1,
	char *caller)
{
	int len;
	int count;
	fspc_map *m;

	char *filespaceLocation1;

	Assert(*fas != NULL);
	Assert(*maxCount > 0);

	count = (*fas)->count;
	Assert(count <= *maxCount);

	if (count == *maxCount)
	{
		fspc_agg_state *oldFas;

		oldFas = *fas;

		(*maxCount) *= 2;		// Double.
		len = FSPC_CHECKPOINT_BYTES(*maxCount);
		*fas = (fspc_agg_state*)palloc0(len);
		memcpy(*fas, oldFas, FSPC_CHECKPOINT_BYTES(count));
		pfree(oldFas);
	}

	m = &(*fas)->maps[count];
	m->filespaceoid = filespace;

	PersistentFilespace_ConvertBlankPaddedLocation(
											&filespaceLocation1,
											path1,
											/* isPrimary */ false);
	if (filespaceLocation1 != NULL)
	{
		strncpy(m->path1, filespaceLocation1, MAXPGPATH);
		pfree(filespaceLocation1);
	}
	else
	{
		// UNDONE: Do we ever not have both a master and mirror path???
		/*
		 * Allow relative paths if we didn't get anything when we looked up
		 * the filespace. We must allow this for the default filespace.
		 */
		m->path1[0] = '.';
		m->path1[1] = '\0';
	}

	if (Debug_persistent_recovery_print)
	{
		SUPPRESS_ERRCONTEXT_DECLARE;

		SUPPRESS_ERRCONTEXT_PUSH();

		elog(PersistentRecovery_DebugPrintLevel(),
			 "mmxlog_add_filespace[%d]: add filespace %u (path1 \"%s\", caller '%s')",
			 count,
			 filespace,
			 m->path1,
			 caller);

		SUPPRESS_ERRCONTEXT_POP();
	}

	(*fas)->count++;
}

void
mmxlog_add_tablespace_init(
	tspc_agg_state **tas, int *maxCount)
{
	int len;

	Assert (*tas == NULL);

	*maxCount = 10;		// Start off with at least this much room.
	len = TSPC_CHECKPOINT_BYTES(*maxCount);
	*tas = (tspc_agg_state*)palloc0(len);
}

void
mmxlog_add_tablespace(
	tspc_agg_state **tas, int *maxCount,
	Oid filespace, Oid tablespace, char *caller)
{
	int len;
	int count;
	tspc_map *m;

	Assert(*tas != NULL);
	Assert(*maxCount > 0);

	count = (*tas)->count;
	Assert(count <= *maxCount);

	if (count == *maxCount)
	{
		tspc_agg_state *oldTas;

		oldTas = *tas;

		(*maxCount) *= 2;		// Double.
		len = TSPC_CHECKPOINT_BYTES(*maxCount);
		*tas = (tspc_agg_state*)palloc0(len);
		memcpy(*tas, oldTas, TSPC_CHECKPOINT_BYTES(count));
		pfree(oldTas);
	}

	m = &(*tas)->maps[count];
	m->filespaceoid = filespace;
	m->tablespaceoid = tablespace;

	if (Debug_persistent_recovery_print)
	{
		SUPPRESS_ERRCONTEXT_DECLARE;

		SUPPRESS_ERRCONTEXT_PUSH();

		elog(PersistentRecovery_DebugPrintLevel(),
			 "mmxlog_add_tablespace[%d]: add tablespace %u (filespace %u, caller '%s')",
			 count,
			 tablespace,
			 filespace,
			 caller);

		SUPPRESS_ERRCONTEXT_POP();
	}

	(*tas)->count++;
}

void
mmxlog_add_database_init(
	dbdir_agg_state **das, int *maxCount)
{
	int len;

	Assert (*das == NULL);

	*maxCount = 10;		// Start off with at least this much room.
	len = DBDIR_CHECKPOINT_BYTES(*maxCount);
	*das = (dbdir_agg_state*)palloc0(len);
}

void
mmxlog_add_database(
	dbdir_agg_state **das, int *maxCount,
	Oid database, Oid tablespace, char *caller)
{
	int len;
	int count;
	dbdir_map *m;

	Assert(*das != NULL);
	Assert(*maxCount > 0);

	count = (*das)->count;
	Assert(count <= *maxCount);

	if (count == *maxCount)
	{
		dbdir_agg_state *oldDas;

		oldDas = *das;

		(*maxCount) *= 2;		// Double.
		len = DBDIR_CHECKPOINT_BYTES(*maxCount);
		*das = (dbdir_agg_state*)palloc0(len);
		memcpy(*das, oldDas, DBDIR_CHECKPOINT_BYTES(count));
		pfree(oldDas);
	}

	m = &(*das)->maps[count];
	m->databaseoid = database;
	m->tablespaceoid = tablespace;

	if (Debug_persistent_recovery_print)
	{
		SUPPRESS_ERRCONTEXT_DECLARE;

		SUPPRESS_ERRCONTEXT_PUSH();

		elog(PersistentRecovery_DebugPrintLevel(),
			 "mmxlog_add_database[%d]: add database %u (tablespace %u, caller '%s')",
			 count,
			 database,
			 tablespace,
			 caller);

		SUPPRESS_ERRCONTEXT_POP();
	}

	(*das)->count++;
}


/*
 * Add filespace and tablespace OID => path maps to the checkpoint payload. This
 * is used by the standby to construct a valid picture of the filespace and
 * tablespace configuration without having to touch the persistent tables --
 * which it cannot do since they're not guaranteed to be in a consistent state.
 *
 * NOTE: You must hold the PersistentObjLock before calling this routine!
 */
void
mmxlog_append_checkpoint_data(XLogRecData rdata[6])
{
	fspc_agg_state *f;
	tspc_agg_state *t;
	dbdir_agg_state *d;

	/*
	 * We must make sure no one traverses the rdata chain into uninitialised
	 * data if we exit early, below.
	 */
	rdata[1].next = NULL;
	rdata[2].next = NULL;
	rdata[3].next = NULL;
	rdata[4].next = NULL;

	if (gp_before_filespace_setup)
	{
		if (Debug_persistent_recovery_print)
		{
			SUPPRESS_ERRCONTEXT_DECLARE;

			SUPPRESS_ERRCONTEXT_PUSH();

			elog(PersistentRecovery_DebugPrintLevel(),
				 "mmxlog_append_checkpoint_data: no tablespace and filespace information for checkpoint because gp_before_filespace_setup GUC is true");

			SUPPRESS_ERRCONTEXT_POP();
		}
		return;
	}

	if (AmStandbyMaster())
	{
		f = (fspc_agg_state  *) palloc0(sizeof(*f));
		t = (tspc_agg_state  *) palloc0(sizeof(*t));
		d = (dbdir_agg_state *) palloc0(sizeof(*d));
		f->count = 0;
		t->count = 0;
		d->count = 0;
	}
	else
	{
		f = NULL;
		get_filespace_data(&f, "mmxlog_append_checkpoint_data");

		t = NULL;
		get_tablespace_data(&t, "mmxlog_append_checkpoint_data");

		d = NULL;
		get_database_data(&d, "mmxlog_append_checkpoint_data");
	}

	rdata[2].data = (char*)f;
	rdata[2].buffer = InvalidBuffer;
	rdata[2].len = FSPC_CHECKPOINT_BYTES(f->count);
	rdata[3].data = (char*)t;
	rdata[3].buffer = InvalidBuffer;
	rdata[3].len = TSPC_CHECKPOINT_BYTES(t->count);
	rdata[4].data = (char*)d;
	rdata[4].buffer = InvalidBuffer;
	rdata[4].len = DBDIR_CHECKPOINT_BYTES(d->count);

	rdata[1].next = &(rdata[2]);
	rdata[2].next = &(rdata[3]);
	rdata[3].next = &(rdata[4]);

	if (Debug_persistent_recovery_print)
	{
		SUPPRESS_ERRCONTEXT_DECLARE;

		SUPPRESS_ERRCONTEXT_PUSH();

		elog(PersistentRecovery_DebugPrintLevel(),
			 "mmxlog_append_checkpoint_data: %d filespaces, %d tablespaces, %d databases checkpoint information",
			 f->count,
			 t->count,
		         d->count);

		SUPPRESS_ERRCONTEXT_POP();
	}
}


/*
 * Return a pointer to the first section following the master/mirror checkpoint information
 */
char *
mmxlog_get_checkpoint_record_suffix(XLogRecord *checkpointRecord)
{
  char               *retPtr;
  TMGXACT_CHECKPOINT *dtxCheckpoint;
  fspc_agg_state     *f;
  tspc_agg_state     *t;
  dbdir_agg_state    *d;
  int                 dtxCheckpointLen;
  int                 fLen;
  int                 tLen;
  int                 dLen;

  dtxCheckpoint = (TMGXACT_CHECKPOINT *)(XLogRecGetData(checkpointRecord) + sizeof(CheckPoint));
  dtxCheckpointLen = TMGXACT_CHECKPOINT_BYTES(dtxCheckpoint->committedCount);

  f = (fspc_agg_state *)(((char*)dtxCheckpoint) + dtxCheckpointLen);
  fLen = FSPC_CHECKPOINT_BYTES(f->count);

  t = (tspc_agg_state *)(((char *)f) + fLen);
  tLen = TSPC_CHECKPOINT_BYTES(t->count);

  d = (dbdir_agg_state *)(((char *)t) + tLen);
  dLen = DBDIR_CHECKPOINT_BYTES(d->count);

  retPtr = ((char *)d) + dLen;

  return retPtr;

}  /* end mmxlog_get_checkpoint_record_suffix */


bool
mmxlog_get_checkpoint_info(char *cpdata, int masterMirroringLen, int checkpointLen, XLogRecPtr *beginLoc, int errlevel,
	fspc_agg_state  **f,
	tspc_agg_state  **t,
	dbdir_agg_state **d)
{
	int remainderLen;
	int filespaceInfoLen;
	int tablespaceInfoLen;
	int databaseInfoLen;

	SUPPRESS_ERRCONTEXT_DECLARE;

	SUPPRESS_ERRCONTEXT_PUSH();

	remainderLen = masterMirroringLen;
	if (remainderLen < FSPC_CHECKPOINT_BYTES(0))
	{
		if (errlevel != -1)
			ereport(errlevel,
				 (errmsg("Bad checkpoint record length %u (Master mirroring filespace information header: expected at least length %u, actual length %u) at location %s",
						 checkpointLen,
						 (uint32)FSPC_CHECKPOINT_BYTES(0),
						 remainderLen,
						 XLogLocationToString(beginLoc))));

		SUPPRESS_ERRCONTEXT_POP();
		return false;
	}
	*f = (fspc_agg_state*)cpdata;
	filespaceInfoLen = FSPC_CHECKPOINT_BYTES((*f)->count);
	if (Debug_persistent_recovery_print)
	{
		elog(PersistentRecovery_DebugPrintLevel(),
			 "mmxlog_get_checkpoint_info: Checkpoint record length %u, %d filespaces, filespaceInfoLen %d, remainder length %d, location %s",
			 checkpointLen,
			 (*f)->count,
			 filespaceInfoLen,
			 remainderLen,
			 XLogLocationToString(beginLoc));
	}
	if (remainderLen < filespaceInfoLen)
	{
		if (errlevel != -1)
			ereport(errlevel,
				 (errmsg("Bad checkpoint record length %u (Master mirroring filesapce information: expected at least length %u, actual length %u, count %d) at location %s",
						 checkpointLen,
						 filespaceInfoLen,
						 remainderLen,
						 (*f)->count,
						 XLogLocationToString(beginLoc))));

		SUPPRESS_ERRCONTEXT_POP();
		return false;
	}

	remainderLen -= filespaceInfoLen;
	if (remainderLen < TSPC_CHECKPOINT_BYTES(0))
	{
		if (errlevel != -1)
			ereport(errlevel,
				 (errmsg("Bad checkpoint record length %u (Master mirroring tablespace information header: expected at least length %u, actual length %u) at location %s",
						 checkpointLen,
						 (uint32)TSPC_CHECKPOINT_BYTES(0),
						 remainderLen,
						 XLogLocationToString(beginLoc))));

		SUPPRESS_ERRCONTEXT_POP();
		return false;
	}
	*t = (tspc_agg_state*) (cpdata + filespaceInfoLen);
	tablespaceInfoLen = TSPC_CHECKPOINT_BYTES((*t)->count);
	if (Debug_persistent_recovery_print)
	{
		elog(PersistentRecovery_DebugPrintLevel(),
			 "mmxlog_get_checkpoint_info: Checkpoint record length %u, %d tablespaces, tablespaceInfoLen %d, remainder length %d, location %s",
			 checkpointLen,
			 (*t)->count,
			 filespaceInfoLen,
			 remainderLen,
			 XLogLocationToString(beginLoc));
	}
	if (remainderLen < tablespaceInfoLen)
	{
		if (errlevel != -1)
			ereport(errlevel,
				 (errmsg("Bad checkpoint record length %u (Master mirroring tablesapce information: expected at least length %u, actual length %u, count %d) at location %s",
						 checkpointLen,
						 tablespaceInfoLen,
						 remainderLen,
						 (*t)->count,
						 XLogLocationToString(beginLoc))));

		SUPPRESS_ERRCONTEXT_POP();
		return false;
	}

	remainderLen -= tablespaceInfoLen;
	if (remainderLen < DBDIR_CHECKPOINT_BYTES(0))
	{
		if (errlevel != -1)
			ereport(errlevel,
				 (errmsg("Bad checkpoint record length %u (Master mirroring database directory information header: expected at least length %u, actual length %u) at location %s",
						 checkpointLen,
						 (uint32)DBDIR_CHECKPOINT_BYTES(0),
						 remainderLen,
						 XLogLocationToString(beginLoc))));

		SUPPRESS_ERRCONTEXT_POP();
		return false;
	}
	*d = (dbdir_agg_state*) (cpdata + filespaceInfoLen + tablespaceInfoLen);
	databaseInfoLen = DBDIR_CHECKPOINT_BYTES((*d)->count);
	if (Debug_persistent_recovery_print)
	{
		elog(PersistentRecovery_DebugPrintLevel(),
			 "mmxlog_get_checkpoint_info: Checkpoint record length %u, %d databases, databaseInfoLen %d, remainder length %d, location %s",
			 checkpointLen,
			 (*d)->count,
			 databaseInfoLen,
			 remainderLen,
			 XLogLocationToString(beginLoc));
	}

	remainderLen -= databaseInfoLen;

	if (remainderLen == 0)
	   elog(WARNING,"mmxlog_get_checkpoint_info: The checkpoint at %s appears to be a 4.0 checkpoint", XLogLocationToString(beginLoc));
	else if (remainderLen < 0)
	{
	  if (errlevel != -1)
	     ereport(errlevel,
		     (errmsg("Bad checkpoint record length %u (Master mirroring database directory information: expected length %u, actual length %u, count %d) at location %s",
			     checkpointLen,
			     databaseInfoLen,
			     remainderLen,
			     (*d)->count,
			     XLogLocationToString(beginLoc))));

	  SUPPRESS_ERRCONTEXT_POP();
	  return false;
	}

	SUPPRESS_ERRCONTEXT_POP();
	return true;

}  /* mmxlog_get_checkpoint_info */

bool
mmxlog_verify_checkpoint_info(char *cpdata, int masterMirroringLen, int checkpointLen, XLogRecPtr *beginLoc, int errlevel)
{
	fspc_agg_state *f;
	tspc_agg_state *t;
	dbdir_agg_state *d;

	return mmxlog_get_checkpoint_info(cpdata, masterMirroringLen, checkpointLen, beginLoc, errlevel, &f, &t, &d);
}

/*
 * If we're on the master standby, we expect to receive filespace and tablespace
 * meta data from a checkpoint.
 */
bool
mmxlog_get_checkpoint_counts(char *cpdata, int masterMirroringLen, int checkpointLen, XLogRecPtr *beginLoc, int errlevel, int *filespaceCount, int *tablespaceCount, int *databaseCount)
{
	fspc_agg_state *f = NULL;
	tspc_agg_state *t = NULL;
	dbdir_agg_state *d = NULL;

	*filespaceCount = 0;
	*tablespaceCount = 0;
	*databaseCount = 0;

	Assert(cpdata != NULL);

	if (!mmxlog_get_checkpoint_info(cpdata, masterMirroringLen, checkpointLen, beginLoc, errlevel, &f, &t, &d))
		return false;

	*filespaceCount = f->count;

	*tablespaceCount = t->count;

	*databaseCount = t->count;
	return true;
}

/*
 * If we're on the master standby, we expect to receive filespace and tablespace
 * meta data from a checkpoint.
 */
void
mmxlog_read_checkpoint_data(char *cpdata, int masterMirroringLen, int checkpointLen, XLogRecPtr *beginLoc)
{
	fspc_agg_state *f = NULL;
	tspc_agg_state *t = NULL;
	dbdir_agg_state *d = NULL;

	fspc_map *fmap;
	tspc_map *tmap;
	dbdir_map *dmap;

	int i;

	Assert(cpdata != NULL);

	if (!GPStandby())
		return;

	mmxlog_get_checkpoint_info(cpdata, masterMirroringLen, checkpointLen, beginLoc, PANIC, &f, &t, &d);

	/*
	 * Push the data down into the hash tables. We calculate the array length
	 * from the byte length of the array. We need to do filespaces first as
	 * they are the root of the space hierarchy.
	 */
	fmap = &(f->maps[0]);
	for (i = 0; i < f->count; i++)
	{
		fspc_map *m = &(fmap[i]);

		if (m->filespaceoid == InvalidOid)
			elog(ERROR, "bad filespace checkpoint information for entry %d", i);

		add_filespace_map_entry(m, beginLoc, "mmxlog_read_checkpoint_data");

		if (mkdir(fmap[i].path2, 0700) == 0)
		{
			if (Debug_persistent_recovery_print)
			{
				elog(PersistentRecovery_DebugPrintLevel(),
					 "mmxlog_read_checkpoint_data: Re-created filespace directory \"%s\"",
					 fmap[i].path2);
			}
		}
		else
		{
			/*
			 * Allowed to already exist.
			 */
			if (errno != EEXIST)
			{
				elog(ERROR, "could not create filespace directory \"%s\": %m",
					 fmap[i].path2);
			}
			else
			{
				if (Debug_persistent_recovery_print)
				{
					elog(PersistentRecovery_DebugPrintLevel(),
						 "mmxlog_read_checkpoint_data: Filespace directory \"%s\" already exists",
						 fmap[i].path2);
				}
			}
		}
	}

	tmap = &(t->maps[0]);
	for (i = 0; i < t->count; i++)
	{
		char path[MAXPGPATH];
		char *tmp;
		tspc_map *m = &(tmap[i]);

		if (m->tablespaceoid == InvalidOid)
			elog(ERROR, "bad tablespace checkpoint information for entry %d", i);

		add_tablespace_map_entry(m, beginLoc, "mmxlog_read_checkpoint_data");

		if (!mmxlog_filespace_get_path(
								m->filespaceoid,
								&tmp))
		{
			elog(ERROR, "cannot find filespace path for filespace OID %u (tablespace %u)",
				 m->filespaceoid, m->tablespaceoid);
		}
		snprintf(path, sizeof(path), "%s/%u", tmp, m->tablespaceoid);

		if (mkdir(path, 0700) == 0)
		{
			if (Debug_persistent_recovery_print)
			{
				elog(PersistentRecovery_DebugPrintLevel(),
					 "mmxlog_read_checkpoint_data: Re-created tablespace directory \"%s\"",
					 path);
			}
		}
		else
		{
			/*
			 * Allowed to already exist.
			 */
			if (errno != EEXIST)
			{
				elog(ERROR, "could not create tablespace directory \"%s\": %m",
					 path);
			}
			else
			{
				if (Debug_persistent_recovery_print)
				{
					elog(PersistentRecovery_DebugPrintLevel(),
						 "mmxlog_read_checkpoint_data: Tablespace directory \"%s\" already exists",
						 path);
				}
			}
		}

		pfree(tmp);
	}

	dmap = &(d->maps[0]);
	for (i = 0; i < d->count; i++)
	{
		char path[MAXPGPATH];
		dbdir_map *m = &(dmap[i]);
		Oid filespaceoid;

		if (m->databaseoid == InvalidOid)
			elog(ERROR, "bad database directory checkpoint information for entry %d", i);

//		add_tablespace_map_entry(m, beginLoc, "mmxlog_read_checkpoint_data");

		if (m->tablespaceoid == GLOBALTABLESPACE_OID)
			elog(ERROR, "should not have the global tablespace in the database directory entries");

		if (m->tablespaceoid == DEFAULTTABLESPACE_OID)
			snprintf(path, sizeof(path), "base/%u", m->databaseoid);
		else
		{
			char *tmp;
			if (!mmxlog_tablespace_get_filespace(
										m->tablespaceoid,
										&filespaceoid))
			{
				elog(ERROR, "cannot find filespace OID for tablespace %u",
					 m->tablespaceoid);
			}
			if (!mmxlog_filespace_get_path(
									filespaceoid,
									&tmp))
			{
				elog(ERROR, "cannot find filespace path for filespace OID %u (tablespace %u)",
					 filespaceoid, m->tablespaceoid);
			}
			snprintf(path, sizeof(path), "%s/%u/%u", tmp, m->tablespaceoid, m->databaseoid);
			pfree(tmp);
		}

		if (mkdir(path, 0700) == 0)
		{
			if (Debug_persistent_recovery_print)
			{
				elog(PersistentRecovery_DebugPrintLevel(),
					 "mmxlog_read_checkpoint_data: Re-created database directory \"%s\"",
					 path);
			}
		}
		else
		{
			/*
			 * Allowed to already exist.
			 */
			if (errno != EEXIST)
			{
				elog(ERROR, "could not create database directory \"%s\": %m",
					 path);
			}
			else
			{
				if (Debug_persistent_recovery_print)
				{
					elog(PersistentRecovery_DebugPrintLevel(),
						 "mmxlog_read_checkpoint_data: Database directory \"%s\" already exists",
						 path);
				}
			}
		}

	}

	if (Debug_persistent_recovery_print)
	{
		elog(PersistentRecovery_DebugPrintLevel(),
			 "mmxlog_read_checkpoint_data: %d filespaces, %d tablespaces, %d databases (beginLoc %s)",
			 f->count,
			 t->count,
			 d->count,
			 XLogLocationToString(beginLoc));
	}
}

/*
 * Given a tablespace OID, get the master and mirror filespace paths.
 */
static void
tblspc_get_filespace_paths(Oid tblspc, char **path)
{
	/*
	 * Built in tablespaces are not known by the PersistentTablespace code
	 * so we need to handle them here.
	 */
	if (tblspc == GLOBALTABLESPACE_OID ||
		tblspc == DEFAULTTABLESPACE_OID)
	{
		*path = NULL;
		/* short circuit */
		return;
	}

	PersistentTablespace_GetFilespacePath(
								tblspc,
								FALSE,
							 	path);
}
