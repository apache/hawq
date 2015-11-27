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
 * filespace.c
 *	  Commands to manipulate filespaces
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"
#include "miscadmin.h"

/* System libraries for file and directory operations */
#include <unistd.h>
#include <dirent.h>
#include <sys/stat.h>

#include "catalog/catalog.h"
#include "catalog/dependency.h"
#include "catalog/gp_segment_config.h"
#include "catalog/heap.h"
#include "catalog/indexing.h"
#include "catalog/pg_filespace.h"
#include "catalog/pg_filespace_entry.h"
#include "catalog/pg_filesystem.h"
#include "catalog/pg_tablespace.h"
#include "commands/comment.h"
#include "commands/filespace.h"
#include "commands/defrem.h"
#include "storage/fd.h"
#include "utils/acl.h"
#include "utils/builtins.h"
#include "utils/fmgroids.h"
#include "utils/lsyscache.h"

#include "cdb/cdbdisp.h"
#include "cdb/cdbmirroredfilesysobj.h"
#include "cdb/cdbvars.h"
#include "cdb/cdbutil.h"
#include "catalog/catquery.h"
#include "access/genam.h"
#include "postmaster/primary_mirror_mode.h"

static void checkPathFormat(char *path, bool url);
static void checkPathPermissions(char *path);
static void filespace_check_empty(Oid fsoid);

static void DeleteFilespaceEntryTuples(Oid fsoid);


/*
 * Calculate maximum filespace path length, Remember that we're going to append
 * '/<tbsoid>/<dboid>/<relid>.<nnn>'  
 *
 *      10 digits for each oid and the extension number,
 *       1 digit for each slash and the '.' of the extension
 *       = 10*4 + 4 = +44 characters
 *
 * Note: This may be overly conservative.  Do we ever form the whole path 
 * explicitly?
 */
#define MAX_FILESPACE_PATH (MAXPGPATH - 44)

/*
 * Set maximum allowed number of filespaces.
 *
 * Expected number of filespaces is < 10
 *
 * Should probably be made into a guc.
 */
#define MAX_FILESPACES 64


static bool isLocalFilesystem(Oid fsysid)
{
	return !OidIsValid(fsysid);
}

static int MAX_FILESPACE_PREFIX_LEN=128;

static char *
EncodeFileLocations(char *fsysName, short rep, char *location)
{
	/* local filesystem will store orig location */
	if (NULL == fsysName || pg_strcasecmp(fsysName, "local") == 0)
		return location;

	/* otherwise, we need to encode location */
	char      prefix[MAX_FILESPACE_PREFIX_LEN];
	char     *writepos  = prefix;
	int       writelen  = 0;
	int       remainlen = MAX_FILESPACE_PREFIX_LEN;
	int       prefixlen = 0;

	/* for non local filesystem, we add protocol part, like 'hdfs://' */
	writelen = snprintf(writepos, remainlen, "%s://", fsysName);
	if(writelen >= remainlen)
		elog(ERROR, "internal error: filespace prefix too long \"%s\"", prefix);
	writepos += writelen;
	remainlen -= writelen;

	/* add options if needed. Options will be encoded like '{key=value,key=value}' */
	if (rep != FS_DEFAULT_REPLICA_NUM)
	{
		writelen = snprintf(writepos, remainlen, "{");
		if(writelen >= remainlen)
			elog(ERROR, "internal error: filespace prefix too long \"%s\"", prefix);
		writepos += writelen;
		remainlen -= writelen;

		if(rep != FS_DEFAULT_REPLICA_NUM)
		{
			writelen = snprintf(writepos, remainlen, "replica=%d", rep);
			if(writelen >= remainlen)
				elog(ERROR, "internal error: filespace prefix too long \"%s\"", prefix);
			writepos += writelen;
			remainlen -= writelen;
		}

		writelen = snprintf(writepos, remainlen, "}");
		if(writelen >= remainlen)
			elog(ERROR, "internal error: filespace prefix too long \"%s\"", prefix);
		writepos += writelen;
		remainlen -= writelen;
	}

	prefixlen = strlen(prefix);
	if(prefixlen == 0)
		return location;

	char           *newlocation = NULL;
	int             reslen = prefixlen + strlen(location) + 1;
	newlocation = palloc(reslen);
	snprintf(newlocation, reslen, "%s%s", prefix, location);
	location = newlocation;

	return location;
}

/*
 * Create a filespace
 *
 * Only superusers can create a filespace. This seems a reasonable restriction
 * since we're determining the system layout and, anyway, we probably have root
 * if we're doing this kind of activity
 */
void
CreateFileSpace(CreateFileSpaceStmt *stmt)
{
	Relation			 rel;	
	HeapTuple			 tuple;
	NameData			 fsname;		/* filespace name */
	Oid					 ownerId;		/* OID of the OWNER of the filespace */
	Oid					 fsoid;	/* OID of the created filespace */
	bool				 nulls[Natts_pg_filespace];
	Datum				 values[Natts_pg_filespace];
	bool				 enulls[Natts_pg_filespace_entry];
	Datum				 evalues[Natts_pg_filespace_entry];
	cqContext			 cqc;
	cqContext			*pcqCtx;
	Oid					 fsysoid;	/* OID of the filesystem type of this filespace */
	short				 fsrep;		/* num of replication */
	ListCell			*cell;

	if (Gp_role != GP_ROLE_DISPATCH)
		elog(ERROR, "cannot create filespaces in utility mode");

	/* Must be super user */
	if (!superuser())
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
				 errmsg("permission denied to create filespace \"%s\"",
						stmt->filespacename),
				 errhint("Must be superuser to create a filespace.")));

	/* However, the eventual owner of the filespace need not be */
	if (stmt->owner)
		ownerId = get_roleid_checked(stmt->owner);
	else
		ownerId = GetUserId();

	/*
	 * Disallow creation of filespaces named "pg_xxx"; we reserve this namespace
	 * for system purposes.
	 */
	if (!allowSystemTableModsDDL && IsReservedName(stmt->filespacename))
	{
		ereport(ERROR,
				(errcode(ERRCODE_RESERVED_NAME),
				 errmsg("unacceptable filespace name \"%s\"",
						stmt->filespacename),
				 errdetail("The prefix \"%s\" is reserved for system filespaces.",
						   GetReservedPrefix(stmt->filespacename))));
	}
	namestrcpy(&fsname, stmt->filespacename);

	/*
	 * check filesystem on which this filespace is built on.
	 * InvalidOid for local filesystem
	 */
	fsysoid = InvalidOid;
	if(stmt->fsysname && pg_strcasecmp(stmt->fsysname, "local") != 0)
	{
		/*
		 * get Oid of filesystem. if filesystem not found,
		 * LookupFileSystemOid will report error and exit
		 */
		fsysoid = LookupFileSystemOid(stmt->fsysname, false);
	}

	/*
	 * get replication and option for filespace
	 */
	fsrep = FS_DEFAULT_REPLICA_NUM;
	foreach(cell, stmt->options)
	{
		DefElem    *defel = (DefElem *) lfirst(cell);
		if (pg_strcasecmp(defel->defname, "NUMREPLICA") == 0)
		{
			int64 rep = defGetInt64(defel);
			if(rep < 0 || rep >= FS_MAX_REPLICA_NUM)
				ereport(ERROR,
						(errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
						 errmsg("replica num "INT64_FORMAT" out of range", rep),
						 errdetail("Replica num should be in range [0, %d).",
								   FS_MAX_REPLICA_NUM)));
			fsrep = (short)rep;
		}
		else
		{
			ereport(ERROR,
                    (errcode(ERRCODE_SYNTAX_ERROR),
                     errmsg("filesystem attribute \"%s\" not recognized",
                            defel->defname)));
		}
	}

	checkPathFormat(stmt->location, true);
	if (false)
		checkPathPermissions(stmt->location);

	/*
	 * Because rollback of filespace creation is unpleasant we prefer
	 * to ensure that we fully serialize CREATE FILESPACE operations.
	 * Therefore we take a big lock up-front.
	 * NOTE: AccessExclusiveLock, not RowExclusiveLock
	 */
	rel = heap_open(FileSpaceRelationId, AccessExclusiveLock);

	pcqCtx = 
			caql_beginscan(
					caql_addrel(cqclr(&cqc), rel),
					cql("INSERT INTO pg_filespace",
						NULL));

	/* Check that there is no other filespace by this name. */
	fsoid = get_filespace_oid(rel, stmt->filespacename);
	if (OidIsValid(fsoid))
		ereport(ERROR,
				(errcode(ERRCODE_DUPLICATE_OBJECT),
				 errmsg("filespace \"%s\" already exists",
						stmt->filespacename)));

	/* The relation was opened up at the top of the function */
	Assert(rel);

	/* Insert tuple into pg_filespace */
	MemSet(nulls, false, sizeof(nulls));
	values[Anum_pg_filespace_fsname - 1]  = NameGetDatum(&fsname);
	values[Anum_pg_filespace_fsowner - 1] = ObjectIdGetDatum(ownerId);
	values[Anum_pg_filespace_fsfsys - 1] = ObjectIdGetDatum(fsysoid);
	values[Anum_pg_filespace_fsrep - 1] = Int16GetDatum(fsrep);
	tuple = caql_form_tuple(pcqCtx, values, nulls);

	/* insert a new tuple */
	fsoid = caql_insert(pcqCtx, tuple); /* implicit update of index as well */
	Assert(OidIsValid(fsoid));

	heap_freetuple(tuple);

	/* Record dependency on owner */
	recordDependencyOnOwner(FileSpaceRelationId, fsoid, ownerId);

	/* Keep the lock until commit/abort */
	caql_endscan(pcqCtx);
	heap_close(rel, NoLock);

	rel = heap_open(FileSpaceEntryRelationId, RowExclusiveLock);
	MemSet(enulls, false, sizeof(enulls));
	evalues[Anum_pg_filespace_entry_fsefsoid - 1] = ObjectIdGetDatum(fsoid);

	/* file system protocol enconding */
	if(strstr(stmt->location, "://")
	   || strstr(stmt->location, "{")
	   || strstr(stmt->location, "}"))
		ereport(ERROR,
				(errcode(ERRCODE_GP_COMMAND_ERROR),
				 errmsg("filespace location cannot contain \"://\" or any of these characters: \"{}\""
						"location:%s", stmt->location)));

	char *encoded = NULL;
	encoded = EncodeFileLocations(stmt->fsysname, fsrep, stmt->location);

	bool existed;
	if (HdfsPathExistAndNonEmpty(encoded, &existed))
		ereport(ERROR, 
				(errcode_for_file_access(),
				 errmsg("%s: File exists and non empty", encoded)));

	add_catalog_filespace_entry(rel, fsoid, 0, encoded);

	heap_close(rel, RowExclusiveLock);

	/* MPP-6929: metadata tracking */
	MetaTrackAddObject(FileSpaceRelationId,
					   fsoid,
					   GetUserId(),
					   "CREATE", "FILESPACE"
			);

	/* Let the Mirrored File IO interfaces see our change to the catalog. */
	CommandCounterIncrement();

	/* 
	 * Update the gp_persistent_filespace_node table.
	 *
	 * The persistent object layer is responsible for ensuring that the
	 * directories are created and maintained in the filesystem.  Most 
	 * importantly this layer knows how to cleanup filesystem objects in the
	 * event that this transaction aborts and the rollback and recovery 
	 * mechanisms know how to use this to cleanup after a hard failure or 
	 * replay the creation for mirror resynchronisation.
	 */
	MirroredFileSysObj_TransactionCreateFilespaceDir(fsoid, encoded, !existed);
}

/*
 * Drop a filespace
 *
 * Be careful to check that the filespace is empty.
 */
void 
RemoveFileSpace(List *names, DropBehavior behavior, bool missing_ok)
{
	Relation      rel;
	char         *fsname;
	Oid			  fsoid;
	ObjectAddress object;
	bool shareStorage;
	/* 
	 * General DROP (object) syntax allows fully qualified names, but
	 * filespaces are global objects that do not live in schemas, so
	 * it is a syntax error if a fully qualified name was given.
	 */
	if (list_length(names) != 1)
		ereport(ERROR,
				(errcode(ERRCODE_SYNTAX_ERROR),
				 errmsg("filespace name may not be qualified")));
	fsname = strVal(linitial(names));

	/* Disallow CASCADE */
	if (behavior == DROP_CASCADE)
		ereport(ERROR,
				(errcode(ERRCODE_SYNTAX_ERROR),
				 errmsg("syntax at or near \"cascade\"")));

	/* 
	 * Because rollback of filespace operations are difficult and expected
	 * usage is anticipated to be light we remove concurency worries by
	 * taking a big lock up front.
	 */
	rel = heap_open(FileSpaceRelationId, AccessExclusiveLock);

	/* Lookup the name in pg_filespace */
	fsoid = get_filespace_oid(rel, fsname);
	if (!OidIsValid(fsoid))
	{
		heap_close(rel, AccessExclusiveLock);

		if (missing_ok)
		{
			ereport(NOTICE,
					(errmsg("filespace \"%s\" does not exist, skipping", 
							fsname)));
			return;
		}
		else
		{
			ereport(ERROR,
					(errcode(ERRCODE_UNDEFINED_OBJECT),
					 errmsg("filespace \"%s\" does not exist", fsname)));
		}
	}

	/* Must be owner */
	if (!pg_filespace_ownercheck(fsoid, GetUserId()))
		aclcheck_error(ACLCHECK_NOT_OWNER, ACL_KIND_FILESPACE, fsname);

	/* Disallow drop of the standard filespaces, even by superuser */
	if (fsoid == SYSTEMFILESPACE_OID || strcmp(fsname, "dfs_system") == 0)
		ereport(ERROR,
				(errmsg("cannot drop filespace %s because it is required "
						"by the database system", fsname)));

	/*
	 * performDeletion only drops things that have dependencies in
	 * pg_depend/pg_shdepend which does NOT include dependencies on tablespaces
	 * (perhaps pg_shdepend should).  So we look for these dependencies by
	 * looking at the pg_tablespace table.
	 */
	filespace_check_empty(fsoid);
	shareStorage = is_filespace_shared(fsoid);

	/* Check for dependencies and remove the filespace */
	object.classId = FileSpaceRelationId;
	object.objectId = fsoid;
	object.objectSubId = 0;
	performDeletion(&object, DROP_RESTRICT);

	/*
	 * Remove any comments on this filespace
	 */
	DeleteSharedComments(fsoid, FileSpaceRelationId);

	/* 
	 * Keep the lock until commit/abort 
	 */
	heap_close(rel, NoLock);

	DeleteFilespaceEntryTuples(fsoid);

	/* MPP-6929: metadata tracking */
	MetaTrackDropObject(FileSpaceRelationId,
							fsoid);

	/* 
	 * The persistent object layer is responsible for actually managing the
	 * actual directory on disk.  Tell it that this filespace is removed by
	 * this transaciton.  This marks the filespace as pending delete and it
	 * will be deleted iff the transaction commits.
	 */
	MirroredFileSysObj_ScheduleDropFilespaceDir(fsoid, shareStorage);
}

/* 
 * RemoveFileSpaceById
 *   Guts of Filespace Deletion, called by dependency.c
 */
void
RemoveFileSpaceById(Oid fsoid)
{
	int numDel;

	numDel = caql_getcount(
			NULL,
			cql("DELETE FROM pg_filespace "
				" WHERE oid = :1 ",
				ObjectIdGetDatum(fsoid)));

	if (numDel != 1) /* shouldn't happen */
		elog(ERROR, "cache lookup failed for filespace %u", fsoid);
}

/* Return list of db_ids for each path. */
static void
DeleteFilespaceEntryTuples(Oid fsoid)
{
	int numDel;

	numDel = caql_getcount(
			NULL,
			cql("DELETE FROM pg_filespace_entry "
				" WHERE fsefsoid = :1 ",
				ObjectIdGetDatum(fsoid)));
}


/*
 * Change filespace owner
 */
void
AlterFileSpaceOwner(List *names, Oid newOwnerId)
{
	char             *fsname;
	Oid               fsoid;
	Relation	      rel;
	Form_pg_filespace fsForm;
	HeapTuple	      tup;
	cqContext		  cqc;
	cqContext		 *pcqCtx;

	/*
	 * This was from a generic AltrStmt node which allows for fully qualified
	 * object names, but filespaces don't exist inside schemas so fully
	 * qualified names are a syntax error.
	 */
	if (list_length(names) != 1)
		ereport(ERROR,
				(errcode(ERRCODE_SYNTAX_ERROR),
				 errmsg("filespace name may not be qualified")));
	fsname = strVal(linitial(names));

	/* Search pg_filespace */
	rel = heap_open(FileSpaceRelationId, RowExclusiveLock);

	pcqCtx = caql_addrel(cqclr(&cqc), rel);

	tup = caql_getfirst(
			pcqCtx,
			cql("SELECT * FROM pg_filespace "
				" WHERE fsname = :1 "
				" FOR UPDATE ",
				CStringGetDatum(fsname)));

	if (!HeapTupleIsValid(tup))
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_OBJECT),
				 errmsg("filespace \"%s\" does not exist", fsname)));
	fsoid  = HeapTupleGetOid(tup);
	fsForm = (Form_pg_filespace) GETSTRUCT(tup);

	/* Cannot alter system filespaces */
	if (!allowSystemTableModsDDL && IsReservedName(fsname))
		ereport(ERROR,
				(errcode(ERRCODE_RESERVED_NAME),
				 errmsg("permission denied: \"%s\" is a system filespace", 
						fsname)));

	/*
	 * If the new owner is the same as the existing owner, consider the
	 * command to have succeeded.  This is for dump restoration purposes.
	 */
	if (fsForm->fsowner != newOwnerId)
	{
		Datum		values[Natts_pg_filespace];
		bool		nulls[Natts_pg_filespace];
		bool		replace[Natts_pg_filespace];
		HeapTuple	newtuple;
		TupleDesc   tupdesc;

		/* Otherwise, must be owner of the existing object */
		if (!pg_filespace_ownercheck(fsoid, GetUserId()))
			aclcheck_error(ACLCHECK_NOT_OWNER, ACL_KIND_FILESPACE, fsname);

		/* Must be able to become new owner */
		check_is_member_of_role(GetUserId(), newOwnerId);

		/*
		 * Normally we would also check for create permissions here, but there
		 * are none for filespaces so we follow what rename filespace does
		 * and omit the create permissions check.
		 *
		 * NOTE: Only superusers may create filespaces to begin with and so
		 * initially only a superuser would be able to change its ownership
		 * anyway.
		 */
		memset(nulls, false, sizeof(nulls));
		memset(replace, false, sizeof(replace));

		replace[Anum_pg_filespace_fsowner - 1] = true;
		values[Anum_pg_filespace_fsowner - 1] = ObjectIdGetDatum(newOwnerId);

		tupdesc = RelationGetDescr(rel);
		newtuple = caql_modify_current(pcqCtx, values, nulls, replace);

		caql_update_current(pcqCtx, newtuple);
		/* and Update indexes (implicit) */

		/* MPP-6929: metadata tracking */
		if (Gp_role == GP_ROLE_DISPATCH)
			MetaTrackUpdObject(FileSpaceRelationId,
							   fsoid,
							   GetUserId(),
							   "ALTER", "OWNER"
					);

		heap_freetuple(newtuple);

		/* Update owner dependency reference */
		changeDependencyOnOwner(FileSpaceRelationId, fsoid, newOwnerId);
	}

	heap_close(rel, RowExclusiveLock);
}


/*
 * Rename a filespace
 */
void
RenameFileSpace(const char *oldname, const char *newname)
{
	Relation	 rel;
	Oid          fsoid;
	HeapTuple	 newtuple;
	cqContext	 cqc;
	cqContext	 cqc2;
	cqContext	*pcqCtx;
	int			 numFsname;
	Form_pg_filespace newform;

	/* Search pg_filespace */
	rel = heap_open(FileSpaceRelationId, RowExclusiveLock);

	pcqCtx = caql_addrel(cqclr(&cqc), rel);

	newtuple = caql_getfirst(
			pcqCtx,
			cql("SELECT * FROM pg_filespace "
				" WHERE fsname = :1 "
				" FOR UPDATE ",
				CStringGetDatum(oldname)));
	if (!HeapTupleIsValid(newtuple))
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_OBJECT),
				 errmsg("filespace \"%s\" does not exist",
						oldname)));

	newform = (Form_pg_filespace) GETSTRUCT(newtuple);

	/* Can't rename system filespaces */
	if (!allowSystemTableModsDDL && IsReservedName(oldname))
		ereport(ERROR,
				(errcode(ERRCODE_RESERVED_NAME),
				 errmsg("permission denied: \"%s\" is a system filespace", 
						oldname)));

	/* Must be owner */
	fsoid = HeapTupleGetOid(newtuple);
	if (!pg_filespace_ownercheck(fsoid, GetUserId()))
		aclcheck_error(ACLCHECK_NO_PRIV, ACL_KIND_FILESPACE, oldname);

	/* Validate new name */
	if (!allowSystemTableModsDDL && IsReservedName(newname))
	{
		ereport(ERROR,
				(errcode(ERRCODE_RESERVED_NAME),
				 errmsg("unacceptable filespace name \"%s\"", newname),
				 errdetail("The prefix \"%s\" is reserved for system filespaces.",
						   GetReservedPrefix(newname))));
	}

	numFsname = caql_getcount(
			caql_addrel(cqclr(&cqc2), rel),
			cql("SELECT COUNT(*) FROM pg_filespace "
				" WHERE fsname = :1 ",
				CStringGetDatum(newname)));

	if (numFsname)
		ereport(ERROR,
				(errcode(ERRCODE_DUPLICATE_OBJECT),
				 errmsg("filespace \"%s\" already exists", newname)));

	/* OK, update the entry */
	namestrcpy(&(newform->fsname), newname);

	caql_update_current(pcqCtx, newtuple);
	/* and Update indexes (implicit) */

	/* MPP-6929: metadata tracking */
	if (Gp_role == GP_ROLE_DISPATCH)
		MetaTrackUpdObject(FileSpaceRelationId,
						   fsoid,
						   GetUserId(),
						   "ALTER", "RENAME"
				);


	heap_close(rel, RowExclusiveLock);
}



/*
 * get_filespace_name - given a filespace OID, look up the name
 *
 * Returns a palloc'd string, or NULL if no such filespace
 */
char *
get_filespace_name(Oid fsoid)
{
	char		*result;

	/*
	 * Search pg_filespace.  We use a heapscan here even though there is an
	 * index on oid, on the theory that pg_filespace will usually have just a
	 * few entries and so an indexed lookup is a waste of effort.
	 */

	result = caql_getcstring(
			NULL,
			cql("SELECT fsname FROM pg_filespace "
				" WHERE oid = :1 ",
				ObjectIdGetDatum(fsoid)));
			
	/* We assume that there can be at most one matching tuple */

	return result;
}

/*
 * is_filespace_shared - given a filespace oid, look up the shared.
 */
bool
is_filespace_shared(Oid fsoid)
{
	Relation	rel;
	HeapScanDesc scandesc;
	HeapTuple	tuple;
	TupleDesc	tupledsc;
	ScanKeyData entry[1];
	bool		isnull;
	Datum		dfsysoid;
	Oid			fsysoid;

	/*
	 * Search pg_filespace.  We use a heapscan here even though there is an
	 * index on oid, on the theory that pg_filespace will usually have just a
	 * few entries and so an indexed lookup is a waste of effort.
	 */
	rel = heap_open(FileSpaceRelationId, AccessShareLock);
	tupledsc = RelationGetDescr(rel);
	ScanKeyInit(&entry[0],
				ObjectIdAttributeNumber,
				BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(fsoid));
	scandesc = heap_beginscan(rel, SnapshotNow, 1, entry);
	tuple = heap_getnext(scandesc, ForwardScanDirection);

	/*
	 * this case happens when master failed to create filespace.
	 * catalog already rollback
	 */
	if (!HeapTupleIsValid(tuple))
	{
	  heap_endscan(scandesc);
	  heap_close(rel, AccessShareLock);

	  return fsoid != SYSTEMFILESPACE_OID;
	}

	dfsysoid = heap_getattr(tuple, Anum_pg_filespace_fsfsys, tupledsc, &isnull);
	fsysoid = (isnull ? InvalidOid : DatumGetObjectId(dfsysoid));

	heap_endscan(scandesc);
	heap_close(rel, AccessShareLock);

	return !isLocalFilesystem(fsysoid);
}


/*
 * get_filespace_oid - given a filespace name, look up the OID
 *
 * Returns InvalidOid if filespace name not found.
 */
Oid
get_filespace_oid(Relation rel, const char *filespacename)
{
	Oid			 result;
	cqContext	 cqc;

	/* We assume that there can be at most one matching tuple */

	result = caql_getoid(
			caql_addrel(cqclr(&cqc), rel),
			cql("SELECT oid FROM pg_filespace "
				" WHERE fsname = :1 ",
				CStringGetDatum(filespacename)));

	return result;
}

/*
 * checkPathFormat(path)
 *
 * Runs simple validations on a path supplied to CREATE FILESPACE:
 *  - Standardizes paths via canonicalize_path()
 *  - Disallow paths with single quotes
 *  - Disallow relative paths
 *  - Disallow paths that are too long.
 *
 * We have other checks to perform, but these are the only ones that we
 * can run based only on the name without the local file system present.
 */
static void 
checkPathFormat(char *path, bool url)
{
	/* Unix-ify the offered path and strip any trailing slashes */
	if (!url)
		canonicalize_path(path);

	/* disallow quotes, else CREATE DATABASE would be at risk */
	if (strchr(path, '\''))
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_NAME),
				 errmsg("filespace location \"%s\" "
						"cannot contain single quotes", path)));
	
	/*
	 * Allowing relative paths seems risky
	 *
	 * this also helps us ensure that path is not empty or whitespace
	 */
	if (!url && !is_absolute_path(path))
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
				 errmsg("filespace location \"%s\" "
						"must be an absolute path", path)));

	/*
	 * Check that location isn't too long. 
	 */
	if (strlen(path) >= MAX_FILESPACE_PATH)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
				 errmsg("filespace location \"%s\" is too long",
						path),
				 errhint("maximum length %d characters", 
						 MAX_FILESPACE_PATH)));
}

/*
 * checkPathPermissions(path)
 *
 * Runs additional validations on a path supplied to CREATE FILESPACE.
 * The assumption is that the path given to us is the original path
 * that was specified in the gpfilespace command plus a path extension
 * to uniquely identify this segment.  We further assume that the original
 * path exists, but that the segment extension does not and must be created.
 * Or... if the extension path does exist then it must be an empty directory.
 *
 *  We must:
 *    - Validate that the specified path does not exist
 *    - Validate that the parent directory exists
 *    - Validate that the parent is a directory.
 *    - Validate that the parent has apropriate permissions
 *
 * Note: Passing these checks does not guarantee that everything is good.
 * In particular we have not checked anywhere that the paths are all 
 * unique on a given host.  We omit this only because this is a difficult
 * test when we don't have metadata about what segments are on the same host.
 * 
 * If there is a conflict we should see it when we actually try to claim
 * the directories for the segments.
 *
 * Note: May need to add additional checks that there is not a pending
 * background delete on this directory location?
 *
 * Note: See FileRepMirror_Validation() in cdb/cdbfilerepmirror.c for the same
 * checks run on the mirror side.
 */
static void 
checkPathPermissions(char *path)
{
	struct stat st;
	char *parentdir;

	/* The specified path should not exist yet */
	if (stat(path, &st) >= 0)
	{
		ereport(ERROR, 
				(errcode_for_file_access(),
				 errmsg("%s: File exists", path)));
	}

	/* Find the parent directory */
	parentdir = pstrdup(path);
	get_parent_directory(parentdir);

	/* The parent directory must already exist */
	if (stat(parentdir, &st) < 0)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("%s: No such file or directory", 
						parentdir)));
		
	/* The parent directory must be a directory */
	if (! S_ISDIR(st.st_mode))
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
				 errmsg("%s: Not a directory", parentdir)));

	/* 
	 * Check write permissions of the parent directory 
	 *
	 * Note: Accornding to the BSD manual access shouldn't be used because it
	 * is a security hole, but what they are actually refering to is the fact
	 * that the permissions could change between the time of the check and the
	 * time an action is taken.  This is primarily a courtousy check to produce
	 * a cleaner error message.  If the filesystem should change between now
	 * and the actual mkdir() then the transaction will abort later with an
	 * uglier error message, but it is not actually a security hole.
	 */
	if (access(parentdir, W_OK|X_OK) < 0)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("%s: Permission denied", path)));
}

/*
 * filespace_check_empty(fsoid):
 *
 * Checks the gp_persistent_tablespace_node table to determine if the specified
 * filespace is empty.
 */
static void 
filespace_check_empty(Oid fsoid)
{
	if (caql_getcount(
				NULL,
				cql("SELECT COUNT(*) FROM pg_tablespace "
					" WHERE spcfsoid = :1 ",
					ObjectIdGetDatum(fsoid))))
	{
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("filespace \"%s\" is not empty", 
						get_filespace_name(fsoid))));
	}
}

/* Add a pg_filespace_entry for a given filespace definition. */
void
add_catalog_filespace_entry(Relation rel, Oid fsoid, int16 dbid, char *location)
{
	HeapTuple	 tuple;
	Datum		 evalues[Natts_pg_filespace_entry];
	bool		 enulls[Natts_pg_filespace_entry];
	cqContext	 cqc;
	cqContext	*pcqCtx;

	/* NOTE: rel must have correct lock mode for INSERT */
	pcqCtx = 
			caql_beginscan(
					caql_addrel(cqclr(&cqc), rel),
					cql("INSERT INTO pg_filespace_entry",
						NULL));

	MemSet(enulls, false, sizeof(enulls));

	evalues[Anum_pg_filespace_entry_fsefsoid - 1] = ObjectIdGetDatum(fsoid);

	evalues[Anum_pg_filespace_entry_fsedbid - 1] = Int16GetDatum(dbid);
	evalues[Anum_pg_filespace_entry_fselocation - 1] =
				DirectFunctionCall1(textin, CStringGetDatum(location));
			
	tuple = caql_form_tuple(pcqCtx, evalues, enulls);

	/* insert a new tuple */
	caql_insert(pcqCtx, tuple); /* implicit update of index as well */

	heap_freetuple(tuple);
	caql_endscan(pcqCtx);

}

void
dbid_remove_filespace_entries(Relation rel, int16 dbid)
{
	int			 numDel;
	cqContext	 cqc;

	/* Use the index to scan only attributes of the target relation */
	numDel = caql_getcount(
			caql_addrel(cqclr(&cqc), rel),
			cql("DELETE FROM pg_filespace_entry "
				" WHERE fsedbid = :1 ",
				Int16GetDatum(dbid)));
}

