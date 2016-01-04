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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/*-------------------------------------------------------------------------
 *
 * filesystemcmds.c
 *
 *	  Commands to manipulate filesystems
 *
 * Portions Copyright (c) 2011, Greenplum/EMC.
 * Portions Copyright (c) 1996-2008, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/heapam.h"
#include "access/genam.h"
#include "catalog/dependency.h"
#include "catalog/indexing.h"
#include "catalog/pg_filesystem.h"
#include "catalog/pg_type.h"
#include "commands/defrem.h"
#include "commands/filesystemcmds.h"
#include "miscadmin.h"
#include "parser/parse_func.h"
#include "parser/parse_type.h"
#include "utils/acl.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/syscache.h"
#include "utils/fmgroids.h"
#include "utils/flatfiles.h"

#include "cdb/cdbvars.h"
#include "cdb/cdbdisp.h"
#include "cdb/dispatcher.h"


/*
 */
static const char *fsysLibFileName = "gpfs_libfile";

/*
 *	DefineFileSystem
 */
void
DefineFileSystem(List *name, List *parameters, Oid newOid, bool trusted)
{
	char	   *fsysName;
	Oid			fsysNamespace;
	AclResult	aclresult;
	List	   *funcNames[FSYS_FUNC_TOTALNUM];
	char	   *fsysLibFile = NULL;
	int			funcNum = 0;
	ListCell   *pl;
	Oid			fsysOid;

	/* Convert list of names to a name and namespace */
	fsysNamespace = QualifiedNameGetCreationNamespace(name, &fsysName);

	/* Check we have creation rights in target namespace */
	aclresult = pg_namespace_aclcheck(fsysNamespace, GetUserId(), ACL_CREATE);
	if (aclresult != ACLCHECK_OK)
		aclcheck_error(aclresult, ACL_KIND_NAMESPACE,
					   get_namespace_name(fsysNamespace));

	for(int i = 0; i < FSYS_FUNC_TOTALNUM; i++)
		funcNames[i] = NIL;

	foreach(pl, parameters)
	{
		DefElem    *defel = (DefElem *) lfirst(pl);
		int funcType;

		if (pg_strcasecmp(defel->defname, fsysLibFileName) == 0)
		{
			if(fsysLibFile == NULL)
			{
				fsysLibFile = strVal(linitial(defGetQualifiedName(defel)));
			}
			else
			{
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("filesystem attribute \"%s\" duplicated",
								defel->defname)));
			}
			continue;
		}

		for(funcType = 0; funcType < FSYS_FUNC_TOTALNUM; funcType++)
		{
			if(pg_strcasecmp(defel->defname, fsys_func_type_to_name(funcType)) == 0)
				break;
		}
		if (funcType >= FSYS_FUNC_TOTALNUM)
			ereport(ERROR,
					(errcode(ERRCODE_SYNTAX_ERROR),
					 errmsg("filesystem attribute \"%s\" not recognized",
							defel->defname)));
		if(funcNames[funcType] == NIL)
			funcNames[funcType] = defGetQualifiedName(defel);
		else
			ereport(ERROR,
					(errcode(ERRCODE_SYNTAX_ERROR),
					 errmsg("filesystem function \"%s\" duplicated",
							defel->defname)));
		funcNum++;
	}

	/*
	 * make sure we have our required definitions
	 */
	if (fsysLibFile == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_SYNTAX_ERROR),
				 errmsg("filesystem need %s but not specified", fsysLibFileName)));

	if (funcNum != FSYS_FUNC_TOTALNUM)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_FUNCTION_DEFINITION),
				 errmsg("filesystem need %d funcs but only get %d", FSYS_FUNC_TOTALNUM, funcNum)));

	/*
	 * Most of the argument-checking is done inside of FileSystemCreate
	 */
	fsysOid = FileSystemCreateWithOid(fsysName,				/* filesystem name */
									  fsysNamespace,		/* namespace */
									  funcNames,			/* functions' name */
									  funcNum,
									  fsysLibFile,
									  newOid,
									  trusted);

	/*
	 * Set flag to update flat filesystem at commit.
	 */
	filesystem_file_update_needed();

	if (Gp_role == GP_ROLE_DISPATCH)
	{
		DefineStmt * stmt = makeNode(DefineStmt);
		stmt->kind = OBJECT_FILESYSTEM;
		stmt->oldstyle = false;  
		stmt->defnames = name;
		stmt->args = NIL;
		stmt->definition = parameters;
		stmt->newOid = fsysOid;
		stmt->shadowOid = 0;
		stmt->ordered = false;
		stmt->trusted = trusted;
		dispatch_statement_node((Node *) stmt, NULL, NULL, NULL);
	}
}


/*
 * RemoveFileSystem
 *		Deletes an file system
 */
void
RemoveFileSystem(List *names, DropBehavior behavior, bool missing_ok)
{
	char	   *fsysName;
	Oid			fsysOid = InvalidOid;
	ObjectAddress object;

	/* 
	 * General DROP (object) syntax allows fully qualified names, but
	 * filesystems are global objects that do not live in schemas, so
	 * it is a syntax error if a fully qualified name was given.
	 */
	if (list_length(names) != 1)
		ereport(ERROR,
				(errcode(ERRCODE_SYNTAX_ERROR),
				 errmsg("filesystem name may not be qualified")));
	fsysName = strVal(linitial(names));

	/* find filesystem Oid. error inline if doesn't exist */
	fsysOid = LookupFileSystemOid(fsysName, missing_ok);

	if (!OidIsValid(fsysOid))
	{
		if (!missing_ok)
		{
			ereport(ERROR,
					(errcode(ERRCODE_UNDEFINED_OBJECT),
					 errmsg("filesystem \"%s\" does not exist",
							fsysName)));
		}
		else
		{
			if (Gp_role != GP_ROLE_EXECUTE)
				ereport(NOTICE,
						(errcode(ERRCODE_UNDEFINED_OBJECT),
						 errmsg("filesystem \"%s\" does not exist, skipping",
								fsysName)));
		}

		return;
	}

	/* Permission check: must own filesystem */
	if (!pg_filesystem_ownercheck(fsysOid, GetUserId()))
		aclcheck_error(ACLCHECK_NOT_OWNER, ACL_KIND_FILESYSTEM, fsysName);

	/*
	 * Do the deletion
	 */
	object.classId = FileSystemRelationId;
	object.objectId = fsysOid;
	object.objectSubId = 0;

	performDeletion(&object, behavior);

	/*
	 * Set flag to update flat filesystem at commit.
	 */
	filesystem_file_update_needed();
}

/*
 * Drop FILESYSTEM by OID. This is the guts of deletion.
 * This is called to clean up dependencies.
 */
void
RemoveFileSystemById(Oid fsysOid)
{
	FileSystemDeleteByOid(fsysOid);

	/*
	 * Set flag to update flat filesystem at commit.
	 */
	filesystem_file_update_needed();
}

/*
 * Change file system owner
 */
void
AlterFileSystemOwner(const char *name, Oid newOwnerId)
{
	HeapTuple	tup;
	Relation	rel;
	TupleDesc	dsc;
	Oid			fsysId;
	Oid			ownerId;
	AclResult	aclresult;
	bool		isNull;
	bool		isTrusted;
	ScanKeyData	key;
	SysScanDesc	scan;
	Datum		ownerDatum;
	Datum		trustedDatum;
	
	
	/*
	 * Check the pg_filesystem relation to be certain the filesystem
	 * is there. 
	 */
	rel = heap_open(FileSystemRelationId, RowExclusiveLock);
	dsc = RelationGetDescr(rel);
	
	ScanKeyInit(&key,
				Anum_pg_filesystem_fsysname,
				BTEqualStrategyNumber,
				F_NAMEEQ,
				CStringGetDatum(name));

	scan = systable_beginscan(rel, 
							  FileSystemRelationId, 
							  true,
							  SnapshotNow, 
							  1, 
							  &key);
	
	tup = systable_getnext(scan);
	
	if (!HeapTupleIsValid(tup))
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_OBJECT),
				 errmsg("filesystem \"%s\" does not exist",
						name)));
	
	fsysId = HeapTupleGetOid(tup);
	
	ownerDatum = heap_getattr(tup, 
							  Anum_pg_filesystem_fsysowner,
							  dsc, 
							  &isNull);
	
	if(isNull)
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_OBJECT),
				 errmsg("internal error: filesystem \"%s\"  has no owner defined",
						 name)));

	ownerId = DatumGetObjectId(ownerDatum);

	trustedDatum = heap_getattr(tup, 
							 Anum_pg_filesystem_fsystrusted,
							 dsc, 
							 &isNull);
	
	if(isNull)
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_OBJECT),
				 errmsg("internal error: filesystem \"%s\" has no trust attribute defined",
						 name)));

	isTrusted = DatumGetBool(trustedDatum);
	
	if (ownerId != newOwnerId)
	{
		Acl		   *newAcl;
		Datum		values[Natts_pg_filesystem];
		bool		nulls[Natts_pg_filesystem];
		bool		replaces[Natts_pg_filesystem];
		HeapTuple	newtuple;
		Datum		aclDatum;

		/* Superusers can always do it */
		if (!superuser())
		{
			/* Must be owner */
			if (!pg_filesystem_ownercheck(fsysId, GetUserId()))
				aclcheck_error(ACLCHECK_NOT_OWNER, ACL_KIND_FILESYSTEM,
							   name);

			/* Must be able to become new owner */
			check_is_member_of_role(GetUserId(), newOwnerId);

			/* New owner must have USAGE privilege on filesystem */
			aclresult = pg_filesystem_aclcheck(fsysId, newOwnerId, ACL_USAGE);

			if (aclresult != ACLCHECK_OK)
				aclcheck_error(aclresult, ACL_KIND_FILESYSTEM, name);
		}

		/* MPP-14592: untrusted? don't allow ALTER OWNER to non-super user */
		if(!isTrusted && !superuser_arg(newOwnerId))
			ereport(ERROR,
					(errcode(ERRCODE_WRONG_OBJECT_TYPE),
					 errmsg("untrusted filesystem \"%s\" can't be owned by non superuser", 
							 name)));

		
		MemSet(values, 0, sizeof(values));
		MemSet(nulls, false, sizeof(nulls));
		MemSet(replaces, false, sizeof(replaces));

		replaces[Anum_pg_filesystem_fsysowner - 1] = true;
		values[Anum_pg_filesystem_fsysowner - 1] = ObjectIdGetDatum(newOwnerId);

		aclDatum = heap_getattr(tup, 
								Anum_pg_filesystem_fsysacl,
								dsc, 
								&isNull);
		
		if (!isNull)
		{
			newAcl = aclnewowner(DatumGetAclP(aclDatum),
								 ownerId, newOwnerId);
			replaces[Anum_pg_filesystem_fsysacl - 1] = true;
			values[Anum_pg_filesystem_fsysacl - 1] = PointerGetDatum(newAcl);
		}

		newtuple = heap_modify_tuple(tup, RelationGetDescr(rel), values,
									 nulls, replaces);

		simple_heap_update(rel, &newtuple->t_self, newtuple);

		/* keep the catalog indexes up to date */
		CatalogUpdateIndexes(rel, newtuple);

		heap_freetuple(newtuple);
		
		/* Update owner dependency reference */
		changeDependencyOnOwner(FileSystemRelationId, fsysId, newOwnerId);
	}

	systable_endscan(scan);
	heap_close(rel, NoLock);

	/*
	 * Set flag to update flat filesystem at commit.
	 */
	filesystem_file_update_needed();
}

/*
 * Change external filesystem owner
 */
void
RenameFileSystem(const char *oldname, const char *newname)
{
	/* FIXME liugd cannot rename filesystem for now */
	elog(ERROR, "rename filesystem rename not supported yet");

	HeapTuple	tup;
	Relation	rel;
	TupleDesc	dsc;
	Oid			fsysId;
	Oid			ownerId;
	bool		isNull;
	ScanKeyData	key;
	SysScanDesc	scan;
	Datum		ownerDatum;
	
	
	/*
	 * Check the pg_filesystem relation to be certain the filesystem 
	 * is there. 
	 */
	rel = heap_open(FileSystemRelationId, RowExclusiveLock);
	dsc = RelationGetDescr(rel);

	ScanKeyInit(&key,
				Anum_pg_filesystem_fsysname,
				BTEqualStrategyNumber,
				F_NAMEEQ,
				CStringGetDatum(oldname));
	
	scan = systable_beginscan(rel, 
							  FileSystemFsysnameIndexId, 
							  true,
							  SnapshotNow, 
							  1, 
							  &key);
	
	tup = systable_getnext(scan);
	
	if (!HeapTupleIsValid(tup))
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_OBJECT),
				 errmsg("filesystem \"%s\" does not exist",
						oldname)));
	
	fsysId = HeapTupleGetOid(tup);
	
	ownerDatum = heap_getattr(tup, 
							  Anum_pg_filesystem_fsysowner,
							  dsc, 
							  &isNull);
	
	if(isNull)
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_OBJECT),
				 errmsg("internal error: filesystem \"%s\"  has no owner defined",
						 oldname)));	

	ownerId = DatumGetObjectId(ownerDatum);

	if (strcmp(oldname, newname) != 0)
	{
		Datum		values[Natts_pg_filesystem];
		bool		nulls[Natts_pg_filesystem];
		bool		replaces[Natts_pg_filesystem];
		HeapTuple	newtuple;

		/* Superusers can always do it */
		if (!superuser())
		{
			/* Must be owner */
			if (!pg_filesystem_ownercheck(fsysId, GetUserId()))
				aclcheck_error(ACLCHECK_NOT_OWNER, ACL_KIND_FILESYSTEM,
							   oldname);
		}

		MemSet(values, 0, sizeof(values));
		MemSet(nulls, false, sizeof(nulls));
		MemSet(replaces, false, sizeof(replaces));

		replaces[Anum_pg_filesystem_fsysname - 1] = true;
		values[Anum_pg_filesystem_fsysname - 1] = CStringGetDatum(newname);
		
		newtuple = heap_modify_tuple(tup, RelationGetDescr(rel), values,
									 nulls, replaces);

		simple_heap_update(rel, &newtuple->t_self, newtuple);

		/* keep the catalog indexes up to date */
		CatalogUpdateIndexes(rel, newtuple);

		heap_freetuple(newtuple);		
	}

	systable_endscan(scan);	
	heap_close(rel, NoLock);

	/*
	 * Set flag to update flat filesystem at commit.
	 */
	filesystem_file_update_needed();
}
