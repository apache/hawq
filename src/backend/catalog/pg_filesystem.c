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
 * pg_filesystem.c
 *	  routines to support manipulation of the pg_filesystem relation
 *
 * Portions Copyright (c) 2011, Greenplum/EMC
 * Portions Copyright (c) 1996-2009, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/genam.h"
#include "access/heapam.h"
#include "catalog/dependency.h"
#include "catalog/indexing.h"
#include "catalog/pg_filesystem.h"
#include "catalog/pg_language.h"
#include "catalog/pg_operator.h"
#include "catalog/pg_type.h"
#include "cdb/cdbvars.h"
#include "miscadmin.h"
#include "parser/parse_coerce.h"
#include "parser/parse_func.h"
#include "parser/parse_oper.h"
#include "utils/acl.h"
#include "utils/builtins.h"
#include "utils/fmgroids.h"
#include "utils/lsyscache.h"
#include "utils/syscache.h"


static char *fsysFuncNames[FSYS_FUNC_TOTALNUM]= {
	"gpfs_connect",
	"gpfs_disconnect",
	"gpfs_open",
	"gpfs_close",
	"gpfs_seek",
	"gpfs_tell",
	"gpfs_read",
	"gpfs_write",
	"gpfs_flush",
	"gpfs_delete",
	"gpfs_chmod",
	"gpfs_mkdir",
	"gpfs_truncate",
	"gpfs_getpathinfo",
	"gpfs_freefileinfo"
};

/*
 * FileSystemCreateWithOid
 */
Oid
FileSystemCreateWithOid(const char		*fsysName,
						Oid				 fsysNamespace,
						List			*funcNames[],
						int				 funcNum,
						const char		*fsysLibFile,
						Oid				 fsysOid,
						bool			 trusted)
{
	Relation	fsysdesc;
	HeapTuple	tup;
	bool		nulls[Natts_pg_filesystem];
	Datum		values[Natts_pg_filesystem];
	NameData	fname;
	TupleDesc	tupDesc;
	int			i;
	Oid 		ownerId = GetUserId();

	/* sanity checks (caller should have caught these) */
	if (!fsysName)
		elog(ERROR, "no filesystem name supplied");

	if (!funcNames || funcNum != FSYS_FUNC_TOTALNUM)
		elog(ERROR, "filesystem need %d funcs, but get %d", FSYS_FUNC_TOTALNUM, funcNum);

	if (!fsysLibFile)
		elog(ERROR, "filesystem need a lib file, but not specified");

	/*
	 * Until we add filesystems to pg_filesystem, make sure no
	 * filesystems with the same name are created.
	 */
	if (strcasecmp(fsysName, "local") == 0)
	{
		ereport(ERROR,
				(errcode(ERRCODE_RESERVED_NAME),
				 errmsg("filesystem \"%s\" is reserved",
						 fsysName),
				 errhint("pick a different filesystem name")));
	}

	/* 
	 * function checks: check existence and correct signature in the catalog
	 */
	for(int i = 0; i < funcNum; i++)
	{
		if(funcNames[i] == NIL)
			elog(ERROR, "filesystem function \"%s\" not specified", fsys_func_type_to_name(i));
	}

	/*
	 * Everything looks okay.  Try to create the pg_filesystem entry for the
	 * filesystem.  (This could fail if there's already a conflicting entry.)
	 */

	/* initialize nulls and values */
	for (i = 0; i < Natts_pg_filesystem; i++)
	{
		nulls[i] = false;
		values[i] = (Datum) 0;
	}
	namestrcpy(&fname, fsysName);
	values[Anum_pg_filesystem_fsysname - 1] = NameGetDatum(&fname);

	for(int i = 0; i < funcNum; i++)
	{
		values[fsys_func_type_to_attnum(i) - 1] = CStringGetDatum(strVal(linitial(funcNames[i])));
	}
	
	values[Anum_pg_filesystem_fsyslibfile - 1] = CStringGetTextDatum(fsysLibFile);
	values[Anum_pg_filesystem_fsysowner - 1] = ObjectIdGetDatum(ownerId);
	values[Anum_pg_filesystem_fsystrusted - 1] = BoolGetDatum(trusted);
	nulls[Anum_pg_filesystem_fsysacl - 1] = true;

	fsysdesc = heap_open(FileSystemRelationId, RowExclusiveLock);
	tupDesc = fsysdesc->rd_att;

	tup = heap_form_tuple(tupDesc, values, nulls);

	if (fsysOid != (Oid) 0)
		HeapTupleSetOid(tup, fsysOid);

	fsysOid = simple_heap_insert(fsysdesc, tup);

	CatalogUpdateIndexes(fsysdesc, tup);

	heap_close(fsysdesc, RowExclusiveLock);

	/*
	 * Create dependencies for the filesystem
	 */

	/* dependency on owner */
	recordDependencyOnOwner(FileSystemRelationId, fsysOid, GetUserId());

	return fsysOid;
}

void
FileSystemDeleteByOid(Oid	fsysOid)
{		
	Relation	rel;
	HeapScanDesc scandesc;
	HeapTuple	tuple;
	ScanKeyData entry[1];

	/*
	 * Search pg_filesystem.  We use a heapscan here even though there is an
	 * index on oid, on the theory that pg_filesystem will usually have just a
	 * few entries and so an indexed lookup is a waste of effort.
	 */
	rel = heap_open(FileSystemRelationId, RowExclusiveLock);
	ScanKeyInit(&entry[0],
				ObjectIdAttributeNumber,
				BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(fsysOid));
	scandesc = heap_beginscan(rel, SnapshotNow, 1, entry);
	tuple = heap_getnext(scandesc, ForwardScanDirection);

	/* We assume that there can be at most one matching tuple */
	if (!HeapTupleIsValid(tuple))
	{
		elog(ERROR, "filesystem %u could not be found", fsysOid);
	}

	simple_heap_delete(rel, &tuple->t_self);
	
	heap_endscan(scandesc);
	heap_close(rel, NoLock);
}

/*
 * Finds an file system by passed in filesystem name.
 * Errors if no such filesystem exist, or if no function to
 * execute this filesystem exists.
 * 
 * Returns the filesystem function to use.
 */
Oid
LookupFileSystemFunction(const char *fsys_name, 
						 FileSystemFuncType fsys_type,
						 bool error)
{
		
	Relation	rel;
	TupleDesc	dsc;
	HeapTuple	tuple;
	bool		isNull;
	ScanKeyData	key;
	SysScanDesc	scan;
	Oid			funcOid = InvalidOid;
	Datum		funcDatum;
	
	/*
	 * Check the pg_filesystem relation to be certain the ao table 
	 * is there. 
	 */
	rel = heap_open(FileSystemRelationId, AccessShareLock);
	dsc = RelationGetDescr(rel);
	
	ScanKeyInit(&key,
				Anum_pg_filesystem_fsysname,
				BTEqualStrategyNumber,
				F_NAMEEQ,
				CStringGetDatum(fsys_name));
	
	scan = systable_beginscan(rel, 
							  FileSystemFsysnameIndexId, 
							  true,
							  SnapshotNow, 
							  1, 
							  &key);
	
	tuple = systable_getnext(scan);
	if (!HeapTupleIsValid(tuple))
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_OBJECT),
				 errmsg("filesystem \"%s\" does not exist",
						fsys_name)));
	
	funcDatum = heap_getattr(tuple, 
							 fsys_func_type_to_attnum(fsys_type),
							 dsc,
							 &isNull);
	
	if (isNull)
	{
		if (error)
			ereport(ERROR,
					(errcode(ERRCODE_UNDEFINED_OBJECT),
					 errmsg("filesystem '%s' has no %s function defined",
							 fsys_name, fsys_func_type_to_name(fsys_type))));	
	}
	else
	{
		funcOid = DatumGetObjectId(funcDatum);
	}
	
	/* Finish up scan and close pg_filesystem catalog. */
	systable_endscan(scan);
	
	heap_close(rel, AccessShareLock);

	return funcOid;

}

/*
 * Same as LookupFileSystemFunction but returns the actual
 * filesystem Oid.
 */
Oid
LookupFileSystemOid(const char *fsys_name, bool missing_ok)
{
	Relation	pg_filesystem_rel;
	TupleDesc	pg_filesystem_dsc;
	HeapTuple	tuple;
	ScanKeyData	key;
	SysScanDesc	scan;
	Oid			fsysOid = InvalidOid;
	
	/*
	 * Check the pg_filesystem relation to be certain the ao table 
	 * is there. 
	 */
	pg_filesystem_rel = heap_open(FileSystemRelationId, AccessShareLock);
	pg_filesystem_dsc = RelationGetDescr(pg_filesystem_rel);
	
	ScanKeyInit(&key,
				Anum_pg_filesystem_fsysname,
				BTEqualStrategyNumber,
				F_NAMEEQ,
				CStringGetDatum(fsys_name));
	
	scan = systable_beginscan(pg_filesystem_rel, 
							  FileSystemFsysnameIndexId,
							  true,
							  SnapshotNow, 
							  1, 
							  &key);
	
	tuple = systable_getnext(scan);
	if (!HeapTupleIsValid(tuple))
	{
		if(!missing_ok)
			ereport(ERROR,
					(errcode(ERRCODE_UNDEFINED_OBJECT),
					 errmsg("filesystem \"%s\" does not exist",
							fsys_name)));
	}
	else
		fsysOid = HeapTupleGetOid(tuple);
	
	/* Finish up scan and close pg_filesystem catalog. */
	systable_endscan(scan);
	
	heap_close(pg_filesystem_rel, AccessShareLock);

	return fsysOid;
}

char *
FileSystemGetNameByOid(Oid	fsysOid)
{		
	Relation	rel;
	HeapScanDesc scandesc;
	HeapTuple	tuple;
	ScanKeyData entry[1];
	Datum		fsysnameDatum;
	Name		fsysname;
	char		*fsysnamestr;
	bool		isNull;
	
	/*
	 * Search pg_filesystem.  We use a heapscan here even though there is an
	 * index on oid, on the theory that pg_filesystem will usually have just a
	 * few entries and so an indexed lookup is a waste of effort.
	 */
	rel = heap_open(FileSystemRelationId, AccessShareLock);
	ScanKeyInit(&entry[0],
				ObjectIdAttributeNumber,
				BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(fsysOid));
	scandesc = heap_beginscan(rel, SnapshotNow, 1, entry);
	tuple = heap_getnext(scandesc, ForwardScanDirection);

	/* We assume that there can be at most one matching tuple */
	if (!HeapTupleIsValid(tuple))
	{
		elog(ERROR, "filesystem %u could not be found", fsysOid);
	}

	fsysnameDatum = heap_getattr(tuple, 
								Anum_pg_filesystem_fsysname,
								rel->rd_att, 
								&isNull);

	fsysname = DatumGetName(fsysnameDatum);
	fsysnamestr = strdup(NameStr(*fsysname));
	
	if(isNull)
		ereport(ERROR,
			(errcode(ERRCODE_UNDEFINED_OBJECT),
			 errmsg("internal error: filesystem '%u' has no name defined",
					 fsysOid)));

	heap_endscan(scandesc);
	heap_close(rel, AccessShareLock);
	
	return fsysnamestr;
}

char *fsys_func_type_to_name(FileSystemFuncType ftype)
{
	if (ftype < FSYS_FUNC_CONNECT || ftype >= FSYS_FUNC_TOTALNUM)
	{
		elog(ERROR, "internal error in pg_filesystem:func_type_to_name");
		return NULL;
	}
	return fsysFuncNames[ftype];
}

int fsys_func_type_to_attnum(FileSystemFuncType ftype)
{
	if (ftype < FSYS_FUNC_CONNECT || ftype >= FSYS_FUNC_TOTALNUM)
	{
		elog(ERROR, "internal error in pg_filesystem:func_type_to_attnum");
		return -1;
	}
	return Anum_pg_filesystem_fsysconnfn + (ftype - FSYS_FUNC_CONNECT);
}
