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
 * filesystem.c
 *	  Plugable file system interface
 *
 * Portions Copyright (c) 2007-2009, Greenplum inc
 * Portions Copyright (c) 1996-2009, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  $PostgreSQL: pgsql/src/backend/storage/file/filesystem.c,v 1.0 2012/05/25 17:10:22 Exp $
 *
 * NOTES:
 */

#include "postgres.h"

#include "storage/filesystem.h"

#include "access/relscan.h"
#include "access/heapam.h"
#include "utils/builtins.h"
#include "utils/rel.h"
#include "utils/hsearch.h"
#include "utils/memutils.h"
#include "utils/fmgroids.h"
#include "utils/flatfiles.h"
#include "catalog/pg_filesystem.h"
#include "libpq/hba.h"

#include "cdb/cdbvars.h"

/* Debugging.... */
#ifdef FDDEBUG
#define DO_DB(A) A
#else
#define DO_DB(A)				/* A */
#endif

#define atooid(x)  ((Oid) strtoul((x), NULL, 10))

/* fault injection */
#ifdef USE_ASSERT_CHECKING

static inline bool
testmode_fault(int percent)
{
	if (random() % 100 < percent)
		return true;
    return false;
}
#endif

typedef struct FsysInterfaceData
{
	char host[MAXPGPATH + 1];
	FmgrInfo fsysFuncs[FSYS_FUNC_TOTALNUM];
} FsysInterfaceData;

typedef struct FsysInterfaceData *FsysInterface;


static HTAB *FsysInterfaceTable = NULL;
static MemoryContext FsysGlobalContext = NULL;
#define EXPECTED_MAX_FSYS_ENTRIES 10

/**
 * 
 */
static int
InitFsysInterfaceFromFlatfile(FsysName name, FsysInterface fsys) {
	int			retval = 0;
	char	   *filename;
	FILE	   *fsys_file;
	List       *list = NIL;
	ListCell   *cell;
	char       *libFile;
	char       *funcName;

	filename = filesystem_getflatfilename();
	fsys_file = AllocateFile(filename, "r");
	if (fsys_file == NULL)
		ereport(FATAL,
				(errcode_for_file_access(),
				 errmsg("could not open file \"%s\": %m", filename)));

	if (!get_pg_filesystem_from_flatfile(filename, fsys_file, name, &list))
		ereport(ERROR,
                (errcode(ERRCODE_UNDEFINED_OBJECT),
                 errmsg("cannot find filesystem \"%s\" in flatfile \"%s\"", name, filename)));

	if (list_length(list) != FSYS_FUNC_TOTALNUM+1)
		elog(ERROR, "not enough items in flatfile for filesystem \"%s\", need %d, found %d",
			 name, FSYS_FUNC_TOTALNUM+1, list_length(list));

	cell = list_head(list);

	/* get fsys libfile */
	libFile = lfirst(cell);
	cell = lnext(cell);

	/* Init all funcs used by filesystem */
	for(int i = 0; i < FSYS_FUNC_TOTALNUM; i++)
	{
		FmgrInfo *finfo = &(fsys->fsysFuncs[i]);
		void	   *libraryhandle;

		funcName = lfirst(cell);
		cell = lnext(cell);

		finfo->fn_addr = load_external_function(libFile, funcName, true,
												&libraryhandle);
		finfo->fn_oid = (Oid) (i+1);
		finfo->fn_nargs = 0;
		finfo->fn_strict = 0;
		finfo->fn_strict = 0;
		finfo->fn_retset = 0;
		finfo->fn_stats = 1;
		finfo->fn_extra = NULL;
		finfo->fn_mcxt = CurrentMemoryContext;
		finfo->fn_expr = NULL;
	}

	FreeFile(fsys_file);
	pfree(filename);
	if (list != NIL)
	{
		foreach (cell, list)
			pfree(lfirst(cell));
		list_free(list);
		list = NIL;
	}

	return retval;
}

static int
InitFsysInterface(FsysName name, FsysInterface fsys) {
	int retval = 0;
	Relation	rel;
	TupleDesc	dsc;
	HeapScanDesc scandesc;
	HeapTuple	tuple;
	ScanKeyData entry[1];
	Datum		funcDatum;
	Datum		libFileDatum;
	char	   *libFile;
	char	   *funcName;
	bool 		isNull;

	/*
	 * Search pg_filesystem.  We use a heapscan here even though there is an
	 * index on oid, on the theory that pg_filesystem will usually have just a
	 * few entries and so an indexed lookup is a waste of effort.
	 */
	rel = heap_open(FileSystemRelationId, AccessShareLock);
	dsc = RelationGetDescr(rel);

	ScanKeyInit(&entry[0],
				Anum_pg_filesystem_fsysname,
				BTEqualStrategyNumber, F_NAMEEQ,
				CStringGetDatum(name));
	scandesc = heap_beginscan(rel, SnapshotNow, 1, entry);
	tuple = heap_getnext(scandesc, ForwardScanDirection);

	/* We assume that there can be at most one matching tuple */
	if (!HeapTupleIsValid(tuple))
		ereport(ERROR,
                (errcode(ERRCODE_UNDEFINED_OBJECT),
                 errmsg("filesystem \"%s\" does not exist", name)));

	/* get libfile */
	libFileDatum = heap_getattr(tuple, Anum_pg_filesystem_fsyslibfile, dsc, &isNull);
	if(isNull)
	{
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_OBJECT),
				 errmsg("filesystem \"%s\" has no libfile specified", name)));
	}
	libFile = TextDatumGetCString(libFileDatum);

	/* Init all funcs used by filesystem */
	for(int i = 0; i < FSYS_FUNC_TOTALNUM; i++)
	{
		FmgrInfo *finfo = &(fsys->fsysFuncs[i]);
		void	   *libraryhandle;

		funcDatum = heap_getattr(tuple, fsys_func_type_to_attnum(i), dsc, &isNull);

		if(isNull)
		{
			ereport(ERROR,
					(errcode(ERRCODE_UNDEFINED_OBJECT),
					 errmsg("filesystem \"%s\" has no %s function defined", name,
							fsys_func_type_to_name(i))));
		}

		funcName = NameStr(*(DatumGetName(funcDatum)));

		finfo->fn_addr = load_external_function(libFile, funcName, true,
												&libraryhandle);
		finfo->fn_oid = (Oid) 1;
		finfo->fn_nargs = 0;
		finfo->fn_strict = 0;
		finfo->fn_strict = 0;
		finfo->fn_retset = 0;
		finfo->fn_stats = 1;
		finfo->fn_extra = NULL;
		finfo->fn_mcxt = CurrentMemoryContext;
		finfo->fn_expr = NULL;
	}

	heap_endscan(scandesc);
	heap_close(rel, AccessShareLock);

	return retval;
}

/**
 *
 */
static FsysInterface FsysInterfaceGet(FsysName name) {
	Assert(NULL != name);
	FsysInterface entry = NULL;
	HASHCTL hash_ctl;
	bool found = false;

	do {
		if(NULL == FsysInterfaceTable) {
			if(NULL == FsysGlobalContext) {
				Assert(NULL != TopMemoryContext);
				FsysGlobalContext = AllocSetContextCreate(TopMemoryContext,
					"Filesystem Global Context", ALLOCSET_DEFAULT_MINSIZE,
					ALLOCSET_DEFAULT_INITSIZE, ALLOCSET_DEFAULT_MAXSIZE);
			}

			MemSet(&hash_ctl, 0, sizeof(hash_ctl));
			hash_ctl.keysize = MAXPGPATH;
			hash_ctl.entrysize = sizeof(*entry);
			hash_ctl.hash = string_hash;
			hash_ctl.hcxt = FsysGlobalContext;

			FsysInterfaceTable = hash_create("filesystem hash table",
						 EXPECTED_MAX_FSYS_ENTRIES, &hash_ctl,
						 HASH_ELEM | HASH_FUNCTION | HASH_CONTEXT);

			if(FsysInterfaceTable == NULL) {
				elog(WARNING, "failed to create hash table: FsysInterfaceTable.");
				break;
			}
		}

		entry = (FsysInterface) hash_search(FsysInterfaceTable, name, HASH_ENTER, &found);

		if(!found) {
			Assert(NULL != entry);
			int ret = 0;
			if (0)
				ret = InitFsysInterface(name, entry);
			else
				ret = InitFsysInterfaceFromFlatfile(name, entry);

			if(0 != ret) {
				hash_search(FsysInterfaceTable, name, HASH_REMOVE, &found);
				entry = NULL;
				elog(WARNING, "fail to init filesystem: %s", name);
				break;
			}
		}

		Assert(NULL != entry);
	} while(0);

	return entry;
}

static FmgrInfo *
FsysInterfaceGetFunc(FsysName name, FileSystemFuncType funcType)
{
	FsysInterface fsysInterface = NULL;
	Assert(NULL != name && funcType >= 0 && funcType < FSYS_FUNC_TOTALNUM);

	fsysInterface = FsysInterfaceGet(name);
	return &(fsysInterface->fsysFuncs[funcType]);
}

hdfsFS
HdfsConnect(FsysName protocol, char * host, uint16_t port, char *ccname, void *token)
{
	FunctionCallInfoData fcinfo;
	FileSystemUdfData fsysUdf;
	FmgrInfo *fsysFunc = FsysInterfaceGetFunc(protocol, FSYS_FUNC_CONNECT);

#ifdef USE_ASSERT_CHECKING
    if (testmode_fault(gp_fsys_fault_inject_percent))
        return NULL;
#endif

	fsysUdf.type = T_FileSystemFunctionData;
	fsysUdf.fsys_host = host;
	fsysUdf.fsys_port = port;
	fsysUdf.fsys_hdfs = NULL;
	fsysUdf.fsys_ccname = ccname;
	fsysUdf.fsys_token = token;

	InitFunctionCallInfoData(/* FunctionCallInfoData */ fcinfo,
							 /* FmgrInfo */ fsysFunc,
							 /* nArgs */ 0, 
							 /* Call Context */ (Node *) (&fsysUdf),
							 /* ResultSetInfo */ NULL);

	FunctionCallInvoke(&fcinfo);

	return FSYS_UDF_GET_HDFS(&fcinfo);
}

int HdfsDisconnect(FsysName protocol, hdfsFS fileSystem)
{
	FunctionCallInfoData fcinfo;
	FileSystemUdfData fsysUdf;
	FmgrInfo *fsysFunc = FsysInterfaceGetFunc(protocol, FSYS_FUNC_DISCONNECT);

#ifdef USE_ASSERT_CHECKING
    if (testmode_fault(gp_fsys_fault_inject_percent))
        return -1;
#endif

	fsysUdf.type = T_FileSystemFunctionData;
	fsysUdf.fsys_hdfs = fileSystem;

	InitFunctionCallInfoData(/* FunctionCallInfoData */ fcinfo,
							 /* FmgrInfo */ fsysFunc,
							 /* nArgs */ 0, 
							 /* Call Context */ (Node *) (&fsysUdf),
							 /* ResultSetInfo */ NULL);

	Datum d = FunctionCallInvoke(&fcinfo);

	return DatumGetInt32(d);
}

hdfsFile HdfsOpenFile(FsysName protocol, hdfsFS fileSystem, char * path, int flags,
					  int bufferSize, short replication, int64_t blocksize)
{
	FunctionCallInfoData fcinfo;
	FileSystemUdfData fsysUdf;
	FmgrInfo *fsysFunc = FsysInterfaceGetFunc(protocol, FSYS_FUNC_OPEN);

#ifdef USE_ASSERT_CHECKING
    if (testmode_fault(gp_fsys_fault_inject_percent))
        return NULL;
#endif

	fsysUdf.type = T_FileSystemFunctionData;
	fsysUdf.fsys_hdfs = fileSystem;
	fsysUdf.fsys_filepath = path;
	fsysUdf.fsys_fileflags = flags;
	fsysUdf.fsys_filebufsize = bufferSize;
	fsysUdf.fsys_replication = replication;
	fsysUdf.fsys_fileblksize = blocksize;
	fsysUdf.fsys_hfile = NULL;

	InitFunctionCallInfoData(/* FunctionCallInfoData */ fcinfo,
							 /* FmgrInfo */ fsysFunc,
							 /* nArgs */ 0, 
							 /* Call Context */ (Node *) (&fsysUdf),
							 /* ResultSetInfo */ NULL);

	FunctionCallInvoke(&fcinfo);

	return FSYS_UDF_GET_HFILE(&fcinfo);
}

int
HdfsSync(FsysName protocol, hdfsFS fileSystem, hdfsFile file)
{
	FunctionCallInfoData fcinfo;
	FileSystemUdfData fsysUdf;
	FmgrInfo *fsysFunc = FsysInterfaceGetFunc(protocol, FSYS_FUNC_SYNC);

#ifdef USE_ASSERT_CHECKING
    if (testmode_fault(gp_fsys_fault_inject_percent))
        return -1;
#endif

	fsysUdf.type = T_FileSystemFunctionData;
	fsysUdf.fsys_hdfs = fileSystem;
	fsysUdf.fsys_hfile = file;

	InitFunctionCallInfoData(/* FunctionCallInfoData */ fcinfo,
							 /* FmgrInfo */ fsysFunc,
							 /* nArgs */ 0, 
							 /* Call Context */ (Node *) (&fsysUdf),
							 /* ResultSetInfo */ NULL);

	Datum d = FunctionCallInvoke(&fcinfo);

	return DatumGetInt32(d);
}

int HdfsCloseFile(FsysName protocol, hdfsFS fileSystem, hdfsFile file)
{
	FunctionCallInfoData fcinfo;
	FileSystemUdfData fsysUdf;
	FmgrInfo *fsysFunc = FsysInterfaceGetFunc(protocol, FSYS_FUNC_CLOSE);

#ifdef USE_ASSERT_CHECKING
    if (testmode_fault(gp_fsys_fault_inject_percent))
        return -1;
#endif

	fsysUdf.type = T_FileSystemFunctionData;
	fsysUdf.fsys_hdfs = fileSystem;
	fsysUdf.fsys_hfile = file;

	InitFunctionCallInfoData(/* FunctionCallInfoData */ fcinfo,
							 /* FmgrInfo */ fsysFunc,
							 /* nArgs */ 0, 
							 /* Call Context */ (Node *) (&fsysUdf),
							 /* ResultSetInfo */ NULL);

	Datum d = FunctionCallInvoke(&fcinfo);

	return DatumGetInt32(d);
}

int HdfsCreateDirectory(FsysName protocol, hdfsFS fileSystem, char * path)
{
	FunctionCallInfoData fcinfo;
	FileSystemUdfData fsysUdf;
	FmgrInfo *fsysFunc = FsysInterfaceGetFunc(protocol, FSYS_FUNC_MKDIR);

#ifdef USE_ASSERT_CHECKING
    if (testmode_fault(gp_fsys_fault_inject_percent))
        return -1;
#endif

	fsysUdf.type = T_FileSystemFunctionData;
	fsysUdf.fsys_hdfs = fileSystem;
	fsysUdf.fsys_filepath = path;

	InitFunctionCallInfoData(/* FunctionCallInfoData */ fcinfo,
							 /* FmgrInfo */ fsysFunc,
							 /* nArgs */ 0, 
							 /* Call Context */ (Node *) (&fsysUdf),
							 /* ResultSetInfo */ NULL);

	Datum d = FunctionCallInvoke(&fcinfo);

	return DatumGetInt32(d);
}

int
HdfsDelete(FsysName protocol, hdfsFS fileSystem, char * path, int recursive)
{
	FunctionCallInfoData fcinfo;
	FileSystemUdfData fsysUdf;
	FmgrInfo *fsysFunc = FsysInterfaceGetFunc(protocol, FSYS_FUNC_DELETE);

#ifdef USE_ASSERT_CHECKING
    if (testmode_fault(gp_fsys_fault_inject_percent))
        return -1;
#endif

	fsysUdf.type = T_FileSystemFunctionData;
	fsysUdf.fsys_hdfs = fileSystem;
	fsysUdf.fsys_filepath = path;
	fsysUdf.fsys_recursive = recursive;

	InitFunctionCallInfoData(/* FunctionCallInfoData */ fcinfo,
							 /* FmgrInfo */ fsysFunc,
							 /* nArgs */ 0, 
							 /* Call Context */ (Node *) (&fsysUdf),
							 /* ResultSetInfo */ NULL);

	Datum d = FunctionCallInvoke(&fcinfo);

	return DatumGetInt32(d);
}

int
HdfsChmod(FsysName protocol, hdfsFS fileSystem, char * path, short mode)
{
	FunctionCallInfoData fcinfo;
	FileSystemUdfData fsysUdf;
	FmgrInfo *fsysFunc = FsysInterfaceGetFunc(protocol, FSYS_FUNC_CHMOD);

#ifdef USE_ASSERT_CHECKING
    if (testmode_fault(gp_fsys_fault_inject_percent))
        return -1;
#endif

	fsysUdf.type = T_FileSystemFunctionData;
	fsysUdf.fsys_hdfs = fileSystem;
	fsysUdf.fsys_filepath = path;
	fsysUdf.fsys_mode = mode;

	InitFunctionCallInfoData(/* FunctionCallInfoData */ fcinfo,
							 /* FmgrInfo */ fsysFunc,
							 /* nArgs */ 0, 
							 /* Call Context */ (Node *) (&fsysUdf),
							 /* ResultSetInfo */ NULL);

	Datum d = FunctionCallInvoke(&fcinfo);

	return DatumGetInt32(d);
}

int
HdfsRead(FsysName protocol, hdfsFS fileSystem, hdfsFile file, void * buffer, int length)
{
	FunctionCallInfoData fcinfo;
	FileSystemUdfData fsysUdf;
	FmgrInfo *fsysFunc = FsysInterfaceGetFunc(protocol, FSYS_FUNC_READ);

#ifdef USE_ASSERT_CHECKING
    if (testmode_fault(gp_fsys_fault_inject_percent))
        return -1;
#endif

	fsysUdf.type = T_FileSystemFunctionData;
	fsysUdf.fsys_hdfs = fileSystem;
	fsysUdf.fsys_hfile = file;
	fsysUdf.fsys_databuf = buffer;
	fsysUdf.fsys_maxbytes = length;

	InitFunctionCallInfoData(/* FunctionCallInfoData */ fcinfo,
							 /* FmgrInfo */ fsysFunc,
							 /* nArgs */ 0, 
							 /* Call Context */ (Node *) (&fsysUdf),
							 /* ResultSetInfo */ NULL);

	Datum d = FunctionCallInvoke(&fcinfo);

	return DatumGetInt32(d);
}

int
HdfsWrite(FsysName protocol, hdfsFS fileSystem, hdfsFile file, const void * buffer, int length)
{
	FunctionCallInfoData fcinfo;
	FileSystemUdfData fsysUdf;
	FmgrInfo *fsysFunc = FsysInterfaceGetFunc(protocol, FSYS_FUNC_WRITE);

#ifdef USE_ASSERT_CHECKING
    if (testmode_fault(gp_fsys_fault_inject_percent))
        return -1;
#endif

	fsysUdf.type = T_FileSystemFunctionData;
	fsysUdf.fsys_hdfs = fileSystem;
	fsysUdf.fsys_hfile = file;
    fsysUdf.fsys_databuf = (char *) buffer;
	fsysUdf.fsys_maxbytes = length;

	InitFunctionCallInfoData(/* FunctionCallInfoData */ fcinfo,
							 /* FmgrInfo */ fsysFunc,
							 /* nArgs */ 0, 
							 /* Call Context */ (Node *) (&fsysUdf),
							 /* ResultSetInfo */ NULL);

	Datum d = FunctionCallInvoke(&fcinfo);

	return DatumGetInt32(d);
}

int
HdfsSeek(FsysName protocol, hdfsFS fileSystem, hdfsFile file, int64_t desiredPos)
{
	FunctionCallInfoData fcinfo;
	FileSystemUdfData fsysUdf;
	FmgrInfo *fsysFunc = FsysInterfaceGetFunc(protocol, FSYS_FUNC_SEEK);

#ifdef USE_ASSERT_CHECKING
    if (testmode_fault(gp_fsys_fault_inject_percent))
        return -1;
#endif

	fsysUdf.type = T_FileSystemFunctionData;
	fsysUdf.fsys_hdfs = fileSystem;
	fsysUdf.fsys_hfile = file;
	fsysUdf.fsys_pos = desiredPos;

	InitFunctionCallInfoData(/* FunctionCallInfoData */ fcinfo,
							 /* FmgrInfo */ fsysFunc,
							 /* nArgs */ 0, 
							 /* Call Context */ (Node *) (&fsysUdf),
							 /* ResultSetInfo */ NULL);

	Datum d = FunctionCallInvoke(&fcinfo);

	return DatumGetInt32(d);
}

int64_t HdfsTell(FsysName protocol, hdfsFS fileSystem, hdfsFile file)
{
	FunctionCallInfoData fcinfo;
	FileSystemUdfData fsysUdf;
	FmgrInfo *fsysFunc = FsysInterfaceGetFunc(protocol, FSYS_FUNC_TELL);

#ifdef USE_ASSERT_CHECKING
    if (testmode_fault(gp_fsys_fault_inject_percent))
        return -1;
#endif

	fsysUdf.type = T_FileSystemFunctionData;
	fsysUdf.fsys_hdfs = fileSystem;
	fsysUdf.fsys_hfile = file;

	InitFunctionCallInfoData(/* FunctionCallInfoData */ fcinfo,
							 /* FmgrInfo */ fsysFunc,
							 /* nArgs */ 0, 
							 /* Call Context */ (Node *) (&fsysUdf),
							 /* ResultSetInfo */ NULL);

	Datum d = FunctionCallInvoke(&fcinfo);

	return DatumGetInt64(d);
}

int HdfsTruncate(FsysName protocol, hdfsFS fileSystem, char * path, int64_t size)
{
	FunctionCallInfoData fcinfo;
	FileSystemUdfData fsysUdf;
	FmgrInfo *fsysFunc = FsysInterfaceGetFunc(protocol, FSYS_FUNC_TRUNCATE);

#ifdef USE_ASSERT_CHECKING
    if (testmode_fault(gp_fsys_fault_inject_percent))
        return -1;
#endif

	fsysUdf.type = T_FileSystemFunctionData;
	fsysUdf.fsys_hdfs = fileSystem;
	fsysUdf.fsys_filepath = path;
	fsysUdf.fsys_pos = size;

	InitFunctionCallInfoData(/* FunctionCallInfoData */ fcinfo,
							 /* FmgrInfo */ fsysFunc,
							 /* nArgs */ 0, 
							 /* Call Context */ (Node *) (&fsysUdf),
							 /* ResultSetInfo */ NULL);

	Datum d = FunctionCallInvoke(&fcinfo);

	return DatumGetInt32(d);
}

hdfsFileInfo * HdfsGetPathInfo(FsysName protocol, hdfsFS fileSystem, char * path)
{
	FunctionCallInfoData fcinfo;
	FileSystemUdfData fsysUdf;
	FmgrInfo *fsysFunc = FsysInterfaceGetFunc(protocol, FSYS_FUNC_GETPATHINFO);

	fsysUdf.type = T_FileSystemFunctionData;
	fsysUdf.fsys_hdfs = fileSystem;
	fsysUdf.fsys_filepath = path;
	fsysUdf.fsys_fileinfo = NULL;

	InitFunctionCallInfoData(/* FunctionCallInfoData */ fcinfo,
							 /* FmgrInfo */ fsysFunc,
							 /* nArgs */ 0, 
							 /* Call Context */ (Node *) (&fsysUdf),
							 /* ResultSetInfo */ NULL);

	(void) FunctionCallInvoke(&fcinfo);

	return FSYS_UDF_GET_FILEINFO(&fcinfo);
}

int HdfsFreeFileInfo(FsysName protocol, hdfsFileInfo * info, int numEntries)
{
	FunctionCallInfoData fcinfo;
	FileSystemUdfData fsysUdf;
	FmgrInfo *fsysFunc = FsysInterfaceGetFunc(protocol, FSYS_FUNC_FREEFILEINFO);

	fsysUdf.type = T_FileSystemFunctionData;
	fsysUdf.fsys_fileinfo = info;
	fsysUdf.fsys_fileinfonum = numEntries;

	InitFunctionCallInfoData(/* FunctionCallInfoData */ fcinfo,
							 /* FmgrInfo */ fsysFunc,
							 /* nArgs */ 0, 
							 /* Call Context */ (Node *) (&fsysUdf),
							 /* ResultSetInfo */ NULL);

	Datum d = FunctionCallInvoke(&fcinfo);

	return DatumGetInt32(d);
}
