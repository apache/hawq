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
 * cdbquerycontextdispatching.h
 *     In gpsql, QD collect execute context which will be used on QE,
 *     it include some catalogs and some global variables.
 *
 *     Currently QD write this execute context to a local temporary file,
 *     and then pass it to all QE depending on its size.
 *     If it is larger than QueryContextDispatchingSizeMemoryLimit,
 *     it will be copied to shared storage (currently HDFS),
 *     and QE will read it and rebuild its execute context,
 *     else pass it by dispatching.
 *
 *     We use a shared storage to pass execute context because it is may big,
 *     and current dispatcher cannot do broadcast work effectively.
 *     Writing into shared storage is a kind of broadcasting.
 *
 *
 *-------------------------------------------------------------------------
 */

#ifndef _CDB_CDBQUERYCONTEXTDISPATCHING_H_
#define _CDB_CDBQUERYCONTEXTDISPATCHING_H_

#include "postgres.h"

#include "nodes/nodes.h"
#include "storage/fd.h"
#include "utils/hsearch.h"
#include "utils/rel.h"
#include "utils/relcache.h"
#include "libpq/pqformat.h"

struct Plan;

extern int
QueryContextDispatchingSizeMemoryLimit;

struct QueryContextInfo
{
    NodeTag 	type;

    bool 		useFile;

    char 	   *sharedPath;     /* path on shared storage */
    File 		file;           /* opened file on shared storage */

    char 	   *buffer;
    int 		size;
    int 		cursor;

    HTAB	   *htab;			/* a hash table used to dedup */

    List	   *errTblOid;		/* already handled error table oid in the statement */

    bool  finalized;   /* whether this query context info is closed */
};

typedef struct QueryContextInfo QueryContextInfo;

/**
 * used to hold information to send back to QD.
 */
struct QueryContextDispatchingSendBackData
{
	Oid	relid;
	int32 segno;
	int32 contentid;
	int64 varblock;
	int64 insertCount;
	int32 numfiles;
	int64 *eof;
	int64 *uncompressed_eof;
};
typedef struct QueryContextDispatchingSendBackData * QueryContextDispatchingSendBack;

extern QueryContextDispatchingSendBack
CreateQueryContextDispatchingSendBack(int nfile);

extern void
UpdateCatalogModifiedOnSegments(QueryContextDispatchingSendBack sendback);

extern StringInfo
PreSendbackChangedCatalog(int aocount);

extern void
AddSendbackChangedCatalogContent(StringInfo buf,
		QueryContextDispatchingSendBack sendback);

extern void
FinishSendbackChangedCatalog(StringInfo buf);

extern void
DropQueryContextDispatchingSendBack(QueryContextDispatchingSendBack sendback);

extern QueryContextInfo *
CreateQueryContextInfo(void);

/* for read */
extern void
InitQueryContextInfoFromFile(QueryContextInfo *cxt);

extern void
FinalizeQueryContextInfo(QueryContextInfo *cxt);

extern void
DropQueryContextInfo(QueryContextInfo *cxt);

extern void
AtEOXact_QueryContext(void);

extern void
RebuildQueryContext(QueryContextInfo *cxt, HTAB **currentFilesystemCredentials,
        MemoryContext *currentFilesystemCredentialsMemoryContext);

extern void
AddAuxInfoToQueryContextInfo(QueryContextInfo *cxt);

extern void
AddEmptyTableFlagToContextInfo(QueryContextInfo *cxt, Oid relid);

extern void
AddTupleToContextInfo(QueryContextInfo *cxt, Oid relid, const char *relname,
        HeapTuple tuple, int32 contentid);

extern void
AddTupleWithToastsToContextInfo(QueryContextInfo *cxt, Oid relid, const char *relname,
        HeapTuple tuple, int32 contentid);

extern void
AddTablespaceLocationToContextInfo(QueryContextInfo *cxt, Oid tspoid,
        const char *fmt);
void
AddPgAosegFileToContextInfo(QueryContextInfo *cxt, Relation rel, Oid relaoseg,
        int numOfTuples, const char *buffer, int size);
void
AddAoFastSequenceToContextInfo(QueryContextInfo *cxt, Relation rel,
        Oid fastsequence, int numOfTuples, const char *buffer, int size);

extern void
prepareDispatchedCatalogTargets(QueryContextInfo *cxt, List *targets);

extern void
prepareDispatchedCatalogPlan(QueryContextInfo *cxt, struct Plan *plan);

extern void
prepareDispatchedCatalog(QueryContextInfo *cxt, List *rtable);

extern void
prepareDispatchedCatalogRelation(QueryContextInfo *cxt, Oid relid,
        bool forInsert, List *segnoMaps);

extern void
prepareDispatchedCatalogNamespace(QueryContextInfo *cxt, Oid tablespace);

extern void
prepareDispatchedCatalogTablespace(QueryContextInfo *cxt, Oid tablespace);

extern void
prepareDispatchedCatalogSingleRelation(QueryContextInfo *cxt, Oid oid,
        bool forInsert, List *segnos);

extern HTAB *
createPrepareDispatchedCatalogRelationDisctinctHashTable(void);

extern List *
fetchSegFileInfos(Oid relid, List *segnos);

extern List *
GetResultRelSegFileInfos(Oid relid, List *segnomaps, List *existing_seginfomaps);

#endif /* _CDB_CDBQUERYCONTEXTDISPATCHING_H_ */
