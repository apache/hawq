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
 * Copyright (c) 2007-2013, Greenplum inc
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

	int64 nextFastSequence;
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
CloseQueryContextInfo(QueryContextInfo *cxt);

extern void
DropQueryContextInfo(QueryContextInfo *cxt);

extern void
AtEOXact_QueryContext(void);

extern void
RebuildQueryContext(QueryContextInfo *cxt);

extern void
AddAuxInfoToQueryContextInfo(QueryContextInfo *cxt);

extern void
AddEmptyTableFlagToContextInfo(QueryContextInfo *cxt, Oid relid);

extern void
AddTupleToContextInfo(QueryContextInfo *cxt, Oid relid, const char *relname,
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
        bool forInsert, int32 segno);

extern HTAB *
createPrepareDispatchedCatalogRelationDisctinctHashTable(void);

#endif /* _CDB_CDBQUERYCONTEXTDISPATCHING_H_ */
