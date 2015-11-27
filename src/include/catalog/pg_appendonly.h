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
* pg_appendonly.h
*	  internal specifications of the appendonly relation storage.
*
*-------------------------------------------------------------------------
*/
#ifndef PG_APPENDONLY_H
#define PG_APPENDONLY_H

#include "catalog/genbki.h"
#include "utils/relcache.h"
#include "utils/tqual.h"

/*
 * pg_appendonly definition.
 */

/* TIDYCAT_BEGINFAKEDEF

   CREATE TABLE pg_appendonly
   with (camelcase=AppendOnly, oid=false, relid=6105)
   (
   relid            oid, 
   blocksize        integer, 
   safefswritesize  integer, 
   compresslevel    smallint, 
   majorversion     smallint, 
   minorversion     smallint, 
   checksum         boolean, 
   compresstype     text, 
   columnstore      boolean,
   segrelid         oid, 
   segidxid         oid, 
   blkdirrelid      oid, 
   blkdiridxid      oid, 
   version          integer,
   pagesize			integer,
   splitsize        integer
   );

   create unique index on pg_appendonly(relid) with (indexid=5007, CamelCase=AppendOnlyRelid);

   alter table pg_appendonly add fk relid on pg_class(oid);

   TIDYCAT_ENDFAKEDEF
*/

#define AppendOnlyRelationId  6105

CATALOG(pg_appendonly,6105) BKI_WITHOUT_OIDS
{
	Oid				relid;				/* relation id */
	int4			blocksize;			/* the max block size of this relation */
	int4			safefswritesize;	/* min write size in bytes to prevent torn-write */
	int2			compresslevel;		/* the (per seg) total number of varblocks */
	int2			majorversion;		/* major version indicating what's stored in this table  */
	int2			minorversion;		/* minor version indicating what's stored in this table  */
	bool			checksum;			/* true if checksum is stored with data and checked */
	text			compresstype;		/* the compressor used (zlib, or quicklz) */
    bool            columnstore;        /* true if co or parquet table, false if ao table*/
    Oid             segrelid;           /* OID of aoseg table; 0 if none */
    Oid             segidxid;           /* if aoseg table, OID of segno index */
    Oid             blkdirrelid;        /* OID of aoblkdir table; 0 if none */
    Oid             blkdiridxid;        /* if aoblkdir table, OID of aoblkdir index */
    int4            version;            /* version of MemTuples and block layout for this table */
	int4			pagesize;			/* the max page size of this relation (parquet)*/
	int4			splitsize;			/* the size of a split */
} FormData_pg_appendonly;


/* ----------------
*		Form_pg_appendonly corresponds to a pointer to a tuple with
*		the format of pg_appendonly relation.
* ----------------
*/
typedef FormData_pg_appendonly *Form_pg_appendonly;

#define Natts_pg_appendonly					16
#define Anum_pg_appendonly_relid			1
#define Anum_pg_appendonly_blocksize		2
#define Anum_pg_appendonly_safefswritesize	3
#define Anum_pg_appendonly_compresslevel	4
#define Anum_pg_appendonly_majorversion		5
#define Anum_pg_appendonly_minorversion		6
#define Anum_pg_appendonly_checksum			7
#define Anum_pg_appendonly_compresstype		8
#define Anum_pg_appendonly_columnstore      9
#define Anum_pg_appendonly_segrelid         10
#define Anum_pg_appendonly_segidxid         11
#define Anum_pg_appendonly_blkdirrelid      12
#define Anum_pg_appendonly_blkdiridxid      13
#define Anum_pg_appendonly_version          14
#define Anum_pg_appendonly_pagesize			15
#define Anum_pg_appendonly_splitsize			16

/*
 * pg_appendonly table values for FormData_pg_attribute.
 *
 * [Similar examples are Schema_pg_type, Schema_pg_proc, Schema_pg_attribute, etc, in
 *  pg_attribute.h]
 */
#define Schema_pg_appendonly \
{ AppendOnlyRelationId, {"relid"}, 					26, -1,	4, 1, 0, -1, -1, true, 'p', 'i', true, false, false, true, 0 }, \
{ AppendOnlyRelationId, {"blocksize"}, 				23, -1, 4, 2, 0, -1, -1, true, 'p', 'i', true, false, false, true, 0 }, \
{ AppendOnlyRelationId, {"safefswritesize"},		27, -1, 4, 3, 0, -1, -1, true, 'p', 'i', true, false, false, true, 0 }, \
{ AppendOnlyRelationId, {"compresslevel"},			21, -1, 2, 4, 0, -1, -1, true, 'p', 's', true, false, false, true, 0 }, \
{ AppendOnlyRelationId, {"majorversion"},			21, -1, 2, 5, 0, -1, -1, true, 'p', 's', true, false, false, true, 0 }, \
{ AppendOnlyRelationId, {"minorversion"},			21, -1, 2, 6, 0, -1, -1, true, 'p', 's', true, false, false, true, 0 }, \
{ AppendOnlyRelationId, {"checksum"},				16, -1, 1, 7, 0, -1, -1, true, 'p', 'c', true, false, false, true, 0 }, \
{ AppendOnlyRelationId, {"compresstype"},			25, -1, -1, 8, 0, -1, -1, false, 'x', 'i', false, false, false, true, 0 }, \
{ AppendOnlyRelationId, {"columnstore"},			16, -1, 1, 9, 0, -1, -1, true, 'p', 'c', false, false, false, true, 0 }, \
{ AppendOnlyRelationId, {"segrelid"},				26, -1, 4, 10, 0, -1, -1, true, 'p', 'i', false, false, false, true, 0 }, \
{ AppendOnlyRelationId, {"segidxid"},				26, -1, 4, 11, 0, -1, -1, true, 'p', 'i', false, false, false, true, 0 }, \
{ AppendOnlyRelationId, {"blkdirrelid"},			26, -1, 4, 12, 0, -1, -1, true, 'p', 'i', false, false, false, true, 0 }, \
{ AppendOnlyRelationId, {"blkdiridxid"},			26, -1, 4, 13, 0, -1, -1, true, 'p', 'i', false, false, false, true, 0 }, \
{ AppendOnlyRelationId, {"version"},				23, -1, 4, 14, 0, -1, -1, true, 'p', 'i', false, false, false, true, 0 }, \
{ AppendOnlyRelationId, {"pagesize"}, 				23, -1, 4, 15, 0, -1, -1, true, 'p', 'i', false, false, false, true, 0 }, \
{ AppendOnlyRelationId, {"splitsize"}, 				23, -1, 4, 15, 0, -1, -1, true, 'p', 'i', false, false, false, true, 0 }

/*
 * pg_appendonly table values for FormData_pg_class.
 */
#define Class_pg_appendonly \
  {"pg_appendonly"}, PG_CATALOG_NAMESPACE, 10293, BOOTSTRAP_SUPERUSERID, 0, \
               AppendOnlyRelationId, DEFAULTTABLESPACE_OID, \
               25, 10000, 0, 0, 0, 0, false, false, RELKIND_RELATION, RELSTORAGE_HEAP, Natts_pg_appendonly, \
               0, 0, 0, 0, 0, false, false, false, false, FirstNormalTransactionId, {0}, {{{'\0','\0','\0','\0'},{'\0'}}}


/*
 * Descriptor of a single AO relation.
 * For now very similar to the catalog row itself but may change in time.
 */
typedef struct AppendOnlyEntry
{
	int		blocksize;
	int		pagesize;
	int		splitsize;
	int		safefswritesize;
	int		compresslevel;
	int		majorversion;
	int		minorversion;
	bool	checksum;
	char*	compresstype;
    bool    columnstore;
	Oid     segrelid;
	Oid     segidxid;
	Oid     blkdirrelid;
	Oid     blkdiridxid;
	int4    version;
} AppendOnlyEntry;

/* No initial contents. */

/*
 * AORelationVersion defines valid values for the version of AppendOnlyEntry.
 * NOTE: When this is updated, AoRelationVersion_GetLatest() must be updated accordingly.
 */
typedef enum AORelationVersion
{
	AORelationVersion_None =  0,
	AORelationVersion_Original =  1,		/* first valid version */
	AORelationVersion_Aligned64bit = 2,		/* version where the fixes for AOBlock and MemTuple
											 * were introduced, see MPP-7251 and MPP-7372. */
	MaxAORelationVersion                    /* must always be last */
} AORelationVersion;

#define AORelationVersion_GetLatest() AORelationVersion_Aligned64bit

#define AORelationVersion_IsValid(version) \
	(version > AORelationVersion_None && version < MaxAORelationVersion)

static inline void AORelationVersion_CheckValid(int version)
{
	if (!AORelationVersion_IsValid(version))
	{
		ereport(ERROR,
	 		    (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
	 		     errmsg("append-only table version %d is invalid", version)));
	}
}

/*
 * Versions higher than AORelationVersion_Original include the fixes for AOBlock and
 * MemTuple alignment.
 */
#define IsAOBlockAndMemtupleAlignmentFixed(version) \
( \
	AORelationVersion_CheckValid(version), \
	(version > AORelationVersion_Original) \
)

extern int test_appendonly_version_default;

extern HeapTuple
CreateAppendOnlyEntry(TupleDesc desp,
					  Oid relid,
		  	  	  	  int blocksize,
		  	  	  	  int pagesize,
		  	  	  	  int splitsize,
		  	  	  	  int safefswritesize,
		  	  	  	  int compresslevel,
		  	  	  	  int majorversion,
		  	  	  	  int minorversion,
		  	  	  	  bool checksum,
		  	  	  	  bool columnstore,
		  	  	  	  char* compresstype,
		  	  	  	  Oid segrelid,
		  	  	  	  Oid segidxid,
		  	  	  	  Oid blkdirrelid,
		  	  	  	  Oid blkdiridxid);

extern void
InsertAppendOnlyEntry(Oid relid, 
					  int blocksize, 
					  int pagesize,
					  int splitsize,
					  int safefswritesize, 
					  int compresslevel,
					  int majorversion,
					  int minorversion,
					  bool checksum,
                      bool columnstore,
					  char* compresstype,
					  Oid segrelid,
					  Oid segidxid,
					  Oid blkdirrelid,
					  Oid blkdiridxid);

extern AppendOnlyEntry *
GetAppendOnlyEntry(Oid relid, Snapshot appendOnlyMetaDataSnapshot);

extern AppendOnlyEntry *
GetAppendOnlyEntryFromTuple(
	Relation	pg_appendonly_rel,
	TupleDesc	pg_appendonly_dsc,
	HeapTuple	tuple,
	Oid			*relationId);

/*
 * Get the OIDs of the auxiliary relations and their indexes for an appendonly
 * relation.
 *
 * The OIDs will be retrieved only when the corresponding output variable is
 * not NULL.
 */
void
GetAppendOnlyEntryAuxOids(Oid relid,
						  Snapshot appendOnlyMetaDataSnapshot,
						  Oid *segrelid,
						  Oid *segidxid,
						  Oid *blkdirrelid,
						  Oid *blkdiridxid);

/*
 * Update the segrelid and/or blkdirrelid if the input new values
 * are valid OIDs.
 */
extern void
UpdateAppendOnlyEntryAuxOids(Oid relid,
							 Oid newSegrelid,
							 Oid newSegidxid,
							 Oid newBlkdirrelid,
							 Oid newBlkdiridxid);

extern void
RemoveAppendonlyEntry(Oid relid);

extern void
TransferAppendonlyEntry(Oid sourceRelId, Oid targetRelId);

extern void
SwapAppendonlyEntries(Oid entryRelId1, Oid entryRelId2);

#endif   /* PG_APPENDONLY_H */
