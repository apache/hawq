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
* aosegfiles.h
*	  internal specifications of the pg_aoseg_* Append Only file segment
*	  list relation.
*
*-------------------------------------------------------------------------
*/
#ifndef AOSEGFILES_H
#define AOSEGFILES_H

#include "utils/rel.h"
#include "utils/tqual.h"
#include "catalog/pg_appendonly.h"

#define Natts_pg_aoseg					5
#define Anum_pg_aoseg_segno				1
#define Anum_pg_aoseg_eof				2
#define Anum_pg_aoseg_tupcount			3
#define Anum_pg_aoseg_varblockcount		4
#define Anum_pg_aoseg_eofuncompressed	5

#define InvalidFileSegNumber			-1
#define InvalidUncompressedEof			-1

#define AO_FILESEGINFO_ARRAY_SIZE		8

/*
 * pg_aoseg_nnnnnn table values for FormData_pg_attribute.
 *
 * [Similar examples are Schema_pg_type, Schema_pg_proc, Schema_pg_attribute, etc, in
 *  pg_attribute.h]
 */
#define Schema_pg_aoseg \
{ -1, {"segno"}, 				23, -1, 4, 1, 0, -1, -1, true, 'p', 'i', false, false, false, true, 0 }, \
{ -1, {"eof"},					701, -1, 8, 2, 0, -1, -1, true, 'p', 'd', false, false, false, true, 0 }, \
{ -1, {"tupcount"},				701, -1, 8, 3, 0, -1, -1, true, 'p', 'd', false, false, false, true, 0 }, \
{ -1, {"varblockcount"},		701, -1, 8, 4, 0, -1, -1, true, 'p', 'd', false, false, false, true, 0 }, \
{ -1, {"eofuncompressed"},		701, -1, 8, 5, 0, -1, -1, true, 'p', 'd', false, false, false, true, 0 }

/*
 * pg_aoseg_nnnnnn table values for FormData_pg_class.
 */
#define Class_pg_aoseg \
  {"pg_appendonly"}, PG_CATALOG_NAMESPACE, -1, BOOTSTRAP_SUPERUSERID, 0, \
               -1, DEFAULTTABLESPACE_OID, \
               25, 10000, 0, 0, 0, 0, false, false, RELKIND_RELATION, RELSTORAGE_HEAP, Natts_pg_aoseg, \
               0, 0, 0, 0, 0, false, false, false, false, FirstNormalTransactionId, {0}, {{{'\0','\0','\0','\0'},{'\0'}}}


/*
 * GUC variables
 */
extern int MaxAppendOnlyTables;		/* Max # of tables */

/*
 * Descriptor of a single AO relation file segment.
 */
typedef struct FileSegInfo
{
	TupleVisibilitySummary	tupleVisibilitySummary;

	int			segno;			/* the file segment number */
	int64		tupcount;		/* total number of tuples in this fileseg */
	int64		varblockcount;	/* total number of varblocks in this fileseg */	
	ItemPointerData  sequence_tid;     /* tid for the unique sequence number for this fileseg */

	int64		eof;			/* the effective eof for this segno  */
	int64		eof_uncompressed;/* what would have been the eof if we didn't 
								   compress this rel (= eof if no compression)*/
} FileSegInfo;

/*
 * Structure that sums up the field total of all file 'segments'.
 * Note that even though we could actually use FileSegInfo for 
 * this purpose we choose not too since it's likely that FileSegInfo
 * will go back to using int instead of float8 now that each segment
 * has a size limit.
 */
typedef struct FileSegTotals
{
	int			totalfilesegs;  /* total number of file segments */
	int64		totalbytes;		/* the sum of all 'eof' values  */
	int64		totaltuples;	/* the sum of all 'tupcount' values */
	int64		totalvarblocks;	/* the sum of all 'varblockcount' values */
	int64		totalbytesuncompressed; /* the sum of all 'eofuncompressed' values */
} FileSegTotals;

typedef enum
{
	SegfileNoLock,
	SegfileTryLock,
	SegfileForceLock
} SegfileLockStrategy;

extern FileSegInfo *NewFileSegInfo(int segno);

extern void 
InsertInitialSegnoEntry(AppendOnlyEntry *aoEntry, int segno);

/*
* GetFileSegInfo
*
* Get the catalog entry for an appendonly (row-oriented) relation from the
* pg_aoseg_* relation that belongs to the currently used
* AppendOnly table.
*
* If a caller intends to append to this file segment entry they must
* already hold a relation Append-Only segment file (transaction-scope) lock (tag
* LOCKTAG_RELATION_APPENDONLY_SEGMENT_FILE) in order to guarantee
* stability of the pg_aoseg information on this segment file and exclusive right
* to append data to the segment file.
*/
extern FileSegInfo *
GetFileSegInfo(Relation parentrel, AppendOnlyEntry *aoEntry, Snapshot appendOnlyMetaDataSnapshot, int segno);

extern FileSegInfo **
GetAllFileSegInfo(Relation parentrel, AppendOnlyEntry *aoEntry, Snapshot appendOnlyMetaDataSnapshot,
		int *totalsegs);

extern FileSegInfo **
GetAllFileSegInfoWithSegno(Relation parentrel, AppendOnlyEntry *aoEntry, Snapshot appendOnlyMetaDataSnapshot,
		int segno, int *totalsegs);

extern FileSegInfo **
GetAllFileSegInfo_pg_aoseg_rel(char *relationName, AppendOnlyEntry *aoEntry, Relation pg_aoseg_rel, Snapshot appendOnlyMetaDataSnapshot, int expectedSegno,int *totalsegs);

extern void
UpdateFileSegInfo(Relation parentrel,
				  AppendOnlyEntry *aoEntry,
				  int segno,
				  int64 eof,
				  int64 eof_uncompressed,
				  int64 tuples_added, 
				  int64 varblocks_added);

extern FileSegTotals *
GetSegFilesTotals(Relation parentrel, Snapshot appendOnlyMetaDataSnapshot);

extern int64 GetAOTotalBytes(Relation parentrel, Snapshot appendOnlyMetaDataSnapshot);

extern void FreeAllSegFileInfo(FileSegInfo **allSegInfo,
							   int totalSegFiles);

extern List *AOGetAllSegFileSplits(AppendOnlyEntry *aoEntry, Snapshot appendOnlyMetaDataSnapshot);

extern void AOFetchSegFileInfo(AppendOnlyEntry *aoEntry, List *segfileinfos, Snapshot appendOnlyMetaDataSnapshot);

extern Datum 
gp_update_ao_master_stats_name(PG_FUNCTION_ARGS);

extern Datum 
gp_update_ao_master_stats_oid(PG_FUNCTION_ARGS);

extern Datum 
get_ao_distribution_name(PG_FUNCTION_ARGS);

extern Datum
get_ao_distribution_oid(PG_FUNCTION_ARGS);

extern Datum 
get_ao_compression_ratio_name(PG_FUNCTION_ARGS);

extern Datum 
get_ao_compression_ratio_oid(PG_FUNCTION_ARGS);

#endif   /* AOSEGFILES_H */
