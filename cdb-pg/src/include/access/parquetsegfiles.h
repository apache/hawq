/*-------------------------------------------------------------------------
 *
 * parquetsegfiles.h
 *	  internal specifications of the pg_aoseg_* Parquet file segment
 *	  list relation.
 *
 * Portions Copyright (c) 2008, Greenplum Inc.
 *-------------------------------------------------------------------------
 */
#ifndef PARQUETSEGFILES_H
#define PARQUETSEGFILES_H

#include "utils/rel.h"
#include "utils/tqual.h"

#define Natts_pg_parquetseg					5
#define Anum_pg_parquetseg_segno			1
#define Anum_pg_parquetseg_eof				2
#define Anum_pg_parquetseg_tupcount			3
#define Anum_pg_parquetseg_eofuncompressed	4
#define Anum_pg_parquetseg_content			5

#define InvalidFileSegNumber			-1
#define InvalidUncompressedEof			-1

/*
 * pg_aoseg_nnnnnn table values for FormData_pg_attribute.
 *
 * [Similar examples are Schema_pg_type, Schema_pg_proc, Schema_pg_attribute, etc, in
 *  pg_attribute.h]
 */
#define Schema_pg_parquetseg \
{ -1, {"segno"}, 				23, -1, 4, 1, 0, -1, -1, true, 'p', 'i', false, false, false, true, 0 }, \
{ -1, {"eof"},					701, -1, 8, 2, 0, -1, -1, true, 'p', 'd', false, false, false, true, 0 }, \
{ -1, {"tupcount"},				701, -1, 8, 3, 0, -1, -1, true, 'p', 'd', false, false, false, true, 0 }, \
{ -1, {"eofuncompressed"},		701, -1, 8, 5, 0, -1, -1, true, 'p', 'd', false, false, false, true, 0 }, \
{ -1, {"contentid"},			23, -1, 4, 21, 0, -1, -1, true, 'p', 'i', true, false, false, true, 0 }

/*
 * pg_parquetseg_nnnnnn table values for FormData_pg_class.
 */
#define Class_pg_parquetseg \
  {"pg_parquet"}, PG_CATALOG_NAMESPACE, -1, BOOTSTRAP_SUPERUSERID, 0, \
               -1, DEFAULTTABLESPACE_OID, \
               25, 10000, 0, 0, 0, 0, false, false, RELKIND_RELATION, RELSTORAGE_HEAP, Natts_pg_parquetseg, \
               0, 0, 0, 0, 0, false, false, false, false, FirstNormalTransactionId, {0}, {{{'\0','\0','\0','\0'},{'\0'}}}

/*
 * Descriptor of a single parquet relation file segment.   should add parquet file metadata columns here.
 */
typedef struct ParquetFileSegInfo {
	TupleVisibilitySummary tupleVisibilitySummary;

	int segno; /* the file segment number */
	int content; /* content id of this tuple */
	int64 tupcount; /* total number of tuples in this fileseg */
	ItemPointerData sequence_tid; /* tid for the unique sequence number for this fileseg */

	int64 eof; /* the effective eof for this segno  */
	int64 eof_uncompressed;/* what would have been the eof if we didn't
	 compress this rel (= eof if no compression)*/
} ParquetFileSegInfo;

/*
 * Structure that sums up the field total of all file 'segments'.
 * Note that even though we could actually use ParquetFileSegInfo for
 * this purpose we choose not too since it's likely that ParquetFileSegInfo
 * will go back to using int instead of float8 now that each segment
 * has a size limit.
 */
typedef struct ParquetFileSegTotals
{
	int			totalfilesegs;  /* total number of file segments */
	int64		totalbytes;		/* the sum of all 'eof' values  */
	int64		totaltuples;	/* the sum of all 'tupcount' values */
	int64		totalbytesuncompressed; /* the sum of all 'eofuncompressed' values */
} ParquetFileSegTotals;

extern void
InsertInitialParquetSegnoEntry(AppendOnlyEntry *aoEntry, int segno,
		int contentid);

extern ParquetFileSegInfo *
GetParquetFileSegInfo(Relation parentrel, AppendOnlyEntry *aoEntry,
		Snapshot parquetMetaDataSnapshot, int segno, int contentid);

extern ParquetFileSegInfo **GetAllParquetFileSegInfo(Relation parentrel,
		AppendOnlyEntry *aoEntry, Snapshot parquetMetaDataSnapshot,
		int *totalsegs);

extern ParquetFileSegInfo **GetAllParquetFileSegInfo_pg_paqseg_rel(
		char *relationName, AppendOnlyEntry *aoEntry,
		Relation pg_parquetseg_rel, Snapshot parquetMetaDataSnapshot,
		bool returnAllSegmentsFiles, int *totalsegs);

extern void
UpdateParquetFileSegInfo(Relation parentrel, AppendOnlyEntry *aoEntry,
		int segno, int64 eof, int64 eof_uncompressed, int64 tuples_added,
		int32 contentid);

extern ParquetFileSegTotals *GetParquetSegFilesTotals(Relation parentrel,
		Snapshot parquetMetaDataSnapshot, int32 contentid);

extern int64 GetParquetTotalBytes(Relation parentrel, Snapshot parquetMetaDataSnapshot);

#endif   /* PARQUETSEGFILES_H */
