/*
 * cdbparquetam.h
 *
 *  Created on: Jul 4, 2013
 *      Author: malili
 */

#ifndef CDBPARQUETAM_H_
#define CDBPARQUETAM_H_

#include "access/parquetsegfiles.h"
#include "access/parquetmetadata_c++/MetadataInterface.h"
#include "cdb/cdbappendonlystoragelayer.h"
#include "cdb/cdbparquetstorageread.h"
#include "cdb/cdbparquetstoragewrite.h"
#include "cdb/cdbmirroredappendonly.h"
#include "executor/tuptable.h"
#include "access/appendonlytid.h"
#include "commands/copy.h"
#include "cdb/cdbparquetcolumn.h"
#include "cdb/cdbparquetrowgroup.h"


/*
 * used for scan of parquet relations
 */
typedef struct ParquetScanDescData {
	/* scan parameters */
	Relation 			pqs_rd; /* target relation descriptor */
	TupleDesc 			pqs_tupDesc;

	/* file segment scan state */
	int 				pqs_filenamepath_maxlen;
	char 				*pqs_filenamepath; /* the current segment file pathname. */
	int 				pqs_total_segfiles; /* the relation file segment number */
	int 				pqs_segfiles_processed; /* num of segfiles already processed */
	ParquetFileSegInfo 	**pqs_segfile_arr; /* array of all segfiles information */
	bool 				pqs_need_new_segfile;
	bool 				pqs_done_all_segfiles;

	/*the projection of columns, which column need to be scanned*/
	bool 				*proj;

	/* synthetic system attributes */
	ItemPointerData 	cdb_fake_ctid;
	int64 				cur_seg_row;

	/*the projection of hawq attribute mapping to parquet column chunks. For each hawq column,
	 *there is an int array which stores corresponding parquet column chunks*/
	int					*hawqAttrToParquetColChunks;

	MemoryContext 		parquetScanInitContext; /* mem context at init time */

	ParquetExecutorReadRowGroup	executorReadRowGroup;    // may need change to row group reader of parquet

	/* current scan state */
	bool 				bufferDone;

	bool 				initedStorageRoutines;

	AppendOnlyStorageAttributes storageAttributes;
	ParquetStorageRead 	storageRead;

	char 				*title;

	QueryContextDispatchingSendBack sendback;

	AppendOnlyEntry 		*aoEntry;

	ParquetMetadata 	parquetMetadata;
} ParquetScanDescData;

typedef ParquetScanDescData *ParquetScanDesc;

/*
 * ParquetInsertDescData is used for storing state related
 * to inserting data into a writable parquet table.
 */
typedef struct ParquetInsertDescData {
	MemoryContext 		memoryContext;
	Relation 			parquet_rel;
	Snapshot 			parquetMetaDataSnapshot;
	File 				parquet_file; /*file handler*/
	int 				parquetFilePathNameMaxLen;
	char 				*parquetFilePathName; /*stores the filePathname, in hdfs path*/
	char 				*relname;

	int 				cur_segno;
	AppendOnlyEntry 	*aoEntry;
	ParquetFileSegInfo 	*fsInfo;

	bool 				metadataExists; /*whether metadata exists*/
	int64 				rowCount;
	int64				insertCount;	/*the records inserted.*/

	int64				fileLen_uncompressed;	/*uncompressed length of the data file*/
	int64				fileLen;	/*file length of the data file*/

	char 				*title;

	ParquetMetadata 	parquetMetadata;
	ParquetRowGroup 	current_rowGroup;

	MirroredAppendOnlyOpen 				*mirroredOpen; /*used for opening segment file*/
	QueryContextDispatchingSendBack 	sendback;

} ParquetInsertDescData;

typedef ParquetInsertDescData *ParquetInsertDesc;

/* ----------------
 *		function prototypes for parquet access method
 * ----------------
 */

extern ParquetScanDesc parquet_beginscan(
		Relation relation,
		Snapshot parquetMetaDataSnapshot,
		TupleDesc relationTupleDesc,
		bool *proj);

extern void parquet_rescan(
		ParquetScanDesc scan);

extern void parquet_endscan(
		ParquetScanDesc scan);

extern void parquet_getnext(
		ParquetScanDesc scan,
		ScanDirection direction,
		TupleTableSlot *slot);

extern ParquetInsertDesc parquet_insert_init(
		Relation rel,
		int segno);

extern Oid parquet_insert(
		ParquetInsertDesc parquetInsertDesc,
		TupleTableSlot *slot);

extern Oid parquet_insert_values(
		ParquetInsertDesc parquetInsertDesc,
		Datum *values,
		bool *nulls,
		AOTupleId *aoTupleId);

extern void parquet_insert_finish(
		ParquetInsertDesc parquetInsertDesc);

#endif /* CDBPARQUETAM_H_ */
