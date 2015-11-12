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
#include "nodes/relation.h"

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
	int 				pqs_splits_processed; /* num of segfiles already processed */
	bool 				pqs_need_new_split;
	bool 				pqs_done_all_splits;

	/*the projection of columns, which column need to be scanned*/
	bool 				*proj;

	/* synthetic system attributes */
	ItemPointerData 	cdb_fake_ctid;
	int64 				cur_seg_row;

	/*the projection of hawq attribute mapping to parquet column chunks. For each hawq column,
	 *there is an int array which stores corresponding parquet column chunks*/
	int					*hawqAttrToParquetColChunks;

	MemoryContext 		parquetScanInitContext; /* mem context at init time */

	ParquetRowGroupReader	rowGroupReader;

	/* current scan state */
	bool 				bufferDone;

	bool 				initedStorageRoutines;

	AppendOnlyStorageAttributes storageAttributes;
	ParquetStorageRead 	storageRead;

	QueryContextDispatchingSendBack sendback;

	AppendOnlyEntry 		*aoEntry;

	List *splits;
	bool toCloseFile; // identify if it's ready to close segment file
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
	File				file_previousmetadata;		/*the read file descriptor for previous metadata*/
	CompactProtocol 	*protocol_read;		/*the footer protocol for reading previous metadata*/
	int 				parquetFilePathNameMaxLen;
	char 				*parquetFilePathName; /*stores the filePathname, in hdfs path*/
	char 				*relname;

	int 				cur_segno;
	AppendOnlyEntry 	*aoEntry;
	ParquetFileSegInfo 	*fsInfo;

	int64 				rowCount;
	int64				insertCount;	/*the records inserted.*/
	int					previous_rowgroupcnt;		/*the origin row group count*/

	int64				fileLen_uncompressed;	/*uncompressed length of the data file*/
	int64				fileLen;	/*file length of the data file*/

	char 				*title;

	ParquetMetadata 	parquetMetadata;
	ParquetRowGroup 	current_rowGroup;

	CompactProtocol		*footerProtocol;	/*footer protocol for processing */

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
		ResultRelSegFileInfo *segfileinfo);

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

extern uint64 memReservedForParquetInsert(
		Oid rel_oid);

extern uint64 memReservedForParquetScan(
		Oid rel_oid,
		List* attr_list);

#endif /* CDBPARQUETAM_H_ */
