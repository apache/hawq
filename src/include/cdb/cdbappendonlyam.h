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
 * appendonlyam.h
 *	  append-only relation access method definitions.
 *
 *
 * Portions Copyright (c) 1996-2006, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2007, Greenplum Inc.
 *
 *-------------------------------------------------------------------------
 */
#ifndef APPENDONLYAM_H
#define APPENDONLYAM_H

#include "access/htup.h"
#include "access/relscan.h"
#include "access/sdir.h"
#include "access/tupmacs.h"
#include "access/xlogutils.h"
#include "executor/tuptable.h"
#include "nodes/primnodes.h"
#include "nodes/relation.h"
#include "storage/block.h"
#include "storage/lmgr.h"
#include "utils/rel.h"
#include "utils/tqual.h"
#include "storage/gp_compress.h"

#include "access/appendonlytid.h"

#include "cdb/cdbvarblock.h"
#include "cdb/cdbvars.h"
#include "cdb/cdbbufferedappend.h"
#include "cdb/cdbbufferedread.h"
#include "cdb/cdbvarblock.h"

#include "cdb/cdbappendonlystoragelayer.h"
#include "cdb/cdbappendonlystorageread.h"
#include "cdb/cdbappendonlystoragewrite.h"

#include "cdb/cdbquerycontextdispatching.h"

#define DEFAULT_COMPRESS_LEVEL				 (0)
#define MIN_APPENDONLY_BLOCK_SIZE			 (8 * 1024)
#define DEFAULT_APPENDONLY_BLOCK_SIZE		(32 * 1024)
#define MAX_APPENDONLY_BLOCK_SIZE			 (2 * 1024 * 1024)
#define DEFAULT_VARBLOCK_TEMPSPACE_LEN   	 (4 * 1024)
#define DEFAULT_FS_SAFE_WRITE_SIZE			 (0)
#define DEFAULT_SPLIT_WRITE_SIZE (appendonly_split_write_size_mb * 1024 * 1024)

/*
 * AppendOnlyInsertDescData is used for inserting data into append-only
 * relations. It serves an equivalent purpose as AppendOnlyScanDescData
 * (relscan.h) only that the later is used for scanning append-only 
 * relations. 
 */
typedef struct AppendOnlyInsertDescData
{
	Relation		aoi_rel;
	Snapshot		appendOnlyMetaDataSnapshot;
	MemTupleBinding *mt_bind;
	File			appendFile;
	int				appendFilePathNameMaxLen;
	char			*appendFilePathName;
	float8			insertCount;
	float8			varblockCount;
	int64           rowCount; /* total row count before insert */
	BlockNumber		cur_segno;
	AppendOnlyEntry *aoEntry;
	FileSegInfo     *fsInfo;
	VarBlockMaker	varBlockMaker;
	int64			bufferCount;
	bool			shouldCompress;
	bool			usingChecksum;
	bool			useNoToast;
	int32			completeHeaderLen;
	uint8			*tempSpace;
	uint8			*uncompressedBuffer; /* used for compression */

	int32			usableBlockSize;
	int32			maxDataLen;
	int32			tempSpaceLen;

	char						*title;
				/*
				 * A phrase that better describes the purpose of the this open.
				 *
				 * We manage the storage for this.
				 */

	/*
	 * These serve the equivalent purpose of the uppercase constants of the same
	 * name in tuptoaster.h but here we make these values dynamic.
	 */	
	int32			toast_tuple_threshold;
	int32			toast_tuple_target;
	AppendOnlyStorageAttributes storageAttributes;
	AppendOnlyStorageWrite		storageWrite;

	uint8			*nonCompressedData;

	QueryContextDispatchingSendBack sendback;

} AppendOnlyInsertDescData;

typedef AppendOnlyInsertDescData *AppendOnlyInsertDesc;

typedef struct AppendOnlyExecutorReadBlock
{
	MemoryContext	memoryContext;

	AppendOnlyStorageRead	*storageRead;

	int				segmentFileNum;

	int64			totalRowsScannned;

	int64			blockFirstRowNum;
	int64			headerOffsetInFile;
	uint8			*dataBuffer;
	int32			dataLen;
	int 			executorBlockKind;
	int 			rowCount;
	bool			isLarge;
	bool			isCompressed;

	uint8			*uncompressedBuffer; /* for decompression */

	uint8			*largeContentBuffer;
	int32			largeContentBufferLen;

	VarBlockReader  varBlockReader;
	int				readerItemCount;
	int				currentItemCount;
	
	uint8			*singleRow;
	int32			singleRowLen;

	/* synthetic system attributes */
	ItemPointerData cdb_fake_ctid;
	MemTupleBinding *mt_bind;
} AppendOnlyExecutorReadBlock;

/*
 * used for scan of append only relations using BufferedRead and VarBlocks
 */
typedef struct AppendOnlyScanDescData
{
	/* scan parameters */
	Relation	aos_rd;				/* target relation descriptor */
	Snapshot	appendOnlyMetaDataSnapshot;

	Index       aos_scanrelid;
	int			aos_nkeys;			/* number of scan keys */
	ScanKey		aos_key;			/* array of scan key descriptors */
	bool		aos_notoast;		/* using toast for this relation? */
	
	/* file segment scan state */
	int			aos_filenamepath_maxlen;
	char		*aos_filenamepath;
									/* the current segment file pathname. */
	int			aos_splits_processed; /* num of segfiles already processed */
	bool		aos_need_new_split;
	bool		aos_done_all_splits;
	
	MemoryContext	aoScanInitContext; /* mem context at init time */

	int32			usableBlockSize;
	int32			maxDataLen;

	AppendOnlyExecutorReadBlock	executorReadBlock;

	/* current scan state */
	bool		bufferDone;

	bool	initedStorageRoutines;

	AppendOnlyStorageAttributes	storageAttributes;
	AppendOnlyStorageRead		storageRead;

	char						*title;
				/*
				 * A phrase that better describes the purpose of the this open.
				 *
				 * We manage the storage for this.
				 */

	AppendOnlyEntry		*aoEntry;
	
	List *splits;

	bool toCloseFile;
}	AppendOnlyScanDescData;

typedef AppendOnlyScanDescData *AppendOnlyScanDesc;

/* ----------------
 *		function prototypes for appendonly access method
 * ----------------
 */

extern AppendOnlyScanDesc appendonly_beginscan(Relation relation,
											   Snapshot appendOnlyMetaDataSnapshot,
											   int nkeys, 
											   ScanKey key);
extern void appendonly_rescan(AppendOnlyScanDesc scan, ScanKey key);
extern void appendonly_endscan(AppendOnlyScanDesc scan);
extern MemTuple appendonly_getnext(AppendOnlyScanDesc scan, 
									ScanDirection direction,
									TupleTableSlot *slot);
extern AppendOnlyInsertDesc appendonly_insert_init(Relation rel, ResultRelSegFileInfo *segfileinfo);
extern void appendonly_insert(
		AppendOnlyInsertDesc aoInsertDesc, 
		MemTuple instup, 
		Oid *tupleOid, 
		AOTupleId *aoTupleId);
extern void appendonly_insert_finish(AppendOnlyInsertDesc aoInsertDesc);
extern BlockNumber RelationGuessNumberOfBlocks(double totalbytes);

extern void AppendOnlyStorageWrite_PadOutForSplit(
		AppendOnlyStorageWrite *storageWrite,
		int32 varblocksize);

#endif   /* APPENDONLYAM_H */
