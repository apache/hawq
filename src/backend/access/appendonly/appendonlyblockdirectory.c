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

/*-----------------------------------------------------------------------------
 *
 * appendonlyblockdirectory
 *    maintain the block directory to blocks in an append-only relation
 * files.
 *
 *-----------------------------------------------------------
 */
#include "postgres.h"

#include "cdb/cdbappendonlyblockdirectory.h"
#include "catalog/aoblkdir.h"
#include "access/heapam.h"
#include "access/genam.h"
#include "catalog/indexing.h"
#include "parser/parse_oper.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/guc.h"
#include "cdb/cdbappendonlyam.h"

int gp_blockdirectory_entry_min_range = 0;
int gp_blockdirectory_minipage_size = NUM_MINIPAGE_ENTRIES;

static inline uint32 minipage_size(uint32 nEntry)
{
	return offsetof(Minipage, entry) +
		sizeof(MinipageEntry) * nEntry;
}

static void load_last_minipage(
	AppendOnlyBlockDirectory *blockDirectory,
	int64 lastSequence,
	int columnGroupNo);
static void init_scankeys(
	TupleDesc tupleDesc,
	int nkeys, ScanKey scanKeys,
	StrategyNumber *strategyNumbers);
static int find_minipage_entry(
	Minipage *minipage,
	uint32 numEntries,
	int64 rowNum);
static void extract_minipage(
	AppendOnlyBlockDirectory *blockDirectory,
	HeapTuple tuple,
	TupleDesc tupleDesc,
	int columnGroupNo);
static void write_minipage(AppendOnlyBlockDirectory *blockDirectory,
						   int columnGroupNo);

void 
AppendOnlyBlockDirectoryEntry_GetBeginRange(
	AppendOnlyBlockDirectoryEntry	*directoryEntry,
	int64							*fileOffset,
	int64							*firstRowNum)
{
	*fileOffset = directoryEntry->range.fileOffset;
	*firstRowNum = directoryEntry->range.firstRowNum;
}

void 
AppendOnlyBlockDirectoryEntry_GetEndRange(
	AppendOnlyBlockDirectoryEntry	*directoryEntry,
	int64							*afterFileOffset,
	int64							*lastRowNum)
{
	*afterFileOffset = directoryEntry->range.afterFileOffset;
	*lastRowNum = directoryEntry->range.lastRowNum;
}

bool
AppendOnlyBlockDirectoryEntry_RangeHasRow(
	AppendOnlyBlockDirectoryEntry	*directoryEntry,
	int64							checkRowNum)
{
	return (checkRowNum >= directoryEntry->range.firstRowNum &&
		    checkRowNum <= directoryEntry->range.lastRowNum);
}

/*
 * init_internal
 *
 * Initialize the block directory structure.
 */
static void
init_internal(AppendOnlyBlockDirectory *blockDirectory)
{
	MemoryContext oldcxt;
	int numScanKeys;
	TupleDesc heapTupleDesc;
	TupleDesc idxTupleDesc;
	int groupNo;
	
	Assert(blockDirectory->blkdirRel != NULL);
	Assert(blockDirectory->blkdirIdx != NULL);
	
	blockDirectory->memoryContext =
		AllocSetContextCreate(CurrentMemoryContext,
							  "BlockDirectoryContext",
							  ALLOCSET_DEFAULT_MINSIZE,
							  ALLOCSET_DEFAULT_INITSIZE,
							  ALLOCSET_DEFAULT_MAXSIZE);

	oldcxt = MemoryContextSwitchTo(blockDirectory->memoryContext);

	heapTupleDesc = RelationGetDescr(blockDirectory->blkdirRel);
	blockDirectory->values = palloc0(sizeof(Datum) * heapTupleDesc->natts);
	blockDirectory->nulls = palloc0(sizeof(bool) * heapTupleDesc->natts);
	blockDirectory->numScanKeys = 3;
	numScanKeys = blockDirectory->numScanKeys;
	blockDirectory->scanKeys = palloc0(numScanKeys * sizeof(ScanKeyData));

	blockDirectory->strategyNumbers = palloc0(numScanKeys * sizeof(StrategyNumber));
	blockDirectory->strategyNumbers[0] = BTEqualStrategyNumber;
	blockDirectory->strategyNumbers[1] = BTEqualStrategyNumber;
	blockDirectory->strategyNumbers[2] = BTLessEqualStrategyNumber;

	idxTupleDesc = RelationGetDescr(blockDirectory->blkdirIdx);

	init_scankeys(idxTupleDesc, numScanKeys,
				  blockDirectory->scanKeys,
				  blockDirectory->strategyNumbers);

	/* Initialize the last minipage */
	blockDirectory->minipages =
		palloc0(sizeof(MinipagePerColumnGroup) * blockDirectory->numColumnGroups);
	for (groupNo = 0; groupNo < blockDirectory->numColumnGroups; groupNo++)
	{
		MinipagePerColumnGroup *minipageInfo = &blockDirectory->minipages[groupNo];
		
		minipageInfo->minipage =
			palloc0(minipage_size(NUM_MINIPAGE_ENTRIES));
		minipageInfo->numMinipageEntries = 0;
	}
	

	MemoryContextSwitchTo(oldcxt);
}

/*
 * AppendOnlyBlockDirectory_Init_forSearch
 *
 * Initialize the block directory to handle the lookup.
 *
 * If the block directory relation for this appendonly relation
 * does not exist before calling this function, set blkdirRel
 * and blkdirIdx to NULL, and return.
 */
void
AppendOnlyBlockDirectory_Init_forSearch(
	AppendOnlyBlockDirectory *blockDirectory,
	AppendOnlyEntry *aoEntry,
	Snapshot appendOnlyMetaDataSnapshot,
	FileSegInfo **segmentFileInfo,
	int totalSegfiles,
	Relation aoRel,
	int numColumnGroups)
{	
	Assert(aoEntry != NULL);

	blockDirectory->aoRel = aoRel;

	if (!OidIsValid(aoEntry->blkdirrelid))
	{
		Assert(!OidIsValid(aoEntry->blkdiridxid));
		blockDirectory->blkdirRel = NULL;
		blockDirectory->blkdirIdx = NULL;

		return;
	}

	if (Debug_appendonly_print_blockdirectory)
		ereport(LOG,
				(errmsg("Append-only block directory init for search: "
						"(totalSegfiles, numColumnGroups)="
						"(%d, %d)",
						totalSegfiles, numColumnGroups)));

	blockDirectory->segmentFileInfo = segmentFileInfo;
	blockDirectory->totalSegfiles = totalSegfiles;
	blockDirectory->aoRel = aoRel;
	blockDirectory->appendOnlyMetaDataSnapshot = appendOnlyMetaDataSnapshot;
	blockDirectory->numColumnGroups = numColumnGroups;
	blockDirectory->currentSegmentFileNum = -1;

	Assert(OidIsValid(aoEntry->blkdirrelid));

	blockDirectory->blkdirRel =
		heap_open(aoEntry->blkdirrelid, AccessShareLock);

	Assert(OidIsValid(aoEntry->blkdiridxid));

	blockDirectory->blkdirIdx =
		index_open(aoEntry->blkdiridxid, AccessShareLock);

	init_internal(blockDirectory);
}

/*
 * AppendOnlyBlockDirectory_Init_forInsert
 *
 * Initialize the block directory to handle the inserts.
 *
 * If the block directory relation for this appendonly relation
 * does not exist before calling this function, set blkdirRel
 * and blkdirIdx to NULL, and return.
 */
void
AppendOnlyBlockDirectory_Init_forInsert(
	AppendOnlyBlockDirectory *blockDirectory,
	AppendOnlyEntry *aoEntry,
	Snapshot appendOnlyMetaDataSnapshot,
	FileSegInfo *segmentFileInfo,
	int64 lastSequence,
	Relation aoRel,
	int segno,
	int numColumnGroups)
{
	int groupNo;

	Assert(aoEntry != NULL);

	blockDirectory->aoRel = aoRel;
	blockDirectory->appendOnlyMetaDataSnapshot = appendOnlyMetaDataSnapshot;

	if (!OidIsValid(aoEntry->blkdirrelid))
	{
		Assert(!OidIsValid(aoEntry->blkdiridxid));
		blockDirectory->blkdirRel = NULL;
		blockDirectory->blkdirIdx = NULL;

		return;
	}

	blockDirectory->segmentFileInfo = NULL;
	blockDirectory->totalSegfiles = -1;
	blockDirectory->currentSegmentFileInfo = segmentFileInfo;
	
	blockDirectory->currentSegmentFileNum = segno;
	blockDirectory->numColumnGroups = numColumnGroups;

	Assert(OidIsValid(aoEntry->blkdirrelid));

	blockDirectory->blkdirRel =
		heap_open(aoEntry->blkdirrelid, RowExclusiveLock);

	Assert(OidIsValid(aoEntry->blkdiridxid));

	blockDirectory->blkdirIdx =
		index_open(aoEntry->blkdiridxid, RowExclusiveLock);

	init_internal(blockDirectory);

	if (Debug_appendonly_print_blockdirectory)
		ereport(LOG,
				(errmsg("Append-only block directory init for insert: "
						"(segno, numColumnGroups, isAOCol, lastSequence)="
						"(%d, %d, " INT64_FORMAT ")",
						segno, numColumnGroups, lastSequence)));

	/*
	 * Load the last minipages from the block directory relation.
	 */
	for(groupNo = 0; groupNo < blockDirectory->numColumnGroups; groupNo++)
	{
		load_last_minipage(blockDirectory, lastSequence, groupNo);
	}
}

static bool
set_directoryentry_range(
	AppendOnlyBlockDirectory *blockDirectory,
	int columnGroupNo,
	int entry_no,
	AppendOnlyBlockDirectoryEntry *directoryEntry)
{
	MinipagePerColumnGroup *minipageInfo =
		&blockDirectory->minipages[columnGroupNo];
	FileSegInfo *fsInfo;
	MinipageEntry *entry;
	MinipageEntry *next_entry = NULL;
	
	Assert(entry_no >= 0 && ((uint32)entry_no) < minipageInfo->numMinipageEntries);
	
	fsInfo = blockDirectory->currentSegmentFileInfo;
	Assert(fsInfo != NULL);

	entry = &(minipageInfo->minipage->entry[entry_no]);
	if (((uint32)entry_no) < minipageInfo->numMinipageEntries - 1)
	{
		next_entry = &(minipageInfo->minipage->entry[entry_no + 1]);
	}
	
	directoryEntry->range.fileOffset = entry->fileOffset;
	directoryEntry->range.firstRowNum = entry->firstRowNum;
	if (next_entry != NULL)
	{
		directoryEntry->range.afterFileOffset = next_entry->fileOffset;
	}
	else
	{
		directoryEntry->range.afterFileOffset = fsInfo->eof;
	}

	directoryEntry->range.lastRowNum = entry->firstRowNum + entry->rowCount - 1;
	if (next_entry == NULL && gp_blockdirectory_entry_min_range != 0)
	{
		directoryEntry->range.lastRowNum = (~(((int64)1) << 63)); /* set to the maximal value */
	}

	/*
	 * When crashes during inserts, or cancellation during inserts,
	 * the block directory may contain out-of-date entries. We
	 * check for the end of file here. If the requested directory entry
	 * is after the end of file, return false.
	 */
	if (directoryEntry->range.fileOffset > fsInfo->eof)
		return false;

	if ((directoryEntry->range.afterFileOffset > fsInfo->eof))
	{
		directoryEntry->range.afterFileOffset = fsInfo->eof;
	}
	
	if (Debug_appendonly_print_blockdirectory)
		ereport(LOG,
				(errmsg("Append-only block directory find entry: "
						"(columnGroupNo, firstRowNum, fileOffset, lastRowNum, afterFileOffset) = "
						"(%d, " INT64_FORMAT ", " INT64_FORMAT ", " INT64_FORMAT ", " INT64_FORMAT ")",
						columnGroupNo, directoryEntry->range.firstRowNum,
						directoryEntry->range.fileOffset, directoryEntry->range.lastRowNum,
						directoryEntry->range.afterFileOffset)));

	return true;
}

/*
 * AppendOnlyBlockDirectory_GetEntry
 *
 * Find a directory entry for the given AOTupleId in the block directory.
 * If such an entry is found, return true. Otherwise, return false.
 *
 * The range for directoryEntry is assigned accordingly in this function.
 *
 * The block directory for the appendonly table should exist before calling
 * this function.
 */
bool
AppendOnlyBlockDirectory_GetEntry(
	AppendOnlyBlockDirectory		*blockDirectory,
	AOTupleId 						*aoTupleId,
	int                             columnGroupNo,
	AppendOnlyBlockDirectoryEntry	*directoryEntry)
{
	int segmentFileNum = AOTupleIdGet_segmentFileNum(aoTupleId);
	int64 rowNum = AOTupleIdGet_rowNum(aoTupleId);
	int		i;
	Relation blkdirRel = blockDirectory->blkdirRel;
	Relation blkdirIdx = blockDirectory->blkdirIdx;
	int numScanKeys = blockDirectory->numScanKeys;
	ScanKey scanKeys = blockDirectory->scanKeys;
	
	TupleDesc heapTupleDesc;
	TupleDesc idxTupleDesc;
	FileSegInfo *fsInfo = NULL;
	IndexScanDesc idxScanDesc;
	HeapTuple tuple = NULL;
	MinipagePerColumnGroup *minipageInfo =
		&blockDirectory->minipages[columnGroupNo];
	int entry_no = -1;
	int tmpGroupNo;
	
	if (blkdirRel == NULL || blkdirIdx == NULL)
	{
		Assert(RelationIsValid(blockDirectory->aoRel));
		
		ereport(ERROR,
				(errcode(ERRCODE_GP_INTERNAL_ERROR),
				 errmsg("Block directory for append-only relation '%s' does not exist",
						RelationGetRelationName(blockDirectory->aoRel))));
		return false;
	}

	if (Debug_appendonly_print_blockdirectory)
		ereport(LOG,
				(errmsg("Append-only block directory get entry: "
						"(columnGroupNo, segmentFileNum, rowNum) = "
						"(%d, %d, " INT64_FORMAT ")",
						columnGroupNo, segmentFileNum, rowNum)));

	/*
	 * If the segment file number is the same as
	 * blockDirectory->currentSegmentFileNum, the in-memory minipage
	 * may contain such an entry. We search the in-memory minipage
	 * first. If such an entry can not be found, we search for the
	 * appropriate minipage by using the block directory btree index.
	 */
	if (segmentFileNum == blockDirectory->currentSegmentFileNum &&
		minipageInfo->numMinipageEntries > 0)
	{
		Assert(blockDirectory->currentSegmentFileInfo != NULL);
		
		/*
		 * Check if the existing minipage contains the requested
		 * rowNum. If so, just get it.
		 */
		entry_no = find_minipage_entry(minipageInfo->minipage,
									   minipageInfo->numMinipageEntries,
									   rowNum);
		if (entry_no != -1)
		{
			return set_directoryentry_range(blockDirectory,
									 columnGroupNo,
									 entry_no,
									 directoryEntry);

		}

		/*
		 * The given rowNum may point to a tuple that does not exist
		 * in the AO table any more, either because of cancellation of
		 * an insert, or due to crashes during an insert. If this is
		 * the case, rowNum is smaller than the highest entry in
		 * the in-memory minipage entry.
		 */
		else
		{
			MinipageEntry *entry =
				&minipageInfo->minipage->entry[minipageInfo->numMinipageEntries - 1];
			
			if (rowNum < entry->firstRowNum + entry->rowCount - 1)
				return false;
		}
	}

	for (i = 0; i < blockDirectory->totalSegfiles; i++)
	{
		fsInfo = blockDirectory->segmentFileInfo[i];
		
		if (segmentFileNum == fsInfo->segno)
			break;
	}

	Assert(fsInfo != NULL);

	/*
	 * Search the btree index to find the minipage that contains
	 * the rowNum. We find the minipages for all column groups, since
	 * currently we will need to access all columns at the same time.
	 */
	heapTupleDesc = RelationGetDescr(blkdirRel);
	idxTupleDesc = RelationGetDescr(blkdirIdx);

	Assert(numScanKeys == 3);

	for (tmpGroupNo = 0; tmpGroupNo < blockDirectory->numColumnGroups; tmpGroupNo++)
	{
		/* Setup the scan keys for the scan. */
		Assert(scanKeys != NULL);
		scanKeys[0].sk_argument = Int32GetDatum(segmentFileNum);
		scanKeys[1].sk_argument = Int32GetDatum(tmpGroupNo);
		scanKeys[2].sk_argument = Int64GetDatum(rowNum);
		
		idxScanDesc = index_beginscan(blkdirRel, blkdirIdx,
									  blockDirectory->appendOnlyMetaDataSnapshot,
									  numScanKeys, scanKeys);
	
		tuple = index_getnext(idxScanDesc, BackwardScanDirection);

		if (tuple != NULL)
		{
			/*
			 * MPP-17061: we need to update currentSegmentFileNum
			 * & currentSegmentFileInfo at the same time when we 
			 * load the minipage for the block directory entry we
			 * found, otherwise we would risk having inconsistency
			 * between currentSegmentFileNum/currentSegmentFileInfo
			 * and minipage contents, which would cause wrong block
			 * header offset being returned in following block 
			 * directory entry look up.
			 */
			blockDirectory->currentSegmentFileNum = segmentFileNum;
			blockDirectory->currentSegmentFileInfo = fsInfo;
			MinipagePerColumnGroup *minipageInfo;
			
			minipageInfo = &blockDirectory->minipages[tmpGroupNo];
			extract_minipage(blockDirectory,
							 tuple,
							 heapTupleDesc,
							 tmpGroupNo);
		}
		else
		{
			/* MPP-17061: index look up failed, row is invisible */
			index_endscan(idxScanDesc);
			return false;
		}	

		index_endscan(idxScanDesc);
	}
	
	{
		MinipagePerColumnGroup *minipageInfo;
		minipageInfo = &blockDirectory->minipages[columnGroupNo];

		/*
		 * Perform a binary search over the minipage to find
		 * the entry about the AO block.
		 */
		entry_no = find_minipage_entry(minipageInfo->minipage,
									   minipageInfo->numMinipageEntries,
									   rowNum);

		/* If there are no entries, return false. */
		if (entry_no == -1 && minipageInfo->numMinipageEntries == 0)
			return false;

		if (entry_no == -1)
		{
			/*
			 * Since the last few blocks may not be logged in the block
			 * directory, we always use the last entry.
			 */
			entry_no = minipageInfo->numMinipageEntries - 1;
		}
		return set_directoryentry_range(blockDirectory,
										columnGroupNo,
										entry_no,
										directoryEntry);
	}
	
	return false;
}

/*
 * AppendOnlyBlockDirectory_InsertEntry
 *
 * Insert an entry to the block directory. This entry is appended to the 
 * in-memory minipage. If the minipage is full, it is written to the block
 * directory relation on disk. After that, the new entry is added to the
 * new in-memory minipage.
 *
 * To reduce the size of a block directory, this function ignores new entries
 * when the range between the offset value of the latest existing entry and
 * the offset of the new entry is smaller than gp_blockdirectory_entry_min_range
 * (if it is set). Otherwise, the latest existing entry is updated with new
 * rowCount value, and the given new entry is appended to the in-memory minipage.
 *
 * If the block directory for the appendonly relation does not exist,
 * this function simply returns.
 *
 * If rowCount is 0, simple return false.
 */
bool
AppendOnlyBlockDirectory_InsertEntry(
	AppendOnlyBlockDirectory *blockDirectory,
	int columnGroupNo,
	int64 firstRowNum,
	int64 fileOffset,
	int64 rowCount)
{
	MinipageEntry *entry = NULL;
	MinipagePerColumnGroup *minipageInfo =
		&blockDirectory->minipages[columnGroupNo];
	int lastEntryNo;

	if (rowCount == 0)
		return false;
	
	if (blockDirectory->blkdirRel == NULL ||
		blockDirectory->blkdirIdx == NULL)
		return false;

	Assert(minipageInfo->numMinipageEntries <= (uint32)NUM_MINIPAGE_ENTRIES);

	lastEntryNo = minipageInfo->numMinipageEntries - 1;
	if (lastEntryNo >= 0)
	{
		entry = &(minipageInfo->minipage->entry[lastEntryNo]);

		Assert(entry->firstRowNum < firstRowNum);
		Assert(entry->fileOffset < fileOffset);
		
		if (gp_blockdirectory_entry_min_range > 0 &&
			fileOffset - entry->fileOffset < gp_blockdirectory_entry_min_range)
			return true;
		
		/* Update the rowCount in the latest entry */
		Assert(entry->rowCount <= firstRowNum - entry->firstRowNum);

		if (Debug_appendonly_print_blockdirectory)
			ereport(LOG,
					(errmsg("Append-only block directory update entry: "
							"(firstRowNum, columnGroupNo, fileOffset, rowCount) = (" INT64_FORMAT
							", %d, " INT64_FORMAT ", " INT64_FORMAT ") at index %d to "
							"(firstRowNum, columnGroupNo, fileOffset, rowCount) = (" INT64_FORMAT
							", %d, " INT64_FORMAT ", " INT64_FORMAT ")",
							entry->firstRowNum, columnGroupNo, entry->fileOffset, entry->rowCount,
							minipageInfo->numMinipageEntries - 1,
							entry->firstRowNum, columnGroupNo, entry->fileOffset,
							firstRowNum - entry->firstRowNum)));
		
		entry->rowCount = firstRowNum - entry->firstRowNum;
	}
	
	if (minipageInfo->numMinipageEntries >= (uint32)gp_blockdirectory_minipage_size)
	{
		write_minipage(blockDirectory, columnGroupNo);

		/* Set tupleTid to invalid */
		ItemPointerSetInvalid(&minipageInfo->tupleTid);

		/*
		 * Clear out the entries.
		 */
		MemSet(minipageInfo->minipage->entry, 0,
			   minipageInfo->numMinipageEntries * sizeof(MinipageEntry));
		minipageInfo->numMinipageEntries = 0;
	}
	
	Assert(minipageInfo->numMinipageEntries < (uint32)gp_blockdirectory_minipage_size);

	entry = &(minipageInfo->minipage->entry[minipageInfo->numMinipageEntries]);
	entry->firstRowNum = firstRowNum;
	entry->fileOffset = fileOffset;
	entry->rowCount = rowCount;
	
	minipageInfo->numMinipageEntries++;
	
	if(Debug_appendonly_print_blockdirectory)
	{
		ereport(LOG,
				(errmsg("Append-only block directory insert entry: "
						"(firstRowNum, columnGroupNo, fileOffset, rowCount) = (" INT64_FORMAT
						", %d, " INT64_FORMAT ", " INT64_FORMAT ") at index %d",
						entry->firstRowNum, columnGroupNo, entry->fileOffset, entry->rowCount,
						minipageInfo->numMinipageEntries - 1)));
	}

	return true;
}

/*
 * init_scankeys
 *
 * Initialize the scan keys.
 */
static void
init_scankeys(TupleDesc tupleDesc,
			  int nkeys, ScanKey scanKeys,
			  StrategyNumber *strategyNumbers)
{
	int keyNo;

	Assert(nkeys <= tupleDesc->natts);
	
	for (keyNo = 0; keyNo < nkeys; keyNo ++)
	{
		ScanKey scanKey = (ScanKey)(((char *)scanKeys) + 
									keyNo * sizeof(ScanKeyData));
		RegProcedure opfuncid;
		StrategyNumber strategyNumber = strategyNumbers[keyNo];

		Assert(strategyNumber <= BTMaxStrategyNumber &&
			   strategyNumber != InvalidStrategy);
		
		if (strategyNumber == BTEqualStrategyNumber)
		{
			opfuncid = equality_oper_funcid(tupleDesc->attrs[keyNo]->atttypid);
			ScanKeyEntryInitialize(scanKey,
								   0, /* sk_flag */
								   keyNo + 1, /* attribute number to scan */
								   BTEqualStrategyNumber, /* strategy */
								   InvalidOid, /* strategy subtype */
								   opfuncid, /* reg proc to use */
								   0 /* constant */
				);
		}
		else
		{
			Oid gtOid, leOid;
			
			gtOid = reverse_ordering_oper_opid(tupleDesc->attrs[keyNo]->atttypid);
			leOid = get_negator(gtOid);
			opfuncid = get_opcode(leOid);
			
			ScanKeyEntryInitialize(scanKey,
								   0, /* sk_flag */
								   keyNo + 1, /* attribute number to scan */
								   strategyNumber, /* strategy */
								   InvalidOid, /* strategy subtype */
								   opfuncid, /* reg proc to use */
								   0 /* constant */
				);
		}
	}
}

/*
 * copy_out_minipage
 *
 * Copy out the minipage content from a deformed tuple.
 */
static inline void
copy_out_minipage(MinipagePerColumnGroup *minipageInfo,
				  Datum minipage_value,
				  bool minipage_isnull)
{
	struct varlena *value;
	struct varlena *detoast_value;

	Assert(!minipage_isnull);

	value = (struct varlena *)
		DatumGetPointer(minipage_value);
	detoast_value = pg_detoast_datum(value);
	Assert( VARSIZE(detoast_value) <= minipage_size(NUM_MINIPAGE_ENTRIES));

	memcpy(minipageInfo->minipage, detoast_value, VARSIZE(detoast_value));
	if (detoast_value != value)
		pfree(detoast_value);
	
	Assert(minipageInfo->minipage->nEntry <= NUM_MINIPAGE_ENTRIES);
	
	minipageInfo->numMinipageEntries = minipageInfo->minipage->nEntry;
}


/*
 * extract_minipage
 *
 * Extract the minipage info from the given tuple. The tupleTid
 * is also set here.
 */
static void
extract_minipage(AppendOnlyBlockDirectory *blockDirectory,
				 HeapTuple tuple,
				 TupleDesc tupleDesc,
				 int columnGroupNo)
{
	Datum *values = blockDirectory->values;
	bool *nulls = blockDirectory->nulls;
	MinipagePerColumnGroup *minipageInfo =
		&blockDirectory->minipages[columnGroupNo];
	FileSegInfo *fsInfo = blockDirectory->currentSegmentFileInfo;
	int64 eof;
	int start, end, mid=0;
	bool found = false;
	
	heap_deform_tuple(tuple, tupleDesc, values, nulls);

	Assert(blockDirectory->currentSegmentFileNum ==
		   DatumGetInt32(values[Anum_pg_aoblkdir_segno - 1]));

	/*
	 * Copy out the minipage
	 */
	copy_out_minipage(minipageInfo,
					  values[Anum_pg_aoblkdir_minipage - 1],
					  nulls[Anum_pg_aoblkdir_minipage - 1]);

	ItemPointerCopy(&tuple->t_self, &minipageInfo->tupleTid);
	
	/*
	 * When crashes during inserts, or cancellation during inserts,
	 * there are out-of-date minipage entries in the block directory.
	 * We reset those entries here.
	 */
	Assert(fsInfo != NULL);
	eof = fsInfo->eof;

	start = 0;
	end = minipageInfo->numMinipageEntries - 1;
	while (start <= end)
	{
		mid = (end - start + 1) / 2 + start;
		if (minipageInfo->minipage->entry[mid].fileOffset > eof)
			end = mid - 1;
		else if (minipageInfo->minipage->entry[mid].fileOffset < eof)
			start = mid + 1;
		else
		{
			found = true;
			break;
		}
	}

	minipageInfo->numMinipageEntries = 0;
	if (found)
		minipageInfo->numMinipageEntries = mid;
	else if (start > 0)
	{
		minipageInfo->numMinipageEntries = start;
		Assert(minipageInfo->minipage->entry[start - 1].fileOffset < eof);
	}
}

/*
 * load_last_minipage
 *
 * Search through the block directory btree to find the last row that
 * contains the last minipage.
 */
static void
load_last_minipage(AppendOnlyBlockDirectory *blockDirectory,
				   int64 lastSequence,
				   int columnGroupNo)
{
	Relation blkdirRel = blockDirectory->blkdirRel;
	Relation blkdirIdx = blockDirectory->blkdirIdx;
	TupleDesc idxTupleDesc;
	TupleDesc heapTupleDesc;
	IndexScanDesc idxScanDesc;
	HeapTuple tuple = NULL;
	MemoryContext oldcxt;
	int numScanKeys = blockDirectory->numScanKeys;
	ScanKey scanKeys = blockDirectory->scanKeys;
	
#ifdef USE_ASSERT_CHECKING
	StrategyNumber *strategyNumbers = blockDirectory->strategyNumbers;
#endif /* USE_ASSERT_CHECKING */
	
	Assert(blockDirectory->aoRel != NULL);
	Assert(blockDirectory->blkdirRel != NULL);
	Assert(blockDirectory->blkdirIdx != NULL);

	oldcxt = MemoryContextSwitchTo(blockDirectory->memoryContext);
	
	heapTupleDesc = RelationGetDescr(blkdirRel);
	idxTupleDesc = RelationGetDescr(blkdirIdx);

	Assert(numScanKeys == 3);
	Assert(blockDirectory->currentSegmentFileInfo != NULL);

	/* Setup the scan keys for the scan. */
	Assert(scanKeys != NULL);
	Assert(strategyNumbers != NULL);
	if (lastSequence == 0)
		lastSequence = 1;
	
	scanKeys[0].sk_argument =
		Int32GetDatum(blockDirectory->currentSegmentFileNum);
	scanKeys[1].sk_argument = Int32GetDatum(columnGroupNo);
	scanKeys[2].sk_argument = Int64GetDatum(lastSequence);

	/*
	 * Search the btree to find the entry in the block directory
	 * that contains the last minipage.
	 */
	idxScanDesc = index_beginscan(blkdirRel, blkdirIdx,
								  blockDirectory->appendOnlyMetaDataSnapshot,
								  numScanKeys, scanKeys);
	
	tuple = index_getnext(idxScanDesc, BackwardScanDirection);
	if (tuple != NULL)
	{
		extract_minipage(blockDirectory,
						 tuple,
						 heapTupleDesc,
						 columnGroupNo);
	}
	
	index_endscan(idxScanDesc);

	MemoryContextSwitchTo(oldcxt);

	if (Debug_appendonly_print_blockdirectory)
		ereport(LOG,
				(errmsg("Append-only block directory load last minipage: "
						"(columnGroupNo, lastSequence, nEntries) = (%d, " INT64_FORMAT ", %u)",
						columnGroupNo, lastSequence,
						blockDirectory->minipages[columnGroupNo].numMinipageEntries)));
	
}

/*
 * find_minipage_entry
 *
 * Find the minipage entry that covers the given rowNum.
 * If such an entry does not exists, -1 is returned. Otherwise
 * the index to such an entry in the minipage array is returned.
 */
static int
find_minipage_entry(Minipage *minipage,
					uint32 numEntries,
					int64 rowNum)
{
	int start_no, end_no;
	int entry_no;
	MinipageEntry *entry;
	
	start_no = 0;
	end_no = numEntries - 1;
	while (start_no <= end_no)
	{
		entry_no = start_no + (end_no - start_no + 1) / 2;
		Assert(entry_no >= start_no && entry_no <= end_no);
		
		entry = &(minipage->entry[entry_no]);
		
		Assert(entry->firstRowNum > 0);
		Assert(entry->rowCount > 0);
		
		if (entry->firstRowNum <= rowNum &&
			entry->firstRowNum + entry->rowCount > rowNum)
			break;
		else if (entry->firstRowNum > rowNum)
		{
			end_no = entry_no - 1;
		}
		else
		{
			start_no = entry_no + 1;
		}
	}
	
	if (start_no <= end_no)
		return entry_no;
	else
		return -1;
}

/*
 * write_minipage
 *
 * Write the in-memory minipage to the block directory relation.
 */
static void
write_minipage(AppendOnlyBlockDirectory *blockDirectory,
			   int columnGroupNo)
{
	HeapTuple tuple;
	MemoryContext oldcxt;
	Datum *values = blockDirectory->values;
	bool *nulls = blockDirectory->nulls;
	Relation blkdirRel = blockDirectory->blkdirRel;
	TupleDesc heapTupleDesc = RelationGetDescr(blkdirRel);
	MinipagePerColumnGroup *minipageInfo =
		&blockDirectory->minipages[columnGroupNo];
	
	Assert(minipageInfo->numMinipageEntries > 0);

	oldcxt = MemoryContextSwitchTo(blockDirectory->memoryContext);
	
	Assert(blkdirRel != NULL);
	
	values[Anum_pg_aoblkdir_segno - 1] =
		Int32GetDatum(blockDirectory->currentSegmentFileNum);
	nulls[Anum_pg_aoblkdir_segno - 1] = false;

	values[Anum_pg_aoblkdir_columngroupno - 1] =
		Int32GetDatum(columnGroupNo);
	nulls[Anum_pg_aoblkdir_columngroupno - 1] = false;

	values[Anum_pg_aoblkdir_firstrownum - 1] =
		Int64GetDatum(minipageInfo->minipage->entry[0].firstRowNum);
	nulls[Anum_pg_aoblkdir_firstrownum - 1] = false;

	SET_VARSIZE(minipageInfo->minipage,
				minipage_size(minipageInfo->numMinipageEntries));
	minipageInfo->minipage->nEntry = minipageInfo->numMinipageEntries;
	values[Anum_pg_aoblkdir_minipage - 1] =
		PointerGetDatum(minipageInfo->minipage);
	nulls[Anum_pg_aoblkdir_minipage - 1] = false;
	
	tuple = heaptuple_form_to(heapTupleDesc,
							  values,
							  nulls,
							  NULL,
							  NULL);
	
	/*
	 * Write out the minipage to the block directory relation.
	 * If this minipage is already in the relation, we update
	 * the row. Otherwise, a new row is inserted.
	 */
	if (ItemPointerIsValid(&minipageInfo->tupleTid))
	{
		if (Debug_appendonly_print_blockdirectory)
			ereport(LOG,
					(errmsg("Append-only block directory update a minipage: "
							"(segno, columnGroupNo, nEntries, firstRowNum) = "
							"(%d, %d, %u, " INT64_FORMAT ")",
							blockDirectory->currentSegmentFileNum,
							columnGroupNo, minipageInfo->numMinipageEntries,
							minipageInfo->minipage->entry[0].firstRowNum)));

		simple_heap_update(blkdirRel, &minipageInfo->tupleTid, tuple);
	}
	else
	{
		if (Debug_appendonly_print_blockdirectory)
			ereport(LOG,
					(errmsg("Append-only block directory insert a minipage: "
							"(segno, columnGroupNo, nEntries, firstRowNum) = "
							"(%d, %d, %u, " INT64_FORMAT ")",
							blockDirectory->currentSegmentFileNum,
							columnGroupNo, minipageInfo->numMinipageEntries,
							minipageInfo->minipage->entry[0].firstRowNum)));

		simple_heap_insert(blkdirRel, tuple);
	}
	
	CatalogUpdateIndexes(blkdirRel, tuple);
	
	heap_freetuple(tuple);
	
	MemoryContextSwitchTo(oldcxt);
}



void
AppendOnlyBlockDirectory_End_forInsert(
	AppendOnlyBlockDirectory *blockDirectory)
{
	int groupNo;

	if (blockDirectory->blkdirRel == NULL ||
		blockDirectory->blkdirIdx == NULL)
		return;
	
	for (groupNo = 0; groupNo < blockDirectory->numColumnGroups; groupNo++)
	{
		MinipagePerColumnGroup *minipageInfo =
			&blockDirectory->minipages[groupNo];
		
		if (minipageInfo->numMinipageEntries > 0)
		{
			write_minipage(blockDirectory, groupNo);
			if (Debug_appendonly_print_blockdirectory)
				ereport(LOG,
						(errmsg("Append-only block directory end of insert write minipage: "
								"(columnGroupNo, nEntries) = (%d, %u)",
								groupNo, minipageInfo->numMinipageEntries)));
		}
		
		pfree(minipageInfo->minipage);
	}

	if (Debug_appendonly_print_blockdirectory)
		ereport(LOG,
				(errmsg("Append-only block directory end for insert: "
						"(segno, numColumnGroups)="
						"(%d, %d)",
						blockDirectory->currentSegmentFileNum,
						blockDirectory->numColumnGroups)));

	pfree(blockDirectory->values);
	pfree(blockDirectory->nulls);
	pfree(blockDirectory->minipages);
	pfree(blockDirectory->scanKeys);
	pfree(blockDirectory->strategyNumbers);
	
	index_close(blockDirectory->blkdirIdx, RowExclusiveLock);
	heap_close(blockDirectory->blkdirRel, RowExclusiveLock);
	
	MemoryContextDelete(blockDirectory->memoryContext);
}

void
AppendOnlyBlockDirectory_End_forSearch(
	AppendOnlyBlockDirectory *blockDirectory)
{
	int groupNo;
	
	if (blockDirectory->blkdirRel == NULL ||
		blockDirectory->blkdirIdx == NULL)
		return;

	for (groupNo = 0; groupNo < blockDirectory->numColumnGroups; groupNo++)
	{
		pfree(blockDirectory->minipages[groupNo].minipage);
	}

	if (Debug_appendonly_print_blockdirectory)
		ereport(LOG,
				(errmsg("Append-only block directory end for search: "
						"(totalSegfiles, numColumnGroups )="
						"(%d, %d)",
						blockDirectory->totalSegfiles,
						blockDirectory->numColumnGroups)));

	pfree(blockDirectory->values);
	pfree(blockDirectory->nulls);
	pfree(blockDirectory->minipages);
	pfree(blockDirectory->scanKeys);
	pfree(blockDirectory->strategyNumbers);
	
	index_close(blockDirectory->blkdirIdx, AccessShareLock);
	heap_close(blockDirectory->blkdirRel, AccessShareLock);

	MemoryContextDelete(blockDirectory->memoryContext);
}
