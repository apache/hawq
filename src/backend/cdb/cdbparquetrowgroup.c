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
 * cdbparquetrowgroup.c
 *
 *  Created on: Sep 29, 2013
 *      Author: malili
 */

#include "cdb/cdbparquetrowgroup.h"
#include "cdb/cdbparquetfooterserializer.h"

static bool ParquetRowGroupReader_Select(FileSplit split,
                                         ParquetMetadata parquetMetadata,
                                         bool *rowGroupInfoProcessed);

/*
 * Initialize the ExecutorReadGroup once.  Assumed to be zeroed out before the call.
 */
void
ParquetRowGroupReader_Init(
	ParquetRowGroupReader	*rowGroupReader,
	Relation				relation,
	ParquetStorageRead		*storageRead)
{
	MemoryContext	oldcontext;

	oldcontext = MemoryContextSwitchTo(storageRead->memoryContext);

	ItemPointerSet(&rowGroupReader->cdb_fake_ctid, 0, 0);

	rowGroupReader->storageRead = storageRead;

	rowGroupReader->memoryContext = storageRead->memoryContext;

	MemoryContextSwitchTo(oldcontext);

}

/**
 * Get the information of row group, including column chunk information
 */
bool
ParquetRowGroupReader_GetRowGroupInfo(
	FileSplit               split,
	ParquetStorageRead		*storageRead,
	ParquetRowGroupReader	*rowGroupReader,
	bool 					*projs,
	TupleDesc 				hawqTupleDesc,
	int 					*hawqAttrToParquetColChunks,
	bool                    toCloseFile)
{
	ParquetMetadata parquetMetadata;
	int rowGroupIndex;
	struct BlockMetadata_4C* rowGroupMetadata;
	int hawqTableAttNum = hawqTupleDesc->natts;
	int colReaderNum = 0;
	bool rowGroupInfoProcessed = false;
	MemoryContext	oldcontext;

	parquetMetadata = storageRead->parquetMetadata;
	rowGroupIndex = storageRead->rowGroupProcessedCount;

	oldcontext = MemoryContextSwitchTo(storageRead->memoryContext);

	/*loop to get next rowgroup metadata within the split range*/
	while (rowGroupIndex < storageRead->rowGroupCount) {
		/*
		 * preRead identify whether next rowGroupInfo is already read or not
		 */
		if (!storageRead->preRead)
			readNextRowGroupInfo(parquetMetadata, storageRead->footerProtocol);
		/*
		 * should reset preRead to false to let readNextRowGroupInfo called in next loop
		 */
		storageRead->preRead = false;

		if (ParquetRowGroupReader_Select(split, parquetMetadata, &rowGroupInfoProcessed))
			break;

		/* done with current split and pre-read the next rowgroup info */
		if (rowGroupInfoProcessed) {
			storageRead->preRead = true;
			if (toCloseFile) {
				freeFooterProtocol(storageRead->footerProtocol);
				storageRead->footerProtocol = NULL;
			}
			return false;
		}

		++storageRead->rowGroupProcessedCount;
		++rowGroupIndex;
	}

	/*if row group processed exceeds the number of total row groups,
	 *there're no row groups available*/
	if(rowGroupIndex >= storageRead->rowGroupCount){
		/*end of serialize footer*/
		endDeserializerFooter(parquetMetadata, &(storageRead->footerProtocol));
		storageRead->footerProtocol = NULL;
		return false;
	}

	/*assign the row group metadata information to executorReadRowGroup*/
	rowGroupMetadata = parquetMetadata->currentBlockMD;

	/*initialize parquet column reader: get column reader numbers*/
	for(int i = 0; i < hawqTableAttNum; i++){
		if(projs[i] == false){
			continue;
		}
		colReaderNum += hawqAttrToParquetColChunks[i];
	}

	/*if not first row group, but column reader count not equals to previous row group,
	 *report error*/
	if(rowGroupReader->columnReaderCount == 0){
		rowGroupReader->columnReaders = (ParquetColumnReader *)palloc0
				(colReaderNum * sizeof(ParquetColumnReader));
		rowGroupReader->columnReaderCount = colReaderNum;
	}
	else if(rowGroupReader->columnReaderCount != colReaderNum){
		ereport(ERROR, (errcode(ERRCODE_GP_INTERNAL_ERROR),
				errmsg("row group column information not compatible with previous"
						"row groups in file %s for relation %s",
						storageRead->segmentFileName, storageRead->relationName)));
	}

	/*initialize parquet column readers*/
	rowGroupReader->rowCount = rowGroupMetadata->rowCount;
	rowGroupReader->rowRead = 0;

	/*initialize individual column reader, by passing by parquet column chunk information*/
	int hawqColIndex = 0;
	int parquetColIndex = 0;
	for(int i = 0; i < hawqTableAttNum; i++){
		/*the number of parquet column chunks for this hawq attribute*/
		int parquetColChunkNum = hawqAttrToParquetColChunks[i];

		/*if no projection, just skip this column*/
		if(projs[i] == false){
			parquetColIndex += parquetColChunkNum;
			continue;
		}

		for(int j = 0; j < parquetColChunkNum; j++){
			/*get the column chunk metadata information*/
			if (rowGroupReader->columnReaders[hawqColIndex + j].columnMetadata == NULL) {
				rowGroupReader->columnReaders[hawqColIndex + j].columnMetadata =
						palloc0(sizeof(struct ColumnChunkMetadata_4C));
			}
			memcpy(rowGroupReader->columnReaders[hawqColIndex + j].columnMetadata,
					&(rowGroupMetadata->columns[parquetColIndex + j]),
					sizeof(struct ColumnChunkMetadata_4C));
			rowGroupReader->columnReaders[hawqColIndex + j].memoryContext =
					rowGroupReader->memoryContext;
		}
		hawqColIndex += parquetColChunkNum;
		parquetColIndex += parquetColChunkNum;
	}

	MemoryContextSwitchTo(oldcontext);

	return true;
}

void
ParquetRowGroupReader_GetContents(
	ParquetRowGroupReader *rowGroupReader)
{
	/*scan the file to get next row group data*/
	ParquetColumnReader *columnReaders = rowGroupReader->columnReaders;
	File file = rowGroupReader->storageRead->file;
	for(int i = 0; i < rowGroupReader->columnReaderCount; i++){
		ParquetExecutorReadColumn(&(columnReaders[i]), file);
	}
	rowGroupReader->storageRead->rowGroupProcessedCount++;

}

/*
 * Get next tuple from current row group into slot.
 *
 * Return false if current row group has no tuple left, true otherwise.
 */
bool
ParquetRowGroupReader_ScanNextTuple(
	TupleDesc 				tupDesc,
	ParquetRowGroupReader	*rowGroupReader,
	int						*hawqAttrToParquetColNum,
	bool 					*projs,
	TupleTableSlot 			*slot)
{
	Assert(slot);

	if (rowGroupReader->rowRead >= rowGroupReader->rowCount)
	{
		ParquetRowGroupReader_FinishedScanRowGroup(rowGroupReader);
		return false;
	}

	/*
	 * get the next item (tuple) from the row group
	 */
	rowGroupReader->rowRead++;

	int natts = slot->tts_tupleDescriptor->natts;
	Assert(natts <=	tupDesc->natts);

	Datum *values = slot_get_values(slot);
	bool *nulls = slot_get_isnull(slot);

	int colReaderIndex = 0;
	for(int i = 0; i < natts; i++)
	{
		if(projs[i] == false)
		{
			nulls[i] = true;
			continue;
		}

		ParquetColumnReader *nextReader =
			&rowGroupReader->columnReaders[colReaderIndex];
		int hawqTypeID = tupDesc->attrs[i]->atttypid;

		if(hawqAttrToParquetColNum[i] == 1)
		{
			ParquetColumnReader_readValue(nextReader, &values[i], &nulls[i], hawqTypeID);
		}
		else
		{
			/*
			 * Because there are some memory reused inside the whole column reader, so need
			 * to switch the context from PerTupleContext to rowgroup->context
			 */
			MemoryContext oldContext = MemoryContextSwitchTo(rowGroupReader->memoryContext);

			switch(hawqTypeID)
			{
				case HAWQ_TYPE_POINT:
					ParquetColumnReader_readPoint(nextReader, &values[i], &nulls[i]);
					break;
				case HAWQ_TYPE_PATH:
					ParquetColumnReader_readPATH(nextReader, &values[i], &nulls[i]);
					break;
				case HAWQ_TYPE_LSEG:
					ParquetColumnReader_readLSEG(nextReader, &values[i], &nulls[i]);
					break;
				case HAWQ_TYPE_BOX:
					ParquetColumnReader_readBOX(nextReader, &values[i], &nulls[i]);
					break;
				case HAWQ_TYPE_CIRCLE:
					ParquetColumnReader_readCIRCLE(nextReader, &values[i], &nulls[i]);
					break;
				case HAWQ_TYPE_POLYGON:
					ParquetColumnReader_readPOLYGON(nextReader, &values[i], &nulls[i]);
					break;
				default:
					/* TODO array type */
					/* TODO UDT */
					Insist(false);
					break;
			}

			MemoryContextSwitchTo(oldContext);
		}

		colReaderIndex += hawqAttrToParquetColNum[i];
	}

	/*construct tuple, and return back*/
	TupSetVirtualTupleNValid(slot, natts);
	return true;
}

/**
 * finish scanning row group, but keeping the structure palloced
 */
void
ParquetRowGroupReader_FinishedScanRowGroup(
	ParquetRowGroupReader		*rowGroupReader)
{
	/*reset rowCount and rowRead*/
	rowGroupReader->rowCount = 0;
	rowGroupReader->rowRead = 0;

	/*memset columnreader content to zero for later use*/
	for(int i = 0; i < rowGroupReader->columnReaderCount; i++){
		ParquetColumnReader_FinishedScanColumn(&(rowGroupReader->columnReaders[i]));
	}
}

static bool ParquetRowGroupReader_Select(FileSplit split,
                                         ParquetMetadata parquetMetadata,
                                         bool *rowGroupInfoProcessed) {
  BlockMetadata_4C *rowGroupMetadata = parquetMetadata->currentBlockMD;

  Assert(rowGroupMetadata != NULL);

  int64 splitStart = split->offsets;
  int64 splitEnd = splitStart + split->lengths;
  int64 rowGroupStart = rowGroupMetadata->columns[0].firstDataPage;

  elog(DEBUG1, "parquetSplitSegNo/EOF: %d/"INT64_FORMAT"", split->segno, split->logiceof);
  elog(DEBUG1, "parquetSplitStart: "INT64_FORMAT"", splitStart);
  elog(DEBUG1, "parquetSplitEnd: "INT64_FORMAT"", splitEnd);
  elog(DEBUG1, "parquetSplitRowGroupStart: "INT64_FORMAT"", rowGroupStart);

  /*
   * any rowgroup start point which is located in [splitStart, splitEnd) should be selected
   */
  if (rowGroupStart >= splitStart && rowGroupStart < splitEnd) return true;

  if (rowGroupStart >= splitEnd)
    *rowGroupInfoProcessed = true;

  return false;
}
