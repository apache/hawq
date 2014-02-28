/*
 * cdbparquetrowgroup.c
 *
 *  Created on: Sep 29, 2013
 *      Author: malili
 */

#include "cdb/cdbparquetrowgroup.h"


/*
 * Initialize the ExecutorReadGroup once.  Assumed to be zeroed out before the call.
 */
void ParquetExecutorReadRowGroup_Init(
	ParquetExecutorReadRowGroup		*executorReadRowGroup,
	Relation						relation,
	MemoryContext					memoryContext,
	ParquetStorageRead				*storageRead)
{
	MemoryContext	oldcontext;

	oldcontext = MemoryContextSwitchTo(memoryContext);

	ItemPointerSet(&executorReadRowGroup->cdb_fake_ctid, 0, 0);

	executorReadRowGroup->storageRead = storageRead;

	executorReadRowGroup->memoryContext = memoryContext;

	MemoryContextSwitchTo(oldcontext);

}

/**
 * Get the information of row group, including column chunk information
 */
bool ParquetExecutorReadRowGroup_GetRowGroupInfo(
	ParquetStorageRead			*storageRead,
	ParquetExecutorReadRowGroup		*executorReadRowGroup,
	bool *projs,
	TupleDesc hawqTupleDesc,
	int *hawqAttrToParquetColChunks)
{
	ParquetMetadata parquetMetadata;
	int rowGroupIndex;
	struct BlockMetadata_4C* rowGroupMetadata;
	int hawqTableAttNum = hawqTupleDesc->natts;
	int colReaderNum = 0;
	MemoryContext	oldcontext;

	parquetMetadata = storageRead->parquetMetadata;
	rowGroupIndex = storageRead->rowGroupProcessedCount;

	/*if row group processed exceeds the number of total row groups,
	 *there're no row groups available*/
	if(rowGroupIndex >= storageRead->rowGroupCount){
		return false;
	}

	/*assign the row group metadata information to executorReadRowGroup*/
	rowGroupMetadata = parquetMetadata->pBlockMD[rowGroupIndex];

	/*initialize parquet column reader: get column reader numbers*/
	for(int i = 0; i < hawqTableAttNum; i++){
		if(projs[i] == false){
			continue;
		}
		colReaderNum += hawqAttrToParquetColChunks[i];
	}

	oldcontext = MemoryContextSwitchTo(storageRead->memoryContext);
	/*if not first row group, but column reader count not equals to previous row group,
	 *report error*/
	if(executorReadRowGroup->columnReaderCount == 0){
		executorReadRowGroup->columnReaders = (ParquetColumnReader *)palloc0
				(colReaderNum * sizeof(ParquetColumnReader));
		executorReadRowGroup->columnReaderCount = colReaderNum;
	}
	else if(executorReadRowGroup->columnReaderCount != colReaderNum){
		ereport(ERROR, (errcode(ERRCODE_GP_INTERNAL_ERROR),
				errmsg("row group column information not compatible with previous"
						"row groups in file %s for relation %s",
						storageRead->segmentFileName, storageRead->relationName)));
	}
	MemoryContextSwitchTo(oldcontext);

	/*initialize parquet column readers*/
	executorReadRowGroup->rowCount = rowGroupMetadata->rowCount;
	executorReadRowGroup->rowRead = 0;

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
			executorReadRowGroup->columnReaders[hawqColIndex + j].columnMetadata =
					&(rowGroupMetadata->columns[parquetColIndex + j]);
		}
		hawqColIndex += parquetColChunkNum;
		parquetColIndex += parquetColChunkNum;
	}

	return true;
}

void ParquetExecutorReadRowGroup_GetContents(
	ParquetExecutorReadRowGroup *executorReadRowGroup,
	MemoryContext 	memoryContext)
{
	/*scan the file to get next row group data*/
	ParquetColumnReader *columnReaders = executorReadRowGroup->columnReaders;
	File file = executorReadRowGroup->storageRead->file;
	for(int i = 0; i < executorReadRowGroup->columnReaderCount; i++){
		ParquetExecutorReadColumn(&(columnReaders[i]), file, memoryContext);
	}
	executorReadRowGroup->storageRead->rowGroupProcessedCount++;

}

/*
 * Get next tuple from current row group into slot.
 *
 * Return false if current row group has no tuple left, true otherwise.
 */
bool ParquetExecutorReadRowGroup_ScanNextTuple(
	TupleDesc 			tupDesc,
	ParquetExecutorReadRowGroup		*executorReadRowGroup,
	int					*hawqAttrToParquetColNum,
	bool 				*projs,
	TupleTableSlot 		*slot)
{
	Assert(slot);

	if (executorReadRowGroup->rowRead >= executorReadRowGroup->rowCount)
	{
		ParquetExecutionReadRowGroup_FinishedScanRowGroup(executorReadRowGroup);
		return false;
	}

	/*
	 * get the next item (tuple) from the row group
	 */
	executorReadRowGroup->rowRead++;

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
			&executorReadRowGroup->columnReaders[colReaderIndex];
		int hawqTypeID = tupDesc->attrs[i]->atttypid;

		if(hawqAttrToParquetColNum[i] == 1)
		{
			ParquetColumnReader_readValue(nextReader, &values[i], &nulls[i], hawqTypeID);
		}
		else
		{
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
void ParquetExecutionReadRowGroup_FinishedScanRowGroup(
	ParquetExecutorReadRowGroup		*rowGroup)
{
	/*reset rowCount and rowRead*/
	rowGroup->rowCount = 0;
	rowGroup->rowRead = 0;

	/*memset columnreader content to zero for later use*/
	for(int i = 0; i < rowGroup->columnReaderCount; i++){
		ParquetExecutionReadColumn_FinishedScanColumn(&(rowGroup->columnReaders[i]));
	}
}
