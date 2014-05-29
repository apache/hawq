/*
 * cdbparquetrowgroup.c
 *
 *  Created on: Sep 29, 2013
 *      Author: malili
 */

#include "cdb/cdbparquetrowgroup.h"
#include "cdb/cdbparquetfooterserializer.h"

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
	ParquetStorageRead		*storageRead,
	ParquetRowGroupReader	*rowGroupReader,
	bool 					*projs,
	TupleDesc 				hawqTupleDesc,
	int 					*hawqAttrToParquetColChunks)
{
	ParquetMetadata parquetMetadata;
	int rowGroupIndex;
	struct BlockMetadata_4C* rowGroupMetadata;
	int hawqTableAttNum = hawqTupleDesc->natts;
	int colReaderNum = 0;
	MemoryContext	oldcontext;

	parquetMetadata = storageRead->parquetMetadata;
	rowGroupIndex = storageRead->rowGroupProcessedCount;

	oldcontext = MemoryContextSwitchTo(storageRead->memoryContext);

	/*if row group processed exceeds the number of total row groups,
	 *there're no row groups available*/
	if(rowGroupIndex >= storageRead->rowGroupCount){
		/*end of serialize footer*/
		endDeserializerFooter(parquetMetadata, &(storageRead->footerProtocol));
		return false;
	}

	/*get next rowgroup metadata*/
	readNextRowGroupInfo(parquetMetadata, storageRead->footerProtocol);

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
	MemoryContextSwitchTo(oldcontext);

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
			rowGroupReader->columnReaders[hawqColIndex + j].columnMetadata =
					&(rowGroupMetadata->columns[parquetColIndex + j]);
			rowGroupReader->columnReaders[hawqColIndex + j].memoryContext =
					rowGroupReader->memoryContext;
		}
		hawqColIndex += parquetColChunkNum;
		parquetColIndex += parquetColChunkNum;
	}

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
