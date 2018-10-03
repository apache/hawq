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
#include "parquet_reader.h"

#include "executor/executor.h"
#include "tuplebatch.h"
#include "vcheck.h"

extern bool getNextRowGroup(ParquetScanDesc scan);
static int
ParquetRowGroupReader_ScanNextTupleBatch(
		TupleDesc 				tupDesc,
		ParquetRowGroupReader	*rowGroupReader,
		int						*hawqAttrToParquetColNum,
		bool 					*projs,
		TupleTableSlot 			*slot);

static void
parquet_vgetnext(ParquetScanDesc scan, ScanDirection direction, TupleTableSlot *slot);

TupleTableSlot *
ParquetVScanNext(ScanState *scanState)
{
	Assert(IsA(scanState, TableScanState) || IsA(scanState, DynamicTableScanState));
	ParquetScanState *node = (ParquetScanState *)scanState;
	Assert(node->opaque != NULL && node->opaque->scandesc != NULL);

	parquet_vgetnext(node->opaque->scandesc, node->ss.ps.state->es_direction, node->ss.ss_ScanTupleSlot);
	return node->ss.ss_ScanTupleSlot;
}

static void
parquet_vgetnext(ParquetScanDesc scan, ScanDirection direction, TupleTableSlot *slot)
{

	//AOTupleId aoTupleId;
	Assert(ScanDirectionIsForward(direction));

	for(;;)
	{
		if(scan->bufferDone)
		{
			/*
			 * Get the next row group. We call this function until we
			 * successfully get a block to process, or finished reading
			 * all the data (all 'segment' files) for this relation.
			 */
			while(!getNextRowGroup(scan))
			{
				/* have we read all this relation's data. done! */
				if(scan->pqs_done_all_splits)
				{
					ExecClearTuple(slot);
					return /*NULL*/;
				}
			}
			scan->bufferDone = false;
		}

		int row_num  = ParquetRowGroupReader_ScanNextTupleBatch(
								scan->pqs_tupDesc,
								&scan->rowGroupReader,
								scan->hawqAttrToParquetColChunks,
								scan->proj,
								slot);
		if(row_num > 0)
			return;

		/* no more items in the row group, get new buffer */
		scan->bufferDone = true;
	}
}

/*
 * Get next tuple batch from current row group into slot.
 *
 * Return the number of tuples fetch out.
 */
static int
ParquetRowGroupReader_ScanNextTupleBatch(
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
		return 0;
	}

	/*
	 * get the next item (tuple) from the row group
	 */
	int ncol = slot->tts_tupleDescriptor->natts;
	TupleBatch tb = (TupleBatch )slot->PRIVATE_tb;

	tb->nrows = 0;
	if (rowGroupReader->rowRead + tb->batchsize > rowGroupReader->rowCount) {
		tb->nrows = rowGroupReader->rowCount-rowGroupReader->rowRead;
		rowGroupReader->rowRead = rowGroupReader->rowCount;
	}
	else {
		tb->nrows = tb->batchsize ;
		rowGroupReader->rowRead += tb->batchsize;
	}

	int colReaderIndex = 0;
	for(int i = 0; i < tb->ncols ; i++)
	{
		if(projs[i] == false)
			continue;

		Oid hawqTypeID = tupDesc->attrs[i]->atttypid;
		if(!tb->datagroup[i])
			tbCreateColumn(tb,i,hawqTypeID);


		vtype* vt = tb->datagroup[i];
		vt->dim = tb->nrows;


		ParquetColumnReader *nextReader =
			&rowGroupReader->columnReaders[colReaderIndex];

		for(int j = 0;j < tb->nrows; j++)
		{

			if(hawqAttrToParquetColNum[i] == 1)
			{
				ParquetColumnReader_readValue(nextReader, vt->values + j , vt->isnull + j, hawqTypeID);
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
						ParquetColumnReader_readPoint(nextReader, vt->values + j, vt->isnull + j);
						break;
					case HAWQ_TYPE_PATH:
						ParquetColumnReader_readPATH(nextReader, vt->values + j, vt->isnull + j);
						break;
					case HAWQ_TYPE_LSEG:
						ParquetColumnReader_readLSEG(nextReader, vt->values + j, vt->isnull + j);
						break;
					case HAWQ_TYPE_BOX:
						ParquetColumnReader_readBOX(nextReader, vt->values + j, vt->isnull + j);
						break;
					case HAWQ_TYPE_CIRCLE:
						ParquetColumnReader_readCIRCLE(nextReader, vt->values + j, vt->isnull + j);
						break;
					case HAWQ_TYPE_POLYGON:
						ParquetColumnReader_readPOLYGON(nextReader, vt->values + j, vt->isnull + j);
						break;
					default:
						Insist(false);
						break;
				}

				MemoryContextSwitchTo(oldContext);
			}
		}

		colReaderIndex += hawqAttrToParquetColNum[i];
	}

	/*construct tuple, and return back*/
	TupSetVirtualTupleNValid(slot, ncol);
	return tb->nrows;
}
