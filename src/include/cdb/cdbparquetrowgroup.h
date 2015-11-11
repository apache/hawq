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
 * cdbparquetrowgroup.h
 *
 *  Created on: Sep 29, 2013
 *      Author: malili
 */

#ifndef CDBPARQUETROWGROUP_H_
#define CDBPARQUETROWGROUP_H_

#include "postgres.h"
#include "cdb/cdbparquetstorageread.h"
#include "cdb/cdbparquetcolumn.h"
#include "access/filesplit.h"
#include "access/htup.h"
#include "executor/tuptable.h"

typedef struct ParquetRowGroupReader
{
	MemoryContext		memoryContext;
	ParquetStorageRead	*storageRead;
	int 				rowCount;
	int					rowRead;
	ParquetColumnReader	*columnReaders;
	int					columnReaderCount;
	/* synthetic system attributes */
	ItemPointerData 	cdb_fake_ctid;
} ParquetRowGroupReader;

/* read row group initialization*/
void
ParquetRowGroupReader_Init(
	ParquetRowGroupReader	*rowGroupReader,
	Relation 				relation,
	ParquetStorageRead		*storageRead);

/* Get row group metadata*/
bool ParquetRowGroupReader_GetRowGroupInfo(
    FileSplit split, ParquetStorageRead *storageRead,
    ParquetRowGroupReader *rowGroupReader, bool *projs, TupleDesc hawqTupleDesc,
    int *hawqAttrToParquetColChunks, bool toCloseFile);

/* Get contents of row group*/
void
ParquetRowGroupReader_GetContents(
	ParquetRowGroupReader	*rowGroupReader);

/* Get next tuple of current row group*/
bool
ParquetRowGroupReader_ScanNextTuple(
	TupleDesc 				pqs_tupDesc,
	ParquetRowGroupReader 	*rowGroupReader,
	int						*hawqAttrToParquetColNum,
	bool 					*projs,
	TupleTableSlot 			*slot);

/* Finish scanning current row group*/
void
ParquetRowGroupReader_FinishedScanRowGroup(
	ParquetRowGroupReader	*rowGroupReader);


#endif /* CDBPARQUETROWGROUP_H_ */
