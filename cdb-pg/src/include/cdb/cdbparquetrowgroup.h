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
#include "access/htup.h"
#include "executor/tuptable.h"

typedef struct ParquetExecutorReadRowGroup
{
	MemoryContext	memoryContext;

	ParquetStorageRead	*storageRead;

	int 			rowCount;
	int				rowRead;

	ParquetColumnReader	*columnReaders;
	int				columnReaderCount;

	/* synthetic system attributes */
	ItemPointerData cdb_fake_ctid;
} ParquetExecutorReadRowGroup;


/* read row group initialization*/
void ParquetExecutorReadRowGroup_Init(
		ParquetExecutorReadRowGroup *executorReadRowGroup,
		Relation relation,
		MemoryContext memoryContext,
		ParquetStorageRead *storageRead);

/* Get row group metadata*/
bool ParquetExecutorReadRowGroup_GetRowGroupInfo(
	ParquetStorageRead			*storageRead,
	ParquetExecutorReadRowGroup		*executorReadRowGroup,
	bool *projs,
	TupleDesc hawqTupleDesc,
	int *hawqAttrToParquetColChunks);

/* Get contents of row group*/
void ParquetExecutorReadRowGroup_GetContents(
		ParquetExecutorReadRowGroup *executorReadRowGroup,
		MemoryContext 	memoryContext);

/* Get next tuple of current row group*/
bool ParquetExecutorReadRowGroup_ScanNextTuple(
	TupleDesc 			pqs_tupDesc,
	ParquetExecutorReadRowGroup *executorReadRowGroup,
	int					*hawqAttrToParquetColNum,
	bool *projs,
	TupleTableSlot *slot);

/* Finish scanning current row group*/
void ParquetExecutionReadRowGroup_FinishedScanRowGroup(ParquetExecutorReadRowGroup *executorReadRowGroup);



#endif /* CDBPARQUETROWGROUP_H_ */
