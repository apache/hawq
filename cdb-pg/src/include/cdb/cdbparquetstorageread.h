/*
 * cdbparquetstorageread.h
 *
 *  Created on: Jul 4, 2013
 *      Author: malili
 */

#ifndef CDBPARQUETSTORAGEREAD_H_
#define CDBPARQUETSTORAGEREAD_H_

#include "cdb/cdbbufferedread.h"
#include "catalog/pg_compression.h"
#include "cdb/cdbparquetfooterprocessor.h"
#include "cdb/cdbappendonlystoragelayer.h"

/*
 * This structure contains read session information.  Consider the fields
 * inside to be private.
 */
typedef struct ParquetStorageRead {
	bool 			isActive;

	/* The memory context to use for buffers and other memory needs.*/
	MemoryContext 	memoryContext;

	/* Name of the relation to use in system logging and error messages.*/
	char 			*relationName;

	/*
	 * A phrase that better describes the purpose of the this open.
	 * The caller manages the storage for this.
	 */
	char 			*title;

	/* The Parquet Storage Attributes from relation creation.*/
	AppendOnlyStorageAttributes storageAttributes;

	/* The handle to the current open segment file.*/
	File 			file;

	/* Name of the current segment file name to use in system logging and error messages.*/
	char 			*segmentFileName;

	/* The number of row groups read since the beginning of the segment file.*/
	int 			rowGroupProcessedCount;

	/* Total number of row groups in current segment file*/
	int 			rowGroupCount;

	ParquetMetadata parquetMetadata;

} ParquetStorageRead;


/*
 * Open the next segment file to read.
 *
 * This routine is responsible for seeking to the proper
 * read location given the logical EOF.
 */
extern void ParquetStorageRead_OpenFile(
		ParquetStorageRead *storageRead,
		char *filePathName,
		int64 logicalEof,
		TupleDesc tableAttrs);

/*
 * Close the current segment file. No error if the current is already closed.
 */
extern void ParquetStorageRead_CloseFile(ParquetStorageRead *storageRead);

extern void ParquetStorageRead_Init(
		ParquetStorageRead *storageRead,
		MemoryContext memoryContext,
		char *relationName,
		char *title,
		AppendOnlyStorageAttributes *storageAttributes);

void
ParquetStorageRead_FinishSession(ParquetStorageRead *storageRead);

#endif /* CDBPARQUETSTORAGEREAD_H_ */
