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

	/* The Parquet Storage Attributes from relation creation.*/
	AppendOnlyStorageAttributes storageAttributes;

	/* The handle to the current open segment file.*/
	File 			file;

	/* The handler for the current open segment file footer processor*/
	File			fileHandlerForFooter;

	/* Name of the current segment file name to use in system logging and error messages.*/
	char 			*segmentFileName;

	/* The number of row groups read since the beginning of the segment file.*/
	int 			rowGroupProcessedCount;

	/* Total number of row groups in current segment file*/
	int 			rowGroupCount;

	ParquetMetadata parquetMetadata;

	CompactProtocol *footerProtocol;	/*protocol for reading parquet file footer*/

} ParquetStorageRead;


/*
 * Open the next segment file to read.
 *
 * This routine is responsible for seeking to the proper
 * read location given the logical EOF.
 */
void
ParquetStorageRead_OpenFile(
		ParquetStorageRead *storageRead,
		char *filePathName,
		int64 logicalEof,
		TupleDesc tableAttrs);

/*
 * Close the current segment file. No error if the current is already closed.
 */
void
ParquetStorageRead_CloseFile(ParquetStorageRead *storageRead);

void
ParquetStorageRead_Init(
		ParquetStorageRead *storageRead,
		MemoryContext memoryContext,
		char *relationName,
		AppendOnlyStorageAttributes *storageAttributes);

void
ParquetStorageRead_FinishSession(ParquetStorageRead *storageRead);

#endif /* CDBPARQUETSTORAGEREAD_H_ */
