/*
 * cdbparquetfooterprocessor.h
 *
 *  Created on: Sep 22, 2013
 *      Author: malili
 */

#ifndef CDBPARQUETFOOTERPROCESSOR_H_
#define CDBPARQUETFOOTERPROCESSOR_H_

#include "postgres.h"
#include "storage/fd.h"
#include "access/parquetmetadata_c++/MetadataInterface.h"
#include "access/tupdesc.h"

#define CURRENT_PARQUET_VERSION 1

typedef struct ParquetMetadata_4C *ParquetMetadata;

void DetectHostEndian(void);

void writeParquetHeader(File dataFile, char *filePathName, int64 *fileLen, int64 *fileLen_uncompressed);

void writeParquetFooter(File dataFile,
		char *filePathName,
		/*ParquetMetadataUtil metaUtil*/
		ParquetMetadata parquetMetadata,
		int64 *fileLen,
		int64 *fileLen_uncompressed);

bool readParquetFooter(File fileHandler,
		ParquetMetadata *parquetMetadata,
		int64 eof,
		char *filePathName);

bool checkAndSyncMetadata(ParquetMetadata parquetmd,
		TupleDesc tupdesc);

#endif /* CDBPARQUETFOOTERPROCESSOR_H_ */
