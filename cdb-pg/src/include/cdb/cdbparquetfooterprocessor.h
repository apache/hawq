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
#include "cdb/cdbparquetfooterserializer_protocol.h"

#define CURRENT_PARQUET_VERSION 1

typedef struct ParquetMetadata_4C *ParquetMetadata;

void DetectHostEndian(void);

void writeParquetHeader(File dataFile, char *filePathName, int64 *fileLen,
		int64 *fileLen_uncompressed);

void writeParquetFooter(File dataFile,
		char *filePathName,
		ParquetMetadata parquetMetadata,
		int64 *fileLen,
		int64 *fileLen_uncompressed,
		CompactProtocol **footer_read_protocol,
		CompactProtocol **footer_write_protocol,
		int	previous_rowgroup_count);

bool readParquetFooter(File fileHandler, ParquetMetadata *parquetMetadata,
		CompactProtocol **footerProtocol, int64 eof, char *filePathName);

bool checkAndSyncMetadata(ParquetMetadata parquetmd, TupleDesc tupdesc);

#endif /* CDBPARQUETFOOTERPROCESSOR_H_ */
