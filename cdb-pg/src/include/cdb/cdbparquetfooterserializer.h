/*
 * cdbparquetfooterserializer.h
 *
 *  Created on: Mar 17, 2014
 *      Author: malili
 */

#ifndef CDBPARQUETFOOTERSERIALIZER_H_
#define CDBPARQUETFOOTERSERIALIZER_H_
#include "postgres.h"
#include "storage/fd.h"
#include "cdb/cdbparquetfooterprocessor.h"

/**
 * Deserialize part functions: read from parquet file
 */

/* Initialize deserialize footer from file, read one rowgroup information once */
void
initDeserializeFooter(
		File file,
		int64 footerLength,
		char *fileName,
		ParquetMetadata *parquetMetadata,
		CompactProtocol **footerProtocol);

/* Get next row group metadata information */
void
readNextRowGroupInfo(
		ParquetMetadata parquetMetadata,
		CompactProtocol *prot);

/* End of deserialize*/
void
endDeserializerFooter(
		ParquetMetadata parquetMetadata,
		CompactProtocol **prot);


/**
 * Deserialize part functions: write to parquet file
 */
void
initSerializeFooter(
		CompactProtocol **footerProtocol,
		char *fileName);

int
endSerializeFooter(CompactProtocol **read_prot,
		CompactProtocol **write_prot,
		char	*fileName,
		File	file,
		ParquetMetadata parquetMetadata,
		int		rowgroup_cnt);

int
writeRowGroupInfo(
		struct BlockMetadata_4C* rowGroupInfo,
		CompactProtocol *prot);

/**
 * Free metadata part functions
 */

void
freeRowGroupInfo(
		struct BlockMetadata_4C *blockMetadata);

void
freeParquetMetadata(
		ParquetMetadata parquetMetadata);



#endif /* CDBPARQUETFOOTERSERIALIZER_H_ */
