/*
 * getMetadata.h
 *
 *  Created on: Jun 25, 2013
 *      Author: malili
 */

#ifndef METADATAUTIL_H_
#define METADATAUTIL_H_

#include <stdint.h>
#include "parquet_types.h"
#include "thrift-util.h"
#include "access/parquetmetadata_c++/MetadataInterface.h"

using namespace std;

namespace hawq {

const parquet::Type::type HAWQ_TO_PARQUET_TYPES[] = {
  parquet::Type::BOOLEAN,
  parquet::Type::BOOLEAN,
  parquet::Type::BOOLEAN,
  parquet::Type::INT32,
  parquet::Type::INT32,
  parquet::Type::INT32,
  parquet::Type::INT64,
  parquet::Type::FLOAT,
  parquet::Type::DOUBLE,
  parquet::Type::INT96,       // Timestamp
  parquet::Type::BYTE_ARRAY,
};

class MetadataUtil {
private:

	ThriftDeserializer *thriftDeserializer;

	static ThriftSerializer *thriftSerializer;

public:
	MetadataUtil() {
	}
	;
	virtual ~MetadataUtil() {
	}
	;

	void readMetadataInit();

	static int writeFileMetadata(uint8_t **buf, uint32_t *len,
			ParquetMetadata_4C* fileMetadata);

	static int writeColumnChunkMetadata(uint8_t **buf, uint32_t *len,
			ColumnChunkMetadata_4C* hawqColumnChunkMetadata);

	static int writePageMetadata(uint8_t **buf, uint32_t *len,
			PageMetadata_4C* pageMetadata);

	static int convertToParquetMetadata(
			parquet::FileMetaData *parquetFileMetadata,
			ParquetMetadata_4C* hawqFileMetadata);

	static int convertToColumnMetadata(parquet::ColumnMetaData *columnchunk_metadata,
			ColumnChunkMetadata_4C* hawqColumnMetadata);

	static int convertToPageMetadata(
			parquet::PageHeader *parquetHeader,
			PageMetadata_4C* hawqPageMetadata);


	int readFileMetadata(uint8_t *buf, uint32_t len, bool compact,
			ParquetMetadata_4C* pfileMedata);

	int readPageMetadata(uint8_t *buf, uint32_t *len, bool compact,
			PageMetadata_4C* pPageMetadata);

	void convertFileMetadata(parquet::FileMetaData& parquetFileMetadata,
			ParquetMetadata_4C* hawqFileMetadata);

	void convertPageMetadata(parquet::PageHeader& parquetHeader,
			PageMetadata_4C* hawqPageMetadata);


	int fromParquetKeyValue(std::vector<parquet::KeyValue> keyValue,
			char **hawqschemastr);
};
}
#endif /* GETMETADATA_H_ */
