/*
 * MetadataUtil.h
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

	static ThriftSerializer *thriftSerializer;

public:

	static int writeColumnChunkMetadata(uint8_t **buf, uint32_t *len,
			ColumnChunkMetadata_4C* hawqColumnChunkMetadata);

	static int writePageMetadata(uint8_t **buf, uint32_t *len,
			PageMetadata_4C* pageMetadata);

	static int convertToColumnMetadata(parquet::ColumnMetaData *columnchunk_metadata,
			ColumnChunkMetadata_4C* hawqColumnMetadata);

	static int convertToPageMetadata(
			parquet::PageHeader *parquetHeader,
			PageMetadata_4C* hawqPageMetadata);

	static int readPageMetadata(uint8_t *buf, uint32_t *len, bool compact,
			PageMetadata_4C* pPageMetadata);

	static void convertPageMetadata(parquet::PageHeader& parquetHeader,
			PageMetadata_4C* hawqPageMetadata);

};
}
#endif /* GETMETADATA_H_ */
