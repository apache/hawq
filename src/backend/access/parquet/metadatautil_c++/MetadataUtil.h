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
