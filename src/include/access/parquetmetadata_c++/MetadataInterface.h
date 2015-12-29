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
 * MetadataInterface.h
 *
 *  Created on: Jul 1, 2013
 *      Author: malili
 */

#ifndef METADATAINTERFACE_H_
#define METADATAINTERFACE_H_

#include <stdint.h>

typedef enum CompressionCodecName
{
	UNCOMPRESSED, SNAPPY, GZIP, LZO
} CompressionCodecName;

typedef enum Encoding
{
	PLAIN, GROUP_VAR_INT, PLAIN_DICTIONARY, RLE, BIT_PACKED
} Encoding;// Encoding;

typedef enum PrimitiveTypeName
{
	BOOLEAN,
	INT32,
	INT64,
	INT96,
	FLOAT,
	DOUBLE,
	BINARY,
	FIXED_LEN_BYTE_ARRAY
} PrimitiveTypeName;

typedef enum RepetitionType
{
  REQUIRED = 0,
  OPTIONAL = 1,
  REPEATED = 2
} RepetitionType;

typedef enum PageType
{
  DATA_PAGE = 0,
  INDEX_PAGE = 1,
  DICTIONARY_PAGE = 2
} PageType;

/**
 * the field description, maybe a primitive type, or a group type corresponding to a nested type.
 * For primitive type, num_children equals 0, and children points to NULL;
 * For group type(internal type), type equals NULL.
 */
typedef struct FileField_4C
{
	char *name;
	int typeLength;
	enum PrimitiveTypeName type;
	enum RepetitionType repetitionType;
	int hawqTypeId;	/*hawq type id*/
	int num_children;
	struct FileField_4C *children;
	int r;	/*repetition level of this field*/
	int d;	/*definition level of this field*/
	int depth;	/*depth of path in schema*/
	char *pathInSchema;
} FileField_4C;

typedef struct ColumnChunkMetadata_4C
{
	enum CompressionCodecName codec;
	char *path;
	char *colName;
	char *pathInSchema;
	int hawqTypeId;
	int r;	/*max repetition level of this column*/
	int d;	/*max definition level of this column*/
	int depth;	/*depth of path in schema*/
	enum PrimitiveTypeName type;
	enum Encoding* pEncodings;
	int EncodingCount;

    /* Byte offset in file_path to the ColumnMetaData */
	int64_t file_offset;

	int64_t firstDataPage;
	long valueCount;

    /* total byte size of all compressed pages in this column chunk (including the headers) */
	int64_t totalSize;

    /* total byte size of all uncompressed pages in this column chunk (including the headers) */
	int64_t totalUncompressedSize;

} ColumnChunkMetadata_4C;

/* rowgroup metadata */
typedef struct BlockMetadata_4C
{
	struct ColumnChunkMetadata_4C* columns;
	int ColChunkCount;
	long rowCount;

    /* Total byte size of all the uncompressed column data in this row group */
	long totalByteSize;
} BlockMetadata_4C;

typedef struct ParquetMetadata_4C
{
	struct FileField_4C* pfield;	/*The first level of field, the field may have children itself*/
	int fieldCount;	/*first level field count*/

	long num_rows;	/*number of rows*/

	struct BlockMetadata_4C *currentBlockMD;	/*the metadata of current row group*/
	int blockCount;		/*count of row groups*/

	char *hawqschemastr;	/*hawq schema str, should output into parquet metadata keyvalue part*/
	int colCount;	/*The number of columns in each row group, which is the expanded columns of pfield*/
	int schemaTreeNodeCount;	/*the count of all the nodes in schema tree, including middle nodes and leaf nodes*/

    int *estimateChunkSizes; /* array of estimated sizes for each columnchunk */

	int version; /*the version of parquet file*/
} ParquetMetadata_4C;

typedef struct PageMetadata_4C
{
	int32_t num_values;
	enum Encoding encoding;
	enum Encoding definition_level_encoding;
	enum Encoding repetition_level_encoding;
	enum PageType page_type;
    /* Uncompressed page size in bytes (not including this header) */
	int32_t uncompressed_page_size;
    /* Compressed page size in bytes (not including this header) */
	int32_t compressed_page_size;
	int32_t crc;
} PageMetadata_4C;


#ifdef __cplusplus
extern "C" {
#endif

int readPageMetadata(uint8_t *buf,
		uint32_t *len,
		int compact,
		struct PageMetadata_4C** ppageMetdata);

int writePageMetadata(uint8_t **buf,
		uint32_t *len,
		struct PageMetadata_4C* ppageMetadata);

int writeColumnChunkMetadata(uint8_t **buf,
		uint32_t *len,
		struct ColumnChunkMetadata_4C* columnChunkMetadata);


#ifdef __cplusplus
}
#endif

#endif /* METADATAINTERFACE_H_ */
