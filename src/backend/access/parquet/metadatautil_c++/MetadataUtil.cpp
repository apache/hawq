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
 * MetadataUtil.cpp
 *
 *  Created on: Jun 25, 2013
 *      Author: malili
 */

#include <fcntl.h>
#include <vector>
#include <string.h>

extern "C" {
#include "postgres.h"
#include "utils/palloc.h"
#include "lib/stringinfo.h"
}

#include "MetadataUtil.h"
#include "parquet_types.h"

using namespace hawq;

ThriftSerializer *MetadataUtil::thriftSerializer  = new ThriftSerializer(true, 1024);

/*
 * Read page metadata from parquet format to hawq internal format
 *
 * buf:		buffer containing the parquet format page metadata
 * len:		buffer length to be read
 * compact:	whether use thrift compact protocol
 * pageMetadata:	the target page metadata. <result>
 */
int
MetadataUtil::readPageMetadata(uint8_t *buf, uint32_t *len, bool compact,
		PageMetadata_4C* pageMetadata) {
	parquet::PageHeader parquetHeader;
	if (ThriftDeserializer::DeserializeThriftMsg(buf, len, compact,
			&parquetHeader) < 0)
		return -1;

	convertPageMetadata(parquetHeader, pageMetadata);
	return 0;
}

/*
 * Write page metadata to buffer
 *
 * buf:		buffer to be writen
 * len:		buffer length to be read
 * pageMetadata: the page metadata to be written out to buffer
 */

int
MetadataUtil::writePageMetadata(uint8_t **buf, uint32_t *len,
		PageMetadata_4C* pageMetadata) {
	parquet::PageHeader page_header_;
	convertToPageMetadata(&page_header_, pageMetadata);
	return thriftSerializer->Serialize(&page_header_, len, buf);
}

/*
 * Write out column chunk metadata to buffer
 */
int
MetadataUtil::writeColumnChunkMetadata(uint8_t **buf, uint32_t *len,
		ColumnChunkMetadata_4C* hawqColumnChunkMetadata) {
	parquet::ColumnMetaData columnchunk_metadata;
	convertToColumnMetadata(&columnchunk_metadata, hawqColumnChunkMetadata);
	return thriftSerializer->Serialize(&columnchunk_metadata, len, buf);
}

/*
 *	Convert from hawq encoding type to parquet encoding type, for serialization purpose.
 */
int
toParquetEncoding(std::vector<parquet::Encoding::type> &encodings,
		Encoding* pEncodings, int encodingCount) {
	if (encodingCount == 0)
		return -1;
	encodings.resize(encodingCount + 1);
	for (int i = 0; i < encodingCount; ++i) {
		encodings[i] = (parquet::Encoding::type) pEncodings[i];
	}
	return 0;
}

/*
 *	Convert from hawq path to parquet path_in_schema in parquet. The path is splited with ":"
 */
int
toParquetPathInSchema(std::vector<std::string> &path_in_schema, int depth, const char* path){
	Assert(NULL != path);

	char *p;
	const char *delim = ":";
	std::string newPath = path;
	path_in_schema.resize(depth);
	path_in_schema[0] = strtok(&newPath[0], delim);

	for (int i = 1; i < depth; i++) {
		p = strtok(NULL, delim);
		if (p == NULL) {
			return -1;
		} else {
			path_in_schema[i] = p;
		}
	}

	return 0;
}

/*
 * Convert from parquet metadata to hawq metadata about page
 */
void
MetadataUtil::convertPageMetadata(parquet::PageHeader& parquetHeader,
		PageMetadata_4C* hawqPageMetadata) {
	hawqPageMetadata->compressed_page_size = parquetHeader.compressed_page_size;
	hawqPageMetadata->uncompressed_page_size =
			parquetHeader.uncompressed_page_size;
	hawqPageMetadata->crc = parquetHeader.crc;
	hawqPageMetadata->page_type = (enum PageType) parquetHeader.type;

	hawqPageMetadata->definition_level_encoding =
			(enum Encoding) parquetHeader.data_page_header.definition_level_encoding;
	hawqPageMetadata->encoding =
			(enum Encoding) parquetHeader.data_page_header.encoding;
	hawqPageMetadata->repetition_level_encoding =
			(enum Encoding) parquetHeader.data_page_header.repetition_level_encoding;
	hawqPageMetadata->num_values = parquetHeader.data_page_header.num_values;
}

/*
 * Convert from hawq page metadata to parquet
 */
int
MetadataUtil::convertToPageMetadata(parquet::PageHeader *parquetHeader,
		PageMetadata_4C* hawqPageMetadata) {
	parquet::DataPageHeader dataPageHeader;

	parquetHeader->__set_type(
			(enum parquet::PageType::type) hawqPageMetadata->page_type);
	parquetHeader->__set_compressed_page_size(
			hawqPageMetadata->compressed_page_size);
	parquetHeader->__set_crc(hawqPageMetadata->crc);
	parquetHeader->__set_uncompressed_page_size(
			hawqPageMetadata->uncompressed_page_size);


	dataPageHeader.__set_definition_level_encoding(
			(enum parquet::Encoding::type) hawqPageMetadata->definition_level_encoding);
	dataPageHeader.__set_encoding(
			(enum parquet::Encoding::type) hawqPageMetadata->encoding);
	dataPageHeader.__set_repetition_level_encoding(
			(enum parquet::Encoding::type) hawqPageMetadata->repetition_level_encoding);
	dataPageHeader.__set_num_values(hawqPageMetadata->num_values);

	parquetHeader->__set_data_page_header(dataPageHeader);
	return 0;
}

/*
 * Convert from hawq column metadata to parquet column metadata
 */
int
MetadataUtil::convertToColumnMetadata(
		parquet::ColumnMetaData *columnchunk_metadata,
		ColumnChunkMetadata_4C* hawqColumnMetadata) {
	std::vector<parquet::Encoding::type> encodings;
	std::vector < std::string > pathschema;
	int subcall = 0;

	columnchunk_metadata->__set_codec(
			(parquet::CompressionCodec::type) hawqColumnMetadata->codec);
	columnchunk_metadata->__set_data_page_offset(
			hawqColumnMetadata->firstDataPage);
	columnchunk_metadata->__set_dictionary_page_offset(-1);
	columnchunk_metadata->__set_index_page_offset(-1);
	columnchunk_metadata->__set_num_values(hawqColumnMetadata->valueCount);
	columnchunk_metadata->__set_total_compressed_size(
			hawqColumnMetadata->totalSize);
	columnchunk_metadata->__set_total_uncompressed_size(
			hawqColumnMetadata->totalUncompressedSize);
	columnchunk_metadata->__set_type(
			(parquet::Type::type) hawqColumnMetadata->type);

	subcall = toParquetEncoding(encodings, hawqColumnMetadata->pEncodings,
			hawqColumnMetadata->EncodingCount);
	if (subcall < 0) {
		return subcall;
	}
	columnchunk_metadata->__set_encodings(encodings);

	subcall = toParquetPathInSchema(pathschema, hawqColumnMetadata->depth,
			hawqColumnMetadata->pathInSchema);
	if (subcall < 0) {
		return subcall;
	}
	columnchunk_metadata->__set_path_in_schema(pathschema);

	return 0;
}
