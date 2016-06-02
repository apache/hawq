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
 * cdbparquetstoragewrite.h
 *
 *  Created on: Jul 30, 2013
 *      Author: malili
 */

#ifndef CDBPARQUETSTORAGEWRITE_H_
#define CDBPARQUETSTORAGEWRITE_H_

#include "postgres.h"
#include "access/parquetmetadata_c++/MetadataInterface.h"
#include "storage/fd.h"
#include "access/tupdesc.h"
#include "cdb/cdbmirroredappendonly.h"
#include "utils/relcache.h"
#include "cdb/cdbparquetrleencoder.h"
#include "cdb/cdbparquetbytepacker.h"
#include "cdb/cdbparquetfooterprocessor.h"

typedef struct PageMetadata_4C* ParquetPageHeader;
typedef struct ColumnChunkMetadata_4C* ColumnChunkMetadata;
typedef struct BlockMetadata_4C* RowGroupMetadata;
typedef struct FileField_4C* ParquetFileField;

#define DEFAULT_PARQUET_ROWGROUP_SIZE 			8*1024*1024
#define DEFAULT_PARQUET_PAGE_SIZE 				1*1024*1024
#define DEFAULT_PARQUET_ROWGROUP_SIZE_PARTITION 8*1024*1024
#define DEFAULT_PARQUET_PAGE_SIZE_PARTITION 	1*1024*1024

#define MIN_PARQUET_PAGE_SIZE			 1024
#define MIN_PARQUET_ROWGROUP_SIZE		 1024
#define MAX_PARQUET_PAGE_SIZE			 1024*1024*1024
#define MAX_PARQUET_ROWGROUP_SIZE		 1024*1024*1024

/*
 * mapping to Parquet primitive type
 */
#define HAWQ_TYPE_BOOL			16
#define HAWQ_TYPE_CHAR			18
#define HAWQ_TYPE_NAME			19
#define HAWQ_TYPE_INT8			20
#define HAWQ_TYPE_INT2			21
#define HAWQ_TYPE_INT4			23
#define HAWQ_TYPE_FLOAT4		700
#define HAWQ_TYPE_FLOAT8		701
#define HAWQ_TYPE_MONEY 		790
#define HAWQ_TYPE_NUMERIC		1700
#define HAWQ_TYPE_BYTE			17
#define HAWQ_TYPE_TEXT			25
#define HAWQ_TYPE_XML			142
#define HAWQ_TYPE_MACADDR		829
#define HAWQ_TYPE_INET			869
#define HAWQ_TYPE_CIDR			650
#define HAWQ_TYPE_BPCHAR		1042
#define HAWQ_TYPE_VARCHAR		1043
#define HAWQ_TYPE_DATE			1082
#define HAWQ_TYPE_TIME			1083
#define HAWQ_TYPE_TIMESTAMP		1114
#define HAWQ_TYPE_TIMETZ		1266
#define HAWQ_TYPE_TIMESTAMPTZ	1184
#define HAWQ_TYPE_INTERVAL		1186
#define HAWQ_TYPE_BIT			1560
#define HAWQ_TYPE_VARBIT		1562
/*
 * mapping to Parquet group type
 */
#define HAWQ_TYPE_POINT			600
#define HAWQ_TYPE_LSEG			601
#define HAWQ_TYPE_PATH			602
#define HAWQ_TYPE_BOX			603
#define HAWQ_TYPE_POLYGON		604
#define HAWQ_TYPE_CIRCLE		718

#define DEFAULT_ROWGROUP_COUNT	20
#define DEFAULT_DATAPAGE_COUNT	1

typedef struct ParquetDataPage_S    *ParquetDataPage;
typedef struct ParquetColumnChunk_S *ParquetColumnChunk;
typedef struct ParquetRowGroup_S    *ParquetRowGroup;

struct ParquetDataPage_S
{
	/* Page header.  This is a union of all page types.*/
	ParquetPageHeader 			header;

	RLEEncoder 					*repetition_level;
	RLEDecoder 					*repetition_level_reader;

	RLEEncoder 					*definition_level;
	RLEDecoder 					*definition_level_reader;

	/* bool value use bitpack encoder */
	ByteBasedBitPackingEncoder	*bool_values;
	ByteBasedBitPackingDecoder	*bool_values_reader;

	/* For non-bool columns, this is where the output is accumulated before compression. */
	uint8_t						*values_buffer;
    int                         values_buffer_capacity; /* palloced size for values_buffer */

    /*
     * For write, this is the page data to write, may be compressed.
     * For read, this is the page data to read, may be decompressed.
     *
     * Page data inculdes r/d/values, not include page header.
     */
	uint8_t						*data;

	uint8_t						*header_buffer; /*stores the buffer for page header*/

	int							header_len; /*the length of data page header*/

	/*If true, this data page has been finalized.  All sizes are computed, header is
	fully populated and any compression is done.*/
	bool 						finalized;

	File 						parquetFile;
};

struct ParquetColumnChunk_S
{
	ColumnChunkMetadata 		columnChunkMetadata;

	ParquetDataPage 			pages;

	ParquetDataPage 			currentPage;

	int 						maxPageCount; /*indicates the allocated array size for pages*/

	int 						pageNumber;
    
    /*
     * Used to estimate size for values_buffer of each data pages.
     * e.g, if estimateChunkSizeRemained is 1.5 * pageSizeLimit, then
     * for the first data page, we init values_buffer_capacity to be pageSizeLimit,
     * for the second data page, we init values_buffer_capacity to be 0.5pageSizeLimit.
     *
     * The estimation is based on uncompressed chunk size of last rowgroup. For chunks in
     * the first rowgroup, the estimation is based on column width.
     */
    int                         estimateChunkSizeRemained;

	int                         pageSizeLimit;          /* pagesize in pg_appendonly */

    char    					*compresstype;
    int     					compresslevel;

	File 						parquetFile;
};

/*
 * working state for the current rowgroup
 */
struct ParquetRowGroup_S
{
    AppendOnlyEntry    			*catalog;

	RowGroupMetadata 			rowGroupMetadata;

	ParquetColumnChunk 			columnChunks;

	int 						columnChunkNumber;

	File 						parquetFile;
};


/*----------------------------------------------------------------
 * rowgroup API
 *----------------------------------------------------------------*/

/*
 * Create and initialize a new rowgroup for inserting, add it's metadata to `parquetmd`.
 *
 * Return the created rowgroup.
 */
ParquetRowGroup addRowGroup(
        ParquetMetadata parquetmd,
        TupleDesc tableAttrs,
        AppendOnlyEntry *aoentry,
        File file);

/*
 * Write out all columns of `rowgroup`.
 */
void flushRowGroup(
		ParquetRowGroup rowgroup,
		ParquetMetadata parquetmd,
		MirroredAppendOnlyOpen *mirroredOpen,
		CompactProtocol	*footerProtocol,
		int64 *fileLen,
		int64 *fileLen_uncompressed);

/*
 * After `rowgroup` has been flushed, this routine releases its memory.
 * Note that RowGroupMetadata is not freed until parquet_insert_finish since all RowGroupMetadata is
 * written at the end of file.
 */
void freeRowGroup(ParquetRowGroup rowgroup);

/*----------------------------------------------------------------
 * insert API
 *----------------------------------------------------------------*/

/*
 * append one row (`values` and `nulls`) to `rowgroup`.
 */
void appendRowValue(ParquetRowGroup rowgroup, ParquetMetadata parquetmd, Datum* values, bool* nulls);

int initparquetMetadata(ParquetMetadata parquetMetadata, TupleDesc tableAttrs, File parquetFile);

int mappingHAWQType(int hawqTypeID);

void estimateColumnWidth(int *columnWidths,
					int *colidx,
					Form_pg_attribute att,
					bool expandEmbeddingType);

/*
 * Generate key_value_metadata filed used in FileMetaData.
 */
char * generateHAWQSchemaStr(ParquetFileField pfields,
					int fieldCount);
#endif /* CDBPARQUETSTORAGEWRITE_H_ */
