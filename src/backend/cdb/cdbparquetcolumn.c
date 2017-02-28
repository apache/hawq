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
 * cdbparquetcolumn.c
 *
 *  Created on: Sep 29, 2013
 *      Author: malili
 */
#include "cdb/cdbparquetcolumn.h"
#include "cdb/cdbparquetrleencoder.h"
#include "utils/inet.h"
#include "utils/date.h"
#include "utils/timestamp.h"
#include "utils/geo_decls.h"
#include "utils/memutils.h"

#include "snappy-c.h"
#include "zlib.h"

#define BUFFER_SCALE_FACTOR	1.2
#define BUFFER_SIZE_LIMIT_BEFORE_SCALED ((Size) ((MaxAllocSize) * 1.0 / (BUFFER_SCALE_FACTOR))) 

static void consume(ParquetColumnReader *columnReader);
static void readRepetitionAndDefinitionLevels(ParquetColumnReader *columnReader);
static void decodeCurrentPage(ParquetColumnReader *columnReader);

static bool decodePlain(Datum *value, uint8_t **buffer, int hawqTypeID);

/* return size of PATH struct given number of points in it */
static inline int get_path_size(int npts) { return offsetof(PATH, p[0]) + sizeof(Point) * npts; }

/* return size of POLYGON struct given number of points in it */
static inline int get_polygon_size(int npts) { return offsetof(POLYGON, p[0]) + sizeof(Point) * npts; }

void
ParquetExecutorReadColumn(ParquetColumnReader *columnReader, File file)
{
	struct ColumnChunkMetadata_4C* columnChunkMetadata = columnReader->columnMetadata;

	int64 firstPageOffset = columnChunkMetadata->firstDataPage;

	int64 columnChunkSize = columnChunkMetadata->totalSize;

	if ( columnChunkSize > MaxAllocSize ) 
	{
        ereport(ERROR,
                (errcode(ERRCODE_GP_INTERNAL_ERROR),
                 errmsg("parquet storage read error on reading column %s due to too large column chunk size: " INT64_FORMAT,
                         columnChunkMetadata->colName, columnChunkSize)));
    }

	int64 actualReadSize = 0;

	MemoryContext oldContext = MemoryContextSwitchTo(columnReader->memoryContext);

	/*reuse the column reader data buffer to avoid memory re-allocation*/
	if(columnReader->dataLen == 0)
	{
		columnReader->dataLen = columnChunkSize < BUFFER_SIZE_LIMIT_BEFORE_SCALED ?
								columnChunkSize * BUFFER_SCALE_FACTOR :
								MaxAllocSize-1;

		columnReader->dataBuffer = (char*) palloc0(columnReader->dataLen);
	}
	else if(columnReader->dataLen < columnChunkSize)
	{
        columnReader->dataLen = columnChunkSize < BUFFER_SIZE_LIMIT_BEFORE_SCALED ?
                                columnChunkSize * BUFFER_SCALE_FACTOR :
                                MaxAllocSize-1;

		columnReader->dataBuffer = (char*) repalloc(columnReader->dataBuffer, columnReader->dataLen);
		memset(columnReader->dataBuffer, 0, columnReader->dataLen);
	}

	char *buffer = columnReader->dataBuffer;

	int64 numValuesInColumnChunk = columnChunkMetadata->valueCount;

	int64 numValuesProcessed = 0;

	/*seek to the beginning of the column chunk*/
	int64 seekResult = FileSeek(file, firstPageOffset, SEEK_SET);
	if (seekResult != firstPageOffset)
	{
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("file seek error to position " INT64_FORMAT ": %s", firstPageOffset, strerror(errno)),
				 errdetail("%s", HdfsGetLastError())));
	}

	/*recursively read, until get the total column chunk data out*/
	while(actualReadSize < columnChunkSize)
	{
		/*read out all the buffer of the column chunk*/
		int columnChunkLen = FileRead(file, buffer + actualReadSize, columnChunkSize - actualReadSize);
		if (columnChunkLen < 0) {
			ereport(ERROR,
					(errcode_for_file_access(),
							errmsg("parquet storage read error on reading column %s ", columnChunkMetadata->colName),
							errdetail("%s", HdfsGetLastError())));
		}
		actualReadSize += columnChunkLen;
	}

	/*only if first column reader set, just need palloc the data pages*/
	if(columnReader->dataPageCapacity == 0)
	{
		columnReader->dataPageCapacity = DAFAULT_DATAPAGE_NUM_PER_COLUMNCHUNK;
		columnReader->dataPageNum = 0;
		columnReader->dataPages = (ParquetDataPage)palloc0
			(columnReader->dataPageCapacity *sizeof(struct ParquetDataPage_S));
	}

	/* read all the data pages of the column chunk */
	while(numValuesProcessed < numValuesInColumnChunk)
	{
		ParquetPageHeader pageHeader;
		ParquetDataPage dataPage;

		uint32_t header_size = (char *) columnReader->dataBuffer + columnChunkSize - buffer;
		if (readPageMetadata((uint8_t*) buffer, &header_size, /*compact*/1, &pageHeader) < 0)
		{
			ereport(ERROR, (errcode(ERRCODE_GP_INTERNAL_ERROR),
				errmsg("thrift deserialize failure on reading page header of column %s ",
						columnChunkMetadata->colName)));
		}

		buffer += header_size;

		/*just process data page now*/
		if(pageHeader->page_type != DATA_PAGE){
			if(pageHeader->page_type == DICTIONARY_PAGE) {
				ereport(ERROR, (errcode(ERRCODE_GP_INTERNAL_ERROR),
								errmsg("HAWQ does not support dictionary page type resolver for Parquet format in column \'%s\' ",
										columnChunkMetadata->colName)));
			}
			buffer += pageHeader->compressed_page_size;
			continue;
		}

		if(columnReader->dataPageNum == columnReader->dataPageCapacity)
		{
			columnReader->dataPages = (ParquetDataPage)repalloc(
					columnReader->dataPages,
					2 * columnReader->dataPageCapacity * sizeof(struct ParquetDataPage_S));
			
			memset(columnReader->dataPages + columnReader->dataPageCapacity, 0,
				   columnReader->dataPageCapacity * sizeof(struct ParquetDataPage_S));

			columnReader->dataPageCapacity *= 2;
		}

		dataPage = &(columnReader->dataPages[columnReader->dataPageNum]);
		dataPage->header	= pageHeader;

		dataPage->data		= (uint8_t *) buffer;
		buffer += pageHeader->compressed_page_size;

		numValuesProcessed += pageHeader->num_values;
		columnReader->dataPageNum++;
	}

	MemoryContextSwitchTo(oldContext);

	columnReader->currentPageValueRemained = 0;	/* indicate to read next page */
	columnReader->dataPageProcessed = 0;

	if (columnChunkMetadata->r > 0)
	{
		consume(columnReader);
	}
}

/*
 * End the current value, move to next r/d/value.
 * Should be called after current value is read.
 */
static void
consume(ParquetColumnReader *columnReader)
{
	/* make sure we have values to read in current page */
	if (columnReader->currentPageValueRemained == 0)
	{
		if (columnReader->dataPageProcessed >= columnReader->dataPageNum)
		{
			/* next r must be 0 when reached chunk end */
			columnReader->repetitionLevel = 0;
			return;
		}

		/* read next page */
		columnReader->currentPage = &columnReader->dataPages[columnReader->dataPageProcessed];
		decodeCurrentPage(columnReader);

		columnReader->currentPageValueRemained = columnReader->currentPage->header->num_values;
		columnReader->dataPageProcessed++;
	}

	readRepetitionAndDefinitionLevels(columnReader);
}

static void
readRepetitionAndDefinitionLevels(ParquetColumnReader *reader)
{
	if (reader->currentPage->repetition_level_reader)
	{
		reader->repetitionLevel =
			RLEDecoder_ReadInt(reader->currentPage->repetition_level_reader);
	}

	if (reader->currentPage->definition_level_reader)
	{
		reader->definitionLevel =
			RLEDecoder_ReadInt(reader->currentPage->definition_level_reader);
	}

	reader->currentPageValueRemained--;
}

/*
 * Decode raw data of chunk's current page into corresponding r/d/bool reader
 * and values_buffer, uncompress data if needed.
 */
static void
decodeCurrentPage(ParquetColumnReader *columnReader)
{
	ColumnChunkMetadata_4C *chunkmd;
	ParquetDataPage page;
	ParquetPageHeader header;
	uint8_t			*buf;	/* store uncompressed or decompressed page data */
	MemoryContext	oldContext;

	chunkmd	= columnReader->columnMetadata;
	page	= columnReader->currentPage;
	header	= page->header;

	oldContext = MemoryContextSwitchTo(columnReader->memoryContext);

	/*----------------------------------------------------------------
	 * Decompress raw data. After this, buf & page->data points to
	 * uncompressed/decompressed data.
	 *----------------------------------------------------------------*/
	if (chunkmd->codec == UNCOMPRESSED)
	{
		buf = page->data;
	}
	else
	{
		/*
		 * make `buf` points to decompressed buffer,
		 * which should be large enough for uncompressed data.
		 */
		if (chunkmd->r > 0)
		{
			/* repeatable column creates decompression buffer for each page */
			buf = palloc0(header->uncompressed_page_size);
		}
		else
		{	
			/* non-repeatable column reuses pageBuffer for decompression */
			if (columnReader->pageBuffer == NULL)
			{
				columnReader->pageBufferLen = header->uncompressed_page_size * BUFFER_SCALE_FACTOR;
				columnReader->pageBuffer = palloc0(columnReader->pageBufferLen);
			}
			else if (columnReader->pageBufferLen < header->uncompressed_page_size)
			{
				columnReader->pageBufferLen = header->uncompressed_page_size * BUFFER_SCALE_FACTOR;
				columnReader->pageBuffer = repalloc(columnReader->pageBuffer, columnReader->pageBufferLen);
			}
			buf = (uint8_t *) columnReader->pageBuffer;
		}

		/*
		 * call corresponding decompress routine
		 */
		switch (chunkmd->codec)
		{
			case SNAPPY:
			{
				size_t uncompressedLen;
				if (snappy_uncompressed_length((char *) page->data,
											   header->compressed_page_size,
											   &uncompressedLen) != SNAPPY_OK)
				{
					ereport(ERROR,
							(errcode(ERRCODE_GP_INTERNAL_ERROR),
							 errmsg("invalid snappy compressed data for column %s, page number %d",
									chunkmd->colName, columnReader->dataPageProcessed)));
				}

				Insist(uncompressedLen == header->uncompressed_page_size);

				if (snappy_uncompress((char *) page->data,		header->compressed_page_size,
									  (char *) buf,				&uncompressedLen) != SNAPPY_OK)
				{
					ereport(ERROR,
							(errcode(ERRCODE_GP_INTERNAL_ERROR),
							 errmsg("failed to decompress snappy data for column %s, page number %d, "
									"uncompressed size %d, compressed size %d",
									chunkmd->colName, columnReader->dataPageProcessed,
									header->uncompressed_page_size, header->compressed_page_size)));
				}
				
				page->data = buf;
				break;
			}
			case GZIP:
			{
				int ret;
				/* 15(default windowBits for deflate) + 16(ouput GZIP header/tailer) */
				const int windowbits = 31;

				z_stream stream;
				stream.zalloc	= Z_NULL;
				stream.zfree	= Z_NULL;
				stream.opaque	= Z_NULL;
				stream.avail_in	= header->compressed_page_size;
				stream.next_in	= (Bytef *) page->data;
				
				ret = inflateInit2(&stream, windowbits);
				if (ret != Z_OK)
				{
					ereport(ERROR,
							(errcode(ERRCODE_GP_INTERNAL_ERROR),
							 errmsg("zlib inflateInit2 failed: %s", stream.msg)));
				}

				size_t uncompressedLen = header->uncompressed_page_size;

				stream.avail_out = uncompressedLen;
				stream.next_out  = (Bytef *) buf;
				ret = inflate(&stream, Z_FINISH);
				if (ret != Z_STREAM_END)
				{
					ereport(ERROR,
							(errcode(ERRCODE_GP_INTERNAL_ERROR),
							 errmsg("zlib inflate failed: %s", stream.msg)));
				
				}
				/* should fill all uncompressed_page_size bytes */
				Assert(stream.avail_out == 0);

				inflateEnd(&stream);

				page->data = buf;
				break;
			}
			case LZO:
				/* TODO */
				Insist(false);
				break;
			default:
				Insist(false);
				break;
		}
	}

	/*----------------------------------------------------------------
	 * get r/d/value part
	 *----------------------------------------------------------------*/
	if(chunkmd->r != 0)
	{
		int num_repetition_bytes = /*le32toh(*/ *((uint32_t *) buf) /*)*/;
		buf += 4;

		page->repetition_level_reader = (RLEDecoder *) palloc0(sizeof(RLEDecoder));
		RLEDecoder_Init(page->repetition_level_reader,
						widthFromMaxInt(chunkmd->r),
						buf,
						num_repetition_bytes);

		buf += num_repetition_bytes;
	}

	if(chunkmd->d != 0)
	{
		int num_definition_bytes = /*le32toh(*/ *((uint32_t *) buf) /*)*/;
		buf += 4;

		page->definition_level_reader = (RLEDecoder *) palloc0(sizeof(RLEDecoder));
		RLEDecoder_Init(page->definition_level_reader,
						widthFromMaxInt(chunkmd->d),
						buf,
						num_definition_bytes);

		buf += num_definition_bytes;
	}

	if (chunkmd->type == BOOLEAN)
	{
		page->bool_values_reader = (ByteBasedBitPackingDecoder *)
				palloc0(sizeof(ByteBasedBitPackingDecoder));
		BitPack_InitDecoder(page->bool_values_reader, buf, /*bitwidth=*/1);
	}
	else
	{
		page->values_buffer = buf;
	}

	MemoryContextSwitchTo(oldContext);
}

/**
 * Read the value from a certain columnReader, the value will be embedded in value,
 * and if the value is null, the null field should be true
 * @columnReader		the column which needs to be read
 * @value				used to store the value of the record
 * @null				used to record whether the value is null
 */
void
ParquetColumnReader_readValue(
		ParquetColumnReader *columnReader,
		Datum *value,
		bool *null,
		int hawqTypeID)
{
	/*
	 * for non-repeatable column, because of using shared `pageBuffer`,
	 * consume should be called after last value is truely returned.
	 */
	if (columnReader->columnMetadata->r == 0)
	{
		consume(columnReader);
	}

	/* current definition level is used to determine null value */
	if (CurrentDefinitionLevel(columnReader) < columnReader->columnMetadata->d)
	{
		*null = true;
	}
	else
	{
		*null = false;

		if (hawqTypeID == HAWQ_TYPE_BOOL)
		{
			*value = BoolGetDatum((bool) BitPack_ReadInt(columnReader->currentPage->bool_values_reader));
		}
		else
		{
			decodePlain(value, &(columnReader->currentPage->values_buffer), hawqTypeID);
		}
	}
	
	/*
	 * for repeatable column, need to preread next value's r
	 */
	if (columnReader->columnMetadata->r > 0)
	{
		consume(columnReader);
	}
}

static bool
decodePlain(Datum *value, uint8_t **buffer, int hawqTypeID)
{
	switch(hawqTypeID)
	{
		case HAWQ_TYPE_INT2:
		case HAWQ_TYPE_INT4:
		case HAWQ_TYPE_DATE:
		case HAWQ_TYPE_FLOAT4:
		{
			*value = *((int32_t*)(*buffer));
			(*buffer) += 4;
			break;
		}

		case HAWQ_TYPE_MONEY:
		{
			/*
			 * money is a pass-by-ref type, the return Datum
			 * should be a pointer, see cash.c
			 */
			(*value) = PointerGetDatum(*buffer);
			(*buffer) += 8;
			break;
		}

		case HAWQ_TYPE_INT8:
		case HAWQ_TYPE_TIME:
		case HAWQ_TYPE_TIMESTAMPTZ:
		case HAWQ_TYPE_TIMESTAMP:
		case HAWQ_TYPE_FLOAT8:
		{
			*value = *((int64_t*)(*buffer));
			(*buffer) += 8;
			break;
		}

		/*----------------------------------------------------------------
		 * fixed length type, mapped to BINARY in Parquet
		 *----------------------------------------------------------------*/
		case HAWQ_TYPE_NAME:
		case HAWQ_TYPE_TIMETZ:
		case HAWQ_TYPE_INTERVAL:
		{
			int datalen = /*le32toh(*/*((int32_t*)(*buffer))/*)*/;
			(*buffer) += 4;
			*value = PointerGetDatum(*buffer);
			(*buffer) += datalen;
			break;
		}
		case HAWQ_TYPE_MACADDR:
		{
			(*buffer) += 4;	/* skip BINARY header */
			macaddr *result = (macaddr *)palloc0(sizeof(macaddr));
			result->a = *((char*)(*buffer));
			(*buffer) += sizeof(char);
			result->b = *((char*)(*buffer));
			(*buffer) += sizeof(char);
			result->c = *((char*)(*buffer));
			(*buffer) += sizeof(char);
			result->d = *((char*)(*buffer));
			(*buffer) += sizeof(char);
			result->e = *((char*)(*buffer));
			(*buffer) += sizeof(char);
			result->f = *((char*)(*buffer));
			(*buffer) += sizeof(char);

			(*value) = PointerGetDatum(result);
			break;
		}

		/* varlena based type
		 * ------------------
		 * The following types are implemented as varlena in HAWQ, they all corresponds
		 * to BINARY in Parquet. However, we have two strategies when storing them, depends
		 * on whether we stores varlena header or not.
		 *
		 * [strategy 1] exclude varlena header:
		 * 				BINARY = [VARSIZE(d) - 4, VARDATA(d)]
		 * 				This strategy works better for text-related type because
		 * 				any parquet client can interprete text binary.
		 * 				When reading:
		 * 				VARSIZE = LEN + 4;
		 * 				VARDATA = buffer + 4;
		 *
		 * [strategy 2] include varlena header in actual data:
		 * 				BINARY = [VARSIZE(d), d]
		 * 				This works better for HAWQ specific type like numeric because
		 * 				we can easily deserialize data. (just get the data part of the byte array)
		 * 				When reading:
		 * 				VARSIZE = LEN;
		 * 				VARDATA = buffer + 4 + 4; first 4 seeks to the varlena struct, the second 4 seeks to
		 * 				the VARDATA part of the struct
		 */

		/* these types use [strategy 1] */
		case HAWQ_TYPE_BYTE:
		case HAWQ_TYPE_CHAR:
		case HAWQ_TYPE_BPCHAR:
		case HAWQ_TYPE_VARCHAR:
		case HAWQ_TYPE_TEXT:
		case HAWQ_TYPE_XML:
		{
			int pureDataLen = /*le32toh(*/*((int32_t*)(*buffer))/*)*/;
			int dataSize = pureDataLen + VARHDRSZ;
			SET_VARSIZE((struct varlena *)(*buffer), dataSize);
			(*value) = PointerGetDatum(*buffer);
			(*buffer) += dataSize;
			break;
		}
		/* these types use [strategy 2] */
		case HAWQ_TYPE_BIT:
		case HAWQ_TYPE_VARBIT:
		case HAWQ_TYPE_NUMERIC:
		case HAWQ_TYPE_INET:
		case HAWQ_TYPE_CIDR:
		{
			int dataSize = /*le32toh(*/ *((int32_t*)(*buffer)) /*)*/;
			(*buffer) += sizeof(int32);
			(*value) = PointerGetDatum(*buffer);
			(*buffer) += dataSize;
			break;
		}


		default:
			Insist(false);
			break;
	}
	return true;
}


/**
 * finish scan current column, free and reset column reader part
 */
void
ParquetColumnReader_FinishedScanColumn(
		ParquetColumnReader *columnReader)
{
	MemoryContext oldContext = MemoryContextSwitchTo(columnReader->memoryContext);

	for(int i = 0; i < columnReader->dataPageNum; i++)
	{
		ParquetDataPage page = columnReader->dataPages + i;

		pfree(page->header);

		/* TODO may be reuse these decoder? */
		if (page->repetition_level_reader != NULL)
		{
			pfree(page->repetition_level_reader);
		}

		if (page->definition_level_reader != NULL)
		{
			pfree(page->definition_level_reader);
		}

		if (page->bool_values_reader != NULL)
		{
			pfree(page->bool_values_reader);
		}

		/*
		 * compressed repeatable column keeps each page's decompressed
		 * content in page->data, which should be freed.
		 */
		if (columnReader->columnMetadata->codec != UNCOMPRESSED &&
			columnReader->columnMetadata->r > 0)
		{
			pfree(page->data);
		}
	}

	if(columnReader->dataLen != 0){
		memset(columnReader->dataBuffer, 0, columnReader->dataLen);
	}

	if(columnReader->dataPageNum != 0)
	{
		memset(columnReader->dataPages, 0, columnReader->dataPageNum
				* sizeof(struct ParquetDataPage_S));
		columnReader->dataPageNum = 0;
	}

	if (columnReader->geoval != NULL)
	{
		pfree(columnReader->geoval);
		columnReader->geoval = NULL;
	}

	MemoryContextSwitchTo(oldContext);

	columnReader->dataPageProcessed = 0;
	columnReader->currentPageValueRemained = 0;
}

/*----------------------------------------------------------------
 * read nested geometry type
 *----------------------------------------------------------------*/

void
ParquetColumnReader_readPoint(
		ParquetColumnReader readers[],
		Datum *value,
		bool *null)
{
	Point *point;
	Datum child_values[2] = {0};
	bool  child_nulls[2] = {0};

	ParquetColumnReader_readValue(&readers[0], &child_values[0], &child_nulls[0], HAWQ_TYPE_FLOAT8);
	ParquetColumnReader_readValue(&readers[1], &child_values[1], &child_nulls[1], HAWQ_TYPE_FLOAT8);

	Insist(child_nulls[0] == child_nulls[1]);
	
	if (child_nulls[0])
	{
		*null = true;
		return;
	}

	point = readers[0].geoval;
	if (point == NULL)
	{
		point = readers[0].geoval = palloc(sizeof *point);
		readers[0].geoval = point;
	}
	point->x = DatumGetFloat8(child_values[0]);
	point->y = DatumGetFloat8(child_values[1]);

	*value = PointerGetDatum(point);
	*null = false;
}

void
ParquetColumnReader_readLSEG(
		ParquetColumnReader readers[],
		Datum *value,
		bool *null)
{
	LSEG *lseg;
	Datum child_values[4] = {0};
	bool  child_nulls[4] = {0};

	ParquetColumnReader_readValue(&readers[0], &child_values[0], &child_nulls[0], HAWQ_TYPE_FLOAT8);
	ParquetColumnReader_readValue(&readers[1], &child_values[1], &child_nulls[1], HAWQ_TYPE_FLOAT8);
	ParquetColumnReader_readValue(&readers[2], &child_values[2], &child_nulls[2], HAWQ_TYPE_FLOAT8);
	ParquetColumnReader_readValue(&readers[3], &child_values[3], &child_nulls[3], HAWQ_TYPE_FLOAT8);

	Insist(child_nulls[0] == child_nulls[1] &&
		   child_nulls[2] == child_nulls[3] &&
		   child_nulls[0] == child_nulls[2]);
	
	if (child_nulls[0])
	{
		*null = true;
		return;
	}

	lseg = readers[0].geoval;
	if (lseg == NULL)
	{
		lseg = palloc(sizeof *lseg);
		readers[0].geoval = lseg;
	}
	lseg->p[0].x = DatumGetFloat8(child_values[0]);
	lseg->p[0].y = DatumGetFloat8(child_values[1]);
	lseg->p[1].x = DatumGetFloat8(child_values[2]);
	lseg->p[1].y = DatumGetFloat8(child_values[3]);

	*value = PointerGetDatum(lseg);
	*null = false;
}

void
ParquetColumnReader_readPATH(
		ParquetColumnReader readers[],
		Datum *value,
		bool *null)
{
	PATH *path;
	Datum child_values[3] = {0};	/* is_open, points.x, points.y */
	bool  child_nulls[3] = {0};
	int npts, maxnpts;

	ParquetColumnReader_readValue(&readers[0], &child_values[0], &child_nulls[0], HAWQ_TYPE_BOOL);
	if (child_nulls[0])
	{
		ParquetColumnReader_readValue(&readers[1], &child_values[1], &child_nulls[1], HAWQ_TYPE_FLOAT8);
		ParquetColumnReader_readValue(&readers[2], &child_values[2], &child_nulls[2], HAWQ_TYPE_FLOAT8);
		Insist(child_nulls[1] && child_nulls[2]);
		*null = true;
		return;
	}

	npts = 0;
	path = readers[0].geoval;
	if (path == NULL)
	{
		maxnpts = 10;
		path = (PATH *)palloc0(get_path_size(maxnpts));
	}
	else
	{
		maxnpts = path->npts;
	}
	path->closed = !DatumGetBool(child_values[0]);

	while (true)
	{
		if (npts >= maxnpts)
		{
			maxnpts *= 2;
			path = (PATH *)repalloc(path, get_path_size(maxnpts));
		}
		
		ParquetColumnReader_readValue(&readers[1], &child_values[1], &child_nulls[1], HAWQ_TYPE_FLOAT8);
		ParquetColumnReader_readValue(&readers[2], &child_values[2], &child_nulls[2], HAWQ_TYPE_FLOAT8);
		
		path->p[npts].x = DatumGetFloat8(child_values[1]);
		path->p[npts].y = DatumGetFloat8(child_values[2]);
		npts++;

		if (NextRepetitionLevel(&readers[1]) == 0)
			break;	/* no more points in this path */
	}

	path = (PATH *)repalloc(path, get_path_size(npts));
	SET_VARSIZE(path, get_path_size(npts));
	path->npts = npts;

	*value = PointerGetDatum(path);
	*null = false;

	readers[0].geoval = path;
}

void
ParquetColumnReader_readBOX(
		ParquetColumnReader readers[],
		Datum *value,
		bool *null)
{
	BOX *box;
	Datum child_values[4] = {0};
	bool  child_nulls[4] = {0};

	ParquetColumnReader_readValue(&readers[0], &child_values[0], &child_nulls[0], HAWQ_TYPE_FLOAT8);
	ParquetColumnReader_readValue(&readers[1], &child_values[1], &child_nulls[1], HAWQ_TYPE_FLOAT8);
	ParquetColumnReader_readValue(&readers[2], &child_values[2], &child_nulls[2], HAWQ_TYPE_FLOAT8);
	ParquetColumnReader_readValue(&readers[3], &child_values[3], &child_nulls[3], HAWQ_TYPE_FLOAT8);

	Insist(child_nulls[0] == child_nulls[1] &&
		   child_nulls[2] == child_nulls[3] &&
		   child_nulls[0] == child_nulls[2]);
	
	if (child_nulls[0])
	{
		*null = true;
		return;
	}

	box = readers[0].geoval;
	if (box == NULL)
	{
		box = palloc(sizeof *box);
		readers[0].geoval = box;
	}
	box->high.x = DatumGetFloat8(child_values[0]);
	box->high.y = DatumGetFloat8(child_values[1]);
	box->low.x = DatumGetFloat8(child_values[2]);
	box->low.y = DatumGetFloat8(child_values[3]);

	*value = PointerGetDatum(box);
	*null = false;
}

void
ParquetColumnReader_readPOLYGON(
		ParquetColumnReader readers[],
		Datum *value,
		bool *null)
{
	POLYGON *polygon;
	Datum child_values[6] = {0};	/* boundbox:{x1,y1,x2,y2}, points:{x,y} */
	bool  child_nulls[6] = {0};
	int npts, maxnpts;

	/*
	 * read BOX boundbox
	 */
	ParquetColumnReader_readValue(&readers[0], &child_values[0], &child_nulls[0], HAWQ_TYPE_FLOAT8);
	ParquetColumnReader_readValue(&readers[1], &child_values[1], &child_nulls[1], HAWQ_TYPE_FLOAT8);
	ParquetColumnReader_readValue(&readers[2], &child_values[2], &child_nulls[2], HAWQ_TYPE_FLOAT8);
	ParquetColumnReader_readValue(&readers[3], &child_values[3], &child_nulls[3], HAWQ_TYPE_FLOAT8);

	Insist(child_nulls[0] == child_nulls[1] &&
		   child_nulls[2] == child_nulls[3] &&
		   child_nulls[0] == child_nulls[2]);
	
	if (child_nulls[0])
	{
		ParquetColumnReader_readValue(&readers[4], &child_values[4], &child_nulls[4], HAWQ_TYPE_FLOAT8);
		ParquetColumnReader_readValue(&readers[5], &child_values[5], &child_nulls[5], HAWQ_TYPE_FLOAT8);
		Insist(child_nulls[4] && child_nulls[5]);
		*null = true;
		return;
	}

	npts = 0;
	polygon = readers[0].geoval;
	if (polygon == NULL)
	{
		maxnpts = 10;
		polygon = palloc(get_polygon_size(maxnpts));
	}
	else
	{
		maxnpts = polygon->npts;
	}

	polygon->boundbox.high.x = DatumGetFloat8(child_values[0]);
	polygon->boundbox.high.y = DatumGetFloat8(child_values[1]);
	polygon->boundbox.low.x = DatumGetFloat8(child_values[2]);
	polygon->boundbox.low.y = DatumGetFloat8(child_values[3]);

	/*
	 * read repeated points
	 */
	while (true)
	{
		if (npts >= maxnpts)
		{
			maxnpts *= 2;
			polygon = repalloc(polygon, get_polygon_size(maxnpts));
		}
		
		ParquetColumnReader_readValue(&readers[4], &child_values[4], &child_nulls[4], HAWQ_TYPE_FLOAT8);
		ParquetColumnReader_readValue(&readers[5], &child_values[5], &child_nulls[5], HAWQ_TYPE_FLOAT8);
		
		polygon->p[npts].x = DatumGetFloat8(child_values[4]);
		polygon->p[npts].y = DatumGetFloat8(child_values[5]);
		npts++;

		if (NextRepetitionLevel(&readers[4]) == 0)
			break;	/* no more points in this polygon */
	}

	polygon = repalloc(polygon, get_polygon_size(npts));
	SET_VARSIZE(polygon, get_polygon_size(npts));
	polygon->npts = npts;

	*value = PointerGetDatum(polygon);
	*null = false;

	readers[0].geoval = polygon;
}

void
ParquetColumnReader_readCIRCLE(
		ParquetColumnReader readers[],
		Datum *value,
		bool *null)
{
	CIRCLE *circle;
	Datum child_values[3] = {0};
	bool  child_nulls[3] = {0};

	ParquetColumnReader_readValue(&readers[0], &child_values[0], &child_nulls[0], HAWQ_TYPE_FLOAT8);
	ParquetColumnReader_readValue(&readers[1], &child_values[1], &child_nulls[1], HAWQ_TYPE_FLOAT8);
	ParquetColumnReader_readValue(&readers[2], &child_values[2], &child_nulls[2], HAWQ_TYPE_FLOAT8);

	Insist(child_nulls[0] == child_nulls[1] &&
		   child_nulls[1] == child_nulls[2]);
	
	if (child_nulls[0])
	{
		*null = true;
		return;
	}

	circle = readers[0].geoval;
	if (circle == NULL)
	{
		circle = palloc(sizeof *circle);
		readers[0].geoval = circle;
	}
	circle->center.x = DatumGetFloat8(child_values[0]);
	circle->center.y = DatumGetFloat8(child_values[1]);
	circle->radius = DatumGetFloat8(child_values[2]);

	*value = PointerGetDatum(circle);
	*null = false;
}
