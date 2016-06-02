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
 * cdbparquetfooterserializer.c
 *
 *  Created on: Mar 17, 2014
 *      Author: malili
 */

#include "cdb/cdbparquetfooterserializer.h"
#include "cdb/cdbparquetstoragewrite.h"
#include "access/parquetmetadata_c++/MetadataInterface.h"
#include "lib/stringinfo.h"
#include "postgres.h"

/** The deserialize part functions*/
static int
readParquetFileMetadata(
		ParquetMetadata *parquetMetadata,
		CompactProtocol *prot);

static int
readSchemaElement(
		CompactProtocol *prot,
		uint32_t schemaListSize,
		FileField_4C** pfields,
		int *fieldCount,
		int *colCount,
		int *schemaTreeNodeCount);

static int
readParquetSchema_SingleField(
		FileField_4C *pfield,
		CompactProtocol *prot,
		int *schemaIndex,
		int r,
		int d,
		int depth,
		int *colCount,
		int *schemaTreeNodeCount,
		char *parentPathInSchema);

static int
readSchemaElement_Single(
		CompactProtocol *prot,
		PrimitiveTypeName *fieldType,
		int32_t *type_length,
		RepetitionType *repetition_type,
		char **fieldName,
		int32_t *num_children);

static int
readRowGroupInfo(
		CompactProtocol *prot,
		struct BlockMetadata_4C* rowGroupInfo,
		struct FileField_4C* pfields,
		int pfieldCount);

static int
readColumnChunk(
		CompactProtocol *prot,
		struct ColumnChunkMetadata_4C *colChunk);

static int
readColumnMetadata(
		CompactProtocol *prot,
		struct ColumnChunkMetadata_4C *colChunk);

static void
assignRDFromFieldToColumnChunk(
		struct ColumnChunkMetadata_4C* columns,
		int *columnIndex,
		struct FileField_4C* pfields,
		int fieldLevelCount);

static uint32_t
readKeyValue(
		CompactProtocol *prot,
		char **key,
		char **value);

static void
freeField(struct FileField_4C *field);

/** The serialize part functions*/
static int
writeColumnMetadata(
		struct ColumnChunkMetadata_4C *columnInfo,
		CompactProtocol *prot);

static int
writeColumnChunk(
		struct ColumnChunkMetadata_4C *columnInfo,
		CompactProtocol *prot);

static int
writeSchemaElement_Single(
		CompactProtocol *prot,
		PrimitiveTypeName *fieldType,
		int32_t type_length,
		RepetitionType *repetition_type,
		char *fieldName,
		int32_t num_children);

static int
writeSchemaElement_SingleField(
		CompactProtocol *prot,
		FileField_4C * field,
		int *colIndex);

static int
writeSchemaElement(
		FileField_4C *pfields,
		int fieldCount,
		int nodeCount,
		CompactProtocol *prot);

static int
writePerviousRowGroupMetadata(
		int rowgroupcnt,
		ParquetMetadata parquetMetadata,
		CompactProtocol *read_prot,
		CompactProtocol *write_prot);

static int
writePreviousParquetFileMetadata(
		ParquetMetadata parquetMetadata,
		char *fileName,
		File file,
		int rowgroupCnt,
		CompactProtocol **read_prot);

static int
writeEndofParquetMetadata(
		ParquetMetadata parquetMetadata,
		CompactProtocol *prot);

/*
 *  Initialize deserialize footer from file, read one row group information once
 */
void initDeserializeFooter(
		File file,
		int64 footerLength,
		char *fileName,
		ParquetMetadata *parquetMetadata,
		CompactProtocol **footerProtocol)
{
	/*Initialize the footer protocol*/
	*footerProtocol =
			(struct CompactProtocol *)palloc0(sizeof(struct CompactProtocol));

	initCompactProtocol(*footerProtocol, file, fileName, footerLength, PARQUET_FOOTER_BUFFERMODE_READ);

	readParquetFileMetadata(parquetMetadata, *footerProtocol);
}

/*
 *  The initialize read method, read file metadata, but just read the first 4 parts,
 *  including version, schema information, number of rows, and rowgroup number, but
 *  doesn't read each rowgroup metadata, and keyvalue part. Read metadata of next
 *  rowgroup before reading the actual data.
 *
 *  @parquetMetadata			parquet metadata information
 *  @prot						footer protocol for reading
 */
int
readParquetFileMetadata(
		ParquetMetadata *parquetMetadata,
		CompactProtocol *prot)
{
	uint32_t xfer = 0;
	TType ftype;
	int16_t fid;

	bool isset_version = false;
	bool isset_schema = false;
	bool isset_num_rows = false;
	bool isset_row_groups = false;


	while (true) {
		xfer += readFieldBegin(prot, &ftype, &fid);
		if (ftype == T_STOP) {
			break;
		}
		switch (fid) {
		case 1:
			/* Process version*/
			if (ftype == T_I32) {
				xfer += readI32(prot, &((*parquetMetadata)->version));
				isset_version = true;
			}
			break;
		case 2:
			/* process schema - field information*/
			if (ftype == T_LIST) {
				{
					uint32_t lsize;
					TType ltype;
					xfer += readListBegin(prot, &ltype, &lsize);

					readSchemaElement(prot, lsize,
							&((*parquetMetadata)->pfield),
							&((*parquetMetadata)->fieldCount),
							&((*parquetMetadata)->colCount),
							&((*parquetMetadata)->schemaTreeNodeCount));
				}
				isset_schema = true;
			}
			break;
		case 3:
			/* process number of rows*/
			if (ftype == T_I64) {
				int64_t num_rows = 0;
				xfer += readI64(prot, &num_rows);
				(*parquetMetadata)->num_rows = num_rows;
				isset_num_rows = true;
			}
			break;
		case 4:
			/* process row group information*/
			if (ftype == T_LIST) {
				/* get row group count*/
				uint32_t lSize;
				TType etype;
				xfer += readListBegin(prot, &etype, &lSize);
				(*parquetMetadata)->blockCount = lSize;
				isset_row_groups = true;
				break;
			}
			break;
		case 5:
			/* Skip this optional field now */
			if (ftype == T_LIST) {
				xfer += skipType(prot, ftype);
			}
			break;
		case 6:
			/* Skip this optional field now */
			if (ftype == T_STRING) {
				xfer += skipType(prot, ftype);
			}
			break;
		default:
			ereport(ERROR,
				(errcode(ERRCODE_GP_INTERNAL_ERROR), errmsg("file metadata field not recognized with fid: %d", fid)));
			break;
		}

		/*hit row groups, break out the while statement*/
		if(isset_row_groups)
			break;
	}

	if (!isset_version)
		ereport(ERROR,
				(errcode(ERRCODE_GP_INTERNAL_ERROR), errmsg("file metadata version not set")));
	if (!isset_schema)
		ereport(ERROR,
				(errcode(ERRCODE_GP_INTERNAL_ERROR), errmsg("file metadata schema not set")));
	if (!isset_num_rows)
		ereport(ERROR,
				(errcode(ERRCODE_GP_INTERNAL_ERROR), errmsg("file metadata num_rows not set")));
	if (!isset_row_groups)
		ereport(ERROR,
				(errcode(ERRCODE_GP_INTERNAL_ERROR),
						errmsg("file metadata row group information not set")));
	return xfer;
}

/**
 * Read schema element from field information of parquet file, and convert it
 * to hawq-specific formats (FileField_4C). In hawq side, the field information
 * can have children member linking children together. In Parquet file side,
 * the children information is stored as a deep-traverse tree. For example,
 * for type A{A.a, A.b}, Parquet file stores it as three fields follows:
 * A, A.a, A.b, while HAWQ side stores it as just an element A, and field A has
 * two children: A.a and A.b.
 * Moreover, for the whole metadata, Parquet file side has the first element
 * which doesn't correspond to any type of HAWQ (This is a parquet specifc
 * format), so we just readout the first element out and ignore it when converting.
 *
 * @prot				The protocol reader
 * @schemaListSize		The size of list schema element in parquet file
 * @pfields				Used to store the field information of parquet tables- need convert from schemaElement
 * @fieldCount			The first level of field count, that is the number of columns in parquet table
 * @colCount			The number of columns in each row group, which is the expanded columns of pfield
 * @schemaTreeNodeCount	The count of all the nodes in schema tree, including middle nodes and leaf nodes
 */
int
readSchemaElement(
		CompactProtocol *prot,
		uint32_t schemaListSize,
		FileField_4C** pfields,
		int *fieldCount,
		int *colCount,
		int *schemaTreeNodeCount)
{
	int fieldIndex;		/*Record the index in HAWQ side: output*/
	int schemaIndex;	/*Record the index in Parquet file side: input*/
	int maxFieldCount = schemaListSize - 1;	/*The max size of output array*/
	*pfields =
			(struct FileField_4C*) palloc0(maxFieldCount * sizeof(struct FileField_4C));

	/*for root, r and d both equals 0*/
	int r = 0;
	int d = 0;
	int depth = 1;

	/*read out the first schema element 'schema', and just ignores it*/
	PrimitiveTypeName fieldType;
	int32_t type_length;
	RepetitionType repetition_type;
	char *fieldName = NULL;
	int32_t num_children;
	readSchemaElement_Single(prot, &fieldType, &type_length, &repetition_type,
			&fieldName, &num_children);

	/* Traverse the input which is a string format of the schema-tree list
	 * (deep-first traverse), and convert it to the result array "pfields".*/
	schemaIndex = 1;
	for (fieldIndex = 0; fieldIndex < maxFieldCount; ++fieldIndex) {
		/* schemaIndex will increase for embedded data type in below function*/
		readParquetSchema_SingleField(&((*pfields)[fieldIndex]), prot,
				&schemaIndex, r, d, depth, colCount, schemaTreeNodeCount,
				NULL);
		/* When input has been all processed, break out the loop*/
		if ((++schemaIndex) == schemaListSize)
			break;
	}
	*fieldCount = fieldIndex + 1;
	return 0;
}

/**
 * Read parquet schema element, and then generate file field information of hawq
 * @pfields:		hawq field
 * @prot:			the protocol for reading
 * @schemaIndex:	current parquet schema processing
 * @r:				The repetition level of parent
 * @d:				The definition level of parent
 * @depth			The depth of field in the schema hierarchy tree
 * @schemaTreeNodeCount	The total number of nodes in the schema hierarchy tree, not including root
 * @parquetPathInSchema	The path in schema of parent node
 */
int
readParquetSchema_SingleField(
		FileField_4C *pfield,
		CompactProtocol *prot,
		int *schemaIndex,
		int r,
		int d,
		int depth,
		int *colCount,
		int *schemaTreeNodeCount,
		char *parentPathInSchema)
{
	uint32_t xfer = 0;
	/**
	 * Step1: read schema element out from underlying protocol
	 */
	PrimitiveTypeName fieldType = 0;
	int32_t type_length = 0;
	RepetitionType repetition_type = 0;
	char *fieldName = NULL;
	int32_t num_children = 0;

	xfer += readSchemaElement_Single(prot, &fieldType, &type_length, &repetition_type,
			&fieldName, &num_children);

	/**
	 * Step 2: Convert the schema element to field of parquet
	 */
	/* assign name*/
	int nameLen = strlen(fieldName);
	int pathInSchemaLen;
	pfield->name = fieldName;

	/* initialize path in schema*/
	if (parentPathInSchema == NULL) {
		pathInSchemaLen = nameLen;
		pfield->pathInSchema = (char*) palloc0(pathInSchemaLen + 1);
		strcpy(pfield->pathInSchema, pfield->name);
	} else {
		pathInSchemaLen = strlen(parentPathInSchema) + nameLen + 1;
		pfield->pathInSchema = (char*) palloc0(pathInSchemaLen + 1);
		strcpy(pfield->pathInSchema, parentPathInSchema);
		strcat(pfield->pathInSchema, ":");
		strcat(pfield->pathInSchema, pfield->name);
	}

	pfield->repetitionType = repetition_type;
	/*if repetition type equals to 'repeated', r increases*/
	pfield->r = (pfield->repetitionType == REPEATED) ? r + 1 : r;
	/*if definition type equals to 'optional' or 'repeated', d increases*/
	pfield->d = (pfield->repetitionType == REQUIRED) ? d : d + 1;
	pfield->depth = depth;
	(*schemaTreeNodeCount)++;

	pfield->num_children = num_children;
	if (pfield->num_children > 0)
	{
		pfield->children =
				(struct FileField_4C*) palloc0(pfield->num_children * sizeof(struct FileField_4C));
		for (int i = 0; i < pfield->num_children; i++)
		{
			*schemaIndex = *schemaIndex + 1;
			/*the first child should sit side by the leaf itself*/
			xfer += readParquetSchema_SingleField(&(pfield->children[i]), prot,
					schemaIndex, pfield->r, pfield->d, pfield->depth + 1,
					colCount, schemaTreeNodeCount, pfield->pathInSchema);
		}
	}
	else
	{
		/*only primitive type have type name */
		pfield->type = fieldType;
		pfield->typeLength = type_length;
		(*colCount)++;
	}
	return xfer;
}

/**
 * Read single field of schema element in parquet file
 * @prot:			The protocol for reading (input)
 * @fieldType:		The type of the field
 * @type_length:	The length of the field type
 * @repetition_type:The repetition level of parent
 * @fieldName		The name of the field
 * @num_children	The children number of field
 */
int
readSchemaElement_Single(
		CompactProtocol *prot,
		PrimitiveTypeName *fieldType,
		int32_t *type_length,
		RepetitionType *repetition_type,
		char **fieldName,
		int32_t *num_children)
{
	uint32_t xfer = 0;
	TType ftype;
	int16_t fid;
	bool isset_name = false;

	readStructBegin(prot);

	while (true) {
		xfer += readFieldBegin(prot, &ftype, &fid);
		if (ftype == T_STOP) {
			break;
		}
		switch (fid) {
		case 1:
			if (ftype == T_I32) {
				int32_t val;
				xfer += readI32(prot, &val);
				*fieldType = (PrimitiveTypeName) val;
			}
			break;
		case 2:
			if (ftype == T_I32) {
				int32_t bit_length = 0;
				xfer += readI32(prot, &bit_length);
				*type_length = bit_length / 8;
			}
			break;
		case 3:
			if (ftype == T_I32) {
				int32_t ecast1;
				xfer += readI32(prot, &ecast1);
				*repetition_type = (RepetitionType) ecast1;
			}
			break;
		case 4:
			if (ftype == T_STRING) {
				isset_name = true;
				xfer += readString(prot, fieldName);
			}
			break;
		case 5:
			if (ftype == T_I32) {
				xfer += readI32(prot, num_children);
			}
			break;
		case 6:
			if (ftype == T_I32) {
				xfer += skipType(prot, ftype);
			}
			break;
		case 7:
			/* Skip this optional field now */
			if (ftype == T_I32) {
				xfer += skipType(prot, ftype);
			}
			break;
		case 8:
			/* Skip this optional field now */
			if (ftype == T_I32) {
				xfer += skipType(prot, ftype);
			}
			break;
		case 9:
			/* Skip this optional field now */
			if (ftype == T_I32) {
				xfer += skipType(prot, ftype);
			}
			break;
		default:
			ereport(ERROR,
				(errcode(ERRCODE_GP_INTERNAL_ERROR), errmsg("file metadata: schema element field not recognizd with fid: %d", fid)));
			break;
		}
	}
	readStructEnd(prot);

	if (!isset_name)
			ereport(ERROR,
					(errcode(ERRCODE_GP_INTERNAL_ERROR), errmsg(
							"file metadata schema element information not correct")));
	return xfer;
}

/**
 * Read parquet file rowgroup information, and convert it to hawq structure
 *
 * @prot			the reading protocol
 * @rowGroupInfo	the row group information
 * @pfields			the schema information of the file
 * @pfieldCount		the field count of schema
 */
int
readRowGroupInfo(
		CompactProtocol *prot,
		struct BlockMetadata_4C* rowGroupInfo,
		struct FileField_4C* pfields,
		int pfieldCount)
{
	uint32_t xfer = 0;
	TType ftype;
	int16_t fid;

	readStructBegin(prot);

	bool isset_columns = false;
	bool isset_total_byte_size = false;
	bool isset_num_rows = false;

	while (true) {
		xfer += readFieldBegin(prot, &ftype, &fid);
		if (ftype == T_STOP) {
			break;
		}
		switch (fid) {
		case 1:
			if (ftype == T_LIST)
			{
				uint32_t colChunkCnt;
				TType etype;
				xfer += readListBegin(prot, &etype, &colChunkCnt);
				rowGroupInfo->ColChunkCount = colChunkCnt;
				rowGroupInfo->columns =
						(struct ColumnChunkMetadata_4C*) palloc0(
								colChunkCnt * sizeof(struct ColumnChunkMetadata_4C));
				for (int i = 0; i < colChunkCnt; i++) {
					xfer += readColumnChunk(prot, &(rowGroupInfo->columns[i]));
				}
				isset_columns = true;
			}
			break;
		case 2:
			if (ftype == T_I64) {
				int64_t val;
				xfer += readI64(prot, &val);
				rowGroupInfo->totalByteSize = val;
				isset_total_byte_size = true;
			}
			break;
		case 3:
			if (ftype == T_I64) {
				int64_t val;
				xfer += readI64(prot, &val);
				rowGroupInfo->rowCount = val;
				isset_num_rows = true;
			}
			break;
		case 4:
			/* Skip this optional field now */
			if (ftype == T_LIST) {
				xfer += skipType(prot, ftype);
			}
			break;
		default:
			ereport(ERROR,
				(errcode(ERRCODE_GP_INTERNAL_ERROR), errmsg("file metadata: row group field not recognized with fid: %d", fid)));
			break;
		}
	}

	readStructEnd(prot);

	if (!isset_columns)
		ereport(ERROR,
				(errcode(ERRCODE_GP_INTERNAL_ERROR), errmsg("file metadata row group column chunk not set")));
	if (!isset_total_byte_size)
		ereport(ERROR,
				(errcode(ERRCODE_GP_INTERNAL_ERROR), errmsg("file metadata row group total byte size not set")));
	if (!isset_num_rows)
		ereport(ERROR,
				(errcode(ERRCODE_GP_INTERNAL_ERROR), errmsg("file metadata row group row count not set")));

	/*assign r and d of fields to column chunks*/
	int columnIndex = 0;
	assignRDFromFieldToColumnChunk(rowGroupInfo->columns, &columnIndex, pfields,
			pfieldCount);

	return xfer;
}

/**
 * read column chunk information
 */
int
readColumnChunk(
		CompactProtocol *prot,
		struct ColumnChunkMetadata_4C *colChunk)
{
	uint32_t xfer = 0;
	TType ftype;
	int16_t fid;

	readStructBegin(prot);
	bool isset_file_offset = false;

	while (true) {
		xfer += readFieldBegin(prot, &ftype, &fid);
		if (ftype == T_STOP) {
			break;
		}
		switch (fid) {
		case 1:
			if (ftype == T_STRING) {
				char *file_path;
				xfer += readString(prot, &file_path);
				colChunk->path = file_path;
			}
			break;
		case 2:
			if (ftype == T_I64) {
				xfer += readI64(prot, &(colChunk->file_offset));
				isset_file_offset = true;
			}
			break;
		case 3:
			if (ftype == T_STRUCT) {
				/*read column metadata*/
				xfer += readColumnMetadata(prot, colChunk);
			}
			break;
		default:
			ereport(ERROR,
				(errcode(ERRCODE_GP_INTERNAL_ERROR), errmsg("file metadata: column chunk field not recognized with fid: %d", fid)));
			break;
		}
	}

	readStructEnd(prot);

	if (!isset_file_offset)
		ereport(ERROR,
				(errcode(ERRCODE_GP_INTERNAL_ERROR), errmsg("file metadata: row group column chunk fileoffset not set")));
	return xfer;

}

/**
 * read column chunk metadata information
 */
int
readColumnMetadata(
		CompactProtocol *prot,
		struct ColumnChunkMetadata_4C *colChunk)
{
	uint32_t xfer = 0;
	TType ftype;
	int16_t fid;
	readStructBegin(prot);

	bool isset_type = false;
	bool isset_encodings = false;
	bool isset_path_in_schema = false;
	bool isset_codec = false;
	bool isset_num_values = false;
	bool isset_total_uncompressed_size = false;
	bool isset_total_compressed_size = false;
	bool isset_data_page_offset = false;

	while (true) {
		xfer += readFieldBegin(prot, &ftype, &fid);
		if (ftype == T_STOP) {
			break;
		}
		switch (fid) {
		case 1:
			if (ftype == T_I32) {
				int32_t type;
				xfer += readI32(prot, &type);
				colChunk->type = (PrimitiveTypeName) type;
				isset_type = true;
			}
			break;
		case 2:
			if (ftype == T_LIST) {
				uint32_t encodingCount;
				TType etype;
				xfer += readListBegin(prot, &etype, &encodingCount);
				colChunk->EncodingCount = encodingCount;
				colChunk->pEncodings =
						(enum Encoding *) palloc0(sizeof(enum Encoding) * encodingCount);
				for (int i = 0; i < encodingCount; i++) {
					int32_t encoding;
					xfer += readI32(prot, &encoding);
					colChunk->pEncodings[i] = (enum Encoding) encoding;
				}
				isset_encodings = true;
			}
			break;
		case 3:
			if (ftype == T_LIST) {
				{
					/*process path in schema, setting colchunk->depth and colchunk->pathInSchema*/
					TType etype;
					uint32_t lsize;
					StringInfoData colNameBuf;

					xfer += readListBegin(prot, &etype, &lsize);
					colChunk->depth = lsize;
					initStringInfo(&colNameBuf);
					char *path_in_schema;
					for (int i = 0; i < lsize - 1; i++) {
						xfer += readString(prot, &path_in_schema);
						appendStringInfo(&colNameBuf, "%s:", path_in_schema);
						pfree(path_in_schema);
					}
					xfer += readString(prot, &path_in_schema);
					appendStringInfo(&colNameBuf, "%s", path_in_schema);

					colChunk->pathInSchema = colNameBuf.data;
					colChunk->colName = path_in_schema;
				}
				isset_path_in_schema = true;
			}
			break;
		case 4:
			if (ftype == T_I32) {
				int32_t compresscode;
				xfer += readI32(prot, &compresscode);
				colChunk->codec = (enum CompressionCodecName) compresscode;
				isset_codec = true;
			}
			break;
		case 5:
			if (ftype == T_I64) {
				int64_t valCnt;
				xfer += readI64(prot, &valCnt);
				colChunk->valueCount = valCnt;
				isset_num_values = true;
			}
			break;
		case 6:
			if (ftype == T_I64) {
				xfer += readI64(prot, &(colChunk->totalUncompressedSize));
				isset_total_uncompressed_size = true;
			}
			break;
		case 7:
			if (ftype == T_I64) {
				xfer += readI64(prot, &(colChunk->totalSize));
				isset_total_compressed_size = true;
			}
			break;
		case 8:
			if (ftype == T_LIST) {
				xfer += skipType(prot, ftype);
			}
			break;
		case 9:
			if (ftype == T_I64) {
				xfer += readI64(prot, &(colChunk->firstDataPage));
				isset_data_page_offset = true;
			}
			break;
		case 10:
			if (ftype == T_I64) {
				xfer += skipType(prot, ftype);
			}
			break;
		case 11:
			if (ftype == T_I64) {
				xfer += skipType(prot, ftype);
			}
			break;
		case 12:
			/* Skip this optional field now */
			if (ftype == T_STRUCT) {
				xfer += skipType(prot, ftype);
			}
			break;
		case 13:
			/* Skip this optional field now */
			if (ftype == T_LIST) {
				xfer += skipType(prot, ftype);
			}
			break;
		default:
			ereport(ERROR,
				(errcode(ERRCODE_GP_INTERNAL_ERROR), errmsg("file metadata: column metadata field not recognized with fid: %d", fid)));
			break;
		}
	}
	readStructEnd(prot);

	if (!isset_type)
		ereport(ERROR,
				(errcode(ERRCODE_GP_INTERNAL_ERROR), errmsg("file metadata: row group column chunk type not set")));
	if (!isset_encodings)
		ereport(ERROR,
				(errcode(ERRCODE_GP_INTERNAL_ERROR), errmsg("file metadata: row group column chunk encoding not set")));
	if (!isset_path_in_schema)
		ereport(ERROR,
				(errcode(ERRCODE_GP_INTERNAL_ERROR), errmsg("file metadata: row group column chunk path_in_schema not set")));
	if (!isset_codec)
		ereport(ERROR,
				(errcode(ERRCODE_GP_INTERNAL_ERROR), errmsg("file metadata: row group column chunk compression code not set")));
	if (!isset_num_values)
		ereport(ERROR,
				(errcode(ERRCODE_GP_INTERNAL_ERROR), errmsg("file metadata: row group column chunk value number not set")));
	if (!isset_total_uncompressed_size)
		ereport(ERROR,
				(errcode(ERRCODE_GP_INTERNAL_ERROR), errmsg("file metadata: row group column chunk total uncompressed size not set")));
	if (!isset_total_compressed_size)
		ereport(ERROR,
				(errcode(ERRCODE_GP_INTERNAL_ERROR), errmsg("file metadata: row group column chunk total compressed size not set")));
	if (!isset_data_page_offset)
		ereport(ERROR,
				(errcode(ERRCODE_GP_INTERNAL_ERROR), errmsg("file metadata: row group column chunk first data page not set")));
	return xfer;
}

/**
 * Assign the r and d value of column chunks, from pfields to column chunks
 */
void
assignRDFromFieldToColumnChunk(
		struct ColumnChunkMetadata_4C* columns,
		int *columnIndex,
		struct FileField_4C* pfields,
		int fieldLevelCount)
{
	for (int i = 0; i < fieldLevelCount; i++)
	{
		if (pfields[i].num_children == 0)
		{
			columns[*columnIndex].r = pfields[i].r;
			columns[*columnIndex].d = pfields[i].d;
			(*columnIndex)++;
		}
		else
		{
			assignRDFromFieldToColumnChunk(columns, columnIndex,
					pfields[i].children, pfields[i].num_children);
		}
	}
}

/**
 * Read key value part out
 */
uint32_t readKeyValue(
		CompactProtocol *prot,
		char **key,
		char **value)
{
	uint32_t xfer = 0;
	TType ftype;
	int16_t fid;
	bool isset_key = false;

	readStructBegin(prot);

	while (true) {
		readFieldBegin(prot, &ftype, &fid);
		if (ftype == T_STOP)
			break;
		switch (fid) {
		case 1:
			if (ftype == T_STRING) {
				xfer += readString(prot, key);
				isset_key = true;
			}
			break;
		case 2:
			if (ftype == T_STRING) {
				xfer += readString(prot, value);
			}
			break;
		default:
			ereport(ERROR,
				(errcode(ERRCODE_GP_INTERNAL_ERROR), errmsg("file metadata: key value field not recognized with fid: %d", fid)));
			break;
		}
	}
	readStructEnd(prot);

	if (!isset_key)
		ereport(ERROR, (errcode(ERRCODE_GP_INTERNAL_ERROR), errmsg("file metadata key value: key not set")));
	return xfer;
}

/*
 * Get next row group metadata information
 *
 * Return value: whether there exists new row group metadata information
 *
 */
void
readNextRowGroupInfo(
		ParquetMetadata parquetMetadata,
		CompactProtocol *prot)
{
	/*free current rowgroup info and read next rowgroup metadata*/
	if(parquetMetadata->currentBlockMD != NULL){
		freeRowGroupInfo(parquetMetadata->currentBlockMD);
		parquetMetadata->currentBlockMD = NULL;
	}

	/*read next row group metadata out*/
	parquetMetadata->currentBlockMD =
			(struct BlockMetadata_4C *) palloc0(sizeof(struct BlockMetadata_4C));
	readRowGroupInfo(prot, parquetMetadata->currentBlockMD,
			parquetMetadata->pfield, parquetMetadata->fieldCount);
}


/**
 * End of serialize footer, read the key/value part out if exists, then read the stop variable
 */
void
endDeserializerFooter(
		ParquetMetadata parquetMetadata,
		CompactProtocol **prot)
{
	TType ftype;
	int16_t fid;
	int xfer = 0;


	while (true) {
		xfer += readFieldBegin(*prot, &ftype, &fid);
		if (ftype == T_STOP) {
			break;
		}
		switch (fid) {
		case 5:
			if (ftype == T_LIST) {
				uint32_t lsize;
				TType etype;
				xfer += readListBegin(*prot, &etype, &lsize);

				for (int i = 0; i < lsize; i++) {
					char *key = NULL;
					char *value = NULL;
					xfer += readKeyValue(*prot, &key, &value);

					if ((key != NULL) && (strcmp(key, "hawq.schema") == 0)) {
						int schemaLen = strlen(value);
						parquetMetadata->hawqschemastr =
								(char*) palloc0(schemaLen + 1);
						strcpy(parquetMetadata->hawqschemastr, value);
					}
				}
			}
			break;
		case 6:
			/* Skip this optional field now */
			if (ftype == T_STRING) {
				xfer += skipType(*prot, ftype);
			}
			break;
		default:
			ereport(ERROR,
					(errcode(ERRCODE_GP_INTERNAL_ERROR), errmsg("incorrect file metadata format with fid: %d", fid)));
			break;
		}
	}

	freeFooterProtocol(*prot);
}

void freeFooterProtocol(CompactProtocol *protocol) {
	if (protocol) {
		freeCompactProtocol(protocol);
		pfree(protocol);
	}
}

/**
 * Write part functions
 */
int
writeColumnMetadata(
		struct ColumnChunkMetadata_4C *columnInfo,
		CompactProtocol *prot)
{
	uint32_t xfer = 0;
	char *elemPath = NULL;
	const char *delim = ":";
	Assert(NULL != columnInfo->pathInSchema);
	char path[strlen(columnInfo->pathInSchema) + 1];

	xfer += writeStructBegin(prot);

	/*write out type*/
	xfer += writeFieldBegin(prot, T_I32, 1);
	xfer += writeI32(prot, columnInfo->type);

	/*write out encoding*/
	xfer += writeFieldBegin(prot, T_LIST, 2);
	xfer += writeListBegin(prot, T_I32, columnInfo->EncodingCount);
	for (int i = 0; i < columnInfo->EncodingCount; i++) {
		xfer += writeI32(prot, (int32_t)(columnInfo->pEncodings[i]));
	}

	/*write out path_in_schema*/
	xfer += writeFieldBegin(prot, T_LIST, 3);
	xfer += writeListBegin(prot, T_STRING, columnInfo->depth);
	strcpy(path, columnInfo->pathInSchema);

	elemPath = strtok(path, delim);
	if (elemPath == NULL) {
		ereport(ERROR,
				(errcode(ERRCODE_GP_INTERNAL_ERROR),
						errmsg("file metadata column metadata(path_in_schema) not correct")));
	}
	xfer += writeString(prot, elemPath, strlen(elemPath));
	for (int i = 1; i < columnInfo->depth; i++) {
		elemPath = strtok(NULL, delim);
		if (elemPath == NULL) {
			ereport(ERROR,
					(errcode(ERRCODE_GP_INTERNAL_ERROR),
							errmsg("file metadata column metadata(path_in_schema) not correct")));
		}
		xfer += writeString(prot, elemPath, strlen(elemPath));
	}

	/*write out codec*/
	xfer += writeFieldBegin(prot, T_I32, 4);
	xfer += writeI32(prot, (int32_t)columnInfo->codec);

	/*write out num of values*/
	xfer += writeFieldBegin(prot, T_I64, 5);
	xfer += writeI64(prot, (int64_t)columnInfo->valueCount);

	/*write total uncompressed size*/
	xfer += writeFieldBegin(prot, T_I64, 6);
	xfer += writeI64(prot, columnInfo->totalUncompressedSize);

	/*write out total compressed size*/
	xfer += writeFieldBegin(prot, T_I64, 7);
	xfer += writeI64(prot, columnInfo->totalSize);

	/*write out key value metadata.*/
	/*There's no key value metadata for parquet storage, don't need to write it out*/

	/*write out data page offset*/
	xfer += writeFieldBegin(prot, T_I64, 9);
	xfer += writeI64(prot, columnInfo->firstDataPage);

	/*write out index page offset and dictionary page offset. No need to write currently*/

	/*write out field stop identifier*/
	xfer += writeFieldStop(prot);
	xfer += writeStructEnd(prot);

	return xfer;
}

int
writeColumnChunk(
		struct ColumnChunkMetadata_4C *columnInfo,
		CompactProtocol *prot)
{
	uint32_t xfer = 0;
	xfer += writeStructBegin(prot);

	/*write out column path*/
	if(columnInfo->path != NULL)
	{
		xfer += writeFieldBegin(prot, T_STRING, 1);
		xfer += writeString(prot, columnInfo->path, strlen(columnInfo->path));
	}

	/*write out file offset*/
	xfer += writeFieldBegin(prot, T_I64, 2);
	xfer += writeI64(prot, columnInfo->file_offset);

	/*write out column metadata*/
	xfer += writeFieldBegin(prot, T_STRUCT, 3);
	xfer += writeColumnMetadata(columnInfo, prot);

	xfer += writeFieldStop(prot);
	xfer += writeStructEnd(prot);

	return xfer;
}


int
writeRowGroupInfo(
		struct BlockMetadata_4C* rowGroupInfo,
		CompactProtocol *prot)
{
	uint32_t xfer = 0;
	xfer += writeStructBegin(prot);

	/*write out the column chunk metadata*/
	xfer += writeFieldBegin(prot, T_LIST, 1);
	xfer += writeListBegin(prot, T_STRUCT, rowGroupInfo->ColChunkCount);
	for(int i = 0; i < rowGroupInfo->ColChunkCount; i++){
		/*write out each column chunk metadata*/
		xfer += writeColumnChunk(&(rowGroupInfo->columns[i]), prot);
	}

	/*write out total byte size*/
	xfer += writeFieldBegin(prot, T_I64, 2);
	xfer += writeI64(prot, rowGroupInfo->totalByteSize);

	/*write out num_rows*/
	xfer += writeFieldBegin(prot, T_I64, 3);
	xfer += writeI64(prot, rowGroupInfo->rowCount);

	xfer += writeFieldStop(prot);
	xfer += writeStructEnd(prot);
	return xfer;
}

int
writeSchemaElement_Single(
		CompactProtocol *prot,
		PrimitiveTypeName *fieldType,
		int32_t type_length,
		RepetitionType *repetition_type,
		char *fieldName,
		int32_t num_children)
{
	uint32_t xfer = 0;
	xfer += writeStructBegin(prot);

	/*write out type*/
	if(fieldType){
		xfer += writeFieldBegin(prot, T_I32, 1);
		xfer += writeI32(prot, *(int32_t*)fieldType);
	}

	/*write out type length*/
	if(type_length != 0){
		xfer += writeFieldBegin(prot, T_I32, 2);
		xfer += writeI32(prot, type_length * 8);
	}

	/*write out repetition type. Is there repetition type for root??? Need verify*/
	if(repetition_type)
	{
		xfer += writeFieldBegin(prot, T_I32, 3);
		xfer += writeI32(prot, *repetition_type);
	}

	/*write out name*/
	xfer += writeFieldBegin(prot, T_STRING, 4);
	xfer += writeString(prot, fieldName, strlen(fieldName));

	/*write out number of children*/
	if(num_children != 0)
	{
		xfer += writeFieldBegin(prot, T_I32, 5);
		xfer += writeI32(prot, num_children);
	}

	/*no need to write out converted type, since there is no converted type
	 *in hawq parquet implemention.*/

	xfer += writeFieldStop(prot);
	xfer += writeStructEnd(prot);
	return xfer;
}

int
writeSchemaElement_SingleField(
		CompactProtocol *prot,
		FileField_4C * field,
		int *colIndex)
{
	uint32_t xfer = 0;
	(*colIndex) = (*colIndex) + 1;
	/*if have children, write out the field, then expand to its children*/
	if(field->num_children > 0){
		xfer += writeSchemaElement_Single(prot, NULL, 0, &(field->repetitionType), field->name, field->num_children);
		for(int i = 0; i < field->num_children; i++)
		{
			xfer += writeSchemaElement_SingleField(prot, &(field->children[i]), colIndex);
		}
	}
	else
	{
		/*if no children, directly write out the field*/
		xfer += writeSchemaElement_Single(prot, &(field->type), field->typeLength, &(field->repetitionType),
				field->name, field->num_children);
	}

	return xfer;
}

int
writeSchemaElement(
		FileField_4C *pfields,
		int fieldCount,
		int nodeCount,
		CompactProtocol *prot)
{
	uint32_t xfer = 0;

	/*write out schema element 0*/
	xfer += writeSchemaElement_Single(prot, NULL, 0, NULL, "schema", fieldCount);

	int colIndex = 0;
	for (int i = 0; i < fieldCount; ++i) {
		FileField_4C *field = &pfields[i];
		xfer += writeSchemaElement_SingleField(prot, field, &colIndex);
	}

	/*colIndex should equals to colCount after processing all the columns*/
	if (nodeCount != colIndex) {
		ereport(ERROR,
						(errcode(ERRCODE_GP_INTERNAL_ERROR),
								errmsg("file metadata schema element not correct")));
	}

	return xfer;
}

/**
 * Deserialize previous row group metadata: copy it from previous location to the new footer
 */
int
writePerviousRowGroupMetadata(
		int rowgroupcnt,
		ParquetMetadata parquetMetadata,
		CompactProtocol *read_prot,
		CompactProtocol *write_prot){
	uint32_t xfer = 0;
	/*for each row group metadata, read it through the read protocol, and then write it out
	 *to write protocol*/
	for(int i = 0; i < rowgroupcnt; i++)
	{
		readNextRowGroupInfo(parquetMetadata, read_prot);
		xfer += writeRowGroupInfo(parquetMetadata->currentBlockMD, write_prot);
	}

	return xfer;
}

/**
 * Write out begin of parquet file metadata (part before rowgroup),including version,
 * schema, and num_rows
 */
int
writePreviousParquetFileMetadata(
		ParquetMetadata parquetMetadata,
		char *fileName,
		File file,
		int rowgroupCnt,
		CompactProtocol **read_prot)
{
	uint32_t xfer = 0;
	CompactProtocol *write_prot = (struct CompactProtocol *) palloc0(sizeof(struct CompactProtocol));

	initCompactProtocol(write_prot, file, fileName, -1,
			PARQUET_FOOTER_BUFFERMODE_WRITE);

	xfer += writeStructBegin(write_prot);

	/*write out version*/
	xfer += writeFieldBegin(write_prot, T_I32, 1);
	xfer += writeI32(write_prot, (int32_t)parquetMetadata->version);

	/*write out schema*/
	xfer += writeFieldBegin(write_prot, T_LIST, 2);
	xfer += writeListBegin(write_prot, T_STRUCT, parquetMetadata->schemaTreeNodeCount + 1);
	xfer += writeSchemaElement(parquetMetadata->pfield, parquetMetadata->fieldCount, parquetMetadata->schemaTreeNodeCount, write_prot);

	/*write out number of rows*/
	xfer += writeFieldBegin(write_prot, T_I64, 3);
	xfer += writeI64(write_prot, (int64_t)parquetMetadata->num_rows);

	/*write out rowgroup size*/
	xfer += writeFieldBegin(write_prot, T_LIST, 4);
	xfer += writeListBegin(write_prot, T_STRUCT, parquetMetadata->blockCount);

	/*write out the previous row group metadata information before deserialize*/
	writePerviousRowGroupMetadata(rowgroupCnt, parquetMetadata, *read_prot, write_prot);

	/*append the first part of footer to file*/
	xfer = appendFooterBufferTempData(file, write_prot->footerProcessor);

	/*free the write protocol for first part of file*/
	freeCompactProtocol(write_prot);
	pfree(write_prot);

	/*if there is previous metadata, should end footer serializer*/
	if(rowgroupCnt != 0)
		endDeserializerFooter(parquetMetadata, read_prot);

	return xfer;
}


int
writeEndofParquetMetadata(
		ParquetMetadata parquetMetadata,
		CompactProtocol *prot)
{
	uint32_t xfer = 0;
	/** write out key value metadata */
	/*hack here. The last field is rowgroup, the id should be 4*/
	setLastFieldId(prot, 4);
	xfer += writeFieldBegin(prot, T_LIST, 5);
	xfer += writeListBegin(prot, T_STRUCT, 1);
	xfer += writeStructBegin(prot);
	/*write out key*/
	xfer += writeFieldBegin(prot, T_STRING, 1);
	xfer += writeString(prot, "hawq.schema", strlen("hawq.schema"));
	/*write out value*/
	xfer += writeFieldBegin(prot, T_STRING, 2);
	if (parquetMetadata->hawqschemastr == NULL)
		parquetMetadata->hawqschemastr = generateHAWQSchemaStr(parquetMetadata->pfield,
							parquetMetadata->fieldCount);
	xfer += writeString(prot, parquetMetadata->hawqschemastr, strlen(parquetMetadata->hawqschemastr));
	/*write out end of key value*/
	xfer += writeFieldStop(prot);
	xfer += writeStructEnd(prot);

	/*write out the file metadata field end identifier*/
	xfer += writeFieldStop(prot);
	xfer += writeStructEnd(prot);

	return xfer;
}

/*
 *  Initialize DeSerialize footer to file
 */
void
initSerializeFooter(
		CompactProtocol **footerProtocol,
		char *fileName){

	*footerProtocol = (struct CompactProtocol *)
		palloc0(sizeof(struct CompactProtocol));

	initCompactProtocol(*footerProtocol, -1, fileName, -1, PARQUET_FOOTER_BUFFERMODE_WRITE);
}

/* End deserialize footer to file, copy from temporary file to data file*/
int
endSerializeFooter(
		CompactProtocol **read_prot,
		CompactProtocol **write_prot,
		char	*fileName,
		File	file,
		ParquetMetadata parquetMetadata,
		int		rowgroup_cnt)
{
	int footerLength = 0;

	/*firstly write out previous parquet file metadata*/
	footerLength += writePreviousParquetFileMetadata(parquetMetadata, fileName,
			file, rowgroup_cnt, read_prot);

	/*secondly write out the new added rowgroup metadata and remaining keyvalue part*/
	writeEndofParquetMetadata(parquetMetadata, *write_prot);
	footerLength += appendFooterBufferTempData(file, (*write_prot)->footerProcessor);

	/*free the compact write protocol*/
	freeCompactProtocol(*write_prot);
	pfree(*write_prot);

	return footerLength;
}


/**
 * pfree the current blockMetadata
 */
void
freeRowGroupInfo(struct BlockMetadata_4C *blockMetadata) {
	/* pfree column chunk information*/
	for (int j = 0; j < blockMetadata->ColChunkCount; j++) {
		struct ColumnChunkMetadata_4C *colChunk = &(blockMetadata->columns[j]);
		if (colChunk->path != NULL)
			pfree(colChunk->path);
		if (colChunk->colName != NULL)
			pfree(colChunk->colName);
		if (colChunk->pathInSchema != NULL)
			pfree(colChunk->pathInSchema);
		if (colChunk->pEncodings != NULL)
			pfree(colChunk->pEncodings);
	}
	pfree(blockMetadata->columns);
	blockMetadata->columns = NULL;
	pfree(blockMetadata);
}

/**
 * Free parquet metadata
 */
void freeParquetMetadata(ParquetMetadata parquetMetadata) {
	/* pfree field information*/
	for (int i = 0; i < parquetMetadata->fieldCount; i++) {
		freeField(&(parquetMetadata->pfield[i]));
	}
	pfree(parquetMetadata->pfield);

	/* pfree hawqschemastr*/
	if (parquetMetadata->hawqschemastr != NULL)
		pfree(parquetMetadata->hawqschemastr);

	if(parquetMetadata->currentBlockMD != NULL)
	{
		freeRowGroupInfo(parquetMetadata->currentBlockMD);
	}

	if (parquetMetadata->estimateChunkSizes != NULL)
	{
		pfree(parquetMetadata->estimateChunkSizes);
	}
	pfree(parquetMetadata);
}

/**
 * Free field information
 */
void
freeField(struct FileField_4C *field) {
	if (field->name != NULL)
		pfree(field->name);
	if (field->pathInSchema != NULL)
		pfree(field->pathInSchema);
	if (field->num_children > 0){
		for (int i = 0; i < field->num_children; i++) {
			freeField(&(field->children[i]));
		}
		pfree(field->children);
	}
}
