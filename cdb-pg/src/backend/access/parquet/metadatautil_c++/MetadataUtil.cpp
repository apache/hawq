/*
 * getMetadata.cpp
 *
 *  Created on: Jun 25, 2013
 *      Author: malili
 */

#include <fcntl.h>
#include <vector>
#include <string.h>
#include "MetadataUtil.h"
#include "parquet_types.h"

extern "C" {
#include "postgres.h"
#include "utils/palloc.h"
#include "lib/stringinfo.h"
}
using namespace hawq;

ThriftSerializer * MetadataUtil::thriftSerializer  = new ThriftSerializer(true, 1024);

/*
 * Init MetadataUtil for read
 */
void MetadataUtil::readMetadataInit() {
	bool compact = true;
	this->thriftDeserializer = new ThriftDeserializer(compact);
}

/*
 * Read from buffer, and get FileMetadata
 *
 * buf:		buffer to be read
 * len:		buffer length to be read
 * compact:	whether use thrift compact protocol
 * */

int MetadataUtil::readFileMetadata(uint8_t *buf, uint32_t len, bool compact,
		ParquetMetadata_4C* fileMetadata) {
	parquet::FileMetaData file_metadata_;
	if (this->thriftDeserializer->DeserializeThriftMsg(buf, &len, compact,
			&file_metadata_) < 0)
		return -1;

	convertFileMetadata(file_metadata_, fileMetadata);
	return 0;
}

int fromParquetEncoding(std::vector<parquet::Encoding::type> encodings,
		Encoding** pEncodings, int& encodingCount) {
	encodingCount = encodings.size();
	*pEncodings =
			(enum Encoding*) palloc0(encodingCount * sizeof(enum Encoding));
	for (int i = 0; i < encodingCount; i++) {
		(*pEncodings)[i] = (Encoding) encodings[i];
	}
	return 0;
}

char *fromParquetPathInSchema(std::vector<std::string> path_in_schema, int &depth){
	StringInfo colNameBuf = makeStringInfo();
	depth = path_in_schema.size();
	for(int i = 0; i < depth - 1; i++){
		appendStringInfo(colNameBuf, "%s:", path_in_schema[i].c_str());
	}
	appendStringInfo(colNameBuf, "%s", path_in_schema[depth-1].c_str());
	return colNameBuf->data;
}

int fromParquetColumnChunk(std::vector<parquet::ColumnChunk> col_chunks,
		ColumnChunkMetadata_4C** pColChunk, int& colChunkCount) {
	colChunkCount = col_chunks.size();
	*pColChunk = (struct ColumnChunkMetadata_4C*) palloc0(
			colChunkCount * sizeof(struct ColumnChunkMetadata_4C));
	for (int i = 0; i < colChunkCount; i++) {
		(*pColChunk)[i].firstDataPage =
				col_chunks[i].meta_data.data_page_offset;
		(*pColChunk)[i].codec =
				(CompressionCodecName) col_chunks[i].meta_data.codec;
		(*pColChunk)[i].file_offset = col_chunks[i].file_offset;
		(*pColChunk)[i].totalSize =
				col_chunks[i].meta_data.total_compressed_size;
		(*pColChunk)[i].totalUncompressedSize =
				col_chunks[i].meta_data.total_uncompressed_size;
		(*pColChunk)[i].valueCount = col_chunks[i].meta_data.num_values;
		(*pColChunk)[i].type = (PrimitiveTypeName) col_chunks[i].meta_data.type;
		(*pColChunk)[i].pathInSchema = fromParquetPathInSchema(col_chunks[i].meta_data.path_in_schema,
				(*pColChunk)[i].depth);
		int colLen = strlen(col_chunks[i].meta_data.path_in_schema
				[(*pColChunk)[i].depth - 1].c_str());
		(*pColChunk)[i].colName = (char*)palloc0(colLen + 1);
		strcpy((*pColChunk)[i].colName, col_chunks[i].meta_data.path_in_schema
				[(*pColChunk)[i].depth - 1].c_str());

		fromParquetEncoding(col_chunks[i].meta_data.encodings,
				&((*pColChunk)[i].pEncodings), (*pColChunk)[i].EncodingCount);
	}
	return 0;
}

/**
 * From parquet schema, generate file field information of hawq
 * @pfields:		hawq field
 * @schema:			parquet schema
 * @schemaIndex:	current parquet schema processing
 * @r:				The repetition level of parent
 * @d:				The definition level of parent
 */
int fromParquetSchema_SingleField(FileField_4C *pfield,
		vector<parquet::SchemaElement> schema, int *schemaIndex,
		int r, int d, int depth, int& colCount, int &schemaTreeNodeCount,
		char *parentPathInSchema) {
	/*initialize name*/
	int nameLen = schema[*schemaIndex].name.length();
	int pathInSchemaLen;
	pfield->name = (char*) palloc0(nameLen + 1);
	strcpy(pfield->name, schema[*schemaIndex].name.c_str());

	/*initialize path in schema*/
	if(parentPathInSchema == NULL){
		pathInSchemaLen = nameLen;
		pfield->pathInSchema = (char*)palloc0(pathInSchemaLen + 1);
		strcpy(pfield->pathInSchema, pfield->name);
	}
	else{
		pathInSchemaLen = strlen(parentPathInSchema) + nameLen + 1;
		pfield->pathInSchema = (char*)palloc0(pathInSchemaLen + 1);
		strcpy(pfield->pathInSchema, parentPathInSchema);
		strcat(pfield->pathInSchema, ":");
		strcat(pfield->pathInSchema, pfield->name);
	}

	pfield->repetitionType =
			(RepetitionType) schema[*schemaIndex].repetition_type;
	/* if repetition type equals to 'repeated', r increases*/
	pfield->r = (pfield->repetitionType == REPEATED) ? r + 1 : r;
	/* if definition type equals to 'optional' or 'repeated', d increases*/
	pfield->d = (pfield->repetitionType == REQUIRED) ? d : d + 1;
	pfield->depth = depth;
	schemaTreeNodeCount++;

	pfield->num_children = schema[*schemaIndex].num_children;
	if (pfield->num_children > 0) {
		pfield->children =
				(struct FileField_4C*) palloc0(pfield->num_children * sizeof(struct FileField_4C));
		for (int i = 0; i < pfield->num_children; i++) {
			*schemaIndex = *schemaIndex + 1;
			/*the first child should sit side by the leaf itself*/
			fromParquetSchema_SingleField(&(pfield->children[i]), schema,
					schemaIndex, pfield->r, pfield->d, pfield->depth + 1, colCount,
					schemaTreeNodeCount, pfield->pathInSchema);
		}
	} else {
		/* only primitive type have type name*/
		pfield->type = (PrimitiveTypeName) schema[*schemaIndex].type;
		pfield->typeLength = schema[*schemaIndex].type_length;
		colCount++;
	}
	return 0;
}

/**
 * The nested parquet schema types is stored as a tree, should traverse that in depth-first
 * @schema:		parquet schema
 * @pfields:	hawq schema
 * @fieldCount:	hawq type number, the count for an embedded type is 1.
 */
int fromParquetSchema(vector<parquet::SchemaElement> schema,
		FileField_4C** pfields, int& fieldCount, int& colCount, int &schemaTreeNodeCount) {
	int maxFieldCount = schema.size() - 1;
	*pfields =
			(struct FileField_4C*) palloc0(maxFieldCount * sizeof(struct FileField_4C));

	int fieldIndex = 0;
	int r = 0;	/*for the root, r and d both equals 0*/
	int d = 0;
	int depth = 1;
	/* ignore the first one ,i begin with 1 */
	for (int i = 1; i < (int)schema.size(); i++) {
		fromParquetSchema_SingleField(&((*pfields)[fieldIndex]), schema, &i,
				r, d, depth, colCount, schemaTreeNodeCount, NULL);
		fieldIndex++;
		if(fieldIndex > maxFieldCount){
			/*should never get here*/
			return -1;
		}
	}

	fieldCount = fieldIndex;
	return 0;
}

/**
 * Assign the r and d value of column chunks, from pfields to column chunks
 */
void assignRDFromFieldToColumnChunk(struct ColumnChunkMetadata_4C* columns,
		int &columnIndex, struct FileField_4C* pfields, int fieldLevelCount)
{
	for (int i = 0; i < fieldLevelCount; i++)
    {
		if (pfields[i].num_children == 0)
        {
			columns[columnIndex].r = pfields[i].r;
			columns[columnIndex].d = pfields[i].d;
			columnIndex++;
		}
		else
        {
			assignRDFromFieldToColumnChunk(columns, columnIndex, pfields[i].children, pfields[i].num_children);
		}
	}
}

int fromParquetRowGroup(std::vector<parquet::RowGroup> row_groups,
		BlockMetadata_4C** pblocks, int& blockCount, int& maxBlockCount,
		struct FileField_4C* pfields, int pfieldCount) {
	blockCount = row_groups.size();
	maxBlockCount = blockCount;
	for (int i = 0; i < blockCount; i++) {
		pblocks[i] =
				(struct BlockMetadata_4C*) palloc0(sizeof(struct BlockMetadata_4C));
		pblocks[i]->rowCount = row_groups[i].num_rows;
		pblocks[i]->totalByteSize = row_groups[i].total_byte_size;
		fromParquetColumnChunk(row_groups[i].columns, &(pblocks[i]->columns),
				pblocks[i]->ColChunkCount);
		/*assign r and d of fields to column chunks*/
		int columnIndex = 0;
		assignRDFromFieldToColumnChunk(pblocks[i]->columns, columnIndex,
				pfields, pfieldCount);
	}

	return 0;
}


void MetadataUtil::convertFileMetadata(
		parquet::FileMetaData& parquetFileMetadata,
		ParquetMetadata_4C* hawqFileMetadata) {
	int iret = fromParquetSchema(parquetFileMetadata.schema,
			&(hawqFileMetadata->pfield), hawqFileMetadata->fieldCount,
			hawqFileMetadata->colCount, hawqFileMetadata->schemaTreeNodeCount);

	hawqFileMetadata->version = parquetFileMetadata.version;
	hawqFileMetadata->num_rows = parquetFileMetadata.num_rows;

	hawqFileMetadata->pBlockMD =
			(struct BlockMetadata_4C**) palloc0(
					parquetFileMetadata.row_groups.size() * sizeof(struct BlockMetadata_4C*));

	iret = fromParquetRowGroup(parquetFileMetadata.row_groups,
			hawqFileMetadata->pBlockMD, hawqFileMetadata->blockCount,
			hawqFileMetadata->maxBlockCount, hawqFileMetadata->pfield,
			hawqFileMetadata->fieldCount);

	/*read hawq schema str*/
	fromParquetKeyValue(parquetFileMetadata.key_value_metadata,
			&hawqFileMetadata->hawqschemastr);

	return;
}

int MetadataUtil::fromParquetKeyValue(std::vector<parquet::KeyValue> keyValue,
		char **hawqschemastr) {
	for (int i = 0; i < (int) keyValue.size(); i++) {
		if (strcmp(keyValue[i].key.c_str(), "hawq.schema") == 0) {
			int schemaLen = strlen(keyValue[i].value.c_str());
			*hawqschemastr = (char*) palloc0(schemaLen + 1);
			strcpy(*hawqschemastr, keyValue[i].value.c_str());
		}
	}
	return 0;
}

/*
 * Write filemetadata to buffer
 *
 * buf:		buffer to be write
 * len:		buffer length to be read
 * compact:	whether use thrift compact protocol
 * */

int MetadataUtil::writeFileMetadata(uint8_t **buf, uint32_t *len,
		ParquetMetadata_4C* fileMetadata) {
	parquet::FileMetaData file_metadata_;
	convertToParquetMetadata(&file_metadata_, fileMetadata);
	return thriftSerializer->Serialize(&file_metadata_, len, buf);
}

/**
 * convert a field of hawq to parquet schema elements
 * schema:		the schema for parquet
 * field:		a hawq field
 * index:		the index of current column in parquet schema
 */
int toParquetSchema_SingleColumn(vector<parquet::SchemaElement> &schema,
		FileField_4C *field, int *colIndex) {
	/*first visit the type itself, then its children!!!*/

	schema[*colIndex + 1].__set_name(field->name);
	schema[*colIndex + 1].__set_repetition_type(
					(parquet::FieldRepetitionType::type) field->repetitionType);
	if (field->num_children > 0) {
		/* only when having children, set_num_children for schema*/
		schema[*colIndex + 1].__set_num_children(field->num_children);
		/* the internal node also need to be written out*/
		*colIndex = *colIndex + 1;
		/* recursively call the children*/
		for (int i = 0; i < field->num_children; i++) {
			toParquetSchema_SingleColumn(schema, &(field->children[i]), colIndex);
		}
	} else {
		schema[*colIndex + 1].__set_type((parquet::Type::type) field->type);
		*colIndex = *colIndex + 1;
	}
	return 0;
}

/**
 * The nested parquet schema types is stored as a tree, should traverse that in depth-first
 * schema:		the schema for parquet
 * pfields:		the hawq fields
 * fieldCount:	the count of hawq field -- in first level
 * nodeCount:	total counts of hawq schema tree nodes, including internal and leaf nodes
 */
int toParquetSchema(vector<parquet::SchemaElement> &schema,
		FileField_4C* pfields, int fieldCount, int nodeCount) {
	if (fieldCount == 0)
		return -1;
	schema.resize(nodeCount + 1);
	schema[0].__set_num_children(fieldCount);
	schema[0].name = "schema";

	int colIndex = 0;
	for (int i = 0; i < fieldCount; ++i) {
		FileField_4C *field = &pfields[i];
		toParquetSchema_SingleColumn(schema, field, &colIndex);
	}

	/*colIndex should equals to colCount after processing all the columns*/
	if (nodeCount != colIndex) {
		return -1;
	}
	return 0;
}

int toParquetEncoding(std::vector<parquet::Encoding::type> &encodings,
		Encoding* pEncodings, int encodingCount) {
	if (encodingCount == 0)
		return -1;
	encodings.resize(encodingCount + 1);
	for (int i = 0; i < encodingCount; ++i) {
		encodings[i] = (parquet::Encoding::type) pEncodings[i];
	}
	return 0;
}

/*get column path in schema. The path is splited with ":"*/
int toParquetPathInSchema(std::vector<std::string> &path_in_schema, int depth, char* path){
	char *newPath = (char*)palloc0(strlen(path) + 1);
	strcpy(newPath, path);
	path_in_schema.resize(depth);
	const char *delim = ":";
	path_in_schema[0] = strtok(newPath, delim);
	for(int i = 1; i < depth; i++){
		path_in_schema[i] = strtok(NULL, delim);
		if(path_in_schema[i].c_str() == NULL){
			return -1;
		}
	}
	return 0;
}

int toParquetColumnChunk(std::vector<parquet::ColumnChunk> &col_chunks,
		ColumnChunkMetadata_4C* pColChunk, int colChunkCount) {
	if (colChunkCount == 0)
		return -1;
	col_chunks.resize(colChunkCount);
	for (int i = 0; i < colChunkCount; ++i) {
		parquet::ColumnChunk& node = col_chunks[i];
		parquet::ColumnMetaData metadata;
		metadata.data_page_offset = pColChunk[i].firstDataPage;
		metadata.dictionary_page_offset = -1;
		metadata.codec = (parquet::CompressionCodec::type) pColChunk[i].codec;
		metadata.total_compressed_size = pColChunk[i].totalSize;
		metadata.total_uncompressed_size = pColChunk[i].totalUncompressedSize;
		metadata.num_values = pColChunk[i].valueCount;
		metadata.type = (parquet::Type::type) pColChunk[i].type;
		toParquetEncoding(metadata.encodings, pColChunk[i].pEncodings,
				pColChunk[i].EncodingCount);
		/*add parquet column chunk path in schema*/
		toParquetPathInSchema(metadata.path_in_schema, pColChunk[i].depth, pColChunk[i].pathInSchema);

		node.__set_file_offset(pColChunk[i].file_offset);
		node.__set_meta_data(metadata);
	}
	return 0;
}

int toParquetRowGroup(std::vector<parquet::RowGroup> &row_groups,
		BlockMetadata_4C** pblocks, int blockCount) {
	if (blockCount == 0)
		return -1;
	row_groups.resize(blockCount);
	for (int i = 0; i < blockCount; ++i) {
		parquet::RowGroup& node = row_groups[i];
		node.num_rows = (*pblocks[i]).rowCount;
		node.total_byte_size = (*pblocks[i]).totalByteSize;
		toParquetColumnChunk(node.columns, (*pblocks[i]).columns,
				(*pblocks[i]).ColChunkCount);
	}
	return 0;
}

int MetadataUtil::convertToParquetMetadata(
		parquet::FileMetaData *parquetFileMetadata,
		ParquetMetadata_4C* hawqFileMetadata) {
	int iret = toParquetSchema(parquetFileMetadata->schema,
			hawqFileMetadata->pfield, hawqFileMetadata->fieldCount,
			hawqFileMetadata->schemaTreeNodeCount);

	parquetFileMetadata->version = hawqFileMetadata->version;
	parquetFileMetadata->num_rows = hawqFileMetadata->num_rows;

	iret = toParquetRowGroup(parquetFileMetadata->row_groups,
			hawqFileMetadata->pBlockMD, hawqFileMetadata->blockCount);

	/* set key value part of file metadata*/
	std::vector<parquet::KeyValue> keyValue;
	keyValue.resize(1);
	keyValue[0].__set_key("hawq.schema");
	keyValue[0].__set_value(hawqFileMetadata->hawqschemastr);
	parquetFileMetadata->__set_key_value_metadata(keyValue);

	return iret;
}

int MetadataUtil::readPageMetadata(uint8_t *buf, uint32_t *len, bool compact,
		PageMetadata_4C* pageMetadata) {
	parquet::PageHeader parquetHeader;
	if (this->thriftDeserializer->DeserializeThriftMsg(buf, len, compact,
			&parquetHeader) < 0)
		return -1;

	convertPageMetadata(parquetHeader, pageMetadata);
	return 0;
}

/*
 * Write page metadata to buffer
 *
 * buf:		buffer to be write
 * len:		buffer length to be read
 * compact:	whether use thrift compact protocol
 * */

int MetadataUtil::writePageMetadata(uint8_t **buf, uint32_t *len,
		PageMetadata_4C* pageMetadata) {
	parquet::PageHeader page_header_;
	convertToPageMetadata(&page_header_, pageMetadata);
	return thriftSerializer->Serialize(&page_header_, len, buf);
}

void MetadataUtil::convertPageMetadata(parquet::PageHeader& parquetHeader,
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

int MetadataUtil::convertToPageMetadata(parquet::PageHeader *parquetHeader,
		PageMetadata_4C* hawqPageMetadata) {
	parquetHeader->__set_type(
			(enum parquet::PageType::type) hawqPageMetadata->page_type);
	parquetHeader->__set_compressed_page_size(
			hawqPageMetadata->compressed_page_size);
	parquetHeader->__set_crc(hawqPageMetadata->crc);
	parquetHeader->__set_uncompressed_page_size(
			hawqPageMetadata->uncompressed_page_size);

	parquet::DataPageHeader dataPageHeader;
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

int MetadataUtil::convertToColumnMetadata(
		parquet::ColumnMetaData *columnchunk_metadata,
		ColumnChunkMetadata_4C* hawqColumnMetadata) {
	columnchunk_metadata->__set_codec(
			(parquet::CompressionCodec::type) hawqColumnMetadata->codec);
	columnchunk_metadata->__set_data_page_offset(
			hawqColumnMetadata->firstDataPage);
	columnchunk_metadata->__set_dictionary_page_offset(-1);
	std::vector<parquet::Encoding::type> encodings;
	toParquetEncoding(encodings, hawqColumnMetadata->pEncodings,
			hawqColumnMetadata->EncodingCount);
	columnchunk_metadata->__set_encodings(encodings);
	columnchunk_metadata->__set_index_page_offset(-1);
	columnchunk_metadata->__set_num_values(hawqColumnMetadata->valueCount);
	/*set path in schema, should be a vector with one column: colName*/
	std::vector < std::string > pathschema;
	toParquetPathInSchema(pathschema, hawqColumnMetadata->depth, hawqColumnMetadata->pathInSchema);
	columnchunk_metadata->__set_path_in_schema(pathschema);
	columnchunk_metadata->__set_total_compressed_size(
			hawqColumnMetadata->totalSize);
	columnchunk_metadata->__set_total_uncompressed_size(
			hawqColumnMetadata->totalUncompressedSize);
	columnchunk_metadata->__set_type(
			(parquet::Type::type) hawqColumnMetadata->type);
	return 0;
}

int MetadataUtil::writeColumnChunkMetadata(uint8_t **buf, uint32_t *len,
		ColumnChunkMetadata_4C* hawqColumnChunkMetadata) {
	parquet::ColumnMetaData columnchunk_metadata;
	convertToColumnMetadata(&columnchunk_metadata, hawqColumnChunkMetadata);
	return thriftSerializer->Serialize(&columnchunk_metadata, len, buf);
}
