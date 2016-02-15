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
 * cdbparquetfooterprocessor.c
 *
 *  Created on: Sep 22, 2013
 *      Author: malili
 */

#include "cdb/cdbparquetfooterprocessor.h"
#include "cdb/cdbparquetstoragewrite.h"
#include "cdb/cdbparquetfooterserializer.h"


void writeParquetHeader(File dataFile, char *filePathName, int64 *fileLen, int64 *fileLen_uncompressed) {
	/* write out magic 'PAR1'*/
	DetectHostEndian();

	char PARQUET_VERSION_NUMBER[4] = { 'P', 'A', 'R', '1' };
	if (FileWrite(dataFile, PARQUET_VERSION_NUMBER, 4) != 4)
	{
		ereport(ERROR,
				(errcode_for_file_access(),
						errmsg("file write error in file '%s': %s", filePathName, strerror(errno)),
						errdetail("%s", HdfsGetLastError())));
	}

	*fileLen += 4;
	*fileLen_uncompressed += 4;
}

void writeParquetFooter(File dataFile,
		char *filePathName,
		ParquetMetadata parquetMetadata,
		int64 *fileLen,
		int64 *fileLen_uncompressed,
		CompactProtocol **footer_read_protocol,
		CompactProtocol **footer_write_protocol,
		int	previous_rowgroup_count) {
	uint32_t footerLen;
	char bufferFooterLen[4];
	int writeRet = 0;

	footerLen = (uint32_t)endSerializeFooter(footer_read_protocol, footer_write_protocol,
			filePathName, dataFile, parquetMetadata, previous_rowgroup_count);

	*fileLen += footerLen;
	*fileLen_uncompressed += footerLen;

	/* write out footer length*/
	bufferFooterLen[0] = (footerLen >> 0) & 0xFF;
	bufferFooterLen[1] = (footerLen >> 8) & 0xFF;
	bufferFooterLen[2] = (footerLen >> 16) & 0xFF;
	bufferFooterLen[3] = (footerLen >> 24) & 0xFF;
	writeRet = FileWrite(dataFile, bufferFooterLen, 4);
	if (writeRet != 4)
	{
		ereport(ERROR,
			(errcode_for_file_access(),
			 errmsg("file write error in file '%s': %s", filePathName, strerror(errno)),
			 errdetail("%s", HdfsGetLastError())));
	}

	/* write out magic 'PAR1'*/
	char PARQUET_VERSION_NUMBER[4] = { 'P', 'A', 'R', '1' };
	writeRet = FileWrite(dataFile, PARQUET_VERSION_NUMBER, 4);
	if (writeRet != 4)
	{
		ereport(ERROR,
				(errcode_for_file_access(),
						errmsg("file write error in file '%s': %s", filePathName, strerror(errno)),
						errdetail("%s", HdfsGetLastError())));
	}

	*fileLen += 8;
	*fileLen_uncompressed += 8;
}

/*
 * read the footer of a parquet file
 *
 * @fileHandler			the file to be read
 * @parquetMetadata		the parquetMetadata which needs to be read out from file
 * @eof					the startring read point of file
 *
 * @return				whether the parquetMetadata be read out. If read out, return true,
 * 						else return false
 *
 * */
bool readParquetFooter(File fileHandler, ParquetMetadata *parquetMetadata,
		CompactProtocol **footerProtocol, int64 eof, char *filePathName) {
	/*int compact = 1;*/
	int actualReadSize = 0;

	DetectHostEndian();

  *parquetMetadata = (struct ParquetMetadata_4C *)
      palloc0(sizeof(struct ParquetMetadata_4C));

	/* if file size is 0, means there's no data in file, return false*/
	if (eof == 0)
		return false;

	/* should judge correctness of eof, at least the file should contain header 'PAR1', and footer
	 * footerLength(4 bytes) and 'PAR1'*/
	if (eof < 12)
	{
		ereport(ERROR,
				(errcode(ERRCODE_GP_INTERNAL_ERROR),
				 errmsg("catalog information for '%s' not correct, eof should be more than 12, but got" INT64_FORMAT,
						 filePathName, eof)));
	}

	/* get footer length*/
	int64 footLengthIndex = FileSeek(fileHandler, eof - 8, SEEK_SET);
	if (footLengthIndex != (eof - 8))
	{
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("file seek error in file '%s' when seeking \'" INT64_FORMAT "\': '%s'"
						 , filePathName, footLengthIndex, strerror(errno)),
				 errdetail("%s", HdfsGetLastError())));
	}
	elog(DEBUG5, "Parquet metadata file footer length index: " INT64_FORMAT "\n", footLengthIndex);

	char buffer[4];
	while(actualReadSize < 4)
	{
		/*read out all the buffer of the column chunk*/
		int readFooterLen = FileRead(fileHandler, buffer + actualReadSize, 4 - actualReadSize);
		if (readFooterLen < 0) {
			ereport(ERROR,
					(errcode_for_file_access(),
							errmsg("file read error in file '%s': %s", filePathName, strerror(errno)),
							errdetail("%s", HdfsGetLastError())));
		}
		actualReadSize += readFooterLen;
	}

	/** get footerlen through little-endian decoding */
	uint32_t footerLen = *(uint32*) buffer;
	elog(DEBUG5, "Parquet metadata file footer length: %u\n",footerLen);


	/** Part 2: read footer itself*/
	int64 footerIndex = footLengthIndex - (int64) footerLen;
	elog(
	DEBUG5, "Parquet metadata file footer Index: " INT64_FORMAT, footerIndex);

	if (FileSeek(fileHandler, footerIndex, SEEK_SET) != footerIndex)
	{
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("file seek error in file '%s' when seeking \'" INT64_FORMAT "\': %s"
						 , filePathName, footerIndex, strerror(errno)),
				 errdetail("%s", HdfsGetLastError())));
	}

	initDeserializeFooter(fileHandler, footerLen, filePathName, parquetMetadata, footerProtocol);

	return true;
}


/**
 * This procedure does two things:
 * 1. check whether the file's metadata matches table's schema, return false if mismatch happens.
 * 2. sync meta info: for each parquet field, set its hawq type id
 *
 * TODO if we only accepts files that DB outputs, do we need this check process?
 */
bool
checkAndSyncMetadata(ParquetMetadata parquetmd,
					 TupleDesc tupdesc)
{
	int numfields, i;
	Form_pg_attribute att;
	FileField_4C *field;

	numfields = parquetmd->fieldCount;
	
	if (numfields != tupdesc->natts)
		return false;

	for (i = 0; i < numfields; ++i)
	{
		field = parquetmd->pfield + i;
		att = tupdesc->attrs[i];

		if (strcmp(field->name, NameStr(att->attname)))
			return false;

		if (field->repetitionType == REPEATED)
			return false; /* top level fields shouldn't be repeated. */

		field->hawqTypeId = att->atttypid;
		/* for non-group type, check whether its PrimitiveTypeName compatible with hawqTypeId */
		if (field->num_children == 0 && field->type != mappingHAWQType(field->hawqTypeId))
			return false;

		/* for nested type, set hawqTypeId for children fields */
		switch(field->hawqTypeId)
		{
			case HAWQ_TYPE_POINT:
				field->children[0].hawqTypeId = HAWQ_TYPE_FLOAT8;
				field->children[1].hawqTypeId = HAWQ_TYPE_FLOAT8;
				break;
			case HAWQ_TYPE_PATH:
				field->children[0].hawqTypeId = HAWQ_TYPE_BOOL;	/* is_open */
				field->children[1].hawqTypeId = HAWQ_TYPE_POINT;/* points */
				field->children[1].children[0].hawqTypeId = HAWQ_TYPE_FLOAT8;
				field->children[1].children[1].hawqTypeId = HAWQ_TYPE_FLOAT8;
				break;
			case HAWQ_TYPE_LSEG:
			case HAWQ_TYPE_BOX:
				field->children[0].hawqTypeId = HAWQ_TYPE_FLOAT8;
				field->children[1].hawqTypeId = HAWQ_TYPE_FLOAT8;
				field->children[2].hawqTypeId = HAWQ_TYPE_FLOAT8;
				field->children[3].hawqTypeId = HAWQ_TYPE_FLOAT8;
				break;
			case HAWQ_TYPE_POLYGON:
				field->children[0].hawqTypeId = HAWQ_TYPE_BOX;
				field->children[0].children[0].hawqTypeId = HAWQ_TYPE_FLOAT8;
				field->children[0].children[1].hawqTypeId = HAWQ_TYPE_FLOAT8;
				field->children[0].children[2].hawqTypeId = HAWQ_TYPE_FLOAT8;
				field->children[0].children[3].hawqTypeId = HAWQ_TYPE_FLOAT8;
				field->children[1].hawqTypeId = HAWQ_TYPE_POINT;
				field->children[1].children[0].hawqTypeId = HAWQ_TYPE_FLOAT8;
				field->children[1].children[1].hawqTypeId = HAWQ_TYPE_FLOAT8;
				break;
			case HAWQ_TYPE_CIRCLE:
				field->children[0].hawqTypeId = HAWQ_TYPE_FLOAT8;
				field->children[1].hawqTypeId = HAWQ_TYPE_FLOAT8;
				field->children[2].hawqTypeId = HAWQ_TYPE_FLOAT8;
				break;
		}
	}

	return true;
}

void
DetectHostEndian(void)
{
	/*uint32_t	O32_LITTLE_ENDIAN = 0x03020100ul;*/
	uint32_t	O32_BIG_ENDIAN = 0x00010203ul;

	static union
	{
		unsigned char	bytes[4];
		uint32_t 		value;
	} o32_host_order = { { 0, 1, 2, 3 } };

	if (O32_BIG_ENDIAN == o32_host_order.value)
		ereport(ERROR,
				(errcode(ERRCODE_GP_INTERNAL_ERROR),
						errmsg("parquet format is not supported on big endian.")));
}

