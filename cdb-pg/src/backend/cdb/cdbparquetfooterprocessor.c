/*
 * cdbparquetfooterprocessor.c
 *
 *  Created on: Sep 22, 2013
 *      Author: malili
 */

#include "cdb/cdbparquetfooterprocessor.h"
#include "cdb/cdbparquetstoragewrite.h"


int writeParquetHeader(File dataFile, int64 *fileLen, int64 *fileLen_uncompressed) {
	/* write out magic 'PAR1'*/
	DetectHostEndian();

	char PARQUET_VERSION_NUMBER[4] = { 'P', 'A', 'R', '1' };
	if (FileWrite(dataFile, PARQUET_VERSION_NUMBER, 4) != 4)
		return -1;

	*fileLen += 4;
	*fileLen_uncompressed += 4;
	return 0;
}

int writeParquetFooter(File dataFile,
		/*ParquetMetadataUtil metaUtil,*/
		ParquetMetadata parquetMetadata,
		int64 *fileLen,
		int64 *fileLen_uncompressed) {
	char *bufferFooter;
	uint32_t footerLen;
	char *bufferFooterLen = (char*) palloc0(4);
	int writeRet = 0;

	/* write out metadata through thrift protocol to buffer*/
	writeRet = writeFileMetadata((uint8_t **) &bufferFooter, &footerLen,
			parquetMetadata/*, metaUtil*/);
	if(writeRet < 0)
	{
		return -1;
	}
	elog(DEBUG5, "footerlen:%d", footerLen);

	/* write out buffer to file*/
	writeRet = FileWrite(dataFile, bufferFooter, footerLen);

	if (writeRet != footerLen) {
		return -1;
	}

	*fileLen += footerLen;
	*fileLen_uncompressed += footerLen;

	/* write out footer length*/
	bufferFooterLen[0] = (footerLen >> 0) & 0xFF;
	bufferFooterLen[1] = (footerLen >> 8) & 0xFF;
	bufferFooterLen[2] = (footerLen >> 16) & 0xFF;
	bufferFooterLen[3] = (footerLen >> 24) & 0xFF;
	writeRet = FileWrite(dataFile, bufferFooterLen, 4);
	if (writeRet != 4)
		return -1;

	/* write out magic 'PAR1'*/
	char PARQUET_VERSION_NUMBER[4] = { 'P', 'A', 'R', '1' };
	writeRet = FileWrite(dataFile, PARQUET_VERSION_NUMBER, 4);
	if (writeRet != 4)
		return -1;

	*fileLen += 8;
	*fileLen_uncompressed += 8;
	return 0;
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
		int64 eof, char *filePathName) {
	int compact = 1;
	int actualReadSize = 0;

	DetectHostEndian();

	DetectHostEndian();

	/* if file size is 0, means there's no data in file, return -1*/
	if (eof == 0)
		return false;

	/* should judge correctness of eof, at least the file should contain header 'PAR1', and footer
	 * footerLength(4 bytes) and 'PAR1'*/
	if (eof < 12)
	{
		ereport(ERROR,
				(errcode(ERRCODE_IO_ERROR),
				 errmsg("Parquet Storage Read error on segment file '%s': eof information not correct "
						 "at least eof should be 12, but got '%lld'", filePathName, (long long)eof)));
	}

	/* get footer length*/
	int64 footLengthIndex = FileSeek(fileHandler, eof - 8, SEEK_SET);
	if (footLengthIndex != (eof - 8))
	{
		ereport(ERROR,
				(errcode(ERRCODE_IO_ERROR),
				 errmsg("Parquet Storage Read error on segment file '%s': seek failure when fetching "
						 "footer length" , filePathName)));
	}
	elog(DEBUG5, "Parquet metadata file footer length index: %lld\n", (long long)footLengthIndex);

	char buffer[4];
	while(actualReadSize < 4)
	{
		/*read out all the buffer of the column chunk*/
		int readFooterLen = FileRead(fileHandler, buffer + actualReadSize, 4 - actualReadSize);
		if (readFooterLen < 0) {
			/*ereport error*/
			ereport(ERROR, (errcode(ERRCODE_IO_ERROR),
			errmsg("Parquet Storage Read error on segment file '%s': reading footer failure", filePathName)));
		}
		actualReadSize += readFooterLen;
	}

/*	uint32_t ch1 = buffer[0];
	uint32_t ch2 = buffer[1];
	uint32_t ch3 = buffer[2];
	uint32_t ch4 = buffer[3];
	uint64_t footerLen = (ch4 << 24) + (ch3 << 16) + (ch2 << 8) + (ch1 << 0);
	elog(DEBUG5, "Parquet metadata file footer length: %llu\n",footerLen);*/

	/** get footerlen through little-endian decoding */
	uint32_t footerLen = *(uint32*) buffer;
	elog(DEBUG5, "Parquet metadata file footer length: %u\n",footerLen);


	/** Part 2: read footer itself*/
	int64 footerIndex = footLengthIndex - (int64) footerLen;
	elog(
	DEBUG5, "Parquet metadata file footer Index: %lld",(long long)footerIndex);

	if (FileSeek(fileHandler, footerIndex, SEEK_SET) != footerIndex)
	{
		ereport(ERROR,
				(errcode(ERRCODE_IO_ERROR),
				 errmsg("Parquet Storage Read error on segment file '%s': seek failure when fetching footer" , filePathName)));
	}

	char bufferFooter[footerLen];
	actualReadSize = 0;
	while(actualReadSize < footerLen)
	{
		/*read out all the buffer of the column chunk*/
		int readLen = FileRead(fileHandler, bufferFooter + actualReadSize, footerLen - actualReadSize);
		if (readLen < 0) {
			/*ereport error*/
			ereport(ERROR, (errcode(ERRCODE_IO_ERROR),
			errmsg("Parquet Storage Read error on segment file '%s': reading footer failure", filePathName)));
		}
		actualReadSize += readLen;
	}


	/** read metadata through thrift protocol*/
	if (readFileMetadata((uint8_t*) bufferFooter, footerLen, compact,
			parquetMetadata) < 0){
		ereport(ERROR,
			(errcode(ERRCODE_IO_ERROR),
			 errmsg("Parquet Storage Read error on segment file '%s': read footer" , filePathName)));
	}

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
				(errmsg("Parquet format is not supported on Big Endian.")));
}

