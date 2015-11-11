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

#include "cdb/cdbparquetfooterserializer_protocol.h"
#include "cdb/cdbparquetfooterbuffer.h"

const int8_t PF_TTypeToCType[16] = {
    CT_STOP, // T_STOP
    0, // unused
    CT_BOOLEAN_TRUE, // T_BOOL
    CT_BYTE, // T_BYTE
    CT_DOUBLE, // T_DOUBLE
    0, // unused
    CT_I16, // T_I16
    0, // unused
    CT_I32, // T_I32
    0, // unused
    CT_I64, // T_I64
    CT_BINARY, // T_STRING
    CT_STRUCT, // T_STRUCT
    CT_MAP, // T_MAP
    CT_SET, // T_SET
    CT_LIST, // T_LIST
};

enum TType getTType(int8_t type);
uint32_t readVarint32(CompactProtocol *prot, int32_t *i32);
uint32_t readVarint64(CompactProtocol *prot, int64_t *i64);
int32_t  zigzagToI32(uint32_t n);
int64_t  zigzagToI64(uint64_t n);

void initCompactProtocol(CompactProtocol *protocol, File fileHandler,
		char *fileName, int64 footerLength, int mode) {
	/*initialize the lastField array*/
	protocol->lastFieldArrMaxSize = PARQUET_MAX_FIELD_DEPTH;
	protocol->lastFieldArr =
			(int16_t*) palloc0(sizeof(int16_t) * protocol->lastFieldArrMaxSize);
	protocol->lastFieldArrSize = 0;

	/*seek to file handler to footerIndex, and initialize cdbbufferprocessor*/
	protocol->footerProcessor = createParquetFooterBuffer
			(fileHandler, fileName, footerLength,
					PARQUET_FOOTER_BUFFER_CAPACITY_DEFAULT, mode);
}

void freeCompactProtocol(CompactProtocol *protocol) {
	/* free the last field array*/
	if (protocol->lastFieldArrMaxSize > 0) {
		pfree(protocol->lastFieldArr);
		protocol->lastFieldArrMaxSize = 0;
		protocol->lastFieldArrSize = 0;
	}

	/* clear the buffer*/
	freeParquetFooterBuffer(protocol->footerProcessor);
}

/**
 * Read struct begin, push the last field id in the array, and reset last field 0
 */
void readStructBegin(CompactProtocol *prot) {
	/* If the array size exceeds max size, repalloc the array*/
	if (prot->lastFieldArrSize >= prot->lastFieldArrMaxSize) {
		prot->lastFieldArrMaxSize *= 2;
		prot->lastFieldArr = (int16_t*) repalloc(prot->lastFieldArr,
				prot->lastFieldArrMaxSize);
	}
	prot->lastFieldArr[prot->lastFieldArrSize++] = prot->lastFieldID;
	prot->lastFieldID = 0;
}

/**
 * Read struct end, get lastFieldID from last field array, and also remove it from the array
 */
void readStructEnd(CompactProtocol *prot) {
	prot->lastFieldID = prot->lastFieldArr[prot->lastFieldArrSize - 1];
	prot->lastFieldArrSize--;
}

/**
 * Get field type from compact type
 */
enum TType getTType(int8_t type) {
	switch (type) {
	case T_STOP:
		return T_STOP;
	case CT_BOOLEAN_FALSE:
	case CT_BOOLEAN_TRUE:
		return T_BOOL;
	case CT_BYTE:
		return T_BYTE;
	case CT_I16:
		return T_I16;
	case CT_I32:
		return T_I32;
	case CT_I64:
		return T_I64;
	case CT_DOUBLE:
		return T_DOUBLE;
	case CT_BINARY:
		return T_STRING;
	case CT_LIST:
		return T_LIST;
	case CT_SET:
		return T_SET;
	case CT_MAP:
		return T_MAP;
	case CT_STRUCT:
		return T_STRUCT;
	default:
		/*ereport error, unknown field type*/
		ereport(ERROR,
				(errcode(ERRCODE_GP_INTERNAL_ERROR), errmsg("parquet footer processor error: unknown field type %c", (char)type)));
	}
	return T_STOP;
}

int8_t getCompactType(const TType ttype) {
    return PF_TTypeToCType[ttype];
}

/**
 * Read field begin, read a single field begin, read its type and fieldid
 */
uint32_t readFieldBegin(CompactProtocol *prot, TType *fieldType,
		int16_t *fieldId) {
	uint32_t rsize = 0;
	int8_t byte;
	int8_t type;

	rsize += readByte(prot, &byte);
	type = (byte & 0x0f);

	/* if it's a stop, then we can return immediately, as the struct is over. */
	if (type == T_STOP) {
		*fieldType = T_STOP;
		*fieldId = 0;
		return rsize;
	}

	/* Get fieldID Information.
	 * Mask off the 4 MSB of the type header. It could contain a field id delta. */
	int16_t modifier = (int16_t)(((uint8_t) byte & 0xf0) >> 4);
	if (modifier == 0) {
		/* not a delta, look ahead for the zigzag varint field id. */
		rsize += readI16(prot, fieldId);
	} else {
		*fieldId = (int16_t)(prot->lastFieldID + modifier);
	}

	/* Get FieldType*/
	*fieldType = getTType(type);

	/* Do special processing for boolean type.
	 * If this happens to be a boolean field, the value is encoded in the type */
	if (type == CT_BOOLEAN_TRUE || type == CT_BOOLEAN_FALSE) {
		/* save the boolean value in a special instance variable. */
		/*boolValue_.hasBoolValue = true;
		 boolValue_.boolValue = (type == CT_BOOLEAN_TRUE ? true : false);*/
	}

	/* push the new field onto the field stack so we can keep the deltas going. */
	prot->lastFieldID = *fieldId;
	return rsize;
}

/**
 * Read List begin. Get the list element type and list size
 */
uint32_t readListBegin(CompactProtocol *prot, TType *elemType, uint32_t *size) {
	int8_t size_and_type;
	uint32_t rsize = 0;
	int32_t lsize;

	rsize += readByte(prot, &size_and_type);

	/* get list size*/
	lsize = ((uint8_t) size_and_type >> 4) & 0x0f;
	if (lsize == 15) {
		rsize += readVarint32(prot, &lsize);
	}
	if (lsize < 0) {
		/*ereport error, list size should not be negative*/
		ereport(ERROR,
				(errcode(ERRCODE_GP_INTERNAL_ERROR), errmsg("parquet footer processor error: list size negative %d", lsize)));
	}

	*elemType = getTType((int8_t)(size_and_type & 0x0f));
	*size = (uint32_t) lsize;
	return rsize;
}

/**
 * Read an i16 from the wire as a zigzag varint.
 */
uint32_t readI16(CompactProtocol *prot, int16_t *i16) {
	int32_t value;
	uint32_t rsize = readVarint32(prot, &value);
	*i16 = (int16_t) zigzagToI32(value);
	return rsize;
}

/**
 * Read an i32 from the wire as a zigzag varint.
 */
uint32_t readI32(CompactProtocol *prot, int32_t *i32) {
	int32_t value;
	uint32_t rsize = readVarint32(prot, &value);
	*i32 = zigzagToI32(value);
	return rsize;
}

/**
 * Read an i64 from the wire as a zigzag varint.
 */
uint32_t readI64(CompactProtocol *prot, int64_t *i64) {
	int64_t value;
	uint32_t rsize = readVarint64(prot, &value);
	*i64 = zigzagToI64(value);
	return rsize;
}

uint32_t readString(CompactProtocol *prot, char **str) {
	int32_t rsize = 0;
	int32_t size = 0;
	uint8_t *tmp = NULL;
  int32_t sizeForRead = 0;
  int32_t strIdx = 0;
  int32_t bufCapacity;
  int bufRet;

	rsize += readVarint32(prot, &size);
	/* Catch empty string case */
	if (size == 0) {
		*str = "";
		return rsize;
	}

	/* Catch error cases */
	if (size < 0) {
		/*ereport error, string size should not be negative*/
		ereport(ERROR,
				(errcode(ERRCODE_GP_INTERNAL_ERROR), errmsg("parquet footer processor error: string size negative %d", size)));
	}

  sizeForRead = size;
  (*str) = (char*)palloc0(size + 1);
  Assert (prot->footerProcessor != NULL);
  bufCapacity = prot->footerProcessor->Capacity;
  /* Read from buffer to string, read length size */
  while (sizeForRead > bufCapacity) {
    bufRet = prepareFooterBufferForwardReading(prot->footerProcessor, bufCapacity, &tmp);
    Assert (bufRet == PARQUET_FOOTER_BUFFER_MOVE_OK);
    memcpy(*str + strIdx, tmp, bufCapacity);
    sizeForRead -= bufCapacity;
    strIdx += bufCapacity;
    tmp = NULL;
  }
  bufRet = prepareFooterBufferForwardReading(prot->footerProcessor, sizeForRead, &tmp);
  Assert (bufRet == PARQUET_FOOTER_BUFFER_MOVE_OK);
  memcpy(*str + strIdx, tmp, sizeForRead);

	return rsize + (uint32_t) size;
}

uint32_t readByte(CompactProtocol *prot, int8_t *byte) {
	uint8_t *b;
	/* Read from buffer to string, read length size */
	/* prot->buffer->read(b, 1);*/
	prepareFooterBufferForwardReading(prot->footerProcessor, 1, &b);
	*byte = *(int8_t*) b;
	return 1;
}

/**
 * Read an i32 from the wire as a varint. The MSB of each byte is set
 * if there is another byte to follow. This can read up to 5 bytes.
 */
uint32_t readVarint32(CompactProtocol *prot, int32_t *i32) {
	int64_t val;
	uint32_t rsize = readVarint64(prot, &val);
	*i32 = (int32_t) val;
	return rsize;
}

/**
 * Read an i64 from the wire as a proper varint. The MSB of each byte is set
 * if there is another byte to follow. This can read up to 10 bytes.
 */
uint32_t readVarint64(CompactProtocol *prot, int64_t *i64) {
	uint32_t rsize = 0;
	uint64_t val = 0;
	int shift = 0;
	uint8_t buf[10]; /* 64 bits / (7 bits/byte) = 10 bytes. */

	/* read int64 recursively*/
	while (true) {
		uint8_t *byte = NULL;
		/*read one byte*/
		if(prepareFooterBufferForwardReading(prot->footerProcessor, 1, &byte)
				== PARQUET_FOOTER_BUFFER_MOVE_ERROR){
			ereport(ERROR,
					(errcode(ERRCODE_GP_INTERNAL_ERROR),
							errmsg("Error in loading parquet footer into memory, %s",
									prot->footerProcessor->FileName)));
		}
		rsize += 1;
		val |= (uint64_t)((*byte) & 0x7f) << shift;
		shift += 7;
		if (!((*byte) & 0x80)) {
			*i64 = val;
			return rsize;
		}
		if (rsize >= sizeof(buf)) {
			ereport(ERROR,
					(errcode(ERRCODE_GP_INTERNAL_ERROR), errmsg("Variable-length int over 10 bytes. %d", rsize)));
		}
	}
}

/**
 * Convert from zigzag int to int.
 */
int32_t zigzagToI32(uint32_t n) {
	return (n >> 1) ^ (uint32_t)(-(int32_t)(n & 1));
}

/**
 * Convert from zigzag long to long.
 */
int64_t zigzagToI64(uint64_t n) {
	return (n >> 1) ^ (uint64_t)(-(int64_t)(n & 1));
}

uint32_t skipType(CompactProtocol *prot, TType type) {
	switch (type) {
		case T_BYTE: {
			int8_t bytev;
			return readByte(prot, &bytev);
		}
		case T_I16: {
			int16_t i16;
			return readI16(prot, &i16);
		}
		case T_I32: {
			int32_t i32;
			return readI32(prot, &i32);
		}
		case T_I64: {
			int64_t i64;
			return readI64(prot, &i64);
		}
		case T_STRING: {
			char *str;
			return readString(prot, &str);
		}
		case T_STRUCT: {
			uint32_t result = 0;
			int16_t fid;
			TType ftype;
			readStructBegin(prot);
			while (true) {
				result += readFieldBegin(prot, &ftype, &fid);
				if (ftype == T_STOP) {
					break;
				}
				result += skipType(prot, ftype);
			}
			readStructEnd(prot);
			return result;
		}
		case T_LIST: {
			uint32_t result = 0;
			TType elemType;
			uint32_t i, size;
			result += readListBegin(prot, &elemType, &size);
			for (i = 0; i < size; i++) {
				result += skipType(prot, elemType);
			}
			return result;
		}
		case T_BOOL:
		case T_STOP:
		case T_DOUBLE:
		case T_SET:
		case T_MAP:
		case T_VOID:
		case T_U64:
		case T_UTF8:
		case T_UTF16:
		{
			ereport(ERROR,
				(errcode(ERRCODE_GP_INTERNAL_ERROR), errmsg("Doesn't support the type %d in parquet", type)));

			break;
		}
	}
	return 0;

}

uint32_t writeVarint64(CompactProtocol *prot, int64_t val)
{
	uint8_t buf[10];
	uint32_t wsize = 0;

	while (true) {
		if ((val & ~0x7FL) == 0) {
			buf[wsize++] = (int8_t) val;
			break;
		} else {
			buf[wsize++] = (int8_t)((val & 0x7F) | 0x80);
			val >>= 7;
		}
	}
    writeToFooterBuffer(prot->footerProcessor, (uint8_t *)buf, wsize);
    return wsize;
}

uint32_t writeVarint32(CompactProtocol *prot, int32_t val)
{
    uint8_t buf[5];
    uint32_t wsize = 0;

    while (true) {
      if ((val & ~0x7F) == 0) {
        buf[wsize++] = (int8_t)val;
        break;
      } else {
        buf[wsize++] = (int8_t)((val & 0x7F) | 0x80);
        val >>= 7;
      }
    }
    writeToFooterBuffer(prot->footerProcessor, (uint8_t *)buf, wsize);
    return wsize;
}


uint32_t writeVarint8(CompactProtocol *prot, int8_t i8)
{
    writeToFooterBuffer(prot->footerProcessor, (uint8_t *)&i8, 1);
    return 1;
}

uint32_t writeBinary(CompactProtocol *prot, void *mem, uint32_t len)
{
    uint32_t strlensize = writeVarint32(prot, len);
    writeToFooterBuffer(prot->footerProcessor, (uint8_t *)mem, len);
    return strlensize + len;
}


uint32_t writeCollectionBegin(CompactProtocol *prot,
                              TType            elemtype,
                              int32_t          listsize )
{
    uint32_t wsize = 0;
    if ( listsize <= 14 ) {
        wsize += writeByte(prot,
                           (listsize << 4) | getCompactType(elemtype));
    } else {
        wsize += writeByte(prot,
                           0xf0 | getCompactType(elemtype));
        wsize += writeVarint32(prot, listsize);
    }
    return wsize;
}

uint32_t writeFieldBegin     (CompactProtocol *prot,
                              TType            fieldtype,
                              int16_t          fieldid)
{
    uint32_t wsize = 0;
    int8_t typetowrite = getCompactType(fieldtype);
    if ( fieldid > prot->lastFieldID &&
         fieldid - prot->lastFieldID <= 15 ) {
        wsize += writeByte(prot,
                           ((fieldid-prot->lastFieldID)<<4) | typetowrite);
    }
    else {
        wsize += writeByte(prot, typetowrite);
        wsize += writeI16(prot, fieldid);
    }
    
    prot->lastFieldID = fieldid;
    return wsize;
    
}

uint32_t
writeFieldStop(CompactProtocol *prot){
	return writeByte(prot, T_STOP);
}

void
setLastFieldId(CompactProtocol *prot, int16_t fieldId){
	prot->lastFieldID = fieldId;
}

uint32_t
writeStructBegin(CompactProtocol *prot)
{
    /* If the array size exceeds max size, repalloc the array*/
	if (prot->lastFieldArrSize >= prot->lastFieldArrMaxSize) {
		prot->lastFieldArrMaxSize *= 2;
		prot->lastFieldArr = (int16_t*) repalloc(prot->lastFieldArr,
                                                 prot->lastFieldArrMaxSize);
	}
	prot->lastFieldArr[prot->lastFieldArrSize++] = prot->lastFieldID;
	prot->lastFieldID = 0;
    return 0;
}

uint32_t writeStructEnd(CompactProtocol *prot)
{
    prot->lastFieldID = prot->lastFieldArr[prot->lastFieldArrSize - 1];
	prot->lastFieldArrSize--;
    return 0;
}

uint32_t writeI16(CompactProtocol *prot, int16_t i16)
{
	int32_t i32 = i16;
	return writeVarint32((prot), I32TOI32ZIGZAG(i32));
}
