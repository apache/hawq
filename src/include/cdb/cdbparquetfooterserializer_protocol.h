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
 * cdbparquetfooterserializer_protocol.h
 *
 *  Created on: Mar 17, 2014
 *      Author: malili
 */

#ifndef CDBPARQUETFOOTERSERIALIZER_PROTOCOL_H_
#define CDBPARQUETFOOTERSERIALIZER_PROTOCOL_H_

#include "cdb/cdbparquetfooterbuffer.h"

typedef enum TType {
  T_STOP       = 0,
  T_VOID       = 1,
  T_BOOL       = 2,
  T_BYTE       = 3,
  T_I08        = 3,
  T_I16        = 6,
  T_I32        = 8,
  T_U64        = 9,
  T_I64        = 10,
  T_DOUBLE     = 4,
  T_STRING     = 11,
  T_UTF7       = 11,
  T_STRUCT     = 12,
  T_MAP        = 13,
  T_SET        = 14,
  T_LIST       = 15,
  T_UTF8       = 16,
  T_UTF16      = 17
} TType;

/*Compact protocol type */
typedef enum CTType {
  CT_STOP           = 0x00,
  CT_BOOLEAN_TRUE   = 0x01,
  CT_BOOLEAN_FALSE  = 0x02,
  CT_BYTE           = 0x03,
  CT_I16            = 0x04,
  CT_I32            = 0x05,
  CT_I64            = 0x06,
  CT_DOUBLE         = 0x07,
  CT_BINARY         = 0x08,
  CT_LIST           = 0x09,
  CT_SET            = 0x0A,
  CT_MAP            = 0x0B,
  CT_STRUCT         = 0x0C
} CTType;

int8_t getCompactType(const TType ttype);

#define PARQUET_MAX_FIELD_DEPTH 5

typedef struct CompactProtocol{
	/**
	 * Used to keep track of the last field for the current and previous structs,
	 * so we can do the delta stuff.
	 */
	int16_t *lastFieldArr;		/* last field array*/
	int16 lastFieldArrMaxSize;	/* max size of last field array*/
	int16 lastFieldArrSize;		/* current size of last field array*/
	int16 lastFieldID;			/* The id of last field*/

	/*Pointer to buffer reader/writer*/
	ParquetFooterBuffer *footerProcessor;
} CompactProtocol;

/**
 * Init and free compact protocol
 */
void initCompactProtocol(CompactProtocol *protocol, File fileHandler, char *fileName,
		int64 footerIndex, int mode);

void freeCompactProtocol(CompactProtocol *protocol);

/**
 * Read method for compact protocol of thrift
 */

void readStructBegin(CompactProtocol *prot);

void readStructEnd(CompactProtocol *prot);

uint32_t readFieldBegin(CompactProtocol *prot,
                        TType *fieldType,
                        int16_t *fieldId);

uint32_t readListBegin(CompactProtocol *prot,
                       TType *elemType,
                       uint32_t *size);

uint32_t readByte(CompactProtocol *prot, int8_t *byte);
uint32_t readI16(CompactProtocol *prot, int16_t *i16);
uint32_t readI32(CompactProtocol *prot, int32_t *i32);
uint32_t readI64(CompactProtocol *prot, int64_t *i64);
uint32_t readString(CompactProtocol *prot, char **str);
uint32_t skipType(CompactProtocol *prot, TType type);


/**
 *  Write method for thrift compact protocol
 */
#define I32ZIGZAGTOI32(n)                                               \
    (int32_t)(                                                          \
                ((uint32_t)(n) >> 1) ^                                  \
                (uint32_t)(-(int32_t)(((int32_t)(n)) & 1))              \
             )

#define I64ZIGZAGTOI64(n)                                               \
    (int64_t)(                                                          \
                ((uint64_t)(n) >> 1) ^                                  \
                (uint64_t)(-(int64_t)(((int64_t)(n)) & 1))              \
             )

#define I64TOI64ZIGZAG(n)                                               \
    (uint64_t)(                                                         \
                (((int64_t)(n))<<1) ^                                   \
                (((int64_t)(n))>>63)                                    \
              )

#define I32TOI32ZIGZAG(n)                                               \
    (uint32_t)(                                                         \
                (((int32_t)(n))<<1) ^                                   \
                (((int32_t)(n))>>31)                                    \
              )

#define I16TOI16ZIGZAG(n)                                               \
    (uint16_t)(                                                         \
                (((int16_t)(n))<<1) ^                                   \
                (((int16_t)(n))>>15)                                    \
              )

#define writeI64(prot, i64)                                             \
    writeVarint64((prot), I64TOI64ZIGZAG(i64))

#define writeI32(prot, i32)                                             \
    writeVarint32((prot), I32TOI32ZIGZAG(i32))

uint32_t writeI16(CompactProtocol *prot, int16_t i16);

#define writeByte(prot, i8)                                             \
    writeVarint8 ((prot), (i8))

#define writeString(prot, mem, len)                                     \
    writeBinary((prot), (mem), (len))

#define writeListBegin(prot, elemtype, listsize)                        \
    writeCollectionBegin((prot),(elemtype),(listsize))        

uint32_t writeVarint64       (CompactProtocol *prot, int64_t i64);
uint32_t writeVarint32       (CompactProtocol *prot, int32_t i32);
//uint32_t writeVarint16       (CompactProtocol *prot, int16_t i16);
uint32_t writeVarint8        (CompactProtocol *prot, int8_t  i8 );

uint32_t writeBinary         (CompactProtocol *prot,
                              void            *mem,
                              uint32_t         len);

uint32_t writeCollectionBegin(CompactProtocol *prot,
                              TType            elemtype,
                              int32_t          listsize);

uint32_t writeFieldBegin     (CompactProtocol *prot,
                              TType            fieldtype,
                              int16_t          fieldid);
uint32_t writeFieldStop		(CompactProtocol *prot);

void
setLastFieldId(CompactProtocol *prot, int16_t fieldId);

uint32_t writeStructBegin    (CompactProtocol *prot);
uint32_t writeStructEnd      (CompactProtocol *prot);
#endif /* CDBPARQUETFOOTERSERIALIZER_PROTOCOL_H_ */
