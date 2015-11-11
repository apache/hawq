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
 * cdbparquetbitstreamutils.h
 *
 *  Created on: Aug 22, 2013
 *      Author: malili
 */

#ifndef CDBPARQUETBITSTREAMUTILS_H_
#define CDBPARQUETBITSTREAMUTILS_H_
#include "c.h"

/*
 * A byte writer to write one or more bytes into its underlying buffer.
 * The capacity will increase automaticly when needed.
 */
typedef struct CapacityByteWriter
{
    uint8_t *buffer;
    int     bufferPos;
    int     capacity;
} CapacityByteWriter;

extern void CapacityByteWriter_Init(CapacityByteWriter *writer, int capacity);

extern void CapacityByteWriter_WriteSingle(CapacityByteWriter *writer, uint8_t value);

extern void CapacityByteWriter_WriteMany(CapacityByteWriter *writer,
                                         uint8_t *values, int offset, int len);

/*
 * Write an unsigned int in Vlq format.
 */
extern void writeUnsignedVarInt(CapacityByteWriter *writer, int value);

/*
 * Write a little endian int to out, using the
 * number of bytes required by bit width.
 */
extern void writeIntLittleEndianPaddedOnBitWidth(CapacityByteWriter *writer, int value, int bitWidth);

/*
 * Read an unsigned int in Vlq format into `val`,
 * return number of bytes read.
 */
extern int readUnsignedVarInt(uint8_t *in, int *val);

/*
 * Read a little endian int from `in` into `val`, using
 * number of bytes required by bit width.
 *
 * Return number of bytes read.
 */
extern int readIntLittleEndianPaddedOnBitWidth(int bitWidth, uint8_t *in, int *val);

/*
 * return the number of bits needed to encode an int given the max value
 */
extern uint32_t widthFromMaxInt(uint32_t bound);

#endif /* CDBPARQUETBITSTREAMUTILS_H_ */
