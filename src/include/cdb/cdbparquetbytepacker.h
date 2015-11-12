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
 * cdbparquetbytepacker.h
 *
 *  Created on: Aug 22, 2013
 *      Author: malili
 */

#ifndef CDBPARQUETBYTEPACKER_H_
#define CDBPARQUETBYTEPACKER_H_

typedef struct ByteBasedBitPackingEncoder
{
	int bitWidth;

	uint8_t	*buffer;        /* contains encoded values */
	int		bufferPos;
	int		bufferSize;     /* bufferSize should be multiple of slabSize */
	int		slabSize;       /* slabSize should be multiple of bitwidth */

	int32_t	inputBuffer[8]; /* cache 8 input values and then pack into `bitWidth` bytes */
	int		inputBufferPos;

	int		numValues;      /* total number of values written */
} ByteBasedBitPackingEncoder;

typedef struct ByteBasedBitPackingDecoder
{
	int bitWidth;

    /* TODO stores inputSize to avoid buffer overflow */
	uint8_t *encode;        /* contains input of encoded values */
	int		encodePos;

	int32_t decode[8];      /* buffers output values */
	int		decodePos;
} ByteBasedBitPackingDecoder;

extern void BitPack_InitEncoder(ByteBasedBitPackingEncoder *encoder, int bitWidth);
extern void BitPack_InitDecoder(ByteBasedBitPackingDecoder *decoder, uint8_t *buffer, int bitWidth);

/* 
 * Write an int to the bitpack encoder, return number of bytes added
 * to the underlying buffer.
 *
 * It's caller's responsibility to make sure the int value
 * is within the bit width of the encoder.
 */
extern int BitPack_WriteInt(ByteBasedBitPackingEncoder *encoder, int value);

/* read an int from the bitpack decoder */
extern int BitPack_ReadInt(ByteBasedBitPackingDecoder *decoder);

/*
 * Flush any pending value to underlying buffer, return number of
 * bytes added to the underlying buffer.
 */
extern int BitPack_Flush(ByteBasedBitPackingEncoder *encoder);

/* size (number of bytes) of the data as it would be written */
extern int BitPack_Size(ByteBasedBitPackingEncoder *encoder);

/*
 * Get underlying buffer of the encoded value.
 */
extern uint8_t *BitPack_Data(ByteBasedBitPackingEncoder *encoder);

extern void pack8Values(int bitWidth, int32_t *in, int inPos, uint8_t* out, int outPos);
extern void unpack8Values(int bitWidth, uint8_t *in, int inPos, int32_t *out, int outPos);

#endif /* CDBPARQUETBYTEPACKER_H_ */
