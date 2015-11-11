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
 * cdbparquetrleencoder.h
 *
 *  Created on: Aug 22, 2013
 *      Author: malili
 */

#ifndef CDBPARQUETRLEENCODER_H_
#define CDBPARQUETRLEENCODER_H_

#include "cdb/cdbparquetbitstreamutils.h"
#include "cdb/cdbparquetbytepacker.h"

#define BITPACK_RUN_MAX_GROUP_COUNT 63  /* 2^6 - 1 */
#define BITPACK_RUN_MAX_VALUE_COUNT 504 /* 63 * 8 */
/*
 * Encodes values using a combination of run length encoding and bit packing,
 * according to the following grammar:
 *
 * rle-bit-packed-hybrid := <length> <encoded-data>
 * length := length of the <encoded-data> in bytes stored as 4 bytes little endian
 * encoded-data := <run>*
 * run := <bit-packed-run> | <rle-run>
 * bit-packed-run := <bit-packed-header> <bit-packed-values>
 * bit-packed-header := varint-encode(<bit-pack-count> << 1 | 1)
 * // we always bit-pack a multiple of 8 values at a time, so we only store the number of values / 8
 * bit-pack-count := (number of values in this run) / 8
 * bit-packed-values :=  bit packed back to back, from LSB to MSB
 * rle-run := <rle-header> <repeated-value>
 * rle-header := varint-encode( (number of times repeated) << 1)
 * repeated-value := value that is repeated, using a fixed-width of round-up-to-next-byte(bit-width)
 *
 * Only supports values >= 0.
 *
 */
typedef struct RLEEncoder
{
    /*
     * The bit width used for bit-packing and for writing
     */
	int bitWidth;

    /*
     * Values that are bit packed 8 at at a time are packed into this
     * buffer, which is then written to bit writer
     */
    uint8_t *packBuffer;

    CapacityByteWriter writer;

    /*
     * We buffer 8 values at a time, and either bit pack them
     * or discard them after writing a rle-run
     */
	int32_t bufferedValues[8];
	int     numBufferedValues;

    /* Previous value written, used to detect repeated values */
	int32_t previousValue;

    /* How many times a value has been repeated */
	int     repeatCount;

	/**
	 * How many groups of 8 values have been written
	 * to the current bit-packed-run.
     *
     * the max group count is 63, which is 2^6 - 1
	 */
	int     bitPackedGroupCount;

    /**
     * record index of a single byte in underlying output buffer,
     * which we use as our bit-packed-header.
     *
     * We are only using one byte for this header,
     * which limits us to writing 504 values per bit-packed-run.
     *
     * MSB must be 0 for varint encoding, LSB must be 1 to signify
     * that this is a bit-packed-header leaves 6 bits to write the
     * number of 8-groups -> (2^6 - 1) * 8 = 504
     */
    int     bitPackedRunHeaderPos;

} RLEEncoder;

#define MODE_RLE 0
#define MODE_BITPACK 1

typedef struct RLEDecoder
{
	int bitWidth;

    /*
     * input buffer, contains encoded values
     */
    uint8_t *input;
    int     inputPos;
    int     inputSize;

    /*
     * which type of run we are at (RLE/BITPACK)
     */
    int mode;

    /*
     * how many values left in the current run
     */
    int valueCount;

    /*
     * for rle-run, we remember its repeated value
     */
    int rleValue;

    /*
     * for bit-packed-run, we unpacked all values in a buffer
     */
    int bitpackBuffer[BITPACK_RUN_MAX_VALUE_COUNT];
    int bitpackBufferSize;

} RLEDecoder;

/*-----------------------------------------
 * encoder API
 * ----------------------------------------*/

extern void RLEEncoder_Init(RLEEncoder *encoder, int bitWidth);

extern void RLEEncoder_WriteInt(RLEEncoder *encoder, int value);

/*
 * Flush any pending values to the result buffer,
 * return the size of underlying buffer.
 */
extern int RLEEncoder_Flush(RLEEncoder *encoder);

/*
 * Return buffer of encoded data. (not including <length> in the grammar)
 * Normally RLEEncoder_Flush should be called before this procudure.
 */
extern uint8_t *RLEEncoder_Data(RLEEncoder *encoder);

/*
 * Get the size of underlying buffer for encoded values.
 * Normally RLEEncoder_Flush should be called before this procudure.
 */
extern int RLEEncoder_Size(RLEEncoder *encoder);

/*-----------------------------------------
 * dncoder API
 * ----------------------------------------*/

extern void RLEDecoder_Init(RLEDecoder *decoder, int bitWidth, uint8_t *in, int inputSize);

extern int  RLEDecoder_ReadInt(RLEDecoder *decoder);

#endif /* CDBPARQUETRLEENCODER_H_ */
