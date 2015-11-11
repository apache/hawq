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
 * cdbparquetrleencoder.c
 *
 *  Created on: Aug 22, 2013
 *      Author: malili
 */

#include "postgres.h"
#include "cdb/cdbparquetrleencoder.h"

#define RDLEVEL_INIT_CAPACITY 1024

static void writeRLERun(RLEEncoder *encoder);
static void writeOrAppendBitPackedRun(RLEEncoder *encoder);
static void endPreviousBitPackedRun(RLEEncoder *encoder);

static void readNextRun(RLEDecoder *decoder);

void
RLEEncoder_Init(RLEEncoder *encoder, int bitWidth)
{
	encoder->bitWidth = bitWidth;
	encoder->packBuffer = (uint8_t *) palloc0(bitWidth);

	encoder->numBufferedValues			= 0;
	encoder->previousValue				= 0;
	encoder->repeatCount				= 0;
	encoder->bitPackedGroupCount 		= 0;
	/* -1 indicate that no current bit-packed-run */
	encoder->bitPackedRunHeaderPos		= -1;

	CapacityByteWriter_Init(&encoder->writer, /* capacity= */RDLEVEL_INIT_CAPACITY);
}

void
RLEEncoder_WriteInt(RLEEncoder *encoder, int32_t value)
{
	if (value == encoder->previousValue)
	{
		/* records how many times we have seen this value */
		encoder->repeatCount++;
		if (encoder->repeatCount >= 8)
		{
			/*
			 * we've seen this at least 8 times, we're
        	 * certainly going to write an rle-run,
    		 * so just keep on counting repeats for now
			 */
			return;
		}
	}
	else
	{
		/* This is a new value, check if it signals the end of an rle-run */
		if (encoder->repeatCount >= 8) {
			writeRLERun(encoder);
		}
		/* re record the repeat count and value */
		encoder->repeatCount = 1;
		encoder->previousValue = value;
	}

	/*
	 * We have not seen enough repeats to justify an rle-run yet,
	 * so buffer this value in case we decide to write a bit-packed-run
	 */
	encoder->bufferedValues[encoder->numBufferedValues++] = value;

	if (encoder->numBufferedValues == 8)
	{
		/*
		 * we've encountered less than 8 repeated values,
		 * so either start a new bit-packed-run or
		 * append to the current bit-packed-run
		 */
		writeOrAppendBitPackedRun(encoder);
	}
}

int
RLEEncoder_Flush(RLEEncoder *encoder)
{
	int i;

	/* write anything that is buffered / queued up for an rle-run */
	if (encoder->repeatCount >= 8)
	{
		writeRLERun(encoder);
	}
	else if (encoder->numBufferedValues > 0)
	{
		/* write buffered value to an bit-packed-run */
		for (i = encoder->numBufferedValues; i < 8; ++i)
			encoder->bufferedValues[i] = 0;
		writeOrAppendBitPackedRun(encoder);
		endPreviousBitPackedRun(encoder);
	}
	else
	{
		endPreviousBitPackedRun(encoder);
	}

	return encoder->writer.bufferPos;
}

uint8_t *
RLEEncoder_Data(RLEEncoder *encoder)
{
	return encoder->writer.buffer;
}

int
RLEEncoder_Size(RLEEncoder *encoder)
{
	return encoder->writer.bufferPos;
}

void
writeOrAppendBitPackedRun(RLEEncoder *encoder)
{
	if (encoder->bitPackedGroupCount >= BITPACK_RUN_MAX_GROUP_COUNT)
	{
		/*
		 * we've packed as many values as we can for this run,
		 * end it and start a new one
		 */
		endPreviousBitPackedRun(encoder);
	}

	if (encoder->bitPackedRunHeaderPos == -1)
	{
		/*
		 * this is a new bit-packed-run, allocate a byte for the header
		 * and keep a "pointer" to it so that it can be mutated later
		 */
		encoder->bitPackedRunHeaderPos = encoder->writer.bufferPos;
		CapacityByteWriter_WriteSingle(&encoder->writer, 0);
	}

	pack8Values(encoder->bitWidth,
				encoder->bufferedValues, 0,
				encoder->packBuffer, 0);

	CapacityByteWriter_WriteMany(&encoder->writer, encoder->packBuffer, 0, encoder->bitWidth);

	/* empty the buffer, they've all been written */
	encoder->numBufferedValues = 0;

	/* 
	 * clear the repeat count, as some repeated values,
	 * may have just been bit packed into this run
	 */
	encoder->repeatCount = 0;

	encoder->bitPackedGroupCount++;

}

/*
 * If we are currently writing a bit-packed-run, update the
 * bit-packed-header and consider this run to be over.
 *
 * Otherwise do nothing.
 */
void
endPreviousBitPackedRun(RLEEncoder *encoder)
{
	if (encoder->bitPackedRunHeaderPos == -1)
		return;

	/* create bit-packed-header, which needs to fit in 1 byte */
	uint8_t bitPackHeader = (uint8_t) ((encoder->bitPackedGroupCount << 1) | 1);

	/* update bit-packed-header */
	encoder->writer.buffer[encoder->bitPackedRunHeaderPos] = bitPackHeader;

	/* mark that this run is over */
	encoder->bitPackedRunHeaderPos = -1;

	/* reset the number of groups */
	encoder->bitPackedGroupCount = 0;
}


/**
 * write out a rle running
 */
void
writeRLERun(RLEEncoder *encoder)
{
	/*
	 * we may have been working on a bit-packed-run
	 * so close that run if it exists before writing this rle run
	 */
	endPreviousBitPackedRun(encoder);

	/* write the rle-header (lsb of 0 signifies a rle run) */
	writeUnsignedVarInt(&encoder->writer, encoder->repeatCount << 1);

	/* write the repeated-value */
	writeIntLittleEndianPaddedOnBitWidth(&encoder->writer,
										 encoder->previousValue,
										 encoder->bitWidth);
	/* reset the repeat count */
	encoder->repeatCount = 0;
	/* throw away all the buffered values,
	 * they were just repeats and they've been written */
	encoder->numBufferedValues = 0;
}

void
RLEDecoder_Init(RLEDecoder *decoder, int bitWidth, uint8_t *in, int inputSize)
{
	decoder->bitWidth	= bitWidth;
	decoder->input		= in;
	decoder->inputPos	= 0;
	decoder->inputSize	= inputSize;
}

int 
RLEDecoder_ReadInt(RLEDecoder *decoder)
{
	int result = -1;

	if (decoder->valueCount == 0)
	{
		readNextRun(decoder);
	}

	switch (decoder->mode)
	{
		case MODE_RLE:
			result = decoder->rleValue;
			break;
		case MODE_BITPACK:
			result = decoder->bitpackBuffer[decoder->bitpackBufferSize - decoder->valueCount];
			break;
		default:
			/* TODO raise error */
			break;
	}

	decoder->valueCount--;
	return result;
}

void 
readNextRun(RLEDecoder *decoder)
{
	int i, header, num_groups;

	if (decoder->inputPos >= decoder->inputSize)
	{
		return;	/* TODO raise error */
	}

	decoder->inputPos += readUnsignedVarInt(decoder->input + decoder->inputPos, &header);
	decoder->mode = ((header & 1) == 0) ? MODE_RLE : MODE_BITPACK;

	switch (decoder->mode)
	{
		case MODE_RLE:
			decoder->valueCount = header >> 1;
			decoder->inputPos += readIntLittleEndianPaddedOnBitWidth(decoder->bitWidth,
																	 decoder->input + decoder->inputPos,
																	 &decoder->rleValue);
			break;
		case MODE_BITPACK:
			/*
			 * each bit-pack group contains 8 packed values,
			 * which takes up `bitWidth` bytes when encoded
			 */
			num_groups = header >> 1;
			decoder->valueCount = num_groups * 8;
			decoder->bitpackBufferSize = decoder->valueCount;

			for (i = 0; i < decoder->valueCount; i += 8)
			{
				unpack8Values(decoder->bitWidth,
							  decoder->input,
							  decoder->inputPos,
							  decoder->bitpackBuffer,
							  i);
				decoder->inputPos += decoder->bitWidth;
			}
			break;
	}
}
