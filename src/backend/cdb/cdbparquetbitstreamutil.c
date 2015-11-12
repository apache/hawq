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
 * cdbparquetbitstreamutil.c
 *
 *  Created on: Aug 23, 2013
 *      Author: malili
 */

#include "postgres.h"
#include "cdb/cdbparquetbitstreamutils.h"


static void writeIntLittleEndianOnOneByte	(CapacityByteWriter *out, uint32_t value);
static void writeIntLittleEndianOnTwoBytes	(CapacityByteWriter *out, uint32_t value);

static int readIntLittleEndianOnOneByte(uint8_t *in);
static int readIntLittleEndianOnTwoBytes(uint8_t *in);

#ifdef NOT_USED
static void writeIntLittleEndianOnThreeBytes(CapacityByteWriter *out, uint32_t value);
static void writeIntLittleEndianOnFourBytes	(CapacityByteWriter *out, uint32_t value);
static int readIntLittleEndianOnThreeBytes(uint8_t *in);
static int readIntLittleEndianOnFourBytes(uint8_t *in);
#endif

static int paddedByteCountFromBits(int bitLength);

/* TODO make inline */
int paddedByteCountFromBits(int bitLength) {
	return (bitLength + 7) / 8;
}

uint32_t
widthFromMaxInt(uint32_t bound)
{
	int numberOfLeadingZeros = 0;

	if (bound == 0) return 32;

	/* 16 left bits are zero? */
	if ((bound & 0xffff0000) == 0) { numberOfLeadingZeros += 16; bound = bound << 16; }
	/* 8 left bits are zero? */
	if ((bound & 0xff000000) == 0) { numberOfLeadingZeros += 8;  bound = bound << 8; }
	/* 4 left bits are zero? */
	if ((bound & 0xf0000000) == 0) { numberOfLeadingZeros += 4;  bound = bound << 4; }
	/* 2 left bits are zero? */
	if ((bound & 0xc0000000) == 0) { numberOfLeadingZeros += 2;  bound = bound << 2; }
	/* top left bits are zero? */
	if ((bound & 0x80000000) == 0) { numberOfLeadingZeros += 1; }

	return 32 - numberOfLeadingZeros;
}

void
CapacityByteWriter_Init(CapacityByteWriter *writer, int capacity)
{
	writer->buffer = (uint8_t *) palloc0(capacity);
	writer->bufferPos = 0;
	writer->capacity = capacity;
}

void
CapacityByteWriter_WriteSingle(CapacityByteWriter *writer, uint8_t value)
{
	if (writer->bufferPos >= writer->capacity)
	{
		writer->buffer = repalloc(writer->buffer, writer->capacity * 2);
		memset(writer->buffer + writer->bufferPos, 0, writer->capacity);
		writer->capacity *= 2;
	}
	writer->buffer[writer->bufferPos++] = value;
}

void
CapacityByteWriter_WriteMany(CapacityByteWriter *writer,
							 uint8_t *values,
							 int offset,
							 int len)
{
	while (len--)
		CapacityByteWriter_WriteSingle(writer, values[offset++]);
}

void
writeUnsignedVarInt(CapacityByteWriter *writer, int value)
{
	while ((value & 0xFFFFFF80) != 0)
	{
		CapacityByteWriter_WriteSingle(writer, (uint8_t) ((value & 0x7F) | 0x80));
		value >>= 7;
	}
	CapacityByteWriter_WriteSingle(writer, (uint8_t) (value & 0x7F));
}

int
readUnsignedVarInt(uint8_t *in, int *val)
{
	int value, i, b;

	value = i = 0;
	while (((b = (int) *in) & 0x80) != 0)
	{
		value |= (b & 0x7f) << i;
		i += 7;
		in++;
	}

	*val = value | (b << i);
	return (i / 7) + 1;
}


void
writeIntLittleEndianPaddedOnBitWidth(CapacityByteWriter *writer, int value, int bitWidth)
{
	switch (paddedByteCountFromBits(bitWidth))
	{
	case 1:
		writeIntLittleEndianOnOneByte(writer, value);
		break;
	case 2:
		writeIntLittleEndianOnTwoBytes(writer, value);
		break;
#ifdef NOT_USED
	case 3:
		writeIntLittleEndianOnThreeBytes(writer, value);
		break;
	case 4:
		writeIntLittleEndianOnFourBytes(writer, value);
		break;
#endif
	default:
		/*ereport error*/
		break;
	}
}

int 
readIntLittleEndianPaddedOnBitWidth(int bitWidth, uint8_t *in, int *val)
{
	int bytes_to_read = paddedByteCountFromBits(bitWidth);
	switch (bytes_to_read)
	{
	case 1:
		*val = readIntLittleEndianOnOneByte(in);
		break;
	case 2:
		*val = readIntLittleEndianOnTwoBytes(in);
		break;
#ifdef NOT_USED
	case 3:
		*val = readIntLittleEndianOnThreeBytes(in);
		break;
	case 4:
		*val = readIntLittleEndianOnFourBytes(in);
		break;
#endif
	default:
		/* TODO raise error */
		return -1;
	}
	return bytes_to_read;
}

void
writeIntLittleEndianOnOneByte(CapacityByteWriter *out, uint32_t value)
{
	CapacityByteWriter_WriteSingle(out, (value >> 0) & 0xFF);
}

void
writeIntLittleEndianOnTwoBytes(CapacityByteWriter *out, uint32_t value)
{
	CapacityByteWriter_WriteSingle(out, (value >> 0) & 0xFF);
	CapacityByteWriter_WriteSingle(out, (value >> 8) & 0xFF);
}

int
readIntLittleEndianOnOneByte(uint8_t *in)
{
	int ch1 = (int) in[0];
	return ch1;
}

int
readIntLittleEndianOnTwoBytes(uint8_t *in)
{
	int ch1 = (int) in[0];
	int ch2 = (int) in[1];
	return ((ch2 << 8) + (ch1 << 0));
}

#ifdef NOT_USED
void
writeIntLittleEndianOnThreeBytes(CapacityByteWriter *out, uint32_t value)
{
	CapacityByteWriter_WriteSingle(out, (value >> 0) & 0xFF);
	CapacityByteWriter_WriteSingle(out, (value >> 8) & 0xFF);
	CapacityByteWriter_WriteSingle(out, (value >> 16) & 0xFF);
}

void
writeIntLittleEndianOnFourBytes(CapacityByteWriter *out, uint32_t value)
{
	CapacityByteWriter_WriteSingle(out, (value >> 0) & 0xFF);
	CapacityByteWriter_WriteSingle(out, (value >> 8) & 0xFF);
	CapacityByteWriter_WriteSingle(out, (value >> 16) & 0xFF);
	CapacityByteWriter_WriteSingle(out, (value >> 24) & 0xFF);
}

int 
readIntLittleEndianOnThreeBytes(uint8_t *in)
{
	int ch1 = (int) in[0];
	int ch2 = (int) in[1];
	int ch3 = (int) in[2];
	return ((ch3 << 16) + (ch2 << 8) + (ch1 << 0));
}

int 
readIntLittleEndianOnFourBytes(uint8_t *in)
{
	int ch1 = (int) in[0];
	int ch2 = (int) in[1];
	int ch3 = (int) in[2];
	int ch4 = (int) in[3];
	return ((ch4 << 24) + (ch3 << 16) + (ch2 << 8) + (ch1 << 0));
}
#endif
