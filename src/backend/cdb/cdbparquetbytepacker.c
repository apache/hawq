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
 * cdbparquetbytepacker.c
 *
 *  Created on: Aug 22, 2013
 *      Author: malili
 */

#include "postgres.h"
#include "cdb/cdbparquetbytepacker.h"

/* Currently these are Little Endian packer, which means
 * it packs and unpacks from the Least Significant Bit first */
static void pack8Values1Bits(int32_t *in, int inPos, uint8_t* out, int outPos);
static void pack8Values2Bits(int32_t *in, int inPos, uint8_t* out, int outPos);
static void unpack8Values1Bits(uint8_t *in, int inPos, int32_t *out, int outPos);
static void unpack8Values2Bits(uint8_t *in, int inPos, int32_t *out, int outPos);

#ifdef NOT_USED
static void pack8Values3Bits(int32_t *in, int inPos, uint8_t* out, int outPos);
static void pack8Values4Bits(int32_t *in, int inPos, uint8_t* out, int outPos);
static void pack8Values5Bits(int32_t *in, int inPos, uint8_t* out, int outPos);
static void pack8Values6Bits(int32_t *in, int inPos, uint8_t* out, int outPos);
static void pack8Values7Bits(int32_t *in, int inPos, uint8_t* out, int outPos);
static void pack8Values8Bits(int32_t *in, int inPos, uint8_t* out, int outPos);
static void unpack8Values3Bits(uint8_t *in, int inPos, int32_t *out, int outPos);
static void unpack8Values4Bits(uint8_t *in, int inPos, int32_t *out, int outPos);
static void unpack8Values5Bits(uint8_t *in, int inPos, int32_t *out, int outPos);
static void unpack8Values6Bits(uint8_t *in, int inPos, int32_t *out, int outPos);
static void unpack8Values7Bits(uint8_t *in, int inPos, int32_t *out, int outPos);
static void unpack8Values8Bits(uint8_t *in, int inPos, int32_t *out, int outPos);
#endif

static int paddedByteCountFromBits(int numBits);
static void pack(ByteBasedBitPackingEncoder *encoder);

int
paddedByteCountFromBits(int numBits)
{
	return (numBits + 7) / 8;
}

void
pack(ByteBasedBitPackingEncoder *encoder)
{
	/* make sure buffer has enough sapce */
	if (encoder->bufferPos == encoder->bufferSize)
	{
		encoder->buffer = repalloc(encoder->buffer, encoder->bufferSize + encoder->slabSize);
		memset(encoder->buffer + encoder->bufferSize, 0, encoder->slabSize);
		encoder->bufferSize += encoder->slabSize;
	}

	pack8Values(encoder->bitWidth,
				encoder->inputBuffer,
				0,
				encoder->buffer,
				encoder->bufferPos);
	encoder->bufferPos += encoder->bitWidth;
	encoder->inputBufferPos = 0;
}

void
BitPack_InitEncoder(ByteBasedBitPackingEncoder *encoder, int bitWidth)
{
	encoder->bitWidth = bitWidth;
	encoder->slabSize = bitWidth * 64 * 1024; /* must be multiple of bitWisth */
	encoder->bufferSize = encoder->slabSize;
	encoder->buffer   = (uint8_t *) palloc0(encoder->bufferSize);
	encoder->bufferPos = 0;
	encoder->inputBufferPos = 0;
	encoder->numValues = 0;
}

void
BitPack_InitDecoder(ByteBasedBitPackingDecoder *decoder, uint8_t *buffer, int bitWidth)
{
	decoder->bitWidth = bitWidth;
	decoder->encode = buffer;
	decoder->encodePos = 0;
	/* decodePos + 1 == 8 means to decode the next byte into 8 values */
	decoder->decodePos = 7;
}

int
BitPack_WriteInt(ByteBasedBitPackingEncoder *encoder, int value)
{
	int bytes_added = 0;
	encoder->inputBuffer[encoder->inputBufferPos++] = value;
	if (encoder->inputBufferPos == 8)
	{
		pack(encoder);
		bytes_added = encoder->bitWidth;
	}
	encoder->numValues++;
	return bytes_added;
}

int
BitPack_ReadInt(ByteBasedBitPackingDecoder *decoder)
{
	decoder->decodePos++;
	if (decoder->decodePos == 8)
	{
		unpack8Values(decoder->bitWidth,
					  decoder->encode,
					  decoder->encodePos,
					  decoder->decode,
					  0);
		/* we decode 8 values at a time, each value takes `bitWidth` bit.
		 * there it takes `bitWidth` bytes totally */
		decoder->encodePos += decoder->bitWidth;
		decoder->decodePos = 0;
	}
	return decoder->decode[decoder->decodePos];
}

int
BitPack_Flush(ByteBasedBitPackingEncoder *encoder)
{
	int i;
	if (encoder->inputBufferPos > 0)
	{
		for (i = encoder->inputBufferPos; i < 8; ++i)
		{
			encoder->inputBuffer[i] = 0;
		}
		pack(encoder);
		return encoder->bitWidth;
	}
	return 0;	/* no bytes added to buffer */
}

int
BitPack_Size(ByteBasedBitPackingEncoder *encoder)
{
	return paddedByteCountFromBits(encoder->numValues * encoder->bitWidth);
}

uint8_t *
BitPack_Data(ByteBasedBitPackingEncoder *encoder)
{
	return encoder->buffer;
}

void
pack8Values(int bitWidth, int32_t *in, int inPos, uint8_t* out, int outPos)
{
	switch(bitWidth){
	case 1:
		pack8Values1Bits(in, inPos, out, outPos);
		break;
	case 2:
		pack8Values2Bits(in, inPos, out, outPos);
		break;
#ifdef NOT_USED
	case 3:
		pack8Values3Bits(in, inPos, out, outPos);
		break;
	case 4:
		pack8Values4Bits(in, inPos, out, outPos);
		break;
	case 5:
		pack8Values5Bits(in, inPos, out, outPos);
		break;
	case 6:
		pack8Values6Bits(in, inPos, out, outPos);
		break;
	case 7:
		pack8Values7Bits(in, inPos, out, outPos);
		break;
	case 8:
		pack8Values8Bits(in, inPos, out, outPos);
		break;
	case 9:
		break;
	case 10:
		break;
	case 11:
		break;
	case 12:
		break;
	case 13:
		break;
	case 14:
		break;
	case 15:
		break;
	case 16:
		break;
	case 17:
		break;
	case 18:
		break;
	case 19:
		break;
	case 20:
		break;
	case 21:
		break;
	case 22:
		break;
	case 23:
		break;
	case 24:
		break;
	case 25:
		break;
	case 26:
		break;
	case 27:
		break;
	case 28:
		break;
	case 29:
		break;
	case 30:
		break;
	case 31:
		break;
	case 32:
		break;
#endif
	default:
		/*ereport error*/
		break;
	}
}

void
unpack8Values(int bitWidth, uint8_t *in, int inPos, int32_t *out, int outPos)
{
	switch(bitWidth){
	case 1:
		unpack8Values1Bits(in, inPos, out, outPos);
		break;
	case 2:
		unpack8Values2Bits(in, inPos, out, outPos);
		break;
#ifdef NOT_USED
	case 3:
		unpack8Values3Bits(in, inPos, out, outPos);
		break;
	case 4:
		unpack8Values4Bits(in, inPos, out, outPos);
		break;
	case 5:
		unpack8Values5Bits(in, inPos, out, outPos);
		break;
	case 6:
		unpack8Values6Bits(in, inPos, out, outPos);
		break;
	case 7:
		unpack8Values7Bits(in, inPos, out, outPos);
		break;
	case 8:
		unpack8Values8Bits(in, inPos, out, outPos);
		break;
	case 9:
		break;
	case 10:
		break;
	case 11:
		break;
	case 12:
		break;
	case 13:
		break;
	case 14:
		break;
	case 15:
		break;
	case 16:
		break;
	case 17:
		break;
	case 18:
		break;
	case 19:
		break;
	case 20:
		break;
	case 21:
		break;
	case 22:
		break;
	case 23:
		break;
	case 24:
		break;
	case 25:
		break;
	case 26:
		break;
	case 27:
		break;
	case 28:
		break;
	case 29:
		break;
	case 30:
		break;
	case 31:
		break;
	case 32:
		break;
#endif
	default:
		/*ereport error*/
		break;
	}
}

void
pack8Values1Bits(int32_t *in, int inPos, uint8_t* out, int outPos)
{
	out[0 + outPos] = (uint8_t) ((
				// [_______0]
				//        [0]
				((in[0 + inPos] & 1))
				// [______1_]
				//       [0]
			|	((in[1 + inPos] & 1) << 1)
				// [_____2__]
				//      [0]
			|	((in[2 + inPos] & 1) << 2)
				// [____3___]
				//     [0]
			| 	((in[3 + inPos] & 1) << 3)
				// [___4____]
				//    [0]
			|	((in[4 + inPos] & 1) << 4)
				// [__5_____]
				//   [0]
			|	((in[5 + inPos] & 1) << 5)
				// [_6______]
				//  [0]
			|	((in[6 + inPos] & 1) << 6)
				// [7_______]
				// [0]
			|	((in[7 + inPos] & 1) << 7)
	) & 255);
}

void
pack8Values2Bits(int32_t *in, int inPos, uint8_t* out, int outPos)
{
	out[0 + outPos] = (uint8_t) ((
				// [______10]
				//       [10]
				((in[0 + inPos] & 3))
			|	// [____32__]
				//     [10]
				((in[1 + inPos] & 3) << 2)
				// [__54____]
				//   [10]
			|	((in[2 + inPos] & 3) << 4)
				// [87______]
				// [10]
			|	((in[3 + inPos] & 3) << 6)
	) & 255);

	out[1 + outPos] = (uint8_t) ((
				// [______10]
				//       [10]
				((in[4 + inPos] & 3))
			|	// [____32__]
				//     [10]
				((in[5 + inPos] & 3) << 2)
				// [__54____]
				//   [10]
			|	((in[6 + inPos] & 3) << 4)
				// [87______]
				// [10]
			|	((in[7 + inPos] & 3) << 6)
	) & 255);
}

#ifdef NOT_USED
void
pack8Values3Bits(int32_t *in, int inPos, uint8_t* out, int outPos)
{
	out[0 + outPos] = (uint8_t)((
			//            [_____210]
			//                 [210]
			((in[0 + inPos] & 7))
			//            [__543___]
		    //              [210]
		|	((in[1 + inPos] & 7) <<  3)
			//            [76______]
			//           [_10]
		|	((in[2 + inPos] & 7) <<  6)) & 255);

	out[1 + outPos] = (uint8_t)((
			//            [_______0]
			//                   [2__]
			((in[2 + inPos] & 7) >> 2)
			//            [____321_]
			//                [210]
		|	((in[3 + inPos] & 7) <<  1)
			//            [_654____]
			//             [210]
		|	((in[4 + inPos] & 7) <<  4)
			//            [7_______]
			//          [__0]
		|	((in[5 + inPos] & 7) <<  7)) & 255);

	out[2 + outPos] = (uint8_t)((
			//            [______10]
			//                  [21_]
			((in[5 + inPos] & 7) >> 1)
			//            [___432__]
			//               [210]
		|	((in[6 + inPos] & 7) <<  2)
			//            [765_____]
			//            [210]
		|	((in[7 + inPos] & 7) <<  5)) & 255);
}
#endif

void
unpack8Values1Bits(uint8_t *in, int inPos, int32_t *out, int outPos)
{
	int i = ((int) in[inPos]) & 255;
	// [_______0]
	//        [0]
	out[0 + outPos] = (i >> 0) & 1;
	// [______1_]
	//       [0]
	out[1 + outPos] = (i >> 1) & 1;
	// [_____2__]
	//      [0]
	out[2 + outPos] = (i >> 2) & 1;
	// [____3___]
	//     [0]
	out[3 + outPos] = (i >> 3) & 1;
	// [___4____]
	//    [0]
	out[4 + outPos] = (i >> 4) & 1;
	// [__5_____]
	//   [0]
	out[5 + outPos] = (i >> 5) & 1;
	// [_6______]
	//  [0]
	out[6 + outPos] = (i >> 6) & 1;
	// [7_______]
	// [0]
	out[7 + outPos] = (i >> 7) & 1;
}

void
unpack8Values2Bits(uint8_t *in, int inPos, int32_t *out, int outPos)
{
	out[ 0 + outPos] =
		//            [______10]
		//                  [10]
		(((((int)in[ 0 + inPos]) & 255) ) & 3);
	out[ 1 + outPos] =
		//            [____32__]
		//                [10]
		(((((int)in[ 0 + inPos]) & 255) >> 2) & 3);
	out[ 2 + outPos] =
		//            [__54____]
		//              [10]
		(((((int)in[ 0 + inPos]) & 255) >> 4) & 3);
	out[ 3 + outPos] =
		//            [76______]
		//            [10]
		(((((int)in[ 0 + inPos]) & 255) >> 6) & 3);
	out[ 4 + outPos] =
		//            [______10]
		//                  [10]
		(((((int)in[ 1 + inPos]) & 255) ) & 3);
	out[ 5 + outPos] =
		//            [____32__]
		//                [10]
		(((((int)in[ 1 + inPos]) & 255) >> 2) & 3);
	out[ 6 + outPos] =
		//            [__54____]
		//              [10]
		(((((int)in[ 1 + inPos]) & 255) >> 4) & 3);
	out[ 7 + outPos] =
		//            [76______]
		//            [10]
		(((((int)in[ 1 + inPos]) & 255) >> 6) & 3);
}

#ifdef NOT_USED
void
unpack8Values3Bits(uint8_t *in, int inPos, int32_t *out, int outPos)
{
	out[ 0 + outPos] =
        //             [_____210]
        //                  [210]
        (((((int)in[ 0 + inPos]) & 255) ) & 7);
    out[ 1 + outPos] =
        //             [__543___]
        //               [210]
        (((((int)in[ 0 + inPos]) & 255) >> 3) & 7);
    out[ 2 + outPos] =
        //             [76______]
        //            [_10]
        (((((int)in[ 0 + inPos]) & 255) >> 6) & 7)
      | //             [_______0]
        //                    [2__]
        (((((int)in[ 1 + inPos]) & 255) << 2) & 7);
    out[ 3 + outPos] =
        //             [____321_]
        //                 [210]
        (((((int)in[ 1 + inPos]) & 255) >> 1) & 7);
    out[ 4 + outPos] =
        //             [_654____]
        //              [210]
        (((((int)in[ 1 + inPos]) & 255) >> 4) & 7);
    out[ 5 + outPos] =
        //             [7_______]
        //           [__0]
        (((((int)in[ 1 + inPos]) & 255) >> 7) & 7)
      | //             [______10]
        //                   [21_]
        (((((int)in[ 2 + inPos]) & 255) << 1) & 7);
    out[ 6 + outPos] =
        //             [___432__]
        //                [210]
        (((((int)in[ 2 + inPos]) & 255) >> 2) & 7);
    out[ 7 + outPos] =
        //             [765_____]
        //             [210]
        (((((int)in[ 2 + inPos]) & 255) >> 5) & 7);
}
#endif
