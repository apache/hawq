/*
 * DatumStreamBlock
 *
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
 *
 */

#ifndef DATUMSTREAMBLOCK_H
#define DATUMSTREAMBLOCK_H

#include "catalog/pg_attribute.h"
#include "utils/guc.h"

typedef enum DatumStreamVersion
{
	DatumStreamVersion_Original =  0,		/* first valid version */
	DatumStreamVersion_Dense = 1,	
											/* Version used for denser header (higher
											 * row count) and RLE_TYPE compression
											 * done by this module.
											 */

	MaxDatumStreamVersion                    /* must always be last */
} DatumStreamVersion;

/* 
 * Datum Stream Block (Original).
 * 16 bytes header.  Followed by data.
 */
typedef struct DatumStreamBlock_Orig
{
    int16 version; 	/* version number */
    int16 flags;	/* some flags */
    int16 ndatum;	/* number of datum, including null */
    int16 unused;   /* unused */
    int32 nullsz; 	/* size nullbitmaps */
    int32 sz;	    /* logical data size, not including header, nullbitmap, and padding */
} DatumStreamBlock_Orig;

/* 
 * Datum Stream Block (Dense).
 *
 * Dense format uses an Append-Only Storage Block header that has a larger RowCount
 * field so we can store more NULL and/or represent more RLE_TYPE compressed items.
 *
 * 16 bytes header.
 */
typedef struct DatumStreamBlock_Dense
{
	struct orig_4_bytes
	{
		int16 version;	/* version number */
		int16 flags;	/* some flags */
	} orig_4_bytes;

	int32 	logical_row_count;	
						/*
						  * Number of physical datum PLUS number of NULLs; The logical
						  * count of rows. 
						  */

	int32	physical_datum_count;	
						/* Number of physical datum.  May be 0 if block is all NULLs. */

	int32	physical_data_size;	
						/* 
						  * Total data size of all physical datums in block.  Does not
						  * include header(s), bitmap(s), padding between headers and
						  * datum area, etc.
						  */

} DatumStreamBlock_Dense;

/* 
 * Datum Stream Block extension to DatumStreamBlock_Dense with RLE_TYPE compression data.
 * 16 bytes more.
 */
typedef struct DatumStreamBlock_Rle_Extension
{
	int32 	norepeats_null_bitmap_count;
						/*
						 * For non-RLE_TYPE compression, if there is a NULL bit-map,
						 * it will have row_count (see above) number of bits.
						 *
						 * However, for RLE_TYPE compression, we do not have a bunch
						 * of NULL bit-map bits for a repeated item.  We just use one bit
						 * (a 0, of course, indicating the repeated item is not NULL).  So,
						 * the NULL bit-map count is different than row_count.
						 */
	
	int32 	compress_bitmap_count;
						/*
						 * Number of bits in the compress bit-map.
						 */

	int32 	repeatcounts_count;
						/*
						 * Total number of repeated items.
						 *
						 * Also, the count of the ON bits in the compress bit-map.
						 */

	int32 	repeatcounts_size;
						/*
						 * Total size of the repeat counts array, when you account for
						 * the different 1, 2, 3, and 4 byte encoding size of each count.
						 */
} DatumStreamBlock_Rle_Extension;


/* Flags */
enum {
    DSB_HAS_NULLBITMAP = 		0x1,
	DSB_HAS_RLE_COMPRESSION = 	0x2,
};


typedef struct DatumStreamBitMapWrite
{
	uint8	*buffer;
	int32	bufferSize;

	uint8 	byteBit;
	uint8 	*bytePointer;
	int32 	bitOnCount;
	int32	bitCount;
} DatumStreamBitMapWrite;


typedef struct DatumStreamBitMapRead
{
	uint8	*buffer;

	uint8	byteBit;
	uint8	*bytePointer;
	int32	bitCount;
	int32	bitPosition;
} DatumStreamBitMapRead;


// Inline Forwards.
static inline void DatumStreamBitMapRead_Init(
	DatumStreamBitMapRead 	*bmr,
	uint8					*buffer,
	int32					bitCount);

static inline bool DatumStreamBitMapRead_IsExhausted(
	DatumStreamBitMapRead 	*bmr);

static inline void DatumStreamBitMapRead_Next(
	DatumStreamBitMapRead 	*bmr);

static inline bool DatumStreamBitMapRead_CurrentIsOn(
	DatumStreamBitMapRead 	*bmr);

/*
 * DatumStreamBitMap routines.
 */

static inline int32 DatumStreamBitMap_Size(
	int32 bitCount)
{
	return ((bitCount + 7) >> 3);
}

static inline int32 DatumStreamBitMap_CountOn(
	uint8					*buffer,
	int32					bitCount)
{
	DatumStreamBitMapRead bmr;

	int count;

	DatumStreamBitMapRead_Init(
							&bmr,
							buffer,
							bitCount);

	/*
	 * Scan the whole bit-map.
	 */
	count = 0;
	while (!DatumStreamBitMapRead_IsExhausted(&bmr))
	{
		DatumStreamBitMapRead_Next(&bmr);
		if (DatumStreamBitMapRead_CurrentIsOn(&bmr))
		{
			count++;
		}
	}

	return count;
}
/*
 * DatumStreamBitMapWrite routines.
 */

// Forward.
static inline void DatumStreamBitMapWrite_Set(
	DatumStreamBitMapWrite 	*bmw);

/*
 * Initialize writing a bit-map.
 *
 * Writing is automatically positioned BEFORE THE FIRST BIT.  
 *
 * Use DatumStreamBitMapWrite_AddBit to add a bit to the bit-map.
 */
static inline void DatumStreamBitMapWrite_Init(
	DatumStreamBitMapWrite 	*bmw,
	uint8					*buffer,
	int32					bufferSize)
{
	bmw->buffer = buffer;
	bmw->bufferSize = bufferSize;

	bmw->bytePointer = buffer;
	bmw->byteBit = 0;
	bmw->bitOnCount = 0;
	bmw->bitCount = 0;
}

static inline void DatumStreamBitMapWrite_ZeroFill(
	DatumStreamBitMapWrite 	*bmw,
	int32					bitCount)
{
	int32 newSize;
	int32 bitNumberInByte;

	/*
	 * Assert the bit-map is newly initialized.
	 */
	Assert(bmw->byteBit == 0);
	Assert(bmw->bitOnCount == 0);
	Assert(bmw->bitCount == 0);

	if (bitCount > 0)
	{
		/*
		 * Assert we are not going beyond the maximum buffer size.
		 */
		newSize = DatumStreamBitMap_Size(bitCount);
#ifdef USE_ASSERT_CHECKING
		if (newSize > bmw->bufferSize)
		{
			elog(ERROR, 
				 "New fill bit-map buffer size %d greater than maximum buffer size %d",
				 newSize,
				 bmw->bufferSize);
		}
#endif

		/*
		 * Zero fill out and set the write variables.
		 */
	    memset(bmw->buffer, 0, newSize);

		bitNumberInByte = (bitCount - 1) & 7;
		bmw->byteBit = 1 << bitNumberInByte;

		bmw->bytePointer = &bmw->buffer[newSize - 1];
		
		bmw->bitCount = bitCount;
	}
}

static inline void DatumStreamBitMapWrite_AddBit(
	DatumStreamBitMapWrite 	*bmw,
	bool					on)
{
	if (bmw->bitCount == 0)
	{
		Assert(bmw->byteBit == 0);

		// Zero first byte of bit-map.
		(*bmw->bytePointer) = 0;

		bmw->byteBit = 1;
	}
	else
	{
	    bmw->byteBit <<= 1;

	    if(bmw->byteBit == 0)
	    {
	    	// Zero next byte of bit-map
	        *(++bmw->bytePointer) = 0;

	        bmw->byteBit = 1;
	    }
	}
	bmw->bitCount++;

	if (on)
	{
		DatumStreamBitMapWrite_Set(bmw);
	}
}

static inline bool DatumStreamBitMapWrite_CurrentIsOn(
	DatumStreamBitMapWrite 	*bmw)
{
	return (((*bmw->bytePointer) & bmw->byteBit) != 0);
}

static inline void DatumStreamBitMapWrite_Set(
	DatumStreamBitMapWrite 	*bmw)
{
	Assert(!DatumStreamBitMapWrite_CurrentIsOn(bmw));
	(*bmw->bytePointer) |= bmw->byteBit;

	bmw->bitOnCount++;
}

static inline int32 DatumStreamBitMapWrite_OnCount(
	DatumStreamBitMapWrite 	*bmw)
{
	return bmw->bitOnCount;
}

static inline int32 DatumStreamBitMapWrite_Count(
	DatumStreamBitMapWrite 	*bmw)
{
	return bmw->bitCount;
}

static inline int32 DatumStreamBitMapWrite_Size(
	DatumStreamBitMapWrite 	*bmw)
{
	return DatumStreamBitMap_Size(bmw->bitCount);
}

static inline int32 DatumStreamBitMapWrite_NextSize(
	DatumStreamBitMapWrite 	*bmw)
{
	return DatumStreamBitMap_Size(bmw->bitCount + 1);
}

static inline int32 DatumStreamBitMapWrite_MaxSize(
	DatumStreamBitMapWrite 	*bmw)
{
	return bmw->bufferSize;
}

static inline void DatumStreamBitMapWrite_CopyToLargerBuffer(
	DatumStreamBitMapWrite 	*bmw,
	uint8					*newBuffer,
	int32					newBufferSize)
{
	Assert(newBufferSize > bmw->bufferSize);

	memcpy(newBuffer, bmw->buffer, DatumStreamBitMapWrite_Size(bmw));
	
	bmw->bytePointer = newBuffer + (bmw->bytePointer - bmw->buffer);

	bmw->buffer = newBuffer;
	bmw->bufferSize = newBufferSize;
}

/*
 * DatumStreamBitMapRead routines.
 */
	
/*
 * Initialize reading a bit-map.
 *
 * Reading is automatically positioned BEFORE THE FIRST BIT.
 */
static inline void DatumStreamBitMapRead_Init(
	DatumStreamBitMapRead 	*bmr,
	uint8					*buffer,
	int32					bitCount)
{
	Assert(bitCount >= 0);

	bmr->buffer =  buffer;

	bmr->bytePointer = buffer;
	bmr->byteBit = 0;
	bmr->bitCount = bitCount;
	bmr->bitPosition = -1;
}

static inline bool DatumStreamBitMapRead_IsExhausted(
	DatumStreamBitMapRead 	*bmr)
{
	return (bmr->bitPosition >= bmr->bitCount - 1);
}

static inline bool DatumStreamBitMapRead_InRange(
	DatumStreamBitMapRead 	*bmr)
{
	return (bmr->bitPosition < bmr->bitCount);
}

static inline bool DatumStreamBitMapRead_CurrentIsOn(
	DatumStreamBitMapRead 	*bmr)
{
	// PERFORMANCE EXPERIMENT: Only do integrity and trace checking for DEBUG builds...
#ifdef USE_ASSERT_CHECKING
	if (bmr->bitPosition >= bmr->bitCount)
	{
		elog(ERROR, "Past end of bitmap (bit position %d, total bit count %d)",
			 bmr->bitPosition,
			 bmr->bitCount);
	}
#endif
	return (((*bmr->bytePointer) & bmr->byteBit) != 0);
}

static inline int32 DatumStreamBitMapRead_Position(
	DatumStreamBitMapRead 	*bmr)
{
	return bmr->bitPosition;
}

static inline void DatumStreamBitMapRead_Next(
	DatumStreamBitMapRead 	*bmr)
{
	// PERFORMANCE EXPERIMENT: Only do integrity and trace checking for DEBUG builds...
#ifdef USE_ASSERT_CHECKING
	if (bmr->bitPosition >= bmr->bitCount)
	{
		elog(ERROR, "Moving past end of bitmap (bit position %d, total bit count %d)",
			 bmr->bitPosition,
			 bmr->bitCount);
	}
#endif

	if (bmr->bitPosition == -1)
	{
		Assert(bmr->byteBit == 0);
		Assert(bmr->bytePointer == bmr->buffer);

		bmr->byteBit = 1;
	}
	else
	{
	    bmr->byteBit <<= 1;

	    if(bmr->byteBit == 0)
	    {
	        ++bmr->bytePointer;

	        bmr->byteBit = 1;
	    }
	}
	bmr->bitPosition++;
}

static inline int32 DatumStreamBitMapRead_Size(
	DatumStreamBitMapRead 	*bmr)
{
	return DatumStreamBitMap_Size(bmr->bitCount);
}

static inline int32 DatumStreamBitMapRead_Count(
	DatumStreamBitMapRead 	*bmr)
{
	return bmr->bitCount;
}

/*
 * DatumStreamBlockInt32Compress.
 */
#define Int32Compress_MaxByteLen 4
#define Int32Compress_1ByteLimit 0x3F
#define Int32Compress_2ByteLimit 0x3FFF
#define Int32Compress_3ByteLimit 0x3FFFFF
#define Int32Compress_4ByteLimit 0x3FFFFFFF
#define Int32Compress_LenFieldMask 0xC0
#define Int32Compress_LenFieldShift 6

inline static int32 DatumStreamInt32Compress_Decode(
	uint8 	*buffer,
	int32	*byteLen);
#ifdef USE_ASSERT_CHECKING
// Currently only used in an Assert expression.
inline static int32 DatumStreamInt32Compress_DecodeLen(
	uint8 	*buffer);
#endif

inline static int32 DatumStreamInt32Compress_Size(int32 value)
{
	if (value <= Int32Compress_1ByteLimit)
	{
		return 1;
	}
	else if (value <= Int32Compress_2ByteLimit)
	{
		return 2;
	}
	else if (value <= Int32Compress_3ByteLimit)
	{
		return 3;
	}
	else
	{
		Assert(value <= Int32Compress_4ByteLimit);
		return 4;
	}
}

inline static int32 DatumStreamInt32Compress_Encode(
	uint8 	*buffer,
	int32 	value)
{
	int32	byteLen;

	if (value <= Int32Compress_1ByteLimit)
	{
		buffer[0] = (uint8)value;
		byteLen = 1;
	}
	else if (value <= Int32Compress_2ByteLimit)
	{
		buffer[0] = (1 << Int32Compress_LenFieldShift) | (uint8)(value >> 8);
		buffer[1] = (uint8)(value);
		byteLen = 2;
	}
	else if (value <= Int32Compress_3ByteLimit)
	{
		buffer[0] = (2 << Int32Compress_LenFieldShift) | (uint8)(value >> 16);
		buffer[1] = (uint8)(value >> 8);
		buffer[2] = (uint8)(value);
		byteLen = 3;
	}
	else
	{
		Assert(value <= Int32Compress_4ByteLimit);

		buffer[0] = (3 << Int32Compress_LenFieldShift) | (uint8)(value >> 24);
		buffer[1] = (uint8)(value >> 16);
		buffer[2] = (uint8)(value >> 8);
		buffer[3] = (uint8)(value);
		byteLen = 4;
	}

#ifdef USE_ASSERT_CHECKING
	{
		int32 decodeLen;
		int32 decodedValue;
		int32 decodedLen;

		decodeLen = DatumStreamInt32Compress_DecodeLen(buffer);
		if (decodeLen != byteLen)
		{
			elog(ERROR, "integer compress encode / decode length difference (expected %d, found %d)",
				 byteLen, decodeLen);
		}

		decodedValue = DatumStreamInt32Compress_Decode(buffer, &decodedLen);
		if (decodedValue != value)
		{
			elog(ERROR, "integer compress encode / decode value difference (expected %d, found %d)",
				 value, decodedValue);
		}

		if (decodedLen != byteLen)
		{
			elog(ERROR, "integer compress encode / decode length difference #2 (expected %d, found %d)",
				 byteLen, decodedLen);
		}
	}
#endif
	return byteLen;
}

#ifdef USE_ASSERT_CHECKING
// Currently only used in an Assert expression.
inline static int32 DatumStreamInt32Compress_DecodeLen(
	uint8 	*buffer)
{
	uint8	masked;
	uint8	lenFieldOnly;

	masked = (buffer[0] & Int32Compress_LenFieldMask);
	lenFieldOnly = masked >> Int32Compress_LenFieldShift;

	return lenFieldOnly + 1;
}
#endif

inline static int32 DatumStreamInt32Compress_Decode(
	uint8 	*buffer,
	int32	*byteLen)
{
	uint8	masked;
	uint8	lenFieldOnly;
	int32	len;
	int32	value = 0;

	masked = (buffer[0] & Int32Compress_LenFieldMask);
	lenFieldOnly = masked >> Int32Compress_LenFieldShift;
	len = lenFieldOnly + 1;
	switch (len)
	{
	case 1:
		value = (buffer[0] & ~Int32Compress_LenFieldMask);
		break;
	case 2:
		value = ((int32)(buffer[0] & ~Int32Compress_LenFieldMask) << 8) |
				buffer[1];
		break;
	case 3:
		value = ((int32)(buffer[0] & ~Int32Compress_LenFieldMask) << 16) |
				((int32)buffer[1] << 8) |
				buffer[2];
		break;
	case 4:
		value = ((int32)(buffer[0] & ~Int32Compress_LenFieldMask) << 24) |
				((int32)buffer[1] << 16) |
				((int32)buffer[2] << 8) |
				buffer[3];
		break;
	default:
		ereport(FATAL,
				(errmsg("Unexpected compressed integer byte length %d", 
			 			len)));
		break;
	}

	*byteLen = len;

	return value;
}

typedef struct DatumStreamTypeInfo
{
    /* Info determined by schema */
    int32 datumlen;		    /* Datum length */
    int32 typid;            /* type id */
    char  align;		    /* Align */
    bool  byval;            /* if it is a by value type */ 
} DatumStreamTypeInfo;

#define MAXREPEAT_COUNT 0x3FFFFFFF

#define DatumStreamBlockWrite_Eyecatcher "DBW"
#define DatumStreamBlockWrite_EyecatcherLen 4

typedef struct DatumStreamBlockWrite
{
	char		eyecatcher[DatumStreamBlockWrite_EyecatcherLen];	
								/* 
								 * Used to validate this is a DatumStreamBlockWrite
								 * reference.  3 characters plus NUL.
								 */

	DatumStreamTypeInfo	*typeInfo;

	/*
	 * Version of datum stream  block format -- original or RLE_TYPE,
	 */
	DatumStreamVersion	datumStreamVersion;

	bool	rle_want_compression;

	int32	initialMaxDatumPerBlock;
	int32	maxDatumPerBlock;

	int32	maxDataBlockSize;

	int		(*errdetailCallback) (void *errdetailArg);
	void	*errdetailArg;
	int		(*errcontextCallback) (void *errcontextArg);
	void	*errcontextArg;

    /* Common current pointer and null bit-map */
    int32 nth;		    /* Current count of datum in the block, including NULLs. */
	int32 physical_datum_count;
    bool  has_null;		/* if we have any NULLs at all */

	DatumStreamBitMapWrite	null_bitmap;

    uint8* datump;		/* pointer to datum */

	int32	always_null_bitmap_count;

	/* RLE_TYPE variables */
	bool 	rle_has_compression;

	uint8	*rle_last_item;
	int32	rle_last_item_size;
	bool	rle_last_item_is_repeated;

	int32	rle_total_repeat_items_written;

	DatumStreamBitMapWrite	rle_compress_bitmap;

	int32 	rle_repeatcounts_count;
	int32 	rle_repeatcounts_current_size;

    /* Common buffers */
    MemoryContext memctxt;

    uint8 *null_bitmap_buffer;
    int  null_bitmap_buffer_size;

    uint8 *datum_buffer;
    int32  datum_buffer_size;
	uint8 *datum_afterp;

    /* RLE_TYPE buffers */
    uint8 *rle_compress_bitmap_buffer;
    int32  rle_compress_bitmap_buffer_size;

    int32 *rle_repeatcounts;
    int32  rle_repeatcounts_maxcount;

    /* EOF of current file */
	int64 savings;
	int64 remember_savings;
} DatumStreamBlockWrite;

#define DatumStreamBlockRead_Eyecatcher "DBE"
#define DatumStreamBlockRead_EyecatcherLen 4

typedef struct DatumStreamBlockRead
{
	// A copy of the type information from DatumStream module since it is
	// referenced so often.
	DatumStreamTypeInfo	typeInfo;

	/*
	 * Version of datum stream  block format -- original or RLE_TYPE,
	 */
	DatumStreamVersion	datumStreamVersion;

    /* Common current pointer and null bit-map */
    int32 	nth;		    /* CURRENT position of datum in the block, including NULLs. */
	int32 	logical_row_count;		
							/* 
							 * Total number of datum in block, including NULLs.
							 */

	uint8* datump;		/* pointer to datum */
							
	int32	physical_datum_index;
	int32	physical_datum_count;

	uint8	*datum_beginp;
	uint8	*datum_afterp;

    bool  	has_null;		/* if we have any NULLs at all */

	/*
	 * Pointer to buffer containing the read data.
	 */
	uint8	*buffer_beginp;

	DatumStreamBitMapRead	null_bitmap;

	/*
	 * Information unpacked from the block header, etc.
	 */
	uint8	*null_bitmap_beginp;

	bool	rle_can_have_compression;

    uint8	*rle_compress_beginp;
	uint8	*rle_repeatcountsp;

	int32 	rle_norepeats_null_bitmap_count;
	int32 	rle_compress_bitmap_count;
	int32 	rle_repeatcounts_count;
	int32 	rle_repeatcounts_size;

	/* Dense write variables */
	bool 	rle_block_was_compressed;

	DatumStreamBitMapRead	rle_compress_bitmap;

	int32	rle_repeatcounts_index;
	bool	rle_in_repeated_item;
	int32	rle_repeated_item_count;

	int32	rle_total_repeat_items_read;

	/*
	 * Keep less frequently accessed fields down here for possible better CPU data cache
	 * performance.
	 */
	
	char		eyecatcher[DatumStreamBlockRead_EyecatcherLen];	
								/* 
								 * Used to validate this is a DatumStreamBlockRead
								 * reference.  3 characters plus NUL.
								 */

	int32	physical_data_size;

	int32	maxDataBlockSize;

	int		(*errdetailCallback) (void *errdetailArg);
	void	*errdetailArg;
	int		(*errcontextCallback) (void *errcontextArg);
	void	*errcontextArg;

	MemoryContext	memctxt;

} DatumStreamBlockRead;

extern char *DatumStreamVersion_String(DatumStreamVersion datumStreamVersion);

/*
 * varlena header info to string.
 */
extern char *VarlenaInfoToString(uint8 *p);
extern char *VarlenaInfoToString2(uint8 *p);

extern int errdetail_datumstreamblockread(
	DatumStreamBlockRead 	*dsr);

extern int errcontext_datumstreamblockread(
	DatumStreamBlockRead 	*dsr);

#ifdef USE_ASSERT_CHECKING
extern void
DatumStreamBlockRead_PrintVarlenaInfo(
								DatumStreamBlockRead *dsr,
								uint8 *p);
#endif

#ifdef USE_ASSERT_CHECKING
extern void DatumStreamBlockRead_CheckDenseGetInvariant(
	DatumStreamBlockRead 	*dsr);
#endif

/* Stream access method */
inline static void DatumStreamBlockRead_Get(DatumStreamBlockRead *dsr, Datum *datum, bool *null)
{
	// PERFORMANCE EXPERIMENT: Only do integrity and trace checking for DEBUG builds...
#ifdef USE_ASSERT_CHECKING
	if (strncmp(dsr->eyecatcher, DatumStreamBlockRead_Eyecatcher, DatumStreamBlockRead_EyecatcherLen) != 0)
		elog(FATAL, "DatumStreamBlockRead data structure not valid (eyecatcher)");
#endif

#ifdef USE_ASSERT_CHECKING
	if (dsr->datumStreamVersion == DatumStreamVersion_Dense)
	{
		DatumStreamBlockRead_CheckDenseGetInvariant(dsr);
	}
#endif

	if(dsr->has_null && DatumStreamBitMapRead_CurrentIsOn(&dsr->null_bitmap))
	{
		// PERFORMANCE EXPERIMENT: Only do integrity and trace checking for DEBUG builds...
#ifdef USE_ASSERT_CHECKING
		if (Debug_appendonly_print_scan_tuple)
		{
			ereport(LOG,
					(errmsg("Datum stream block %s read is returning NULL "
							"(nth %d)",			
							DatumStreamVersion_String(dsr->datumStreamVersion),
							dsr->nth),
					 errOmitLocation(true),
					 errdetail_datumstreamblockread(dsr),
					 errcontext_datumstreamblockread(dsr)));
		}
#endif

		*null = true;
		return;
	}
	else
		*null = false;

	if (dsr->typeInfo.datumlen < 0)
	{
#ifdef USE_ASSERT_CHECKING
		int32 varLen;
#endif
		*datum = PointerGetDatum(dsr->datump);
		Assert(VARATT_IS_SHORT_D(*datum) || !VARATT_IS_EXTERNAL_D(*datum));
	
		// PERFORMANCE EXPERIMENT: Only do integrity and trace checking for DEBUG builds...
#ifdef USE_ASSERT_CHECKING
		varLen = VARSIZE_ANY_D(*datum);
	
		if (varLen < 0 || varLen > dsr->physical_data_size)
		{
			ereport(ERROR,
					(errmsg("Datum stream block %s read variable-length item index %d length too large "
							"(nth %d, logical row count %d, "
							"item length %d, total physical data size %d, "
							"current datum pointer %p, after data pointer %p)",
							DatumStreamVersion_String(dsr->datumStreamVersion),
							dsr->physical_datum_index,
							dsr->nth,
							dsr->logical_row_count,
							varLen,
							dsr->physical_data_size,
							dsr->datump,
							dsr->datum_afterp),
					 errOmitLocation(false),
					 errdetail_datumstreamblockread(dsr),
					 errcontext_datumstreamblockread(dsr)));
		}
	
		if (dsr->datump + varLen > dsr->datum_afterp)
		{
			ereport(ERROR,
					(errmsg("Datum stream block %s read variable-length item index %d length goes beyond end of block "
							"(nth %d, logical row count %d, "
							"item length %d, "
							"current datum pointer %p, after data pointer %p)",
							DatumStreamVersion_String(dsr->datumStreamVersion),
							dsr->physical_datum_index,
							dsr->nth,
							dsr->logical_row_count,
							varLen,
							dsr->datump,
							dsr->datum_afterp),
					 errOmitLocation(false),
					 errdetail_datumstreamblockread(dsr),
					 errcontext_datumstreamblockread(dsr)));
		}
	
		if (Debug_datumstream_read_print_varlena_info)
		{
			DatumStreamBlockRead_PrintVarlenaInfo(
												dsr,
												dsr->datump);
		}
	
		if (Debug_appendonly_print_scan_tuple)
		{
			ereport(LOG,
					(errmsg("Datum stream block %s read is returning variable-length item #%d "
							"(nth %d, item begin %p, item offset " INT64_FORMAT ")",
							DatumStreamVersion_String(dsr->datumStreamVersion),
							dsr->physical_datum_index,
							dsr->nth,
							dsr->datump,
							(int64)(dsr->datump - dsr->datum_beginp)),
					 errOmitLocation(true),
					 errdetail_datumstreamblockread(dsr),
					 errcontext_datumstreamblockread(dsr)));
		}
#endif
	}
	else if (!dsr->typeInfo.byval)
	{
		*datum = PointerGetDatum(dsr->datump);
		
			// PERFORMANCE EXPERIMENT: Only do integrity and trace checking for DEBUG builds...
#ifdef USE_ASSERT_CHECKING
		if (Debug_appendonly_print_scan_tuple)
		{
			ereport(LOG,
					(errmsg("Datum stream block %s read is returning fixed-length item #%d "
							"(nth %d, item size %d, item begin %p, item offset " INT64_FORMAT ")",	
							DatumStreamVersion_String(dsr->datumStreamVersion),
							dsr->physical_datum_index,
							dsr->nth,
							dsr->typeInfo.datumlen,
							dsr->datump,
							(int64)(dsr->datump - dsr->datum_beginp)),
					 errOmitLocation(true),
					 errdetail_datumstreamblockread(dsr),
					 errcontext_datumstreamblockread(dsr)));
		}
#endif
		
	}
	else
	{
		/*
		 * Performance is so critical we don't use a switch statement here.
		 */
		if (dsr->typeInfo.datumlen == 1)
		{
			*datum = *(uint8 *) dsr->datump;
		}
		else if (dsr->typeInfo.datumlen == 2)
		{
			Assert(IsAligned(dsr->datump, 2));
			*datum = *(uint16 *) dsr->datump;
		}
		else if (dsr->typeInfo.datumlen == 4)
		{
			Assert(IsAligned(dsr->datump, 4));
			*datum = *(uint32 *) dsr->datump;
		}
		else if (dsr->typeInfo.datumlen == 8)
		{
			Assert(IsAligned(dsr->datump, 8) || IsAligned(dsr->datump, 4));
			*datum = *(Datum *) dsr->datump;
		}
		else
		{
			*datum = 0;
			Assert(false);
		}

			// PERFORMANCE EXPERIMENT: Only do integrity and trace checking for DEBUG builds...
#ifdef USE_ASSERT_CHECKING
		if (Debug_appendonly_print_scan_tuple)
		{
			ereport(LOG,
					(errmsg("Datum stream block %s read is returning fixed-length item #%d "
							"(nth %d, item size %d, item begin %p, item offset " INT64_FORMAT ", integer " INT64_FORMAT ")",			
							DatumStreamVersion_String(dsr->datumStreamVersion),
							dsr->physical_datum_index,
							dsr->nth,
							dsr->typeInfo.datumlen,
							dsr->datump,
							(int64)(dsr->datump - dsr->datum_beginp),
							(int64)*datum),
					 errOmitLocation(true),
					 errdetail_datumstreamblockread(dsr),
					 errcontext_datumstreamblockread(dsr)));
		}
#endif
		
	}
}

inline static int DatumStreamBlockRead_AdvanceOrig(DatumStreamBlockRead *dsr)
{
    Assert(dsr);
	
    ++dsr->nth;		// Initially, -1.

    /* Advance out of bounds? */
    if(dsr->nth >= dsr->logical_row_count)
        return 0;

    if(dsr->has_null)
    {
		DatumStreamBitMapRead_Next(&dsr->null_bitmap);
		Assert(DatumStreamBitMapRead_InRange(&dsr->null_bitmap));

        if (DatumStreamBitMapRead_CurrentIsOn(&dsr->null_bitmap))
		{
			// PERFORMANCE EXPERIMENT: Only do integrity and trace checking for DEBUG builds...
#ifdef USE_ASSERT_CHECKING
			if (Debug_appendonly_print_scan_tuple)
			{
				ereport(LOG,
						(errmsg("Datum stream block read is positioned to NULL "
							    "(nth %d)",
							    dsr->nth),
						 errOmitLocation(true),
						 errdetail_datumstreamblockread(dsr),
				         errcontext_datumstreamblockread(dsr)));
			}
#endif

			return 1;
		}
    }

	Assert(dsr->datump >= dsr->datum_beginp);
	Assert (dsr->datump < dsr->datum_afterp);

    ++dsr->physical_datum_index;		// Initially, -1.

	if (dsr->physical_datum_index == 0)
	{
		// Pre-positioned by block read to first item.
		
		// PERFORMANCE EXPERIMENT: Only do integrity and trace checking for DEBUG builds...
#ifdef USE_ASSERT_CHECKING
		if (Debug_appendonly_print_scan_tuple)
		{
			ereport(LOG,
					(errmsg("Datum stream block read advance is positioned to first item "
						    "(nth %d, logical row count %d, item begin %p, item offset " INT64_FORMAT ")",
						    dsr->nth,
						    dsr->logical_row_count,
						    dsr->datump,
						    (int64)(dsr->datump - dsr->datum_beginp)),
					 errOmitLocation(true),
					 errdetail_datumstreamblockread(dsr),
					 errcontext_datumstreamblockread(dsr)));
		}
#endif
	}
	else
	{
		// PERFORMANCE EXPERIMENT: Only do integrity and trace checking for DEBUG builds...
#ifdef USE_ASSERT_CHECKING
		uint8 *item_beginp;
		
		item_beginp = dsr->datump;
#endif

		/*
		 * Advance the item pointer.
		 */
	    if(dsr->typeInfo.datumlen < 0)
	    {
	        struct varlena *s = (struct varlena *) dsr->datump;

	        Assert(dsr->typeInfo.datumlen == -1);
			Assert(dsr->datump >= dsr->datum_beginp);
			Assert(dsr->datump < dsr->datum_afterp);

	        dsr->datump += VARSIZE_ANY(s);
			
			/* 
			 * Skip any possible zero paddings AFTER PREVIOUS varlena data.
			 */
	        if(*dsr->datump == 0)
			{
	            dsr->datump = (uint8 *) att_align(dsr->datump, dsr->typeInfo.align);
			}

			// PERFORMANCE EXPERIMENT: Only do integrity and trace checking for DEBUG builds...
#ifdef USE_ASSERT_CHECKING
			if (dsr->datump > dsr->datum_afterp)
			{
				ereport(ERROR,
						(errmsg("Datum stream block read pointer to variable-length item index %d out of bounds "
								"(nth %d, logical row count %d, "
								"current datum pointer %p, next datum pointer %p, after data pointer %p)",
							    dsr->physical_datum_index,
							    dsr->nth,
							    dsr->logical_row_count,
							    item_beginp,
							    dsr->datump,
							    dsr->datum_afterp),
						 errOmitLocation(false),
						 errdetail_datumstreamblockread(dsr),
				         errcontext_datumstreamblockread(dsr)));
			}
			
			if (Debug_appendonly_print_scan_tuple)
			{
				ereport(LOG,
						(errmsg("Datum stream block read advanced to variable-length item index %d "
							    "(nth %d, logical row count %d, "
							    "previous item begin %p, previous item offset " INT64_FORMAT ", next item begin %p)",
							    dsr->physical_datum_index,
							    dsr->nth,
							    dsr->logical_row_count,
							    item_beginp,
							    (int64)(item_beginp - dsr->datum_beginp),
							    dsr->datump),
						 errOmitLocation(true),
						 errdetail_datumstreamblockread(dsr),
				         errcontext_datumstreamblockread(dsr)));
			}
#endif
		}
	    else
		{
	        dsr->datump += dsr->typeInfo.datumlen;
			
				// PERFORMANCE EXPERIMENT: Only do integrity and trace checking for DEBUG builds...
#ifdef USE_ASSERT_CHECKING
			if (Debug_appendonly_print_scan_tuple)
			{
				ereport(LOG,
						(errmsg("Datum stream block read advanced to fixed-item item index %d "
							    "(nth %d, logical row count %d, "
							    "item size %d, previous item begin %p, previous item offset " INT64_FORMAT ", next item begin %p)",
							    dsr->physical_datum_index,
							    dsr->nth,
							    dsr->logical_row_count,
							    dsr->typeInfo.datumlen,
							    item_beginp,
							    (int64)(item_beginp - dsr->datum_beginp),
							    dsr->datump),
						 errOmitLocation(true),
						 errdetail_datumstreamblockread(dsr),
				         errcontext_datumstreamblockread(dsr)));
			}
#endif

		}
	}

    return 1;
}

inline static int DatumStreamBlockRead_AdvanceDense(DatumStreamBlockRead *dsr)
{
	Assert(dsr);

	++dsr->nth; 	// Initially, -1.

	/* Advance out of bounds? */
	if(dsr->nth >= dsr->logical_row_count)
		return 0;

	if (!dsr->rle_block_was_compressed)
	{
		/*
		 * A Dense block with optional NULL bit-map.
		 */
		if(dsr->has_null)
		{
			DatumStreamBitMapRead_Next(&dsr->null_bitmap);
			Assert(DatumStreamBitMapRead_InRange(&dsr->null_bitmap));
		
			if (DatumStreamBitMapRead_CurrentIsOn(&dsr->null_bitmap))
			{
				/*
				 * NULL item.
				 */
				return 1;
			}
		}
	}
	else
	{
		/*
		 * For RLE_TYPE compression, we only represent the repeated item once in
		 * the NULL bit-map.
		 */

		if (dsr->rle_in_repeated_item)
		{
			Assert(dsr->rle_repeated_item_count > 0);
#ifdef USE_ASSERT_CHECKING
			if (dsr->has_null)
			{
				if (!DatumStreamBitMapRead_InRange(&dsr->null_bitmap))
				{
					ereport(ERROR,
							(errmsg("Datum stream block read advance NULL bit-map out-of-range "
									"(nth %d, logical row count %d, NULL bit-map count %d)",
									dsr->nth,
									dsr->logical_row_count,
									DatumStreamBitMapRead_Count(&dsr->null_bitmap)),
							 errOmitLocation(true),
							 errdetail_datumstreamblockread(dsr),
							 errcontext_datumstreamblockread(dsr)));
				}
				if (DatumStreamBitMapRead_CurrentIsOn(&dsr->null_bitmap))
				{
					ereport(ERROR,
							(errmsg("Datum stream block read advance NULL bit-map ON for repeated item "
									"(nth %d, logical row count %d, NULL bit-map count %d)",
									dsr->nth,
									dsr->logical_row_count,
									DatumStreamBitMapRead_Count(&dsr->null_bitmap)),
							 errOmitLocation(true),
							 errdetail_datumstreamblockread(dsr),
							 errcontext_datumstreamblockread(dsr)));
				}
			}

			if (!DatumStreamBitMapRead_InRange(&dsr->rle_compress_bitmap))
			{
				ereport(ERROR,
						(errmsg("Datum stream block read advance COMPRESS bit-map out-of-range "
								"(nth %d, logical row count %d, COMPRESS bit-map count %d)",
								dsr->nth,
								dsr->logical_row_count,
								DatumStreamBitMapRead_Count(&dsr->rle_compress_bitmap)),
						 errOmitLocation(true),
						 errdetail_datumstreamblockread(dsr),
						 errcontext_datumstreamblockread(dsr)));
			}
			if (!DatumStreamBitMapRead_CurrentIsOn(&dsr->rle_compress_bitmap))
			{
				ereport(ERROR,
						(errmsg("Datum stream block read advance COMPRESS bit-map OFF for repeated item "
								"(nth %d, logical row count %d, COMPRESS bit-map count %d)",
								dsr->nth,
								dsr->logical_row_count,
								DatumStreamBitMapRead_Count(&dsr->rle_compress_bitmap)),
						 errOmitLocation(true),
						 errdetail_datumstreamblockread(dsr),
						 errcontext_datumstreamblockread(dsr)));
			}
#endif

			dsr->rle_repeated_item_count--;
			dsr->rle_total_repeat_items_read++;

			if (dsr->rle_repeated_item_count <= 0)
			{
				/*
				 * The nth item is the last of the repeated item.
				 *
				 * Leave the RLE_TYPE compression bit-map positions and
				 * optional NULL bit-map positions the same since they still represent
				 * the last copy of the repeated item.
				 */
				dsr->rle_in_repeated_item = false;
			}

			/*
			 * Item pointers are already setup.
			 */
			return 1;
		}

		/*
		 * The first step of advancing not being inside current repeated item is advancing the
		 * NULL bit-map.  
		 *
		 * (We only advance the RLE_TYPE compress bit-map inside a non-NULL item)
		 */
		if(dsr->has_null)
		{
			DatumStreamBitMapRead_Next(&dsr->null_bitmap);
#ifdef USE_ASSERT_CHECKING
			if (!DatumStreamBitMapRead_InRange(&dsr->null_bitmap))
			{
//				pg_usleep(300 * 1000000L);
				ereport(ERROR,
						(errmsg("Datum stream block read advance NULL bit-map out-of-range "
								"(nth %d, logical row count %d, NULL bit-map count %d, "
								"physical datum count %d, COMPRESS bit-map count %d)",
								dsr->nth,
								dsr->logical_row_count,
								DatumStreamBitMapRead_Count(&dsr->null_bitmap),
								dsr->physical_datum_count,
								DatumStreamBitMapRead_Count(&dsr->rle_compress_bitmap)),
						 errOmitLocation(true),
						 errdetail_datumstreamblockread(dsr),
						 errcontext_datumstreamblockread(dsr)));
			}
#endif
		
			if (DatumStreamBitMapRead_CurrentIsOn(&dsr->null_bitmap))
			{
				/*
				 * NULL item.
				 */
				return 1;
			}
		}

		/*
		 * Now that we have an item that is not NULL, see if it is repeated.
		 */
		DatumStreamBitMapRead_Next(&dsr->rle_compress_bitmap);
#ifdef USE_ASSERT_CHECKING
		if (!DatumStreamBitMapRead_InRange(&dsr->rle_compress_bitmap))
		{
			ereport(ERROR,
					(errmsg("Datum stream block read advance COMPRESS bit-map out-of-range "
							"(nth %d, logical row count %d, COMPRESS bit-map count %d)",
							dsr->nth,
							dsr->logical_row_count,
							DatumStreamBitMapRead_Count(&dsr->rle_compress_bitmap)),
					 errOmitLocation(true),
					 errdetail_datumstreamblockread(dsr),
					 errcontext_datumstreamblockread(dsr)));
		}
#endif
		if (DatumStreamBitMapRead_CurrentIsOn(&dsr->rle_compress_bitmap))
		{
			int32 repeatCount;
			int32 byteLen;

			/*
			 * New repeated item.
			 */
			dsr->rle_repeatcounts_index++;

			dsr->rle_total_repeat_items_read++;

			repeatCount = DatumStreamInt32Compress_Decode(dsr->rle_repeatcountsp, &byteLen);
			dsr->rle_repeatcountsp += byteLen;

			// UNDONE: Integrity check.

			dsr->rle_repeated_item_count = repeatCount;
			dsr->rle_in_repeated_item = true;
		}

		/*
		 * Fall through and setup pointer to new item.
		 */
	}

	Assert(dsr->datump >= dsr->datum_beginp);
	Assert (dsr->datump < dsr->datum_afterp);

	++dsr->physical_datum_index;		// Initially, -1.

	if (dsr->physical_datum_index == 0)
	{
		// Pre-positioned by block read to first item.
		
		// PERFORMANCE EXPERIMENT: Only do integrity and trace checking for DEBUG builds...
#ifdef USE_ASSERT_CHECKING
		if (Debug_appendonly_print_scan_tuple)
		{
			ereport(LOG,
					(errmsg("Datum stream block read advance is positioned to first item "
						    "(nth %d, logical row count %d, item begin %p, item offset " INT64_FORMAT ")",
						    dsr->nth,
						    dsr->logical_row_count,
						    dsr->datump,
						    (int64)(dsr->datump - dsr->datum_beginp)),
					 errOmitLocation(true),
					 errdetail_datumstreamblockread(dsr),
					 errcontext_datumstreamblockread(dsr)));
		}
#endif
	}
	else
	{
		// PERFORMANCE EXPERIMENT: Only do integrity and trace checking for DEBUG builds...
#ifdef USE_ASSERT_CHECKING
		uint8 *item_beginp;
		
		item_beginp = dsr->datump;
#endif

		/*
		 * Advance the item pointer.
		 */
		if(dsr->typeInfo.datumlen < 0)
		{
	        struct varlena *s;

	        Assert(dsr->typeInfo.datumlen == -1);
			Assert(dsr->datump >= dsr->datum_beginp);
			Assert(dsr->datump < dsr->datum_afterp);

	        s = (struct varlena *) dsr->datump;
			dsr->datump += VARSIZE_ANY(s);

			/* 
			 * Skip any possible zero paddings AFTER varlena data.
			 */
	        if(*dsr->datump == 0)
			{
	            dsr->datump = (uint8 *) att_align(dsr->datump, dsr->typeInfo.align);
			}

				// PERFORMANCE EXPERIMENT: Only do integrity and trace checking for DEBUG builds...
#ifdef USE_ASSERT_CHECKING
			if (dsr->datump > dsr->datum_afterp)
			{
				ereport(ERROR,
						(errmsg("Datum stream block read pointer to variable-length item index %d out of bounds "
								"(nth %d, logical row count %d, "
								"current datum pointer %p, next datum pointer %p, after data pointer %p)",
								dsr->physical_datum_index,
								dsr->nth,
								dsr->logical_row_count,
								item_beginp,
								dsr->datump,
								dsr->datum_afterp),
						 errOmitLocation(false),
						 errdetail_datumstreamblockread(dsr),
						 errcontext_datumstreamblockread(dsr)));
			}

			if (Debug_appendonly_print_scan_tuple)
			{
				ereport(LOG,
						(errmsg("Datum stream block read advanced to variable-length item index %d "
								"(nth %d, logical row count %d, "
								"previous item begin %p, previous item offset " INT64_FORMAT ", next item begin %p)",
								dsr->physical_datum_index,
								dsr->nth,
								dsr->logical_row_count,
								item_beginp,
								(int64)(item_beginp - dsr->datum_beginp),
								dsr->datump),
						 errOmitLocation(true),
						 errdetail_datumstreamblockread(dsr),
						 errcontext_datumstreamblockread(dsr)));
			}
#endif
		}
		else
		{
			dsr->datump += dsr->typeInfo.datumlen;
		}
	}

	return 1;
}

inline static int DatumStreamBlockRead_Advance(DatumStreamBlockRead *dsr)
{
		// PERFORMANCE EXPERIMENT: Only do integrity and trace checking for DEBUG builds...
#ifdef USE_ASSERT_CHECKING
	if (strncmp(dsr->eyecatcher, DatumStreamBlockRead_Eyecatcher, DatumStreamBlockRead_EyecatcherLen) != 0)
		elog(FATAL, "DatumStreamBlockRead data structure not valid (eyecatcher)");
#endif

	if (dsr->datumStreamVersion == DatumStreamVersion_Original)
	{
		return DatumStreamBlockRead_AdvanceOrig(dsr);
	}
	else
	{
		Assert(dsr->datumStreamVersion == DatumStreamVersion_Dense);
		return DatumStreamBlockRead_AdvanceDense(dsr);
	}
}



inline static int DatumStreamBlockRead_Nth(DatumStreamBlockRead *dsr)
{
	return dsr->nth;
}


extern void DatumStreamBlockRead_GetReadyOrig(
	DatumStreamBlockRead	*dsr,
	uint8					*buffer,
	int32					bufferSize,
	int64					firstRowNum,
	int32					rowCount,
	bool					*hadToAdjustRowCount,
	int32					*adjustedRowCount);
extern void DatumStreamBlockRead_GetReadyDense(
	DatumStreamBlockRead	*dsr,
	uint8					*buffer,
	int32					bufferSize,
	int64					firstRowNum,
	int32					rowCount,
	bool					*hadToAdjustRowCount,
	int32					*adjustedRowCount);

inline static void
DatumStreamBlockRead_GetReady(
	DatumStreamBlockRead	*dsr,
	uint8					*buffer,
	int32					bufferSize,
	int64					firstRowNum,
	int32					rowCount,
	bool					*hadToAdjustRowCount,
	int32					*adjustedRowCount)
{
	if (dsr->datumStreamVersion == DatumStreamVersion_Original)
	{
		return DatumStreamBlockRead_GetReadyOrig(
												dsr, 
												buffer, 
												bufferSize, 
												firstRowNum, 
												rowCount,
												hadToAdjustRowCount,
												adjustedRowCount);
	}
	else
	{
		Assert(dsr->datumStreamVersion == DatumStreamVersion_Dense);
		return DatumStreamBlockRead_GetReadyDense(
												dsr, 
												buffer, 
												bufferSize, 
												firstRowNum, 
												rowCount,
												hadToAdjustRowCount,
												adjustedRowCount);
	}
}

extern void DatumStreamBlockRead_ResetOrig(DatumStreamBlockRead *dsr);
extern void DatumStreamBlockRead_ResetDense(DatumStreamBlockRead *dsr);

inline static void DatumStreamBlockRead_Reset(DatumStreamBlockRead *dsr)
{
	if (dsr->datumStreamVersion == DatumStreamVersion_Original)
	{
		DatumStreamBlockRead_ResetOrig(dsr);
	}
	else
	{
		Assert(dsr->datumStreamVersion == DatumStreamVersion_Dense);
		DatumStreamBlockRead_ResetDense(dsr);
	}
}


extern void DatumStreamBlockRead_Init(
	DatumStreamBlockRead 	*dsr,
	DatumStreamTypeInfo 	*typeInfo,
	DatumStreamVersion		datumStreamVersion,
	bool 					rle_can_have_compression,
	int						(*errdetailCallback) (void *errdetailArg),
	void	   				*errdetailArg,
	int						(*errcontextCallback) (void *errcontextArg),
	void	   				*errcontextArg);
extern void DatumStreamBlockRead_Finish(
	DatumStreamBlockRead 	*dsr);

extern void DatumStreamBlockWrite_Init(
	DatumStreamBlockWrite 	*dsw,
	DatumStreamTypeInfo 	*typeInfo,
	DatumStreamVersion		datumStreamVersion,
	bool 					rle_want_compression,
	int32					initialMaxDatumPerBlock,
	int32					maxDatumPerBlock,
	int32					maxDataBlockSize,
	int						(*errdetailCallback) (void *errdetailArg),
	void	   				*errdetailArg,
	int						(*errcontextCallback) (void *errcontextArg),
	void	   				*errcontextArg);
extern void DatumStreamBlockWrite_Finish(
	DatumStreamBlockWrite 	*dsw);

extern int DatumStreamBlockWrite_Put(
	DatumStreamBlockWrite 	*dsw, 
	Datum 					d, 
	bool 					null,
	void					**toFree);
extern int DatumStreamBlockWrite_Nth(DatumStreamBlockWrite *dsw);
extern void DatumStreamBlockWrite_GetReady(
	DatumStreamBlockWrite *dsw);
extern int64 DatumStreamBlockWrite_Block(
	DatumStreamBlockWrite	*dsw,
	uint8 					*buffer);

#endif /* DATUMSTREAMBLOCK_H */

