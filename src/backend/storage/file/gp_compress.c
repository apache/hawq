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
 * gp_compress.c
 *      Common compression utilities
 *
 */

#include "postgres.h"

#include "catalog/pg_compression.h"
#include "cdb/cdbappendonlystoragelayer.h"
#include "storage/gp_compress.h"
#include "utils/guc.h"

static void gp_trycompress_generic(uint8 *sourceData, int32 sourceLen,
								   uint8 *compressedBuffer,
								   int32 compressedBufferLen,
								   int32 *compressedLen,
								   PGFunction compressor,
								   CompressionState *compressionState);

static void gp_decompress_generic(uint8 *compressed, int32 compressedLen,
								  uint8 *uncompressed, int32 uncompressedLen,
								  PGFunction decompressor,
								  CompressionState *compressionState,
								  int64 bufferCount);


int gp_trycompress_new(
	 uint8		*sourceData,
	 int32		sourceLen,
	 uint8		*compressedBuffer,
	 int32		compressedBufferWithOverrrunLen,
	 int32		maxCompressedLen,	// The largest compressed result we would like to see.
	 int32		*compressedLen,
	 int		compressLevel,
	 PGFunction     compressor,
	 CompressionState *compressionState)
{
	Insist(PointerIsValid(compressor));

	gp_trycompress_generic(sourceData, sourceLen, compressedBuffer,
						   compressedBufferWithOverrrunLen, compressedLen,
						   compressor, compressionState);

	/* XXX: this interface is AWFUL! */
	return Z_OK;
}

/*---------------------------------------------------------------------------*/
static void
gp_trycompress_generic( uint8			*sourceData
						  , int32			 sourceLen
						  , uint8			*compressedBuffer
						  , int32			 compressedBufferLen
						  , int32			*compressedLen
						  , PGFunction		compressor
						  , CompressionState *compressionState
						  )

{
	callCompressionActuator( compressor
						   , (const void *)sourceData
						   , (size_t)sourceLen
						   , (char*)compressedBuffer
						   , compressedBufferLen
						   , compressedLen
						   , compressionState
						   );
}

/*---------------------------------------------------------------------------*/
static void gp_decompress_generic(
	  uint8	 *compressed,
	  int32	 compressedLen,
	  uint8	 *uncompressed,
	  int32	 uncompressedLen,
	  PGFunction	 decompressor,
	  CompressionState *compressionState,
	  int64	 bufferCount
	  )
{
	unsigned long compressedSize;
	unsigned long uncompressedSize;
	int32				 resultingUncompressedLen;

	compressedSize = (unsigned long)compressedLen;
	uncompressedSize = (unsigned long)uncompressedLen;

	callCompressionActuator(decompressor,
							(const void *)compressed,
							(size_t)compressedLen,
							(char*)uncompressed,
							uncompressedLen,
							&resultingUncompressedLen,
							compressionState);


	if (resultingUncompressedLen != uncompressedLen)
		elog(ERROR,
			 "Uncompress returned length %d which is different than the "
			 "expected length %d (block count " INT64_FORMAT ")",
			 resultingUncompressedLen,
			 uncompressedLen,
			 bufferCount);

}  /* end gp_decompress_generic */

/*---------------------------------------------------------------------------*/
void gp_decompress_new(
			uint8			*compressed,
			int32			compressedLen,
			uint8			*uncompressed,
			int32			uncompressedLen,
			PGFunction     decompressor,
			CompressionState *compressionState,
			int64			bufferCount)
{
	unsigned long compressedSize;
	unsigned long uncompressedSize;

	int32 resultingUncompressedLen;

	compressedSize = (unsigned long)compressedLen;
	uncompressedSize = (unsigned long)uncompressedLen;

	gp_decompress_generic( compressed
						, compressedLen
						, uncompressed
						, uncompressedLen
						, decompressor
						, compressionState
						, bufferCount
	);

	resultingUncompressedLen = (int32)uncompressedSize;

	if (resultingUncompressedLen != uncompressedLen)
		elog(ERROR,
			 "Uncompress returned length %d which is different than the "
			 "expected length %d (block count " INT64_FORMAT ")",
			 resultingUncompressedLen,
			 uncompressedLen,
			 bufferCount);
}

void
gp_issuecompresserror(
	int				zlibCompressError,
	int32			sourceLen)
{
	char *detail = NULL;

	switch(zlibCompressError)
	{
	case Z_MEM_ERROR:
		detail = "Insufficient memory";
		break;
	case Z_BUF_ERROR:
		detail = "The buffer was not large enough to hold the compressed data";
		break;
	case Z_STREAM_ERROR:
		detail = "The level was not Z_DEFAULT_LEVEL, or was not between 0 and 9";
		break;
	default:
		elog(ERROR,"ZLIB compress2 failed -- reason %d unknown (uncompressed length %d)",
			 zlibCompressError, sourceLen);
		break;
	}

	elog(ERROR,"ZLIB compress2 failed (detail: '%s', uncompressed length %d)",
		 detail, sourceLen);
}


