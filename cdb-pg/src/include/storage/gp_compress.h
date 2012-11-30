/*
 * gp_compress.h
 *      gpdb compression utilities.
 *
 * Copyright (c) 2009, Greenplum Inc.
 */

#ifndef GP_COMPRESS_H
#define GP_COMPRESS_H

#include "fmgr.h"

#include "catalog/pg_compression.h"
#include "storage/quicklz1.h"
#include "storage/quicklz3.h"

#ifdef HAVE_LIBZ
#include <zlib.h>
#endif


/*
 *	Represents the compressor type used for an AO table.
 *	Note that we treat quicklz with compression levels 1 and 3 as
 *	two separate types of compressors, this is because we have to
 *	compile them separately and have a separate API for each. Zlib
 *	compression will have a compression level attached to it, checked
 *	separately.
 */

// UNDONE: Is there a QLZ DEFINE for this?
#define MAX_OVERRUN_QUICKLZ 400

extern int gp_trycompress_new(
		 uint8			*sourceData,
		 int32			 sourceLen,
		 uint8			*compressedBuffer,
		 int32			 compressedBufferWithOverrrunLen,
		 int32			 maxCompressedLen,	// The largest compressed result we would like to see.
		 int32			*compressedLen,
		 int				 compressLevel,
		 PGFunction	 compressor,
		 CompressionState *compressionState);


extern void gp_decompress_new(
				uint8			*compressed,
				int32			 compressedLen,
				uint8			*uncompressed,
				int32			 uncompressedLen,
			  PGFunction decompressor,
			  CompressionState *compressionState,
				int64			 bufferCount);

extern void gp_issuecompresserror(
	int				zlibCompressError,
	int32			sourceLen);

#endif
