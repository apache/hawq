/*
 * cdbparquetstoragelayer.h
 *
 *  Created on: Jul 4, 2013
 *      Author: malili
 */

#ifndef CDBPARQUETSTORAGELAYER_H_
#define CDBPARQUETSTORAGELAYER_H_

/*
 * This structure contains attribute values about the
 * Parquet relation that are used by the storage
 * layer.
 */
typedef struct ParquetStorageAttributes {
	bool compress;
	/* When true, content compression used. */

	char *compressType;
	/*
	 * The bulk-compression used on the contents
	 * of the Append-Only Storage Blocks.
	 */
	int compressLevel;
	/*
	 * A parameter directs the bulk-compression
	 * library on how to compress/decompress.
	 *
	 * The level can be equally important on
	 * decompression since it may specify which
	 * sub-library to use for decompress
	 * (e.g. QuickLZ).
	 */
	int overflowSize;
	/*
	 * The additional size (in bytes) required by the bulk-compression
	 * algorithm.
	 */

	bool checksum;
	/*
	 * When true, checksums protect the header
	 * and content.  Otherwise, no checksums.
	 */

	int safeFSWriteSize;
	/*
	 * The page round out with zero padding byte length.
	 * When 0, do no zero pad.
	 */

	int version;
	/*
	 * Version of the MemTuple and block layout for this AO table.
	 */

} ParquetStorageAttributes;

#endif /* CDBPARQUETSTORAGELAYER_H_ */
