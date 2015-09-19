/*
 * cdbparquetcolum.h
 *
 *  Created on: Sep 29, 2013
 *      Author: malili
 */

#ifndef CDBPARQUETCOLUM_H_
#define CDBPARQUETCOLUM_H_

#include "cdb/cdbparquetstoragewrite.h"

#define DAFAULT_DATAPAGE_NUM_PER_COLUMNCHUNK 10

/*
 * we call it CurrentDefinitionLevel because definition level is used to
 * determine whether the current value is null, the MACRO is used before
 * we consume() the current r/d/value triplet.
 *
 * we call it NextRepetitionLevel because we use next triplet's repetition
 * level to decide next value is repeated on which level, the MACRO is used
 * after the current triplet has been consume().
 *
 * `repetitionLevel` is guaranteed to be 0 after we consumed the last value.
 */
#define CurrentDefinitionLevel(columnReader) (columnReader)->definitionLevel
#define NextRepetitionLevel(columnReader) (columnReader)->repetitionLevel


typedef struct ParquetColumnReader
{
	MemoryContext					memoryContext;
	struct ColumnChunkMetadata_4C	*columnMetadata;
	ParquetDataPage					dataPages;
	ParquetDataPage 				currentPage;
	int 							currentPageValueRemained;/*the number of values remained for read*/
	int 							dataPageNum;
	int 							dataPageCapacity;
	int 							dataPageProcessed;/*the number of data pages have already been processed*/

    int                             repetitionLevel;
    int                             definitionLevel;

    /*
     * dataBuffer stores column chunk's raw data read from file.
     * This buffer is reused accross multiple row group.
     */
	char 							*dataBuffer;
	int32							dataLen;

    /*
     * For compressed non-repeatable (r == 0) column, we reuse a shared buffer
     * `pageBuffer` to store decompressed content for each page to save memory.
     *
     * For compressed repeatable (r > 0) column, we must keep each page's
     * decompressed content until FinishScanColumn since column values of
     * one record may span multiple pages.
     *
     * For uncompressed column, all data can be read from page->data, which
     * just points to some location at `dataBuffer` above.
     */
    char                            *pageBuffer;
    int32                           pageBufferLen;

	/*buffer reused for embedded type, avoid palloc each time for each tuple*/
    void                            *geoval;
} ParquetColumnReader;


extern void ParquetExecutorReadColumn(
		ParquetColumnReader *columnReaders,
		File file);

extern void ParquetColumnReader_readValue(ParquetColumnReader *columnReader,
		Datum *value, bool *null, int hawqTypeID);

extern void ParquetColumnReader_readPoint(ParquetColumnReader readers[], Datum *value, bool *null);
extern void ParquetColumnReader_readLSEG(ParquetColumnReader readers[], Datum *value, bool *null);
extern void ParquetColumnReader_readPATH(ParquetColumnReader readers[], Datum *value, bool *null);
extern void ParquetColumnReader_readBOX(ParquetColumnReader readers[], Datum *value, bool *null);
extern void ParquetColumnReader_readPOLYGON(ParquetColumnReader readers[], Datum *value, bool *null);
extern void ParquetColumnReader_readCIRCLE(ParquetColumnReader readers[], Datum *value, bool *null);

extern void ParquetColumnReader_FinishedScanColumn(ParquetColumnReader *columnReader);

#endif /* CDBPARQUETCOLUM_H_ */
