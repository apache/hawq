/*-------------------------------------------------------------------------
 *
 * analyzeutils.h
 *
 *	  Header file for utils functions in analyzeutils.c
 *
 * Copyright (c) 2015, Pivotal Inc.
 *
 *-------------------------------------------------------------------------
 */

#ifndef ANALYZEUTILS_H
#define ANALYZEUTILS_H

#include "utils/array.h"

/* extern functions called by commands/analyze.c */
extern void aggregate_leaf_partition_MCVs(Oid relationOid,
		AttrNumber attnum,
		unsigned int nEntries,
		ArrayType **result);
extern void aggregate_leaf_partition_histograms(Oid relationOid,
		AttrNumber attnum,
		unsigned int nEntries,
		ArrayType **result);
extern bool datumCompare(Datum d1, Datum d2, Oid opFuncOid);

/*
 * Helper functions for ANALYZE.
 */

/**
 * Drops the sample table created during ANALYZE.
 */
extern void dropSampleTable(Oid sampleTableOid, bool isExternal);

/**
 * Generates a table name for the auxiliary sample table that may be created during ANALYZE.
 */
extern char* temporarySampleTableName(Oid relationOid, char* prefix);

/* Convenience */
extern ArrayType * SPIResultToArray(int resultAttributeNumber, MemoryContext allocationContext);

/* spi execution helpers */
typedef void (*spiCallback)(void *clientDataOut);
extern void spiExecuteWithCallback(const char *src, bool read_only, long tcount,
           spiCallback callbackFn, void *clientData);

typedef struct
{
    int numColumns;
    MemoryContext memoryContext;
    ArrayType ** output;
} EachResultColumnAsArraySpec;

extern void spiCallback_getEachResultColumnAsArray(void *clientData);
extern void spiCallback_getProcessedAsFloat4(void *clientData);
extern void spiCallback_getSingleResultRowColumnAsFloat4(void *clientData);

#endif  /* ANALYZEUTILS_H */
