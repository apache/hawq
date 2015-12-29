/*-------------------------------------------------------------------------
 *
 * analyzeutils.h
 *
 *	  Header file for utils functions in analyzeutils.c
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
