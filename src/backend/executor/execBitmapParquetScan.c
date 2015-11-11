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
 * execBitmapParquetScan.c
 *   Support routines for scanning parquet tables using bitmaps.
 *
 */

#include "postgres.h"
#include "nodes/execnodes.h"
#include "executor/tuptable.h"

void BitmapParquetScanBegin(ScanState *scanState);
void BitmapParquetScanEnd(ScanState *scanState);
TupleTableSlot* BitmapParquetScanNext(ScanState *scanState);
void BitmapParquetScanReScan(ScanState *scanState);

/*
 * Prepares for a new parquet scan.
 */
void
BitmapParquetScanBegin(ScanState *scanState)
{
	Insist(!"Bitmap index scan on parquet table is not supported");
}

/*
 * Cleans up after the scanning is done.
 */
void
BitmapParquetScanEnd(ScanState *scanState)
{
	Insist(!"Bitmap index scan on parquet table is not supported");
}

/*
 * Returns the next matching tuple.
 */
TupleTableSlot *
BitmapParquetScanNext(ScanState *scanState)
{
	Insist(!"Bitmap index scan on parquet table is not supported");
}

/*
 * Prepares for a re-scan.
 */
void
BitmapParquetScanReScan(ScanState *scanState)
{
	Insist(!"Bitmap index scan on parquet table is not supported");
}

