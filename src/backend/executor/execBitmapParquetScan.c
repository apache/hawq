/*
 * execBitmapParquetScan.c
 *   Support routines for scanning parquet tables using bitmaps.
 *
 * Copyright (c) 2014 Pivotal, Inc.
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

