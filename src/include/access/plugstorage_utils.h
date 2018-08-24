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

/*-------------------------------------------------------------------------
 *
 * plugstorage_utils.h
 *
 * Pluggable storage utility definitions. Support external table for now.
 *
 *-------------------------------------------------------------------------
 */

#ifndef PLUGSTORAGE_UTILS_H
#define PLUGSTORAGE_UTILS_H

#include "fmgr.h"

/*
 * Scan functions for pluggable format
 */
typedef struct PlugStorageScanFuncs
{
	FmgrInfo *beginscan;
	FmgrInfo *getnext_init;
	FmgrInfo *getnext;
	FmgrInfo *rescan;
	FmgrInfo *endscan;
	FmgrInfo *stopscan;

} PlugStorageScanFuncs;

/*
 * Insert functions for pluggable format
 */
typedef struct PlugStorageInsertFuncs
{
	FmgrInfo *insert_init;
	FmgrInfo *insert;
	FmgrInfo *insert_finish;

} PlugStorageInsertFuncs;

/*
 * ExternalTableType
 *
 *    enum for different types of external tables.
 *
 *    The different types of external tables can be combinations of different
 *    protocols and formats. To be specific:
 *    {GPFDIST, GPFDISTS, HTTP, COMMAND, HDFS} X {TEXT, CSV, ORC}
 *
 *    NOTE:
 *
 *    The GENERIC external table type is used to simplify the call back
 *    implementation for different combination of external table formats
 *    and protocols. It need to be improved so that each external table
 *    type should get its access method with minimum cost during runtime.
 *
 *    The fact is that for custom protocol (HDFS), the format types for
 *    TEXT, CSV, and ORC external tables are all 'b' in pg_exttable catalog
 *    table. The formatter information is stored in format options which is
 *    a list strings. Thus, during read and write access of these tables,
 *    there will be performance issue if we get the format type and access
 *    method for them for each tuple.
 *
 */
typedef enum {
  ExternalTableType_GENERIC, /* GENERIC external table format and protocol */
  ExternalTableType_TEXT, /* TEXT format with gpfdist(s), http, command protocol
                           */
  ExternalTableType_CSV,  /* CSV format with gpfdist(s), http, command protocol
                           */
  ExternalTableType_TEXT_CUSTOM, /* TEXT format with hdfs protocol */
  ExternalTableType_CSV_CUSTOM,  /* CSV format with hdfs protocol */
  ExternalTableType_PLUG, /* Pluggable format with hdfs protocol, i.e., ORC */
  ExternalTableType_Invalid
} ExternalTableType;

#endif	/* PLUGSTORAGE_UTILS_H */
