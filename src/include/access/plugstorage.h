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
 * plugstorage.h
 *
 *    Pluggable storage definitions. Support external table for now.
 *
 *-------------------------------------------------------------------------
 */

#ifndef PLUGSTORAGE_H
#define PLUGSTORAGE_H

#include "postgres.h"
#include "postgres_ext.h"
#include "fmgr.h"
#include "nodes/pg_list.h"
#include "nodes/plannodes.h"
#include "nodes/execnodes.h"
#include "access/sdir.h"
#include "access/relscan.h"
#include "access/extprotocol.h"
#include "access/tupdesc.h"
#include "access/fileam.h"
#include "access/plugstorage_utils.h"
#include "utils/relcache.h"
#include "executor/tuptable.h"

/* From src/include/access/fileam.h */
extern char *getExtTblFormatterTypeInFmtOptsStr(char *fmtStr);
extern char *getExtTblFormatterTypeInFmtOptsList(List *fmtOpts);



/*
 * PlugStorageValidatorData is the node type that is passed as fmgr "context"
 * info when a function is called by the Pluggable Storage Framework.
 */
typedef struct PlugStorageValidatorData
{
	/* see T_PlugStorageValidatorData */
	NodeTag		type;

	/* check if format interfaces are implemented in pg_proc */
	char		*format_name;

	/* check if format options and their values are valid */
	List		*format_opts;
	char		*format_str;
	char		*encoding_name;
	bool		is_writable;

	/* check if format data types are supported in table definition */
	TupleDesc	tuple_desc;

} PlugStorageValidatorData;

typedef struct PlugStorageValidatorData *PlugStorageValidator;

/*
 * PlugStorageData is used to pass args between hawq and pluggable data formats.
 */
typedef struct PlugStorageData
{
	NodeTag                type;           /* see T_PlugStorageData */
	int                    ps_table_type;
	int                    ps_formatter_type;
	char                  *ps_formatter_name;
	int                    ps_error_ao_seg_no;
	Relation               ps_relation;
	PlanState             *ps_plan_state;
	ExternalScan          *ps_ext_scan;
	ScanState             *ps_scan_state;
	ScanDirection          ps_scan_direction;
	FileScanDesc           ps_file_scan_desc;
	ExternalScanState     *ps_ext_scan_state;
	ResultRelSegFileInfo  *ps_result_seg_file_info;
	ExternalInsertDesc     ps_ext_insert_desc;
	ExternalSelectDesc     ps_ext_select_desc;
	bool                   ps_has_tuple;
	Oid                    ps_tuple_oid;
	TupleTableSlot        *ps_tuple_table_slot;

} PlugStorageData;

typedef PlugStorageData *PlugStorage;


/*
 * getExternalTableTypeList
 * getExternalTableTypeStr
 *
 *    Return the table type for a given external table
 *
 *    Currently it is an implementation with performance cost due to the
 *    fact that the format types for TEXT, CSV, and ORC external tables
 *    with customer protocol (HDFS) are all 'b' in pg_exttable catalog
 *    table. It needs to visit the format options to get the table type.
 */
void getExternalTableTypeInList(const char formatType,
                              List *formatOptions,
                              int *formatterType,
                              char **formatterName);

void getExternalTableTypeInStr(const char formatType,
                             char *formatOptions,
                             int *formatterType,
                             char **formatterName);

void checkPlugStorageFormatOption(char **opt,
                                  const char *key,
                                  const char *val,
                                  const bool needopt,
                                  const int nvalidvals,
                                  const char **validvals);

Oid LookupPlugStorageValidatorFunc(char *formatter,
                                   char *validator);

void InvokePlugStorageValidationFormatInterfaces(Oid procOid,
                                                 char *formatName);

void InvokePlugStorageValidationFormatOptions(Oid procOid,
                                              List *formatOptions,
                                              char *formatStr,
                                              bool isWritable);

void InvokePlugStorageValidationFormatEncodings(Oid procOid,
                                                char *encodingName);

void InvokePlugStorageValidationFormatDataTypes(Oid procOid,
                                                TupleDesc tupDesc);

FileScanDesc InvokePlugStorageFormatBeginScan(FmgrInfo *func,
                                              ExternalScan *extScan,
                                              ScanState *scanState,
                                              Relation relation,
                                              int formatterType,
                                              char *formatterNam);

ExternalSelectDesc InvokePlugStorageFormatGetNextInit(FmgrInfo *func,
                                                      PlanState *planState,
                                                      ExternalScanState *extScanState);

bool InvokePlugStorageFormatGetNext(FmgrInfo *func,
                                    FileScanDesc fileScanDesc,
                                    ScanDirection scanDirection,
                                    ExternalSelectDesc extSelectDesc,
                                    ScanState *scanState,
                                    TupleTableSlot *tupTableSlot);

void InvokePlugStorageFormatReScan(FmgrInfo *func,
                                   FileScanDesc fileScanDesc);

void InvokePlugStorageFormatEndScan(FmgrInfo *func,
                                    FileScanDesc fileScanDesc);

void InvokePlugStorageFormatStopScan(FmgrInfo *func,
                                     FileScanDesc fileScanDesc);

ExternalInsertDesc InvokePlugStorageFormatInsertInit(FmgrInfo *func,
                                                     Relation relation,
                                                     int formatterType,
                                                     char *formatterName);

Oid InvokePlugStorageFormatInsert(FmgrInfo *func,
                                  ExternalInsertDesc extInsertDesc,
                                  TupleTableSlot *tupTableSlot);

void InvokePlugStorageFormatInsertFinish(FmgrInfo *func,
                                         ExternalInsertDesc extInsertDesc);

#endif	/* PLUGSTORAGE_H */
