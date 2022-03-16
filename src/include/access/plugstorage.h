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
#include "catalog/pg_exttable.h"
#include "utils/relcache.h"
#include "executor/tuptable.h"
#include "magma/cwrapper/magma-client-c.h"

extern char *database;

/* From src/include/access/fileam.h */
extern char *getExtTblCategoryInFmtOptsStr(char *fmtStr);
extern char *getExtTblFormatterTypeInFmtOptsStr(char *fmtStr);
extern char *getExtTblFormatterTypeInFmtOptsList(List *fmtOpts);


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
typedef enum
{
	ExternalTableType_GENERIC,     /* GENERIC external table format and protocol */
	ExternalTableType_TEXT,        /* TEXT format with gpfdist(s), http, command protocol */
	ExternalTableType_CSV,         /* CSV format with gpfdist(s), http, command protocol */
	ExternalTableType_TEXT_CUSTOM, /* TEXT format with hdfs protocol */
	ExternalTableType_CSV_CUSTOM,  /* CSV format with hdfs protocol */
	ExternalTableType_PLUG,        /* Pluggable format with hdfs protocol, i.e., ORC */
	ExternalTableType_Invalid
} ExternalTableType;

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
	NodeTag                 type;           /* see T_PlugStorageData */
	int                     ps_formatter_type;
	char                   *ps_formatter_name;
	int                     ps_error_ao_seg_no;
	Relation                ps_relation;
	ExtTableEntry          *ps_exttable;
	PlanState              *ps_plan_state;
	ExternalScan           *ps_ext_scan;
	ScanState              *ps_scan_state;
	ScanDirection           ps_scan_direction;
	FileScanDesc            ps_file_scan_desc;
	ResultRelSegFileInfo   *ps_result_seg_file_info;
	ExternalInsertDesc      ps_ext_insert_desc;
	ExternalInsertDesc      ps_ext_delete_desc;
	ExternalInsertDesc      ps_ext_update_desc;
	ExternalSelectDesc      ps_ext_select_desc;
	bool                    ps_has_tuple;
	bool                    ps_is_external;
	Oid                     ps_tuple_oid;
	TupleTableSlot         *ps_tuple_table_slot;
	char                   *ps_db_name;
	char                   *ps_schema_name;
	char                   *ps_table_name;
	MagmaSnapshot           ps_snapshot;
	List                   *ps_table_elements;
	List                   *ps_ext_locations;
	List                   *ps_distributed_key;
	List                   *ps_magma_splits;
	char                   *ps_magma_serializeSchema;
	int                     ps_magma_serializeSchemaLen;
	int                     ps_magma_rangenum;
	IndexStmt              *ps_primary_key;
	int                     ps_num_cols;
	int                    *ps_col_indexes;
	char                  **ps_col_names;
	int                    *ps_col_datatypes;
	PlugStorageTransaction  ps_transaction;
	int                     ps_segno;
	int                     ps_update_count;  // Number of rows actually updated
	bool                    ps_magma_skip_tid;
	char                   *ps_hive_url;
	/* Add for magma index info */
	MagmaIndex              magma_idx;
	/* For beginTransaction */
	List*                   magma_talbe_full_names;
	/* The following two fields are for parameterized index scan */
	IndexRuntimeKeyInfo*    runtime_key_info;
	int                     num_run_time_keys;
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
void getExternalTableTypeList(const char formatType,
                              List *formatOptions,
                              int *formatterType,
                              char **formatterName);

void getExternalTableTypeStr(const char formatType,
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
                                              TupleDesc tupDesc,
                                              bool isWritable);

void InvokePlugStorageValidationFormatEncodings(Oid procOid,
                                                char *encodingName);

void InvokePlugStorageValidationFormatDataTypes(Oid procOid,
                                                TupleDesc tupDesc);

FileScanDesc InvokePlugStorageFormatBeginScan(FmgrInfo *func,
                                              PlannedStmt* plannedstmt,
                                              ExternalScan *extScan,
                                              ScanState* scanState,
                                              char *serializeSchema,
                                              int serializeSchemaLen,
                                              Relation relation,
                                              int formatterType,
                                              char *formatterName,
                                              MagmaSnapshot *snapshot);

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
                                   FileScanDesc fileScanDesc,
                                   ScanState* scanState,
                                   MagmaSnapshot* snapshot,
                                   IndexRuntimeKeyInfo* runtimeKeyInfo,
                                   int numRuntimeKeys,
                                   TupleTableSlot *tupTableSlot);

void InvokePlugStorageFormatEndScan(FmgrInfo *func,
                                    FileScanDesc fileScanDesc);

void InvokePlugStorageFormatStopScan(FmgrInfo *func,
                                     FileScanDesc fileScanDesc,
                                     TupleTableSlot *tupTableSlot);

ExternalInsertDesc InvokePlugStorageFormatInsertInit(FmgrInfo *func,
                                                     Relation relation,
                                                     int formatterType,
                                                     char *formatterName,
                                                     PlannedStmt* plannedstmt,
                                                     int segno,
                                                     MagmaSnapshot *snapshot);

Oid InvokePlugStorageFormatInsert(FmgrInfo *func,
                                  ExternalInsertDesc extInsertDesc,
                                  TupleTableSlot *tupTableSlot);

void InvokePlugStorageFormatInsertFinish(FmgrInfo *func,
                                         ExternalInsertDesc extInsertDesc);

void InvokePlugStorageFormatTransaction(PlugStorageTransaction txn, List* magmaTableFullNames);

#endif	/* PLUGSTORAGE_H */
