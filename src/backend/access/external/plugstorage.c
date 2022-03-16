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
 * plugstorage.c
 *
 *    Pluggable storage implementation. Support external table for now.
 *
 *-------------------------------------------------------------------------
 */
#include "access/filesplit.h"
#include "access/plugstorage.h"
#include "catalog/pg_type.h"
#include "catalog/pg_proc.h"
#include "catalog/pg_exttable.h"
#include "cdb/cdbdatalocality.h"
#include "nodes/value.h"
#include "parser/parse_func.h"

char *database = NULL; /* for magma format DML database */

/*
 * getExternalTableTypeList
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
                              char **formatterName)
{
	if (formatType == 't')
	{
		*formatterType = ExternalTableType_TEXT;
		*formatterName = NULL;
	}
	else if (formatType == 'c')
	{
		*formatterType = ExternalTableType_CSV;
		*formatterName = NULL;
	}
	else if (formatType == 'b')
	{
		Assert(formatOptions);

		*formatterName = getExtTblFormatterTypeInFmtOptsList(formatOptions);

		if (pg_strncasecmp(*formatterName, "text", strlen("text")) == 0)
		{
			*formatterType = ExternalTableType_TEXT_CUSTOM;
		}
		else if (pg_strncasecmp(*formatterName, "csv", strlen("csv")) == 0)
		{
			*formatterType = ExternalTableType_CSV_CUSTOM;
		}
		else
		{
			*formatterType = ExternalTableType_PLUG;
		}
	}
	else
	{
		*formatterType = ExternalTableType_Invalid;
		*formatterName = NULL;
		elog(ERROR, "undefined external table format type: %c", formatType);
	}
}

/*
 * getExternalTableTypeStr
 *
 */
void getExternalTableTypeStr(const char formatType,
                             char *formatOptions,
							 int *formatterType,
                             char **formatterName)
{
	if (formatType == 't')
	{
		*formatterType = ExternalTableType_TEXT;
		*formatterName = NULL;
	}
	else if (formatType == 'c')
	{
		*formatterType = ExternalTableType_CSV;
		*formatterName = NULL;
	}
	else if (formatType == 'b')
	{
		Assert(formatOptions);

		*formatterName = getExtTblFormatterTypeInFmtOptsStr(formatOptions);
		Assert(*formatterName);

		if (pg_strncasecmp(*formatterName, "text", strlen("text")) == 0)
		{
			*formatterType = ExternalTableType_TEXT_CUSTOM;
		}
		else if (pg_strncasecmp(*formatterName, "csv", strlen("csv")) == 0)
		{
			*formatterType = ExternalTableType_CSV_CUSTOM;
		}
		else
		{
			*formatterType = ExternalTableType_PLUG;
		}
	}
	else
	{
		*formatterType = ExternalTableType_Invalid;
		*formatterName = NULL;
		elog(ERROR, "undefined external table format type: %c", formatType);
	}
}

/*
 * Check if values for options of custom external table are valid
 */
void checkPlugStorageFormatOption(char **opt,
                                  const char *key,
                                  const char *val,
                                  const bool needopt,
                                  const int nvalidvals,
                                  const char **validvals)
{
	Assert(opt);

	// check if need to check option
	if (!needopt)
	{
		ereport(ERROR,
		        (errcode(ERRCODE_SYNTAX_ERROR),
		        errmsg("redundant option %s", key),
		        errOmitLocation(true)));
	}

	// check if option is redundant
	if (*opt)
	{
		ereport(ERROR,
		        (errcode(ERRCODE_SYNTAX_ERROR),
		        errmsg("conflicting or redundant options \"%s\"", key),
		        errOmitLocation(true)));
	}

	*opt = val;

	// check if value for option is valid
	bool valid = false;
	for (int i = 0; i < nvalidvals; i++)
	{
		if (strcasecmp(*opt, validvals[i]) == 0)
		{
			valid = true;
		}
	}

	if (!valid && nvalidvals > 0)
	{
		ereport(ERROR,
		        (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
		        errmsg("invalid value for option %s: \"%s\"", key, val),
		        errOmitLocation(true)));
	}
}

Oid LookupPlugStorageValidatorFunc(char *formatter,
                                   char *validator)
{
	List*	funcname	= NIL;
	Oid		procOid		= InvalidOid;
	Oid		argList[1];
	Oid		returnOid;

	elog(DEBUG3, "find validator function for %s_%s", formatter, validator);

	char *new_func_name = (char *)palloc0(strlen(formatter)+strlen(validator) + 2);
	sprintf(new_func_name, "%s_%s", formatter, validator);
	funcname = lappend(funcname, makeString(new_func_name));
	returnOid = VOIDOID;
	procOid = LookupFuncName(funcname, 0, argList, true);

	pfree(new_func_name);

	return procOid;
}

void InvokePlugStorageValidationFormatInterfaces(Oid procOid,
                                                 char *formatName)
{

	PlugStorageValidatorData psvdata;
	FmgrInfo psvfunc;
	FunctionCallInfoData fcinfo;

	fmgr_info(procOid, &psvfunc);

	psvdata.type        = T_PlugStorageValidatorData;
	psvdata.format_name = formatName;

	InitFunctionCallInfoData(fcinfo,   // FunctionCallInfoData
	                         &psvfunc, // FmgrInfo
	                         0,        // nArgs
	                         (Node *)(&psvdata), // Call Context
	                         NULL);              // ResultSetInfo

	// Invoke validator. if this function returns - validation passed
	FunctionCallInvoke(&fcinfo);

	// We do not expect a null result
	if (fcinfo.isnull)
	{
		elog(ERROR, "validator function %u returned NULL",
		            fcinfo.flinfo->fn_oid);
	}
}

void InvokePlugStorageValidationFormatOptions(Oid procOid,
                                              List *formatOptions,
                                              char *formatStr,
                                              TupleDesc tupDesc,
                                              bool isWritable)
{

	PlugStorageValidatorData psvdata;
	FmgrInfo psvfunc;
	FunctionCallInfoData fcinfo;

	fmgr_info(procOid, &psvfunc);

	psvdata.type        = T_PlugStorageValidatorData;
	psvdata.format_opts = formatOptions;
	psvdata.format_str  = formatStr;
	psvdata.is_writable = isWritable;
	psvdata.tuple_desc = tupDesc;

	InitFunctionCallInfoData(fcinfo,   // FunctionCallInfoData
	                         &psvfunc, // FmgrInfo
	                         0,        // nArgs
	                         (Node *)(&psvdata), // Call Context
	                         NULL);              // ResultSetInfo

	// Invoke validator. if this function returns - validation passed
	FunctionCallInvoke(&fcinfo);

	// We do not expect a null result
	if (fcinfo.isnull)
	{
		elog(ERROR, "validator function %u returned NULL",
		            fcinfo.flinfo->fn_oid);
	}
}

void InvokePlugStorageValidationFormatEncodings(Oid procOid,
                                                char *encodingName)
{

	PlugStorageValidatorData psvdata;
	FmgrInfo psvfunc;
	FunctionCallInfoData fcinfo;

	fmgr_info(procOid, &psvfunc);

	psvdata.type			= T_PlugStorageValidatorData;
	psvdata.encoding_name	= encodingName;

	InitFunctionCallInfoData(fcinfo,   // FunctionCallInfoData
	                         &psvfunc, // FmgrInfo
	                         0,        // nArgs
	                         (Node *)(&psvdata), // Call Context
	                         NULL);              // ResultSetInfo

	// Invoke validator. if this function returns - validation passed
	FunctionCallInvoke(&fcinfo);

	// We do not expect a null result
	if (fcinfo.isnull)
	{
		elog(ERROR, "validator function %u returned NULL",
		            fcinfo.flinfo->fn_oid);
	}
}

void InvokePlugStorageValidationFormatDataTypes(Oid procOid,
                                                TupleDesc tupDesc)
{
	PlugStorageValidatorData psvdata;
	FmgrInfo psvfunc;
	FunctionCallInfoData fcinfo;

	fmgr_info(procOid, &psvfunc);

	psvdata.type       = T_PlugStorageValidatorData;
	psvdata.tuple_desc = tupDesc;

	InitFunctionCallInfoData(fcinfo,   // FunctionCallInfoData
	                         &psvfunc, // FmgrInfo
	                         0,        // nArgs
	                         (Node *)(&psvdata), // Call Context
	                         NULL);              // ResultSetInfo

	// Invoke validator. if this function returns - validation passed
	FunctionCallInvoke(&fcinfo);

	// We do not expect a null result
	if (fcinfo.isnull)
	{
		elog(ERROR, "validator function %u returned NULL",
		            fcinfo.flinfo->fn_oid);
	}
}

FileScanDesc InvokePlugStorageFormatBeginScan(FmgrInfo *func,
                                              PlannedStmt* plannedstmt,
                                              ExternalScan *extScan,
                                              ScanState *scanState,
                                              char *serializeSchema,
                                              int serializeSchemaLen,
                                              Relation relation,
                                              int formatterType,
                                              char *formatterName,
                                              MagmaSnapshot *snapshot)
{
	PlugStorageData psdata;
	FunctionCallInfoData fcinfo;

	psdata.type = T_PlugStorageData;
	psdata.ps_ext_scan = extScan;
	psdata.ps_scan_state = scanState;
	psdata.ps_relation = relation;
	psdata.ps_formatter_type = formatterType;
	psdata.ps_formatter_name = formatterName;
	psdata.ps_hive_url = extScan->hiveUrl ? pstrdup(extScan->hiveUrl) : NULL;

	if (strncmp(formatterName, "magma", strlen("magma")) == 0)
	{
		Insist(snapshot != NULL);

		// save range schema
		psdata.ps_magma_splits =
		    GetFileSplitsOfSegmentMagma(plannedstmt->scantable_splits, relation->rd_id);
		psdata.ps_magma_serializeSchema = serializeSchema;
		psdata.ps_magma_serializeSchemaLen = serializeSchemaLen;

		// save current transaction in snapshot
		psdata.ps_snapshot.currentTransaction.txnId =
		    snapshot->currentTransaction.txnId;
		psdata.ps_snapshot.currentTransaction.txnStatus =
		    snapshot->currentTransaction.txnStatus;;

		psdata.ps_snapshot.cmdIdInTransaction = snapshot->cmdIdInTransaction;

		// allocate txnActions
		psdata.ps_snapshot.txnActions.txnActionStartOffset =
		    snapshot->txnActions.txnActionStartOffset;
		psdata.ps_snapshot.txnActions.txnActions =
		    (MagmaTxnAction *)palloc0(sizeof(MagmaTxnAction) * snapshot->txnActions
		                              .txnActionSize);

		// save txnActionsp
		psdata.ps_snapshot.txnActions.txnActionSize = snapshot->txnActions
		    .txnActionSize;
		for (int i = 0; i < snapshot->txnActions.txnActionSize; ++i)
		{
		    psdata.ps_snapshot.txnActions.txnActions[i].txnId =
		        snapshot->txnActions.txnActions[i].txnId;
		    psdata.ps_snapshot.txnActions.txnActions[i].txnStatus =
		        snapshot->txnActions.txnActions[i].txnStatus;
		}

    if (plannedstmt->commandType == CMD_SELECT ||
        plannedstmt->commandType == CMD_INSERT)
      psdata.ps_magma_skip_tid = true;
    else
      psdata.ps_magma_skip_tid = false;
    }

	InitFunctionCallInfoData(fcinfo,  // FunctionCallInfoData
	                         func,    // FmgrInfo
	                         0,       // nArgs
	                         (Node *)(&psdata), // Call Context
	                         NULL);             // ResultSetInfo

	// Invoke function
	FunctionCallInvoke(&fcinfo);

	// free memory for magma snapshot
	if (strncmp(formatterName, "magma", strlen("magma")) == 0)
	{
	    pfree(psdata.ps_snapshot.txnActions.txnActions);
	}

	// We do not expect a null result
	if (fcinfo.isnull)
	{
		elog(ERROR, "function %u returned NULL",
		            fcinfo.flinfo->fn_oid);
	}

	FileScanDesc fileScanDesc = psdata.ps_file_scan_desc;

	return fileScanDesc;
}

ExternalSelectDesc InvokePlugStorageFormatGetNextInit(FmgrInfo *func,
                                                      PlanState *planState,
                                                      ExternalScanState *extScanState)
{
	PlugStorageData psdata;
	FunctionCallInfoData fcinfo;

	psdata.type              = T_PlugStorageData;

	InitFunctionCallInfoData(fcinfo,  // FunctionCallInfoData
	                         func,    // FmgrInfo
	                         0,       // nArgs
	                         (Node *)(&psdata), // Call Context
	                         NULL);             // ResultSetInfo

	// Invoke function
	FunctionCallInvoke(&fcinfo);

	// We do not expect a null result
	if (fcinfo.isnull)
	{
		elog(ERROR, "function %u returned NULL",
		            fcinfo.flinfo->fn_oid);
	}

	ExternalSelectDesc extSelectDesc = psdata.ps_ext_select_desc;

	return extSelectDesc;
}

bool InvokePlugStorageFormatGetNext(FmgrInfo *func,
                                    FileScanDesc fileScanDesc,
                                    ScanDirection scanDirection,
									ExternalSelectDesc extSelectDesc,
                                    ScanState *scanState,
                                    TupleTableSlot *tupTableSlot)
{
	PlugStorageData psdata;
	FunctionCallInfoData fcinfo;

	psdata.type                = T_PlugStorageData;
	psdata.ps_file_scan_desc   = fileScanDesc;
	psdata.ps_scan_direction   = scanDirection;
	psdata.ps_ext_select_desc  = extSelectDesc;
	psdata.ps_scan_state       = scanState;
	psdata.ps_tuple_table_slot = tupTableSlot;

	InitFunctionCallInfoData(fcinfo,  // FunctionCallInfoData
	                         func,    // FmgrInfo
	                         0,       // nArgs
	                         (Node *)(&psdata), // Call Context
	                         NULL);             // ResultSetInfo

	// Invoke function
	FunctionCallInvoke(&fcinfo);

	// We do not expect a null result
	if (fcinfo.isnull)
	{
		elog(ERROR, "function %u returned NULL",
		            fcinfo.flinfo->fn_oid);
	}

	bool has_tuple = psdata.ps_has_tuple;

	return has_tuple;
}

void InvokePlugStorageFormatReScan(FmgrInfo *func,
                                   FileScanDesc fileScanDesc,
                                   ScanState* scanState,
                                   MagmaSnapshot* snapshot,
                                   IndexRuntimeKeyInfo* runtimeKeyInfo,
                                   int numRuntimeKeys,
                                   TupleTableSlot *tupTableSlot)
{
	PlugStorageData psdata;
	FunctionCallInfoData fcinfo;

	psdata.type                = T_PlugStorageData;
	psdata.ps_scan_state       = scanState;
	psdata.ps_file_scan_desc   = fileScanDesc;
	psdata.runtime_key_info    = runtimeKeyInfo;
	psdata.num_run_time_keys   = numRuntimeKeys;
	psdata.ps_tuple_table_slot = tupTableSlot;

        if (strncmp(fileScanDesc->fs_formatter_name, "magma", strlen("magma")) == 0)
        {
                Insist(snapshot != NULL);

                // save current transaction in snapshot
                psdata.ps_snapshot.currentTransaction.txnId =
                    snapshot->currentTransaction.txnId;
                psdata.ps_snapshot.currentTransaction.txnStatus =
                    snapshot->currentTransaction.txnStatus;;

                psdata.ps_snapshot.cmdIdInTransaction = snapshot->cmdIdInTransaction;

                // allocate txnActions
                psdata.ps_snapshot.txnActions.txnActionStartOffset =
                    snapshot->txnActions.txnActionStartOffset;
                psdata.ps_snapshot.txnActions.txnActions =
                    (MagmaTxnAction *)palloc0(sizeof(MagmaTxnAction) * snapshot->txnActions
                        .txnActionSize);

                // save txnActionsp
                psdata.ps_snapshot.txnActions.txnActionSize = snapshot->txnActions
                    .txnActionSize;
                for (int i = 0; i < snapshot->txnActions.txnActionSize; ++i)
                {
                        psdata.ps_snapshot.txnActions.txnActions[i].txnId =
                              snapshot->txnActions.txnActions[i].txnId;
                        psdata.ps_snapshot.txnActions.txnActions[i].txnStatus =
                              snapshot->txnActions.txnActions[i].txnStatus;
                }
        }

	InitFunctionCallInfoData(fcinfo,  // FunctionCallInfoData
	                         func,    // FmgrInfo
	                         0,       // nArgs
	                         (Node *)(&psdata), // Call Context
	                         NULL);             // ResultSetInfo

	// Invoke function
	FunctionCallInvoke(&fcinfo);

	// free memory for magma snapshot
	if (strncmp(fileScanDesc->fs_formatter_name, "magma", strlen("magma")) == 0)
	{
	        pfree(psdata.ps_snapshot.txnActions.txnActions);
	}

	// We do not expect a null result
	if (fcinfo.isnull)
	{
		elog(ERROR, "function %u returned NULL",
		            fcinfo.flinfo->fn_oid);
	}
}

void InvokePlugStorageFormatEndScan(FmgrInfo *func,
                                    FileScanDesc fileScanDesc)
{
	PlugStorageData psdata;
	FunctionCallInfoData fcinfo;

	psdata.type              = T_PlugStorageData;
	psdata.ps_file_scan_desc = fileScanDesc;

	InitFunctionCallInfoData(fcinfo,  // FunctionCallInfoData
	                         func,    // FmgrInfo
	                         0,       // nArgs
	                         (Node *)(&psdata), // Call Context
	                         NULL);             // ResultSetInfo

	// Invoke function
	FunctionCallInvoke(&fcinfo);

	// We do not expect a null result
	if (fcinfo.isnull)
	{
		elog(ERROR, "function %u returned NULL",
		            fcinfo.flinfo->fn_oid);
	}
}

void InvokePlugStorageFormatStopScan(FmgrInfo *func,
                                     FileScanDesc fileScanDesc,
                                     TupleTableSlot *tupTableSlot)
{
	PlugStorageData psdata;
	FunctionCallInfoData fcinfo;

	psdata.type              = T_PlugStorageData;
	psdata.ps_file_scan_desc = fileScanDesc;
	psdata.ps_tuple_table_slot = tupTableSlot;

	InitFunctionCallInfoData(fcinfo,  // FunctionCallInfoData
	                         func,    // FmgrInfo
	                         0,       // nArgs
	                         (Node *)(&psdata), // Call Context
	                         NULL);             // ResultSetInfo

	// Invoke function
	FunctionCallInvoke(&fcinfo);

	// We do not expect a null result
	if (fcinfo.isnull)
	{
		elog(ERROR, "function %u returned NULL",
		            fcinfo.flinfo->fn_oid);
	}
}

ExternalInsertDesc InvokePlugStorageFormatInsertInit(FmgrInfo *func,
                                                     Relation relation,
                                                     int formatterType,
                                                     char *formatterName,
                                                     PlannedStmt *plannedstmt,
                                                     int segno,
                                                     MagmaSnapshot *snapshot)
{
	PlugStorageData psdata;
	FunctionCallInfoData fcinfo;

	psdata.type = T_PlugStorageData;
	psdata.ps_relation = relation;
	psdata.ps_formatter_type = formatterType;
	psdata.ps_formatter_name = formatterName;
	psdata.ps_segno = segno;

	if (strncmp(formatterName, "magma", strlen("magma")) == 0)
	{
		Insist(snapshot != NULL);

		// save range schema
		GetMagmaSchemaByRelid(plannedstmt->scantable_splits, relation->rd_id,
		                      &(psdata.ps_magma_serializeSchema),
		                      &(psdata.ps_magma_serializeSchemaLen));

		// save file split
		psdata.ps_magma_splits =
		    GetFileSplitsOfSegmentMagma(plannedstmt->scantable_splits, relation->rd_id);
		/*
		psdata.ps_magma_splits =
		    GetFileSplitsOfSegment(plannedstmt->scantable_splits, relation->rd_id, GetQEIndex());
		*/

		// save current transaction in snapshot
		psdata.ps_snapshot.currentTransaction.txnId =
		    snapshot->currentTransaction.txnId;
		psdata.ps_snapshot.currentTransaction.txnStatus =
		    snapshot->currentTransaction.txnStatus;
		psdata.ps_snapshot.cmdIdInTransaction = snapshot->cmdIdInTransaction;

		// allocate txnActions
		psdata.ps_snapshot.txnActions.txnActionStartOffset =
		    snapshot->txnActions.txnActionStartOffset;
		psdata.ps_snapshot.txnActions.txnActions =
		    (MagmaTxnAction *)palloc0(sizeof(MagmaTxnAction) * snapshot->txnActions
		                              .txnActionSize);

		// save txnActions
		psdata.ps_snapshot.txnActions.txnActionSize = snapshot->txnActions
		    .txnActionSize;
		for (int i = 0; i < snapshot->txnActions.txnActionSize; ++i)
		{
		    psdata.ps_snapshot.txnActions.txnActions[i].txnId =
		        snapshot->txnActions.txnActions[i].txnId;
		    psdata.ps_snapshot.txnActions.txnActions[i].txnStatus =
		        snapshot->txnActions.txnActions[i].txnStatus;
		}
	}

	if (dataStoredInHive(relation)) {
		psdata.ps_hive_url = pstrdup(plannedstmt->hiveUrl);
	}

	InitFunctionCallInfoData(fcinfo,  // FunctionCallInfoData
	                         func,    // FmgrInfo
	                         0,       // nArgs
	                         (Node *)(&psdata),  // Call Context
	                         NULL);              // ResultSetInfo

	// Invoke function
	FunctionCallInvoke(&fcinfo);

	// free memory for magma snapshot
	if (strncmp(formatterName, "magma", strlen("magma")) == 0)
	{
	    pfree(psdata.ps_snapshot.txnActions.txnActions);
	}

	// We do not expect a null result
	if (fcinfo.isnull)
	{
		elog(ERROR, "function %u returned NULL",
		            fcinfo.flinfo->fn_oid);
	}
	ExternalInsertDesc extInsertDesc = psdata.ps_ext_insert_desc;
	return extInsertDesc;
}

Oid InvokePlugStorageFormatInsert(FmgrInfo *func,
                                  ExternalInsertDesc extInsertDesc,
								  TupleTableSlot *tupTableSlot)
{
	PlugStorageData psdata;
	FunctionCallInfoData fcinfo;

	psdata.type                = T_PlugStorageData;
	psdata.ps_ext_insert_desc  = extInsertDesc;
	psdata.ps_tuple_table_slot = tupTableSlot;

	InitFunctionCallInfoData(fcinfo,  // FunctionCallInfoData
	                         func,    // FmgrInfo
	                         0,       // nArgs
	                         (Node *)(&psdata),  // Call Context
	                         NULL);              // ResultSetInfo

	// Invoke function
	FunctionCallInvoke(&fcinfo);

	// We do not expect a null result
	if (fcinfo.isnull)
	{
		elog(ERROR, "function %u returned NULL",
		            fcinfo.flinfo->fn_oid);
	}

	Oid tuple_oid = psdata.ps_tuple_oid;

	return tuple_oid;
}

void InvokePlugStorageFormatInsertFinish(FmgrInfo *func,
                                         ExternalInsertDesc extInsertDesc)
{
	PlugStorageData psdata;
	FunctionCallInfoData fcinfo;

	psdata.type                = T_PlugStorageData;
	psdata.ps_ext_insert_desc  = extInsertDesc;

	InitFunctionCallInfoData(fcinfo,  // FunctionCallInfoData
	                         func,    // FmgrInfo
	                         0,       // nArgs
	                         (Node *)(&psdata), // Call Context
	                         NULL);             // ResultSetInfo

	// Invoke function
	FunctionCallInvoke(&fcinfo);

	// We do not expect a null result
	if (fcinfo.isnull)
	{
		elog(ERROR, "function %u returned NULL",
		            fcinfo.flinfo->fn_oid);
	}
}

void InvokePlugStorageFormatTransaction(PlugStorageTransaction txn, List* magmaTableFullNames)
{
	PlugStorageData psdata;
	FunctionCallInfoData fcinfo;
	FmgrInfo *func;

	psdata.type                = T_PlugStorageData;
	psdata.ps_transaction      = txn;
	psdata.magma_talbe_full_names = magmaTableFullNames;
	func                       = &(txn->pst_transaction_fmgr_info);

	InitFunctionCallInfoData(fcinfo,  // FunctionCallInfoData
	                         func,    // FmgrInfo
	                         0,       // nArgs
	                         (Node *)(&psdata), // Call Context
	                         NULL);             // ResultSetInfo

	// Invoke function
	FunctionCallInvoke(&fcinfo);

	// We do not expect a null result
	if (fcinfo.isnull)
	{
		elog(ERROR, "function %u returned NULL",
		            fcinfo.flinfo->fn_oid);
	}
}
