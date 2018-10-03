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


#include <json-c/json.h>

#include "c.h"
#include "port.h"
#include "postgres.h"
#include "fmgr.h"
#include "funcapi.h"
#include "nodes/pg_list.h"
#include "utils/memutils.h"
#include "utils/relcache.h"
#include "utils/uri.h"
#include "utils/formatting.h"
#include "utils/lsyscache.h"
#include "utils/datetime.h"
#include "mb/pg_wchar.h"
#include "commands/defrem.h"
#include "commands/copy.h"
#include "access/tupdesc.h"
#include "access/filesplit.h"
#include "access/plugstorage.h"
#include "cdb/cdbvars.h"
#include "catalog/pg_exttable.h"
#include "catalog/namespace.h"
#include "postmaster/identity.h"
#include "nodes/makefuncs.h"
#include "nodes/plannodes.h"
#include "utils/uri.h"


#define ORC_TIMESTAMP_EPOCH_JDATE	2457024 /* == date2j(2015, 1, 1) */
#define MAX_ORC_ARRAY_DIMS        10000

/* Do the module magic dance */
PG_MODULE_MAGIC;

/* Validators for pluggable storage format ORC */
PG_FUNCTION_INFO_V1(orc_validate_interfaces);
PG_FUNCTION_INFO_V1(orc_validate_options);
PG_FUNCTION_INFO_V1(orc_validate_encodings);
PG_FUNCTION_INFO_V1(orc_validate_datatypes);

/* Accessors for pluggable storage format ORC */
PG_FUNCTION_INFO_V1(orc_beginscan);
PG_FUNCTION_INFO_V1(orc_getnext_init);
PG_FUNCTION_INFO_V1(orc_getnext);
PG_FUNCTION_INFO_V1(orc_rescan);
PG_FUNCTION_INFO_V1(orc_endscan);
PG_FUNCTION_INFO_V1(orc_stopscan);
PG_FUNCTION_INFO_V1(orc_insert_init);
PG_FUNCTION_INFO_V1(orc_insert);
PG_FUNCTION_INFO_V1(orc_insert_finish);

/* Definitions of validators for pluggable storage format ORC */
Datum orc_validate_interfaces(PG_FUNCTION_ARGS);
Datum orc_validate_options(PG_FUNCTION_ARGS);
Datum orc_validate_encodings(PG_FUNCTION_ARGS);
Datum orc_validate_datatypes(PG_FUNCTION_ARGS);

/* Definitions of accessors for pluggable storage format ORC */
Datum orc_beginscan(PG_FUNCTION_ARGS);
Datum orc_getnext_init(PG_FUNCTION_ARGS);
Datum orc_getnext(PG_FUNCTION_ARGS);
Datum orc_rescan(PG_FUNCTION_ARGS);
Datum orc_endscan(PG_FUNCTION_ARGS);
Datum orc_stopscan(PG_FUNCTION_ARGS);
Datum orc_insert_init(PG_FUNCTION_ARGS);
Datum orc_insert(PG_FUNCTION_ARGS);
Datum orc_insert_finish(PG_FUNCTION_ARGS);


Datum orc_validate_interfaces(PG_FUNCTION_ARGS)
{
	elog(ERROR, "Funtion orc_validate_interfaces has not be completed, please fill it");
	PG_RETURN_VOID();
}

/*
 * void
 * orc_validate_options(List *formatOptions,
 *                      char *formatStr,
 *                      bool isWritable)
 */
Datum orc_validate_options(PG_FUNCTION_ARGS)
{

	elog(ERROR, "Funtion orc_validate_options has not be completed, please fill it");
	PG_RETURN_VOID();
}

/*
 * void
 * orc_validate_encodings(char *encodingName)
 */
Datum orc_validate_encodings(PG_FUNCTION_ARGS)
{
	elog(ERROR, "Funtion orc_validate_encodings has not be completed, please fill it");
	PG_RETURN_VOID();
}

/*
 * void
 * orc_validate_datatypes(TupleDesc tupDesc)
 */
Datum orc_validate_datatypes(PG_FUNCTION_ARGS)
{

	elog(ERROR, "Funtion orc_validate_datatypes has not be completed, please fill it");
	PG_RETURN_VOID();
}

/*
 * FileScanDesc
 * orc_beginscan(ExternalScan *extScan,
 *               ScanState *scanState,
 *               Relation relation,
 *               int formatterType,
 *               char *formatterName)
 */
Datum orc_beginscan(PG_FUNCTION_ARGS)
{

	elog(ERROR, "Funtion orc_beginscan has not be completed, please fill it");
	PG_RETURN_POINTER(NULL);
}

/*
 * ExternalSelectDesc
 * orc_getnext_init(PlanState *planState,
 *                  ExternalScanState *extScanState)
 */
Datum orc_getnext_init(PG_FUNCTION_ARGS)
{

	elog(ERROR, "Funtion orc_getnext_init has not be completed, please fill it");
	PG_RETURN_POINTER(NULL);
}

/*
 * bool
 * orc_getnext(FileScanDesc fileScanDesc,
 *             ScanDirection direction,
 *             ExternalSelectDesc extSelectDesc,
 *             ScanState *scanState,
 *             TupleTableSlot *tupTableSlot)
 */
Datum orc_getnext(PG_FUNCTION_ARGS)
{
	elog(ERROR, "Funtion orc_getnext has not be completed, please fill it");
	PG_RETURN_VOID();
}

/*
 * void
 * orc_rescan(FileScanDesc scan)
 */
Datum orc_rescan(PG_FUNCTION_ARGS)
{
	elog(ERROR, "Funtion orc_rescan has not be completed, please fill it");
	PG_RETURN_VOID();
}

/*
 * void
 * orc_endscan(FileScanDesc scan)
 */
Datum orc_endscan(PG_FUNCTION_ARGS)
{

	elog(ERROR, "Funtion orc_endscan has not be completed, please fill it");
	PG_RETURN_VOID();
}

/*
 * void
 * orc_stopscan(FileScanDesc scan)
 */
Datum orc_stopscan(PG_FUNCTION_ARGS)
{
	elog(ERROR, "Funtion orc_stopscan has not be completed, please fill it");
	PG_RETURN_VOID();
}

/*
 * ExternalInsertDesc
 * orc_insert_init(Relation relation,
 *                 int formatterType,
 *                 char *formatterName)
 */
Datum orc_insert_init(PG_FUNCTION_ARGS)
{

	elog(ERROR, "Funtion orc_insert_init has not be completed, please fill it");
	PG_RETURN_POINTER(NULL);
}

/*
 * Oid
 * orc_insert(ExternalInsertDesc extInsertDesc,
 *            TupleTableSlot *tupTableSlot)
 */
Datum orc_insert(PG_FUNCTION_ARGS)
{

	elog(ERROR, "Funtion orc_insert has not be completed, please fill it");
	PG_RETURN_OID(InvalidOid);
}

/*
 * void
 * orc_insert_finish(ExternalInsertDesc extInsertDesc)
 */
Datum orc_insert_finish(PG_FUNCTION_ARGS)
{
	elog(ERROR, "Funtion orc_insert_finish has not be completed, please fill it");
	PG_RETURN_VOID();
}

