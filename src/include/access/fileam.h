/*-------------------------------------------------------------------------
*
* fileam.h
*	  external file access method definitions.
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
#ifndef FILEAM_H
#define FILEAM_H

#include "c.h"
#include "access/formatter.h"
#include "access/htup.h"
#include "access/relscan.h"
#include "access/sdir.h"
#include "access/tupmacs.h"
#include "access/xlogutils.h"
#include "nodes/primnodes.h"
#include "storage/lmgr.h"
#include "utils/rel.h"
#include "utils/tqual.h"
#include "access/url.h"



/*
 * ExternalInsertDescData is used for storing state related
 * to inserting data into a writable external table.
 */
typedef struct ExternalInsertDescData
{
	Relation		ext_rel;
	FILE*			ext_file;
	char*			ext_uri;		/* "command:<cmd>" or "tablespace:<path>" */
	bool			ext_noop;		/* no op. this segdb needs to do nothing (e.g. mirror seg) */
	
	TupleDesc		ext_tupDesc;
	Datum*			ext_values;
	bool*			ext_nulls;
	
	FormatterData*  ext_formatter_data;

	struct CopyStateData *ext_pstate; 	/* data parser control chars and state */

} ExternalInsertDescData;

typedef ExternalInsertDescData *ExternalInsertDesc;

/*
 * ExternalSelectDescData is used for storing state related
 * to selecting data from an external table.
 */
typedef struct ExternalSelectDescData
{
	ProjectionInfo *projInfo;
	// Information needed for aggregate pushdown
	int  agg_type;
} ExternalSelectDescData;

typedef enum DataLineStatus
{
	LINE_OK,
	LINE_ERROR,
	NEED_MORE_DATA,
	END_MARKER
} DataLineStatus;

extern FileScanDesc external_beginscan(Relation relation, Index scanrelid,
								   uint32 scancounter, List *uriList,
								   List *fmtOpts, char fmtType, bool isMasterOnly,
								   int rejLimit, bool rejLimitInRows,
								   Oid fmterrtbl, ResultRelSegFileInfo *segfileinfo, int encoding,
								   List *scanquals);
extern void external_rescan(FileScanDesc scan);
extern void external_endscan(FileScanDesc scan);
extern void external_stopscan(FileScanDesc scan);
extern ExternalSelectDesc external_getnext_init(PlanState *state, ExternalScanState *es_state);
extern HeapTuple external_getnext(FileScanDesc scan, ScanDirection direction, ExternalSelectDesc desc);
extern ExternalInsertDesc external_insert_init(Relation rel, int errAosegno);
extern Oid external_insert(ExternalInsertDesc extInsertDesc, HeapTuple instup);
extern void external_insert_finish(ExternalInsertDesc extInsertDesc);
extern void external_set_env_vars(extvar_t *extvar, char* uri, bool csv, char* escape, char* quote, bool header, uint32 scancounter);
extern void AtAbort_ExtTables(void);
char*	linenumber_atoi(char buffer[20],int64 linenumber);


#endif   /* FILEAM_H */
