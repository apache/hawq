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

//---------------------------------------------------------------------------
//	@filename:
//		gpdbdefs.h
//
//	@doc:
//		C Linkage for GPDB functions used by GP optimizer
//
//	@test:
//
//
//---------------------------------------------------------------------------

#ifndef GPDBDefs_H
#define GPDBDefs_H

extern "C" {

#include "postgres.h"
#include <string.h>
#include "nodes/nodes.h"
#include "nodes/plannodes.h"
#include "nodes/execnodes.h"
#include "nodes/print.h"
#include "nodes/pg_list.h"
#include "executor/execdesc.h"
#include "executor/nodeMotion.h"
#include "parser/parsetree.h"
#include "utils/lsyscache.h"
#include "utils/datum.h"
#include "utils/array.h"
#include "utils/builtins.h"
#include "optimizer/walkers.h"
#include "optimizer/planmain.h"
#include "parser/parse_expr.h"
#include "parser/parse_relation.h"
#include "parser/parse_clause.h"

#include "catalog/namespace.h"
#include "catalog/pg_exttable.h"
#include "cdb/cdbpartition.h"
#include "cdb/cdblink.h"
#include "cdb/partitionselection.h"
#include "cdb/cdbhash.h"
#include "cdb/cdbutil.h"
#include "cdb/cdbmutate.h"
#include "commands/defrem.h"
#include "utils/typcache.h"
#include "utils/numeric.h"
#include "optimizer/tlist.h"
#include "nodes/makefuncs.h"
#include "catalog/pg_operator.h"
#include "lib/stringinfo.h"
#include "utils/elog.h"
#include "utils/rel.h"
#include "utils/uri.h"
#include "access/relscan.h"
#include "access/heapam.h"
#include "access/hd_work_mgr.h"
#include "catalog/pg_proc.h"
#include "tcop/dest.h"
#include "commands/trigger.h"
#include "parser/parse_coerce.h"
#include "utils/selfuncs.h"
#include "postmaster/identity.h"
#include "utils/faultinjector.h"

extern
Query *preprocess_query_optimizer(Query *pquery, ParamListInfo boundParams);

extern
List *pg_parse_and_rewrite(const char *query_string, Oid *paramTypes, int iNumParams);

extern
PlannedStmt *pg_plan_query(Query *pqueryTree, ParamListInfo boundParams, QueryResourceLife resource_life);

extern
char * get_rel_name(Oid relid);

extern
Relation RelationIdGetRelation(Oid relationId);

extern
void RelationClose(Relation relation);

extern
Oid get_atttype(Oid relid, AttrNumber attnum);

extern
RegProcedure get_opcode(Oid opid);

extern
void ExecutorStart(QueryDesc *pqueryDesc, int iEFlags);

extern
TupleTableSlot *ExecutorRun(QueryDesc *pqueryDesc, ScanDirection direction, long lCount);

extern
void ExecutorEnd(QueryDesc *pqueryDesc);

extern 
char *read_file(const char *filename);

extern FaultInjectorType_e FaultInjector_InjectFaultIfSet(
							   FaultInjectorIdentifier_e identifier,
							   DDLStatement_e			 ddlStatement,
							   char*					 databaseName,
							   char*					 tableName);

} // end extern C



#endif // GPDBDefs_H

// EOF
