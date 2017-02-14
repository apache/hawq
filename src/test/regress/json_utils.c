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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/*
 * json_utils.c
 *
 *  Created on: Mar 5, 2015
 *      Author: antova
 */

#include "catalog/external/externalmd.h"
#include "postgres.h"
#include "access/hd_work_mgr.h"
#include "funcapi.h"
#include "catalog/catquery.h"
#include "catalog/gp_policy.h"
#include "catalog/namespace.h"
#include "catalog/pg_database.h"
#include "catalog/pg_exttable.h"
#include "catalog/pg_namespace.h"
#include "cdb/cdbinmemheapam.h"
#include "nodes/makefuncs.h"
#include "utils/array.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "access/transam.h"

/*
 * number of output columns for the UDFs for scanning in memory catalog tables
 */
#define NUM_COLS 3
#define NUM_COLS_EXTTABLE 5

static 
char *read_file(const char *filename);

static
Oid GetRelationOid(char *tableName);

/* utility functions for loading json files */
extern Datum load_json_data(PG_FUNCTION_ARGS);
extern Datum caql_scan_in_memory_pg_namespace(PG_FUNCTION_ARGS);
extern Datum caql_scan_in_memory_pg_type(PG_FUNCTION_ARGS);
extern Datum caql_scan_in_memory_gp_distribution_policy(PG_FUNCTION_ARGS);
extern Datum caql_scan_in_memory_pg_exttable(PG_FUNCTION_ARGS);
extern Datum caql_scan_in_memory_pg_attribute(PG_FUNCTION_ARGS);
extern Datum caql_insert_into_heap_pg_class(PG_FUNCTION_ARGS);
extern Datum caql_delete_from_heap_pg_class(PG_FUNCTION_ARGS);
extern Datum get_next_external_oid(PG_FUNCTION_ARGS);
extern Datum set_next_external_oid(PG_FUNCTION_ARGS);
extern Datum min_external_oid(PG_FUNCTION_ARGS);

PG_FUNCTION_INFO_V1(load_json_data);
PG_FUNCTION_INFO_V1(caql_scan_in_memory_pg_namespace);
PG_FUNCTION_INFO_V1(caql_scan_in_memory_pg_type);
PG_FUNCTION_INFO_V1(caql_scan_in_memory_gp_distribution_policy);
PG_FUNCTION_INFO_V1(caql_scan_in_memory_pg_exttable);
PG_FUNCTION_INFO_V1(caql_scan_in_memory_pg_attribute);
PG_FUNCTION_INFO_V1(caql_insert_into_heap_pg_class);
PG_FUNCTION_INFO_V1(caql_delete_from_heap_pg_class);
PG_FUNCTION_INFO_V1(get_next_external_oid);
PG_FUNCTION_INFO_V1(set_next_external_oid);
PG_FUNCTION_INFO_V1(min_external_oid);

Datum
load_json_data(PG_FUNCTION_ARGS)
{
	text *filename = PG_GETARG_TEXT_P(0);
	char *filenameStr = text_to_cstring(filename);

	char *pcbuf = read_file(filenameStr);
	StringInfoData buf;
	initStringInfo(&buf);
	appendStringInfo(&buf, "%s", pcbuf);
    
	List *items = ParsePxfEntries(&buf, HiveProfileName, HcatalogDbOid);
	pfree(buf.data);
	
	StringInfoData tblNames;
	initStringInfo(&tblNames);
	ListCell *lc = NULL;
	foreach (lc, items)
	{
		PxfItem *item = (PxfItem *) lfirst(lc);
		appendStringInfo(&tblNames, "%s.%s ", item->path, item->name);
	}
	
	PG_RETURN_TEXT_P(cstring_to_text(tblNames.data));
}

char *
read_file(const char *filename)
{
	FILE *pf = fopen(filename, "r");
	if (NULL == pf)
	{
		elog(ERROR, "Could not open file %s for reading", filename);
	}
	
	StringInfoData strinfo;
	initStringInfo(&strinfo);

	const int len = 1024;
	char buf[len];
	
	while (NULL != fgets(buf, len, pf))
	{
		appendStringInfo(&strinfo, "%s", buf);
	}

	fclose(pf);
	return strinfo.data;
}

Datum
caql_scan_in_memory_pg_namespace(PG_FUNCTION_ARGS)
{
	text *namespaceNameText = PG_GETARG_TEXT_P(0);
	char *namespaceNameStr =  text_to_cstring(namespaceNameText);

	FuncCallContext    *funcctx;
	Datum				result;
	MemoryContext		oldcontext;
	TupleDesc	tupdesc;
	HeapTuple	restuple;
	HeapTuple readtup = NULL;
	cqContext  *pcqCtx;
	StringInfoData buf;
	initStringInfo(&buf);

	if (SRF_IS_FIRSTCALL())
	{
		funcctx = SRF_FIRSTCALL_INIT();
		/* switch context when allocating stuff to be used in later calls */
		oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

		tupdesc = CreateTemplateTupleDesc(NUM_COLS, false);
		TupleDescInitEntry(tupdesc, (AttrNumber) 1, "nspname",
						   TEXTOID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 2, "nspdboid",
						   OIDOID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 3, "oid",
						   OIDOID, -1, 0);
		funcctx->tuple_desc = BlessTupleDesc(tupdesc);

		/* create tuples for pg_namespace table */
		pcqCtx = caql_beginscan(
				NULL,
				cql("SELECT * FROM pg_namespace "
						" WHERE nspname = :1 and nspdboid = :2",
						CStringGetDatum((char *) namespaceNameStr), ObjectIdGetDatum(HcatalogDbOid)));

		funcctx->user_fctx = pcqCtx;

		/* return to original context when allocating transient memory */
		MemoryContextSwitchTo(oldcontext);
	}

	funcctx = SRF_PERCALL_SETUP();
	pcqCtx = (cqContext *)funcctx->user_fctx;

	if (NULL != (readtup = caql_getnext(pcqCtx)))
	{
		Datum values[NUM_COLS];
		bool nulls[NUM_COLS];

		Relation relation = RelationIdGetRelation(NamespaceRelationId);
		TupleDesc tupleDesc = RelationGetDescr(relation);
		
		char *nspname = DatumGetCString(tuple_getattr(readtup, tupleDesc, Anum_pg_namespace_nspname));
		text *t = cstring_to_text(nspname);

		values[0] = PointerGetDatum(t);
		nulls[0]  = false;

		values[1] = tuple_getattr(readtup, tupleDesc, Anum_pg_namespace_nspdboid);
		nulls[1]  = false;

		values[2] = ObjectIdGetDatum(HeapTupleGetOid(readtup));
		nulls[2]  = false;
		
		elog(DEBUG1, "Namespace oid: %d", HeapTupleGetOid(readtup));
		
		/* build tuple */
		restuple = heap_form_tuple(funcctx->tuple_desc, values, nulls);

		/* make the tuple into a datum */
		result = HeapTupleGetDatum(restuple);

		RelationClose(relation);
		SRF_RETURN_NEXT(funcctx, result);
	}
	else
	{
		caql_endscan(pcqCtx);
		SRF_RETURN_DONE(funcctx);
	}
}

Datum
caql_scan_in_memory_pg_type(PG_FUNCTION_ARGS)
{
	text *relNameText = PG_GETARG_TEXT_P(0);
	char *relNameStr =  text_to_cstring(relNameText);

	FuncCallContext    *funcctx;
	Datum				result;
	MemoryContext		oldcontext;
	TupleDesc	tupdesc;
	HeapTuple	restuple;
	HeapTuple readtup = NULL;
	cqContext  *pcqCtx;
	StringInfoData buf;
	initStringInfo(&buf);

	if (SRF_IS_FIRSTCALL())
	{
		funcctx = SRF_FIRSTCALL_INIT();
		/* switch context when allocating stuff to be used in later calls */
		oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

		tupdesc = CreateTemplateTupleDesc(NUM_COLS, false);
		TupleDescInitEntry(tupdesc, (AttrNumber) 1, "typoid",
						   OIDOID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 2, "typname",
						   TEXTOID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 3, "typnamespace",
						   OIDOID, -1, 0);
		funcctx->tuple_desc = BlessTupleDesc(tupdesc);

		/* create tuples for pg_type table */
		pcqCtx = caql_beginscan(
				NULL,
				cql("SELECT * FROM pg_type "
						" WHERE typname = :1",
						CStringGetDatum((char *) relNameStr)));

		funcctx->user_fctx = pcqCtx;

		/* return to original context when allocating transient memory */
		MemoryContextSwitchTo(oldcontext);
	}

	funcctx = SRF_PERCALL_SETUP();
	pcqCtx = (cqContext *)funcctx->user_fctx;

	if (NULL != (readtup = caql_getnext(pcqCtx)))
	{
		Datum values[NUM_COLS];
		bool nulls[NUM_COLS];

		Relation relation = RelationIdGetRelation(TypeRelationId);
		TupleDesc tupleDesc = RelationGetDescr(relation);

		values[0] = ObjectIdGetDatum(HeapTupleGetOid(readtup));
		nulls[0]  = false;

		elog(DEBUG1, "Type oid: %d", HeapTupleGetOid(readtup));

		char *typname = DatumGetCString(tuple_getattr(readtup, tupleDesc, Anum_pg_type_typname));
		text *t = cstring_to_text(typname);

		values[1] = PointerGetDatum(t);
		nulls[1]  = false;

		values[2] = tuple_getattr(readtup, tupleDesc, Anum_pg_type_typnamespace);
		nulls[2]  = false;

		/* build tuple */
		restuple = heap_form_tuple(funcctx->tuple_desc, values, nulls);

		/* make the tuple into a datum */
		result = HeapTupleGetDatum(restuple);

		RelationClose(relation);
		SRF_RETURN_NEXT(funcctx, result);
	}
	else
	{
		caql_endscan(pcqCtx);
		SRF_RETURN_DONE(funcctx);
	}
}

Datum
caql_scan_in_memory_gp_distribution_policy(PG_FUNCTION_ARGS)
{
	text *tableName = PG_GETARG_TEXT_P(0);
	char *tableNameStr = text_to_cstring(tableName);
	Oid reloid = InvalidOid;

	FuncCallContext    *funcctx;
	Datum				result;
	MemoryContext		oldcontext;
	TupleDesc	tupdesc;
	HeapTuple	restuple;
	HeapTuple pgclasstup = NULL;
	HeapTuple readtup = NULL;
	cqContext  *pcqCtx;
	StringInfoData buf;
	initStringInfo(&buf);

	if (SRF_IS_FIRSTCALL())
	{
		funcctx = SRF_FIRSTCALL_INIT();
		/* switch context when allocating stuff to be used in later calls */
		oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

		tupdesc = CreateTemplateTupleDesc(NUM_COLS, false);
		TupleDescInitEntry(tupdesc, (AttrNumber) 1, "tableoid",
						   OIDOID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 2, "tablename",
						   TEXTOID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 3, "distpolicy",
						   TEXTOID, -1, 0);
		funcctx->tuple_desc = BlessTupleDesc(tupdesc);

		/* iterate on pg_class and query
		 * gp_distribution_policy by given oid */
		pcqCtx = caql_beginscan(
						NULL,
						cql("SELECT oid FROM pg_class "
						" WHERE relname = :1",
						CStringGetDatum(tableNameStr)));

		funcctx->user_fctx = pcqCtx;

		/* return to original context when allocating transient memory */
		MemoryContextSwitchTo(oldcontext);
	}

	funcctx = SRF_PERCALL_SETUP();
	pcqCtx = (cqContext *)funcctx->user_fctx;

	if (NULL != (pgclasstup = caql_getnext(pcqCtx)))
	{
		Datum values[NUM_COLS];
		bool nulls[NUM_COLS];

		/* create tuples for pg_exttable table */
		cqContext* pcqCtx1 = caql_beginscan(
				NULL,
				cql("SELECT * FROM gp_distribution_policy "
				" WHERE localoid = :1",
				ObjectIdGetDatum(HeapTupleGetOid(pgclasstup))));

		Relation relation = RelationIdGetRelation(GpPolicyRelationId);
		TupleDesc tupleDesc = RelationGetDescr(relation);

		readtup = caql_getnext(pcqCtx1);

		values[0] = ObjectIdGetDatum(reloid);
		nulls[0]  = false;

		text *t1 = cstring_to_text(tableNameStr);

		values[1] = PointerGetDatum(t1);
		nulls[1]  = false;

		text *t2 = NULL;
		bool isnull;
		Datum policy = heap_getattr(readtup, Anum_gp_policy_attrnums, tupleDesc, &isnull);
		if (isnull)
		{
			t2 = cstring_to_text("null");
		}
		else
		{
			/* not tested! */
			Datum* elems = NULL;
			int nelems;
			deconstruct_array(DatumGetArrayTypeP(policy),
					INT2OID, -1, false, 'i',
					&elems, NULL, &nelems);
			Assert(nelems > 0);
			StringInfoData elems_str;
			initStringInfo(&elems_str);
			for (int i = 0; i < nelems; i++)
			{
				appendStringInfo(&elems_str, "%d", DatumGetInt16(elems[0]));
				if (i != nelems)
				{
					appendStringInfo(&elems_str, ", ");
				}
			}
			t2 = cstring_to_text(elems_str.data);
			pfree(elems_str.data);
		}

		values[2] = PointerGetDatum(t2);
		nulls[2]  = false;

		/* build tuple */
		restuple = heap_form_tuple(funcctx->tuple_desc, values, nulls);

		/* make the tuple into a datum */
		result = HeapTupleGetDatum(restuple);

		RelationClose(relation);
		/* there should be only one value per oid */
		Assert(NULL == caql_getnext(pcqCtx1)) ;
		caql_endscan(pcqCtx1);

		SRF_RETURN_NEXT(funcctx, result);
	}
	else
	{
		caql_endscan(pcqCtx);
		SRF_RETURN_DONE(funcctx);
	}
}

Datum
caql_scan_in_memory_pg_exttable(PG_FUNCTION_ARGS)
{
	text *tableName = PG_GETARG_TEXT_P(0);
	char *tableNameStr = text_to_cstring(tableName);
	Oid reloid = InvalidOid;

	FuncCallContext    *funcctx;
	Datum				result;
	MemoryContext		oldcontext;
	TupleDesc	tupdesc;
	HeapTuple	restuple;
	HeapTuple pgclasstup = NULL;
	HeapTuple readtup = NULL;
	cqContext  *pcqCtx;
	StringInfoData buf;
	initStringInfo(&buf);

	if (SRF_IS_FIRSTCALL())
	{
		funcctx = SRF_FIRSTCALL_INIT();
		/* switch context when allocating stuff to be used in later calls */
		oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

		tupdesc = CreateTemplateTupleDesc(NUM_COLS_EXTTABLE, false);
		TupleDescInitEntry(tupdesc, (AttrNumber) 1, "exttableoid",
						   OIDOID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 2, "exttablename",
						   TEXTOID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 3, "exttablelocation",
						   TEXTOID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 4, "exttableformat",
						   TEXTOID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 5, "exttableformatoptions",
						   TEXTOID, -1, 0);
		funcctx->tuple_desc = BlessTupleDesc(tupdesc);

		/* iterate on pg_class and query
		 * pg_exttable by given oid */
		pcqCtx = caql_beginscan(
				NULL,
				cql("SELECT oid FROM pg_class "
				" WHERE relname = :1",
				CStringGetDatum(tableNameStr)));

		funcctx->user_fctx = pcqCtx;

		/* return to original context when allocating transient memory */
		MemoryContextSwitchTo(oldcontext);
	}

	funcctx = SRF_PERCALL_SETUP();
	pcqCtx = (cqContext *)funcctx->user_fctx;

	if (NULL != (pgclasstup = caql_getnext(pcqCtx)))
	{
		Datum values[NUM_COLS_EXTTABLE];
		bool nulls[NUM_COLS_EXTTABLE];

		/* create tuples for pg_exttable table */
		cqContext* pcqCtx1 = caql_beginscan(
				NULL,
				cql("SELECT * FROM pg_exttable "
				" WHERE reloid = :1",
				ObjectIdGetDatum(HeapTupleGetOid(pgclasstup))));

		Relation relation = RelationIdGetRelation(ExtTableRelationId);
		TupleDesc tupleDesc = RelationGetDescr(relation);

        readtup = caql_getnext(pcqCtx1);

		values[0] = ObjectIdGetDatum(reloid);
		nulls[0]  = false;

		text *t1 = cstring_to_text(tableNameStr);

		values[1] = PointerGetDatum(t1);
		nulls[1]  = false;

		Datum locations = tuple_getattr(readtup, tupleDesc, Anum_pg_exttable_location);
		Datum fmttype = tuple_getattr(readtup, tupleDesc, Anum_pg_exttable_fmttype);
		Datum fmtopts = tuple_getattr(readtup, tupleDesc, Anum_pg_exttable_fmtopts);
		Datum* elems = NULL;
		int nelems;
		deconstruct_array(DatumGetArrayTypeP(locations),
						  TEXTOID, -1, false, 'i',
						  &elems, NULL, &nelems);
		Assert(nelems > 0);
		char *loc_str = DatumGetCString(DirectFunctionCall1(textout, elems[0]));
		text *t2 = cstring_to_text(loc_str);
		values[2] = PointerGetDatum(t2);
		nulls[2]  = false;

		char fmttype_chr = DatumGetChar(fmttype);
		text *t3 = cstring_to_text_with_len(&fmttype_chr, 1);
		values[3] = PointerGetDatum(t3);
		nulls[3]  = false;

		char *fmtopts_str = DatumGetCString(fmtopts);
		text *t4 = cstring_to_text(fmtopts_str);
		values[4] = PointerGetDatum(t4);
		nulls[4]  = false;

		/* build tuple */
		restuple = heap_form_tuple(funcctx->tuple_desc, values, nulls);

		/* make the tuple into a datum */
		result = HeapTupleGetDatum(restuple);

		RelationClose(relation);
		/* there should be only one value per oid */
		Assert(NULL == caql_getnext(pcqCtx1)) ;
		caql_endscan(pcqCtx1);

		SRF_RETURN_NEXT(funcctx, result);
	}
	else
	{
		caql_endscan(pcqCtx);
		SRF_RETURN_DONE(funcctx);
	}
}

Datum
caql_scan_in_memory_pg_attribute(PG_FUNCTION_ARGS)
{
	text *tableNameText = PG_GETARG_TEXT_P(0);
	char *tableNameStr =  text_to_cstring(tableNameText);

	FuncCallContext    *funcctx;
	Datum				result;
	MemoryContext		oldcontext;
	TupleDesc	tupdesc;
	HeapTuple	restuple;
	HeapTuple readtup = NULL;
	cqContext  *pcqCtx;
	StringInfoData buf;
	initStringInfo(&buf);

	Oid relid = GetRelationOid(tableNameStr);
	if (SRF_IS_FIRSTCALL())
	{
		funcctx = SRF_FIRSTCALL_INIT();
		/* switch context when allocating stuff to be used in later calls */
		oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

		tupdesc = CreateTemplateTupleDesc(NUM_COLS, false);
		TupleDescInitEntry(tupdesc, (AttrNumber) 1, "attname",
						   TEXTOID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 2, "atttype",
						   OIDOID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 3, "atttypmod",
						   INT4OID, -1, 0);
		funcctx->tuple_desc = BlessTupleDesc(tupdesc);

		/* create tuples for pg_attribute table */
		pcqCtx = caql_beginscan(
				NULL,
				cql("SELECT * FROM pg_attribute "
						" WHERE attrelid = :1",
						ObjectIdGetDatum(relid)));
		funcctx->user_fctx = pcqCtx;

		/* return to original context when allocating transient memory */
		MemoryContextSwitchTo(oldcontext);
	}

	funcctx = SRF_PERCALL_SETUP();
	pcqCtx = (cqContext *)funcctx->user_fctx;

	if (NULL != (readtup = caql_getnext(pcqCtx)))
	{
		Datum values[NUM_COLS];
		bool nulls[NUM_COLS];

		Relation relation = RelationIdGetRelation(AttributeRelationId);
		TupleDesc tupleDesc = RelationGetDescr(relation);

		char *attname = DatumGetCString(tuple_getattr(readtup, tupleDesc, Anum_pg_attribute_attname));
		text *t = cstring_to_text(attname);

		values[0] = PointerGetDatum(t);
		nulls[0]  = false;

		values[1] = tuple_getattr(readtup, tupleDesc, Anum_pg_attribute_atttypid);
		nulls[1]  = false;

		values[2] = tuple_getattr(readtup, tupleDesc, Anum_pg_attribute_atttypmod);
		nulls[2]  = false;
		
		/* build tuple */
		restuple = heap_form_tuple(funcctx->tuple_desc, values, nulls);

		/* make the tuple into a datum */
		result = HeapTupleGetDatum(restuple);

		RelationClose(relation);
		SRF_RETURN_NEXT(funcctx, result);
	}
	else
	{
		caql_endscan(pcqCtx);
		SRF_RETURN_DONE(funcctx);
	}
}

Oid GetRelationOid(char *tableName)
{
	cqContext *pcqCtx = caql_beginscan(
			NULL,
			cql("SELECT * FROM pg_class "
					" WHERE relname = :1",
					CStringGetDatum((char *) tableName)));

	HeapTuple ht = caql_getnext(pcqCtx);
	Oid result = InvalidOid;
	if (NULL != ht)
	{
		result = HeapTupleGetOid(ht);
	}

	elog(DEBUG1, "Found relname %s, oid: %d", tableName, result);
	caql_endscan(pcqCtx);
	return result;
}

/*
 * This function must be used in conjunction with
 * caql_delete_from_heap_pg_class
 * and exclusively for testing external Oid setting
 */
Datum
caql_insert_into_heap_pg_class(PG_FUNCTION_ARGS)
	{
	Oid relid = PG_GETARG_OID(0);
	char *tblname = text_to_cstring(PG_GETARG_TEXT_P(1));

	Datum values[Natts_pg_class];
	bool nulls[Natts_pg_class];

	for (int i = 0; i < Natts_pg_class; i++)
	{
		nulls[i] = true;
		values[i] = (Datum) 0;
	}

	NameData name;
	namestrcpy(&name, tblname);

	values[Anum_pg_class_relname - 1] = NameGetDatum(&name);
	values[Anum_pg_class_relnamespace - 1] = ObjectIdGetDatum((Oid) NSPDBOID_CURRENT);
	nulls[Anum_pg_class_relname - 1] = false;
	nulls[Anum_pg_class_relnamespace - 1] = false;

	cqContext  *pcqCtx = caql_beginscan(
			NULL,
			cql("INSERT INTO pg_class", NULL));

	HeapTuple tup = caql_form_tuple(pcqCtx, values, nulls);
	HeapTupleSetOid(tup, relid);

	caql_insert(pcqCtx, tup);
	caql_endscan(pcqCtx);

	StringInfoData buf;
	initStringInfo(&buf);

	appendStringInfo(&buf, "inserted tuple to heap table pg_class (oid %u, relname %s)", relid, tblname);

	PG_RETURN_TEXT_P(cstring_to_text(buf.data));
}

/*
 * This function must be used in conjunction with
 * caql_insert_into_heap_pg_class
 * and exclusively for testing external Oid setting
 */
Datum
caql_delete_from_heap_pg_class(PG_FUNCTION_ARGS)
{
	Oid relid = PG_GETARG_OID(0);

	cqContext *pcqCtx = caql_beginscan(
			NULL,
			cql("SELECT * FROM pg_class "
				" WHERE oid = :1 "
				" FOR UPDATE ",
				ObjectIdGetDatum(relid)));

	HeapTuple tuple = caql_getnext(pcqCtx);

	if (!HeapTupleIsValid(tuple))
		ereport(ERROR,
			(errcode(ERRCODE_UNDEFINED_OBJECT),
			 errmsg("pg_class table relid=%u does not exist", relid)));

	/* Delete the pg_class table entry from the catalog (pg_class) */
	caql_delete_current(pcqCtx);

	/* Finish up scan and close pg_class catalog */
	caql_endscan(pcqCtx);

	StringInfoData buf;
	initStringInfo(&buf);

	appendStringInfo(&buf, "deleted tuple oid=%u from heap table pg_class", relid);

	PG_RETURN_TEXT_P(cstring_to_text(buf.data));
}

Datum
get_next_external_oid(PG_FUNCTION_ARGS)
{
	PG_RETURN_OID(GetCurrentExternalObjectId());
}

Datum
set_next_external_oid(PG_FUNCTION_ARGS)
{
	Oid oid = PG_GETARG_OID(0);
	SetCurrentExternalObjectId(oid);
	
	PG_RETURN_OID(GetCurrentExternalObjectId());
}

Datum
min_external_oid(PG_FUNCTION_ARGS)
{
	PG_RETURN_OID(FirstExternalObjectId);
}
