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

#include "postgres.h"
#include "funcapi.h"
#include "catalog/catquery.h"
#include "catalog/pg_namespace.h"
#include "utils/builtins.h"

/*
 * number of output columns for the UDF for scanning in memory pg_class
 */
#define NUM_COLS 3

/* tests for caql in-memory-only support */
extern Datum caql_copy_to_in_memory_pg_class(PG_FUNCTION_ARGS);
extern Datum caql_scan_in_memory_pg_class(PG_FUNCTION_ARGS);
extern Datum caql_scan_backward_in_memory_pg_class(PG_FUNCTION_ARGS);

extern Datum caql_insert_to_in_memory_pg_class(PG_FUNCTION_ARGS);
extern Datum caql_insert_to_in_memory_pg_attribute(PG_FUNCTION_ARGS);
extern Datum caql_insert_to_in_memory_pg_namespace(PG_FUNCTION_ARGS);

static HeapTuple
getnext(cqContext* pcqCtx, bool isForward);
static Datum
caql_scan_in_memory_pg_class_private(PG_FUNCTION_ARGS, bool isForward);

PG_FUNCTION_INFO_V1(caql_copy_to_in_memory_pg_class);
Datum
caql_copy_to_in_memory_pg_class(PG_FUNCTION_ARGS)
{

	text *inText = PG_GETARG_TEXT_P(0);;
	char *inStr = text_to_cstring(inText);
	char kind = PG_GETARG_CHAR(1);

	StringInfoData buf;
	initStringInfo(&buf);

	/* create tuples for pg_class table */
	HeapTuple reltup = NULL;
	HeapTuple copytup = NULL;
	Form_pg_class relform;
	cqContext  *pcqCtx;
	cqContext  *pcqCtxInsert;

	pcqCtx = caql_beginscan(
			NULL,
			cql("SELECT * FROM pg_class "
				" WHERE relname = :1",
				CStringGetDatum((char *) inStr)));

	reltup = caql_getnext(pcqCtx);

	if (NULL == reltup)
	{
		appendStringInfo(&buf, "no tuples with relname=%s found!", inStr);
	}
	else
	{
	    copytup = heaptuple_copy_to(reltup, NULL, NULL);

		relform = (Form_pg_class) GETSTRUCT(copytup);
		relform->relkind = kind;
		appendStringInfo(&buf, "table pg_class, insert 1 line (relname %s, relkind %c)", NameStr(relform->relname), kind);

		/* insert */
		pcqCtxInsert = caql_beginscan(
				NULL,
				cql("INSERT INTO pg_class", NULL));
		caql_insert_inmem(pcqCtxInsert, copytup);
		caql_endscan(pcqCtxInsert);

		heap_freetuple(copytup);
	}

	caql_endscan(pcqCtx);

	PG_RETURN_TEXT_P(cstring_to_text(buf.data));
}

PG_FUNCTION_INFO_V1(caql_scan_in_memory_pg_class);
Datum
caql_scan_in_memory_pg_class(PG_FUNCTION_ARGS)
{
	return caql_scan_in_memory_pg_class_private(fcinfo, true);
}

PG_FUNCTION_INFO_V1(caql_scan_backward_in_memory_pg_class);
Datum
caql_scan_backward_in_memory_pg_class(PG_FUNCTION_ARGS)
{
	return caql_scan_in_memory_pg_class_private(fcinfo, false);
}

static Datum
caql_scan_in_memory_pg_class_private(PG_FUNCTION_ARGS, bool isForward)
{
	text *inText = PG_GETARG_TEXT_P(0);
	char *inStr = text_to_cstring(inText);;

	FuncCallContext    *funcctx;
	Datum				result;
	MemoryContext		oldcontext;
	TupleDesc	tupdesc;
	HeapTuple	restuple;
	//int count = 0;
	HeapTuple readtup = NULL;
	cqContext  *pcqCtx;
	StringInfoData buf;
	initStringInfo(&buf);

	if (SRF_IS_FIRSTCALL())
	{
		funcctx = SRF_FIRSTCALL_INIT();
		/* switch context when allocating stuff to be used in later calls */
		oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

		tupdesc = CreateTemplateTupleDesc(NUM_COLS, false /*hasoid*/);
		TupleDescInitEntry(tupdesc, (AttrNumber) 1, "relname",
						   TEXTOID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 2, "relkind",
						   CHAROID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 3, "oid",
						   OIDOID, -1, 0);
		funcctx->tuple_desc = BlessTupleDesc(tupdesc);

		/* create tuples for pg_class table */
		pcqCtx = caql_beginscan(
				NULL,
				cql("SELECT * FROM pg_class "
						" WHERE relname = :1",
						CStringGetDatum((char *) inStr)));
		funcctx->user_fctx = pcqCtx;

		/* return to original context when allocating transient memory */
		MemoryContextSwitchTo(oldcontext);
	}

	funcctx = SRF_PERCALL_SETUP();
	pcqCtx = (cqContext *)funcctx->user_fctx;

	if (NULL != (readtup = getnext(pcqCtx, isForward)))
	{
		Datum values[NUM_COLS];
		bool nulls[NUM_COLS];

		Relation relation = RelationIdGetRelation(RelationRelationId);
		TupleDesc tupleDesc = RelationGetDescr(relation);

		char *nspname = DatumGetCString(tuple_getattr(readtup, tupleDesc, Anum_pg_class_relname));
		text *t = cstring_to_text(nspname);

		values[0] = PointerGetDatum(t);
		nulls[0] = false;

		values[1] = CharGetDatum(tuple_getattr(readtup, tupleDesc, Anum_pg_class_relkind));
		nulls[1] = false;

		values[2] = ObjectIdGetDatum(HeapTupleGetOid(readtup));
		nulls[2] = false;
		
		/* build tuple */
		restuple = heap_form_tuple(funcctx->tuple_desc, values, nulls);

		/* make the tuple into a datum */
		result = HeapTupleGetDatum(restuple);
		elog(DEBUG1, "Reloid: %d", HeapTupleGetOid(readtup));
		
		RelationClose(relation);
		SRF_RETURN_NEXT(funcctx, result);
	}
	else
	{
		caql_endscan(pcqCtx);
		SRF_RETURN_DONE(funcctx);
	}
}

static HeapTuple
getnext(cqContext* pcqCtx, bool isForward)
{
	if (isForward)
	{
		return caql_getnext(pcqCtx);
	}
	else
	{
		return caql_getprev(pcqCtx);
	}
}

PG_FUNCTION_INFO_V1(caql_insert_to_in_memory_pg_class);
Datum
caql_insert_to_in_memory_pg_class(PG_FUNCTION_ARGS)
{
	Oid relid = PG_GETARG_OID(0);
	char *tblname = text_to_cstring(PG_GETARG_TEXT_P(1));
	Oid nspid = PG_GETARG_OID(2);

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
	values[Anum_pg_class_relnamespace - 1] = ObjectIdGetDatum(nspid);
	nulls[Anum_pg_class_relname - 1] = false;
	nulls[Anum_pg_class_relnamespace - 1] = false;
	
	cqContext  *pcqCtx = caql_beginscan(
			NULL,
			cql("INSERT INTO pg_class", NULL));

	HeapTuple tup = caql_form_tuple(pcqCtx, values, nulls);
	HeapTupleSetOid(tup, relid);

	caql_insert_inmem(pcqCtx, tup);
	caql_endscan(pcqCtx);
	
	StringInfoData buf;
	initStringInfo(&buf);

	appendStringInfo(&buf, "inserted tuple to pg_class (oid %d, relname %s, relnamespace %d)", relid, tblname, nspid);

	PG_RETURN_TEXT_P(cstring_to_text(buf.data));
}

PG_FUNCTION_INFO_V1(caql_insert_to_in_memory_pg_namespace);
Datum
caql_insert_to_in_memory_pg_namespace(PG_FUNCTION_ARGS)
{
	Oid nspid = PG_GETARG_OID(0);
	char *nspname = text_to_cstring(PG_GETARG_TEXT_P(1));
	Oid nspdboid = PG_GETARG_OID(2);
	HeapTuple tuple;

	Datum values[Natts_pg_namespace];
	bool nulls[Natts_pg_namespace];

	/* initialize nulls and values */
	for (int i = 0; i < Natts_pg_namespace; i++)
	{
		nulls[i] = true;
		values[i] = (Datum) 0;
	}

	NameData name;
	namestrcpy(&name, nspname);

	values[Anum_pg_namespace_nspname - 1] = NameGetDatum(&name);
	values[Anum_pg_namespace_nspdboid - 1] = ObjectIdGetDatum(nspdboid);
	nulls[Anum_pg_namespace_nspname - 1] = false;
	nulls[Anum_pg_namespace_nspdboid - 1] = false;
	
	cqContext  *pcqCtx = caql_beginscan(
			NULL,
			cql("INSERT INTO pg_namespace", NULL));

	HeapTuple tup = caql_form_tuple(pcqCtx, values, nulls);
	HeapTupleSetOid(tup, nspid);

	caql_insert_inmem(pcqCtx, tup);
	caql_endscan(pcqCtx);

	StringInfoData buf;
	initStringInfo(&buf);
	appendStringInfo(&buf, "inserted tuple to pg_namespace (oid %d, nspname %s, nspdboid %d)", nspid, nspname, nspdboid);

	pcqCtx = caql_beginscan(
			NULL,
			cql("SELECT * FROM pg_namespace "
					"WHERE nspname = :1",
					CStringGetDatum((char *) nspname)));

	while (HeapTupleIsValid(tuple = caql_getnext(pcqCtx)))
	{
		Oid thisoid = HeapTupleGetOid(tuple);

		bool isNull;
		Datum data = caql_getattr(pcqCtx, Anum_pg_namespace_nspdboid, &isNull);
		Oid namespacedboid = DatumGetObjectId(data);
		data = caql_getattr(pcqCtx, Anum_pg_namespace_nspname, &isNull);
		char *namespacename = DatumGetCString(data);

		appendStringInfo(&buf, "\nresult tuple: (oid %d, nspname %s, nspdboid %d)", thisoid, namespacename, namespacedboid);
	}

	caql_endscan(pcqCtx);

	PG_RETURN_TEXT_P(cstring_to_text(buf.data));
}


PG_FUNCTION_INFO_V1(caql_insert_to_in_memory_pg_attribute);
Datum
caql_insert_to_in_memory_pg_attribute(PG_FUNCTION_ARGS)
{
	Oid attrelid = PG_GETARG_OID(0);
	char *attname = text_to_cstring(PG_GETARG_TEXT_P(1));
	AttrNumber attno = PG_GETARG_INT16(2);

	cqContext  *pcqCtx = caql_beginscan(
			NULL,
			cql("INSERT INTO pg_attribute", NULL));
	
	FormData_pg_attribute attributeD;
	HeapTuple attributeTuple = heap_addheader(Natts_pg_attribute,
									false,
									ATTRIBUTE_TUPLE_SIZE,
									(void *) &attributeD);
	
	Form_pg_attribute attribute = (Form_pg_attribute) GETSTRUCT(attributeTuple);
	
	attribute->attrelid = attrelid;
	namestrcpy(&(attribute->attname), attname);
	attribute->attnum = attno;

	caql_insert_inmem(pcqCtx, attributeTuple);
	caql_endscan(pcqCtx);
	
	StringInfoData buf;
	initStringInfo(&buf);

	appendStringInfo(&buf, "inserted tuple to pg_attribute (attrelid %d, attname %s, attno %d)", attribute->attrelid, NameStr(attribute->attname), attribute->attnum);

	PG_RETURN_TEXT_P(cstring_to_text(buf.data));
}
