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
#include "cdb/cdbvars.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"
#include "catalog/catquery.h"
#include "catalog/pg_namespace.h"
#include "catalog/pg_database.h"

/* utility functions for converting schemas to hcatalog and back to internal */
extern Datum convert_to_hcatalog_schema(PG_FUNCTION_ARGS);
extern Datum convert_to_internal_schema(PG_FUNCTION_ARGS);

static bool convert_schema_type(char *schema, Oid oldDboid, Oid newDboid);

PG_FUNCTION_INFO_V1(convert_to_hcatalog_schema);
Datum
convert_to_hcatalog_schema(PG_FUNCTION_ARGS)
{
	text *schemaText = PG_GETARG_TEXT_P(0);
	char *schemaStr = text_to_cstring(schemaText);

	StringInfoData buf;
	initStringInfo(&buf);

	if (!convert_schema_type(schemaStr, NSPDBOID_CURRENT, HcatalogDbOid))
	{
		appendStringInfo(&buf, "Schema %s not found!", schemaStr);
	}
	else
	{
		appendStringInfo(&buf, "Converted %s to hcatalog schema", schemaStr);	
	}

	PG_RETURN_TEXT_P(cstring_to_text(buf.data));
}

PG_FUNCTION_INFO_V1(convert_to_internal_schema);
Datum
convert_to_internal_schema(PG_FUNCTION_ARGS)
{
	text *schemaText = PG_GETARG_TEXT_P(0);
	char *schemaStr = text_to_cstring(schemaText);

	StringInfoData buf;
	initStringInfo(&buf);

	if (!convert_schema_type(schemaStr, HcatalogDbOid, NSPDBOID_CURRENT))
	{
		appendStringInfo(&buf, "Schema hcatalog.%s not found!", schemaStr);
	}
	else
	{
		appendStringInfo(&buf, "Converted %s to internal schema", schemaStr);	
	}

	PG_RETURN_TEXT_P(cstring_to_text(buf.data));
}

/* convert schema type from old dboid to newdboid */
bool convert_schema_type(char *schema, Oid oldDboid, Oid newDboid)
{
	Assert(NSPDBOID_CURRENT == oldDboid || NSPDBOID_CURRENT == newDboid);
	Assert(HcatalogDbOid == oldDboid || HcatalogDbOid == newDboid);
	Assert(newDboid != oldDboid);

	/* create tuples for pg_class table */
	HeapTuple nsptup = NULL;
	HeapTuple copytup = NULL;
	cqContext	cqc;

	Assert(Gp_role != GP_ROLE_EXECUTE);

	Relation rel = heap_open(NamespaceRelationId, RowExclusiveLock);

	cqContext *pcqCtx = caql_addrel(cqclr(&cqc), rel);
	nsptup =  caql_getfirst(
			pcqCtx,
			cql("SELECT * FROM pg_namespace "
				" WHERE nspname = :1 and nspdboid = :2",
				CStringGetDatum(schema), ObjectIdGetDatum(oldDboid)));


	if (!HeapTupleIsValid(nsptup))
	{
		heap_close(rel, NoLock);
		return false;
	}

	Datum values[Natts_pg_namespace];
	bool nulls[Natts_pg_namespace];
	bool replaces[Natts_pg_namespace];
	
	MemSet(values, 0, sizeof(values));
	MemSet(nulls, false, sizeof(nulls));
	MemSet(replaces, false, sizeof(replaces));
	
	replaces[Anum_pg_namespace_nspdboid - 1] = true;
	values[Anum_pg_namespace_nspdboid - 1] = newDboid;
	copytup = caql_modify_current(pcqCtx, values, nulls, replaces);

	caql_update_current(pcqCtx, copytup);
	heap_close(rel, NoLock);

	heap_freetuple(copytup);
	
	return true;
}
