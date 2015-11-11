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
/*
 * externalmd.c
 *
 *  Created on: Mar 6, 2015
 *      Author: antova
 *
 *
 *		Utilities for loading external hcatalog metadata
 *
 */

#include "postgres.h"
#include <json/json.h>

#include "miscadmin.h"
#include "access/transam.h"
#include "catalog/catquery.h"
#include "catalog/pg_database.h"
#include "catalog/pg_exttable.h"
#include "catalog/pg_namespace.h"
#include "catalog/namespace.h"
#include "catalog/hcatalog/externalmd.h"
#include "commands/typecmds.h"
#include "utils/builtins.h"
#include "utils/fmgroids.h"
#include "utils/lsyscache.h"
#include "utils/numeric.h"
#include "utils/guc.h"

static HCatalogTable *ParseHCatalogTable(struct json_object *hcatalogMD);
static void LoadHCatalogEntry(HCatalogTable *hcatalogTable);
static Oid LoadHCatalogNamespace(const char *namespaceName);
static void LoadHCatalogTable(Oid namespaceOid, HCatalogTable *hcatalogTable);
static void LoadHCatalogType(Oid relid, Oid reltypeoid, NameData relname, Oid relnamespaceoid);
static void LoadHCatalogDistributionPolicy(Oid relid, HCatalogTable *hcatalogTable);
static void LoadHCatalogExtTable(Oid relid, HCatalogTable *hcatalogTable);
static void LoadHCatalogColumns(Oid relid, List *columns);
int ComputeTypeMod(Oid typeOid, const char *colname, int *typemod, int nTypeMod);

const int maxNumTypeModifiers = 2;
/*
 * Parse a json response containing HCatalog metadata and load it in the in-memory heap tables,
 * Return the list of the parsed tables
 */
List *ParseHCatalogEntries(StringInfo json)
{
	struct json_object *jsonObj = json_tokener_parse(json->data);
	if ((NULL == jsonObj ) || is_error(jsonObj))
	{
		return NIL;
	}
	
	List *tables = NIL;
	struct json_object *jsonTables = json_object_object_get(jsonObj, "PXFMetadata");
	if ((jsonTables == NULL) || is_error(jsonTables))
	{
		return NIL;
	}
	
	const int numTables = json_object_array_length(jsonTables);
	for (int i = 0; i < numTables; i++)
	{
		struct json_object *jsonTable = json_object_array_get_idx(jsonTables, i);
		HCatalogTable *hcatalogTable = ParseHCatalogTable(jsonTable);
		LoadHCatalogEntry(hcatalogTable);
		tables = lappend(tables, hcatalogTable);
	}
		
	return tables;
}

/*
 * ParseHcatalogTable
 * 		Parse the given json object representing a single HCatalog table into the internal
 * 		representation
 */
HCatalogTable *ParseHCatalogTable(struct json_object *hcatalogMD)
{
	HCatalogTable *hcatalogTable = palloc0(sizeof(HCatalogTable));

	/* parse table name */
	struct json_object *jsonTable = json_object_object_get(hcatalogMD, "table");
	char *dbName = pstrdup(json_object_get_string(json_object_object_get(jsonTable, "dbName")));
	char *tableName = pstrdup(json_object_get_string(json_object_object_get(jsonTable, "tableName")));
	
	hcatalogTable->dbName = dbName;
	hcatalogTable->tableName = tableName;
	
	elog(DEBUG1, "Parsed table %s, namespace %s", tableName, dbName);
		
	/* parse columns */
	struct json_object *jsonColumns = json_object_object_get(hcatalogMD, "fields");
	const int numColumns = json_object_array_length(jsonColumns);
	for (int i = 0; i < numColumns; i++)
	{
		HCatalogColumn *hcatalogCol = palloc0(sizeof(HCatalogColumn));
		struct json_object *jsonCol = json_object_array_get_idx(jsonColumns, i);

		struct json_object *colName = json_object_object_get(jsonCol, "name");
		hcatalogCol->colName = pstrdup(json_object_get_string(colName));

		struct json_object *colType = json_object_object_get(jsonCol, "type");
		hcatalogCol->typeName = pstrdup(json_object_get_string(colType));
		hcatalogCol->nTypeModifiers = 0;
		
		elog(DEBUG1, "Parsing column %s, type %s", hcatalogCol->colName, hcatalogCol->typeName);

		struct json_object *jsonModifiers = json_object_object_get(jsonCol, "modifiers");
		if (NULL != jsonModifiers)
		{
			const int numModifiers = json_object_array_length(jsonModifiers);
			Assert(2 >= numModifiers);
			
			hcatalogCol->nTypeModifiers = numModifiers;
			for (int j = 0; j < numModifiers; j++)
			{
				struct json_object *jsonMod = json_object_array_get_idx(jsonModifiers, j);
				hcatalogCol->typeModifiers[j] = json_object_get_int(jsonMod);
				
				elog(DEBUG1, "modifier[%d]: %d", j, hcatalogCol->typeModifiers[j]);
			}
		}
		hcatalogTable->columns = lappend(hcatalogTable->columns, hcatalogCol);
	}

	return hcatalogTable;
}

/*
 * LoadHcatalogTable
 * 		Load the given hcatalog table into in-memory heap tables
 */
void LoadHCatalogEntry(HCatalogTable *hcatalogTable)
{
	Oid namespaceOid = LookupNamespaceId(hcatalogTable->dbName, HcatalogDbOid);

	if (!OidIsValid(namespaceOid))
	{
		/* hcatalog database name has not been mapped to a namespace yet: create it */
		namespaceOid = LoadHCatalogNamespace(hcatalogTable->dbName);
		elog(DEBUG1, "No namespace found: %s. Generated new namespace oid: %u", hcatalogTable->dbName, namespaceOid);
	}
	
	LoadHCatalogTable(namespaceOid, hcatalogTable);
}

/*
 * CreateHCatalogNamespace
 * 		Create an entry for the given HCatalog namespace in the in-memory heap tables and
 * 		return the reserved namespace oid
 */
Oid LoadHCatalogNamespace(const char *namespaceName)
{
	Oid namespaceOid = GetNewExternalObjectId();

	bool nulls[Natts_pg_namespace] = {false};
	Datum values[Natts_pg_namespace] = {(Datum) 0};
	
	NameData name;
	namestrcpy(&name, namespaceName);
	values[Anum_pg_namespace_nspname - 1] = NameGetDatum(&name);
	values[Anum_pg_namespace_nspdboid - 1] = ObjectIdGetDatum((Oid) HcatalogDbOid);
	values[Anum_pg_namespace_nspowner - 1] = ObjectIdGetDatum(GetUserId());
	nulls[Anum_pg_namespace_nspacl - 1] = true;

	cqContext  *pcqCtx = caql_beginscan(
			NULL,
			cql("INSERT INTO pg_namespace", NULL));
	
	HeapTuple tup = caql_form_tuple(pcqCtx, values, nulls);
	HeapTupleSetOid(tup, namespaceOid);
	
	caql_insert_inmem(pcqCtx, tup);
	caql_endscan(pcqCtx);
	
	return namespaceOid;
}

/*
 * LoadHCatalogTable
 * 		Load the metadata for an HCatalog table to pg_class and related catalog tables.
 */
void LoadHCatalogTable(Oid namespaceOid, HCatalogTable *hcatalogTable)
{
	/*
	 * assert entry is not already loaded in pg_class
	 */
	Oid relid = caql_getoid_only(
			NULL,
			NULL,
			cql("SELECT oid FROM pg_class "
				" WHERE relname = :1 and relnamespace = :2",
				CStringGetDatum(hcatalogTable->tableName), ObjectIdGetDatum(namespaceOid)));
	if (InvalidOid != relid)
	{
		ereport(ERROR,
				(errcode(ERRCODE_DUPLICATE_TABLE),
				 errmsg("relation \"hcatalog.%s.%s\" already exists",
						hcatalogTable->dbName, hcatalogTable->tableName),
				errOmitLocation(true)));
	}
	
	/* generate a new relid for the table */
	relid = GetNewExternalObjectId();
	elog(DEBUG1, "Generated new relation oid: %u", relid);

	/* generate new oid for pg_type entry */
	Oid reltypeoid = GetNewExternalObjectId();
	elog(DEBUG1, "Generated new reltype oid: %u", reltypeoid);

	Datum		values[Natts_pg_class];
	bool		nulls[Natts_pg_class];
	for (int i = 0; i < Natts_pg_class; i++)
	{
		nulls[i] = false;
		values[i] = (Datum) 0;
	}

	NameData name;
	namestrcpy(&name, hcatalogTable->tableName);

	values[Anum_pg_class_relname - 1] = NameGetDatum(&name);
	values[Anum_pg_class_relnamespace - 1] = ObjectIdGetDatum(namespaceOid);
	values[Anum_pg_class_reltype - 1] = ObjectIdGetDatum(reltypeoid); // TODO: Jun 05, 2015 - nhorn: check
	values[Anum_pg_class_relowner - 1] = ObjectIdGetDatum(GetUserId());
	values[Anum_pg_class_relam - 1] = ObjectIdGetDatum(InvalidOid); /* access method for indexes */
	values[Anum_pg_class_relfilenode - 1] = ObjectIdGetDatum(InvalidOid); /* physical storage file id */
	values[Anum_pg_class_reltablespace - 1] = ObjectIdGetDatum(InvalidOid);
	values[Anum_pg_class_relpages - 1] = Int32GetDatum(1); /* TODO: Mar 13, 2015 - lantova: get table statistics from HCatalog */
	values[Anum_pg_class_reltuples - 1] = 1; /* TODO: Mar 13, 2015 - lantova: get table statistics from HCatalog */
	values[Anum_pg_class_reltoastrelid - 1] = ObjectIdGetDatum(InvalidOid);
	values[Anum_pg_class_reltoastidxid - 1] = ObjectIdGetDatum(InvalidOid);
	values[Anum_pg_class_relaosegrelid - 1] = ObjectIdGetDatum(InvalidOid);
	values[Anum_pg_class_relaosegidxid - 1] = ObjectIdGetDatum(InvalidOid);
	values[Anum_pg_class_relhasindex - 1] = BoolGetDatum(false);
	values[Anum_pg_class_relisshared - 1] = BoolGetDatum(false);
	values[Anum_pg_class_relkind - 1] = CharGetDatum(RELKIND_RELATION);
	values[Anum_pg_class_relstorage - 1] = CharGetDatum(RELSTORAGE_EXTERNAL);
	values[Anum_pg_class_relnatts - 1] = Int16GetDatum(list_length(hcatalogTable->columns));
	values[Anum_pg_class_relchecks - 1] = Int16GetDatum(0);
	values[Anum_pg_class_reltriggers - 1] = Int16GetDatum(0);
	values[Anum_pg_class_relukeys - 1] = Int16GetDatum(0);
	values[Anum_pg_class_relfkeys - 1] = Int16GetDatum(0);
	values[Anum_pg_class_relrefs - 1] = Int16GetDatum(0);
	values[Anum_pg_class_relhasoids - 1] = BoolGetDatum(false);
	values[Anum_pg_class_relhaspkey - 1] = BoolGetDatum(false);
	values[Anum_pg_class_relhasrules - 1] = BoolGetDatum(false);
	values[Anum_pg_class_relhassubclass - 1] = BoolGetDatum(false);
	values[Anum_pg_class_relfrozenxid - 1] = TransactionIdGetDatum(InvalidTransactionId);
	nulls[Anum_pg_class_relacl - 1] = true;
	nulls[Anum_pg_class_reloptions - 1] = true;

	cqContext  *pcqCtx = caql_beginscan(
			NULL,
			cql("INSERT INTO pg_class", NULL));

	HeapTuple tup = caql_form_tuple(pcqCtx, values, nulls);
	HeapTupleSetOid(tup, relid);

	caql_insert_inmem(pcqCtx, tup);
	caql_endscan(pcqCtx);

	LoadHCatalogType(relid, reltypeoid, name, namespaceOid);
	LoadHCatalogDistributionPolicy(relid, hcatalogTable);
	LoadHCatalogExtTable(relid, hcatalogTable);
	LoadHCatalogColumns(relid, hcatalogTable->columns);
}

/*
 * LoadHCatalogType
 * 		Load the metadata for an HCatalog table to pg_type
 */
static void LoadHCatalogType(Oid relid, Oid reltypeoid, NameData relname, Oid relnamespaceoid)
{
	Datum		values[Natts_pg_type];
	bool		nulls[Natts_pg_type];
	for (int i = 0; i < Natts_pg_type; i++)
	{
		nulls[i] = false;
		values[i] = (Datum) 0;
	}

	values[Anum_pg_type_typname - 1] = NameGetDatum(&relname);
	values[Anum_pg_type_typnamespace - 1] = ObjectIdGetDatum(relnamespaceoid);
	values[Anum_pg_type_typowner - 1] = ObjectIdGetDatum(GetUserId());
	values[Anum_pg_type_typlen - 1] = Int16GetDatum(-1);
	values[Anum_pg_type_typbyval - 1] = BoolGetDatum(false);
	values[Anum_pg_type_typtype - 1] = CharGetDatum(TYPTYPE_COMPOSITE);
	values[Anum_pg_type_typisdefined - 1] = BoolGetDatum(true);
	values[Anum_pg_type_typdelim - 1] = CharGetDatum(DEFAULT_TYPDELIM);
	values[Anum_pg_type_typrelid - 1] = ObjectIdGetDatum(relid);
	values[Anum_pg_type_typelem - 1] = ObjectIdGetDatum(0); /* not an array */
	values[Anum_pg_type_typinput - 1] = ObjectIdGetDatum(F_RECORD_IN);
	values[Anum_pg_type_typoutput - 1] = ObjectIdGetDatum(F_RECORD_OUT); ;
	values[Anum_pg_type_typreceive - 1] = ObjectIdGetDatum(F_RECORD_RECV);
	values[Anum_pg_type_typsend - 1] = ObjectIdGetDatum(F_RECORD_SEND);
	values[Anum_pg_type_typanalyze - 1] = ObjectIdGetDatum(0);
	values[Anum_pg_type_typalign - 1] = CharGetDatum('d'); /* DOUBLE alignment */
	values[Anum_pg_type_typstorage - 1] = CharGetDatum('x'); /* EXTENDED storage */
	values[Anum_pg_type_typnotnull - 1] = BoolGetDatum(false);
	values[Anum_pg_type_typbasetype - 1] = ObjectIdGetDatum(0);
	values[Anum_pg_type_typtypmod - 1] = Int32GetDatum(-1); /* this type is not a domain */
	values[Anum_pg_type_typndims - 1] = Int32GetDatum(0);
	nulls[Anum_pg_type_typdefaultbin - 1] = true;
	nulls[Anum_pg_type_typdefault - 1] = true;

	cqContext  *pcqCtx = caql_beginscan(
			NULL,
			cql("INSERT INTO pg_type", NULL));

	HeapTuple tup = caql_form_tuple(pcqCtx, values, nulls);
	HeapTupleSetOid(tup, reltypeoid);

	caql_insert_inmem(pcqCtx, tup);
	caql_endscan(pcqCtx);
}

/*
 * LoadHCatalogDistributionPolicy
 * 		Load the metadata for an HCatalog table to gp_distribution_policy
 */
void LoadHCatalogDistributionPolicy(Oid relid, HCatalogTable *hcatalogTable)
{
	Datum		values[Natts_gp_policy];
	bool		nulls[Natts_gp_policy];
	for (int i = 0; i < Natts_gp_policy; i++)
	{
		nulls[i] = false;
		values[i] = (Datum) 0;
	}

	values[Anum_gp_policy_localoid - 1] = ObjectIdGetDatum(relid);
	nulls[Anum_gp_policy_attrnums - 1] = true; /* default distribution */

	cqContext  *pcqCtx = caql_beginscan(
			NULL,
			cql("INSERT INTO gp_distribution_policy", NULL));

	HeapTuple tup = caql_form_tuple(pcqCtx, values, nulls);

	caql_insert_inmem(pcqCtx, tup);
	caql_endscan(pcqCtx);
}

/*
 * LoadHCatalogExtTable
 * 		Load the metadata for an HCatalog table to pg_exttable
 */
void LoadHCatalogExtTable(Oid relid, HCatalogTable *hcatalogTable)
{
	Datum		values[Natts_pg_exttable];
	bool		nulls[Natts_pg_exttable];
	for (int i = 0; i < Natts_pg_exttable; i++)
	{
		nulls[i] = false;
		values[i] = (Datum) 0;
	}

	/* location - should be an array of text with one element:
	 * pxf://<ip:port/namaservice>/<hive db>.<hive table>?Profile=Hive */
	StringInfoData locationStr;
	initStringInfo(&locationStr);
	appendStringInfo(&locationStr, "pxf://%s/%s.%s?Profile=Hive",
			pxf_service_address, hcatalogTable->dbName, hcatalogTable->tableName);
	Size len = VARHDRSZ + locationStr.len;
	/* +1 leaves room for sprintf's trailing null */
	text *t = (text *) palloc(len + 1);
	SET_VARSIZE(t, len);
	sprintf((char *) VARDATA(t), "%s", locationStr.data);
	ArrayBuildState *astate = NULL;
	astate = accumArrayResult(astate, PointerGetDatum(t),
							  false, TEXTOID,
							  CurrentMemoryContext);
	pfree(locationStr.data);
	Assert(NULL != astate);
	Datum location = makeArrayResult(astate, CurrentMemoryContext);

	/* format options - should be "formatter 'pxfwritable_import'" */
	StringInfoData formatStr;
	initStringInfo(&formatStr);
	appendStringInfo(&formatStr, "formatter 'pxfwritable_import'");
	Datum format_opts = DirectFunctionCall1(textin, CStringGetDatum(formatStr.data));
	pfree(formatStr.data);

	values[Anum_pg_exttable_reloid - 1] = ObjectIdGetDatum(relid);
	values[Anum_pg_exttable_location - 1] = location;
	values[Anum_pg_exttable_fmttype - 1] = CharGetDatum('b' /* binary */);
	values[Anum_pg_exttable_fmtopts - 1] = format_opts;
	nulls[Anum_pg_exttable_command - 1] = true;
	nulls[Anum_pg_exttable_rejectlimit - 1] = true;
	nulls[Anum_pg_exttable_rejectlimittype - 1] = true;
	nulls[Anum_pg_exttable_fmterrtbl - 1] = true;
	values[Anum_pg_exttable_encoding - 1] = Int32GetDatum(pg_get_client_encoding() /* default encoding */);
	values[Anum_pg_exttable_writable - 1] = BoolGetDatum(false /* not writable */);

	cqContext  *pcqCtx = caql_beginscan(
			NULL,
			cql("INSERT INTO pg_exttable", NULL));

	HeapTuple tup = caql_form_tuple(pcqCtx, values, nulls);

	caql_insert_inmem(pcqCtx, tup);
	caql_endscan(pcqCtx);
}

/*
 * LoadHCatalogColumns
 * 		Load the column metadata for an HCatalog table to pg_attribute
 */
void LoadHCatalogColumns(Oid relid, List *columns)
{
	Assert(OidIsValid(relid));
	Assert(NULL != columns);
	
	cqContext  *pcqCtx = caql_beginscan(
			NULL,
			cql("INSERT INTO pg_attribute", NULL));
	
	ListCell *lc = NULL;
	AttrNumber attno = 1;
	foreach(lc, columns)
	{
		HCatalogColumn *hcatCol = lfirst(lc);
		Oid typeOid = 
				caql_getoid_only(
						NULL,
						NULL,
						cql("SELECT oid FROM pg_type "
							" WHERE typname = :1 and typnamespace = :2",
							CStringGetDatum(hcatCol->typeName), ObjectIdGetDatum((Oid) PG_CATALOG_NAMESPACE)));
		
		if (!OidIsValid(typeOid))
		{
			elog(ERROR, "Unsupported type %s for imported column %s", hcatCol->typeName, hcatCol->colName);
		}

		FormData_pg_attribute attributeD;
		HeapTuple attributeTuple = heap_addheader(Natts_pg_attribute,
										false,
										ATTRIBUTE_TUPLE_SIZE,
										(void *) &attributeD);
		
		Form_pg_attribute attribute = (Form_pg_attribute) GETSTRUCT(attributeTuple);
		
		int16 typlen = 0;
		bool typbyval = false;
		char typealign = 'c'; /* CHAR/no alignment by default */

		get_typlenbyvalalign(typeOid, &typlen, &typbyval, &typealign);

		attribute->attrelid = relid;
		namestrcpy(&(attribute->attname), hcatCol->colName);
		attribute->atttypid = typeOid;
		attribute->attstattarget = 0; /* no stats collection for column */
		attribute->attlen = typlen;
		attribute->attcacheoff = -1;
		attribute->atttypmod = ComputeTypeMod(typeOid, hcatCol->colName, hcatCol->typeModifiers, hcatCol->nTypeModifiers);
		attribute->attnum = attno;
		attribute->attbyval = typbyval;
		attribute->attndims = 0; /* array types not supported */
		attribute->attstorage = get_typstorage(typeOid);
		attribute->attalign = typealign;
		attribute->attnotnull = false;
		attribute->atthasdef = false;
		attribute->attisdropped = false;
		attribute->attislocal = true;
		attribute->attinhcount = 0;

		caql_insert_inmem(pcqCtx, attributeTuple);

		attno++;
	}
	
	caql_endscan(pcqCtx);

}

/*
 * get_typemod
 * 		Compute the type modifiers for a column of the given type
 */
int ComputeTypeMod(Oid typeOid, const char *colname, int *typemod, int nTypeMod)
{
	Assert(0 <= nTypeMod && nTypeMod <= maxNumTypeModifiers);
	if (0 == nTypeMod)
	{
		if (BPCHAROID == typeOid)
		{
			/* "char" without length corresponds to "char(1)" */
			return VARHDRSZ + 1;
		}
		
		return -1;
	}

	if (BPCHAROID == typeOid || VARCHAROID == typeOid)
	{
		/* typemod specifies the length */
		if (1 != nTypeMod)
		{
			elog(ERROR, "Invalid typemod for imported column %s", colname);
		}

		return VARHDRSZ + typemod[0];
	}

	if (NUMERICOID != typeOid)
	{
		/* TODO: Mar 16, 2015 - lantova: do we need to support typemod with types other than BPCHAR, VARCHAR, NUMERIC? */
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("Invalid typemod for imported column %s",
						colname)));
	}

	/* pack the scale and precision for NUMERIC into a single number to store in the typmod field in pg_attribute */
	int precision = typemod[0];
	if (1 > precision || NUMERIC_MAX_PRECISION < precision)
	{
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("Invalid typemod for imported column %s. NUMERIC precision %d must be between 1 and %d",
						colname, precision, NUMERIC_MAX_PRECISION)));
	}
	
	int result = precision << 16;
	
	if (maxNumTypeModifiers == nTypeMod)
	{
		int scale = typemod[1];
		
		if (0 > scale || precision < scale)
		{
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("Invalid typemod for imported column %s. NUMERIC scale %d must be between 0 and precision %d",
							colname, scale, precision)));
		}
		result = result | scale;
	}
	
	return VARHDRSZ + result;
}

