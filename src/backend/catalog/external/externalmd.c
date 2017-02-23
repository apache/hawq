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
 *		Utilities for loading external PXF metadata
 *
 */


#include "postgres.h"
#include <json-c/json.h>

#include "miscadmin.h"
#include "access/transam.h"
#include "catalog/catquery.h"
#include "catalog/external/externalmd.h"
#include "catalog/pg_database.h"
#include "catalog/pg_exttable.h"
#include "catalog/pg_namespace.h"
#include "catalog/namespace.h"
#include "commands/typecmds.h"
#include "utils/builtins.h"
#include "utils/fmgroids.h"
#include "utils/lsyscache.h"
#include "utils/numeric.h"
#include "utils/guc.h"


List *ParsePxfEntries(StringInfo json, char *profile, Oid dboid);
static PxfItem *ParsePxfItem(struct json_object *pxfMD, char* profile);
static void LoadPxfItem(PxfItem *pxfItem, Oid dboid);
static Oid LoadNamespace(const char *namespaceName, Oid dboid);
static void LoadTable(Oid namespaceOid, PxfItem *pxfItem);
static void LoadType(Oid relid, Oid reltypeoid, NameData relname, Oid relnamespaceoid);
static void LoadDistributionPolicy(Oid relid, PxfItem *pxfItem);
static void LoadExtTable(Oid relid, PxfItem *pxfItem);
static void LoadColumns(Oid relid, List *columns);
static int ComputeTypeMod(Oid typeOid, const char *colname, int *typemod, int nTypeMod);
static Datum GetFormatTypeForProfile(const List *outputFormats);
static Datum GetFormatOptionsForProfile(const List *outputFormats, int delimiter);
static Datum GetLocationForFormat(char *profile, List *outputFormats, char *pxf_service_address, char *path, char *name, int delimiter);

const int maxNumTypeModifiers = 2;
/*
 * Parse a json response containing PXF metadata and load it in the in-memory heap tables,
 * to database with Oid=dboid if dboid is not NULL
 * Return the list of the parsed tables
 */
List *ParsePxfEntries(StringInfo json, char *profile, Oid dboid)
{
	struct json_object *jsonObj = json_tokener_parse(json->data);
	if ((NULL == jsonObj ) || is_error(jsonObj))
	{
		return NIL;
	}
	
	List *tables = NIL;
	struct json_object *jsonItems = json_object_object_get(jsonObj, "PXFMetadata");
	if ((jsonItems == NULL) || is_error(jsonItems))
	{
		return NIL;
	}
	
	const int numItems = json_object_array_length(jsonItems);
	for (int i = 0; i < numItems; i++)
	{
		struct json_object *jsonItem = json_object_array_get_idx(jsonItems, i);
		PxfItem *pxfItem = ParsePxfItem(jsonItem, profile);
		if (dboid != InvalidOid)
			LoadPxfItem(pxfItem, dboid);
		tables = lappend(tables, pxfItem);
	}
		
	return tables;
}

/*
 * ParsePxfItem
 * 		Parse the given json object representing a single PXF item into the internal
 * 		representation
 * 		Reports error and exits if any of mandatory attributes in given json are missing
 * 		Input JSON schema:
 *
 * 		{"PXFMetadata":[{"item":{"path":"<ITEM_PATH>","name":"<ITEM_NAME>"},"fields":[{"name":"<FIELD_NAME>","type":"<FIELD_TYPE>","sourceType":"<SOURCE_TYPE>"[,"modifiers":["<MODIFIER1>","<MODIFIER2>"]]},...]}, ...]}
 *
 */
static PxfItem *ParsePxfItem(struct json_object *pxfMD, char* profile)
{
	PxfItem *pxfItem = palloc0(sizeof(PxfItem));

	/* parse item name */
	struct json_object *jsonItem = json_object_object_get(pxfMD, "item");
	if (NULL == jsonItem)
		ereport(ERROR,
			(errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
			 errmsg("Could not parse PXF item, expected not null value for attribute \"item\"")));

	struct json_object *itemPath = json_object_object_get(jsonItem, "path");
	if (NULL == itemPath)
		ereport(ERROR,
			(errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
			 errmsg("Could not parse PXF item, expected not null value for attribute \"path\"")));

	struct json_object *itemName = json_object_object_get(jsonItem, "name");
	if (NULL == itemName)
		ereport(ERROR,
			(errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
			 errmsg("Could not parse PXF item, expected not null value for attribute \"name\"")));
	pxfItem->profile = profile;
	pxfItem->path = pstrdup(json_object_get_string(itemPath));
	pxfItem->name = pstrdup(json_object_get_string(itemName));

	/* parse output formats */
	struct json_object *jsonOutputFormats = json_object_object_get(pxfMD, "outputFormats");

	if (NULL != jsonOutputFormats)
	{
		const int numOutputFormats = json_object_array_length(jsonOutputFormats);
		for (int i = 0; i < numOutputFormats; i++)
		{
			PxfField *pxfField = palloc0(sizeof(PxfField));
			struct json_object *jsonOutputFormat = json_object_array_get_idx(jsonOutputFormats, i);
			char *outupFormat = pstrdup(json_object_get_string(jsonOutputFormat));
			pxfItem->outputFormats = lappend(pxfItem->outputFormats, outupFormat);
		}
	}

	/* parse delimiter */
	struct json_object *jsonOutputParameters = json_object_object_get(pxfMD, "outputParameters");
	if (NULL != jsonOutputParameters)
	{
		struct json_object *outputParameterDelimiter = json_object_object_get(jsonOutputParameters, "DELIMITER");
		pxfItem->delimiter = atoi(pstrdup(json_object_get_string(outputParameterDelimiter)));
	}

	elog(DEBUG1, "Parsed item %s, namespace %s", itemName, itemPath);
		
	/* parse columns */
	struct json_object *jsonFields = json_object_object_get(pxfMD, "fields");

	if (NULL == jsonFields)
		ereport(ERROR,
			(errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
			 errmsg("Could not parse PXF item, expected not null value for attribute \"fields\"")));

	const int numFields = json_object_array_length(jsonFields);
	for (int i = 0; i < numFields; i++)
	{
		PxfField *pxfField = palloc0(sizeof(PxfField));
		struct json_object *jsonCol = json_object_array_get_idx(jsonFields, i);

		struct json_object *fieldName = json_object_object_get(jsonCol, "name");

		if (NULL == fieldName)
			ereport(ERROR,
				(errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
				 errmsg("Could not parse PXF item, expected not null value for attribute \"name\"")));

		pxfField->name = pstrdup(json_object_get_string(fieldName));

		struct json_object *fieldType = json_object_object_get(jsonCol, "type");

		if (NULL == fieldType)
			ereport(ERROR,
				(errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
				 errmsg("Could not parse PXF item, expected not null value for attribute \"type\"")));

		pxfField->type = pstrdup(json_object_get_string(fieldType));

		struct json_object *sourceFieldType = json_object_object_get(jsonCol, "sourceType");

		if (NULL == sourceFieldType)
			ereport(ERROR,
				(errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
				 errmsg("Could not parse PXF item, expected not null value for attribute \"sourceType\"")));

		pxfField->sourceType = pstrdup(json_object_get_string(sourceFieldType));

		pxfField->nTypeModifiers = 0;
		
		elog(DEBUG1, "Parsing field %s, type %s", pxfField->name, pxfField->type);

		struct json_object *jsonModifiers = json_object_object_get(jsonCol, "modifiers");
		if (NULL != jsonModifiers)
		{
			const int numModifiers = json_object_array_length(jsonModifiers);
			Assert(2 >= numModifiers);
			
			pxfField->nTypeModifiers = numModifiers;
			for (int j = 0; j < numModifiers; j++)
			{
				struct json_object *jsonMod = json_object_array_get_idx(jsonModifiers, j);
				pxfField->typeModifiers[j] = json_object_get_int(jsonMod);
				
				elog(DEBUG1, "modifier[%d]: %d", j, pxfField->typeModifiers[j]);
			}
		}
		pxfItem->fields = lappend(pxfItem->fields, pxfField);
	}

	return pxfItem;
}

/*
 * LoadPxfItem
 * 		Load the given PXF item into in-memory heap tables
 */
static void LoadPxfItem(PxfItem *pxfItem, Oid dboid)
{
	Oid namespaceOid = LookupNamespaceId(pxfItem->path, dboid);

	if (!OidIsValid(namespaceOid))
	{
		/* external database name has not been mapped to a namespace yet: create it */
		namespaceOid = LoadNamespace(pxfItem->path, dboid);
		elog(DEBUG1, "No namespace found: %s. Generated new namespace oid: %u", pxfItem->path, namespaceOid);
	}
	
	LoadTable(namespaceOid, pxfItem);
}

/*
 * LoadNamespace
 * 		Create an entry for the given PXF namespace in the in-memory heap tables and
 * 		return the reserved namespace oid
 */
static Oid LoadNamespace(const char *namespaceName, Oid dboid)
{
	Assert(OidIsValid(dboid));

	Oid namespaceOid = GetNewExternalObjectId();

	bool nulls[Natts_pg_namespace] = {false};
	Datum values[Natts_pg_namespace] = {(Datum) 0};
	
	NameData name;
	namestrcpy(&name, namespaceName);
	values[Anum_pg_namespace_nspname - 1] = NameGetDatum(&name);
	values[Anum_pg_namespace_nspdboid - 1] = ObjectIdGetDatum((Oid) dboid);
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
 * LoadTable
 * 		Load the metadata for an PXF table to pg_class and related catalog tables.
 */
static void LoadTable(Oid namespaceOid, PxfItem *pxfItem)
{
	/*
	 * assert entry is not already loaded in pg_class
	 */
	Oid relid = caql_getoid_only(
			NULL,
			NULL,
			cql("SELECT oid FROM pg_class "
				" WHERE relname = :1 and relnamespace = :2",
				CStringGetDatum(pxfItem->name), ObjectIdGetDatum(namespaceOid)));
	if (InvalidOid != relid)
	{

		HeapTuple	tup;
		Form_pg_namespace namespace;

		tup = caql_getfirst(
				NULL,
				cql("SELECT * FROM pg_namespace "
					" WHERE oid = :1 ",
					ObjectIdGetDatum(namespaceOid)));

		if (!HeapTupleIsValid(tup))
			elog(ERROR, "could not find tuple for namespace with oid=%u",
				namespaceOid);

		namespace = (Form_pg_namespace) GETSTRUCT(tup);

		ereport(ERROR,
				(errcode(ERRCODE_DUPLICATE_TABLE),
				 errmsg("relation \"%s.%s.%s\" already exists",
						 NameStr(namespace->nspname), pxfItem->path, pxfItem->name),
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
	namestrcpy(&name, pxfItem->name);

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
	values[Anum_pg_class_relnatts - 1] = Int16GetDatum(list_length(pxfItem->fields));
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

	LoadType(relid, reltypeoid, name, namespaceOid);
	LoadDistributionPolicy(relid, pxfItem);
	LoadExtTable(relid, pxfItem);
	LoadColumns(relid, pxfItem->fields);
}

/*
 * LoadType
 * 		Load the metadata for an PXF table to pg_type
 */
static void LoadType(Oid relid, Oid reltypeoid, NameData relname, Oid relnamespaceoid)
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
 * LoadDistributionPolicy
 * 		Load the metadata for an PXF table to gp_distribution_policy
 */
static void LoadDistributionPolicy(Oid relid, PxfItem *pxfItem)
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
 * LoadExtTable
 * 		Load the metadata for an PXF table to pg_exttable
 */
static void LoadExtTable(Oid relid, PxfItem *pxfItem)
{
	Datum		values[Natts_pg_exttable];
	bool		nulls[Natts_pg_exttable];
	for (int i = 0; i < Natts_pg_exttable; i++)
	{
		nulls[i] = false;
		values[i] = (Datum) 0;
	}

	values[Anum_pg_exttable_reloid - 1] = ObjectIdGetDatum(relid);
	values[Anum_pg_exttable_location - 1] = GetLocationForFormat(pxfItem->profile, pxfItem->outputFormats, pxf_service_address, pxfItem->path, pxfItem->name, pxfItem->delimiter);
	values[Anum_pg_exttable_fmttype - 1] = GetFormatTypeForProfile(pxfItem->outputFormats);
	values[Anum_pg_exttable_fmtopts - 1] = GetFormatOptionsForProfile(pxfItem->outputFormats, pxfItem->delimiter);
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
 * LoadPxfColumns
 * 		Load the column metadata for an PXF table to pg_attribute
 */
static void LoadColumns(Oid relid, List *columns)
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
		PxfField *field = lfirst(lc);
		Oid typeOid = 
				caql_getoid_only(
						NULL,
						NULL,
						cql("SELECT oid FROM pg_type "
							" WHERE typname = :1 and typnamespace = :2",
							CStringGetDatum(field->type), ObjectIdGetDatum((Oid) PG_CATALOG_NAMESPACE)));
		
		if (!OidIsValid(typeOid))
		{
			elog(ERROR, "Unsupported type %s for imported column %s", field->type, field->name);
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
		namestrcpy(&(attribute->attname), field->name);
		attribute->atttypid = typeOid;
		attribute->attstattarget = 0; /* no stats collection for column */
		attribute->attlen = typlen;
		attribute->attcacheoff = -1;
		attribute->atttypmod = ComputeTypeMod(typeOid, field->name, field->typeModifiers, field->nTypeModifiers);
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
static int ComputeTypeMod(Oid typeOid, const char *colname, int *typemod, int nTypeMod)
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

static Datum GetFormatTypeForProfile(const List *outputFormats)
{

	/* if table is homogeneous and output format is text - use text*/
	if (list_length(outputFormats) == 1 && strcmp(lfirst(list_head(outputFormats)), TextFormatName) == 0)
	{
		return CharGetDatum(TextFormatType);
	} else
	{
		return CharGetDatum(CustomFormatType);
	}
}

static Datum GetFormatOptionsForProfile(const List *outputFormats, int delimiter)
{
	StringInfoData formatStr;
	initStringInfo(&formatStr);

	/* "delimiter 'delimiter' null '\\N' escape '\\'"*/
	char formatArr[35] = { 0x64, 0x65, 0x6c, 0x69, 0x6d, 0x69, 0x74, 0x65,
			0x72, 0x20, 0x27, delimiter, 0x27, 0x20, 0x6e, 0x75, 0x6c, 0x6c,
			0x20, 0x27, 0x5c, 0x4e, 0x27, 0x20, 0x65, 0x73, 0x63, 0x61, 0x70,
			0x65, 0x20, 0x27, 0x5c, 0x27, 0x00 };

	if (list_length(outputFormats) == 1 && strcmp(lfirst(list_head(outputFormats)),TextFormatName) == 0)
	{
		appendStringInfo(&formatStr, "%s", formatArr);
	} else {
		appendStringInfo(&formatStr, "formatter 'pxfwritable_import'");
	}
	Datum format_opts = DirectFunctionCall1(textin, CStringGetDatum(formatStr.data));
	pfree(formatStr.data);
	return format_opts;
}

/* location - should be an array of text with one element:
 * pxf://<ip:port/namaservice>/<path>.<name>?Profile=profileName&delimiter=delimiterCode */
static Datum GetLocationForFormat(char *profile, List *outputFormats, char *pxf_service_address, char *path, char *name, int delimiter)
{
	StringInfoData locationStr;
	initStringInfo(&locationStr);
	appendStringInfo(&locationStr, "pxf://%s/%s.%s?Profile=%s", pxf_service_address, path, name, profile);
	bool hasTextOutputFormat = false;
	ListCell *lc = NULL;
	foreach (lc, outputFormats)
	{
		char *outputFormat = (char *) lfirst(lc);
		if (strcmp(outputFormat, TextFormatName) == 0)
		{
			hasTextOutputFormat = true;
			break;
		}
	}
	if (delimiter)
	{
		appendStringInfo(&locationStr, "&delimiter=%cx%02x", '\\', delimiter);
	} else if (hasTextOutputFormat)
	{
		ereport(ERROR,
			(errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
			 errmsg("delimiter attribute is mandatory for output format \"TEXT\"")));
	}
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
	return location;
}
