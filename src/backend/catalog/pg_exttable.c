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

/*-------------------------------------------------------------------------
 *
 * pg_exttable.c
 *	  routines to support manipulation of the pg_exttable relation
 *
 * Portions Copyright (c) 2009, Greenplum Inc
 * Portions Copyright (c) 1996-2006, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "catalog/pg_exttable.h"
#include "catalog/pg_type.h"
#include "catalog/pg_proc.h"
#include "access/genam.h"
#include "catalog/catquery.h"
#include "access/fileam.h"
#include "access/heapam.h"
#include "catalog/dependency.h"
#include "catalog/indexing.h"
#include "mb/pg_wchar.h"
#include "utils/array.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/syscache.h"
#include "utils/fmgroids.h"
#include "utils/uri.h"

#define MAGMA "magma"
#define ORC "orc"
/*
 * InsertExtTableEntry
 * 
 * Adds an entry into the pg_exttable catalog table. The entry
 * includes the reloid of the external relation that was created
 * in pg_class and a text array of external location URIs among
 * other external table properties.
 */
void
InsertExtTableEntry(Oid 	tbloid, 
					bool 	iswritable,
					bool 	isweb,
					bool	issreh,
					char	formattype,
					char	rejectlimittype,
					char*	commandString,
					int		rejectlimit,
					Oid		fmtErrTblOid,
					int		encoding,
					Datum	formatOptStr,
					Datum	locationExec,
					Datum	locationUris)
{
	Relation	pg_exttable_rel;
	HeapTuple	pg_exttable_tuple = NULL;
	bool		nulls[Natts_pg_exttable];
	Datum		values[Natts_pg_exttable];
	cqContext	cqc;
	cqContext  *pcqCtx;

 	MemSet(values, 0, sizeof(values));
	MemSet(nulls, false, sizeof(nulls));

    /*
     * Open and lock the pg_exttable catalog.
     */
	pg_exttable_rel = heap_open(ExtTableRelationId, RowExclusiveLock);

	pcqCtx = caql_beginscan(
			caql_addrel(cqclr(&cqc), pg_exttable_rel),
			cql("INSERT INTO pg_exttable",
				NULL));

	values[Anum_pg_exttable_reloid - 1] = ObjectIdGetDatum(tbloid);
	values[Anum_pg_exttable_fmttype - 1] = CharGetDatum(formattype);
	values[Anum_pg_exttable_fmtopts - 1] = formatOptStr;

	if(commandString)
	{
		/* EXECUTE type table - store command and command location */

		values[Anum_pg_exttable_command - 1] =
		DirectFunctionCall1(textin, CStringGetDatum(commandString));
		values[Anum_pg_exttable_location - 1] = locationExec;

	}
	else
	{
		/* LOCATION or MAGMA type table - store uri locations. command is NULL */

		values[Anum_pg_exttable_location - 1] = locationUris;
		values[Anum_pg_exttable_command - 1] = 0;
		nulls[Anum_pg_exttable_command - 1] = true;
	}

	if(issreh)
	{
		values[Anum_pg_exttable_rejectlimit -1] = Int32GetDatum(rejectlimit);
		values[Anum_pg_exttable_rejectlimittype - 1] = CharGetDatum(rejectlimittype);

		/* if error table specified store its OID, otherwise put a NULL */
		if(fmtErrTblOid != InvalidOid)
			values[Anum_pg_exttable_fmterrtbl - 1] = ObjectIdGetDatum(fmtErrTblOid);
		else
			nulls[Anum_pg_exttable_fmterrtbl - 1] = true;
	}
	else
	{
		nulls[Anum_pg_exttable_rejectlimit -1] = true;
		nulls[Anum_pg_exttable_rejectlimittype - 1] = true;
		nulls[Anum_pg_exttable_fmterrtbl - 1] = true;
	}

	values[Anum_pg_exttable_encoding - 1] = Int32GetDatum(encoding);
	values[Anum_pg_exttable_writable - 1] = BoolGetDatum(iswritable);

	pg_exttable_tuple = caql_form_tuple(pcqCtx, values, nulls);

	/* insert a new tuple */
	caql_insert(pcqCtx, pg_exttable_tuple); 
	/* and Update indexes (implicit) */

	caql_endscan(pcqCtx);

	/*
     * Close the pg_exttable relcache entry without unlocking.
     * We have updated the catalog: consequently the lock must be held until
     * end of transaction.
     */
    heap_close(pg_exttable_rel, NoLock);

}

/*
 * Get the catalog entry for an exttable relation (from pg_exttable)
 */
ExtTableEntry*
GetExtTableEntry(Oid relid)
{
	
	Relation	pg_exttable_rel;
	HeapTuple	tuple;
	cqContext	cqc;
	ExtTableEntry *extentry;
	Datum		locations,
				fmtcode, 
				fmtopts, 
				command, 
				rejectlimit, 
				rejectlimittype, 
				fmterrtbl, 
				encoding, 
				iswritable;
	bool		isNull;
	bool		locationNull = false;
	
	
	pg_exttable_rel = heap_open(ExtTableRelationId, RowExclusiveLock);

	tuple = caql_getfirst(
			caql_addrel(cqclr(&cqc), pg_exttable_rel),			
			cql("SELECT * FROM pg_exttable "
				" WHERE reloid = :1 "
				" FOR UPDATE ",
				ObjectIdGetDatum(relid)));

	if (!HeapTupleIsValid(tuple))
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_OBJECT),
				 errmsg("missing pg_exttable entry for relation \"%s\"",
						get_rel_name(relid))));


	extentry = (ExtTableEntry *) palloc0(sizeof(ExtTableEntry));
	
	/* get the location list */
	locations = heap_getattr(tuple, 
							 Anum_pg_exttable_location, 
							 RelationGetDescr(pg_exttable_rel), 
							 &isNull);

	if (isNull)
	{
		Insist(false); /* location list is always populated (url or ON X) */
	}
	else
	{
		Datum	   *elems;
		int			nelems;
		int			i;
		char*		loc_str = NULL;
		
		deconstruct_array(DatumGetArrayTypeP(locations),
						  TEXTOID, -1, false, 'i',
						  &elems, NULL, &nelems);

		for (i = 0; i < nelems; i++)
		{
			loc_str = DatumGetCString(DirectFunctionCall1(textout, elems[i]));

			/* append to a list of Value nodes, size nelems */
			extentry->locations = lappend(extentry->locations, makeString(pstrdup(loc_str)));
		}
		
		if(loc_str && (IS_FILE_URI(loc_str) || IS_GPFDIST_URI(loc_str) || IS_GPFDISTS_URI(loc_str)))
			extentry->isweb = false;
		else
			extentry->isweb = true;

	}
		
	/* get the execute command */
	command = heap_getattr(tuple, 
						   Anum_pg_exttable_command, 
						   RelationGetDescr(pg_exttable_rel), 
						   &isNull);
	
	if(isNull)
	{
		if(locationNull)
			ereport(ERROR,
					(errcode(ERRCODE_UNDEFINED_OBJECT),
					 errmsg("got invalid pg_exttable tuple. location and command are both NULL")));	
		
		extentry->command = NULL;
	}
	else
	{
		extentry->command = DatumGetCString(DirectFunctionCall1(textout, command));
	}
	

	/* get the format code */
	fmtcode = heap_getattr(tuple, 
						   Anum_pg_exttable_fmttype, 
						   RelationGetDescr(pg_exttable_rel), 
						   &isNull);
	
	Insist(!isNull);
	extentry->fmtcode = DatumGetChar(fmtcode);
	Insist(extentry->fmtcode == 'c' || extentry->fmtcode == 't' || extentry->fmtcode == 'b'
			|| extentry->fmtcode == 'p' || extentry->fmtcode == 'o');

	/* get the format options string */
	fmtopts = heap_getattr(tuple, 
						   Anum_pg_exttable_fmtopts, 
						   RelationGetDescr(pg_exttable_rel), 
						   &isNull);
	
	Insist(!isNull);
	extentry->fmtopts = DatumGetCString(DirectFunctionCall1(textout, fmtopts));
	


	/* get the reject limit */
	rejectlimit = heap_getattr(tuple, 
							   Anum_pg_exttable_rejectlimit, 
							   RelationGetDescr(pg_exttable_rel), 
							   &isNull);
	
	if(!isNull)
		extentry->rejectlimit = DatumGetInt32(rejectlimit);
	else
		extentry->rejectlimit = -1; /* mark that no SREH requested */

	/* get the reject limit type */
	rejectlimittype = heap_getattr(tuple, 
								   Anum_pg_exttable_rejectlimittype, 
								   RelationGetDescr(pg_exttable_rel), 
								   &isNull);
	
	extentry->rejectlimittype = DatumGetChar(rejectlimittype);
	if(!isNull)
		Insist(extentry->rejectlimittype == 'r' || extentry->rejectlimittype == 'p');
	else
		extentry->rejectlimittype = -1;
	
	/* get the error table oid */
	fmterrtbl = heap_getattr(tuple, 
							 Anum_pg_exttable_fmterrtbl, 
							 RelationGetDescr(pg_exttable_rel), 
							 &isNull);
    
	if(isNull)
		extentry->fmterrtbl = InvalidOid;
	else
		extentry->fmterrtbl = DatumGetObjectId(fmterrtbl);

	/* get the table encoding */
	encoding = heap_getattr(tuple, 
							Anum_pg_exttable_encoding, 
							RelationGetDescr(pg_exttable_rel), 
							&isNull);
	
	Insist(!isNull);
	extentry->encoding = DatumGetInt32(encoding);
	Insist(PG_VALID_ENCODING(extentry->encoding));

	/* get the table encoding */
	iswritable = heap_getattr(tuple, 
							  Anum_pg_exttable_writable, 
							  RelationGetDescr(pg_exttable_rel), 
							  &isNull);
	Insist(!isNull);
	extentry->iswritable = DatumGetBool(iswritable);

	
	/* Finish up scan and close pg_exttable catalog. */
	
	heap_close(pg_exttable_rel, RowExclusiveLock);
	
	return extentry;
}

/*
 * Get the catalog entry for a exttable relation tuple.
 */
ExtTableEntry *
GetExtTableEntryFromTuple(
	Relation		pg_exttable_rel,
	TupleDesc	pg_exttable_dsc,
	HeapTuple	tuple,
	Oid			*relationId)
{
	Datum		relid,
				locations,
				fmtcode,
				fmtopts,
				command,
				rejectlimit,
				rejectlimittype,
				fmterrtbl,
				encoding,
				iswritable;

	bool		isNull;
	bool		locationNull = false;

	ExtTableEntry *extentry;

	extentry = (ExtTableEntry *) palloc0(sizeof(ExtTableEntry));

	/* get the relid */
	relid = heap_getattr(tuple,
							 Anum_pg_exttable_reloid,
							 pg_exttable_dsc,
							 &isNull);

	if(isNull)
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_OBJECT),
				 errmsg("got invalid relid value: NULL")));

	/* get the location list */
	locations = heap_getattr(tuple,
							 Anum_pg_exttable_location, 
							 pg_exttable_dsc,
							 &isNull);

	if (isNull)
	{
		Insist(false); /* location list is always populated (url or ON X) */
	}
	else
	{
		Datum	   *elems;
		int			nelems;
		int			i;
		char*		loc_str = NULL;

		deconstruct_array(DatumGetArrayTypeP(locations),
						  TEXTOID, -1, false, 'i',
						  &elems, NULL, &nelems);

		for (i = 0; i < nelems; i++)
		{
			loc_str = DatumGetCString(DirectFunctionCall1(textout, elems[i]));

			/* append to a list of Value nodes, size nelems */
			extentry->locations = lappend(extentry->locations, makeString(pstrdup(loc_str)));
		}

		if(loc_str && (IS_FILE_URI(loc_str) || IS_GPFDIST_URI(loc_str) || IS_GPFDISTS_URI(loc_str)))
			extentry->isweb = false;
		else
			extentry->isweb = true;

	}

	/* get the execute command */
	command = heap_getattr(tuple,
						   Anum_pg_exttable_command,
						   pg_exttable_dsc,
						   &isNull);

	if(isNull)
	{
		if(locationNull)
			ereport(ERROR,
					(errcode(ERRCODE_UNDEFINED_OBJECT),
					 errmsg("got invalid pg_exttable tuple. location and command are both NULL")));	

		extentry->command = NULL;
	}
	else
	{
		extentry->command = DatumGetCString(DirectFunctionCall1(textout, command));
	}


	/* get the format code */
	fmtcode = heap_getattr(tuple,
						   Anum_pg_exttable_fmttype,
						   pg_exttable_dsc,
						   &isNull);

	Insist(!isNull);
	extentry->fmtcode = DatumGetChar(fmtcode);
	Insist(extentry->fmtcode == 'c' || extentry->fmtcode == 't' || extentry->fmtcode == 'b'
			|| extentry->fmtcode == 'p' || extentry->fmtcode == 'o');

	/* get the format options string */
	fmtopts = heap_getattr(tuple,
						   Anum_pg_exttable_fmtopts,
						   pg_exttable_dsc,
						   &isNull);

	Insist(!isNull);
	extentry->fmtopts = DatumGetCString(DirectFunctionCall1(textout, fmtopts));



	/* get the reject limit */
	rejectlimit = heap_getattr(tuple,
							   Anum_pg_exttable_rejectlimit,
							   pg_exttable_dsc,
							   &isNull);

	if(!isNull)
		extentry->rejectlimit = DatumGetInt32(rejectlimit);
	else
		extentry->rejectlimit = -1; /* mark that no SREH requested */

	/* get the reject limit type */
	rejectlimittype = heap_getattr(tuple,
								   Anum_pg_exttable_rejectlimittype,
								   pg_exttable_dsc,
								   &isNull);

	extentry->rejectlimittype = DatumGetChar(rejectlimittype);
	if(!isNull)
		Insist(extentry->rejectlimittype == 'r' || extentry->rejectlimittype == 'p');
	else
		extentry->rejectlimittype = -1;

	/* get the error table oid */
	fmterrtbl = heap_getattr(tuple,
							 Anum_pg_exttable_fmterrtbl,
							 pg_exttable_dsc,
							 &isNull);

	if(isNull)
		extentry->fmterrtbl = InvalidOid;
	else
		extentry->fmterrtbl = DatumGetObjectId(fmterrtbl);

	/* get the table encoding */
	encoding = heap_getattr(tuple,
							Anum_pg_exttable_encoding,
							pg_exttable_dsc,
							&isNull);

	Insist(!isNull);
	extentry->encoding = DatumGetInt32(encoding);
	Insist(PG_VALID_ENCODING(extentry->encoding));

	/* get the table encoding */
	iswritable = heap_getattr(tuple,
							  Anum_pg_exttable_writable,
							  pg_exttable_dsc,
							  &isNull);
	Insist(!isNull);
	extentry->iswritable = DatumGetBool(iswritable);

    *relationId = DatumGetObjectId(relid);

	return extentry;
}

/*
 * RemoveExtTableEntry
 * 
 * Remove an external table entry from pg_exttable. Caller's 
 * responsibility to ensure that the relation has such an entry.
 */
void
RemoveExtTableEntry(Oid relid)
{
	Relation	pg_exttable_rel;
	cqContext	cqc;

	/*
	 * now remove the pg_exttable entry
	 */
	pg_exttable_rel = heap_open(ExtTableRelationId, RowExclusiveLock);

	if (0 == caql_getcount(
				caql_addrel(cqclr(&cqc), pg_exttable_rel),
				cql("DELETE FROM pg_exttable "
					" WHERE reloid = :1 ",
					ObjectIdGetDatum(relid))))
	{
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_OBJECT),
				 errmsg("external table object id \"%d\" does not exist",
						relid)));
	}

	/*
	 * Delete the external table entry from the catalog (pg_exttable).
	 */

	/* Finish up scan and close exttable catalog. */

	heap_close(pg_exttable_rel, NoLock);
}

bool RelationIsPluggableStorage(Oid relid) {
	bool result;
	ExtTableEntry *extEntry = GetExtTableEntry(relid);
	result = fmttype_is_custom(extEntry->fmtcode);
	pfree(extEntry);
	return result;
}

bool RelationIsMagmaTable(Oid relid) {
	bool result;
	ExtTableEntry *extEntry = GetExtTableEntry(relid);
	List *entry_locations = extEntry->locations;
	Assert(entry_locations);
	ListCell *entry_location = list_head(entry_locations);
	char *url = ((Value*)lfirst(entry_location))->val.str;
	result = IS_MAGMA_URI(url);
	pfree(extEntry);
	return result;
}

bool RelationIsORCTable(Oid relid) {
	char *formatOpt = caql_getcstring(
	    NULL,
	    cql("SELECT fmtopts FROM pg_exttable WHERE reloid = :1",
	    ObjectIdGetDatum(relid)));
	if (formatOpt) {
		char *formatName = getExtTblFormatterTypeInFmtOptsStr(formatOpt);
		if (formatName) {
			if (pg_strncasecmp(formatName, ORC, sizeof(ORC)-1) == 0) {
				pfree(formatName);
				return true;
			} else {
				pfree(formatName);
				return false;
			}
		} else {
			return false;
		}
	} else {
		return false;
	}

}

bool RelationIsMagmaTable2(Oid relid) {
	char *formatOpt = caql_getcstring(
	    NULL,
	    cql("SELECT fmtopts FROM pg_exttable WHERE reloid = :1",
	    ObjectIdGetDatum(relid)));
	if (formatOpt) {
		char *formatName = getExtTblFormatterTypeInFmtOptsStr(formatOpt);
		if (formatName) {
			if (pg_strncasecmp(formatName, MAGMA, sizeof(MAGMA)-1) == 0) {
				pfree(formatName);
				return true;
			} else {
				pfree(formatName);
				return false;
			}
		} else {
			return false;
		}
	} else {
		return false;
	}
}
