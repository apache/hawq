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
* pg_appendonly.c
*	  routines to support manipulation of the pg_appendonly relation
*
* Portions Copyright (c) 2008, Greenplum Inc
* Portions Copyright (c) 1996-2006, PostgreSQL Global Development Group
* Portions Copyright (c) 1994, Regents of the University of California
*
*-------------------------------------------------------------------------
*/

#include "postgres.h"

#include "catalog/pg_appendonly.h"
#include "catalog/pg_type.h"
#include "catalog/pg_proc.h"
#include "access/genam.h"
#include "access/heapam.h"
#include "catalog/catquery.h"
#include "catalog/dependency.h"
#include "catalog/indexing.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/syscache.h"
#include "utils/fmgroids.h"
#include "utils/guc.h"
#include "cdb/cdbpersistentfilesysobj.h"
#include "cdb/cdbvars.h"

HeapTuple
CreateAppendOnlyEntry(TupleDesc desp,
					  Oid relid,
		  	  	  	  int blocksize,
		  	  	  	  int pagesize,
		  	  	  	  int splitsize,
		  	  	  	  int safefswritesize,
		  	  	  	  int compresslevel,
		  	  	  	  int majorversion,
		  	  	  	  int minorversion,
		  	  	  	  bool checksum,
		  	  	  	  bool columnstore,
		  	  	  	  char* compresstype,
		  	  	  	  Oid segrelid,
		  	  	  	  Oid segidxid,
		  	  	  	  Oid blkdirrelid,
		  	  	  	  Oid blkdiridxid)
{
	Datum	   *values;
	bool	   *nulls;

	HeapTuple	pg_appendonly_tuple = NULL;

	int			natts = Natts_pg_appendonly;

	values = palloc0(sizeof(Datum) * natts);
	nulls = palloc0(sizeof(bool) * natts);


	values[Anum_pg_appendonly_relid - 1] = ObjectIdGetDatum(relid);
	values[Anum_pg_appendonly_blocksize - 1] = Int32GetDatum(blocksize);
	values[Anum_pg_appendonly_pagesize- 1] = Int32GetDatum(pagesize);
	values[Anum_pg_appendonly_splitsize - 1] = Int32GetDatum(splitsize);
	values[Anum_pg_appendonly_safefswritesize - 1] = Int32GetDatum(
			safefswritesize);
	values[Anum_pg_appendonly_compresslevel - 1] = Int32GetDatum(compresslevel);
	values[Anum_pg_appendonly_majorversion - 1] = Int32GetDatum(majorversion);
	values[Anum_pg_appendonly_minorversion - 1] = Int32GetDatum(minorversion);
	values[Anum_pg_appendonly_checksum - 1] = BoolGetDatum(checksum);
	values[Anum_pg_appendonly_columnstore - 1] = BoolGetDatum(columnstore);
	values[Anum_pg_appendonly_segrelid - 1] = ObjectIdGetDatum(segrelid);
	values[Anum_pg_appendonly_segidxid - 1] = ObjectIdGetDatum(segidxid);
	values[Anum_pg_appendonly_blkdirrelid - 1] = ObjectIdGetDatum(blkdirrelid);
	values[Anum_pg_appendonly_blkdiridxid - 1] = ObjectIdGetDatum(blkdiridxid);
	values[Anum_pg_appendonly_version - 1] = test_appendonly_version_default;

	if (compresstype)
	{
		/*
		 * check compresstype string length.
		 *
		 * when compresstype column was added to pg_appendonly (release 3.3) we
		 * skipped the creation of a toast table even though it's normally
		 * required for tables with text columns. In this case it's not
		 * necessary because compresstype is always very short (or NULL) and we
		 * never expect it to be toasted. so, for sanity, make sure that it's
		 * indeed very short. otherwise, it's an internal error.
		 */
		Insist(strlen(compresstype) < 100);

		values[Anum_pg_appendonly_compresstype - 1] = DirectFunctionCall1(
				textin, CStringGetDatum(compresstype));

	}
	else
	{
		Assert(compresslevel == 0);
		nulls[Anum_pg_appendonly_compresstype - 1] = true;
	}

	/*
	 * form the tuple and insert it
	 */
	pg_appendonly_tuple = heaptuple_form_to(desp, values, nulls, NULL, NULL );

	pfree(values);
	pfree(nulls);

	return pg_appendonly_tuple;
}


/*
 * Adds an entry into the pg_appendonly catalog table. The entry
 * includes the new relfilenode of the appendonly relation that 
 * was just created and an initial eof and reltuples values of 0
 */
void
InsertAppendOnlyEntry(Oid relid, 
					  int blocksize, 
					  int pagesize,
					  int splitsize,
					  int safefswritesize, 
					  int compresslevel,
					  int majorversion,
					  int minorversion,
					  bool checksum,
                      bool columnstore,
					  char* compresstype,
					  Oid segrelid,
					  Oid segidxid,
					  Oid blkdirrelid,
					  Oid blkdiridxid)
{
	Relation	pg_appendonly_rel;
	HeapTuple	pg_appendonly_tuple = NULL;
	cqContext	cqc;
	cqContext  *pcqCtx;

    /*
     * Open and lock the pg_appendonly catalog.
     */
	pg_appendonly_rel = heap_open(AppendOnlyRelationId, RowExclusiveLock);
	pcqCtx = caql_beginscan(
			caql_addrel(cqclr(&cqc), pg_appendonly_rel),
			cql("INSERT INTO pg_appendonly",
				NULL));

	pg_appendonly_tuple = CreateAppendOnlyEntry(RelationGetDescr(pg_appendonly_rel),
												relid,
												blocksize,
												pagesize,
												splitsize,
												safefswritesize,
												compresslevel,
												majorversion,
												minorversion,
												checksum,
												columnstore,
												compresstype,
												segrelid,
												segidxid,
												blkdirrelid,
												blkdiridxid);

	/* insert a new tuple */
	caql_insert(pcqCtx, pg_appendonly_tuple); 
	/* and Update indexes (implicit) */

	caql_endscan(pcqCtx);
	
	/*
     * Close the pg_appendonly_rel relcache entry without unlocking.
     * We have updated the catalog: consequently the lock must be 
	 * held until end of transaction.
     */
    heap_close(pg_appendonly_rel, NoLock);

    heap_freetuple(pg_appendonly_tuple);
}

/*
 * Get the OIDs of the auxiliary relations and their indexes for an appendonly
 * relation.
 *
 * The OIDs will be retrieved only when the corresponding output variable is
 * not NULL.
 */
void
GetAppendOnlyEntryAuxOids(Oid relid,
						  Snapshot appendOnlyMetaDataSnapshot,
						  Oid *segrelid,
						  Oid *segidxid,
						  Oid *blkdirrelid,
						  Oid *blkdiridxid)
{
	HeapTuple	tuple;
	cqContext	cqc;
	cqContext  *pcqCtx;
	Datum auxOid;
	bool isNull;
	
	/*
	 * Check the pg_appendonly relation to be certain the ao table 
	 * is there. 
	 */
	pcqCtx = caql_beginscan(
			caql_snapshot(cqclr(&cqc), appendOnlyMetaDataSnapshot),
			cql("SELECT * FROM pg_appendonly "
				" WHERE relid = :1 ",
				ObjectIdGetDatum(relid)));

	tuple = caql_getnext(pcqCtx);

	if (!HeapTupleIsValid(tuple))
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_OBJECT),
				 errmsg("missing pg_appendonly entry for relation \"%s\"",
						get_rel_name(relid))));
	
	if (segrelid != NULL)
	{
		auxOid = caql_getattr(pcqCtx,
							  Anum_pg_appendonly_segrelid,
							  &isNull);
		Assert(!isNull);
		if(isNull)
			ereport(ERROR,
					(errcode(ERRCODE_UNDEFINED_OBJECT),
					 errmsg("got invalid segrelid value: NULL")));	
		
		*segrelid = DatumGetObjectId(auxOid);
	}
	
	if (segidxid != NULL)
	{
		auxOid = caql_getattr(pcqCtx,
							  Anum_pg_appendonly_segidxid,
							  &isNull);
		Assert(!isNull);
		if(isNull)
			ereport(ERROR,
					(errcode(ERRCODE_UNDEFINED_OBJECT),
					 errmsg("got invalid segidxid value: NULL")));	
		
		*segidxid = DatumGetObjectId(auxOid);
	}
	
	if (blkdirrelid != NULL)
	{
		auxOid = caql_getattr(pcqCtx,
							  Anum_pg_appendonly_blkdirrelid,
							  &isNull);
		Assert(!isNull);
		if(isNull)
			ereport(ERROR,
					(errcode(ERRCODE_UNDEFINED_OBJECT),
					 errmsg("got invalid blkdirrelid value: NULL")));	
		
		*blkdirrelid = DatumGetObjectId(auxOid);
	}

	if (blkdiridxid != NULL)
	{
		auxOid = caql_getattr(pcqCtx,
							  Anum_pg_appendonly_blkdiridxid,
							  &isNull);
		Assert(!isNull);
		if(isNull)
			ereport(ERROR,
					(errcode(ERRCODE_UNDEFINED_OBJECT),
					 errmsg("got invalid blkdiridxid value: NULL")));	
		
		*blkdiridxid = DatumGetObjectId(auxOid);
	}

	/* Finish up scan and close pg_appendonly catalog. */
	caql_endscan(pcqCtx);
}

/*
 * Get the catalog entry for an appendonly relation
 */
AppendOnlyEntry *
GetAppendOnlyEntry(Oid relid, Snapshot appendOnlyMetaDataSnapshot)
{
	
	Relation	pg_appendonly_rel;
	TupleDesc	pg_appendonly_dsc;
	HeapTuple	tuple;
	cqContext	cqc;
	cqContext  *pcqCtx;

	AppendOnlyEntry *aoentry;

	Oid			relationId;
	
	/*
	 * Check the pg_appendonly relation to be certain the ao table 
	 * is there. 
	 */
	pg_appendonly_rel = heap_open(AppendOnlyRelationId, RowExclusiveLock);
	pg_appendonly_dsc = RelationGetDescr(pg_appendonly_rel);
	
	pcqCtx = caql_beginscan(
			caql_snapshot(caql_addrel(cqclr(&cqc), pg_appendonly_rel), 
						  appendOnlyMetaDataSnapshot),				
			cql("SELECT * FROM pg_appendonly "
				" WHERE relid = :1 "
				" FOR UPDATE ",
				ObjectIdGetDatum(relid)));

	tuple = caql_getnext(pcqCtx);

	if (!HeapTupleIsValid(tuple))
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_OBJECT),
				 errmsg("missing pg_appendonly entry for relation \"%s\"",
						get_rel_name(relid))));

	aoentry = GetAppendOnlyEntryFromTuple(
								pg_appendonly_rel,
								pg_appendonly_dsc,
								tuple,
								&relationId);
	Assert(relid == relationId);
									
	/* Finish up scan and close pg_appendonly catalog. */
	caql_endscan(pcqCtx);
	heap_close(pg_appendonly_rel, RowExclusiveLock);

	return aoentry;
}

/*
 * Get the catalog entry for an appendonly relation tuple.
 */
AppendOnlyEntry *
GetAppendOnlyEntryFromTuple(
	Relation	pg_appendonly_rel,
	TupleDesc	pg_appendonly_dsc,
	HeapTuple	tuple,
	Oid			*relationId)
{
	Datum		relid,
				blocksize,
				pagesize,
				splitsize,
				safefswritesize,
				compresslevel,
				compresstype,
				majorversion,
				minorversion,
                checksum,
				columnstore,
				segrelid,
				segidxid,
				blkdirrelid,
				blkdiridxid,
				version
				;

	bool		isNull;
	AppendOnlyEntry *aoentry;

	aoentry = (AppendOnlyEntry *) palloc(sizeof(AppendOnlyEntry));

	/* get the relid */
	relid = heap_getattr(tuple, 
							 Anum_pg_appendonly_relid, 
							 pg_appendonly_dsc, 
							 &isNull);
	
	if(isNull)
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_OBJECT),
				 errmsg("got invalid relid value: NULL")));	
	
	/* get the blocksize */
	blocksize = heap_getattr(tuple, 
							 Anum_pg_appendonly_blocksize, 
							 pg_appendonly_dsc, 
							 &isNull);
	
	if(isNull)
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_OBJECT),
				 errmsg("got invalid blocksize value: NULL")));	
	
	/* get the pagesize */
	pagesize = heap_getattr(tuple,
							 Anum_pg_appendonly_pagesize,
							 pg_appendonly_dsc,
							 &isNull);
	if(isNull)
			ereport(ERROR,
					(errcode(ERRCODE_UNDEFINED_OBJECT),
					 errmsg("got invalid pagesize value: NULL")));

	/* get the splitsize */
	splitsize = heap_getattr(tuple,
							Anum_pg_appendonly_splitsize,
							pg_appendonly_dsc,
							&isNull);
    if(isNull)
    {
        if (gp_upgrade_mode && (Gp_role == GP_ROLE_DISPATCH || Gp_role == GP_ROLE_UTILITY)){
            splitsize = 67108864; //64M
        }else{
			ereport(ERROR,
					(errcode(ERRCODE_UNDEFINED_OBJECT),
					errmsg("got invalid splitsize value: NULL")));
        }
    }
	/* get the safefswritesize */
	safefswritesize = heap_getattr(tuple, 
								   Anum_pg_appendonly_safefswritesize, 
								   pg_appendonly_dsc, 
								   &isNull);
	
	if(isNull)
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_OBJECT),
				 errmsg("got invalid safe fs write size value: NULL")));	
	
	/* get the compresslevel */
	compresslevel = heap_getattr(tuple, 
								 Anum_pg_appendonly_compresslevel, 
								 pg_appendonly_dsc, 
								 &isNull);
	
	if(isNull)
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_OBJECT),
				 errmsg("got invalid compresslevel value: NULL")));	

	/* get the majorversion */
	majorversion = heap_getattr(tuple, 
								 Anum_pg_appendonly_majorversion, 
								 pg_appendonly_dsc, 
								 &isNull);
	
	if(isNull)
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_OBJECT),
				 errmsg("got invalid major version value: NULL")));	

	/* get the minorversion */
	minorversion = heap_getattr(tuple, 
								Anum_pg_appendonly_minorversion, 
								pg_appendonly_dsc, 
								&isNull);
	
	if(isNull)
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_OBJECT),
				 errmsg("got invalid minor version value: NULL")));	
	
	checksum = heap_getattr(tuple, 
							Anum_pg_appendonly_checksum, 
							pg_appendonly_dsc, 
							&isNull);
	
	if(isNull)
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_OBJECT),
				 errmsg("got invalid checksum value: NULL")));	

	columnstore = heap_getattr(tuple, 
							Anum_pg_appendonly_columnstore, 
							pg_appendonly_dsc, 
							&isNull);
    Assert(!isNull);
	if(isNull)
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_OBJECT),
				 errmsg("got invalid columnstore value: NULL")));	

	/* get the compressor type */
	compresstype = heap_getattr(tuple, 
								Anum_pg_appendonly_compresstype, 
								pg_appendonly_dsc, 
								&isNull);
	
	if(isNull)
	{
		Assert(DatumGetInt16(compresslevel) == 0);
		aoentry->compresstype = NULL;
	}
	else
		aoentry->compresstype = DatumGetCString(DirectFunctionCall1(textout, compresstype));
	
    segrelid = heap_getattr(tuple, 
							Anum_pg_appendonly_segrelid,
							pg_appendonly_dsc, 
							&isNull);
    Assert(!isNull);
	if(isNull)
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_OBJECT),
				 errmsg("got invalid segrelid value: NULL")));	

    segidxid = heap_getattr(tuple, 
							Anum_pg_appendonly_segidxid,
							pg_appendonly_dsc, 
							&isNull);
    Assert(!isNull);
	if(isNull)
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_OBJECT),
				 errmsg("got invalid segidxid value: NULL")));	

    blkdirrelid = heap_getattr(tuple, 
							   Anum_pg_appendonly_blkdirrelid,
							   pg_appendonly_dsc, 
							   &isNull);
    Assert(!isNull);
	if(isNull)
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_OBJECT),
				 errmsg("got invalid blkdirrelid value: NULL")));	

    blkdiridxid = heap_getattr(tuple, 
							   Anum_pg_appendonly_blkdiridxid,
							   pg_appendonly_dsc, 
							   &isNull);
    Assert(!isNull);
	if(isNull)
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_OBJECT),
				 errmsg("got invalid blkdiridxid value: NULL")));	

	/* get the version */
	version = heap_getattr(tuple,
						   Anum_pg_appendonly_version,
						   pg_appendonly_dsc,
						   &isNull);

	if(isNull)
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_OBJECT),
				 errmsg("got invalid version value: NULL")));

	aoentry->blocksize = DatumGetInt32(blocksize);
	aoentry->pagesize = DatumGetInt32(pagesize);
	aoentry->splitsize = DatumGetInt32(splitsize);
	aoentry->safefswritesize = DatumGetInt32(safefswritesize);
	aoentry->compresslevel = DatumGetInt16(compresslevel);
	aoentry->majorversion = DatumGetInt16(majorversion);
	aoentry->minorversion = DatumGetInt16(minorversion);
	aoentry->checksum = DatumGetBool(checksum);
    aoentry->columnstore = DatumGetBool(columnstore);
    aoentry->segrelid = DatumGetObjectId(segrelid);
    aoentry->segidxid = DatumGetObjectId(segidxid);
    aoentry->blkdirrelid = DatumGetObjectId(blkdirrelid);
    aoentry->blkdiridxid = DatumGetObjectId(blkdiridxid);
    aoentry->version = DatumGetInt32(version);

    AORelationVersion_CheckValid(aoentry->version);

    *relationId = DatumGetObjectId(relid);
	
	return aoentry;
}

/*
 * Update the segrelid and/or blkdirrelid if the input new values
 * are valid OIDs.
 */
void
UpdateAppendOnlyEntryAuxOids(Oid relid,
							 Oid newSegrelid,
							 Oid newSegidxid,
							 Oid newBlkdirrelid,
							 Oid newBlkdiridxid)
{
	HeapTuple	tuple, newTuple;
	cqContext  *pcqCtx;
	Datum		newValues[Natts_pg_appendonly];
	bool		newNulls[Natts_pg_appendonly];
	bool		replace[Natts_pg_appendonly];
	
	/*
	 * Check the pg_appendonly relation to be certain the ao table 
	 * is there. 
	 */
	pcqCtx = caql_beginscan(
			NULL,
			cql("SELECT * FROM pg_appendonly "
				" WHERE relid = :1 "
				" FOR UPDATE ",
				ObjectIdGetDatum(relid)));

	tuple = caql_getnext(pcqCtx);

	if (!HeapTupleIsValid(tuple))
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_OBJECT),
				 errmsg("missing pg_appendonly entry for relation \"%s\"",
						get_rel_name(relid))));
	
	MemSet(newValues, 0, sizeof(newValues));
	MemSet(newNulls, false, sizeof(newNulls));
	MemSet(replace, false, sizeof(replace));

	if (OidIsValid(newSegrelid))
	{
		replace[Anum_pg_appendonly_segrelid - 1] = true;
		newValues[Anum_pg_appendonly_segrelid - 1] = newSegrelid;
	}
	
	if (OidIsValid(newSegidxid))
	{
		replace[Anum_pg_appendonly_segidxid - 1] = true;
		newValues[Anum_pg_appendonly_segidxid - 1] = newSegidxid;
	}
	
	if (OidIsValid(newBlkdirrelid))
	{
		replace[Anum_pg_appendonly_blkdirrelid - 1] = true;
		newValues[Anum_pg_appendonly_blkdirrelid - 1] = newBlkdirrelid;
	}
	
	if (OidIsValid(newBlkdiridxid))
	{
		replace[Anum_pg_appendonly_blkdiridxid - 1] = true;
		newValues[Anum_pg_appendonly_blkdiridxid - 1] = newBlkdiridxid;
	}
	
	newTuple = caql_modify_current(pcqCtx,
								   newValues, newNulls, replace);
	caql_update_current(pcqCtx, newTuple);
	/* and Update indexes (implicit) */

	heap_freetuple(newTuple);
	
	/* Finish up scan and close appendonly catalog. */
	caql_endscan(pcqCtx);

}

/*
 * Remove all pg_appendonly entries that the table we are DROPing
 * refers to (using the table's relfilenode)
 *
 * The gp_fastsequence entries associate with the table is also
 * deleted here.
 */
void
RemoveAppendonlyEntry(Oid relid)
{
	Relation	pg_appendonly_rel;
	HeapTuple	tuple;
	cqContext	cqc;
	cqContext  *pcqCtx;
	Oid aosegrelid = InvalidOid;
	
	/*
	 * now remove the pg_appendonly entry 
	 */
	pg_appendonly_rel = heap_open(AppendOnlyRelationId, RowExclusiveLock);
	
	pcqCtx = caql_beginscan(
			caql_addrel(cqclr(&cqc), pg_appendonly_rel),
			cql("SELECT * FROM pg_appendonly "
				" WHERE relid = :1 "
				" FOR UPDATE ",
				ObjectIdGetDatum(relid)));

	tuple = caql_getnext(pcqCtx);

	if (!HeapTupleIsValid(tuple))
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_OBJECT),
				 errmsg("appendonly table relid \"%d\" does not exist in "
						"pg_appendonly", relid)));
	
	{
		bool isNull;
		Datum datum = caql_getattr(pcqCtx, 
								   Anum_pg_appendonly_segrelid,
								   &isNull);

		Assert(!isNull);
		aosegrelid = DatumGetObjectId(datum);
		Assert(OidIsValid(aosegrelid));
	}

	/*
	 * Delete the appendonly table entry from the catalog (pg_appendonly).
	 */
	caql_delete_current(pcqCtx);
	
	
	/* Finish up scan and close appendonly catalog. */
	caql_endscan(pcqCtx);
	heap_close(pg_appendonly_rel, NoLock);
}

static void
TransferDependencyLink(
	Oid baseOid, 
	Oid oid,
	const char *tabletype)
{
	ObjectAddress 	baseobject;
	ObjectAddress	newobject;
	long			count;

	MemSet(&baseobject, 0, sizeof(ObjectAddress));
	MemSet(&newobject, 0, sizeof(ObjectAddress));

	Assert(OidIsValid(baseOid));
	Assert(OidIsValid(oid));

	/* Delete old dependency */
	count = deleteDependencyRecordsFor(RelationRelationId, oid);
	if (count != 1)
		elog(LOG, "expected one dependency record for %s table, oid %u, found %ld",
			 tabletype, oid, count);

	/* Register new dependencies */
	baseobject.classId = RelationRelationId;
	baseobject.objectId = baseOid;
	newobject.classId = RelationRelationId;
	newobject.objectId = oid;
	
	recordDependencyOn(&newobject, &baseobject, DEPENDENCY_INTERNAL);
}

static HeapTuple 
GetAppendEntryForMove(
	Relation	pg_appendonly_rel,

	TupleDesc	pg_appendonly_dsc,

	Oid 		relId,

	Oid 		*aosegrelid,
	
	Oid 		*aoblkdirrelid)
{
	HeapTuple	tuple;
	cqContext	cqc;
	cqContext  *pcqCtx;
	bool		isNull;

	pcqCtx = caql_addrel(cqclr(&cqc), pg_appendonly_rel);

	tuple = caql_getfirst(
			pcqCtx,
			cql("SELECT * FROM pg_appendonly "
				" WHERE relid = :1 ",
				ObjectIdGetDatum(relId)));

	if (!HeapTupleIsValid(tuple))
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_OBJECT),
				 errmsg("appendonly table relid \"%d\" does not exist in "
						"pg_appendonly", relId)));

    *aosegrelid = caql_getattr(pcqCtx,
							   Anum_pg_appendonly_segrelid,
							   &isNull);
    Assert(!isNull);
	if(isNull)
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_OBJECT),
				 errmsg("got invalid segrelid value: NULL")));	

    *aoblkdirrelid = caql_getattr(pcqCtx,
								  Anum_pg_appendonly_blkdirrelid,
								  &isNull);
    Assert(!isNull);
	if(isNull)
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_OBJECT),
				 errmsg("got invalid blkdirrelid value: NULL")));	

	/* tupleCopy = heap_copytuple(tuple); XXX XXX already a copy */

	/* Finish up scan and close appendonly catalog. */

	return tuple;
}

/*
 * Transfer pg_appendonly entry from one table to another.
 */
void
TransferAppendonlyEntry(Oid sourceRelId, Oid targetRelId)
{
	Relation	pg_appendonly_rel;
	TupleDesc	pg_appendonly_dsc;
	HeapTuple	tuple;
	HeapTuple	tupleCopy;
	Datum 		*newValues;
	bool 		*newNulls;
	bool 		*replace;
	Oid			aosegrelid;
	Oid			aoblkdirrelid;
	
	/*
	 * Fetch the pg_appendonly entry 
	 */
	pg_appendonly_rel = heap_open(AppendOnlyRelationId, RowExclusiveLock);
	pg_appendonly_dsc = RelationGetDescr(pg_appendonly_rel);
	
	tuple = GetAppendEntryForMove(
							pg_appendonly_rel,
							pg_appendonly_dsc,
							sourceRelId,
							&aosegrelid,
							&aoblkdirrelid);

	/* Since gp_fastsequence entry is referenced by aosegrelid, it rides along  */

	/*
	 * Delete the appendonly table entry from the catalog (pg_appendonly).
	 */
	tupleCopy = heap_copytuple(tuple);
	simple_heap_delete(pg_appendonly_rel, &tuple->t_self);
	
	newValues = palloc0(pg_appendonly_dsc->natts * sizeof(Datum));
	newNulls = palloc0(pg_appendonly_dsc->natts * sizeof(bool));
	replace = palloc0(pg_appendonly_dsc->natts * sizeof(bool));

	replace[Anum_pg_appendonly_relid - 1] = true;
	newValues[Anum_pg_appendonly_relid - 1] = targetRelId;

	tupleCopy = heap_modify_tuple(tupleCopy, pg_appendonly_dsc,
								  newValues, newNulls, replace);

	simple_heap_insert(pg_appendonly_rel, tupleCopy);
	CatalogUpdateIndexes(pg_appendonly_rel, tupleCopy);

	heap_freetuple(tupleCopy);

	heap_close(pg_appendonly_rel, NoLock);

	pfree(newValues);
	pfree(newNulls);
	pfree(replace);

	if (OidIsValid(aosegrelid))
	{
		TransferDependencyLink(targetRelId, aosegrelid, "aoseg");
	}

	if (OidIsValid(aoblkdirrelid))
	{
		TransferDependencyLink(targetRelId, aoblkdirrelid, "aoblkdir");
	}

	if (Debug_persistent_print)
		elog(Persistent_DebugPrintLevel(), 
			 "TransferAppendonlyEntry: source relation id %u, target relation id %u, aosegrelid %u, aoblkdirrelid %u",
			 sourceRelId,
			 targetRelId,
			 aosegrelid,
			 aoblkdirrelid);
}

/*
 * Swap pg_appendonly entries between tables.
 */
void
SwapAppendonlyEntries(Oid entryRelId1, Oid entryRelId2)
{
	Relation	pg_appendonly_rel;
	TupleDesc	pg_appendonly_dsc;
	HeapTuple	tuple;
	HeapTuple	tupleCopy1;
	HeapTuple	tupleCopy2;
	Datum 		*newValues;
	bool 		*newNulls;
	bool 		*replace;
	Oid			aosegrelid1;
	Oid			aoblkdirrelid1;
	Oid			aosegrelid2;
	Oid			aoblkdirrelid2;

	pg_appendonly_rel = heap_open(AppendOnlyRelationId, RowExclusiveLock);
	pg_appendonly_dsc = RelationGetDescr(pg_appendonly_rel);
	
	tuple = GetAppendEntryForMove(
							pg_appendonly_rel,
							pg_appendonly_dsc,
							entryRelId1,
							&aosegrelid1,
							&aoblkdirrelid1);

	/* Since gp_fastsequence entry is referenced by aosegrelid, it rides along  */

	/*
	 * Delete the appendonly table entry from the catalog (pg_appendonly).
	 */
	tupleCopy1 = heap_copytuple(tuple);
	simple_heap_delete(pg_appendonly_rel, &tuple->t_self);
	
	tuple = GetAppendEntryForMove(
							pg_appendonly_rel,
							pg_appendonly_dsc,
							entryRelId2,
							&aosegrelid2,
							&aoblkdirrelid2);

	/* Since gp_fastsequence entry is referenced by aosegrelid, it rides along  */

	/*
	 * Delete the appendonly table entry from the catalog (pg_appendonly).
	 */
	tupleCopy2 = heap_copytuple(tuple);
	simple_heap_delete(pg_appendonly_rel, &tuple->t_self);

	/*
	 * (Re)insert.
	 */
	newValues = palloc0(pg_appendonly_dsc->natts * sizeof(Datum));
	newNulls = palloc0(pg_appendonly_dsc->natts * sizeof(bool));
	replace = palloc0(pg_appendonly_dsc->natts * sizeof(bool));

	replace[Anum_pg_appendonly_relid - 1] = true;
	newValues[Anum_pg_appendonly_relid - 1] = entryRelId2;

	tupleCopy1 = heap_modify_tuple(tupleCopy1, pg_appendonly_dsc,
								  newValues, newNulls, replace);

	simple_heap_insert(pg_appendonly_rel, tupleCopy1);
	CatalogUpdateIndexes(pg_appendonly_rel, tupleCopy1);

	heap_freetuple(tupleCopy1);

	newValues[Anum_pg_appendonly_relid - 1] = entryRelId1;

	tupleCopy2 = heap_modify_tuple(tupleCopy2, pg_appendonly_dsc,
								  newValues, newNulls, replace);

	simple_heap_insert(pg_appendonly_rel, tupleCopy2);
	CatalogUpdateIndexes(pg_appendonly_rel, tupleCopy2);

	heap_freetuple(tupleCopy2);

	heap_close(pg_appendonly_rel, NoLock);

	pfree(newValues);
	pfree(newNulls);
	pfree(replace);

	if ((aosegrelid1 || aosegrelid2) && (aosegrelid1 != aosegrelid2))
	{
		if (OidIsValid(aosegrelid1))
		{
			TransferDependencyLink(entryRelId2, aosegrelid1, "aoseg");
		}
		if (OidIsValid(aosegrelid2))
		{
			TransferDependencyLink(entryRelId1, aosegrelid2, "aoseg");
		}
	}
	
	if ((aoblkdirrelid1 || aoblkdirrelid2) && (aoblkdirrelid1 != aoblkdirrelid2))
	{
		if (OidIsValid(aoblkdirrelid1))
		{
			TransferDependencyLink(entryRelId2, aoblkdirrelid1, "aoblkdir");
		}
		if (OidIsValid(aoblkdirrelid2))
		{
			TransferDependencyLink(entryRelId1, aoblkdirrelid2, "aoblkdir");
		}
	}

	if (Debug_persistent_print)
		elog(Persistent_DebugPrintLevel(), 
			 "SwapAppendonlyEntries: relation id #1 %u, aosegrelid1 %u, aoblkdirrelid1 %u, "
			 "relation id #2 %u, aosegrelid2 %u, aoblkdirrelid2 %u",
			 entryRelId1,
			 aosegrelid1,
			 aoblkdirrelid1,
			 entryRelId2,
			 aosegrelid2,
			 aoblkdirrelid2);
}

