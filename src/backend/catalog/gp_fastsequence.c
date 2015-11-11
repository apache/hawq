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
/*-------------------------------------------------------------------------
 *
 * gp_fastsequence.c
 *    routines to maintain a light-weight sequence table.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "catalog/catquery.h"
#include "catalog/gp_fastsequence.h"
#include "catalog/indexing.h"
#include "utils/relcache.h"
#include "utils/fmgroids.h"
#include "access/genam.h"
#include "access/htup.h"
#include "access/heapam.h"
#include "cdb/cdbvars.h"

static void update_fastsequence(
	Relation gp_fastsequence_rel,
	HeapTuple oldTuple,
	TupleDesc tupleDesc,
	Oid objid,
	int64 objmod,
	int64 newLastSequence,
	ItemPointer tid);

/*
 * InsertFastSequenceEntry
 *
 * Insert a new fast sequence entry for a given object. If the given
 * object already exists in the table, this function replaces the old
 * entry with a fresh initial value.
 *
 * The tid for the new entry is returned.
 */
void
InsertFastSequenceEntry(Oid objid, int64 objmod, int64 lastSequence,
		ItemPointer tid)
{
	Relation gp_fastsequence_rel;
	TupleDesc tupleDesc;
	int natts = 0;
	Datum *values;
	bool *nulls;
	HeapTuple tuple = NULL;
	cqContext	 cqc;
	
	/*
	 * Open and lock the gp_fastsequence catalog table.
	 */
	gp_fastsequence_rel = heap_open(FastSequenceRelationId, RowExclusiveLock);
	tupleDesc = RelationGetDescr(gp_fastsequence_rel);
	
	tuple = caql_getfirst(
			caql_addrel(cqclr(&cqc), gp_fastsequence_rel),
			cql("SELECT * FROM gp_fastsequence "
				" WHERE objid = :1 "
				" AND objmod = :2 "
				" AND contentid = :3 "
				" FOR UPDATE ",
				ObjectIdGetDatum(objid),
				Int64GetDatum(objmod),
				Int32GetDatum(-1)));

	if (tuple == NULL)
	{
		natts = tupleDesc->natts;
		values = palloc0(sizeof(Datum) * natts);
		nulls = palloc0(sizeof(bool) * natts);
	
		values[Anum_gp_fastsequence_objid - 1] = ObjectIdGetDatum(objid);
		values[Anum_gp_fastsequence_objmod - 1] = Int64GetDatum(objmod);
		values[Anum_gp_fastsequence_last_sequence - 1] = Int64GetDatum(lastSequence);
		values[Anum_gp_fastsequence_contentid - 1] = Int32GetDatum(-1);

		tuple = heaptuple_form_to(tupleDesc, values, nulls, NULL, NULL);
		frozen_heap_insert(gp_fastsequence_rel, tuple);
		/*
		 * in hawq, index only exists on master
		 */
		if (Gp_role != GP_ROLE_EXECUTE)
			CatalogUpdateIndexes(gp_fastsequence_rel, tuple);
	
		ItemPointerCopy(&tuple->t_self, tid);

		heap_freetuple(tuple);
		pfree(values);
		pfree(nulls);
	}
	else
	{
		update_fastsequence(gp_fastsequence_rel,
							tuple,
							tupleDesc,
							objid,
							objmod,
							lastSequence,
							tid);
	}

	/*
	 * Since the tid for this row may be used later in this transaction, 
	 * we keep the lock until the end of the transaction.
	 */
	heap_close(gp_fastsequence_rel, NoLock);
}

/*
 * update_fastsequnece -- update the fast sequence number for (objid, objmod).
 *
 * If such an entry exists in the table, it is provided in oldTuple. This tuple
 * is updated with the new value. Otherwise, a new tuple is inserted into the
 * table.
 *
 * The tuple id value for the entry is copied out to 'tid'.
 *
 * NOTE: in hawq, we do not add new tuple in QE, it should be dispatched
 * from master, and only update is allowed on QE.
 */
static void
update_fastsequence(Relation gp_fastsequence_rel,
					HeapTuple oldTuple,
					TupleDesc tupleDesc,
					Oid objid,
					int64 objmod,
					int64 newLastSequence,
					ItemPointer tid)
{
	Datum *values;
	bool *nulls;
	HeapTuple newTuple;

	values = palloc0(sizeof(Datum) * tupleDesc->natts);
	nulls = palloc0(sizeof(bool) * tupleDesc->natts);

	/*
	 * If such a tuple does not exist, insert a new one.
	 */
	if (oldTuple == NULL)
	{
		Assert(Gp_role != GP_ROLE_EXECUTE);

		values[Anum_gp_fastsequence_objid - 1] = ObjectIdGetDatum(objid);
		values[Anum_gp_fastsequence_objmod - 1] = Int64GetDatum(objmod);
		values[Anum_gp_fastsequence_last_sequence - 1] = Int64GetDatum(newLastSequence);
		values[Anum_gp_fastsequence_contentid - 1] = Int32GetDatum(-1);

		newTuple = heaptuple_form_to(tupleDesc, values, nulls, NULL, NULL);
		frozen_heap_insert(gp_fastsequence_rel, newTuple);

		CatalogUpdateIndexes(gp_fastsequence_rel, newTuple);

		ItemPointerCopy(&newTuple->t_self, tid);

		heap_freetuple(newTuple);
	}

	else
	{
#ifdef USE_ASSERT_CHECKING
		Oid oldObjid;
		int64 oldObjmod;
		bool isNull;
		
		oldObjid = heap_getattr(oldTuple, Anum_gp_fastsequence_objid, tupleDesc, &isNull);
		Assert(!isNull);
		oldObjmod = heap_getattr(oldTuple, Anum_gp_fastsequence_objmod, tupleDesc, &isNull);
		Assert(!isNull);
		Assert(oldObjid == objid && oldObjmod == objmod);
#endif

		values[Anum_gp_fastsequence_objid - 1] = ObjectIdGetDatum(objid);
		values[Anum_gp_fastsequence_objmod - 1] = Int64GetDatum(objmod);
		values[Anum_gp_fastsequence_last_sequence - 1] = Int64GetDatum(newLastSequence);
		values[Anum_gp_fastsequence_contentid - 1] = Int32GetDatum(-1);

		newTuple = heap_form_tuple(tupleDesc, values, nulls);
		newTuple->t_data->t_ctid = oldTuple->t_data->t_ctid;
		newTuple->t_self = oldTuple->t_self;
		if (tupleDesc->tdhasoid)
			HeapTupleSetOid(newTuple, HeapTupleGetOid(oldTuple));
		
		if (Gp_role != GP_ROLE_EXECUTE)
		{
			heap_inplace_update(gp_fastsequence_rel, newTuple);
		}
		else
		{
			InMemHeapRelation inmemrel = OidGetInMemHeapRelation(
					gp_fastsequence_rel->rd_id, INMEM_HEAP_MAPPING);
			if (NULL == inmemrel)
			{
				elog(ERROR, "cannot find in-memory table: %s",
						RelationGetRelationName(gp_fastsequence_rel));
			}

			InMemHeap_Update(inmemrel, &oldTuple->t_self, newTuple);
		}

		ItemPointerCopy(&newTuple->t_self, tid);

		heap_freetuple(newTuple);
	}
	
	pfree(values);
	pfree(nulls);
}

/*
 * GetFastSequences
 *
 * Get a list of consecutive sequence numbers. The starting sequence
 * number is the maximal value between 'lastsequence' + 1 and minSequence.
 * The length of the list is given.
 *
 * If there is not such an entry for objid in the table, create
 * one here.
 *
 * The existing entry for objid in the table is updated with a new
 * lastsequence value.
 *
 * The tuple id value for this entry is copied out to 'tid'.
 */
int64 GetFastSequences(Oid objid, int64 objmod,
					   int64 minSequence, int64 numSequences,
					   ItemPointer tid)
{
	Relation gp_fastsequence_rel;
	TupleDesc tupleDesc;
	HeapTuple tuple;
	int64 firstSequence = minSequence;
	Datum lastSequenceDatum;
	int64 newLastSequence;

	Assert(tid != NULL);
	Assert(Gp_role != GP_ROLE_DISPATCH);
	
	gp_fastsequence_rel = heap_open(FastSequenceRelationId, RowExclusiveLock);
	tupleDesc = RelationGetDescr(gp_fastsequence_rel);
	
	cqContext *pcqCtx = caql_beginscan(
					NULL,
					cql("SELECT * FROM gp_fastsequence "
						" WHERE objid = :1 "
						" AND objmod = :2 "
						" FOR UPDATE ",
						ObjectIdGetDatum(objid),
						Int64GetDatum(objmod)));

	tuple = caql_getnext(pcqCtx);

	if (tuple == NULL)
	{
		elog(ERROR, "gp_fastsequence should be dispatched to QE, "
				"objid = %u, objmod = "INT64_FORMAT", minseq = "INT64_FORMAT", numseq = "INT64_FORMAT".",
				objid, objmod, minSequence, numSequences);

		newLastSequence = firstSequence + numSequences - 1;
	}
	else
	{
		bool isNull;

		lastSequenceDatum = heap_getattr(tuple, Anum_gp_fastsequence_last_sequence,
										tupleDesc, &isNull);
		
		if (isNull)
			ereport(ERROR,
					(errcode(ERRCODE_UNDEFINED_OBJECT),
					 errmsg("got an invalid lastsequence number: NULL")));
		
		if (DatumGetInt64(lastSequenceDatum) + 1 > firstSequence)
			firstSequence = DatumGetInt64(lastSequenceDatum) + 1;
		newLastSequence = firstSequence + numSequences - 1;
	}
	
	update_fastsequence(gp_fastsequence_rel, tuple, tupleDesc,
						objid, objmod, newLastSequence, tid);
		

	caql_endscan(pcqCtx);

	/*
	 * Since the tid for this row may be used later in this transaction, 
	 * we keep the lock until the end of the transaction.
	 */
	heap_close(gp_fastsequence_rel, NoLock);

	return firstSequence;
}

/*
 * GetFastSequencesByTid
 *
 * Same as GetFastSequences, except that the tuple tid is given, and the tuple id
 * is not valid.
 */
int64
GetFastSequencesByTid(ItemPointer tid,
					  int64 minSequence,
					  int64 numSequences)
{
	Relation gp_fastsequence_rel;
	TupleDesc tupleDesc;
	HeapTupleData tuple;
	Buffer userbuf;
	bool found = false;
	Datum lastSequenceDatum;
	int64 newLastSequence;
	int64 firstSequence = minSequence;
	bool isNull;
	Oid objidDatum;
	int64 objmodDatum;

	gp_fastsequence_rel = heap_open(FastSequenceRelationId, RowExclusiveLock);
	tupleDesc = RelationGetDescr(gp_fastsequence_rel);

	Assert(ItemPointerIsValid(tid));

	ItemPointerCopy(tid, &tuple.t_self);

	found = heap_fetch(gp_fastsequence_rel, SnapshotNow, &tuple,
					   &userbuf, false, NULL);
	Assert(found);
	
	lastSequenceDatum = heap_getattr(&tuple, Anum_gp_fastsequence_last_sequence,
									 gp_fastsequence_rel->rd_att, &isNull);
	if (isNull)
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_OBJECT),
				 errmsg("got an invalid lastsequence number: NULL")));
	
	objidDatum = heap_getattr(&tuple, Anum_gp_fastsequence_objid,
							  gp_fastsequence_rel->rd_att, &isNull);
	if (isNull)
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_OBJECT),
				 errmsg("got an invalid objid: NULL")));
	
	objmodDatum = heap_getattr(&tuple, Anum_gp_fastsequence_objmod,
							   gp_fastsequence_rel->rd_att, &isNull);
	if (isNull)
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_OBJECT),
				 errmsg("got an invalid objmod: NULL")));
	
	if (DatumGetInt64(lastSequenceDatum) + 1 > minSequence)
		firstSequence = DatumGetInt64(lastSequenceDatum) + 1;
	newLastSequence = firstSequence + numSequences - 1;
	
	update_fastsequence(gp_fastsequence_rel,
						&tuple,
						tupleDesc,
						DatumGetObjectId(objidDatum),
						DatumGetInt64(objmodDatum),
						newLastSequence,
						tid);
	
	ReleaseBuffer(userbuf);
	
	/*
	 * Since the tid for this row may be used later in this transaction, 
	 * we keep the lock until the end of the transaction.
	 */
	heap_close(gp_fastsequence_rel, NoLock);

	return firstSequence;
}

/*
 * RemoveFastSequenceEntry
 *
 * Remove all entries associated with the given object id.
 *
 * If the given objid is an invalid OID, this function simply
 * returns.
 *
 * It is okay for the given valid objid to have no entries in
 * gp_fastsequence.
 */
void
RemoveFastSequenceEntry(Oid objid)
{
	int numDel;
	
	if (!OidIsValid(objid))
		return;

	numDel = caql_getcount(
			NULL,
			cql("DELETE FROM gp_fastsequence "
				" WHERE objid = :1 ",
				ObjectIdGetDatum(objid)));

}

