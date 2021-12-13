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
 * execDML.c
 *        Implementation of execDML.
 *        This file performs INSERT, DELETE and UPDATE DML operations.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"
#include "fmgr.h"

#include "access/attnum.h"
#include "access/fileam.h"
#include "access/orcam.h"
#include "access/plugstorage.h"
#include "access/xact.h"
#include "catalog/pg_constraint.h"
#include "catalog/pg_exttable.h"
#include "cdb/cdbappendonlyam.h"
#include "cdb/cdbdatalocality.h"
#include "cdb/cdbparquetam.h"
#include "cdb/cdbpartition.h"
#include "cdb/cdbvars.h"
#include "commands/trigger.h"
#include "executor/execdebug.h"
#include "executor/execDML.h"
#include "executor/tuptable.h"
#include "nodes/pg_list.h"
#include "utils/acl.h"
#include "utils/hsearch.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"

/*
 * reconstructTupleValues
 *   Re-construct tuple values based on the pre-defined mapping.
 */
void
reconstructTupleValues(AttrMap *map,
					Datum *oldValues, bool *oldIsnull, int oldNumAttrs,
					Datum *newValues, bool *newIsnull, int newNumAttrs)
{
	for (int oldAttNo = 1; oldAttNo <= oldNumAttrs; oldAttNo++)
	{
		int newAttNo = attrMap(map, oldAttNo);
		if (newAttNo == 0)
		{
			continue;
		}

		Assert(newAttNo <= newNumAttrs);
		newValues[newAttNo - 1] = oldValues[oldAttNo - 1];
		newIsnull[newAttNo - 1] = oldIsnull[oldAttNo - 1];
	}
}

/*
 * Use the supplied ResultRelInfo to create an appropriately restructured
 * version of the tuple in the supplied slot, if necessary.
 *
 * slot -- slot containing the input tuple
 * resultRelInfo -- info pertaining to the target part of an insert
 *
 * If no restructuring is required, the result is the argument slot, else
 * it is the slot from the argument result info updated to hold the
 * restructured tuple.
 */
TupleTableSlot *
reconstructMatchingTupleSlot(TupleTableSlot *slot, ResultRelInfo *resultRelInfo)
{
	int natts;
	Datum *values;
	bool *isnull;
	AttrMap *map;
	TupleTableSlot *partslot;
	Datum *partvalues;
	bool *partisnull;

	map = resultRelInfo->ri_partInsertMap;

	TupleDesc inputTupDesc = slot->tts_tupleDescriptor;
	TupleDesc resultTupDesc = resultRelInfo->ri_RelationDesc->rd_att;
	bool tupleDescMatch = (resultRelInfo->tupdesc_match == 1);
	if (resultRelInfo->tupdesc_match == 0)
	{
		tupleDescMatch = equalTupleDescs(inputTupDesc, resultTupDesc, false);

		if (tupleDescMatch)
		{
			resultRelInfo->tupdesc_match = 1;
		}
		else
		{
			resultRelInfo->tupdesc_match = -1;
		}
	}

	/* No map and matching tuple descriptor means no restructuring needed. */
	if (map == NULL && tupleDescMatch)
		return slot;

	/* Put the given tuple into attribute arrays. */
	natts = slot->tts_tupleDescriptor->natts;
	slot_getallattrs(slot);
	values = slot_get_values(slot);
	isnull = slot_get_isnull(slot);

	/*
	 * Get the target slot ready. If this is a child partition table,
	 * set target slot to ri_partSlot. Otherwise, use ri_resultSlot.
	 */
	if (map != NULL)
	{
		Assert(resultRelInfo->ri_partSlot != NULL);
		partslot = resultRelInfo->ri_partSlot;
	}
	else
	{
		if (resultRelInfo->ri_resultSlot == NULL)
		{
			resultRelInfo->ri_resultSlot = MakeSingleTupleTableSlot(resultTupDesc);
		}

		partslot = resultRelInfo->ri_resultSlot;
	}

	partslot = ExecStoreAllNullTuple(partslot);
	partvalues = slot_get_values(partslot);
	partisnull = slot_get_isnull(partslot);

	/* Restructure the input tuple.  Non-zero map entries are attribute
	 * numbers in the target tuple, however, not every attribute
	 * number of the input tuple need be present.  In particular,
	 * attribute numbers corresponding to dropped attributes will be
	 * missing.
	 */
	reconstructTupleValues(map, values, isnull, natts,
						partvalues, partisnull, partslot->tts_tupleDescriptor->natts);
	partslot = ExecStoreVirtualTuple(partslot);

	return partslot;
}

/* ----------------------------------------------------------------
 *		ExecInsert
 *
 *		INSERTs have to add the tuple into
 *		the base relation and insert appropriate tuples into the
 *		index relations.
 *		Insert can be part of an update operation when
 *		there is a preceding SplitUpdate node. 
 * ----------------------------------------------------------------
 */
void
ExecInsert(TupleTableSlot *slot,
		DestReceiver *dest,
		EState *estate,
		PlanGenerator planGen,
		bool isUpdate,
		bool isInputSorted)
{
	void		*tuple = NULL;
	ResultRelInfo *resultRelInfo = NULL;
	Relation	resultRelationDesc = NULL;
	Oid			newId = InvalidOid;
	TupleTableSlot *partslot = NULL;

	AOTupleId	aoTupleId = AOTUPLEID_INIT;

	bool		rel_is_heap = false;
	bool 		rel_is_aorows = false;
	bool		rel_is_external = false;
  bool		rel_is_parquet = false;
  bool    rel_is_orc = false;

	/*
	 * get information on the (current) result relation
	 */
	if (estate->es_result_partitions)
	{
		resultRelInfo = slot_get_partition(slot, estate);
		estate->es_result_relation_info = resultRelInfo;

		if (NULL != resultRelInfo->ri_insertSendBack)
		{
			/*
			 * The Parquet part we are about to insert into
			 * has sendBack information. This means we're inserting into the
			 * part twice, which is not supported. Error out (GPSQL-2291)
			 */
			Assert(gp_parquet_insert_sort);
			ereport(ERROR, (errcode(ERRCODE_CDB_FEATURE_NOT_YET),
					errmsg("Cannot insert out-of-order tuples in parquet partitions"),
					errhint("Sort the data on the partitioning key(s) before inserting"),
					errOmitLocation(true)));
		}

		/*
		 * Check if we need to close the last parquet partition we
		 * inserted into (GPSQL-2291).
		 */
		Oid new_part_oid = resultRelInfo->ri_RelationDesc->rd_id;

		if (isInputSorted &&
				PLANGEN_OPTIMIZER == planGen &&
				InvalidOid != estate->es_last_inserted_part &&
				new_part_oid != estate->es_last_inserted_part)
		{

			Assert(NULL != estate->es_partition_state->result_partition_hash);

			ResultPartHashEntry *entry = hash_search(estate->es_partition_state->result_partition_hash,
					&estate->es_last_inserted_part,
					HASH_FIND,
					NULL /* found */);

			Assert(NULL != entry);
			Assert(entry->offset < estate->es_num_result_relations);

			ResultRelInfo *oldResultRelInfo = & estate->es_result_relations[entry->offset];
			Assert(NULL != oldResultRelInfo);


			/*
			 * We need to preserve the "sendback" information that needs to be
			 * sent back to the QD process from this part.
			 * Compute it here, and store it for later use.
			 */
			QueryContextDispatchingSendBack sendback = CreateQueryContextDispatchingSendBack(1);
			sendback->relid = RelationGetRelid(oldResultRelInfo->ri_RelationDesc);

			Relation oldRelation = oldResultRelInfo->ri_RelationDesc;
			if (RelationIsAoRows(oldRelation))
			{
				AppendOnlyInsertDescData *oldInsertDesc = oldResultRelInfo->ri_aoInsertDesc;
				Assert(NULL != oldInsertDesc);

				elog(DEBUG1, "AO: Switching from old part oid=%d name=[%s] to new part oid=%d name=[%s]",
						estate->es_last_inserted_part,
						oldResultRelInfo->ri_RelationDesc->rd_rel->relname.data,
						new_part_oid,
						resultRelInfo->ri_RelationDesc->rd_rel->relname.data);

				oldInsertDesc->sendback = sendback;

				appendonly_insert_finish(oldInsertDesc);
				oldResultRelInfo->ri_aoInsertDesc = NULL;

			}
			else if (RelationIsParquet(oldRelation))
			{
				ParquetInsertDescData *oldInsertDesc = oldResultRelInfo->ri_parquetInsertDesc;
				Assert(NULL != oldInsertDesc);

				elog(DEBUG1, "PARQ: Switching from old part oid=%d name=[%s] to new part oid=%d name=[%s]",
						estate->es_last_inserted_part,
						oldResultRelInfo->ri_RelationDesc->rd_rel->relname.data,
						new_part_oid,
						resultRelInfo->ri_RelationDesc->rd_rel->relname.data);

				oldInsertDesc->sendback = sendback;

				parquet_insert_finish(oldInsertDesc);
				oldResultRelInfo->ri_parquetInsertDesc = NULL;

			}
			else if (RelationIsOrc(oldRelation))
			{
			  OrcInsertDescData *oldInsertDesc = oldResultRelInfo->ri_orcInsertDesc;
        Assert(NULL != oldInsertDesc);

        oldInsertDesc->sendback = sendback;

        orcEndInsert(oldInsertDesc);
        oldResultRelInfo->ri_orcInsertDesc = NULL;
			}
			else
			{
				Assert(false && "Unreachable");
			}

			/* Store the sendback information in the resultRelInfo for this part */
			oldResultRelInfo->ri_insertSendBack = sendback;

			estate->es_last_inserted_part = InvalidOid;
		}
	}
	else
	{
		resultRelInfo = estate->es_result_relation_info;
	}

	Assert (!resultRelInfo->ri_projectReturning);

	resultRelationDesc = resultRelInfo->ri_RelationDesc;

	rel_is_heap = RelationIsHeap(resultRelationDesc);
	rel_is_aorows = RelationIsAoRows(resultRelationDesc);
	rel_is_external = RelationIsExternal(resultRelationDesc);
  rel_is_parquet = RelationIsParquet(resultRelationDesc);
  rel_is_orc = RelationIsOrc(resultRelationDesc);

	/* Validate that insert is not part of an non-allowed update operation. */
	if (isUpdate && (rel_is_aorows || rel_is_parquet))
	{
		ereport(ERROR,
			(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				errmsg("Append-only tables are not updatable. Operation not permitted."),
				errOmitLocation(true)));
	}

	partslot = reconstructMatchingTupleSlot(slot, resultRelInfo);
	if (rel_is_heap)
	{
		tuple = ExecFetchSlotHeapTuple(partslot);
	}
	else if (rel_is_aorows)
	{
		tuple = ExecFetchSlotMemTuple(partslot, false);
	}
	else if (rel_is_parquet || rel_is_orc || rel_is_external)
	{
		tuple = NULL;
	}

	Assert( partslot != NULL );
	Assert( rel_is_parquet || rel_is_orc || rel_is_external || (tuple != NULL));

	/* Execute triggers in Planner-generated plans */
	if (planGen == PLANGEN_PLANNER)
	{
		/* BEFORE ROW INSERT Triggers */
		if (resultRelInfo->ri_TrigDesc &&
			resultRelInfo->ri_TrigDesc->n_before_row[TRIGGER_EVENT_INSERT] > 0)
		{
			HeapTuple	newtuple;

			/* NYI */
			if(rel_is_parquet)
				elog(ERROR, "triggers are not supported on tables that use column-oriented storage");

			newtuple = ExecBRInsertTriggers(estate, resultRelInfo, tuple);

			if (newtuple == NULL)	/* "do nothing" */
			{
				return;
			}

			if (newtuple != tuple)	/* modified by Trigger(s) */
			{
				/*
				 * Put the modified tuple into a slot for convenience of routines
				 * below.  We assume the tuple was allocated in per-tuple memory
				 * context, and therefore will go away by itself. The tuple table
				 * slot should not try to clear it.
				 */
				TupleTableSlot *newslot = estate->es_trig_tuple_slot;

				if (newslot->tts_tupleDescriptor != partslot->tts_tupleDescriptor)
					ExecSetSlotDescriptor(newslot, partslot->tts_tupleDescriptor);
				ExecStoreGenericTuple(newtuple, newslot, false);
				newslot->tts_tableOid = partslot->tts_tableOid; /* for constraints */
				tuple = newtuple;
				partslot = newslot;
			}
		}
	}
	/*
	 * Check the constraints of the tuple
	 */
	if (resultRelationDesc->rd_att->constr &&
			planGen == PLANGEN_PLANNER)
	{
		ExecConstraints(resultRelInfo, partslot, estate);
	}
	/*
	 * insert the tuple
	 *
	 * Note: heap_insert returns the tid (location) of the new tuple in the
	 * t_self field.
	 *
	 * NOTE: for append-only relations we use the append-only access methods.
	 */
	if (rel_is_aorows)
	{
		if (resultRelInfo->ri_aoInsertDesc == NULL)
		{
			ResultRelSegFileInfo *segfileinfo = NULL;
			/* Set the pre-assigned fileseg number to insert into */
			ResultRelInfoSetSegFileInfo(resultRelInfo, estate->es_result_segfileinfos);
			segfileinfo = (ResultRelSegFileInfo *)list_nth(resultRelInfo->ri_aosegfileinfos, GetQEIndex());
			resultRelInfo->ri_aoInsertDesc =
				appendonly_insert_init(resultRelationDesc,
									   segfileinfo);
			estate->es_last_inserted_part = resultRelationDesc->rd_id;
		}

		appendonly_insert(resultRelInfo->ri_aoInsertDesc, tuple, &newId, &aoTupleId);
	}
	else if (rel_is_external)
	{
		ExternalInsertDesc extInsertDesc = resultRelInfo->ri_extInsertDesc;

		/* Writable external table */
		if (extInsertDesc == NULL)
		{
			/* Get pg_exttable information for the external table */
			ExtTableEntry *extEntry =
					GetExtTableEntry(RelationGetRelid(resultRelationDesc));

			/* Get formatter type and name for the external table */
			int formatterType = ExternalTableType_Invalid;
			char *formatterName = NULL;

			getExternalTableTypeStr(extEntry->fmtcode, extEntry->fmtopts,
			                        &formatterType, &formatterName);

			pfree(extEntry);

			if (formatterType == ExternalTableType_Invalid)
			{
				elog(ERROR, "invalid formatter type for external table: %s", __func__);
			}
			else if (formatterType != ExternalTableType_PLUG)
			{
				resultRelInfo->ri_extInsertDesc = external_insert_init(
						resultRelationDesc, 0, formatterType, formatterName, estate->es_plannedstmt);
			}
			else
			{
				Assert(formatterName);

				Oid	procOid = LookupPlugStorageValidatorFunc(formatterName,
				                                             "insert_init");

				if (OidIsValid(procOid))
				{
					FmgrInfo insertInitFunc;
					fmgr_info(procOid, &insertInitFunc);

					ResultRelSegFileInfo *segfileinfo = NULL;
					ResultRelInfoSetSegFileInfo(resultRelInfo,
							estate->es_result_segfileinfos);
					segfileinfo = (ResultRelSegFileInfo *) list_nth(
							resultRelInfo->ri_aosegfileinfos, GetQEIndex());

					resultRelInfo->ri_extInsertDesc =

							InvokePlugStorageFormatInsertInit(&insertInitFunc,
															 resultRelationDesc,
															 formatterType,
															 formatterName,
															 estate->es_plannedstmt,
															 segfileinfo->segno,
															 PlugStorageGetTransactionSnapshot(NULL));
				}
				else
				{
					elog(ERROR, "%s_insert_init function was not found", formatterName);
				}
			}
		}

		extInsertDesc = resultRelInfo->ri_extInsertDesc;

		if (extInsertDesc->ext_formatter_type == ExternalTableType_Invalid)
		{
			elog(ERROR, "invalid formatter type for external table: %s", __func__);
		}
		else if (extInsertDesc->ext_formatter_type != ExternalTableType_PLUG)
		{
			newId = external_insert(resultRelInfo->ri_extInsertDesc, partslot);
		}
		else
		{
			Assert(extInsertDesc->ext_formatter_name);

			/* Form virtual tuple */
			slot_getallattrs(partslot);

			FmgrInfo *insertFunc = extInsertDesc->ext_ps_insert_funcs.insert;

			if (insertFunc)
			{
				newId = InvokePlugStorageFormatInsert(insertFunc,
				                                      extInsertDesc,
				                                      partslot);
			}
			else
			{
				elog(ERROR, "%s_insert function was not found",
				            extInsertDesc->ext_formatter_name);
			}
		}
	}
    else if(rel_is_parquet)
	{
		/* If there is no parquet insert descriptor, create it now. */
		if (resultRelInfo->ri_parquetInsertDesc == NULL)
		{
			ResultRelSegFileInfo *segfileinfo = NULL;
			ResultRelInfoSetSegFileInfo(resultRelInfo, estate->es_result_segfileinfos);
			segfileinfo = (ResultRelSegFileInfo *)list_nth(resultRelInfo->ri_aosegfileinfos, GetQEIndex());
			resultRelInfo->ri_parquetInsertDesc = parquet_insert_init(resultRelationDesc, segfileinfo);

			/*
			 * Just opened a new parquet partition for insert. Save the Oid
			 * in estate, so that we can close it when switching to a
			 * new partition (GPSQL-2291)
			 */
			estate->es_last_inserted_part = resultRelationDesc->rd_id;
		}

		newId = parquet_insert(resultRelInfo->ri_parquetInsertDesc, partslot);
    } else if (rel_is_orc) {
      if (resultRelInfo->ri_orcInsertDesc == NULL) {
        ResultRelSegFileInfo *segfileinfo = NULL;
        ResultRelInfoSetSegFileInfo(resultRelInfo,
                                    estate->es_result_segfileinfos);
        segfileinfo = (ResultRelSegFileInfo *)list_nth(
            resultRelInfo->ri_aosegfileinfos, GetQEIndex());
        resultRelInfo->ri_orcInsertDesc =
            orcBeginInsert(resultRelationDesc, segfileinfo);

        estate->es_last_inserted_part = resultRelationDesc->rd_id;
      }

      newId = orcInsert(resultRelInfo->ri_orcInsertDesc, partslot);
    } else {
      Insist(rel_is_heap);

      newId = heap_insert(resultRelationDesc, tuple,
                          estate->es_snapshot->curcid, true, true,
                          GetCurrentTransactionId());
    }

  IncrAppended();
	(estate->es_processed)++;
	(resultRelInfo->ri_aoprocessed)++;
	estate->es_lastoid = newId;

	partslot->tts_tableOid = RelationGetRelid(resultRelationDesc);

	if (rel_is_aorows || rel_is_parquet || rel_is_orc || rel_is_external)
	{

		/* NOTE: Current version does not support index upon parquet table. */
		/*
		 * insert index entries for AO Row-Store tuple
		 */
		if (resultRelInfo->ri_NumIndices > 0 && !rel_is_parquet && !rel_is_orc)
			ExecInsertIndexTuples(partslot, (ItemPointer)&aoTupleId, estate, false);
	}
	else
	{
		/* Use parttuple for index update in case this is an indexed heap table. */
		TupleTableSlot *xslot = partslot;
		void *xtuple = tuple;

		setLastTid(&(((HeapTuple) xtuple)->t_self));

		/*
		 * insert index entries for tuple
		 */
		if (resultRelInfo->ri_NumIndices > 0)
			ExecInsertIndexTuples(xslot, &(((HeapTuple) xtuple)->t_self), estate, false);

	}

	if (planGen == PLANGEN_PLANNER)
	{
		/* AFTER ROW INSERT Triggers */
		ExecARInsertTriggers(estate, resultRelInfo, tuple);
	}
}

/* ----------------------------------------------------------------
 *		ExecDelete
 *
 *		DELETE is like UPDATE, except that we delete the tuple and no
 *		index modifications are needed.
 *		DELETE can be part of an update operation when
 *		there is a preceding SplitUpdate node. 
 *
 * ----------------------------------------------------------------
 */
void
ExecDelete(ItemPointer tupleid,
		   Datum segmentid,
		   TupleTableSlot *planSlot,
		   DestReceiver *dest,
		   EState *estate,
		   PlanGenerator planGen,
		   bool isUpdate)
{
	ResultRelInfo *resultRelInfo;
	Relation resultRelationDesc;
	HTSU_Result result;
	ItemPointerData update_ctid;
	TransactionId update_xmax;

	/*
	 * Get information on the (current) result relation.
	 */
	if (estate->es_result_partitions && planGen == PLANGEN_OPTIMIZER)
	{
		Assert(estate->es_result_partitions->part->parrelid);

#ifdef USE_ASSERT_CHECKING
		Oid parent = estate->es_result_partitions->part->parrelid;
#endif

		/* Obtain part for current tuple. */
		resultRelInfo = slot_get_partition(planSlot, estate);
		estate->es_result_relation_info = resultRelInfo;

#ifdef USE_ASSERT_CHECKING
		Oid part = RelationGetRelid(resultRelInfo->ri_RelationDesc);
#endif

		Assert(parent != part);
	}
	else
	{
		resultRelInfo = estate->es_result_relation_info;
	}
	resultRelationDesc = resultRelInfo->ri_RelationDesc;

	Assert (!resultRelInfo->ri_projectReturning);

	if (planGen == PLANGEN_PLANNER)
	{
		/* BEFORE ROW DELETE Triggers */
		if (resultRelInfo->ri_TrigDesc &&
			resultRelInfo->ri_TrigDesc->n_before_row[TRIGGER_EVENT_DELETE] > 0)
		{
			bool		dodelete;

			dodelete = ExecBRDeleteTriggers(estate, resultRelInfo, tupleid,
											estate->es_snapshot->curcid);

			if (!dodelete)			/* "do nothing" */
				return;
		}
	}

	bool isHeapTable = RelationIsHeap(resultRelationDesc);
	bool isExternalTable = RelationIsExternal(resultRelationDesc);
	bool isNativeOrc = RelationIsOrc(resultRelationDesc);

	/*
	 * delete the tuple
	 *
	 * Note: if es_crosscheck_snapshot isn't InvalidSnapshot, we check that
	 * the row to be deleted is visible to that snapshot, and throw a can't-
	 * serialize error if not.	This is a special-case behavior needed for
	 * referential integrity updates in serializable transactions.
	 */
ldelete:;
	if (isHeapTable)
	{
		result = heap_delete(resultRelationDesc, tupleid,
		                     &update_ctid, &update_xmax,
		                     estate->es_snapshot->curcid,
		                     estate->es_crosscheck_snapshot,
		                     true /* wait for commit */ );
	}
	else if (isExternalTable)
	{
	    Assert(estate->es_ext_del_oid_desc != NULL);

	    ExternalInsertDescHashEntry *extDelDescEntry = NULL;
	    bool found = false;
	    extDelDescEntry = hash_search(estate->es_ext_del_oid_desc,
	                                  (void *) (&(RelationGetRelid(resultRelationDesc))),
	                                  HASH_ENTER,
	                                  &found);

	    if (!found)
	    {
	        Assert(extDelDescEntry != NULL);
	        /*
	         * Step 1: prepare ExternalInsertDescData which will be shared
	         * during begindelete, delete, and enddelete, if necessary
	         */
	        ExtTableEntry *extEntry =
	                GetExtTableEntry(RelationGetRelid(resultRelationDesc));

	        /* Get formatter type and name for the external table */
	        int formatterType = ExternalTableType_Invalid;
	        char *formatterName = NULL;

	        getExternalTableTypeStr(extEntry->fmtcode, extEntry->fmtopts,
	                                &formatterType, &formatterName);
	        pfree(extEntry);

	        Oid procOid = LookupMagmaFunction(formatterName, "begindelete");
	        FmgrInfo procInfo;
	        if (OidIsValid(procOid))
	        {
	            fmgr_info(procOid, &procInfo);

	            /* Step 2: begin delete tuple */
	            extDelDescEntry->ext_ins_desc =
	                    InvokeMagmaBeginDelete(&procInfo,
	                                           resultRelationDesc,
	                                           estate->es_plannedstmt,
	                                           PlugStorageGetTransactionSnapshot(NULL));
		    }
	        else
	        {
	            elog(ERROR, "magma_begindelete function was not found for pluggable storage");
	        }
	    }

	    /* Step 3: delete tuple */
	    ExternalInsertDesc extDelDesc = extDelDescEntry->ext_ins_desc;
	    extDelDesc->ext_rangeId = GetRangeIdFromCtidAndSegmentid(segmentid);
	    extDelDesc->ext_rowId = GetRowIdFromCtidAndSegmentid(tupleid, segmentid);
	    InvokeMagmaDelete(extDelDesc->ext_ps_delete_funcs.deletes,
	                      extDelDesc,
	                      planSlot);

	    result = HeapTupleMayBeUpdated;
  } else if (isNativeOrc) {
    if (resultRelInfo->ri_orcDeleteDesc == NULL) {
      List *splits =
          GetFileSplitsOfSegment(estate->es_plannedstmt->scantable_splits,
                                 resultRelationDesc->rd_id, GetQEIndex());
      resultRelInfo->ri_orcDeleteDesc = orcBeginDelete(
          resultRelationDesc, splits, estate->es_plannedstmt->relFileNodeInfo,
          false,
          isDirectDispatch(estate->es_plannedstmt->planTree));
    }
    resultRelInfo->ri_orcDeleteDesc->rowId = (Datum)(ItemPointerGetRowIdFromFakeCtid(tupleid));
    orcDelete(resultRelInfo->ri_orcDeleteDesc);
    result = HeapTupleMayBeUpdated;
  } else {
    Insist(0);
  }

  switch (result)
	{
		case HeapTupleSelfUpdated:
			/* already deleted by self; nothing to do */
		
			/*
			 * In an scenario in which R(a,b) and S(a,b) have 
			 *        R               S
			 *    ________         ________
			 *     (1, 1)           (1, 2)
			 *                      (1, 7)
 			 *
   			 *  An update query such as:
 			 *   UPDATE R SET a = S.b  FROM S WHERE R.b = S.a;
 			 *   
 			 *  will have an non-deterministic output. The tuple in R 
			 * can be updated to (2,1) or (7,1).
 			 * Since the introduction of SplitUpdate, these queries will 
			 * send multiple requests to delete the same tuple. Therefore, 
			 * in order to avoid a non-deterministic output, 
			 * an error is reported in such scenario.
 			 */
			if (isUpdate)
			{

				ereport(ERROR,
					(errcode(ERRCODE_IN_FAILED_SQL_TRANSACTION ),
					errmsg("multiple updates to a row by the same query is not allowed")));
			}

			return;

		case HeapTupleMayBeUpdated:
			break;

		case HeapTupleUpdated:
			if (IsXactIsoLevelSerializable)
				ereport(ERROR,
						(errcode(ERRCODE_T_R_SERIALIZATION_FAILURE),
						 errmsg("could not serialize access due to concurrent update")));
			else if (!ItemPointerEquals(tupleid, &update_ctid))
			{
				TupleTableSlot *epqslot;

				epqslot = EvalPlanQual(estate,
									   resultRelInfo->ri_RangeTableIndex,
									   &update_ctid,
									   update_xmax,
									   estate->es_snapshot->curcid);
				if (!TupIsNull(epqslot))
				{
					*tupleid = update_ctid;
					goto ldelete;
				}
			}
			/* tuple already deleted; nothing to do */
			return;

		default:
			elog(ERROR, "unrecognized heap_delete status: %u", result);
			return;
	}

	if (!isUpdate && !isNativeOrc)
	{
		IncrDeleted();
		(estate->es_processed)++;
	}

	/*
	 * Note: Normally one would think that we have to delete index tuples
	 * associated with the heap tuple now...
	 *
	 * ... but in POSTGRES, we have no need to do this because VACUUM will
	 * take care of it later.  We can't delete index tuples immediately
	 * anyway, since the tuple is still visible to other transactions.
	 */


	if (planGen == PLANGEN_PLANNER)
	{
		/* AFTER ROW DELETE Triggers */
		ExecARDeleteTriggers(estate, resultRelInfo, tupleid);
	}
}

/* ----------------------------------------------------------------
 *		ExecUpdate
 *
 *		note: we can't run UPDATE queries with transactions
 *		off because UPDATEs are actually INSERTs and our
 *		scan will mistakenly loop forever, updating the tuple
 *		it just inserted..	This should be fixed but until it
 *		is, we don't want to get stuck in an infinite loop
 *		which corrupts your database..
 * ----------------------------------------------------------------
 */
void
ExecUpdate(TupleTableSlot *slot,
		   ItemPointer tupleid,
		   Datum segmentid,
		   TupleTableSlot *planSlot,
		   DestReceiver *dest,
		   EState *estate)
{
	HeapTuple	tuple;
	ResultRelInfo *resultRelInfo;
	Relation	resultRelationDesc;
	HTSU_Result result;
	ItemPointerData update_ctid;
	TransactionId update_xmax;

	/*
	 * abort the operation if not running transactions
	 */
	if (IsBootstrapProcessingMode())
		elog(ERROR, "cannot UPDATE during bootstrap");

	/*
	 * get the heap tuple out of the tuple table slot, making sure we have a
	 * writable copy
	 */
	tuple = ExecFetchSlotHeapTuple(slot);

	/*
	 * get information on the (current) result relation
	 */
	resultRelInfo = estate->es_result_relation_info;
	resultRelationDesc = resultRelInfo->ri_RelationDesc;

	/* see if this update would move the tuple to a different partition */
	if (estate->es_result_partitions)
	{
		AttrNumber max_attr;
		Datum *values;
		bool *nulls;
		Oid targetid;

		Assert(estate->es_partition_state != NULL &&
			   estate->es_partition_state->accessMethods != NULL);
		if (!estate->es_partition_state->accessMethods->part_cxt)
			estate->es_partition_state->accessMethods->part_cxt =
				GetPerTupleExprContext(estate)->ecxt_per_tuple_memory;

		Assert(PointerIsValid(estate->es_result_partitions));

		max_attr = estate->es_partition_state->max_partition_attr;

		slot_getsomeattrs(slot, max_attr);
		values = slot_get_values(slot);
		nulls = slot_get_isnull(slot);

		targetid = selectPartition(estate->es_result_partitions, values,
								   nulls, slot->tts_tupleDescriptor,
								   estate->es_partition_state->accessMethods);

		if (!OidIsValid(targetid))
			ereport(ERROR,
					(errcode(ERRCODE_NO_PARTITION_FOR_PARTITIONING_KEY),
					 errmsg("no partition for partitioning key")));

		if (RelationGetRelid(resultRelationDesc) != targetid)
		{
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("moving tuple from partition \"%s\" to "
							"partition \"%s\" not supported",
							get_rel_name(RelationGetRelid(resultRelationDesc)),
							get_rel_name(targetid)),
					 errOmitLocation(true)));
		}
	}

	/* BEFORE ROW UPDATE Triggers */
	if (resultRelInfo->ri_TrigDesc &&
		resultRelInfo->ri_TrigDesc->n_before_row[TRIGGER_EVENT_UPDATE] > 0)
	{
		HeapTuple	newtuple;

		newtuple = ExecBRUpdateTriggers(estate, resultRelInfo,
										tupleid, tuple,
										estate->es_snapshot->curcid);

		if (newtuple == NULL)	/* "do nothing" */
			return;

		if (newtuple != tuple)	/* modified by Trigger(s) */
		{
			/*
			 * Put the modified tuple into a slot for convenience of routines
			 * below.  We assume the tuple was allocated in per-tuple memory
			 * context, and therefore will go away by itself. The tuple table
			 * slot should not try to clear it.
			 */
			TupleTableSlot *newslot = estate->es_trig_tuple_slot;

			if (newslot->tts_tupleDescriptor != slot->tts_tupleDescriptor)
				ExecSetSlotDescriptor(newslot, slot->tts_tupleDescriptor);
			ExecStoreGenericTuple(newtuple, newslot, false);
            newslot->tts_tableOid = slot->tts_tableOid; /* for constraints */
			slot = newslot;
			tuple = newtuple;
		}
	}

	/*
	 * Check the constraints of the tuple
	 *
	 * If we generate a new candidate tuple after EvalPlanQual testing, we
	 * must loop back here and recheck constraints.  (We don't need to redo
	 * triggers, however.  If there are any BEFORE triggers then trigger.c
	 * will have done heap_lock_tuple to lock the correct tuple, so there's no
	 * need to do them again.)
	 */
lreplace:;
	if (resultRelationDesc->rd_att->constr)
		ExecConstraints(resultRelInfo, slot, estate);

	bool isExternalTable = false;
	bool isNativeOrc = false;
	if (!GpPersistent_IsPersistentRelation(resultRelationDesc->rd_id))
	{
		/*
		 * Normal UPDATE path.
		 */

		bool isHeapTable = RelationIsHeap(resultRelationDesc);
		isExternalTable = RelationIsExternal(resultRelationDesc);
		isNativeOrc = RelationIsOrc(resultRelationDesc);

		/*
		 * replace the heap tuple
		 *
		 * Note: if es_crosscheck_snapshot isn't InvalidSnapshot, we check that
		 * the row to be updated is visible to that snapshot, and throw a can't-
		 * serialize error if not.	This is a special-case behavior needed for
		 * referential integrity updates in serializable transactions.
		 */
		if (isHeapTable)
		{
			result = heap_update(resultRelationDesc, tupleid, tuple,
			                     &update_ctid, &update_xmax,
			                     estate->es_snapshot->curcid,
			                     estate->es_crosscheck_snapshot,
			                     true /* wait for commit */ );
		}
		else if (isExternalTable)
		{
		    Assert(estate->es_ext_upd_oid_desc != NULL);

		    ExternalInsertDescHashEntry *extUpdDescEntry = NULL;
		    bool found = false;
		    extUpdDescEntry = (ExternalInsertDescHashEntry *)hash_search(estate->es_ext_upd_oid_desc,
		                                                                 (void *) (&(RelationGetRelid(resultRelationDesc))),
		                                                                 HASH_ENTER,
		                                                                 &found);

		    if (!found)
		    {
		        Assert(extUpdDescEntry != NULL);
		        /*
		         * Step 1: prepare ExternalInsertDescData which will be shared
		         * during beginupdate, update, and endupdate, if necessary
		         */
		        ExtTableEntry *extEntry =
		                GetExtTableEntry(RelationGetRelid(resultRelationDesc));

		        /* Get formatter type and name for the external table */
		        int formatterType = ExternalTableType_Invalid;
		        char *formatterName = NULL;

		        getExternalTableTypeStr(extEntry->fmtcode, extEntry->fmtopts,
		                                &formatterType, &formatterName);
		        pfree(extEntry);

		        Oid procOid = LookupMagmaFunction(formatterName, "beginupdate");
		        FmgrInfo procInfo;
		        if (OidIsValid(procOid))
		        {
		            fmgr_info(procOid, &procInfo);

		            /* Step 2: begin delete tuple */
		            extUpdDescEntry->ext_ins_desc =
		                    InvokeMagmaBeginUpdate(&procInfo,
		                                           resultRelationDesc,
		                                           estate->es_plannedstmt,
		                                           PlugStorageGetTransactionSnapshot(NULL));
		            elog(LOG, "exec update begin update: %d", extUpdDescEntry->ext_ins_oid);
		        }
		        else
		        {
		            elog(ERROR, "magma_beginupdate function was not found for pluggable storage");
		        }
		    }

		    /* Step 3: update tuple */
		    ExternalInsertDesc extUpdDesc = extUpdDescEntry->ext_ins_desc;
		    extUpdDesc->ext_rangeId = GetRangeIdFromCtidAndSegmentid(segmentid);
		    extUpdDesc->ext_rowId = GetRowIdFromCtidAndSegmentid(tupleid, segmentid);
		    estate->es_processed += InvokeMagmaUpdate(extUpdDesc->ext_ps_update_funcs.updates,
		                      extUpdDesc,
		                      planSlot);
			result = HeapTupleMayBeUpdated;
    } else if (isNativeOrc) {
      if (resultRelInfo->ri_orcUpdateDesc == NULL) {
        List *splits =
            GetFileSplitsOfSegment(estate->es_plannedstmt->scantable_splits,
                                   resultRelationDesc->rd_id, GetQEIndex());
        resultRelInfo->ri_orcUpdateDesc = orcBeginUpdate(
            resultRelationDesc, splits, estate->es_plannedstmt->relFileNodeInfo,
            false,
            isDirectDispatch(estate->es_plannedstmt->planTree));
      }
      resultRelInfo->ri_orcUpdateDesc->rowId =
          (Datum)(ItemPointerGetRowIdFromFakeCtid(tupleid));
      resultRelInfo->ri_orcUpdateDesc->slot = planSlot;
      orcUpdate(resultRelInfo->ri_orcUpdateDesc);
      result = HeapTupleMayBeUpdated;
    } else {
      Insist(0);
    }

    switch (result)
		{
			case HeapTupleSelfUpdated:
				/* already deleted by self; nothing to do */
				return;

			case HeapTupleMayBeUpdated:
				break;

			case HeapTupleUpdated:
				if (IsXactIsoLevelSerializable)
					ereport(ERROR,
							(errcode(ERRCODE_T_R_SERIALIZATION_FAILURE),
							 errmsg("could not serialize access due to concurrent update")));
				else if (!ItemPointerEquals(tupleid, &update_ctid))
				{
					TupleTableSlot *epqslot;

					epqslot = EvalPlanQual(estate,
										   resultRelInfo->ri_RangeTableIndex,
										   &update_ctid,
										   update_xmax,
										   estate->es_snapshot->curcid);
					if (!TupIsNull(epqslot))
					{
						*tupleid = update_ctid;
						slot = ExecFilterJunk(estate->es_junkFilter, epqslot);
						tuple = ExecFetchSlotHeapTuple(slot);
						goto lreplace;
					}
				}
				/* tuple already deleted; nothing to do */
				return;

			default:
				elog(ERROR, "unrecognized heap_update status: %u", result);
				return;
		}
	}
	else
	{
		HeapTuple persistentTuple;

		/*
		 * Persistent metadata path.
		 */
		persistentTuple = heap_copytuple(tuple);
		persistentTuple->t_self = *tupleid;

		frozen_heap_inplace_update(resultRelationDesc, persistentTuple);

		heap_freetuple(persistentTuple);
	}

	IncrReplaced();
	if (!isExternalTable && !isNativeOrc)
	 (estate->es_processed)++;

	/*
	 * Note: instead of having to update the old index tuples associated with
	 * the heap tuple, all we do is form and insert new index tuples. This is
	 * because UPDATEs are actually DELETEs and INSERTs, and index tuple
	 * deletion is done later by VACUUM (see notes in ExecDelete).	All we do
	 * here is insert new index tuples.  -cim 9/27/89
	 */
	/*
	 * insert index entries for tuple
	 *
	 * Note: heap_update returns the tid (location) of the new tuple in the
	 * t_self field.
	 */
	if (resultRelInfo->ri_NumIndices > 0)
		ExecInsertIndexTuples(slot, &(tuple->t_self), estate, false);

	/* AFTER ROW UPDATE Triggers */
	ExecARUpdateTriggers(estate, resultRelInfo, tupleid, tuple);

}

extern Oid
LookupMagmaFunction(char *formatter, char *func)
{
	List*	funcname	= NIL;
	Oid		procOid		= InvalidOid;
	Oid		argList[1];
	Oid		returnOid;

	elog(DEBUG3, "find magma function for %s_%s", formatter, func);

	char *new_func_name = (char *)palloc0(strlen(formatter)+strlen(func) + 2);
	sprintf(new_func_name, "%s_%s", formatter, func);
	funcname = lappend(funcname, makeString(new_func_name));

	returnOid = VOIDOID;
	procOid = LookupFuncName(funcname, 0, argList, true);

	pfree(new_func_name);
	list_free_deep(funcname);

	return procOid;
}

extern ExternalInsertDesc
InvokeMagmaBeginDelete(FmgrInfo *func,
                       Relation relation,
                       PlannedStmt *plannedstmt,
                       MagmaSnapshot *snapshot)
{
	PlugStorageData psdata;
	FunctionCallInfoData fcinfo;

	psdata.type = T_PlugStorageData;
	psdata.ps_relation = relation;
	psdata.ps_magma_splits =
	    GetFileSplitsOfSegmentMagma(plannedstmt->scantable_splits, relation->rd_id);
	/*
	psdata.ps_magma_splits =
	    GetFileSplitsOfSegment(plannedstmt->scantable_splits, relation->rd_id, GetQEIndex());
	*/

	GetMagmaSchemaByRelid(plannedstmt->scantable_splits, relation->rd_id,
	                      &(psdata.ps_magma_serializeSchema),
	                      &(psdata.ps_magma_serializeSchemaLen));
    psdata.ps_snapshot.currentTransaction.txnId = 0;
    psdata.ps_snapshot.currentTransaction.txnStatus = 0;
    psdata.ps_snapshot.txnActions.txnActions = NULL;
    psdata.ps_snapshot.txnActions.txnActionSize = 0;
    psdata.ps_snapshot.txnActions.txnActionStartOffset = 0;

    if (snapshot != NULL)
    {
        // save current transaction in snapshot
        psdata.ps_snapshot.currentTransaction.txnId =
            snapshot->currentTransaction.txnId;
        psdata.ps_snapshot.currentTransaction.txnStatus =
            snapshot->currentTransaction.txnStatus;
        psdata.ps_snapshot.cmdIdInTransaction = snapshot->cmdIdInTransaction;

        // allocate txnActions
        psdata.ps_snapshot.txnActions.txnActionStartOffset =
            snapshot->txnActions.txnActionStartOffset;
        psdata.ps_snapshot.txnActions.txnActions = (MagmaTxnAction *)palloc0(
            sizeof(MagmaTxnAction) * snapshot->txnActions.txnActionSize);

        // save txnActionsp
        psdata.ps_snapshot.txnActions.txnActionSize =
            snapshot->txnActions.txnActionSize;
        for (int i = 0; i < snapshot->txnActions.txnActionSize; ++i) {
          psdata.ps_snapshot.txnActions.txnActions[i].txnId =
              snapshot->txnActions.txnActions[i].txnId;
          psdata.ps_snapshot.txnActions.txnActions[i].txnStatus =
              snapshot->txnActions.txnActions[i].txnStatus;
        }
    }

	InitFunctionCallInfoData(fcinfo,  // FunctionCallInfoData
	                         func,    // FmgrInfo
	                         0,       // nArgs
	                         (Node *)(&psdata), // Call Context
	                         NULL);             // ResultSetInfo

	// Invoke function
	FunctionCallInvoke(&fcinfo);

	// Free snapshot memory
	pfree(psdata.ps_snapshot.txnActions.txnActions);

	// We do not expect a null result
	if (fcinfo.isnull)
	{
		elog(ERROR, "function %u returned NULL", fcinfo.flinfo->fn_oid);
	}

	return psdata.ps_ext_delete_desc;
}

extern void
InvokeMagmaDelete(FmgrInfo *func,
                        ExternalInsertDesc extDelDesc,
                        TupleTableSlot *tupTableSlot)
{
	PlugStorageData psdata;
	FunctionCallInfoData fcinfo;

	psdata.type = T_PlugStorageData;
	psdata.ps_ext_delete_desc = extDelDesc;
	psdata.ps_tuple_table_slot = tupTableSlot;

	InitFunctionCallInfoData(fcinfo,  // FunctionCallInfoData
	                         func,    // FmgrInfo
	                         0,       // nArgs
	                         (Node *)(&psdata), // Call Context
	                         NULL);             // ResultSetInfo

	// Invoke function
	FunctionCallInvoke(&fcinfo);

	// We do not expect a null result
	if (fcinfo.isnull)
	{
		elog(ERROR, "function %u returned NULL", fcinfo.flinfo->fn_oid);
	}

	return;
}

extern void
InvokeMagmaEndDelete(FmgrInfo *func,
                           ExternalInsertDesc extDelDesc)
{
	PlugStorageData psdata;
	FunctionCallInfoData fcinfo;

	psdata.type = T_PlugStorageData;
	psdata.ps_ext_delete_desc = extDelDesc;

	InitFunctionCallInfoData(fcinfo,  // FunctionCallInfoData
	                         func,    // FmgrInfo
	                         0,       // nArgs
	                         (Node *)(&psdata), // Call Context
	                         NULL);             // ResultSetInfo

	// Invoke function
	FunctionCallInvoke(&fcinfo);

	// We do not expect a null result
	if (fcinfo.isnull)
	{
		elog(ERROR, "function %u returned NULL", fcinfo.flinfo->fn_oid);
	}

	return;
}

extern ExternalInsertDesc
InvokeMagmaBeginUpdate(FmgrInfo *func,
                       Relation relation,
                       PlannedStmt *plannedstmt,
                       MagmaSnapshot *snapshot)
{
	PlugStorageData psdata;
	FunctionCallInfoData fcinfo;

	psdata.type = T_PlugStorageData;
	psdata.ps_relation = relation;
	psdata.ps_magma_splits =
	    GetFileSplitsOfSegmentMagma(plannedstmt->scantable_splits, relation->rd_id);
	/*
	psdata.ps_magma_splits =
	    GetFileSplitsOfSegment(plannedstmt->scantable_splits, relation->rd_id, GetQEIndex());
	*/
	GetMagmaSchemaByRelid(plannedstmt->scantable_splits, relation->rd_id,
	                      &(psdata.ps_magma_serializeSchema),
	                      &(psdata.ps_magma_serializeSchemaLen));

    psdata.ps_snapshot.currentTransaction.txnId = 0;
    psdata.ps_snapshot.currentTransaction.txnStatus = 0;
    psdata.ps_snapshot.txnActions.txnActions = NULL;
    psdata.ps_snapshot.txnActions.txnActionSize = 0;
    psdata.ps_snapshot.txnActions.txnActionStartOffset = 0;
    if (snapshot != NULL)
    {
        // save current transaction in snapshot
        psdata.ps_snapshot.currentTransaction.txnId =
            snapshot->currentTransaction.txnId;
        psdata.ps_snapshot.currentTransaction.txnStatus =
            snapshot->currentTransaction.txnStatus;
        psdata.ps_snapshot.cmdIdInTransaction = snapshot->cmdIdInTransaction;

        // allocate txnActions
        psdata.ps_snapshot.txnActions.txnActionStartOffset =
            snapshot->txnActions.txnActionStartOffset;
        psdata.ps_snapshot.txnActions.txnActions = (MagmaTxnAction *)palloc0(
            sizeof(MagmaTxnAction) * snapshot->txnActions.txnActionSize);

        // save txnActionsp
        psdata.ps_snapshot.txnActions.txnActionSize =
            snapshot->txnActions.txnActionSize;
        for (int i = 0; i < snapshot->txnActions.txnActionSize; ++i) {
          psdata.ps_snapshot.txnActions.txnActions[i].txnId =
              snapshot->txnActions.txnActions[i].txnId;
          psdata.ps_snapshot.txnActions.txnActions[i].txnStatus =
              snapshot->txnActions.txnActions[i].txnStatus;
        }
    }

	InitFunctionCallInfoData(fcinfo,  // FunctionCallInfoData
	                         func,    // FmgrInfo
	                         0,       // nArgs
	                         (Node *)(&psdata), // Call Context
	                         NULL);             // ResultSetInfo

	// Invoke function
	FunctionCallInvoke(&fcinfo);

	// Free snapshot memory
	pfree(psdata.ps_snapshot.txnActions.txnActions);

	// We do not expect a null result
	if (fcinfo.isnull)
	{
		elog(ERROR, "function %u returned NULL", fcinfo.flinfo->fn_oid);
	}

	return psdata.ps_ext_update_desc;
}

extern int
InvokeMagmaUpdate(FmgrInfo *func,
                        ExternalInsertDesc extUpdDesc,
                        TupleTableSlot *tupTableSlot)
{
	PlugStorageData psdata;
	FunctionCallInfoData fcinfo;

	psdata.type = T_PlugStorageData;
	psdata.ps_ext_update_desc = extUpdDesc;
	psdata.ps_tuple_table_slot = tupTableSlot;

	InitFunctionCallInfoData(fcinfo,  // FunctionCallInfoData
	                         func,    // FmgrInfo
	                         0,       // nArgs
	                         (Node *)(&psdata), // Call Context
	                         NULL);             // ResultSetInfo

	// Invoke function
	FunctionCallInvoke(&fcinfo);

	// We do not expect a null result
	if (fcinfo.isnull)
	{
		elog(ERROR, "function %u returned NULL", fcinfo.flinfo->fn_oid);
	}

	return psdata.ps_update_count;
}

extern int
InvokeMagmaEndUpdate(FmgrInfo *func,
                           ExternalInsertDesc extUpdDesc)
{
	PlugStorageData psdata;
	FunctionCallInfoData fcinfo;

	psdata.type = T_PlugStorageData;
	psdata.ps_ext_update_desc = extUpdDesc;

	InitFunctionCallInfoData(fcinfo,  // FunctionCallInfoData
	                         func,    // FmgrInfo
	                         0,       // nArgs
	                         (Node *)(&psdata), // Call Context
	                         NULL);             // ResultSetInfo

	// Invoke function
	FunctionCallInvoke(&fcinfo);

	// We do not expect a null result
	if (fcinfo.isnull)
	{
		elog(ERROR, "function %u returned NULL", fcinfo.flinfo->fn_oid);
	}

	return psdata.ps_update_count;
}

