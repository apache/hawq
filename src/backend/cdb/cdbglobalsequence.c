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
 * cdbglobalsequence.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"
#include "miscadmin.h"
#include "pgstat.h"
#include "utils/palloc.h"

#include "cdb/cdbglobalsequence.h"

#include "catalog/catalog.h"
#include "catalog/pg_namespace.h"
#include "catalog/pg_authid.h"
#include "catalog/pg_tablespace.h"
#include "catalog/pg_database.h"
#include "catalog/gp_persistent.h"
#include "cdb/cdbdirectopen.h"

#include "storage/itemptr.h"
#include "access/genam.h"
#include "access/heapam.h"
#include "access/transam.h"
#include "utils/guc.h"
#include "storage/smgr.h"
#include "storage/ipc.h"

static void GlobalSequence_MakeTid(
	GpGlobalSequence		gpGlobalSequence,

	ItemPointer 			globalSequenceTid)
				/* TID of the sequence counter tuple. */
{
	/*
	 * For now, everything is in block 0.
	 */
	ItemPointerSet(globalSequenceTid, 0, gpGlobalSequence);
}

static void GlobalSequence_UpdateTuple(
	GpGlobalSequence		gpGlobalSequence,

	int64					newSequenceNum)

{
	Relation	gpGlobalSequenceRel;
	bool 		nulls[Anum_gp_global_sequence_sequence_num];
	Datum 		values[Anum_gp_global_sequence_sequence_num];
	HeapTuple	globalSequenceTuple = NULL;

	MemSet(nulls, 0 , sizeof(nulls));
	
	GpGlobalSequence_SetDatumValues(
								values,
								newSequenceNum);
	
	gpGlobalSequenceRel = 
				DirectOpen_GpGlobalSequenceOpenShared();
		
	/*
	 * Form the tuple.
	 */
	globalSequenceTuple = heap_form_tuple(
									gpGlobalSequenceRel->rd_att, 
									values, 
									nulls);
	if (!HeapTupleIsValid(globalSequenceTuple))
		elog(ERROR, "Failed to build global sequence tuple");

	GlobalSequence_MakeTid(
						gpGlobalSequence,
						&globalSequenceTuple->t_self);
		
	frozen_heap_inplace_update(gpGlobalSequenceRel, globalSequenceTuple);

	/* MPP-17181 : we need to persistently sync the newly allocated 
	 * sequence numbers to disk before returning them.
	 */
	FlushRelationBuffers(gpGlobalSequenceRel);
	smgrimmedsync(gpGlobalSequenceRel->rd_smgr);

	heap_freetuple(globalSequenceTuple);

	DirectOpen_GpGlobalSequenceClose(gpGlobalSequenceRel);
}

static void GlobalSequence_ReadTuple(
	GpGlobalSequence		gpGlobalSequence,

	int64					*currentSequenceNum)
{
	Relation	gpGlobalSequenceRel;
	bool 		nulls[Anum_gp_global_sequence_sequence_num];
	Datum 		values[Anum_gp_global_sequence_sequence_num];

	HeapTupleData 	globalSequenceTuple;
	Buffer			buffer;

	gpGlobalSequenceRel = 
				DirectOpen_GpGlobalSequenceOpenShared();

	GlobalSequence_MakeTid(
						gpGlobalSequence,
						&globalSequenceTuple.t_self);
	
	if (!heap_fetch(gpGlobalSequenceRel, SnapshotAny,
					&globalSequenceTuple, &buffer, false, NULL))
		elog(ERROR, "Failed to fetch global sequence tuple at %s",
			 ItemPointerToString(&globalSequenceTuple.t_self));

	heap_deform_tuple(
				&globalSequenceTuple, 
				gpGlobalSequenceRel->rd_att, 
				values, 
				nulls);

	GpGlobalSequence_GetValues(
							values,
							currentSequenceNum);

	ReleaseBuffer(buffer);
	
	DirectOpen_GpGlobalSequenceClose(gpGlobalSequenceRel);
}

int64 GlobalSequence_Next(
	GpGlobalSequence		gpGlobalSequence)
{
	int64 sequenceNum;

	GlobalSequence_ReadTuple(gpGlobalSequence, &sequenceNum);
	GlobalSequence_UpdateTuple(gpGlobalSequence, ++sequenceNum);

	return sequenceNum;
}

int64 GlobalSequence_NextInterval(
	GpGlobalSequence		gpGlobalSequence,

	int64					interval)
{
	int64 sequenceNum;

	GlobalSequence_ReadTuple(gpGlobalSequence, &sequenceNum);
	GlobalSequence_UpdateTuple(gpGlobalSequence, ++sequenceNum + interval - 1);

	return sequenceNum;
}

int64 GlobalSequence_Current(
	GpGlobalSequence		gpGlobalSequence)
{
	int64 sequenceNum;

	GlobalSequence_ReadTuple(gpGlobalSequence, &sequenceNum);

	return sequenceNum;
}

void GlobalSequence_Set(
	GpGlobalSequence		gpGlobalSequence,

	int64					newSequenceNum)
{
	GlobalSequence_UpdateTuple(gpGlobalSequence, newSequenceNum);
}
