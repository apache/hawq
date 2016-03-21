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
 * execDML.h
 *	  Prototypes for execDML.
 *
 *-------------------------------------------------------------------------
 */

#ifndef EXECDML_H
#define EXECDML_H

/*
 * For a partitioned insert target only:
 * This type represents an entry in the per-part hash table stored at
 * estate->es_partition_state->result_partition_hash.   The table maps
 * part OID -> ResultRelInfo and avoids repeated calculation of the
 * result information.
 */
typedef struct ResultPartHashEntry
{
	Oid targetid; /* OID of part relation */
	int offset; /* Index ResultRelInfo in es_result_partitions */
} ResultPartHashEntry;


extern void
reconstructTupleValues(AttrMap *map,
					Datum *oldValues, bool *oldIsnull, int oldNumAttrs,
					Datum *newValues, bool *newIsnull, int newNumAttrs);

extern TupleTableSlot *
reconstructMatchingTupleSlot(TupleTableSlot *slot, ResultRelInfo *resultRelInfo);

extern void
ExecInsert(TupleTableSlot *slot,
		   DestReceiver *dest,
		   EState *estate,
		   PlanGenerator planGen,
		   bool isUpdate,
		   bool isInputSorted);

extern void
ExecDelete(ItemPointer tupleid,
		   TupleTableSlot *planSlot,
		   DestReceiver *dest,
		   EState *estate,
		   PlanGenerator planGen,
		   bool isUpdate);

extern void
ExecUpdate(TupleTableSlot *slot,
		   ItemPointer tupleid,
		   TupleTableSlot *planSlot,
		   DestReceiver *dest,
		   EState *estate);


#endif   /* EXECDML_H */

