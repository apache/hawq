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
 * tuplestorenew.h
 * 	A better tuple store
 */

#ifndef TUPSTORE_NEW_H
#define TUPSTORE_NEW_H

#include "executor/tuptable.h"
#include "utils/workfile_mgr.h"

typedef struct NTupleStorePos
{
	long blockn;
	int slotn;
} NTupleStorePos;

/* Opaque data types */
typedef struct NTupleStoreAccessor NTupleStoreAccessor;
typedef struct NTupleStore NTupleStore;

/* Instrument tuple store 
 * Caller must ensure ins ptr remain valid during the lifetype of the tuple store 
 */
void ntuplestore_setinstrument(NTupleStore* ts, struct Instrumentation *ins);

/* Tuple store method */
extern NTupleStore *ntuplestore_create(int maxBytes);
extern NTupleStore *ntuplestore_create_readerwriter(const char* filename, int maxBytes, bool isWriter);
extern NTupleStore *ntuplestore_create_workset(workfile_set *workSet, bool cachedWorkfilesFound, int maxBytes);
extern bool ntuplestore_is_readerwriter_reader(NTupleStore* nts);
extern bool ntuplestore_is_readerwriter_writer(NTupleStore* nts);
extern bool ntuplestore_is_readerwriter(NTupleStore* nts);
extern void ntuplestore_reset(NTupleStore *ts);
extern void ntuplestore_flush(NTupleStore *ts);
extern void ntuplestore_destroy(NTupleStore *ts);
extern void ntuplestore_trim(NTupleStore* ts, NTupleStorePos *pos);
extern int ntuplestore_compare_pos(NTupleStore *ts, NTupleStorePos *pos1, NTupleStorePos *pos2);

/* Tuple store accessor method 
 * Create Accessor: current we support 1 writer, many reader per store.  After created, the accessor
 * is positioned at the first tuple or eof (if there is no tuple).
 */
extern NTupleStoreAccessor *ntuplestore_create_accessor(NTupleStore *ts, bool isWriter);
extern void ntuplestore_destroy_accessor(NTupleStoreAccessor *acc);

/* Put slot/data automatically postion the accessor to the last entry */
extern void ntuplestore_acc_put_tupleslot(NTupleStoreAccessor *tsa, TupleTableSlot *slot);
extern void ntuplestore_acc_put_data(NTupleStoreAccessor *tsa, void *data, int len);

/* return true if sucess, false if beyond last (first) valid position */
extern bool ntuplestore_acc_advance(NTupleStoreAccessor *tsa, int n);

/* Get data.  The slot/pointer returned is guaranteed to be valid till the accessor
 * call advance.
 * NOTE: trim may make current postion of an accessor invalid.  It is caller's reponsibilty
 * to make sure trim does not trim too far ahead
 */
extern bool ntuplestore_acc_current_tupleslot(NTupleStoreAccessor *tsa, TupleTableSlot *slot);
extern bool ntuplestore_acc_current_data(NTupleStoreAccessor *tsa, void **data, int *len);

/* Tell/seek postion of accessor. */

/* Tell fill in the pos, return false if accessor points to an invalid position.
 * Caller can pass in pos = NULL to simply tell if the accessor is at a valid postion.
 */
extern bool ntuplestore_acc_tell(NTupleStoreAccessor *tsa, NTupleStorePos *pos);

/* put the current row at a certain pos (or first/last).  Return false pos is invalid */
extern bool ntuplestore_acc_seek(NTupleStoreAccessor *tsa, NTupleStorePos *pos);
extern bool ntuplestore_acc_seek_first(NTupleStoreAccessor *tsa);
extern bool ntuplestore_acc_seek_last(NTupleStoreAccessor *tsa);
extern void ntuplestore_acc_seek_bof(NTupleStoreAccessor *tsa);
extern void ntuplestore_acc_seek_eof(NTupleStoreAccessor *tsa);

extern int ntuplestore_count_slot(NTupleStore *nts, NTupleStorePos *pos1, NTupleStorePos *pos2);
extern int ntuplestore_count_slot_acc(NTupleStore *nts, NTupleStoreAccessor* tsa1, NTupleStoreAccessor *tsa2);
extern void  ntuplestore_acc_set_invalid(NTupleStoreAccessor *tsa);
extern bool ntuplestore_acc_is_before(NTupleStoreAccessor *tsa1, NTupleStoreAccessor *tsa2);
/* workfile set functions */
extern void ntuplestore_mark_workset_complete(NTupleStore *nts);
extern bool ntuplestore_created_reusable_workfiles(NTupleStore *nts);
#endif /* TUPSTORE_NEW_H */
