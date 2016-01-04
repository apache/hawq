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
 * nodeHash.h
 *	  prototypes for nodeHash.c
 *
 *
 * Portions Copyright (c) 2007-2008, Greenplum inc
 * Portions Copyright (c) 1996-2008, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * $PostgreSQL: pgsql/src/include/executor/nodeHash.h,v 1.41 2006/06/27 21:31:20 tgl Exp $
 *
 *-------------------------------------------------------------------------
 */
#ifndef NODEHASH_H
#define NODEHASH_H

#include "nodes/execnodes.h"
#include "executor/hashjoin.h"  /* for HJTUPLE_OVERHEAD */
#include "access/memtup.h"

extern int	ExecCountSlotsHash(Hash *node);
extern HashState *ExecInitHash(Hash *node, EState *estate, int eflags);
extern struct TupleTableSlot *ExecHash(HashState *node);
extern Node *MultiExecHash(HashState *node);
extern void ExecEndHash(HashState *node);
extern void ExecReScanHash(HashState *node, ExprContext *exprCtxt);

extern HashJoinTable ExecHashTableCreate(HashState *hashState, HashJoinState *hjstate, List *hashOperators, uint64 operatorMemKB, workfile_set * sfs);
extern void ExecHashTableDestroy(HashState *hashState, HashJoinTable hashtable);
extern void ExecHashTableInsert(HashState *hashState, HashJoinTable hashtable,
					struct TupleTableSlot *slot,
					uint32 hashvalue);
extern bool ExecHashGetHashValue(HashState *hashState, HashJoinTable hashtable,
					 ExprContext *econtext,
					 List *hashkeys,
					 bool keep_nulls,
					 uint32 *hashvalue,
					 bool *hashkeys_null);
extern void ExecHashGetBucketAndBatch(HashJoinTable hashtable,
						  uint32 hashvalue,
						  int *bucketno,
						  int *batchno);
extern HashJoinTuple ExecScanHashBucket(HashState *hashState, HashJoinState *hjstate,
				   ExprContext *econtext);
extern void ExecHashTableReset(HashState *hashState, HashJoinTable hashtable);
extern void ExecHashTableExplainInit(HashState *hashState, HashJoinState *hjstate,
                                     HashJoinTable  hashtable);
extern void ExecHashTableExplainBatchEnd(HashState *hashState, HashJoinTable hashtable);

enum 
{
	GPMON_HASH_SPILLBATCH = GPMON_QEXEC_M_NODE_START,
	GPMON_HASH_SPILLTUPLE,
	GPMON_HASH_SPILLBYTE,
	GPMON_HASH_TOTAL,
};

static inline gpmon_packet_t * GpmonPktFromHashState(HashState *node)
{
	return &node->ps.gpmon_pkt;
}

static inline int
ExecHashRowSize(int tupwidth)
{
    return HJTUPLE_OVERHEAD +
		MAXALIGN(sizeof(MemTupleData)) +
		MAXALIGN(tupwidth);
}                               /* ExecHashRowSize */
#endif   /* NODEHASH_H */
