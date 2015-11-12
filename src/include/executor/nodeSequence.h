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
 * nodeSequence.h
 *    header file for nodeSequence.c.
 *
 */
#ifndef NODESEQUENCE_H
#define NODESEQUENCE_H

#include "executor/tuptable.h"
#include "nodes/execnodes.h"

extern SequenceState *ExecInitSequence(Sequence *node, EState *estate, int eflags);
extern TupleTableSlot *ExecSequence(SequenceState *node);
extern void ExecReScanSequence(SequenceState *node, ExprContext *exprCtxt);
extern void ExecEndSequence(SequenceState *node);
extern int ExecCountSlotsSequence(Sequence *node);

enum 
{
	GPMON_SEQUENCE_CURRTABLE = GPMON_QEXEC_M_NODE_START,
	GPMON_SEQUENCE_TOTAL
};

static inline gpmon_packet_t * GpmonPktFromSequenceState(SequenceState *node)
{
	return &node->ps.gpmon_pkt;
}

#endif
