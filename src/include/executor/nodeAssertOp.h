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
 * nodeAssertOp.h
 *	  Prototypes for nodeAssertOp.
 *
 *-------------------------------------------------------------------------
 */

#ifndef NODEASSERTOP_H
#define NODEASSERTOP_H

extern void ExecAssertOpExplainEnd(PlanState *planstate, struct StringInfoData *buf);
extern TupleTableSlot* ExecAssertOp(AssertOpState *node);
extern AssertOpState* ExecInitAssertOp(AssertOp *node, EState *estate, int eflags);
extern void ExecEndAssertOp(AssertOpState *node);
extern int ExecCountSlotsAssertOp(AssertOp *node);
extern void ExecReScanAssertOp(AssertOpState *node, ExprContext *exprCtxt);
extern void initGpmonPktForAssertOp(Plan *planNode, gpmon_packet_t *gpmon_pkt, EState *estate);

#endif   /* NODEASSERTOP_H */


