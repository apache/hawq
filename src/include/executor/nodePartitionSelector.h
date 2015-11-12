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
 * nodePartitionSelector.h
 *	  implement the execution of PartitionSelector for selecting partition
 *	  Oids based on a given set of predicates. It works for both constant
 *	  partition elimination and join partition elimination
 *
 *-------------------------------------------------------------------------
 */

#ifndef NODEPARTITIONSELECTOR_H
#define NODEPARTITIONSELECTOR_H

extern TupleTableSlot* ExecPartitionSelector(PartitionSelectorState *node);
extern PartitionSelectorState* ExecInitPartitionSelector(PartitionSelector *node, EState *estate, int eflags);
extern void ExecEndPartitionSelector(PartitionSelectorState *node);
extern void ExecReScanPartitionSelector(PartitionSelectorState *node, ExprContext *exprCtxt);
extern int ExecCountSlotsPartitionSelector(PartitionSelector *node);
extern void initGpmonPktForPartitionSelector(Plan *planNode, gpmon_packet_t *gpmon_pkt, EState *estate);

#endif   /* NODEPARTITIONSELECTOR_H */

