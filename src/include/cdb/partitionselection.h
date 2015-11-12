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

/*--------------------------------------------------------------------------
 *
 * partitionselection.h
 *	 Definitions and API functions for partitionselection.c
 *
 *
 *--------------------------------------------------------------------------
 */
#ifndef PARTITIONSELECTION_H
#define PARTITIONSELECTION_H

#include "executor/tuptable.h"
#include "nodes/relation.h"
#include "nodes/execnodes.h"

/* ----------------
 * PartitionConstraints node
 * used during PartitionSelector
 * ----------------
 */
typedef struct PartitionConstraints
{
	NodeTag type;
	PartitionRule * pRule;
	bool defaultPart;
	Const *lowerBound;
	Const *upperBound;
	bool lbInclusive;
	bool upInclusive;
	bool lbOpen;
	bool upOpen;
} PartitionConstraints;

/* ----------------
 * SelectedParts node
 * This is the result of partition selection. It has a list of leaf part oids
 * and the corresponding ScanIds to which they should be propagated
 * ----------------
 */
typedef struct SelectedParts
{
	List *partOids;
	List *scanIds;
} SelectedParts;

extern PartitionSelectorState *initPartitionSelection(bool isRunTime, PartitionSelector *node, EState *estate);
extern void getPartitionNodeAndAccessMethod(Oid rootOid, List *partsMetadata, MemoryContext memoryContext,
											PartitionNode **partsAndRules, PartitionAccessMethods **accessMethods);
extern SelectedParts *processLevel(PartitionSelectorState *node, int level, TupleTableSlot *inputTuple);
extern SelectedParts *static_part_selection(PartitionSelector *ps);

#endif   /* PARTITIONSELECTION_H */
