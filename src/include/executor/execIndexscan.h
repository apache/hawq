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
 * execIndexScan.h
 */
#ifndef EXECINDEXSCAN_H
#define EXECINDEXSCAN_H

#include "nodes/execnodes.h"


extern void
InitCommonIndexScanState(IndexScanState *indexstate, IndexScan *node, EState *estate, int eflags);

extern Relation
OpenIndexRelation(EState *estate, Oid indexOid, Index tableRtIndex);

extern void
InitRuntimeKeysContext(IndexScanState *indexstate);

extern void
FreeRuntimeKeysContext(IndexScanState *indexstate);

extern void
ExecIndexBuildScanKeys(PlanState *planstate, Relation index,
					List *quals, List *strategies, List *subtypes,
					ScanKey *scanKeys, int *numScanKeys,
					IndexRuntimeKeyInfo **runtimeKeys, int *numRuntimeKeys,
					IndexArrayKeyInfo **arrayKeys, int *numArrayKeys);


#endif
