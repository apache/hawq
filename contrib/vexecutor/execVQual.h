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
#ifndef __EXEC_VQUAL_H___
#define __EXEC_VQUAL_H___
#include "postgres.h"

#include "access/heapam.h"
#include "cdb/cdbvars.h"
#include "cdb/partitionselection.h"
#include "executor/execdebug.h"
#include "executor/nodeAgg.h"
#include "tuplebatch.h"

extern TupleTableSlot *
ExecVProject(ProjectionInfo *projInfo, ExprDoneCond *isDone);

extern vbool*
ExecVQual(List *qual, ExprContext *econtext, bool resultForNull);

extern bool
VirtualNodeProc(TupleTableSlot *slot);

extern ExprState *
VExecInitExpr(Expr *node, PlanState *parent);

#endif
