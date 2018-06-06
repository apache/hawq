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

#ifndef __VEXECUTOR_H__
#define __VEXECUTOR_H__

#include "postgres.h"
#include "fmgr.h"
#include "executor/executor.h"
#include "vtype.h"
#include "vtype_ext.h"
#include "vagg.h"

/* Function declarations for extension loading and unloading */
extern void _PG_init(void);
extern void _PG_fini(void);

extern bool HasVecExecOprator(Plan *plan);
extern void BackportTupleDescriptor(PlanState* ps,TupleDesc td);

#endif
