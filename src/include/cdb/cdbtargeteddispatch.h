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
 * The targeted dispatch code will use query information to assign content ids
 *    to the root node of a plan, when that information is calculatable.  
 *
 * For example, in the query select * from t1 where t1.distribution_key = 100
 *
 * Then it is known that t1.distribution_key must equal 100, and so the content
 *    id can be calculated from that.
 *
 * See MPP-6939 for more information.
 *
 *
 *-------------------------------------------------------------------------
 */
#ifndef CDBTARGETEDDISPATCH_H
#define CDBTARGETEDDISPATCH_H

#include "nodes/plannodes.h"
#include "nodes/parsenodes.h"
#include "nodes/relation.h"

/**
 * @param query the query that produced the given plan
 * @param plan the plan to augment with directDispatch info (in its directDispatch field)
 */
extern void AssignContentIdsToPlanData(Query *query, Plan *plan, PlannerInfo *root);

#endif   /* CDBTARGETEDDISPATCH_H */
