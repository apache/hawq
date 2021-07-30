/* 
 * planshare.h
 * 		Prototypes and data structures for plan sharing
 *
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
 *
 */

#ifndef _PLANSHARE_H_
#define _PLANSHARE_H_

#include "nodes/plannodes.h"


extern List *share_plan(PlannerInfo *root, Plan *common, int numpartners);
extern Cost cost_share_plan(Plan *common, PlannerInfo *root, int numpartners);

extern Plan *prepare_plan_for_sharing(PlannerInfo *root, Plan *common);
extern Plan *share_prepared_plan(PlannerInfo *root, Plan *common);


#endif /* _PLANSHARE_H_ */

