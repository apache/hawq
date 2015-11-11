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

#ifndef HAWQ_RESOURCE_MANAGER_RESOURCE_BROKER_NONE_H
#define HAWQ_RESOURCE_MANAGER_RESOURCE_BROKER_NONE_H
#include "envswitch.h"
#include "resourcebroker_API.h"
#include "dynrm.h"

/* Acquire and return resource. */
int RB_NONE_acquireResource(uint32_t memorymb, uint32_t core, List *preferred);
int RB_NONE_returnResource(List **containers);
void RB_NONE_createEntries(RB_FunctionEntries entries);
void RB_NONE_handleError(int errorcode);
#endif /* HAWQ_RESOURCE_MANAGER_RESOURCE_BROKER_NONE_H */
