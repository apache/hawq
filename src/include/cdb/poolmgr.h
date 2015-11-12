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


#ifndef POOLMGR_H
#define POOLMGR_H

#include "utils/palloc.h"

struct PoolMgrState;
typedef void *PoolItem;
typedef void *PoolIterateArg;
typedef void (*PoolMgrCleanCallback) (PoolItem item);
typedef void (*PoolMgrIterateCallback) (PoolItem item, PoolIterateArg data);
typedef bool (*PoolMgrIterateFilter) (PoolItem item);

extern struct PoolMgrState *poolmgr_create_pool(MemoryContext ctx, PoolMgrCleanCallback callback);
extern bool poolmgr_drop_pool(struct PoolMgrState *pool);
extern PoolItem poolmgr_get_item_by_name(struct PoolMgrState *pool, const char *name);
extern PoolItem poolmgr_get_random_item(struct PoolMgrState *pool);
extern void poolmgr_put_item(struct PoolMgrState *pool, const char *name, PoolItem item);
extern void poolmgr_iterate(struct PoolMgrState *pool, PoolMgrIterateFilter filter, PoolMgrIterateCallback callback, PoolIterateArg data);

#endif	/* POOLMGR_H */

