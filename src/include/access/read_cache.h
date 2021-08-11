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
///////////////////////////////////////////////////////////////////////////////

#ifndef ACCESS_READ_CACHE_H
#define ACCESS_READ_CACHE_H

#include "nodes/plannodes.h"
#include "nodes/relation.h"

extern void InitReadCache(void);
extern Size ReadCacheShmemSize(void);

extern void ReadCacheHashEntryReviseOnCommit(Oid relid, bool dropTable);
extern void AtEOXact_ReadCache(bool commit);

extern void resetReadCache(bool enable);
extern void initReadCache(PlannedStmt *plannedStmt,const char *sourceText);
extern bool readCacheEnabled();
extern bool readCacheEof();
extern char *readCache(int32_t *len);
extern bool writeCacheEnabled();
extern void writeCache(const char *buf, int32_t size);
extern void commitWriteCache();

#endif  // ACCESS_READ_CACHE_H
