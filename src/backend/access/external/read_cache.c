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

#include "postgres.h"
#include "miscadmin.h"

#include "access/read_cache.h"
#include "cdb/cdbvars.h"
#include "optimizer/clauses.h"
#include "optimizer/newPlanner.h"
#include "storage/shmem.h"
#include "utils/hsearch.h"

#include "executor/cwrapper/cached-result.h"

#define READ_CACHE_SIZE (102400)

// oid->revision num
static HTAB *ReadCacheHash = NULL;
typedef struct ReadCacheHashData {
  Oid relid;
  int64_t revisionNum;
} ReadCacheHashData;

static HTAB *ReadCacheHashEntryPendingRevise = NULL;

typedef struct ReadCacheHashEntryPendingReviseData {
  Oid relid;
  bool drop;
  int64_t count;
} ReadCacheHashEntryPendingReviseData;

static int64_t ReadCacheLookupHashEntry(Oid relid);

void InitReadCache(void) {
  HASHCTL info;
  int hash_flags;

  MemSet(&info, 0, sizeof(info));

  info.keysize = sizeof(Oid);
  info.entrysize = sizeof(ReadCacheHashData);
  info.hash = tag_hash;
  hash_flags = (HASH_ELEM | HASH_FUNCTION);

  ReadCacheHash = ShmemInitHash("Read Cache", READ_CACHE_SIZE, READ_CACHE_SIZE,
                                &info, hash_flags);

  if (!ReadCacheHash)
    ereport(FATAL, (errcode(ERRCODE_OUT_OF_MEMORY),
                    errmsg("not enough shared memory for read cache init")));

  CacheFileCleanUp();
}

Size ReadCacheShmemSize(void) {
  return hash_estimate_size((Size)READ_CACHE_SIZE, sizeof(ReadCacheHashData));
}

void ReadCacheHashEntryReviseOnCommit(Oid relid, bool dropTable) {
  if (Gp_role != GP_ROLE_DISPATCH) return;

  if (NULL == ReadCacheHashEntryPendingRevise) {
    HASHCTL info;
    int hash_flags;

    MemSet(&info, 0, sizeof(info));

    info.keysize = sizeof(Oid);
    info.entrysize = sizeof(ReadCacheHashEntryPendingReviseData);
    info.hash = tag_hash;
    hash_flags = (HASH_ELEM | HASH_FUNCTION);

    ReadCacheHashEntryPendingRevise =
        hash_create("ReadCacheHashEntryPendingRevise", 10, &info, hash_flags);

    if (!ReadCacheHashEntryPendingRevise)
      elog(ERROR, "failed to create ReadCacheHashEntryPendingRevise");
  }

  bool found;
  ReadCacheHashEntryPendingReviseData *node =
      (ReadCacheHashEntryPendingReviseData *)hash_search(
          ReadCacheHashEntryPendingRevise, (void *)&relid, HASH_ENTER, &found);

  if (found) {
    if (dropTable) node->drop = true;
    ++node->count;
  } else {
    node->relid = relid;
    node->drop = false;
    node->count = 1;
  }
}

void AtEOXact_ReadCache(bool commit) {
  if (Gp_role != GP_ROLE_DISPATCH) return;

  if (ReadCacheHashEntryPendingRevise) {
    if (commit && hash_get_num_entries(ReadCacheHashEntryPendingRevise) > 0) {
      ReadCacheHashEntryPendingReviseData *pending;
      HASH_SEQ_STATUS status;
      hash_seq_init(&status, ReadCacheHashEntryPendingRevise);
      LWLockAcquire(ReadCacheLock, LW_EXCLUSIVE);
      while ((pending = (ReadCacheHashEntryPendingReviseData *)hash_seq_search(
                  &status)) != NULL) {
        bool found;
        if (pending->drop) {
          hash_search(ReadCacheHash, (void *)&pending->relid, HASH_REMOVE,
                      &found);
        } else {
          ReadCacheHashData *entry = (ReadCacheHashData *)hash_search(
              ReadCacheHash, (void *)&pending->relid, HASH_ENTER_NULL, &found);
          if (!entry)
            ereport(
                FATAL,
                (errcode(ERRCODE_OUT_OF_MEMORY),
                 errmsg("not enough shared memory for read cache add entry")));
          if (found) {
            entry->revisionNum += pending->count;
          } else {
            entry->revisionNum = pending->count;
          }
        }
      }
      LWLockRelease(ReadCacheLock);
    }

    hash_destroy(ReadCacheHashEntryPendingRevise);
    ReadCacheHashEntryPendingRevise = NULL;
  }
}

void resetReadCache(bool enable) {
  if (enable_secure_filesystem || !magma_cache_read) enable = false;
  CachedResultReset(MyCachedResult, enable);
}

void initReadCache(PlannedStmt *plannedStmt, const char *sourceText) {
  if (magma_cache_read && CachedResultSafeGuard(MyCachedResult)) {
    // check the relation oid the plan depends on
    bool toContinue = (plannedStmt->relationOids != NIL);
    ListCell *cell = NULL;
    foreach (cell, plannedStmt->relationOids) {
      if (!RelationIsMagmaTable2(lfirst_oid(cell))) {
        toContinue = false;
        break;
      }
    }

    if (toContinue) {
      CommonPlanContext ctx;
      ctx.base.node = (Node *)plannedStmt;
      toContinue = !plan_tree_walker((Node *)plannedStmt->planTree,
                                     check_volatile_functions, &ctx);
    }

    if (toContinue) {
      outfast_workfile_mgr_init(plannedStmt->rtable);
      int32_t len1 = -1;
      char *str1 = nodeToBinaryStringFast((Node *)plannedStmt->planTree, &len1);
      int32_t len2 = -1;
      char *str2 = nodeToBinaryStringFast((Node *)plannedStmt->subplans, &len2);
      int32_t len3 = -1;
      char *str3 = nodeToBinaryStringFast((Node *)plannedStmt->rtable, &len3);
      outfast_workfile_mgr_end();

      int32_t relNum = list_length(plannedStmt->relationOids);
      int64_t *relids = (int64_t *)palloc(relNum * sizeof(int64_t));
      int64_t *revisions = (int64_t *)palloc(relNum * sizeof(int64_t));
      LWLockAcquire(ReadCacheLock, LW_SHARED);
      int32_t i = 0;
      foreach (cell, plannedStmt->relationOids) {
        Oid relid = lfirst_oid(cell);
        relids[i] = relid;
        revisions[i] = ReadCacheLookupHashEntry(relid);
        ++i;
      }
      LWLockRelease(ReadCacheLock);
      CachedResultInit(MyCachedResult, GetCurrentTransactionId(), relids,
                       revisions, relNum, sourceText, str1, len1, str2, len2,
                       str3, len3);
      pfree(relids);
      pfree(revisions);
    } else {
      CachedResultReset(MyCachedResult, false);
    }
  }
}

bool readCacheEnabled() {
  return enableOushuDbExtensiveFeatureSupport() ?
      CachedResultIsRead(MyCachedResult) : false;
}

bool readCacheEof() { return CachedResultReadEof(MyCachedResult); }

char *readCache(int32_t *len) { return CachedResultRead(MyCachedResult, len); }

bool writeCacheEnabled() {
  return enableOushuDbExtensiveFeatureSupport() ?
      CachedResultIsWrite(MyCachedResult) : false;
}

void writeCache(const char *buf, int32_t size) {
  CachedResultWrite(MyCachedResult, buf, size);
}

void commitReadCache() {
  if (writeCacheEnabled()) {
    CachedResultEndWrite(MyCachedResult);
  }
  resetReadCache(false);
}

static int64_t ReadCacheLookupHashEntry(Oid relid) {
  int64_t revisionNum = 0;
  bool found;

  LWLockAcquire(ReadCacheLock, LW_SHARED);
  ReadCacheHashData *entry = (ReadCacheHashData *)hash_search(
      ReadCacheHash, (void *)&relid, HASH_FIND, &found);
  LWLockRelease(ReadCacheLock);

  if (entry) revisionNum = entry->revisionNum;
  if (ReadCacheHashEntryPendingRevise) {
    ReadCacheHashEntryPendingReviseData *node =
        (ReadCacheHashEntryPendingReviseData *)hash_search(
            ReadCacheHashEntryPendingRevise, (void *)&relid, HASH_FIND, &found);
    if (node) {
      revisionNum += node->count;
      revisionNum *= -1;
    }
  }
  return revisionNum;
}
