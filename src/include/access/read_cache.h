///////////////////////////////////////////////////////////////////////////////
// Copyright 2019, Oushu Inc.
// All rights reserved.
//
// Author:
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
