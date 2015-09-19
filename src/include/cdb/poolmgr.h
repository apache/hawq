
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

