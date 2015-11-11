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

/*
 * poolmgr.c
 *	Have to be accessed in main thread!!!!
 */

#include "postgres.h"

#include "cdb/poolmgr.h"
#include "utils/hsearch.h"
#include "nodes/pg_list.h"

typedef struct PoolMgrState {
	MemoryContext			ctx;
	PoolMgrCleanCallback	callback;
	HTAB					*hashtable;
} PoolMgrState;

#define POOLMGR_HASH_TABLE_KEY_SIZE			128
#define POOLMGR_HASH_TABLE_ELEM_SIZE		1024

typedef struct PoolItemEntry {
	char		key[POOLMGR_HASH_TABLE_KEY_SIZE];
	List		*list;
} PoolItemEntry;


PoolMgrState *
poolmgr_create_pool(MemoryContext ctx, PoolMgrCleanCallback callback)
{
	PoolMgrState	*pool;
	HASHCTL			hash_ctl;

	pool = MemoryContextAlloc(ctx, sizeof(PoolMgrState));

	pool->ctx = ctx;
	pool->callback = callback;

	memset(&hash_ctl, 0, sizeof(hash_ctl));
	hash_ctl.keysize = POOLMGR_HASH_TABLE_KEY_SIZE;
	hash_ctl.entrysize = sizeof(PoolItemEntry);
	hash_ctl.hcxt = pool->ctx;

	pool->hashtable = hash_create("Pool Manager Hash",
							POOLMGR_HASH_TABLE_ELEM_SIZE,
							&hash_ctl,
							HASH_ELEM | HASH_CONTEXT);

	return pool;
}

bool
poolmgr_drop_pool(PoolMgrState *pool)
{
	HASH_SEQ_STATUS		hash_seq;
	PoolItemEntry		*entry;

	if (!pool)
		return true;

	hash_seq_init(&hash_seq, pool->hashtable);
	while ((entry = hash_seq_search(&hash_seq)))
	{
		ListCell	*lc;

		foreach(lc, entry->list)
		{
			if (pool->callback)
				pool->callback(lfirst(lc));
		}

		list_free(entry->list);
		entry->list = NULL;
	}

	hash_destroy(pool->hashtable);
	pfree(pool);
	return true;
}

PoolItem
poolmgr_get_random_item(PoolMgrState *pool)
{
	HASH_SEQ_STATUS		hash_seq;
	PoolItemEntry		*entry;
	char				*random_key_name = NULL;
	Assert(pool);

	hash_seq_init(&hash_seq, pool->hashtable);
	while ((entry = hash_seq_search(&hash_seq)))
	{
		if (list_length(entry->list) == 0)
			continue;

		random_key_name = entry->key;
		hash_seq_term(&hash_seq);
		break;
	}

	if (random_key_name == NULL)
		return NULL;

	return poolmgr_get_item_by_name(pool, random_key_name);
}


PoolItem
poolmgr_get_item_by_name(PoolMgrState *pool, const char *name)
{
	PoolItemEntry	*entry;
	PoolItem		*item;
	bool			found;
	Assert(pool);

	entry = hash_search(pool->hashtable, name, HASH_ENTER, &found);
	if (!found)
		entry->list = NIL;
	item = (PoolItem) (list_head(entry->list) ? linitial(entry->list) : NULL);
	entry->list = list_delete_first(entry->list);

	return item;
}

void
poolmgr_put_item(PoolMgrState *pool, const char *name, PoolItem item)
{
	PoolItemEntry	*entry;
	MemoryContext	old;
	bool			found;
	Assert(pool);

	entry = hash_search(pool->hashtable, name, HASH_ENTER, &found);
	if (!found)
		entry->list = NIL;
	old = MemoryContextSwitchTo(pool->ctx);
	entry->list = lappend(entry->list, item);
	MemoryContextSwitchTo(old);
}

void
poolmgr_iterate(PoolMgrState *pool, PoolMgrIterateFilter filter, PoolMgrIterateCallback callback, PoolIterateArg data)
{
	HASH_SEQ_STATUS		hash_seq;
	PoolItemEntry		*entry;

	if (!pool)
		return;

	hash_seq_init(&hash_seq, pool->hashtable);
	while ((entry = hash_seq_search(&hash_seq)))
	{
		ListCell	*lc;

		foreach(lc, entry->list)
		{
			PoolItem	item = lfirst(lc);

			if (filter && !filter(item))
				continue;

			callback(item, data);
		}

	}

	return;
}

