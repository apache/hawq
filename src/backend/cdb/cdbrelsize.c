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
 * cdbrelsize.h
 *
 * Get the max size of the relation across the segDBs
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <math.h>

#include "utils/lsyscache.h"
#include "utils/relcache.h"
#include "utils/syscache.h"
#include "catalog/catalog.h"
#include "cdb/cdbvars.h"
#include "miscadmin.h"
#include "cdb/cdbdisp.h"
#include "gp-libpq-fe.h"
#include "lib/stringinfo.h"
#include "utils/int8.h"
#include "utils/lsyscache.h"
#include "utils/builtins.h"

#include "cdb/cdbrelsize.h"

#define relsize_cache_size 100

struct relsize_cache_entry 
{
	Oid	relOid;
	int64 size;
};

static struct relsize_cache_entry relsize_cache[relsize_cache_size] = { {0,0} };

static int last_cache_entry = -1;		/* -1 for cache not initialized yet */

/*
 * in dbsize.c
 */
extern int64 calculate_relation_size(Relation rel);

void clear_relsize_cache(void)
{
	int i;
	for (i=0; i < relsize_cache_size; i++)
	{
		relsize_cache[i].relOid = InvalidOid;
		relsize_cache[i].size = 0;
	}
	last_cache_entry = -1;
}

int64 cdbRelSize(Relation rel)
{
	int64	size = 0;
	int		i;

	if (last_cache_entry  >= 0)
	{
		for (i=0; i < relsize_cache_size; i++)
		{
			if (relsize_cache[i].relOid == RelationGetRelid(rel))
				return relsize_cache[i].size;
		}
	}

	size = calculate_relation_size(rel);

	if (size >= 0)	/* Cache the size even if it is zero, as table might be empty */
	{
		if (last_cache_entry < 0)
			last_cache_entry = 0;

		relsize_cache[last_cache_entry].relOid = RelationGetRelid(rel);
		relsize_cache[last_cache_entry].size = size;
		last_cache_entry = (last_cache_entry+1) % relsize_cache_size;
	}

	return size;
}
