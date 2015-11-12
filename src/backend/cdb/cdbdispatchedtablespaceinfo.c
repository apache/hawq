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
 * cdbdispatchedtablespaceinfo.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "catalog/pg_tablespace.h"
#include "cdb/cdbdispatchedtablespaceinfo.h"
#include "utils/guc.h"
#include "utils/hsearch.h"
#include "utils/memutils.h"

/*
 * never destroy it until process exit
 */
static HTAB * DispatchedFilespaceDirHashTable = NULL;
static bool DispatchedFileSpace_SeqSearch_Initialized = false;
static HASH_SEQ_STATUS DispatchedFileSpace_SeqSearch;

/*
 * init gloal hash table DispatchedFilespaceDirHashTable
 */
static void
DispatchedFilespace_HashTableInit(void)
{
	HASHCTL			info;
	int				hash_flags;

	/* Make sure the seq search is not initialized */
	DispatchedFileSpace_SeqSearch_Initialized = false;

	if (DispatchedFilespaceDirHashTable)
		return;

	/* Set key and entry sizes. */
	MemSet(&info, 0, sizeof(info));
	info.keysize = sizeof(Oid);
	info.entrysize = sizeof(DispatchedFilespaceDirEntryData);
	info.hash = tag_hash;
	info.hcxt = TopMemoryContext;
	hash_flags = (HASH_CONTEXT | HASH_ELEM | HASH_FUNCTION);

	DispatchedFilespaceDirHashTable =
			hash_create("Dispatched Filespace Hash",
								   gp_max_tablespaces,
								   &info,
								   hash_flags);
	Assert(NULL != DispatchedFilespaceDirHashTable);

}

/*
 * add a entry into DispatchedFilespaceDirHashTable,
 * replace it if already exist
 */
void
DispatchedFilespace_AddForTablespace(Oid tablespace, const char * path)
{
	bool found;
	DispatchedFilespaceDirEntry entry;

	Assert(NULL != path);

	if (!DispatchedFilespaceDirHashTable)
	{
		DispatchedFilespace_HashTableInit();
	}

	entry = (DispatchedFilespaceDirEntry) hash_search(
			DispatchedFilespaceDirHashTable, (void *) &tablespace, HASH_ENTER,
			&found);

	Assert(NULL != entry);

	StrNCpy(entry->location, path, FilespaceLocationBlankPaddedWithNullTermLen);
}

/*
 * get a tablespace location by oid.
 */
void
DispatchedFilespace_GetPathForTablespace(Oid tablespace, char **filespacePath, bool * found)
{
	DispatchedFilespaceDirEntry entry;

	Assert(NULL != filespacePath);
	Assert(OidIsValid(tablespace));

	*filespacePath = NULL;
	*found = FALSE;

	if (IsBuiltinTablespace(tablespace))
	{
		/*
		 * Optimize out the common cases.
		 */
		return;
	}

	Assert(NULL != DispatchedFilespaceDirHashTable);

	entry = (DispatchedFilespaceDirEntry) hash_search(
			DispatchedFilespaceDirHashTable, (void *) &tablespace, HASH_FIND,
			found);

	if (!*found)
		*filespacePath = NULL;
	else
		*filespacePath = pstrdup(entry->location);
}

/*
 * initialize the seq search of the DispatchedFilespaceDirHashTable.
 */
static void
DispatchedFilespace_SeqSearch_Init(void)
{
	if (DispatchedFileSpace_SeqSearch_Initialized || (!DispatchedFilespaceDirHashTable))
	{
		return;
	}
	hash_seq_init(&DispatchedFileSpace_SeqSearch, DispatchedFilespaceDirHashTable);
	DispatchedFileSpace_SeqSearch_Initialized = true;
}

/*
 * Get the next DispatchedFilespaceDirEntry
 * from the DispatchedFilespaceDirHashTable.
 */
DispatchedFilespaceDirEntry
DispatchedFilespace_SeqSearch_GetNext(void)
{
	DispatchedFilespaceDirEntry entry;

	if (!DispatchedFilespaceDirHashTable)
	{
		return NULL;
	}
	if (!DispatchedFileSpace_SeqSearch_Initialized)
	{
		DispatchedFilespace_SeqSearch_Init();
	}

	entry = (DispatchedFilespaceDirEntry)hash_seq_search(&DispatchedFileSpace_SeqSearch);
	if (!entry)
	{
		DispatchedFileSpace_SeqSearch_Initialized = false;
	}
	return entry;
}

/*
 * Terminate the seq search of the DispatchedFilespaceDirHashTable.
 */
void
DispatchedFilespace_SeqSearch_Term(void)
{
	if (!DispatchedFileSpace_SeqSearch_Initialized)
	{
		return;
	}
	hash_seq_term(&DispatchedFileSpace_SeqSearch);
	DispatchedFileSpace_SeqSearch_Initialized = false;
}
