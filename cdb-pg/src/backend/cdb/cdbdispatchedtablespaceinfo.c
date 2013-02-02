/*-------------------------------------------------------------------------
 *
 * cdbdispatchedtablespaceinfo.c
 *
 * Copyright (c) 2009-2012, Greenplum inc
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

/*
 * init gloal hash table DispatchedFilespaceDirHashTable
 */
static void
DispatchedFilespace_HashTableInit(void)
{
	HASHCTL			info;
	int				hash_flags;

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

	strncpy(entry->location, path, FilespaceLocationBlankPaddedWithNullTermLen);
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
