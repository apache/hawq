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
 * mdver_local_mdvsn.c
 *	 Implementation of Local MDVSN for metadata versioning
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"
#include "utils/mdver.h"
#include "miscadmin.h"
#include "cdb/cdbvars.h"

/* Number of buckets for a Local MDVSN hashtable */
#define MDVER_LOCAL_MDVSN_NBUCKETS 1024

/*
 * Look up an entry in the specified Local MDVSN component. This function
 *  doesn't traverse to the parent Local MDVSN.
 *
 * 	 Returns the entry if found, NULL otherwise.
 *
 * 	 local_mdvsn: Which MDVSN to look into. For subtransactions, we have
 * 	   a separate Local MDVSN component for each level
 * 	 key: The key of the object to look for
 */
mdver_entry *
mdver_local_mdvsn_find(mdver_local_mdvsn *local_mdvsn, Oid key)
{
	Assert(NULL != local_mdvsn);
	Assert(InvalidOid != key);

	mdver_entry *entry = (mdver_entry *) hash_search(local_mdvsn->htable, &key,
			HASH_FIND, NULL /* foundPtr */);

	ereport(gp_mdversioning_loglevel,
			(errmsg("LocalMDVSN find for key=%d. Found=%s, nukeHappened=%s",
			key,
			(NULL!=entry)?"yes":"no",
			local_mdvsn->nuke_happened?"yes":"no"),
			errprintstack(true)));


	return entry;
}

/*
 * Inserts or updates an entry in the Local MDVSN component.
 * Entry is inserted if not already existing, or updated otherwise.
 *
 * 	local_mdvsn: Which MDVSN to insert/update into. For subtransactions, we have
 * 	  a separate Local MDVSN compoment for each level.
 * 	entry: The mdver_entry to be inserted or updated. A copy of it will be inserted
 * 	  in the hashtable
 * 	local: true if the message was generated locally, and it's coming from CVQ.
 * 	  false if message is coming from the global queue SVQ
 */
void
mdver_local_mdvsn_add(mdver_local_mdvsn *local_mdvsn, mdver_entry *entry, bool local)
{
	Assert(NULL != local_mdvsn);
	Assert(NULL != local_mdvsn->htable);
	Assert(NULL != entry);

	bool found_ptr = false;
	mdver_entry *inserted_entry = (mdver_entry *) hash_search(local_mdvsn->htable,
			&entry->key, HASH_ENTER, &found_ptr);
	Assert(NULL != inserted_entry);

#ifdef MD_VERSIONING_INSTRUMENTATION
	if (found_ptr)
	{
		ereport(gp_mdversioning_loglevel,
				(errmsg("local_mdvsn_add: updating entry Oid=%d [" UINT64_FORMAT ", " UINT64_FORMAT "] --> [" UINT64_FORMAT ", " UINT64_FORMAT "]",
						inserted_entry->key, inserted_entry->ddl_version, inserted_entry->dml_version,
						entry->ddl_version, entry->dml_version),
						errprintstack(false)));
	}
	else
	{
		ereport(gp_mdversioning_loglevel,
				(errmsg("local_mdvsn_add: inserting entry Oid=%d [" UINT64_FORMAT ", " UINT64_FORMAT "]",
						inserted_entry->key,
						entry->ddl_version, entry->dml_version),
						errprintstack(false)));
	}
#endif

		memcpy(inserted_entry, entry, sizeof(mdver_entry));

}

/*
 * Clears the contents of the entire Local MDVSN specified.
 */
void
mdver_local_mdvsn_nuke(mdver_local_mdvsn *local_mdvsn)
{
	Assert(NULL != local_mdvsn);
	Assert(NULL != local_mdvsn->htable);

	HASH_SEQ_STATUS iterator;
	hash_seq_init(&iterator, local_mdvsn->htable);
	mdver_entry *entry = NULL;
	int num_deleted = 0;

	while ((entry = (mdver_entry *) hash_seq_search(&iterator)) != NULL)
	{
#if USE_ASSERT_CHECKING
		mdver_entry *result = (mdver_entry *)
#endif
		hash_search(local_mdvsn->htable,
				(void *) &(entry->key),
				HASH_REMOVE,
				NULL);

		Assert(NULL != result);

		num_deleted++;

	}

#ifdef MD_VERSIONING_INSTRUMENTATION
	elog(gp_mdversioning_loglevel, "Nuke at Local MDVSN deleted %d entries", num_deleted);
#endif

	local_mdvsn->nuke_happened = true;

	return;
}

/*
 * Allocate a new Local MDVSN data structure in the Current Memory Context
 *
 */
static mdver_local_mdvsn *
mdver_local_mdvsn_create_hashtable(const char *htable_name)
{

	mdver_local_mdvsn *new_local_mdvsn = palloc0(sizeof(mdver_local_mdvsn));

	HASHCTL ctl;
	ctl.keysize = sizeof(Oid);
	ctl.entrysize = sizeof(mdver_entry);
	ctl.hash = oid_hash;

	HTAB *htable = hash_create(htable_name,
			MDVER_LOCAL_MDVSN_NBUCKETS,
			&ctl,
			HASH_ELEM | HASH_FUNCTION);

	new_local_mdvsn->htable = htable;
	new_local_mdvsn->nuke_happened = false;

	return new_local_mdvsn;
}

/*
 * Creates the Local MDVSN for the current transaction or subtransaction.
 *
 * For a top-level transaction, returns the session-level Local MDVSN,
 * which has already been created at the start of the session.
 *
 * For a subtransaction, it creates a new Local MDVSN for the life of the
 *  subtransaction.
 *
 *    nesting_level: The nesting level of the current transaction.
 *      nesting_level is 0 for outside any transaction, 1 for top-level
 *      transaction, and > 1 for subtransaction
 */
mdver_local_mdvsn *
mdver_create_local_mdvsn(int nesting_level)
{

	Assert(1 <= nesting_level);

	/*
	 * nesting_level is 0 for outside any transaction, 1 for top-level
	 * transaction, and > 1 for subtransaction.
	 */


	StringInfo cache_name = makeStringInfo();

	if (nesting_level == 1)
	{
		/* Top-level transaction */
		appendStringInfo(cache_name, "TopLevel Xact MDVSN");

	}
	else
	{
		/* nesting_level > 1: Subtransaction level */
		appendStringInfo(cache_name, "SubXact MDVSN nesting_level=%d", nesting_level);
	}

	elog(gp_mdversioning_loglevel,
			"In mdver_get_local_mdvsn. Creating Local MDVSN: [%s]. Gp_role = %d, Gp_identity=%d",
			cache_name->data,
			Gp_role, GpIdentity.segindex);

	mdver_local_mdvsn *local_mdvsn = mdver_local_mdvsn_create_hashtable(cache_name->data);

	pfree(cache_name->data);
	pfree(cache_name);

	return local_mdvsn;
}

/*
 * Frees up the memory allocated to a Local MDVSN component
 *   local_mdvsn: the Local MDVSN to be freed
 *   nesting_level: Nesting level of the owner transaction or subtransaction
 *
 *   After this function is called, the local_mdvsn is a dangling pointer.
 *   Caller should set this pointer to NULL.
 *
 *   FIXME gcaragea 6/2/2014: Pass double pointer and assign to NULL here
 */
void
mdver_destroy_local_mdvsn(mdver_local_mdvsn *local_mdvsn, int nesting_level)
{
	Assert(NULL != local_mdvsn);
	Assert(nesting_level >= 1);

	elog(gp_mdversioning_loglevel, "Destroying Local MDVSN, level=%d", nesting_level);

	hash_destroy(local_mdvsn->htable);
	pfree(local_mdvsn);
}
