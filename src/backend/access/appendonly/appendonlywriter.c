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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/*-------------------------------------------------------------------------
 *
 * appendonlywriter.c
 *	  routines to support AO concurrent writes via shared memory structures.
 *
 * Portions Copyright (c) 2008, Greenplum Inc
 * Portions Copyright (c) 1996-2006, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "access/appendonlywriter.h"
#include "access/appendonlytid.h"	  /* AOTupleId_MaxRowNum  */
#include "access/parquetsegfiles.h"
#include "access/heapam.h"			  /* heap_open            */
#include "access/transam.h"			  /* InvalidTransactionId */
#include "utils/lsyscache.h"

#include "cdb/cdbvars.h"			  /* Gp_role              */
#include "utils/tqual.h"

#include "catalog/catalog.h"
#include "catalog/pg_tablespace.h"
#include "commands/dbcommands.h"
#include "cdb/cdbdisp.h"
#include "cdb/cdbfilesystemcredential.h"

#include "utils/builtins.h"
#include "utils/tqual.h"
#include "funcapi.h"
#include "gp-libpq-fe.h"
#include "lib/stringinfo.h"
#include "miscadmin.h"
#include "access/aomd.h"
#include "cdb/dispatcher.h"

/*
 * GUC variables
 */
int		MaxAppendOnlyTables;		/* Max # of tables */
int MaxAORelSegFileStatus;  /* Max # of AO relation segfiles */
extern bool		filesystem_support_truncate;

/*
 * Global Variables
 */
static HTAB *AppendOnlyHash;			/* Hash of AO tables */
AppendOnlyWriterData	*AppendOnlyWriter;
AOSegfileStatus *AOSegfileStatusPool;

/*
 * local functions
 */
static bool AOHashTableInit(void);
static AORelHashEntry AppendOnlyRelHashNew(Oid relid, bool *exists);
static AORelHashEntry AORelGetHashEntry(Oid relid);
static AORelHashEntry AORelLookupHashEntry(Oid relid);
static bool AORelCreateHashEntry(Oid relid);
static int AORelGetSegfileStatus(AORelHashEntry currentEntry);
static void AORelPutSegfileStatus(int old_status);
static AOSegfileStatus *AORelLookupSegfileStatus(int segno, AORelHashEntry entry);
static bool CheckSegFileForWriteIfNeeded(AORelHashEntryData *aoentry, AOSegfileStatus * segfilesstatus);
static int addCandidateSegno(AOSegfileStatus **maxSegno4Seg, int segment_num, AOSegfileStatus *candidata_status, bool isKeepHash);

extern Datum tablespace_support_truncate(PG_FUNCTION_ARGS);

static bool TestCurrentTspSupportTruncateInternal(Oid tsp, const char *tspPathPrefix);

typedef struct AppendOnlyHashEntryPendingCleanup
{
    Oid       relid;           // relation id
    int       nestedLevel;     //in this sub transaction we want to remove this entry
} AppendOnlyHashEntryPendingCleanup;

static HTAB * AppendOnlyHashEntryPendingDeleteCleanup = NULL;  //a list of AppendOnlyHashEntryPendingDelete

typedef struct AORelHelpData
{
    Oid             segrelid;
    char            tspPathPrefix[MAXPGPATH + 1];

} AORelHelpData;
typedef AORelHelpData *AORelHelp;


struct TspSupportTruncate
{
    Oid     *tsps;
    bool    *values;
    int     size;
    int     capacity;
};

static struct TspSupportTruncate TspSupportTruncateMap = {NULL, NULL, 0, 0};


static AORelHelp
GetAOHashTableHelpEntry(Oid relid)
{
    int             i;
    char            *prefix = NULL;
    Oid             tsp;
    bool            currentTspSupportTruncate = false;
    AppendOnlyEntry *aoEntry;
    AORelHelp       helpEntry = NULL;
    Relation        aorel = heap_open(relid, RowExclusiveLock);

    tsp = get_database_dts(aorel->rd_node.dbNode);

    /*
     * test if the relation's tsp support truncate
     */
    if (filesystem_support_truncate)
    {
        /*
         * first lookup in cache
         */
        for (i = 0; i < TspSupportTruncateMap.size; ++i)
        {
            if (TspSupportTruncateMap.tsps[i] == tsp)
            {
                currentTspSupportTruncate = TspSupportTruncateMap.values[i];
                break;
            }
        }

        /*
         * not cached, test it
         */
        if (i == TspSupportTruncateMap.size) {
            GetFilespacePathPrefix(tsp, &prefix);
            currentTspSupportTruncate = TestCurrentTspSupportTruncateInternal(aorel->rd_node.spcNode, prefix);
        }
    }

    /*
     * current tsp does not support truncate, we have to create a help struct
     */
    if (!currentTspSupportTruncate)
    {
        helpEntry = palloc(sizeof(AORelHelpData));

        if (!prefix)
            GetFilespacePathPrefix(tsp, &prefix);

        StrNCpy(helpEntry->tspPathPrefix, prefix, sizeof(helpEntry->tspPathPrefix));

        aoEntry = GetAppendOnlyEntry(relid, SnapshotNow);
        helpEntry->segrelid = aoEntry->segrelid;
        pfree(aoEntry);
    }

    heap_close(aorel, RowExclusiveLock);

    if (prefix)
        pfree(prefix);

    return helpEntry;
}

/*
 * AppendOnlyWriterShmemSize -- estimate size the append only writer structures
 * will need in shared memory.
 *
 */
Size
AppendOnlyWriterShmemSize(void)
{
	Size		size;

	Insist(Gp_role == GP_ROLE_DISPATCH);

	/* The hash of append only relations */
	size = hash_estimate_size((Size)MaxAppendOnlyTables,
							  sizeof(AORelHashEntryData));

	/* The writer structure. */
	size = add_size(size, sizeof(AppendOnlyWriterData));

	/* The size of the AOSegfileStatus pool */
	size = add_size(size, mul_size((Size)MaxAORelSegFileStatus, sizeof(AOSegfileStatus)));

	/* Add a safety margin */
	size = add_size(size, size / 10);

	return size;
}

/*
 * InitAppendOnlyWriter -- initialize the AO writer hash in shared memory.
 *
 * The AppendOnlyHash table has no data in it as yet.
 */
void
InitAppendOnlyWriter(void)
{
	bool		found;
	bool		ok;
	Size poolAOSegfileStatusTotalSize;
	int i;
	AOSegfileStatus *status;

	Insist(Gp_role == GP_ROLE_DISPATCH);

	/* Create the writer structure. */
	AppendOnlyWriter = (AppendOnlyWriterData *)
	ShmemInitStruct("Append Only Writer Data",
					sizeof(AppendOnlyWriterData),
					&found);

	if (found && AppendOnlyHash)
		return;			/* We are already initialized and have a hash table */

	/* Specify that we have no AO rel information yet. */
	AppendOnlyWriter->num_existing_aorels = 0;

	/* Create AppendOnlyHash (empty at this point). */
	ok = AOHashTableInit();
	if (!ok)
		ereport(FATAL,
				(errcode(ERRCODE_OUT_OF_MEMORY),
				 errmsg("not enough shared memory for append only writer")));

	/* Create the pool of AOSegfileStatus. */
	poolAOSegfileStatusTotalSize = mul_size((Size)MaxAORelSegFileStatus, sizeof(AOSegfileStatus));
	AOSegfileStatusPool = (AOSegfileStatus *)
			ShmemInitStruct("Append Only Segment Status Pool",
					poolAOSegfileStatusTotalSize,
					&found);

	/* Initialize all segfile status */
	status = AOSegfileStatusPool;
	for(i = 0; i < MaxAORelSegFileStatus; status++, i++)
	{
		status->id = i;
		status->next = i + 1;
	}
	AOSegfileStatusPool[MaxAORelSegFileStatus-1].next = NEXT_END_OF_LIST;

	/* Specify we have no open segment files now */
	AppendOnlyWriter->head_free_segfilestatus = 0;
	AppendOnlyWriter->num_existing_segfilestatus = 0;

	ereport(DEBUG1, (errmsg("initialized append only writer")));

	return;
}

/*
 * AOHashTableInit
 *
 * initialize the hash table of AO rels and their fileseg entries.
 */
static bool
AOHashTableInit(void)
{
	HASHCTL			info;
	int				hash_flags;

	Insist(Gp_role == GP_ROLE_DISPATCH);

	/* Set key and entry sizes. */
	MemSet(&info, 0, sizeof(info));
	info.keysize = sizeof(Oid);
	info.entrysize = sizeof(AORelHashEntryData);
	info.hash = tag_hash;
	hash_flags = (HASH_ELEM | HASH_FUNCTION);

	if (Debug_appendonly_print_segfile_choice)
	{
		ereport(LOG, (errmsg("AOHashTableInit: Creating hash table for %d append only tables",
				MaxAppendOnlyTables)));
	}

	AppendOnlyHash = ShmemInitHash("Append Only Hash",
								   MaxAppendOnlyTables,
								   MaxAppendOnlyTables,
								   &info,
								   hash_flags);

	if (!AppendOnlyHash)
		return false;

	return true;
}


/*
 * AORelCreateEntry -- initialize the elements for an AO relation entry.
 *					   if an entry already exists, return successfully.
 * Notes:
 *	It is expected that the appropriate lightweight lock is held before
 *	calling this - unless we are the startup process.
 */
static bool
AORelCreateHashEntry(Oid relid)
{
	bool			exists = false;
	FileSegInfo		**allfsinfo = NULL;
	ParquetFileSegInfo **allfsinfoParquet = NULL;
	int				i;
	int				total_segfiles = 0;
	AORelHashEntry	aoHashEntry = NULL;
	Relation		aorel;
	AppendOnlyEntry *aoEntry;
	bool			isParquet = false;
	Insist(Gp_role == GP_ROLE_DISPATCH);

	/*
	 * Momentarily release the AOSegFileLock so we can safely access the system catalog
	 * (i.e. without risking a deadlock).
	 */
	LWLockRelease(AOSegFileLock);

	/*
	 * Now get all the segment files information for this relation
	 * from the QD aoseg table. then update our segment file array
	 * in this hash entry.
	 */
	aorel = heap_open(relid, RowExclusiveLock);

	/*
	 * Use SnapshotNow since we have an exclusive lock on the relation.
	 */
	aoEntry = GetAppendOnlyEntry(relid, SnapshotNow);

	if(RelationIsParquet(aorel))
	{
		isParquet = true;
		allfsinfo = NULL;
		allfsinfoParquet = GetAllParquetFileSegInfo(aorel, aoEntry, SnapshotNow, &total_segfiles);
	}
	else /* AO */
	{
		allfsinfo = GetAllFileSegInfo(aorel, aoEntry, SnapshotNow, &total_segfiles);
		allfsinfoParquet = NULL;
	}
	pfree(aoEntry);
	heap_close(aorel, RowExclusiveLock);

	/*
	 * Re-acquire AOSegFileLock.
	 *
	 * Note: Another session may have raced-in and created it.
	 */
	LWLockAcquire(AOSegFileLock, LW_EXCLUSIVE);
	
	aoHashEntry = AppendOnlyRelHashNew(relid, &exists);

	/* Does an entry for this relation exists already? exit early */
	if(exists)
	{
		if(allfsinfo)
			pfree(allfsinfo);
		if(allfsinfoParquet)
			pfree(allfsinfoParquet);
		return true;
	}

	/*
	 * If the new aoHashEntry pointer is NULL, then we are out of allowed # of
	 * AO tables used for write concurrently, and don't have any unused entries
	 * to free
	 */
	if (!aoHashEntry)
	{
		Assert(AppendOnlyWriter->num_existing_aorels >= MaxAppendOnlyTables);
		return false;
	}


	Insist(aoHashEntry->relid == relid);
	aoHashEntry->txns_using_rel = 0;
  aoHashEntry->staleTid = InvalidTransactionId;

	/*
	 * Segment file number 0 is reserved for utility mode operations.
	 */
	aoHashEntry->head_rel_segfile.segno = 0;
	aoHashEntry->head_rel_segfile.next = NEXT_END_OF_LIST;
	aoHashEntry->head_rel_segfile.inuse = true;
	aoHashEntry->head_rel_segfile.xid = InvalidTransactionId;
	aoHashEntry->head_rel_segfile.latestWriteXid = InvalidTransactionId;
	aoHashEntry->head_rel_segfile.isfull = true;
	aoHashEntry->head_rel_segfile.needCheck = true;
	aoHashEntry->head_rel_segfile.tupcount = 0;
	aoHashEntry->head_rel_segfile.tupsadded = 0;
	aoHashEntry->max_seg_no = 0;

	/*
	 * update the tupcount of each 'segment' file in the append
	 * only hash according to tupcounts in the pg_aoseg table.
	 */
	if (isParquet)
	{
		for (i = 0 ; i < total_segfiles; i++)
		{
			AOSegfileStatus *status;
			int id = AORelGetSegfileStatus(aoHashEntry);
			/* TODO: we need to release this aoHashEntry here */
			if (id == NEXT_END_OF_LIST)
			{
				pfree(allfsinfoParquet);

				ereport(ERROR, (errmsg("cannot open more than %d "
				      "append-only table segment "
				      "files cocurrently",
				      MaxAORelSegFileStatus)));

				return false;
			}
			status = &AOSegfileStatusPool[id];
			status->inuse = false;
			status->isfull = false;
            status->needCheck = true;
			status->latestWriteXid = InvalidTransactionId;
			status->xid = InvalidTransactionId;
			status->tupsadded = 0;
			status->next = aoHashEntry->head_rel_segfile.next;
			status->tupcount = allfsinfoParquet[i]->tupcount;
			status->segno = allfsinfoParquet[i]->segno;
			aoHashEntry->head_rel_segfile.next = status->id;
			if (status->segno > aoHashEntry->max_seg_no)
			{
				aoHashEntry->max_seg_no = status->segno;
			}
		}
	}
	else
	{
		for (i = 0 ; i < total_segfiles; i++)
		{
			AOSegfileStatus *status;
			int id = AORelGetSegfileStatus(aoHashEntry);
			/* TODO: we need to release this aoHashEntry here */
			if (id == NEXT_END_OF_LIST)
			{
				pfree(allfsinfo);

				ereport(ERROR, (errmsg("cannot open more than %d "
				      "append-only table segment "
				      "files cocurrently",
				      MaxAORelSegFileStatus)));

				return false;
			}
			status = &AOSegfileStatusPool[id];
			status->inuse = false;
			status->isfull = false;
            status->needCheck = true;
			status->latestWriteXid = InvalidTransactionId;
			status->xid = InvalidTransactionId;
			status->tupsadded = 0;
			status->next = aoHashEntry->head_rel_segfile.next;
			status->tupcount = allfsinfo[i]->tupcount;
			status->segno = allfsinfo[i]->segno;
			aoHashEntry->head_rel_segfile.next = status->id;
			if (status->segno > aoHashEntry->max_seg_no)
			{
				aoHashEntry->max_seg_no = status->segno;
			}
		}
	}

	/* record the fact that another hash entry is now taken */
	AppendOnlyWriter->num_existing_aorels++;

	if(allfsinfo)
		pfree(allfsinfo);
	if(allfsinfoParquet)
		pfree(allfsinfoParquet);

	if (Debug_appendonly_print_segfile_choice)
	{
		ereport(LOG, (errmsg("AORelCreateHashEntry: Created a hash entry for append-only "
							 "relation %d ", relid)));
	}

	return true;
}

/*
 * AORelRemoveEntry -- remove the hash entry for a given relation.
 *
 * Notes
 *	The append only lightweight lock (AOSegFileLock) *must* be held for
 *	this operation.
 */
bool
AORelRemoveHashEntry(Oid relid, bool checkIsStale)
{
	bool				found;
	AORelHashEntryData *aoentry;
	int next = 0;
	int old_next = 0;
	Insist(Gp_role == GP_ROLE_DISPATCH);

	aoentry = (AORelHashEntryData *)hash_search(AppendOnlyHash,
							  (void *) &relid,
							  HASH_FIND,
							  &found);

	if(checkIsStale)
		Insist(NULL!=aoentry && aoentry->staleTid==GetTopTransactionId());

	if (found == false)
		return false;

	/* Release all segment file statuses used by this relation. */
	next = aoentry->head_rel_segfile.next;
	while (next != NEXT_END_OF_LIST)
	{
		old_next = next;
		next = AOSegfileStatusPool[next].next;

		AORelPutSegfileStatus(old_next);
	}
	aoentry->head_rel_segfile.next = NEXT_END_OF_LIST;

	aoentry = (AORelHashEntryData *)hash_search(AppendOnlyHash,
							  (void *) &relid,
							  HASH_REMOVE,
							  &found);

	if (Debug_appendonly_print_segfile_choice)
	{
		ereport(LOG, (errmsg("AORelRemoveHashEntry: Remove hash entry for inactive append-only "
							 "relation %d (found %s)", 
							 relid,
							 (aoentry != NULL ? "true" : "false"))));
	}

	AppendOnlyWriter->num_existing_aorels--;
	Assert(AppendOnlyWriter->num_existing_aorels >= 0);

	return true;
}


    void
AORelRemoveHashEntryOnCommit(Oid relid)
{
    AORelHashEntry aoentry;
    bool found;

    Insist(Gp_role == GP_ROLE_DISPATCH);

    if (NULL == AppendOnlyHashEntryPendingDeleteCleanup)
    {
        HASHCTL         info;
        int             hash_flags;

        /* Set key and entry sizes. */
        MemSet(&info, 0, sizeof(info));
        info.keysize = sizeof(Oid);
        info.entrysize = sizeof(AppendOnlyHashEntryPendingCleanup);
        info.hash = tag_hash;
        hash_flags = (HASH_ELEM | HASH_FUNCTION);

        AppendOnlyHashEntryPendingDeleteCleanup = hash_create("AppendOnlyHashEntryPendingDelete",
                MaxAppendOnlyTables,
                &info,
                hash_flags);

        if (!AppendOnlyHashEntryPendingDeleteCleanup)
            elog(ERROR, "failed to create AppendOnlyHashEntryPendingDeleteList");
    }

    AppendOnlyHashEntryPendingCleanup * node =
        (AppendOnlyHashEntryPendingCleanup *) hash_search(AppendOnlyHashEntryPendingDeleteCleanup,
                (void *) &relid,
                HASH_ENTER,
                &found);

    int level = GetCurrentTransactionNestLevel();

    if (found)
        node->nestedLevel = node->nestedLevel < level? node->nestedLevel : level;
    else
        node->nestedLevel = level;

    node->relid = relid;

    LWLockAcquire(AOSegFileLock, LW_EXCLUSIVE);

    /*
     * The most common case is likely the entry exists already.
     */
    aoentry = AORelLookupHashEntry(relid);
    if (aoentry == NULL)
    {
        /*
         * We need to create a hash entry for this relation.
         *
         * However, we need to access the pg_appendonly system catalog
         * table, so AORelCreateHashEntry will carefully release the AOSegFileLock,
         * gather the information, and then re-acquire AOSegFileLock.
         */
        if(!AORelCreateHashEntry(relid))
        {
            LWLockRelease(AOSegFileLock);
            ereport(ERROR, (errmsg("can't have more than %d different append-only "
                            "tables open for writing data at the same time. "
                            "if tables are heavily partitioned or if your "
                            "workload requires, increase the value of "
                            "max_appendonly_tables and retry",
                            MaxAppendOnlyTables)));
        }

        /* get the hash entry for this relation (must exist) */
        aoentry = AORelGetHashEntry(relid);
    }

    /*
     * mark it as staled in case the following insert
     */
    aoentry->staleTid = GetTopTransactionId();

    LWLockRelease(AOSegFileLock);
}



/*
 * AORelLookupEntry -- return the AO hash entry for a given AO relation,
 *					   it exists.
 */
static AORelHashEntry
AORelLookupHashEntry(Oid relid)
{
	bool				found;
	AORelHashEntryData	*aoentry;

	Insist(Gp_role == GP_ROLE_DISPATCH);

	aoentry = (AORelHashEntryData *) hash_search(AppendOnlyHash,
												 (void *) &relid,
												 HASH_FIND,
												 &found);

	if (!aoentry)
		return NULL;

	return (AORelHashEntry) aoentry;
}

/*
 * AORelGetEntry -- Same as AORelLookupEntry but here the caller is expecting
 *					an entry to exist. We error out if it doesn't.
 */
static AORelHashEntry
AORelGetHashEntry(Oid relid)
{
	AORelHashEntryData	*aoentry = AORelLookupHashEntry(relid);

	if(!aoentry)
		ereport(ERROR, (errmsg("expected an AO hash entry for relid %d but "
							   "found none", relid)));

	if (Debug_appendonly_print_segfile_choice)
	{
		ereport(LOG, (errmsg("AORelGetHashEntry: Retrieved hash entry for append-only relation "
							 "%d", relid)));
	}

	return (AORelHashEntry) aoentry;
}

/*
 * AppendOnlyRelHashNew -- return a new (empty) aorel hash object to initialize.
 *
 * Notes
 *	The appendonly lightweight lock (AOSegFileLock) *must* be held for
 *	this operation.
 */
static AORelHashEntry
AppendOnlyRelHashNew(Oid relid, bool *exists)
{
	AORelHashEntryData	*aorelentry = NULL;

	Insist(Gp_role == GP_ROLE_DISPATCH);

	/*
	 * We do not want to exceed the max number of allowed entries since we don't
	 * drop entries when we're done with them (so we could reuse them). Note
	 * that the hash table will allow going past max_size (it's not a hard
	 * limit) so we do the check ourselves.
	 *
	 * Therefore, check for this condition first and deal with it if exists.
	 */
	if(AppendOnlyWriter->num_existing_aorels >= MaxAppendOnlyTables)
	{
		/* see if there's already an existing entry for this relid */
		aorelentry = AORelLookupHashEntry(relid);

		/*
		 * already have an entry for this rel? reuse it.
		 * don't have? try to drop an unused entry and create our entry there
		 */
		if(aorelentry)
		{
			*exists = true; /* reuse */
			return NULL;
		}
		else
		{
			HASH_SEQ_STATUS status;
			AORelHashEntryData	*hentry;

			Assert(AppendOnlyWriter->num_existing_aorels == MaxAppendOnlyTables);

			/*
			 * loop through all entries looking for an unused one
			 */
			hash_seq_init(&status, AppendOnlyHash);

			while ((hentry = (AORelHashEntryData *) hash_seq_search(&status)) != NULL)
			{
                if(hentry->txns_using_rel == 0 && InvalidTransactionId==hentry->staleTid)
				{
					if (Debug_appendonly_print_segfile_choice)
					{
						ereport(LOG, (errmsg("AppendOnlyRelHashNew: Appendonly Writer removing an "
											 "unused entry (rel %d) to make "
											 "room for a new one (rel %d)",
											 hentry->relid, relid)));
					}

					/* remove this unused entry */
					/* TODO: remove the LRU entry, not just any unused one */
          AORelRemoveHashEntry(hentry->relid, false);
					hash_seq_term(&status);

					/* we now have room for a new entry, create it */
					aorelentry = (AORelHashEntryData *) hash_search(AppendOnlyHash,
																	(void *) &relid,
																	HASH_ENTER_NULL,
																	exists);

					Insist(aorelentry);
					return (AORelHashEntry) aorelentry;
				}

			}

			/*
			 * No room for a new entry. give up.
			 */
			return NULL;
		}

	}

	/*
	 * We don't yet have a full hash table. Create a new entry if not exists
	 */
	aorelentry = (AORelHashEntryData *) hash_search(AppendOnlyHash,
													(void *) &relid,
													HASH_ENTER_NULL,
													exists);

	/* caller should check "exists" to know if relid entry exists already */
	if(*exists)
		return NULL;

	return (AORelHashEntry) aorelentry;
}

#define SEGFILE_CAPACITY_THRESHOLD	0.9

/*
 * segfileMaxRowThreshold
 *
 * Returns the row count threshold - when a segfile more than this number of
 * rows we don't allow inserting more data into it anymore.
 */
static
int64  segfileMaxRowThreshold(void)
{
	int64 maxallowed = (int64)AOTupleId_MaxRowNum;

	if(maxallowed < 0)
		ereport(ERROR,
				(errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
				 errmsg("int64 out of range")));

	return 	(SEGFILE_CAPACITY_THRESHOLD * maxallowed);
}

/*
 * usedByConcurrentTransaction
 *
 * Return true if segno has been used by any transactions
 * that are running concurrently with the current transaction.
 */
static bool
usedByConcurrentTransaction(AOSegfileStatus *segfilestat)
{
	TransactionId latestWriteXid =
		segfilestat->latestWriteXid;

	int segno = segfilestat->segno;

	/*
	 * If latestWriteXid is invalid, simple return false.
	 */
	if (!TransactionIdIsValid(latestWriteXid))
	{
		if (Debug_appendonly_print_segfile_choice)
		{
			ereport(LOG, (errmsg("usedByConcurrentTransaction: latestWriterXid %u of segno %d not in use, so it is NOT considered concurrent",
								 latestWriteXid,
								 segno)));
		}
		return false;
	}
	
	/*
	 * If the current transaction is started earlier than latestWriteXid,
	 * this segno is surely used by a concurrent transaction. So
	 * return true here.
	 */
	if (TransactionIdPrecedes(GetMasterTransactionId(),
							  latestWriteXid))
	{
		if (Debug_appendonly_print_segfile_choice)
		{
			ereport(LOG, (errmsg("usedByConcurrentTransaction: current transaction id %u preceeds latestWriterXid %x of segno %d, so it is considered concurrent",
								 GetMasterTransactionId(),
								 latestWriteXid,
								 segno)));
		}
		return true;
	}
	
	/*
	 * Obtain the snapshot that is taken at the beginning of the
	 * transaction. We can't simply call GetTransactionSnapshot() here because
	 * it will create a new distributed snapshot for non-serializable
	 * transaction isolation level, and it may be too late.
	 */
	Snapshot snapshot = NULL;
	if (SerializableSnapshot == NULL) 
	{ 
		Assert(LatestSnapshot == NULL); 
		snapshot = GetTransactionSnapshot(); 
	}
	else if (LatestSnapshot != NULL) 
	{ 
		snapshot = LatestSnapshot; 
	} 
	else 
	{ 
		snapshot = SerializableSnapshot; 
	}

	/* If latestWriterXid is invisible to me, return true. */
	if (latestWriteXid >= snapshot->xmax)
	{
		if (Debug_appendonly_print_segfile_choice)
		{
			ereport(LOG, (errmsg("usedByConcurrentTransaction: latestWriterXid %x of segno %d is >= snapshot xmax %u, so it is considered concurrent",
								 latestWriteXid,
								 segno,
								 snapshot->xmax)));
		}
		return true;
	}

	/*
	 * If latestWriteXid is in in-progress, return true.
	 */
	int32 inProgressCount = snapshot->xcnt;
	
	for (int inProgressNo = 0; inProgressNo < inProgressCount; inProgressNo++)
	{
		if (latestWriteXid == snapshot->xip[inProgressNo])
		{
			if (Debug_appendonly_print_segfile_choice)
			{
				ereport(LOG, (errmsg("usedByConcurrentTransaction: latestWriterXid %x of segno %d is equal to snapshot in-flight %u, so it is considered concurrent",
									 latestWriteXid,
									 segno,
									 snapshot->xip[inProgressNo])));
			}
			return true;
		}
	}
	
	if (Debug_appendonly_print_segfile_choice)
	{
		ereport(LOG, (errmsg("usedByConcurrentTransaction: snapshot can see latestWriteXid %u as committed, so it is NOT considered concurrent",
							 latestWriteXid)));
	}
	return false;
}

/* Add candidate segno to array maxSegno4Seg which log the max segno candidate for each segment node
 * Param maxSegno4Seg: store max available segno for each segment node
 * Param segment_num: segment node number, should be same with array dimension of maxSegno4Seg
 * Return remaining_num:  the remaining num of segment node which doesn't have any candidate.
 */
int
addCandidateSegno(AOSegfileStatus **maxSegno4Seg, int segment_num, AOSegfileStatus *candidate_status, bool isKeepHash)
{
	int remaining_num = segment_num;
	if(isKeepHash){
		int idx = (candidate_status->segno+segment_num-1) % segment_num;	// from 1 to segment_num-1, then 0(segment_num)
		if (NULL==maxSegno4Seg[idx] || maxSegno4Seg[idx]->segno > candidate_status->segno)  //using the min seg no firstly.
        	maxSegno4Seg[idx] = candidate_status;
		
		for(int i=0; i<segment_num; i++)
    	{
    		if(NULL!=maxSegno4Seg[i] && maxSegno4Seg[i]->segno > 0){
				remaining_num--;
          	}
    	}
	}else{
		bool assigned = false; // candidate_status assigned?
		remaining_num  = 0;
		for(int i=0; i<segment_num; i++){
			if (NULL==maxSegno4Seg[i] || maxSegno4Seg[i]->segno <= 0){
				if(!assigned){
					maxSegno4Seg[i]= candidate_status;
					assigned = true;
				}else{
					remaining_num++;
				}
			}
		}
	}
	return remaining_num;
}

/*
 * SetSegnoForWrite
 *
 * This function includes the key logic of the append only writer.
 *
 * Depending on the gp role and existingsegno make a decision on which
 * segment nubmer should be used to write into during the COPY or INSERT
 * operation we're executing.
 *
 * the return value is a segment file number to use for inserting by each
 * segdb into its local AO table.
 *
 * ROLE_DISPATCH - when this function is called on a QD the QD needs to select
 *				   a segment file number for writing data. It does so by looking
 *				   at the in-memory hash table and selecting a segment number
 *				   that the most empty across the database and is also not
 *				   currently used. Or, if we are in an explicit transaction and
 *				   inserting into the same table we use the same segno over and
 *				   over again. the passed in parameter 'existingsegno' is
 *				   meaningless for this role.
 *
 * ROLE_EXECUTE  - we need to use the segment file number that the QD decided
 *				   on and sent to us. it is the passed in parameter - use it.
 *
 * ROLE_UTILITY  - always use the reserved segno RESERVED_SEGNO
 *
 */
List *SetSegnoForWrite(List *existing_segnos, Oid relid, int segment_num,
        bool forNewRel, bool reuse_segfilenum_in_same_xid,
        bool keepHash)
{
    /* these vars are used in GP_ROLE_DISPATCH only */
    AORelHashEntryData	*aoentry = NULL;
    TransactionId 		CurrentXid = GetTopTransactionId();
    int next;
    AOSegfileStatus *segfilestatus = NULL;
    int remaining_num = segment_num;
    bool has_same_txn_status = false;
    AOSegfileStatus **maxSegno4Segment = NULL;

    switch(Gp_role)
    {
        case GP_ROLE_EXECUTE:

            Assert(existing_segnos != NIL);
            Assert(list_length(existing_segnos) == segment_num);
            return existing_segnos;

        case GP_ROLE_UTILITY:

            Assert(existing_segnos == NIL);
            Assert(segment_num == 1);
            return list_make1_int(RESERVED_SEGNO);

        case GP_ROLE_DISPATCH:

            Assert(existing_segnos == NIL);
            Assert(segment_num > 0);

            if (forNewRel)
            {
                int i;
                for (i = 1; i <= segment_num; i++)
                {
                    existing_segnos = lappend_int(existing_segnos, i);
                }
                return existing_segnos;
            }

            if (Debug_appendonly_print_segfile_choice)
            {
                ereport(LOG, (errmsg("SetSegnoForWrite: Choosing a segno for append-only "
                                "relation \"%s\" (%d) ",
                                get_rel_name(relid), relid)));
            }

            LWLockAcquire(AOSegFileLock, LW_EXCLUSIVE);

            /*
             * The most common case is likely the entry exists already.
             */
            aoentry = AORelLookupHashEntry(relid);
            if (aoentry == NULL)
            {
                /*
                 * We need to create a hash entry for this relation.
                 *
                 * However, we need to access the pg_appendonly system catalog
                 * table, so AORelCreateHashEntry will carefully release the AOSegFileLock,
                 * gather the information, and then re-acquire AOSegFileLock.
                 */
                if(!AORelCreateHashEntry(relid))
                {
                    LWLockRelease(AOSegFileLock);
                    ereport(ERROR, (errmsg("can't have more than %d different append-only "
                                    "tables open for writing data at the same time. "
                                    "if tables are heavily partitioned or if your "
                                    "workload requires, increase the value of "
                                    "max_appendonly_tables and retry",
                                    MaxAppendOnlyTables)));
                }

                /* get the hash entry for this relation (must exist) */
                aoentry = AORelGetHashEntry(relid);
            }
            aoentry->txns_using_rel++;

            if (Debug_appendonly_print_segfile_choice)
            {
                ereport(LOG, (errmsg("SetSegnoForWrite: got the hash entry for relation \"%s\" (%d). "
                                "setting txns_using_rel to %d",
                                get_rel_name(relid), relid,
                                aoentry->txns_using_rel)));
            }

            /*
             * Now pick a segment that is not in use and is not over the
             * allowed size threshold (90% full).
             *
             * However, if we already picked a segno for a previous statement
             * in this very same transaction we are still in (explicit txn) we
             * pick the same one to insert into it again.
             */

            /* We go through the open segfile status first to check
             * whether are some in the same txn.
             */
            maxSegno4Segment = palloc0(sizeof(struct AOSegfileStatus*)*segment_num);
            segfilestatus = &(aoentry->head_rel_segfile);
            do
            {
                if (!segfilestatus->isfull)
                {
                    if (segfilestatus->xid == CurrentXid && reuse_segfilenum_in_same_xid)
                    {
                        has_same_txn_status = true;
                        if(CheckSegFileForWriteIfNeeded(aoentry, segfilestatus)){
                            remaining_num = addCandidateSegno(maxSegno4Segment, segment_num, segfilestatus, keepHash);
                        }
                    }
                }
                next = segfilestatus->next;
                if (next == NEXT_END_OF_LIST)
                {
                    break;
                }
                segfilestatus = &AOSegfileStatusPool[next];
            } while(remaining_num > 0);

            /*
             * In the same txn.
             */
            if (has_same_txn_status)
            {
                aoentry->txns_using_rel--; /* re-adjust */
            }

            /*
             * Go through the open segfile status again to
             * find enough ones
             */
            if (remaining_num > 0)
            {
                segfilestatus = &(aoentry->head_rel_segfile);
                do
                {
                    if (!segfilestatus->isfull)
                    {
                        if(!segfilestatus->inuse && !usedByConcurrentTransaction(segfilestatus))
                        {
                            if(CheckSegFileForWriteIfNeeded(aoentry, segfilestatus)){
                                remaining_num = addCandidateSegno(maxSegno4Segment, segment_num, segfilestatus, keepHash);
                            }
                        }
                    }
                    next = segfilestatus->next;
                    if (next == NEXT_END_OF_LIST)
                    {
                        break;
                    }
                    segfilestatus = &AOSegfileStatusPool[next];
                } while(remaining_num > 0);
            }

            /* If the found segfile status are still no enough,
             * we need to get new ones from the status pool.
             */
			while(remaining_num>0)
			{
				//generate new segment_num to make sure that in keepHash mode, all segment node has at least one segfile is writable
				int new_status = AORelGetSegfileStatus(aoentry);
				if (new_status == NEXT_END_OF_LIST)
				{
					LWLockRelease(AOSegFileLock);
					ereport(ERROR, (errmsg("cannot open more than %d append-only table segment files concurrently",
									MaxAORelSegFileStatus)));
				}
				AOSegfileStatusPool[new_status].segno = ++aoentry->max_seg_no;
				AOSegfileStatusPool[new_status].next = aoentry->head_rel_segfile.next;
				aoentry->head_rel_segfile.next = new_status;
				remaining_num = addCandidateSegno(maxSegno4Segment, segment_num,  &AOSegfileStatusPool[new_status], keepHash);
			}
			Assert(remaining_num==0);//make sure all segno got a candidate


            for (int i = 0; i < segment_num; i++)
            {
                AOSegfileStatus *status = maxSegno4Segment[i];  
                status->inuse = true;
                status->xid = CurrentXid;
                existing_segnos = lappend_int(existing_segnos,  status->segno);
            }
            Assert(list_length(existing_segnos) == segment_num);

            LWLockRelease(AOSegFileLock);

            if(maxSegno4Segment) pfree(maxSegno4Segment);
            return existing_segnos;

            /* fix this for dispatch agent. for now it's broken anyway. */
        default:
            Assert(false);
            return NIL;
    }

}

/*
 * assignPerRelSegno
 *
 * For each relation that may get data inserted into it call SetSegnoForWrite
 * to assign a segno to insert into. Create a list of relid-to-segno mappings
 * and return it to the caller.
 *
 * when the all_relids list has more than one entry in it we are inserting into
 * a partitioned table. note that we assign a segno for each AO partition, even
 * if eventually no data will get inserted into it (since we can't know ahead
 * of time).
 */
List *assignPerRelSegno(List *all_relids, int segment_num)
{
	ListCell *cell;
	List     *mapping = NIL;

	foreach(cell, all_relids)
	{
		Oid cur_relid = lfirst_oid(cell);
        Relation rel = heap_open(cur_relid, RowExclusiveLock);

		if (RelationIsAoRows(rel) || RelationIsParquet(rel))
		{
			SegfileMapNode *n;

			n = makeNode(SegfileMapNode);
			n->relid = cur_relid;

			n->segnos = SetSegnoForWrite(NIL, cur_relid, segment_num, false, true, true);

			Assert(n->relid != InvalidOid);
			Assert(n->segnos != NIL);

			mapping = lappend(mapping, n);

			if (Debug_appendonly_print_segfile_choice)
			{
				/*
				ereport(LOG, (errmsg("assignPerRelSegno: Appendonly Writer assigned segno %d to "
									"relid %d for this write operation",
									 n->segno, n->relid)));
				*/
			}

		}

        /*
         * hold RowExclusiveLock until the end of transaction
         */
		heap_close(rel, NoLock);
	}

	return mapping;
}



/*
 * UpdateMasterAosegTotals
 *
 * Update the aoseg table on the master node with the updated row count of
 * the whole distributed relation. We use this information later on to keep
 * track of file 'segments' and their EOF's and decide which segno to use in
 * future writes into the table.
 */
void UpdateMasterAosegTotals(Relation parentrel, int segno, uint64 tupcount)
{
	AORelHashEntry	aoHashEntry = NULL;
	AppendOnlyEntry *aoEntry = NULL;
	AOSegfileStatus *segfilestatus = NULL;

	Assert(Gp_role == GP_ROLE_DISPATCH);
	Assert(segno >= 0);

	if (Debug_appendonly_print_segfile_choice)
	{
		ereport(LOG,(errmsg("UpdateMasterAosegTotals: Updating aoseg entry for append-only relation %d "
							"with " INT64_FORMAT " new tuples for segno %d",
							RelationGetRelid(parentrel), (int64)tupcount, segno)));
	}

	if((int64) tupcount < 0)
		ereport(ERROR,
				(errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
				 errmsg("int64 out of range (" INT64_FORMAT ")", (int64) tupcount)));

	aoEntry = GetAppendOnlyEntry(RelationGetRelid(parentrel), SnapshotNow);
	Assert(aoEntry != NULL);

	// CONSIDER: We should probably get this lock even sooner.
	LockRelationAppendOnlySegmentFile(
									&parentrel->rd_node,
									segno,
									AccessExclusiveLock,
									/* dontWait */ false);

	if(RelationIsAoRows(parentrel))
	{
		FileSegInfo		*fsinfo;

		/* get the information for the file segment we inserted into */

		/*
		 * Since we have an exclusive lock on the segment-file entry, we can use SnapshotNow.
		 */
        fsinfo = GetFileSegInfo(parentrel, aoEntry, SnapshotNow, segno);
		if (fsinfo == NULL)
		{
			InsertInitialSegnoEntry(aoEntry, segno);
		}
		else
		{
			pfree(fsinfo);
		}

		/*
		 * Update the master AO segment info table with correct tuple count total
		 */
        UpdateFileSegInfo(parentrel, aoEntry, segno, 0, 0, tupcount, 0);
	}
    else
    {
    		Assert(RelationIsParquet(parentrel));

    		ParquetFileSegInfo		*fsinfo;

		/* get the information for the file segment we inserted into */

		/*
		 * Since we have an exclusive lock on the segment-file entry, we can use SnapshotNow.
		 */
		fsinfo = GetParquetFileSegInfo(parentrel, aoEntry, SnapshotNow, segno);
		if (fsinfo == NULL)
		{
			InsertInitialParquetSegnoEntry(aoEntry, segno);
		}
		else
		{
			pfree(fsinfo);
		}

		/*
		 * Update the master Parquet segment info table with correct tuple count total
		 */
		UpdateParquetFileSegInfo(parentrel, aoEntry, segno, 0, 0, tupcount);
    }

	/*
	 * Now, update num of tups added in the hash table. This pending count will
	 * get added to the total count when the transaction actually commits.
	 * (or will get discarded if it aborts). We don't need to do the same
	 * trick for the aoseg table since MVCC protects us there in case we abort.
	 */
	aoHashEntry = AORelGetHashEntry(RelationGetRelid(parentrel));
	segfilestatus = AORelLookupSegfileStatus(segno, aoHashEntry);
	segfilestatus->tupsadded += tupcount;

	pfree(aoEntry);
}


/*
 * Pre-commit processing for append only tables.
 *
 * Update the tup count of all hash entries for all AO table writes that
 * were executed in this transaction.
 *
 * Note that we do NOT free the segno entry quite yet! we must do it later on,
 * in AtEOXact_AppendOnly
 *
 * NOTE: if you change this function, take a look at AtAbort_AppendOnly and
 * AtEOXact_AppendOnly as well.
 */
    void
AtCommit_AppendOnly(bool isSubTransaction)
{
    HASH_SEQ_STATUS status;
    AORelHashEntry	aoentry = NULL;
    AppendOnlyHashEntryPendingCleanup *pending;
    TransactionId 	CurrentXid = GetTopTransactionId();

    if (Gp_role != GP_ROLE_DISPATCH)
        return;

    LWLockAcquire(AOSegFileLock, LW_EXCLUSIVE);

    /*
     * merge into parent transaction pending delete list
     */
    if (isSubTransaction)
    {
        int currentLevel = GetCurrentTransactionNestLevel();

        hash_seq_init(&status, AppendOnlyHashEntryPendingDeleteCleanup);

        while ((pending = (AppendOnlyHashEntryPendingCleanup *) hash_seq_search(&status)) != NULL)
        {
            /*
             * merge sub transaction request to parent
             */
            if (pending->nestedLevel >= currentLevel)
                pending->nestedLevel = currentLevel - 1;
        }

        LWLockRelease(AOSegFileLock);
        return;
    }

    /*
     * at top transaction commit, remove all required entry.
     */
    hash_seq_init(&status, AppendOnlyHashEntryPendingDeleteCleanup);

    while ((pending = (AppendOnlyHashEntryPendingCleanup *) hash_seq_search(&status)) != NULL)
    {
        if (InvalidOid != pending->relid)
            AORelRemoveHashEntry(pending->relid, true);
    }


    hash_seq_init(&status, AppendOnlyHash);

	/*
	 * for each AO table hash entry
	 */
	while ((aoentry = (AORelHashEntryData *) hash_seq_search(&status)) != NULL)
	{
		/*
		 * Only look at tables that are marked in use currently
		 */
		if(aoentry->txns_using_rel > 0)
		{
			AOSegfileStatus *segfilestat = &aoentry->head_rel_segfile;
			int next = 0;

			/*
			 * Was any segfile was updated in our own transaction?
			 */
			do
			{
				TransactionId 	InsertingXid = segfilestat->xid;

				if(InsertingXid == CurrentXid)
				{
					/* bingo! */

					Assert(segfilestat->inuse);

					if (Debug_appendonly_print_segfile_choice)
					{
						ereport(LOG, (errmsg("AtCommit_AppendOnly: found a segno that inserted in "
								  "our txn for table %d. Updating segno %d "
								  "tupcount: old count " INT64_FORMAT ", tups "
								  "added in this txn " INT64_FORMAT " new "
								  "count " INT64_FORMAT, aoentry->relid, segfilestat->segno,
								  (int64)segfilestat->tupcount,
								  (int64)segfilestat->tupsadded,
								  (int64)segfilestat->tupcount +
								  (int64)segfilestat->tupsadded )));
					}

					/* now do the in memory update */
					segfilestat->tupcount += segfilestat->tupsadded;
					segfilestat->tupsadded = 0;
					segfilestat->isfull =
							(segfilestat->tupcount > segfileMaxRowThreshold());

					//Assert(!TransactionIdIsValid(segfilestat->latestWriteXid) ||
					//	   TransactionIdPrecedes(segfilestat->latestWriteXid,
					//			   	   	   	   	   GetMasterTransactionId()));

					segfilestat->latestWriteXid = GetMasterTransactionId();
				}
				next = segfilestat->next;
				if (next == NEXT_END_OF_LIST)
				{
					break;
				}
				segfilestat = &AOSegfileStatusPool[next];
			}while (true);
		}
	}

	LWLockRelease(AOSegFileLock);
}

/*
 * Abort processing for append only tables.
 *
 * Zero-out the tupcount of all hash entries for all AO table writes that
 * were executed in this transaction.
 *
 * Note that we do NOT free the segno entry quite yet! we must do it later on,
 * in AtEOXact_AppendOnly
 *
 * NOTE: if you change this function, take a look at AtCommit_AppendOnly and
 * AtEOXact_AppendOnly as well.
 */
    void
AtAbort_AppendOnly(bool isSubTransaction)
{
    bool            found;
    HASH_SEQ_STATUS status;
    AORelHashEntry	aoentry = NULL;
    AppendOnlyHashEntryPendingCleanup *pending;
    TransactionId 	CurrentXid = GetTopTransactionId();

    if (Gp_role != GP_ROLE_DISPATCH|| CurrentXid == InvalidTransactionId)
        return;

    LWLockAcquire(AOSegFileLock, LW_EXCLUSIVE);

    if (isSubTransaction)
    {
        int currentLevel = GetCurrentTransactionNestLevel();

        /*
         * ignore aborted request
         */
        hash_seq_init(&status, AppendOnlyHashEntryPendingDeleteCleanup);

        while ((pending = (AppendOnlyHashEntryPendingCleanup *) hash_seq_search(&status)) != NULL)
        {
            if (pending->nestedLevel >= currentLevel)
            {
                /*
                 * the sub transaction which include the truncate/alter operator has been aborted,
                 * so its aoentry is not staled anymore.
                 */

                aoentry = AORelLookupHashEntry(pending->relid);

                if (aoentry)
                {
                    Insist(aoentry->staleTid == CurrentXid);
                    aoentry->staleTid = InvalidTransactionId; //clear flag stale
                }

                hash_search(AppendOnlyHashEntryPendingDeleteCleanup,
                        (void *) &pending->relid,
                        HASH_REMOVE,
                        &found);
            }
        }
    }

    hash_seq_init(&status, AppendOnlyHash);

    /*
     * for each AO table hash entry
     */
    while ((aoentry = (AORelHashEntryData *) hash_seq_search(&status)) != NULL)
    {
        if (!isSubTransaction)
        {
            /*
             * the truncate/alter has aborted, not staled anymore
             */
            if (aoentry->staleTid == CurrentXid)
            {
                aoentry->staleTid = InvalidTransactionId;
            }
        }
        /*
		 * Only look at tables that are marked in use currently
		 */
		if(aoentry->txns_using_rel > 0)
		{
			AOSegfileStatus *segfilestat = &aoentry->head_rel_segfile;
			int next = 0;
			/*
			 * Was any segfile  updated in our own transaction?
			 */
			do
			{
				TransactionId 	InsertingXid = segfilestat->xid;

				if(InsertingXid == CurrentXid)
				{
					/* bingo! */

					Assert(segfilestat->inuse);

					if (Debug_appendonly_print_segfile_choice)
					{
						ereport(LOG, (errmsg("AtAbort_AppendOnly: found a segno that inserted in our txn for "
								  "table %d. Cleaning segno %d tupcount: old "
								  "count " INT64_FORMAT " tups added in this "
								  "txn " INT64_FORMAT ", count "
								  "remains " INT64_FORMAT, aoentry->relid, segfilestat->segno,
								  (int64)segfilestat->tupcount,
								  (int64)segfilestat->tupsadded,
								  (int64)segfilestat->tupcount)));
					}

                    /* now do the in memory cleanup. tupcount not touched */
                    if(!isSubTransaction)
                        segfilestat->tupsadded = 0;

                    segfilestat->needCheck = true;
				}
				next = segfilestat->next;
				if (next == NEXT_END_OF_LIST)
				{
					break;
				}
				segfilestat = &AOSegfileStatusPool[next];
			}while (true);
		}
	}

	LWLockRelease(AOSegFileLock);
}

/*
 * End of txn processing for append only tables.
 *
 * This must be called after the AtCommit or AtAbort functions above did their
 * job already updating the tupcounts in shared memory. We now find the relevant
 * entries again and mark them not in use, so that the AO writer could allocate
 * them to future txns.
 *
 * NOTE: if you change this function, take a look at AtCommit_AppendOnly and
 * AtAbort_AppendOnly as well.
 */

    void
AtEOXact_AppendOnly(void)
{
	HASH_SEQ_STATUS status;
	AORelHashEntry	aoentry = NULL;
    TransactionId 	CurrentXid = GetTopTransactionId();
	bool			entry_updated = false;

	if (Gp_role != GP_ROLE_DISPATCH)
		return;

	hash_seq_init(&status, AppendOnlyHash);

	LWLockAcquire(AOSegFileLock, LW_EXCLUSIVE);

    //clean up
    if(AppendOnlyHashEntryPendingDeleteCleanup)
    {
        hash_destroy(AppendOnlyHashEntryPendingDeleteCleanup);
        AppendOnlyHashEntryPendingDeleteCleanup = NULL;
    }
	/*
	 * for each AO table hash entry
	 */
	while ((aoentry = (AORelHashEntryData *) hash_seq_search(&status)) != NULL)
	{
	    Insist(aoentry->staleTid != CurrentXid);
		/*
		 * Only look at tables that are marked in use currently
		 */
		if(aoentry->txns_using_rel > 0)
		{
			AOSegfileStatus *segfilestat = &aoentry->head_rel_segfile;
			int next = 0;
			entry_updated = false;

			/*
			 * Was any segfile  updated in our own transaction?
			 */
			do
			{
				TransactionId 	InsertingXid = segfilestat->xid;
				if (InsertingXid == CurrentXid)
				{
					/* bingo! */

					Assert(segfilestat->inuse);

					if (Debug_appendonly_print_segfile_choice)
					{
						ereport(LOG, (errmsg("AtEOXact_AppendOnly: found a segno that inserted in our txn for "
								  "table %d. Releasing segno %d. It can now be "
								  "used by another incoming AO transaction",
								  aoentry->relid, segfilestat->segno)));
					}
					segfilestat->inuse = false;
					segfilestat->xid = InvalidTransactionId;
					entry_updated = true;
				}
				next = segfilestat->next;
				if (next == NEXT_END_OF_LIST)
				{
					break;
				}
				segfilestat = &AOSegfileStatusPool[next];
			} while (true);


			/* done with this AO table entry */
			if(entry_updated)
			{
				aoentry->txns_using_rel--;
				
				if (Debug_appendonly_print_segfile_choice)
				{
					ereport(LOG, (errmsg("AtEOXact_AppendOnly: updated txns_using_rel, it is now %d",
										  aoentry->txns_using_rel)));	
				}
			}
				
				
		}
	}

	LWLockRelease(AOSegFileLock);

}

void ValidateAppendOnlyMetaDataSnapshot(
	Snapshot *appendOnlyMetaDataSnapshot)
{
	// Placeholder.
}

static bool
AORelFreeSegfileStatus(AORelHashEntry currentEntry)
{
  HASH_SEQ_STATUS status;
  AORelHashEntryData  *hentry;
  bool freedOne = false;
  /*
   * loop through all entries looking for an unused one
   */
  hash_seq_init(&status, AppendOnlyHash);

  while ((hentry = (AORelHashEntryData *) hash_seq_search(&status)) != NULL)
  {
    if(hentry->txns_using_rel == 0 && InvalidTransactionId==hentry->staleTid
    		&& hentry != currentEntry)
    {
      if (Debug_appendonly_print_segfile_choice)
      {
        ereport(LOG, (errmsg("AppendOnlyRelHashNew: Appendonly Writer removing an "
                   "unused entry (rel %d) to make "
                   "room for a new one",
                   hentry->relid)));
      }

      /* remove this unused entry */
      /* TODO: remove the LRU entry, not just any unused one */
      freedOne = AORelRemoveHashEntry(hentry->relid, false);
      hash_seq_term(&status);
      break;
    }
  }
  return freedOne;
}

static int
AORelGetSegfileStatus(AORelHashEntry currentEntry)
{
	int result;


	while (AppendOnlyWriter->num_existing_segfilestatus + 1 > MaxAORelSegFileStatus)
	{
	  bool freedOne = AORelFreeSegfileStatus(currentEntry);
	  if (!freedOne)
	  {
	    return NEXT_END_OF_LIST;
	  }
	}

	result = AppendOnlyWriter->head_free_segfilestatus;
	Assert(result != NEXT_END_OF_LIST);
	AppendOnlyWriter->num_existing_segfilestatus++;
	AppendOnlyWriter->head_free_segfilestatus = AOSegfileStatusPool[result].next;

	return result;
}

static void
AORelPutSegfileStatus(int id)
{
	Assert((id >= 0) && (id < MaxAORelSegFileStatus));
	Assert(AOSegfileStatusPool[id].id == id);

	AOSegfileStatusPool[id].next = AppendOnlyWriter->head_free_segfilestatus;
	AppendOnlyWriter->head_free_segfilestatus = id;

	AppendOnlyWriter->num_existing_segfilestatus--;

	return;
}

static AOSegfileStatus *
AORelLookupSegfileStatus(int segno, AORelHashEntry entry)
{
	AOSegfileStatus *result = NULL;
	AOSegfileStatus *status = &entry->head_rel_segfile;
	int next;
	do
	{
		if (status->segno == segno)
		{
			result = status;
			break;
		}
		next = status->next;
		if (next == NEXT_END_OF_LIST)
		{
			break;
		}
		status = &AOSegfileStatusPool[next];
	}while (true);

	return result;
}

struct SegFileMap
{
    int64	eof;
    bool	ok;
};

/*  Get segment file length info from catalog.
 *  The length is maintained by hawq, which is different from hdfs file length,
 *  because hdfs file may contain some garbag data which is generated by rollbacking the insert transaction
 */
    static struct SegFileMap *
GetSegmentFileLengthMapping(Relation rel, Oid segrelid, int segno, int *totalsegs)
{
    AppendOnlyEntry		aoEntry;
    struct SegFileMap	*retval;
    int					i;

    memset(&aoEntry, 0, sizeof(aoEntry));
    aoEntry.segrelid = segrelid;

    if (RelationIsAoRows(rel)) {
        FileSegInfo **fsinfo;

        fsinfo = GetAllFileSegInfoWithSegno(rel, &aoEntry, SnapshotNow, segno, totalsegs);
        if (fsinfo == NULL)
        {
            return NULL;
        }
        else
        {
            retval = palloc0(sizeof(struct SegFileMap) * *totalsegs);

            for (i = 0; i < *totalsegs; ++i)
            {
                retval[i].eof = fsinfo[i]->eof;
                retval[i].ok = false;
            }

            pfree(fsinfo);
        }
    }
    else
    {
        Assert(RelationIsParquet(rel));
        ParquetFileSegInfo **fsinfo;

        fsinfo = GetAllParquetFileSegInfoWithSegno(rel, &aoEntry, SnapshotNow, segno, totalsegs);

        if (fsinfo == NULL)
        {
            return NULL;
        }
        else
        {
            retval = palloc0(sizeof(struct SegFileMap) * *totalsegs);

            for (i = 0; i < *totalsegs; ++i)
            {
                retval[i].eof = fsinfo[i]->eof;
                retval[i].ok = false;
            }

            pfree(fsinfo);
        }
    }

    for (i = 0; i < *totalsegs; ++i)
    {
        if (retval[i].eof != 0)
            return retval;
    }

    pfree(retval);
    return NULL;
}

/*  Check the seg file status for write: those segfile which contain garbage data can't be be used for INSERT any more.
 *  Comparing the seg file length in system catalog with the length fetched from hdfs.
 *
 */
    static bool
CheckSegFileForWrite(AORelHashEntryData *aoentry, int segno)
{
    bool retVal=true;
    int64 len;
    int				i, j, totalsegs;
    char    		*token = "";
    char *basepath, *segfile_path;
    Relation rel = NULL;
    RelFileNode		*node = NULL;
    AORelHelp aohelp = NULL;
    struct SegFileMap * mapping = NULL;

    if (Gp_role == GP_ROLE_EXECUTE)
        elog(ERROR, "CheckSegFileForWrite() cannot be called on segment node");


    rel = heap_open(aoentry->relid, RowExclusiveLock);
    node = &rel->rd_node;
    /* get necessary information before lock.
     * aohelp maybe NULL.
     */
    aohelp = GetAOHashTableHelpEntry(aoentry->relid);
    if (!aohelp){
        goto ReturnPoint;
    }

    //get seg file length from catalog
    mapping = GetSegmentFileLengthMapping(rel,
            aohelp->segrelid, segno, &totalsegs);

    if (!mapping){
        goto ReturnPoint;
    }
    //get seg file path in hdfs
    basepath = relpath(rel->rd_node);
    segfile_path = (char*) palloc0(strlen(basepath)+9);

    // AOCS is not supported any more
    FormatAOSegmentFileName(basepath, segno, -1, 0,&segno, segfile_path);

    if (enable_secure_filesystem)
    {
        add_filesystem_credential(aohelp->tspPathPrefix);
        token = find_filesystem_credential_with_uri(aohelp->tspPathPrefix);
        if (token != NULL && strlen(token) > 0)
            add_filesystem_credential_to_cache((const char*)segfile_path, token);
    }

    len = HdfsGetFileLength(segfile_path);
    if (len < 0)
    {
        ereport(ERROR,
                (errcode(ERRCODE_IO_ERROR),
                 errmsg( "failed to get file length %s", segfile_path),
                 errdetail("%s", HdfsGetLastError())));
    }

    for (j = 0; j < totalsegs; ++j)
    {
        if(mapping[j].eof == len) // && mapping[j].contentid == GpIdentity.segindex)
        {
            mapping[j].ok = true;
            break;
        }
    }

    for (i = 0; i < totalsegs; ++i)
    {
        if (mapping[i].ok == false){
            retVal = false;
            break;
        }
    }

ReturnPoint:
    heap_close(rel, RowExclusiveLock);
    if(mapping)
        pfree(mapping);
    if(aohelp)
        pfree(aohelp);

    return retVal;
}

/* Only if the flag AOSegfileStatus::needcheck is set, then CheckSegFileForWrite(), and clear this flag.
 * Return:  true --the segfile is valid for write.
 *
 *  Now we just perform check according the flag in AOSegfileStatus::needcheck, which is set to true initially. The cause why
 *  We don't add a flag in the system catalog is:
 *  If the system crashed/powerdown befor the load tx commit, the readonly flag is not correctly set
 *  because AO doesnot write WAL. And also we only need to check when performing insert, which is not
 *  common operation for AO. So we just check each time before write seg file.
 */
    static bool
CheckSegFileForWriteIfNeeded(AORelHashEntryData *aoentry, AOSegfileStatus * segfilesstatus)
{
    bool isOkay = true;

    if(segfilesstatus->needCheck){
        isOkay = CheckSegFileForWrite(aoentry, segfilesstatus->segno);
        if(!isOkay){//clear the flag
            //elog(INFO, "Segfile %d is invalid and became read only for relation %d.",  segfilesstatus->segno, aoentry->relid);
            segfilesstatus->inuse = true;
            segfilesstatus->xid = InvalidTransactionId;
        }else{
            //elog(INFO, "Segfile %d is checked, but still ready for write relation %d.",  segfilesstatus->segno, aoentry->relid);
        }
        segfilesstatus->needCheck = false;
    }
    return isOkay;
}

    static bool
TestCurrentTspSupportTruncateInternal(Oid tsp, const char *tspPathPrefix)
{
    int             rc, i;
    MemoryContext   old;
    char            path[MAXPGPATH + 1];

    for (i = 0; i < TspSupportTruncateMap.size; ++i)
    {
        if (TspSupportTruncateMap.tsps[i] == tsp)
            return TspSupportTruncateMap.values[i];
    }

    if (TspSupportTruncateMap.size >= TspSupportTruncateMap.capacity)
    {
        old = MemoryContextSwitchTo(TopMemoryContext);

        if (TspSupportTruncateMap.capacity == 0)
        {
            TspSupportTruncateMap.tsps = palloc(sizeof(Oid));
            TspSupportTruncateMap.values = palloc(sizeof(Oid));
            TspSupportTruncateMap.size = 0;
            TspSupportTruncateMap.capacity = 1;
        }
        else
        {
            TspSupportTruncateMap.tsps = repalloc(TspSupportTruncateMap.tsps,
                    sizeof(Oid) * TspSupportTruncateMap.capacity * 2);
            TspSupportTruncateMap.values = repalloc(
                    TspSupportTruncateMap.values,
                    sizeof(Oid) * TspSupportTruncateMap.capacity * 2);
            TspSupportTruncateMap.capacity *= 2;
        }

        MemoryContextSwitchTo(old);
    }

    snprintf(path, sizeof(path), "%s0/__NOTEXIST__", tspPathPrefix);

    errno = 0;
    rc = PathFileTruncate(path);
    Insist(rc < 0 && "Truncate a non exist file shuld always fail");

    TspSupportTruncateMap.tsps[TspSupportTruncateMap.size] = tsp;
    TspSupportTruncateMap.values[TspSupportTruncateMap.size] = !(errno == ENOTSUP);
    TspSupportTruncateMap.size += 1;

    return !(errno == ENOTSUP);
}

    bool
TestCurrentTspSupportTruncate(Oid tsp)
{
    char 	*prefix = NULL;
    bool	retval;
    GetFilespacePathPrefix(tsp, &prefix);
    retval = TestCurrentTspSupportTruncateInternal(tsp, prefix);
    pfree(prefix);
    return retval;
}

    Datum
tablespace_support_truncate(PG_FUNCTION_ARGS)
{
    Oid tsp = PG_GETARG_OID(0);

    if (Gp_role == GP_ROLE_EXECUTE)
        elog(ERROR, "tablespace_support_truncate cannot be called on segment");

    if (tsp == DEFAULTTABLESPACE_OID || tsp == GLOBALTABLESPACE_OID)
        elog(ERROR, "tablespace %u does not on distributed file system", tsp);

    PG_RETURN_BOOL(TestCurrentTspSupportTruncate(tsp));
}
