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
#include "access/aocssegfiles.h"                  /* AOCS */
#include "access/heapam.h"			  /* heap_open            */
#include "access/transam.h"			  /* InvalidTransactionId */
#include "utils/lsyscache.h"

#include "cdb/cdbvars.h"			  /* Gp_role              */
#include "cdb/cdbtm.h"
#include "utils/tqual.h"

/*
 * GUC variables
 */
int		MaxAppendOnlyTables;		/* Max # of tables */

/*
 * Global Variables
 */
static HTAB *AppendOnlyHash;			/* Hash of AO tables */
AppendOnlyWriterData	*AppendOnlyWriter;

/*
 * local functions
 */
static bool AOHashTableInit(void);
static AORelHashEntry AppendOnlyRelHashNew(Oid relid, bool *exists);
static AORelHashEntry AORelGetHashEntry(Oid relid);
static AORelHashEntry AORelLookupHashEntry(Oid relid);
static bool AORelCreateHashEntry(Oid relid);

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
	FileSegInfo		**allfsinfo;
	int				i;
	int				total_segfiles = 0;
	AORelHashEntry	aoHashEntry = NULL;
	Relation		aorel;
	AppendOnlyEntry *aoEntry;

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

	allfsinfo = GetAllFileSegInfo(aorel, aoEntry, SnapshotNow, &total_segfiles);
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
	
	/*
	 * Initialize all segfile array to zero
	 */
	for (i = 0 ; i < MAX_AOREL_CONCURRENCY ; i++)
	{
		aoHashEntry->relsegfiles[i].inuse = false;
		aoHashEntry->relsegfiles[i].xid = InvalidTransactionId;
		aoHashEntry->relsegfiles[i].latestWriteXid = InvalidDistributedTransactionId;
		aoHashEntry->relsegfiles[i].isfull = false;
		aoHashEntry->relsegfiles[i].tupcount = 0;
		aoHashEntry->relsegfiles[i].tupsadded = 0;

	}

	/*
	 * Segment file number 0 is reserved for utility mode operations.
	 */
	aoHashEntry->relsegfiles[RESERVED_SEGNO].inuse = true;
	aoHashEntry->relsegfiles[RESERVED_SEGNO].xid = InvalidTransactionId;
	aoHashEntry->relsegfiles[RESERVED_SEGNO].latestWriteXid = InvalidDistributedTransactionId;
	aoHashEntry->relsegfiles[RESERVED_SEGNO].isfull = true;
	aoHashEntry->relsegfiles[RESERVED_SEGNO].tupcount = 0;
	aoHashEntry->relsegfiles[RESERVED_SEGNO].tupsadded = 0;


	/*
	 * update the tupcount of each 'segment' file in the append
	 * only hash according to tupcounts in the pg_aoseg table.
	 */
	for (i = 0 ; i < total_segfiles; i++)
	{
		aoHashEntry->relsegfiles[allfsinfo[i]->segno].tupcount = allfsinfo[i]->tupcount;
	}

	/* record the fact that another hash entry is now taken */
	AppendOnlyWriter->num_existing_aorels++;

	if(allfsinfo)
		pfree(allfsinfo);

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
AORelRemoveHashEntry(Oid relid)
{
	bool				found;
	void				*aoentry;

	Insist(Gp_role == GP_ROLE_DISPATCH);

	aoentry = hash_search(AppendOnlyHash,
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

	if (aoentry == NULL)
		return false;

	AppendOnlyWriter->num_existing_aorels--;

	return true;
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
				if(hentry->txns_using_rel == 0)
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
					AORelRemoveHashEntry(hentry->relid);
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
usedByConcurrentTransaction(AOSegfileStatus *segfilestat, int segno)
{
	DistributedTransactionId latestWriteXid =
		segfilestat->latestWriteXid;

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
	if (TransactionIdPrecedes(getDistributedTransactionId(),
							  latestWriteXid))
	{
		if (Debug_appendonly_print_segfile_choice)
		{
			ereport(LOG, (errmsg("usedByConcurrentTransaction: current distributed transaction id %u preceeds latestWriterXid %x of segno %d, so it is considered concurrent",
								 getDistributedTransactionId(),
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

	if (Debug_appendonly_print_segfile_choice)
	{
		elog(LOG, "usedByConcurrentTransaction: current distributed transaction id = %x, latestWriteXid that uses segno %d is %x",
			 getDistributedTransactionId(), segno, latestWriteXid);
		if (SerializableSnapshot != NULL && SerializableSnapshot->haveDistribSnapshot)
			LogDistributedSnapshotInfo(SerializableSnapshot, "SerializableSnapshot: ");
		if (LatestSnapshot != NULL && LatestSnapshot->haveDistribSnapshot)
			LogDistributedSnapshotInfo(LatestSnapshot, "LatestSnapshot: ");
		if (snapshot->haveDistribSnapshot)
			LogDistributedSnapshotInfo(snapshot, "Used snapshot: ");
	}
	
	if (!snapshot->haveDistribSnapshot)
	{
		if (Debug_appendonly_print_segfile_choice)
		{
			ereport(LOG, (errmsg("usedByConcurrentTransaction: snapshot is not distributed, so it is NOT considered concurrent")));
		}
		return false;
	}

	/* If latestWriterXid is invisible to me, return true. */
	if (latestWriteXid >= snapshot->distribSnapshotWithLocalMapping.header.xmax)
	{
		if (Debug_appendonly_print_segfile_choice)
		{
			ereport(LOG, (errmsg("usedByConcurrentTransaction: latestWriterXid %x of segno %d is >= distributed snapshot xmax %u, so it is considered concurrent",
								 latestWriteXid,
								 segno,
								 snapshot->distribSnapshotWithLocalMapping.header.xmax)));
		}
		return true;
	}

	/*
	 * If latestWriteXid is in in-progress, return true.
	 */
	int32 inProgressCount = snapshot->distribSnapshotWithLocalMapping.header.count;
	
	for (int inProgressNo = 0; inProgressNo < inProgressCount; inProgressNo++)
	{
		if (latestWriteXid ==
			snapshot->distribSnapshotWithLocalMapping.inProgressEntryArray[inProgressNo].distribXid)
		{
			if (Debug_appendonly_print_segfile_choice)
			{
				ereport(LOG, (errmsg("usedByConcurrentTransaction: latestWriterXid %x of segno %d is equal to distributed snapshot in-flight %u, so it is considered concurrent",
									 latestWriteXid,
									 segno,
									 snapshot->distribSnapshotWithLocalMapping.inProgressEntryArray[inProgressNo].distribXid)));
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
int SetSegnoForWrite(int existingsegno, Oid relid)
{
	/* these vars are used in GP_ROLE_DISPATCH only */
	int					i, usesegno = -1;
	bool				segno_chosen = false;
	AORelHashEntryData	*aoentry = NULL;
	TransactionId 		CurrentXid = GetTopTransactionId();


	switch(Gp_role)
	{
		case GP_ROLE_EXECUTE:

			Assert(existingsegno != InvalidFileSegNumber);
			Assert(existingsegno != RESERVED_SEGNO);
			return existingsegno;

		case GP_ROLE_UTILITY:

			Assert(existingsegno == InvalidFileSegNumber);
			return RESERVED_SEGNO;

		case GP_ROLE_DISPATCH:

			Assert(existingsegno == InvalidFileSegNumber);

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
			for (i = 0 ; i < MAX_AOREL_CONCURRENCY ; i++)
			{
				AOSegfileStatus *segfilestat = &aoentry->relsegfiles[i];

				if(!segfilestat->isfull)
				{
					if(!segfilestat->inuse && !segno_chosen &&
					   !usedByConcurrentTransaction(segfilestat, i))
					{
						/*
						 * this segno is avaiable and not full. use it.
						 *
						 * Notice that we don't break out of the loop quite yet.
						 * We still need to check the rest of the segnos, if our
						 * txn is already using one of them. see below.
						 */
						usesegno = i;
						segno_chosen = true;
					}

					if(segfilestat->xid == CurrentXid)
					{
						/* we already used this segno in our txn. use it again */
						Assert(segfilestat->inuse);
						usesegno = i;
						segno_chosen = true;
						aoentry->txns_using_rel--; /* same txn. re-adjust */
						
						if (Debug_appendonly_print_segfile_choice)
						{
							ereport(LOG, (errmsg("SetSegnoForWrite: reusing segno %d for append-"
									 "only relation "
									 "%d. there are " INT64_FORMAT " tuples "
									 "added to it from previous operations "
									 "in this not yet committed txn. decrementing"
									 "txns_using_rel back to %d",
									 usesegno, relid,
									 (int64) segfilestat->tupsadded,
									 aoentry->txns_using_rel)));
						}

						break;
					}
				}
			}

			if(!segno_chosen)
			{
				LWLockRelease(AOSegFileLock);
				ereport(ERROR, (errmsg("could not find segment file to use for "
									   "inserting into relation %s (%d).", 
									   get_rel_name(relid), relid)));
			}
				
			Insist(usesegno != RESERVED_SEGNO);

			/* mark this segno as in use */
			aoentry->relsegfiles[usesegno].inuse = true;
			aoentry->relsegfiles[usesegno].xid = CurrentXid;

			LWLockRelease(AOSegFileLock);

			Assert(usesegno >= 0);
			Assert(usesegno != RESERVED_SEGNO);

			if (Debug_appendonly_print_segfile_choice)
			{
				ereport(LOG, (errmsg("Segno chosen for append-only relation \"%s\" (%d)"
									 "is %d", get_rel_name(relid), relid, usesegno)));
			}

			return usesegno;

		/* fix this for dispatch agent. for now it's broken anyway. */
		default:
			Assert(false);
			return InvalidFileSegNumber;
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
List *assignPerRelSegno(List *all_relids)
{
	ListCell *cell;
	List     *mapping = NIL;

	foreach(cell, all_relids)
	{
		Oid cur_relid = lfirst_oid(cell);
		Relation rel = heap_open(cur_relid, NoLock);

		if (RelationIsAoCols(rel) || RelationIsAoRows(rel))
		{
			SegfileMapNode *n;

			n = makeNode(SegfileMapNode);
			n->relid = cur_relid;
			n->segno = SetSegnoForWrite(InvalidFileSegNumber, cur_relid);

			Assert(n->relid != InvalidOid);
			Assert(n->segno != InvalidFileSegNumber);

			mapping = lappend(mapping, n);

			if (Debug_appendonly_print_segfile_choice)
			{
				ereport(LOG, (errmsg("assignPerRelSegno: Appendonly Writer assigned segno %d to "
									"relid %d for this write operation",
									 n->segno, n->relid)));
			}

		}

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
		AOCSFileSegInfo *seginfo;

    	/* AO column store */
    	Assert(RelationIsAoCols(parentrel));

		seginfo = GetAOCSFileSegInfo(
							parentrel, 
							aoEntry, 
							SnapshotNow, 
							segno);
		if (seginfo == NULL)
		{
			InsertInitialAOCSFileSegInfo(aoEntry->segrelid, segno, parentrel->rd_att->natts);
		}
		else
		{
			pfree(seginfo);
		}
    	AOCSFileSegInfoAddCount(parentrel, aoEntry, segno, tupcount, 0);
    }

	/*
	 * Now, update num of tups added in the hash table. This pending count will
	 * get added to the total count when the transaction actually commits.
	 * (or will get discarded if it aborts). We don't need to do the same
	 * trick for the aoseg table since MVCC protects us there in case we abort.
	 */
	aoHashEntry = AORelGetHashEntry(RelationGetRelid(parentrel));
	aoHashEntry->relsegfiles[segno].tupsadded += tupcount;

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
AtCommit_AppendOnly(void)
{
	HASH_SEQ_STATUS status;
	AORelHashEntry	aoentry = NULL;
	TransactionId 	CurrentXid = GetTopTransactionId();

	if (Gp_role != GP_ROLE_DISPATCH)
		return;
	
	hash_seq_init(&status, AppendOnlyHash);

	LWLockAcquire(AOSegFileLock, LW_EXCLUSIVE);

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
			int i;

			/*
			 * Was any segfile was updated in our own transaction?
			 */
			for (i = 0 ; i < MAX_AOREL_CONCURRENCY ; i++)
			{
				AOSegfileStatus *segfilestat = &aoentry->relsegfiles[i];
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
								  "count " INT64_FORMAT, aoentry->relid, i,
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

					Assert(!TransactionIdIsValid(segfilestat->latestWriteXid) ||
						   TransactionIdPrecedes(segfilestat->latestWriteXid,
												 getDistributedTransactionId()));

					segfilestat->latestWriteXid = getDistributedTransactionId();
				}
			}
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
AtAbort_AppendOnly(void)
{
	HASH_SEQ_STATUS status;
	AORelHashEntry	aoentry = NULL;
	TransactionId 	CurrentXid = GetCurrentTransactionIdIfAny();

	if (Gp_role != GP_ROLE_DISPATCH)
		return;

	hash_seq_init(&status, AppendOnlyHash);

	LWLockAcquire(AOSegFileLock, LW_EXCLUSIVE);

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
			int i;

			/*
			 * Was any segfile  updated in our own transaction?
			 */
			for (i = 0 ; i < MAX_AOREL_CONCURRENCY ; i++)
			{
				AOSegfileStatus *segfilestat = &aoentry->relsegfiles[i];
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
								  "remains " INT64_FORMAT, aoentry->relid, i,
								  (int64)segfilestat->tupcount,
								  (int64)segfilestat->tupsadded,
								  (int64)segfilestat->tupcount)));
					}

					/* now do the in memory cleanup. tupcount not touched */
					segfilestat->tupsadded = 0;
				}
			}
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
	TransactionId 	CurrentXid = GetCurrentTransactionIdIfAny();
	bool			entry_updated = false;

	if (Gp_role != GP_ROLE_DISPATCH)
		return;

	hash_seq_init(&status, AppendOnlyHash);

	LWLockAcquire(AOSegFileLock, LW_EXCLUSIVE);

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
			int i;

			entry_updated = false;

			/*
			 * Was any segfile  updated in our own transaction?
			 */
			for (i = 0 ; i < MAX_AOREL_CONCURRENCY ; i++)
			{
				AOSegfileStatus *segfilestat = &aoentry->relsegfiles[i];
				TransactionId 	InsertingXid = segfilestat->xid;

				if(InsertingXid == CurrentXid)
				{
					/* bingo! */

					Assert(segfilestat->inuse);

					if (Debug_appendonly_print_segfile_choice)
					{
						ereport(LOG, (errmsg("AtEOXact_AppendOnly: found a segno that inserted in our txn for "
								  "table %d. Releasing segno %d. It can now be "
								  "used by another incoming AO transaction",
								  aoentry->relid, i)));
					}

					/* now do the in memory cleanup. tupcount not touched */
					segfilestat->inuse = false;
					segfilestat->xid = InvalidTransactionId;
					entry_updated = true;
				}
			}

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
