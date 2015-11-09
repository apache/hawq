/*-------------------------------------------------------------------------
 *
 * inval.c
 *	  POSTGRES cache invalidation dispatcher code.
 *
 *	This is subtle stuff, so pay attention:
 *
 *	When a tuple is updated or deleted, our standard time qualification rules
 *	consider that it is *still valid* so long as we are in the same command,
 *	ie, until the next CommandCounterIncrement() or transaction commit.
 *	(See utils/time/tqual.c, and note that system catalogs are generally
 *	scanned under SnapshotNow rules by the system, or plain user snapshots
 *	for user queries.)	At the command boundary, the old tuple stops
 *	being valid and the new version, if any, becomes valid.  Therefore,
 *	we cannot simply flush a tuple from the system caches during heap_update()
 *	or heap_delete().  The tuple is still good at that point; what's more,
 *	even if we did flush it, it might be reloaded into the caches by a later
 *	request in the same command.  So the correct behavior is to keep a list
 *	of outdated (updated/deleted) tuples and then do the required cache
 *	flushes at the next command boundary.  We must also keep track of
 *	inserted tuples so that we can flush "negative" cache entries that match
 *	the new tuples; again, that mustn't happen until end of command.
 *
 *	Once we have finished the command, we still need to remember inserted
 *	tuples (including new versions of updated tuples), so that we can flush
 *	them from the caches if we abort the transaction.  Similarly, we'd better
 *	be able to flush "negative" cache entries that may have been loaded in
 *	place of deleted tuples, so we still need the deleted ones too.
 *
 *	If we successfully complete the transaction, we have to broadcast all
 *	these invalidation events to other backends (via the SI message queue)
 *	so that they can flush obsolete entries from their caches.	Note we have
 *	to record the transaction commit before sending SI messages, otherwise
 *	the other backends won't see our updated tuples as good.
 *
 *	When a subtransaction aborts, we can process and discard any events
 *	it has queued.	When a subtransaction commits, we just add its events
 *	to the pending lists of the parent transaction.
 *
 *	In short, we need to remember until xact end every insert or delete
 *	of a tuple that might be in the system caches.	Updates are treated as
 *	two events, delete + insert, for simplicity.  (There are cases where
 *	it'd be possible to record just one event, but we don't currently try.)
 *
 *	We do not need to register EVERY tuple operation in this way, just those
 *	on tuples in relations that have associated catcaches.	We do, however,
 *	have to register every operation on every tuple that *could* be in a
 *	catcache, whether or not it currently is in our cache.	Also, if the
 *	tuple is in a relation that has multiple catcaches, we need to register
 *	an invalidation message for each such catcache.  catcache.c's
 *	PrepareToInvalidateCacheTuple() routine provides the knowledge of which
 *	catcaches may need invalidation for a given tuple.
 *
 *	Also, whenever we see an operation on a pg_class or pg_attribute tuple,
 *	we register a relcache flush operation for the relation described by that
 *	tuple.	pg_class updates trigger an smgr flush operation as well.
 *
 *	We keep the relcache and smgr flush requests in lists separate from the
 *	catcache tuple flush requests.	This allows us to issue all the pending
 *	catcache flushes before we issue relcache flushes, which saves us from
 *	loading a catcache tuple during relcache load only to flush it again
 *	right away.  Also, we avoid queuing multiple relcache flush requests for
 *	the same relation, since a relcache flush is relatively expensive to do.
 *	(XXX is it worth testing likewise for duplicate catcache flush entries?
 *	Probably not.)
 *
 *	If a relcache flush is issued for a system relation that we preload
 *	from the relcache init file, we must also delete the init file so that
 *	it will be rebuilt during the next backend restart.  The actual work of
 *	manipulating the init file is in relcache.c, but we keep track of the
 *	need for it here.
 *
 *	The request lists proper are kept in CurTransactionContext of their
 *	creating (sub)transaction, since they can be forgotten on abort of that
 *	transaction but must be kept till top-level commit otherwise.  For
 *	simplicity we keep the controlling list-of-lists in TopTransactionContext.
 *
 *
 * Portions Copyright (c) 1996-2008, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  $PostgreSQL: pgsql/src/backend/utils/cache/inval.c,v 1.78 2006/10/04 00:30:00 momjian Exp $
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/twophase_rmgr.h"
#include "access/xact.h"
#include "catalog/catalog.h"
#include "catalog/gp_policy.h"
#include "miscadmin.h"
#include "storage/sinval.h"
#include "storage/smgr.h"
#include "utils/inval.h"
#include "utils/mdver.h"
#include "utils/memutils.h"
#include "utils/relcache.h"
#include "utils/simex.h"
#include "utils/syscache.h"
#include "commands/dbcommands.h"
#include "access/heapam.h"
#include "utils/guc.h"



/*
 * To minimize palloc traffic, we keep pending requests in successively-
 * larger chunks (a slightly more sophisticated version of an expansible
 * array).	All request types can be stored as SharedInvalidationMessage
 * records.  The ordering of requests within a list is important for some
 * components (e.g. Metadata Versioning), so we make sure to maintain it.
 */
typedef struct InvalidationChunk
{
	struct InvalidationChunk *next;		/* list link */
	int			nitems;			/* # items currently stored in chunk */
	int			maxitems;		/* size of allocated array in this chunk */
	SharedInvalidationMessage msgs[1];	/* VARIABLE LENGTH ARRAY */
} InvalidationChunk;			/* VARIABLE LENGTH STRUCTURE */

typedef struct InvalidationListHeader
{
	InvalidationChunk *cclist;	/* list of chunks holding catcache msgs */
	InvalidationChunk *rclist;	/* list of chunks holding relcache/smgr msgs */
	InvalidationChunk *velist;  /* list of chunks holding versioning event msgs */
} InvalidationListHeader;

/*----------------
 * Invalidation info is divided into two lists:
 *	1) events so far in current command, not yet reflected to caches.
 *	2) events in previous commands of current transaction; these have
 *	   been reflected to local caches, and must be either broadcast to
 *	   other backends or rolled back from local cache when we commit
 *	   or abort the transaction.
 * Actually, we need two such lists for each level of nested transaction,
 * so that we can discard events from an aborted subtransaction.  When
 * a subtransaction commits, we append its lists to the parent's lists.
 *
 * The relcache-file-invalidated flag can just be a simple boolean,
 * since we only act on it at transaction commit; we don't care which
 * command of the transaction set it.
 *----------------
 */

typedef struct TransInvalidationInfo
{
	/* Back link to parent transaction's info */
	struct TransInvalidationInfo *parent;

	/* Subtransaction nesting depth */
	int			my_level;

	/*
	 * head of current-command event list.
	 * In Metadata versioning, this is the Command Versioning Queue (CVQ)
	 */
	InvalidationListHeader CurrentCmdInvalidMsgs;

	/*
	 * head of previous-commands event list.
	 * In Metadata versioning, this is the Transaction Versioning Queue (XVQ)
	 */
	InvalidationListHeader PriorCmdInvalidMsgs;

	/* init file must be invalidated? */
	bool		RelcacheInitFileInval;

	/* Metadata Versioning: Local Metadata Version cache/hashtable */
	mdver_local_mdvsn *local_mdvsn;

} TransInvalidationInfo;

/*
 * This global variable is used for testing Metadata Versioning, and
 * therefore it needs to be non-static
 */
TransInvalidationInfo *transInvalInfo = NULL;

/*
 * Dynamically-registered callback functions.  Current implementation
 * assumes there won't be very many of these at once; could improve if needed.
 */

#define MAX_CACHE_CALLBACKS 20

static struct CACHECALLBACK
{
	int16		id;				/* cache number or message type id */
	CacheCallbackFunction function;
	Datum		arg;
}	cache_callback_list[MAX_CACHE_CALLBACKS];

static int	cache_callback_count = 0;

/* info values for 2PC callback */
#define TWOPHASE_INFO_MSG			0	/* SharedInvalidationMessage */
#define TWOPHASE_INFO_FILE_BEFORE	1	/* relcache file inval */
#define TWOPHASE_INFO_FILE_AFTER	2	/* relcache file inval */

static void PersistInvalidationMessage(SharedInvalidationMessage *msg);
static void PrepareForRelcacheInvalidation(Oid relid, HeapTuple tuple);
static bool MdVer_IsRedundantNukeEvent(InvalidationListHeader *hdr, mdver_event *mdev);
static void MdVer_PreProcessInvalidMsgs(InvalidationListHeader *dest,
				InvalidationListHeader *src);
/* ----------------------------------------------------------------
 *				Invalidation list support functions
 *
 * These three routines encapsulate processing of the "chunked"
 * representation of what is logically just a list of messages.
 * ----------------------------------------------------------------
 */

/*
 * AddInvalidationMessage
 *		Add an invalidation message to a list (of chunks).
 *
 * We make sure that we maintain the original order of messages by
 * always appending at the end of the list.
 */
static void
AddInvalidationMessage(InvalidationChunk **listHdr,
					   SharedInvalidationMessage *msg)
{
	InvalidationChunk *lastChunk = *listHdr;

	if (lastChunk == NULL)
	{
		/* First time through; create initial chunk */
#define FIRSTCHUNKSIZE 16
		lastChunk = (InvalidationChunk *)
			MemoryContextAlloc(CurTransactionContext,
							   sizeof(InvalidationChunk) +
					(FIRSTCHUNKSIZE - 1) *sizeof(SharedInvalidationMessage));
		lastChunk->nitems = 0;
		lastChunk->maxitems = FIRSTCHUNKSIZE;
		lastChunk->next = *listHdr;
		*listHdr = lastChunk;
	}
	else
	{
		/* We already have chunks in the list. Go to the last one */
		while (NULL != lastChunk->next)
		{
			lastChunk = lastChunk->next;
		}

		/* Is there room in the last chunk? */
		if (lastChunk->nitems >= lastChunk->maxitems)
		{
			/* Need another chunk; double size of last chunk */
			int			chunksize = 2 * lastChunk->maxitems;

			InvalidationChunk * newChunk = (InvalidationChunk *)
					MemoryContextAlloc(CurTransactionContext,
							sizeof(InvalidationChunk) +
							(chunksize - 1) *sizeof(SharedInvalidationMessage));
			newChunk->nitems = 0;
			newChunk->maxitems = chunksize;
			newChunk->next = NULL;

			/* Append new chunk after the last one */
			lastChunk->next = newChunk;
			lastChunk = newChunk;
		}
	}

	/* Okay, add message to last chunk */
	lastChunk->msgs[lastChunk->nitems] = *msg;

	lastChunk->nitems++;
}

/*
 * Append one list of invalidation message chunks to another, resetting
 * the source chunk-list pointer to NULL.
 * Source is appended at the end of the destination to preserve ordering.
 */
static void
AppendInvalidationMessageList(InvalidationChunk **destHdr,
							  InvalidationChunk **srcHdr)
{
	if (NULL == *srcHdr)
	{
		/* nothing to do */
		return;
	}

	InvalidationChunk *chunk = *destHdr;

	if (NULL == chunk)
	{
		/* Destination is empty */
		*destHdr = *srcHdr;
	}
	else
	{
		/* Find last chunk of destination list */
		while (chunk->next != NULL)
			chunk = chunk->next;

		/* Append source list at the end of destination */
		chunk->next = *srcHdr;
	}

	*srcHdr = NULL;
}

/*
 * Process a list of invalidation messages.
 *
 * This is a macro that executes the given code fragment for each message in
 * a message chunk list.  The fragment should refer to the message as *msg.
 */
#define ProcessMessageList(listHdr, codeFragment) \
	do { \
		InvalidationChunk *_chunk; \
		for (_chunk = (listHdr); _chunk != NULL; _chunk = _chunk->next) \
		{ \
			int		_cindex; \
			for (_cindex = 0; _cindex < _chunk->nitems; _cindex++) \
			{ \
				SharedInvalidationMessage *msg = &_chunk->msgs[_cindex]; \
				codeFragment; \
			} \
		} \
	} while (0)

/*
 * Process a list of invalidation messages group-wise.
 *
 * As above, but the code fragment can handle an array of messages.
 * The fragment should refer to the messages as msgs[], with n entries.
 */
#define ProcessMessageListMulti(listHdr, codeFragment) \
	do { \
		InvalidationChunk *_chunk; \
		for (_chunk = (listHdr); _chunk != NULL; _chunk = _chunk->next) \
		{ \
			SharedInvalidationMessage *msgs = _chunk->msgs; \
			int		n = _chunk->nitems; \
			codeFragment; \
		} \
	} while (0)


/* ----------------------------------------------------------------
 *				Invalidation set support functions
 *
 * These routines understand about the division of a logical invalidation
 * list into separate physical lists for catcache and relcache/smgr entries.
 * ----------------------------------------------------------------
 */

/*
 * Add a catcache inval entry
 */
static void
AddCatcacheInvalidationMessage(InvalidationListHeader *hdr,
							   int id, uint32 hashValue,
							   ItemPointer tuplePtr, Oid dbId,
							   SysCacheInvalidateAction action)
{
	SharedInvalidationMessage msg;

	msg.cc.id = (int16) id;
	msg.cc.tuplePtr = *tuplePtr;
	msg.cc.dbId = dbId;
	msg.cc.hashValue = hashValue;
	AddInvalidationMessage(&hdr->cclist, &msg);
	
#ifdef MD_VERSIONING_INSTRUMENTATION
	elog(gp_mdversioning_loglevel, "Invalidation: TYPE=CATCACHE CACHEID=%d ACTION=%d", id, action);
#endif
}

/* 
 * Add a Metadata Versioning versioning event entry to a message list
 *   hdr: The list to be added to
 *   mdev: The event to be added
 */
static void
AddVersioningEventMessage(InvalidationListHeader *hdr, mdver_event *mdev)
{
	Assert(NULL != hdr);
	Assert(NULL != mdev);

	/* Check for last event in the queue. If we're trying to add a nuke, and it's already nuke, skip it */
	if (MdVer_IsRedundantNukeEvent(hdr, mdev))
	{
		return;
	}

	SharedInvalidationMessage msg;
	msg.ve.id = SHAREDVERSIONINGMSG_ID;
	msg.ve.local = true;
	msg.ve.verEvent = *mdev;
	AddInvalidationMessage(&hdr->velist, &msg);
}

/*
 * Detect if adding a Metadata Versioning Nuke event to the event list is redundant.
 * A Nuke message is redundant if the last message in the list is already a Nuke.
 */
static bool
MdVer_IsRedundantNukeEvent(InvalidationListHeader *hdr, mdver_event *mdev)
{
	Assert(NULL != hdr);
	Assert(NULL != mdev);

	if (!mdver_is_nuke_event(mdev)) {
		return false;
	}

	InvalidationChunk *chunk = hdr->velist;
	if (NULL == chunk)
	{
		/* Destination is empty */
		return false;
	}

	/* Find last chunk of destination list */
	while (chunk->next != NULL)
	{
		chunk = chunk->next;
	}

	/* Get the last event from the last chunk. Then check if it's a NUKE */
	SharedInvalidationMessage *last_message = &chunk->msgs[chunk->nitems - 1];
	Assert(last_message->id == SHAREDVERSIONINGMSG_ID);
	mdver_event *last_event = &(last_message->ve.verEvent);
	if (mdver_is_nuke_event(last_event)) {
		return true;
	}

	return false;
}

/*
 * Add a relcache inval entry
 */
static void
AddRelcacheInvalidationMessage(InvalidationListHeader *hdr,
							   Oid dbId, Oid relId)
{
	SharedInvalidationMessage msg;

	/* Don't add a duplicate item */
	/* We assume dbId need not be checked because it will never change */
	ProcessMessageList(hdr->rclist,
					   if (msg->rc.id == SHAREDINVALRELCACHE_ID &&
						   msg->rc.relId == relId)
					   return);

	/* OK, add the item */
	msg.rc.id = SHAREDINVALRELCACHE_ID;
	msg.rc.dbId = dbId;
	msg.rc.relId = relId;
	AddInvalidationMessage(&hdr->rclist, &msg);


#ifdef MD_VERSIONING_INSTRUMENTATION
	elog(gp_mdversioning_loglevel, "Invalidation: TYPE=RELCACHE RELID=%d", relId);
#endif	
}

/*
 * Add an smgr inval entry
 */
static void
AddSmgrInvalidationMessage(InvalidationListHeader *hdr,
						   RelFileNode rnode)
{
	SharedInvalidationMessage msg;

	/* Don't add a duplicate item */
	ProcessMessageList(hdr->rclist,
					   if (msg->sm.id == SHAREDINVALSMGR_ID &&
						   RelFileNodeEquals(msg->sm.rnode, rnode))
					   return);

	/* OK, add the item */
	msg.sm.id = SHAREDINVALSMGR_ID;
	msg.sm.rnode = rnode;
	AddInvalidationMessage(&hdr->rclist, &msg);
#ifdef MD_VERSIONING_INSTRUMENTATION
	elog(gp_mdversioning_loglevel, "Invalidation: TYPE=SMGR RELID=%d", rnode.relNode);
#endif	
}

/*
 * Append one list of invalidation messages to another, resetting
 * the source list to empty.
 */
static void
AppendInvalidationMessages(InvalidationListHeader *dest,
						   InvalidationListHeader *src)
{
	AppendInvalidationMessageList(&dest->cclist, &src->cclist);
	AppendInvalidationMessageList(&dest->rclist, &src->rclist);
	AppendInvalidationMessageList(&dest->velist, &src->velist);
}

/*
 * Execute the given function for all the messages in an invalidation list.
 * The list is not altered.
 *
 * catcache entries are processed first, for reasons mentioned above.
 */
static void
ProcessInvalidationMessages(InvalidationListHeader *hdr,
							void (*func) (SharedInvalidationMessage *msg))
{
	ProcessMessageList(hdr->cclist, func(msg));
	ProcessMessageList(hdr->rclist, func(msg));
	ProcessMessageList(hdr->velist, func(msg));
}

/*
 * As above, but the function is able to process an array of messages
 * rather than just one at a time.
 */
static void
ProcessInvalidationMessageMulti(InvalidationListHeader *hdr,
				 void (*func) (SharedInvalidationMessage *msgs, int n))
{
	ProcessMessageListMulti(hdr->cclist, func(msgs, n));
	ProcessMessageListMulti(hdr->rclist, func(msgs, n));
	ProcessMessageListMulti(hdr->velist, func(msgs, n));
}

/* ----------------------------------------------------------------
 *					  private support functions
 * ----------------------------------------------------------------
 */

/*
 * RegisterCatcacheInvalidation
 *
 * Register an invalidation event for a catcache tuple entry.
 */
static void
RegisterCatcacheInvalidation(int cacheId,
							 uint32 hashValue,
							 ItemPointer tuplePtr,
							 Oid dbId,
							 SysCacheInvalidateAction action)
{
	AddCatcacheInvalidationMessage(&transInvalInfo->CurrentCmdInvalidMsgs,
								   cacheId, hashValue, tuplePtr, dbId, action);
}

/*
 * RegisterRelcacheInvalidation
 *
 * As above, but register a relcache invalidation event.
 */
static void
RegisterRelcacheInvalidation(Oid dbId, Oid relId)
{
	AddRelcacheInvalidationMessage(&transInvalInfo->CurrentCmdInvalidMsgs,
								   dbId, relId);

	/*
	 * Most of the time, relcache invalidation is associated with system
	 * catalog updates, but there are a few cases where it isn't.  Quick
	 * hack to ensure that the next CommandCounterIncrement() will think
	 * that we need to do CommandEndInvalidationMessages().
	 */
	(void) GetCurrentCommandId(/*true*/);

	/*
	 * If the relation being invalidated is one of those cached in the
	 * relcache init file, mark that we need to zap that file at commit.
	 */
	if (RelationIdIsInInitFile(relId))
		transInvalInfo->RelcacheInitFileInval = true;
}

/*
 * RegisterSmgrInvalidation
 *
 * As above, but register an smgr invalidation event.
 */
static void
RegisterSmgrInvalidation(RelFileNode rnode)
{
	AddSmgrInvalidationMessage(&transInvalInfo->CurrentCmdInvalidMsgs,
							   rnode);
							   
	/*
	 * As above, just in case there is not an associated catalog change.
	 */
	(void) GetCurrentCommandId(/*true*/);
}

#ifdef USE_ASSERT_CHECKING
static char *
si_to_str(SharedInvalidationMessage *msg)
{
	StringInfoData buf;
	int i;
	char *s;

	initStringInfo(&buf);
	appendStringInfo(&buf, "message id = %d", msg->id);
	s = (char *)&(msg->cc);
	for (i = 0; i < sizeof(SharedInvalCatcacheMsg); i++)
	{
		if (i == 0)
			appendStringInfo(&buf, " CC:");

		appendStringInfo(&buf, " %x", *(s + i));

	}
	s = (char *)&(msg->rc);
	for (i = 0; i < sizeof(SharedInvalRelcacheMsg); i++)
	{
		if (i == 0)
			appendStringInfo(&buf, " RC:");

		appendStringInfo(&buf, " %x", *(s + i));

	}
	return buf.data;
}
#endif

/*
 * LocalExecuteInvalidationMessage
 *
 * Process a single invalidation message (which could be of any type).
 * Only the local caches are flushed; this does not transmit the message
 * to other backends.
 */
static void
LocalExecuteInvalidationMessage(SharedInvalidationMessage *msg)
{
	int			i;

	if (msg->id >= 0)
	{
		/* When msg->id > 0, this is a CatCache invalidation message */
		if (msg->cc.dbId == MyDatabaseId || msg->cc.dbId == 0)
		{
			CatalogCacheIdInvalidate(msg->cc.id,
									 msg->cc.hashValue,
									 &msg->cc.tuplePtr);

			for (i = 0; i < cache_callback_count; i++)
			{
				struct CACHECALLBACK *ccitem = cache_callback_list + i;

				if (ccitem->id == msg->cc.id)
					(*ccitem->function) (ccitem->arg, InvalidOid);
			}
		}

		return;
	}

	/*
	 * If we got here, we must have msg->id < 0. This means the invalidation
	 * message is for one of the other caches. Find out which one and
	 * take the specific actions for each.
	 */
	Assert(msg->id < 0);
	switch (msg->id)
	{
	case SHAREDINVALRELCACHE_ID:
		if (msg->rc.dbId == MyDatabaseId || msg->rc.dbId == InvalidOid)
		{
			RelationCacheInvalidateEntry(msg->rc.relId);

			for (i = 0; i < cache_callback_count; i++)
			{
				struct CACHECALLBACK *ccitem = cache_callback_list + i;

				if (ccitem->id == SHAREDINVALRELCACHE_ID)
					(*ccitem->function) (ccitem->arg, msg->rc.relId);
			}
		}
		break;

	case SHAREDINVALSMGR_ID:
		/*
		 * We could have smgr entries for relations of other databases, so no
		 * short-circuit test is possible here.
		 */
		smgrclosenode(msg->sm.rnode);
		break;

	case SHAREDVERSIONINGMSG_ID:
		mdver_localhandler_new_event(msg);
		break;

	default:
#ifdef USE_ASSERT_CHECKING
		elog(NOTICE, "invalid SI message: %s", si_to_str(msg));
#endif
		elog(FATAL, "unrecognized SI message id: %d", msg->id);
	}
}

/*
 *		InvalidateSystemCaches
 *
 *		This blows away all tuples in the system catalog caches and
 *		all the cached relation descriptors and smgr cache entries.
 *		Relation descriptors that have positive refcounts are then rebuilt.
 *
 *		We call this when we see a shared-inval-queue overflow signal,
 *		since that tells us we've lost some shared-inval messages and hence
 *		don't know what needs to be invalidated.
 */
static void
InvalidateSystemCaches(void)
{
	int			i;

	ResetCatalogCaches();
	RelationCacheInvalidate();	/* gets smgr cache too */

	mdver_local_mdvsn *local_mdvsn = GetCurrentLocalMDVSN();
	if (NULL != local_mdvsn && mdver_enabled())
	{
		mdver_local_mdvsn_nuke(local_mdvsn);
	}

	for (i = 0; i < cache_callback_count; i++)
	{
		struct CACHECALLBACK *ccitem = cache_callback_list + i;

		(*ccitem->function) (ccitem->arg, InvalidOid);
	}
}

/*
 * This is identical to InvalidateSystemCache, for now, with exception that this is
 * a public interface, mainly because we want to keep the old interface private.
 * At some point in the future we may want to consolidate them, though.
 */
void
ResetSystemCaches(void)
{
	InvalidateSystemCaches();
}

/*
 * PrepareForTupleInvalidation
 *		Detect whether invalidation of this tuple implies invalidation
 *		of catalog/relation cache entries; if so, register inval events.
 */
static void
PrepareForTupleInvalidation(Relation relation, HeapTuple tuple, SysCacheInvalidateAction action)
{
	Oid			tupleRelId;

	/* Do nothing during bootstrap */
	if (IsBootstrapProcessingMode())
		return;

	/*
	 * We only need to worry about invalidation for tuples that are in system
	 * relations; user-relation tuples are never in catcaches and can't affect
	 * the relcache either.
	 */
	if (!IsSystemRelation(relation))
		return;

	/*
	 * TOAST tuples can likewise be ignored here. Note that TOAST tables are
	 * considered system relations so they are not filtered by the above test.
	 */
	if (IsToastRelation(relation))
		return;

	/*
	 * First let the catcache do its thing
	 */
	PrepareToInvalidateCacheTuple(relation, tuple, action,
								  RegisterCatcacheInvalidation);

	/*
	 * Now, is this tuple one of the primary definers of a relcache entry?
	 */
	tupleRelId = RelationGetRelid(relation);
	PrepareForRelcacheInvalidation(tupleRelId, tuple);
}

/*
 * PrepareForRelcacheInvalidation
 *		Detect whether invalidation of this tuple implies invalidation
 *		of catalog/relation cache entries; if so, register inval events.
 */
static void
PrepareForRelcacheInvalidation(Oid relid, HeapTuple tuple)
{
	Oid relationId = InvalidOid;
	Oid databaseId = InvalidOid;
	if (relid == RelationRelationId)
	{
		Form_pg_class classtup = (Form_pg_class) GETSTRUCT(tuple);
		RelFileNode rnode;

		relationId = HeapTupleGetOid(tuple);
		if (classtup->relisshared)
			databaseId = InvalidOid;
		else
			databaseId = MyDatabaseId;

		/*
		 * We need to send out an smgr inval as well as a relcache inval. This
		 * is needed because other backends might possibly possess smgr cache
		 * but not relcache entries for the target relation.
		 *
		 * Note: during a pg_class row update that assigns a new relfilenode
		 * or reltablespace value, we will be called on both the old and new
		 * tuples, and thus will broadcast invalidation messages showing both
		 * the old and new RelFileNode values.	This ensures that other
		 * backends will close smgr references to the old file.
		 *
		 * XXX possible future cleanup: it might be better to trigger smgr
		 * flushes explicitly, rather than indirectly from pg_class updates.
		 */
		if (classtup->reltablespace)
			rnode.spcNode = classtup->reltablespace;
		else if (relstorage_is_ao(classtup->relstorage))
			rnode.spcNode = get_database_dts(databaseId);
		else
			rnode.spcNode = MyDatabaseTableSpace;
		
		rnode.dbNode = databaseId;
		rnode.relNode = classtup->relfilenode;
		RegisterSmgrInvalidation(rnode);
	}
	else if (relid == AttributeRelationId)
	{
		Form_pg_attribute atttup = (Form_pg_attribute) GETSTRUCT(tuple);

		relationId = atttup->attrelid;

		/*
		 * KLUGE ALERT: we always send the relcache event with MyDatabaseId,
		 * even if the rel in question is shared (which we can't easily tell).
		 * This essentially means that only backends in this same database
		 * will react to the relcache flush request. This is in fact
		 * appropriate, since only those backends could see our pg_attribute
		 * change anyway.  It looks a bit ugly though.	(In practice, shared
		 * relations can't have schema changes after bootstrap, so we should
		 * never come here for a shared rel anyway.)
		 */
		databaseId = MyDatabaseId;
	}
	else if (relid == GpPolicyRelationId)
	{
		FormData_gp_policy *gptup = (FormData_gp_policy *) GETSTRUCT(tuple);

		relationId = gptup->localoid;
		databaseId = MyDatabaseId;
	}
	else if (relid == IndexRelationId)
	{
		Form_pg_index indextup = (Form_pg_index) GETSTRUCT(tuple);

		/*
		 * When a pg_index row is updated, we should send out a relcache inval
		 * for the index relation.	As above, we don't know the shared status
		 * of the index, but in practice it doesn't matter since indexes of
		 * shared catalogs can't have such updates.
		 */
		relationId = indextup->indexrelid;
		databaseId = MyDatabaseId;
	}
	else
		return;

	/*
	 * Yes.  We need to register a relcache invalidation event.
	 */
	RegisterRelcacheInvalidation(databaseId, relationId);
}

/* ----------------------------------------------------------------
 *					  public functions
 * ----------------------------------------------------------------
 */

/*
 * AcceptInvalidationMessages
 *		Read and process invalidation messages from the shared invalidation
 *		message queue.
 *
 * Note:
 *		This should be called as the first step in processing a transaction.
 */
void
AcceptInvalidationMessages(void)
{
	ReceiveSharedInvalidMessages(LocalExecuteInvalidationMessage,
								 InvalidateSystemCaches);

	Assert(SysCacheFlushForce_IsValid(gp_test_system_cache_flush_force));

#ifdef USE_TEST_UTILS
	/*
	 * Test code to force cache flushes anytime a flush could happen.
	 *
	 * If used with CLOBBER_FREED_MEMORY, gp_test_system_cache_flush_force provides
	 * a fairly thorough test that the system contains no cache-flush hazards.
	 * However, it also makes the system unbelievably slow --- the regression
	 * tests take about 100 times longer than normal.
	 *
	 * gp_test_system_cache_flush_force_recursive slows things by
	 * at least a factor of 10000, so I wouldn't suggest
	 * trying to run the entire regression tests that way.	It's useful to try
	 * a few simple tests, to make sure that cache reload isn't subject to
	 * internal cache-flush hazards, but after you've done a few thousand
	 * recursive reloads it's unlikely you'll learn more.
	 */
	if (SysCacheFlushForce_Recursive == gp_test_system_cache_flush_force)
	{
		/* potentially recursive cache invalidation */
		InvalidateSystemCaches();
	}
	else
	{
		static bool in_recursion = false;

		if (!in_recursion)
		{
			bool invalidate = (SysCacheFlushForce_NonRecursive == gp_test_system_cache_flush_force);

			if (!invalidate &&
				gp_simex_init &&
				gp_simex_run && 
				gp_simex_class == SimExESClass_CacheInvalidation)
			{
				/*
				 * Same basic idea as above, except using the SimEx facility, the main
				 * advantage of this approach is that it only triggers the invalidation
				 * once per unique call stack, which should make testing significantly
				 * faster.
				 */
				invalidate = (SimExESSubClass_CacheInvalidation == SimEx_CheckInject());
			}

			if (invalidate)
			{
				/* avoid recursive cache invalidation */
				in_recursion = true;
				InvalidateSystemCaches();
				in_recursion = false;
			}
		}
	}
#endif
}

/*
 * AtStart_Inval
 *		Initialize inval lists at start of a main transaction.
 */
void
AtStart_Inval(void)
{
	Assert(transInvalInfo == NULL);
	transInvalInfo = (TransInvalidationInfo *)
		MemoryContextAllocZero(TopTransactionContext,
							   sizeof(TransInvalidationInfo));
	transInvalInfo->my_level = GetCurrentTransactionNestLevel();

	if (mdver_enabled())
	{
		/*
		 * Since we create the TransInvalidationInfo in the TopTransactionContext,
		 * we should create the local mdvsn in the same context as well.
		 */
		MemoryContext oldcxt = MemoryContextSwitchTo(TopTransactionContext);
		transInvalInfo->local_mdvsn = mdver_create_local_mdvsn(transInvalInfo->my_level);
		MemoryContextSwitchTo(oldcxt);
	}
}

/*
 * AtPrepare_Inval
 *		Save the inval lists state at 2PC transaction prepare.
 *
 * In this phase we just generate 2PC records for all the pending invalidation
 * work.
 */
void
AtPrepare_Inval(void)
{
	/* Must be at top of stack */
	Assert(transInvalInfo != NULL && transInvalInfo->parent == NULL);

	/*
	 * Relcache init file invalidation requires processing both before and
	 * after we send the SI messages.
	 */
	if (transInvalInfo->RelcacheInitFileInval)
		RegisterTwoPhaseRecord(TWOPHASE_RM_INVAL_ID, TWOPHASE_INFO_FILE_BEFORE,
							   NULL, 0);

	AppendInvalidationMessages(&transInvalInfo->PriorCmdInvalidMsgs,
							   &transInvalInfo->CurrentCmdInvalidMsgs);

	ProcessInvalidationMessages(&transInvalInfo->PriorCmdInvalidMsgs,
								PersistInvalidationMessage);

	if (transInvalInfo->RelcacheInitFileInval)
		RegisterTwoPhaseRecord(TWOPHASE_RM_INVAL_ID, TWOPHASE_INFO_FILE_AFTER,
							   NULL, 0);
}

/*
 * PostPrepare_Inval
 *		Clean up after successful PREPARE.
 *
 * Here, we want to act as though the transaction aborted, so that we will
 * undo any syscache changes it made, thereby bringing us into sync with the
 * outside world, which doesn't believe the transaction committed yet.
 *
 * If the prepared transaction is later aborted, there is nothing more to
 * do; if it commits, we will receive the consequent inval messages just
 * like everyone else.
 */
void
PostPrepare_Inval(void)
{
	AtEOXact_Inval(false);
}

/*
 * AtSubStart_Inval
 *		Initialize inval lists at start of a subtransaction.
 */
void
AtSubStart_Inval(void)
{
	TransInvalidationInfo *myInfo;

	Assert(transInvalInfo != NULL);
	myInfo = (TransInvalidationInfo *)
		MemoryContextAllocZero(TopTransactionContext,
							   sizeof(TransInvalidationInfo));
	myInfo->parent = transInvalInfo;
	myInfo->my_level = GetCurrentTransactionNestLevel();

	if (mdver_enabled())
	{
		/*
		 * Since we create the TransInvalidationInfo in the TopTransactionContext,
		 * we should create the local mdvsn in the same context as well.
		 */
		MemoryContext oldcxt = MemoryContextSwitchTo(TopTransactionContext);
		myInfo->local_mdvsn = mdver_create_local_mdvsn(myInfo->my_level);
		MemoryContextSwitchTo(oldcxt);
	}

	transInvalInfo = myInfo;

}

/*
 * PersistInvalidationMessage
 *		Write an invalidation message to the 2PC state file.
 */
static void
PersistInvalidationMessage(SharedInvalidationMessage *msg)
{
	RegisterTwoPhaseRecord(TWOPHASE_RM_INVAL_ID, TWOPHASE_INFO_MSG,
						   msg, sizeof(SharedInvalidationMessage));
}

/*
 * inval_twophase_postcommit
 *		Process an invalidation message from the 2PC state file.
 */
void
inval_twophase_postcommit(TransactionId xid, uint16 info,
						  void *recdata, uint32 len)
{
	SharedInvalidationMessage *msg;

	switch (info)
	{
		case TWOPHASE_INFO_MSG:
			msg = (SharedInvalidationMessage *) recdata;
			Assert(len == sizeof(SharedInvalidationMessage));
			SendSharedInvalidMessages(msg, 1);
			break;
		case TWOPHASE_INFO_FILE_BEFORE:
			RelationCacheInitFileInvalidate(true);
			break;
		case TWOPHASE_INFO_FILE_AFTER:
			RelationCacheInitFileInvalidate(false);
			break;
		default:
			Assert(false);
			break;
	}
}


/*
 * AtEOXact_Inval
 *		Process queued-up invalidation messages at end of main transaction.
 *
 * If isCommit, we must send out the messages in our PriorCmdInvalidMsgs list
 * to the shared invalidation message queue.  Note that these will be read
 * not only by other backends, but also by our own backend at the next
 * transaction start (via AcceptInvalidationMessages).	This means that
 * we can skip immediate local processing of anything that's still in
 * CurrentCmdInvalidMsgs, and just send that list out too.
 *
 * If not isCommit, we are aborting, and must locally process the messages
 * in PriorCmdInvalidMsgs.	No messages need be sent to other backends,
 * since they'll not have seen our changed tuples anyway.  We can forget
 * about CurrentCmdInvalidMsgs too, since those changes haven't touched
 * the caches yet.
 *
 * In any case, reset the various lists to empty.  We need not physically
 * free memory here, since TopTransactionContext is about to be emptied
 * anyway.
 *
 * Note:
 *		This should be called as the last step in processing a transaction.
 */
void
AtEOXact_Inval(bool isCommit)
{
	if (isCommit)
	{
		/* Must be at top of stack */
		Assert(transInvalInfo != NULL && transInvalInfo->parent == NULL);

		/*
		 * Relcache init file invalidation requires processing both before and
		 * after we send the SI messages.  However, we need not do anything
		 * unless we committed.
		 */
		if (transInvalInfo->RelcacheInitFileInval)
			RelationCacheInitFileInvalidate(true);

		AppendInvalidationMessages(&transInvalInfo->PriorCmdInvalidMsgs,
								   &transInvalInfo->CurrentCmdInvalidMsgs);

		ProcessInvalidationMessageMulti(&transInvalInfo->PriorCmdInvalidMsgs,
										SendSharedInvalidMessages);

		if (transInvalInfo->RelcacheInitFileInval)
		{
			RelationCacheInitFileInvalidate(false);
		}

	}
	else if (transInvalInfo != NULL)
	{
		/* Must be at top of stack */
		Assert(transInvalInfo->parent == NULL);

		ProcessInvalidationMessages(&transInvalInfo->PriorCmdInvalidMsgs,
									LocalExecuteInvalidationMessage);

		/* TODO gcaragea 05/07/2014: Add support for aborting transactions (MPP-23505) */

		if (mdver_enabled())
		{
			mdver_destroy_local_mdvsn(transInvalInfo->local_mdvsn, transInvalInfo->my_level);
			transInvalInfo->local_mdvsn = NULL;
		}
	}

	transInvalInfo = NULL;
}

/*
 * AtEOSubXact_Inval
 *		Process queued-up invalidation messages at end of subtransaction.
 *
 * If isCommit, process CurrentCmdInvalidMsgs if any (there probably aren't),
 * and then attach both CurrentCmdInvalidMsgs and PriorCmdInvalidMsgs to the
 * parent's PriorCmdInvalidMsgs list.
 *
 * If not isCommit, we are aborting, and must locally process the messages
 * in PriorCmdInvalidMsgs.	No messages need be sent to other backends.
 * We can forget about CurrentCmdInvalidMsgs too, since those changes haven't
 * touched the caches yet.
 *
 * In any case, pop the transaction stack.	We need not physically free memory
 * here, since CurTransactionContext is about to be emptied anyway
 * (if aborting).  Beware of the possibility of aborting the same nesting
 * level twice, though.
 */
void
AtEOSubXact_Inval(bool isCommit)
{
	int			my_level = GetCurrentTransactionNestLevel();
	TransInvalidationInfo *myInfo = transInvalInfo;

	if (isCommit)
	{
		/* Must be at non-top of stack */
		Assert(myInfo != NULL && myInfo->parent != NULL);
		Assert(myInfo->my_level == my_level);

		/* If CurrentCmdInvalidMsgs still has anything, fix it */
		CommandEndInvalidationMessages();

		/* Pass up my inval messages to parent */
		AppendInvalidationMessages(&myInfo->parent->PriorCmdInvalidMsgs,
								   &myInfo->PriorCmdInvalidMsgs);

		/* Pending relcache inval becomes parent's problem too */
		if (myInfo->RelcacheInitFileInval)
			myInfo->parent->RelcacheInitFileInval = true;

		/* Pop the transaction state stack */
		transInvalInfo = myInfo->parent;

		if (mdver_enabled())
		{
			mdver_destroy_local_mdvsn(myInfo->local_mdvsn, myInfo->my_level);
			myInfo->local_mdvsn = NULL;
		}

		/* Need not free anything else explicitly */
		pfree(myInfo);
	}
	else if (myInfo != NULL && myInfo->my_level == my_level)
	{
		/* Must be at non-top of stack */
		Assert(myInfo->parent != NULL);

		ProcessInvalidationMessages(&myInfo->PriorCmdInvalidMsgs,
									LocalExecuteInvalidationMessage);

		/* Pop the transaction state stack */
		transInvalInfo = myInfo->parent;

		if (mdver_enabled())
		{
			mdver_destroy_local_mdvsn(myInfo->local_mdvsn, myInfo->my_level);
			myInfo->local_mdvsn = NULL;
		}

		/* Need not free anything else explicitly */
		pfree(myInfo);
	}

	elog(gp_mdversioning_loglevel, "In AtEOSubXact_Inval. Freeing subxact MDVSN");

}

/*
 * CommandEndInvalidationMessages
 *		Process queued-up invalidation messages at end of one command
 *		in a transaction.
 *
 * Here, we send no messages to the shared queue, since we don't know yet if
 * we will commit.	We do need to locally process the CurrentCmdInvalidMsgs
 * list, so as to flush our caches of any entries we have outdated in the
 * current command.  We then move the current-cmd list over to become part
 * of the prior-cmds list.
 *
 * Note:
 *		This should be called during CommandCounterIncrement(),
 *		after we have advanced the command ID.
 */
void
CommandEndInvalidationMessages(void)
{
	/*
	 * You might think this shouldn't be called outside any transaction, but
	 * bootstrap does it, and also ABORT issued when not in a transaction. So
	 * just quietly return if no state to work on.
	 */
	if (transInvalInfo == NULL)
		return;

	MdVer_PreProcessInvalidMsgs(&transInvalInfo->PriorCmdInvalidMsgs,
			   &transInvalInfo->CurrentCmdInvalidMsgs);

	ProcessInvalidationMessages(&transInvalInfo->CurrentCmdInvalidMsgs,
								LocalExecuteInvalidationMessage);
	AppendInvalidationMessages(&transInvalInfo->PriorCmdInvalidMsgs,
							   &transInvalInfo->CurrentCmdInvalidMsgs);
}

/*
 * CacheInvalidateHeapTuple
 *		Register the given tuple for invalidation at end of command
 *		(ie, current command is creating or outdating this tuple).
 */
void
CacheInvalidateHeapTuple(Relation relation, HeapTuple tuple, SysCacheInvalidateAction action)
{
	PrepareForTupleInvalidation(relation, tuple, action);
}

/*
 * CacheAddVersioningEvent
 * 		Register a new versioning event for propagation at the end of
 * 		command. A copy of the event is added to the queue.
 */
void
CacheAddVersioningEvent(mdver_event *mdev)
{
	if (transInvalInfo == NULL)
	{
		return;
	}

	AddVersioningEventMessage(
			&transInvalInfo->CurrentCmdInvalidMsgs,
			mdev);
}

/*
 * CacheInvalidateRelcache
 *		Register invalidation of the specified relation's relcache entry
 *		at end of command.
 *
 * This is used in places that need to force relcache rebuild but aren't
 * changing any of the tuples recognized as contributors to the relcache
 * entry by PrepareForTupleInvalidation.  (An example is dropping an index.)
 * We assume in particular that relfilenode/reltablespace aren't changing
 * (so the rd_node value is still good).
 *
 * XXX most callers of this probably don't need to force an smgr flush.
 */
void
CacheInvalidateRelcache(Relation relation)
{
	Oid			databaseId;
	Oid			relationId;

	relationId = RelationGetRelid(relation);
	if (relation->rd_rel->relisshared)
		databaseId = InvalidOid;
	else
		databaseId = MyDatabaseId;

	RegisterRelcacheInvalidation(databaseId, relationId);
	RegisterSmgrInvalidation(relation->rd_node);
}

/*
 * CacheInvalidateRelcacheByTuple
 *		As above, but relation is identified by passing its pg_class tuple.
 */
void
CacheInvalidateRelcacheByTuple(HeapTuple classTuple)
{
	Form_pg_class classtup = (Form_pg_class) GETSTRUCT(classTuple);
	Oid			databaseId;
	Oid			relationId;
	RelFileNode rnode;

	relationId = HeapTupleGetOid(classTuple);
	if (classtup->relisshared)
		databaseId = InvalidOid;
	else
		databaseId = MyDatabaseId;

	if (classtup->reltablespace)
		rnode.spcNode = classtup->reltablespace;
	else if (relstorage_is_ao(classtup->relstorage))
		rnode.spcNode = get_database_dts(databaseId);
	else
		rnode.spcNode = MyDatabaseTableSpace;

	rnode.dbNode = databaseId;
	rnode.relNode = classtup->relfilenode;

	RegisterRelcacheInvalidation(databaseId, relationId);
	RegisterSmgrInvalidation(rnode);
}

/*
 * CacheInvalidateRelcacheByRelid
 *		As above, but relation is identified by passing its OID.
 *		This is the least efficient of the three options; use one of
 *		the above routines if you have a Relation or pg_class tuple.
 */
void
CacheInvalidateRelcacheByRelid(Oid relid)
{
	HeapTuple	tup;

	tup = SearchSysCache(RELOID,
						 ObjectIdGetDatum(relid),
						 0, 0, 0);
	if (!HeapTupleIsValid(tup))
		elog(ERROR, "cache lookup failed for relation %u", relid);
	CacheInvalidateRelcacheByTuple(tup);
	ReleaseSysCache(tup);
}

/*
 * CacheRegisterSyscacheCallback
 *		Register the specified function to be called for all future
 *		invalidation events in the specified cache.
 *
 * NOTE: currently, the OID argument to the callback routine is not
 * provided for syscache callbacks; the routine doesn't really get any
 * useful info as to exactly what changed.	It should treat every call
 * as a "cache flush" request.
 */
void
CacheRegisterSyscacheCallback(int cacheid,
							  CacheCallbackFunction func,
							  Datum arg)
{
	if (cache_callback_count >= MAX_CACHE_CALLBACKS)
		elog(FATAL, "out of cache_callback_list slots");

	cache_callback_list[cache_callback_count].id = cacheid;
	cache_callback_list[cache_callback_count].function = func;
	cache_callback_list[cache_callback_count].arg = arg;

	++cache_callback_count;
}

/*
 * CacheRegisterRelcacheCallback
 *		Register the specified function to be called for all future
 *		relcache invalidation events.  The OID of the relation being
 *		invalidated will be passed to the function.
 *
 * NOTE: InvalidOid will be passed if a cache reset request is received.
 * In this case the called routines should flush all cached state.
 */
void
CacheRegisterRelcacheCallback(CacheCallbackFunction func,
							  Datum arg)
{
	if (cache_callback_count >= MAX_CACHE_CALLBACKS)
		elog(FATAL, "out of cache_callback_list slots");

	cache_callback_list[cache_callback_count].id = SHAREDINVALRELCACHE_ID;
	cache_callback_list[cache_callback_count].function = func;
	cache_callback_list[cache_callback_count].arg = arg;

	++cache_callback_count;
}

/*
 * Returns the Local Metadata Version cache handler corresponding
 * to the current subtransaction nesting level.
 */
mdver_local_mdvsn *
GetCurrentLocalMDVSN(void)
{

	if (NULL != transInvalInfo)
	{
		return transInvalInfo->local_mdvsn;
	}

	return NULL;
}

/*
 * When moving messages from the Current Command Invalidation Queue (CVQ) to the
 *   Prior Command Invalidation Queue (XVQ), pre-process the event queue to
 *   eliminate any events that will have no effect.
 *   If a versioning event is followed by a nuke event, it will have no effect.
 *   Moreover, when a nuke event is moved to XVQ, everything prior in XVQ will
 *   also have no effect.
 *   Therefore we look for Nuke events in CVQ. Find the last one (in the order
 *   of creation), then move all messages from then on to XVQ, overwriting XVQ.
 */
static void
MdVer_PreProcessInvalidMsgs(InvalidationListHeader *dest,
						   InvalidationListHeader *src)
{

	/* Go through src from first until last. Keep track of last nuke found
	 * If nuke found, then:
	 *  -- move msgs from nuke onward from cvq to svq
	 *     -- move msgs to the beginning of first chunk
	 *     -- make the first chunk of cvq to be the firs chunk of svq
	 *  -- at this point, make cvq null, we already moved everything
	 */

	InvalidationChunk *chunk = src->velist;
	if (NULL == chunk)
	{
		/* Source is empty */
		return;
	}

	/* Find chunk and index of the last nuke message */
	InvalidationChunk *last_good_chunk = NULL;
	int last_good_index = -1;
	while (NULL != chunk)
	{
		for (int i = 0; i < chunk->nitems; i++)
		{
			SharedInvalidationMessage *crt_message = &chunk->msgs[i];
			Assert(crt_message->id == SHAREDVERSIONINGMSG_ID);
			mdver_event *crt_event = &(crt_message->ve.verEvent);
			if (mdver_is_nuke_event(crt_event))
			{
				last_good_chunk = chunk;
				last_good_index = i;
			}
		}
		chunk = chunk->next;
	}


	if (NULL != last_good_chunk) {
		/* Found a nuke, and we have the chunk and index saved */
		Assert(last_good_index >= 0);

		/* 1. Free up previous chunks from src, as we'll discard them */
		InvalidationChunk *src_chunk = src->velist;
		while (src_chunk != last_good_chunk) {
			InvalidationChunk *next_chunk = src_chunk->next;
			pfree(src_chunk);
			src_chunk = next_chunk;
		}
		src->velist = last_good_chunk;

		/* 1. Move messages to the beginning of the chunk */
		for (int i = last_good_index; i < last_good_chunk->nitems; i++)
		{
			last_good_chunk->msgs[i - last_good_index] = last_good_chunk->msgs[i];
		}
		/* 2. Update nitems value */
		last_good_chunk->nitems = last_good_chunk->nitems - last_good_index;

		/* Free up all the chunks from dest */
		InvalidationChunk *dest_chunk = dest->velist;
		while (NULL != dest_chunk) {
			InvalidationChunk *next_chunk = dest_chunk->next;
			pfree(dest_chunk);
			dest_chunk = next_chunk;
		}

		/* Move messages from src to overwrite dest */
		dest->velist = last_good_chunk;

		/* Set src to NULL as we're done with the transfer */
		src->velist = NULL;
	}
}


