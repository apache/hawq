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
 * cdbgang.c
 *	  Query Executor Factory for gangs of QEs
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <unistd.h>				/* getpid() */
#include <pthread.h>
#include <limits.h>

#include "gp-libpq-fe.h"
#include "miscadmin.h"			/* MyDatabaseId */
#include "storage/proc.h"		/* MyProc */
#include "storage/ipc.h"
#include "utils/memutils.h"
#include "utils/faultinjector.h"

#include "catalog/namespace.h"
#include "commands/variable.h"
#include "nodes/execnodes.h"	/* CdbProcess, Slice, SliceTable */
#include "postmaster/postmaster.h"
#include "tcop/tcopprot.h"
#include "utils/portal.h"
#include "tcop/pquery.h"

extern int	CommitDelay;
extern int	CommitSiblings;
extern char *default_tablespace;

#include "cdb/cdbconn.h"		/* SegmentDatabaseDescriptor */
#include "cdb/cdbfts.h"
#include "cdb/cdbdisp.h"
#include "cdb/cdbgang.h"		/* me */
#include "cdb/cdbutil.h"		/* CdbComponentDatabaseInfo */
#include "cdb/cdbvars.h"		/* Gp_role, etc. */
#include "storage/bfz.h"
#include "gp-libpq-fe.h"
#include "gp-libpq-int.h"
#include "libpq/libpq-be.h"
#include "libpq/ip.h"

#include "commands/dbcommands.h"
#include "catalog/pg_authid.h"
#include "utils/lsyscache.h"

#define MAX_CACHED_1_GANGS 1
/*
 *	thread_DoConnect is the thread proc used to perform the connection to one of the qExecs.
 */
static void disconnectAndDestroyGang(Gang *gp);
static void disconnectAndDestroyAllReaderGangs(bool destroyAllocated);

static MemoryContext GangContext = NULL;

int
gp_pthread_create(pthread_t * thread,
				  void *(*start_routine) (void *),
				  void *arg, const char *caller)
{
	int			pthread_err = 0;
	pthread_attr_t t_atts;

	/*
	 * Call some init function. Before any thread is created, we need to init
	 * some static stuff. The main purpose is to guarantee the non-thread safe
	 * stuff are called in main thread, before any child thread get running.
	 * Note these staic data structure should be read only after init.	Thread
	 * creation is a barrier, so there is no need to get lock before we use
	 * these data structures.
	 *
	 * So far, we know we need to do this for getpwuid_r (See MPP-1971, glibc
	 * getpwuid_r is not thread safe).
	 */
#ifndef WIN32
	get_gp_passwdptr();
#endif

	/*
	 * save ourselves some memory: the defaults for thread stack size are
	 * large (1M+)
	 */
	pthread_err = pthread_attr_init(&t_atts);
	if (pthread_err != 0)
	{
		elog(LOG, "%s: pthread_attr_init failed.  Error %d", caller, pthread_err);
		return pthread_err;
	}

#ifdef pg_on_solaris
	/* Solaris doesn't have PTHREAD_STACK_MIN ? */
	pthread_err = pthread_attr_setstacksize(&t_atts, (256 * 1024));
#else
	pthread_err = pthread_attr_setstacksize(&t_atts, Max(PTHREAD_STACK_MIN, (256 * 1024)));
#endif
	if (pthread_err != 0)
	{
		elog(LOG, "%s: pthread_attr_setstacksize failed.  Error %d", caller, pthread_err);
		pthread_attr_destroy(&t_atts);
		return pthread_err;
	}

	pthread_err = pthread_create(thread, &t_atts, start_routine, arg);

	pthread_attr_destroy(&t_atts);

	return pthread_err;
}

/*
 * cdbgang_parse_gpqeid_params
 *
 * Called very early in backend initialization, to interpret the "gpqeid"
 * parameter value that a qExec receives from its qDisp.
 *
 * At this point, client authentication has not been done; the backend
 * command line options have not been processed; GUCs have the settings
 * inherited from the postmaster; etc; so don't try to do too much in here.
 */
static bool
gpqeid_next_param(char **cpp, char **npp)
{
	*cpp = *npp;
	if (!*cpp)
		return false;

	*npp = strchr(*npp, ';');
	if (*npp)
	{
		**npp = '\0';
		++*npp;
	}
	return true;
}

void
cdbgang_parse_gpqeid_params(struct Port * port __attribute__((unused)), const char *gpqeid_value)
{
	char	   *gpqeid = pstrdup(gpqeid_value);
	char	   *cp;
	char	   *np = gpqeid;
	char		*master_host = NULL;
	int			master_port = 0;

	/* The presence of an gpqeid string means this backend is a qExec. */
	SetConfigOption("gp_session_role", "execute",
					PGC_POSTMASTER, PGC_S_OVERRIDE);

	/* gp_session_id */
	if (gpqeid_next_param(&cp, &np))
		SetConfigOption("gp_session_id", cp,
						PGC_POSTMASTER, PGC_S_OVERRIDE);

	/* PgStartTime */
	if (gpqeid_next_param(&cp, &np))
	{
#ifdef HAVE_INT64_TIMESTAMP
		if (!scanint8(cp, true, &PgStartTime))
			goto bad;
#else
		PgStartTime = strtod(cp, NULL);
#endif
	}

	if(gpqeid_next_param(&cp, &np))
		master_host = cp;

	if (gpqeid_next_param(&cp, &np))
		master_port = atoi(cp);

	SetMasterAddress(master_host, master_port);

	/* Gp_is_writer */
	if (gpqeid_next_param(&cp, &np))
		SetConfigOption("gp_is_writer", cp,
						PGC_POSTMASTER, PGC_S_OVERRIDE);
		

	/* Too few items, or too many? */
	if (!cp || np)
		goto bad;

	if (gp_session_id <= 0 ||
		PgStartTime <= 0)
		goto bad;

	pfree(gpqeid);
	return;

bad:
	elog(FATAL, "Segment dispatched with invalid option: 'gpqeid=%s'", gpqeid_value);
}	/* cdbgang_parse_gpqeid_params */


/*
 * This is where we keep track of all the gangs that exist for this session.
 * On a QD, gangs can either be "available" (not currently in use), or "allocated".
 *
 * On a Dispatch Agent, we just store them in the "available" lists, as the DA doesn't
 * keep track of allocations (it assumes the QD will keep track of what is allocated or not).
 *
 */

static List *allocatedReaderGangsN = NIL;
static List *availableReaderGangsN = NIL;
static List *allocatedReaderGangs1 = NIL;
static List *availableReaderGangs1 = NIL;
static Gang *primaryWriterGang = NULL;

/*
 * getCdbProcessForQD:	Manufacture a CdbProcess representing the QD,
 * as if it were a worker from the executor factory.
 *
 * NOTE: Does not support multiple (mirrored) QDs.
 */
List *
getCdbProcessesForQD(int isPrimary)
{
	CdbProcess *proc = makeNode(CdbProcess);

	Assert(Gp_role == GP_ROLE_DISPATCH);

	/*
	 * Set QD listener address to NULL. This
	 * will be filled during starting up outgoing
	 * interconnect connection.
	 */
	proc->listenerAddr = NULL;
	if (Gp_interconnect_type == INTERCONNECT_TYPE_UDP)
		proc->listenerPort = (Gp_listener_port >> 16) & 0x0ffff;
	else
		proc->listenerPort = (Gp_listener_port & 0x0ffff);
	proc->pid = MyProcPid;
	proc->contentid = MASTER_CONTENT_ID;

	return list_make1(proc);
}

static bool NeedResetSession = false;
static bool NeedSessionIdChange = false;
static Oid  OldTempNamespace = InvalidOid;

/*
 * cleanupIdleReaderGangs() and cleanupAllIdleGangs().
 *
 * These two routines are used when a session has been idle for a while (waiting for the
 * client to send us SQL to execute).  The idea is to consume less resources while sitting idle.
 *
 * The expectation is that if the session is logged on, but nobody is sending us work to do,
 * we want to free up whatever resources we can.  Usually it means there is a human being at the
 * other end of the connection, and that person has walked away from their terminal, or just hasn't
 * decided what to do next.  We could be idle for a very long time (many hours).
 *
 * Of course, freeing gangs means that the next time the user does send in an SQL statement,
 * we need to allocate gangs (at least the writer gang) to do anything.  This entails extra work,
 * so we don't want to do this if we don't think the session has gone idle.
 *
 * Only call these routines from an idle session.
 *
 * These routines are called from the sigalarm signal handler (hopefully that is safe to do).
 *
 */

/*
 * Destroy all idle (i.e available) reader gangs.
 * It is always safe to get rid of the reader gangs.
 *
 * call only from an idle session.
 */
void
cleanupIdleReaderGangs(void)
{
	if (gp_log_gang >= GPVARS_VERBOSITY_DEBUG)
		elog(DEBUG4, "cleanupIdleReaderGangs beginning");

	disconnectAndDestroyAllReaderGangs(false);

	if (gp_log_gang >= GPVARS_VERBOSITY_DEBUG)
		elog(DEBUG4, "cleanupIdleReaderGangs done");

	return;
}

/*
 * Destroy all gangs to free all resources on the segDBs, if it is possible (safe) to do so.
 *
 * Call only from an idle session.
 */
void
cleanupAllIdleGangs(void)
{
	if (gp_log_gang >= GPVARS_VERBOSITY_DEBUG)
		elog(DEBUG4, "cleanupAllIdleGangs beginning");

	/*
	 * It's always safe to get rid of the reader gangs.
	 *
	 * Since we are idle, any reader gangs will be available but not allocated.
	 */
	disconnectAndDestroyAllReaderGangs(false);

	return;
}

static int64
getMaxGangMop(Gang *g)
{
	int64		maxmop = 0;
	int			i;

	for (i = 0; i < g->size; ++i)
	{
		SegmentDatabaseDescriptor *segdbDesc = &(g->db_descriptors[i]);

		if (!segdbDesc)
			continue;
		if (segdbDesc->conn && PQstatus(segdbDesc->conn) != CONNECTION_BAD)
		{
			if (segdbDesc->conn->mop_high_watermark > maxmop)
				maxmop = segdbDesc->conn->mop_high_watermark;
		}
	}

	return maxmop;
}

static List *
cleanupPortalGangList(List *gplist, int cachelimit)
{
	Gang	   *gp;

	if (gplist != NIL)
	{
		int			ngang = list_length(gplist);
		ListCell   *prev = NULL;
		ListCell   *curr = list_head(gplist);

		while (curr)
		{
			bool		destroy = ngang > cachelimit;

			gp = lfirst(curr);

			if (!destroy)
			{
				int64		maxmop = getMaxGangMop(gp);

				if ((maxmop >> 20) > gp_vmem_protect_gang_cache_limit)
					destroy = true;
			}

			if (destroy)
			{
				disconnectAndDestroyGang(gp);
				gplist = list_delete_cell(gplist, curr, prev);
				if (!prev)
					curr = list_head(gplist);
				else
					curr = lnext(prev);
				--ngang;
			}
			else
			{
				prev = curr;
				curr = lnext(prev);
			}
		}
	}

	return gplist;
}

/*
 * Portal drop... Clean up what gangs we hold
 */
void
cleanupPortalGangs(Portal portal)
{
	MemoryContext oldContext;
	const char *portal_name;

	if (portal->name && strcmp(portal->name, "") != 0)
	{
		portal_name = portal->name;
		elog(DEBUG3, "cleanupPortalGangs %s", portal_name);
	}
	else
	{
		portal_name = NULL;
		elog(DEBUG3, "cleanupPortalGangs (unnamed portal)");
	}


	if (GangContext)
		oldContext = MemoryContextSwitchTo(GangContext);
	else
		oldContext = MemoryContextSwitchTo(TopMemoryContext);

	availableReaderGangsN = cleanupPortalGangList(availableReaderGangsN, gp_cached_gang_threshold);
	availableReaderGangs1 = cleanupPortalGangList(availableReaderGangs1, MAX_CACHED_1_GANGS);

	elog(DEBUG4, "cleanupPortalGangs '%s'. Reader gang inventory: "
		 "allocatedN=%d availableN=%d allocated1=%d available1=%d",
		 (portal_name ? portal_name : "unnamed portal"),
	  list_length(allocatedReaderGangsN), list_length(availableReaderGangsN),
	 list_length(allocatedReaderGangs1), list_length(availableReaderGangs1));

	MemoryContextSwitchTo(oldContext);
}

void
disconnectAndDestroyGang(Gang *gp)
{
	int			i;

	if (gp == NULL)
		return;

	if (gp_log_gang >= GPVARS_VERBOSITY_DEBUG)
		elog(DEBUG5, "disconnectAndDestroyGang entered");

	if (gp->active || gp->allocated)
		elog(DEBUG2, "Warning: disconnectAndDestroyGang called on an %s gang",
			 gp->active ? "active" : "allocated");

	if (gp->gang_id < 1 || gp->gang_id > 100000000 || gp->type < GANGTYPE_UNALLOCATED || gp->type > GANGTYPE_PRIMARY_WRITER || gp->size > 100000)
	{
		elog(LOG, "disconnectAndDestroyGang on bad gang");
		return;
	}

	/*
	 * Loop through the segment_database_descriptors array and, for each
	 * SegmentDatabaseDescriptor:
	 *	   1) discard the query results (if any),
	 *	   2) disconnect the session, and
	 *	   3) discard any connection error message.
	 */
	for (i = 0; i < gp->size; i++)
	{
		SegmentDatabaseDescriptor *segdbDesc = &(gp->db_descriptors[i]);

		if (segdbDesc == NULL)
			continue;

		if (segdbDesc->conn && PQstatus(segdbDesc->conn) != CONNECTION_BAD)
		{

			PGTransactionStatusType status =
			PQtransactionStatus(segdbDesc->conn);

			elog((Debug_print_full_dtm ? LOG : DEBUG5),
				 "disconnectAndDestroyGang: got QEDistributedTransactionId = %u, QECommandId = %u, and QEDirty = %s",
				 segdbDesc->conn->QEWriter_DistributedTransactionId,
				 segdbDesc->conn->QEWriter_CommandId,
				 (segdbDesc->conn->QEWriter_Dirty ? "true" : "false"));

			if (gp_log_gang >= GPVARS_VERBOSITY_TERSE)
			{
				const char *ts;

				switch (status)
				{
					case PQTRANS_IDLE:
						ts = "idle";
						break;
					case PQTRANS_ACTIVE:
						ts = "active";
						break;
					case PQTRANS_INTRANS:
						ts = "idle, within transaction";
						break;
					case PQTRANS_INERROR:
						ts = "idle, within failed transaction";
						break;
					case PQTRANS_UNKNOWN:
						ts = "unknown transaction status";
						break;
					default:
						ts = "invalid transaction status";
						break;
				}
				elog(DEBUG3, "Finishing connection with %s; %s",
					 segdbDesc->whoami, ts);
			}

			if (status == PQTRANS_ACTIVE)
			{
				char		errbuf[256];
				PGcancel   *cn = PQgetCancel(segdbDesc->conn);

				if (Debug_cancel_print)
					elog(LOG, "Calling PQcancel for %s", segdbDesc->whoami);

				if (PQcancel(cn, errbuf, 256) == 0)
				{
					elog(LOG, "Unable to cancel %s: %s", segdbDesc->whoami, errbuf);
				}
				PQfreeCancel(cn);
			}

			PQfinish(segdbDesc->conn);

			segdbDesc->conn = NULL;
		}

		/* Free memory owned by the segdbDesc. */
		cdbconn_termSegmentDescriptor(segdbDesc);
	}

	/*
	 * when we get rid of the primary writer gang we MUST also get rid of the reader
	 * gangs due to the shared local snapshot code that is shared between
	 * readers and writers.
	 */
	if (gp->type == GANGTYPE_PRIMARY_WRITER)
	{
		disconnectAndDestroyAllReaderGangs(true);
	}

	/*
	 * Discard the segment array and the cluster descriptor
	 */
	pfree(gp->db_descriptors);
	gp->db_descriptors = NULL;
	gp->size = 0;
	if (gp->portal_name != NULL)
		pfree(gp->portal_name);
	pfree(gp);

	/*
	 * this is confusing, gp is local variable, no need to null it. gp = NULL;
	 */
	if (gp_log_gang >= GPVARS_VERBOSITY_DEBUG)
		elog(DEBUG5, "disconnectAndDestroyGang done");
}

/*
 * disconnectAndDestroyAllReaderGangs
 *
 * Here we destroy all reader gangs regardless of the portal they belong to.
 * TODO: This may need to be done more carefully when multiple cursors are
 * enabled.
 * If the parameter destroyAllocated is true, then destroy allocated as well as
 * available gangs.
 */
static void
disconnectAndDestroyAllReaderGangs(bool destroyAllocated)
{
	Gang	   *gp;

	if (allocatedReaderGangsN != NIL && destroyAllocated)
	{
		gp = linitial(allocatedReaderGangsN);
		while (gp != NULL)
		{
			disconnectAndDestroyGang(gp);
			allocatedReaderGangsN = list_delete_first(allocatedReaderGangsN);
			if (allocatedReaderGangsN != NIL)
				gp = linitial(allocatedReaderGangsN);
			else
				gp = NULL;
		}
	}
	if (availableReaderGangsN != NIL)
	{
		gp = linitial(availableReaderGangsN);
		while (gp != NULL)
		{
			disconnectAndDestroyGang(gp);
			availableReaderGangsN = list_delete_first(availableReaderGangsN);
			if (availableReaderGangsN != NIL)
				gp = linitial(availableReaderGangsN);
			else
				gp = NULL;
		}
	}
	if (allocatedReaderGangs1 != NIL && destroyAllocated)
	{
		gp = linitial(allocatedReaderGangs1);
		while (gp != NULL)
		{
			disconnectAndDestroyGang(gp);
			allocatedReaderGangs1 = list_delete_first(allocatedReaderGangs1);
			if (allocatedReaderGangs1 != NIL)
				gp = linitial(allocatedReaderGangs1);
			else
				gp = NULL;
		}
	}
	if (availableReaderGangs1 != NIL)
	{
		gp = linitial(availableReaderGangs1);
		while (gp != NULL)
		{
			disconnectAndDestroyGang(gp);
			availableReaderGangs1 = list_delete_first(availableReaderGangs1);
			if (availableReaderGangs1 != NIL)
				gp = linitial(availableReaderGangs1);
			else
				gp = NULL;
		}
	}
	if (destroyAllocated)
		allocatedReaderGangsN = NIL;
	availableReaderGangsN = NIL;

	if (destroyAllocated)
		allocatedReaderGangs1 = NIL;
	availableReaderGangs1 = NIL;
}

/*
 * Drop any temporary tables associated with the current session and
 * use a new session id since we have effectively reset the session.
 *
 * Call this procedure outside of a transaction.
 */
void
CheckForResetSession(void)
{
  int     oldSessionId = 0;
  int     newSessionId = 0;
  Oid     dropTempNamespaceOid;

  if (!NeedResetSession)
    return;

  /*
   * Do the session id change early.
   */
  if (NeedSessionIdChange)
  {
    /* If we have gangs, we can't change our session ID. */
    Assert(!gangsExist());

    oldSessionId = gp_session_id;
    ProcNewMppSessionId(&newSessionId);

    gp_session_id = newSessionId;
    gp_command_count = 0;

    elog(LOG, "The previous session was reset because its gang was disconnected (session id = %d). "
       "The new session id = %d",
       oldSessionId, newSessionId);

    NeedSessionIdChange = false;
  }

  if (IsTransactionOrTransactionBlock())
  {
    NeedResetSession = false;
    return;
  }

  dropTempNamespaceOid = OldTempNamespace;
  OldTempNamespace = InvalidOid;
  NeedResetSession = false;

  if (dropTempNamespaceOid != InvalidOid)
  {
    PG_TRY();
    {
      DropTempTableNamespaceForResetSession(dropTempNamespaceOid);
    }
    PG_CATCH();
    {
      /*
       * But first demote the error to something much less
       * scary.
       */
      if (!elog_demote(WARNING))
      {
        elog(LOG, "unable to demote error");
        PG_RE_THROW();
      }

      EmitErrorReport();
      FlushErrorState();
    }
    PG_END_TRY();
  }

}

void
disconnectAndDestroyAllGangs(void)
{
	if (Gp_role == GP_ROLE_UTILITY)
		return;

	/* for now, destroy all readers, regardless of the portal that owns them */
	disconnectAndDestroyAllReaderGangs(true);

	disconnectAndDestroyGang(primaryWriterGang);
	primaryWriterGang = NULL;
}

/*
 * Set segdb states, called by FtsReConfigureMPP.
 */
void
detectFailedConnections(void)
{
	int			i;
//	CdbComponentDatabaseInfo *segInfo;
	bool		fullScan = true;

	/*
	 * check primary gang
	 */
	if (primaryWriterGang != NULL)
	{
		for (i = 0; i < primaryWriterGang->size; i++)
		{
			//segInfo = primaryWriterGang->db_descriptors[i].segment_database_info;

			/*
			 * Note: the probe process is responsible for doing the
			 * actual marking of the segments.
			 */
			//FtsTestConnection(segInfo, fullScan);
			fullScan = false;
		}
	}
}

bool
gangsExist(void)
{
	return (primaryWriterGang != NULL ||
			allocatedReaderGangsN != NIL ||
			availableReaderGangsN != NIL ||
			allocatedReaderGangs1 != NIL ||
			availableReaderGangs1 != NIL);
}

#ifdef USE_ASSERT_CHECKING
/**
 * Assert that slicetable is valid. Must be called after ExecInitMotion, which sets up the slice table
 */
void
AssertSliceTableIsValid(SliceTable *st, struct PlannedStmt *pstmt)
{
	if (!st)
	{
		return;
	}

	Assert(st);
	Assert(pstmt);

	Assert(pstmt->nMotionNodes == st->nMotions);
	Assert(pstmt->nInitPlans == st->nInitPlans);

	ListCell *lc = NULL;
	int i = 0;

	int maxIndex = st->nMotions + st->nInitPlans + 1;

	Assert(maxIndex == list_length(st->slices));

	foreach (lc, st->slices)
	{
		Slice *s = (Slice *) lfirst(lc);

		/* The n-th slice entry has sliceIndex of n */
		Assert(s->sliceIndex == i && "slice index incorrect");

		/* The root index of a slice is either 0 or is a slice corresponding to an init plan */
		Assert((s->rootIndex == 0)
				|| (s->rootIndex > st->nMotions && s->rootIndex < maxIndex));

		/* Parent slice index */
		if (s->sliceIndex == s->rootIndex )
		{
			/* Current slice is a root slice. It will have parent index -1.*/
			Assert(s->parentIndex == -1 && "expecting parent index of -1");
		}
		else
		{
			/* All other slices must have a valid parent index */
			Assert(s->parentIndex >= 0 && s->parentIndex < maxIndex && "slice's parent index out of range");
		}

		/* Current slice's children must consider it the parent */
		ListCell *lc1 = NULL;
		foreach (lc1, s->children)
		{
			int childIndex = lfirst_int(lc1);
			Assert(childIndex >= 0 && childIndex < maxIndex && "invalid child slice");
			Slice *sc = (Slice *) list_nth(st->slices, childIndex);
			Assert(sc->parentIndex == s->sliceIndex && "slice's child does not consider it the parent");
		}

		/* Current slice must be in its parent's children list */
		if (s->parentIndex >= 0)
		{
			Slice *sp = (Slice *) list_nth(st->slices, s->parentIndex);

			bool found = false;
			foreach (lc1, sp->children)
			{
				int childIndex = lfirst_int(lc1);
				Assert(childIndex >= 0 && childIndex < maxIndex && "invalid child slice");
				Slice *sc = (Slice *) list_nth(st->slices, childIndex);

				if (sc->sliceIndex == s->sliceIndex)
				{
					found = true;
					break;
				}
			}

			Assert(found && "slice's parent does not consider it a child");
		}



		i++;
	}
}

#endif
