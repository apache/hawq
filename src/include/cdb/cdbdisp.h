/*-------------------------------------------------------------------------
 *
 * cdbdisp.h
 * routines for dispatching commands from the dispatcher process
 * to the qExec processes.
 *
 * Copyright (c) 2005-2008, Greenplum inc
 *
 *-------------------------------------------------------------------------
 */
#ifndef CDBDISP_H
#define CDBDISP_H

#include "lib/stringinfo.h"         /* StringInfo */

#include "cdb/cdbselect.h"
#include <pthread.h>

#define CDB_MOTION_LOST_CONTACT_STRING "Interconnect error master lost contact with segment."

struct CdbDispatchResults;          /* #include "cdb/cdbdispatchresult.h" */
struct pg_result;                   /* #include "gp-libpq-fe.h" */
struct Node;                        /* #include "nodes/nodes.h" */

/*
 * Parameter structure for Greenplum Database Queries
 */
typedef struct DispatchCommandQueryParms
{
	/*
	 * The SQL command
	 */
	const char	*strCommand;
	int			strCommandlen;
	char		*serializedQuerytree;
	int			serializedQuerytreelen;
	char		*serializedPlantree;
	int			serializedPlantreelen;
	char		*serializedParams;
	int			serializedParamslen;
	char		*serializedSliceInfo;
	int			serializedSliceInfolen;
	char		*serializedQueryResource;
	int			serializedQueryResourcelen;
	
	/*
	 * serialized DTX context string
	 */
	char		*serializedDtxContextInfo;
	int			serializedDtxContextInfolen;
	
	int			rootIdx;

	/*
	 * the sequence server info.
	 */
	char *seqServerHost;		/* If non-null, sequence server host name. */
	int seqServerHostlen;
	int seqServerPort;			/* If seqServerHost non-null, sequence server port. */
	
	/*
	 * Used by dispatch agent if NOT using sliced execution
	 */
	int			primary_gang_id;

} DispatchCommandQueryParms;

struct pg_result **
cdbdisp_returnResults(int segmentNum,
						struct CdbDispatchResults *primaryResults,
						StringInfo errmsgbuf,
						int *numresults);

/* this is used for dire cleanup emergencies (in portalmem.c) */
void
CdbShutdownPortals(void);

struct PlannedStmt;
struct PlannerInfo;

/* used in the interconnect on the dispatcher to avoid error-cleanup deadlocks. */

/* 
 * make a plan constant, if possible. Call must say if we're doing single row
 * inserts.
 */
extern Node *exec_make_plan_constant(struct PlannedStmt *stmt, bool is_SRI);
extern Node *planner_make_plan_constant(struct PlannerInfo *root, Node *n, bool is_SRI);

/*--------------------------------------------------------------------*/

#endif   /* CDBDISP_H */
