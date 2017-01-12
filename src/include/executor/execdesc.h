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
 * execdesc.h
 *	  plan and query descriptor accessor macros used by the executor
 *	  and related modules.
 *
 *
 * Portions Copyright (c) 2005-2009, Greenplum inc
 * Portions Copyright (c) 1996-2009, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * $PostgreSQL: pgsql/src/include/executor/execdesc.h,v 1.32 2006/07/11 16:35:33 momjian Exp $
 *
 *-------------------------------------------------------------------------
 */
#ifndef EXECDESC_H
#define EXECDESC_H

#include "nodes/parsenodes.h"
#include "nodes/plannodes.h"
#include "tcop/dest.h"
#include "gpmon/gpmon.h"

struct CdbExplain_ShowStatCtx;  /* private, in "cdb/cdbexplain.c" */
struct EState;                  /* #include "nodes/execnodes.h" */
struct PlanState;               /* #include "nodes/execnodes.h" */

#define HOSTNAME_MAX_LENGTH (256)

typedef enum QueryResourceLife {
	QRL_NONE,			/* Don't allocate resource. */
	QRL_ONCE,			/* Auto free during destroy QueryDesc. */
	QRL_INHERIT,		/* Use the parent resources, in SPI/function cases. */
} QueryResourceLife;

/* 
 *
 */
typedef struct QueryResource {
	NodeTag				type;
	QueryResourceLife	life;
	int					resource_id;
	struct Segment		*master;
	List				*segments;
	uint32_t			segment_memory_mb;
	double				segment_vcore;
	int					numSegments;
	int					*segment_vcore_agg;
	int         *segment_vcore_writer;
	TimestampTz			master_start_time;
} QueryResource;

/*
 *
 */
typedef struct HostnameVolumeInfo
{
	char hostname[HOSTNAME_MAX_LENGTH];
	int64 datavolume;
} HostnameVolumeInfo;

/* ----------------
 *		query descriptor:
 *
 *	a QueryDesc encapsulates everything that the executor
 *	needs to execute the query.
 *
 *	For the convenience of SQL-language functions, we also support QueryDescs
 *	containing utility statements; these must not be passed to the executor
 *	however.
 * ---------------------
 */
typedef struct QueryDesc
{
	/* These fields are provided by CreateQueryDesc */
	CmdType		operation;		/* CMD_SELECT, CMD_UPDATE, etc. */
	PlannedStmt *plannedstmt;	/* planner's output, or null if utility */
	Node	   *utilitystmt;	/* utility statement, or null */
	const char *sourceText;		/* source text of the query */
	Snapshot	snapshot;		/* snapshot to use for query */
	Snapshot	crosscheck_snapshot;	/* crosscheck for RI update/delete */
	DestReceiver *dest;			/* the destination for tuple output */
	ParamListInfo params;		/* param values being passed in */
	bool		doInstrument;	/* TRUE requests runtime instrumentation */
	
	/* These fields are set by ExecutorStart */
	TupleDesc	tupDesc;		/* descriptor for result tuples */
	struct EState      *estate;			/* executor's query-wide state */
	struct PlanState   *planstate;		/* tree of per-plan-node state */
	
	/* This field is set by ExecutorEnd after collecting cdbdisp results */
	uint64		es_processed;	/* # of tuples processed */
	Oid			es_lastoid;		/* oid of row inserted */
	bool		extended_query;   /* simple or extended query protocol? */
	char		*portal_name;	/* NULL for unnamed portal */
	
	/* CDB: EXPLAIN ANALYZE statistics */
	struct CdbExplain_ShowStatCtx  *showstatctx;
	
	/* Gpmon */
	gpmon_packet_t *gpmon_pkt;
	
	/* This is always set NULL by the core system, but plugins can change it */
	struct Instrumentation *totaltime;	/* total time spent in ExecutorRun */

	struct QueryResource	*resource;
	struct QueryResource *savedResource;
	int						planner_segments;
} QueryDesc;

/* in pquery.c */
extern QueryDesc *CreateQueryDesc(PlannedStmt *plannedstmt,
								  const char *sourceText,
				Snapshot snapshot,
				Snapshot crosscheck_snapshot,
				DestReceiver *dest,
				ParamListInfo params,
				bool doInstrument);

extern QueryDesc *CreateUtilityQueryDesc(Node *utilitystmt,
										 const char *sourceText,
										 Snapshot snapshot,
										 DestReceiver *dest,
										 ParamListInfo params);

extern void FreeQueryDesc(QueryDesc *qdesc);

extern struct QueryResource *AllocateResource(QueryResourceLife   life,
											  int32 			  slice_size,
											  int64_t			  iobytes,
											  int   			  max_target_segment_num,
											  int   			  min_target_segment_num,
											  HostnameVolumeInfo *vol_info,
											  int 				  vol_info_size);

extern void FreeResource(struct QueryResource *resource);
extern void AutoFreeResource(struct QueryResource *resource);
extern void SetActiveQueryResource(struct QueryResource *resource);
extern QueryResource *GetActiveQueryResource(void);
extern void UnsetActiveQueryResource(void);
extern void CleanupActiveQueryResource(void);
extern void CleanupGlobalQueryResources(void);
extern struct QueryResource *GetDispatcherQueryResoruce(QueryDesc *queryDesc);

extern void SetActiveRelType(List *relsType);
extern List* GetActiveRelType(void);
extern void UnsetActiveRelType(void);

extern void GetResourceQuota(int		max_target_segment_num,
					  	  	 int		min_target_segment_num,
							 uint32   *seg_num,
							 uint32   *seg_num_min,
							 uint32   *seg_memory_mb,
							 double   *seg_core);

#endif   /* EXECDESC_H  */
