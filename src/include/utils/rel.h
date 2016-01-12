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
 * rel.h
 *	  POSTGRES relation descriptor (a/k/a relcache entry) definitions.
 *
 *
 * Portions Copyright (c) 2005-2009, Greenplum inc.
 * Portions Copyright (c) 1996-2009, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * $PostgreSQL: pgsql/src/include/utils/rel.h,v 1.92 2006/10/04 00:30:10 momjian Exp $
 *
 *-------------------------------------------------------------------------
 */
#ifndef REL_H
#define REL_H

#include "access/tupdesc.h"
#include "catalog/pg_am.h"
#include "catalog/pg_class.h"
#include "catalog/pg_index.h"
#include "fmgr.h"
#include "nodes/bitmapset.h"
#include "rewrite/prs2lock.h"
#include "storage/block.h"
#include "storage/relfilenode.h"
#include "utils/relcache.h"
#include "catalog/gp_persistent.h"


/*
 * LockRelId and LockInfo really belong to lmgr.h, but it's more convenient
 * to declare them here so we can have a LockInfoData field in a Relation.
 */

typedef struct LockRelId
{
	Oid			relId;			/* a relation identifier */
	Oid			dbId;			/* a database identifier */
} LockRelId;

typedef struct LockInfoData
{
	LockRelId	lockRelId;
} LockInfoData;

typedef LockInfoData *LockInfo;

/*
 * Likewise, this struct really belongs to trigger.h, but for convenience
 * we put it here.
 */
typedef struct Trigger
{
	Oid			tgoid;			/* OID of trigger (pg_trigger row) */
	/* Remaining fields are copied from pg_trigger, see pg_trigger.h */
	char	   *tgname;
	Oid			tgfoid;
	int16		tgtype;
	bool		tgenabled;
	bool		tgisconstraint;
	Oid			tgconstrrelid;
	bool		tgdeferrable;
	bool		tginitdeferred;
	int16		tgnargs;
	int16		tgnattr;
	int16	   *tgattr;
	char	  **tgargs;
} Trigger;

typedef struct TriggerDesc
{
	/*
	 * Index data to identify which triggers are which.  Since each trigger
	 * can appear in more than one class, for each class we provide a list of
	 * integer indexes into the triggers array.  The class codes are defined
	 * by TRIGGER_EVENT_xxx macros in commands/trigger.h.
	 */
#define TRIGGER_NUM_EVENT_CLASSES  4

	uint16		n_before_statement[TRIGGER_NUM_EVENT_CLASSES];
	uint16		n_before_row[TRIGGER_NUM_EVENT_CLASSES];
	uint16		n_after_row[TRIGGER_NUM_EVENT_CLASSES];
	uint16		n_after_statement[TRIGGER_NUM_EVENT_CLASSES];
	int		   *tg_before_statement[TRIGGER_NUM_EVENT_CLASSES];
	int		   *tg_before_row[TRIGGER_NUM_EVENT_CLASSES];
	int		   *tg_after_row[TRIGGER_NUM_EVENT_CLASSES];
	int		   *tg_after_statement[TRIGGER_NUM_EVENT_CLASSES];

	/* The actual array of triggers is here */
	Trigger    *triggers;
	int			numtriggers;
} TriggerDesc;


/*
 * Cached lookup information for the index access method functions defined
 * by the pg_am row associated with an index relation.
 */
typedef struct RelationAmInfo
{
	FmgrInfo	aminsert;
	FmgrInfo	ambeginscan;
	FmgrInfo	amgettuple;
	FmgrInfo	amgetmulti;
	FmgrInfo	amrescan;
	FmgrInfo	amendscan;
	FmgrInfo	ammarkpos;
	FmgrInfo	amrestrpos;
	FmgrInfo	ambuild;
	FmgrInfo	ambulkdelete;
	FmgrInfo	amvacuumcleanup;
	FmgrInfo	amcostestimate;
	FmgrInfo	amoptions;
} RelationAmInfo;

typedef struct RelationNodeInfo
{
	bool	isPresent;

	bool	tidAllowedToBeZero;
				// Persistent TID allowed to be zero for "Before Persistence" or Recovery.

	ItemPointerData		persistentTid;
	int64				persistentSerialNum;
} RelationNodeInfo;

/*
 * Here are the contents of a relation cache entry.
 */

typedef struct RelationData
{
	RelFileNode rd_node;		/* relation physical identifier */
	/* use "struct" here to avoid needing to include smgr.h: */
	struct SMgrRelationData *rd_smgr;	/* cached file handle, or NULL */
	BlockNumber rd_targblock;	/* current insertion target block, or
								 * InvalidBlockNumber */
	int			rd_refcnt;		/* reference count */
	bool		rd_istemp;		/* CDB: true => skip locking, logging, fsync */
	bool		rd_issyscat;	/* GP: true => system catalog table (has "pg_" prefix) */
	bool		rd_isnailed;	/* rel is nailed in cache */
	bool		rd_isvalid;		/* relcache entry is valid */
	char		rd_indexvalid;	/* state of rd_indexlist: 0 = not valid, 1 =
								 * valid, 2 = temporarily forced */
	SubTransactionId rd_createSubid;	/* rel was created in current xact */

	/*
	 * Debugging information, Values from CREATE TABLE, if present.
	 */
	bool				rd_haveCreateDebugInfo;
	bool				rd_createDebugIsZeroTid;
	ItemPointerData		rd_createDebugPersistentTid;
	int64				rd_createDebugPersistentSerialNum;

	/*
	 * rd_createSubid is the ID of the highest subtransaction the rel has
	 * survived into; or zero if the rel was not created in the current top
	 * transaction.  This should be relied on only for optimization purposes;
	 * it is possible for new-ness to be "forgotten" (eg, after CLUSTER).
	 */
	Form_pg_class rd_rel;		/* RELATION tuple */
	TupleDesc	rd_att;			/* tuple descriptor */
	Oid			rd_id;			/* relation's object id */
	List	   *rd_indexlist;	/* list of OIDs of indexes on relation */
	Oid			rd_oidindex;	/* OID of unique index on OID, if any */
	LockInfoData rd_lockInfo;	/* lock mgr's info for locking relation */
	RuleLock   *rd_rules;		/* rewrite rules */
	MemoryContext rd_rulescxt;	/* private memory cxt for rd_rules, if any */
	TriggerDesc *trigdesc;		/* Trigger info, or NULL if rel has none */
    struct GpPolicy *rd_cdbpolicy; /* Partitioning info if distributed rel */
    bool        rd_cdbDefaultStatsWarningIssued;
	bool		rd_isLocalBuf;  /* CDB: true => rel uses the local buffer mgr */

	/*
	 * rd_options is set whenever rd_rel is loaded into the relcache entry.
	 * Note that you can NOT look into rd_rel for this data.  NULL means "use
	 * defaults".
	 */
	bytea	   *rd_options;		/* parsed pg_class.reloptions */

	/* These are non-NULL only for an index relation: */
	Form_pg_index rd_index;		/* pg_index tuple describing this index */
	struct HeapTupleData *rd_indextuple;		/* all of pg_index tuple */
	/* "struct HeapTupleData *" avoids need to include htup.h here	*/
	oidvector  *rd_indclass;	/* extracted pointer to rd_index field */
	Form_pg_am	rd_am;			/* pg_am tuple for index's AM */

	/*
	 * index access support info (used only for an index relation)
	 *
	 * Note: only default operators and support procs for each opclass are
	 * cached, namely those with subtype zero.	The arrays are indexed by
	 * strategy or support number, which is a sufficient identifier given that
	 * restriction.
	 *
	 * Note: rd_amcache is available for index AMs to cache private data about
	 * an index.  This must be just a cache since it may get reset at any time
	 * (in particular, it will get reset by a relcache inval message for the
	 * index).	If used, it must point to a single memory chunk palloc'd in
	 * rd_indexcxt.  A relcache reset will include freeing that chunk and
	 * setting rd_amcache = NULL.
	 */
	MemoryContext rd_indexcxt;	/* private memory cxt for this stuff */
	RelationAmInfo *rd_aminfo;	/* lookup info for funcs found in pg_am */
	Oid		   *rd_operator;	/* OIDs of index operators */
	RegProcedure *rd_support;	/* OIDs of support procedures */
	FmgrInfo   *rd_supportinfo; /* lookup info for support procedures */
	List	   *rd_indexprs;	/* index expression trees, if any */
	List	   *rd_indpred;		/* index predicate tree, if any */
	void	   *rd_amcache;		/* available for use by index AM */

	/*
	 * Physical file-system information.
	 */
	struct RelationNodeInfo rd_relationnodeinfo;
								/* Values from gp_persistent_relation_node, if present */

	/* use "struct" here to avoid needing to include pgstat.h: */
	struct PgStat_TableStatus *pgstat_info;		/* statistics collection area */

	/*
	 * in gpsql, master create all segfiles for segments,
	 * therefore we need an array.
	 * use this varaiable instead of rd_segfile0_relationnodeinfo,
	 * rd_segfile0_relationnodeinfo should be removed finally.
	 */
	/*
	 * in HAWQ 2.0, the number of segments is not fixed.
	 */
	/*
	struct RelationNodeInfo * rd_segfile0_relationnodeinfos;
	int32 rd_segfile0_count;
	*/

} RelationData;

/*
 * StdRdOptions
 *		Standard contents of rd_options for heaps and generic indexes.
 *
 * RelationGetFillFactor() and RelationGetTargetPageFreeSpace() can only
 * be applied to relations that use this format or a superset for
 * private options data.
 */
 /* autovacuum-related reloptions. */
typedef struct AutoVacOpts
{
	bool		enabled;
	int			vacuum_threshold;
	int			analyze_threshold;
	int			vacuum_cost_delay;
	int			vacuum_cost_limit;
	int			freeze_min_age;
	int			freeze_max_age;
	int			freeze_table_age;
	float8		vacuum_scale_factor;
	float8		analyze_scale_factor;
} AutoVacOpts;

typedef struct StdRdOptions
{
	int32		vl_len_;		/* varlena header (do not touch directly!) */
	int			fillfactor;		/* page fill factor in percent (0..100) */
	AutoVacOpts autovacuum;		/* autovacuum-related options */
	bool		appendonly;		/* is this an appendonly relation? */
	int			blocksize;		/* max varblock size (AO rels only) */
	int			pagesize;		/* page size(Parquet rels only) */
	int			rowgroupsize;	/* row group size (Parquet rels only)*/
	int			compresslevel;  /* compression level (AO rels only) */
	char*		compresstype;   /* compression type (AO rels only) */
	bool		checksum;		/* checksum (AO rels only) */
	char 		columnstore;	/* columnstore (AO only, 'a' for ao, 'c' for co, 'p' for parquet) */
	bool		forceHeap;		/* specified appendonly=false */
	bool		errorTable;		/* skip GOH tablespace checking. */
	int 		bucket_num;		/* default init segment num for random/hash/external table */
} StdRdOptions;

#define HEAP_MIN_FILLFACTOR			10
#define HEAP_DEFAULT_FILLFACTOR		100

typedef struct TidycatOptions
{
	/*
	 *  Options only allowed during upgrade (tidycat option)
	 *  Not all tidycat option are of interest to us:
	 *    "shared" is not needed because it's derived from tablespace
	 */
	Oid         relid;          /* relid of the table in pg_class */
	Oid         reltype_oid;    /* static reltype in pg_type */
	Oid         toast_oid;      /* oid of the toast table */
	Oid         toast_index;    /* oid of the toast index */
	Oid         toast_reltype;  /* static reltype of the toast table in pg_type */
	Oid         indexid;
} TidycatOptions;

/*
 * RelationGetFillFactor
 *		Returns the relation's fillfactor.  Note multiple eval of argument!
 */
#define RelationGetFillFactor(relation, defaultff) \
	((relation)->rd_options ? \
	 ((StdRdOptions *) (relation)->rd_options)->fillfactor : (defaultff))

/*
 * RelationGetTargetPageUsage
 *		Returns the relation's desired space usage per page in bytes.
 */
#define RelationGetTargetPageUsage(relation, defaultff) \
	(BLCKSZ * RelationGetFillFactor(relation, defaultff) / 100)

/*
 * RelationGetTargetPageFreeSpace
 *		Returns the relation's desired freespace per page in bytes.
 */
#define RelationGetTargetPageFreeSpace(relation, defaultff) \
	(BLCKSZ * (100 - RelationGetFillFactor(relation, defaultff)) / 100)

/*
 * RelationIsValid
 *		True iff relation descriptor is valid.
 */
#define RelationIsValid(relation) PointerIsValid(relation)

#define InvalidRelation ((Relation) NULL)

/*
 * RelationIsHeap
 * 		True iff relation has heap storage
 */
#define RelationIsHeap(relation) \
	((bool)((relation)->rd_rel->relstorage == RELSTORAGE_HEAP))

/*
 * RelationIsExternal
 * 		True iff relation has external storage
 */
#define RelationIsExternal(relation) \
	((bool)((relation)->rd_rel->relstorage == RELSTORAGE_EXTERNAL))

/*
 * RelationIsAoRows
 * 		True iff relation has append only storage with row orientation
 */
#define RelationIsAoRows(relation) \
	((bool)(((relation)->rd_rel->relstorage == RELSTORAGE_AOROWS)))

/*
 * RelationIsParquet
 * 		True iff relation has append only storage with parquet orientation
 */
#define RelationIsParquet(relation) \
	((bool)(((relation)->rd_rel->relstorage == RELSTORAGE_PARQUET)))


/*
 * RelationIsForeign
 * 		True iff relation has foreign storage
 */
#define RelationIsForeign(relation) \
	((bool)((relation)->rd_rel->relstorage == RELSTORAGE_FOREIGN))

/*
 * RelationIsBitmapIndex
 *      True iff relation is a bitmap index
 */
#define RelationIsBitmapIndex(relation) \
	((bool)((relation)->rd_rel->relam == BITMAP_AM_OID))

/*
 * RelationHasReferenceCountZero
 *		True iff relation reference count is zero.
 *
 * Note:
 *		Assumes relation descriptor is valid.
 */
#define RelationHasReferenceCountZero(relation) \
		((bool)((relation)->rd_refcnt == 0))

/*
 * RelationGetForm
 *		Returns pg_class tuple for a relation.
 *
 * Note:
 *		Assumes relation descriptor is valid.
 */
#define RelationGetForm(relation) ((relation)->rd_rel)

/*
 * RelationGetRelid
 *		Returns the OID of the relation
 */
#define RelationGetRelid(relation) ((relation)->rd_id)

/*
 * RelationGetNumberOfAttributes
 *		Returns the number of attributes in a relation.
 */
#define RelationGetNumberOfAttributes(relation) ((relation)->rd_rel->relnatts)

/*
 * RelationGetDescr
 *		Returns tuple descriptor for a relation.
 */
#define RelationGetDescr(relation) ((relation)->rd_att)

/*
 * RelationGetRelationName
 *		Returns the rel's name.
 *
 * Note that the name is only unique within the containing namespace.
 */
#define RelationGetRelationName(relation) \
	(NameStr((relation)->rd_rel->relname))

/*
 * RelationGetNamespace
 *		Returns the rel's namespace OID.
 */
#define RelationGetNamespace(relation) \
	((relation)->rd_rel->relnamespace)

/*
 * RelationOpenSmgr
 *		Open the relation at the smgr level, if not already done.
 */
#define RelationOpenSmgr(relation) \
	do { \
		if ((relation)->rd_smgr == NULL) \
			smgrsetowner(&((relation)->rd_smgr), smgropen((relation)->rd_node)); \
	} while (0)

/*
 * RelationCloseSmgr
 *		Close the relation at the smgr level, if not already done.
 *
 * Note: smgrclose should unhook from owner pointer, hence the Assert.
 */
#define RelationCloseSmgr(relation) \
	do { \
		if ((relation)->rd_smgr != NULL) \
		{ \
			smgrclose((relation)->rd_smgr); \
			Assert((relation)->rd_smgr == NULL); \
		} \
	} while (0)

/*
 * RELATION_IS_LOCAL
 *		If a rel is either temp or newly created in the current transaction,
 *		it can be assumed to be visible only to the current backend.
 *
 * Beware of multiple eval of argument
 */
#define RELATION_IS_LOCAL(relation) \
	((relation)->rd_istemp || \
	 (relation)->rd_createSubid != InvalidSubTransactionId)

/* routines in utils/cache/relcache.c */
extern void RelationIncrementReferenceCount(Relation rel);
extern void RelationDecrementReferenceCount(Relation rel);

#endif   /* REL_H */
