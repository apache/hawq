/*-------------------------------------------------------------------------
 *
 * analyze.c
 *	  the Postgres statistics generator
 *
 * Portions Copyright (c) 2005-2010, Greenplum inc
 * Portions Copyright (c) 1996-2009, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * The new ANALYZE implementation uses routine query processing
 * capabilities of GPDB to compute statistics on relations.  It
 * differs from old ANALYZE functionally in that it computes global
 * statistics which improves cardinalities of join/group-by
 * operations.
 * It uses SPI under the covers to issue queries. The main point of
 * entry is the function analyzeStatement. This function does the
 * heavy lifting of setting up transactions and getting locks.
 *
 * IDENTIFICATION
 *	  $PostgreSQL: pgsql/src/backend/commands/analyze.c,v 1.101 2006/11/05 22:42:08 tgl Exp $
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <math.h>

#include "access/heapam.h"
#include "catalog/heap.h"
#include "access/transam.h"
#include "access/tuptoaster.h"
#include "access/aosegfiles.h"
#include "access/aocssegfiles.h"
#include "catalog/index.h"
#include "catalog/indexing.h"
#include "catalog/namespace.h"
#include "catalog/catalog.h"
#include "cdb/cdbappendonlyam.h"
#include "cdb/cdbaocsam.h"
#include "cdb/cdbpartition.h"
#include "commands/vacuum.h"
#include "executor/executor.h"
#include "lib/stringinfo.h"
#include "libpq/pqformat.h"             /* pq_beginmessage() etc. */
#include "miscadmin.h"
#include "parser/parse_expr.h"
#include "parser/parse_oper.h"
#include "parser/parse_relation.h"
#include "pgstat.h"
#include "utils/acl.h"
#include "utils/datum.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/syscache.h"
#include "utils/tuplesort.h"
#include "utils/pg_locale.h"
#include "utils/builtins.h"
#include "utils/inval.h"
#include "cdb/cdbvars.h"
#include "cdb/cdbanalyze.h"
#include "cdb/cdbrelsize.h"
#include "utils/fmgroids.h"
#include "storage/backendid.h"
#include "executor/spi.h"
#include "postmaster/autovacuum.h"
#include "cdb/cdbtm.h"
#include "catalog/pg_namespace.h"
#include "utils/debugbreak.h"
#include "nodes/makefuncs.h"
#include "cdb/cdbhash.h"

/**
 * Statistics related parameters.
 */

int				default_statistics_target = 25;
double			analyze_relative_error = 0.25;
bool			gp_statistics_pullup_from_child_partition = FALSE;
bool			gp_statistics_use_fkeys = FALSE;
int				gp_statistics_blocks_target = 25;
double			gp_statistics_ndistinct_scaling_ratio_threshold = 0.10;
double			gp_statistics_sampling_threshold = 10000;

/**
 * This struct contains statistics produced during ANALYZE
 * on a column. 
 */
typedef struct AttributeStatistics
{
	float4 		ndistinct; /* number of distinct values. If less than 0, represents fraction of values that are distinct */
	float4 		nullFraction; /* fraction of values that are null */
	float4 		avgWidth;	/* average width of values */
	ArrayType  	*mcv;		/* most common values */
	ArrayType	*freq;		/* frequencies of most common values */
	ArrayType	*hist;		/* equi-depth histogram bounds */
} AttributeStatistics;

/**
 * Logging level.
 */
static int 			elevel = -1;

/* Top level functions */
static void analyzeRelation(Relation relation, List *lAttributeNames);
static void analyzeComputeAttributeStatistics(Oid relationOid, 
		const char *attributeName,
		float4 relTuples, 
		Oid sampleTableOid, 
		float4 sampleTableRelTuples, 
		AttributeStatistics *stats);
static List* analyzableRelations(void);
static bool analyzePermitted(Oid relationOid);
static List *analyzableAttributes(Relation candidateRelation);
static List	*buildExplicitAttributeNames(Oid relationOid, VacuumStmt *stmt);

/* Reltuples/relpages estimation functions */
static void gp_statistics_estimate_reltuples_relpages_heap(Relation rel, float4 *reltuples, float4 *relpages);
static void gp_statistics_estimate_reltuples_relpages_ao_rows(Relation rel, float4 *reltuples, float4 *relpages);
static void gp_statistics_estimate_reltuples_relpages_ao_cs(Relation rel, float4 *reltuples, float4 *relpages);
static void analyzeEstimateReltuplesRelpages(Oid relationOid, float4 *relTuples, float4 *relPages);
static void analyzeEstimateIndexpages(Oid relationOid, Oid indexOid, float4 *indexPages);

/* Attribute-type related functions */
static bool isOrderedAndHashable(Oid relationOid, const char *attributeName);
static bool isBoolType(Oid relationOid, const char *attributeName);
static bool isNotNull(Oid relationOid, const char *attributeName);
static bool	isFixedWidth(Oid relationOid, const char *attributeName, float4 *width);
static bool hasMaxDefined(Oid relationOid, const char *attributeName);

/* Sampling related */
static float4 estimateSampleSize(Oid relationOid, const char *attributeName, float4 relTuples);
static char* temporarySampleTableName(Oid relationOid);
static Oid buildSampleTable(Oid relationOid, 
		List *lAttributeNames, 
		float4	relTuples,
		float4 	requestedSampleSize, 
		float4 *sampleTableRelTuples);
static void dropSampleTable(Oid sampleTableOid);

/* Attribute statistics computation */
static int4 numberOfMCVEntries(Oid relationOid, const char *attributeName);
static int4 numberOfHistogramEntries(Oid relationOid, const char *attributeName);
static float4 analyzeComputeNDistinct(Oid sampleTableOid, 
		float4 sampleTableRelTuples, 
		const char *attributeName);
static float4 analyzeComputeNRepeating(Oid relationOid, 
		const char *attributeName);
static float4 analyzeNullCount(Oid relationOid, const char *attributeName);
static float4 analyzeComputeAverageWidth(Oid relationOid, 
		const char *attributeName,
		float4 relTuples);
static void analyzeComputeMCV(Oid relationOid, 
		const char *attributeName,
		float4 relTuples,
		unsigned int nEntries,
		ArrayType **mcv,
		ArrayType **freq);
static void analyzeComputeHistogram(Oid relationOid,  
		const char *attributeName,
		float4 relTuples,
		unsigned int nEntries,
		ArrayType *mcv,
		ArrayType **hist);

/* Catalog related */
static void updateAttributeStatisticsInCatalog(Oid relationOid, const char *attributeName, 
		AttributeStatistics *stats);
static void updateReltuplesRelpagesInCatalog(Oid relationOid, float4 relTuples, float4 relPages);

/* Convenience */
static ArrayType * SPIResultToArray(int resultAttributeNumber, MemoryContext allocationContext);

/* spi execution helpers */
typedef void (*spiCallback)(void *clientDataOut);
static void spiExecuteWithCallback(const char *src, bool read_only, long tcount,
           spiCallback callbackFn, void *clientData);

typedef struct
{
    int numColumns;
    MemoryContext memoryContext;
    ArrayType ** output;
} EachResultColumnAsArraySpec;

static void spiCallback_getEachResultColumnAsArray(void *clientData);
static void spiCallback_getProcessedAsFloat4(void *clientData);
static void spiCallback_getSingleResultRowColumnAsFloat4(void *clientData);

/**
 * Extern stuff.
 */

extern List 		*find_all_inheritors(Oid parentrel);
extern BlockNumber RelationGuessNumberOfBlocks(double totalbytes);
extern Oid	LookupFuncName(List *funcname, int nargs, const Oid *argtypes, bool noError);

/*
 * To avoid consuming too much memory during analysis and/or too much space
 * in the resulting pg_statistic rows, we ignore varlena datums that are wider
 * than WIDTH_THRESHOLD (after detoasting!).  This is legitimate for MCV
 * and distinct-value calculations since a wide value is unlikely to be
 * duplicated at all, much less be a most-common value.  For the same reason,
 * ignoring wide values will not affect our estimates of histogram bin
 * boundaries very much.
 */
#define COLUMN_WIDTH_THRESHOLD  1024


/**
 * This is the main entry point for analyze execution. Three possible ways of calling this method.
 * 1. Full database ANALYZE. No relations are explicitly specified.
 * 2. List of relations is specified (Usually by autovacuum).
 * 3. One relation is specified (optionally, a list of columns).
 * This method can only be called in DISPATCH or UTILITY roles.
 * Input:
 * 	vacstmt - Vacuum statement.
 * 	relids  - Usually NULL except when called by autovacuum.
 */
void analyzeStatement(VacuumStmt *stmt, List *relids)
{
	/* MPP-14608: Analyze may create temp tables.
	 * Disable autostats so that analyze is not called during their creation. */

	GpAutoStatsModeValue autostatvalBackup = gp_autostats_mode;
	GpAutoStatsModeValue autostatInFunctionsvalBackup = gp_autostats_mode_in_functions;

	gp_autostats_mode = GP_AUTOSTATS_NONE;
	gp_autostats_mode_in_functions = GP_AUTOSTATS_NONE;

	PG_TRY();
	{
		analyzeStmt(stmt, relids);
		gp_autostats_mode = autostatvalBackup;
		gp_autostats_mode_in_functions = autostatInFunctionsvalBackup;
	}

	/* Clean up in case of error. */
	PG_CATCH();
	{
		gp_autostats_mode = autostatvalBackup;
		gp_autostats_mode_in_functions = autostatInFunctionsvalBackup;

		/* Carry on with error handling. */
		PG_RE_THROW();
	}
	PG_END_TRY();
	Assert(gp_autostats_mode == autostatvalBackup);
	Assert(gp_autostats_mode_in_functions == autostatInFunctionsvalBackup);
}

/**
 * This method can only be called in DISPATCH or UTILITY roles.
 * Input:
 * 	vacstmt - Vacuum statement.
 * 	relids  - Usually NULL except when called by autovacuum.
 */
void analyzeStmt(VacuumStmt *stmt, List *relids)
{
	List	   			  	*lRelOids = NIL;
	MemoryContext			callerContext = NULL;
	MemoryContext 			analyzeStatementContext = NULL;
	MemoryContext 			analyzeRelationContext = NULL;
	bool					bUseOwnXacts = false;
	ListCell				*le1 = NULL;

	/**
	 * Ensure that an ANALYZE is requested.
	 */
	Assert(stmt->analyze);	
	
	/**
	 * Ensure that vacuum was not requested.
	 */
	Assert(!stmt->vacuum);
	
	/**
	 * Both relids and stmt->relation cannot be non-null.
	 */
	Assert(!(relids != NIL && stmt->relation != NULL));
	
	/**
	 * Works only in DISPATCH and UTILITY mode.
	 */
	Assert(Gp_role == GP_ROLE_DISPATCH || Gp_role == GP_ROLE_UTILITY);
	
	/**
	 * Only works in normal processing mode - should not be called in bootstrapping or
	 * init mode.
	 */
	Assert(IsNormalProcessingMode());
	
	/* If running in diagnostic mode, simply return */
	if (Gp_interconnect_type == INTERCONNECT_TYPE_NIL)
	{
		return;
	}
	
	if (stmt->verbose)
		elevel = INFO;
	else
		elevel = DEBUG2;

	callerContext = CurrentMemoryContext;

	/*
	 * This is the statement-level context. This will be cleaned up when we exit this
	 * function.
	 */
	analyzeStatementContext = AllocSetContextCreate(PortalContext,
			"Analyze",
			ALLOCSET_DEFAULT_MINSIZE,
			ALLOCSET_DEFAULT_INITSIZE,
			ALLOCSET_DEFAULT_MAXSIZE);

	MemoryContextSwitchTo(analyzeStatementContext);


	/*
	 * This is a per relation context.
	 */
	analyzeRelationContext = AllocSetContextCreate(analyzeStatementContext,
			"AnalyzeRel",
			ALLOCSET_DEFAULT_MINSIZE,
			ALLOCSET_DEFAULT_INITSIZE,
			ALLOCSET_DEFAULT_MAXSIZE);

	/**
	 * What relations need to be ANALYZED.
	 */
	if (relids == NIL && stmt->relation == NULL)
	{
		/**
		 * ANALYZE entire DB.
		 */
		lRelOids = analyzableRelations();
	}
	else if (relids != NIL)
	{
		/**
		 * ANALYZE called by autovacuum.
		 */
		lRelOids = relids;
	}
	else
	{
		/**
		 * ANALYZE one relation (optionally, a list of columns).
		 * If this happens to be a partitioned table, we create an explicit
		 * list of relids with all partitioned relations.
		 * TODO: In the future, we must do away with treating partitions separately.
		 */
		Oid relationOid = InvalidOid;
		Assert(relids == NIL);
		Assert(stmt->relation != NULL);
		relationOid = RangeVarGetRelid(stmt->relation, false);
		if (rel_is_partitioned(relationOid))
		{
			PartitionNode *pn = get_parts(relationOid, 0 /*level*/ ,
					0 /*parent*/, false /* inctemplate */, CurrentMemoryContext);
			Assert(pn);
			lRelOids = all_partition_relids(pn);
			lRelOids = lappend_oid(lRelOids, relationOid);
		}
		else
		{
			lRelOids = list_make1_oid(relationOid);
		}
	}

	/*
	 * Decide whether we need to start/commit our own transactions.
	 * The scenarios in which we can start/commit our own transactions are:
	 * 1. We are not in a transaction block and there are multiple relations specified (some of them may be implcit)
	 * 2. We are in autovacuum mode
	 */

	if ((!IsInTransactionChain((void *) stmt) && list_length(lRelOids) > 1)
			|| IsAutoVacuumProcess())
		bUseOwnXacts = true;

	/**
	 * Iterate through all relids in the list and issue analyze on all columns on each relation.
	 */

	if (bUseOwnXacts)
	{
		/*
		 * We commit the transaction started in PostgresMain() here, and start
		 * another one before exiting to match the commit waiting for us back in
		 * PostgresMain().
		 */
		CommitTransactionCommand();
		MemoryContextSwitchTo(analyzeStatementContext);
	}

	foreach (le1, lRelOids)
	{
		Oid				candidateOid	  = InvalidOid;
		Relation		candidateRelation = NULL;
		bool			bTemp;

		bTemp = false;

		Assert(analyzeStatementContext == CurrentMemoryContext);

		if (bUseOwnXacts)
		{
			/**
			 * We use a different transaction per relation so that we
			 * may release locks on relations as soon as possible.
			 */
			StartTransactionCommand();
			ActiveSnapshot = CopySnapshot(GetTransactionSnapshot());
			MemoryContextSwitchTo(analyzeStatementContext);
		}

		candidateOid = lfirst_oid(le1);
		candidateRelation =
				try_relation_open(candidateOid, ShareUpdateExclusiveLock, false);

		if (candidateRelation)
		{
			/**
			 * We got a lock on the relation. Good!
			 */
			if (analyzePermitted(RelationGetRelid(candidateRelation)))
			{
				/*
				 * We have permission to ANALYZE.
				 */

				/* MPP-7576: don't track internal namespace tables */
				switch (candidateRelation->rd_rel->relnamespace)
				{
				case PG_CATALOG_NAMESPACE:
					/* MPP-7773: don't track objects in system namespace
					 * if modifying system tables (eg during upgrade)
					 */
					if (allowSystemTableModsDDL)
						bTemp = true;
					break;

				case PG_TOAST_NAMESPACE:
				case PG_BITMAPINDEX_NAMESPACE:
				case PG_AOSEGMENT_NAMESPACE:
					bTemp = true;
					break;
				default:
					break;
				}

				/* MPP-7572: Don't track metadata if table in any
				 * temporary namespace
				 */
				if (!bTemp)
					bTemp = isAnyTempNamespace(
							candidateRelation->rd_rel->relnamespace);

				if (candidateRelation->rd_rel->relkind != RELKIND_RELATION)
				{
					/**
					 * Is the relation the right kind?
					 */
					ereport(WARNING,
							(errmsg("skipping \"%s\" --- cannot analyze indexes, views, external tables or special system tables",
									RelationGetRelationName(candidateRelation))));
					relation_close(candidateRelation, ShareUpdateExclusiveLock);
				}
				else if (isOtherTempNamespace(RelationGetNamespace(candidateRelation)))
				{
					/* Silently ignore tables that are temp tables of other backends. */
					relation_close(candidateRelation, ShareUpdateExclusiveLock);
				}
				else
				{
					List 		*lAttNames = NIL;

					/* Switch to per relation context */
					MemoryContextSwitchTo(analyzeRelationContext);

					if (stmt->va_cols)
					{
						/**
						 * Column names have been provided. Should have specified relation name as well.
						 */
						Assert(stmt->relation && "Column names specified but not relation name");
						lAttNames = buildExplicitAttributeNames(RelationGetRelid(candidateRelation), stmt);
					}
					else
					{
						lAttNames = analyzableAttributes(candidateRelation);
					}

					analyzeRelation(candidateRelation, lAttNames);

					/* Switch back to statement context and reset relation context */
					MemoryContextSwitchTo(analyzeStatementContext);
					MemoryContextResetAndDeleteChildren(analyzeRelationContext);

					/*
					 * Close source relation now, but keep lock so
					 * that no one deletes it before we commit.  (If
					 * someone did, they'd fail to clean up the
					 * entries we made in pg_statistic.  Also,
					 * releasing the lock before commit would expose
					 * us to concurrent-update failures.)
					 */

					relation_close(candidateRelation, NoLock);

					/* MPP-6929: metadata tracking */
					if (!bTemp && (Gp_role == GP_ROLE_DISPATCH))
					{
						char *asubtype = "";

						if (IsAutoVacuumProcess())
							asubtype = "AUTO";

						MetaTrackUpdObject(RelationRelationId,
								RelationGetRelid(candidateRelation),
								GetUserId(),
								"ANALYZE",
								asubtype
						);
					}
				}
			}
			else
			{
				/**
				 * We don't have permissions to ANALYZE the relation. Print warning and move on
				 * to the next relation.
				 */
				ereport(WARNING,
						(errmsg("Skipping \"%s\" --- only table or database owner can analyze it",
								RelationGetRelationName(candidateRelation))));
				relation_close(candidateRelation, ShareUpdateExclusiveLock);
			} /* if (analyzePermitted(RelationGetRelid(candidateRelation))) */
		}
		else
		{
			/*
			 * Relation may have been dropped out from under us.
			 * TODO: should we print a warning here? Do we print it during
			 * ANALYZE DB or AutoVacuum?
			 */
		} /* if (candidateRelation) */

		if (bUseOwnXacts)
		{
			/**
			 * We commit the transaction so that locks on the relation may be released.
			 */
			CommitTransactionCommand();
			MemoryContextSwitchTo(analyzeStatementContext);
		}
	}

	if (bUseOwnXacts)
	{
		/**
		 * We start a new transaction command to match the one in PostgresMain().
		 */
		/*
		 * in gpsql, there is no distributed transaction
		 */
		/*setupRegularDtxContext();*/
		StartTransactionCommand();
		ActiveSnapshot = CopySnapshot(GetTransactionSnapshot());
		MemoryContextSwitchTo(analyzeStatementContext);
	}

	Assert(analyzeStatementContext == CurrentMemoryContext);
	MemoryContextSwitchTo(callerContext);
	MemoryContextDelete(analyzeStatementContext);
}


/*
 * This method extracts the explicit attributes listed in a vacuum statement. It must
 * be called only when it is known that explicit columns have been specified in the vacuum
 * statement. It checks if attribute exists and also if attribute has been dropped. It
 * also silently drops attributes that have stats target set to 0.
 * Input:
 * 	vacstmt - vacuum statement
 * Output:
 * 	list of attribute names that the vacuum statement requests
 */
static List*	buildExplicitAttributeNames(Oid relationOid, VacuumStmt *stmt)
{
	List	*lExplicitAttNames = NIL;
	ListCell *le = NULL;
	Assert(stmt->va_cols != NULL);
	/**
	 * va_col contains list of attributes that need to be analyzed. 
	 */
	foreach (le, stmt->va_cols)
	{
		HeapTuple	attributeTuple;
		char *attributeName = strVal(lfirst(le));
		Assert(attributeName);

		attributeTuple = SearchSysCacheAttName(relationOid, attributeName);
		/* Ensure that we can actually analyze the attribute. */
		if (HeapTupleIsValid(attributeTuple))
		{
			Form_pg_attribute att_tup = (Form_pg_attribute) GETSTRUCT(attributeTuple);
			/* If attribute is dropped, print an error message */
			if (att_tup->attisdropped)
				ereport(ERROR, 
						(errcode(ERRCODE_UNDEFINED_COLUMN),
								errmsg("Attribute %s has been dropped in relation %s.", 
										attributeName, get_rel_name(relationOid))));						

			/* If statstarget=0 or type is "unknown", silently skip it. */
			if (att_tup->attstattarget != 0
					&& att_tup->atttypid != UNKNOWNOID)
			{
				lExplicitAttNames = lappend(lExplicitAttNames, attributeName);
			}
			
			ReleaseSysCache(attributeTuple);
		}
		else
		{
			/* Attribute does not exist in relation. Error out */
			ereport(ERROR, 
					(errcode(ERRCODE_UNDEFINED_COLUMN),
							errmsg("Relation %s does not have an attribute named %s.", 
									get_rel_name(relationOid),attributeName)));
		}
	}
	return lExplicitAttNames;
}

/**
 * This function determines if a table can be analyzed or not. 
 * Input:
 * 	relationOid
 * Output:
 * 	true or false
 */
static bool analyzePermitted(Oid relationOid)
{
	return (pg_class_ownercheck(relationOid, GetUserId()) 
			|| pg_database_ownercheck(MyDatabaseId, GetUserId()));
}

/**
 * If ANALYZE is requested with no relations specified, this method is called to build
 * the implicit list of relations from pg_class. Only those with relkind == RELKIND_RELATION
 * are considered.
 * 
 * Input:
 * 	None
 * Output:
 * 	List of relids
 */
static List* analyzableRelations(void)
{
	List	   		*lRelOids = NIL;
	Relation		pgclass = NULL;
	HeapScanDesc 	scan = NULL;
	HeapTuple		tuple = NULL;
	ScanKeyData 	key;

	ScanKeyInit(&key,
			Anum_pg_class_relkind,
			BTEqualStrategyNumber, F_CHAREQ,
			CharGetDatum(RELKIND_RELATION));

	pgclass = heap_open(RelationRelationId, AccessShareLock);

	scan = heap_beginscan(pgclass, SnapshotNow, 1 /* key length */, &key);

	while ((tuple = heap_getnext(scan, ForwardScanDirection)) != NULL)
	{
		Oid candidateOid = HeapTupleGetOid(tuple);
		if (analyzePermitted(candidateOid)
				&& candidateOid != StatisticRelationId)
		{
			lRelOids = lappend_oid(lRelOids, candidateOid);
		}
	}

	heap_endscan(scan);
	heap_close(pgclass, AccessShareLock);
	return lRelOids;
}

/**
 * Given a relation's Oid, generate the list of attribute names that may be analyzed.
 * This ignores columns that have been dropped or if stattarget is set to 0 by user.
 * Input:
 * 	relation
 * Output:
 * 	List of attribute names. This will need to be free'd by the caller.
 */
static List *analyzableAttributes(Relation candidateRelation)
{
	int 	i = 0;
	List	*lAttNames = NIL;
	Assert(candidateRelation != NULL);
	for (i = 0; i < candidateRelation->rd_att->natts; i++)
	{
		Form_pg_attribute attr = candidateRelation->rd_att->attrs[i];
		Assert(attr);
		/* 
		 * Skip if one of these conditions is true:
		 * 1. Attribute is dropped
		 * 2. Stats target for attribute is 0
		 * 3. Attribute has "unknown" type.
		 */
		if (!(attr->attisdropped
				|| attr->attstattarget == 0
				|| attr->atttypid == UNKNOWNOID))
		{
			char	*attName = NULL;
			attName = pstrdup(NameStr(attr->attname)); //needs to be pfree'd by caller
			Assert(attName);
			lAttNames = lappend(lAttNames, (void *) attName);
		}
	}		
	return lAttNames;
}

/**
 * This method is called once all the transactions and snapshots have been set up.
 * At a very high-level, it performs two functions:
 * 	1. Compute and update reltuples, relages for the relation.
 * 	2. Compute and update statistics on requested attributes.
 * If the input relation is too large, it may create a sampled version of the relation
 * and compute statistics.
 * TODO: Check with Daria/Eric about messages.
 * Input:
 * 	relation		- relation
 * 	lAttributeNames - list of attribute names. 
 * Output:
 * 	None
 */
static void analyzeRelation(Relation relation, List *lAttributeNames)
{
	Oid			sampleTableOid = InvalidOid;
	float4		minSampleTableSize = 0;
	bool		sampleTableRequired = true;
	ListCell	*le = NULL;
	Oid			relationOid = InvalidOid;
	float4 estimatedRelTuples = 0.0;
	float4 estimatedRelPages = 0.0;
	float4 sampleTableRelTuples = 0.0;
	List	*indexOidList = NIL;
	ListCell	*lc = NULL;
	
	relationOid = RelationGetRelid(relation);
	
	/**
	 * Step 1: estimate reltuples, relpages for the relation. 
	 */
	analyzeEstimateReltuplesRelpages(relationOid, &estimatedRelTuples, &estimatedRelPages);
	
	elog(elevel, "ANALYZE estimated reltuples=%f, relpages=%f for table %s", estimatedRelTuples, estimatedRelPages, RelationGetRelationName(relation));
	
	/**
	 * Step 2: update the pg_class entry.
	 */
	updateReltuplesRelpagesInCatalog(relationOid, estimatedRelTuples, estimatedRelPages);
	
	/* Find relpages of each index and update these as well */
	indexOidList = RelationGetIndexList(relation);
	foreach (lc, indexOidList)
	{
		float4 estimatedIndexTuples = estimatedRelTuples;
		float4 estimatedIndexPages = 0;

		Oid	indexOid = lfirst_oid(lc);
		Assert(indexOid != InvalidOid);
		
		if (estimatedRelTuples < 1.0)
		{
			/**
			 * If there are no rows in the relation, no point trying to estimate
			 * number of pages in the index.
			 */
			elog(elevel, "ANALYZE skipping index %s since relation %s has no rows.", get_rel_name(indexOid), get_rel_name(relationOid));
		}
		else 
		{
			/**
			 * NOTE: we don't attempt to estimate the number of tuples in an index.
			 * We will assume it to be equal to the estimated number of tuples in the relation.
			 * This does not hold for partial indexes. The number of tuples matching will be
			 * derived in selfuncs.c using the base table statistics.
			 */
			analyzeEstimateIndexpages(relationOid, indexOid, &estimatedIndexPages);
			elog(elevel, "ANALYZE estimated reltuples=%f, relpages=%f for index %s", estimatedIndexTuples, estimatedIndexPages, get_rel_name(indexOid));
		}
		
		updateReltuplesRelpagesInCatalog(indexOid, estimatedIndexTuples, estimatedIndexPages);
	}
	
	/* report results to the stats collector, too */
	pgstat_report_analyze(relation, estimatedRelTuples, 0 /*totaldeadrows*/);
	
	/**
	 * Does the relation have any rows. If not, no point analyzing columns.
	 */
	if (estimatedRelTuples < 1.0)
	{
		elog(elevel, "ANALYZE skipping computing statistics on table %s because it has no rows.", RelationGetRelationName(relation));
		return;
	}

	/* Cannot compute statistics on pg_statistic due to locking issues */
	if (relationOid == StatisticRelationId)
	{
		elog(elevel, "ANALYZE skipping computing statistics on pg_statistic.");
		return;
	}

	/**
	 * Determine how many rows need to be sampled.
	 */
	foreach (le, lAttributeNames)
	{
		const char *attributeName = (const char *) lfirst(le);
		float4 minRowsForAttribute = estimateSampleSize(relationOid, attributeName, estimatedRelTuples);
		minSampleTableSize = Max(minSampleTableSize, minRowsForAttribute);
	}
	
	/**
	 * If no statistics are needed on any attribute, then fall through quickly.
	 */
	if (minSampleTableSize == 0)
	{
		elog(elevel, "ANALYZE skipping computing statistics on table %s because no attribute needs it.", RelationGetRelationName(relation));
		return;		
	}

	/**
	 * Determine if a sample table needs to be created. If reltuples is very small,
	 * then, we'd rather work off the entire table. Also, if the sample required is
	 * the size of the table, then we'd rather work off the entire table.
	 */
	if (estimatedRelTuples <= gp_statistics_sampling_threshold 
			|| minSampleTableSize >= estimatedRelTuples) /* maybe this should be K% of reltuples or something? */
	{
		sampleTableRequired = false;
	}

	/**
	 * Step 3: If required, create a sample table
	 */
	if (sampleTableRequired)
	{
		elog(elevel, "ANALYZE building sample table of size %.0f on table %s because it has too many rows.", minSampleTableSize, RelationGetRelationName(relation));
		sampleTableOid = buildSampleTable(relationOid, lAttributeNames, estimatedRelTuples, minSampleTableSize, &sampleTableRelTuples);
		
		/* We must have a non-empty sample table */
		Assert(sampleTableRelTuples > 0.0);	
	}
	
	/**
	 * Step 4: ANALYZE attributes, one at a time.
	 */
	foreach (le, lAttributeNames)
	{
		AttributeStatistics	stats;
		const char *lAttributeName = (const char *) lfirst(le);
		elog(elevel, "ANALYZE computing statistics on attribute %s", lAttributeName);
		if (sampleTableRequired)			
			analyzeComputeAttributeStatistics(relationOid, lAttributeName, estimatedRelTuples, sampleTableOid, sampleTableRelTuples,  &stats);
		else
			analyzeComputeAttributeStatistics(relationOid, lAttributeName, estimatedRelTuples, relationOid, estimatedRelTuples, &stats);
		updateAttributeStatisticsInCatalog(relationOid, lAttributeName, &stats);
	}
	
	/**
	 * Step 5: Cleanup. Drop the sample table.
	 */
	if (sampleTableRequired)
	{
		elog(elevel, "ANALYZE dropping sample table");
		dropSampleTable(sampleTableOid);
	}
	
	return;
}

/**
 * Generates a table name for the auxiliary sample table that may be created during ANALYZE.
 * This is not super random. However, this should be sufficient for our purpose.
 * Input:
 * 	relationOid 	- relation
 * 	backendId	- pid of the backend.
 * Output:
 * 	sample table name. This must be pfree'd by the caller.
 */
static char* temporarySampleTableName(Oid relationOid)
{
	char tmpname[NAMEDATALEN];
	snprintf(tmpname, NAMEDATALEN, "pg_analyze_%u_%i", relationOid, MyBackendId);
	return pstrdup(tmpname);
}

/**
 * This method determines the number of mcv entries to be computed for a particular attribute.
 * Input:
 * 	relationOid - Oid of relation
 * 	attributeName - Name of attribute
 * Output:
 * 	number of mcv entries.
 */
static int4 numberOfMCVEntries(Oid relationOid, const char *attributeName)
{
	HeapTuple			attributeTuple = NULL;
	Form_pg_attribute 	attribute = NULL;
	int4	 			nMCVEntries = 0;
	
	attributeTuple = SearchSysCacheAttName(relationOid, attributeName);
	Assert(HeapTupleIsValid(attributeTuple));
	attribute = (Form_pg_attribute) GETSTRUCT(attributeTuple);
	nMCVEntries = attribute->attstattarget;
	ReleaseSysCache(attributeTuple);
	if (nMCVEntries < 0)
		nMCVEntries = (int4) default_statistics_target;
	return nMCVEntries;
}

/**
 * This method determines the number of histogram entries to be computed for a particular attribute.
 * Input:
 * 	relationOid	- relation's Oid
 * 	attributeName - Name of attribute
 * Output:
 * 	number of histogram entries. Currently, this is the same as the number of MCV entries.
 */
static int4 numberOfHistogramEntries(Oid relationOid, const char *attributeName)
{
	/** for now, the number of histogram entries is same as mcv_entries */
	return numberOfMCVEntries(relationOid, attributeName);
}

/**
 * Determine the number of tuples from relation that will need to be sampled
 * to compute statistics on the specific attribute. This method looks up the
 * stats target for the said attribute (if one is not found, the default_statistics_target
 * is used) and employs a formula (explained below) to compute the number of rows
 * needed.
 * Input:
 * 	relationOid 		- relation whose sample size must be determined
 * 	attributeName 	- attribute
 * 	relTuples			- number of tuples in relation.
 * Output:
 * 	sample size
 * 
 */
static float4 estimateSampleSize(Oid relationOid, const char *attributeName, float4 relTuples)
{
	float4		sampleSize = 0.0;
	int4		statsTarget = 0.0;
	
	statsTarget = Max(numberOfHistogramEntries(relationOid, attributeName), 
						numberOfMCVEntries(relationOid, attributeName));
	Assert(statsTarget >= 0);
	/*--------------------
	 * The following choice of minrows is based on the paper
	 * "Random sampling for histogram construction: how much is enough?"
	 * by Surajit Chaudhuri, Rajeev Motwani and Vivek Narasayya, in
	 * Proceedings of ACM SIGMOD International Conference on Management
	 * of Data, 1998, Pages 436-447.  Their Corollary 1 to Theorem 5
	 * says that for table size n, histogram size k, maximum relative
	 * error fraction, and error probability gamma, the minimum
	 * random sample size is
	 *		r = 4 * k * ln(2*n/gamma) / error_fraction^2
	 * We use gamma=0.01 in our calculations.
	 *--------------------
	 */
	Assert(relTuples > 0.0);
	if (analyze_relative_error > 0.0)
	{
		sampleSize = (float4) 4 * statsTarget * log(2 * relTuples / 0.01) / (analyze_relative_error * analyze_relative_error);
	}
	else
	{
		sampleSize = relTuples;
	}
	
	sampleSize = Max(sampleSize, 0.0);	/* Sanity check for low relTuples */
	sampleSize = Min(sampleSize, relTuples);	/* Bound sample size to table size */
	
	return sampleSize;
}

/**
 * This is a helper method that executes a SQL statement using the SPI interface.
 * It optionally calls a callback function with result pointer.
 * Input:
 * 	src - SQL string
 * 	read_only - is it a read-only call?
 * 	tcount - execution tuple-count limit, or 0 for none
 * 	callbackFn - callback function to be executed once SPI is done.
 * 	clientData - argument to call back function (usually pointer to data-structure 
 * 				that the callback function populates).
 * 
 */
static void spiExecuteWithCallback(
		const char *src,
		bool read_only,
		long tcount,
		spiCallback callbackFn,
		void *clientData)
{
	bool connected = false;
	int ret = 0;

	PG_TRY();
	{
		if (SPI_OK_CONNECT != SPI_connect())
		{
			ereport(ERROR, (errcode(ERRCODE_CDB_INTERNAL_ERROR),
					errmsg("Unable to connect to execute internal query.")));
		}
		connected = true;

		elog(elevel, "Executing SQL: %s", src);
		
		/* Do the query. */
		ret = SPI_execute(src, read_only, tcount);
		Assert(ret > 0);

		if (callbackFn)
		{
			callbackFn(clientData);
		}
		connected = false;
		SPI_finish();
	}
	/* Clean up in case of error. */
	PG_CATCH();
	{
		if (connected)
			SPI_finish();

		/* Carry on with error handling. */
		PG_RE_THROW();
	}
	PG_END_TRY();
}

/**
 * A callback function for use with spiExecuteWithCallback.  Asserts that exactly one row was returned.
 *  Gets the row's first column as a float, using 0.0 if the value is null
 */
static void spiCallback_getSingleResultRowColumnAsFloat4(void *clientData)
{
	Datum datum_f;
	bool isnull = false;
	float4 *out = (float4*) clientData;

    Assert(SPI_tuptable != NULL); // must have result
    Assert(SPI_processed == 1); //we expect only one tuple.
	Assert(SPI_tuptable->tupdesc->attrs[0]->atttypid == FLOAT4OID); // must be float4

	datum_f = heap_getattr(SPI_tuptable->vals[0], 1, SPI_tuptable->tupdesc, &isnull);

	if (isnull)
	{
		*out = 0.0;
	}
	else
	{
		*out = DatumGetFloat4(datum_f);
	}
}

/**
 * A callback function for use with spiExecuteWithCallback.  Copies the SPI_processed value into
 *    *clientDataOut, treating it as a float4 pointer.
 */
static void spiCallback_getProcessedAsFloat4(void *clientData)
{
    float4 *out = (float4*) clientData;
    *out = (float4)SPI_processed;
}

/**
 * A callback function for use with spiExecuteWithCallback.  Reads each column of output into an array
 *   The number of arrays, the memory context for them, and the output location are determined by
 *   treating *clientData as a EachResultColumnAsArraySpec and using the values there
 */
static void spiCallback_getEachResultColumnAsArray(void *clientData)
{
    EachResultColumnAsArraySpec * spec = (EachResultColumnAsArraySpec*) clientData;
    int i;
    ArrayType ** out = spec->output;

    Assert(SPI_tuptable != NULL);
    Assert(SPI_tuptable->tupdesc);

    for ( i = 1; i <= spec->numColumns; i++ )
    {
        *out = SPIResultToArray(i, spec->memoryContext);
        Assert(*out);
        out++;
    }
}

/**
 * This method builds a sampled version of the given relation. The sampled
 * version is created in a temp namespace. Note that ANALYZE can be
 * executed only by the database owner. It is safe to assume that the database
 * owner has permissions to create temp tables. The sampling is done by
 * using the random() built-in function. 
 * 
 * TODO: Once tablesample becomes available, rewrite this function to utilize tablesample.
 * 
 * Input:
 * 	relationOid 	- relation to be sampled
 * 	lAttributeNames - attributes to be included in the sample
 * 	relTuples		- estimated size of relation
 * 	requestedSampleSize - as determined by attribute statistics requirements.
 * 	sampleLimit		- limit on size of the sample.
 * Output:
 * 	sampleTableRelTuples - number of tuples in the sample table created.
 */
static Oid buildSampleTable(Oid relationOid, 
		List *lAttributeNames, 
		float4	relTuples,
		float4 	requestedSampleSize, 
		float4 *sampleTableRelTuples)
{
	int nAttributes = -1;
	StringInfoData str;
	int i = 0;
	ListCell *le = NULL;
	const char *schemaName = NULL;
	const char *tableName = NULL;
	char	*sampleSchemaName = pstrdup("pg_temp"); 
	char 	*sampleTableName = NULL;
	Oid			sampleTableOid = InvalidOid;
	float4		randomThreshold = 0.0;
	RangeVar 	*rangeVar = NULL;
	
	Assert(requestedSampleSize > 0.0);
	Assert(relTuples > 0.0);
	
	randomThreshold = requestedSampleSize / relTuples;
	
	schemaName = get_namespace_name(get_rel_namespace(relationOid)); //must be pfreed
	tableName = get_rel_name(relationOid); //must be pfreed
	sampleTableName = temporarySampleTableName(relationOid); // must be pfreed 

	initStringInfo(&str);
	appendStringInfo(&str, "create table %s.%s as (select ", 
			quote_identifier(sampleSchemaName), quote_identifier(sampleTableName)); 
	
	nAttributes = list_length(lAttributeNames);

	foreach_with_count(le, lAttributeNames, i)
	{
		appendStringInfo(&str, "Ta.%s", quote_identifier((const char *) lfirst(le)));
		if (i < nAttributes - 1)
		{
			appendStringInfo(&str, ", ");
		}
		else
		{
			appendStringInfo(&str, " ");			
		}
	}
	
	appendStringInfo(&str, "from only %s.%s as Ta where random() < %.38f limit %lu) distributed randomly", 
			quote_identifier(schemaName), 
			quote_identifier(tableName), randomThreshold, (unsigned long) requestedSampleSize);

	spiExecuteWithCallback(str.data, false /*readonly*/, 0 /*tcount */, 
	                        spiCallback_getProcessedAsFloat4, sampleTableRelTuples);

	pfree(str.data);
		
	elog(elevel, "Created sample table %s.%s with nrows=%.0f", 
			quote_identifier(sampleSchemaName), 
			quote_identifier(sampleTableName), 
			*sampleTableRelTuples);

	rangeVar = makeRangeVar(sampleSchemaName, sampleTableName, -1);
	sampleTableOid = RangeVarGetRelid(rangeVar, true);

	Assert(sampleTableOid != InvalidOid);

	/**
	 * MPP-10723: Very rarely, we may be unlucky and generate an empty sample table. We error out in this case rather than
	 * generate bad statistics.
	 */
	
	if (*sampleTableRelTuples < 1.0)
	{
		elog(ERROR, "ANALYZE unable to generate accurate statistics on table %s.%s. Try lowering gp_analyze_relative_error", 
				quote_identifier(schemaName), 
				quote_identifier(tableName));
	}
	
	/* 
	 * Update the sample table's reltuples, relpages. Without these, the queries to the sample table would call cdbRelsize which can be an expensive call. 
	 * We know the number of tuples in the sample table, but don't have the information about the number of pages. We set it to 2 arbitrarily.
	 */
	updateReltuplesRelpagesInCatalog(sampleTableOid, *sampleTableRelTuples, 2);

	pfree((void *) rangeVar);
	pfree((void *) sampleTableName);
	pfree((void *) tableName);
	pfree((void *) schemaName);
	pfree((void *) sampleSchemaName);
	return sampleTableOid;
}

/**
 * Drops the sample table created during ANALYZE.
 */
static void dropSampleTable(Oid sampleTableOid)
{
	StringInfoData str;
	const char *sampleSchemaName = NULL;
	const char *sampleTableName = NULL;

	sampleSchemaName = get_namespace_name(get_rel_namespace(sampleTableOid)); //must be pfreed
	sampleTableName = get_rel_name(sampleTableOid); // must be pfreed 	

	initStringInfo(&str);
	appendStringInfo(&str, "drop table %s.%s", 
			quote_identifier(sampleSchemaName), 
			quote_identifier(sampleTableName));
	
	spiExecuteWithCallback(str.data, false /*readonly*/, 0 /*tcount */, 
	                        NULL, NULL);

	pfree(str.data);
	pfree((void *)sampleSchemaName);
	pfree((void *)sampleTableName);
}


/**
 * This method determines the number of pages corresponding to an index.
 * Input:
 * 	relationOid - relation being analyzed
 * 	indexOid - index whose size is to be determined
 * Output:
 * 	indexPages - number of pages in the index
 */
static void analyzeEstimateIndexpages(Oid relationOid, Oid indexOid, float4 *indexPages)
{
	*indexPages = 0;			
		
	Relation rel = try_relation_open(indexOid, AccessShareLock, false);

	if (rel != NULL)
	{
		Assert(rel->rd_rel->relkind == RELKIND_INDEX);

		*indexPages = RelationGetNumberOfBlocks(rel);

		relation_close(rel, AccessShareLock);
	}
	return;
}

/**
 * This method estimates reltuples/relpages for a relation. To do this, it employs
 * the built-in function 'gp_statistics_estimate_reltuples_relpages'. If the table to be
 * analyzed is a system table, then it calculates statistics only using the master.
 * Input:
 * 	relationOid - relation's Oid
 * Output:
 * 	relTuples - estimated number of tuples
 * 	relPages  - estimated number of pages
 */
static void analyzeEstimateReltuplesRelpages(Oid relationOid, float4 *relTuples, float4 *relPages)
{	
	*relPages = 0.0;		
	*relTuples = 0.0;			
		
	Relation rel = try_relation_open(relationOid, AccessShareLock, false);

	if (rel != NULL ) {
		Assert(rel->rd_rel->relkind == RELKIND_RELATION);
		if (RelationIsHeap(rel))
		{
			/*
			 * in gpsql, all heap table should not be distributed.
			 */
			gp_statistics_estimate_reltuples_relpages_heap(rel, relTuples,
					relPages);
		} 
		else if (RelationIsAoRows(rel))
		{
			gp_statistics_estimate_reltuples_relpages_ao_rows(rel, relTuples,
					relPages);
		} 
		else if (RelationIsAoCols(rel))
		{
			gp_statistics_estimate_reltuples_relpages_ao_cs(rel, relTuples,
					relPages);
		}
	}

	relation_close(rel, AccessShareLock);
}

/**
 * This method updates reltuples, relpages in the pg_class entry
 * for the specified table.
 * Input:
 * 	relationOid - relation whose entry must be changed
 * 	reltuples 	- number of tuples as estimated during ANALYZE
 * 	relpages	- number of pages as estimated during ANALYZE
 */
static void updateReltuplesRelpagesInCatalog(Oid relationOid, float4 relTuples, float4 relPages)
{
	Relation	pgclass = NULL;
	HeapTuple	tuple = NULL;
	Form_pg_class pgcform = NULL;
	bool		dirty = false;

	Assert(relationOid != InvalidOid);
	Assert(relTuples > -1.0);
	Assert(relPages > -1.0);
	
	/* 
	 * We need a way to distinguish these 2 cases:
	 * a) ANALYZEd table is empty
	 * b) Table has never been ANALYZEd
	 * To do this, in case (a), we set relPages = 1. For case (b), relPages = 0.
	 */
	if (relPages < 1.0)
	{
		Assert(relTuples < 1.0);
		relPages = 1.0;
	}

	/*
	 * update number of tuples and number of pages in pg_class
	 */
	pgclass = heap_open(RelationRelationId, RowExclusiveLock);

	/* Fetch a copy of the tuple to scribble on */
	tuple = SearchSysCacheCopy(RELOID,
							  ObjectIdGetDatum(relationOid),
							  0, 0, 0);
	/* We have locked the relation. We should not have trouble finding pg_class tuple */
	Assert(HeapTupleIsValid(tuple));
	pgcform = (Form_pg_class) GETSTRUCT(tuple);

	/* Apply required updates, if any, to copied tuple */

	if (pgcform->relpages != (int32) relPages)
	{
		pgcform->relpages = (int32) relPages;
		dirty = true;
	}
	if (pgcform->reltuples != (float4) relTuples)
	{
		pgcform->reltuples = (float4) relTuples;
		dirty = true;
	}

	elog(DEBUG3, "ANALYZE oid=%u pages=%d tuples=%f",
		 relationOid, pgcform->relpages, pgcform->reltuples);
	
	/*
	 * If anything changed, write out the tuple.  
	 */
	if (dirty)
	{
		heap_inplace_update(pgclass, tuple);
		/* the above sends a cache inval message */
	}

	heap_close(pgclass, RowExclusiveLock);
}

/**
 * Does this attribute have a total ordering defined i.e. does it have "<", "="
 * and is the attribute hashable?
 * Input:
 * 	relationOid - relation's Oid
 * 	attributeName - attribute in question
 * Output:
 * 	true if operators are defined and the attribute type is hashable, false otherwise. 
 */
static bool isOrderedAndHashable(Oid relationOid, const char *attributeName)
{
	HeapTuple	attributeTuple = NULL;
	Form_pg_attribute attribute = NULL;
	Operator	equalityOperator = NULL;
	Operator	ltOperator = NULL;
	bool		result = true;
	
	attributeTuple = SearchSysCacheAttName(relationOid, attributeName);
	Assert(HeapTupleIsValid(attributeTuple));	
	attribute = (Form_pg_attribute) GETSTRUCT(attributeTuple);	

	/* Does type have "=" operator */
	equalityOperator = equality_oper(attribute->atttypid, true);
	if (!equalityOperator)
		result = false;
	else
		ReleaseSysCache(equalityOperator);

	/* Does type have "<" operator */
	ltOperator = ordering_oper(attribute->atttypid, true);
	if (!ltOperator)
		result = false;
	else
		ReleaseSysCache(ltOperator);
	
	/* Is the attribute hashable?*/
	if (!isGreenplumDbHashable(attribute->atttypid))
		result = false;
	
	ReleaseSysCache(attributeTuple);
	
	return result;
}
/**
 * Does attribute have "max" aggregate function defined? We can compute histogram
 * only if this function is defined.
 * Input:
 * 	relationOid - relation's Oid
 * 	attributeName - attribute in question
 * Output:
 * 	true if "max" function is defined, false otherwise. 
 */
static bool hasMaxDefined(Oid relationOid, const char *attributeName)
{
	HeapTuple	attributeTuple = NULL;
	Form_pg_attribute attribute = NULL;
	Oid			maxAggregateFunction = InvalidOid;
	bool		result = true;
	List		*funcNames = NIL;
	
	attributeTuple = SearchSysCacheAttName(relationOid, attributeName);
	Assert(HeapTupleIsValid(attributeTuple));	
	attribute = (Form_pg_attribute) GETSTRUCT(attributeTuple);	
	
	/* Does type have "max" operator */
	funcNames = list_make1(makeString("max"));
	maxAggregateFunction = LookupFuncName(funcNames, 1 /* nargs to function */, 
										&attribute->atttypid, true);
	if (maxAggregateFunction == InvalidOid)
	{
		result = false;
	}
	
	ReleaseSysCache(attributeTuple);	
	return result;
}


/**
 * Is the attribute of type boolean? 
 * Input:
 * 	relationOid - relation's Oid
 * 	attributeName - attribute in question
 * Output:
 * 	true or false
 */
static bool isBoolType(Oid relationOid, const char *attributeName)
{
	HeapTuple	attributeTuple = NULL;
	Form_pg_attribute attribute = NULL;
	bool		isBool = false;
	
	attributeTuple = SearchSysCacheAttName(relationOid, attributeName);
	Assert(HeapTupleIsValid(attributeTuple));
	attribute = (Form_pg_attribute) GETSTRUCT(attributeTuple);
	isBool = (attribute->atttypid == BOOLOID);
	ReleaseSysCache(attributeTuple);
	return isBool;
}

/**
 * Compute absolute number of distinct values in the sample table. This method constructs
 * a SQL string using the sampleTableOid and issues an SPI query.
 * Input:
 * 	sampleTableOid - sample table's oid
 * 	sampleTableRelTuples - number of tuples in the sample table (this may be estimated or actual)
 * 	attributeName - attribute in question
 * Output:
 * 	number of distinct values of attribute
 */
static float4 analyzeComputeNDistinct(Oid sampleTableOid, 
										float4 sampleTableRelTuples, 
										const char *attributeName)
{

	StringInfoData str;
	float4	ndistinct = -1.0;

	const char *sampleSchemaName = NULL;
	const char *sampleTableName = NULL;
	
	sampleSchemaName = get_namespace_name(get_rel_namespace(sampleTableOid)); //must be pfreed
	sampleTableName = get_rel_name(sampleTableOid); // must be pfreed 	

	initStringInfo(&str);
	appendStringInfo(&str, "select count(*)::float4 from (select Ta.%s from only %s.%s as Ta group by Ta.%s) as Tb", 
			quote_identifier(attributeName), 
			quote_identifier(sampleSchemaName), 
			quote_identifier(sampleTableName),
			quote_identifier(attributeName));

    spiExecuteWithCallback(str.data, false /*readonly*/, 0 /*tcount */,
    	                        spiCallback_getSingleResultRowColumnAsFloat4, &ndistinct);

	pfree(str.data);
	pfree((void *) sampleTableName);
	pfree((void *) sampleSchemaName);

	elog(elevel, "count(ndistinct()) gives %f values.", ndistinct);
	
	return ndistinct;
}

/**
 * Compute the number of repeating values in a relation.
 * Input:
 * 	relationOid - relation's oid
 * 	attributeName - attribute
 * Output:
 * 	number of values that repeat i.e. their frequency is greater than 1.
 * 
 * TODO: this query is very similar to MCV query. Can we do some optimization here?
 */
static float4 analyzeComputeNRepeating(Oid relationOid, 
									  const char *attributeName)
{
	StringInfoData str;
	float4	nRepeating = -1.0;
	
	const char *sampleSchemaName = NULL;
	const char *sampleTableName = NULL;
	
	sampleSchemaName = get_namespace_name(get_rel_namespace(relationOid)); //must be pfreed
	sampleTableName = get_rel_name(relationOid); // must be pfreed 	

	initStringInfo(&str);
	appendStringInfo(&str, "select count(v)::float4 from (select Ta.%s as v, count(Ta.%s) as f from only %s.%s as Ta group by Ta.%s) as foo where f > 1", 
			quote_identifier(attributeName), 
			quote_identifier(attributeName),
			quote_identifier(sampleSchemaName), 
			quote_identifier(sampleTableName),
			quote_identifier(attributeName));

    spiExecuteWithCallback(str.data, false /*readonly*/, 0 /*tcount */,
    	                        spiCallback_getSingleResultRowColumnAsFloat4, &nRepeating);

	pfree(str.data);
	pfree((void *) sampleTableName);
	pfree((void *) sampleSchemaName);

	return nRepeating;
}

/**
 * This routine creates an array from one of the result attributes after an SPI call.
 * It allocates this array in the specified context. Note that, in general, the allocation 
 * context must not the SPI context because that is likely to get cleaned out soon. 
 * 
 * Input:
 * 	resultAttributeNumber - attribute to flatten into an array (1 based)
 * Output:
 * 	array of attribute type
 */
static ArrayType * SPIResultToArray(int resultAttributeNumber, MemoryContext allocationContext)
{
	ArrayType *result = NULL;
	int i = 0;
	Oid typOutput = InvalidOid;
	bool isVarLena = false;
	MemoryContext 	callerContext;
	Form_pg_attribute attribute = SPI_tuptable->tupdesc->attrs[resultAttributeNumber - 1];

	Assert(attribute);

	callerContext = MemoryContextSwitchTo(allocationContext);	
	result = construct_empty_array(attribute->atttypid);
	
	/**
	 * We should not need to detoast the type.
	 */
	getTypeOutputInfo(attribute->atttypid, &typOutput, &isVarLena);
	
	for (i=0;i<SPI_processed;i++)
	{
		Datum			dValue = 0;
		bool			isnull = false;
		int				index = 0;
		Datum			deToastedValue = 0;
		
		dValue = heap_getattr(SPI_tuptable->vals[i], resultAttributeNumber, SPI_tuptable->tupdesc, &isnull);

		if (!isnull)
		{
			if (isVarLena)
			{
				deToastedValue = PointerGetDatum(PG_DETOAST_DATUM(dValue));
			}
			else
			{
				deToastedValue = dValue;
			}

			/**
			 * Add this value to the result array.
			 */
			index = i+1; /* array indices are 1 based */
			result = array_set(result, 1 /* nSubscripts */, &index, deToastedValue, isnull,
					-1 /* arraytyplen */, 
					attribute->attlen, 
					attribute->attbyval, 
					attribute->attalign);

			if (deToastedValue != dValue)
			{
				pfree(DatumGetPointer(deToastedValue));
			}
		}
	}
	MemoryContextSwitchTo(callerContext);
	return result;
}

/**
 * This method determines if a particular attribute has the "not null" property.
 * Input:
 * 	relationOid - relation's oid
 * 	attributeName
 * Output:
 * 	true if attribute is not-null. false otherwise.
 */

static bool isNotNull(Oid relationOid, const char *attributeName)
{
	HeapTuple	attributeTuple = NULL;
	Form_pg_attribute attribute = NULL;
	bool		nonNull = false;
	
	attributeTuple = SearchSysCacheAttName(relationOid, attributeName);
	Assert(HeapTupleIsValid(attributeTuple));
	attribute = (Form_pg_attribute) GETSTRUCT(attributeTuple);
	nonNull = attribute->attnotnull;
	ReleaseSysCache(attributeTuple);
	if (nonNull)
		return true;
	else
		return false;
}

/**
 * Computes the absolute number of NULLs in the sample table.
 * Input:
 * 	relationOid - table to query
 * 	attributeName  - attribute in question
 * Output:
 * 	Number of NULLs
 */
static float4 analyzeNullCount(Oid relationOid, const char *attributeName)
{
	StringInfoData str;
	float4	nullcount = 0.0;
	const char *sampleSchemaName = NULL;
	const char *sampleTableName = NULL;
	
	sampleSchemaName = get_namespace_name(get_rel_namespace(relationOid)); //must be pfreed
	sampleTableName = get_rel_name(relationOid); // must be pfreed 	

	initStringInfo(&str);
	appendStringInfo(&str, "select count(*)::float4 from only %s.%s as Ta where Ta.%s is null", 
			quote_identifier(sampleSchemaName), 
			quote_identifier(sampleTableName), 
			quote_identifier(attributeName));

    spiExecuteWithCallback(str.data, false /*readonly*/, 0 /*tcount */,
    	                        spiCallback_getSingleResultRowColumnAsFloat4, &nullcount);

	pfree(str.data);
	pfree((void *) sampleTableName);
	pfree((void *) sampleSchemaName);

	return nullcount;
}

/**
 * Is the attribute fixed-width? If so, this method extracts the width of the attribute.
 * Input:
 * 	relationOid	- relation's oid
 * 	attributeName - name of attribute
 * Output:
 * 	returns true if fixed width
 * 	width - must be non-null. sets width if it returns true.
 */

static bool	isFixedWidth(Oid relationOid, const char *attributeName, float4 *width)
{
	HeapTuple	attributeTuple = NULL;
	Form_pg_attribute attribute = NULL;
	float4	avgwidth = 0.0;
	Assert(width);
	
	attributeTuple = SearchSysCacheAttName(relationOid, attributeName);
	Assert(HeapTupleIsValid(attributeTuple));
	attribute = (Form_pg_attribute) GETSTRUCT(attributeTuple);
	avgwidth = get_typlen(attribute->atttypid);
	ReleaseSysCache(attributeTuple);
	
	if (avgwidth > 0)
	{
		*width = avgwidth;
		return true;
	}
	else 
	{
		return false;
	}
}

/**
 * Computes average width of an attribute. This uses the built-in pg_column_size.
 * Input:
 * 	sampleTableOid - relation on which to compute average width.
 * 	attributeName - name of attribute
 * 	sampleTableRelTuples - size of relation
 * Output:
 * 	Average width of attribute.
 */
static float4 analyzeComputeAverageWidth(Oid relationOid, 
										const char *attributeName,
										float4 relTuples)
{
	StringInfoData str;
	float4	avgwidth = 0.0;
	
	const char *sampleSchemaName = NULL;
	const char *sampleTableName = NULL;
		
	sampleSchemaName = get_namespace_name(get_rel_namespace(relationOid)); //must be pfreed
	sampleTableName = get_rel_name(relationOid); // must be pfreed 	
	
	initStringInfo(&str);
	appendStringInfo(&str, "select avg(pg_column_size(Ta.%s))::float4 from only %s.%s as Ta where Ta.%s is not null", 
			quote_identifier(attributeName), 
			quote_identifier(sampleSchemaName), 
			quote_identifier(sampleTableName), 
			quote_identifier(attributeName));

    spiExecuteWithCallback(str.data, false /*readonly*/, 0 /*tcount */,
    	                        spiCallback_getSingleResultRowColumnAsFloat4, &avgwidth);

	pfree(str.data);
	pfree((void *) sampleTableName);
	pfree((void *) sampleSchemaName);

	return avgwidth;
}

/**
 * Computes the most common values and their frequencies for relation.
 * Input:
 * 	relationOid - relation's oid
 * 	attributeName - attribute in question
 * 	relTuples - number of rows in relation
 * 	nEntries  - number of MCV entries
 * Output:
 * 	mcv - array of most common values (type is the same as attribute type)
 * 	freq - array of float4's
 */
static void analyzeComputeMCV(Oid relationOid, 
		const char *attributeName,
		float4 relTuples,
		unsigned int nEntries,
		ArrayType **mcv,
		ArrayType **freq)
{
	StringInfoData str;
	const char *sampleSchemaName = NULL;
	const char *sampleTableName = NULL;
    ArrayType *spiResult[2];
	EachResultColumnAsArraySpec spec;

	Assert(relTuples > 0.0);
	Assert(nEntries > 0);
	
	sampleSchemaName = get_namespace_name(get_rel_namespace(relationOid)); //must be pfreed
	sampleTableName = get_rel_name(relationOid); // must be pfreed 	
	
	initStringInfo(&str);
	appendStringInfo(&str, "select Ta.%s as v, count(Ta.%s)::float4/%f::float4 as f from only %s.%s as Ta "
			"where Ta.%s is not null group by (Ta.%s) order by f desc limit %u", 
			quote_identifier(attributeName), 
			quote_identifier(attributeName), 
			relTuples,
			quote_identifier(sampleSchemaName), 
			quote_identifier(sampleTableName), 
			quote_identifier(attributeName), 
			quote_identifier(attributeName),
			nEntries);


    spec.numColumns = 2;
    spec.output = spiResult;
    spec.memoryContext = CurrentMemoryContext;
    spiExecuteWithCallback(str.data, false /*readonly*/, 0 /*tcount */,
    	                        spiCallback_getEachResultColumnAsArray, &spec);

    *mcv = spiResult[0]; /* first attribute of result are mcvs */
    *freq = spiResult[1]; /* second attribute of result are freqs */

	pfree(str.data);
	pfree((void *) sampleTableName);
	pfree((void *) sampleSchemaName);

	return;
}

/**
 * Compute histogram entries for relation.
 * Input:
 * 	relationOid - relation
 * 	attributeName - attribute
 * 	relTuples 	 - estimated number of tuples in relation
 * 	nEntries	 - number of histogram entries requested
 * 	mcv			 - most common values calculated. these must be excluded before calculating
 * 				   histogram. this may be null.
 * Output:
 * 	hist - array of values representing a histogram. This histogram contains no repeating values.
 */
static void analyzeComputeHistogram(Oid relationOid,  
		const char *attributeName,
		float4 relTuples,
		unsigned int nEntries,
		ArrayType *mcv,
		ArrayType **hist)
{
	StringInfoData str;
	unsigned int bucketSize = 0;
	EachResultColumnAsArraySpec spec;
	StringInfoData whereClause;
	
	const char *sampleSchemaName = NULL;
	const char *sampleTableName = NULL;

	Assert(relTuples > 0.0);
	Assert(nEntries > 0);
	
	sampleSchemaName = get_namespace_name(get_rel_namespace(relationOid)); //must be pfreed
	sampleTableName = get_rel_name(relationOid); // must be pfreed 	

	bucketSize = relTuples / nEntries;
	
	if (bucketSize < 1) /* Any self-respecting histogram bucket must have at least 1 entry. */
		bucketSize = 1;
	
	initStringInfo(&str);
	initStringInfo(&whereClause);
	appendStringInfo(&whereClause, "%s is not null", quote_identifier(attributeName));
	
	/**
	 * This query orders the table by rank on the value and chooses values that occur every
	 * 'bucketSize' interval. It also chooses the maximum value from the relation. It then
	 * removes duplicate values and orders the values in ascending order. This corresponds to
	 * the histogram.
	 */
	
	appendStringInfo(&str, "select v from ("
			"select Ta.%s as v, row_number() over (order by Ta.%s) as r from only %s.%s as Ta where %s "
			"union select max(Tb.%s) as v, 1 as r from only %s.%s as Tb where %s) as foo "
			"where r %% %u = 1 group by v order by v", 
			quote_identifier(attributeName), 
			quote_identifier(attributeName), 
			quote_identifier(sampleSchemaName), 
			quote_identifier(sampleTableName), 
			whereClause.data,
			quote_identifier(attributeName), 
			quote_identifier(sampleSchemaName), 
			quote_identifier(sampleTableName), 
			whereClause.data,
			bucketSize);

    spec.numColumns = 1;
    spec.output = hist;
    spec.memoryContext = CurrentMemoryContext;

    spiExecuteWithCallback(str.data, false /*readonly*/, 0 /*tcount */,
    	                        spiCallback_getEachResultColumnAsArray, &spec);
	pfree(str.data);
	pfree((void *) sampleTableName);
	pfree((void *) sampleSchemaName);
	return;
}


/**
 * Responsible for computing statistics on relationOid. 
 * Input:
 * 	relationOid - original relation on which statistics must be computed.
 * 	attributeName - name of attribute
 * 	relTuples   - expected number of tuples in relation
 * 	sampleTableOid - Oid of the sampled version of the table. It is possible that sampleTableOid == relationOid.
 * 	sampleTableRelTuples - number of tuples in sampled version
 * Output:
 * 	stats - structure containing nullfrac, avgwidth, ndistinct, mcv, hist
 */
static void analyzeComputeAttributeStatistics(Oid relationOid, 
		const char *attributeName,
		float4 relTuples, 
		Oid sampleTableOid, 
		float4 sampleTableRelTuples, 
		AttributeStatistics *stats)
{
	bool computeWidth = true;
	bool computeDistinct = true;
	bool computeMCV = true;
	bool computeHist = true;
	
	Assert(stats != NULL);
	Assert(sampleTableRelTuples > 0.0);
	
	/* Default values */
	stats->ndistinct = -1.0;
	stats->nullFraction = 0.0;
	stats->avgWidth = 0.0;
	stats->mcv = NULL;
	stats->freq = NULL;
	stats->hist = NULL;
		
	if (isNotNull(relationOid, attributeName))
	{
		stats->nullFraction = 0.0;
	}
	else
	{
		float4 nullCount = analyzeNullCount(sampleTableOid, attributeName);
		if (ceil(nullCount) >= ceil(sampleTableRelTuples))
		{
			/**
			 * All values are null. no point computing other statistics.
			 */
			computeWidth = false;
			computeDistinct = false;
			computeMCV = false;
			computeHist = false;
		}
		stats->nullFraction = Min(nullCount / sampleTableRelTuples, 1.0);
	}
	elog(elevel, "nullfrac = %.38f", stats->nullFraction);		
	
	if (computeWidth && !isFixedWidth(relationOid, attributeName, &stats->avgWidth))
	{
		stats->avgWidth = analyzeComputeAverageWidth(sampleTableOid, attributeName, sampleTableRelTuples);
	}
	
	elog(elevel, "avgwidth = %f", stats->avgWidth);		
	
	if (!isOrderedAndHashable(relationOid, attributeName))
	{
		computeDistinct = false;
		computeMCV = false;
		computeHist = false;
	}
	
	if (!hasMaxDefined(relationOid, attributeName))
	{
		computeHist = false;
	}

	if (isBoolType(relationOid, attributeName))
	{
		stats->ndistinct = 2;
		computeDistinct = false;
		computeHist = false;
	}
	
	if (stats->avgWidth > COLUMN_WIDTH_THRESHOLD)
	{
		/* Extremely wide columns are considered to be fully distinct. See comments
		 * against COLUMN_WIDTH_THRESHOLD */
		computeDistinct = false;
		computeMCV = false;
		computeHist = false;
	}
		
	if (computeDistinct)
	{
		float4 	nDistinctAbsolute = analyzeComputeNDistinct(sampleTableOid, sampleTableRelTuples, attributeName);

		if (nDistinctAbsolute < 1.0)
		{
			/*
			 * This can happen if no sample table was employed and all values happen
			 * to be null and we did not estimate relTuples accurately.
			 */
			Assert(sampleTableOid == relationOid);
			stats->ndistinct = 0.0; 
			computeMCV = false;
			computeHist = false;
		}
		else if (ceil(sampleTableRelTuples) == ceil(nDistinctAbsolute))
		{
			/**
			 * Since all values are distinct, we assume that all values in the original
			 * relation are distinct.
			 */
			stats->ndistinct = -1.0;
			
			/* 
			 * If there are many more distinct values than mcv buckets,
			 * we can ignore computing mcv values.
			 */
			if (nDistinctAbsolute > numberOfMCVEntries(relationOid, attributeName))
				computeMCV = false;
		}
		else
		{
			/**
			 * Some values repeated - the number of repeating values is used
			 * to determine the total number of distinct values in the original relation.
			 */
			float4 nRepeatingAbsolute = analyzeComputeNRepeating(sampleTableOid, attributeName);

			/**
			 * MPP-10301. Note that some time has passed since nDistinct was computed. If the table being analyzed
			 * is a catalog table, it could have changed in this period. 
			 */
			if (nRepeatingAbsolute > nDistinctAbsolute)
			{
				nRepeatingAbsolute = nDistinctAbsolute;
			}

			if (ceil(nRepeatingAbsolute) == ceil(nDistinctAbsolute))
			{
				/* All distinct values are in the sample. Assumption is that there exist no distinct values outside this world. */					
				stats->ndistinct = nDistinctAbsolute;
			}
			else
			{
				/*----------
				 * Estimate the number of distinct values using the estimator
				 * proposed by Haas and Stokes in IBM Research Report RJ 10025:
				 *		n*d / (n - f1 + f1*n/N)
				 * where f1 is the number of distinct values that occurred
				 * exactly once in our sample of n rows (from a total of N),
				 * and d is the total number of distinct values in the sample.
				 * This is their Duj1 estimator; the other estimators they
				 * recommend are considerably more complex, and are numerically
				 * very unstable when n is much smaller than N.
				 */
				double onceInSample = nDistinctAbsolute - nRepeatingAbsolute;				
				double numer = sampleTableRelTuples * nDistinctAbsolute;
				double denom = sampleTableRelTuples - onceInSample + onceInSample * (sampleTableRelTuples / relTuples);
				stats->ndistinct = (float4) numer / denom;

				/* clamp range */
				if (stats->ndistinct < nDistinctAbsolute)
					stats->ndistinct = nDistinctAbsolute;
				if (stats->ndistinct > relTuples)
					stats->ndistinct = relTuples;
			}
			
			/**
			 * Does ndistinct scale with reltuples?
			 */
			if (stats->ndistinct > relTuples * gp_statistics_ndistinct_scaling_ratio_threshold) 
			{
				stats->ndistinct = -1.0 * stats->ndistinct / relTuples;
			}			
		}
	}
	elog(elevel, "ndistinct = %f", stats->ndistinct);
	
	if (computeMCV)
	{
		unsigned int nMCVEntries = numberOfMCVEntries(relationOid, attributeName);
		analyzeComputeMCV(sampleTableOid, attributeName, sampleTableRelTuples, 
				nMCVEntries, &stats->mcv, &stats->freq);
		if (stats->mcv)
			elog(elevel, "mcv=%s", OidOutputFunctionCall(751,PointerGetDatum(stats->mcv)));
		if (stats->freq)
			elog(elevel, "freq=%s", OidOutputFunctionCall(751,PointerGetDatum(stats->freq)));
	}	
	
	if (computeHist)	
	{
		unsigned int nHistEntries = numberOfHistogramEntries(relationOid, attributeName);
		analyzeComputeHistogram(sampleTableOid, attributeName, sampleTableRelTuples, 
				nHistEntries, stats->mcv, &stats->hist);
		if (stats->hist)
			elog(elevel, "hist=%s", OidOutputFunctionCall(751,PointerGetDatum(stats->hist)));
	}
}

/**
 * This method updates the pg_statistic tuple for the specific attribute. It populates
 * it with the statistics computed by ANALYZE.
 * Input:
 * 	relationOid - relation
 * 	attributeName - name of the attribute
 *  stats		- struct containing all the computed statistics
 *  
 */
static void updateAttributeStatisticsInCatalog(Oid relationOid, const char *attributeName, AttributeStatistics *stats)
{
	Datum		values[Natts_pg_statistic];
	bool		nulls[Natts_pg_statistic];
	bool		replaces[Natts_pg_statistic];
	int2		attNum = -1;	
	int 		i = 0;
	Oid			equalityOid = InvalidOid;
	Oid			ltOid = InvalidOid;
	
	Assert(stats);

	{
		/**
		 * For some reason, pg_statistic stores the "<" and "=" operator associated
		 * with the attribute. 
		 */
		HeapTuple	attributeTuple;
		Form_pg_attribute attribute;
		Operator	equalityOperator;
		Operator	ltOperator;

		attributeTuple = SearchSysCacheAttName(relationOid, attributeName);
		Assert(HeapTupleIsValid(attributeTuple));
		attribute = (Form_pg_attribute) GETSTRUCT(attributeTuple);
		attNum = attribute->attnum;
		equalityOperator = equality_oper(attribute->atttypid, true);
		
		if (equalityOperator)
		{
			equalityOid = oprid(equalityOperator);
			ReleaseSysCache(equalityOperator);
		}
		
		ltOperator = ordering_oper(attribute->atttypid, true);
		
		if (ltOperator)
		{
			ltOid = oprid(ltOperator);
			ReleaseSysCache(ltOperator);
		}
		ReleaseSysCache(attributeTuple);
	}

	for (i = 0; i < Natts_pg_statistic; ++i)
	{
		nulls[i] = false;
		replaces[i] = true;
	}

	values[Anum_pg_statistic_starelid - 1] = ObjectIdGetDatum(relationOid);	/* starelid */
	values[Anum_pg_statistic_staattnum - 1] = Int16GetDatum(attNum);		/* staattnum */
	values[Anum_pg_statistic_stanullfrac - 1] = Float4GetDatum(stats->nullFraction);		/* stanullfrac */
	values[Anum_pg_statistic_stawidth - 1] = Int32GetDatum((int32) stats->avgWidth);	/* stawidth */
	values[Anum_pg_statistic_stadistinct - 1] = Float4GetDatum(stats->ndistinct);		/* stadistinct */


	/* We will always put MCV in slot 1 and histogram in slot 2 */
	
	if (stats->mcv)
	{
		values[Anum_pg_statistic_stakind1 - 1] = Int16GetDatum(STATISTIC_KIND_MCV);
	}
	else 
	{
		values[Anum_pg_statistic_stakind1 - 1] = Int16GetDatum((int2) 0); /* no mcv */
	}
	
	if (stats->hist)
	{
		values[Anum_pg_statistic_stakind2 - 1] = Int16GetDatum(STATISTIC_KIND_HISTOGRAM);
	}
	else
	{
		values[Anum_pg_statistic_stakind2 - 1] = Int16GetDatum((int2) 0);		
	}
	
	/* correlation */
	values[Anum_pg_statistic_stakind3 - 1] = Int16GetDatum((int2) 0); /* we do not compute correlation anymore */
	
	/* an extra slot */
	values[Anum_pg_statistic_stakind4 - 1] = Int16GetDatum((int2) 0);
	
	/* staops .. which correspond to operators. Sigh.. */
	if (stats->mcv)
	{
		values[Anum_pg_statistic_staop1 - 1] = ObjectIdGetDatum(equalityOid);
	}
	else
	{
		values[Anum_pg_statistic_staop1 - 1] = 0;
	}

	if (stats->hist)
	{
		values[Anum_pg_statistic_staop2 - 1] = ObjectIdGetDatum(ltOid);
	}
	else
	{
		values[Anum_pg_statistic_staop2 - 1] = 0;
	}
	
	/* dummy fields */
	values[Anum_pg_statistic_staop3 - 1] = 0;
	values[Anum_pg_statistic_staop4 - 1] = 0;
	
	/* Now working on stanumbers */
	if (stats->freq)
	{
		values[Anum_pg_statistic_stanumbers1 - 1] = PointerGetDatum(stats->freq);
	}
	else
	{
		nulls[Anum_pg_statistic_stanumbers1 - 1] = true;
		values[Anum_pg_statistic_stanumbers1 - 1] = 0;
	}

	/* dummy fields */
	nulls[Anum_pg_statistic_stanumbers2 - 1] = true;
	values[Anum_pg_statistic_stanumbers2 - 1] = 0;
	nulls[Anum_pg_statistic_stanumbers3 - 1] = true;
	values[Anum_pg_statistic_stanumbers3 - 1] = 0;
	nulls[Anum_pg_statistic_stanumbers4 - 1] = true;
	values[Anum_pg_statistic_stanumbers4 - 1] = 0;

	/* Now working on stavalues */
	if (stats->mcv)
	{
		values[Anum_pg_statistic_stavalues1 - 1] = PointerGetDatum(stats->mcv);
	}
	else
	{
		nulls[Anum_pg_statistic_stavalues1 - 1] = true;
		values[Anum_pg_statistic_stavalues1 - 1] = 0;
	}
	
	if (stats->hist)
	{
		values[Anum_pg_statistic_stavalues2 - 1] = PointerGetDatum(stats->hist);
	}
	else
	{
		nulls[Anum_pg_statistic_stavalues2 - 1] = true;
		values[Anum_pg_statistic_stavalues2 - 1] = 0;
	}
	
	/* dummy values */
	nulls[Anum_pg_statistic_stavalues3 - 1] = true;
	values[Anum_pg_statistic_stavalues3 - 1] = 0;
	nulls[Anum_pg_statistic_stavalues4 - 1] = true;
	values[Anum_pg_statistic_stavalues4 - 1] = 0;

	/* Now work on pg_statistic */
	{
		HeapTuple oldStatisticsTuple;
		Relation	pgstatisticRel;

		pgstatisticRel = heap_open(StatisticRelationId, RowExclusiveLock);
		oldStatisticsTuple = SearchSysCache(STATRELATT, 
											ObjectIdGetDatum(relationOid),
											Int16GetDatum(attNum),
											0,0);

		if (HeapTupleIsValid(oldStatisticsTuple))
		{
			/* pg_statistic tuple exists. */
			HeapTuple stup = heap_modify_tuple(oldStatisticsTuple,
					RelationGetDescr(pgstatisticRel),
					values,
					nulls,
					replaces);
			ReleaseSysCache(oldStatisticsTuple);
			simple_heap_update(pgstatisticRel, &stup->t_self, stup);
			CatalogUpdateIndexes(pgstatisticRel, stup);
			heap_freetuple(stup);
		}
		else
		{
			/* insert new tuple into pg_statistic. we are guaranteed no-one else
			 * will overwrite this row because of ShareUpdateExclusiveLock on the relation. */
			HeapTuple stup = heap_form_tuple(RelationGetDescr(pgstatisticRel), values, nulls);
			simple_heap_insert(pgstatisticRel, stup);
			CatalogUpdateIndexes(pgstatisticRel, stup);
			heap_freetuple(stup);
		}

		heap_close(pgstatisticRel, RowExclusiveLock);	
	}

}


/**
 * This method estimates the number of tuples and pages in a heaptable relation. Getting the number of blocks is straightforward.
 * Estimating the number of tuples is a little trickier. There are two factors that complicate this:
 * 	1. Tuples may be of variable length.
 * 	2. There may be dead tuples lying around.
 * To do this, it chooses a certain number of blocks (as determined by a guc) randomly. The process of choosing is not strictly
 * uniformly random since we have a target number of blocks in mind. We start processing blocks in order and choose an block 
 * with a probability p determined by the ratio of target to total blocks. It is possible that we get really unlucky and reject
 * a large number of blocks up front. We compensate for this by increasing p dynamically. Thus, we are guaranteed to choose the target number
 * of blocks. We read all heaptuples from these blocks and keep count of number of live tuples. We scale up this count to
 * estimate reltuples. Relpages is an exact value.
 * 
 * Input:
 * 	rel - Relation. Must be a heaptable. 
 * 
 * Output:
 * 	reltuples - estimated number of tuples in relation.
 * 	relpages  - exact number of pages.
 */
static void gp_statistics_estimate_reltuples_relpages_heap(Relation rel, float4 *reltuples, float4 *relpages)
{
	MIRROREDLOCK_BUFMGR_DECLARE;

	float4		nrowsseen = 0;	/* # rows seen */
	float4		nrowsdead = 0;	/* # rows seen */
	BlockNumber nblockstotal = 0;	/* nblocks in relation */
	BlockNumber nblockstarget = (BlockNumber) gp_statistics_blocks_target; 
	BlockNumber nblocksseen = 0;
	int			j = 0; /* counter */
	
	/**
	 * Ensure that the right kind of relation with the right kind of storage is passed to us.
	 */
	Assert(rel->rd_rel->relkind == RELKIND_RELATION);
	Assert(RelationIsHeap(rel));
					
	nblockstotal = RelationGetNumberOfBlocks(rel);

	if (nblockstotal == 0 || nblockstarget == 0)
	{		
		/**
		 * If there are no blocks, there cannot be tuples.
		 */
		*reltuples = 0.0;
		*relpages = 0.0;
		return; 
	}
		
	for (j=0 ; j<nblockstotal; j++)
	{
		/**
		 * Threshold is dynamically adjusted based on how many blocks we need to examine and how many blocks
		 * are left.
		 */
		double threshold = ((double) nblockstarget - nblocksseen)/((double) nblockstotal - j);
		
		/**
		 * Random dice thrown to determine if current block is chosen.
		 */
		double diceValue = ((double) random()) / ((double) MAX_RANDOM_VALUE);
		
		if (threshold >= 1.0 || diceValue <= threshold)
		{
			/**
			 * Block j shall be examined!
			 */
			BlockNumber targblock = j;
			Buffer		targbuffer;
			Page		targpage;
			OffsetNumber targoffset,
						maxoffset;

			/**
			 * Check for cancellations.
			 */
			CHECK_FOR_INTERRUPTS();

			/*
			 * We must maintain a pin on the target page's buffer to ensure that
			 * the maxoffset value stays good (else concurrent VACUUM might delete
			 * tuples out from under us).  Hence, pin the page until we are done
			 * looking at it.  We don't maintain a lock on the page, so tuples
			 * could get added to it, but we ignore such tuples.
			 */

			// -------- MirroredLock ----------
			MIRROREDLOCK_BUFMGR_LOCK;

			targbuffer = ReadBuffer(rel, targblock);
			LockBuffer(targbuffer, BUFFER_LOCK_SHARE);
			targpage = BufferGetPage(targbuffer);
			maxoffset = PageGetMaxOffsetNumber(targpage);

			/* Inner loop over all tuples on the selected block. */
			for (targoffset = FirstOffsetNumber; targoffset <= maxoffset; targoffset++)
			{
				ItemId itemid;
				itemid = PageGetItemId(targpage, targoffset);
				nrowsseen++;
				if(!ItemIdIsNormal(itemid))
				{
					nrowsdead += 1;
				}
				else
				{
					HeapTupleData targtuple;
					ItemPointerSet(&targtuple.t_self, targblock, targoffset);
					targtuple.t_data = (HeapTupleHeader) PageGetItem(targpage, itemid);
					targtuple.t_len = ItemIdGetLength(itemid);

					if(!HeapTupleSatisfiesVisibility(rel, &targtuple, SnapshotNow, targbuffer))
					{
						nrowsdead += 1;
					}
				}
			}

			/* Now release the pin on the page */
			UnlockReleaseBuffer(targbuffer);

			MIRROREDLOCK_BUFMGR_UNLOCK;
			// -------- MirroredLock ----------

			nblocksseen++;
		}		
	}

	Assert(nblocksseen > 0);
	/**
	 * To calculate reltuples, scale up the number of live rows per block seen to the total number
	 * of blocks. 
	 */
	*reltuples = ceil((nrowsseen - nrowsdead) * nblockstotal / nblocksseen);
	*relpages = nblockstotal;
	return;
}

/**
 * This method estimates the number of tuples and pages in an vertical oriented append-only relation. 
 * Require access to segment catalogs to determine reltuples.
 * Relpages is obtained by fudging AO block sizes.
 * 
 * Input:
 * 	rel - Relation. Must be an AO CS table.
 * 
 * Output:
 * 	reltuples - estimated number of tuples in relation.
 * 	relpages  - exact number of pages.
 */

static void gp_statistics_estimate_reltuples_relpages_ao_cs(Relation rel, float4 *reltuples, float4 *relpages)
{
    AOCSFileSegInfo	**aocsInfo = NULL;
    int				nsegs = 0;
    double			totalBytes = 0;
	AppendOnlyEntry *aoEntry;
	
	/*
	 * TODO, in gpsql
	 */
	Insist(!"not implemented");

	/**
	 * Ensure that the right kind of relation with the right type of storage is passed to us.
	 */
	Assert(rel->rd_rel->relkind == RELKIND_RELATION);
	Assert(RelationIsAoCols(rel));
	
	*reltuples = 0.0;
	*relpages = 0.0;
	
    /* get table level statistics from the pg_aoseg table */
	aoEntry = GetAppendOnlyEntry(RelationGetRelid(rel), SnapshotNow);
    aocsInfo = GetAllAOCSFileSegInfo(rel, aoEntry, SnapshotNow, &nsegs);
    if (aocsInfo)
    {
    	int i = 0;
    	int j = 0;
    	for(i = 0; i < nsegs; i++)
    	{
    		for(j = 0; j < RelationGetNumberOfAttributes(rel); j++)
    		{
    			AOCSVPInfoEntry *e = getAOCSVPEntry(aocsInfo[i], j);
    			Assert(e);
    			totalBytes += e->eof_uncompressed;
    		}

    		*reltuples += aocsInfo[i]->tupcount;
    	}
    	/**
    	 * The planner doesn't understand AO's blocks, so need this method to try to fudge up a number for
    	 * the planner. 
    	 */
    	*relpages = RelationGuessNumberOfBlocks(totalBytes);
    }

	pfree(aoEntry);
	  
	return;
}
/**
 * This method estimates the number of tuples and pages in an append-only relation. AO tables maintain accurate
 * tuple counts in the catalog. Therefore, we will only require access to segment catalogs to determine reltuples.
 * Relpages is obtained by fudging AO block sizes.
 * 
 * Input:
 * 	rel - Relation. Must be an AO table.
 * 
 * Output:
 * 	reltuples - estimated number of tuples in relation.
 * 	relpages  - exact number of pages.
 */

static void gp_statistics_estimate_reltuples_relpages_ao_rows(Relation rel, float4 *reltuples, float4 *relpages)
{
	int i;
	FileSegTotals		*fstotal;
	
	/**
	 * Ensure that the right kind of relation with the right type of storage is passed to us.
	 */
	Assert(rel->rd_rel->relkind == RELKIND_RELATION);
	Assert(RelationIsAoRows(rel));
	
	Assert(Gp_role == GP_ROLE_DISPATCH);

	*relpages = 0.0;
	*reltuples = 0.0;

	for (i = 0 ; i < GpIdentity.numsegments ; ++i)
	{
		fstotal = GetSegFilesTotals(rel, SnapshotNow, i);
		Assert(fstotal);
		/**
		 * The planner doesn't understand AO's blocks, so need this method to try to fudge up a number for
		 * the planner.
		 */
		*relpages += RelationGuessNumberOfBlocks((double) fstotal->totalbytes);
		/**
		 * The number of tuples in AO table is known accurately. Therefore, we just utilize this value.
		 */
		*reltuples += (double) fstotal->totaltuples;

		pfree(fstotal);
	}
	
	return;
}


