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
 * copyfuncs.c
 *	  Copy functions for Postgres tree nodes.
 *
 * NOTE: we currently support copying all node types found in parse and
 * plan trees.	We do not support copying executor state trees; there
 * is no need for that, and no point in maintaining all the code that
 * would be needed.  We also do not support copying Path trees, mainly
 * because the circular linkages between RelOptInfo and Path nodes can't
 * be handled easily in a simple depth-first traversal.
 *
 *
 * Portions Copyright (c) 2005-2010, Greenplum inc
 * Portions Copyright (c) 1996-2008, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  $PostgreSQL: pgsql/src/backend/nodes/copyfuncs.c,v 1.353.2.2 2007/08/31 01:44:14 tgl Exp $
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "access/attnum.h"
#include "access/filesplit.h"
#include "catalog/caqlparse.h"
#include "catalog/gp_policy.h"
#include "nodes/plannodes.h"
#include "nodes/execnodes.h" /* CdbProcess, Slice, and SliceTable. */
#include "nodes/relation.h"
#include "utils/datum.h"
#include "cdb/cdbgang.h"
#include "cdb/cdbdatalocality.h"
#include "nodes/nodeFuncs.h"

/*
 * Macros to simplify copying of different kinds of fields.  Use these
 * wherever possible to reduce the chance for silly typos.	Note that these
 * hard-wire the convention that the local variables in a Copy routine are
 * named 'newnode' and 'from'.
 */

/* Copy a simple scalar field (int, float, bool, enum, etc) */
#define COPY_SCALAR_FIELD(fldname) \
	(newnode->fldname = from->fldname)

/* Copy a field that is a pointer to some kind of Node or Node tree */
#define COPY_NODE_FIELD(fldname) \
	(newnode->fldname = copyObject(from->fldname))

/* Copy a field that is a pointer to a Bitmapset */
#define COPY_BITMAPSET_FIELD(fldname) \
	(newnode->fldname = bms_copy(from->fldname))

/* Copy a field that is a pointer to a C string, or perhaps NULL */
#define COPY_STRING_FIELD(fldname) \
	(newnode->fldname = from->fldname ? pstrdup(from->fldname) : (char *) NULL)

/* Copy a field that is a pointer to a simple palloc'd object of size sz */
#define COPY_POINTER_FIELD(fldname, sz) \
	do { \
		Size	_size = (sz); \
		newnode->fldname = palloc(_size); \
		memcpy(newnode->fldname, from->fldname, _size); \
	} while (0)

#define COPY_BINARY_FIELD(fldname, sz) \
	do { \
		Size _size = (sz); \
		memcpy(&newnode->fldname, &from->fldname, _size); \
	} while (0)

/* Copy a field that is a varlena datum */
#define COPY_VARLENA_FIELD(fldname, len) \
	do { \
		if (from->fldname) \
		{ \
			newnode->fldname = DatumGetPointer( \
					datumCopy(PointerGetDatum(from->fldname), false, len)); \
		} \
	} while (0)

/* Copy a parse location field (for Copy, this is same as scalar case) */
#define COPY_LOCATION_FIELD(fldname) \
	(newnode->fldname = from->fldname)


/* ****************************************************************
 *					 plannodes.h copy functions
 * ****************************************************************
 */

/*
 * CopyPlanFields
 *
 *		This function copies the fields of the Plan node.  It is used by
 *		all the copy functions for classes which inherit from Plan.
 */
static void
CopyPlanFields(Plan *from, Plan *newnode)
{

	COPY_SCALAR_FIELD(plan_node_id);
	COPY_SCALAR_FIELD(plan_parent_node_id);

	COPY_SCALAR_FIELD(startup_cost);
	COPY_SCALAR_FIELD(total_cost);
	COPY_SCALAR_FIELD(plan_rows);
	COPY_SCALAR_FIELD(plan_width);
	COPY_NODE_FIELD(targetlist);
	COPY_NODE_FIELD(qual);
	COPY_NODE_FIELD(lefttree);
	COPY_NODE_FIELD(righttree);
	COPY_NODE_FIELD(initPlan);
	COPY_BITMAPSET_FIELD(extParam);
	COPY_BITMAPSET_FIELD(allParam);
	COPY_SCALAR_FIELD(nParamExec);
	COPY_NODE_FIELD(flow);
	COPY_SCALAR_FIELD(dispatch);
	COPY_SCALAR_FIELD(nMotionNodes);
	COPY_SCALAR_FIELD(nInitPlans);
	COPY_NODE_FIELD(sliceTable);

	COPY_SCALAR_FIELD(directDispatch.isDirectDispatch);
	COPY_NODE_FIELD(directDispatch.contentIds);
	COPY_SCALAR_FIELD(operatorMemKB);

}

/*
 * CopyLogicalIndexInfo
 *
 *		This function copies the LogicalIndexInfo, which is part of
 *		DynamicIndexScan node.
 */
void
CopyLogicalIndexInfo(const LogicalIndexInfo *from, LogicalIndexInfo *newnode)
{
	COPY_SCALAR_FIELD(logicalIndexOid);
	COPY_SCALAR_FIELD(nColumns);
	COPY_POINTER_FIELD(indexKeys, from->nColumns * sizeof(AttrNumber));
	COPY_NODE_FIELD(indPred);
	COPY_NODE_FIELD(indExprs);
	COPY_SCALAR_FIELD(indIsUnique);
	COPY_SCALAR_FIELD(indType);
	COPY_NODE_FIELD(partCons);
	COPY_NODE_FIELD(defaultLevels);
}

/*
 * _copyPlannedStmt 
 */
static PlannedStmt *
_copyPlannedStmt(PlannedStmt *from)
{
	PlannedStmt   *newnode = makeNode(PlannedStmt);

	COPY_SCALAR_FIELD(commandType);
	COPY_SCALAR_FIELD(planGen);
	COPY_SCALAR_FIELD(canSetTag);
	COPY_SCALAR_FIELD(transientPlan);

	COPY_NODE_FIELD(planTree);
	COPY_NODE_FIELD(rtable);
	
	COPY_NODE_FIELD(resultRelations);
	COPY_NODE_FIELD(utilityStmt);
	COPY_NODE_FIELD(intoClause);
	COPY_NODE_FIELD(subplans);
	COPY_NODE_FIELD(rewindPlanIDs);
	COPY_NODE_FIELD(returningLists);
	
	COPY_NODE_FIELD(result_partitions);
	COPY_NODE_FIELD(result_aosegnos);
	COPY_NODE_FIELD(result_segfileinfos);
	COPY_NODE_FIELD(scantable_splits);
	COPY_NODE_FIELD(into_aosegnos);
	COPY_NODE_FIELD(queryPartOids);
	COPY_NODE_FIELD(queryPartsMetadata);
	COPY_NODE_FIELD(numSelectorsPerScanId);
	COPY_NODE_FIELD(rowMarks);
	COPY_NODE_FIELD(relationOids);
	COPY_SCALAR_FIELD(invalItems);
	COPY_SCALAR_FIELD(nCrossLevelParams);
	COPY_SCALAR_FIELD(nMotionNodes);
	COPY_SCALAR_FIELD(nInitPlans);
	
	if (from->intoPolicy)
	{
		COPY_POINTER_FIELD(intoPolicy,sizeof(GpPolicy) + from->intoPolicy->nattrs*sizeof(from->intoPolicy->attrs[0]));
	}
	else
		newnode->intoPolicy = NULL;
	COPY_NODE_FIELD(sliceTable);

	COPY_SCALAR_FIELD(backoff_weight);
	COPY_SCALAR_FIELD(query_mem);

	COPY_SCALAR_FIELD(resource); // ?? What does this mean?
	COPY_SCALAR_FIELD(planner_segments);

	return newnode;
}

/*
 * _copyPlan
 */
static Plan *
_copyPlan(Plan *from)
{
	Plan	   *newnode = makeNode(Plan);

	/*
	 * copy node superclass fields
	 */
	CopyPlanFields(from, newnode);

	return newnode;
}


/*
 * _copyResult
 */
static Result *
_copyResult(Result *from)
{
	Result	   *newnode = makeNode(Result);

	/*
	 * copy node superclass fields
	 */
	CopyPlanFields((Plan *) from, (Plan *) newnode);

	/*
	 * copy remainder of node
	 */
	COPY_NODE_FIELD(resconstantqual);

	COPY_SCALAR_FIELD(hashFilter);
	COPY_NODE_FIELD(hashList);

	return newnode;
}

/*
 * _copyRepeat
 */
static Repeat *
_copyRepeat(Repeat *from)
{
	Repeat *newnode = makeNode(Repeat);

	/*
	 * copy node superclass fields
	 */
	CopyPlanFields((Plan *)from, (Plan *)newnode);

	/*
	 * copy remainder of node
	 */
	COPY_NODE_FIELD(repeatCountExpr);
	COPY_SCALAR_FIELD(grouping);

	return newnode;
}

/*
 * _copyAppend
 */
static Append *
_copyAppend(Append *from)
{
	Append	   *newnode = makeNode(Append);

	/*
	 * copy node superclass fields
	 */
	CopyPlanFields((Plan *) from, (Plan *) newnode);

	/*
	 * copy remainder of node
	 */
	COPY_NODE_FIELD(appendplans);
	COPY_SCALAR_FIELD(isTarget);
	COPY_SCALAR_FIELD(isZapped);
	COPY_SCALAR_FIELD(hasXslice);

	return newnode;
}

static Sequence *
_copySequence(Sequence *from)
{
	Sequence *newnode = makeNode(Sequence);
	CopyPlanFields((Plan *) from, (Plan *) newnode);
	COPY_NODE_FIELD(subplans);

	return newnode;
}

/*
 * _copyBitmapAnd
 */
static BitmapAnd *
_copyBitmapAnd(BitmapAnd *from)
{
	BitmapAnd  *newnode = makeNode(BitmapAnd);

	/*
	 * copy node superclass fields
	 */
	CopyPlanFields((Plan *) from, (Plan *) newnode);

	/*
	 * copy remainder of node
	 */
	COPY_NODE_FIELD(bitmapplans);

	return newnode;
}

/*
 * _copyBitmapOr
 */
static BitmapOr *
_copyBitmapOr(BitmapOr *from)
{
	BitmapOr   *newnode = makeNode(BitmapOr);

	/*
	 * copy node superclass fields
	 */
	CopyPlanFields((Plan *) from, (Plan *) newnode);

	/*
	 * copy remainder of node
	 */
	COPY_NODE_FIELD(bitmapplans);

	return newnode;
}


/*
 * CopyScanFields
 *
 *		This function copies the fields of the Scan node.  It is used by
 *		all the copy functions for classes which inherit from Scan.
 */
static void
CopyScanFields(Scan *from, Scan *newnode)
{
	CopyPlanFields((Plan *) from, (Plan *) newnode);

	COPY_SCALAR_FIELD(scanrelid);
	COPY_SCALAR_FIELD(partIndex);
	COPY_SCALAR_FIELD(partIndexPrintable);
}

/*
 * _copyScan
 */
static Scan *
_copyScan(Scan *from)
{
	Scan	   *newnode = makeNode(Scan);

	/*
	 * copy node superclass fields
	 */
	CopyScanFields((Scan *) from, (Scan *) newnode);

	return newnode;
}

/*
 * _copySeqScan
 */
static SeqScan *
_copySeqScan(SeqScan *from)
{
	SeqScan    *newnode = makeNode(SeqScan);

	/*
	 * copy node superclass fields
	 */
	CopyScanFields((Scan *) from, (Scan *) newnode);

	return newnode;
}

/*
 * _copyAppendOnlyScan
 */
static AppendOnlyScan *
_copyAppendOnlyScan(AppendOnlyScan *from)
{
	AppendOnlyScan    *newnode = makeNode(AppendOnlyScan);

	/*
	 * copy node superclass fields
	 */
	CopyScanFields((Scan *) from, (Scan *) newnode);

	return newnode;
}

static TableScan *
_copyTableScan(TableScan *from)
{
	TableScan *newnode = makeNode(TableScan);
	CopyScanFields((Scan *) from, (Scan *) newnode);

	return newnode;
}

static DynamicTableScan *
_copyDynamicTableScan(DynamicTableScan *from)
{
	DynamicTableScan *newnode = makeNode(DynamicTableScan);
	CopyScanFields((Scan *) from, (Scan *) newnode);
	COPY_SCALAR_FIELD(partIndex);
	COPY_SCALAR_FIELD(partIndexPrintable);

	return newnode;
}

/*
 * _copyParquetScan
 */
static ParquetScan *
_copyParquetScan(ParquetScan *from)
{
	ParquetScan    *newnode = makeNode(ParquetScan);

	/*
	 * copy node superclass fields
	 */
	CopyScanFields((Scan *) from, (Scan *) newnode);

	return newnode;
}

/*
 * _copyExternalScan
 */
static ExternalScan *
_copyExternalScan(ExternalScan *from)
{
	ExternalScan    *newnode = makeNode(ExternalScan);

	/*
	 * copy node superclass fields
	 */
	CopyScanFields((Scan *) from, (Scan *) newnode);

	/*
	 * copy remainder of node
	 */
	COPY_NODE_FIELD(uriList);
	COPY_NODE_FIELD(fmtOpts);
	COPY_SCALAR_FIELD(fmtType);
	COPY_SCALAR_FIELD(isMasterOnly);
	COPY_SCALAR_FIELD(rejLimit);
	COPY_SCALAR_FIELD(rejLimitInRows);
	COPY_SCALAR_FIELD(fmterrtbl);
	COPY_NODE_FIELD(errAosegnos);
	COPY_NODE_FIELD(err_aosegfileinfos);
	COPY_SCALAR_FIELD(encoding);
	COPY_SCALAR_FIELD(scancounter);

	return newnode;
}

static void
copyIndexScanFields(const IndexScan *from, IndexScan *newnode)
{
	CopyScanFields((Scan *) from, (Scan *) newnode);

	COPY_SCALAR_FIELD(indexid);
	COPY_NODE_FIELD(indexqual);
	COPY_NODE_FIELD(indexqualorig);
	COPY_NODE_FIELD(indexstrategy);
	COPY_NODE_FIELD(indexsubtype);
	COPY_SCALAR_FIELD(indexorderdir);

	/*
	 * If we don't have a valid partIndex, we also don't have
	 * a valid logicalIndexInfo (it should be set to NULL). So,
	 * we don't copy.
	 */
	if (isDynamicScan(&from->scan))
	{
		Assert(NULL != ((IndexScan *) from)->logicalIndexInfo);
		Assert(NULL == newnode->logicalIndexInfo);
		newnode->logicalIndexInfo = palloc(sizeof(LogicalIndexInfo));

		CopyLogicalIndexInfo(((IndexScan *) from)->logicalIndexInfo, newnode->logicalIndexInfo);
	}
	else
	{
		Assert(newnode->logicalIndexInfo == NULL);
	}
}

/*
 * _copyIndexScan
 */
static IndexScan *
_copyIndexScan(IndexScan *from)
{
	IndexScan  *newnode = makeNode(IndexScan);

	copyIndexScanFields(from, newnode);

	return newnode;
}

/*
 * _copyDynamicIndexScan
 */
static DynamicIndexScan *
_copyDynamicIndexScan(const DynamicIndexScan *from)
{
	DynamicIndexScan  *newnode = makeNode(DynamicIndexScan);

	/* DynamicIndexScan has some content from IndexScan */
	copyIndexScanFields((IndexScan *)from, (IndexScan *)newnode);

	return newnode;
}

/*
 * _copyBitmapIndexScan
 */
static BitmapIndexScan *
_copyBitmapIndexScan(BitmapIndexScan *from)
{
	BitmapIndexScan *newnode = makeNode(BitmapIndexScan);

	/* DynamicIndexScan has some content from IndexScan */
	copyIndexScanFields((IndexScan *)from, (IndexScan *)newnode);

	return newnode;
}

/*
 * _copyBitmapHeapScan
 */
static BitmapHeapScan *
_copyBitmapHeapScan(BitmapHeapScan *from)
{
	BitmapHeapScan *newnode = makeNode(BitmapHeapScan);

	/*
	 * copy node superclass fields
	 */
	CopyScanFields((Scan *) from, (Scan *) newnode);

	/*
	 * copy remainder of node
	 */
	COPY_NODE_FIELD(bitmapqualorig);

	return newnode;
}

/*
 * _copyBitmapTableScan
 */
static BitmapTableScan *
_copyBitmapTableScan(BitmapTableScan *from)
{
	BitmapTableScan *newnode = makeNode(BitmapTableScan);

	/*
	 * copy node superclass fields
	 */
	CopyScanFields((Scan *) from, (Scan *) newnode);

	/*
	 * copy remainder of node
	 */
	COPY_NODE_FIELD(bitmapqualorig);

	return newnode;
}

/*
 * _copyTidScan
 */
static TidScan *
_copyTidScan(TidScan *from)
{
	TidScan    *newnode = makeNode(TidScan);

	/*
	 * copy node superclass fields
	 */
	CopyScanFields((Scan *) from, (Scan *) newnode);

	/*
	 * copy remainder of node
	 */
	COPY_NODE_FIELD(tidquals);

	return newnode;
}

/*
 * _copySubqueryScan
 */
static SubqueryScan *
_copySubqueryScan(SubqueryScan *from)
{
	SubqueryScan *newnode = makeNode(SubqueryScan);

	/*
	 * copy node superclass fields
	 */
	CopyScanFields((Scan *) from, (Scan *) newnode);

	/*
	 * copy remainder of node
	 */
	COPY_NODE_FIELD(subplan);

	COPY_NODE_FIELD(subrtable);
	
	return newnode;
}

/*
 * _copyFunctionScan
 */
static FunctionScan *
_copyFunctionScan(FunctionScan *from)
{
	FunctionScan *newnode = makeNode(FunctionScan);

	/*
	 * copy node superclass fields
	 */
	CopyScanFields((Scan *) from, (Scan *) newnode);

	return newnode;
}

/*
 * _copyValuesScan
 */
static ValuesScan *
_copyValuesScan(ValuesScan *from)
{
	ValuesScan *newnode = makeNode(ValuesScan);

	/*
	 * copy node superclass fields
	 */
	CopyScanFields((Scan *) from, (Scan *) newnode);

	return newnode;
}

/*
 * CopyJoinFields
 *
 *		This function copies the fields of the Join node.  It is used by
 *		all the copy functions for classes which inherit from Join.
 */
static void
CopyJoinFields(Join *from, Join *newnode)
{
	CopyPlanFields((Plan *) from, (Plan *) newnode);

    COPY_SCALAR_FIELD(prefetch_inner);

	COPY_SCALAR_FIELD(jointype);
	COPY_NODE_FIELD(joinqual);
}


/*
 * _copyJoin
 */
static Join *
_copyJoin(Join *from)
{
	Join	   *newnode = makeNode(Join);

	/*
	 * copy node superclass fields
	 */
	CopyJoinFields(from, newnode);

	return newnode;
}


/*
 * _copyNestLoop
 */
static NestLoop *
_copyNestLoop(NestLoop *from)
{
	NestLoop   *newnode = makeNode(NestLoop);

	/*
	 * copy node superclass fields
	 */
	CopyJoinFields((Join *) from, (Join *) newnode);

    COPY_SCALAR_FIELD(outernotreferencedbyinner);   /*CDB*/
    COPY_SCALAR_FIELD(shared_outer);
    COPY_SCALAR_FIELD(singleton_outer); /*CDB-OLAP*/

    return newnode;
}

/*
 * _copyMergeJoin
 */
static MergeJoin *
_copyMergeJoin(MergeJoin *from)
{
	MergeJoin  *newnode = makeNode(MergeJoin);

	/*
	 * copy node superclass fields
	 */
	CopyJoinFields((Join *) from, (Join *) newnode);

	/*
	 * copy remainder of node
	 */
	COPY_NODE_FIELD(mergeclauses);
	COPY_SCALAR_FIELD(unique_outer);

	return newnode;
}

/*
 * _copyHashJoin
 */
static HashJoin *
_copyHashJoin(HashJoin *from)
{
	HashJoin   *newnode = makeNode(HashJoin);

	/*
	 * copy node superclass fields
	 */
	CopyJoinFields((Join *) from, (Join *) newnode);

	/*
	 * copy remainder of node
	 */
	COPY_NODE_FIELD(hashclauses);
	COPY_NODE_FIELD(hashqualclauses);

	return newnode;
}

/*
 * _copyShareInputScan
 */
static ShareInputScan *
_copyShareInputScan(ShareInputScan *from)
{
	ShareInputScan *newnode = makeNode(ShareInputScan);

	/* copy node superclass fields */
	CopyPlanFields((Plan *) from, (Plan *) newnode);
	COPY_SCALAR_FIELD(share_type);
	COPY_SCALAR_FIELD(share_id);
	COPY_SCALAR_FIELD(driver_slice);
	return newnode;
}


/*
 * _copyMaterial
 */
static Material *
_copyMaterial(Material *from)
{
	Material   *newnode = makeNode(Material);

	/*
	 * copy node superclass fields
	 */
	CopyPlanFields((Plan *) from, (Plan *) newnode);
    COPY_SCALAR_FIELD(cdb_strict);
	COPY_SCALAR_FIELD(share_type);
	COPY_SCALAR_FIELD(share_id);
	COPY_SCALAR_FIELD(driver_slice);
	COPY_SCALAR_FIELD(nsharer);
	COPY_SCALAR_FIELD(nsharer_xslice);

    return newnode;
}


/*
 * _copySort
 */
static Sort *
_copySort(Sort *from)
{
	Sort	   *newnode = makeNode(Sort);

	/*
	 * copy node superclass fields
	 */
	CopyPlanFields((Plan *) from, (Plan *) newnode);

	COPY_SCALAR_FIELD(numCols);
	COPY_POINTER_FIELD(sortColIdx, from->numCols * sizeof(AttrNumber));
	COPY_POINTER_FIELD(sortOperators, from->numCols * sizeof(Oid));

    /* CDB */
	COPY_NODE_FIELD(limitOffset);
	COPY_NODE_FIELD(limitCount);
	COPY_SCALAR_FIELD(noduplicates);

	COPY_SCALAR_FIELD(share_type);
	COPY_SCALAR_FIELD(share_id);
	COPY_SCALAR_FIELD(driver_slice);
	COPY_SCALAR_FIELD(nsharer);
	COPY_SCALAR_FIELD(nsharer_xslice);

	return newnode;
}

/*
 * _copyAgg
 */
static Agg *
_copyAgg(Agg *from)
{
	Agg		   *newnode = makeNode(Agg);

	CopyPlanFields((Plan *) from, (Plan *) newnode);

	COPY_SCALAR_FIELD(aggstrategy);
	COPY_SCALAR_FIELD(numCols);
	if (from->numCols > 0)
		COPY_POINTER_FIELD(grpColIdx, from->numCols * sizeof(AttrNumber));
	COPY_SCALAR_FIELD(numGroups);
	COPY_SCALAR_FIELD(transSpace);
	COPY_SCALAR_FIELD(numNullCols);
	COPY_SCALAR_FIELD(inputGrouping);
	COPY_SCALAR_FIELD(grouping);
	COPY_SCALAR_FIELD(inputHasGrouping);
	COPY_SCALAR_FIELD(rollupGSTimes);
	COPY_SCALAR_FIELD(lastAgg);
	COPY_SCALAR_FIELD(streaming);

	return newnode;
}

/*
 * _copyWindowKey
 */
static WindowKey *
_copyWindowKey(WindowKey *from)
{
	WindowKey   *newnode = makeNode(WindowKey);

	COPY_SCALAR_FIELD(numSortCols);
	if (from->numSortCols > 0)
	{
		COPY_POINTER_FIELD(sortColIdx, from->numSortCols * sizeof(AttrNumber));
		COPY_POINTER_FIELD(sortOperators, from->numSortCols * sizeof(Oid));
	}
	COPY_NODE_FIELD(frame);

	return newnode;
}

/*
 * _copyWindow
 */
static Window *
_copyWindow(Window *from)
{
	Window	*newnode = makeNode(Window);

	CopyPlanFields((Plan *) from, (Plan *) newnode);

	COPY_SCALAR_FIELD(numPartCols);
	if (from->numPartCols > 0)
		COPY_POINTER_FIELD(partColIdx, from->numPartCols * sizeof(AttrNumber));
	COPY_NODE_FIELD(windowKeys);

	return newnode;
}

/*
 * _copyTableFunctionScan
 */
static TableFunctionScan *
_copyTableFunctionScan(TableFunctionScan *from)
{
	TableFunctionScan	*newnode = makeNode(TableFunctionScan);

	CopyScanFields((Scan *) from, (Scan *) newnode);

	return newnode;
}


/*
 * _copyUnique
 */
static Unique *
_copyUnique(Unique *from)
{
	Unique	   *newnode = makeNode(Unique);

	/*
	 * copy node superclass fields
	 */
	CopyPlanFields((Plan *) from, (Plan *) newnode);

	/*
	 * copy remainder of node
	 */
	COPY_SCALAR_FIELD(numCols);
	COPY_POINTER_FIELD(uniqColIdx, from->numCols * sizeof(AttrNumber));

	return newnode;
}

/*
 * _copyHash
 */
static Hash *
_copyHash(Hash *from)
{
	Hash	   *newnode = makeNode(Hash);

	/*
	 * copy node superclass fields
	 */
	CopyPlanFields((Plan *) from, (Plan *) newnode);

	/*
	 * copy remainder of node
	 */

	return newnode;
}

/*
 * _copySetOp
 */
static SetOp *
_copySetOp(SetOp *from)
{
	SetOp	   *newnode = makeNode(SetOp);

	/*
	 * copy node superclass fields
	 */
	CopyPlanFields((Plan *) from, (Plan *) newnode);

	/*
	 * copy remainder of node
	 */
	COPY_SCALAR_FIELD(cmd);
	COPY_SCALAR_FIELD(numCols);
	COPY_POINTER_FIELD(dupColIdx, from->numCols * sizeof(AttrNumber));
	COPY_SCALAR_FIELD(flagColIdx);

	return newnode;
}

/*
 * _copyLimit
 */
static Limit *
_copyLimit(Limit *from)
{
	Limit	   *newnode = makeNode(Limit);

	/*
	 * copy node superclass fields
	 */
	CopyPlanFields((Plan *) from, (Plan *) newnode);

	/*
	 * copy remainder of node
	 */
	COPY_NODE_FIELD(limitOffset);
	COPY_NODE_FIELD(limitCount);

	return newnode;
}

/*
 * _copyMotion
 */
static Motion *
_copyMotion(Motion *from)
{
	Motion	   *newnode = makeNode(Motion);

	/*
	 * copy node superclass fields
	 */
	CopyPlanFields((Plan *) from, (Plan *) newnode);

	COPY_SCALAR_FIELD(sendSorted);
	COPY_SCALAR_FIELD(motionID);

	COPY_SCALAR_FIELD(motionType);

	COPY_NODE_FIELD(hashExpr);
	COPY_NODE_FIELD(hashDataTypes);

	COPY_SCALAR_FIELD(numOutputSegs);
	COPY_POINTER_FIELD(outputSegIdx, from->numOutputSegs * sizeof(int));

	COPY_SCALAR_FIELD(numSortCols);
	COPY_POINTER_FIELD(sortColIdx, from->numSortCols * sizeof(AttrNumber));
	COPY_POINTER_FIELD(sortOperators, from->numSortCols * sizeof(Oid));
	
	COPY_SCALAR_FIELD(segidColIdx);
	
	return newnode;
}

/*
 * _copyDML
 */
static DML *
_copyDML(const DML *from)
{
	DML *newnode = makeNode(DML);

	/*
	 * copy node superclass fields
	 */
	CopyPlanFields((Plan *) from, (Plan *) newnode);

	COPY_SCALAR_FIELD(scanrelid);
	COPY_SCALAR_FIELD(oidColIdx);
	COPY_SCALAR_FIELD(actionColIdx);
	COPY_SCALAR_FIELD(ctidColIdx);
	COPY_SCALAR_FIELD(tupleoidColIdx);

	return newnode;
}

/*
 * _copySplitUpdate
 */
static SplitUpdate *
_copySplitUpdate(const SplitUpdate *from)
{
	SplitUpdate *newnode = makeNode(SplitUpdate);

	/*
	 * copy node superclass fields
	 */
	CopyPlanFields((Plan *) from, (Plan *) newnode);

	COPY_SCALAR_FIELD(actionColIdx);
	COPY_SCALAR_FIELD(ctidColIdx);
	COPY_SCALAR_FIELD(tupleoidColIdx);
	COPY_NODE_FIELD(insertColIdx);
	COPY_NODE_FIELD(deleteColIdx);

	return newnode;
}

/*
 * _copyRowTrigger
 */
static RowTrigger *
_copyRowTrigger(const RowTrigger *from)
{
	RowTrigger *newnode = makeNode(RowTrigger);

	/*
	 * copy node superclass fields
	 */
	CopyPlanFields((Plan *) from, (Plan *) newnode);

	COPY_SCALAR_FIELD(relid);
	COPY_SCALAR_FIELD(eventFlags);
	COPY_NODE_FIELD(oldValuesColIdx);
	COPY_NODE_FIELD(newValuesColIdx);

	return newnode;
}

/*
 * _copyAssertOp
 */
static AssertOp *
_copyAssertOp(const AssertOp *from)
{
	AssertOp *newnode = makeNode(AssertOp);

	/*
	 * copy node superclass fields
	 */
	CopyPlanFields((Plan *) from, (Plan *) newnode);

	COPY_SCALAR_FIELD(errcode);
	COPY_NODE_FIELD(errmessage);
	
	return newnode;
}

/*
 * _copyPartitionSelector
 */
static PartitionSelector *
_copyPartitionSelector(const PartitionSelector *from)
{
	PartitionSelector *newnode = makeNode(PartitionSelector);

	/*
	 * copy node superclass fields
	 */
	CopyPlanFields((Plan *) from, (Plan *) newnode);

	COPY_SCALAR_FIELD(relid);
	COPY_SCALAR_FIELD(nLevels);
	COPY_SCALAR_FIELD(scanId);
	COPY_SCALAR_FIELD(selectorId);
	COPY_NODE_FIELD(levelEqExpressions);
	COPY_NODE_FIELD(levelExpressions);
	COPY_NODE_FIELD(residualPredicate);
	COPY_NODE_FIELD(propagationExpression);
	COPY_NODE_FIELD(printablePredicate);
	COPY_SCALAR_FIELD(staticSelection);
	COPY_NODE_FIELD(staticPartOids);
	COPY_NODE_FIELD(staticScanIds);

	return newnode;
}

/* ****************************************************************
 *					   primnodes.h copy functions
 * ****************************************************************
 */

/*
 * _copyAlias
 */
static Alias *
_copyAlias(Alias *from)
{
	Alias	   *newnode = makeNode(Alias);

	COPY_STRING_FIELD(aliasname);
	COPY_NODE_FIELD(colnames);

	return newnode;
}

/*
 * _copyRangeVar
 */
static RangeVar *
_copyRangeVar(RangeVar *from)
{
	RangeVar   *newnode = makeNode(RangeVar);

	Assert(from->schemaname == NULL || strlen(from->schemaname)>0);
	COPY_STRING_FIELD(catalogname);
	COPY_STRING_FIELD(schemaname);
	COPY_STRING_FIELD(relname);
	COPY_SCALAR_FIELD(inhOpt);
	COPY_SCALAR_FIELD(istemp);
	COPY_NODE_FIELD(alias);
	COPY_SCALAR_FIELD(location);    /*CDB*/

	return newnode;
}

/*
 * _copyIntoClause
 */
static IntoClause *
_copyIntoClause(IntoClause *from)
{
	IntoClause *newnode = makeNode(IntoClause);
	
	COPY_NODE_FIELD(rel);
	COPY_NODE_FIELD(colNames);
	COPY_NODE_FIELD(options);
	COPY_SCALAR_FIELD(onCommit);
	COPY_STRING_FIELD(tableSpaceName);
	COPY_SCALAR_FIELD(oidInfo.relOid);
	COPY_SCALAR_FIELD(oidInfo.comptypeOid);
	COPY_SCALAR_FIELD(oidInfo.toastOid);
	COPY_SCALAR_FIELD(oidInfo.toastIndexOid);
	COPY_SCALAR_FIELD(oidInfo.toastComptypeOid);
	COPY_SCALAR_FIELD(oidInfo.aosegOid);
	COPY_SCALAR_FIELD(oidInfo.aosegIndexOid);
	COPY_SCALAR_FIELD(oidInfo.aosegComptypeOid);
	
	return newnode;
}

/*
 * We don't need a _copyExpr because Expr is an abstract supertype which
 * should never actually get instantiated.	Also, since it has no common
 * fields except NodeTag, there's no need for a helper routine to factor
 * out copying the common fields...
 */

/*
 * _copyVar
 */
static Var *
_copyVar(Var *from)
{
	Var		   *newnode = makeNode(Var);

	COPY_SCALAR_FIELD(varno);
	COPY_SCALAR_FIELD(varattno);
	COPY_SCALAR_FIELD(vartype);
	COPY_SCALAR_FIELD(vartypmod);
	COPY_SCALAR_FIELD(varlevelsup);
	COPY_SCALAR_FIELD(varnoold);
	COPY_SCALAR_FIELD(varoattno);

	return newnode;
}

/*
 * _copyConst
 */
static Const *
_copyConst(Const *from)
{
	Const	   *newnode = makeNode(Const);

	COPY_SCALAR_FIELD(consttype);
	COPY_SCALAR_FIELD(constlen);

	if (from->constbyval || from->constisnull)
	{
		/*
		 * passed by value so just copy the datum. Also, don't try to copy
		 * struct when value is null!
		 */
		newnode->constvalue = from->constvalue;
	}
	else
	{
		/*
		 * passed by reference.  We need a palloc'd copy.
		 */
		newnode->constvalue = datumCopy(from->constvalue,
										from->constbyval,
										from->constlen);
	}

	COPY_SCALAR_FIELD(constisnull);
	COPY_SCALAR_FIELD(constbyval);

	return newnode;
}

/*
 * _copyParam
 */
static Param *
_copyParam(Param *from)
{
	Param	   *newnode = makeNode(Param);

	COPY_SCALAR_FIELD(paramkind);
	COPY_SCALAR_FIELD(paramid);
	COPY_SCALAR_FIELD(paramtype);

	return newnode;
}

/*
 * _copyAggref
 */
static Aggref *
_copyAggref(Aggref *from)
{
	Aggref	   *newnode = makeNode(Aggref);

	COPY_SCALAR_FIELD(aggfnoid);
	COPY_SCALAR_FIELD(aggtype);
	COPY_NODE_FIELD(args);
	COPY_SCALAR_FIELD(agglevelsup);
	COPY_SCALAR_FIELD(aggstar);
	COPY_SCALAR_FIELD(aggdistinct);
	COPY_SCALAR_FIELD(aggstage);
    COPY_NODE_FIELD(aggorder);

	return newnode;
}

/*
 * _copyAggOrder
 */
static AggOrder *
_copyAggOrder(AggOrder *from)
{
	AggOrder   *newnode = makeNode(AggOrder);

    COPY_SCALAR_FIELD(sortImplicit);
	COPY_NODE_FIELD(sortTargets);
    COPY_NODE_FIELD(sortClause);

	return newnode;
}

/*
 * _copyWindowRef
 */
static WindowRef *
_copyWindowRef(WindowRef *from)
{
	WindowRef	   *newnode = makeNode(WindowRef);

	COPY_SCALAR_FIELD(winfnoid);
	COPY_SCALAR_FIELD(restype);
	COPY_NODE_FIELD(args);
	COPY_SCALAR_FIELD(winlevelsup);
	COPY_SCALAR_FIELD(windistinct);
	COPY_SCALAR_FIELD(winspec);
	COPY_SCALAR_FIELD(winindex);
	COPY_SCALAR_FIELD(winstage);
	COPY_SCALAR_FIELD(winlevel);

	return newnode;
}

/*
 * _copyArrayRef
 */
static ArrayRef *
_copyArrayRef(ArrayRef *from)
{
	ArrayRef   *newnode = makeNode(ArrayRef);

	COPY_SCALAR_FIELD(refrestype);
	COPY_SCALAR_FIELD(refarraytype);
	COPY_SCALAR_FIELD(refelemtype);
	COPY_NODE_FIELD(refupperindexpr);
	COPY_NODE_FIELD(reflowerindexpr);
	COPY_NODE_FIELD(refexpr);
	COPY_NODE_FIELD(refassgnexpr);

	return newnode;
}

/*
 * _copyFuncExpr
 */
static FuncExpr *
_copyFuncExpr(FuncExpr *from)
{
	FuncExpr   *newnode = makeNode(FuncExpr);

	COPY_SCALAR_FIELD(funcid);
	COPY_SCALAR_FIELD(funcresulttype);
	COPY_SCALAR_FIELD(funcretset);
	COPY_SCALAR_FIELD(funcformat);
	COPY_NODE_FIELD(args);
	COPY_SCALAR_FIELD(is_tablefunc);

	return newnode;
}

/*
 * _copyOpExpr
 */
static OpExpr *
_copyOpExpr(OpExpr *from)
{
	OpExpr	   *newnode = makeNode(OpExpr);

	COPY_SCALAR_FIELD(opno);
	COPY_SCALAR_FIELD(opfuncid);
	COPY_SCALAR_FIELD(opresulttype);
	COPY_SCALAR_FIELD(opretset);
	COPY_NODE_FIELD(args);

	return newnode;
}

/*
 * _copyDistinctExpr (same as OpExpr)
 */
static DistinctExpr *
_copyDistinctExpr(DistinctExpr *from)
{
	DistinctExpr *newnode = makeNode(DistinctExpr);

	COPY_SCALAR_FIELD(opno);
	COPY_SCALAR_FIELD(opfuncid);
	COPY_SCALAR_FIELD(opresulttype);
	COPY_SCALAR_FIELD(opretset);
	COPY_NODE_FIELD(args);

	return newnode;
}

/*
 * _copyScalarArrayOpExpr
 */
static ScalarArrayOpExpr *
_copyScalarArrayOpExpr(ScalarArrayOpExpr *from)
{
	ScalarArrayOpExpr *newnode = makeNode(ScalarArrayOpExpr);

	COPY_SCALAR_FIELD(opno);
	COPY_SCALAR_FIELD(opfuncid);
	COPY_SCALAR_FIELD(useOr);
	COPY_NODE_FIELD(args);

	return newnode;
}

/*
 * _copyBoolExpr
 */
static BoolExpr *
_copyBoolExpr(BoolExpr *from)
{
	BoolExpr   *newnode = makeNode(BoolExpr);

	COPY_SCALAR_FIELD(boolop);
	COPY_NODE_FIELD(args);

	return newnode;
}

/*
 * _copySubLink
 */
static SubLink *
_copySubLink(SubLink *from)
{
	SubLink    *newnode = makeNode(SubLink);

	COPY_SCALAR_FIELD(subLinkType);
	COPY_NODE_FIELD(testexpr);
	COPY_NODE_FIELD(operName);
	COPY_SCALAR_FIELD(location);    /*CDB*/
	COPY_NODE_FIELD(subselect);

	return newnode;
}

/*
 * _copySubPlan
 */
static SubPlan *
_copySubPlan(SubPlan *from)
{
	SubPlan    *newnode = makeNode(SubPlan);

	COPY_SCALAR_FIELD(subLinkType);
	COPY_SCALAR_FIELD(qDispSliceId);    /*CDB*/
	COPY_NODE_FIELD(testexpr);
	COPY_NODE_FIELD(paramIds);
	COPY_SCALAR_FIELD(plan_id);
	COPY_STRING_FIELD(plan_name);
	COPY_SCALAR_FIELD(firstColType);
	COPY_SCALAR_FIELD(firstColTypmod);
	COPY_SCALAR_FIELD(useHashTable);
	COPY_SCALAR_FIELD(unknownEqFalse);
	COPY_SCALAR_FIELD(is_initplan);	/*CDB*/
	COPY_SCALAR_FIELD(is_multirow);	/*CDB*/
	COPY_NODE_FIELD(setParam);
	COPY_NODE_FIELD(parParam);
	COPY_NODE_FIELD(args);

	return newnode;
}

/*
 * _copyFieldSelect
 */
static FieldSelect *
_copyFieldSelect(FieldSelect *from)
{
	FieldSelect *newnode = makeNode(FieldSelect);

	COPY_NODE_FIELD(arg);
	COPY_SCALAR_FIELD(fieldnum);
	COPY_SCALAR_FIELD(resulttype);
	COPY_SCALAR_FIELD(resulttypmod);

	return newnode;
}

/*
 * _copyFieldStore
 */
static FieldStore *
_copyFieldStore(FieldStore *from)
{
	FieldStore *newnode = makeNode(FieldStore);

	COPY_NODE_FIELD(arg);
	COPY_NODE_FIELD(newvals);
	COPY_NODE_FIELD(fieldnums);
	COPY_SCALAR_FIELD(resulttype);

	return newnode;
}

/*
 * _copyRelabelType
 */
static RelabelType *
_copyRelabelType(RelabelType *from)
{
	RelabelType *newnode = makeNode(RelabelType);

	COPY_NODE_FIELD(arg);
	COPY_SCALAR_FIELD(resulttype);
	COPY_SCALAR_FIELD(resulttypmod);
	COPY_SCALAR_FIELD(relabelformat);

	return newnode;
}

/*
 * _copyConvertRowtypeExpr
 */
static ConvertRowtypeExpr *
_copyConvertRowtypeExpr(ConvertRowtypeExpr *from)
{
	ConvertRowtypeExpr *newnode = makeNode(ConvertRowtypeExpr);

	COPY_NODE_FIELD(arg);
	COPY_SCALAR_FIELD(resulttype);
	COPY_SCALAR_FIELD(convertformat);

	return newnode;
}

/*
 * _copyCaseExpr
 */
static CaseExpr *
_copyCaseExpr(CaseExpr *from)
{
	CaseExpr   *newnode = makeNode(CaseExpr);

	COPY_SCALAR_FIELD(casetype);
	COPY_NODE_FIELD(arg);
	COPY_NODE_FIELD(args);
	COPY_NODE_FIELD(defresult);

	return newnode;
}

/*
 * _copyCaseWhen
 */
static CaseWhen *
_copyCaseWhen(CaseWhen *from)
{
	CaseWhen   *newnode = makeNode(CaseWhen);

	COPY_NODE_FIELD(expr);
	COPY_NODE_FIELD(result);

	return newnode;
}

/*
 * _copyCaseTestExpr
 */
static CaseTestExpr *
_copyCaseTestExpr(CaseTestExpr *from)
{
	CaseTestExpr *newnode = makeNode(CaseTestExpr);

	COPY_SCALAR_FIELD(typeId);
	COPY_SCALAR_FIELD(typeMod);

	return newnode;
}

/*
 * _copyArrayExpr
 */
static ArrayExpr *
_copyArrayExpr(ArrayExpr *from)
{
	ArrayExpr  *newnode = makeNode(ArrayExpr);

	COPY_SCALAR_FIELD(array_typeid);
	COPY_SCALAR_FIELD(element_typeid);
	COPY_NODE_FIELD(elements);
	COPY_SCALAR_FIELD(multidims);

	return newnode;
}

/*
 * _copyRowExpr
 */
static RowExpr *
_copyRowExpr(RowExpr *from)
{
	RowExpr    *newnode = makeNode(RowExpr);

	COPY_NODE_FIELD(args);
	COPY_SCALAR_FIELD(row_typeid);
	COPY_SCALAR_FIELD(row_format);

	return newnode;
}

/*
 * _copyRowCompareExpr
 */
static RowCompareExpr *
_copyRowCompareExpr(RowCompareExpr *from)
{
	RowCompareExpr *newnode = makeNode(RowCompareExpr);

	COPY_SCALAR_FIELD(rctype);
	COPY_NODE_FIELD(opnos);
	COPY_NODE_FIELD(opclasses);
	COPY_NODE_FIELD(largs);
	COPY_NODE_FIELD(rargs);

	return newnode;
}

/*
 * _copyCoalesceExpr
 */
static CoalesceExpr *
_copyCoalesceExpr(CoalesceExpr *from)
{
	CoalesceExpr *newnode = makeNode(CoalesceExpr);

	COPY_SCALAR_FIELD(coalescetype);
	COPY_NODE_FIELD(args);

	return newnode;
}

/*
 * _copyMinMaxExpr
 */
static MinMaxExpr *
_copyMinMaxExpr(MinMaxExpr *from)
{
	MinMaxExpr *newnode = makeNode(MinMaxExpr);

	COPY_SCALAR_FIELD(minmaxtype);
	COPY_SCALAR_FIELD(op);
	COPY_NODE_FIELD(args);

	return newnode;
}

/*
 * _copyNullIfExpr (same as OpExpr)
 */
static NullIfExpr *
_copyNullIfExpr(NullIfExpr *from)
{
	NullIfExpr *newnode = makeNode(NullIfExpr);

	COPY_SCALAR_FIELD(opno);
	COPY_SCALAR_FIELD(opfuncid);
	COPY_SCALAR_FIELD(opresulttype);
	COPY_SCALAR_FIELD(opretset);
	COPY_NODE_FIELD(args);

	return newnode;
}

/*
 * _copyNullTest
 */
static NullTest *
_copyNullTest(NullTest *from)
{
	NullTest   *newnode = makeNode(NullTest);

	COPY_NODE_FIELD(arg);
	COPY_SCALAR_FIELD(nulltesttype);

	return newnode;
}

/*
 * _copyBooleanTest
 */
static BooleanTest *
_copyBooleanTest(BooleanTest *from)
{
	BooleanTest *newnode = makeNode(BooleanTest);

	COPY_NODE_FIELD(arg);
	COPY_SCALAR_FIELD(booltesttype);

	return newnode;
}

/*
 * _copyCoerceToDomain
 */
static CoerceToDomain *
_copyCoerceToDomain(CoerceToDomain *from)
{
	CoerceToDomain *newnode = makeNode(CoerceToDomain);

	COPY_NODE_FIELD(arg);
	COPY_SCALAR_FIELD(resulttype);
	COPY_SCALAR_FIELD(resulttypmod);
	COPY_SCALAR_FIELD(coercionformat);

	return newnode;
}

/*
 * _copyCoerceToDomainValue
 */
static CoerceToDomainValue *
_copyCoerceToDomainValue(CoerceToDomainValue *from)
{
	CoerceToDomainValue *newnode = makeNode(CoerceToDomainValue);

	COPY_SCALAR_FIELD(typeId);
	COPY_SCALAR_FIELD(typeMod);

	return newnode;
}

/*
 * _copySetToDefault
 */
static SetToDefault *
_copySetToDefault(SetToDefault *from)
{
	SetToDefault *newnode = makeNode(SetToDefault);

	COPY_SCALAR_FIELD(typeId);
	COPY_SCALAR_FIELD(typeMod);

	return newnode;
}

/*
 * _copyCurrentOfExpr
 */
static CurrentOfExpr *
_copyCurrentOfExpr(CurrentOfExpr *from)
{
	CurrentOfExpr *newnode = makeNode(CurrentOfExpr);

	COPY_STRING_FIELD(cursor_name);
	COPY_SCALAR_FIELD(cvarno);
	COPY_SCALAR_FIELD(target_relid);
	COPY_SCALAR_FIELD(gp_segment_id);
	COPY_BINARY_FIELD(ctid, sizeof(ItemPointerData));
	COPY_SCALAR_FIELD(tableoid);

	return newnode;
}

/*
 * _copyTargetEntry
 */
static TargetEntry *
_copyTargetEntry(TargetEntry *from)
{
	TargetEntry *newnode = makeNode(TargetEntry);

	COPY_NODE_FIELD(expr);
	COPY_SCALAR_FIELD(resno);
	COPY_STRING_FIELD(resname);
	COPY_SCALAR_FIELD(ressortgroupref);
	COPY_SCALAR_FIELD(resorigtbl);
	COPY_SCALAR_FIELD(resorigcol);
	COPY_SCALAR_FIELD(resjunk);

	return newnode;
}

/*
 * _copyRangeTblRef
 */
static RangeTblRef *
_copyRangeTblRef(RangeTblRef *from)
{
	RangeTblRef *newnode = makeNode(RangeTblRef);

	COPY_SCALAR_FIELD(rtindex);

	return newnode;
}

/*
 * _copyJoinExpr
 */
static JoinExpr *
_copyJoinExpr(JoinExpr *from)
{
	JoinExpr   *newnode = makeNode(JoinExpr);

	COPY_SCALAR_FIELD(jointype);
	COPY_SCALAR_FIELD(isNatural);
	COPY_NODE_FIELD(larg);
	COPY_NODE_FIELD(rarg);
	COPY_NODE_FIELD(usingClause);
	COPY_NODE_FIELD(quals);
	COPY_NODE_FIELD(alias);
	COPY_SCALAR_FIELD(rtindex);
	COPY_NODE_FIELD(subqfromlist);          /*CDB*/

	return newnode;
}

/*
 * _copyFromExpr
 */
static FromExpr *
_copyFromExpr(FromExpr *from)
{
	FromExpr   *newnode = makeNode(FromExpr);

	COPY_NODE_FIELD(fromlist);
	COPY_NODE_FIELD(quals);

	return newnode;
}

/*
 * _copyFlow
 */
static Flow *
_copyFlow(Flow *from)
{
	Flow   *newnode = makeNode(Flow);

	COPY_SCALAR_FIELD(flotype);
	COPY_SCALAR_FIELD(req_move);
	COPY_SCALAR_FIELD(locustype);
	COPY_SCALAR_FIELD(segindex);
	COPY_SCALAR_FIELD(numSortCols);
	COPY_POINTER_FIELD(sortColIdx, from->numSortCols*sizeof(AttrNumber));
	COPY_POINTER_FIELD(sortOperators, from->numSortCols*sizeof(Oid));
	COPY_NODE_FIELD(hashExpr);
	COPY_NODE_FIELD(flow_before_req_move);

	return newnode;
}


/* ****************************************************************
 *						relation.h copy functions
 *
 * We don't support copying RelOptInfo, IndexOptInfo, or Path nodes.
 * There are some subsidiary structs that are useful to copy, though.
 * ****************************************************************
 */

/*
 * _copyCdbRelColumnInfo
 */
static CdbRelColumnInfo *
_copyCdbRelColumnInfo(CdbRelColumnInfo *from)
{
	CdbRelColumnInfo *newnode = makeNode(CdbRelColumnInfo);

	COPY_SCALAR_FIELD(pseudoattno);
    COPY_SCALAR_FIELD(targetresno);
	COPY_NODE_FIELD(defexpr);
	COPY_BITMAPSET_FIELD(where_needed);
	COPY_SCALAR_FIELD(attr_width);
    COPY_BINARY_FIELD(colname, sizeof(from->colname));

	return newnode;
}

/*
 * _copyPathKeyItem
 */
static PathKeyItem *
_copyPathKeyItem(PathKeyItem *from)
{
	PathKeyItem *newnode = makeNode(PathKeyItem);

	COPY_NODE_FIELD(key);
	COPY_SCALAR_FIELD(sortop);
	COPY_BITMAPSET_FIELD(cdb_key_relids);   /*CDB*/
	COPY_SCALAR_FIELD(cdb_num_relids);      /*CDB*/

	return newnode;
}

/*
 * _copyRestrictInfo
 */
static RestrictInfo *
_copyRestrictInfo(RestrictInfo *from)
{
	RestrictInfo *newnode = makeNode(RestrictInfo);

	COPY_NODE_FIELD(clause);
	COPY_SCALAR_FIELD(is_pushed_down);
	COPY_SCALAR_FIELD(outerjoin_delayed);
	COPY_SCALAR_FIELD(can_join);
	COPY_SCALAR_FIELD(pseudoconstant);
	COPY_BITMAPSET_FIELD(clause_relids);
	COPY_BITMAPSET_FIELD(required_relids);
	COPY_BITMAPSET_FIELD(left_relids);
	COPY_BITMAPSET_FIELD(right_relids);
	COPY_NODE_FIELD(orclause);
	COPY_SCALAR_FIELD(eval_cost);
	COPY_SCALAR_FIELD(this_selec);
	COPY_SCALAR_FIELD(mergejoinoperator);
	COPY_SCALAR_FIELD(left_sortop);
	COPY_SCALAR_FIELD(right_sortop);

	/*
	 * Do not copy pathkeys, since they'd not be canonical in a copied query
	 */
	newnode->left_pathkey = NIL;
	newnode->right_pathkey = NIL;

	COPY_SCALAR_FIELD(left_mergescansel);
	COPY_SCALAR_FIELD(right_mergescansel);
	COPY_SCALAR_FIELD(hashjoinoperator);
	COPY_SCALAR_FIELD(left_bucketsize);
	COPY_SCALAR_FIELD(right_bucketsize);

	return newnode;
}

/*
 * _copyOuterJoinInfo
 */
static OuterJoinInfo *
_copyOuterJoinInfo(OuterJoinInfo *from)
{
	OuterJoinInfo *newnode = makeNode(OuterJoinInfo);

	COPY_BITMAPSET_FIELD(min_lefthand);
	COPY_BITMAPSET_FIELD(min_righthand);
	COPY_BITMAPSET_FIELD(syn_lefthand);
	COPY_BITMAPSET_FIELD(syn_righthand);
	COPY_SCALAR_FIELD(join_type);
	COPY_SCALAR_FIELD(lhs_strict);
	COPY_SCALAR_FIELD(delay_upper_joins);
    COPY_NODE_FIELD(left_equi_key_list);
    COPY_NODE_FIELD(right_equi_key_list);
	return newnode;
}

/*
 * _copyInClauseInfo
 */
static InClauseInfo *
_copyInClauseInfo(InClauseInfo *from)
{
	InClauseInfo *newnode = makeNode(InClauseInfo);

	COPY_BITMAPSET_FIELD(righthand);
	COPY_NODE_FIELD(sub_targetlist);

    COPY_SCALAR_FIELD(try_join_unique);                 /*CDB*/

	return newnode;
}

/*
 * _copyAppendRelInfo
 */
static AppendRelInfo *
_copyAppendRelInfo(AppendRelInfo *from)
{
	AppendRelInfo *newnode = makeNode(AppendRelInfo);

	COPY_SCALAR_FIELD(parent_relid);
	COPY_SCALAR_FIELD(child_relid);
	COPY_SCALAR_FIELD(parent_reltype);
	COPY_SCALAR_FIELD(child_reltype);
	COPY_NODE_FIELD(col_mappings);
	COPY_NODE_FIELD(translated_vars);
	COPY_SCALAR_FIELD(parent_reloid);

	return newnode;
}

/* ****************************************************************
 *					parsenodes.h copy functions
 * ****************************************************************
 */

static RangeTblEntry *
_copyRangeTblEntry(RangeTblEntry *from)
{
	RangeTblEntry *newnode = makeNode(RangeTblEntry);

	COPY_SCALAR_FIELD(rtekind);
	COPY_SCALAR_FIELD(relid);
	COPY_NODE_FIELD(subquery);
	COPY_NODE_FIELD(funcexpr);
	COPY_NODE_FIELD(funccoltypes);
	COPY_NODE_FIELD(funccoltypmods);
	COPY_VARLENA_FIELD(funcuserdata, -1);
	COPY_NODE_FIELD(values_lists);
	COPY_SCALAR_FIELD(jointype);
	COPY_NODE_FIELD(joinaliasvars);
	COPY_NODE_FIELD(alias);
	COPY_NODE_FIELD(eref);
	COPY_SCALAR_FIELD(inh);
	COPY_SCALAR_FIELD(inFromCl);
	COPY_SCALAR_FIELD(requiredPerms);
	COPY_SCALAR_FIELD(checkAsUser);

	COPY_STRING_FIELD(ctename);
	COPY_SCALAR_FIELD(ctelevelsup);
	COPY_SCALAR_FIELD(self_reference);
	COPY_NODE_FIELD(ctecoltypes);
	COPY_NODE_FIELD(ctecoltypmods);

	COPY_SCALAR_FIELD(forceDistRandom);
    COPY_NODE_FIELD(pseudocols);                /*CDB*/

	return newnode;
}

static FkConstraint *
_copyFkConstraint(FkConstraint *from)
{
	FkConstraint *newnode = makeNode(FkConstraint);

	COPY_STRING_FIELD(constr_name);
	COPY_SCALAR_FIELD(constrOid);
	COPY_NODE_FIELD(pktable);
	COPY_NODE_FIELD(fk_attrs);
	COPY_NODE_FIELD(pk_attrs);
	COPY_SCALAR_FIELD(fk_matchtype);
	COPY_SCALAR_FIELD(fk_upd_action);
	COPY_SCALAR_FIELD(fk_del_action);
	COPY_SCALAR_FIELD(deferrable);
	COPY_SCALAR_FIELD(initdeferred);
	COPY_SCALAR_FIELD(skip_validation);
	COPY_SCALAR_FIELD(old_pktable_oid);
	COPY_SCALAR_FIELD(trig1Oid);
	COPY_SCALAR_FIELD(trig2Oid);
	COPY_SCALAR_FIELD(trig3Oid);
	COPY_SCALAR_FIELD(trig4Oid);

	return newnode;
}

static SortClause *
_copySortClause(SortClause *from)
{
	SortClause *newnode = makeNode(SortClause);

	COPY_SCALAR_FIELD(tleSortGroupRef);
	COPY_SCALAR_FIELD(sortop);

	return newnode;
}

static GroupClause *
_copyGroupClause(GroupClause *from)
{
	GroupClause *newnode = makeNode(GroupClause);

	COPY_SCALAR_FIELD(tleSortGroupRef);
	COPY_SCALAR_FIELD(sortop);

	return newnode;
}

static GroupingClause *
_copyGroupingClause(GroupingClause *from)
{
	GroupingClause *newnode = makeNode(GroupingClause);
	COPY_SCALAR_FIELD(groupType);
	COPY_NODE_FIELD(groupsets);

	return newnode;
}

static GroupingFunc *
_copyGroupingFunc(GroupingFunc *from)
{
	GroupingFunc *newnode = makeNode(GroupingFunc);

	COPY_NODE_FIELD(args);
	COPY_SCALAR_FIELD(ngrpcols);

	return newnode;
}

static Grouping *
_copyGrouping(Grouping *from)
{
	Grouping *newnode = makeNode(Grouping);

	return newnode;
}

static GroupId *
_copyGroupId(GroupId *from)
{
	GroupId *newnode = makeNode(GroupId);

	return newnode;
}

static WindowSpecParse *
_copyWindowSpecParse(WindowSpecParse *from)
{
	WindowSpecParse *newnode = makeNode(WindowSpecParse);

	COPY_STRING_FIELD(name);
	COPY_NODE_FIELD(elems);

	return newnode;
}

static WindowSpec *
_copyWindowSpec(WindowSpec *from)
{
	WindowSpec *newnode = makeNode(WindowSpec);

	COPY_STRING_FIELD(name);
	COPY_STRING_FIELD(parent);
	COPY_NODE_FIELD(partition);
	COPY_NODE_FIELD(order);
	COPY_NODE_FIELD(frame);
	COPY_SCALAR_FIELD(location);

	return newnode;
}

static WindowFrame *
_copyWindowFrame(WindowFrame *from)
{
	WindowFrame *newnode = makeNode(WindowFrame);

	COPY_SCALAR_FIELD(is_rows);
	COPY_SCALAR_FIELD(is_between);
	COPY_NODE_FIELD(trail);
	COPY_NODE_FIELD(lead);
	COPY_SCALAR_FIELD(exclude);

	return newnode;
}

static WindowFrameEdge *
_copyWindowFrameEdge(WindowFrameEdge *from)
{
	WindowFrameEdge *newnode = makeNode(WindowFrameEdge);

	COPY_SCALAR_FIELD(kind);
	COPY_NODE_FIELD(val);

	return newnode;
}

static PercentileExpr *
_copyPercentileExpr(PercentileExpr *from)
{
	PercentileExpr *newnode = makeNode(PercentileExpr);

	COPY_SCALAR_FIELD(perctype);
	COPY_NODE_FIELD(args);
	COPY_SCALAR_FIELD(perckind);
	COPY_NODE_FIELD(sortClause);
	COPY_NODE_FIELD(sortTargets);
	COPY_NODE_FIELD(pcExpr);
	COPY_NODE_FIELD(tcExpr);
	COPY_SCALAR_FIELD(location);

	return newnode;
}

static RowMarkClause *
_copyRowMarkClause(RowMarkClause *from)
{
	RowMarkClause *newnode = makeNode(RowMarkClause);

	COPY_SCALAR_FIELD(rti);
	COPY_SCALAR_FIELD(forUpdate);
	COPY_SCALAR_FIELD(noWait);

	return newnode;
}

static WithClause *
_copyWithClause(WithClause *from)
{
	WithClause *newnode = makeNode(WithClause);
	
	COPY_NODE_FIELD(ctes);
	COPY_SCALAR_FIELD(recursive);
	COPY_SCALAR_FIELD(location);
	
	return newnode;
}

static CommonTableExpr *
_copyCommonTableExpr(CommonTableExpr *from)
{
	CommonTableExpr *newnode = makeNode(CommonTableExpr);
	
	COPY_STRING_FIELD(ctename);
	COPY_NODE_FIELD(aliascolnames);
	COPY_NODE_FIELD(ctequery);
	COPY_SCALAR_FIELD(location);
	COPY_SCALAR_FIELD(cterecursive);
	COPY_SCALAR_FIELD(cterefcount);
	COPY_NODE_FIELD(ctecolnames);
	COPY_NODE_FIELD(ctecoltypes);
	COPY_NODE_FIELD(ctecoltypmods);

	return newnode;
}

static A_Expr *
_copyAExpr(A_Expr *from)
{
	A_Expr	   *newnode = makeNode(A_Expr);

	COPY_SCALAR_FIELD(kind);
	COPY_NODE_FIELD(name);
	COPY_NODE_FIELD(lexpr);
	COPY_NODE_FIELD(rexpr);
	COPY_SCALAR_FIELD(location);

	return newnode;
}

static ColumnRef *
_copyColumnRef(ColumnRef *from)
{
	ColumnRef  *newnode = makeNode(ColumnRef);

	COPY_NODE_FIELD(fields);
	COPY_SCALAR_FIELD(location);

	return newnode;
}

static ParamRef *
_copyParamRef(ParamRef *from)
{
	ParamRef   *newnode = makeNode(ParamRef);

	COPY_SCALAR_FIELD(number);
	COPY_SCALAR_FIELD(location);    /*CDB*/

	return newnode;
}

static A_Const *
_copyAConst(A_Const *from)
{
	A_Const    *newnode = makeNode(A_Const);

	/* This part must duplicate _copyValue */
	COPY_SCALAR_FIELD(val.type);
	switch (from->val.type)
	{
		case T_Integer:
			COPY_SCALAR_FIELD(val.val.ival);
			break;
		case T_Float:
		case T_String:
		case T_BitString:
			COPY_STRING_FIELD(val.val.str);
			break;
		case T_Null:
			/* nothing to do */
			break;
		default:
			elog(ERROR, "unrecognized node type: %d",
				 (int) from->val.type);
			break;
	}

	COPY_NODE_FIELD(typname);
	COPY_SCALAR_FIELD(location);    /*CDB*/

	return newnode;
}

static FuncCall *
_copyFuncCall(FuncCall *from)
{
	FuncCall   *newnode = makeNode(FuncCall);

	COPY_NODE_FIELD(funcname);
	COPY_NODE_FIELD(args);
    COPY_NODE_FIELD(agg_order);
	COPY_SCALAR_FIELD(agg_star);
	COPY_SCALAR_FIELD(agg_distinct);
	COPY_NODE_FIELD(over);
	COPY_SCALAR_FIELD(location);
	COPY_NODE_FIELD(agg_filter);

	return newnode;
}

static A_Indices *
_copyAIndices(A_Indices *from)
{
	A_Indices  *newnode = makeNode(A_Indices);

	COPY_NODE_FIELD(lidx);
	COPY_NODE_FIELD(uidx);

	return newnode;
}

static A_Indirection *
_copyA_Indirection(A_Indirection *from)
{
	A_Indirection *newnode = makeNode(A_Indirection);

	COPY_NODE_FIELD(arg);
	COPY_NODE_FIELD(indirection);

	return newnode;
}

static ResTarget *
_copyResTarget(ResTarget *from)
{
	ResTarget  *newnode = makeNode(ResTarget);

	COPY_STRING_FIELD(name);
	COPY_NODE_FIELD(indirection);
	COPY_NODE_FIELD(val);
	COPY_SCALAR_FIELD(location);

	return newnode;
}

static TypeName *
_copyTypeName(TypeName *from)
{
	TypeName   *newnode = makeNode(TypeName);

	COPY_NODE_FIELD(names);
	COPY_SCALAR_FIELD(typid);
	COPY_SCALAR_FIELD(timezone);
	COPY_SCALAR_FIELD(setof);
	COPY_SCALAR_FIELD(pct_type);
	COPY_SCALAR_FIELD(typmod);
	COPY_NODE_FIELD(arrayBounds);
	COPY_SCALAR_FIELD(location);

	return newnode;
}

static SortBy *
_copySortBy(SortBy *from)
{
	SortBy	   *newnode = makeNode(SortBy);

	COPY_SCALAR_FIELD(sortby_kind);
	COPY_NODE_FIELD(useOp);
	COPY_NODE_FIELD(node);

	return newnode;
}

static RangeSubselect *
_copyRangeSubselect(RangeSubselect *from)
{
	RangeSubselect *newnode = makeNode(RangeSubselect);

	COPY_NODE_FIELD(subquery);
	COPY_NODE_FIELD(alias);

	return newnode;
}

static RangeFunction *
_copyRangeFunction(RangeFunction *from)
{
	RangeFunction *newnode = makeNode(RangeFunction);

	COPY_NODE_FIELD(funccallnode);
	COPY_NODE_FIELD(alias);
	COPY_NODE_FIELD(coldeflist);

	return newnode;
}

static TypeCast *
_copyTypeCast(TypeCast *from)
{
	TypeCast   *newnode = makeNode(TypeCast);

	COPY_NODE_FIELD(arg);
	COPY_NODE_FIELD(typname);

	return newnode;
}

static IndexElem *
_copyIndexElem(IndexElem *from)
{
	IndexElem  *newnode = makeNode(IndexElem);

	COPY_STRING_FIELD(name);
	COPY_NODE_FIELD(expr);
	COPY_NODE_FIELD(opclass);

	return newnode;
}

static ColumnDef *
_copyColumnDef(ColumnDef *from)
{
	ColumnDef  *newnode = makeNode(ColumnDef);

	COPY_STRING_FIELD(colname);
	COPY_NODE_FIELD(typname);
	COPY_SCALAR_FIELD(inhcount);
	COPY_SCALAR_FIELD(is_local);
	COPY_SCALAR_FIELD(is_not_null);
	COPY_NODE_FIELD(raw_default);
	COPY_SCALAR_FIELD(default_is_null);
	COPY_STRING_FIELD(cooked_default);
	COPY_NODE_FIELD(constraints);
	COPY_NODE_FIELD(encoding);

	return newnode;
}

static ColumnReferenceStorageDirective *
_copyColumnReferenceStorageDirective(ColumnReferenceStorageDirective *from)
{
	ColumnReferenceStorageDirective *newnode =
		makeNode(ColumnReferenceStorageDirective);

	COPY_NODE_FIELD(column);
	COPY_SCALAR_FIELD(deflt);
	COPY_NODE_FIELD(encoding);

	return newnode;
}

static Constraint *
_copyConstraint(Constraint *from)
{
	Constraint *newnode = makeNode(Constraint);

	COPY_SCALAR_FIELD(contype);
	COPY_SCALAR_FIELD(conoid);
	COPY_STRING_FIELD(name);
	COPY_NODE_FIELD(raw_expr);
	COPY_STRING_FIELD(cooked_expr);
	COPY_NODE_FIELD(keys);
	COPY_NODE_FIELD(options);
	COPY_STRING_FIELD(indexspace);

	return newnode;
}

static DefElem *
_copyDefElem(DefElem *from)
{
	DefElem    *newnode = makeNode(DefElem);

	COPY_STRING_FIELD(defname);
	COPY_NODE_FIELD(arg);
	COPY_SCALAR_FIELD(defaction);

	return newnode;
}

static LockingClause *
_copyLockingClause(LockingClause *from)
{
	LockingClause *newnode = makeNode(LockingClause);

	COPY_NODE_FIELD(lockedRels);
	COPY_SCALAR_FIELD(forUpdate);
	COPY_SCALAR_FIELD(noWait);

	return newnode;
}

static DMLActionExpr *
_copyDMLActionExpr(const DMLActionExpr *from)
{
	DMLActionExpr *newnode = makeNode(DMLActionExpr);

	return newnode;
}

static PartOidExpr *
_copyPartOidExpr(const PartOidExpr *from)
{
	PartOidExpr *newnode = makeNode(PartOidExpr);
	COPY_SCALAR_FIELD(level);

	return newnode;
}

static PartDefaultExpr *
_copyPartDefaultExpr(const PartDefaultExpr *from)
{
	PartDefaultExpr *newnode = makeNode(PartDefaultExpr);
	COPY_SCALAR_FIELD(level);

	return newnode;
}

static PartBoundExpr *
_copyPartBoundExpr(const PartBoundExpr *from)
{
	PartBoundExpr *newnode = makeNode(PartBoundExpr);
	COPY_SCALAR_FIELD(level);
	COPY_SCALAR_FIELD(boundType);
	COPY_SCALAR_FIELD(isLowerBound);

	return newnode;
}

static PartBoundInclusionExpr *
_copyPartBoundInclusionExpr(const PartBoundInclusionExpr *from)
{
	PartBoundInclusionExpr *newnode = makeNode(PartBoundInclusionExpr);
	COPY_SCALAR_FIELD(level);
	COPY_SCALAR_FIELD(isLowerBound);

	return newnode;
}

static PartBoundOpenExpr *
_copyPartBoundOpenExpr(const PartBoundOpenExpr *from)
{
	PartBoundOpenExpr *newnode = makeNode(PartBoundOpenExpr);
	COPY_SCALAR_FIELD(level);
	COPY_SCALAR_FIELD(isLowerBound);

	return newnode;
}

static Query *
_copyQuery(Query *from)
{
	Query	   *newnode = makeNode(Query);

	COPY_SCALAR_FIELD(commandType);
	COPY_SCALAR_FIELD(querySource);
	COPY_SCALAR_FIELD(canSetTag);
	COPY_NODE_FIELD(utilityStmt);
	COPY_SCALAR_FIELD(resultRelation);
	COPY_NODE_FIELD(intoClause);
	COPY_SCALAR_FIELD(hasAggs);
	COPY_SCALAR_FIELD(hasWindFuncs);
	COPY_SCALAR_FIELD(hasSubLinks);
	COPY_NODE_FIELD(rtable);
	COPY_NODE_FIELD(jointree);
	COPY_NODE_FIELD(targetList);
	COPY_NODE_FIELD(returningList);
	COPY_NODE_FIELD(groupClause);
	COPY_NODE_FIELD(havingQual);
	COPY_NODE_FIELD(windowClause);
	COPY_NODE_FIELD(distinctClause);
	COPY_NODE_FIELD(sortClause);
	COPY_NODE_FIELD(scatterClause);
	COPY_NODE_FIELD(cteList);
	COPY_SCALAR_FIELD(hasRecursive);
	COPY_SCALAR_FIELD(hasModifyingCTE);
	COPY_NODE_FIELD(limitOffset);
	COPY_NODE_FIELD(limitCount);
	COPY_NODE_FIELD(rowMarks);
	COPY_NODE_FIELD(setOperations);
	COPY_NODE_FIELD(resultRelations);
	COPY_NODE_FIELD(returningLists);
	if (from->intoPolicy)
	{
		COPY_POINTER_FIELD(intoPolicy,sizeof(GpPolicy) + from->intoPolicy->nattrs*sizeof(from->intoPolicy->attrs[0]));
	}
	else
		newnode->intoPolicy = NULL;

	return newnode;
}

static InsertStmt *
_copyInsertStmt(InsertStmt *from)
{
	InsertStmt *newnode = makeNode(InsertStmt);

	COPY_NODE_FIELD(relation);
	COPY_NODE_FIELD(cols);
	COPY_NODE_FIELD(selectStmt);
	COPY_NODE_FIELD(returningList);

	return newnode;
}

static DeleteStmt *
_copyDeleteStmt(DeleteStmt *from)
{
	DeleteStmt *newnode = makeNode(DeleteStmt);

	COPY_NODE_FIELD(relation);
	COPY_NODE_FIELD(usingClause);
	COPY_NODE_FIELD(whereClause);
	COPY_NODE_FIELD(returningList);

	return newnode;
}

static UpdateStmt *
_copyUpdateStmt(UpdateStmt *from)
{
	UpdateStmt *newnode = makeNode(UpdateStmt);

	COPY_NODE_FIELD(relation);
	COPY_NODE_FIELD(targetList);
	COPY_NODE_FIELD(whereClause);
	COPY_NODE_FIELD(fromClause);
	COPY_NODE_FIELD(returningList);

	return newnode;
}

static SelectStmt *
_copySelectStmt(SelectStmt *from)
{
	SelectStmt *newnode = makeNode(SelectStmt);

	COPY_NODE_FIELD(distinctClause);
	COPY_NODE_FIELD(intoClause);
	COPY_NODE_FIELD(targetList);
	COPY_NODE_FIELD(fromClause);
	COPY_NODE_FIELD(whereClause);
	COPY_NODE_FIELD(groupClause);
	COPY_NODE_FIELD(havingClause);
	COPY_NODE_FIELD(windowClause);
	COPY_NODE_FIELD(valuesLists);
	COPY_NODE_FIELD(sortClause);
	COPY_NODE_FIELD(scatterClause);
	COPY_NODE_FIELD(withClause);
	COPY_NODE_FIELD(limitOffset);
	COPY_NODE_FIELD(limitCount);
	COPY_NODE_FIELD(lockingClause);
	COPY_SCALAR_FIELD(op);
	COPY_SCALAR_FIELD(all);
	COPY_NODE_FIELD(larg);
	COPY_NODE_FIELD(rarg);
	COPY_NODE_FIELD(distributedBy);

	return newnode;
}

static SetOperationStmt *
_copySetOperationStmt(SetOperationStmt *from)
{
	SetOperationStmt *newnode = makeNode(SetOperationStmt);

	COPY_SCALAR_FIELD(op);
	COPY_SCALAR_FIELD(all);
	COPY_NODE_FIELD(larg);
	COPY_NODE_FIELD(rarg);
	COPY_NODE_FIELD(colTypes);
	COPY_NODE_FIELD(colTypmods);
	return newnode;
}

static AlterTableStmt *
_copyAlterTableStmt(AlterTableStmt *from)
{
	AlterTableStmt *newnode = makeNode(AlterTableStmt);

	COPY_NODE_FIELD(relation);
	COPY_NODE_FIELD(cmds);
	COPY_SCALAR_FIELD(relkind);
	
	/* No need to copy AT workspace fields. */

	return newnode;
}

static AlterTableCmd *
_copyAlterTableCmd(AlterTableCmd *from)
{
	AlterTableCmd *newnode = makeNode(AlterTableCmd);

	COPY_SCALAR_FIELD(subtype);
	COPY_STRING_FIELD(name);
	COPY_NODE_FIELD(def);
	COPY_NODE_FIELD(transform);
	COPY_SCALAR_FIELD(behavior);
	COPY_SCALAR_FIELD(part_expanded);
	
	/* Need to copy AT workspace since process uses copy internally. */
	COPY_NODE_FIELD(partoids);

	return newnode;
}

static InheritPartitionCmd *
_copyInheritPartitionCmd(InheritPartitionCmd *from)
{
	InheritPartitionCmd *newnode = makeNode(InheritPartitionCmd);

	COPY_NODE_FIELD(parent);

	return newnode;
}

static AlterPartitionCmd *
_copyAlterPartitionCmd(AlterPartitionCmd *from)
{
	AlterPartitionCmd *newnode = makeNode(AlterPartitionCmd);

	COPY_NODE_FIELD(partid);
	COPY_NODE_FIELD(arg1);
	COPY_NODE_FIELD(arg2);
	COPY_NODE_FIELD(scantable_splits);
	COPY_NODE_FIELD(newpart_aosegnos);

	return newnode;
}

static AlterPartitionId *
_copyAlterPartitionId(AlterPartitionId *from)
{
	AlterPartitionId *newnode = makeNode(AlterPartitionId);

	COPY_SCALAR_FIELD(idtype);
	COPY_NODE_FIELD(partiddef);

	return newnode;
}


static AlterDomainStmt *
_copyAlterDomainStmt(AlterDomainStmt *from)
{
	AlterDomainStmt *newnode = makeNode(AlterDomainStmt);

	COPY_SCALAR_FIELD(subtype);
	COPY_NODE_FIELD(typname);
	COPY_STRING_FIELD(name);
	COPY_NODE_FIELD(def);
	COPY_SCALAR_FIELD(behavior);

	return newnode;
}

static GrantStmt *
_copyGrantStmt(GrantStmt *from)
{
	GrantStmt  *newnode = makeNode(GrantStmt);

	COPY_SCALAR_FIELD(is_grant);
	COPY_SCALAR_FIELD(objtype);
	COPY_NODE_FIELD(objects);
	COPY_NODE_FIELD(privileges);
	COPY_NODE_FIELD(grantees);
	COPY_SCALAR_FIELD(grant_option);
	COPY_SCALAR_FIELD(behavior);
	COPY_NODE_FIELD(cooked_privs);

	return newnode;
}

static PrivGrantee *
_copyPrivGrantee(PrivGrantee *from)
{
	PrivGrantee *newnode = makeNode(PrivGrantee);

	COPY_STRING_FIELD(rolname);

	return newnode;
}

static FuncWithArgs *
_copyFuncWithArgs(FuncWithArgs *from)
{
	FuncWithArgs *newnode = makeNode(FuncWithArgs);

	COPY_NODE_FIELD(funcname);
	COPY_NODE_FIELD(funcargs);

	return newnode;
}

static GrantRoleStmt *
_copyGrantRoleStmt(GrantRoleStmt *from)
{
	GrantRoleStmt *newnode = makeNode(GrantRoleStmt);

	COPY_NODE_FIELD(granted_roles);
	COPY_NODE_FIELD(grantee_roles);
	COPY_SCALAR_FIELD(is_grant);
	COPY_SCALAR_FIELD(admin_opt);
	COPY_STRING_FIELD(grantor);
	COPY_SCALAR_FIELD(behavior);

	return newnode;
}

static DeclareCursorStmt *
_copyDeclareCursorStmt(DeclareCursorStmt *from)
{
	DeclareCursorStmt *newnode = makeNode(DeclareCursorStmt);

	COPY_STRING_FIELD(portalname);
	COPY_SCALAR_FIELD(options);
	COPY_NODE_FIELD(query);
	COPY_SCALAR_FIELD(is_simply_updatable);

	return newnode;
}

static ClosePortalStmt *
_copyClosePortalStmt(ClosePortalStmt *from)
{
	ClosePortalStmt *newnode = makeNode(ClosePortalStmt);

	COPY_STRING_FIELD(portalname);

	return newnode;
}

static ClusterStmt *
_copyClusterStmt(ClusterStmt *from)
{
	ClusterStmt *newnode = makeNode(ClusterStmt);

	COPY_NODE_FIELD(relation);
	COPY_STRING_FIELD(indexname);
	COPY_SCALAR_FIELD(oidInfo.relOid);
	COPY_SCALAR_FIELD(oidInfo.comptypeOid);
	COPY_SCALAR_FIELD(oidInfo.toastOid);
	COPY_SCALAR_FIELD(oidInfo.toastIndexOid);
	COPY_SCALAR_FIELD(oidInfo.toastComptypeOid);
	COPY_NODE_FIELD(new_ind_oids);

	return newnode;
}

static SingleRowErrorDesc *
_copySingleRowErrorDesc(SingleRowErrorDesc *from)
{
	SingleRowErrorDesc *newnode = makeNode(SingleRowErrorDesc);

	COPY_NODE_FIELD(errtable);
	COPY_SCALAR_FIELD(rejectlimit);
	COPY_SCALAR_FIELD(is_keep);
	COPY_SCALAR_FIELD(is_limit_in_rows);
	COPY_SCALAR_FIELD(reusing_existing_errtable);

	return newnode;
}

static CopyStmt *
_copyCopyStmt(CopyStmt *from)
{
	CopyStmt   *newnode = makeNode(CopyStmt);

	COPY_NODE_FIELD(relation);
	COPY_NODE_FIELD(query);
	COPY_NODE_FIELD(attlist);
	COPY_SCALAR_FIELD(is_from);
	COPY_STRING_FIELD(filename);
	COPY_NODE_FIELD(options);
	COPY_NODE_FIELD(sreh);
	COPY_NODE_FIELD(err_aosegnos);
	COPY_NODE_FIELD(scantable_splits);

	return newnode;
}

static CreateStmt *
_copyCreateStmt(CreateStmt *from)
{
	CreateStmt *newnode = makeNode(CreateStmt);

	COPY_NODE_FIELD(relation);
	COPY_NODE_FIELD(tableElts);
	COPY_NODE_FIELD(inhRelations);
	COPY_NODE_FIELD(constraints);
	COPY_NODE_FIELD(options);
	COPY_SCALAR_FIELD(oncommit);
	COPY_STRING_FIELD(tablespacename);
	COPY_NODE_FIELD(distributedBy);
	COPY_SCALAR_FIELD(oidInfo.relOid);
	COPY_SCALAR_FIELD(oidInfo.comptypeOid);
	COPY_SCALAR_FIELD(oidInfo.toastOid);
	COPY_SCALAR_FIELD(oidInfo.toastIndexOid);
	COPY_SCALAR_FIELD(oidInfo.toastComptypeOid);
	COPY_SCALAR_FIELD(relKind);
	COPY_SCALAR_FIELD(relStorage);
	if (from->policy)
	{
		COPY_POINTER_FIELD(policy,sizeof(GpPolicy) + from->policy->nattrs*sizeof(from->policy->attrs[0]));
	}
	else
		newnode->policy = NULL;
	/* postCreate omitted (why?) */
	COPY_NODE_FIELD(deferredStmts);
	COPY_SCALAR_FIELD(is_part_child);
	COPY_SCALAR_FIELD(is_add_part);
	COPY_SCALAR_FIELD(ownerid);
	COPY_SCALAR_FIELD(buildAoBlkdir);
	COPY_NODE_FIELD(attr_encodings);

	return newnode;
}

static PartitionBy *
_copyPartitionBy(PartitionBy *from)
{
	PartitionBy *newnode = makeNode(PartitionBy);

	COPY_SCALAR_FIELD(partType);
	COPY_NODE_FIELD(keys);
	COPY_NODE_FIELD(keyopclass);
	COPY_NODE_FIELD(partNum);
	COPY_NODE_FIELD(subPart);
	COPY_NODE_FIELD(partSpec);
	COPY_SCALAR_FIELD(partDepth);
	COPY_NODE_FIELD(parentRel);
	COPY_SCALAR_FIELD(partQuiet);
	COPY_SCALAR_FIELD(location);

	return newnode;
}

static PartitionSpec *
_copyPartitionSpec(PartitionSpec *from)
{
	PartitionSpec *newnode = makeNode(PartitionSpec);

	COPY_NODE_FIELD(partElem);
	COPY_NODE_FIELD(subSpec);
	COPY_SCALAR_FIELD(istemplate);
	COPY_NODE_FIELD(enc_clauses);
	COPY_SCALAR_FIELD(location);

	return newnode;
}

static PartitionValuesSpec *
_copyPartitionValuesSpec(PartitionValuesSpec *from)
{
	PartitionValuesSpec *newnode = makeNode(PartitionValuesSpec);

	COPY_NODE_FIELD(partValues);
	COPY_SCALAR_FIELD(location);

	return newnode;
}

static PartitionElem *
_copyPartitionElem(PartitionElem *from)
{
	PartitionElem *newnode = makeNode(PartitionElem);

	COPY_NODE_FIELD(partName);
	COPY_NODE_FIELD(boundSpec);
	COPY_NODE_FIELD(subSpec);
	COPY_SCALAR_FIELD(isDefault);
	COPY_NODE_FIELD(storeAttr);
	COPY_SCALAR_FIELD(partno);
	COPY_SCALAR_FIELD(rrand);
	COPY_NODE_FIELD(colencs);
	COPY_SCALAR_FIELD(location);

	return newnode;
}

static PartitionRangeItem *
_copyPartitionRangeItem(PartitionRangeItem *from)
{
	PartitionRangeItem *newnode = makeNode(PartitionRangeItem);

	COPY_NODE_FIELD(partRangeVal);
	COPY_SCALAR_FIELD(partedge);
	COPY_SCALAR_FIELD(location);

	return newnode;
}

static PartitionBoundSpec *
_copyPartitionBoundSpec(PartitionBoundSpec *from)
{
	PartitionBoundSpec *newnode = makeNode(PartitionBoundSpec);

	COPY_NODE_FIELD(partStart);
	COPY_NODE_FIELD(partEnd);
	COPY_NODE_FIELD(partEvery);
	COPY_SCALAR_FIELD(location);

	return newnode;
}

static PgPartRule *
_copyPgPartRule(PgPartRule *from)
{
	PgPartRule *newnode = makeNode(PgPartRule);

	COPY_NODE_FIELD(pNode);
	COPY_NODE_FIELD(topRule);
	COPY_STRING_FIELD(partIdStr);
	COPY_SCALAR_FIELD(isName);
	COPY_SCALAR_FIELD(topRuleRank);
	COPY_STRING_FIELD(relname);

	return newnode;
}

static Partition *
_copyPartition(Partition *from)
{
	Partition *newnode = makeNode(Partition);

	COPY_SCALAR_FIELD(partid);
	COPY_SCALAR_FIELD(parrelid);
	COPY_SCALAR_FIELD(parkind);
	COPY_SCALAR_FIELD(parlevel);
	COPY_SCALAR_FIELD(paristemplate);
	COPY_SCALAR_FIELD(parnatts);
	COPY_POINTER_FIELD(paratts, from->parnatts * sizeof(AttrNumber));
	COPY_POINTER_FIELD(parclass, from->parnatts * sizeof(Oid));

	return newnode;
}

static PartitionRule *
_copyPartitionRule(PartitionRule *from)
{
	PartitionRule *newnode = makeNode(PartitionRule);

	COPY_SCALAR_FIELD(parruleid);
	COPY_SCALAR_FIELD(paroid);
	COPY_SCALAR_FIELD(parchildrelid);
	COPY_SCALAR_FIELD(parparentoid);
	COPY_SCALAR_FIELD(parisdefault);
	COPY_STRING_FIELD(parname);
	COPY_NODE_FIELD(parrangestart);
	COPY_SCALAR_FIELD(parrangestartincl);
	COPY_NODE_FIELD(parrangeend);
	COPY_SCALAR_FIELD(parrangeendincl);
	COPY_NODE_FIELD(parrangeevery);
	COPY_NODE_FIELD(parlistvalues);
	COPY_SCALAR_FIELD(parruleord);
	COPY_NODE_FIELD(parreloptions);
	COPY_SCALAR_FIELD(partemplatespaceId);
	COPY_NODE_FIELD(children); /* sub partition */

	return newnode;
}

static PartitionNode *
_copyPartitionNode(PartitionNode *from)
{
	PartitionNode *newnode = makeNode(PartitionNode);

	COPY_NODE_FIELD(part);
	COPY_NODE_FIELD(default_part);
	COPY_NODE_FIELD(rules);

	return newnode;
}

static ExtTableTypeDesc *
_copyExtTableTypeDesc(ExtTableTypeDesc *from)
{
	ExtTableTypeDesc *newnode = makeNode(ExtTableTypeDesc);

	COPY_SCALAR_FIELD(exttabletype);
	COPY_NODE_FIELD(location_list);
	COPY_NODE_FIELD(on_clause);
	COPY_STRING_FIELD(command_string);

	return newnode;
}

static CreateExternalStmt *
_copyCreateExternalStmt(CreateExternalStmt *from)
{
	CreateExternalStmt *newnode = makeNode(CreateExternalStmt);

	COPY_NODE_FIELD(relation);
	COPY_NODE_FIELD(tableElts);
	COPY_NODE_FIELD(exttypedesc);
	COPY_STRING_FIELD(format);
	COPY_NODE_FIELD(formatOpts);
	COPY_SCALAR_FIELD(isweb);
	COPY_SCALAR_FIELD(iswritable);
	COPY_NODE_FIELD(sreh);
	COPY_NODE_FIELD(encoding);
	COPY_NODE_FIELD(distributedBy);	
	if (from->policy)
	{
		COPY_POINTER_FIELD(policy,sizeof(GpPolicy) + from->policy->nattrs*sizeof(from->policy->attrs[0]));
	}
	else
		newnode->policy = NULL;

	return newnode;
}

static CreateForeignStmt *
_copyCreateForeignStmt(CreateForeignStmt *from)
{
	CreateForeignStmt *newnode = makeNode(CreateForeignStmt);

	COPY_NODE_FIELD(relation);
	COPY_NODE_FIELD(tableElts);
	COPY_STRING_FIELD(srvname);
	COPY_NODE_FIELD(options);

	return newnode;
}

static InhRelation *
_copyInhRelation(InhRelation *from)
{
	InhRelation *newnode = makeNode(InhRelation);

	COPY_NODE_FIELD(relation);
	COPY_NODE_FIELD(options);

	return newnode;
}

static DefineStmt *
_copyDefineStmt(DefineStmt *from)
{
	DefineStmt *newnode = makeNode(DefineStmt);

	COPY_SCALAR_FIELD(kind);
	COPY_SCALAR_FIELD(oldstyle);
	COPY_NODE_FIELD(defnames);
	COPY_NODE_FIELD(args);
	COPY_NODE_FIELD(definition);
	COPY_SCALAR_FIELD(newOid);
	COPY_SCALAR_FIELD(shadowOid);
	COPY_SCALAR_FIELD(ordered);  /* CDB */
	COPY_SCALAR_FIELD(trusted);  /* CDB */

	return newnode;
}

static DropStmt *
_copyDropStmt(DropStmt *from)
{
	DropStmt   *newnode = makeNode(DropStmt);

	COPY_SCALAR_FIELD(removeType);
	COPY_NODE_FIELD(objects);
	COPY_SCALAR_FIELD(behavior);
	COPY_SCALAR_FIELD(missing_ok);
	COPY_SCALAR_FIELD(bAllowPartn);

	return newnode;
}

static TruncateStmt *
_copyTruncateStmt(TruncateStmt *from)
{
	TruncateStmt *newnode = makeNode(TruncateStmt);

	COPY_NODE_FIELD(relations);
	COPY_SCALAR_FIELD(behavior);

	return newnode;
}

static CommentStmt *
_copyCommentStmt(CommentStmt *from)
{
	CommentStmt *newnode = makeNode(CommentStmt);

	COPY_SCALAR_FIELD(objtype);
	COPY_NODE_FIELD(objname);
	COPY_NODE_FIELD(objargs);
	COPY_STRING_FIELD(comment);

	return newnode;
}

static FetchStmt *
_copyFetchStmt(FetchStmt *from)
{
	FetchStmt  *newnode = makeNode(FetchStmt);

	COPY_SCALAR_FIELD(direction);
	COPY_SCALAR_FIELD(howMany);
	COPY_STRING_FIELD(portalname);
	COPY_SCALAR_FIELD(ismove);

	return newnode;
}

static IndexStmt *
_copyIndexStmt(IndexStmt *from)
{
	IndexStmt  *newnode = makeNode(IndexStmt);

	COPY_STRING_FIELD(idxname);
	COPY_NODE_FIELD(relation);
	COPY_STRING_FIELD(accessMethod);
	COPY_STRING_FIELD(tableSpace);
	COPY_NODE_FIELD(indexParams);
	COPY_NODE_FIELD(options);
	COPY_NODE_FIELD(whereClause);
	COPY_NODE_FIELD(rangetable);
	COPY_SCALAR_FIELD(is_part_child);
	COPY_SCALAR_FIELD(unique);
	COPY_SCALAR_FIELD(primary);
	COPY_SCALAR_FIELD(isconstraint);
	COPY_STRING_FIELD(altconname);
	COPY_SCALAR_FIELD(constrOid);
	COPY_SCALAR_FIELD(concurrent);
	COPY_NODE_FIELD(idxOids);
	COPY_SCALAR_FIELD(do_part);

	return newnode;
}

static CreateFunctionStmt *
_copyCreateFunctionStmt(CreateFunctionStmt *from)
{
	CreateFunctionStmt *newnode = makeNode(CreateFunctionStmt);

	COPY_SCALAR_FIELD(replace);
	COPY_NODE_FIELD(funcname);
	COPY_NODE_FIELD(parameters);
	COPY_NODE_FIELD(returnType);
	COPY_NODE_FIELD(options);
	COPY_NODE_FIELD(withClause);
	COPY_SCALAR_FIELD(funcOid);
	COPY_SCALAR_FIELD(shelltypeOid);

	return newnode;
}

static FunctionParameter *
_copyFunctionParameter(FunctionParameter *from)
{
	FunctionParameter *newnode = makeNode(FunctionParameter);

	COPY_STRING_FIELD(name);
	COPY_NODE_FIELD(argType);
	COPY_SCALAR_FIELD(mode);

	return newnode;
}

static AlterFunctionStmt *
_copyAlterFunctionStmt(AlterFunctionStmt *from)
{
	AlterFunctionStmt *newnode = makeNode(AlterFunctionStmt);

	COPY_NODE_FIELD(func);
	COPY_NODE_FIELD(actions);

	return newnode;
}

static RemoveFuncStmt *
_copyRemoveFuncStmt(RemoveFuncStmt *from)
{
	RemoveFuncStmt *newnode = makeNode(RemoveFuncStmt);

	COPY_SCALAR_FIELD(kind);
	COPY_NODE_FIELD(name);
	COPY_NODE_FIELD(args);
	COPY_SCALAR_FIELD(behavior);
	COPY_SCALAR_FIELD(missing_ok);

	return newnode;
}

static RemoveOpClassStmt *
_copyRemoveOpClassStmt(RemoveOpClassStmt *from)
{
	RemoveOpClassStmt *newnode = makeNode(RemoveOpClassStmt);

	COPY_NODE_FIELD(opclassname);
	COPY_STRING_FIELD(amname);
	COPY_SCALAR_FIELD(behavior);
	COPY_SCALAR_FIELD(missing_ok);

	return newnode;
}

static RenameStmt *
_copyRenameStmt(RenameStmt *from)
{
	RenameStmt *newnode = makeNode(RenameStmt);

	COPY_SCALAR_FIELD(renameType);
	COPY_NODE_FIELD(relation);
	COPY_SCALAR_FIELD(objid);
	COPY_NODE_FIELD(object);
	COPY_NODE_FIELD(objarg);
	COPY_STRING_FIELD(subname);
	COPY_STRING_FIELD(newname);
	COPY_SCALAR_FIELD(bAllowPartn);

	return newnode;
}

static AlterObjectSchemaStmt *
_copyAlterObjectSchemaStmt(AlterObjectSchemaStmt *from)
{
	AlterObjectSchemaStmt *newnode = makeNode(AlterObjectSchemaStmt);

	COPY_SCALAR_FIELD(objectType);
	COPY_NODE_FIELD(relation);
	COPY_NODE_FIELD(object);
	COPY_NODE_FIELD(objarg);
	COPY_STRING_FIELD(addname);
	COPY_STRING_FIELD(newschema);

	return newnode;
}

static AlterOwnerStmt *
_copyAlterOwnerStmt(AlterOwnerStmt *from)
{
	AlterOwnerStmt *newnode = makeNode(AlterOwnerStmt);

	COPY_SCALAR_FIELD(objectType);
	COPY_NODE_FIELD(relation);
	COPY_NODE_FIELD(object);
	COPY_NODE_FIELD(objarg);
	COPY_STRING_FIELD(addname);
	COPY_STRING_FIELD(newowner);

	return newnode;
}

static RuleStmt *
_copyRuleStmt(RuleStmt *from)
{
	RuleStmt   *newnode = makeNode(RuleStmt);

	COPY_NODE_FIELD(relation);
	COPY_STRING_FIELD(rulename);
	COPY_NODE_FIELD(whereClause);
	COPY_SCALAR_FIELD(event);
	COPY_SCALAR_FIELD(instead);
	COPY_NODE_FIELD(actions);
	COPY_SCALAR_FIELD(replace);
	COPY_SCALAR_FIELD(ruleOid);

	return newnode;
}

static NotifyStmt *
_copyNotifyStmt(NotifyStmt *from)
{
	NotifyStmt *newnode = makeNode(NotifyStmt);

	COPY_NODE_FIELD(relation);

	return newnode;
}

static ListenStmt *
_copyListenStmt(ListenStmt *from)
{
	ListenStmt *newnode = makeNode(ListenStmt);

	COPY_NODE_FIELD(relation);

	return newnode;
}

static UnlistenStmt *
_copyUnlistenStmt(UnlistenStmt *from)
{
	UnlistenStmt *newnode = makeNode(UnlistenStmt);

	COPY_NODE_FIELD(relation);

	return newnode;
}

static TransactionStmt *
_copyTransactionStmt(TransactionStmt *from)
{
	TransactionStmt *newnode = makeNode(TransactionStmt);

	COPY_SCALAR_FIELD(kind);
	COPY_NODE_FIELD(options);
	COPY_STRING_FIELD(gid);

	return newnode;
}

static CompositeTypeStmt *
_copyCompositeTypeStmt(CompositeTypeStmt *from)
{
	CompositeTypeStmt *newnode = makeNode(CompositeTypeStmt);

	COPY_NODE_FIELD(typevar);
	COPY_NODE_FIELD(coldeflist);
	COPY_SCALAR_FIELD(relOid);
	COPY_SCALAR_FIELD(comptypeOid);

	return newnode;
}

static ViewStmt *
_copyViewStmt(ViewStmt *from)
{
	ViewStmt   *newnode = makeNode(ViewStmt);

	COPY_NODE_FIELD(view);
	COPY_NODE_FIELD(aliases);
	COPY_NODE_FIELD(query);
	COPY_SCALAR_FIELD(replace);
	COPY_SCALAR_FIELD(relOid);
	COPY_SCALAR_FIELD(comptypeOid);
	COPY_SCALAR_FIELD(rewriteOid);

	return newnode;
}

static LoadStmt *
_copyLoadStmt(LoadStmt *from)
{
	LoadStmt   *newnode = makeNode(LoadStmt);

	COPY_STRING_FIELD(filename);

	return newnode;
}

static CreateDomainStmt *
_copyCreateDomainStmt(CreateDomainStmt *from)
{
	CreateDomainStmt *newnode = makeNode(CreateDomainStmt);

	COPY_NODE_FIELD(domainname);
	COPY_NODE_FIELD(typname);
	COPY_NODE_FIELD(constraints);
	COPY_SCALAR_FIELD(domainOid);

	return newnode;
}

static CreateOpClassStmt *
_copyCreateOpClassStmt(CreateOpClassStmt *from)
{
	CreateOpClassStmt *newnode = makeNode(CreateOpClassStmt);

	COPY_NODE_FIELD(opclassname);
	COPY_STRING_FIELD(amname);
	COPY_NODE_FIELD(datatype);
	COPY_NODE_FIELD(items);
	COPY_SCALAR_FIELD(isDefault);

	return newnode;
}

static CreateOpClassItem *
_copyCreateOpClassItem(CreateOpClassItem *from)
{
	CreateOpClassItem *newnode = makeNode(CreateOpClassItem);

	COPY_SCALAR_FIELD(itemtype);
	COPY_NODE_FIELD(name);
	COPY_NODE_FIELD(args);
	COPY_SCALAR_FIELD(number);
	COPY_SCALAR_FIELD(recheck);
	COPY_NODE_FIELD(storedtype);

	return newnode;
}

static CreatedbStmt *
_copyCreatedbStmt(CreatedbStmt *from)
{
	CreatedbStmt *newnode = makeNode(CreatedbStmt);

	COPY_STRING_FIELD(dbname);
	COPY_NODE_FIELD(options);
	COPY_SCALAR_FIELD(dbOid);

	return newnode;
}

static AlterDatabaseStmt *
_copyAlterDatabaseStmt(AlterDatabaseStmt *from)
{
	AlterDatabaseStmt *newnode = makeNode(AlterDatabaseStmt);

	COPY_STRING_FIELD(dbname);
	COPY_NODE_FIELD(options);

	return newnode;
}

static AlterDatabaseSetStmt *
_copyAlterDatabaseSetStmt(AlterDatabaseSetStmt *from)
{
	AlterDatabaseSetStmt *newnode = makeNode(AlterDatabaseSetStmt);

	COPY_STRING_FIELD(dbname);
	COPY_STRING_FIELD(variable);
	COPY_NODE_FIELD(value);

	return newnode;
}

static DropdbStmt *
_copyDropdbStmt(DropdbStmt *from)
{
	DropdbStmt *newnode = makeNode(DropdbStmt);

	COPY_STRING_FIELD(dbname);
	COPY_SCALAR_FIELD(missing_ok);

	return newnode;
}

static VacuumStmt *
_copyVacuumStmt(VacuumStmt *from)
{
	VacuumStmt *newnode = makeNode(VacuumStmt);

	COPY_SCALAR_FIELD(vacuum);
	COPY_SCALAR_FIELD(full);
	COPY_SCALAR_FIELD(analyze);
	COPY_SCALAR_FIELD(verbose);
	COPY_SCALAR_FIELD(rootonly);
	COPY_SCALAR_FIELD(freeze_min_age);
	COPY_NODE_FIELD(relation);
	COPY_NODE_FIELD(va_cols);
	COPY_NODE_FIELD(expanded_relids);
	COPY_NODE_FIELD(extra_oids);

	return newnode;
}

static ExplainStmt *
_copyExplainStmt(ExplainStmt *from)
{
	ExplainStmt *newnode = makeNode(ExplainStmt);

	COPY_NODE_FIELD(query);
	COPY_SCALAR_FIELD(verbose);
	COPY_SCALAR_FIELD(analyze);

	return newnode;
}

static CreateSeqStmt *
_copyCreateSeqStmt(CreateSeqStmt *from)
{
	CreateSeqStmt *newnode = makeNode(CreateSeqStmt);

	COPY_NODE_FIELD(sequence);
	COPY_NODE_FIELD(options);
	COPY_SCALAR_FIELD(relOid);
	COPY_SCALAR_FIELD(comptypeOid);

	return newnode;
}

static AlterSeqStmt *
_copyAlterSeqStmt(AlterSeqStmt *from)
{
	AlterSeqStmt *newnode = makeNode(AlterSeqStmt);

	COPY_NODE_FIELD(sequence);
	COPY_NODE_FIELD(options);

	return newnode;
}

static VariableSetStmt *
_copyVariableSetStmt(VariableSetStmt *from)
{
	VariableSetStmt *newnode = makeNode(VariableSetStmt);

	COPY_STRING_FIELD(name);
	COPY_NODE_FIELD(args);
	COPY_SCALAR_FIELD(is_local);

	return newnode;
}

static VariableShowStmt *
_copyVariableShowStmt(VariableShowStmt *from)
{
	VariableShowStmt *newnode = makeNode(VariableShowStmt);

	COPY_STRING_FIELD(name);

	return newnode;
}

static VariableResetStmt *
_copyVariableResetStmt(VariableResetStmt *from)
{
	VariableResetStmt *newnode = makeNode(VariableResetStmt);

	COPY_STRING_FIELD(name);

	return newnode;
}

static CreateFileSpaceStmt *
_copyCreateFileSpaceStmt(CreateFileSpaceStmt *from)
{
	CreateFileSpaceStmt *newnode = makeNode(CreateFileSpaceStmt);

	COPY_STRING_FIELD(filespacename);
	COPY_STRING_FIELD(owner);
	COPY_STRING_FIELD(fsysname);
	COPY_STRING_FIELD(location);
	COPY_NODE_FIELD(options);

	return newnode;
}

static CreateTableSpaceStmt *
_copyCreateTableSpaceStmt(CreateTableSpaceStmt *from)
{
	CreateTableSpaceStmt *newnode = makeNode(CreateTableSpaceStmt);

	COPY_STRING_FIELD(tablespacename);
	COPY_STRING_FIELD(owner);
	COPY_STRING_FIELD(filespacename);
	COPY_SCALAR_FIELD(tsoid);

	return newnode;
}

static CreateFdwStmt *
_copyCreateFdwStmt(CreateFdwStmt *from)
{
	CreateFdwStmt *newnode = makeNode(CreateFdwStmt);

	COPY_STRING_FIELD(fdwname);
	COPY_NODE_FIELD(validator);
	COPY_NODE_FIELD(options);

	return newnode;
}

static AlterFdwStmt *
_copyAlterFdwStmt(AlterFdwStmt *from)
{
	AlterFdwStmt *newnode = makeNode(AlterFdwStmt);

	COPY_STRING_FIELD(fdwname);
	COPY_NODE_FIELD(validator);
	COPY_SCALAR_FIELD(change_validator);
	COPY_NODE_FIELD(options);

	return newnode;
}

static DropFdwStmt *
_copyDropFdwStmt(DropFdwStmt *from)
{
	DropFdwStmt *newnode = makeNode(DropFdwStmt);

	COPY_STRING_FIELD(fdwname);
	COPY_SCALAR_FIELD(missing_ok);
	COPY_SCALAR_FIELD(behavior);

	return newnode;
}

static CreateForeignServerStmt *
_copyCreateForeignServerStmt(CreateForeignServerStmt *from)
{
	CreateForeignServerStmt *newnode = makeNode(CreateForeignServerStmt);

	COPY_STRING_FIELD(servername);
	COPY_STRING_FIELD(servertype);
	COPY_STRING_FIELD(version);
	COPY_STRING_FIELD(fdwname);
	COPY_NODE_FIELD(options);

	return newnode;
}

static AlterForeignServerStmt *
_copyAlterForeignServerStmt(AlterForeignServerStmt *from)
{
	AlterForeignServerStmt *newnode = makeNode(AlterForeignServerStmt);

	COPY_STRING_FIELD(servername);
	COPY_STRING_FIELD(version);
	COPY_NODE_FIELD(options);
	COPY_SCALAR_FIELD(has_version);

	return newnode;
}

static DropForeignServerStmt *
_copyDropForeignServerStmt(DropForeignServerStmt *from)
{
	DropForeignServerStmt *newnode = makeNode(DropForeignServerStmt);

	COPY_STRING_FIELD(servername);
	COPY_SCALAR_FIELD(missing_ok);
	COPY_SCALAR_FIELD(behavior);

	return newnode;
}

static CreateUserMappingStmt *
_copyCreateUserMappingStmt(CreateUserMappingStmt *from)
{
	CreateUserMappingStmt *newnode = makeNode(CreateUserMappingStmt);

	COPY_STRING_FIELD(username);
	COPY_STRING_FIELD(servername);
	COPY_NODE_FIELD(options);

	return newnode;
}

static AlterUserMappingStmt *
_copyAlterUserMappingStmt(AlterUserMappingStmt *from)
{
	AlterUserMappingStmt *newnode = makeNode(AlterUserMappingStmt);

	COPY_STRING_FIELD(username);
	COPY_STRING_FIELD(servername);
	COPY_NODE_FIELD(options);

	return newnode;
}

static DropUserMappingStmt *
_copyDropUserMappingStmt(DropUserMappingStmt *from)
{
	DropUserMappingStmt *newnode = makeNode(DropUserMappingStmt);

	COPY_STRING_FIELD(username);
	COPY_STRING_FIELD(servername);
	COPY_SCALAR_FIELD(missing_ok);

	return newnode;
}

static CreateTrigStmt *
_copyCreateTrigStmt(CreateTrigStmt *from)
{
	CreateTrigStmt *newnode = makeNode(CreateTrigStmt);

	COPY_STRING_FIELD(trigname);
	COPY_NODE_FIELD(relation);
	COPY_NODE_FIELD(funcname);
	COPY_NODE_FIELD(args);
	COPY_SCALAR_FIELD(before);
	COPY_SCALAR_FIELD(row);
	strcpy(newnode->actions, from->actions);	/* in-line string field */
	COPY_SCALAR_FIELD(isconstraint);
	COPY_SCALAR_FIELD(deferrable);
	COPY_SCALAR_FIELD(initdeferred);
	COPY_NODE_FIELD(constrrel);

	return newnode;
}

static DropPropertyStmt *
_copyDropPropertyStmt(DropPropertyStmt *from)
{
	DropPropertyStmt *newnode = makeNode(DropPropertyStmt);

	COPY_NODE_FIELD(relation);
	COPY_STRING_FIELD(property);
	COPY_SCALAR_FIELD(removeType);
	COPY_SCALAR_FIELD(behavior);
	COPY_SCALAR_FIELD(missing_ok);

	return newnode;
}

static CreatePLangStmt *
_copyCreatePLangStmt(CreatePLangStmt *from)
{
	CreatePLangStmt *newnode = makeNode(CreatePLangStmt);

	COPY_STRING_FIELD(plname);
	COPY_NODE_FIELD(plhandler);
	COPY_NODE_FIELD(plvalidator);
	COPY_SCALAR_FIELD(pltrusted);
	COPY_SCALAR_FIELD(plangOid);
	COPY_SCALAR_FIELD(plhandlerOid);
	COPY_SCALAR_FIELD(plvalidatorOid);

	return newnode;
}

static DropPLangStmt *
_copyDropPLangStmt(DropPLangStmt *from)
{
	DropPLangStmt *newnode = makeNode(DropPLangStmt);

	COPY_STRING_FIELD(plname);
	COPY_SCALAR_FIELD(behavior);
	COPY_SCALAR_FIELD(missing_ok);

	return newnode;
}

static CreateRoleStmt *
_copyCreateRoleStmt(CreateRoleStmt *from)
{
	CreateRoleStmt *newnode = makeNode(CreateRoleStmt);

	COPY_SCALAR_FIELD(stmt_type);
	COPY_STRING_FIELD(role);
	COPY_NODE_FIELD(options);
	COPY_SCALAR_FIELD(roleOid);

	return newnode;
}

static DenyLoginInterval *
_copyDenyLoginInterval(DenyLoginInterval *from)
{
	DenyLoginInterval *newnode = makeNode(DenyLoginInterval);

	COPY_NODE_FIELD(start);
	COPY_NODE_FIELD(end);

	return newnode;
}

static DenyLoginPoint *
_copyDenyLoginPoint(DenyLoginPoint *from)
{
	DenyLoginPoint *newnode = makeNode(DenyLoginPoint);

	COPY_NODE_FIELD(day);
	COPY_NODE_FIELD(time);

	return newnode;
}

static AlterRoleStmt *
_copyAlterRoleStmt(AlterRoleStmt *from)
{
	AlterRoleStmt *newnode = makeNode(AlterRoleStmt);

	COPY_STRING_FIELD(role);
	COPY_NODE_FIELD(options);
	COPY_SCALAR_FIELD(action);

	return newnode;
}

static AlterRoleSetStmt *
_copyAlterRoleSetStmt(AlterRoleSetStmt *from)
{
	AlterRoleSetStmt *newnode = makeNode(AlterRoleSetStmt);

	COPY_STRING_FIELD(role);
	COPY_STRING_FIELD(variable);
	COPY_NODE_FIELD(value);

	return newnode;
}

static DropRoleStmt *
_copyDropRoleStmt(DropRoleStmt *from)
{
	DropRoleStmt *newnode = makeNode(DropRoleStmt);

	COPY_NODE_FIELD(roles);
	COPY_SCALAR_FIELD(missing_ok);

	return newnode;
}

static LockStmt *
_copyLockStmt(LockStmt *from)
{
	LockStmt   *newnode = makeNode(LockStmt);

	COPY_NODE_FIELD(relations);
	COPY_SCALAR_FIELD(mode);
	COPY_SCALAR_FIELD(nowait);

	return newnode;
}

static ConstraintsSetStmt *
_copyConstraintsSetStmt(ConstraintsSetStmt *from)
{
	ConstraintsSetStmt *newnode = makeNode(ConstraintsSetStmt);

	COPY_NODE_FIELD(constraints);
	COPY_SCALAR_FIELD(deferred);

	return newnode;
}

static ReindexStmt *
_copyReindexStmt(ReindexStmt *from)
{
	ReindexStmt *newnode = makeNode(ReindexStmt);

	COPY_SCALAR_FIELD(kind);
	COPY_NODE_FIELD(relation);
	COPY_STRING_FIELD(name);
	COPY_SCALAR_FIELD(do_system);
	COPY_SCALAR_FIELD(do_user);
	COPY_NODE_FIELD(new_ind_oids);

	return newnode;
}

static CreateSchemaStmt *
_copyCreateSchemaStmt(CreateSchemaStmt *from)
{
	CreateSchemaStmt *newnode = makeNode(CreateSchemaStmt);

	COPY_STRING_FIELD(schemaname);
	COPY_STRING_FIELD(authid);
	COPY_NODE_FIELD(schemaElts);
	COPY_SCALAR_FIELD(istemp);
	COPY_SCALAR_FIELD(schemaOid);

	return newnode;
}

static CreateConversionStmt *
_copyCreateConversionStmt(CreateConversionStmt *from)
{
	CreateConversionStmt *newnode = makeNode(CreateConversionStmt);

	COPY_NODE_FIELD(conversion_name);
	COPY_STRING_FIELD(for_encoding_name);
	COPY_STRING_FIELD(to_encoding_name);
	COPY_NODE_FIELD(func_name);
	COPY_SCALAR_FIELD(def);

	return newnode;
}

static CreateCastStmt *
_copyCreateCastStmt(CreateCastStmt *from)
{
	CreateCastStmt *newnode = makeNode(CreateCastStmt);

	COPY_NODE_FIELD(sourcetype);
	COPY_NODE_FIELD(targettype);
	COPY_NODE_FIELD(func);
	COPY_SCALAR_FIELD(context);

	return newnode;
}

static DropCastStmt *
_copyDropCastStmt(DropCastStmt *from)
{
	DropCastStmt *newnode = makeNode(DropCastStmt);

	COPY_NODE_FIELD(sourcetype);
	COPY_NODE_FIELD(targettype);
	COPY_SCALAR_FIELD(behavior);
	COPY_SCALAR_FIELD(missing_ok);

	return newnode;
}

static PrepareStmt *
_copyPrepareStmt(PrepareStmt *from)
{
	PrepareStmt *newnode = makeNode(PrepareStmt);

	COPY_STRING_FIELD(name);
	COPY_NODE_FIELD(argtypes);
	COPY_NODE_FIELD(argtype_oids);
	COPY_NODE_FIELD(query);

	return newnode;
}

static ExecuteStmt *
_copyExecuteStmt(ExecuteStmt *from)
{
	ExecuteStmt *newnode = makeNode(ExecuteStmt);

	COPY_STRING_FIELD(name);
	COPY_NODE_FIELD(into);
	COPY_NODE_FIELD(params);

	return newnode;
}

static DeallocateStmt *
_copyDeallocateStmt(DeallocateStmt *from)
{
	DeallocateStmt *newnode = makeNode(DeallocateStmt);

	COPY_STRING_FIELD(name);

	return newnode;
}

static DropOwnedStmt *
_copyDropOwnedStmt(DropOwnedStmt *from)
{
	DropOwnedStmt *newnode = makeNode(DropOwnedStmt);

	COPY_NODE_FIELD(roles);
	COPY_SCALAR_FIELD(behavior);

	return newnode;
}

static ReassignOwnedStmt *
_copyReassignOwnedStmt(ReassignOwnedStmt *from)
{
	ReassignOwnedStmt *newnode = makeNode(ReassignOwnedStmt);

	COPY_NODE_FIELD(roles);
	COPY_SCALAR_FIELD(newrole);

	return newnode;
}



static CdbProcess *
_copyCdbProcess(CdbProcess *from)
{
	CdbProcess *newnode = makeNode(CdbProcess);

	COPY_STRING_FIELD(listenerAddr);
	COPY_SCALAR_FIELD(listenerPort);
	COPY_SCALAR_FIELD(pid);
	COPY_SCALAR_FIELD(contentid);

	return newnode;
}

static Slice *
_copySlice(Slice *from)
{
	Slice *newnode = makeNode(Slice);

	COPY_SCALAR_FIELD(sliceIndex);
	COPY_SCALAR_FIELD(rootIndex);
	COPY_SCALAR_FIELD(gangType);
	COPY_SCALAR_FIELD(gangSize);
	COPY_SCALAR_FIELD(numGangMembersToBeActive);
	COPY_SCALAR_FIELD(directDispatch.isDirectDispatch);
	COPY_NODE_FIELD(directDispatch.contentIds);

	COPY_SCALAR_FIELD(primary_gang_id);
	COPY_SCALAR_FIELD(parentIndex);
	COPY_NODE_FIELD(children);
	COPY_NODE_FIELD(primaryProcesses);

	return newnode;
}

static SliceTable *
_copySliceTable(SliceTable *from)
{
	SliceTable *newnode = makeNode(SliceTable);

	COPY_SCALAR_FIELD(nMotions);
	COPY_SCALAR_FIELD(nInitPlans);
	COPY_SCALAR_FIELD(localSlice);
	COPY_NODE_FIELD(slices);
    COPY_SCALAR_FIELD(doInstrument);
    COPY_SCALAR_FIELD(ic_instance_id);

	return newnode;
}



static CreateQueueStmt *
_copyCreateQueueStmt(CreateQueueStmt *from)
{
	CreateQueueStmt *newnode = makeNode(CreateQueueStmt);

	COPY_STRING_FIELD(queue);
	COPY_NODE_FIELD(options);

	return newnode;
}

static AlterQueueStmt *
_copyAlterQueueStmt(AlterQueueStmt *from)
{
	AlterQueueStmt *newnode = makeNode(AlterQueueStmt);

	COPY_STRING_FIELD(queue);
	COPY_NODE_FIELD(options);

	return newnode;
}

static DropQueueStmt *
_copyDropQueueStmt(DropQueueStmt *from)
{
	DropQueueStmt *newnode = makeNode(DropQueueStmt);

	COPY_STRING_FIELD(queue);

	return newnode;
}

static TableValueExpr *
_copyTableValueExpr(TableValueExpr *from)
{
	TableValueExpr *newnode = makeNode(TableValueExpr);

	COPY_NODE_FIELD(subquery);

	return newnode;
}

static AlterTypeStmt *
_copyAlterTypeStmt(AlterTypeStmt *from)
{
	AlterTypeStmt *newnode = makeNode(AlterTypeStmt);

	COPY_NODE_FIELD(typname);
	COPY_NODE_FIELD(encoding);

	return newnode;
}

static ResultRelSegFileInfo *
_copyResultRelSegFileInfo(ResultRelSegFileInfo *from)
{
  ResultRelSegFileInfo *newnode = makeNode(ResultRelSegFileInfo);

  COPY_SCALAR_FIELD(segno);
  COPY_SCALAR_FIELD(varblock);
  COPY_SCALAR_FIELD(tupcount);
  COPY_SCALAR_FIELD(numfiles);
  COPY_POINTER_FIELD(eof, from->numfiles * sizeof(uint64));
  COPY_POINTER_FIELD(uncompressed_eof, from->numfiles * sizeof(uint64));

  return newnode;
}

static SegFileSplitMapNode *
_copySegFileSplitMapNode(SegFileSplitMapNode *from)
{
  SegFileSplitMapNode *newnode = makeNode(SegFileSplitMapNode);

  COPY_SCALAR_FIELD(relid);
  COPY_NODE_FIELD(splits);

  return newnode;
}

static FileSplitNode *
_copyFileSplitNode(FileSplitNode *from)
{
  FileSplitNode *newnode = makeNode(FileSplitNode);

  COPY_SCALAR_FIELD(segno);
  COPY_SCALAR_FIELD(logiceof);
  COPY_SCALAR_FIELD(offsets);
  COPY_SCALAR_FIELD(lengths);

  return newnode;
}

static CaQLSelect *
_copyCaQLSelect(const CaQLSelect *from)
{
	CaQLSelect *newnode = makeNode(CaQLSelect);

	COPY_NODE_FIELD(targetlist);
	COPY_STRING_FIELD(from);
	COPY_NODE_FIELD(where);
	COPY_NODE_FIELD(orderby);
	COPY_SCALAR_FIELD(forupdate);
	COPY_SCALAR_FIELD(count);

	return newnode;
}

static CaQLInsert *
_copyCaQLInsert(const CaQLInsert *from)
{
	CaQLInsert *newnode = makeNode(CaQLInsert);

	COPY_STRING_FIELD(into);

	return newnode;
}

static CaQLDelete *
_copyCaQLDelete(const CaQLDelete *from)
{
	CaQLDelete *newnode = makeNode(CaQLDelete);

	COPY_STRING_FIELD(from);
	COPY_NODE_FIELD(where);

	return newnode;
}

static CaQLExpr *
_copyCaQLExpr(const CaQLExpr *from)
{
	CaQLExpr *newnode = makeNode(CaQLExpr);

	COPY_STRING_FIELD(left);
	COPY_STRING_FIELD(op);
	COPY_SCALAR_FIELD(right);
	COPY_SCALAR_FIELD(attnum);
	COPY_SCALAR_FIELD(strategy);
	COPY_SCALAR_FIELD(fnoid);
	COPY_SCALAR_FIELD(typid);

	return newnode;
}

static VirtualSegmentNode *
_copyVirtualSegmentNode(const VirtualSegmentNode *from)
{
	VirtualSegmentNode *newnode = makeNode(VirtualSegmentNode);
	newnode->hostname = pstrdup(from->hostname);

	return newnode;
}

/* ****************************************************************
 *					pg_list.h copy functions
 * ****************************************************************
 */

/*
 * Perform a deep copy of the specified list, using copyObject(). The
 * list MUST be of type T_List; T_IntList and T_OidList nodes don't
 * need deep copies, so they should be copied via list_copy()
 */
#define COPY_NODE_CELL(new, old)					\
	(new) = (ListCell *) palloc(sizeof(ListCell));	\
	lfirst(new) = copyObject(lfirst(old));

static List *
_copyList(List *from)
{
	List	   *new;
	ListCell   *curr_old;
	ListCell   *prev_new;

	Assert(list_length(from) >= 1);

	new = makeNode(List);
	new->length = from->length;

	COPY_NODE_CELL(new->head, from->head);
	prev_new = new->head;
	curr_old = lnext(from->head);

	while (curr_old)
	{
		COPY_NODE_CELL(prev_new->next, curr_old);
		prev_new = prev_new->next;
		curr_old = curr_old->next;
	}
	prev_new->next = NULL;
	new->tail = prev_new;

	return new;
}

/* ****************************************************************
 *					value.h copy functions
 * ****************************************************************
 */
static Value *
_copyValue(Value *from)
{
	Value	   *newnode = makeNode(Value);

	/* See also _copyAConst when changing this code! */

	COPY_SCALAR_FIELD(type);
	switch (from->type)
	{
		case T_Integer:
			COPY_SCALAR_FIELD(val.ival);
			break;
		case T_Float:
		case T_String:
		case T_BitString:
			COPY_STRING_FIELD(val.str);
			break;
		case T_Null:
			/* nothing to do */
			break;
		default:
			elog(ERROR, "unrecognized node type: %d",
				 (int) from->type);
			break;
	}
	return newnode;
}

/*
 * copyObject
 *
 * Create a copy of a Node tree or list.  This is a "deep" copy: all
 * substructure is copied too, recursively.
 */
void *
copyObject(void *from)
{
	void	   *retval;

	if (from == NULL)
		return NULL;

	switch (nodeTag(from))
	{
			/*
			 * PLAN NODES
			 */
		case T_PlannedStmt:
			retval = _copyPlannedStmt(from);
			break;
		case T_Plan:
			retval = _copyPlan(from);
			break;
		case T_Result:
			retval = _copyResult(from);
			break;
		case T_Repeat:
			retval = _copyRepeat(from);
			break;
		case T_Append:
			retval = _copyAppend(from);
			break;
		case T_Sequence:
			retval = _copySequence(from);
			break;
		case T_BitmapAnd:
			retval = _copyBitmapAnd(from);
			break;
		case T_BitmapOr:
			retval = _copyBitmapOr(from);
			break;
		case T_Scan:
			retval = _copyScan(from);
			break;
		case T_SeqScan:
			retval = _copySeqScan(from);
			break;
		case T_AppendOnlyScan:
			retval = _copyAppendOnlyScan(from);
			break;
		case T_TableScan:
			retval = _copyTableScan(from);
			break;
		case T_DynamicTableScan:
			retval = _copyDynamicTableScan(from);
			break;
		case T_ParquetScan:
			retval = _copyParquetScan(from);
			break;
		case T_ExternalScan:
			retval = _copyExternalScan(from);
			break;
		case T_IndexScan:
			retval = _copyIndexScan(from);
			break;
		case T_DynamicIndexScan:
			retval = _copyDynamicIndexScan(from);
			break;
		case T_BitmapIndexScan:
			retval = _copyBitmapIndexScan(from);
			break;
		case T_BitmapHeapScan:
			retval = _copyBitmapHeapScan(from);
			break;
		case T_BitmapTableScan:
			retval = _copyBitmapTableScan(from);
			break;
		case T_TidScan:
			retval = _copyTidScan(from);
			break;
		case T_SubqueryScan:
			retval = _copySubqueryScan(from);
			break;
		case T_FunctionScan:
			retval = _copyFunctionScan(from);
			break;
		case T_ValuesScan:
			retval = _copyValuesScan(from);
			break;
		case T_Join:
			retval = _copyJoin(from);
			break;
		case T_NestLoop:
			retval = _copyNestLoop(from);
			break;
		case T_MergeJoin:
			retval = _copyMergeJoin(from);
			break;
		case T_HashJoin:
			retval = _copyHashJoin(from);
			break;
		case T_ShareInputScan:
			retval = _copyShareInputScan(from);
			break;
		case T_Material:
			retval = _copyMaterial(from);
			break;
		case T_Sort:
			retval = _copySort(from);
			break;
		case T_Agg:
			retval = _copyAgg(from);
			break;
		case T_WindowKey:
			retval = _copyWindowKey(from);
			break;
		case T_Window:
			retval = _copyWindow(from);
			break;
		case T_TableFunctionScan:
			retval = _copyTableFunctionScan(from);
			break;
		case T_Unique:
			retval = _copyUnique(from);
			break;
		case T_Hash:
			retval = _copyHash(from);
			break;
		case T_SetOp:
			retval = _copySetOp(from);
			break;
		case T_Limit:
			retval = _copyLimit(from);
			break;
		case T_Motion:
			retval = _copyMotion(from);
			break;
		case T_DML:
			retval = _copyDML(from);
			break;
		case T_SplitUpdate:
			retval = _copySplitUpdate(from);
			break;
		case T_RowTrigger:
			retval = _copyRowTrigger(from);
			break;
		case T_AssertOp:
			retval = _copyAssertOp(from);
			break;
		case T_PartitionSelector:
			retval = _copyPartitionSelector(from);
			break;

			/*
			 * PRIMITIVE NODES
			 */
		case T_Alias:
			retval = _copyAlias(from);
			break;
		case T_RangeVar:
			retval = _copyRangeVar(from);
			break;
		case T_IntoClause:
			retval = _copyIntoClause(from);
			break;
		case T_Var:
			retval = _copyVar(from);
			break;
		case T_Const:
			retval = _copyConst(from);
			break;
		case T_Param:
			retval = _copyParam(from);
			break;
		case T_Aggref:
			retval = _copyAggref(from);
			break;
		case T_AggOrder:
			retval = _copyAggOrder(from);
			break;
		case T_WindowRef:
			retval = _copyWindowRef(from);
			break;
		case T_ArrayRef:
			retval = _copyArrayRef(from);
			break;
		case T_FuncExpr:
			retval = _copyFuncExpr(from);
			break;
		case T_OpExpr:
			retval = _copyOpExpr(from);
			break;
		case T_DistinctExpr:
			retval = _copyDistinctExpr(from);
			break;
		case T_ScalarArrayOpExpr:
			retval = _copyScalarArrayOpExpr(from);
			break;
		case T_BoolExpr:
			retval = _copyBoolExpr(from);
			break;
		case T_SubLink:
			retval = _copySubLink(from);
			break;
		case T_SubPlan:
			retval = _copySubPlan(from);
			break;
		case T_FieldSelect:
			retval = _copyFieldSelect(from);
			break;
		case T_FieldStore:
			retval = _copyFieldStore(from);
			break;
		case T_RelabelType:
			retval = _copyRelabelType(from);
			break;
		case T_ConvertRowtypeExpr:
			retval = _copyConvertRowtypeExpr(from);
			break;
		case T_CaseExpr:
			retval = _copyCaseExpr(from);
			break;
		case T_CaseWhen:
			retval = _copyCaseWhen(from);
			break;
		case T_CaseTestExpr:
			retval = _copyCaseTestExpr(from);
			break;
		case T_ArrayExpr:
			retval = _copyArrayExpr(from);
			break;
		case T_RowExpr:
			retval = _copyRowExpr(from);
			break;
		case T_RowCompareExpr:
			retval = _copyRowCompareExpr(from);
			break;
		case T_CoalesceExpr:
			retval = _copyCoalesceExpr(from);
			break;
		case T_MinMaxExpr:
			retval = _copyMinMaxExpr(from);
			break;
		case T_NullIfExpr:
			retval = _copyNullIfExpr(from);
			break;
		case T_NullTest:
			retval = _copyNullTest(from);
			break;
		case T_BooleanTest:
			retval = _copyBooleanTest(from);
			break;
		case T_CoerceToDomain:
			retval = _copyCoerceToDomain(from);
			break;
		case T_CoerceToDomainValue:
			retval = _copyCoerceToDomainValue(from);
			break;
		case T_SetToDefault:
			retval = _copySetToDefault(from);
			break;
		case T_CurrentOfExpr:
			retval = _copyCurrentOfExpr(from);
			break;
		case T_TargetEntry:
			retval = _copyTargetEntry(from);
			break;
		case T_RangeTblRef:
			retval = _copyRangeTblRef(from);
			break;
		case T_JoinExpr:
			retval = _copyJoinExpr(from);
			break;
		case T_FromExpr:
			retval = _copyFromExpr(from);
			break;
		case T_Flow:
			retval = _copyFlow(from);
			break;

			/*
			 * RELATION NODES
			 */
		case T_CdbRelColumnInfo:
			retval = _copyCdbRelColumnInfo(from);
			break;
		case T_PathKeyItem:
			retval = _copyPathKeyItem(from);
			break;
		case T_RestrictInfo:
			retval = _copyRestrictInfo(from);
			break;
		case T_OuterJoinInfo:
			retval = _copyOuterJoinInfo(from);
			break;
		case T_InClauseInfo:
			retval = _copyInClauseInfo(from);
			break;
		case T_AppendRelInfo:
			retval = _copyAppendRelInfo(from);
			break;

			/*
			 * VALUE NODES
			 */
		case T_Integer:
		case T_Float:
		case T_String:
		case T_BitString:
		case T_Null:
			retval = _copyValue(from);
			break;

			/*
			 * LIST NODES
			 */
		case T_List:
			retval = _copyList(from);
			break;

			/*
			 * Lists of integers and OIDs don't need to be deep-copied, so we
			 * perform a shallow copy via list_copy()
			 */
		case T_IntList:
		case T_OidList:
			retval = list_copy(from);
			break;

			/*
			 * PARSE NODES
			 */
		case T_Query:
			retval = _copyQuery(from);
			break;
		case T_InsertStmt:
			retval = _copyInsertStmt(from);
			break;
		case T_DeleteStmt:
			retval = _copyDeleteStmt(from);
			break;
		case T_UpdateStmt:
			retval = _copyUpdateStmt(from);
			break;
		case T_SelectStmt:
			retval = _copySelectStmt(from);
			break;
		case T_SetOperationStmt:
			retval = _copySetOperationStmt(from);
			break;
		case T_AlterTableStmt:
			retval = _copyAlterTableStmt(from);
			break;
		case T_AlterTableCmd:
			retval = _copyAlterTableCmd(from);
			break;
		case T_InheritPartitionCmd:
			retval = _copyInheritPartitionCmd(from);
			break;
		case T_AlterPartitionCmd:
			retval = _copyAlterPartitionCmd(from);
			break;
		case T_AlterPartitionId:
			retval = _copyAlterPartitionId(from);
			break;
		case T_AlterDomainStmt:
			retval = _copyAlterDomainStmt(from);
			break;
		case T_GrantStmt:
			retval = _copyGrantStmt(from);
			break;
		case T_GrantRoleStmt:
			retval = _copyGrantRoleStmt(from);
			break;
		case T_DeclareCursorStmt:
			retval = _copyDeclareCursorStmt(from);
			break;
		case T_ClosePortalStmt:
			retval = _copyClosePortalStmt(from);
			break;
		case T_ClusterStmt:
			retval = _copyClusterStmt(from);
			break;
		case T_SingleRowErrorDesc:
			retval = _copySingleRowErrorDesc(from);
			break;
		case T_CopyStmt:
			retval = _copyCopyStmt(from);
			break;
		case T_CreateStmt:
			retval = _copyCreateStmt(from);
			break;
		case T_PartitionBy:
			retval = _copyPartitionBy(from);
			break;
		case T_PartitionSpec:
			retval = _copyPartitionSpec(from);
			break;
		case T_PartitionValuesSpec:
			retval = _copyPartitionValuesSpec(from);
			break;
		case T_PartitionElem:
			retval = _copyPartitionElem(from);
			break;
		case T_PartitionRangeItem:
			retval = _copyPartitionRangeItem(from);
			break;
		case T_PartitionBoundSpec:
			retval = _copyPartitionBoundSpec(from);
			break;
		case T_PgPartRule:
			retval = _copyPgPartRule(from);
			break;
		case T_PartitionNode:
			retval = _copyPartitionNode(from);
			break;
		case T_Partition:
			retval = _copyPartition(from);
			break;
		case T_PartitionRule:
			retval = _copyPartitionRule(from);
			break;
		case T_ExtTableTypeDesc:
			retval = _copyExtTableTypeDesc(from);
			break;
		case T_CreateExternalStmt:
			retval = _copyCreateExternalStmt(from);
			break;
		case T_CreateForeignStmt:
			retval = _copyCreateForeignStmt(from);
			break;			
		case T_InhRelation:
			retval = _copyInhRelation(from);
			break;
		case T_DefineStmt:
			retval = _copyDefineStmt(from);
			break;
		case T_DropStmt:
			retval = _copyDropStmt(from);
			break;
		case T_TruncateStmt:
			retval = _copyTruncateStmt(from);
			break;
		case T_CommentStmt:
			retval = _copyCommentStmt(from);
			break;
		case T_FetchStmt:
			retval = _copyFetchStmt(from);
			break;
		case T_IndexStmt:
			retval = _copyIndexStmt(from);
			break;
		case T_CreateFunctionStmt:
			retval = _copyCreateFunctionStmt(from);
			break;
		case T_FunctionParameter:
			retval = _copyFunctionParameter(from);
			break;
		case T_AlterFunctionStmt:
			retval = _copyAlterFunctionStmt(from);
			break;
		case T_RemoveFuncStmt:
			retval = _copyRemoveFuncStmt(from);
			break;
		case T_RemoveOpClassStmt:
			retval = _copyRemoveOpClassStmt(from);
			break;
		case T_RenameStmt:
			retval = _copyRenameStmt(from);
			break;
		case T_AlterObjectSchemaStmt:
			retval = _copyAlterObjectSchemaStmt(from);
			break;
		case T_AlterOwnerStmt:
			retval = _copyAlterOwnerStmt(from);
			break;
		case T_RuleStmt:
			retval = _copyRuleStmt(from);
			break;
		case T_NotifyStmt:
			retval = _copyNotifyStmt(from);
			break;
		case T_ListenStmt:
			retval = _copyListenStmt(from);
			break;
		case T_UnlistenStmt:
			retval = _copyUnlistenStmt(from);
			break;
		case T_TransactionStmt:
			retval = _copyTransactionStmt(from);
			break;
		case T_CompositeTypeStmt:
			retval = _copyCompositeTypeStmt(from);
			break;
		case T_ViewStmt:
			retval = _copyViewStmt(from);
			break;
		case T_LoadStmt:
			retval = _copyLoadStmt(from);
			break;
		case T_CreateDomainStmt:
			retval = _copyCreateDomainStmt(from);
			break;
		case T_CreateOpClassStmt:
			retval = _copyCreateOpClassStmt(from);
			break;
		case T_CreateOpClassItem:
			retval = _copyCreateOpClassItem(from);
			break;
		case T_CreatedbStmt:
			retval = _copyCreatedbStmt(from);
			break;
		case T_AlterDatabaseStmt:
			retval = _copyAlterDatabaseStmt(from);
			break;
		case T_AlterDatabaseSetStmt:
			retval = _copyAlterDatabaseSetStmt(from);
			break;
		case T_DropdbStmt:
			retval = _copyDropdbStmt(from);
			break;
		case T_VacuumStmt:
			retval = _copyVacuumStmt(from);
			break;
		case T_ExplainStmt:
			retval = _copyExplainStmt(from);
			break;
		case T_CreateSeqStmt:
			retval = _copyCreateSeqStmt(from);
			break;
		case T_AlterSeqStmt:
			retval = _copyAlterSeqStmt(from);
			break;
		case T_VariableSetStmt:
			retval = _copyVariableSetStmt(from);
			break;
		case T_VariableShowStmt:
			retval = _copyVariableShowStmt(from);
			break;
		case T_VariableResetStmt:
			retval = _copyVariableResetStmt(from);
			break;
		case T_CreateFileSpaceStmt:
			retval = _copyCreateFileSpaceStmt(from);
			break;
		case T_CreateTableSpaceStmt:
			retval = _copyCreateTableSpaceStmt(from);
			break;
		case T_CreateFdwStmt:
			retval = _copyCreateFdwStmt(from);
			break;
		case T_AlterFdwStmt:
			retval = _copyAlterFdwStmt(from);
			break;
		case T_DropFdwStmt:
			retval = _copyDropFdwStmt(from);
			break;
		case T_CreateForeignServerStmt:
			retval = _copyCreateForeignServerStmt(from);
			break;
		case T_AlterForeignServerStmt:
			retval = _copyAlterForeignServerStmt(from);
			break;
		case T_DropForeignServerStmt:
			retval = _copyDropForeignServerStmt(from);
			break;
		case T_CreateUserMappingStmt:
			retval = _copyCreateUserMappingStmt(from);
			break;
		case T_AlterUserMappingStmt:
			retval = _copyAlterUserMappingStmt(from);
			break;
		case T_DropUserMappingStmt:
			retval = _copyDropUserMappingStmt(from);
			break;
		case T_CreateTrigStmt:
			retval = _copyCreateTrigStmt(from);
			break;
		case T_DropPropertyStmt:
			retval = _copyDropPropertyStmt(from);
			break;
		case T_CreatePLangStmt:
			retval = _copyCreatePLangStmt(from);
			break;
		case T_DropPLangStmt:
			retval = _copyDropPLangStmt(from);
			break;
		case T_CreateRoleStmt:
			retval = _copyCreateRoleStmt(from);
			break;
		case T_AlterRoleStmt:
			retval = _copyAlterRoleStmt(from);
			break;
		case T_AlterRoleSetStmt:
			retval = _copyAlterRoleSetStmt(from);
			break;
		case T_DropRoleStmt:
			retval = _copyDropRoleStmt(from);
			break;
		case T_LockStmt:
			retval = _copyLockStmt(from);
			break;
		case T_ConstraintsSetStmt:
			retval = _copyConstraintsSetStmt(from);
			break;
		case T_ReindexStmt:
			retval = _copyReindexStmt(from);
			break;
		case T_CheckPointStmt:
			retval = (void *) makeNode(CheckPointStmt);
			break;
		case T_CreateSchemaStmt:
			retval = _copyCreateSchemaStmt(from);
			break;
		case T_CreateConversionStmt:
			retval = _copyCreateConversionStmt(from);
			break;
		case T_CreateCastStmt:
			retval = _copyCreateCastStmt(from);
			break;
		case T_DropCastStmt:
			retval = _copyDropCastStmt(from);
			break;
		case T_PrepareStmt:
			retval = _copyPrepareStmt(from);
			break;
		case T_ExecuteStmt:
			retval = _copyExecuteStmt(from);
			break;
		case T_DeallocateStmt:
			retval = _copyDeallocateStmt(from);
			break;
		case T_DropOwnedStmt:
			retval = _copyDropOwnedStmt(from);
			break;
		case T_ReassignOwnedStmt:
			retval = _copyReassignOwnedStmt(from);
			break;

		case T_CreateQueueStmt:
			retval = _copyCreateQueueStmt(from);
			break;
		case T_AlterQueueStmt:
			retval = _copyAlterQueueStmt(from);
			break;
		case T_DropQueueStmt:
			retval = _copyDropQueueStmt(from);
			break;
		case T_A_Expr:
			retval = _copyAExpr(from);
			break;
		case T_ColumnRef:
			retval = _copyColumnRef(from);
			break;
		case T_ParamRef:
			retval = _copyParamRef(from);
			break;
		case T_A_Const:
			retval = _copyAConst(from);
			break;
		case T_FuncCall:
			retval = _copyFuncCall(from);
			break;
		case T_A_Indices:
			retval = _copyAIndices(from);
			break;
		case T_A_Indirection:
			retval = _copyA_Indirection(from);
			break;
		case T_ResTarget:
			retval = _copyResTarget(from);
			break;
		case T_TypeCast:
			retval = _copyTypeCast(from);
			break;
		case T_SortBy:
			retval = _copySortBy(from);
			break;
		case T_RangeSubselect:
			retval = _copyRangeSubselect(from);
			break;
		case T_RangeFunction:
			retval = _copyRangeFunction(from);
			break;
		case T_TypeName:
			retval = _copyTypeName(from);
			break;
		case T_IndexElem:
			retval = _copyIndexElem(from);
			break;
		case T_ColumnDef:
			retval = _copyColumnDef(from);
			break;
		case T_ColumnReferenceStorageDirective:
			retval = _copyColumnReferenceStorageDirective(from);
			break;
		case T_Constraint:
			retval = _copyConstraint(from);
			break;
		case T_DefElem:
			retval = _copyDefElem(from);
			break;
		case T_LockingClause:
			retval = _copyLockingClause(from);
			break;
		case T_DMLActionExpr:
			retval = _copyDMLActionExpr(from);
			break;
		case T_PartOidExpr:
			retval = _copyPartOidExpr(from);
			break;
		case T_PartDefaultExpr:
			retval = _copyPartDefaultExpr(from);
			break;
		case T_PartBoundExpr:
			retval = _copyPartBoundExpr(from);
			break;
		case T_PartBoundInclusionExpr:
			retval = _copyPartBoundInclusionExpr(from);
			break;
		case T_PartBoundOpenExpr:
			retval = _copyPartBoundOpenExpr(from);
			break;
		case T_RangeTblEntry:
			retval = _copyRangeTblEntry(from);
			break;
		case T_SortClause:
			retval = _copySortClause(from);
			break;
		case T_GroupClause:
			retval = _copyGroupClause(from);
			break;
		case T_GroupingClause:
			retval = _copyGroupingClause(from);
			break;
		case T_GroupingFunc:
			retval = _copyGroupingFunc(from);
			break;
		case T_Grouping:
			retval = _copyGrouping(from);
			break;
		case T_GroupId:
			retval = _copyGroupId(from);
			break;
		case T_WindowSpecParse:
			retval = _copyWindowSpecParse(from);
			break;
		case T_WindowSpec:
			retval = _copyWindowSpec(from);
			break;
		case T_WindowFrame:
			retval = _copyWindowFrame(from);
			break;
		case T_WindowFrameEdge:
			retval = _copyWindowFrameEdge(from);
			break;
		case T_PercentileExpr:
			retval = _copyPercentileExpr(from);
			break;
		case T_RowMarkClause:
			retval = _copyRowMarkClause(from);
			break;
		case T_WithClause:
			retval = _copyWithClause(from);
			break;
		case T_CommonTableExpr:
			retval = _copyCommonTableExpr(from);
			break;
		case T_FkConstraint:
			retval = _copyFkConstraint(from);
			break;
		case T_PrivGrantee:
			retval = _copyPrivGrantee(from);
			break;
		case T_FuncWithArgs:
			retval = _copyFuncWithArgs(from);
			break;
		case T_CdbProcess:
			retval = _copyCdbProcess(from);
			break;
		case T_Slice:
			retval = _copySlice(from);
			break;
		case T_SliceTable:
			retval = _copySliceTable(from);
			break;
		case T_TableValueExpr:
			retval = _copyTableValueExpr(from);
			break;
		case T_AlterTypeStmt:
			retval = _copyAlterTypeStmt(from);
			break;
		case T_ResultRelSegFileInfo:
		  retval = _copyResultRelSegFileInfo(from);
		  break;
		case T_SegFileSplitMapNode:
		  retval = _copySegFileSplitMapNode(from);
		  break;
		case T_FileSplitNode:
		  retval = _copyFileSplitNode(from);
		  break;

		case T_DenyLoginInterval:
			retval = _copyDenyLoginInterval(from);
			break;
		case T_DenyLoginPoint:
			retval = _copyDenyLoginPoint(from);
			break;

		case T_CaQLSelect:
			retval = _copyCaQLSelect(from);
			break;
		case T_CaQLInsert:
			retval = _copyCaQLInsert(from);
			break;
		case T_CaQLDelete:
			retval = _copyCaQLDelete(from);
			break;
		case T_CaQLExpr:
			retval = _copyCaQLExpr(from);
			break;
		case T_VirtualSegmentNode:
			retval = _copyVirtualSegmentNode(from);
			break;
		default:
			elog(ERROR, "unrecognized node type: %d", (int) nodeTag(from));
			retval = from;		/* keep compiler quiet */
			break;
	}

	return retval;
}
