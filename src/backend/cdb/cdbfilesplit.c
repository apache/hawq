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
* filesplit.c
*	  routines for file split.
*
*-------------------------------------------------------------------------
*/

#include "postgres.h"

#include "access/filesplit.h"
#include "access/htup.h"
#include "access/genam.h"
#include "access/aosegfiles.h"
#include "access/parquetsegfiles.h"
#include "catalog/pg_appendonly.h"
#include "catalog/gp_policy.h"
#include "cdb/cdbpartition.h"
#include "nodes/pg_list.h"
#include "optimizer/prep.h"
#include "utils/syscache.h"
#include "utils/lsyscache.h"

static void fileSplit_free(FileSplitNode *split);

static List *
computeSplitToSegmentMaps(Oid relid, GpPolicy *targetPolicy, List *splits,
						List *segment_infos, bool keep_hash_policy,
						int target_segment_num);

static List *
postProcessSplitsPerSegment(List *oldSplitToSegmentMaps);

static int
fileSplitNodeCmp(const void *left, const void *right);

static SegFileSplitMapNode *
AssignSingleAOSegFileSplitToSegment(Oid relid, List *segment_infos, bool keep_hash_policy, int target_segment_num)
{
	SegFileSplitMapNode *result = NULL;
	char storageChar;

	AppendOnlyEntry *aoEntry = NULL;
	List *splits = NIL;
	List *splitToSegmentMaps = NIL;
	GpPolicy *targetPolicy;

	storageChar = get_relation_storage_type(relid);
	result = makeNode(SegFileSplitMapNode);
	result->relid = relid;

	/*
	 * Here we only consider append only tables
	 */
	if ((storageChar != RELSTORAGE_AOROWS) &&
		(storageChar != RELSTORAGE_PARQUET))
	{
		result->splits = NIL;
		return result;
	}

	/*
	 * Get pg_appendonly information for this table.
	 */
	aoEntry = GetAppendOnlyEntry(relid, SnapshotNow);

	/*
	 * Based on the pg_appendonly information, generate
	 * a list of splits associated with this relation.
	 */
	if (RELSTORAGE_AOROWS == storageChar)
	{
		splits = AOGetAllSegFileSplits(aoEntry, SnapshotSelf);
	}
	else
	{
		Assert(RELSTORAGE_PARQUET == storageChar);
		splits = ParquetGetAllSegFileSplits(aoEntry, SnapshotSelf);
	}

	/*
	 * Get gp_distribution_policy information for this relation.
	 */
	targetPolicy = GpPolicyFetch(CurrentMemoryContext, relid);
	Assert(targetPolicy);

	/*
	 * Compute the mapping from splits to segments, taking
	 * data locality into account.
	 */
	splitToSegmentMaps = computeSplitToSegmentMaps(relid, targetPolicy, splits, segment_infos,
											keep_hash_policy, target_segment_num);
	Assert(splitToSegmentMaps);
	list_free(splits);

	result->splits = splitToSegmentMaps;

	return result;
}

List *
AssignAOSegFileSplitToSegment(Oid relid, List *segment_infos, bool keep_hash_policy, int target_segment_num, List *existings)
{
	SegFileSplitMapNode *result = NULL;

	if (rel_is_partitioned(relid))
	{
		List *children = NIL;
		ListCell *child;
		children = find_all_inheritors(relid);
		foreach (child, children)
		{
			Oid myrelid = lfirst_oid(child);
			result = AssignSingleAOSegFileSplitToSegment(myrelid, segment_infos, keep_hash_policy,
					target_segment_num);
			existings = lappend(existings, result);
		}
		list_free(children);

		return existings;
	}
	else
	{
		result = AssignSingleAOSegFileSplitToSegment(relid, segment_infos, keep_hash_policy,
													target_segment_num);
		existings = lappend(existings, result);
	}

	return existings;
}

/*
 * If keep_hash_policy is false, then the table is treated as randomly
 * distributed, regardless of its distribution policy.
 *
 * If segment_infos is NIL, then data locality is not needed.
 */
static List *
computeSplitToSegmentMaps(Oid relid, GpPolicy *targetPolicy, List *splits, List *segment_infos,
						bool keep_hash_policy, int target_segment_num)
{
	List *splitToSegmentMaps = NIL;
	if (keep_hash_policy)
	{
		/*
		 * If we want to keep the hash distribution policy,
		 * we need to make sure the data partition number
		 * equals to the number of target segments.
		 */
		Assert(targetPolicy->bucketnum == target_segment_num);
	}

	if (segment_infos != NIL)
	{
		/*
		 * If the segment information is given, we need to
		 * make sure the number of segments given equals to
		 * the number of target segments.
		 */
		Assert(list_length(segment_infos) == target_segment_num);
	}

	/*
	 * In this case, no data locality is needed.
	 */
	if (segment_infos == NIL)
	{
		/*
		 * Sub case 1: we need to keep the hash distribution policy.
		 *
		 * In this case, we just need consider the segment file number.
		 * This case works exactly the same as HAWQ1.x.
		 */
		if (keep_hash_policy)
		{
			ListCell *lc;
			int i;
			for (i = 0; i < target_segment_num; i++)
			{
				splitToSegmentMaps = lappend(splitToSegmentMaps, NIL);
			}

			foreach(lc, splits)
			{
				int assigned_seg_no;
				ListCell *per_seg_split;
				FileSplit split = (FileSplitNode *)lfirst(lc);
				Assert(split);
				assigned_seg_no = (split->segno - 1) % target_segment_num;
				per_seg_split = list_nth_cell(splitToSegmentMaps, assigned_seg_no);
				lfirst(per_seg_split) = lappend((List *)lfirst(per_seg_split), split);
			}
		}
		else
		{
			ereport(ERROR,
					(errcode(ERRCODE_CDB_FEATURE_NOT_YET),
					errmsg("Assigning splits to segment without keeping hash distribution policy is not allowed")));
		}
	}
	else
	{
		ereport(ERROR,
				(errcode(ERRCODE_CDB_FEATURE_NOT_YET),
				errmsg("Assigning splits to segment based on data locality is not allowed")));
	}

	/*
	 * After assigning each split to one segment, we need to do some post-processing,
	 * such as merging two splits assigned to the same segment if they can be merged.
	 */
	splitToSegmentMaps = postProcessSplitsPerSegment(splitToSegmentMaps);

	return splitToSegmentMaps;
}

/*
 * post process all splits assigned to the same segment.
 */
static List *
postProcessSplitsPerSegment(List *oldSplitToSegmentMaps)
{
	List *newSplitToSegmentMaps = NIL;
	ListCell *lc;
	foreach(lc, oldSplitToSegmentMaps)
	{
		List *splits = (List *)lfirst(lc);
		if (splits == NIL || list_length(splits) == 0)
		{
			newSplitToSegmentMaps = lappend(newSplitToSegmentMaps, NIL);
		}
		else
		{
			int len = list_length(splits);
			int i = 0;
			List *newSplits = NIL;
			FileSplitNode **split_arr = palloc(sizeof(FileSplitNode *) * len);
			FileSplit last;
			ListCell *fsn_lc;
			foreach(fsn_lc, splits)
			{
				FileSplit split = (FileSplitNode *)lfirst(fsn_lc);
				split_arr[i++] = split;
			}

			qsort((char *)split_arr, len, sizeof(FileSplitNode *), fileSplitNodeCmp);

			last = split_arr[0];
			for(i = 1; i < len; i++)
			{
				if ((last->segno == split_arr[i]->segno)
						&& (last->offsets + last->lengths >= split_arr[i]->offsets))
				{
					/*
					 * the above conditions mean that these two splits can be merged into one.
					 */
					last->lengths += split_arr[i]->lengths;

					/*
					 * After merging, we can now free the useless
					 * splits.
					 */
					fileSplit_free(split_arr[i]);
				}
				else
				{
					newSplits = lappend(newSplits, last);
					last = split_arr[i];
				}
			}
			newSplits = lappend(newSplits, last);

			newSplitToSegmentMaps = lappend(newSplitToSegmentMaps, newSplits);
			pfree(split_arr);
		}
		if (splits != NIL)
		{
			list_free(splits);
		}
	}
	list_free(oldSplitToSegmentMaps);

	return newSplitToSegmentMaps;
}

/*
 * The comparison routine that sorts an array of FileSplitNode.
 */
static int
fileSplitNodeCmp(const void *left, const void *right)
{
	FileSplit left_split = *((FileSplitNode **)left);
	FileSplit right_split = *((FileSplitNode **)right);

	if (left_split->segno < right_split->segno)
	{
		return -1;
	}

	if (left_split->segno > right_split->segno)
	{
		return 1;
	}

	if (left_split->offsets < right_split->offsets)
	{
		return -1;
	}

	if (left_split->offsets > right_split->offsets)
	{
		return 1;
	}

	return 0;
}

static void
fileSplit_free(FileSplitNode *split)
{
	if (split != NULL)
	{
		pfree(split);
	}
	return;
}

List *
GetFileSplitsOfSegment(List *splitToSegmentMaps, Oid relid, int segment_index)
{
	if ((segment_index < 0) || (splitToSegmentMaps == NIL) ||
			(list_length(splitToSegmentMaps) == 0))
	{
		return NIL;
	}
	else
	{
		ListCell *lc;
		foreach(lc, splitToSegmentMaps)
		{
			SegFileSplitMap map = (SegFileSplitMapNode *)lfirst(lc);
			if (map->relid == relid)
			{
				List *splits = map->splits;
				if ((splits == NIL) || (segment_index >= list_length(splits)))
				{
					return NIL;
				}
				return (List *)list_nth(splits, segment_index);
			}
		}
	}

	return NIL;
}
