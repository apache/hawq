/*-------------------------------------------------------------------------
*
* filesplit.h
*	  data structures for file split.
*
* Portions Copyright (c) 2014, Pivotal Inc.
*-------------------------------------------------------------------------
*/
#ifndef FILESPLIT_H
#define FILESPLIT_H

#include "c.h"

#include "nodes/nodes.h"
#include "nodes/pg_list.h"

/*
 * FileSplit is used to represent a portion of a file.
 */
typedef struct FileSplitNode
{
	NodeTag type;
	int segno;
	int64 logiceof;
	int64 offsets;
	int64 lengths;
} FileSplitNode;

typedef FileSplitNode *FileSplit;


/*
 * SegFileSplitMap is used to represent which segment should which
 * portion of a table file.
 */
typedef struct SegFileSplitMapNode
{
	NodeTag type;
	Oid relid;
	List *splits;
} SegFileSplitMapNode;

typedef SegFileSplitMapNode *SegFileSplitMap;


/*
 * Given the segment lists and relation id, compute
 * which segment should scan which portion of the
 * table data.
 */
List *
AssignAOSegFileSplitToSegment(Oid relid, List *segment_infos, bool keep_hash_policy, int target_segment_num, List *existings);

/*
 * Given the relid, and the segment index, return the splits assigned
 * to this segment.
 */
extern List *
GetFileSplitsOfSegment(List *segFileSplitMaps, Oid relid, int segment_index);

#endif /* FILESPLIT_H */
