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
* filesplit.h
*	  data structures for file split.
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
AssignAOSegFileSplitToSegment(Oid relid, List *segment_infos, int target_segment_num, List *existings);

/*
 * Given the relid, and the segment index, return the splits assigned
 * to this segment.
 */
extern List *
GetFileSplitsOfSegment(List *segFileSplitMaps, Oid relid, int segment_index);

#endif /* FILESPLIT_H */
