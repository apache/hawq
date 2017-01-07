/*-------------------------------------------------------------------------
*
* hd_work_mgr.h
*	  distributes hadoop data fragments (e.g. hdfs file splits or hbase table regions)
*	  for processing between GP segments
*
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
*
*-------------------------------------------------------------------------
*/
#ifndef HDWORKMGR_H
#define HDWORKMGR_H

#include "c.h"
#include "utils/rel.h"
#include "nodes/pg_list.h"
#include "lib/stringinfo.h"

extern char** map_hddata_2gp_segments(char *uri, int total_segs, int working_segs, Relation relation, List* quals);
extern void free_hddata_2gp_segments(char **segs_work_map, int total_segs);

/*
 * Structure that describes fragments statistics element received from PXF service
 */
typedef struct sPxfFragmentStatsElem
{
	int numFrags;
	float4 firstFragSize; /* size of the first fragment */
	float4 totalSize; /* size of the total datasource */
} PxfFragmentStatsElem;
PxfFragmentStatsElem *get_pxf_fragments_statistics(char *uri, Relation rel);

List *get_pxf_item_metadata(char *profile, char *pattern, Oid dboid);

#define HiveProfileName "Hive"
#define HiveTextProfileName "HiveText"
#define HiveRCProfileName "HiveRC"

#endif   /* HDWORKMGR_H */
