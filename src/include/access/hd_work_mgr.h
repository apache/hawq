/*-------------------------------------------------------------------------
*
* hd_work_mgr.h
*	  distributes hadoop data fragments (e.g. hdfs file splits or hbase table regions)
*	  for processing between GP segments
*
* Copyright (c) 2007-2008, Greenplum inc
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
	int firstFragSize; /* size of the first fragment */
	int totalSize; /* size of the total datasource */
} PxfFragmentStatsElem;
PxfFragmentStatsElem *get_pxf_fragments_statistics(char *uri, Relation rel, StringInfo err_msg);

List *get_pxf_hcat_metadata(char *relation_location);

#endif   /* HDWORKMGR_H */
