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

extern char** map_hddata_2gp_segments(char *uri, int total_segs, int working_segs, Relation relation);
extern void free_hddata_2gp_segments(char **segs_work_map, int total_segs);

/*
 * Structure that describes one Statistics element received from the PXF service
 */
typedef struct sPxfStatsElem
{
	int   blockSize; /* size of a block size in the PXF target datasource */
	int   numBlocks;
	int   numTuples;
} PxfStatsElem;
PxfStatsElem *get_pxf_statistics(char *uri, Relation rel, StringInfo err_msg);

#endif   /* HDWORKMGR_H */
