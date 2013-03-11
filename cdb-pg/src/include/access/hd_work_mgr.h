/*-------------------------------------------------------------------------
*
* hd_work_mgr.h
*	  distributes hdfs file splits or hbase table regions for processing 
*     between GP segments
*
* Copyright (c) 2007-2008, Greenplum inc
*
*-------------------------------------------------------------------------
*/
#ifndef HDWORKMGR_H
#define HDWORKMGR_H

#include "c.h"
#include "nodes/pg_list.h"

char** map_hddata_2gp_segments(char *uri, int num_segs);
void free_hddata_2gp_segments(char **segs_work_map, int num_segs);

/*
 * Structure that describes one Statistics element received from the GPXF service
 */
typedef struct sGpxfStatsElem
{
	int   blockSize; /* size of a block size in the GPXF target datasource */
	int   numBlocks;
	int   numTuples;
} GpxfStatsElem;

List *get_gpxf_statistics(char *uri);

#endif   /* HDWORKMGR_H */
