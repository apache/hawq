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
 * cdbdatalocality.h
 *	  Manages data locality.
 *
 *-------------------------------------------------------------------------
 */
#ifndef CDBDATALOCALITY_H
#define CDBDATALOCALITY_H

#include "postgres.h"

#include "catalog/gp_policy.h"
#include "nodes/parsenodes.h"
#include "executor/execdesc.h"
#include "catalog/pg_exttable.h"
#include "magma/cwrapper/magma-client-c.h"
#include "access/extprotocol.h"


/*
 * structure containing information about data residence
 * at the host.
 */
typedef struct SplitAllocResult
{
  QueryResource *resource;
  List *alloc_results;
  int planner_segments;
  List *relsType;// relation type after datalocality changing
  StringInfo datalocalityInfo;
  double datalocalityTime;
  char *hiveUrl;
} SplitAllocResult;

/*
 * structure containing all relation range table entries.
 */
typedef struct udf_collector_context {
	bool udf_exist;
} udf_collector_context;

/*
 * structure containing rel and type when execution
 */
typedef struct CurrentRelType {
	Oid relid;
	bool isHash;
	uint16_t range_num;
} CurrentRelType;

/*
 * structure for virtual segment.
 */
typedef struct VirtualSegmentNode
{
	NodeTag type;
	char *hostname;
} VirtualSegmentNode;

typedef struct blocklocation_file{
	BlockLocation *locations;
	int block_num;
	char *file_uri;
}blocklocation_file;

/*
 * calculate_planner_segment_num: based on the parse tree,
 * we calculate the appropriate planner segment_num.
 */
SplitAllocResult * calculate_planner_segment_num(PlannedStmt *plannedstmt, Query *query,
		QueryResourceLife resourceLife, int fixedVsegNum);

/*
 * set_magma_range_rg_map: set up the range and replica maps.
 */
void set_magma_range_vseg_map(List *SegFileSplitMaps, int nvseg);

/*
 * find the range and virtual segment map by nvseg
 */
void get_magma_range_vseg_map(int **map, int *nmap, int nvseg);

/*
 * get filesplits of magma table for insert
 */
List* get_magma_scansplits(List *all_relids);
void fetch_magma_result_splits_from_plan(List **alloc_result, PlannedStmt* plannedstmt, int vsegNum);
void build_magma_scansplits_for_result_relations(List **alloc_result, List *relOids, int vsegNum);

/*
 * udf_collector_walker: the routine to file udfs.
 */
bool udf_collector_walker(Node *node,	udf_collector_context *context);

/*
 * find_udf: collect all udf, and store them into the udf_collector_context.
 */
void find_udf(Query *query, udf_collector_context *context);


/* used for magma analyze*/
Oid LookupCustomProtocolBlockLocationFunc(char *protoname);
Oid LookupCustomProtocolTableSizeFunc(char *protoname);
Oid LookupCustomProtocolDatabaseSizeFunc(char *protoname);

void InvokeMagmaProtocolBlockLocation(ExtTableEntry *ext_entry,
                                             Oid    procOid,
                                             char  *dbname,
                                             char  *schemaname,
                                             char  *tablemame,
                                             MagmaSnapshot *snapshot,
                                             bool useClientCacheDirectly,
                                             ExtProtocolBlockLocationData **bldata);

void InvokeMagmaProtocolTableSize(ExtTableEntry *ext_entry,
                                             Oid procOid,
                                             char *dbname,
                                             char *schemaname,
                                             char *tablename,
                                             MagmaSnapshot *snapshot,
                                             ExtProtocolTableSizeData **tsdata);

void InvokeMagmaProtocolDatabaseSize(Oid procOid,
                                     const char *dbname,
                                     MagmaSnapshot *snapshot,
                                     ExtProtocolDatabaseSizeData **dbsdata);

bool dataStoredInMagma(Relation rel);

bool dataStoredInMagmaByOid(Oid relid);

bool dataStoredInHive(Relation rel);

void fetchUrlStoredInHive(Oid relid, char **hiveUrl);

void fetchDistributionPolicy(Oid relid, int32 *n_dist_keys,
                             int16 **dist_keys);

List *magma_build_range_to_rg_map(List *splits, uint32 *range_to_rg_map);

void magma_build_rg_to_url_map(List *splits, List *rg, uint16 *rgIds,
                                      char **rgUrls);

char *search_hostname_by_ipaddr(const char *ipaddr);

FILE *fp;
FILE *fpaoseg;
FILE *fpsegnum;
FILE *fpratio;
#endif /* CDBDATALOCALITY_H */
