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
 * cdbdatalocality.c
 *	  Manages data locality.
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "access/genam.h"
#include "access/aomd.h"
#include "access/heapam.h"
#include "access/filesplit.h"
#include "access/orcsegfiles.h"
#include "access/parquetsegfiles.h"
#include "access/pxfutils.h"
#include "access/xact.h"
#include "catalog/catalog.h"
#include "catalog/catquery.h"
#include "catalog/pg_exttable.h"
#include "catalog/pg_inherits.h"
#include "catalog/pg_proc.h"
#include "catalog/pg_extprotocol.h"
#include "cdb/cdbdatalocality.h"
#include "cdb/cdbhash.h"
#include "cdb/cdbutil.h"
#include "cdb/cdbvars.h"
#include "cdb/cdbpartition.h"
#include "utils/lsyscache.h"
#include "utils/tqual.h"
#include "utils/memutils.h"
#include "executor/execdesc.h"
#include "executor/spi.h"
#include "nodes/nodes.h"
#include "nodes/parsenodes.h"
#include "optimizer/walkers.h"
#include "optimizer/planmain.h"
#include "parser/parsetree.h"
#include "storage/fd.h"
#include "parser/parse_func.h"
#include "postmaster/identity.h"
#include "cdb/cdbmetadatacache.h"
#include "resourcemanager/utils/network_utils.h"
#include "access/skey.h"
#include "utils/fmgroids.h"
#include "utils/uri.h"
#include "catalog/pg_proc.h"
#include "postgres.h"
#include "resourcemanager/utils/hashtable.h"

#include "c.h"
#include "miscadmin.h"
#include "commands/dbcommands.h"
#include "utils/lsyscache.h"
#include "storage/cwrapper/hive-file-system-c.h"
#include "access/plugstorage.h"


/* We need to build a mapping from host name to host index */

extern bool		optimizer; /* Enable the optimizer */

static MemoryContext MagmaGlobalMemoryContext = NULL;
#define MAX_MAGMA_RANGE_VSEG_MAP_NUM 20
#define MAX_INT32	(2147483600)

typedef struct segmentFilenoPair {
	int segmentid;
	int fileno;
} segmentFilenoPair;
typedef struct HostnameIndexKey {
	char hostname[HOSTNAME_MAX_LENGTH];
} HostnameIndexKey;

typedef struct HostnameIndexEntry {
	HostnameIndexKey key;
	int index;
} HostnameIndexEntry;

/*
 * structure containing all relation range table entries.
 */
typedef struct collect_scan_rangetable_context {
	plan_tree_base_prefix base;
	List *indexscan_range_tables;  // range table for index only scan
	List *indexscan_indexs;  // index oid for range table
	List *parquetscan_range_tables; // range table for parquet scan
	List *range_tables; // range table for scan only
	List *full_range_tables;  // full range table
} collect_scan_rangetable_context;

/*
 * structure containing information about how much a
 * host holds.
 */
typedef struct HostDataVolumeInfo {
	HostnameIndexEntry *hashEntry;
	int64 datavolume;
	int occur_count;
} HostDataVolumeInfo;

/*
 * structure for data distribution statistics.
 */
typedef struct data_dist_stat_context {
	int size;
	int max_size;
	HostDataVolumeInfo *volInfos;
} data_dist_stat_context;

/*
 * structure for prefer host.
 */
typedef struct Prefer_Host {
	char *hostname;
	int64 data_size;
} Prefer_Host;

/*
 * structure for file portion.
 */
typedef struct File_Split {
	uint32_t range_id;
	uint32_t replicaGroup_id;
	int64 offset;
	int64 length;
	int64 logiceof;
	int host;
	bool is_local_read;
	char *ext_file_uri;
} File_Split;

typedef enum DATALOCALITY_RELATION_TYPE {
	DATALOCALITY_APPENDONLY,
	DATALOCALITY_PARQUET,
	DATALOCALITY_HDFS,
	DATALOCALITY_HIVE,
	DATALOCALITY_MAGMA,
	DATALOCALITY_UNKNOWN
} DATALOCALITY_RELATION_TYPE;

/*
 * structure for detailed file split.
 */
typedef struct Detailed_File_Split {
	Oid rel_oid;
	int segno; // file name suffix
	uint32_t range_id;
	uint32_t replicaGroup_id;
	int index;
	int host;
	int64 logiceof;
	int64 offset;
	int64 length;
	char *ext_file_uri_string;
} Detailed_File_Split;

/*
 * structure for block hosts.
 */
typedef struct Host_Index {
	int index;
	char *hostname; // used to sort host index
} Host_Index;

/*
 * structure for block hosts.
 */
typedef struct Block_Host_Index {
	int replica_num;
	int* hostIndex; // hdfs host name list(size is replica_num)
	Host_Index *hostIndextoSort; // used to sore host index
	int insertHost; // the host which inserted this block.
} Block_Host_Index;

/*
 * structure for one Relation File.
 */
typedef struct Relation_File {
	int segno; // file name suffix
	BlockLocation *locations;
	Block_Host_Index *hostIDs;
	int block_num;
	File_Split *splits;
	int split_num;
	int segmentid;
	int64 logic_len; // eof length.
	double continue_ratio;
} Relation_File;

/*
 * structure for one Relation File.
 */
typedef struct Split_Block {
	int fileIndex;
	int splitIndex;
} Split_Block;

/*
 * structure for all relation data.
 */
typedef struct Relation_Data {
	Oid relid;
	DATALOCALITY_RELATION_TYPE type;
	int64 total_size;
	List *files;
	Oid partition_parent_relid;
	int64 block_count;
	char *serializeSchema;
	int serializeSchemaLen;
	MagmaTableFullName* magmaTableFullName;
} Relation_Data;

/*
 * structure for allocating all splits
 * of one relation.
 */
typedef struct Relation_Assignment_Context {
	int virtual_segment_num;
	int64 upper_bound;
	int64 *vols;
	int64 *totalvols;
	int64 *totalvols_with_penalty;
	int *split_num;
	int *continue_split_num;
	int roundrobinIndex;
	int total_split_num;
	HASHTABLE vseg_to_splits_map;
	HASHTABLE patition_parent_size_map; /*record avg vseg size of each partition table*/
	HASHTABLE partitionvols_map;  /*record vseg size of each partition table*/
	HASHTABLE partitionvols_with_penalty_map;  /*record vseg size of each partition table*/
	HASHTABLE table_blocks_num_map; /*record block number of a table*/
	double avg_size_of_whole_query; /*average size per vseg for all all the relation in a query*/
	int64 avg_size_of_whole_partition_table; /*average size per vseg for all all the relation in a query*/
	int block_lessthan_vseg_round_robin_no;
} Relation_Assignment_Context;

/*
 * structure for print data locality result
 */
typedef struct Assignment_Log_Context {
	double totalDataSize;
	double datalocalityRatio;
	int maxSegmentNumofHost;
	int minSegmentNumofHost;
	int avgSegmentNumofHost;
	int numofDifferentHost;
	double avgSizeOverall;
	int64 maxSizeSegmentOverall;
	int64 minSizeSegmentOverall;
	double avgSizeOverallPenalty;
	int64 maxSizeSegmentOverallPenalty;
	int64 minSizeSegmentOverallPenalty;
	double avgContinuityOverall;
	double maxContinuityOverall;
	double minContinuityOverall;
	int64 localDataSizePerRelation;
	int64 totalDataSizePerRelation;
} Assignment_Log_Context;

/*
 * structure for target segment ID mapping.
 */
typedef struct TargetSegmentIDMap {
	int target_segment_num;   // number of virtual segments
	int *global_IDs;   // virtual segment number -> global hostIndex
	char** hostname;   // hostname of each virtual segment
} TargetSegmentIDMap;

/*
 * structure for storing all HDFS block locations.
 */
typedef struct collect_hdfs_split_location_context {
	List *relations;
} collect_hdfs_split_location_context;

/*
 * structure for host split assignment result.
 */
typedef struct Host_Assignment_Result {
	int count;
	int max_size;
	Detailed_File_Split *splits;
} Host_Assignment_Result;

/*
 * structure for total split assignment result.
 */
typedef struct Split_Assignment_Result {
	int host_num;
	Host_Assignment_Result *host_assigns;
} Split_Assignment_Result;

/*
 * structure for host data statistics.
 */
typedef struct hostname_volume_stat_context {
	int size;
	HostnameVolumeInfo *hostnameVolInfos;
} hostname_volume_stat_context;

/*
 * structure for tracking the whole procedure
 * of computing the split to segment mapping
 * for each query.
 */
typedef struct split_to_segment_mapping_context {
	collect_scan_rangetable_context srtc_context;
	data_dist_stat_context dds_context;
	collect_hdfs_split_location_context chsl_context;
	hostname_volume_stat_context host_context;
	HTAB *hostname_map;
	bool keep_hash;
	int prefer_segment_num;
	int64 split_size;
	MemoryContext old_memorycontext;
	MemoryContext datalocality_memorycontext;
	int externTableForceSegNum;  //expected virtual segment number when external table exists
	int externTableLocationSegNum;  //expected virtual segment number when external table exists
	int tableFuncSegNum;  //expected virtual segment number when table function exists
	int hashSegNum;  // expected virtual segment number when there is hash table in from clause
	int randomSegNum; // expected virtual segment number when there is random table in from clause
	int resultRelationHashSegNum; // expected virtual segment number when hash table as a result relation
	int minimum_segment_num; //default is 1.
	int64 randomRelSize; //all the random relation size
	int64 hashRelSize; //all the hash relation size

	int64 total_size; /* total data size for all relations */
	int64 total_split_count;
	int64 total_file_count;

	int64 total_metadata_logic_len;

    int metadata_cache_time_us;
    int alloc_resource_time_us;
    int cal_datalocality_time_us;
    bool isMagmaTableExist;
    bool isTargetNoMagma;
    int magmaRangeNum;
    char *hiveUrl;
} split_to_segment_mapping_context;

/* global map for range and replica_group of magma tables */
HTAB *magma_range_vseg_maps = NULL;
typedef struct range_vseg_map_data
{
  int vseg_num;
	int *range_vseg_map;
	int range_num;
} range_vseg_map_data;

typedef struct vseg_list{
	List* vsegList;
}vseg_list;

#define HOSTIP_MAX_LENGTH (64)

#define PORT_STRING_SIZE 16

typedef struct HostnameIpKey {
  char hostip[HOSTIP_MAX_LENGTH];
} HostnameIpKey;

static MemoryContext HostNameGlobalMemoryContext = NULL;

HTAB *magma_ip_hostname_map = NULL;

typedef struct HostnameIpEntry {
  HostnameIpKey key;
  char hostname[HOSTNAME_MAX_LENGTH];
} HostnameIpEntry;

static MemoryContext DataLocalityMemoryContext = NULL;

static void init_datalocality_memory_context(void);

static void init_split_assignment_result(Split_Assignment_Result *result,
		int host_num);

static void init_datalocality_context(PlannedStmt *plannedstmt,
		split_to_segment_mapping_context *context);

static bool collect_scan_rangetable(Node *node,
		collect_scan_rangetable_context *cxt);

static void convert_range_tables_to_oids(List **range_tables,
                                         MemoryContext my_memorycontext);

static void check_magma_table_exsit(List *full_range_tables, bool *isMagmaTableExist);

static void check_keep_hash_and_external_table(
		split_to_segment_mapping_context *collector_context, Query *query,
		GpPolicy *intoPolicy);

static int64 get_block_locations_and_calculate_table_size(
		split_to_segment_mapping_context *collector_context);

static bool dataStoredInHdfs(Relation rel);

static List *get_virtual_segments(QueryResource *resource);

static List *run_allocation_algorithm(SplitAllocResult *result, List *virtual_segments, QueryResource ** resourcePtr,
		split_to_segment_mapping_context *context);

static void double_check_hdfs_metadata_logic_length(BlockLocation * locations,int block_num,int64 logic_len);

static void AOGetSegFileDataLocation(Relation relation,
		AppendOnlyEntry *aoEntry, Snapshot metadataSnapshot,
		split_to_segment_mapping_context *context, int64 splitsize,
		Relation_Data *rel_data, int* hitblocks,
		int* allblocks, GpPolicy *targetPolicy);

static void ParquetGetSegFileDataLocation(Relation relation,
		Oid segrelid, Snapshot metadataSnapshot, List *idx_scan_ids,
		split_to_segment_mapping_context *context, int64 splitsize,
		Relation_Data *rel_data, int* hitblocks,
		int* allblocks, GpPolicy *targetPolicy);

static void ParquetGetSegFileDataLocationWrapper(
    Relation relation, AppendOnlyEntry *aoEntry, Snapshot metadataSnapshot,
    split_to_segment_mapping_context *context, int64 splitsize,
    Relation_Data *rel_data, int *hitblocks, int *allblocks,
    GpPolicy *targetPolicy);

static void ExternalGetHdfsFileDataLocation(
    Relation relation, split_to_segment_mapping_context *context,
    int64 splitsize, Relation_Data *rel_data, int *allblocks,
    GpPolicy *targetPolicy);

static void ExternalGetHiveFileDataLocation(Relation relation,
    split_to_segment_mapping_context *context, int64 splitsize,
    Relation_Data *rel_data, int* allblocks);

static void ExternalGetMagmaRangeDataLocation(
                const char* dbname, const char* schemaname,
                const char* tablename,
                split_to_segment_mapping_context *context, int64 rangesize,
		Relation_Data *rel_data, bool useClientCacheDirectly, int* allranges);

Oid LookupCustomProtocolBlockLocationFunc(char *protoname);

static BlockLocation *fetch_hdfs_data_block_location(char *filepath, int64 len,
		int *block_num, RelFileNode rnode, uint32_t segno, double* hit_ratio, bool index_scan);

static void free_hdfs_data_block_location(BlockLocation *locations,
		int block_num);

static void free_magma_data_range_locations(BlockLocation *locations,
                                                  int range_num);

static Block_Host_Index * update_data_dist_stat(
		split_to_segment_mapping_context *context, BlockLocation *locations,
		int block_num);

static HostDataVolumeInfo *search_host_in_stat_context(
		split_to_segment_mapping_context *context, char *hostname);

static bool IsBuildInFunction(Oid funcOid);

static void allocate_hash_relation_for_magma_file(Relation_Data* rel_data,
		Assignment_Log_Context* log_context, TargetSegmentIDMap* idMap,
		Relation_Assignment_Context* assignment_context,
		split_to_segment_mapping_context *context, bool parentIsHashExist, bool parentIsHash);

static bool allocate_hash_relation(Relation_Data* rel_data,
		Assignment_Log_Context *log_context, TargetSegmentIDMap* idMap,
		Relation_Assignment_Context* assignment_context,
		split_to_segment_mapping_context *context, bool parentIsHashExist, bool parentIsHash);

static void allocate_random_relation(Relation_Data* rel_data,
		Assignment_Log_Context *log_context, TargetSegmentIDMap* idMap,
		Relation_Assignment_Context* assignment_context,
		split_to_segment_mapping_context *context);

static void print_datalocality_overall_log_information(SplitAllocResult *result,
		List *virtual_segments, int relationCount,
		Assignment_Log_Context *log_context,
		Relation_Assignment_Context* assignment_context,
		split_to_segment_mapping_context *context);

static void caculate_per_relation_data_locality_result(Relation_Data* rel_data,
		Assignment_Log_Context* log_context,
		Relation_Assignment_Context* assignment_context);

static void combine_all_splits(Detailed_File_Split **splits,
		Relation_Assignment_Context* assignment_context, TargetSegmentIDMap* idMap,
		Assignment_Log_Context* log_context,
		split_to_segment_mapping_context* context);

static int remedy_non_localRead(int fileIndex, int splitIndex, int parentPos,
		Relation_File** file_vector, int fileCount, int64 maxExtendedSizePerSegment,
		TargetSegmentIDMap* idMap,
		Relation_Assignment_Context* assignment_context);

static int select_random_host_algorithm(Relation_Assignment_Context *context,
		int64 splitsize, int64 maxExtendedSizePerSegment, TargetSegmentIDMap *idMap,
		Block_Host_Index** hostid,int fileindex, Oid partition_parent_oid, bool* isLocality);

static int compare_detailed_file_split(const void *e1, const void *e2);

static int compare_container_segment(const void *e1, const void *e2);

static int compare_file_segno(const void *e1, const void *e2);

static int compare_relation_size(const void *e1, const void *e2);

static int compare_file_continuity(const void *e1, const void *e2);

static int compare_hostid(const void *e1, const void *e2);

static void assign_split_to_host(Host_Assignment_Result *result,
		Detailed_File_Split *split);

static void assign_splits_to_hosts(Split_Assignment_Result *result,
		Detailed_File_Split *splits, int split_num);

static List *post_process_assign_result(Split_Assignment_Result *result);

static List *search_map_node(List *result, Oid rel_oid, int host_num,
		SegFileSplitMapNode **found_map_node);

static void cleanup_allocation_algorithm(
		split_to_segment_mapping_context *context);

//static void print_split_alloc_result(List *alloc_result);

//static int64 costTransform(Cost totalCost);

static void change_hash_virtual_segments_order(QueryResource ** resourcePtr,
		Relation_Data *rel_data, Relation_Assignment_Context *assignment_context,
		TargetSegmentIDMap* idMap_ptr);

static void change_hash_virtual_segments_order_magma_file(QueryResource ** resourcePtr,
		Relation_Data *rel_data,
		Relation_Assignment_Context *assignment_context_ptr,
		TargetSegmentIDMap* idMap_ptr);

static void change_hash_virtual_segments_order_orc_file(QueryResource ** resourcePtr,
		Relation_Data *rel_data, Relation_Assignment_Context *assignment_context,
		TargetSegmentIDMap* idMap_ptr);

static bool is_relation_hash(GpPolicy *targetPolicy);

static void allocation_preparation(List *hosts, TargetSegmentIDMap* idMap,
		Relation_Assignment_Context* assignment_context,
		split_to_segment_mapping_context *context);

static Relation_File** change_file_order_based_on_continuity(
		Relation_Data *rel_data, TargetSegmentIDMap* idMap, int host_num,
		int* fileCount, Relation_Assignment_Context *assignment_context);

static int64 set_maximum_segment_volume_parameter(Relation_Data *rel_data,
		int host_num, double* maxSizePerSegment);

static void InvokeHDFSProtocolBlockLocation(Oid    procOid,
                                            List  *locs,
                                            List **blockLocations);

static void InvokeHIVEProtocolBlockLocation(Oid    procOid,
                                            List  *locs,
                                            List **blockLocations);

/*
 * Setup /cleanup the memory context for this run
 * of data locality algorithm.
 */
static void init_datalocality_memory_context(void) {
	if (DataLocalityMemoryContext == NULL) {
		DataLocalityMemoryContext = AllocSetContextCreate(TopMemoryContext,
				"DataLocalityMemoryContext",
				ALLOCSET_DEFAULT_MINSIZE,
				ALLOCSET_DEFAULT_INITSIZE,
				ALLOCSET_DEFAULT_MAXSIZE);
	} else {
		MemoryContextResetAndDeleteChildren(DataLocalityMemoryContext);
	}

	return;
}

static void init_split_assignment_result(Split_Assignment_Result *result,
		int host_num) {
	int i;

	result->host_num = host_num;
	result->host_assigns = (Host_Assignment_Result *) palloc(
			sizeof(Host_Assignment_Result) * host_num);

	for (i = 0; i < host_num; i++) {
		result->host_assigns[i].count = 0;
		result->host_assigns[i].max_size = 2;
		result->host_assigns[i].splits = (Detailed_File_Split *) palloc(
				sizeof(Detailed_File_Split) * 2);
	}

	return;
}

static void init_datalocality_context(PlannedStmt *plannedstmt,
		split_to_segment_mapping_context *context) {
	context->old_memorycontext = CurrentMemoryContext;
	context->datalocality_memorycontext = DataLocalityMemoryContext;

	context->chsl_context.relations = NIL;
	context->srtc_context.range_tables = NIL;
	context->srtc_context.indexscan_indexs = NIL;
	context->srtc_context.indexscan_range_tables = NIL;
	context->srtc_context.parquetscan_range_tables = NIL;
	context->srtc_context.full_range_tables = plannedstmt->rtable;
	context->srtc_context.base.node = (Node *)plannedstmt;

	context->externTableForceSegNum = 0;
	context->externTableLocationSegNum = 0;
	context->tableFuncSegNum = 0;
	context->hashSegNum = 0;
	context->resultRelationHashSegNum = 0;
	context->randomSegNum = 0;
	context->randomRelSize = 0;
	context->hashRelSize = 0;
	context->minimum_segment_num = 1;
	context->hiveUrl = NULL;
	/*
	 * initialize the data distribution
	 * static context.
	 */
	{
		context->dds_context.size = 0;
		context->dds_context.max_size = 4;
		MemoryContextSwitchTo(context->datalocality_memorycontext);
		context->dds_context.volInfos = (HostDataVolumeInfo *) palloc(
				sizeof(HostDataVolumeInfo) * context->dds_context.max_size);
		MemSet(context->dds_context.volInfos, 0,
				sizeof(HostDataVolumeInfo) * context->dds_context.max_size);
		MemoryContextSwitchTo(context->old_memorycontext);
	}

	/*
	 * initialize the hostname map hash.
	 */
	{
		HASHCTL ctl;

		ctl.keysize = sizeof(HostnameIndexKey);
		ctl.entrysize = sizeof(HostnameIndexEntry);
		ctl.hcxt = context->datalocality_memorycontext;
		/* hostname_map shouldn't use topmemctx */
		context->hostname_map = hash_create("Hostname Index Map Hash", 16, &ctl, HASH_CONTEXT | HASH_ELEM);
	}

	context->keep_hash = false;
	context->prefer_segment_num = -1;
	context->split_size = split_read_size_mb;
	context->split_size <<= 20;

	context->total_size = 0;
	context->total_split_count = 0;
	context->total_file_count = 0;
	context->total_metadata_logic_len = 0;

    context->metadata_cache_time_us = 0;
    context->alloc_resource_time_us = 0;
    context->cal_datalocality_time_us = 0;
	return;
}

bool collect_scan_rangetable(Node *node, collect_scan_rangetable_context *cxt) {
  if (NULL == node) return false;

  switch (nodeTag(node)) {
    case T_OrcIndexScan:  // Same as T_OrcIndexOnlyScan
    case T_OrcIndexOnlyScan: {
      RangeTblEntry *rte =
          rt_fetch(((Scan *)node)->scanrelid, cxt->full_range_tables);
      cxt->indexscan_range_tables =
          lappend_oid(cxt->indexscan_range_tables, rte->relid);
      cxt->indexscan_indexs =
          lappend_oid(cxt->indexscan_indexs, ((IndexScan *)node)->indexid);
      cxt->range_tables = lappend(cxt->range_tables, rte);
      break;
    }
    // FIXME(sxwang): Should we append relid to parquetscan_range_tables for all
    // these kind of scan?
    case T_ExternalScan:        // Fall to ParquetScan
    case T_MagmaIndexScan:      // Fall to ParquetScan
    case T_MagmaIndexOnlyScan:  // Fall to ParquetScan
    case T_AppendOnlyScan:      // Fall to ParquetScan
    case T_ParquetScan: {
      RangeTblEntry *rte =
          rt_fetch(((Scan *)node)->scanrelid, cxt->full_range_tables);
      cxt->parquetscan_range_tables =
          lappend_oid(cxt->parquetscan_range_tables, rte->relid);
      cxt->range_tables = lappend(cxt->range_tables, rte);
      break;
    }
    default:
      break;
  }

  return plan_tree_walker(node, collect_scan_rangetable, cxt);
}

/*
 *
 */
static bool IsBuildInFunction(Oid foid) {

	cqContext  *pcqCtx;
	HeapTuple	procedureTuple;
	Form_pg_proc procedureStruct;

	/*
	 * get the procedure tuple corresponding to the given function Oid
	 */
	pcqCtx = caql_beginscan(
			NULL,
			cql("SELECT * FROM pg_proc "
				" WHERE oid = :1 ",
				ObjectIdGetDatum(foid)));

	procedureTuple = caql_getnext(pcqCtx);

	if (!HeapTupleIsValid(procedureTuple))
		elog(ERROR, "cache lookup failed for function %u", foid);
	procedureStruct = (Form_pg_proc) GETSTRUCT(procedureTuple);
	caql_endscan(pcqCtx);
	/* we treat proc namespace = 11 to build in function.*/
	if (procedureStruct->pronamespace == 11) {
		return true;
	} else {
		return false;
	}
}

/*
 *
 */
static void convert_range_tables_to_oids(List **range_tables,
                                         MemoryContext my_memorycontext) {
  MemoryContext old_memorycontext;
  old_memorycontext = MemoryContextSwitchTo(my_memorycontext);

  List *new_range_tables = NIL;
  ListCell *old_lc;
  foreach (old_lc, *range_tables) {
    RangeTblEntry *entry = (RangeTblEntry *)lfirst(old_lc);
    if (entry->rtekind != RTE_RELATION || rel_is_partitioned(entry->relid))
      continue;
    new_range_tables = list_append_unique_oid(new_range_tables, entry->relid);
  }

  MemoryContextSwitchTo(old_memorycontext);

  *range_tables = new_range_tables;
}

/*
 * check if magma table exists in this query.
 */
static void check_magma_table_exsit(List *full_range_tables, bool *isMagmaTableExist){
	ListCell *range_table;
	foreach(range_table, full_range_tables)
	{
		Oid myrelid = lfirst_oid(range_table);
		if(dataStoredInMagmaByOid(myrelid)){
			*isMagmaTableExist=true;
			return;
		}
	}
	return;
}

/*
 * check_keep_hash_dist_policy: determine whether keep the hash distribution policy.
 */
static void check_keep_hash_and_external_table(
		split_to_segment_mapping_context *context, Query *query,
		GpPolicy *intoPolicy) {
	ListCell *lc;

	MemoryContextSwitchTo(context->datalocality_memorycontext);

	char *formatterName = NULL;
	int formatterType = ExternalTableType_Invalid;
	context->isTargetNoMagma = false;

	if (query->resultRelation != 0) /* This is a insert command */
	{
		RangeTblEntry *rte = rt_fetch(query->resultRelation, query->rtable);
		context->isTargetNoMagma = !dataStoredInMagmaByOid(rte->relid);
		if (rte->rtekind == RTE_RELATION &&
			get_relation_storage_type(rte->relid) == RELSTORAGE_EXTERNAL) {
		   /* Get pg_exttable information for the external table */
		   ExtTableEntry *extEntry = GetExtTableEntry(rte->relid);

		   getExternalTableTypeStr(extEntry->fmtcode, extEntry->fmtopts,
		                        &formatterType, &formatterName);
		   pfree(extEntry);
		}

		GpPolicy *targetPolicy = NULL;
		Assert(rte->rtekind == RTE_RELATION);
		targetPolicy = GpPolicyFetch(CurrentMemoryContext, rte->relid);
		if (targetPolicy->nattrs > 0) /* distributed by table */
		{
			context->keep_hash = true;

			if (formatterName != NULL &&
				(strncasecmp(formatterName, "magmatp", strlen("magmatp")) == 0 ||
			     strncasecmp(formatterName, "magmaap", strlen("magmaap")) == 0))
			{
				context->resultRelationHashSegNum =
					default_magma_hash_table_nvseg_per_seg * slaveHostNumber;
			}
			else
			{
				context->resultRelationHashSegNum = targetPolicy->bucketnum;
			}
		} else if (formatterType == ExternalTableType_TEXT ||
               formatterType == ExternalTableType_CSV) {
      context->minimum_segment_num = targetPolicy->bucketnum;
    }
    pfree(targetPolicy);
	}
	/*
	 * This is a CREATE TABLE AS statement
	 * SELECT * INTO newtable from origintable would create a random table default. Not hash table previously.
	 */
	if ((intoPolicy != NULL) && (intoPolicy->nattrs > 0))
	{
		context->isTargetNoMagma = true;
		context->keep_hash = true;
		if (formatterName != NULL &&
			(strncasecmp(formatterName, "magmatp", strlen("magmatp")) == 0 ||
			 strncasecmp(formatterName, "magmaap", strlen("magmaap")) == 0))
		{
			context->resultRelationHashSegNum =
			default_magma_hash_table_nvseg_per_seg * slaveHostNumber;
		}
		else
		{
			context->resultRelationHashSegNum = intoPolicy->bucketnum;
		}
	}

	if (formatterName != NULL)
	{
		pfree(formatterName);
	}

	foreach(lc, context->srtc_context.range_tables)
	{
		GpPolicy *targetPolicy = NULL;
		Relation rel = NULL;
		Oid myrelid = lfirst_oid(lc);

		targetPolicy = GpPolicyFetch(CurrentMemoryContext, myrelid);
		rel = relation_open(myrelid, AccessShareLock);

		if (RelationIsExternal(rel)) {
			/* targetPolicy->bucketnum is bucket number of external table,
			 * whose default value is set to default_segment_num
			 */
			ExtTableEntry* extEnrty = GetExtTableEntry(rel->rd_id);

			bool isPxf = false;
			if (!extEnrty->command && extEnrty->locations) {
				char* first_uri_str = (char *) strVal(lfirst(list_head(extEnrty->locations)));
				if (first_uri_str) {
					Uri* uri = ParseExternalTableUri(first_uri_str);
					if (uri && uri->protocol == URI_CUSTOM && is_pxf_protocol(uri)) {
						isPxf = true;
					} else if (uri && (is_hdfs_protocol(uri) || is_magma_protocol(uri))) {
						relation_close(rel, AccessShareLock);
						if (targetPolicy->nattrs > 0)
						{
							/*select the maximum hash bucket number as hashSegNum of query*/
							int bucketnumTmp = targetPolicy->bucketnum;
							if (is_magma_protocol(uri))
							{
								bucketnumTmp =
									default_magma_hash_table_nvseg_per_seg * slaveHostNumber;
							}
							if (context->hashSegNum < bucketnumTmp)
							{
								context->hashSegNum = bucketnumTmp;
								context->keep_hash = true;
							}
						}
						continue;
					}
				}
			}
			if (extEnrty->command || isPxf) {
				// command external table or pxf case
				if (context->externTableForceSegNum == 0) {
					context->externTableForceSegNum = targetPolicy->bucketnum;
				} else {
					if (context->externTableForceSegNum != targetPolicy->bucketnum) {
						/*
						 * In this case, two external table join but with different bucket number
						 * we cannot allocate the right segment number.
						 */
						elog(ERROR, "All external tables in one query must have the same bucket number!");
					}
				}
			}
			else {
				// gpfdist location case and others
				if (context->externTableLocationSegNum < targetPolicy->bucketnum) {
					context->externTableLocationSegNum = targetPolicy->bucketnum;
					context->minimum_segment_num =  targetPolicy->bucketnum;
				}
			}
		}
		/*for hash relation */
		else if (targetPolicy->nattrs > 0) {
			/*select the maximum hash bucket number as hashSegNum of query*/
			if (context->hashSegNum < targetPolicy->bucketnum) {
				context->hashSegNum = targetPolicy->bucketnum;
				context->keep_hash = true;
			}
		}
		relation_close(rel, AccessShareLock);
		pfree(targetPolicy);
	}

	MemoryContextSwitchTo(context->old_memorycontext);

	return;
}

/*
 * get_virtual_segments: fetch the virtual segments from the
 * resource management.
 */
static List *
get_virtual_segments(QueryResource *resource) {
	List *result = NIL;
	ListCell *lc;

	foreach (lc, resource->segments)
	{
		Segment *info = (Segment *) lfirst(lc);
		VirtualSegmentNode *vs = makeNode(VirtualSegmentNode);
		vs->hostname = pstrdup(
				info->hdfsHostname == NULL ? info->hostname : info->hdfsHostname);
		result = lappend(result, vs);
	}

	return result;
}

/*
 * get_block_locations_and_calculate_table_size: the HDFS block information
 * corresponding to the required relations, and calculate relation size
 */
int64 get_block_locations_and_calculate_table_size(split_to_segment_mapping_context *context) {
	uint64_t allRelationFetchBegintime = 0;
	uint64_t allRelationFetchLeavetime = 0;
	int totalFileCount = 0;
	int hitblocks = 0;
	int allblocks = 0;
	allRelationFetchBegintime = gettime_microsec();
	ListCell *lc;
	int64 total_size = 0;
  bool udOper =
      (((PlannedStmt *)context->srtc_context.base.node)->commandType ==
           CMD_UPDATE ||
       ((PlannedStmt *)context->srtc_context.base.node)->commandType ==
           CMD_DELETE);

  MemoryContextSwitchTo(context->datalocality_memorycontext);

	Snapshot savedActiveSnapshot = ActiveSnapshot;
  if (udOper)
    ActiveSnapshot = SnapshotNow;
  else {
    if (ActiveSnapshot == NULL) ActiveSnapshot = GetTransactionSnapshot();
    ActiveSnapshot = CopySnapshot(ActiveSnapshot);
    ActiveSnapshot->curcid = GetCurrentCommandId();
  }

  List* magmaTableFullNames = NIL;
  List* magmaNonResultRelations = NIL;
	foreach(lc, context->srtc_context.range_tables)
	{
	  Oid rel_oid = lfirst_oid(lc);
		Relation rel = relation_open(rel_oid, AccessShareLock);

		/*
		 * We only consider the data stored in HDFS.
		 */
		bool isDataStoredInHdfs = dataStoredInHdfs(rel);
		bool isdataStoredInMagma = dataStoredInMagma(rel);
		bool isDataStoredInHive = dataStoredInHive(rel);
		if (isDataStoredInHdfs || isdataStoredInMagma || isDataStoredInHive) {
		  if (isDataStoredInHive) {
		    fetchUrlStoredInHive(rel_oid, &(context->hiveUrl));
		  }
			GpPolicy *targetPolicy = GpPolicyFetch(CurrentMemoryContext, rel_oid);
			Relation_Data *rel_data = (Relation_Data *) palloc(sizeof(Relation_Data));
			rel_data->relid = rel_oid;
			rel_data->files = NIL;
			rel_data->partition_parent_relid = 0;
			rel_data->block_count = 0;
			rel_data->serializeSchema = NULL;
			rel_data->serializeSchemaLen = 0;
			rel_data->magmaTableFullName = NULL;

			if (RelationIsAo(rel)) {
				/*
				 * Get pg_appendonly information for this table.
				 */
				AppendOnlyEntry *aoEntry = GetAppendOnlyEntry(rel_oid, ActiveSnapshot);
				/*
				 * Based on the pg_appendonly information, calculate the data
				 * location information associated with this relation.
				 */
				if (RelationIsAoRows(rel)) {
					rel_data->type = DATALOCALITY_APPENDONLY;
					AOGetSegFileDataLocation(rel, aoEntry, ActiveSnapshot, context,
							aoEntry->splitsize, rel_data, &hitblocks,
							&allblocks, targetPolicy);
				} else {
				  // for orc table, we just reuse the parquet logic here
					rel_data->type = DATALOCALITY_PARQUET;
					ParquetGetSegFileDataLocationWrapper(rel, aoEntry, ActiveSnapshot, context,
							context->split_size, rel_data, &hitblocks,
							&allblocks, targetPolicy);
				}
			} else if (RelationIsExternal(rel)) {
				if (isDataStoredInHdfs) {
					// we can't use metadata cache, so hitblocks will always be 0
					rel_data->type = DATALOCALITY_HDFS;
					ExternalGetHdfsFileDataLocation(rel, context, context->split_size,
					                                rel_data, &allblocks,targetPolicy);
				} else if (isDataStoredInHive) {
					// we can't use metadata cache, so hitblocks will always be 0
					rel_data->type = DATALOCALITY_HIVE;
					ExternalGetHiveFileDataLocation(rel, context, context->split_size,
					                                rel_data, &allblocks);
				} else if (isdataStoredInMagma) {
          MagmaTableFullName* mtfn = palloc0(sizeof(MagmaTableFullName));
          mtfn->databaseName = get_database_name(MyDatabaseId);
          mtfn->schemaName = get_namespace_name(rel->rd_rel->relnamespace);
          mtfn->tableName = palloc0(sizeof(rel->rd_rel->relname.data));
          memcpy(mtfn->tableName, &(rel->rd_rel->relname.data), sizeof(rel->rd_rel->relname.data));
          relation_close(rel, AccessShareLock);
          magmaTableFullNames = lappend(magmaTableFullNames, mtfn);

          rel_data->magmaTableFullName = mtfn;
          rel_data->type = DATALOCALITY_MAGMA;
          context->chsl_context.relations = lappend(context->chsl_context.relations,
                                                    rel_data);
          magmaNonResultRelations = lappend(magmaNonResultRelations, rel_data);
          pfree(targetPolicy);
          continue;
				}
			}

      total_size += rel_data->total_size;
      totalFileCount += list_length(rel_data->files);
      //for hash relation
      if (targetPolicy->nattrs > 0) {
        context->hashRelSize += rel_data->total_size;
      } else {
        context->randomRelSize += rel_data->total_size;
      }

			context->chsl_context.relations = lappend(context->chsl_context.relations,
					rel_data);
			pfree(targetPolicy);
		}

		relation_close(rel, AccessShareLock);
	}

  bool useClientCacheDirectly = false;
  if (context->isMagmaTableExist) {
          // start transaction in magma for SELECT/INSERT/UPDATE/DELETE/ANALYZE
          if (PlugStorageGetTransactionStatus() == PS_TXN_STS_DEFAULT)
          {
            PlugStorageStartTransaction();
                  useClientCacheDirectly = true;
          }
          if (((PlannedStmt *)context->srtc_context.base.node)->commandType ==
                  CMD_SELECT ||
              context->isTargetNoMagma) {
            PlugStorageGetTransactionSnapshot(magmaTableFullNames);
          } else {
            PlugStorageGetTransactionId(magmaTableFullNames);
          }
          Assert(PlugStorageGetTransactionStatus() == PS_TXN_STS_STARTED);
  }

  foreach(lc, magmaNonResultRelations) {
          Relation_Data *rel_data = lfirst(lc);
          ExternalGetMagmaRangeDataLocation(rel_data->magmaTableFullName->databaseName,
                                rel_data->magmaTableFullName->schemaName,
                                rel_data->magmaTableFullName->tableName,
                                context, context->split_size,
                                rel_data, useClientCacheDirectly, &allblocks);
          total_size += rel_data->total_size;
          totalFileCount += list_length(rel_data->files);
          context->hashRelSize += rel_data->total_size;
  }

  foreach (lc, magmaTableFullNames) {
          MagmaTableFullName* mtfn = lfirst(lc);
          pfree(mtfn->databaseName);
          pfree(mtfn->schemaName);
          pfree(mtfn->tableName);
  }
  list_free_deep(magmaTableFullNames);

	ActiveSnapshot = savedActiveSnapshot;

	MemoryContextSwitchTo(context->old_memorycontext);

	allRelationFetchLeavetime = gettime_microsec();
	int eclaspeTime = allRelationFetchLeavetime - allRelationFetchBegintime;
	double hitrate = (allblocks == 0) ? 0 : (double) hitblocks / allblocks;
	if (debug_print_split_alloc_result) {
		elog(LOG, "fetch blocks of %d files overall execution time: %d us with hit rate %f",
				totalFileCount, eclaspeTime, hitrate);
	}
	context->total_file_count = totalFileCount;
	context->total_size = total_size;
	context->metadata_cache_time_us = eclaspeTime;

	if(debug_datalocality_time){
		elog(LOG, "metadata overall execution time: %d us. \n", eclaspeTime);
	}
	return total_size;
}

bool dataStoredInHdfs(Relation rel) {
	if (RelationIsAo(rel)) {
		return true;
	} else if (RelationIsExternal(rel)) {
		ExtTableEntry* extEnrty = GetExtTableEntry(rel->rd_id);
		bool isHdfsProtocol = false;
		if (!extEnrty->command && extEnrty->locations) {
			char* first_uri_str = (char *) strVal(lfirst(list_head(extEnrty->locations)));
			if (first_uri_str) {
				Uri* uri = ParseExternalTableUri(first_uri_str);
				if (uri && is_hdfs_protocol(uri)) {
					isHdfsProtocol = true;
				}
			}
		}
		return isHdfsProtocol;
	}
	return false;
}

bool dataStoredInHive(Relation rel)
{
	if (RelationIsExternal(rel)) {
		ExtTableEntry* extEnrty = GetExtTableEntry(rel->rd_id);
		bool isHiveProtocol = false;
		if (!extEnrty->command && extEnrty->locations) {
			char* first_uri_str = (char *) strVal(lfirst(list_head(extEnrty->locations)));
			if (first_uri_str) {
				Uri* uri = ParseExternalTableUri(first_uri_str);
				if (uri && is_hive_protocol(uri)) {
					isHiveProtocol = true;
				}
			}
		}
		return isHiveProtocol;
	}
	return false;
}

bool dataStoredInMagma(Relation rel)
{
	if (RelationIsExternal(rel)) {
		ExtTableEntry* extEnrty = GetExtTableEntry(rel->rd_id);
		bool isMagmaProtocol = false;
		if (!extEnrty->command && extEnrty->locations) {
			char* first_uri_str = (char *) strVal(lfirst(list_head(extEnrty->locations)));
			if (first_uri_str) {
				Uri* uri = ParseExternalTableUri(first_uri_str);
				if (uri && is_magma_protocol(uri)) {
					isMagmaProtocol = true;
				}
			}
		}
		return isMagmaProtocol;
	}
	return false;
}

/*
 * fetchDistributionPolicy
 *
 * fetch distribution policy for table
 * rg is a collection of multiple ranges
 * range is like hdfs blocklocation in magma
 * splits is same as range in hawq
 */
void fetchDistributionPolicy(Oid relid, int32 *n_dist_keys, int16 **dist_keys)
{
  Relation dist_policy_rel = heap_open(GpPolicyRelationId, AccessShareLock);
  TupleDesc dist_policy_des = RelationGetDescr(dist_policy_rel);

  HeapTuple tuple;
  ScanKeyData scan_key;
  SysScanDesc scan_des;

  ScanKeyInit(&scan_key, Anum_gp_policy_localoid, BTEqualStrategyNumber,
              F_OIDEQ, ObjectIdGetDatum(relid));
  scan_des = systable_beginscan(dist_policy_rel, InvalidOid, FALSE, SnapshotNow,
                                1, &scan_key);
  tuple = systable_getnext(scan_des);
  if (HeapTupleIsValid(tuple)) {
    bool isnull;
    Datum dist_policy;
    dist_policy =
        fastgetattr(tuple, Anum_gp_policy_attrnums, dist_policy_des, &isnull);
    Assert(isnull == false);

    ArrayType *array_type;
    array_type = DatumGetArrayTypeP(dist_policy);
    Assert(ARR_NDIM(array_type) == 1);
    Assert(ARR_ELEMTYPE(array_type) == INT2OID);
    Assert(ARR_LBOUND(array_type)[0] == 1);
    *n_dist_keys = ARR_DIMS(array_type)[0];
    *dist_keys = (int16 *)ARR_DATA_PTR(array_type);
    Assert(*n_dist_keys >= 0);

    systable_endscan(scan_des);
    heap_close(dist_policy_rel, AccessShareLock);
  } else {
    systable_endscan(scan_des);
    heap_close(dist_policy_rel, AccessShareLock);
    elog(ERROR,
         "fetchDistributionPolicy: get no table in gp_distribution_policy");
  }
}

/*
 * every rg in magma may be has more than one range, according to the splits
 * info in hawq, established a range to rg correspondence
 */
List *magma_build_range_to_rg_map(List *splits, uint32 *range_to_rg_map)
{
  assert(splits != NULL);

  List *rg = NIL;
  ListCell *lc_split;
  foreach (lc_split, splits) {
    List *split = (List *)lfirst(lc_split);
    ListCell *lc_range;
    foreach (lc_range, split) {
      FileSplit origFS = (FileSplit)lfirst(lc_range);
      /* move the range room id from the highest 16 bits of range_id to
       * low 16 bits of replicaGroup_id */
      range_to_rg_map[origFS->range_id] = origFS->replicaGroup_id;
      /* not find the rgid in the list, add it */
      if (list_find_int(rg, origFS->replicaGroup_id) == -1) {
        rg = lappend_int(rg, origFS->replicaGroup_id);
        elog(DEBUG3, "executor get rg and its url: [%d] = [%s]",
             origFS->replicaGroup_id, origFS->ext_file_uri_string);
      }
      elog(DEBUG3, "executor get range to rg map entry: range[%d] = %d",
           origFS->range_id, origFS->replicaGroup_id);
    }
  }
  /* sanity check to make sure all range is in the map */
  elog(DEBUG3, "executor get rg with url size: %d", list_length(rg));
  return rg;
}

/*
 * used to establish the corresponding relationship between rg and url
 * url is the address of one rg, client can use this url connect to rg
 */
void magma_build_rg_to_url_map(List *splits, List *rg, uint16 *rgIds,
                               char **rgUrls)
{
  int rgSize = list_length(rg);
  int16_t map[rgSize];
  memset(map, 0, sizeof(map));
  int16_t index = 0;
  ListCell *lc_split1;
  foreach (lc_split1, splits) {
    List *split = (List *)lfirst(lc_split1);
    ListCell *lc_range;
    foreach (lc_range, split) {
      if (index >= rgSize) {
        break;
      }
      FileSplit origFS = (FileSplit)lfirst(lc_range);
      int pos = list_find_int(rg, origFS->replicaGroup_id);
      if (pos == -1) {
        elog(ERROR, "executor cannot find rgid [%d] and it should be in list",
             origFS->replicaGroup_id);
      } else {
        if (map[pos] > 0) {
          elog(DEBUG3, "continue with pos %d, map[pos]: %d", pos, map[pos]);
          continue;
        }
        map[pos]++;
        rgIds[index] = origFS->replicaGroup_id;
        rgUrls[index] = pstrdup(origFS->ext_file_uri_string);
        elog(DEBUG3, "executor add rg and its url: rgid[%d] = [%s]",
             rgIds[index], rgUrls[index]);
        index++;
      }
    }
    if (index >= rgSize) {
      break;
    }
  }
  assert(rgSize == index);
}

/*determine whether the relation is a magma table by Oid*/
bool dataStoredInMagmaByOid(Oid relid)
{
	if (get_relation_storage_type(relid) == RELSTORAGE_EXTERNAL) {
		ExtTableEntry* extEnrty = GetExtTableEntry(relid);
		bool isMagmaProtocol = false;
		if (!extEnrty->command && extEnrty->locations) {
			char* first_uri_str = (char *) strVal(lfirst(list_head(extEnrty->locations)));
			if (first_uri_str) {
				Uri* uri = ParseExternalTableUri(first_uri_str);
				if (uri && is_magma_protocol(uri)) {
					isMagmaProtocol = true;
				}
			}
		}
		return isMagmaProtocol;
	}
	return false;
}

/* fetch table url if relation is hive format */
void fetchUrlStoredInHive(Oid relid, char **hiveUrl) {
  if (get_relation_storage_type(relid) == RELSTORAGE_EXTERNAL) {
    ExtTableEntry *extEnrty = GetExtTableEntry(relid);
    if (!extEnrty->command && extEnrty->locations) {
      char *first_uri_str =
          (char *)strVal(lfirst(list_head(extEnrty->locations)));
      if (first_uri_str) {
        Uri *uri = ParseExternalTableUri(first_uri_str);
        if (uri && is_hive_protocol(uri)) {
          /* get hdfs path by dbname and tblname. */
          uint32_t pathLen = strlen(uri->path);
          char dbname[pathLen];
          memset(dbname, 0, pathLen);
          char tblname[pathLen];
          memset(tblname, 0, pathLen);
          sscanf(uri->path, "/%[^/]/%s", dbname, tblname);
          statusHiveC status;
          getHiveDataDirectoryC(uri->hostname, uri->port, dbname, tblname,
                                hiveUrl, &status);
          if (status.errorCode != ERRCODE_SUCCESSFUL_COMPLETION) {
            FreeExternalTableUri(uri);
            elog(ERROR,
                 "hiveprotocol_validate : "
                 "failed to get table info, %s ",
                 status.errorMessage);
          }
          FreeExternalTableUri(uri);
        }
      }
    }
  }
}

/*
 * search_host_in_stat_context: search a host name in the statistic
 * context; if not found, create a new one.
 */
static HostDataVolumeInfo *
search_host_in_stat_context(split_to_segment_mapping_context *context,
		char *hostname) {
	HostnameIndexKey key;
	HostnameIndexEntry *entry;
	bool found;

	if (strncmp(hostname,"127.0.0.1", strlen(hostname)) == 0) {
		strncpy(hostname, "localhost", sizeof("localhost"));
	}
	int a,b,c,d = 0;
	if (4==sscanf(hostname,"%d.%d.%d.%d",&a,&b,&c,&d)) {
		if (0<=a && a<=255
				&& 0<=b && b<=255
				&& 0<=c && c<=255
				&& 0<=d && d<=255) {
		  char *ipaddr = pstrdup(hostname);
		  char *hostnameNew = search_hostname_by_ipaddr(ipaddr);
			pfree(ipaddr);
		  MemSet(&(key.hostname), 0, HOSTNAME_MAX_LENGTH);
		  strncpy(key.hostname, hostnameNew, HOSTNAME_MAX_LENGTH - 1);
		}
	} else {
	MemSet(&(key.hostname), 0, HOSTNAME_MAX_LENGTH);
	strncpy(key.hostname, hostname, HOSTNAME_MAX_LENGTH - 1);
	}
	entry = (HostnameIndexEntry *) hash_search(context->hostname_map,
			(void *) &key, HASH_ENTER, &found);

	if (!found) {
		if (context->dds_context.size >= context->dds_context.max_size) {
			int offset = context->dds_context.max_size;
			context->dds_context.max_size <<= 1;
			context->dds_context.volInfos = (HostDataVolumeInfo *) repalloc(
					context->dds_context.volInfos,
					sizeof(HostDataVolumeInfo) * context->dds_context.max_size);
			MemSet(context->dds_context.volInfos + offset, 0,
					sizeof(HostDataVolumeInfo)
							* (context->dds_context.max_size - offset));
		}
		entry->index = context->dds_context.size++;
		context->dds_context.volInfos[entry->index].hashEntry = entry;
		context->dds_context.volInfos[entry->index].datavolume = 0;
		context->dds_context.volInfos[entry->index].occur_count = 0;
	}
	if(context->dds_context.size > slaveHostNumber) {
		for (int i = 0; i < context->dds_context.size ; i++) {
			elog(DEBUG3,"the %d th hostname is %s,", i,
			context->dds_context.volInfos[i].hashEntry->key.hostname);
		}
		/* the hostname principle maybe different for Resource Manager, hdfs and magma,
		 * this will cause the same host be recognised as different host, need to be fixed in future.
		 * elog(ERROR,"the host number calculated in datalocality is larger than "
				"the host exits. Maybe the same host has different expression,"
				"please check.");
		*/
	}
	return context->dds_context.volInfos + entry->index;
}

/*
 * fetch_hdfs_data_block_location: given a HDFS file path,
 * collect all its data block location information.
 */
static BlockLocation *
fetch_hdfs_data_block_location(char *filepath, int64 len, int *block_num,
		RelFileNode rnode, uint32_t segno, double* hit_ratio, bool index_scan) {
	// for fakse test, the len of file always be zero
	if(len == 0  && !debug_fake_datalocality){
		*hit_ratio = 0.0;
		return NULL;
	}
	BlockLocation *locations = NULL;
	HdfsFileInfo *file_info = NULL;
	//double hit_ratio;
	uint64_t beginTime;
	beginTime = gettime_microsec();

	if (metadata_cache_enable && !index_scan) {
		file_info = CreateHdfsFileInfo(rnode, segno);
		if (metadata_cache_testfile && metadata_cache_testfile[0]) {
			locations = GetHdfsFileBlockLocationsForTest(filepath, len, block_num);
			if (locations) {
				DumpHdfsFileBlockLocations(locations, *block_num);
			}
		} else {
			locations = GetHdfsFileBlockLocations(file_info, len, block_num,
					hit_ratio);
		}
		DestroyHdfsFileInfo(file_info);
	} else {
	  BlockLocation *hdfs_locations = HdfsGetFileBlockLocations(filepath, len, block_num);
	  locations = CreateHdfsFileBlockLocations(hdfs_locations, *block_num);
	  if (hdfs_locations)
	  {
	    HdfsFreeFileBlockLocations(hdfs_locations, *block_num);
	  }
	}
	if (debug_print_split_alloc_result) {
		uint64 endTime = gettime_microsec();
		int eclaspeTime = endTime - beginTime;
		elog(LOG, "fetch blocks of file relationid %d segno %d execution time:"
		" %d us with hit rate %f \n", rnode.relNode,segno,eclaspeTime,*hit_ratio);
	}
	if(locations == NULL && !debug_fake_datalocality){
		ereport(ERROR,
						(errcode(ERRCODE_IO_ERROR),
								errmsg("cannot fetch block locations"),
								errdetail("%s", HdfsGetLastError())));
	}

  if (locations != NULL) {
    /* we replace host names by ip addresses */
    for (int k = 0; k < *block_num; k++) {
      /*initialize rangeId and RGID for ao table*/
      locations[k].rangeId = -1;
      locations[k].replicaGroupId = -1;
      for (int j = 0; j < locations[k].numOfNodes; j++) {
        char *colon = strchr(locations[k].names[j], ':');
        if (colon != NULL) {
          pfree(locations[k].hosts[j]);
          int l = colon - locations[k].names[j];
          locations[k].hosts[j] = palloc0(l + 1);
          memcpy(locations[k].hosts[j], locations[k].names[j], l);
          locations[k].hosts[j][l] = '\0'; /* dup setting to terminate */
        }
      }
    }
  }

	return locations;
}

/*
 * free_hdfs_data_block_location: free the memory allocated when
 * calling fetch_hdfs_data_block_location.
 */
static void free_hdfs_data_block_location(BlockLocation *locations,
		int block_num) {
	if (metadata_cache_enable) {
		FreeHdfsFileBlockLocations(locations, block_num);
	} else {
		HdfsFreeFileBlockLocations(locations, block_num);
	}

	return;
}

static void free_magma_data_range_locations(BlockLocation *locations,
                                                  int range_num) {
	Insist(locations != NULL);
	Insist(range_num >= 0);

	for (int i = 0; i < range_num; ++i)
	{
		if (locations[i].hosts)
		{
			pfree(locations[i].hosts);
		}

		if (locations[i].names)
		{
			pfree(locations[i].names);
		}

		if (locations[i].topologyPaths)
		{
			pfree(locations[i].topologyPaths);
		}
	}
}


/*
 * update_data_dist_stat: update the data distribution
 * statistics.
 */
static Block_Host_Index *
update_data_dist_stat(split_to_segment_mapping_context *context,
		BlockLocation *locations, int block_num) {
	int i;
	Block_Host_Index *hostIDs;

	hostIDs = (Block_Host_Index *) palloc(sizeof(Block_Host_Index) * block_num);
	for (i = 0; i < block_num; i++) {
		int j;
		hostIDs[i].replica_num = locations[i].numOfNodes;
		hostIDs[i].insertHost = -1;
		hostIDs[i].hostIndex = (int *) palloc(sizeof(int) * hostIDs[i].replica_num);
		hostIDs[i].hostIndextoSort = (Host_Index *) palloc(
				sizeof(Host_Index) * hostIDs[i].replica_num);

		for (j = 0; j < locations[i].numOfNodes; j++) {
			char *hostname = pstrdup(locations[i].hosts[j]);
			HostDataVolumeInfo *info = search_host_in_stat_context(context, hostname);
			info->datavolume += locations[i].length;
			hostIDs[i].hostIndextoSort[j].index = info->hashEntry->index;
			hostIDs[i].hostIndextoSort[j].hostname = hostname;
			if (output_hdfs_block_location) {
				elog(LOG, "block%d has replica %d on %s",i+1,j+1,hostname);
			}
		}
		qsort(hostIDs[i].hostIndextoSort, locations[i].numOfNodes,
				sizeof(Host_Index), compare_hostid);
		for (j = 0; j < locations[i].numOfNodes; j++) {
			hostIDs[i].hostIndex[j] = hostIDs[i].hostIndextoSort[j].index;
		}
		pfree(hostIDs[i].hostIndextoSort);
	}

	return hostIDs;
}

/*
 * check hdfs file length equals to pg_aoseg file logic length
 */
static void double_check_hdfs_metadata_logic_length(BlockLocation * locations,int block_num,int64 logic_len) {
	//double check hdfs file length equals to pg_aoseg logic length
	int64 hdfs_file_len = 0;
	for(int i=0;i<block_num;i++) {
		hdfs_file_len += locations[i].length;
	}
	if(logic_len > hdfs_file_len) {
		elog(ERROR, "hdfs file length does not equal to metadata logic length!");
	}
}

/*
 * AOGetSegFileDataLocation: fetch the data location of the
 * segment files of the AO relation.
 */
static void AOGetSegFileDataLocation(Relation relation,
		AppendOnlyEntry *aoEntry, Snapshot metadataSnapshot,
		split_to_segment_mapping_context *context, int64 splitsize,
		Relation_Data *rel_data, int* hitblocks,
		int* allblocks, GpPolicy *targetPolicy) {
	char *basepath;
	char *segfile_path;
	int filepath_maxlen;

	Relation pg_aoseg_rel;
	TupleDesc pg_aoseg_dsc;
	HeapTuple tuple;
	SysScanDesc aoscan;

	int64 total_size = 0;

	/*
	 * calculate the base file path for the segment files.
	 */
	basepath = relpath(relation->rd_node);
	filepath_maxlen = strlen(basepath) + 9;
	segfile_path = (char *) palloc0(filepath_maxlen);

	// fake data locality
	if (debug_fake_datalocality) {
		fpaoseg = fopen("/tmp/aoseg.result", "r");
		if (fpaoseg == NULL) {
			elog(ERROR, "Could not open file!");
			return;
		}
		int fileCount = 0;
		int reloid = 0;
		while (true) {
			int res = fscanf(fpaoseg, "%d", &reloid);
			if (res == 0) {
				elog(ERROR, "cannot find relation in fake aoseg!");
			}
			res = fscanf(fpaoseg, "%d", &fileCount);
			if (res == 0) {
				elog(ERROR, "cannot find file count in fake aoseg!");
			}
			if (rel_data->relid == reloid) {
				break;
			}
		}
		fclose(fpaoseg);
		for (int i = 0; i < fileCount; i++) {
			BlockLocation *locations = NULL;
			int block_num =0;
			Relation_File *file;

			int segno = i + 1;
			int64 logic_len = 0;
			bool isRelationHash = true;
			if (targetPolicy->nattrs == 0) {
				isRelationHash = false;
			}

			if (!context->keep_hash || !isRelationHash) {
				FormatAOSegmentFileName(basepath, segno, -1, 0, &segno, segfile_path);
				double hit_ratio=0.0;
				locations = fetch_hdfs_data_block_location(segfile_path, logic_len,
						&block_num, relation->rd_node, segno, &hit_ratio, false);
				*allblocks += block_num;
				*hitblocks += block_num * hit_ratio;
				//fake data locality need to recalculate logic length
				if ((locations != NULL) && (block_num > 0)) {
					logic_len = 0;
					for (int i = 0; i < block_num; i++) {
						logic_len += locations[i].length;
					}
				}
				if ((locations != NULL) && (block_num > 0)) {
					Block_Host_Index * host_index = update_data_dist_stat(context,
							locations, block_num);

					for (int k = 0; k < block_num; k++) {
						fprintf(fp, "block %d of file %d of relation %d is on ", k, segno,
								rel_data->relid);
						for (int j = 0; j < locations[k].numOfNodes; j++) {
							char *hostname = pstrdup(locations[k].hosts[j]); /* locations[i].hosts[j]; */
							fprintf(fp, "host No%d name:%s, ", host_index[k].hostIndex[j],
									hostname);
						}
						fprintf(fp, "\n");
					}
					fflush(fp);

					file = (Relation_File *) palloc(sizeof(Relation_File));
					file->segno = segno;
					file->block_num = block_num;
					file->locations = locations;
					file->hostIDs = host_index;
					file->logic_len = logic_len;

					if (aoEntry->majorversion < 2) {
						File_Split *split = (File_Split *) palloc(sizeof(File_Split));
						split->offset = 0;
						split->length = logic_len;
						split->ext_file_uri = NULL;
						file->split_num = 1;
						file->splits = split;
						context->total_split_count += file->split_num;
					} else {
						File_Split *splits;
						int split_num;
						int realSplitNum;
						int64 offset = 0;
						// split equals to block
						split_num = file->block_num;
						splits = (File_Split *) palloc(sizeof(File_Split) * split_num);
						for (realSplitNum = 0; realSplitNum < split_num; realSplitNum++) {
							splits[realSplitNum].host = -1;
							splits[realSplitNum].is_local_read = true;
							splits[realSplitNum].offset = offset;
							splits[realSplitNum].ext_file_uri = NULL;
							splits[realSplitNum].length =
									file->locations[realSplitNum].length;
							if (logic_len - offset <= splits[realSplitNum].length) {
								splits[realSplitNum].length = logic_len - offset;
								realSplitNum++;
								break;
							}
							offset += splits[realSplitNum].length;
						}
						file->split_num = realSplitNum;
						file->splits = splits;
						context->total_split_count += realSplitNum;
					}
					rel_data->files = lappend(rel_data->files, file);
				}
			} else {

				FormatAOSegmentFileName(basepath, segno, -1, 0, &segno, segfile_path);
				double hit_ratio = 0.0;
				locations = fetch_hdfs_data_block_location(segfile_path, logic_len,
						&block_num, relation->rd_node, segno, &hit_ratio, false);
				*allblocks += block_num;
				*hitblocks += block_num * hit_ratio;
				//fake data locality need to recalculate logic length
				if ((locations != NULL) && (block_num > 0)) {
					logic_len = 0;
					for (int i = 0; i < block_num; i++) {
						logic_len += locations[i].length;
					}
				}

				File_Split *split = (File_Split *) palloc(sizeof(File_Split));
				file = (Relation_File *) palloc0(sizeof(Relation_File));
				file->segno = segno;
				split->range_id = -1;
				split->replicaGroup_id = -1;
				split->offset = 0;
				split->length = logic_len;
				split->host = -1;
				split->is_local_read = true;
				split->ext_file_uri = NULL;
				file->split_num = 1;
				file->splits = split;
				file->logic_len = logic_len;
				if ((locations != NULL) && (block_num > 0)) {
					Block_Host_Index * host_index = update_data_dist_stat(context,
							locations, block_num);
					// fake data locality
					for (int k = 0; k < block_num; k++) {
						fprintf(fp, "block %d of file %d of relation %d is on ", k, segno,
								rel_data->relid);
						for (int j = 0; j < locations[k].numOfNodes; j++) {
							char *hostname = pstrdup(locations[k].hosts[j]); /* locations[i].hosts[j]; */
							fprintf(fp, "host No%d name:%s, ", host_index[k].hostIndex[j],
									hostname);
						}
						fprintf(fp, "\n");
					}
					fflush(fp);

					file->block_num = block_num;
					file->locations = locations;
					file->hostIDs = host_index;
					// for hash, we need to add block number to total_split_count
					context->total_split_count += block_num;
				} else {
					file->block_num = 0;
					file->locations = NULL;
					file->hostIDs = NULL;
				}

				rel_data->files = lappend(rel_data->files, file);
			}

			total_size += logic_len;
			if (!context->keep_hash) {
				MemSet(segfile_path, 0, filepath_maxlen);
			}
		}
	} else {
		pg_aoseg_rel = heap_open(aoEntry->segrelid, AccessShareLock);
		pg_aoseg_dsc = RelationGetDescr(pg_aoseg_rel);
		aoscan = systable_beginscan(pg_aoseg_rel, InvalidOid, FALSE,
				metadataSnapshot, 0, NULL);

		while (HeapTupleIsValid(tuple = systable_getnext(aoscan))) {
			BlockLocation *locations = NULL;
			int block_num = 0;
			Relation_File *file;

			int segno = DatumGetInt32(
					fastgetattr(tuple, Anum_pg_aoseg_segno, pg_aoseg_dsc,
					NULL));
			int64 logic_len = (int64) DatumGetFloat8(
					fastgetattr(tuple, Anum_pg_aoseg_eof, pg_aoseg_dsc, NULL));
			context->total_metadata_logic_len += logic_len;
			bool isRelationHash = true;
			if (targetPolicy->nattrs == 0) {
				isRelationHash = false;
			}

			if (!context->keep_hash || !isRelationHash) {
				FormatAOSegmentFileName(basepath, segno, -1, 0, &segno, segfile_path);
				double hit_ratio = 0.0;
				locations = fetch_hdfs_data_block_location(segfile_path, logic_len,
						&block_num, relation->rd_node, segno, &hit_ratio, false);
				*allblocks += block_num;
				*hitblocks += block_num * hit_ratio;
				if ((locations != NULL) && (block_num > 0)) {
					Block_Host_Index * host_index = update_data_dist_stat(context,
							locations, block_num);
					double_check_hdfs_metadata_logic_length(locations, block_num, logic_len);

					file = (Relation_File *) palloc(sizeof(Relation_File));
					file->segno = segno;
					file->block_num = block_num;
					file->locations = locations;
					file->hostIDs = host_index;
					file->logic_len = logic_len;

					if (aoEntry->majorversion < 2) {
						File_Split *split = (File_Split *) palloc(sizeof(File_Split));
						split->offset = 0;
						split->length = logic_len;
						split->range_id = -1;
						split->replicaGroup_id = -1;
						split->ext_file_uri = NULL;
						file->split_num = 1;
						file->splits = split;
						context->total_split_count += file->split_num;
					} else {
						File_Split *splits;
						int split_num;
						// for truncate real split number may be less than hdfs split number
						int realSplitNum;
						int64 offset = 0;
						// split equals to block
						split_num = file->block_num;
						splits = (File_Split *) palloc(sizeof(File_Split) * split_num);
						for (realSplitNum = 0; realSplitNum < split_num; realSplitNum++) {
							splits[realSplitNum].host = -1;
							splits[realSplitNum].is_local_read = true;
							splits[realSplitNum].offset = offset;
							splits[realSplitNum].range_id = -1;
							splits[realSplitNum].replicaGroup_id = -1;
							splits[realSplitNum].ext_file_uri = NULL;
							splits[realSplitNum].length =
									file->locations[realSplitNum].length;
							if (logic_len - offset <= splits[realSplitNum].length) {
								splits[realSplitNum].length = logic_len - offset;
								realSplitNum++;
								break;
							}
							offset += splits[realSplitNum].length;
						}
						file->split_num = realSplitNum;
						file->splits = splits;
						context->total_split_count += realSplitNum;
					}
					rel_data->files = lappend(rel_data->files, file);
				}
			} else {

				FormatAOSegmentFileName(basepath, segno, -1, 0, &segno, segfile_path);
				double hit_ratio = 0.0;
				locations = fetch_hdfs_data_block_location(segfile_path, logic_len,
						&block_num, relation->rd_node, segno, &hit_ratio, false);
				*allblocks += block_num;
				*hitblocks += block_num * hit_ratio;
				//fake data locality need to recalculate logic length
				if (debug_fake_datalocality) {
					if ((locations != NULL) && (block_num > 0)) {
						logic_len = 0;
						for (int i = 0; i < block_num; i++) {
							logic_len += locations[i].length;
						}
					}
				}
				//end fake

				File_Split *split = (File_Split *) palloc(sizeof(File_Split));
				file = (Relation_File *) palloc0(sizeof(Relation_File));
				file->segno = segno;
				split->range_id = -1;
				split->replicaGroup_id = -1;
				split->offset = 0;
				split->length = logic_len;
				split->host = -1;
				split->is_local_read = true;
				split->ext_file_uri = NULL;
				file->split_num = 1;
				file->splits = split;
				file->logic_len = logic_len;
				if ((locations != NULL) && (block_num > 0)) {
					Block_Host_Index * host_index = update_data_dist_stat(context,
							locations, block_num);
					double_check_hdfs_metadata_logic_length(locations, block_num, logic_len);
					// fake data locality
					if (debug_fake_datalocality) {
						for (int k = 0; k < block_num; k++) {
							fprintf(fp, "block %d of file %d of relation %d is on ", k, segno,
									rel_data->relid);
							for (int j = 0; j < locations[k].numOfNodes; j++) {
								char *hostname = pstrdup(locations[k].hosts[j]); /* locations[i].hosts[j]; */
								fprintf(fp, "host No%d name:%s, ", host_index[k].hostIndex[j],
										hostname);
							}
							fprintf(fp, "\n");
						}
						fflush(fp);
					}
					file->block_num = block_num;
					file->locations = locations;
					file->hostIDs = host_index;
					// for hash, we need to add block number to total_split_count
					context->total_split_count += block_num;
				} else {
					file->block_num = 0;
					file->locations = NULL;
					file->hostIDs = NULL;
				}

				rel_data->files = lappend(rel_data->files, file);
			}

			total_size += logic_len;
			if (!context->keep_hash) {
				MemSet(segfile_path, 0, filepath_maxlen);
			}
		}

		systable_endscan(aoscan);
		heap_close(pg_aoseg_rel, AccessShareLock);
	}

	pfree(segfile_path);

	rel_data->total_size = total_size;

	return;
}

static List *GetAllIdxScanIds(collect_scan_rangetable_context *context,
                              Oid rd_id) {
  List *ret = NULL;

  ListCell *table;
  ListCell *id;
  for (table = list_head(context->indexscan_range_tables),
      id = list_head(context->indexscan_indexs);
       table != NULL; table = lnext(table), id = lnext(id)) {
    if (lfirst_oid(table) == rd_id) {
      if (ret == NULL) {
        ret = list_make1_oid(lfirst_oid(id));
      } else {
        ret = lappend_oid(ret, lfirst_oid(id));
      }
    }
  }

  return ret;
}

/*
 * ParquetGetSegFileDataLocation: fetch the data location of the
 * segment files of the Parquet relation.
 */
static void ParquetGetSegFileDataLocation(Relation relation,
		Oid segrelid, Snapshot metadataSnapshot, List* idx_scan_ids,
		split_to_segment_mapping_context *context, int64 splitsize,
		Relation_Data *rel_data, int* hitblocks,
		int* allblocks, GpPolicy *targetPolicy) {
	bool index_scan = idx_scan_ids != NULL;
	char *basepath;
	char *segfile_path;
	int filepath_maxlen;
	int64 total_size = 0;

	Relation pg_parquetseg_rel;
	TupleDesc pg_parquetseg_dsc;
	HeapTuple tuple;
	SysScanDesc parquetscan;

	basepath = relpath(relation->rd_node);
	filepath_maxlen = strlen(basepath) + 25;
	segfile_path = (char *) palloc0(filepath_maxlen);

	pg_parquetseg_rel = heap_open(segrelid, AccessShareLock);
	pg_parquetseg_dsc = RelationGetDescr(pg_parquetseg_rel);
	parquetscan = systable_beginscan(pg_parquetseg_rel, InvalidOid, FALSE,
			metadataSnapshot, 0, NULL);

	while (HeapTupleIsValid(tuple = systable_getnext(parquetscan))) {
		BlockLocation *locations;
		int block_num = 0;
		Relation_File *file;
		int segno = 0;
		int64 logic_len = 0;
		Oid idx_scan_id = InvalidOid;
		if (index_scan){
		  idx_scan_id = DatumGetObjectId(
		      fastgetattr(tuple, Anum_pg_orcseg_idx_idxoid, pg_parquetseg_dsc, NULL));
		 if  (!list_member_oid(idx_scan_ids, idx_scan_id)) continue;
		  segno = DatumGetInt32(
		      fastgetattr(tuple, Anum_pg_orcseg_idx_segno, pg_parquetseg_dsc, NULL));
		  logic_len = (int64) DatumGetFloat8(
		      fastgetattr(tuple, Anum_pg_orcseg_idx_eof, pg_parquetseg_dsc, NULL));
		} else {
		  segno = DatumGetInt32(
		      fastgetattr(tuple, Anum_pg_parquetseg_segno, pg_parquetseg_dsc, NULL));
		  logic_len = (int64) DatumGetFloat8(
		      fastgetattr(tuple, Anum_pg_parquetseg_eof, pg_parquetseg_dsc, NULL));
		}
		context->total_metadata_logic_len += logic_len;
		bool isRelationHash = true;
		if (targetPolicy->nattrs == 0) {
			isRelationHash = false;
		}

		if (index_scan) {
		  FormatAOSegmentIndexFileName(basepath, segno, idx_scan_id, -1, 0, &segno, segfile_path);
		} else {
		  FormatAOSegmentFileName(basepath, segno, -1, 0, &segno, segfile_path);
		}
		if (!context->keep_hash || !isRelationHash) {
			double hit_ratio = 0.0;
			locations = fetch_hdfs_data_block_location(segfile_path, logic_len,
					&block_num, relation->rd_node, segno, &hit_ratio, index_scan);
			*allblocks += block_num;
			*hitblocks += block_num * hit_ratio;
			//fake data locality need to recalculate logic length
			if (debug_fake_datalocality) {
				if ((locations != NULL) && (block_num > 0)) {
					logic_len = 0;
					for (int i = 0; i < block_num; i++) {
						logic_len += locations[i].length;
					}
				}
			}
			//end fake
			if ((locations != NULL) && (block_num > 0)) {
				File_Split *splits;
				int split_num;
				int64 offset = 0;
				Block_Host_Index * host_index = update_data_dist_stat(context,
						locations, block_num);

				double_check_hdfs_metadata_logic_length(locations, block_num, logic_len);

				file = (Relation_File *) palloc(sizeof(Relation_File));
				file->segno = segno;
				file->block_num = block_num;
				file->locations = locations;
				file->hostIDs = host_index;
				file->logic_len = logic_len;

				// split equals to block
				int realSplitNum = 0;
				split_num = file->block_num;
				splits = (File_Split *) palloc(sizeof(File_Split) * split_num);

				for (realSplitNum = 0; realSplitNum < split_num; realSplitNum++) {
					splits[realSplitNum].host = -1;
					splits[realSplitNum].is_local_read = true;
					splits[realSplitNum].range_id = -1;
					// XXX(sxwang): hack way to pass idx_scan_id.
					splits[realSplitNum].replicaGroup_id = idx_scan_id;
					splits[realSplitNum].offset = offset;
					splits[realSplitNum].length = file->locations[realSplitNum].length;
					splits[realSplitNum].logiceof = logic_len;
					splits[realSplitNum].ext_file_uri = NULL;

					if (logic_len - offset <= splits[realSplitNum].length) {
						splits[realSplitNum].length = logic_len - offset;
						realSplitNum++;
						break;
					}
					offset += splits[realSplitNum].length;
				}
				file->split_num = realSplitNum;
				file->splits = splits;
				context->total_split_count += realSplitNum;

				rel_data->files = lappend(rel_data->files, file);
			}
		} else {
			double hit_ratio = 0.0;
			locations = fetch_hdfs_data_block_location(segfile_path, logic_len,
					&block_num, relation->rd_node, segno, &hit_ratio, index_scan);
			*allblocks += block_num;
			*hitblocks += block_num * hit_ratio;
			File_Split *split = (File_Split *) palloc(sizeof(File_Split));
			file = (Relation_File *) palloc0(sizeof(Relation_File));
			file->segno = segno;
			split->offset = 0;
			split->range_id = -1;
			// XXX(sxwang): hack way to pass idx_scan_id.
			split->replicaGroup_id = idx_scan_id;
			split->length = logic_len;
			split->logiceof = logic_len;
			split->host = -1;
			split->is_local_read = true;
			split->ext_file_uri = NULL;
			file->split_num = 1;
			file->splits = split;
			file->logic_len = logic_len;
			if ((locations != NULL) && (block_num > 0)) {
				Block_Host_Index * host_index = update_data_dist_stat(context,
						locations, block_num);

				double_check_hdfs_metadata_logic_length(locations, block_num, logic_len);

				file->block_num = block_num;
				file->locations = locations;
				file->hostIDs = host_index;
				// for hash, we need to add block number to total_split_count
				context->total_split_count += block_num;
			} else {
				file->block_num = 0;
				file->locations = NULL;
				file->hostIDs = NULL;
			}
			rel_data->files = lappend(rel_data->files, file);
		}

		total_size += logic_len;
		if (!context->keep_hash) {
			MemSet(segfile_path, 0, filepath_maxlen);
		}
	}

	systable_endscan(parquetscan);
	heap_close(pg_parquetseg_rel, AccessShareLock);

	pfree(segfile_path);

	rel_data->total_size = total_size;

	return;
}

static void ParquetGetSegFileDataLocationWrapper(
    Relation relation, AppendOnlyEntry *aoEntry, Snapshot metadataSnapshot,
    split_to_segment_mapping_context *context, int64 splitsize,
    Relation_Data *rel_data, int *hitblocks, int *allblocks,
    GpPolicy *targetPolicy) {
  // ParquetScan
  if (list_member_oid(context->srtc_context.parquetscan_range_tables,
                      relation->rd_id)) {
    ParquetGetSegFileDataLocation(relation, aoEntry->segrelid, metadataSnapshot,
                                  NULL, context, splitsize, rel_data, hitblocks,
                                  allblocks, targetPolicy);
  }
  // IndexScan
  if (list_member_oid(context->srtc_context.indexscan_range_tables,
                      relation->rd_id)) {
    List *idx_scan_ids =
        GetAllIdxScanIds(&(context->srtc_context), relation->rd_id);
    ParquetGetSegFileDataLocation(
        relation, aoEntry->blkdirrelid, metadataSnapshot, idx_scan_ids, context,
        splitsize, rel_data, hitblocks, allblocks, targetPolicy);
    list_free(idx_scan_ids);
  }
}

static void InvokeHDFSProtocolBlockLocation(Oid    procOid,
                                            List  *locs,
                                            List **blockLocations)
{
	ExtProtocolValidatorData   *validator_data;
	FmgrInfo				   *validator_udf;
	FunctionCallInfoData		fcinfo;

	validator_data = (ExtProtocolValidatorData *)
					 palloc0 (sizeof(ExtProtocolValidatorData));
	validator_udf = palloc(sizeof(FmgrInfo));
	fmgr_info(procOid, validator_udf);

	validator_data->type 		= T_ExtProtocolValidatorData;
	validator_data->url_list 	= locs;
	validator_data->format_opts = NULL;
	validator_data->errmsg		= NULL;
	validator_data->direction 	= EXT_VALIDATE_READ;
	validator_data->action		= EXT_VALID_ACT_GETBLKLOC;

	InitFunctionCallInfoData(/* FunctionCallInfoData */ fcinfo,
							 /* FmgrInfo */ validator_udf,
							 /* nArgs */ 0,
							 /* Call Context */ (Node *) validator_data,
							 /* ResultSetInfo */ NULL);

	/* invoke validator. if this function returns - validation passed */
	FunctionCallInvoke(&fcinfo);

	ExtProtocolBlockLocationData *bls =
		(ExtProtocolBlockLocationData *)(fcinfo.resultinfo);
	/* debug output block location. */
	if (bls != NULL)
	{
		ListCell *c;
		foreach(c, bls->files)
		{
			blocklocation_file *blf = (blocklocation_file *)(lfirst(c));
			elog(DEBUG3, "DEBUG LOCATION for %s with %d blocks",
					     blf->file_uri, blf->block_num);
			for ( int i = 0 ; i < blf->block_num ; ++i )
			{
				BlockLocation *pbl = &(blf->locations[i]);
				elog(DEBUG3, "DEBUG LOCATION for block %d : %d, "
						     INT64_FORMAT ", " INT64_FORMAT ", %d",
						     i,
						     pbl->corrupt, pbl->length, pbl->offset,
							 pbl->numOfNodes);
				for ( int j = 0 ; j < pbl->numOfNodes ; ++j )
				{
					elog(DEBUG3, "DEBUG LOCATION for block %d : %s, %s, %s",
							     i,
							     pbl->hosts[j], pbl->names[j],
								 pbl->topologyPaths[j]);
				}
			}
		}
	}

	elog(DEBUG3, "after invoking get block location API");

	/* get location data from fcinfo.resultinfo. */
	if (bls != NULL)
	{
		Assert(bls->type == T_ExtProtocolBlockLocationData);
		while(list_length(bls->files) > 0)
		{
			void *v = lfirst(list_head(bls->files));
			bls->files = list_delete_first(bls->files);
			*blockLocations = lappend(*blockLocations, v);
		}
	}
	pfree(validator_data);
	pfree(validator_udf);
}

static void InvokeHIVEProtocolBlockLocation(Oid    procOid,
                                            List  *locs,
                                            List **blockLocations)
{
  ExtProtocolValidatorData   *validator_data;
  FmgrInfo           *validator_udf;
  FunctionCallInfoData    fcinfo;

  validator_data = (ExtProtocolValidatorData *)
           palloc0 (sizeof(ExtProtocolValidatorData));
  validator_udf = palloc(sizeof(FmgrInfo));
  fmgr_info(procOid, validator_udf);

  validator_data->type    = T_ExtProtocolValidatorData;
  validator_data->url_list  = locs;
  validator_data->format_opts = NULL;
  validator_data->errmsg    = NULL;
  validator_data->direction   = EXT_VALIDATE_READ;
  validator_data->action    = EXT_VALID_ACT_GETBLKLOC;

  InitFunctionCallInfoData(/* FunctionCallInfoData */ fcinfo,
               /* FmgrInfo */ validator_udf,
               /* nArgs */ 0,
               /* Call Context */ (Node *) validator_data,
               /* ResultSetInfo */ NULL);

  /* invoke validator. if this function returns - validation passed */
  FunctionCallInvoke(&fcinfo);

  ExtProtocolBlockLocationData *bls =
    (ExtProtocolBlockLocationData *)(fcinfo.resultinfo);
  /* debug output block location. */
  if (bls != NULL)
  {
    ListCell *c;
    foreach(c, bls->files)
    {
      blocklocation_file *blf = (blocklocation_file *)(lfirst(c));
      elog(DEBUG3, "DEBUG LOCATION for %s with %d blocks",
               blf->file_uri, blf->block_num);
      for ( int i = 0 ; i < blf->block_num ; ++i )
      {
        BlockLocation *pbl = &(blf->locations[i]);
        elog(DEBUG3, "DEBUG LOCATION for block %d : %d, "
                 INT64_FORMAT ", " INT64_FORMAT ", %d",
                 i,
                 pbl->corrupt, pbl->length, pbl->offset,
               pbl->numOfNodes);
        for ( int j = 0 ; j < pbl->numOfNodes ; ++j )
        {
          elog(DEBUG3, "DEBUG LOCATION for block %d : %s, %s, %s",
                   i,
                   pbl->hosts[j], pbl->names[j],
                 pbl->topologyPaths[j]);
        }
      }
    }
  }

  elog(DEBUG3, "after invoking get block location API");

  /* get location data from fcinfo.resultinfo. */
  if (bls != NULL)
  {
    Assert(bls->type == T_ExtProtocolBlockLocationData);
    while(list_length(bls->files) > 0)
    {
      void *v = lfirst(list_head(bls->files));
      bls->files = list_delete_first(bls->files);
      *blockLocations = lappend(*blockLocations, v);
    }
  }
  pfree(validator_data);
  pfree(validator_udf);
}

void InvokeMagmaProtocolBlockLocation(ExtTableEntry *ext_entry,
                                             Oid procOid,
                                             char *dbname,
                                             char *schemaname,
                                             char *tablename,
                                             MagmaSnapshot *snapshot,
                                             bool useClientCacheDirectly,
                                             ExtProtocolBlockLocationData **bldata)
{
	ExtProtocolValidatorData   *validator_data;
	FmgrInfo				   *validator_udf;
	FunctionCallInfoData		fcinfo;

	validator_data = (ExtProtocolValidatorData *)
					 palloc0(sizeof(ExtProtocolValidatorData));
	validator_udf = palloc(sizeof(FmgrInfo));
	fmgr_info(procOid, validator_udf);

	validator_data->useClientCacheDirectly = useClientCacheDirectly;

	validator_data->type = T_ExtProtocolValidatorData;
	validator_data->dbname = dbname;
	validator_data->schemaname = schemaname;
	validator_data->tablename = tablename;

    validator_data->snapshot.currentTransaction.txnId = 0;
    validator_data->snapshot.currentTransaction.txnStatus = 0;
    validator_data->snapshot.txnActions.txnActionStartOffset = 0;
    validator_data->snapshot.txnActions.txnActions = NULL;
    validator_data->snapshot.txnActions.txnActionSize = 0;
    if (snapshot != NULL)
    {
        // save current transaction in snapshot
        validator_data->snapshot.currentTransaction.txnId =
                snapshot->currentTransaction.txnId;
        validator_data->snapshot.currentTransaction.txnStatus =
                snapshot->currentTransaction.txnStatus;

        validator_data->snapshot.cmdIdInTransaction = GetCurrentCommandId();

        // allocate txnActions
        validator_data->snapshot.txnActions.txnActionStartOffset =
            snapshot->txnActions.txnActionStartOffset;
        validator_data->snapshot.txnActions.txnActions =
            (MagmaTxnAction *)palloc0(sizeof(MagmaTxnAction) * snapshot->txnActions
                                      .txnActionSize);

        // save txnActionsp
        validator_data->snapshot.txnActions.txnActionSize = snapshot->txnActions
            .txnActionSize;
        for (int i = 0; i < snapshot->txnActions.txnActionSize; ++i)
        {
            validator_data->snapshot.txnActions.txnActions[i].txnId =
                snapshot->txnActions.txnActions[i].txnId;
            validator_data->snapshot.txnActions.txnActions[i].txnStatus =
                snapshot->txnActions.txnActions[i].txnStatus;
        }
    }

	validator_data->format_opts = lappend(validator_data->format_opts, makeString(ext_entry->fmtopts));
	validator_data->url_list = ext_entry->locations;

	InitFunctionCallInfoData(/* FunctionCallInfoData */ fcinfo,
							 /* FmgrInfo */ validator_udf,
							 /* nArgs */ 0,
							 /* Call Context */ (Node *) validator_data,
							 /* ResultSetInfo */ NULL);

	/* invoke validator. if this function returns - validation passed */
	FunctionCallInvoke(&fcinfo);

	*bldata = (ExtProtocolBlockLocationData *) (fcinfo.resultinfo);

	/* get location data from fcinfo.resultinfo. */
	if (*bldata != NULL)
	{
		Assert((*bldata)->type == T_ExtProtocolBlockLocationData);
	}

	// Free snapshot memory
	pfree(validator_data->snapshot.txnActions.txnActions);

	pfree(validator_data);
	pfree(validator_udf);
}

Oid
LookupCustomProtocolBlockLocationFunc(char *protoname)
{
	List*	funcname 	= NIL;
	Oid		procOid		= InvalidOid;
	Oid		argList[1];
	Oid		returnOid;

	char*   new_func_name = (char *)palloc0(strlen(protoname) + 16);
	sprintf(new_func_name, "%s_blocklocation", protoname);
	funcname = lappend(funcname, makeString(new_func_name));
	returnOid = VOIDOID;
	procOid = LookupFuncName(funcname, 0, argList, true);

	if (!OidIsValid(procOid))
		ereport(ERROR, (errcode(ERRCODE_UNDEFINED_FUNCTION),
						errmsg("protocol function %s was not found.",
								new_func_name),
						errhint("Create it with CREATE FUNCTION."),
						errOmitLocation(true)));

	/* check return type matches */
	if (get_func_rettype(procOid) != returnOid)
		ereport(ERROR, (errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
						errmsg("protocol function %s has an incorrect return type",
								new_func_name),
						errOmitLocation(true)));

	/* check allowed volatility */
	if (func_volatile(procOid) != PROVOLATILE_STABLE)
		ereport(ERROR, (errcode(ERRCODE_INVALID_FUNCTION_DEFINITION),
				 	 	errmsg("protocol function %s is not declared STABLE.",
						new_func_name),
						errOmitLocation(true)));
	pfree(new_func_name);

	return procOid;
}

void InvokeMagmaProtocolDatabaseSize(Oid procOid,
                                     const char *dbname,
                                     MagmaSnapshot *snapshot,
                                     ExtProtocolDatabaseSizeData **dbsdata)
{
  ExtProtocolValidatorData   *validator_data;
  FmgrInfo           *validator_udf;
  FunctionCallInfoData    fcinfo;

  validator_data = (ExtProtocolValidatorData *)
           palloc0(sizeof(ExtProtocolValidatorData));
  validator_udf = palloc(sizeof(FmgrInfo));
  fmgr_info(procOid, validator_udf);

  validator_data->type = T_ExtProtocolValidatorData;
  validator_data->dbname = dbname;
  validator_data->schemaname = NULL;
  validator_data->tablename = NULL;

    validator_data->snapshot.currentTransaction.txnId = 0;
    validator_data->snapshot.currentTransaction.txnStatus = 0;
    validator_data->snapshot.txnActions.txnActionStartOffset = 0;
    validator_data->snapshot.txnActions.txnActions = NULL;
    validator_data->snapshot.txnActions.txnActionSize = 0;
    if (snapshot != NULL)
    {
      // save current transaction in snapshot
      validator_data->snapshot.currentTransaction.txnId =
              snapshot->currentTransaction.txnId;
      validator_data->snapshot.currentTransaction.txnStatus =
              snapshot->currentTransaction.txnStatus;

      validator_data->snapshot.cmdIdInTransaction = GetCurrentCommandId();

      // allocate txnActions
      validator_data->snapshot.txnActions.txnActionStartOffset =
          snapshot->txnActions.txnActionStartOffset;
      validator_data->snapshot.txnActions.txnActions =
          (MagmaTxnAction *)palloc0(sizeof(MagmaTxnAction) * snapshot->txnActions
                                    .txnActionSize);

      // save txnActionsp
      validator_data->snapshot.txnActions.txnActionSize = snapshot->txnActions
          .txnActionSize;
      for (int i = 0; i < snapshot->txnActions.txnActionSize; ++i)
      {
          validator_data->snapshot.txnActions.txnActions[i].txnId =
              snapshot->txnActions.txnActions[i].txnId;
          validator_data->snapshot.txnActions.txnActions[i].txnStatus =
              snapshot->txnActions.txnActions[i].txnStatus;
      }
  }

  validator_data->format_opts = NIL;
  validator_data->url_list = NIL;

  InitFunctionCallInfoData(/* FunctionCallInfoData */ fcinfo,
               /* FmgrInfo */ validator_udf,
               /* nArgs */ 0,
               /* Call Context */ (Node *) validator_data,
               /* ResultSetInfo */ NULL);

  /* invoke validator. if this function returns - validation passed */
  FunctionCallInvoke(&fcinfo);

  *dbsdata = (ExtProtocolDatabaseSizeData *) (fcinfo.resultinfo);

  /* get location data from fcinfo.resultinfo. */
  if (*dbsdata != NULL)
  {
    Assert((*dbsdata)->type == T_ExtProtocolDatabaseSizeData);
  }

  // Free snapshot memory
  pfree(validator_data->snapshot.txnActions.txnActions);

  pfree(validator_data);
  pfree(validator_udf);
}

void InvokeMagmaProtocolTableSize(ExtTableEntry *ext_entry,
                                             Oid procOid,
                                             char *dbname,
                                             char *schemaname,
                                             char *tablename,
                                             MagmaSnapshot *snapshot,
                                             ExtProtocolTableSizeData **tsdata)
{
  ExtProtocolValidatorData   *validator_data;
  FmgrInfo           *validator_udf;
  FunctionCallInfoData    fcinfo;

  validator_data = (ExtProtocolValidatorData *)
           palloc0(sizeof(ExtProtocolValidatorData));
  validator_udf = palloc(sizeof(FmgrInfo));
  fmgr_info(procOid, validator_udf);

  validator_data->type = T_ExtProtocolValidatorData;
  validator_data->dbname = dbname;
  validator_data->schemaname = schemaname;
  validator_data->tablename = tablename;

    validator_data->snapshot.currentTransaction.txnId = 0;
    validator_data->snapshot.currentTransaction.txnStatus = 0;
    validator_data->snapshot.txnActions.txnActionStartOffset = 0;
    validator_data->snapshot.txnActions.txnActions = NULL;
    validator_data->snapshot.txnActions.txnActionSize = 0;
    if (snapshot != NULL)
    {
      // save current transaction in snapshot
      validator_data->snapshot.currentTransaction.txnId =
              snapshot->currentTransaction.txnId;
      validator_data->snapshot.currentTransaction.txnStatus =
              snapshot->currentTransaction.txnStatus;
      validator_data->snapshot.cmdIdInTransaction = GetCurrentCommandId();

      // allocate txnActions
      validator_data->snapshot.txnActions.txnActionStartOffset =
          snapshot->txnActions.txnActionStartOffset;
      validator_data->snapshot.txnActions.txnActions =
          (MagmaTxnAction *)palloc0(sizeof(MagmaTxnAction) * snapshot->txnActions
                                    .txnActionSize);

      // save txnActionsp
      validator_data->snapshot.txnActions.txnActionSize = snapshot->txnActions
          .txnActionSize;
      for (int i = 0; i < snapshot->txnActions.txnActionSize; ++i)
      {
          validator_data->snapshot.txnActions.txnActions[i].txnId =
              snapshot->txnActions.txnActions[i].txnId;
          validator_data->snapshot.txnActions.txnActions[i].txnStatus =
              snapshot->txnActions.txnActions[i].txnStatus;
      }
  }

  validator_data->format_opts = lappend(validator_data->format_opts, makeString(ext_entry->fmtopts));
  validator_data->url_list = ext_entry->locations;;

  InitFunctionCallInfoData(/* FunctionCallInfoData */ fcinfo,
               /* FmgrInfo */ validator_udf,
               /* nArgs */ 0,
               /* Call Context */ (Node *) validator_data,
               /* ResultSetInfo */ NULL);

  /* invoke validator. if this function returns - validation passed */
  FunctionCallInvoke(&fcinfo);

  *tsdata = (ExtProtocolDatabaseSizeData *) (fcinfo.resultinfo);

  /* get location data from fcinfo.resultinfo. */
  if (*tsdata != NULL)
  {
    Assert((*tsdata)->type == T_ExtProtocolTableSizeData);
  }

  // Free snapshot memory
  pfree(validator_data->snapshot.txnActions.txnActions);

  pfree(validator_data);
  pfree(validator_udf);
}

Oid
LookupCustomProtocolTableSizeFunc(char *protoname)
{
  List* funcname  = NIL;
  Oid   procOid   = InvalidOid;
  Oid   argList[1];
  Oid   returnOid;

  char*   new_func_name = (char *)palloc0(strlen(protoname) + 13);
  sprintf(new_func_name, "%s_tablesize", protoname);
  funcname = lappend(funcname, makeString(new_func_name));
  returnOid = VOIDOID;
  procOid = LookupFuncName(funcname, 0, argList, true);

  if (!OidIsValid(procOid))
    ereport(ERROR, (errcode(ERRCODE_UNDEFINED_FUNCTION),
            errmsg("protocol function %s was not found.",
                new_func_name),
            errhint("Create it with CREATE FUNCTION."),
            errOmitLocation(true)));

  /* check return type matches */
  if (get_func_rettype(procOid) != returnOid)
    ereport(ERROR, (errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
            errmsg("protocol function %s has an incorrect return type",
                new_func_name),
            errOmitLocation(true)));

  /* check allowed volatility */
  if (func_volatile(procOid) != PROVOLATILE_STABLE)
    ereport(ERROR, (errcode(ERRCODE_INVALID_FUNCTION_DEFINITION),
            errmsg("protocol function %s is not declared STABLE.",
            new_func_name),
            errOmitLocation(true)));
  pfree(new_func_name);

  return procOid;
}

Oid
LookupCustomProtocolDatabaseSizeFunc(char *protoname)
{
  List* funcname  = NIL;
  Oid   procOid   = InvalidOid;
  Oid   argList[1];
  Oid   returnOid;

  char*   new_func_name = (char *)palloc0(strlen(protoname) + 16);
  sprintf(new_func_name, "%s_databasesize", protoname);
  funcname = lappend(funcname, makeString(new_func_name));
  returnOid = VOIDOID;
  procOid = LookupFuncName(funcname, 0, argList, true);

  if (!OidIsValid(procOid))
    ereport(ERROR, (errcode(ERRCODE_UNDEFINED_FUNCTION),
            errmsg("protocol function %s was not found.",
                new_func_name),
            errhint("Create it with CREATE FUNCTION."),
            errOmitLocation(true)));

  /* check return type matches */
  if (get_func_rettype(procOid) != returnOid)
    ereport(ERROR, (errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
            errmsg("protocol function %s has an incorrect return type",
                new_func_name),
            errOmitLocation(true)));

  /* check allowed volatility */
  if (func_volatile(procOid) != PROVOLATILE_STABLE)
    ereport(ERROR, (errcode(ERRCODE_INVALID_FUNCTION_DEFINITION),
            errmsg("protocol function %s is not declared STABLE.",
            new_func_name),
            errOmitLocation(true)));
  pfree(new_func_name);

  return procOid;
}

static void ExternalGetHdfsFileDataLocation(
				Relation relation,
				split_to_segment_mapping_context *context,
				int64 splitsize,
				Relation_Data *rel_data,
				int* allblocks,GpPolicy *targetPolicy) {
	rel_data->serializeSchema = NULL;
	rel_data->serializeSchemaLen = 0;
	Oid	procOid = LookupCustomProtocolBlockLocationFunc("hdfs");
	Assert(OidIsValid(procOid));

	List *bls = NULL; /* Block locations */
	ExtTableEntry *ext_entry = GetExtTableEntry(rel_data->relid);
  InvokeHDFSProtocolBlockLocation(procOid, ext_entry->locations, &bls);

	bool isRelationHash = is_relation_hash(targetPolicy);

	/* Go through each files */
	ListCell *cbl = NULL;
	int *hashRelationSegnoIndex = NULL;
	int64 total_size = 0;
	int segno = 1;
	foreach(cbl, bls)
	{
		blocklocation_file *f = (blocklocation_file *)lfirst(cbl);
		BlockLocation *locations = f->locations;
		int block_num = f->block_num;
		int64 logic_len = 0;
		*allblocks += block_num;
		if ((locations != NULL) && (block_num > 0)) {
			// calculate length for one specific file
			for (int j = 0; j < block_num; ++j) {
				logic_len += locations[j].length;
			}
			total_size += logic_len;

			Block_Host_Index * host_index = update_data_dist_stat(context,
					locations, block_num);

			Relation_File *file = (Relation_File *) palloc(sizeof(Relation_File));
      if (!context->keep_hash || !isRelationHash) {
        file->segno = segno++;
        file->block_num = block_num;
        file->locations = locations;
        file->hostIDs = host_index;
        file->logic_len = logic_len;

        // do the split logic
        int realSplitNum = 0;
        int split_num = file->block_num;
        int64 offset = 0;
        File_Split *splits = (File_Split *)palloc(
            sizeof(File_Split) * split_num);
        while (realSplitNum < split_num) {
          splits[realSplitNum].host = -1;
          splits[realSplitNum].range_id =
              file->locations[realSplitNum].rangeId;
          splits[realSplitNum].replicaGroup_id =
              file->locations[realSplitNum].replicaGroupId;
          splits[realSplitNum].is_local_read = true;
          splits[realSplitNum].offset = offset;
          splits[realSplitNum].length =
              file->locations[realSplitNum].length;
          splits[realSplitNum].logiceof = logic_len;
          splits[realSplitNum].ext_file_uri =
              pstrdup(f->file_uri);

          if (logic_len - offset <=
              splits[realSplitNum].length) {
            splits[realSplitNum].length = logic_len - offset;
            ++realSplitNum;
            break;
          }
          offset += splits[realSplitNum].length;
          ++realSplitNum;
        }
        file->split_num = realSplitNum;
        file->splits = splits;
        context->total_split_count += realSplitNum;
      } else {
        // if (hashRelationSegnoIndex == NULL)
        //  hashRelationSegnoIndex = palloc0(sizeof(int) * list_length(bls));
        File_Split *split = (File_Split *)palloc(sizeof(File_Split));
        int idx = atoi(strrchr(f->file_uri, '/') + 1);
        // file->segno =
        //    hashRelationSegnoIndex[idx - 1] * targetPolicy->bucketnum + idx;
        // hashRelationSegnoIndex[idx - 1] += 1;
        file->segno = idx;
        split->offset = 0;
        split->range_id = -1;
        split->replicaGroup_id = -1;
        split->length = logic_len;
        split->logiceof = logic_len;
        split->host = -1;
        split->is_local_read = true;
        split->ext_file_uri = pstrdup(f->file_uri);
        file->split_num = 1;
        file->splits = split;
        file->logic_len = logic_len;
        file->block_num = block_num;
        file->locations = locations;
        file->hostIDs = host_index;
        context->total_split_count += block_num;
      }

      rel_data->files = lappend(rel_data->files, file);
		}
	}
	context->total_metadata_logic_len += total_size;
	rel_data->total_size = total_size;
}

static void ExternalGetHiveFileDataLocation(
        Relation relation,
        split_to_segment_mapping_context *context,
        int64 splitsize,
        Relation_Data *rel_data,
        int* allblocks) {
  ExtTableEntry *ext_entry = GetExtTableEntry(rel_data->relid);
  rel_data->serializeSchema = NULL;
  rel_data->serializeSchemaLen = 0;
  Assert(ext_entry->locations != NIL);
  int64 total_size = 0;
  int segno = 1;

  /*
   * Step 1. get external HIVE location from URI.
   */
  char* first_uri_str = (char *) strVal(lfirst(list_head(ext_entry->locations)));
  /* We must have at least one location. */
  Assert(first_uri_str != NULL);
  Uri* uri = ParseExternalTableUri(first_uri_str);
  bool isHive = false;
  if (uri != NULL && is_hive_protocol(uri)) {
    isHive = true;
  }
  Assert(isHive);  /* Currently, we accept HIVE only. */

  /*
   * Step 2. Get function to call for getting location information. This work
   * is done by validator function registered for this external protocol.
   */
  Oid procOid = InvalidOid;
  procOid = LookupCustomProtocolBlockLocationFunc("hive");

  /*
   * Step 3. Call validator to get location data.
   */

  /* Prepare function call parameter by passing into location string. This is
   * only called at dispatcher side. */
  List *bls = NULL; /* Block locations */
  if (OidIsValid(procOid) && Gp_role == GP_ROLE_DISPATCH) {
    List* locations = NIL;
    locations = lappend(locations, makeString(pstrdup(context->hiveUrl)));
    InvokeHIVEProtocolBlockLocation(procOid, locations, &bls);
  }

  /*
   * Step 4. Build data location info for optimization after this call.
   */

  /* Go through each files */
  ListCell *cbl = NULL;
  foreach (cbl, bls) {
    blocklocation_file *f = (blocklocation_file *)lfirst(cbl);
    BlockLocation *locations = f->locations;
    int block_num = f->block_num;
    int64 logic_len = 0;
    *allblocks += block_num;
    if ((locations != NULL) && (block_num > 0)) {
      // calculate length for one specific file
      for (int j = 0; j < block_num; ++j) {
        logic_len += locations[j].length;
      }
      total_size += logic_len;

      Block_Host_Index *host_index =
          update_data_dist_stat(context, locations, block_num);

      Relation_File *file = (Relation_File *)palloc(sizeof(Relation_File));
      file->segno = segno++;
      file->block_num = block_num;
      file->locations = locations;
      file->hostIDs = host_index;
      file->logic_len = logic_len;

      // do the split logic
      int realSplitNum = 0;
      int split_num = file->block_num;
      int64 offset = 0;
      File_Split *splits = (File_Split *)palloc(sizeof(File_Split) * split_num);
      while (realSplitNum < split_num) {
        splits[realSplitNum].host = -1;
        splits[realSplitNum].is_local_read = true;
        splits[realSplitNum].offset = offset;
        splits[realSplitNum].length = file->locations[realSplitNum].length;
        splits[realSplitNum].logiceof = logic_len;
        splits[realSplitNum].ext_file_uri = pstrdup(f->file_uri);

        if (logic_len - offset <= splits[realSplitNum].length) {
          splits[realSplitNum].length = logic_len - offset;
          ++realSplitNum;
          break;
        }
        offset += splits[realSplitNum].length;
        ++realSplitNum;
      }
      file->split_num = realSplitNum;
      file->splits = splits;
      context->total_split_count += realSplitNum;

      rel_data->files = lappend(rel_data->files, file);
    }
  }
  context->total_metadata_logic_len += total_size;
  rel_data->total_size = total_size;
}

static void ExternalGetMagmaRangeDataLocation(
                const char* dbname,
		const char* schemaname,
		const char* tablename,
		split_to_segment_mapping_context *context,
		int64 rangesize,
		Relation_Data *rel_data,
		bool useClientCacheDirectly,
		int *allranges)
{
	/*
	 * Step 1. make sure URI is magma
	 */
	ExtTableEntry *ext_entry = GetExtTableEntry(rel_data->relid);
	Assert(ext_entry->locations != NIL);

	char* first_uri_str = (char *) strVal(lfirst(list_head(ext_entry->locations)));
	Assert(first_uri_str != NULL);

	Uri* uri = ParseExternalTableUri(first_uri_str);
	Assert(uri && is_magma_protocol(uri));

	int64 total_size = 0;
	int segno = 1;

    /*
     * Step 2. Get function to call for location information. This is done by
     * block location function registered for magma protocol.
     */
	Oid procOid = InvalidOid;
	procOid = LookupCustomProtocolBlockLocationFunc("magma");

	/*
	 * Step 3. Call block location function to get location data. This is
	 * only called at dispatcher side.
	 */

	ExtProtocolBlockLocationData *bldata = NULL;
	if (Gp_role == GP_ROLE_DISPATCH)
	{
		if (OidIsValid(procOid))
		{
			// get range location from magma now
			Assert(PlugStorageGetTransactionStatus() == PS_TXN_STS_STARTED);
			InvokeMagmaProtocolBlockLocation(ext_entry, procOid, dbname, schemaname,
			                                 tablename, PlugStorageGetTransactionSnapshot(NULL),
			                                 useClientCacheDirectly,
			                                 &bldata);
		}
		else
		{
			elog(ERROR, "failed to find data locality function for magma");
		}
	}

	/*
	 * Step 4. build file splits
	 */
	rel_data->serializeSchema = pnstrdup(bldata->serializeSchema,
			bldata->serializeSchemaLen);
	rel_data->serializeSchemaLen = bldata->serializeSchemaLen;
	context->magmaRangeNum = bldata->files->length;
	ListCell *cbl = NULL;
	foreach(cbl, bldata->files)
	{
		blocklocation_file *f = (blocklocation_file *)lfirst(cbl);
		BlockLocation *locations = f->locations;
		int block_num = f->block_num;
		int64 logic_len = 0;
		*allranges += block_num;
		if ((locations != NULL) && (block_num > 0)) {
			// calculate length for one specific file
			for (int j = 0; j < block_num; ++j) {
				logic_len += locations[j].length;
			}
			total_size += logic_len;

			Block_Host_Index * host_index = update_data_dist_stat(context,
					locations, block_num);

			Relation_File *file = (Relation_File *) palloc(sizeof(Relation_File));
			file->segno = segno++;
			file->block_num = block_num;
			file->locations = locations;
			file->hostIDs = host_index;
			file->logic_len = logic_len;

			// do the split logic
			int realSplitNum = 0;
			int split_num = file->block_num;
			int64 offset = 0;
			char *p = NULL;
			File_Split *splits = (File_Split *) palloc(sizeof(File_Split) * split_num);
			while (realSplitNum < split_num) {
					splits[realSplitNum].host = -1;
					splits[realSplitNum].is_local_read = true;
					splits[realSplitNum].range_id = file->locations[realSplitNum].rangeId;
					splits[realSplitNum].replicaGroup_id = file->locations[realSplitNum].replicaGroupId;
					splits[realSplitNum].offset = offset;
					splits[realSplitNum].length = file->locations[realSplitNum].length;
					splits[realSplitNum].logiceof = logic_len;
					p = file->locations[realSplitNum].topologyPaths[0];
					splits[realSplitNum].ext_file_uri = p ? pstrdup(p) : (char *) NULL;

					if (logic_len - offset <= splits[realSplitNum].length) {
							splits[realSplitNum].length = logic_len - offset;
							++realSplitNum;
							break;
					}
					offset += splits[realSplitNum].length;
					++realSplitNum;
			}
			file->split_num = realSplitNum;
			file->splits = splits;
			context->total_split_count += realSplitNum;

			rel_data->files = lappend(rel_data->files, file);
		}
	}

	/*
	 * Step 5. calculate total size
	 */
	context->total_metadata_logic_len += total_size;
	rel_data->total_size = total_size;
}

/*
 * step 1 search segments with local read and segment is not full after being assigned the block
 * step 2 search segments with local read and segment is not full before being assigned the block
 * step 3 search segments which is not full after being assigned the block with net_disk_ratio penalty
 * step 4 all the segments are full, then select the segment with smallest size after being assigned the block
 * step 5 avoid assigning all of full split to vseg 0
 * note: assign a non-local blocks to segment will increase size of segment with block size * net_disk_ratio
 */
static int select_random_host_algorithm(Relation_Assignment_Context *context,
		int64 splitsize, int64 maxExtendedSizePerSegment, TargetSegmentIDMap *idMap,
		Block_Host_Index **hostID, int fileindex, Oid partition_parent_oid, bool* isLocality) {

	*isLocality = false;
	bool isExceedVolume = false;
	bool isExceedWholeSize =false;
	bool isExceedPartitionTableSize =false;
	//step1
	int64 minvols = INT64_MAX;
	int minindex = 0;

	ListCell* lc;
	for (int l = 0; l < (*hostID)->replica_num; l++) {
		uint32_t key = (*hostID)->hostIndex[l];
		PAIR pair = getHASHTABLENode(context->vseg_to_splits_map,
				TYPCONVERT(void *, key));
		if (pair == NULL) {
			continue;
		}
		List* val = (List*) (pair->Value);
		foreach(lc, val)
		{
			int j = lfirst_int(lc);
			isExceedVolume = splitsize + context->vols[j] > maxExtendedSizePerSegment;
			isExceedWholeSize = balance_on_whole_query_level
					&& splitsize + context->totalvols_with_penalty[j]
							> context->avg_size_of_whole_query;
			int64** partitionvols_with_penalty=NULL;
			if(partition_parent_oid > 0){
				PAIR p = getHASHTABLENode(context->partitionvols_with_penalty_map,
						TYPCONVERT(void *, partition_parent_oid));
				partitionvols_with_penalty = (int64 **) (&(p->Value));
				isExceedPartitionTableSize = balance_on_partition_table_level && splitsize
						+ (*partitionvols_with_penalty)[j]
						> context->avg_size_of_whole_partition_table;
			}
			else{
				isExceedPartitionTableSize= false;
			}
			if (context->block_lessthan_vseg_round_robin_no >= 0 && context->vols[j] > 0) {
				continue;
			}
			if ((!isExceedWholeSize || context->totalvols_with_penalty[j] == 0)
					&& (!isExceedVolume || context->vols[j] == 0)
					&& (!isExceedPartitionTableSize || (*partitionvols_with_penalty)[j] ==0)) {
				{
					*isLocality = true;
					if (minvols > context->vols[j]) {
						minvols = context->vols[j];
						minindex = j;
					} else if(minvols == context->vols[j]){
						/*prefer insert host if exists*/
						int inserthost = (*hostID)->insertHost;
						if(inserthost == idMap->global_IDs[j]){
							minindex = j;
						}
						else if (context->block_lessthan_vseg_round_robin_no >= 0) {
							if (l == context->block_lessthan_vseg_round_robin_no) {
								minindex = j;
								context->block_lessthan_vseg_round_robin_no = (context->block_lessthan_vseg_round_robin_no+1)%(*hostID)->replica_num;
							}
						}
					}
				}
			}
		}
	}

	if (*isLocality) {
		return minindex;
	}

	//step2
	minvols = INT64_MAX;
	bool isFound = false;
	for (int j = 0; j < context->virtual_segment_num; j++) {
		isExceedVolume = net_disk_ratio * splitsize + context->vols[j]
				> maxExtendedSizePerSegment;
		isExceedWholeSize = balance_on_whole_query_level
				&& net_disk_ratio * splitsize + context->totalvols_with_penalty[j]
						> context->avg_size_of_whole_query;
		int64** partitionvols_with_penalty=NULL;
		if(partition_parent_oid > 0){
			PAIR p = getHASHTABLENode(context->partitionvols_with_penalty_map,
					TYPCONVERT(void *, partition_parent_oid));
			partitionvols_with_penalty = (int64 **) (&(p->Value));
			isExceedPartitionTableSize = balance_on_partition_table_level &&
					splitsize + (*partitionvols_with_penalty)[j]
					> context->avg_size_of_whole_partition_table;
		} else {
			isExceedPartitionTableSize = false;
		}
		if ((!isExceedWholeSize || context->totalvols_with_penalty[j] == 0)
				&& (!isExceedVolume || context->vols[j] == 0)
				&& (!isExceedPartitionTableSize || (*partitionvols_with_penalty)[j] ==0)) {
			isFound = true;
			if (minvols > context->vols[j]) {
				minvols = context->vols[j];
				minindex = j;
			} else if (minvols == context->vols[j]
					&& context->totalvols_with_penalty[j]
							< context->totalvols_with_penalty[minindex]*0.9) {
				minindex = j;
			}
		}
	}

	if (isFound) {
		return minindex;
	}

	//step3
	minvols = INT64_MAX;
	minindex = 0;
	isFound = false;
	for (int j = 0; j < context->virtual_segment_num; j++) {

		bool isLocalSegment = false;
		for (int l = 0; l < (*hostID)->replica_num; l++) {
			if ((*hostID)->hostIndex[l] == idMap->global_IDs[j]) {
				isLocalSegment = true;
				if (balance_on_whole_query_level
						&& context->totalvols_with_penalty[j] + splitsize
								> context->avg_size_of_whole_query && context->totalvols_with_penalty[j] > 0) {
					continue;
				}
				if (partition_parent_oid > 0) {
					PAIR p = getHASHTABLENode(context->partitionvols_with_penalty_map,
							TYPCONVERT(void *, partition_parent_oid));
					int64** partitionvols_with_penalty = (int64 **) (&(p->Value));
					if (balance_on_partition_table_level
							&& splitsize + (*partitionvols_with_penalty)[j]
									> context->avg_size_of_whole_partition_table
							&& (*partitionvols_with_penalty)[j] != 0) {
						continue;
					}
				}
				if (context->vols[j] + splitsize < minvols) {
					minvols = context->vols[j] + splitsize;
					minindex = j;
					*isLocality = true;
					isFound = true;
				}
				else if (context->vols[j] + splitsize == minvols
						&& context->totalvols_with_penalty[j]
								< context->totalvols_with_penalty[minindex] * 0.9) {
					minindex = j;
					*isLocality = true;
					isFound = true;
				}
			}
		}
		if (!isLocalSegment) {
			if (balance_on_whole_query_level
					&& context->totalvols_with_penalty[j] + net_disk_ratio * splitsize
							> context->avg_size_of_whole_query
					&& context->totalvols_with_penalty[j] > 0) {
				continue;
			}
			if (partition_parent_oid > 0) {
				PAIR p = getHASHTABLENode(context->partitionvols_with_penalty_map,
						TYPCONVERT(void *, partition_parent_oid));
				int64** partitionvols_with_penalty = (int64 **) (&(p->Value));
				if (balance_on_partition_table_level
						&& net_disk_ratio * splitsize + (*partitionvols_with_penalty)[j]
								> context->avg_size_of_whole_partition_table
						&& (*partitionvols_with_penalty)[j] != 0) {
					continue;
				}
			}
			if (context->vols[j] + net_disk_ratio * splitsize < minvols) {
				minvols = context->vols[j] + net_disk_ratio * splitsize;
				minindex = j;
				*isLocality = false;
				isFound = true;
			} else if (context->vols[j] + net_disk_ratio * splitsize == minvols
					&& context->totalvols_with_penalty[j]
							< context->totalvols_with_penalty[minindex]*0.9) {
				minindex = j;
				*isLocality = false;
				isFound = true;
			}
		}
	}
	if(isFound){
		return minindex;
	}
	//step4
	minvols = INT64_MAX;
	minindex = 0;
	for (int j = 0; j < context->virtual_segment_num; j++) {

		bool isLocalSegment = false;
		for (int l = 0; l < (*hostID)->replica_num; l++) {
			if ((*hostID)->hostIndex[l] == idMap->global_IDs[j]) {
				isLocalSegment = true;
				if (context->vols[j] + splitsize < minvols) {
					minvols = context->vols[j] + splitsize;
					minindex = j;
					*isLocality = true;
				} else if (context->vols[j] + splitsize == minvols
						&& context->totalvols_with_penalty[j]
								< context->totalvols_with_penalty[minindex] *0.9) {
					minindex = j;
					*isLocality = true;
				}
			}
		}
		if (!isLocalSegment) {
			if (context->vols[j] + net_disk_ratio * splitsize < minvols) {
				minvols = context->vols[j] + net_disk_ratio * splitsize;
				minindex = j;
				*isLocality = false;
			} else if (context->vols[j] + net_disk_ratio * splitsize == minvols
					&& context->totalvols_with_penalty[j]
							< context->totalvols_with_penalty[minindex] *0.9) {
				minindex = j;
				*isLocality = false;
			}
		}
	}
	if (debug_fake_datalocality) {
		fprintf(fp,
				"cur_size_of_whole_query is:%.0f, avg_size_of_whole_query is: %.3f",
				context->totalvols_with_penalty[minindex] + net_disk_ratio * splitsize,
				context->avg_size_of_whole_query);
	}
	return minindex;
}

static void assign_split_to_host(Host_Assignment_Result *result,
		Detailed_File_Split *split) {
	Detailed_File_Split *last_split;

	if (split->length == 0) {
		return;
	}

	/*
	 * for the first split.
	 */
	if (result->count == 0) {
		result->splits[result->count++] = *split;
		return;
	}

	/*
	 * we try to merge the new split into
	 * the old split.
	 */
	last_split = result->splits + result->count - 1;
	if ((last_split->rel_oid == split->rel_oid)
			&& (last_split->segno == split->segno)
			&& (last_split->offset + last_split->length == split->offset)
			&& (split->range_id == -1)) {
		last_split->length += split->length;
		return;
	}

	/*
	 * cannot merge into the last split.
	 */
	if (result->count < result->max_size) {
		result->splits[result->count++] = *split;
		return;
	}

	/*
	 * not enough space.
	 */
	result->max_size <<= 1;
	result->splits = (Detailed_File_Split *) repalloc(result->splits,
			sizeof(Detailed_File_Split) * (result->max_size));
	result->splits[result->count++] = *split;

	return;
}

static void assign_splits_to_hosts(Split_Assignment_Result *result,
		Detailed_File_Split *splits, int split_num) {
	int i;

	for (i = 0; i < split_num; i++) {
		Detailed_File_Split *split = splits + i;
		assign_split_to_host(result->host_assigns + split->host, split);
	}

	return;
}

static List *
search_map_node(List *result, Oid rel_oid, int host_num,
		SegFileSplitMapNode **found_map_node) {
	SegFileSplitMapNode *map_node = NULL;
	ListCell *lc;
	int i;
	foreach(lc, result)
	{
		SegFileSplitMapNode *split_map_node = (SegFileSplitMapNode *) lfirst(lc);
		if (split_map_node->relid == rel_oid) {
			*found_map_node = split_map_node;
			return result;
		}
	}

	map_node = makeNode(SegFileSplitMapNode);
	map_node->relid = rel_oid;
	map_node->splits = NIL;
	for (i = 0; i < host_num; i++) {
		map_node->splits = lappend(map_node->splits, NIL);
	}

	*found_map_node = map_node;
	result = lappend(result, map_node);

	return result;
}

static List *
post_process_assign_result(Split_Assignment_Result *assign_result) {
	List *final_result = NIL;
	int i;

	for (i = 0; i < assign_result->host_num; i++) {
		Host_Assignment_Result *har = assign_result->host_assigns + i;
		int j = 0;
		Oid last_oid = InvalidOid;
		SegFileSplitMapNode *last_mapnode = NULL;

		while (j < har->count) {
			Detailed_File_Split *split = har->splits + j;
			ListCell *per_seg_splits;
			bool empty_seg = false;
			char *p = NULL;
			FileSplit fileSplit = makeNode(FileSplitNode);
			if (split->rel_oid != last_oid) {
				last_oid = split->rel_oid;
				final_result = search_map_node(final_result, last_oid,
						assign_result->host_num, &last_mapnode);
			}

			fileSplit->segno = (split->segno - split->index) + 1;
			fileSplit->logiceof = split->logiceof;

			if (split->length <= 0) {
				empty_seg = true;
			}

			fileSplit->range_id = split->range_id;
			fileSplit->replicaGroup_id = split->replicaGroup_id;
			fileSplit->offsets = split->offset;
			fileSplit->lengths = split->length;
			p = split->ext_file_uri_string;
			fileSplit->ext_file_uri_string = p ? pstrdup(p) : (char *) NULL;

			j += 1;

			if (empty_seg) {
				if (fileSplit->ext_file_uri_string) {
					pfree(fileSplit->ext_file_uri_string);
				}
				pfree(fileSplit);
			} else {
				per_seg_splits = list_nth_cell((List *) (last_mapnode->splits), i);
				lfirst(per_seg_splits) = lappend((List *) lfirst(per_seg_splits),
						fileSplit);
			}
		}
	}

	return final_result;
}

/*
 * compare two relation
 */
static int compare_relation_size(const void *e1, const void *e2) {
	Relation_Data **s1 = (Relation_Data **) e1;
	Relation_Data **s2 = (Relation_Data **) e2;

	/*
	 * host id first.
	 */
	if ((*s1)->total_size < (*s2)->total_size) {
		return 1;
	}

	if ((*s1)->total_size > (*s2)->total_size) {
		return -1;
	}

	return 0;
}

/*
 * compare two file based on continuity
 */
static int compare_file_continuity(const void *e1, const void *e2) {
	Relation_File **s1 = (Relation_File **) e1;
	Relation_File **s2 = (Relation_File **) e2;

	/*
	 * host id first.
	 */
	if ((*s1)->continue_ratio < (*s2)->continue_ratio) {
		return 1;
	}

	if ((*s1)->continue_ratio > (*s2)->continue_ratio) {
		return -1;
	}

	return 0;
}

/*
 * compare two file based on segno.
 */
static int compare_file_segno(const void *e1, const void *e2) {
	Relation_File **s1 = (Relation_File **) e1;
	Relation_File **s2 = (Relation_File **) e2;

	/*
	 * host id first.
	 */
	if ((*s1)->segno < (*s2)->segno) {
		return -1;
	}

	if ((*s1)->segno > (*s2)->segno) {
		return 1;
	}

	return 0;
}

/*
 * compare two container-segment Pair.
 */
static int compare_container_segment(const void *e1, const void *e2) {
	segmentFilenoPair *s1 = (segmentFilenoPair *) e1;
	segmentFilenoPair *s2 = (segmentFilenoPair *) e2;

	/*
	 * host id first.
	 */
	if (s1->fileno < s2->fileno) {
		return -1;
	}

	if (s1->fileno > s2->fileno) {
		return 1;
	}

	return 0;
}

/*
 * compare two detailed file splits.
 */
static int compare_detailed_file_split(const void *e1, const void *e2) {
	Detailed_File_Split *s1 = (Detailed_File_Split *) e1;
	Detailed_File_Split *s2 = (Detailed_File_Split *) e2;

	/*
	 * host id first.
	 */
	if (s1->host < s2->host) {
		return -1;
	}

	if (s1->host > s2->host) {
		return 1;
	}

	/*
	 * relation id second.
	 */
	if (s1->rel_oid < s2->rel_oid) {
		return -1;
	}

	if (s1->rel_oid > s2->rel_oid) {
		return 1;
	}

	/*
	 * file id third.
	 */
	if (s1->segno < s2->segno) {
		return -1;
	}

	if (s1->segno > s2->segno) {
		return 1;
	}

	/*
	 * offset forth.
	 */
	if (s1->offset < s2->offset) {
		return -1;
	}

	if (s1->offset > s2->offset) {
		return 1;
	}

	return 0;
}

/*
 * compare two container-segment Pair.
 */
static int
compare_hostid(const void *e1, const void *e2)
{
	Host_Index* s1 = (Host_Index*) e1;
	Host_Index* s2 = (Host_Index*) e2;

	return strcmp((s1)->hostname,(s2)->hostname);
}
/*
 *
 */
static void change_hash_virtual_segments_order(QueryResource ** resourcePtr,
		Relation_Data *rel_data,
		Relation_Assignment_Context *assignment_context_ptr,
		TargetSegmentIDMap* idMap_ptr) {

	// first we check if datalocality is one without changing vseg order
	ListCell *lc_file;
	bool datalocalityEqualsOne = true;
	foreach(lc_file, rel_data->files)
	{
		Relation_File *rel_file = (Relation_File *) lfirst(lc_file);
		for (int i = 0; i < rel_file->split_num; i++) {
			int targethost = (rel_file->segno - 1)
					% ((*assignment_context_ptr).virtual_segment_num);
			for (int p = 0; p < rel_file->block_num; p++) {
				bool islocal = false;
				Block_Host_Index *hostID = rel_file->hostIDs + p;
				for (int l = 0; l < hostID->replica_num; l++) {
					if (hostID->hostIndex[l] == (*idMap_ptr).global_IDs[targethost]) {
						islocal = true;
						break;
					}
				}
				if (!islocal) {
					datalocalityEqualsOne = false;
				}
			}
		}
	}
	if(datalocalityEqualsOne){
		if (debug_print_split_alloc_result) {
			elog(LOG, "didn't change virtual segments order");
		}
		return;
	}

	if (debug_print_split_alloc_result) {
		elog(LOG, "change virtual segments order");
	}
	TargetSegmentIDMap idMap = *idMap_ptr;
	Relation_Assignment_Context assignment_context = *assignment_context_ptr;
	//ListCell *lc_file;
	int fileCount = list_length(rel_data->files);

	// empty table may have zero filecount
	if (fileCount > 0) {
		int *vs2fileno = (int *) palloc(
				sizeof(int) * assignment_context.virtual_segment_num);
		for (int i = 0; i < assignment_context.virtual_segment_num; i++) {
			vs2fileno[i] = -1;
		}
		Relation_File** rel_file_vector = (Relation_File**) palloc(
				sizeof(Relation_File*) * fileCount);
		int i = 0;
		foreach(lc_file, rel_data->files)
		{
			rel_file_vector[i++] = (Relation_File *) lfirst(lc_file);
		}
		qsort(rel_file_vector, fileCount, sizeof(Relation_File*),
				compare_file_segno);
		// fileCount = assignment_context.virtual_segment_num * const value
		// when there is parallel insert, const value can be greater than 1
		int filesPerSegment = fileCount / assignment_context.virtual_segment_num;
		int* numOfLocalRead = (int *) palloc(sizeof(int) * assignment_context.virtual_segment_num);
		// assign files to the virtual segments with max local read. one file group to one virtual segment
		// file group is files with the same module value to virtual_segment_num
		ListCell* lc;

		// when it is magma table, the file count may be not mutiple of virtual_segment_num,
		// need to skip the assignment of file which not exist.
		for (i = 0; i < assignment_context.virtual_segment_num; i++) {
			int largestFileIndex = i;
			int maxLogicLen = -1;
			for (int j = 0; j < filesPerSegment; j++) {
				if ((i + j * assignment_context.virtual_segment_num < fileCount) &&
						(rel_file_vector[i + j * assignment_context.virtual_segment_num]->logic_len > maxLogicLen)) {
					largestFileIndex = i + j * assignment_context.virtual_segment_num;
					maxLogicLen =
							rel_file_vector[i + j * assignment_context.virtual_segment_num]->logic_len;
				}
			}
			for (int j = 0; j < assignment_context.virtual_segment_num; j++) {
				numOfLocalRead[j] = 0;
			}

			for(int k=0;k<rel_file_vector[largestFileIndex]->block_num;k++){
				Block_Host_Index *hostID = rel_file_vector[largestFileIndex]->hostIDs + k;
				for (int l = 0; l < hostID->replica_num; l++) {
					uint32_t key = hostID->hostIndex[l];
					PAIR pair = getHASHTABLENode(assignment_context_ptr->vseg_to_splits_map,TYPCONVERT(void *,key));
					if( pair == NULL)
					{
						continue;
					}
					List* val = (List*)(pair->Value);
					foreach(lc, val)
					{
						int j = lfirst_int(lc);
						if (vs2fileno[j] == -1) {
							numOfLocalRead[j] = numOfLocalRead[j] + 1;
						}
					}
				}
			}
			int localMax = -1;
			for (int j = 0; j < assignment_context.virtual_segment_num; j++) {
				if (numOfLocalRead[j] > localMax && vs2fileno[j] == -1) {
					// assign file group to the same host(container) index.
					for (int k = 0; k < filesPerSegment; k++) {
							rel_file_vector[i + k * assignment_context.virtual_segment_num]->segmentid = j;
					}
					localMax = numOfLocalRead[j];
				}
			}
			if (debug_print_split_alloc_result) {
				elog(LOG, "hash file segno %d 's max locality is %d",rel_file_vector[i]->segno,rel_file_vector[i]->segmentid);
			}
			vs2fileno[rel_file_vector[i]->segmentid] = rel_file_vector[i]->segno;
		}
		MemoryContext old = MemoryContextSwitchTo(TopMemoryContext);

		int segmentCount = assignment_context.virtual_segment_num;

		segmentFilenoPair* sfPairVector = (segmentFilenoPair *) palloc(
				sizeof(segmentFilenoPair) * segmentCount);
		int p = 0;
		for (p = 0; p < segmentCount; p++) {
			sfPairVector[p].segmentid = p;
			sfPairVector[p].fileno = vs2fileno[p];
		}
		qsort(sfPairVector, segmentCount, sizeof(segmentFilenoPair),
				compare_container_segment);
		Segment** segmentsVector = (Segment **) palloc(
				sizeof(Segment*) * segmentCount);
		p = 0;
		foreach (lc, (*resourcePtr)->segments)
		{
			Segment *info = (Segment *) lfirst(lc);
			segmentsVector[p++] = info;
		}
		TargetSegmentIDMap tmpidMap;
		tmpidMap.target_segment_num = idMap.target_segment_num;
		tmpidMap.global_IDs = (int *) palloc(
				sizeof(int) * tmpidMap.target_segment_num);
		tmpidMap.hostname = (char **) palloc(
				sizeof(char*) * tmpidMap.target_segment_num);
		for (int l = 0; l < tmpidMap.target_segment_num; l++) {
			tmpidMap.hostname[l] = (char *) palloc(
					sizeof(char) * HOSTNAME_MAX_LENGTH);
		}

		for (int l = 0; l < tmpidMap.target_segment_num; l++) {
			tmpidMap.global_IDs[l] = idMap.global_IDs[l];
			strncpy(tmpidMap.hostname[l], idMap.hostname[l], HOSTNAME_MAX_LENGTH - 1);
		}

		for (p = 0; p < segmentCount; p++) {
			segmentsVector[sfPairVector[p].segmentid]->segindex = p;
			/* Adjust the segment order shouldn't apply extra memory yourself
			 * because it's not easy to find the right place to free the memory from topMemctx
			 */
			list_nth_replace((*resourcePtr)->segments, p, segmentsVector[sfPairVector[p].segmentid]);
			(*idMap_ptr).global_IDs[p] =
					tmpidMap.global_IDs[sfPairVector[p].segmentid];
			strncpy((*idMap_ptr).hostname[p],
					tmpidMap.hostname[sfPairVector[p].segmentid],
					HOSTNAME_MAX_LENGTH-1);
			if (debug_print_split_alloc_result) {
				elog(LOG, "virtual segment No%d 's name is %s.",p, (*idMap_ptr).hostname[p]);
			}
		}
		pfree(tmpidMap.global_IDs);
		for (int l = 0; l < tmpidMap.target_segment_num; l++) {
			pfree(tmpidMap.hostname[l]);
		}
		pfree(tmpidMap.hostname);
		pfree(sfPairVector);
		pfree(segmentsVector);

		MemoryContextSwitchTo(old);
		pfree(vs2fileno);
		pfree(rel_file_vector);
		pfree(numOfLocalRead);
	}
}

static void change_hash_virtual_segments_order_magma_file(QueryResource ** resourcePtr,
    Relation_Data *rel_data,
    Relation_Assignment_Context *assignment_context_ptr,
    TargetSegmentIDMap* idMap_ptr) {

  assert(rel_data->type == DATALOCALITY_MAGMA);
  ListCell *lc_file;

  if (debug_print_split_alloc_result) {
    elog(LOG, "change virtual segments order");
  }
  Relation_Assignment_Context assignment_context = *assignment_context_ptr;
  //ListCell *lc_file;
  int fileCount = list_length(rel_data->files);

  TargetSegmentIDMap idMap = *idMap_ptr;

  // empty table may have zero filecount
  if (fileCount > 0) {
    Relation_File** rel_file_vector = (Relation_File**) palloc(
        sizeof(Relation_File*) * fileCount);
    int i = 0;
    foreach(lc_file, rel_data->files)
    {
      rel_file_vector[i++] = (Relation_File *) lfirst(lc_file);
    }
    qsort(rel_file_vector, fileCount, sizeof(Relation_File*),
        compare_file_segno);
    // fileCount = assignment_context.virtual_segment_num * const value
    // when there is parallel insert, const value can be greater than 1

    if (fileCount != assignment_context.virtual_segment_num) {
      int* numOfLocalRead = (int *) palloc(sizeof(int) * assignment_context.virtual_segment_num);
      int* numOfFile2Vseg = (int *) palloc(sizeof(int) * assignment_context.virtual_segment_num);
      for (int j = 0; j < assignment_context.virtual_segment_num; j++) {
        numOfFile2Vseg[j] = 0;
      }
      // assign files to the virtual segments with max local read. one file group to one virtual segment
      // file group is files with the same module value to virtual_segment_num
      ListCell* lc;

      // when it is magma table, the file count may be not mutiple of virtual_segment_num,
      // need to skip the assignment of file which not exist.
      for (i = 0; i < fileCount; i++) {
        for (int j = 0; j < assignment_context.virtual_segment_num; j++) {
          numOfLocalRead[j] = 0;
        }

        for (int k=0;k<rel_file_vector[i]->block_num;k++){
          Block_Host_Index *hostID = rel_file_vector[i]->hostIDs + k;
          for (int l = 0; l < hostID->replica_num; l++) {
            uint32_t key = hostID->hostIndex[l];
            PAIR pair = getHASHTABLENode(assignment_context_ptr->vseg_to_splits_map,TYPCONVERT(void *,key));
            if( pair == NULL)
            {
              continue;
            }
            List* val = (List*)(pair->Value);
            foreach(lc, val)
            {
              int j = lfirst_int(lc);
              numOfLocalRead[j] += 1;
            }
          }
        }

        // assign file to the vseg with max datalocality and least file to process
        int localMax = -1;
        List *MaxLocalVseg = NIL;
        for (int j = 0; j < assignment_context.virtual_segment_num; j++) {
          if (numOfLocalRead[j] > localMax) {
            localMax = numOfLocalRead[j];
          }
        }
        int *vsegNo;
        for (int j = 0; j < assignment_context.virtual_segment_num; j++) {
          if (numOfLocalRead[j] == localMax) {
            vsegNo = palloc(sizeof(int));
            *vsegNo = j;
            MaxLocalVseg = lappend(MaxLocalVseg, (void *)vsegNo);
          }
        }
        ListCell *vseg;
        int LeastFileNum = MAX_INT32;
        int LeastFileNumVsegIndex = -1;
        foreach(vseg, MaxLocalVseg) {
          int *v = (int *) lfirst(vseg);
          if (numOfFile2Vseg[*v] < LeastFileNum) {
            LeastFileNumVsegIndex = *v;
            LeastFileNum = numOfFile2Vseg[*v];
          }
        }
        rel_file_vector[i]->segmentid = LeastFileNumVsegIndex;
        numOfFile2Vseg[LeastFileNumVsegIndex] += 1;

        // after assign file to each vseg,
        if (debug_print_split_alloc_result) {
          elog(LOG, "hash file segno %d 's max locality is %d",rel_file_vector[i]->segno,rel_file_vector[i]->segmentid);
        }
      }
      pfree(numOfLocalRead);
      pfree(numOfFile2Vseg);
    } else {
      (*resourcePtr)->vsegChangedMagma = true;
      int* numOfLocalRead = (int *) palloc(sizeof(int) * assignment_context.virtual_segment_num);
      int *vs2fileno = (int *) palloc(
          sizeof(int) * assignment_context.virtual_segment_num);
      for (int i = 0; i < assignment_context.virtual_segment_num; i++) {
        vs2fileno[i] = -1;
      }

      for (i = 0; i < assignment_context.virtual_segment_num; i++) {

        for (int j = 0; j < assignment_context.virtual_segment_num; j++) {
          numOfLocalRead[j] = 0;
        }

        ListCell *lc = NULL;
        for(int k=0;k<rel_file_vector[i]->block_num;k++){
          Block_Host_Index *hostID = rel_file_vector[i]->hostIDs + k;
          for (int l = 0; l < hostID->replica_num; l++) {
            uint32_t key = hostID->hostIndex[l];
            PAIR pair = getHASHTABLENode(assignment_context_ptr->vseg_to_splits_map,TYPCONVERT(void *,key));
            if( pair == NULL)
            {
              continue;
            }
            List* val = (List*)(pair->Value);
            foreach(lc, val)
            {
              int j = lfirst_int(lc);
              if (vs2fileno[j] == -1) {
                numOfLocalRead[j] = numOfLocalRead[j] + 1;
              }
            }
          }
        }
        int localMax = -1;
        for (int j = 0; j < assignment_context.virtual_segment_num; j++) {
          if (numOfLocalRead[j] > localMax && vs2fileno[j] == -1) {
            // assign file group to the same host(container) index.
            rel_file_vector[i]->segmentid = j;
            localMax = numOfLocalRead[j];
          }
        }
        if (debug_print_split_alloc_result) {
          elog(LOG, "hash file segno %d 's max locality is %d",rel_file_vector[i]->segno,rel_file_vector[i]->segmentid);
        }
        vs2fileno[rel_file_vector[i]->segmentid] = rel_file_vector[i]->segno;
      }
      int segmentCount = assignment_context.virtual_segment_num;

      MemoryContext old = MemoryContextSwitchTo(TopMemoryContext);

      for (i = 0; i < fileCount; i++) {
          vs2fileno[rel_file_vector[i]->segmentid] = rel_file_vector[i]->segno;
      }
      segmentFilenoPair* sfPairVector = (segmentFilenoPair *) palloc(
          sizeof(segmentFilenoPair) * segmentCount);
      int p = 0;
      for (p = 0; p < segmentCount; p++) {
        sfPairVector[p].segmentid = p;
        sfPairVector[p].fileno = vs2fileno[p];
      }
      qsort(sfPairVector, segmentCount, sizeof(segmentFilenoPair),
          compare_container_segment);
      Segment** segmentsVector = (Segment **) palloc(
          sizeof(Segment*) * segmentCount);
      p = 0;
      ListCell* lc;
      foreach (lc, (*resourcePtr)->segments)
      {
        Segment *info = (Segment *) lfirst(lc);
        segmentsVector[p++] = info;
      }
      TargetSegmentIDMap tmpidMap;
      tmpidMap.target_segment_num = idMap.target_segment_num;
      tmpidMap.global_IDs = (int *) palloc(
          sizeof(int) * tmpidMap.target_segment_num);
      tmpidMap.hostname = (char **) palloc(
          sizeof(char*) * tmpidMap.target_segment_num);
      for (int l = 0; l < tmpidMap.target_segment_num; l++) {
        tmpidMap.hostname[l] = (char *) palloc(
            sizeof(char) * HOSTNAME_MAX_LENGTH);
      }

      for (int l = 0; l < tmpidMap.target_segment_num; l++) {
        tmpidMap.global_IDs[l] = idMap.global_IDs[l];
        strncpy(tmpidMap.hostname[l], idMap.hostname[l], HOSTNAME_MAX_LENGTH - 1);
      }

      for (p = 0; p < segmentCount; p++) {
        segmentsVector[sfPairVector[p].segmentid]->segindex = p;
        /* Adjust the segment order shouldn't apply extra memory yourself
         * because it's not easy to find the right place to free the memory from topMemctx
         */
        list_nth_replace((*resourcePtr)->segments, p, segmentsVector[sfPairVector[p].segmentid]);
        (*idMap_ptr).global_IDs[p] =
            tmpidMap.global_IDs[sfPairVector[p].segmentid];
        strncpy((*idMap_ptr).hostname[p],
            tmpidMap.hostname[sfPairVector[p].segmentid],
            HOSTNAME_MAX_LENGTH-1);
        if (debug_print_split_alloc_result) {
          elog(LOG, "virtual segment No%d 's name is %s.",p, (*idMap_ptr).hostname[p]);
        }
      }
      pfree(tmpidMap.global_IDs);
      for (int l = 0; l < tmpidMap.target_segment_num; l++) {
        pfree(tmpidMap.hostname[l]);
      }
      pfree(tmpidMap.hostname);
      pfree(sfPairVector);
      pfree(segmentsVector);
      MemoryContextSwitchTo(old);
      pfree(numOfLocalRead);
      pfree(vs2fileno);
    }
    pfree(rel_file_vector);
  }
}

static void change_hash_virtual_segments_order_orc_file(QueryResource ** resourcePtr,
		Relation_Data *rel_data,
		Relation_Assignment_Context *assignment_context_ptr,
		TargetSegmentIDMap* idMap_ptr) {

	// first we check if datalocality is one without changing vseg order
	ListCell *lc_file;
	bool datalocalityEqualsOne = true;
	foreach(lc_file, rel_data->files)
	{
		Relation_File *rel_file = (Relation_File *) lfirst(lc_file);
		for (int i = 0; i < rel_file->split_num; i++) {
			int targethost = (rel_file->segno - 1)
					% ((*assignment_context_ptr).virtual_segment_num);
			for (int p = 0; p < rel_file->block_num; p++) {
				bool islocal = false;
				Block_Host_Index *hostID = rel_file->hostIDs + p;
				for (int l = 0; l < hostID->replica_num; l++) {
					if (hostID->hostIndex[l] == (*idMap_ptr).global_IDs[targethost]) {
						islocal = true;
						break;
					}
				}
				if (!islocal) {
					datalocalityEqualsOne = false;
				}
			}
		}
	}
	if(datalocalityEqualsOne){
		if (debug_print_split_alloc_result) {
			elog(LOG, "didn't change virtual segments order");
		}
		return;
	}

	if (debug_print_split_alloc_result) {
		elog(LOG, "change virtual segments order");
	}
	TargetSegmentIDMap idMap = *idMap_ptr;
	Relation_Assignment_Context assignment_context = *assignment_context_ptr;
	//ListCell *lc_file;
	int fileCount = list_length(rel_data->files);

	// empty table may have zero filecount
	if (fileCount > 0) {
		int *vs2fileno = (int *) palloc0(
				sizeof(int) * assignment_context.virtual_segment_num);
		for (int i = 0; i < assignment_context.virtual_segment_num; i++) {
			vs2fileno[i] = -1;
		}
		Relation_File** rel_file_vector = (Relation_File**) palloc0(
				sizeof(Relation_File*) * fileCount);
		int i = 0;
		foreach(lc_file, rel_data->files)
		{
			rel_file_vector[i++] = (Relation_File *) lfirst(lc_file);
		}
		qsort(rel_file_vector, fileCount, sizeof(Relation_File*),
				compare_file_segno);

		int segmentCount = assignment_context.virtual_segment_num;
		int activeSegCount = 0;
		bool *segmentHasFiles = (bool *) palloc0(sizeof(bool) * segmentCount);
		int *filesPerSegment = (int *) palloc0(sizeof(int) * segmentCount);

		for (i = 0; i < rel_data->files->length; i++)
		{
			if (!segmentHasFiles[rel_file_vector[i]->segno - 1])
			{
				activeSegCount++;
				segmentHasFiles[rel_file_vector[i]->segno - 1] = true;
			}
			filesPerSegment[activeSegCount - 1]++;
		}

		int* numOfLocalRead = (int *) palloc0(sizeof(int) * segmentCount);
		// assign files to the virtual segments with max local read. one file group to one virtual segment
		// file group is files with the same module value to virtual_segment_num
		ListCell* lc;
		int fileNo = 0;
		for (i = 0; i < activeSegCount; i++) {
			int largestFileIndex = i;
			int maxLogicLen = -1;
			for (int j = 0; j < filesPerSegment[i]; j++)
			{
				if (rel_file_vector[fileNo + j]->logic_len > maxLogicLen)
				{
					largestFileIndex = fileNo + j;
					maxLogicLen = rel_file_vector[fileNo + j]->logic_len;
				}
			}
			for (int j = 0; j < activeSegCount; j++) {
				numOfLocalRead[j] = 0;
			}

			for(int k=0;k<rel_file_vector[largestFileIndex]->block_num;k++){
				Block_Host_Index *hostID = rel_file_vector[largestFileIndex]->hostIDs + k;
				for (int l = 0; l < hostID->replica_num; l++) {
					uint32_t key = hostID->hostIndex[l];
					PAIR pair = getHASHTABLENode(assignment_context_ptr->vseg_to_splits_map,TYPCONVERT(void *,key));
					if( pair == NULL)
					{
						continue;
					}
					List* val = (List*)(pair->Value);
					foreach(lc, val)
					{
						int j = lfirst_int(lc);
						if (vs2fileno[j] == -1) {
							numOfLocalRead[j] = numOfLocalRead[j] + 1;
						}
					}
				}
			}
			int localMax = -1;
			for (int j = 0; j < activeSegCount; j++) {
				if (numOfLocalRead[j] > localMax && vs2fileno[j] == -1) {
					// assign file group to the same host(container) index.
					for (int k = 0; k < filesPerSegment[i]; k++) {
						rel_file_vector[fileNo + k]->segmentid = j;
					}
					localMax = numOfLocalRead[j];
				}
			}
			if (debug_print_split_alloc_result) {
				elog(LOG, "hash file segno %d 's max locality is %d",rel_file_vector[i]->segno,rel_file_vector[i]->segmentid);
			}
			vs2fileno[rel_file_vector[i]->segmentid] = rel_file_vector[i]->segno;
			fileNo += filesPerSegment[i];
		}
		MemoryContext old = MemoryContextSwitchTo(TopMemoryContext);

		segmentFilenoPair* sfPairVector = (segmentFilenoPair *) palloc0(
				sizeof(segmentFilenoPair) * segmentCount);
		int p = 0;
		for (p = 0; p < segmentCount; p++) {
			sfPairVector[p].segmentid = p;
			sfPairVector[p].fileno = vs2fileno[p];
		}
		qsort(sfPairVector, segmentCount, sizeof(segmentFilenoPair),
				compare_container_segment);
		Segment** segmentsVector = (Segment **) palloc0(
				sizeof(Segment*) * segmentCount);
		p = 0;
		foreach (lc, (*resourcePtr)->segments)
		{
			Segment *info = (Segment *) lfirst(lc);
			segmentsVector[p++] = info;
		}
		TargetSegmentIDMap tmpidMap;
		tmpidMap.target_segment_num = idMap.target_segment_num;
		tmpidMap.global_IDs = (int *) palloc0(
				sizeof(int) * tmpidMap.target_segment_num);
		tmpidMap.hostname = (char **) palloc0(
				sizeof(char*) * tmpidMap.target_segment_num);
		for (int l = 0; l < tmpidMap.target_segment_num; l++) {
			tmpidMap.hostname[l] = (char *) palloc0(
					sizeof(char) * HOSTNAME_MAX_LENGTH);
		}

		for (int l = 0; l < tmpidMap.target_segment_num; l++) {
			tmpidMap.global_IDs[l] = idMap.global_IDs[l];
			strncpy(tmpidMap.hostname[l], idMap.hostname[l], HOSTNAME_MAX_LENGTH - 1);
		}

		for (p = 0; p < segmentCount; p++) {
			segmentsVector[sfPairVector[p].segmentid]->segindex = p;
			/* Adjust the segment order shouldn't apply extra memory yourself
			 * because it's not easy to find the right place to free the memory from topMemctx
			 */
			list_nth_replace((*resourcePtr)->segments, p, segmentsVector[sfPairVector[p].segmentid]);
			(*idMap_ptr).global_IDs[p] =
					tmpidMap.global_IDs[sfPairVector[p].segmentid];
			strncpy((*idMap_ptr).hostname[p],
					tmpidMap.hostname[sfPairVector[p].segmentid],
					HOSTNAME_MAX_LENGTH-1);
			if (debug_print_split_alloc_result) {
				elog(LOG, "virtual segment No%d 's name is %s.",p, (*idMap_ptr).hostname[p]);
			}
		}
		pfree(tmpidMap.global_IDs);
		for (int l = 0; l < tmpidMap.target_segment_num; l++) {
			pfree(tmpidMap.hostname[l]);
		}
		pfree(tmpidMap.hostname);
		pfree(sfPairVector);
		pfree(segmentsVector);

		MemoryContextSwitchTo(old);
		pfree(vs2fileno);
		pfree(segmentHasFiles);
		pfree(filesPerSegment);
		pfree(rel_file_vector);
		pfree(numOfLocalRead);
	}
}

/*
 *
 */
static void allocation_preparation(List *hosts, TargetSegmentIDMap* idMap,
		Relation_Assignment_Context* assignment_context,
		split_to_segment_mapping_context *context) {
	/*
	 * initialize the ID mapping.
	 */
	idMap->target_segment_num = list_length(hosts);
	idMap->global_IDs = (int *) palloc(sizeof(int) * idMap->target_segment_num);
	idMap->hostname = (char **) palloc(sizeof(char*) * idMap->target_segment_num);
	for (int p = 0; p < idMap->target_segment_num; p++) {
		idMap->hostname[p] = (char *) palloc(sizeof(char) * HOSTNAME_MAX_LENGTH);
	}
	int i = 0;

	assignment_context->vseg_to_splits_map = createHASHTABLE(
			context->datalocality_memorycontext, 2048,
			HASHTABLE_SLOT_VOLUME_DEFAULT_MAX, HASHTABLE_KEYTYPE_UINT32,
			NULL);

	ListCell *lc;
	foreach(lc, hosts)
	{
		VirtualSegmentNode *vsn = (VirtualSegmentNode *) lfirst(lc);

		HostDataVolumeInfo *hdvInfo = search_host_in_stat_context(context,
				vsn->hostname);
		idMap->global_IDs[i] = hdvInfo->hashEntry->index;

		// add vseg to hashtable
		uint32_t key=idMap->global_IDs[i];
		if(getHASHTABLENode(assignment_context->vseg_to_splits_map,TYPCONVERT(void *,key))==NULL){
			setHASHTABLENode(assignment_context->vseg_to_splits_map, TYPCONVERT(void *,key), NIL, false);
		}
		PAIR p = getHASHTABLENode(assignment_context->vseg_to_splits_map,TYPCONVERT(void *,key));
		List** val = (List **)(&(p->Value));
		*val = lappend_int(*val, i);

		hdvInfo->occur_count++;
		strncpy(idMap->hostname[i], vsn->hostname, HOSTNAME_MAX_LENGTH-1);
		if (debug_print_split_alloc_result) {
			elog(LOG, "datalocality using segment No%d hostname/id: %s/%d",i,vsn->hostname,idMap->global_IDs[i]);
		}
		i++;
		// fake data locality
		if (debug_fake_datalocality) {
			fprintf(fp, "virtual segment No%d: %s\n", i - 1,
					vsn->hostname);
		}
	}

	/*
	 * initialize the assignment context.
	 */
	assignment_context->virtual_segment_num = idMap->target_segment_num;
	assignment_context->vols = (int64 *) palloc(
			sizeof(int64) * assignment_context->virtual_segment_num);
	assignment_context->totalvols = (int64 *) palloc(
			sizeof(int64) * assignment_context->virtual_segment_num);
	assignment_context->totalvols_with_penalty = (int64 *) palloc(
				sizeof(int64) * assignment_context->virtual_segment_num);
	assignment_context->continue_split_num = (int *) palloc(
			sizeof(int) * assignment_context->virtual_segment_num);
	assignment_context->split_num = (int *) palloc(
			sizeof(int) * assignment_context->virtual_segment_num);
	assignment_context->roundrobinIndex = 0;
	assignment_context->total_split_num = 0;
	assignment_context->avg_size_of_whole_query =0.0;
	MemSet(assignment_context->totalvols, 0,
			sizeof(int64) * assignment_context->virtual_segment_num);
	MemSet(assignment_context->totalvols_with_penalty, 0,
				sizeof(int64) * assignment_context->virtual_segment_num);
	MemSet(assignment_context->continue_split_num, 0,
			sizeof(int) * assignment_context->virtual_segment_num);
	MemSet(assignment_context->split_num, 0,
			sizeof(int) * assignment_context->virtual_segment_num);
}
/*
 *change_file_order_based_on_continuity
 */
static Relation_File** change_file_order_based_on_continuity(
		Relation_Data *rel_data, TargetSegmentIDMap* idMap, int host_num,
		int* fileCount, Relation_Assignment_Context *assignment_context) {

	Relation_File** file_vector = NULL;
	int* isBlockContinue = (int *) palloc(sizeof(int) * host_num);
	for (int i = 0; i < host_num; i++) {
		isBlockContinue[i] = 0;
	}
	*fileCount = 0;
	ListCell* lc_file;

// sort relations by size.
	foreach(lc_file, rel_data->files)
	{
		Relation_File *rel_file = (Relation_File *) lfirst(lc_file);
		if (rel_file->hostIDs == NULL) {
			rel_file->continue_ratio = 0;
			*fileCount = *fileCount + 1;
			continue;
		}
		bool isBlocksBegin = true;
		bool isLocalContinueBlockFound = false;
		int file_total_block_count = 0;
		int file_continue_block_count = 0;
		ListCell* lc;
		for (int i = 0; i < host_num; i++) {
			isBlockContinue[i] = 0;
		}
		int continuityBeginIndex = 0;
		file_total_block_count = rel_file->split_num;
		for (int i = 0; i < rel_file->split_num; i++) {
			if (isBlocksBegin) {
				Block_Host_Index *hostID = rel_file->hostIDs + i;
				for (int l = 0; l < hostID->replica_num; l++) {
					uint32_t key = hostID->hostIndex[l];
					PAIR pair = getHASHTABLENode(assignment_context->vseg_to_splits_map,
							TYPCONVERT(void *, key));
					if (pair == NULL) {
						continue;
					}
					List* val = (List*) (pair->Value);
					foreach(lc, val)
					{
						int j = lfirst_int(lc);
						isBlockContinue[j]++;
						isLocalContinueBlockFound = true;
						isBlocksBegin = false;
					}
				}
			} else {

				Block_Host_Index *hostID = rel_file->hostIDs + i;
				for (int l = 0; l < hostID->replica_num; l++) {
					uint32_t key = hostID->hostIndex[l];
					PAIR pair = getHASHTABLENode(assignment_context->vseg_to_splits_map,
							TYPCONVERT(void *, key));
					if (pair == NULL) {
						continue;
					}
					List* val = (List*) (pair->Value);
					foreach(lc, val)
					{
						int j = lfirst_int(lc);
						if (isBlockContinue[j] == i - continuityBeginIndex) {
							isBlockContinue[j]++;
							isLocalContinueBlockFound = true;
						}
					}
				}
			}
			if (!isLocalContinueBlockFound || i == rel_file->split_num - 1) {
				int maxBlockContinue = 0;
				for (int k = 0; k < host_num; k++) {
					if (isBlockContinue[k] > maxBlockContinue) {
						maxBlockContinue = isBlockContinue[k];
					}
				}
				if (maxBlockContinue >= 2) {
					file_continue_block_count += maxBlockContinue;
				}
				for (int k = 0; k < host_num; k++) {
					isBlockContinue[k] = 0;
				}
				isBlocksBegin = true;
				if (maxBlockContinue == 0) {
					continuityBeginIndex = i + 1;
				} else if (i != rel_file->split_num - 1) {
					continuityBeginIndex = i;
					i = i - 1;
				}
			}
			isLocalContinueBlockFound = false;
		}
		rel_file->continue_ratio = (double) file_continue_block_count
				/ file_total_block_count;

		if (debug_print_split_alloc_result) {
			elog(LOG, "file %d continuity ratio %f",rel_file->segno,rel_file->continue_ratio);
		}
		*fileCount = *fileCount + 1;
	}

	if (*fileCount > 0) {
		file_vector = (Relation_File**) palloc(sizeof(Relation_File*) * *fileCount);
		int fileindex = 0;
		foreach(lc_file, rel_data->files)
		{
			file_vector[fileindex++] = (Relation_File *) lfirst(lc_file);
		}
		qsort(file_vector, *fileCount, sizeof(Relation_File*),
				compare_file_continuity);
	}

	pfree(isBlockContinue);
	return file_vector;
}

/*
 *set_maximum_segment_volume_parameter
 */
static int64 set_maximum_segment_volume_parameter(Relation_Data *rel_data,
		int vseg_num, double *maxSizePerSegment) {
	int64 maxSizePerSegmentDiffBigVolume = 0;
	int64 maxSizePerSegmentDiffSmallVolume = 0;
	int64 maxSizePerSegmentDiffScalar = 0;
	*maxSizePerSegment = rel_data->total_size / (double) vseg_num;
	ListCell* lc_file;
	int64 totalRelLastBlockSize = 0;

	foreach(lc_file, rel_data->files)
	{
		Relation_File *rel_file = (Relation_File *) lfirst(lc_file);
		for (int i = 0; i < rel_file->split_num; i++) {
			if (i == rel_file->split_num - 1) {
				totalRelLastBlockSize += rel_file->splits[i].length;
			}
		}
	}
	double bigVolumeRatio = 0.001;
	double smallVolumeRatio = 0.05;
	maxSizePerSegmentDiffBigVolume = *maxSizePerSegment * bigVolumeRatio;
	maxSizePerSegmentDiffSmallVolume = totalRelLastBlockSize / (double) vseg_num
			* smallVolumeRatio;
	maxSizePerSegmentDiffScalar = 32 << 20;

	// when curSize > maxSizePerSegment, we allow some segments exceed the average volumes.
	// with conditions: it less than 0.001* totalrelationsize and
	// less than 32M(in case of Big Table such as 1T*0.001=1G which lead to data extremely
	// imbalance) or it less than 0.05* the sum of size of all the last blocks of relation,
	// which we call it as maxSizePerSegmentDiffSmallVolume (consider we have 64 small files with 1.5M
	// avg size and 16 segment to assign. maxSizePerSegmentshould be 1.5*4 =6M and we can allow
	// a exceed about 6+ 6*0.05=6.3M)
	if (maxSizePerSegmentDiffBigVolume > maxSizePerSegmentDiffScalar){
		maxSizePerSegmentDiffBigVolume = maxSizePerSegmentDiffScalar;
	}
	if(maxSizePerSegmentDiffBigVolume > maxSizePerSegmentDiffSmallVolume){
		return maxSizePerSegmentDiffBigVolume + (int64)(*maxSizePerSegment) + 1;
	}
	else{
		return maxSizePerSegmentDiffSmallVolume + (int64)(*maxSizePerSegment) + 1;
	}
}

/*
 *allocate_hash_relation
 */
/*
 *allocate_random_relation
 */
static bool allocate_hash_relation(Relation_Data* rel_data,
		Assignment_Log_Context* log_context, TargetSegmentIDMap* idMap,
		Relation_Assignment_Context* assignment_context,
		split_to_segment_mapping_context *context, bool parentIsHashExist, bool parentIsHash) {
	/*allocation unit in hash relation is file, we assign all the blocks of one file to one virtual segments*/
	ListCell *lc_file;
	int fileCount = 0;
	Oid myrelid = rel_data->relid;
	double relationDatalocality=1.0;

	uint64_t before_allocate_hash = gettime_microsec();
	foreach(lc_file, rel_data->files)
	{
		fileCount++;
		Relation_File *rel_file = (Relation_File *) lfirst(lc_file);
		int split_num = rel_file->split_num;
		/*calculate keephash datalocality*/
		/*for keep hash one file corresponds to one split*/
		for (int i = 0; i < rel_file->block_num; i++) {
			/*
			 * traverse each block, calculate total data size of relation and data
			 * size which are processed locally.
			 */
			int64 split_size = rel_file->locations[i].length;
			int targethost = (rel_file->segno - 1) % (assignment_context->virtual_segment_num);
			bool islocal = false;
			Block_Host_Index *hostID = rel_file->hostIDs + i;
			for (int l = 0; l < hostID->replica_num; l++) {
				/*
				 * traverse each replica of one block. If there is one replica processed by
				 * virtual segment which is on the same host with it, the block is processed locally.
				 */
				if (debug_print_split_alloc_result) {
					elog(LOG, "file id is %d; vd id is %d",hostID->hostIndex[l],idMap->global_IDs[targethost]);
				}
				if (hostID->hostIndex[l] == idMap->global_IDs[targethost]) {
					/*
					 * for hash relation, blocks are assigned to virtual segment which
					 * index (targethost) is calculated by segno and virtual segment number.
					 */
					log_context->localDataSizePerRelation +=
							rel_file->locations[i].length;
					islocal = true;
					break;
				}
			}
			if (debug_print_split_alloc_result && !islocal) {
				elog(LOG, "non local relation %u, file: %d, block: %d",myrelid,rel_file->segno,i);
			}
			log_context->totalDataSizePerRelation += split_size;
		}
	}
	/* ao table merged all the splits to one in hash distribution, magma table don't merge because
	 range is correspond to hash value and the range and RG map is stored in each split*/
	uint64_t after_allocate_hash = gettime_microsec();
	int time_allocate_hash_firstfor = after_allocate_hash - before_allocate_hash;

	if(log_context->totalDataSizePerRelation > 0){
			relationDatalocality = log_context->localDataSizePerRelation / log_context->totalDataSizePerRelation;
	}
	double hash2RandomDatalocalityThreshold= 0.9;
	/*for partition hash table, whether to convert random table to hash
	 * is determined by the datalocality of the first partition*/
	if (parentIsHashExist) {
		if (!parentIsHash) {
			log_context->totalDataSizePerRelation = 0;
			log_context->localDataSizePerRelation = 0;
			return true;
		}
	}
	else if((hash_to_random_flag == ENFORCE_HASH_TO_RANDOM ||
			(relationDatalocality < hash2RandomDatalocalityThreshold && relationDatalocality >= 0 ))
			&& hash_to_random_flag != ENFORCE_KEEP_HASH){
		log_context->totalDataSizePerRelation =0;
		log_context->localDataSizePerRelation =0;
		return true;
	}
	fileCount =0;
	foreach(lc_file, rel_data->files)
	{
		fileCount++;
		Relation_File *rel_file = (Relation_File *) lfirst(lc_file);
		for (int i = 0; i < rel_file->split_num; i++) {
			int64 split_size = rel_file->splits[i].length;
			Assert(rel_file->segno > 0);
			int targethost = (rel_file->segno - 1)
					% (assignment_context->virtual_segment_num);
			rel_file->splits[i].host = targethost;
			assignment_context->totalvols[targethost] += split_size;
			assignment_context->split_num[targethost]++;
			assignment_context->continue_split_num[targethost]++;
			if (debug_print_split_alloc_result) {
				elog(LOG, "file %d assigned to host %s",rel_file->segno,
				idMap->hostname[targethost]);
			}

		}
		assignment_context->total_split_num += rel_file->split_num;
	}
	uint64_t after_assigned_time = gettime_microsec();
	int time_allocate_second_for = after_assigned_time - before_allocate_hash;
	if( debug_fake_datalocality ){
		fprintf(fp, "The time of allocate hash relation first for is : %d us.\n", time_allocate_hash_firstfor);
		fprintf(fp, "The time of allocate hash relation is : %d us.\n", time_allocate_second_for);
	}
	return false;
}
static void allocate_hash_relation_for_magma_file(Relation_Data* rel_data,
		Assignment_Log_Context* log_context, TargetSegmentIDMap* idMap,
		Relation_Assignment_Context* assignment_context,
		split_to_segment_mapping_context *context, bool parentIsHashExist, bool parentIsHash) {
	/*allocation unit in hash relation is file, we assign all the blocks of one file to one virtual segments*/
	ListCell *lc_file;
	int fileCount = 0;
	Oid myrelid = rel_data->relid;
	double relationDatalocality=1.0;

	assert(rel_data->type == DATALOCALITY_MAGMA);
	uint64_t before_allocate_hash = gettime_microsec();
	int numoflocal = 0;
	fileCount = rel_data->files ? rel_data->files->length : 0;
	elog(DEBUG3,"before caculate datalocality, the localDataSizePerRelation is %d,"
			"total is %d", log_context->localDataSizePerRelation, log_context->totalDataSizePerRelation);
	foreach(lc_file, rel_data->files)
	{
		Relation_File *rel_file = (Relation_File *) lfirst(lc_file);

		int targethost = fileCount == idMap->target_segment_num ? (rel_file->segno - 1) : rel_file->segmentid;
		elog(DEBUG3,"the block number of this file %d is %d", rel_file->segno, rel_file->block_num);
		/*calculate keephash datalocality*/

		/*for keep hash one file corresponds to one split*/
		for (int p = 0; p < rel_file->block_num; p++)
		{
			bool islocal = false;
			Block_Host_Index *hostID = rel_file->hostIDs + p;
			for (int l = 0; l < hostID->replica_num; l++)
			{
				if (debug_print_split_alloc_result)
				{
					elog(LOG, "file id is %d; vd id is %d",hostID->hostIndex[l],
							idMap->global_IDs[targethost]);
				}
				if (hostID->hostIndex[l] == idMap->global_IDs[targethost])
				{
					log_context->localDataSizePerRelation +=
							rel_file->locations[p].length;
					numoflocal++;
					islocal = true;
					break;
				}
			}
			if (debug_print_split_alloc_result && !islocal)
			{
				elog(LOG, "non local relation %u, file: %d, block: %d",myrelid,
						rel_file->segno,p);
			}
			log_context->totalDataSizePerRelation += rel_file->locations[p].length;
		}
	}
	/* ao table merged all the splits to one in hash distribution, magma table don't merge because
	 range is correspond to hash value and the range and RG map is stored in each split*/
	uint64_t after_allocate_hash = gettime_microsec();
	int time_allocate_hash_firstfor = after_allocate_hash - before_allocate_hash;

	elog(LOG,"after caculate datalocality, the localDataSizePerRelation is %d,"
				"total is %d,the relationDatalocality is %d, num of local block is %d",
				log_context->localDataSizePerRelation, log_context->totalDataSizePerRelation,
				log_context->totalDataSizePerRelation == 0 ? 0 :
				    (log_context->localDataSizePerRelation / log_context->totalDataSizePerRelation),
				numoflocal);
	if(log_context->totalDataSizePerRelation > 0)
	{
			relationDatalocality = log_context->localDataSizePerRelation / log_context->totalDataSizePerRelation;
	}
	// the datalocality ratio in magma table must be One.
	/*if (relationDatalocality != 1.0) {
		elog(ERROR,"datalocality of magma table %d is %f , not equal to One.",
				rel_data->relid, relationDatalocality);
	};*/
	double hash2RandomDatalocalityThreshold= 0.9;
	/*for partition hash table, whether to convert random table to hash
	 * is determined by the datalocality of the first partition*/
	foreach(lc_file, rel_data->files)
	{
		Relation_File *rel_file = (Relation_File *) lfirst(lc_file);
		for (int i = 0; i < rel_file->split_num; i++) {
			int64 split_size = rel_file->splits[i].length;
			Assert(rel_file->segno > 0);
			int targethost = fileCount == idMap->target_segment_num ? (rel_file->segno - 1) : rel_file->segmentid;
			rel_file->splits[i].host = targethost;
			assignment_context->totalvols[targethost] += split_size;
			assignment_context->split_num[targethost]++;
			assignment_context->continue_split_num[targethost]++;
			if (debug_print_split_alloc_result) {
				elog(LOG, "file %d assigned to host %s",rel_file->segno,
				idMap->hostname[targethost]);
			}

		}
		assignment_context->total_split_num += rel_file->split_num;
	}
	uint64_t after_assigned_time = gettime_microsec();
	int time_allocate_second_for = after_assigned_time - before_allocate_hash;
	if( debug_fake_datalocality ){
		fprintf(fp, "The time of allocate hash relation first for is : %d us.\n", time_allocate_hash_firstfor);
		fprintf(fp, "The time of allocate hash relation is : %d us.\n", time_allocate_second_for);
	}
	return;
}

static void allocate_random_relation(Relation_Data* rel_data,
		Assignment_Log_Context* log_context, TargetSegmentIDMap* idMap,
		Relation_Assignment_Context* assignment_context,
		split_to_segment_mapping_context *context) {
	/*different from hash relation, allocation unit in random relation is block*/

	/*first set max size per virtual segments.
	 *size can be exceeded by different strategy for big and small table
	 */
	double maxSizePerSegment = 0.0;
	int64 maxExtendedSizePerSegment = set_maximum_segment_volume_parameter(rel_data,
			assignment_context->virtual_segment_num, &maxSizePerSegment);

	/* sort file based on the ratio of continue local read.
	 * and we will assign the block of the file with maximum continuity
	 */
	int fileCount = 0;
	Oid myrelid = rel_data->relid;
	Oid partition_parent_oid = rel_data->partition_parent_relid;

	if(partition_parent_oid > 0){
		PAIR pa = getHASHTABLENode(assignment_context->patition_parent_size_map,
						TYPCONVERT(void *, partition_parent_oid));
		int64* partitionParentAvgSize = (int64 *) (pa->Value);
		assignment_context->avg_size_of_whole_partition_table = *partitionParentAvgSize;
		if(debug_print_split_alloc_result){
			elog(LOG, "partition table  "INT64_FORMAT" of relation %u",
				assignment_context->avg_size_of_whole_partition_table,partition_parent_oid);
		}
	}

	uint64_t before_change_order = gettime_microsec();
	Relation_File** file_vector = change_file_order_based_on_continuity(rel_data,
			idMap, assignment_context->virtual_segment_num, &fileCount, assignment_context);
	uint64_t after_change_order = gettime_microsec();
	int change_file_order_time = after_change_order - before_change_order;
	if ( debug_fake_datalocality ){
		fprintf(fp, "The time of change_file_order_based_continuity is : %d us.\n", change_file_order_time);
	}

	/* put split into nonContinueLocalQueue when encounter non continue split
	 * put split into networkQueue when split need remote read
	 */
	List *networkQueue = NIL;
	List *nonContinueLocalQueue = NIL;
	int i=0;
	//int j=0;
	bool isExceedMaxSize = false;
	bool isExceedPartitionTableSize = false;
	bool isExceedWholeSize = false;
	int* isBlockContinue = (int *) palloc(
			sizeof(int) * assignment_context->virtual_segment_num);


	/*find the insert node for each block*/
	uint64_t before_run_find_insert_host = gettime_microsec();
	int *hostOccurTimes = (int *) palloc(sizeof(int) * context->dds_context.size);
	for (int fi = 0; fi < fileCount; fi++) {
		Relation_File *rel_file = file_vector[fi];
		/*for hash file whose bucket number doesn't equal to segment number*/
		if (rel_file->hostIDs == NULL) {
			rel_file->splits[0].host = 0;
			continue;
		}
		MemSet(hostOccurTimes, 0,	sizeof(int) * context->dds_context.size);
		for (i = 0; i < rel_file->split_num; i++) {
			Block_Host_Index *hostID = rel_file->hostIDs + i;
			for (int l = 0; l < hostID->replica_num; l++) {
				uint32_t key = hostID->hostIndex[l];
				hostOccurTimes[key]++;
			}
		}
		int maxOccurTime = -1;
		int inserthost = -1;
		int hostsWithSameOccurTimesExist = true;
		for (int i = 0; i < context->dds_context.size; i++) {
			if (hostOccurTimes[i] > maxOccurTime) {
				maxOccurTime = hostOccurTimes[i];
				inserthost = i;
				hostsWithSameOccurTimesExist = false;
			} else if (hostOccurTimes[i] == maxOccurTime) {
				hostsWithSameOccurTimesExist = true;
			}
		}

		/* currently we consider the insert hosts are the same for all the blocks in the same file.
		 * this logic can be changed in future, so we store the state in block level not file level
		 * if hostsWithSameOccurTimesExist we cannot determine which is insert host
		 * if maxOccurTime <2 we cannot determine which is insert host either*/
		if (maxOccurTime < rel_file->split_num || maxOccurTime < 2 || hostsWithSameOccurTimesExist) {
			inserthost = -1;
		}
		for (i = 0; i < rel_file->split_num; i++) {
			Block_Host_Index *hostID = rel_file->hostIDs + i;
			hostID->insertHost = inserthost;
		}
	}
	pfree(hostOccurTimes);

	uint64_t end_run_find_insert_host = gettime_microsec();
	int run_find_insert_host = end_run_find_insert_host - before_run_find_insert_host;
	if(debug_datalocality_time){
		elog(LOG, "find insert host time: %d us. \n", run_find_insert_host);
	}

	/*three stage allocation algorithm*/
	for (int fi = 0; fi < fileCount; fi++) {
		Relation_File *rel_file = file_vector[fi];

		/*for hash file whose bucket number doesn't equal to segment number*/
		if (rel_file->hostIDs == NULL) {
			rel_file->splits[0].host = 0;
			assignment_context->total_split_num += 1;
			continue;
		}

		bool isBlocksBegin = true;
		bool isLocalContinueBlockFound = false;
		ListCell *lc;
		int beginIndex = 0;
		for (i = 0; i < assignment_context->virtual_segment_num; i++) {
			isBlockContinue[i] = 0;
		}
		/* we assign split(block) to host base on continuity
		 * the length of continue blocks of local host determines
		 * the final assignment (we prefer longer one).
		 */
		for (i = 0; i < rel_file->split_num; i++) {
			int64 split_size = rel_file->splits[i].length;
			int64 currentSequenceSize = 0;
			for (int r = beginIndex; r <= i; r++) {
				currentSequenceSize += rel_file->splits[r].length;
			}
			/* first block in one file doesn't need to consider continuity,
			 * but the following blocks must consider it.
			 */
			if (isBlocksBegin) {

				Block_Host_Index *hostID = rel_file->hostIDs + i;
				for (int l = 0; l < hostID->replica_num; l++) {
					uint32_t key = hostID->hostIndex[l];
					PAIR pair = getHASHTABLENode(assignment_context->vseg_to_splits_map,TYPCONVERT(void *,key));
					if( pair == NULL)
					{
						continue;
					}
					List* val = (List*)(pair->Value);
					foreach(lc, val)
					{
						int j = lfirst_int(lc);
						isExceedMaxSize = currentSequenceSize + assignment_context->vols[j]
								> maxExtendedSizePerSegment;
						isExceedWholeSize = balance_on_whole_query_level
								&& currentSequenceSize
										+ assignment_context->totalvols_with_penalty[j]
										> assignment_context->avg_size_of_whole_query;
						int64** partitionvols_with_penalty=NULL;
						if(partition_parent_oid > 0){
						PAIR p = getHASHTABLENode(assignment_context->partitionvols_with_penalty_map,TYPCONVERT(void *,partition_parent_oid));
					  partitionvols_with_penalty = (int64 **)(&(p->Value));
						isExceedPartitionTableSize = balance_on_partition_table_level && currentSequenceSize
								+ (*partitionvols_with_penalty)[j]
							  > assignment_context->avg_size_of_whole_partition_table;
						}else{
							isExceedPartitionTableSize =false;
						}
						if ((!isExceedWholeSize
								|| assignment_context->totalvols_with_penalty[j] == 0)
								&& (!isExceedMaxSize
										|| (i == beginIndex && assignment_context->vols[j] == 0))
								&& (!isExceedPartitionTableSize || (*partitionvols_with_penalty)[j] ==0)) {
							isBlockContinue[j]++;
							isLocalContinueBlockFound = true;
							isBlocksBegin = false;
						}
					}
				}
			} else {

				Block_Host_Index *hostID = rel_file->hostIDs + i;
				for (int l = 0; l < hostID->replica_num; l++) {
					uint32_t key = hostID->hostIndex[l];
					PAIR pair = getHASHTABLENode(assignment_context->vseg_to_splits_map,TYPCONVERT(void *,key));
					if( pair == NULL)
					{
						continue;
					}
					List* val = (List*)(pair->Value);
					foreach(lc, val)
					{
						int j = lfirst_int(lc);
						isExceedMaxSize = currentSequenceSize + assignment_context->vols[j]
								> maxExtendedSizePerSegment;
						isExceedWholeSize = balance_on_whole_query_level
								&& currentSequenceSize
										+ assignment_context->totalvols_with_penalty[j]
										> assignment_context->avg_size_of_whole_query;
						int64** partitionvols_with_penalty = NULL;
						if (partition_parent_oid > 0) {
							PAIR p = getHASHTABLENode(
									assignment_context->partitionvols_with_penalty_map,
									TYPCONVERT(void *, partition_parent_oid));
							partitionvols_with_penalty = (int64 **) (&(p->Value));
							isExceedPartitionTableSize = balance_on_partition_table_level && currentSequenceSize
									+ (*partitionvols_with_penalty)[j]
									> assignment_context->avg_size_of_whole_partition_table;
						} else {
							isExceedPartitionTableSize = false;
						}
						if ((!isExceedWholeSize
								|| assignment_context->totalvols_with_penalty[j] == 0)
								&& (!isExceedMaxSize
										|| (i == beginIndex && assignment_context->vols[j] == 0))
								&& (!isExceedPartitionTableSize || (*partitionvols_with_penalty)[j] ==0)
								&& isBlockContinue[j] == i - beginIndex) {
							isBlockContinue[j]++;
							isLocalContinueBlockFound = true;
						}
					}
				}
			}
			/* no local & continue block found, then assign the former blocks.*/
			if (!isLocalContinueBlockFound || i == rel_file->split_num - 1) {
				int assignedVSeg = -1;
				int maxBlockContinue = 0;
				for (int k = 0; k < assignment_context->virtual_segment_num; k++) {
					if (isBlockContinue[k] > maxBlockContinue) {
						maxBlockContinue = isBlockContinue[k];
						assignedVSeg = k;
					} else if (isBlockContinue[k] > 0
							&& isBlockContinue[k] == maxBlockContinue
							&& assignment_context->vols[k]
									< assignment_context->vols[assignedVSeg]*0.9) {
						assignedVSeg = k;
					}
				}
				/* if there is no local virtual segments to assign
				 * , then push this split to networkQueue
				 */
				if (assignedVSeg == -1) {
					Split_Block *onesplit = makeNode(Split_Block);
					onesplit->fileIndex = fi;
					onesplit->splitIndex = i;
					nonContinueLocalQueue = lappend(nonContinueLocalQueue, onesplit);
					beginIndex = i + 1;
				}
				/* non continue block will be pushed into
				 *nonContinueLocalQueue and processed later.
				 */
				else if (maxBlockContinue == 1 /*&& (i != rel_file->split_num - 1)*/) {
					Split_Block *onesplit = makeNode(Split_Block);
					onesplit->fileIndex = fi;
					if (!isLocalContinueBlockFound) {
						onesplit->splitIndex = i - 1;
						nonContinueLocalQueue = lappend(nonContinueLocalQueue, onesplit);
						beginIndex = i;
						i = i - 1;
					} else {
						onesplit->splitIndex = i;
						nonContinueLocalQueue = lappend(nonContinueLocalQueue, onesplit);
					}
				} else {
					/*split from beginIdex to i-1 should be  assigned to assignedVSeg*/
					for (int r = beginIndex; r < i; r++) {
						assignment_context->vols[assignedVSeg] += rel_file->splits[r].length;
						assignment_context->totalvols[assignedVSeg] += rel_file->splits[r].length;
						assignment_context->totalvols_with_penalty[assignedVSeg] += rel_file->splits[r].length;
						if (partition_parent_oid > 0) {
							PAIR p = getHASHTABLENode(
									assignment_context->partitionvols_with_penalty_map,
									TYPCONVERT(void *, partition_parent_oid));
							int64** partitionvols_with_penalty = (int64 **) (&(p->Value));
							(*partitionvols_with_penalty)[assignedVSeg] +=
									rel_file->splits[r].length;
							p = getHASHTABLENode(assignment_context->partitionvols_map,
									TYPCONVERT(void *, partition_parent_oid));
							int64** partitionvols = (int64 **) (&(p->Value));
							(*partitionvols)[assignedVSeg] +=
									rel_file->splits[r].length;
						}
						rel_file->splits[r].host = assignedVSeg;
						if (debug_print_split_alloc_result) {
//							elog(LOG, "local2 split %d offset "INT64_FORMAT" of file %d is assigned to host %d",r,rel_file->splits[r].offset, rel_file->segno,assignedVSeg);
						}
						log_context->localDataSizePerRelation += rel_file->splits[r].length;
						assignment_context->split_num[assignedVSeg]++;
						assignment_context->continue_split_num[assignedVSeg]++;
					}
					if (!isLocalContinueBlockFound) {
						beginIndex = i;
						i = i - 1;
					}
					/* last split is continue*/
					else {
						log_context->localDataSizePerRelation += split_size;
						assignment_context->vols[assignedVSeg] += split_size;
						assignment_context->totalvols[assignedVSeg] += split_size;
						assignment_context->totalvols_with_penalty[assignedVSeg] += split_size;
						if (partition_parent_oid > 0) {
							PAIR p = getHASHTABLENode(
									assignment_context->partitionvols_with_penalty_map,
									TYPCONVERT(void *, partition_parent_oid));
							int64** partitionvols_with_penalty = (int64 **) (&(p->Value));
							(*partitionvols_with_penalty)[assignedVSeg] += split_size;
							p = getHASHTABLENode(assignment_context->partitionvols_map,
									TYPCONVERT(void *, partition_parent_oid));
							int64** partitionvols = (int64 **) (&(p->Value));
							(*partitionvols)[assignedVSeg] += split_size;
						}
						if (debug_print_split_alloc_result) {
							elog(LOG, "local4 split %d offset "INT64_FORMAT" of file %d is assigned to host %d",i,rel_file->splits[i].offset, rel_file->segno,assignedVSeg);
						}
						rel_file->splits[i].host = assignedVSeg;
						assignment_context->continue_split_num[assignedVSeg]++;
						assignment_context->split_num[assignedVSeg]++;
					}

				}

				for (int r = 0; r < assignment_context->virtual_segment_num; r++) {
					isBlockContinue[r] = 0;
				}
				isBlocksBegin = true;
			}
			isLocalContinueBlockFound = false;
		}
		for (i = 0; i < rel_file->split_num; i++) {
			int64 split_size = rel_file->splits[i].length;
			log_context->totalDataSizePerRelation += split_size;
		}
		assignment_context->total_split_num += rel_file->split_num;
	}

	uint64_t after_continue_block = gettime_microsec();
	int time_of_continue = after_continue_block - after_change_order;
	if ( debug_fake_datalocality ){
		fprintf(fp, "The time of allocate continue block is : %d us.\n", time_of_continue);
		fprintf(fp, "The size of nonContinueLocalQueue is : %d .\n", list_length(nonContinueLocalQueue));
	}

	/*process non cotinue local queue*/
	ListCell *file_split;
	foreach(file_split, nonContinueLocalQueue)
	{
		Split_Block *onesplit = (Split_Block *) lfirst(file_split);

		Relation_File *cur_file = file_vector[onesplit->fileIndex];
		int64 cur_split_size = cur_file->splits[onesplit->splitIndex].length;

		Block_Host_Index *hostID = cur_file->hostIDs + onesplit->splitIndex;
		bool isLocality = false;
		int assignedVSeg = select_random_host_algorithm(assignment_context,
				cur_split_size, maxExtendedSizePerSegment, idMap,
				&hostID, onesplit->fileIndex, partition_parent_oid, &isLocality);

		if (isLocality) {
			assignment_context->vols[assignedVSeg] += cur_split_size;
			assignment_context->totalvols[assignedVSeg] += cur_split_size;
			assignment_context->totalvols_with_penalty[assignedVSeg] += cur_split_size;
			if (partition_parent_oid > 0) {
				PAIR p = getHASHTABLENode(
						assignment_context->partitionvols_with_penalty_map,
						TYPCONVERT(void *, partition_parent_oid));
				int64** partitionvols_with_penalty = (int64 **) (&(p->Value));
				(*partitionvols_with_penalty)[assignedVSeg] += cur_split_size;

				p = getHASHTABLENode(assignment_context->partitionvols_map,
						TYPCONVERT(void *, partition_parent_oid));
				int64** partitionvols = (int64 **) (&(p->Value));
				(*partitionvols)[assignedVSeg] += cur_split_size;
			}
			log_context->localDataSizePerRelation += cur_split_size;
			if (debug_print_split_alloc_result) {
				elog(LOG, "local1 split %d offset "INT64_FORMAT"of file %d is assigned to host %d",onesplit->splitIndex,cur_file->splits[onesplit->splitIndex].offset ,cur_file->segno,assignedVSeg);
			}
			bool isSplitOfFileExistInVseg = false;
			for (int splitindex = 0; splitindex < cur_file->split_num; splitindex++) {
				if (assignedVSeg == cur_file->splits[splitindex].host) {
					isSplitOfFileExistInVseg = true;
				}
			}
			if(!isSplitOfFileExistInVseg){
				assignment_context->continue_split_num[assignedVSeg]++;
			}
			assignment_context->split_num[assignedVSeg]++;
			cur_file->splits[onesplit->splitIndex].host = assignedVSeg;
		} else {
			Split_Block *networksplit = makeNode(Split_Block);
			networksplit->fileIndex = onesplit->fileIndex;
			networksplit->splitIndex = onesplit->splitIndex;
			networkQueue = lappend(networkQueue, networksplit);
		}
	}

	uint64_t after_noncontinue_block = gettime_microsec();
	int time_of_noncontinue = after_noncontinue_block - after_continue_block;
	if (debug_fake_datalocality) {
		fprintf(fp,
				"maxExtendedSizePerSegment is:"INT64_FORMAT", avg_size_of_whole_query is: %.3f",
				maxExtendedSizePerSegment, assignment_context->avg_size_of_whole_query);
		fprintf(fp, "The time of allocate non continue block is : %d us.\n", time_of_noncontinue);
		fprintf(fp, "The size of networkQueue is : %d .\n", list_length(networkQueue));
	}

	/*process networkqueue*/
	foreach(file_split, networkQueue)
	{
		Split_Block *onesplit = (Split_Block *) lfirst(file_split);
		Relation_File *cur_file = file_vector[onesplit->fileIndex];
		int64 cur_split_size = cur_file->splits[onesplit->splitIndex].length;

		Block_Host_Index *hostID = cur_file->hostIDs + onesplit->splitIndex;
		bool isLocality = false;
		int assignedVSeg = select_random_host_algorithm(assignment_context,
				cur_split_size, maxExtendedSizePerSegment, idMap,
				&hostID, onesplit->fileIndex, partition_parent_oid, &isLocality);

		if (isLocality) {
			assignment_context->vols[assignedVSeg] += cur_split_size;
			assignment_context->totalvols[assignedVSeg] += cur_split_size;
			assignment_context->totalvols_with_penalty[assignedVSeg] += cur_split_size;
			if (partition_parent_oid > 0) {
				PAIR p = getHASHTABLENode(
						assignment_context->partitionvols_with_penalty_map,
						TYPCONVERT(void *, partition_parent_oid));
				int64** partitionvols_with_penalty = (int64 **) (&(p->Value));
				(*partitionvols_with_penalty)[assignedVSeg] += cur_split_size;
				p = getHASHTABLENode(assignment_context->partitionvols_map,
						TYPCONVERT(void *, partition_parent_oid));
				int64** partitionvols = (int64 **) (&(p->Value));
				(*partitionvols)[assignedVSeg] += cur_split_size;
			}
			log_context->localDataSizePerRelation += cur_split_size;
			if (debug_print_split_alloc_result) {
				elog(LOG, "local10 split %d offset "INT64_FORMAT"of file %d is assigned to host %d",onesplit->splitIndex,cur_file->splits[onesplit->splitIndex].offset ,cur_file->segno,assignedVSeg);
			}
		} else {
			int64 network_split_size = net_disk_ratio * cur_split_size;
			double network_incre_size = (net_disk_ratio-1)*cur_split_size /assignment_context->virtual_segment_num;
			if (datalocality_remedy_enable) {
				int remedyVseg = remedy_non_localRead(onesplit->fileIndex,
						onesplit->splitIndex, 0, file_vector, fileCount,
						maxExtendedSizePerSegment, idMap, assignment_context);
				if(remedyVseg != -1){
					assignedVSeg = remedyVseg;
					log_context->localDataSizePerRelation += cur_split_size;
					network_split_size = cur_split_size;
					network_incre_size =0;
				}else{
					cur_file->splits[onesplit->splitIndex].is_local_read =false;
				}
			}
			assignment_context->vols[assignedVSeg] += network_split_size;
			assignment_context->totalvols_with_penalty[assignedVSeg] +=
					network_split_size;
			if (partition_parent_oid > 0) {
				PAIR p = getHASHTABLENode(
						assignment_context->partitionvols_with_penalty_map,
						TYPCONVERT(void *, partition_parent_oid));
				int64** partitionvols_with_penalty = (int64 **) (&(p->Value));
				(*partitionvols_with_penalty)[assignedVSeg] += network_split_size;
				p = getHASHTABLENode(assignment_context->partitionvols_map,
						TYPCONVERT(void *, partition_parent_oid));
				int64** partitionvols = (int64 **) (&(p->Value));
				(*partitionvols)[assignedVSeg] += cur_split_size;
			}
			assignment_context->totalvols[assignedVSeg] += cur_split_size;
			maxExtendedSizePerSegment += network_incre_size;
			assignment_context->avg_size_of_whole_query += network_incre_size;
			if(partition_parent_oid > 0){
				PAIR pa = getHASHTABLENode(assignment_context->patition_parent_size_map,
								TYPCONVERT(void *, partition_parent_oid));
				int64* partitionParentAvgSize = (int64 *) (pa->Value);
				*partitionParentAvgSize += network_incre_size;
			}

			if (debug_print_split_alloc_result) {
				elog(LOG, "non local10 split %d offset "INT64_FORMAT" of file %d is assigned to host %d",onesplit->splitIndex,cur_file->splits[onesplit->splitIndex].offset, cur_file->segno,assignedVSeg);
			}
		}

		bool isSplitOfFileExistInVseg = false;
		for (int splitindex = 0; splitindex < cur_file->split_num; splitindex++) {
			if (assignedVSeg == cur_file->splits[splitindex].host) {
				isSplitOfFileExistInVseg = true;
			}
		}
		if (!isSplitOfFileExistInVseg) {
			assignment_context->continue_split_num[assignedVSeg]++;
		}
		assignment_context->split_num[assignedVSeg]++;
		cur_file->splits[onesplit->splitIndex].host = assignedVSeg;
	}
	uint64_t after_network_block = gettime_microsec();
	int time_of_network = after_network_block- after_noncontinue_block;
	if (debug_fake_datalocality) {
		fprintf(fp,
				"maxExtendedSizePerSegment is:"INT64_FORMAT", avg_size_of_whole_query is: %.3f",
				maxExtendedSizePerSegment, assignment_context->avg_size_of_whole_query);
		fprintf(fp, "The time of allocate non continue block is : %d us.\n", time_of_network);
	}

	if (debug_print_split_alloc_result) {
		for (int j = 0; j < assignment_context->virtual_segment_num; j++) {
			if (partition_parent_oid > 0) {
				PAIR p = getHASHTABLENode(assignment_context->partitionvols_map,
						TYPCONVERT(void *, partition_parent_oid));
				int64** partitionvols_with_penalty = (int64 **) (&(p->Value));
				elog(LOG, "for partition parent table %u: sub partition table size of of vseg %d "
						"is "INT64_FORMAT"",partition_parent_oid,j, (*partitionvols_with_penalty)[j]);
			}
		}
		int64 maxvsSize = 0;
		int64 minvsSize = INT64_MAX;
		int64 totalMaxvsSize = 0;
		int64 totalMinvsSize = INT64_MAX;
		int64 totalMaxvsSizePenalty = 0;
		int64 totalMinvsSizePenalty = INT64_MAX;
		for (int j = 0; j < assignment_context->virtual_segment_num; j++) {
			if (maxvsSize < assignment_context->vols[j]) {
				maxvsSize = assignment_context->vols[j];
			}
			if (minvsSize > assignment_context->vols[j]) {
				minvsSize = assignment_context->vols[j];
			}
			if (totalMaxvsSize < assignment_context->totalvols[j]) {
				totalMaxvsSize = assignment_context->totalvols[j];
			}
			if (totalMinvsSize > assignment_context->totalvols[j]) {
				totalMinvsSize = assignment_context->totalvols[j];
			}
			if (totalMaxvsSizePenalty < assignment_context->totalvols_with_penalty[j]) {
				totalMaxvsSizePenalty = assignment_context->totalvols_with_penalty[j];
			}
			if (totalMinvsSizePenalty > assignment_context->totalvols_with_penalty[j]) {
				totalMinvsSizePenalty = assignment_context->totalvols_with_penalty[j];
			}
//			elog(LOG, "size with penalty of vs%d is "INT64_FORMAT"",j, assignment_context->vols[j]);
//			elog(LOG, "total size of vs%d is "INT64_FORMAT"",j, assignment_context->totalvols[j]);
//			elog(LOG, "total size with penalty of vs%d is "INT64_FORMAT"",j, assignment_context->totalvols_with_penalty[j]);
		}
		elog(LOG, "avg,max,min volume of segments are"
		" %f,"INT64_FORMAT","INT64_FORMAT" for relation %u.",
		maxSizePerSegment,maxvsSize,minvsSize,myrelid);
		elog(LOG, "total max,min volume of segments are "INT64_FORMAT","INT64_FORMAT" for relation %u.",
				totalMaxvsSize,totalMinvsSize,myrelid);
		elog(LOG, "total max,min volume with penalty of segments are "INT64_FORMAT","INT64_FORMAT" for relation %u.",
						totalMaxvsSizePenalty,totalMinvsSizePenalty,myrelid);
	}

	/*we don't need to free file_vector[i]*/
	if (fileCount > 0) {
		pfree(file_vector);
	}
	pfree(isBlockContinue);
}

/*
 * remedy_non_localRead when there is a non local read block
 */
static int remedy_non_localRead(int fileIndex, int splitIndex, int parentPos,
		Relation_File** file_vector, int fileCount, int64 maxExtendedSizePerSegment,
		TargetSegmentIDMap* idMap,
		Relation_Assignment_Context* assignment_context) {

	Relation_File *cur_file = file_vector[fileIndex];
	Block_Host_Index *hostID = cur_file->hostIDs + splitIndex;

	for (int i = 0; i < fileCount; i++) {
		Relation_File *former_file = file_vector[i];
		for (int j = 0; j < former_file->split_num; j++) {
			if (former_file->splits[j].host >= 0
					&& idMap->global_IDs[former_file->splits[j].host]
							== hostID->hostIndex[parentPos]) {
				bool isDone = false;
				int orivseg = former_file->splits[j].host;
				int64 former_split_size = former_file->splits[j].length;
				// after swap with current split, the vseg should not exceed its volume
				bool isExceedMaxSizeCurSplit = cur_file->splits[splitIndex].length
						- former_split_size + assignment_context->vols[orivseg]
						> maxExtendedSizePerSegment;
				bool isExceedWholeSizeCurSplit = balance_on_whole_query_level
						&& cur_file->splits[splitIndex].length - former_split_size
								+ assignment_context->totalvols_with_penalty[orivseg]
								> assignment_context->avg_size_of_whole_query;
				// the adjusted split should be the similar size with the current split
				// and the adjusted split must also be a local read split
				if (isExceedMaxSizeCurSplit || isExceedWholeSizeCurSplit
				|| former_split_size/(double)cur_file->splits[splitIndex].length>1.1
				|| former_split_size/(double)cur_file->splits[splitIndex].length<0.9
				|| !former_file->splits[j].is_local_read) {
					continue;
				}
				for (int p = 0; p < 3; p++) {
					Block_Host_Index *formerHostID = former_file->hostIDs + j;
					if (formerHostID->hostIndex[p]
							== hostID->hostIndex[parentPos]) {
						continue;
					}

					for (int v = 0; v < assignment_context->virtual_segment_num; v++) {
						if (idMap->global_IDs[v] == formerHostID->hostIndex[p]) {
							bool isExceedMaxSize = former_split_size
									+ assignment_context->vols[v] > maxExtendedSizePerSegment;
							bool isExceedWholeSize = balance_on_whole_query_level
									&& former_split_size
											+ assignment_context->totalvols_with_penalty[v]
											> assignment_context->avg_size_of_whole_query;
							if (!isExceedWholeSize&& !isExceedMaxSize){
								//removesplit
								assignment_context->split_num[orivseg]--;
								assignment_context->vols[orivseg] -= former_split_size;
								assignment_context->totalvols[orivseg] -= former_split_size;
								assignment_context->totalvols_with_penalty[orivseg] -=
										former_split_size;
								//insertsplit
								former_file->splits[j].host = v;
								assignment_context->split_num[v]++;
								// we simply treat adjusted split as continue one
								assignment_context->continue_split_num[v]++;
								assignment_context->vols[v] += former_split_size;
								assignment_context->totalvols[v] += former_split_size;
								assignment_context->totalvols_with_penalty[v] +=
										former_split_size;
								isDone = true;
								break;
							}
						}
					}
					if (isDone) {
						return orivseg;
					}
				}
			}
		}
	}
	return -1;
}

/*
 *	 show data loaclity log for performance tuning
 */
static void print_datalocality_overall_log_information(SplitAllocResult *result, List *virtual_segments, int relationCount,
		Assignment_Log_Context *log_context, Relation_Assignment_Context* assignment_context,
		split_to_segment_mapping_context *context) {

	/*init Assignment_Log_Context*/
	//log_context->totalDataSize = 0.0;
	//log_context->datalocalityRatio = 0.0;
	log_context->maxSegmentNumofHost = 0;
	log_context->minSegmentNumofHost = INT_MAX;
	log_context->avgSegmentNumofHost = 0;
	log_context->numofDifferentHost = 0;
	log_context->avgSizeOverall = 0;
	log_context->maxSizeSegmentOverall = 0;
	log_context->minSizeSegmentOverall = INT64_MAX;
	log_context->avgSizeOverallPenalty = 0;
	log_context->maxSizeSegmentOverallPenalty = 0;
	log_context->minSizeSegmentOverallPenalty = INT64_MAX;
	log_context->avgContinuityOverall = 0;
	log_context->maxContinuityOverall = 0;
	log_context->minContinuityOverall = DBL_MAX;

	if (relationCount > 0 && log_context->totalDataSize > 0) {
		log_context->datalocalityRatio = log_context->datalocalityRatio / log_context->totalDataSize;
	}
	/*compute avgSegmentNumofHost log information for performance tuning*/
	ListCell *lc;
	foreach(lc, virtual_segments)
	{
		VirtualSegmentNode *vsn = (VirtualSegmentNode *) lfirst(lc);

		HostDataVolumeInfo *hdvInfo = search_host_in_stat_context(context,
				vsn->hostname);
		if (hdvInfo->occur_count > 0) {
			if (hdvInfo->occur_count > log_context->maxSegmentNumofHost) {
				log_context->maxSegmentNumofHost = hdvInfo->occur_count;
			}
			if (hdvInfo->occur_count < log_context->minSegmentNumofHost) {
				log_context->minSegmentNumofHost = hdvInfo->occur_count;
			}
		}
	}
	for (int k = 0; k < context->dds_context.max_size; k++) {
		if (context->dds_context.volInfos[k].occur_count > 0) {
			log_context->avgSegmentNumofHost +=
					context->dds_context.volInfos[k].occur_count;
			log_context->numofDifferentHost++;
		}
	}
	if (log_context->numofDifferentHost > 0) {
		log_context->avgSegmentNumofHost /= log_context->numofDifferentHost;
	}

	for (int j = 0; j < assignment_context->virtual_segment_num; j++) {
		if (log_context->maxSizeSegmentOverall < assignment_context->totalvols[j]) {
			log_context->maxSizeSegmentOverall = assignment_context->totalvols[j];
		}
		if (log_context->minSizeSegmentOverall > assignment_context->totalvols[j]) {
			log_context->minSizeSegmentOverall = assignment_context->totalvols[j];
		}
		log_context->avgSizeOverall += assignment_context->totalvols[j];

		if (log_context->maxSizeSegmentOverallPenalty < assignment_context->totalvols_with_penalty[j]) {
			log_context->maxSizeSegmentOverallPenalty = assignment_context->totalvols_with_penalty[j];
		}
		if (log_context->minSizeSegmentOverallPenalty > assignment_context->totalvols_with_penalty[j]) {
			log_context->minSizeSegmentOverallPenalty = assignment_context->totalvols_with_penalty[j];
		}
		log_context->avgSizeOverallPenalty += assignment_context->totalvols_with_penalty[j];

		double cur_continue_value = 0.0;
		if (assignment_context->split_num[j] > 1) {
			cur_continue_value = assignment_context->continue_split_num[j]
					/ (double) assignment_context->split_num[j];
			if(cur_continue_value > 1){
				cur_continue_value = 1;
			}
		} else if (assignment_context->split_num[j] == 1) {
			/*if there is only one split, we also consider it as continue one;*/
			cur_continue_value = 1;
		}
		if (log_context->maxContinuityOverall < cur_continue_value) {
			log_context->maxContinuityOverall = cur_continue_value;
		}
		if (log_context->minContinuityOverall > cur_continue_value) {
			log_context->minContinuityOverall = cur_continue_value;
		}
		log_context->avgContinuityOverall += cur_continue_value;
	}
	if (assignment_context->virtual_segment_num > 0) {
	  log_context->avgContinuityOverall /= assignment_context->virtual_segment_num;
	  log_context->avgSizeOverall /= assignment_context->virtual_segment_num;
	  log_context->avgSizeOverallPenalty /= assignment_context->virtual_segment_num;
	}
	/* print data locality result*/
	elog(
			LOG, "data locality ratio: %.3f; virtual segment number: %d; "
			"different host number: %d; virtual segment number per host(avg/min/max): (%d/%d/%d); "
			"segment size(avg/min/max): (%.3f B/"INT64_FORMAT" B/"INT64_FORMAT" B); "
			"segment size with penalty(avg/min/max): (%.3f B/"INT64_FORMAT" B/"INT64_FORMAT" B); continuity(avg/min/max): (%.3f/%.3f/%.3f)."
			,log_context->datalocalityRatio,assignment_context->virtual_segment_num,log_context->numofDifferentHost,
			log_context->avgSegmentNumofHost,log_context->minSegmentNumofHost,log_context->maxSegmentNumofHost,
			log_context->avgSizeOverall,log_context->minSizeSegmentOverall,log_context->maxSizeSegmentOverall,
			log_context->avgSizeOverallPenalty,log_context->minSizeSegmentOverallPenalty,log_context->maxSizeSegmentOverallPenalty,
			log_context->avgContinuityOverall,log_context->minContinuityOverall,log_context->maxContinuityOverall
			);

	appendStringInfo(result->datalocalityInfo, "data locality ratio: %.3f; virtual segment number: %d; "
			"different host number: %d; virtual segment number per host(avg/min/max): (%d/%d/%d); "
			"segment size(avg/min/max): (%.3f B/"INT64_FORMAT" B/"INT64_FORMAT" B); "
			"segment size with penalty(avg/min/max): (%.3f B/"INT64_FORMAT" B/"INT64_FORMAT" B); continuity(avg/min/max): (%.3f/%.3f/%.3f); "
			,log_context->datalocalityRatio,assignment_context->virtual_segment_num,log_context->numofDifferentHost,
			log_context->avgSegmentNumofHost,log_context->minSegmentNumofHost,log_context->maxSegmentNumofHost,
			log_context->avgSizeOverall,log_context->minSizeSegmentOverall,log_context->maxSizeSegmentOverall,
			log_context->avgSizeOverallPenalty,log_context->minSizeSegmentOverallPenalty,log_context->maxSizeSegmentOverallPenalty,
			log_context->avgContinuityOverall,log_context->minContinuityOverall,log_context->maxContinuityOverall);

	if (debug_fake_datalocality) {
			fprintf(fp, "datalocality ratio: %.3f; virtual segments number: %d, "
					"different host number: %d, segment number per host(avg/min/max): (%d/%d/%d); "
					"segments size(avg/min/max): (%.3f/"INT64_FORMAT"/"INT64_FORMAT"); "
					"segments size with penalty(avg/min/max): (%.3f/"INT64_FORMAT"/"INT64_FORMAT"); continuity(avg/min/max): (%.3f/%.3f/%.3f)."
					,log_context->datalocalityRatio,assignment_context->virtual_segment_num,log_context->numofDifferentHost,
					log_context->avgSegmentNumofHost,log_context->minSegmentNumofHost,log_context->maxSegmentNumofHost,
					log_context->avgSizeOverall,log_context->minSizeSegmentOverall,log_context->maxSizeSegmentOverall,
					log_context->avgSizeOverallPenalty,log_context->minSizeSegmentOverallPenalty,log_context->maxSizeSegmentOverallPenalty,
					log_context->avgContinuityOverall,log_context->minContinuityOverall,log_context->maxContinuityOverall
					);
			fpratio = fopen("/tmp/datalocality_ratio", "w+");
			fprintf(fpratio, "datalocality_ratio=%.3f\n",log_context->datalocalityRatio);
			fprintf(fpratio, "virtual_segments_number=%d\n",assignment_context->virtual_segment_num);
			if(log_context->minSegmentNumofHost > 0 ){
				fprintf(fpratio, "segmentnumber_perhost_max/min=%.2f\n", (double)(log_context->maxSegmentNumofHost / log_context->minSegmentNumofHost));
			}else{
				fprintf(fpratio, "segmentnumber_perhost_max/min=" INT64_FORMAT "\n", INT64_MAX);
			}
			if(log_context->avgSegmentNumofHost > 0 ){
				fprintf(fpratio, "segmentnumber_perhost_max/avg=%.2f\n", (double)(log_context->maxSegmentNumofHost / log_context->avgSegmentNumofHost));
			}else{
				fprintf(fpratio, "segmentnumber_perhost_max/avg=" INT64_FORMAT "\n", INT64_MAX);
			}

			if (log_context->minSizeSegmentOverall > 0){
				fprintf(fpratio, "segments_size_max/min=%.5f\n", (double)log_context->maxSizeSegmentOverall / (double)log_context->minSizeSegmentOverall);
			}else{
				fprintf(fpratio, "segments_size_max/min=" INT64_FORMAT "\n", INT64_MAX);
			}
			if (log_context->avgSizeOverall > 0){
				fprintf(fpratio, "segments_size_max/avg=%.5f\n", log_context->maxSizeSegmentOverall / log_context->avgSizeOverall);
			}else{
				fprintf(fpratio, "segments_size_max/avg=" INT64_FORMAT "\n", INT64_MAX);
			}

			if (log_context->minSizeSegmentOverallPenalty > 0){
				fprintf(fpratio, "segments_size_penalty_max/min=%.5f\n",(double)log_context->maxSizeSegmentOverallPenalty / (double)log_context->minSizeSegmentOverallPenalty);
			}else{
				fprintf(fpratio, "segments_size_penalty_max/min=" INT64_FORMAT "\n", INT64_MAX);
			}
			if (log_context->avgSizeOverallPenalty > 0){
				fprintf(fpratio, "segments_size_penalty_max/avg=%.5f\n",log_context->maxSizeSegmentOverallPenalty / log_context->avgSizeOverallPenalty);
			}else{
				fprintf(fpratio, "segments_size_penalty_max/avg=" INT64_FORMAT "\n", INT64_MAX);
			}

			if (log_context->minContinuityOverall > 0){
				fprintf(fpratio, "continuity_max/min=%.5f\n",log_context->maxContinuityOverall / log_context->minContinuityOverall);
			}else{
				fprintf(fpratio, "continuity_max/min=" INT64_FORMAT "\n", INT64_MAX);
			}
			if (log_context->avgContinuityOverall > 0){
				fprintf(fpratio, "continuity_max/avg=%.5f\n",log_context->maxContinuityOverall / log_context->avgContinuityOverall);
			}else{
				fprintf(fpratio, "continuity_max/avg=" INT64_FORMAT "\n", INT64_MAX);
			}
			fflush(fpratio);
			fclose(fpratio);
			fpratio = NULL;

	}
}

/*
 * check whether a relation is hash
 */
static bool is_relation_hash(GpPolicy *targetPolicy) {
	if (targetPolicy->nattrs == 0) {
		return false;
	} else {
		return true;
	}
}

/*
 * caculate_per_relation_data_locality_result
 */
static void caculate_per_relation_data_locality_result(Relation_Data* rel_data,
		Assignment_Log_Context* log_context,
		Relation_Assignment_Context* assignment_context) {
	double datalocalityRatioPerRelation = 0.0;
	/* we need to log data locality of every relation*/
	if (log_context->totalDataSizePerRelation > 0) {
		datalocalityRatioPerRelation =
				(double) log_context->localDataSizePerRelation
						/ log_context->totalDataSizePerRelation;
	}
	log_context->datalocalityRatio +=
			(double) log_context->localDataSizePerRelation;
	if (debug_print_split_alloc_result) {
		elog(
				LOG, "datalocality relation:%u relation ratio: %f with "
				INT64_FORMAT" local, "INT64_FORMAT" total, %d virtual segments",
				rel_data->relid,datalocalityRatioPerRelation,
				log_context->localDataSizePerRelation,
				log_context->totalDataSizePerRelation,
				assignment_context->virtual_segment_num);
	}
	if (debug_fake_datalocality) {
		fprintf(fp, "datalocality relation %u ratio is:%f \n", rel_data->relid,
				datalocalityRatioPerRelation);
	}
	log_context->totalDataSize += log_context->totalDataSizePerRelation;
}

/*
 *  combine all splits to Detailed_File_Split
 */
static void combine_all_splits(Detailed_File_Split **splits,
		Relation_Assignment_Context* assignment_context, TargetSegmentIDMap* idMap,
		Assignment_Log_Context* log_context,
		split_to_segment_mapping_context* context) {

	ListCell *lc;
	*splits = (Detailed_File_Split *) palloc(
			sizeof(Detailed_File_Split) * assignment_context->total_split_num);
	int total_split_index = 0;
	bool nonLocalExist = false;
	int64 splitTotalLength = 0;
	/* go through all splits again. combine all splits to Detailed_File_Split structure*/
	foreach(lc, context->chsl_context.relations)
	{
		ListCell *lc_file;
		Relation_Data *rel_data = (Relation_Data *) lfirst(lc);

		bool isRelationHash = true;
		GpPolicy *targetPolicy = NULL;
		Oid myrelid = rel_data->relid;
		targetPolicy = GpPolicyFetch(CurrentMemoryContext, myrelid);
		if (targetPolicy->nattrs == 0) {
			isRelationHash = false;
		}
		foreach(lc_file, rel_data->files)
		{
			Relation_File *rel_file = (Relation_File *) lfirst(lc_file);

			for (int i = 0; i < rel_file->split_num; i++) {
				char *p = NULL;
				/* fake data locality */
				if (debug_fake_datalocality) {
					bool isLocalRead = false;
					int localCount = 0;
					if (isRelationHash && context->keep_hash
							&& assignment_context->virtual_segment_num
									== targetPolicy->bucketnum) {
						for (int p = 0; p < rel_file->block_num; p++) {
							Block_Host_Index *hostID = rel_file->hostIDs + p;
							for (int l = 0; l < hostID->replica_num; l++) {
								if (hostID->hostIndex[l]
										== idMap->global_IDs[rel_file->splits[i].host]) {
									localCount++;
									break;
								}
							}
						}
					} else {/*if random*/
						Block_Host_Index *hostID = rel_file->hostIDs + i;
						for (int l = 0; l < hostID->replica_num; l++) {
							if (hostID->hostIndex[l]
									== idMap->global_IDs[rel_file->splits[i].host]) {
								isLocalRead = true;
								break;
							}
						}
					}
					if (localCount == rel_file->block_num || isLocalRead) {
						fprintf(fp,
								"split %d of file %d of relation %d is assigned to virtual segment No%d: Local Read .\n",
								i, rel_file->segno, rel_data->relid, rel_file->splits[i].host);
					} else if (localCount == 0 && !isLocalRead) {
						fprintf(fp,
								"split %d of file %d of relation %d is assigned to virtual segment No%d: Remote Read .\n",
								i, rel_file->segno, rel_data->relid, rel_file->splits[i].host);
					} else {
						fprintf(fp,
								"split %d of file %d of relation %d is assigned to virtual segment No%d: Local Read Ratio : %d / %d \n",
								i, rel_file->segno, rel_data->relid, rel_file->splits[i].host,
								localCount, rel_file->block_num);
					}
				}
				/*double check when datalocality is 1.0*/
				if (log_context->datalocalityRatio == 1.0 && !nonLocalExist) {
					bool isLocal = false;
					Block_Host_Index *hostID = rel_file->hostIDs + i;
					if (hostID && rel_file->splits[i].host >= 0
							&& rel_file->splits[i].host < idMap->target_segment_num) {
						for (int l = 0; l < hostID->replica_num; l++) {
							if (hostID->hostIndex[l]
									== idMap->global_IDs[rel_file->splits[i].host]) {
								isLocal = true;
								break;
							}
						}
						if (!isLocal) {
							nonLocalExist = true;
							elog(
									LOG, "datalocality is not 1.0 when split%d of file %d.",i,rel_file->segno);
						}
					}
				}
				(*splits)[total_split_index].rel_oid = rel_data->relid;
				(*splits)[total_split_index].segno = rel_file->segno;
				(*splits)[total_split_index].index = 1;
				(*splits)[total_split_index].host = rel_file->splits[i].host;
				if ((*splits)[total_split_index].host == -1) {
					(*splits)[total_split_index].host = 0;
				}
				(*splits)[total_split_index].range_id = rel_file->splits[i].range_id;
				(*splits)[total_split_index].replicaGroup_id = rel_file->splits[i].replicaGroup_id;
				(*splits)[total_split_index].offset = rel_file->splits[i].offset;
				(*splits)[total_split_index].length = rel_file->splits[i].length;
				(*splits)[total_split_index].logiceof = rel_file->logic_len;
				p = rel_file->splits[i].ext_file_uri;
				(*splits)[total_split_index].ext_file_uri_string = p ? pstrdup(p) : (char *) NULL;
				total_split_index++;
				splitTotalLength += rel_file->splits[i].length;
			}
		}
	}

	if(context->total_metadata_logic_len != splitTotalLength){
		elog(ERROR, "total split length does not equal to metadata total logic length!");
	}
}

/*
 * The driver of the allocation algorithm.
 */
static List *
run_allocation_algorithm(SplitAllocResult *result, List *virtual_segments, QueryResource ** resourcePtr,
		split_to_segment_mapping_context *context) {
	uint64_t before_run_allocation = 0;
	before_run_allocation = gettime_microsec();

	List *alloc_result = NIL;
	ListCell *lc;
	TargetSegmentIDMap idMap;
	Relation_Assignment_Context assignment_context;
	Assignment_Log_Context log_context;
	Split_Assignment_Result split_assign_result;

	Relation_Data** rel_data_vector = NULL;
	int relationCount = 0;

	MemoryContextSwitchTo(context->datalocality_memorycontext);

	/*before assign splits to virtual segments, we index virtual segments in different hosts to different hash value*/
	allocation_preparation(virtual_segments, &idMap, &assignment_context, context);

	log_context.totalDataSize = 0.0;
	log_context.datalocalityRatio = 0.0;
	/*sort relations by size.*/
	relationCount = list_length(context->chsl_context.relations);
	if (relationCount > 0) {
		rel_data_vector = (Relation_Data**) palloc(
				sizeof(Relation_Data*) * relationCount);
		int i = 0;
		foreach(lc, context->chsl_context.relations)
		{
			rel_data_vector[i++] = (Relation_Data *) lfirst(lc);
		}
		qsort(rel_data_vector, relationCount, sizeof(Relation_Data*),
				compare_relation_size);
	}

	assignment_context.patition_parent_size_map = createHASHTABLE(
				context->datalocality_memorycontext, 16,
				HASHTABLE_SLOT_VOLUME_DEFAULT_MAX, HASHTABLE_KEYTYPE_UINT32,
				NULL);
	assignment_context.partitionvols_with_penalty_map = createHASHTABLE(
					context->datalocality_memorycontext, 16,
					HASHTABLE_SLOT_VOLUME_DEFAULT_MAX, HASHTABLE_KEYTYPE_UINT32,
					NULL);
	assignment_context.partitionvols_map = createHASHTABLE(
					context->datalocality_memorycontext, 16,
					HASHTABLE_SLOT_VOLUME_DEFAULT_MAX, HASHTABLE_KEYTYPE_UINT32,
					NULL);

	Relation	inhrel;
	inhrel = heap_open(InheritsRelationId, AccessShareLock);
	cqContext  *pcqCtx;
	cqContext	cqc;
	HeapTuple	inhtup;
	/*calculate average size per vseg for all all the relation in a query
	 * and initialize the patition_parent_size_map*/
	for (int relIndex = 0; relIndex < relationCount; relIndex++) {
			Relation_Data *rel_data = rel_data_vector[relIndex];
			pcqCtx = caql_beginscan(
							caql_addrel(cqclr(&cqc), inhrel), cql("SELECT * FROM pg_inherits "
							" WHERE inhrelid = :1 ", ObjectIdGetDatum(rel_data->relid)));
		while (HeapTupleIsValid(inhtup = caql_getnext(pcqCtx))) {
			int64 block_count=0;
			if (rel_data->total_size == 0 && rel_data->files != NULL) {
				rel_data->files = NULL;
			}
			ListCell* lc_file;
			foreach(lc_file, rel_data->files)
			{
				Relation_File *rel_file = (Relation_File *) lfirst(lc_file);
				block_count += rel_file->split_num;
			}
			rel_data->block_count = block_count;

			Form_pg_inherits inh = (Form_pg_inherits) GETSTRUCT(inhtup);
			Oid inhparent = inh->inhparent;
			rel_data->partition_parent_relid = inhparent;
			uint32_t key = (uint32_t) inhparent;
			if (getHASHTABLENode(assignment_context.patition_parent_size_map,
					TYPCONVERT(void *, key)) == NULL) {
				int64 size=0;
				setHASHTABLENode(assignment_context.patition_parent_size_map,
						TYPCONVERT(void *, key),&size , false);
			}
			PAIR p = getHASHTABLENode(assignment_context.patition_parent_size_map,
					TYPCONVERT(void *, key));
			int64* val = (int64 *) (p->Value);
			*val += (int64) (rel_data->total_size * 1.05
					/ (double) assignment_context.virtual_segment_num);

			if (getHASHTABLENode(assignment_context.partitionvols_with_penalty_map,
					TYPCONVERT(void *, key)) == NULL) {
				setHASHTABLENode(assignment_context.partitionvols_with_penalty_map,
						TYPCONVERT(void *, key), NIL, false);
				p = getHASHTABLENode(assignment_context.partitionvols_with_penalty_map,
						TYPCONVERT(void *, key));
				int64** partitionvols_with_penalty = (int64 **) (&(p->Value));
				*partitionvols_with_penalty = (int64 *) palloc(
						sizeof(int64) * assignment_context.virtual_segment_num);
				MemSet(*partitionvols_with_penalty, 0,
						sizeof(int64) * assignment_context.virtual_segment_num);
			}
			if (getHASHTABLENode(assignment_context.partitionvols_map,
					TYPCONVERT(void *, key)) == NULL) {
				setHASHTABLENode(assignment_context.partitionvols_map,
						TYPCONVERT(void *, key), NIL, false);
				p = getHASHTABLENode(assignment_context.partitionvols_map,
						TYPCONVERT(void *, key));
				int64** partitionvols = (int64 **) (&(p->Value));
				*partitionvols = (int64 *) palloc(
						sizeof(int64) * assignment_context.virtual_segment_num);
				MemSet(*partitionvols, 0,
						sizeof(int64) * assignment_context.virtual_segment_num);
			}

			break;
		}
		caql_endscan(pcqCtx);
		assignment_context.avg_size_of_whole_query += rel_data->total_size;
	}
	heap_close(inhrel, AccessShareLock);
	assignment_context.avg_size_of_whole_query /= (double) assignment_context.virtual_segment_num;
	/* vseg can do more than 1.05 times workload than average.*/
	assignment_context.avg_size_of_whole_query *= 1.05;

	if (debug_print_split_alloc_result) {
			elog(LOG, "avg_size_of_whole_query is:%f",assignment_context.avg_size_of_whole_query);
	}

	int allocate_hash_or_random_time = 0;

	bool vSegOrderChanged = false;
	List* parentRelsType =NULL;
	for (int relIndex = 0; relIndex < relationCount; relIndex++) {
		log_context.localDataSizePerRelation = 0;
		log_context.totalDataSizePerRelation = 0;
		Relation_Data *rel_data = rel_data_vector[relIndex];

		/*empty file need to be skipped*/
		if (rel_data->total_size == 0 && rel_data->files != NULL) {
			rel_data->files = NULL;
		}

		/* for each relation, set context.vols to zero */
		MemSet(assignment_context.vols, 0,
				sizeof(int64) * assignment_context.virtual_segment_num);

		/* the whole query context maybe keephash but some relation is random
		 * in this case, we set isRelationHash=false when relation is random.
		 */
		Oid myrelid = rel_data->relid;
		bool isMagmaHashTable = rel_data->type == DATALOCALITY_MAGMA;

		GpPolicy *targetPolicy = NULL;
		targetPolicy = GpPolicyFetch(CurrentMemoryContext, myrelid);
		bool isRelationHash = is_relation_hash(targetPolicy);

		int fileCountInRelation = list_length(rel_data->files);
		bool FileCountBucketNumMismatch = false;
		bool isDataStoredInHdfs = false;
		if (targetPolicy->bucketnum > 0) {
		  Relation rel = heap_open(rel_data->relid, NoLock);
		  if (!RelationIsExternal(rel)) {
		    FileCountBucketNumMismatch = fileCountInRelation %
		      targetPolicy->bucketnum == 0 ? false : true;
      } else {
        // TODO(Zongtian): no mismatch in magma table
        ListCell *lc_file;
        int maxsegno = 0;
        isDataStoredInHdfs = dataStoredInHdfs(rel);
        foreach (lc_file, rel_data->files) {
          Relation_File *rel_file =
              (Relation_File *)lfirst(lc_file);
          if (rel_file->segno > maxsegno)
            maxsegno = rel_file->segno;
        }
        FileCountBucketNumMismatch =
            maxsegno > targetPolicy->bucketnum ? true : false;
      }
      heap_close(rel, NoLock);
		}
		if (isRelationHash && FileCountBucketNumMismatch && !allow_file_count_bucket_num_mismatch) {
		  elog(ERROR, "file count %d in catalog is not in proportion to the bucket "
		      "number %d of hash table with oid=%u, some data may be lost, if you "
		      "still want to continue the query by considering the table as random, set GUC "
		      "allow_file_count_bucket_num_mismatch to on and try again.",
		      fileCountInRelation, targetPolicy->bucketnum, myrelid);
		}
		/* change the virtual segment order when keep hash.
		 * order of idMap should also be changed.
		 * if file count of the table is not equal to or multiple of
		 * bucket number, we should process it as random table.
		 */
		/*each magma table need to change the vseg order, if the orc
		 * hash table conflict with magma, turn it to random*/

		if (isRelationHash && context->keep_hash
				&& (assignment_context.virtual_segment_num == targetPolicy->bucketnum || isMagmaHashTable)
				&& (!vSegOrderChanged || isMagmaHashTable)  && (!((*resourcePtr)->vsegChangedMagma)) && !FileCountBucketNumMismatch) {
			if (isDataStoredInHdfs) {
				change_hash_virtual_segments_order_orc_file(resourcePtr,
						rel_data, &assignment_context, &idMap);
			} else if (isMagmaHashTable){
				change_hash_virtual_segments_order_magma_file(resourcePtr,
						rel_data, &assignment_context, &idMap);
			} else{
				change_hash_virtual_segments_order(resourcePtr, rel_data,
						&assignment_context, &idMap);
			}
			for (int p = 0; p < idMap.target_segment_num; p++) {
				if (debug_fake_datalocality) {
					fprintf(fp, "After resort virtual segment No%d: %s\n", p,
							idMap.hostname[p]);
				}
			}
			vSegOrderChanged = true;
		}

		if (debug_print_split_alloc_result) {
			for (int w = 0; w < idMap.target_segment_num; w++) {
				elog(LOG, "After resort datalocality using segment No%d hostname: %s,hostnameid: %d"
						,w,idMap.hostname[w],idMap.global_IDs[w]);
			}
		}
		assignment_context.block_lessthan_vseg_round_robin_no =-1;
		if(rel_data->block_count > 0 && rel_data->block_count < assignment_context.virtual_segment_num){
			assignment_context.block_lessthan_vseg_round_robin_no = 0 ;
		}

		uint64_t before_run_allocate_hash_or_random = gettime_microsec();
		/*allocate hash relation*/
		if (isRelationHash) {
		  /*
		   * if file count of the table is not equal to or multiple of
		   * bucket number, we should process it as random table.
		   */
		  bool keepRelationHash =false;
		  if (!isMagmaHashTable) {
		    /* when magma table exist, use bucket number, virtual segment number and range number to
		     * determine whether no magma table should keep as hash table. Otherwise, use bucket number
		     * and virtual segment number to determine.
		     * TODO(zongtian): should set no magma table to hash when bucket number equals range number.*/
			  if (context->isMagmaTableExist) {
				  if (assignment_context.virtual_segment_num== targetPolicy->bucketnum &&
						  targetPolicy->bucketnum == context->magmaRangeNum) {
					  keepRelationHash = true;
				  } else {
					  keepRelationHash = false;
				  }
			  } else {
				  if (assignment_context.virtual_segment_num== targetPolicy->bucketnum) {
					  keepRelationHash = true;
				  } else {
					  keepRelationHash = false;
				  }
			  }
		  }
			if (context->keep_hash
			    && (keepRelationHash || isMagmaHashTable)
			    && !FileCountBucketNumMismatch) {
				ListCell* parlc;
				bool parentIsHashExist=false;
				bool parentIsHash =false;
				/*check whether relation is partition table and need to be checked as random relation*/
				if (parentRelsType != NULL) {
					foreach(parlc, parentRelsType)
					{
						CurrentRelType* prtype = (CurrentRelType *) lfirst(parlc);
						if(prtype->relid == rel_data->partition_parent_relid || prtype->relid == rel_data->relid){
							parentIsHashExist=true;
							parentIsHash = prtype->isHash;
						}
					}
				}
				bool needToChangeHash2Random = false;
				if (isMagmaHashTable) {
					allocate_hash_relation_for_magma_file(rel_data,
							&log_context, &idMap, &assignment_context, context,
							parentIsHashExist, parentIsHash);
				} else {
					needToChangeHash2Random = allocate_hash_relation(rel_data,
							&log_context, &idMap, &assignment_context, context,
							parentIsHashExist, parentIsHash);
				}
				if (!parentIsHashExist) {
					/*for partition table, whether to convert from hash to random is determined by the first partition.
					 * it doesn't need by planner, so it doesn't need to be in global memory context*/
					CurrentRelType* parentRelType = (CurrentRelType *) palloc(
							sizeof(CurrentRelType));
					parentRelType->relid = rel_data->partition_parent_relid;
					parentRelType->isHash = !needToChangeHash2Random;
					parentRelsType = lappend(parentRelsType, parentRelType);
				}
				MemoryContext cur_memorycontext;
				cur_memorycontext = MemoryContextSwitchTo(context->old_memorycontext);
				CurrentRelType* relType = (CurrentRelType *) palloc(sizeof(CurrentRelType));
				relType->relid = rel_data->relid;
				relType->range_num = 0;
				if (dataStoredInMagmaByOid(relType->relid)){
				  relType->range_num = rel_data->files->length;
				}
				if (needToChangeHash2Random) {
					relType->isHash = false;
				} else {
					relType->isHash = true;
				}
				/* if this is insert command, target table is not magma table and virtual segment number is
				 * different with range number, turn magma table to random. */
				if (dataStoredInMagmaByOid(relType->relid) && context->isTargetNoMagma
				    && assignment_context.virtual_segment_num != relType->range_num) {
				  relType->isHash = false;
				}
				result->relsType = lappend(result->relsType, relType);
				MemoryContextSwitchTo(cur_memorycontext);
				if (needToChangeHash2Random) {
					allocate_random_relation(rel_data, &log_context, &idMap, 	&assignment_context, context);
				}
			}
			/*allocate hash relation as a random relation*/
			else{
				MemoryContext cur_memorycontext;
				cur_memorycontext = MemoryContextSwitchTo(context->old_memorycontext);
				CurrentRelType* relType = (CurrentRelType *) palloc(
						sizeof(CurrentRelType));
				relType->relid = rel_data->relid;
				relType->isHash = false;
				result->relsType = lappend(result->relsType, relType);
				MemoryContextSwitchTo(cur_memorycontext);
				allocate_random_relation(rel_data, &log_context,&idMap, &assignment_context, context);
			}

		}
		/*allocate random relation*/
		else {
			allocate_random_relation(rel_data, &log_context,&idMap, &assignment_context, context);
		}
		uint64_t after_run_allocate_hash_or_random = gettime_microsec();
		allocate_hash_or_random_time = after_run_allocate_hash_or_random - before_run_allocate_hash_or_random;

		caculate_per_relation_data_locality_result(rel_data, &log_context,&assignment_context);
	}

	print_datalocality_overall_log_information(result,virtual_segments, relationCount,
			&log_context, &assignment_context, context);

	/* go through all splits again. combine all splits to Detailed_File_Split structure*/
	Detailed_File_Split *splits =NULL;
	combine_all_splits(&splits, &assignment_context, &idMap, &log_context,
			context);

	uint64_t after_run_allocation = 0;
	after_run_allocation = gettime_microsec();
	int eclaspeTime = after_run_allocation - before_run_allocation;
	if (debug_fake_datalocality) {
		fprintf(fp, "datalocality ratio is:%f\n", log_context.datalocalityRatio);
		fprintf(fp, "The time of run_allocation_algorithm is : %d us. \n", eclaspeTime);
		fprintf(fp, "The time of run_allocate_hash_or_random is : %d us. \n", allocate_hash_or_random_time);
		fflush(fp);
		fclose(fp);
		fp = NULL;
		elog(ERROR, "Abort fake data locality!");
	}
	/*
	 * sort all the splits.
	 */
	qsort(splits, assignment_context.total_split_num, sizeof(Detailed_File_Split),
			compare_detailed_file_split);

	init_split_assignment_result(&split_assign_result,
			assignment_context.virtual_segment_num);

	assign_splits_to_hosts(&split_assign_result, splits, assignment_context.total_split_num);

	MemoryContextSwitchTo(context->old_memorycontext);

	alloc_result = post_process_assign_result(&split_assign_result);

	/* set range and replica_group map for every magma relation */
	set_magma_range_vseg_map(alloc_result, virtual_segments->length);

	ListCell *relmap, *rel;
	foreach (relmap, alloc_result)
	{
		SegFileSplitMapNode *splits_map = (SegFileSplitMapNode*)lfirst(relmap);
		for (int relIndex = 0; relIndex < relationCount; relIndex++)
		{
			Relation_Data *rel_data = rel_data_vector[relIndex];
			if (rel_data->relid == splits_map->relid && rel_data->serializeSchema != NULL)
			{
				splits_map->serializeSchema = pnstrdup(rel_data->serializeSchema,
				                                       rel_data->serializeSchemaLen);
				splits_map->serializeSchemaLen = rel_data->serializeSchemaLen;
				continue;
			}
		}
	}

	if (relationCount > 0) {
		pfree(rel_data_vector);
	}

	uint64_t run_datalocality = 0;
	run_datalocality = gettime_microsec();
	int dl_overall_time = run_datalocality - before_run_allocation;

    context->cal_datalocality_time_us = dl_overall_time;

	if(debug_datalocality_time){
		elog(LOG, "datalocality overall execution time: %d us. \n", dl_overall_time);
	}

    result->datalocalityTime = (double)(context->metadata_cache_time_us + context->alloc_resource_time_us + context->cal_datalocality_time_us)/ 1000;
    appendStringInfo(result->datalocalityInfo, "DFS metadatacache: %.3f ms; resource allocation: %.3f ms; datalocality calculation: %.3f ms.",
            (double)context->metadata_cache_time_us/1000, (double)context->alloc_resource_time_us/1000, (double)context->cal_datalocality_time_us/1000);

	return alloc_result;
}

/*
 * cleanup_allocation_algorithm: free all the resources
 * used during the allocation algorithm.
 */
static void cleanup_allocation_algorithm(
		split_to_segment_mapping_context *context) {
	ListCell *lc;

	foreach(lc, context->chsl_context.relations)
	{
		Relation_Data *rel_data = (Relation_Data *) lfirst(lc);
		if ((rel_data->type == DATALOCALITY_APPENDONLY)
				|| (rel_data->type == DATALOCALITY_PARQUET)
				|| (rel_data->type == DATALOCALITY_HDFS)) {
			ListCell *lc_file;
			foreach(lc_file, rel_data->files)
			{
				Relation_File *rel_file = (Relation_File *) lfirst(lc_file);
				if (rel_file->locations != NULL) {
					free_hdfs_data_block_location(rel_file->locations,
							rel_file->block_num);
				}
			}
		}
		else if (rel_data->type == DATALOCALITY_MAGMA) {
			ListCell *lc_file;
			foreach(lc_file, rel_data->files)
			{
				Relation_File *rel_file = (Relation_File *) lfirst(lc_file);
				if (rel_file->locations != NULL) {
					free_magma_data_range_locations(rel_file->locations,
					                                      rel_file->block_num);
				}
			}
		}
	}

	if(DataLocalityMemoryContext){
	  MemoryContextResetAndDeleteChildren(DataLocalityMemoryContext);
	}

	return;
}

/*
 * udf_collector_walker: the routine to file udfs.
 */
bool udf_collector_walker(Node *node,
		udf_collector_context *context) {
	if (node == NULL) {
		return false;
	}

	if (IsA(node, Query)) {
		return query_tree_walker((Query *) node, udf_collector_walker,
				(void *) context,
				QTW_EXAMINE_RTES);
	}

	/*For Aggref, we don't consider it as udf.*/

	if(IsA(node,FuncExpr)){
		if(!IsBuildInFunction(((FuncExpr *) node)->funcid)){
			context->udf_exist = true;
		}
		return false;
	}

	return expression_tree_walker(node, udf_collector_walker,
			(void *) context);

	return false;
}

/*
 * find_udf: collect all udf, and store them into the udf_collector_context.
 */
void find_udf(Query *query, udf_collector_context *context) {

	query_tree_walker(query, udf_collector_walker, (void *) context,
	QTW_EXAMINE_RTES);

	return;
}


/*
 * calculate_planner_segment_num
 * fixedVsegNum is used by PBE, since all the execute should use the same number of vsegs.
 */
SplitAllocResult *
calculate_planner_segment_num(PlannedStmt *plannedstmt, Query *query,
		QueryResourceLife resourceLife, int fixedVsegNum) {
	SplitAllocResult *result = NULL;
	QueryResource *resource = NULL;
	List *virtual_segments = NIL;
	List *alloc_result = NIL;
	Node *planTree = plannedstmt->planTree;
	GpPolicy *intoPolicy = plannedstmt->intoPolicy;
	int sliceNum = plannedstmt->nMotionNodes + plannedstmt->nInitPlans + 1;
	split_to_segment_mapping_context context;
	context.chsl_context.relations = NIL;
	context.isMagmaTableExist = false;
	context.magmaRangeNum = -1;

	int planner_segments = 0; /*virtual segments number for explain statement */

	result = (SplitAllocResult *) palloc(sizeof(SplitAllocResult));
	result->resource = NULL;
	result->alloc_results = NIL;
	result->relsType = NIL;
	result->planner_segments = 0;
	result->datalocalityInfo = makeStringInfo();
	result->datalocalityTime = 0;
	result->hiveUrl = NULL;

	/* fake data locality */
	if (debug_fake_datalocality) {
		fp = fopen("/tmp/cdbdatalocality.result", "w+");
		if (fp == NULL) {
			elog(ERROR, "Could not open file!");
			return result;
		}
	}

	if (Gp_role != GP_ROLE_DISPATCH) {
		result->resource = NULL;
		result->alloc_results = NIL;
		result->relsType = NIL;
		result->planner_segments = 0;
		return result;
	}

	PG_TRY();
	{
		init_datalocality_memory_context();

		init_datalocality_context(plannedstmt, &context);

		collect_scan_rangetable(planTree, &(context.srtc_context));

		bool isTableFunctionExists = false;

		/*
		 * the number of virtual segments is determined by 5 factors:
		 * 1 bucket number of external table
		 * 2 whether function exists
		 * 3 bucket number of hash result relation
		 * 4 bucket number of hash "from" relation
		 * 5 data size of random "from" relation
		 */

		udf_collector_context udf_context;
		udf_context.udf_exist = false;

		find_udf(query, &udf_context);
		isTableFunctionExists = udf_context.udf_exist;

		convert_range_tables_to_oids(
		    &(context.srtc_context.range_tables),
				context.datalocality_memorycontext);

		/*
		 * check whether magma table exsit in this query
		 */
		check_magma_table_exsit(context.srtc_context.range_tables, &(context.isMagmaTableExist));

		/* set expected virtual segment number for hash table and external table*/
		/* calculate hashSegNum, externTableSegNum, resultRelationHashSegNum */
		check_keep_hash_and_external_table(&context, query, intoPolicy);

		/*Table Function VSeg Number = default_segment_number(configured in GUC) if table function exists or gpfdist exists,
		 *0 Otherwise.
		 */
		if (isTableFunctionExists) {
			context.tableFuncSegNum = GetUserDefinedFunctionVsegNum();
		}

		/* get block location and calculate relation size*/
		get_block_locations_and_calculate_table_size(&context);
		if(context.hiveUrl){
		  result->hiveUrl = pstrdup(context.hiveUrl);
		}
		/*use inherit resource*/
		if (resourceLife == QRL_INHERIT) {

			if ( SPI_IsInPrepare() && (GetActiveQueryResource() == NULL) )
			{
				resource = NULL;
			}
			else
			{
				resource = AllocateResource(resourceLife, sliceNum, 0, 0, 0, NULL, 0);
			}

			if (resource != NULL) {
				if ((context.keep_hash)
						&& (list_length(resource->segments) != context.hashSegNum)
						&& !context.isMagmaTableExist) {
					context.keep_hash = false;
				}
			}
		}

		/*allocate new resource*/
		if (((resourceLife == QRL_INHERIT) && (resource == NULL))
				|| (resourceLife == QRL_ONCE) || (resourceLife == QRL_NONE)) {
			/*generate hostname-volume pair to help RM to choose a host with
			 *maximum data locality(only when the vseg number less than host number)
			 */
			if(enable_prefer_list_to_rm){
				context.host_context.size = context.dds_context.size;
				MemoryContextSwitchTo(context.datalocality_memorycontext);
				context.host_context.hostnameVolInfos = (HostnameVolumeInfo *) palloc(
						sizeof(HostnameVolumeInfo) * context.host_context.size);
				for (int i = 0; i < context.host_context.size; i++) {
					MemSet(&(context.host_context.hostnameVolInfos[i].hostname), 0,
							HOSTNAME_MAX_LENGTH);
					strncpy(context.host_context.hostnameVolInfos[i].hostname,
							context.dds_context.volInfos[i].hashEntry->key.hostname,
							HOSTNAME_MAX_LENGTH-1);
					context.host_context.hostnameVolInfos[i].datavolume = context.dds_context.volInfos[i].datavolume;
				}
				MemoryContextSwitchTo(context.old_memorycontext);
			}else{
				context.host_context.size = 0;
				context.host_context.hostnameVolInfos = NULL;
			}

			/* determine the random table segment number by the following 4 steps*/
			/* Step1 we expect one split(block) processed by one virtual segment*/
			context.randomSegNum = context.total_split_count;
			/* Step2 combine segment when splits are with small size*/
			int64 min_split_size = min_datasize_to_combine_segment; /*default 128M*/
			min_split_size <<= 20;
			int expected_segment_num_with_minsize = (context.total_size + min_split_size - 1)
					/ min_split_size;
			if (context.randomSegNum > expected_segment_num_with_minsize) {
				context.randomSegNum = expected_segment_num_with_minsize;
			}
			/* Step3 split segment when there are tow many files (default add one more segment per 100(guc) files)*/
			int expected_segment_num_with_max_filecount = (context.total_file_count
					+ max_filecount_notto_split_segment - 1)
					/ max_filecount_notto_split_segment;
			if (context.randomSegNum < expected_segment_num_with_max_filecount) {
				context.randomSegNum = expected_segment_num_with_max_filecount;
			}
			/* Step4 we at least use one segment*/
			if (context.randomSegNum < context.minimum_segment_num) {
				context.randomSegNum = context.minimum_segment_num;
			}

			int maxExpectedNonRandomSegNum = 0;
			if (maxExpectedNonRandomSegNum < context.tableFuncSegNum)
				maxExpectedNonRandomSegNum = context.tableFuncSegNum;
			if (maxExpectedNonRandomSegNum < context.hashSegNum)
				maxExpectedNonRandomSegNum = context.hashSegNum;

			if (debug_fake_segmentnum){
				fpsegnum = fopen("/tmp/segmentnumber", "w+");
				fprintf(fpsegnum, "Default segment num : %d.\n", GetHashDistPartitionNum());
				fprintf(fpsegnum, "\n");
				fprintf(fpsegnum, "From random relation segment num : %d.\n", context.randomSegNum);
				fprintf(fpsegnum, "Result relation hash segment num : %d.\n", context.resultRelationHashSegNum);
				fprintf(fpsegnum, "\n");
				fprintf(fpsegnum, "Table  function      segment num : %d.\n", context.tableFuncSegNum);
				fprintf(fpsegnum, "Extern table         segment num : %d.\n", context.externTableForceSegNum);
				fprintf(fpsegnum, "From hash relation   segment num : %d.\n", context.hashSegNum);
				fprintf(fpsegnum, "MaxExpectedNonRandom segment num : %d.\n", maxExpectedNonRandomSegNum);
				fprintf(fpsegnum, "\n");
			}

			int minTargetSegmentNumber = 0;
			int maxTargetSegmentNumber = 0;
			/* we keep resultRelationHashSegNum in the highest priority*/
			if (context.resultRelationHashSegNum != 0) {
				if ((context.resultRelationHashSegNum < context.externTableForceSegNum
						&& context.externTableForceSegNum != 0)
						|| (context.resultRelationHashSegNum < context.externTableLocationSegNum)) {
					/* bucket number of result table must be equal to or larger than
					 * location number of external table.*/
					elog(ERROR, "bucket number of result hash table and external table should match each other");
				}
				maxTargetSegmentNumber = context.resultRelationHashSegNum;
				minTargetSegmentNumber = context.resultRelationHashSegNum;
			}
			else if(context.externTableForceSegNum > 0){
				/* location number of external table must be less than the number of virtual segments*/
				if(context.externTableForceSegNum < context.externTableLocationSegNum){
					elog(ERROR, "external table bucket number should match each other");
				}
				maxTargetSegmentNumber = context.externTableForceSegNum;
				minTargetSegmentNumber = context.externTableForceSegNum;
			}
			else if (maxExpectedNonRandomSegNum > 0) {
				if (maxExpectedNonRandomSegNum == context.hashSegNum) {
					/* in general, we keep bucket number of hash table equals to the number of virtual segments
					 * but this rule can be broken when there is a large random table in the range tables list
					 */
					context.hashSegNum =
							context.hashSegNum < context.minimum_segment_num ?
							context.minimum_segment_num : context.hashSegNum;
					double considerRandomWhenHashExistRatio = 1.5;
					/*if size of random table >1.5 *hash table, we consider relax the restriction of hash bucket number*/
					if (context.randomRelSize
							> considerRandomWhenHashExistRatio * context.hashRelSize) {
						if (context.randomSegNum < context.hashSegNum) {
							context.randomSegNum = context.hashSegNum;
						}
						maxTargetSegmentNumber = context.randomSegNum;
						minTargetSegmentNumber = context.minimum_segment_num;
					} else {
						maxTargetSegmentNumber = context.hashSegNum;
						minTargetSegmentNumber = context.hashSegNum;
					}
				} else if (maxExpectedNonRandomSegNum == context.tableFuncSegNum) {
					/* if there is a table function, we should at least use tableFuncSegNum virtual segments*/
					context.tableFuncSegNum =
							context.tableFuncSegNum < context.minimum_segment_num ?
									context.minimum_segment_num : context.tableFuncSegNum;
					if (context.randomSegNum < context.tableFuncSegNum) {
						context.randomSegNum = context.tableFuncSegNum;
					}
					maxTargetSegmentNumber = context.randomSegNum;
					minTargetSegmentNumber = context.minimum_segment_num;
				}
			} else {
				maxTargetSegmentNumber = context.randomSegNum;
				if(context.externTableLocationSegNum > 0 && maxTargetSegmentNumber < GetQueryVsegNum()){
					/*
					 * adjust max segment number for random table by rm_nvseg_perquery_perseg_limit
					 * and rm_nvseg_perquery_limit.
					 */
					maxTargetSegmentNumber = GetQueryVsegNum();
				}
				minTargetSegmentNumber = context.minimum_segment_num;
			}

			if (enforce_virtual_segment_number > 0) {
			  /*
			   * this is the last factor to determine the virtual segment number,
			   * it has the highest priority in all conditions.
			   */
				maxTargetSegmentNumber = enforce_virtual_segment_number;
				minTargetSegmentNumber = enforce_virtual_segment_number;
			}
			/* in PBE mode, the execute should use the same vseg number. */
			if(fixedVsegNum > 0 ){
				maxTargetSegmentNumber = fixedVsegNum;
				minTargetSegmentNumber = fixedVsegNum;
			}
			if(maxTargetSegmentNumber < minTargetSegmentNumber){
				maxTargetSegmentNumber = minTargetSegmentNumber;
			}
			uint64_t before_rm_allocate_resource = gettime_microsec();

			/* cost is use by RM to balance workload between hosts. the cost is at least one block size*/
			int64 mincost = min_cost_for_each_query;
			mincost <<= 20;
			int64 queryCost = context.total_size < mincost ? mincost : context.total_size;
			if (QRL_NONE != resourceLife) {

				if (SPI_IsInPrepare())
				{
					resource = NULL;
					/*
					 * prepare need to get resource quota from RM
					 * and pass quota(planner_segments) to Orca or Planner to generate plan
					 * the following executes(in PBE) should reallocate the same number
					 * of resources.
					 */
					uint32 seg_num;
					uint32 seg_num_min;
					uint32 seg_memory_mb;
					double seg_core;

					GetResourceQuota(maxTargetSegmentNumber,
					                 minTargetSegmentNumber,
					                 &seg_num,
					                 &seg_num_min,
					                 &seg_memory_mb,
					                 &seg_core);

					planner_segments = seg_num;
					minTargetSegmentNumber = planner_segments;
					maxTargetSegmentNumber = planner_segments;
				}
				else
				{
					resource = AllocateResource(QRL_ONCE, sliceNum, queryCost,
					                            maxTargetSegmentNumber,
					                            minTargetSegmentNumber,
					                            context.host_context.hostnameVolInfos,
					                            context.host_context.size);
				}
			}
			/* for explain statement, we doesn't allocate resource physically*/
			else {
				uint32 seg_num, seg_num_min, seg_memory_mb;
				double seg_core;
				GetResourceQuota(maxTargetSegmentNumber, minTargetSegmentNumber, &seg_num,
						&seg_num_min, &seg_memory_mb, &seg_core);
				planner_segments = seg_num;
			}
			uint64_t after_rm_allocate_resource = gettime_microsec();
			int eclaspeTime = after_rm_allocate_resource - before_rm_allocate_resource;

            context.alloc_resource_time_us = eclaspeTime;

			if(debug_datalocality_time){
				elog(LOG, "rm allocate resource overall execution time: %d us. \n", eclaspeTime);
			}

			if (resource == NULL) {
				result->resource = NULL;
				result->alloc_results = NIL;
				result->relsType = NIL;
				result->planner_segments = planner_segments;
				return result;
			}

			if (debug_fake_segmentnum){
				fprintf(fpsegnum, "Target segment num Min: %d.\n", minTargetSegmentNumber);
				fprintf(fpsegnum, "Target segment num Max: %d.\n", maxTargetSegmentNumber);
			}
		}

		MemoryContextSwitchTo(context.datalocality_memorycontext);

		virtual_segments = get_virtual_segments(resource);

		int VirtualSegmentNumber = list_length(virtual_segments);

		if (debug_fake_segmentnum){
			fprintf(fpsegnum, "Real   segment num    : %d.\n", VirtualSegmentNumber);
			fflush(fpsegnum);
			fclose(fpsegnum);
			fpsegnum = NULL;
			elog(ERROR, "Abort fake segment number!");
		}

		/* for normal query if containerCount equals to 0, then stop the query.*/
		if (resourceLife != QRL_NONE && VirtualSegmentNumber == 0) {
			elog(ERROR, "Could not allocate enough resource!");
		}

		MemoryContextSwitchTo(context.old_memorycontext);

		/* data locality allocation algorithm*/
		alloc_result = run_allocation_algorithm(result, virtual_segments, &resource, &context);
		fetch_magma_result_splits_from_plan(&alloc_result, plannedstmt, VirtualSegmentNumber);
		set_magma_range_vseg_map(alloc_result, VirtualSegmentNumber);

		result->resource = resource;
		result->alloc_results = alloc_result;
		result->planner_segments = list_length(resource->segments);
	}
	PG_CATCH();
	{
		cleanup_allocation_algorithm(&context);
		PG_RE_THROW();
	}
	PG_END_TRY();
	cleanup_allocation_algorithm(&context);

	if(debug_datalocality_time){
		elog(ERROR, "Abort debug metadata, datalocality, rm Time.");
	}

	return result;
}

/* set up the range to rg map for magma table by relid */
void set_magma_range_vseg_map(List *SegFileSplitMaps, int nvseg)
{
  Assert(nvseg > 0);
  if (SegFileSplitMaps == NIL) {
    // no need to set range vseg map when there isn't magma table
    return;
  }

  if (magma_range_vseg_maps == NULL) {
    HASHCTL ctl;
    ctl.keysize = sizeof(int);
    ctl.entrysize = sizeof(range_vseg_map_data);
    ctl.hcxt = TopMemoryContext;
    magma_range_vseg_maps =
        hash_create("Range and Vseg Map Hash",
                    MAX_MAGMA_RANGE_VSEG_MAP_NUM, &ctl, HASH_ELEM);
  }

  ListCell *relCell;
  bool found;
  foreach (relCell, SegFileSplitMaps) {
    SegFileSplitMapNode *splits_map = (SegFileSplitMapNode *)lfirst(relCell);

    if (!dataStoredInMagmaByOid(splits_map->relid)) {
      continue;
    }

    range_vseg_map_data *entry = (range_vseg_map_data *)hash_search(
        magma_range_vseg_maps, &nvseg, HASH_ENTER, &found);
    Assert(entry != NULL);
    if (found) {
      pfree(entry->range_vseg_map);
    }

    entry->range_num = 0;
    ListCell *fileCell = NULL;
    foreach (fileCell, splits_map->splits) {
      List *file = (List *)lfirst(fileCell);
      entry->range_num += file ? file->length : 0;
    }
    entry->range_vseg_map = (int *)MemoryContextAlloc(
        TopMemoryContext, entry->range_num * sizeof(int));

    int i = 0;
    foreach (fileCell, splits_map->splits) {
      List *file = (List *)lfirst(fileCell);

      ListCell *splitCell = NULL;
      foreach (splitCell, file) {
        FileSplit origFS = (FileSplit)lfirst(splitCell);
        entry->range_vseg_map[(origFS->range_id) & 0xFFFF] = i;
      }
      i++;
    }
    return;
  }
  /* for query without magma table, set a dummy map */
  range_vseg_map_data *entry = (range_vseg_map_data *)hash_search(
      magma_range_vseg_maps, &nvseg, HASH_ENTER, &found);
  if (!found) {
    entry->range_vseg_map = (int *)MemoryContextAlloc(
        TopMemoryContext, nvseg * sizeof(int));
    for (int i = 0 ; i < nvseg; i++) {
      entry->range_vseg_map[i] = i;
    }
    entry->range_num = nvseg;
  }
  return;
}

/* find the range and vseg map for magma table by relid */
void get_magma_range_vseg_map(int **map, int *nmap, int nvseg) {
  bool found;
  if (magma_range_vseg_maps == NULL) {
    HASHCTL ctl;
    ctl.keysize = sizeof(int);
    ctl.entrysize = sizeof(range_vseg_map_data);
    ctl.hcxt = TopMemoryContext;
    magma_range_vseg_maps =
        hash_create("Range and Vseg Map Hash",
                    MAX_MAGMA_RANGE_VSEG_MAP_NUM, &ctl, HASH_ELEM);
  }
  range_vseg_map_data *entry = (range_vseg_map_data *)hash_search(
      magma_range_vseg_maps, &nvseg, HASH_FIND, &found);
  // XXX(hzt): direct dispatch calculate motion before allocate virtual segment, workaround here.
  if (!found) {
    entry = (range_vseg_map_data *)hash_search(magma_range_vseg_maps, &nvseg,
                                               HASH_ENTER, &found);
    entry->range_num = nvseg;
    entry->range_vseg_map = (int *)MemoryContextAlloc(
        TopMemoryContext, entry->range_num * sizeof(int));
    int i;
    for (i = 0; i < nvseg; i++) {
      entry->range_vseg_map[i] = i;
    }
    entry = (range_vseg_map_data *)hash_search(magma_range_vseg_maps, &nvseg,
                                               HASH_FIND, &found);
  }
  *map = found ? entry->range_vseg_map : NULL;
  *nmap = found ? entry->range_num : 0;
}

/*get magma filesplits for insert*/
List* get_magma_scansplits(List *all_relids)
{
  List *alloc_result = NIL;
  List *virtual_segments = NIL;
  split_to_segment_mapping_context context;
  PlannedStmt plannedstmt;
  QueryResource* resource = GetActiveQueryResource();
  SplitAllocResult *result = NULL;
  result = (SplitAllocResult *) palloc(sizeof(SplitAllocResult));
  PG_TRY();
  {
    init_datalocality_memory_context();
    init_datalocality_context(&plannedstmt, &context);
    context.srtc_context.range_tables = all_relids;
    result->resource = NULL;
    result->alloc_results = NIL;
    result->relsType = NIL;
    result->planner_segments = 0;
    result->datalocalityInfo = makeStringInfo();
    result->datalocalityTime = 0;
    get_block_locations_and_calculate_table_size(&context);
    virtual_segments = get_virtual_segments(resource);
    alloc_result = run_allocation_algorithm(result, virtual_segments,
                                            &resource, &context);
  }
PG_CATCH();
{
  cleanup_allocation_algorithm(&context);
  pfree(result);
  PG_RE_THROW();
}
PG_END_TRY();
cleanup_allocation_algorithm(&context);
pfree(result);
return alloc_result;
}

void fetch_magma_result_splits_from_plan(List **alloc_result, PlannedStmt* plannedstmt, int vsegNum) {
  List *magma_oids = NIL;
  ListCell *lc_resIndex = NULL;
  foreach(lc_resIndex, plannedstmt->resultRelations) {
    int OidIndex = lfirst_int(lc_resIndex) - 1;
    RangeTblEntry *rte = list_nth(plannedstmt->rtable, OidIndex);
    Oid relOid = rte->relid;
    if (rel_is_partitioned(relOid)) {
      List *all_oids = NIL;
      all_oids = find_all_inheritors(relOid);
      ListCell *lc_oid = NULL;
      foreach (lc_oid, all_oids) {
        Oid rel_oid = lfirst_oid(lc_oid);
        if (dataStoredInMagmaByOid(rel_oid)) {
          magma_oids = lappend_oid(magma_oids, rel_oid);
        }
      }
    } else if (dataStoredInMagmaByOid(relOid)) {
      magma_oids = lappend_oid(magma_oids, relOid);
    }
  }

  ListCell *relmap = NULL;
  foreach (relmap, *alloc_result) {
    SegFileSplitMapNode *splits_map =
        (SegFileSplitMapNode *)lfirst(relmap);
    Oid relOid = splits_map->relid;
    if (list_member_oid(magma_oids, relOid)) {
      magma_oids = list_delete_oid(magma_oids, relOid);
    }
  }

  // build scansplits for magma result relation
  if (magma_oids) {
    build_magma_scansplits_for_result_relations(alloc_result, magma_oids,
                                                vsegNum);
  }
}
/*get magma filesplits for insert*/
void build_magma_scansplits_for_result_relations(List **alloc_result, List *relOids, int vsegNum)
{
  bool found = false;
  List *scansplits = NIL;
  Oid procOid = InvalidOid;
  procOid = LookupCustomProtocolBlockLocationFunc("magma");

  if (!OidIsValid(procOid)) {
    elog(ERROR, "failed to find data locality function for magma");
  }

  QueryResource* resource = GetActiveQueryResource();

  if (magma_range_vseg_maps == NULL) {
    HASHCTL ctl;
    ctl.keysize = sizeof(int);
    ctl.entrysize = sizeof(range_vseg_map_data);
    ctl.hcxt = TopMemoryContext;
    magma_range_vseg_maps =
        hash_create("Range and Vseg Map Hash",
                    MAX_MAGMA_RANGE_VSEG_MAP_NUM, &ctl, HASH_ELEM);
  }

  bool map_found = false;
  int range_num = 0;
  int *range_vseg_map = NULL;
  ListCell *relCell = NULL;
  foreach (relCell, *alloc_result) {
    SegFileSplitMapNode *splits_map = (SegFileSplitMapNode *)lfirst(relCell);

    if (!dataStoredInMagmaByOid(splits_map->relid)) {
      continue;
    }

    ListCell *fileCell = NULL;
    foreach (fileCell, splits_map->splits) {
      List *file = (List *)lfirst(fileCell);
      range_num += file ? file->length : 0;
    }
    range_vseg_map = (int *)MemoryContextAlloc(
        TopMemoryContext, range_num * sizeof(int));

    int i = 0;
    foreach (fileCell, splits_map->splits) {
      List *file = (List *)lfirst(fileCell);

      ListCell *splitCell = NULL;
      foreach (splitCell, file) {
        FileSplit origFS = (FileSplit)lfirst(splitCell);
        range_vseg_map[(origFS->range_id) & 0xFFFF] = i;
      }
      i++;
    }
    map_found = true;
    break;
  }

  ListCell *lc = NULL;
  foreach(lc, relOids) {
    Oid relid = lfirst_oid(lc);
    Relation relation = relation_open(relid, AccessShareLock);
    ExtTableEntry *ext_entry = GetExtTableEntry(relid);
    char *dbname = get_database_name(MyDatabaseId);
    char *schemaname = get_namespace_name(relation->rd_rel->relnamespace);
    char *tablename = NameStr(relation->rd_rel->relname);
    relation_close(relation, AccessShareLock);
    // get range location from magma now
    // start transaction in magma for SELECT/INSERT/UPDATE/DELETE/ANALYZE
    if (PlugStorageGetTransactionStatus() == PS_TXN_STS_DEFAULT)
    {
      PlugStorageStartTransaction();
    }
    PlugStorageGetTransactionId(NULL);
    Assert(PlugStorageGetTransactionStatus() == PS_TXN_STS_STARTED);

    ExtProtocolBlockLocationData *bldata = NULL;
    InvokeMagmaProtocolBlockLocation(
        ext_entry, procOid, dbname, schemaname, tablename,
        PlugStorageGetTransactionSnapshot(NULL), false, &bldata);

    pfree(dbname);
    pfree(schemaname);

    FileSplit fileSplit = makeNode(FileSplitNode);
    SegFileSplitMapNode *splits_map = makeNode(SegFileSplitMapNode);
    splits_map->relid = relid;
    splits_map->serializeSchema = pnstrdup(bldata->serializeSchema, bldata->serializeSchemaLen);
    splits_map->serializeSchemaLen = bldata->serializeSchemaLen;
    splits_map->splits = NIL;

    for (int i = 0; i < vsegNum; i++) {
      splits_map->splits = lappend(splits_map->splits, NIL);
    }

    int segno = 1;
    ListCell *blc = NULL;
    ListCell *per_seg_splits = NULL;
    foreach(blc, bldata->files)
    {
      blocklocation_file *f = (blocklocation_file *)lfirst(blc);
      BlockLocation *locations = f->locations;
      int splitnum = f->block_num;

      for (int i = 0; i < splitnum; i++) {
        FileSplit fileSplit = makeNode(FileSplitNode);
        fileSplit->segno = segno++;
        fileSplit->lengths = locations[i].length;
        char *p = locations[i].topologyPaths[0];
        fileSplit->ext_file_uri_string = p ? pstrdup(p) : (char *) NULL;
        fileSplit->range_id = locations[i].rangeId;
        fileSplit->replicaGroup_id = locations[i].replicaGroupId;
        fileSplit->offsets = 0;
        fileSplit->logiceof = 0;
        int vsegIndex = map_found ? (range_vseg_map[fileSplit->range_id]) : (fileSplit->range_id % vsegNum);
        per_seg_splits = list_nth_cell((List *) (splits_map->splits), vsegIndex);
        lfirst(per_seg_splits) = lappend((List *) lfirst(per_seg_splits),
            fileSplit);
      }
    }
    *alloc_result = lappend(*alloc_result, splits_map);
  }
  return;
}

static void init_host_ip_memory_context(void) {
  if (HostNameGlobalMemoryContext == NULL) {
    HostNameGlobalMemoryContext = AllocSetContextCreate(
        TopMemoryContext, "HostNameGlobalMemoryContext",
        ALLOCSET_DEFAULT_MINSIZE, ALLOCSET_DEFAULT_INITSIZE,
        ALLOCSET_DEFAULT_MAXSIZE);
  } else {
    MemoryContextResetAndDeleteChildren(HostNameGlobalMemoryContext);
  }
}

static void getHostNameByIp(const char *ipaddr, char *hostname) {
  struct sockaddr_in sa; /* input */
  socklen_t len;         /* input */

  memset(&sa, 0, sizeof(struct sockaddr_in));

  /* For IPv4*/
  sa.sin_family = AF_INET;
  sa.sin_addr.s_addr = inet_addr(ipaddr);
  len = sizeof(struct sockaddr_in);

  if (getnameinfo((struct sockaddr *)&sa, len, hostname, HOSTNAME_MAX_LENGTH,
                  NULL, 0, NI_NAMEREQD)) {
    elog(LOG, "could not resolve hostname\n");
  }
}

char *search_hostname_by_ipaddr(const char *ipaddr) {
  if (magma_ip_hostname_map == NULL) {
    init_host_ip_memory_context();

    HASHCTL ctl;

    ctl.keysize = sizeof(HostnameIpKey);
    ctl.entrysize = sizeof(HostnameIpEntry);
    ctl.hcxt = HostNameGlobalMemoryContext;
    magma_ip_hostname_map =
        hash_create("Hostname Ip Map Hash", 16, &ctl, HASH_ELEM);
  }

  HostnameIpKey key;
  HostnameIpEntry *entry = NULL;
  bool found = false;

  MemSet(key.hostip, 0, HOSTIP_MAX_LENGTH);
  strncpy(key.hostip, ipaddr, HOSTIP_MAX_LENGTH - 1);

  entry = (HostnameIpEntry *)hash_search(magma_ip_hostname_map, &key,
                                         HASH_ENTER, &found);
  Assert(entry != NULL);

  if (!found) {
    MemSet(entry->hostname, 0, HOSTNAME_MAX_LENGTH);
    getHostNameByIp(ipaddr, entry->hostname);
  }
  return entry->hostname;
}
