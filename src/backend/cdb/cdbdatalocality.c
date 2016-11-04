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
#include "access/parquetsegfiles.h"
#include "catalog/catalog.h"
#include "catalog/catquery.h"
#include "catalog/pg_exttable.h"
#include "catalog/pg_inherits.h"
#include "catalog/pg_proc.h"
#include "cdb/cdbdatalocality.h"
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
#include "parser/parsetree.h"
#include "storage/fd.h"
#include "postmaster/identity.h"
#include "cdb/cdbmetadatacache.h"
#include "resourcemanager/utils/network_utils.h"
#include "access/skey.h"
#include "utils/fmgroids.h"
#include "utils/uri.h"
#include "catalog/pg_proc.h"
#include "postgres.h"
#include "resourcemanager/utils/hashtable.h"

/* We need to build a mapping from host name to host index */

extern bool		optimizer; /* Enable the optimizer */

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
typedef struct range_table_collector_context {
	List *range_tables; /* tables without result relation(insert into etc) */
	List *full_range_tables; /* every table include result relation  */
} range_table_collector_context;

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
	int64 offset;
	int64 length;
	int64 logiceof;
	int host;
	bool is_local_read;
} File_Split;

typedef enum DATALOCALITY_RELATION_TYPE {
	DATALOCALITY_APPENDONLY, DATALOCALITY_PARQUET, DATALOCALITY_UNKNOWN
} DATALOCALITY_RELATION_TYPE;

/*
 * structure for detailed file split.
 */
typedef struct Detailed_File_Split {
	Oid rel_oid;
	int segno; // file name suffix
	int index;
	int host;
	int64 logiceof;
	int64 offset;
	int64 length;
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
	int target_segment_num;
	int *global_IDs;
	char** hostname;
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
	range_table_collector_context rtc_context;
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
} split_to_segment_mapping_context;

typedef struct vseg_list{
	List* vsegList;
}vseg_list;

static MemoryContext DataLocalityMemoryContext = NULL;

static void init_datalocality_memory_context(void);

static void init_split_assignment_result(Split_Assignment_Result *result,
		int host_num);

static void init_datalocality_context(split_to_segment_mapping_context *context);

static bool range_table_collector_walker(Node *node,
		range_table_collector_context *context);

static void collect_range_tables(Query *query, List* full_range_table,
		range_table_collector_context *context);

static void convert_range_tables_to_oids_and_check_table_functions(List **range_tables, bool* isUDFExists,
		MemoryContext my_memorycontext);

static void check_keep_hash_and_external_table(
		split_to_segment_mapping_context *collector_context, Query *query,
		GpPolicy *intoPolicy);

static int64 get_block_locations_and_claculte_table_size(
		split_to_segment_mapping_context *collector_context);

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
		AppendOnlyEntry *aoEntry, Snapshot metadataSnapshot,
		split_to_segment_mapping_context *context, int64 splitsize,
		Relation_Data *rel_data, int* hitblocks,
		int* allblocks, GpPolicy *targetPolicy);

static BlockLocation *fetch_hdfs_data_block_location(char *filepath, int64 len,
		int *block_num, RelFileNode rnode, uint32_t segno, double* hit_ratio);

static void free_hdfs_data_block_location(BlockLocation *locations,
		int block_num);

static Block_Host_Index * update_data_dist_stat(
		split_to_segment_mapping_context *context, BlockLocation *locations,
		int block_num);

static HostDataVolumeInfo *search_host_in_stat_context(
		split_to_segment_mapping_context *context, char *hostname);

static bool IsBuildInFunction(Oid funcOid);

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

static bool is_relation_hash(GpPolicy *targetPolicy);

static void allocation_preparation(List *hosts, TargetSegmentIDMap* idMap,
		Relation_Assignment_Context* assignment_context,
		split_to_segment_mapping_context *context);

static Relation_File** change_file_order_based_on_continuity(
		Relation_Data *rel_data, TargetSegmentIDMap* idMap, int host_num,
		int* fileCount, Relation_Assignment_Context *assignment_context);

static int64 set_maximum_segment_volume_parameter(Relation_Data *rel_data,
		int host_num, double* maxSizePerSegment);

/*
 * saveQueryResourceParameters: save QueryResourceParameters
 * in prepare statement along with query plan so that the query
 * resource can be re-allocated during multiple executions of
 * the plan
 */
void saveQueryResourceParameters(
		QueryResourceParameters	*resource_parameters,
		QueryResourceLife       life,
		int32                   slice_size,
		int64_t                 iobytes,
		int                     max_target_segment_num,
		int                     min_target_segment_num,
		HostnameVolumeInfo      *vol_info,
		int                     vol_info_size)

{
	resource_parameters->life = life;
	resource_parameters->slice_size = slice_size;
	resource_parameters->iobytes = iobytes;
	resource_parameters->max_target_segment_num = max_target_segment_num;
	resource_parameters->min_target_segment_num = min_target_segment_num;
	resource_parameters->vol_info = vol_info;
	resource_parameters->vol_info_size = vol_info_size;
}

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

static void init_datalocality_context(split_to_segment_mapping_context *context) {
	context->old_memorycontext = CurrentMemoryContext;
	context->datalocality_memorycontext = DataLocalityMemoryContext;

	context->chsl_context.relations = NIL;
	context->rtc_context.range_tables = NIL;
	context->rtc_context.full_range_tables = NIL;

	context->externTableForceSegNum = 0;
	context->externTableLocationSegNum = 0;
	context->tableFuncSegNum = 0;
	context->hashSegNum = 0;
	context->resultRelationHashSegNum = 0;
	context->randomSegNum = 0;
	context->randomRelSize = 0;
	context->hashRelSize = 0;
	context->minimum_segment_num = 1;
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
		context->hostname_map = hash_create("Hostname Index Map Hash", 16, &ctl,
		HASH_ELEM);
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

/*
 * range_table_collector_walker: the routine to collect all range table relations.
 */
static bool range_table_collector_walker(Node *node,
		range_table_collector_context *context) {
	if (node == NULL) {
		return false;
	}

	if (IsA(node, Query)) {
		return query_tree_walker((Query *) node, range_table_collector_walker,
				(void *) context,
				QTW_EXAMINE_RTES);
	}

	if (IsA(node, RangeTblEntry)) {
		if (((RangeTblEntry *) node)->rtekind == RTE_RELATION) {
			context->range_tables = lappend(context->range_tables, node);
		}

		return false;
	}

	return expression_tree_walker(node, range_table_collector_walker,
			(void *) context);
}

/*
 * collect_range_tables: collect all range table relations, and store
 * them into the range_table_collector_context.
 */
static void collect_range_tables(Query *query, List* full_range_table,
		range_table_collector_context *context) {

	query_tree_walker(query, range_table_collector_walker, (void *) context,
	QTW_EXAMINE_RTES);
	if (query->resultRelation > 0) {
		RangeTblEntry* resultRte = rt_fetch(query->resultRelation, query->rtable);
		ListCell *lc;
		List *new_range_tables = NIL;
		bool isFound = false;
		foreach(lc, context->range_tables)
		{
			RangeTblEntry *entry = (RangeTblEntry *) lfirst(lc);
			if (resultRte->relid == entry->relid && !isFound) {
				isFound = true;
			} else {
				new_range_tables = lappend(new_range_tables, entry);
			}
		}
		context->range_tables = new_range_tables;
	}
	context->full_range_tables = full_range_table;
	return;
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
static void convert_range_tables_to_oids_and_check_table_functions(List **range_tables, bool* isTableFunctionExists,
		MemoryContext my_memorycontext) {
	List *new_range_tables = NIL;
	ListCell *old_lc;
	MemoryContext old_memorycontext;

	old_memorycontext = MemoryContextSwitchTo(my_memorycontext);
	foreach(old_lc, *range_tables)
	{
		RangeTblEntry *entry = (RangeTblEntry *) lfirst(old_lc);
		if (entry->rtekind != RTE_RELATION) {
			continue;
		}
		Oid rel_oid = entry->relid;
		List *children = NIL;
		ListCell *child;

		children = find_all_inheritors(rel_oid);
		foreach(child, children)
		{
			Oid myrelid = lfirst_oid(child);
			ListCell *new_lc;
			bool found = false;
			foreach(new_lc, new_range_tables)
			{
				Oid relid = lfirst_oid(new_lc);
				if (myrelid == relid) {
					found = true;
					break;
				}
			}
			if (!found) {
				new_range_tables = lappend_oid(new_range_tables, myrelid);
			}
		}
	}
	MemoryContextSwitchTo(old_memorycontext);

	*range_tables = new_range_tables;

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

	if (query->resultRelation != 0) /* This is a insert command */
	{
		GpPolicy *targetPolicy = NULL;
		RangeTblEntry *rte = rt_fetch(query->resultRelation, query->rtable);
		Assert(rte->rtekind == RTE_RELATION);
		targetPolicy = GpPolicyFetch(CurrentMemoryContext, rte->relid);
		if (targetPolicy->nattrs > 0) /* distributed by table */
		{
			context->keep_hash = true;
			context->resultRelationHashSegNum = targetPolicy->bucketnum;
		}
		pfree(targetPolicy);
	}
	/*
	 * This is a CREATE TABLE AS statement
	 * SELECT * INTO newtable from origintable would create a random table default. Not hash table previously.
	 */
	if ((intoPolicy != NULL) && (intoPolicy->nattrs > 0))
	{
		context->keep_hash = true;
		context->resultRelationHashSegNum = intoPolicy->bucketnum;
	}

	foreach(lc, context->rtc_context.range_tables)
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
 * get_block_locations_and_claculte_table_size: the HDFS block information
 * corresponding to the required relations, and calculate relation size
 */
int64 get_block_locations_and_claculte_table_size(split_to_segment_mapping_context *context) {
	uint64_t allRelationFetchBegintime = 0;
	uint64_t allRelationFetchLeavetime = 0;
	int totalFileCount = 0;
	int hitblocks = 0;
	int allblocks = 0;
	allRelationFetchBegintime = gettime_microsec();
	ListCell *lc;
	int64 total_size = 0;
	Snapshot saveActiveSnapshot = ActiveSnapshot;

	MemoryContextSwitchTo(context->datalocality_memorycontext);

	if (ActiveSnapshot == NULL)
	{
		ActiveSnapshot = GetTransactionSnapshot();
	}
	ActiveSnapshot = CopySnapshot(ActiveSnapshot);
	ActiveSnapshot->curcid = GetCurrentCommandId();

	foreach(lc, context->rtc_context.full_range_tables)
	{
		Oid rel_oid = lfirst_oid(lc);
		Relation rel = relation_open(rel_oid, AccessShareLock);

		/*
		 * We only consider the data stored in HDFS.
		 */
		if (RelationIsAoRows(rel) || RelationIsParquet(rel)) {
			Relation_Data *rel_data = NULL;
			/*
			 * Get pg_appendonly information for this table.
			 */
			AppendOnlyEntry *aoEntry = GetAppendOnlyEntry(rel_oid, SnapshotNow);

			rel_data = (Relation_Data *) palloc(sizeof(Relation_Data));
			rel_data->relid = rel_oid;
			rel_data->files = NIL;
			rel_data->partition_parent_relid = 0;
			rel_data->block_count = 0;

			GpPolicy *targetPolicy = NULL;
			targetPolicy = GpPolicyFetch(CurrentMemoryContext, rel_oid);
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
				rel_data->type = DATALOCALITY_PARQUET;
				ParquetGetSegFileDataLocation(rel, aoEntry, ActiveSnapshot, context,
						context->split_size, rel_data, &hitblocks,
						&allblocks, targetPolicy);
			}

			bool isResultRelation = true;
			ListCell *nonResultlc;
			foreach(nonResultlc, context->rtc_context.range_tables)
			{
				Oid nonResultRel_oid = lfirst_oid(nonResultlc);
				if (rel_oid == nonResultRel_oid) {
					isResultRelation = false;
				}
			}
			if (!isResultRelation) {
				total_size += rel_data->total_size;
				totalFileCount += list_length(rel_data->files);
				//for hash relation
				if (targetPolicy->nattrs > 0) {
					context->hashRelSize += rel_data->total_size;
				} else {
					context->randomRelSize += rel_data->total_size;
				}
			}
			context->chsl_context.relations = lappend(context->chsl_context.relations,
					rel_data);
			pfree(targetPolicy);
		}

		relation_close(rel, AccessShareLock);
	}

	MemoryContextSwitchTo(context->old_memorycontext);

	ActiveSnapshot = saveActiveSnapshot;

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

	MemSet(&(key.hostname), 0, HOSTNAME_MAX_LENGTH);
	strncpy(key.hostname, hostname, HOSTNAME_MAX_LENGTH - 1);

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

	return context->dds_context.volInfos + entry->index;
}

/*
 * fetch_hdfs_data_block_location: given a HDFS file path,
 * collect all its data block location information.
 */
static BlockLocation *
fetch_hdfs_data_block_location(char *filepath, int64 len, int *block_num,
		RelFileNode rnode, uint32_t segno, double* hit_ratio) {
	// for fakse test, the len of file always be zero
	if(len == 0  && !debug_fake_datalocality){
		*hit_ratio = 0.0;
		return NULL;
	}
	BlockLocation *locations;
	HdfsFileInfo *file_info;
	//double hit_ratio;
	uint64_t beginTime;
	beginTime = gettime_microsec();

	if (metadata_cache_enable) {
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
		locations = HdfsGetFileBlockLocations(filepath, len, block_num);
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
			char *hostname = pstrdup(locations[i].hosts[j]); /* locations[i].hosts[j]; */
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
						&block_num, relation->rd_node, segno, &hit_ratio);
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
						&block_num, relation->rd_node, segno, &hit_ratio);
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
				split->offset = 0;
				split->length = logic_len;
				split->host = -1;
				split->is_local_read = true;
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
						&block_num, relation->rd_node, segno, &hit_ratio);
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
						&block_num, relation->rd_node, segno, &hit_ratio);
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
				split->offset = 0;
				split->length = logic_len;
				split->host = -1;
				split->is_local_read = true;
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

/*
 * ParquetGetSegFileDataLocation: fetch the data location of the
 * segment files of the Parquet relation.
 */
static void ParquetGetSegFileDataLocation(Relation relation,
		AppendOnlyEntry *aoEntry, Snapshot metadataSnapshot,
		split_to_segment_mapping_context *context, int64 splitsize,
		Relation_Data *rel_data, int* hitblocks,
		int* allblocks, GpPolicy *targetPolicy) {
	char *basepath;
	char *segfile_path;
	int filepath_maxlen;
	int64 total_size = 0;

	Relation pg_parquetseg_rel;
	TupleDesc pg_parquetseg_dsc;
	HeapTuple tuple;
	SysScanDesc parquetscan;

	basepath = relpath(relation->rd_node);
	filepath_maxlen = strlen(basepath) + 9;
	segfile_path = (char *) palloc0(filepath_maxlen);

	pg_parquetseg_rel = heap_open(aoEntry->segrelid, AccessShareLock);
	pg_parquetseg_dsc = RelationGetDescr(pg_parquetseg_rel);
	parquetscan = systable_beginscan(pg_parquetseg_rel, InvalidOid, FALSE,
			metadataSnapshot, 0, NULL);

	while (HeapTupleIsValid(tuple = systable_getnext(parquetscan))) {
		BlockLocation *locations;
		int block_num = 0;
		Relation_File *file;

		int segno = DatumGetInt32(
				fastgetattr(tuple, Anum_pg_parquetseg_segno, pg_parquetseg_dsc, NULL));
		int64 logic_len = (int64) DatumGetFloat8(
				fastgetattr(tuple, Anum_pg_parquetseg_eof, pg_parquetseg_dsc, NULL));
		context->total_metadata_logic_len += logic_len;
		bool isRelationHash = true;
		if (targetPolicy->nattrs == 0) {
			isRelationHash = false;
		}

		if (!context->keep_hash || !isRelationHash) {
			FormatAOSegmentFileName(basepath, segno, -1, 0, &segno, segfile_path);
			double hit_ratio = 0.0;
			locations = fetch_hdfs_data_block_location(segfile_path, logic_len,
					&block_num, relation->rd_node, segno, &hit_ratio);
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
					splits[realSplitNum].offset = offset;
					splits[realSplitNum].length = file->locations[realSplitNum].length;
					splits[realSplitNum].logiceof = logic_len;
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
			FormatAOSegmentFileName(basepath, segno, -1, 0, &segno, segfile_path);
			double hit_ratio = 0.0;
			locations = fetch_hdfs_data_block_location(segfile_path, logic_len,
					&block_num, relation->rd_node, segno, &hit_ratio);
			*allblocks += block_num;
			*hitblocks += block_num * hit_ratio;
			File_Split *split = (File_Split *) palloc(sizeof(File_Split));
			file = (Relation_File *) palloc0(sizeof(Relation_File));
			file->segno = segno;
			split->offset = 0;
			split->length = logic_len;
			split->logiceof = logic_len;
			split->host = -1;
			split->is_local_read = true;
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
			&& (last_split->offset + last_split->length == split->offset)) {
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
			fileSplit->offsets = split->offset;
			fileSplit->lengths = split->length;

			j += 1;

			if (empty_seg) {
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
		for (i = 0; i < assignment_context.virtual_segment_num; i++) {
			int largestFileIndex = i;
			int maxLogicLen = -1;
			for (int j = 0; j < filesPerSegment; j++) {
				if (rel_file_vector[i + j * assignment_context.virtual_segment_num]->logic_len
						> maxLogicLen) {
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
		(*resourcePtr)->segments = NIL;
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
			(*resourcePtr)->segments = lappend((*resourcePtr)->segments,
					segmentsVector[sfPairVector[p].segmentid]);
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
			for (int i = 0; i < rel_file->split_num; i++) {
				int64 split_size = rel_file->splits[i].length;
				int targethost = (rel_file->segno - 1) % (assignment_context->virtual_segment_num);
				/*calculate keephash datalocality*/
				/*for keep hash one file corresponds to one split*/
				for (int p = 0; p < rel_file->block_num; p++) {
					bool islocal = false;
					Block_Host_Index *hostID = rel_file->hostIDs + p;
					for (int l = 0; l < hostID->replica_num; l++) {
						if (debug_print_split_alloc_result) {
							elog(LOG, "file id is %d; vd id is %d",hostID->hostIndex[l],idMap->global_IDs[targethost]);
						}
						if (hostID->hostIndex[l] == idMap->global_IDs[targethost]) {
							log_context->localDataSizePerRelation +=
									rel_file->locations[p].length;
							islocal = true;
							break;
						}
					}
					if (debug_print_split_alloc_result && !islocal) {
						elog(LOG, "non local relation %u, file: %d, block: %d",myrelid,rel_file->segno,p);
					}
				}
				log_context->totalDataSizePerRelation += split_size;
			}
	}
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
							elog(LOG, "local2 split %d offset "INT64_FORMAT" of file %d is assigned to host %d",r,rel_file->splits[r].offset, rel_file->segno,assignedVSeg);
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
			elog(LOG, "size with penalty of vs%d is "INT64_FORMAT"",j, assignment_context->vols[j]);
			elog(LOG, "total size of vs%d is "INT64_FORMAT"",j, assignment_context->totalvols[j]);
			elog(LOG, "total size with penalty of vs%d is "INT64_FORMAT"",j, assignment_context->totalvols_with_penalty[j]);
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
				fprintf(fpratio, "segmentnumber_perhost_max/min=%lld\n", INT64_MAX);
			}
			if(log_context->avgSegmentNumofHost > 0 ){
				fprintf(fpratio, "segmentnumber_perhost_max/avg=%.2f\n", (double)(log_context->maxSegmentNumofHost / log_context->avgSegmentNumofHost));
			}else{
				fprintf(fpratio, "segmentnumber_perhost_max/avg=%lld\n", INT64_MAX);
			}

			if (log_context->minSizeSegmentOverall > 0){
				fprintf(fpratio, "segments_size_max/min=%.5f\n", (double)log_context->maxSizeSegmentOverall / (double)log_context->minSizeSegmentOverall);
			}else{
				fprintf(fpratio, "segments_size_max/min=%lld\n", INT64_MAX);
			}
			if (log_context->avgSizeOverall > 0){
				fprintf(fpratio, "segments_size_max/avg=%.5f\n", log_context->maxSizeSegmentOverall / log_context->avgSizeOverall);
			}else{
				fprintf(fpratio, "segments_size_max/avg=%lld\n", INT64_MAX);
			}

			if (log_context->minSizeSegmentOverallPenalty > 0){
				fprintf(fpratio, "segments_size_penalty_max/min=%.5f\n",(double)log_context->maxSizeSegmentOverallPenalty / (double)log_context->minSizeSegmentOverallPenalty);
			}else{
				fprintf(fpratio, "segments_size_penalty_max/min=%lld\n", INT64_MAX);
			}
			if (log_context->avgSizeOverallPenalty > 0){
				fprintf(fpratio, "segments_size_penalty_max/avg=%.5f\n",log_context->maxSizeSegmentOverallPenalty / log_context->avgSizeOverallPenalty);
			}else{
				fprintf(fpratio, "segments_size_penalty_max/avg=%lld\n", INT64_MAX);
			}

			if (log_context->minContinuityOverall > 0){
				fprintf(fpratio, "continuity_max/min=%.5f\n",log_context->maxContinuityOverall / log_context->minContinuityOverall);
			}else{
				fprintf(fpratio, "continuity_max/min=%lld\n", INT64_MAX);
			}
			if (log_context->avgContinuityOverall > 0){
				fprintf(fpratio, "continuity_max/avg=%.5f\n",log_context->maxContinuityOverall / log_context->avgContinuityOverall);
			}else{
				fprintf(fpratio, "continuity_max/avg=%lld\n", INT64_MAX);
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
				LOG, "datalocality relation:%u relation ratio: %f with %d virtual segments",
				rel_data->relid,datalocalityRatioPerRelation,assignment_context->virtual_segment_num);
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
				(*splits)[total_split_index].offset = rel_file->splits[i].offset;
				(*splits)[total_split_index].length = rel_file->splits[i].length;
				(*splits)[total_split_index].logiceof = rel_file->logic_len;
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
		GpPolicy *targetPolicy = NULL;
		targetPolicy = GpPolicyFetch(CurrentMemoryContext, myrelid);
		bool isRelationHash = is_relation_hash(targetPolicy);

		int fileCountInRelation = list_length(rel_data->files);
		bool FileCountBucketNumMismatch = false;
		if (targetPolicy->bucketnum > 0) {
		  FileCountBucketNumMismatch = fileCountInRelation %
		    targetPolicy->bucketnum == 0 ? false : true;
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
		if (isRelationHash && context->keep_hash
				&& assignment_context.virtual_segment_num == targetPolicy->bucketnum
				&& !vSegOrderChanged && !FileCountBucketNumMismatch) {
			change_hash_virtual_segments_order(resourcePtr, rel_data,
					&assignment_context, &idMap);
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
			if (context->keep_hash
			    && assignment_context.virtual_segment_num== targetPolicy->bucketnum
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
				needToChangeHash2Random = allocate_hash_relation(rel_data,
											&log_context, &idMap, &assignment_context, context, parentIsHashExist,parentIsHash);
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
				if (needToChangeHash2Random) {
					relType->isHash = false;
				} else {
					relType->isHash = true;
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



	if (relationCount > 0) {
		pfree(rel_data_vector);
	}

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
				|| (rel_data->type == DATALOCALITY_PARQUET)) {
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
calculate_planner_segment_num(Query *query, QueryResourceLife resourceLife,
		List *fullRangeTable, GpPolicy *intoPolicy, int sliceNum, int fixedVsegNum) {
	SplitAllocResult *result = NULL;
	QueryResource *resource = NULL;
	QueryResourceParameters *resource_parameters = NULL;

	List *virtual_segments = NIL;
	List *alloc_result = NIL;
	split_to_segment_mapping_context context;

	int planner_segments = 0; /*virtual segments number for explain statement */

	result = (SplitAllocResult *) palloc(sizeof(SplitAllocResult));
	result->resource = NULL;
	result->resource_parameters = NULL;
	result->alloc_results = NIL;
	result->relsType = NIL;
	result->planner_segments = 0;
	result->datalocalityInfo = makeStringInfo();
    result->datalocalityTime = 0;

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
		result->resource_parameters = NULL;
		result->alloc_results = NIL;
		result->relsType = NIL;
		result->planner_segments = 0;
		return result;
	}

	PG_TRY();
	{
		init_datalocality_memory_context();

		init_datalocality_context(&context);

		resource_parameters = makeNode(QueryResourceParameters);

		collect_range_tables(query, fullRangeTable, &(context.rtc_context));

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
		/*convert range table list to oid list and check whether table function exists
		 *we keep a full range table list and a range table list without result relation separately
		 */
		convert_range_tables_to_oids_and_check_table_functions(
				&(context.rtc_context.full_range_tables), &isTableFunctionExists,
				context.datalocality_memorycontext);
		convert_range_tables_to_oids_and_check_table_functions(
				&(context.rtc_context.range_tables), &isTableFunctionExists,
				context.datalocality_memorycontext);

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
		get_block_locations_and_claculte_table_size(&context);

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

			saveQueryResourceParameters(
							resource_parameters,  /* resource_parameters */
							resourceLife,         /* life */
							sliceNum,             /* slice_size */
							0,                    /* iobytes */
							0,                    /* max_target_segment_num */
							0,                    /* min_target_segment_num */
							NULL,                 /* vol_info */
							0                     /* vol_info_size */
							);

			if (resource != NULL) {
				if ((context.keep_hash)
						&& (list_length(resource->segments) != context.hashSegNum)) {
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
					cleanup_allocation_algorithm(&context);
					elog(ERROR, "Could not allocate enough memory! "
							"bucket number of result hash table and external table should match each other");
				}
				maxTargetSegmentNumber = context.resultRelationHashSegNum;
				minTargetSegmentNumber = context.resultRelationHashSegNum;
			}
			else if(context.externTableForceSegNum > 0){
				/* bucket number of external table must be the same with the number of virtual segments*/
				if(context.externTableForceSegNum < context.externTableLocationSegNum){
					cleanup_allocation_algorithm(&context);
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
					maxTargetSegmentNumber = GetQueryVsegNum();
				}
				minTargetSegmentNumber = context.minimum_segment_num;
			}

			if (enforce_virtual_segment_number > 0) {
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

				saveQueryResourceParameters(
								resource_parameters,                   /* resource_parameters */
								QRL_ONCE,                              /* life */
								sliceNum,                              /* slice_size */
								queryCost,                             /* iobytes */
								maxTargetSegmentNumber,                /* max_target_segment_num */
								minTargetSegmentNumber,                /* min_target_segment_num */
								context.host_context.hostnameVolInfos, /* vol_info */
								context.host_context.size              /* vol_info_size */
								);

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
				result->resource_parameters = resource_parameters;
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
			cleanup_allocation_algorithm(&context);
			elog(ERROR, "Could not allocate enough resource!");
		}

		MemoryContextSwitchTo(context.old_memorycontext);

		/* data locality allocation algorithm*/
		alloc_result = run_allocation_algorithm(result, virtual_segments, &resource, &context);

		result->resource = resource;
		result->resource_parameters = resource_parameters;
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
