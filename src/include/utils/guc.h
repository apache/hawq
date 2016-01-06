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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/*--------------------------------------------------------------------
 * guc.h
 *
 * External declarations pertaining to backend/utils/misc/guc.c and
 * backend/utils/misc/guc-file.l
 *
 * Portions Copyright (c) 2007-2010, Greenplum inc
 * Copyright (c) 2000-2008, PostgreSQL Global Development Group
 * Written by Peter Eisentraut <peter_e@gmx.net>.
 *
 * $PostgreSQL: pgsql/src/include/utils/guc.h,v 1.76 2006/10/19 18:32:47 tgl Exp $
 *--------------------------------------------------------------------
 */
#ifndef GUC_H
#define GUC_H

#include "tcop/dest.h"
#include "utils/array.h"

#define MAX_MAX_BACKENDS (INT_MAX / BLCKSZ)

struct StringInfoData;                  /* #include "lib/stringinfo.h" */


/*
 * Certain options can only be set at certain times. The rules are
 * like this:
 *
 * INTERNAL options cannot be set by the user at all, but only through
 * internal processes ("server_version" is an example).  These are GUC
 * variables only so they can be shown by SHOW, etc.
 *
 * POSTMASTER options can only be set when the postmaster starts,
 * either from the configuration file or the command line.
 *
 * SIGHUP options can only be set at postmaster startup or by changing
 * the configuration file and sending the HUP signal to the postmaster
 * or a backend process. (Notice that the signal receipt will not be
 * evaluated immediately. The postmaster and the backend check it at a
 * certain point in their main loop. It's safer to wait than to read a
 * file asynchronously.)
 *
 * BACKEND options can only be set at postmaster startup, from the
 * configuration file, or by client request in the connection startup
 * packet (e.g., from libpq's PGOPTIONS variable).  Furthermore, an
 * already-started backend will ignore changes to such an option in the
 * configuration file.	The idea is that these options are fixed for a
 * given backend once it's started, but they can vary across backends.
 *
 * SUSET options can be set at postmaster startup, with the SIGHUP
 * mechanism, or from SQL if you're a superuser.
 *
 * USERSET options can be set by anyone any time.
 */
typedef enum
{
	PGC_INTERNAL,
	PGC_POSTMASTER,
	PGC_SIGHUP,
	PGC_BACKEND,
	PGC_SUSET,
	PGC_USERSET
} GucContext;

/*
 * The following type records the source of the current setting.  A
 * new setting can only take effect if the previous setting had the
 * same or lower level.  (E.g, changing the config file doesn't
 * override the postmaster command line.)  Tracking the source allows us
 * to process sources in any convenient order without affecting results.
 * Sources <= PGC_S_OVERRIDE will set the default used by RESET, as well
 * as the current value.  Note that source == PGC_S_OVERRIDE should be
 * used when setting a PGC_INTERNAL option.
 *
 * PGC_S_INTERACTIVE isn't actually a source value, but is the
 * dividing line between "interactive" and "non-interactive" sources for
 * error reporting purposes.
 *
 * PGC_S_TEST is used when testing values to be stored as per-database or
 * per-user defaults ("doit" will always be false, so this never gets stored
 * as the actual source of any value).	This is an interactive case, but
 * it needs its own source value because some assign hooks need to make
 * different validity checks in this case.
 *
 * NB: see GucSource_Names in guc.c if you change this.
 */
typedef enum
{
	PGC_S_DEFAULT,				/* wired-in default */
	PGC_S_ENV_VAR,				/* postmaster environment variable */
	PGC_S_FILE,					/* postgresql.conf */
	PGC_S_ARGV,					/* postmaster command line */
	PGC_S_DATABASE,				/* per-database setting */
	PGC_S_USER,					/* per-user setting */
	PGC_S_CLIENT,				/* from client connection request */
	PGC_S_OVERRIDE,				/* special case to forcibly set default */
	PGC_S_INTERACTIVE,			/* dividing line for error reporting */
	PGC_S_TEST,					/* test per-database or per-user setting */
	PGC_S_SESSION				/* SET command */
} GucSource;

typedef const char *(*GucStringAssignHook) (const char *newval, bool doit, GucSource source);
typedef bool (*GucBoolAssignHook) (bool newval, bool doit, GucSource source);
typedef bool (*GucIntAssignHook) (int newval, bool doit, GucSource source);
typedef bool (*GucRealAssignHook) (double newval, bool doit, GucSource source);

typedef const char *(*GucShowHook) (void);

#define GUC_QUALIFIER_SEPARATOR '.'

/* GUC lists for gp_guc_list_show().  (List of struct config_generic) */
extern List    *gp_guc_list_for_explain;
extern List    *gp_guc_list_for_no_plan;

/* GUC vars that are actually declared in guc.c, rather than elsewhere */
extern bool log_duration;
extern bool Debug_print_plan;
extern bool Debug_print_parse;
extern bool Debug_print_rewritten;
extern bool Debug_pretty_print;
extern bool Explain_pretty_print;
extern bool	Debug_print_full_dtm;
extern bool	Debug_print_snapshot_dtm;
extern bool	Debug_print_qd_mirroring;
extern bool Debug_permit_same_host_standby;
extern bool Debug_print_semaphore_detail;
extern bool Debug_disable_distributed_snapshot;
extern bool Debug_abort_after_distributed_prepared;
extern bool Debug_abort_after_segment_prepared;
extern bool Debug_appendonly_print_insert;
extern bool Debug_appendonly_print_insert_tuple;
extern bool Debug_appendonly_print_scan;
extern bool Debug_appendonly_print_scan_tuple;
extern bool Debug_appendonly_print_storage_headers;
extern bool Debug_appendonly_print_verify_write_block;
extern bool Debug_appendonly_use_no_toast;
extern bool Debug_appendonly_print_blockdirectory;
extern bool Debug_appendonly_print_read_block;
extern bool Debug_appendonly_print_append_block;
extern bool Debug_appendonly_print_segfile_choice;
extern int  Debug_appendonly_bad_header_print_level;
extern bool Debug_appendonly_print_datumstream;
extern bool Debug_querycontext_print_tuple;
extern bool Debug_querycontext_print;
extern bool Debug_gp_relation_node_fetch_wait_for_debugging;
extern bool gp_crash_recovery_abort_suppress_fatal;
extern bool gp_persistent_statechange_suppress_error;
extern bool Debug_bitmap_print_insert;
extern bool Test_appendonly_override;
extern bool	Gohdb_appendonly_override;
extern bool	Test_enable_broken_quicklz3;
extern bool	gp_permit_persistent_metadata_update;
extern bool gp_permit_relation_node_change;
extern bool Test_checksum_override;
extern int  Test_compresslevel_override;
extern int	Test_blocksize_override;
extern int  Test_safefswritesize_override;
extern bool Master_mirroring_administrator_disable;
extern bool gp_local_distributed_cache_stats;
extern bool gp_appendonly_verify_block_checksums;
extern bool gp_appendonly_verify_write_block;
extern bool gp_heap_require_relhasoids_match;
extern bool	Debug_appendonly_rezero_quicklz_compress_scratch;
extern bool	Debug_appendonly_rezero_quicklz_decompress_scratch;
extern bool	Debug_appendonly_guard_end_quicklz_scratch;
extern bool	Debug_xlog_insert_print;
extern bool	Debug_persistent_print;
extern int	Debug_persistent_print_level;
extern bool	Debug_persistent_recovery_print;
extern int	Debug_persistent_recovery_print_level;
extern bool	Debug_persistent_store_print;
extern bool Debug_persistent_bootstrap_print;
extern bool Debug_bulk_load_bypass_wal;
extern bool Debug_persistent_appendonly_commit_count_print;
extern bool Debug_cancel_print;
extern bool Debug_datumstream_write_print_small_varlena_info;
extern bool Debug_datumstream_write_print_large_varlena_info;
extern bool Debug_datumstream_read_check_large_varlena_integrity;
extern bool Debug_datumstream_block_read_check_integrity;
extern bool Debug_datumstream_block_write_check_integrity;
extern bool Debug_datumstream_read_print_varlena_info;
extern bool Debug_datumstream_write_use_small_initial_buffers;
extern int	Debug_persistent_store_print_level;
extern bool	Debug_database_command_print;
extern int	Debug_database_command_print_level;
extern int  gp_max_relations;
extern int	gp_max_databases;
extern int	gp_max_tablespaces;
extern int	gp_max_filespaces;
extern bool gp_initdb_mirrored;
extern bool gp_before_persistence_work;
extern bool gp_before_filespace_setup;
extern bool gp_startup_integrity_checks;
extern bool gp_change_tracking;
extern bool	gp_persistent_skip_free_list;
extern bool	gp_persistent_repair_global_sequence;
extern bool Debug_print_xlog_relation_change_info;
extern bool Debug_print_xlog_relation_change_info_skip_issues_only;
extern bool Debug_print_xlog_relation_change_info_backtrace_skip_issues;
extern bool Debug_filerep_crc_on;
extern bool Debug_filerep_print;
extern bool Debug_filerep_gcov;
extern bool Debug_filerep_config_print;
extern bool Debug_filerep_verify_performance_print;
extern bool Debug_filerep_memory_log_flush;
extern bool filerep_inject_listener_fault;
extern bool filerep_inject_db_startup_fault;
extern bool filerep_inject_change_tracking_recovery_fault;
extern bool gp_crash_recovery_suppress_ao_eof;
extern bool Debug_check_for_invalid_persistent_tid;
extern bool gp_allow_non_uniform_partitioning_ddl;

extern bool gp_upgrade_mode;
extern bool gp_maintenance_mode;
extern bool gp_maintenance_conn;
extern bool allow_segment_DML;

extern bool gp_ignore_window_exclude;

extern int verify_checkpoint_interval;

extern bool Debug_rle_type_compression;
extern bool rle_type_compression_stats;

extern bool Debug_print_tablespace;
extern bool	Debug_print_server_processes;
extern bool Debug_print_control_checkpoints;
extern bool Debug_print_execution_detail;
extern bool	Debug_dtm_action_primary;

extern bool gp_log_optimization_time;
extern bool log_parser_stats;
extern bool log_planner_stats;
extern bool log_executor_stats;
extern bool log_statement_stats;
extern bool log_dispatch_stats;
extern bool log_btree_build_stats;

extern PGDLLIMPORT bool check_function_bodies;
extern bool default_with_oids;
extern bool SQL_inheritance;

extern int	log_min_error_statement;
extern int	log_min_messages;
extern int	client_min_messages;
extern int	log_min_duration_statement;

extern int	num_temp_buffers;

extern bool gp_cancel_query_print_log;
extern int gp_cancel_query_delay_time;

extern bool gp_partitioning_dynamic_selection_log;
extern int gp_max_partition_level;

extern bool gp_crash_handler_async;

extern bool gp_temporary_files_filespace_repair;
extern bool gp_perfmon_print_packet_info;
extern bool gp_plpgsql_clear_cache_always;
extern bool gp_disable_catalog_access_on_segment;
extern int gp_mdversioning_loglevel;

extern bool gp_called_by_pgdump;

/* Debug DTM Action */
typedef enum
{
	DEBUG_DTM_ACTION_NONE = 0,
	DEBUG_DTM_ACTION_DELAY = 1,
	DEBUG_DTM_ACTION_FAIL_BEGIN_COMMAND = 2,
	DEBUG_DTM_ACTION_FAIL_END_COMMAND = 3,
	DEBUG_DTM_ACTION_PANIC_BEGIN_COMMAND = 4,

	DEBUG_DTM_ACTION_LAST = 4
}	DebugDtmAction;

/* Debug DTM Action */
typedef enum
{
	DEBUG_DTM_ACTION_TARGET_NONE = 0,
	DEBUG_DTM_ACTION_TARGET_PROTOCOL = 1,
	DEBUG_DTM_ACTION_TARGET_SQL = 2,

	DEBUG_DTM_ACTION_TARGET_LAST = 2
}	DebugDtmActionTarget;

extern int Debug_dtm_action;
extern int Debug_dtm_action_target;
extern int Debug_dtm_action_protocol;
extern int Debug_dtm_action_delay_ms;
extern int Debug_dtm_action_segment;

extern char *ConfigFileName;
extern char *HbaFileName;
extern char *IdentFileName;
extern char *external_pid_file;

extern char *application_name;

extern char *Debug_dtm_action_sql_command_tag;
extern char *Debug_dtm_action_str;
extern char *Debug_dtm_action_target_str;
extern char *Debug_dtm_action_protocol_str;

/* Enable check for compatibility of encoding and locale in createdb */
extern bool gp_encoding_check_locale_compatibility;

extern int	tcp_keepalives_idle;
extern int	tcp_keepalives_interval;
extern int	tcp_keepalives_count;

extern int	gp_filerep_tcp_keepalives_idle;
extern int	gp_filerep_tcp_keepalives_interval;
extern int	gp_filerep_tcp_keepalives_count;

extern int  WalSendClientTimeout;

extern char  *data_directory;

/* ORCA related definitions */
#define OPTIMIZER_XFORMS_COUNT 400 /* number of transformation rules */

/* types of optimizer failures */
#define OPTIMIZER_ALL_FAIL 			0  /* all failures */
#define OPTIMIZER_UNEXPECTED_FAIL 	1  /* unexpected failures */
#define OPTIMIZER_EXPECTED_FAIL 	2 /* expected failures */

/* optimizer minidump mode */
#define OPTIMIZER_MINIDUMP_FAIL  	0  /* create optimizer minidump on failure */
#define OPTIMIZER_MINIDUMP_ALWAYS 	1  /* always create optimizer minidump */

/* optimizer cost model */
#define OPTIMIZER_GPDB_LEGACY           0       /* GPDB's legacy cost model */
#define OPTIMIZER_GPDB_CALIBRATED       1       /* GPDB's calibrated cost model */

// ORCA-related gucs
extern bool	optimizer;
extern bool	optimizer_log;
extern bool optimizer_minidump;
extern int  optimizer_cost_model;
extern bool optimizer_print_query;
extern bool optimizer_print_plan;
extern bool optimizer_print_xform;
extern bool optimizer_release_mdcache;
extern bool optimizer_disable_xform_result_printing;
extern bool	optimizer_print_memo_after_exploration;
extern bool	optimizer_print_memo_after_implementation;
extern bool	optimizer_print_memo_after_optimization;
extern bool	optimizer_print_job_scheduler;
extern bool	optimizer_print_expression_properties;
extern bool	optimizer_print_group_properties;
extern bool	optimizer_print_optimization_context;
extern bool optimizer_print_optimization_stats;
extern bool	optimizer_parallel;
extern bool	optimizer_local;
extern int  optimizer_retries;
extern bool  optimizer_xforms[OPTIMIZER_XFORMS_COUNT];
extern char *optimizer_search_strategy_path;
extern bool optimizer_extract_dxl_stats;
extern bool optimizer_extract_dxl_stats_all_nodes;
extern bool optimizer_disable_missing_stats_collection;
extern bool optimizer_dpe_stats;
extern bool optimizer_enable_indexjoin;
extern bool optimizer_enable_motions_masteronly_queries;
extern bool optimizer_enable_motions;
extern bool optimizer_enable_motion_broadcast;
extern bool optimizer_enable_motion_gather;
extern bool optimizer_enable_motion_redistribute;
extern bool optimizer_enable_sort;
extern bool optimizer_enable_materialize;
extern bool optimizer_enable_partition_propagation;
extern bool optimizer_enable_partition_selection;
extern bool optimizer_enable_outerjoin_rewrite;
extern bool optimizer_enable_space_pruning;
extern bool optimizer_prefer_multistage_agg;
extern bool optimizer_enable_multiple_distinct_aggs;
extern bool optimizer_prefer_expanded_distinct_aggs;
extern bool optimizer_prune_computed_columns;
extern bool optimizer_push_requirements_from_consumer_to_producer;
extern bool optimizer_enable_hashjoin_redistribute_broadcast_children;
extern bool optimizer_enable_broadcast_nestloop_outer_child;
extern bool optimizer_enforce_subplans;
extern bool optimizer_enable_assert_maxonerow;
extern bool optimizer_enumerate_plans;
extern bool optimizer_sample_plans;
extern int	optimizer_plan_id;
extern int	optimizer_samples_number;
extern int optimizer_log_failure;
extern double optimizer_cost_threshold;
extern double optimizer_nestloop_factor;
extern double locality_upper_bound;
extern bool optimizer_cte_inlining;
extern double net_disk_ratio;
extern int optimizer_cte_inlining_bound;
extern double optimizer_damping_factor_filter;
extern double optimizer_damping_factor_join;
extern double optimizer_damping_factor_groupby;
extern int optimizer_segments;
extern bool optimizer_analyze_root_partition;
extern bool optimizer_analyze_midlevel_partition;
extern bool optimizer_enable_constant_expression_evaluation;
extern bool optimizer_use_external_constant_expression_evaluation_for_ints;
extern bool optimizer_enable_bitmapscan;
extern bool optimizer_enable_outerjoin_to_unionall_rewrite;
extern bool optimizer_apply_left_outer_to_union_all_disregarding_stats;
extern bool optimizer_enable_ctas;
extern bool optimizer_remove_order_below_dml;
extern bool optimizer_static_partition_selection;
extern bool optimizer_enable_partial_index;
extern bool optimizer_dml_triggers;
extern bool	optimizer_dml_constraints;
extern bool optimizer_direct_dispatch;
extern bool optimizer_enable_master_only_queries;
extern bool sort_segments_enable;
extern bool optimizer_multilevel_partitioning;
extern bool optimizer_enable_derive_stats_all_groups;
extern bool optimizer_explain_show_status;
extern bool optimizer_prefer_scalar_dqa_multistage_agg;

/**
 * Enable logging of DPE match in optimizer.
 */
extern bool	optimizer_partition_selection_log;

/*
 * During insertion in a table with parquet partitions,
 * require tuples to be sorted by partition key.
 *
 * This reduces the amount of memory required during execution by
 * keeping only one partition open at a time.
 */
extern bool gp_parquet_insert_sort;

#if USE_EMAIL
extern char  *gp_email_smtp_server;
extern char  *gp_email_smtp_userid;
extern char  *gp_email_smtp_password;
extern char  *gp_email_from;
extern char  *gp_email_to;
extern int   gp_email_connect_timeout;
extern int   gp_email_connect_failures;
extern int   gp_email_connect_avoid_duration; 
#endif

#if USE_SNMP
extern char   *gp_snmp_community;
extern char   *gp_snmp_monitor_address;
extern char   *gp_snmp_use_inform_or_trap;
extern char   *gp_snmp_debug_log;
#endif

/* Extension Framework GUCs */
extern bool   pxf_enable_filter_pushdown; /* turn pushdown logic on/off     */
extern bool   pxf_enable_stat_collection; /* turn off stats collection if needed */
extern int    pxf_stat_max_fragments; /* max fragments to be sampled during analyze */
extern bool   pxf_enable_locality_optimizations; /* turn locality optimization in the data allocation algorithm on/off     */
/*
 * Is Isilon the target storage system ?
 */
extern bool   pxf_isilon;
/*
 * PXF service port. Default value (for tcserver) 51200.
 * This GUC can be changed only on startup.
 * NOTE: This is a temporary GUC, until the port will be read from a conf file.
 */
extern int    pxf_service_port;
/*
 * PXF service address (ip:port or nameservice)
 * Default value (for non-secure, non-HA cluster) localhost:51200.
 * NOTE: This is a temporary GUC, until the address will be read from a conf file.
 */
extern char	*pxf_service_address;
/*
 * A development GUC to use with single cluster.
 * When true, call several PXF instances residing on the same
 * host, but assuming that their ports are incremented consecutively
 * starting from pxf_service_port port.
 */
extern bool   pxf_service_singlecluster;
/*
 * Temporary GUCs to forward login/password information to PXF
 * connector. These can be used to access remote services requiring
 * authentication.
 *
 * These GUCs will be deprecated once a credentials table is introduced.
 */
extern char   *pxf_remote_service_login;
extern char   *pxf_remote_service_secret;

/* Hcatalog GUC */
extern bool   hcatalog_enable;

/* Time based authentication GUC */
extern char  *gp_auth_time_override_str;

extern void SetConfigOption(const char *name, const char *value,
				GucContext context, GucSource source);

extern void DefineCustomBoolVariable(
						 const char *name,
						 const char *short_desc,
						 const char *long_desc,
						 bool *valueAddr,
						 GucContext context,
						 GucBoolAssignHook assign_hook,
						 GucShowHook show_hook);

extern void DefineCustomIntVariable(
						const char *name,
						const char *short_desc,
						const char *long_desc,
						int *valueAddr,
						int minValue,
						int maxValue,
						GucContext context,
						GucIntAssignHook assign_hook,
						GucShowHook show_hook);

extern void DefineCustomRealVariable(
						 const char *name,
						 const char *short_desc,
						 const char *long_desc,
						 double *valueAddr,
						 double minValue,
						 double maxValue,
						 GucContext context,
						 GucRealAssignHook assign_hook,
						 GucShowHook show_hook);

extern void DefineCustomStringVariable(
						   const char *name,
						   const char *short_desc,
						   const char *long_desc,
						   char **valueAddr,
						   GucContext context,
						   GucStringAssignHook assign_hook,
						   GucShowHook show_hook);

extern void EmitWarningsOnPlaceholders(const char *className);

extern const char *GetConfigOption(const char *name);
extern const char *GetConfigOptionResetString(const char *name);
extern bool IsSuperuserConfigOption(const char *name);
extern void ProcessConfigFile(GucContext context);
extern void InitializeGUCOptions(void);
extern bool SelectConfigFiles(const char *userDoption, const char *progname);
extern void ResetAllOptions(void);
extern void AtEOXact_GUC(bool isCommit, bool isSubXact);
extern void BeginReportingGUCOptions(void);
extern void ParseLongOption(const char *string, char **name, char **value);
extern bool set_config_option(const char *name, const char *value,
				  GucContext context, GucSource source,
				  bool isLocal, bool changeVal);
extern char *GetConfigOptionByName(const char *name, const char **varname);
extern void GetConfigOptionByNum(int varnum, const char **values, bool *noshow);
extern int	GetNumConfigOptions(void);

extern void SetPGVariable(const char *name, List *args, bool is_local);
extern void SetPGVariableDispatch(const char *name, List *args, bool is_local);
extern void GetPGVariable(const char *name, DestReceiver *dest);
extern TupleDesc GetPGVariableResultDesc(const char *name);
extern void ResetPGVariable(const char *name);

extern char *flatten_set_variable_args(const char *name, List *args);

extern void ProcessGUCArray(ArrayType *array, GucSource source);
extern ArrayType *GUCArrayAdd(ArrayType *array, const char *name, const char *value);
extern ArrayType *GUCArrayDelete(ArrayType *array, const char *name);
extern ArrayType *GUCArrayReset(ArrayType *array);

extern void pg_timezone_abbrev_initialize(void);

extern int  gp_guc_list_show(struct StringInfoData    *buf,
                              const char               *pfx,
                              const char               *fmt,
                              GucSource                 excluding,
                              List                     *guclist)
                /* This extension allows gcc to check the format string */
                __attribute__((__format__(__printf__, 3, 0)));

#ifdef EXEC_BACKEND
extern void write_nondefault_variables(GucContext context);
extern void read_nondefault_variables(void);
#endif

/*
 * The following functions are not in guc.c, but are declared here to avoid
 * having to include guc.h in some widely used headers that it really doesn't
 * belong in.
 */

/* in commands/tablespace.c */
extern const char *assign_default_tablespace(const char *newval,
						  bool doit, GucSource source);

/* in utils/adt/regexp.c */
extern const char *assign_regex_flavor(const char *value,
					bool doit, GucSource source);

/* in catalog/namespace.c */
extern const char *assign_search_path(const char *newval,
				   bool doit, GucSource source);

/* in access/transam/xlog.c */
extern const char *assign_xlog_sync_method(const char *method,
						bool doit, GucSource source);

#endif   /* GUC_H */
