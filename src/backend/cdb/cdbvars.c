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
 * cdbvars.c
 *	  Provides storage areas and processing routines for Greenplum Database variables
 *	  managed by GUC.
 *
 * NOTES
 *	  See src/backend/utils/misc/guc.c for variable external specification.
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"
#include "miscadmin.h"
#include "utils/guc.h"
#include "catalog/gp_segment_config.h"
#include "cdb/cdbvars.h"
#include "cdb/cdbfts.h"
#include "cdb/cdbutil.h"
#include "lib/stringinfo.h"
#include "libpq/libpq-be.h"
#include "utils/memutils.h"
#include "storage/bfz.h"
#include "cdb/memquota.h"

/*
 * ----------------
 *		GUC/global variables
 *
 *	Initial values are set by guc.c function "InitializeGUCOptions" called
 *	*very* early during postmaster, postgres, or bootstrap initialization.
 * ----------------
 */



GpRoleValue Gp_role;			/* Role paid by this Greenplum Database backend */
char	   *gp_role_string;	/* Staging area for guc.c */
bool		gp_set_read_only;	/* Staging area for guc.c */

GpRoleValue Gp_session_role;	/* Role paid by this Greenplum Database backend */
char	   *gp_session_role_string;	/* Staging area for guc.c */

bool		Gp_is_writer; 		/* is this qExec a "writer" process. */

int 		gp_session_id;    /* global unique id for session. */

int         gp_command_count;          /* num of commands from client */

bool        gp_debug_pgproc;           /* print debug info for PGPROC */
bool		Debug_print_prelim_plan;	/* Shall we log argument of
										 * cdbparallelize? */

bool		Debug_print_slice_table;	/* Shall we log the slice table? */

bool		Debug_print_dispatch_plan;	/* Shall we log the plan we'll dispatch? */

bool		Debug_print_plannedstmt;	/* Shall we log the final planned statement? */

bool            gp_backup_directIO = false;     /* disable\enable direct I/O dump */

int             gp_backup_directIO_read_chunk_mb = 20; /* size of readChunk buffer for directIO dump */

bool		gp_external_enable_exec = true; /* allow ext tables with EXECUTE */

bool		gp_external_grant_privileges = false; /* allow creating http/gpfdist/gpfdists for non-su */

int			gp_external_max_segs;      /* max segdbs per gpfdist/gpfdists URI */

int			gp_safefswritesize;  /* set for safe AO writes in non-mature fs */

int			gp_connections_per_thread; /* How many libpq connections are
										 * handled in each thread */

int			gp_cached_gang_threshold; /*How many gangs to keep around from stmt to stmt.*/

bool		gp_reraise_signal=false;	/* try to dump core when we get SIGABRT & SIGSEGV */

bool		gp_version_mismatch_error=true;	/* Enforce same-version on QD&QE. */

bool		gp_set_proc_affinity=false; /* set processor affinity (if platform supports it) */

int			gp_reject_percent_threshold; /* SREH reject % kicks off only after *
										  * <num> records have been processed  */

int			gp_max_csv_line_length;		/* max allowed len for csv data line in bytes */

bool          gp_select_invisible=false; /* debug mode to allow select to see "invisible" rows */

/*
 * Configurable timeout for snapshot add: exceptionally busy systems may take
 * longer than our old hard-coded version -- so here is a tuneable version.
 */
int     gp_snapshotadd_timeout=10;

/*
 * gp_enable_delete_as_truncate
 *
 * piggy-back a truncate on simple delete statements (statements
 * without qualifiers "delete from foo").
 */
bool	gp_enable_delete_as_truncate = false;

/**
 * Hash-join node releases hash table when it returns last tuple.
 */
bool gp_eager_hashtable_release = true;

/*
 * Debug_print_combocid_detail: request details when we hit combocid limits.
 */
bool	Debug_print_combocid_detail = false;

/*
 * TCP port the Interconnect listens on for incoming connections from other
 * backends.  Assigned by initMotionLayerIPC() at process startup.  This port
 * is used for the duration of this process and should never change.
 */
int			Gp_listener_port;

int			Gp_max_packet_size;	/* max Interconnect packet size */

int			Gp_interconnect_queue_depth=4;	/* max number of messages
											 * waiting in rx-queue
											 * before we drop.*/
int			Gp_interconnect_snd_queue_depth=4;
int			Gp_interconnect_timer_period=5;
int			Gp_interconnect_timer_checking_period=20;
int			Gp_interconnect_default_rtt=20;
int			Gp_interconnect_min_rto=20;
int			Gp_interconnect_fc_method=INTERCONNECT_FC_METHOD_LOSS;
int		    Gp_interconnect_transmit_timeout=3600;
int			Gp_interconnect_min_retries_before_timeout=100;

int			Gp_interconnect_hash_multiplier=2;	/* sets the size of the hash table used by the UDP-IC */

int			interconnect_setup_timeout=7200;

int			Gp_interconnect_type = INTERCONNECT_TYPE_TCP;

bool		gp_interconnect_aggressive_retry=true; /* fast-track app-level retry */

bool gp_interconnect_full_crc=false; /* sanity check UDP data. */

bool gp_interconnect_elide_setup=true; /* under some conditions we can eliminate the setup */

bool gp_interconnect_log_stats=false; /* emit stats at log-level */

bool gp_interconnect_cache_future_packets=true;

int			Gp_udp_bufsize_k; /* UPD recv buf size, in KB */

#ifdef USE_ASSERT_CHECKING
/*
 * UDP-IC Test hooks (for fault injection).
 *
 * Dropseg: specifies which segment to apply the drop_percent to.
 */
int gp_udpic_dropseg = UNDEF_SEGMENT;
int gp_udpic_dropxmit_percent = 0;
int gp_udpic_dropacks_percent = 0;
int gp_udpic_fault_inject_percent = 0;
int gp_udpic_fault_inject_bitmap = 0;
int gp_udpic_network_disable_ipv6 = 0;

/*
 * FileSystem Test hooks (for falt injection)
 */

int gp_fsys_fault_inject_percent = 0;
#endif

/*
 * Each slice table has a unique ID (certain commands like "vacuum analyze"
 * run many many slice-tables for each gp_command_id).
 */
uint32 gp_interconnect_id=0;

/* --------------------------------------------------------------------------------------------------
 * Resource management
 */

/*
 * gp_process_memory_cutoff (real)
 * Deprecated.  Will remove in next release.
 */
double  gp_process_memory_cutoff;           /* SET/SHOW in units of kB */


double gp_hashagg_respill_bias = 1;

/* --------------------------------------------------------------------------------------------------
 * Greenplum Optimizer GUCs
 */

bool        enable_adaptive_nestloop = true;
double      gp_motion_cost_per_row = 0;
int         gp_segments_for_planner = 0;

int         gp_hashagg_default_nbatches = 32;

bool		gp_adjust_selectivity_for_outerjoins = TRUE;
bool		gp_selectivity_damping_for_scans = false;
bool		gp_selectivity_damping_for_joins = false;
double		gp_selectivity_damping_factor = 1;
bool		gp_selectivity_damping_sigsort = true;

int			gp_hashjoin_tuples_per_bucket = 5;
int			gp_hashagg_groups_per_bucket = 5;
int			gp_hashjoin_metadata_memory_percent = 20;


/* default value to 0, which means we do not try to control number of spill batches */
int 		gp_hashagg_spillbatch_min = 0;
int 		gp_hashagg_spillbatch_max = 0;

/* hash join to use bloom filter: default to 0, means not used */
int 	 	gp_hashjoin_bloomfilter = 0;

/* Analyzing aid */
int 		gp_motion_slice_noop = 0;
#ifdef ENABLE_LTRACE
int 		gp_ltrace_flag = 0;
#endif

/* Internal Features */
bool		gp_enable_alter_table_inherit_cols = false;

/* During insertion in a table with parquet partitions, require tuples to be sorted by partition key */
bool		gp_parquet_insert_sort = true;

/* The following GUCs is for HAWQ 2.o */

bool optimizer_enforce_hash_dist_policy;
int external_table_init_segment_num;
int appendonly_split_write_size_mb;
int split_read_size_mb;
int enforce_virtual_segment_number;
bool debug_print_split_alloc_result;
bool prefer_datalocality_to_iobalance;
bool balance_on_partition_table_level;
bool balance_on_whole_query_level;
bool output_hdfs_block_location;
int max_filecount_notto_split_segment;
int min_datasize_to_combine_segment;
int datalocality_algorithm_version;
int hash_to_random_flag;
int min_cost_for_each_query;
bool metadata_cache_enable;
int metadata_cache_block_capacity;

/* GUCs for vectorized executor */
bool vectorized_executor_enable;

/* The 5 gucs below related to metadatacache_test . */
int metadata_cache_check_interval;
int metadata_cache_refresh_interval;
int metadata_cache_refresh_timeout;
int metadata_cache_refresh_max_num;
double metadata_cache_free_block_max_ratio;
double metadata_cache_free_block_normal_ratio;

int metadata_cache_max_hdfs_file_num;
double metadata_cache_flush_ratio;
double metadata_cache_reduce_ratio;

char *metadata_cache_testfile;
bool debug_fake_datalocality;
bool datalocality_remedy_enable;
bool get_tmpdir_from_rm;
bool debug_fake_segmentnum;
bool debug_datalocality_time;
bool enable_prefer_list_to_rm;

/* New HAWQ 2.0 basic GUCs. Some of these are duplicate variables, they are
 * reserved to facilitate showing settings in hawq-site.xml. */
char  *master_addr_host;
int    master_addr_port;
char  *standby_addr_host;
int    seg_addr_port;
char  *dfs_url;
char  *master_directory;
char  *seg_directory;
int    segment_history_keep_period;

/* HAWQ 2.0 resource manager GUCs */
int    rm_master_port;
int	   rm_segment_port;
int	   rm_master_domain_port;
bool   rm_enable_connpool;
int	   rm_connpool_sameaddr_buffersize;

int    rm_nvseg_perquery_limit;
int	   rm_nvseg_perquery_perseg_limit;
int	   rm_nslice_perseg_limit;

char  *rm_seg_memory_use;
double rm_seg_core_use;

char   *rm_global_rm_type;
char   *rm_grm_yarn_rm_addr;
char   *rm_grm_yarn_sched_addr;
char   *rm_grm_yarn_queue;
char   *rm_grm_yarn_app_name;
int		rm_return_percentage_on_overcommit;
int     rm_cluster_report_period;

char   *rm_stmt_vseg_mem_str;
int		rm_stmt_nvseg;

int		rm_min_resource_perseg;
bool	rm_force_fifo_queue;
bool	rm_force_alterqueue_cancel_queued_request;

bool	rm_session_lease_heartbeat_enable;
int     rm_session_lease_timeout; 			/* How many seconds to wait before
											   expiring allocated resource. */
int		rm_resource_allocation_timeout;		/* How may seconds to wait before
										   	   expiring queuing query resource
										   	   request. */
int		rm_resource_timeout;				/* How many seconds to wait before
											   returning resource back to the
											   resource broker. */
int		rm_request_timeoutcheck_interval; 	/* How many seconds to wait before
											   checking resource contexts for
											   timeout. */
int		rm_session_lease_heartbeat_interval;/* How many seconds to wait before
											   sending another heart-beat to
											   resource manager. */
int		rm_nocluster_timeout;				/* How many seconds to wait before
											   getting enough number of available
											   segments registered. */
int		rm_segment_heartbeat_interval;		/* How many seconds to wait before
											   sending another heart-beat to
											   from a segment to resource
											   manager. */
int		rm_segment_heartbeat_timeout;		/* How many seconds to wait before
											   setting down a segment that does
											   not have heart-beat sent
											   successfully to resource
											   manager. */
int		rm_segment_config_refresh_interval; /* How many seconds to wait before
 	 	 	 	 	 	 	 	 	 	 	   another refreshing local segment
 	 	 	 	 	 	 	 	 	 	 	   configuration. */
int		rm_segment_tmpdir_detect_interval;	/* How many seconds to wait before
											   another detecting local temporary
											   directories. */

int		rm_nvseg_variance_among_seg_limit;
int		rm_container_batch_limit;

double	rm_tolerate_nseg_limit;
double	rm_rejectrequest_nseg_limit;

char   *rm_resourcepool_test_filename;

bool	rm_enforce_cpu_enable;
char	*rm_enforce_cgrp_mnt_pnt;
char	*rm_enforce_cgrp_hier_name;
double	rm_enforce_cpu_weight;
double	rm_enforce_core_vpratio;
int		rm_enforce_cleanup_period;

int	rm_allocation_policy;

char   *rm_master_tmp_dirs;
char   *rm_seg_tmp_dirs;

int     rm_log_level;
int     rm_nresqueue_limit;

double	rm_regularize_io_max;
double	rm_regularize_nvseg_max;
double	rm_regularize_io_factor;
double	rm_regularize_usage_factor;
double	rm_regularize_nvseg_factor;

int		rm_clusterratio_core_to_memorygb_factor;

int		rm_nvseg_variance_among_seg_respool_limit;

/* Greenplum Database Experimental Feature GUCs */
int         gp_distinct_grouping_sets_threshold = 32;
bool		gp_enable_explain_allstat = FALSE;
bool		gp_enable_motion_deadlock_sanity = FALSE; /* planning time sanity check */

#ifdef USE_ASSERT_CHECKING
bool		gp_mk_sort_check = false;
#endif
bool 		trace_sort = false;
int			gp_sort_flags = 0;
int			gp_dbg_flags = 0;
int 		gp_sort_max_distinct = 20000;

bool		gp_enable_hash_partitioned_tables = FALSE;
bool		gp_foreign_data_access = FALSE;
bool		gp_setwith_alter_storage = FALSE;

bool		gp_enable_tablespace_auto_mkdir = FALSE;

/* MPP-9772, MPP-9773: remove support for CREATE INDEX CONCURRENTLY */
bool		gp_create_index_concurrently = FALSE;

/* Enable check for compatibility of encoding and locale in createdb */
bool gp_encoding_check_locale_compatibility = true;

/* Priority for the segworkers relative to the postmaster's priority */
int gp_segworker_relative_priority = PRIO_MAX;

/* Max size of dispatched plans; 0 if no limit */
int			gp_max_plan_size = 0;

/* Disable setting of tuple hints while reading */
bool		gp_disable_tuple_hints = false;
int		gp_hashagg_compress_spill_files = 0;

int gp_workfile_compress_algorithm = 0;
bool gp_workfile_checksumming = false;
bool gp_workfile_caching = false;
int gp_workfile_caching_loglevel = DEBUG1;
int gp_sessionstate_loglevel = DEBUG1;
/* Maximum disk space to use for workfiles on a segment, in kilobytes */
double gp_workfile_limit_per_segment = 0;
/* Maximum disk space to use for workfiles per query on a segment, in kilobytes */
double gp_workfile_limit_per_query = 0;
/* Maximum number of workfiles to be created by a query */
int gp_workfile_limit_files_per_query = 0;
bool gp_workfile_faultinject = false;
int gp_workfile_bytes_to_checksum = 16;

/* The type of work files that HashJoin should use */
int gp_workfile_type_hashjoin = 0;

/* Gpmon */
bool gp_enable_gpperfmon = false;
int gp_gpperfmon_send_interval = 1;

/* Enable single-slice single-row inserts ?*/
bool		gp_enable_fast_sri=true;

/* Enable single-mirror pair dispatch. */
bool		gp_enable_direct_dispatch=true;

/* Disable logging while creating mapreduce objects */
bool        gp_mapreduce_define=false;

/* request fault-prober pause */
bool		gp_fts_probe_pause=false;

/* Force core dump on memory context error */
bool 		coredump_on_memerror=false;

/* if catquery.c is built with the logquery option, allow caql logging */
bool		gp_enable_caql_logging = true;

/* Force query use data directory for temporary files. */
bool		gp_force_use_default_temporary_directory = false;
int			gp_temporary_directory_mark_error = 0;

/* Experimental feature for MPP-4082. Please read doc before setting this guc */
GpAutoStatsModeValue gp_autostats_mode;
char                *gp_autostats_mode_string;
GpAutoStatsModeValue gp_autostats_mode_in_functions;
char                *gp_autostats_mode_in_functions_string;
int                  gp_autostats_on_change_threshold = 100000;
bool				 log_autostats=true;
/* --------------------------------------------------------------------------------------------------
 * Miscellaneous developer use
 */

bool	gp_dev_notice_agg_cost = false;

/* --------------------------------------------------------------------------------------------------
 * Server debugging
 */

/*
 * gp_debug_linger (integer)
 *
 * Upon an error with severity FATAL and error code ERRCODE_INTERNAL_ERROR,
 * errfinish() will sleep() for the specified number of seconds before
 * termination, to let the user attach a debugger.
 */
int  gp_debug_linger = 30;

/* ----------------
 * Non-GUC globals
 */

int			currentSliceId = UNSET_SLICE_ID;	/* used by elog to show the
												 * current slice the process
												 * is executing. */
SeqServerControlBlock *seqServerCtl;

/* Segment id where singleton gangs are to be dispatched. */
int         gp_singleton_segindex;

bool        gp_cost_hashjoin_chainwalk = false;

/* ----------------
 * This variable is initialized by the postmaster from command line arguments
 *
 * Any code needing the "numsegments"
 * can simply #include cdbvars.h, and use GpIdentity.numsegments
 */
GpId GpIdentity = {UNINITIALIZED_GP_IDENTITY_VALUE, UNINITIALIZED_GP_IDENTITY_VALUE, UNINITIALIZED_GP_IDENTITY_VALUE};

/*
 * Local macro to provide string values of numeric defines.
 */
#define CppNumericAsString(s) CppAsString(s)

/*
 *	Forward declarations of local function.
 */
GpRoleValue string_to_role(const char *string);
const char *role_to_string(GpRoleValue role);


/*
 * Convert a Greenplum Database role string (as for gp_session_role or gp_role) to an
 * enum value of type GpRoleValue. Return GP_ROLE_UNDEFINED in case the
 * string is unrecognized.
 */
GpRoleValue
string_to_role(const char *string)
{
	GpRoleValue role = GP_ROLE_UNDEFINED;

	if (pg_strcasecmp(string, "dispatch") == 0 || pg_strcasecmp(string, "") == 0)
	{
		role = GP_ROLE_DISPATCH;
	}
	else if (pg_strcasecmp(string, "execute") == 0)
	{
		role = GP_ROLE_EXECUTE;
	}
	else if (pg_strcasecmp(string, "utility") == 0)
	{
		role = GP_ROLE_UTILITY;
	}

	return role;
}

/*
 * Convert a GpRoleValue to a role string (as for gp_session_role or
 * gp_role).  Return eyecatcher in the unexpected event that the value
 * is unknown or undefined.
 */
const char *
role_to_string(GpRoleValue role)
{
	switch (role)
	{
		case GP_ROLE_DISPATCH:
			return "dispatch";
		case GP_ROLE_EXECUTE:
			return "execute";
		case GP_ROLE_UTILITY:
			return "utility";
		case GP_ROLE_UNDEFINED:
		default:
			return "*undefined*";
	}
}


/*
 * Assign hook routine for "gp_session_role" option.  Because this variable
 * has context PGC_BACKEND, we expect this assigment to happen only during
 * setup of a BACKEND, e.g., based on the role value specified on the connect
 * request.
 *
 * See src/backend/util/misc/guc.c for option definition.
 */
const char *
assign_gp_session_role(const char *newval, bool doit, GucSource source __attribute__((unused)) )
{

#if FALSE
	elog(DEBUG1, "assign_gp_session_role: gp_session_role=%s, newval=%s, doit=%s",
		 show_gp_session_role(), newval, (doit ? "true" : "false"));
#endif

	GpRoleValue newrole = string_to_role(newval);

	if (newrole == GP_ROLE_UNDEFINED)
	{
		return NULL;
	}

	if (doit)
	{
		Gp_session_role = newrole;
		Gp_role = Gp_session_role;
	}
	return newval;
}



/*
 * Assign hook routine for "gp_role" option.  This variablle has context
 * PGC_SUSET so that is can only be set by a superuser via the SET command.
 * (It can also be set using an option on postmaster start, but this isn't
 * interesting beccause the derived global CdbRole is always set (along with
 * CdbSessionRole) on backend startup for a new connection.
 *
 * See src/backend/util/misc/guc.c for option definition.
 */
const char *
assign_gp_role(const char *newval, bool doit, GucSource source)
{

#if FALSE
	elog(DEBUG1, "assign_gp_role: gp_role=%s, newval=%s, doit=%s",
		 show_gp_role(), newval, (doit ? "true" : "false"));
#endif


	GpRoleValue newrole = string_to_role(newval);
	GpRoleValue oldrole = Gp_role;

	if (newrole == GP_ROLE_UNDEFINED)
	{
		return NULL;
	}

	if (doit)
	{
		/*
		 * When changing between roles, we must
		 * call cdb_cleanup and then cdb_setup to get
		 * setup and connections appropriate to the new role.
		 */
		bool		do_disconnect = false;
		bool		do_connect = false;

		if (Gp_role != newrole && IsUnderPostmaster)
		{
			if (Gp_role != GP_ROLE_UTILITY)
				do_disconnect = true;

			if (newrole != GP_ROLE_UTILITY)
				do_connect = true;
		}

		if (do_disconnect)
			cdb_cleanup(0,0);

		Gp_role = newrole;

		if (source != PGC_S_DEFAULT)
		{
			if (do_connect)
			{
				/*
				 * In case there are problems with the Greenplum Database tables or data,
				 * we catch any error coming out of cdblink_setup so we can set the
				 * gp_role back to what it was.  Otherwise we may be left with
				 * inappropriate connections for the new role.
				 */
				PG_TRY();
				{
					cdb_setup();
				}
				PG_CATCH();
				{
					cdb_cleanup(0,0);
					Gp_role = oldrole;
					if (Gp_role != GP_ROLE_UTILITY)
						cdb_setup();
					PG_RE_THROW();
				}
				PG_END_TRY();
			}
		}
	}

	return newval;
}


/*
 * Assign hook routine for "gp_connections_per_thread" option.  This variablle has context
 * PGC_SUSET so that is can only be set by a superuser via the SET command.
 * (It can also be set in config file, but not inside of PGOPTIONS.)
 *
 * See src/backend/util/misc/guc.c for option definition.
 */
bool
assign_gp_connections_per_thread(int newval, bool doit, GucSource source __attribute__((unused)) )
{

#if FALSE
	elog(DEBUG1, "assign_gp_connections_per_thread: gp_connections_per_thread=%s, newval=%d, doit=%s",
	   show_gp_connections_per_thread(), newval, (doit ? "true" : "false"));
#endif

	if (doit)
	{
		if (newval < 0)
			return false;

		gp_connections_per_thread = newval;
	}

	return true;
}

/*
 * Assign hook routine for "assign_gp_use_dispatch_agent" option.  This variable has context
 * PGC_USERSET
 *
 * See src/backend/util/misc/guc.c for option definition.
 */
void disconnectAndDestroyAllGangs(void);

/*
 * Show hook routine for "gp_session_role" option.
 *
 * See src/backend/util/misc/guc.c for option definition.
 */
const char *
show_gp_session_role(void)
{
	return role_to_string(Gp_session_role);
}


/*
 * Show hook routine for "gp_role" option.
 *
 * See src/backend/util/misc/guc.c for option definition.
 */
const char *
show_gp_role(void)
{
	return role_to_string(Gp_role);
}

/*
 * Show hook routine for "gp_connections_per_thread" option.
 *
 * See src/backend/util/misc/guc.c for option definition.
 */
const char *
show_gp_connections_per_thread(void)
{
	/*
	 * We rely on the fact that the memory context will clean up the memory
	 * for the buffer.data.
	 */
	StringInfoData buffer;

	initStringInfo(&buffer);

	appendStringInfo(&buffer, "%d", gp_connections_per_thread);

	return buffer.data;
}



/* --------------------------------------------------------------------------------------------------
 * Logging
 */


/*
 * gp_log_gangs (string)
 *
 * Should creation, reallocation and cleanup of gangs of QE processes be logged?
 * "OFF"     -> only errors are logged
 * "TERSE"   -> terse logging of routine events, e.g. creation of new qExecs
 * "VERBOSE" -> gang allocation per command is logged
 * "DEBUG"   -> additional events are logged at severity level DEBUG1 to DEBUG5
 *
 * The messages that are enabled by the TERSE and VERBOSE settings are
 * written with a severity level of LOG.
 */
GpVars_Verbosity   gp_log_gang;

/*
 * gp_log_interconnect (string)
 *
 * Should connections between internal processes be logged?  (qDisp/qExec/etc)
 * "OFF"     -> connection errors are logged
 * "TERSE"   -> terse logging of routine events, e.g. successful connections
 * "VERBOSE" -> most interconnect setup events are logged
 * "DEBUG"   -> additional events are logged at severity level DEBUG1 to DEBUG5.
 *
 * The messages that are enabled by the TERSE and VERBOSE settings are
 * written with a severity level of LOG.
 */
GpVars_Verbosity   gp_log_interconnect;

/*
 * gpvars_string_to_verbosity
 */
GpVars_Verbosity
gpvars_string_to_verbosity(const char *s)
{
	GpVars_Verbosity result;

	if (!s ||
        !s[0] ||
        !pg_strcasecmp("terse", s))
		result = GPVARS_VERBOSITY_TERSE;
	else if (!pg_strcasecmp("off", s))
		result = GPVARS_VERBOSITY_OFF;
	else if (!pg_strcasecmp("verbose", s))
		result = GPVARS_VERBOSITY_VERBOSE;
	else if (!pg_strcasecmp("debug", s))
		result = GPVARS_VERBOSITY_DEBUG;
    else
        result = GPVARS_VERBOSITY_UNDEFINED;
	return result;
}                               /* gpvars_string_to_verbosity */

/*
 * gpvars_verbosity_to_string
 */
const char *
gpvars_verbosity_to_string(GpVars_Verbosity verbosity)
{
	switch (verbosity)
	{
		case GPVARS_VERBOSITY_OFF:
			return "off";
		case GPVARS_VERBOSITY_TERSE:
			return "terse";
		case GPVARS_VERBOSITY_VERBOSE:
			return "verbose";
		case GPVARS_VERBOSITY_DEBUG:
			return "debug";
		default:
			return "*undefined*";
	}
}                               /* gpvars_verbosity_to_string */


/*
 * gpvars_assign_gp_log_gangs
 * gpvars_show_gp_log_gangs
 */
const char *
gpvars_assign_gp_log_gang(const char *newval, bool doit, GucSource source __attribute__((unused)) )
{
	GpVars_Verbosity v = gpvars_string_to_verbosity(newval);

	if (v == GPVARS_VERBOSITY_UNDEFINED)
        return NULL;
	if (doit)
		gp_log_gang = v;
	return newval;
}                               /* gpvars_assign_gp_log_gangs */

const char *
gpvars_show_gp_log_gang(void)
{
	return gpvars_verbosity_to_string(gp_log_gang);
}                               /* gpvars_show_gp_log_gangs */


/*
 * gpvars_assign_gp_log_interconnect
 * gpvars_show_gp_log_interconnect
 */
const char *
gpvars_assign_gp_log_interconnect(const char *newval, bool doit, GucSource source __attribute__((unused)) )
{
	GpVars_Verbosity v = gpvars_string_to_verbosity(newval);

	if (v == GPVARS_VERBOSITY_UNDEFINED)
        return NULL;
	if (doit)
		gp_log_interconnect = v;
	return newval;
}                               /* gpvars_assign_gp_log_interconnect */

const char *
gpvars_show_gp_log_interconnect(void)
{
	return gpvars_verbosity_to_string(gp_log_interconnect);
}                               /* gpvars_show_gp_log_interconnect */


/*
 * gpvars_assign_gp_interconnect_type
 * gpvars_show_gp_interconnect_type
 */
const char *
gpvars_assign_gp_interconnect_type(const char *newval, bool doit, GucSource source __attribute__((unused)) )
{
	int newtype = 0;

	if (newval == NULL || newval[0] == 0 ||
		!pg_strcasecmp("tcp", newval))
		newtype = INTERCONNECT_TYPE_TCP;
	else if (!pg_strcasecmp("udp", newval))
		newtype = INTERCONNECT_TYPE_UDP;
	else if (!pg_strcasecmp("nil", newval))
		newtype = INTERCONNECT_TYPE_NIL;
	else
		elog(ERROR, "Unknown interconnect type. (current type is '%s')", gpvars_show_gp_interconnect_type());

	if (doit)
	{
		if (newtype == INTERCONNECT_TYPE_NIL)
		{
			if (Gp_role == GP_ROLE_DISPATCH)
				elog(WARNING, "Nil-Interconnect diagnostic mode enabled (tuple will be dropped).");
			else
				elog(LOG, "Nil-Interconnect diagnostic mode enabled (tuple will be dropped).");

		}
		else if (Gp_interconnect_type == INTERCONNECT_TYPE_NIL)
		{
			if (Gp_role == GP_ROLE_DISPATCH)
				elog(WARNING, "Nil-Interconnect diagnostic mode disabled.");
			else
				elog(LOG, "Nil-Interconnect diagnostic mode disabled.");
		}

		Gp_interconnect_type = newtype;
	}

	return newval;
}                               /* gpvars_assign_gp_log_interconnect */

const char *
gpvars_show_gp_interconnect_type(void)
{
	switch(Gp_interconnect_type)
	{
		case INTERCONNECT_TYPE_UDP:
			return "UDP";
		case INTERCONNECT_TYPE_NIL:
			return "NIL";
		case INTERCONNECT_TYPE_TCP:
		default:
			return "TCP";
	}
}                               /* gpvars_show_gp_log_interconnect */

/*
 * gpvars_assign_gp_interconnect_fc_method
 * gpvars_show_gp_interconnect_fc_method
 */
const char *
gpvars_assign_gp_interconnect_fc_method(const char *newval, bool doit, GucSource source __attribute__((unused)) )
{
	int newmethod = 0;

	if (newval == NULL || newval[0] == 0 ||
		!pg_strcasecmp("capacity", newval))
		newmethod = INTERCONNECT_FC_METHOD_CAPACITY;
	else if (!pg_strcasecmp("loss", newval))
		newmethod = INTERCONNECT_FC_METHOD_LOSS;
	else
		elog(ERROR, "Unknown interconnect flow control method. (current method is '%s')", gpvars_show_gp_interconnect_fc_method());

	if (doit)
	{
		Gp_interconnect_fc_method = newmethod;
	}

	return newval;
}                               /* gpvars_assign_gp_interconnect_fc_method */

const char *
gpvars_show_gp_interconnect_fc_method(void)
{
	switch(Gp_interconnect_fc_method)
	{
		case INTERCONNECT_FC_METHOD_CAPACITY:
			return "CAPACITY";
		case INTERCONNECT_FC_METHOD_LOSS:
			return "LOSS";
		default:
			return "CAPACITY";
	}
}                               /* gpvars_show_gp_interconnect_fc_method */

/*
 * Parse the string value of gp_autostats_mode and gp_autostats_mode_in_functions
 */
static int
gpvars_parse_gp_autostats_mode(const char *newval, bool inFunctions)
{
	int newtype = 0;

	if (newval == NULL || newval[0] == 0 ||
		!pg_strcasecmp("none", newval))
	{
		newtype = GP_AUTOSTATS_NONE;
	}
	else if (!pg_strcasecmp("on_change", newval) || !pg_strcasecmp("onchange", newval))
	{
		newtype = GP_AUTOSTATS_ON_CHANGE;
	}
	else if (!pg_strcasecmp("on_no_stats", newval))
	{
		newtype = GP_AUTOSTATS_ON_NO_STATS;
	}
	else
	{
		const char *autostats_mode_string;
		if (inFunctions)
		{
			autostats_mode_string = gpvars_show_gp_autostats_mode_in_functions();
		}
		else
		{
			autostats_mode_string = gpvars_show_gp_autostats_mode();
		}
		elog(ERROR, "Unknown autostats mode. (current type is '%s')", autostats_mode_string);
	}

	return newtype;
}

/*
 * gpvars_assign_gp_autostats_mode
 * gpvars_show_gp_autostats_mode
 */
const char *
gpvars_assign_gp_autostats_mode(const char *newval, bool doit, GucSource source __attribute__((unused)) )
{
	int newtype = gpvars_parse_gp_autostats_mode(newval, false /* inFunctions */);

	if (doit)
	{
		gp_autostats_mode = newtype;
	}

	return newval;
}

/*
 * Common function to show the value of the gp_autostats_mode
 * and gp_autostats_mode_in_functions GUCs
 */
static const char *
gpvars_show_gp_autostats_mode_common(bool inFunctions)
{
	GpAutoStatsModeValue autostats_mode;
	if (inFunctions)
	{
		autostats_mode = gp_autostats_mode_in_functions;
	}
	else
	{
		autostats_mode = gp_autostats_mode;
	}
	switch(autostats_mode)
	{
		case GP_AUTOSTATS_NONE:
			return "NONE";
		case GP_AUTOSTATS_ON_CHANGE:
			return "ON_CHANGE";
		case GP_AUTOSTATS_ON_NO_STATS:
			return "ON_NO_STATS";
		default:
			return "NONE";
	}
}

const char *
gpvars_show_gp_autostats_mode(void)
{
	return gpvars_show_gp_autostats_mode_common(false /* inFunctions */);
}

/*
 * gpvars_assign_gp_autostats_mode_in_functions
 * gpvars_show_gp_autostats_mode_in_functions
 */

const char *
gpvars_assign_gp_autostats_mode_in_functions(const char *newval, bool doit, GucSource source __attribute__((unused)) )
{
	bool inFunctions = true;
	int newtype = gpvars_parse_gp_autostats_mode(newval, inFunctions);

	if (doit)
	{
		gp_autostats_mode_in_functions = newtype;
	}

	return newval;
}


const char *
gpvars_show_gp_autostats_mode_in_functions(void)
{
	return gpvars_show_gp_autostats_mode_common(true /* inFunctions */);
}

/* gp_enable_gpperfmon and gp_gpperfmon_send_interval are GUCs that we'd like
 * to have propagate from master to segments but we don't want non-super users
 * to be able to set it.  Unfortunately, as long as we use libpq to connect to
 * the segments its hard to create a clean way of doing this.
 *
 * Here we check and enforce that if the value is being set on the master its being
 * done as superuser and not a regular user.
 *
 */
bool
gpvars_assign_gp_enable_gpperfmon(bool newval, bool doit, GucSource source)
{
	if (doit)
	{

		if (Gp_role == GP_ROLE_DISPATCH && IsUnderPostmaster && GetCurrentRoleId() != InvalidOid && !superuser())
		{
			ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
				 errmsg("must be superuser to set gp_enable_gpperfmon")));
		}
		else
		{
			gp_enable_gpperfmon=newval;
		}
	}

	return true;
}

bool
gpvars_assign_gp_gpperfmon_send_interval(int newval, bool doit, GucSource source)
{
	if (doit)
	{
		if (Gp_role == GP_ROLE_DISPATCH && IsUnderPostmaster && GetCurrentRoleId() != InvalidOid && !superuser())
		{
			ereport(ERROR,
					(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
					 errmsg("must be superuser to set gp_gpperfmon_send_interval")));
		}
		else
		{
			gp_gpperfmon_send_interval=newval;
		}
	}

	return true;
}

/*
 * Request the fault-prober to suspend probes -- no fault actions will
 * be taken based on in-flight probes until the prober is unpaused.
 */
bool
gpvars_assign_gp_fts_probe_pause(bool newval, bool doit, GucSource source)
{
	if (doit)
	{
		/*
		 * We only want to do fancy stuff on the master (where we have a prober).
		 */
		if (ftsProbeInfo && AmIMaster())
		{
			/*
			 * fts_pauseProbes is externally set/cleared;
			 * fts_cancelProbes is externally set and cleared by FTS
			 */
			ftsLock();
			ftsProbeInfo->fts_pauseProbes = newval;
			ftsProbeInfo->fts_discardResults = ftsProbeInfo->fts_discardResults || newval;
			ftsUnlock();

			/*
			 * If we're unpausing, we want to force the prober to
			 * re-read everything. (we want FtsNotifyProber()).
			 */
			if (!newval)
			{
				FtsNotifyProber();
			}
		}
		gp_fts_probe_pause = newval;
	}

	return true;
}

bool
gpvars_assign_gp_hash_index(bool newval, bool doit, GucSource source)
{
	if (doit && newval)
	{
		if(Gp_role == GP_ROLE_DISPATCH)
			ereport(WARNING,
						(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
						 errmsg("gp_hash_index is deprecated and has no effect")));
	}

	return true;
}

/*
 * gpvars_assign_statement_mem
 */
bool
gpvars_assign_statement_mem(int newval, bool doit, GucSource source __attribute__((unused)) )
{
	if (doit)
	{
		statement_mem = newval;
	}

	return true;
}

/*
 * increment_command_count
 *    Increment gp_command_count. If the new command count is 0 or a negative number, reset it to 1.
 */
void
increment_command_count()
{

	if (gp_cancel_query_print_log)
	{
		ereport(LOG,
				(errmsg("Incrementing command count from %d to %d",
						gp_command_count, gp_command_count+1)));
	}

	gp_command_count++;
	if (gp_command_count <= 0)
	{
		gp_command_count = 1;
	}
}
