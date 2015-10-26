/*-------------------------------------------------------------------------
 *
 * cdbvars.h
 *	  definitions for Greenplum-specific global variables
 *
 * Copyright (c) 2003-2010, Greenplum inc
 *
 * NOTES
 *	  See src/backend/utils/misc/guc.c for variable external specification.
 *
 *-------------------------------------------------------------------------
 */

#ifndef GPVARS_H
#define GPVARS_H

#include "access/xlogdefs.h"  /*XLogRecPtr*/
#include "utils/guc.h"
#include "postmaster/identity.h"

/*
 * ----- Declarations of Greenplum-specific global variables ------
 */

#ifdef sparc
#define TUPLE_CHUNK_ALIGN	4
#else
#define TUPLE_CHUNK_ALIGN	1
#endif

#ifndef PRIO_MAX
#define PRIO_MAX 20
#endif
/*
 * Parameters gp_session_role and gp_role
 *
 * The run-time parameters (GUC variables) gp_session_role and
 * gp_role report and provide control over the role assumed by a
 * postgres process.
 *
 * Valid  roles are the following:
 *
 *	dispatch	The process acts as a parallel SQL dispatcher.
 *	execute		The process acts as a parallel SQL executor.
 *	utility		The process acts as a simple SQL engine.
 *
 * Both parameters are initialized to the same value at connection
 * time and are local to the backend process resulting from the
 * connection.	The default is dispatch which is the normal setting
 * for a user of Greenplum connecting to a node of  the Greenplum cluster.
 * Neither parameter appears in the configuration file.
 *
 * gp_session_role
 *
 * - does not affect the operation of the backend, and
 * - does not change during the lifetime of PostgreSQL session.
 *
 * gp_role
 *
 * - determines the operating role of the backend, and
 * - may be changed by a superuser via the SET command.
 *
 * The connection time value of gp_session_role used by a
 * libpq-based client application can be specified using the
 * environment variable PGOPTIONS.	For example, this is how to
 * invoke psql on database test as a utility:
 *
 *	PGOPTIONS='-c gp_session_role=utility' psql test
 *
 * Alternatively, libpq-based applications can be modified to set
 * the value directly via an argument to one of the libpq functions
 * PQconnectdb, PQsetdbLogin or PQconnectStart.
 *
 * Don't try to set gp_role this way.  At the time options are
 * processed it is unknown whether the user is a superuser, so
 * the attempt will be rejected.
 *
 * ----------
 *
 * Greenplum Developers can access the values of these parameters via the
 * global variables Gp_session_role and Gp_role of type
 * GpRoleValue. For example
 *
 *	#include "cdb/cdbvars.h"
 *
 *	switch ( Gp_role  )
 *	{
 *		case GP_ROLE_DISPATCH:
 *			... Act like a query dispatcher ...
 *			break;
 *		case GP_ROLE_EXECUTE:
 *			... Act like a query executor ...
 *			break;
 *		case GP_ROLE_UTILITY:
 *			... Act like an unmodified PostgreSQL backend. ...
 *			break;
 *		default:
 *			... Won't happen ..
 *			break;
 *	}
 *
 * You can also modify Gp_role (even if the session doesn't have
 * superuser privileges) by setting it to one of the three valid
 * values, however this must be well documented to avoid
 * disagreements between modules.  Don't modify the value  of
 * Gp_session_role.
 *
 */

typedef enum
{
	GP_ROLE_UTILITY = 0,		/* Operating as a simple database engine */
	GP_ROLE_DISPATCH,			/* Operating as the parallel query dispatcher */
	GP_ROLE_EXECUTE,			/* Operating as a parallel query executor */
	GP_ROLE_UNDEFINED			/* Should never see this role in use */
} GpRoleValue;


extern GpRoleValue Gp_session_role;	/* GUC var - server startup mode.  */
extern char *gp_session_role_string;	/* Use by guc.c as staging area for
										 * value. */
extern const char *assign_gp_session_role(const char *newval, bool doit, GucSource source);
extern const char *show_gp_session_role(void);

extern GpRoleValue Gp_role;	/* GUC var - server operating mode.  */
extern char *gp_role_string;	/* Use by guc.c as staging area for value. */
extern const char *assign_gp_role(const char *newval, bool doit, GucSource source);
extern const char *show_gp_role(void);

extern bool gp_reraise_signal; /* try to force a core dump ?*/

extern bool gp_version_mismatch_error;	/* Enforce same-version on QD&QE. */

extern bool gp_set_proc_affinity; /* try to bind postmaster to a processor */

/* Parameter Gp_is_writer
 *
 * This run_time parameter indicates whether session is a qExec that is a
 * "writer" process.  Only writers are allowed to write changes to the database
 * and currently there is only one writer per group of segmates.
 *
 * It defaults to false, but may be specified as true using a connect option.
 * This should only ever be set by a QD connecting to a QE, rather than
 * directly.
 */
extern bool Gp_is_writer;

/* Parameter gp_session_id
 *
 * This run time parameter indicates a unique id to identify a particular user
 * session throughout the entire Greenplum array.
 */
extern int gp_session_id;

/* The Hostname where this segment's QD is located. This variable is NULL for the QD itself */
extern char * qdHostname;

/*
 * Allow callback query?
 */
extern bool gp_enable_functions;

/*How many gangs to keep around from stmt to stmt.*/
extern int			gp_cached_gang_threshold;

/*
 * gp_reject_percent_threshold
 *
 * When reject limit in single row error handling is specified in PERCENT,
 * we don't want to start calculating the percent of rows rejected right
 * from the 1st row, since for example, if the second row is bad this will
 * mean that 50% of the data is bad even if all the following rows are valid.
 * Therefore we want to keep track of count of bad rows but start calculating
 * the percent at a later stage in the game. By default we'll start the
 * calculation after 100 rows were processed, but this GUC can be set for
 * some other number if it makes more sense in certain situations.
 */
extern int			gp_reject_percent_threshold;

/*
 * gp_max_csv_line_length
 *
 * maximum csv line length for COPY and external tables. It is best to keep
 * the default value of 64K to as it protects against a never ending rejection
 * of valid csv data as a result of a quote related formatting error. however,
 * there may be cases where data lines are indeed larger than 64K and that's
 * when this guc must be increased in order to be able to load the data.
 */
extern int 			gp_max_csv_line_length;

/*
 * For use while debugging DTM issues: alter MVCC semantics such that
 * "invisible" rows are returned.
 */
extern bool           gp_select_invisible;

/*
 * MPP-4145: convert certain delete statements into truncate statements.
 */
extern bool           gp_enable_delete_as_truncate;

/*
 * MPP-6926: Resource Queues on by default
 */
#define GP_DEFAULT_RESOURCE_QUEUE_NAME "pg_default"

/**
 * Hash-join node releases hash table when it returns last tuple.
 */
extern bool gp_eager_hashtable_release;

/* Parameter debug_print_combocid_detail
 *
 * This run-time parameter requests extra details when we hit the combocid
 * limit in combocid.c
 */
extern bool Debug_print_combocid_detail;

/* Parameter gp_debug_pgproc
 *
 * This run-time parameter requests to print out detailed info relevant to
 * PGPROC.
 */
extern bool gp_debug_pgproc;

/* Parameter debug_print_prelim_plan
 *
 * This run-time parameter is closely related to the PostgreSQL parameter
 * debug_print_plan which, if true, causes the final plan to display on the
 * server log prior to execution.  This parameter, if true, causes the
 * preliminary plan (from the optimizer prior to cdbparallelize) to display
 * on the log.
 */
extern bool Debug_print_prelim_plan;

/* Parameter debug_print_slice_table
 *
 * This run-time parameter, if true, causes the slice table for a plan to
 * display on the server log.  On a QD this occurs prior to sending the
 * slice table to any QE.  On a QE it occurs just prior to execution of
 * the plan slice.
 */
extern bool Debug_print_slice_table;

/* Parameter debug_print_dispatch_plan
 *
 * This run-time parameter is closely related to the PostgreSQL parameter
 * debug_print_plan which, if true, causes the final plan to display on the
 * server log prior to execution. In GPDB, some plan changes occur after
 * planning just prior to dispatch. This parameter, if true, causes the
 * dispatchable plan to display on the log.
 */
extern bool Debug_print_dispatch_plan;

/* Parameter debug_print_plannedstmt
 *
 * This run-time parameter causes the PlannedStmt structure containing 
 * the final plan to display on the server log prior to execution. 
 */
extern bool Debug_print_plannedstmt;

/*
 * gp_backup_directIO
 *
 * when set to 'true' the dump with direct I\O is enabled
 */
extern bool gp_backup_directIO;


/*
 * gp_backup_directIO_readChunk
 *
 *  This parameter controls the readChunk that is used during directI\O backup
 *  The value of this parameter is in megabyte
 */
extern int gp_backup_directIO_read_chunk_mb;

/*
 * gp_external_enable_exec
 *
 * when set to 'true' external tables that were defined with an EXECUTE
 * clause (will execute OS level commands) are allowed to be SELECTed from.
 * When set to 'false' trying to SELECT from such a table will result in an
 * error. Default is 'true'
 */
extern bool gp_external_enable_exec;

/*
 * gp_external_grant_privileges
 *
 * when set to 'false' only superusers can create external tables.
 * when set to 'true' http, gpfdist and gpfdists external tables are also allowed to be
 * created by non superusers. Default is 'false'
 */
extern bool gp_external_grant_privileges;

/* gp_external_max_segs
 *
 * The maximum number of segment databases that will get assigned to
 * connect to a specific gpfdist (or gpfdists) client (1 gpfdist:// URI). The limit
 * is good to have as there is a number of segdbs that as we increase
 * it won't add to performance but may actually decrease it, while
 * occupying unneeded resources. By having a limit we can control our
 * server resources better while not harming performance.
 */
extern int gp_external_max_segs;

/*
 * gp_command_count
 *
 * This GUC is 0 at the beginning of a client session and
 * increments for each command received from the client.
 */
extern int gp_command_count;

/*
 * gp_safefswritesize
 *
 * should only be set to <N bytes> on file systems that the so called
 * 'torn-write' problem may occur. This guc should be set for the minimum
 * write size that is always safe on this particular file system. The side
 * effect of increasing this value for torn write protection is that single row
 * INSERTs into append only tables will use <N bytes> of space and therefore
 * also slow down SELECTs and become strongly discouraged.
 *
 * On mature file systems where torn write may not occur this GUC should be
 * set to 0 (the default).
 */
extern int gp_safefswritesize;

/*
 * Request a pause to the fault-prober.
 */
extern bool gp_fts_probe_pause;

extern bool gpvars_assign_gp_fts_probe_pause(bool newval, bool doit, GucSource source);


/*
 * Parameter gp_connections_per_thread
 *
 * The run-time parameter (GUC variables) gp_connections_per_thread
 * controls how many libpq connections to qExecs are processed in each
 * thread.
 *
 * Any number >= 1 is valid.
 *
 * 1 means each connection has its own thread.
 *
 * This can be set in the config file, or at runtime by a superuser using
 * SQL: set gp_connections_per_thread = x;
 *
 * The default is 256.	So, if there are fewer than 256 segdbs, all would be handled
 * by the same thread.
 *
 * Currently, this is used in two situation:
 *		1) In cdblink_setup, when the libpq connections are obtained by the dispatcher
 *				to the qExecs.
 *		2) In CdbDispatchCommand, when commands are sent from the dispatcher to the qExecs.
 *
 */
extern int	gp_connections_per_thread; /* GUC var - server operating mode.  */

extern bool assign_gp_connections_per_thread(int newval, bool doit, GucSource source);
extern const char *show_gp_connections_per_thread(void);

/*
 * If number of subtransactions within a transaction exceed this limit,
 * then a warning is given to the user.
 */
extern int32 gp_subtrans_warn_limit;

extern char *gp_fault_action_string;	/* Use by guc.c as user defined fault
										 * action */
extern bool gp_set_read_only;
extern const char *role_to_string(GpRoleValue role);

extern int	gp_segment_connect_timeout; /* GUC var - timeout specifier for gang creation */
extern int  gp_snapshotadd_timeout; /* GUC var - timeout specifier for snapshot-creation wait */

extern int	gp_fts_probe_retries; /* GUC var - specifies probe number of retries for FTS */
extern int	gp_fts_probe_timeout; /* GUC var - specifies probe timeout for FTS */
extern int	gp_fts_probe_interval; /* GUC var - specifies polling interval for FTS */
extern int	gp_fts_probe_threadcount; /* GUC var - specifies number of threads to use for FTS probes */
extern bool	gp_fts_transition_parallel; /* GUC var - controls parallel segment transition for FTS */

/*
 * Parameter Gp_max_packet_size
 *
 * The run-time parameter gp_max_packet_size controls the largest packet
 * size that the interconnect will transmit.  In general, the interconnect
 * tries to squeeze as many tuplechunks as possible into a packet without going
 * above this size limit.
 *
 * The largest tuple chunk size that the system will form can be derived
 * from this with:
 *		MAX_CHUNK_SIZE = Gp_max_packet_size - PACKET_HEADER_SIZE - TUPLE_CHUNK_HEADER_SIZE
 *
 *
 */
extern int	Gp_max_packet_size;	/* GUC var */

#define DEFAULT_PACKET_SIZE 8192
#define MIN_PACKET_SIZE 512
#define MAX_PACKET_SIZE 65507 /* Max payload for IPv4/UDP (subtract 20 more for IPv6 without extensions) */

/*
 * Support for multiple "types" of interconnect
 */

#define INTERCONNECT_TYPE_TCP (0)
#define INTERCONNECT_TYPE_UDP (1)
#define INTERCONNECT_TYPE_NIL (2)

extern int Gp_interconnect_type;

extern const char *gpvars_assign_gp_interconnect_type(const char *newval, bool doit, GucSource source __attribute__((unused)) );
extern const char *gpvars_show_gp_interconnect_type(void);

#define INTERCONNECT_FC_METHOD_CAPACITY (0)
#define INTERCONNECT_FC_METHOD_LOSS     (2)

extern int Gp_interconnect_fc_method;

extern const char *gpvars_assign_gp_interconnect_fc_method(const char *newval, bool doit, GucSource source __attribute__((unused)) );
extern const char *gpvars_show_gp_interconnect_fc_method(void);

/*
 * Parameter Gp_interconnect_queue_depth
 *
 * The run-time parameter Gp_interconnect_queue_depth controls the
 * number of outstanding messages allowed to accumulated on a
 * 'connection' before the peer will start to backoff.
 *
 * This guc is specific to the UDP-interconnect.
 *
 */
extern int	Gp_interconnect_queue_depth;

/*
 * Parameter Gp_interconnect_snd_queue_depth
 *
 * The run-time parameter Gp_interconnect_snd_queue_depth controls the
 * average number of outstanding messages on the sender side
 *
 * This guc is specific to the UDP-interconnect.
 *
 */
extern int	Gp_interconnect_snd_queue_depth;
extern int	Gp_interconnect_timer_period;
extern int	Gp_interconnect_timer_checking_period;
extern int	Gp_interconnect_default_rtt;
extern int	Gp_interconnect_min_rto;
extern int  Gp_interconnect_transmit_timeout;
extern int	Gp_interconnect_min_retries_before_timeout;

/* UDP recv buf size in KB.  For testing */
extern int 	Gp_udp_bufsize_k;

/*
 * Parameter Gp_interconnect_hash_multiplier
 *
 * The run-time parameter Gp_interconnect_hash_multiplier
 * controls the number of hash buckets used to track 'connections.'
 *
 * This guc is specific to the UDP-interconnect.
 *
 */
extern int	Gp_interconnect_hash_multiplier;

/*
 * Parameter gp_interconnect_aggressive_retry
 *
 * The run-time parameter gp_interconnect_aggressive_retry controls the
 * activation of the application-level retry (which acts much faster than the OS-level
 * TCP retries); In most cases this should stay enabled.
 */
extern bool gp_interconnect_aggressive_retry; /* fast-track app-level retry */

/*
 * Parameter gp_interconnect_full_crc
 *
 * Perform a full CRC on UDP-packets as they depart and arrive.
 */
extern bool gp_interconnect_full_crc;

/*
 * Parameter gp_interconnect_elide_setup
 *
 * Perform a full initial handshake for every statement ?
 */
extern bool gp_interconnect_elide_setup;

/*
 * Parameter gp_interconnect_log_stats
 *
 * Emit inteconnect statistics at log-level, instead of debug1
 */
extern bool gp_interconnect_log_stats;

extern bool gp_interconnect_cache_future_packets;

#define UNDEF_SEGMENT -2

/*
 * Parameter interconnect_setup_timeout
 *
 * The run-time parameter (GUC variable) interconnect_setup_timeout is used
 * during SetupInterconnect() to timeout the operation.  This value is in
 * seconds.  Setting it to zero effectively disables the timeout.
 */
extern int	interconnect_setup_timeout;

#ifdef USE_ASSERT_CHECKING
/*
 * UDP-IC Test hooks (for fault injection).
 */
extern int gp_udpic_dropseg; /* specifies which segments to apply the
							  * following two gucs -- if set to
							  * UNDEF_SEGMENT, all segments apply
							  * them. */
extern int gp_udpic_dropxmit_percent;
extern int gp_udpic_dropacks_percent;
extern int gp_udpic_fault_inject_percent;
extern int gp_udpic_fault_inject_bitmap;
extern int gp_udpic_network_disable_ipv6;

/*
 * Filesystem Test Hooks (for fault injection)
 */
extern int gp_fsys_fault_inject_percent;
#endif

/*
 * Each slice table has a unique ID (certain commands like "vacuum
 * analyze" run many many slice-tables for each gp_command_id). This
 * gets passed around as part of the slice-table (not used as a guc!).
 */
extern uint32 gp_interconnect_id;

/* --------------------------------------------------------------------------------------------------
 * Logging
 */

typedef enum GpVars_Verbosity
{
	GPVARS_VERBOSITY_UNDEFINED = 0,
    GPVARS_VERBOSITY_OFF,
	GPVARS_VERBOSITY_TERSE,
	GPVARS_VERBOSITY_VERBOSE,
    GPVARS_VERBOSITY_DEBUG,
} GpVars_Verbosity;

GpVars_Verbosity
gpvars_string_to_verbosity(const char *s);
const char *
gpvars_verbosity_to_string(GpVars_Verbosity verbosity);

/* Enable single-slice single-row inserts. */
extern bool gp_enable_fast_sri;

/* Enable single-mirror pair dispatch. */
extern bool gp_enable_direct_dispatch;

/* Name of pseudo-function to access any table as if it was randomly distributed. */
#define GP_DIST_RANDOM_NAME "GP_DIST_RANDOM"

/*
 * gp_log_gang (string)
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
extern GpVars_Verbosity    gp_log_gang;

const char *gpvars_assign_gp_log_gang(const char *newval, bool doit, GucSource source);
const char *gpvars_show_gp_log_gang(void);

/*
 * gp_log_fts (string)
 *
 * What kind of messages should be logged by the fault-prober
 * "OFF"     -> only errors are logged
 * "TERSE"   -> terse logging of routine events
 * "VERBOSE" -> more messages
 * "DEBUG"   -> additional events are logged at severity level DEBUG1 to DEBUG5
 *
 * The messages that are enabled by the TERSE and VERBOSE settings are
 * written with a severity level of LOG.
 */
extern GpVars_Verbosity    gp_log_fts;

const char *gpvars_assign_gp_log_fts(const char *newval, bool doit, GucSource source);
const char *gpvars_show_gp_log_fts(void);

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
extern GpVars_Verbosity    gp_log_interconnect;

const char *gpvars_assign_gp_log_interconnect(const char *newval, bool doit, GucSource source __attribute__((unused)) );
const char *gpvars_show_gp_log_interconnect(void);


/* --------------------------------------------------------------------------------------------------
 * Resource management
 */

/*
 * gp_process_memory_cutoff (real)
 *
 * Deprecated.  Will remove in next release.
 */
extern double   gp_process_memory_cutoff;           /* SET/SHOW in units of kB */

/* --------------------------------------------------------------------------------------------------
 * Greenplum Optimizer GUCs
 */


/*
 * enable_adaptive_nestloop
 */
extern bool enable_adaptive_nestloop;

/*
 * "gp_motion_cost_per_row"
 *
 * If >0, the planner uses this value -- instead of 2 * "cpu_tuple_cost" --
 * for Motion operator cost estimation.
 */
extern double   gp_motion_cost_per_row;

/*
 * "gp_segments_for_planner"
 *
 * If >0, number of segment dbs for the planner to assume in its cost and size
 * estimates.  If 0, estimates are based on the actual number of segment dbs.
 */
extern int      gp_segments_for_planner;

/*
 * "gp_enable_multiphase_agg"
 *
 * Unlike some other enable... vars, gp_enable_multiphase_agg is not cost based.
 * When set to false, the planner will not use multi-phase aggregation.
 */
extern bool gp_enable_multiphase_agg;

extern bool fast_path_expressions;

/*
 * Perform a post-planning scan of the final plan looking for motion deadlocks:
 * emit verbose messages about any found.
 */
extern bool gp_enable_motion_deadlock_sanity;

/*
 * Adjust selectivity for nulltests atop of outer joins;
 * Special casing prominent use case to work around lack of (NOT) IN subqueries
 */
extern bool gp_adjust_selectivity_for_outerjoins;

/*
 * Target density for hash-node (HJ).
 */
extern int gp_hashjoin_tuples_per_bucket;
extern int gp_hashagg_groups_per_bucket;

/*
 * Capping the amount of memory used for metadata (buckets and batches pointers)
 * for spilling HashJoins. This is in addition to the operator memory quota,
 * which is used only for storing tuples (MPP-22417)
 */
extern int gp_hashjoin_metadata_memory_percent;

/*
 * Damping of selectivities of clauses which pertain to the same base
 * relation; compensates for undetected correlation
 */
extern bool gp_selectivity_damping_for_scans;

/*
 * Damping of selectivities of clauses in joins
 */
extern bool gp_selectivity_damping_for_joins;

/*
 * Damping factor for selecticity damping
 */
extern double gp_selectivity_damping_factor;

/*
 * Sort selectivities by significance before applying
 * damping (ON by default)
 */
extern bool gp_selectivity_damping_sigsort;


/* ----- Internal Features ----- */

/*
 * gp_enable_alter_table_inherit_cols
 *
 * Allows the use of the ALTER TABLE client INHERIT parent [( column_list )]
 * optional column_list.  If this variable is false (the default), use of
 * the optional column_list.  This variable is used by pg_dump --gp-migrator
 * to enable use of the extended syntax.
 */
extern bool gp_enable_alter_table_inherit_cols;


/* ----- Experimental Features ----- */

/*
 * "gp_enable_agg_distinct"
 *
 * May Greenplum redistribute on the argument of a lone aggregate distinct in
 * order to use 2-phase aggregation?
 *
 * The code does uses planner estimates to decide whether to use this feature,
 * when enabled.
 */
extern bool gp_enable_agg_distinct;

/*
 * "gp_enable_agg_distinct_pruning"
 *
 * May Greenplum use grouping in the first phases of 3-phase aggregation to
 * prune values from DISTINCT-qualified aggregate function arguments?
 *
 * The code uses planner estimates to decide whether to use this feature,
 * when enabled.  See, however, gp_eager_dqa_pruning.
 */
extern bool gp_enable_dqa_pruning;

/*
 * "gp_eager_agg_distinct_pruning"
 *
 * Should Greenplum bias planner estimates so as to favor the use of grouping
 * in the first phases of 3-phase aggregation to prune values from DISTINCT-
 * qualified aggregate function arguments?
 *
 * Note that this has effect only when gp_enable_dqa_pruning it true.  It
 * provided to facilitate testing and is not a tuning parameter.
 */
extern bool gp_eager_dqa_pruning;

/*
 * "gp_eager_one_phase_agg"
 *
 * Should Greenplum bias planner estimates so as to favor the use of one
 * phase aggregation?
 *
 * It is provided to facilitate testing and is not a tuning parameter.
 */
extern bool gp_eager_one_phase_agg;

/*
 * "gp_eager_two_phase_agg"
 *
 * Should Greenplum bias planner estimates so as to favor the use of two
 * phase aggregation?
 *
 * It is provided to facilitate testing and is not a tuning parameter.
 */
extern bool gp_eager_two_phase_agg;

/*
 * "gp_enable_groupext_distinct_pruning"
 *
 * Should Greenplum bias planner estimates so as to favor the use of
 * grouping in the first phases of 3-phase aggregation to prune values
 * from DISTINCT-qualified aggregate function arguments on a grouping
 * extension query?
 */
extern bool gp_enable_groupext_distinct_pruning;

/*
 * "gp_enable_groupext_distinct_gather"
 *
 * Should Greenplum bias planner estimates so as to favor the use of
 * gathering motion to gather the data into a single node to compute
 * DISTINCT-qualified aggregates on a grouping extension query?
 */
extern bool gp_enable_groupext_distinct_gather;

/*
 * "gp_distinct_grouping_sets_threshold"
 *
 * The planner will treat gp_enable_groupext_distinct_pruning as 'off'
 * when the number of grouping sets that have been rewritten based
 * on the multi-phrase aggregation exceeds the threshold value here divided by
 * the number of distinct-qualified aggregates.
 */
extern int gp_distinct_grouping_sets_threshold;

/* May Greenplum apply Unique operator (and possibly a Sort) in parallel prior
 * to the collocation motion for a Unique operator?  The idea is to reduce
 * the number of rows moving over the interconnect.
 *
 * The code uses planner estimates for this.  If enabled, the tactic is used
 * only if it is estimated to be cheaper that a 1-phase approach.  However,
 * see gp_eqger_preunique.
 */
extern bool gp_enable_preunique;

/* If gp_enable_preunique is true, then  apply the associated optimzation
 * in an "eager" fashion.  In effect, this setting overrides the cost-
 * based decision whether to use a 2-phase approach to duplicate removal.
 */
extern bool gp_eager_preunique;

/* May Greenplum use sequential window plans instead of parallel window
 * plans? (OLAP Experimental.)
 *
 * The code does not currently use planner estimates for this.  If enabled,
 * the tactic is used whenever possible.
 */
extern bool gp_enable_sequential_window_plans;

/* May Greenplum dump statistics for all segments as a huge ugly string
 * during EXPLAIN ANALYZE?
 *
 */
extern bool gp_enable_explain_allstat;

/* May Greenplum restrict ORDER BY sorts to the first N rows if the ORDER BY
 * is wrapped by a LIMIT clause (where N=OFFSET+LIMIT)?
 *
 * The code does not currently use planner estimates for this.  If enabled,
 * the tactic is used whenever possible.
 */
extern bool gp_enable_sort_limit;

/* May Greenplum discard duplicate rows in sort if it is is wrapped by a
 * DISTINCT clause (unique aggregation operator)?
 *
 * The code does not currently use planner estimates for this.  If enabled,
 * the tactic is used whenever possible.
 */
extern bool gp_enable_sort_distinct;

/* Greenplum MK Sort */
extern bool gp_enable_mk_sort;
extern bool gp_enable_motion_mk_sort;

#ifdef USE_ASSERT_CHECKING
extern bool gp_mk_sort_check;
#endif

extern bool trace_sort;

/* Generic Greenplum sort flag for testing.
 *
 *
 */
extern int gp_sort_flags;
extern int gp_dbg_flags;

/* If Greenplum is discarding duplicate rows in sort, switch back to
 * standard sort if the number of distinct values exceeds max_distinct.
 * (If the number of distinct values is too large the behavior of the
 * insertion sort is inferior to the heapsort)
 */
extern int gp_sort_max_distinct;

/* turn the hash partitioned tables on */

extern bool	gp_enable_hash_partitioned_tables;

/**
 * Enable dynamic pruning of partitions based on join condition.
 */
extern bool gp_dynamic_partition_pruning;

/**
 * Sharing of plan fragments for common table expressions
 */
extern bool gp_cte_sharing;

/* turn SQL/MED functionality on */

extern bool	gp_foreign_data_access;

/* MPP-7770: disallow altering storage using SET WITH */

extern bool	gp_setwith_alter_storage;

/* let tablespace make missing LOCATION directory if necessary */

extern bool	gp_enable_tablespace_auto_mkdir;

/* MPP-9772, MPP-9773: remove support for CREATE INDEX CONCURRENTLY */
extern bool	gp_create_index_concurrently;

/* Priority for the segworkers relative to the postmaster's priority */
extern int gp_segworker_relative_priority;

/*  Max size of dispatched plans; 0 if no limit */
extern int gp_max_plan_size;

/* The maximum number of times on average that the hybrid hashed aggregation
 * algorithm will plan to spill an input row to disk before including it in
 * an aggregation.  Increasing this parameter will cause the planner to choose
 * hybrid hashed aggregation at lower settings of work_mem than it otherwise
 * would.
 */
extern double gp_hashagg_rewrite_limit;

/* Before the hybrid hash aggregator starts to spill it can
 * re-evaluate the density of groups in its input to come up with a
 * (possibly better) batching scheme */
extern bool gp_hashagg_recalc_density;

/* If we use two stage hashagg, we can stream the bottom half */
extern bool gp_hashagg_streambottom;

/* The default number of batches to use when the hybrid hashed aggregation
 * algorithm (re-)spills in-memory groups to disk.
 */
extern int gp_hashagg_default_nbatches;

/* Hashagg spill: minimum number of spill batches */
extern int gp_hashagg_spillbatch_min;
/* Hashagg spill: max number of spill batches */
extern int gp_hashagg_spillbatch_max;

/* Hashjoin use bloom filter */
extern int gp_hashjoin_bloomfilter;

/* Get statistics for partitioned parent from a child */
extern bool 	gp_statistics_pullup_from_child_partition;

/* Extract numdistinct from foreign key relationship */
extern bool		gp_statistics_use_fkeys;

/* Analyze related gucs */
extern int 		gp_statistics_blocks_target;
extern double	gp_statistics_ndistinct_scaling_ratio_threshold;
extern double	gp_statistics_sampling_threshold;

/* Analyze tools */
extern int gp_motion_slice_noop;
#ifdef ENABLE_LTRACE
extern int gp_ltrace_flag;
#endif

/* Disable setting of hint-bits while reading db pages */
extern bool gp_disable_tuple_hints;

/* Enable gpmon */
extern bool gpvars_assign_gp_enable_gpperfmon(bool newval, bool doit, GucSource source);
extern bool gpvars_assign_gp_gpperfmon_send_interval(int newval, bool doit, GucSource source);
extern bool gp_enable_gpperfmon;
extern int gp_gpperfmon_send_interval;
extern bool force_bitmap_table_scan;

extern int gp_hashagg_compress_spill_files;
extern int gp_workfile_compress_algorithm;
extern bool gp_workfile_checksumming;
extern bool gp_workfile_caching;
extern bool gp_metadata_versioning;
extern double gp_workfile_limit_per_segment;
extern double gp_workfile_limit_per_query;
extern int gp_workfile_limit_files_per_query;
extern int gp_workfile_caching_loglevel;
extern int gp_sessionstate_loglevel;
extern bool gp_workfile_faultinject;
extern int gp_workfile_bytes_to_checksum;
/* The type of work files that HashJoin should use */
extern int gp_workfile_type_hashjoin;

/* Disable logging while creating mapreduce views */
extern bool gp_mapreduce_define;
extern bool coredump_on_memerror;

/* if catquery.c is built with the logquery option, allow caql logging */
extern bool	gp_enable_caql_logging;

/* Autostats feature for MPP-4082. */
typedef enum
{
	GP_AUTOSTATS_NONE = 0,		/* Autostats is switched off */
	GP_AUTOSTATS_ON_CHANGE,		/* Autostats is enabled on change (insert/delete/update/ctas) */
	GP_AUTOSTATS_ON_NO_STATS,		/* Autostats is enabled on ctas or copy or insert if no stats are present */
} GpAutoStatsModeValue;
extern GpAutoStatsModeValue gp_autostats_mode;
extern char                *gp_autostats_mode_string;
extern GpAutoStatsModeValue gp_autostats_mode_in_functions;
extern char                *gp_autostats_mode_in_functions_string;
extern int                  gp_autostats_on_change_threshold;
extern bool					log_autostats;
/* hook functions to set gp_autostats_mode and gp_autostats_mode_in_functions */
extern const char *gpvars_assign_gp_autostats_mode(const char *newval, bool doit, GucSource source);
extern const char *gpvars_show_gp_autostats_mode(void);
extern const char *gpvars_assign_gp_autostats_mode_in_functions(const char *newval, bool doit, GucSource source);
extern const char *gpvars_show_gp_autostats_mode_in_functions(void);


/* --------------------------------------------------------------------------------------------------
 * Miscellaneous developer use
 */

/*
 * gp_dev_notice_agg_cost (bool)
 *
 * Issue aggregation optimizer cost estimates as NOTICE level messages
 * during GPDB aggregation planning.  This is intended to facilitate
 * tuning the cost algorithm.  The presence of this switch should not
 * be published.  The name is intended to suggest that it is for developer
 * use only.
 */
extern bool  gp_dev_notice_agg_cost;

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
extern int  gp_debug_linger;

/* --------------------------------------------------------------------------------------------------
 * Non-GUC globals
 */

#define UNSET_SLICE_ID -1
extern int	currentSliceId;

/* Segment id where singleton gangs are to be dispatched. */
extern int  gp_singleton_segindex;

/* Enable ading the cost for walking the chain in the hash join. */
extern bool gp_cost_hashjoin_chainwalk;

typedef struct GpId
{
	int4		numsegments;	/* count of distinct segindexes */
	int4		dbid;			/* the dbid of this database */
	int4		segindex;		/* content indicator: -1 for entry database,
								 * 0, ..., n-1 for segment database *
								 * a primary and its mirror have the same segIndex */
} GpId;

/* --------------------------------------------------------------------------------------------------
 * Global variable declaration for the data for the single row of gp_id table
 */
extern GpId GpIdentity;
#define UNINITIALIZED_GP_IDENTITY_VALUE (-10000)
#define AmActiveMaster()	AmIMaster() //(GpIdentity.segindex == MASTER_CONTENT_ID && GpIdentity.dbid == MASTER_DBID)
#define AmStandbyMaster()	AmIStandby() //(GpIdentity.segindex == MASTER_CONTENT_ID && GpIdentity.dbid != MASTER_DBID)

/* Stores the listener port that this process uses to listen for incoming
 * Interconnect connections from other Motion nodes.
 */
extern int	Gp_listener_port;



/* SequenceServer information to be shared with everyone */
typedef struct SeqServerControlBlock
{
	int4		seqServerPort;
	XLogRecPtr  lastXlogEntry;
}	SeqServerControlBlock;

extern SeqServerControlBlock *seqServerCtl;

/*
 * Thread-safe routine to write to the log
 */
extern void write_log(const char *fmt,...) __attribute__((format(printf, 1, 2)));

/* format timestamp into string */
extern void get_timestamp(char * strfbuf, int length);

/* control current usability of enabling hash index */
extern bool gpvars_assign_gp_hash_index(bool newval, bool doit, GucSource source);

/* wire off SET WITH() for alter table distributed by */
extern bool gp_disable_atsdb_set_with;

extern const char *gpvars_assign_gp_resqueue_memory_policy(const char *newval, bool doit, GucSource source __attribute__((unused)) );
extern const char *gpvars_show_gp_resqueue_memory_policy(void);

extern bool gpvars_assign_statement_mem(int newval, bool doit, GucSource source __attribute__((unused)) );

extern void increment_command_count(void);

/*
 * switch to control inverse distribution function strategy.
 */
extern char *gp_idf_deduplicate_str;

/* Force query use data directory for temporary files. */
extern bool	gp_force_use_default_temporary_directory;
extern int	gp_temporary_directory_mark_error;

/* HAWQ 2.0 GUCs */

extern bool optimizer_enforce_hash_dist_policy;
extern int default_segment_num;
extern int appendonly_split_write_size_mb;
extern int split_read_size_mb;
extern int enforce_virtual_segment_number;
extern bool debug_print_split_alloc_result;
extern bool output_hdfs_block_location;
extern bool metadata_cache_enable;
extern bool prefer_datalocality_to_iobalance;
extern bool balance_on_partition_table_level;
extern bool balance_on_whole_query_level;
extern int metadata_cache_block_capacity;
extern int debug_fix_vseg_num;
/* The 5 gucs below related to metadatacache_test.*/
extern int metadata_cache_check_interval;
extern int metadata_cache_refresh_interval;
extern int metadata_cache_refresh_timeout;
extern int metadata_cache_refresh_max_num;
extern double metadata_cache_free_block_max_ratio;
extern double metadata_cache_free_block_normal_ratio;

extern char *metadata_cache_testfile;
extern bool debug_fake_datalocality;
extern bool datalocality_remedy_enable;
extern bool get_tmpdir_from_rm;
extern bool debug_fake_segmentnum;

/* New HAWQ 2.0 basic GUCs */
extern char   *master_addr_host;
extern int     master_addr_port;
extern char   *standby_addr_host;
extern int     seg_addr_port;
extern char   *dfs_url;
extern char   *master_directory;
extern char   *seg_directory;

/* HAWQ 2.0 resource manager GUCs */
extern int	   rm_master_domain_port;
extern int     rm_master_port;
extern int	   rm_segment_port;

extern char   *rm_global_rm_type;

extern char   *rm_seg_memory_use;
extern double  rm_seg_core_use;

extern char   *rm_grm_yarn_rm_addr;
extern char	  *rm_grm_yarn_sched_addr;
extern char   *rm_grm_yarn_queue;
extern char   *rm_grm_yarn_app_name;
extern int	   rm_grm_breath_return_percentage;

extern char   *rm_stmt_vseg_mem_str;
extern int	   rm_stmt_nvseg;

extern int     rm_nvseg_perquery_limit;
extern int	   rm_nvseg_perquery_perseg_limit;
extern int	   rm_nslice_perseg_limit;
extern int	   rm_seg_container_default_waterlevel;

extern int     rm_session_lease_timeout;
extern bool    rm_session_lease_heartbeat_enable;

extern int 	   rm_query_resource_noresource_timeout;
extern int	   rm_resource_timeout;
extern int	   rm_resource_heartbeat_interval;
extern int	   rm_tolerate_nseg_limit;
extern int	   rm_rejectrequest_nseg_limit;
extern int	   rm_nvseg_variance_among_seg_limit;
extern char   *rm_resourcepool_test_filename;
extern bool	   rm_force_fifo_queue;

extern bool	   rm_enforce_cpu_enable;
extern bool    rm_enforce_blkio_enable;
extern char   *rm_enforce_cgrp_mnt_pnt;
extern char   *rm_enforce_cgrp_hier_name;
extern double  rm_enforce_qry_cost_thld;
extern double  rm_enforce_cpu_weight;
extern double  rm_enforce_blkio_weight;
extern double  rm_enforce_core_vpratio;
extern double  rm_enforce_disk_vpratio;
extern int     rm_enforce_cleanup_period;
extern int     rm_enforce_cgrp_timeout;
extern int     rm_allocation_policy;

extern char   *rm_tmp_dirs;
extern char   *rm_master_tmp_dirs;
extern char   *rm_seg_tmp_dirs;

extern int     rm_log_level;
extern int     rm_max_resource_queue_number;

extern int max_filecount_notto_split_segment;
extern int min_datasize_to_combine_segment;
extern int datalocality_algorithm_version;

typedef enum
{
	HASH_TO_RANDOM_BASEDON_DATALOCALITY = 0,
	ENFORCE_HASH_TO_RANDOM ,
	ENFORCE_KEEP_HASH,
} HashToRandomFlagValue;

extern int hash_to_random_flag;
#endif   /* GPVARS_H */
