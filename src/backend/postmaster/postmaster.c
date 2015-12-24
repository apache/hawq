/*-------------------------------------------------------------------------
 *
 * postmaster.c
 *	  This program acts as a clearing house for requests to the
 *	  POSTGRES system.	Frontend programs send a startup message
 *	  to the Postmaster and the postmaster uses the info in the
 *	  message to setup a backend process.
 *
 *	  The postmaster also manages system-wide operations such as
 *	  startup and shutdown. The postmaster itself doesn't do those
 *	  operations, mind you --- it just forks off a subprocess to do them
 *	  at the right times.  It also takes care of resetting the system
 *	  if a backend crashes.
 *
 *	  The postmaster process creates the shared memory and semaphore
 *	  pools during startup, but as a rule does not touch them itself.
 *	  In particular, it is not a member of the PGPROC array of backends
 *	  and so it cannot participate in lock-manager operations.	Keeping
 *	  the postmaster away from shared memory operations makes it simpler
 *	  and more reliable.  The postmaster is almost always able to recover
 *	  from crashes of individual backends by resetting shared memory;
 *	  if it did much with shared memory then it would be prone to crashing
 *	  along with the backends.
 *
 *	  When a request message is received, we now fork() immediately.
 *	  The child process performs authentication of the request, and
 *	  then becomes a backend if successful.  This allows the auth code
 *	  to be written in a simple single-threaded style (as opposed to the
 *	  crufty "poor man's multitasking" code that used to be needed).
 *	  More importantly, it ensures that blockages in non-multithreaded
 *	  libraries like SSL or PAM cannot cause denial of service to other
 *	  clients.
 *
 *
 * Portions Copyright (c) 2005-2009, Greenplum inc
 * Portions Copyright (c) 1996-2009, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  $PostgreSQL: pgsql/src/backend/postmaster/postmaster.c,v 1.505.2.3 2007/02/11 15:12:21 mha Exp $
 *
 *
 * NOTES
 *
 * Initialization:
 *		The Postmaster sets up shared memory data structures
 *		for the backends.
 *
 * Synchronization:
 *		The Postmaster shares memory with the backends but should avoid
 *		touching shared memory, so as not to become stuck if a crashing
 *		backend screws up locks or shared memory.  Likewise, the Postmaster
 *		should never block on messages from frontend clients.
 *
 * Garbage Collection:
 *		The Postmaster cleans up after backends if they have an emergency
 *		exit and/or core dump.
 *
 * Error Reporting:
 *		Use write_stderr() only for reporting "interactive" errors
 *		(essentially, bogus arguments on the command line).  Once the
 *		postmaster is launched, use ereport().	In particular, don't use
 *		write_stderr() for anything that occurs after pmdaemonize.
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include <unistd.h>
#include <signal.h>
#include <time.h>
#include <sys/wait.h>
#include <ctype.h>
#include <sys/stat.h>
#include <sys/socket.h>
#include <fcntl.h>
#include <sys/param.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <netdb.h>
#include <limits.h>

/* headers required for process affinity bindings */
#if defined(pg_on_solaris)
#include <sys/types.h>
#include <sys/processor.h>
#include <sys/procset.h>
#endif
#ifdef HAVE_NUMA_H
#define NUMA_VERSION1_COMPATIBILITY 1
#include <numa.h>
#endif

#ifdef HAVE_SYS_SELECT_H
#include <sys/select.h>
#endif

#ifdef HAVE_GETOPT_H
#include <getopt.h>
#endif

#ifdef USE_BONJOUR
#include <DNSServiceDiscovery/DNSServiceDiscovery.h>
#endif

#include "access/transam.h"
#include "access/xlog.h"
#include "bootstrap/bootstrap.h"
#include "catalog/pg_control.h"
#include "catalog/pg_database.h"
#include "cdb/cdbutil.h"
#include "commands/async.h"
#include "lib/dllist.h"
#include "libpq/auth.h"
#include "libpq/ip.h"
#include "libpq/libpq.h"
#include "libpq/pqsignal.h"
#include "libpq/pqformat.h"
#include "miscadmin.h"
#include "pgstat.h"
#include "postmaster/autovacuum.h"
#include "postmaster/fork_process.h"
#include "postmaster/pgarch.h"
#include "postmaster/postmaster.h"
#include "postmaster/seqserver.h"
#include "postmaster/checkpoint.h"
#include "postmaster/fts.h"
#include "postmaster/perfmon.h"
#include "postmaster/primary_mirror_mode.h"
#include "postmaster/walsendserver.h"
#include "postmaster/walredoserver.h"
#include "postmaster/ddaserver.h"
#include "postmaster/syslogger.h"
#include "postmaster/perfmon_segmentinfo.h"
#include "storage/fd.h"
#include "storage/ipc.h"
#include "storage/pg_shmem.h"
#include "storage/pmsignal.h"
#include "storage/proc.h"
#include "storage/procarray.h"
#include "tcop/tcopprot.h"
#include "utils/builtins.h"
#include "utils/datetime.h"
#include "utils/faultinjector.h"
#include "utils/memutils.h"
#include "utils/ps_status.h"
#include "utils/resscheduler.h"

#include "cdb/cdbgang.h"                /* cdbgang_parse_gpqeid_params */
#include "cdb/cdblogsync.h"
#include "cdb/cdbvars.h"

#include "cdb/cdbfilerep.h"
#include "cdb/cdbmetadatacache.h"

#include "resourcemanager/envswitch.h"
#include "resourcemanager/communication/rmcomm_QD2RM.h"
#include "resourcemanager/resourcemanager.h"
#include "resourcemanager/errorcode.h"

#ifdef EXEC_BACKEND
#include "storage/spin.h"
void SeqServerMain(int argc, char *argv[]);

void FtsProbeMain(int argc, char *argv[]);
#endif


/*
 * List of active backends (or child processes anyway; we don't actually
 * know whether a given child has become a backend or is still in the
 * authorization phase).  This is used mainly to keep track of how many
 * children we have and send them appropriate signals when necessary.
 *
 * "Special" children such as the startup, bgwriter and autovacuum launcher
 * tasks are not in this list.	Autovacuum worker processes are in it.
 * Also, "dead_end" children are in it: these are children launched just
 * for the purpose of sending a friendly rejection message to a would-be
 * client.	We must track them because they are attached to shared memory,
 * but we know they will never become live backends.  dead_end children are
 * not assigned a PMChildSlot.
 */
typedef struct bkend
{
	pid_t		pid;			/* process id of backend */
	long		cancel_key;		/* cancel key for cancels for this backend */
	int			child_slot;		/* PMChildSlot for this backend, if any */
	bool		is_autovacuum;	/* is it an autovacuum process? */
	bool		dead_end;		/* is it going to send an error and quit? */
	Dlelem		elem;			/* list link in BackendList */
} Backend;

static Dllist *BackendList;

/* #define DO_DDA_SERV 1 */
/* CDB */
typedef enum pmsub_type
{
	SeqServerProc = 0,
	WalSendServerProc,
	WalRedoServerProc,
#ifdef DO_DDA_SERV
	DdaServerProc,
#endif
	PerfmonProc,
	PerfmonSegmentInfoProc,
    MetadataCacheProc,
    ResouceManagerProc,
	MaxPMSubType
} PMSubType;

#ifdef EXEC_BACKEND
static Backend *ShmemBackendArray;
#endif

/* The socket number we are listening for connections on */
int			PostPortNumber;
char	   *UnixSocketDir;
char	   *ListenAddresses;

/*
 * ReservedBackends is the number of backends reserved for superuser use.
 * This number is taken out of the pool size given by MaxBackends so
 * number of backend slots available to non-superusers is
 * (MaxBackends - ReservedBackends).  Note what this really means is
 * "if there are <= ReservedBackends connections available, only superusers
 * can make new connections" --- pre-existing superuser connections don't
 * count against the limit.
 */
int			ReservedBackends;

/* The socket(s) we're listening to. */
#define MAXLISTEN	64
static int	ListenSocket[MAXLISTEN];

/*
 * Set by the -o option
 */
static char ExtraOptions[MAXPGPATH];

/*
 * These globals control the behavior of the postmaster in case some
 * backend dumps core.	Normally, it kills all peers of the dead backend
 * and reinitializes shared memory.  By specifying -s or -n, we can have
 * the postmaster stop (rather than kill) peers and not reinitialize
 * shared data structures.	(Reinit is currently dead code, though.)
 */
static bool Reinit = true;
static int	SendStop = false;

/* still more option variables */
bool		EnableSSL = false;
bool		SilentMode = false; /* silent_mode */

int			PreAuthDelay = 0;
int			AuthenticationTimeout = 60;

bool		log_hostname;		/* for ps display and logging */
bool		Log_connections = false;
bool		Db_user_namespace = false;

char	   *bonjour_name;

static PrimaryMirrorMode gInitialMode = PMModeMirrorlessSegment;

/* PIDs of special child processes; 0 when not running */
static pid_t StartupPID = 0,
			StartupPass2PID = 0,
			StartupPass3PID = 0,
			StartupPass4PID = 0,
			BgWriterPID = 0,
			CheckpointPID = 0,
			AutoVacPID = 0,
			PgArchPID = 0,
			PgStatPID = 0,
			SysLoggerPID = 0;

#define StartupPidsAllZero() (StartupPID == 0 || StartupPass2PID == 0 || StartupPass3PID == 0 || StartupPass4PID == 0)

static volatile sig_atomic_t filerep_has_signaled_state_change = false;
static volatile sig_atomic_t filerep_requires_postmaster_reset = false;
static volatile sig_atomic_t filerep_has_signaled_backends_shutdown = false;

/* Startup/shutdown state */
#define			NoShutdown		    0
#define			SmartShutdown	    1
#define			FastShutdown        2
#define         ImmediateShutdown   3

static int	Shutdown = NoShutdown;

/* T if recovering from a backend crash, up to the point of resetting shared memory and starting up again
 * (or going into quiescent mode if it's a primary/mirror that has reset) */
static bool FatalError = false;
static bool RecoveryError = false;		/* T if WAL recovery failed */
static int pmResetCounter = 0;

static bool gHaveCalledOnetimeInitFunctions = false;

/*
 * We use a simple state machine to control startup, shutdown, and
 * crash recovery (which is rather like shutdown followed by startup).
 *
 * After doing all the postmaster initialization work, we enter PM_STARTUP
 * state and the startup process is launched. The startup process begins by
 * reading the control file and other preliminary initialization steps.
 * In a normal startup, or after crash recovery, the startup process exits
 * with exit code 0 and we switch to PM_RUN state.  However, archive recovery
 * is handled specially since it takes much longer and we would like to support
 * hot standby during archive recovery.
 *
 * When the startup process is ready to start archive recovery, it signals the
 * postmaster, and we switch to PM_RECOVERY state. The background writer is
 * launched, while the startup process continues applying WAL.
 * After reaching a consistent point in WAL redo, startup process signals
 * us again, and we switch to PM_RECOVERY_CONSISTENT state. There's currently
 * no difference between PM_RECOVERY and PM_RECOVERY_CONSISTENT, but we
 * could start accepting connections to perform read-only queries at this
 * point, if we had the infrastructure to do that.
 * When archive recovery is finished, the startup process exits with exit
 * code 0 and we switch to PM_RUN state.
 *
 * Normal child backends can only be launched when we are in PM_RUN state.
 * (We also allow it in PM_WAIT_BACKUP state, but only for superusers.)
 * In other states we handle connection requests by launching "dead_end"
 * child processes, which will simply send the client an error message and
 * quit.  (We track these in the BackendList so that we can know when they
 * are all gone; this is important because they're still connected to shared
 * memory, and would interfere with an attempt to destroy the shmem segment,
 * possibly leading to SHMALL failure when we try to make a new one.)
 * In PM_WAIT_DEAD_END state we are waiting for all the dead_end children
 * to drain out of the system, and therefore stop accepting connection
 * requests at all until the last existing child has quit (which hopefully
 * will not be very long).
 *
 * Notice that this state variable does not distinguish *why* we entered
 * states later than PM_RUN --- Shutdown and FatalError must be consulted
 * to find that out.  FatalError is never true in PM_INIT through PM_RUN
 * states, nor in PM_SHUTDOWN states (because we don't enter those states
 * when trying to recover from a crash).
 *
 * RecoveryError means that we have crashed during recovery, and
 * should not try to restart.
 */
typedef enum
{
	PM_INIT,					/* postmaster starting */
	PM_STARTUP,					/* waiting for startup subprocess */
	PM_STARTUP_PASS2, 			/* waiting for startup pass 2 subprocess */
	PM_STARTUP_PASS3,			/* waiting for startup pass 3 subprocess */
	PM_STARTUP_PASS4,			/* waiting for startup pass 4 subprocess */
	PM_RECOVERY,				/* in archive recovery mode */
	PM_RECOVERY_CONSISTENT,		/* consistent recovery mode */
	PM_RUN,						/* normal "database is alive" state */
	//PM_WAIT_BACKUP,				/* waiting for online backup mode to end */

    /** ORDER OF SHUTDOWN ENUMS MATTERS: we move through the states by adding 1 to the current state */
    /* shutdown has begun. */
    PM_CHILD_STOP_BEGIN,

    /* check to make sure startup processes are done */
	PM_CHILD_STOP_WAIT_STARTUP_PROCESSES,

    /* waiting for live backends to exit */
	PM_CHILD_STOP_WAIT_BACKENDS,

	/* waiting for pre-bgwriter services to exit */
	PM_CHILD_STOP_WAIT_PREBGWRITER,

	/* waiting for bgwriter to do shutdown ckpt */
	PM_CHILD_STOP_WAIT_BGWRITER_CHECKPOINT,

    /* waiting for post-bgwriter services */
    PM_CHILD_STOP_WAIT_POSTBGWRITER,

	/* waiting for dead_end children to exit */
	PM_CHILD_STOP_WAIT_DEAD_END_CHILDREN,

	/* all important children have exited */
	PM_CHILD_STOP_WAIT_NO_CHILDREN,

	/* this is NOT a stopping state; this state is used after everything has been stopped for postmaster reset
	 *   during mirroring -- if we should reset the peer as well and start the system back up, we enter this
	 *   state */
	PM_POSTMASTER_RESET_FILEREP_PEER,

    PM__ENUMERATION_COUNT
} PMState;

/**
 * WARNING: some of these labels are used in management scripts -- so don't
 *  change them here without checking the management scripts (check for starting with child_stop_
 *  as well -- management scripts use startWith in some cases)
 */
static const char *const gPmStateLabels[] =
{
	"init",
    "startup",
    "startup_pass2",
    "startup_pass3",
	"startup_pass4",
    "recovery",
    "recovery_consistent",
    "run",

    "child_stop_begin",
    "child_stop_wait_startup_processes",
    "child_stop_wait_backends",
    "child_stop_wait_pre_bgwriter",
    "child_stop_bgwriter_checkpoint",
    "child_stop_post_bgwriter",
    "child_stop_dead_end_children",
    "child_stop_no_children",

    "reset_filerep_peer"
};

typedef enum
{
	PEER_RESET_NONE,
	PEER_RESET_FAILED,
	PEER_RESET_SUCCEEDED,
} PeerResetResult;

static PeerResetResult peerResetResult = PEER_RESET_NONE;

static PMState pmState = PM_INIT;

/**
 * We set this to true to force the state machine to run the actions for transitioning to its current state,
 *  even if the state is not changed
 */
static bool pmStateMachineMustRunActions = false;


/* CDB */

typedef enum PMSUBPROC_FLAGS
{
	PMSUBPROC_FLAG_QD 					= 0x1,
	PMSUBPROC_FLAG_STANDBY 				= 0x2,
	PMSUBPROC_FLAG_QE 					= 0x4,
	PMSUBPROC_FLAG_QD_AND_QE 			= (PMSUBPROC_FLAG_QD|PMSUBPROC_FLAG_QE),
	PMSUBPROC_FLAG_STOP_AFTER_BGWRITER 	= 0x8,
} PMSUBPROC_FLAGS;



typedef struct pmsubproc
{
	pid_t		pid;			/* process id (0 when not running) */
	PMSubType   procType;       /* process type */
	PMSubStartCallback *serverStart; /* server start function */
	char       *procName;
	int        flags;          /* flags indicating in which kind
							    * of instance to start the process */
	bool        cleanupBackend; /* flag if process failure should
								* cause cleanup of backend processes
								* false = do not cleanup (eg. for gpperfmon) */
} PMSubProc;

static PMSubProc PMSubProcList[MaxPMSubType] =
{
	{0, SeqServerProc,
	 (PMSubStartCallback*)&seqserver_start,
	 "seqserver process", PMSUBPROC_FLAG_QD, true},
	{0, WalSendServerProc,
	 (PMSubStartCallback*)&walsendserver_start,
	 "walsendserver process",(PMSUBPROC_FLAG_QD|PMSUBPROC_FLAG_STOP_AFTER_BGWRITER), true},
	{0, WalRedoServerProc,
	 (PMSubStartCallback*)&walredoserver_start,
	 "walredoserver process", PMSUBPROC_FLAG_STANDBY, true},
	{0, PerfmonProc,
	(PMSubStartCallback*)&perfmon_start,
	"perfmon process", PMSUBPROC_FLAG_QD, false},
	{0, PerfmonSegmentInfoProc,
	(PMSubStartCallback*)&perfmon_segmentinfo_start,
	"stats sender process", PMSUBPROC_FLAG_QD_AND_QE, true},
    {0, MetadataCacheProc,
    (PMSubStartCallback*)&metadatacache_start,
    "metadatacache process", PMSUBPROC_FLAG_QD, true},
    {0, ResouceManagerProc,
    (PMSubStartCallback*)&ResManagerProcessStartup,
    "resourcemanager process", PMSUBPROC_FLAG_QD_AND_QE, true},
#ifdef DO_DDA_SERV
	{0, DdaServerProc,
	 (PMSubStartCallback*)&ddaserver_start,
	 "ddaserver process", PMSUBPROC_FLAG_QD_AND_QE, true},
#endif
};


bool		ClientAuthInProgress = false;		/* T during new-client
												 * authentication */

bool		redirection_done = false;	/* stderr redirected for syslogger? */

static volatile bool force_autovac = false; /* received START_AUTOVAC signal */

/*
 * State for assigning random salts and cancel keys.
 * Also, the global MyCancelKey passes the cancel key assigned to a given
 * backend from the postmaster to that backend (via fork).
 */
static unsigned int random_seed = 0;
static struct timeval random_start_time;

extern char *optarg;
extern int	optind,
			opterr;

#ifdef HAVE_INT_OPTRESET
extern int	optreset;			/* might not be declared by system headers */
#endif

/* some GUC values used in fetching status from status transition */
extern char	   *locale_monetary;
extern char	   *locale_numeric;
extern char    *locale_collate;


/*
 * postmaster.c - function prototypes
 */
static void getInstallationPaths(const char *argv0);
static void checkDataDir(void);
static void checkPgDir(const char *dir);
static void checkPgDir2(const char *dir);

#ifdef USE_TEST_UTILS
static void SimExProcExit(void);
#endif /* USE_TEST_UTILS */

#ifdef USE_BONJOUR
static void reg_reply(DNSServiceRegistrationReplyErrorType errorCode,
		  void *context);
#endif
static void pmdaemonize(void);
static Port *ConnCreate(int serverFd);
static void ConnFree(Port *port);

/**
 * @param isReset if true, then this is a reset (as opposed to the initial creation of shared memory on startup)
 */
static void reset_shared(int port, bool isReset);
static void SIGHUP_handler(SIGNAL_ARGS);
static void pmdie(SIGNAL_ARGS);

static volatile int need_call_reaper = 0;

static void reaper(SIGNAL_ARGS);
static void do_reaper(void);
static void do_immediate_shutdown_reaper(void);
static bool ServiceProcessesExist(int excludeFlags);
static bool StopServices(int excludeFlags, int signal);
static char *GetServerProcessTitle(int pid);
static void sigusr1_handler(SIGNAL_ARGS);
static void dummy_handler(SIGNAL_ARGS);
static void CleanupBackend(int pid, int exitstatus, bool resetRequired);
static void HandleChildCrash(int pid, int exitstatus, const char *procname);
static void LogChildExit(int lev, const char *procname,
			 int pid, int exitstatus);
static void PostmasterStateMachine(void);
static void BackendInitialize(Port *port);
static int	BackendRun(Port *port);
static void ExitPostmaster(int status);
static bool ServiceStartable(PMSubProc *subProc);
static int	ServerLoop(void);
static int	BackendStartup(Port *port);
static int	ProcessStartupPacket(Port *port, bool SSLdone);
static void processCancelRequest(Port *port, void *pkt);
static void processPrimaryMirrorTransitionRequest(Port *port, void *pkt);
static void processPrimaryMirrorTransitionQuery(Port *port, void *pkt);
static int	initMasks(fd_set *rmask);
static void report_fork_failure_to_client(Port *port, int errnum);
static enum CAC_state canAcceptConnections(void);
static long PostmasterRandom(void);
static void RandomSalt(char *md5Salt);
static void signal_child(pid_t pid, int signal);
static void SignalSomeChildren(int signal, bool only_autovac);

#define SignalChildren(sig)			SignalSomeChildren(sig, false)
#define SignalAutovacWorkers(sig)	SignalSomeChildren(sig, true)
static int	CountChildren(void);
static bool CreateOptsFile(int argc, char *argv[], char *fullprogname);
static pid_t StartChildProcess(AuxProcType type);

static void setProcAffinity(int id);

#ifdef EXEC_BACKEND

#ifdef WIN32
static pid_t win32_waitpid(int *exitstatus);
static void WINAPI pgwin32_deadchild_callback(PVOID lpParameter, BOOLEAN TimerOrWaitFired);

static HANDLE win32ChildQueue;

typedef struct
{
	HANDLE		waitHandle;
	HANDLE		procHandle;
	DWORD		procId;
} win32_deadchild_waitinfo;

HANDLE		PostmasterHandle;
#endif

static pid_t backend_forkexec(Port *port);
static pid_t internal_forkexec(int argc, char *argv[], Port *port);

/* Type for a socket that can be inherited to a client process */
#ifdef WIN32
typedef struct
{
	SOCKET		origsocket;		/* Original socket value, or -1 if not a
								 * socket */
	WSAPROTOCOL_INFO wsainfo;
} InheritableSocket;
#else
typedef int InheritableSocket;
#endif

typedef struct LWLock LWLock;	/* ugly kluge */

/*
 * Structure contains all variables passed to exec:ed backends
 */
typedef struct
{
	Port		port;
	InheritableSocket portsocket;
	char		DataDir[MAXPGPATH];
	int			ListenSocket[MAXLISTEN];
	long		MyCancelKey;
	int			MyPMChildSlot;
	unsigned long UsedShmemSegID;
	void	   *UsedShmemSegAddr;
	slock_t    *ShmemLock;
	VariableCache ShmemVariableCache;
	Backend    *ShmemBackendArray;
	LWLock	   *LWLockArray;
	PROC_HDR   *ProcGlobal;
	PGPROC	   *AuxiliaryProcs;
	PMSignalData *PMSignalState;
	InheritableSocket pgStatSock;
	pid_t		PostmasterPid;
	TimestampTz PgStartTime;
	TimestampTz PgReloadTime;
	bool		redirection_done;
#ifdef WIN32
	HANDLE		PostmasterHandle;
	HANDLE		initial_signal_pipe;
	HANDLE		syslogPipe[2];
#else
	int			syslogPipe[2];
#endif
	char		my_exec_path[MAXPGPATH];
	char		pkglib_path[MAXPGPATH];
	char		ExtraOptions[MAXPGPATH];
	char		lc_collate[NAMEDATALEN];
	char		lc_ctype[NAMEDATALEN];
}	BackendParameters;

static void read_backend_variables(char *id, Port *port);
static void restore_backend_variables(BackendParameters *param, Port *port);

#ifndef WIN32
static bool save_backend_variables(BackendParameters *param, Port *port);
#else
static bool save_backend_variables(BackendParameters *param, Port *port,
					   HANDLE childProcess, pid_t childPid);
#endif

static void ShmemBackendArrayAdd(Backend *bn);
static void ShmemBackendArrayRemove(Backend *bn);
#endif   /* EXEC_BACKEND */

#define StartupDataBase()		StartChildProcess(StartupProcess)
#define StartupPass2DataBase()	StartChildProcess(StartupPass2Process)
#define StartupPass3DataBase()	StartChildProcess(StartupPass3Process)
#define StartupPass4DataBase()	StartChildProcess(StartupPass4Process)
#define StartBackgroundWriter() StartChildProcess(BgWriterProcess)
#define StartCheckpointServer() StartChildProcess(CheckpointProcess)

static bool SyncMaster = false;

/* Macros to check exit status of a child process */
#define EXIT_STATUS_0(st)  ((st) == 0)
#define EXIT_STATUS_1(st)  (WIFEXITED(st) && WEXITSTATUS(st) == 1)
#define EXIT_STATUS_2(st)  (WIFEXITED(st) && WEXITSTATUS(st) == 2)

static int Save_MppLocalProcessCounter = 0;

bool GPStandby()
{
	return (SyncMaster);
}

/**
 * filerep process is interested in the bg writer's pid, so
 *   all sets of bgwriter pid should go through this function
 *   so that we update shared memory
 */
static void SetBGWriterPID(pid_t pid)
{
    BgWriterPID = pid;
}

/**
 * return true if we are allowed to start actual database processes in this state.
 *
 * Note that this checks whether we are in fault from postmaster reset, so should not be used for
 *      assertions in do_reaper -- because it may have faulted from the time we started the process
 *      to the time the reaper is called
 */
static bool
isFullPostmasterAndDatabaseIsAllowed(void)
{
    return isPrimaryMirrorModeAFullPostmaster(false) && ! isInFaultFromPostmasterReset();
}

/*
 * gpsyncMaster main entry point
 */
int
gpsyncMain(int argc, char *argv[])
{
    SyncMaster = true;
    return PostmasterMain( argc, argv );
}

int
PostmasterGetMppLocalProcessCounter(void)
{
	return Save_MppLocalProcessCounter;
}

static void
signal_child_if_up(int pid, int signal)
{
	if ( pid != 0)
	{
		signal_child(pid, signal);
	}
}

/*
 * Postmaster main entry point
 */
int
PostmasterMain(int argc, char *argv[])
{
	int			opt;
	int			status;
	char       *userDoption = NULL;
	int			i;
	char		stack_base;

	COMPILE_ASSERT(ARRAY_SIZE(gPmStateLabels) == PM__ENUMERATION_COUNT);

	MyProcPid = PostmasterPid = getpid();

	MyStartTime = time(NULL);

	IsPostmasterEnvironment = true;

	/* Set up reference point for stack depth checking */
	stack_base_ptr = &stack_base;

	/*
	 * for security, no dir or file created can be group or other accessible
	 */
	umask((mode_t) 0077);

	/*
	 * Fire up essential subsystems: memory management
	 */
	MemoryContextInit();

	/*
	 * By default, palloc() requests in the postmaster will be allocated in
	 * the PostmasterContext, which is space that can be recycled by backends.
	 * Allocated data that needs to be available to backends should be
	 * allocated in TopMemoryContext.
	 */
	PostmasterContext = AllocSetContextCreate(TopMemoryContext,
											  "Postmaster",
											  ALLOCSET_DEFAULT_MINSIZE,
											  ALLOCSET_DEFAULT_INITSIZE,
											  ALLOCSET_DEFAULT_MAXSIZE);
	MemoryContextSwitchTo(PostmasterContext);

	/* Initialize paths to installation files */
	getInstallationPaths(argv[0]);

	/*
	 * Options setup
	 */
	InitializeGUCOptions();

	opterr = 1;

	/*
	 * Parse command-line options.	CAUTION: keep this in sync with
	 * tcop/postgres.c (the option sets should not conflict) and with the
	 * common help() function in main/main.c.
	 */
	while ((opt = getopt(argc, argv, "A:b:B:C:c:D:d:EeFf:h:ijk:lN:mM:nOo:Pp:r:S:sTt:UW:y:x:z:-:")) != -1)
	{
		switch (opt)
		{
			case 'A':
				SetConfigOption("debug_assertions", optarg, PGC_POSTMASTER, PGC_S_ARGV);
				break;

			case 'B':
				SetConfigOption("shared_buffers", optarg, PGC_POSTMASTER, PGC_S_ARGV);
				break;

			case 'D':
				userDoption = optarg;
				break;

			case 'd':
				set_debug_options(atoi(optarg), PGC_POSTMASTER, PGC_S_ARGV);
				break;

			/*
			 * Normal PostgreSQL used 'E' flag to mean "log_statement='all'",
			 * but we co-opted this letter for this... I'm afraid to change it,
			 * because I don't know where this is used.
             * ... it's used only here in postmaster.c; it causes the postmaster to
             * fork a seqserver process.
			 */
			case 'E':
				/*
				 * 	SetConfigOption("log_statement", "all", PGC_POSTMASTER, PGC_S_ARGV);
				 */
				break;

			case 'e':
				SetConfigOption("datestyle", "euro", PGC_POSTMASTER, PGC_S_ARGV);
				break;

			case 'F':
				SetConfigOption("fsync", "false", PGC_POSTMASTER, PGC_S_ARGV);
				break;

			case 'f':
				if (!set_plan_disabling_options(optarg, PGC_POSTMASTER, PGC_S_ARGV))
				{
					write_stderr("%s: invalid argument for option -f: \"%s\"\n",
								 progname, optarg);
					ExitPostmaster(1);
				}
				break;

			case 'h':
				SetConfigOption("listen_addresses", optarg, PGC_POSTMASTER, PGC_S_ARGV);
				break;

			case 'i':
				SetConfigOption("listen_addresses", "*", PGC_POSTMASTER, PGC_S_ARGV);
				break;

			case 'j':
				/* only used by interactive backend */
				break;

			case 'k':
				SetConfigOption("unix_socket_directory", optarg, PGC_POSTMASTER, PGC_S_ARGV);
				break;

			case 'l':
				SetConfigOption("ssl", "true", PGC_POSTMASTER, PGC_S_ARGV);
				break;

			case 'm':
				/*
				 * In maintenance mode:
				 * 	1. allow DML on catalog table
				 * 	2. allow DML on segments
				 */
				SetConfigOption("maintenance_mode",  	  	"true", PGC_POSTMASTER, PGC_S_ARGV);
				SetConfigOption("allow_segment_DML", 	  	"true", PGC_POSTMASTER, PGC_S_ARGV);
				SetConfigOption("allow_system_table_mods",	"dml",  PGC_POSTMASTER, PGC_S_ARGV);
				break;

			case 'M':

				/* note that M is a postmaster-only option */
				gInitialMode = decipherPrimaryMirrorModeArgument(optarg);
				if (gInitialMode == PMModeUninitialized)
				{
					write_stderr("%s: invalid argument for option -M: \"%s\"\n", progname, optarg);
					ExitPostmaster(1);
				}
				/*
				 * M defines the role of this instance,
				 * it can be master, standby or segment
				 */
				SetSegmentIdentity(optarg);
				break;

			case 'N':
				SetConfigOption("max_connections", optarg, PGC_POSTMASTER, PGC_S_ARGV);
				break;

			case 'n':
				/* Don't reinit shared mem after abnormal exit */
				Reinit = false;
				break;

			case 'O':
				/* Only use in single user mode */
				SetConfigOption("allow_system_table_mods", "all", PGC_POSTMASTER, PGC_S_ARGV);
				break;

			case 'o':
				/* Other options to pass to the backend on the command line */
				snprintf(ExtraOptions + strlen(ExtraOptions),
						 sizeof(ExtraOptions) - strlen(ExtraOptions),
						 " %s", optarg);
				break;

			case 'P':
				SetConfigOption("ignore_system_indexes", "true", PGC_POSTMASTER, PGC_S_ARGV);
				break;

			case 'p':
				SetConfigOption("port", optarg, PGC_POSTMASTER, PGC_S_ARGV);
				break;

			case 'r':
				/* only used by single-user backend */
				break;

			case 'S':
				SetConfigOption("work_mem", optarg, PGC_POSTMASTER, PGC_S_ARGV);
				break;

			case 's':
				SetConfigOption("log_statement_stats", "true", PGC_POSTMASTER, PGC_S_ARGV);
				break;

			case 'T':

				/*
				 * In the event that some backend dumps core, send SIGSTOP,
				 * rather than SIGQUIT, to all its peers.  This lets the wily
				 * post_hacker collect core dumps from everyone.
				 */
				SendStop = true;
				break;

			case 't':
				{
					const char *tmp = get_stats_option_name(optarg);

					if (tmp)
					{
						SetConfigOption(tmp, "true", PGC_POSTMASTER, PGC_S_ARGV);
					}
					else
					{
						write_stderr("%s: invalid argument for option -t: \"%s\"\n",
									 progname, optarg);
						ExitPostmaster(1);
					}
					break;
				}

			case 'U':
				/*
				 * In upgrade mode, we indicate we're in upgrade mode and
				 * 1. allow DML on persistent table & catalog table
				 * 2. alter DDL on catalog table (NOTE: upgrade_mode must set beforehand)
				 * 3. TODO: disable the 4.1 xlog format (stick with the old)
				 */
				SetConfigOption("upgrade_mode",                         "true", PGC_POSTMASTER, PGC_S_ARGV);
				SetConfigOption("gp_permit_persistent_metadata_update", "true", PGC_POSTMASTER, PGC_S_ARGV);
				SetConfigOption("allow_segment_DML",  		            "true", PGC_POSTMASTER, PGC_S_ARGV);
				SetConfigOption("allow_system_table_mods",              "all",  PGC_POSTMASTER, PGC_S_ARGV);
				break;

			case 'W':
				SetConfigOption("post_auth_delay", optarg, PGC_POSTMASTER, PGC_S_ARGV);
				break;

			case 'c':
			case '-':
				{
					char	   *name,
							   *value;

					ParseLongOption(optarg, &name, &value);
					if (!value)
					{
						if (opt == '-')
							ereport(ERROR,
									(errcode(ERRCODE_SYNTAX_ERROR),
									 errmsg("--%s requires a value",
											optarg)));
						else
							ereport(ERROR,
									(errcode(ERRCODE_SYNTAX_ERROR),
									 errmsg("-c %s requires a value",
											optarg)));
					}

					SetConfigOption(name, value, PGC_POSTMASTER, PGC_S_ARGV);
					free(name);
					if (value)
						free(value);
					break;
				}

			case 'y':
				// Indicate that gpstart did not start the standby master.
				SetConfigOption("master_mirroring_administrator_disable", "true", PGC_POSTMASTER, PGC_S_ARGV);
				break;

			case 'x': /* standby master dbid */
				SetConfigOption("gp_standby_dbid", optarg, PGC_POSTMASTER, PGC_S_ARGV);
				break;
            case 'z':
                SetConfigOption("gp_num_contents_in_cluster", optarg, PGC_POSTMASTER, PGC_S_ARGV);
                break;

			default:
				write_stderr("Try \"%s --help\" for more information.\n",
							 progname);
				ExitPostmaster(1);
		}
	}

	/*
	 * Postmaster accepts no non-option switch arguments.
	 */
	if (optind < argc)
	{
		write_stderr("%s: invalid argument: \"%s\"\n",
					 progname, argv[optind]);
		write_stderr("Try \"%s --help\" for more information.\n",
					 progname);
		ExitPostmaster(1);
	}

	/*
	 * Locate the proper configuration files and data directory, and read
	 * postgresql.conf for the first time.
	 */
	if (!SelectConfigFiles(userDoption, progname))
		ExitPostmaster(2);

	/*
	 * CDB/MPP/GPDB: Set the processor affinity (may be a no-op on
	 * some platforms). The port number is nice to use because we know
	 * that different segments on a single host will not have the same
	 * port numbers.
	 *
	 * We want to do this as early as we can -- so that the OS knows
	 * about our binding, and all of our child processes will inherit
	 * the same binding.
 	 */
	if (gp_set_proc_affinity)
		setProcAffinity(PostPortNumber);

	/* Verify that DataDir looks reasonable */
	checkDataDir();

	/* And switch working directory into it */
	ChangeToDataDir();

	/*
     * CDB: Decouple NBuffers from MaxBackends.  The entry db doesn't benefit
     * from buffers in excess of the global catalog size; this is typically
     * small and unrelated to the number of clients.  Segment dbs need enough
     * buffers to accommodate the QEs concurrently accessing the database; but
     * non-leaf QEs don't necessarily access the database (some are used only
     * for sorting, hashing, etc); so again the number of buffers need not be
     * in proportion to the number of connections.
	 */
	if (NBuffers < 16)
	{
		/*
		 * Do not accept -B so small that backends are likely to starve for
		 * lack of buffers.  The specific choices here are somewhat arbitrary.
		 */
		write_stderr("%s: the number of buffers (-B) must be at least 16\n", progname);
		ExitPostmaster(1);
	}

	/*
	 * Check for invalid combinations of GUC settings.
	 */
	if (ReservedBackends > MaxBackends)
	{
		write_stderr("%s: superuser_reserved_connections must be less than max_connections\n", progname);
		ExitPostmaster(1);
	}

	/*
	 * Other one-time internal sanity checks can go here, if they are fast.
	 * (Put any slow processing further down, after postmaster.pid creation.)
	 */
	if (!CheckDateTokenTables())
	{
		write_stderr("%s: invalid datetoken tables, please fix\n", progname);
		ExitPostmaster(1);
	}

	/*
	 * Now that we are done processing the postmaster arguments, reset
	 * getopt(3) library so that it will work correctly in subprocesses.
	 */
	optind = 1;
#ifdef HAVE_INT_OPTRESET
	optreset = 1;				/* some systems need this too */
#endif

	/* For debugging: display postmaster environment */
	{
		extern char **environ;
		char	  **p;

		ereport(DEBUG3,
				(errmsg_internal("%s: PostmasterMain: initial environ dump:",
								 progname)));
		ereport(DEBUG3,
			 (errmsg_internal("-----------------------------------------")));
		for (p = environ; *p; ++p)
			ereport(DEBUG3,
					(errmsg_internal("\t%s", *p)));
		ereport(DEBUG3,
			 (errmsg_internal("-----------------------------------------")));
	}

	/*
	 * Fork away from controlling terminal, if silent_mode specified.
	 *
	 * Must do this before we grab any interlock files, else the interlocks
	 * will show the wrong PID.
	 */
	if (SilentMode)
		pmdaemonize();

	/*
	 * Create lockfile for data directory.
	 *
	 * We want to do this before we try to grab the input sockets, because the
	 * data directory interlock is more reliable than the socket-file
	 * interlock (thanks to whoever decided to put socket files in /tmp :-().
	 * For the same reason, it's best to grab the TCP socket(s) before the
	 * Unix socket.
	 */
	CreateDataDirLockFile(true);

	/*
	 * If timezone is not set, determine what the OS uses.	(In theory this
	 * should be done during GUC initialization, but because it can take as
	 * much as several seconds, we delay it until after we've created the
	 * postmaster.pid file.  This prevents problems with boot scripts that
	 * expect the pidfile to appear quickly.  Also, we avoid problems with
	 * trying to locate the timezone files too early in initialization.)
	 */
	pg_timezone_initialize();

	/*
	 * Likewise, init timezone_abbreviations if not already set.
	 */
	pg_timezone_abbrev_initialize();

	/*
	 * Remember postmaster startup time
     * CDB: Moved this code up from below for use in error message headers.
	 */
	PgStartTime = GetCurrentTimestamp();

	/*
	 * Initialize SSL library, if specified.
	 */
#ifdef USE_SSL
	if (EnableSSL)
		secure_initialize();
#endif

	/*
	 * process any libraries that should be preloaded at postmaster start
	 */
	process_shared_preload_libraries();

	/*
	 * Remove old temporary files.	At this point there can be no other
	 * Postgres processes running in this directory, so this should be safe.
	 */
	RemovePgTempFiles();

	/*
	 * Establish input sockets.
	 */
	for (i = 0; i < MAXLISTEN; i++)
		ListenSocket[i] = -1;

	if (ListenAddresses)
	{
		char	   *rawstring;
		List	   *elemlist;
		ListCell   *l;
		int			success = 0;

		/* Need a modifiable copy of ListenAddresses */
		rawstring = pstrdup(ListenAddresses);

		/* Parse string into list of identifiers */
		if (!SplitIdentifierString(rawstring, ',', &elemlist))
		{
			/* syntax error in list */
			ereport(FATAL,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("invalid list syntax for \"listen_addresses\"")));
		}

		foreach(l, elemlist)
		{
			char	   *curhost = (char *) lfirst(l);

			if (strcmp(curhost, "*") == 0)
				status = StreamServerPort(AF_UNSPEC, NULL,
										  (unsigned short) PostPortNumber,
										  UnixSocketDir,
										  ListenSocket, MAXLISTEN);
			else
				status = StreamServerPort(AF_UNSPEC, curhost,
										  (unsigned short) PostPortNumber,
										  UnixSocketDir,
										  ListenSocket, MAXLISTEN);
			if (status == STATUS_OK)
				success++;
			else
				ereport(WARNING,
						(errmsg("could not create listen socket for \"%s\"",
								curhost)));
		}

		if (!success && list_length(elemlist))
			ereport(FATAL,
					(errmsg("could not create any TCP/IP sockets")));

		list_free(elemlist);
		pfree(rawstring);
	}

#ifdef USE_BONJOUR
	/* Register for Bonjour only if we opened TCP socket(s) */
	if (ListenSocket[0] != -1 && bonjour_name != NULL)
	{
		DNSServiceRegistrationCreate(bonjour_name,
									 "_postgresql._tcp.",
									 "",
									 htons(PostPortNumber),
									 "",
									 (DNSServiceRegistrationReply) reg_reply,
									 NULL);
	}
#endif

#ifdef HAVE_UNIX_SOCKETS
	status = StreamServerPort(AF_UNIX, NULL,
							  (unsigned short) PostPortNumber,
							  UnixSocketDir,
							  ListenSocket, MAXLISTEN);
	if (status != STATUS_OK)
		ereport(WARNING,
				(errmsg("could not create Unix-domain socket")));
#endif

	/*
	 * check that we have some socket to listen on
	 */
	if (ListenSocket[0] == -1)
		ereport(FATAL,
				(errmsg("no socket created for listening")));

	/*
	 * Set up shared memory and semaphores.
	 */
	reset_shared(PostPortNumber, false);

	/*
	 * Estimate number of openable files.  This must happen after setting up
	 * semaphores, because on some platforms semaphores count as open files.
	 */
	set_max_safe_fds();

	/*
	 * Initialize the list of active backends.
	 */
	BackendList = DLNewList();

#ifdef WIN32

	/*
	 * Initialize I/O completion port used to deliver list of dead children.
	 */
	win32ChildQueue = CreateIoCompletionPort(INVALID_HANDLE_VALUE, NULL, 0, 1);
	if (win32ChildQueue == NULL)
		ereport(FATAL,
		   (errmsg("could not create I/O completion port for child queue")));

	/*
	 * Set up a handle that child processes can use to check whether the
	 * postmaster is still running.
	 */
	if (DuplicateHandle(GetCurrentProcess(),
						GetCurrentProcess(),
						GetCurrentProcess(),
						&PostmasterHandle,
						0,
						TRUE,
						DUPLICATE_SAME_ACCESS) == 0)
		ereport(FATAL,
				(errmsg_internal("could not duplicate postmaster handle: error code %d",
								 (int) GetLastError())));
#endif

	/*
	 * Record postmaster options.  We delay this till now to avoid recording
	 * bogus options (eg, NBuffers too high for available memory).
	 */
	if (!CreateOptsFile(argc, argv, my_exec_path))
		ExitPostmaster(1);

#ifdef EXEC_BACKEND
	/* Write out nondefault GUC settings for child processes to use */
	write_nondefault_variables(PGC_POSTMASTER);
#endif

	/*
	 * Write the external PID file if requested
	 */
	if (external_pid_file)
	{
		FILE	   *fpidfile = fopen(external_pid_file, "w");

		if (fpidfile)
		{
			fprintf(fpidfile, "%d\n", MyProcPid);
			fclose(fpidfile);
			/* Should we remove the pid file on postmaster exit? */
		}
		else
			write_stderr("%s: could not write external PID file \"%s\": %s\n",
						 progname, external_pid_file, strerror(errno));
	}

	/*
	 * Set up signal handlers for the postmaster process.
	 *
	 * CAUTION: when changing this list, check for side-effects on the signal
	 * handling setup of child processes.  See tcop/postgres.c,
	 * bootstrap/bootstrap.c, postmaster/bgwriter.c, postmaster/walsendserver.c,
	 * postmaster/autovacuum.c, postmaster/pgarch.c, postmaster/pgstat.c, and
	 * postmaster/syslogger.c.
	 */
	pqinitmask();
	PG_SETMASK(&BlockSig);

	pqsignal(SIGHUP, SIGHUP_handler);	/* reread config file and have
										 * children do same */
	pqsignal(SIGINT, pmdie);	/* send SIGTERM and shut down */
	pqsignal(SIGQUIT, pmdie);	/* send SIGQUIT and die */
	pqsignal(SIGTERM, pmdie);	/* wait for children and shut down */
	pqsignal(SIGALRM, SIG_IGN); /* ignored */
	pqsignal(SIGPIPE, SIG_IGN); /* ignored */
	pqsignal(SIGUSR1, sigusr1_handler); /* message from child process */
	pqsignal(SIGUSR2, dummy_handler);	/* unused, reserve for children */
	pqsignal(SIGCHLD, reaper);	/* handle child termination */
	pqsignal(SIGTTIN, SIG_IGN); /* ignored */
	pqsignal(SIGTTOU, SIG_IGN); /* ignored */
	/* ignore SIGXFSZ, so that ulimit violations work like disk full */
#ifdef SIGXFSZ
	pqsignal(SIGXFSZ, SIG_IGN); /* ignored */
#endif

	/*
	 * If enabled, start up syslogger collection subprocess
	 */
	SysLoggerPID = SysLogger_Start();

	/*
	 * Reset whereToSendOutput from DestDebug (its starting state) to
	 * DestNone. This stops ereport from sending log messages to stderr unless
	 * Log_destination permits.  We don't do this until the postmaster is
	 * fully launched, since startup failures may as well be reported to
	 * stderr.
	 */
	whereToSendOutput = DestNone;

	/*
	 * Load configuration files for client authentication.
	 */
	if (!load_hba())
	{
		/*
		 * It makes no sense to continue if we fail to load the HBA file,
		 * since there is no way to connect to the database in this case.
		 */
		ereport(FATAL,
				(errmsg("could not load pg_hba.conf"),
				 errOmitLocation(true)));
	}
	load_ident();


	/* PostmasterRandom wants its own copy */
	gettimeofday(&random_start_time, NULL);

	status = ServerLoop();

	/*
	 * ServerLoop probably shouldn't ever return, but if it does, close down.
	 */
	ExitPostmaster(status != STATUS_OK);

	return 0;					/* not reached */
}

/**
 * Run the startup procedure for an operational (non-mirror/non-quiescent)
 *   postmaster.
 */
void
StartMasterOrPrimaryPostmasterProcesses(void)
{
    if ( ! gHaveCalledOnetimeInitFunctions )
	{
        /*
         * Initialize stats collection subsystem (this does NOT start the
         * collector process!)
    	 */
	    pgstat_init();

        /*
         * Initialize the autovacuum subsystem (again, no process start yet)
         */
	    autovac_init();
	    gHaveCalledOnetimeInitFunctions = true;
	}

	XLogStartupInit(); /* required to get collation parameters read from the control file */

	/*
	 * We're ready to rock and roll...
	 */
	if (SyncMaster)
	{
		pmState = PM_RUN;
	}
	else /* non-SyncMaster */
	{
		StartupPID = StartupDataBase();
		Assert(StartupPID != 0);
		pmState = PM_STARTUP;
	}
}

void
NotifyProcessesOfFilerepStateChange(void)
{
    primaryMirrorRecordSegmentStateToPostmasterLocalMemory();
	filerep_has_signaled_state_change = false;
}

/*
 * Compute and check the directory paths to files that are part of the
 * installation (as deduced from the postgres executable's own location)
 */
static void
getInstallationPaths(const char *argv0)
{
	DIR		   *pdir;

	/* Locate the postgres executable itself */
	if (find_my_exec(argv0, my_exec_path) < 0)
		elog(FATAL, "%s: could not locate my own executable path", argv0);

#ifdef EXEC_BACKEND
	/* Locate executable backend before we change working directory */
	if (find_other_exec(argv0, "postgres", PG_BACKEND_VERSIONSTR,
						postgres_exec_path) < 0)
		ereport(FATAL,
				(errmsg("%s: could not locate matching postgres executable",
						argv0)));
#endif

	/*
	 * Locate the pkglib directory --- this has to be set early in case we try
	 * to load any modules from it in response to postgresql.conf entries.
	 */
	get_pkglib_path(my_exec_path, pkglib_path);

	/*
	 * Verify that there's a readable directory there; otherwise the Postgres
	 * installation is incomplete or corrupt.  (A typical cause of this
	 * failure is that the postgres executable has been moved or hardlinked to
	 * some directory that's not a sibling of the installation lib/
	 * directory.)
	 */
	pdir = AllocateDir(pkglib_path);
	if (pdir == NULL)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not open directory \"%s\": %m",
						pkglib_path),
				 errSendAlert(true),
				 errhint("This may indicate an incomplete PostgreSQL installation, or that the file \"%s\" has been moved away from its proper location.",
						 my_exec_path)));
	FreeDir(pdir);

	/*
	 * XXX is it worth similarly checking the share/ directory?  If the lib/
	 * directory is there, then share/ probably is too.
	 */
}


/*
 * Validate the proposed data directory
 */
static void
checkDataDir(void)
{
	struct stat stat_buf;

	Assert(DataDir);

	if (stat(DataDir, &stat_buf) != 0)
	{
		if (errno == ENOENT)
			ereport(FATAL,
					(errcode_for_file_access(),
					 errmsg("data directory \"%s\" does not exist",
							DataDir)));
		else
			ereport(FATAL,
					(errcode_for_file_access(),
				 errmsg("could not read permissions of directory \"%s\": %m",
						DataDir)));
	}

	/* eventual chdir would fail anyway, but let's test ... */
	if (!S_ISDIR(stat_buf.st_mode))
		ereport(FATAL,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("specified data directory \"%s\" is not a directory",
						DataDir)));

	/*
	 * Check that the directory belongs to my userid; if not, reject.
	 *
	 * This check is an essential part of the interlock that prevents two
	 * postmasters from starting in the same directory (see CreateLockFile()).
	 * Do not remove or weaken it.
	 *
	 * XXX can we safely enable this check on Windows?
	 */
#if !defined(WIN32) && !defined(__CYGWIN__)
	if (stat_buf.st_uid != geteuid())
		ereport(FATAL,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("data directory \"%s\" has wrong ownership",
						DataDir),
				 errhint("The server must be started by the user that owns the data directory.")));
#endif

	/*
	 * Check if the directory has group or world access.  If so, reject.
	 *
	 * It would be possible to allow weaker constraints (for example, allow
	 * group access) but we cannot make a general assumption that that is
	 * okay; for example there are platforms where nearly all users
	 * customarily belong to the same group.  Perhaps this test should be
	 * configurable.
	 *
	 * XXX temporarily suppress check when on Windows, because there may not
	 * be proper support for Unix-y file permissions.  Need to think of a
	 * reasonable check to apply on Windows.
	 */
#if !defined(WIN32) && !defined(__CYGWIN__)
	if (stat_buf.st_mode & (S_IRWXG | S_IRWXO))
		ereport(FATAL,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("data directory \"%s\" has group or world access",
						DataDir),
				 errdetail("Permissions should be u=rwx (0700).")));
#endif

	/* Look for PG_VERSION before looking for pg_control */
	ValidatePgVersion(DataDir);

    /* for mirroring, the check for pg_control was removed.  a mirror must be able to start
     *  without a pg_control file.  The  check for pg_control will be done
     *  when the control file is read by xlog initialization.
     */
}


/*
 * check if file or directory under "DataDir" exists and is accessible
 */
static void checkPgDir(const char *dir)
{
	Assert(DataDir);

	struct stat st;
	char buf[strlen(DataDir) + strlen(dir) + 32];

	snprintf(buf, ARRAY_SIZE(buf), "%s%s", DataDir, dir);
	buf[ARRAY_SIZE(buf) - 1] = '\0';

	if (stat(buf, &st) != 0)
	{
		/* check if pg_log is there */
		snprintf(buf, ARRAY_SIZE(buf), "%s%s", DataDir, "/pg_log");
		if (stat(buf, &st) == 0)
		{
			elog(LOG, "System file or directory missing (%s), shutting down segment", dir);
		}

		/* quit all processes and exit */
		pmdie(SIGQUIT);
	}
}

/*
 * check if file or directory under current transaction filespace exists and is accessible
 */
static void checkPgDir2(const char *dir)
{
	Assert(DataDir);

	struct stat st;
	char buf[MAXPGPATH];
	char *path = makeRelativeToTxnFilespace((char*)dir);

	snprintf(buf, MAXPGPATH, "%s", path);
	buf[ARRAY_SIZE(buf) - 1] = '\0';

	if (stat(buf, &st) != 0)
	{
		/* check if pg_log is there */
		snprintf(buf, ARRAY_SIZE(buf), "%s%s", DataDir, "/pg_log");
		if (stat(buf, &st) == 0)
		{
			elog(LOG, "System file or directory missing (%s), shutting down segment", path);
		}

		pfree(path);
		/* quit all processes and exit */
		pmdie(SIGQUIT);
	}
	
	pfree(path);
}


#ifdef USE_TEST_UTILS
/*
 * Simulate an unexpected process exit using SimEx
 */
static void SimExProcExit()
{
	if (gp_simex_init &&
	    gp_simex_run &&
	    pmState == PM_RUN)
	{
		pid_t pid = 0;
		int sig = 0;
		const char *procName = NULL;
		const char *sigName = NULL;

		if (gp_simex_class == SimExESClass_ProcKill)
		{
			sig = SIGKILL;
			sigName = "SIGKILL";

			SimExESSubClass subclass = SimEx_CheckInject();
			if (subclass == SimExESSubClass_ProcKill_BgWriter)
			{
				pid = BgWriterPID;
				procName = "writer process";
			}
			else
			{
				Assert(subclass == SimExESSubClass_OK &&
				       "Unexpected ES subclass for SIGKILL injection");
			}
		}

		if (pid != 0)
		{
			Assert(sig != 0);
 			Assert(procName != NULL);
 			Assert(sigName != NULL);

 			ereport(LOG,
 					(errmsg_internal("sending %s to %s (%d)", sigName, procName, (int) pid)));
 			signal_child(pid, sig);
 		}
 	}
}
#endif /* USE_TEST_UTILS */


#ifdef USE_BONJOUR

/*
 * empty callback function for DNSServiceRegistrationCreate()
 */
static void
reg_reply(DNSServiceRegistrationReplyErrorType errorCode, void *context)
{
}
#endif   /* USE_BONJOUR */


/*
 * Fork away from the controlling terminal (silent_mode option)
 *
 * Since this requires disconnecting from stdin/stdout/stderr (in case they're
 * linked to the terminal), we re-point stdin to /dev/null and stdout/stderr
 * to "pg_log/startup.log" from the data directory, where we're already chdir'd.
 */
static void
pmdaemonize(void)
{
#ifndef WIN32
	const char *pmlogname = "pg_log/startup.log";
	int			dvnull;
	int			pmlog;
	pid_t		pid;
	int			res;

	/*
	 * Make sure we can open the files we're going to redirect to.  If this
	 * fails, we want to complain before disconnecting.  Mention the full path
	 * of the logfile in the error message, even though we address it by
	 * relative path.
	 */
	dvnull = open(DEVNULL, O_RDONLY, 0);
	if (dvnull < 0)
	{
		write_stderr("%s: could not open file \"%s\": %s\n",
					 progname, DEVNULL, strerror(errno));
		ExitPostmaster(1);
	}
	pmlog = open(pmlogname, O_CREAT | O_WRONLY | O_APPEND, 0600);
	if (pmlog < 0)
	{
		write_stderr("%s: could not open log file \"%s/%s\": %s\n",
					 progname, DataDir, pmlogname, strerror(errno));
		ExitPostmaster(1);
	}

	/*
	 * Okay to fork.
	 */
	pid = fork_process();
	if (pid == (pid_t) -1)
	{
		write_stderr("%s: could not fork background process: %s\n",
					 progname, strerror(errno));
		ExitPostmaster(1);
	}
	else if (pid)
	{							/* parent */
		/* Parent should just exit, without doing any atexit cleanup */
		_exit(0);
	}

	MyProcPid = PostmasterPid = getpid();		/* reset PID vars to child */

	MyStartTime = time(NULL);

	/*
	 * Some systems use setsid() to dissociate from the TTY's process group,
	 * while on others it depends on stdin/stdout/stderr.  Do both if possible.
	 */
#ifdef HAVE_SETSID
	if (setsid() < 0)
	{
		write_stderr("%s: could not dissociate from controlling TTY: %s\n",
					 progname, strerror(errno));
		ExitPostmaster(1);
	}
#endif

	/*
	 * Reassociate stdin/stdout/stderr.  fork_process() cleared any pending
	 * output, so this should be safe.  The only plausible error is EINTR,
	 * which just means we should retry.
	 */
	do {
		res = dup2(dvnull, 0);
	} while (res < 0 && errno == EINTR);
	close(dvnull);
	do {
		res = dup2(pmlog, 1);
	} while (res < 0 && errno == EINTR);
	do {
		res = dup2(pmlog, 2);
	} while (res < 0 && errno == EINTR);
	close(pmlog);
#else							/* WIN32 */
	/* not supported */
	elog(FATAL, "silent_mode is not supported under Windows");
#endif   /* WIN32 */
}

static bool
ServiceStartable(PMSubProc *subProc)
{
	int flagNeeded;
	bool result;

	flagNeeded = 0;
	if (!SyncMaster)
	{
		if (AmIMaster())
			flagNeeded = PMSUBPROC_FLAG_QD;
		else
			flagNeeded = PMSUBPROC_FLAG_QE;
	}
	else
		flagNeeded = PMSUBPROC_FLAG_STANDBY;

	/*
	 * GUC gp_enable_gpperfmon controls the start
	 * of both the 'perfmon' and 'stats sender' processes
	 */
	if ((subProc->procType == PerfmonProc || subProc->procType == PerfmonSegmentInfoProc)
	    && !gp_enable_gpperfmon)
		result = 0;
	else
		result = ((subProc->flags & flagNeeded) != 0);

	return result;
}

/**
 * Reset pmState to PM_INIT and begin initialization sequence.
 */
static void
RestartAfterPostmasterReset(void)
{
	pmState = PM_INIT;
	FatalError = false;

	/* done..serverLoop will process startup request */
}

static void
BeginResetOfPostmasterAfterChildrenAreShutDown(void)
{
	elog(LOG, "BeginResetOfPostmasterAfterChildrenAreShutDown: counter %d", pmResetCounter);
	pmResetCounter++;

	PrimaryMirrorMode pm_mode;
	SegmentState_e s_state;
	DataState_e d_state;
	FaultType_e f_type;
	getPrimaryMirrorStatusCodes(&pm_mode, &s_state, &d_state, &f_type);

	/* set flag indicating mirroring fault */
	bool hasFaultTypeMirror = false;
	setFaultAfterReset(hasFaultTypeMirror);

	/* copy most recent transition request to local memory */
	copyTransitionParametersToLocalMemory();

	/*
	 * Save the MPP session number from shared-memory before we reset
	 * everything.  There is some risk here accessing the shared-memory,
	 * but we don't want to reuse session-ids on the master.
	 */
	if (ProcGetMppLocalProcessCounter(&Save_MppLocalProcessCounter))
		ereport(LOG,
				(errmsg("gp_session_id high-water mark is %d",
						Save_MppLocalProcessCounter)));

	/*
	 * reset shared memory and semaphores;
	 * this happens before forking the filerep peer reset process;
	 * the latter process should not use information from shared memory;
	 */
	shmem_exit(0 /*code*/);
	reset_shared(PostPortNumber, true /*isReset*/);

	/*
	 * Remove old temporary files.	At this point there can be no other
	 * Postgres processes running in this directory, so this should be safe.
	 */
	RemovePgTempFiles();

	ereport(LOG,
		(errmsg("all server processes terminated; reinitializing")));
	RestartAfterPostmasterReset();
}


/**
 * loop waiting for immediate shutdown to complete
 */
static void
ImmediateShutdownLoop(void)
{
	PG_SETMASK(&BlockSig);
	if (need_call_reaper)
	{
		do_immediate_shutdown_reaper();
	}

    ExitPostmaster(0);
}

/*
 * Main idle loop of postmaster
 */
static int
ServerLoop(void)
{
	fd_set		readmask;
	int			nSockets;
	time_t		now,
		last_touch_time;

	last_touch_time = time(NULL);

	nSockets = initMasks(&readmask);

	/* initialize tm sync */
	if (SyncMaster)
		cdb_init_log_sync();

	XLogStartupInit();

	for (;;)
	{
		fd_set		rmask;
		int			selres;
		int			s;
        struct timeval timeout;

		/*
		 * Wait for a connection request to arrive.
		 *
		 * We wait at most one minute, to ensure that the other background
		 * tasks handled below get done even when no requests are arriving.
		 *
		 * If we are in PM_WAIT_DEAD_END state, then we don't want to accept
		 * any new connections, so we don't call select() at all; just sleep
		 * for a little bit with signals unblocked.
		 */
		memcpy((char *) &rmask, (char *) &readmask, sizeof(fd_set));

		PG_SETMASK(&UnBlockSig);

		/*
		 * we allow new connections in many cases (which could later be rejected because the database is shutting down,
		 *   but that gives a nicer message than potentially hanging the client's connection)
		 */
        bool acceptNewConnections;

        if ( Shutdown == NoShutdown && FatalError)
        {
        	/** during postmaster reset, accept new connections to allow us to coordinate the reset */
        	acceptNewConnections = pmState < PM_CHILD_STOP_WAIT_DEAD_END_CHILDREN ||
        							pmState == PM_POSTMASTER_RESET_FILEREP_PEER;
		}
		else
		{
         	acceptNewConnections = pmState < PM_CHILD_STOP_WAIT_DEAD_END_CHILDREN &&
                                    Shutdown != ImmediateShutdown;
		}

        /* If something that needs handling is received while we were doing other things with signals
         * blocked then we need to skip select as quickly as possible and process it.
         *
         * This assumes that all signals are delivered immediately upon unblocking.
         *
         * I think there may still be a race in which we are signaled between the completion of unblock and the sleep.
         */
        if (Shutdown == ImmediateShutdown ||
			filerep_has_signaled_state_change ||
			filerep_requires_postmaster_reset ||
			need_call_reaper)
        {
            /* must set timeout each time; some OSes change it! */
            timeout.tv_sec = 0;
            timeout.tv_usec = 0;
        }
        else if (isPrimaryMirrorModeTransitionRequested() ||
                 ! acceptNewConnections )
        {
            /* must set timeout each time; some OSes change it! */
            timeout.tv_sec = 0;
            timeout.tv_usec = 100 * 1000;
        }
        else
        {
            /* must set timeout each time; some OSes change it! */
            timeout.tv_sec = 60;
            timeout.tv_usec = 0;
        }

#ifdef USE_TEST_UTILS
        SimExProcExit();
#endif /* USE_TEST_UTILS */

        errno = 0;
        if (acceptNewConnections)
        {
            selres = select(nSockets, &rmask, NULL, NULL, &timeout);

            /* check the select() result */
            if (selres < 0)
            {
                if (errno != EINTR && errno != EWOULDBLOCK)
                {
                    ereport(LOG,
                            (errcode_for_socket_access(),
                             errmsg("select() failed in postmaster: %m")));
                    return STATUS_ERROR;
                }
            }
#ifdef FAULT_INJECTOR
			FaultInjector_InjectFaultIfSet(
										   Postmaster,
										   DDLNotSpecified,
										   "",	//databaseName
										   ""); // tableName
#endif											
        }
        else
        {
            /* will be the medium wait time unless waiting for reaper */
            pg_usleep( timeout.tv_usec + timeout.tv_sec * 1000 * 1000);
            selres = 0;
        }

		/* Sanity check for system directories on all segments */
		if (AmISegment())
		{
			checkPgDir("");
			checkPgDir("/base");
			checkPgDir("/global");
			checkPgDir("/pg_twophase");
			checkPgDir("/pg_utilitymodedtmredo");
			checkPgDir2("pg_distributedlog");
			checkPgDir2("pg_distributedxidmap");
			checkPgDir2("pg_multixact");
			checkPgDir2("pg_subtrans");
			checkPgDir2("pg_xlog");
			checkPgDir2("pg_clog");
			checkPgDir2("pg_multixact/members");
			checkPgDir2("pg_multixact/offsets");
			checkPgDir2("pg_xlog/archive_status");	
		}

#ifdef USE_TEST_UTILS
		SimExProcExit();
#endif /* USE_TEST_UTILS */

		/*
		 * Block all signals until we wait again.  (This makes it safe for our
		 * signal handlers to do nontrivial work.)
		 */
		PG_SETMASK(&BlockSig);

		if (Shutdown == ImmediateShutdown)
        {
		    ImmediateShutdownLoop();
        }

        if (filerep_requires_postmaster_reset)
        {
            filerep_requires_postmaster_reset = false;
            HandleChildCrash(-1, -1, "");
        }

		/*
		 * GPDB Change:  Reaper just sets a flag, and we call the real reaper code here.
		 * I assume this was done because we wanted to excecute some code that wasn't safe in a
		 * signal handler context (debugging code, most likely).
		 * But the real reason is lost in antiquity.
		 */
		if (need_call_reaper)
		{
			do_reaper();
		}

		if (filerep_has_signaled_state_change)
		{
			NotifyProcessesOfFilerepStateChange();
		}

		/*
		 * New connection pending on any of our sockets? If so, fork a child
		 * process to deal with it.
		 */
		if (selres > 0)
		{
			int			i;

			for (i = 0; i < MAXLISTEN; i++)
			{
				if (ListenSocket[i] == -1)
					break;
				if (FD_ISSET(ListenSocket[i], &rmask))
				{
					Port	   *port;

					port = ConnCreate(ListenSocket[i]);
					if (port)
					{
						BackendStartup(port);

						/*
						 * We no longer need the open socket or port structure
						 * in this process
						 */
						StreamClose(port->sock);
						ConnFree(port);
					}
				}
			}
		}

#ifdef USE_TEST_UTILS
        SimExProcExit();
#endif /* USE_TEST_UTILS */

        if (!FatalError)
        {
        	static int saved_pmResetCounter = 0;

			/* Initial start or crash recovery should restart again. */
			if (pmState == PM_INIT ||
				pmResetCounter != saved_pmResetCounter)
			{
				StartMasterOrPrimaryPostmasterProcesses();
				saved_pmResetCounter = pmResetCounter;
			}
        }

		/* If we have lost the log collector, try to start a new one */
		if (SysLoggerPID == 0 && Redirect_stderr)
		{
			SysLoggerPID = SysLogger_Start();
			if (Debug_print_server_processes)
				elog(LOG,"restarted 'system logger process' as pid %ld",
					 (long)SysLoggerPID);
		}

		if ( isFullPostmasterAndDatabaseIsAllowed())
		{
			/*
			 * If no background writer process is running, and we are not in a
			 * state that prevents it, start one.  It doesn't matter if this
			 * fails, we'll just try again later.
			 */
			if (BgWriterPID == 0 &&
			    StartupPidsAllZero() && pmState > PM_STARTUP_PASS4 &&
			    !FatalError && !SyncMaster &&
			    pmState < PM_CHILD_STOP_BEGIN)
			{
				SetBGWriterPID(StartBackgroundWriter());
				if (Debug_print_server_processes)
					elog(LOG,"restarted 'background writer process' as pid %ld",
						 (long)BgWriterPID);
				/* If shutdown is pending, set it going */
				if (Shutdown > NoShutdown && BgWriterPID != 0)
					signal_child(BgWriterPID, SIGUSR2);
			}

			if (CheckpointPID == 0 &&
				StartupPidsAllZero() && pmState > PM_STARTUP_PASS4 &&
				!FatalError && !SyncMaster &&
				Shutdown == NoShutdown)
			{
				CheckpointPID = StartCheckpointServer();
				if (Debug_print_server_processes)
					elog(LOG,"restarted 'checkpoint process' as pid %ld",
						 (long)CheckpointPID);
			}

			/*
			 * Start a new autovacuum process, if there isn't one running already.
			 * (It'll die relatively quickly.)  We check that it's not started too
			 * frequently in autovac_start.
			 */
			if ((AutoVacuumingActive() || force_autovac) && AutoVacPID == 0 &&
				StartupPidsAllZero() && pmState > PM_STARTUP_PASS4 &&
				!FatalError && Shutdown == NoShutdown)
			{
				AutoVacPID = autovac_start();
				if (Debug_print_server_processes)
					elog(LOG,"restarted 'autovacuum process' as pid %ld",
						 (long)AutoVacPID);
				if (AutoVacPID != 0)
					force_autovac = false;	/* signal successfully processed */
			}

			/* If we have lost the stats collector, try to start a new one */
			if (PgStatPID == 0 &&
				StartupPidsAllZero() && pmState > PM_STARTUP_PASS4 &&
				!FatalError && Shutdown == NoShutdown && !SyncMaster)
			{
				PgStatPID = pgstat_start();
				if (Debug_print_server_processes)
					elog(LOG,"restarted 'statistics collector process' as pid %ld",
						 (long)PgStatPID);

			}

			/* MPP: If we have lost one of our servers, try to start a new one */
			for (s=0; s < MaxPMSubType; s++)
			{
				PMSubProc *subProc = &PMSubProcList[s];

				if (subProc->pid == 0 &&
					StartupPidsAllZero() &&
					pmState > PM_STARTUP_PASS4 &&
					!FatalError &&
					Shutdown == NoShutdown &&
					ServiceStartable(subProc))
				{
					subProc->pid =
						(subProc->serverStart)();
					if (Debug_print_server_processes)
						elog(LOG,"started '%s' as pid %ld",
							 subProc->procName, (long)subProc->pid);
				}
			}
		}

		/*
		 * Touch the socket and lock file every 58 minutes, to ensure that
		 * they are not removed by overzealous /tmp-cleaning tasks.  We assume
		 * no one runs cleaners with cutoff times of less than an hour ...
		 */
		now = time(NULL);
		if (now - last_touch_time >= 58 * SECS_PER_MINUTE)
		{
			TouchSocketFile();
			TouchSocketLockFile();
			last_touch_time = now;
		}
	}
}


/*
 * Initialise the masks for select() for the ports we are listening on.
 * Return the number of sockets to listen on.
 */
static int
initMasks(fd_set *rmask)
{
	int			maxsock = -1;
	int			i;

	FD_ZERO(rmask);

	for (i = 0; i < MAXLISTEN; i++)
	{
		int			fd = ListenSocket[i];

		if (fd == PGINVALID_SOCKET)
			break;
		FD_SET(fd, rmask);

		if (fd > maxsock)
			maxsock = fd;
	}

	return maxsock + 1;
}


/*
 * Read a client's startup packet and do something according to it.
 *
 * Returns STATUS_OK or STATUS_ERROR, or might call ereport(FATAL) and
 * not return at all.
 *
 * (Note that ereport(FATAL) stuff is sent to the client, so only use it
 * if that's what you want.  Return STATUS_ERROR if you don't want to
 * send anything to the client, which would typically be appropriate
 * if we detect a communications failure.)
 */
static int
ProcessStartupPacket(Port *port, bool SSLdone)
{
	int32		len;
	void	   *buf;
	ProtocolVersion proto;
	MemoryContext oldcontext;
    char       *gpqeid = NULL;

	if (pq_getbytes((char *) &len, 4) == EOF)
	{
		/*
		 * EOF after SSLdone probably means the client didn't like our
		 * response to NEGOTIATE_SSL_CODE.	That's not an error condition, so
		 * don't clutter the log with a complaint.else if (strcmp(nameptr, "replication") == 0)
			{
				if (!parse_bool(valptr, &am_walsender))
					ereport(FATAL,
							(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
							 errmsg("invalid value for boolean option \"replication\"")));
			}
		 */
		if (!SSLdone)
			ereport(COMMERROR,
					(errcode(ERRCODE_PROTOCOL_VIOLATION),
					 errmsg("incomplete startup packet")));
		return STATUS_ERROR;
	}

	len = ntohl(len);
	len -= 4;

	if (len < (int32) sizeof(ProtocolVersion) ||
		len > MAX_STARTUP_PACKET_LENGTH)
	{
		ereport(COMMERROR,
				(errcode(ERRCODE_PROTOCOL_VIOLATION),
				 errmsg("invalid length of startup packet %ld",(long)len)));
		return STATUS_ERROR;
	}

	/*
	 * Allocate at least the size of an old-style startup packet, plus one
	 * extra byte, and make sure all are zeroes.  This ensures we will have
	 * null termination of all strings, in both fixed- and variable-length
	 * packet layouts.
	 */
	if (len <= (int32) sizeof(StartupPacket))
		buf = palloc0(sizeof(StartupPacket) + 1);
	else
		buf = palloc0(len + 1);

	if (pq_getbytes(buf, len) == EOF)
	{
		ereport(COMMERROR,
				(errcode(ERRCODE_PROTOCOL_VIOLATION),
				 errmsg("incomplete startup packet")));
		return STATUS_ERROR;
	}

	/*
	 * The first field is either a protocol version number or a special
	 * request code.
	 */
	port->proto = proto = ntohl(*((ProtocolVersion *) buf));

	if (proto == CANCEL_REQUEST_CODE)
	{
		processCancelRequest(port, buf);
		return 127;				/* XXX */
	}
	else if (proto == PRIMARY_MIRROR_TRANSITION_REQUEST_CODE)
	{
	    /* disable the authentication timeout in case it takes a long time */
        if (!disable_sig_alarm(false))
            elog(FATAL, "could not disable timer for authorization timeout");
		processPrimaryMirrorTransitionRequest(port, buf);
		return 127;
	}
	else if (proto == PRIMARY_MIRROR_TRANSITION_QUERY_CODE)
	{
	    /* disable the authentication timeout in case it takes a long time */
        if (!disable_sig_alarm(false))
            elog(FATAL, "could not disable timer for authorization timeout");
		processPrimaryMirrorTransitionQuery(port, buf);
		return 127;
	}

	/* Otherwise this is probably a normal postgres-message */

	if (proto == NEGOTIATE_SSL_CODE && !SSLdone)
	{
		char		SSLok;

#ifdef USE_SSL
		/* No SSL when disabled or on Unix sockets */
		if (!EnableSSL || IS_AF_UNIX(port->laddr.addr.ss_family))
			SSLok = 'N';
		else
			SSLok = 'S';		/* Support for SSL */
#else
		SSLok = 'N';			/* No support for SSL */
#endif

retry1:
		if (send(port->sock, &SSLok, 1, 0) != 1)
		{
			if (errno == EINTR)
				goto retry1;	/* if interrupted, just retry */
			ereport(COMMERROR,
					(errcode_for_socket_access(),
					 errmsg("failed to send SSL negotiation response: %m")));
			return STATUS_ERROR;	/* close the connection */
		}

#ifdef USE_SSL
		if (SSLok == 'S' && secure_open_server(port) == -1)
			return STATUS_ERROR;
#endif
		/* regular startup packet, cancel, etc packet should follow... */
		/* but not another SSL negotiation request */
		return ProcessStartupPacket(port, true);
	}

	/* Could add additional special packet types here */

	/*
	 * Set FrontendProtocol now so that ereport() knows what format to send if
	 * we fail during startup.
	 */
	FrontendProtocol = proto;

	/* Check we can handle the protocol the frontend is using. */

	if (PG_PROTOCOL_MAJOR(proto) < PG_PROTOCOL_MAJOR(PG_PROTOCOL_EARLIEST) ||
		PG_PROTOCOL_MAJOR(proto) > PG_PROTOCOL_MAJOR(PG_PROTOCOL_LATEST) ||
		(PG_PROTOCOL_MAJOR(proto) == PG_PROTOCOL_MAJOR(PG_PROTOCOL_LATEST) &&
		 PG_PROTOCOL_MINOR(proto) > PG_PROTOCOL_MINOR(PG_PROTOCOL_LATEST)))
		ereport(FATAL,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("unsupported frontend protocol %u.%u: server supports %u.0 to %u.%u",
						PG_PROTOCOL_MAJOR(proto), PG_PROTOCOL_MINOR(proto),
						PG_PROTOCOL_MAJOR(PG_PROTOCOL_EARLIEST),
						PG_PROTOCOL_MAJOR(PG_PROTOCOL_LATEST),
						PG_PROTOCOL_MINOR(PG_PROTOCOL_LATEST))));

	/*
	 * Now fetch parameters out of startup packet and save them into the Port
	 * structure.  All data structures attached to the Port struct must be
	 * allocated in TopMemoryContext so that they won't disappear when we pass
	 * them to PostgresMain (see BackendRun).  We need not worry about leaking
	 * this storage on failure, since we aren't in the postmaster process
	 * anymore.
	 */
	oldcontext = MemoryContextSwitchTo(TopMemoryContext);

	if (PG_PROTOCOL_MAJOR(proto) >= 3)
	{
		int32		offset = sizeof(ProtocolVersion);

		/*
		 * Scan packet body for name/option pairs.	We can assume any string
		 * beginning within the packet body is null-terminated, thanks to
		 * zeroing extra byte above.
		 */
		port->guc_options = NIL;

		while (offset < len)
		{
			char	   *nameptr = ((char *) buf) + offset;
			int32		valoffset;
			char	   *valptr;

			if (*nameptr == '\0')
				break;			/* found packet terminator */
			valoffset = offset + strlen(nameptr) + 1;
			if (valoffset >= len)
				break;			/* missing value, will complain below */
			valptr = ((char *) buf) + valoffset;

			if (strcmp(nameptr, "database") == 0)
				port->database_name = pstrdup(valptr);
			else if (strcmp(nameptr, "user") == 0)
				port->user_name = pstrdup(valptr);
			else if (strcmp(nameptr, "options") == 0)
				port->cmdline_options = pstrdup(valptr);
			else if (strcmp(nameptr, "dboid") == 0)
				port->dboid = atoi(valptr);
			else if (strcmp(nameptr, "dbdtsoid") == 0)
				port->dbdtsoid = atoi(valptr);
			else if (strcmp(nameptr, "bootstrap_user") == 0)
				port->bootstrap_user = pstrdup(valptr);
			else if (strcmp(nameptr, "encoding") == 0)
				port->encoding = atoi(valptr);
			else if (strcmp(nameptr, "gpqeid") == 0)
				gpqeid = valptr;
			else
			{
				/* Assume it's a generic GUC option */
				port->guc_options = lappend(port->guc_options,
											pstrdup(nameptr));
				port->guc_options = lappend(port->guc_options,
											pstrdup(valptr));
			}
			offset = valoffset + strlen(valptr) + 1;
		}

		/*
		 * If we didn't find a packet terminator exactly at the end of the
		 * given packet length, complain.
		 */
		if (offset != len - 1)
			ereport(FATAL,
					(errcode(ERRCODE_PROTOCOL_VIOLATION),
					 errmsg("invalid startup packet layout: expected terminator as last byte")));
	}
	else
	{
		/*
		 * Get the parameters from the old-style, fixed-width-fields startup
		 * packet as C strings.  The packet destination was cleared first so a
		 * short packet has zeros silently added.  We have to be prepared to
		 * truncate the pstrdup result for oversize fields, though.
		 */
		StartupPacket *packet = (StartupPacket *) buf;

		port->database_name = pstrdup(packet->database);
		if (strlen(port->database_name) > sizeof(packet->database))
			port->database_name[sizeof(packet->database)] = '\0';
		port->user_name = pstrdup(packet->user);
		if (strlen(port->user_name) > sizeof(packet->user))
			port->user_name[sizeof(packet->user)] = '\0';
		port->cmdline_options = pstrdup(packet->options);
		if (strlen(port->cmdline_options) > sizeof(packet->options))
			port->cmdline_options[sizeof(packet->options)] = '\0';
		port->guc_options = NIL;
	}

	/* Check a user name was given. */
	if (port->user_name == NULL || port->user_name[0] == '\0')
		ereport(FATAL,
				(errcode(ERRCODE_INVALID_AUTHORIZATION_SPECIFICATION),
			 errmsg("no PostgreSQL user name specified in startup packet")));

	/* The database defaults to the user name. */
	if (port->database_name == NULL || port->database_name[0] == '\0')
		port->database_name = pstrdup(port->user_name);

	if (Db_user_namespace)
	{
		/*
		 * If user@, it is a global user, remove '@'. We only want to do this
		 * if there is an '@' at the end and no earlier in the user string or
		 * they may fake as a local user of another database attaching to this
		 * database.
		 */
		if (strchr(port->user_name, '@') ==
			port->user_name + strlen(port->user_name) - 1)
			*strchr(port->user_name, '@') = '\0';
		else
		{
			/* Append '@' and dbname */
			char	   *db_user;

			db_user = palloc(strlen(port->user_name) +
							 strlen(port->database_name) + 2);
			sprintf(db_user, "%s@%s", port->user_name, port->database_name);
			port->user_name = db_user;
		}
	}

	/*
	 * Truncate given database and user names to length of a Postgres name.
	 * This avoids lookup failures when overlength names are given.
	 */
	if (strlen(port->database_name) >= NAMEDATALEN)
		port->database_name[NAMEDATALEN - 1] = '\0';
	if (strlen(port->user_name) >= NAMEDATALEN)
		port->user_name[NAMEDATALEN - 1] = '\0';

    /*
     * CDB: Process "gpqeid" parameter string for qExec startup.
     */
    if (gpqeid)
        cdbgang_parse_gpqeid_params(port, gpqeid);

	/*
	 * Done putting stuff in TopMemoryContext.
	 */
	MemoryContextSwitchTo(oldcontext);

	/*
	 * If we're going to reject the connection due to database state, say so
	 * now instead of wasting cycles on an authentication exchange. (This also
	 * allows a pg_ping utility to be written.)
	 */
	switch (port->canAcceptConnections)
	{
		case CAC_STARTUP:
			ereport(FATAL,
					(errcode(ERRCODE_CANNOT_CONNECT_NOW),
					 errSendAlert(false),
					 errOmitLocation(true),
					 errmsg(POSTMASTER_IN_STARTUP_MSG)));
			break;
		case CAC_SHUTDOWN:
			ereport(FATAL,
					(errcode(ERRCODE_CANNOT_CONNECT_NOW),
					 errSendAlert(false),
					 errOmitLocation(true),
					 errmsg("the database system is shutting down")));
			break;
		case CAC_RECOVERY:
			ereport(FATAL,
					(errcode(ERRCODE_CANNOT_CONNECT_NOW),
					 errSendAlert(true),
					 errOmitLocation(true),
					 errmsg(POSTMASTER_IN_RECOVERY_MSG)));
			break;
        case CAC_MIRROR_OR_QUIESCENT:
			ereport(FATAL,
					(errcode(ERRCODE_CANNOT_CONNECT_NOW),
					 errSendAlert(true),
					 errOmitLocation(true),
					 errmsg("the database system is in mirror or uninitialized mode")));
			break;
		case CAC_TOOMANY:
			ereport(FATAL,
					(errcode(ERRCODE_TOO_MANY_CONNECTIONS),
					 errSendAlert(true),
					 errOmitLocation(true),
					 errmsg("sorry, too many clients already")));
			break;
		case CAC_OK:
			break;
	}

	return STATUS_OK;
}

/**
 * Reads the next string up to a newline
 * if no string can be read then NULL is returned
 * otherwise a newly allocated, NULL-terminated string is returned
 */
static char *
readNextStringFromString(char *buf, int *offsetInOut, int length)
{
	char *result;
	int offset = *offsetInOut;
	int strEnd = offset;
	if (offset == length)
	{
		return NULL;
	}

	while (strEnd < length && buf[strEnd] != '\n')
	{
		strEnd++;
	}
	result = palloc(sizeof(char) * (strEnd - offset + 1));
	memcpy(result, buf + offset, strEnd - offset);
	result[strEnd - offset] = '\0';

	/* skip over newline */
	if ( strEnd < length)
		strEnd++;

	/* result */
	*offsetInOut = strEnd;
	return result;
}

#ifdef FAULT_INJECTOR
/**
 *  Returns 0 if the string could not be read and sets *wasRead (if wasRead is non-NULL) to false
 */
static int
readIntFromString( char *buf, int *offsetInOut, int length, bool *wasRead)
{
	int res;
	char *val = readNextStringFromString(buf, offsetInOut, length);
	if (val == NULL)
	{
		if (wasRead)
			*wasRead = false;
		return 0;
	}

	if (wasRead)
		*wasRead = true;
	res = atoi(val);
	pfree(val);
	return res;
}
#endif

static void sendPrimaryMirrorTransitionResult( const char *msg)
{
	StringInfoData buf;

	initStringInfo(&buf);

	pq_beginmessage(&buf, '\0');
	pq_send_ascii_string(&buf, msg);
	pq_endmessage(&buf);
	pq_flush();
}

#define PROTOCOL_VIOLATION ("protocol_violation")

static void
processTransitionRequest_beginPostmasterReset(void)
{
	char statusBuf[10000];

 	PrimaryMirrorModeTransitionArguments args = primaryMirrorGetArgumentsFromLocalMemory();
 	bool resetRequired = false;

	if (args.mode == PMModeMaster)
	{
		elog(LOG, "request to reset is ignored by the master");
		snprintf(statusBuf, sizeof(statusBuf), "Failure: attempt to reset the master");
	}

	sendPrimaryMirrorTransitionResult(statusBuf);

	if (resetRequired)
	{
		SendPostmasterSignal(PMSIGNAL_POSTMASTER_RESET_BY_PEER);
	}
}

static void
processTransitionRequest_getPostmasterResetStatus(void)
{
	char statusBuf[10000];

 	/* note: this uses the FatalError, pmState, and pmResetCounter values inherited at the time of forking
 	 * the process to satisfy the transition request.  This is enough for our purposes. */
	if (FatalError && pmState == PM_POSTMASTER_RESET_FILEREP_PEER)
	{
		snprintf(statusBuf, sizeof(statusBuf), "Success:\nisInResetPivotPoint\n%09d", pmResetCounter);
	}
	else
	{
		snprintf(statusBuf, sizeof(statusBuf), "Success:\nresetStatusServerIsStartingOrStopping\n%09d", pmResetCounter);
	}

	sendPrimaryMirrorTransitionResult(statusBuf);
}

static void
processTransitionRequest_getStatus(void)
{
	char statusBuf[10000];
	getPrimaryMirrorModeStatus(statusBuf, sizeof(statusBuf));
	sendPrimaryMirrorTransitionResult(statusBuf);
}

static void
processTransitionRequest_getCollationAndDataDir(void)
{
	char statusBuf[10000 + MAXPGPATH];

	snprintf(statusBuf, sizeof(statusBuf), "Success: lc_collate:%s\n"
				"lc_monetary:%s\n"
				"lc_numeric:%s\n"
				"datadir:%s\n",
				locale_collate,
				locale_monetary,
				locale_numeric,
				data_directory );

	sendPrimaryMirrorTransitionResult(statusBuf);
}

static void
processTransitionRequest_getVersion(void)
{
	char statusBuf[10000];
	snprintf(statusBuf, sizeof(statusBuf), "Success:%s", TextDatumGetCString(pgsql_version(NULL /* fcinfo */)));
	sendPrimaryMirrorTransitionResult(statusBuf);
}

static void processTransitionRequest_getFaultInjectStatus(void * buf, int *offsetPtr, int length)
{
#ifdef FAULT_INJECTOR
	init_ps_display("filerep fault inject status process", "", "", "");

	char *faultName = readNextStringFromString(buf, offsetPtr, length);
	bool isDone = false;

	if ( ! faultName)
	{
		sendPrimaryMirrorTransitionResult(PROTOCOL_VIOLATION);
		ereport(COMMERROR, (errcode(ERRCODE_PROTOCOL_VIOLATION),
							errmsg("invalid fault injection status packet (missing some data values)")));
		return;
	}

	if (FaultInjector_IsFaultInjected(FaultInjectorIdentifierStringToEnum(faultName))) {
		isDone = true;
	}

	sendPrimaryMirrorTransitionResult(isDone ? "Success: done" : "Success: waitMore");
#else
	sendPrimaryMirrorTransitionResult("Failure: Fault Injector not available");
#endif
}

static void
processTransitionRequest_faultInject(void * inputBuf, int *offsetPtr, int length)
{
#ifdef FAULT_INJECTOR
	bool wasRead;
	char *faultName = readNextStringFromString(inputBuf, offsetPtr, length);
	char *type = readNextStringFromString(inputBuf, offsetPtr, length);
	char *ddlStatement = readNextStringFromString(inputBuf, offsetPtr, length);
	char *databaseName = readNextStringFromString(inputBuf, offsetPtr, length);
	char *tableName = readNextStringFromString(inputBuf, offsetPtr, length);
	int numOccurrences = readIntFromString(inputBuf, offsetPtr, length, &wasRead);
	int sleepTimeSeconds = readIntFromString(inputBuf, offsetPtr, length, &wasRead);
	FaultInjectorEntry_s	faultInjectorEntry;
	char buf[1000];

	init_ps_display("filerep fault inject process", "", "", "");

	if ( ! wasRead)
	{
		sendPrimaryMirrorTransitionResult(PROTOCOL_VIOLATION);
		ereport(COMMERROR, (errcode(ERRCODE_PROTOCOL_VIOLATION),
							errmsg("invalid fault injection packet (missing some data values)")));
		snprintf(buf, sizeof(buf), "Failure: invalid fault injection packet (missing some data values)");
		goto exit;
	}

	elog(DEBUG1, "FAULT INJECTED: Name %s Type %s, DDL %s, DB %s, Table %s, NumOccurrences %d  SleepTime %d",
		 faultName, type, ddlStatement, databaseName, tableName, numOccurrences, sleepTimeSeconds );

	faultInjectorEntry.faultInjectorIdentifier = FaultInjectorIdentifierStringToEnum(faultName);
	if (faultInjectorEntry.faultInjectorIdentifier == FaultInjectorIdNotSpecified ||
		faultInjectorEntry.faultInjectorIdentifier == FaultInjectorIdMax) {
		ereport(COMMERROR, (errcode(ERRCODE_PROTOCOL_VIOLATION),
							errmsg("could not recognize fault name")));

		snprintf(buf, sizeof(buf), "Failure: could not recognize fault name");
		goto exit;
	}

	faultInjectorEntry.faultInjectorType = FaultInjectorTypeStringToEnum(type);
	if (faultInjectorEntry.faultInjectorType == FaultInjectorTypeNotSpecified ||
		faultInjectorEntry.faultInjectorType == FaultInjectorTypeMax) {
		ereport(COMMERROR, (errcode(ERRCODE_PROTOCOL_VIOLATION),
							errmsg("could not recognize fault type")));

		snprintf(buf, sizeof(buf), "Failure: could not recognize fault type");
		goto exit;
	}

	faultInjectorEntry.sleepTime = sleepTimeSeconds;
	if (sleepTimeSeconds < 0 || sleepTimeSeconds > 7200) {
		ereport(COMMERROR, (errcode(ERRCODE_PROTOCOL_VIOLATION),
							errmsg("invalid sleep time, allowed range [0, 7200 sec]")));

		snprintf(buf, sizeof(buf), "Failure: invalid sleep time, allowed range [0, 7200 sec]");
		goto exit;
	}

	faultInjectorEntry.ddlStatement = FaultInjectorDDLStringToEnum(ddlStatement);
	if (faultInjectorEntry.ddlStatement == DDLMax) {
		ereport(COMMERROR, (errcode(ERRCODE_PROTOCOL_VIOLATION),
							errmsg("could not recognize DDL statement")));

		snprintf(buf, sizeof(buf), "Failure: could not recognize DDL statement");
		goto exit;
	}

	snprintf(faultInjectorEntry.databaseName, sizeof(faultInjectorEntry.databaseName), "%s", databaseName);

	snprintf(faultInjectorEntry.tableName, sizeof(faultInjectorEntry.tableName), "%s", tableName);

	faultInjectorEntry.occurrence = numOccurrences;
	if (numOccurrences > 1000) {
		ereport(COMMERROR, (errcode(ERRCODE_PROTOCOL_VIOLATION),
							errmsg("invalid occurrence number, allowed range [1, 1000]")));

		snprintf(buf, sizeof(buf), "Failure: invalid occurrence number, allowed range [1, 1000]");
		goto exit;
	}


	if (FaultInjector_SetFaultInjection(&faultInjectorEntry) == STATUS_OK) {
		if (faultInjectorEntry.faultInjectorType == FaultInjectorTypeStatus) {
			snprintf(buf, sizeof(buf), "%s", faultInjectorEntry.bufOutput);
		} else {
			snprintf(buf, sizeof(buf), "Success:");
		}
	} else {
		snprintf(buf, sizeof(buf), "Failure: %s", faultInjectorEntry.bufOutput);
	}

exit:
	sendPrimaryMirrorTransitionResult(buf);

#else
	sendPrimaryMirrorTransitionResult("Failure: Fault Injector not available");
#endif

}

/**
 * Called during startup packet processing.
 *
 * Note that we don't worry about freeing memory here because this is
 * called on a new backend which is closed after this operation
 */
static void
processPrimaryMirrorTransitionRequest(Port *port, void *pkt)
{
	PrimaryMirrorTransitionPacket *transition = (PrimaryMirrorTransitionPacket *) pkt;
	void * buf;
	char *targetModeStr;
	int length, offset;

	length = (int)ntohl(transition->dataLength);
	if (length > 10000000 || length < 1)
	{
		sendPrimaryMirrorTransitionResult(PROTOCOL_VIOLATION);
		ereport(COMMERROR, (errcode(ERRCODE_PROTOCOL_VIOLATION),
				errmsg("corrupt length value %d in primary mirror transition packet", length)));
		Assert(0);
		return;
	}

	buf = palloc0(length+1);
	if (pq_getbytes(buf, length) == EOF)
	{
		sendPrimaryMirrorTransitionResult(PROTOCOL_VIOLATION);
		ereport(COMMERROR, (errcode(ERRCODE_PROTOCOL_VIOLATION),
							errmsg("incomplete primary mirror transition packet")));
		return;
	}
	((char*)buf)[length] = '\0';
	offset = 0;

	/* read off the transition requested, see if it is a special named one to handle */
	bool handledRequest = true;
	targetModeStr = readNextStringFromString(buf, &offset, length);
	if (targetModeStr == NULL)
	{
		sendPrimaryMirrorTransitionResult(PROTOCOL_VIOLATION);
		ereport(COMMERROR, (errcode(ERRCODE_PROTOCOL_VIOLATION),
							errmsg("invalid primary mirror transition packet (unable to read target mode value)")));
	}


	else if (strcmp("beginPostmasterReset", targetModeStr) == 0)
	{
		processTransitionRequest_beginPostmasterReset();
	}
	else if (strcmp("getPostmasterResetStatus", targetModeStr) == 0)
	{
		processTransitionRequest_getPostmasterResetStatus();
	}
	else if (strcmp("getStatus", targetModeStr) == 0)
	{
		processTransitionRequest_getStatus();
	}
	else if (strcmp("getCollationAndDataDirSettings", targetModeStr) == 0)
	{
		processTransitionRequest_getCollationAndDataDir();
    }
	else if (strcmp("getVersion", targetModeStr) == 0 )
	{
		processTransitionRequest_getVersion();
	}
	else if (strcmp("getFaultInjectStatus", targetModeStr ) == 0)
	{
		processTransitionRequest_getFaultInjectStatus(buf, &offset, length);
	}
	else if (strcmp("faultInject", targetModeStr) == 0)
	{
		processTransitionRequest_faultInject(buf, &offset, length);
	}
	else
	{
		handledRequest = false;
	}

	if (handledRequest)
	{
		/* return if the request wasn't handled by one of the non-else branches above */
		return;
	}
}

static void
sendPrimaryMirrorTransitionQuery(uint32 mode, uint32 segstate, uint32 datastate, uint32 faulttype)
{
	StringInfoData buf;

	initStringInfo(&buf);

	pq_beginmessage(&buf, '\0');

	pq_sendint(&buf, mode, 4);
	pq_sendint(&buf, segstate, 4);
	pq_sendint(&buf, datastate, 4);
	pq_sendint(&buf, faulttype, 4);

	pq_endmessage(&buf);
	pq_flush();
}

/**
 * Called during startup packet processing.
 *
 * Note that we don't worry about freeing memory here because this is
 * called on a new backend which is closed after this operation
 */
static void
processPrimaryMirrorTransitionQuery(Port *port, void *pkt)
{
	PrimaryMirrorTransitionPacket *transition = (PrimaryMirrorTransitionPacket *) pkt;
	int length;

	PrimaryMirrorMode pm_mode;
	SegmentState_e s_state;
	DataState_e d_state;
	FaultType_e f_type;

	init_ps_display("filerep status query process", "", "", "");

	length = (int)ntohl(transition->dataLength);
	if (length != 0)
	{
		ereport(COMMERROR, (errcode(ERRCODE_PROTOCOL_VIOLATION),
				 errmsg("corrupt length value %d in primary mirror query packet", length)));
		return;
	}

#ifdef FAULT_INJECTOR
	FaultInjector_InjectFaultIfSet
		(
		SegmentProbeResponse,
		DDLNotSpecified,
		"",	//databaseName
		""  // tableName
		)
		;
#endif

	getPrimaryMirrorStatusCodes(&pm_mode, &s_state, &d_state, &f_type);

	/* check if segment is mirrorless primary */
	if (pm_mode == PMModeMirrorlessSegment)
	{
		Assert(d_state == DataStateNotInitialized);

		/* report segment as in sync to conform with master's segment configuration */
		d_state = DataStateInSync;
	}

	sendPrimaryMirrorTransitionQuery((uint32)pm_mode, (uint32)s_state, (uint32)d_state, (uint32)f_type);

	return;
}

/*
 * The client has sent a cancel request packet, not a normal
 * start-a-new-connection packet.  Perform the necessary processing.
 * Nothing is sent back to the client.
 */
static void
processCancelRequest(Port *port, void *pkt)
{
	CancelRequestPacket *canc = (CancelRequestPacket *) pkt;
	int			backendPID;
	long		cancelAuthCode;
	Backend    *bp;

#ifndef EXEC_BACKEND
	Dlelem	   *curr;
#else
	int			i;
#endif

	backendPID = (int) ntohl(canc->backendPID);
	cancelAuthCode = (long) ntohl(canc->cancelAuthCode);

	/*
	 * See if we have a matching backend.  In the EXEC_BACKEND case, we can no
	 * longer access the postmaster's own backend list, and must rely on the
	 * duplicate array in shared memory.
	 */
#ifndef EXEC_BACKEND
	for (curr = DLGetHead(BackendList); curr; curr = DLGetSucc(curr))
	{
		bp = (Backend *) DLE_VAL(curr);
#else
	for (i = MaxLivePostmasterChildren() - 1; i >= 0; i--)
	{
		bp = (Backend *) &ShmemBackendArray[i];
#endif
		if (bp->pid == backendPID)
		{
			if (bp->cancel_key == cancelAuthCode)
			{
				/* Found a match; signal that backend to cancel current op */
				ereport(DEBUG2,
						(errmsg_internal("processing cancel request: sending SIGINT to process %d",
										 backendPID)));
				signal_child(bp->pid, SIGINT);
			}
			else
				/* Right PID, wrong key: no way, Jose */
				ereport(LOG,
						(errmsg("wrong key in cancel request for process %d",
								backendPID)));
			return;
		}
	}

	/* No matching backend */
	ereport(LOG,
			(errmsg("PID %d in cancel request did not match any process",
					backendPID)));
}

/*
 * canAcceptConnections --- check to see if database state allows connections.
 */
static enum CAC_state
canAcceptConnections(void)
{
	/*
	 * Can't start backends when in startup/shutdown/recovery state.
	 */
	if (pmState != PM_RUN)
	{
		if (Shutdown > NoShutdown)
			return CAC_SHUTDOWN;	/* shutdown is pending */
		if (!FatalError &&
			(pmState == PM_STARTUP ||
			 pmState == PM_STARTUP_PASS2 ||
			 pmState == PM_STARTUP_PASS3 ||
			 pmState == PM_STARTUP_PASS4 ||
			 pmState == PM_RECOVERY ||
			 pmState == PM_RECOVERY_CONSISTENT))
			return CAC_STARTUP; /* normal startup */

		return CAC_RECOVERY;	/* else must be crash recovery */
	}

	/*
	 * Don't start too many children.
	 *
	 * We allow more connections than we can have backends here because some
	 * might still be authenticating; they might fail auth, or some existing
	 * backend might exit before the auth cycle is completed. The exact
	 * MaxBackends limit is enforced when a new backend tries to join the
	 * shared-inval backend array.
	 *
	 * The limit here must match the sizes of the per-child-process arrays;
	 * see comments for MaxLivePostmasterChildren().
	 */
	if (CountChildren() >= MaxLivePostmasterChildren())
		return CAC_TOOMANY;

	return CAC_OK;
}


/*
 * ConnCreate -- create a local connection data structure
 */
static Port *
ConnCreate(int serverFd)
{
	Port	   *port;

	if (!(port = (Port *) calloc(1, sizeof(Port))))
	{
		ereport(LOG,
				(errcode(ERRCODE_OUT_OF_MEMORY),
				 errmsg("out of memory")));
		ExitPostmaster(1);
	}

	if (StreamConnection(serverFd, port) != STATUS_OK)
	{
		if (port->sock >= 0)
			StreamClose(port->sock);
		ConnFree(port);
		port = NULL;
	}
	else
	{
		/*
		 * Precompute password salt values to use for this connection. It's
		 * slightly annoying to do this long in advance of knowing whether
		 * we'll need 'em or not, but we must do the random() calls before we
		 * fork, not after.  Else the postmaster's random sequence won't get
		 * advanced, and all backends would end up using the same salt...
		 */
		RandomSalt(port->md5Salt);
	}

	/*
	 * Allocate GSSAPI specific state struct
	 */
#ifndef EXEC_BACKEND
#if defined(ENABLE_GSS) || defined(ENABLE_SSPI)
	port->gss = (pg_gssinfo *) calloc(1, sizeof(pg_gssinfo));
	if (!port->gss)
	{
		ereport(LOG,
				(errcode(ERRCODE_OUT_OF_MEMORY),
				 errmsg("out of memory")));
		ExitPostmaster(1);
	}
#endif
#endif

	return port;
}


/*
 * ConnFree -- free a local connection data structure
 */
static void
ConnFree(Port *conn)
{
#ifdef USE_SSL
	secure_close(conn);
#endif
	if (conn->gss)
		free(conn->gss);
	free(conn);
}


/*
 * ClosePostmasterPorts -- close all the postmaster's open sockets
 *
 * This is called during child process startup to release file descriptors
 * that are not needed by that child process.  The postmaster still has
 * them open, of course.
 *
 * Note: we pass am_syslogger as a boolean because we don't want to set
 * the global variable yet when this is called.
 */
void
ClosePostmasterPorts(bool am_syslogger)
{
	int			i;

	/* Close the listen sockets */
	for (i = 0; i < MAXLISTEN; i++)
	{
		if (ListenSocket[i] != -1)
		{
			StreamClose(ListenSocket[i]);
			ListenSocket[i] = -1;
		}
	}

	/* If using syslogger, close the read side of the pipe */
	if (!am_syslogger)
	{
#ifndef WIN32
		if (syslogPipe[0] >= 0)
			close(syslogPipe[0]);
		syslogPipe[0] = -1;
#else
		if (syslogPipe[0])
			CloseHandle(syslogPipe[0]);
		syslogPipe[0] = 0;
#endif
	}
}


/*
 * reset_shared -- reset shared memory and semaphores
 * @param isReset if true, then this is a reset (as opposed to the initial creation of shared memory on startup)
 */
static void
reset_shared(int port, bool isReset)
{
	if (isReset)
	{
		ereport(LOG,
			(errmsg("resetting shared memory")));
	}
	else
	{
		/* On startup, we get here before we know what file we should be using for logging */
		ereport(DEBUG1,
			(errmsg("setting up shared memory")));
	}

	/*
	 * Create or re-create shared memory and semaphores.
	 *
	 * Note: in each "cycle of life" we will normally assign the same IPC keys
	 * (if using SysV shmem and/or semas), since the port number is used to
	 * determine IPC keys.	This helps ensure that we will clean up dead IPC
	 * objects if the postmaster crashes and is restarted.
	 */
	CreateSharedMemoryAndSemaphores(false, port);

	if (isReset)
	{
		primaryMirrorHandlePostmasterReset();
	}

	setInitialRequestedPrimaryMirrorMode(gInitialMode);
}

/*
 * SIGHUP -- reread config files, and tell children to do same
 */
static void
SIGHUP_handler(SIGNAL_ARGS)
{
	int			save_errno = errno;

	PG_SETMASK(&BlockSig);

	if (Shutdown <= SmartShutdown)
	{
		ereport(LOG,
				(errmsg("received SIGHUP, reloading configuration files"),
				 errOmitLocation(true)));
		ProcessConfigFile(PGC_SIGHUP);
		SignalChildren(SIGHUP);
		signal_child_if_up(StartupPID, SIGHUP);
        signal_child_if_up(StartupPass2PID, SIGHUP);
        signal_child_if_up(StartupPass3PID, SIGHUP);
        signal_child_if_up(StartupPass4PID, SIGHUP);
		signal_child_if_up(BgWriterPID, SIGHUP);
		signal_child_if_up(CheckpointPID, SIGHUP);
		signal_child_if_up(AutoVacPID, SIGHUP);
		signal_child_if_up(PgArchPID, SIGHUP);
		{
			int ii;

			for (ii=0; ii < MaxPMSubType; ii++)
			{
				PMSubProc *subProc = &PMSubProcList[ii];

				signal_child_if_up(subProc->pid, SIGHUP);
			}
		}
		signal_child_if_up(SysLoggerPID, SIGHUP);
		signal_child_if_up(PgStatPID, SIGHUP);

		/* Reload authentication config files too */
		if (!load_hba())
			ereport(WARNING,
					(errmsg("pg_hba.conf not reloaded")));

		load_ident();

#ifdef EXEC_BACKEND
		/* Update the starting-point file for future children */
		write_nondefault_variables(PGC_SIGHUP);
#endif
	}

	PG_SETMASK(&UnBlockSig);

	errno = save_errno;
}


/*
 * pmdie -- signal handler for processing various postmaster signals.
 */
static void
pmdie(SIGNAL_ARGS)
{
	int			save_errno = errno;

	PG_SETMASK(&BlockSig);

	ereport(DEBUG2,
			(errmsg_internal("postmaster received signal %d",
							 postgres_signal_arg)));

	switch (postgres_signal_arg)
	{
		case SIGTERM:

			/*
			 * Smart Shutdown:
			 *
			 * Wait for children to end their work, then shut down.
			 */
			if (Shutdown >= SmartShutdown)
				break;
			Shutdown = SmartShutdown;
			ereport(LOG,
					(errmsg("received smart shutdown request"),
					 errSendAlert(true),
					 errOmitLocation(true)));

			need_call_reaper = true;
			if ( pmState < PM_CHILD_STOP_BEGIN)
			    pmState = PM_CHILD_STOP_BEGIN;
			break;

		case SIGINT:

			/*
			 * Fast Shutdown:
			 *
			 * Abort all children with SIGTERM (rollback active transactions
			 * and exit) and shut down when they are gone.
			 */
			if (Shutdown >= FastShutdown)
				break;
			Shutdown = FastShutdown;
			ereport(LOG,
					(errmsg("received fast shutdown request"),
					 errSendAlert(true),
					 errOmitLocation(true)));

			need_call_reaper = true;
			if ( pmState < PM_CHILD_STOP_BEGIN)
			    pmState = PM_CHILD_STOP_BEGIN;
            else
            {
                /* we were already stopping children ... rerun the signals for the current state to
                 *   ensure that the correct signal level is applied */
                pmStateMachineMustRunActions = true;
            }
            break;

		case SIGQUIT:

			/*
			 * Immediate Shutdown:
			 *
			 * abort all children with SIGQUIT and exit without attempt to
			 * properly shut down data base system.
			 */
			ereport(LOG,
					(errmsg("received immediate shutdown request"),
				     errSendAlert(true),
					 errOmitLocation(true)));

			SignalChildren(SIGQUIT);
			signal_child_if_up(StartupPID, SIGQUIT);
            signal_child_if_up(StartupPass2PID, SIGQUIT);
            signal_child_if_up(StartupPass3PID, SIGQUIT);
            signal_child_if_up(StartupPass4PID, SIGQUIT);
 			StopServices(0, SIGQUIT);
			signal_child_if_up(BgWriterPID, SIGQUIT);
			signal_child_if_up(CheckpointPID, SIGQUIT);
            signal_child_if_up(AutoVacPID, SIGQUIT);
            signal_child_if_up(PgArchPID, SIGQUIT);
            signal_child_if_up(PgStatPID, SIGQUIT);

            /* if you add more processes here then also update do_immediate_shutdown_reaper */

            Shutdown = ImmediateShutdown;
			break;
	}

	if (Debug_print_server_processes)
		elog(LOG,"returning from pmdie");

	PG_SETMASK(&UnBlockSig);

	errno = save_errno;
}

static void
zeroIfPidEqual(int pid, pid_t *ptrToPidToCheckAndZero)
{
    if ( pid == *ptrToPidToCheckAndZero)
    {
        *ptrToPidToCheckAndZero = 0;
    }
}

/* These macros hide platform variations in getting child status.
 * They are used by do_reaper and do_immediate_shutdown_reaper
 */
#ifdef HAVE_WAITPID
	int			status;			/* child exit status */

#define REAPER_LOOPTEST()		((pid = waitpid(-1, &status, WNOHANG)) > 0)
#define REAPER_LOOPHEADER()	(exitstatus = status)
#else							/* !HAVE_WAITPID */
#ifndef WIN32
	union wait	status;			/* child exit status */

#define REAPER_LOOPTEST()		((pid = wait3(&status, WNOHANG, NULL)) > 0)
#define REAPER_LOOPHEADER()	(exitstatus = status.w_status)
#else							/* WIN32 */
#define REAPER_LOOPTEST()		((pid = win32_waitpid(&exitstatus)) > 0)
#define REAPER_LOOPHEADER()
#endif   /* WIN32 */
#endif   /* HAVE_WAITPID */


/**
 * Check for dead processes during immediate shutdown
 */
static void
do_immediate_shutdown_reaper(void)
{
	int         s;
	int			pid;			/* process id of dead child process */
	int			exitstatus;		/* its exit status */

	need_call_reaper = 0;

	ereport(DEBUG4, (errmsg_internal("reaping dead processes")));

	while (REAPER_LOOPTEST())
	{
		REAPER_LOOPHEADER();

        for (Dlelem *curr = DLGetHead(BackendList); curr; curr = DLGetSucc(curr))
        {
            Backend    *bp = (Backend *) DLE_VAL(curr);
            zeroIfPidEqual(pid, &bp->pid);
        }

        zeroIfPidEqual(pid, &StartupPID);
        zeroIfPidEqual(pid, &StartupPass2PID);
        zeroIfPidEqual(pid, &StartupPass3PID);
        zeroIfPidEqual(pid, &StartupPass4PID);

        /* services */
		for (s = 0; s < MaxPMSubType; s++)
		{
            zeroIfPidEqual(pid, &PMSubProcList[s].pid);
        }

        zeroIfPidEqual(pid, &BgWriterPID);
        zeroIfPidEqual(pid, &CheckpointPID);
        zeroIfPidEqual(pid, &AutoVacPID);
        zeroIfPidEqual(pid, &PgArchPID);
        zeroIfPidEqual(pid, &PgStatPID);
    }
}

/*
* Startup succeeded, commence normal operations
*/
static bool CommenceNormalOperations(void)
{
	bool didServiceProcessWork = false;
	int s;

	FatalError = false;
	pmState = PM_RUN;

	/*
	 * Load the flat authorization file into postmaster's cache. The
	 * startup process has recomputed this from the database contents,
	 * so we wait till it finishes before loading it.
	 */
	load_role();

	/*
	 * Crank up the background writer, if we didn't do that already
	 * when we entered consistent recovery state.  It doesn't matter
	 * if this fails, we'll just try again later.
	 */
	if (BgWriterPID == 0)
	{
		SetBGWriterPID(StartBackgroundWriter());
		if (Debug_print_server_processes)
		{
			elog(LOG,"on startup successful: started 'background writer' as pid %ld",
				 (long)BgWriterPID);
		}
	}

	if (CheckpointPID == 0)
	{
		CheckpointPID = StartCheckpointServer();
		if (Debug_print_server_processes)
		{
			elog(LOG,"on startup successful: started 'checkpoint service' as pid %ld",
				 (long)CheckpointPID);
		}
	}

	/*
	 * Likewise, start other special children as needed.  In a restart
	 * situation, some of them may be alive already.
	 */
	if (Shutdown > NoShutdown && BgWriterPID != 0)
		signal_child(BgWriterPID, SIGUSR2);
	else if (Shutdown == NoShutdown)
	{
		if (XLogArchivingActive() && PgArchPID == 0)
		{
			PgArchPID = pgarch_start();
			if (Debug_print_server_processes)
			{
				elog(LOG,"on startup successful: started 'archiver process' as pid %ld",
					 (long)PgArchPID);
				didServiceProcessWork = true;
			}
		}
		if (PgStatPID == 0)
		{
			PgStatPID = pgstat_start();
			if (Debug_print_server_processes)
			{
				elog(LOG,"on startup successful: started 'statistics collector process' as pid %ld",
					 (long)PgStatPID);
				didServiceProcessWork = true;
			}
		}

		for (s = 0; s < MaxPMSubType; s++)
		{
			PMSubProc *subProc = &PMSubProcList[s];

			if (subProc->pid == 0 &&
				ServiceStartable(subProc))
			{
				subProc->pid =
					(subProc->serverStart)();

				if (Debug_print_server_processes)
				{
					elog(LOG,"on startup successful: started '%s' as pid %ld",
						 subProc->procName, (long)subProc->pid);
					didServiceProcessWork = true;
				}
			}
		}
	}

	/* at this point we are really open for business */
	{
		char version[512];

		strcpy(version, PG_VERSION_STR " compiled on " __DATE__ " " __TIME__);

#ifdef USE_ASSERT_CHECKING
		strcat(version, " (with assert checking)");
#endif
		ereport(LOG,(errmsg("%s", version)));


		ereport(LOG,
			 (errmsg("database system is ready to accept connections"),
			  errdetail("%s",version),
			  errSendAlert(true),
			  errOmitLocation(true)));
	}

	return didServiceProcessWork;
}

/*
 * Reaper -- signal handler to cleanup after a child process dies.
 */
static void
reaper(SIGNAL_ARGS)
{
    need_call_reaper = 1;
}

static void do_reaper()
{
	int			save_errno = errno;
	int         s;
	int			pid;			/* process id of dead child process */
	int			exitstatus;		/* its exit status */
	bool        wasServiceProcess = false;
	bool        didServiceProcessWork = false;

	need_call_reaper = 0;

	ereport(DEBUG4,
			(errmsg_internal("reaping dead processes")));

	while (REAPER_LOOPTEST())
	{
		REAPER_LOOPHEADER();

        Assert(pid != 0);

		bool resetRequired = freeProcEntryAndReturnReset(pid);

		if (Debug_print_server_processes)
		{
			char *procName;

			procName = GetServerProcessTitle(pid);
			if (procName != NULL)
			{
				elog(LOG,"'%s' pid %ld exit status %d",
				     procName, (long)pid, exitstatus);
				didServiceProcessWork = true; /* TODO: Should this be set in code that depends on a Debug GUC ? */
			}
		}

		/*
		 * Check if this child was a startup process.
		 */
		if (pid == StartupPID)
		{
			StartupPID = 0;

			Assert(isPrimaryMirrorModeAFullPostmaster(true));

			/*
			 * Unexpected exit of startup process (including FATAL exit)
			 * during PM_STARTUP is treated as catastrophic. There are no
			 * other processes running yet, so we can just exit.
			 */
			if (pmState == PM_STARTUP && !EXIT_STATUS_0(exitstatus))
			{
				LogChildExit(LOG, _("startup process"),
							 pid, exitstatus);
				ereport(LOG,
						(errmsg("aborting startup due to startup process failure")));
				ExitPostmaster(1);
			}

			/*
			 * Startup process exited in response to a shutdown request (or it
			 * completed normally regardless of the shutdown request).
			 */
			if (Shutdown > NoShutdown &&
				(EXIT_STATUS_0(exitstatus) || EXIT_STATUS_1(exitstatus)))
			{
				/* PostmasterStateMachine logic does the rest */
				continue;
			}

			/*
			 * Any unexpected exit (including FATAL exit) of the startup
			 * process is treated as a crash, except that we don't want to
			 * reinitialize.
			 */
			if (!EXIT_STATUS_0(exitstatus))
			{
				RecoveryError = true;
				HandleChildCrash(pid, exitstatus,
								 _("startup process"));
				continue;
			}

			/*
			 * When we are not doing single backend startup, we do
			 * multiple startup passes.
			 *
			 * If Crash Recovery is needed we do Pass 2 and 3.
			 *
			 * Pass 4 is a verification pass done in for a clean shutdown as
			 * well as after Crash Recovery.
			 */
			if (XLogStartupMultipleRecoveryPassesNeeded())
			{
			    Assert(StartupPass2PID == 0);
				StartupPass2PID = StartupPass2DataBase();
				Assert(StartupPass2PID != 0);

				pmState = PM_STARTUP_PASS2;
			}
			else if (XLogStartupIntegrityCheckNeeded())
			{
				/*
				 * Clean shutdown case and wants (and can do) integrity checks.
				 */
			    Assert(StartupPass4PID == 0);
				StartupPass4PID = StartupPass4DataBase();
				Assert(StartupPass4PID != 0);

				pmState = PM_STARTUP_PASS4;
			}
			else
			{
				/*
				 * Startup succeeded, commence normal operations
				 */
				if (CommenceNormalOperations())
					didServiceProcessWork = true;
			}

			continue;
		}

		/*
		 * Check if this child was a startup pass 2 process.
		 */
		if (pid == StartupPass2PID)
		{
			StartupPass2PID = 0;

			Assert(isPrimaryMirrorModeAFullPostmaster(true));

			/*
			 * Unexpected exit of startup process (including FATAL exit)
			 * during PM_STARTUP is treated as catastrophic. There are no
			 * other processes running yet, so we can just exit.
			 */
			if (pmState == PM_STARTUP_PASS2 && !EXIT_STATUS_0(exitstatus))
			{
				LogChildExit(LOG, _("startup pass 2 process"),
							 pid, exitstatus);
				ereport(LOG,
						(errmsg("aborting startup due to startup process failure")));
				ExitPostmaster(1);
			}

			/*
			 * Startup process exited in response to a shutdown request (or it
			 * completed normally regardless of the shutdown request).
			 */
			if (Shutdown > NoShutdown &&
				(EXIT_STATUS_0(exitstatus) || EXIT_STATUS_1(exitstatus)))
			{
				/* PostmasterStateMachine logic does the rest */
				continue;
			}

			/*
			 * Any unexpected exit (including FATAL exit) of the startup
			 * process is treated as a crash, except that we don't want to
			 * reinitialize.
			 */
			if (!EXIT_STATUS_0(exitstatus))
			{
				RecoveryError = true;
				HandleChildCrash(pid, exitstatus,
								 _("startup pass 2 process"));
				continue;
			}

		    Assert(StartupPass3PID == 0);
			StartupPass3PID = StartupPass3DataBase();
			Assert(StartupPass3PID != 0);

			pmState = PM_STARTUP_PASS3;
			continue;
		}

		/*
		 * Check if this child was a startup pass 3 process.
		 */
		if (pid == StartupPass3PID)
		{
			StartupPass3PID = 0;

			Assert(isPrimaryMirrorModeAFullPostmaster(true));

			/*
			 * Unexpected exit of startup process (including FATAL exit)
			 * during PM_STARTUP is treated as catastrophic. There are no
			 * other processes running yet, so we can just exit.
			 */
			if (pmState == PM_STARTUP_PASS3 && !EXIT_STATUS_0(exitstatus))
			{
				LogChildExit(LOG, _("startup pass 3 process"),
							 pid, exitstatus);
				ereport(LOG,
						(errmsg("aborting startup due to startup process failure")));
				ExitPostmaster(1);
			}

			/*
			 * Startup process exited in response to a shutdown request (or it
			 * completed normally regardless of the shutdown request).
			 */
			if (Shutdown > NoShutdown &&
				(EXIT_STATUS_0(exitstatus) || EXIT_STATUS_1(exitstatus)))
			{
				/* PostmasterStateMachine logic does the rest */
				continue;
			}

			/*
			 * Any unexpected exit (including FATAL exit) of the startup
			 * process is treated as a crash, except that we don't want to
			 * reinitialize.
			 */
			if (!EXIT_STATUS_0(exitstatus))
			{
				RecoveryError = true;
				HandleChildCrash(pid, exitstatus,
								 _("startup pass 3 process"));
				continue;
			}

			if (XLogStartupIntegrityCheckNeeded())
			{
				/*
				 * Wants (and can do) integrity checks.
				 */
			    Assert(StartupPass4PID == 0);
				StartupPass4PID = StartupPass4DataBase();
				Assert(StartupPass4PID != 0);

				pmState = PM_STARTUP_PASS4;
			}
			else
			{
				/*
				 * Startup succeeded, commence normal operations
				 */
				if (CommenceNormalOperations())
					didServiceProcessWork = true;
			}
			continue;
		}

		/*
		 * Check if this child was a startup pass 4 process.
		 */
		if (pid == StartupPass4PID)
		{
			StartupPass4PID = 0;

			Assert(isPrimaryMirrorModeAFullPostmaster(true));

			/*
			 * Unexpected exit of startup process (including FATAL exit)
			 * during PM_STARTUP is treated as catastrophic. There are no
			 * other processes running yet, so we can just exit.
			 */
			if (pmState == PM_STARTUP_PASS4 && !EXIT_STATUS_0(exitstatus))
			{
				LogChildExit(LOG, _("startup pass 4 process"),
							 pid, exitstatus);
				ereport(LOG,
						(errmsg("aborting startup due to startup process failure")));
				ExitPostmaster(1);
			}

			/*
			 * Startup process exited in response to a shutdown request (or it
			 * completed normally regardless of the shutdown request).
			 */
			if (Shutdown > NoShutdown &&
				(EXIT_STATUS_0(exitstatus) || EXIT_STATUS_1(exitstatus)))
			{
				/* PostmasterStateMachine logic does the rest */
				continue;
			}

			/*
			 * Any unexpected exit (including FATAL exit) of the startup
			 * process is treated as a crash, except that we don't want to
			 * reinitialize.
			 */
			if (!EXIT_STATUS_0(exitstatus))
			{
				RecoveryError = true;
				HandleChildCrash(pid, exitstatus,
								 _("startup pass 4 process"));
				continue;
			}

			if (PgStatPID == 0)
			{
				PgStatPID = pgstat_start();
				if (Debug_print_server_processes)
					elog(LOG,"on startup successful: started 'statistics collector process' as pid %ld",
						 (long)PgStatPID);
				didServiceProcessWork = true;
			}

			/*
			 * Startup succeeded, commence normal operations
			 */
			if (CommenceNormalOperations())
				didServiceProcessWork = true;
			continue;
		}

		/*
		 * MPP: Was it one of our servers? If so, just try to start a new one;
		 * no need to force reset of the rest of the system. (If fail, we'll
		 * try again in future cycles of the main loop.)
		 */
		wasServiceProcess = false;
		for (s = 0; s < MaxPMSubType; s++)
		{
			PMSubProc *subProc = &PMSubProcList[s];

			if (subProc->pid != 0 &&
				pid == subProc->pid)
			{
				subProc->pid = 0;

				if (!EXIT_STATUS_0(exitstatus))
					LogChildExit(LOG, subProc->procName,
								 pid, exitstatus);

				if (ServiceStartable(subProc))
				{
					if (StartupPidsAllZero() && 
						!FatalError && Shutdown == NoShutdown)
					{
						/*
						 * Before we attempt a restart -- let's make
						 * sure that any backend Proc structures are
						 * tidied up.
						 */
						if (subProc->cleanupBackend == true)
						{
							Assert(subProc->procName && strcmp(subProc->procName, "perfmon process") != 0);
							CleanupBackend(pid, exitstatus, resetRequired);
						}

						/*
						 * MPP-7676, we can't restart during
						 * do_reaper() since we may initiate a
						 * postmaster reset (in which case we'll wind
						 * up waiting for the restarted process to
						 * die). Leave the startup to ServerLoop().
						 */
						/*
						  subProc->pid =
						  (subProc->serverStart)();
						  if (Debug_print_server_processes)
						  {
						  elog(LOG,"restarted '%s' as pid %ld", subProc->procName, (long)subProc->pid);
						  didServiceProcessWork = true;
						  }
						*/
					}
				}

				wasServiceProcess = true;
				break;
			}
		}

		if (wasServiceProcess)
			continue;

		/*
		 * Was it the bgwriter?
		 */
		if (pid == BgWriterPID)
		{
		    SetBGWriterPID(0);

			if (EXIT_STATUS_0(exitstatus) &&
			    (pmState == PM_CHILD_STOP_WAIT_BGWRITER_CHECKPOINT
			    || (FatalError && pmState >= PM_CHILD_STOP_BEGIN)))
			{
			    Assert(Shutdown > NoShutdown);

                /* clean exit: PostmasterStateMachine logic does the rest */
                continue;
			}

            /*
             * Any unexpected exit of the bgwriter (including FATAL exit) is treated as a crash.
             */
            HandleChildCrash(pid, exitstatus,
                             _("background writer process"));

			/*
			 * If the bgwriter crashed while trying to write the shutdown
			 * checkpoint, we may as well just stop here; any recovery
			 * required will happen on next postmaster start.
			 */
            if (Shutdown > NoShutdown &&
				!DLGetHead(BackendList) &&
				AutoVacPID == 0)
			{
				ereport(LOG,
						(errmsg("abnormal database system shutdown"),errSendAlert(true)));
				ExitPostmaster(1);
			}

			/* Else, proceed as in normal crash recovery */
			continue;
		}

		/*
		 * Was it the checkpoint server process?  Normal or FATAL exit can be
		 * ignored; we'll start a new one at the next iteration of the
		 * postmaster's main loop, if necessary.  Any other exit condition
		 * is treated as a crash.
		 */
		if (pid == CheckpointPID)
		{
			CheckpointPID = 0;
			if (!EXIT_STATUS_0(exitstatus) && !EXIT_STATUS_1(exitstatus))
				HandleChildCrash(pid, exitstatus,
								 _("checkpoint process"));
			continue;
		}

		/*
		 * Was it the autovacuum process?  Normal or FATAL exit can be
		 * ignored; we'll start a new one at the next iteration of the
		 * postmaster's main loop, if necessary.  Any other exit condition
		 * is treated as a crash.
		 */
		if (pid == AutoVacPID)
		{
			AutoVacPID = 0;
			autovac_stopped();
			if (!EXIT_STATUS_0(exitstatus) && !EXIT_STATUS_1(exitstatus) && resetRequired)
				HandleChildCrash(pid, exitstatus,
								 _("autovacuum process"));
			continue;
		}

		/*
		 * Was it the archiver?  If so, just try to start a new one; no need
		 * to force reset of the rest of the system.  (If fail, we'll try
		 * again in future cycles of the main loop.)  But if we were waiting
		 * for it to shut down, advance to the next shutdown step.
		 */
		if (pid == PgArchPID)
		{
			PgArchPID = 0;
			if (!EXIT_STATUS_0(exitstatus))
				LogChildExit(LOG, _("archiver process"),
							 pid, exitstatus);
			if (XLogArchivingActive() && pmState == PM_RUN)
			{
				PgArchPID = pgarch_start();
				if (Debug_print_server_processes)
				{
					elog(LOG,"restarted 'archiver process' as pid %ld",
						 (long)PgArchPID);
					didServiceProcessWork = true;
				}
			}
			continue;
		}

		/*
		 * Was it the statistics collector?  If so, just try to start a new
		 * one; no need to force reset of the rest of the system.  (If fail,
		 * we'll try again in future cycles of the main loop.)
		 */
		if (pid == PgStatPID)
		{
			PgStatPID = 0;
			if (!EXIT_STATUS_0(exitstatus))
				LogChildExit(LOG, _("statistics collector process"),
							 pid, exitstatus);
			if (pmState == PM_RUN)
            {
				PgStatPID = pgstat_start();
				if (Debug_print_server_processes)
				{
					elog(LOG,"restarted 'statistics collector process' as pid %ld",
						 (long)PgStatPID);
					didServiceProcessWork = true;
				}
			}
			continue;
		}

		/* Was it the system logger?  If so, try to start a new one */
		if (pid == SysLoggerPID)
		{
			SysLoggerPID = 0;
			/* for safety's sake, launch new logger *first* */
			SysLoggerPID = SysLogger_Start();
			if (Debug_print_server_processes)
			{
				elog(LOG,"restarted 'system logger process' as pid %ld",
					 (long)SysLoggerPID);
				didServiceProcessWork = true; /* TODO: Should this be set in code that depends on a Debug GUC? */
			}
			if (!EXIT_STATUS_0(exitstatus))
				LogChildExit(LOG, _("system logger process"),
							 pid, exitstatus);
			continue;
		}

		/*
		 * Else do standard backend child cleanup.
		 */
		CleanupBackend(pid, exitstatus, resetRequired);
	}							/* loop over pending child-death reports */

	/*
	 * TODO: All of the code in this if block should be in PostmasterStateMachine, and
	 * this code should be removed.  Chuck
	 *
	 * TODO: CHAD_PM should this also proceed through normal child exit state logic and simply
	 *   do something at the right time (when, for example, all post-bgwriters are done) (When that state is
	 *   reached and we  have fatalerror with no shutdown request then do the shared memory reset and restart)
 	 */
	if (FatalError)
	{
		PrimaryMirrorMode pm_mode;
		SegmentState_e s_state;
		DataState_e d_state;
		FaultType_e f_type;
		getPrimaryMirrorStatusCodes(&pm_mode, &s_state, &d_state, &f_type);

		/*
		 * Wait for all important children to exit, then reset shmem and
		 * redo database startup.  (We can ignore the archiver and stats processes
		 * here since they are not connected to shmem.)
		 */
		if (DLGetHead(BackendList) ||
		    StartupPID != 0 ||
		    StartupPass2PID != 0 ||
		    StartupPass3PID != 0 ||
		    StartupPass4PID != 0 ||
		    BgWriterPID != 0 ||
		    CheckpointPID != 0 ||
			AutoVacPID != 0 ||
			ServiceProcessesExist(0))
        {
            /* important child is still going...wait longer */
			goto reaper_done;
        }

        if ( RecoveryError )
        {
    		ereport(LOG,
				(errmsg("database recovery failed; exiting")));

            /*
             * If recovery failed, wait for all non-syslogger children to exit, and
             * then exit postmaster. We don't try to reinitialize when recovery fails,
             * because more than likely it will just fail again and we will keep
             * trying forever.
             */
            ExitPostmaster(1);
        }

        if ( Shutdown > NoShutdown)
        {
    		ereport(LOG,
				(errmsg("error during requested shutdown; exiting")));

            /**
             * We hit a fatal error because of, or prior to, a requested shutdown.  Just exit now.
             */
            ExitPostmaster(1);
        }

		if ( pmState != PM_POSTMASTER_RESET_FILEREP_PEER )
		{
        	BeginResetOfPostmasterAfterChildrenAreShutDown();
		}

        /** fall-through, state machine will handle PM_INIT or PM_POSTMASTER_RESET_FILEREP_PEER  */
	}

	/*
	 * After cleaning out the SIGCHLD queue, see if we have any state changes
	 * or actions to make.
	 */
	PostmasterStateMachine();

reaper_done:

	if (Debug_print_server_processes && didServiceProcessWork)
		elog(LOG,"returning from reaper");

	errno = save_errno;
}

static bool
ServiceProcessesExist(int excludeFlags)
{
	int s;

	for (s=0; s < MaxPMSubType; s++)
	{
		PMSubProc *subProc = &PMSubProcList[s];
		if (subProc->pid != 0 &&
			(subProc->flags & excludeFlags) == 0)
			return true;
	}

	return false;
}

static bool
StopServices(int excludeFlags, int signal)
{
	int s;
	bool signaled = false;

	for (s=0; s < MaxPMSubType; s++)
	{
		PMSubProc *subProc = &PMSubProcList[s];

		if (subProc->pid != 0 &&
			(subProc->flags & excludeFlags) == 0)
		{
			signal_child(subProc->pid, signal);
			signaled = true;
		}
	}

	return signaled;
}

static char *
GetServerProcessTitle(int pid)
{
	int s;

	for (s=0; s < MaxPMSubType; s++)
	{
		PMSubProc *subProc = &PMSubProcList[s];
		if (subProc->pid == pid)
			return subProc->procName;
	}

	if (pid == BgWriterPID)
		return "background writer process";
	if (pid == CheckpointPID)
		return "checkpoint process";
	else if (pid == AutoVacPID)
		return "autovacuum process";
	else if (pid == PgStatPID)
		return "statistics collector process";
	else if (pid == PgArchPID)
		return "archiver process";
	else if (pid == SysLoggerPID)
		return "system logger process";
	else if (pid == StartupPID)
		return "startup process";
    else if (pid == StartupPass2PID)
        return "startup pass 2 process";
    else if (pid == StartupPass3PID)
        return "startup pass 3 process";
    else if (pid == StartupPass4PID)
        return "startup pass 4 process";
	else if (pid == PostmasterPid)
		return "postmaster process";

	return NULL;
}

/*
 * CleanupBackend -- cleanup after terminated backend.
 *
 * Remove all local state associated with backend.
 */
static void
CleanupBackend(int pid,
			   int exitstatus,	/* child's exit status. */
			   bool resetRequired) /* postmaster reset is required */
{
	Dlelem	   *curr;

	LogChildExit(DEBUG2, _("server process"), pid, exitstatus);

	/*
	 * If a backend dies in an ugly way then we must signal all other backends
	 * to quickdie.  If exit status is zero (normal) or one (FATAL exit), we
	 * assume everything is all right and proceed to remove the backend from
	 * the active backend list.
	 */
#ifdef WIN32
	/*
	 * On win32, also treat ERROR_WAIT_NO_CHILDREN (128) as nonfatal
	 * case, since that sometimes happens under load when the process fails
	 * to start properly (long before it starts using shared memory).
	 */
	if (exitstatus == ERROR_WAIT_NO_CHILDREN)
	{
		LogChildExit(LOG, _("server process"), pid, exitstatus);
		exitstatus = 0;
	}
#endif

	if (!EXIT_STATUS_0(exitstatus) && !EXIT_STATUS_1(exitstatus) && resetRequired)
	{
		HandleChildCrash(pid, exitstatus, _("server process"));
		return;
	}

	for (curr = DLGetHead(BackendList); curr; curr = DLGetSucc(curr))
	{
		Backend    *bp = (Backend *) DLE_VAL(curr);

		if (bp->pid == pid)
		{
			if (!bp->dead_end)
			{
				if (!ReleasePostmasterChildSlot(bp->child_slot))
				{
					/*
					 * Uh-oh, the child failed to clean itself up.	Treat as a
					 * crash after all.
					 */
					HandleChildCrash(pid, exitstatus, _("server process"));
					return;
				}
#ifdef EXEC_BACKEND
				ShmemBackendArrayRemove(bp);
#endif
			}
			DLRemove(curr);
			free(bp);
			break;
		}
	}
}

/*
 * HandleChildCrash -- cleanup after failed backend, bgwriter, walwriter,
 * or autovacuum.
 *
 * The objectives here are to clean up our local state about the child
 * process, and to signal all other remaining children to quickdie.
 *
 * @param pid the pid of the crashed child, or -1 if don't know the pid and don't want to log the failed child (this
 *            can happen when the filerep controller process signals us to reset, or when a primary/mirror peer
 *            signals us to reset)
 */
static void
HandleChildCrash(int pid, int exitstatus, const char *procname)
{
	Dlelem	   *curr,
			   *next;
	Backend    *bp;

	/*
	 * Make log entry unless there was a previous crash (if so, nonzero exit
	 * status is to be expected in SIGQUIT response; don't clutter log)
	 */
	if (!FatalError)
	{
		if (pid != -1)
		{
			LogChildExit(LOG, procname, pid, exitstatus);
		}
		ereport(LOG,
				(errmsg("terminating any other active server processes")));
	}

	/* Process regular backends */
	for (curr = DLGetHead(BackendList); curr; curr = next)
	{
		next = DLGetSucc(curr);
		bp = (Backend *) DLE_VAL(curr);
		if (bp->pid == pid)
		{
			/*
			 * Found entry for freshly-dead backend, so remove it.
			 */
			if (!bp->dead_end)
			{
				(void) ReleasePostmasterChildSlot(bp->child_slot);
#ifdef EXEC_BACKEND
				ShmemBackendArrayRemove(bp);
#endif
			}
			DLRemove(curr);
			free(bp);
			/* Keep looping so we can signal remaining backends */
		}
		else
		{
			/*
			 * This backend is still alive.  Unless we did so already, tell it
			 * to commit hara-kiri.
			 *
			 * SIGQUIT is the special signal that says exit without proc_exit
			 * and let the user know what's going on. But if SendStop is set
			 * (-s on command line), then we send SIGSTOP instead, so that we
			 * can get core dumps from all backends by hand.
			 *
			 * We could exclude dead_end children here, but at least in the
			 * SIGSTOP case it seems better to include them.
			 */
			if (!FatalError)
			{
				ereport((Debug_print_server_processes ? LOG : DEBUG2),
						(errmsg_internal("sending %s to process %d",
										 (SendStop ? "SIGSTOP" : "SIGQUIT"),
										 (int) bp->pid)));
				signal_child(bp->pid, (SendStop ? SIGSTOP : SIGQUIT));
			}
		}
	}

	/* Take care of the startup process too */
	if (pid == StartupPID)
		StartupPID = 0;
	else if (StartupPID != 0 && !FatalError)
	{
		ereport(DEBUG2,
				(errmsg_internal("sending %s to process %d",
								 (SendStop ? "SIGSTOP" : "SIGQUIT"),
								 (int) StartupPID)));
		signal_child(StartupPID, (SendStop ? SIGSTOP : SIGQUIT));
	}

	/* Take care of the startup process too */
	if (pid == StartupPass2PID)
		StartupPass2PID = 0;
	else if (StartupPass2PID != 0 && !FatalError)
	{
		ereport(DEBUG2,
				(errmsg_internal("sending %s to process %d",
								 (SendStop ? "SIGSTOP" : "SIGQUIT"),
								 (int) StartupPass2PID)));
		signal_child(StartupPass2PID, (SendStop ? SIGSTOP : SIGQUIT));
	}

	/* Take care of the startup process too */
	if (pid == StartupPass3PID)
		StartupPass3PID = 0;
	else if (StartupPass3PID != 0 && !FatalError)
	{
		ereport(DEBUG2,
				(errmsg_internal("sending %s to process %d",
								 (SendStop ? "SIGSTOP" : "SIGQUIT"),
								 (int) StartupPass3PID)));
		signal_child(StartupPass3PID, (SendStop ? SIGSTOP : SIGQUIT));
	}
	
	/* Take care of the startup process too */
	if (pid == StartupPass4PID)
		StartupPass4PID = 0;
	else if (StartupPass4PID != 0 && !FatalError)
	{
		ereport(DEBUG2,
				(errmsg_internal("sending %s to process %d",
								 (SendStop ? "SIGSTOP" : "SIGQUIT"),
								 (int) StartupPass4PID)));
		signal_child(StartupPass4PID, (SendStop ? SIGSTOP : SIGQUIT));
	}

    /* Take care of the bgwriter too */
	if (pid == BgWriterPID)
    {
        SetBGWriterPID(0);
    }
	else if (BgWriterPID != 0 && !FatalError)
	{
		ereport((Debug_print_server_processes ? LOG : DEBUG2),
				(errmsg_internal("sending %s to process %d",
								 (SendStop ? "SIGSTOP" : "SIGQUIT"),
								 (int) BgWriterPID)));
		signal_child(BgWriterPID, (SendStop ? SIGSTOP : SIGQUIT));
	}

    /* Take care of the checkpoint too */
	if (pid == CheckpointPID)
    {
        CheckpointPID = 0;
    }
	else if (CheckpointPID != 0 && !FatalError)
	{
		ereport((Debug_print_server_processes ? LOG : DEBUG2),
				(errmsg_internal("sending %s to process %d",
								 (SendStop ? "SIGSTOP" : "SIGQUIT"),
								 (int) CheckpointPID)));
		signal_child(CheckpointPID, (SendStop ? SIGSTOP : SIGQUIT));
	}

	/* Take care of the autovacuum daemon too */
	if (pid == AutoVacPID)
		AutoVacPID = 0;
	else if (AutoVacPID != 0 && !FatalError)
	{
		ereport((Debug_print_server_processes ? LOG : DEBUG2),
				(errmsg_internal("sending %s to process %d",
								 (SendStop ? "SIGSTOP" : "SIGQUIT"),
								 (int) AutoVacPID)));
		signal_child(AutoVacPID, (SendStop ? SIGSTOP : SIGQUIT));
	}

	/*
	 * Force a power-cycle of the pgarch process too.  (This isn't absolutely
	 * necessary, but it seems like a good idea for robustness, and it
	 * simplifies the state-machine logic in the case where a shutdown request
	 * arrives during crash processing.)
	 */
	if (PgArchPID != 0 && !FatalError)
	{
		ereport((Debug_print_server_processes ? LOG : DEBUG2),
				(errmsg_internal("sending %s to process %d",
								 "SIGQUIT",
								 (int) PgArchPID)));
		signal_child(PgArchPID, SIGQUIT);
	}

	/* Force a power-cycle of the seqserver process too */
	/* (Shouldn't be necessary, but just for luck) */
	{
		int ii;

		for (ii=0; ii < MaxPMSubType; ii++)
		{
			PMSubProc *subProc = &PMSubProcList[ii];

			if (subProc->pid != 0 && !FatalError)
			{
				ereport((Debug_print_server_processes ? LOG : DEBUG2),
						(errmsg_internal("sending %s to process %d",
										 "SIGQUIT",
										 (int) subProc->pid)));
				signal_child(subProc->pid, SIGQUIT);
			}
		}
	}

	/*
	 * Force a power-cycle of the pgstat process too.  (This isn't absolutely
	 * necessary, but it seems like a good idea for robustness, and it
	 * simplifies the state-machine logic in the case where a shutdown request
	 * arrives during crash processing.)
	 */
	if (PgStatPID != 0 && !FatalError)
	{
		ereport((Debug_print_server_processes ? LOG : DEBUG2),
				(errmsg_internal("sending %s to process %d",
								 "SIGQUIT",
								 (int) PgStatPID)));
		signal_child(PgStatPID, SIGQUIT);
		allow_immediate_pgstat_restart();
	}

	/* We do NOT restart the syslogger */

	FatalError = true;

	/* We now transit into a state of waiting for children to die */
	if (pmState <= PM_RUN )
    {
		pmState = PM_CHILD_STOP_BEGIN;
    }
}


// This is a duplicate of strsignal, for those platforms that have it.
static const char *
signal_to_name(int signal)
{
#if defined(HAVE_DECL_SYS_SIGLIST) && HAVE_DECL_SYS_SIGLIST
	// sys_siglist knows all possible signal names on ths platform.
	if (signal < NSIG)
		return sys_siglist[signal];
	else
		return NULL;
#else
 	switch (signal)
	{
	case SIGINT:	return "SIGINT";
	case SIGTERM:	return "SIGTERM";
	case SIGQUIT:	return "SIGQUIT";
	case SIGSTOP:	return "SIGSTOP";
	case SIGUSR1:	return "SIGUSR1";
	case SIGUSR2:	return "SIGUSR2";
	default:		return NULL;
	}
#endif
}

/*
 * Log the death of a child process.
 */
static void
LogChildExit(int lev, const char *procname, int pid, int exitstatus)
{
    char pidLabel[100];
    sprintf(pidLabel, " (PID %d)", pid);

	if (WIFEXITED(exitstatus))
		ereport(lev,

		/*------
		  translator: %s is a noun phrase describing a child process, such as
		  "server process" */
				(errmsg("%s%s exited with exit code %d",
						procname, pidLabel, WEXITSTATUS(exitstatus))));
	else if (WIFSIGNALED(exitstatus))
#if defined(WIN32)
		ereport(lev,

		/*------
		  translator: %s is a noun phrase describing a child process, such as
		  "server process" */
				(errmsg("%s%s was terminated by exception 0x%X",
						procname, pidLabel, WTERMSIG(exitstatus)),
				 errhint("See C include file \"ntstatus.h\" for a description of the hexadecimal value.")));
#elif defined(HAVE_DECL_SYS_SIGLIST) && HAVE_DECL_SYS_SIGLIST
	ereport(lev,

	/*------
	  translator: %s is a noun phrase describing a child process, such as
	  "server process" */
	// strsignal() is preferred over the deprecated use of sys_siglist, on platforms that support it.
	// Solaris and Linux do support it, but I think MAC OSX doesn't?
			(errmsg("%s%s was terminated by signal %d: %s",
					procname, pidLabel, WTERMSIG(exitstatus),
					WTERMSIG(exitstatus) < NSIG ?
					sys_siglist[WTERMSIG(exitstatus)] : "(unknown)")));
#else
	{
		// If we don't have strsignal or sys_siglist, do our own translation
		const char *signalName;
		signalName = signal_to_name(WTERMSIG(exitstatus));
		if (signalName == NULL)
			signalName = "(unknown)";
		ereport(lev,

		/*------
		  translator: %s is a noun phrase describing a child process, such as
		  "server process" */
			    (errmsg("%s%s was terminated by signal %d: %s",
						procname, pidLabel, WTERMSIG(exitstatus), signalName)));
	}
#endif
	else
		ereport(lev,

		/*------
		  translator: %s is a noun phrase describing a child process, such as
		  "server process" */
				(errmsg("%s%s exited with unrecognized status %d",
						procname, pidLabel, exitstatus)));
}

/**
 * POSTMASTER STATE MACHINE WAITING!
 */

/**
 *  Check to see if PM_CHILD_STOP_WAIT_BACKENDS state is done
 */
static PMState StateMachineCheck_WaitBackends(void)
{
    bool moveToNextState = false;

    Assert(pmState == PM_CHILD_STOP_WAIT_BACKENDS );

    /*
     * If we are in a state-machine state that implies waiting for backends to
     * exit, see if they're all gone, and change state if so.
     */
    if (! isPrimaryMirrorModeAFullPostmaster(true))
    {
        /* wait_backends has ended because we don't have any! */

        moveToNextState = true;
    }
    else
    {
        // note: if wal writer is added, check this here: WalWriterPID == 0 &&
        int childCount = CountChildren();
        bool autovacShutdown = AutoVacPID == 0;

        if (childCount == 0 &&
            (BgWriterPID == 0 || !FatalError) && /* todo: CHAD_PM why wait for BgWriterPID here?  Can't we just allow
                                                          normal state advancement to hit there? */
            autovacShutdown)
        {
            /* note: on fatal error, children are killed all at once by HandleChildCrash, so asserts are not valid */
            if ( ! FatalError )
            {
                Assert(StartupPID == 0);
            }

            /*
             * PM_CHILD_STOP_WAIT_BACKENDS state ends when we have no regular backends
             * (including autovac workers) and no walwriter or autovac launcher.
             * If we are doing crash recovery then we expect the bgwriter to exit
             * too, otherwise not.	The archiver, stats, and syslogger processes
             * are disregarded since they are not connected to shared memory; we
             * also disregard dead_end children here.
             */
            if (FatalError)
            {
                moveToNextState = true;
            }
            else
            {
                /*
                 * This state change causes ServerLoop to stop creating new ones.
                 */
                Assert(Shutdown > NoShutdown);
                moveToNextState = true;
            }
        }
        else
        {
            if ( Debug_print_server_processes )
            {
                elog(LOG, "Postmaster State Machine: waiting on backends, children %d, av %s",
                    childCount,
                    autovacShutdown ? "stopped" : "running" );
            }
        }
    }

    return moveToNextState ? (pmState+1) : pmState;
}

/**
 * Check to see if PM_POSTMASTER_RESET_FILEREP_PEER state is done,
 *
 *  possibly
 */
static PMState StateMachineCheck_WaitFilerepPeerReset(void)
{
	Assert(pmState == PM_POSTMASTER_RESET_FILEREP_PEER );
	switch (peerResetResult)
	{
		case PEER_RESET_NONE:
			/* no change */
			break;
		case PEER_RESET_FAILED:

			/* mark fault in shared memory */
			setTransitionToFault();

			RestartAfterPostmasterReset();
			break;
		case PEER_RESET_SUCCEEDED:
			RestartAfterPostmasterReset();
			break;
	}
	return pmState;
}

/**
 * Check to see if PM_CHILD_STOP_WAIT_DEAD_END_CHILDREN state is done
 */
static PMState StateMachineCheck_WaitDeadEndChildren(void)
{
    bool moveToNextState = false;
    Assert(pmState == PM_CHILD_STOP_WAIT_DEAD_END_CHILDREN );

    /*
     * PM_CHILD_STOP_WAIT_DEAD_END_CHILDREN state ends when the BackendList is entirely empty
     * (ie, no dead_end children remain), and the archiver and stats
     * collector are gone too.
     *
     * The reason we wait for those two is to protect them against a new
     * postmaster starting conflicting subprocesses; this isn't an
     * ironclad protection, but it at least helps in the
     * shutdown-and-immediately-restart scenario.  Note that they have
     * already been sent appropriate shutdown signals, either during a
     * normal state transition leading up to PM_WAIT_DEAD_END, or during
     * FatalError processing.
     */
    if (DLGetHead(BackendList) == NULL)
    {
        /* These other guys should be dead already */
        /* todo: ALL other processes should be dead at this point */
        /* note: on fatal error, children are killed all at once by HandleChildCrash, so asserts are not valid */
        if ( ! FatalError )
        {
            Assert(StartupPID == 0);
            Assert(BgWriterPID == 0);
            Assert(CheckpointPID == 0);
            Assert(AutoVacPID == 0);
        }
        /* syslogger is not considered here */
        moveToNextState = true;

    }

    return moveToNextState ? (pmState+1) : pmState;
}

/**
 * Check to see if PM_CHILD_STOP_WAIT_STARTUP_PROCESSES state is done
 */
static PMState StateMachineCheck_WaitStartupProcesses(void)
{
    Assert(pmState == PM_CHILD_STOP_WAIT_STARTUP_PROCESSES);

    bool moveToNextState = StartupPID == 0 && StartupPass2PID == 0 && StartupPass3PID == 0 && StartupPass4PID == 0;
    return moveToNextState ? (pmState+1) : pmState;
}

/**
 * Check to see if PM_CHILD_STOP_WAIT_PREBGWRITER state is done
 */
static PMState StateMachineCheck_WaitPreBgWriter(void)
{
    Assert(pmState == PM_CHILD_STOP_WAIT_PREBGWRITER );

    /* waiting for pre-bgwriter services to exit */
    bool moveToNextState = 
    		(!ServiceProcessesExist(/* excludeFlags */ PMSUBPROC_FLAG_STOP_AFTER_BGWRITER) &&
    		 CheckpointPID == 0);
    return moveToNextState ? (pmState+1) : pmState;
}

/**
 * Check to see if PM_CHILD_STOP_WAIT_POSTBGWRITER state is done
 */
static PMState StateMachineCheck_WaitPostBgWriter(void)
{
    Assert(pmState == PM_CHILD_STOP_WAIT_POSTBGWRITER);

    /* waiting for post-bgwriter services to exit */
    bool moveToNextState = true;

    /* note: on fatal error, children are killed all at once by HandleChildCrash, so asserts are not valid */
    if ( ! FatalError )
    {
        Assert(BgWriterPID == 0);
    }

    if ( ServiceProcessesExist(/* excludeFlags */ 0))
        moveToNextState = false;
    else if (PgArchPID  != 0 ||
        PgStatPID  != 0)
    {
        moveToNextState = false;
    }
    return moveToNextState ? (pmState+1) : pmState;
}

/**
 * Check to see if PM_CHILD_STOP_WAIT_BGWRITER_CHECKPOINT state is done
 */
static PMState StateMachineCheck_WaitBgWriterCheckpointComplete(void)
{
    Assert(pmState == PM_CHILD_STOP_WAIT_BGWRITER_CHECKPOINT);

    /* waiting for bgwriter to FINISH ckpt and exit */
    bool moveToNextState = BgWriterPID == 0;

    return moveToNextState ? (pmState+1) : pmState;
}

/***************************************
 * POSTMASTER STATE MACHINE TRANSITIONS
 ***************************************/

/**
 * Called to transition to PM_CHILD_STOP_WAIT_STARTUP_PROCESSES -- so tell any startup processes to complete
 */
static void StateMachineTransition_ShutdownStartupProcesses(void)
{
    if ( Shutdown == FastShutdown )
    {
        signal_child_if_up(StartupPID, SIGTERM);
        signal_child_if_up(StartupPass2PID, SIGTERM);
        signal_child_if_up(StartupPass3PID, SIGTERM);
        signal_child_if_up(StartupPass4PID, SIGTERM);
    }
    else
    {
        /* else: normal shutdown, wait for startup to complete naturally */
    }

}

/**
 * Called to transition to PM_CHILD_STOP_WAIT_BACKENDS : waiting for all backends to finish
 */
static void StateMachineTransition_ShutdownBackends(void)
{
    if ( Shutdown == FastShutdown )
    {
        /* shut down all backend, including autovac workers */
        SignalChildren(SIGTERM);
    }
    else
    {
        /* only autovacuum workers are told to shut down immediately */
        SignalAutovacWorkers(SIGTERM);
    }

    /* and the autovac launcher too */
    signal_child_if_up(AutoVacPID, SIGTERM);
}

/**
 * Called to transition to PM_CHILD_STOP_WAIT_PREBGWRITER : waiting for pre-bgwriter processes to finish
 */
static void StateMachineTransition_ShutdownPreBgWriter(void)
{
    /* SIGUSR2, regardless of shutdown mode */
    StopServices(/* excludeFlags */ PMSUBPROC_FLAG_STOP_AFTER_BGWRITER, SIGUSR2 );
	signal_child_if_up(CheckpointPID, SIGUSR2);
}

/**
 * Called to transition to PM_CHILD_STOP_WAIT_BGWRITER_CHECKPOINT : waiting for bgwriter to run a checkpoint and quit
 */
static void StateMachineTransition_ShutdownBgWriterWithCheckpoint(void)
{
    if (! FatalError &&
        ! RecoveryError &&
        ! SyncMaster &&
        isFullPostmasterAndDatabaseIsAllowed())
    {
        /* Start the bgwriter if not running */
        if (BgWriterPID == 0 )
        {
            SetBGWriterPID(StartBackgroundWriter());
        }
    }

    /* SIGUSR2, regardless of shutdown mode */
    signal_child_if_up(BgWriterPID, SIGUSR2);
}

/**
 * Called to transition to PM_CHILD_STOP_WAIT_POSTBGWRITER  : waiting for post-bgwriter services to exit
 */
static void StateMachineTransition_ShutdownPostBgWriter(void)
{
    /* these services take the same signal regardless of fast vs smart shutdown */
    StopServices(/* excludeFlags */ 0, SIGUSR2);
    signal_child_if_up(PgArchPID, SIGQUIT);
    signal_child_if_up(PgStatPID, SIGQUIT);
}

/**
 * Called to transition tp PM_CHILD_STOP_WAIT_DEAD_END_CHILDREN : waiting for dead-end children to complete
 */
static void StateMachineTransition_ShutdownDeadEndChildren(void)
{
    /* nothing to do, just waiting */
}

/**
 * Called to transition to final PM_CHILD_STOP_WAIT_NO_CHILDREN state.  Note that in some cases this will
 *   actually exit the process because a requested shutdown is complete!
 */
static void StateMachineTransition_NoChildren(void)
{
     Assert(pmState == PM_CHILD_STOP_WAIT_NO_CHILDREN);
     if(Shutdown > NoShutdown)
     {
         /*
          * If we've been told to shut down, we exit as soon as there are no
          * remaining children.	If there was a crash, cleanup will occur at the
          * next startup.  (Before PostgreSQL 8.3, we tried to recover from the
          * crash before exiting, but that seems unwise if we are quitting because
          * we got SIGTERM from init --- there may well not be time for recovery
          * before init decides to SIGKILL us.)
          *
          * Note that the syslogger continues to run.  It will exit when it sees
          * EOF on its input pipe, which happens when there are no more upstream
          * processes.
          */
        if (FatalError)
        {
            ereport(LOG, (errmsg("abnormal database system shutdown"),errSendAlert(true)));
            ExitPostmaster(1);
        }
        else
        {
            /*
             * Terminate backup mode to avoid recovery after a clean fast
             * shutdown.
             */
             /* GPDB doesn't have backup mode, so nothing to cancel */
            //CancelBackup();

            /* Normal exit from the postmaster is here */
            ExitPostmaster(0);
        }
    }
    else if (RecoveryError)
    {
        ereport(LOG,
            (errmsg("database recovery failed; exiting")));

        /*
         * If recovery failed, wait for all non-syslogger children to exit, and
         * then exit postmaster. We don't try to reinitialize when recovery fails,
         * because more than likely it will just fail again and we will keep
         * trying forever.
         */
        ExitPostmaster(1);
    }
    else if (FatalError)
    {
    	BeginResetOfPostmasterAfterChildrenAreShutDown();
    }
    else
    {
        Assert(!"NoChildren state reached when not in shutdown and not experiencing a fatal error");
    }
}


 /*
 * Advance the postmaster's state machine and take actions as appropriate
 *
 *
 * Right now, this state machine is concerned with the orderly shutdown sequence.
 * We should be able to make it cover startup and PM_RUN as well (where we restart crashed processes)
 *
 * Note also that because of the fact that it only handles orderly shutdown directly, pmState is
 *   sometimes changed by a transition or wait function (as when resetting the filerep peer during postmaster
 *   reset).  This could probably be structured better so it all worked better
 *
 *
 * This is common code for pmdie() and reaper(), which receive the signals
 * that might mean we need to change state.
 *
 */
static void
PostmasterStateMachine(void)
{
    int passThroughStateMachine;
    const int minStateForLogging = PM_CHILD_STOP_BEGIN;

#define DO_STATE_MACHINE_LOGGING (pmState >= minStateForLogging && Debug_print_server_processes)

    for (passThroughStateMachine = 0;; passThroughStateMachine++)
    {
        int nextPmState = pmState;
        Assert(passThroughStateMachine < 100);

        if (DO_STATE_MACHINE_LOGGING)
        {
            elog(LOG, "Postmaster State Machine: checking state %s", gPmStateLabels[pmState]);
        }

        /* first, look at the current pmState and see if that state has been completed */
        switch ( pmState )
        {
            case PM_INIT:
            case PM_STARTUP:
            case PM_STARTUP_PASS2:
			case PM_STARTUP_PASS3:
			case PM_STARTUP_PASS4:
            case PM_RECOVERY:
            case PM_RECOVERY_CONSISTENT:
            case PM_RUN:
                nextPmState = pmState;
                break;

            case PM_CHILD_STOP_BEGIN:
                nextPmState = pmState + 1; /* immediately move to next step and run its transition actions ... */
                break;

            /* shutdown has been requested -- check to make sure startup processes are done */
            case PM_CHILD_STOP_WAIT_STARTUP_PROCESSES:
                nextPmState = StateMachineCheck_WaitStartupProcesses();
                break;

            /* waiting for live backends to exit */
            case PM_CHILD_STOP_WAIT_BACKENDS:
                nextPmState = StateMachineCheck_WaitBackends();
                break;

            /* waiting for pre-backend services to exit */
            case PM_CHILD_STOP_WAIT_PREBGWRITER:
                nextPmState = StateMachineCheck_WaitPreBgWriter();
                break;

            /* waiting for bgwriter to do shutdown ckpt */
            case PM_CHILD_STOP_WAIT_BGWRITER_CHECKPOINT:
                nextPmState = StateMachineCheck_WaitBgWriterCheckpointComplete();
                break;

            /* waiting for pre-backend services to exit */
            case PM_CHILD_STOP_WAIT_POSTBGWRITER:
                nextPmState = StateMachineCheck_WaitPostBgWriter();
                break;

            /* waiting for dead_end children to exit */
            case PM_CHILD_STOP_WAIT_DEAD_END_CHILDREN:
                nextPmState = StateMachineCheck_WaitDeadEndChildren();
                break;

            case PM_CHILD_STOP_WAIT_NO_CHILDREN:
                Assert(!"Unreachable state");
                break;

          	case PM_POSTMASTER_RESET_FILEREP_PEER:
          		nextPmState = StateMachineCheck_WaitFilerepPeerReset();
                break;

            default:
                Assert(!"Invalid pmState");
                break;
        }

        if ( nextPmState != pmState )
        {
            pmState = nextPmState;

            if ( DO_STATE_MACHINE_LOGGING )
                elog(LOG, "Postmaster State Machine: entering state %s", gPmStateLabels[pmState]);
        }
        else if ( pmStateMachineMustRunActions)
        {
            if ( DO_STATE_MACHINE_LOGGING )
                elog(LOG, "Postmaster State Machine: rerunning actions for %s", gPmStateLabels[pmState]);
        }
        else
        {
            if ( DO_STATE_MACHINE_LOGGING )
                elog(LOG, "Postmaster State Machine: no change from %s", gPmStateLabels[pmState]);
                
            /* current state was not completed ... quit and wait to be called again */
            break;
        }

        pmStateMachineMustRunActions = false;

        switch ( pmState )
        {
            case PM_INIT:
            case PM_STARTUP:
            case PM_STARTUP_PASS2:
			case PM_STARTUP_PASS3:
			case PM_STARTUP_PASS4:
            case PM_RECOVERY:
            case PM_RECOVERY_CONSISTENT:
            case PM_RUN:
                break;

            case PM_CHILD_STOP_BEGIN:
                Assert("Unreachable state; the only change of pmState to PM_CHILD_STOP_BEGIN is in the pmdie handler.");
                break;

            case PM_CHILD_STOP_WAIT_STARTUP_PROCESSES:
                /* shutdown has been requested -- check to make sure startup processes are done */
                StateMachineTransition_ShutdownStartupProcesses();
                break;

            case PM_CHILD_STOP_WAIT_BACKENDS:
                /* tell live backends to exit */
                StateMachineTransition_ShutdownBackends();
                break;

            case PM_CHILD_STOP_WAIT_PREBGWRITER :
                /* tell pre-bgwriter services to exit */
                StateMachineTransition_ShutdownPreBgWriter();
                break;

            case PM_CHILD_STOP_WAIT_BGWRITER_CHECKPOINT:
                /* tell bgwriter to do shutdown ckpt and exit */
                StateMachineTransition_ShutdownBgWriterWithCheckpoint();
                break;

            case PM_CHILD_STOP_WAIT_POSTBGWRITER :
                /* tell post-bgwriter services to exit */
                StateMachineTransition_ShutdownPostBgWriter();
                break;

            case PM_CHILD_STOP_WAIT_DEAD_END_CHILDREN:
                /* waiting for dead_end children to exit */
                StateMachineTransition_ShutdownDeadEndChildren();
                break;

            case PM_CHILD_STOP_WAIT_NO_CHILDREN:
                /* all children are done, now we can exit or restart after crash, perhaps set up a transition to
                 *  StateMachineTransition_ResetFilerepPeer
                 */
                StateMachineTransition_NoChildren();
                break;

          	case PM_POSTMASTER_RESET_FILEREP_PEER:
          		Assert(!"Invalid pmState PM_POSTMASTER_RESET_FILEREP_PEER : "
          				"is not reached through a nextPmState assignment");
          		break;

            default:
                Assert(!"Invalid pmState");
                break;
        }

        /* and now loop again to see if the new pmState has already been satisfied, most likely if our
         *   current actions on entering the state were no-ops */
    }
}


/*
 * Send a signal to a postmaster child process
 *
 * On systems that have setsid(), each child process sets itself up as a
 * process group leader.  For signals that are generally interpreted in the
 * appropriate fashion, we signal the entire process group not just the
 * direct child process.  This allows us to, for example, SIGQUIT a blocked
 * archive_recovery script, or SIGINT a script being run by a backend via
 * system().
 *
 * There is a race condition for recently-forked children: they might not
 * have executed setsid() yet.	So we signal the child directly as well as
 * the group.  We assume such a child will handle the signal before trying
 * to spawn any grandchild processes.  We also assume that signaling the
 * child twice will not cause any problems.
 */
static void
signal_child(pid_t pid, int signal)
{

	if (Debug_print_server_processes)
	{
		char *procName;

		procName = GetServerProcessTitle(pid);
		if (procName != NULL)
		{
			const char *signalName;

			signalName = signal_to_name(signal);
			if (signalName != NULL)
				elog(LOG,"signal %s sent to '%s' pid %ld",
				     signalName, procName, (long)pid);
			else
				elog(LOG,"signal %d sent to '%s' pid %ld",
				     signal, procName, (long)pid);
		}
	}

	if (kill(pid, signal) < 0)
		elog(DEBUG3, "kill(%ld,%d) failed: %m", (long) pid, signal);
#ifdef HAVE_SETSID
	switch (signal)
	{
		case SIGINT:
		case SIGTERM:
		case SIGQUIT:
		case SIGSTOP:
			if (kill(-pid, signal) < 0)
				elog(DEBUG3, "kill(%ld,%d) failed: %m", (long) (-pid), signal);
			break;
		default:
			break;
	}
#endif
}

/*
 * Send a signal to all backend children, including autovacuum workers
 * (but NOT special children; dead_end children are never signaled, either).
 * If only_autovac is TRUE, only the autovacuum worker processes are signalled.
 */
static void
SignalSomeChildren(int signal, bool only_autovac)
{
	Dlelem	   *curr;

	for (curr = DLGetHead(BackendList); curr; curr = DLGetSucc(curr))
	{
		Backend    *bp = (Backend *) DLE_VAL(curr);

		if (bp->dead_end)
			continue;
		if (only_autovac && !bp->is_autovacuum)
			continue;

		ereport((Debug_print_server_processes ? LOG : DEBUG4),
				(errmsg_internal("sending signal %d to process %d",
								 signal, (int) bp->pid)));
		signal_child(bp->pid, signal);
	}
}

/*
 * BackendStartup -- start backend process
 *
 * returns: STATUS_ERROR if the fork failed, STATUS_OK otherwise.
 *
 * Note: if you change this code, also consider StartAutovacuumWorker.
 */
static int
BackendStartup(Port *port)
{
	Backend    *bn;				/* for backend cleanup */
	pid_t		pid;

	/*
	 * Create backend data structure.  Better before the fork() so we can
	 * handle failure cleanly.
	 */
	bn = (Backend *) malloc(sizeof(Backend));
	if (!bn)
	{
		ereport(LOG,
				(errcode(ERRCODE_OUT_OF_MEMORY),
				 errmsg("out of memory")));
		return STATUS_ERROR;
	}

	/*
	 * Compute the cancel key that will be assigned to this backend. The
	 * backend will have its own copy in the forked-off process' value of
	 * MyCancelKey, so that it can transmit the key to the frontend.
	 */
	MyCancelKey = PostmasterRandom();
	bn->cancel_key = MyCancelKey;

	/* Pass down canAcceptConnections state */
	port->canAcceptConnections = canAcceptConnections();
	bn->dead_end = (port->canAcceptConnections != CAC_OK);

	/*
	 * Unless it's a dead_end child, assign it a child slot number
	 */
	if (!bn->dead_end)
		bn->child_slot = MyPMChildSlot = AssignPostmasterChildSlot();
	else
		bn->child_slot = 0;

#ifdef EXEC_BACKEND
	pid = backend_forkexec(port);
#else							/* !EXEC_BACKEND */
	pid = fork_process();

	if (pid == 0)				/* child */
	{
		free(bn);

		/*
		 * Let's clean up ourselves as the postmaster child, and close the
		 * postmaster's listen sockets.  (In EXEC_BACKEND case this is all
		 * done in SubPostmasterMain.)
		 */
		IsUnderPostmaster = true;		/* we are a postmaster subprocess now */

		MyProcPid = getpid();	/* reset MyProcPid */

		MyStartTime = time(NULL);

		/* We don't want the postmaster's proc_exit() handlers */
		on_exit_reset();

		/* Close the postmaster's sockets */
		ClosePostmasterPorts(false);

		/* Perform additional initialization and client authentication */
		BackendInitialize(port);

		/* And run the backend */
		int brres = BackendRun(port);

		cleanupQD2RMComm();

		proc_exit(brres);
	}
#endif   /* EXEC_BACKEND */

	if (pid < 0)
	{
		/* in parent, fork failed */
		int			save_errno = errno;

		if (!bn->dead_end)
			(void) ReleasePostmasterChildSlot(bn->child_slot);
		free(bn);
		errno = save_errno;
		ereport(LOG,
				(errmsg("could not fork new process for connection: %m")));
		report_fork_failure_to_client(port, save_errno);
		return STATUS_ERROR;
	}

	/* in parent, successful fork */
	ereport(DEBUG2,
			(errmsg_internal("forked new backend, pid=%d socket=%d",
							 (int) pid, port->sock)));

	/*
	 * Everything's been successful, it's safe to add this backend to our list
	 * of backends.
	 */
	bn->pid = pid;
	bn->is_autovacuum = false;
	DLInitElem(&bn->elem, bn);
	DLAddHead(BackendList, &bn->elem);
#ifdef EXEC_BACKEND
	if (!bn->dead_end)
	    ShmemBackendArrayAdd(bn);
#endif

	return STATUS_OK;
}

/*
 * Try to report backend fork() failure to client before we close the
 * connection.	Since we do not care to risk blocking the postmaster on
 * this connection, we set the connection to non-blocking and try only once.
 *
 * This is grungy special-purpose code; we cannot use backend libpq since
 * it's not up and running.
 */
static void
report_fork_failure_to_client(Port *port, int errnum)
{
	char		buffer[1000];
	int			rc;

	/* Format the error message packet (always V2 protocol) */
	snprintf(buffer, sizeof(buffer), "E%s%s\n",
			 _("could not fork new process for connection: "),
			 strerror(errnum));

	/* Set port to non-blocking.  Don't do send() if this fails */
	if (!pg_set_noblock(port->sock))
		return;

	/* We'll retry after EINTR, but ignore all other failures */
	do
	{
		rc = send(port->sock, buffer, strlen(buffer) + 1, 0);
	} while (rc < 0 && errno == EINTR);
}


/*
 * split_opts -- split a string of options and append it to an argv array
 *
 * NB: the string is destructively modified!
 *
 * Since no current POSTGRES arguments require any quoting characters,
 * we can use the simple-minded tactic of assuming each set of space-
 * delimited characters is a separate argv element.
 *
 * If you don't like that, well, we *used* to pass the whole option string
 * as ONE argument to execl(), which was even less intelligent...
 */
static void
split_opts(char **argv, int *argcp, char *s)
{
	while (s && *s)
	{
		while (isspace((unsigned char) *s))
			++s;
		if (*s == '\0')
			break;
		argv[(*argcp)++] = s;
		while (*s && !isspace((unsigned char) *s))
			++s;
		if (*s)
			*s++ = '\0';
	}
}


/*
 * BackendInitialize -- initialize an interactive (postmaster-child)
 *				backend process, and perform client authentication.
 *
 * returns: nothing.  Will not return at all if there's any failure.
 *
 * Note: this code does not depend on having any access to shared memory.
 * In the EXEC_BACKEND case, we are physically attached to shared memory
 * but have not yet set up most of our local pointers to shmem structures.
 */
static void
BackendInitialize(Port *port)
{
	int			status;
	char		remote_host[NI_MAXHOST];
	char		remote_port[NI_MAXSERV];
	char		remote_ps_data[NI_MAXHOST];
	MemoryContext	old;

	/* Save port etc. for ps status */
	MyProcPort = port;
	old = MemoryContextSwitchTo(TopMemoryContext);
	initStringInfo(&MyProcPort->override_options);
	MemoryContextSwitchTo(old);

	/*
	 * PreAuthDelay is a debugging aid for investigating problems in the
	 * authentication cycle: it can be set in postgresql.conf to allow time to
	 * attach to the newly-forked backend with a debugger. (See also the -W
	 * backend switch, which we allow clients to pass through PGOPTIONS, but
	 * it is not honored until after authentication.)
	 */
	if (PreAuthDelay > 0)
		pg_usleep(PreAuthDelay * 1000000L);

	/* This flag will remain set until InitPostgres finishes authentication */
	ClientAuthInProgress = true;	/* limit visibility of log messages */

	/* save process start time */
	port->SessionStartTime = GetCurrentTimestamp();
	MyStartTime = timestamptz_to_time_t(port->SessionStartTime);

	/* set these to empty in case they are needed before we set them up */
	port->remote_host = "";
	port->remote_port = "";

	/*
	 * Initialize libpq and enable reporting of ereport errors to the client.
	 * Must do this now because authentication uses libpq to send messages.
	 */
	pq_init();					/* initialize libpq to talk to client */
	whereToSendOutput = DestRemote;		/* now safe to ereport to client */

	/*
	 * If possible, make this process a group leader, so that the postmaster
	 * can signal any child processes too.	(We do this now on the off chance
	 * that something might spawn a child process during authentication.)
	 */
#ifdef HAVE_SETSID
	if (setsid() < 0)
		elog(FATAL, "setsid() failed: %m");
#endif

	/*
	 * We arrange for a simple exit(1) if we receive SIGTERM or SIGQUIT during
	 * any client authentication related communication. Otherwise the
	 * postmaster cannot shutdown the database FAST or IMMED cleanly if a
	 * buggy client blocks a backend during authentication.
	 */
	pqsignal(SIGTERM, authdie);
	pqsignal(SIGQUIT, authdie);
	pqsignal(SIGALRM, authdie);
	PG_SETMASK(&StartupBlockSig);

	/*
	 * Get the remote host name and port for logging and status display.
	 */
	remote_host[0] = '\0';
	remote_port[0] = '\0';
	if (pg_getnameinfo_all(&port->raddr.addr, port->raddr.salen,
						   remote_host, sizeof(remote_host),
						   remote_port, sizeof(remote_port),
					   (log_hostname ? 0 : NI_NUMERICHOST) | NI_NUMERICSERV))
	{
		int			ret = pg_getnameinfo_all(&port->raddr.addr, port->raddr.salen,
											 remote_host, sizeof(remote_host),
											 remote_port, sizeof(remote_port),
											 NI_NUMERICHOST | NI_NUMERICSERV);

		if (ret)
			ereport(WARNING,
					(errmsg_internal("pg_getnameinfo_all() failed: %s",
									 gai_strerror(ret))));
	}
	snprintf(remote_ps_data, sizeof(remote_ps_data),
			 remote_port[0] == '\0' ? "%s" : "%s(%s)",
			 remote_host, remote_port);

	if (Log_connections)
		ereport(LOG,
				(errmsg("connection received: host=%s%s%s",
						remote_host, remote_port[0] ? " port=" : "",
						remote_port),
				 errOmitLocation(true)));

	/*
	 * save remote_host and remote_port in port structure
	 */
	port->remote_host = strdup(remote_host);
	port->remote_port = strdup(remote_port);

	/*
	 * In EXEC_BACKEND case, we didn't inherit the contents of pg_hba.conf
	 * etcetera from the postmaster, and have to load them ourselves. Build
	 * the PostmasterContext (which didn't exist before, in this process) to
	 * contain the data.
	 *
	 * FIXME: [fork/exec] Ugh.	Is there a way around this overhead?
	 */
#ifdef EXEC_BACKEND
	Assert(PostmasterContext == NULL);
	PostmasterContext = AllocSetContextCreate(TopMemoryContext,
											  "Postmaster",
											  ALLOCSET_DEFAULT_MINSIZE,
											  ALLOCSET_DEFAULT_INITSIZE,
											  ALLOCSET_DEFAULT_MAXSIZE);
	MemoryContextSwitchTo(PostmasterContext);

	if (!load_hba())
	{
		/*
		 * It makes no sense to continue if we fail to load the HBA file,
		 * since there is no way to connect to the database in this case.
		 */
		ereport(FATAL,
				(errmsg("could not load pg_hba.conf")));
	}
	load_ident();
	load_role();
#endif

	/*
	 * Ready to begin client interaction.  We will give up and exit(0) after a
	 * time delay, so that a broken client can't hog a connection
	 * indefinitely.  PreAuthDelay doesn't count against the time limit.
	 */
	if (!enable_sig_alarm(AuthenticationTimeout * 1000, false))
		elog(FATAL, "could not set timer for authorization timeout");

	/*
	 * Receive the startup packet (which might turn out to be a cancel request
	 * packet).
	 */
	status = ProcessStartupPacket(port, false);

	/*
	 * Stop here if it was bad or a cancel packet.	ProcessStartupPacket
	 * already did any appropriate error reporting.
	 */
	if (status != STATUS_OK)
		proc_exit(0);

	/*
	 * Now that we have the user and database name, we can set the process
	 * title for ps.  It's good to do this as early as possible in startup.
	 */
    if (SyncMaster)
        init_ps_display("gpsyncagent process", "", "", "");
    else
	    init_ps_display(port->user_name, port->database_name, remote_ps_data,
					update_process_title ? "authentication" : "");

	/*
	 * Now perform authentication exchange.
	 */
	ClientAuthentication(port);

	/*
	 * Done with authentication.  Disable timeout, and prevent SIGTERM/SIGQUIT
	 * again until backend startup is complete.
	 */
	if (!disable_sig_alarm(false))
		elog(FATAL, "could not disable timer for authorization timeout");
	PG_SETMASK(&BlockSig);

	if (Log_connections)
		ereport(LOG,
				(errmsg("connection authorized: user=%s database=%s",
						port->user_name, port->database_name),
				 errOmitLocation(true)));
}


/*
 * BackendRun -- set up the backend's argument list and invoke PostgresMain()
 *
 * returns:
 *		Shouldn't return at all.
 *		If PostgresMain() fails, return status.
 */
static int
BackendRun(Port *port)
{
	char	  **av;
	int			maxac;
	int			ac;
	long		secs;
	int			usecs;
	char		protobuf[32];
	int			i;

	/*
	 * Don't want backend to be able to see the postmaster random number
	 * generator state.  We have to clobber the static random_seed *and* start
	 * a new random sequence in the random() library function.
	 */
	random_seed = 0;
	random_start_time.tv_usec = 0;
	/* slightly hacky way to get integer microseconds part of timestamptz */
	TimestampDifference(0, port->SessionStartTime, &secs, &usecs);
	srandom((unsigned int) (MyProcPid ^ usecs));

	/* ----------------
	 * Now, build the argv vector that will be given to PostgresMain.
	 *
	 * The layout of the command line is
	 *		postgres [secure switches] -y databasename [insecure switches]
	 * where the switches after -y come from the client request.
	 *
	 * The maximum possible number of commandline arguments that could come
	 * from ExtraOptions or port->cmdline_options is (strlen + 1) / 2; see
	 * split_opts().
	 * ----------------
	 */
	maxac = 10;					/* for fixed args supplied below */
	maxac += (strlen(ExtraOptions) + 1) / 2;
	if (port->cmdline_options)
		maxac += (strlen(port->cmdline_options) + 1) / 2;

	av = (char **) MemoryContextAlloc(TopMemoryContext,
									  maxac * sizeof(char *));
	ac = 0;

	av[ac++] = "postgres";

	/*
	 * Pass any backend switches specified with -o in the postmaster's own
	 * command line.  We assume these are secure.  (It's OK to mangle
	 * ExtraOptions now, since we're safely inside a subprocess.)
	 */
	split_opts(av, &ac, ExtraOptions);

	/* Tell the backend what protocol the frontend is using. */
	snprintf(protobuf, sizeof(protobuf), "-v%u", port->proto);
	av[ac++] = protobuf;

	/*
	 * Tell the backend it is being called from the postmaster, and which
	 * database to use.  -y marks the end of secure switches.
	 */
	av[ac++] = "-y";
	av[ac++] = port->database_name;

	/*
	 * Pass the (insecure) option switches from the connection request. (It's
	 * OK to mangle port->cmdline_options now.)
	 */
	if (port->cmdline_options)
		split_opts(av, &ac, port->cmdline_options);

	av[ac] = NULL;

	Assert(ac < maxac);

	/*
	 * Release postmaster's working memory context so that backend can recycle
	 * the space.  Note this does not trash *MyProcPort, because ConnCreate()
	 * allocated that space with malloc() ... else we'd need to copy the Port
	 * data here.  Also, subsidiary data such as the username isn't lost
	 * either; see ProcessStartupPacket().
	 */
	MemoryContextSwitchTo(TopMemoryContext);
	MemoryContextDelete(PostmasterContext);
	PostmasterContext = NULL;

	/*
	 * Debug: print arguments being passed to backend
	 */
	ereport(DEBUG3,
			(errmsg_internal("%s child[%d]: starting with (",
							 progname, (int) getpid())));
	for (i = 0; i < ac; ++i)
		ereport(DEBUG3,
				(errmsg_internal("\t%s", av[i])));
	ereport(DEBUG3,
			(errmsg_internal(")")));

	ClientAuthInProgress = false;		/* client_min_messages is active now */

    if (SyncMaster)
       return (SyncAgentMain(ac, av, port->user_name));
    else
       return (PostgresMain(ac, av, port->user_name));
}


#ifdef EXEC_BACKEND

/*
 * postmaster_forkexec -- fork and exec a postmaster subprocess
 *
 * The caller must have set up the argv array already, except for argv[2]
 * which will be filled with the name of the temp variable file.
 *
 * Returns the child process PID, or -1 on fork failure (a suitable error
 * message has been logged on failure).
 *
 * All uses of this routine will dispatch to SubPostmasterMain in the
 * child process.
 */
pid_t
postmaster_forkexec(int argc, char *argv[])
{
	Port		port;

	/* This entry point passes dummy values for the Port variables */
	memset(&port, 0, sizeof(port));
	return internal_forkexec(argc, argv, &port);
}

/*
 * backend_forkexec -- fork/exec off a backend process
 *
 * Some operating systems (WIN32) don't have fork() so we have to simulate
 * it by storing parameters that need to be passed to the child and
 * then create a new child process.
 *
 * returns the pid of the fork/exec'd process, or -1 on failure
 */
static pid_t
backend_forkexec(Port *port)
{
	char	   *av[4];
	int			ac = 0;

	av[ac++] = "postgres";
	av[ac++] = "--forkbackend";
	av[ac++] = NULL;			/* filled in by internal_forkexec */

	av[ac] = NULL;
	Assert(ac < lengthof(av));

	return internal_forkexec(ac, av, port);
}

#ifndef WIN32

/*
 * internal_forkexec non-win32 implementation
 *
 * - writes out backend variables to the parameter file
 * - fork():s, and then exec():s the child process
 */
static pid_t
internal_forkexec(int argc, char *argv[], Port *port)
{
	static unsigned long tmpBackendFileNum = 0;
	pid_t		pid;
	char		tmpfilename[MAXPGPATH];
	BackendParameters param;
	FILE	   *fp;

	if (!save_backend_variables(&param, port))
		return -1;				/* log made by save_backend_variables */

	/* Calculate name for temp file */
	snprintf(tmpfilename, MAXPGPATH, "%s/%s.backend_var.%d.%lu",
			 PG_TEMP_FILES_DIR, PG_TEMP_FILE_PREFIX,
			 MyProcPid, ++tmpBackendFileNum);

	/* Open file */
	fp = AllocateFile(tmpfilename, PG_BINARY_W);
	if (!fp)
	{
		/* As in OpenTemporaryFile, try to make the temp-file directory */
		mkdir(PG_TEMP_FILES_DIR, S_IRWXU);

		fp = AllocateFile(tmpfilename, PG_BINARY_W);
		if (!fp)
		{
			ereport(LOG,
					(errcode_for_file_access(),
					 errmsg("could not create file \"%s\": %m",
							tmpfilename)));
			return -1;
		}
	}

	if (fwrite(&param, sizeof(param), 1, fp) != 1)
	{
		ereport(LOG,
				(errcode_for_file_access(),
				 errmsg("could not write to file \"%s\": %m", tmpfilename)));
		FreeFile(fp);
		return -1;
	}

	/* Release file */
	if (FreeFile(fp))
	{
		ereport(LOG,
				(errcode_for_file_access(),
				 errmsg("could not write to file \"%s\": %m", tmpfilename)));
		return -1;
	}

	/* Make sure caller set up argv properly */
	Assert(argc >= 3);
	Assert(argv[argc] == NULL);
	Assert(strncmp(argv[1], "--fork", 6) == 0);
	Assert(argv[2] == NULL);

	/* Insert temp file name after --fork argument */
	argv[2] = tmpfilename;

	/* Fire off execv in child */
	if ((pid = fork_process()) == 0)
	{
		if (execv(postgres_exec_path, argv) < 0)
		{
			ereport(LOG,
					(errmsg("could not execute server process \"%s\": %m",
							postgres_exec_path)));
			/* We're already in the child process here, can't return */
			exit(1);
		}
	}

	return pid;					/* Parent returns pid, or -1 on fork failure */
}
#else							/* WIN32 */

/*
 * internal_forkexec win32 implementation
 *
 * - starts backend using CreateProcess(), in suspended state
 * - writes out backend variables to the parameter file
 *	- during this, duplicates handles and sockets required for
 *	  inheritance into the new process
 * - resumes execution of the new process once the backend parameter
 *	 file is complete.
 */
static pid_t
internal_forkexec(int argc, char *argv[], Port *port)
{
	STARTUPINFO si;
	PROCESS_INFORMATION pi;
	int			i;
	int			j;
	char		cmdLine[MAXPGPATH * 2];
	HANDLE		paramHandle;
	BackendParameters *param;
	SECURITY_ATTRIBUTES sa;
	char		paramHandleStr[32];
	win32_deadchild_waitinfo *childinfo;

	/* Make sure caller set up argv properly */
	Assert(argc >= 3);
	Assert(argv[argc] == NULL);
	Assert(strncmp(argv[1], "--fork", 6) == 0);
	Assert(argv[2] == NULL);

	/* Set up shared memory for parameter passing */
	ZeroMemory(&sa, sizeof(sa));
	sa.nLength = sizeof(sa);
	sa.bInheritHandle = TRUE;
	paramHandle = CreateFileMapping(INVALID_HANDLE_VALUE,
									&sa,
									PAGE_READWRITE,
									0,
									sizeof(BackendParameters),
									NULL);
	if (paramHandle == INVALID_HANDLE_VALUE)
	{
		elog(LOG, "could not create backend parameter file mapping: error code %d",
			 (int) GetLastError());
		return -1;
	}

	param = MapViewOfFile(paramHandle, FILE_MAP_WRITE, 0, 0, sizeof(BackendParameters));
	if (!param)
	{
		elog(LOG, "could not map backend parameter memory: error code %d",
			 (int) GetLastError());
		CloseHandle(paramHandle);
		return -1;
	}

	/* Insert temp file name after --fork argument */
	sprintf(paramHandleStr, "%lu", (DWORD) paramHandle);
	argv[2] = paramHandleStr;

	/* Format the cmd line */
	cmdLine[sizeof(cmdLine) - 1] = '\0';
	cmdLine[sizeof(cmdLine) - 2] = '\0';
	snprintf(cmdLine, sizeof(cmdLine) - 1, "\"%s\"", postgres_exec_path);
	i = 0;
	while (argv[++i] != NULL)
	{
		j = strlen(cmdLine);
		snprintf(cmdLine + j, sizeof(cmdLine) - 1 - j, " \"%s\"", argv[i]);
	}
	if (cmdLine[sizeof(cmdLine) - 2] != '\0')
	{
		elog(LOG, "subprocess command line too long");
		return -1;
	}

	memset(&pi, 0, sizeof(pi));
	memset(&si, 0, sizeof(si));
	si.cb = sizeof(si);

	/*
	 * Create the subprocess in a suspended state. This will be resumed later,
	 * once we have written out the parameter file.
	 */
	if (!CreateProcess(NULL, cmdLine, NULL, NULL, TRUE, CREATE_SUSPENDED,
					   NULL, NULL, &si, &pi))
	{
		elog(LOG, "CreateProcess call failed: %m (error code %d)",
			 (int) GetLastError());
		return -1;
	}

	if (!save_backend_variables(param, port, pi.hProcess, pi.dwProcessId))
	{
		/*
		 * log made by save_backend_variables, but we have to clean up the
		 * mess with the half-started process
		 */
		if (!TerminateProcess(pi.hProcess, 255))
			ereport(ERROR,
					(errmsg_internal("could not terminate unstarted process: error code %d",
									 (int) GetLastError())));
		CloseHandle(pi.hProcess);
		CloseHandle(pi.hThread);
		return -1;				/* log made by save_backend_variables */
	}

	/* Drop the parameter shared memory that is now inherited to the backend */
	if (!UnmapViewOfFile(param))
		elog(LOG, "could not unmap view of backend parameter file: error code %d",
			 (int) GetLastError());
	if (!CloseHandle(paramHandle))
		elog(LOG, "could not close handle to backend parameter file: error code %d",
			 (int) GetLastError());

	/*
	 * Reserve the memory region used by our main shared memory segment before we
	 * resume the child process.
	 */
	if (!pgwin32_ReserveSharedMemoryRegion(pi.hProcess))
	{
		/*
		 * Failed to reserve the memory, so terminate the newly created
		 * process and give up.
		 */
		if (!TerminateProcess(pi.hProcess, 255))
			ereport(ERROR,
					(errmsg_internal("could not terminate process that failed to reserve memory: error code %d",
									 (int) GetLastError())));
		CloseHandle(pi.hProcess);
		CloseHandle(pi.hThread);
		return -1;			/* logging done made by pgwin32_ReserveSharedMemoryRegion() */
	}

	/*
	 * Now that the backend variables are written out, we start the child
	 * thread so it can start initializing while we set up the rest of the
	 * parent state.
	 */
	if (ResumeThread(pi.hThread) == -1)
	{
		if (!TerminateProcess(pi.hProcess, 255))
		{
			ereport(ERROR,
					(errmsg_internal("could not terminate unstartable process: error code %d",
									 (int) GetLastError())));
			CloseHandle(pi.hProcess);
			CloseHandle(pi.hThread);
			return -1;
		}
		CloseHandle(pi.hProcess);
		CloseHandle(pi.hThread);
		ereport(ERROR,
				(errmsg_internal("could not resume thread of unstarted process: error code %d",
								 (int) GetLastError())));
		return -1;
	}

	/*
	 * Queue a waiter for to signal when this child dies. The wait will be
	 * handled automatically by an operating system thread pool.
	 *
	 * Note: use malloc instead of palloc, since it needs to be thread-safe.
	 * Struct will be free():d from the callback function that runs on a
	 * different thread.
	 */
	childinfo = malloc(sizeof(win32_deadchild_waitinfo));
	if (!childinfo)
		ereport(FATAL,
				(errcode(ERRCODE_OUT_OF_MEMORY),
				 errmsg("out of memory")));

	childinfo->procHandle = pi.hProcess;
	childinfo->procId = pi.dwProcessId;

	if (!RegisterWaitForSingleObject(&childinfo->waitHandle,
									 pi.hProcess,
									 pgwin32_deadchild_callback,
									 childinfo,
									 INFINITE,
								WT_EXECUTEONLYONCE | WT_EXECUTEINWAITTHREAD))
		ereport(FATAL,
		(errmsg_internal("could not register process for wait: error code %d",
						 (int) GetLastError())));

	/* Don't close pi.hProcess here - the wait thread needs access to it */

	CloseHandle(pi.hThread);

	return pi.dwProcessId;
}
#endif   /* WIN32 */

#ifdef EXEC_BACKEND
/* This should really be in a header file */
NON_EXEC_STATIC void
PerfmonMain(int argc, char *argv[]);
#endif 

/*
 * SubPostmasterMain -- Get the fork/exec'd process into a state equivalent
 *			to what it would be if we'd simply forked on Unix, and then
 *			dispatch to the appropriate place.
 *
 * The first two command line arguments are expected to be "--forkFOO"
 * (where FOO indicates which postmaster child we are to become), and
 * the name of a variables file that we can read to load data that would
 * have been inherited by fork() on Unix.  Remaining arguments go to the
 * subprocess FooMain() routine.
 */
int
SubPostmasterMain(int argc, char *argv[])
{
	Port		port;
	MemoryContext	old;

	/* Do this sooner rather than later... */
	IsUnderPostmaster = true;	/* we are a postmaster subprocess now */

	MyProcPid = getpid();		/* reset MyProcPid */

	MyStartTime = time(NULL);

	/*
	 * make sure stderr is in binary mode before anything can possibly be
	 * written to it, in case it's actually the syslogger pipe, so the pipe
	 * chunking protocol isn't disturbed. Non-logpipe data gets translated on
	 * redirection (e.g. via pg_ctl -l) anyway.
	 */
#ifdef WIN32
	_setmode(fileno(stderr), _O_BINARY);
#endif

	/* Lose the postmaster's on-exit routines (really a no-op) */
	on_exit_reset();

	/* In EXEC_BACKEND case we will not have inherited these settings */
	IsPostmasterEnvironment = true;
	whereToSendOutput = DestNone;

	/* Setup essential subsystems (to ensure elog() behaves sanely) */
	MemoryContextInit();
	InitializeGUCOptions();

	/* Read in the variables file */
	memset(&port, 0, sizeof(Port));
	old = MemoryContextSwitchTo(TopMemoryContext);
	initStringInfo(&port->override_options);
	MemoryContextSwitchTo(old);
	read_backend_variables(argv[2], &port);

	/*
	 * Set up memory area for GSS information. Mirrors the code in ConnCreate
	 * for the non-exec case.
	 */
#if defined(ENABLE_GSS) || defined(ENABLE_SSPI)
	port.gss = (pg_gssinfo *) calloc(1, sizeof(pg_gssinfo));
	if (!port.gss)
		ereport(FATAL,
				(errcode(ERRCODE_OUT_OF_MEMORY),
				 errmsg("out of memory")));
#endif


	/* Check we got appropriate args */
	if (argc < 3)
		elog(FATAL, "invalid subpostmaster invocation");

	/*
	 * If appropriate, physically re-attach to shared memory segment. We want
	 * to do this before going any further to ensure that we can attach at the
	 * same address the postmaster used.
	 */
	if (strcmp(argv[1], "--forkbackend") == 0   ||
		strcmp(argv[1], "--forkavlauncher") == 0 ||
		strcmp(argv[1], "--forkavworker") == 0 ||
		strcmp(argv[1], "--forkautovac") == 0   ||
		strcmp(argv[1], "--forkseqserver") == 0 ||
		strcmp(argv[1], "--forkboot") == 0)
		PGSharedMemoryReAttach();

	/*
	 * Start our win32 signal implementation. This has to be done after we
	 * read the backend variables, because we need to pick up the signal pipe
	 * from the parent process.
	 */
#ifdef WIN32
	pgwin32_signal_initialize();
#endif

	/* In EXEC_BACKEND case we will not have inherited these settings */
	pqinitmask();
	PG_SETMASK(&BlockSig);

	/* Read in remaining GUC variables */
	read_nondefault_variables();

	/*
	 * Reload any libraries that were preloaded by the postmaster.	Since we
	 * exec'd this process, those libraries didn't come along with us; but we
	 * should load them into all child processes to be consistent with the
	 * non-EXEC_BACKEND behavior.
	 */
	process_shared_preload_libraries();

	/* Run backend or appropriate child */
	if (strcmp(argv[1], "--forkbackend") == 0)
	{
		Assert(argc == 3);		/* shouldn't be any more args */

		/* Close the postmaster's sockets */
		ClosePostmasterPorts(false);

		/*
		 * Need to reinitialize the SSL library in the backend, since the
		 * context structures contain function pointers and cannot be passed
		 * through the parameter file.
		 *
		 * XXX should we do this in all child processes?  For the moment it's
		 * enough to do it in backend children.
		 */
#ifdef USE_SSL
		if (EnableSSL)
			secure_initialize();
#endif

		/*
		 * Perform additional initialization and client authentication.
		 *
		 * We want to do this before InitProcess() for a couple of reasons: 1.
		 * so that we aren't eating up a PGPROC slot while waiting on the
		 * client. 2. so that if InitProcess() fails due to being out of
		 * PGPROC slots, we have already initialized libpq and are able to
		 * report the error to the client.
		 */
		BackendInitialize(&port);

		/* Restore basic shared memory pointers */
		InitShmemAccess(UsedShmemSegAddr);

		/* Need a PGPROC to run CreateSharedMemoryAndSemaphores */
		InitProcess();

		/*
		 * Attach process to shared data structures.  If testing EXEC_BACKEND
		 * on Linux, you must run this as root before starting the postmaster:
		 *
		 * echo 0 >/proc/sys/kernel/randomize_va_space
		 *
		 * This prevents a randomized stack base address that causes child
		 * shared memory to be at a different address than the parent, making
		 * it impossible to attached to shared memory.	Return the value to
		 * '1' when finished.
		 */
		CreateSharedMemoryAndSemaphores(false, 0);

		/* And run the backend */
		proc_exit(BackendRun(&port));
	}
	if (strcmp(argv[1], "--forkboot") == 0)
	{
		/* Close the postmaster's sockets */
		ClosePostmasterPorts(false);

		/* Restore basic shared memory pointers */
		InitShmemAccess(UsedShmemSegAddr);

		/* Need a PGPROC to run CreateSharedMemoryAndSemaphores */
		InitAuxiliaryProcess();

		/* Attach process to shared data structures */
		CreateSharedMemoryAndSemaphores(false, 0);

		AuxiliaryProcessMain(argc - 2, argv + 2);
		proc_exit(0);
	}
	if (strcmp(argv[1], "--forkautovac") == 0)
	{
		/* Close the postmaster's sockets */
		ClosePostmasterPorts(false);

		/* Restore basic shared memory pointers */
		InitShmemAccess(UsedShmemSegAddr);

		/* Need a PGPROC to run CreateSharedMemoryAndSemaphores */
		InitProcess();

		/* Attach process to shared data structures */
		CreateSharedMemoryAndSemaphores(false, 0);

		AutoVacMain(argc - 2, argv + 2);
		proc_exit(0);
	}
	if (strcmp(argv[1], "--forkarch") == 0)
	{
		/* Close the postmaster's sockets */
		ClosePostmasterPorts(false);

		/* Do not want to attach to shared memory */

		PgArchiverMain(argc, argv);
		proc_exit(0);
	}
	if (strcmp(argv[1], "--forkcol") == 0)
	{
		/* Close the postmaster's sockets */
		ClosePostmasterPorts(false);

		/* Do not want to attach to shared memory */

		PgstatCollectorMain(argc, argv);
		proc_exit(0);
	}
	if (strcmp(argv[1], "--forklog") == 0)
	{
		/* Close the postmaster's sockets */
		ClosePostmasterPorts(true);

		/* Do not want to attach to shared memory */

		SysLoggerMain(argc, argv);
		proc_exit(0);
	}
	if (strcmp(argv[1], "--forkseqserver") == 0)
	{
		/* Close the postmaster's sockets */
		ClosePostmasterPorts(false);

		/* Restore basic shared memory pointers */
		InitShmemAccess(UsedShmemSegAddr);

		/* Need a PGPROC to run CreateSharedMemoryAndSemaphores */
		InitAuxiliaryProcess();

		/* Attach process to shared data structures */
		CreateSharedMemoryAndSemaphores(false, 0);

		SeqServerMain(argc - 2, argv + 2);
		proc_exit(0);
	}
	if (strcmp(argv[1], "--forkftsprobe") == 0)
	{
		/* Close the postmaster's sockets */
		ClosePostmasterPorts(false);

		/* Restore basic shared memory pointers */
		InitShmemAccess(UsedShmemSegAddr);

		/* Need a PGPROC to run CreateSharedMemoryAndSemaphores */
		InitAuxiliaryProcess();

		/* Attach process to shared data structures */
		CreateSharedMemoryAndSemaphores(false, 0);

		FtsProbeMain(argc - 2, argv + 2);
		proc_exit(0);
	}
	if (strcmp(argv[1], "--forkperfmon") == 0)
	{
		/* Close the postmaster's sockets */
		ClosePostmasterPorts(false);

		/* Do not want to attach to shared memory */

		PerfmonMain(argc - 2, argv + 2);
		proc_exit(0);
	}

	return 1;					/* shouldn't get here */
}
#endif   /* EXEC_BACKEND */


/*
 * ExitPostmaster -- cleanup
 *
 * Do NOT call exit() directly --- always go through here!
 */
static void
ExitPostmaster(int status)
{
	/* should cleanup shared memory and kill all backends */

	/*
	 * Not sure of the semantics here.	When the Postmaster dies, should the
	 * backends all be killed? probably not.
	 *
	 * MUST		-- vadim 05-10-1999
	 */

	proc_exit(status);
}

/*
 * sigusr1_handler - handle signal conditions from child processes
 */
static void
sigusr1_handler(SIGNAL_ARGS)
{
	int			save_errno = errno;

	PG_SETMASK(&BlockSig);

	/*
	 * RECOVERY_STARTED and RECOVERY_CONSISTENT signals are ignored in
	 * unexpected states. If the startup process quickly starts up, completes
	 * recovery, exits, we might process the death of the startup process
	 * first. We don't want to go back to recovery in that case.
	 */
	if (CheckPostmasterSignal(PMSIGNAL_RECOVERY_STARTED) &&
		pmState == PM_STARTUP)
	{
		Assert(isPrimaryMirrorModeAFullPostmaster(true));

		/* WAL redo has started. We're out of reinitialization. */
		FatalError = false;

		/*
		 * Crank up the background writer.	It doesn't matter if this fails,
		 * we'll just try again later.
		 */
		Assert(BgWriterPID == 0);
		SetBGWriterPID(StartBackgroundWriter());

		pmState = PM_RECOVERY;
	}
	if (CheckPostmasterSignal(PMSIGNAL_RECOVERY_CONSISTENT) &&
		pmState == PM_RECOVERY)
	{
		Assert(isPrimaryMirrorModeAFullPostmaster(true));

		/*
		 * Load the flat authorization file into postmaster's cache. The
		 * startup process won't have recomputed this from the database yet,
		 * so it may change following recovery.
		 */
		load_role();

		/*
		 * Likewise, start other special children as needed.
		 */
		Assert(PgStatPID == 0);
		PgStatPID = pgstat_start();

		/* XXX at this point we could accept read-only connections */
		ereport(DEBUG1,
				(errmsg("database system is in consistent recovery mode")));

		pmState = PM_RECOVERY_CONSISTENT;
	}

	if (CheckPostmasterSignal(PMSIGNAL_PASSWORD_CHANGE))
	{
		/*
		 * Authorization file has changed.
		 */
		load_role();
	}

	if (CheckPostmasterSignal(PMSIGNAL_WAKEN_CHILDREN))
	{
		/*
		 * Send SIGUSR1 to all children (triggers CatchupInterruptHandler).
		 * See storage/ipc/sinval[adt].c for the use of this.
		 */
		if (Shutdown <= SmartShutdown)
		{
			SignalChildren(SIGUSR1);
            signal_child_if_up(AutoVacPID, SIGUSR1);
		}
	}

	if (CheckPostmasterSignal(PMSIGNAL_WAKEN_ARCHIVER) &&
		PgArchPID != 0)
	{
		/*
		 * Send SIGUSR1 to archiver process, to wake it up and begin archiving
		 * next transaction log file.
		 */
		signal_child(PgArchPID, SIGUSR1);
	}

	if (CheckPostmasterSignal(PMSIGNAL_ROTATE_LOGFILE) &&
		SysLoggerPID != 0)
	{
		/* Tell syslogger to rotate logfile */
		signal_child(SysLoggerPID, SIGUSR1);
	}

	if (CheckPostmasterSignal(PMSIGNAL_START_AUTOVAC))
	{
		/*
		 * Start one iteration of the autovacuum daemon, even if autovacuuming
		 * is nominally not enabled.  This is so we can have an active defense
		 * against transaction ID wraparound.  We set a flag for the main loop
		 * to do it rather than trying to do it here --- this is because the
		 * autovac process itself may send the signal, and we want to handle
		 * that by launching another iteration as soon as the current one
		 * completes.
		 */
		force_autovac = true;
	}

	if (CheckPostmasterSignal(PMSIGNAL_POSTMASTER_RESET_BY_PEER))
	{
		/*
		 * set a flag that will be handled in the main loop;
		 * this signal comes from handling of postmaster
		 * reset as requested by the primary/mirror peer;
		 */
		filerep_requires_postmaster_reset = true;
	}

	PG_SETMASK(&UnBlockSig);

	errno = save_errno;
}


/*
 * Dummy signal handler
 *
 * We use this for signals that we don't actually use in the postmaster,
 * but we do use in backends.  If we were to SIG_IGN such signals in the
 * postmaster, then a newly started backend might drop a signal that arrives
 * before it's able to reconfigure its signal processing.  (See notes in
 * tcop/postgres.c.)
 */
static void
dummy_handler(SIGNAL_ARGS)
{
}

/*
 * RandomSalt
 */
static void
RandomSalt(char *md5Salt)
{
	long		rand;

	/*
	 * We use % 255, sacrificing one possible byte value, so as to ensure that
	 * all bits of the random() value participate in the result. While at it,
	 * add one to avoid generating any null bytes.
	 */
	rand = PostmasterRandom();
	md5Salt[0] = (rand % 255) + 1;
	rand = PostmasterRandom();
	md5Salt[1] = (rand % 255) + 1;
	rand = PostmasterRandom();
	md5Salt[2] = (rand % 255) + 1;
	rand = PostmasterRandom();
	md5Salt[3] = (rand % 255) + 1;
}

/*
 * PostmasterRandom
 */
static long
PostmasterRandom(void)
{
	/*
	 * Select a random seed at the time of first receiving a request.
	 */
	if (random_seed == 0)
	{
		do
		{
			struct timeval random_stop_time;

			gettimeofday(&random_stop_time, NULL);

			/*
			 * We are not sure how much precision is in tv_usec, so we swap
			 * the high and low 16 bits of 'random_stop_time' and XOR them
			 * with 'random_start_time'. On the off chance that the result is
			 * 0, we loop until it isn't.
			 */
			random_seed = random_start_time.tv_usec ^
				((random_stop_time.tv_usec << 16) |
				 ((random_stop_time.tv_usec >> 16) & 0xffff));
		}
		while (random_seed == 0);

		srandom(random_seed);
	}

	return random();
}

/*
 * Count up number of child processes (excluding special children and
 * dead_end children)
 */
static int
CountChildren(void)
{
	Dlelem	   *curr;
	int			cnt = 0;

	for (curr = DLGetHead(BackendList); curr; curr = DLGetSucc(curr))
	{
		Backend    *bp = (Backend *) DLE_VAL(curr);

		if (!bp->dead_end)
			cnt++;
	}
	return cnt;
}


/*
 * StartChildProcess -- start an auxiliary process for the postmaster
 *
 * xlop determines what kind of child will be started.	All child types
 * initially go to AuxiliaryProcessMain, which will handle common setup.
 *
 * Return value of StartChildProcess is subprocess' PID, or 0 if failed
 * to start subprocess.
 */
static pid_t
StartChildProcess(AuxProcType type)
{
	pid_t		pid;
	char	   *av[10];
	int			ac = 0;
	char		typebuf[32];

	/*
	 * Set up command-line arguments for subprocess
	 */
	av[ac++] = "postgres";

#ifdef EXEC_BACKEND
	av[ac++] = "--forkboot";
	av[ac++] = NULL;			/* filled in by postmaster_forkexec */
#endif

	snprintf(typebuf, sizeof(typebuf), "-x%d", type);
	av[ac++] = typebuf;

	av[ac] = NULL;
	Assert(ac < lengthof(av));

    switch (type)
	{
        case BgWriterProcess:
		case CheckpointProcess:
        case StartupProcess:
        case StartupPass2Process:
		case StartupPass3Process:
		case StartupPass4Process:
        case WalWriterProcess:
            Assert(isPrimaryMirrorModeAFullPostmaster(true));
            break;
        case CheckerProcess:
        case BootstrapProcess:
            break;
	}

#ifdef EXEC_BACKEND
	pid = postmaster_forkexec(ac, av);
#else							/* !EXEC_BACKEND */
	pid = fork_process();

	if (pid == 0)				/* child */
	{
		IsUnderPostmaster = true;		/* we are a postmaster subprocess now */

		/* Close the postmaster's sockets */
		ClosePostmasterPorts(false);

		/* Lose the postmaster's on-exit routines and port connections */
		on_exit_reset();

		/* Release postmaster's working memory context */
		MemoryContextSwitchTo(TopMemoryContext);
		MemoryContextDelete(PostmasterContext);
		PostmasterContext = NULL;

		AuxiliaryProcessMain(ac, av);
		ExitPostmaster(0);
	}
#endif   /* EXEC_BACKEND */

	if (pid < 0)
	{
		/* in parent, fork failed */
		int			save_errno = errno;

		errno = save_errno;
		switch (type)
		{
			case StartupProcess:
				ereport(LOG,
						(errmsg("could not fork startup process: %m")));
				break;
			case StartupPass2Process:
				ereport(LOG,
						(errmsg("could not fork startup pass 2 process: %m")));
				break;
			case StartupPass3Process:
				ereport(LOG,
						(errmsg("could not fork startup pass 3 process: %m")));
				break;
			case StartupPass4Process:
				ereport(LOG,
						(errmsg("could not fork startup pass 4 process: %m")));
				break;
			case BgWriterProcess:
				ereport(LOG,
				   (errmsg("could not fork background writer process: %m")));
				break;
			case CheckpointProcess:
				ereport(LOG,
				   (errmsg("could not fork background checkpoint process: %m")));
				break;
			default:
				ereport(LOG,
						(errmsg("could not fork process: %m")));
				break;
		}

		/*
		 * fork failure is fatal during startup, but there's no need to choke
		 * immediately if starting other child types fails.
		 */
		if (type == StartupProcess)
			ExitPostmaster(1);
		return 0;
	}

	/*
	 * in parent, successful fork
	 */
	return pid;
}

/*
 * Create the opts file
 */
static bool
CreateOptsFile(int argc, char *argv[], char *fullprogname)
{
	FILE	   *fp;
	int			i;

#define OPTS_FILE	"postmaster.opts"

	if ((fp = fopen(OPTS_FILE, "w")) == NULL)
	{
		elog(LOG, "could not create file \"%s\": %m", OPTS_FILE);
		return false;
	}

	fprintf(fp, "%s", fullprogname);
	for (i = 1; i < argc; i++)
		fprintf(fp, " \"%s\"", argv[i]);
	fputs("\n", fp);

	if (fclose(fp))
	{
		elog(LOG, "could not write file \"%s\": %m", OPTS_FILE);
		return false;
	}

	return true;
}


/*
 * MaxLivePostmasterChildren
 *
 * This reports the number of entries needed in per-child-process arrays
 * (the PMChildFlags array, and if EXEC_BACKEND the ShmemBackendArray).
 * These arrays include regular backends and autovac workers, but not special
 * children nor dead_end children.	This allows the arrays to have a fixed
 * maximum size, to wit the same too-many-children limit enforced by
 * canAcceptConnections().	The exact value isn't too critical as long as
 * it's more than MaxBackends.
 */
int
MaxLivePostmasterChildren(void)
{
	return 2 * MaxBackends;
}


#ifdef EXEC_BACKEND

/*
 * The following need to be available to the save/restore_backend_variables
 * functions
 */
extern slock_t *ShmemLock;
extern LWLock *LWLockArray;
extern PROC_HDR *ProcGlobal;
extern PGPROC *AuxiliaryProcs;
extern PMSignalData *PMSignalState;
extern int	pgStatSock;

#ifndef WIN32
#define write_inheritable_socket(dest, src, childpid) (*(dest) = (src))
#define read_inheritable_socket(dest, src) (*(dest) = *(src))
#else
static void write_duplicated_handle(HANDLE *dest, HANDLE src, HANDLE child);
static void write_inheritable_socket(InheritableSocket *dest, SOCKET src,
						 pid_t childPid);
static void read_inheritable_socket(SOCKET *dest, InheritableSocket *src);
#endif


/* Save critical backend variables into the BackendParameters struct */
#ifndef WIN32
static bool
save_backend_variables(BackendParameters *param, Port *port)
#else
static bool
save_backend_variables(BackendParameters *param, Port *port,
					   HANDLE childProcess, pid_t childPid)
#endif
{
	memcpy(&param->port, port, sizeof(Port));
	write_inheritable_socket(&param->portsocket, port->sock, childPid);

	strlcpy(param->DataDir, DataDir, MAXPGPATH);

	memcpy(&param->ListenSocket, &ListenSocket, sizeof(ListenSocket));

	param->MyCancelKey = MyCancelKey;
	param->MyPMChildSlot = MyPMChildSlot;

	param->UsedShmemSegID = UsedShmemSegID;
	param->UsedShmemSegAddr = UsedShmemSegAddr;

	param->ShmemLock = ShmemLock;
	param->ShmemVariableCache = ShmemVariableCache;
	param->ShmemBackendArray = ShmemBackendArray;

	param->LWLockArray = LWLockArray;
	param->ProcGlobal = ProcGlobal;
	param->AuxiliaryProcs = AuxiliaryProcs;
	param->PMSignalState = PMSignalState;
	write_inheritable_socket(&param->pgStatSock, pgStatSock, childPid);

	param->PostmasterPid = PostmasterPid;
	param->PgStartTime = PgStartTime;
	param->PgReloadTime = PgReloadTime;

	param->redirection_done = redirection_done;

#ifdef WIN32
	param->PostmasterHandle = PostmasterHandle;
	write_duplicated_handle(&param->initial_signal_pipe,
							pgwin32_create_signal_listener(childPid),
							childProcess);
#endif

	memcpy(&param->syslogPipe, &syslogPipe, sizeof(syslogPipe));

	strlcpy(param->my_exec_path, my_exec_path, MAXPGPATH);

	strlcpy(param->pkglib_path, pkglib_path, MAXPGPATH);

	strlcpy(param->ExtraOptions, ExtraOptions, MAXPGPATH);

	strlcpy(param->lc_collate, setlocale(LC_COLLATE, NULL), LOCALE_NAME_BUFLEN);
	strlcpy(param->lc_ctype, setlocale(LC_CTYPE, NULL), LOCALE_NAME_BUFLEN);

	return true;
}


#ifdef WIN32
/*
 * Duplicate a handle for usage in a child process, and write the child
 * process instance of the handle to the parameter file.
 */
static void
write_duplicated_handle(HANDLE *dest, HANDLE src, HANDLE childProcess)
{
	HANDLE		hChild = INVALID_HANDLE_VALUE;

	if (!DuplicateHandle(GetCurrentProcess(),
						 src,
						 childProcess,
						 &hChild,
						 0,
						 TRUE,
						 DUPLICATE_CLOSE_SOURCE | DUPLICATE_SAME_ACCESS))
		ereport(ERROR,
				(errmsg_internal("could not duplicate handle to be written to backend parameter file: error code %d",
								 (int) GetLastError())));

	*dest = hChild;
}

/*
 * Duplicate a socket for usage in a child process, and write the resulting
 * structure to the parameter file.
 * This is required because a number of LSPs (Layered Service Providers) very
 * common on Windows (antivirus, firewalls, download managers etc) break
 * straight socket inheritance.
 */
static void
write_inheritable_socket(InheritableSocket *dest, SOCKET src, pid_t childpid)
{
	dest->origsocket = src;
	if (src != 0 && src != -1)
	{
		/* Actual socket */
		if (WSADuplicateSocket(src, childpid, &dest->wsainfo) != 0)
			ereport(ERROR,
					(errmsg("could not duplicate socket %d for use in backend: error code %d",
							src, WSAGetLastError())));
	}
}

/*
 * Read a duplicate socket structure back, and get the socket descriptor.
 */
static void
read_inheritable_socket(SOCKET *dest, InheritableSocket *src)
{
	SOCKET		s;

	if (src->origsocket == -1 || src->origsocket == 0)
	{
		/* Not a real socket! */
		*dest = src->origsocket;
	}
	else
	{
		/* Actual socket, so create from structure */
		s = WSASocket(FROM_PROTOCOL_INFO,
					  FROM_PROTOCOL_INFO,
					  FROM_PROTOCOL_INFO,
					  &src->wsainfo,
					  0,
					  0);
		if (s == INVALID_SOCKET)
		{
			write_stderr("could not create inherited socket: error code %d\n",
						 WSAGetLastError());
			exit(1);
		}
		*dest = s;

		/*
		 * To make sure we don't get two references to the same socket, close
		 * the original one. (This would happen when inheritance actually
		 * works..
		 */
		closesocket(src->origsocket);
	}
}
#endif

static void
read_backend_variables(char *id, Port *port)
{
	BackendParameters param;

#ifndef WIN32
	/* Non-win32 implementation reads from file */
	FILE	   *fp;

	/* Open file */
	fp = AllocateFile(id, PG_BINARY_R);
	if (!fp)
	{
		write_stderr("could not read from backend variables file \"%s\": %s\n",
					 id, strerror(errno));
		exit(1);
	}

	if (fread(&param, sizeof(param), 1, fp) != 1)
	{
		write_stderr("could not read from backend variables file \"%s\": %s\n",
					 id, strerror(errno));
		exit(1);
	}

	/* Release file */
	FreeFile(fp);
	if (unlink(id) != 0)
	{
		write_stderr("could not remove file \"%s\": %s\n",
					 id, strerror(errno));
		exit(1);
	}
#else
	/* Win32 version uses mapped file */
	HANDLE		paramHandle;
	BackendParameters *paramp;

	paramHandle = (HANDLE) atol(id);
	paramp = MapViewOfFile(paramHandle, FILE_MAP_READ, 0, 0, 0);
	if (!paramp)
	{
		write_stderr("could not map view of backend variables: error code %d\n",
					 (int) GetLastError());
		exit(1);
	}

	memcpy(&param, paramp, sizeof(BackendParameters));

	if (!UnmapViewOfFile(paramp))
	{
		write_stderr("could not unmap view of backend variables: error code %d\n",
					 (int) GetLastError());
		exit(1);
	}

	if (!CloseHandle(paramHandle))
	{
		write_stderr("could not close handle to backend parameter variables: error code %d\n",
					 (int) GetLastError());
		exit(1);
	}
#endif

	restore_backend_variables(&param, port);
}

/* Restore critical backend variables from the BackendParameters struct */
static void
restore_backend_variables(BackendParameters *param, Port *port)
{
	memcpy(port, &param->port, sizeof(Port));
	read_inheritable_socket(&port->sock, &param->portsocket);

	SetDataDir(param->DataDir);

	memcpy(&ListenSocket, &param->ListenSocket, sizeof(ListenSocket));

	MyCancelKey = param->MyCancelKey;
	MyPMChildSlot = param->MyPMChildSlot;

	UsedShmemSegID = param->UsedShmemSegID;
	UsedShmemSegAddr = param->UsedShmemSegAddr;

	ShmemLock = param->ShmemLock;
	ShmemVariableCache = param->ShmemVariableCache;
	ShmemBackendArray = param->ShmemBackendArray;

	LWLockArray = param->LWLockArray;
	ProcGlobal = param->ProcGlobal;
	AuxiliaryProcs = param->AuxiliaryProcs;
	PMSignalState = param->PMSignalState;
	read_inheritable_socket(&pgStatSock, &param->pgStatSock);

	PostmasterPid = param->PostmasterPid;
	PgStartTime = param->PgStartTime;
	PgReloadTime = param->PgReloadTime;

	redirection_done = param->redirection_done;

#ifdef WIN32
	PostmasterHandle = param->PostmasterHandle;
	pgwin32_initial_signal_pipe = param->initial_signal_pipe;
#endif

	memcpy(&syslogPipe, &param->syslogPipe, sizeof(syslogPipe));

	strlcpy(my_exec_path, param->my_exec_path, MAXPGPATH);

	strlcpy(pkglib_path, param->pkglib_path, MAXPGPATH);

	strlcpy(ExtraOptions, param->ExtraOptions, MAXPGPATH);

	setlocale(LC_COLLATE, param->lc_collate);
	setlocale(LC_CTYPE, param->lc_ctype);
}


Size
ShmemBackendArraySize(void)
{
	return mul_size(MaxLivePostmasterChildren(), sizeof(Backend));
}

void
ShmemBackendArrayAllocation(void)
{
	Size		size = ShmemBackendArraySize();

	ShmemBackendArray = (Backend *) ShmemAlloc(size);
	/* Mark all slots as empty */
	memset(ShmemBackendArray, 0, size);
}

static void
ShmemBackendArrayAdd(Backend *bn)
{
	/* The array slot corresponding to my PMChildSlot should be free */
	int			i = bn->child_slot - 1;

	Assert(ShmemBackendArray[i].pid == 0);
	ShmemBackendArray[i] = *bn;
}

static void
ShmemBackendArrayRemove(Backend *bn)
{
	int			i = bn->child_slot - 1;

	Assert(ShmemBackendArray[i].pid == bn->pid);
	/* Mark the slot as empty */
	ShmemBackendArray[i].pid = 0;
}
#endif   /* EXEC_BACKEND */


#ifdef WIN32

static pid_t
win32_waitpid(int *exitstatus)
{
	DWORD		dwd;
	ULONG_PTR	key;
	OVERLAPPED *ovl;

	/*
	 * Check if there are any dead children. If there are, return the pid of
	 * the first one that died.
	 */
	if (GetQueuedCompletionStatus(win32ChildQueue, &dwd, &key, &ovl, 0))
	{
		*exitstatus = (int) key;
		return dwd;
	}

	return -1;
}

/*
 * Note! Code below executes on a thread pool! All operations must
 * be thread safe! Note that elog() and friends must *not* be used.
 */
static void WINAPI
pgwin32_deadchild_callback(PVOID lpParameter, BOOLEAN TimerOrWaitFired)
{
	win32_deadchild_waitinfo *childinfo = (win32_deadchild_waitinfo *) lpParameter;
	DWORD		exitcode;

	if (TimerOrWaitFired)
		return;					/* timeout. Should never happen, since we use
								 * INFINITE as timeout value. */

	/*
	 * Remove handle from wait - required even though it's set to wait only
	 * once
	 */
	UnregisterWaitEx(childinfo->waitHandle, NULL);

	if (!GetExitCodeProcess(childinfo->procHandle, &exitcode))
	{
		/*
		 * Should never happen. Inform user and set a fixed exitcode.
		 */
		write_stderr("could not read exit code for process\n");
		exitcode = 255;
	}

	if (!PostQueuedCompletionStatus(win32ChildQueue, childinfo->procId, (ULONG_PTR) exitcode, NULL))
		write_stderr("could not post child completion status\n");

	/*
	 * Handle is per-process, so we close it here instead of in the
	 * originating thread
	 */
	CloseHandle(childinfo->procHandle);

	/*
	 * Free struct that was allocated before the call to
	 * RegisterWaitForSingleObject()
	 */
	free(childinfo);

	/* Queue SIGCHLD signal */
	pg_queue_signal(SIGCHLD);
}

#endif   /* WIN32 */

#if defined(pg_on_solaris)
/* SOLARIS */
static void
setProcAffinity(int id)
{
	int i, r;
	processor_info_t info;
	int cpu_count=0;
	int cpus[64];

	int proc_to_bind;

	memset(cpus, 0, sizeof(cpus));

	/* first we figure out how many processors we're dealing with (up
	 * to 64). Note: we can't just assume that when we see the first
	 * failure that we're done -- The Solaris documentation claims
	 * that there can be "gaps." */
	for (i=0; i < 64; i++)
	{
		r = processor_info(i, &info);
		if (r == 0)
		{
			cpu_count++;
			cpus[cpu_count - 1] = i;
		}
	}

	/* now we use the id our caller provided to pick a processor. */
	proc_to_bind = cpus[id % cpu_count];

	r = processor_bind(P_PID, P_MYID, proc_to_bind, NULL);
	/* ignore an error -- we'll just stay with out old (default
	 * binding) */
	if (r == 0)
		elog(LOG, "Bound postmaster to processor %d", proc_to_bind);
	else
		elog(LOG, "process_bind() failed, will remain unbound");

	return;
}
#elif defined(HAVE_NUMA_H) && defined(HAVE_LIBNUMA)
/* LINUX */
static void
setProcAffinity(int id)
{
	int limit;
	nodemask_t mask;

	if (numa_available() < 0)
	{
		elog(LOG, "Numa unavailable, will remain unbound.");
		return;
	}

	limit = numa_max_node() + 1;

	nodemask_zero(&mask);
	nodemask_set(&mask, (id % limit));

	elog(LOG, "Numa binding to numa-node %d", (id % limit));

	/* this sets the memory */
	numa_bind(&mask);

	return;
}
#else
/* UNSUPPORTED */
static void
setProcAffinity(int id)
{
	elog(LOG, "gp_proc_affinity setting ignored; feature not configured");
}
#endif
