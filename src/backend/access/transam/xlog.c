/*-------------------------------------------------------------------------
 *
 * xlog.c
 *		PostgreSQL transaction log manager
 *
 *
 * Portions Copyright (c) 1996-2008, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * $PostgreSQL: pgsql/src/backend/access/transam/xlog.c,v 1.258.2.3 2008/04/17 00:00:00 tgl Exp $
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include <ctype.h>
#include <signal.h>
#include <time.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <sys/wait.h>
#include <unistd.h>

#include "access/clog.h"
#include "access/heapam.h"
#include "access/multixact.h"
#include "access/subtrans.h"
#include "access/transam.h"
#include "access/twophase.h"
#include "access/xact.h"
#include "access/xlog_internal.h"
#include "access/xlogmm.h"
#include "access/xlogutils.h"
#include "catalog/catalog.h"
#include "catalog/catversion.h"
#include "catalog/pg_control.h"
#include "catalog/pg_type.h"
#include "catalog/pg_database.h"
#include "catalog/pg_tablespace.h"
#include "funcapi.h"
#include "libpq/pqsignal.h"
#include "miscadmin.h"
#include "pgstat.h"
#include "postmaster/postmaster.h"
#include "postmaster/checkpoint.h"
#include "postmaster/walsendserver.h"
#include "storage/proc.h"
#include "storage/bufpage.h"
#include "storage/fd.h"
#include "storage/ipc.h"
#include "storage/pmsignal.h"
#include "storage/procarray.h"
#include "storage/spin.h"
#include "utils/builtins.h"
#include "utils/faultinjector.h"
#include "utils/flatfiles.h"
#include "utils/pg_locale.h"
#include "utils/nabstime.h"
#include "utils/guc.h"
#include "utils/ps_status.h"
#include "utils/resscheduler.h"
#include "pg_trace.h"
#include "utils/catcache.h"
#include "utils/memutils.h"
#include "utils/syscache.h"
#include "utils/pg_crc.h"
#include "port/pg_crc32c.h"
#include "storage/backendid.h"
#include "storage/sinvaladt.h"

#include "cdb/cdblink.h"
#include "cdb/cdbfilerep.h"
#include "cdb/cdbvars.h"
#include "cdb/cdbpersistentrelfile.h"
#include "cdb/cdbmirroredflatfile.h"
#include "cdb/cdbpersistentrecovery.h"
#include "cdb/cdbresynchronizechangetracking.h"
#include "cdb/cdbpersistentfilesysobj.h"
#include "cdb/cdbfilerep.h"
#include "postmaster/primary_mirror_mode.h"
#include "utils/elog.h"

// Avoid trying to pull in cdbfts.h
extern bool isQDMirroringPendingCatchup(void);
extern void FtsQDMirroringCatchup(XLogRecPtr *flushedLocation);
extern void FtsCheckReadyToSend(bool *ready, bool *haveNewCheckpointLocation, XLogRecPtr *newCheckpointLocation);
extern void FtsQDMirroringNewCheckpointLoc(XLogRecPtr *newCheckpointLocation);
extern void disableQDMirroring_WalSendServerError(char *detail);
extern bool QDMirroringWriteCheck(void);

/*
 *	Because O_DIRECT bypasses the kernel buffers, and because we never
 *	read those buffers except during crash recovery, it is a win to use
 *	it in all cases where we sync on each write().	We could allow O_DIRECT
 *	with fsync(), but because skipping the kernel buffer forces writes out
 *	quickly, it seems best just to use it for O_SYNC.  It is hard to imagine
 *	how fsync() could be a win for O_DIRECT compared to O_SYNC and O_DIRECT.
 *	Also, O_DIRECT is never enough to force data to the drives, it merely
 *	tries to bypass the kernel cache, so we still need O_SYNC or fsync().
 */
#ifdef O_DIRECT
#define PG_O_DIRECT				O_DIRECT
#else
#define PG_O_DIRECT				0
#endif

/*
 * This chunk of hackery attempts to determine which file sync methods
 * are available on the current platform, and to choose an appropriate
 * default method.	We assume that fsync() is always available, and that
 * configure determined whether fdatasync() is.
 */
#if defined(O_SYNC)
#define BARE_OPEN_SYNC_FLAG		O_SYNC
#elif defined(O_FSYNC)
#define BARE_OPEN_SYNC_FLAG		O_FSYNC
#endif
#ifdef BARE_OPEN_SYNC_FLAG
#define OPEN_SYNC_FLAG			(BARE_OPEN_SYNC_FLAG | PG_O_DIRECT)
#endif

#if defined(O_DSYNC)
#if defined(OPEN_SYNC_FLAG)
/* O_DSYNC is distinct? */
#if O_DSYNC != BARE_OPEN_SYNC_FLAG
#define OPEN_DATASYNC_FLAG		(O_DSYNC | PG_O_DIRECT)
#endif
#else							/* !defined(OPEN_SYNC_FLAG) */
/* Win32 only has O_DSYNC */
#define OPEN_DATASYNC_FLAG		(O_DSYNC | PG_O_DIRECT)
#endif
#endif

/*
 * We don't want the default for Solaris to be OPEN_DATASYNC, because
 * (for some reason) it is absurdly slow.
 */
#if !defined(pg_on_solaris) && defined(OPEN_DATASYNC_FLAG)
#define DEFAULT_SYNC_METHOD_STR "open_datasync"
#define DEFAULT_SYNC_METHOD		SYNC_METHOD_OPEN
#define DEFAULT_SYNC_FLAGBIT	OPEN_DATASYNC_FLAG
#elif defined(HAVE_FDATASYNC)
#define DEFAULT_SYNC_METHOD_STR "fdatasync"
#define DEFAULT_SYNC_METHOD		SYNC_METHOD_FDATASYNC
#define DEFAULT_SYNC_FLAGBIT	0
#elif defined(HAVE_FSYNC_WRITETHROUGH_ONLY)
#define DEFAULT_SYNC_METHOD_STR "fsync_writethrough"
#define DEFAULT_SYNC_METHOD		SYNC_METHOD_FSYNC_WRITETHROUGH
#define DEFAULT_SYNC_FLAGBIT	0
#else
#define DEFAULT_SYNC_METHOD_STR "fsync"
#define DEFAULT_SYNC_METHOD		SYNC_METHOD_FSYNC
#define DEFAULT_SYNC_FLAGBIT	0
#endif


/*
 * Limitation of buffer-alignment for direct IO depends on OS and filesystem,
 * but XLOG_BLCKSZ is assumed to be enough for it.
 */
#ifdef O_DIRECT
#define ALIGNOF_XLOG_BUFFER		XLOG_BLCKSZ
#else
#define ALIGNOF_XLOG_BUFFER		ALIGNOF_BUFFER
#endif


/* File path names (all relative to $PGDATA) */
#define BACKUP_LABEL_FILE		"backup_label"
#define BACKUP_LABEL_OLD		"backup_label.old"
#define RECOVERY_COMMAND_FILE	"recovery.conf"
#define RECOVERY_COMMAND_DONE	"recovery.done"


/* User-settable parameters */
int			CheckPointSegments = 3;
int			XLOGbuffers = 8;
int			XLogArchiveTimeout = 0;
char	   *XLogArchiveCommand = NULL;
char	   *XLOG_sync_method = NULL;
const char	XLOG_sync_method_default[] = DEFAULT_SYNC_METHOD_STR;
bool		fullPageWrites = true;

#ifdef WAL_DEBUG
bool		XLOG_DEBUG = false;
#endif

/*
 * XLOGfileslop is used in the code as the allowed "fuzz" in the number of
 * preallocated XLOG segments --- we try to have at least XLOGfiles advance
 * segments but no more than XLOGfileslop segments.  This could
 * be made a separate GUC variable, but at present I think it's sufficient
 * to hardwire it as 2*CheckPointSegments+1.  Under normal conditions, a
 * checkpoint will free no more than 2*CheckPointSegments log segments, and
 * we want to recycle all of them; the +1 allows boundary cases to happen
 * without wasting a delete/create-segment cycle.
 */

#define XLOGfileslop	(2*CheckPointSegments + 1)


/* these are derived from XLOG_sync_method by assign_xlog_sync_method */
int			sync_method = DEFAULT_SYNC_METHOD;
static int	open_sync_bit = DEFAULT_SYNC_FLAGBIT;

#define XLOG_SYNC_BIT  (enableFsync ? open_sync_bit : 0)


/*
 * ThisTimeLineID will be same in all backends --- it identifies current
 * WAL timeline for the database system.
 */
TimeLineID	ThisTimeLineID = 0;

/* Are we doing recovery from XLOG */
bool		InRecovery = false;

/*
 * GOH: During about transaction, some catalog cannot be accessed. Set this
 * parameter will tell the codes which startup pass we are in, so it will
 * use this to access the information from correct place. Such as: primaryOnly
 * will be set correctly in pass 2.
 */
int			InRecoveryPass = 0;

/* Can we emit xlog records */
bool		CanEmitXLogRecords = true;

/* Are we recovering using offline XLOG archives? */
static bool InArchiveRecovery = false;

/* Was the last xlog file restored from archive, or local? */
static bool restoredFromArchive = false;

/* options taken from recovery.conf */
static char *recoveryRestoreCommand = NULL;
static bool recoveryTarget = false;
static bool recoveryTargetExact = false;
static bool recoveryTargetInclusive = true;
static TransactionId recoveryTargetXid;
static time_t recoveryTargetTime;

/* if recoveryStopsHere returns true, it saves actual stop xid/time here */
static TransactionId recoveryStopXid;
static time_t recoveryStopTime;
static bool recoveryStopAfter;

/*
 * During normal operation, the only timeline we care about is ThisTimeLineID.
 * During recovery, however, things are more complicated.  To simplify life
 * for rmgr code, we keep ThisTimeLineID set to the "current" timeline as we
 * scan through the WAL history (that is, it is the line that was active when
 * the currently-scanned WAL record was generated).  We also need these
 * timeline values:
 *
 * recoveryTargetTLI: the desired timeline that we want to end in.
 *
 * expectedTLIs: an integer list of recoveryTargetTLI and the TLIs of
 * its known parents, newest first (so recoveryTargetTLI is always the
 * first list member).	Only these TLIs are expected to be seen in the WAL
 * segments we read, and indeed only these TLIs will be considered as
 * candidate WAL files to open at all.
 *
 * curFileTLI: the TLI appearing in the name of the current input WAL file.
 * (This is not necessarily the same as ThisTimeLineID, because we could
 * be scanning data that was copied from an ancestor timeline when the current
 * file was created.)  During a sequential scan we do not allow this value
 * to decrease.
 */
static TimeLineID recoveryTargetTLI;
List *expectedTLIs;
static TimeLineID curFileTLI;

/*
 * MyLastRecPtr points to the start of the last XLOG record inserted by the
 * current transaction.  If MyLastRecPtr.xrecoff == 0, then the current
 * xact hasn't yet inserted any transaction-controlled XLOG records.
 *
 * Note that XLOG records inserted outside transaction control are not
 * reflected into MyLastRecPtr.  They do, however, cause MyXactMadeXLogEntry
 * to be set true.	The latter can be used to test whether the current xact
 * made any loggable changes (including out-of-xact changes, such as
 * sequence updates).
 *
 * When we insert/update/delete a tuple in a temporary relation, we do not
 * make any XLOG record, since we don't care about recovering the state of
 * the temp rel after a crash.	However, we will still need to remember
 * whether our transaction committed or aborted in that case.  So, we must
 * set MyXactMadeTempRelUpdate true to indicate that the XID will be of
 * interest later.
 */
XLogRecPtr	MyLastRecPtr = {0, 0};

bool		MyXactMadeXLogEntry = false;

bool		MyXactMadeTempRelUpdate = false;

/*
 * ProcLastRecPtr points to the start of the last XLOG record inserted by the
 * current backend.  It is updated for all inserts, transaction-controlled
 * or not.	ProcLastRecEnd is similar but points to end+1 of last record.
 */
static XLogRecPtr ProcLastRecPtr = {0, 0};

XLogRecPtr	ProcLastRecEnd = {0, 0};

static uint32 ProcLastRecTotalLen = 0;

static uint32 ProcLastRecDataLen = 0;

/*
 * RedoRecPtr is this backend's local copy of the REDO record pointer
 * (which is almost but not quite the same as a pointer to the most recent
 * CHECKPOINT record).	We update this from the shared-memory copy,
 * XLogCtl->Insert.RedoRecPtr, whenever we can safely do so (ie, when we
 * hold the Insert lock).  See XLogInsert for details.	We are also allowed
 * to update from XLogCtl->Insert.RedoRecPtr if we hold the info_lck;
 * see GetRedoRecPtr.  A freshly spawned backend obtains the value during
 * InitXLOGAccess.
 */
static XLogRecPtr RedoRecPtr;

/*----------
 * Shared-memory data structures for XLOG control
 *
 * LogwrtRqst indicates a byte position that we need to write and/or fsync
 * the log up to (all records before that point must be written or fsynced).
 * LogwrtResult indicates the byte positions we have already written/fsynced.
 * These structs are identical but are declared separately to indicate their
 * slightly different functions.
 *
 * We do a lot of pushups to minimize the amount of access to lockable
 * shared memory values.  There are actually three shared-memory copies of
 * LogwrtResult, plus one unshared copy in each backend.  Here's how it works:
 *		XLogCtl->LogwrtResult is protected by info_lck
 *		XLogCtl->Write.LogwrtResult is protected by WALWriteLock
 *		XLogCtl->Insert.LogwrtResult is protected by WALInsertLock
 * One must hold the associated lock to read or write any of these, but
 * of course no lock is needed to read/write the unshared LogwrtResult.
 *
 * XLogCtl->LogwrtResult and XLogCtl->Write.LogwrtResult are both "always
 * right", since both are updated by a write or flush operation before
 * it releases WALWriteLock.  The point of keeping XLogCtl->Write.LogwrtResult
 * is that it can be examined/modified by code that already holds WALWriteLock
 * without needing to grab info_lck as well.
 *
 * XLogCtl->Insert.LogwrtResult may lag behind the reality of the other two,
 * but is updated when convenient.	Again, it exists for the convenience of
 * code that is already holding WALInsertLock but not the other locks.
 *
 * The unshared LogwrtResult may lag behind any or all of these, and again
 * is updated when convenient.
 *
 * The request bookkeeping is simpler: there is a shared XLogCtl->LogwrtRqst
 * (protected by info_lck), but we don't need to cache any copies of it.
 *
 * Note that this all works because the request and result positions can only
 * advance forward, never back up, and so we can easily determine which of two
 * values is "more up to date".
 *
 * info_lck is only held long enough to read/update the protected variables,
 * so it's a plain spinlock.  The other locks are held longer (potentially
 * over I/O operations), so we use LWLocks for them.  These locks are:
 *
 * WALInsertLock: must be held to insert a record into the WAL buffers.
 *
 * WALWriteLock: must be held to write WAL buffers to disk (XLogWrite or
 * XLogFlush).
 *
 * ControlFileLock: must be held to read/update control file or create
 * new log file.
 *
 * CheckpointLock: must be held to do a checkpoint (ensures only one
 * checkpointer at a time; currently, with all checkpoints done by the
 * bgwriter, this is just pro forma).
 *
 *----------
 */

typedef struct XLogwrtRqst
{
	XLogRecPtr	Write;			/* last byte + 1 to write out */
	XLogRecPtr	Flush;			/* last byte + 1 to flush */
} XLogwrtRqst;

typedef struct XLogwrtResult
{
	XLogRecPtr	Write;			/* last byte + 1 written out */
	XLogRecPtr	Flush;			/* last byte + 1 flushed */
} XLogwrtResult;

/*
 * Shared state data for XLogInsert.
 */
typedef struct XLogCtlInsert
{
	XLogwrtResult LogwrtResult; /* a recent value of LogwrtResult */
	XLogRecPtr	PrevRecord;		/* start of previously-inserted record */
	int			curridx;		/* current block index in cache */
	XLogPageHeader currpage;	/* points to header of block in cache */
	char	   *currpos;		/* current insertion point in cache */
	XLogRecPtr	RedoRecPtr;		/* current redo point for insertions */
	bool		forcePageWrites;	/* forcing full-page writes for PITR? */
} XLogCtlInsert;

/*
 * Shared state data for XLogWrite/XLogFlush.
 */
typedef struct XLogCtlWrite
{
	XLogwrtResult LogwrtResult; /* current value of LogwrtResult */
	int			curridx;		/* cache index of next block to write */
	time_t		lastSegSwitchTime;		/* time of last xlog segment switch */
} XLogCtlWrite;

/*
 * Total shared-memory state for XLOG.
 */
typedef struct XLogCtlData
{
	/* Protected by WALInsertLock: */
	XLogCtlInsert Insert;

	/* Protected by info_lck: */
	XLogwrtRqst LogwrtRqst;
	XLogwrtResult LogwrtResult;
	uint32		ckptXidEpoch;	/* nextXID & epoch of latest checkpoint */
	TransactionId ckptXid;

	/* Protected by WALWriteLock: */
	XLogCtlWrite Write;

	/* Protected by ChangeTrackingTransitionLock. */
	XLogRecPtr	lastChangeTrackingEndLoc;
								/*
								 * End + 1 of the last XLOG record inserted and
 								 * (possible) change tracked.
 								 */

	/* Resynchronize */
	bool		sendingResynchronizeTransitionMsg;
	slock_t		resynchronize_lck;		/* locks shared variables shown above */

	/*
	 * These values do not change after startup, although the pointed-to pages
	 * and xlblocks values certainly do.  Permission to read/write the pages
	 * and xlblocks values depends on WALInsertLock and WALWriteLock.
	 */
	char	   *pages;			/* buffers for unwritten XLOG pages */
	XLogRecPtr *xlblocks;		/* 1st byte ptr-s + XLOG_BLCKSZ */
	Size		XLogCacheByte;	/* # bytes in xlog buffers */
	int			XLogCacheBlck;	/* highest allocated xlog buffer index */
	TimeLineID	ThisTimeLineID;

	slock_t		info_lck;		/* locks shared variables shown above */

	/*
	 * Save the location of the last checkpoint record to enable supressing
	 * unnecessary checkpoint records -- when no new xlog has been written
	 * since the last one.
	 */
	bool 		haveLastCheckpointLoc;
	XLogRecPtr	lastCheckpointLoc;
	XLogRecPtr	lastCheckpointEndLoc;

	/*
	 * Save the redo range used in Pass 1 recovery so it can be used in subsequent passes.
	 */
	bool		multipleRecoveryPassesNeeded;
	XLogRecPtr	pass1StartLoc;
	XLogRecPtr	pass1LastLoc;
        XLogRecPtr      pass1LastCheckpointLoc;

	bool		integrityCheckNeeded;

} XLogCtlData;

static XLogCtlData *XLogCtl = NULL;

/*
 * We maintain an image of pg_control in shared memory.
 */
static ControlFileData *ControlFile = NULL;

typedef struct ControlFileWatch
{
	bool		watcherInitialized;
	XLogRecPtr	current_checkPointLoc;		/* current last check point record ptr */
	XLogRecPtr	current_prevCheckPointLoc;  /* current previous check point record ptr */
	XLogRecPtr	current_checkPointCopy_redo;
								/* current checkpointCopy value for
								 * next RecPtr available when we began to
								 * create CheckPoint (i.e. REDO start point) */

} ControlFileWatch;


/*
 * We keep the watcher in shared memory.
 */
static ControlFileWatch *ControlFileWatcher = NULL;

/*
 * Macros for managing XLogInsert state.  In most cases, the calling routine
 * has local copies of XLogCtl->Insert and/or XLogCtl->Insert->curridx,
 * so these are passed as parameters instead of being fetched via XLogCtl.
 */

/* Free space remaining in the current xlog page buffer */
#define INSERT_FREESPACE(Insert)  \
	(XLOG_BLCKSZ - ((Insert)->currpos - (char *) (Insert)->currpage))

/* Construct XLogRecPtr value for current insertion point */
#define INSERT_RECPTR(recptr,Insert,curridx)  \
	( \
	  (recptr).xlogid = XLogCtl->xlblocks[curridx].xlogid, \
	  (recptr).xrecoff = \
		XLogCtl->xlblocks[curridx].xrecoff - INSERT_FREESPACE(Insert) \
	)

#define PrevBufIdx(idx)		\
		(((idx) == 0) ? XLogCtl->XLogCacheBlck : ((idx) - 1))

#define NextBufIdx(idx)		\
		(((idx) == XLogCtl->XLogCacheBlck) ? 0 : ((idx) + 1))

/*
 * Private, possibly out-of-date copy of shared LogwrtResult.
 * See discussion above.
 */
static XLogwrtResult LogwrtResult = {{0, 0}, {0, 0}};

/*
 * openLogFile is -1 or a kernel FD for an open log file segment.
 * When it's open, openLogOff is the current seek offset in the file.
 * openLogId/openLogSeg identify the segment.  These variables are only
 * used to write the XLOG, and so will normally refer to the active segment.
 */
static MirroredFlatFileOpen	mirroredLogFileOpen = MirroredFlatFileOpen_Init;
static uint32 openLogId = 0;
static uint32 openLogSeg = 0;
static uint32 openLogOff = 0;

/*
 * These variables are used similarly to the ones above, but for reading
 * the XLOG.  Note, however, that readOff generally represents the offset
 * of the page just read, not the seek position of the FD itself, which
 * will be just past that page.
 */
static int	readFile = -1;
static uint32 readId = 0;
static uint32 readSeg = 0;
static uint32 readOff = 0;

/* Buffer for currently read page (XLOG_BLCKSZ bytes) */
static char *readBuf = NULL;

/* Buffer for current ReadRecord result (expandable) */
static char *readRecordBuf = NULL;
static uint32 readRecordBufSize = 0;

/* State information for XLOG reading */
static XLogRecPtr ReadRecPtr;	/* start of last record read */
static XLogRecPtr EndRecPtr;	/* end+1 of last record read */
static XLogRecord *nextRecord = NULL;
static TimeLineID lastPageTLI = 0;

static bool InRedo = false;

/* Aligning xlog mirrored buffer */
static int32 writeBufLen = 0;
static char	*writeBuf = NULL;

char	*writeBufAligned = NULL;

/*
 * Flag set by interrupt handlers for later service in the redo loop.
 */
static volatile sig_atomic_t got_SIGHUP = false;
static volatile sig_atomic_t shutdown_requested = false;
/*
 * Flag set when executing a restore command, to tell SIGTERM signal handler
 * that it's safe to just proc_exit.
 */
static volatile sig_atomic_t in_restore_command = false;

static void XLogArchiveNotify(const char *xlog);
static void XLogArchiveNotifySeg(uint32 log, uint32 seg);
static bool XLogArchiveCheckDone(const char *xlog);
static void XLogArchiveCleanup(const char *xlog);
static void exitArchiveRecovery(TimeLineID endTLI,
					uint32 endLogId, uint32 endLogSeg);
static bool recoveryStopsHere(XLogRecord *record, bool *includeThis);
static void CheckPointGuts(XLogRecPtr checkPointRedo);
static void Checkpoint_RecoveryPass(XLogRecPtr checkPointRedo);
static bool XLogCheckBuffer(XLogRecData *rdata, bool doPageWrites,
				XLogRecPtr *lsn, BkpBlock *bkpb);
static bool AdvanceXLInsertBuffer(bool new_segment);
static void XLogWrite(XLogwrtRqst WriteRqst, bool flexible, bool xlog_switch);
static void XLogFileInit(
			 MirroredFlatFileOpen *mirroredOpen,
			 uint32 log, uint32 seg,
			 bool *use_existent, bool use_lock);
static bool InstallXLogFileSegment(uint32 *log, uint32 *seg, char *tmppath,
					   bool find_free, int *max_advance,
					   bool use_lock, char *tmpsimpleFileName);
static void XLogFileOpen(
				MirroredFlatFileOpen *mirroredOpen,
				uint32 log,
				uint32 seg);
static int	XLogFileRead(uint32 log, uint32 seg, int emode);

static bool RestoreArchivedFile(char *path, const char *xlogfname,
					const char *recovername, off_t expectedSize);
static int	PreallocXlogFiles(XLogRecPtr endptr);
static void MoveOfflineLogs(uint32 log, uint32 seg, XLogRecPtr endptr,
				int *nsegsremoved, int *nsegsrecycled);
static void CleanupBackupHistory(void);
static bool ValidXLOGHeader(XLogPageHeader hdr, int emode);

static bool existsTimeLineHistory(TimeLineID probeTLI);
static TimeLineID findNewestTimeLine(TimeLineID startTLI);
static void writeTimeLineHistory(TimeLineID newTLI, TimeLineID parentTLI,
					 TimeLineID endTLI,
					 uint32 endLogId, uint32 endLogSeg);
static void ControlFileWatcherSaveInitial(void);
static void ControlFileWatcherCheckForChange(void);
static bool XLogGetWriteAndFlushedLoc(XLogRecPtr *writeLoc, XLogRecPtr *flushedLoc);
static XLogRecPtr XLogInsert_Internal(RmgrId rmid, uint8 info, XLogRecData *rdata, TransactionId headerXid);
static void WriteControlFile(void);
static void ReadControlFile(void);
static char *str_time(pg_time_t tnow);
#ifdef suppress
static void issue_xlog_fsync(void);
#endif
static void pg_start_backup_callback(int code, Datum arg);
static bool read_backup_label(XLogRecPtr *checkPointLoc,
				  XLogRecPtr *minRecoveryLoc);

typedef struct RedoErrorCallBack
{
	XLogRecPtr	location;

	XLogRecord 	*record;
} RedoErrorCallBack;

static void rm_redo_error_callback(void *arg);

static void XLogQDMirrorWrite(int startidx, int npages, TimeLineID timeLineID, uint32 logId, uint32 logSeg, uint32 logOff);
static void XLogQDMirrorFlush(void);
static bool XLogCatchupQDSegment(uint32 logId, uint32 seg, uint32 offset, uint32 end, struct timeval *timeout, bool *shutdownGlobal);
static bool XLogQDMirrorWaitForResponse(bool waitForever);

/*
 * Whether we need to always generate transaction log (XLOG), or if we can
 * bypass it and get better performance.
 *
 * For GPDB, we do not support XLogArchivingActive(), so we don't use it as a condition.
 */
bool XLog_CanBypassWal(void)
{
	if (Debug_bulk_load_bypass_wal)
	{
		/*
		 * We need the XLOG to be transmitted to the standby master since it is not using
		 * FileRep technology yet.
		 */
		return !AmActiveMaster();
	}
	else
	{
		return false;
	}
}

/*
 * For FileRep code that doesn't have the Bypass WAL logic yet.
 */
bool XLog_UnconvertedCanBypassWal(void)
{
	return false;
}

static char *XLogContiguousCopy(
	XLogRecord 		*record,

	XLogRecData 	*rdata)
{
	XLogRecData *rdt;
	int32 len;
	char *buffer;

	rdt = rdata;
	len = sizeof(XLogRecord);
	while (rdt != NULL)
	{
		if (rdt->data != NULL)
		{
			len += rdt->len;
		}
		rdt = rdt->next;
	}

	buffer = (char*)palloc(len);

	memcpy(buffer, record, sizeof(XLogRecord));
	rdt = rdata;
	len = sizeof(XLogRecord);
	while (rdt != NULL)
	{
		if (rdt->data != NULL)
		{
			memcpy(&buffer[len], rdt->data, rdt->len);
			len += rdt->len;
		}
		rdt = rdt->next;
	}

	return buffer;
}

/*
 * Insert an XLOG record having the specified RMID and info bytes,
 * with the body of the record being the data chunk(s) described by
 * the rdata chain (see xlog.h for notes about rdata).
 *
 * Returns XLOG pointer to end of record (beginning of next record).
 * This can be used as LSN for data pages affected by the logged action.
 * (LSN is the XLOG point up to which the XLOG must be flushed to disk
 * before the data page can be written out.  This implements the basic
 * WAL rule "write the log before the data".)
 *
 * NB: this routine feels free to scribble on the XLogRecData structs,
 * though not on the data they reference.  This is OK since the XLogRecData
 * structs are always just temporaries in the calling code.
 */
XLogRecPtr
XLogInsert(RmgrId rmid, uint8 info, XLogRecData *rdata)
{
	return XLogInsert_Internal(rmid, info, rdata, GetCurrentTransactionIdIfAny());
}

XLogRecPtr
XLogInsert_OverrideXid(RmgrId rmid, uint8 info, XLogRecData *rdata, TransactionId overrideXid)
{
	return XLogInsert_Internal(rmid, info, rdata, overrideXid);
}


static XLogRecPtr
XLogInsert_Internal(RmgrId rmid, uint8 info, XLogRecData *rdata, TransactionId headerXid)
{

	XLogCtlInsert *Insert = &XLogCtl->Insert;
	XLogRecord *record;
	XLogContRecord *contrecord;
	XLogRecPtr	RecPtr;
	XLogRecPtr	WriteRqst;
	uint32		freespace;
	int			curridx;
	XLogRecData *rdt;
	Buffer		dtbuf[XLR_MAX_BKP_BLOCKS];
	bool		dtbuf_bkp[XLR_MAX_BKP_BLOCKS];
	BkpBlock	dtbuf_xlg[XLR_MAX_BKP_BLOCKS];
	XLogRecPtr	dtbuf_lsn[XLR_MAX_BKP_BLOCKS];
	XLogRecData dtbuf_rdt1[XLR_MAX_BKP_BLOCKS];
	XLogRecData dtbuf_rdt2[XLR_MAX_BKP_BLOCKS];
	XLogRecData dtbuf_rdt3[XLR_MAX_BKP_BLOCKS];
	pg_crc32	rdata_crc;
	uint32		len,
				write_len;
	unsigned	i;
	XLogwrtRqst LogwrtRqst;
	bool		updrqst;
	bool		doPageWrites;
	bool		isLogSwitch = (rmid == RM_XLOG_ID && info == XLOG_SWITCH);
	bool		no_tran = (rmid == RM_XLOG_ID);

	MemSet(&dtbuf_lsn, '\0', sizeof(dtbuf_lsn));

	if (info & XLR_INFO_MASK)
	{
		if ((info & XLR_INFO_MASK) != XLOG_NO_TRAN)
			elog(PANIC, "invalid xlog info mask %02X", (info & XLR_INFO_MASK));
		no_tran = true;
		info &= ~XLR_INFO_MASK;
	}

	/*
	 * In bootstrap mode, we don't actually log anything but XLOG resources;
	 * return a phony record pointer.
	 */
	if (IsBootstrapProcessingMode() && rmid != RM_XLOG_ID)
	{
		RecPtr.xlogid = 0;
		RecPtr.xrecoff = SizeOfXLogLongPHD;		/* start of 1st chkpt record */
		return RecPtr;
	}

	/*
	 * Here we scan the rdata chain, determine which buffers must be backed
	 * up, and compute the CRC values for the data.  Note that the record
	 * header isn't added into the CRC initially since we don't know the final
	 * length or info bits quite yet.  Thus, the CRC will represent the CRC of
	 * the whole record in the order "rdata, then backup blocks, then record
	 * header".
	 *
	 * We may have to loop back to here if a race condition is detected below.
	 * We could prevent the race by doing all this work while holding the
	 * insert lock, but it seems better to avoid doing CRC calculations while
	 * holding the lock.  This means we have to be careful about modifying the
	 * rdata chain until we know we aren't going to loop back again.  The only
	 * change we allow ourselves to make earlier is to set rdt->data = NULL in
	 * chain items we have decided we will have to back up the whole buffer
	 * for.  This is OK because we will certainly decide the same thing again
	 * for those items if we do it over; doing it here saves an extra pass
	 * over the chain later.
	 */
begin:;
	for (i = 0; i < XLR_MAX_BKP_BLOCKS; i++)
	{
		dtbuf[i] = InvalidBuffer;
		dtbuf_bkp[i] = false;
	}

	/*
	 * Decide if we need to do full-page writes in this XLOG record: true if
	 * full_page_writes is on or we have a PITR request for it.  Since we
	 * don't yet have the insert lock, forcePageWrites could change under us,
	 * but we'll recheck it once we have the lock.
	 */
	doPageWrites = fullPageWrites || Insert->forcePageWrites;

	INIT_CRC32C(rdata_crc);
	len = 0;
	for (rdt = rdata;;)
	{
		if (rdt->buffer == InvalidBuffer)
		{
			/* Simple data, just include it */
			len += rdt->len;
			COMP_CRC32C(rdata_crc, rdt->data, rdt->len);
		}
		else
		{
			/* Find info for buffer */
			for (i = 0; i < XLR_MAX_BKP_BLOCKS; i++)
			{
				if (rdt->buffer == dtbuf[i])
				{
					/* Buffer already referenced by earlier chain item */
					if (dtbuf_bkp[i])
						rdt->data = NULL;
					else if (rdt->data)
					{
						len += rdt->len;
						COMP_CRC32C(rdata_crc, rdt->data, rdt->len);
					}
					break;
				}
				if (dtbuf[i] == InvalidBuffer)
				{
					/* OK, put it in this slot */
					dtbuf[i] = rdt->buffer;
					if (XLogCheckBuffer(rdt, doPageWrites,
										&(dtbuf_lsn[i]), &(dtbuf_xlg[i])))
					{
						dtbuf_bkp[i] = true;
						rdt->data = NULL;
					}
					else if (rdt->data)
					{
						len += rdt->len;
						COMP_CRC32C(rdata_crc, rdt->data, rdt->len);
					}
					break;
				}
			}
			if (i >= XLR_MAX_BKP_BLOCKS)
				elog(PANIC, "can backup at most %d blocks per xlog record",
					 XLR_MAX_BKP_BLOCKS);
		}
		/* Break out of loop when rdt points to last chain item */
		if (rdt->next == NULL)
			break;
		rdt = rdt->next;
	}

	/*
	 * Now add the backup block headers and data into the CRC
	 */
	for (i = 0; i < XLR_MAX_BKP_BLOCKS; i++)
	{
		if (dtbuf_bkp[i])
		{
			BkpBlock   *bkpb = &(dtbuf_xlg[i]);
			char	   *page;

			COMP_CRC32C(rdata_crc,
					   (char *) bkpb,
					   sizeof(BkpBlock));
			page = (char *) BufferGetBlock(dtbuf[i]);
			if (bkpb->hole_length == 0)
			{
				COMP_CRC32C(rdata_crc,
						   page,
						   BLCKSZ);
			}
			else
			{
				/* must skip the hole */
				COMP_CRC32C(rdata_crc,
						   page,
						   bkpb->hole_offset);
				COMP_CRC32C(rdata_crc,
						   page + (bkpb->hole_offset + bkpb->hole_length),
						   BLCKSZ - (bkpb->hole_offset + bkpb->hole_length));
			}
		}
	}

	/*
	 * NOTE: We disallow len == 0 because it provides a useful bit of extra
	 * error checking in ReadRecord.  This means that all callers of
	 * XLogInsert must supply at least some not-in-a-buffer data.  However, we
	 * make an exception for XLOG SWITCH records because we don't want them to
	 * ever cross a segment boundary.
	 */
	if (len == 0 && !isLogSwitch)
		elog(PANIC, "invalid xlog record length %u", len);

	START_CRIT_SECTION();

	/* update LogwrtResult before doing cache fill check */
	{
		/* use volatile pointer to prevent code rearrangement */
		volatile XLogCtlData *xlogctl = XLogCtl;

		SpinLockAcquire(&xlogctl->info_lck);
		LogwrtRqst = xlogctl->LogwrtRqst;
		LogwrtResult = xlogctl->LogwrtResult;
		SpinLockRelease(&xlogctl->info_lck);
	}

	/*
	 * If cache is half filled then try to acquire write lock and do
	 * XLogWrite. Ignore any fractional blocks in performing this check.
	 */
	LogwrtRqst.Write.xrecoff -= LogwrtRqst.Write.xrecoff % XLOG_BLCKSZ;
	if (LogwrtRqst.Write.xlogid != LogwrtResult.Write.xlogid ||
		(LogwrtRqst.Write.xrecoff >= LogwrtResult.Write.xrecoff +
		 XLogCtl->XLogCacheByte / 2))
	{
		if (LWLockConditionalAcquire(WALWriteLock, LW_EXCLUSIVE))
		{
			/*
			 * Since the amount of data we write here is completely optional
			 * anyway, tell XLogWrite it can be "flexible" and stop at a
			 * convenient boundary.  This allows writes triggered by this
			 * mechanism to synchronize with the cache boundaries, so that in
			 * a long transaction we'll basically dump alternating halves of
			 * the buffer array.
			 */
			LogwrtResult = XLogCtl->Write.LogwrtResult;
			if (XLByteLT(LogwrtResult.Write, LogwrtRqst.Write))
				XLogWrite(LogwrtRqst, true, false);
			LWLockRelease(WALWriteLock);
		}
	}

	/* Now wait to get insert lock */
	LWLockAcquire(WALInsertLock, LW_EXCLUSIVE);

	/*
	 * Check to see if my RedoRecPtr is out of date.  If so, may have to go
	 * back and recompute everything.  This can only happen just after a
	 * checkpoint, so it's better to be slow in this case and fast otherwise.
	 *
	 * If we aren't doing full-page writes then RedoRecPtr doesn't actually
	 * affect the contents of the XLOG record, so we'll update our local copy
	 * but not force a recomputation.
	 */
	if (!XLByteEQ(RedoRecPtr, Insert->RedoRecPtr))
	{
		Assert(XLByteLT(RedoRecPtr, Insert->RedoRecPtr));
		RedoRecPtr = Insert->RedoRecPtr;

		if (doPageWrites)
		{
			for (i = 0; i < XLR_MAX_BKP_BLOCKS; i++)
			{
				if (dtbuf[i] == InvalidBuffer)
					continue;
				if (dtbuf_bkp[i] == false &&
					XLByteLE(dtbuf_lsn[i], RedoRecPtr))
				{
					/*
					 * Oops, this buffer now needs to be backed up, but we
					 * didn't think so above.  Start over.
					 */
					LWLockRelease(WALInsertLock);

					END_CRIT_SECTION();
					goto begin;
				}
			}
		}
	}

	/*
	 * Also check to see if forcePageWrites was just turned on; if we weren't
	 * already doing full-page writes then go back and recompute. (If it was
	 * just turned off, we could recompute the record without full pages, but
	 * we choose not to bother.)
	 */
	if (Insert->forcePageWrites && !doPageWrites)
	{
		/* Oops, must redo it with full-page data */
		LWLockRelease(WALInsertLock);

		END_CRIT_SECTION();
		goto begin;
	}

	/*
	 * Make additional rdata chain entries for the backup blocks, so that we
	 * don't need to special-case them in the write loop.  Note that we have
	 * now irrevocably changed the input rdata chain.  At the exit of this
	 * loop, write_len includes the backup block data.
	 *
	 * Also set the appropriate info bits to show which buffers were backed
	 * up. The i'th XLR_SET_BKP_BLOCK bit corresponds to the i'th distinct
	 * buffer value (ignoring InvalidBuffer) appearing in the rdata chain.
	 */
	write_len = len;
	for (i = 0; i < XLR_MAX_BKP_BLOCKS; i++)
	{
		BkpBlock   *bkpb;
		char	   *page;

		if (!dtbuf_bkp[i])
			continue;

		info |= XLR_SET_BKP_BLOCK(i);

		bkpb = &(dtbuf_xlg[i]);
		page = (char *) BufferGetBlock(dtbuf[i]);

		rdt->next = &(dtbuf_rdt1[i]);
		rdt = rdt->next;

		rdt->data = (char *) bkpb;
		rdt->len = sizeof(BkpBlock);
		write_len += sizeof(BkpBlock);

		rdt->next = &(dtbuf_rdt2[i]);
		rdt = rdt->next;

		if (bkpb->hole_length == 0)
		{
			rdt->data = page;
			rdt->len = BLCKSZ;
			write_len += BLCKSZ;
			rdt->next = NULL;
		}
		else
		{
			/* must skip the hole */
			rdt->data = page;
			rdt->len = bkpb->hole_offset;
			write_len += bkpb->hole_offset;

			rdt->next = &(dtbuf_rdt3[i]);
			rdt = rdt->next;

			rdt->data = page + (bkpb->hole_offset + bkpb->hole_length);
			rdt->len = BLCKSZ - (bkpb->hole_offset + bkpb->hole_length);
			write_len += rdt->len;
			rdt->next = NULL;
		}
	}

	/*
	 * If there isn't enough space on the current XLOG page for a record
	 * header, advance to the next page (leaving the unused space as zeroes).
	 */
	updrqst = false;
	freespace = INSERT_FREESPACE(Insert);
	if (freespace < SizeOfXLogRecord)
	{
		updrqst = AdvanceXLInsertBuffer(false);
		freespace = INSERT_FREESPACE(Insert);
	}

	/* Compute record's XLOG location */
	curridx = Insert->curridx;
	INSERT_RECPTR(RecPtr, Insert, curridx);

	/*
	 * If the record is an XLOG_SWITCH, and we are exactly at the start of a
	 * segment, we need not insert it (and don't want to because we'd like
	 * consecutive switch requests to be no-ops).  Instead, make sure
	 * everything is written and flushed through the end of the prior segment,
	 * and return the prior segment's end address.
	 */
	if (isLogSwitch &&
		(RecPtr.xrecoff % XLogSegSize) == SizeOfXLogLongPHD)
	{
		LWLockRelease(WALInsertLock);

		RecPtr.xrecoff -= SizeOfXLogLongPHD;
		if (RecPtr.xrecoff == 0)
		{
			/* crossing a logid boundary */
			RecPtr.xlogid -= 1;
			RecPtr.xrecoff = XLogFileSize;
		}

		LWLockAcquire(WALWriteLock, LW_EXCLUSIVE);
		LogwrtResult = XLogCtl->Write.LogwrtResult;
		if (!XLByteLE(RecPtr, LogwrtResult.Flush))
		{
			XLogwrtRqst FlushRqst;

			FlushRqst.Write = RecPtr;
			FlushRqst.Flush = RecPtr;
			XLogWrite(FlushRqst, false, false);
		}
		LWLockRelease(WALWriteLock);

		END_CRIT_SECTION();

		return RecPtr;
	}

	/* Insert record header */

	record = (XLogRecord *) Insert->currpos;
	record->xl_prev = Insert->PrevRecord;
	record->xl_xid = headerXid;
	record->xl_tot_len = SizeOfXLogRecord + write_len;
	record->xl_len = len;		/* doesn't include backup blocks */
	record->xl_info = info;
	record->xl_rmid = rmid;

	/* Now we can finish computing the record's CRC */
	COMP_CRC32C(rdata_crc, (char *) record + sizeof(pg_crc32),
			   SizeOfXLogRecord - sizeof(pg_crc32));
	FIN_CRC32C(rdata_crc);
	record->xl_crc = rdata_crc;

	/* Record begin of record in appropriate places */
	if (!no_tran)
		MyLastRecPtr = RecPtr;
	ProcLastRecPtr = RecPtr;
	Insert->PrevRecord = RecPtr;
	MyXactMadeXLogEntry = true;

	ProcLastRecTotalLen = record->xl_tot_len;
	ProcLastRecDataLen = write_len;

	Insert->currpos += SizeOfXLogRecord;
	freespace -= SizeOfXLogRecord;

	if (Debug_xlog_insert_print)
	{
		StringInfoData buf;
		char *contiguousCopy;

		initStringInfo(&buf);
		appendStringInfo(&buf, "XLOG INSERT @ %s, total length %u, data length %u: ",
						 XLogLocationToString(&RecPtr),
						 ProcLastRecTotalLen,
						 ProcLastRecDataLen);
		XLog_OutRec(&buf, record);

		contiguousCopy = XLogContiguousCopy(record, rdata);
		appendStringInfo(&buf, " - ");
		RmgrTable[record->xl_rmid].rm_desc(&buf, RecPtr, (XLogRecord*)contiguousCopy);
		pfree(contiguousCopy);

		elog(LOG, "%s", buf.data);
		pfree(buf.data);
	}

	/*
	 * Append the data, including backup blocks if any
	 */
	while (write_len)
	{
		while (rdata->data == NULL)
			rdata = rdata->next;

		if (freespace > 0)
		{
			if (rdata->len > freespace)
			{
				memcpy(Insert->currpos, rdata->data, freespace);
				rdata->data += freespace;
				rdata->len -= freespace;
				write_len -= freespace;
			}
			else
			{
				/* enough room to write whole data. do it. */
				memcpy(Insert->currpos, rdata->data, rdata->len);
				freespace -= rdata->len;
				write_len -= rdata->len;
				Insert->currpos += rdata->len;
				rdata = rdata->next;
				continue;
			}
		}

		/* Use next buffer */
		updrqst = AdvanceXLInsertBuffer(false);
		curridx = Insert->curridx;
		/* Insert cont-record header */
		Insert->currpage->xlp_info |= XLP_FIRST_IS_CONTRECORD;
		contrecord = (XLogContRecord *) Insert->currpos;
		contrecord->xl_rem_len = write_len;
		Insert->currpos += SizeOfXLogContRecord;
		freespace = INSERT_FREESPACE(Insert);
	}

	/* Ensure next record will be properly aligned */
	Insert->currpos = (char *) Insert->currpage +
		MAXALIGN(Insert->currpos - (char *) Insert->currpage);
	freespace = INSERT_FREESPACE(Insert);

	/*
	 * The recptr I return is the beginning of the *next* record. This will be
	 * stored as LSN for changed data pages...
	 */
	INSERT_RECPTR(RecPtr, Insert, curridx);

	/*
	 * If the record is an XLOG_SWITCH, we must now write and flush all the
	 * existing data, and then forcibly advance to the start of the next
	 * segment.  It's not good to do this I/O while holding the insert lock,
	 * but there seems too much risk of confusion if we try to release the
	 * lock sooner.  Fortunately xlog switch needn't be a high-performance
	 * operation anyway...
	 */
	if (isLogSwitch)
	{
		XLogCtlWrite *Write = &XLogCtl->Write;
		XLogwrtRqst FlushRqst;
		XLogRecPtr	OldSegEnd;

		LWLockAcquire(WALWriteLock, LW_EXCLUSIVE);

		/*
		 * Flush through the end of the page containing XLOG_SWITCH, and
		 * perform end-of-segment actions (eg, notifying archiver).
		 */
		WriteRqst = XLogCtl->xlblocks[curridx];
		FlushRqst.Write = WriteRqst;
		FlushRqst.Flush = WriteRqst;
		XLogWrite(FlushRqst, false, true);

		/* Set up the next buffer as first page of next segment */
		/* Note: AdvanceXLInsertBuffer cannot need to do I/O here */
		(void) AdvanceXLInsertBuffer(true);

		/* There should be no unwritten data */
		curridx = Insert->curridx;
		Assert(curridx == Write->curridx);

		/* Compute end address of old segment */
		OldSegEnd = XLogCtl->xlblocks[curridx];
		OldSegEnd.xrecoff -= XLOG_BLCKSZ;
		if (OldSegEnd.xrecoff == 0)
		{
			/* crossing a logid boundary */
			OldSegEnd.xlogid -= 1;
			OldSegEnd.xrecoff = XLogFileSize;
		}

		/* Make it look like we've written and synced all of old segment */
		LogwrtResult.Write = OldSegEnd;
		LogwrtResult.Flush = OldSegEnd;

		/*
		 * Update shared-memory status --- this code should match XLogWrite
		 */
		{
			/* use volatile pointer to prevent code rearrangement */
			volatile XLogCtlData *xlogctl = XLogCtl;

			SpinLockAcquire(&xlogctl->info_lck);
			xlogctl->LogwrtResult = LogwrtResult;
			if (XLByteLT(xlogctl->LogwrtRqst.Write, LogwrtResult.Write))
				xlogctl->LogwrtRqst.Write = LogwrtResult.Write;
			if (XLByteLT(xlogctl->LogwrtRqst.Flush, LogwrtResult.Flush))
				xlogctl->LogwrtRqst.Flush = LogwrtResult.Flush;
			SpinLockRelease(&xlogctl->info_lck);
		}

		Write->LogwrtResult = LogwrtResult;

		LWLockRelease(WALWriteLock);

		updrqst = false;		/* done already */
	}
	else
	{
		/* normal case, ie not xlog switch */

		/* Need to update shared LogwrtRqst if some block was filled up */
		if (freespace < SizeOfXLogRecord)
		{
			/* curridx is filled and available for writing out */
			updrqst = true;
		}
		else
		{
			/* if updrqst already set, write through end of previous buf */
			curridx = PrevBufIdx(curridx);
		}
		WriteRqst = XLogCtl->xlblocks[curridx];
	}

	LWLockRelease(WALInsertLock);

	if (updrqst)
	{
		/* use volatile pointer to prevent code rearrangement */
		volatile XLogCtlData *xlogctl = XLogCtl;

		SpinLockAcquire(&xlogctl->info_lck);
		/* advance global request to include new block(s) */
		if (XLByteLT(xlogctl->LogwrtRqst.Write, WriteRqst))
			xlogctl->LogwrtRqst.Write = WriteRqst;
		/* update local result copy while I have the chance */
		LogwrtResult = xlogctl->LogwrtResult;
		SpinLockRelease(&xlogctl->info_lck);
	}

	ProcLastRecEnd = RecPtr;

	END_CRIT_SECTION();

	return RecPtr;
}

XLogRecPtr
XLogLastInsertBeginLoc(void)
{
	return ProcLastRecPtr;
}

XLogRecPtr
XLogLastInsertEndLoc(void)
{
	return ProcLastRecEnd;
}

XLogRecPtr
XLogLastChangeTrackedLoc(void)
{
	return XLogCtl->lastChangeTrackingEndLoc;
}

uint32
XLogLastInsertTotalLen(void)
{
	return ProcLastRecTotalLen;
}

uint32
XLogLastInsertDataLen(void)
{
	return ProcLastRecDataLen;
}

/*
 * Determine whether the buffer referenced by an XLogRecData item has to
 * be backed up, and if so fill a BkpBlock struct for it.  In any case
 * save the buffer's LSN at *lsn.
 */
static bool
XLogCheckBuffer(XLogRecData *rdata, bool doPageWrites,
				XLogRecPtr *lsn, BkpBlock *bkpb)
{
	PageHeader	page;

	page = (PageHeader) BufferGetBlock(rdata->buffer);

	/*
	 * XXX We assume page LSN is first data on *every* page that can be passed
	 * to XLogInsert, whether it otherwise has the standard page layout or
	 * not.
	 */
	*lsn = page->pd_lsn;

	if (doPageWrites &&
		XLByteLE(page->pd_lsn, RedoRecPtr))
	{
		/*
		 * The page needs to be backed up, so set up *bkpb
		 */
		bkpb->node = BufferGetFileNode(rdata->buffer);
		bkpb->block = BufferGetBlockNumber(rdata->buffer);

		if (rdata->buffer_std)
		{
			/* Assume we can omit data between pd_lower and pd_upper */
			uint16		lower = page->pd_lower;
			uint16		upper = page->pd_upper;

			if (lower >= SizeOfPageHeaderData &&
				upper > lower &&
				upper <= BLCKSZ)
			{
				bkpb->hole_offset = lower;
				bkpb->hole_length = upper - lower;
			}
			else
			{
				/* No "hole" to compress out */
				bkpb->hole_offset = 0;
				bkpb->hole_length = 0;
			}
		}
		else
		{
			/* Not a standard page header, don't try to eliminate "hole" */
			bkpb->hole_offset = 0;
			bkpb->hole_length = 0;
		}

		return true;			/* buffer requires backup */
	}

	return false;				/* buffer does not need to be backed up */
}

/*
 * XLogArchiveNotify
 *
 * Create an archive notification file
 *
 * The name of the notification file is the message that will be picked up
 * by the archiver, e.g. we write 0000000100000001000000C6.ready
 * and the archiver then knows to archive XLOGDIR/0000000100000001000000C6,
 * then when complete, rename it to 0000000100000001000000C6.done
 */
static void
XLogArchiveNotify(const char *xlog)
{
	char		archiveStatusPath[MAXPGPATH];
	FILE	   *fd;

	/* insert an otherwise empty file called <XLOG>.ready */
	StatusFilePath(archiveStatusPath, xlog, ".ready");
	fd = AllocateFile(archiveStatusPath, "w");
	if (fd == NULL)
	{
		ereport(LOG,
				(errcode_for_file_access(),
				 errmsg("could not create archive status file \"%s\": %m",
						archiveStatusPath)));
		return;
	}
	if (FreeFile(fd))
	{
		ereport(LOG,
				(errcode_for_file_access(),
				 errmsg("could not write archive status file \"%s\": %m",
						archiveStatusPath)));
		return;
	}

	/* Notify archiver that it's got something to do */
	if (IsUnderPostmaster)
		SendPostmasterSignal(PMSIGNAL_WAKEN_ARCHIVER);
}

/*
 * Convenience routine to notify using log/seg representation of filename
 */
static void
XLogArchiveNotifySeg(uint32 log, uint32 seg)
{
	char		xlog[MAXFNAMELEN];

	XLogFileName(xlog, ThisTimeLineID, log, seg);
	XLogArchiveNotify(xlog);
}

/*
 * XLogArchiveCheckDone
 *
 * This is called when we are ready to delete or recycle an old XLOG segment
 * file or backup history file.  If it is okay to delete it then return true.
 * If it is not time to delete it, make sure a .ready file exists, and return
 * false.
 *
 * If <XLOG>.done exists, then return true; else if <XLOG>.ready exists,
 * then return false; else create <XLOG>.ready and return false.
 *
 * The reason we do things this way is so that if the original attempt to
 * create <XLOG>.ready fails, we'll retry during subsequent checkpoints.
 */
static bool
XLogArchiveCheckDone(const char *xlog)
{
	char		archiveStatusPath[MAXPGPATH];
	struct stat stat_buf;

	/* Always deletable if archiving is off */
	if (!XLogArchivingActive())
		return true;

	/* First check for .done --- this means archiver is done with it */
	StatusFilePath(archiveStatusPath, xlog, ".done");
	if (stat(archiveStatusPath, &stat_buf) == 0)
		return true;

	/* check for .ready --- this means archiver is still busy with it */
	StatusFilePath(archiveStatusPath, xlog, ".ready");
	if (stat(archiveStatusPath, &stat_buf) == 0)
		return false;

	/* Race condition --- maybe archiver just finished, so recheck */
	StatusFilePath(archiveStatusPath, xlog, ".done");
	if (stat(archiveStatusPath, &stat_buf) == 0)
		return true;

	/* Retry creation of the .ready file */
	XLogArchiveNotify(xlog);
	return false;
}

/*
 * XLogArchiveCleanup
 *
 * Cleanup archive notification file(s) for a particular xlog segment
 */
static void
XLogArchiveCleanup(const char *xlog)
{
	char		archiveStatusPath[MAXPGPATH];

	/* Remove the .done file */
	StatusFilePath(archiveStatusPath, xlog, ".done");
	unlink(archiveStatusPath);
	/* should we complain about failure? */

	/* Remove the .ready file if present --- normally it shouldn't be */
	StatusFilePath(archiveStatusPath, xlog, ".ready");
	unlink(archiveStatusPath);
	/* should we complain about failure? */
}

/*
 * Advance the Insert state to the next buffer page, writing out the next
 * buffer if it still contains unwritten data.
 *
 * If new_segment is TRUE then we set up the next buffer page as the first
 * page of the next xlog segment file, possibly but not usually the next
 * consecutive file page.
 *
 * The global LogwrtRqst.Write pointer needs to be advanced to include the
 * just-filled page.  If we can do this for free (without an extra lock),
 * we do so here.  Otherwise the caller must do it.  We return TRUE if the
 * request update still needs to be done, FALSE if we did it internally.
 *
 * Must be called with WALInsertLock held.
 */
static bool
AdvanceXLInsertBuffer(bool new_segment)
{
	XLogCtlInsert *Insert = &XLogCtl->Insert;
	XLogCtlWrite *Write = &XLogCtl->Write;
	int			nextidx = NextBufIdx(Insert->curridx);
	bool		update_needed = true;
	XLogRecPtr	OldPageRqstPtr;
	XLogwrtRqst WriteRqst;
	XLogRecPtr	NewPageEndPtr;
	XLogPageHeader NewPage;

	/* Use Insert->LogwrtResult copy if it's more fresh */
	if (XLByteLT(LogwrtResult.Write, Insert->LogwrtResult.Write))
		LogwrtResult = Insert->LogwrtResult;

	/*
	 * Get ending-offset of the buffer page we need to replace (this may be
	 * zero if the buffer hasn't been used yet).  Fall through if it's already
	 * written out.
	 */
	OldPageRqstPtr = XLogCtl->xlblocks[nextidx];
	if (!XLByteLE(OldPageRqstPtr, LogwrtResult.Write))
	{
		/* nope, got work to do... */
		XLogRecPtr	FinishedPageRqstPtr;

		FinishedPageRqstPtr = XLogCtl->xlblocks[Insert->curridx];

		/* Before waiting, get info_lck and update LogwrtResult */
		{
			/* use volatile pointer to prevent code rearrangement */
			volatile XLogCtlData *xlogctl = XLogCtl;

			SpinLockAcquire(&xlogctl->info_lck);
			if (XLByteLT(xlogctl->LogwrtRqst.Write, FinishedPageRqstPtr))
				xlogctl->LogwrtRqst.Write = FinishedPageRqstPtr;
			LogwrtResult = xlogctl->LogwrtResult;
			SpinLockRelease(&xlogctl->info_lck);
		}

		update_needed = false;	/* Did the shared-request update */

		if (XLByteLE(OldPageRqstPtr, LogwrtResult.Write))
		{
			/* OK, someone wrote it already */
			Insert->LogwrtResult = LogwrtResult;
		}
		else
		{
			/* Must acquire write lock */
			LWLockAcquire(WALWriteLock, LW_EXCLUSIVE);
			LogwrtResult = Write->LogwrtResult;
			if (XLByteLE(OldPageRqstPtr, LogwrtResult.Write))
			{
				/* OK, someone wrote it already */
				LWLockRelease(WALWriteLock);
				Insert->LogwrtResult = LogwrtResult;
			}
			else
			{
				/*
				 * Have to write buffers while holding insert lock. This is
				 * not good, so only write as much as we absolutely must.
				 */
				WriteRqst.Write = OldPageRqstPtr;
				WriteRqst.Flush.xlogid = 0;
				WriteRqst.Flush.xrecoff = 0;
				XLogWrite(WriteRqst, false, false);
				LWLockRelease(WALWriteLock);
				Insert->LogwrtResult = LogwrtResult;
			}
		}
	}

	/*
	 * Now the next buffer slot is free and we can set it up to be the next
	 * output page.
	 */
	NewPageEndPtr = XLogCtl->xlblocks[Insert->curridx];

	if (new_segment)
	{
		/* force it to a segment start point */
		NewPageEndPtr.xrecoff += XLogSegSize - 1;
		NewPageEndPtr.xrecoff -= NewPageEndPtr.xrecoff % XLogSegSize;
	}

	if (NewPageEndPtr.xrecoff >= XLogFileSize)
	{
		/* crossing a logid boundary */
		NewPageEndPtr.xlogid += 1;
		NewPageEndPtr.xrecoff = XLOG_BLCKSZ;
	}
	else
		NewPageEndPtr.xrecoff += XLOG_BLCKSZ;
	XLogCtl->xlblocks[nextidx] = NewPageEndPtr;
	NewPage = (XLogPageHeader) (XLogCtl->pages + nextidx * (Size) XLOG_BLCKSZ);

	Insert->curridx = nextidx;
	Insert->currpage = NewPage;

	Insert->currpos = ((char *) NewPage) +SizeOfXLogShortPHD;

	/*
	 * Be sure to re-zero the buffer so that bytes beyond what we've written
	 * will look like zeroes and not valid XLOG records...
	 */
	MemSet((char *) NewPage, 0, XLOG_BLCKSZ);

	/*
	 * Fill the new page's header
	 */
	NewPage   ->xlp_magic = XLOG_PAGE_MAGIC;

	/* NewPage->xlp_info = 0; */	/* done by memset */
	NewPage   ->xlp_tli = ThisTimeLineID;
	NewPage   ->xlp_pageaddr.xlogid = NewPageEndPtr.xlogid;
	NewPage   ->xlp_pageaddr.xrecoff = NewPageEndPtr.xrecoff - XLOG_BLCKSZ;

	/*
	 * If first page of an XLOG segment file, make it a long header.
	 */
	if ((NewPage->xlp_pageaddr.xrecoff % XLogSegSize) == 0)
	{
		XLogLongPageHeader NewLongPage = (XLogLongPageHeader) NewPage;

		NewLongPage->xlp_sysid = ControlFile->system_identifier;
		NewLongPage->xlp_seg_size = XLogSegSize;
		NewLongPage->xlp_xlog_blcksz = XLOG_BLCKSZ;
		NewPage   ->xlp_info |= XLP_LONG_HEADER;

		Insert->currpos = ((char *) NewPage) +SizeOfXLogLongPHD;
	}

	return update_needed;
}

void XLogGetBuffer(int startidx, int npages, char **from, Size *nbytes)
{
	*from = XLogCtl->pages + startidx * (Size) XLOG_BLCKSZ;
	*nbytes = npages * (Size) XLOG_BLCKSZ;
}

static bool
XLogCatchupQDSegment(uint32 logId, uint32 seg, uint32 offset, uint32 end, struct timeval *timeout, bool *shutdownGlobal)
{
	char		fname[MAXPGPATH];
	char		path[MAXPGPATH];
	int			fd;
	int			seekEnd;
	char		buf[XLOG_BLCKSZ];	// UNDONE: Any issues with having this as a local?
	int			remainingLen;
	int			requestLen;
	int			readLen;
	char 		cmd[MAXFNAMELEN + 30];
	bool		successful;
	char		*xlogDir = NULL;

	elog((Debug_print_qd_mirroring ? LOG : DEBUG1), "catch-up segment logId %u, seg %u, offset 0x%X, end 0x%X",
		 logId, seg, offset, end);
	Assert(offset < end);

	XLogFileName(fname, ThisTimeLineID, logId, seg);
	elog((Debug_print_qd_mirroring ? LOG : DEBUG1),"formed '%s' for time-line %u, log %u, seg %u",
		 fname, ThisTimeLineID, logId, seg);

	xlogDir = makeRelativeToTxnFilespace(XLOGDIR);
    	if (snprintf(path, MAXPGPATH, "%s/%s", xlogDir, fname) >= MAXPGPATH)
	{
		ereport(ERROR, (errmsg("could not generate xlog filename (%s/%s)", xlogDir, fname)));
	}
	pfree(xlogDir);

	fd = BasicOpenFile(path, O_RDONLY | PG_BINARY, 0);
	if (fd < 0)
	{
		if (errno == ENOENT)
			ereport(ERROR,
					(errcode_for_file_access(),
			   errmsg("could not open transaction log file for catch-up purposes -- it has been reclaimed (\"%s\")",
					  path)));
		else
			ereport(ERROR,
					(errcode_for_file_access(),
			   errmsg("could not open transaction log file for catch-up purposes (\"%s\"): %m",
					  path)));
	}

	if ((int32)end == -1)
	{
		/*
		 * Determine the end.
		 */
		seekEnd = lseek(fd, 0, SEEK_END);
		if (seekEnd < 0)
			ereport(ERROR,
					(errcode_for_file_access(),
			   errmsg("could not seek to end of file \"%s\" (log file %u, segment %u): %m",
					  path, logId, seg)));
		end = seekEnd;
		elog((Debug_print_qd_mirroring ? LOG : DEBUG1),"determined end of \"%s\" to be 0x%X",
			 path, end);
	}

	if (lseek(fd, (off_t) offset, SEEK_SET) < 0)
		ereport(ERROR,
				(errcode_for_file_access(),
		   errmsg("could not seek to offset %u file \"%s\" (log file %u, segment %u): %m",
				  offset, path, logId, seg)));
	elog((Debug_print_qd_mirroring ? LOG : DEBUG1),"seek to offset 0x%X in file \"%s\"",
		 offset, path);

	while(true)
	{
		remainingLen = end - offset;
		if (remainingLen == 0)
			break;

		if (remainingLen < XLOG_BLCKSZ)
			requestLen = end - offset;
		else
			requestLen = XLOG_BLCKSZ;

		readLen = read(fd, buf, requestLen);
        if (readLen == -1)
        {
			ereport(ERROR,
					(errcode_for_file_access(),
			   errmsg("could not read at offset %u file \"%s\" (log file %u, segment %u): %m",
					  offset, path, logId, seg)));
        }
		if (readLen != requestLen)
		{
			elog(ERROR,"XLogCatchupQDSegment: short read (request %d, actual %d)",
				 requestLen, readLen);
		}

		elog((Debug_print_qd_mirroring ? LOG : DEBUG1), "succesfully read buffer at 0x%X for length 0x%X",
			 offset, readLen);

        if (snprintf(cmd, sizeof(cmd), "xlog %d %d %d %d",
			         logId, seg, offset, requestLen) >= sizeof(cmd))
		{
            elog(ERROR, "XLogCatchupQDSegment: error formatting command to mirror 'xlog %d %d %d %d' (insufficient space)", logId, seg, offset, requestLen);
		}

        successful = write_qd_sync(cmd, buf, requestLen, timeout, shutdownGlobal);
		if (!successful)
			return false;

		offset += requestLen;
	}

	close(fd);

	return true;
}

/*
 * Send primary XLOG to standby master to catch it up.
 */
bool
XLogCatchupQDMirror(XLogRecPtr *standbyLocation, XLogRecPtr *flushedLocation, struct timeval *timeout, bool *shutdownGlobal)
{
	uint32		standbyLogId;
	uint32		standbySeg;
	uint32		flushedLogId;
	uint32		flushedSeg;
	uint32		logId;
	uint32		seg;
	uint32		offset;
	uint32		end;
	bool        successful;

	Assert(XLByteLT(*standbyLocation, *flushedLocation));
	XLByteToSeg(*standbyLocation, standbyLogId, standbySeg);
	XLByteToSeg(*flushedLocation, flushedLogId, flushedSeg);

	logId = standbyLogId;
	seg = standbySeg;
	while(true)
	{
		if (logId == standbyLogId && seg == standbySeg)
		{
			offset = standbyLocation->xrecoff % XLogSegSize;

			/* Round down to block */
			offset = (offset / XLOG_BLCKSZ) * XLOG_BLCKSZ;
		}
		else
			offset = 0;

		if (logId == flushedLogId && seg == flushedSeg)
			end = flushedLocation->xrecoff % XLogSegSize;
		else
			end = -1;

		successful = XLogCatchupQDSegment(logId, seg, offset, end, timeout, shutdownGlobal);
		if (!successful)
			return false;

		if (logId == flushedLogId && seg == flushedSeg)
			break;

		NextLogSeg(logId, seg);
		elog((Debug_print_qd_mirroring ? LOG : DEBUG1), "next logId %u seg %u", logId, seg);
	}

	return true;
}

bool
XLogQDMirrorPositionToEnd(void)
{
	WalSendRequest walSendRequest;
	bool successful;

	MemSet(&walSendRequest, 0, sizeof(walSendRequest));
	walSendRequest.command = PositionToEnd;

	successful = WalSendServerClientSendRequest(&walSendRequest);

	if (successful)
	{
		/*
		 * Infinite timeout since we do not know how long it will
		 * take to determine the end position.
		 */
		successful = XLogQDMirrorWaitForResponse(/* waitForever */ true);
	}

	return successful;
}

static bool
XLogQDMirrorWaitForResponse(bool waitForever)
{
	WalSendResponse walSendResponse;
	bool receiveSuccessful;
	struct timeval timeout;

	if (!waitForever)
		WalSendServerGetClientTimeout(&timeout);

	receiveSuccessful =
		WalSendServerClientReceiveResponse(
					&walSendResponse,
					(waitForever ? NULL : &timeout));

	if (receiveSuccessful && !walSendResponse.ok)
		elog(ERROR,"response returned not OK");

	return receiveSuccessful;
}

bool
XLogQDMirrorCatchup(XLogRecPtr *flushedLocation)
{
	WalSendRequest walSendRequest;
	bool connected;
	bool successful;

	Assert(flushedLocation != NULL);

	MemSet(&walSendRequest, 0, sizeof(walSendRequest));
	walSendRequest.command = Catchup;
	walSendRequest.flushedLocation = *flushedLocation;

	connected = WalSendServerClientConnect(/* complain */ true);
	if (!connected)
	{
		elog(LOG, "disabling master mirroring (cannot connect to WAL Send server)");

		return false;
	}

	successful = WalSendServerClientSendRequest(&walSendRequest);

	if (successful)
	{
		/*
		 * Infinite timeout since we do not know how long it will
		 * take to catch-up.
		 */
		successful = XLogQDMirrorWaitForResponse(/* waitForever */ true);
	}

	return successful;
}

static void
XLogQDMirrorWrite(int startidx, int npages, TimeLineID timeLineID, uint32 logId, uint32 logSeg, uint32 logOff)
{
	bool connected;
	WalSendRequest walSendRequest;
	bool successful;

	elog((Debug_print_qd_mirroring ? LOG : DEBUG1),"Master mirroring WriteWalPages parameters startidx %d, npages %d, timeLineID %d, logId %u, logSeg %u, logOff %X",
		 startidx, npages, timeLineID, logId, logSeg, logOff);

	if (!QDMirroringWriteCheck())
	{
		if (Debug_print_qd_mirroring)
			elog(LOG,"Skipping master mirroring -- not enabled");
		return;
	}
	connected = WalSendServerClientConnect(/* complain */ true);
	if (!connected)
	{
		disableQDMirroring_WalSendServerError("Cannot connect to the WAL Send server");
		return;
	}

	memset(&walSendRequest, 0, sizeof(walSendRequest));
	walSendRequest.command = WriteWalPages;
	walSendRequest.startidx = startidx;
	walSendRequest.npages = npages;
	walSendRequest.timeLineID = timeLineID;
	walSendRequest.logId = logId;
	walSendRequest.logSeg = logSeg;
	walSendRequest.logOff = logOff;

	successful = WalSendServerClientSendRequest(&walSendRequest);

	if (!successful)
	{
		disableQDMirroring_WalSendServerError("Error occurred sending the WriteWalPages request to the WAL Send server");
		return;
	}

	successful = XLogQDMirrorWaitForResponse(/* waitForever */ false);

	if (!successful)
	{
		disableQDMirroring_WalSendServerError("Error occurred receiving the WriteWalPages response from the WAL Send server");
		return;
	}
}

static void
XLogQDMirrorFlush(void)
{
	bool ready;
	bool connected;
	bool haveNewCheckpointLocation;
	XLogRecPtr newCheckpointLocation;
	WalSendRequest walSendRequest;
	bool successful;

	elog((Debug_print_qd_mirroring ? LOG : DEBUG1),"Master mirroring FlushWalPages");

	FtsCheckReadyToSend(&ready,
		                &haveNewCheckpointLocation, &newCheckpointLocation);
	if (!ready)
	{
		if (Debug_print_qd_mirroring)
			elog(LOG,"Skipping master mirroring -- not enabled");
		return;
	}
	connected = WalSendServerClientConnect(/* complain */ true);
	if (!connected)
	{
		disableQDMirroring_WalSendServerError("Cannot connect to the WAL Send server");
		return;
	}

	memset(&walSendRequest, 0, sizeof(walSendRequest));
	walSendRequest.command = FlushWalPages;
	walSendRequest.haveNewCheckpointLocation = haveNewCheckpointLocation;
	walSendRequest.newCheckpointLocation = newCheckpointLocation;

	successful = WalSendServerClientSendRequest(&walSendRequest);

	if (!successful)
	{
		disableQDMirroring_WalSendServerError("Error occurred sending the FlushWalPages request to the WAL Send server");
		return;
	}

	successful = XLogQDMirrorWaitForResponse(/* waitForever */ false);

	if (!successful)
	{
		disableQDMirroring_WalSendServerError("Error occurred receiving the FlushWalPages response from the WAL Send server");
		return;
	}
}

static void
XLogQDMirrorCloseForShutdown(void)
{
	bool connected;
	WalSendRequest walSendRequest;
	bool successful;

	elog((Debug_print_qd_mirroring ? LOG : DEBUG1),"Master mirroring CloseForShutdown");

	if (isQDMirroringNotConfigured() ||
		isQDMirroringNotKnownYet())
	{
		if (Debug_print_qd_mirroring)
			elog(LOG,"Skipping master mirroring -- not configured or known");
		return;
	}
	if (isQDMirroringDisabled())
	{
		if (Debug_print_qd_mirroring)
			elog(LOG,"Skipping master mirroring -- disabled");
		return;
	}
	connected = WalSendServerClientConnect(/* complain */ false);
	if (!connected)
	{
		/* Ignore */
		elog((Debug_print_qd_mirroring ? LOG : DEBUG1),"cannot connect to WAL Send server");
		return;
	}

	memset(&walSendRequest, 0, sizeof(walSendRequest));
	walSendRequest.command = CloseForShutdown;

	successful = WalSendServerClientSendRequest(&walSendRequest);

	if (!successful)
	{
		/* Ignore */
		elog((Debug_print_qd_mirroring ? LOG : DEBUG1),"could not send to WAL Send server");
		return;
	}

	successful = XLogQDMirrorWaitForResponse(/* waitForever */ false);

	if (!successful)
	{
		/* Ignore */
		elog((Debug_print_qd_mirroring ? LOG : DEBUG1),"could not get response from WAL Send server");
		return;
	}

	elog((Debug_print_qd_mirroring ? LOG : DEBUG1),"Successful master mirroring CloseForShutdown");
}


/*
 * Write and/or fsync the log at least as far as WriteRqst indicates.
 *
 * If flexible == TRUE, we don't have to write as far as WriteRqst, but
 * may stop at any convenient boundary (such as a cache or logfile boundary).
 * This option allows us to avoid uselessly issuing multiple writes when a
 * single one would do.
 *
 * If xlog_switch == TRUE, we are intending an xlog segment switch, so
 * perform end-of-segment actions after writing the last page, even if
 * it's not physically the end of its segment.  (NB: this will work properly
 * only if caller specifies WriteRqst == page-end and flexible == false,
 * and there is some data to write.)
 *
 * Must be called with WALWriteLock held.
 */
static void
XLogWrite(XLogwrtRqst WriteRqst, bool flexible, bool xlog_switch)
{
	XLogCtlWrite *Write = &XLogCtl->Write;
	bool		ispartialpage;
	bool		last_iteration;
	bool		finishing_seg;
	bool		use_existent;
	int			curridx;
	int			npages;
	int			startidx;
	uint32		startoffset;

	/* We should always be inside a critical section here */
	Assert(CritSectionCount > 0);

	/*
	 * Update local LogwrtResult (caller probably did this already, but...)
	 */
	LogwrtResult = Write->LogwrtResult;

	/*
	 * Since successive pages in the xlog cache are consecutively allocated,
	 * we can usually gather multiple pages together and issue just one
	 * write() call.  npages is the number of pages we have determined can be
	 * written together; startidx is the cache block index of the first one,
	 * and startoffset is the file offset at which it should go. The latter
	 * two variables are only valid when npages > 0, but we must initialize
	 * all of them to keep the compiler quiet.
	 */
	npages = 0;
	startidx = 0;
	startoffset = 0;

	/*
	 * Within the loop, curridx is the cache block index of the page to
	 * consider writing.  We advance Write->curridx only after successfully
	 * writing pages.  (Right now, this refinement is useless since we are
	 * going to PANIC if any error occurs anyway; but someday it may come in
	 * useful.)
	 */
	curridx = Write->curridx;

	while (XLByteLT(LogwrtResult.Write, WriteRqst.Write))
	{
		/*
		 * Make sure we're not ahead of the insert process.  This could happen
		 * if we're passed a bogus WriteRqst.Write that is past the end of the
		 * last page that's been initialized by AdvanceXLInsertBuffer.
		 */
		if (!XLByteLT(LogwrtResult.Write, XLogCtl->xlblocks[curridx]))
			elog(PANIC, "xlog write request %X/%X is past end of log %X/%X",
				 LogwrtResult.Write.xlogid, LogwrtResult.Write.xrecoff,
				 XLogCtl->xlblocks[curridx].xlogid,
				 XLogCtl->xlblocks[curridx].xrecoff);

		/* Advance LogwrtResult.Write to end of current buffer page */
		LogwrtResult.Write = XLogCtl->xlblocks[curridx];
		ispartialpage = XLByteLT(WriteRqst.Write, LogwrtResult.Write);

		if (!XLByteInPrevSeg(LogwrtResult.Write, openLogId, openLogSeg))
		{
			/*
			 * Switch to new logfile segment.  We cannot have any pending
			 * pages here (since we dump what we have at segment end).
			 */
			Assert(npages == 0);
			if (MirroredFlatFile_IsActive(&mirroredLogFileOpen))
				XLogFileClose();
			XLByteToPrevSeg(LogwrtResult.Write, openLogId, openLogSeg);

			/* create/use new log file */
			use_existent = true;

			XLogFileInit(
					&mirroredLogFileOpen,
					openLogId, openLogSeg,
					&use_existent, true);
			openLogOff = 0;

			/* update pg_control, unless someone else already did */
			LWLockAcquire(ControlFileLock, LW_EXCLUSIVE);
			if (ControlFile->logId < openLogId ||
				(ControlFile->logId == openLogId &&
				 ControlFile->logSeg < openLogSeg + 1))
			{
				ControlFile->logId = openLogId;
				ControlFile->logSeg = openLogSeg + 1;
				ControlFile->time = time(NULL);
				UpdateControlFile();

				/*
				 * Signal bgwriter to start a checkpoint if it's been too long
				 * since the last one.	(We look at local copy of RedoRecPtr
				 * which might be a little out of date, but should be close
				 * enough for this purpose.)
				 *
				 * A straight computation of segment number could overflow 32
				 * bits.  Rather than assuming we have working 64-bit
				 * arithmetic, we compare the highest-order bits separately,
				 * and force a checkpoint immediately when they change.
				 */
				if (IsUnderPostmaster)
				{
					uint32		old_segno,
								new_segno;
					uint32		old_highbits,
								new_highbits;

					old_segno = (RedoRecPtr.xlogid % XLogSegSize) * XLogSegsPerFile +
						(RedoRecPtr.xrecoff / XLogSegSize);
					old_highbits = RedoRecPtr.xlogid / XLogSegSize;
					new_segno = (openLogId % XLogSegSize) * XLogSegsPerFile +
						openLogSeg;
					new_highbits = openLogId / XLogSegSize;
					if (new_highbits != old_highbits ||
						new_segno >= old_segno + (uint32) CheckPointSegments)
					{
#ifdef WAL_DEBUG
						if (XLOG_DEBUG)
							elog(LOG, "time for a checkpoint, signaling bgwriter");
#endif
						if (Debug_print_qd_mirroring)
							elog(LOG, "time for a checkpoint, signaling bgwriter");
						RequestCheckpoint(false, true);
					}
				}
			}
			LWLockRelease(ControlFileLock);
		}

		/* Make sure we have the current logfile open */
		if (!MirroredFlatFile_IsActive(&mirroredLogFileOpen))
		{
			XLByteToPrevSeg(LogwrtResult.Write, openLogId, openLogSeg);
			XLogFileOpen(
					&mirroredLogFileOpen,
					openLogId,
					openLogSeg);
			openLogOff = 0;
		}

		/* Add current page to the set of pending pages-to-dump */
		if (npages == 0)
		{
			/* first of group */
			startidx = curridx;
			startoffset = (LogwrtResult.Write.xrecoff - XLOG_BLCKSZ) % XLogSegSize;
		}
		npages++;

		/*
		 * Dump the set if this will be the last loop iteration, or if we are
		 * at the last page of the cache area (since the next page won't be
		 * contiguous in memory), or if we are at the end of the logfile
		 * segment.
		 */
		last_iteration = !XLByteLT(LogwrtResult.Write, WriteRqst.Write);

		finishing_seg = !ispartialpage &&
			(startoffset + npages * XLOG_BLCKSZ) >= XLogSegSize;

		if (last_iteration ||
			curridx == XLogCtl->XLogCacheBlck ||
			finishing_seg)
		{
			char	   *from;
			Size		nbytes;

			/* Need to seek in the file? */
			if (openLogOff != startoffset)
			{
				openLogOff = startoffset;
			}

			/* OK to write the page(s) */
			from = XLogCtl->pages + startidx * (Size) XLOG_BLCKSZ;
			nbytes = npages * (Size) XLOG_BLCKSZ;

			/* The following code is a sanity check to try to catch the issue described in MPP-12611 */
			if (!IsBootstrapProcessingMode())
			  {
			  char   simpleFileName[MAXPGPATH];
			  XLogFileName(simpleFileName, ThisTimeLineID, openLogId, openLogSeg);
                          if (strcmp(simpleFileName, mirroredLogFileOpen.simpleFileName) != 0)
			    {
			      ereport( PANIC
				       , (errmsg_internal("Expected Xlog file name does not match current open xlog file name. \
                                                           Expected file = %s, \
                                                           open file = %s, \
                                                           WriteRqst.Write = %s, \
                                                           WriteRqst.Flush = %s "
							 , simpleFileName
							 , mirroredLogFileOpen.simpleFileName
							 , XLogLocationToString(&(WriteRqst.Write))
							 , XLogLocationToString(&(WriteRqst.Flush)))));
			    }
			  }

			if (MirroredFlatFile_Write(
							&mirroredLogFileOpen,
							openLogOff,
							from,
							nbytes,
							/* suppressError */ true))
			{
				ereport(PANIC,
						(errcode_for_file_access(),
						 errmsg("could not write to log file %u, segment %u "
								"at offset %u, length %lu: %m",
								openLogId, openLogSeg,
								openLogOff, (unsigned long) nbytes)));
			}

			/*
			 * Send information about the write to the WAL Send server.
			 * For now, it will copy the directly out of the cache.
			 * Later, we will tell the WAL Send server to actually send
			 * the information to standby as part of flush.
	         */
			XLogQDMirrorWrite(
						startidx,
						npages,
						ThisTimeLineID,
						openLogId,
						openLogSeg,
						openLogOff);

			/* Update state for write */
			openLogOff += nbytes;
			Write->curridx = ispartialpage ? curridx : NextBufIdx(curridx);
			npages = 0;

			/*
			 * If we just wrote the whole last page of a logfile segment,
			 * fsync the segment immediately.  This avoids having to go back
			 * and re-open prior segments when an fsync request comes along
			 * later. Doing it here ensures that one and only one backend will
			 * perform this fsync.
			 *
			 * We also do this if this is the last page written for an xlog
			 * switch.
			 *
			 * This is also the right place to notify the Archiver that the
			 * segment is ready to copy to archival storage, and to update the
			 * timer for archive_timeout.
			 */
			if (finishing_seg || (xlog_switch && last_iteration))
			{
				if (MirroredFlatFile_IsActive(&mirroredLogFileOpen))
					MirroredFlatFile_Flush(
									&mirroredLogFileOpen,
									/* suppressError */ false);

				elog((Debug_print_qd_mirroring ? LOG : DEBUG5),
					 "XLogWrite (#1): flush loc %s; write loc %s",
					 XLogLocationToString_Long(&LogwrtResult.Flush),
					 XLogLocationToString2_Long(&LogwrtResult.Write));
				XLogQDMirrorFlush();

				LogwrtResult.Flush = LogwrtResult.Write;		/* end of page */

				if (XLogArchivingActive())
					XLogArchiveNotifySeg(openLogId, openLogSeg);

				Write->lastSegSwitchTime = time(NULL);
			}
		}

		if (ispartialpage)
		{
			/* Only asked to write a partial page */
			LogwrtResult.Write = WriteRqst.Write;
			break;
		}
		curridx = NextBufIdx(curridx);

		/* If flexible, break out of loop as soon as we wrote something */
		if (flexible && npages == 0)
			break;
	}

	Assert(npages == 0);
	Assert(curridx == Write->curridx);

	/*
	 * If asked to flush, do so
	 */
	if (XLByteLT(LogwrtResult.Flush, WriteRqst.Flush) &&
		XLByteLT(LogwrtResult.Flush, LogwrtResult.Write))
	{
		/*
		 * Could get here without iterating above loop, in which case we might
		 * have no open file or the wrong one.	However, we do not need to
		 * fsync more than one file.
		 */
		if (sync_method != SYNC_METHOD_OPEN)
		{
			if (MirroredFlatFile_IsActive(&mirroredLogFileOpen) &&
				!XLByteInPrevSeg(LogwrtResult.Write, openLogId, openLogSeg))
				XLogFileClose();
			if (!MirroredFlatFile_IsActive(&mirroredLogFileOpen))
			{
				XLByteToPrevSeg(LogwrtResult.Write, openLogId, openLogSeg);
				XLogFileOpen(
						&mirroredLogFileOpen,
						openLogId,
						openLogSeg);
				openLogOff = 0;
			}
			if (MirroredFlatFile_IsActive(&mirroredLogFileOpen))
				MirroredFlatFile_Flush(
								&mirroredLogFileOpen,
								/* suppressError */ false);

			elog((Debug_print_qd_mirroring ? LOG : DEBUG5),
				 "XLogWrite (#2): flush loc %s; write loc %s",
				 XLogLocationToString_Long(&LogwrtResult.Flush),
				 XLogLocationToString2_Long(&LogwrtResult.Write));

			XLogQDMirrorFlush();
		}

		LogwrtResult.Flush = LogwrtResult.Write;
	}

	/*
	 * Update shared-memory status
	 *
	 * We make sure that the shared 'request' values do not fall behind the
	 * 'result' values.  This is not absolutely essential, but it saves some
	 * code in a couple of places.
	 */
	{
		/* use volatile pointer to prevent code rearrangement */
		volatile XLogCtlData *xlogctl = XLogCtl;

		SpinLockAcquire(&xlogctl->info_lck);
		xlogctl->LogwrtResult = LogwrtResult;
		if (XLByteLT(xlogctl->LogwrtRqst.Write, LogwrtResult.Write))
			xlogctl->LogwrtRqst.Write = LogwrtResult.Write;
		if (XLByteLT(xlogctl->LogwrtRqst.Flush, LogwrtResult.Flush))
			xlogctl->LogwrtRqst.Flush = LogwrtResult.Flush;
		SpinLockRelease(&xlogctl->info_lck);
	}

	Write->LogwrtResult = LogwrtResult;
}

/*
 * Ensure that all XLOG data through the given position is flushed to disk.
 *
 * NOTE: this differs from XLogWrite mainly in that the WALWriteLock is not
 * already held, and we try to avoid acquiring it if possible.
 */
void
XLogFlush(XLogRecPtr record)
{
	XLogRecPtr	WriteRqstPtr;
	XLogwrtRqst WriteRqst;

	/* Disabled during REDO */
	if (InRedo)
		return;

	if (Debug_print_qd_mirroring)
		elog(LOG, "xlog flush request %s; write %s; flush %s",
			 XLogLocationToString(&record),
			 XLogLocationToString2(&LogwrtResult.Write),
			 XLogLocationToString3(&LogwrtResult.Flush));

	/* Quick exit if already known flushed */
	if (XLByteLE(record, LogwrtResult.Flush))
		return;

#ifdef WAL_DEBUG
	if (XLOG_DEBUG)
		elog(LOG, "xlog flush request %X/%X; write %X/%X; flush %X/%X",
			 record.xlogid, record.xrecoff,
			 LogwrtResult.Write.xlogid, LogwrtResult.Write.xrecoff,
			 LogwrtResult.Flush.xlogid, LogwrtResult.Flush.xrecoff);
#endif

	START_CRIT_SECTION();

	/*
	 * Since fsync is usually a horribly expensive operation, we try to
	 * piggyback as much data as we can on each fsync: if we see any more data
	 * entered into the xlog buffer, we'll write and fsync that too, so that
	 * the final value of LogwrtResult.Flush is as large as possible. This
	 * gives us some chance of avoiding another fsync immediately after.
	 */

	/* initialize to given target; may increase below */
	WriteRqstPtr = record;

	/* read LogwrtResult and update local state */
	{
		/* use volatile pointer to prevent code rearrangement */
		volatile XLogCtlData *xlogctl = XLogCtl;

		SpinLockAcquire(&xlogctl->info_lck);
		if (XLByteLT(WriteRqstPtr, xlogctl->LogwrtRqst.Write))
			WriteRqstPtr = xlogctl->LogwrtRqst.Write;
		LogwrtResult = xlogctl->LogwrtResult;
		SpinLockRelease(&xlogctl->info_lck);
	}

	/* done already? */
	if (!XLByteLE(record, LogwrtResult.Flush))
	{
		/* now wait for the write lock */
		LWLockAcquire(WALWriteLock, LW_EXCLUSIVE);
		LogwrtResult = XLogCtl->Write.LogwrtResult;
		if (!XLByteLE(record, LogwrtResult.Flush))
		{
			/* try to write/flush later additions to XLOG as well */
			if (LWLockConditionalAcquire(WALInsertLock, LW_EXCLUSIVE))
			{
				XLogCtlInsert *Insert = &XLogCtl->Insert;
				uint32		freespace = INSERT_FREESPACE(Insert);

				if (freespace < SizeOfXLogRecord)		/* buffer is full */
					WriteRqstPtr = XLogCtl->xlblocks[Insert->curridx];
				else
				{
					WriteRqstPtr = XLogCtl->xlblocks[Insert->curridx];
					WriteRqstPtr.xrecoff -= freespace;
				}
				LWLockRelease(WALInsertLock);
				WriteRqst.Write = WriteRqstPtr;
				WriteRqst.Flush = WriteRqstPtr;
			}
			else
			{
				WriteRqst.Write = WriteRqstPtr;
				WriteRqst.Flush = record;
			}
			XLogWrite(WriteRqst, false, false);

			/*
			 * 1) We are holding the write lock which prevents other calls
			 *    to XLogWrite -- which sends requests to the
			 *    WAL Send server.
			 *
			 * 2) We just called XLogWrite requesting it to flush.
			 *
			 * So, this seems to be a good point to synchronize
			 * the QD Mirror.
			 */
			if (isQDMirroringPendingCatchup())
			{
				elog((Debug_print_qd_mirroring ? LOG : DEBUG5),
					 "synchronize rendezvous (write loc %s, flushed loc %s)",
					 XLogLocationToString_Long(&LogwrtResult.Write),
					 XLogLocationToString2_Long(&LogwrtResult.Flush));

				FtsQDMirroringCatchup(&LogwrtResult.Flush);
			}

		}
		LWLockRelease(WALWriteLock);
	}

	END_CRIT_SECTION();

	/*
	 * If we still haven't flushed to the request point then we have a
	 * problem; most likely, the requested flush point is past end of XLOG.
	 * This has been seen to occur when a disk page has a corrupted LSN.
	 *
	 * Formerly we treated this as a PANIC condition, but that hurts the
	 * system's robustness rather than helping it: we do not want to take down
	 * the whole system due to corruption on one data page.  In particular, if
	 * the bad page is encountered again during recovery then we would be
	 * unable to restart the database at all!  (This scenario has actually
	 * happened in the field several times with 7.1 releases. Note that we
	 * cannot get here while InRedo is true, but if the bad page is brought in
	 * and marked dirty during recovery then CreateCheckPoint will try to
	 * flush it at the end of recovery.)
	 *
	 * The current approach is to ERROR under normal conditions, but only
	 * WARNING during recovery, so that the system can be brought up even if
	 * there's a corrupt LSN.  Note that for calls from xact.c, the ERROR will
	 * be promoted to PANIC since xact.c calls this routine inside a critical
	 * section.  However, calls from bufmgr.c are not within critical sections
	 * and so we will not force a restart for a bad LSN on a data page.
	 */
	if (XLByteLT(LogwrtResult.Flush, record))
		elog(InRecovery ? WARNING : ERROR,
		"xlog flush request %X/%X is not satisfied --- flushed only to %X/%X",
			 record.xlogid, record.xrecoff,
			 LogwrtResult.Flush.xlogid, LogwrtResult.Flush.xrecoff);
}

/*
 * Create a new XLOG file segment, or open a pre-existing one.
 *
 * log, seg: identify segment to be created/opened.
 *
 * *use_existent: if TRUE, OK to use a pre-existing file (else, any
 * pre-existing file will be deleted).	On return, TRUE if a pre-existing
 * file was used.
 *
 * use_lock: if TRUE, acquire ControlFileLock while moving file into
 * place.  This should be TRUE except during bootstrap log creation.  The
 * caller must *not* hold the lock at call.
 *
 * Returns FD of opened file.
 *
 * Note: errors here are ERROR not PANIC because we might or might not be
 * inside a critical section (eg, during checkpoint there is no reason to
 * take down the system on failure).  They will promote to PANIC if we are
 * in a critical section.
 */
static void
XLogFileInit(
	MirroredFlatFileOpen *mirroredOpen,
	uint32 log, uint32 seg,
	bool *use_existent, bool use_lock)
{
	char		simpleFileName[MAXPGPATH];
	char		tmpsimple[MAXPGPATH];
	char		tmppath[MAXPGPATH];

	MirroredFlatFileOpen tmpMirroredOpen;

	char		zbuffer[XLOG_BLCKSZ];
	uint32		installed_log;
	uint32		installed_seg;
	int			max_advance;
	int			nbytes;
	char			*xlogDir = NULL;

	XLogFileName(simpleFileName, ThisTimeLineID, log, seg);

	/*
	 * Try to use existent file (checkpoint maker may have created it already)
	 */
	if (*use_existent)
	{
		if (MirroredFlatFile_Open(
							mirroredOpen,
							XLOGDIR,
							simpleFileName,
							O_RDWR | PG_BINARY | XLOG_SYNC_BIT,
						    S_IRUSR | S_IWUSR,
						    /* suppressError */ true,
							/* atomic operation */ false,
							/*isMirrorRecovery */ false))
		{
			char		path[MAXPGPATH];

			XLogFilePath(path, ThisTimeLineID, log, seg);

			if (errno != ENOENT)
				ereport(ERROR,
						(errcode_for_file_access(),
						 errmsg("could not open file \"%s\" (log file %u, segment %u): %m",
								path, log, seg)));
		}
		else
			return;
	}

	/*
	 * Initialize an empty (all zeroes) segment.  NOTE: it is possible that
	 * another process is doing the same thing.  If so, we will end up
	 * pre-creating an extra log segment.  That seems OK, and better than
	 * holding the lock throughout this lengthy process.
	 */
	xlogDir = makeRelativeToTxnFilespace(XLOGDIR);
		
	if (snprintf(tmpsimple, MAXPGPATH, "xlogtemp.%d", (int) getpid()) > MAXPGPATH)
	{
		ereport(ERROR, (errmsg("could not generate filename xlogtemp.%d", (int)getpid())));
        }

	if (snprintf(tmppath, MAXPGPATH, "%s/%s", xlogDir, tmpsimple) > MAXPGPATH)
	{
		ereport(ERROR, (errmsg("could not generate filename %s/%s", xlogDir, tmpsimple)));
        }


	MirroredFlatFile_Drop(
						  XLOGDIR,
						  tmpsimple,
						  /* suppressError */ true,
						  /*isMirrorRecovery */ false);

	/* do not use XLOG_SYNC_BIT here --- want to fsync only at end of fill */
	MirroredFlatFile_Open(
						&tmpMirroredOpen,
						XLOGDIR,
						tmpsimple,
						O_RDWR | O_CREAT | O_EXCL | PG_BINARY,
					    S_IRUSR | S_IWUSR,
					    /* suppressError */ false,
						/* atomic operation */ false,
						/*isMirrorRecovery */ false);

	/*
	 * Zero-fill the file.	We have to do this the hard way to ensure that all
	 * the file space has really been allocated --- on platforms that allow
	 * "holes" in files, just seeking to the end doesn't allocate intermediate
	 * space.  This way, we know that we have all the space and (after the
	 * fsync below) that all the indirect blocks are down on disk.	Therefore,
	 * fdatasync(2) or O_DSYNC will be sufficient to sync future writes to the
	 * log file.
	 */
	MemSet(zbuffer, 0, sizeof(zbuffer));
	for (nbytes = 0; nbytes < XLogSegSize; nbytes += sizeof(zbuffer))
	{
		errno = 0;
		if (MirroredFlatFile_Append(
							&tmpMirroredOpen,
							zbuffer,
							sizeof(zbuffer),
							/* suppressError */ true))
		{
			int			save_errno = errno;

			/*
			 * If we fail to make the file, delete it to release disk space
			 */
			MirroredFlatFile_Drop(
							XLOGDIR,
							tmpsimple,
							/* suppressError */ false,
							/*isMirrorRecovery */ false);

			/* if write didn't set errno, assume problem is no disk space */
			errno = save_errno ? save_errno : ENOSPC;

			ereport(ERROR,
					(errcode_for_file_access(),
					 errmsg("could not write to file \"%s\": %m", tmppath)));
		}
	}

	MirroredFlatFile_Flush(
				&tmpMirroredOpen,
				/* suppressError */ false);

	MirroredFlatFile_Close(&tmpMirroredOpen);

	/*
	 * Now move the segment into place with its final name.
	 *
	 * If caller didn't want to use a pre-existing file, get rid of any
	 * pre-existing file.  Otherwise, cope with possibility that someone else
	 * has created the file while we were filling ours: if so, use ours to
	 * pre-create a future log segment.
	 */
	installed_log = log;
	installed_seg = seg;
	max_advance = XLOGfileslop;
	if (!InstallXLogFileSegment(&installed_log, &installed_seg, tmppath,
								*use_existent, &max_advance,
								use_lock, tmpsimple))
	{
		/* No need for any more future segments... */
		MirroredFlatFile_Drop(
						XLOGDIR,
						tmpsimple,
						/* suppressError */ false,
						/*isMirrorRecovery */ false);
	}

	/* Set flag to tell caller there was no existent file */
	*use_existent = false;

	/* Now open original target segment (might not be file I just made) */
	MirroredFlatFile_Open(
						mirroredOpen,
						XLOGDIR,
						simpleFileName,
						O_RDWR | PG_BINARY | XLOG_SYNC_BIT,
					    S_IRUSR | S_IWUSR,
					    /* suppressError */ false,
						/* atomic operation */ false,
						/*isMirrorRecovery */ false);

	pfree(xlogDir);
}

/*
 * Create a new XLOG file segment by copying a pre-existing one.
 *
 * log, seg: identify segment to be created.
 *
 * srcTLI, srclog, srcseg: identify segment to be copied (could be from
 *		a different timeline)
 *
 * Currently this is only used during recovery, and so there are no locking
 * considerations.	But we should be just as tense as XLogFileInit to avoid
 * emplacing a bogus file.
 */
static void
XLogFileCopy(uint32 log, uint32 seg,
			 TimeLineID srcTLI, uint32 srclog, uint32 srcseg)
{
	char		path[MAXPGPATH];
	char		tmppath[MAXPGPATH];
	char		buffer[XLOG_BLCKSZ];
	int			srcfd;
	int			fd;
	int			nbytes;
	char		*xlogDir = NULL;

	/*
	 * Open the source file
	 */
	XLogFilePath(path, srcTLI, srclog, srcseg);
	srcfd = BasicOpenFile(path, O_RDONLY | PG_BINARY, 0);
	if (srcfd < 0)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not open file \"%s\": %m", path)));

	/*
	 * Copy into a temp file name.
	 */
	xlogDir = makeRelativeToTxnFilespace(XLOGDIR);
	if (snprintf(tmppath, MAXPGPATH, "%s/xlogtemp.%d", xlogDir, (int) getpid()))
	{
		ereport(ERROR, (errmsg("could not generate filename %s/xlogtemp.%d", xlogDir, (int) getpid())));
        }
	pfree(xlogDir);	
	unlink(tmppath);

	elog((Debug_print_qd_mirroring ? LOG : DEBUG5), "Master Mirroring: copying xlog file '%s' to '%s'",
		 path, tmppath);

	/* do not use XLOG_SYNC_BIT here --- want to fsync only at end of fill */
	fd = BasicOpenFile(tmppath, O_RDWR | O_CREAT | O_EXCL | PG_BINARY,
					   S_IRUSR | S_IWUSR);
	if (fd < 0)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not create file \"%s\": %m", tmppath)));

	/*
	 * Do the data copying.
	 */
	for (nbytes = 0; nbytes < XLogSegSize; nbytes += sizeof(buffer))
	{
		errno = 0;
		if ((int) read(srcfd, buffer, sizeof(buffer)) != (int) sizeof(buffer))
		{
			if (errno != 0)
				ereport(ERROR,
						(errcode_for_file_access(),
						 errmsg("could not read file \"%s\": %m", path)));
			else
				ereport(ERROR,
						(errmsg("not enough data in file \"%s\"", path)));
		}
		errno = 0;
		if ((int) write(fd, buffer, sizeof(buffer)) != (int) sizeof(buffer))
		{
			int			save_errno = errno;

			/*
			 * If we fail to make the file, delete it to release disk space
			 */
			unlink(tmppath);
			/* if write didn't set errno, assume problem is no disk space */
			errno = save_errno ? save_errno : ENOSPC;

			ereport(ERROR,
					(errcode_for_file_access(),
					 errmsg("could not write to file \"%s\": %m", tmppath)));
		}
	}

	if (pg_fsync(fd) != 0)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not fsync file \"%s\": %m", tmppath)));

	if (close(fd))
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not close file \"%s\": %m", tmppath)));

	close(srcfd);

	/*
	 * Now move the segment into place with its final name.
	 */
	if (!InstallXLogFileSegment(&log, &seg, tmppath, false, NULL, false, NULL))
		elog(ERROR, "InstallXLogFileSegment should not have failed");
}

/*
 * Install a new XLOG segment file as a current or future log segment.
 *
 * This is used both to install a newly-created segment (which has a temp
 * filename while it's being created) and to recycle an old segment.
 *
 * *log, *seg: identify segment to install as (or first possible target).
 * When find_free is TRUE, these are modified on return to indicate the
 * actual installation location or last segment searched.
 *
 * tmppath: initial name of file to install.  It will be renamed into place.
 *
 * find_free: if TRUE, install the new segment at the first empty log/seg
 * number at or after the passed numbers.  If FALSE, install the new segment
 * exactly where specified, deleting any existing segment file there.
 *
 * *max_advance: maximum number of log/seg slots to advance past the starting
 * point.  Fail if no free slot is found in this range.  On return, reduced
 * by the number of slots skipped over.  (Irrelevant, and may be NULL,
 * when find_free is FALSE.)
 *
 * use_lock: if TRUE, acquire ControlFileLock while moving file into
 * place.  This should be TRUE except during bootstrap log creation.  The
 * caller must *not* hold the lock at call.
 *
 * Returns TRUE if file installed, FALSE if not installed because of
 * exceeding max_advance limit.  On Windows, we also return FALSE if we
 * can't rename the file into place because someone's got it open.
 * (Any other kind of failure causes ereport().)
 */
static bool
InstallXLogFileSegment(uint32 *log, uint32 *seg, char *tmppath,
					   bool find_free, int *max_advance,
					   bool use_lock, char* tmpsimpleFileName)
{
	char		path[MAXPGPATH];
	char		simpleFileName[MAXPGPATH];
	struct stat stat_buf;
	int retval = 0;

	errno = 0;

	XLogFileName(simpleFileName, ThisTimeLineID, *log, *seg);

	XLogFilePath(path, ThisTimeLineID, *log, *seg);

	/*
	 * We want to be sure that only one process does this at a time.
	 */
	if (use_lock)
		LWLockAcquire(ControlFileLock, LW_EXCLUSIVE);

	if (!find_free)
	{
		/* Force installation: get rid of any pre-existing segment file */
		if (tmpsimpleFileName) {

			MirroredFlatFile_Drop(
								  XLOGDIR,
								  simpleFileName,
								  /* suppressError */ true,
								  /*isMirrorRecovery */ false);
		} else {
			unlink(path);
		}
	}
	else
	{
		/* Find a free slot to put it in */
		while (stat(path, &stat_buf) == 0)
		{
			if (*max_advance <= 0)
			{
				/* Failed to find a free slot within specified range */
				if (use_lock)
					LWLockRelease(ControlFileLock);
				return false;
			}
			NextLogSeg(*log, *seg);
			(*max_advance)--;

			XLogFileName(simpleFileName, ThisTimeLineID, *log, *seg);
			XLogFilePath(path, ThisTimeLineID, *log, *seg);
		}
	}

	/*
	 * Prefer link() to rename() here just to be really sure that we don't
	 * overwrite an existing logfile.  However, there shouldn't be one, so
	 * rename() is an acceptable substitute except for the truly paranoid.
	 */
#if HAVE_WORKING_LINK

	if (tmpsimpleFileName) {
		retval = MirroredFlatFile_Rename(
										 XLOGDIR,
										 /* old name */ tmpsimpleFileName,
										 /* new name */ simpleFileName,
										 /* can exist? */ false,
										 /* isMirrorRecovery */ false);
	} else {
		retval = link(tmppath, path);
	}

	if (retval < 0)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not link file \"%s\" to \"%s\" (initialization of log file %u, segment %u): %m",
						tmppath, path, *log, *seg)));

	if (tmpsimpleFileName) {

		MirroredFlatFile_Drop(
						  XLOGDIR,
						  tmpsimpleFileName,
						  /* suppressError */ true,
						  /*isMirrorRecovery */ false);
	} else {
		unlink(tmppath);
	}

#else
	if (tmpsimpleFileName) {
		retval = MirroredFlatFile_Rename(
						  XLOGDIR,
						  /* old name */ tmpsimpleFileName,
						  /* new name */ simpleFileName,
						  /* can exist */ false,
							/* isMirrorRecovery */ false);
	} else {
		retval = rename(tmppath, path);
	}

	if (retval < 0)
	{
#ifdef WIN32
#if !defined(__CYGWIN__)
		if (GetLastError() == ERROR_ACCESS_DENIED)
#else
		if (errno == EACCES)
#endif
		{
			if (use_lock)
				LWLockRelease(ControlFileLock);
			return false;
		}
#endif /* WIN32 */

		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not rename file \"%s\" to \"%s\" (initialization of log file %u, segment %u): %m",
						tmppath, path, *log, *seg)));
	}
#endif

	if (use_lock)
		LWLockRelease(ControlFileLock);

	return true;
}

/*
 * Open a pre-existing logfile segment for writing.
 */
static void
XLogFileOpen(
	MirroredFlatFileOpen *mirroredOpen,
	uint32 log,
	uint32 seg)
{
	char		simpleFileName[MAXPGPATH];

	XLogFileName(simpleFileName, ThisTimeLineID, log, seg);

	if (MirroredFlatFile_Open(
					mirroredOpen,
					XLOGDIR,
					simpleFileName,
					O_RDWR | PG_BINARY | XLOG_SYNC_BIT,
					S_IRUSR | S_IWUSR,
					/* suppressError */ false,
					/* atomic operation */ false,
					/*isMirrorRecovery */ false))
	{
		char		path[MAXPGPATH];

		XLogFileName(path, ThisTimeLineID, log, seg);

		ereport(PANIC,
				(errcode_for_file_access(),
		   errmsg("could not open file \"%s\" (log file %u, segment %u): %m",
				  path, log, seg)));
	}
}

/*
 * Open a logfile segment for reading (during recovery).
 */
static int
XLogFileRead(uint32 log, uint32 seg, int emode)
{
	char		path[MAXPGPATH];
	char		xlogfname[MAXFNAMELEN];
	ListCell   *cell;
	int			fd;

	/*
	 * Loop looking for a suitable timeline ID: we might need to read any of
	 * the timelines listed in expectedTLIs.
	 *
	 * We expect curFileTLI on entry to be the TLI of the preceding file in
	 * sequence, or 0 if there was no predecessor.	We do not allow curFileTLI
	 * to go backwards; this prevents us from picking up the wrong file when a
	 * parent timeline extends to higher segment numbers than the child we
	 * want to read.
	 */
	foreach(cell, expectedTLIs)
	{
		TimeLineID	tli = (TimeLineID) lfirst_int(cell);

		if (tli < curFileTLI)
			break;				/* don't bother looking at too-old TLIs */

		if (InArchiveRecovery)
		{
			XLogFileName(xlogfname, tli, log, seg);
			restoredFromArchive = RestoreArchivedFile(path, xlogfname,
													  "RECOVERYXLOG",
													  XLogSegSize);
		}
		else
			XLogFilePath(path, tli, log, seg);

		elog((Debug_print_qd_mirroring ? LOG : DEBUG1),"opening \"%s\" for reading (log %u, seg %u)",
                         path, log, seg);
		fd = BasicOpenFile(path, O_RDONLY | PG_BINARY, 0);
		if (fd >= 0)
		{
			/* Success! */
			curFileTLI = tli;
			return fd;
		}
		if (errno != ENOENT)	/* unexpected failure? */
			ereport(PANIC,
					(errcode_for_file_access(),
			errmsg("could not open file \"%s\" (log file %u, segment %u): %m",
				   path, log, seg)));
		else
		{
			ereport(emode,
					(errcode_for_file_access(),
					 errmsg("could not open file \"%s\" (log file %u, segment %u): %m",
							path, log, seg)));
			return -1;
		}

	}

	/* Couldn't find it.  For simplicity, complain about front timeline */
	XLogFilePath(path, recoveryTargetTLI, log, seg);
	errno = ENOENT;
	ereport(emode,
			(errcode_for_file_access(),
		   errmsg("could not open file \"%s\" (log file %u, segment %u): %m",
				  path, log, seg)));
	return -1;
}

/*
 * Close the current logfile segment for writing.
 */
void
XLogFileClose(void)
{
	Assert(MirroredFlatFile_IsActive(&mirroredLogFileOpen));

	/*
	 * posix_fadvise is problematic on many platforms: on older x86 Linux it
	 * just dumps core, and there are reports of problems on PPC platforms as
	 * well.  The following is therefore disabled for the time being. We could
	 * consider some kind of configure test to see if it's safe to use, but
	 * since we lack hard evidence that there's any useful performance gain to
	 * be had, spending time on that seems unprofitable for now.
	 */
#ifdef NOT_USED

	/*
	 * WAL segment files will not be re-read in normal operation, so we advise
	 * OS to release any cached pages.	But do not do so if WAL archiving is
	 * active, because archiver process could use the cache to read the WAL
	 * segment.
	 *
	 * While O_DIRECT works for O_SYNC, posix_fadvise() works for fsync() and
	 * O_SYNC, and some platforms only have posix_fadvise().
	 */
#if defined(HAVE_DECL_POSIX_FADVISE) && defined(POSIX_FADV_DONTNEED)
	if (!XLogArchivingActive())
		posix_fadvise(openLogFile, 0, 0, POSIX_FADV_DONTNEED);
#endif
#endif   /* NOT_USED */

	MirroredFlatFile_Close(&mirroredLogFileOpen);
}

/*
 * Attempt to retrieve the specified file from off-line archival storage.
 * If successful, fill "path" with its complete path (note that this will be
 * a temp file name that doesn't follow the normal naming convention), and
 * return TRUE.
 *
 * If not successful, fill "path" with the name of the normal on-line file
 * (which may or may not actually exist, but we'll try to use it), and return
 * FALSE.
 *
 * For fixed-size files, the caller may pass the expected size as an
 * additional crosscheck on successful recovery.  If the file size is not
 * known, set expectedSize = 0.
 */
static bool
RestoreArchivedFile(char *path, const char *xlogfname,
					const char *recovername, off_t expectedSize)
{
	char		xlogpath[MAXPGPATH];
	char		xlogRestoreCmd[MAXPGPATH];
	char	   *dp;
	char	   *endp;
	const char *sp;
	int			rc;
	bool		signaled;
	struct stat stat_buf;
	char		*xlogDir = NULL;

	ereport((Debug_print_qd_mirroring ? LOG : DEBUG3),
			(errmsg_internal("restore archived file path = \"%s\", xlogfname = \"%s\", recovername = \"%s\", expectedSize %d",
							 path, xlogfname, recovername, (int)expectedSize)));
	/*
	 * When doing archive recovery, we always prefer an archived log file even
	 * if a file of the same name exists in XLOGDIR.  The reason is that the
	 * file in XLOGDIR could be an old, un-filled or partly-filled version
	 * that was copied and restored as part of backing up $PGDATA.
	 *
	 * We could try to optimize this slightly by checking the local copy
	 * lastchange timestamp against the archived copy, but we have no API to
	 * do this, nor can we guarantee that the lastchange timestamp was
	 * preserved correctly when we copied to archive. Our aim is robustness,
	 * so we elect not to do this.
	 *
	 * If we cannot obtain the log file from the archive, however, we will try
	 * to use the XLOGDIR file if it exists.  This is so that we can make use
	 * of log segments that weren't yet transferred to the archive.
	 *
	 * Notice that we don't actually overwrite any files when we copy back
	 * from archive because the recoveryRestoreCommand may inadvertently
	 * restore inappropriate xlogs, or they may be corrupt, so we may wish to
	 * fallback to the segments remaining in current XLOGDIR later. The
	 * copy-from-archive filename is always the same, ensuring that we don't
	 * run out of disk space on long recoveries.
	 */
	xlogDir = makeRelativeToTxnFilespace(XLOGDIR);
	if (snprintf(xlogpath, MAXPGPATH, "%s/%s", xlogDir, recovername) > MAXPGPATH)
	{
		ereport(ERROR, (errmsg("cannot generate filename %s/%s", xlogDir, recovername)));
	}

	/*
	 * Make sure there is no existing file named recovername.
	 */
	if (stat(xlogpath, &stat_buf) != 0)
	{
		if (errno != ENOENT)
			ereport(FATAL,
					(errcode_for_file_access(),
					 errmsg("could not stat file \"%s\": %m",
							xlogpath)));
	}
	else
	{
		if (unlink(xlogpath) != 0)
			ereport(FATAL,
					(errcode_for_file_access(),
					 errmsg("could not remove file \"%s\": %m",
							xlogpath)));
	}

	/*
	 * construct the command to be executed
	 */
	dp = xlogRestoreCmd;
	endp = xlogRestoreCmd + MAXPGPATH - 1;
	*endp = '\0';

	for (sp = recoveryRestoreCommand; *sp; sp++)
	{
		if (*sp == '%')
		{
			switch (sp[1])
			{
				case 'p':
					/* %p: relative path of target file */
					sp++;
					StrNCpy(dp, xlogpath, endp - dp);
					make_native_path(dp);
					dp += strlen(dp);
					break;
				case 'f':
					/* %f: filename of desired file */
					sp++;
					StrNCpy(dp, xlogfname, endp - dp);
					dp += strlen(dp);
					break;
				case '%':
					/* convert %% to a single % */
					sp++;
					if (dp < endp)
						*dp++ = *sp;
					break;
				default:
					/* otherwise treat the % as not special */
					if (dp < endp)
						*dp++ = *sp;
					break;
			}
		}
		else
		{
			if (dp < endp)
				*dp++ = *sp;
		}
	}
	*dp = '\0';

	ereport((Debug_print_qd_mirroring ? LOG : DEBUG3),
			(errmsg_internal("executing restore command \"%s\"",
							 xlogRestoreCmd)));

	/*
	 * Copy xlog from archival storage to XLOGDIR
	 */
	rc = system(xlogRestoreCmd);
	if (rc == 0)
	{
		/*
		 * command apparently succeeded, but let's make sure the file is
		 * really there now and has the correct size.
		 *
		 * XXX I made wrong-size a fatal error to ensure the DBA would notice
		 * it, but is that too strong?	We could try to plow ahead with a
		 * local copy of the file ... but the problem is that there probably
		 * isn't one, and we'd incorrectly conclude we've reached the end of
		 * WAL and we're done recovering ...
		 */
		if (stat(xlogpath, &stat_buf) == 0)
		{
			if (expectedSize > 0 && stat_buf.st_size != expectedSize)
				ereport(FATAL,
						(errmsg("archive file \"%s\" has wrong size: %lu instead of %lu",
								xlogfname,
								(unsigned long) stat_buf.st_size,
								(unsigned long) expectedSize)));
			else
			{
				ereport(LOG,
						(errmsg("restored log file \"%s\" from archive",
								xlogfname)));
				strcpy(path, xlogpath);
				return true;
			}
		}
		else
		{
			/* stat failed */
			if (errno != ENOENT)
				ereport(FATAL,
						(errcode_for_file_access(),
						 errmsg("could not stat file \"%s\": %m",
								xlogpath)));
		}
	}

	/*
	 * Remember, we rollforward UNTIL the restore fails so failure here is
	 * just part of the process... that makes it difficult to determine
	 * whether the restore failed because there isn't an archive to restore,
	 * or because the administrator has specified the restore program
	 * incorrectly.  We have to assume the former.
	 *
	 * However, if the failure was due to any sort of signal, it's best to
	 * punt and abort recovery.  (If we "return false" here, upper levels
	 * will assume that recovery is complete and start up the database!)
	 * It's essential to abort on child SIGINT and SIGQUIT, because per spec
	 * system() ignores SIGINT and SIGQUIT while waiting; if we see one of
	 * those it's a good bet we should have gotten it too.  Aborting on other
	 * signals such as SIGTERM seems a good idea as well.
	 *
	 * Per the Single Unix Spec, shells report exit status > 128 when
	 * a called command died on a signal.  Also, 126 and 127 are used to
	 * report problems such as an unfindable command; treat those as fatal
	 * errors too.
	 */
	signaled = WIFSIGNALED(rc) || WEXITSTATUS(rc) > 125;

	ereport(signaled ? FATAL : DEBUG2,
		(errmsg("could not restore file \"%s\" from archive: return code %d",
				xlogfname, rc)));

	/*
	 * if an archived file is not available, there might still be a version of
	 * this file in XLOGDIR, so return that as the filename to open.
	 *
	 * In many recovery scenarios we expect this to fail also, but if so that
	 * just means we've reached the end of WAL.
	 */
	if (snprintf(path, MAXPGPATH, "%s/%s", xlogDir, xlogfname) > MAXPGPATH)
	{
		ereport(ERROR, (errmsg("cannot generate filename %s/%s", xlogDir, xlogfname)));
	}
	pfree(xlogDir);
	return false;
}

/*
 * Preallocate log files beyond the specified log endpoint, according to
 * the XLOGfile user parameter.
 */
static int
PreallocXlogFiles(XLogRecPtr endptr)
{
	int			nsegsadded = 0;
	uint32		_logId;
	uint32		_logSeg;

	MirroredFlatFileOpen	mirroredOpen;

	bool		use_existent;

	XLByteToPrevSeg(endptr, _logId, _logSeg);
	if ((endptr.xrecoff - 1) % XLogSegSize >=
		(uint32) (0.75 * XLogSegSize))
	{
		NextLogSeg(_logId, _logSeg);
		use_existent = true;
		XLogFileInit(
			&mirroredOpen,
			_logId, _logSeg, &use_existent, true);
		MirroredFlatFile_Close(&mirroredOpen);
		if (!use_existent)
			nsegsadded++;
	}
	return nsegsadded;
}

/*
 * Remove or move offline all log files older or equal to passed log/seg#
 *
 * endptr is current (or recent) end of xlog; this is used to determine
 * whether we want to recycle rather than delete no-longer-wanted log files.
 */
static void
MoveOfflineLogs(uint32 log, uint32 seg, XLogRecPtr endptr,
				int *nsegsremoved, int *nsegsrecycled)
{
	uint32		endlogId;
	uint32		endlogSeg;
	int			max_advance;
	DIR		   *xldir;
	struct dirent *xlde;
	char		lastoff[MAXFNAMELEN];
	char		path[MAXPGPATH];
	char		*xlogDir = NULL;

	*nsegsremoved = 0;
	*nsegsrecycled = 0;

	/*
	 * Initialize info about where to try to recycle to.  We allow recycling
	 * segments up to XLOGfileslop segments beyond the current XLOG location.
	 */
	XLByteToPrevSeg(endptr, endlogId, endlogSeg);
	max_advance = XLOGfileslop;

	xlogDir = makeRelativeToTxnFilespace(XLOGDIR);
	xldir = AllocateDir(xlogDir);
	if (xldir == NULL)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not open transaction log directory \"%s\": %m",
						xlogDir)));

	XLogFileName(lastoff, ThisTimeLineID, log, seg);

	while ((xlde = ReadDir(xldir, xlogDir)) != NULL)
	{
		/*
		 * We ignore the timeline part of the XLOG segment identifiers in
		 * deciding whether a segment is still needed.	This ensures that we
		 * won't prematurely remove a segment from a parent timeline. We could
		 * probably be a little more proactive about removing segments of
		 * non-parent timelines, but that would be a whole lot more
		 * complicated.
		 *
		 * We use the alphanumeric sorting property of the filenames to decide
		 * which ones are earlier than the lastoff segment.
		 */
		if (strlen(xlde->d_name) == 24 &&
			strspn(xlde->d_name, "0123456789ABCDEF") == 24 &&
			strcmp(xlde->d_name + 8, lastoff + 8) <= 0)
		{
			if (XLogArchiveCheckDone(xlde->d_name))
			{
				if (snprintf(path, MAXPGPATH, "%s/%s", xlogDir, xlde->d_name) > MAXPGPATH)
				{
					ereport(ERROR, (errmsg("cannot generate filename %s/%s", xlogDir, xlde->d_name)));
				}

				/*
				 * Before deleting the file, see if it can be recycled as a
				 * future log segment.
				 */
				if (InstallXLogFileSegment(&endlogId, &endlogSeg, path,
										   true, &max_advance,
										   true, xlde->d_name))
				{
					ereport(DEBUG2,
							(errmsg("recycled transaction log file \"%s\"",
									xlde->d_name)));
					(*nsegsrecycled)++;
					/* Needn't recheck that slot on future iterations */
					if (max_advance > 0)
					{
						NextLogSeg(endlogId, endlogSeg);
						max_advance--;
					}
				}
				else
				{
					/* No need for any more future segments... */
					ereport(DEBUG2,
							(errmsg("removing transaction log file \"%s\"",
									xlde->d_name)));

					MirroredFlatFile_Drop(
										  XLOGDIR,
										  xlde->d_name,
										  /* suppressError */ true,
										  /*isMirrorRecovery */ false);

					(*nsegsremoved)++;
				}

				XLogArchiveCleanup(xlde->d_name);
			}
		}
	}

	FreeDir(xldir);
	pfree(xlogDir);
}

/*
 * Remove log files older or equal to passed log/seg# on the standby
 *
 * This is a modified version of MoveOfflineLogs.
 *
 */
void
XLogRemoveStandbyLogs(uint32 log, uint32 seg, int *nsegsremoved)
{
	DIR		   *xldir;
	struct dirent *xlde;
	char		lastoff[MAXFNAMELEN];
	char		path[MAXPGPATH];
	char		*xlogDir = NULL;	

	Assert(nsegsremoved != NULL);

	*nsegsremoved = 0;

	xlogDir = makeRelativeToTxnFilespace(XLOGDIR);
	xldir = AllocateDir(xlogDir);
	if (xldir == NULL)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("QDSYNC: could not open standby transaction log directory \"%s\": %m",
						xlogDir)));

	XLogFileName(lastoff, ThisTimeLineID, log, seg);

	while ((xlde = ReadDir(xldir, xlogDir)) != NULL)
	{
		/*
		 * We ignore the timeline part of the XLOG segment identifiers in
		 * deciding whether a segment is still needed.	This ensures that we
		 * won't prematurely remove a segment from a parent timeline. We could
		 * probably be a little more proactive about removing segments of
		 * non-parent timelines, but that would be a whole lot more
		 * complicated.
		 *
		 * We use the alphanumeric sorting property of the filenames to decide
		 * which ones are earlier than the lastoff segment.
		 */
		if (strlen(xlde->d_name) == 24 &&
			strspn(xlde->d_name, "0123456789ABCDEF") == 24 &&
			strcmp(xlde->d_name + 8, lastoff + 8) <= 0)
		{
			if (snprintf(path, MAXPGPATH, "%s/%s", xlogDir, xlde->d_name) > MAXPGPATH)
			{
				ereport(ERROR, (errmsg("cannot generate filename %s/%s", xlogDir, xlde->d_name)));
			}

			if (unlink(path) < 0)
			{
				ereport(WARNING,
						(errcode_for_file_access(),
						 errmsg("QDSYNC: could not unlink standby transaction log file \"%s\": %m",
						        xlde->d_name)));

			}
			else
			{
				ereport(LOG,
						(errmsg("QDSYNC: removed standby transaction log file \"%s\"",
								xlde->d_name)));
				(*nsegsremoved)++;
			}
		}
	}

	FreeDir(xldir);
	pfree(xlogDir);
}


/*
 * Print log files in the system log.
 *
 */
void
XLogPrintLogNames(void)
{
	DIR		   *xldir;
	struct dirent *xlde;
	int count = 0;
	char *xlogDir = NULL;

	xlogDir = makeRelativeToTxnFilespace(XLOGDIR);
	xldir = AllocateDir(xlogDir);
	if (xldir == NULL)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not open transaction log directory \"%s\": %m",
						xlogDir)));

	while ((xlde = ReadDir(xldir, xlogDir)) != NULL)
	{
		if (strlen(xlde->d_name) == 24 &&
			strspn(xlde->d_name, "0123456789ABCDEF") == 24)
		{
			elog(LOG,"found log file \"%s\"",
				 xlde->d_name);
			count++;
		}
	}

	FreeDir(xldir);
	pfree(xlogDir);

	elog(LOG,"%d files found", count);
}

/*
 * Remove previous backup history files.  This also retries creation of
 * .ready files for any backup history files for which XLogArchiveNotify
 * failed earlier.
 */
static void
CleanupBackupHistory(void)
{
	DIR		   *xldir;
	struct dirent *xlde;
	char		path[MAXPGPATH];
	char	*xlogDir = NULL;

	xlogDir = makeRelativeToTxnFilespace(XLOGDIR);
	xldir = AllocateDir(xlogDir);
	if (xldir == NULL)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not open transaction log directory \"%s\": %m",
						xlogDir)));

	while ((xlde = ReadDir(xldir, xlogDir)) != NULL)
	{
		if (strlen(xlde->d_name) > 24 &&
			strspn(xlde->d_name, "0123456789ABCDEF") == 24 &&
			strcmp(xlde->d_name + strlen(xlde->d_name) - strlen(".backup"),
				   ".backup") == 0)
		{
			if (XLogArchiveCheckDone(xlde->d_name))
			{
				ereport(DEBUG2,
				(errmsg("removing transaction log backup history file \"%s\"",
						xlde->d_name)));
				if (snprintf(path, MAXPGPATH, "%s/%s", xlogDir, xlde->d_name) > MAXPGPATH)
				{
					elog(LOG, "CleanupBackupHistory: Cannot generate filename %s/%s", xlogDir, xlde->d_name);
				}
				unlink(path);
				XLogArchiveCleanup(xlde->d_name);
			}
		}
	}

	pfree(xlogDir);
	FreeDir(xldir);
}

/*
 * Restore the backup blocks present in an XLOG record, if any.
 *
 * We assume all of the record has been read into memory at *record.
 *
 * Note: when a backup block is available in XLOG, we restore it
 * unconditionally, even if the page in the database appears newer.
 * This is to protect ourselves against database pages that were partially
 * or incorrectly written during a crash.  We assume that the XLOG data
 * must be good because it has passed a CRC check, while the database
 * page might not be.  This will force us to replay all subsequent
 * modifications of the page that appear in XLOG, rather than possibly
 * ignoring them as already applied, but that's not a huge drawback.
 */
static void
RestoreBkpBlocks(XLogRecord *record, XLogRecPtr lsn)
{
	MIRROREDLOCK_BUFMGR_DECLARE;

	Relation	reln;
	Buffer		buffer;
	Page		page;
	BkpBlock	bkpb;
	char	   *blk;
	int			i;

	blk = (char *) XLogRecGetData(record) + record->xl_len;
	for (i = 0; i < XLR_MAX_BKP_BLOCKS; i++)
	{
		if (!(record->xl_info & XLR_SET_BKP_BLOCK(i)))
			continue;

		memcpy(&bkpb, blk, sizeof(BkpBlock));
		blk += sizeof(BkpBlock);

		reln = XLogOpenRelation(bkpb.node);

		// -------- MirroredLock ----------
		MIRROREDLOCK_BUFMGR_LOCK;

		buffer = XLogReadBuffer(reln, bkpb.block, true);
		Assert(BufferIsValid(buffer));
		page = (Page) BufferGetPage(buffer);

		if (bkpb.hole_length == 0)
		{
			memcpy((char *) page, blk, BLCKSZ);
		}
		else
		{
			/* must zero-fill the hole */
			MemSet((char *) page, 0, BLCKSZ);
			memcpy((char *) page, blk, bkpb.hole_offset);
			memcpy((char *) page + (bkpb.hole_offset + bkpb.hole_length),
				   blk + bkpb.hole_offset,
				   BLCKSZ - (bkpb.hole_offset + bkpb.hole_length));
		}

		PageSetLSN(page, lsn);
		PageSetTLI(page, ThisTimeLineID);
		MarkBufferDirty(buffer);
		UnlockReleaseBuffer(buffer);

		MIRROREDLOCK_BUFMGR_UNLOCK;
		// -------- MirroredLock ----------

		blk += BLCKSZ - bkpb.hole_length;
	}
}

/*
 * CRC-check an XLOG record.  We do not believe the contents of an XLOG
 * record (other than to the minimal extent of computing the amount of
 * data to read in) until we've checked the CRCs.
 *
 * We assume all of the record has been read into memory at *record.
 */
static bool
RecordIsValid(XLogRecord *record, XLogRecPtr recptr, int emode)
{
	pg_crc32	crc;
	int			i;
	uint32		len = record->xl_len;
	BkpBlock	bkpb;
	char	   *blk;

	/*
	 * Calculate the crc using the new fast crc32c algorithm
	 */

	/* First the rmgr data */
	INIT_CRC32C(crc);
	COMP_CRC32C(crc, XLogRecGetData(record), len);

	/* Add in the backup blocks, if any */
	blk = (char *) XLogRecGetData(record) + len;
	for (i = 0; i < XLR_MAX_BKP_BLOCKS; i++)
	{
		uint32		blen;

		if (!(record->xl_info & XLR_SET_BKP_BLOCK(i)))
			continue;

		memcpy(&bkpb, blk, sizeof(BkpBlock));
		if (bkpb.hole_offset + bkpb.hole_length > BLCKSZ)
		{
			ereport(emode,
					(errmsg("incorrect hole size in record at %X/%X",
							recptr.xlogid, recptr.xrecoff)));
			return false;
		}
		blen = sizeof(BkpBlock) + BLCKSZ - bkpb.hole_length;
		COMP_CRC32C(crc, blk, blen);
		blk += blen;
	}

	/* Check that xl_tot_len agrees with our calculation */
	if (blk != (char *) record + record->xl_tot_len)
	{
		ereport(emode,
				(errmsg("incorrect total length in record at %X/%X",
						recptr.xlogid, recptr.xrecoff)));
		return false;
	}

	/* Finally include the record header */
	COMP_CRC32C(crc, (char *) record + sizeof(pg_crc32),
			   SizeOfXLogRecord - sizeof(pg_crc32));
	FIN_CRC32C(crc);

	if (!EQ_LEGACY_CRC32(record->xl_crc, crc))
	{
		/*
		 * Ok, the crc failed, but it may be that we have a record using the old crc algorithm.
		 * Re-compute the crc using the old algorithm, and check that.
		 */

		/* First the rmgr data */
		INIT_LEGACY_CRC32(crc);
		COMP_LEGACY_CRC32(crc, XLogRecGetData(record), len);

		/* Add in the backup blocks, if any */
		blk = (char *) XLogRecGetData(record) + len;
		for (i = 0; i < XLR_MAX_BKP_BLOCKS; i++)
		{
			uint32		blen;

			if (!(record->xl_info & XLR_SET_BKP_BLOCK(i)))
				continue;

			memcpy(&bkpb, blk, sizeof(BkpBlock));
			if (bkpb.hole_offset + bkpb.hole_length > BLCKSZ)
			{
				ereport(emode,
						(errmsg("incorrect hole size in record at %X/%X",
								recptr.xlogid, recptr.xrecoff)));
				return false;
			}
			blen = sizeof(BkpBlock) + BLCKSZ - bkpb.hole_length;
			COMP_LEGACY_CRC32(crc, blk, blen);
			blk += blen;
		}

		/* Finally include the record header */
		COMP_LEGACY_CRC32(crc, (char *) record + sizeof(pg_crc32),
				   SizeOfXLogRecord - sizeof(pg_crc32));
		FIN_LEGACY_CRC32(crc);
	}

	if (!EQ_LEGACY_CRC32(record->xl_crc, crc))
	{
		ereport(emode,
		(errmsg("incorrect resource manager data checksum in record at %X/%X",
				recptr.xlogid, recptr.xrecoff)));
		return false;
	}

	return true;
}

/*
 * Attempt to read an XLOG record.
 *
 * If RecPtr is not NULL, try to read a record at that position.  Otherwise
 * try to read a record just after the last one previously read.
 *
 * If no valid record is available, returns NULL, or fails if emode is PANIC.
 * (emode must be either PANIC or LOG.)
 *
 * The record is copied into readRecordBuf, so that on successful return,
 * the returned record pointer always points there.
 */
XLogRecord *
XLogReadRecord(XLogRecPtr *RecPtr, int emode)
{
	XLogRecord *record;
	char	   *buffer;
	XLogRecPtr	tmpRecPtr = EndRecPtr;
	bool		randAccess = false;
	uint32		len,
				total_len;
	uint32		targetPageOff;
	uint32		targetRecOff;
	uint32		pageHeaderSize;
	uint32		actual_len;

	if (readBuf == NULL)
	{
		/*
		 * First time through, permanently allocate readBuf.  We do it this
		 * way, rather than just making a static array, for two reasons: (1)
		 * no need to waste the storage in most instantiations of the backend;
		 * (2) a static char array isn't guaranteed to have any particular
		 * alignment, whereas malloc() will provide MAXALIGN'd storage.
		 */
		readBuf = (char *) malloc(XLOG_BLCKSZ);
		if(!readBuf)
			ereport(PANIC, (errmsg("Cannot allocate memory for read log record.  Out of Memory")));
	}

	if (RecPtr == NULL)
	{
		RecPtr = &tmpRecPtr;
		/* fast case if next record is on same page */
		if (nextRecord != NULL)
		{
			record = nextRecord;
			goto got_record;
		}
		/* align old recptr to next page */
		if (tmpRecPtr.xrecoff % XLOG_BLCKSZ != 0)
			tmpRecPtr.xrecoff += (XLOG_BLCKSZ - tmpRecPtr.xrecoff % XLOG_BLCKSZ);
		if (tmpRecPtr.xrecoff >= XLogFileSize)
		{
			(tmpRecPtr.xlogid)++;
			tmpRecPtr.xrecoff = 0;
		}
		/* We will account for page header size below */
	}
	else
	{
		if (!XRecOffIsValid(RecPtr->xrecoff))
			ereport(PANIC,
					(errmsg("invalid record offset at %X/%X",
							RecPtr->xlogid, RecPtr->xrecoff)));

		/*
		 * Since we are going to a random position in WAL, forget any prior
		 * state about what timeline we were in, and allow it to be any
		 * timeline in expectedTLIs.  We also set a flag to allow curFileTLI
		 * to go backwards (but we can't reset that variable right here, since
		 * we might not change files at all).
		 */
		lastPageTLI = 0;		/* see comment in ValidXLOGHeader */
		randAccess = true;		/* allow curFileTLI to go backwards too */
	}

	if (readFile >= 0 && !XLByteInSeg(*RecPtr, readId, readSeg))
	{
		close(readFile);
		readFile = -1;
	}
	XLByteToSeg(*RecPtr, readId, readSeg);
	if (readFile < 0)
	{
		/* Now it's okay to reset curFileTLI if random fetch */
		if (randAccess)
			curFileTLI = 0;

		readFile = XLogFileRead(readId, readSeg, emode);
		if (readFile < 0)
			goto next_record_is_invalid;

		/*
		 * Whenever switching to a new WAL segment, we read the first page of
		 * the file and validate its header, even if that's not where the
		 * target record is.  This is so that we can check the additional
		 * identification info that is present in the first page's "long"
		 * header.
		 */
		readOff = 0;
		if (read(readFile, readBuf, XLOG_BLCKSZ) != XLOG_BLCKSZ)
		{
			ereport(emode,
					(errcode_for_file_access(),
					 errmsg("could not read from log file %u, segment %u, offset %u: %m",
							readId, readSeg, readOff)));
			goto next_record_is_invalid;
		}
		if (!ValidXLOGHeader((XLogPageHeader) readBuf, emode))
			goto next_record_is_invalid;
	}

	targetPageOff = ((RecPtr->xrecoff % XLogSegSize) / XLOG_BLCKSZ) * XLOG_BLCKSZ;
	if (readOff != targetPageOff)
	{
		readOff = targetPageOff;
		if (lseek(readFile, (off_t) readOff, SEEK_SET) < 0)
		{
			ereport(emode,
					(errcode_for_file_access(),
					 errmsg("could not seek in log file %u, segment %u to offset %u: %m",
							readId, readSeg, readOff)));
			goto next_record_is_invalid;
		}
		if (read(readFile, readBuf, XLOG_BLCKSZ) != XLOG_BLCKSZ)
		{
			ereport(emode,
					(errcode_for_file_access(),
					 errmsg("could not read from log file %u, segment %u at offset %u: %m",
							readId, readSeg, readOff)));
			goto next_record_is_invalid;
		}
		if (!ValidXLOGHeader((XLogPageHeader) readBuf, emode))
			goto next_record_is_invalid;
	}
	pageHeaderSize = XLogPageHeaderSize((XLogPageHeader) readBuf);
	targetRecOff = RecPtr->xrecoff % XLOG_BLCKSZ;
	if (targetRecOff == 0)
	{
		/*
		 * Can only get here in the continuing-from-prev-page case, because
		 * XRecOffIsValid eliminated the zero-page-offset case otherwise. Need
		 * to skip over the new page's header.
		 */
		tmpRecPtr.xrecoff += pageHeaderSize;
		targetRecOff = pageHeaderSize;
	}
	else if (targetRecOff < pageHeaderSize)
	{
		ereport(emode,
				(errmsg("invalid record offset at %X/%X",
						RecPtr->xlogid, RecPtr->xrecoff)));
		goto next_record_is_invalid;
	}
	if ((((XLogPageHeader) readBuf)->xlp_info & XLP_FIRST_IS_CONTRECORD) &&
		targetRecOff == pageHeaderSize)
	{
		ereport(emode,
				(errmsg("contrecord is requested by %X/%X",
						RecPtr->xlogid, RecPtr->xrecoff)));
		goto next_record_is_invalid;
	}
	record = (XLogRecord *) ((char *) readBuf + RecPtr->xrecoff % XLOG_BLCKSZ);

got_record:;

	/*
	 * xl_len == 0 is bad data for everything except XLOG SWITCH, where it is
	 * required.
	 */
	if (record->xl_rmid == RM_XLOG_ID && record->xl_info == XLOG_SWITCH)
	{
		if (record->xl_len != 0)
		{
			ereport(emode,
					(errmsg("invalid xlog switch record at %X/%X",
							RecPtr->xlogid, RecPtr->xrecoff)));
			goto next_record_is_invalid;
		}
	}
	else if (record->xl_len == 0)
	{
		ereport(emode,
				(errmsg("record with zero length at %X/%X",
						RecPtr->xlogid, RecPtr->xrecoff)));
		goto next_record_is_invalid;
	}
	if (record->xl_tot_len < SizeOfXLogRecord + record->xl_len ||
		record->xl_tot_len > SizeOfXLogRecord + record->xl_len +
		XLR_MAX_BKP_BLOCKS * (sizeof(BkpBlock) + BLCKSZ))
	{
		ereport(emode,
				(errmsg("invalid record length at %X/%X",
						RecPtr->xlogid, RecPtr->xrecoff)));
		goto next_record_is_invalid;
	}
	if (record->xl_rmid > RM_MAX_ID)
	{
		ereport(emode,
				(errmsg("invalid resource manager ID %u at %X/%X",
						record->xl_rmid, RecPtr->xlogid, RecPtr->xrecoff)));
		goto next_record_is_invalid;
	}
	if (randAccess)
	{
		/*
		 * We can't exactly verify the prev-link, but surely it should be less
		 * than the record's own address.
		 */
		if (!XLByteLT(record->xl_prev, *RecPtr))
		{
			ereport(emode,
					(errmsg("record with incorrect prev-link %X/%X at %X/%X",
							record->xl_prev.xlogid, record->xl_prev.xrecoff,
							RecPtr->xlogid, RecPtr->xrecoff)));
			goto next_record_is_invalid;
		}
	}
	else
	{
		/*
		 * Record's prev-link should exactly match our previous location. This
		 * check guards against torn WAL pages where a stale but valid-looking
		 * WAL record starts on a sector boundary.
		 */
		if (!XLByteEQ(record->xl_prev, ReadRecPtr))
		{
			ereport(emode,
					(errmsg("record with incorrect prev-link %X/%X at %X/%X",
							record->xl_prev.xlogid, record->xl_prev.xrecoff,
							RecPtr->xlogid, RecPtr->xrecoff)));
			goto next_record_is_invalid;
		}
	}

	/*
	 * Allocate or enlarge readRecordBuf as needed.  To avoid useless small
	 * increases, round its size to a multiple of XLOG_BLCKSZ, and make sure
	 * it's at least 4*Max(BLCKSZ, XLOG_BLCKSZ) to start with.  (That is
	 * enough for all "normal" records, but very large commit or abort records
	 * might need more space.)
	 */
	total_len = record->xl_tot_len;
	if (total_len > readRecordBufSize)
	{
		uint32		newSize = total_len;

		newSize += XLOG_BLCKSZ - (newSize % XLOG_BLCKSZ);
		newSize = Max(newSize, 4 * Max(BLCKSZ, XLOG_BLCKSZ));
		if (readRecordBuf)
			free(readRecordBuf);
		readRecordBuf = (char *) malloc(newSize);
		if (!readRecordBuf)
		{
			readRecordBufSize = 0;
			/* We treat this as a "bogus data" condition */
			ereport(emode,
					(errmsg("record length %u at %X/%X too long",
							total_len, RecPtr->xlogid, RecPtr->xrecoff)));
			goto next_record_is_invalid;
		}
		readRecordBufSize = newSize;
	}

	buffer = readRecordBuf;
	nextRecord = NULL;
	len = XLOG_BLCKSZ - RecPtr->xrecoff % XLOG_BLCKSZ;
	if (total_len > len)
	{
		/* Need to reassemble record */
		XLogContRecord *contrecord;
		uint32		gotlen = len;

		memcpy(buffer, record, len);
		record = (XLogRecord *) buffer;
		buffer += len;
		for (;;)
		{
			readOff += XLOG_BLCKSZ;
			if (readOff >= XLogSegSize)
			{
				close(readFile);
				readFile = -1;
				NextLogSeg(readId, readSeg);
				readFile = XLogFileRead(readId, readSeg, emode);
				if (readFile < 0)
					goto next_record_is_invalid;
				readOff = 0;
			}
			Assert(readFile >= 0);
			actual_len = read(readFile, readBuf, XLOG_BLCKSZ);
			if ((int32)actual_len < 0)
			{
				elog(LOG,"read failed #3 offset 0x%X", readOff);

				ereport(emode,
						(errcode_for_file_access(),
						 errmsg("could not read from log file %u, segment %u, offset %u: %m",
								readId, readSeg, readOff)));
				goto next_record_is_invalid;
			}
			if (actual_len != XLOG_BLCKSZ)
			{
				ereport(emode,
						 (errmsg("short read for log file %u, segment %u, offset %u (expected %d, found %d)",
								readId, readSeg, readOff,XLOG_BLCKSZ,actual_len)));
				goto next_record_is_invalid;
			}
			if (!ValidXLOGHeader((XLogPageHeader) readBuf, emode))
				goto next_record_is_invalid;
			if (!(((XLogPageHeader) readBuf)->xlp_info & XLP_FIRST_IS_CONTRECORD))
			{
				ereport(emode,
						(errmsg("there is no contrecord flag in log file %u, segment %u, offset %u",
								readId, readSeg, readOff)));
				goto next_record_is_invalid;
			}
			pageHeaderSize = XLogPageHeaderSize((XLogPageHeader) readBuf);
			contrecord = (XLogContRecord *) ((char *) readBuf + pageHeaderSize);
			if (contrecord->xl_rem_len == 0 ||
				total_len != (contrecord->xl_rem_len + gotlen))
			{
				ereport(emode,
						(errmsg("invalid contrecord length %u in log file %u, segment %u, offset %u",
								contrecord->xl_rem_len,
								readId, readSeg, readOff)));
				goto next_record_is_invalid;
			}
			len = XLOG_BLCKSZ - pageHeaderSize - SizeOfXLogContRecord;
			if (contrecord->xl_rem_len > len)
			{
				memcpy(buffer, (char *) contrecord + SizeOfXLogContRecord, len);
				gotlen += len;
				buffer += len;
				continue;
			}
			memcpy(buffer, (char *) contrecord + SizeOfXLogContRecord,
				   contrecord->xl_rem_len);
			break;
		}
		if (!RecordIsValid(record, *RecPtr, emode))
			goto next_record_is_invalid;
		pageHeaderSize = XLogPageHeaderSize((XLogPageHeader) readBuf);
		if (XLOG_BLCKSZ - SizeOfXLogRecord >= pageHeaderSize +
			MAXALIGN(SizeOfXLogContRecord + contrecord->xl_rem_len))
		{
			nextRecord = (XLogRecord *) ((char *) contrecord +
					MAXALIGN(SizeOfXLogContRecord + contrecord->xl_rem_len));
		}
		EndRecPtr.xlogid = readId;
		EndRecPtr.xrecoff = readSeg * XLogSegSize + readOff +
			pageHeaderSize +
			MAXALIGN(SizeOfXLogContRecord + contrecord->xl_rem_len);
		ReadRecPtr = *RecPtr;
		/* needn't worry about XLOG SWITCH, it can't cross page boundaries */
		return record;
	}

	/* Record does not cross a page boundary */
	if (!RecordIsValid(record, *RecPtr, emode))
		goto next_record_is_invalid;
	if (XLOG_BLCKSZ - SizeOfXLogRecord >= RecPtr->xrecoff % XLOG_BLCKSZ +
		MAXALIGN(total_len))
		nextRecord = (XLogRecord *) ((char *) record + MAXALIGN(total_len));
	EndRecPtr.xlogid = RecPtr->xlogid;
	EndRecPtr.xrecoff = RecPtr->xrecoff + MAXALIGN(total_len);
	ReadRecPtr = *RecPtr;
	memcpy(buffer, record, total_len);

	/*
	 * Special processing if it's an XLOG SWITCH record
	 */
	if (record->xl_rmid == RM_XLOG_ID && record->xl_info == XLOG_SWITCH)
	{
		/* Pretend it extends to end of segment */
		EndRecPtr.xrecoff += XLogSegSize - 1;
		EndRecPtr.xrecoff -= EndRecPtr.xrecoff % XLogSegSize;
		nextRecord = NULL;		/* definitely not on same page */

		/*
		 * Pretend that readBuf contains the last page of the segment. This is
		 * just to avoid Assert failure in StartupXLOG if XLOG ends with this
		 * segment.
		 */
		readOff = XLogSegSize - XLOG_BLCKSZ;
	}
	return (XLogRecord *) buffer;

next_record_is_invalid:;
	if (readFile >= 0)
	{
		close(readFile);
		readFile = -1;
	}
	nextRecord = NULL;
	return NULL;
}

void
XLogCloseReadRecord(void)
{
	if (readFile >= 0)
	{
		close(readFile);
		readFile = -1;
	}
	else
	{
		Assert(readFile == -1);
	}

	readId = 0;
	readSeg = 0;
	nextRecord = NULL;

	memset(&ReadRecPtr, 0, sizeof(XLogRecPtr));
	memset(&EndRecPtr, 0, sizeof(XLogRecPtr));

	elog((Debug_print_qd_mirroring ? LOG : DEBUG1),"close read record");
}

/*
 * Check whether the xlog header of a page just read in looks valid.
 *
 * This is just a convenience subroutine to avoid duplicated code in
 * ReadRecord.	It's not intended for use from anywhere else.
 */
static bool
ValidXLOGHeader(XLogPageHeader hdr, int emode)
{
	XLogRecPtr	recaddr;

	if (hdr->xlp_magic != XLOG_PAGE_MAGIC)
	{
		ereport(emode,
				(errmsg("invalid magic number %04X in log file %u, segment %u, offset %u",
						hdr->xlp_magic, readId, readSeg, readOff)));
		return false;
	}
	if ((hdr->xlp_info & ~XLP_ALL_FLAGS) != 0)
	{
		ereport(emode,
				(errmsg("invalid info bits %04X in log file %u, segment %u, offset %u",
						hdr->xlp_info, readId, readSeg, readOff)));
		return false;
	}
	if (hdr->xlp_info & XLP_LONG_HEADER)
	{
		XLogLongPageHeader longhdr = (XLogLongPageHeader) hdr;

		if (longhdr->xlp_sysid != ControlFile->system_identifier)
		{
			char		fhdrident_str[32];
			char		sysident_str[32];

			/*
			 * Format sysids separately to keep platform-dependent format code
			 * out of the translatable message string.
			 */
			snprintf(fhdrident_str, sizeof(fhdrident_str), UINT64_FORMAT,
					 longhdr->xlp_sysid);
			snprintf(sysident_str, sizeof(sysident_str), UINT64_FORMAT,
					 ControlFile->system_identifier);
			ereport(emode,
					(errmsg("WAL file is from different system"),
					 errdetail("WAL file SYSID is %s, pg_control SYSID is %s",
							   fhdrident_str, sysident_str)));
			return false;
		}
		if (longhdr->xlp_seg_size != XLogSegSize)
		{
			ereport(emode,
					(errmsg("WAL file is from different system"),
					 errdetail("Incorrect XLOG_SEG_SIZE in page header.")));
			return false;
		}
		if (longhdr->xlp_xlog_blcksz != XLOG_BLCKSZ)
		{
			ereport(emode,
					(errmsg("WAL file is from different system"),
					 errdetail("Incorrect XLOG_BLCKSZ in page header.")));
			return false;
		}
	}
	else if (readOff == 0)
	{
		/* hmm, first page of file doesn't have a long header? */
		ereport(emode,
				(errmsg("invalid info bits %04X in log file %u, segment %u, offset %u",
						hdr->xlp_info, readId, readSeg, readOff)));
		return false;
	}

	recaddr.xlogid = readId;
	recaddr.xrecoff = readSeg * XLogSegSize + readOff;
	if (!XLByteEQ(hdr->xlp_pageaddr, recaddr))
	{
		ereport(emode,
				(errmsg("unexpected pageaddr %X/%X in log file %u, segment %u, offset %u",
						hdr->xlp_pageaddr.xlogid, hdr->xlp_pageaddr.xrecoff,
						readId, readSeg, readOff)));
		return false;
	}

	/*
	 * Check page TLI is one of the expected values.
	 */
	if (!list_member_int(expectedTLIs, (int) hdr->xlp_tli))
	{
		ereport(emode,
				(errmsg("unexpected timeline ID %u in log file %u, segment %u, offset %u",
						hdr->xlp_tli,
						readId, readSeg, readOff)));
		return false;
	}

	/*
	 * Since child timelines are always assigned a TLI greater than their
	 * immediate parent's TLI, we should never see TLI go backwards across
	 * successive pages of a consistent WAL sequence.
	 *
	 * Of course this check should only be applied when advancing sequentially
	 * across pages; therefore ReadRecord resets lastPageTLI to zero when
	 * going to a random page.
	 */
	if (hdr->xlp_tli < lastPageTLI)
	{
		ereport(emode,
				(errmsg("out-of-sequence timeline ID %u (after %u) in log file %u, segment %u, offset %u",
						hdr->xlp_tli, lastPageTLI,
						readId, readSeg, readOff)));
		return false;
	}
	lastPageTLI = hdr->xlp_tli;
	return true;
}

/*
 * Try to read a timeline's history file.
 *
 * If successful, return the list of component TLIs (the given TLI followed by
 * its ancestor TLIs).	If we can't find the history file, assume that the
 * timeline has no parents, and return a list of just the specified timeline
 * ID.
 */
List *
XLogReadTimeLineHistory(TimeLineID targetTLI)
{
	List	   *result;
	char		path[MAXPGPATH];
	char		histfname[MAXFNAMELEN];
	char		fline[MAXPGPATH];
	FILE	   *fd;

	if (InArchiveRecovery)
	{
		TLHistoryFileName(histfname, targetTLI);
		RestoreArchivedFile(path, histfname, "RECOVERYHISTORY", 0);
	}
	else
		TLHistoryFilePath(path, targetTLI);

	fd = AllocateFile(path, "r");
	if (fd == NULL)
	{
		if (errno != ENOENT)
			ereport(FATAL,
					(errcode_for_file_access(),
					 errmsg("could not open file \"%s\": %m", path)));
		/* Not there, so assume no parents */
		return list_make1_int((int) targetTLI);
	}

	result = NIL;

	/*
	 * Parse the file...
	 */
	while (fgets(fline, MAXPGPATH, fd) != NULL)
	{
		/* skip leading whitespace and check for # comment */
		char	   *ptr;
		char	   *endptr;
		TimeLineID	tli;

		for (ptr = fline; *ptr; ptr++)
		{
			if (!isspace((unsigned char) *ptr))
				break;
		}
		if (*ptr == '\0' || *ptr == '#')
			continue;

		/* expect a numeric timeline ID as first field of line */
		tli = (TimeLineID) strtoul(ptr, &endptr, 0);
		if (endptr == ptr)
			ereport(FATAL,
					(errmsg("syntax error in history file: %s", fline),
					 errhint("Expected a numeric timeline ID.")));

		if (result &&
			tli <= (TimeLineID) linitial_int(result))
			ereport(FATAL,
					(errmsg("invalid data in history file: %s", fline),
				   errhint("Timeline IDs must be in increasing sequence.")));

		/* Build list with newest item first */
		result = lcons_int((int) tli, result);

		/* we ignore the remainder of each line */
	}

	FreeFile(fd);

	if (result &&
		targetTLI <= (TimeLineID) linitial_int(result))
		ereport(FATAL,
				(errmsg("invalid data in history file \"%s\"", path),
			errhint("Timeline IDs must be less than child timeline's ID.")));

	result = lcons_int((int) targetTLI, result);

	ereport(DEBUG3,
			(errmsg_internal("history of timeline %u is %s",
							 targetTLI, nodeToString(result))));

	return result;
}

/*
 * Probe whether a timeline history file exists for the given timeline ID
 */
static bool
existsTimeLineHistory(TimeLineID probeTLI)
{
	char		path[MAXPGPATH];
	char		histfname[MAXFNAMELEN];
	FILE	   *fd;

	if (InArchiveRecovery)
	{
		TLHistoryFileName(histfname, probeTLI);
		RestoreArchivedFile(path, histfname, "RECOVERYHISTORY", 0);
	}
	else
		TLHistoryFilePath(path, probeTLI);

	fd = AllocateFile(path, "r");
	if (fd != NULL)
	{
		FreeFile(fd);
		return true;
	}
	else
	{
		if (errno != ENOENT)
			ereport(FATAL,
					(errcode_for_file_access(),
					 errmsg("could not open file \"%s\": %m", path)));
		return false;
	}
}

/*
 * Find the newest existing timeline, assuming that startTLI exists.
 *
 * Note: while this is somewhat heuristic, it does positively guarantee
 * that (result + 1) is not a known timeline, and therefore it should
 * be safe to assign that ID to a new timeline.
 */
static TimeLineID
findNewestTimeLine(TimeLineID startTLI)
{
	TimeLineID	newestTLI;
	TimeLineID	probeTLI;

	/*
	 * The algorithm is just to probe for the existence of timeline history
	 * files.  XXX is it useful to allow gaps in the sequence?
	 */
	newestTLI = startTLI;

	for (probeTLI = startTLI + 1;; probeTLI++)
	{
		if (existsTimeLineHistory(probeTLI))
		{
			newestTLI = probeTLI;		/* probeTLI exists */
		}
		else
		{
			/* doesn't exist, assume we're done */
			break;
		}
	}

	return newestTLI;
}

/*
 * Create a new timeline history file.
 *
 *	newTLI: ID of the new timeline
 *	parentTLI: ID of its immediate parent
 *	endTLI et al: ID of the last used WAL file, for annotation purposes
 *
 * Currently this is only used during recovery, and so there are no locking
 * considerations.	But we should be just as tense as XLogFileInit to avoid
 * emplacing a bogus file.
 */
static void
writeTimeLineHistory(TimeLineID newTLI, TimeLineID parentTLI,
					 TimeLineID endTLI, uint32 endLogId, uint32 endLogSeg)
{
	char		path[MAXPGPATH];
	char		tmppath[MAXPGPATH];
	char		histfname[MAXFNAMELEN];
	char		xlogfname[MAXFNAMELEN];
	char		buffer[BLCKSZ];
	int			srcfd;
	int			fd;
	int			nbytes;
	char		*xlogDir = NULL;
	
	Assert(newTLI > parentTLI); /* else bad selection of newTLI */

	/*
	 * Write into a temp file name.
	 */
	xlogDir = makeRelativeToTxnFilespace(XLOGDIR);
	if (snprintf(tmppath, MAXPGPATH, "%s/xlogtemp.%d", xlogDir, (int) getpid()) > MAXPGPATH)
	{
		ereport(ERROR, (errmsg("cannot generate filename %s/xlogtemp.%d", xlogDir, (int) getpid())));
	}	
	pfree(xlogDir);
	unlink(tmppath);

	/* do not use XLOG_SYNC_BIT here --- want to fsync only at end of fill */
	fd = BasicOpenFile(tmppath, O_RDWR | O_CREAT | O_EXCL,
					   S_IRUSR | S_IWUSR);
	if (fd < 0)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not create file \"%s\": %m", tmppath)));

	/*
	 * If a history file exists for the parent, copy it verbatim
	 */
	if (InArchiveRecovery)
	{
		TLHistoryFileName(histfname, parentTLI);
		RestoreArchivedFile(path, histfname, "RECOVERYHISTORY", 0);
	}
	else
		TLHistoryFilePath(path, parentTLI);

	srcfd = BasicOpenFile(path, O_RDONLY, 0);
	if (srcfd < 0)
	{
		if (errno != ENOENT)
			ereport(ERROR,
					(errcode_for_file_access(),
					 errmsg("could not open file \"%s\": %m", path)));
		/* Not there, so assume parent has no parents */
	}
	else
	{
		for (;;)
		{
			errno = 0;
			nbytes = (int) read(srcfd, buffer, sizeof(buffer));
			if (nbytes < 0 || errno != 0)
				ereport(ERROR,
						(errcode_for_file_access(),
						 errmsg("could not read file \"%s\": %m", path)));
			if (nbytes == 0)
				break;
			errno = 0;
			if ((int) write(fd, buffer, nbytes) != nbytes)
			{
				int			save_errno = errno;

				/*
				 * If we fail to make the file, delete it to release disk
				 * space
				 */
				unlink(tmppath);

				/*
				 * if write didn't set errno, assume problem is no disk space
				 */
				errno = save_errno ? save_errno : ENOSPC;

				ereport(ERROR,
						(errcode_for_file_access(),
					 errmsg("could not write to file \"%s\": %m", tmppath)));
			}
		}
		close(srcfd);
	}

	/*
	 * Append one line with the details of this timeline split.
	 *
	 * If we did have a parent file, insert an extra newline just in case the
	 * parent file failed to end with one.
	 */
	XLogFileName(xlogfname, endTLI, endLogId, endLogSeg);

	snprintf(buffer, sizeof(buffer),
			 "%s%u\t%s\t%s transaction %u at %s\n",
			 (srcfd < 0) ? "" : "\n",
			 parentTLI,
			 xlogfname,
			 recoveryStopAfter ? "after" : "before",
			 recoveryStopXid,
			 str_time(recoveryStopTime));

	nbytes = strlen(buffer);
	errno = 0;
	if ((int) write(fd, buffer, nbytes) != nbytes)
	{
		int			save_errno = errno;

		/*
		 * If we fail to make the file, delete it to release disk space
		 */
		unlink(tmppath);
		/* if write didn't set errno, assume problem is no disk space */
		errno = save_errno ? save_errno : ENOSPC;

		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not write to file \"%s\": %m", tmppath)));
	}

	if (pg_fsync(fd) != 0)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not fsync file \"%s\": %m", tmppath)));

	if (close(fd))
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not close file \"%s\": %m", tmppath)));


	/*
	 * Now move the completed history file into place with its final name.
	 */
	TLHistoryFilePath(path, newTLI);

	/*
	 * Prefer link() to rename() here just to be really sure that we don't
	 * overwrite an existing logfile.  However, there shouldn't be one, so
	 * rename() is an acceptable substitute except for the truly paranoid.
	 */
#if HAVE_WORKING_LINK
	if (link(tmppath, path) < 0)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not link file \"%s\" to \"%s\": %m",
						tmppath, path)));
	unlink(tmppath);
#else
	if (rename(tmppath, path) < 0)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not rename file \"%s\" to \"%s\": %m",
						tmppath, path)));
#endif

	/* The history file can be archived immediately. */
	TLHistoryFileName(histfname, newTLI);
	XLogArchiveNotify(histfname);
	pfree(xlogDir);
}

static void
ControlFileWatcherSaveInitial(void)
{
	ControlFileWatcher->current_checkPointLoc = ControlFile->checkPoint;
	ControlFileWatcher->current_prevCheckPointLoc = ControlFile->prevCheckPoint;
	ControlFileWatcher->current_checkPointCopy_redo = ControlFile->checkPointCopy.redo;

	if (Debug_print_control_checkpoints)
		elog(LOG,"pg_control checkpoint: initial values (checkpoint loc %s, previous loc %s, copy's redo loc %s)",
			 XLogLocationToString_Long(&ControlFile->checkPoint),
			 XLogLocationToString2_Long(&ControlFile->prevCheckPoint),
			 XLogLocationToString3_Long(&ControlFile->checkPointCopy.redo));

	ControlFileWatcher->watcherInitialized = true;
}

static void
ControlFileWatcherCheckForChange(void)
{
	XLogRecPtr  writeLoc;
	XLogRecPtr  flushedLoc;

	if (!XLByteEQ(ControlFileWatcher->current_checkPointLoc,ControlFile->checkPoint) ||
		!XLByteEQ(ControlFileWatcher->current_prevCheckPointLoc,ControlFile->prevCheckPoint) ||
		!XLByteEQ(ControlFileWatcher->current_checkPointCopy_redo,ControlFile->checkPointCopy.redo))
	{
		ControlFileWatcher->current_checkPointLoc = ControlFile->checkPoint;
		ControlFileWatcher->current_prevCheckPointLoc = ControlFile->prevCheckPoint;
		ControlFileWatcher->current_checkPointCopy_redo = ControlFile->checkPointCopy.redo;

		if (XLogGetWriteAndFlushedLoc(&writeLoc, &flushedLoc))
		{
			bool problem = XLByteLE(flushedLoc,ControlFile->checkPoint);
			if (problem)
				elog(PANIC,"Checkpoint location %s for pg_control file is not flushed (write loc %s, flushed loc is %s)",
				     XLogLocationToString_Long(&ControlFile->checkPoint),
				     XLogLocationToString2_Long(&writeLoc),
				     XLogLocationToString3_Long(&flushedLoc));

			if (Debug_print_control_checkpoints)
				elog(LOG,"pg_control checkpoint: change (checkpoint loc %s, previous loc %s, copy's redo loc %s, write loc %s, flushed loc %s)",
					 XLogLocationToString_Long(&ControlFile->checkPoint),
					 XLogLocationToString2_Long(&ControlFile->prevCheckPoint),
					 XLogLocationToString3_Long(&ControlFile->checkPointCopy.redo),
					 XLogLocationToString4_Long(&writeLoc),
					 XLogLocationToString5_Long(&flushedLoc));
		}
		else
		{
			if (Debug_print_control_checkpoints)
				elog(LOG,"pg_control checkpoint: change (checkpoint loc %s, previous loc %s, copy's redo loc %s)",
					 XLogLocationToString_Long(&ControlFile->checkPoint),
					 XLogLocationToString2_Long(&ControlFile->prevCheckPoint),
					 XLogLocationToString3_Long(&ControlFile->checkPointCopy.redo));
		}
	}
}

/*
 * I/O routines for pg_control
 *
 * *ControlFile is a buffer in shared memory that holds an image of the
 * contents of pg_control.	WriteControlFile() initializes pg_control
 * given a preloaded buffer, ReadControlFile() loads the buffer from
 * the pg_control file (during postmaster or standalone-backend startup),
 * and UpdateControlFile() rewrites pg_control after we modify xlog state.
 *
 * For simplicity, WriteControlFile() initializes the fields of pg_control
 * that are related to checking backend/database compatibility, and
 * ReadControlFile() verifies they are correct.  We could split out the
 * I/O and compatibility-check functions, but there seems no need currently.
 */
static void
WriteControlFile(void)
{
	MirroredFlatFileOpen	mirroredOpen;

	char		buffer[PG_CONTROL_SIZE];		/* need not be aligned */
	char	   *localeptr;

	/*
	 * Initialize version and compatibility-check fields
	 */
	ControlFile->pg_control_version = PG_CONTROL_VERSION;
	ControlFile->catalog_version_no = CATALOG_VERSION_NO;

	ControlFile->maxAlign = MAXIMUM_ALIGNOF;
	ControlFile->floatFormat = FLOATFORMAT_VALUE;

	ControlFile->blcksz = BLCKSZ;
	ControlFile->relseg_size = RELSEG_SIZE;
	ControlFile->xlog_blcksz = XLOG_BLCKSZ;
	ControlFile->xlog_seg_size = XLOG_SEG_SIZE;

	ControlFile->nameDataLen = NAMEDATALEN;
	ControlFile->indexMaxKeys = INDEX_MAX_KEYS;

#ifdef HAVE_INT64_TIMESTAMP
	ControlFile->enableIntTimes = TRUE;
#else
	ControlFile->enableIntTimes = FALSE;
#endif

	ControlFile->localeBuflen = LOCALE_NAME_BUFLEN;
	localeptr = setlocale(LC_COLLATE, NULL);
	if (!localeptr)
		ereport(PANIC,
				(errmsg("invalid LC_COLLATE setting")));
	StrNCpy(ControlFile->lc_collate, localeptr, LOCALE_NAME_BUFLEN);
	localeptr = setlocale(LC_CTYPE, NULL);
	if (!localeptr)
		ereport(PANIC,
				(errmsg("invalid LC_CTYPE setting")));
	StrNCpy(ControlFile->lc_ctype, localeptr, LOCALE_NAME_BUFLEN);

	/* Contents are protected with a CRC */
	INIT_CRC32C(ControlFile->crc);
	COMP_CRC32C(ControlFile->crc,
			   (char *) ControlFile,
			   offsetof(ControlFileData, crc));
	FIN_CRC32C(ControlFile->crc);

	/*
	 * We write out PG_CONTROL_SIZE bytes into pg_control, zero-padding the
	 * excess over sizeof(ControlFileData).  This reduces the odds of
	 * premature-EOF errors when reading pg_control.  We'll still fail when we
	 * check the contents of the file, but hopefully with a more specific
	 * error than "couldn't read pg_control".
	 */
	if (sizeof(ControlFileData) > PG_CONTROL_SIZE)
		elog(PANIC, "sizeof(ControlFileData) is larger than PG_CONTROL_SIZE; fix either one");

	memset(buffer, 0, PG_CONTROL_SIZE);
	memcpy(buffer, ControlFile, sizeof(ControlFileData));

	MirroredFlatFile_Open(
					&mirroredOpen,
					XLOG_CONTROL_FILE_SUBDIR,
					XLOG_CONTROL_FILE_SIMPLE,
					O_RDWR | O_CREAT | O_EXCL | PG_BINARY,
					S_IRUSR | S_IWUSR,
					/* suppressError */ false,
					/* atomic operation */ false,
					/*isMirrorRecovery */ false);

	MirroredFlatFile_Write(
					&mirroredOpen,
					0,
					buffer,
					PG_CONTROL_SIZE,
					/* suppressError */ false);

	MirroredFlatFile_Flush(
					&mirroredOpen,
					/* suppressError */ false);

	MirroredFlatFile_Close(&mirroredOpen);

	ControlFileWatcherSaveInitial();
}

static void
ReadControlFile(void)
{
	pg_crc32	crc;
	int			fd;

	/*
	 * Read data...
	 */
	fd = BasicOpenFile(XLOG_CONTROL_FILE,
					   O_RDWR | PG_BINARY,
					   S_IRUSR | S_IWUSR);
	if (fd < 0)
		ereport(PANIC,
				(errcode_for_file_access(),
				 errmsg("could not open control file \"%s\": %m",
						XLOG_CONTROL_FILE)));

	if (read(fd, ControlFile, sizeof(ControlFileData)) != sizeof(ControlFileData))
		ereport(PANIC,
				(errcode_for_file_access(),
				 errmsg("could not read from control file: %m")));

	close(fd);

	/*
	 * Check for expected pg_control format version.  If this is wrong, the
	 * CRC check will likely fail because we'll be checking the wrong number
	 * of bytes.  Complaining about wrong version will probably be more
	 * enlightening than complaining about wrong CRC.
	 */
	if (ControlFile->pg_control_version != PG_CONTROL_VERSION)
		ereport(FATAL,
				(errmsg("database files are incompatible with server"),
				 errdetail("The database cluster was initialized with PG_CONTROL_VERSION %d,"
				  " but the server was compiled with PG_CONTROL_VERSION %d.",
						ControlFile->pg_control_version, PG_CONTROL_VERSION),
				 errhint("It looks like you need to initdb.")));

	/* Now check the CRC. */
	INIT_CRC32C(crc);
	COMP_CRC32C(crc,
			   (char *) ControlFile,
			   offsetof(ControlFileData, crc));
	FIN_CRC32C(crc);

	if (!EQ_LEGACY_CRC32(crc, ControlFile->crc))
	{
		/* We might have an old record.  Recompute using old crc algorithm, and re-check. */
		INIT_LEGACY_CRC32(crc);
		COMP_LEGACY_CRC32(crc,
				   (char *) ControlFile,
				   offsetof(ControlFileData, crc));
		FIN_LEGACY_CRC32(crc);
		if (!EQ_LEGACY_CRC32(crc, ControlFile->crc))
				ereport(FATAL,
						(errmsg("incorrect checksum in control file")));
	}

	/*
	 * Do compatibility checking immediately, except during upgrade.
	 * We do this here for 2 reasons:
	 *
	 * (1) if the database isn't compatible with the backend executable, we
	 * want to abort before we can possibly do any damage;
	 *
	 * (2) this code is executed in the postmaster, so the setlocale() will
	 * propagate to forked backends, which aren't going to read this file for
	 * themselves.	(These locale settings are considered critical
	 * compatibility items because they can affect sort order of indexes.)
	 */
	if (ControlFile->catalog_version_no != CATALOG_VERSION_NO &&
		!gp_upgrade_mode)
		ereport(FATAL,
				(errmsg("database files are incompatible with server"),
				 errdetail("The database cluster was initialized with CATALOG_VERSION_NO %d,"
				  " but the server was compiled with CATALOG_VERSION_NO %d.",
						ControlFile->catalog_version_no, CATALOG_VERSION_NO),
				 errhint("It looks like you need to initdb.")));
	if (ControlFile->maxAlign != MAXIMUM_ALIGNOF)
		ereport(FATAL,
				(errmsg("database files are incompatible with server"),
		   errdetail("The database cluster was initialized with MAXALIGN %d,"
					 " but the server was compiled with MAXALIGN %d.",
					 ControlFile->maxAlign, MAXIMUM_ALIGNOF),
				 errhint("It looks like you need to initdb.")));
	if (ControlFile->floatFormat != FLOATFORMAT_VALUE)
		ereport(FATAL,
				(errmsg("database files are incompatible with server"),
				 errdetail("The database cluster appears to use a different floating-point number format than the server executable."),
				 errhint("It looks like you need to initdb.")));
	if (ControlFile->blcksz != BLCKSZ)
		ereport(FATAL,
				(errmsg("database files are incompatible with server"),
			 errdetail("The database cluster was initialized with BLCKSZ %d,"
					   " but the server was compiled with BLCKSZ %d.",
					   ControlFile->blcksz, BLCKSZ),
				 errhint("It looks like you need to recompile or initdb.")));
	if (ControlFile->relseg_size != RELSEG_SIZE)
		ereport(FATAL,
				(errmsg("database files are incompatible with server"),
		errdetail("The database cluster was initialized with RELSEG_SIZE %d,"
				  " but the server was compiled with RELSEG_SIZE %d.",
				  ControlFile->relseg_size, RELSEG_SIZE),
				 errhint("It looks like you need to recompile or initdb.")));
	if (ControlFile->xlog_blcksz != XLOG_BLCKSZ)
		ereport(FATAL,
				(errmsg("database files are incompatible with server"),
		errdetail("The database cluster was initialized with XLOG_BLCKSZ %d,"
				  " but the server was compiled with XLOG_BLCKSZ %d.",
				  ControlFile->xlog_blcksz, XLOG_BLCKSZ),
				 errhint("It looks like you need to recompile or initdb.")));
	if (ControlFile->xlog_seg_size != XLOG_SEG_SIZE)
		ereport(FATAL,
				(errmsg("database files are incompatible with server"),
				 errdetail("The database cluster was initialized with XLOG_SEG_SIZE %d,"
					   " but the server was compiled with XLOG_SEG_SIZE %d.",
						   ControlFile->xlog_seg_size, XLOG_SEG_SIZE),
				 errhint("It looks like you need to recompile or initdb.")));
	if (ControlFile->nameDataLen != NAMEDATALEN)
		ereport(FATAL,
				(errmsg("database files are incompatible with server"),
		errdetail("The database cluster was initialized with NAMEDATALEN %d,"
				  " but the server was compiled with NAMEDATALEN %d.",
				  ControlFile->nameDataLen, NAMEDATALEN),
				 errhint("It looks like you need to recompile or initdb.")));
	if (ControlFile->indexMaxKeys != INDEX_MAX_KEYS)
		ereport(FATAL,
				(errmsg("database files are incompatible with server"),
				 errdetail("The database cluster was initialized with INDEX_MAX_KEYS %d,"
					  " but the server was compiled with INDEX_MAX_KEYS %d.",
						   ControlFile->indexMaxKeys, INDEX_MAX_KEYS),
				 errhint("It looks like you need to recompile or initdb.")));

#ifdef HAVE_INT64_TIMESTAMP
	if (ControlFile->enableIntTimes != TRUE)
		ereport(FATAL,
				(errmsg("database files are incompatible with server"),
				 errdetail("The database cluster was initialized without HAVE_INT64_TIMESTAMP"
				  " but the server was compiled with HAVE_INT64_TIMESTAMP."),
				 errhint("It looks like you need to recompile or initdb.")));
#else
	if (ControlFile->enableIntTimes != FALSE)
		ereport(FATAL,
				(errmsg("database files are incompatible with server"),
				 errdetail("The database cluster was initialized with HAVE_INT64_TIMESTAMP"
			   " but the server was compiled without HAVE_INT64_TIMESTAMP."),
				 errhint("It looks like you need to recompile or initdb.")));
#endif

	if (ControlFile->localeBuflen != LOCALE_NAME_BUFLEN)
		ereport(FATAL,
				(errmsg("database files are incompatible with server"),
				 errdetail("The database cluster was initialized with LOCALE_NAME_BUFLEN %d,"
				  " but the server was compiled with LOCALE_NAME_BUFLEN %d.",
						   ControlFile->localeBuflen, LOCALE_NAME_BUFLEN),
				 errhint("It looks like you need to recompile or initdb.")));
	if (pg_perm_setlocale(LC_COLLATE, ControlFile->lc_collate) == NULL)
		ereport(FATAL,
			(errmsg("database files are incompatible with operating system"),
			 errdetail("The database cluster was initialized with LC_COLLATE \"%s\","
					   " which is not recognized by setlocale().",
					   ControlFile->lc_collate),
			 errhint("It looks like you need to initdb or install locale support.")));
	if (pg_perm_setlocale(LC_CTYPE, ControlFile->lc_ctype) == NULL)
		ereport(FATAL,
			(errmsg("database files are incompatible with operating system"),
		errdetail("The database cluster was initialized with LC_CTYPE \"%s\","
				  " which is not recognized by setlocale().",
				  ControlFile->lc_ctype),
			 errhint("It looks like you need to initdb or install locale support.")));

	/* Make the fixed locale settings visible as GUC variables, too */
	SetConfigOption("lc_collate", ControlFile->lc_collate,
					PGC_INTERNAL, PGC_S_OVERRIDE);
	SetConfigOption("lc_ctype", ControlFile->lc_ctype,
					PGC_INTERNAL, PGC_S_OVERRIDE);

	if (!ControlFileWatcher->watcherInitialized)
	{
		ControlFileWatcherSaveInitial();
	}
	else
	{
		ControlFileWatcherCheckForChange();
	}
}

static bool
XLogGetWriteAndFlushedLoc(XLogRecPtr *writeLoc, XLogRecPtr *flushedLoc)
{
	/* use volatile pointer to prevent code rearrangement */
	volatile XLogCtlData *xlogctl = XLogCtl;

	SpinLockAcquire(&xlogctl->info_lck);
	*writeLoc = xlogctl->LogwrtResult.Write;
	*flushedLoc = xlogctl->LogwrtResult.Flush;
	SpinLockRelease(&xlogctl->info_lck);

	return (writeLoc->xlogid != 0 || writeLoc->xrecoff != 0);
}

void
UpdateControlFile(void)
{
	MirroredFlatFileOpen	mirroredOpen;

	INIT_CRC32C(ControlFile->crc);
	COMP_CRC32C(ControlFile->crc,
				   (char *) ControlFile,
				   offsetof(ControlFileData, crc));
	FIN_CRC32C(ControlFile->crc);

	MirroredFlatFile_Open(
					&mirroredOpen,
					XLOG_CONTROL_FILE_SUBDIR,
					XLOG_CONTROL_FILE_SIMPLE,
					O_RDWR | PG_BINARY,
					S_IRUSR | S_IWUSR,
					/* suppressError */ false,
					/* atomic operation */ false,
					/*isMirrorRecovery */ false);

	MirroredFlatFile_Write(
					&mirroredOpen,
					0,
					ControlFile,
					PG_CONTROL_SIZE,
					/* suppressError */ false);

	MirroredFlatFile_Flush(
					&mirroredOpen,
					/* suppressError */ false);

	MirroredFlatFile_Close(&mirroredOpen);

	Assert (ControlFileWatcher->watcherInitialized);

	ControlFileWatcherCheckForChange();
}

/*
 * Initialization of shared memory for XLOG
 */
Size
XLOGShmemSize(void)
{
	Size		size;

	/* XLogCtl */
	size = sizeof(XLogCtlData);
	/* xlblocks array */
	size = add_size(size, mul_size(sizeof(XLogRecPtr), XLOGbuffers));
	/* extra alignment padding for XLOG I/O buffers */
	size = add_size(size, ALIGNOF_XLOG_BUFFER);
	/* and the buffers themselves */
	size = add_size(size, mul_size(XLOG_BLCKSZ, XLOGbuffers));

	/*
	 * Note: we don't count ControlFileData, it comes out of the "slop factor"
	 * added by CreateSharedMemoryAndSemaphores.  This lets us use this
	 * routine again below to compute the actual allocation size.
	 */

	/*
	 * Similary, we also don't PgControlWatch for the above reasons, too.
	 */

	return size;
}

void
XLOGShmemInit(void)
{
	bool		foundCFile,
				foundXLog,
				foundCFileWatcher;
	char	   *allocptr;

	ControlFile = (ControlFileData *)
		ShmemInitStruct("Control File", sizeof(ControlFileData), &foundCFile);
	ControlFileWatcher = (ControlFileWatch *)
		ShmemInitStruct("Control File Watcher", sizeof(ControlFileWatch), &foundCFileWatcher);
	XLogCtl = (XLogCtlData *)
		ShmemInitStruct("XLOG Ctl", XLOGShmemSize(), &foundXLog);

	if (foundCFile || foundXLog || foundCFileWatcher)
	{
		/* both should be present or neither */
		Assert(foundCFile && foundXLog && foundCFileWatcher);
		return;
	}

	memset(XLogCtl, 0, sizeof(XLogCtlData));

	/*
	 * Since XLogCtlData contains XLogRecPtr fields, its sizeof should be a
	 * multiple of the alignment for same, so no extra alignment padding is
	 * needed here.
	 */
	allocptr = ((char *) XLogCtl) + sizeof(XLogCtlData);
	XLogCtl->xlblocks = (XLogRecPtr *) allocptr;
	memset(XLogCtl->xlblocks, 0, sizeof(XLogRecPtr) * XLOGbuffers);
	allocptr += sizeof(XLogRecPtr) * XLOGbuffers;

	/*
	 * Align the start of the page buffers to an ALIGNOF_XLOG_BUFFER boundary.
	 */
	allocptr = (char *) TYPEALIGN(ALIGNOF_XLOG_BUFFER, allocptr);
	XLogCtl->pages = allocptr;
	memset(XLogCtl->pages, 0, (Size) XLOG_BLCKSZ * XLOGbuffers);

	/*
	 * Do basic initialization of XLogCtl shared data. (StartupXLOG will fill
	 * in additional info.)
	 */
	XLogCtl->XLogCacheByte = (Size) XLOG_BLCKSZ *XLOGbuffers;

	XLogCtl->XLogCacheBlck = XLOGbuffers - 1;
	XLogCtl->Insert.currpage = (XLogPageHeader) (XLogCtl->pages);
	SpinLockInit(&XLogCtl->info_lck);

	XLogCtl->haveLastCheckpointLoc = false;
	memset(&XLogCtl->lastCheckpointLoc, 0, sizeof(XLogRecPtr));
	memset(&XLogCtl->lastCheckpointEndLoc, 0, sizeof(XLogRecPtr));

	SpinLockInit(&XLogCtl->resynchronize_lck);
}

/**
 * This should be called when we are sure that it is safe to try to read the control file and BEFORE
 *  we have launched any child processes that need access to collation and ctype data.
 *
 * It is not safe to read the control file on a mirror because it may not be synchronized
 */
void
XLogStartupInit(void)
{
	if (!IsBootstrapProcessingMode())
		ReadControlFile();
}

/*
 * This func must be called ONCE on system install.  It creates pg_control
 * and the initial XLOG segment.
 */
void
BootStrapXLOG(void)
{
	CheckPoint	checkPoint;
	char	   *buffer;
	XLogPageHeader page;
	XLogLongPageHeader longpage;
	XLogRecord *record;
	bool		use_existent;
	uint64		sysidentifier;
	struct timeval tv;
	pg_crc32	crc;

	/*
	 * Select a hopefully-unique system identifier code for this installation.
	 * We use the result of gettimeofday(), including the fractional seconds
	 * field, as being about as unique as we can easily get.  (Think not to
	 * use random(), since it hasn't been seeded and there's no portable way
	 * to seed it other than the system clock value...)  The upper half of the
	 * uint64 value is just the tv_sec part, while the lower half is the XOR
	 * of tv_sec and tv_usec.  This is to ensure that we don't lose uniqueness
	 * unnecessarily if "uint64" is really only 32 bits wide.  A person
	 * knowing this encoding can determine the initialization time of the
	 * installation, which could perhaps be useful sometimes.
	 */
	gettimeofday(&tv, NULL);
	sysidentifier = ((uint64) tv.tv_sec) << 32;
	sysidentifier |= (uint32) (tv.tv_sec | tv.tv_usec);

	/* First timeline ID is always 1 */
	ThisTimeLineID = 1;

	/* page buffer must be aligned suitably for O_DIRECT */
	buffer = (char *) palloc(XLOG_BLCKSZ + ALIGNOF_XLOG_BUFFER);
	page = (XLogPageHeader) TYPEALIGN(ALIGNOF_XLOG_BUFFER, buffer);
	memset(page, 0, XLOG_BLCKSZ);

	/* Set up information for the initial checkpoint record */
	checkPoint.redo.xlogid = 0;
	checkPoint.redo.xrecoff = SizeOfXLogLongPHD;
	checkPoint.undo = checkPoint.redo;
	checkPoint.ThisTimeLineID = ThisTimeLineID;
	checkPoint.nextXidEpoch = 0;
	checkPoint.nextXid = FirstNormalTransactionId;
	checkPoint.nextOid = FirstBootstrapObjectId;
	checkPoint.nextMulti = FirstMultiXactId;
	checkPoint.nextMultiOffset = 0;
	checkPoint.time = time(NULL);

	ShmemVariableCache->nextXid = checkPoint.nextXid;
	ShmemVariableCache->nextOid = checkPoint.nextOid;
	ShmemVariableCache->oidCount = 0;
	MultiXactSetNextMXact(checkPoint.nextMulti, checkPoint.nextMultiOffset);

	/* Set up the XLOG page header */
	page->xlp_magic = XLOG_PAGE_MAGIC;
	page->xlp_info = XLP_LONG_HEADER;
	page->xlp_tli = ThisTimeLineID;
	page->xlp_pageaddr.xlogid = 0;
	page->xlp_pageaddr.xrecoff = 0;
	longpage = (XLogLongPageHeader) page;
	longpage->xlp_sysid = sysidentifier;
	longpage->xlp_seg_size = XLogSegSize;
	longpage->xlp_xlog_blcksz = XLOG_BLCKSZ;

	/* Insert the initial checkpoint record */
	record = (XLogRecord *) ((char *) page + SizeOfXLogLongPHD);
	record->xl_prev.xlogid = 0;
	record->xl_prev.xrecoff = 0;
	record->xl_xid = InvalidTransactionId;
	record->xl_tot_len = SizeOfXLogRecord + sizeof(checkPoint);
	record->xl_len = sizeof(checkPoint);
	record->xl_info = XLOG_CHECKPOINT_SHUTDOWN;
	record->xl_rmid = RM_XLOG_ID;
	memcpy(XLogRecGetData(record), &checkPoint, sizeof(checkPoint));

	INIT_CRC32C(crc);
 	COMP_CRC32C(crc, &checkPoint, sizeof(checkPoint));
	COMP_CRC32C(crc, (char *) record + sizeof(pg_crc32),
			   SizeOfXLogRecord - sizeof(pg_crc32));
	FIN_CRC32C(crc);

	record->xl_crc = crc;

	/* Create first XLOG segment file */
	use_existent = false;
	XLogFileInit(
		&mirroredLogFileOpen,
		0, 0, &use_existent, false);

	/* Write the first page with the initial record */
	errno = 0;
	if (MirroredFlatFile_Append(
			&mirroredLogFileOpen,
			page,
			XLOG_BLCKSZ,
			/* suppressError */ true))
	{
		ereport(PANIC,
				(errcode_for_file_access(),
			  errmsg("could not write bootstrap transaction log file: %m")));
	}

	if (MirroredFlatFile_Flush(
			&mirroredLogFileOpen,
			/* suppressError */ true))
		ereport(PANIC,
				(errcode_for_file_access(),
			  errmsg("could not fsync bootstrap transaction log file: %m")));

	MirroredFlatFile_Close(
			&mirroredLogFileOpen);

	/* Now create pg_control */

	memset(ControlFile, 0, sizeof(ControlFileData));
	/* Initialize pg_control status fields */
	ControlFile->system_identifier = sysidentifier;
	ControlFile->state = DB_SHUTDOWNED;
	ControlFile->time = checkPoint.time;
	ControlFile->logId = 0;
	ControlFile->logSeg = 1;
	ControlFile->checkPoint = checkPoint.redo;
	ControlFile->checkPointCopy = checkPoint;
	/* some additional ControlFile fields are set in WriteControlFile() */

	WriteControlFile();

	/* Bootstrap the commit log, too */
	BootStrapCLOG();
	BootStrapSUBTRANS();
	BootStrapMultiXact();

	pfree(buffer);
}

static char *
str_time(pg_time_t tnow)
{
	static char buf[128];

	pg_strftime(buf, sizeof(buf),
			 /* Win32 timezone names are too long so don't print them */
#ifndef WIN32
			 "%Y-%m-%d %H:%M:%S %Z",
#else
			 "%Y-%m-%d %H:%M:%S",
#endif
			 pg_localtime(&tnow, log_timezone ? log_timezone : gmt_timezone));

	return buf;
}

static bool StandbyTakeover = false;

/*
 * See if there is a recovery command file (recovery.conf) indicating
 * there was a standby master that is being started as the primary.
 *
 * This is basically the first part of the XLogReadRecoveryCommandFile routine.
 */
static bool
StandbyTakeoverTest(void)
{
	FILE	   *fd;

	fd = AllocateFile(RECOVERY_COMMAND_FILE, "r");
	if (fd == NULL)
	{
		if (errno == ENOENT)
			return false;				/* not there */
		ereport(FATAL,
				(errcode_for_file_access(),
				 errmsg("could not open recovery command file \"%s\": %m",
						RECOVERY_COMMAND_FILE)));
	}
	ereport(LOG,
		(errmsg("found recovery.conf file indicating standby takeover recovery needed")));


	return true;
}

static void
exitStandbyTakeover(void)
{
	/*
	 * Rename the config file out of the way, so that we don't accidentally
	 * re-enter recovery mode in a subsequent crash.
	 */
	unlink(RECOVERY_COMMAND_DONE);
	if (rename(RECOVERY_COMMAND_FILE, RECOVERY_COMMAND_DONE) != 0)
		ereport(FATAL,
				(errcode_for_file_access(),
				 errmsg("could not rename file \"%s\" to \"%s\": %m",
						RECOVERY_COMMAND_FILE, RECOVERY_COMMAND_DONE)));

	ereport(LOG,
			(errmsg("standby takeover recovery complete")));
}

/*
 * See if there is a recovery command file (recovery.conf), and if so
 * read in parameters for archive recovery.
 *
 * XXX longer term intention is to expand this to
 * cater for additional parameters and controls
 * possibly use a flex lexer similar to the GUC one
 */
void
XLogReadRecoveryCommandFile(int emode)
{
	FILE	   *fd;
	char		cmdline[MAXPGPATH];
	TimeLineID	rtli = 0;
	bool		rtliGiven = false;
	bool		syntaxError = false;

	fd = AllocateFile(RECOVERY_COMMAND_FILE, "r");
	if (fd == NULL)
	{
		if (errno == ENOENT)
			return;				/* not there, so no archive recovery */
		ereport(FATAL,
				(errcode_for_file_access(),
				 errmsg("could not open recovery command file \"%s\": %m",
						RECOVERY_COMMAND_FILE)));
	}

	ereport(emode,
			(errmsg("starting archive recovery")));

	/*
	 * Parse the file...
	 */
	while (fgets(cmdline, MAXPGPATH, fd) != NULL)
	{
		/* skip leading whitespace and check for # comment */
		char	   *ptr;
		char	   *tok1;
		char	   *tok2;

		for (ptr = cmdline; *ptr; ptr++)
		{
			if (!isspace((unsigned char) *ptr))
				break;
		}
		if (*ptr == '\0' || *ptr == '#')
			continue;

		/* identify the quoted parameter value */
		tok1 = strtok(ptr, "'");
		if (!tok1)
		{
			syntaxError = true;
			break;
		}
		tok2 = strtok(NULL, "'");
		if (!tok2)
		{
			syntaxError = true;
			break;
		}
		/* reparse to get just the parameter name */
		tok1 = strtok(ptr, " \t=");
		if (!tok1)
		{
			syntaxError = true;
			break;
		}

		if (strcmp(tok1, "restore_command") == 0)
		{
			recoveryRestoreCommand = pstrdup(tok2);
			ereport(emode,
					(errmsg("restore_command = \"%s\"",
							recoveryRestoreCommand)));
		}
		else if (strcmp(tok1, "recovery_target_timeline") == 0)
		{
			rtliGiven = true;
			if (strcmp(tok2, "latest") == 0)
				rtli = 0;
			else
			{
				errno = 0;
				rtli = (TimeLineID) strtoul(tok2, NULL, 0);
				if (errno == EINVAL || errno == ERANGE)
					ereport(FATAL,
							(errmsg("recovery_target_timeline is not a valid number: \"%s\"",
									tok2)));
			}
			if (rtli)
				ereport(emode,
						(errmsg("recovery_target_timeline = %u", rtli)));
			else
				ereport(emode,
						(errmsg("recovery_target_timeline = latest")));
		}
		else if (strcmp(tok1, "recovery_target_xid") == 0)
		{
			errno = 0;
			recoveryTargetXid = (TransactionId) strtoul(tok2, NULL, 0);
			if (errno == EINVAL || errno == ERANGE)
				ereport(FATAL,
				 (errmsg("recovery_target_xid is not a valid number: \"%s\"",
						 tok2)));
			ereport(emode,
					(errmsg("recovery_target_xid = %u",
							recoveryTargetXid)));
			recoveryTarget = true;
			recoveryTargetExact = true;
		}
		else if (strcmp(tok1, "recovery_target_time") == 0)
		{
			/*
			 * if recovery_target_xid specified, then this overrides
			 * recovery_target_time
			 */
			if (recoveryTargetExact)
				continue;
			recoveryTarget = true;
			recoveryTargetExact = false;

			/*
			 * Convert the time string given by the user to the time_t format.
			 * We use type abstime's input converter because we know abstime
			 * has the same representation as time_t.
			 */
			recoveryTargetTime = (time_t)
				DatumGetAbsoluteTime(DirectFunctionCall1(abstimein,
													 CStringGetDatum(tok2)));
			ereport(emode,
					(errmsg("recovery_target_time = %s",
							DatumGetCString(DirectFunctionCall1(abstimeout,
				AbsoluteTimeGetDatum((AbsoluteTime) recoveryTargetTime))))));
		}
		else if (strcmp(tok1, "recovery_target_inclusive") == 0)
		{
			/*
			 * does nothing if a recovery_target is not also set
			 */
			if (strcmp(tok2, "true") == 0)
				recoveryTargetInclusive = true;
			else
			{
				recoveryTargetInclusive = false;
				tok2 = "false";
			}
			ereport(emode,
					(errmsg("recovery_target_inclusive = %s", tok2)));
		}
		else
			ereport(FATAL,
					(errmsg("unrecognized recovery parameter \"%s\"",
							tok1)));
	}

	FreeFile(fd);

	if (syntaxError)
		ereport(FATAL,
				(errmsg("syntax error in recovery command file: %s",
						cmdline),
			  errhint("Lines should have the format parameter = 'value'.")));

	/* Check that required parameters were supplied */
	if (recoveryRestoreCommand == NULL)
		ereport(FATAL,
				(errmsg("recovery command file \"%s\" did not specify restore_command",
						RECOVERY_COMMAND_FILE)));

	/* Enable fetching from archive recovery area */
	InArchiveRecovery = true;

	/*
	 * If user specified recovery_target_timeline, validate it or compute the
	 * "latest" value.	We can't do this until after we've gotten the restore
	 * command and set InArchiveRecovery, because we need to fetch timeline
	 * history files from the archive.
	 */
	if (rtliGiven)
	{
		if (rtli)
		{
			/* Timeline 1 does not have a history file, all else should */
			if (rtli != 1 && !existsTimeLineHistory(rtli))
				ereport(FATAL,
						(errmsg("recovery_target_timeline %u does not exist",
								rtli)));
			recoveryTargetTLI = rtli;
		}
		else
		{
			/* We start the "latest" search from pg_control's timeline */
			recoveryTargetTLI = findNewestTimeLine(recoveryTargetTLI);
		}
	}

	elog((Debug_print_qd_mirroring ? LOG : DEBUG5),
		 "finishing XLogReadRecoveryCommandFile recoveryTargetTLI %d",
		 recoveryTargetTLI);
}

/*
 * Exit archive-recovery state
 */
static void
exitArchiveRecovery(TimeLineID endTLI, uint32 endLogId, uint32 endLogSeg)
{
	char		recoveryPath[MAXPGPATH];
	char		xlogpath[MAXPGPATH];
	char		*xlogDir = NULL;

	/*
	 * We are no longer in archive recovery state.
	 */
	InArchiveRecovery = false;

	/*
	 * We should have the ending log segment currently open.  Verify, and then
	 * close it (to avoid problems on Windows with trying to rename or delete
	 * an open file).
	 */
	Assert(readFile >= 0);
	Assert(readId == endLogId);
	Assert(readSeg == endLogSeg);

	close(readFile);
	readFile = -1;

	/*
	 * If the segment was fetched from archival storage, we want to replace
	 * the existing xlog segment (if any) with the archival version.  This is
	 * because whatever is in XLOGDIR is very possibly older than what we have
	 * from the archives, since it could have come from restoring a PGDATA
	 * backup.	In any case, the archival version certainly is more
	 * descriptive of what our current database state is, because that is what
	 * we replayed from.
	 *
	 * Note that if we are establishing a new timeline, ThisTimeLineID is
	 * already set to the new value, and so we will create a new file instead
	 * of overwriting any existing file.  (This is, in fact, always the case
	 * at present.)
	 */
	xlogDir = makeRelativeToTxnFilespace(XLOGDIR);
	if (snprintf(recoveryPath, MAXPGPATH, "%s/RECOVERYXLOG", xlogDir) > MAXPGPATH)
	{
		ereport(ERROR, (errmsg("cannot generate path %s/RECOVERYXLOG", xlogDir)));	
	}
	XLogFilePath(xlogpath, ThisTimeLineID, endLogId, endLogSeg);

	if (restoredFromArchive)
	{
		ereport(DEBUG3,
				(errmsg_internal("moving last restored xlog to \"%s\"",
								 xlogpath)));
		unlink(xlogpath);		/* might or might not exist */
		if (rename(recoveryPath, xlogpath) != 0)
			ereport(FATAL,
					(errcode_for_file_access(),
					 errmsg("could not rename file \"%s\" to \"%s\": %m",
							recoveryPath, xlogpath)));
		/* XXX might we need to fix permissions on the file? */
	}
	else
	{
		/*
		 * If the latest segment is not archival, but there's still a
		 * RECOVERYXLOG laying about, get rid of it.
		 */
		unlink(recoveryPath);	/* ignore any error */

		/*
		 * If we are establishing a new timeline, we have to copy data from
		 * the last WAL segment of the old timeline to create a starting WAL
		 * segment for the new timeline.
		 */
		if (endTLI != ThisTimeLineID)
			XLogFileCopy(endLogId, endLogSeg,
						 endTLI, endLogId, endLogSeg);
	}

	/*
	 * Let's just make real sure there are not .ready or .done flags posted
	 * for the new segment.
	 */
	XLogFileName(xlogpath, ThisTimeLineID, endLogId, endLogSeg);
	XLogArchiveCleanup(xlogpath);

	/* Get rid of any remaining recovered timeline-history file, too */
	if (snprintf(recoveryPath, MAXPGPATH, "%s/RECOVERYHISTORY", xlogDir) > MAXPGPATH)
	{
		ereport(ERROR, (errmsg("cannot generate path %s/RECOVERYHISTORY", xlogDir)));
	}
	unlink(recoveryPath);		/* ignore any error */

	/*
	 * Rename the config file out of the way, so that we don't accidentally
	 * re-enter archive recovery mode in a subsequent crash.
	 */
	unlink(RECOVERY_COMMAND_DONE);
	if (rename(RECOVERY_COMMAND_FILE, RECOVERY_COMMAND_DONE) != 0)
		ereport(FATAL,
				(errcode_for_file_access(),
				 errmsg("could not rename file \"%s\" to \"%s\": %m",
						RECOVERY_COMMAND_FILE, RECOVERY_COMMAND_DONE)));

	ereport(LOG,
			(errmsg("archive recovery complete")));
	pfree(xlogDir);
}

/*
 * For point-in-time recovery, this function decides whether we want to
 * stop applying the XLOG at or after the current record.
 *
 * Returns TRUE if we are stopping, FALSE otherwise.  On TRUE return,
 * *includeThis is set TRUE if we should apply this record before stopping.
 * Also, some information is saved in recoveryStopXid et al for use in
 * annotating the new timeline's history file.
 */
static bool
recoveryStopsHere(XLogRecord *record, bool *includeThis)
{
	bool		stopsHere;
	uint8		record_info;
	time_t		recordXtime;

	/* Do we have a PITR target at all? */
	if (!recoveryTarget)
		return false;

	/* We only consider stopping at COMMIT or ABORT records */
	if (record->xl_rmid != RM_XACT_ID)
		return false;
	record_info = record->xl_info & ~XLR_INFO_MASK;
	if (record_info == XLOG_XACT_COMMIT)
	{
		xl_xact_commit *recordXactCommitData;

		recordXactCommitData = (xl_xact_commit *) XLogRecGetData(record);
		recordXtime = recordXactCommitData->xtime;
	}
	else if (record_info == XLOG_XACT_ABORT)
	{
		xl_xact_abort *recordXactAbortData;

		recordXactAbortData = (xl_xact_abort *) XLogRecGetData(record);
		recordXtime = recordXactAbortData->xtime;
	}
	else
		return false;

	if (recoveryTargetExact)
	{
		/*
		 * there can be only one transaction end record with this exact
		 * transactionid
		 *
		 * when testing for an xid, we MUST test for equality only, since
		 * transactions are numbered in the order they start, not the order
		 * they complete. A higher numbered xid will complete before you about
		 * 50% of the time...
		 */
		stopsHere = (record->xl_xid == recoveryTargetXid);
		if (stopsHere)
			*includeThis = recoveryTargetInclusive;
	}
	else
	{
		/*
		 * there can be many transactions that share the same commit time, so
		 * we stop after the last one, if we are inclusive, or stop at the
		 * first one if we are exclusive
		 */
		if (recoveryTargetInclusive)
			stopsHere = (recordXtime > recoveryTargetTime);
		else
			stopsHere = (recordXtime >= recoveryTargetTime);
		if (stopsHere)
			*includeThis = false;
	}

	if (stopsHere)
	{
		recoveryStopXid = record->xl_xid;
		recoveryStopTime = recordXtime;
		recoveryStopAfter = *includeThis;

		if (record_info == XLOG_XACT_COMMIT)
		{
			if (recoveryStopAfter)
				ereport(LOG,
						(errmsg("recovery stopping after commit of transaction %u, time %s",
							  recoveryStopXid, str_time(recoveryStopTime))));
			else
				ereport(LOG,
						(errmsg("recovery stopping before commit of transaction %u, time %s",
							  recoveryStopXid, str_time(recoveryStopTime))));
		}
		else
		{
			if (recoveryStopAfter)
				ereport(LOG,
						(errmsg("recovery stopping after abort of transaction %u, time %s",
							  recoveryStopXid, str_time(recoveryStopTime))));
			else
				ereport(LOG,
						(errmsg("recovery stopping before abort of transaction %u, time %s",
							  recoveryStopXid, str_time(recoveryStopTime))));
		}
	}

	return stopsHere;
}

static void
printEndOfXLogFile(XLogRecPtr	*loc)
{
	uint32 seg = loc->xrecoff / XLogSegSize;

	XLogRecPtr roundedDownLoc;

	XLogRecord *record;
	XLogRecPtr	LastRec;

	/*
	 * Go back to the beginning of the log file and read forward to find
	 * the end of the transaction log.
	 */
	roundedDownLoc.xlogid = loc->xlogid;
	roundedDownLoc.xrecoff = (seg * XLogSegSize) + SizeOfXLogLongPHD;

	XLogCloseReadRecord();

	record = XLogReadRecord(&roundedDownLoc, LOG);
	if (record == NULL)
	{
		elog(LOG,"Couldn't read transaction log file (logid %d, seg %d)",
			 loc->xlogid, seg);
		return;
	}

	do
	{
		LastRec = ReadRecPtr;

		record = XLogReadRecord(NULL, DEBUG5);
	} while (record != NULL);

	record = XLogReadRecord(&LastRec, ERROR);

	elog(LOG,"found end of transaction log file %s",
		 XLogLocationToString_Long(&EndRecPtr));

	XLogCloseReadRecord();
}

static void ShutdownReadFile(void)
{
	/* Shut down readFile facility, free space */
	if (readFile >= 0)
	{
		close(readFile);
		readFile = -1;
	}
	if (readBuf)
	{
		free(readBuf);
		readBuf = NULL;
	}
	if (readRecordBuf)
	{
		free(readRecordBuf);
		readRecordBuf = NULL;
		readRecordBufSize = 0;
	}
}

static void
StartupXLOG_InProduction(void)
{
	TransactionId oldestActiveXID;

	/* Pre-scan prepared transactions to find out the range of XIDs present */
	oldestActiveXID = PrescanPreparedTransactions();

	elog(LOG, "Oldest active transaction from prepared transactions %u", oldestActiveXID);

	/* Start up the commit log and related stuff, too */
	StartupCLOG();
	StartupSUBTRANS(oldestActiveXID);
	StartupMultiXact();

	/* Reload shared-memory state for prepared transactions */
	RecoverPreparedTransactions();

	/*
	 * Perform a checkpoint to update all our recovery activity to disk.
	 *
	 * Note that we write a shutdown checkpoint rather than an on-line
	 * one. This is not particularly critical, but since we may be
	 * assigning a new TLI, using a shutdown checkpoint allows us to have
	 * the rule that TLI only changes in shutdown checkpoints, which
	 * allows some extra error checking in xlog_redo.
	 */
	CreateCheckPoint(true, true);

	ControlFile->state = DB_IN_PRODUCTION;
	ControlFile->time = time(NULL);
	UpdateControlFile();

	ereport(LOG,
			(errmsg("database system is ready")));

	{
		char version[512];

		StrNCpy(version, PG_VERSION_STR " compiled on " __DATE__ " " __TIME__, sizeof(version));

#ifdef USE_ASSERT_CHECKING
		strcat(version, " (with assert checking)");
#endif
		ereport(LOG,(errmsg("%s", version)));

	}

	BuildFlatFiles(false);

}

/*
 * Error context callback for tracing or errors occurring during PASS 1 redo.
 */
static void
StartupXLOG_RedoPass1Context(void *arg)
{
	XLogRecord		*record = (XLogRecord*) arg;

	StringInfoData buf;

	initStringInfo(&buf);
	appendStringInfo(&buf, "REDO PASS 1 @ %s; LSN %s: ",
					 XLogLocationToString(&ReadRecPtr),
					 XLogLocationToString2(&EndRecPtr));
	XLog_OutRec(&buf, record);
	appendStringInfo(&buf, " - ");
	RmgrTable[record->xl_rmid].rm_desc(&buf,
									   ReadRecPtr,
									   record);

	errcontext("%s", buf.data);

	pfree(buf.data);
}

/*
 * Error context callback for tracing or errors occurring during PASS 1 redo.
 */
static void
StartupXLOG_RedoPass3Context(void *arg)
{
	XLogRecord		*record = (XLogRecord*) arg;

	StringInfoData buf;

	initStringInfo(&buf);
	appendStringInfo(&buf, "REDO PASS 3 @ %s; LSN %s: ",
					 XLogLocationToString(&ReadRecPtr),
					 XLogLocationToString2(&EndRecPtr));
	XLog_OutRec(&buf, record);
	appendStringInfo(&buf, " - ");
	RmgrTable[record->xl_rmid].rm_desc(&buf,
									   ReadRecPtr,
									   record);

	errcontext("%s", buf.data);

	pfree(buf.data);
}


static void
ApplyStartupRedo(
	XLogRecPtr		*beginLoc,

	XLogRecPtr		*lsn,

	XLogRecord		*record)
{
	MIRROREDLOCK_BUFMGR_VERIFY_NO_LOCK_LEAK_DECLARE;
	RedoErrorCallBack redoErrorCallBack;

	ErrorContextCallback errcontext;

	MIRROREDLOCK_BUFMGR_VERIFY_NO_LOCK_LEAK_ENTER;

	/* Setup error traceback support for ereport() */
	redoErrorCallBack.location = *beginLoc;
	redoErrorCallBack.record = record;

	errcontext.callback = rm_redo_error_callback;
	errcontext.arg = (void *) &redoErrorCallBack;
	errcontext.previous = error_context_stack;
	error_context_stack = &errcontext;

	/* nextXid must be beyond record's xid */
	if (TransactionIdFollowsOrEquals(record->xl_xid,
									 ShmemVariableCache->nextXid))
	{
		ShmemVariableCache->nextXid = record->xl_xid;
		TransactionIdAdvance(ShmemVariableCache->nextXid);
	}

	if (record->xl_info & XLR_BKP_BLOCK_MASK)
		RestoreBkpBlocks(record, *lsn);

	RmgrTable[record->xl_rmid].rm_redo(*beginLoc, *lsn, record);

	/* Pop the error context stack */
	error_context_stack = errcontext.previous;

	MIRROREDLOCK_BUFMGR_VERIFY_NO_LOCK_LEAK_EXIT;

}

/*
 * This must be called ONCE during postmaster or standalone-backend startup
 *
 *	How Recovery works ?
 *---------------------------------------------------------------
 *| Clean Shutdown case    	| 	Not Clean Shutdown  case|
 *|(InRecovery = false)		|	(InRecovery = true)	|
 *---------------------------------------------------------------
 *|				|		   |		|
 *|				|record after	   |record after|
 *|				|checkpoint =	   |checkpoint =|
 *|				|NULL		   |NOT NULL	|
 *|				|(bypass Redo  	   |(dont bypass|
 *|				|  	   	   |Redo	|
 *---------------------------------------------------------------
 *|				|		   |		|
 *|	No Redo			|No Redo	   |Redo done	|
 *|	No Recovery Passes	|Recovery Pass done|Recovery 	|
 *|				|		   |Pass done	|
 *---------------------------------------------------------------
 */


void
StartupXLOG(void)
{
	XLogCtlInsert *Insert;
	CheckPoint	checkPoint;
	bool		wasShutdown;
	bool		reachedStopPoint = false;
	bool		haveBackupLabel = false;
	XLogRecPtr	RecPtr,
				LastRec,
				checkPointLoc,
				minRecoveryLoc,
				EndOfLog;
	uint32		endLogId;
	uint32		endLogSeg;
	XLogRecord *record;
	uint32		freespace;
	bool		multipleRecoveryPassesNeeded = false;


#if 0
        /* KAS debugging */
        int temp1 = 0;
        while (temp1 == 0)
	  {
	    sleep(1);
	  }
#endif

	/*
	 * Read control file and check XLOG status looks valid.
	 *
	 * Note: in most control paths, *ControlFile is already valid and we need
	 * not do ReadControlFile() here, but might as well do it to be sure.
	 */
	ReadControlFile();

	if (ControlFile->logSeg == 0 ||
		ControlFile->state < DB_SHUTDOWNED ||
		ControlFile->state > DB_IN_PRODUCTION ||
		!XRecOffIsValid(ControlFile->checkPoint.xrecoff))
		ereport(FATAL,
				(errmsg("control file contains invalid data")));

	if (ControlFile->state == DB_SHUTDOWNED)
		ereport(LOG,
				(errmsg("database system was shut down at %s",
						str_time(ControlFile->time))));
	else if (ControlFile->state == DB_SHUTDOWNING)
		ereport(LOG,
				(errmsg("database system shutdown was interrupted at %s",
						str_time(ControlFile->time))));
	else if (ControlFile->state == DB_IN_CRASH_RECOVERY)
		ereport(LOG,
		   (errmsg("database system was interrupted while in recovery at %s",
				   str_time(ControlFile->time)),
			errhint("This probably means that some data is corrupted and"
					" you will have to use the last backup for recovery."),
			errSendAlert(true)));
	else if (ControlFile->state == DB_IN_ARCHIVE_RECOVERY)
		ereport(LOG,
				(errmsg("database system was interrupted while in recovery at log time %s",
						str_time(ControlFile->checkPointCopy.time)),
				 errhint("If this has occurred more than once some data may be corrupted"
				" and you may need to choose an earlier recovery target."),
				 errSendAlert(true)));
	else if (ControlFile->state == DB_IN_PRODUCTION)
		ereport(LOG,
				(errmsg("database system was interrupted at %s",
						str_time(ControlFile->time))));

	/* This is just to allow attaching to startup process with a debugger */
#ifdef XLOG_REPLAY_DELAY
	if (ControlFile->state != DB_SHUTDOWNED)
		pg_usleep(60000000L);
#endif

	/*
	 * Initialize on the assumption we want to recover to the same timeline
	 * that's active according to pg_control.
	 */
	recoveryTargetTLI = ControlFile->checkPointCopy.ThisTimeLineID;

	/*
	 * Check for recovery control file, and if so set up state for offline
	 * recovery
     *
	 * GP: We do not support offline recovery. The effect of not calling
	 * the XLogReadRecoveryCommandFile routine is InArchiveRecovery will be
	 * false.  This will prevent later mischief in the code sometimes tries
	 * to increment the timeline.  For GP, our timeline is always 1.
	 */
//	XLogReadRecoveryCommandFile(LOG);
	StandbyTakeover = StandbyTakeoverTest();

	/* Now we can determine the list of expected TLIs */
	expectedTLIs = XLogReadTimeLineHistory(recoveryTargetTLI);

	/*
	 * If pg_control's timeline is not in expectedTLIs, then we cannot
	 * proceed: the backup is not part of the history of the requested
	 * timeline.
	 */
	if (!list_member_int(expectedTLIs,
						 (int) ControlFile->checkPointCopy.ThisTimeLineID))
		ereport(FATAL,
				(errmsg("requested timeline %u is not a child of database system timeline %u",
						recoveryTargetTLI,
						ControlFile->checkPointCopy.ThisTimeLineID)));

	if (read_backup_label(&checkPointLoc, &minRecoveryLoc))
	{
		/*
		 * When a backup_label file is present, we want to roll forward from
		 * the checkpoint it identifies, rather than using pg_control.
		 */
		record = ReadCheckpointRecord(checkPointLoc, 0);
		if (record != NULL)
		{
			ereport(LOG,
					(errmsg("checkpoint record is at %X/%X",
							checkPointLoc.xlogid, checkPointLoc.xrecoff)));
			InRecovery = true;	/* force recovery even if SHUTDOWNED */
		}
		else
		{
			ereport(PANIC,
					(errmsg("could not locate required checkpoint record"),
					 errhint("If you are not restoring from a backup, try removing the file \"%s/backup_label\".", DataDir)));
		}
		/* set flag to delete it later */
		haveBackupLabel = true;
	}
	else
	{
		/*
		 * Get the last valid checkpoint record.  If the latest one according
		 * to pg_control is broken, try the next-to-last one.
		 */
		checkPointLoc = ControlFile->checkPoint;

		record = ReadCheckpointRecord(checkPointLoc, 1);
		if (record != NULL)
		{
			ereport(LOG,
					(errmsg("checkpoint record is at %X/%X",
							checkPointLoc.xlogid, checkPointLoc.xrecoff)));
		}
		else
		{
			printEndOfXLogFile(&checkPointLoc);

			checkPointLoc = ControlFile->prevCheckPoint;
			record = ReadCheckpointRecord(checkPointLoc, 2);
			if (record != NULL)
			{
				ereport(LOG,
						(errmsg("using previous checkpoint record at %X/%X",
							  checkPointLoc.xlogid, checkPointLoc.xrecoff)));
				InRecovery = true;		/* force recovery even if SHUTDOWNED */
			}
			else
			{
				printEndOfXLogFile(&checkPointLoc);
				ereport(PANIC,
					 (errmsg("could not locate a valid checkpoint record")));
			}
		}
	}

	XLogCtl->pass1LastCheckpointLoc = checkPointLoc;

	LastRec = RecPtr = checkPointLoc;
	memcpy(&checkPoint, XLogRecGetData(record), sizeof(CheckPoint));
	wasShutdown = (record->xl_info == XLOG_CHECKPOINT_SHUTDOWN);

	ereport(LOG,
	 (errmsg("redo record is at %X/%X; undo record is at %X/%X; shutdown %s",
			 checkPoint.redo.xlogid, checkPoint.redo.xrecoff,
			 checkPoint.undo.xlogid, checkPoint.undo.xrecoff,
			 wasShutdown ? "TRUE" : "FALSE")));
	ereport(LOG,
			(errmsg("next transaction ID: %u/%u; next OID: %u",
					checkPoint.nextXidEpoch, checkPoint.nextXid,
					checkPoint.nextOid)));
	ereport(LOG,
			(errmsg("next MultiXactId: %u; next MultiXactOffset: %u",
					checkPoint.nextMulti, checkPoint.nextMultiOffset)));
	if (!TransactionIdIsNormal(checkPoint.nextXid))
		ereport(PANIC,
				(errmsg("invalid next transaction ID")));

	ShmemVariableCache->nextXid = checkPoint.nextXid;
	ShmemVariableCache->nextOid = checkPoint.nextOid;
	ShmemVariableCache->oidCount = 0;
	MultiXactSetNextMXact(checkPoint.nextMulti, checkPoint.nextMultiOffset);

	/*
	 * We must replay WAL entries using the same TimeLineID they were created
	 * under, so temporarily adopt the TLI indicated by the checkpoint (see
	 * also xlog_redo()).
	 */
	ThisTimeLineID = checkPoint.ThisTimeLineID;

	RedoRecPtr = XLogCtl->Insert.RedoRecPtr = checkPoint.redo;

	if (XLByteLT(RecPtr, checkPoint.redo))
		ereport(PANIC,
				(errmsg("invalid redo in checkpoint record")));
	if (checkPoint.undo.xrecoff == 0)
		checkPoint.undo = RecPtr;

	/*
	 * Check whether we need to force recovery from WAL.  If it appears to
	 * have been a clean shutdown and we did not have a recovery.conf file,
	 * then assume no recovery needed.
	 */
	if (XLByteLT(checkPoint.undo, RecPtr) ||
		XLByteLT(checkPoint.redo, RecPtr))
	{
		if (wasShutdown)
			ereport(PANIC,
				(errmsg("invalid redo/undo record in shutdown checkpoint")));
		InRecovery = true;
	}
	else if (ControlFile->state != DB_SHUTDOWNED)
		InRecovery = true;
	else if (StandbyTakeover || InArchiveRecovery)
	{
		/* force recovery due to presence of recovery.conf */
		ereport(LOG,
				(errmsg("Forcing Crash Recovery for Master Standby takeover")));
		InRecovery = true;
	}

	if (InRecovery && !IsUnderPostmaster)
	{
		ereport(FATAL,
				(errmsg("Database must be shutdown cleanly when using single backend start")));
	}

	if (InRecovery && gp_before_persistence_work)
	{
		ereport(FATAL,
				(errmsg("Database must be shutdown cleanly when using gp_before_persistence_work = on")));
	}

	/* Recovery from xlog */
	if (InRecovery)
	{
		int			rmid;

		CanEmitXLogRecords = false; /* we cannot emit records while in recovery */

		/*
		 * Update pg_control to show that we are recovering and to show the
		 * selected checkpoint as the place we are starting from. We also mark
		 * pg_control with any minimum recovery stop point obtained from a
		 * backup history file.
		 */
		if (InArchiveRecovery)
		{
			ereport(LOG,
					(errmsg("automatic recovery in progress")));
			ControlFile->state = DB_IN_ARCHIVE_RECOVERY;
		}
		else if (StandbyTakeover)
		{
			ereport(LOG,
					(errmsg("standby takeover recovery in progress")));
			ControlFile->state = DB_IN_CRASH_RECOVERY;
		}
		else
		{
			ereport(LOG,
					(errmsg("database system was not properly shut down; "
							"automatic recovery in progress")));
			ControlFile->state = DB_IN_CRASH_RECOVERY;
		}
		ControlFile->prevCheckPoint = ControlFile->checkPoint;
		ControlFile->checkPoint = checkPointLoc;
		ControlFile->checkPointCopy = checkPoint;
		if (minRecoveryLoc.xlogid != 0 || minRecoveryLoc.xrecoff != 0)
			ControlFile->minRecoveryPoint = minRecoveryLoc;
		ControlFile->time = time(NULL);
		UpdateControlFile();

		/*
		 * If there was a backup label file, it's done its job and the info
		 * has now been propagated into pg_control.  We must get rid of the
		 * label file so that if we crash during recovery, we'll pick up at
		 * the latest recovery restartpoint instead of going all the way back
		 * to the backup start point.  It seems prudent though to just rename
		 * the file out of the way rather than delete it completely.
		 */
		if (haveBackupLabel)
		{
			unlink(BACKUP_LABEL_OLD);
			if (rename(BACKUP_LABEL_FILE, BACKUP_LABEL_OLD) != 0)
				ereport(FATAL,
						(errcode_for_file_access(),
						 errmsg("could not rename file \"%s\" to \"%s\": %m",
								BACKUP_LABEL_FILE, BACKUP_LABEL_OLD)));
		}

		/* Start up the recovery environment */
		XLogInitRelationCache();

		for (rmid = 0; rmid <= RM_MAX_ID; rmid++)
		{
			if (RmgrTable[rmid].rm_startup != NULL)
				RmgrTable[rmid].rm_startup();
		}

		/*
		 * Find the first record that logically follows the checkpoint --- it
		 * might physically precede it, though.
		 */
		if (XLByteLT(checkPoint.redo, RecPtr))
		{
			/* back up to find the record */
			record = XLogReadRecord(&(checkPoint.redo), PANIC);
		}
		else
		{
			/* just have to read next record after CheckPoint */
			record = XLogReadRecord(NULL, LOG);
		}

		/*
		 *	AP: To handle case where its not a clean shutdown but it doesnt have a record following the checkpoint record.
		 *	In such case, just proceed with the Pass 2, 3, 4 to clear any inconsistent enteries in Persistent Tables without 
		 *	doing the whole redo loop below.
		 */
		if (record == NULL)	
		{
			/*
			 * There are no WAL records following the checkpoint
			 */
			ereport(LOG,
					(errmsg("no record for redo after checkpoint, skip redo and proceed for recovery pass")));
		}

		XLogCtl->pass1StartLoc = ReadRecPtr;

		/* 
		 * MPP-11179
		 * Recovery Passes will be done in both the cases:
		 * 1. When record after checkpoint = NULL (No redo)
		 * 2. When record after checkpoint != NULL (redo also)
		 */
		multipleRecoveryPassesNeeded = true;

		/*
		 * main redo apply loop, executed if we have record after checkpoint
		 */
		if (record != NULL)
		{
			bool		recoveryContinue = true;
			bool		recoveryApply = true;
			InRedo = true;
			CurrentResourceOwner = ResourceOwnerCreate(NULL, "xlog");

			ereport(LOG,
						(errmsg("redo starts at %X/%X",
								ReadRecPtr.xlogid, ReadRecPtr.xrecoff)));
			do
			{
				ErrorContextCallback errcontext;

				/*
				 * Have we reached our recovery target?
				 */
				if (recoveryStopsHere(record, &recoveryApply))
				{
					reachedStopPoint = true;		/* see below */
					recoveryContinue = false;
					if (!recoveryApply)
						break;
				}

				/* Setup error traceback support for ereport() */
				errcontext.callback = StartupXLOG_RedoPass1Context;
				errcontext.arg = (void *) record;
				errcontext.previous = error_context_stack;
				error_context_stack = &errcontext;

				if (PersistentRecovery_ShouldHandlePass1XLogRec(&ReadRecPtr, &EndRecPtr, record))
				{
					ApplyStartupRedo(&ReadRecPtr, &EndRecPtr, record);
				}

				/* Pop the error context stack */
				error_context_stack = errcontext.previous;

				LastRec = ReadRecPtr;

				record = XLogReadRecord(NULL, LOG);
			} while (record != NULL && recoveryContinue);

			ereport(LOG,
					(errmsg("redo done at %X/%X",
							ReadRecPtr.xlogid, ReadRecPtr.xrecoff)));

			CurrentResourceOwner = GetTopResourceOwner();

			InRedo = false;

		}
		/*
		 * end of main redo apply loop
		 */

		CanEmitXLogRecords = true;
	}

	/*
	 * Re-fetch the last valid or last applied record, so we can identify the
	 * exact endpoint of what we consider the valid portion of WAL.
	 */
	record = XLogReadRecord(&LastRec, PANIC);
	EndOfLog = EndRecPtr;
	XLByteToPrevSeg(EndOfLog, endLogId, endLogSeg);

	elog(LOG,"end of transaction log location is %s",
		 XLogLocationToString(&EndOfLog));

	XLogCtl->pass1LastLoc = ReadRecPtr;

	/*
	 * Complain if we did not roll forward far enough to render the backup
	 * dump consistent.
	 */
	if (XLByteLT(EndOfLog, ControlFile->minRecoveryPoint))
	{
		if (reachedStopPoint)	/* stopped because of stop request */
			ereport(FATAL,
					(errmsg("requested recovery stop point is before end time of backup dump")));
		else					/* ran off end of WAL */
			ereport(FATAL,
					(errmsg("WAL ends before end time of backup dump")));
	}

	/*
	 * Consider whether we need to assign a new timeline ID.
	 *
	 * If we are doing an archive recovery, we always assign a new ID.  This
	 * handles a couple of issues.  If we stopped short of the end of WAL
	 * during recovery, then we are clearly generating a new timeline and must
	 * assign it a unique new ID.  Even if we ran to the end, modifying the
	 * current last segment is problematic because it may result in trying
	 * to overwrite an already-archived copy of that segment, and we encourage
	 * DBAs to make their archive_commands reject that.  We can dodge the
	 * problem by making the new active segment have a new timeline ID.
	 *
	 * In a normal crash recovery, we can just extend the timeline we were in.
	 */
	if (InArchiveRecovery)
	{
		ThisTimeLineID = findNewestTimeLine(recoveryTargetTLI) + 1;
		ereport(LOG,
				(errmsg("selected new timeline ID: %u", ThisTimeLineID)));
		writeTimeLineHistory(ThisTimeLineID, recoveryTargetTLI,
							 curFileTLI, endLogId, endLogSeg);
	}

	/* Save the selected TimeLineID in shared memory, too */
	XLogCtl->ThisTimeLineID = ThisTimeLineID;

	/*
	 * We are now done reading the old WAL.  Turn off archive fetching if it
	 * was active, and make a writable copy of the last WAL segment. (Note
	 * that we also have a copy of the last block of the old WAL in readBuf;
	 * we will use that below.)
	 */
	if (InArchiveRecovery)
		exitArchiveRecovery(curFileTLI, endLogId, endLogSeg);

	if (StandbyTakeover)
		exitStandbyTakeover();

	/*
	 * Prepare to write WAL starting at EndOfLog position, and init xlog
	 * buffer cache using the block containing the last record from the
	 * previous incarnation.
	 */
	openLogId = endLogId;
	openLogSeg = endLogSeg;
	XLogFileOpen(
			&mirroredLogFileOpen,
			openLogId,
			openLogSeg);
	openLogOff = 0;
	ControlFile->logId = openLogId;
	ControlFile->logSeg = openLogSeg + 1;
	Insert = &XLogCtl->Insert;
	Insert->PrevRecord = LastRec;
	XLogCtl->xlblocks[0].xlogid = openLogId;
	XLogCtl->xlblocks[0].xrecoff =
		((EndOfLog.xrecoff - 1) / XLOG_BLCKSZ + 1) * XLOG_BLCKSZ;

	/*
	 * Tricky point here: readBuf contains the *last* block that the LastRec
	 * record spans, not the one it starts in.	The last block is indeed the
	 * one we want to use.
	 */
	Assert(readOff == (XLogCtl->xlblocks[0].xrecoff - XLOG_BLCKSZ) % XLogSegSize);
	memcpy((char *) Insert->currpage, readBuf, XLOG_BLCKSZ);
	Insert->currpos = (char *) Insert->currpage +
		(EndOfLog.xrecoff + XLOG_BLCKSZ - XLogCtl->xlblocks[0].xrecoff);

	LogwrtResult.Write = LogwrtResult.Flush = EndOfLog;

	XLogCtl->Write.LogwrtResult = LogwrtResult;
	Insert->LogwrtResult = LogwrtResult;
	XLogCtl->LogwrtResult = LogwrtResult;

	XLogCtl->LogwrtRqst.Write = EndOfLog;
	XLogCtl->LogwrtRqst.Flush = EndOfLog;

	freespace = INSERT_FREESPACE(Insert);
	if (freespace > 0)
	{
		/* Make sure rest of page is zero */
		MemSet(Insert->currpos, 0, freespace);
		XLogCtl->Write.curridx = 0;
	}
	else
	{
		/*
		 * Whenever Write.LogwrtResult points to exactly the end of a page,
		 * Write.curridx must point to the *next* page (see XLogWrite()).
		 *
		 * Note: it might seem we should do AdvanceXLInsertBuffer() here, but
		 * this is sufficient.	The first actual attempt to insert a log
		 * record will advance the insert state.
		 */
		XLogCtl->Write.curridx = NextBufIdx(0);
	}

	if (InRecovery)
	{
		/*
		 * Close down Recovery for Startup PASS 1.
		 */
		int			rmid;

		/*
		 * Allow resource managers to do any required cleanup.
		 */
		for (rmid = 0; rmid <= RM_MAX_ID; rmid++)
		{
			if (RmgrTable[rmid].rm_cleanup != NULL)
				RmgrTable[rmid].rm_cleanup();
		}

		/*
		 * Check to see if the XLOG sequence contained any unresolved
		 * references to uninitialized pages.
		 */
		XLogCheckInvalidPages();

		/*
		 * Reset pgstat data, because it may be invalid after recovery.
		 */
		pgstat_reset_all();

		/*
		 * We are not finished with multiple passes, so we do not do a
		 * shutdown checkpoint here as we did in the past.
		 *
		 * We only flush out the Resource Managers.
		 */
		Checkpoint_RecoveryPass(XLogCtl->pass1LastLoc);

		/*
		 * Close down recovery environment
		 */
		XLogCloseRelationCache();
	}

	/*
	 * Preallocate additional log files, if wanted.
	 */
	(void) PreallocXlogFiles(EndOfLog);

	/*
	 * Okay, we're finished with Pass 1.
	 */
	InRecovery = false;

	/* start the archive_timeout timer running */
	XLogCtl->Write.lastSegSwitchTime = ControlFile->time;

	/* initialize shared-memory copy of latest checkpoint XID/epoch */
	XLogCtl->ckptXidEpoch = ControlFile->checkPointCopy.nextXidEpoch;
	XLogCtl->ckptXid = ControlFile->checkPointCopy.nextXid;

	if (!gp_before_persistence_work)
	{
		/*
		 * Create a resource owner to keep track of our resources (currently only
		 * buffer pins).
		 */
		CurrentResourceOwner = ResourceOwnerCreate(NULL, "StartupXLOG");

		/*
		 * We don't have any hope of running a real relcache, but we can use the
		 * same fake-relcache facility that WAL replay uses.
		 */
		XLogInitRelationCache();

		/*
		 * During startup after we have performed recovery is the only place we
		 * scan in the persistent meta-data into memory on already initdb database.
		 */
		PersistentFileSysObj_StartupInitScan();

		/*
		 * Close down recovery environment
		 */
		XLogCloseRelationCache();
	}

	if (!IsUnderPostmaster)
	{
		Assert(!multipleRecoveryPassesNeeded);

		StartupXLOG_InProduction();

		ereport(LOG,
				(errmsg("Finished single backend startup")));
	}
	else
	{
		XLogCtl->multipleRecoveryPassesNeeded = multipleRecoveryPassesNeeded;

		if (!gp_startup_integrity_checks)
		{
			ereport(LOG,
					(errmsg("Integrity checks will be skipped because gp_startup_integrity_checks = off")));
		}
		else
		{
			XLogCtl->integrityCheckNeeded = true;
		}

		if (!XLogCtl->multipleRecoveryPassesNeeded)
		{
			StartupXLOG_InProduction();

			ereport(LOG,
					(errmsg("Finished normal startup for clean shutdown case")));

		}
		else
		{
			ereport(LOG,
					(errmsg("Finished startup pass 1.  Proceeding to startup crash recovery passes 2 and 3.")));
		}
	}

	ShutdownReadFile();
}

bool XLogStartupMultipleRecoveryPassesNeeded(void)
{
	Assert(XLogCtl != NULL);
	return XLogCtl->multipleRecoveryPassesNeeded;
}

bool XLogStartupIntegrityCheckNeeded(void)
{
	Assert(XLogCtl != NULL);
	return XLogCtl->integrityCheckNeeded;
}

static void
GetRedoRelationFileName(char *path)
{
	char *xlogDir = makeRelativeToTxnFilespace(XLOGDIR);
	if (snprintf(path, MAXPGPATH, "%s/RedoRelationFile", xlogDir) > MAXPGPATH)
	{
		ereport(ERROR, (errmsg("cannot generate pathname %s/RedoRelationFile", xlogDir)));
	}
	pfree(xlogDir);
}

static int
CreateRedoRelationFile(void)
{
	char	path[MAXPGPATH];

	int		result;

	GetRedoRelationFileName(path);

	result = open(path, O_RDWR | O_CREAT | O_TRUNC, S_IRUSR | S_IWUSR);
	if (result < 0)
	{
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not create redo relation file \"%s\"",
						path)));
	}

	return result;
}

static int
OpenRedoRelationFile(void)
{
	char	path[MAXPGPATH];

	int		result;

	GetRedoRelationFileName(path);

	result = open(path, O_RDONLY, S_IRUSR | S_IWUSR);
	if (result < 0)
	{
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not open redo relation file \"%s\"",
						path)));
	}

	return result;
}

static void
UnlinkRedoRelationFile(void)
{
	char	path[MAXPGPATH];

	GetRedoRelationFileName(path);

	if (unlink(path) < 0)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not unlink redo relation file \"%s\": %m", path)));
}

/*
 * This must be called ONCE during postmaster or standalone-backend startup
 */
void
StartupXLOG_Pass2(void)
{
	XLogRecord *record;

	int redoRelationFile;

	if (Debug_persistent_recovery_print)
		elog(PersistentRecovery_DebugPrintLevel(),
		     "Entering StartupXLOG_Pass2");

//	pg_usleep(45 * 1000000L);

	/*
	 * Read control file and verify XLOG status looks valid.
	 *
	 */
	ReadControlFile();

	if (ControlFile->logSeg == 0 ||
		ControlFile->state < DB_SHUTDOWNED ||
		ControlFile->state > DB_IN_PRODUCTION ||
		!XRecOffIsValid(ControlFile->checkPoint.xrecoff))
		ereport(FATAL,
				(errmsg("Startup Pass 2: control file contains invalid data")));

	recoveryTargetTLI = ControlFile->checkPointCopy.ThisTimeLineID;

	/* Now we can determine the list of expected TLIs */
	expectedTLIs = XLogReadTimeLineHistory(recoveryTargetTLI);

	if (Debug_persistent_recovery_print)
		elog(PersistentRecovery_DebugPrintLevel(),
		     "ControlFile with recoveryTargetTLI %u, transaction log to start location is %s",
			 recoveryTargetTLI,
			 XLogLocationToString(&XLogCtl->pass1StartLoc));

	record = XLogReadRecord(&XLogCtl->pass1StartLoc, PANIC);

	/*
	 * Pass 2 XLOG scan
	 */
	while (true)
	{
		PersistentRecovery_HandlePass2XLogRec(&ReadRecPtr, &EndRecPtr, record);

		if (XLByteEQ(ReadRecPtr, XLogCtl->pass1LastLoc))
			break;

		Assert(XLByteLE(ReadRecPtr,XLogCtl->pass1LastLoc));

		record = XLogReadRecord(NULL, PANIC);
	}
	ShutdownReadFile();

	InRecoveryPass = 2;
	PersistentRecovery_Scan();

	PersistentRecovery_CrashAbort();

	PersistentRecovery_Update();

	PersistentRecovery_Drop();

	/*PersistentRecovery_UpdateAppendOnlyMirrorResyncEofs();*/
	InRecoveryPass = 0;

#ifdef USE_ASSERT_CHECKING
//	PersistentRecovery_VerifyTablesAgainstMemory();
#endif

	Checkpoint_RecoveryPass(XLogCtl->pass1LastLoc);

	/*
	 * Create a file that passes information to pass 3.
	 */
	redoRelationFile = CreateRedoRelationFile();

	PersistentRecovery_SerializeRedoRelationFile(redoRelationFile);

	close(redoRelationFile);

	ereport(LOG,
			(errmsg("Finished startup crash recovery pass 2")));

	if (Debug_persistent_recovery_print)
		elog(PersistentRecovery_DebugPrintLevel(),
		     "Exiting StartupXLOG_Pass2");
}

/*
 * This must be called ONCE during postmaster or standalone-backend startup
 */
void
StartupXLOG_Pass3(void)
{
	int redoRelationFile;

	XLogRecord *record;

	int 		rmid;

	if (Debug_persistent_recovery_print)
		elog(PersistentRecovery_DebugPrintLevel(),
		     "Entering StartupXLOG_Pass3");

	redoRelationFile = OpenRedoRelationFile();

	PersistentRecovery_DeserializeRedoRelationFile(redoRelationFile);

	close(redoRelationFile);

	UnlinkRedoRelationFile();

	if (Debug_persistent_recovery_print)
		elog(PersistentRecovery_DebugPrintLevel(),
			 "StartupXLOG_Pass3: Begin re-scanning XLOG");

	InRecovery = true;

	/*
	 * Read control file and verify XLOG status looks valid.
	 *
	 */
	ReadControlFile();

	if (ControlFile->logSeg == 0 ||
		ControlFile->state < DB_SHUTDOWNED ||
		ControlFile->state > DB_IN_PRODUCTION ||
		!XRecOffIsValid(ControlFile->checkPoint.xrecoff))
		ereport(FATAL,
				(errmsg("Startup Pass 2: control file contains invalid data")));

	recoveryTargetTLI = ControlFile->checkPointCopy.ThisTimeLineID;

	/* Now we can determine the list of expected TLIs */
	expectedTLIs = XLogReadTimeLineHistory(recoveryTargetTLI);

	if (Debug_persistent_recovery_print)
	{
		elog(PersistentRecovery_DebugPrintLevel(),
			 "ControlFile with recoveryTargetTLI %u, transaction log to start location is %s",
			 recoveryTargetTLI,
			 XLogLocationToString(&XLogCtl->pass1StartLoc));
		elog(PersistentRecovery_DebugPrintLevel(),
		     "StartupXLOG_RedoPass3Context: Control File checkpoint location is %s",
		     XLogLocationToString(&ControlFile->checkPoint));
	}

	/* Start up the recovery environment */
	XLogInitRelationCache();

	for (rmid = 0; rmid <= RM_MAX_ID; rmid++)
	{
		if (RmgrTable[rmid].rm_startup != NULL)
			RmgrTable[rmid].rm_startup();
	}

	record = XLogReadRecord(&XLogCtl->pass1StartLoc, PANIC);

	/*
	 * Pass 3 XLOG scan
	 */
	while (true)
	{
		ErrorContextCallback errcontext;

		/* Setup error traceback support for ereport() */
		errcontext.callback = StartupXLOG_RedoPass3Context;
		errcontext.arg = (void *) record;
		errcontext.previous = error_context_stack;
		error_context_stack = &errcontext;

		if (PersistentRecovery_ShouldHandlePass3XLogRec(&ReadRecPtr, &EndRecPtr, record))
			ApplyStartupRedo(&ReadRecPtr, &EndRecPtr, record);

		/* Pop the error context stack */
		error_context_stack = errcontext.previous;

		/*
		 * For Pass 3, we read through the new log generated by Pass 2 in case
		 * there are Master Mirror XLOG records we need to take action on.
		 *
		 * It is obscure: Pass 3 REDO of Create fs-obj may need to be compensated for
		 * by Deletes generated in Pass 2...
		 */
		record = XLogReadRecord(NULL, LOG);
		if (record == NULL)
			break;
	}

	ShutdownReadFile();

	/*
	 * Allow resource managers to do any required cleanup.
	 */
	for (rmid = 0; rmid <= RM_MAX_ID; rmid++)
	{
		if (RmgrTable[rmid].rm_cleanup != NULL)
			RmgrTable[rmid].rm_cleanup();
	}

	/*
	 * Check to see if the XLOG sequence contained any unresolved
	 * references to uninitialized pages.
	 */
	XLogCheckInvalidPages();

	/*
	 * Reset pgstat data, because it may be invalid after recovery.
	 */
	pgstat_reset_all();

	/*
	 * Close down recovery environment
	 */
	XLogCloseRelationCache();

	if (Debug_persistent_recovery_print)
		elog(PersistentRecovery_DebugPrintLevel(),
			 "StartupXLOG_Pass3: End re-scanning XLOG");

	InRecovery = false;

	StartupXLOG_InProduction();

	ereport(LOG,
			(errmsg("Finished startup crash recovery pass 3")));

	if (Debug_persistent_recovery_print)
		elog(PersistentRecovery_DebugPrintLevel(),
			 "Exiting StartupXLOG_Pass3");
}

/*
 * This must be called ONCE during postmaster or standalone-backend startup
 */
void
StartupXLOG_Pass4(void)
{
	if (Debug_persistent_recovery_print)
		elog(PersistentRecovery_DebugPrintLevel(),
		     "Entering StartupXLOG_Pass4");

	PersistentFileSysObj_StartupIntegrityCheck();

	/* 
	 * Do the check for inconsistencies in global sequence number after the catalog cache is set up 
	 * MPP-17207. Inconsistent global sequence number can be fixed with setting the guc
	 * gp_persistent_repair_global_sequence 
	 */
	
	PersistentFileSysObj_DoGlobalSequenceScan();

	ereport(LOG,
			(errmsg("Finished startup integrity checking")));

	if (Debug_persistent_recovery_print)
		elog(PersistentRecovery_DebugPrintLevel(),
			 "Exiting StartupXLOG_Pass4");
}

/*
 * Determine the recovery redo start location from the pg_control file.
 *
 *    1) Only uses information from the pg_control file.
 *    2) This simplified routine does not examine the offline recovery file or
 *       the online backup labels, etc.
 *    3) This routine is a heavily reduced version of StartXLOG.
 *    4) IMPORTANT NOTE: This routine sets global variables that establish
 *       the timeline context necessary to do ReadRecord.  The ThisTimeLineID
 *       and expectedTLIs globals are set.
 *
 */
void
XLogGetRecoveryStart(char *callerStr, char *reasonStr, XLogRecPtr *redoCheckPointLoc, CheckPoint *redoCheckPoint)
{
	CheckPoint	checkPoint;
	XLogRecPtr	checkPointLoc;
	XLogRecord *record;
	bool previous;
	XLogRecPtr checkPointLSN;

	Assert(redoCheckPointLoc != NULL);
	Assert(redoCheckPoint != NULL);

	ereport((Debug_print_qd_mirroring ? LOG : DEBUG1),
			(errmsg("%s: determine restart location %s",
			 callerStr, reasonStr)));

	XLogCloseReadRecord();

	if (Debug_print_qd_mirroring)
	{
		XLogPrintLogNames();
	}

	/*
	 * Read control file and verify XLOG status looks valid.
	 *
	 */
	ReadControlFile();

	if (ControlFile->logSeg == 0 ||
		ControlFile->state < DB_SHUTDOWNED ||
		ControlFile->state > DB_IN_PRODUCTION ||
		!XRecOffIsValid(ControlFile->checkPoint.xrecoff))
		ereport(FATAL,
				(errmsg("%s: control file contains invalid data", callerStr)));

	/*
	 * Get the last valid checkpoint record.  If the latest one according
	 * to pg_control is broken, try the next-to-last one.
	 */
	checkPointLoc = ControlFile->checkPoint;
	ThisTimeLineID = ControlFile->checkPointCopy.ThisTimeLineID;

	elog((Debug_print_qd_mirroring ? LOG : DEBUG1),"ControlFile with ThisTimeLineID %u, checkpoint location %s",
		 ThisTimeLineID,
		 XLogLocationToString(&checkPointLoc));

	/*
	 * Check for recovery control file, and if so set up state for offline
	 * recovery
	 */
	XLogReadRecoveryCommandFile(DEBUG5);

	/* Now we can determine the list of expected TLIs */
	expectedTLIs = XLogReadTimeLineHistory(ThisTimeLineID);

	record = ReadCheckpointRecord(checkPointLoc, 1);
	if (record != NULL)
	{
		previous = false;
		ereport((Debug_print_qd_mirroring ? LOG : DEBUG1),
				(errmsg("%s: checkpoint record is at %s (LSN %s)",
						callerStr,
						XLogLocationToString(&checkPointLoc),
						XLogLocationToString2(&EndRecPtr))));
	}
	else
	{
		previous = true;
		checkPointLoc = ControlFile->prevCheckPoint;
		record = ReadCheckpointRecord(checkPointLoc, 2);
		if (record != NULL)
		{
			ereport((Debug_print_qd_mirroring ? LOG : DEBUG1),
					(errmsg("%s: using previous checkpoint record at %s (LSN %s)",
						    callerStr,
							XLogLocationToString(&checkPointLoc),
						    XLogLocationToString2(&EndRecPtr))));
		}
		else
		{
			ereport(ERROR,
				 (errmsg("%s: could not locate a valid checkpoint record", callerStr)));
		}
	}

	memcpy(&checkPoint, XLogRecGetData(record), sizeof(CheckPoint));
	checkPointLSN = EndRecPtr;

	if (XLByteEQ(checkPointLoc,checkPoint.redo))
	{
		{
			char	tmpBuf[1024];

			snprintf(tmpBuf, sizeof(tmpBuf),
					 "control file has restart '%s' and redo start checkpoint at location(lsn) '%s(%s)' ",
					 (previous ? "previous " : ""),
					 XLogLocationToString3(&checkPointLoc),
					 XLogLocationToString4(&checkPointLSN));

			/* TODO: ereport */
		}
	}
 	else if (XLByteLT(checkPointLoc, checkPoint.redo))
	{
		ereport(ERROR,
				(errmsg("%s: invalid redo in checkpoint record", callerStr)));
	}
	else
	{
		XLogRecord *record;

		record = XLogReadRecord(&checkPoint.redo, LOG);
		if (record == NULL)
		{
			ereport(ERROR,
			 (errmsg("%s: first redo record before checkpoint not found at %s",
					 callerStr, XLogLocationToString(&checkPoint.redo))));
		}

		{
			char	tmpBuf[1024];

			snprintf(tmpBuf, sizeof(tmpBuf),
					 "control file has restart '%s' checkpoint at location(lsn) '%s(%s)', redo starts at location(lsn) '%s(%s)' ",
					 (previous ? "previous " : ""),
					 XLogLocationToString3(&checkPointLoc),
					 XLogLocationToString4(&checkPointLSN),
					 XLogLocationToString(&checkPoint.redo),
					 XLogLocationToString2(&EndRecPtr));

			/* TODO: ereport */
		}
	}

	XLogCloseReadRecord();

	*redoCheckPointLoc = checkPointLoc;
	*redoCheckPoint = checkPoint;

}

/*
 * Scan the XLOG to determine the end of the XLOG.
 *
 */
void
XLogScanForStandbyEndLocation(XLogRecPtr *startLoc, XLogRecPtr *endLoc)
{
	XLogRecord *record;
	XLogRecPtr	LastRec;

	elog(LOG,"QDSYNC: scan forward starting at restart location to determine end location");

	elog((Debug_print_qd_mirroring ? LOG : DEBUG1),
		 "scan with ThisTimeLineID %u, location %s",
		 ThisTimeLineID,
		 XLogLocationToString(startLoc));

	XLogCloseReadRecord();

	record = XLogReadRecord(startLoc, DEBUG1);
	if (record == NULL)
	{
		elog(ERROR,"QDSYNC: couldn't read start location %s",
			 XLogLocationToString(startLoc));
	}

	do
	{
		LastRec = ReadRecPtr;

		record = XLogReadRecord(NULL, DEBUG1);
	} while (record != NULL);

	record = XLogReadRecord(&LastRec, ERROR);
	*endLoc = EndRecPtr;

	XLogCloseReadRecord();
}

/*
 * Recover the database for a specified range of the XLOG.
 *
 * The last record position should be a checkpoint record.
 *
 */
void
XLogStandbyRecoverRange(XLogRecPtr *redoCheckpointLoc, CheckPoint *redoCheckPoint, XLogRecPtr *lastRecordLoc)
{
	XLogRecord *record;
	StringInfoData buf;
	int rmid;
	ErrorContextCallback errcontext;
	XLogRecPtr prevReadLoc;

	elog((Debug_print_qd_mirroring ? LOG : DEBUG1),
		 "recover range with ThisTimeLineID %u, checkpoint.redo %s, last record location %s",
		 ThisTimeLineID,
		 XLogLocationToString(&redoCheckPoint->redo),
		 XLogLocationToString2(lastRecordLoc));

	if (XLByteLE(*lastRecordLoc,redoCheckPoint->redo))
	{
		elog((Debug_print_qd_mirroring ? LOG : DEBUG1),"QDSYNC: nothing to redo");
		return;
	}

	XLogCloseReadRecord();

	/*
	 * Copy code from StartupXLOG necessary to do recovery.
	 */
	ereport((Debug_print_qd_mirroring ? LOG : DEBUG1),
			(errmsg("QDSYNC: next transaction ID: %u/%u; next OID: %u",
					redoCheckPoint->nextXidEpoch, redoCheckPoint->nextXid,
					redoCheckPoint->nextOid)));
	ereport((Debug_print_qd_mirroring ? LOG : DEBUG1),
			(errmsg("QDSYNC: next MultiXactId: %u; next MultiXactOffset: %u",
					redoCheckPoint->nextMulti, redoCheckPoint->nextMultiOffset)));
	if (!TransactionIdIsNormal(redoCheckPoint->nextXid))
		ereport(PANIC,
				(errmsg("QDSYNC: invalid next transaction ID")));

	ShmemVariableCache->nextXid = redoCheckPoint->nextXid;
	ShmemVariableCache->nextOid = redoCheckPoint->nextOid;
	ShmemVariableCache->oidCount = 0;
	MultiXactSetNextMXact(redoCheckPoint->nextMulti, redoCheckPoint->nextMultiOffset);

	/*
	 * Should not emit XLog during standby redo. This method is called only
	 * by standby walredo process.
	 */
	CanEmitXLogRecords = false;

	/* Start up the recovery environment */
	XLogInitRelationCache();

	for (rmid = 0; rmid <= RM_MAX_ID; rmid++)
	{
		if (RmgrTable[rmid].rm_startup != NULL)
			RmgrTable[rmid].rm_startup();
	}

	RedoRecPtr = redoCheckPoint->redo;

	/*
	 * Find the first record that logically follows the checkpoint --- it
	 * might physically precede it, though.
	 */
	if (XLByteLT(redoCheckPoint->redo, *redoCheckpointLoc))
	{
		/* back up to find the record */
		record = XLogReadRecord(&(redoCheckPoint->redo), DEBUG1);
		if (record == NULL)
		{
			/*
			 * Why don't we have the redo record?
			 */
			elog(ERROR,"QDSYNC: could not find first redo record %s",
			     XLogLocationToString(&redoCheckPoint->redo));
		}
		ereport(LOG,
				(errmsg("QDSYNC: starting redo at %s from the redo location in the checkpoint at %s",
						XLogLocationToString(&ReadRecPtr),
						XLogLocationToString2(redoCheckpointLoc))));
	}
	else
	{
		/* just have to read next record after CheckPoint */
		record = XLogReadRecord(redoCheckpointLoc, DEBUG1);
		if (record == NULL)
		{
			/*
			 * Why couldn't we find the checkpoint record?
			 */
			elog(ERROR,"QDSYNC: could not find the checkpoint record %s",
			     XLogLocationToString(redoCheckpointLoc));
		}
		elog((Debug_print_qd_mirroring ? LOG : DEBUG1),
			 "QDSYNC: found the checkpoint record %s",
		     XLogLocationToString(redoCheckpointLoc));
		record = XLogReadRecord(NULL, DEBUG1);
		if (record == NULL)
		{
			/*
			 * We hit end of WAL first???
			 */

			XLogCloseReadRecord();

			elog(LOG,"QDSYNC: no records to process after checkpoint at %s",
				 XLogLocationToString(redoCheckpointLoc));
			return;
		}
		ereport(LOG,
				(errmsg("QDSYNC: starting at %s with the first record after the checkpoint at %s",
						XLogLocationToString(&ReadRecPtr),
						XLogLocationToString2(redoCheckpointLoc))));
	}

	InRecovery = true;
	InRedo = true;
	CurrentResourceOwner = ResourceOwnerCreate(NULL, "xlog");
	while (true)
	{
		RedoErrorCallBack redoErrorCallBack;

		if (XLByteEQ(*lastRecordLoc,ReadRecPtr))
			break;
		if (XLByteLT(*lastRecordLoc,ReadRecPtr))
		{
			/*
			 * We shouldn't have been able to read past the specified last
			 * record...
			 */
			elog(ERROR,"QDSYNC: read past specified last record location %s (record location %s)",
			     XLogLocationToString(lastRecordLoc),
			     XLogLocationToString2(&ReadRecPtr));
		}

		if (Debug_print_qd_mirroring)
		{
			initStringInfo(&buf);
			appendStringInfo(&buf, "apply @ %s; LSN %s: ",
							 XLogLocationToString(&ReadRecPtr),
							 XLogLocationToString2(&EndRecPtr));
			XLog_OutRec(&buf, record);
			appendStringInfo(&buf, " - ");
			RmgrTable[record->xl_rmid].rm_desc(&buf,
											   ReadRecPtr,
											   record);
			elog(LOG, "%s", buf.data);
			pfree(buf.data);
		}
		if (Debug_persistent_recovery_print)
		{
			initStringInfo(&buf);
			appendStringInfo(&buf, "XLogStandbyRecoverRange: Apply @ %s; LSN %s: ",
							 XLogLocationToString(&ReadRecPtr),
							 XLogLocationToString2(&EndRecPtr));
			XLog_OutRec(&buf, record);
			appendStringInfo(&buf, " - ");
			RmgrTable[record->xl_rmid].rm_desc(&buf,
											   ReadRecPtr,
											   record);
			elog(PersistentRecovery_DebugPrintLevel(), "%s", buf.data);
			pfree(buf.data);
		}

		/* Setup error traceback support for ereport() */
		redoErrorCallBack.location = ReadRecPtr;
		redoErrorCallBack.record = record;

		errcontext.callback = rm_redo_error_callback;
		errcontext.arg = (void *) &redoErrorCallBack;
		errcontext.previous = error_context_stack;
		error_context_stack = &errcontext;

		/* nextXid must be beyond record's xid */
		if (TransactionIdFollowsOrEquals(record->xl_xid,
										 ShmemVariableCache->nextXid))
		{
			ShmemVariableCache->nextXid = record->xl_xid;
			TransactionIdAdvance(ShmemVariableCache->nextXid);
		}

		if (record->xl_info & XLR_BKP_BLOCK_MASK)
			RestoreBkpBlocks(record, EndRecPtr);

		RmgrTable[record->xl_rmid].rm_redo(ReadRecPtr, EndRecPtr, record);

		/* Pop the error context stack */
		error_context_stack = errcontext.previous;

		prevReadLoc = ReadRecPtr;
		record = XLogReadRecord(NULL, DEBUG1);
		if (record == NULL)
		{
			/*
			 * We hit end of WAL first???
			 */
			elog(ERROR,"QDSYNC: hit end of WAL before finding %s (previous record %s)",
			     XLogLocationToString(lastRecordLoc),
			     XLogLocationToString2(&prevReadLoc));
		}
	}

	/*
	 * Since we stop short of redoing the last record -- a checkpoint -- we need to
	 * flush here because our filespace / tablespace hash tables get emptied the next
	 * time we call XLogStandbyRecoverRange and the Buffer Pool manager will not be
	 * able to find where to write and fsync the relation files when trying to process
	 * a checkpoint...
	 */
	FlushBufferPool();

	CurrentResourceOwner = GetTopResourceOwner();

	InRedo = false;
	InRecovery = false;

	/*
	 * Allow resource managers to do any required cleanup.
	 */
	for (rmid = 0; rmid <= RM_MAX_ID; rmid++)
	{
		if (RmgrTable[rmid].rm_cleanup != NULL)
			RmgrTable[rmid].rm_cleanup();
	}

	/*
	 * Check to see if the XLOG sequence contained any unresolved
	 * references to uninitialized pages.
	 */
	XLogCheckInvalidPages();

	/*
	 * Reset pgstat data, because it may be invalid after recovery.
	 */
	pgstat_reset_all();

	/*
	 * Close down recovery environment
	 */
	XLogCloseRelationCache();

	XLogCloseReadRecord();

	ereport(LOG,
			(errmsg("QDSYNC: finished redo pass")));
}

/*
 * Subroutine to try to fetch and validate a prior checkpoint record.
 *
 * whichChkpt identifies the checkpoint (merely for reporting purposes).
 * 1 for "primary", 2 for "secondary", 0 for "other" (backup_label)
 */
XLogRecord *
ReadCheckpointRecord(XLogRecPtr RecPtr, int whichChkpt)
{
	XLogRecord *record;
	bool sizeOk;
	uint32 delta_xl_tot_len;		/* delta of total len of entire record */
	uint32 delta_xl_len;			/* delta of total len of rmgr data */

	if (!XRecOffIsValid(RecPtr.xrecoff))
	{
		switch (whichChkpt)
		{
			case 1:
				ereport(LOG,
				(errmsg("invalid primary checkpoint link in control file")));
				break;
			case 2:
				ereport(LOG,
						(errmsg("invalid secondary checkpoint link in control file")));
				break;
			default:
				ereport(LOG,
				   (errmsg("invalid checkpoint link in backup_label file")));
				break;
		}
		return NULL;
	}

	record = XLogReadRecord(&RecPtr, LOG);

	if (record == NULL)
	{
		switch (whichChkpt)
		{
			case 1:
				ereport(LOG,
						(errmsg("invalid primary checkpoint record at location %s",
						        XLogLocationToString_Long(&RecPtr))));
				break;
			case 2:
				ereport(LOG,
						(errmsg("invalid secondary checkpoint record at location %s",
						        XLogLocationToString_Long(&RecPtr))));
				break;
			default:
				ereport(LOG,
						(errmsg("invalid checkpoint record at location %s",
						        XLogLocationToString_Long(&RecPtr))));
				break;
		}
		return NULL;
	}
	if (record->xl_rmid != RM_XLOG_ID)
	{
		switch (whichChkpt)
		{
			case 1:
				ereport(LOG,
						(errmsg("invalid resource manager ID in primary checkpoint record at location %s",
						        XLogLocationToString_Long(&RecPtr))));
				break;
			case 2:
				ereport(LOG,
						(errmsg("invalid resource manager ID in secondary checkpoint record at location %s",
						        XLogLocationToString_Long(&RecPtr))));
				break;
			default:
				ereport(LOG,
				(errmsg("invalid resource manager ID in checkpoint record at location %s",
				        XLogLocationToString_Long(&RecPtr))));
				break;
		}
		return NULL;
	}
	if (record->xl_info != XLOG_CHECKPOINT_SHUTDOWN &&
		record->xl_info != XLOG_CHECKPOINT_ONLINE)
	{
		switch (whichChkpt)
		{
			case 1:
				ereport(LOG,
				   (errmsg("invalid xl_info in primary checkpoint record at location %s",
				           XLogLocationToString_Long(&RecPtr))));
				break;
			case 2:
				ereport(LOG,
				 (errmsg("invalid xl_info in secondary checkpoint record at location %s",
				         XLogLocationToString_Long(&RecPtr))));
				break;
			default:
				ereport(LOG,
						(errmsg("invalid xl_info in checkpoint record at location %s",
						        XLogLocationToString_Long(&RecPtr))));
				break;
		}
		return NULL;
	}

	sizeOk = false;
	if (record->xl_len == sizeof(CheckPoint) &&
		record->xl_tot_len == SizeOfXLogRecord + sizeof(CheckPoint))
	{
		sizeOk = true;
	}
	else if (record->xl_len > sizeof(CheckPoint) &&
		record->xl_tot_len > SizeOfXLogRecord + sizeof(CheckPoint))
	{
		delta_xl_len = record->xl_len - sizeof(CheckPoint);
		delta_xl_tot_len = record->xl_tot_len - (SizeOfXLogRecord + sizeof(CheckPoint));

		if (delta_xl_len == delta_xl_tot_len)
		{
			sizeOk = true;
		}
	}

	if (!sizeOk)
	{
		switch (whichChkpt)
		{
			case 1:
				ereport(LOG,
					(errmsg("invalid length of primary checkpoint at location %s",
					        XLogLocationToString_Long(&RecPtr))));
				break;
			case 2:
				ereport(LOG,
				  (errmsg("invalid length of secondary checkpoint record at location %s",
				          XLogLocationToString_Long(&RecPtr))));
				break;
			default:
				ereport(LOG,
						(errmsg("invalid length of checkpoint record at location %s",
						        XLogLocationToString_Long(&RecPtr))));
				break;
		}
		return NULL;
	}
	return record;
}

/*
 * This must be called during startup of a backend process, except that
 * it need not be called in a standalone backend (which does StartupXLOG
 * instead).  We need to initialize the local copies of ThisTimeLineID and
 * RedoRecPtr.
 *
 * Note: before Postgres 8.0, we went to some effort to keep the postmaster
 * process's copies of ThisTimeLineID and RedoRecPtr valid too.  This was
 * unnecessary however, since the postmaster itself never touches XLOG anyway.
 */
void
InitXLOGAccess(void)
{
	/* ThisTimeLineID doesn't change so we need no lock to copy it */
	ThisTimeLineID = XLogCtl->ThisTimeLineID;
	/* Use GetRedoRecPtr to copy the RedoRecPtr safely */
	(void) GetRedoRecPtr();
}

/*
 * Once spawned, a backend may update its local RedoRecPtr from
 * XLogCtl->Insert.RedoRecPtr; it must hold the insert lock or info_lck
 * to do so.  This is done in XLogInsert() or GetRedoRecPtr().
 */
XLogRecPtr
GetRedoRecPtr(void)
{
	/* use volatile pointer to prevent code rearrangement */
	volatile XLogCtlData *xlogctl = XLogCtl;

	SpinLockAcquire(&xlogctl->info_lck);
	Assert(XLByteLE(RedoRecPtr, xlogctl->Insert.RedoRecPtr));
	RedoRecPtr = xlogctl->Insert.RedoRecPtr;
	SpinLockRelease(&xlogctl->info_lck);

	return RedoRecPtr;
}

/*
 * Get the time of the last xlog segment switch
 */
time_t
GetLastSegSwitchTime(void)
{
	time_t		result;

	/* Need WALWriteLock, but shared lock is sufficient */
	LWLockAcquire(WALWriteLock, LW_SHARED);
	result = XLogCtl->Write.lastSegSwitchTime;
	LWLockRelease(WALWriteLock);

	return result;
}

/*
 * GetNextXidAndEpoch - get the current nextXid value and associated epoch
 *
 * This is exported for use by code that would like to have 64-bit XIDs.
 * We don't really support such things, but all XIDs within the system
 * can be presumed "close to" the result, and thus the epoch associated
 * with them can be determined.
 */
void
GetNextXidAndEpoch(TransactionId *xid, uint32 *epoch)
{
	uint32		ckptXidEpoch;
	TransactionId ckptXid;
	TransactionId nextXid;

	/* Must read checkpoint info first, else have race condition */
	{
		/* use volatile pointer to prevent code rearrangement */
		volatile XLogCtlData *xlogctl = XLogCtl;

		SpinLockAcquire(&xlogctl->info_lck);
		ckptXidEpoch = xlogctl->ckptXidEpoch;
		ckptXid = xlogctl->ckptXid;
		SpinLockRelease(&xlogctl->info_lck);
	}

	/* Now fetch current nextXid */
	nextXid = ReadNewTransactionId();

	/*
	 * nextXid is certainly logically later than ckptXid.  So if it's
	 * numerically less, it must have wrapped into the next epoch.
	 */
	if (nextXid < ckptXid)
		ckptXidEpoch++;

	*xid = nextXid;
	*epoch = ckptXidEpoch;
}

/*
 * This must be called ONCE during postmaster or standalone-backend shutdown
 */
void
ShutdownXLOG(int code __attribute__((unused)) , Datum arg __attribute__((unused)) )
{
	ereport(LOG,
			(errmsg("shutting down")));

	if (!GPStandby())
	{
		CreateCheckPoint(true, true);

		/*
		 * Now that we have written the final shutdown checkpoint,
		 * let's shutdown QD mirroring.
		 */
		XLogQDMirrorCloseForShutdown();

		ShutdownCLOG();
		ShutdownSUBTRANS();
		ShutdownMultiXact();
	}
	else
		elog((Debug_print_qd_mirroring ? LOG : DEBUG5),"skipping shutdown steps for standby");

	ereport(LOG,
			(errmsg("database system is shut down"),
					errSendAlert(true),errOmitLocation(true)));
}

/*
 * Perform a checkpoint --- either during shutdown, or on-the-fly
 *
 * If force is true, we force a checkpoint regardless of whether any XLOG
 * activity has occurred since the last one.
 */
void
CreateCheckPoint(bool shutdown, bool force)
{
	CheckPoint	checkPoint;
	XLogRecPtr	recptr;
	XLogCtlInsert *Insert = &XLogCtl->Insert;
	XLogRecData rdata[1];
	uint32		freespace;
	uint32		_logId;
	uint32		_logSeg;
	int			nsegsadded = 0;
	int			nsegsremoved = 0;
	int			nsegsrecycled = 0;

	if (Debug_persistent_recovery_print)
	  {
	    elog(PersistentRecovery_DebugPrintLevel(),
                         "CreateCheckPoint: entering..."
		 );
	  }
#if 0
	if (GpIdentity.dbid == MASTER_DBID)
	  {
	/* KAS debugging */
        int temp1 = 0;
        while (temp1 == 0)
          {
            sleep(1);
          }
	  }
#endif
	if (shutdown && ControlFile->state == DB_STARTUP)
	{
		return;
	}

	/*
	 * Acquire CheckpointLock to ensure only one checkpoint happens at a time.
	 * (This is just pro forma, since in the present system structure there is
	 * only one process that is allowed to issue checkpoints at any given
	 * time.)
	 */
	LWLockAcquire(CheckpointLock, LW_EXCLUSIVE);

	/*
	 * Use a critical section to force system panic if we have trouble.
	 */
	START_CRIT_SECTION();

	if (shutdown)
	{
		ControlFile->state = DB_SHUTDOWNING;
		ControlFile->time = time(NULL);
		UpdateControlFile();
	}

	MemSet(&checkPoint, 0, sizeof(checkPoint));
	checkPoint.ThisTimeLineID = ThisTimeLineID;
	checkPoint.time = time(NULL);

	/*
	 * The WRITE_PERSISTENT_STATE_ORDERED_LOCK gets these locks:
	 *    MirroredLock SHARED, and
	 *    CheckpointStartLock SHARED,
	 *    PersistentObjLock EXCLUSIVE.
	 *
	 * The READ_PERSISTENT_STATE_ORDERED_LOCK gets this lock:
	 *    PersistentObjLock SHARED.
	 *
	 * They do this to prevent Persistent object changes during checkpoint and
	 * prevent persistent object reads while writing.  And acquire the MirroredLock
	 * at a level that blocks DDL during FileRep statechanges...
	 *
	 * We get the CheckpointStartLock to prevent Persistent object writers as
	 * we collect the Master Mirroring information from mmxlog_append_checkpoint_data
	 * until finally after the checkpoint record is inserted into the XLOG to prevent the
	 * persistent information from changing, and all buffers have been flushed to disk..
	 *
	 * We must hold CheckpointStartLock while determining the checkpoint REDO
	 * pointer.  This ensures that any concurrent transaction commits will be
	 * either not yet logged, or logged and recorded in pg_clog. See notes in
	 * RecordTransactionCommit().
	 */
	LWLockAcquire(CheckpointStartLock, LW_EXCLUSIVE);

	/* And we need WALInsertLock too */
	LWLockAcquire(WALInsertLock, LW_EXCLUSIVE);

	/*
	 * If this isn't a shutdown or forced checkpoint, and we have not inserted
	 * any XLOG records since the start of the last checkpoint, skip the
	 * checkpoint.	The idea here is to avoid inserting duplicate checkpoints
	 * when the system is idle. That wastes log space, and more importantly it
	 * exposes us to possible loss of both current and previous checkpoint
	 * records if the machine crashes just as we're writing the update.
	 * (Perhaps it'd make even more sense to checkpoint only when the previous
	 * checkpoint record is in a different xlog page?)
	 *
	 * We have to make two tests to determine that nothing has happened since
	 * the start of the last checkpoint: current insertion point must match
	 * the end of the last checkpoint record, and its redo pointer must point
	 * to itself.
	 */
	if (!shutdown && !force)
	{
		XLogRecPtr	curInsert;

		INSERT_RECPTR(curInsert, Insert, Insert->curridx);
#ifdef originalCheckpointChecking
		if (curInsert.xlogid == ControlFile->checkPoint.xlogid &&
			curInsert.xrecoff == ControlFile->checkPoint.xrecoff +
			MAXALIGN(SizeOfXLogRecord + sizeof(CheckPoint)) &&
			ControlFile->checkPoint.xlogid ==
			ControlFile->checkPointCopy.redo.xlogid &&
			ControlFile->checkPoint.xrecoff ==
			ControlFile->checkPointCopy.redo.xrecoff)
#else
		/*
		 * GP: Modified since the checkpoint record is not fixed length
		 * so we keep track of the last checkpoint locations (beginning and
		 * end) and use thoe values for comparison.
		 */
		if (XLogCtl->haveLastCheckpointLoc &&
			XLByteEQ(XLogCtl->lastCheckpointLoc,ControlFile->checkPoint) &&
			XLByteEQ(curInsert,XLogCtl->lastCheckpointEndLoc) &&
			XLByteEQ(ControlFile->checkPoint,ControlFile->checkPointCopy.redo))
#endif
		{
			LWLockRelease(WALInsertLock);
			LWLockRelease(CheckpointStartLock);
			LWLockRelease(CheckpointLock);

			END_CRIT_SECTION();
			return;
		}
	}

	/*
	 * Compute new REDO record ptr = location of next XLOG record.
	 *
	 * NB: this is NOT necessarily where the checkpoint record itself will be,
	 * since other backends may insert more XLOG records while we're off doing
	 * the buffer flush work.  Those XLOG records are logically after the
	 * checkpoint, even though physically before it.  Got that?
	 */
	freespace = INSERT_FREESPACE(Insert);
	if (freespace < SizeOfXLogRecord)
	{
		(void) AdvanceXLInsertBuffer(false);
		/* OK to ignore update return flag, since we will do flush anyway */
		freespace = INSERT_FREESPACE(Insert);
	}
	INSERT_RECPTR(checkPoint.redo, Insert, Insert->curridx);

	/*
	 * Here we update the shared RedoRecPtr for future XLogInsert calls; this
	 * must be done while holding the insert lock AND the info_lck.
	 *
	 * Note: if we fail to complete the checkpoint, RedoRecPtr will be left
	 * pointing past where it really needs to point.  This is okay; the only
	 * consequence is that XLogInsert might back up whole buffers that it
	 * didn't really need to.  We can't postpone advancing RedoRecPtr because
	 * XLogInserts that happen while we are dumping buffers must assume that
	 * their buffer changes are not included in the checkpoint.
	 */
	{
		/* use volatile pointer to prevent code rearrangement */
		volatile XLogCtlData *xlogctl = XLogCtl;

		SpinLockAcquire(&xlogctl->info_lck);
		RedoRecPtr = xlogctl->Insert.RedoRecPtr = checkPoint.redo;
		SpinLockRelease(&xlogctl->info_lck);
	}

	/*
	 * Now we can release insert lock and checkpoint start lock, allowing
	 * other xacts to proceed even while we are flushing disk buffers.
	 */
	LWLockRelease(WALInsertLock);

	// We used to release the CheckpointStartLock here.  Now we do it after the checkpoint
	// record is written...
//	LWLockRelease(CheckpointStartLock);

	if (!shutdown)
		ereport(DEBUG2,
				(errmsg("checkpoint starting")));

	/*
	 * Get the other info we need for the checkpoint record.
	 */
	LWLockAcquire(XidGenLock, LW_SHARED);
	checkPoint.nextXid = ShmemVariableCache->nextXid;
	LWLockRelease(XidGenLock);

	/* Increase XID epoch if we've wrapped around since last checkpoint */
	checkPoint.nextXidEpoch = ControlFile->checkPointCopy.nextXidEpoch;
	if (checkPoint.nextXid < ControlFile->checkPointCopy.nextXid)
		checkPoint.nextXidEpoch++;

	LWLockAcquire(OidGenLock, LW_SHARED);
	checkPoint.nextOid = ShmemVariableCache->nextOid;
	if (!shutdown)
		checkPoint.nextOid += ShmemVariableCache->oidCount;
	LWLockRelease(OidGenLock);

	MultiXactGetCheckptMulti(shutdown,
							 &checkPoint.nextMulti,
							 &checkPoint.nextMultiOffset);

	/*
	 * Having constructed the checkpoint record, ensure all shmem disk buffers
	 * and commit-log buffers are flushed to disk.
	 *
	 * This I/O could fail for various reasons.  If so, we will fail to
	 * complete the checkpoint, but there is no reason to force a system
	 * panic. Accordingly, exit critical section while doing it.
	 */
	END_CRIT_SECTION();

	CheckPointGuts(checkPoint.redo);

	START_CRIT_SECTION();

	rdata[0].data = (char *) (&checkPoint);
	rdata[0].len = sizeof(checkPoint);
	rdata[0].buffer = InvalidBuffer;
	rdata[0].next = NULL;

	recptr = XLogInsert(RM_XLOG_ID,
			            shutdown ? XLOG_CHECKPOINT_SHUTDOWN : XLOG_CHECKPOINT_ONLINE,
			            rdata);

	if (Debug_persistent_recovery_print)
	{
		elog(PersistentRecovery_DebugPrintLevel(),
			 "CreateCheckPoint: Checkpoint location = %s, total length %u, data length %d",
			 XLogLocationToString(&recptr),
			 XLogLastInsertTotalLen(),
			 XLogLastInsertDataLen());
	}

	// See the comments above where this lock is acquired.
	LWLockRelease(CheckpointStartLock);

	XLogFlush(recptr);

	/*
	 * We now have ProcLastRecPtr = start of actual checkpoint record, recptr
	 * = end of actual checkpoint record.
	 */
	if (shutdown && !XLByteEQ(checkPoint.redo, ProcLastRecPtr))
		ereport(PANIC,
				(errmsg("concurrent transaction log activity while database system is shutting down")));

	/*
	 * Select point at which we can truncate the log, which we base on the
	 * prior checkpoint's earliest info or the oldest prepared transaction xlog record's info.
	 */
	XLByteToSeg(ControlFile->checkPointCopy.redo, _logId, _logSeg);

	elog((Debug_print_qd_mirroring ? LOG : DEBUG1),
		 "CreateCheckPoint: previous checkpoint's earliest info (copy redo location %s, previous checkpoint location %s)",
		 XLogLocationToString(&ControlFile->checkPointCopy.redo),
		 XLogLocationToString2(&ControlFile->prevCheckPoint));
	FtsQDMirroringNewCheckpointLoc(&ControlFile->prevCheckPoint);

	/*
	 * Update the control file.
	 */
	LWLockAcquire(ControlFileLock, LW_EXCLUSIVE);
	if (shutdown)
		ControlFile->state = DB_SHUTDOWNED;
	ControlFile->prevCheckPoint = ControlFile->checkPoint;
	ControlFile->checkPoint = ProcLastRecPtr;
	ControlFile->checkPointCopy = checkPoint;
	ControlFile->time = time(NULL);

	/*
	 * Save the last checkpoint position.
	 */
	XLogCtl->haveLastCheckpointLoc = true;
	XLogCtl->lastCheckpointLoc = ProcLastRecPtr;
	XLogCtl->lastCheckpointEndLoc = ProcLastRecEnd;

	UpdateControlFile();
	LWLockRelease(ControlFileLock);

	/* Update shared-memory copy of checkpoint XID/epoch */
	{
		/* use volatile pointer to prevent code rearrangement */
		volatile XLogCtlData *xlogctl = XLogCtl;

		SpinLockAcquire(&xlogctl->info_lck);
		xlogctl->ckptXidEpoch = checkPoint.nextXidEpoch;
		xlogctl->ckptXid = checkPoint.nextXid;
		SpinLockRelease(&xlogctl->info_lck);
	}

	/*
	 * We are now done with critical updates; no need for system panic if we
	 * have trouble while fooling with offline log segments.
	 */
	END_CRIT_SECTION();

	/*
	 * Delete offline log files (those no longer needed even for previous
	 * checkpoint).
	 */
	if (gp_keep_all_xlog == false && (_logId || _logSeg))
	{
		PrevLogSeg(_logId, _logSeg);
		MoveOfflineLogs(_logId, _logSeg, recptr,
						&nsegsremoved, &nsegsrecycled);
	}

	/*
	 * Make more log segments if needed.  (Do this after deleting offline log
	 * segments, to avoid having peak disk space usage higher than necessary.)
	 */
	if (!shutdown)
		nsegsadded = PreallocXlogFiles(recptr);

	/*
	 * Truncate pg_subtrans if possible.  We can throw away all data before
	 * the oldest XMIN of any running transaction.	No future transaction will
	 * attempt to reference any pg_subtrans entry older than that (see Asserts
	 * in subtrans.c).	During recovery, though, we mustn't do this because
	 * StartupSUBTRANS hasn't been called yet.
	 */
	if (!InRecovery)
		TruncateSUBTRANS(GetOldestXmin(true));

	if (!shutdown)
		ereport(DEBUG2,
				(errmsg("checkpoint complete; %d transaction log file(s) added, %d removed, %d recycled",
						nsegsadded, nsegsremoved, nsegsrecycled)));

	if (Debug_persistent_recovery_print)
		elog(PersistentRecovery_DebugPrintLevel(),
			 "CreateCheckPoint: shutdown %s, force %s, checkpoint location %s, redo location %s",
			 (shutdown ? "true" : "false"),
			 (force ? "true" : "false"),
			 XLogLocationToString(&ControlFile->checkPoint),
			 XLogLocationToString2(&checkPoint.redo));

	LWLockRelease(CheckpointLock);
}

/*
 * Flush all data in shared memory to disk, and fsync
 *
 * This is the common code shared between regular checkpoints and
 * recovery restartpoints.
 */
static void
CheckPointGuts(XLogRecPtr checkPointRedo)
{
	CheckPointCLOG();
	CheckPointSUBTRANS();
	CheckPointMultiXact();
	FlushBufferPool();			/* performs all required fsyncs */
	/* We deliberately delay 2PC checkpointing as long as possible */
	CheckPointTwoPhase(checkPointRedo);
}

static void Checkpoint_RecoveryPass(XLogRecPtr checkPointRedo)
{
	CheckPointGuts(checkPointRedo);
}

/*
 * Set a recovery restart point if appropriate
 *
 * This is similar to CreateCheckpoint, but is used during WAL recovery
 * to establish a point from which recovery can roll forward without
 * replaying the entire recovery log.  This function is called each time
 * a checkpoint record is read from XLOG; it must determine whether a
 * restartpoint is needed or not.
 */
static void
RecoveryRestartPoint(const CheckPoint *checkPoint)
{
//	int			elapsed_secs;
	int			rmid;

	/*
	 * Do nothing if the elapsed time since the last restartpoint is less than
	 * half of checkpoint_timeout.	(We use a value less than
	 * checkpoint_timeout so that variations in the timing of checkpoints on
	 * the master, or speed of transmission of WAL segments to a slave, won't
	 * make the slave skip a restartpoint once it's synced with the master.)
	 * Checking true elapsed time keeps us from doing restartpoints too often
	 * while rapidly scanning large amounts of WAL.
	 */

	// UNDONE: For now, turn this off!
//	elapsed_secs = time(NULL) - ControlFile->time;
//	if (elapsed_secs < CheckPointTimeout / 2)
//		return;

	/*
	 * Is it safe to checkpoint?  We must ask each of the resource managers
	 * whether they have any partial state information that might prevent a
	 * correct restart from this point.  If so, we skip this opportunity, but
	 * return at the next checkpoint record for another try.
	 */
	for (rmid = 0; rmid <= RM_MAX_ID; rmid++)
	{
		if (RmgrTable[rmid].rm_safe_restartpoint != NULL)
			if (!(RmgrTable[rmid].rm_safe_restartpoint()))
				return;
	}

	/*
	 * OK, force data out to disk
	 */
	CheckPointGuts(checkPoint->redo);

	/*
	 * Update pg_control so that any subsequent crash will restart from this
	 * checkpoint.	Note: ReadRecPtr gives the XLOG address of the checkpoint
	 * record itself.
	 */
	ControlFile->prevCheckPoint = ControlFile->checkPoint;
	ControlFile->checkPoint = ReadRecPtr;
	ControlFile->checkPointCopy = *checkPoint;
	ControlFile->time = time(NULL);

	/*
	 * Save the last checkpoint position.
	 */
	XLogCtl->haveLastCheckpointLoc = true;
	XLogCtl->lastCheckpointLoc = ReadRecPtr;
	XLogCtl->lastCheckpointEndLoc = EndRecPtr;

	UpdateControlFile();

	ereport(LOG,
			(errmsg("recovery restart point at %X/%X",
					checkPoint->redo.xlogid, checkPoint->redo.xrecoff)));
	elog((Debug_print_qd_mirroring ? LOG : DEBUG1), "RecoveryRestartPoint: checkpoint copy redo location %s, previous checkpoint location %s",
		 XLogLocationToString(&ControlFile->checkPointCopy.redo),
		 XLogLocationToString2(&ControlFile->prevCheckPoint));
}

/*
 * Write a NEXTOID log record
 */
void
XLogPutNextOid(Oid nextOid)
{
	XLogRecData rdata;

	rdata.data = (char *) (&nextOid);
	rdata.len = sizeof(Oid);
	rdata.buffer = InvalidBuffer;
	rdata.next = NULL;
	(void) XLogInsert(RM_XLOG_ID, XLOG_NEXTOID, &rdata);

	/*
	 * We need not flush the NEXTOID record immediately, because any of the
	 * just-allocated OIDs could only reach disk as part of a tuple insert or
	 * update that would have its own XLOG record that must follow the NEXTOID
	 * record.	Therefore, the standard buffer LSN interlock applied to those
	 * records will ensure no such OID reaches disk before the NEXTOID record
	 * does.
	 *
	 * Note, however, that the above statement only covers state "within" the
	 * database.  When we use a generated OID as a file or directory name,
	 * we are in a sense violating the basic WAL rule, because that filesystem
	 * change may reach disk before the NEXTOID WAL record does.  The impact
	 * of this is that if a database crash occurs immediately afterward,
	 * we might after restart re-generate the same OID and find that it
	 * conflicts with the leftover file or directory.  But since for safety's
	 * sake we always loop until finding a nonconflicting filename, this poses
	 * no real problem in practice. See pgsql-hackers discussion 27-Sep-2006.
	 */
}

/*
 * Write an XLOG SWITCH record.
 *
 * Here we just blindly issue an XLogInsert request for the record.
 * All the magic happens inside XLogInsert.
 *
 * The return value is either the end+1 address of the switch record,
 * or the end+1 address of the prior segment if we did not need to
 * write a switch record because we are already at segment start.
 */
XLogRecPtr
RequestXLogSwitch(void)
{
	XLogRecPtr	RecPtr;
	XLogRecData rdata;

	/* XLOG SWITCH, alone among xlog record types, has no data */
	rdata.buffer = InvalidBuffer;
	rdata.data = NULL;
	rdata.len = 0;
	rdata.next = NULL;

	RecPtr = XLogInsert(RM_XLOG_ID, XLOG_SWITCH, &rdata);

	return RecPtr;
}

/*
 * XLOG resource manager's routines
 *
 * Definitions of info values are in include/catalog/pg_control.h, though
 * not all records types are related to control file processing.
 */
void
xlog_redo(XLogRecPtr beginLoc __attribute__((unused)), XLogRecPtr lsn __attribute__((unused)), XLogRecord *record)
{
	uint8		info = record->xl_info & ~XLR_INFO_MASK;

	if (info == XLOG_NEXTOID)
	{
		Oid			nextOid;

		memcpy(&nextOid, XLogRecGetData(record), sizeof(Oid));
		if (ShmemVariableCache->nextOid < nextOid)
		{
			ShmemVariableCache->nextOid = nextOid;
			ShmemVariableCache->oidCount = 0;
		}
	}
	else if (info == XLOG_CHECKPOINT_SHUTDOWN)
	{
		CheckPoint	checkPoint;

		memcpy(&checkPoint, XLogRecGetData(record), sizeof(CheckPoint));
		/* In a SHUTDOWN checkpoint, believe the counters exactly */
		ShmemVariableCache->nextXid = checkPoint.nextXid;
		ShmemVariableCache->nextOid = checkPoint.nextOid;
		ShmemVariableCache->oidCount = 0;
		MultiXactSetNextMXact(checkPoint.nextMulti,
							  checkPoint.nextMultiOffset);

		/* ControlFile->checkPointCopy always tracks the latest ckpt XID */
		ControlFile->checkPointCopy.nextXidEpoch = checkPoint.nextXidEpoch;
		ControlFile->checkPointCopy.nextXid = checkPoint.nextXid;

		/*
		 * TLI may change in a shutdown checkpoint, but it shouldn't decrease
		 */
		if (checkPoint.ThisTimeLineID != ThisTimeLineID)
		{
			if (checkPoint.ThisTimeLineID < ThisTimeLineID ||
				!list_member_int(expectedTLIs,
								 (int) checkPoint.ThisTimeLineID))
				ereport(PANIC,
						(errmsg("unexpected timeline ID %u (after %u) in checkpoint record",
								checkPoint.ThisTimeLineID, ThisTimeLineID)));
			/* Following WAL records should be run with new TLI */
			ThisTimeLineID = checkPoint.ThisTimeLineID;
		}

		RecoveryRestartPoint(&checkPoint);
	}
	else if (info == XLOG_CHECKPOINT_ONLINE)
	{
		CheckPoint	checkPoint;

		memcpy(&checkPoint, XLogRecGetData(record), sizeof(CheckPoint));
		/* In an ONLINE checkpoint, treat the counters like NEXTOID */
		if (TransactionIdPrecedes(ShmemVariableCache->nextXid,
								  checkPoint.nextXid))
			ShmemVariableCache->nextXid = checkPoint.nextXid;
		if (ShmemVariableCache->nextOid < checkPoint.nextOid)
		{
			ShmemVariableCache->nextOid = checkPoint.nextOid;
			ShmemVariableCache->oidCount = 0;
		}
		MultiXactAdvanceNextMXact(checkPoint.nextMulti,
								  checkPoint.nextMultiOffset);

		/* ControlFile->checkPointCopy always tracks the latest ckpt XID */
		ControlFile->checkPointCopy.nextXidEpoch = checkPoint.nextXidEpoch;
		ControlFile->checkPointCopy.nextXid = checkPoint.nextXid;

		/* TLI should not change in an on-line checkpoint */
		if (checkPoint.ThisTimeLineID != ThisTimeLineID)
			ereport(PANIC,
					(errmsg("unexpected timeline ID %u (should be %u) in checkpoint record",
							checkPoint.ThisTimeLineID, ThisTimeLineID)));

		RecoveryRestartPoint(&checkPoint);
	}
	else if (info == XLOG_SWITCH)
	{
		/* nothing to do here */
	}
}

void
xlog_desc(StringInfo buf, XLogRecPtr beginLoc, XLogRecord *record)
{
	uint8		info = record->xl_info & ~XLR_INFO_MASK;
	char		*rec = XLogRecGetData(record);

	if (info == XLOG_CHECKPOINT_SHUTDOWN ||
		info == XLOG_CHECKPOINT_ONLINE)
	{
		CheckPoint *checkpoint = (CheckPoint *) rec;

		appendStringInfo(buf, "checkpoint: redo %X/%X; undo %X/%X; "
						 "tli %u; xid %u/%u; oid %u; multi %u; offset %u; %s",
						 checkpoint->redo.xlogid, checkpoint->redo.xrecoff,
						 checkpoint->undo.xlogid, checkpoint->undo.xrecoff,
						 checkpoint->ThisTimeLineID,
						 checkpoint->nextXidEpoch, checkpoint->nextXid,
						 checkpoint->nextOid,
						 checkpoint->nextMulti,
						 checkpoint->nextMultiOffset,
				 (info == XLOG_CHECKPOINT_SHUTDOWN) ? "shutdown" : "online");
	}
	else if (info == XLOG_NEXTOID)
	{
		Oid			nextOid;

		memcpy(&nextOid, rec, sizeof(Oid));
		appendStringInfo(buf, "nextOid: %u", nextOid);
	}
	else if (info == XLOG_SWITCH)
	{
		appendStringInfo(buf, "xlog switch");
	}
	else
		appendStringInfo(buf, "UNKNOWN");
}

void
XLog_OutRec(StringInfo buf, XLogRecord *record)
{
	int			i;

	appendStringInfo(buf, "prev %X/%X; xid %u",
					 record->xl_prev.xlogid, record->xl_prev.xrecoff,
					 record->xl_xid);

	for (i = 0; i < XLR_MAX_BKP_BLOCKS; i++)
	{
		if (record->xl_info & XLR_SET_BKP_BLOCK(i))
			appendStringInfo(buf, "; bkpb%d", i + 1);
	}

	appendStringInfo(buf, ": %s", RmgrTable[record->xl_rmid].rm_name);
}


/*
 * GUC support
 */
const char *
assign_xlog_sync_method(const char *method, bool doit, GucSource source __attribute__((unused)) )
{
	int			new_sync_method;
	int			new_sync_bit;

	if (pg_strcasecmp(method, "fsync") == 0)
	{
		new_sync_method = SYNC_METHOD_FSYNC;
		new_sync_bit = 0;
	}
#ifdef HAVE_FSYNC_WRITETHROUGH
	else if (pg_strcasecmp(method, "fsync_writethrough") == 0)
	{
		new_sync_method = SYNC_METHOD_FSYNC_WRITETHROUGH;
		new_sync_bit = 0;
	}
#endif
#ifdef HAVE_FDATASYNC
	else if (pg_strcasecmp(method, "fdatasync") == 0)
	{
		new_sync_method = SYNC_METHOD_FDATASYNC;
		new_sync_bit = 0;
	}
#endif
#ifdef OPEN_SYNC_FLAG
	else if (pg_strcasecmp(method, "open_sync") == 0)
	{
		new_sync_method = SYNC_METHOD_OPEN;
		new_sync_bit = OPEN_SYNC_FLAG;
	}
#endif
#ifdef OPEN_DATASYNC_FLAG
	else if (pg_strcasecmp(method, "open_datasync") == 0)
	{
		new_sync_method = SYNC_METHOD_OPEN;
		new_sync_bit = OPEN_DATASYNC_FLAG;
	}
#endif
	else
		return NULL;

	if (!doit)
		return method;

	if (sync_method != new_sync_method || open_sync_bit != new_sync_bit)
	{
		/*
		 * To ensure that no blocks escape unsynced, force an fsync on the
		 * currently open log segment (if any).  Also, if the open flag is
		 * changing, close the log file so it will be reopened (with new flag
		 * bit) at next use.
		 */
		if (MirroredFlatFile_IsActive(&mirroredLogFileOpen))
		{
			if (MirroredFlatFile_Flush(
								&mirroredLogFileOpen,
								/* suppressError */ true))
				ereport(PANIC,
						(errcode_for_file_access(),
						 errmsg("could not fsync log file %u, segment %u: %m",
								openLogId, openLogSeg)));
			if (open_sync_bit != new_sync_bit)
				XLogFileClose();
		}
		sync_method = new_sync_method;
		open_sync_bit = new_sync_bit;
	}

	return method;
}

#ifdef suppress
/*
 * Issue appropriate kind of fsync (if any) on the current XLOG output file
 */
static void
issue_xlog_fsync(void)
{
	switch (sync_method)
	{
		case SYNC_METHOD_FSYNC:
			if (pg_fsync_no_writethrough(openLogFile) != 0)
				ereport(PANIC,
						(errcode_for_file_access(),
						 errmsg("could not fsync log file %u, segment %u: %m",
								openLogId, openLogSeg)));
			break;
#ifdef HAVE_FSYNC_WRITETHROUGH
		case SYNC_METHOD_FSYNC_WRITETHROUGH:
			if (pg_fsync_writethrough(openLogFile) != 0)
				ereport(PANIC,
						(errcode_for_file_access(),
						 errmsg("could not fsync write-through log file %u, segment %u: %m",
								openLogId, openLogSeg)));
			break;
#endif
#ifdef HAVE_FDATASYNC
		case SYNC_METHOD_FDATASYNC:
			if (pg_fdatasync(openLogFile) != 0)
				ereport(PANIC,
						(errcode_for_file_access(),
					errmsg("could not fdatasync log file %u, segment %u: %m",
						   openLogId, openLogSeg)));
			break;
#endif
		case SYNC_METHOD_OPEN:
			/* write synced it already */
			break;
		default:
			elog(PANIC, "unrecognized wal_sync_method: %d", sync_method);
			break;
	}
}
#endif

/*
 * pg_start_backup: set up for taking an on-line backup dump
 *
 * Essentially what this does is to create a backup label file in $PGDATA,
 * where it will be archived as part of the backup dump.  The label file
 * contains the user-supplied label string (typically this would be used
 * to tell where the backup dump will be stored) and the starting time and
 * starting WAL location for the dump.
 */
Datum
pg_start_backup(PG_FUNCTION_ARGS)
{
	text	   *backupid = PG_GETARG_TEXT_P(0);
	text	   *result;
	char	   *backupidstr;
	XLogRecPtr	checkpointloc;
	XLogRecPtr	startpoint;
	pg_time_t		stamp_time;
	char		strfbuf[128];
	char		xlogfilename[MAXFNAMELEN];
	uint32		_logId;
	uint32		_logSeg;
	struct stat stat_buf;
	FILE	   *fp;

	if (!superuser())
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
				 (errmsg("must be superuser to run a backup"))));

	if (!XLogArchivingActive())
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 (errmsg("WAL archiving is not active"),
				  (errhint("archive_command must be defined before "
						   "online backups can be made safely.")))));

	backupidstr = DatumGetCString(DirectFunctionCall1(textout,
												 PointerGetDatum(backupid)));

	/*
	 * Mark backup active in shared memory.  We must do full-page WAL writes
	 * during an on-line backup even if not doing so at other times, because
	 * it's quite possible for the backup dump to obtain a "torn" (partially
	 * written) copy of a database page if it reads the page concurrently with
	 * our write to the same page.	This can be fixed as long as the first
	 * write to the page in the WAL sequence is a full-page write. Hence, we
	 * turn on forcePageWrites and then force a CHECKPOINT, to ensure there
	 * are no dirty pages in shared memory that might get dumped while the
	 * backup is in progress without having a corresponding WAL record.  (Once
	 * the backup is complete, we need not force full-page writes anymore,
	 * since we expect that any pages not modified during the backup interval
	 * must have been correctly captured by the backup.)
	 *
	 * We must hold WALInsertLock to change the value of forcePageWrites, to
	 * ensure adequate interlocking against XLogInsert().
	 */
	LWLockAcquire(WALInsertLock, LW_EXCLUSIVE);
	if (XLogCtl->Insert.forcePageWrites)
	{
		LWLockRelease(WALInsertLock);
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("a backup is already in progress"),
				 errhint("Run pg_stop_backup() and try again.")));
	}
	XLogCtl->Insert.forcePageWrites = true;
	LWLockRelease(WALInsertLock);

	/* Ensure we release forcePageWrites if fail below */
	PG_ENSURE_ERROR_CLEANUP(pg_start_backup_callback, (Datum) 0);
	{
		/*
		 * Force a CHECKPOINT.	Aside from being necessary to prevent torn
		 * page problems, this guarantees that two successive backup runs will
		 * have different checkpoint positions and hence different history
		 * file names, even if nothing happened in between.
		 *
		 * We don't use CHECKPOINT_IMMEDIATE, hence this can take awhile.
		 */
		RequestCheckpoint(true, false);

		/*
		 * Now we need to fetch the checkpoint record location, and also its
		 * REDO pointer.  The oldest point in WAL that would be needed to
		 * restore starting from the checkpoint is precisely the REDO pointer.
		 */
		LWLockAcquire(ControlFileLock, LW_EXCLUSIVE);
		checkpointloc = ControlFile->checkPoint;
		startpoint = ControlFile->checkPointCopy.redo;
		LWLockRelease(ControlFileLock);

		XLByteToSeg(startpoint, _logId, _logSeg);
		XLogFileName(xlogfilename, ThisTimeLineID, _logId, _logSeg);

		/* Use the log timezone here, not the session timezone */
		stamp_time = (pg_time_t) time(NULL);
		pg_strftime(strfbuf, sizeof(strfbuf),
				 "%Y-%m-%d %H:%M:%S %Z",
				 pg_localtime(&stamp_time, log_timezone ? log_timezone : gmt_timezone));

		/*
		 * Check for existing backup label --- implies a backup is already
		 * running.  (XXX given that we checked forcePageWrites above, maybe
		 * it would be OK to just unlink any such label file?)
		 */
		if (stat(BACKUP_LABEL_FILE, &stat_buf) != 0)
		{
			if (errno != ENOENT)
				ereport(ERROR,
						(errcode_for_file_access(),
						 errmsg("could not stat file \"%s\": %m",
								BACKUP_LABEL_FILE)));
		}
		else
			ereport(ERROR,
					(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
					 errmsg("a backup is already in progress"),
					 errhint("If you're sure there is no backup in progress, remove file \"%s\" and try again.",
							 BACKUP_LABEL_FILE)));

		/*
		 * Okay, write the file
		 */
		fp = AllocateFile(BACKUP_LABEL_FILE, "w");
		if (!fp)
			ereport(ERROR,
					(errcode_for_file_access(),
					 errmsg("could not create file \"%s\": %m",
							BACKUP_LABEL_FILE)));
		fprintf(fp, "START WAL LOCATION: %X/%X (file %s)\n",
				startpoint.xlogid, startpoint.xrecoff, xlogfilename);
		fprintf(fp, "CHECKPOINT LOCATION: %X/%X\n",
				checkpointloc.xlogid, checkpointloc.xrecoff);
		fprintf(fp, "START TIME: %s\n", strfbuf);
		fprintf(fp, "LABEL: %s\n", backupidstr);
		if (fflush(fp) || ferror(fp) || FreeFile(fp))
			ereport(ERROR,
					(errcode_for_file_access(),
					 errmsg("could not write file \"%s\": %m",
							BACKUP_LABEL_FILE)));
	}
	PG_END_ENSURE_ERROR_CLEANUP(pg_start_backup_callback, (Datum) 0);

	/*
	 * We're done.  As a convenience, return the starting WAL location.
	 */
	snprintf(xlogfilename, sizeof(xlogfilename), "%X/%X",
			 startpoint.xlogid, startpoint.xrecoff);
	result = DatumGetTextP(DirectFunctionCall1(textin,
											 CStringGetDatum(xlogfilename)));
	PG_RETURN_TEXT_P(result);
}

/* Error cleanup callback for pg_start_backup */
static void
pg_start_backup_callback(int code __attribute__((unused)), Datum arg __attribute__((unused)))
{
	/* Turn off forcePageWrites on failure */
	LWLockAcquire(WALInsertLock, LW_EXCLUSIVE);
	XLogCtl->Insert.forcePageWrites = false;
	LWLockRelease(WALInsertLock);
}

/*
 * pg_stop_backup: finish taking an on-line backup dump
 *
 * We remove the backup label file created by pg_start_backup, and instead
 * create a backup history file in pg_xlog (whence it will immediately be
 * archived).  The backup history file contains the same info found in
 * the label file, plus the backup-end time and WAL location.
 * Note: different from CancelBackup which just cancels online backup mode.
 */
Datum
pg_stop_backup(PG_FUNCTION_ARGS __attribute__((unused)) )
{
	text	   *result;
	XLogRecPtr	startpoint;
	XLogRecPtr	stoppoint;
	pg_time_t		stamp_time;
	char		strfbuf[128];
	char		histfilepath[MAXPGPATH];
	char		startxlogfilename[MAXFNAMELEN];
	char		stopxlogfilename[MAXFNAMELEN];
	uint32		_logId;
	uint32		_logSeg;
	FILE	   *lfp;
	FILE	   *fp;
	char		ch;
	int			ich;

	if (!superuser())
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
				 (errmsg("must be superuser to run a backup"))));

	/*
	 * OK to clear forcePageWrites
	 */
	LWLockAcquire(WALInsertLock, LW_EXCLUSIVE);
	XLogCtl->Insert.forcePageWrites = false;
	LWLockRelease(WALInsertLock);

	/*
	 * Force a switch to a new xlog segment file, so that the backup is valid
	 * as soon as archiver moves out the current segment file. We'll report
	 * the end address of the XLOG SWITCH record as the backup stopping point.
	 */
	stoppoint = RequestXLogSwitch();

	XLByteToSeg(stoppoint, _logId, _logSeg);
	XLogFileName(stopxlogfilename, ThisTimeLineID, _logId, _logSeg);

	/* Use the log timezone here, not the session timezone */
	stamp_time = (pg_time_t) time(NULL);
	pg_strftime(strfbuf, sizeof(strfbuf),
			 "%Y-%m-%d %H:%M:%S %Z",
			 pg_localtime(&stamp_time, log_timezone ? log_timezone : gmt_timezone));

	/*
	 * Open the existing label file
	 */
	lfp = AllocateFile(BACKUP_LABEL_FILE, "r");
	if (!lfp)
	{
		if (errno != ENOENT)
			ereport(ERROR,
					(errcode_for_file_access(),
					 errmsg("could not read file \"%s\": %m",
							BACKUP_LABEL_FILE)));
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("a backup is not in progress")));
	}

	/*
	 * Read and parse the START WAL LOCATION line (this code is pretty crude,
	 * but we are not expecting any variability in the file format).
	 */
	if (fscanf(lfp, "START WAL LOCATION: %X/%X (file %24s)%c",
			   &startpoint.xlogid, &startpoint.xrecoff, startxlogfilename,
			   &ch) != 4 || ch != '\n')
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("invalid data in file \"%s\"", BACKUP_LABEL_FILE)));

	/*
	 * Write the backup history file
	 */
	XLByteToSeg(startpoint, _logId, _logSeg);
	BackupHistoryFilePath(histfilepath, ThisTimeLineID, _logId, _logSeg,
						  startpoint.xrecoff % XLogSegSize);
	fp = AllocateFile(histfilepath, "w");
	if (!fp)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not create file \"%s\": %m",
						histfilepath)));
	fprintf(fp, "START WAL LOCATION: %X/%X (file %s)\n",
			startpoint.xlogid, startpoint.xrecoff, startxlogfilename);
	fprintf(fp, "STOP WAL LOCATION: %X/%X (file %s)\n",
			stoppoint.xlogid, stoppoint.xrecoff, stopxlogfilename);
	/* transfer remaining lines from label to history file */
	while ((ich = fgetc(lfp)) != EOF)
		fputc(ich, fp);
	fprintf(fp, "STOP TIME: %s\n", strfbuf);
	if (fflush(fp) || ferror(fp) || FreeFile(fp))
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not write file \"%s\": %m",
						histfilepath)));

	/*
	 * Close and remove the backup label file
	 */
	if (ferror(lfp) || FreeFile(lfp))
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not read file \"%s\": %m",
						BACKUP_LABEL_FILE)));
	if (unlink(BACKUP_LABEL_FILE) != 0)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not remove file \"%s\": %m",
						BACKUP_LABEL_FILE)));

	/*
	 * Clean out any no-longer-needed history files.  As a side effect, this
	 * will post a .ready file for the newly created history file, notifying
	 * the archiver that history file may be archived immediately.
	 */
	CleanupBackupHistory();

	/*
	 * We're done.  As a convenience, return the ending WAL location.
	 */
	snprintf(stopxlogfilename, sizeof(stopxlogfilename), "%X/%X",
			 stoppoint.xlogid, stoppoint.xrecoff);
	result = DatumGetTextP(DirectFunctionCall1(textin,
										 CStringGetDatum(stopxlogfilename)));
	PG_RETURN_TEXT_P(result);
}

/*
 * pg_switch_xlog: switch to next xlog file
 */
Datum
pg_switch_xlog(PG_FUNCTION_ARGS __attribute__((unused)) )
{
	text	   *result;
	XLogRecPtr	switchpoint;
	char		location[MAXFNAMELEN];

	if (!superuser())
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
				 (errmsg("must be superuser to switch transaction log files"))));

	switchpoint = RequestXLogSwitch();

	/*
	 * As a convenience, return the WAL location of the switch record
	 */
	snprintf(location, sizeof(location), "%X/%X",
			 switchpoint.xlogid, switchpoint.xrecoff);
	result = DatumGetTextP(DirectFunctionCall1(textin,
											   CStringGetDatum(location)));
	PG_RETURN_TEXT_P(result);
}

/*
 * Report the current WAL write location (same format as pg_start_backup etc)
 *
 * This is useful for determining how much of WAL is visible to an external
 * archiving process.  Note that the data before this point is written out
 * to the kernel, but is not necessarily synced to disk.
 */
Datum
pg_current_xlog_location(PG_FUNCTION_ARGS __attribute__((unused)) )
{
	text	   *result;
	char		location[MAXFNAMELEN];

	/* Make sure we have an up-to-date local LogwrtResult */
	{
		/* use volatile pointer to prevent code rearrangement */
		volatile XLogCtlData *xlogctl = XLogCtl;

		SpinLockAcquire(&xlogctl->info_lck);
		LogwrtResult = xlogctl->LogwrtResult;
		SpinLockRelease(&xlogctl->info_lck);
	}

	snprintf(location, sizeof(location), "%X/%X",
			 LogwrtResult.Write.xlogid, LogwrtResult.Write.xrecoff);

	result = DatumGetTextP(DirectFunctionCall1(textin,
											   CStringGetDatum(location)));
	PG_RETURN_TEXT_P(result);
}

/*
 * Report the current WAL insert location (same format as pg_start_backup etc)
 *
 * This function is mostly for debugging purposes.
 */
Datum
pg_current_xlog_insert_location(PG_FUNCTION_ARGS __attribute__((unused)) )
{
	text	   *result;
	XLogCtlInsert *Insert = &XLogCtl->Insert;
	XLogRecPtr	current_recptr;
	char		location[MAXFNAMELEN];

	/*
	 * Get the current end-of-WAL position ... shared lock is sufficient
	 */
	LWLockAcquire(WALInsertLock, LW_SHARED);
	INSERT_RECPTR(current_recptr, Insert, Insert->curridx);
	LWLockRelease(WALInsertLock);

	snprintf(location, sizeof(location), "%X/%X",
			 current_recptr.xlogid, current_recptr.xrecoff);

	result = DatumGetTextP(DirectFunctionCall1(textin,
											   CStringGetDatum(location)));
	PG_RETURN_TEXT_P(result);
}

/*
 * Compute an xlog file name and decimal byte offset given a WAL location,
 * such as is returned by pg_stop_backup() or pg_xlog_switch().
 *
 * Note that a location exactly at a segment boundary is taken to be in
 * the previous segment.  This is usually the right thing, since the
 * expected usage is to determine which xlog file(s) are ready to archive.
 */
Datum
pg_xlogfile_name_offset(PG_FUNCTION_ARGS)
{
	text	   *location = PG_GETARG_TEXT_P(0);
	char	   *locationstr;
	unsigned int uxlogid;
	unsigned int uxrecoff;
	uint32		xlogid;
	uint32		xlogseg;
	uint32		xrecoff;
	XLogRecPtr	locationpoint;
	char		xlogfilename[MAXFNAMELEN];
	Datum		values[2];
	bool		isnull[2];
	TupleDesc	resultTupleDesc;
	HeapTuple	resultHeapTuple;
	Datum		result;

	/*
	 * Read input and parse
	 */
	locationstr = DatumGetCString(DirectFunctionCall1(textout,
												 PointerGetDatum(location)));

	if (sscanf(locationstr, "%X/%X", &uxlogid, &uxrecoff) != 2)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("could not parse transaction log location \"%s\"",
						locationstr)));

	locationpoint.xlogid = uxlogid;
	locationpoint.xrecoff = uxrecoff;

	/*
	 * Construct a tuple descriptor for the result row.  This must match this
	 * function's pg_proc entry!
	 */
	resultTupleDesc = CreateTemplateTupleDesc(2, false);
	TupleDescInitEntry(resultTupleDesc, (AttrNumber) 1, "file_name",
					   TEXTOID, -1, 0);
	TupleDescInitEntry(resultTupleDesc, (AttrNumber) 2, "file_offset",
					   INT4OID, -1, 0);

	resultTupleDesc = BlessTupleDesc(resultTupleDesc);

	/*
	 * xlogfilename
	 */
	XLByteToPrevSeg(locationpoint, xlogid, xlogseg);
	XLogFileName(xlogfilename, ThisTimeLineID, xlogid, xlogseg);

	values[0] = DirectFunctionCall1(textin,
									CStringGetDatum(xlogfilename));
	isnull[0] = false;

	/*
	 * offset
	 */
	xrecoff = locationpoint.xrecoff - xlogseg * XLogSegSize;

	values[1] = UInt32GetDatum(xrecoff);
	isnull[1] = false;

	/*
	 * Tuple jam: Having first prepared your Datums, then squash together
	 */
	resultHeapTuple = heap_form_tuple(resultTupleDesc, values, isnull);

	result = HeapTupleGetDatum(resultHeapTuple);

	PG_RETURN_DATUM(result);
}

/*
 * Compute an xlog file name given a WAL location,
 * such as is returned by pg_stop_backup() or pg_xlog_switch().
 */
Datum
pg_xlogfile_name(PG_FUNCTION_ARGS)
{
	text	   *location = PG_GETARG_TEXT_P(0);
	text	   *result;
	char	   *locationstr;
	unsigned int uxlogid;
	unsigned int uxrecoff;
	uint32		xlogid;
	uint32		xlogseg;
	XLogRecPtr	locationpoint;
	char		xlogfilename[MAXFNAMELEN];

	locationstr = DatumGetCString(DirectFunctionCall1(textout,
												 PointerGetDatum(location)));

	if (sscanf(locationstr, "%X/%X", &uxlogid, &uxrecoff) != 2)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("could not parse transaction log location \"%s\"",
						locationstr)));

	locationpoint.xlogid = uxlogid;
	locationpoint.xrecoff = uxrecoff;

	XLByteToPrevSeg(locationpoint, xlogid, xlogseg);
	XLogFileName(xlogfilename, ThisTimeLineID, xlogid, xlogseg);

	result = DatumGetTextP(DirectFunctionCall1(textin,
											 CStringGetDatum(xlogfilename)));
	PG_RETURN_TEXT_P(result);
}

/*
 * read_backup_label: check to see if a backup_label file is present
 *
 * If we see a backup_label during recovery, we assume that we are recovering
 * from a backup dump file, and we therefore roll forward from the checkpoint
 * identified by the label file, NOT what pg_control says.	This avoids the
 * problem that pg_control might have been archived one or more checkpoints
 * later than the start of the dump, and so if we rely on it as the start
 * point, we will fail to restore a consistent database state.
 *
 * We also attempt to retrieve the corresponding backup history file.
 * If successful, set *minRecoveryLoc to constrain valid PITR stopping
 * points.
 *
 * Returns TRUE if a backup_label was found (and fills the checkpoint
 * location into *checkPointLoc); returns FALSE if not.
 */
static bool
read_backup_label(XLogRecPtr *checkPointLoc, XLogRecPtr *minRecoveryLoc)
{
	XLogRecPtr	startpoint;
	XLogRecPtr	stoppoint;
	char		histfilename[MAXFNAMELEN];
	char		histfilepath[MAXPGPATH];
	char		startxlogfilename[MAXFNAMELEN];
	char		stopxlogfilename[MAXFNAMELEN];
	TimeLineID	tli;
	uint32		_logId;
	uint32		_logSeg;
	FILE	   *lfp;
	FILE	   *fp;
	char		ch;

	/* Default is to not constrain recovery stop point */
	minRecoveryLoc->xlogid = 0;
	minRecoveryLoc->xrecoff = 0;

	/*
	 * See if label file is present
	 */
	lfp = AllocateFile(BACKUP_LABEL_FILE, "r");
	if (!lfp)
	{
		if (errno != ENOENT)
			ereport(FATAL,
					(errcode_for_file_access(),
					 errmsg("could not read file \"%s\": %m",
							BACKUP_LABEL_FILE)));
		return false;			/* it's not there, all is fine */
	}

	/*
	 * Read and parse the START WAL LOCATION and CHECKPOINT lines (this code
	 * is pretty crude, but we are not expecting any variability in the file
	 * format).
	 */
	if (fscanf(lfp, "START WAL LOCATION: %X/%X (file %08X%16s)%c",
			   &startpoint.xlogid, &startpoint.xrecoff, &tli,
			   startxlogfilename, &ch) != 5 || ch != '\n')
		ereport(FATAL,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("invalid data in file \"%s\"", BACKUP_LABEL_FILE)));
	if (fscanf(lfp, "CHECKPOINT LOCATION: %X/%X%c",
			   &checkPointLoc->xlogid, &checkPointLoc->xrecoff,
			   &ch) != 3 || ch != '\n')
		ereport(FATAL,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("invalid data in file \"%s\"", BACKUP_LABEL_FILE)));
	if (ferror(lfp) || FreeFile(lfp))
		ereport(FATAL,
				(errcode_for_file_access(),
				 errmsg("could not read file \"%s\": %m",
						BACKUP_LABEL_FILE)));

	/*
	 * Try to retrieve the backup history file (no error if we can't)
	 */
	XLByteToSeg(startpoint, _logId, _logSeg);
	BackupHistoryFileName(histfilename, tli, _logId, _logSeg,
						  startpoint.xrecoff % XLogSegSize);

	if (InArchiveRecovery)
		RestoreArchivedFile(histfilepath, histfilename, "RECOVERYHISTORY", 0);
	else
		BackupHistoryFilePath(histfilepath, tli, _logId, _logSeg,
							  startpoint.xrecoff % XLogSegSize);

	fp = AllocateFile(histfilepath, "r");
	if (fp)
	{
		/*
		 * Parse history file to identify stop point.
		 */
		if (fscanf(fp, "START WAL LOCATION: %X/%X (file %24s)%c",
				   &startpoint.xlogid, &startpoint.xrecoff, startxlogfilename,
				   &ch) != 4 || ch != '\n')
			ereport(FATAL,
					(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
					 errmsg("invalid data in file \"%s\"", histfilename)));
		if (fscanf(fp, "STOP WAL LOCATION: %X/%X (file %24s)%c",
				   &stoppoint.xlogid, &stoppoint.xrecoff, stopxlogfilename,
				   &ch) != 4 || ch != '\n')
			ereport(FATAL,
					(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
					 errmsg("invalid data in file \"%s\"", histfilename)));
		*minRecoveryLoc = stoppoint;
		if (ferror(fp) || FreeFile(fp))
			ereport(FATAL,
					(errcode_for_file_access(),
					 errmsg("could not read file \"%s\": %m",
							histfilepath)));
	}

	return true;
}

/*
 * Error context callback for errors occurring during rm_redo().
 */
static void
rm_redo_error_callback(void *arg)
{
	RedoErrorCallBack *redoErrorCallBack = (RedoErrorCallBack*) arg;
	StringInfoData buf;

	initStringInfo(&buf);
	RmgrTable[redoErrorCallBack->record->xl_rmid].rm_desc(
												   &buf,
												   redoErrorCallBack->location,
												   redoErrorCallBack->record);

	/* don't bother emitting empty description */
	if (buf.len > 0)
		errcontext("xlog redo %s", buf.data);

	pfree(buf.data);
}

static char *
XLogLocationToBuffer(char *buffer, XLogRecPtr *loc, bool longFormat)
{

	if (longFormat)
	{
		uint32 seg = loc->xrecoff / XLogSegSize;
		uint32 offset = loc->xrecoff % XLogSegSize;
		sprintf(buffer,
			    "%X/%X (==> seg %d, offset 0x%X)",
			    loc->xlogid, loc->xrecoff,
			    seg, offset);
	}
	else
		sprintf(buffer,
			    "%X/%X",
			    loc->xlogid, loc->xrecoff);

	return buffer;
}

static char xlogLocationBuffer[50];
static char xlogLocationBuffer2[50];
static char xlogLocationBuffer3[50];
static char xlogLocationBuffer4[50];
static char xlogLocationBuffer5[50];

char *
XLogLocationToString(XLogRecPtr *loc)
{
	return XLogLocationToBuffer(xlogLocationBuffer, loc, Debug_print_qd_mirroring);
}

char *
XLogLocationToString2(XLogRecPtr *loc)
{
	return XLogLocationToBuffer(xlogLocationBuffer2, loc, Debug_print_qd_mirroring);
}

char *
XLogLocationToString3(XLogRecPtr *loc)
{
	return XLogLocationToBuffer(xlogLocationBuffer3, loc, Debug_print_qd_mirroring);
}

char *
XLogLocationToString4(XLogRecPtr *loc)
{
	return XLogLocationToBuffer(xlogLocationBuffer4, loc, Debug_print_qd_mirroring);
}

char *
XLogLocationToString5(XLogRecPtr *loc)
{
	return XLogLocationToBuffer(xlogLocationBuffer5, loc, Debug_print_qd_mirroring);
}

char *
XLogLocationToString_Long(XLogRecPtr *loc)
{
	return XLogLocationToBuffer(xlogLocationBuffer, loc, true);
}

char *
XLogLocationToString2_Long(XLogRecPtr *loc)
{
	return XLogLocationToBuffer(xlogLocationBuffer2, loc, true);
}

char *
XLogLocationToString3_Long(XLogRecPtr *loc)
{
	return XLogLocationToBuffer(xlogLocationBuffer3, loc, true);
}

char *
XLogLocationToString4_Long(XLogRecPtr *loc)
{
	return XLogLocationToBuffer(xlogLocationBuffer4, loc, true);
}

char *
XLogLocationToString5_Long(XLogRecPtr *loc)
{
	return XLogLocationToBuffer(xlogLocationBuffer5, loc, true);
}


void xlog_print_redo_read_buffer_not_found(
	Relation 		reln,
	BlockNumber 	blkno,
	XLogRecPtr 		lsn,
	const char 		*funcName)
{
	if (funcName != NULL)
		elog(PersistentRecovery_DebugPrintLevel(),
			 "%s redo for %u/%u/%u did not find buffer for block %d (LSN %s)",
			 funcName,
			 reln->rd_node.spcNode,
			 reln->rd_node.dbNode,
			 reln->rd_node.relNode,
			 blkno,
			 XLogLocationToString(&lsn));
	else
		elog(PersistentRecovery_DebugPrintLevel(),
			 "Redo for %u/%u/%u did not find buffer for block %d (LSN %s)",
			 reln->rd_node.spcNode,
			 reln->rd_node.dbNode,
			 reln->rd_node.relNode,
			 blkno,
			 XLogLocationToString(&lsn));
}

void xlog_print_redo_lsn_application(
	Relation 		reln,
	BlockNumber 	blkno,
	void			*pagePtr,
	XLogRecPtr 		lsn,
	const char 		*funcName)
{
	Page page = (Page)pagePtr;
	XLogRecPtr	pageCurrentLsn = PageGetLSN(page);
	bool willApplyChange;

	willApplyChange = XLByteLT(pageCurrentLsn, lsn);

	if (funcName != NULL)
		elog(PersistentRecovery_DebugPrintLevel(),
			 "%s redo application for %u/%u/%u, block %d, willApplyChange = %s, current LSN %s, change LSN %s",
			 funcName,
			 reln->rd_node.spcNode,
			 reln->rd_node.dbNode,
			 reln->rd_node.relNode,
			 blkno,
			 (willApplyChange ? "true" : "false"),
			 XLogLocationToString(&pageCurrentLsn),
			 XLogLocationToString2(&lsn));
	else
		elog(PersistentRecovery_DebugPrintLevel(),
			 "Redo application for %u/%u/%u, block %d, willApplyChange = %s, current LSN %s, change LSN %s",
			 reln->rd_node.spcNode,
			 reln->rd_node.dbNode,
			 reln->rd_node.relNode,
			 blkno,
			 (willApplyChange ? "true" : "false"),
			 XLogLocationToString(&pageCurrentLsn),
			 XLogLocationToString2(&lsn));
}

/* ------------------------------------------------------
 *  Startup Process main entry point and signal handlers
 * ------------------------------------------------------
 */

/*
 * startupproc_quickdie() occurs when signalled SIGQUIT by the postmaster.
 *
 * Some backend has bought the farm,
 * so we need to stop what we're doing and exit.
 */
static void
startupproc_quickdie(SIGNAL_ARGS __attribute__((unused)))
{
	PG_SETMASK(&BlockSig);

	/*
	 * We DO NOT want to run proc_exit() callbacks -- we're here because
	 * shared memory may be corrupted, so we don't want to try to clean up our
	 * transaction.  Just nail the windows shut and get out of town.  Now that
	 * there's an atexit callback to prevent third-party code from breaking
	 * things by calling exit() directly, we have to reset the callbacks
	 * explicitly to make this work as intended.
	 */
	on_exit_reset();

	/*
	 * Note we do exit(2) not exit(0).	This is to force the postmaster into a
	 * system reset cycle if some idiot DBA sends a manual SIGQUIT to a random
	 * backend.  This is necessary precisely because we don't clean up our
	 * shared memory state.  (The "dead man switch" mechanism in pmsignal.c
	 * should ensure the postmaster sees this as a crash, too, but no harm in
	 * being doubly sure.)
	 */
	exit(2);
}


/* SIGHUP: set flag to re-read config file at next convenient time */
static void
StartupProcSigHupHandler(SIGNAL_ARGS __attribute__((unused)))
{
	got_SIGHUP = true;
}

/* SIGTERM: set flag to abort redo and exit */
static void
StartupProcShutdownHandler(SIGNAL_ARGS __attribute__((unused)))
{
	if (in_restore_command)
		proc_exit(1);
	else
		shutdown_requested = true;
	proc_exit(1);
}

static void
HandleCrash(SIGNAL_ARGS)
{
    /**
     * Handle crash is registered as a signal handler for SIGILL/SIGBUS/SIGSEGV
     *
     * This simply calls the standard handler which will log the signal and reraise the
     *      signal if needed
     */
    StandardHandlerForSigillSigsegvSigbus_OnMainThread("a startup process", PASS_SIGNAL_ARGS);
}

extern bool FindMyDatabase(const char *name, Oid *db_id, Oid *db_tablespace);

static char *knownDatabase = "template1";

/* Main entry point for startup process */
void
StartupProcessMain(int passNum)
{
	char	   *fullpath;

	/*
	 * If possible, make this process a group leader, so that the postmaster
	 * can signal any child processes too.
	 */
#ifdef HAVE_SETSID
	if (setsid() < 0)
		elog(FATAL, "setsid() failed: %m");
#endif

	/*
	 * Properly accept or ignore signals the postmaster might send us
	 */
	pqsignal(SIGHUP, StartupProcSigHupHandler);	 /* reload config file */
	pqsignal(SIGINT, SIG_IGN);					/* ignore query cancel */
	pqsignal(SIGTERM, StartupProcShutdownHandler); /* request shutdown */
	pqsignal(SIGQUIT, startupproc_quickdie);		/* hard crash time */
	pqsignal(SIGALRM, SIG_IGN);
	pqsignal(SIGPIPE, SIG_IGN);
	pqsignal(SIGUSR1, SIG_IGN);
	pqsignal(SIGUSR2, SIG_IGN);

#ifdef SIGBUS
	pqsignal(SIGBUS, HandleCrash);
#endif
#ifdef SIGILL
    pqsignal(SIGILL, HandleCrash);
#endif
#ifdef SIGSEGV
	pqsignal(SIGSEGV, HandleCrash);
#endif

	/*
	 * Reset some signals that are accepted by postmaster but not here
	 */
	pqsignal(SIGCHLD, SIG_DFL);
	pqsignal(SIGTTIN, SIG_DFL);
	pqsignal(SIGTTOU, SIG_DFL);
	pqsignal(SIGCONT, SIG_DFL);
	pqsignal(SIGWINCH, SIG_DFL);

	/*
	 * Unblock signals (they were blocked when the postmaster forked us)
	 */
	PG_SETMASK(&UnBlockSig);

	switch (passNum)
	{
	case 1:
		StartupXLOG();
		break;

	case 2:
	case 4:
		/*
		 * NOTE: The following initialization logic was borrrowed from ftsprobe.
		 */
		SetProcessingMode(InitProcessing);

		/*
		 * Create a resource owner to keep track of our resources (currently only
		 * buffer pins).
		 */
		if (passNum == 2)
		{
			CurrentResourceOwner = ResourceOwnerCreate(NULL, "Startup Pass 2");
		}
		else
		{
			Assert(passNum == 4);
			CurrentResourceOwner = ResourceOwnerCreate(NULL, "Startup Pass 4");
		}

		/*
		 * NOTE: AuxiliaryProcessMain has already called:
		 * NOTE:      BaseInit,
		 * NOTE:      InitAuxiliaryProcess instead of InitProcess, and
		 * NOTE:      InitBufferPoolBackend.
		 */

		InitXLOGAccess();

		SetProcessingMode(NormalProcessing);

		/*
		 * In order to access the catalog, we need a database, and a
		 * tablespace; our access to the heap is going to be slightly
		 * limited, so we'll just use some defaults.
		 */
		MyDatabaseId = TemplateDbOid;
		MyDatabaseTableSpace = DEFAULTTABLESPACE_OID;

		if (!FindMyDatabase(knownDatabase, &MyDatabaseId, &MyDatabaseTableSpace))
			ereport(FATAL, (errcode(ERRCODE_UNDEFINED_DATABASE),
				errmsg("database 'postgres' does not exist")));

		fullpath = GetDatabasePath(MyDatabaseId, MyDatabaseTableSpace);

		SetDatabasePath(fullpath);

		/*
		 * Finish filling in the PGPROC struct, and add it to the ProcArray. (We
		 * need to know MyDatabaseId before we can do this, since it's entered
		 * into the PGPROC struct.)
		 *
		 * Once I have done this, I am visible to other backends!
		 */
		InitProcessPhase2();

		/*
		 * Initialize my entry in the shared-invalidation manager's array of
		 * per-backend data.
		 *
		 * Sets up MyBackendId, a unique backend identifier.
		 */
		MyBackendId = InvalidBackendId;

		/*
		 * Though this is a startup process and currently no one sends invalidation
		 * messages concurrently, we set sendOnly = false, since we have relcaches.
		 */
		SharedInvalBackendInit(false);

		if (MyBackendId > MaxBackends || MyBackendId <= 0)
			elog(FATAL, "bad backend id: %d", MyBackendId);

		/*
		 * bufmgr needs another initialization call too
		 */
		InitBufferPoolBackend();

		/* heap access requires the rel-cache */
		RelationCacheInitialize();
		InitCatalogCache();

		/*
		 * It's now possible to do real access to the system catalogs.
		 *
		 * Load relcache entries for the system catalogs.  This must create at
		 * least the minimum set of "nailed-in" cache entries.
		 */
		RelationCacheInitializePhase2();

		if (passNum == 2)
		{
			StartupXLOG_Pass2();
		}
		else
		{
			Assert(passNum == 4);
			StartupXLOG_Pass4();
		}

//		ResourceOwnerRelease(CurrentResourceOwner,
//							 RESOURCE_RELEASE_BEFORE_LOCKS,
//							 false, true);
		break;

	case 3:
		/*
		 * Pass 3 does REDO work for all non-meta-data (i.e. not the gp_persistent_* tables).
		 */
		SetProcessingMode(InitProcessing);

		/*
		 * Create a resource owner to keep track of our resources (currently only
		 * buffer pins).
		 */
		CurrentResourceOwner = ResourceOwnerCreate(NULL, "Startup Pass 3");

		/*
		 * NOTE: AuxiliaryProcessMain has already called:
		 * NOTE:      BaseInit,
		 * NOTE:      InitAuxiliaryProcess instead of InitProcess, and
		 * NOTE:      InitBufferPoolBackend.
		 */

		InitXLOGAccess();

		SetProcessingMode(NormalProcessing);

		StartupXLOG_Pass3();

		break;

	default:
		elog(PANIC, "Unexpected pass number %d", passNum);
	}

	/*
	 * Exit normally. Exit code 0 tells postmaster that we completed
	 * recovery successfully.
	 */
	proc_exit(0);
}

/*
 * The following two gucs
 *					a) fsync=on
 *					b) wal_sync_method=open_sync
 * open XLOG files with O_DIRECT flag.
 * O_DIRECT flag requires XLOG buffer to be 512 byte aligned.
 */
void
XLogInitMirroredAlignedBuffer(int32 bufferLen)
{
	if (bufferLen > writeBufLen)
	{
		if (writeBuf != NULL)
		{
			pfree(writeBuf);
			writeBuf = NULL;
			writeBufAligned = NULL;
			writeBufLen = 0;
		}
	}

	if (writeBuf == NULL)
	{
		writeBufLen = bufferLen;

		writeBuf = MemoryContextAlloc(TopMemoryContext, writeBufLen + ALIGNOF_XLOG_BUFFER);
		if (writeBuf == NULL)
			ereport(ERROR,
					(errcode(ERRCODE_OUT_OF_MEMORY),
					 (errmsg("could not allocate memory for mirrored aligned buffer"))));
		writeBufAligned = (char *) TYPEALIGN(ALIGNOF_XLOG_BUFFER, writeBuf);
	}
}
