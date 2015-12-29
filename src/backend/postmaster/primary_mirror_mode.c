/**
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
 *
 * Manages transitions between primary/mirror modes.
 *
 */
#include "postgres.h"
#include "postmaster/postmaster.h"
#include "postmaster/primary_mirror_mode.h"
#include "storage/pmsignal.h"
#include "storage/shmem.h"
#include "storage/spin.h"
#include "miscadmin.h"
#include "access/xlog.h"
#include "utils/guc.h"
#include "utils/netcheck.h"
#include "catalog/pg_filespace.h"
#include "cdb/cdbpersistentfilespace.h"
#include "cdb/cdbvars.h"
#include <string.h>
#include "access/slru.h"
#include "access/xlog_internal.h"

#include <unistd.h>
#include <sys/stat.h>

#define BUFFER_LEN (2 * (MAXPGPATH))
/**
 * todo MIRRORING Review setup of signal handlers here --
 *                if we are inside a transition will we quit out properly on shutdown?
 */

/**
 * The transition state is used when the postmaster needs to return back to the server loop.
 *   The primary/mirror mode function will mark its transition as being in-progress...
 *   later calls to doRequestedPrimaryMirrorModeTransition will check this field.
 *
 * Note: if the order or # of values is changed then update the labels below.
 *
 * this implements a state machine.  Each transition (like to-primary-segment, or to-mirror-segment)
 *             manages its own transition order through this machine.
 *
 * Note that some places (notably, getFileRepRoleAndState) rely on the numerical order of these
 *   for implementation.  Be careful of that when changing this.
 */
typedef enum
{
        TSNone = 0,

        /* for two-phase recovery (where filerep goes into changelogging before database startup and
         *   needs to be notified after the database is up) */

        TSDoDatabaseStartup,
        TSDoneDatabaseStartup,

        TSDone,

        /* note: these should be in sync with getTransitionStateLabel */

        /* the number of elements in the enumeration */
        TS__EnumerationCount
} TransitionState;

/* State management */
static void assertModuleInitialized(void);

/**
 * Shared globals for this module live in a single instance of PMModuleState.
 */
typedef struct PMModuleState
{
	/** all accesses to the isTransitionRequested and requestedTransition must lock on this */
	slock_t    lock;

    /* TRANSITION INFORMATION */

    /**
     * Has a transition been requested (and not yet fulfilled completed by the postmaster)?
     */
	volatile bool isTransitionRequested;

	/**
	 * The requested transition.
	 */
	volatile PrimaryMirrorModeTransitionArguments requestedTransition;

	/**
	 * The result of the transition; only valid when isTransitionRequested is false (postmaster done) and
	 *    isTransitionListenerFinished is also false (backend has not yet received this transitionResult
	 */
	volatile PrimaryMirrorModeTransitionResult transitionResult;

	/**
	 * The extra information about the transition result; used to report more info
	 *   back to the caller (such as to differentiate a mirroring failure from other failure cases)
	 */
	volatile char transitionResultExtraInfo[MAX_TRANSITION_RESULT_EXTRA_INFO];

	/**
	 * For transitions that require the postmaster to do multiple steps of work, this will hold the current
	 *    progress point.  Only valid while isTransitionRequested is true
	 */
	volatile TransitionState transitionState;

	/**
	  * Has the postmaster copied the currently requested transition to its local memory yet?
	  */
	volatile bool transitionCopiedToPMLocalMem;

    /* FILE REPLICATION PARAMETERS */
	volatile PrimaryMirrorMode mode;
	volatile SegmentState_e segmentState;
	volatile DataState_e dataState;
	volatile FaultType_e faultType;

	/* connection info for self (which can be a mirror or primary, depending on mode) */
	volatile char hostAddress[PM_MAX_HOST_NAME_LENGTH+1];

	/* file replication port on self */
	volatile int hostPort;

	/* connection info for the peer (which can be a mirror or primary, depending on mode) */
	volatile char peerAddress[PM_MAX_HOST_NAME_LENGTH+1];

	/* file replication port on self */
	volatile int peerPort;

    /* OTHER FIELDS */
	volatile bool haveStartedDatabase;

	/** used by resync processes, this field will be kept up-to-date by the postmaster */
	volatile pid_t bgWriterPID;

    /**
     * a counter used to assign values to transitions; that values are for logging.
     */
	volatile int transitionNumberCounter;

	/**
	 *  if true, means we are in fault after postmaster reset and waiting for a transition to be sent
	 */
    volatile bool isInFaultFromPostmasterReset;

	/* Filespace details for temp files */
	slock_t			tempFilespaceLock;
	volatile int	numOfFilespacePath;
	volatile int	numOfValidFilespacePath;
	volatile int	lastAllocateIdx;
	volatile char **tempFilespacePath;
	volatile bool  *tempFilespacePathHasError;
	volatile bool	isUsingDefaultFilespaceForTempFiles;

	/* Filespace details for transaction files */
	volatile char txnFilespacePath[MAXPGPATH];
	volatile char peerTxnFilespacePath[MAXPGPATH];
	volatile Oid txnFilespaceOID;
	volatile bool isUsingDefaultFilespaceForTxnFiles;

} PMModuleState;

static volatile PMModuleState *pmModuleState = NULL;

/**
 * Local globals live in a single PMLocalState instance
 */
typedef struct PMLocalState
{
    /**
     * The most recent transition applied.  This variable is only valid
     *   in the postmaster process; it is used to save around transitions
     *   for later postmaster reset.
     *
     * Note also that the dataState value in this transition MAY CHANGE because of an internal transition
     *    to resync!
     */
    PrimaryMirrorModeTransitionArguments mostRecentTransition;

    /**
     * See mostRecentTransition
     */
    bool haveSetMostRecentTransition;

    /**
     * Used if the reset of the filerep peer cannot be done to indicate that the postmaster reset should
     *   transition to fault rather than trying a peer postmaster reset
     */
    bool doNextResetToFault;

    /**
     * Was the most recent (non-fault) segment state "change tracking disabled"?
     */
    bool mostRecentSegmentStateWasChangeTrackingDisabled;

    /**
     * only valid when between calls to
     *   acquireModuleSpinLockAndMaybeStartCriticalSection and releaseModuleSpinLockAndEndCriticalSection
     * tells the value that was passed to acquireModuleSpinLockAndMaybeStartCriticalSection
     */
    bool criticalSectionStarted;
} PMLocalState;

PMLocalState pmLocalState;
bool gHaveInitializedLocalState = false;

static int isAbsolutePath(const char *path);

static bool	TemporaryDirectoryIsOk(const char *path);



/**
 * Extra result information that can be returned to the calling script so it
 *       can choose to take a different action besides just standard, generic handling
 */
#define RESULT_INFO_MIRRORING_FAILURE "MirroringFailure"
#define RESULT_INFO_POSTMASTER_DIED "PostmasterDied"
#define RESULT_INFO_IN_SHUTDOWN "ServerIsInShutdown"
#define RESULT_INFO_INVALID_STATE_TRANSITION "InvalidStateTransition"

/*
 * getNumOfTempFilespacePaths
 *	This function will fill up the share memory structure and 
 *	If computeNumberOnly is true, this function will check the file format
 *	correctness and return the number of temporary filespace paths only.
 */
static int
getNumOfTempFilespacePaths(bool computeNumberOnly)
{
	char	buffer[BUFFER_LEN];
	FILE	*fp = NULL;
	int		numOfTempFilespacePaths = 0;

	/* Read the temp directories details from gp_temporary_files_directories flat file */
	fp = fopen(TEMPFILES_DIRECTORIES_FLATFILE, "r");
	if (fp)
	{
		while (true)
		{
			MemSet(buffer, 0, BUFFER_LEN);
			if (fgets(buffer, BUFFER_LEN, fp) == NULL)
				break;

			buffer[strlen(buffer) - 1] = '\0';
			ereport(LOG, (errmsg("get temporary directory: %s", buffer)));

			numOfTempFilespacePaths++;
			if (computeNumberOnly)
				continue;

			/* Populate the temp paths */
			strcpy((char *) pmModuleState->tempFilespacePath[numOfTempFilespacePaths - 1], buffer);

			/* If we have a valid path name, we mark that we are not using default filespace. */
			if (pmModuleState->tempFilespacePath[numOfTempFilespacePaths - 1][0])
				pmModuleState->isUsingDefaultFilespaceForTempFiles = 0;
		}

		fclose(fp);
	}
	else
	{
		/* Don't make palloc sad. */
		numOfTempFilespacePaths = 1;

		if (!computeNumberOnly)
		{
			ereport(LOG, (errmsg("temporary files using default location")));
		}
	}

	return numOfTempFilespacePaths;
}

/**
 * LABELS and parsing of values
 */

/**
 * Get the label for a primary mirror mode.  Returns a pointer to static memory.
 *
 * Note that the label is externally visible (through a getStatus call to the postmaster, for example)
 *
 * note also that some python code (as used by gpstate) may use these label values, so when changing them
 *  be sure to search the management scripts code.
 */
const char *
getMirrorModeLabel(PrimaryMirrorMode mode)
{
	static const char *labels[] =
		{
			"Uninitialized", "Master", "Standby", "MirrorlessSegment"
		};

	COMPILE_ASSERT(ARRAY_SIZE(labels) == PMMode__EnumerationCount);

	return labels[mode];
}

/**
 * Get the label for a data state.  Returns a pointer to static memory.
 *
 * Note that the label is externally visible (through a getStatus call to the postmaster, for example)
 *
 * note also that some python code (as used by gpstate) may use these label values, so when changing them
 *  be sure to search the management scripts code.
 */
const char *
getDataStateLabel(DataState_e state)
{
	static const char *labels[] =
		{
			"NotInitialized", "InSync", "InChangeTracking"
		};
	COMPILE_ASSERT(ARRAY_SIZE(labels) == DataState__EnumerationCount);

	return labels[state];
}

/**
 * Note that the label is externally visible (through a getStatus call to the postmaster, for example)
 *
 * note also that some python code (as used by gpstate) may use these label values, so when changing them
 *  be sure to search the management scripts code.
 */
const char *
getSegmentStateLabel(SegmentState_e state)
{
	static const char *labels[] =
		{
			"NotInitialized", "Initialization", "InChangeTrackingTransition", "InResyncTransition",
			"InSyncTransition", "Ready", "ChangeTrackingDisabled", "Fault",
			"ShutdownBackends", "Shutdown", "ImmediateShutdown"
		};
	COMPILE_ASSERT(ARRAY_SIZE(labels) == SegmentState__EnumerationCount);

	return labels[state];
}

/**
 * Note that the label is externally visible (through a getStatus call to the postmaster, for example)
 *
 * note also that some python code (as used by gpstate) may use these label values, so when changing them
 *  be sure to search the management scripts code.
 */
const char *
getFaultTypeLabel(FaultType_e faultType)
{
	static const char *labels[] =
	{
		"NotInitialized",
		"FaultIO",
		"FaultDB",
		"FaultNet",
	};
	COMPILE_ASSERT(ARRAY_SIZE(labels) == FaultType__EnumerationCount);

	return labels[faultType];
}

/**
 * Get the label for a transition result.  Returns a pointer to static memory.
 *
 * Note that scripts use these values in strings that may be parsed, so please keep
 *      them as alphabetic values only
 */
const char *
getTransitionResultLabel(PrimaryMirrorModeTransitionResult res)
{
	static const char *labels[] =
		{
			"Success", "InvalidTargetMode", "OtherTransitionInProgress",
			"InvalidInputParameter", "Error", "OkayButStillInProgress"
		};
	COMPILE_ASSERT(ARRAY_SIZE(labels) == PMTransition__EnumerationCount);

	return labels[res];
}


/**
 * Translate PrimaryMirrorMode to character compatible with 'role', as stored in gp_segment_configuration
 */
char getRole(PrimaryMirrorMode mode)
{
	Assert(mode >= PMModeUninitialized && mode < PMMode__EnumerationCount);

	switch (mode)
	{
		case PMModeMaster:
		case PMModeMirrorlessSegment:
			return 'p';

		case PMModeUninitialized:
			return '-';

		default:
			Assert(!"Invalid role");
			return '\0';
	}
}


/**
 * Translate DataState_e to character compatible with 'mode', as stored in gp_segment_configuration
 */
char getMode(DataState_e state)
{
	Assert(state >= DataStateNotInitialized && state < DataState__EnumerationCount);

	switch (state)
	{
		case DataStateInSync:
			return 's';

		case DataStateNotInitialized:
			return '-';

		default:
			Assert(!"Invalid mode");
			return '\0';
	}
}


/**
 * Convert the given string name for a primary mirror mode to a mode.  Note
 *    that any unparseable arg value returns PMModeUninitialized.
 *
 * @param arg should be non-NULL
 */
PrimaryMirrorMode
decipherPrimaryMirrorModeArgument(const char *arg)
{
	if (strcmp("master", arg) == 0)
		return PMModeMaster;
	else if (strcmp("segment", arg) == 0 || strcmp("mirrorless", arg) == 0 )
		return PMModeMirrorlessSegment;
	else if (strcmp("standby", arg) == 0)
		return PMModeStandby;
	else
		return PMModeUninitialized;
}

/**
 * acquire the module spin lock that is used to protect the contents of pmModuleState,
 *   and perhaps start a critical section (START_CRIT_SECTION), according to the parameter startCriticalSection
 */
static void
acquireModuleSpinLockAndMaybeStartCriticalSection(bool startCriticalSection)
{
	Assert(!pmLocalState.criticalSectionStarted);
	if (startCriticalSection)
	{
		pmLocalState.criticalSectionStarted = true;
		START_CRIT_SECTION();
	}
	SpinLockAcquire(&pmModuleState->lock);
}

/**
 * release the module spin lock that is used to protect the contents of pmModuleState
 */
static void
releaseModuleSpinLockAndEndCriticalSectionAsNeeded(void)
{
	SpinLockRelease(&pmModuleState->lock);
	if (pmLocalState.criticalSectionStarted)
	{
		pmLocalState.criticalSectionStarted = false;
		END_CRIT_SECTION();
	}
}

static void
clearTransitionArgs(volatile PrimaryMirrorModeTransitionArguments *args)
{
	args->mode = PMModeUninitialized;
	args->dataState = DataStateNotInitialized;
	args->hostAddress[0] = '\0';
	args->hostPort = 0;
	args->peerAddress[0] = '\0';
	args->peerPort = 0;
	args->transitionToFault = false;
	args->transitionToChangeTrackingDisabledMode = false;
}

PrimaryMirrorModeTransitionArguments *
createNewTransitionArguments(void)
{
	PrimaryMirrorModeTransitionArguments *result = palloc(sizeof(PrimaryMirrorModeTransitionArguments));
	clearTransitionArgs(result);
	return result;
}

static void
assertModuleInitialized(void)
{
	Assert(pmModuleState != NULL && "init must be called first");
}

void
getPrimaryMirrorStatusCodes(PrimaryMirrorMode *pm_mode,
							SegmentState_e *s_state,
							DataState_e *d_state,
							FaultType_e *f_type)
{
	acquireModuleSpinLockAndMaybeStartCriticalSection(false);
	{
        if (pm_mode)
            *pm_mode = pmModuleState->mode;

        if (s_state)
            *s_state = pmModuleState->segmentState;

        if (d_state)
            *d_state = pmModuleState->dataState;

		if (f_type)
			*f_type = pmModuleState->faultType;
	}
	releaseModuleSpinLockAndEndCriticalSectionAsNeeded();
}


void
getPrimaryMirrorModeStatus(char *bufOut, int bufSize)
{
	PrimaryMirrorMode pm_mode;
	SegmentState_e s_state;
	DataState_e d_state;
	FaultType_e f_type;

	assertModuleInitialized();

	getPrimaryMirrorStatusCodes(&pm_mode, &s_state, &d_state, &f_type);

	snprintf(bufOut, bufSize, "mode: %s\n""segmentState: %s\n""dataState: %s\n""faultType: %s",
			 getMirrorModeLabel(pm_mode), getSegmentStateLabel(s_state), getDataStateLabel(d_state), getFaultTypeLabel(f_type));
}

/**
 * While holding the module spin lock, return whether or not we are in a valid transition.
 *
 * This is more than just checking pmModulestate->isTransitionRequested because that requested transition
 *      may be illegal
 */
static bool
isInValidTransition_UnderLock(void)
{
    return
		pmModuleState->isTransitionRequested && pmModuleState->transitionState >= TSDoDatabaseStartup;
}

/**
 *
 * Fetch the current file and target replication role and states.  Returns values through the pointer arguments.
 *
 * Note that any of the arguments may be NULL in which case that piece of information is not returned.
 *
 * @param fileRepRoleOut may be NULL.  If non-null, *fileRepRoleOut is filled in with the current filerep role
 * @param segmentStateOut may be NULL.  If non-null, *segmentStateOut is filled in with the current segment state
 * @param dataStateOut  may be NULL.  If non-null, *dataStateOut is filled in with the current segment state
 * @param isInFilerepTransitionOut  may be NULL.  If non-null, *isInTransitionOut is filled in with true or false depending on
 *                           if the segment is in filerep transition to primary or mirror mode
 * @param transitionTargetDataStateOut  may be NULL.  If non-null, *transitionTargetDataStateOut is filled
 *                           in with the target data state, or DataStateNotInitialized if there is
 *                           no transition to primary or mirror mode is in progress.
 */
void
getFileRepRoleAndState(
	FileRepRole_e	*fileRepRoleOut,
	SegmentState_e	*segmentStateOut,
	DataState_e		*dataStateOut,

	bool *isInFilerepTransitionOut,
	DataState_e *transitionTargetDataStateOut
	)
{
	assertModuleInitialized();

	FileRepRole_e fileRepRole;
	SegmentState_e	segmentState;
	DataState_e		dataState;
	bool isInFilerepTransition;
	DataState_e transitionTargetDataState;

	acquireModuleSpinLockAndMaybeStartCriticalSection(false);
	{
		/* check to see if postmaster is still processing a transition.  Note that here we don't
		 *  need to check to see if the listener is complete
		 **/
		isInFilerepTransition = isInValidTransition_UnderLock();

		fileRepRole = FileRepNoRoleConfigured;
		dataState = DataStateNotInitialized;
		segmentState = SegmentStateNotInitialized;
		transitionTargetDataState = DataStateNotInitialized;
	}
	releaseModuleSpinLockAndEndCriticalSectionAsNeeded();

	/* now fill in output! */
	if (fileRepRoleOut)
		*fileRepRoleOut = fileRepRole;
	if (segmentStateOut)
		*segmentStateOut = segmentState;
	if (dataStateOut)
		*dataStateOut = dataState;

	if (isInFilerepTransitionOut)
		*isInFilerepTransitionOut = isInFilerepTransition;
	if (transitionTargetDataStateOut)
		*transitionTargetDataStateOut = transitionTargetDataState;
}

/**
 * NOTE: in some cases the caller should also call isInFaultFromPostmasterReset() to check to
 *       make sure the database will be started/should be started.  When in fault from postmaster reset,
 *       database processes should not be started.
 */
bool
isPrimaryMirrorModeAFullPostmaster(bool checkTargetModeAsWell)
{
	bool result;

	assertModuleInitialized();

	/* the locking here may not be technically necessary */
	acquireModuleSpinLockAndMaybeStartCriticalSection(false);
	result = pmModuleState->mode == PMModeStandby ||
			 pmModuleState->mode == PMModeMaster  ||
			 pmModuleState->mode == PMModeMirrorlessSegment;
	releaseModuleSpinLockAndEndCriticalSectionAsNeeded();
	return result;
}

bool
isInFaultFromPostmasterReset()
{
    bool result;

    assertModuleInitialized();
	/* the locking here may not be technically necessary */
	acquireModuleSpinLockAndMaybeStartCriticalSection(false);
	{
		result = pmModuleState->isInFaultFromPostmasterReset;
	}
	releaseModuleSpinLockAndEndCriticalSectionAsNeeded();
	return result;
}

Size
primaryMirrorModeShmemSize(void)
{
	Size		size;
	int			numOfPaths = getNumOfTempFilespacePaths(true);

	size = sizeof(PMModuleState);
	/* temp filespace path */
	size += sizeof(char *) * numOfPaths;
	size += sizeof(char) * MAXPGPATH * numOfPaths;
	size += sizeof(bool) * numOfPaths;
	return size;
}

void
primaryMirrorModeShmemInit(void)
{
	int	i;

	if (!gHaveInitializedLocalState)
	{
		/* only initialize local state once */
		pmLocalState.haveSetMostRecentTransition = false;
		pmLocalState.mostRecentSegmentStateWasChangeTrackingDisabled = false;
		pmLocalState.criticalSectionStarted = false;

		gHaveInitializedLocalState = true;
	}

	pmModuleState = (PMModuleState *) ShmemAlloc(sizeof(PMModuleState));
	pmModuleState->isTransitionRequested = false;
	pmModuleState->transitionCopiedToPMLocalMem = false;
	pmModuleState->mode = PMModeUninitialized;
	pmModuleState->segmentState = SegmentStateNotInitialized;
	pmModuleState->dataState = DataStateNotInitialized;
	pmModuleState->faultType = FaultTypeNotInitialized;
	pmModuleState->transitionState = TSNone;
	pmModuleState->transitionResult = PMTransitionSuccess;

	pmModuleState->hostAddress[0] = '\0';
	pmModuleState->hostPort = 0;
	pmModuleState->peerAddress[0] = '\0';
	pmModuleState->peerPort = 0;

	pmModuleState->transitionResultExtraInfo[0] = '\0';

	pmModuleState->haveStartedDatabase = false;
	pmModuleState->bgWriterPID = 0;
	pmModuleState->transitionNumberCounter = 1;
	pmModuleState->isInFaultFromPostmasterReset = false;

	/* temp filespace path */
	pmModuleState->numOfFilespacePath = getNumOfTempFilespacePaths(true);
	pmModuleState->numOfValidFilespacePath = pmModuleState->numOfFilespacePath;
	Assert(pmModuleState->numOfFilespacePath > 0);
	pmModuleState->tempFilespacePath = (volatile char **) ShmemAlloc(sizeof(char *) * pmModuleState->numOfFilespacePath);
	pmModuleState->tempFilespacePathHasError = (volatile bool *) ShmemAlloc(sizeof(bool) * pmModuleState->numOfFilespacePath);
	for (i = 0; i < pmModuleState->numOfFilespacePath; i++)
	{
		pmModuleState->tempFilespacePath[i] = (char *) ShmemAlloc(sizeof(char) * MAXPGPATH);
		pmModuleState->tempFilespacePath[i][0] = '\0';
		pmModuleState->tempFilespacePathHasError[i] = false;
	}
	/* Will be changed in primaryMirrorPopulateFilespaceInfo(). */
    pmModuleState->isUsingDefaultFilespaceForTempFiles = 1;

    primaryMirrorPopulateFilespaceInfo();

    pmModuleState->txnFilespacePath[0] = '\0';
    pmModuleState->peerTxnFilespacePath[0] = '\0';
    pmModuleState->txnFilespaceOID = SYSTEMFILESPACE_OID;
    pmModuleState->isUsingDefaultFilespaceForTxnFiles = 1;

	clearTransitionArgs(&pmModuleState->requestedTransition);

	SpinLockInit(&pmModuleState->lock);
	SpinLockInit(&pmModuleState->tempFilespaceLock);
}

/**
 * Reset all necessary parameters EXCEPT for the requested transition itself.  Caller must do that.
 *
 * Will be used for setting initial mode (on startup) and handling postmaster reset
 *
 * The only requestedTransition field reset is the transitionNumber
 */
static void
resetModuleStateForInitialTransition(void)
{
	pmModuleState->isTransitionRequested = true;
	pmModuleState->transitionCopiedToPMLocalMem = false;
	pmModuleState->transitionState = TSNone;
    pmModuleState->requestedTransition.transitionNumber = pmModuleState->transitionNumberCounter++;
}

void
setInitialRequestedPrimaryMirrorMode(PrimaryMirrorMode targetMode)
{
	Assert(pmModuleState->mode == PMModeUninitialized);

	pmModuleState->requestedTransition.mode = targetMode;
	pmModuleState->mode = targetMode;
    resetModuleStateForInitialTransition();
}

/**
 * Mark that after postmaster reset (as processed by primaryMirrorHandlePostmasterReset)
 * segment should transition to mirroring fault state
 */
void
setFaultAfterReset(bool val)
{
	pmLocalState.doNextResetToFault = val;
}


/**
 * Update shared memory to transition segment to mirroring fault
 */
void
setTransitionToFault()
{
	/* mark mirroring fault if segment is not marked to transition to change-tracking */
	Assert(pmModuleState->requestedTransition.dataState != DataStateInChangeTracking);

	pmModuleState->requestedTransition.transitionToFault = true;
}


/**
 * Get flag from shared memory indicating transition to mirroring fault
 */
bool
getTransitionToFault()
{
	return pmModuleState->requestedTransition.transitionToFault;
}

/**
 * Check if one of the NICs where the primary and mirror are attached is not active
 */
bool
primaryMirrorCheckNICFailure()
{
	char localHost[sizeof(pmModuleState->hostAddress)];
	char peerHost[sizeof(pmModuleState->peerAddress)];

	acquireModuleSpinLockAndMaybeStartCriticalSection(true);
	{
		/* get local and peer host names */
		strncpy(localHost, (const char *) pmModuleState->hostAddress, sizeof(localHost));
		strncpy(peerHost, (const char *) pmModuleState->peerAddress, sizeof(peerHost));
	}
	releaseModuleSpinLockAndEndCriticalSectionAsNeeded();

	/*
	 * if primary and mirror are uninitialized or co-located,
	 * there is no NIC redundancy
	 */
	if (0 == strcmp(localHost, peerHost))
	{
		return false;
	}

	int running = 0;
	if (NetCheckNIC(localHost))
	{
		running++;
	}
	if (NetCheckNIC(peerHost))
	{
		running++;
	}

	Assert(running > 0);

	/*
	 * report only a single NIC failure;
	 * double failures indicate errors in NIC configuration retrieval
	 */
	return (running == 1);
}


/**
 * Called on postmaster reset, after all children have been shut down but before starting the children back up.
 *   Returns true if the postmaster should try to reset the peer before doing a reset.
 */
bool
primaryMirrorPostmasterResetShouldRestartPeer(void)
{
	Assert(pmLocalState.haveSetMostRecentTransition);

	/* if we're a primary AND we were in resync/sync then try to restart in that mode */
	return false;
}

/**
 * Handle a postmaster reset.  This is called AFTER shared memory has been reset.
 *
 * This function will use the locally stored data from the most recently requested transition
 *    in order to transition.
 */
void
primaryMirrorHandlePostmasterReset(void)
{
	bool transitionToFault = false;

	Assert(pmModuleState->mode == PMModeUninitialized);
	Assert(pmLocalState.haveSetMostRecentTransition);

	elog(LOG,
		 "PrimaryMirrorMode: Processing postmaster reset with recent mode of %d",
		 pmLocalState.mostRecentTransition.mode);

	transitionToFault = false;

	pmLocalState.doNextResetToFault = false;

	elog(LOG,
		 "PrimaryMirrorMode: Processing postmaster reset to %s state",
		 transitionToFault ? "fault" : "non-fault");

	pmModuleState->requestedTransition = pmLocalState.mostRecentTransition;
	pmModuleState->requestedTransition.transitionToFault = transitionToFault;

	/* come after assigning requestedTransition, so we get the transition number set */
	resetModuleStateForInitialTransition();
}

/**
 * To be called by the postmaster.  The answer may become true immediately
 *  after this call but shouldn't (because postmaster is the only one who resets it)
 *  become false
 *
 * Does not acquire the lock as we are only peeking.
 */
bool
isPrimaryMirrorModeTransitionRequested(void)
{
	return pmModuleState->isTransitionRequested;
}


/*
 * Copy most recent transition request from shared memory to local memory
 */
void copyTransitionParametersToLocalMemory()
{
	/* this is called before resetting shared memory so no locking is required */
	pmLocalState.mostRecentTransition = pmModuleState->requestedTransition;
	pmLocalState.haveSetMostRecentTransition = true;
	pmModuleState->transitionCopiedToPMLocalMem = true;
}

void primaryMirrorRecordSegmentStateToPostmasterLocalMemory(void)
{
	Assert(!IsUnderPostmaster);

	SegmentState_e segmentState = pmModuleState->segmentState;
	switch (segmentState)
	{
		case SegmentStateInitialization:
		case SegmentStateInResyncTransition:
		case SegmentStateInSyncTransition:
		case SegmentStateReady:
			pmLocalState.mostRecentSegmentStateWasChangeTrackingDisabled = false;
			break;

		case SegmentStateChangeTrackingDisabled:
			pmLocalState.mostRecentSegmentStateWasChangeTrackingDisabled = true;
			break;

		case SegmentStateNotInitialized:
		case SegmentStateInChangeTrackingTransition:
		case SegmentStateFault:
		case SegmentStateShutdownFilerepBackends:
		case SegmentStateShutdown:
		case SegmentStateImmediateShutdown:
			/* does not change mostRecentSegmentStateWasChangeTrackingDisabled */
			break;

		case SegmentState__EnumerationCount:
			Assert(0);
			break;
	}
}

/**
 * Get the arguments that were most recently copied to local memory.  This is ONLY valid
 *  in the postmaster and in children that are forked most recently after the transition.
 */
PrimaryMirrorModeTransitionArguments primaryMirrorGetArgumentsFromLocalMemory(void)
{
	if (!pmLocalState.haveSetMostRecentTransition)
	{
		elog(ERROR, "Request for filerep arguments from local memory made when they do not exist.");
	}
	return pmLocalState.mostRecentTransition;
}

Oid
primaryMirrorGetTxnFilespaceOID(void)
{
        assertModuleInitialized();
        return pmModuleState->txnFilespaceOID;
}

char*
primaryMirrorGetTxnFilespacePath(void)
{
        assertModuleInitialized();
        return (char*)pmModuleState->txnFilespacePath;
}

char *
primaryMirrorGetTempFilespacePath(void)
{
	return DatabasePath;
#if 0
	int	i;
	int	lastAllocateIdx;
	int	numOfDirectories;

	assertModuleInitialized();
	/* Don't increase the allocate counter. */
	if (gp_force_use_default_temporary_directory)
		return DatabasePath;

	if (pmModuleState->numOfValidFilespacePath == 0)
		return DatabasePath;

	/*
	 * Writer will choose temporary and store it in shared snapshot. Use it if
	 * we have it.
	 */
	if (SharedLocalSnapshotSlot &&
		SharedLocalSnapshotSlot->session_temporary_directory)
		return SharedLocalSnapshotSlot->session_temporary_directory;

	/* Prevent the others. */
	SpinLockAcquire(&pmModuleState->tempFilespaceLock);

	lastAllocateIdx = pmModuleState->lastAllocateIdx;
	numOfDirectories = pmModuleState->numOfFilespacePath;
	for (i = 1; i <= numOfDirectories; i++)
	{
		int	idx = (lastAllocateIdx + i) % numOfDirectories;

		if (pmModuleState->tempFilespacePathHasError[idx])
			continue;

		/*
		 * Allocate an error temporary will cause segment startup failed, we
		 * check it here to reduce the race condition.
		 */
		if (!TemporaryDirectoryIsOk((const char *) pmModuleState->tempFilespacePath[idx]))
		{
			pmModuleState->tempFilespacePathHasError[idx] = true;
			pmModuleState->numOfValidFilespacePath--;
			continue;
		}

		break;
	}

	if (i <= numOfDirectories)
	{
		pmModuleState->lastAllocateIdx = (lastAllocateIdx + i) % numOfDirectories;
                lastAllocateIdx = pmModuleState->lastAllocateIdx; 
		SpinLockRelease(&pmModuleState->tempFilespaceLock);

		elog(DEBUG1, "Temp directory id %d path: \"%s\" was chosen",
					lastAllocateIdx,
					(char *) pmModuleState->tempFilespacePath[lastAllocateIdx]);
		return (char *) pmModuleState->tempFilespacePath[lastAllocateIdx];
	}

	/* Use the default temporary directory. */
	pmModuleState->numOfValidFilespacePath = 0;
	SpinLockRelease(&pmModuleState->tempFilespaceLock);

	return DatabasePath;
#endif
}

bool
primaryMirrorIsUsingDefaultFilespaceForTempFiles(void)
{
	bool ret = pmModuleState->isUsingDefaultFilespaceForTempFiles;

	/* It is safe to access pmModuleState->numOfValidFilespacePath without lock here. */
	assertModuleInitialized();

	if (ret)
		elog(DEBUG1, "Temp directory default: \"%s\" was chosen", DatabasePath);

	return ret;
}

bool
primaryMirrorIsUsingDefaultFilespaceForTxnFiles(void)
{
	assertModuleInitialized();
	return pmModuleState->isUsingDefaultFilespaceForTxnFiles;
}

/* The caller needs to free the memory of the return value */
char*
makeRelativeToTxnFilespace(char *path)
{
        char *fullPath = NULL;

        assertModuleInitialized();
        fullPath = (char*)palloc(MAXPGPATH);

        if (pmModuleState->isUsingDefaultFilespaceForTxnFiles)
	{
                if (snprintf(fullPath, MAXPGPATH, "%s", path) > MAXPGPATH)
		{
			ereport(ERROR, (errmsg("cannot form path %s\n", path)));
		}
	}
        else
	{
                if (snprintf(fullPath, MAXPGPATH, "%s/%s", primaryMirrorGetTxnFilespacePath(), path) > MAXPGPATH)
		{
			ereport(ERROR, (errmsg("cannot form path %s/%s", primaryMirrorGetTxnFilespacePath(), path)));
		}
	}

        return fullPath;
}

char*
primaryMirrorGetPeerTxnFilespacePath(void)
{
        assertModuleInitialized();

        return (char *)pmModuleState->peerTxnFilespacePath;
}

char*
makeRelativeToPeerTxnFilespace(char *path)
{
        char *fullPath = NULL;

        assertModuleInitialized();
        fullPath = (char*)palloc(MAXPGPATH);

        if (pmModuleState->isUsingDefaultFilespaceForTxnFiles)
                sprintf(fullPath, "%s", path);
        else
                sprintf(fullPath, "%s/%s", primaryMirrorGetPeerTxnFilespacePath(), path);
        return fullPath;
}

void
primaryMirrorPopulateFilespaceInfo(void)
{
	int	i;

	/* Read the temp directories details from gp_temporary_files_directories flat file */
	getNumOfTempFilespacePaths(false);
	for (i = 0; i < pmModuleState->numOfFilespacePath; i++)
		TemporaryDirectorySanityCheck((char *) pmModuleState->tempFilespacePath[i], 0, false);

	ereport(LOG, (errmsg("transaction files using default pg_system filespace")));
}

/*
 * Return true if the directory is one of those that can be configured on
 * a different filespace
 */
bool
isTxnDir(char *path)
{
	/* Return true only if the directory is relative to default directory */
	if (isAbsolutePath(path))
		return false;

	if (strstr(path, XLOGDIR))
		return true;

	if (strstr(path, SUBTRANS_DIR))
		return true;

	if (strstr(path, CLOG_DIR))
		return true;

	if (strstr(path, DISTRIBUTEDLOG_DIR))
		return true;

	if (strstr(path, DISTRIBUTEDXIDMAP_DIR))
		return true;

	if (strstr(path, MULTIXACT_MEMBERS_DIR))
		return true;

	if (strstr(path, MULTIXACT_OFFSETS_DIR))
		return true;

	return false;
}

int
isAbsolutePath(const char *path)
{
        if (path[0] == '/')
                return 1;
        return 0;
}

bool
isFilespaceUsedForTxnFiles(Oid fsoid)
{
	assertModuleInitialized();
	if ((!pmModuleState->isUsingDefaultFilespaceForTxnFiles) && (fsoid == pmModuleState->txnFilespaceOID))
		return true;

	return false;
}

/*
 * Reset (unlock) all Primary Mirror state spin locks.
 */
void primaryMirrorModeResetSpinLocks(void)
{
	if (pmModuleState != NULL)
	{
		SpinLockInit(&pmModuleState->lock);
		SpinLockInit(&pmModuleState->tempFilespaceLock);
	}
}

/* 
 * TemporaryDirectoryFaultInjection
 */
void
TemporaryDirectoryFaultInjection(int temp_dir_idx)
{
	int	abs_dir_idx = abs(temp_dir_idx);

	/* System will set gucs earlier than pmModule initialization. */
	if (!pmModuleState)
		return;

	if (pmModuleState->isUsingDefaultFilespaceForTempFiles)
		return;

	/* Display config and reset session. */
	if (temp_dir_idx == 0)
	{
		elog(NOTICE, "current default temporary directory: \"%s\"", getCurrentTempFilePath);
		TemporaryDirectorySanityCheck(getCurrentTempFilePath, 0, true);
		return;
	}

	/* switch the temporary directory error state. */
	if (abs_dir_idx >= 1 &&
		abs_dir_idx <= pmModuleState->numOfFilespacePath)
	{
		char	*state = NULL;

		SpinLockAcquire(&pmModuleState->tempFilespaceLock);
		if (temp_dir_idx > 0)
		{
			pmModuleState->tempFilespacePathHasError[abs_dir_idx - 1] = false;
			pmModuleState->numOfValidFilespacePath++;
			state = "no error";
		}
		else
		{
			pmModuleState->tempFilespacePathHasError[abs_dir_idx - 1] = true;
			pmModuleState->numOfValidFilespacePath--;
			state = "error";
		}
		SpinLockRelease(&pmModuleState->tempFilespaceLock);

		elog(NOTICE, "temp dir id: %d path: %s marked %s",
					temp_dir_idx,
					pmModuleState->tempFilespacePath[abs_dir_idx - 1],
					state);
		return;
	}

	elog(WARNING, "temporary directory index range is between [%d, %d]",
				1,
				pmModuleState->numOfFilespacePath);
}

/*
 * TemporaryDirectorySanityCheck
 */
void
TemporaryDirectorySanityCheck(const char *path, int err, bool process_die)
{
	int i;
	int	error_level = process_die ? FATAL : WARNING;

	if (pmModuleState->isUsingDefaultFilespaceForTempFiles)
		return;

	for (i = 0; i < pmModuleState->numOfFilespacePath; i++)
	{
		/* match the prefix. */
		if (strncmp(path, (char *) pmModuleState->tempFilespacePath[i], strlen((char *) pmModuleState->tempFilespacePath[i])) == 0)
		{
			if (TemporaryDirectoryIsOk((char *) pmModuleState->tempFilespacePath[i]))
				break;

			SpinLockAcquire(&pmModuleState->tempFilespaceLock);
			pmModuleState->tempFilespacePathHasError[i] = true;
			pmModuleState->numOfValidFilespacePath--;
			SpinLockRelease(&pmModuleState->tempFilespaceLock);

			/*
			 * Some paths may ask us to check path sanity, in this case, the
			 * err is 0. Use errno if err == 0.
			 */
			elog(error_level, "temporary directory id: %d path \"%s\" has error(errno: %d)",
					i, path, err == 0 ? errno : err);
			break;
		}
	}

	/* default path will not be marked. */
}

/*
 * TemporaryDirectoryTest
 */
static bool
TemporaryDirectoryIsOk(const char *path)
{
	struct stat st;

	/* check file exists */
	if (stat(path, &st) < 0)
	{
		elog(DEBUG1, "temporary direcotry does not exists for: \"%s\"", path);
		return false;
	}

	/* check is directory */
	if (!S_ISDIR(st.st_mode))
	{
		elog(DEBUG1, "temporary direcotry is not a valid directory for: \"%s\"", path);
		return false;
	}

	/* check permission */
	if (access(path, W_OK|X_OK) < 0)
	{
		elog(DEBUG1, "temporary direcotry permission denied for: \"%s\"", path);
		return false;
	}

	return true;
}

