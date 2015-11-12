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
 * cdbfts.h
 *
 *
 *-------------------------------------------------------------------------
 */
#ifndef CDBFTS_H
#define CDBFTS_H

#include "storage/lwlock.h"
#include "cdb/cdbconn.h"
#include "utils/guc.h"
#include "cdb/cdbgang.h"

typedef enum QDMIRRORState
{
	QDMIRROR_STATE_NONE = 0,
	QDMIRROR_STATE_NOTCONFIGURED,
	QDMIRROR_STATE_CONNECTINGWALSENDSERVER,
	QDMIRROR_STATE_POSITIONINGTOEND,
	QDMIRROR_STATE_CATCHUPPENDING,
	QDMIRROR_STATE_CATCHINGUP,
	QDMIRROR_STATE_SYNCHRONIZED,
	QDMIRROR_STATE_DISABLED,
	QDMIRROR_STATE_MAX /* must always be last */
} QDMIRRORState;

typedef enum QDMIRRORDisabledReason
{
	QDMIRROR_DISABLEDREASON_NONE = 0,
	QDMIRROR_DISABLEDREASON_CONNECTIONERROR,
	QDMIRROR_DISABLEDREASON_TOOFARBEHIND,
	QDMIRROR_DISABLEDREASON_WALSENDSERVERERROR,
	QDMIRROR_DISABLEDREASON_UNEXPECTEDERROR,
	QDMIRROR_DISABLEDREASON_SHUTDOWN,
	QDMIRROR_DISABLEDREASON_ADMINISTRATORDISABLED,
	QDMIRROR_DISABLEDREASON_MAX /* must always be last */
} QDMIRRORDisabledReason;


typedef enum QDMIRRORUpdateMask
{
	QDMIRROR_UPDATEMASK_NONE 				= 0,
	QDMIRROR_UPDATEMASK_VALIDFLAG 			= 0x1,
	QDMIRROR_UPDATEMASK_MASTERMIRRORING		= 0x2,
	QDMIRROR_UPDATEMASK_MAX /* must always be last */
} QDMIRRORUpdateMask;

#define QDMIRRORErrorMessageSize 200

struct FtsQDMirrorInfo
{
	bool			configurationChecked;
	QDMIRRORState 	state;

	QDMIRRORUpdateMask	updateMask;

	QDMIRRORDisabledReason	disabledReason;

	struct timeval lastLogTimeVal;

	char errorMessage[QDMIRRORErrorMessageSize];

	bool			haveNewCheckpointLocation;
	XLogRecPtr		newCheckpointLocation;

	/* stored copies of QD parameters, required so that WalSendServer
	 * process knows how to connect */
	char			name[256];
	uint16			port;

	/*
	 * Used to update or delete the standby row of gp_segment_config.
	 */
	int2			dbid;
	bool			valid;

	/*
	 * Indicates if an alert message has been sent for failure in master mirroring synchronization.
	 */
	bool QDMirroringNotSynchronizedWarningGiven;
};


struct FtsSegDBState
{
	bool	valid;
	bool	primary;
	int16	id;
};

#define FTS_MAX_DBS (128 * 1024)

#define FTS_STATUS_ALIVE				(1<<0)

#define FTS_STATUS_TEST(dbid, status, flag) (((status)[(dbid)] & (flag)) ? true : false)
#define FTS_STATUS_ISALIVE(dbid, status) FTS_STATUS_TEST((dbid), (status), FTS_STATUS_ALIVE)

typedef struct FtsProbeInfo
{
	volatile uint32		fts_probePid;
	volatile uint64		fts_probeScanRequested;
	volatile uint64		fts_statusVersion;
	volatile bool		fts_pauseProbes;
	volatile bool		fts_discardResults;
	volatile uint8		fts_status[FTS_MAX_DBS];
} FtsProbeInfo;

#define FTS_MAX_TRANSIENT_STATE 100

typedef struct FtsControlBlock
{
	bool		ftsEnabled;
	bool		ftsShutdownMaster;

	LWLockId	ControlLock;

	bool		ftsReadOnlyFlag;
	bool		ftsAdminRequestedRO;

	FtsProbeInfo fts_probe_info;

	LWLockId ftsQDMirrorLock;
	LWLockId ftsQDMirrorUpdateConfigLock;
	struct FtsQDMirrorInfo	ftsQDMirror;
}	FtsControlBlock;

extern volatile bool *ftsEnabled;

extern FtsProbeInfo *ftsProbeInfo;

#define isFTSEnabled() (ftsEnabled != NULL && *ftsEnabled)

#define disableFTS() (*ftsEnabled = false)

extern int	FtsShmemSize(void);
extern void FtsShmemInit(void);

extern void FtsHandleNetFailure(SegmentDatabaseDescriptor **, int);

extern bool verifyFtsSyncCount(void);
extern void FtsCondSetTxnReadOnly(bool *);
extern void ftsLock(void);
extern void ftsUnlock(void);

void FtsNotifyProber(void);

extern void CheckForQDMirroringWork(void);
extern void disableQDMirroring(char *detail, char *errorMessage, QDMIRRORDisabledReason disabledReason);
extern void disableQDMirroring_WalSendServerError(char *detail);
extern void disableQDMirroring_ConnectionError(char *detail, char *errorMessage);
extern void disableQDMirroring_TooFarBehind(char *detail);
extern void disableQDMirroring_UnexpectedError(char *detail);
extern void disableQDMirroring_ShutDown(void);
extern void disableQDMirroring_AdministratorDisabled(void);
extern void enableQDMirroring(char *message, char *detail);
extern char *QDMirroringStateString(void);
extern char *QDMirroringDisabledReasonString(void);

extern bool isFtsReadOnlySet(void);

/* markStandbyStatus forces persistent state change ?! */
#define markStandbyStatus(dbid, state) (markSegDBPersistentState((dbid), (state)))

extern void FtsGetQDMirrorInfo(char **hostname, uint16 *port);
void FtsDumpQDMirrorInfo(void);
extern bool isMirrorQDConfigurationChecked(void);
extern bool isQDMirroringPendingCatchup(void);
extern void FtsSetQDMirroring(struct Segment *master);
extern void FtsSetNoQDMirroring(void);
extern void FtsQDMirroringCatchup(XLogRecPtr *flushedLocation);
extern void FtsCheckReadyToSend(bool *ready, bool *haveNewCheckpointLocation, XLogRecPtr *newCheckpointLocation);
extern void FtsQDMirroringNewCheckpointLoc(XLogRecPtr *newCheckpointLocation);
extern char *QDMirroringStateToString(QDMIRRORState state);
extern bool QDMirroringWriteCheck(void);

#endif   /* CDBFTS_H */
