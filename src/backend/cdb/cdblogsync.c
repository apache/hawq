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
 *	cdbtmtest.c
 *-------------------------------------------------------------------------
 */
#include "postgres.h"
#include <stdio.h>
#include <fcntl.h>
#include <unistd.h>
#include <signal.h>
#include <sys/stat.h>
#define PROVIDE_64BIT_CRC


#include "utils/elog.h"
#include "utils/guc.h"
#include "access/xlog_internal.h"
#include "miscadmin.h"
#include "libpq/libpq.h"
#include "libpq/pqformat.h"
#include "storage/ipc.h"

#include "tcop/dest.h"

#include "cdb/cdblogsync.h"
#include "catalog/pg_control.h"
#include "postmaster/walredoserver.h"
#include "storage/fd.h"

/*
 * There is a protocol consisting of 3 messages from primary->mirror:
 *
 * Q position_to_end
 * Q xlog logid seg woffset wlen
 * Q close
 */

/* for xlog */
static char xlogfilename[100];
static int	xlogfilefd = -1;
static uint32 xlogid = -1;
static uint32 xseg = -1;
static int	xlogfileoffset = -1;

/* param sent by primary segdb */
static int	cmdtype;
static uint32 wlogid;
static uint32 wseg;
static int	woffset;
static int	wlen;

static uint32 logidNewCheckpoint;
static uint32 segNewCheckpoint;
static int offsetNewCheckpoint;

/* XXX: file global buffer, called "buf" ??? At least it is allocated and free()ed as needed. */
static void *buf = NULL;
static int	buflen;

static void cdb_sync_xlog(void);
static void parseCmd(const char *);
static void readLogMessage(void *, int);
static void syncWriteLog(int, void *, int, int);
static void ensureBufferSize(void);
static void write_with_ereport(int fd, void *data, int len);
static void cdb_position_to_end(void);
static void cdb_new_checkpoint_loc(void);
static void cdb_close(void);


static void
write_with_ereport(int fd, void *data, int len)
{
	int			write_len;

	write_len = write(fd, data, len);

	if (write_len != len)
	{
		close(fd);
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("error writing file: %m")));
	}
}


/*
 * initialize log sync - create recovery.conf file ready for recovery
 */
void
cdb_init_log_sync(void)
{
	/* create recovery.conf file for recovery */
	int			rconffd = -1;
	char		path[MAXPGPATH];

	/* the first time or file is changed */
	snprintf(path, MAXPGPATH, "%s/recovery.conf", DataDir);
	if ((rconffd = open(path, O_RDWR, 0)) < 0)
	{
		char	   *cmd = "restore_command=''\n";

		elog((Debug_print_qd_mirroring ? LOG : DEBUG5), "QDSYNC: creating recovery.conf file");
		rconffd = open(path, O_RDWR | O_CREAT | O_EXCL, S_IRUSR | S_IWUSR);
		if (rconffd < 0)
		{
			ereport(ERROR,
					(errcode_for_file_access(),
				 errmsg("QDSYNC: could not create recovery.conf file \"%s\"",
						path)));
		}

		write_with_ereport(rconffd, cmd, strlen(cmd));
	}

	close(rconffd);
}

/*
 * cdb_sync_command - process sync command
 */
bool
cdb_sync_command(const char *cmd)
{
	elog((Debug_print_qd_mirroring ? LOG : DEBUG5), "QDSYNC: %s", cmd);
	parseCmd(cmd);

	switch (cmdtype)
	{
		case SYNC_XLOG:
			cdb_sync_xlog();
			break;
		case SYNC_POSITION_TO_END:
			cdb_position_to_end();
			break;
		case SYNC_NEW_CHECKPOINT_LOC:
			cdb_new_checkpoint_loc();
			break;
		case SYNC_SHUTDOWN_TOO_FAR_BEHIND:
			return true;
		case SYNC_CLOSE:
			cdb_close();
			elog(LOG,"QDSYNC: master is closing...");
			break;
	}

	return false;
}

/*
 * open xlog file
 */
static void
openXlogEnd(XLogRecPtr *endLocation)
{
	char		path[MAXPGPATH];
	uint32		logid;
	uint32		seg;
	char		*xlogDir = NULL;

	XLByteToSeg(*endLocation, logid, seg);
	XLogFileName(xlogfilename, ThisTimeLineID, logid, seg);
	elog((Debug_print_qd_mirroring ? LOG : DEBUG5), "QDSYNC: opening logid %d seg %d file %s",
		 logid, seg, xlogfilename);

	xlogDir = makeRelativeToTxnFilespace(XLOGDIR);
	/* the first time or file is changed */
	if (snprintf(path, MAXPGPATH, "%s/%s", xlogDir, xlogfilename) >= MAXPGPATH)
	{
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("QDSYNC: could not create xlog file \"%s/%s\"",
						xlogDir, xlogfilename)));
	}

	if (xlogfilefd >= 0)
	{
		close(xlogfilefd);
		xlogfilefd = -1;
		xlogfileoffset = -1;
	}

	xlogfilefd = open(path, O_RDWR, 0);
	if (xlogfilefd < 0)
	{
		elog((Debug_print_qd_mirroring ? LOG : DEBUG5), "QDSYNC: creating xlog file %s", xlogfilename);
		xlogfilefd = open(path, O_RDWR | O_CREAT | O_EXCL, S_IRUSR | S_IWUSR);
		if (xlogfilefd < 0)
		{
			ereport(ERROR,
					(errcode_for_file_access(),
					 errmsg("QDSYNC: could not create xlog file \"%s\"",
							path)));
		}
	}

	xlogid = logid;
	xseg = seg;
	xlogfileoffset = endLocation->xrecoff % XLogSegSize;

	elog((Debug_print_qd_mirroring ? LOG : DEBUG5),"QDSYNC: opened '%s' offset 0x%X",
		 xlogfilename, xlogfileoffset);

	pfree(xlogDir);
}

static void
putEndLocationReply(XLogRecPtr *endLocation)
{
	StringInfoData buf;
	
	pq_beginmessage(&buf, 's');
	pq_sendint(&buf, endLocation->xlogid, 4);
	pq_sendint(&buf, endLocation->xrecoff, 4);
	pq_endmessage(&buf);
	pq_flush();
}

static XLogRecPtr syncRedoLoc;

static void
cdb_position_to_end(void)
{
	XLogRecPtr redoCheckpointLoc;
	CheckPoint redoCheckpoint;
	XLogRecPtr endLocation;

	// Throw in extra new line to make log more readable.
	elog(LOG,"--------------------------");
	
	XLogGetRecoveryStart("QDSYNC", "to get initial restart location", &redoCheckpointLoc, &redoCheckpoint);
	syncRedoLoc = redoCheckpoint.redo;

	// UNDONE: Minimum of redoCheckpointLoc and redoCheckpoint.redo?
	
	XLogScanForStandbyEndLocation(&syncRedoLoc, &endLocation);

	ereport(LOG,
	 (errmsg("QDSYNC: reporting recovery start location %s and scanned end location %s",
		 XLogLocationToString(&syncRedoLoc),
		 XLogLocationToString2(&endLocation))));

	// Throw in extra new line to make log more readable.
	elog(LOG,"--------------------------");
	
	/*
	 * Open up end location segment and set offset to end.
	 */
	openXlogEnd(&endLocation);

	/*
	 * Extra reply information that gives our standby master XLOG end location
	 * to the primary.
	 */
	putEndLocationReply(&endLocation);
}

static void
cdb_new_checkpoint_loc(void)
{
	XLogRecPtr newCheckpointLoc;
	bool successful;

	newCheckpointLoc.xlogid = logidNewCheckpoint;
	newCheckpointLoc.xrecoff = segNewCheckpoint * XLogSegSize 
		                     + offsetNewCheckpoint;
	if (newCheckpointLoc.xlogid == 0 && newCheckpointLoc.xrecoff == 0)
	{
		elog(ERROR, "QDSYNC: found invalid new checkpoint location");
	}

	successful = WalRedoServerNewCheckpointLocation(&newCheckpointLoc);
	if (!successful)
	{
		elog(ERROR, "QDSYNC: redo error occurred");
	}
}

void
cdb_shutdown_too_far_behind(void)
{
	if (kill(PostmasterPid, SIGINT) < 0)
		ereport(ERROR,
		 (errmsg(
		     "kill(%ld,%d) failed: %m",
			 (long) PostmasterPid, SIGINT)));
	ereport(LOG,
	 (errmsg(
	     "QDSYNC: standby is too far behind the master to be synchronized -- "
		 "requested the standby to do a fast shutdown by signaling the postmaster (pid %d) with SIGINT",
		 (int)PostmasterPid)));
}

void
cdb_perform_redo(XLogRecPtr *redoCheckPointLoc, CheckPoint *redoCheckPoint, XLogRecPtr *newCheckpointLoc)
{
	CheckPoint oldRedoCheckpoint;
	uint32 logid;
	uint32 seg;
	int nsegsremoved;
	
	if (redoCheckPointLoc->xlogid == 0 && redoCheckPointLoc->xrecoff == 0)
	{
		XLogGetRecoveryStart("QDSYNC", "for redo apply", redoCheckPointLoc, redoCheckPoint);
	}
	
	XLogStandbyRecoverRange(redoCheckPointLoc, redoCheckPoint, newCheckpointLoc);

	/*
	 * Sample the recovery start location now to see if appling redo
	 * processed checkpoint records and moved the restart location forward.
	 */
	oldRedoCheckpoint = *redoCheckPoint;

	XLogGetRecoveryStart("QDSYNC", "for redo progress check", redoCheckPointLoc, redoCheckPoint);

	if (XLByteLT(oldRedoCheckpoint.redo,redoCheckPoint->redo))
	{
		ereport(LOG,
		 (errmsg("QDSYNC: transaction redo moved the restart location from %s to %s",
			     XLogLocationToString(&oldRedoCheckpoint.redo),
			     XLogLocationToString2(&redoCheckPoint->redo))));
	}
	else
	{
		Assert(XLByteEQ(oldRedoCheckpoint.redo,redoCheckPoint->redo));
		ereport(LOG,
		 (errmsg("QDSYNC: transaction redo did not move the restart location %s forward this pass",
			     XLogLocationToString(&oldRedoCheckpoint.redo))));
		return;
	}

	XLByteToSeg(redoCheckPoint->redo, logid, seg);
	
	/*
	 * Delete offline log files (those no longer needed even for previous
	 * checkpoint).
	 */
	elog((Debug_print_qd_mirroring ? LOG : DEBUG5),
	     "QDSYNC: keep log files as far back as (logid %d, seg %d)",
		 logid, seg);

	if (logid || seg)
	{
		PrevLogSeg(logid, seg);
		elog((Debug_print_qd_mirroring ? LOG : DEBUG5),
			 "QDSYNC: delete offline log files up to (logid %d, seg %d)",
			 logid, seg);
		
		XLogRemoveStandbyLogs(logid, seg, &nsegsremoved);

		if (nsegsremoved > 0)
		{
		// Throw in extra new line to make log more readable.
			ereport(LOG,
			 (errmsg("QDSYNC: %d logs removed through logid %d, seg %d\n",
				     nsegsremoved,
				     logid, seg)));
		}

	}
	// Throw in extra new line to make log more readable.
	elog(LOG,"--------------------------");
}

static void
cdb_close(void)
{
	WalRedoServerQuiesce();
}

/*
 * Most of this procedure is from XLogFileInit.
 */
static void
createZeroFilledNewFile(char *path)
{
	char		tmppath[MAXPGPATH];
	int         fd;
	char		zbuffer[XLOG_BLCKSZ];
	int         nbytes;
	char 		*xlogDir = NULL;
	
	/*
	 * Initialize an empty (all zeroes) segment.
	 */
	xlogDir = makeRelativeToTxnFilespace(XLOGDIR);
	if (snprintf(tmppath, MAXPGPATH, "%s/xlogtemp.%d", xlogDir, (int) getpid()) > MAXPGPATH)
	{
		ereport(ERROR, (errmsg("cannot generate dir path %s/xlogtemp.%d", xlogDir, (int) getpid())));
	}

	pfree(xlogDir);
	unlink(tmppath);

	/* do not use XLOG_SYNC_BIT here --- want to fsync only at end of fill */
	fd = open(tmppath, O_RDWR | O_CREAT | O_EXCL | PG_BINARY,
			  S_IRUSR | S_IWUSR);
	if (fd < 0)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not create file \"%s\": %m", tmppath)));

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
		if ((int) write(fd, zbuffer, sizeof(zbuffer)) != (int) sizeof(zbuffer))
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
	{
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not fsync file \"%s\": %m", tmppath)));
	}

	if (close(fd))
	{
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not close file \"%s\": %m", tmppath)));
	}

	if (rename(tmppath, path) < 0)
	{
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not rename file \"%s\" to \"%s\" (initialization of log file %u, segment %u): %m",
						tmppath, path, xlogid, xseg)));
	}
	
	/*
	 * Re-open with different open flags.
	 */
	xlogfilefd = open(path, O_RDWR, 0);
	if (xlogfilefd < 0)
	{
			ereport(ERROR,
					(errcode_for_file_access(),
					 errmsg("QDSYNC: could not create xlog file \"%s\"",
							path)));
	}
	
	elog((Debug_print_qd_mirroring ? LOG : DEBUG5), "QDSYNC: created zero-filled xlog file %s", xlogfilename);
}

/*
 * open xlog file
 */
static void
openXlogNextFile(void)
{
	char		path[MAXPGPATH];
	char		*xlogDir = NULL;

	XLogFileName(xlogfilename, ThisTimeLineID, xlogid, xseg);
	elog((Debug_print_qd_mirroring ? LOG : DEBUG5), "QDSYNC: opening next logid %d seg %d file %s",
		 xlogid, xseg, xlogfilename);

	xlogDir = makeRelativeToTxnFilespace(XLOGDIR);

	/* the first time or file is changed */
	if (snprintf(path, MAXPGPATH, "%s/%s", xlogDir, xlogfilename) >= MAXPGPATH)
	{
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("QDSYNC: could not create xlog file \"%s/%s\"",
						xlogDir, xlogfilename)));
	}

	pfree(xlogDir);

	xlogfilefd = open(path, O_RDWR, 0);
	if (xlogfilefd < 0)
	{
		createZeroFilledNewFile(path);
	}

	xlogfileoffset = 0;

	elog((Debug_print_qd_mirroring ? LOG : DEBUG5),
		 "QDSYNC: openXlogNextFile: opened '%s' offset 0x%X",
		 xlogfilename, xlogfileoffset);
}

/*
 * cdb_sync_xlog - process xlog sync
 */
static void
cdb_sync_xlog(void)
{
	uint32 currentBlockOffset;
	XLogRecPtr writeLoc;

	elog((Debug_print_qd_mirroring ? LOG : DEBUG5), "QDSYNC: write logid %d seg %d woffset 0x%X, wlen 0x%X",
		 wlogid, wseg, woffset, wlen);
	if (woffset % XLOG_BLCKSZ != 0)
	{
		elog(ERROR,"QDSYNC: not on block boundaries 0x%X",
			 woffset);
	}

	if (wlogid != xlogid || wseg != xseg)
	{
		elog((Debug_print_qd_mirroring ? LOG : DEBUG5), "QDSYNC: closing previous file %s",
		     xlogfilename);
		if (xlogfilefd >= 0)
		{
			close(xlogfilefd);
			xlogfilefd = -1;
			xlogfileoffset = -1;
		}
		xlogid = wlogid;
		xseg = wseg;
		openXlogNextFile();

		/*
		 * Assume caller knows where to write.
		 */
		xlogfileoffset = woffset;
	}
	
	/* 
	 * Validate we are appending or overwritting previous block
	 */
	currentBlockOffset = (xlogfileoffset / XLOG_BLCKSZ) * XLOG_BLCKSZ;
	if (woffset != currentBlockOffset && 
		woffset + XLOG_BLCKSZ != currentBlockOffset)
	{
		elog(ERROR,"QDSYNC: not appending to end (primary: 0x%X, standby: 0x%X)",
			   woffset, xlogfileoffset);
	}
	
	/*
	 * no validation checking on xlog. xlog sync is by block and may repeat
	 * the same block, so we do not have any way to check it now. we will rely
	 * on tmlog checking for now.
	 */
	ensureBufferSize();
	readLogMessage(buf, wlen);
	if ((wlen / XLOG_BLCKSZ) * XLOG_BLCKSZ != wlen)
	{	
		int roundedUp;
		int padLen;
		
		/*
		 * Pad buffer out with zeros.
		 */
		roundedUp = ((wlen + XLOG_BLCKSZ - 1) / XLOG_BLCKSZ) * XLOG_BLCKSZ;
		Assert(buflen >= roundedUp);
		padLen = roundedUp - wlen;
		
		elog((Debug_print_qd_mirroring ? LOG : DEBUG5), 
			 "QDSYNC: padding buffer with %d zeros (wlen %d, roundedUp %d)",
			 padLen, wlen, roundedUp);
		memset(&((char*) buf)[wlen], 0, padLen); 
		
		wlen = roundedUp;
	}

	writeLoc.xlogid = wlogid;
	writeLoc.xrecoff = wseg * XLogSegSize + 
					   woffset;
	syncWriteLog(xlogfilefd, buf, woffset, wlen);
	elog((Debug_print_qd_mirroring ? LOG : DEBUG5), 
		 "QDSYNC: wrote location %s len 0x%X", 
		 XLogLocationToString(&writeLoc),
		 wlen);

	xlogfileoffset = woffset + wlen;
}

/*
 * We parse incoming commands into a file-local static set of parameters.
 */
static void
getXlogCmdArgs(const char *cmdArgs)
{
	char	   *pos;

	wlogid = atoi(cmdArgs);

	pos = strchr(cmdArgs, ' ');
	if (pos == NULL)
		elog(ERROR, "QDSYNC: Cannot parse cmd args '%s' -- no seg", cmdArgs);

	pos++;
	wseg = atoi(pos);

	pos = strchr(pos, ' ');
	if (pos == NULL)
		elog(ERROR, "QDSYNC: Cannot parse cmd args '%s' -- no offset", cmdArgs);

	pos++;
	woffset = atoi(pos);

	pos = strchr(pos, ' ');
	if (pos == NULL)
		elog(ERROR, "QDSYNC: Cannot parse cmd args '%s' -- no len", cmdArgs);

	pos++;
	wlen = atoi(pos);
}

/*
 * We parse incoming commands into a file-local static set of parameters.
 */
static void
getNewCheckpointLocCmdArgs(const char *cmdArgs)
{
	char	   *pos;

	logidNewCheckpoint = atoi(cmdArgs);

	pos = strchr(cmdArgs, ' ');
	if (pos == NULL)
		elog(ERROR, "QDSYNC: cannot parse cmd args '%s' -- no seg", cmdArgs);

	pos++;
	segNewCheckpoint = atoi(pos);

	pos = strchr(pos, ' ');
	if (pos == NULL)
		elog(ERROR, "QDSYNC: cannot parse cmd args '%s' -- no offset", cmdArgs);

	pos++;
	offsetNewCheckpoint = atoi(pos);
}

/*
 * parse sync command
 */
static void
parseCmd(const char *cmd)
{
	if (strncmp(cmd, "xlog", 4) == 0)
	{
		/* xlog command */
		cmdtype = SYNC_XLOG;
		getXlogCmdArgs(cmd + strlen("xlog") + 1);
	}
	else if (strncmp(cmd, "position_to_end", 15) == 0)
	{
		cmdtype = SYNC_POSITION_TO_END;
	}
	else if (strncmp(cmd, "new_checkpoint_location", 23) == 0)
	{
		cmdtype = SYNC_NEW_CHECKPOINT_LOC;
		getNewCheckpointLocCmdArgs(cmd + strlen("new_checkpoint_location") + 1);
	}
	else if (strncmp(cmd, "shutdown_too_far_behind",23) == 0)
	{
		cmdtype = SYNC_SHUTDOWN_TOO_FAR_BEHIND;
	}
	else if (strncmp(cmd, "close", 5) == 0)
	{
		cmdtype = SYNC_CLOSE;
	}
	else
		ereport(FATAL,
				(errcode(ERRCODE_PROTOCOL_VIOLATION),
				 errmsg("queries cannot be executed against the standby master"),
				 errhint("Queries should be issued against the activate master. "
						 "If this host is not available, activate the standby so "
						 "that it can process queries.")));
}

/*
 * readLogMessage - read log message sent from client
 */
static void
readLogMessage(void *buf, int plen)
{
	int32		len;

	if (pq_getbytes((char *) &len, 4) == EOF)
	{
		ereport(ERROR,
				(errcode(ERRCODE_PROTOCOL_VIOLATION),
				 errmsg("QDSYNC: incomplete log packet")));
	}

	len = ntohl(len);
	len -= 4;
	pq_getbytes(buf, len);

	elog(DEBUG4, "QDSYNC: readLogMessage: plen %d len: %d", plen, len);
}


/*
 * syncWriteLog - sync log
 */
static void
syncWriteLog(int fd, void *buf, int offset, int len)
{
	int			loffset = lseek(fd, offset, SEEK_SET);

	if (loffset != offset)
	{
		elog(ERROR, "QDSYNC: error lseek location: %d, offset: %d, filename '%s', errno: %d", loffset, offset, xlogfilename, errno);
	}
	write_with_ereport(fd, buf, len);
	
	if (pg_fsync(fd) != 0)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("QDSYNC: could not fsync file '%s': %m", xlogfilename)));
}

/*
 * ensureBufferSize - increment buffer size to hold log message if necessary
 */
static void
ensureBufferSize(void)
{
	int roundedUp;

	roundedUp = ((wlen + XLOG_BLCKSZ - 1) / XLOG_BLCKSZ) * XLOG_BLCKSZ;
	
	if (buf != NULL && buflen < roundedUp)
	{
		/* buf is smaller, reallocate it */
		free(buf);
		buf = NULL;
	}

	if (buf == NULL)
	{
		buf = malloc(roundedUp);
		if (buf == NULL)
			elog(ERROR, "QDSYNC: malloc failed in sync tmlog, possiblly running out of memory");
		buflen = roundedUp;
	}
}

