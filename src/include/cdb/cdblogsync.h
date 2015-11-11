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
 * cdblogsync.h
 *
 *
 *-------------------------------------------------------------------------
 */
#ifndef CDBLOGSYNC_H
#define CDBLOGSYNC_H

#include "catalog/pg_control.h"

/* sync commands */
typedef enum
{
	SYNC_POSITION_TO_END,			/* position to WAL end */
	SYNC_XLOG,						/* xlog sync */
	SYNC_NEW_CHECKPOINT_LOC,		/* new checkpoint location */
	SYNC_SHUTDOWN_TOO_FAR_BEHIND,	/* shutdown -- too far behind to synchronize */
	SYNC_CLOSE						/* close */
}	syncCommand;

extern void cdb_init_log_sync(void);
extern bool cdb_sync_command(const char *);
extern void cdb_shutdown_too_far_behind(void);
extern void cdb_perform_redo(XLogRecPtr *redoCheckPointLoc, CheckPoint *redoCheckPoint, XLogRecPtr *newCheckpointLoc);

#endif   /* CDBLOGSYNC_H */
