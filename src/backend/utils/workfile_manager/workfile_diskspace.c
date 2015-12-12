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
 * workfile_diskspace.c
 *	 Implementation of workfile manager disk space accounting. This is
 *	 just a wrapper for WorkfileQueryspace and WorkfileSegspace
 *
 *-------------------------------------------------------------------------
 */

#include <postgres.h>
#include "storage/shmem.h"
#include "utils/atomic.h"
#include "cdb/cdbvars.h"
#include "utils/workfile_mgr.h"
#include "miscadmin.h"

/*
 * This is set when a backend fails to write to a workfile,
 * presumably because of an out-of-diskspace error (logical or physical)
 */
static bool workfile_diskfull = false;

/*
 * Initialize shared memory area for the WorkfileDiskspace module
 */
void
WorkfileDiskspace_Init(void)
{
	WorkfileQueryspace_Init();
	WorkfileSegspace_Init();
	WorkfileDiskspace_SetFull(false /* isFull*/);
}

/*
 * Returns the amount of shared memory needed for the WorkfileDiskspace module
 */
Size
WorkfileDiskspace_ShMemSize(void)
{
	return add_size(WorkfileQueryspace_ShMemSize(), WorkfileSegspace_ShMemSize());
}

/*
 * Reserve 'bytes' bytes to write to disk
 *   This should be called before actually writing to disk
 *
 *   If enough disk space is available, increments the global counter and returns true
 *   Otherwise, returns false
 *
 *   When either the query limit or the segment limit are hit, the
 *   workfile_diskfull flag is set to true by the respective subsystem to
 *   prevent further writes.
 */
bool
WorkfileDiskspace_Reserve(int64 bytes_to_reserve)
{
	if (bytes_to_reserve == 0)
	{
		return true;
	}

	bool queryspace_reserved = true;
	bool segspace_reserved = true;

	/* Try the per-query limit first, then the per-segment */
	if (gp_workfile_limit_per_query > 0)
	{
		queryspace_reserved =  WorkfileQueryspace_Reserve(bytes_to_reserve);
	}

	if (queryspace_reserved && gp_workfile_limit_per_segment > 0)
	{
		segspace_reserved = WorkfileSegspace_Reserve(bytes_to_reserve);
	}

	return (queryspace_reserved && segspace_reserved);
}

/*
 * Notify of how many bytes were actually written to disk
 *
 * This should be called after writing to disk, with the actual number
 * of bytes written. This must be less or equal than the amount we reserved
 *
 * update_query_space is true if commit should also be applied to the queryspace
 *
 * Returns the current used_diskspace after the commit
 */
void
WorkfileDiskspace_Commit(int64 commit_bytes, int64 reserved_bytes, bool update_query_space)
{
	Assert(reserved_bytes >= commit_bytes);
	if (reserved_bytes == commit_bytes)
	{
		/* Nothing to do, save some work and just return */
		return;
	}

	if ((gp_workfile_limit_per_query > 0) && update_query_space)
	{
		WorkfileQueryspace_Commit(commit_bytes, reserved_bytes);
	}

	if (gp_workfile_limit_per_segment > 0)
	{
		WorkfileSegspace_Commit(commit_bytes, reserved_bytes);
	}
}

/*
 * Sets the flag that marks if on a segment we reached the allowed amount of
 * diskspace to use for workfiles (physical or logical).
 */
void
WorkfileDiskspace_SetFull(bool isFull)
{
	workfile_diskfull = isFull;
}

/*
 * Returns true if we hit the amount of diskspace allowed for workfiles on this
 * segment (physical or logical)
 */
bool
WorkfileDiskspace_IsFull(void)
{
	return workfile_diskfull;
}

/* EOF */
