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

/*-------------------------------------------------------------------------
 *
 * aomd.c
 *	  This code manages append only relations that reside on magnetic disk.
 *	  It serves the same general purpose as smgr/md.c however we introduce
 *    AO specific file access functions mainly because would like to bypass 
 *	  md.c's and bgwriter's fsyncing. AO relations also use a non constant
 *	  block number to file segment mapping unlike heap relations.
 *
 *	  As of now we still let md.c create and unlink AO relations for us. This
 *	  may need to change if inconsistencies arise.
 *
 * Portions Copyright (c) 2008, Greenplum Inc.
 * Portions Copyright (c) 1996-2008, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"
#include "access/aomd.h"
#include "catalog/catalog.h"

#include <unistd.h>
#include <fcntl.h>
#include <sys/file.h>

#include "utils/guc.h"
#include "access/appendonlytid.h"
#include "cdb/cdbappendonlystorage.h"
#include "cdb/cdbmirroredappendonly.h"
#include "cdb/cdbvars.h"

int
AOSegmentFilePathNameLen(Relation rel)
{
	char		*basepath;
	int 		len;
		
	/* Get base path for this relation file */
	basepath = relpath(rel->rd_node);

	/*
	 * The basepath will be the RelFileNode number.  Optional part is dot "." plus 
	 * 6 digit segment file number.
	 */
	len = strlen(basepath) + 8;	// Generous.
	
	pfree(basepath);

	return len;
}

/*
 * Formats an Append Only relation file segment file name.
 *
 * The filepathname parameter assume sufficient space.
 */
void
FormatAOSegmentFileName(
							char *basepath, 
							int segno, 
							int col,
							int numCols,
							int32 *fileSegNo,
							char *filepathname)
{
	int	pseudoSegNo;
	
	if (col < 0)
	{
		/*
		 * Row oriented Append-Only.
		 */
		pseudoSegNo = segno;		
	}
	else
	{
		/*
		 * Column oriented Append-only.
		 */
		pseudoSegNo = ((segno - 1) * numCols) + (col + 1);
	}
	
	*fileSegNo = pseudoSegNo;

	if (pseudoSegNo > 0)
	{
		sprintf(filepathname, "%s/%u", basepath, pseudoSegNo);
	}
	else
		sprintf(filepathname, "%s", basepath);
}

/*
 * Make an Append Only relation file segment file name.
 *
 * The filepathname parameter assume sufficient space.
 */
void
MakeAOSegmentFileName(
							Relation rel, 
							int segno, 
							int col, 
							int32 *fileSegNo,
							char *filepathname)
{
	char	*basepath;
	int32   fileSegNoLocal;
	int numCols;
	
	/* Get base path for this relation file */
	basepath = relpath(rel->rd_node);
	numCols = rel->rd_att->natts;

	FormatAOSegmentFileName(basepath, segno, col, numCols, &fileSegNoLocal, filepathname);
	
	*fileSegNo = fileSegNoLocal;
	
	pfree(basepath);
}

/*
 * Open an Append Only relation file segment
 *
 * The fd module's PathNameOpenFile() is used to open the file, so the
 * the File* routines can be used to read, write, close, etc, the file.
 */
bool
OpenAOSegmentFile(
					Relation rel, 
					char *filepathname, 
				  int32	segmentFileNum,
				  int64	logicalEof,
				  MirroredAppendOnlyOpen *mirroredOpen)
{	
	ItemPointerData persistentTid;
	int64 persistentSerialNum;

	int primaryError;

	if (!ReadGpRelfileNode(
				rel->rd_node.relNode,
				segmentFileNum,
				&persistentTid,
				&persistentSerialNum))
	{
		if (logicalEof == 0)
			return false;

		elog(ERROR, "Did not find gp_relation_node entry for relation name %s, relation id %u, relfilenode %u, segment file #%d, logical eof " INT64_FORMAT,
			 rel->rd_rel->relname.data,
			 rel->rd_id,
			 rel->rd_node.relNode,
			 segmentFileNum,
			 logicalEof);
	}

	MirroredAppendOnly_OpenReadWrite(
							mirroredOpen, 
							&rel->rd_node,
							segmentFileNum,
							/*
							 * TODO, in hawq, fix later
							 */
							/* relationName */ NULL,		// Ok to be NULL -- we don't know the name here.
							logicalEof,
							true,
							&primaryError);
	if (primaryError != 0)
		ereport(ERROR,
			   (errcode_for_file_access(),
			    errmsg("Could not open Append-Only segment file '%s': %s", filepathname, strerror(primaryError)),
			    errdetail("%s", HdfsGetLastError())));

	return true;
}


/*
 * Close an Append Only relation file segment
 */
void
CloseAOSegmentFile(MirroredAppendOnlyOpen *mirroredOpen)
{
	/*bool mirrorDataLossOccurred;*/	// UNDONE: We need to do something now...

	Assert(mirroredOpen->primaryFile > 0);
	
	MirroredAppendOnly_Close(
						mirroredOpen/*,
						&mirrorDataLossOccurred*/);
}

/*
 * Truncate all bytes from offset to end of file.
 */
void
TruncateAOSegmentFile(MirroredAppendOnlyOpen *mirroredOpen, Relation rel, int64 offset, int elevel)
{
	int primaryError;
	/*bool mirrorDataLossOccurred;*/	// We'll look at this at close time.

	char *relname = RelationGetRelationName(rel);
	
	Assert(mirroredOpen->primaryFile > 0);
	Assert(offset >= 0);

	/*
	 * Call the 'fd' module with a 64-bit length since AO segment files
	 * can be multi-gigabyte to the terabytes...
	 */
	MirroredAppendOnly_Truncate(
							mirroredOpen,
							offset,
							&primaryError/*,
							&mirrorDataLossOccurred*/);
	if (primaryError != 0)
		ereport(elevel,
				(errmsg("\"%s\": failed to truncate data after eof: %s", 
					    relname,
					    strerror(primaryError))));
	
}

