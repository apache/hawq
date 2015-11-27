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
 * aomd.h
 *	  declarations and functions for supporting aomd.c
 *
 *-------------------------------------------------------------------------
 */
#ifndef AOMD_H
#define AOMD_H

#include "storage/fd.h"
#include "utils/rel.h"

struct MirroredAppendOnlyOpen;  /* Defined in cdb/cdbmirroredappendonly.h */

extern int
AOSegmentFilePathNameLen(Relation rel);

extern void
FormatAOSegmentFileName(
							char *basepath, 
							int segno, 
							int col,
							int numColumn,
							int32 *fileSegNo,
							char *filepathname);

extern void
MakeAOSegmentFileName(
							Relation rel, 
							int segno, 
							int col, 
							int32 *fileSegNo,
							char *filepathname);

extern bool
OpenAOSegmentFile(
					Relation rel, 
					char *filepathname, 
					int32	segmentFileNum,
					int64	logicalEof,
					struct MirroredAppendOnlyOpen *mirroredOpen);

extern void
CloseAOSegmentFile(
				   struct MirroredAppendOnlyOpen *mirroredOpen);

extern void
TruncateAOSegmentFile(
					  struct MirroredAppendOnlyOpen *mirroredOpen, 
					  Relation rel, 
					  int64 offset, 
					  int elevel);

#endif   /* AOMD_H */
