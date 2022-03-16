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

#ifndef ORCSEGFILES_H
#define ORCSEGFILES_H

#include "access/aosegfiles.h"

#define ORC_DIRECT_DISPATCH -1

#define Natts_pg_orcseg 4
#define Anum_pg_orcseg_segno 1
#define Anum_pg_orcseg_eof 2
#define Anum_pg_orcseg_tupcount 3
#define Anum_pg_orcseg_eofuncompressed 4

#define Natts_pg_orcseg_idx 3
#define Anum_pg_orcseg_idx_idxoid 1
#define Anum_pg_orcseg_idx_segno 2
#define Anum_pg_orcseg_idx_eof 3

extern void insertInitialOrcSegnoEntry(AppendOnlyEntry *aoEntry, int segNo);
extern void insertOrcSegnoEntry(AppendOnlyEntry *aoEntry, int segNo,
                                float8 tupleCount, float8 eof,
                                float8 uncompressedEof);

extern void updateOrcFileSegInfo(Relation rel, AppendOnlyEntry *aoEntry,
                                 int segNo, int64 eof, int64 uncompressedEof,
                                 int64 tupCountAdded, bool forInsert);

extern void insertInitialOrcIndexEntry(AppendOnlyEntry *aoEntry, int idxOid, int segNo);
extern void updateOrcIndexFileInfo(AppendOnlyEntry *aoEntry, int idxOid, int segNo, int64 eof);
extern void deleteOrcIndexFileInfo(Relation rel, AppendOnlyEntry *aoEntry, int idxOid);
extern void deleteOrcIndexHdfsFiles(Relation rel, int32 segmentFileNum, int32 idx);

extern List *orcGetAllSegFileSplits(AppendOnlyEntry *aoEntry,
                                    Snapshot snapshot);

extern FileSegInfo **getAllOrcFileSegInfo(AppendOnlyEntry *aoEntry,
                                          Snapshot snapshot, int *totalSegs);

extern FileSegInfo **getAllOrcFileSegInfoWithSegNo(AppendOnlyEntry *aoEntry,
                                                   Snapshot snapshot, int segNo,
                                                   int *totalSegs);

extern FileSegInfo *getOrcFileSegInfo(Relation rel, AppendOnlyEntry *aoEntry,
                                      Snapshot snapshot, int segNo);

extern void fetchOrcSegFileInfo(AppendOnlyEntry *aoEntry, List *segFileInfo,
                                Snapshot snapshot);

extern FileSegTotals *getOrcSegFileStats(Relation rel, Snapshot snapshot);
extern int64 getOrcTotalBytes(Relation rel, Snapshot snapshot);
extern Datum getOrcCompressionRatio(Relation rel);

#endif /* ORCSEGFILES_H */
