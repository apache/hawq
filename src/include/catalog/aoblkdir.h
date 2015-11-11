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
 * aoblkdir.h
 *
 *   This file contains some definitions to support creation of aoblkdir tables.
 *
 *-------------------------------------------------------------------------
 */
#ifndef AOBLKDIR_H
#define AOBLKDIR_H

/*
 * Macros to the attribute number for each attribute
 * in the block directory relation.
 */
#define Natts_pg_aoblkdir              4
#define Anum_pg_aoblkdir_segno         1
#define Anum_pg_aoblkdir_columngroupno 2
#define Anum_pg_aoblkdir_firstrownum   3
#define Anum_pg_aoblkdir_minipage      4

extern void AlterTableCreateAoBlkdirTable(Oid relOid);
extern void AlterTableCreateAoBlkdirTableWithOid(
	Oid relOid, Oid newOid, Oid newIndexOid,
	Oid * comptypeOid, bool is_part_child);

#endif
