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

#include "postgres.h"

#include "catalog/skylon_index.h"
#include "catalog/pg_type.h"
#include "catalog/pg_proc.h"
#include "access/genam.h"
#include "catalog/catquery.h"
#include "access/fileam.h"
#include "access/heapam.h"
#include "catalog/dependency.h"
#include "catalog/indexing.h"
#include "mb/pg_wchar.h"
#include "utils/array.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/syscache.h"
#include "utils/fmgroids.h"
#include "utils/uri.h"

void InsertSkylonIndexEntry(const char* schemaname , const char* graphname,
                       const char* elename, char indextype, const char* indexname,
                       const int2* indexkeys, int indexkeysnum, const int2* includekeys, int includekeysnum) {
  Relation  skylon_index_rel;
  HeapTuple skylon_index_tuple = NULL;
  bool    nulls[Natts_skylon_index];
  Datum   values[Natts_skylon_index];
  cqContext cqc;
  cqContext  *pcqCtx;

  MemSet(values, 0, sizeof(values));
  MemSet(nulls, false, sizeof(nulls));

  skylon_index_rel = heap_open(SkylonIndexRelationId, RowExclusiveLock);

  pcqCtx = caql_beginscan(
      caql_addrel(cqclr(&cqc), skylon_index_rel),
      cql("INSERT INTO skylon_index",
        NULL));
  NameData  name1;
  namestrcpy(&name1, schemaname);
  values[Anum_skylon_index_schemaname - 1] = NameGetDatum(&name1);
  NameData  name2;
  namestrcpy(&name2, graphname);
  values[Anum_skylon_index_graphname - 1] = NameGetDatum(&name2);
  NameData  name3;
  namestrcpy(&name3, elename);
  values[Anum_skylon_index_elename - 1] = NameGetDatum(&name3);
  NameData  name4;
  namestrcpy(&name4, indexname);
  values[Anum_skylon_index_indexname - 1] = NameGetDatum(&name4);
  values[Anum_skylon_index_indextype - 1] = CharGetDatum(indextype);
  int2vector *indkeys = buildint2vector(NULL, indexkeysnum);
  for (int i = 0; i < indexkeysnum; i++)
    indkeys->values[i] = indexkeys[i];
  values[Anum_skylon_index_indexkeys - 1] = PointerGetDatum(indkeys);
  int2vector *incldkeys = buildint2vector(NULL, includekeysnum);
  for (int i = 0; i < includekeysnum; i++)
    incldkeys->values[i] = includekeys[i];
  values[Anum_skylon_index_includekeys - 1] = PointerGetDatum(incldkeys);

  skylon_index_tuple = caql_form_tuple(pcqCtx, values, nulls);
  caql_insert(pcqCtx, skylon_index_tuple);
  caql_endscan(pcqCtx);
  heap_close(skylon_index_rel, RowExclusiveLock);
}
