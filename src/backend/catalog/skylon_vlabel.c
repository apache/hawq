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

#include "catalog/skylon_vlabel.h"
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

void
InsertVlabelEntry(const char* vlabelname,
                  const char* schemaname){
  Relation  skylon_vlabel_rel;
  HeapTuple skylon_vlabel_tuple = NULL;
  bool    nulls[Natts_skylon_vlabel];
  Datum   values[Natts_skylon_vlabel];
  cqContext cqc;
  cqContext  *pcqCtx;

  MemSet(values, 0, sizeof(values));
  MemSet(nulls, false, sizeof(nulls));

    /*
     * Open and lock the pg_exttable catalog.
     */
  skylon_vlabel_rel = heap_open(VlabelRelationId, RowExclusiveLock);

  pcqCtx = caql_beginscan(
      caql_addrel(cqclr(&cqc), skylon_vlabel_rel),
      cql("INSERT INTO skylon_vlabel",
        NULL));
  NameData  name1;
  namestrcpy(&name1, vlabelname);
  values[Anum_skylon_vlabel_vlabelname - 1] = NameGetDatum(&name1);
  NameData  name2;
  namestrcpy(&name2, schemaname);
  values[Anum_skylon_vlabel_schemaname - 1] = NameGetDatum(&name2);
  skylon_vlabel_tuple = caql_form_tuple(pcqCtx, values, nulls);
  caql_insert(pcqCtx, skylon_vlabel_tuple);
  caql_endscan(pcqCtx);
  heap_close(skylon_vlabel_rel, RowExclusiveLock);
}
