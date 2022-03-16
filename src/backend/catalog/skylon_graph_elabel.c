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

#include "access/fileam.h"
#include "access/genam.h"
#include "access/heapam.h"
#include "catalog/catquery.h"
#include "catalog/dependency.h"
#include "catalog/indexing.h"
#include "catalog/pg_proc.h"
#include "catalog/pg_type.h"
#include "catalog/skylon_graph_elabel.h"
#include "mb/pg_wchar.h"
#include "utils/array.h"
#include "utils/builtins.h"
#include "utils/fmgroids.h"
#include "utils/lsyscache.h"
#include "utils/syscache.h"
#include "utils/uri.h"

void InsertGraphElabelEntry(const char* schemaname, const char* graphname, const char* elabelname, Oid reloid) {
  Relation skylon_graph_elabel_rel;
  HeapTuple skylon_graph_elabel_tuple = NULL;
  bool nulls[Natts_skylon_graph_elabel];
  Datum values[Natts_skylon_graph_elabel];
  cqContext cqc;
  cqContext* pcqCtx;

  MemSet(values, 0, sizeof(values));
  MemSet(nulls, false, sizeof(nulls));

  /*
   * Open and lock the pg_exttable catalog.
   */
  skylon_graph_elabel_rel = heap_open(GraphElabelRelationId, RowExclusiveLock);

  pcqCtx = caql_beginscan(caql_addrel(cqclr(&cqc), skylon_graph_elabel_rel),
                          cql("INSERT INTO skylon_graph_elabel", NULL));
  NameData name0;
  namestrcpy(&name0, schemaname);
  values[Anum_skylon_graph_elabel_schemaname - 1] = NameGetDatum(&name0);
  NameData name1;
  namestrcpy(&name1, graphname);
  values[Anum_skylon_graph_elabel_graphname - 1] = NameGetDatum(&name1);
  NameData name2;
  namestrcpy(&name2, elabelname);
  values[Anum_skylon_graph_elabel_elabelname - 1] = NameGetDatum(&name2);
  values[Anum_skylon_graph_elabel_reloid - 1] = ObjectIdGetDatum(reloid);
  skylon_graph_elabel_tuple = caql_form_tuple(pcqCtx, values, nulls);
  caql_insert(pcqCtx, skylon_graph_elabel_tuple);
  caql_endscan(pcqCtx);
  heap_close(skylon_graph_elabel_rel, RowExclusiveLock);
}
