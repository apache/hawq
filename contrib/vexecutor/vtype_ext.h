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
#ifndef __VTYPE_EXT_H__
#define __VTYPE_EXT_H__
#include "postgres.h"
#include "fmgr.h"
#include "catalog/pg_type.h"
#include "utils/date.h"
#include "vtype.h"

typedef struct vtype vdateadt;

extern Datum vdateadtin(PG_FUNCTION_ARGS);
extern Datum vdateadtout(PG_FUNCTION_ARGS);
extern Datum vdateadt_eq(PG_FUNCTION_ARGS);
extern Datum vdateadt_ne(PG_FUNCTION_ARGS);
extern Datum vdateadt_lt(PG_FUNCTION_ARGS);
extern Datum vdateadt_le(PG_FUNCTION_ARGS);
extern Datum vdateadt_gt(PG_FUNCTION_ARGS);
extern Datum vdateadt_ge(PG_FUNCTION_ARGS);
extern Datum vdateadt_mi(PG_FUNCTION_ARGS);
extern Datum vdateadt_pli(PG_FUNCTION_ARGS);
extern Datum vdateadt_mii(PG_FUNCTION_ARGS);

extern Datum vdateadt_eq_dateadt(PG_FUNCTION_ARGS);
extern Datum vdateadt_ne_dateadt(PG_FUNCTION_ARGS);
extern Datum vdateadt_lt_dateadt(PG_FUNCTION_ARGS);
extern Datum vdateadt_le_dateadt(PG_FUNCTION_ARGS);
extern Datum vdateadt_gt_dateadt(PG_FUNCTION_ARGS);
extern Datum vdateadt_ge_dateadt(PG_FUNCTION_ARGS);
extern Datum vdateadt_mi_dateadt(PG_FUNCTION_ARGS);
extern Datum vdateadt_mii_int4(PG_FUNCTION_ARGS);
extern Datum vdateadt_pli_int4(PG_FUNCTION_ARGS);
#endif
