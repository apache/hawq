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
 * cdbpath.h
 *
 *
 *-------------------------------------------------------------------------
 */
#ifndef CDBPATH_H
#define CDBPATH_H

#include "cdb/cdbvars.h"        /* getgpsegmentCount, gp_opt_segments, etc */

void
cdbpath_cost_motion(PlannerInfo *root, CdbMotionPath *motionpath);

Path *
cdbpath_create_motion_path(PlannerInfo     *root,
                           Path            *subpath,
                           List            *pathkeys,
                           bool             require_existing_order,
                           CdbPathLocus     locus);

CdbPathLocus
cdbpath_motion_for_join(PlannerInfo    *root,
                        JoinType        jointype,           /* JOIN_INNER/FULL/LEFT/RIGHT/IN */
                        Path          **p_outer_path,       /* INOUT */
                        Path          **p_inner_path,       /* INOUT */
                        List           *mergeclause_list,   /* equijoin RestrictInfo list */
                        List           *outer_pathkeys,
                        List           *inner_pathkeys,
                        bool            outer_require_existing_order,
                        bool            inner_require_existing_order);

void 
cdbpath_dedup_fixup(PlannerInfo *root, Path *path);

/*
 * cdbpath_rows
 *
 * Returns a Path's estimated number of result rows.
 */
static inline double
cdbpath_rows(PlannerInfo *root, Path *path)
{
    double rows;
    Path  *p;

	p = (IsA(path, CdbMotionPath))  ? ((CdbMotionPath *)path)->subpath
		: path;

	rows = IsA(p, BitmapHeapPath)   ? ((BitmapHeapPath *)p)->rows
		: IsA(p, BitmapAppendOnlyPath) ? ((BitmapAppendOnlyPath *)p)->rows
		: IsA(p, IndexPath)        ? ((IndexPath *)p)->rows
		: IsA(p, UniquePath)       ? ((UniquePath *)p)->rows
		: (CdbPathLocus_IsBottleneck(path->locus) ||
		   CdbPathLocus_IsReplicated(path->locus)) 
		? path->parent->rows * root->config->cdbpath_segments
		: path->parent->rows;

    return rows;
}                               /* cdbpath_rows */

#endif   /* CDBPATH_H */
