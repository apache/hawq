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
 * cdbplan.h
 *	  definitions for cdbplan.c utilities
 *
 *
 *
 * NOTES
 *	  See src/backend/utils/misc/guc.c for variable external specification.
 *
 *-------------------------------------------------------------------------
 */

#ifndef CDBPLAN_H
#define CDBPLAN_H

#include "optimizer/walkers.h"





typedef struct XsliceInAppendContext
{
	plan_tree_base_prefix base;
	
	/* The slice number for the node that is being processed */
	int currentSliceNo;

	/* A set of slice numbers that contain cross-slice shared nodes */
	Bitmapset *slices;
} XsliceInAppendContext;


extern Node * plan_tree_mutator(Node *node, Node *(*mutator) (), void *context);

extern bool set_hasxslice_in_append_walker(Node *node, void *ctx);

#endif   /* CDBPLAN_H */
