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
 * cdbglobalsequence.h
 *
 *
 *-------------------------------------------------------------------------
 */
#ifndef CDBGLOBALSEQUENCE_H
#define CDBGLOBALSEQUENCE_H

#include "utils/palloc.h"
#include "storage/fd.h"
#include "storage/itemptr.h"
#include "utils/rel.h"
#include "catalog/gp_global_sequence.h"

int64 GlobalSequence_Next(
	GpGlobalSequence		gpGlobalSequence);

int64 GlobalSequence_NextInterval(
	GpGlobalSequence		gpGlobalSequence,

	int64					interval);

int64 GlobalSequence_Current(
	GpGlobalSequence		gpGlobalSequence);

void GlobalSequence_Set(
	GpGlobalSequence		gpGlobalSequence,

	int64					newSequenceNum);

#endif   /* CDBGLOBALSEQUENCE_H */
