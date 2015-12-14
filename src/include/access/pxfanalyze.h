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
* pxfanalyze.h
*	  Helper functions to perform ANALYZE on PXF tables.
*
*-------------------------------------------------------------------------
*/
#ifndef PXFANALYZE_H
#define PXFANALYZE_H

#include "c.h"
#include "utils/rel.h"
#include "nodes/pg_list.h"
#include "lib/stringinfo.h"

/*
 * Creates a sample table with data from a PXF table.
 */
extern Oid buildPxfSampleTable(Oid relationOid,
		char* sampleTableName,
		List *lAttributeNames,
		float4	relTuples,
		float4  relFrags,
		float4 	requestedSampleSize,
		float4 *sampleTableRelTuples);
/*
 * get tuple count estimate, page count estimate (which is
 * the number of fragments) of a given PXF table.
 */
void analyzePxfEstimateReltuplesRelpages(Relation relation,
		StringInfo location,
		float4* estimatedRelTuples,
		float4* estimatedRelPages);

#endif   /* PXFANALYZE_H */
