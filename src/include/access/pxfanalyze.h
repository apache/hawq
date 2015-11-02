/*-------------------------------------------------------------------------
*
* pxfanalyze.h
*	  Helper functions to perform ANALYZE on PXF tables.
*
* Copyright (c) 2007-2008, Greenplum inc
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
		float4* estimatedRelPages,
		StringInfo err_msg);

#endif   /* PXFANALYZE_H */
