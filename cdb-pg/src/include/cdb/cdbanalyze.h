/*-------------------------------------------------------------------------
 *
 * cdbanalyze.h
 *	  Provides routines for performing distributed analyze cdb
 *
 * Copyright (c) 2005-2008, Greenplum inc
 *
 *-------------------------------------------------------------------------
 */

#ifndef CDBANALYZE_H
#define CDBANALYZE_H

#include "commands/vacuum.h"
#include "cdb/cdbdispatchresult.h" /* CdbDispatchResults */

extern const int gp_external_table_default_number_of_pages;
extern const int gp_external_table_default_number_of_tuples;

void gp_statistics_estimate_reltuples_relpages_external_gpxf(Relation rel, 
															 StringInfo location,
															 float4 *reltuples, 
															 float4 *relpages,
															 StringInfo err_msg);

#endif   /* CDBANALYZE_H */
