/*-------------------------------------------------------------------------
 *
 * cdbanalyze.c
 *	  Provides routines for performing distributed analyze cdb
 *
 * Copyright (c) 2005-2008, Greenplum inc
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"
#include "executor/spi.h"
#include "lib/stringinfo.h"
#include "commands/vacuum.h"
#include "cdb/cdbpathlocus.h"	/* cdbpathlocus_querysegmentcatalogs */
#include "cdb/cdbvars.h"		/* Gp_role */
#include "cdb/cdbanalyze.h"		/* me */
#include "catalog/pg_statistic.h"
#include "utils/lsyscache.h"
#include "fmgr.h"
#include "gp-libpq-fe.h"
#include "gp-libpq-int.h"
