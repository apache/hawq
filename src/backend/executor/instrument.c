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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/*-------------------------------------------------------------------------
 *
 * instrument.c
 *	 functions for instrumentation of plan execution
 *
 *
 * Portions Copyright (c) 2006-2009, Greenplum inc
 * Copyright (c) 2001-2009, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  $PostgreSQL: pgsql/src/backend/executor/instrument.c,v 1.18 2006/06/09 19:30:56 tgl Exp $
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <unistd.h>

#include "executor/instrument.h"
#include "utils/guc.h"

/* Allocate new instrumentation structure(s) */
Instrumentation *
InstrAlloc(int n)
{
	Instrumentation *instr = palloc0(n * sizeof(Instrumentation));

	/* we don't need to do any initialization except zero 'em */
	instr->numPartScanned = 0;

	return instr;
}

/* Entry to a plan node */
void
InstrStartNode(Instrumentation *instr)
{
	if (INSTR_TIME_IS_ZERO(instr->starttime))
		INSTR_TIME_SET_CURRENT(instr->starttime);
	else
		elog(DEBUG2, "InstrStartNode called twice in a row");
}

/* Exit from a plan node */
void
InstrStopNode(Instrumentation *instr, double nTuples)
{
	instr_time	endtime;

	/* count the returned tuples */
	instr->tuplecount += nTuples;

	if (INSTR_TIME_IS_ZERO(instr->starttime))
	{
		elog(DEBUG2, "InstrStopNode called without start");
		return;
	}

	INSTR_TIME_SET_CURRENT(endtime);
	INSTR_TIME_ACCUM_DIFF(instr->counter, endtime, instr->starttime);

	/* Is this the first tuple of this cycle? */
	if (!instr->running)
	{
		instr->running = true;
		instr->firsttuple = INSTR_TIME_GET_DOUBLE(instr->counter);
		/* CDB: save this start time as the first start */
		instr->firststart = instr->starttime;
	}

	INSTR_TIME_SET_ZERO(instr->starttime);
}

/* Finish a run cycle for a plan node */
void
InstrEndLoop(Instrumentation *instr)
{
	double		totaltime;

	/* Skip if nothing has happened, or already shut down */
	if (!instr->running)
		return;

	if (!INSTR_TIME_IS_ZERO(instr->starttime))
		elog(DEBUG2, "InstrEndLoop called on running node");

	/* Accumulate per-cycle statistics into totals */
	totaltime = INSTR_TIME_GET_DOUBLE(instr->counter);

    /* CDB: Report startup time from only the first cycle. */
    if (instr->nloops == 0)
        instr->startup = instr->firsttuple;

    instr->total += totaltime;
    instr->totalLast += totaltime;
	instr->ntuples += instr->tuplecount;
	instr->nloops += 1;

	if (Debug_print_execution_detail) {
    elog(DEBUG1,"instr->total: %.3f ms, instr->ntuples: %.3f, instr->nloops: %.3f",
         1000.0 * instr->total, instr->ntuples, instr->nloops);
  }

	/* Reset for next cycle (if any) */
	instr->running = false;
	INSTR_TIME_SET_ZERO(instr->starttime);
	INSTR_TIME_SET_ZERO(instr->counter);
	instr->firsttuple = 0;
	instr->tuplecount = 0;
}
