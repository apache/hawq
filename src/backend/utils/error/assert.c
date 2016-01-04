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
 * assert.c
 *	  Assert code.
 *
 * Portions Copyright (c) 2005-2009, Greenplum inc
 * Portions Copyright (c) 1996-2009, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  $PostgreSQL: pgsql/src/backend/utils/error/assert.c,v 1.31 2005/10/15 02:49:32 momjian Exp $
 *
 * NOTE
 *	  This should eventually work with elog()
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "libpq/pqsignal.h"
#include "cdb/cdbvars.h"                /* currentSliceId */

#include <unistd.h>

/*
 * ExceptionalCondition - Handles the failure of an Assert()
 *
 * Note: this can't actually return, but we declare it as returning int
 * because the TrapMacro() macro might get wonky otherwise.
 */
int
ExceptionalCondition(const char *conditionName,
					 const char *errorType,
					 const char *fileName,
					 int         lineNumber)
{
    /* CDB: Try to tell the QD or client what happened. */
    if (errstart(FATAL, fileName, lineNumber, NULL,TEXTDOMAIN))
    {
		if (!PointerIsValid(conditionName)
			|| !PointerIsValid(fileName)
			|| !PointerIsValid(errorType))
			errfinish(errcode(ERRCODE_INTERNAL_ERROR),
					  errFatalReturn(gp_reraise_signal),
					  errmsg("TRAP: ExceptionalCondition: bad arguments"));
		else
			errfinish(errcode(ERRCODE_INTERNAL_ERROR),
					  errFatalReturn(gp_reraise_signal),
					  errmsg("Unexpected internal error"),
					  errdetail("%s(\"%s\", File: \"%s\", Line: %d)\n",
								errorType, conditionName, fileName, lineNumber)
				);
				
		/* Usually this shouldn't be needed, but make sure the msg went out */
		fflush(stderr);
		
	}
	
    abort();
	return 0;
}
