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
* cdbsreh.h
*	  routines for single row error handling
*
*
*-------------------------------------------------------------------------
*/

#ifndef CDBSREH_H
#define CDBSREH_H

#include "c.h"
#include "cdb/cdbcopy.h"
#include "utils/memutils.h"


/*
 * The error table is ALWAYS of the following format
 * cmdtime     timestamptz,
 * relname     text,
 * filename    text,
 * linenum     int,
 * bytenum     int,
 * errmsg      text,
 * rawdata     text,
 * rawbytes    bytea
 */
#define NUM_ERRORTABLE_ATTR 8
#define errtable_cmdtime 1
#define errtable_relname 2
#define errtable_filename 3   
#define errtable_linenum 4    
#define errtable_bytenum 5    
#define errtable_errmsg 6    
#define errtable_rawdata 7    
#define errtable_rawbytes 8   

/*
 * In cases of invalid csv input data we end up with not being able to parse the
 * data, resulting in very large data rows. In copy.c we throw an error ("line
 * too long") and continue to try and parse. In some cases this is enough to 
 * recover and continue parsing. However in other cases, especially in input 
 * data that includes a lot of valid embedded newlines, we may never be able to
 * recover from an error and will continue to parse huge lines and abort. In
 * here we try to detect this case and abort the operation.
 */
#define CSV_IS_UNPARSABLE(sreh) (sreh->consec_csv_err == 3 ? (true) : (false))

/*
 * All the Single Row Error Handling state is kept here.
 * When an error happens and we are in single row error handling
 * mode this struct is updated and handed to the single row
 * error handling manager (cdbsreh.c).
 */
typedef struct CdbSreh
{
	/* bad row information */
	char	*errmsg;		/* the error message for this bad data row */
	char	*rawdata;		/* the bad data row */
	char	*relname;		/* target relation */
	int64		linenumber;		/* line number of error in original file */
	uint64  processed;      /* num logical input rows processed so far */
	bool	is_server_enc;	/* was bad row converted to server encoding? */
	int		consec_csv_err; /* # of consecutive invalid csv errors */

	/* reject limit state */
	int		rejectlimit;	/* SEGMENT REJECT LIMIT value */
	int		rejectcount;	/* how many were rejected so far */
	bool	is_limit_in_rows; /* ROWS = true, PERCENT = false */
	
	/* the error table */
	Relation errtbl;		/* the error table we use (if any) */
	struct AppendOnlyInsertDescData *err_aoInsertDesc;
	int err_aosegno;
	ResultRelSegFileInfo *err_aosegfileinfo;
	
	/* error table lifespan */
	bool	is_keep;		/* if true error table should not get DROP'ed */
	bool	reusing_errtbl; /* true if we are using an existing table (did not auto generate a new one) */
	bool	should_drop;	/* true if we decide to DROP errtbl at end of execution (depends on previous 2 vars) */
	
	/* COPY only vars */
	CdbCopy *cdbcopy;		/* for QD COPY to send bad rows to random QE */
	int		lastsegid;		/* last QE COPY segid that QD COPY sent bad row to */
	
	MemoryContext badrowcontext;	/* per-badrow evaluation context */
	char	   filename[256];		/* "uri [filename]" */

} CdbSreh;

extern CdbSreh *makeCdbSreh(bool is_keep, bool reusing_existing_errtable, 
							int rejectlimit,
							bool is_limit_in_rows, RangeVar *errtbl, ResultRelSegFileInfo *segfileinfo,
							char *filename, char *relname);
extern void destroyCdbSreh(CdbSreh *cdbsreh);
extern void HandleSingleRowError(CdbSreh *cdbsreh);
extern void ReportSrehResults(CdbSreh *cdbsreh, int total_rejected);
extern void SendNumRowsRejected(int numrejected);
extern void ValidateErrorTableMetaData(Relation rel);
extern void SetErrorTableVerdict(CdbSreh *cdbsreh, int total_rejected);
extern bool IsRejectLimitReached(CdbSreh *cdbsreh);
extern void emitSameTxnWarning(void);
extern void VerifyRejectLimit(char rejectlimittype, int rejectlimit);


#endif /* CDBSREH_H */
