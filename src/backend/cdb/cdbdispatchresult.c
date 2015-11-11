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
 * cdbdispatchresult.c
 *    Functions to dispatch commands to QExecutors
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"
#include <pthread.h>

#include "gp-libpq-fe.h"               /* prerequisite for libpq-int.h */
#include "gp-libpq-int.h"              /* PQExpBufferData */

#include "lib/stringinfo.h"         /* StringInfoData */
#include "utils/guc.h"              /* log_min_messages */

#include "cdb/cdbconn.h"            /* SegmentDatabaseDescriptor */
#include "cdb/cdbpartition.h"
#include "cdb/cdbvars.h"
#include "cdb/cdbsreh.h"
#include "cdb/cdbdispatchresult.h"  /* me */
#include "commands/tablecmds.h"


/* This mutex serializes writes by dispatcher threads to the
 * iFirstError and errcode fields of CdbDispatchResults objects.
 */
static pthread_mutex_t  setErrcodeMutex = PTHREAD_MUTEX_INITIALIZER;


/*--------------------------------------------------------------------*/

/* Make sure buffer has no trailing newline, whitespace, etc. */
static void
noTrailingNewline(StringInfo buf)
{
    while (buf->len > 0 &&
           buf->data[buf->len-1] <= ' ' &&
           buf->data[buf->len-1] > '\0')
        buf->data[--buf->len] = '\0';
}                               /* noTrailingNewline */

static void
noTrailingNewlinePQ(PQExpBuffer buf)
{
    while (buf->len > 0 &&
           buf->data[buf->len-1] <= ' ' &&
           buf->data[buf->len-1] > '\0')
        buf->data[--buf->len] = '\0';
}                               /* noTrailingNewlinePQ */


/* If buffer is nonempty, make sure it has exactly one trailing newline. */
static void
oneTrailingNewline(StringInfo buf)
{
    noTrailingNewline(buf);
    if (buf->len > 0)
        appendStringInfoChar(buf, '\n');
}                               /* oneTrailingNewline */

static void
oneTrailingNewlinePQ(PQExpBuffer buf)
{
    noTrailingNewlinePQ(buf);
    if (buf->len > 0)
        appendPQExpBufferChar(buf, '\n');
}                               /* oneTrailingNewlinePQ */


/*--------------------------------------------------------------------*/

/*
 * Create a CdbDispatchResult object, appending it to the
 * resultArray of a given CdbDispatchResults object.
 */
CdbDispatchResult *
cdbdisp_makeResult(struct CdbDispatchResults           *meleeResults,
                   struct SegmentDatabaseDescriptor    *segdbDesc,
                   int                                  sliceIndex)
{
    CdbDispatchResult  *dispatchResult;
    int                 meleeIndex;

    Assert(meleeResults &&
           meleeResults->resultArray &&
           meleeResults->resultCount < meleeResults->resultCapacity);

    /* Allocate a slot for the new CdbDispatchResult object. */
    meleeIndex = meleeResults->resultCount++;
    dispatchResult = &meleeResults->resultArray[meleeIndex];

    /* Initialize CdbDispatchResult. */
    dispatchResult->meleeResults = meleeResults;
    dispatchResult->meleeIndex = meleeIndex;
    dispatchResult->segdbDesc = segdbDesc;
    dispatchResult->resultbuf = createPQExpBuffer();
    dispatchResult->error_message = NULL;
	dispatchResult->numrowsrejected = 0;

	if (PQExpBufferBroken(dispatchResult->resultbuf))
	{
		destroyPQExpBuffer(dispatchResult->resultbuf);
		dispatchResult->resultbuf = NULL;
		/* caller is responsible for cleanup -- can't elog(ERROR, ...) from here. */
		return NULL;
	}

    /* Reset summary indicators. */
    cdbdisp_resetResult(dispatchResult);

    /* Update slice map entry. */
    if (sliceIndex >= 0 &&
        sliceIndex < meleeResults->sliceCapacity)
    {
        CdbDispatchResults_SliceInfo *si = &meleeResults->sliceMap[sliceIndex];
        if (si->resultBegin == si->resultEnd)
        {
            si->resultBegin = meleeIndex;
            si->resultEnd = meleeIndex + 1;
        }
        else
        {
            if (si->resultBegin > meleeIndex)
                si->resultBegin = meleeIndex;
            if (si->resultEnd <= meleeIndex)
                si->resultEnd = meleeIndex + 1;
        }
    }

    /* Insert the slice index in error messages related to this QE. */
    if (segdbDesc)
        cdbconn_setSliceIndex(segdbDesc, sliceIndex);

    return dispatchResult;
}                               /* cdbdisp_initResult */


/* Destroy a CdbDispatchResult object. */
void
cdbdisp_termResult(CdbDispatchResult  *dispatchResult)
{
	PQExpBuffer trash;

    dispatchResult->segdbDesc = NULL;

    /* Free the PGresult objects. */
    cdbdisp_resetResult(dispatchResult);

    /* Free the error message buffer and result buffer. */
	trash = dispatchResult->resultbuf;
	dispatchResult->resultbuf = NULL;
    destroyPQExpBuffer(trash);

	trash = dispatchResult->error_message;
	dispatchResult->error_message = NULL;
    destroyPQExpBuffer(trash);
}                               /* cdbdisp_termResult */


/* Reset a CdbDispatchResult object for possible reuse. */
void
cdbdisp_resetResult(CdbDispatchResult  *dispatchResult)
{
    PQExpBuffer buf = dispatchResult->resultbuf;
    PGresult  **begp = (PGresult **)buf->data;
    PGresult  **endp = (PGresult **)(buf->data + buf->len);
    PGresult  **p;

    /* Free the PGresult objects. */
    for (p = begp; p < endp; ++p)
    {
        Assert(*p != NULL);
        PQclear(*p);
    }

    /* Reset summary indicators. */
    dispatchResult->errcode = 0;
    dispatchResult->errindex = -1;
    dispatchResult->okindex = -1;

    /* Reset progress indicators. */
    dispatchResult->hasDispatched = false;
    dispatchResult->stillRunning = false;
    dispatchResult->requestedCancel = false;
    dispatchResult->wasCanceled = false;

    /* Empty (but don't free) the error message buffer and result buffer. */
    resetPQExpBuffer(dispatchResult->resultbuf);
    resetPQExpBuffer(dispatchResult->error_message);
}                               /* cdbdisp_resetResult */


/* Take note of an error.
 * 'errcode' is the ERRCODE_xxx value for setting the client's SQLSTATE.
 * NB: This can be called from a dispatcher thread, so it must not use
 * palloc/pfree or elog/ereport because they are not thread safe.
 */
void
cdbdisp_seterrcode(int                  errcode,        /* ERRCODE_xxx or 0 */
                   int                  resultIndex,    /* -1 if no PGresult */
                   CdbDispatchResult   *dispatchResult)
{
    CdbDispatchResults *meleeResults = dispatchResult->meleeResults;

    /* Was the command canceled? */
    if (errcode == ERRCODE_GP_OPERATION_CANCELED ||
        errcode == ERRCODE_QUERY_CANCELED)
        dispatchResult->wasCanceled = true;

    /* If this is the first error from this QE, save the error code
     * and the index of the PGresult buffer entry.  We assume the
     * caller has not yet added the item to the PGresult buffer.
     */
    if (!dispatchResult->errcode)
    {
        dispatchResult->errcode =
            (errcode == 0) ? ERRCODE_INTERNAL_ERROR : errcode;
        if (resultIndex >= 0)
            dispatchResult->errindex = resultIndex;
    }

    if (!meleeResults)
        return;

    /*
     * Remember which QE reported an error first among the gangs,
     * but keep quiet about cancellation done at our request.
     *
     * Interconnection errors are given lower precedence because often
     * they are secondary to an earlier and more interesting error.
     */
    if (errcode == ERRCODE_GP_OPERATION_CANCELED &&
        dispatchResult->requestedCancel)
    {}                          /* nop */

    else if (meleeResults->errcode == 0 || 
			 (meleeResults->errcode == ERRCODE_GP_INTERCONNECTION_ERROR &&
			  errcode != ERRCODE_GP_INTERCONNECTION_ERROR))
    {
        pthread_mutex_lock(&setErrcodeMutex);
        if ((errcode != 0) && (meleeResults->errcode == 0 ||
			(meleeResults->errcode == ERRCODE_GP_INTERCONNECTION_ERROR &&
			 errcode != ERRCODE_GP_INTERCONNECTION_ERROR)))
        {
            meleeResults->errcode = errcode;
            meleeResults->iFirstError = dispatchResult->meleeIndex;
        }
        pthread_mutex_unlock(&setErrcodeMutex);
    }
}                               /* cdbdisp_seterrcode */


/* Transfer connection error messages to dispatchResult from segdbDesc. */
bool                            /* returns true if segdbDesc had err info */
cdbdisp_mergeConnectionErrors(CdbDispatchResult                *dispatchResult,
                              struct SegmentDatabaseDescriptor *segdbDesc)
{
    if (!segdbDesc)
        return false;
    if (segdbDesc->errcode == 0 &&
        segdbDesc->error_message.len == 0)
        return false;

    /* Error code should always be accompanied by text and vice-versa. */
    Assert(segdbDesc->errcode != 0 && segdbDesc->error_message.len > 0);

    /* Append error message text and save error code. */
    cdbdisp_appendMessage(dispatchResult, 0, segdbDesc->errcode, "%s",
                          segdbDesc->error_message.data);

    /* Reset connection object's error info. */
    segdbDesc->errcode = 0;
    segdbDesc->error_message.len = 0;
    segdbDesc->error_message.data[0] = '\0';

    return true;
}                               /* cdbdisp_mergeConnectionErrors */


/* Format a message, printf-style, and append to the error_message buffer.
 * Also write it to stderr if logging is enabled for messages of the
 * given severity level 'elevel' (for example, DEBUG1; or 0 to suppress).
 * 'errcode' is the ERRCODE_xxx value for setting the client's SQLSTATE.
 * NB: This can be called from a dispatcher thread, so it must not use
 * palloc/pfree or elog/ereport because they are not thread safe.
 */
void
cdbdisp_appendMessage(CdbDispatchResult    *dispatchResult,
                      int                   elevel,
                      int                   errcode,
                      const char           *fmt,
                      ...)
{
    va_list args;
    int     msgoff;

    /* Remember first error. */
    cdbdisp_seterrcode(errcode, -1, dispatchResult);

    /* Allocate buffer if first message.
     * Insert newline between previous message and new one.
     */
    if (!dispatchResult->error_message)
	{
        dispatchResult->error_message = createPQExpBuffer();

		if (PQExpBufferBroken(dispatchResult->error_message))
		{
			destroyPQExpBuffer(dispatchResult->error_message);
			dispatchResult->error_message = NULL;

			write_log("cdbdisp_appendMessage: allocation failed, can't save error-message.");
			return;
		}
	}
    else
        oneTrailingNewlinePQ(dispatchResult->error_message);

    msgoff = dispatchResult->error_message->len;

    /* Format the message and append it to the buffer. */
    va_start(args, fmt);
    appendPQExpBufferVA(dispatchResult->error_message, fmt, args);
    va_end(args);

    /* Display the message on stderr for debugging, if requested.
     * This helps to clarify the actual timing of threaded events.
     */
    if (elevel >= log_min_messages)
    {
        oneTrailingNewlinePQ(dispatchResult->error_message);
        write_log("%s", dispatchResult->error_message->data + msgoff);
    }

    /* In case the caller wants to hand the buffer to ereport(),
     * follow the ereport() convention of not ending with a newline.
     */
    noTrailingNewlinePQ(dispatchResult->error_message);

}                               /* cdbdisp_appendMessage */


/* Store a PGresult object ptr in the result buffer.
 * NB: Caller must not PQclear() the PGresult object.
 */
void
cdbdisp_appendResult(CdbDispatchResult *dispatchResult,
                     struct pg_result  *res)
{
    Assert(dispatchResult && res);

    /* Attach the QE identification string to the PGresult */
    if (dispatchResult->segdbDesc &&
        dispatchResult->segdbDesc->whoami)
        pqSaveMessageField(res, PG_DIAG_GP_PROCESS_TAG,
                           dispatchResult->segdbDesc->whoami);

    appendBinaryPQExpBuffer(dispatchResult->resultbuf, (char *)&res, sizeof(res));
}                               /* cdbdisp_appendResult */


/* Return the i'th PGresult object ptr (if i >= 0), or
 * the n+i'th one (if i < 0), or NULL (if i out of bounds).
 * NB: Caller must not PQclear() the PGresult object.
 */
struct pg_result *
cdbdisp_getPGresult(CdbDispatchResult *dispatchResult, int i)
{
    if (dispatchResult)
    {
        PQExpBuffer buf = dispatchResult->resultbuf;
        PGresult  **begp = (PGresult **)buf->data;
        PGresult  **endp = (PGresult **)(buf->data + buf->len);
        PGresult  **p = (i >= 0) ? &begp[i] : &endp[i];

        if (p >= begp && p < endp)
        {
            Assert(*p != NULL);
            return *p;
        }
    }
    return NULL;
}                               /* cdbdisp_getPGresult */


/* Return the number of PGresult objects in the result buffer. */
int
cdbdisp_numPGresult(CdbDispatchResult  *dispatchResult)
{
    return dispatchResult ? dispatchResult->resultbuf->len / sizeof(PGresult *)
                          : 0;
}                               /* cdbdisp_numPGresult */


/* Remove all of the PGresult ptrs from a CdbDispatchResult object
 * and place them into an array provided by the caller.  The caller
 * becomes responsible for PQclear()ing them.  Returns the number of
 * PGresult ptrs placed in the array.
 */
int
cdbdisp_snatchPGresults(CdbDispatchResult  *dispatchResult,
                        struct pg_result  **pgresultptrs,
                        int                 maxresults)
{
    PQExpBuffer     buf = dispatchResult->resultbuf;
    PGresult      **begp = (PGresult **)buf->data;
    PGresult      **endp = (PGresult **)(buf->data + buf->len);
    PGresult      **p;
    int             nresults = 0;

    /* Snatch the PGresult objects. */
    for (p = begp; p < endp; ++p)
    {
        Assert(*p != NULL);
        Assert(nresults < maxresults);
        pgresultptrs[nresults++] = *p;
        *p = NULL;
    }

    /* Empty our PGresult array. */
    resetPQExpBuffer(buf);
    dispatchResult->errindex = -1;
    dispatchResult->okindex = -1;

    return nresults;
}                               /* cdbdisp_snatchPGresults */


/* Display a CdbDispatchResult in the log for debugging.
 * Call only from main thread, during or after cdbdisp_checkDispatchResults.
 */
void
cdbdisp_debugDispatchResult(CdbDispatchResult  *dispatchResult,
                            int                 elevel_error,
                            int                 elevel_success)
{
    char        esqlstate[8];
    int         ires;
    int         nres;

    /* Skip if user has messages turned off. */
    if (elevel_error < log_min_messages &&
        elevel_success < log_min_messages)
        return;
    
    if (dispatchResult == NULL)
    	return;

    /* PGresult messages */
    nres = cdbdisp_numPGresult(dispatchResult);
    for (ires = 0; ires < nres; ++ires)
    {                           /* for each PGresult */
        PGresult       *pgresult = cdbdisp_getPGresult(dispatchResult, ires);
        ExecStatusType  resultStatus = PQresultStatus(pgresult);
        char   *whoami = PQresultErrorField(pgresult, PG_DIAG_GP_PROCESS_TAG);

        if (!whoami)
            whoami = "no process id";

        /* QE success */
        if (resultStatus == PGRES_COMMAND_OK ||
            resultStatus == PGRES_TUPLES_OK ||
            resultStatus == PGRES_COPY_IN ||
            resultStatus == PGRES_COPY_OUT ||
            resultStatus == PGRES_EMPTY_QUERY)
        {
            char *cmdStatus = PQcmdStatus(pgresult);

            elog(elevel_success, "DispatchResult: ok %s (%s)",
                 cmdStatus ? cmdStatus : "(no cmdStatus)",
                 whoami);
        }

        /* QE error or libpq error */
        else
        {
            char   *sqlstate = PQresultErrorField(pgresult, PG_DIAG_SQLSTATE);
            char   *pri = PQresultErrorField(pgresult, PG_DIAG_MESSAGE_PRIMARY);
            char   *dtl = PQresultErrorField(pgresult, PG_DIAG_MESSAGE_DETAIL);
            int     lenpri = (pri == NULL)? 0 :strlen(pri);

            if (!sqlstate)
                sqlstate = "no SQLSTATE";

            while (lenpri > 0 &&
                   pri[lenpri-1] <= ' ' &&
                   pri[lenpri-1] > '\0')
                lenpri--;

            ereport(elevel_error, (
                 errmsg("DispatchResult: (%s) %s %.*s (%s)",
                        sqlstate,
                        PQresStatus(PQresultStatus(pgresult)),
                        lenpri,
                        pri ? pri : "",
                        whoami),
                 errdetail("(%s:%s) %s",
                           PQresultErrorField(pgresult, PG_DIAG_SOURCE_FILE),
                           PQresultErrorField(pgresult, PG_DIAG_SOURCE_LINE),
                           dtl ? dtl : "") ));
        }
    }                           /* for each PGresult */

    /* Error found on our side of the libpq interface? */
    if (dispatchResult->error_message &&
        dispatchResult->error_message->len > 0)
    {
        cdbdisp_errcode_to_sqlstate(dispatchResult->errcode, esqlstate);
        elog(elevel_error, "DispatchResult: (%s) %s",
             esqlstate,
             dispatchResult->error_message->data);
    }

    /* Connection error? */
    if (dispatchResult->segdbDesc &&
        dispatchResult->segdbDesc->error_message.len > 0)
	{
        cdbdisp_errcode_to_sqlstate(dispatchResult->segdbDesc->errcode, esqlstate);
        elog(elevel_error, "DispatchResult: (%s) %s",
             esqlstate,
             dispatchResult->segdbDesc->error_message.data);
	}

    /* Should have either an error code or an ok result. */
    if (!dispatchResult->errcode &&
        dispatchResult->okindex < 0)
    {
        elog(elevel_error, "DispatchResult: No ending status from %s",
             dispatchResult->segdbDesc ? dispatchResult->segdbDesc->whoami
                                       : "?");
    }
}                               /* cdbdisp_debugDispatchResult */


/*
 * Format a CdbDispatchResult into a StringInfo buffer provided by caller.
 * If verbose = true, reports all info; else reports at most one error.
 */
void
cdbdisp_dumpDispatchResult(CdbDispatchResult       *dispatchResult,
                           bool                     verbose,
                           struct StringInfoData   *buf)
{
    SegmentDatabaseDescriptor  *segdbDesc = dispatchResult->segdbDesc;
    int     ires;
    int     nres;

    if (!dispatchResult ||
        !buf)
        return;

    /* Format PGresult messages */
    nres = cdbdisp_numPGresult(dispatchResult);
    for (ires = 0; ires < nres; ++ires)
    {                           /* for each PGresult */
        PGresult       *pgresult = cdbdisp_getPGresult(dispatchResult, ires);
        ExecStatusType  resultStatus = PQresultStatus(pgresult);
        char   *whoami = PQresultErrorField(pgresult, PG_DIAG_GP_PROCESS_TAG);

        /* QE success */
        if (resultStatus == PGRES_COMMAND_OK ||
            resultStatus == PGRES_TUPLES_OK ||
            resultStatus == PGRES_COPY_IN ||
            resultStatus == PGRES_COPY_OUT ||
            resultStatus == PGRES_EMPTY_QUERY)
        {
            if (verbose)
            {
                char *cmdStatus = PQcmdStatus(pgresult);

                oneTrailingNewline(buf);
                appendStringInfo(buf, "ok: %s", cmdStatus ? cmdStatus : "");
                if (whoami)
                    appendStringInfo(buf," (%s)", whoami);
            }
        }

        /* QE error or libpq error */
        else
        {
            char   *pri = PQresultErrorField(pgresult, PG_DIAG_MESSAGE_PRIMARY);
            char   *dtl = PQresultErrorField(pgresult, PG_DIAG_MESSAGE_DETAIL);
            char   *ctx = PQresultErrorField(pgresult, PG_DIAG_CONTEXT);

            oneTrailingNewline(buf);
            if (pri)
            {
            	appendStringInfoString(buf, pri);
            }
            else
            {
            	elog(LOG,"No primary message?");
            	appendStringInfoString(buf,PQresultErrorMessage(pgresult));
            }

            if (whoami)
            {
                noTrailingNewline(buf);
                appendStringInfo(buf,"  (%s)", whoami);
            }

            if (dtl)
            {
                oneTrailingNewline(buf);
                appendStringInfo(buf, "%s", dtl);
            }
			
            if (ctx)
            {
                oneTrailingNewline(buf);
                appendStringInfo(buf, "%s", ctx);
            }

            if (!verbose)
                goto done;
        }
    }                           /* for each PGresult */

    /* segdbDesc error message shouldn't be possible here, but check anyway.
     *   Ordinarily dispatchResult->segdbDesc is NULL here because we are
     *   called after it has been cleared by CdbCheckDispatchResult().
     */
    if (dispatchResult->segdbDesc)
        cdbdisp_mergeConnectionErrors(dispatchResult, segdbDesc);

    /* Error found on our side of the libpq interface? */
    if (dispatchResult->error_message &&
        dispatchResult->error_message->len > 0)
    {
        oneTrailingNewline(buf);
        appendStringInfoString(buf, dispatchResult->error_message->data);
        if (!verbose)
            goto done;
    }

done:
    noTrailingNewline(buf);
}                               /* cdbdisp_dumpDispatchResult */


/*--------------------------------------------------------------------*/


/*
 * cdbdisp_makeDispatchResults:
 * Allocates a CdbDispatchResults object in the current memory context.
 * The caller is responsible for calling DestroyCdbDispatchResults on the returned
 * pointer when done using it.
 */
CdbDispatchResults *
cdbdisp_makeDispatchResults(int     resultCapacity,
                            int     sliceCapacity,
                            bool    cancelOnError)
{
    CdbDispatchResults *results = palloc0(sizeof(*results));
    int     nbytes = resultCapacity * sizeof(results->resultArray[0]);

    results->resultArray = palloc0(nbytes);
    results->resultCapacity = resultCapacity;
    results->resultCount = 0;
    results->iFirstError = -1;
    results->errcode = 0;
    results->cancelOnError = cancelOnError;

    results->sliceMap = NULL;
    results->sliceCapacity = sliceCapacity;
    if (sliceCapacity > 0)
    {
        nbytes = sliceCapacity * sizeof(results->sliceMap[0]);
        results->sliceMap = palloc0(nbytes);
    }

    return results;
}                               /* cdbdisp_makeDispatchResults */

/*
 * cdbdisp_destroyDispatchResults:
 * Frees all memory allocated in CdbDispatchResults struct.
 */
void
cdbdisp_destroyDispatchResults(CdbDispatchResults * results)
{
    if (!results)
        return;

    if (results->resultArray)
    {
        int i;
        for (i = 0; i < results->resultCount; i++)
        {
            cdbdisp_termResult(&results->resultArray[i]);
        }
        pfree(results->resultArray);
		results->resultArray = NULL;
    }

    if (results->sliceMap)
    {
        pfree(results->sliceMap);
        results->sliceMap = NULL;
    }

    pfree(results);
}                               /* cdbdisp_destroyDispatchResults */


/* Format a CdbDispatchResults object.
 * Appends error messages to caller's StringInfo buffer.
 * Returns ERRCODE_xxx if some error was found, or 0 if no errors.
 * Before calling this function, you must call CdbCheckDispatchResult().
 */
int
cdbdisp_dumpDispatchResults(struct CdbDispatchResults  *meleeResults,
                            struct StringInfoData      *buffer,
                            bool                        verbose)
{
    CdbDispatchResult  *dispatchResult;

    /* Quick exit if no error (not counting ERRCODE_GP_OPERATION_CANCELED). */
    if (!meleeResults ||
        !meleeResults->errcode)
        return 0;

    /* Find the CdbDispatchResult of the first QE that got an error. */
    Assert(meleeResults->iFirstError >= 0 &&
           meleeResults->iFirstError < meleeResults->resultCount);

    dispatchResult = &meleeResults->resultArray[meleeResults->iFirstError];

    Assert(dispatchResult->meleeResults == meleeResults &&
           dispatchResult->errcode != 0);

    /* Format one QE's result. */
    cdbdisp_dumpDispatchResult(dispatchResult, verbose, buffer);

    /* Optionally, format results from the rest of the QEs that got errors. */
    if (verbose)
    {
        int i;
        for (i = 0; i < meleeResults->resultCount; ++i)
        {
            dispatchResult = &meleeResults->resultArray[i];
            if (i == meleeResults->iFirstError)
                continue;
            if (dispatchResult->errcode)
                cdbdisp_dumpDispatchResult(dispatchResult, verbose, buffer);
        }
    }

    return meleeResults->errcode;
}                               /* cdbdisp_dumpDispatchResults */


/* Return sum of the cmdTuples values from CdbDispatchResult
 * entries that have a successful PGresult.  If sliceIndex >= 0,
 * uses only the results belonging to the specified slice.
 */
int64
cdbdisp_sumCmdTuples(CdbDispatchResults *results, int sliceIndex)
{
    CdbDispatchResult  *dispatchResult;
    CdbDispatchResult  *resultEnd = cdbdisp_resultEnd(results, sliceIndex);
    PGresult           *pgresult;
    int64              sum = 0;

    for (dispatchResult = cdbdisp_resultBegin(results, sliceIndex);
         dispatchResult < resultEnd;
         ++dispatchResult)
    {
        pgresult = cdbdisp_getPGresult(dispatchResult, dispatchResult->okindex);
        if (pgresult &&
            !dispatchResult->errcode)
        {
            char   *cmdTuples = PQcmdTuples(pgresult);
            if (cmdTuples)
                sum += atoll(cmdTuples);
        }
    }
    return sum;
}                               /* cdbdisp_sumCmdTuples */

/*
 * If several tuples were eliminated/rejected from the result because of
 * bad data formatting (this is currenly only possible in external tables
 * with single row error handling) - sum up the total rows rejected from
 * all QE's and notify the client.
 */
void
cdbdisp_sumRejectedRows(CdbDispatchResults *results)
{
    CdbDispatchResult  *dispatchResult;
    CdbDispatchResult  *resultEnd = cdbdisp_resultEnd(results, -1);
    PGresult           *pgresult;
    int					totalRejected = 0;
	
    for (dispatchResult = cdbdisp_resultBegin(results, -1);
         dispatchResult < resultEnd;
         ++dispatchResult)
    {
        pgresult = cdbdisp_getPGresult(dispatchResult, dispatchResult->okindex);
        if (pgresult &&
            !dispatchResult->errcode)
        {
			/* add num rows rejected from this QE to the total */
			totalRejected += dispatchResult->numrowsrejected;
        }
    }
    
	if(totalRejected > 0)
		ReportSrehResults(NULL, totalRejected);

}                               /* cdbdisp_sumRejectedRows */

HTAB *
process_aotupcounts(PartitionNode *parts, HTAB *ht,
					void *aotupcounts, int naotupcounts)
{
	PQaoRelTupCount *ao = (PQaoRelTupCount *)aotupcounts;
	
	if (Debug_appendonly_print_insert)
		ereport(LOG,(errmsg("found %d AO tuple counts to process",
							naotupcounts)));

	if (naotupcounts)
	{
		int j;

		for (j = 0; j < naotupcounts; j++)
		{
			if (OidIsValid(ao->aorelid))
			{
				bool found;
				PQaoRelTupCount *entry;

				if (!ht)
				{
					HASHCTL ctl;
					/* reasonable assumption? */
					long num_buckets = 
						list_length(all_partition_relids(parts));
					num_buckets /= num_partition_levels(parts);

					ctl.keysize = sizeof(Oid);
					ctl.entrysize = sizeof(*entry);
					ht = hash_create("AO hash map",
									 num_buckets,
									 &ctl,
									 HASH_ELEM);
				}

				entry = hash_search(ht,
									&(ao->aorelid),
									HASH_ENTER,
									&found);
				
				if (found)
					entry->tupcount += ao->tupcount;
				else
					entry->tupcount = ao->tupcount;
								
				if (Debug_appendonly_print_insert)
					ereport(LOG,(errmsg("processed AO tuple counts for partitioned "
										"relation %d. found total " INT64_FORMAT 
										"tuples", ao->aorelid, entry->tupcount)));
			}
			ao++;
		}
	}


	return ht;
}

void
cdbdisp_handleModifiedCatalogOnSegments(CdbDispatchResults *results,
		void (*handler)(QueryContextDispatchingSendBack sendback))
{
	int i;
	for (i = 0; i < results->resultCount; ++i)
	{
		CdbDispatchResult *dispatchResult = &results->resultArray[i];
		int nres = cdbdisp_numPGresult(dispatchResult);
		int ires;
		for (ires = 0; ires < nres; ++ires)
		{
			/* for each PGresult */
			PGresult *pgresult = cdbdisp_getPGresult(dispatchResult, ires);
			if (handler && pgresult && pgresult->sendback)
			{
				int j;
				for (j = 0 ; j < pgresult->numSendback ; ++j)
				{
					handler(&pgresult->sendback[j]);
				}
			}
		}
	}
}

void
cdbdisp_iterate_results_sendback(PGresult **results, int numresults,
			void (*handler)(QueryContextDispatchingSendBack sendback))
{
	int i, j;

	if (handler == NULL)
		return;

	for (i = 0; i < numresults; i++)
	{
		if (results[i] == NULL || results[i]->sendback == 0)
			continue;

		for (j = 0; j < results[i]->numSendback; j++)
			handler(&results[i]->sendback[j]);		
	}
}

/*
 * sum tuple counts that were added into a partitioned AO table
 */
HTAB *
cdbdisp_sumAoPartTupCount(PartitionNode *parts,
						  CdbDispatchResults *results)
{
	int i;
	HTAB *ht = NULL;

	if (!parts)
		return NULL;


	for (i = 0; i < results->resultCount; ++i)
	{
		CdbDispatchResult  *dispatchResult = &results->resultArray[i];
		int nres = cdbdisp_numPGresult(dispatchResult);
		int ires;
		for (ires = 0; ires < nres; ++ires)
		{						   /* for each PGresult */
			PGresult *pgresult = cdbdisp_getPGresult(dispatchResult, ires);

			ht = process_aotupcounts(parts, ht, (void *)pgresult->aotupcounts,
									 pgresult->naotupcounts);
		}
	}

	return ht;
}
/*
 * Find the max of the lastOid values returned from the QEs
 */
Oid
cdbdisp_maxLastOid(CdbDispatchResults *results, int sliceIndex)
{
    CdbDispatchResult  *dispatchResult;
    CdbDispatchResult  *resultEnd = cdbdisp_resultEnd(results, sliceIndex);
    PGresult           *pgresult;
    Oid                 oid = InvalidOid;

    for (dispatchResult = cdbdisp_resultBegin(results, sliceIndex);
         dispatchResult < resultEnd;
         ++dispatchResult)
    {
        pgresult = cdbdisp_getPGresult(dispatchResult, dispatchResult->okindex);
        if (pgresult &&
            !dispatchResult->errcode)
        {
            Oid tmpoid = PQoidValue(pgresult);
            if (tmpoid >oid)
                oid = tmpoid;
        }
    }

    return oid;
}                               /* cdbdisp_maxLastOid */

/* Return ptr to first resultArray entry for a given sliceIndex. */
CdbDispatchResult *
cdbdisp_resultBegin(CdbDispatchResults *results, int sliceIndex)
{
    CdbDispatchResults_SliceInfo   *si;

    if (!results)
        return NULL;

    if (sliceIndex < 0)
        return &results->resultArray[0];
    
    si = &results->sliceMap[sliceIndex];

    Assert(sliceIndex < results->sliceCapacity &&
           si->resultBegin >= 0 &&
           si->resultBegin <= si->resultEnd &&
           si->resultEnd <= results->resultCount);

    return &results->resultArray[si->resultBegin];
}                               /* cdbdisp_resultBegin */


/* Return ptr to last+1 resultArray entry for a given sliceIndex. */
CdbDispatchResult *
cdbdisp_resultEnd(CdbDispatchResults *results, int sliceIndex)
{
    CdbDispatchResults_SliceInfo   *si;

    if (!results)
        return NULL;

    if (sliceIndex < 0)
        return &results->resultArray[results->resultCount];
    
    si = &results->sliceMap[sliceIndex];

    return &results->resultArray[si->resultEnd];
}                               /* cdbdisp_resultEnd */


/*--------------------------------------------------------------------*/


/* Convert compact error code (ERRCODE_xxx) to 5-char SQLSTATE string,
 * and put it into a 6-char buffer provided by caller.
 */
char *                          /* returns outbuf+5 */
cdbdisp_errcode_to_sqlstate(int errcode, char outbuf[6])
{
    int i;
    for (i = 0; i < 5; ++i)
    {
        outbuf[i] = PGUNSIXBIT(errcode);
        errcode >>= 6;
    }
    outbuf[5] = '\0';
    return &outbuf[5];
}                               /* cdbdisp_errcode_to_sqlstate */


/* Convert SQLSTATE string to compact error code (ERRCODE_xxx). */
int
cdbdisp_sqlstate_to_errcode(const char *sqlstate)
{
    return MAKE_SQLSTATE(sqlstate[0], sqlstate[1], sqlstate[2],
        sqlstate[3], sqlstate[4]);
}                               /* cdbdisp_sqlstate_to_errcode */
